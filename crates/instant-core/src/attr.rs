use std::collections::HashMap;

use serde_json::{json, Map, Value};
use sqlx::{PgConnection, PgExecutor, Row};
use uuid::Uuid;

use crate::error::{InstantError, Result};
use crate::system_catalog;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    Blob,
    Ref,
}

impl ValueType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ValueType::Blob => "blob",
            ValueType::Ref => "ref",
        }
    }
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "blob" => Ok(ValueType::Blob),
            "ref" => Ok(ValueType::Ref),
            other => Err(InstantError::validation_failed(
                "attributes",
                format!("invalid value-type {other:?}"),
                json!([]),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cardinality {
    One,
    Many,
}

impl Cardinality {
    pub fn as_str(&self) -> &'static str {
        match self {
            Cardinality::One => "one",
            Cardinality::Many => "many",
        }
    }
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "one" => Ok(Cardinality::One),
            "many" => Ok(Cardinality::Many),
            other => Err(InstantError::validation_failed(
                "attributes",
                format!("invalid cardinality {other:?}"),
                json!([]),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckedDataType {
    String,
    Number,
    Boolean,
    Date,
}

impl CheckedDataType {
    pub fn as_str(&self) -> &'static str {
        match self {
            CheckedDataType::String => "string",
            CheckedDataType::Number => "number",
            CheckedDataType::Boolean => "boolean",
            CheckedDataType::Date => "date",
        }
    }
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "string" => Ok(CheckedDataType::String),
            "number" => Ok(CheckedDataType::Number),
            "boolean" => Ok(CheckedDataType::Boolean),
            "date" => Ok(CheckedDataType::Date),
            other => Err(InstantError::validation_failed(
                "attributes",
                format!("invalid checked-data-type {other:?}"),
                json!([]),
            )),
        }
    }
}

/// One logical attribute (attrs row + its ident rows).
#[derive(Debug, Clone)]
pub struct Attr {
    pub id: Uuid,
    pub value_type: ValueType,
    pub cardinality: Cardinality,
    pub forward_ident: Uuid,
    pub etype: String,
    pub label: String,
    pub reverse_ident: Option<Uuid>,
    pub reverse_etype: Option<String>,
    pub reverse_label: Option<String>,
    pub is_unique: bool,
    pub is_indexed: bool,
    pub is_required: bool,
    pub checked_data_type: Option<CheckedDataType>,
    pub on_delete_cascade: bool,
    pub on_delete_reverse_cascade: bool,
    /// true when this attr belongs to the system-catalog app.
    pub is_system: bool,
    /// In-flight indexing-job markers (attrs.indexing / checking_data_type /
    /// setting_unique): true while a background job is rewriting the attr's
    /// triples. Query planning ignores an index / type / uniqueness whose
    /// job is still running (legacy attr_pat.clj best-index).
    pub indexing: bool,
    pub checking_data_type: bool,
    pub setting_unique: bool,
    /// `attrs.inferred_types` bitset (legacy attr.clj `types`: number=1,
    /// string=2, boolean=4, json=8); None until a value was ever written.
    pub inferred_types: Option<u32>,
    /// `attrs.metadata` (jsonb); on the wire whenever the column is non-null
    /// (legacy row->attr) — e.g. `{}` after a soft-delete/restore round trip.
    pub metadata: Option<Value>,
}

/// Bit for each inferred type, in legacy's `types` vector order
/// (attr.clj:22-33).
pub const INFERRED_NUMBER: u32 = 1;
pub const INFERRED_STRING: u32 = 2;
pub const INFERRED_BOOLEAN: u32 = 4;
pub const INFERRED_JSON: u32 = 8;

/// Legacy `inferred-value-type` (attr.clj:41-46): nil contributes nothing,
/// uuids and strings are strings, everything non-scalar is json.
pub fn inferred_type_bit(v: &Value) -> Option<u32> {
    match v {
        Value::Null => None,
        Value::String(_) => Some(INFERRED_STRING),
        Value::Number(_) => Some(INFERRED_NUMBER),
        Value::Bool(_) => Some(INFERRED_BOOLEAN),
        Value::Array(_) | Value::Object(_) => Some(INFERRED_JSON),
    }
}

/// Wire form of the bitset: legacy renders the Clojure keyword set, whose
/// iteration order is number, string, json, boolean (observed live against
/// the legacy server — hash order, not declaration order).
pub fn inferred_types_wire(bits: Option<u32>) -> Value {
    match bits {
        None => Value::Null,
        Some(b) => Value::Array(
            [
                (INFERRED_NUMBER, "number"),
                (INFERRED_STRING, "string"),
                (INFERRED_JSON, "json"),
                (INFERRED_BOOLEAN, "boolean"),
            ]
            .into_iter()
            .filter(|(bit, _)| b & bit != 0)
            .map(|(_, name)| json!(name))
            .collect(),
        ),
    }
}

impl Attr {
    /// Wire JSON exactly as the legacy server serializes attrs (kebab keys, `?` suffixes).
    pub fn to_wire(&self) -> Value {
        let mut m = Map::new();
        m.insert("id".into(), json!(self.id));
        m.insert("value-type".into(), json!(self.value_type.as_str()));
        m.insert("cardinality".into(), json!(self.cardinality.as_str()));
        m.insert(
            "forward-identity".into(),
            json!([self.forward_ident, self.etype, self.label]),
        );
        if let (Some(rid), Some(retype), Some(rlabel)) =
            (self.reverse_ident, &self.reverse_etype, &self.reverse_label)
        {
            m.insert("reverse-identity".into(), json!([rid, retype, rlabel]));
        }
        m.insert("unique?".into(), json!(self.is_unique));
        m.insert("index?".into(), json!(self.is_indexed));
        m.insert("required?".into(), json!(self.is_required));
        m.insert(
            "inferred-types".into(),
            inferred_types_wire(self.inferred_types),
        );
        m.insert(
            "catalog".into(),
            json!(if self.is_system { "system" } else { "user" }),
        );
        if self.on_delete_cascade {
            m.insert("on-delete".into(), json!("cascade"));
        }
        if self.on_delete_reverse_cascade {
            m.insert("on-delete-reverse".into(), json!("cascade"));
        }
        if let Some(cdt) = self.checked_data_type {
            m.insert("checked-data-type".into(), json!(cdt.as_str()));
        }
        // legacy row->attr: present only while true
        if self.checking_data_type {
            m.insert("checking-data-type?".into(), json!(true));
        }
        if self.indexing {
            m.insert("indexing?".into(), json!(true));
        }
        if self.setting_unique {
            m.insert("setting-unique?".into(), json!(true));
        }
        if let Some(md) = &self.metadata {
            m.insert("metadata".into(), md.clone());
        }
        Value::Object(m)
    }

    /// `index?` as the query planner sees it: an index whose job is still
    /// running is not usable (triples are only partially flagged).
    pub fn indexed_for_query(&self) -> bool {
        self.is_indexed && !self.indexing
    }

    /// `unique?` as the query planner sees it.
    pub fn unique_for_query(&self) -> bool {
        self.is_unique && !self.setting_unique
    }

    /// `checked-data-type` as the query planner sees it: None while a
    /// check-data-type job is still flagging triples.
    pub fn checked_type_for_query(&self) -> Option<CheckedDataType> {
        if self.checking_data_type {
            None
        } else {
            self.checked_data_type
        }
    }

    /// Parse an attr map from an `add-attr` tx-step (client wire format).
    pub fn from_wire(v: &Value) -> Result<Attr> {
        let obj = v.as_object().ok_or_else(|| {
            InstantError::validation_failed("attributes", "add-attr expects an object", json!([]))
        })?;
        let get_uuid = |v: &Value| -> Result<Uuid> {
            v.as_str()
                .and_then(|s| Uuid::parse_str(s).ok())
                .ok_or_else(|| {
                    InstantError::validation_failed("attributes", "expected a uuid", json!([]))
                })
        };
        let id = get_uuid(obj.get("id").unwrap_or(&Value::Null))?;
        let fwd = obj
            .get("forward-identity")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                InstantError::validation_failed("attributes", "missing forward-identity", json!([]))
            })?;
        if fwd.len() < 3 {
            return Err(InstantError::validation_failed(
                "attributes",
                "forward-identity must be [id, etype, label]",
                json!([]),
            ));
        }
        let forward_ident = get_uuid(&fwd[0])?;
        let etype = fwd[1].as_str().unwrap_or_default().to_string();
        let label = fwd[2].as_str().unwrap_or_default().to_string();
        let (reverse_ident, reverse_etype, reverse_label) = match obj.get("reverse-identity") {
            Some(Value::Array(rev)) if rev.len() >= 3 => (
                Some(get_uuid(&rev[0])?),
                Some(rev[1].as_str().unwrap_or_default().to_string()),
                Some(rev[2].as_str().unwrap_or_default().to_string()),
            ),
            _ => (None, None, None),
        };
        let value_type = ValueType::parse(
            obj.get("value-type")
                .and_then(|v| v.as_str())
                .unwrap_or("blob"),
        )?;
        let cardinality = Cardinality::parse(
            obj.get("cardinality")
                .and_then(|v| v.as_str())
                .unwrap_or("one"),
        )?;
        if value_type == ValueType::Ref && reverse_ident.is_none() {
            return Err(InstantError::validation_failed(
                "attributes",
                "ref attrs require a reverse-identity",
                json!([]),
            ));
        }
        Ok(Attr {
            id,
            value_type,
            cardinality,
            forward_ident,
            etype,
            label,
            reverse_ident,
            reverse_etype,
            reverse_label,
            is_unique: obj
                .get("unique?")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            is_indexed: obj.get("index?").and_then(|v| v.as_bool()).unwrap_or(false),
            is_required: obj
                .get("required?")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            checked_data_type: match obj.get("checked-data-type").and_then(|v| v.as_str()) {
                Some(s) => Some(CheckedDataType::parse(s)?),
                None => None,
            },
            on_delete_cascade: obj.get("on-delete").and_then(|v| v.as_str()) == Some("cascade"),
            on_delete_reverse_cascade: obj.get("on-delete-reverse").and_then(|v| v.as_str())
                == Some("cascade"),
            is_system: false,
            indexing: false,
            checking_data_type: false,
            setting_unique: false,
            inferred_types: None,
            metadata: None,
        })
    }

    /// Triple index flags derived from this attr (see DATAMODEL.md §3).
    pub fn flags(&self) -> TripleFlags {
        let is_ref = self.value_type == ValueType::Ref;
        TripleFlags {
            ea: self.cardinality == Cardinality::One,
            eav: is_ref,
            av: self.is_unique,
            ave: self.is_indexed,
            vae: is_ref,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TripleFlags {
    pub ea: bool,
    pub eav: bool,
    pub av: bool,
    pub ave: bool,
    pub vae: bool,
}

/// In-memory view of an app's attrs (its own + the system catalog's).
#[derive(Debug, Clone, Default)]
pub struct AttrMap {
    by_id: HashMap<Uuid, Attr>,
    by_fwd: HashMap<(String, String), Uuid>,
    by_rev: HashMap<(String, String), Uuid>,
}

impl AttrMap {
    pub fn insert(&mut self, attr: Attr) {
        self.by_fwd
            .insert((attr.etype.clone(), attr.label.clone()), attr.id);
        if let (Some(re), Some(rl)) = (&attr.reverse_etype, &attr.reverse_label) {
            self.by_rev.insert((re.clone(), rl.clone()), attr.id);
        }
        self.by_id.insert(attr.id, attr);
    }

    pub fn remove(&mut self, id: &Uuid) {
        if let Some(attr) = self.by_id.remove(id) {
            self.by_fwd
                .remove(&(attr.etype.clone(), attr.label.clone()));
            if let (Some(re), Some(rl)) = (&attr.reverse_etype, &attr.reverse_label) {
                self.by_rev.remove(&(re.clone(), rl.clone()));
            }
        }
    }

    pub fn get(&self, id: &Uuid) -> Option<&Attr> {
        self.by_id.get(id)
    }

    pub fn get_mut(&mut self, id: &Uuid) -> Option<&mut Attr> {
        self.by_id.get_mut(id)
    }

    pub fn by_fwd_name(&self, etype: &str, label: &str) -> Option<&Attr> {
        self.by_fwd
            .get(&(etype.to_string(), label.to_string()))
            .and_then(|id| self.by_id.get(id))
    }

    pub fn by_rev_name(&self, etype: &str, label: &str) -> Option<&Attr> {
        self.by_rev
            .get(&(etype.to_string(), label.to_string()))
            .and_then(|id| self.by_id.get(id))
    }

    pub fn id_attr_of(&self, etype: &str) -> Option<&Attr> {
        self.by_fwd_name(etype, "id")
    }

    pub fn iter(&self) -> impl Iterator<Item = &Attr> {
        self.by_id.values()
    }

    pub fn attrs_of_etype<'a>(&'a self, etype: &'a str) -> impl Iterator<Item = &'a Attr> + 'a {
        self.by_id.values().filter(move |a| a.etype == etype)
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Wire array of attrs, with server-only system attrs hidden
    /// (see attr.clj remove-hidden).
    pub fn to_wire_visible(&self) -> Value {
        // Legacy sends every attr definition, system catalog included
        // ($oauthCodes, $magicCodes, ... — verified against the legacy server
        // by scripts/differential/replay.mjs); clients key attrs by id and
        // ignore unreferenced ones. Deterministic order keeps attrs-changed
        // hashing and conformance diffing reliable.
        let mut attrs: Vec<&Attr> = self.by_id.values().collect();
        attrs.sort_by_key(|a| a.id);
        Value::Array(attrs.into_iter().map(|a| a.to_wire()).collect())
    }
}

const ATTR_COLUMNS: &str = r#"
SELECT a.id, a.app_id, a.value_type, a.cardinality, a.is_unique, a.is_indexed,
       coalesce(a.is_required, false) AS is_required,
       a.forward_ident, a.reverse_ident,
       a.etype, a.label, a.reverse_etype, a.reverse_label,
       a.checked_data_type::text AS checked_data_type,
       (a.on_delete = 'cascade') AS on_delete_cascade,
       (a.on_delete_reverse = 'cascade') AS on_delete_reverse_cascade,
       coalesce(a.indexing, false) AS indexing,
       coalesce(a.checking_data_type, false) AS checking_data_type,
       coalesce(a.setting_unique, false) AS setting_unique,
       a.inferred_types::int4 AS inferred_types,
       a.metadata
FROM attrs a
"#;

const ATTR_SELECT: &str = r#"
SELECT a.id, a.app_id, a.value_type, a.cardinality, a.is_unique, a.is_indexed,
       coalesce(a.is_required, false) AS is_required,
       a.forward_ident, a.reverse_ident,
       a.etype, a.label, a.reverse_etype, a.reverse_label,
       a.checked_data_type::text AS checked_data_type,
       (a.on_delete = 'cascade') AS on_delete_cascade,
       (a.on_delete_reverse = 'cascade') AS on_delete_reverse_cascade,
       coalesce(a.indexing, false) AS indexing,
       coalesce(a.checking_data_type, false) AS checking_data_type,
       coalesce(a.setting_unique, false) AS setting_unique,
       a.inferred_types::int4 AS inferred_types,
       a.metadata
FROM attrs a
WHERE a.deletion_marked_at IS NULL AND a.app_id = ANY($1)
"#;

fn row_to_attr(row: &sqlx::postgres::PgRow) -> Result<Attr> {
    let app_id: Uuid = row.get("app_id");
    Ok(Attr {
        id: row.get("id"),
        value_type: ValueType::parse(row.get::<String, _>("value_type").as_str())?,
        cardinality: Cardinality::parse(row.get::<String, _>("cardinality").as_str())?,
        forward_ident: row.get("forward_ident"),
        etype: row.get("etype"),
        label: row.get("label"),
        reverse_ident: row.get("reverse_ident"),
        reverse_etype: row.get("reverse_etype"),
        reverse_label: row.get("reverse_label"),
        is_unique: row.get("is_unique"),
        is_indexed: row.get("is_indexed"),
        is_required: row.get("is_required"),
        checked_data_type: match row.get::<Option<String>, _>("checked_data_type") {
            Some(s) => Some(CheckedDataType::parse(&s)?),
            None => None,
        },
        on_delete_cascade: row
            .get::<Option<bool>, _>("on_delete_cascade")
            .unwrap_or(false),
        on_delete_reverse_cascade: row
            .get::<Option<bool>, _>("on_delete_reverse_cascade")
            .unwrap_or(false),
        is_system: app_id == system_catalog::SYSTEM_CATALOG_APP_ID,
        indexing: row.get("indexing"),
        checking_data_type: row.get("checking_data_type"),
        setting_unique: row.get("setting_unique"),
        inferred_types: row
            .get::<Option<i32>, _>("inferred_types")
            .map(|b| b as u32),
        metadata: row.get::<Option<Value>, _>("metadata"),
    })
}

/// Legacy `insert-attr-inferred-types-cte` (triple.clj:120-153): OR the
/// value types written by this tx into each attr's bitset. Skips nulls, bits
/// already present in the in-memory catalog, and system-catalog attrs; the
/// `IS DISTINCT FROM` guard makes the steady state a zero-row update.
/// Returns the ids of the attr rows that actually changed (callers treat
/// that as an attrs change so caches and clients pick up the new catalog).
pub async fn record_inferred_types(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &mut AttrMap,
    values: impl IntoIterator<Item = (Uuid, u32)>,
) -> Result<Vec<Uuid>> {
    let mut pending: HashMap<Uuid, u32> = HashMap::new();
    for (attr_id, bit) in values {
        let Some(attr) = attrs.get(&attr_id) else {
            continue;
        };
        if attr.is_system || attr.inferred_types.unwrap_or(0) & bit != 0 {
            continue;
        }
        *pending.entry(attr_id).or_insert(0) |= bit;
    }
    if pending.is_empty() {
        return Ok(vec![]);
    }
    let (ids, bits): (Vec<Uuid>, Vec<i32>) = pending.iter().map(|(id, b)| (*id, *b as i32)).unzip();
    let rows = sqlx::query(
        r#"
        UPDATE attrs
           SET inferred_types = coalesce(attrs.inferred_types, 0::bit(32)) | u.typ::bit(32)
          FROM unnest($2::uuid[], $3::int4[]) AS u(id, typ)
         WHERE attrs.id = u.id
           AND attrs.app_id = $1
           AND attrs.inferred_types IS DISTINCT FROM
               (coalesce(attrs.inferred_types, 0::bit(32)) | u.typ::bit(32))
        RETURNING attrs.id, attrs.inferred_types::int4 AS inferred_types
        "#,
    )
    .bind(app_id)
    .bind(&ids)
    .bind(&bits)
    .fetch_all(&mut *conn)
    .await?;
    let mut changed = Vec::with_capacity(rows.len());
    for row in &rows {
        let id: Uuid = row.get("id");
        let bits: i32 = row.get("inferred_types");
        if let Some(a) = attrs.get_mut(&id) {
            a.inferred_types = Some(bits as u32);
        }
        changed.push(id);
    }
    Ok(changed)
}

/// Load an app's attrs plus the system catalog attrs.
pub async fn get_by_app_id<'e, E: PgExecutor<'e>>(exec: E, app_id: Uuid) -> Result<AttrMap> {
    let rows = sqlx::query(ATTR_SELECT)
        .bind(vec![app_id, system_catalog::SYSTEM_CATALOG_APP_ID])
        .fetch_all(exec)
        .await?;
    let mut map = AttrMap::default();
    for row in &rows {
        map.insert(row_to_attr(row)?);
    }
    Ok(map)
}

/// Legacy `get-soft-deleted-by-app-id` (attr.clj:963-979): the app's
/// soft-deleted attrs (branded names, `deletion_marked_at` set), by id.
pub async fn get_soft_deleted_by_app_id<'e, E: PgExecutor<'e>>(
    exec: E,
    app_id: Uuid,
) -> Result<Vec<Attr>> {
    let sql = format!(
        "{ATTR_COLUMNS} WHERE a.app_id = $1 AND a.deletion_marked_at IS NOT NULL ORDER BY a.id ASC"
    );
    let rows = sqlx::query(&sql).bind(app_id).fetch_all(exec).await?;
    rows.iter().map(row_to_attr).collect()
}

/// Load specific (live) attrs by id.
pub async fn get_by_ids<'e, E: PgExecutor<'e>>(
    exec: E,
    app_id: Uuid,
    ids: &[Uuid],
) -> Result<Vec<Attr>> {
    let sql = format!(
        "{ATTR_COLUMNS} WHERE a.deletion_marked_at IS NULL AND a.app_id = $1 AND a.id = ANY($2)"
    );
    let rows = sqlx::query(&sql)
        .bind(app_id)
        .bind(ids)
        .fetch_all(exec)
        .await?;
    rows.iter().map(row_to_attr).collect()
}

/// Legacy `restore-multi!` (attr.clj:657-726): un-brand the `{id}_deleted$`
/// prefix from etype/label (and the reverse names), clear
/// `deletion_marked_at` and the `soft_delete_snapshot`, and leave the attr
/// un-indexed and not required so existing triples stay valid. Only rows
/// that are currently soft-deleted match. Returns the restored attrs.
pub async fn restore(conn: &mut PgConnection, app_id: Uuid, ids: &[Uuid]) -> Result<Vec<Attr>> {
    let rows = sqlx::query(
        r#"
        WITH restored_attrs AS (
          UPDATE attrs
             SET deletion_marked_at = NULL,
                 is_indexed = false,
                 is_required = false,
                 metadata = metadata - 'soft_delete_snapshot',
                 etype = substring(etype from position('$' in etype) + 1),
                 label = substring(label from position('$' in etype) + 1),
                 reverse_etype = CASE WHEN reverse_etype IS NOT NULL
                                      THEN substring(reverse_etype from position('$' in etype) + 1)
                                      ELSE NULL END,
                 reverse_label = CASE WHEN reverse_label IS NOT NULL
                                      THEN substring(reverse_label from position('$' in etype) + 1)
                                      ELSE NULL END
           WHERE app_id = $1 AND id = ANY($2) AND deletion_marked_at IS NOT NULL
          RETURNING *
        ), restored_forward_idents AS (
          UPDATE idents fw SET etype = a.etype, label = a.label
            FROM restored_attrs a
           WHERE fw.app_id = $1 AND fw.id = a.forward_ident
        ), restored_rev_idents AS (
          UPDATE idents rv SET etype = a.reverse_etype, label = a.reverse_label
            FROM restored_attrs a
           WHERE rv.app_id = $1 AND rv.id = a.reverse_ident
        )
        SELECT id FROM restored_attrs
        "#,
    )
    .bind(app_id)
    .bind(ids)
    .fetch_all(&mut *conn)
    .await?;
    let restored: Vec<Uuid> = rows.iter().map(|r| r.get("id")).collect();
    if restored.is_empty() {
        return Ok(vec![]);
    }
    get_by_ids(&mut *conn, app_id, &restored).await
}

/// Reserved names, checked on the forward AND reverse identity like legacy
/// `validate-system-ident-names!` (db/model/attr.clj:313-332, run on add and
/// update): users can't claim a catalog ident, nor open a `$` namespace
/// outside the editable etypes — a link whose reverse side lands in
/// `$magicCodes` would otherwise pollute a system table and expose its rows
/// to `data.ref`.
pub fn validate_ident_names(app_id: Uuid, attr: &Attr) -> Result<()> {
    if app_id == system_catalog::SYSTEM_CATALOG_APP_ID {
        return Ok(());
    }
    let fwd = (attr.etype.as_str(), attr.label.as_str());
    let rev = attr
        .reverse_etype
        .as_deref()
        .zip(attr.reverse_label.as_deref());
    for (etype, label) in std::iter::once(fwd).chain(rev) {
        if system_catalog::is_reserved_ident(etype, label) {
            let m = format!("{etype}.{label} is a system column and it already exists.");
            return Err(InstantError::validation_failed(
                "attributes",
                m.clone(),
                json!([{"message": m}]),
            ));
        }
        if etype.starts_with('$') && !system_catalog::is_editable_etype(etype) {
            let m = format!("$ is reserved for system tables. You can't create {etype}");
            return Err(InstantError::validation_failed(
                "attributes",
                m.clone(),
                json!([{"message": m}]),
            ));
        }
    }
    Ok(())
}

/// Legacy `validate-add-required!` (attr.clj:334-350): a new required attr
/// on a namespace that already has entities is refused up front.
pub async fn validate_add_required<'e, E: PgExecutor<'e>>(
    exec: E,
    app_id: Uuid,
    attr: &Attr,
) -> Result<()> {
    let row = sqlx::query(
        "SELECT 1 AS x FROM attrs JOIN triples ON attrs.id = triples.attr_id
         WHERE attrs.app_id = $1 AND attrs.etype = $2 AND triples.app_id = $1 LIMIT 1",
    )
    .bind(app_id)
    .bind(&attr.etype)
    .fetch_optional(exec)
    .await?;
    if row.is_some() {
        let m = format!(
            "Can't create attribute `{}` as required because `{}` already have entities",
            attr.label, attr.etype
        );
        return Err(InstantError::validation_failed(
            "attributes",
            m.clone(),
            json!([{"message": m}]),
        ));
    }
    Ok(())
}

/// Legacy `validate-update-required!` (attr.clj:533-580): flipping an attr
/// to required needs every entity of the namespace to carry a non-null value.
pub async fn validate_update_required(
    conn: &mut sqlx::PgConnection,
    app_id: Uuid,
    attrs: &AttrMap,
    attr_ids: &[Uuid],
) -> Result<()> {
    for id in attr_ids {
        let Some(attr) = attrs.get(id) else { continue };
        if !attr.is_required {
            continue;
        }
        let row = sqlx::query(
            "SELECT
               (SELECT count(DISTINCT entity_id) FROM triples
                 WHERE app_id = $1 AND attr_id = $2 AND value IS NOT NULL AND value <> 'null') AS attr_count,
               (SELECT count(DISTINCT entity_id) FROM triples
                 WHERE app_id = $1
                   AND attr_id IN (SELECT id FROM attrs WHERE app_id = $1 AND etype = $3)
                   AND value IS NOT NULL AND value <> 'null') AS etype_count",
        )
        .bind(app_id)
        .bind(id)
        .bind(&attr.etype)
        .fetch_one(&mut *conn)
        .await?;
        let attr_count: i64 = row.get("attr_count");
        let etype_count: i64 = row.get("etype_count");
        if attr_count != etype_count {
            let m = format!(
                "Can't update attribute `{}` to required because `{}` already have entities without it",
                attr.label, attr.etype
            );
            return Err(InstantError::validation_failed(
                "attributes",
                m.clone(),
                json!([{"message": m}]),
            ));
        }
    }
    Ok(())
}

/// Insert one attr (attrs row + ident rows). Idempotent for identical re-inserts.
pub async fn insert<'e, E: PgExecutor<'e>>(exec: E, app_id: Uuid, attr: &Attr) -> Result<()> {
    validate_ident_names(app_id, attr)?;
    let res = sqlx::query(
        r#"
        WITH fwd AS (
          INSERT INTO idents (id, app_id, attr_id, etype, label)
          VALUES ($1, $2, $3, $4, $5)
          ON CONFLICT DO NOTHING
        ), rev AS (
          INSERT INTO idents (id, app_id, attr_id, etype, label)
          SELECT $6, $2, $3, $7, $8 WHERE $6 IS NOT NULL
          ON CONFLICT DO NOTHING
        )
        INSERT INTO attrs (id, app_id, value_type, cardinality, is_unique, is_indexed,
                           forward_ident, reverse_ident, is_required, checked_data_type,
                           on_delete, on_delete_reverse, etype, label, reverse_etype, reverse_label)
        VALUES ($3, $2, $9, $10, $11, $12, $1, $6, $13, $14::checked_data_type,
                CASE WHEN $15 THEN 'cascade'::attr_on_delete END,
                CASE WHEN $16 THEN 'cascade'::attr_on_delete END,
                $4, $5, $7, $8)
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .bind(attr.forward_ident)
    .bind(app_id)
    .bind(attr.id)
    .bind(&attr.etype)
    .bind(&attr.label)
    .bind(attr.reverse_ident)
    .bind(&attr.reverse_etype)
    .bind(&attr.reverse_label)
    .bind(attr.value_type.as_str())
    .bind(attr.cardinality.as_str())
    .bind(attr.is_unique)
    .bind(attr.is_indexed)
    .bind(attr.is_required)
    .bind(attr.checked_data_type.map(|c| c.as_str()))
    .bind(attr.on_delete_cascade)
    .bind(attr.on_delete_reverse_cascade)
    .execute(exec)
    .await;

    match res {
        Ok(_) => Ok(()),
        Err(sqlx::Error::Database(db)) if db.code().as_deref() == Some("23505") => {
            Err(InstantError::new(
                "record-not-unique",
                400,
                format!("`{}` already exists on `{}`", attr.label, attr.etype),
                Some(json!({
                    "record-type": "ident",
                    "etype": attr.etype,
                    "label": attr.label,
                })),
            ))
        }
        Err(e) => Err(e.into()),
    }
}

/// Update an attr in place (rename identities, toggle unique/index/cardinality,
/// on-delete). Rewrites derived triple flags for existing rows.
pub async fn update<'e, E: PgExecutor<'e>>(
    exec: E,
    app_id: Uuid,
    existing: &Attr,
    patch: &Value,
) -> Result<Attr> {
    let mut updated = existing.clone();
    if let Some(fwd) = patch.get("forward-identity").and_then(|v| v.as_array()) {
        if fwd.len() >= 3 {
            updated.etype = fwd[1].as_str().unwrap_or(&updated.etype).to_string();
            updated.label = fwd[2].as_str().unwrap_or(&updated.label).to_string();
        }
    }
    if let Some(rev) = patch.get("reverse-identity").and_then(|v| v.as_array()) {
        if rev.len() >= 3 {
            updated.reverse_etype = rev[1].as_str().map(|s| s.to_string());
            updated.reverse_label = rev[2].as_str().map(|s| s.to_string());
        }
    }
    if let Some(u) = patch.get("unique?").and_then(|v| v.as_bool()) {
        updated.is_unique = u;
    }
    if let Some(i) = patch.get("index?").and_then(|v| v.as_bool()) {
        updated.is_indexed = i;
    }
    if let Some(r) = patch.get("required?").and_then(|v| v.as_bool()) {
        updated.is_required = r;
    }
    if let Some(c) = patch.get("cardinality").and_then(|v| v.as_str()) {
        updated.cardinality = Cardinality::parse(c)?;
    }
    if let Some(cdt) = patch.get("checked-data-type") {
        updated.checked_data_type = match cdt.as_str() {
            Some(s) => Some(CheckedDataType::parse(s)?),
            None => None,
        };
    }
    if let Some(od) = patch.get("on-delete") {
        updated.on_delete_cascade = od.as_str() == Some("cascade");
    }
    if let Some(od) = patch.get("on-delete-reverse") {
        updated.on_delete_reverse_cascade = od.as_str() == Some("cascade");
    }
    // legacy update-multi! runs validate-system-ident-names! (attr.clj:585)
    validate_ident_names(app_id, &updated)?;

    sqlx::query(
        r#"
        WITH attr_up AS (
          UPDATE attrs SET value_type = $3, cardinality = $4, is_unique = $5, is_indexed = $6,
                 is_required = $7, checked_data_type = $8::checked_data_type,
                 on_delete = CASE WHEN $9 THEN 'cascade'::attr_on_delete END,
                 on_delete_reverse = CASE WHEN $10 THEN 'cascade'::attr_on_delete END,
                 etype = $11, label = $12, reverse_etype = $13, reverse_label = $14
          WHERE app_id = $1 AND id = $2
        ), fwd_up AS (
          UPDATE idents SET etype = $11, label = $12
          WHERE app_id = $1 AND attr_id = $2 AND id = $15
        ), rev_up AS (
          UPDATE idents SET etype = $13, label = $14
          WHERE app_id = $1 AND attr_id = $2 AND id = $16 AND $16 IS NOT NULL
        )
        UPDATE triples SET ea = $17, eav = $18, av = $19, ave = $20, vae = $21,
               checked_data_type = $8::checked_data_type
        WHERE app_id = $1 AND attr_id = $2
          AND (ea IS DISTINCT FROM $17 OR eav IS DISTINCT FROM $18 OR av IS DISTINCT FROM $19
               OR ave IS DISTINCT FROM $20 OR vae IS DISTINCT FROM $21
               OR checked_data_type IS DISTINCT FROM $8::checked_data_type)
        "#,
    )
    .bind(app_id)
    .bind(updated.id)
    .bind(updated.value_type.as_str())
    .bind(updated.cardinality.as_str())
    .bind(updated.is_unique)
    .bind(updated.is_indexed)
    .bind(updated.is_required)
    .bind(updated.checked_data_type.map(|c| c.as_str()))
    .bind(updated.on_delete_cascade)
    .bind(updated.on_delete_reverse_cascade)
    .bind(&updated.etype)
    .bind(&updated.label)
    .bind(&updated.reverse_etype)
    .bind(&updated.reverse_label)
    .bind(updated.forward_ident)
    .bind(updated.reverse_ident)
    .bind(updated.flags().ea)
    .bind(updated.flags().eav)
    .bind(updated.flags().av)
    .bind(updated.flags().ave)
    .bind(updated.flags().vae)
    .execute(exec)
    .await?;
    Ok(updated)
}

/// Soft-delete an attr: mark deletion, brand names to free them, snapshot state.
/// Legacy `soft-delete-multi!` (attr.clj:728-824): brand etype/label (and
/// the reverse names) with `{id}_deleted$`, drop index/required, and merge a
/// `soft_delete_snapshot` ({is_indexed, is_required, id_attr_id}) into
/// metadata — the dashboard's "recently deleted" view groups deleted attrs
/// by `id_attr_id`. Triples are left in place; `restore` reverses this.
pub async fn soft_delete<'e, E: PgExecutor<'e>>(exec: E, app_id: Uuid, attr: &Attr) -> Result<()> {
    sqlx::query(
        r#"
        WITH target_attrs AS (
          SELECT app_id, id, etype FROM attrs
           WHERE app_id = $1 AND id = $2 AND deletion_marked_at IS NULL
        ), snaps AS (
          SELECT t.app_id AS target_app_id, t.id AS target_attr_id, id_attr.id AS id_attr_id
            FROM target_attrs t
            LEFT JOIN LATERAL (
              SELECT id FROM attrs id_attr
               WHERE (id_attr.app_id = t.app_id OR id_attr.app_id = $3)
                 AND id_attr.etype = t.etype AND id_attr.label = 'id'
               LIMIT 1
            ) id_attr ON true
        ), soft_deleted_attrs AS (
          UPDATE attrs a
             SET deletion_marked_at = now(),
                 is_indexed = false,
                 is_required = false,
                 etype = id::text || '_deleted$' || etype,
                 label = id::text || '_deleted$' || label,
                 metadata = coalesce(metadata, '{}'::jsonb) || jsonb_build_object(
                   'soft_delete_snapshot', jsonb_build_object(
                     'is_indexed', is_indexed,
                     'is_required', is_required,
                     'id_attr_id', s.id_attr_id)),
                 reverse_etype = CASE WHEN reverse_etype IS NOT NULL
                                      THEN id::text || '_deleted$' || reverse_etype ELSE NULL END,
                 reverse_label = CASE WHEN reverse_label IS NOT NULL
                                      THEN id::text || '_deleted$' || reverse_label ELSE NULL END
            FROM snaps s
           WHERE a.app_id = s.target_app_id AND a.id = s.target_attr_id
             AND a.deletion_marked_at IS NULL
          RETURNING a.*
        ), changed_forward_idents AS (
          UPDATE idents fw SET etype = a.etype, label = a.label
            FROM soft_deleted_attrs a
           WHERE fw.app_id = $1 AND fw.id = a.forward_ident
        ), changed_rev_idents AS (
          UPDATE idents rv SET etype = a.reverse_etype, label = a.reverse_label
            FROM soft_deleted_attrs a
           WHERE rv.app_id = $1 AND rv.id = a.reverse_ident
        )
        SELECT count(*) FROM soft_deleted_attrs
        "#,
    )
    .bind(app_id)
    .bind(attr.id)
    .bind(system_catalog::SYSTEM_CATALOG_APP_ID)
    .execute(exec)
    .await?;
    Ok(())
}
