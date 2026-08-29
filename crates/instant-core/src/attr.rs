use std::collections::HashMap;

use serde_json::{json, Map, Value};
use sqlx::{PgExecutor, Row};
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
        m.insert("inferred-types".into(), Value::Null);
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
        Value::Object(m)
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
                InstantError::validation_failed(
                    "attributes",
                    "missing forward-identity",
                    json!([]),
                )
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
            obj.get("value-type").and_then(|v| v.as_str()).unwrap_or("blob"),
        )?;
        let cardinality = Cardinality::parse(
            obj.get("cardinality").and_then(|v| v.as_str()).unwrap_or("one"),
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
            is_unique: obj.get("unique?").and_then(|v| v.as_bool()).unwrap_or(false),
            is_indexed: obj.get("index?").and_then(|v| v.as_bool()).unwrap_or(false),
            is_required: obj.get("required?").and_then(|v| v.as_bool()).unwrap_or(false),
            checked_data_type: match obj.get("checked-data-type").and_then(|v| v.as_str()) {
                Some(s) => Some(CheckedDataType::parse(s)?),
                None => None,
            },
            on_delete_cascade: obj.get("on-delete").and_then(|v| v.as_str()) == Some("cascade"),
            on_delete_reverse_cascade: obj.get("on-delete-reverse").and_then(|v| v.as_str())
                == Some("cascade"),
            is_system: false,
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
            self.by_fwd.remove(&(attr.etype.clone(), attr.label.clone()));
            if let (Some(re), Some(rl)) = (&attr.reverse_etype, &attr.reverse_label) {
                self.by_rev.remove(&(re.clone(), rl.clone()));
            }
        }
    }

    pub fn get(&self, id: &Uuid) -> Option<&Attr> {
        self.by_id.get(id)
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
        let mut out = vec![];
        for attr in self.by_id.values() {
            if attr.is_system && !system_catalog::is_client_visible(&attr.etype, &attr.label) {
                continue;
            }
            out.push(attr.to_wire());
        }
        Value::Array(out)
    }
}

const ATTR_SELECT: &str = r#"
SELECT a.id, a.app_id, a.value_type, a.cardinality, a.is_unique, a.is_indexed,
       coalesce(a.is_required, false) AS is_required,
       a.forward_ident, a.reverse_ident,
       a.etype, a.label, a.reverse_etype, a.reverse_label,
       a.checked_data_type::text AS checked_data_type,
       (a.on_delete = 'cascade') AS on_delete_cascade,
       (a.on_delete_reverse = 'cascade') AS on_delete_reverse_cascade
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
        on_delete_cascade: row.get::<Option<bool>, _>("on_delete_cascade").unwrap_or(false),
        on_delete_reverse_cascade: row
            .get::<Option<bool>, _>("on_delete_reverse_cascade")
            .unwrap_or(false),
        is_system: app_id == system_catalog::SYSTEM_CATALOG_APP_ID,
    })
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

/// Insert one attr (attrs row + ident rows). Idempotent for identical re-inserts.
pub async fn insert<'e, E: PgExecutor<'e>>(exec: E, app_id: Uuid, attr: &Attr) -> Result<()> {
    // Reserved names: users can't claim system idents outside editable etypes.
    if app_id != system_catalog::SYSTEM_CATALOG_APP_ID
        && attr.etype.starts_with('$')
        && !system_catalog::is_editable_etype(&attr.etype)
    {
        return Err(InstantError::validation_failed(
            "attributes",
            format!("The `{}` namespace is reserved for system use.", attr.etype),
            json!([]),
        ));
    }
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
                    "record-type": "idents",
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
pub async fn soft_delete<'e, E: PgExecutor<'e>>(exec: E, app_id: Uuid, attr: &Attr) -> Result<()> {
    let brand = format!("{}_deleted$", attr.id);
    sqlx::query(
        r#"
        WITH ident_up AS (
          UPDATE idents SET etype = $3 || etype, label = $3 || label
          WHERE app_id = $1 AND attr_id = $2
        )
        UPDATE attrs SET deletion_marked_at = now(), is_indexed = false, is_required = false,
               etype = $3 || etype, label = $3 || label,
               reverse_etype = CASE WHEN reverse_etype IS NULL THEN NULL ELSE $3 || reverse_etype END,
               reverse_label = CASE WHEN reverse_label IS NULL THEN NULL ELSE $3 || reverse_label END,
               metadata = jsonb_build_object('soft_delete_snapshot',
                 jsonb_build_object('etype', etype, 'label', label,
                                    'reverse_etype', reverse_etype, 'reverse_label', reverse_label,
                                    'is_indexed', is_indexed, 'is_required', is_required))
        WHERE app_id = $1 AND id = $2
        "#,
    )
    .bind(app_id)
    .bind(attr.id)
    .bind(&brand)
    .execute(exec)
    .await?;
    Ok(())
}
