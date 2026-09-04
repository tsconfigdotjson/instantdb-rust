//! Triple write primitives. Port of LEGACY db/model/triple.clj semantics:
//! cardinality-one upsert, many-cardinality add, lookup refs, deep-merge,
//! delete-entity with cascade, indexed-null backfill.

use std::collections::{HashMap, HashSet};

use serde_json::{json, Value};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::attr::{Attr, AttrMap, ValueType};
use crate::error::{InstantError, Result};

/// Entity reference in a tx-step: a concrete uuid or a lookup ref
/// `[unique-attr-id, value]`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EidRef {
    Id(Uuid),
    Lookup(Uuid, CanonicalValue),
}

/// serde_json::Value wrapper with Eq/Hash via canonical text, so lookups can
/// key hash maps.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CanonicalValue(pub String);

impl CanonicalValue {
    pub fn of(v: &Value) -> Self {
        CanonicalValue(v.to_string())
    }
    pub fn value(&self) -> Value {
        serde_json::from_str(&self.0).unwrap_or(Value::Null)
    }
}

pub fn parse_eid(v: &Value) -> Result<EidRef> {
    match v {
        Value::String(s) => Uuid::parse_str(s).map(EidRef::Id).map_err(|_| {
            InstantError::validation_failed(
                "tx-steps",
                format!("expected a uuid or lookup ref, got {s:?}"),
                json!([]),
            )
        }),
        Value::Array(arr) if arr.len() == 2 => {
            let attr_id = arr[0]
                .as_str()
                .and_then(|s| Uuid::parse_str(s).ok())
                .ok_or_else(|| {
                    InstantError::validation_failed(
                        "tx-steps",
                        "lookup ref must be [attr-uuid, value]",
                        json!([]),
                    )
                })?;
            Ok(EidRef::Lookup(attr_id, CanonicalValue::of(&arr[1])))
        }
        other => Err(InstantError::validation_failed(
            "tx-steps",
            format!("expected a uuid or lookup ref, got {other}"),
            json!([]),
        )),
    }
}

/// Is this triple value a lookup ref? (2-array whose first element is a uuid
/// string, used as the value of a ref attr.)
pub fn value_lookup(v: &Value) -> Option<(Uuid, CanonicalValue)> {
    if let Value::Array(arr) = v {
        if arr.len() == 2 {
            if let Some(id) = arr[0].as_str().and_then(|s| Uuid::parse_str(s).ok()) {
                return Some((id, CanonicalValue::of(&arr[1])));
            }
        }
    }
    None
}

pub const JSON_NULL_MD5: &str = "37a6259cc0c1dae299a7866489dff0bd";

/// Resolves lookup refs to entity ids. `create_missing`: upsert semantics for
/// eid-position lookups in add/merge steps.
pub struct LookupResolver {
    pub resolved: HashMap<(Uuid, CanonicalValue), Uuid>,
    /// entities created by lookup upserts in this tx
    pub created: HashSet<Uuid>,
    /// legacy validates lookup namespaces only on the non-admin path
    /// (permissioned_transaction.clj:683-687); admins fall through to the
    /// plain "does not exist" lookup miss
    pub validate_namespaces: bool,
}

impl Default for LookupResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl LookupResolver {
    pub fn new() -> Self {
        LookupResolver {
            resolved: HashMap::new(),
            created: HashSet::new(),
            validate_namespaces: false,
        }
    }

    fn validate_lookup_attr(attrs: &AttrMap, attr_id: &Uuid) -> Result<Attr> {
        // legacy resolves lookup attrs via a unique-only join and RAISEs when
        // the attr is missing or not unique (triple.clj hsql-attr-id-or-raise;
        // verified live by scripts/differential/replay.mjs step 16)
        let attr = attrs.get(attr_id).filter(|a| a.is_unique).ok_or_else(|| {
            InstantError::sql_raise(format!(
                "We could not find an attribute with id = '{attr_id}'"
            ))
        })?;
        Ok(attr.clone())
    }

    /// Resolve one lookup, optionally creating the entity (with its lookup
    /// triple + id triple) when missing.
    pub async fn resolve(
        &mut self,
        conn: &mut PgConnection,
        app_id: Uuid,
        attrs: &AttrMap,
        attr_id: Uuid,
        value: &CanonicalValue,
        create_missing: bool,
    ) -> Result<Option<Uuid>> {
        let key = (attr_id, value.clone());
        if let Some(eid) = self.resolved.get(&key) {
            return Ok(Some(*eid));
        }
        let attr = Self::validate_lookup_attr(attrs, &attr_id)?;
        let row = sqlx::query(
            "SELECT entity_id FROM triples
             WHERE app_id = $1 AND attr_id = $2 AND av
               AND json_null_to_null(value) IS NOT DISTINCT FROM json_null_to_null($3::jsonb)",
        )
        .bind(app_id)
        .bind(attr_id)
        .bind(&value.0)
        .fetch_optional(&mut *conn)
        .await?;
        if let Some(row) = row {
            let eid: Uuid = row.get("entity_id");
            self.resolved.insert(key, eid);
            return Ok(Some(eid));
        }
        if !create_missing {
            return Ok(None);
        }
        // Create the entity: id = the value itself when the lookup attr is the
        // id attr, else a fresh uuid.
        let eid = if attr.label == "id" {
            value
                .value()
                .as_str()
                .and_then(|s| Uuid::parse_str(s).ok())
                .ok_or_else(|| {
                    InstantError::validation_failed(
                        "lookup",
                        "id lookup value must be a uuid",
                        json!([]),
                    )
                })?
        } else {
            Uuid::new_v4()
        };
        let flags = attr.flags();
        sqlx::query(
            "INSERT INTO triples (app_id, entity_id, attr_id, value, value_md5,
                                  ea, eav, av, ave, vae, checked_data_type)
             VALUES ($1, $2, $3, $4::jsonb, md5(($4::jsonb)::text),
                     $5, $6, $7, $8, $9, $10::checked_data_type)",
        )
        .bind(app_id)
        .bind(eid)
        .bind(attr_id)
        .bind(&value.0)
        .bind(flags.ea)
        .bind(flags.eav)
        .bind(flags.av)
        .bind(flags.ave)
        .bind(flags.vae)
        .bind(attr.checked_data_type.map(|c| c.as_str()))
        .execute(&mut *conn)
        .await
        .map_err(|e| translate_unique_violation(e, attrs))?;
        self.resolved.insert(key, eid);
        self.created.insert(eid);
        Ok(Some(eid))
    }
}

pub fn translate_unique_violation(e: sqlx::Error, attrs: &AttrMap) -> InstantError {
    if let sqlx::Error::Database(db) = &e {
        if db.code().as_deref() == Some("23505") {
            let detail = db.message().to_string();
            // Try to find which attr from the constraint detail; fall back generic.
            for attr in attrs.iter() {
                if detail.contains(&attr.id.to_string()) {
                    return InstantError::record_not_unique(
                        &attr.etype,
                        &attr.label,
                        Some(&attr.id.to_string()),
                        None,
                    );
                }
            }
            return InstantError::new(
                "record-not-unique",
                400,
                "Record not unique",
                Some(json!({"record-type": "triples"})),
            );
        }
        if db.code().as_deref() == Some("23514") {
            let msg = db.message().to_string();
            if msg.contains("valid_value_data_type") {
                return InstantError::new(
                    "validation-failed",
                    400,
                    "Invalid value type for a type-checked attribute.",
                    Some(json!({"data-type": "triples"})),
                );
            }
            if msg.contains("indexed_values_are_constrained") {
                return InstantError::new(
                    "validation-failed",
                    400,
                    "Value is too large for a unique/indexed attribute (max 1024 bytes).",
                    Some(json!({"data-type": "triples"})),
                );
            }
            if msg.contains("valid_ref_value") {
                return InstantError::new(
                    "validation-failed",
                    400,
                    "Link values must be valid uuids.",
                    Some(json!({"data-type": "triples"})),
                );
            }
        }
    }
    e.into()
}

/// One resolved triple ready for insert.
#[derive(Debug, Clone)]
pub struct ResolvedTriple {
    pub entity_id: Uuid,
    pub attr: Attr,
    pub value: Value,
}

/// Parallel columns for a multi-row triple insert (UNNEST binding).
struct InsertCols {
    entity_ids: Vec<Uuid>,
    attr_ids: Vec<Uuid>,
    values: Vec<String>,
    ea: Vec<bool>,
    eav: Vec<bool>,
    av: Vec<bool>,
    ave: Vec<bool>,
    vae: Vec<bool>,
    cdt: Vec<Option<String>>,
}

impl InsertCols {
    fn with_capacity(n: usize) -> InsertCols {
        InsertCols {
            entity_ids: Vec::with_capacity(n),
            attr_ids: Vec::with_capacity(n),
            values: Vec::with_capacity(n),
            ea: Vec::with_capacity(n),
            eav: Vec::with_capacity(n),
            av: Vec::with_capacity(n),
            ave: Vec::with_capacity(n),
            vae: Vec::with_capacity(n),
            cdt: Vec::with_capacity(n),
        }
    }
    fn push(&mut self, t: &ResolvedTriple) {
        let flags = t.attr.flags();
        self.entity_ids.push(t.entity_id);
        self.attr_ids.push(t.attr.id);
        self.values.push(t.value.to_string());
        self.ea.push(flags.ea);
        self.eav.push(flags.eav);
        self.av.push(flags.av);
        self.ave.push(flags.ave);
        self.vae.push(flags.vae);
        self.cdt
            .push(t.attr.checked_data_type.map(|c| c.as_str().to_string()));
    }
}

/// Insert a batch of add-triple writes. Returns the set of entity ids that were
/// newly created (their id triple inserted fresh).
pub async fn insert_triples(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &AttrMap,
    triples: &[ResolvedTriple],
) -> Result<HashSet<Uuid>> {
    let mut created: HashSet<Uuid> = HashSet::new();

    // Split ea (cardinality-one upsert) from non-ea; dedupe ea keeping the last
    // write per (entity, attr).
    let mut ea_last: HashMap<(Uuid, Uuid), &ResolvedTriple> = HashMap::new();
    let mut ea_order: Vec<(Uuid, Uuid)> = vec![];
    let mut non_ea: Vec<&ResolvedTriple> = vec![];
    for t in triples {
        if t.attr.flags().ea {
            let key = (t.entity_id, t.attr.id);
            if ea_last.insert(key, t).is_none() {
                ea_order.push(key);
            }
        } else {
            non_ea.push(t);
        }
    }

    // One multi-row statement per group (UNNEST over parallel arrays): a
    // 300-triple seed is two round trips instead of 300, and the
    // change-capture trigger fires once per statement instead of per row.
    if !ea_order.is_empty() {
        let mut cols = InsertCols::with_capacity(ea_order.len());
        for key in &ea_order {
            cols.push(ea_last[key]);
        }
        let rows = sqlx::query(
            r#"
            INSERT INTO triples (app_id, entity_id, attr_id, value, value_md5,
                                 ea, eav, av, ave, vae, checked_data_type)
            SELECT $1, e, a, v::jsonb, md5((v::jsonb)::text),
                   ea, eav, av, ave, vae, cdt::checked_data_type
            FROM UNNEST($2::uuid[], $3::uuid[], $4::text[], $5::bool[], $6::bool[],
                        $7::bool[], $8::bool[], $9::bool[], $10::text[])
                 AS t(e, a, v, ea, eav, av, ave, vae, cdt)
            ON CONFLICT (app_id, entity_id, attr_id) WHERE ea
            DO UPDATE SET value = excluded.value, value_md5 = excluded.value_md5,
                          checked_data_type = excluded.checked_data_type
            RETURNING entity_id, attr_id, (xmax = 0) AS inserted
            "#,
        )
        .bind(app_id)
        .bind(&cols.entity_ids)
        .bind(&cols.attr_ids)
        .bind(&cols.values)
        .bind(&cols.ea)
        .bind(&cols.eav)
        .bind(&cols.av)
        .bind(&cols.ave)
        .bind(&cols.vae)
        .bind(&cols.cdt)
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| translate_unique_violation(e, attrs))?;
        for row in rows {
            let inserted: bool = row.get("inserted");
            if !inserted {
                continue;
            }
            let (eid, aid): (Uuid, Uuid) = (row.get("entity_id"), row.get("attr_id"));
            if attrs.get(&aid).is_some_and(|a| a.label == "id") {
                created.insert(eid);
            }
        }
    }

    if !non_ea.is_empty() {
        // dedupe exact repeats so one statement never proposes a row twice
        let mut seen: HashSet<(Uuid, Uuid, String)> = HashSet::new();
        let mut cols = InsertCols::with_capacity(non_ea.len());
        for t in non_ea {
            if seen.insert((t.entity_id, t.attr.id, t.value.to_string())) {
                cols.push(t);
            }
        }
        sqlx::query(
            r#"
            INSERT INTO triples (app_id, entity_id, attr_id, value, value_md5,
                                 ea, eav, av, ave, vae, checked_data_type)
            SELECT $1, e, a, v::jsonb, md5((v::jsonb)::text),
                   ea, eav, av, ave, vae, cdt::checked_data_type
            FROM UNNEST($2::uuid[], $3::uuid[], $4::text[], $5::bool[], $6::bool[],
                        $7::bool[], $8::bool[], $9::bool[], $10::text[])
                 AS t(e, a, v, ea, eav, av, ave, vae, cdt)
            ON CONFLICT (app_id, entity_id, attr_id, value_md5) DO NOTHING
            "#,
        )
        .bind(app_id)
        .bind(&cols.entity_ids)
        .bind(&cols.attr_ids)
        .bind(&cols.values)
        .bind(&cols.ea)
        .bind(&cols.eav)
        .bind(&cols.av)
        .bind(&cols.ave)
        .bind(&cols.vae)
        .bind(&cols.cdt)
        .execute(&mut *conn)
        .await
        .map_err(|e| translate_unique_violation(e, attrs))?;
    }

    Ok(created)
}

/// Back-fill `'null'::jsonb` rows for indexed blob attrs of newly created
/// entities (so `ave` scans see nulls). Legacy: indexed-null-triples.
pub async fn backfill_indexed_nulls(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &AttrMap,
    new_entities: &[(Uuid, String)],
) -> Result<()> {
    let mut cols = InsertCols::with_capacity(new_entities.len());
    for (eid, etype) in new_entities {
        for attr in attrs.attrs_of_etype(etype) {
            // legacy indexed-null-triples joins attrs on the user's app id, so
            // system-catalog attrs ($users.email) are never null-backfilled
            if attr.value_type != ValueType::Blob
                || !attr.is_indexed
                || attr.label == "id"
                || attr.is_system
            {
                continue;
            }
            cols.push(&ResolvedTriple {
                entity_id: *eid,
                attr: attr.clone(),
                value: Value::Null,
            });
        }
    }
    if cols.entity_ids.is_empty() {
        return Ok(());
    }
    sqlx::query(
        r#"
        INSERT INTO triples (app_id, entity_id, attr_id, value, value_md5,
                             ea, eav, av, ave, vae, checked_data_type)
        SELECT $1, e, a, 'null'::jsonb, $11, ea, eav, av, ave, vae, cdt::checked_data_type
        FROM UNNEST($2::uuid[], $3::uuid[], $4::text[], $5::bool[], $6::bool[],
                    $7::bool[], $8::bool[], $9::bool[], $10::text[])
             AS t(e, a, v, ea, eav, av, ave, vae, cdt)
        ON CONFLICT (app_id, entity_id, attr_id) WHERE ea DO NOTHING
        "#,
    )
    .bind(app_id)
    .bind(&cols.entity_ids)
    .bind(&cols.attr_ids)
    .bind(&cols.values)
    .bind(&cols.ea)
    .bind(&cols.eav)
    .bind(&cols.av)
    .bind(&cols.ave)
    .bind(&cols.vae)
    .bind(&cols.cdt)
    .bind(JSON_NULL_MD5)
    .execute(&mut *conn)
    .await
    .map_err(|e| translate_unique_violation(e, attrs))?;
    Ok(())
}

/// Back-fill nulls for all existing entities of an etype when a new indexed
/// blob attr is created.
pub async fn backfill_nulls_for_new_attr(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &AttrMap,
    attr: &Attr,
) -> Result<()> {
    if attr.value_type != ValueType::Blob || !attr.is_indexed {
        return Ok(());
    }
    let id_attr = match attrs.id_attr_of(&attr.etype) {
        Some(a) => a,
        None => return Ok(()),
    };
    let flags = attr.flags();
    sqlx::query(
        r#"
        INSERT INTO triples (app_id, entity_id, attr_id, value, value_md5,
                             ea, eav, av, ave, vae, checked_data_type)
        SELECT $1, t.entity_id, $2, 'null'::jsonb, $3, $4, $5, $6, $7, $8, $9::checked_data_type
        FROM triples t
        WHERE t.app_id = $1 AND t.attr_id = $10 AND t.ea
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(app_id)
    .bind(attr.id)
    .bind(JSON_NULL_MD5)
    .bind(flags.ea)
    .bind(flags.eav)
    .bind(flags.av)
    .bind(flags.ave)
    .bind(flags.vae)
    .bind(attr.checked_data_type.map(|c| c.as_str()))
    .bind(id_attr.id)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Deep-merge patches into a cardinality-one blob triple.
/// JSON objects merge recursively; null deletes keys; other values replace.
/// Server-side merge, matching legacy `jsonb_deep_merge` byte-for-byte
/// (LEGACY migrations/24_deep_merge_null.up.sql, behavior verified against
/// the live legacy server by scripts/differential/fuzz.mjs):
/// - both sides objects: per-key — nested objects merge recursively, a null
///   patch value DELETES the key, anything else is set;
/// - a non-object on either side returns the patch VERBATIM (nulls kept);
/// - an empty-object patch leaves an object base unchanged.
pub fn deep_merge(base: &Value, patch: &Value) -> Value {
    match (base, patch) {
        (Value::Object(b), Value::Object(p)) => {
            let mut out = b.clone();
            for (k, pv) in p {
                if out.get(k).is_some_and(|bv| bv.is_object()) && pv.is_object() {
                    let merged = deep_merge(&out[k], pv);
                    out.insert(k.clone(), merged);
                } else if pv.is_null() {
                    out.remove(k);
                } else {
                    out.insert(k.clone(), pv.clone());
                }
            }
            Value::Object(out)
        }
        (_, p) => p.clone(),
    }
}

pub async fn deep_merge_triples(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &AttrMap,
    // (entity, attr, ordered patches)
    merges: &[(Uuid, Attr, Vec<Value>)],
) -> Result<HashSet<Uuid>> {
    let mut created = HashSet::new();
    for (eid, attr, patches) in merges {
        if attr.value_type == ValueType::Ref {
            // legacy RAISEs from SQL here (triple.clj:356)
            return Err(InstantError::sql_raise(
                "merge operation is not supported for links",
            ));
        }
        let existing = sqlx::query(
            "SELECT value FROM triples
             WHERE app_id = $1 AND entity_id = $2 AND attr_id = $3 AND ea FOR UPDATE",
        )
        .bind(app_id)
        .bind(eid)
        .bind(attr.id)
        .fetch_optional(&mut *conn)
        .await?;
        let base = existing
            .as_ref()
            .map(|r| r.get::<Value, _>("value"))
            .unwrap_or(Value::Null);
        let mut merged = base;
        for p in patches {
            merged = deep_merge(&merged, p);
        }
        let t = ResolvedTriple {
            entity_id: *eid,
            attr: attr.clone(),
            value: merged,
        };
        let newly = insert_triples(conn, app_id, attrs, &[t]).await?;
        created.extend(newly);
    }
    Ok(created)
}

/// Delete exact triples (retract-triple).
pub async fn delete_triples(
    conn: &mut PgConnection,
    app_id: Uuid,
    triples: &[(Uuid, Uuid, Value)],
) -> Result<()> {
    for (eid, attr_id, value) in triples {
        sqlx::query(
            "DELETE FROM triples
             WHERE app_id = $1 AND entity_id = $2 AND attr_id = $3
               AND value_md5 = md5(($4::jsonb)::text)",
        )
        .bind(app_id)
        .bind(eid)
        .bind(attr_id)
        .bind(value.to_string())
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// Expand delete-entity cascades: entities linked via attrs with
/// on_delete = cascade (children pointing at the deleted entity) and
/// on_delete_reverse = cascade (entities the deleted entity points to).
pub async fn expand_delete_cascade(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &AttrMap,
    seed: &[(Uuid, String)],
) -> Result<Vec<(Uuid, String)>> {
    let mut seen: HashSet<(Uuid, String)> = seed.iter().cloned().collect();
    let mut frontier: Vec<(Uuid, String)> = seed.to_vec();
    let mut out: Vec<(Uuid, String)> = seed.to_vec();

    while let Some((eid, etype)) = frontier.pop() {
        // on_delete = cascade: attrs whose reverse_etype = etype; the entities
        // that point AT eid (via vae) get deleted with their own (forward) etype.
        for attr in attrs.iter() {
            if attr.on_delete_cascade && attr.reverse_etype.as_deref() == Some(etype.as_str()) {
                let rows = sqlx::query(
                    "SELECT entity_id FROM triples
                     WHERE app_id = $1 AND attr_id = $2 AND vae
                       AND json_uuid_to_uuid(value) = $3",
                )
                .bind(app_id)
                .bind(attr.id)
                .bind(eid)
                .fetch_all(&mut *conn)
                .await?;
                for row in rows {
                    let child: Uuid = row.get("entity_id");
                    let key = (child, attr.etype.clone());
                    if seen.insert(key.clone()) {
                        frontier.push(key.clone());
                        out.push(key);
                    }
                }
            }
            // on_delete_reverse = cascade: deleting the forward-side entity
            // deletes the reverse-side entities it points to (via eav).
            if attr.on_delete_reverse_cascade && attr.etype == etype {
                let rows = sqlx::query(
                    "SELECT json_uuid_to_uuid(value) AS target FROM triples
                     WHERE app_id = $1 AND attr_id = $2 AND entity_id = $3 AND eav",
                )
                .bind(app_id)
                .bind(attr.id)
                .bind(eid)
                .fetch_all(&mut *conn)
                .await?;
                if let Some(retype) = &attr.reverse_etype {
                    for row in rows {
                        let target: Uuid = row.get("target");
                        let key = (target, retype.clone());
                        if seen.insert(key.clone()) {
                            frontier.push(key.clone());
                            out.push(key);
                        }
                    }
                }
            }
        }
    }
    Ok(out)
}

/// Delete all triples of the given entities (per etype) plus reverse-ref
/// triples pointing at them.
pub async fn delete_entities(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &AttrMap,
    entities: &[(Uuid, String)],
) -> Result<()> {
    for (eid, etype) in entities {
        let etype_attr_ids: Vec<Uuid> = attrs.attrs_of_etype(etype).map(|a| a.id).collect();
        sqlx::query(
            "DELETE FROM triples
             WHERE app_id = $1 AND entity_id = $2 AND attr_id = ANY($3)",
        )
        .bind(app_id)
        .bind(eid)
        .bind(&etype_attr_ids)
        .execute(&mut *conn)
        .await?;
        // Reverse refs: triples [other, ref-attr, eid] where the ref attr's
        // reverse etype is this etype.
        let rev_attr_ids: Vec<Uuid> = attrs
            .iter()
            .filter(|a| {
                a.value_type == ValueType::Ref && a.reverse_etype.as_deref() == Some(etype.as_str())
            })
            .map(|a| a.id)
            .collect();
        if !rev_attr_ids.is_empty() {
            sqlx::query(
                "DELETE FROM triples
                 WHERE app_id = $1 AND attr_id = ANY($2) AND vae
                   AND json_uuid_to_uuid(value) = $3",
            )
            .bind(app_id)
            .bind(&rev_attr_ids)
            .bind(eid)
            .execute(&mut *conn)
            .await?;
        }
        // Forward refs pointing away from a deleted entity whose attr belongs
        // to another etype are covered when that etype is in `entities`;
        // refs where this entity is the value are the rev_attr case above.
    }
    Ok(())
}

/// Resolve etypes for delete-entity steps missing an etype: find all etypes
/// with an id triple for this entity.
pub async fn resolve_etypes_for_delete(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &AttrMap,
    eid: Uuid,
) -> Result<Vec<String>> {
    let rows =
        sqlx::query("SELECT DISTINCT attr_id FROM triples WHERE app_id = $1 AND entity_id = $2")
            .bind(app_id)
            .bind(eid)
            .fetch_all(&mut *conn)
            .await?;
    let mut etypes = HashSet::new();
    for row in rows {
        let attr_id: Uuid = row.get("attr_id");
        if let Some(attr) = attrs.get(&attr_id) {
            etypes.insert(attr.etype.clone());
        }
    }
    Ok(etypes.into_iter().collect())
}

/// Check that every touched, still-existing entity has non-null values for all
/// required attrs of its etype.
pub async fn validate_required(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &AttrMap,
    touched: &[(Uuid, String)],
) -> Result<()> {
    for (eid, etype) in touched {
        let id_attr = match attrs.id_attr_of(etype) {
            Some(a) => a,
            None => continue,
        };
        // Entity may have been deleted later in the tx.
        let exists = sqlx::query(
            "SELECT 1 AS x FROM triples WHERE app_id = $1 AND entity_id = $2 AND attr_id = $3",
        )
        .bind(app_id)
        .bind(eid)
        .bind(id_attr.id)
        .fetch_optional(&mut *conn)
        .await?
        .is_some();
        if !exists {
            continue;
        }
        for attr in attrs.attrs_of_etype(etype) {
            if !attr.is_required || attr.label == "id" {
                continue;
            }
            let ok = sqlx::query(
                "SELECT 1 AS x FROM triples
                 WHERE app_id = $1 AND entity_id = $2 AND attr_id = $3
                   AND value != 'null'::jsonb",
            )
            .bind(app_id)
            .bind(eid)
            .bind(attr.id)
            .fetch_optional(&mut *conn)
            .await?
            .is_some();
            if !ok {
                return Err(InstantError::new(
                    "validation-failed",
                    400,
                    format!(
                        "Missing required attribute `{}/{}`: {}",
                        etype, attr.label, eid
                    ),
                    // legacy hint (triple.clj:227-235): the failing rows, no data-type
                    Some(
                        json!({"records": [{"entity_id": eid, "etype": etype, "label": attr.label}]}),
                    ),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deep_merge_matches_legacy_jsonb_deep_merge() {
        // nulls delete existing keys when both sides are objects
        let base = json!({"a": {"b": 1, "c": 2}, "d": 3});
        let patch = json!({"a": {"b": null, "e": 4}, "d": 5});
        assert_eq!(
            deep_merge(&base, &patch),
            json!({"a": {"c": 2, "e": 4}, "d": 5})
        );
        // a null patch value for a missing key stays absent
        assert_eq!(
            deep_merge(&json!({"k0": 3.5}), &json!({"k2": null})),
            json!({"k0": 3.5})
        );
        // non-object replaces
        assert_eq!(deep_merge(&json!({"a": 1}), &json!(7)), json!(7));
        // an empty-object patch leaves an object base unchanged
        assert_eq!(deep_merge(&json!({"a": 1}), &json!({})), json!({"a": 1}));
        // merging into null/missing keeps the patch VERBATIM — nulls included
        assert_eq!(
            deep_merge(&Value::Null, &json!({"a": 1, "b": null})),
            json!({"a": 1, "b": null})
        );
        // a null value deletes a whole nested object
        assert_eq!(
            deep_merge(&json!({"a": {"b": 2}}), &json!({"a": null})),
            json!({})
        );
    }
}
