//! tx-steps parsing and execution. Port of LEGACY db/transaction.clj.

use std::collections::{HashMap, HashSet};

use serde_json::{json, Value};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::attr::{Attr, AttrMap};
use crate::error::{InstantError, Result};
use crate::system_catalog;
use crate::triple::{
    backfill_indexed_nulls, backfill_nulls_for_new_attr, delete_entities, delete_triples,
    deep_merge_triples, expand_delete_cascade, insert_triples, parse_eid, resolve_etypes_for_delete,
    validate_required, value_lookup, CanonicalValue, EidRef, LookupResolver, ResolvedTriple,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    Create,
    Update,
    Upsert,
}

#[derive(Debug, Clone)]
pub enum TxStep {
    AddAttr(Attr),
    UpdateAttr(Value),
    DeleteAttr(Uuid),
    RestoreAttr(Uuid),
    AddTriple { eid: EidRef, attr_id: Uuid, value: Value, mode: WriteMode },
    DeepMergeTriple { eid: EidRef, attr_id: Uuid, value: Value, mode: WriteMode },
    RetractTriple { eid: EidRef, attr_id: Uuid, value: Value },
    DeleteEntity { eid: EidRef, etype: Option<String> },
    RuleParams { eid: EidRef, etype: Option<String>, params: Value },
}

impl TxStep {
    fn group_key(&self) -> &'static str {
        match self {
            TxStep::AddAttr(_) => "add-attr",
            TxStep::UpdateAttr(_) => "update-attr",
            TxStep::DeleteAttr(_) => "delete-attr",
            TxStep::RestoreAttr(_) => "restore-attr",
            TxStep::AddTriple { .. } => "add-triple",
            TxStep::DeepMergeTriple { .. } => "deep-merge-triple",
            TxStep::RetractTriple { .. } => "retract-triple",
            TxStep::DeleteEntity { .. } => "delete-entity",
            TxStep::RuleParams { .. } => "rule-params",
        }
    }
}

fn parse_mode(opts: Option<&Value>) -> WriteMode {
    match opts.and_then(|o| o.get("mode")).and_then(|m| m.as_str()) {
        Some("create") => WriteMode::Create,
        Some("update") => WriteMode::Update,
        _ => WriteMode::Upsert,
    }
}

fn parse_attr_uuid(v: Option<&Value>) -> Result<Uuid> {
    v.and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| {
            InstantError::validation_failed("tx-steps", "expected an attr uuid", json!([]))
        })
}

/// Parse the wire `tx-steps` array.
pub fn parse_tx_steps(steps: &Value) -> Result<Vec<TxStep>> {
    let arr = steps.as_array().ok_or_else(|| {
        InstantError::validation_failed("tx-steps", "tx-steps must be an array", json!([]))
    })?;
    let mut out = vec![];
    for step in arr {
        let step_arr = step.as_array().ok_or_else(|| {
            InstantError::validation_failed("tx-steps", "each tx-step must be an array", json!([]))
        })?;
        let op = step_arr.first().and_then(|v| v.as_str()).ok_or_else(|| {
            InstantError::validation_failed("tx-steps", "tx-step missing op", json!([]))
        })?;
        let parsed = match op {
            "add-attr" => TxStep::AddAttr(Attr::from_wire(step_arr.get(1).unwrap_or(&Value::Null))?),
            "update-attr" => {
                let patch = step_arr.get(1).cloned().unwrap_or(Value::Null);
                if !patch.is_object() {
                    return Err(InstantError::validation_failed(
                        "tx-steps",
                        "update-attr expects an object",
                        json!([]),
                    ));
                }
                TxStep::UpdateAttr(patch)
            }
            "delete-attr" => TxStep::DeleteAttr(parse_attr_uuid(step_arr.get(1))?),
            "restore-attr" => TxStep::RestoreAttr(parse_attr_uuid(step_arr.get(1))?),
            "add-triple" => TxStep::AddTriple {
                eid: parse_eid(step_arr.get(1).unwrap_or(&Value::Null))?,
                attr_id: parse_attr_uuid(step_arr.get(2))?,
                value: step_arr.get(3).cloned().unwrap_or(Value::Null),
                mode: parse_mode(step_arr.get(4)),
            },
            "deep-merge-triple" => TxStep::DeepMergeTriple {
                eid: parse_eid(step_arr.get(1).unwrap_or(&Value::Null))?,
                attr_id: parse_attr_uuid(step_arr.get(2))?,
                value: step_arr.get(3).cloned().unwrap_or(Value::Null),
                mode: parse_mode(step_arr.get(4)),
            },
            "retract-triple" => TxStep::RetractTriple {
                eid: parse_eid(step_arr.get(1).unwrap_or(&Value::Null))?,
                attr_id: parse_attr_uuid(step_arr.get(2))?,
                value: step_arr.get(3).cloned().unwrap_or(Value::Null),
            },
            "delete-entity" => TxStep::DeleteEntity {
                eid: parse_eid(step_arr.get(1).unwrap_or(&Value::Null))?,
                etype: step_arr.get(2).and_then(|v| v.as_str()).map(|s| s.to_string()),
            },
            "rule-params" => TxStep::RuleParams {
                eid: parse_eid(step_arr.get(1).unwrap_or(&Value::Null))?,
                etype: step_arr.get(2).and_then(|v| v.as_str()).map(|s| s.to_string()),
                params: step_arr.get(3).cloned().unwrap_or(Value::Null),
            },
            other => {
                return Err(InstantError::validation_failed(
                    "tx-steps",
                    format!("unknown tx-step op {other:?}"),
                    json!([]),
                ))
            }
        };
        out.push(parsed);
    }
    Ok(out)
}

/// Result of a committed transaction.
#[derive(Debug)]
pub struct TxReport {
    pub tx_id: i64,
    /// entities newly created (eid, etype)
    pub created: Vec<(Uuid, String)>,
    /// entities deleted (eid, etype), including cascades
    pub deleted: Vec<(Uuid, String)>,
    /// all touched (eid, etype), for perms/required checks
    pub touched: Vec<(Uuid, String)>,
    /// true when the tx changed the attr catalog
    pub attrs_changed: bool,
    /// rule-params per (eid, etype)
    pub rule_params: HashMap<(Uuid, String), Value>,
    /// resolved eids for lookup refs used in the tx
    pub resolved_lookups: HashMap<(Uuid, CanonicalValue), Uuid>,
}

pub struct TxOptions {
    /// bypass the system-catalog write guard (server-internal writes)
    pub allow_system_catalog_writes: bool,
}

impl Default for TxOptions {
    fn default() -> Self {
        TxOptions { allow_system_catalog_writes: false }
    }
}

/// Execute tx-steps inside the given open DB transaction. The caller commits.
/// `attrs` must be the app's current attr map; it is updated in place with
/// attr-level changes.
pub async fn transact(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &mut AttrMap,
    steps: Vec<TxStep>,
    opts: &TxOptions,
) -> Result<TxReport> {
    // First write: the transactions row (tx-id + WAL ordering anchor).
    let row = sqlx::query("INSERT INTO transactions (app_id) VALUES ($1) RETURNING id")
        .bind(app_id)
        .fetch_one(&mut *conn)
        .await?;
    let tx_id: i64 = row.get("id");
    // tags triple writes in this tx for the change-capture trigger (sync tables)
    sqlx::query("SELECT set_config('instant.rust_tx_id', $1, true)")
        .bind(tx_id.to_string())
        .execute(&mut *conn)
        .await?;

    // Guard system-catalog triple writes unless explicitly allowed.
    if !opts.allow_system_catalog_writes {
        for step in &steps {
            let attr_id = match step {
                TxStep::AddTriple { attr_id, .. }
                | TxStep::DeepMergeTriple { attr_id, .. }
                | TxStep::RetractTriple { attr_id, .. } => Some(*attr_id),
                _ => None,
            };
            if let Some(attr_id) = attr_id {
                if let Some(attr) = attrs.get(&attr_id) {
                    if attr.is_system
                        && !system_catalog::is_editable_triple_ident(&attr.etype, &attr.label)
                    {
                        return Err(InstantError::validation_failed(
                            "tx-steps",
                            format!(
                                "The attribute {}.{} is read-only and cannot be modified.",
                                attr.etype, attr.label
                            ),
                            json!([]),
                        ));
                    }
                }
            }
            if let TxStep::DeleteEntity { etype: Some(etype), .. } = step {
                if etype.starts_with('$') && !matches!(etype.as_str(), "$users" | "$files") {
                    return Err(InstantError::validation_failed(
                        "tx-steps",
                        format!("Cannot delete entities of system type {etype}"),
                        json!([]),
                    ));
                }
            }
        }
    }

    // Group steps by op, groups ordered by first appearance.
    let mut groups: Vec<(&'static str, Vec<TxStep>)> = vec![];
    for step in steps {
        let key = step.group_key();
        match groups.last_mut() {
            Some((k, v)) if *k == key => v.push(step),
            _ => {
                if let Some((_, v)) = groups.iter_mut().find(|(k, _)| *k == key) {
                    v.push(step);
                } else {
                    groups.push((key, vec![step]));
                }
            }
        }
    }

    let mut resolver = LookupResolver::new();
    let mut report = TxReport {
        tx_id,
        created: vec![],
        deleted: vec![],
        touched: vec![],
        attrs_changed: false,
        rule_params: HashMap::new(),
        resolved_lookups: HashMap::new(),
    };
    // eid -> etype for entities created in this tx (for null backfill)
    let mut created_etypes: HashMap<Uuid, String> = HashMap::new();

    for (op, group) in groups {
        match op {
            "add-attr" => {
                for step in group {
                    let TxStep::AddAttr(attr) = step else { unreachable!() };
                    // Ignore exact re-adds of attrs that already exist under the
                    // same forward name (client-generated ids may differ).
                    if let Some(existing) = attrs.by_fwd_name(&attr.etype, &attr.label) {
                        if existing.id != attr.id {
                            // Client didn't know the server id; treat as no-op.
                            continue;
                        }
                    }
                    crate::attr::insert(&mut *conn, app_id, &attr).await?;
                    backfill_nulls_for_new_attr(&mut *conn, app_id, attrs, &attr).await?;
                    attrs.insert(attr);
                    report.attrs_changed = true;
                }
            }
            "update-attr" => {
                for step in group {
                    let TxStep::UpdateAttr(patch) = step else { unreachable!() };
                    let id = parse_attr_uuid(patch.get("id"))?;
                    let existing = attrs.get(&id).cloned().ok_or_else(|| {
                        InstantError::record_not_found("attr", format!("attr {id} not found"))
                    })?;
                    if existing.is_system {
                        return Err(InstantError::validation_failed(
                            "attributes",
                            "System attributes cannot be modified.",
                            json!([]),
                        ));
                    }
                    let updated = crate::attr::update(&mut *conn, app_id, &existing, &patch).await?;
                    attrs.remove(&id);
                    attrs.insert(updated);
                    report.attrs_changed = true;
                }
            }
            "delete-attr" => {
                for step in group {
                    let TxStep::DeleteAttr(id) = step else { unreachable!() };
                    let existing = attrs.get(&id).cloned().ok_or_else(|| {
                        InstantError::record_not_found("attr", format!("attr {id} not found"))
                    })?;
                    if existing.is_system {
                        return Err(InstantError::validation_failed(
                            "attributes",
                            "System attributes cannot be deleted.",
                            json!([]),
                        ));
                    }
                    crate::attr::soft_delete(&mut *conn, app_id, &existing).await?;
                    attrs.remove(&id);
                    report.attrs_changed = true;
                }
            }
            "restore-attr" => {
                // Rarely used; treated as a no-op for now (restore of soft
                // deletes is an admin dashboard operation).
            }
            "add-triple" => {
                let mut items: Vec<(EidRef, Uuid, Value, WriteMode)> = vec![];
                for step in group {
                    let TxStep::AddTriple { eid, attr_id, value, mode } = step else {
                        unreachable!()
                    };
                    items.push((eid, attr_id, value, mode));
                }
                let resolved = resolve_add_batch(
                    &mut *conn,
                    app_id,
                    attrs,
                    &mut resolver,
                    &mut created_etypes,
                    items,
                )
                .await?;
                for t in &resolved {
                    report.touched.push((t.entity_id, t.attr.etype.clone()));
                }
                let newly = insert_triples(&mut *conn, app_id, attrs, &resolved).await?;
                for eid in newly {
                    if let Some(t) = resolved.iter().find(|t| t.entity_id == eid) {
                        created_etypes.insert(eid, t.attr.etype.clone());
                    }
                }
            }
            "deep-merge-triple" => {
                let mut merges: Vec<(Uuid, Attr, Vec<Value>)> = vec![];
                let mut order: HashMap<(Uuid, Uuid), usize> = HashMap::new();
                for step in group {
                    let TxStep::DeepMergeTriple { eid, attr_id, value, mode } = step else {
                        unreachable!()
                    };
                    let attr = attrs.get(&attr_id).cloned().ok_or_else(|| {
                        InstantError::record_not_found("attr", format!("attr {attr_id} not found"))
                    })?;
                    let eid = resolve_eid(
                        &mut *conn,
                        app_id,
                        attrs,
                        &mut resolver,
                        &mut created_etypes,
                        &eid,
                        mode != WriteMode::Update,
                        &attr.etype,
                    )
                    .await?;
                    validate_mode(&mut *conn, app_id, attrs, eid, &attr.etype, mode, &created_etypes)
                        .await?;
                    report.touched.push((eid, attr.etype.clone()));
                    let key = (eid, attr.id);
                    match order.get(&key) {
                        Some(&i) => merges[i].2.push(value),
                        None => {
                            order.insert(key, merges.len());
                            merges.push((eid, attr, vec![value]));
                        }
                    }
                }
                let newly = deep_merge_triples(&mut *conn, app_id, attrs, &merges).await?;
                for eid in newly {
                    if let Some((_, attr, _)) = merges.iter().find(|(e, _, _)| *e == eid) {
                        created_etypes.insert(eid, attr.etype.clone());
                    }
                }
            }
            "retract-triple" => {
                let mut dels: Vec<(Uuid, Uuid, Value)> = vec![];
                for step in group {
                    let TxStep::RetractTriple { eid, attr_id, value } = step else {
                        unreachable!()
                    };
                    let attr = attrs.get(&attr_id).cloned().ok_or_else(|| {
                        InstantError::record_not_found("attr", format!("attr {attr_id} not found"))
                    })?;
                    let eid = match eid {
                        EidRef::Id(id) => Some(id),
                        EidRef::Lookup(a, v) => {
                            resolver.resolve(&mut *conn, app_id, attrs, a, &v, false).await?
                        }
                    };
                    let Some(eid) = eid else { continue };
                    // Value may itself be a lookup ref (unlink by lookup).
                    let value = match value_lookup(&value) {
                        Some((a, v)) if attrs.get(&a).map(|x| x.is_unique).unwrap_or(false) => {
                            match resolver.resolve(&mut *conn, app_id, attrs, a, &v, false).await? {
                                Some(target) => json!(target),
                                None => continue,
                            }
                        }
                        _ => value,
                    };
                    report.touched.push((eid, attr.etype.clone()));
                    dels.push((eid, attr_id, value));
                }
                delete_triples(&mut *conn, app_id, &dels).await?;
            }
            "delete-entity" => {
                let mut seed: Vec<(Uuid, String)> = vec![];
                for step in group {
                    let TxStep::DeleteEntity { eid, etype } = step else { unreachable!() };
                    let eid = match eid {
                        EidRef::Id(id) => Some(id),
                        EidRef::Lookup(a, v) => {
                            resolver.resolve(&mut *conn, app_id, attrs, a, &v, false).await?
                        }
                    };
                    let Some(eid) = eid else { continue };
                    match etype {
                        Some(etype) => seed.push((eid, etype)),
                        None => {
                            for etype in
                                resolve_etypes_for_delete(&mut *conn, app_id, attrs, eid).await?
                            {
                                seed.push((eid, etype));
                            }
                        }
                    }
                }
                let expanded = expand_delete_cascade(&mut *conn, app_id, attrs, &seed).await?;
                delete_entities(&mut *conn, app_id, attrs, &expanded).await?;
                report.deleted.extend(expanded);
            }
            "rule-params" => {
                for step in group {
                    let TxStep::RuleParams { eid, etype, params } = step else { unreachable!() };
                    let eid = match eid {
                        EidRef::Id(id) => Some(id),
                        EidRef::Lookup(a, v) => {
                            resolver.resolve(&mut *conn, app_id, attrs, a, &v, false).await?
                        }
                    };
                    if let (Some(eid), Some(etype)) = (eid, etype) {
                        report.rule_params.insert((eid, etype), params);
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    let new_entities: Vec<(Uuid, String)> =
        created_etypes.iter().map(|(k, v)| (*k, v.clone())).collect();
    backfill_indexed_nulls(&mut *conn, app_id, attrs, &new_entities).await?;
    report.created = new_entities;

    let touched = report.touched.clone();
    validate_required(&mut *conn, app_id, attrs, &touched).await?;

    report.resolved_lookups = resolver.resolved.clone();
    Ok(report)
}

async fn resolve_eid(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &AttrMap,
    resolver: &mut LookupResolver,
    created_etypes: &mut HashMap<Uuid, String>,
    eid: &EidRef,
    create_missing: bool,
    _etype: &str,
) -> Result<Uuid> {
    match eid {
        EidRef::Id(id) => Ok(*id),
        EidRef::Lookup(attr_id, value) => {
            let lookup_attr_etype = attrs.get(attr_id).map(|a| a.etype.clone());
            let resolved = resolver
                .resolve(&mut *conn, app_id, attrs, *attr_id, value, create_missing)
                .await?;
            match resolved {
                Some(id) => {
                    if resolver.created.contains(&id) {
                        if let Some(et) = lookup_attr_etype {
                            created_etypes.entry(id).or_insert(et);
                        }
                    }
                    Ok(id)
                }
                None => Err(InstantError::validation_failed(
                    "lookup",
                    "The entity for the lookup does not exist.",
                    json!([{
                        "message": "The entity for the lookup does not exist.",
                        "attr-id": attr_id,
                        "value": value.value(),
                    }]),
                )),
            }
        }
    }
}

/// create-mode: entity must not already exist; update-mode: must exist.
async fn validate_mode(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &AttrMap,
    eid: Uuid,
    etype: &str,
    mode: WriteMode,
    created_etypes: &HashMap<Uuid, String>,
) -> Result<()> {
    if mode == WriteMode::Upsert {
        return Ok(());
    }
    let id_attr = match attrs.id_attr_of(etype) {
        Some(a) => a,
        None => return Ok(()),
    };
    let existed_before_tx = sqlx::query(
        "SELECT 1 AS x FROM triples WHERE app_id = $1 AND entity_id = $2 AND attr_id = $3",
    )
    .bind(app_id)
    .bind(eid)
    .bind(id_attr.id)
    .fetch_optional(&mut *conn)
    .await?
    .is_some()
        && !created_etypes.contains_key(&eid);
    match mode {
        WriteMode::Create if existed_before_tx => Err(InstantError::validation_failed(
            "tx-steps",
            format!("Creating entities that exist: {eid}"),
            json!([]),
        )),
        WriteMode::Update if !existed_before_tx && !created_etypes.contains_key(&eid) => {
            Err(InstantError::validation_failed(
                "tx-steps",
                format!("Updating entities that don't exist: {eid}"),
                json!([]),
            ))
        }
        _ => Ok(()),
    }
}

async fn resolve_add_batch(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &AttrMap,
    resolver: &mut LookupResolver,
    created_etypes: &mut HashMap<Uuid, String>,
    items: Vec<(EidRef, Uuid, Value, WriteMode)>,
) -> Result<Vec<ResolvedTriple>> {
    let mut out = vec![];
    let mut mode_checked: HashSet<(Uuid, String)> = HashSet::new();
    for (eid, attr_id, value, mode) in items {
        let attr = attrs.get(&attr_id).cloned().ok_or_else(|| {
            InstantError::record_not_found("attr", format!("attr {attr_id} not found"))
        })?;
        let eid = resolve_eid(
            &mut *conn,
            app_id,
            attrs,
            resolver,
            created_etypes,
            &eid,
            mode != WriteMode::Update,
            &attr.etype,
        )
        .await?;
        if mode != WriteMode::Upsert && mode_checked.insert((eid, attr.etype.clone())) {
            validate_mode(&mut *conn, app_id, attrs, eid, &attr.etype, mode, created_etypes).await?;
        }
        // Ref values (and id-triple values) may be lookup refs to resolve.
        let value = if attr.value_type == crate::attr::ValueType::Ref || attr.label == "id" {
            match value_lookup(&value) {
                Some((a, v)) => {
                    let target = resolver.resolve(&mut *conn, app_id, attrs, a, &v, true).await?;
                    match target {
                        Some(t) => {
                            if resolver.created.contains(&t) {
                                if let Some(la) = attrs.get(&a) {
                                    created_etypes.entry(t).or_insert(la.etype.clone());
                                }
                            }
                            json!(t)
                        }
                        None => {
                            return Err(InstantError::validation_failed(
                                "lookup",
                                "The entity for the lookup does not exist.",
                                json!([]),
                            ))
                        }
                    }
                }
                None => value,
            }
        } else {
            value
        };
        out.push(ResolvedTriple { entity_id: eid, attr, value });
    }
    Ok(out)
}

/// Check the app allows writes (status gate).
pub async fn assert_write_allowed(conn: &mut PgConnection, app_id: Uuid) -> Result<()> {
    let row = sqlx::query("SELECT status FROM apps WHERE id = $1")
        .bind(app_id)
        .fetch_optional(&mut *conn)
        .await?;
    let status: String = match row {
        Some(r) => r.get("status"),
        None => return Err(InstantError::record_not_found("app", "app not found")),
    };
    match status.as_str() {
        "active" => Ok(()),
        "read-only" => Err(InstantError::new(
            "validation-failed",
            400,
            "The app is read-only.",
            Some(json!({"status": "read-only"})),
        )),
        _ => Err(InstantError::new(
            "validation-failed",
            400,
            "The app is disabled.",
            Some(json!({"status": "disabled"})),
        )),
    }
}
