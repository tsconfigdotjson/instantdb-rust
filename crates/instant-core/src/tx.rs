//! tx-steps parsing and execution. Port of LEGACY db/transaction.clj.

use std::collections::{HashMap, HashSet};

use serde_json::{json, Value};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::attr::{Attr, AttrMap};
use crate::error::{InstantError, Result};
use crate::system_catalog;
use crate::triple::{
    backfill_indexed_nulls, backfill_nulls_for_new_attr, deep_merge_triples, delete_entities,
    delete_triples, expand_delete_cascade, insert_triples, parse_eid, resolve_etypes_for_delete,
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
    AddTriple {
        eid: EidRef,
        attr_id: Uuid,
        value: Value,
        mode: WriteMode,
    },
    DeepMergeTriple {
        eid: EidRef,
        attr_id: Uuid,
        value: Value,
        mode: WriteMode,
    },
    RetractTriple {
        eid: EidRef,
        attr_id: Uuid,
        value: Value,
    },
    DeleteEntity {
        eid: EidRef,
        etype: Option<String>,
    },
    RuleParams {
        eid: EidRef,
        etype: Option<String>,
        params: Value,
    },
}

impl TxStep {
    /// The step in legacy's `vectorize-tx-step` form (transaction.clj:106-124),
    /// which is what validation errors echo as their `input`.
    pub fn vectorize(&self) -> Value {
        let eid = |e: &EidRef| match e {
            EidRef::Id(id) => json!(id),
            EidRef::Lookup(attr, v) => json!([
                attr,
                serde_json::from_str::<Value>(&v.0).unwrap_or(Value::Null)
            ]),
        };
        let mode_opts = |m: &WriteMode| match m {
            WriteMode::Create => json!({"mode": "create"}),
            WriteMode::Update => json!({"mode": "update"}),
            WriteMode::Upsert => Value::Null,
        };
        match self {
            TxStep::AddAttr(a) => json!(["add-attr", a.to_wire()]),
            TxStep::UpdateAttr(v) => json!(["update-attr", v]),
            TxStep::DeleteAttr(id) => json!(["delete-attr", id]),
            TxStep::RestoreAttr(id) => json!(["restore-attr", id]),
            TxStep::AddTriple {
                eid: e,
                attr_id,
                value,
                mode,
            } => json!(["add-triple", eid(e), attr_id, value, mode_opts(mode)]),
            TxStep::DeepMergeTriple {
                eid: e,
                attr_id,
                value,
                mode,
            } => json!(["deep-merge-triple", eid(e), attr_id, value, mode_opts(mode)]),
            TxStep::RetractTriple {
                eid: e,
                attr_id,
                value,
            } => json!(["retract-triple", eid(e), attr_id, value, Value::Null]),
            TxStep::DeleteEntity { eid: e, etype } => json!(["delete-entity", eid(e), etype]),
            TxStep::RuleParams {
                eid: e,
                etype,
                params,
            } => json!(["rule-params", eid(e), etype, params]),
        }
    }

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

/// Legacy coerce-value-uuids (permissioned_transaction.clj:111-122): the
/// value of a ref attr must be a lookup ref or a uuid; anything else fails
/// with a `Validation failed for eid` error before any deeper processing.
fn check_ref_value(attrs: &AttrMap, attr_id: &Uuid, value: &Value) -> Result<()> {
    let Some(attr) = attrs.get(attr_id) else {
        return Ok(());
    };
    if attr.value_type != crate::attr::ValueType::Ref {
        return Ok(());
    }
    let ok = match value {
        Value::Array(_) => true, // lookup ref
        Value::String(s) => Uuid::parse_str(s).is_ok(),
        _ => false,
    };
    if ok {
        Ok(())
    } else {
        Err(InstantError::validation_failed(
            "eid",
            "Expected link value to be a uuid.",
            json!([{"message": "Expected link value to be a uuid."}]),
        ))
    }
}

/// Step-shape (spec-level) failure. Legacy's message for these is the bare
/// "Validation failed for tx-steps" — spec explain data lives in the hint
/// (util/exception.clj throw-validation-err! with coercion errors).
fn coerce_err(detail: impl Into<String>) -> InstantError {
    InstantError::new(
        "validation-failed",
        400,
        "Validation failed for tx-steps",
        Some(json!({"data-type": "tx-steps", "errors": [{"message": detail.into()}]})),
    )
}

/// Parse the wire `tx-steps` array.
pub fn parse_tx_steps(steps: &Value) -> Result<Vec<TxStep>> {
    let arr = steps
        .as_array()
        .ok_or_else(|| coerce_err("tx-steps must be an array"))?;
    let mut out = vec![];
    for step in arr {
        let step_arr = step
            .as_array()
            .ok_or_else(|| coerce_err("each tx-step must be an array"))?;
        let op = step_arr
            .first()
            .and_then(|v| v.as_str())
            .ok_or_else(|| coerce_err("tx-step missing op"))?;
        let parsed = match op {
            "add-attr" => {
                TxStep::AddAttr(Attr::from_wire(step_arr.get(1).unwrap_or(&Value::Null))?)
            }
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
                etype: step_arr
                    .get(2)
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
            },
            "rule-params" => TxStep::RuleParams {
                eid: parse_eid(step_arr.get(1).unwrap_or(&Value::Null))?,
                etype: step_arr
                    .get(2)
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                params: step_arr.get(3).cloned().unwrap_or(Value::Null),
            },
            other => return Err(coerce_err(format!("unknown tx-step op {other:?}"))),
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
    /// true when the tx changed the attr catalog in any way (flags, idents,
    /// inferred types): attr caches must be reloaded
    pub attrs_changed: bool,
    /// legacy `schema-changes-require-refreshing-sessions?`
    /// (reactive/invalidator.clj:45-63): attr inserts / deletes and ident
    /// changes refresh EVERY session of the app so they all see the new
    /// catalog; other attr updates (flags, inferred types) only reach
    /// sessions whose queries went stale anyway
    pub schema_changed: bool,
    /// attrs whose rows this tx wrote (added, updated, deleted, restored,
    /// inferred types): legacy derives a `[_ #{attr-id} _]` topic from every
    /// attrs-row change (reactive/topics.clj topics-for-attr-upsert), so any
    /// query mentioning the attr is refreshed
    pub changed_attrs: Vec<Uuid>,
    /// rule-params per (eid, etype)
    pub rule_params: HashMap<(Uuid, String), Value>,
    /// resolved eids for lookup refs used in the tx
    pub resolved_lookups: HashMap<(Uuid, CanonicalValue), Uuid>,
}

#[derive(Default)]
pub struct TxOptions {
    /// bypass the system-catalog write guard (server-internal writes)
    pub allow_system_catalog_writes: bool,
    /// admin transact: legacy `prevent-system-column-updates` lets admins
    /// write system columns (except `$files` / `$streams`) and delete any
    /// system entity
    pub admin: bool,
}

/// legacy `throw-tx-step-validation-err!` (permissioned_transaction.clj:38-42):
/// the offending step, vectorized, rides in `hint.input`.
fn tx_step_validation_err(step: &TxStep, message: String) -> InstantError {
    InstantError::new(
        "validation-failed",
        400,
        format!("Validation failed for tx-step: {message}"),
        Some(json!({
            "data-type": "tx-step",
            "input": step.vectorize(),
            "errors": [{"message": message}],
        })),
    )
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
    // ...and tag the triple writes of this tx for the change-capture trigger
    // (sync tables, topics) in the same round trip.
    let row = sqlx::query(
        "WITH t AS (INSERT INTO transactions (app_id) VALUES ($1) RETURNING id)
         SELECT id, set_config('instant.rust_tx_id', id::text, true) AS tag FROM t",
    )
    .bind(app_id)
    .fetch_one(&mut *conn)
    .await?;
    let tx_id: i64 = row.get("id");

    // legacy validate-system-triple-op! (permissioned_transaction.clj:72-79):
    // stream-backed file paths are never editable, admins included
    if !opts.allow_system_catalog_writes {
        for step in &steps {
            if let TxStep::AddTriple { attr_id, value, .. }
            | TxStep::DeepMergeTriple { attr_id, value, .. } = step
            {
                let is_path = attrs
                    .get(attr_id)
                    .map(|a| a.etype == "$files" && a.label == "path")
                    .unwrap_or(false);
                if is_path && value.as_str().is_some_and(|s| s.starts_with("$stream/")) {
                    return Err(tx_step_validation_err(
                        step,
                        "The path for stream files can't be edited.".to_string(),
                    ));
                }
            }
        }
    }
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
                    // legacy validate-system-triple-op! (permissioned_transaction.clj:49-71)
                    let editable =
                        system_catalog::is_editable_triple_ident(&attr.etype, &attr.label);
                    let admin_ok =
                        opts.admin && !matches!(attr.etype.as_str(), "$files" | "$streams");
                    if attr.is_system && !editable && !admin_ok {
                        let message = format!(
                            "{}.{} is a system column. You aren't allowed to change this directly.",
                            attr.etype, attr.label
                        );
                        return Err(tx_step_validation_err(step, message));
                    }
                }
            }
            if let TxStep::DeleteEntity {
                etype: Some(etype), ..
            } = step
            {
                // legacy validate-system-delete-entity!: only admins delete
                // system entities, except $files
                if etype.starts_with('$') && !(opts.admin || etype == "$files") {
                    let message = format!(
                        "{etype} is a system entity. You aren't allowed to delete this directly."
                    );
                    return Err(tx_step_validation_err(step, message));
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
    let mut required_updates: Vec<Uuid> = vec![];
    let mut report = TxReport {
        tx_id,
        created: vec![],
        deleted: vec![],
        touched: vec![],
        attrs_changed: false,
        schema_changed: false,
        changed_attrs: vec![],
        rule_params: HashMap::new(),
        resolved_lookups: HashMap::new(),
    };
    // eid -> etype for entities created in this tx (for null backfill)
    let mut created_etypes: HashMap<Uuid, String> = HashMap::new();

    for (op, group) in groups {
        match op {
            "add-attr" => {
                for step in group {
                    let TxStep::AddAttr(attr) = step else {
                        unreachable!()
                    };
                    // A second attr under an existing forward name trips
                    // legacy's `app_ident_uq` (exception.clj:227-240) and the
                    // client sees `record-not-unique`; a later add-triple with
                    // the client's id would fail anyway
                    if let Some(existing) = attrs.by_fwd_name(&attr.etype, &attr.label) {
                        if existing.id != attr.id {
                            return Err(InstantError::new(
                                "record-not-unique",
                                400,
                                format!("`{}` already exists on `{}`", attr.label, attr.etype),
                                Some(json!({
                                    "record-type": "idents",
                                    "etype": attr.etype,
                                    "label": attr.label,
                                })),
                            ));
                        }
                    }
                    // legacy validate-add-required! (attr.clj:334-350)
                    if attr.is_required {
                        crate::attr::validate_add_required(&mut *conn, app_id, &attr).await?;
                    }
                    crate::attr::insert(&mut *conn, app_id, &attr).await?;
                    backfill_nulls_for_new_attr(&mut *conn, app_id, attrs, &attr).await?;
                    report.changed_attrs.push(attr.id);
                    attrs.insert(attr);
                    report.attrs_changed = true;
                    report.schema_changed = true;
                }
            }
            "update-attr" => {
                for step in group {
                    let TxStep::UpdateAttr(patch) = step else {
                        unreachable!()
                    };
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
                    // legacy update-multi! only writes idents rows when the
                    // step carries an identity (ident-table-values), and
                    // only ident rows count as a schema change
                    let touches_idents = patch.get("forward-identity").is_some()
                        || patch.get("reverse-identity").is_some();
                    let updated =
                        crate::attr::update(&mut *conn, app_id, &existing, &patch).await?;
                    if updated.is_required && !existing.is_required {
                        required_updates.push(updated.id);
                    }
                    attrs.remove(&id);
                    attrs.insert(updated);
                    report.attrs_changed = true;
                    report.schema_changed |= touches_idents;
                    report.changed_attrs.push(id);
                }
            }
            "delete-attr" => {
                for step in group {
                    let TxStep::DeleteAttr(id) = step else {
                        unreachable!()
                    };
                    // Legacy soft-delete-multi! is a plain UPDATE ... WHERE id IN
                    // (...): an unknown (or already deleted) id is a no-op.
                    let Some(existing) = attrs.get(&id).cloned() else {
                        continue;
                    };
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
                    report.schema_changed = true; // idents are branded
                    report.changed_attrs.push(id);
                }
            }
            "restore-attr" => {
                // Legacy restore-multi! (attr.clj:657-726): un-brand the
                // soft-deleted attr and its idents, clear the snapshot, and
                // leave it un-indexed / not required. Unknown or live ids
                // match nothing (plain UPDATE), like delete-attr.
                let ids: Vec<Uuid> = group
                    .into_iter()
                    .map(|step| {
                        let TxStep::RestoreAttr(id) = step else {
                            unreachable!()
                        };
                        id
                    })
                    .collect();
                let restored = crate::attr::restore(&mut *conn, app_id, &ids).await?;
                for attr in restored {
                    report.changed_attrs.push(attr.id);
                    attrs.remove(&attr.id);
                    attrs.insert(attr);
                    report.attrs_changed = true;
                    report.schema_changed = true; // idents are un-branded
                }
            }
            "add-triple" => {
                let mut items: Vec<(EidRef, Uuid, Value, WriteMode)> = vec![];
                for step in group {
                    let TxStep::AddTriple {
                        eid,
                        attr_id,
                        value,
                        mode,
                    } = step
                    else {
                        unreachable!()
                    };
                    check_ref_value(attrs, &attr_id, &value)?;
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
                let inferred = resolved
                    .iter()
                    .filter_map(|t| Some((t.attr.id, crate::attr::inferred_type_bit(&t.value)?)))
                    .collect::<Vec<_>>();
                let changed =
                    crate::attr::record_inferred_types(&mut *conn, app_id, attrs, inferred).await?;
                if !changed.is_empty() {
                    report.attrs_changed = true;
                    report.changed_attrs.extend(changed);
                }
            }
            "deep-merge-triple" => {
                let mut merges: Vec<(Uuid, Attr, Vec<Value>)> = vec![];
                let mut order: HashMap<(Uuid, Uuid), usize> = HashMap::new();
                for step in group {
                    let TxStep::DeepMergeTriple {
                        eid,
                        attr_id,
                        value,
                        mode,
                    } = step
                    else {
                        unreachable!()
                    };
                    let attr = attrs.get(&attr_id).cloned().ok_or_else(|| {
                        InstantError::record_not_found("attr", format!("attr {attr_id} not found"))
                    })?;
                    check_ref_value(attrs, &attr_id, &value)?;
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
                    validate_mode(
                        &mut *conn,
                        app_id,
                        attrs,
                        eid,
                        &attr.etype,
                        mode,
                        &created_etypes,
                    )
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
                // legacy deep-merge-multi! infers from the PATCH values, not
                // the merged result (transaction_test.clj:4180-4210)
                let inferred = merges
                    .iter()
                    .flat_map(|(_, attr, vals)| {
                        vals.iter().filter_map(move |v| {
                            Some((attr.id, crate::attr::inferred_type_bit(v)?))
                        })
                    })
                    .collect::<Vec<_>>();
                let changed =
                    crate::attr::record_inferred_types(&mut *conn, app_id, attrs, inferred).await?;
                if !changed.is_empty() {
                    report.attrs_changed = true;
                    report.changed_attrs.extend(changed);
                }
            }
            "retract-triple" => {
                let mut dels: Vec<(Uuid, Uuid, Value)> = vec![];
                for step in group {
                    let TxStep::RetractTriple {
                        eid,
                        attr_id,
                        value,
                    } = step
                    else {
                        unreachable!()
                    };
                    let attr = attrs.get(&attr_id).cloned().ok_or_else(|| {
                        InstantError::record_not_found("attr", format!("attr {attr_id} not found"))
                    })?;
                    check_ref_value(attrs, &attr_id, &value)?;
                    let eid = match eid {
                        EidRef::Id(id) => Some(id),
                        EidRef::Lookup(a, v) => {
                            resolver
                                .resolve(&mut *conn, app_id, attrs, a, &v, false)
                                .await?
                        }
                    };
                    let Some(eid) = eid else { continue };
                    // Value may itself be a lookup ref (unlink by lookup).
                    let value = match value_lookup(&value) {
                        Some((a, v)) if attrs.get(&a).map(|x| x.is_unique).unwrap_or(false) => {
                            match resolver
                                .resolve(&mut *conn, app_id, attrs, a, &v, false)
                                .await?
                            {
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
                    let TxStep::DeleteEntity { eid, etype } = step else {
                        unreachable!()
                    };
                    let eid = match eid {
                        EidRef::Id(id) => Some(id),
                        EidRef::Lookup(a, v) => {
                            resolver
                                .resolve(&mut *conn, app_id, attrs, a, &v, false)
                                .await?
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
                let referrers = referrers_of(&mut *conn, app_id, attrs, &expanded).await?;
                delete_entities(&mut *conn, app_id, attrs, &expanded).await?;
                report.touched.extend(referrers);
                report.deleted.extend(expanded);
            }
            "rule-params" => {
                for step in group {
                    let TxStep::RuleParams { eid, etype, params } = step else {
                        unreachable!()
                    };
                    let eid = match eid {
                        EidRef::Id(id) => Some(id),
                        EidRef::Lookup(a, v) => {
                            resolver
                                .resolve(&mut *conn, app_id, attrs, a, &v, false)
                                .await?
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

    let new_entities: Vec<(Uuid, String)> = created_etypes
        .iter()
        .map(|(k, v)| (*k, v.clone()))
        .collect();
    backfill_indexed_nulls(&mut *conn, app_id, attrs, &new_entities).await?;
    report.created = new_entities;

    let touched = report.touched.clone();
    validate_required(&mut *conn, app_id, attrs, &touched).await?;
    // legacy validate-update-required! (attr.clj:533-580, transaction.clj:627)
    crate::attr::validate_update_required(&mut *conn, app_id, attrs, &required_updates).await?;
    // legacy validate-system-* create guard (permissioned_transaction.clj:589-596)
    if !opts.allow_system_catalog_writes && !opts.admin {
        if let Some((eid, _)) = report.created.iter().find(|(_, et)| et == "$users") {
            return Err(InstantError::validation_failed(
                "tx-step",
                "$users is a system entity. You aren't allowed to create this directly.",
                json!([{"message": "$users is a system entity. You aren't allowed to create this directly.", "eid": eid}]),
            ));
        }
    }

    report.resolved_lookups = resolver.resolved.clone();
    Ok(report)
}

#[allow(clippy::too_many_arguments)]
async fn resolve_eid(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &AttrMap,
    resolver: &mut LookupResolver,
    created_etypes: &mut HashMap<Uuid, String>,
    eid: &EidRef,
    create_missing: bool,
    etype: &str,
) -> Result<Uuid> {
    match eid {
        EidRef::Id(id) => Ok(*id),
        EidRef::Lookup(attr_id, value) => {
            let lookup_attr_etype = attrs.get(attr_id).map(|a| a.etype.clone());
            // legacy validate-lookup-etypes (permissioned_transaction.clj:124-140)
            match &lookup_attr_etype {
                None => {
                    let m = "Invalid lookup. Could not determine namespace from lookup attribute.";
                    return Err(InstantError::validation_failed(
                        "lookup",
                        m,
                        json!([{"message": m, "attr-id": attr_id, "value": value.value()}]),
                    ));
                }
                Some(le) if !etype.is_empty() && le != etype => {
                    let m = "Invalid transaction. The namespace in the lookup attribute is different from the namespace of the attribute that is being set";
                    return Err(InstantError::validation_failed(
                        "tx-step",
                        m,
                        json!([{"message": m}]),
                    ));
                }
                _ => {}
            }
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

/// Entities that link *to* any of `targets` (the reverse rows legacy's
/// delete returns, triple.clj:1188-1209), so a required link attr on the
/// referrer is re-validated after the delete.
async fn referrers_of(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &AttrMap,
    targets: &[(Uuid, String)],
) -> Result<Vec<(Uuid, String)>> {
    if targets.is_empty() {
        return Ok(vec![]);
    }
    let ids: Vec<Uuid> = targets.iter().map(|(e, _)| *e).collect();
    let rows = sqlx::query(
        "SELECT DISTINCT entity_id, attr_id FROM triples
         WHERE app_id = $1 AND vae AND json_uuid_to_uuid(value) = ANY($2)",
    )
    .bind(app_id)
    .bind(&ids)
    .fetch_all(&mut *conn)
    .await?;
    let mut out = vec![];
    for r in rows {
        let eid: Uuid = r.get("entity_id");
        let attr_id: Uuid = r.get("attr_id");
        if ids.contains(&eid) {
            continue;
        }
        if let Some(a) = attrs.get(&attr_id) {
            out.push((eid, a.etype.clone()));
        }
    }
    Ok(out)
}

/// Legacy `validate-value-lookup-etypes` (transaction.clj:532-556): a
/// value-position lookup on a ref attr must name an attr of the link's
/// reverse etype.
fn validate_value_lookup_etype(attrs: &AttrMap, attr: &Attr, lookup_attr: Uuid) -> Result<()> {
    let Some(rev_etype) = attr.reverse_etype.as_deref() else {
        return Ok(());
    };
    match attrs.get(&lookup_attr) {
        None => {
            let m = "Invalid lookup. Could not determine namespace from lookup attribute.";
            Err(InstantError::validation_failed(
                "lookup",
                m,
                json!([{"message": m, "attr-id": lookup_attr}]),
            ))
        }
        Some(la) if la.etype != rev_etype => {
            let m = "Invalid transaction. The namespace in the lookup attribute is different from the namespace of the attribute that is being set";
            Err(InstantError::validation_failed(
                "tx-step",
                m,
                json!([{"message": m}]),
            ))
        }
        _ => Ok(()),
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
    // legacy reports these under the singular `tx-step` input type
    // (LEGACY transaction.clj:346-358)
    match mode {
        WriteMode::Create if existed_before_tx => {
            let m = format!("Creating entities that exist: {eid}");
            Err(InstantError::validation_failed(
                "tx-step",
                m.clone(),
                json!([{"message": m}]),
            ))
        }
        WriteMode::Update if !existed_before_tx && !created_etypes.contains_key(&eid) => {
            let m = format!("Updating entities that don't exist: {eid}");
            Err(InstantError::validation_failed(
                "tx-step",
                m.clone(),
                json!([{"message": m}]),
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
            validate_mode(
                &mut *conn,
                app_id,
                attrs,
                eid,
                &attr.etype,
                mode,
                created_etypes,
            )
            .await?;
        }
        // Ref values (and id-triple values) may be lookup refs to resolve.
        let value = if attr.value_type == crate::attr::ValueType::Ref || attr.label == "id" {
            match value_lookup(&value) {
                Some((a, v)) => {
                    // legacy validate-value-lookup-etypes (transaction.clj:532-556)
                    validate_value_lookup_etype(attrs, &attr, a)?;
                    // a value-position lookup only resolves against existing
                    // entities (plus eid-position lookups earlier in this tx);
                    // legacy's `lookups` CTE raises `missing-lookup-value`
                    // otherwise (triple.clj:885-899) instead of forking a
                    // phantom entity
                    let target = resolver
                        .resolve(&mut *conn, app_id, attrs, a, &v, false)
                        .await?;
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
                            let m = "The entity for the lookup does not exist.";
                            return Err(InstantError::validation_failed(
                                "lookup",
                                m,
                                json!([{
                                    "message": m,
                                    "attribute-id": a,
                                    "namespace": attrs.get(&a).map(|la| la.etype.clone()),
                                    "label": attrs.get(&a).map(|la| la.label.clone()),
                                    "value": v.value(),
                                }]),
                            ));
                        }
                    }
                }
                None => value,
            }
        } else {
            value
        };
        out.push(ResolvedTriple {
            entity_id: eid,
            attr,
            value,
        });
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
    match write_gate_error(&status) {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Legacy `throw-app-read-only!` / `throw-app-disabled!` (util/exception.clj:331-339).
pub fn app_read_only_error() -> InstantError {
    InstantError::new(
        "app-read-only",
        400,
        "This app is in read-only mode.",
        Some(json!({"status": "read-only"})),
    )
}

pub fn app_disabled_error() -> InstantError {
    InstantError::new(
        "app-disabled",
        400,
        "This app is currently disabled.",
        Some(json!({"status": "disabled"})),
    )
}

/// Legacy `assert-write-allowed!` (model/app.clj:375-380): read-only and
/// disabled apps reject writes; anything else (incl. unknown values, which
/// legacy `get-own-status` coerces to active) is writable.
pub fn write_gate_error(status: &str) -> Option<InstantError> {
    match status {
        "read-only" => Some(app_read_only_error()),
        "disabled" => Some(app_disabled_error()),
        _ => None,
    }
}

/// Legacy `assert-read-allowed!` (model/app.clj:382-387): only a disabled
/// app rejects reads.
pub fn read_gate_error(status: &str) -> Option<InstantError> {
    match status {
        "disabled" => Some(app_disabled_error()),
        _ => None,
    }
}
