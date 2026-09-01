//! /admin/* HTTP API (see docs/ADMIN.md), wire-compatible with @instantdb/admin.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::Json;
use instant_core::attr::{AttrMap, Cardinality, ValueType};
use instant_core::error::{InstantError, Result};
use instant_core::instaql::{self, EntityNode, QueryResult};
use instant_core::system_catalog as sc;
use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::auth;
use crate::routes::runtime::{err_response, json_or_err, user_json};
use crate::service::{self, PermsCtx};
use crate::state::AppState;

// ---------------------------------------------------------------------------
// Auth

pub struct AdminCtx {
    pub app_id: Uuid,
    pub perms: PermsCtx,
}

pub async fn authed(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
) -> Result<AdminCtx> {
    let app_id = headers
        .get("app-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .or_else(|| params.get("app_id").cloned())
        .and_then(|s| Uuid::parse_str(&s).ok())
        .ok_or_else(|| InstantError::param_missing("Missing parameter: app-id"))?;

    // Per-app limit on all /admin/* routes (legacy with-rate-limiting,
    // docs/ADMIN.md §2). Checked before token auth so a hammering client
    // can't run a DB lookup per request.
    state
        .limiters
        .admin
        .check(app_id, 1.0)
        .map_err(crate::rate_limit::rate_limited_err)?;

    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|s| s.trim().to_string());

    let as_token = headers.get("as-token").and_then(|v| v.to_str().ok());
    let as_email = headers.get("as-email").and_then(|v| v.to_str().ok());
    let as_guest = headers.get("as-guest").is_some();

    // impersonation (as-token/as-guest work without a valid admin token)
    if let Some(token) = as_token {
        let user = auth::user_by_refresh_token(state, app_id, token)
            .await?
            .ok_or_else(|| {
                InstantError::record_not_found("app-user", "Record not found: app-user")
            })?;
        return Ok(AdminCtx {
            app_id,
            perms: PermsCtx {
                admin: false,
                user_id: Some(user.id),
                user_map: None,
                rule_params: None,
            },
        });
    }
    let check_admin = |token: Option<String>| async move {
        let token = token.ok_or_else(|| {
            InstantError::record_not_found(
                "app-admin-token",
                "Record not found: app-admin-token. This admin token may be expired or invalid. Or you may have provided an incorrect app ID.",
            )
        })?;
        if !auth::check_admin_token(state, app_id, &token).await? {
            return Err(InstantError::record_not_found(
                "app-admin-token",
                "Record not found: app-admin-token. This admin token may be expired or invalid. Or you may have provided an incorrect app ID.",
            ));
        }
        Ok(())
    };
    if let Some(email) = as_email {
        check_admin(bearer).await?;
        let user = auth::user_by_email(state, app_id, email)
            .await?
            .ok_or_else(|| {
                InstantError::record_not_found("app-user", "Record not found: app-user")
            })?;
        return Ok(AdminCtx {
            app_id,
            perms: PermsCtx {
                admin: false,
                user_id: Some(user.id),
                user_map: None,
                rule_params: None,
            },
        });
    }
    if as_guest {
        return Ok(AdminCtx {
            app_id,
            perms: PermsCtx {
                admin: false,
                user_id: None,
                user_map: None,
                rule_params: None,
            },
        });
    }
    check_admin(bearer).await?;
    Ok(AdminCtx {
        app_id,
        perms: PermsCtx {
            admin: true,
            user_id: None,
            user_map: None,
            rule_params: None,
        },
    })
}

// ---------------------------------------------------------------------------
// Object tree builder (/admin/query response format)

fn entity_created_at(node: &EntityNode, attrs: &AttrMap) -> i64 {
    node.triples
        .iter()
        .find(|t| attrs.get(&t.a).map(|a| a.label == "id").unwrap_or(false))
        .map(|t| t.t)
        .unwrap_or(0)
}

fn node_to_object(
    state: &AppState,
    app_id: Uuid,
    node: &EntityNode,
    attrs: &AttrMap,
    forms: Option<&[instaql::Form]>,
    inference: bool,
) -> Value {
    let mut m = Map::new();
    m.insert("id".into(), json!(node.eid));
    let mut location_id: Option<String> = None;
    for t in &node.triples {
        let Some(attr) = attrs.get(&t.a) else {
            continue;
        };
        if attr.value_type != ValueType::Blob {
            continue;
        }
        if attr.label == "id" {
            continue;
        }
        if node.etype == "$files" && attr.label == "location-id" {
            location_id = t.v.as_str().map(|s| s.to_string());
            continue;
        }
        if !t.v.is_null() {
            m.insert(attr.label.clone(), t.v.clone());
        }
    }
    if node.etype == "$files" {
        if let Some(loc) = &location_id {
            m.insert(
                "url".into(),
                json!(crate::storage::download_url(state, app_id, loc)),
            );
        }
    }
    for child in &node.children {
        let child_form = forms.and_then(|fs| fs.iter().find(|f| f.k == child.k));
        let mut entities: Vec<(&EntityNode, i64)> = child
            .entities
            .iter()
            .map(|e| (e, entity_created_at(e, attrs)))
            .collect();
        // nested ordering: order key or serverCreatedAt asc
        sort_entities(&mut entities, child_form, attrs);
        // nested limit/first/last (admin applies server-side)
        if let Some(f) = child_form {
            if let Some(l) = f.opts.effective_limit() {
                if f.opts.last.is_some() {
                    let skip = entities.len().saturating_sub(l as usize);
                    entities.drain(..skip);
                } else {
                    entities.truncate(l as usize);
                }
            }
        }
        let vals: Vec<Value> = entities
            .iter()
            .map(|(e, _)| {
                node_to_object(
                    state,
                    app_id,
                    e,
                    attrs,
                    child_form.map(|f| f.children.as_slice()),
                    inference,
                )
            })
            .collect();
        // inference: singular for forward-cardinality-one links (or reverse-unique)
        let singular = inference && {
            match attrs.by_fwd_name(&node.etype, &child.k) {
                Some(a) if a.value_type == ValueType::Ref => a.cardinality == Cardinality::One,
                _ => match attrs.by_rev_name(&node.etype, &child.k) {
                    Some(a) => a.is_unique,
                    None => false,
                },
            }
        };
        if singular {
            if let Some(first) = vals.into_iter().next() {
                m.insert(child.k.clone(), first);
            }
        } else {
            m.insert(child.k.clone(), Value::Array(vals));
        }
    }
    Value::Object(m)
}

fn sort_entities(
    entities: &mut [(&EntityNode, i64)],
    form: Option<&instaql::Form>,
    attrs: &AttrMap,
) {
    let order = form.and_then(|f| f.opts.order.clone());
    match order {
        Some(o) if o.key != "serverCreatedAt" => {
            entities.sort_by(|(a, at), (b, bt)| {
                let av = a
                    .triples
                    .iter()
                    .find(|t| attrs.get(&t.a).map(|x| x.label == o.key).unwrap_or(false))
                    .map(|t| t.v.clone())
                    .unwrap_or(Value::Null);
                let bv = b
                    .triples
                    .iter()
                    .find(|t| attrs.get(&t.a).map(|x| x.label == o.key).unwrap_or(false))
                    .map(|t| t.v.clone())
                    .unwrap_or(Value::Null);
                let ord = cmp_json(&av, &bv).then(at.cmp(bt));
                if o.dir == instaql::Dir::Desc {
                    ord.reverse()
                } else {
                    ord
                }
            });
        }
        Some(o) if o.dir == instaql::Dir::Desc => {
            entities.sort_by(|(_, at), (_, bt)| bt.cmp(at));
        }
        _ => {
            entities.sort_by_key(|(_, at)| *at);
        }
    }
}

fn cmp_json(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Number(x), Value::Number(y)) => x
            .as_f64()
            .partial_cmp(&y.as_f64())
            .unwrap_or(Ordering::Equal),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        _ => a.to_string().cmp(&b.to_string()),
    }
}

pub fn object_tree(
    state: &AppState,
    app_id: Uuid,
    result: &QueryResult,
    attrs: &AttrMap,
    q: &Value,
    inference: bool,
) -> Value {
    let forms = instaql::parse_query(q).unwrap_or_default();
    let mut out = Map::new();
    for form in &result.forms {
        if let Some(count) = form.aggregate {
            // aggregate namespaces are exposed via aggregate key on SDK;
            // admin SDK reads objects; keep both shapes simple
            out.insert(form.k.clone(), json!({"count": count}));
            continue;
        }
        let f = forms.iter().find(|f| f.k == form.k);
        let vals: Vec<Value> = form
            .entities
            .iter()
            .map(|e| {
                node_to_object(
                    state,
                    app_id,
                    e,
                    attrs,
                    f.map(|f| f.children.as_slice()),
                    inference,
                )
            })
            .collect();
        out.insert(form.k.clone(), Value::Array(vals));
    }
    Value::Object(out)
}

// ---------------------------------------------------------------------------
// /admin/query

pub async fn query(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    Json(body): Json<Value>,
) -> Response {
    json_or_err(query_impl(&state, &headers, &params, &body).await)
}

async fn query_impl(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
    body: &Value,
) -> Result<Value> {
    let ctx = authed(state, headers, params).await?;
    let q = body
        .get("query")
        .filter(|q| q.is_object())
        .ok_or_else(|| InstantError::param_missing("Missing parameter: [\"body\" \"query\"]"))?;
    let inference = body
        .get("inference?")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let attrs = service::load_attrs(state, ctx.app_id).await?;
    let result = service::run_query(state, ctx.app_id, &attrs, &ctx.perms, q).await?;
    Ok(object_tree(
        state, ctx.app_id, &result, &attrs, q, inference,
    ))
}

// ---------------------------------------------------------------------------
// /admin/transact — admin steps grammar translation

fn eid_to_lookup(
    attrs: &AttrMap,
    etype: &str,
    v: &Value,
    new_attrs: &mut Vec<Value>,
    throw_missing: bool,
) -> Result<Value> {
    // -> client eid: uuid string or [attr-uuid, value]
    match v {
        Value::String(s) => {
            if Uuid::parse_str(s).is_ok() {
                return Ok(v.clone());
            }
            if let Some(rest) = s.strip_prefix("lookup__") {
                let mut parts = rest.splitn(2, "__");
                let attr_name = parts.next().unwrap_or_default();
                let raw = parts.next().unwrap_or_default();
                let value: Value = serde_json::from_str(raw).map_err(|_| invalid_eid(s))?;
                let attr_id =
                    resolve_lookup_attr(attrs, etype, attr_name, new_attrs, throw_missing)?;
                return Ok(json!([attr_id, value]));
            }
            Err(invalid_eid(s))
        }
        Value::Array(arr) if arr.len() == 2 => {
            let attr_name = arr[0].as_str().ok_or_else(|| invalid_eid(&v.to_string()))?;
            if Uuid::parse_str(attr_name).is_ok() {
                return Ok(v.clone());
            }
            let attr_id = resolve_lookup_attr(attrs, etype, attr_name, new_attrs, throw_missing)?;
            Ok(json!([attr_id, arr[1]]))
        }
        Value::Object(m) if m.len() == 1 => {
            let (attr_name, value) = m.iter().next().unwrap();
            let attr_id = resolve_lookup_attr(attrs, etype, attr_name, new_attrs, throw_missing)?;
            Ok(json!([attr_id, value]))
        }
        other => Err(invalid_eid(&other.to_string())),
    }
}

fn invalid_eid(x: &str) -> InstantError {
    InstantError::validation_failed(
        "steps",
        format!("Invalid entity ID '{x}'. Entity IDs must be UUIDs or lookup references."),
        json!([{"message": format!("Invalid entity ID '{x}'. Entity IDs must be UUIDs or lookup references.")}]),
    )
}

fn resolve_lookup_attr(
    attrs: &AttrMap,
    etype: &str,
    attr_name: &str,
    new_attrs: &mut Vec<Value>,
    throw_missing: bool,
) -> Result<Uuid> {
    if let Some(a) = attrs.by_fwd_name(etype, attr_name) {
        if !a.is_unique {
            return Err(InstantError::validation_failed(
                "steps",
                format!("{attr_name} is not a unique attribute on {etype}"),
                json!([]),
            ));
        }
        return Ok(a.id);
    }
    // check pending new attrs
    for na in new_attrs.iter() {
        if let Some(obj) = na.get(1) {
            if obj
                .get("forward-identity")
                .and_then(|f| f.get(1))
                .and_then(|v| v.as_str())
                == Some(etype)
                && obj
                    .get("forward-identity")
                    .and_then(|f| f.get(2))
                    .and_then(|v| v.as_str())
                    == Some(attr_name)
            {
                return Ok(obj
                    .get("id")
                    .and_then(|v| v.as_str())
                    .and_then(|s| Uuid::parse_str(s).ok())
                    .unwrap());
            }
        }
    }
    if throw_missing {
        return Err(missing_attrs_err(&[format!("{etype}.{attr_name}")]));
    }
    let id = Uuid::new_v4();
    new_attrs.push(json!(["add-attr", {
        "id": id,
        "forward-identity": [Uuid::new_v4(), etype, attr_name],
        "value-type": "blob", "cardinality": "one",
        "unique?": true, "index?": true
    }]));
    Ok(id)
}

fn missing_attrs_err(names: &[String]) -> InstantError {
    InstantError::new(
        "validation-failed",
        400,
        "Attributes are missing in your schema",
        Some(json!({
            "data-type": "steps",
            "errors": [{
                "message": "Attributes are missing in your schema",
                "hint": {"attributes": names}
            }]
        })),
    )
}

fn resolve_obj_attr(
    attrs: &AttrMap,
    etype: &str,
    label: &str,
    new_attrs: &mut Vec<Value>,
    throw_missing: bool,
) -> Result<Uuid> {
    if let Some(a) = attrs.by_fwd_name(etype, label) {
        return Ok(a.id);
    }
    for na in new_attrs.iter() {
        if let Some(obj) = na.get(1) {
            let f = obj.get("forward-identity");
            if f.and_then(|f| f.get(1)).and_then(|v| v.as_str()) == Some(etype)
                && f.and_then(|f| f.get(2)).and_then(|v| v.as_str()) == Some(label)
            {
                return Ok(obj
                    .get("id")
                    .and_then(|v| v.as_str())
                    .and_then(|s| Uuid::parse_str(s).ok())
                    .unwrap());
            }
        }
    }
    if throw_missing {
        return Err(missing_attrs_err(&[format!("{etype}.{label}")]));
    }
    let id = Uuid::new_v4();
    new_attrs.push(json!(["add-attr", {
        "id": id,
        "forward-identity": [Uuid::new_v4(), etype, label],
        "value-type": "blob", "cardinality": "one",
        "unique?": false, "index?": false
    }]));
    Ok(id)
}

fn resolve_link_attr(
    attrs: &AttrMap,
    etype: &str,
    label: &str,
    new_attrs: &mut Vec<Value>,
    throw_missing: bool,
) -> Result<(Uuid, bool)> {
    // returns (attr id, forward?)
    if let Some(a) = attrs.by_fwd_name(etype, label) {
        if a.value_type == ValueType::Ref {
            return Ok((a.id, true));
        }
    }
    if let Some(a) = attrs.by_rev_name(etype, label) {
        return Ok((a.id, false));
    }
    for na in new_attrs.iter() {
        if let Some(obj) = na.get(1) {
            let f = obj.get("forward-identity");
            if f.and_then(|f| f.get(1)).and_then(|v| v.as_str()) == Some(etype)
                && f.and_then(|f| f.get(2)).and_then(|v| v.as_str()) == Some(label)
            {
                return Ok((
                    obj.get("id")
                        .and_then(|v| v.as_str())
                        .and_then(|s| Uuid::parse_str(s).ok())
                        .unwrap(),
                    true,
                ));
            }
        }
    }
    if throw_missing {
        return Err(missing_attrs_err(&[format!("{etype}.{label}")]));
    }
    let id = Uuid::new_v4();
    new_attrs.push(json!(["add-attr", {
        "id": id,
        "forward-identity": [Uuid::new_v4(), etype, label],
        "reverse-identity": [Uuid::new_v4(), label, etype],
        "value-type": "ref", "cardinality": "many",
        "unique?": false, "index?": false
    }]));
    Ok((id, true))
}

/// Translate admin steps -> client tx-steps.
pub fn translate_steps(attrs: &AttrMap, steps: &Value, throw_missing: bool) -> Result<Value> {
    let arr = steps
        .as_array()
        .ok_or_else(|| InstantError::param_missing("Missing parameter: [\"body\" \"steps\"]"))?;
    let mut new_attrs: Vec<Value> = vec![];
    let mut out: Vec<Value> = vec![];
    for step in arr {
        let sarr = step.as_array().ok_or_else(|| {
            InstantError::validation_failed("steps", "step must be an array", json!([]))
        })?;
        let action = sarr.first().and_then(|v| v.as_str()).unwrap_or_default();
        match action {
            "create" | "update" | "merge" => {
                let etype = sarr.get(1).and_then(|v| v.as_str()).ok_or_else(|| {
                    InstantError::validation_failed("steps", "missing etype", json!([]))
                })?;
                let eid = eid_to_lookup(
                    attrs,
                    etype,
                    sarr.get(2).unwrap_or(&Value::Null),
                    &mut new_attrs,
                    throw_missing,
                )?;
                let obj = sarr
                    .get(3)
                    .and_then(|v| v.as_object())
                    .cloned()
                    .unwrap_or_default();
                let opts = sarr.get(4).and_then(|v| v.as_object());
                let mode: Option<&str> = match action {
                    "create" => Some("create"),
                    "update" => {
                        match opts.and_then(|o| o.get("upsert")).and_then(|v| v.as_bool()) {
                            Some(false) => Some("update"),
                            _ => None,
                        }
                    }
                    _ => None,
                };
                // id triple first
                let id_attr = resolve_obj_attr(attrs, etype, "id", &mut new_attrs, throw_missing)?;
                let mut push_step = |op: &str, attr_id: Uuid, value: Value| {
                    let mut s = vec![json!(op), eid.clone(), json!(attr_id), value];
                    if let Some(m) = mode {
                        s.push(json!({"mode": m}));
                    }
                    out.push(Value::Array(s));
                };
                push_step("add-triple", id_attr, eid.clone());
                for (label, value) in &obj {
                    if label == "id" {
                        continue;
                    }
                    let attr_id =
                        resolve_obj_attr(attrs, etype, label, &mut new_attrs, throw_missing)?;
                    let op = if action == "merge" {
                        "deep-merge-triple"
                    } else {
                        "add-triple"
                    };
                    push_step(op, attr_id, value.clone());
                }
            }
            "link" | "unlink" => {
                let etype = sarr.get(1).and_then(|v| v.as_str()).ok_or_else(|| {
                    InstantError::validation_failed("steps", "missing etype", json!([]))
                })?;
                let eid = eid_to_lookup(
                    attrs,
                    etype,
                    sarr.get(2).unwrap_or(&Value::Null),
                    &mut new_attrs,
                    throw_missing,
                )?;
                let obj = sarr
                    .get(3)
                    .and_then(|v| v.as_object())
                    .cloned()
                    .unwrap_or_default();
                let op = if action == "link" {
                    "add-triple"
                } else {
                    "retract-triple"
                };
                for (label, value) in &obj {
                    let (attr_id, forward) =
                        resolve_link_attr(attrs, etype, label, &mut new_attrs, throw_missing)?;
                    let targets: Vec<Value> = match value {
                        Value::Array(a)
                            if !(a.len() == 2 && a[0].is_string() && !a[1].is_array()) =>
                        {
                            a.clone()
                        }
                        other => vec![other.clone()],
                    };
                    // target etype for lookups on the other side
                    let target_etype = attrs
                        .get(&attr_id)
                        .map(|a| {
                            if forward {
                                a.reverse_etype.clone().unwrap_or_default()
                            } else {
                                a.etype.clone()
                            }
                        })
                        .unwrap_or_default();
                    for target in targets {
                        let target = eid_to_lookup(
                            attrs,
                            &target_etype,
                            &target,
                            &mut new_attrs,
                            throw_missing,
                        )
                        .unwrap_or(target.clone());
                        if forward {
                            out.push(json!([op, eid, attr_id, target]));
                        } else {
                            out.push(json!([op, target, attr_id, eid]));
                        }
                    }
                }
            }
            "delete" => {
                let etype = sarr.get(1).and_then(|v| v.as_str()).unwrap_or_default();
                let eid = eid_to_lookup(
                    attrs,
                    etype,
                    sarr.get(2).unwrap_or(&Value::Null),
                    &mut new_attrs,
                    throw_missing,
                )?;
                out.push(json!(["delete-entity", eid, etype]));
            }
            "ruleParams" | "rule-params" => {
                let etype = sarr.get(1).and_then(|v| v.as_str()).unwrap_or_default();
                let eid = eid_to_lookup(
                    attrs,
                    etype,
                    sarr.get(2).unwrap_or(&Value::Null),
                    &mut new_attrs,
                    throw_missing,
                )?;
                out.push(json!([
                    "rule-params",
                    eid,
                    etype,
                    sarr.get(3).cloned().unwrap_or(json!({}))
                ]));
            }
            "add-attr" | "update-attr" | "delete-attr" | "restore-attr" | "add-triple"
            | "deep-merge-triple" | "retract-triple" | "delete-entity" => {
                out.push(step.clone());
            }
            other => {
                return Err(InstantError::validation_failed(
                    "steps",
                    format!("unknown step action {other:?}"),
                    json!([]),
                ))
            }
        }
    }
    let mut all = new_attrs;
    all.extend(out);
    Ok(Value::Array(all))
}

pub async fn transact(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    Json(body): Json<Value>,
) -> Response {
    json_or_err(transact_impl(&state, &headers, &params, &body).await)
}

async fn transact_impl(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
    body: &Value,
) -> Result<Value> {
    let ctx = authed(state, headers, params).await?;
    let steps = body
        .get("steps")
        .ok_or_else(|| InstantError::param_missing("Missing parameter: [\"body\" \"steps\"]"))?;
    let throw_missing = body
        .get("throw-on-missing-attrs?")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let attrs = service::load_attrs(state, ctx.app_id).await?;
    let tx_steps = translate_steps(&attrs, steps, throw_missing)?;
    let report = service::run_transact(state, ctx.app_id, &ctx.perms, &tx_steps).await?;
    Ok(json!({"tx-id": report.tx_id}))
}

// ---------------------------------------------------------------------------
// auth endpoints

pub async fn refresh_tokens(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    Json(body): Json<Value>,
) -> Response {
    json_or_err(refresh_tokens_impl(&state, &headers, &params, &body).await)
}

async fn refresh_tokens_impl(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
    body: &Value,
) -> Result<Value> {
    let ctx = authed(state, headers, params).await?;
    let email = body.get("email").and_then(|v| v.as_str());
    let id = body
        .get("id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok());
    let (user_id, created) = match (email, id) {
        (Some(email), _) => match auth::user_by_email(state, ctx.app_id, email).await? {
            Some(u) => (u.id, false),
            None => {
                let uid = Uuid::new_v4();
                let steps = json!([
                    ["add-triple", uid, sc::attr_id("$users", "id"), uid],
                    ["add-triple", uid, sc::attr_id("$users", "email"), email]
                ]);
                service::run_system_transact(state, ctx.app_id, &steps).await?;
                (uid, true)
            }
        },
        (None, Some(id)) => match auth::user_by_id(state, ctx.app_id, id).await? {
            Some(u) => (u.id, false),
            None => {
                let steps = json!([["add-triple", id, sc::attr_id("$users", "id"), id]]);
                service::run_system_transact(state, ctx.app_id, &steps).await?;
                (id, true)
            }
        },
        _ => {
            return Err(InstantError::validation_failed(
                "body",
                "Please provide an `email` or `id`",
                json!([{"message": "Please provide an `email` or `id`"}]),
            ))
        }
    };
    let token = auth::mint_refresh_token(state, ctx.app_id, user_id).await?;
    let user = user_json(state, ctx.app_id, user_id, Some(token)).await?;
    Ok(json!({"user": user, "created": created}))
}

pub async fn sign_out(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    Json(body): Json<Value>,
) -> Response {
    json_or_err(sign_out_impl(&state, &headers, &params, &body).await)
}

async fn sign_out_impl(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
    body: &Value,
) -> Result<Value> {
    let ctx = authed(state, headers, params).await?;
    if let Some(id) = body
        .get("id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
    {
        auth::sign_out(state, ctx.app_id, Some(id), None).await?;
    } else if let Some(email) = body.get("email").and_then(|v| v.as_str()) {
        let user = auth::user_by_email(state, ctx.app_id, email)
            .await?
            .ok_or_else(|| {
                InstantError::record_not_found("app-user", "Record not found: app-user")
            })?;
        auth::sign_out(state, ctx.app_id, Some(user.id), None).await?;
    } else if let Some(token) = body.get("refresh_token").and_then(|v| v.as_str()) {
        auth::sign_out(state, ctx.app_id, None, Some(token)).await?;
    } else {
        return Err(InstantError::validation_failed(
            "body",
            "Please provide an `id`, `email`, or `refresh_token`",
            json!([{"message": "Please provide an `id`, `email`, or `refresh_token`"}]),
        ));
    }
    Ok(json!({"ok": true}))
}

pub async fn get_user(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    json_or_err(get_user_impl(&state, &headers, &params).await)
}

async fn find_user_by_params(
    state: &AppState,
    app_id: Uuid,
    params: &HashMap<String, String>,
) -> Result<Option<auth::AppUser>> {
    if let Some(email) = params.get("email") {
        return auth::user_by_email(state, app_id, email).await;
    }
    if let Some(token) = params.get("refresh_token") {
        return auth::user_by_refresh_token(state, app_id, token).await;
    }
    if let Some(id) = params.get("id") {
        let id = Uuid::parse_str(id)
            .map_err(|_| InstantError::param_malformed("Malformed parameter: id"))?;
        return auth::user_by_id(state, app_id, id).await;
    }
    Err(InstantError::validation_failed(
        "params",
        "Please provide a user id, email, or refresh_token",
        json!([{"message": "Please provide a user id, email, or refresh_token"}]),
    ))
}

async fn get_user_impl(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
) -> Result<Value> {
    let ctx = authed(state, headers, params).await?;
    let user = find_user_by_params(state, ctx.app_id, params).await?;
    match user {
        Some(u) => {
            let user = user_json(state, ctx.app_id, u.id, None).await?;
            Ok(json!({"user": user}))
        }
        None => Err(InstantError::record_not_found(
            "app-user",
            "Record not found: app-user",
        )),
    }
}

pub async fn delete_user(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    json_or_err(delete_user_impl(&state, &headers, &params).await)
}

async fn delete_user_impl(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
) -> Result<Value> {
    let ctx = authed(state, headers, params).await?;
    let user = find_user_by_params(state, ctx.app_id, params).await?;
    match user {
        Some(u) => {
            let user = user_json(state, ctx.app_id, u.id, None).await?;
            service::run_system_transact(
                state,
                ctx.app_id,
                &json!([["delete-entity", u.id, "$users"]]),
            )
            .await?;
            Ok(json!({"deleted": user}))
        }
        None => Err(InstantError::record_not_found(
            "app-user",
            "Record not found: app-user",
        )),
    }
}

pub async fn presence(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    json_or_err(presence_impl(&state, &headers, &params).await)
}

async fn presence_impl(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
) -> Result<Value> {
    let ctx = authed(state, headers, params).await?;
    let room_id = params
        .get("room-id")
        .ok_or_else(|| InstantError::param_missing("Missing parameter: room-id"))?;
    let snapshot = crate::presence::room_snapshot(state, ctx.app_id, room_id).await?;
    Ok(json!({"sessions": snapshot}))
}

// ---------------------------------------------------------------------------
// storage

pub async fn storage_upload(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    json_or_err(storage_upload_impl(&state, &headers, &params, body).await)
}

async fn storage_upload_impl(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
    body: Bytes,
) -> Result<Value> {
    let ctx = authed(state, headers, params).await?;
    let path = headers
        .get("path")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .ok_or_else(|| InstantError::param_missing("Missing parameter: path"))?;
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty() && *s != "null" && *s != "undefined")
        .map(|s| s.to_string());
    let content_disposition = headers
        .get("content-disposition")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let location_id = Uuid::new_v4().to_string();
    let size = crate::storage::put_blob(state, ctx.app_id, &location_id, &body).await?;

    // upsert $files row by path lookup (replacing a path orphans its old blob)
    let path_attr = sc::attr_id("$files", "path");
    let old_location = file_location_by_path(state, ctx.app_id, &path).await;
    let lookup = json!([path_attr, path]);
    let mut steps = vec![
        json!(["add-triple", lookup, sc::attr_id("$files", "id"), lookup]),
        json!(["add-triple", lookup, sc::attr_id("$files", "path"), path]),
        json!(["add-triple", lookup, sc::attr_id("$files", "size"), size]),
        json!([
            "add-triple",
            lookup,
            sc::attr_id("$files", "location-id"),
            location_id
        ]),
        json!([
            "add-triple",
            lookup,
            sc::attr_id("$files", "key-version"),
            1
        ]),
    ];
    if let Some(ct) = &content_type {
        steps.push(json!([
            "add-triple",
            lookup,
            sc::attr_id("$files", "content-type"),
            ct
        ]));
    }
    if let Some(cd) = &content_disposition {
        steps.push(json!([
            "add-triple",
            lookup,
            sc::attr_id("$files", "content-disposition"),
            cd
        ]));
    }
    service::run_system_transact(state, ctx.app_id, &Value::Array(steps)).await?;

    // fetch the file entity id
    use sqlx::Row;
    let row = sqlx::query(
        "SELECT entity_id FROM triples
         WHERE app_id = $1 AND attr_id = $2 AND av AND value = to_jsonb($3::text) LIMIT 1",
    )
    .bind(ctx.app_id)
    .bind(path_attr)
    .bind(&path)
    .fetch_one(&state.pool)
    .await
    .map_err(InstantError::from)?;
    let file_id: Uuid = row.get("entity_id");
    if let Some(old) = old_location {
        crate::storage::delete_blob(state, ctx.app_id, &old).await;
    }
    Ok(json!({"data": {"id": file_id, "location-id": location_id, "size": size}}))
}

/// location-id of the $files row at `path`, if any.
async fn file_location_by_path(state: &AppState, app_id: Uuid, path: &str) -> Option<String> {
    use sqlx::Row;
    let path_attr = sc::attr_id("$files", "path");
    let loc_attr = sc::attr_id("$files", "location-id");
    let row = sqlx::query(
        "SELECT l.value AS loc FROM triples t
         JOIN triples l ON l.app_id = t.app_id AND l.entity_id = t.entity_id AND l.attr_id = $3
         WHERE t.app_id = $1 AND t.attr_id = $2 AND t.av AND t.value = to_jsonb($4::text) LIMIT 1",
    )
    .bind(app_id)
    .bind(path_attr)
    .bind(loc_attr)
    .bind(path)
    .fetch_optional(&state.pool)
    .await
    .ok()??;
    row.get::<Value, _>("loc").as_str().map(|s| s.to_string())
}

pub async fn storage_delete(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    json_or_err(storage_delete_impl(&state, &headers, &params).await)
}

async fn storage_delete_impl(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
) -> Result<Value> {
    let ctx = authed(state, headers, params).await?;
    let filename = params
        .get("filename")
        .ok_or_else(|| InstantError::param_missing("Missing parameter: filename"))?;
    use sqlx::Row;
    let path_attr = sc::attr_id("$files", "path");
    let row = sqlx::query(
        "SELECT entity_id FROM triples
         WHERE app_id = $1 AND attr_id = $2 AND av AND value = to_jsonb($3::text) LIMIT 1",
    )
    .bind(ctx.app_id)
    .bind(path_attr)
    .bind(filename)
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    match row {
        Some(row) => {
            let id: Uuid = row.get("entity_id");
            let old = file_location_by_path(state, ctx.app_id, filename).await;
            service::run_system_transact(
                state,
                ctx.app_id,
                &json!([["delete-entity", id, "$files"]]),
            )
            .await?;
            if let Some(old) = old {
                crate::storage::delete_blob(state, ctx.app_id, &old).await;
            }
            Ok(json!({"data": {"id": id}}))
        }
        None => Ok(json!({"data": {"id": null}})),
    }
}

// ---------------------------------------------------------------------------
// admin magic codes / guests

pub async fn magic_code(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    Json(body): Json<Value>,
) -> Response {
    json_or_err(magic_code_impl(&state, &headers, &params, &body).await)
}

async fn magic_code_impl(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
    body: &Value,
) -> Result<Value> {
    let ctx = authed(state, headers, params).await?;
    let email = body
        .get("email")
        .and_then(|v| v.as_str())
        .ok_or_else(|| InstantError::param_missing("Missing parameter: email"))?
        .trim()
        .to_lowercase();
    let code: String = {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        (0..6)
            .map(|_| char::from(b'0' + rng.gen_range(0..9)))
            .collect()
    };
    let entity = Uuid::new_v4();
    let steps = json!([
        [
            "add-triple",
            entity,
            sc::attr_id("$magicCodes", "id"),
            entity
        ],
        [
            "add-triple",
            entity,
            sc::attr_id("$magicCodes", "codeHash"),
            auth::hash_string(&code)
        ],
        [
            "add-triple",
            entity,
            sc::attr_id("$magicCodes", "email"),
            email
        ]
    ]);
    service::run_system_transact(state, ctx.app_id, &steps).await?;
    Ok(json!({"code": code}))
}

pub async fn admin_verify_magic_code(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    Json(body): Json<Value>,
) -> Response {
    // reuse the runtime handler logic by rewriting the body to kebab keys
    match verify_magic_code_admin_impl(&state, &headers, &params, &body).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => err_response(&e),
    }
}

async fn verify_magic_code_admin_impl(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
    body: &Value,
) -> Result<Value> {
    let ctx = authed(state, headers, params).await?;
    let mut rt_body = body.clone();
    rt_body["app-id"] = json!(ctx.app_id);
    crate::routes::runtime::verify_magic_code_shared(state, &rt_body).await
}

pub async fn admin_sign_in_guest(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    Json(body): Json<Value>,
) -> Response {
    match admin_sign_in_guest_impl(&state, &headers, &params, &body).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => err_response(&e),
    }
}

async fn admin_sign_in_guest_impl(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
    _body: &Value,
) -> Result<Value> {
    let ctx = authed(state, headers, params).await?;
    let uid = Uuid::new_v4();
    let steps = json!([
        ["add-triple", uid, sc::attr_id("$users", "id"), uid],
        ["add-triple", uid, sc::attr_id("$users", "type"), "guest"]
    ]);
    service::run_system_transact(state, ctx.app_id, &steps).await?;
    let token = auth::mint_refresh_token(state, ctx.app_id, uid).await?;
    let user = user_json(state, ctx.app_id, uid, Some(token)).await?;
    Ok(json!({"user": user}))
}

// ---------------------------------------------------------------------------
// client-facing storage routes (browser SDK, refresh-token bearer)

async fn client_storage_ctx(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
) -> Result<(Uuid, PermsCtx)> {
    let app_id = headers
        .get("app-id")
        .or_else(|| headers.get("app_id"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .or_else(|| params.get("app_id").cloned())
        .and_then(|s| Uuid::parse_str(&s).ok())
        .ok_or_else(|| InstantError::param_missing("Missing parameter: app-id"))?;
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|s| s.trim().to_string());
    let user_id = match bearer {
        Some(token) => auth::user_by_refresh_token(state, app_id, &token)
            .await?
            .map(|u| u.id),
        None => None,
    };
    Ok((
        app_id,
        PermsCtx {
            admin: false,
            user_id,
            user_map: None,
            rule_params: None,
        },
    ))
}

/// Evaluate a $files rule (create/delete/view) for a path. Default deny.
async fn check_files_perm(
    state: &AppState,
    app_id: Uuid,
    perms: &PermsCtx,
    action: &str,
    path: &str,
) -> Result<()> {
    let mut conn = state.pool.acquire().await.map_err(InstantError::from)?;
    let rules = instant_core::perms::Rules::load(&mut conn, app_id).await?;
    let program = rules.program("$files", action);
    let auth_ctx = instant_core::perms::AuthCtx {
        user_id: perms.user_id,
        user_map: None,
    };
    let auth_val = if let Some(uid) = auth_ctx.user_id {
        let attrs = service::load_attrs(state, app_id).await?;
        instant_core::perms::fetch_entity_map(&mut conn, app_id, &attrs, "$users", uid)
            .await?
            .map(Value::Object)
            .unwrap_or(Value::Null)
    } else {
        Value::Null
    };
    let data = json!({"path": path});
    let ok = instant_core::perms::eval_program(&program, &data, None, &auth_val, &json!({}))?;
    if !ok {
        return Err(InstantError::permission_denied(
            json!(["$files", action]),
            "Permission denied: not perms-pass?",
        ));
    }
    Ok(())
}

pub async fn client_storage_upload(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    json_or_err(client_storage_upload_impl(&state, &headers, &params, body).await)
}

async fn client_storage_upload_impl(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
    body: Bytes,
) -> Result<Value> {
    let (app_id, perms) = client_storage_ctx(state, headers, params).await?;
    state
        .limiters
        .storage_upload
        .check(app_id, 1.0)
        .map_err(crate::rate_limit::rate_limited_err)?;
    let path = headers
        .get("path")
        .or_else(|| headers.get("filename"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .ok_or_else(|| InstantError::param_missing("Missing parameter: path"))?;
    check_files_perm(state, app_id, &perms, "create", &path).await?;
    // reuse the admin upload path (system transact under the hood)
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty() && *s != "null" && *s != "undefined")
        .map(|s| s.to_string());
    let content_disposition = headers
        .get("content-disposition")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let location_id = Uuid::new_v4().to_string();
    let size = crate::storage::put_blob(state, app_id, &location_id, &body).await?;
    let path_attr = sc::attr_id("$files", "path");
    let old_location = file_location_by_path(state, app_id, &path).await;
    let lookup = json!([path_attr, path]);
    let mut steps = vec![
        json!(["add-triple", lookup, sc::attr_id("$files", "id"), lookup]),
        json!(["add-triple", lookup, sc::attr_id("$files", "path"), path]),
        json!(["add-triple", lookup, sc::attr_id("$files", "size"), size]),
        json!([
            "add-triple",
            lookup,
            sc::attr_id("$files", "location-id"),
            location_id
        ]),
        json!([
            "add-triple",
            lookup,
            sc::attr_id("$files", "key-version"),
            1
        ]),
    ];
    if let Some(ct) = &content_type {
        steps.push(json!([
            "add-triple",
            lookup,
            sc::attr_id("$files", "content-type"),
            ct
        ]));
    }
    if let Some(cd) = &content_disposition {
        steps.push(json!([
            "add-triple",
            lookup,
            sc::attr_id("$files", "content-disposition"),
            cd
        ]));
    }
    service::run_system_transact(state, app_id, &Value::Array(steps)).await?;
    use sqlx::Row;
    let row = sqlx::query(
        "SELECT entity_id FROM triples
         WHERE app_id = $1 AND attr_id = $2 AND av AND value = to_jsonb($3::text) LIMIT 1",
    )
    .bind(app_id)
    .bind(path_attr)
    .bind(&path)
    .fetch_one(&state.pool)
    .await
    .map_err(InstantError::from)?;
    let file_id: Uuid = row.get("entity_id");
    if let Some(old) = old_location {
        crate::storage::delete_blob(state, app_id, &old).await;
    }
    Ok(json!({"data": {"id": file_id, "location-id": location_id, "size": size}}))
}

pub async fn client_storage_delete(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    json_or_err(client_storage_delete_impl(&state, &headers, &params).await)
}

async fn client_storage_delete_impl(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
) -> Result<Value> {
    let (app_id, perms) = client_storage_ctx(state, headers, params).await?;
    state
        .limiters
        .storage_upload
        .check(app_id, 1.0)
        .map_err(crate::rate_limit::rate_limited_err)?;
    let filename = params
        .get("filename")
        .ok_or_else(|| InstantError::param_missing("Missing parameter: filename"))?;
    check_files_perm(state, app_id, &perms, "delete", filename).await?;
    use sqlx::Row;
    let path_attr = sc::attr_id("$files", "path");
    let row = sqlx::query(
        "SELECT entity_id FROM triples
         WHERE app_id = $1 AND attr_id = $2 AND av AND value = to_jsonb($3::text) LIMIT 1",
    )
    .bind(app_id)
    .bind(path_attr)
    .bind(filename)
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    match row {
        Some(row) => {
            let id: Uuid = row.get("entity_id");
            let old = file_location_by_path(state, app_id, filename).await;
            service::run_system_transact(state, app_id, &json!([["delete-entity", id, "$files"]]))
                .await?;
            if let Some(old) = old {
                crate::storage::delete_blob(state, app_id, &old).await;
            }
            Ok(json!({"data": {"id": id}}))
        }
        None => Ok(json!({"data": {"id": null}})),
    }
}

pub async fn client_signed_download_url(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    json_or_err(client_signed_download_url_impl(&state, &headers, &params).await)
}

async fn client_signed_download_url_impl(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
) -> Result<Value> {
    let (app_id, perms) = client_storage_ctx(state, headers, params).await?;
    state
        .limiters
        .storage_serve
        .check(app_id, 1.0)
        .map_err(crate::rate_limit::rate_limited_err)?;
    let filename = params
        .get("filename")
        .ok_or_else(|| InstantError::param_missing("Missing parameter: filename"))?;
    check_files_perm(state, app_id, &perms, "view", filename).await?;
    use sqlx::Row;
    let path_attr = sc::attr_id("$files", "path");
    let loc_attr = sc::attr_id("$files", "location-id");
    let row = sqlx::query(
        "SELECT l.value AS loc FROM triples t
         JOIN triples l ON l.app_id = t.app_id AND l.entity_id = t.entity_id AND l.attr_id = $3
         WHERE t.app_id = $1 AND t.attr_id = $2 AND t.av AND t.value = to_jsonb($4::text) LIMIT 1",
    )
    .bind(app_id)
    .bind(path_attr)
    .bind(loc_attr)
    .bind(filename)
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    match row {
        Some(r) => {
            let loc: Value = r.get("loc");
            let url = loc
                .as_str()
                .map(|l| crate::storage::download_url(state, app_id, l));
            Ok(json!({"data": url}))
        }
        None => Ok(json!({"data": null})),
    }
}

// ---------------------------------------------------------------------------
// perms-check debug endpoints (db.debugQuery / db.debugTransact)

pub async fn query_perms_check(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    Json(body): Json<Value>,
) -> Response {
    json_or_err(query_perms_check_impl(&state, &headers, &params, &body).await)
}

async fn query_perms_check_impl(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
    body: &Value,
) -> Result<Value> {
    let ctx = authed(state, headers, params).await?;
    if ctx.perms.admin {
        return Err(InstantError::validation_failed(
            "body",
            "Cannot test perms as admin",
            json!([{"message": "Cannot test perms as admin"}]),
        ));
    }
    let q = body
        .get("query")
        .filter(|q| q.is_object())
        .ok_or_else(|| InstantError::param_missing("Missing parameter: [\"body\" \"query\"]"))?;
    let inference = body
        .get("inference?")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let attrs = service::load_attrs(state, ctx.app_id).await?;

    // run unfiltered, then evaluate view per top-level entity for check-results
    let admin_perms = PermsCtx {
        admin: true,
        user_id: None,
        user_map: None,
        rule_params: None,
    };
    let unfiltered = service::run_query(state, ctx.app_id, &attrs, &admin_perms, q).await?;

    let mut conn = state.pool.acquire().await.map_err(InstantError::from)?;
    let rules = match body.get("rules-override") {
        Some(code) if code.is_object() => instant_core::perms::Rules { code: code.clone() },
        _ => instant_core::perms::Rules::load(&mut conn, ctx.app_id).await?,
    };
    let auth_ctx = instant_core::perms::AuthCtx {
        user_id: ctx.perms.user_id,
        user_map: None,
    };
    let mut check_results = vec![];
    for form in &unfiltered.forms {
        let program = rules.program(&form.etype, "view");
        for e in &form.entities {
            let mut record = serde_json::Map::new();
            record.insert("id".to_string(), json!(e.eid));
            for t in &e.triples {
                if let Some(a) = attrs.get(&t.a) {
                    if a.value_type == ValueType::Blob && a.label != "id" && !t.v.is_null() {
                        record.insert(a.label.clone(), t.v.clone());
                    }
                }
            }
            let mut data = instant_core::perms::base_entity_map(&attrs, &form.etype, e.eid);
            for (k, v) in &record {
                data.insert(k.clone(), v.clone());
            }
            let auth_val = if let Some(uid) = auth_ctx.user_id {
                instant_core::perms::fetch_entity_map(&mut conn, ctx.app_id, &attrs, "$users", uid)
                    .await?
                    .map(Value::Object)
                    .unwrap_or(Value::Null)
            } else {
                Value::Null
            };
            let rule_params = q.get("$$ruleParams").cloned().unwrap_or(json!({}));
            let ok = instant_core::perms::eval_program(
                &program,
                &Value::Object(data),
                None,
                &auth_val,
                &rule_params,
            )?;
            check_results.push(json!({
                "id": e.eid,
                "entity": form.etype,
                "record": record,
                "program": {
                    "etype": form.etype,
                    "action": "view",
                    "code": program.expr,
                    "display-code": program.expr,
                },
                "check": ok,
            }));
        }
    }

    // the actual filtered result
    let result = service::run_query(state, ctx.app_id, &attrs, &ctx.perms, q).await?;
    let tree = object_tree(state, ctx.app_id, &result, &attrs, q, inference);
    Ok(json!({
        "check-results": check_results,
        "result": tree,
        "rule-wheres": [],
    }))
}

pub async fn transact_perms_check(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    Json(body): Json<Value>,
) -> Response {
    json_or_err(transact_perms_check_impl(&state, &headers, &params, &body).await)
}

async fn transact_perms_check_impl(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
    body: &Value,
) -> Result<Value> {
    let ctx = authed(state, headers, params).await?;
    if ctx.perms.admin {
        return Err(InstantError::validation_failed(
            "body",
            "Cannot test perms as admin",
            json!([{"message": "Cannot test perms as admin"}]),
        ));
    }
    let steps = body
        .get("steps")
        .ok_or_else(|| InstantError::param_missing("Missing parameter: [\"body\" \"steps\"]"))?;
    let throw_missing = body
        .get("throw-on-missing-attrs?")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let commit = body
        .get("dangerously-commit-tx")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let mut attrs = service::load_attrs(state, ctx.app_id).await?;
    let tx_steps = translate_steps(&attrs, steps, throw_missing)?;
    let parsed = instant_core::tx::parse_tx_steps(&tx_steps)?;

    let mut dbtx = state.pool.begin().await.map_err(InstantError::from)?;
    let rules = match body.get("rules-override") {
        Some(code) if code.is_object() => instant_core::perms::Rules { code: code.clone() },
        _ => instant_core::perms::Rules::load(&mut dbtx, ctx.app_id).await?,
    };
    let auth_ctx = instant_core::perms::AuthCtx {
        user_id: ctx.perms.user_id,
        user_map: None,
    };
    let (report, checks) = instant_core::perms::permissioned_transact_checked(
        &mut dbtx,
        ctx.app_id,
        &mut attrs,
        parsed,
        &rules,
        &auth_ctx,
        &json!({}),
        false,
    )
    .await?;
    let all_ok = checks.iter().all(|c| {
        c.get("check-pass?")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    });
    let committed = commit && all_ok;
    if committed {
        dbtx.commit().await.map_err(InstantError::from)?;
        service::notify_tx(state, ctx.app_id, report.tx_id).await;
    } else {
        dbtx.rollback().await.map_err(InstantError::from)?;
    }
    Ok(json!({
        "tx-id": report.tx_id,
        "all-checks-ok?": all_ok,
        "committed?": committed,
        "check-results": checks,
    }))
}
