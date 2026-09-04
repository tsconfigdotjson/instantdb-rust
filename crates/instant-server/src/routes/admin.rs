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

/// The app id of an /admin request: `app-id` header, else `app_id` query
/// param (legacy req->app-id-untrusted! via get-some-param!, which names the
/// first path in its error and lists the rest as `possible-ins`).
pub(crate) fn app_id_param(headers: &HeaderMap, params: &HashMap<String, String>) -> Result<Uuid> {
    let header = headers
        .get("app-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| (["headers", "app-id"], s.to_string()));
    let (path, raw) = header
        .or_else(|| {
            params
                .get("app_id")
                .map(|s| (["query-params", "app_id"], s.clone()))
        })
        .ok_or_else(|| {
            InstantError::new(
                "param-missing",
                400,
                "Missing parameter: [\"headers\" \"app-id\"]",
                Some(json!({
                    "in": ["headers", "app-id"],
                    "possible-ins": [["query-params", "app_id"]],
                })),
            )
        })?;
    Uuid::parse_str(&raw).map_err(|_| {
        InstantError::new(
            "param-malformed",
            400,
            format!("Malformed parameter: [\"{}\" \"{}\"]", path[0], path[1]),
            Some(json!({"in": path, "original-input": raw})),
        )
    })
}

/// `[:body :query]` must be a map (legacy `get-param!` with a `map?`
/// coercer: absent → param-missing, anything else → param-malformed).
pub(crate) fn body_query(body: &Value) -> Result<&Value> {
    match body.get("query") {
        None | Some(Value::Null) => Err(InstantError::new(
            "param-missing",
            400,
            "Missing parameter: [\"body\" \"query\"]",
            Some(json!({"in": ["body", "query"]})),
        )),
        Some(q) if q.is_object() => Ok(q),
        Some(q) => Err(InstantError::new(
            "param-malformed",
            400,
            "Malformed parameter: [\"body\" \"query\"]",
            Some(json!({"in": ["body", "query"], "original-input": q})),
        )),
    }
}

/// Legacy app-admin-token-model/fetch!: the hint carries the lookup args
/// and the explanatory message; the message itself is the bare record name.
pub(crate) fn admin_token_not_found(app_id: Uuid, token: &str) -> InstantError {
    InstantError::new(
        "record-not-found",
        400,
        "Record not found: app-admin-token",
        Some(json!({
            "record-type": "app-admin-token",
            "args": [{"app-id": app_id, "token": token}],
            "message": "This admin token may be expired or invalid. Or you may have provided an incorrect app ID.",
        })),
    )
}

/// Legacy `ex/assert-record! ... :app-user {:args [params]}`.
fn app_user_not_found(args: Value) -> InstantError {
    InstantError::new(
        "record-not-found",
        400,
        "Record not found: app-user",
        Some(json!({"record-type": "app-user", "args": [args]})),
    )
}

/// Which legacy auth helper a route uses.
///
/// * `Impersonating` — legacy `get-perms!` (routes.clj:78-110): `as-token`
///   and `as-guest` authenticate on their own (a refresh token / nothing) for
///   the untrusted app id, `as-email` and the plain form need the app's
///   admin token. Only `/admin/query`, `/admin/transact`, the SSE session
///   routes and the storage upload / delete routes use it.
/// * `AdminOnly` — legacy `req->app-id-authed!` (routes.clj:59-76): the
///   bearer admin token is required and the `as-*` headers are ignored.
///   Every other `/admin/*` route (auth, users, presence, signed urls, file
///   list) uses it: without this gate an app id alone (public in every
///   client bundle) would mint refresh tokens, read magic codes and delete
///   users.
/// * `AdminThenImpersonating` — the perms-check routes, which call
///   `req->app-id-authed!` first ("make sure the admin has access") and then
///   `get-perms!` (routes.clj:220-221, :325-326).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Gate {
    Impersonating,
    AdminOnly,
    AdminThenImpersonating,
}

/// `get-perms!`: impersonation headers honored (see [`Gate`]).
pub async fn authed(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
) -> Result<AdminCtx> {
    authed_with(state, headers, params, Gate::Impersonating).await
}

/// `req->app-id-authed!`: admin token required, `as-*` headers ignored.
pub async fn authed_admin(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
) -> Result<AdminCtx> {
    authed_with(state, headers, params, Gate::AdminOnly).await
}

/// `req->app-id-authed!` then `get-perms!` (the perms-check routes).
pub async fn authed_admin_then_impersonating(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
) -> Result<AdminCtx> {
    authed_with(state, headers, params, Gate::AdminThenImpersonating).await
}

/// legacy req->bearer-token!: the header is a required param, then the
/// token row must exist (routes.clj:69-72, util/http.clj:22-25)
async fn check_admin_header(state: &AppState, app_id: Uuid, header: Option<&str>) -> Result<()> {
    let header = header.ok_or_else(|| {
        InstantError::new(
            "param-missing",
            400,
            "Missing parameter: [\"headers\" \"authorization\"]",
            Some(json!({"in": ["headers", "authorization"]})),
        )
    })?;
    let token = header
        .strip_prefix("Bearer ")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            InstantError::new(
                "param-malformed",
                400,
                "Malformed parameter: [\"headers\" \"authorization\"]",
                Some(json!({"in": ["headers", "authorization"], "original-input": header})),
            )
        })?;
    if !auth::check_admin_token(state, app_id, token).await? {
        return Err(admin_token_not_found(app_id, token));
    }
    Ok(())
}

async fn authed_with(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
    gate: Gate,
) -> Result<AdminCtx> {
    let app_id = app_id_param(headers, params)?;

    // Per-app limit on all /admin/* routes (legacy with-rate-limiting,
    // docs/ADMIN.md §2). Checked before token auth so a hammering client
    // can't run a DB lookup per request.
    state
        .limiters
        .admin
        .check(app_id, 1.0)
        .map_err(crate::rate_limit::rate_limited_err)?;

    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

    // request.ip / request.origin for rules come from this HTTP request
    // (legacy binds *request-info* around every handler, util/http.clj:84-118)
    let request = crate::ws::request_ctx_from_headers(headers);
    let ctx = |admin: bool, user_id: Option<Uuid>| AdminCtx {
        app_id,
        perms: PermsCtx {
            admin,
            user_id,
            user_map: None,
            rule_params: None,
            ip: request.ip.clone(),
            origin: request.origin.clone(),
        },
    };

    match gate {
        Gate::AdminOnly => {
            check_admin_header(state, app_id, auth_header).await?;
            return Ok(ctx(true, None));
        }
        Gate::AdminThenImpersonating => {
            check_admin_header(state, app_id, auth_header).await?;
        }
        Gate::Impersonating => {}
    }

    let as_token = headers.get("as-token").and_then(|v| v.to_str().ok());
    let as_email = headers.get("as-email").and_then(|v| v.to_str().ok());
    let as_guest = headers.get("as-guest").is_some();

    // impersonation (as-token/as-guest work without a valid admin token)
    if let Some(token) = as_token {
        // legacy coerces the header to a uuid first (routes.clj:86-91)
        if Uuid::parse_str(token).is_err() {
            return Err(InstantError::new(
                "param-malformed",
                400,
                "Malformed parameter: [\"asUser\" \"token\"]",
                Some(json!({"in": ["asUser", "token"], "original-input": token})),
            ));
        }
        let user = auth::user_by_refresh_token(state, app_id, token)
            .await?
            .ok_or_else(|| app_user_not_found(json!({"app-id": app_id, "refresh-token": token})))?;
        return Ok(ctx(false, Some(user.id)));
    }
    if let Some(email) = as_email {
        if gate == Gate::Impersonating {
            check_admin_header(state, app_id, auth_header).await?;
        }
        let user = auth::user_by_email(state, app_id, email)
            .await?
            .ok_or_else(|| app_user_not_found(json!({"app-id": app_id, "email": email})))?;
        return Ok(ctx(false, Some(user.id)));
    }
    if as_guest {
        return Ok(ctx(false, None));
    }
    if gate == Gate::Impersonating {
        check_admin_header(state, app_id, auth_header).await?;
    }
    Ok(ctx(true, None))
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
    node: &EntityNode,
    attrs: &AttrMap,
    forms: Option<&[instaql::Form]>,
    inference: bool,
) -> Value {
    let mut m = Map::new();
    // legacy builds the object from the entity's triples, so `id` is only
    // there when the etype has an id attr: a link auto-created for a bare
    // label (`link docs.owner`) has reverse etype `owner`, which has no
    // attrs, and its nested entities come back as `{}`
    if attrs.id_attr_of(&node.etype).is_some() {
        m.insert("id".into(), json!(node.eid));
    }
    // `$files` rows arrive with their synthetic `url` triple already in
    // place (service::inject_file_urls), and like legacy keep `location-id`
    // unless a fields projection dropped it.
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
        // legacy triples->map keeps every triple, nulls included (indexed
        // attrs backfill a null triple, so `score: null` is on the wire)
        m.insert(attr.label.clone(), t.v.clone());
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

pub fn object_tree(result: &QueryResult, attrs: &AttrMap, q: &Value, inference: bool) -> Value {
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
            .map(|e| node_to_object(e, attrs, f.map(|f| f.children.as_slice()), inference))
            .collect();
        out.insert(form.k.clone(), Value::Array(vals));
    }
    Value::Object(out)
}

/// `result-meta` of a tree-shaped result: page-info and aggregate keyed by
/// top-level form, both maps always present (legacy
/// util/instaql.clj instaql-nodes->object-meta; read by
/// admin/src/subscribe.ts formatPageInfo).
pub fn object_meta(result: &QueryResult) -> Value {
    let mut page_info = Map::new();
    let mut aggregate = Map::new();
    for form in &result.forms {
        if let Some(pi) = &form.page_info {
            page_info.insert(
                form.k.clone(),
                json!({
                    "start-cursor": pi.start_cursor,
                    "end-cursor": pi.end_cursor,
                    "has-next-page?": pi.has_next_page,
                    "has-previous-page?": pi.has_previous_page,
                }),
            );
        }
        if let Some(count) = form.aggregate {
            aggregate.insert(form.k.clone(), json!({"count": count}));
        }
    }
    json!({"page-info": page_info, "aggregate": aggregate})
}

/// AuthCtx for the perms-check debug routes: the request's own ip/origin,
/// overridable per call (`ip-override` / `origin-override`; legacy
/// admin/routes.clj:230-246, :332-363; non-blank strings only).
fn perms_check_auth_ctx(
    state: &AppState,
    perms: &PermsCtx,
    body: &Value,
) -> instant_core::perms::AuthCtx {
    let mut auth = perms.auth_ctx(state);
    let non_blank = |k: &str| {
        body.get(k)
            .and_then(|v| v.as_str())
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
    };
    if let Some(ip) = non_blank("ip-override") {
        auth.request.ip = Some(ip);
    }
    if let Some(origin) = non_blank("origin-override") {
        auth.request.origin = Some(origin);
    }
    auth
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
    let q = body_query(body)?;
    let inference = body
        .get("inference?")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    service::assert_read_allowed(state, ctx.app_id).await?;
    let attrs = service::load_attrs(state, ctx.app_id).await?;
    let result = service::run_query(state, ctx.app_id, &attrs, &ctx.perms, q).await?;
    Ok(object_tree(&result, &attrs, q, inference))
}

// ---------------------------------------------------------------------------
// /admin/transact — admin steps grammar translation

fn eid_to_lookup(
    attrs: &AttrMap,
    etype: &str,
    v: &Value,
    new_attrs: &mut Vec<Value>,
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
                let attr_id = resolve_lookup_attr(attrs, etype, attr_name, &value, v, new_attrs)?;
                return Ok(json!([attr_id, value]));
            }
            Err(invalid_eid(s))
        }
        Value::Array(arr) if arr.len() == 2 => {
            let attr_name = arr[0].as_str().ok_or_else(|| invalid_eid(&v.to_string()))?;
            if Uuid::parse_str(attr_name).is_ok() {
                return Ok(v.clone());
            }
            let attr_id = resolve_lookup_attr(attrs, etype, attr_name, &arr[1], v, new_attrs)?;
            Ok(json!([attr_id, arr[1]]))
        }
        Value::Object(m) if m.len() == 1 => {
            let (attr_name, value) = m.iter().next().unwrap();
            let attr_id = resolve_lookup_attr(attrs, etype, attr_name, value, v, new_attrs)?;
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

fn lookup_err(message: String, input: Value) -> InstantError {
    // legacy admin/model.clj throw-validation-err! :lookup <input> [...]
    let mut e =
        InstantError::validation_failed("lookup", message.clone(), json!([{"message": message}]));
    if let Some(Value::Object(h)) = e.hint.as_mut() {
        h.insert("input".into(), input);
    }
    e
}

fn pending_attr_id(new_attrs: &[Value], etype: &str, label: &str) -> Option<Uuid> {
    new_attrs.iter().find_map(|na| {
        let obj = na.get(1)?;
        let fwd = obj.get("forward-identity")?;
        (fwd.get(1)?.as_str() == Some(etype) && fwd.get(2)?.as_str() == Some(label))
            .then(|| {
                obj.get("id")?
                    .as_str()
                    .and_then(|s| Uuid::parse_str(s).ok())
            })
            .flatten()
    })
}

/// Legacy `extract-lookup` (admin/model.clj:31-93). A dotted name that is
/// not itself an attr (`ref-lookup?`) is a ref lookup: `owner.id` names the
/// unique forward link `<etype>.owner`, matched on the linked entity's id.
/// Missing attrs are auto-created (schemaless): a plain lookup becomes a
/// unique indexed blob, a ref lookup a unique indexed cardinality-one link
/// (`add-attrs-for-ref-lookup`); `throw-on-missing-attrs?` is checked once
/// every step is translated (legacy `transform`), see [`translate_steps`].
/// Error hints echo legacy's `input`: the raw eid for the unique check, the
/// `[name, value]` pair for a bad ref path.
fn resolve_lookup_attr(
    attrs: &AttrMap,
    etype: &str,
    attr_name: &str,
    value: &Value,
    raw_eid: &Value,
    new_attrs: &mut Vec<Value>,
) -> Result<Uuid> {
    let ref_lookup = attr_name.contains('.')
        && attrs.by_fwd_name(etype, attr_name).is_none()
        && pending_attr_id(new_attrs, etype, attr_name).is_none();
    let label = if ref_lookup {
        let mut parts = attr_name.split('.');
        let fwd_name = parts.next().unwrap_or_default();
        let id_ident = parts.next();
        if id_ident != Some("id") || parts.next().is_some() {
            return Err(lookup_err(
                format!("{attr_name} is not a valid lookup attribute."),
                json!([attr_name, value]),
            ));
        }
        fwd_name
    } else {
        attr_name
    };
    if let Some(a) = attrs.by_fwd_name(etype, label) {
        if !a.is_unique {
            return Err(lookup_err(
                format!("{attr_name} is not a unique attribute on {etype}"),
                raw_eid.clone(),
            ));
        }
        return Ok(a.id);
    }
    if let Some(id) = pending_attr_id(new_attrs, etype, label) {
        return Ok(id);
    }
    if ref_lookup && attrs.by_rev_name(etype, label).is_some() {
        // legacy only seeks the forward name and never auto-creates over an
        // existing reverse link: `(:unique? nil)` fails the unique check
        return Err(lookup_err(
            format!("{attr_name} is not a unique attribute on {etype}"),
            raw_eid.clone(),
        ));
    }
    let id = Uuid::new_v4();
    if ref_lookup {
        new_attrs.push(json!(["add-attr", {
            "id": id,
            "forward-identity": [Uuid::new_v4(), etype, label],
            "reverse-identity": [Uuid::new_v4(), label, etype],
            "value-type": "ref", "cardinality": "one",
            "unique?": true, "index?": true
        }]));
    } else {
        new_attrs.push(json!(["add-attr", {
            "id": id,
            "forward-identity": [Uuid::new_v4(), etype, label],
            "value-type": "blob", "cardinality": "one",
            "unique?": true, "index?": true
        }]));
    }
    Ok(id)
}

/// Legacy `transform` (admin/model.clj:381-396): with
/// `throw-on-missing-attrs?`, every attr the steps would have auto-created
/// is reported at once, via throw-validation-err! :steps <steps>.
fn missing_attrs_err(names: &[String], steps: &Value) -> InstantError {
    InstantError::new(
        "validation-failed",
        400,
        "Validation failed for steps: Attributes are missing in your schema",
        Some(json!({
            "data-type": "steps",
            "input": steps,
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
    let id = Uuid::new_v4();
    // legacy admin/model.clj:281-284: the `id` attr is created unique+indexed
    let is_id = label == "id";
    new_attrs.push(json!(["add-attr", {
        "id": id,
        "forward-identity": [Uuid::new_v4(), etype, label],
        "value-type": "blob", "cardinality": "one",
        "unique?": is_id, "index?": is_id
    }]));
    Ok(id)
}

fn resolve_link_attr(
    attrs: &AttrMap,
    etype: &str,
    label: &str,
    new_attrs: &mut Vec<Value>,
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
                let id_attr = resolve_obj_attr(attrs, etype, "id", &mut new_attrs)?;
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
                    let attr_id = resolve_obj_attr(attrs, etype, label, &mut new_attrs)?;
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
                // legacy with-id-attr-for-lookup (admin/model.clj:95-103): a
                // lookup eid gets its `id` triple first, so the entity is
                // created (and its create rule runs) when it doesn't exist
                if eid.is_array() {
                    let id_attr = resolve_obj_attr(attrs, etype, "id", &mut new_attrs)?;
                    out.push(json!(["add-triple", eid, id_attr, eid]));
                }
                for (label, value) in &obj {
                    let (attr_id, forward) =
                        resolve_link_attr(attrs, etype, label, &mut new_attrs)?;
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
                        let target = eid_to_lookup(attrs, &target_etype, &target, &mut new_attrs)
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
    if throw_missing && !new_attrs.is_empty() {
        let names: Vec<String> = new_attrs
            .iter()
            .filter_map(|na| {
                let fwd = na.get(1)?.get("forward-identity")?;
                Some(format!(
                    "{}.{}",
                    fwd.get(1)?.as_str()?,
                    fwd.get(2)?.as_str()?
                ))
            })
            .collect();
        return Err(missing_attrs_err(&names, steps));
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
    let ctx = authed_admin(state, headers, params).await?;
    let email = body.get("email").and_then(|v| v.as_str());
    let id = body
        .get("id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok());
    // legacy refresh-tokens-post (admin/routes.clj:398-427): assert-signup!
    // with skip-perm-check? validates extra-fields against the $users schema
    let extra_fields = auth::extra_fields_of(body, "extra-fields");
    let attrs = service::load_attrs(state, ctx.app_id).await?;
    auth::validate_extra_fields(&attrs, extra_fields)?;
    let (user_id, created) = match (email, id) {
        (Some(email), _) => match auth::user_by_email(state, ctx.app_id, email).await? {
            Some(u) => (u.id, false),
            None => {
                let uid = Uuid::new_v4();
                let mut steps = vec![
                    json!(["add-triple", uid, sc::attr_id("$users", "id"), uid]),
                    json!(["add-triple", uid, sc::attr_id("$users", "email"), email]),
                ];
                steps.extend(auth::extra_field_steps(&attrs, uid, extra_fields));
                service::run_system_transact(state, ctx.app_id, &Value::Array(steps)).await?;
                (uid, true)
            }
        },
        (None, Some(id)) => match auth::user_by_id(state, ctx.app_id, id).await? {
            Some(u) => (u.id, false),
            None => {
                let mut steps = vec![json!(["add-triple", id, sc::attr_id("$users", "id"), id])];
                steps.extend(auth::extra_field_steps(&attrs, id, extra_fields));
                service::run_system_transact(state, ctx.app_id, &Value::Array(steps)).await?;
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
    let ctx = authed_admin(state, headers, params).await?;
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
    let ctx = authed_admin(state, headers, params).await?;
    let user = find_user_by_params(state, ctx.app_id, params).await?;
    match user {
        Some(u) => {
            let user = user_json(state, ctx.app_id, u.id, None).await?;
            Ok(json!({"user": user}))
        }
        // legacy app-users-get uses the non-throwing getter (routes.clj:490-492)
        None => Ok(json!({"user": null})),
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
    let ctx = authed_admin(state, headers, params).await?;
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
        // legacy app-users-delete: nothing to delete is `{deleted: null}` (routes.clj:494-499)
        None => Ok(json!({"deleted": null})),
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
    let ctx = authed_admin(state, headers, params).await?;
    // legacy presence-get (admin/routes.clj:739-765): `room-type` is
    // required (unused) alongside `room-id`, and every peer's stored
    // `{id}` user is replaced with its current $users entity
    let get_param = |name: &str| -> Result<String> {
        match params.get(name) {
            None => Err(InstantError::new(
                "param-missing",
                400,
                format!("Missing parameter: [\"params\" \"{name}\"]"),
                Some(json!({"in": ["params", name]})),
            )),
            Some(v) if v.trim().is_empty() => Err(InstantError::new(
                "param-malformed",
                400,
                format!("Malformed parameter: [\"params\" \"{name}\"]"),
                Some(json!({"in": ["params", name], "original-input": v})),
            )),
            Some(v) => Ok(v.clone()),
        }
    };
    let _room_type = get_param("room-type")?;
    let room_id = get_param("room-id")?;
    let mut snapshot = crate::presence::room_snapshot(state, ctx.app_id, &room_id).await?;
    let attrs = service::load_attrs(state, ctx.app_id).await?;
    let mut conn = state.pool.acquire().await.map_err(InstantError::from)?;
    let mut users: HashMap<Uuid, Value> = HashMap::new();
    if let Some(sessions) = snapshot.as_object_mut() {
        for sess in sessions.values_mut() {
            let Some(uid) = sess
                .get("user")
                .and_then(|u| u.get("id"))
                .and_then(|v| v.as_str())
                .and_then(|s| Uuid::parse_str(s).ok())
            else {
                continue;
            };
            if !users.contains_key(&uid) {
                let entity = instant_core::perms::fetch_entity_map(
                    &mut conn, ctx.app_id, &attrs, "$users", uid,
                )
                .await?
                .map(|m| {
                    // legacy get-entities: only the triples the row
                    // has, plus id
                    Value::Object(m.into_iter().filter(|(_, v)| !v.is_null()).collect())
                })
                .unwrap_or(Value::Null);
                users.insert(uid, entity);
            }
            sess["user"] = users.get(&uid).cloned().unwrap_or(Value::Null);
        }
    }
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
    // legacy upload-put (admin/routes.clj:628-643): path header, storage
    // perms skipped for the admin token, `create` rule for impersonation
    let path = headers
        .get("path")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .ok_or_else(|| {
            // legacy get-param! params ["path"] (admin/routes.clj:631)
            InstantError::new(
                "param-missing",
                400,
                "Missing parameter: [\"path\"]",
                Some(json!({"in": ["path"]})),
            )
        })?;
    if !ctx.perms.admin {
        check_files_perm(state, ctx.app_id, &ctx.perms, "create", &path).await?;
    }
    let meta = blob_meta_from_headers(headers)?;
    store_file(state, ctx.app_id, &path, &body, &meta).await
}

/// Upload metadata the way legacy coerces it (`coerce-content-type` drops
/// blank / "null" / "undefined"; blank disposition is dropped) and S3 fills
/// in its defaults, which then land on the `$files` row.
pub(crate) fn blob_meta_from_headers(headers: &HeaderMap) -> Result<crate::storage::BlobMeta> {
    let raw = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    };
    let non_blank = |s: Option<String>| s.map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    let content_type = non_blank(raw("content-type"))
        .filter(|s| s != "null" && s != "undefined")
        .unwrap_or_else(|| crate::storage::DEFAULT_CONTENT_TYPE.to_string());
    // get-optional-param! with coerce-non-blank-str: a present-but-blank
    // header is malformed rather than ignored
    let disposition_raw = raw("content-disposition");
    let content_disposition = match non_blank(disposition_raw.clone()) {
        Some(s) => s,
        None if disposition_raw.is_some() => {
            return Err(InstantError::new(
                "param-malformed",
                400,
                "Malformed parameter: [\"content-disposition\"]",
                Some(json!({"in": ["content-disposition"], "original-input": disposition_raw})),
            ))
        }
        None => crate::storage::DEFAULT_CONTENT_DISPOSITION.to_string(),
    };
    Ok(crate::storage::BlobMeta {
        content_type,
        content_disposition,
    })
}

/// Store a blob and upsert its `$files` row by path (legacy
/// `storage-coordinator/upload-file!` + `app-file-model/create!`: id, size,
/// content-type, content-disposition, location-id, key-version). Replacing a
/// path gives it a fresh location-id; the old blob is removed.
pub(crate) async fn store_file(
    state: &AppState,
    app_id: Uuid,
    path: &str,
    body: &[u8],
    meta: &crate::storage::BlobMeta,
) -> Result<Value> {
    let location_id = Uuid::new_v4().to_string();
    let size = crate::storage::put_blob(state, app_id, &location_id, body, meta).await?;

    let path_attr = sc::attr_id("$files", "path");
    let old_location = file_location_by_path(state, app_id, path).await;
    let lookup = json!([path_attr, path]);
    let steps = vec![
        json!(["add-triple", lookup, sc::attr_id("$files", "id"), lookup]),
        json!(["add-triple", lookup, sc::attr_id("$files", "path"), path]),
        json!(["add-triple", lookup, sc::attr_id("$files", "size"), size]),
        json!([
            "add-triple",
            lookup,
            sc::attr_id("$files", "content-type"),
            meta.content_type
        ]),
        json!([
            "add-triple",
            lookup,
            sc::attr_id("$files", "content-disposition"),
            meta.content_disposition
        ]),
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
    service::run_system_transact(state, app_id, &Value::Array(steps)).await?;

    // fetch the file entity id
    use sqlx::Row;
    let row = sqlx::query(
        "SELECT entity_id FROM triples
         WHERE app_id = $1 AND attr_id = $2 AND av AND value = to_jsonb($3::text) LIMIT 1",
    )
    .bind(app_id)
    .bind(path_attr)
    .bind(path)
    .fetch_one(&state.pool)
    .await
    .map_err(InstantError::from)?;
    let file_id: Uuid = row.get("entity_id");
    if let Some(old) = old_location {
        crate::storage::delete_blob(state, app_id, &old).await;
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
    let filename = admin_filename_param(params)?;
    if !ctx.perms.admin {
        check_files_perm(state, ctx.app_id, &ctx.perms, "delete", filename).await?;
    }
    let id = delete_file_by_path(state, ctx.app_id, filename).await?;
    Ok(json!({"data": {"id": id}}))
}

/// Delete the `$files` row at `path` and its blob; the deleted id, or None
/// when nothing was there (legacy `app-file-model/delete-by-path!`).
pub(crate) async fn delete_file_by_path(
    state: &AppState,
    app_id: Uuid,
    path: &str,
) -> Result<Option<Uuid>> {
    use sqlx::Row;
    let path_attr = sc::attr_id("$files", "path");
    let row = sqlx::query(
        "SELECT entity_id FROM triples
         WHERE app_id = $1 AND attr_id = $2 AND av AND value = to_jsonb($3::text) LIMIT 1",
    )
    .bind(app_id)
    .bind(path_attr)
    .bind(path)
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let id: Uuid = row.get("entity_id");
    let old = file_location_by_path(state, app_id, path).await;
    service::run_system_transact(state, app_id, &json!([["delete-entity", id, "$files"]])).await?;
    if let Some(old) = old {
        crate::storage::delete_blob(state, app_id, &old).await;
    }
    Ok(Some(id))
}

/// `POST /admin/storage/files/delete` — `db.storage.deleteMany` (legacy
/// files-delete, admin/routes.clj:654-661): `{filenames: [...]}` →
/// `{data: {ids: [...]}}` with only the ids that existed.
pub async fn storage_delete_many(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    Json(body): Json<Value>,
) -> Response {
    json_or_err(storage_delete_many_impl(&state, &headers, &params, &body).await)
}

async fn storage_delete_many_impl(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
    body: &Value,
) -> Result<Value> {
    let ctx = authed(state, headers, params).await?;
    let filenames: Vec<String> = match body.get("filenames") {
        None | Some(Value::Null) => {
            return Err(InstantError::new(
                "param-missing",
                400,
                "Missing parameter: [\"body\" \"filenames\"]",
                Some(json!({"in": ["body", "filenames"]})),
            ))
        }
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect(),
        Some(v) => {
            return Err(InstantError::new(
                "param-malformed",
                400,
                "Malformed parameter: [\"body\" \"filenames\"]",
                Some(json!({"in": ["body", "filenames"], "original-input": v})),
            ))
        }
    };
    if !ctx.perms.admin {
        for path in &filenames {
            check_files_perm(state, ctx.app_id, &ctx.perms, "delete", path).await?;
        }
    }
    let mut ids = vec![];
    for path in &filenames {
        if let Some(id) = delete_file_by_path(state, ctx.app_id, path).await? {
            ids.push(id);
        }
    }
    Ok(json!({"data": {"ids": ids}}))
}

/// `GET /admin/storage/files` — the deprecated `db.storage.list` (legacy
/// files-get, admin/routes.clj:719-737): the `$files` query in the old
/// StorageFile shape whose `key` is the S3 object key.
pub async fn storage_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    json_or_err(storage_list_impl(&state, &headers, &params).await)
}

async fn storage_list_impl(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
) -> Result<Value> {
    let ctx = authed_admin(state, headers, params).await?;
    service::assert_read_allowed(state, ctx.app_id).await?;
    let attrs = service::load_attrs(state, ctx.app_id).await?;
    let q = json!({"$files": {}});
    let result = service::run_query(state, ctx.app_id, &attrs, &ctx.perms, &q).await?;
    let tree = object_tree(&result, &attrs, &q, false);
    let files = tree
        .get("$files")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let data: Vec<Value> = files
        .iter()
        .map(|f| {
            let loc = f
                .get("location-id")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            json!({
                "key": crate::storage::object_key(ctx.app_id, loc),
                "name": f.get("path").cloned().unwrap_or(Value::Null),
                "size": f.get("size").cloned().unwrap_or(Value::Null),
                "etag": null,
                "last_modified": null,
            })
        })
        .collect();
    Ok(json!({"data": data}))
}

/// `GET /admin/storage/signed-download-url?filename=` — deprecated
/// `db.storage.getDownloadUrl` (admin/routes.clj:710-716): perms skipped,
/// unknown path → `{data: null}`.
pub async fn admin_signed_download_url(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    json_or_err(admin_signed_download_url_impl(&state, &headers, &params).await)
}

async fn admin_signed_download_url_impl(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
) -> Result<Value> {
    let ctx = authed_admin(state, headers, params).await?;
    let filename = admin_filename_param(params)?;
    let url = file_location_by_path(state, ctx.app_id, filename)
        .await
        .map(|loc| crate::storage::download_url(state, ctx.app_id, &loc));
    Ok(json!({"data": url}))
}

/// Deprecated upload-URL flow (`db.storage.upload` / core `upload`): the
/// server hands out a single-use `<origin>/storage/<id>/consume-upload-url`
/// backed by the legacy `app_upload_urls` table (5-minute expiry).
async fn create_upload_url(state: &AppState, app_id: Uuid, path: &str) -> Result<String> {
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO app_upload_urls (id, app_id, path) VALUES ($1, $2, $3)")
        .bind(id)
        .bind(app_id)
        .bind(path)
        .execute(&state.pool)
        .await
        .map_err(InstantError::from)?;
    Ok(format!(
        "{}/storage/{}/consume-upload-url",
        state.cfg.base_url, id
    ))
}

/// `POST /admin/storage/signed-upload-url` (admin/routes.clj:702-708).
pub async fn admin_signed_upload_url(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    Json(body): Json<Value>,
) -> Response {
    json_or_err(admin_signed_upload_url_impl(&state, &headers, &params, &body).await)
}

async fn admin_signed_upload_url_impl(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
    body: &Value,
) -> Result<Value> {
    let ctx = authed_admin(state, headers, params).await?;
    let filename = body
        .get("filename")
        .and_then(|v| v.as_str())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            InstantError::new(
                "param-missing",
                400,
                "Missing parameter: [\"body\" \"filename\"]",
                Some(json!({"in": ["body", "filename"]})),
            )
        })?;
    let url = create_upload_url(state, ctx.app_id, filename).await?;
    Ok(json!({"data": url}))
}

/// `POST /storage/signed-upload-url` (storage/routes.clj:46-52): app id and
/// path come from the JSON body, the user from the bearer refresh token,
/// and the `create` rule applies.
pub async fn client_signed_upload_url(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    Json(body): Json<Value>,
) -> Response {
    json_or_err(client_signed_upload_url_impl(&state, &headers, &params, &body).await)
}

async fn client_signed_upload_url_impl(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
    body: &Value,
) -> Result<Value> {
    let mut merged = params.clone();
    if let Some(a) = body
        .get("app-id")
        .or_else(|| body.get("app_id"))
        .and_then(|v| v.as_str())
    {
        merged.insert("app_id".into(), a.to_string());
    }
    let (app_id, perms) = client_storage_ctx(state, headers, &merged).await?;
    state
        .limiters
        .storage_upload
        .check(app_id, 1.0)
        .map_err(crate::rate_limit::rate_limited_err)?;
    let path = body
        .get("path")
        .or_else(|| body.get("filename"))
        .and_then(|v| v.as_str())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| client_param_missing("path", "filename"))?;
    check_files_perm(state, app_id, &perms, "create", path).await?;
    let url = create_upload_url(state, app_id, path).await?;
    Ok(json!({"data": url}))
}

/// `PUT /storage/:upload-id/consume-upload-url` (storage/routes.clj:54-62):
/// consumes the single-use row, rejects expired ones, then uploads with
/// perms skipped (they were checked when the URL was issued).
pub async fn consume_upload_url(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(upload_id): axum::extract::Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    json_or_err(consume_upload_url_impl(&state, &upload_id, &headers, body).await)
}

async fn consume_upload_url_impl(
    state: &AppState,
    upload_id: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<Value> {
    let upload_id = Uuid::parse_str(upload_id).map_err(|_| {
        InstantError::new(
            "param-malformed",
            400,
            "Malformed parameter: [\"params\" \"upload-id\"]",
            Some(json!({"in": ["params", "upload-id"], "original-input": upload_id})),
        )
    })?;
    use sqlx::Row;
    let row = sqlx::query(
        "DELETE FROM app_upload_urls WHERE id = $1
         RETURNING app_id, path, (expired_at < now()) AS expired",
    )
    .bind(upload_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    let (app_id, path) = match row {
        Some(r) if !r.get::<bool, _>("expired") => {
            (r.get::<Uuid, _>("app_id"), r.get::<String, _>("path"))
        }
        _ => {
            // legacy throw-validation-err! with a bare string (coordinator.clj:125-129)
            return Err(InstantError::new(
                "validation-failed",
                400,
                "Validation failed for app-upload-url",
                Some(json!({
                    "data-type": "app-upload-url",
                    "input": upload_id,
                    "errors": "The upload URL is expired or invalid.",
                })),
            ));
        }
    };
    // only the content type travels with a presigned-style PUT
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .unwrap_or(crate::storage::DEFAULT_CONTENT_TYPE)
        .to_string();
    let meta = crate::storage::BlobMeta {
        content_type,
        content_disposition: crate::storage::DEFAULT_CONTENT_DISPOSITION.to_string(),
    };
    store_file(state, app_id, &path, &body, &meta).await
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
    let ctx = authed_admin(state, headers, params).await?;
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

pub async fn admin_send_magic_code(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    Json(body): Json<Value>,
) -> Response {
    json_or_err(admin_send_magic_code_impl(&state, &headers, &params, &body).await)
}

/// Legacy send-magic-code-post (admin/routes.clj:506-511): admin-authed, then
/// the same generate + deliver path as the runtime route.
async fn admin_send_magic_code_impl(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
    body: &Value,
) -> Result<Value> {
    let ctx = authed_admin(state, headers, params).await?;
    let email = body.get("email").and_then(|v| v.as_str()).ok_or_else(|| {
        InstantError::new(
            "param-missing",
            400,
            "Missing parameter: [\"body\" \"email\"]",
            Some(json!({"in": ["body", "email"]})),
        )
    })?;
    let email = crate::routes::runtime::coerce_email_pub(email)?;
    state
        .limiters
        .magic_code_send
        .check((ctx.app_id, email.clone()), 1.0)
        .map_err(crate::rate_limit::email_rate_limited_err)?;
    crate::routes::runtime::send_magic_code_for(state, ctx.app_id, &email).await
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
    let ctx = authed_admin(state, headers, params).await?;
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
    body: &Value,
) -> Result<Value> {
    let ctx = authed_admin(state, headers, params).await?;
    let uid = Uuid::new_v4();
    // legacy sign-in-guest-post (admin/routes.clj:534-554): extra-fields are
    // validated (no rule check) and written with the guest
    let extra_fields = auth::extra_fields_of(body, "extra-fields");
    let attrs = service::load_attrs(state, ctx.app_id).await?;
    auth::validate_extra_fields(&attrs, extra_fields)?;
    let mut steps = vec![
        json!(["add-triple", uid, sc::attr_id("$users", "id"), uid]),
        json!(["add-triple", uid, sc::attr_id("$users", "type"), "guest"]),
    ];
    steps.extend(auth::extra_field_steps(&attrs, uid, extra_fields));
    service::run_system_transact(state, ctx.app_id, &Value::Array(steps)).await?;
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
        .ok_or_else(|| client_param_missing("app-id", "app_id"))?;
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
    let request = crate::ws::request_ctx_from_headers(headers);
    Ok((
        app_id,
        PermsCtx {
            admin: false,
            user_id,
            user_map: None,
            rule_params: None,
            ip: request.ip,
            origin: request.origin,
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
    let auth_ctx = perms.auth_ctx(state);
    let env = instant_core::perms::EvalEnv::new(app_id, &rules, &auth_ctx.request);
    // legacy binds `auth` as an AuthCelMap here too (coordinator.clj:19-38),
    // so `auth.ref('$user...')` resolves
    let attrs = service::load_attrs(state, app_id).await?;
    let auth_val =
        instant_core::perms::build_auth_value(&mut conn, app_id, &attrs, &auth_ctx, &[&program])
            .await?;
    let data = json!({"path": path});
    let ok = instant_core::perms::eval_program(&program, &data, None, &auth_val, &json!({}), &env)
        .await?;
    if !ok {
        // legacy assert-permitted! :has-storage-permission? (coordinator.clj:20-38)
        return Err(InstantError::new(
            "permission-denied",
            400,
            "Permission denied: not has-storage-permission?",
            Some(json!({"input": ["$files", action], "expected": "has-storage-permission?"})),
        ));
    }
    Ok(())
}

/// legacy `req->app-file!` names its params as `["path"]` (else `filename`)
/// and `["app-id"]` (else `app_id`) — storage/routes.clj:14-27.
fn client_param_missing(name: &str, alt: &str) -> InstantError {
    InstantError::new(
        "param-missing",
        400,
        format!("Missing parameter: [\"{name}\"]"),
        Some(json!({"in": [name], "possible-ins": [[alt]]})),
    )
}

/// legacy `get-param! req [:params :filename]` on the admin storage routes.
fn admin_filename_param(params: &HashMap<String, String>) -> Result<&str> {
    params
        .get("filename")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            InstantError::new(
                "param-missing",
                400,
                "Missing parameter: [\"params\" \"filename\"]",
                Some(json!({"in": ["params", "filename"]})),
            )
        })
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
    // legacy req->app-file! reads `path` or `filename` (storage/routes.clj:14-27)
    let path = headers
        .get("path")
        .or_else(|| headers.get("filename"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .ok_or_else(|| client_param_missing("path", "filename"))?;
    check_files_perm(state, app_id, &perms, "create", &path).await?;
    let meta = blob_meta_from_headers(headers)?;
    store_file(state, app_id, &path, &body, &meta).await
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
        .ok_or_else(|| client_param_missing("path", "filename"))?;
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
        .ok_or_else(|| client_param_missing("path", "filename"))?;
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
    let ctx = authed_admin_then_impersonating(state, headers, params).await?;
    if ctx.perms.admin {
        return Err(InstantError::validation_failed(
            "body",
            "Cannot test perms as admin",
            json!([{"message": "Cannot test perms as admin"}]),
        ));
    }
    let q = body_query(body)?;
    let inference = body
        .get("inference?")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    service::assert_read_allowed(state, ctx.app_id).await?;
    let attrs = service::load_attrs(state, ctx.app_id).await?;

    // run unfiltered, then evaluate view per top-level entity for check-results
    let admin_perms = PermsCtx {
        admin: true,
        ..PermsCtx::default()
    };
    let unfiltered = service::run_query(state, ctx.app_id, &attrs, &admin_perms, q).await?;

    let mut conn = state.pool.acquire().await.map_err(InstantError::from)?;
    let rules = match body.get("rules-override") {
        Some(code) if code.is_object() => instant_core::perms::Rules { code: code.clone() },
        _ => instant_core::perms::Rules::load(&mut conn, ctx.app_id).await?,
    };
    // debugQuery / debugTransact may override request.ip / request.origin
    // (admin/routes.clj:230-231, :332-333 ip-override / origin-override)
    let auth_ctx = perms_check_auth_ctx(state, &ctx.perms, body);
    let env = instant_core::perms::EvalEnv::new(ctx.app_id, &rules, &auth_ctx.request);
    let mut check_results = vec![];
    for form in &unfiltered.forms {
        let program = rules.program(&form.etype, "view");
        for e in &form.entities {
            // legacy `entity-map` (instaql.clj:1956-1963) re-fetches the whole
            // entity for the check regardless of any `fields` projection
            let mut record = serde_json::Map::new();
            record.insert("id".to_string(), json!(e.eid));
            for t in &e.triples {
                if let Some(a) = attrs.get(&t.a) {
                    if a.value_type == ValueType::Blob && a.label != "id" && !t.v.is_null() {
                        record.insert(a.label.clone(), t.v.clone());
                    }
                }
            }
            let data = instant_core::perms::fetch_entity_map(
                &mut conn,
                ctx.app_id,
                &attrs,
                &form.etype,
                e.eid,
            )
            .await?
            .unwrap_or_else(|| instant_core::perms::base_entity_map(&attrs, &form.etype, e.eid));
            let auth_val = instant_core::perms::build_auth_value(
                &mut conn,
                ctx.app_id,
                &attrs,
                &auth_ctx,
                &[&program],
            )
            .await?;
            let rule_params = q.get("$$ruleParams").cloned().unwrap_or(json!({}));
            let ok = instant_core::perms::eval_program(
                &program,
                &Value::Object(data),
                None,
                &auth_val,
                &rule_params,
                &env,
            )
            .await?;
            // legacy keys check results by [etype id label] (instaql.clj:2028-2037);
            // `label` is nil for the entity-level view check
            check_results.push(json!({
                "id": e.eid,
                "entity": form.etype,
                "label": null,
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
    let tree = object_tree(&result, &attrs, q, inference);
    Ok(json!({
        "check-results": check_results,
        "result": tree,
        // legacy's map of {etype -> {short-circuit? where-clauses rate-limits}}
        // from its rule-where rewriter (instaql.clj:2116-2152); rules are
        // evaluated per entity here, so no where clauses are ever derived
        "rule-wheres": {},
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
    let ctx = authed_admin_then_impersonating(state, headers, params).await?;
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
    let mut attrs = (*service::load_attrs(state, ctx.app_id).await?).clone();
    let tx_steps = translate_steps(&attrs, steps, throw_missing)?;
    let parsed = instant_core::tx::parse_tx_steps(&tx_steps)?;

    let mut dbtx = state.pool.begin().await.map_err(InstantError::from)?;
    let rules = match body.get("rules-override") {
        Some(code) if code.is_object() => instant_core::perms::Rules { code: code.clone() },
        _ => instant_core::perms::Rules::load(&mut dbtx, ctx.app_id).await?,
    };
    let auth_ctx = perms_check_auth_ctx(state, &ctx.perms, body);
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
        service::notify_tx(state, ctx.app_id, &service::TxNotice::from(&report)).await;
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
