//! /admin/* HTTP API, wire-compatible with @instantdb/admin.

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
use crate::routes::runtime::{
    body_email_param, coerce_email_value, coerce_uuid_value, err_response, json_or_err,
    optional_body_email, optional_body_uuid, optional_body_value, user_json,
};
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
    // get-some-param! malformed hint: the path found, every candidate path,
    // and the raw input (exception.clj:441-457)
    Uuid::parse_str(&raw).map_err(|_| {
        InstantError::new(
            "param-malformed",
            400,
            format!("Malformed parameter: [\"{}\" \"{}\"]", path[0], path[1]),
            Some(json!({
                "in": path,
                "possible-ins": [["headers", "app-id"], ["query-params", "app_id"]],
                "original-input": raw,
            })),
        )
    })
}

/// Legacy `boolean` coercion of an optional body flag: absent and null are
/// false, a JSON false is false, and every other JSON value (0, "", [], {})
/// is truthy, exactly like Clojure's `boolean` and `get-optional-param!`
/// (exception.clj:430-438). `.as_bool()` would silently read "false" as false
/// and reject the truthy values legacy accepts.
pub(crate) fn bool_param(body: &Value, key: &str) -> bool {
    match body.get(key) {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(_) => true,
    }
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

    // Per-app limit on all /admin/* routes (legacy with-rate-limiting).
    // Checked before token auth so a hammering client
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
    form: Option<&instaql::Form>,
    inference: bool,
) -> Value {
    let mut m = Map::new();
    // legacy builds the object from the entity's triples, so `id` is only
    // there when the entity actually has its id triple: a link auto-created
    // for a bare label (`link docs.owner`) has reverse etype `owner`, which
    // has no attrs, and a target created by a link step alone can have none
    // even when its etype has an id attr; both come back without `id`
    let has_id_triple = node
        .triples
        .iter()
        .any(|t| attrs.get(&t.a).map(|a| a.label == "id").unwrap_or(false));
    if has_id_triple {
        m.insert("id".into(), json!(node.eid));
    }
    // `$files` rows arrive with their synthetic `url` triple already in
    // place (service::inject_file_urls), and like legacy keep `location-id`
    // unless a fields projection dropped it.
    for t in &node.triples {
        let Some(attr) = attrs.get(&t.a) else {
            continue;
        };
        // legacy's object tree runs over the row fields, not the wider socket
        // projection: a `fields` list that omits the order attr does not get
        // that attr back (the socket result does, see instaql.rs fetch_entities)
        if let (Some(fields), Some(order_key)) = (
            form.and_then(|f| f.opts.fields.as_ref()),
            form.and_then(|f| f.opts.order.as_ref()).map(|o| &o.key),
        ) {
            if &attr.label == order_key && !fields.contains(&attr.label) {
                continue;
            }
        }
        // legacy triples->map labels every triple of the entity, its forward
        // links included: a link the query doesn't nest shows up as the
        // linked id (`owner: "<uuid>"`, the last triple for a many link); a
        // nested form of the same label replaces it below (obj-node merges
        // the ref entries over the blob entries)
        if attr.value_type != ValueType::Blob && attr.etype != node.etype {
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
        let child_form = form.and_then(|f| f.children.iter().find(|c| c.k == child.k));
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
            .map(|(e, _)| node_to_object(e, attrs, child_form, inference))
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
            // legacy `(first entries)` of an empty link is nil, and JSON
            // encodes the key as null (instaql.clj:88-97)
            m.insert(
                child.k.clone(),
                vals.into_iter().next().unwrap_or(Value::Null),
            );
        } else {
            m.insert(child.k.clone(), Value::Array(vals));
        }
    }
    Value::Object(m)
}

/// Legacy `sort-entries` (util/instaql.clj:64-87): every object-tree level is
/// ordered by the form's order field (default `$serverCreatedAt`, the id
/// triple's timestamp) and ties are broken by the entity id
/// (`uuid-util/pg-compare`, the same unsigned byte order as Postgres' uuid
/// comparison). A checked-data-type `date` field sorts by instant, not by the
/// raw jsonb string.
fn sort_entities(
    entities: &mut [(&EntityNode, i64)],
    form: Option<&instaql::Form>,
    attrs: &AttrMap,
) {
    use std::cmp::Ordering;
    if entities.is_empty() {
        return;
    }
    let order = form.and_then(|f| f.opts.order.clone());
    let key = order
        .as_ref()
        .map(|o| o.key.clone())
        .unwrap_or_else(|| "serverCreatedAt".to_string());
    let desc = matches!(order.as_ref().map(|o| o.dir), Some(instaql::Dir::Desc));
    // legacy sort-entries runs over the object entries, after `fields` has
    // selected the row fields: an order attr the projection leaves out is
    // absent from every entry, so the sort key is nil for all and only the id
    // tie-break remains.
    let field_hidden = form
        .and_then(|f| f.opts.fields.as_ref())
        .map(|fields| key != "serverCreatedAt" && !fields.contains(&key))
        .unwrap_or(false);
    let sort_value = |node: &EntityNode, created: i64| -> Value {
        if key == "serverCreatedAt" {
            return json!(created);
        }
        if field_hidden {
            return Value::Null;
        }
        node.triples
            .iter()
            .find(|t| attrs.get(&t.a).map(|x| x.label == key).unwrap_or(false))
            .map(|t| t.v.clone())
            .unwrap_or(Value::Null)
    };
    let checked = (key != "serverCreatedAt")
        .then(|| attrs.by_fwd_name(&entities[0].0.etype, &key))
        .flatten()
        .and_then(|a| a.checked_data_type);
    let cmp = |av: &Value, bv: &Value| -> Ordering {
        match checked {
            Some(instant_core::attr::CheckedDataType::Number)
                if av.is_number() && bv.is_number() =>
            {
                av.as_f64()
                    .unwrap_or(0.0)
                    .partial_cmp(&bv.as_f64().unwrap_or(0.0))
                    .unwrap_or(Ordering::Equal)
            }
            Some(instant_core::attr::CheckedDataType::String)
                if av.is_string() && bv.is_string() =>
            {
                av.as_str()
                    .unwrap_or_default()
                    .cmp(bv.as_str().unwrap_or_default())
            }
            Some(instant_core::attr::CheckedDataType::Boolean)
                if av.is_boolean() && bv.is_boolean() =>
            {
                av.as_bool()
                    .unwrap_or(false)
                    .cmp(&bv.as_bool().unwrap_or(false))
            }
            Some(instant_core::attr::CheckedDataType::Date) if !av.is_null() && !bv.is_null() => {
                fn instant(v: &Value) -> Option<i64> {
                    match v {
                        Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
                        Value::String(s) => instant_core::cel_ext::parse_date_string(s)
                            .map(|t| t.timestamp_millis()),
                        _ => None,
                    }
                }
                match (instant(av), instant(bv)) {
                    (Some(x), Some(y)) => x.cmp(&y),
                    _ => cmp_json(av, bv),
                }
            }
            _ => cmp_json(av, bv),
        }
    };
    entities.sort_by(|(a, at), (b, bt)| {
        let ord = cmp(&sort_value(a, *at), &sort_value(b, *bt)).then_with(|| a.eid.cmp(&b.eid));
        if desc {
            ord.reverse()
        } else {
            ord
        }
    });
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
        // legacy instaql-nodes->object-tree sorts every level through
        // sort-entries, including the top-level form (util/instaql.clj:64-97).
        // The SQL order uses the database collation for typed strings, while
        // legacy's object tree re-sorts with Clojure compare; sorting here
        // keeps the admin object tree (and its ties) on the legacy side of
        // that difference.
        let mut entities: Vec<(&EntityNode, i64)> = form
            .entities
            .iter()
            .map(|e| (e, entity_created_at(e, attrs)))
            .collect();
        sort_entities(&mut entities, f, attrs);
        let vals: Vec<Value> = entities
            .iter()
            .map(|(e, _)| node_to_object(e, attrs, f, inference))
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
    let inference = bool_param(body, "inference?");
    service::assert_read_allowed(state, ctx.app_id).await?;
    let attrs = service::load_attrs(state, ctx.app_id).await?;
    // legacy query-post (admin/routes.clj:295-316): a query that outlives
    // the statement timeout is `timeout`
    let result = tokio::time::timeout(
        service::query_timeout(),
        service::run_query(state, ctx.app_id, &attrs, &ctx.perms, q),
    )
    .await
    .map_err(|_| InstantError::query_timeout())??;
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
            // Legacy treats the first element as an attribute name even when
            // it is a UUID string (admin/model.clj eid->lookup-pair ->
            // extract-lookup). That auto-creates `etype.<uuid-label>`, whose
            // label tx/coerce! then turns into a UUID and the attr spec
            // rejects. [`uuid_attr_label_index`] reports that same tx-steps
            // validation error after translation.
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

/// `ex/get-param! req [:body :steps] vec` (admin/routes.clj:267,335): absent
/// is param-missing, present but not an array is param-malformed.
fn body_steps(body: &Value) -> Result<&Value> {
    let steps = body.get("steps").filter(|v| !v.is_null()).ok_or_else(|| {
        InstantError::new(
            "param-missing",
            400,
            "Missing parameter: [\"body\" \"steps\"]",
            Some(json!({"in": ["body", "steps"]})),
        )
    })?;
    if !steps.is_array() {
        return Err(InstantError::new(
            "param-malformed",
            400,
            "Malformed parameter: [\"body\" \"steps\"]",
            Some(json!({"in": ["body", "steps"], "original-input": steps})),
        ));
    }
    Ok(steps)
}

/// Legacy `throw-validation-err! :steps <steps> [{message, in [idx 2]}]`
/// (admin/model.clj:423): stamp the whole steps array and the position of
/// the offending entity id onto an `invalid_eid` error.
fn at_step(e: InstantError, steps: &Value, idx: usize, pos: usize) -> InstantError {
    let mut e = e;
    if let Some(h) = e.hint.as_mut().and_then(|h| h.as_object_mut()) {
        if h.get("data-type").and_then(|d| d.as_str()) == Some("steps") {
            h.insert("input".into(), steps.clone());
            if let Some(errs) = h.get_mut("errors").and_then(|v| v.as_array_mut()) {
                for err in errs.iter_mut() {
                    if let Some(m) = err.as_object_mut() {
                        m.insert("in".into(), json!([idx, pos]));
                    }
                }
            }
        }
    }
    e
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
        // legacy create-object-attr (admin/model.clj:280-284): the auto-created
        // `id` attr is unique but not indexed
        "unique?": is_id, "index?": false
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
    for (idx, step) in arr.iter().enumerate() {
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
                )
                .map_err(|e| at_step(e, steps, idx, 2))?;
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
                )
                .map_err(|e| at_step(e, steps, idx, 2))?;
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
                        // legacy expand-link (admin/model.clj): every array is a list of
                        // targets; a lookup target is a `lookup__` string, an object or an
                        // [attr, value] pair inside the array
                        Value::Array(a) => a.clone(),
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
                )
                .map_err(|e| at_step(e, steps, idx, 2))?;
                out.push(json!(["delete-entity", eid, etype]));
            }
            "ruleParams" => {
                let etype = sarr.get(1).and_then(|v| v.as_str()).unwrap_or_default();
                let eid = eid_to_lookup(
                    attrs,
                    etype,
                    sarr.get(2).unwrap_or(&Value::Null),
                    &mut new_attrs,
                )
                .map_err(|e| at_step(e, steps, idx, 2))?;
                out.push(json!([
                    "rule-params",
                    eid,
                    etype,
                    sarr.get(3).cloned().unwrap_or(json!({}))
                ]));
            }
            // legacy's ::ops spec admits only these raw schema ops beside the
            // admin actions above (admin/model.clj:468-492); the client
            // tx-steps (add-triple, retract-triple, deep-merge-triple,
            // delete-entity, restore-attr) are not part of this route and
            // fall through to the spec error below like any unknown action.
            "add-attr" | "update-attr" | "delete-attr" => {
                out.push(step.clone());
            }
            _ => {
                // legacy spec explain of ::ops (admin/model.clj:503): the
                // best problem is the last `s/or` branch's op literal
                return Err(InstantError::validation_failed_input(
                    "steps",
                    steps.clone(),
                    json!([{"expected": ["delete-attr"], "in": [idx, 0]}]),
                ));
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
    let selected = Value::Array(all);
    // Legacy auto-creates an attr for the `[<attr-uuid>, value]` lookup form,
    // then tx/coerce! UUIDifies its forward label and the attr spec rejects
    // it (`expected string?` at the label). This is how legacy refuses the
    // uuid-attr form on /admin/transact; its hint carries the whole
    // translated tx-steps, so match that rather than trusting the caller.
    if let Some(idx) = uuid_attr_label_index(selected.as_array().map(Vec::as_slice).unwrap_or(&[]))
    {
        return Err(InstantError::validation_failed_input(
            "tx-steps",
            selected,
            json!([{"expected": "string?", "in": [idx, 1, "forward-identity", 2]}]),
        ));
    }
    Ok(selected)
}

/// Index of the first `add-attr` whose forward label would be UUID-coerced by
/// legacy's `tx/coerce!`, making the attr spec fail. The admin `[attr-uuid,
/// value]` lookup form reaches this through the attr that
/// `create-missing-attrs` auto-creates for the UUID string (admin/model.clj
/// `add-attrs-for-lookup`), and a raw `add-attr` step with a UUID label does
/// the same. Only forward labels matter for the lookup gap; other UUID-valued
/// strings in an attr are not part of this route's contract.
fn uuid_attr_label_index(steps: &[Value]) -> Option<usize> {
    steps.iter().position(|step| {
        step.get(0).and_then(Value::as_str) == Some("add-attr")
            && step
                .get(1)
                .and_then(|attr| attr.get("forward-identity"))
                .and_then(|identity| identity.get(2))
                .and_then(Value::as_str)
                .is_some_and(|label| Uuid::parse_str(label).is_ok())
    })
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
    let steps = body_steps(body)?;
    let throw_missing = bool_param(body, "throw-on-missing-attrs?");
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
    // legacy refresh-tokens-post parses email then id with get-optional-param!
    // (admin/routes.clj:398-408), so an invalid value is param-malformed even
    // when the other identifier would have been enough.
    let email = optional_body_email(body, "email")?;
    let id = optional_body_uuid(body, "id")?;
    let email = email.as_deref();
    // legacy refresh-tokens-post (admin/routes.clj:398-427): assert-signup!
    // with skip-perm-check? validates extra-fields against the $users schema
    let extra_fields = auth::extra_fields_of(body, "extra-fields");
    let attrs = service::load_attrs(state, ctx.app_id).await?;
    let (user_id, created) = match (email, id) {
        (Some(email), _) => match auth::user_by_email(state, ctx.app_id, email).await? {
            Some(u) => (u.id, false),
            None => {
                auth::validate_extra_fields(&attrs, extra_fields)?;
                // legacy refresh-tokens-post uses the caller's id when given,
                // even on the email branch (admin/routes.clj:415)
                let uid = id.unwrap_or_else(Uuid::new_v4);
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
                auth::validate_extra_fields(&attrs, extra_fields)?;
                // Legacy create! receives :email nil on this branch (the
                // route always passes the optional email) and writes it as a
                // null email triple, so the returned user carries email: null.
                let mut steps = vec![
                    json!(["add-triple", id, sc::attr_id("$users", "id"), id]),
                    json!([
                        "add-triple",
                        id,
                        sc::attr_id("$users", "email"),
                        Value::Null
                    ]),
                ];
                steps.extend(auth::extra_field_steps(&attrs, id, extra_fields));
                service::run_system_transact(state, ctx.app_id, &Value::Array(steps)).await?;
                (id, true)
            }
        },
        _ => {
            return Err(InstantError::validation_failed_input(
                "body",
                body.clone(),
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
    // legacy sign-out-post's cond parses only the branch it takes
    // (admin/routes.clj:429-452), in id / email / refresh_token order.
    if let Some(raw) = optional_body_value(body, "id") {
        let id = coerce_uuid_value(raw, &["body", "id"])?;
        auth::sign_out(state, ctx.app_id, Some(id), None).await?;
    } else if let Some(raw) = optional_body_value(body, "email") {
        let email = coerce_email_value(raw, &["body", "email"])?;
        let user = auth::user_by_email(state, ctx.app_id, &email)
            .await?
            .ok_or_else(|| app_user_not_found(json!({"app-id": ctx.app_id, "email": email})))?;
        auth::sign_out(state, ctx.app_id, Some(user.id), None).await?;
    } else if let Some(raw) = optional_body_value(body, "refresh_token") {
        let token = coerce_uuid_value(raw, &["body", "refresh_token"])?;
        auth::sign_out(state, ctx.app_id, None, Some(&token.to_string())).await?;
    } else {
        return Err(InstantError::validation_failed_input(
            "body",
            body.clone(),
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
    // legacy req->app-user! (admin/routes.clj:454-486) takes the first
    // present key in email / refresh_token / id order and coerces that one.
    if let Some(raw) = params.get("email") {
        let raw = Value::String(raw.clone());
        let email = coerce_email_value(&raw, &["params", "email"])?;
        return auth::user_by_email(state, app_id, &email).await;
    }
    if let Some(raw) = params.get("refresh_token") {
        let raw = Value::String(raw.clone());
        let token = coerce_uuid_value(&raw, &["params", "refresh_token"])?;
        return auth::user_by_refresh_token(state, app_id, &token.to_string()).await;
    }
    if let Some(raw) = params.get("id") {
        let raw = Value::String(raw.clone());
        let id = coerce_uuid_value(&raw, &["params", "id"])?;
        return auth::user_by_id(state, app_id, id).await;
    }
    Err(InstantError::validation_failed_input(
        "params",
        json!(params),
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
            let entity = match users.get(&uid) {
                Some(cached) => cached.clone(),
                None => {
                    // legacy app-user-model/get-by-ids: the app-user shape
                    // (`id`, `app_id`, `email`, `created_at`, `isGuest`)
                    let fetched = user_json(state, ctx.app_id, uid, None)
                        .await
                        .unwrap_or(Value::Null);
                    users.insert(uid, fetched.clone());
                    fetched
                }
            };
            sess["user"] = entity;
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
    // perms skipped for the admin token, `create` rule for impersonation.
    // Legacy reads and coerces every header param before upload-file! runs
    // the permission check, so a blank content-disposition is malformed even
    // when the caller would be denied.
    let path = single_param(header_string(headers, "path"), &["path"])?;
    let meta = blob_meta_from_headers(headers)?;
    if !ctx.perms.admin {
        check_files_perm(state, ctx.app_id, &ctx.perms, "create", &path).await?;
    }
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
    crate::usage::check_upload(state, app_id, body.len()).await?;
    let location_id = Uuid::new_v4().to_string();
    let size = crate::storage::put_blob(state, app_id, &location_id, body, meta).await?;

    let path_attr = sc::attr_id("$files", "path");
    let old_location = file_location_by_path(state, app_id, path).await;
    let lookup = json!([path_attr, path]);
    // legacy app-file/create! (app_file.clj:13-35): the lookup writes `path`;
    // writing it again through the lookup would be refused for a new file
    let steps = vec![
        json!(["add-triple", lookup, sc::attr_id("$files", "id"), lookup]),
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
        check_files_perm(state, ctx.app_id, &ctx.perms, "delete", &filename).await?;
    }
    let id = delete_file_by_path(state, ctx.app_id, &filename).await?;
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
    let url = file_location_by_path(state, ctx.app_id, &filename)
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
    let filename = single_param(body_string(body, "filename"), &["body", "filename"])?;
    let url = create_upload_url(state, ctx.app_id, &filename).await?;
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
    let path = some_param(&[
        ("path", body_string(body, "path")),
        ("filename", body_string(body, "filename")),
    ])?;
    check_files_perm(state, app_id, &perms, "create", &path).await?;
    let url = create_upload_url(state, app_id, &path).await?;
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
    // legacy magic-code-post uses get-param! with email/coerce
    // (admin/routes.clj:514-520)
    let email = body_email_param(body, "email")?;
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
    let email = body_email_param(body, "email")?;
    let code = crate::routes::runtime::send_magic_code_for(state, ctx.app_id, &email).await?;
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
    let ctx = authed_admin(state, headers, params).await?;
    // The shared verifier parses email, code and then app-id with legacy's
    // get-param! semantics; only the app id comes from the auth context here.
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

fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

fn body_string(body: &Value, name: &str) -> Option<String> {
    body.get(name)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn coerce_non_blank_param(raw: &str) -> Option<String> {
    let s = raw.trim();
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

/// Clojure prints a parameter path as `["params" "filename"]` (spaces between
/// elements), not as compact JSON.
fn format_param_path(path: &[&str]) -> String {
    let parts: Vec<String> = path.iter().map(|p| format!("\"{p}\"")).collect();
    format!("[{}]", parts.join(" "))
}

/// `ex/get-param!` with `coerce-non-blank-str`: absent is param-missing,
/// present-but-blank is param-malformed (exception.clj:421-428).
fn single_param(value: Option<String>, path: &[&str]) -> Result<String> {
    let Some(raw) = value else {
        return Err(InstantError::new(
            "param-missing",
            400,
            format!("Missing parameter: {}", format_param_path(path)),
            Some(json!({"in": path})),
        ));
    };
    coerce_non_blank_param(&raw).ok_or_else(|| {
        InstantError::new(
            "param-malformed",
            400,
            format!("Malformed parameter: {}", format_param_path(path)),
            Some(json!({"in": path, "original-input": raw})),
        )
    })
}

/// `ex/get-some-param!` with `coerce-non-blank-str` over ordered candidates
/// (`[:path]` then `[:filename]`): a present candidate wins even when blank,
/// so blank is malformed rather than missing (exception.clj:440-457).
fn some_param(candidates: &[(&str, Option<String>)]) -> Result<String> {
    let Some((name, raw)) = candidates.iter().find(|(_, v)| v.is_some()) else {
        let first = candidates.first().map(|(n, _)| *n).unwrap_or("path");
        let rest: Vec<Value> = candidates.iter().skip(1).map(|(n, _)| json!([n])).collect();
        return Err(InstantError::new(
            "param-missing",
            400,
            format!("Missing parameter: [\"{first}\"]"),
            Some(json!({"in": [first], "possible-ins": rest})),
        ));
    };
    let raw = raw.clone().unwrap_or_default();
    coerce_non_blank_param(&raw).ok_or_else(|| {
        let possible: Vec<Value> = candidates.iter().map(|(n, _)| json!([n])).collect();
        InstantError::new(
            "param-malformed",
            400,
            format!("Malformed parameter: [\"{name}\"]"),
            Some(json!({"in": [name], "possible-ins": possible, "original-input": raw})),
        )
    })
}

/// legacy `get-param! req [:params :filename]` on the admin storage routes.
fn admin_filename_param(params: &HashMap<String, String>) -> Result<String> {
    single_param(params.get("filename").cloned(), &["params", "filename"])
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
    // legacy req->app-file! reads `path` or `filename` (storage/routes.clj:14-27),
    // coercing the header before upload-file! runs the permission check.
    let path = some_param(&[
        ("path", header_string(headers, "path")),
        ("filename", header_string(headers, "filename")),
    ])?;
    let meta = blob_meta_from_headers(headers)?;
    check_files_perm(state, app_id, &perms, "create", &path).await?;
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
    let filename = some_param(&[
        ("path", params.get("path").cloned()),
        ("filename", params.get("filename").cloned()),
    ])?;
    check_files_perm(state, app_id, &perms, "delete", &filename).await?;
    use sqlx::Row;
    let path_attr = sc::attr_id("$files", "path");
    let row = sqlx::query(
        "SELECT entity_id FROM triples
         WHERE app_id = $1 AND attr_id = $2 AND av AND value = to_jsonb($3::text) LIMIT 1",
    )
    .bind(app_id)
    .bind(path_attr)
    .bind(&filename)
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    match row {
        Some(row) => {
            let id: Uuid = row.get("entity_id");
            let old = file_location_by_path(state, app_id, &filename).await;
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
    let filename = some_param(&[
        ("path", params.get("path").cloned()),
        ("filename", params.get("filename").cloned()),
    ])?;
    check_files_perm(state, app_id, &perms, "view", &filename).await?;
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
    .bind(&filename)
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
        // legacy throw-validation-err! :non-admin :non-admin (routes.clj:222,327)
        return Err(InstantError::validation_failed_input(
            "non-admin",
            json!("non-admin"),
            json!([{"message": "Cannot test perms as admin"}]),
        ));
    }
    let q = body_query(body)?;
    let inference = bool_param(body, "inference?");
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
        // legacy throw-validation-err! :non-admin :non-admin (routes.clj:222,327)
        return Err(InstantError::validation_failed_input(
            "non-admin",
            json!("non-admin"),
            json!([{"message": "Cannot test perms as admin"}]),
        ));
    }
    let steps = body_steps(body)?;
    let throw_missing = bool_param(body, "throw-on-missing-attrs?");
    let commit = bool_param(body, "dangerously-commit-tx");
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

#[cfg(test)]
mod link_value_tests {
    use super::*;
    use instant_core::attr::Attr;

    fn attr(id: Uuid, etype: &str, label: &str, rev: Option<(&str, &str)>, unique: bool) -> Attr {
        Attr {
            id,
            value_type: if rev.is_some() {
                ValueType::Ref
            } else {
                ValueType::Blob
            },
            cardinality: if rev.is_some() {
                Cardinality::Many
            } else {
                Cardinality::One
            },
            forward_ident: Uuid::new_v4(),
            etype: etype.into(),
            label: label.into(),
            reverse_ident: rev.map(|_| Uuid::new_v4()),
            reverse_etype: rev.map(|(e, _)| e.into()),
            reverse_label: rev.map(|(_, l)| l.into()),
            is_unique: unique,
            is_indexed: unique,
            is_required: false,
            checked_data_type: None,
            on_delete_cascade: false,
            on_delete_reverse_cascade: false,
            is_system: false,
            indexing: false,
            checking_data_type: false,
            setting_unique: false,
            inferred_types: None,
            metadata: None,
        }
    }

    struct Fx {
        attrs: AttrMap,
        tags_link: Uuid,
        tags_name: Uuid,
    }

    fn fixture() -> Fx {
        let (tags_link, tags_name) = (Uuid::new_v4(), Uuid::new_v4());
        let mut attrs = AttrMap::default();
        attrs.insert(attr(Uuid::new_v4(), "posts", "id", None, true));
        attrs.insert(attr(Uuid::new_v4(), "tags", "id", None, true));
        attrs.insert(attr(tags_name, "tags", "name", None, true));
        attrs.insert(attr(
            tags_link,
            "posts",
            "tags",
            Some(("tags", "posts")),
            false,
        ));
        Fx {
            attrs,
            tags_link,
            tags_name,
        }
    }

    /// targets of the add-triple steps a `link posts p {tags: value}` expands to
    fn link_targets(fx: &Fx, p: Uuid, value: Value) -> Vec<Value> {
        let steps = json!([["link", "posts", p.to_string(), {"tags": value}]]);
        let out = translate_steps(&fx.attrs, &steps, true).expect("translate");
        out.as_array()
            .unwrap()
            .iter()
            .filter(|s| s[0] == "add-triple" && s[2] == json!(fx.tags_link.to_string()))
            .map(|s| s[3].clone())
            .collect()
    }

    #[test]
    fn boolean_flags_use_legacy_truthiness() {
        assert!(!bool_param(&json!({}), "inference?"));
        assert!(!bool_param(&json!({"inference?": null}), "inference?"));
        assert!(!bool_param(&json!({"inference?": false}), "inference?"));
        assert!(bool_param(&json!({"inference?": true}), "inference?"));
        // legacy `boolean` / `(not commit-tx)`: every non-nil non-false value
        assert!(bool_param(&json!({"inference?": 0}), "inference?"));
        assert!(bool_param(&json!({"inference?": "false"}), "inference?"));
        assert!(bool_param(&json!({"inference?": []}), "inference?"));
    }

    #[test]
    fn uuid_attr_lookups_are_refused_like_legacy() {
        let fx = fixture();
        let p = Uuid::new_v4();
        // same namespace (posts.id) and another namespace (tags.name); both
        // are unique attrs, but legacy never resolves an attr id in this form
        for attr_id in [fx.attrs.id_attr_of("posts").unwrap().id, fx.tags_name] {
            let steps = json!([["update", "posts", [attr_id.to_string(), "v"], {"title": "x"}]]);
            let err = translate_steps(&fx.attrs, &steps, false).unwrap_err();
            assert_eq!(err.status, 400);
            assert_eq!(err.error_type, "validation-failed");
            assert_eq!(err.message, "Validation failed for tx-steps");
            let hint = err.hint.unwrap();
            assert_eq!(hint["data-type"], "tx-steps");
            assert_eq!(
                hint["errors"],
                json!([{"expected": "string?", "in": [0, 1, "forward-identity", 2]}])
            );
            // the hint echoes legacy's translated steps: an auto-created attr
            // whose forward label is the attr-id string, then the triples
            assert_eq!(hint["input"][0][0], "add-attr");
            assert_eq!(
                hint["input"][0][1]["forward-identity"][2],
                attr_id.to_string()
            );
            // the object's new `title` attr is prepended too, then the triples.
            // The lookup names the auto-created attr, never the caller's id.
            let lookup = json!([hint["input"][0][1]["id"].clone(), "v"]);
            assert!(hint["input"]
                .as_array()
                .unwrap()
                .iter()
                .any(|step| step[0] == "add-triple" && step[1] == lookup));
        }
        // the name forms keep matching legacy: a name pair and a single-key
        // object both auto-create a unique posts.title attr and target it
        for eid in [json!(["title", "v"]), json!({"title": "v"})] {
            let steps = json!([["update", "posts", eid, {"body": "x"}]]);
            let out = translate_steps(&fx.attrs, &steps, false).expect("name lookup");
            let title = out
                .as_array()
                .unwrap()
                .iter()
                .find_map(|s| {
                    (s[0] == "add-attr"
                        && s[1]["forward-identity"][1] == "posts"
                        && s[1]["forward-identity"][2] == "title")
                        .then(|| s[1]["id"].as_str().unwrap().to_string())
                })
                .expect("auto-created posts.title");
            let lookup = json!([title, "v"]);
            assert!(out
                .as_array()
                .unwrap()
                .iter()
                .any(|s| s[0] == "add-triple" && s[1] == lookup && s[3] == lookup));
        }
        // the same form as a link target is refused too, and
        // throw-on-missing-attrs? keeps legacy's earlier missing-attr error
        let link =
            json!([["link", "posts", p.to_string(), {"tags": [[fx.tags_name.to_string(), "v"]]}]]);
        let err = translate_steps(&fx.attrs, &link, false).unwrap_err();
        assert_eq!(err.message, "Validation failed for tx-steps");
        let err = translate_steps(&fx.attrs, &link, true).unwrap_err();
        assert_eq!(
            err.message,
            "Validation failed for steps: Attributes are missing in your schema"
        );
    }

    #[test]
    fn raw_client_tx_steps_are_refused_like_legacy_spec() {
        let fx = fixture();
        let p = Uuid::new_v4();
        let steps = json!([[
            "add-triple",
            p.to_string(),
            fx.attrs.id_attr_of("posts").unwrap().id,
            p.to_string()
        ]]);
        assert!(translate_steps(&fx.attrs, &steps, false).is_err());
        // the raw schema ops the legacy spec does admit still pass through
        let ok = translate_steps(
            &fx.attrs,
            &json!([["add-attr", {"id": Uuid::new_v4(), "forward-identity": [Uuid::new_v4(), "x", "y"], "value-type": "blob", "cardinality": "one", "unique?": false, "index?": false}]]),
            false,
        );
        assert!(ok.is_ok());
    }

    #[test]
    fn admin_tree_omits_missing_id_and_keeps_null_singular_link() {
        let posts_id = Uuid::new_v4();
        let owner_attr = Uuid::new_v4();
        let mut attrs = AttrMap::default();
        attrs.insert(attr(posts_id, "posts", "id", None, true));
        let mut owner = attr(
            owner_attr,
            "posts",
            "owner",
            Some(("owners", "posts")),
            false,
        );
        owner.cardinality = Cardinality::One;
        attrs.insert(owner);
        let post = Uuid::new_v4();
        let result = QueryResult {
            forms: vec![instaql::FormOut {
                k: "posts".into(),
                etype: "posts".into(),
                where_rows: vec![],
                entities: vec![EntityNode {
                    eid: post,
                    etype: "posts".into(),
                    triples: vec![instaql::TripleOut {
                        e: post,
                        a: posts_id,
                        v: json!(post),
                        t: 1,
                    }],
                    children: vec![instaql::ChildResult {
                        k: "owner".into(),
                        etype: "owners".into(),
                        link_triples: vec![],
                        rows: vec![],
                        entities: vec![],
                    }],
                }],
                page_info: None,
                aggregate: None,
            }],
        };
        let tree = object_tree(&result, &attrs, &json!({"posts": {"owner": {}}}), true);
        assert_eq!(tree["posts"][0]["owner"], Value::Null);
        assert_eq!(tree["posts"][0]["id"], json!(post));
        // an entity with no id triple (created by a bare link) has no id key
        let mut no_id = result.clone();
        no_id.forms[0].entities[0].triples.clear();
        let tree = object_tree(&no_id, &attrs, &json!({"posts": {}}), false);
        assert!(tree["posts"][0].get("id").is_none());
    }

    #[test]
    fn link_array_of_entity_ids_is_a_list_of_targets() {
        let fx = fixture();
        let p = Uuid::new_v4();
        let ids: Vec<String> = (0..3).map(|_| Uuid::new_v4().to_string()).collect();
        for n in 1..=3 {
            let got = link_targets(&fx, p, json!(ids[..n]));
            let want: Vec<Value> = ids[..n].iter().map(|s| json!(s)).collect();
            assert_eq!(got, want, "{n} id(s)");
        }
        // a lookup__ string beside an id: two targets
        let lk = "lookup__name__\"t0\"";
        let got = link_targets(&fx, p, json!([lk, ids[0]]));
        assert_eq!(
            got,
            vec![json!([fx.tags_name.to_string(), "t0"]), json!(ids[0])]
        );
    }

    #[test]
    fn link_lookup_targets_match_legacy() {
        let fx = fixture();
        let p = Uuid::new_v4();
        let want = vec![json!([fx.tags_name.to_string(), "t0"])];
        // an object, or a pair inside the array: one lookup target
        assert_eq!(link_targets(&fx, p, json!({"name": "t0"})), want);
        assert_eq!(link_targets(&fx, p, json!([["name", "t0"]])), want);
        assert_eq!(link_targets(&fx, p, json!("lookup__name__\"t0\"")), want);
    }
}
