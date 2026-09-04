//! /runtime/auth/* + /runtime/signout endpoints (see docs/AUTH.md).

use std::sync::Arc;

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::Json;
use instant_core::error::{InstantError, Result};
use instant_core::system_catalog as sc;
use serde_json::{json, Map, Value};
use sqlx::Row;
use uuid::Uuid;

use crate::auth;
use crate::rate_limit;
use crate::service;
use crate::state::AppState;

/// Per-app limit shared by the /runtime/auth/* routes (legacy's
/// with-rate-limiting wrapper, docs/AUTH.md §0).
fn check_auth_limit(state: &AppState, app_id: Uuid) -> Result<()> {
    state
        .limiters
        .auth
        .check(app_id, 1.0)
        .map_err(rate_limit::rate_limited_err)
}

/// HTTP error body. Legacy's wrap-errors (util/http.clj) stamps every error
/// response with the request's trace-id next to type/message/hint.
pub fn err_response(e: &InstantError) -> Response {
    let status =
        axum::http::StatusCode::from_u16(e.status).unwrap_or(axum::http::StatusCode::BAD_REQUEST);
    let mut body = e.to_body();
    if let Value::Object(m) = &mut body {
        m.insert(
            "trace-id".into(),
            Value::String(crate::state::new_trace_id()),
        );
    }
    (status, Json(body)).into_response()
}

pub fn json_or_err(r: Result<Value>) -> Response {
    match r {
        Ok(v) => Json(v).into_response(),
        Err(e) => err_response(&e),
    }
}

fn get_str<'a>(body: &'a Value, key: &str) -> Result<&'a str> {
    body.get(key)
        .and_then(|v| v.as_str())
        .ok_or_else(|| InstantError::param_missing(format!("Missing required parameter: {key}")))
}

fn get_app_id(body: &Value, key: &str) -> Result<Uuid> {
    Uuid::parse_str(get_str(body, key)?)
        .map_err(|_| InstantError::param_malformed(format!("Malformed parameter: {key}")))
}

pub fn coerce_email_pub(raw: &str) -> Result<String> {
    coerce_email(raw)
}

fn coerce_email(raw: &str) -> Result<String> {
    let email = raw.trim().to_lowercase();
    let ok = email.contains('@')
        && email.split('@').count() == 2
        && email
            .split('@')
            .nth(1)
            .map(|d| d.contains('.'))
            .unwrap_or(false)
        && !email.contains(' ');
    if !ok {
        return Err(InstantError::param_malformed("Malformed parameter: email"));
    }
    Ok(email)
}

/// Full `user` JSON object per docs/AUTH.md §0.
pub async fn user_json(
    state: &AppState,
    app_id: Uuid,
    user_id: Uuid,
    refresh_token: Option<Uuid>,
) -> Result<Value> {
    let attrs = service::load_attrs(state, app_id).await?;
    let rows = sqlx::query(
        "SELECT attr_id, value, created_at FROM triples
         WHERE app_id = $1 AND entity_id = $2",
    )
    .bind(app_id)
    .bind(user_id)
    .fetch_all(&state.pool)
    .await
    .map_err(InstantError::from)?;
    if rows.is_empty() {
        return Err(InstantError::record_not_found(
            "app-user",
            "Record not found: app-user",
        ));
    }
    let mut m = Map::new();
    let mut created_at_ms: i64 = 0;
    let id_attr = sc::attr_id("$users", "id");
    for row in &rows {
        let attr_id: Uuid = row.get("attr_id");
        let Some(attr) = attrs.get(&attr_id) else {
            continue;
        };
        if attr.etype != "$users" {
            continue;
        }
        let v: Value = row.get("value");
        if attr_id == id_attr {
            created_at_ms = row.get::<Option<i64>, _>("created_at").unwrap_or(0);
        }
        if attr.value_type == instant_core::attr::ValueType::Blob && !v.is_null() {
            m.insert(attr.label.clone(), v);
        }
    }
    m.insert("id".into(), json!(user_id));
    m.insert("app_id".into(), json!(app_id));
    let is_guest = m.get("type").and_then(|t| t.as_str()) == Some("guest");
    m.insert("isGuest".into(), json!(is_guest));
    if created_at_ms > 0 {
        let dt =
            chrono::DateTime::from_timestamp_millis(created_at_ms).unwrap_or_else(chrono::Utc::now);
        m.insert(
            "created_at".into(),
            json!(dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()),
        );
    }
    if let Some(t) = refresh_token {
        m.insert("refresh_token".into(), json!(t));
    }
    Ok(Value::Object(m))
}

// ---------------------------------------------------------------------------

pub async fn send_magic_code(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Value>,
) -> Response {
    json_or_err(send_magic_code_impl(&state, &body).await)
}

async fn send_magic_code_impl(state: &Arc<AppState>, body: &Value) -> Result<Value> {
    let app_id = get_app_id(body, "app-id")?;
    check_auth_limit(state, app_id)?;
    let email = coerce_email(get_str(body, "email")?)?;
    send_magic_code_for(state, app_id, &email).await
}

/// Generate, store and deliver a magic code (legacy magic-code-auth/send!);
/// shared by `/runtime/auth/send_magic_code` and `/admin/send_magic_code`.
pub async fn send_magic_code_for(
    state: &Arc<AppState>,
    app_id: Uuid,
    email: &str,
) -> Result<Value> {
    let app = service::get_app(state, app_id).await?;
    let email = email.to_string();
    // per-(app, email) budget, matching legacy's 20/hour default
    // (magic_code_auth.clj:32-59)
    state
        .limiters
        .magic_code_send
        .check((app_id, email.clone()), 1.0)
        .map_err(rate_limit::email_rate_limited_err)?;
    // 6-digit numeric code (legacy quirk: digits 0-8)
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
    service::run_system_transact(state, app_id, &steps).await?;
    // Fire-and-forget delivery (log-only by default); the response never
    // depends on whether the email actually goes out.
    crate::email::deliver_magic_code(state, app_id, &app.title, &email, &code);
    Ok(json!({"sent": true}))
}

pub async fn verify_magic_code(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Value>,
) -> Response {
    json_or_err(verify_magic_code_runtime(&state, &body).await)
}

/// Client-facing path only: admin verify (verify_magic_code_shared) is
/// already covered by the /admin/* bucket and its token check. The
/// (app, email) bucket doubles as the 6-digit-code brute-force guard.
async fn verify_magic_code_runtime(state: &AppState, body: &Value) -> Result<Value> {
    let app_id = get_app_id(body, "app-id")?;
    check_auth_limit(state, app_id)?;
    let email = coerce_email(get_str(body, "email")?)?;
    state
        .limiters
        .magic_code_verify
        .check((app_id, email), 1.0)
        .map_err(rate_limit::email_rate_limited_err)?;
    verify_magic_code_impl(state, body, false).await
}

pub async fn verify_magic_code_shared(state: &AppState, body: &Value) -> Result<Value> {
    verify_magic_code_impl(state, body, true).await
}

async fn verify_magic_code_impl(state: &AppState, body: &Value, admin: bool) -> Result<Value> {
    let app_id = get_app_id(body, "app-id")?;
    let email = coerce_email(get_str(body, "email")?)?;
    let code = get_str(body, "code")?.trim().to_string();
    let code_hash = auth::hash_string(&code);

    // guest-upgrade token must resolve if provided; legacy only treats it
    // as a guest upgrade when that user's type is "guest" (routes.clj:99-104)
    let mut guest_user: Option<auth::AppUser> = None;
    if let Some(token) = body.get("refresh-token").and_then(|v| v.as_str()) {
        match auth::user_by_refresh_token(state, app_id, token).await? {
            Some(u) => {
                let is_guest = user_json(state, app_id, u.id, None)
                    .await
                    .map(|j| j.get("isGuest").and_then(|g| g.as_bool()).unwrap_or(false))
                    .unwrap_or(false);
                if is_guest {
                    guest_user = Some(u);
                }
            }
            None => {
                return Err(InstantError::record_not_found(
                    "app-user",
                    "Record not found: app-user",
                ))
            }
        }
    }

    // find code entity by (codeHash, email)
    let code_attr = sc::attr_id("$magicCodes", "codeHash");
    let email_attr = sc::attr_id("$magicCodes", "email");
    let row = sqlx::query(
        r#"
        SELECT c.entity_id, cid.created_at
        FROM triples c
        JOIN triples e ON e.app_id = c.app_id AND e.entity_id = c.entity_id AND e.attr_id = $3
        JOIN triples cid ON cid.app_id = c.app_id AND cid.entity_id = c.entity_id AND cid.attr_id = $4
        WHERE c.app_id = $1 AND c.attr_id = $2
          AND c.value = to_jsonb($5::text) AND e.value = to_jsonb($6::text)
        LIMIT 1
        "#,
    )
    .bind(app_id)
    .bind(code_attr)
    .bind(email_attr)
    .bind(sc::attr_id("$magicCodes", "id"))
    .bind(&code_hash)
    .bind(&email)
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    let Some(row) = row else {
        return Err(InstantError::record_not_found(
            "app-user-magic-code",
            "Record not found: app-user-magic-code",
        ));
    };
    let entity: Uuid = row.get("entity_id");
    let created_at: i64 = row.get::<Option<i64>, _>("created_at").unwrap_or(0);

    // legacy checks `$users.allow.create` before consuming the code so a
    // failed check doesn't burn it (magic_code_auth.clj:277-288); the id a
    // new user would get is the guest's when upgrading
    let prospective_id = guest_user
        .as_ref()
        .map(|g| g.id)
        .unwrap_or_else(Uuid::new_v4);
    if !admin && auth::user_by_email(state, app_id, &email).await?.is_none() {
        auth::assert_signup(state, app_id, prospective_id, Some(&email)).await?;
    }
    // consume (one-time)
    service::run_system_transact(
        state,
        app_id,
        &json!([["delete-entity", entity, "$magicCodes"]]),
    )
    .await?;

    // expiry (per-app column; default 1440 minutes)
    let expiry_minutes: i64 = sqlx::query(
        "SELECT coalesce(magic_code_expiry_minutes, 1440) AS m FROM apps WHERE id = $1",
    )
    .bind(app_id)
    .fetch_one(&state.pool)
    .await
    .map_err(InstantError::from)?
    .get::<i32, _>("m") as i64;
    let age_ms = chrono::Utc::now().timestamp_millis() - created_at;
    if age_ms > expiry_minutes * 60_000 {
        return Err(InstantError::new(
            "record-expired",
            400,
            "Record expired: app-user-magic-code",
            Some(json!({"record-type": "app-user-magic-code"})),
        ));
    }

    // upsert user
    let existing = auth::user_by_email(state, app_id, &email).await?;
    let created = existing.is_none();
    let user = match existing {
        Some(u) => u,
        None => {
            // legacy magic_code_auth.clj:284 `(or guest-user-id (random-uuid))`:
            // a guest upgrading to a NEW email is upgraded in place (same id)
            let uid = prospective_id;
            let steps = json!([
                ["add-triple", uid, sc::attr_id("$users", "id"), uid],
                ["add-triple", uid, sc::attr_id("$users", "email"), email],
                ["add-triple", uid, sc::attr_id("$users", "type"), "user"]
            ]);
            service::run_system_transact(state, app_id, &steps).await?;
            auth::AppUser {
                id: uid,
                email: None,
            }
        }
    };
    // guest linking (legacy link-guest): the email already belongs to another
    // user, so the guest row points at it via $users.linkedPrimaryUser
    if let Some(guest) = guest_user {
        if guest.id != user.id {
            let steps = json!([[
                "add-triple",
                guest.id,
                sc::attr_id("$users", "linkedPrimaryUser"),
                user.id
            ]]);
            service::run_system_transact(state, app_id, &steps).await?;
        }
    }
    let token = auth::mint_refresh_token(state, app_id, user.id).await?;
    let user = user_json(state, app_id, user.id, Some(token)).await?;
    Ok(json!({"user": user, "created": created}))
}

pub async fn verify_refresh_token(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Value>,
) -> Response {
    json_or_err(verify_refresh_token_impl(&state, &body).await)
}

async fn verify_refresh_token_impl(state: &AppState, body: &Value) -> Result<Value> {
    let app_id = get_app_id(body, "app-id")?;
    check_auth_limit(state, app_id)?;
    let token = get_str(body, "refresh-token")?;
    let user = auth::user_by_refresh_token(state, app_id, token)
        .await?
        .ok_or_else(|| InstantError::record_not_found("app-user", "Record not found: app-user"))?;
    let token_uuid = Uuid::parse_str(token).ok();
    let user = user_json(state, app_id, user.id, token_uuid).await?;
    Ok(json!({"user": user}))
}

pub async fn sign_in_guest(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Value>,
) -> Response {
    json_or_err(sign_in_guest_impl(&state, &body).await)
}

async fn sign_in_guest_impl(state: &AppState, body: &Value) -> Result<Value> {
    let app_id = get_app_id(body, "app-id")?;
    check_auth_limit(state, app_id)?;
    service::get_app(state, app_id).await?;
    let uid = Uuid::new_v4();
    auth::assert_signup(state, app_id, uid, None).await?;
    let steps = json!([
        ["add-triple", uid, sc::attr_id("$users", "id"), uid],
        ["add-triple", uid, sc::attr_id("$users", "type"), "guest"]
    ]);
    service::run_system_transact(state, app_id, &steps).await?;
    let token = auth::mint_refresh_token(state, app_id, uid).await?;
    let user = user_json(state, app_id, uid, Some(token)).await?;
    Ok(json!({"user": user}))
}

/// `POST /runtime/framework/query` — legacy `framework-query-triples`
/// (runtime/routes.clj:728-743), what `@instantdb/core`'s FrameworkClient
/// (SSR) calls: optional bearer refresh token, `app-id` header or `app_id`
/// query param, body `{query, versions}`; answers the permissioned
/// join-rows result plus the app's attrs.
pub async fn framework_query(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    Json(body): Json<Value>,
) -> Response {
    json_or_err(framework_query_impl(&state, &headers, &params, &body).await)
}

async fn framework_query_impl(
    state: &AppState,
    headers: &axum::http::HeaderMap,
    params: &std::collections::HashMap<String, String>,
    body: &Value,
) -> Result<Value> {
    let app_id = crate::routes::admin::app_id_param(headers, params)?;
    check_auth_limit(state, app_id)?;
    let q = crate::routes::admin::body_query(body)?;
    // legacy req->bearer-token (non-throwing): an absent or unknown token is
    // an anonymous query
    let mut user_id = None;
    if let Some(token) = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .map(|s| s.trim())
    {
        user_id = auth::user_by_refresh_token(state, app_id, token)
            .await?
            .map(|u| u.id);
    }
    service::assert_read_allowed(state, app_id).await?;
    let attrs = service::load_attrs(state, app_id).await?;
    let request = crate::ws::request_ctx_from_headers(headers);
    let perms = service::PermsCtx {
        admin: false,
        user_id,
        user_map: None,
        rule_params: None,
        ip: request.ip,
        origin: request.origin,
    };
    let result = service::run_query(state, app_id, &attrs, &perms, q).await?;
    let (wire, _, _) = crate::ws::format_query_result(&result, &attrs, q, false, false);
    Ok(json!({"result": wire, "attrs": attrs.to_wire_visible()}))
}

pub async fn signout(State(state): State<Arc<AppState>>, Json(body): Json<Value>) -> Response {
    json_or_err(signout_impl(&state, &body).await)
}

async fn signout_impl(state: &AppState, body: &Value) -> Result<Value> {
    let app_id = get_app_id(body, "app_id")?;
    let token = get_str(body, "refresh_token")?;
    auth::sign_out(state, app_id, None, Some(token)).await?;
    Ok(json!({}))
}
