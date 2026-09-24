//! The dashboard's Google login (legacy dash/routes.clj `oauth-start`,
//! `oauth-callback`, `oauth-token-callback`, :887-1090), the get-a-db app
//! creation (dash/get_a_db.clj), `track-import` and the active-session
//! stats. Every route here talks to the same tables legacy uses
//! (`instant_oauth_redirects`, `instant_oauth_codes`, migration 09) and keys
//! them by the sha256 of the state / code uuid like
//! `crypt-util/uuid->sha256`.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use instant_core::error::{InstantError, Result};
use serde_json::{json, Value};
use sqlx::Row;
use uuid::Uuid;

use crate::routes::dash::{dash_user, param_malformed, param_missing, parse_body};
use crate::routes::dash_apps::{
    body_str, body_uuid, create_app, path_uuid, record_not_found, GET_A_DB_CREATOR_EMAIL,
};
use crate::routes::dash_manage::{
    create_dashboard_refresh_token, dashboard_login_user, signup_allowed,
};
use crate::routes::platform_oauth::constant_eq;
use crate::routes::runtime::{coerce_email_pub, json_or_err};
use crate::routes::superadmin::{superadmin_user, Scope};
use crate::state::AppState;

const COOKIE_NAME: &str = "__session";
/// legacy `instant-oauth-redirect-model/expired?`
const REDIRECT_TTL_MINUTES: i64 = 10;
/// legacy `instant-oauth-code-model/expired?`
const CODE_TTL_MINUTES: i64 = 5;
/// legacy `dashboard-signup-denied-message` (dash/routes.clj:247-248)
const SIGNUP_DENIED: &str = "This email is not allowed to sign up for this Instant deployment.";

fn uuid_sha256(id: Uuid) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    Sha256::digest(id.as_bytes()).to_vec()
}

/// `java.net.URLEncoder/encode`: form encoding, space as `+`.
fn form_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'*' | b'-' | b'.' | b'_' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// legacy `coerce-redirect-path`
fn coerce_redirect_path(path: Option<&str>) -> String {
    match path.map(str::trim) {
        None | Some("") => "/dash".into(),
        Some(p) if p.starts_with('/') => p.into(),
        Some(p) => format!("/{p}"),
    }
}

fn google_client(state: &AppState) -> Result<(String, String)> {
    match (
        &state.cfg.google_oauth_client_id,
        &state.cfg.google_oauth_client_secret,
    ) {
        (Some(id), Some(secret)) => Ok((id.clone(), secret.clone())),
        (None, None) => Ok((String::new(), String::new())),
        // legacy `get-google-oauth-client` throws when only one is set
        _ => Err(InstantError::internal(
            "INSTANT_DASHBOARD_GOOGLE_OAUTH_CLIENT_ID and INSTANT_DASHBOARD_GOOGLE_OAUTH_CLIENT_SECRET must be set together",
        )),
    }
}

fn callback_url(state: &AppState) -> String {
    format!("{}/dash/oauth/callback", state.cfg.base_url)
}

/// A postgres `?::uuid` cast of a non-uuid string: legacy raises the
/// sql-exception instead of a param error.
fn invalid_uuid_cast() -> InstantError {
    InstantError::new(
        "sql-exception",
        500,
        "SQL Exception: invalid-text-representation",
        Some(
            json!({"table": null, "condition": "invalid-text-representation", "constraint": null}),
        ),
    )
}

/// ring's `response/found`: a 302 with no body and no content-type.
fn bare_found(location: String, extra: Option<(header::HeaderName, String)>) -> Response {
    let mut resp = (StatusCode::FOUND, axum::body::Body::empty()).into_response();
    if let Ok(v) = HeaderValue::from_str(&location) {
        resp.headers_mut().insert(header::LOCATION, v);
    }
    if let Some((name, value)) = extra {
        if let Ok(v) = HeaderValue::from_str(&value) {
            resp.headers_mut().insert(name, v);
        }
    }
    resp
}

/// GET /dash/oauth/start?redirect_path&redirect_to_dev&ticket — legacy
/// oauth-start (:902-940): records the redirect keyed by the hashed state,
/// sets the `__session` cookie for `/dash/oauth`, and sends the browser to
/// Google with legacy's param order.
pub async fn oauth_start(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let r = async {
        let (client_id, _) = google_client(&state)?;
        let cookie = Uuid::new_v4();
        let st = Uuid::new_v4();
        let ticket = match params.get("ticket").map(|s| s.trim()) {
            None | Some("") => None,
            Some(t) => Some(Uuid::parse_str(t).map_err(|_| invalid_uuid_cast())?),
        };
        let redirect_path = coerce_redirect_path(params.get("redirect_path").map(String::as_str));
        let redirect_to_dev = params.get("redirect_to_dev").map(String::as_str) == Some("true");
        let query = [
            ("scope", "email".to_string()),
            ("response_type", "code".to_string()),
            ("state", st.to_string()),
            ("redirect_uri", callback_url(&state)),
            ("client_id", client_id),
        ]
        .iter()
        .map(|(k, v)| format!("{k}={}", form_encode(v)))
        .collect::<Vec<_>>()
        .join("&");
        sqlx::query(
            "INSERT INTO instant_oauth_redirects (lookup_key, state, cookie, service, redirect_path, redirect_to_dev, ticket)
             VALUES ($1, $2, $3, 'google', $4, $5, $6)",
        )
        .bind(uuid_sha256(st))
        .bind(st)
        .bind(cookie)
        .bind(&redirect_path)
        .bind(redirect_to_dev)
        .bind(ticket)
        .execute(&state.pool)
        .await
        .map_err(|e| match &e {
            // a ticket that is not a registered CLI login: legacy's
            // translate-and-throw-psql-exception! (util/exception.clj:633-649)
            sqlx::Error::Database(db) if db.code().as_deref() == Some("23503") => {
                InstantError::new(
                    "record-foreign-key-invalid",
                    400,
                    "Foreign Key Invalid: foreign-key-violation",
                    Some(json!({
                        "table": db.table(),
                        "condition": "foreign-key-violation",
                        "constraint": db.constraint(),
                    })),
                )
            }
            _ => InstantError::from(e),
        })?;
        let expires =
            (chrono::Utc::now() + chrono::Duration::hours(1)).format("%a, %d %b %Y %H:%M:%S GMT");
        let secure = if state.cfg.base_url.starts_with("https://") {
            "; Secure"
        } else {
            ""
        };
        let cookie_header = format!(
            "{COOKIE_NAME}={cookie}; HttpOnly{secure}; Expires={expires}; Path=/dash/oauth; SameSite=Lax"
        );
        Ok(bare_found(
            format!("{}?{query}", state.cfg.google_oauth_auth_url),
            Some((header::SET_COOKIE, cookie_header)),
        ))
    }
    .await;
    match r {
        Ok(resp) => resp,
        Err(e) => crate::routes::runtime::err_response(&e),
    }
}

fn cookie_param(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| {
            raw.split(';').map(str::trim).find_map(|p| {
                let (k, v) = p.split_once('=')?;
                (k.trim() == COOKIE_NAME).then(|| v.trim().to_string())
            })
        })
}

struct OauthRedirect {
    cookie: Uuid,
    redirect_path: String,
    ticket: Option<Uuid>,
    created_at: chrono::DateTime<chrono::Utc>,
}

/// `instant-oauth-redirect-model/consume!`: one use only.
async fn consume_redirect(state: &AppState, st: Uuid) -> Result<Option<OauthRedirect>> {
    let row = sqlx::query(
        "DELETE FROM instant_oauth_redirects WHERE lookup_key = $1
         RETURNING cookie, redirect_path, ticket, created_at",
    )
    .bind(uuid_sha256(st))
    .fetch_optional(&state.pool)
    .await?;
    Ok(row.map(|r| OauthRedirect {
        cookie: r.get("cookie"),
        redirect_path: r.get("redirect_path"),
        ticket: r.get("ticket"),
        created_at: r.get("created_at"),
    }))
}

struct GoogleTokenResponse {
    success: bool,
    body: Value,
}

/// legacy's `clj-http/post` to Google's token endpoint with the form params
/// and `:coerce :always` (error bodies are JSON too).
async fn exchange_google_code(state: &AppState, code: &str) -> Result<GoogleTokenResponse> {
    let (client_id, client_secret) = google_client(state)?;
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|e| InstantError::internal(format!("http client: {e}")))?;
    let resp = http
        .post(&state.cfg.google_oauth_token_url)
        .form(&[
            ("client_id", client_id.as_str()),
            ("client_secret", client_secret.as_str()),
            ("code", code),
            ("grant_type", "authorization_code"),
            ("redirect_uri", &callback_url(state)),
        ])
        .send()
        .await
        .map_err(|e| InstantError::internal(format!("Google token request failed: {e}")))?;
    let success = resp.status().is_success();
    let text = resp
        .text()
        .await
        .map_err(|e| InstantError::internal(format!("Google token response unreadable: {e}")))?;
    let body: Value = serde_json::from_str(&text)
        .map_err(|e| InstantError::internal(format!("Google token response is not JSON: {e}")))?;
    Ok(GoogleTokenResponse { success, body })
}

/// The id_token's claims: base64url-decode the middle segment (legacy
/// catches the decoder's IllegalArgumentException as "no id token").
fn id_token_claims(id_token: &str) -> Option<Value> {
    let payload = id_token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// legacy `oauth-callback-response`
fn callback_response(
    state: &AppState,
    error: Option<&str>,
    code: Option<Uuid>,
    ticket: Option<Uuid>,
) -> Response {
    // `(config/dashboard-origin {:env :dev})` for redirect_to_dev: both read
    // INSTANT_DASHBOARD_URL first, so they are the same origin here
    let base = format!("{}/dash/oauth/callback", state.cfg.dashboard_origin);
    let mut url = match (error, code) {
        (Some(e), _) => format!("{base}?error={}", form_encode(e)),
        (None, Some(c)) => format!("{base}?code={c}"),
        (None, None) => format!("{base}?code="),
    };
    if let Some(t) = ticket {
        url.push_str(&format!("&ticket={t}"));
    }
    bare_found(url, None)
}

struct UserRow {
    id: Uuid,
    email: String,
    google_sub: Option<String>,
}

async fn users_by_email_or_sub(
    state: &AppState,
    email: &str,
    google_sub: &str,
) -> Result<Vec<UserRow>> {
    let rows = sqlx::query(
        "SELECT id, email, google_sub FROM instant_users WHERE email = $1 OR google_sub = $2",
    )
    .bind(email)
    .bind(google_sub)
    .fetch_all(&state.pool)
    .await?;
    Ok(rows
        .iter()
        .map(|r| UserRow {
            id: r.get("id"),
            email: r.get("email"),
            google_sub: r.get("google_sub"),
        })
        .collect())
}

/// legacy `upsert-user-from-google-sub!` (:943-975): one matching user gets
/// its email or (else) its google_sub brought up to date, none creates the
/// user, several is a failure.
async fn upsert_user(state: &AppState, email: &str, google_sub: &str) -> Result<Option<Uuid>> {
    let users = users_by_email_or_sub(state, email, google_sub).await?;
    match users.len() {
        0 => {
            let id = Uuid::new_v4();
            sqlx::query("INSERT INTO instant_users (id, email, google_sub) VALUES ($1, $2, $3)")
                .bind(id)
                .bind(email)
                .bind(google_sub)
                .execute(&state.pool)
                .await?;
            Ok(Some(id))
        }
        1 => {
            let u = &users[0];
            if u.email != email {
                sqlx::query("UPDATE instant_users SET email = $1 WHERE id = $2")
                    .bind(email)
                    .bind(u.id)
                    .execute(&state.pool)
                    .await?;
            } else if u.google_sub.as_deref() != Some(google_sub) {
                sqlx::query("UPDATE instant_users SET google_sub = $1 WHERE id = $2")
                    .bind(google_sub)
                    .bind(u.id)
                    .execute(&state.pool)
                    .await?;
            }
            Ok(Some(u.id))
        }
        _ => {
            tracing::error!(
                email,
                google_sub,
                "oauth/upsert-user-from-google-sub!: multiple users"
            );
            Ok(None)
        }
    }
}

/// GET /dash/oauth/callback — legacy oauth-callback (:990-1073): the same
/// side effects in the same order (the redirect is consumed as soon as state
/// and cookie parse, Google is asked as soon as there is a code and a
/// redirect), then legacy's error precedence.
pub async fn oauth_callback(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let r = async {
        let error_param = params.get("error").cloned();
        let state_param = params.get("state").cloned();
        let cookie_param = cookie_param(&headers);
        let st = state_param
            .as_deref()
            .and_then(|s| Uuid::parse_str(s.trim()).ok());
        let cookie = cookie_param
            .as_deref()
            .and_then(|s| Uuid::parse_str(s.trim()).ok());
        let redirect = match (st, cookie) {
            (Some(st), Some(_)) => consume_redirect(&state, st).await?,
            _ => None,
        };
        let code = params.get("code").cloned();
        let user_info = match (&code, &redirect) {
            (Some(code), Some(_)) => Some(exchange_google_code(&state, code).await?),
            _ => None,
        };
        let claims = user_info
            .as_ref()
            .and_then(|u| u.body.get("id_token"))
            .and_then(|v| v.as_str())
            .and_then(id_token_claims);
        let email_verified = claims
            .as_ref()
            .and_then(|c| c.get("email_verified"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let google_sub = if email_verified {
            claims
                .as_ref()
                .and_then(|c| c.get("sub"))
                .and_then(|v| v.as_str())
                .map(str::to_string)
        } else {
            None
        };
        let email = claims
            .as_ref()
            .and_then(|c| c.get("email"))
            .and_then(|v| v.as_str())
            .and_then(|e| coerce_email_pub(e).ok());
        let user_info_error = match &user_info {
            Some(u) if !u.success => Some(format!(
                "Error fetching user data from Google: {}.",
                u.body
                    .get("error_description")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Unknown error")
            )),
            _ => None,
        };
        let mut new_user_blocked = false;
        if let (Some(e), Some(sub)) = (&email, &google_sub) {
            if users_by_email_or_sub(&state, e, sub).await?.is_empty()
                && !signup_allowed(&state, e)
            {
                new_user_blocked = true;
            }
        }
        let expired = redirect.as_ref().is_some_and(|r| {
            chrono::Utc::now() - r.created_at > chrono::Duration::minutes(REDIRECT_TTL_MINUTES)
        });
        let error: Option<String> = if let Some(e) = error_param {
            Some(format!("Error from Google: {e}"))
        } else if state_param.is_none() {
            Some("Missing state param in OAuth redirect.".into())
        } else if cookie_param.is_none() {
            Some("Missing cookie.".into())
        } else if st.is_none() {
            Some("Invalid state param in OAuth redirect.".into())
        } else if cookie.is_none() {
            Some("Invalid cookie.".into())
        } else if code.is_none() {
            Some("Missing code param in OAuth redirect.".into())
        } else if redirect.is_none() {
            Some("Could not find OAuth request.".into())
        } else if !uuid_opt_eq(cookie, redirect.as_ref().map(|r| r.cookie)) {
            Some("Mismatch in OAuth request cookie.".into())
        } else if let Some(e) = user_info_error {
            Some(e)
        } else if claims.is_none() {
            Some("Invalid response from Google.".into())
        } else if !email_verified {
            Some("Could not verify email.".into())
        } else if email.is_none() {
            Some("Could not determine email.".into())
        } else if google_sub.is_none() {
            Some("Could not determine user info.".into())
        } else if expired {
            Some("Request is expired.".into())
        } else if new_user_blocked {
            Some(SIGNUP_DENIED.into())
        } else {
            None
        };
        if let Some(e) = error {
            return Ok(callback_response(&state, Some(&e), None, None));
        }
        let redirect = redirect.expect("checked above");
        let (email, google_sub) = (email.expect("checked"), google_sub.expect("checked"));
        let Some(user_id) = upsert_user(&state, &email, &google_sub).await? else {
            return Ok(callback_response(
                &state,
                Some("Could not create or update user."),
                None,
                None,
            ));
        };
        let code = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO instant_oauth_codes (lookup_key, user_id, redirect_path) VALUES ($1, $2, $3)",
        )
        .bind(uuid_sha256(code))
        .bind(user_id)
        .bind(&redirect.redirect_path)
        .execute(&state.pool)
        .await?;
        Ok(callback_response(&state, None, Some(code), redirect.ticket))
    }
    .await;
    match r {
        Ok(resp) => resp,
        Err(e) => crate::routes::runtime::err_response(&e),
    }
}

/// POST /dash/oauth/token {code} — legacy oauth-token-callback (:1075-1090)
/// with `instant-oauth-code-model/consume!` (one use, 5-minute expiry).
pub async fn oauth_token(State(state): State<Arc<AppState>>, body: Bytes) -> Response {
    let r = async {
        let body = parse_body(&body)?;
        let raw = body
            .get("code")
            .filter(|v| !v.is_null())
            .ok_or_else(|| param_missing(&["body", "code"]))?;
        let code = raw
            .as_str()
            .and_then(|s| Uuid::parse_str(s.trim()).ok())
            .ok_or_else(|| param_malformed(&["body", "code"], raw.clone()))?;
        let args = json!([{"code": code}]);
        let row = sqlx::query(
            "DELETE FROM instant_oauth_codes WHERE lookup_key = $1
             RETURNING user_id, redirect_path, created_at",
        )
        .bind(uuid_sha256(code))
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| record_not_found("instant-oauth-code", json!({"args": args})))?;
        let created: chrono::DateTime<chrono::Utc> = row.get("created_at");
        if chrono::Utc::now() - created > chrono::Duration::minutes(CODE_TTL_MINUTES) {
            return Err(InstantError::new(
                "record-expired",
                400,
                "Record expired: instant-oauth-code",
                Some(json!({"args": args})),
            ));
        }
        let user_id: Uuid = row.get("user_id");
        let token = create_dashboard_refresh_token(&state, user_id).await?;
        Ok(json!({
            "token": token,
            "redirect_path": row.get::<String, _>("redirect_path"),
            "user": dashboard_login_user(&state, user_id).await?,
        }))
    }
    .await;
    json_or_err(r)
}

/// POST /dash/apps/:app_id/track-import — legacy apps-track-import
/// (:659-668): a posthog event and `{ok: true}`; no auth beyond the uuid.
pub async fn track_import(Path(app_id): Path<String>) -> Response {
    json_or_err(path_uuid(&app_id, "app_id").map(|_| json!({"ok": true})))
}

/// POST /dash/apps/get_a_db — legacy get_a_db.clj http-post-handler
/// (:26-58): the get-a-db service user (a PAT with `apps-write`) creates an
/// app it owns, with optional rules and schema.
pub async fn get_a_db_post(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let user = superadmin_user(&state, &headers, Scope::AppsWrite).await?;
        let creator: Option<Uuid> = sqlx::query("SELECT id FROM instant_users WHERE email = $1")
            .bind(GET_A_DB_CREATOR_EMAIL)
            .fetch_optional(&state.pool)
            .await?
            .map(|r| r.get("id"));
        if creator != Some(user.id) {
            return Err(InstantError::new(
                "permission-denied",
                400,
                "Permission denied: not get-a-db-user?",
                Some(json!({"input": user.id, "expected": "get-a-db-user?"})),
            ));
        }
        let body = parse_body(&body)?;
        let title = body_str(&body, "title")?;
        let schema = body.get("schema").cloned().filter(|v| !v.is_null());
        let rules_code = body
            .get("rules")
            .and_then(|r| r.get("code"))
            .cloned()
            .filter(|v| !v.is_null());
        if let Some(code) = &rules_code {
            let errors = instant_core::perms::validation_errors(code);
            if !errors.is_empty() {
                return Err(InstantError::validation_failed_input(
                    "rule",
                    code.clone(),
                    json!(errors),
                ));
            }
        }
        let app_id = Uuid::new_v4();
        let app = create_app(&state, app_id, &title, creator, None, Uuid::new_v4()).await?;
        if let Some(code) = &rules_code {
            sqlx::query(
                "INSERT INTO rules (app_id, code) VALUES ($1, $2)
                 ON CONFLICT (app_id) DO UPDATE SET code = excluded.code, version = rules.version + 1
                 WHERE rules.code IS DISTINCT FROM excluded.code",
            )
            .bind(app_id)
            .bind(code)
            .execute(&state.pool)
            .await?;
        }
        if let Some(schema) = schema {
            let plan_body = json!({
                "schema": schema,
                "check_types": true,
                "supports_background_updates": false,
            });
            crate::routes::dash::plan_and_apply(&state, app_id, &plan_body).await?;
        }
        Ok(json!({"app": app}))
    }
    .await;
    json_or_err(r)
}

/// GET /dash/stats/active_sessions — legacy active-sessions-get (:2457-2460)
/// sums cached per-machine reports; this node counts its own sessions and
/// their subscribed queries.
pub async fn active_sessions(State(state): State<Arc<AppState>>) -> Response {
    let mut total_queries = 0usize;
    let sessions: Vec<_> = state.sessions.iter().map(|e| e.value().clone()).collect();
    for s in sessions {
        total_queries += s.state.lock().await.queries.len();
    }
    json_or_err(Ok(json!({
        "total-count": state.sessions.len(),
        "total-queries": total_queries,
    })))
}

// ---------------------------------------------------------------------------
// CLI login (legacy dash/routes.clj cli-auth-* :2375-2413, model
// instant_cli_login.clj): `instant-cli login` registers a ticket + secret, the
// dashboard's Google / magic-code login claims the ticket for the user, and
// the CLI polls `check` with the secret until it gets a refresh token.

/// legacy `instant-cli-login-model/expired?`
const CLI_LOGIN_TTL_MINUTES: i64 = 2;

/// POST /dash/cli/auth/register — a fresh ticket (the row id) and the secret
/// stored as its sha256.
pub async fn cli_auth_register(State(state): State<Arc<AppState>>) -> Response {
    let r = async {
        let secret = Uuid::new_v4();
        let ticket = Uuid::new_v4();
        sqlx::query("INSERT INTO instant_cli_logins (id, secret) VALUES ($1, $2)")
            .bind(ticket)
            .bind(uuid_sha256(secret))
            .execute(&state.pool)
            .await?;
        Ok(json!({"secret": secret, "ticket": ticket}))
    }
    .await;
    json_or_err(r)
}

/// POST /dash/cli/auth/claim {ticket} — the logged-in user attaches
/// themselves to the ticket (an unknown ticket updates nothing, like legacy).
/// Unlike legacy's `claim!` (instant_cli_login.clj), a ticket another user
/// already claimed is refused instead of re-pointed at the caller.
pub async fn cli_auth_claim(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let user = dash_user(&state, &headers).await?;
        let body = parse_body(&body)?;
        let ticket = body_uuid(&body, "ticket")?;
        let claimed_by: Option<Option<Uuid>> = sqlx::query_scalar(
            "WITH claimed AS (
               UPDATE instant_cli_logins SET user_id = $1
                WHERE id = $2 AND (user_id IS NULL OR user_id = $1)
               RETURNING user_id)
             SELECT user_id FROM instant_cli_logins
              WHERE id = $2 AND NOT EXISTS (SELECT 1 FROM claimed)",
        )
        .bind(user.id)
        .bind(ticket)
        .fetch_optional(&state.pool)
        .await?;
        if let Some(Some(_other)) = claimed_by {
            return Err(cli_login_validation(
                json!(ticket),
                "user-already-claimed",
                "This request has already been claimed",
            ));
        }
        Ok(json!({"ticket": ticket}))
    }
    .await;
    json_or_err(r)
}

/// POST /dash/cli/auth/void {ticket} — the user denies the request.
pub async fn cli_auth_void(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        dash_user(&state, &headers).await?;
        let body = parse_body(&body)?;
        let ticket = body_uuid(&body, "ticket")?;
        sqlx::query("UPDATE instant_cli_logins SET used = true WHERE id = $1")
            .bind(ticket)
            .execute(&state.pool)
            .await?;
        Ok(json!({}))
    }
    .await;
    json_or_err(r)
}

fn cli_login_validation(input: Value, issue: &str, message: &str) -> InstantError {
    InstantError::validation_failed_input(
        "instant-cli-login",
        input,
        json!([{"issue": issue, "message": message}]),
    )
}

/// POST /dash/cli/auth/check {secret} — legacy `instant-cli-login-model/use!`:
/// unknown → record-not-found, older than 2 minutes → record-expired, voided
/// (used without a user) / unclaimed / already used → validation-failed with
/// the CLI's `issue` codes (the already-claimed one reports `:id` as its
/// input, which is the literal string "id" on the wire), then a refresh
/// token for the claiming user.
pub async fn cli_auth_check(State(state): State<Arc<AppState>>, body: Bytes) -> Response {
    let r = async {
        let body = parse_body(&body)?;
        let secret = body_uuid(&body, "secret")?;
        let key = uuid_sha256(secret);
        let login = sqlx::query(
            "SELECT id, used, user_id, created_at FROM instant_cli_logins WHERE secret = $1",
        )
        .bind(&key)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| record_not_found("instant-cli-login", json!({})))?;
        let id: Uuid = login.get("id");
        let used: bool = login.get("used");
        let user_id: Option<Uuid> = login.get("user_id");
        let created: chrono::NaiveDateTime = login.get("created_at");
        if chrono::Utc::now().naive_utc() - created
            > chrono::Duration::minutes(CLI_LOGIN_TTL_MINUTES)
        {
            return Err(InstantError::new(
                "record-expired",
                400,
                "Record expired: instant-cli-login",
                Some(json!({"args": [id]})),
            ));
        }
        if used && user_id.is_none() {
            return Err(cli_login_validation(
                json!(id),
                "user-voided-request",
                "This request has been denied",
            ));
        }
        let Some(user_id) = user_id else {
            return Err(cli_login_validation(
                json!(id),
                "waiting-for-user",
                "Waiting for a user to accept this request",
            ));
        };
        let claimed = sqlx::query(
            "UPDATE instant_cli_logins SET used = true
              WHERE secret = $1 AND user_id IS NOT NULL AND used = false RETURNING id",
        )
        .bind(&key)
        .fetch_optional(&state.pool)
        .await?;
        if claimed.is_none() {
            return Err(cli_login_validation(
                json!("id"),
                "user-already-claimed",
                "This request has already been claimed",
            ));
        }
        let token = create_dashboard_refresh_token(&state, user_id).await?;
        let email: String = sqlx::query("SELECT email FROM instant_users WHERE id = $1")
            .bind(user_id)
            .fetch_optional(&state.pool)
            .await?
            .map(|r| r.get("email"))
            .ok_or_else(|| record_not_found("instant-user", json!({"args": [{"id": user_id}]})))?;
        Ok(json!({"token": token, "email": email}))
    }
    .await;
    json_or_err(r)
}

/// legacy `crypt-util/constant-uuid=`: both present and equal, compared in
/// constant time.
fn uuid_opt_eq(a: Option<Uuid>, b: Option<Uuid>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => constant_eq(a.as_bytes(), b.as_bytes()),
        _ => false,
    }
}
