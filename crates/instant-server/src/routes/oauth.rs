//! /runtime/oauth/* — OAuth/OIDC sign-in (Google via generic OIDC discovery).
//! See docs/AUTH.md §1.6-1.10, §2.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, RawForm, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use axum::Json;
use base64::Engine;
use instant_core::error::{InstantError, Result};
use instant_core::system_catalog as sc;
use serde_json::{json, Value};
use sqlx::Row;
use uuid::Uuid;

use crate::auth;
use crate::routes::runtime::{err_response, json_or_err, user_json};
use crate::service;
use crate::state::AppState;

fn oauth_err(msg: impl Into<String>) -> InstantError {
    let msg = msg.into();
    InstantError::new("oauth-error", 400, msg, None)
}

/// oauth-error bodies are {"type": "oauth-error", "error": msg} (no message/hint)
fn oauth_err_response(e: &InstantError) -> Response {
    if e.error_type == "oauth-error" {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"type": "oauth-error", "error": e.message})),
        )
            .into_response();
    }
    err_response(e)
}

// ---------------------------------------------------------------------------
// OAuth client storage (triples)

#[derive(Debug, Clone)]
pub struct OAuthClient {
    pub id: Uuid,
    #[allow(dead_code)]
    pub client_name: String,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub discovery_endpoint: Option<String>,
    pub provider_id: Option<Uuid>,
    pub meta: Value,
}

pub async fn client_by_name(
    state: &AppState,
    app_id: Uuid,
    name: &str,
) -> Result<Option<OAuthClient>> {
    let name_attr = sc::attr_id("$oauthClients", "name");
    let row = sqlx::query(
        "SELECT entity_id FROM triples
         WHERE app_id = $1 AND attr_id = $2 AND av AND value = to_jsonb($3::text) LIMIT 1",
    )
    .bind(app_id)
    .bind(name_attr)
    .bind(name)
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    let Some(row) = row else { return Ok(None) };
    let eid: Uuid = row.get("entity_id");
    let rows =
        sqlx::query("SELECT attr_id, value FROM triples WHERE app_id = $1 AND entity_id = $2")
            .bind(app_id)
            .bind(eid)
            .fetch_all(&state.pool)
            .await
            .map_err(InstantError::from)?;
    let mut client = OAuthClient {
        id: eid,
        client_name: name.to_string(),
        client_id: None,
        client_secret: None,
        discovery_endpoint: None,
        provider_id: None,
        meta: json!({}),
    };
    for row in rows {
        let attr_id: Uuid = row.get("attr_id");
        let v: Value = row.get("value");
        if attr_id == sc::attr_id("$oauthClients", "clientId") {
            client.client_id = v.as_str().map(|s| s.to_string());
        } else if attr_id == sc::attr_id("$oauthClients", "encryptedClientSecret") {
            client.client_secret = v.as_str().map(|s| s.to_string());
        } else if attr_id == sc::attr_id("$oauthClients", "discoveryEndpoint") {
            client.discovery_endpoint = v.as_str().map(|s| s.to_string());
        } else if attr_id == sc::attr_id("$oauthClients", "$oauthProvider") {
            client.provider_id = v.as_str().and_then(|s| Uuid::parse_str(s).ok());
        } else if attr_id == sc::attr_id("$oauthClients", "meta") {
            client.meta = v;
        }
    }
    Ok(Some(client))
}

// ---------------------------------------------------------------------------
// Discovery + JWKS caches

async fn fetch_json_cached(state: &AppState, cache_key: &str, url: &str) -> Result<Value> {
    if let Some(entry) = state.oauth_cache.get(cache_key) {
        let (v, at) = entry.value();
        if at.elapsed().as_secs() < 3600 {
            return Ok(v.clone());
        }
    }
    let resp = reqwest::get(url)
        .await
        .map_err(|e| oauth_err(format!("Failed to fetch {url}: {e}")))?;
    let v: Value = resp
        .json()
        .await
        .map_err(|e| oauth_err(format!("Invalid JSON from {url}: {e}")))?;
    state.oauth_cache.insert(
        cache_key.to_string(),
        (v.clone(), std::time::Instant::now()),
    );
    Ok(v)
}

async fn discovery(state: &AppState, client: &OAuthClient) -> Result<Value> {
    let url = client
        .discovery_endpoint
        .as_ref()
        .ok_or_else(|| oauth_err("OAuth client has no discovery endpoint."))?;
    fetch_json_cached(state, &format!("disc:{url}"), url).await
}

// ---------------------------------------------------------------------------
// Redirect origin validation

async fn origin_authorized(state: &AppState, app_id: Uuid, url: &str) -> Result<bool> {
    let parsed = url::Url::parse(url).map_err(|_| oauth_err("Invalid redirect_uri."))?;
    let scheme = parsed.scheme();
    let host = match (parsed.host_str(), parsed.port()) {
        (Some(h), Some(p)) => format!("{h}:{p}"),
        (Some(h), None) => h.to_string(),
        _ => String::new(),
    };
    // legacy allows localhost / exp:// without configuration only for
    // shared-credential clients (app_authorized_redirect_origin.clj:77-116);
    // this server has none, so every redirect target must be listed
    // (scripts/create-oauth-client.sh adds one)
    let rows = sqlx::query(
        "SELECT service, params FROM app_authorized_redirect_origins WHERE app_id = $1",
    )
    .bind(app_id)
    .fetch_all(&state.pool)
    .await
    .map_err(InstantError::from)?;
    for row in rows {
        let service: String = row.get("service");
        let params: Vec<String> = row.get("params");
        match service.as_str() {
            "generic" => {
                if params.first().map(|p| p == &host).unwrap_or(false) {
                    return Ok(true);
                }
            }
            "custom-scheme" => {
                if params.first().map(|p| p == scheme).unwrap_or(false)
                    && !matches!(scheme, "http" | "https")
                {
                    return Ok(true);
                }
            }
            "netlify" => {
                if let Some(site) = params.first() {
                    let h = parsed.host_str().unwrap_or("");
                    if h == format!("{site}.netlify.app")
                        || (h.ends_with(".netlify.app") || h.ends_with(".netlify.live"))
                            && h.contains(&format!("--{site}."))
                    {
                        return Ok(true);
                    }
                }
            }
            "vercel" if params.len() >= 2 => {
                let h = parsed.host_str().unwrap_or("");
                if h.starts_with(&params[1]) && h.ends_with(&params[0]) {
                    return Ok(true);
                }
            }
            _ => {}
        }
    }
    Ok(false)
}

// ---------------------------------------------------------------------------
// GET /runtime/oauth/start

pub async fn start(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    match start_impl(&state, &params).await {
        Ok(resp) => resp,
        Err(e) => oauth_err_response(&e),
    }
}

pub async fn start_with_app(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    Query(mut params): Query<HashMap<String, String>>,
) -> Response {
    params.insert("app_id".to_string(), app_id);
    match start_impl(&state, &params).await {
        Ok(resp) => resp,
        Err(e) => oauth_err_response(&e),
    }
}

async fn start_impl(state: &AppState, params: &HashMap<String, String>) -> Result<Response> {
    let app_id = params
        .get("app_id")
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| InstantError::param_missing("Missing required parameter: app_id"))?;
    let client_name = params
        .get("client_name")
        .or_else(|| params.get("client_id"))
        .ok_or_else(|| InstantError::param_missing("Missing required parameter: client_name"))?;
    let redirect_uri = params
        .get("redirect_uri")
        .ok_or_else(|| InstantError::param_missing("Missing required parameter: redirect_uri"))?;

    // legacy wraps both start routes in with-rate-limiting (runtime/routes.clj:754-759)
    state
        .limiters
        .auth
        .check(app_id, 1.0)
        .map_err(crate::rate_limit::rate_limited_err)?;
    let client = client_by_name(state, app_id, client_name)
        .await?
        .ok_or_else(|| {
            InstantError::record_not_found("app-oauth-client", "Record not found: app-oauth-client")
        })?;

    if !origin_authorized(state, app_id, redirect_uri).await? {
        return Err(InstantError::validation_failed(
            "redirect_uri",
            "Invalid redirect_uri. If you're the developer, make sure to add your website to the list of approved domains.",
            json!([]),
        ));
    }

    // append app state to the final redirect url
    let mut final_redirect = redirect_uri.clone();
    if let Some(app_state) = params.get("state") {
        let sep = if final_redirect.contains('?') {
            '&'
        } else {
            '?'
        };
        final_redirect = format!(
            "{final_redirect}{sep}state={}",
            urlencoding::encode(app_state)
        );
    }

    let cookie_uuid = Uuid::new_v4();
    let state_uuid = Uuid::new_v4();
    let entity = Uuid::new_v4();
    let callback_url = format!("{}/runtime/oauth/callback", state.cfg.base_url);
    let mut steps = vec![
        json!([
            "add-triple",
            entity,
            sc::attr_id("$oauthRedirects", "id"),
            entity
        ]),
        json!([
            "add-triple",
            entity,
            sc::attr_id("$oauthRedirects", "stateHash"),
            auth::hash_string(&state_uuid.to_string())
        ]),
        json!([
            "add-triple",
            entity,
            sc::attr_id("$oauthRedirects", "cookieHash"),
            auth::hash_string(&cookie_uuid.to_string())
        ]),
        json!([
            "add-triple",
            entity,
            sc::attr_id("$oauthRedirects", "redirectUrl"),
            final_redirect
        ]),
        json!([
            "add-triple",
            entity,
            sc::attr_id("$oauthRedirects", "redirectTo"),
            callback_url
        ]),
        json!([
            "add-triple",
            entity,
            sc::attr_id("$oauthRedirects", "$oauthClient"),
            client.id
        ]),
    ];
    if let Some(challenge) = params.get("code_challenge") {
        steps.push(json!([
            "add-triple",
            entity,
            sc::attr_id("$oauthRedirects", "codeChallenge"),
            challenge
        ]));
        let method = params
            .get("code_challenge_method")
            .cloned()
            .unwrap_or_else(|| "plain".to_string());
        steps.push(json!([
            "add-triple",
            entity,
            sc::attr_id("$oauthRedirects", "codeChallengeMethod"),
            method
        ]));
    }
    service::run_system_transact(state, app_id, &Value::Array(steps)).await?;

    let disc = discovery(state, &client).await?;
    let auth_endpoint = disc
        .get("authorization_endpoint")
        .and_then(|v| v.as_str())
        .ok_or_else(|| oauth_err("Discovery document missing authorization_endpoint."))?;
    let oauth_state = format!("{app_id}{state_uuid}");
    let mut auth_url =
        url::Url::parse(auth_endpoint).map_err(|_| oauth_err("Invalid authorization endpoint."))?;
    {
        let mut qp = auth_url.query_pairs_mut();
        qp.append_pair("scope", "email openid");
        qp.append_pair("response_type", "code");
        qp.append_pair("response_mode", "form_post");
        qp.append_pair("state", &oauth_state);
        qp.append_pair("redirect_uri", &callback_url);
        qp.append_pair("client_id", client.client_id.as_deref().unwrap_or_default());
        if let Some(hd) = params.get("hd") {
            qp.append_pair("hd", hd);
        }
    }

    let cookie = format!(
        "__session=instantdb_{cookie_uuid}; HttpOnly; Path=/runtime/oauth; Max-Age=3600; SameSite=Lax"
    );
    let mut resp = Redirect::temporary(auth_url.as_str()).into_response();
    resp.headers_mut()
        .insert(header::SET_COOKIE, cookie.parse().unwrap());
    Ok(resp)
}

// ---------------------------------------------------------------------------
// GET|POST /runtime/oauth/callback

pub async fn callback_get(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    callback_impl(&state, &headers, params).await
}

pub async fn callback_post(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    RawForm(form): RawForm,
) -> Response {
    let params: HashMap<String, String> = url::form_urlencoded::parse(&form)
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    callback_impl(&state, &headers, params).await
}

struct RedirectEntity {
    redirect_url: String,
    client_id_entity: Option<Uuid>,
    code_challenge: Option<String>,
    code_challenge_method: Option<String>,
    created_at: i64,
    cookie_hash: Option<String>,
}

async fn consume_redirect(
    state: &AppState,
    state_uuid_str: &str,
) -> Result<Option<(Uuid, RedirectEntity)>> {
    let state_hash = auth::hash_string(state_uuid_str);
    let hash_attr = sc::attr_id("$oauthRedirects", "stateHash");
    let row = sqlx::query(
        "SELECT app_id, entity_id FROM triples
         WHERE attr_id = $1 AND av AND value = to_jsonb($2::text) LIMIT 1",
    )
    .bind(hash_attr)
    .bind(&state_hash)
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    let Some(row) = row else { return Ok(None) };
    let app_id: Uuid = row.get("app_id");
    let eid: Uuid = row.get("entity_id");
    let rows = sqlx::query(
        "SELECT attr_id, value, created_at FROM triples WHERE app_id = $1 AND entity_id = $2",
    )
    .bind(app_id)
    .bind(eid)
    .fetch_all(&state.pool)
    .await
    .map_err(InstantError::from)?;
    let mut ent = RedirectEntity {
        redirect_url: String::new(),
        client_id_entity: None,
        code_challenge: None,
        code_challenge_method: None,
        created_at: 0,
        cookie_hash: None,
    };
    for r in rows {
        let attr_id: Uuid = r.get("attr_id");
        let v: Value = r.get("value");
        if attr_id == sc::attr_id("$oauthRedirects", "redirectUrl") {
            ent.redirect_url = v.as_str().unwrap_or_default().to_string();
        } else if attr_id == sc::attr_id("$oauthRedirects", "$oauthClient") {
            ent.client_id_entity = v.as_str().and_then(|s| Uuid::parse_str(s).ok());
        } else if attr_id == sc::attr_id("$oauthRedirects", "codeChallenge") {
            ent.code_challenge = v.as_str().map(|s| s.to_string());
        } else if attr_id == sc::attr_id("$oauthRedirects", "codeChallengeMethod") {
            ent.code_challenge_method = v.as_str().map(|s| s.to_string());
        } else if attr_id == sc::attr_id("$oauthRedirects", "cookieHash") {
            ent.cookie_hash = v.as_str().map(|s| s.to_string());
        } else if attr_id == sc::attr_id("$oauthRedirects", "id") {
            ent.created_at = r.get::<Option<i64>, _>("created_at").unwrap_or(0);
        }
    }
    // consume
    service::run_system_transact(
        state,
        app_id,
        &json!([["delete-entity", eid, "$oauthRedirects"]]),
    )
    .await?;
    Ok(Some((app_id, ent)))
}

/// 302 like legacy's `response/found`.
fn found(url: &str) -> Response {
    (StatusCode::FOUND, [(header::LOCATION, url.to_string())]).into_response()
}

fn add_query_params(url: &str, params: &[(&str, &str)]) -> String {
    let mut out = url.to_string();
    for (k, v) in params {
        let sep = if out.contains('?') { '&' } else { '?' };
        out.push(sep);
        out.push_str(k);
        out.push('=');
        out.push_str(&urlencoding::encode(v));
    }
    out
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Legacy `oauth-callback-testing-landing` (runtime/routes.clj:379-432):
/// `?test-redirect` on the callback renders a page saying the redirect
/// works, so a developer can check the client's callback URL.
fn testing_landing() -> Response {
    let body = "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\"><title>OAuth Redirect Test</title><style>body { margin: 0; height: 100vh; display: flex; align-items: center; justify-content: center; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f6f6f6; color: #111; } p { font-size: 1.25rem; }</style></head><body><p>Your OAuth redirect looks good!</p></body></html>";
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html")],
        body.to_string(),
    )
        .into_response()
}

/// Legacy `oauth-callback-landing` (runtime/routes.clj:434-504): a redirect
/// to a non-http scheme (a native app) can't be a plain 302 without leaving
/// a dangling tab, so the page opens the app itself and offers a button.
fn callback_landing(email: Option<&str>, redirect_url: &str) -> Response {
    let escaped_url = html_escape(redirect_url);
    let who = email.map(html_escape).unwrap_or_default();
    let script = "window.open(document.getElementById('redirect-script').getAttribute('data-redirect-uri'), '_self')";
    let body = format!(
        "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\"><meta http-equiv=\"refresh\" content=\"0; url={escaped_url}\"><title>Finish Sign In</title><style>body {{ margin: 0; height: 100vh; display: flex; flex-direction: column; align-items: center; justify-content: center; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f6f6f6; color: #111; }} .button {{ display: inline-block; padding: 0.75rem 1.5rem; border-radius: 0.5rem; background: #111; color: #fff; text-decoration: none; font-weight: 600; }}</style></head><body><p>Logged in as {who}</p><p><a class=\"button\" href=\"{escaped_url}\">Open app</a></p><script type=\"text/javascript\" id=\"redirect-script\" data-redirect-uri=\"{escaped_url}\">{script}</script></body></html>"
    );
    (StatusCode::OK, [(header::CONTENT_TYPE, "text/html")], body).into_response()
}

/// Legacy `oauth-callback*` (runtime/routes.clj:506-601). Every problem up to
/// and including the OAuth client lookup is a 400 `oauth-error`; only a
/// failed user-info exchange (or a missing `sub`) redirects back to the app
/// with `?error=`, because by then the redirect record has been consumed and
/// verified.
async fn callback_impl(
    state: &AppState,
    headers: &HeaderMap,
    params: HashMap<String, String>,
) -> Response {
    if params.contains_key("test-redirect") {
        return testing_landing();
    }
    match callback_inner(state, headers, params).await {
        Ok(resp) => resp,
        Err((Some(redirect_url), e)) => found(&add_query_params(
            &redirect_url,
            &[("error", &e.message), ("_instant_oauth_redirect", "true")],
        )),
        Err((None, e)) => oauth_err_response(&e),
    }
}

type CallbackErr = (Option<String>, InstantError);

async fn callback_inner(
    state: &AppState,
    headers: &HeaderMap,
    params: HashMap<String, String>,
) -> std::result::Result<Response, CallbackErr> {
    let bad = |e: InstantError| (None, e);
    if let Some(err) = params.get("error") {
        return Err(bad(oauth_err(err.clone())));
    }
    let state_param = params
        .get("state")
        .ok_or_else(|| bad(oauth_err("Missing state param in OAuth redirect.")))?;
    let valid_state = state_param.len() == 72
        && Uuid::parse_str(&state_param[..36]).is_ok()
        && Uuid::parse_str(&state_param[36..]).is_ok();
    if !valid_state {
        return Err(bad(oauth_err("Invalid state param in OAuth redirect.")));
    }
    let state_uuid_str = &state_param[36..];
    let cookie_val = headers
        .get(header::COOKIE)
        .and_then(|c| c.to_str().ok())
        .and_then(|c| {
            // legacy reads `instantdb_<uuid>`; anything else is no cookie
            c.split(';').map(|p| p.trim()).find_map(|p| {
                p.strip_prefix("__session=")
                    .and_then(|v| v.strip_prefix("instantdb_"))
                    .map(|v| v.to_string())
            })
        })
        .filter(|v| Uuid::parse_str(v).is_ok())
        .ok_or_else(|| bad(oauth_err("Missing cookie.")))?;
    let (app_id, ent) = consume_redirect(state, state_uuid_str)
        .await
        .map_err(bad)?
        .ok_or_else(|| bad(oauth_err("Could not find OAuth request.")))?;
    // expired? (app_oauth_redirect.clj:57-60: more than 10 minutes old)
    let age_ms = chrono::Utc::now().timestamp_millis() - ent.created_at;
    if age_ms > 10 * 60_000 {
        return Err(bad(oauth_err("The request is expired.")));
    }
    if ent.cookie_hash.as_deref() != Some(auth::hash_string(&cookie_val).as_str()) {
        return Err(bad(oauth_err("Mismatch in OAuth request cookie.")));
    }
    let code = params
        .get("code")
        .ok_or_else(|| bad(oauth_err("Missing code param in OAuth redirect.")))?;
    let client_entity = ent
        .client_id_entity
        .ok_or_else(|| bad(oauth_err("Missing OAuth client.")))?;
    let client = load_client_by_id(state, app_id, client_entity)
        .await
        .map_err(bad)?
        .ok_or_else(|| bad(oauth_err("Missing OAuth client.")))?;

    // from here on failures ride back to the app
    let redirect_url = ent.redirect_url.clone();
    let fail = |e: InstantError| (Some(redirect_url.clone()), e);
    let user_info = exchange_code(state, &client, code).await.map_err(&fail)?;
    if user_info.get("sub").and_then(|v| v.as_str()).is_none() {
        return Err(fail(oauth_err("Missing sub.")));
    }
    let email = user_info
        .get("email")
        .and_then(|v| v.as_str())
        .map(|e| e.to_string());

    // one-time app-level code
    let app_code = Uuid::new_v4();
    let entity = Uuid::new_v4();
    let mut steps = vec![
        json!([
            "add-triple",
            entity,
            sc::attr_id("$oauthCodes", "id"),
            entity
        ]),
        json!([
            "add-triple",
            entity,
            sc::attr_id("$oauthCodes", "codeHash"),
            auth::hash_string(&app_code.to_string())
        ]),
        json!([
            "add-triple",
            entity,
            sc::attr_id("$oauthCodes", "userInfo"),
            user_info
        ]),
        json!([
            "add-triple",
            entity,
            sc::attr_id("$oauthCodes", "$oauthClient"),
            client.id
        ]),
    ];
    if let Some(challenge) = &ent.code_challenge {
        steps.push(json!([
            "add-triple",
            entity,
            sc::attr_id("$oauthCodes", "codeChallenge"),
            challenge
        ]));
    }
    if let Some(method) = &ent.code_challenge_method {
        steps.push(json!([
            "add-triple",
            entity,
            sc::attr_id("$oauthCodes", "codeChallengeMethod"),
            method
        ]));
    }
    service::run_system_transact(state, app_id, &Value::Array(steps))
        .await
        .map_err(fail)?;

    let target = add_query_params(
        &redirect_url,
        &[
            ("code", &app_code.to_string()),
            ("_instant_oauth_redirect", "true"),
        ],
    );
    let http_scheme = url::Url::parse(&target)
        .map(|u| u.scheme().starts_with("http"))
        .unwrap_or(false);
    if http_scheme {
        Ok(found(&target))
    } else {
        Ok(callback_landing(email.as_deref(), &target))
    }
}

async fn load_client_by_id(
    state: &AppState,
    app_id: Uuid,
    eid: Uuid,
) -> Result<Option<OAuthClient>> {
    let row = sqlx::query(
        "SELECT value FROM triples WHERE app_id = $1 AND entity_id = $2 AND attr_id = $3 LIMIT 1",
    )
    .bind(app_id)
    .bind(eid)
    .bind(sc::attr_id("$oauthClients", "name"))
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    match row {
        Some(r) => {
            let name: Value = r.get("value");
            client_by_name(state, app_id, name.as_str().unwrap_or_default()).await
        }
        None => Ok(None),
    }
}

/// Exchange provider code -> user info {email, sub, imageURL}.
async fn exchange_code(state: &AppState, client: &OAuthClient, code: &str) -> Result<Value> {
    let disc = discovery(state, client).await?;
    let token_endpoint = disc
        .get("token_endpoint")
        .and_then(|v| v.as_str())
        .ok_or_else(|| oauth_err("Discovery document missing token_endpoint."))?;
    let callback_url = format!("{}/runtime/oauth/callback", state.cfg.base_url);
    let http = reqwest::Client::new();
    let resp = http
        .post(token_endpoint)
        .form(&[
            ("client_id", client.client_id.as_deref().unwrap_or_default()),
            (
                "client_secret",
                client.client_secret.as_deref().unwrap_or_default(),
            ),
            ("code", code),
            ("grant_type", "authorization_code"),
            ("redirect_uri", &callback_url),
        ])
        .send()
        .await
        .map_err(|e| oauth_err(format!("Token exchange failed: {e}")))?;
    let body: Value = resp
        .json()
        .await
        .map_err(|e| oauth_err(format!("Invalid token response: {e}")))?;
    if let Some(err) = body.get("error") {
        return Err(oauth_err(format!("Provider error: {err}")));
    }
    let id_token = body.get("id_token").and_then(|v| v.as_str());
    let claims = match id_token {
        Some(jwt) => decode_jwt_payload(jwt)?,
        None => {
            // fall back to userinfo endpoint
            let userinfo = disc
                .get("userinfo_endpoint")
                .and_then(|v| v.as_str())
                .ok_or_else(|| oauth_err("No id_token and no userinfo endpoint."))?;
            let access_token = body
                .get("access_token")
                .and_then(|v| v.as_str())
                .ok_or_else(|| oauth_err("No access token in response."))?;
            http.get(userinfo)
                .bearer_auth(access_token)
                .send()
                .await
                .map_err(|e| oauth_err(format!("userinfo failed: {e}")))?
                .json()
                .await
                .map_err(|e| oauth_err(format!("Invalid userinfo response: {e}")))?
        }
    };
    let email_verified = claims
        .get("email_verified")
        .map(|v| v == &json!(true) || v == &json!("true"))
        .unwrap_or(false);
    let email = if email_verified {
        claims.get("email").cloned().unwrap_or(Value::Null)
    } else {
        Value::Null
    };
    let sub = claims
        .get("sub")
        .cloned()
        .ok_or_else(|| oauth_err("The id_token had no subject."))?;
    Ok(json!({
        "email": email,
        "sub": sub,
        "imageURL": claims.get("picture").cloned().unwrap_or(Value::Null),
    }))
}

fn decode_jwt_payload(jwt: &str) -> Result<Value> {
    let parts: Vec<&str> = jwt.split('.').collect();
    if parts.len() < 2 {
        return Err(oauth_err("Malformed id_token."));
    }
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .map_err(|_| oauth_err("Malformed id_token payload."))?;
    serde_json::from_slice(&payload).map_err(|_| oauth_err("Malformed id_token payload."))
}

// ---------------------------------------------------------------------------
// POST /runtime/oauth/token

pub async fn token(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    match token_impl(&state, &headers, &body, false).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => oauth_err_response(&e),
    }
}

pub async fn token_with_app(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    Json(mut body): Json<Value>,
) -> Response {
    body["app_id"] = json!(app_id);
    // legacy wraps only the app-scoped route in with-rate-limiting
    // (runtime/routes.clj:768)
    match token_impl(&state, &headers, &body, true).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => oauth_err_response(&e),
    }
}

/// Legacy `assert-authorized-request-origin!` (runtime/routes.clj:204-211,
/// :622, :671): when the browser sends an Origin, it must be one of the
/// app's authorized redirect origins.
async fn assert_request_origin(state: &AppState, app_id: Uuid, headers: &HeaderMap) -> Result<()> {
    let Some(origin) = headers.get("origin").and_then(|v| v.to_str().ok()) else {
        return Ok(());
    };
    let ok = match url::Url::parse(origin) {
        Ok(_) => origin_authorized(state, app_id, origin).await?,
        Err(_) => false,
    };
    if !ok {
        return Err(InstantError::validation_failed(
            "origin",
            "Unauthorized origin.",
            json!([{"message": "Unauthorized origin."}]),
        ));
    }
    Ok(())
}

/// Legacy `verify-pkce!` (auth/oauth.clj:364-415) for the `app-oauth-code`
/// record: a verifier without a challenge (and vice versa) is an error, the
/// method must be exactly `plain` or `S256`, and an undecodable S256
/// challenge has its own message.
fn verify_pkce(
    challenge: Option<&str>,
    method: Option<&str>,
    verifier: Option<&str>,
) -> Result<()> {
    let fail = |message: &str| {
        InstantError::new(
            "validation-failed",
            400,
            format!("Validation failed for app-oauth-code: {message}"),
            Some(json!({
                "data-type": "app-oauth-code",
                "input": {"code_verifier": verifier},
                "errors": [{"message": message}],
            })),
        )
    };
    let (challenge, verifier) = match (challenge, verifier) {
        (None, None) => return Ok(()),
        (None, Some(_)) => {
            return Err(fail(
                "The code_verifier was provided, but no code_challenge was provided.",
            ))
        }
        (Some(_), None) => {
            return Err(fail(
                "The code_challenge was provided, but no code_verifier was provided.",
            ))
        }
        (Some(c), Some(v)) => (c, v),
    };
    match method {
        Some("plain") => {
            if constant_time_eq(challenge.as_bytes(), verifier.as_bytes()) {
                Ok(())
            } else {
                Err(fail("The code_challenge and code_verifier do not match."))
            }
        }
        Some("S256") => {
            let hashed = {
                use sha2::{Digest, Sha256};
                let mut h = Sha256::new();
                h.update(verifier.as_bytes());
                h.finalize().to_vec()
            };
            // java.util.Base64 url decoder: padding optional
            let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(challenge.trim_end_matches('='))
                .map_err(|_| fail("Invalid code_verifier. Expected a url-safe Base64 string."))?;
            if constant_time_eq(&decoded, &hashed) {
                Ok(())
            } else {
                Err(fail("The code_challenge and code_verifier do not match."))
            }
        }
        _ => Err(fail("Unknown code challenge method.")),
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

async fn token_impl(
    state: &AppState,
    headers: &HeaderMap,
    body: &Value,
    rate_limited: bool,
) -> Result<Value> {
    let app_id = body
        .get("app_id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| InstantError::param_missing("Missing required parameter: app_id"))?;
    if rate_limited {
        state
            .limiters
            .auth
            .check(app_id, 1.0)
            .map_err(crate::rate_limit::rate_limited_err)?;
    }
    let code = body
        .get("code")
        .and_then(|v| v.as_str())
        .ok_or_else(|| InstantError::param_missing("Missing required parameter: code"))?;

    // consume $oauthCodes by hash
    let code_hash = auth::hash_string(code);
    let hash_attr = sc::attr_id("$oauthCodes", "codeHash");
    let row = sqlx::query(
        "SELECT entity_id FROM triples
         WHERE app_id = $1 AND attr_id = $2 AND av AND value = to_jsonb($3::text) LIMIT 1",
    )
    .bind(app_id)
    .bind(hash_attr)
    .bind(&code_hash)
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    let Some(row) = row else {
        return Err(InstantError::record_not_found(
            "app-oauth-code",
            "Record not found: app-oauth-code",
        ));
    };
    let eid: Uuid = row.get("entity_id");
    let rows = sqlx::query(
        "SELECT attr_id, value, created_at FROM triples WHERE app_id = $1 AND entity_id = $2",
    )
    .bind(app_id)
    .bind(eid)
    .fetch_all(&state.pool)
    .await
    .map_err(InstantError::from)?;
    let mut user_info = Value::Null;
    let mut challenge: Option<String> = None;
    let mut method: Option<String> = None;
    let mut client_entity: Option<Uuid> = None;
    let mut created_at = 0i64;
    for r in rows {
        let attr_id: Uuid = r.get("attr_id");
        let v: Value = r.get("value");
        if attr_id == sc::attr_id("$oauthCodes", "userInfo") {
            user_info = v;
        } else if attr_id == sc::attr_id("$oauthCodes", "codeChallenge") {
            challenge = v.as_str().map(|s| s.to_string());
        } else if attr_id == sc::attr_id("$oauthCodes", "codeChallengeMethod") {
            method = v.as_str().map(|s| s.to_string());
        } else if attr_id == sc::attr_id("$oauthCodes", "$oauthClient") {
            client_entity = v.as_str().and_then(|s| Uuid::parse_str(s).ok());
        } else if attr_id == sc::attr_id("$oauthCodes", "id") {
            created_at = r.get::<Option<i64>, _>("created_at").unwrap_or(0);
        }
    }
    service::run_system_transact(
        state,
        app_id,
        &json!([["delete-entity", eid, "$oauthCodes"]]),
    )
    .await?;
    if chrono::Utc::now().timestamp_millis() - created_at > 5 * 60_000 {
        return Err(InstantError::new(
            "record-expired",
            400,
            "Record expired: app-oauth-code",
            Some(json!({"record-type": "app-oauth-code"})),
        ));
    }
    assert_request_origin(state, app_id, headers).await?;

    // PKCE (legacy verify-pkce!, auth/oauth.clj:364-415)
    verify_pkce(
        challenge.as_deref(),
        method.as_deref(),
        body.get("code_verifier").and_then(|v| v.as_str()),
    )?;

    let client_entity = client_entity.ok_or_else(|| oauth_err("Missing oauth client."))?;
    let client = load_client_by_id(state, app_id, client_entity)
        .await?
        .ok_or_else(|| oauth_err("Could not find oauth client."))?;

    let guest = match body.get("refresh_token").and_then(|v| v.as_str()) {
        Some(t) => auth::guest_by_refresh_token(state, app_id, t).await?,
        None => None,
    };
    let (user_id, created) = upsert_oauth_link(
        state,
        app_id,
        &client,
        &user_info,
        guest.map(|g| g.id),
        auth::extra_fields_of(body, "extra_fields"),
    )
    .await?;
    let token = auth::mint_refresh_token(state, app_id, user_id).await?;
    let user = user_json(state, app_id, user_id, Some(token)).await?;
    Ok(json!({"user": user, "created": created, "refresh_token": token}))
}

// ---------------------------------------------------------------------------
// POST /runtime/oauth/id_token

pub async fn id_token(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    match id_token_impl(&state, &headers, &body).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => oauth_err_response(&e),
    }
}

async fn id_token_impl(state: &AppState, headers: &HeaderMap, body: &Value) -> Result<Value> {
    let app_id = body
        .get("app_id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| InstantError::param_missing("Missing required parameter: app_id"))?;
    let jwt = body
        .get("id_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| InstantError::param_missing("Missing required parameter: id_token"))?;
    let client_name = body
        .get("client_name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| InstantError::param_missing("Missing required parameter: client_name"))?;
    let client = client_by_name(state, app_id, client_name)
        .await?
        .ok_or_else(|| {
            InstantError::record_not_found("app-oauth-client", "Record not found: app-oauth-client")
        })?;

    assert_request_origin(state, app_id, headers).await?;
    let disc = discovery(state, &client).await?;
    let issuer = disc
        .get("issuer")
        .and_then(|v| v.as_str())
        .unwrap_or_default();

    // verify signature via JWKS
    let claims = verify_jwt(state, &disc, jwt).await?;

    if claims.get("iss").and_then(|v| v.as_str()) != Some(issuer) {
        return Err(InstantError::validation_failed(
            "id_token",
            format!("The id_token wasn't issued by {issuer}."),
            json!([]),
        ));
    }
    // nonce: skipped for google
    if issuer != "https://accounts.google.com" {
        let req_nonce = body.get("nonce").and_then(|v| v.as_str());
        let tok_nonce = claims.get("nonce").and_then(|v| v.as_str());
        match (req_nonce, tok_nonce) {
            (None, None) => {}
            (Some(_), None) => {
                return Err(InstantError::validation_failed(
                    "id_token",
                    "The id_token is missing a nonce.",
                    json!([]),
                ))
            }
            (None, Some(_)) => {
                return Err(InstantError::validation_failed(
                    "id_token",
                    "The nonce parameter was not provided in the request.",
                    json!([]),
                ))
            }
            (Some(r), Some(t)) => {
                if r != t && auth::hash_string(r) != t {
                    return Err(InstantError::validation_failed(
                        "id_token",
                        "The nonces do not match.",
                        json!([]),
                    ));
                }
            }
        }
    }
    // audience (unless secret-less client)
    if client.client_secret.is_some() {
        let aud_ok = match claims.get("aud") {
            Some(Value::String(a)) => Some(a.as_str()) == client.client_id.as_deref(),
            Some(Value::Array(auds)) => auds
                .iter()
                .any(|a| a.as_str() == client.client_id.as_deref()),
            _ => false,
        };
        if !aud_ok {
            return Err(InstantError::validation_failed(
                "id_token",
                "The id_token was generated for the wrong OAuth client.",
                json!([]),
            ));
        }
    }
    let sub = claims
        .get("sub")
        .cloned()
        .filter(|s| s.is_string())
        .ok_or_else(|| {
            InstantError::validation_failed("id_token", "The id_token had no subject.", json!([]))
        })?;
    let allow_unverified = client
        .meta
        .get("allowUnverifiedEmail")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let email_verified = claims
        .get("email_verified")
        .map(|v| v == &json!(true) || v == &json!("true"))
        .unwrap_or(false);
    let email = if email_verified || allow_unverified {
        claims.get("email").cloned().unwrap_or(Value::Null)
    } else {
        Value::Null
    };
    let user_info = json!({
        "email": email,
        "sub": sub,
        "imageURL": claims.get("picture").cloned().unwrap_or(Value::Null),
    });

    let guest = match body.get("refresh_token").and_then(|v| v.as_str()) {
        Some(t) => auth::guest_by_refresh_token(state, app_id, t).await?,
        None => None,
    };
    let (user_id, created) = upsert_oauth_link(
        state,
        app_id,
        &client,
        &user_info,
        guest.map(|g| g.id),
        auth::extra_fields_of(body, "extra_fields"),
    )
    .await?;

    // token reuse: if the supplied refresh_token belongs to the same user
    let mut token: Option<Uuid> = None;
    if let Some(supplied) = body.get("refresh_token").and_then(|v| v.as_str()) {
        if let Some(u) = auth::user_by_refresh_token(state, app_id, supplied).await? {
            if u.id == user_id {
                token = Uuid::parse_str(supplied).ok();
            }
        }
    }
    let token = match token {
        Some(t) => t,
        None => auth::mint_refresh_token(state, app_id, user_id).await?,
    };
    let user = user_json(state, app_id, user_id, Some(token)).await?;
    Ok(json!({"user": user, "created": created}))
}

async fn verify_jwt(state: &AppState, disc: &Value, jwt: &str) -> Result<Value> {
    use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
    let jwks_uri = disc
        .get("jwks_uri")
        .and_then(|v| v.as_str())
        .ok_or_else(|| oauth_err("Discovery document missing jwks_uri."))?;
    let jwks = fetch_json_cached(state, &format!("jwks:{jwks_uri}"), jwks_uri).await?;
    let header =
        decode_header(jwt).map_err(|_| oauth_err("Error validating JWT. Malformed token."))?;
    let keys = jwks
        .get("keys")
        .and_then(|k| k.as_array())
        .cloned()
        .unwrap_or_default();
    let jwk = keys
        .iter()
        .find(|k| {
            header
                .kid
                .as_deref()
                .map(|kid| k.get("kid").and_then(|v| v.as_str()) == Some(kid))
                .unwrap_or(true)
        })
        .ok_or_else(|| oauth_err("Error validating JWT. No matching key."))?;
    let key = match jwk.get("kty").and_then(|v| v.as_str()) {
        Some("RSA") => DecodingKey::from_rsa_components(
            jwk.get("n").and_then(|v| v.as_str()).unwrap_or_default(),
            jwk.get("e").and_then(|v| v.as_str()).unwrap_or_default(),
        )
        .map_err(|_| oauth_err("Error validating JWT. Bad RSA key."))?,
        Some("EC") => DecodingKey::from_ec_components(
            jwk.get("x").and_then(|v| v.as_str()).unwrap_or_default(),
            jwk.get("y").and_then(|v| v.as_str()).unwrap_or_default(),
        )
        .map_err(|_| oauth_err("Error validating JWT. Bad EC key."))?,
        _ => return Err(oauth_err("Error validating JWT. Unsupported key type.")),
    };
    let alg = match header.alg {
        Algorithm::RS256 => Algorithm::RS256,
        Algorithm::ES256 => Algorithm::ES256,
        _ => return Err(oauth_err("The id_token used an unsupported algorithm.")),
    };
    // legacy also rejects algorithms the discovery document doesn't list
    // (auth/oauth.clj:223-224 `id_token_signing_alg_values_supported`)
    if let Some(supported) = disc
        .get("id_token_signing_alg_values_supported")
        .and_then(|v| v.as_array())
    {
        let name = format!("{alg:?}");
        if !supported.iter().any(|v| v.as_str() == Some(name.as_str())) {
            return Err(oauth_err("The id_token used an unsupported algorithm."));
        }
    }
    let mut validation = Validation::new(alg);
    validation.validate_aud = false;
    validation.validate_exp = true;
    let data = decode::<Value>(jwt, &key, &validation)
        .map_err(|_| oauth_err("Error validating JWT. Signature is invalid."))?;
    Ok(data.claims)
}

// ---------------------------------------------------------------------------
// User upsert via $oauthUserLinks

/// Legacy `upsert-oauth-link!` (runtime/routes.clj:276-340). A guest
/// upgrading through OAuth keeps its id for a fresh account, or gets
/// `linkedPrimaryUser` pointed at the account that already owns the email /
/// provider sub (`link-guest`, app_user.clj:281-295).
async fn upsert_oauth_link(
    state: &AppState,
    app_id: Uuid,
    client: &OAuthClient,
    user_info: &Value,
    guest_user_id: Option<Uuid>,
    extra_fields: Option<&serde_json::Map<String, Value>>,
) -> Result<(Uuid, bool)> {
    let (user_id, created) = upsert_oauth_link_inner(
        state,
        app_id,
        client,
        user_info,
        guest_user_id,
        extra_fields,
    )
    .await?;
    if let Some(guest) = guest_user_id {
        if guest != user_id {
            let steps = json!([[
                "add-triple",
                guest,
                sc::attr_id("$users", "linkedPrimaryUser"),
                user_id
            ]]);
            service::run_system_transact(state, app_id, &steps).await?;
        }
    }
    Ok((user_id, created))
}

async fn upsert_oauth_link_inner(
    state: &AppState,
    app_id: Uuid,
    client: &OAuthClient,
    user_info: &Value,
    guest_user_id: Option<Uuid>,
    extra_fields: Option<&serde_json::Map<String, Value>>,
) -> Result<(Uuid, bool)> {
    let sub = user_info
        .get("sub")
        .and_then(|v| v.as_str())
        .ok_or_else(|| oauth_err("Missing subject."))?;
    let provider_id = client
        .provider_id
        .ok_or_else(|| oauth_err("OAuth client has no provider."))?;
    let composite = format!("{sub}+{provider_id}");
    let email = user_info.get("email").and_then(|v| v.as_str());
    let image_url = user_info.get("imageURL").and_then(|v| v.as_str());

    // find user by link
    let comp_attr = sc::attr_id("$oauthUserLinks", "sub+$oauthProvider");
    let link_row = sqlx::query(
        "SELECT entity_id FROM triples
         WHERE app_id = $1 AND attr_id = $2 AND av AND value = to_jsonb($3::text) LIMIT 1",
    )
    .bind(app_id)
    .bind(comp_attr)
    .bind(&composite)
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    if let Some(row) = link_row {
        let link_eid: Uuid = row.get("entity_id");
        let user_row = sqlx::query(
            "SELECT json_uuid_to_uuid(value) AS uid FROM triples
             WHERE app_id = $1 AND entity_id = $2 AND attr_id = $3 LIMIT 1",
        )
        .bind(app_id)
        .bind(link_eid)
        .bind(sc::attr_id("$oauthUserLinks", "$user"))
        .fetch_optional(&state.pool)
        .await
        .map_err(InstantError::from)?;
        if let Some(user_row) = user_row {
            if let Some(uid) = user_row.get::<Option<Uuid>, _>("uid") {
                // refresh imageURL
                if let Some(img) = image_url {
                    let steps =
                        json!([["add-triple", uid, sc::attr_id("$users", "imageURL"), img]]);
                    let _ = service::run_system_transact(state, app_id, &steps).await;
                }
                return Ok((uid, false));
            }
        }
    }

    // find user by email
    let mut created = false;
    let user_id = match email {
        Some(email) => match auth::user_by_email(state, app_id, email).await? {
            Some(u) => u.id,
            None => {
                created = true;
                // legacy `(or guest-user-id (random-uuid))` + assert-signup!
                let uid = guest_user_id.unwrap_or_else(Uuid::new_v4);
                auth::assert_signup(state, app_id, uid, Some(email), extra_fields, false).await?;
                let mut steps = vec![
                    json!(["add-triple", uid, sc::attr_id("$users", "id"), uid]),
                    json!(["add-triple", uid, sc::attr_id("$users", "email"), email]),
                    json!(["add-triple", uid, sc::attr_id("$users", "type"), "user"]),
                ];
                if let Some(img) = image_url {
                    steps.push(json!([
                        "add-triple",
                        uid,
                        sc::attr_id("$users", "imageURL"),
                        img
                    ]));
                }
                let attrs = service::load_attrs(state, app_id).await?;
                steps.extend(auth::extra_field_steps(&attrs, uid, extra_fields));
                service::run_system_transact(state, app_id, &Value::Array(steps)).await?;
                uid
            }
        },
        None => {
            created = true;
            let uid = guest_user_id.unwrap_or_else(Uuid::new_v4);
            auth::assert_signup(state, app_id, uid, None, extra_fields, false).await?;
            let mut steps = vec![
                json!(["add-triple", uid, sc::attr_id("$users", "id"), uid]),
                json!(["add-triple", uid, sc::attr_id("$users", "type"), "user"]),
            ];
            let attrs = service::load_attrs(state, app_id).await?;
            steps.extend(auth::extra_field_steps(&attrs, uid, extra_fields));
            service::run_system_transact(state, app_id, &Value::Array(steps)).await?;
            uid
        }
    };

    // create the link
    let link = Uuid::new_v4();
    let steps = json!([
        [
            "add-triple",
            link,
            sc::attr_id("$oauthUserLinks", "id"),
            link
        ],
        [
            "add-triple",
            link,
            sc::attr_id("$oauthUserLinks", "sub"),
            sub
        ],
        [
            "add-triple",
            link,
            sc::attr_id("$oauthUserLinks", "sub+$oauthProvider"),
            composite
        ],
        [
            "add-triple",
            link,
            sc::attr_id("$oauthUserLinks", "$user"),
            user_id
        ],
        [
            "add-triple",
            link,
            sc::attr_id("$oauthUserLinks", "$oauthProvider"),
            provider_id
        ]
    ]);
    service::run_system_transact(state, app_id, &steps).await?;
    Ok((user_id, created))
}

// ---------------------------------------------------------------------------

pub async fn well_known(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
) -> Response {
    json_or_err(Ok(json!({
        "authorization_endpoint": format!("{}/runtime/{}/oauth/start", state.cfg.base_url, app_id),
        "token_endpoint": format!("{}/runtime/{}/oauth/token", state.cfg.base_url, app_id),
    })))
}
