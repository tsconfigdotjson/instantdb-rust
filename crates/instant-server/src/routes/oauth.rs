//! /runtime/oauth/* — OAuth/OIDC sign-in: generic OIDC discovery (Google,
//! LinkedIn, ...) and GitHub's non-OIDC OAuth client.
//! Port of LEGACY runtime/routes.clj + auth/oauth.clj.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, RawForm, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
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

/// Legacy `get-some-param!` (util/exception.clj:440-457): the first path
/// holding a value wins; none is param-missing on the first path (the rest as
/// `possible-ins`), a value `coerce` rejects is param-malformed on its path.
fn some_param<T>(
    paths: &[(&[&str], Option<&Value>)],
    coerce: impl Fn(&Value) -> Option<T>,
) -> Result<T> {
    let all: Vec<&[&str]> = paths.iter().map(|(p, _)| *p).collect();
    // clojure truthiness: nil and false are absent
    let found = paths.iter().find_map(|(p, v)| {
        v.filter(|v| !v.is_null() && **v != Value::Bool(false))
            .map(|v| (*p, v))
    });
    let Some((path, v)) = found else {
        return Err(InstantError::new(
            "param-missing",
            400,
            format!(
                "Missing parameter: {}",
                crate::routes::dash::clj_vec(all[0])
            ),
            Some(json!({"in": all[0], "possible-ins": &all[1..]})),
        ));
    };
    coerce(v).ok_or_else(|| {
        InstantError::new(
            "param-malformed",
            400,
            format!(
                "Malformed parameter: {}",
                crate::routes::dash::clj_vec(path)
            ),
            Some(json!({"in": path, "possible-ins": all, "original-input": v})),
        )
    })
}

/// Legacy `get-param!`: one path.
fn get_param<T>(
    path: &[&str],
    v: Option<&Value>,
    coerce: impl Fn(&Value) -> Option<T>,
) -> Result<T> {
    match v.filter(|v| !v.is_null() && **v != Value::Bool(false)) {
        None => Err(crate::routes::dash::param_missing(path)),
        Some(v) => coerce(v).ok_or_else(|| crate::routes::dash::param_malformed(path, v.clone())),
    }
}

fn coerce_uuid(v: &Value) -> Option<Uuid> {
    v.as_str().and_then(|s| Uuid::parse_str(s.trim()).ok())
}

/// `string-util/coerce-non-blank-str`
fn coerce_non_blank(v: &Value) -> Option<String> {
    v.as_str()
        .filter(|s| !s.trim().is_empty())
        .map(str::to_string)
}

fn qp(params: &HashMap<String, String>, k: &str) -> Option<Value> {
    params.get(k).map(|v| Value::String(v.clone()))
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
    /// the `$oauthProviders` name the client belongs to (`github` picks the
    /// GitHub client when there's no discovery endpoint)
    pub provider_name: Option<String>,
    pub meta: Value,
    /// the client's custom callback URL (`redirect_to`), sent to the
    /// provider as `redirect_uri` instead of this server's callback
    pub redirect_to: Option<String>,
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
        provider_name: None,
        meta: json!({}),
        redirect_to: None,
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
        } else if attr_id == sc::attr_id("$oauthClients", "redirectTo") {
            client.redirect_to = v.as_str().map(|s| s.to_string());
        }
    }
    if let Some(provider_id) = client.provider_id {
        let row = sqlx::query(
            "SELECT value FROM triples WHERE app_id = $1 AND entity_id = $2 AND attr_id = $3 LIMIT 1",
        )
        .bind(app_id)
        .bind(provider_id)
        .bind(sc::attr_id("$oauthProviders", "name"))
        .fetch_optional(&state.pool)
        .await
        .map_err(InstantError::from)?;
        client.provider_name =
            row.and_then(|r| r.get::<Value, _>("value").as_str().map(str::to_string));
    }
    Ok(Some(client))
}

/// Which `OAuthClient` record legacy `->OAuthClient` builds
/// (model/app_oauth_client.clj:144-178): a discovery endpoint makes it a
/// generic OIDC client whatever the provider is called; otherwise a
/// `github` provider gets the GitHub client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientKind {
    Oidc,
    GitHub,
}

impl OAuthClient {
    fn kind(&self) -> ClientKind {
        if self.discovery_endpoint.is_none() && self.provider_name.as_deref() == Some("github") {
            ClientKind::GitHub
        } else {
            // a non-github client without a discovery endpoint fails on the
            // discovery lookup (legacy: "Unsupported OAuth client")
            ClientKind::Oidc
        }
    }
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
    // discovery endpoints are app-supplied, and so is everything their
    // documents point at: SSRF-guarded (legacy fetch-discovery / safe-get)
    let v = crate::ssrf::get_json(url, None, state.cfg.oauth_allow_private)
        .await
        .map_err(oauth_err)?;
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
    // legacy oauth-start (runtime/routes.clj:213-236), params in its order
    let app_id = get_param(
        &["params", "app_id"],
        qp(params, "app_id").as_ref(),
        coerce_uuid,
    )?;
    let client_name = &some_param(
        &[
            (
                &["params", "client_name"],
                qp(params, "client_name").as_ref(),
            ),
            (&["params", "client_id"], qp(params, "client_id").as_ref()),
        ],
        coerce_non_blank,
    )?;
    let redirect_uri = &get_param(
        &["params", "redirect_uri"],
        qp(params, "redirect_uri").as_ref(),
        coerce_non_blank,
    )?;

    // legacy wraps both start routes in with-rate-limiting (runtime/routes.clj:754-759)
    state
        .limiters
        .auth
        .check(app_id, 1.0)
        .map_err(crate::rate_limit::rate_limited_err)?;
    let client = client_by_name(state, app_id, client_name)
        .await?
        .ok_or_else(|| {
            InstantError::record_not_found_args(
                "app-oauth-client",
                json!({"app-id": app_id, "client-name": client_name}),
            )
        })?;

    if !origin_authorized(state, app_id, redirect_uri).await? {
        return Err(InstantError::validation_failed_input(
            "redirect-uri",
            json!(redirect_uri),
            json!([{"message": "Invalid redirect_uri. If you're the developer, make sure to add your website to the list of approved domains."}]),
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
    // legacy `(or (:redirect_to client) oauth-redirect-url)` (runtime/routes.clj
    // :249-263): the provider's redirect_uri, remembered on the redirect
    // record for the code exchange
    let callback_url = client
        .redirect_to
        .clone()
        .unwrap_or_else(|| default_callback_url(state));
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

    let oauth_state = format!("{app_id}{state_uuid}");
    let auth_url = match client.kind() {
        ClientKind::GitHub => {
            github::authorization_url(state, &client, &oauth_state, &callback_url)?
        }
        ClientKind::Oidc => {
            let disc = discovery(state, &client).await?;
            let auth_endpoint = disc
                .get("authorization_endpoint")
                .and_then(|v| v.as_str())
                .ok_or_else(|| oauth_err("Discovery document missing authorization_endpoint."))?;
            let mut auth_url = url::Url::parse(auth_endpoint)
                .map_err(|_| oauth_err("Invalid authorization endpoint."))?;
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
            auth_url
        }
    };

    let cookie = format!(
        "__session=instantdb_{cookie_uuid}; HttpOnly; Path=/runtime/oauth; Max-Age=3600; SameSite=Lax"
    );
    // legacy `response/found`
    let mut resp = found(auth_url.as_str());
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

fn default_callback_url(state: &AppState) -> String {
    format!("{}/runtime/oauth/callback", state.cfg.base_url)
}

struct RedirectEntity {
    redirect_url: String,
    redirect_to: Option<String>,
    client_id_entity: Option<Uuid>,
    code_challenge: Option<String>,
    code_challenge_method: Option<String>,
    created_at: i64,
    cookie_hash: Option<String>,
}

/// Legacy `app-oauth-redirect-model/consume!` looks the record up by the app
/// id AND the state the `state` param carries; a record of another app is
/// not found (and not consumed).
async fn consume_redirect(
    state: &AppState,
    expected_app_id: Uuid,
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
    if app_id != expected_app_id {
        return Ok(None);
    }
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
        redirect_to: None,
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
        } else if attr_id == sc::attr_id("$oauthRedirects", "redirectTo") {
            ent.redirect_to = v.as_str().map(|s| s.to_string());
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
    // byte length + ascii before slicing: a 72-byte state with a multibyte
    // char across offset 36 must not panic the handler (legacy `subs` can't)
    let valid_state = state_param.len() == 72
        && state_param.is_ascii()
        && Uuid::parse_str(&state_param[..36]).is_ok()
        && Uuid::parse_str(&state_param[36..]).is_ok();
    if !valid_state {
        return Err(bad(oauth_err("Invalid state param in OAuth redirect.")));
    }
    let state_app_id = Uuid::parse_str(&state_param[..36]).expect("validated above");
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
    let (app_id, ent) = consume_redirect(state, state_app_id, state_uuid_str)
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
    // the redirect_uri the authorization request used (routes.clj:554-557)
    let redirect_to = ent
        .redirect_to
        .clone()
        .unwrap_or_else(|| default_callback_url(state));
    let user_info = match client.kind() {
        ClientKind::GitHub => github::user_info(state, &client, code, &redirect_to).await,
        ClientKind::Oidc => exchange_code(state, &client, code, &redirect_to).await,
    }
    .map_err(fail)?;
    // legacy `(email/coerce (:email user-info))`: lower-cased and trimmed, and
    // dropped when it isn't an email (GitHub hands back the address as typed)
    let email = coerce_email(user_info.get("email"));
    let Some(sub) = user_info.get("sub").filter(|v| v.is_string()).cloned() else {
        return Err(fail(oauth_err("Missing sub.")));
    };
    let user_info = json!({
        "email": email,
        "sub": sub,
        "imageURL": user_info.get("imageURL").cloned().unwrap_or(Value::Null),
    });

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
async fn exchange_code(
    state: &AppState,
    client: &OAuthClient,
    code: &str,
    callback_url: &str,
) -> Result<Value> {
    let disc = discovery(state, client).await?;
    let token_endpoint = disc
        .get("token_endpoint")
        .and_then(|v| v.as_str())
        .ok_or_else(|| oauth_err("Discovery document missing token_endpoint."))?;
    // legacy safe-post-form: the token endpoint comes from the discovery doc
    let body = crate::ssrf::post_form_json(
        token_endpoint,
        &[
            ("client_id", client.client_id.as_deref().unwrap_or_default()),
            (
                "client_secret",
                client.client_secret.as_deref().unwrap_or_default(),
            ),
            ("code", code),
            ("grant_type", "authorization_code"),
            ("redirect_uri", callback_url),
        ],
        state.cfg.oauth_allow_private,
    )
    .await
    .map_err(|e| oauth_err(format!("Token exchange failed: {e}")))?;
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
            crate::ssrf::get_json(userinfo, Some(access_token), state.cfg.oauth_allow_private)
                .await
                .map_err(|e| oauth_err(format!("userinfo failed: {e}")))?
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

/// Legacy `email/coerce` (util/email.clj:15-21) on a provider's email claim.
fn coerce_email(v: Option<&Value>) -> Option<String> {
    v.and_then(|v| v.as_str())
        .map(|e| e.to_lowercase().trim().to_string())
        .filter(|e| crate::routes::runtime::valid_email(e))
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
    match token_impl(&state, &headers, &body, None).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => oauth_err_response(&e),
    }
}

pub async fn token_with_app(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    // legacy wraps only the app-scoped route in with-rate-limiting
    // (runtime/routes.clj:768); the path's app id is one of its `:params`
    match token_impl(&state, &headers, &body, Some(Value::String(app_id))).await {
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
        // `(ex/throw-validation-err! :origin origin ...)`: the hint carries it
        return Err(InstantError::validation_failed_input(
            "origin",
            json!(origin),
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
    verify_pkce_for("app-oauth-code", challenge, method, verifier)
}

/// `verify-pkce!` (auth/oauth.clj:370-412) with the caller's record type
/// (`:app-oauth-code` for app sign-in, `:oauth-code` for the platform).
pub(crate) fn verify_pkce_for(
    record_type: &str,
    challenge: Option<&str>,
    method: Option<&str>,
    verifier: Option<&str>,
) -> Result<()> {
    let fail = |message: &str| {
        InstantError::new(
            "validation-failed",
            400,
            format!("Validation failed for {record_type}: {message}"),
            Some(json!({
                "data-type": record_type,
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
    path_app_id: Option<Value>,
) -> Result<Value> {
    // legacy oauth-token-callback (runtime/routes.clj:607-615): each param is
    // looked up under `param-paths` (params, body, form-params; keyword and
    // string keys, printed alike)
    let body_app_id = body.get("app_id");
    let app_id = some_param(
        &[
            (&["params", "app_id"], path_app_id.as_ref()),
            (&["params", "app_id"], path_app_id.as_ref()),
            (&["body", "app_id"], body_app_id),
            (&["body", "app_id"], body_app_id),
            (&["form-params", "app_id"], None),
            (&["form-params", "app_id"], None),
        ],
        coerce_uuid,
    )?;
    let rate_limited = path_app_id.is_some();
    if rate_limited {
        state
            .limiters
            .auth
            .check(app_id, 1.0)
            .map_err(crate::rate_limit::rate_limited_err)?;
    }
    let body_code = body.get("code");
    let code = some_param(
        &[
            (&["params", "code"], None),
            (&["params", "code"], None),
            (&["body", "code"], body_code),
            (&["body", "code"], body_code),
            (&["form-params", "code"], None),
            (&["form-params", "code"], None),
        ],
        coerce_uuid,
    )?
    .to_string();
    let code = code.as_str();
    let verifier = body.get("code_verifier").cloned().unwrap_or(Value::Null);

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
        return Err(InstantError::record_not_found_args(
            "app-oauth-code",
            json!({"code": code, "app-id": app_id, "verifier": verifier}),
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
    // legacy oauth-id-token-callback (runtime/routes.clj:661-672), params in
    // its order
    let jwt = &get_param(
        &["body", "id_token"],
        body.get("id_token"),
        coerce_non_blank,
    )?;
    let app_id = get_param(&["body", "app_id"], body.get("app_id"), coerce_uuid)?;
    let client_name = &get_param(
        &["body", "client_name"],
        body.get("client_name"),
        coerce_non_blank,
    )?;
    let client = client_by_name(state, app_id, client_name)
        .await?
        .ok_or_else(|| {
            InstantError::record_not_found_args(
                "app-oauth-client",
                json!({"app-id": app_id, "client-name": client_name}),
            )
        })?;

    assert_request_origin(state, app_id, headers).await?;
    if client.kind() == ClientKind::GitHub {
        // GitHub is OAuth2, not OIDC: no id_tokens (auth/oauth.clj:110-112)
        return Err(InstantError::validation_failed_input(
            "id_token",
            Value::Null,
            json!([{"message": "GitHub OAuth does not support ID tokens."}]),
        ));
    }
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
        coerce_email(claims.get("email"))
    } else {
        None
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

/// A `$users` row as `get-by-email-or-oauth-link-qualified` reads it
/// (model/app_user.clj:225-260).
struct LinkUser {
    id: Uuid,
    email: Option<String>,
    image_url: Option<String>,
}

async fn link_user(state: &AppState, app_id: Uuid, id: Uuid) -> Result<LinkUser> {
    let rows = sqlx::query(
        "SELECT attr_id, value FROM triples
         WHERE app_id = $1 AND entity_id = $2 AND attr_id = ANY($3)",
    )
    .bind(app_id)
    .bind(id)
    .bind(vec![
        sc::attr_id("$users", "email"),
        sc::attr_id("$users", "imageURL"),
    ])
    .fetch_all(&state.pool)
    .await
    .map_err(InstantError::from)?;
    let mut user = LinkUser {
        id,
        email: None,
        image_url: None,
    };
    for r in rows {
        let attr_id: Uuid = r.get("attr_id");
        let v: Value = r.get("value");
        if attr_id == sc::attr_id("$users", "email") {
            user.email = v.as_str().map(str::to_string);
        } else {
            user.image_url = v.as_str().map(str::to_string);
        }
    }
    Ok(user)
}

/// Legacy `upsert-oauth-link!` (runtime/routes.clj:276-375). The candidates
/// are the user the provider sub is linked to and, when the provider gave
/// an email, the user with that email:
/// - none: a new user (the guest's id when a guest is upgrading) with the
///   email (possibly none) and image, plus the link;
/// - one: the image follows the provider's, a changed email is written to
///   the linked user, and a user found only by email gets the link;
/// - two (the sub is linked to one user, the email belongs to another): the
///   email wins and the link moves to that user.
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

    // the user the sub is linked to
    let comp_attr = sc::attr_id("$oauthUserLinks", "sub+$oauthProvider");
    let link_row = sqlx::query(
        "SELECT t.entity_id AS link_id, json_uuid_to_uuid(u.value) AS uid
           FROM triples t
           JOIN triples u ON u.app_id = t.app_id AND u.entity_id = t.entity_id AND u.attr_id = $4
          WHERE t.app_id = $1 AND t.attr_id = $2 AND t.av AND t.value = to_jsonb($3::text)
          LIMIT 1",
    )
    .bind(app_id)
    .bind(comp_attr)
    .bind(&composite)
    .bind(sc::attr_id("$oauthUserLinks", "$user"))
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    let linked = link_row.and_then(|r| {
        let uid: Option<Uuid> = r.get("uid");
        uid.map(|uid| (r.get::<Uuid, _>("link_id"), uid))
    });
    let linked_user = match linked {
        Some((_, uid)) => Some(link_user(state, app_id, uid).await?),
        None => None,
    };
    // ...and the user with the provider's email (none without one)
    let email_user = match email {
        Some(email) => match auth::user_by_email(state, app_id, email).await? {
            Some(u) => Some(link_user(state, app_id, u.id).await?),
            None => None,
        },
        None => None,
    };

    let (user, moved_link) = match (linked_user, email_user) {
        (None, None) => {
            // legacy `(or guest-user-id (random-uuid))` + assert-signup!
            let uid = guest_user_id.unwrap_or_else(Uuid::new_v4);
            auth::assert_signup(state, app_id, uid, email, extra_fields, false).await?;
            let mut steps = vec![json!(["add-triple", uid, sc::attr_id("$users", "id"), uid])];
            if let Some(email) = email {
                steps.push(json!([
                    "add-triple",
                    uid,
                    sc::attr_id("$users", "email"),
                    email
                ]));
            }
            steps.push(json!([
                "add-triple",
                uid,
                sc::attr_id("$users", "type"),
                "user"
            ]));
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
            create_link(state, app_id, uid, sub, &composite, provider_id).await?;
            return Ok((uid, true));
        }
        (Some(l), Some(e)) if l.id != e.id => (e, linked.map(|(link_id, _)| link_id)),
        (Some(u), _) => (u, None),
        (None, Some(u)) => (u, None),
    };

    let mut steps = vec![];
    if let Some(img) = image_url {
        if user.image_url.as_deref() != Some(img) {
            steps.push(json!([
                "add-triple",
                user.id,
                sc::attr_id("$users", "imageURL"),
                img
            ]));
        }
    }
    let has_link = linked.map(|(_, uid)| uid) == Some(user.id);
    match (email, moved_link) {
        // the provider's email changed since the link was made
        (Some(email), _) if user.email.as_deref() != Some(email) => {
            steps.push(json!([
                "add-triple",
                user.id,
                sc::attr_id("$users", "email"),
                email
            ]));
        }
        (_, Some(link_id)) => {
            steps.push(json!([
                "add-triple",
                link_id,
                sc::attr_id("$oauthUserLinks", "$user"),
                user.id
            ]));
        }
        _ if !has_link => {
            create_link(state, app_id, user.id, sub, &composite, provider_id).await?;
        }
        _ => {}
    }
    if !steps.is_empty() {
        service::run_system_transact(state, app_id, &Value::Array(steps)).await?;
    }
    Ok((user.id, false))
}

async fn create_link(
    state: &AppState,
    app_id: Uuid,
    user_id: Uuid,
    sub: &str,
    composite: &str,
    provider_id: Uuid,
) -> Result<()> {
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
    Ok(())
}

// ---------------------------------------------------------------------------

pub async fn well_known(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
) -> Response {
    // legacy openid-configuration-get: `(ex/get-param! req [:params :app_id] uuid-util/coerce)`
    if let Err(e) = get_param(
        &["params", "app_id"],
        Some(&Value::String(app_id.clone())),
        coerce_uuid,
    ) {
        return json_or_err(Err(e));
    }
    json_or_err(Ok(json!({
        "authorization_endpoint": format!("{}/runtime/{}/oauth/start", state.cfg.base_url, app_id),
        "token_endpoint": format!("{}/runtime/{}/oauth/token", state.cfg.base_url, app_id),
    })))
}

// ---------------------------------------------------------------------------
// GitHub (non-OIDC) client

/// Legacy `GitHubOAuthClient` (auth/oauth.clj:31-113). GitHub's endpoints are
/// fixed hosts (operator-overridable for tests, `state.rs`), so unlike the
/// discovery-driven client nothing here fetches an app-supplied URL.
mod github {
    use super::*;

    /// legacy `(:default-scope github-config)`
    const SCOPE: &str = "read:user user:email";
    /// GitHub's JSON responses are a few KB; the token response is smaller
    const MAX_BYTES: usize = 1024 * 1024;

    /// `create-authorization-url`: no `response_mode` (GitHub redirects back
    /// with a GET) and no extra params (`hd` is Google's).
    pub(super) fn authorization_url(
        state: &AppState,
        client: &OAuthClient,
        oauth_state: &str,
        redirect_uri: &str,
    ) -> Result<url::Url> {
        let mut url = url::Url::parse(&state.cfg.github_oauth_auth_url)
            .map_err(|_| oauth_err("Invalid authorization endpoint."))?;
        url.query_pairs_mut()
            .append_pair("scope", SCOPE)
            .append_pair("response_type", "code")
            .append_pair("state", oauth_state)
            .append_pair("redirect_uri", redirect_uri)
            .append_pair("client_id", client.client_id.as_deref().unwrap_or_default());
        Ok(url)
    }

    fn http() -> Result<reqwest::Client> {
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| InstantError::internal(format!("http client: {e}")))
    }

    /// legacy `(:headers github-config)`, plus the bearer token on API calls
    fn with_headers(req: reqwest::RequestBuilder, token: Option<&str>) -> reqwest::RequestBuilder {
        let req = req
            .header(header::ACCEPT, "application/json")
            .header(header::USER_AGENT, "InstantDB OAuth");
        match token {
            Some(t) => req.bearer_auth(t),
            None => req,
        }
    }

    /// legacy `clj-http` with `:throw-exceptions false :as :json :coerce
    /// :always`: whether the status was a success, and the body as JSON. A
    /// transport failure or a body that isn't JSON reads as an unsuccessful
    /// empty response, so the caller's default message applies (legacy's
    /// client throws there and answers a 500 instead of redirecting back).
    async fn send(req: reqwest::RequestBuilder) -> (bool, Value) {
        let Ok(mut resp) = req.send().await else {
            return (false, Value::Null);
        };
        let ok = resp.status().is_success();
        let mut buf: Vec<u8> = vec![];
        loop {
            match resp.chunk().await {
                Ok(Some(chunk)) => {
                    buf.extend_from_slice(&chunk);
                    if buf.len() > MAX_BYTES {
                        return (false, Value::Null);
                    }
                }
                Ok(None) => break,
                Err(_) => return (false, Value::Null),
            }
        }
        match serde_json::from_slice(&buf) {
            Ok(v) => (ok, v),
            Err(_) => (false, Value::Null),
        }
    }

    /// clojure truthiness: nil and false are absent
    fn truthy(v: Option<&Value>) -> Option<&Value> {
        v.filter(|v| !v.is_null() && **v != Value::Bool(false))
    }

    /// the message at `key` of an error body, else legacy's default
    fn message(body: &Value, key: &str, default: &str) -> InstantError {
        oauth_err(
            body.get(key)
                .and_then(|v| v.as_str())
                .unwrap_or(default)
                .to_string(),
        )
    }

    /// `fetch-github-primary-email`: the address GitHub marks both primary
    /// and verified; none (or a failed call) is no email.
    async fn primary_email(
        state: &AppState,
        http: &reqwest::Client,
        token: &str,
    ) -> Option<String> {
        let url = format!("{}/user/emails", state.cfg.github_api_url);
        let (ok, body) = send(with_headers(http.get(url), Some(token))).await;
        if !ok {
            return None;
        }
        body.as_array()?.iter().find_map(|e| {
            (truthy(e.get("verified")).is_some() && truthy(e.get("primary")).is_some())
                .then(|| e.get("email").and_then(|v| v.as_str()).map(str::to_string))
                .flatten()
        })
    }

    /// `get-user-info`: exchange the code for an access token, then read
    /// `/user` (id, avatar) and `/user/emails` (the primary verified email).
    /// Every error message is legacy's; the callback redirects back with it.
    pub(super) async fn user_info(
        state: &AppState,
        client: &OAuthClient,
        code: &str,
        redirect_uri: &str,
    ) -> Result<Value> {
        let http = http()?;
        let (ok, token_body) = send(
            with_headers(http.post(&state.cfg.github_oauth_token_url), None).form(&[
                ("client_id", client.client_id.as_deref().unwrap_or_default()),
                (
                    "client_secret",
                    client.client_secret.as_deref().unwrap_or_default(),
                ),
                ("code", code),
                ("grant_type", "authorization_code"),
                ("redirect_uri", redirect_uri),
            ]),
        )
        .await;
        if !ok {
            return Err(message(
                &token_body,
                "error_description",
                "Error exchanging code for token.",
            ));
        }
        // GitHub answers a bad or expired code with a 200 that carries an
        // `error` and no token
        let Some(access_token) = truthy(token_body.get("access_token")).and_then(|v| v.as_str())
        else {
            return Err(oauth_err("No access token received from GitHub."));
        };
        let user_url = format!("{}/user", state.cfg.github_api_url);
        let (ok, user) = send(with_headers(http.get(user_url), Some(access_token))).await;
        if !ok {
            return Err(message(
                &user,
                "message",
                "Failed to fetch user info from GitHub.",
            ));
        }
        let email = primary_email(state, &http, access_token).await;
        // `(str user-id)`: GitHub's numeric id is the subject
        let sub = match truthy(user.get("id")) {
            None => return Err(oauth_err("Missing user ID from GitHub")),
            Some(Value::String(s)) => s.clone(),
            Some(v) => v.to_string(),
        };
        Ok(json!({
            "email": email,
            "sub": sub,
            "imageURL": user.get("avatar_url").cloned().unwrap_or(Value::Null),
        }))
    }
}
