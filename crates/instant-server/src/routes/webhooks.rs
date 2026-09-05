//! Webhook HTTP routes: the dashboard's management routes (LEGACY
//! dash/routes.clj:2196-2372, `/dash/apps/:app_id/webhooks*`) and the
//! receiver-facing ones (LEGACY webhook_routes.clj: the JWK set and the
//! payload fetch).

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use instant_core::error::{InstantError, Result};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::routes::dash::{
    dash_authed_with_role, param_malformed, param_missing, parse_body, DashRole,
};
use crate::routes::dash_apps::path_uuid;
use crate::routes::runtime::json_or_err;
use crate::routes::superadmin::Scope;
use crate::service;
use crate::state::AppState;
use crate::webhooks::{self, Isn};

/// legacy `coerce-string-vec`
fn string_vec(body: &Value, key: &str, required: bool) -> Result<Option<Vec<String>>> {
    match body.get(key) {
        None | Some(Value::Null) if !required => Ok(None),
        None | Some(Value::Null) => Err(param_missing(&["body", key])),
        Some(v) => {
            let arr = v.as_array().filter(|a| a.iter().all(|x| x.is_string()));
            arr.map(|a| {
                Some(
                    a.iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect(),
                )
            })
            .ok_or_else(|| param_malformed(&["body", key], v.clone()))
        }
    }
}

fn body_str(body: &Value, key: &str, required: bool) -> Result<Option<String>> {
    match body.get(key) {
        None | Some(Value::Null) if !required => Ok(None),
        None | Some(Value::Null) => Err(param_missing(&["body", key])),
        Some(v) => v
            .as_str()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| Some(s.to_string()))
            .ok_or_else(|| param_malformed(&["body", key], v.clone())),
    }
}

async fn hook_json(state: &AppState, app_id: Uuid, webhook_id: Uuid) -> Result<Value> {
    let attrs = service::load_attrs(state, app_id).await?;
    let w = webhooks::get_by_id(state, app_id, webhook_id).await?;
    Ok(json!({"webhook": webhooks::webhook_json(&attrs, &w)}))
}

/// GET /dash/apps/:app_id/webhooks
pub async fn list(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let app = dash_authed_with_role(&state, &headers, &app_id, DashRole::Collaborator, Scope::DataRead).await?;
        let attrs = service::load_attrs(&state, app.id).await?;
        let hooks = webhooks::get_all(&state, app.id).await?;
        Ok(json!({"webhooks": hooks.iter().map(|w| webhooks::webhook_json(&attrs, w)).collect::<Vec<_>>()}))
    }
    .await;
    json_or_err(r)
}

/// POST /dash/apps/:app_id/webhooks — `{url, namespaces, actions}`
pub async fn create(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let app = dash_authed_with_role(
            &state,
            &headers,
            &app_id,
            DashRole::Collaborator,
            Scope::DataWrite,
        )
        .await?;
        let body = parse_body(&body)?;
        let url = body_str(&body, "url", true)?.unwrap_or_default();
        let namespaces = string_vec(&body, "namespaces", true)?.unwrap_or_default();
        let actions = string_vec(&body, "actions", true)?.unwrap_or_default();
        let id = webhooks::create(&state, app.id, &url, &namespaces, &actions).await?;
        hook_json(&state, app.id, id).await
    }
    .await;
    json_or_err(r)
}

/// POST /dash/apps/:app_id/webhooks/:webhook_id — partial update
pub async fn update(
    State(state): State<Arc<AppState>>,
    Path((app_id, webhook_id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let app = dash_authed_with_role(
            &state,
            &headers,
            &app_id,
            DashRole::Collaborator,
            Scope::DataWrite,
        )
        .await?;
        let webhook_id = path_uuid(&webhook_id, "webhook_id")?;
        let body = parse_body(&body)?;
        let obj = body.as_object().cloned().unwrap_or_default();
        let patch = webhooks::WebhookPatch {
            url: if obj.contains_key("url") {
                Some(body_str(&body, "url", true)?.unwrap_or_default())
            } else {
                None
            },
            namespaces: if obj.contains_key("namespaces") {
                string_vec(&body, "namespaces", true)?
            } else {
                None
            },
            actions: if obj.contains_key("actions") {
                string_vec(&body, "actions", true)?
            } else {
                None
            },
        };
        webhooks::update(&state, app.id, webhook_id, patch).await?;
        hook_json(&state, app.id, webhook_id).await
    }
    .await;
    json_or_err(r)
}

/// DELETE /dash/apps/:app_id/webhooks/:webhook_id
pub async fn delete(
    State(state): State<Arc<AppState>>,
    Path((app_id, webhook_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let app = dash_authed_with_role(
            &state,
            &headers,
            &app_id,
            DashRole::Collaborator,
            Scope::DataWrite,
        )
        .await?;
        let webhook_id = path_uuid(&webhook_id, "webhook_id")?;
        let before = hook_json(&state, app.id, webhook_id).await?;
        webhooks::delete(&state, app.id, webhook_id).await?;
        Ok(before)
    }
    .await;
    json_or_err(r)
}

/// POST /dash/apps/:app_id/webhooks/:webhook_id/enable
pub async fn enable(
    State(state): State<Arc<AppState>>,
    Path((app_id, webhook_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let app = dash_authed_with_role(
            &state,
            &headers,
            &app_id,
            DashRole::Collaborator,
            Scope::DataWrite,
        )
        .await?;
        let webhook_id = path_uuid(&webhook_id, "webhook_id")?;
        webhooks::enable(&state, app.id, webhook_id).await?;
        hook_json(&state, app.id, webhook_id).await
    }
    .await;
    json_or_err(r)
}

/// POST /dash/apps/:app_id/webhooks/:webhook_id/disable — `{reason?}`
pub async fn disable(
    State(state): State<Arc<AppState>>,
    Path((app_id, webhook_id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let app = dash_authed_with_role(
            &state,
            &headers,
            &app_id,
            DashRole::Collaborator,
            Scope::DataWrite,
        )
        .await?;
        let webhook_id = path_uuid(&webhook_id, "webhook_id")?;
        let body = parse_body(&body)?;
        let reason = body_str(&body, "reason", false)?;
        webhooks::disable(&state, app.id, webhook_id, reason.as_deref()).await?;
        hook_json(&state, app.id, webhook_id).await
    }
    .await;
    json_or_err(r)
}

const EVENTS_PAGE_SIZE: i64 = 100;

/// `encode-events-cursor`: base64url of epoch seconds (i64) + nanos (i32) +
/// the 12 isn bytes
fn encode_cursor(created_at: chrono::DateTime<chrono::Utc>, isn: Isn) -> String {
    let mut buf = Vec::with_capacity(24);
    buf.extend_from_slice(&created_at.timestamp().to_be_bytes());
    buf.extend_from_slice(&(created_at.timestamp_subsec_nanos() as i32).to_be_bytes());
    buf.extend_from_slice(&isn.to_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
}

fn decode_cursor(s: &str) -> Option<(chrono::DateTime<chrono::Utc>, Isn)> {
    let b = base64::engine::general_purpose::URL_SAFE
        .decode(s)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(s))
        .ok()?;
    if b.len() != 24 {
        return None;
    }
    let secs = i64::from_be_bytes(b[..8].try_into().ok()?);
    let nanos = i32::from_be_bytes(b[8..12].try_into().ok()?);
    let isn = Isn::from_bytes(&b[12..])?;
    let t = chrono::DateTime::from_timestamp(secs, nanos.max(0) as u32)?;
    Some((t, isn))
}

/// GET /dash/apps/:app_id/webhooks/:webhook_id/events?after=
pub async fn events(
    State(state): State<Arc<AppState>>,
    Path((app_id, webhook_id)): Path<(String, String)>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let r = async {
        let app = dash_authed_with_role(
            &state,
            &headers,
            &app_id,
            DashRole::Collaborator,
            Scope::DataRead,
        )
        .await?;
        let webhook_id = path_uuid(&webhook_id, "webhook_id")?;
        let after = match params.get("after") {
            None => None,
            Some(s) if s.is_empty() => None,
            Some(s) => Some(
                decode_cursor(s).ok_or_else(|| param_malformed(&["params", "after"], json!(s)))?,
            ),
        };
        let evs =
            webhooks::get_events(&state, app.id, webhook_id, after, EVENTS_PAGE_SIZE + 1).await?;
        let has_next = evs.len() as i64 == EVENTS_PAGE_SIZE + 1;
        let page: Vec<&webhooks::WebhookEvent> =
            evs.iter().take(EVENTS_PAGE_SIZE as usize).collect();
        Ok(json!({
            "events": page.iter().map(|e| webhooks::event_json(e)).collect::<Vec<_>>(),
            "pageInfo": {
                "startCursor": page.first().map(|e| encode_cursor(e.created_at, e.isn)),
                "endCursor": page.last().map(|e| encode_cursor(e.created_at, e.isn)),
                "hasNextPage": has_next,
            }
        }))
    }
    .await;
    json_or_err(r)
}

fn parse_isn(raw: &str) -> Result<Isn> {
    Isn::parse(raw).ok_or_else(|| param_malformed(&["params", "*"], json!(raw)))
}

/// GET /dash/apps/:app_id/webhooks/:webhook_id/events/*isn
pub async fn event(
    State(state): State<Arc<AppState>>,
    Path((app_id, webhook_id, isn)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let app = dash_authed_with_role(&state, &headers, &app_id, DashRole::Collaborator, Scope::DataRead).await?;
        let webhook_id = path_uuid(&webhook_id, "webhook_id")?;
        let isn = parse_isn(&isn)?;
        let ev = webhooks::get_event(&state, app.id, webhook_id, isn).await?.ok_or_else(|| {
            InstantError::new(
                "record-not-found",
                400,
                "Record not found: webhook-event",
                Some(json!({
                    "args": [{"app-id": app.id, "webhook-id": webhook_id, "isn": isn.to_string_legacy()}],
                    "record-type": "webhook-event",
                })),
            )
        })?;
        Ok(json!({"event": webhooks::event_json(&ev)}))
    }
    .await;
    json_or_err(r)
}

/// POST /dash/apps/:app_id/webhooks/:webhook_id/events/*isn — resend
pub async fn resend(
    State(state): State<Arc<AppState>>,
    Path((app_id, webhook_id, isn)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let app = dash_authed_with_role(
            &state,
            &headers,
            &app_id,
            DashRole::Collaborator,
            Scope::DataWrite,
        )
        .await?;
        let webhook_id = path_uuid(&webhook_id, "webhook_id")?;
        let isn = parse_isn(&isn)?;
        match webhooks::requeue(&state, app.id, webhook_id, isn).await? {
            Some(ev) => {
                state.webhook_notify.notify_one();
                Ok(json!({"event": webhooks::event_json(&ev)}))
            }
            None => Err(InstantError::validation_failed_input(
                "webhook-events",
                json!({"app-id": app.id, "webhook-id": webhook_id, "isn": isn.to_string_legacy()}),
                json!([{"message": "Could not resend event. Try again in one minute."}]),
            )),
        }
    }
    .await;
    json_or_err(r)
}

/// GET /.well-known/webhooks/jwks.json
pub async fn jwks(State(state): State<Arc<AppState>>) -> Response {
    match state.webhook_key.get() {
        Some(k) => axum::Json(k.jwks()).into_response(),
        None => json_or_err(Err(InstantError::internal(
            "webhook signing key not loaded",
        ))),
    }
}

/// GET /webhooks/payload/:app_id/:webhook_id/*isn — `req->app-id-and-webhook-authed!`:
/// the payload JWT, or any credential the dashboard routes take.
pub async fn payload(
    State(state): State<Arc<AppState>>,
    Path((app_id, webhook_id, isn)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let webhook_id = path_uuid(&webhook_id, "webhook_id")?;
        let isn = parse_isn(&isn)?;
        let app_uuid = path_uuid(&app_id, "app_id")?;
        let token = crate::routes::superadmin::bearer_string(&headers)?;
        let app_id = if token.starts_with("eyJ") {
            webhooks::verify_payload_jwt(&state, &token, app_uuid, webhook_id, isn)?;
            app_uuid
        } else {
            dash_authed_with_role(
                &state,
                &headers,
                &app_id,
                DashRole::Collaborator,
                Scope::DataRead,
            )
            .await?
            .id
        };
        let webhook = webhooks::get_by_id(&state, app_id, webhook_id).await?;
        let data = webhooks::payload_records(&state, app_id, &webhook, isn).await?;
        Ok(json!({
            "data": data,
            "idempotencyKey": webhooks::payload_idempotency_key(webhook_id, isn),
        }))
    }
    .await;
    match r {
        Ok(v) => (
            [
                (header::CACHE_CONTROL, "no-store, private"),
                (header::PRAGMA, "no-cache"),
                (header::EXPIRES, "0"),
                (header::VARY, "Authorization"),
            ],
            axum::Json(v),
        )
            .into_response(),
        Err(e) => crate::routes::runtime::err_response(&e),
    }
}
