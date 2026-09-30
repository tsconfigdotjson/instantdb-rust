//! Server-sent-event transports over the reactive session machinery
//! (LEGACY reactive/session.clj, admin/routes.clj):
//!
//! - `/runtime/sse` — the client SDK's fallback transport. The stream opens
//!   with an `sse-init` event; the client POSTs its messages back to the same
//!   URL with the machine/session/token envelope.
//! - `POST /admin/subscribe-query` — `@instantdb/admin` `subscribeQuery`:
//!   an admin-authed session that registers the body's query with the
//!   `tree` return-type (object tree + `result-meta`) and streams
//!   `add-query-ok` / `refresh-ok` / `error`.
//! - `POST /admin/sse` + `POST /admin/sse/push` — the generic admin session
//!   (`db.streams`): same auth, messages pushed back over HTTP.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::Json;
use instant_core::error::{InstantError, Result};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::routes::dash::{param_malformed, param_missing};
use crate::routes::runtime::err_response;
use crate::state::{AppState, Outgoing, Session, SessionUser};

/// Legacy `sse-retry-interval-ms` flag default (session.clj:1375-1377): the
/// admin streams tell EventSource to reconnect half a second after a drop.
const ADMIN_RETRY: std::time::Duration = std::time::Duration::from_millis(500);

pub async fn stream(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    // legacy sse-get: `(ex/get-param! req [:params :app_id] uuid-util/coerce)`
    // (runtime/routes.clj:57-62)
    if let Err(e) = uuid_param(
        &["params", "app_id"],
        params
            .get("app_id")
            .map(|v| Value::String(v.clone()))
            .as_ref(),
    ) {
        return err_response(&e);
    }
    let session_id = Uuid::new_v4();
    let sse_token = Uuid::new_v4();
    let (tx, rx) = mpsc::unbounded_channel::<Outgoing>();
    let session = state.new_session(session_id, tx);
    {
        let request = crate::ws::request_ctx_from_headers(&headers);
        let mut st = session.state.lock().await;
        st.sse_token = Some(sse_token);
        st.ip = request.ip;
        st.origin = request.origin;
    }
    send_sse_init(&state, &session, sse_token);
    open_stream(state, session, rx, None)
}

fn send_sse_init(state: &AppState, session: &Session, sse_token: Uuid) {
    // legacy handle-sse-init! key set (session.clj:204-209)
    session.send(json!({
        "op": "sse-init",
        "machine-id": state.node_id,
        "session-id": session.id,
        "sse-token": sse_token,
    }));
}

/// Turns a session's outgoing queue into the `text/event-stream` response.
/// The session is torn down when the response body is dropped.
fn open_stream(
    state: Arc<AppState>,
    session: Arc<Session>,
    rx: mpsc::UnboundedReceiver<Outgoing>,
    retry: Option<std::time::Duration>,
) -> Response {
    let guard = RxGuard {
        rx: Some(rx),
        state,
        session,
    };
    let event_stream = futures::stream::unfold(guard, move |mut guard| async move {
        if guard
            .session
            .overflowed
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            // slow consumer: end the stream, the client reconnects
            return None;
        }
        match guard.rx().recv().await {
            Some(msg) => {
                guard.session.dequeued(1);
                let event = Event::default().data(msg.into_string());
                Some((Ok::<Event, Infallible>(event), guard))
            }
            None => None,
        }
    });
    // legacy sends the retry hint in on-open, before sse-init is queued
    let retry_stream =
        futures::stream::iter(retry.map(|d| Ok::<Event, Infallible>(Event::default().retry(d))));
    Sse::new(futures::StreamExt::chain(retry_stream, event_stream))
        .keep_alive(KeepAlive::new().interval(std::time::Duration::from_secs(15)))
        .into_response()
}

/// Holds the receiver and cleans up the session when the SSE stream drops.
struct RxGuard {
    rx: Option<mpsc::UnboundedReceiver<Outgoing>>,
    state: Arc<AppState>,
    session: Arc<Session>,
}

impl RxGuard {
    fn rx(&mut self) -> &mut mpsc::UnboundedReceiver<Outgoing> {
        self.rx.as_mut().unwrap()
    }
}

impl Drop for RxGuard {
    fn drop(&mut self) {
        let state = self.state.clone();
        let session = self.session.clone();
        let session_id = session.id;
        tokio::spawn(async move {
            session.scheduler.shutdown().await;
            state.drop_session(session_id);
            crate::presence::leave_all(&state, session_id).await;
        });
    }
}

/// `POST /runtime/sse?app_id=...` — legacy sse-post (runtime/routes.clj
/// :64-70): the params in its order, then the same session lookup as the
/// admin push.
pub async fn push(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    let r = async {
        let body = parse_body(&body)?;
        let machine_id = uuid_param(&["body", "machine_id"], body.get("machine_id"))?;
        uuid_param(
            &["params", "app_id"],
            params
                .get("app_id")
                .map(|v| Value::String(v.clone()))
                .as_ref(),
        )?;
        let session_id = uuid_param(&["body", "session_id"], body.get("session_id"))?;
        let sse_token = uuid_param(&["body", "sse_token"], body.get("sse_token"))?;
        let messages = body
            .get("messages")
            .filter(|m| !m.is_null())
            .ok_or_else(|| param_missing(&["body", "messages"]))?
            .clone();
        enqueue_messages(&state, machine_id, session_id, sse_token, &messages).await
    }
    .await;
    match r {
        Ok(v) => Json(v).into_response(),
        Err(e) => err_response(&e),
    }
}

// ---------------------------------------------------------------------------
// admin SSE (legacy admin/routes.clj:160-210, session.clj:1358-1396)

/// Body of the admin SSE requests; the admin SDK always sends JSON, but an
/// empty body opens a plain session too.
fn parse_body(body: &Bytes) -> Result<Value> {
    if body.iter().all(|b| b.is_ascii_whitespace()) {
        return Ok(Value::Null);
    }
    serde_json::from_slice(body)
        .map_err(|e| InstantError::param_malformed(format!("Malformed JSON body: {e}")))
}

/// `POST /admin/subscribe-query?local_connection_id=...` — subscribeQuery.
pub async fn admin_subscribe_query(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    let body = match parse_body(&body) {
        Ok(b) => b,
        Err(e) => return err_response(&e),
    };
    // legacy query-sse validates the query before auth (routes.clj:161)
    let q = match crate::routes::admin::body_query(&body) {
        Ok(q) => q.clone(),
        Err(e) => return err_response(&e),
    };
    match open_admin_session(&state, &headers, &params, &body).await {
        Ok((session, rx)) => {
            // queued behind sse-init like legacy's receive-queue
            // (session.clj:1386-1392); the add-query runs once the response
            // is streaming
            let st = state.clone();
            let sess = session.clone();
            tokio::spawn(async move {
                let msg = json!({
                    "op": "add-query",
                    "q": q,
                    "return-type": "tree",
                    "client-event-id": Uuid::new_v4(),
                });
                crate::scheduler::dispatch(&st, &sess, msg).await;
            });
            open_stream(state, session, rx, Some(ADMIN_RETRY))
        }
        Err(e) => err_response(&e),
    }
}

/// `POST /admin/sse?app_id=...` — generic admin session (db.streams).
pub async fn admin_stream(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    let body = match parse_body(&body) {
        Ok(b) => b,
        Err(e) => return err_response(&e),
    };
    match open_admin_session(&state, &headers, &params, &body).await {
        Ok((session, rx)) => open_stream(state, session, rx, Some(ADMIN_RETRY)),
        Err(e) => err_response(&e),
    }
}

/// Authenticates like every /admin route and pre-initializes a session the
/// way legacy admin-init! does (session.clj:211-226): no `init` round trip,
/// auth from the admin / impersonation headers, `inference?` and `versions`
/// from the body. Legacy never derives feature flags for these sessions
/// (`:session/features` stays unset), so refresh-ok always carries attrs and
/// frames are never batched.
async fn open_admin_session(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
    body: &Value,
) -> Result<(Arc<Session>, mpsc::UnboundedReceiver<Outgoing>)> {
    let ctx = crate::routes::admin::authed(state, headers, params).await?;
    let inference = body
        .get("inference?")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let session_id = Uuid::new_v4();
    let sse_token = Uuid::new_v4();
    let (tx, rx) = mpsc::unbounded_channel::<Outgoing>();
    let session = state.new_session(session_id, tx);
    {
        let mut st = session.state.lock().await;
        st.app_id = Some(ctx.app_id);
        st.admin = ctx.perms.admin;
        st.user = ctx.perms.user_id.map(|id| SessionUser { id, email: None });
        st.versions = body.get("versions").cloned();
        st.inference = inference;
        st.sse_token = Some(sse_token);
        st.ip = ctx.perms.ip.clone();
        st.origin = ctx.perms.origin.clone();
    }
    state.register_app_session(ctx.app_id, session_id);
    send_sse_init(state, &session, sse_token);
    Ok((session, rx))
}

/// `POST /admin/sse/push?app_id=...` — body `{machine_id, session_id,
/// sse_token, messages}` (legacy admin-sse-push, routes.clj:203-210;
/// sse-on-messages, session.clj:1250-1261).
pub async fn admin_push(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    match admin_push_impl(&state, &headers, &params, &body).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => err_response(&e),
    }
}

pub(crate) fn uuid_param(ks: &[&str], v: Option<&Value>) -> Result<Uuid> {
    let v = v
        .filter(|v| !v.is_null())
        .ok_or_else(|| param_missing(ks))?;
    v.as_str()
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| param_malformed(ks, v.clone()))
}

async fn admin_push_impl(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
    body: &Bytes,
) -> Result<Value> {
    // the with-rate-limiting wrapper reads the app id first (header, else
    // query param) and its error names the header path
    let app_id = crate::routes::admin::app_id_param(headers, params)?;
    state
        .limiters
        .admin
        .check(app_id, 1.0)
        .map_err(crate::rate_limit::rate_limited_err)?;
    let body = parse_body(body)?;
    let machine_id = uuid_param(&["body", "machine_id"], body.get("machine_id"))?;
    let session_id = uuid_param(&["body", "session_id"], body.get("session_id"))?;
    let sse_token = uuid_param(&["body", "sse_token"], body.get("sse_token"))?;
    let messages = body
        .get("messages")
        .filter(|m| !m.is_null())
        .ok_or_else(|| param_missing(&["body", "messages"]))?
        .clone();
    enqueue_messages(state, machine_id, session_id, sse_token, &messages).await
}

/// Legacy `sse/enqueue-messages` (session.clj:1250-1261): find the session,
/// check its token, queue the messages.
async fn enqueue_messages(
    state: &Arc<AppState>,
    machine_id: Uuid,
    session_id: Uuid,
    sse_token: Uuid,
    messages: &Value,
) -> Result<Value> {
    let session = state.sessions.get(&session_id).map(|s| s.clone());
    let session = match session {
        Some(s) => s,
        // the session is not on this node. Legacy forwards to the machine
        // that owns it over hazelcast; this server has no cluster transport,
        // so SSE needs session affinity — report it the way legacy does when
        // the owning machine is gone
        None if machine_id != state.node_id => {
            return Err(InstantError::new(
                "member-missing",
                400,
                format!("Member missing for machine id: {machine_id}"),
                Some(json!({"machine-id": machine_id})),
            ));
        }
        None => return Err(session_missing(session_id)),
    };
    {
        // constant-time compare in legacy; a uuid equality is not an oracle
        // anyone can time from outside
        let st = session.state.lock().await;
        if st.sse_token != Some(sse_token) {
            return Err(session_missing(session_id));
        }
    }
    if let Some(messages) = messages.as_array() {
        for msg in messages {
            crate::scheduler::dispatch(state, &session, msg.clone()).await;
        }
    }
    Ok(json!({}))
}

/// Legacy sse-on-messages calls `throw-session-missing!` with a map instead
/// of the id (session.clj:1256), so the wire shows the printed map in the
/// message and nested under the hint — reproduced verbatim.
fn session_missing(session_id: Uuid) -> InstantError {
    InstantError::new(
        "session-missing",
        400,
        format!("Session missing for id: {{:sess-id #uuid \"{session_id}\"}}"),
        Some(json!({"sess-id": {"sess-id": session_id}})),
    )
}
