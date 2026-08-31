//! /runtime/session websocket handler — the client sync protocol
//! (see docs/PROTOCOL.md).

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use futures::{SinkExt, StreamExt};
use instant_core::error::InstantError;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::service::{self, PermsCtx};
use crate::state::{value_hash, AppState, QueryEntry, Session, SessionUser};

pub async fn handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| session_loop(socket, state))
}

async fn session_loop(socket: WebSocket, state: Arc<AppState>) {
    let session_id = Uuid::new_v4();
    let (tx, mut rx) = mpsc::unbounded_channel::<Value>();
    let session = Arc::new(Session {
        id: session_id,
        tx,
        state: Default::default(),
        refresh_lock: Default::default(),
    });
    state.sessions.insert(session_id, session.clone());

    let (mut ws_tx, mut ws_rx) = socket.split();

    // writer task: outgoing queue -> socket, plus keepalive pings
    let writer = tokio::spawn(async move {
        let mut ping = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Some(v) => {
                            if ws_tx.send(Message::Text(v.to_string().into())).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                _ = ping.tick() => {
                    if ws_tx.send(Message::Ping(vec![].into())).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    while let Some(Ok(msg)) = ws_rx.next().await {
        match msg {
            Message::Text(text) => {
                let parsed: Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                handle_message(&state, &session, parsed).await;
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    // cleanup
    state.drop_session(session_id);
    state.stream_subs.iter_mut().for_each(|mut e| {
        e.value_mut().retain(|(sid, _)| *sid != session_id);
    });
    crate::presence::leave_all(&state, session_id).await;
    writer.abort();
}

fn err_msg(original: &Value, e: &InstantError) -> Value {
    let mut m = json!({
        "op": "error",
        "status": e.status,
        "type": e.error_type,
        "message": e.message,
        "original-event": original,
    });
    if let Some(h) = &e.hint {
        m["hint"] = h.clone();
    }
    if let Some(ceid) = original.get("client-event-id") {
        m["client-event-id"] = ceid.clone();
    }
    m
}

pub(crate) async fn handle_message(state: &Arc<AppState>, session: &Arc<Session>, msg: Value) {
    let op = msg.get("op").and_then(|o| o.as_str()).unwrap_or("");
    let result = match op {
        "init" => handle_init(state, session, &msg).await,
        "add-query" => handle_add_query(state, session, &msg).await,
        "remove-query" => handle_remove_query(session, &msg).await,
        "transact" => handle_transact(state, session, &msg).await,
        "join-room" => handle_join_room(state, session, &msg).await,
        "leave-room" => handle_leave_room(state, session, &msg).await,
        "set-presence" => handle_set_presence(state, session, &msg).await,
        "client-broadcast" => handle_client_broadcast(state, session, &msg).await,
        "start-sync" => crate::sync_table::handle_start_sync(state, session, &msg).await,
        "resync-table" => crate::sync_table::handle_resync_table(state, session, &msg).await,
        "remove-sync" => crate::sync_table::handle_remove_sync(state, session, &msg).await,
        "start-stream" => crate::streams::handle_start_stream(state, session, &msg).await,
        "append-stream" => crate::streams::handle_append_stream(state, session, &msg).await,
        "subscribe-stream" => crate::streams::handle_subscribe_stream(state, session, &msg).await,
        "unsubscribe-stream" => {
            crate::streams::handle_unsubscribe_stream(state, session, &msg).await
        }
        _ => Ok(()), // unknown ops ignored (client tolerates)
    };
    if let Err(e) = result {
        session.send(err_msg(&msg, &e));
    }
}

type HandlerResult = std::result::Result<(), InstantError>;

/// patch-presence is accepted by @instantdb/core > 0.17.5.
fn supports_patch_presence(versions: Option<&Value>) -> bool {
    let Some(v) = versions
        .and_then(|v| v.get("@instantdb/core"))
        .and_then(|v| v.as_str())
    else {
        return false;
    };
    let v = v.trim_start_matches('v');
    let parts: Vec<u64> = v.split('.').filter_map(|p| p.parse().ok()).collect();
    if parts.len() < 3 {
        return false;
    }
    (parts[0], parts[1], parts[2]) > (0, 17, 5)
}

async fn handle_init(state: &Arc<AppState>, session: &Arc<Session>, msg: &Value) -> HandlerResult {
    let app_id = msg
        .get("app-id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| InstantError::param_missing("missing app-id"))?;
    let app = service::get_app(state, app_id).await?;

    let mut user: Option<SessionUser> = None;
    if let Some(token) = msg.get("refresh-token").and_then(|v| v.as_str()) {
        match crate::auth::user_by_refresh_token(state, app_id, token).await? {
            Some(u) => {
                user = Some(SessionUser {
                    id: u.id,
                    email: u.email,
                });
            }
            None => {
                return Err(InstantError::record_not_found(
                    "app-user",
                    "Could not find user for refresh token.",
                ));
            }
        }
    }
    let mut admin = false;
    if let Some(token) = msg.get("__admin-token").and_then(|v| v.as_str()) {
        admin = crate::auth::check_admin_token(state, app_id, token).await?;
    }

    let attrs = service::load_attrs(state, app_id).await?;
    {
        let mut st = session.state.lock().await;
        st.app_id = Some(app_id);
        st.admin = admin;
        st.user = user.clone();
        st.versions = msg.get("versions").cloned();
        st.supports_patch_presence = supports_patch_presence(msg.get("versions"));
    }
    state.register_app_session(app_id, session.id);

    session.send(json!({
        "op": "init-ok",
        "session-id": session.id,
        "client-event-id": msg.get("client-event-id"),
        "attrs": attrs.to_wire_visible(),
        "auth": {
            "app": {"id": app.id, "title": app.title},
            "user": user.map(|u| json!({"id": u.id, "email": u.email})),
            "admin?": admin,
        },
        "app-status": {"status": app.status},
    }));
    Ok(())
}

async fn session_ctx(
    session: &Arc<Session>,
) -> std::result::Result<(Uuid, PermsCtx), InstantError> {
    let st = session.state.lock().await;
    let app_id = st
        .app_id
        .ok_or_else(|| InstantError::param_malformed("session not initialized"))?;
    Ok((
        app_id,
        PermsCtx {
            admin: st.admin,
            user_id: st.user.as_ref().map(|u| u.id),
            user_map: None,
            rule_params: None,
        },
    ))
}

async fn handle_add_query(
    state: &Arc<AppState>,
    session: &Arc<Session>,
    msg: &Value,
) -> HandlerResult {
    let (app_id, perms) = session_ctx(session).await?;
    let q = msg
        .get("q")
        .cloned()
        .ok_or_else(|| InstantError::param_missing("missing q"))?;
    let key = q.to_string();
    {
        let st = session.state.lock().await;
        if st.queries.contains_key(&key) {
            session.send(json!({
                "op": "add-query-exists",
                "q": q,
                "client-event-id": msg.get("client-event-id"),
            }));
            return Ok(());
        }
    }
    let attrs = service::load_attrs(state, app_id).await?;
    let result = service::run_query(state, app_id, &attrs, &perms, &q).await?;
    let ws_result = result.to_ws_result();
    let processed_tx_id = service::max_tx_id(state, app_id).await?;
    {
        let mut st = session.state.lock().await;
        st.queries.insert(
            key,
            QueryEntry {
                q: q.clone(),
                result_hash: value_hash(&ws_result),
            },
        );
    }
    session.send(json!({
        "op": "add-query-ok",
        "q": q,
        "result": ws_result,
        "processed-tx-id": processed_tx_id,
        "client-event-id": msg.get("client-event-id"),
    }));
    Ok(())
}

async fn handle_remove_query(session: &Arc<Session>, msg: &Value) -> HandlerResult {
    if let Some(q) = msg.get("q") {
        let key = q.to_string();
        let mut st = session.state.lock().await;
        st.queries.remove(&key);
        session.send(json!({
            "op": "remove-query-ok",
            "q": q,
            "client-event-id": msg.get("client-event-id"),
        }));
    }
    Ok(())
}

async fn handle_transact(
    state: &Arc<AppState>,
    session: &Arc<Session>,
    msg: &Value,
) -> HandlerResult {
    let (app_id, perms) = session_ctx(session).await?;
    let steps = msg
        .get("tx-steps")
        .ok_or_else(|| InstantError::param_missing("missing tx-steps"))?;
    let report = service::run_transact(state, app_id, &perms, steps).await?;
    session.send(json!({
        "op": "transact-ok",
        "client-event-id": msg.get("client-event-id"),
        "tx-id": report.tx_id,
    }));
    Ok(())
}

async fn handle_join_room(
    state: &Arc<AppState>,
    session: &Arc<Session>,
    msg: &Value,
) -> HandlerResult {
    let (app_id, _) = session_ctx(session).await?;
    let room_id = msg
        .get("room-id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| InstantError::param_missing("missing room-id"))?;
    let user = {
        let st = session.state.lock().await;
        st.user.as_ref().map(|u| json!({"id": u.id}))
    };
    crate::presence::join_room(
        state,
        app_id,
        room_id,
        session.id,
        user,
        msg.get("data").cloned(),
    )
    .await?;
    {
        let mut st = session.state.lock().await;
        st.rooms.insert(room_id.to_string());
    }
    session.send(json!({
        "op": "join-room-ok",
        "room-id": room_id,
        "client-event-id": msg.get("client-event-id"),
    }));
    // immediate snapshot for the joiner (peers get theirs via NOTIFY)
    let snapshot = crate::presence::room_snapshot(state, app_id, room_id).await?;
    session.send(json!({
        "op": "refresh-presence",
        "room-id": room_id,
        "data": snapshot,
    }));
    Ok(())
}

async fn handle_leave_room(
    state: &Arc<AppState>,
    session: &Arc<Session>,
    msg: &Value,
) -> HandlerResult {
    let (app_id, _) = session_ctx(session).await?;
    let room_id = msg
        .get("room-id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| InstantError::param_missing("missing room-id"))?;
    crate::presence::leave_room(state, app_id, room_id, session.id).await?;
    {
        let mut st = session.state.lock().await;
        st.rooms.remove(room_id);
    }
    session.send(json!({
        "op": "leave-room-ok",
        "room-id": room_id,
        "client-event-id": msg.get("client-event-id"),
    }));
    Ok(())
}

async fn handle_set_presence(
    state: &Arc<AppState>,
    session: &Arc<Session>,
    msg: &Value,
) -> HandlerResult {
    let (app_id, _) = session_ctx(session).await?;
    let room_id = msg
        .get("room-id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| InstantError::param_missing("missing room-id"))?;
    let data = msg.get("data").cloned().unwrap_or(json!({}));
    crate::presence::set_presence(state, app_id, room_id, session.id, data).await?;
    Ok(())
}

async fn handle_client_broadcast(
    state: &Arc<AppState>,
    session: &Arc<Session>,
    msg: &Value,
) -> HandlerResult {
    let (app_id, _) = session_ctx(session).await?;
    let room_id = msg
        .get("room-id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| InstantError::param_missing("missing room-id"))?;
    let user = {
        let st = session.state.lock().await;
        st.user.as_ref().map(|u| json!({"id": u.id}))
    };
    service::notify_json(
        state,
        "instant_broadcast",
        json!({
            "app_id": app_id,
            "room_id": room_id,
            "topic": msg.get("topic").cloned().unwrap_or(Value::Null),
            "peer_id": session.id,
            "user": user,
            "data": msg.get("data").cloned().unwrap_or(Value::Null),
        }),
    )
    .await;
    Ok(())
}
