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
use crate::state::{value_hash, AppState, Outgoing, QueryEntry, Session, SessionUser};

pub async fn handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    // tungstenite preallocates read_buffer_size (128 KiB by default) per
    // connection — ~1.3 GB for 10k idle sockets. Client frames are small
    // (the buffer still grows for a large transact), and every outgoing
    // frame is flushed as it is sent, so neither buffer needs to be large.
    ws.read_buffer_size(8 * 1024)
        .write_buffer_size(0)
        .on_upgrade(move |socket| session_loop(socket, state))
}

async fn session_loop(socket: WebSocket, state: Arc<AppState>) {
    let session_id = Uuid::new_v4();
    let (tx, mut rx) = mpsc::unbounded_channel::<Outgoing>();
    let session = state.new_session(session_id, tx);

    let (mut ws_tx, mut ws_rx) = socket.split();

    // writer task: outgoing queue -> socket, plus keepalive pings. For clients
    // that accept JSON-array frames (core > 0.22.75, Reactor.js:1798-1804),
    // queued-up messages are coalesced into one array frame like legacy does.
    let writer_session = session.clone();
    let writer = tokio::spawn(async move {
        let mut ping = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            tokio::select! {
                msg = rx.recv() => {
                    let Some(v) = msg else { break };
                    let batch_ok = writer_session
                        .batch_messages
                        .load(std::sync::atomic::Ordering::Relaxed);
                    let mut pending = vec![v];
                    if batch_ok {
                        while pending.len() < 100 {
                            match rx.try_recv() {
                                Ok(next) => pending.push(next),
                                Err(_) => break,
                            }
                        }
                    }
                    writer_session.dequeued(pending.len());
                    if writer_session
                        .overflowed
                        .load(std::sync::atomic::Ordering::Relaxed)
                    {
                        // slow consumer: the queue cap was hit and messages
                        // were dropped, so the client's view is no longer
                        // consistent — close and let it reconnect
                        let _ = ws_tx.send(Message::Close(None)).await;
                        break;
                    }
                    let frame = if pending.len() == 1 {
                        pending.pop().unwrap().into_string()
                    } else {
                        // JSON-array frame without re-parsing raw members
                        let parts: Vec<String> =
                            pending.into_iter().map(Outgoing::into_string).collect();
                        let mut frame = String::with_capacity(
                            parts.iter().map(|p| p.len() + 1).sum::<usize>() + 2,
                        );
                        frame.push('[');
                        for (i, p) in parts.iter().enumerate() {
                            if i > 0 {
                                frame.push(',');
                            }
                            frame.push_str(p);
                        }
                        frame.push(']');
                        frame
                    };
                    if ws_tx.send(Message::Text(frame.into())).await.is_err() {
                        break;
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
                crate::metrics::METRICS.ws_messages_received_total.inc();
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

/// Error frame matching legacy handle-error! key-for-key (session.clj:589-616):
/// status/client-event-id/original-event/type/message/hint are always present,
/// null when unknown.
fn err_msg(original: &Value, e: &InstantError) -> Value {
    json!({
        "op": "error",
        "status": e.status,
        "type": e.error_type,
        "message": e.message,
        "hint": e.hint.clone().unwrap_or(Value::Null),
        "original-event": original,
        "client-event-id": original.get("client-event-id").cloned().unwrap_or(Value::Null),
    })
}

pub(crate) async fn handle_message(state: &Arc<AppState>, session: &Arc<Session>, msg: Value) {
    let op = msg.get("op").and_then(|o| o.as_str()).unwrap_or("");
    // Per-app rate limit (issue #1). `init` carries the app id in the
    // message; every later op uses the session's app. Pre-init ops have no
    // app scope and fail in their handlers anyway.
    let limit_app_id = if op == "init" {
        msg.get("app-id")
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok())
    } else {
        session.state.lock().await.app_id
    };
    if let Some(app_id) = limit_app_id {
        if let Err(retry) = state
            .limiters
            .ws
            .check(app_id, crate::rate_limit::ws_op_cost(op))
        {
            session.send(err_msg(&msg, &crate::rate_limit::rate_limited_err(retry)));
            return;
        }
    }
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

/// Parsed @instantdb/core version from the init `versions` map.
fn core_version(versions: Option<&Value>) -> Option<(u64, u64, u64)> {
    let v = versions?.get("@instantdb/core")?.as_str()?;
    let v = v.trim_start_matches('v');
    let parts: Vec<u64> = v.split('.').filter_map(|p| p.parse().ok()).collect();
    if parts.len() < 3 {
        return None;
    }
    Some((parts[0], parts[1], parts[2]))
}

/// patch-presence is accepted by @instantdb/core > 0.17.5.
fn supports_patch_presence(versions: Option<&Value>) -> bool {
    core_version(versions).is_some_and(|v| v > (0, 17, 5))
}

/// refresh-ok may omit unchanged attrs for @instantdb/core > 0.20.4
/// (legacy refresh-skip-attrs-min-version, session.clj:96).
fn supports_skip_attrs(versions: Option<&Value>) -> bool {
    core_version(versions).is_some_and(|v| v > (0, 20, 4))
}

/// JSON-array frames are accepted by @instantdb/core > 0.22.75
/// (legacy batch-messages-min-version, session.clj:99).
fn supports_batch_messages(versions: Option<&Value>) -> bool {
    core_version(versions).is_some_and(|v| v > (0, 22, 75))
}

async fn handle_init(state: &Arc<AppState>, session: &Arc<Session>, msg: &Value) -> HandlerResult {
    {
        // legacy rejects a second init on the same session (session.clj:158-159)
        let st = session.state.lock().await;
        if st.app_id.is_some() {
            return Err(InstantError::validation_failed(
                "init",
                "`init` has already run for this session.",
                json!([{"message": "`init` has already run for this session."}]),
            ));
        }
    }
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
    let wire_attrs = attrs.to_wire_visible();
    {
        let mut st = session.state.lock().await;
        st.app_id = Some(app_id);
        st.admin = admin;
        st.user = user.clone();
        st.versions = msg.get("versions").cloned();
        st.supports_patch_presence = supports_patch_presence(msg.get("versions"));
        st.supports_skip_attrs = supports_skip_attrs(msg.get("versions"));
        if st.supports_skip_attrs {
            st.attrs_hash = Some(crate::state::value_hash(&wire_attrs));
        }
    }
    session.batch_messages.store(
        supports_batch_messages(msg.get("versions")),
        std::sync::atomic::Ordering::Relaxed,
    );
    state.register_app_session(app_id, session.id);

    session.send(json!({
        "op": "init-ok",
        "session-id": session.id,
        "client-event-id": msg.get("client-event-id"),
        "attrs": wire_attrs,
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
    let started = std::time::Instant::now();
    let attrs = service::load_attrs(state, app_id).await?;
    let outcome = service::run_query_full(state, app_id, &attrs, &perms, &q, None).await?;
    let ws_result = outcome.result.to_ws_result();
    let processed_tx_id = service::max_tx_id(state, app_id).await?;
    {
        let mut st = session.state.lock().await;
        st.queries.insert(
            key,
            QueryEntry {
                q: q.clone(),
                result_hash: value_hash(&ws_result),
                topics: Some(Arc::new(outcome.topics)),
            },
        );
    }
    crate::metrics::METRICS
        .add_query_seconds
        .observe_since(started);
    session.send(json!({
        "op": "add-query-ok",
        "q": q,
        "result": ws_result,
        // legacy add-query-ok superset (session.clj:264-270): result-meta is
        // only populated for the tree return-type (admin SSE), null on the
        // ws join-rows path; processed-isn is the WAL watermark.
        "result-meta": Value::Null,
        "processed-tx-id": processed_tx_id,
        "processed-isn": service::current_isn(state).await,
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
        // legacy attaches the tx's ISN (session.clj:580-584); reading the WAL
        // position after commit keeps refresh isn >= transact isn.
        "isn": service::current_isn(state).await,
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
    assert_in_room(session, room_id).await?;
    let data = msg.get("data").cloned().unwrap_or(json!({}));
    crate::presence::set_presence(state, app_id, room_id, session.id, data).await?;
    // legacy acks with set-presence-ok (session.clj:679-681); the client has
    // no handler for it but suppresses it from logs.
    session.send(json!({
        "op": "set-presence-ok",
        "room-id": room_id,
        "client-event-id": msg.get("client-event-id"),
    }));
    Ok(())
}

/// Legacy asserts room membership before set-presence / client-broadcast
/// (session.clj assert-in-room!).
async fn assert_in_room(session: &Arc<Session>, room_id: &str) -> HandlerResult {
    let st = session.state.lock().await;
    if !st.rooms.contains(room_id) {
        return Err(InstantError::validation_failed(
            "room",
            "You have not entered this room yet.",
            json!([{"message": "You have not entered this room yet."}]),
        ));
    }
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
    assert_in_room(session, room_id).await?;
    let user = {
        let st = session.state.lock().await;
        st.user.as_ref().map(|u| json!({"id": u.id}))
    };
    let topic = msg.get("topic").cloned().unwrap_or(Value::Null);
    let data = msg.get("data").cloned().unwrap_or(Value::Null);
    service::notify_json(
        state,
        "instant_broadcast",
        json!({
            "app_id": app_id,
            "room_id": room_id,
            "topic": topic.clone(),
            "peer_id": session.id,
            "user": user.clone(),
            "data": data.clone(),
        }),
    )
    .await;
    // legacy acks the sender with client-broadcast-ok carrying the fanned-out
    // envelope (session.clj:728-730); the client ignores unknown ops.
    session.send(json!({
        "op": "client-broadcast-ok",
        "room-id": room_id,
        "topic": topic,
        "data": {"peer-id": session.id, "user": user, "data": data},
        "client-event-id": msg.get("client-event-id"),
    }));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn versions(core: &str) -> Value {
        json!({"@instantdb/core": core})
    }

    #[test]
    fn version_gates_match_legacy_thresholds() {
        // patch-presence: core > 0.17.5
        assert!(!supports_patch_presence(None));
        assert!(!supports_patch_presence(Some(&versions("v0.17.5"))));
        assert!(supports_patch_presence(Some(&versions("v0.17.6"))));
        // skip-attrs: core > 0.20.4
        assert!(!supports_skip_attrs(Some(&versions("v0.20.4"))));
        assert!(supports_skip_attrs(Some(&versions("v0.20.5"))));
        assert!(supports_skip_attrs(Some(&versions("v0.21.0"))));
        // batch-messages: core > 0.22.75
        assert!(!supports_batch_messages(Some(&versions("v0.22.75"))));
        assert!(supports_batch_messages(Some(&versions("v0.22.76"))));
        assert!(supports_batch_messages(Some(&versions("v1.0.0"))));
        // unparseable versions get no features (legacy get-supported-features)
        assert!(!supports_skip_attrs(Some(&versions("beta"))));
        assert!(!supports_skip_attrs(Some(
            &json!({"@instantdb/react": "v9.9.9"})
        )));
    }

    #[test]
    fn err_msg_has_legacy_key_set() {
        let e = InstantError::param_missing("missing q");
        let m = err_msg(&json!({"op": "add-query"}), &e);
        let keys: Vec<&str> = m.as_object().unwrap().keys().map(|k| k.as_str()).collect();
        for k in [
            "op",
            "status",
            "type",
            "message",
            "hint",
            "original-event",
            "client-event-id",
        ] {
            assert!(keys.contains(&k), "missing key {k}");
        }
        assert!(m["hint"].is_null());
        assert!(m["client-event-id"].is_null());
    }
}
