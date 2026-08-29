//! LISTEN/NOTIFY driven invalidation + room/broadcast fan-out. Any node can
//! serve any session; coordination happens entirely through Postgres.

use std::sync::Arc;

use serde_json::{json, Value};
use sqlx::postgres::PgListener;
use uuid::Uuid;

use crate::service::{self, PermsCtx};
use crate::state::{value_hash, AppState, Session};

pub async fn run(state: Arc<AppState>) {
    loop {
        match listen_once(&state).await {
            Ok(()) => {}
            Err(e) => {
                tracing::error!("invalidator listener error: {e}; reconnecting in 1s");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

async fn listen_once(state: &Arc<AppState>) -> Result<(), sqlx::Error> {
    let mut listener = PgListener::connect_with(&state.pool).await?;
    listener
        .listen_all(["instant_tx", "instant_room", "instant_broadcast"])
        .await?;
    loop {
        let notification = listener.recv().await?;
        let payload: Value = match serde_json::from_str(notification.payload()) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let payload = match service::resolve_spill(state, payload).await {
            Some(p) => p,
            None => continue,
        };
        match notification.channel() {
            "instant_tx" => {
                let (Some(app_id), Some(tx_id)) = (
                    payload.get("app_id").and_then(|v| v.as_str()).and_then(|s| Uuid::parse_str(s).ok()),
                    payload.get("tx_id").and_then(|v| v.as_i64()),
                ) else {
                    continue;
                };
                let state = state.clone();
                tokio::spawn(async move {
                    refresh_app_sessions(state, app_id, tx_id).await;
                });
            }
            "instant_room" => {
                let (Some(app_id), Some(room_id)) = (
                    payload.get("app_id").and_then(|v| v.as_str()).and_then(|s| Uuid::parse_str(s).ok()),
                    payload.get("room_id").and_then(|v| v.as_str()).map(|s| s.to_string()),
                ) else {
                    continue;
                };
                let state = state.clone();
                tokio::spawn(async move {
                    crate::presence::broadcast_room_refresh(&state, app_id, &room_id).await;
                });
            }
            "instant_broadcast" => {
                let (Some(app_id), Some(room_id)) = (
                    payload.get("app_id").and_then(|v| v.as_str()).and_then(|s| Uuid::parse_str(s).ok()),
                    payload.get("room_id").and_then(|v| v.as_str()).map(|s| s.to_string()),
                ) else {
                    continue;
                };
                let peer_id = payload
                    .get("peer_id")
                    .and_then(|v| v.as_str())
                    .and_then(|s| Uuid::parse_str(s).ok());
                let msg = json!({
                    "op": "server-broadcast",
                    "room-id": room_id,
                    "topic": payload.get("topic").cloned().unwrap_or(Value::Null),
                    "data": {
                        "peer-id": peer_id,
                        "user": payload.get("user").cloned().unwrap_or(Value::Null),
                        "data": payload.get("data").cloned().unwrap_or(Value::Null),
                    },
                });
                for s in state.sessions_for_room(app_id, &room_id) {
                    if Some(s.id) != peer_id {
                        s.send(msg.clone());
                    }
                }
            }
            _ => {}
        }
    }
}

/// Recompute all registered queries for this app's local sessions; push
/// refresh-ok with the changed ones.
pub async fn refresh_app_sessions(state: Arc<AppState>, app_id: Uuid, tx_id: i64) {
    let sessions = state.sessions_for_app(app_id);
    if sessions.is_empty() {
        return;
    }
    let attrs = match service::load_attrs(&state, app_id).await {
        Ok(a) => a,
        Err(e) => {
            tracing::error!("refresh: failed to load attrs: {e}");
            return;
        }
    };
    for session in sessions {
        let state = state.clone();
        let attrs = attrs.clone();
        tokio::spawn(async move {
            refresh_session(state, session, app_id, tx_id, attrs).await;
        });
    }
}

async fn refresh_session(
    state: Arc<AppState>,
    session: Arc<Session>,
    app_id: Uuid,
    tx_id: i64,
    attrs: instant_core::attr::AttrMap,
) {
    let _guard = session.refresh_lock.lock().await;
    // snapshot queries + auth under the state lock
    let (queries, perms) = {
        let st = session.state.lock().await;
        if st.app_id != Some(app_id) {
            return;
        }
        let perms = PermsCtx {
            admin: st.admin,
            user_id: st.user.as_ref().map(|u| u.id),
            user_map: None,
            rule_params: None,
        };
        (st.queries.clone(), perms)
    };
    let mut computations = vec![];
    let mut new_hashes = vec![];
    for (key, entry) in &queries {
        match service::run_query(&state, app_id, &attrs, &perms, &entry.q).await {
            Ok(result) => {
                let ws_result = result.to_ws_result();
                let h = value_hash(&ws_result);
                if h != entry.result_hash {
                    computations.push(json!({
                        "instaql-query": entry.q,
                        "instaql-result": ws_result,
                    }));
                    new_hashes.push((key.clone(), h));
                }
            }
            Err(e) => {
                tracing::warn!("refresh query failed: {e}");
            }
        }
    }
    if computations.is_empty() {
        return;
    }
    {
        let mut st = session.state.lock().await;
        for (key, h) in new_hashes {
            if let Some(entry) = st.queries.get_mut(&key) {
                entry.result_hash = h;
            }
        }
    }
    session.send(json!({
        "op": "refresh-ok",
        "processed-tx-id": tx_id,
        "attrs": attrs.to_wire_visible(),
        "computations": computations,
    }));
}
