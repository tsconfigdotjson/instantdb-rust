//! Presence/rooms backed by Postgres (stateless across nodes) with
//! LISTEN/NOTIFY fan-out.

use instant_core::error::{InstantError, Result};
use serde_json::{json, Map, Value};
use sqlx::Row;
use uuid::Uuid;

use crate::service::notify_json;
use crate::state::AppState;

pub async fn join_room(
    state: &AppState,
    app_id: Uuid,
    room_id: &str,
    session_id: Uuid,
    user: Option<Value>,
    data: Option<Value>,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO rust_presence (app_id, room_id, session_id, user_json, data, node_id)
        VALUES ($1, $2, $3, $4, coalesce($5, '{}'::jsonb), $6)
        ON CONFLICT (app_id, room_id, session_id)
        DO UPDATE SET data = coalesce($5, rust_presence.data), updated_at = now()
        "#,
    )
    .bind(app_id)
    .bind(room_id)
    .bind(session_id)
    .bind(user)
    .bind(data)
    .bind(state.node_id)
    .execute(&state.pool)
    .await
    .map_err(InstantError::from)?;
    state
        .room_sessions
        .entry((app_id, room_id.to_string()))
        .or_default()
        .insert(session_id);
    notify_room(state, app_id, room_id).await;
    Ok(())
}

pub async fn set_presence(
    state: &AppState,
    app_id: Uuid,
    room_id: &str,
    session_id: Uuid,
    data: Value,
) -> Result<()> {
    sqlx::query(
        "UPDATE rust_presence SET data = $4, updated_at = now()
         WHERE app_id = $1 AND room_id = $2 AND session_id = $3",
    )
    .bind(app_id)
    .bind(room_id)
    .bind(session_id)
    .bind(data)
    .execute(&state.pool)
    .await
    .map_err(InstantError::from)?;
    notify_room(state, app_id, room_id).await;
    Ok(())
}

pub async fn leave_room(
    state: &AppState,
    app_id: Uuid,
    room_id: &str,
    session_id: Uuid,
) -> Result<()> {
    sqlx::query(
        "DELETE FROM rust_presence WHERE app_id = $1 AND room_id = $2 AND session_id = $3",
    )
    .bind(app_id)
    .bind(room_id)
    .bind(session_id)
    .execute(&state.pool)
    .await
    .map_err(InstantError::from)?;
    if let Some(mut set) = state.room_sessions.get_mut(&(app_id, room_id.to_string())) {
        set.remove(&session_id);
    }
    notify_room(state, app_id, room_id).await;
    Ok(())
}

/// Remove a session from every room it joined (on disconnect).
pub async fn leave_all(state: &AppState, session_id: Uuid) {
    let rows = sqlx::query(
        "DELETE FROM rust_presence WHERE session_id = $1 RETURNING app_id, room_id",
    )
    .bind(session_id)
    .fetch_all(&state.pool)
    .await
    .unwrap_or_default();
    for row in rows {
        let app_id: Uuid = row.get("app_id");
        let room_id: String = row.get("room_id");
        notify_room(state, app_id, &room_id).await;
    }
}

async fn notify_room(state: &AppState, app_id: Uuid, room_id: &str) {
    notify_json(
        state,
        "instant_room",
        json!({"app_id": app_id, "room_id": room_id}),
    )
    .await;
}

/// Room snapshot in refresh-presence shape:
/// {session-id: {"peer-id": sid, "user": ..., "data": {...}}}
pub async fn room_snapshot(state: &AppState, app_id: Uuid, room_id: &str) -> Result<Value> {
    let rows = sqlx::query(
        "SELECT session_id, user_json, data FROM rust_presence
         WHERE app_id = $1 AND room_id = $2",
    )
    .bind(app_id)
    .bind(room_id)
    .fetch_all(&state.pool)
    .await
    .map_err(InstantError::from)?;
    let mut m = Map::new();
    for row in rows {
        let sid: Uuid = row.get("session_id");
        let user: Option<Value> = row.get("user_json");
        let data: Value = row.get("data");
        m.insert(
            sid.to_string(),
            json!({"peer-id": sid, "user": user, "data": data}),
        );
    }
    Ok(Value::Object(m))
}

/// Fan a room's presence change to this node's local sessions: incremental
/// patch-presence for clients that support it, full refresh-presence otherwise.
pub async fn broadcast_room_refresh(state: &AppState, app_id: Uuid, room_id: &str) {
    let sessions = state.sessions_for_room(app_id, room_id);
    let key = (app_id, room_id.to_string());
    if sessions.is_empty() {
        state.room_snapshots.remove(&key);
        return;
    }
    let Ok(snapshot) = room_snapshot(state, app_id, room_id).await else {
        return;
    };
    let prev = state.room_snapshots.insert(key, snapshot.clone());
    let edits = prev.as_ref().and_then(|p| diff_snapshots(p, &snapshot));
    let refresh_msg = json!({
        "op": "refresh-presence",
        "room-id": room_id,
        "data": snapshot,
    });
    let patch_msg = edits.map(|edits| {
        json!({
            "op": "patch-presence",
            "room-id": room_id,
            "edits": edits,
        })
    });
    for s in sessions {
        let use_patch = {
            let st = s.state.try_lock();
            match st {
                Ok(st) => st.supports_patch_presence,
                Err(_) => false,
            }
        };
        match (&patch_msg, use_patch) {
            (Some(p), true) => s.send(p.clone()),
            _ => s.send(refresh_msg.clone()),
        }
    }
}

/// Edits transforming `prev` into `next`: [[path, op, value?], ...] with ops
/// "+" (add peer), "-" (remove peer), "r" (replace a peer's data).
fn diff_snapshots(prev: &Value, next: &Value) -> Option<Vec<Value>> {
    let (prev, next) = (prev.as_object()?, next.as_object()?);
    let mut edits = vec![];
    for (sid, entry) in next {
        match prev.get(sid) {
            None => edits.push(json!([[sid], "+", entry])),
            Some(old) => {
                if old.get("data") != entry.get("data") {
                    edits.push(json!([[sid, "data"], "r", entry.get("data")]));
                }
            }
        }
    }
    for sid in prev.keys() {
        if !next.contains_key(sid) {
            edits.push(json!([[sid], "-"]));
        }
    }
    if edits.is_empty() {
        None
    } else {
        Some(edits)
    }
}

/// Periodic sweeper: heartbeat this node, purge presence rows of dead nodes
/// and stale spill payloads.
pub async fn heartbeat_loop(state: std::sync::Arc<AppState>) {
    loop {
        let _ = sqlx::query(
            "INSERT INTO rust_nodes (node_id, heartbeat_at) VALUES ($1, now())
             ON CONFLICT (node_id) DO UPDATE SET heartbeat_at = now()",
        )
        .bind(state.node_id)
        .execute(&state.pool)
        .await;
        let stale = sqlx::query(
            "DELETE FROM rust_presence WHERE node_id IN (
               SELECT node_id FROM rust_nodes WHERE heartbeat_at < now() - interval '30 seconds')
             RETURNING app_id, room_id",
        )
        .fetch_all(&state.pool)
        .await
        .unwrap_or_default();
        for row in stale {
            let app_id: Uuid = row.get("app_id");
            let room_id: String = row.get("room_id");
            notify_room(&state, app_id, &room_id).await;
        }
        let _ = sqlx::query(
            "DELETE FROM rust_nodes WHERE heartbeat_at < now() - interval '60 seconds'",
        )
        .execute(&state.pool)
        .await;
        let _ = sqlx::query(
            "DELETE FROM rust_spill WHERE created_at < now() - interval '5 minutes'",
        )
        .execute(&state.pool)
        .await;
        let _ = sqlx::query(
            "DELETE FROM rust_tx_changes WHERE logged_at < now() - interval '1 hour'",
        )
        .execute(&state.pool)
        .await;
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
}
