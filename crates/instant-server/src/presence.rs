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
    let row = sqlx::query(
        r#"
        INSERT INTO rust_presence (app_id, room_id, session_id, user_json, data, node_id)
        VALUES ($1, $2, $3, $4, coalesce($5, '{}'::jsonb), $6)
        ON CONFLICT (app_id, room_id, session_id)
        DO UPDATE SET data = coalesce($5, rust_presence.data), updated_at = now()
        RETURNING user_json, data
        "#,
    )
    .bind(app_id)
    .bind(room_id)
    .bind(session_id)
    .bind(user)
    .bind(data)
    .bind(state.node_id)
    .fetch_one(&state.pool)
    .await
    .map_err(InstantError::from)?;
    state
        .room_sessions
        .entry((app_id, room_id.to_string()))
        .or_default()
        .insert(session_id);
    let user: Option<Value> = row.get("user_json");
    let data: Value = row.get("data");
    notify_room(state, app_id, room_id, delta_set(session_id, user, data)).await;
    Ok(())
}

/// NOTIFY delta for a peer joining/updating: nodes with a cached snapshot
/// apply it instead of re-reading the room.
fn delta_set(session_id: Uuid, user: Option<Value>, data: Value) -> Value {
    json!({"set": {"session_id": session_id, "user": user, "data": data}})
}

fn delta_leave(session_id: Uuid) -> Value {
    json!({"leave": session_id})
}

pub async fn set_presence(
    state: &AppState,
    app_id: Uuid,
    room_id: &str,
    session_id: Uuid,
    data: Value,
) -> Result<()> {
    let row = sqlx::query(
        "UPDATE rust_presence SET data = $4, updated_at = now()
         WHERE app_id = $1 AND room_id = $2 AND session_id = $3
         RETURNING user_json, data",
    )
    .bind(app_id)
    .bind(room_id)
    .bind(session_id)
    .bind(data)
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    let delta = match row {
        Some(row) => delta_set(session_id, row.get("user_json"), row.get("data")),
        // not in the room: nothing changed, but keep the refresh semantics
        None => Value::Null,
    };
    notify_room(state, app_id, room_id, delta).await;
    Ok(())
}

pub async fn leave_room(
    state: &AppState,
    app_id: Uuid,
    room_id: &str,
    session_id: Uuid,
) -> Result<()> {
    sqlx::query("DELETE FROM rust_presence WHERE app_id = $1 AND room_id = $2 AND session_id = $3")
        .bind(app_id)
        .bind(room_id)
        .bind(session_id)
        .execute(&state.pool)
        .await
        .map_err(InstantError::from)?;
    if let Some(mut set) = state.room_sessions.get_mut(&(app_id, room_id.to_string())) {
        set.remove(&session_id);
    }
    notify_room(state, app_id, room_id, delta_leave(session_id)).await;
    Ok(())
}

/// Remove a session from every room it joined (on disconnect).
pub async fn leave_all(state: &AppState, session_id: Uuid) {
    let rows =
        sqlx::query("DELETE FROM rust_presence WHERE session_id = $1 RETURNING app_id, room_id")
            .bind(session_id)
            .fetch_all(&state.pool)
            .await
            .unwrap_or_default();
    for row in rows {
        let app_id: Uuid = row.get("app_id");
        let room_id: String = row.get("room_id");
        notify_room(state, app_id, &room_id, delta_leave(session_id)).await;
    }
}

/// `delta` is `{"set": {...}}`, `{"leave": sid}` or null (full reload).
async fn notify_room(state: &AppState, app_id: Uuid, room_id: &str, delta: Value) {
    notify_json(
        state,
        "instant_room",
        json!({"app_id": app_id, "room_id": room_id, "delta": delta}),
    )
    .await;
}

/// Cached snapshots older than this are re-read in full, so a missed
/// notification (listener reconnect) heals within a minute.
const SNAPSHOT_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(60);

/// Apply a NOTIFY delta to a snapshot; false if the delta is not applicable.
fn apply_delta(snapshot: &mut Value, delta: &Value) -> bool {
    let Some(m) = snapshot.as_object_mut() else {
        return false;
    };
    if let Some(set) = delta.get("set") {
        let Some(sid) = set.get("session_id").and_then(|v| v.as_str()) else {
            return false;
        };
        m.insert(
            sid.to_string(),
            json!({"peer-id": sid, "user": set.get("user").cloned().unwrap_or(Value::Null),
                   "data": set.get("data").cloned().unwrap_or(json!({}))}),
        );
        return true;
    }
    if let Some(sid) = delta.get("leave").and_then(|v| v.as_str()) {
        m.remove(sid);
        return true;
    }
    false
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
///
/// With a delta from the NOTIFY payload and a fresh cached snapshot the new
/// snapshot is derived locally; otherwise (first event for the room on this
/// node, stale cache, unknown delta) the room is read from Postgres.
pub async fn broadcast_room_refresh(
    state: &AppState,
    app_id: Uuid,
    room_id: &str,
    delta: Option<&Value>,
) {
    let sessions = state.sessions_for_room(app_id, room_id);
    let key = (app_id, room_id.to_string());
    if sessions.is_empty() {
        state.room_snapshots.remove(&key);
        return;
    }
    let prev: Option<(Value, std::time::Instant)> =
        state.room_snapshots.get(&key).map(|e| e.value().clone());
    let derived = match (delta, &prev) {
        (Some(d), Some((snap, loaded_at))) if loaded_at.elapsed() < SNAPSHOT_MAX_AGE => {
            let mut next = snap.clone();
            apply_delta(&mut next, d).then_some((next, *loaded_at))
        }
        _ => None,
    };
    let (snapshot, loaded_at) = match derived {
        Some(x) => x,
        None => match room_snapshot(state, app_id, room_id).await {
            Ok(s) => (s, std::time::Instant::now()),
            Err(_) => return,
        },
    };
    state
        .room_snapshots
        .insert(key, (snapshot.clone(), loaded_at));
    let edits = prev
        .as_ref()
        .and_then(|(p, _)| diff_snapshots(p, &snapshot));
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
             RETURNING app_id, room_id, session_id",
        )
        .fetch_all(&state.pool)
        .await
        .unwrap_or_default();
        for row in stale {
            let app_id: Uuid = row.get("app_id");
            let room_id: String = row.get("room_id");
            let session_id: Uuid = row.get("session_id");
            notify_room(&state, app_id, &room_id, delta_leave(session_id)).await;
        }
        state
            .query_cache
            .retain(|_, e| e.created.elapsed() < crate::state::QUERY_CACHE_TTL);
        let _ = sqlx::query(
            "DELETE FROM rust_nodes WHERE heartbeat_at < now() - interval '60 seconds'",
        )
        .execute(&state.pool)
        .await;
        let _ =
            sqlx::query("DELETE FROM rust_spill WHERE created_at < now() - interval '5 minutes'")
                .execute(&state.pool)
                .await;
        let _ =
            sqlx::query("DELETE FROM rust_tx_changes WHERE logged_at < now() - interval '1 hour'")
                .execute(&state.pool)
                .await;
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
}
