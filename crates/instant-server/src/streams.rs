//! Streams: append-only text streams with live tailing (PROTOCOL.md §2.10 /
//! §3.14). Stream metadata lives in the $streams system namespace; bytes live
//! in the storage dir; live fan-out crosses nodes via pg NOTIFY.

use std::sync::Arc;

use instant_core::error::{InstantError, Result};
use instant_core::system_catalog as sc;
use serde_json::{json, Value};
use sqlx::Row;
use uuid::Uuid;

use crate::service;
use crate::state::{AppState, Session};

/// blob key for a stream's bytes (prefix keeps it apart from $files blobs)
fn stream_key(stream_id: Uuid) -> String {
    format!("stream-{stream_id}")
}

async fn stream_by_client_id(
    state: &AppState,
    app_id: Uuid,
    client_id: &str,
) -> Result<Option<Uuid>> {
    let attr = sc::attr_id("$streams", "clientId");
    let row = sqlx::query(
        "SELECT entity_id FROM triples
         WHERE app_id = $1 AND attr_id = $2 AND av AND value = to_jsonb($3::text) LIMIT 1",
    )
    .bind(app_id)
    .bind(attr)
    .bind(client_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    Ok(row.map(|r| r.get("entity_id")))
}

async fn stream_field(
    state: &AppState,
    app_id: Uuid,
    stream_id: Uuid,
    label: &str,
) -> Option<Value> {
    let attr = sc::attr_id("$streams", label);
    let row = sqlx::query(
        "SELECT value FROM triples
         WHERE app_id = $1 AND entity_id = $2 AND attr_id = $3 LIMIT 1",
    )
    .bind(app_id)
    .bind(stream_id)
    .bind(attr)
    .fetch_optional(&state.pool)
    .await
    .ok()??;
    Some(row.get("value"))
}

async fn check_stream_perm(
    state: &AppState,
    app_id: Uuid,
    session: &Arc<Session>,
    action: &str,
    rule_params: Option<&Value>,
) -> Result<()> {
    let (admin, user_id) = {
        let st = session.state.lock().await;
        (st.admin, st.user.as_ref().map(|u| u.id))
    };
    if admin {
        return Ok(());
    }
    let mut conn = state.pool.acquire().await.map_err(InstantError::from)?;
    let rules = instant_core::perms::Rules::load(&mut conn, app_id).await?;
    let program = rules.program("$streams", action);
    let auth_ctx = instant_core::perms::AuthCtx {
        user_id,
        user_map: None,
    };
    let attrs = service::load_attrs(state, app_id).await?;
    let auth_val = if let Some(uid) = user_id {
        instant_core::perms::fetch_entity_map(&mut conn, app_id, &attrs, "$users", uid)
            .await?
            .map(Value::Object)
            .unwrap_or(Value::Null)
    } else {
        Value::Null
    };
    let _ = auth_ctx;
    let ok = instant_core::perms::eval_program(
        &program,
        &json!({}),
        None,
        &auth_val,
        rule_params.unwrap_or(&json!({})),
    )?;
    if !ok {
        return Err(InstantError::permission_denied(
            json!(["$streams", action]),
            "Permission denied: not perms-pass?",
        ));
    }
    Ok(())
}

pub async fn handle_start_stream(
    state: &Arc<AppState>,
    session: &Arc<Session>,
    msg: &Value,
) -> std::result::Result<(), InstantError> {
    let app_id = {
        let st = session.state.lock().await;
        st.app_id
            .ok_or_else(|| InstantError::param_malformed("session not initialized"))?
    };
    let client_id = msg
        .get("client-id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| InstantError::param_missing("missing client-id"))?;
    let reconnect_token = msg.get("reconnect-token").and_then(|v| v.as_str());
    check_stream_perm(state, app_id, session, "create", msg.get("rule-params")).await?;

    let existing = stream_by_client_id(state, app_id, client_id).await?;
    let (stream_id, offset) = match existing {
        Some(sid) => {
            // resume: reconnect token must match (legacy session.clj:776-786)
            let stored = stream_field(state, app_id, sid, "hashedReconnectToken").await;
            let supplied_hash = reconnect_token.map(crate::auth::hash_string);
            if stored.as_ref().and_then(|v| v.as_str()) != supplied_hash.as_deref() {
                let m = "A stream with that clientId already exists. Reconnect token is invalid.";
                return Err(InstantError::validation_failed(
                    "start-stream",
                    m,
                    json!([{"message": m}]),
                ));
            }
            if stream_field(state, app_id, sid, "done")
                .await
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                return Err(InstantError::validation_failed(
                    "start-stream",
                    "Stream is closed.",
                    json!([{"message": "Stream is closed."}]),
                ));
            }
            let size = stream_field(state, app_id, sid, "size")
                .await
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            (sid, size)
        }
        None => {
            let sid = Uuid::new_v4();
            let mut steps = vec![
                json!(["add-triple", sid, sc::attr_id("$streams", "id"), sid]),
                json!([
                    "add-triple",
                    sid,
                    sc::attr_id("$streams", "clientId"),
                    client_id
                ]),
                json!([
                    "add-triple",
                    sid,
                    sc::attr_id("$streams", "machineId"),
                    state.node_id.to_string()
                ]),
                json!(["add-triple", sid, sc::attr_id("$streams", "size"), 0]),
                json!(["add-triple", sid, sc::attr_id("$streams", "done"), false]),
            ];
            if let Some(t) = reconnect_token {
                steps.push(json!([
                    "add-triple",
                    sid,
                    sc::attr_id("$streams", "hashedReconnectToken"),
                    crate::auth::hash_string(t)
                ]));
            }
            service::run_system_transact(state, app_id, &Value::Array(steps)).await?;
            (sid, 0)
        }
    };

    {
        let mut st = session.state.lock().await;
        st.writing_streams.insert(stream_id);
    }
    session.send(json!({
        "op": "start-stream-ok",
        "client-event-id": msg.get("client-event-id"),
        "client-id": client_id,
        "stream-id": stream_id,
        "offset": offset,
    }));
    Ok(())
}

pub async fn handle_append_stream(
    state: &Arc<AppState>,
    session: &Arc<Session>,
    msg: &Value,
) -> std::result::Result<(), InstantError> {
    let app_id = {
        let st = session.state.lock().await;
        st.app_id
            .ok_or_else(|| InstantError::param_malformed("session not initialized"))?
    };
    let stream_id = msg
        .get("stream-id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| InstantError::param_missing("missing stream-id"))?;
    {
        let st = session.state.lock().await;
        if !st.writing_streams.contains(&stream_id) {
            return Err(InstantError::validation_failed(
                "stream",
                "This session is not the stream's writer.",
                json!([]),
            ));
        }
    }
    let chunks: Vec<String> = msg
        .get("chunks")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|c| c.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let offset = msg.get("offset").and_then(|v| v.as_i64()).unwrap_or(0);
    let done = msg.get("done").and_then(|v| v.as_bool()).unwrap_or(false);
    let abort_reason = msg.get("abort-reason").and_then(|v| v.as_str());

    let content = chunks.concat();
    let bytes = content.as_bytes();
    let prev_size = crate::storage::blob_size(state, app_id, &stream_key(stream_id)).await;
    let new_size =
        crate::storage::append_blob(state, app_id, &stream_key(stream_id), offset, bytes).await?;
    let new_bytes_len = (new_size - prev_size).max(0) as usize;
    let new_bytes = if new_bytes_len == 0 {
        &[] as &[u8]
    } else {
        &bytes[bytes.len() - new_bytes_len..]
    };

    // update metadata
    let mut steps = vec![
        json!([
            "add-triple",
            stream_id,
            sc::attr_id("$streams", "size"),
            new_size
        ]),
        json!([
            "add-triple",
            stream_id,
            sc::attr_id("$streams", "done"),
            done
        ]),
    ];
    if let Some(reason) = abort_reason {
        steps.push(json!([
            "add-triple",
            stream_id,
            sc::attr_id("$streams", "abortReason"),
            reason
        ]));
    }
    service::run_system_transact(state, app_id, &Value::Array(steps)).await?;

    // legacy stream-flushed carries no client-event-id (session.clj:855-858)
    session.send(json!({
        "op": "stream-flushed",
        "stream-id": stream_id,
        "offset": new_size,
        "done": done,
    }));

    // fan out to subscribers (all nodes)
    let client_id = stream_field(state, app_id, stream_id, "clientId").await;
    service::notify_json(
        state,
        "instant_stream",
        json!({
            "app_id": app_id,
            "stream_id": stream_id,
            "client_id": client_id,
            "offset": new_size - new_bytes.len() as i64,
            "content": String::from_utf8_lossy(new_bytes),
            "done": done,
            "abort_reason": abort_reason,
        }),
    )
    .await;
    Ok(())
}

pub async fn handle_subscribe_stream(
    state: &Arc<AppState>,
    session: &Arc<Session>,
    msg: &Value,
) -> std::result::Result<(), InstantError> {
    let app_id = {
        let st = session.state.lock().await;
        st.app_id
            .ok_or_else(|| InstantError::param_malformed("session not initialized"))?
    };
    // legacy validates params before perms (session.clj:889-896 missing ids,
    // :928-931 missing stream)
    let missing_stream = || {
        InstantError::validation_failed(
            "subscribe-stream",
            "Stream is missing.",
            json!([{"message": "Stream is missing."}]),
        )
    };
    if msg.get("stream-id").and_then(|v| v.as_str()).is_none()
        && msg.get("client-id").and_then(|v| v.as_str()).is_none()
    {
        let m = "Must provide either a stream-id or a client-id";
        return Err(InstantError::validation_failed(
            "subscribe-stream",
            m,
            json!([{"message": m}]),
        ));
    }
    check_stream_perm(state, app_id, session, "view", msg.get("rule-params")).await?;
    let stream_id = match msg.get("stream-id").and_then(|v| v.as_str()) {
        Some(s) => {
            Uuid::parse_str(s).map_err(|_| InstantError::param_malformed("malformed stream-id"))?
        }
        None => {
            let client_id = msg.get("client-id").and_then(|v| v.as_str()).unwrap();
            stream_by_client_id(state, app_id, client_id)
                .await?
                .ok_or_else(missing_stream)?
        }
    };
    // the id form must also resolve to a real stream (legacy get-stream check)
    if stream_field(state, app_id, stream_id, "id").await.is_none() {
        return Err(missing_stream());
    }
    let subscribe_event_id = msg
        .get("client-event-id")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let offset = msg.get("offset").and_then(|v| v.as_i64()).unwrap_or(0);

    // Register before reading the catch-up snapshot so an append landing
    // between the two is delivered live instead of lost. The subscriber may
    // then see a range twice (once live, once in the catch-up), but frames
    // apply content at their absolute offset (Stream.ts:1077-1090), so
    // overlap is idempotent; and a live frame can only precede the catch-up
    // frame if its write committed first, in which case the catch-up read
    // includes it and cannot truncate it away.
    state
        .stream_subs
        .entry((app_id, stream_id))
        .or_default()
        .insert((session.id, subscribe_event_id.clone()));

    // catch-up: send stored content from offset
    let stored = crate::storage::read_blob(state, app_id, &stream_key(stream_id))
        .await
        .unwrap_or_default();
    let done = stream_field(state, app_id, stream_id, "done")
        .await
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let abort_reason = stream_field(state, app_id, stream_id, "abortReason")
        .await
        .filter(|v| v.is_string());
    let client_id = stream_field(state, app_id, stream_id, "clientId").await;
    let catchup = if (offset as usize) < stored.len() {
        String::from_utf8_lossy(&stored[offset as usize..]).to_string()
    } else {
        String::new()
    };
    // legacy catch-up shape (session.clj:918-927): aborts surface as
    // done + abort-reason, not as error/retry (the client only reads
    // error/retry for transport failures, Stream.ts:1064-1076). The
    // abort-reason key is present only when the stream was aborted.
    let mut reply = json!({
        "op": "stream-append",
        "client-event-id": subscribe_event_id.clone(),
        "stream-id": stream_id,
        "client-id": client_id,
        "offset": offset.min(stored.len() as i64),
        "content": catchup,
        "done": done,
    });
    if let Some(reason) = &abort_reason {
        reply["abort-reason"] = reason.clone();
    }
    session.send(reply);

    if done {
        // already-done streams get no live appends; drop the registration
        // (mirrors deliver_append's cleanup on done)
        if let Some(mut subs) = state.stream_subs.get_mut(&(app_id, stream_id)) {
            subs.remove(&(session.id, subscribe_event_id));
        }
    }
    Ok(())
}

pub async fn handle_unsubscribe_stream(
    state: &Arc<AppState>,
    session: &Arc<Session>,
    msg: &Value,
) -> std::result::Result<(), InstantError> {
    let target = msg
        .get("subscribe-event-id")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    state.stream_subs.iter_mut().for_each(|mut e| {
        e.value_mut()
            .retain(|(sid, ev)| !(*sid == session.id && *ev == target));
    });
    Ok(())
}

/// Deliver a live append to this node's subscribers (from the NOTIFY listener).
pub async fn deliver_append(state: &Arc<AppState>, payload: &Value) {
    let (Some(app_id), Some(stream_id)) = (
        payload
            .get("app_id")
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok()),
        payload
            .get("stream_id")
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok()),
    ) else {
        return;
    };
    let Some(subs) = state
        .stream_subs
        .get(&(app_id, stream_id))
        .map(|s| s.clone())
    else {
        return;
    };
    let done = payload
        .get("done")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    for (session_id, event_id) in subs {
        if let Some(session) = state.sessions.get(&session_id).map(|s| s.clone()) {
            let mut msg = json!({
                "op": "stream-append",
                "client-event-id": event_id,
                "stream-id": stream_id,
                "client-id": payload.get("client_id").cloned().unwrap_or(Value::Null),
                "offset": payload.get("offset").cloned().unwrap_or(json!(0)),
                "content": payload.get("content").cloned().unwrap_or(json!("")),
                "done": done,
            });
            if let Some(reason) = payload.get("abort_reason").filter(|r| r.is_string()) {
                msg["abort-reason"] = reason.clone();
            }
            session.send(msg);
        }
    }
    if done {
        state.stream_subs.remove(&(app_id, stream_id));
    }
}
