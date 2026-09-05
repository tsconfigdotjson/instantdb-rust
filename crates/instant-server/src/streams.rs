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

/// Legacy `ex/get-param!` error shapes (util/exception.clj:410-428).
fn param_missing_at(path: &[&str]) -> InstantError {
    InstantError::new(
        "param-missing",
        400,
        format!("Missing parameter: {}", json!(path)),
        Some(json!({"in": path})),
    )
}
fn param_malformed_at(path: &[&str], input: &Value) -> InstantError {
    InstantError::new(
        "param-malformed",
        400,
        format!("Malformed parameter: {}", json!(path)),
        Some(json!({"in": path, "original-input": input})),
    )
}

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
    stream: Option<Uuid>,
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
    let request = instant_core::perms::RequestCtx::default().with_pool(state.pool.clone());
    let env = instant_core::perms::EvalEnv::new(app_id, &rules, &request);
    let attrs = service::load_attrs(state, app_id).await?;
    // legacy binds `auth` as an AuthCelMap (app_stream.clj:38-90), so
    // `auth.ref('$user...')` resolves here too
    let auth_ctx = instant_core::perms::AuthCtx {
        user_id,
        user_map: None,
        request: request.clone(),
    };
    let auth_val =
        instant_core::perms::build_auth_value(&mut conn, app_id, &attrs, &auth_ctx, &[&program])
            .await?;
    // the rule's `data` is the $streams row when there is one (legacy binds
    // the fetched stream for view checks, session.clj:897-908)
    let data = match stream {
        Some(sid) => {
            instant_core::perms::fetch_entity_map(&mut conn, app_id, &attrs, "$streams", sid)
                .await?
                .map(Value::Object)
                .unwrap_or_else(|| json!({}))
        }
        None => json!({}),
    };
    let ok = instant_core::perms::eval_program(
        &program,
        &data,
        None,
        &auth_val,
        rule_params.unwrap_or(&json!({})),
        &env,
    )
    .await?;
    if !ok {
        // legacy assert-permitted! :has-streams-permission? (app_stream.clj:44-47)
        return Err(InstantError::new(
            "permission-denied",
            400,
            "Permission denied: not has-streams-permission?",
            Some(json!({"input": ["$streams", action], "expected": "has-streams-permission?"})),
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
        st.app_id.ok_or_else(|| crate::ws::not_initialized(session.id))?
    };
    let client_id = msg
        .get("client-id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| InstantError::param_missing("missing client-id"))?;
    // legacy `ex/get-param! event [:reconnect-token] uuid-util/coerce`
    // (session.clj:767-769): the token is required, so every stream has a
    // hashed token and resuming under an existing client-id always proves
    // knowledge of it. Optional here would let a second session take over
    // a token-less stream by replaying its client-id.
    let reconnect_token = match msg.get("reconnect-token") {
        None | Some(Value::Null) => {
            return Err(InstantError::new(
                "param-missing",
                400,
                "Missing parameter: [\"reconnect-token\"]",
                Some(json!({"in": ["reconnect-token"]})),
            ))
        }
        Some(v) => match v.as_str().and_then(|s| Uuid::parse_str(s).ok()) {
            Some(u) => u.to_string(),
            None => {
                return Err(InstantError::new(
                    "param-malformed",
                    400,
                    "Malformed parameter: [\"reconnect-token\"]",
                    Some(json!({"in": ["reconnect-token"], "original-input": v})),
                ))
            }
        },
    };
    check_stream_perm(
        state,
        app_id,
        session,
        "create",
        msg.get("rule-params"),
        None,
    )
    .await?;

    let existing = stream_by_client_id(state, app_id, client_id).await?;
    let (stream_id, offset) = match existing {
        Some(sid) => {
            // resume: reconnect token must match (legacy session.clj:776-786)
            let stored = stream_field(state, app_id, sid, "hashedReconnectToken").await;
            let supplied_hash = Some(crate::auth::hash_string(&reconnect_token));
            if stored.as_ref().and_then(|v| v.as_str()) != supplied_hash.as_deref() {
                let m = "A stream with that clientId already exists. Reconnect token is invalid.";
                return Err(InstantError::validation_failed_input(
                    "start-stream",
                    json!({"sess-id": session.id, "client-id": client_id}),
                    json!([{"message": m}]),
                ));
            }
            if stream_field(state, app_id, sid, "done")
                .await
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                return Err(InstantError::validation_failed_input(
                    "start-stream",
                    json!({"sess-id": session.id, "client-id": client_id}),
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
            steps.push(json!([
                "add-triple",
                sid,
                sc::attr_id("$streams", "hashedReconnectToken"),
                crate::auth::hash_string(&reconnect_token)
            ]));
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
        st.app_id.ok_or_else(|| crate::ws::not_initialized(session.id))?
    };
    let stream_id = msg
        .get("stream-id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| InstantError::param_missing("missing stream-id"))?;
    // legacy `ex/get-param!` on chunks (a vector of strings) and offset
    let chunks: Vec<String> = match msg.get("chunks") {
        None | Some(Value::Null) => return Err(param_missing_at(&["chunks"])),
        Some(v) => match v.as_array() {
            Some(a) if a.iter().all(|c| c.is_string()) => a
                .iter()
                .filter_map(|c| c.as_str().map(|s| s.to_string()))
                .collect(),
            _ => return Err(param_malformed_at(&["chunks"], v)),
        },
    };
    let offset = match msg.get("offset") {
        None | Some(Value::Null) => return Err(param_missing_at(&["offset"])),
        Some(v) => v
            .as_i64()
            .filter(|n| *n >= 0)
            .ok_or_else(|| param_malformed_at(&["offset"], v))?,
    };
    let done = msg.get("done").and_then(|v| v.as_bool()).unwrap_or(false);
    let abort_reason = msg.get("abort-reason").and_then(|v| v.as_str());
    // legacy rs/get-stream-object-for-append (session.clj:828-838): only the
    // session that started the stream holds its append object; anyone else
    // (and an unknown stream id) is "Stream is missing."
    {
        let st = session.state.lock().await;
        if !st.writing_streams.contains(&stream_id) {
            return Err(InstantError::validation_failed_input(
                "append-stream",
                json!({"sess-id": session.id, "stream-id": stream_id}),
                json!([{"message": "Stream is missing."}]),
            ));
        }
    }

    // legacy app-stream-model/append (app_stream.clj:444-457)
    if stream_field(state, app_id, stream_id, "done")
        .await
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        return Err(InstantError::validation_failed_input(
            "append-stream",
            json!({"stream-id": stream_id}),
            json!([{"message": "Stream is completed."}]),
        ));
    }
    let content = chunks.concat();
    let bytes = content.as_bytes();
    let prev_size = crate::storage::blob_size(state, app_id, &stream_key(stream_id)).await;
    if prev_size != offset {
        return Err(InstantError::validation_failed_input(
            "append-stream",
            json!({"stream-id": stream_id, "expected-offset": offset, "offset": prev_size}),
            json!([{"message": "Invalid offset for stream."}]),
        ));
    }
    let new_size =
        crate::storage::append_blob(state, app_id, &stream_key(stream_id), offset, bytes).await?;
    // two writers racing on one stream can make prev_size stale; never
    // slice past what this append actually carried
    let new_bytes_len = ((new_size - prev_size).max(0) as usize).min(bytes.len());
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
    if done {
        // S3 backend: move the finished stream from the postgres spool to
        // the bucket (no-op elsewhere). Best effort — the spool stays
        // readable if it fails.
        if let Err(e) = crate::storage::finalize_blob(state, app_id, &stream_key(stream_id)).await {
            tracing::warn!("stream {stream_id} finalize: {e}");
        }
    }
    Ok(())
}

pub async fn handle_subscribe_stream(
    state: &Arc<AppState>,
    session: &Arc<Session>,
    msg: &Value,
) -> std::result::Result<(), InstantError> {
    let app_id = {
        let st = session.state.lock().await;
        st.app_id.ok_or_else(|| crate::ws::not_initialized(session.id))?
    };
    // legacy validates params before perms (session.clj:889-896 missing ids,
    // :928-931 missing stream)
    let missing_stream = || {
        InstantError::validation_failed_input(
            "subscribe-stream",
            json!({
                "sess-id": session.id,
                "stream-id": msg.get("stream-id").and_then(|v| v.as_str()),
                "client-id": msg.get("client-id").and_then(|v| v.as_str()),
            }),
            json!([{"message": "Stream is missing."}]),
        )
    };
    if msg.get("stream-id").and_then(|v| v.as_str()).is_none()
        && msg.get("client-id").and_then(|v| v.as_str()).is_none()
    {
        let m = "Must provide either a stream-id or a client-id";
        return Err(InstantError::validation_failed_input(
            "subscribe-stream",
            json!({"sess-id": session.id}),
            json!([{"message": m}]),
        ));
    }
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
    check_stream_perm(
        state,
        app_id,
        session,
        "view",
        msg.get("rule-params"),
        Some(stream_id),
    )
    .await?;
    let subscribe_event_id = msg
        .get("client-event-id")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let offset = msg.get("offset").and_then(|v| v.as_i64()).unwrap_or(0);

    // Register before reading the catch-up snapshot so an append landing
    // between the two is delivered instead of lost. Live frames that arrive
    // meanwhile (including a NOTIFY still in flight from an append that
    // committed before the subscription) are parked and replayed after the
    // snapshot: the client rejects a frame ahead of what it has seen
    // (Stream.ts:565-570), while overlap is idempotent (:577).
    let sub_key = (session.id, subscribe_event_id.clone());
    state.stream_catchup.insert(sub_key.clone(), vec![]);
    state
        .stream_subs
        .entry((app_id, stream_id))
        .or_default()
        .insert(sub_key.clone());

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
    // replay the frames parked during the snapshot: anything the snapshot
    // already covers is dropped, a frame adding bytes beyond it (its offset
    // is at most the snapshot end, appends are contiguous) or carrying
    // `done` / an abort is delivered
    let snapshot_end = stored.len() as i64;
    let parked = state
        .stream_catchup
        .remove(&sub_key)
        .map(|(_, v)| v)
        .unwrap_or_default();
    for frame in parked {
        let offset = frame.get("offset").and_then(|v| v.as_i64()).unwrap_or(0);
        let len = frame
            .get("content")
            .and_then(|v| v.as_str())
            .map(|c| c.len() as i64)
            .unwrap_or(0);
        let frame_done = frame.get("done").and_then(|v| v.as_bool()).unwrap_or(false);
        if offset + len > snapshot_end
            || (frame_done && !done)
            || frame.get("abort-reason").is_some()
        {
            session.send(frame);
        }
    }

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
    let mut removed = false;
    state.stream_subs.iter_mut().for_each(|mut e| {
        let before = e.value().len();
        e.value_mut()
            .retain(|(sid, ev)| !(*sid == session.id && *ev == target));
        removed |= e.value().len() != before;
    });
    state.stream_catchup.remove(&(session.id, target.clone()));
    if !removed {
        return Err(InstantError::validation_failed_input(
            "unsubscribe-stream",
            json!({"sess-id": session.id, "subscribe-event-id": target}),
            json!([{"message": "Stream subscription is missing."}]),
        ));
    }
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
            // a subscriber still receiving its snapshot gets the frame after it
            if let Some(mut parked) = state.stream_catchup.get_mut(&(session_id, event_id)) {
                parked.push(msg);
                continue;
            }
            session.send(msg);
        }
    }
    if done {
        state.stream_subs.remove(&(app_id, stream_id));
    }
}
