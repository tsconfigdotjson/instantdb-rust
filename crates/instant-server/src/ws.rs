//! /runtime/session websocket handler — the client sync protocol
//! (client `Reactor.js`, LEGACY reactive/session.clj).

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
use crate::state::{
    value_hash, AppState, Outgoing, QueryCacheEntry, QueryCacheKey, QueryEntry, Session,
    SessionUser, QUERY_CACHE_TTL,
};
use serde::Serialize;
use serde_json::value::RawValue;

pub async fn handler(
    ws: WebSocketUpgrade,
    headers: axum::http::HeaderMap,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let request = request_ctx_from_headers(&headers);
    // tungstenite preallocates read_buffer_size (128 KiB by default) per
    // connection — ~1.3 GB for 10k idle sockets. Client frames are small
    // (the buffer still grows for a large transact), and every outgoing
    // frame is flushed as it is sent, so neither buffer needs to be large.
    ws.read_buffer_size(8 * 1024)
        .write_buffer_size(0)
        .on_upgrade(move |socket| session_loop(socket, state, request))
}

/// `request.ip` / `request.origin` for rules, from the Origin and
/// x-forwarded-for headers exactly like legacy (util/http.clj:84-118).
pub fn request_ctx_from_headers(
    headers: &axum::http::HeaderMap,
) -> instant_core::perms::RequestCtx {
    instant_core::perms::RequestCtx::from_headers(
        headers.get("origin").and_then(|v| v.to_str().ok()),
        headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()),
    )
}

async fn session_loop(
    socket: WebSocket,
    state: Arc<AppState>,
    request: instant_core::perms::RequestCtx,
) {
    let session_id = Uuid::new_v4();
    let (tx, mut rx) = mpsc::unbounded_channel::<Outgoing>();
    let session = state.new_session(session_id, tx);
    {
        let mut st = session.state.lock().await;
        st.ip = request.ip;
        st.origin = request.origin;
    }

    let (mut ws_tx, mut ws_rx) = socket.split();

    // writer task: outgoing queue -> socket, plus keepalive pings. For clients
    // that accept JSON-array frames (core > 0.22.75, Reactor.js:1798-1804),
    // queued-up messages are coalesced into one array frame like legacy does.
    let writer_session = session.clone();
    let writer = tokio::spawn(async move {
        // 15s keeps NAT/proxy idle timeouts (typically >= 60s) happy at a
        // third of the per-session wakeups of the previous 5s
        let mut ping = tokio::time::interval(std::time::Duration::from_secs(15));
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
                crate::scheduler::dispatch(&state, &session, parsed).await;
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    // cleanup, once the ops still running have finished
    session.scheduler.shutdown().await;
    state.drop_session(session_id);
    state.stream_subs.iter_mut().for_each(|mut e| {
        e.value_mut().retain(|(sid, _)| *sid != session_id);
    });
    state
        .stream_catchup
        .retain(|(sid, _), _| *sid != session_id);
    crate::presence::leave_all(&state, session_id).await;
    writer.abort();
}

/// `INSTANT_HANDLE_RECEIVE_TIMEOUT_MS`, default legacy's 5000.
fn handle_receive_timeout_ms() -> u64 {
    static MS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *MS.get_or_init(|| {
        std::env::var("INSTANT_HANDLE_RECEIVE_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|n| *n > 0)
            .unwrap_or(5000)
    })
}

/// Legacy `ex/get-param!` error shapes (util/exception.clj:410-428).
pub(crate) fn param_missing_at(path: &[&str]) -> InstantError {
    InstantError::new(
        "param-missing",
        400,
        format!("Missing parameter: {}", json!(path)),
        Some(json!({"in": path})),
    )
}
pub(crate) fn param_malformed_at(path: &[&str], input: &Value) -> InstantError {
    InstantError::new(
        "param-malformed",
        400,
        format!("Malformed parameter: {}", json!(path)),
        Some(json!({"in": path, "original-input": input})),
    )
}
/// `ex/get-param! event [key] coercer` for a non-blank string.
fn required_str<'a>(msg: &'a Value, key: &str) -> Result<&'a str, InstantError> {
    match msg.get(key) {
        None | Some(Value::Null) => Err(param_missing_at(&[key])),
        Some(v) => v
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| param_malformed_at(&[key], v)),
    }
}

/// Error frame matching legacy handle-error! key-for-key (session.clj:589-616):
/// status/client-event-id/original-event/type/message/hint are always present,
/// null when unknown.
pub(crate) fn err_msg(original: &Value, e: &InstantError) -> Value {
    // legacy session.clj:1040-1060 sends every request-scoped error type,
    // rate-limited and timeout included, with status 400 on the socket (429
    // is HTTP-only, util/http.clj:188-199)
    let status = if e.error_type == "rate-limited" || e.error_type == "timeout" {
        400
    } else {
        e.status
    };
    json!({
        "op": "error",
        "status": status,
        "type": e.error_type,
        "message": e.message,
        "hint": e.hint.clone().unwrap_or(Value::Null),
        "original-event": original,
        "client-event-id": original.get("client-event-id").cloned().unwrap_or(Value::Null),
    })
}

pub(crate) async fn handle_message(state: &Arc<AppState>, session: &Arc<Session>, mut msg: Value) {
    // events a combined transact took over (scheduler.rs): answered with
    // its result, each under its own client-event-id (session.clj:579-584,
    // handle-error! :596-605)
    let redundant = match msg
        .as_object_mut()
        .and_then(|m| m.remove(crate::scheduler::REDUNDANT))
    {
        Some(Value::Array(v)) => v,
        _ => vec![],
    };
    let send_err = |e: &InstantError| {
        for r in &redundant {
            session.send(err_msg(r, e));
        }
        session.send(err_msg(&msg, e));
    };
    let op = msg.get("op").and_then(|o| o.as_str()).unwrap_or("");
    // Per-app rate limit (issue #1). Like legacy handle-event
    // (reactive/session.clj:974-984) the bucket is keyed by the session's
    // established app and `init` is exempt: a frame's own `app-id` is
    // untrusted and pre-init ops have no app scope (they fail in their
    // handlers anyway).
    let limit_app_id = if op == "init" {
        None
    } else {
        session.state.lock().await.app_id
    };
    if let Some(app_id) = limit_app_id {
        if let Err(retry) = state
            .limiters
            .ws
            .check(app_id, crate::rate_limit::ws_op_cost(op))
        {
            send_err(&crate::rate_limit::rate_limited_err(retry));
            return;
        }
    }
    let handler = async {
        match op {
            "init" => handle_init(state, session, &msg).await,
            "add-query" => handle_add_query(state, session, &msg).await,
            "remove-query" => handle_remove_query(session, &msg).await,
            "transact" => handle_transact(state, session, &msg, &redundant).await,
            "join-room" => handle_join_room(state, session, &msg).await,
            "leave-room" => handle_leave_room(state, session, &msg).await,
            "set-presence" => handle_set_presence(state, session, &msg).await,
            "client-broadcast" => handle_client_broadcast(state, session, &msg).await,
            "start-sync" => crate::sync_table::handle_start_sync(state, session, &msg).await,
            "resync-table" => crate::sync_table::handle_resync_table(state, session, &msg).await,
            "remove-sync" => crate::sync_table::handle_remove_sync(state, session, &msg).await,
            "start-stream" => crate::streams::handle_start_stream(state, session, &msg).await,
            "append-stream" => crate::streams::handle_append_stream(state, session, &msg).await,
            "subscribe-stream" => {
                crate::streams::handle_subscribe_stream(state, session, &msg).await
            }
            "unsubscribe-stream" => {
                crate::streams::handle_unsubscribe_stream(state, session, &msg).await
            }
            // legacy: `{type: param-malformed, message: "Invalid op", hint: {op}}`
            _ => Err(InstantError::new(
                "param-malformed",
                400,
                "Invalid op",
                Some(json!({"op": op})),
            )),
        }
    };
    // legacy handle-receive-timeout-ms (session.clj:58): a handler that
    // overruns is cancelled and the client gets `operation-timed-out`
    // (util/exception.clj:462-465) instead of waiting forever
    let timeout_ms = handle_receive_timeout_ms();
    let result =
        match tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), handler).await {
            Ok(r) => r,
            Err(_) => Err(InstantError::new(
                "operation-timed-out",
                500,
                "Operation timed out: handle-receive",
                Some(json!({"timeout-ms": timeout_ms})),
            )),
        };
    if let Err(e) = result {
        send_err(&e);
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
            return Err(InstantError::validation_failed_input(
                "init",
                legacy_event_input(msg),
                json!([{"message": "`init` has already run for this session."}]),
            ));
        }
    }
    let app_id = match msg.get("app-id") {
        None | Some(Value::Null) => return Err(param_missing_at(&["app-id"])),
        Some(v) => v
            .as_str()
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(|| param_malformed_at(&["app-id"], v))?,
    };
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
                // legacy app-user-model/get-by-refresh-token! (exception.clj:130-135)
                return Err(InstantError::record_not_found_args(
                    "app-user",
                    json!({"app-id": app_id, "refresh-token": token}),
                ));
            }
        }
    }
    let mut admin = false;
    if let Some(token) = msg.get("__admin-token").and_then(|v| v.as_str()) {
        // legacy app-admin-token-model/fetch! asserts the record (session.clj:167-170)
        admin = crate::auth::check_admin_token(state, app_id, token).await?;
        if !admin {
            return Err(crate::routes::admin::admin_token_not_found(app_id, token));
        }
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

/// Legacy `get-auth!` (session.clj:198-202): the hint's input is the
/// session id.
pub(crate) fn not_initialized(session_id: Uuid) -> InstantError {
    InstantError::validation_failed_input(
        "init",
        json!({"sess-id": session_id}),
        json!([{"message": "`init` has not run for this session."}]),
    )
}

/// A client event as legacy echoes it in validation hints: the event map
/// plus the grouped-queue bookkeeping keys it carries by then
/// (session.clj:158-159 passes the whole event as the input).
pub(crate) fn legacy_event_input(msg: &Value) -> Value {
    let mut m = msg.as_object().cloned().unwrap_or_default();
    m.insert(
        "instant.grouped-queue/put-at".into(),
        json!(chrono::Utc::now().timestamp_millis()),
    );
    m.insert("total-delay-ms".into(), json!(0));
    m.insert("ws-ping-latency-ms".into(), json!(0));
    Value::Object(m)
}

async fn session_ctx(
    session: &Arc<Session>,
) -> std::result::Result<(Uuid, PermsCtx), InstantError> {
    let st = session.state.lock().await;
    let app_id = st.app_id.ok_or_else(|| not_initialized(session.id))?;
    Ok((
        app_id,
        PermsCtx {
            admin: st.admin,
            user_id: st.user.as_ref().map(|u| u.id),
            user_map: None,
            rule_params: None,
            ip: st.ip.clone(),
            origin: st.origin.clone(),
        },
    ))
}

/// Formats a query result for the wire. `tree`: legacy's `:tree`
/// return-type (admin SSE subscribeQuery) — the object tree plus
/// `result-meta`; otherwise the join-rows nodes with a null result-meta.
/// The hash covers the join-rows form in both cases, so result-changed
/// detection does not depend on the shape a session asked for.
pub(crate) fn format_query_result(
    result: &instant_core::instaql::QueryResult,
    attrs: &instant_core::attr::AttrMap,
    q: &Value,
    tree: bool,
    inference: bool,
) -> (Value, Value, u64) {
    let ws_result = result.to_ws_result();
    let hash = value_hash(&ws_result);
    if tree {
        let obj = crate::routes::admin::object_tree(result, attrs, q, inference);
        (obj, crate::routes::admin::object_meta(result), hash)
    } else {
        (ws_result, Value::Null, hash)
    }
}

async fn handle_add_query(
    state: &Arc<AppState>,
    session: &Arc<Session>,
    msg: &Value,
) -> HandlerResult {
    let (app_id, perms) = session_ctx(session).await?;
    let q = match msg.get("q") {
        None | Some(Value::Null) => {
            return Err(InstantError::validation_failed_input(
                "add-query",
                json!({"q": null}),
                json!([{"message": "Query can not be null."}]),
            ))
        }
        Some(q) => q.clone(),
    };
    // legacy `(keyword (or return-type "join-rows"))`: only `tree` is
    // special, anything else renders join-rows (session.clj:249, query.clj:139-144)
    let tree = msg.get("return-type").and_then(|v| v.as_str()) == Some("tree");
    service::assert_read_allowed(state, app_id).await?;
    let key = q.to_string();
    let inference = {
        let st = session.state.lock().await;
        if st.queries.contains_key(&key) {
            session.send(json!({
                "op": "add-query-exists",
                "q": q,
                "client-event-id": msg.get("client-event-id"),
            }));
            return Ok(());
        }
        st.inference
    };
    let started = std::time::Instant::now();
    let rules = if perms.admin {
        None
    } else {
        let mut conn = state.pool.acquire().await.map_err(InstantError::from)?;
        Some(instant_core::perms::Rules::load(&mut conn, app_id).await?)
    };
    // the shared cache holds join-rows frames; tree subscribers are few. A
    // rule that calls rateLimit charges its bucket on every evaluation
    // (legacy has no result cache), so those results are never shared.
    let cacheable = !tree
        && perms.user_map.is_none()
        && perms.rule_params.is_none()
        && q.get("$$ruleParams").is_none()
        && !rules.as_ref().is_some_and(|r| r.uses_rate_limits());
    let cache_key: QueryCacheKey = (
        app_id,
        key.clone(),
        perms.admin,
        perms.user_id,
        perms.ip.clone(),
        perms.origin.clone(),
        rules.as_ref().map(|r| value_hash(&r.code)).unwrap_or(0),
    );
    // Watermark first, so a result (cached or fresh) is never older than
    // the tx id the client is told it reflects.
    let processed_tx_id = service::max_tx_id(state, app_id).await?;
    let processed_isn = service::current_isn(state).await;
    let attr_gen = service::attr_generation(state, app_id);
    let cached = if cacheable {
        state.query_cache.get(&cache_key).and_then(|e| {
            (e.tx_id == processed_tx_id
                && e.attr_gen == attr_gen
                && e.created.elapsed() < QUERY_CACHE_TTL)
                .then(|| (e.ws_json.clone(), e.hash, e.topics.clone()))
        })
    } else {
        None
    };
    let mut result_meta = Value::Null;
    let (ws_json, hash, topics) = match cached {
        Some(hit) => {
            crate::metrics::METRICS.query_cache_hits_total.inc();
            hit
        }
        None => {
            crate::metrics::METRICS.query_cache_misses_total.inc();
            let attrs = service::load_attrs(state, app_id).await?;
            let outcome =
                service::run_query_full(state, app_id, &attrs, &perms, &q, rules.as_ref()).await?;
            let (wire, meta, hash) =
                format_query_result(&outcome.result, &attrs, &q, tree, inference);
            result_meta = meta;
            let ws_json = Arc::new(
                RawValue::from_string(wire.to_string()).expect("serde_json output is valid JSON"),
            );
            let topics = Arc::new(outcome.topics);
            if cacheable {
                state.query_cache.insert(
                    cache_key.clone(),
                    QueryCacheEntry {
                        ws_json: ws_json.clone(),
                        hash,
                        topics: topics.clone(),
                        tx_id: processed_tx_id,
                        attr_gen,
                        created: std::time::Instant::now(),
                    },
                );
            }
            (ws_json, hash, topics)
        }
    };
    crate::metrics::METRICS
        .add_query_seconds
        .observe_since(started);
    // legacy add-query-ok superset (session.clj:264-270): result-meta is
    // only populated for the tree return-type (admin SSE), null on the
    // ws join-rows path; processed-isn is the WAL watermark.
    let reply = AddQueryOkWire {
        op: "add-query-ok",
        q: &q,
        result: &ws_json,
        result_meta: &result_meta,
        processed_tx_id,
        processed_isn,
        client_event_id: msg.get("client-event-id"),
        trace_id: crate::state::new_trace_id(),
    };
    let frame = match serde_json::to_string(&reply) {
        Ok(frame) => frame,
        Err(e) => {
            tracing::error!("add-query-ok serialization failed: {e}");
            return Ok(());
        }
    };
    // Register and reply in one critical section. The invalidator records
    // and sends refresh-ok frames under this same lock, so the entry's
    // hash always matches the last result the client received: a refresh
    // can't slip out ahead of this reply and then be overwritten on the
    // client by the older add-query-ok (issue #51).
    {
        let mut st = session.state.lock().await;
        st.queries.insert(
            key.clone(),
            QueryEntry {
                q: q.clone(),
                result_hash: hash,
                topics: Some(topics),
                tree,
            },
        );
        session.send_raw(frame);
    }
    // A tx that committed after the watermark read may have been matched
    // against this session's queries before this one was registered (ops
    // of one session run concurrently, so a transact next to this
    // add-query is the common case). Recheck the query through the app's
    // refresh worker, which sends refreshes in order.
    let latest = service::max_tx_id(state, app_id).await?;
    if latest > processed_tx_id {
        crate::invalidator::enqueue_recheck(state, app_id, session.id, key, latest);
    }
    Ok(())
}

/// add-query-ok on the wire; the (possibly shared) result is spliced in as
/// raw bytes.
#[derive(Serialize)]
struct AddQueryOkWire<'a> {
    op: &'static str,
    q: &'a Value,
    result: &'a RawValue,
    #[serde(rename = "result-meta")]
    result_meta: &'a Value,
    #[serde(rename = "processed-tx-id")]
    processed_tx_id: i64,
    #[serde(rename = "processed-isn")]
    processed_isn: Value,
    #[serde(rename = "client-event-id")]
    client_event_id: Option<&'a Value>,
    #[serde(rename = "trace-id")]
    trace_id: String,
}

async fn handle_remove_query(session: &Arc<Session>, msg: &Value) -> HandlerResult {
    session_ctx(session).await?;
    let q = msg.get("q").cloned().unwrap_or(Value::Null);
    if !q.is_null() {
        let key = q.to_string();
        let mut st = session.state.lock().await;
        st.queries.remove(&key);
    }
    session.send(json!({
        "op": "remove-query-ok",
        "q": q,
        "client-event-id": msg.get("client-event-id"),
    }));
    Ok(())
}

async fn handle_transact(
    state: &Arc<AppState>,
    session: &Arc<Session>,
    msg: &Value,
    redundant: &[Value],
) -> HandlerResult {
    let (app_id, perms) = session_ctx(session).await?;
    let steps = msg.get("tx-steps").ok_or_else(|| {
        InstantError::validation_failed_input(
            "tx-steps",
            Value::Null,
            json!([{"expected": "coll?", "in": []}]),
        )
    })?;
    let report = service::run_transact(state, app_id, &perms, steps).await?;
    // legacy attaches the tx's ISN (session.clj:580-584); reading the WAL
    // position after commit keeps refresh isn >= transact isn.
    let isn = service::current_isn(state).await;
    for event in redundant.iter().chain(std::iter::once(msg)) {
        session.send(json!({
            "op": "transact-ok",
            "client-event-id": event.get("client-event-id"),
            "tx-id": report.tx_id,
            "isn": isn,
        }));
    }
    Ok(())
}

async fn handle_join_room(
    state: &Arc<AppState>,
    session: &Arc<Session>,
    msg: &Value,
) -> HandlerResult {
    let (app_id, _) = session_ctx(session).await?;
    let room_id = required_str(msg, "room-id")?;
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
    let room_id = required_str(msg, "room-id")?;
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
    let room_id = required_str(msg, "room-id")?;
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
        return Err(InstantError::validation_failed_input(
            "room",
            json!({"app-id": st.app_id, "room-id": room_id}),
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
    let room_id = required_str(msg, "room-id")?;
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
