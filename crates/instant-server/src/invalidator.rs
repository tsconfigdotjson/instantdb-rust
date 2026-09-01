//! LISTEN/NOTIFY driven invalidation + room/broadcast fan-out. Any node can
//! serve any session; coordination happens entirely through Postgres.
//!
//! Refresh pipeline (issue #11 / #4): tx notifications are queued per app and
//! drained by one worker per app, so a burst of transactions becomes a single
//! refresh batch (`processed-tx-id` = the newest tx in the batch). A batch
//! loads the tx's triple changes once, matches them against every registered
//! query's topics, recomputes only the matching queries — once per distinct
//! (query, auth) across all sessions — and pushes refresh-ok to each session
//! whose result hash changed.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use futures::StreamExt;
use instant_core::perms::Rules;
use instant_core::topics::{QueryTopics, TxChange, TxTopics};
use serde_json::{json, Value};
use sqlx::postgres::PgListener;
use sqlx::Row;
use uuid::Uuid;

use crate::metrics::METRICS;
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
        .listen_all([
            "instant_tx",
            "instant_room",
            "instant_broadcast",
            "instant_stream",
        ])
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
                    payload
                        .get("app_id")
                        .and_then(|v| v.as_str())
                        .and_then(|s| Uuid::parse_str(s).ok()),
                    payload.get("tx_id").and_then(|v| v.as_i64()),
                ) else {
                    continue;
                };
                let attrs_changed = payload
                    .get("attrs_changed")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if let Some(ts) = payload.get("ts").and_then(|v| v.as_i64()) {
                    let lag_ms = chrono::Utc::now().timestamp_millis() - ts;
                    METRICS
                        .notify_lag_seconds
                        .observe(lag_ms.max(0) as f64 / 1000.0);
                }
                if attrs_changed {
                    service::invalidate_attrs(state, app_id);
                }
                enqueue_tx(state, app_id, tx_id, attrs_changed);
            }
            "instant_room" => {
                let (Some(app_id), Some(room_id)) = (
                    payload
                        .get("app_id")
                        .and_then(|v| v.as_str())
                        .and_then(|s| Uuid::parse_str(s).ok()),
                    payload
                        .get("room_id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
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
                    payload
                        .get("app_id")
                        .and_then(|v| v.as_str())
                        .and_then(|s| Uuid::parse_str(s).ok()),
                    payload
                        .get("room_id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
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
            "instant_stream" => {
                crate::streams::deliver_append(state, &payload).await;
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Per-app refresh queue

/// Transactions waiting for the next refresh batch of one app.
#[derive(Default)]
pub struct RefreshQueue {
    pending: Mutex<Pending>,
}

#[derive(Default)]
struct Pending {
    tx_ids: BTreeSet<i64>,
    attrs_changed: bool,
    /// when the oldest pending tx was enqueued (for batch latency)
    since: Option<Instant>,
    /// a worker task is draining this queue
    running: bool,
}

impl RefreshQueue {
    pub fn pending_len(&self) -> usize {
        self.pending.lock().map(|p| p.tx_ids.len()).unwrap_or(0)
    }
}

/// Queue a tx for refresh; starts the app's worker if none is running.
pub fn enqueue_tx(state: &Arc<AppState>, app_id: Uuid, tx_id: i64, attrs_changed: bool) {
    let queue = state.refresh_queues.entry(app_id).or_default().clone();
    let start_worker = {
        let mut p = queue.pending.lock().unwrap_or_else(|e| e.into_inner());
        p.tx_ids.insert(tx_id);
        p.attrs_changed |= attrs_changed;
        p.since.get_or_insert_with(Instant::now);
        if p.running {
            false
        } else {
            p.running = true;
            true
        }
    };
    if start_worker {
        let state = state.clone();
        tokio::spawn(async move {
            refresh_worker(state, app_id, queue).await;
        });
    }
}

async fn refresh_worker(state: Arc<AppState>, app_id: Uuid, queue: Arc<RefreshQueue>) {
    loop {
        let batch = {
            let mut p = queue.pending.lock().unwrap_or_else(|e| e.into_inner());
            if p.tx_ids.is_empty() {
                p.running = false;
                break;
            }
            let tx_ids: Vec<i64> = std::mem::take(&mut p.tx_ids).into_iter().collect();
            let attrs_changed = std::mem::take(&mut p.attrs_changed);
            let since = p.since.take().unwrap_or_else(Instant::now);
            (tx_ids, attrs_changed, since)
        };
        let (tx_ids, attrs_changed, since) = batch;
        METRICS.refresh_batches_total.inc();
        METRICS.refresh_txs_total.add(tx_ids.len() as u64);
        refresh_batch(&state, app_id, &tx_ids, attrs_changed).await;
        METRICS.refresh_batch_seconds.observe_since(since);
    }
}

/// Load the triple changes of a set of transactions as tx topics. Unknown
/// changes (query error, nothing logged, catalog change) degrade to a
/// catch-all so no query is ever wrongly skipped.
async fn load_tx_topics(state: &AppState, app_id: Uuid, tx_ids: &[i64]) -> TxTopics {
    let rows = sqlx::query(
        "SELECT entity_id, attr_id, value FROM rust_tx_changes
         WHERE app_id = $1 AND tx_id = ANY($2)",
    )
    .bind(app_id)
    .bind(tx_ids)
    .fetch_all(&state.pool)
    .await;
    match rows {
        Ok(rows) if !rows.is_empty() => TxTopics::from_changes(rows.iter().map(|r| TxChange {
            e: r.get("entity_id"),
            a: r.get("attr_id"),
            v: r.get("value"),
        })),
        Ok(_) => TxTopics::catch_all(),
        Err(e) => {
            tracing::warn!("refresh: failed to load tx changes: {e}");
            TxTopics::catch_all()
        }
    }
}

/// One (query, auth) recomputation shared by every session registering it.
struct Job {
    q: Value,
    perms: PermsCtx,
    /// (session index, query key) pairs waiting on this job
    subscribers: Vec<(usize, String)>,
}

/// (query key, admin?, user id): sessions sharing this see identical results.
type JobKey = (String, bool, Option<Uuid>);
/// Subscribers of a job and its outcome (None when the query failed).
type JobOutcome = (Vec<(usize, String)>, Option<JobResult>);

struct JobResult {
    ws_result: Value,
    hash: u64,
    topics: Arc<QueryTopics>,
    duration_ms: u64,
}

/// Snapshot of one session's registered queries taken under its lock.
struct SessionPlan {
    session: Arc<Session>,
    skip_attrs: bool,
    prev_attrs_hash: Option<u64>,
    /// query key -> previous result hash, for queries that need recomputing
    stale: Vec<(String, u64)>,
}

/// Refresh every local session of an app for a batch of transactions.
pub async fn refresh_batch(
    state: &Arc<AppState>,
    app_id: Uuid,
    tx_ids: &[i64],
    attrs_changed: bool,
) {
    let sessions = state.sessions_for_app(app_id);
    if sessions.is_empty() {
        return;
    }
    let latest = tx_ids.iter().copied().max().unwrap_or(0);
    let attrs = match service::load_attrs(state, app_id).await {
        Ok(a) => a,
        Err(e) => {
            tracing::error!("refresh: failed to load attrs: {e}");
            return;
        }
    };
    let tx_topics = if attrs_changed {
        TxTopics::catch_all()
    } else {
        load_tx_topics(state, app_id, tx_ids).await
    };

    // sync tables: independent per session, fire and forget
    for session in &sessions {
        let state = state.clone();
        let session = session.clone();
        tokio::spawn(async move {
            crate::sync_table::push_updates(&state, &session, app_id, latest).await;
        });
    }

    // 1. snapshot queries under each session lock; decide what to recompute
    let mut plans: Vec<SessionPlan> = Vec::with_capacity(sessions.len());
    let mut jobs: HashMap<JobKey, Job> = HashMap::new();
    for session in sessions {
        let st = session.state.lock().await;
        if st.app_id != Some(app_id) {
            continue;
        }
        let perms = PermsCtx {
            admin: st.admin,
            user_id: st.user.as_ref().map(|u| u.id),
            user_map: None,
            rule_params: None,
        };
        let mut stale = vec![];
        for (key, entry) in &st.queries {
            let matched = match &entry.topics {
                Some(t) => tx_topics.matches(t),
                None => true,
            };
            if !matched {
                METRICS.refresh_queries_skipped_total.inc();
                continue;
            }
            let idx = plans.len();
            let job_key = (key.clone(), perms.admin, perms.user_id);
            match jobs.get_mut(&job_key) {
                Some(job) => {
                    METRICS.refresh_queries_deduped_total.inc();
                    job.subscribers.push((idx, key.clone()));
                }
                None => {
                    jobs.insert(
                        job_key,
                        Job {
                            q: entry.q.clone(),
                            perms: perms.clone(),
                            subscribers: vec![(idx, key.clone())],
                        },
                    );
                }
            }
            stale.push((key.clone(), entry.result_hash));
        }
        let skip_attrs = st.supports_skip_attrs;
        let prev_attrs_hash = st.attrs_hash;
        drop(st);
        plans.push(SessionPlan {
            session,
            skip_attrs,
            prev_attrs_hash,
            stale,
        });
    }

    let wire_attrs = attrs.to_wire_visible();
    let attrs_hash = value_hash(&wire_attrs);
    let needs_attrs_only = plans
        .iter()
        .any(|p| p.skip_attrs && p.prev_attrs_hash != Some(attrs_hash));
    if jobs.is_empty() && !needs_attrs_only {
        return;
    }

    // 2. rules once per batch (non-admin sessions only)
    let rules = if jobs.values().any(|j| !j.perms.admin) {
        match state.pool.acquire().await {
            Ok(mut conn) => match Rules::load(&mut conn, app_id).await {
                Ok(r) => Some(r),
                Err(e) => {
                    tracing::warn!("refresh: failed to load rules: {e}");
                    return;
                }
            },
            Err(e) => {
                tracing::warn!("refresh: pool exhausted: {e}");
                return;
            }
        }
    } else {
        None
    };

    // 3. run distinct recomputations with bounded concurrency
    let concurrency = state.cfg.refresh_concurrency;
    let results: Vec<JobOutcome> = futures::stream::iter(jobs.into_values())
        .map(|job| {
            let state = state.clone();
            let attrs = attrs.clone();
            let rules = rules.as_ref();
            async move {
                METRICS.refresh_queries_computed_total.inc();
                let started = Instant::now();
                let out =
                    service::run_query_full(&state, app_id, &attrs, &job.perms, &job.q, rules)
                        .await;
                let result = match out {
                    Ok(out) => {
                        let ws_result = out.result.to_ws_result();
                        Some(JobResult {
                            hash: value_hash(&ws_result),
                            ws_result,
                            topics: Arc::new(out.topics),
                            duration_ms: started.elapsed().as_millis() as u64,
                        })
                    }
                    Err(e) => {
                        tracing::warn!("refresh query failed: {e}");
                        None
                    }
                };
                (job.subscribers, result)
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    // 4. per session: collect changed computations, update entries, send
    let mut per_session: Vec<Vec<(String, Arc<JobResult>)>> =
        (0..plans.len()).map(|_| vec![]).collect();
    for (subscribers, result) in results {
        let Some(result) = result else { continue };
        let result = Arc::new(result);
        for (idx, key) in subscribers {
            per_session[idx].push((key, result.clone()));
        }
    }
    let processed_isn = service::current_isn(state).await;
    for (plan, outcomes) in plans.into_iter().zip(per_session) {
        let prev: HashMap<&str, u64> = plan.stale.iter().map(|(k, h)| (k.as_str(), *h)).collect();
        let mut computations = vec![];
        let mut updates: Vec<(String, Option<u64>, Arc<QueryTopics>)> = vec![];
        for (key, r) in &outcomes {
            let changed = prev.get(key.as_str()) != Some(&r.hash);
            if changed {
                METRICS.refresh_queries_changed_total.inc();
                // key set mirrors legacy recompute-instaql-query!
                // (session.clj:459-465); result-meta is only populated for the
                // tree return-type; instaql-topic? reports whether a refined
                // topic program was compiled (never — we use coarse topics).
                computations.push((key.clone(), r.clone()));
            }
            updates.push((key.clone(), changed.then_some(r.hash), r.topics.clone()));
        }
        let attrs_changed_for_session = plan.prev_attrs_hash != Some(attrs_hash);
        if computations.is_empty() && !(plan.skip_attrs && attrs_changed_for_session) {
            continue;
        }
        let mut computation_json = Vec::with_capacity(computations.len());
        {
            let mut st = plan.session.state.lock().await;
            if st.app_id != Some(app_id) {
                continue;
            }
            for (key, hash, topics) in updates {
                if let Some(entry) = st.queries.get_mut(&key) {
                    if let Some(h) = hash {
                        entry.result_hash = h;
                    }
                    // topics track the latest (pre-perms) result even when
                    // the visible result did not change
                    entry.topics = Some(topics);
                }
            }
            for (key, r) in computations {
                // the client may have removed the query meanwhile
                let Some(entry) = st.queries.get(&key) else {
                    continue;
                };
                computation_json.push(json!({
                    "instaql-query": entry.q,
                    "instaql-query-hash": value_hash(&entry.q) as u32,
                    "instaql-result": r.ws_result,
                    "result-meta": Value::Null,
                    "result-changed?": true,
                    "duration-ms": r.duration_ms,
                    "instaql-topic?": false,
                }));
            }
            if plan.skip_attrs {
                st.attrs_hash = Some(attrs_hash);
            }
        }
        if computation_json.is_empty() && !(plan.skip_attrs && attrs_changed_for_session) {
            continue;
        }
        let mut msg = json!({
            "op": "refresh-ok",
            "processed-tx-id": latest,
            "processed-isn": processed_isn,
            "computations": computation_json,
        });
        // legacy omits attrs for core > 0.20.4 unless they changed
        // (session.clj:503-533 skip-attrs gating).
        if !plan.skip_attrs || attrs_changed_for_session {
            msg["attrs"] = wire_attrs.clone();
        }
        METRICS.refresh_ok_sent_total.inc();
        plan.session.send(msg);
    }
}
