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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use futures::StreamExt;
use instant_core::perms::Rules;
use instant_core::topics::{QueryTopics, TxChange, TxTopics};
use serde::Serialize;
use serde_json::value::RawValue;
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
            "instant_app_status",
            "instant_rules",
            "instant_webhooks",
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
                // older nodes (rolling deploy) omit the key: treat every
                // catalog change as a schema change like they did
                let schema_changed = payload
                    .get("schema_changed")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(attrs_changed);
                // older nodes (rolling deploy) omit the key: they refreshed
                // every query on any schema change
                let requery_all = payload
                    .get("requery_all")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(schema_changed);
                let changed_attrs: Vec<Uuid> = payload
                    .get("changed_attrs")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().and_then(|s| Uuid::parse_str(s).ok()))
                            .collect()
                    })
                    .unwrap_or_default();
                if let Some(ts) = payload.get("ts").and_then(|v| v.as_i64()) {
                    let lag_ms = chrono::Utc::now().timestamp_millis() - ts;
                    METRICS
                        .notify_lag_seconds
                        .observe(lag_ms.max(0) as f64 / 1000.0);
                }
                if attrs_changed {
                    service::invalidate_attrs(state, app_id);
                }
                enqueue_tx(
                    state,
                    app_id,
                    tx_id,
                    schema_changed,
                    requery_all,
                    changed_attrs,
                );
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
                let delta = payload.get("delta").cloned();
                let state = state.clone();
                tokio::spawn(async move {
                    crate::presence::broadcast_room_refresh(
                        &state,
                        app_id,
                        &room_id,
                        delta.as_ref(),
                    )
                    .await;
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
            "instant_rules" => {
                let Some(app_id) = payload
                    .get("app_id")
                    .and_then(|v| v.as_str())
                    .and_then(|s| Uuid::parse_str(s).ok())
                else {
                    continue;
                };
                state.query_cache.retain(|k, _| k.0 != app_id);
            }
            "instant_webhooks" => {
                if let Some(app_id) = payload
                    .get("app_id")
                    .and_then(|v| v.as_str())
                    .and_then(|s| Uuid::parse_str(s).ok())
                {
                    crate::webhooks::invalidate_cache(state, app_id);
                }
            }
            "instant_app_status" => {
                let (Some(app_id), Some(status)) = (
                    payload
                        .get("app_id")
                        .and_then(|v| v.as_str())
                        .and_then(|s| Uuid::parse_str(s).ok()),
                    payload.get("status").and_then(|v| v.as_str()),
                ) else {
                    continue;
                };
                service::set_app_status(state, app_id, status.to_string());
                // legacy frame: {op, status} (+ trace-id), no version gating
                // (cache_evict.clj:74-83; Reactor.js:899-920)
                let msg = json!({"op": "app-status-changed", "status": status});
                for s in state.sessions_for_app(app_id) {
                    s.send(msg.clone());
                }
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
    /// a pending tx requires refreshing every session (attr insert / delete,
    /// ident change)
    schema_changed: bool,
    /// a pending tx stales every query (a blob attr restored)
    requery_all: bool,
    /// attrs whose rows a pending tx changed (attr-wildcard topics)
    changed_attrs: HashSet<Uuid>,
    /// queries to recompute whatever the batch's topics say, by session id:
    /// registered by an add-query that a tx may have raced (enqueue_recheck)
    recheck: HashMap<Uuid, HashSet<String>>,
    /// newest tx id a recheck was requested at
    recheck_tx_id: i64,
    /// when the oldest pending tx was enqueued (for batch latency)
    since: Option<Instant>,
    /// a worker task is draining this queue
    running: bool,
}

impl RefreshQueue {
    pub fn pending_len(&self) -> usize {
        self.pending.lock().map(|p| p.tx_ids.len()).unwrap_or(0)
    }

    /// Mark the queue as having work; true when the caller must start the
    /// worker.
    fn claim_worker(p: &mut Pending) -> bool {
        p.since.get_or_insert_with(Instant::now);
        !std::mem::replace(&mut p.running, true)
    }
}

fn spawn_worker(state: &Arc<AppState>, app_id: Uuid, queue: Arc<RefreshQueue>) {
    let state = state.clone();
    tokio::spawn(async move {
        refresh_worker(state, app_id, queue).await;
    });
}

/// Queue a tx for refresh; starts the app's worker if none is running.
pub fn enqueue_tx(
    state: &Arc<AppState>,
    app_id: Uuid,
    tx_id: i64,
    schema_changed: bool,
    requery_all: bool,
    changed_attrs: Vec<Uuid>,
) {
    let queue = state.refresh_queues.entry(app_id).or_default().clone();
    let start_worker = {
        let mut p = queue.pending.lock().unwrap_or_else(|e| e.into_inner());
        p.tx_ids.insert(tx_id);
        p.schema_changed |= schema_changed;
        p.requery_all |= requery_all;
        p.changed_attrs.extend(changed_attrs);
        RefreshQueue::claim_worker(&mut p)
    };
    if start_worker {
        spawn_worker(state, app_id, queue);
    }
}

/// Queue a recomputation of one session's query as of `tx_id`, whatever
/// the batch's topics. add-query registers a query after computing it, so
/// a tx committing in between can be matched against the session's queries
/// before this one is there; rechecking it on the app's refresh worker
/// keeps its refresh-ok frames in order with every other refresh.
pub fn enqueue_recheck(
    state: &Arc<AppState>,
    app_id: Uuid,
    session_id: Uuid,
    query_key: String,
    tx_id: i64,
) {
    let queue = state.refresh_queues.entry(app_id).or_default().clone();
    let start_worker = {
        let mut p = queue.pending.lock().unwrap_or_else(|e| e.into_inner());
        p.recheck.entry(session_id).or_default().insert(query_key);
        p.recheck_tx_id = p.recheck_tx_id.max(tx_id);
        RefreshQueue::claim_worker(&mut p)
    };
    if start_worker {
        spawn_worker(state, app_id, queue);
    }
}

async fn refresh_worker(state: Arc<AppState>, app_id: Uuid, queue: Arc<RefreshQueue>) {
    loop {
        let batch = {
            let mut p = queue.pending.lock().unwrap_or_else(|e| e.into_inner());
            if p.tx_ids.is_empty() && p.recheck.is_empty() {
                p.running = false;
                break;
            }
            let tx_ids: Vec<i64> = std::mem::take(&mut p.tx_ids).into_iter().collect();
            let batch = Batch {
                latest: tx_ids
                    .iter()
                    .copied()
                    .max()
                    .unwrap_or(0)
                    .max(std::mem::take(&mut p.recheck_tx_id)),
                tx_ids,
                schema_changed: std::mem::take(&mut p.schema_changed),
                requery_all: std::mem::take(&mut p.requery_all),
                changed_attrs: std::mem::take(&mut p.changed_attrs),
                recheck: std::mem::take(&mut p.recheck),
            };
            let since = p.since.take().unwrap_or_else(Instant::now);
            (batch, since)
        };
        let (batch, since) = batch;
        METRICS.refresh_batches_total.inc();
        METRICS.refresh_txs_total.add(batch.tx_ids.len() as u64);
        refresh_batch(&state, app_id, &batch).await;
        METRICS.refresh_batch_seconds.observe_since(since);
    }
}

/// Load the triple changes of a set of transactions as tx topics. Unknown
/// changes (query error, nothing logged, catalog change) degrade to a
/// catch-all so no query is ever wrongly skipped.
/// Topics of a batch of txs: the captured triple changes plus a wildcard
/// per changed attr row (legacy topics-for-attr-upsert). A batch with
/// neither is unknown (nothing was captured) and refreshes every query.
async fn load_tx_topics(
    state: &AppState,
    app_id: Uuid,
    tx_ids: &[i64],
    changed_attrs: &HashSet<Uuid>,
) -> TxTopics {
    let rows = sqlx::query(
        "SELECT entity_id, attr_id, value FROM rust_tx_changes
         WHERE app_id = $1 AND tx_id = ANY($2)",
    )
    .bind(app_id)
    .bind(tx_ids)
    .fetch_all(&state.pool)
    .await;
    let triples = match rows {
        Ok(rows) if !rows.is_empty() => TxTopics::from_changes(rows.iter().map(|r| TxChange {
            e: r.get("entity_id"),
            a: r.get("attr_id"),
            v: r.get("value"),
        })),
        // no logged change: the tx rewrote values unchanged (the capture
        // trigger skips those) or only touched attr rows. Legacy's topics
        // for an unchanged rewrite match only queries on that very triple,
        // whose results can't have moved, so nothing goes stale; staling
        // every query instead would recompute them and charge `rateLimit`
        // view buckets legacy never charges.
        Ok(_) => TxTopics::default(),
        Err(e) => {
            tracing::warn!("refresh: failed to load tx changes: {e}");
            return TxTopics::catch_all();
        }
    };
    triples.with_attr_wildcards(changed_attrs.iter().copied())
}

/// One drained refresh queue: the txs to refresh for plus the queries
/// rechecked regardless of topics.
pub struct Batch {
    pub tx_ids: Vec<i64>,
    /// processed-tx-id of the batch: the newest tx or recheck
    pub latest: i64,
    pub schema_changed: bool,
    pub requery_all: bool,
    pub changed_attrs: HashSet<Uuid>,
    pub recheck: HashMap<Uuid, HashSet<String>>,
}

/// One (query, auth) recomputation shared by every session registering it.
struct Job {
    q: Value,
    perms: PermsCtx,
    /// tree return-type (admin SSE subscribeQuery) and the session's
    /// `inference?`, which shape the frame
    tree: bool,
    inference: bool,
    /// (session index, query key) pairs waiting on this job
    subscribers: Vec<(usize, String)>,
}

/// (query key, admin?, user id, ip, origin, tree?, inference?, session):
/// sessions sharing this see identical frames. The session slot is only
/// filled when the app's rules call `rateLimit`: legacy recomputes per
/// session, so each session's refresh charges the buckets.
type JobKey = (
    String,
    bool,
    Option<Uuid>,
    Option<String>,
    Option<String>,
    bool,
    bool,
    Option<usize>,
);
/// Subscribers of a job and its outcome.
type JobOutcome = (
    Vec<(usize, String)>,
    Result<JobResult, instant_core::error::InstantError>,
);

struct JobResult {
    /// the instaql-result, serialized once for every subscriber
    ws_json: Box<RawValue>,
    /// page-info / aggregate for tree results, null for join-rows
    result_meta: Value,
    hash: u64,
    topics: Arc<QueryTopics>,
    duration_ms: u64,
}

/// refresh-ok on the wire; the result is spliced in as raw bytes. Key set
/// mirrors legacy recompute-instaql-query! (session.clj:459-465).
#[derive(Serialize)]
struct RefreshOkWire<'a> {
    op: &'static str,
    #[serde(rename = "processed-tx-id")]
    processed_tx_id: i64,
    #[serde(rename = "processed-isn")]
    processed_isn: &'a Value,
    computations: Vec<ComputationWire<'a>>,
    /// legacy omits attrs for core > 0.20.4 unless they changed
    /// (session.clj:503-533 skip-attrs gating)
    #[serde(skip_serializing_if = "Option::is_none")]
    attrs: Option<&'a Value>,
    #[serde(rename = "trace-id")]
    trace_id: String,
}

#[derive(Serialize)]
struct ComputationWire<'a> {
    #[serde(rename = "instaql-query")]
    instaql_query: &'a Value,
    #[serde(rename = "instaql-query-hash")]
    instaql_query_hash: u32,
    #[serde(rename = "instaql-result")]
    instaql_result: &'a RawValue,
    /// only populated for the tree return-type
    #[serde(rename = "result-meta")]
    result_meta: &'a Value,
    #[serde(rename = "result-changed?")]
    result_changed: bool,
    #[serde(rename = "duration-ms")]
    duration_ms: u64,
    /// whether a refined topic program was compiled (never — coarse topics)
    #[serde(rename = "instaql-topic?")]
    instaql_topic: bool,
}

/// Snapshot of one session's registered queries taken under its lock.
struct SessionPlan {
    session: Arc<Session>,
    skip_attrs: bool,
    prev_attrs_hash: Option<u64>,
    /// keys of the queries that need recomputing
    stale: Vec<String>,
}

/// A stale query of one session, before recomputations are shared.
struct StaleQuery {
    plan: usize,
    key: String,
    q: Value,
    perms: PermsCtx,
    tree: bool,
    inference: bool,
}

/// Refresh every local session of an app for a batch of transactions.
#[tracing::instrument(level = "debug", skip_all, fields(app_id = %app_id, txs = batch.tx_ids.len()))]
pub async fn refresh_batch(state: &Arc<AppState>, app_id: Uuid, batch: &Batch) {
    let Batch {
        tx_ids,
        latest,
        schema_changed,
        requery_all,
        changed_attrs,
        recheck,
    } = batch;
    let (latest, schema_changed, requery_all) = (*latest, *schema_changed, *requery_all);
    let sessions = state.sessions_for_app(app_id);
    if sessions.is_empty() {
        return;
    }
    let attrs = match service::load_attrs(state, app_id).await {
        Ok(a) => a,
        Err(e) => {
            tracing::error!("refresh: failed to load attrs: {e}");
            return;
        }
    };
    // Legacy stales only the queries whose topics a change matches — attr
    // rows included (topics-for-attr-changes) — and recomputes only stale
    // queries (store.clj get-stale-instaql-queries). A schema change
    // additionally reaches every session (invalidator.clj:225-231), so they
    // learn the new attrs, but it does not recompute their queries; only
    // restoring a blob attr stales every query (topics-for-attr-upsert's
    // `[#{:ea} _ _ _]`).
    let tx_topics = if requery_all {
        TxTopics::catch_all()
    } else if tx_ids.is_empty() && changed_attrs.is_empty() {
        // rechecks only
        TxTopics::default()
    } else {
        load_tx_topics(state, app_id, tx_ids, changed_attrs).await
    };

    // 1. snapshot queries under each session lock; decide what to recompute
    let mut plans: Vec<SessionPlan> = Vec::with_capacity(sessions.len());
    let mut stale_queries: Vec<StaleQuery> = vec![];
    for session in sessions {
        let st = session.state.lock().await;
        if st.app_id != Some(app_id) {
            continue;
        }
        if !st.sync_subs.is_empty() {
            // sync tables: independent per session, fire and forget (only
            // the few sessions that subscribed — not a task per session)
            let state = state.clone();
            let session = session.clone();
            tokio::spawn(async move {
                crate::sync_table::push_updates(&state, &session, app_id, latest).await;
            });
        }
        let perms = PermsCtx {
            admin: st.admin,
            user_id: st.user.as_ref().map(|u| u.id),
            user_map: None,
            rule_params: None,
            ip: st.ip.clone(),
            origin: st.origin.clone(),
        };
        let mut stale = vec![];
        let rechecked = recheck.get(&session.id);
        for (key, entry) in &st.queries {
            let matched = rechecked.is_some_and(|keys| keys.contains(key))
                || match &entry.topics {
                    Some(t) => tx_topics.matches(t),
                    None => true,
                };
            if !matched {
                METRICS.refresh_queries_skipped_total.inc();
                continue;
            }
            stale_queries.push(StaleQuery {
                plan: plans.len(),
                key: key.clone(),
                q: entry.q.clone(),
                perms: perms.clone(),
                tree: entry.tree,
                inference: st.inference,
            });
            stale.push(key.clone());
        }
        // legacy only runs handle-refresh! for sockets the invalidator
        // picked (a stale query, or every socket on a schema change); a
        // session with nothing stale gets no frame, not even an attrs-only
        // refresh-ok when the attrs hash moved (session.clj:467-530)
        if stale.is_empty() && !schema_changed {
            continue;
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
    if stale_queries.is_empty() && !needs_attrs_only && !schema_changed {
        return;
    }

    // 2. rules once per batch (non-admin sessions only)
    let rules = if stale_queries.iter().any(|q| !q.perms.admin) {
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
    let per_session_jobs = rules.as_ref().is_some_and(|r| r.uses_rate_limits());

    // 3. share recomputations between sessions that would see the same
    //    frames; rules may read request.ip / request.origin, so only
    //    sessions with the same request facts share one
    let mut jobs: HashMap<JobKey, Job> = HashMap::new();
    for sq in stale_queries {
        let job_key = (
            sq.key.clone(),
            sq.perms.admin,
            sq.perms.user_id,
            sq.perms.ip.clone(),
            sq.perms.origin.clone(),
            sq.tree,
            sq.inference,
            (per_session_jobs && !sq.perms.admin).then_some(sq.plan),
        );
        match jobs.get_mut(&job_key) {
            Some(job) => {
                METRICS.refresh_queries_deduped_total.inc();
                job.subscribers.push((sq.plan, sq.key));
            }
            None => {
                jobs.insert(
                    job_key,
                    Job {
                        q: sq.q,
                        perms: sq.perms,
                        tree: sq.tree,
                        inference: sq.inference,
                        subscribers: vec![(sq.plan, sq.key)],
                    },
                );
            }
        }
    }

    // 4. run distinct recomputations with bounded concurrency
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
                        let (wire, result_meta, hash) = crate::ws::format_query_result(
                            &out.result,
                            &attrs,
                            &job.q,
                            job.tree,
                            job.inference,
                        );
                        let ws_json = RawValue::from_string(wire.to_string())
                            .expect("serde_json output is valid JSON");
                        Ok(JobResult {
                            ws_json,
                            result_meta,
                            hash,
                            topics: Arc::new(out.topics),
                            duration_ms: started.elapsed().as_millis() as u64,
                        })
                    }
                    Err(e) => {
                        tracing::debug!("refresh query failed: {e}");
                        METRICS.refresh_queries_failed_total.inc();
                        Err(e)
                    }
                };
                (job.subscribers, result)
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    // 5. per session: collect changed computations, update entries, send
    let mut per_session: Vec<Vec<(String, Arc<JobResult>)>> =
        (0..plans.len()).map(|_| vec![]).collect();
    let mut failed: Vec<Vec<(String, Arc<instant_core::error::InstantError>)>> =
        (0..plans.len()).map(|_| vec![]).collect();
    for (subscribers, result) in results {
        match result {
            Ok(result) => {
                let result = Arc::new(result);
                for (idx, key) in subscribers {
                    per_session[idx].push((key, result.clone()));
                }
            }
            Err(e) => {
                let e = Arc::new(e);
                for (idx, key) in subscribers {
                    failed[idx].push((key, e.clone()));
                }
            }
        }
    }
    state.chaos_pause().await;
    let processed_isn = service::current_isn(state).await;
    for ((plan, outcomes), failures) in plans.into_iter().zip(per_session).zip(failed) {
        if !failures.is_empty() {
            refresh_failed(&plan, &outcomes, &failures, latest, &processed_isn).await;
            continue;
        }
        let attrs_changed_for_session = plan.prev_attrs_hash != Some(attrs_hash);
        // legacy refreshes every session on a schema change
        // (schema-changes-require-refreshing-sessions?) and a session without
        // skip-attrs gets the refresh-ok even with nothing recomputed, so its
        // attrs are current (session.clj:503-533)
        let attrs_only = schema_changed && !plan.skip_attrs;
        let send_attrs = (plan.skip_attrs && attrs_changed_for_session) || attrs_only;
        if outcomes.is_empty() && !send_attrs {
            continue;
        }
        let frame = {
            let mut st = plan.session.state.lock().await;
            if st.app_id != Some(app_id) {
                continue;
            }
            // Changed means "differs from what the client last received":
            // the entry's hash now, under the lock every sender of this
            // session's query frames holds. The hash snapshotted in step 1 can
            // be stale by now (add-query re-registered the query meanwhile).
            let mut changed: Vec<&(String, Arc<JobResult>)> = vec![];
            for item in &outcomes {
                let (key, r) = item;
                // the client may have removed the query meanwhile
                let Some(entry) = st.queries.get_mut(key) else {
                    continue;
                };
                // topics track the latest (pre-perms) result even when
                // the visible result did not change
                entry.topics = Some(r.topics.clone());
                if entry.result_hash != r.hash {
                    entry.result_hash = r.hash;
                    METRICS.refresh_queries_changed_total.inc();
                    changed.push(item);
                }
            }
            if changed.is_empty() && !send_attrs {
                continue;
            }
            if plan.skip_attrs {
                st.attrs_hash = Some(attrs_hash);
            }
            // key set mirrors legacy recompute-instaql-query!
            // (session.clj:459-465); result-meta is only populated for the
            // tree return-type; instaql-topic? reports whether a refined
            // topic program was compiled (never — we use coarse topics).
            let wire: Vec<ComputationWire> = changed
                .iter()
                .filter_map(|(key, r)| {
                    let entry = st.queries.get(key)?;
                    Some(ComputationWire {
                        instaql_query: &entry.q,
                        instaql_query_hash: value_hash(&entry.q) as u32,
                        instaql_result: &r.ws_json,
                        result_meta: &r.result_meta,
                        result_changed: true,
                        duration_ms: r.duration_ms,
                        instaql_topic: false,
                    })
                })
                .collect();
            let msg = RefreshOkWire {
                op: "refresh-ok",
                processed_tx_id: latest,
                processed_isn: &processed_isn,
                computations: wire,
                attrs: (!plan.skip_attrs || attrs_changed_for_session).then_some(&wire_attrs),
                trace_id: crate::state::new_trace_id(),
            };
            match serde_json::to_string(&msg) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("refresh-ok serialization failed: {e}");
                    continue;
                }
            }
        };
        METRICS.refresh_ok_sent_total.inc();
        plan.session.send_raw(frame);
    }
}

/// A session whose refresh hit a failing recomputation, the way legacy's
/// handle-refresh! fails: its recomputations run in one `pmap`, so one
/// throwing query fails the whole refresh. Every recomputation that did
/// finish has already recorded its result (query.clj
/// instaql-query-reactive! -> add-instaql-query!), the failing query was
/// unsubscribed (its catch -> rs/remove-query!), no refresh-ok goes out and
/// the attrs hash stays put; the exception reaches the client as an `error`
/// frame whose original-event is the refresh event
/// (session.clj handle-instant-exception / handle-error!).
async fn refresh_failed(
    plan: &SessionPlan,
    outcomes: &[(String, Arc<JobResult>)],
    failures: &[(String, Arc<instant_core::error::InstantError>)],
    latest: i64,
    processed_isn: &Value,
) {
    let frame = {
        let mut st = plan.session.state.lock().await;
        for (key, r) in outcomes {
            if let Some(entry) = st.queries.get_mut(key) {
                entry.result_hash = r.hash;
                entry.topics = Some(r.topics.clone());
            }
        }
        let queries: Vec<Value> = plan
            .stale
            .iter()
            .filter_map(|k| st.queries.get(k).map(|e| e.q.clone()))
            .collect();
        for (key, _) in failures {
            st.queries.remove(key);
        }
        let original = json!({
            "op": "refresh",
            "session-id": plan.session.id,
            "tx-id": latest,
            "processed-tx-id": latest,
            "processed-isn": processed_isn,
            "instaql-queries": queries,
        });
        crate::ws::err_msg(&original, &failures[0].1)
    };
    METRICS.refresh_errors_sent_total.inc();
    plan.session.send(frame);
}
