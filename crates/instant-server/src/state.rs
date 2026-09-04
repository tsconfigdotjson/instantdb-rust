use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use instant_core::attr::AttrMap;
use instant_core::topics::QueryTopics;
use serde_json::Value;
use sqlx::PgPool;
use tokio::sync::{mpsc, Mutex};
use uuid::Uuid;

#[derive(Clone)]
pub struct Config {
    pub database_url: String,
    pub port: u16,
    /// Public base URL of this server (for oauth redirects, file urls).
    pub base_url: String,
    /// Secret for signing storage URLs. Taken from SERVER_SECRET, or — when
    /// unset — resolved at boot to a random secret persisted in Postgres
    /// (service::load_or_generate_secret), so there is no guessable default.
    pub secret: String,
    /// Postgres pool sizing (`PG_POOL_MAX` / `PG_POOL_MIN`).
    pub pg_pool_max: u32,
    pub pg_pool_min: u32,
    /// Max concurrent query recomputations per app refresh batch
    /// (`INSTANT_REFRESH_CONCURRENCY`); bounded so one busy app can't drain
    /// the pool for everyone else.
    pub refresh_concurrency: usize,
    /// Outgoing messages a session may have queued before it is treated as a
    /// dead/slow consumer and disconnected (`INSTANT_MAX_QUEUED_MESSAGES`).
    pub max_queued_messages: usize,
    /// Rows an indexing job rewrites per step (`INSTANT_INDEXING_BATCH_SIZE`,
    /// legacy batch-size 1000); the job is released between steps.
    pub indexing_batch_size: usize,
    /// How often each node looks for unowned indexing jobs
    /// (`INSTANT_INDEXING_SWEEP_SECS`).
    pub indexing_sweep_secs: u64,
    /// A processing job with no progress for this long is treated as
    /// orphaned by a dead node and reclaimed (`INSTANT_INDEXING_STALE_SECS`).
    pub indexing_stale_secs: u64,
}

fn env_num<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

impl Config {
    pub fn from_env() -> Self {
        let port = env_num("PORT", 8888u16);
        Config {
            database_url: std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://instant:instant@localhost:5432/instant".into()),
            port,
            base_url: std::env::var("BASE_URL")
                .unwrap_or_else(|_| format!("http://localhost:{port}")),
            // Empty means "not configured"; main() fills it in from Postgres.
            secret: std::env::var("SERVER_SECRET").unwrap_or_default(),
            pg_pool_max: env_num("PG_POOL_MAX", 20u32).max(2),
            pg_pool_min: env_num("PG_POOL_MIN", 2u32),
            refresh_concurrency: env_num("INSTANT_REFRESH_CONCURRENCY", 8usize).max(1),
            max_queued_messages: env_num("INSTANT_MAX_QUEUED_MESSAGES", 10_000usize).max(100),
            indexing_batch_size: env_num("INSTANT_INDEXING_BATCH_SIZE", 1000usize).max(1),
            indexing_sweep_secs: env_num("INSTANT_INDEXING_SWEEP_SECS", 60u64).max(1),
            indexing_stale_secs: env_num("INSTANT_INDEXING_STALE_SECS", 600u64).max(30),
        }
    }
}

/// Authenticated user attached to a session.
#[derive(Debug, Clone)]
pub struct SessionUser {
    pub id: Uuid,
    pub email: Option<String>,
}

#[derive(Debug, Default)]
pub struct SessionState {
    pub app_id: Option<Uuid>,
    pub user: Option<SessionUser>,
    pub admin: bool,
    /// canonical-q-string -> query json
    pub queries: HashMap<String, QueryEntry>,
    pub rooms: HashSet<String>,
    /// rule params attached via queries ($$ruleParams)
    pub versions: Option<Value>,
    /// client accepts patch-presence (core > 0.17.5)
    pub supports_patch_presence: bool,
    /// client accepts refresh-ok without attrs when unchanged (core > 0.20.4)
    pub supports_skip_attrs: bool,
    /// hash of the last attrs array sent, for skip-attrs change detection
    pub attrs_hash: Option<u64>,
    /// set for SSE-transport sessions; validates /runtime/sse pushes
    pub sse_token: Option<Uuid>,
    /// legacy `:session/inference?` (admin SSE sessions, `inference?` in the
    /// request body): singular links in tree-shaped results
    pub inference: bool,
    /// active sync-table subscriptions: sub id -> state
    pub sync_subs: HashMap<Uuid, SyncSub>,
    /// stream ids this session is the writer for
    pub writing_streams: HashSet<Uuid>,
    /// `request.ip` / `request.origin` from the upgrade request (legacy
    /// socket-ip / socket-origin, reactive/store.clj:497-508); every op on
    /// the socket, refreshes included, evaluates rules with these
    pub ip: Option<String>,
    pub origin: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SyncSub {
    pub etype: String,
    pub last_tx: i64,
    /// the etype's cardinality-one attrs when the subscription started (or
    /// was resynced): legacy derives the sub's topics then and keeps them
    /// for the session's lifetime, so an attr added later is not synced
    /// until a resync
    pub attr_ids: Vec<Uuid>,
}

#[derive(Debug, Clone)]
pub struct QueryEntry {
    pub q: Value,
    /// hash of the last result sent, for refresh spam suppression
    pub result_hash: u64,
    /// invalidation topics of the last result (instant_core::topics); None
    /// means "unknown — recompute on every tx"
    pub topics: Option<Arc<QueryTopics>>,
    /// legacy `:instaql-query/return-type`: `tree` (admin SSE subscribeQuery,
    /// object tree + result-meta) instead of the default join-rows
    pub tree: bool,
}

/// One outgoing message: a JSON value, or a frame already serialized (the
/// refresh fan-out serializes each shared result once and hands every
/// subscriber the bytes).
pub enum Outgoing {
    Json(Value),
    Raw(String),
}

impl Outgoing {
    pub fn into_string(self) -> String {
        match self {
            Outgoing::Json(v) => v.to_string(),
            Outgoing::Raw(s) => s,
        }
    }
}

pub struct Session {
    pub id: Uuid,
    pub tx: mpsc::UnboundedSender<Outgoing>,
    pub state: Mutex<SessionState>,
    /// client accepts JSON-array frames (core > 0.22.75); read by the ws writer
    pub batch_messages: AtomicBool,
    /// messages queued for the transport writer but not yet written
    pub queued: AtomicUsize,
    /// queue cap; exceeding it flags the session as overflowed
    pub max_queued: usize,
    /// set once the outgoing queue overflowed — the transport closes the
    /// session instead of buffering without bound
    pub overflowed: AtomicBool,
}

impl Session {
    pub fn new(id: Uuid, tx: mpsc::UnboundedSender<Outgoing>, max_queued: usize) -> Session {
        Session {
            id,
            tx,
            state: Default::default(),
            batch_messages: Default::default(),
            queued: AtomicUsize::new(0),
            max_queued,
            overflowed: AtomicBool::new(false),
        }
    }

    pub fn send(&self, mut msg: Value) {
        // Legacy stamps every outgoing event with a trace-id (rs/send-event!).
        if let Value::Object(m) = &mut msg {
            m.entry("trace-id").or_insert_with(|| new_trace_id().into());
        }
        self.enqueue(Outgoing::Json(msg));
    }

    /// Queue an already-serialized frame (must carry its own trace-id).
    pub fn send_raw(&self, frame: String) {
        self.enqueue(Outgoing::Raw(frame));
    }

    fn enqueue(&self, msg: Outgoing) {
        if self.overflowed.load(Ordering::Relaxed) {
            return;
        }
        if self.queued.load(Ordering::Relaxed) >= self.max_queued {
            // Slow/dead consumer: stop feeding it. The transport task sees
            // the flag and closes the connection; the client reconnects and
            // re-registers its queries, which is cheaper than unbounded
            // buffering on the server.
            self.overflowed.store(true, Ordering::Relaxed);
            crate::metrics::METRICS.ws_sessions_overflowed_total.inc();
            return;
        }
        self.queued.fetch_add(1, Ordering::Relaxed);
        crate::metrics::METRICS.ws_messages_sent_total.inc();
        if self.tx.send(msg).is_err() {
            self.queued.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Called by the transport after taking a message off the queue.
    pub fn dequeued(&self, n: usize) {
        self.queued.fetch_sub(n, Ordering::Relaxed);
    }
}

/// Random 32-hex-char trace id, matching the OTel-style ids legacy emits.
pub fn new_trace_id() -> String {
    let id = Uuid::new_v4();
    id.simple().to_string()
}

/// Cached add-query result. Reconnect storms register the same handful of
/// queries from thousands of sessions within seconds; sessions sharing an
/// (app, query, auth) at the same tx watermark share one computation.
pub struct QueryCacheEntry {
    /// the instaql-result, serialized once
    pub ws_json: Arc<Box<serde_json::value::RawValue>>,
    pub hash: u64,
    pub topics: Arc<QueryTopics>,
    /// app tx watermark (max tx id) read before the query ran
    pub tx_id: i64,
    pub attr_gen: u64,
    pub created: std::time::Instant,
}

/// (app id, canonical query, admin?, user id)
/// (app, query, admin?, user, request.ip, request.origin): rules may read
/// `request.ip` / `request.origin`, so results are only shared between
/// sessions with the same request facts.
pub type QueryCacheKey = (
    Uuid,
    String,
    bool,
    Option<Uuid>,
    Option<String>,
    Option<String>,
);

/// Entries older than this are never served (bounds the staleness of results
/// computed under permission rules that changed without a transaction).
pub const QUERY_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(5);

/// Cached attr catalog for one app (service::load_attrs).
pub struct AttrCacheEntry {
    pub attrs: Arc<AttrMap>,
    pub loaded_at: std::time::Instant,
}

pub struct AppState {
    pub cfg: Config,
    pub email: crate::email::EmailConfig,
    pub pool: PgPool,
    pub node_id: Uuid,
    pub sessions: DashMap<Uuid, Arc<Session>>,
    /// app id -> session ids on this node
    pub app_sessions: DashMap<Uuid, HashSet<Uuid>>,
    /// (app id, room id) -> session ids on this node
    pub room_sessions: DashMap<(Uuid, String), HashSet<Uuid>>,
    /// oauth discovery/JWKS cache
    pub oauth_cache: DashMap<String, (Value, std::time::Instant)>,
    /// last presence snapshot per (app, room) and when it was last read in
    /// full from Postgres — deltas from NOTIFY payloads are applied on top
    pub room_snapshots: DashMap<(Uuid, String), (Value, std::time::Instant)>,
    /// add-query result cache (QueryCacheEntry)
    pub query_cache: DashMap<QueryCacheKey, QueryCacheEntry>,
    /// live stream subscribers on this node: (app, stream) -> (session, subscribe event id)
    pub stream_subs: DashMap<(Uuid, Uuid), HashSet<(Uuid, String)>>,
    /// subscribers still receiving their catch-up snapshot: live appends
    /// that arrive meanwhile are parked here (keyed by (session, subscribe
    /// event id)) and replayed after the snapshot, so the reader never sees
    /// a frame ahead of what it has been told (Stream.ts:565-570 treats a
    /// gap as a corrupted stream)
    pub stream_catchup: DashMap<(Uuid, String), Vec<Value>>,
    /// per-app token buckets (issue #1)
    pub limiters: crate::rate_limit::Limiters,
    /// per-app attr catalog cache (issue #11), invalidated on attrs_changed
    /// tx notifications
    pub attr_cache: DashMap<Uuid, AttrCacheEntry>,
    /// per-app invalidation generation for the attr cache
    pub attr_gen: DashMap<Uuid, u64>,
    /// per-app pending refresh work (invalidator::RefreshQueue)
    pub refresh_queues: DashMap<Uuid, Arc<crate::invalidator::RefreshQueue>>,
    /// per-app `apps.status` cache for the read gate, refreshed by the
    /// `instant_app_status` NOTIFY (and a TTL as the safety net)
    pub app_status_cache: DashMap<Uuid, (String, std::time::Instant)>,
}

impl AppState {
    pub fn new(cfg: Config, pool: PgPool) -> Arc<Self> {
        Arc::new(AppState {
            cfg,
            email: crate::email::EmailConfig::from_env(),
            pool,
            node_id: Uuid::new_v4(),
            sessions: DashMap::new(),
            app_sessions: DashMap::new(),
            room_sessions: DashMap::new(),
            oauth_cache: DashMap::new(),
            room_snapshots: DashMap::new(),
            query_cache: DashMap::new(),
            stream_subs: DashMap::new(),
            stream_catchup: DashMap::new(),
            limiters: crate::rate_limit::Limiters::from_env(),
            attr_cache: DashMap::new(),
            attr_gen: DashMap::new(),
            refresh_queues: DashMap::new(),
            app_status_cache: DashMap::new(),
        })
    }

    pub fn new_session(&self, id: Uuid, tx: mpsc::UnboundedSender<Outgoing>) -> Arc<Session> {
        let session = Arc::new(Session::new(id, tx, self.cfg.max_queued_messages));
        self.sessions.insert(id, session.clone());
        crate::metrics::METRICS.ws_connections_total.inc();
        session
    }

    pub fn register_app_session(&self, app_id: Uuid, session_id: Uuid) {
        self.app_sessions
            .entry(app_id)
            .or_default()
            .insert(session_id);
    }

    pub fn drop_session(&self, session_id: Uuid) {
        if let Some((_, session)) = self.sessions.remove(&session_id) {
            drop(session);
        }
        self.app_sessions.retain(|_, set| {
            set.remove(&session_id);
            !set.is_empty()
        });
        self.room_sessions.iter_mut().for_each(|mut e| {
            e.value_mut().remove(&session_id);
        });
    }

    pub fn sessions_for_app(&self, app_id: Uuid) -> Vec<Arc<Session>> {
        self.app_sessions
            .get(&app_id)
            .map(|set| {
                set.iter()
                    .filter_map(|sid| self.sessions.get(sid).map(|s| s.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn sessions_for_room(&self, app_id: Uuid, room_id: &str) -> Vec<Arc<Session>> {
        self.room_sessions
            .get(&(app_id, room_id.to_string()))
            .map(|set| {
                set.iter()
                    .filter_map(|sid| self.sessions.get(sid).map(|s| s.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Stable hash for refresh spam suppression.
pub fn value_hash(v: &Value) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    v.to_string().hash(&mut h);
    h.finish()
}
