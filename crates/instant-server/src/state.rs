use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dashmap::DashMap;
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
    /// Secret for signing storage URLs and oauth cookies.
    pub secret: String,
}

impl Config {
    pub fn from_env() -> Self {
        let port = std::env::var("PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8888);
        Config {
            database_url: std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://instant:instant@localhost:5432/instant".into()),
            port,
            base_url: std::env::var("BASE_URL")
                .unwrap_or_else(|_| format!("http://localhost:{port}")),
            secret: std::env::var("SERVER_SECRET").unwrap_or_else(|_| "dev-secret".into()),
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
    /// active sync-table subscriptions: sub id -> state
    pub sync_subs: HashMap<Uuid, SyncSub>,
    /// stream ids this session is the writer for
    pub writing_streams: HashSet<Uuid>,
}

#[derive(Debug, Clone)]
pub struct SyncSub {
    pub etype: String,
    pub last_tx: i64,
}

#[derive(Debug, Clone)]
pub struct QueryEntry {
    pub q: Value,
    /// hash of the last result sent, for refresh spam suppression
    pub result_hash: u64,
}

pub struct Session {
    pub id: Uuid,
    pub tx: mpsc::UnboundedSender<Value>,
    pub state: Mutex<SessionState>,
    /// serializes refreshes per session
    pub refresh_lock: Mutex<()>,
    /// client accepts JSON-array frames (core > 0.22.75); read by the ws writer
    pub batch_messages: std::sync::atomic::AtomicBool,
}

impl Session {
    pub fn send(&self, mut msg: Value) {
        // Legacy stamps every outgoing event with a trace-id (rs/send-event!).
        if let Value::Object(m) = &mut msg {
            m.entry("trace-id").or_insert_with(|| new_trace_id().into());
        }
        let _ = self.tx.send(msg);
    }
}

/// Random 32-hex-char trace id, matching the OTel-style ids legacy emits.
pub fn new_trace_id() -> String {
    let id = Uuid::new_v4();
    id.simple().to_string()
}

pub struct AppState {
    pub cfg: Config,
    pub pool: PgPool,
    pub node_id: Uuid,
    pub sessions: DashMap<Uuid, Arc<Session>>,
    /// app id -> session ids on this node
    pub app_sessions: DashMap<Uuid, HashSet<Uuid>>,
    /// (app id, room id) -> session ids on this node
    pub room_sessions: DashMap<(Uuid, String), HashSet<Uuid>>,
    /// oauth discovery/JWKS cache
    pub oauth_cache: DashMap<String, (Value, std::time::Instant)>,
    /// last presence snapshot sent per (app, room) — for patch-presence diffs
    pub room_snapshots: DashMap<(Uuid, String), Value>,
    /// live stream subscribers on this node: (app, stream) -> (session, subscribe event id)
    pub stream_subs: DashMap<(Uuid, Uuid), HashSet<(Uuid, String)>>,
}

impl AppState {
    pub fn new(cfg: Config, pool: PgPool) -> Arc<Self> {
        Arc::new(AppState {
            cfg,
            pool,
            node_id: Uuid::new_v4(),
            sessions: DashMap::new(),
            app_sessions: DashMap::new(),
            room_sessions: DashMap::new(),
            oauth_cache: DashMap::new(),
            room_snapshots: DashMap::new(),
            stream_subs: DashMap::new(),
        })
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
        self.app_sessions.iter_mut().for_each(|mut e| {
            e.value_mut().remove(&session_id);
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
