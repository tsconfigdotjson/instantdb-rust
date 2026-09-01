//! Load-test harness for the sync layer (issue #11).
//!
//!   instant-loadtest --apps <id[,id...]> [options]
//!
//! Opens N websocket clients (spread across the given apps and server urls),
//! registers M queries each, runs W closed-loop writers for D seconds, and
//! reports:
//!   - connect/init latency + rate
//!   - add-query latency
//!   - transact latency + throughput
//!   - refresh fan-out latency (transact sent -> refresh-ok observed by each
//!     subscriber), delivery completeness
//!   - server-side counters from /metrics (RSS, CPU, pool, topic skips, ...)
//!
//! Options (defaults in brackets):
//!   --url ws://h:p[,ws://h:p]  server base url(s); clients and writers are
//!                             spread across them [ws://localhost:8888]
//!   --clients N               websocket clients [200]
//!   --queries M               queries per client, 1..4 [3]
//!   --writers W               concurrent writers [4]
//!   --inflight K              transacts in flight per writer [1]
//!   --think-ms MS             pause per writer between transacts [0]
//!   --duration S              write phase seconds [15]
//!   --seed E                  seeded todos/notes per app [50]
//!   --connect-concurrency C   clients connecting at once [100]
//!   --out FILE                write the JSON report here
//!   --smoke                   assert delivery/latency thresholds (CI)
//!   --max-p99-ms MS           fail if fan-out p99 exceeds this (with --smoke)
//!   --cleanup                 delete seeded entities + attrs at the end
//!
//! Query mix per client (first M of): counters, todos, notes, todos-by-owner.
//! Writers bump their own counter and rewrite one todo per tx; notes are never
//! written, so with topic narrowing the `notes` query is never recomputed.
//!
//! A client costs a few tens of KB here (vs ~200 KB for a node ws client), so
//! a 2-vCPU / 1 GB load generator drives 10k+ connections.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Semaphore};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use uuid::Uuid;

const CORE_VERSION: &str = "v0.22.80"; // skip-attrs + batched frames, like a current SDK

// ---------------------------------------------------------------------------
// args

struct Args {
    map: HashMap<String, String>,
}

impl Args {
    fn parse() -> Args {
        let argv: Vec<String> = std::env::args().skip(1).collect();
        let mut map = HashMap::new();
        let mut i = 0;
        while i < argv.len() {
            let a = &argv[i];
            if let Some(key) = a.strip_prefix("--") {
                match argv.get(i + 1) {
                    Some(next) if !next.starts_with("--") => {
                        map.insert(key.to_string(), next.clone());
                        i += 2;
                    }
                    _ => {
                        map.insert(key.to_string(), "true".into());
                        i += 1;
                    }
                }
            } else {
                i += 1;
            }
        }
        Args { map }
    }
    fn num<T: std::str::FromStr>(&self, k: &str, d: T) -> T {
        self.map.get(k).and_then(|v| v.parse().ok()).unwrap_or(d)
    }
    fn flag(&self, k: &str) -> bool {
        self.map.contains_key(k)
    }
    fn str(&self, k: &str) -> Option<&str> {
        self.map.get(k).map(|s| s.as_str())
    }
}

struct Config {
    urls: Vec<String>,
    apps: Vec<Uuid>,
    clients: usize,
    queries: usize,
    writers: usize,
    inflight: usize,
    think_ms: u64,
    duration: u64,
    seed: usize,
    connect_concurrency: usize,
    out: Option<String>,
    smoke: bool,
    max_p99_ms: Option<f64>,
    cleanup: bool,
}

impl Config {
    fn probe() -> Config {
        Config {
            urls: vec![],
            apps: vec![],
            clients: 0,
            queries: 0,
            writers: 0,
            inflight: 1,
            think_ms: 0,
            duration: 0,
            seed: 0,
            connect_concurrency: 1,
            out: None,
            smoke: false,
            max_p99_ms: None,
            cleanup: false,
        }
    }
}

fn die(msg: &str) -> ! {
    eprintln!("{msg}");
    std::process::exit(2)
}

// ---------------------------------------------------------------------------
// histogram: fixed 0.25ms bins up to 30s, overflow bin after (lock-free adds)

struct Hist {
    bin_ms: f64,
    bins: Vec<AtomicU32>,
    count: AtomicU64,
    sum_us: AtomicU64,
    max_us: AtomicU64,
}

impl Hist {
    fn new() -> Hist {
        let bin_ms = 0.25;
        let n = (30000.0 / bin_ms) as usize + 1;
        Hist {
            bin_ms,
            bins: (0..n).map(|_| AtomicU32::new(0)).collect(),
            count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
        }
    }
    fn add(&self, ms: f64) {
        let i = ((ms / self.bin_ms) as usize).min(self.bins.len() - 1);
        self.bins[i].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        let us = (ms * 1000.0) as u64;
        self.sum_us.fetch_add(us, Ordering::Relaxed);
        self.max_us.fetch_max(us, Ordering::Relaxed);
    }
    fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
    fn pct(&self, p: f64) -> Option<f64> {
        let count = self.count();
        if count == 0 {
            return None;
        }
        let target = ((p / 100.0) * count as f64).ceil() as u64;
        let mut acc = 0u64;
        for (i, b) in self.bins.iter().enumerate() {
            acc += b.load(Ordering::Relaxed) as u64;
            if acc >= target {
                return Some((i + 1) as f64 * self.bin_ms);
            }
        }
        Some(self.max_us.load(Ordering::Relaxed) as f64 / 1000.0)
    }
    fn summary(&self) -> Value {
        let count = self.count();
        if count == 0 {
            return json!({"count": 0});
        }
        json!({
            "count": count,
            "mean": round(self.sum_us.load(Ordering::Relaxed) as f64 / 1000.0 / count as f64),
            "p50": round(self.pct(50.0).unwrap()),
            "p95": round(self.pct(95.0).unwrap()),
            "p99": round(self.pct(99.0).unwrap()),
            "max": round(self.max_us.load(Ordering::Relaxed) as f64 / 1000.0),
        })
    }
}

fn round(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

// ---------------------------------------------------------------------------
// shared state

struct Schema {
    todos_id: Uuid,
    todos_title: Uuid,
    todos_done: Uuid,
    todos_owner: Uuid,
    notes_id: Uuid,
    notes_body: Uuid,
    counters_id: Uuid,
    counters_seq: Uuid,
    counters_writer: Uuid,
}

impl Schema {
    fn new() -> Schema {
        Schema {
            todos_id: Uuid::new_v4(),
            todos_title: Uuid::new_v4(),
            todos_done: Uuid::new_v4(),
            todos_owner: Uuid::new_v4(),
            notes_id: Uuid::new_v4(),
            notes_body: Uuid::new_v4(),
            counters_id: Uuid::new_v4(),
            counters_seq: Uuid::new_v4(),
            counters_writer: Uuid::new_v4(),
        }
    }
    fn slots(&mut self) -> Vec<(&'static str, &'static str, &mut Uuid)> {
        vec![
            ("todos", "id", &mut self.todos_id),
            ("todos", "title", &mut self.todos_title),
            ("todos", "done", &mut self.todos_done),
            ("todos", "owner", &mut self.todos_owner),
            ("notes", "id", &mut self.notes_id),
            ("notes", "body", &mut self.notes_body),
            ("counters", "id", &mut self.counters_id),
            ("counters", "seq", &mut self.counters_seq),
            ("counters", "writer", &mut self.counters_writer),
        ]
    }
    fn attrs(&self) -> Vec<(&'static str, &'static str, Uuid)> {
        vec![
            ("todos", "id", self.todos_id),
            ("todos", "title", self.todos_title),
            ("todos", "done", self.todos_done),
            ("todos", "owner", self.todos_owner),
            ("notes", "id", self.notes_id),
            ("notes", "body", self.notes_body),
            ("counters", "id", self.counters_id),
            ("counters", "seq", self.counters_seq),
            ("counters", "writer", self.counters_writer),
        ]
    }
}

struct App {
    id: Uuid,
    schema: Schema,
    todos: Vec<Uuid>,
    notes: Vec<Uuid>,
}

struct WriterInfo {
    app: usize,
    counter: Uuid,
    /// send instant of seq s at index s-1
    send_times: Mutex<Vec<Instant>>,
}

struct Shared {
    cfg: Config,
    apps: Vec<App>,
    writers: Vec<WriterInfo>,
    /// counters.seq attr ids across apps
    seq_attrs: HashSet<Uuid>,
    /// counter eid -> writer index
    writer_by_counter: HashMap<Uuid, usize>,
    fanout: Hist,
    connect_hist: Hist,
    add_query_hist: Hist,
    tx_hist: Hist,
    refresh_ok: AtomicU64,
    bytes: AtomicU64,
    errors: AtomicU64,
    tx_ok: AtomicU64,
    tx_err: AtomicU64,
    shutdown: AtomicBool,
}

impl Shared {
    fn new(cfg: Config, apps: Vec<App>, writers: Vec<WriterInfo>) -> Shared {
        Shared {
            cfg,
            apps,
            writers,
            seq_attrs: HashSet::new(),
            writer_by_counter: HashMap::new(),
            fanout: Hist::new(),
            connect_hist: Hist::new(),
            add_query_hist: Hist::new(),
            tx_hist: Hist::new(),
            refresh_ok: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            tx_ok: AtomicU64::new(0),
            tx_err: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
        }
    }

    /// Minimal state for a probe client (setup, before schemas are final).
    fn empty(app_ids: Vec<Uuid>) -> Shared {
        let apps = app_ids
            .into_iter()
            .map(|id| App {
                id,
                schema: Schema::new(),
                todos: vec![],
                notes: vec![],
            })
            .collect();
        Shared::new(Config::probe(), apps, vec![])
    }

    /// Derived lookups, once every app's schema ids are final.
    fn finalize(&mut self) {
        self.seq_attrs = self.apps.iter().map(|a| a.schema.counters_seq).collect();
        self.writer_by_counter = self
            .writers
            .iter()
            .enumerate()
            .map(|(i, w)| (w.counter, i))
            .collect();
    }

    fn schema(&self, app: usize) -> &Schema {
        &self.apps[app].schema
    }
}

// ---------------------------------------------------------------------------
// client

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct Client {
    name: String,
    app: usize,
    ws: Ws,
    shared: Arc<Shared>,
    /// last seq observed per writer (index = writer)
    last_seen: Vec<u64>,
    /// published copy of last_seen for the drain check
    seen: Arc<Vec<AtomicU64>>,
}

impl Client {
    async fn connect(
        shared: Arc<Shared>,
        name: String,
        app: usize,
        url: &str,
    ) -> Result<Client, String> {
        let app_id = shared.apps[app].id;
        // we barely write; don't preallocate a 128 KB write buffer per socket
        let wcfg = WebSocketConfig {
            write_buffer_size: 0,
            ..Default::default()
        };
        let (ws, _) = tokio_tungstenite::connect_async_with_config(
            format!("{url}/runtime/session?app_id={app_id}"),
            Some(wcfg),
            false,
        )
        .await
        .map_err(|e| format!("ws connect ({name}): {e}"))?;
        let n = shared.writers.len();
        Ok(Client {
            name,
            app,
            ws,
            shared,
            last_seen: vec![0; n],
            seen: Arc::new((0..n).map(|_| AtomicU64::new(0)).collect()),
        })
    }

    async fn send(&mut self, mut msg: Value) -> Result<String, String> {
        let id = Uuid::new_v4().to_string();
        msg["client-event-id"] = Value::String(id.clone());
        self.ws
            .send(Message::Text(msg.to_string()))
            .await
            .map_err(|e| format!("send ({}): {e}", self.name))?;
        Ok(id)
    }

    /// Next batch of server messages (a frame may carry a JSON array).
    async fn recv(&mut self) -> Result<Vec<Value>, String> {
        loop {
            match self.ws.next().await {
                Some(Ok(Message::Text(t))) => {
                    self.shared
                        .bytes
                        .fetch_add(t.len() as u64, Ordering::Relaxed);
                    let v: Value = match serde_json::from_str(&t) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    return Ok(match v {
                        Value::Array(a) => a,
                        v => vec![v],
                    });
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = self.ws.send(Message::Pong(p)).await;
                }
                Some(Ok(Message::Close(_))) | None => {
                    return Err(format!("closed ({})", self.name));
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(format!("recv ({}): {e}", self.name)),
            }
        }
    }

    fn observe(&mut self, m: &Value) {
        match m.get("op").and_then(|v| v.as_str()) {
            Some("refresh-ok") => {
                self.shared.refresh_ok.fetch_add(1, Ordering::Relaxed);
                self.observe_refresh(m);
            }
            Some("error") => {
                let n = self.shared.errors.fetch_add(1, Ordering::Relaxed) + 1;
                if n <= 5 {
                    eprintln!(
                        "[{}] error: {} {}",
                        self.name,
                        m.get("type").and_then(|v| v.as_str()).unwrap_or(""),
                        m.get("message").and_then(|v| v.as_str()).unwrap_or("")
                    );
                }
            }
            _ => {}
        }
    }

    /// Fan-out latency: each writer's counter carries a seq; every seq the
    /// client has not yet seen is attributed to its own send time.
    fn observe_refresh(&mut self, m: &Value) {
        let now = Instant::now();
        let Some(comps) = m.get("computations").and_then(|v| v.as_array()) else {
            return;
        };
        for c in comps {
            let rows = c
                .get("instaql-result")
                .and_then(|v| v.get(0))
                .and_then(|v| v.get("data"))
                .and_then(|v| v.get("datalog-result"))
                .and_then(|v| v.get("join-rows"))
                .and_then(|v| v.get(0))
                .and_then(|v| v.as_array());
            let Some(rows) = rows else { continue };
            for row in rows {
                let Some(row) = row.as_array() else { continue };
                let (Some(e), Some(a), Some(v)) = (
                    row.first()
                        .and_then(|v| v.as_str())
                        .and_then(|s| Uuid::parse_str(s).ok()),
                    row.get(1)
                        .and_then(|v| v.as_str())
                        .and_then(|s| Uuid::parse_str(s).ok()),
                    row.get(2).and_then(|v| v.as_u64()),
                ) else {
                    continue;
                };
                if !self.shared.seq_attrs.contains(&a) {
                    continue;
                }
                let Some(&w) = self.shared.writer_by_counter.get(&e) else {
                    continue;
                };
                let prev = self.last_seen[w];
                if v <= prev {
                    continue;
                }
                {
                    let times = self.shared.writers[w].send_times.lock().unwrap();
                    for s in prev + 1..=v {
                        if let Some(t0) = times.get((s - 1) as usize) {
                            self.shared
                                .fanout
                                .add(now.duration_since(*t0).as_secs_f64() * 1000.0);
                        }
                    }
                }
                self.last_seen[w] = v;
                self.seen[w].store(v, Ordering::Relaxed);
            }
        }
    }

    /// Send a request and wait for its reply (ok op or error), observing
    /// everything else that arrives meanwhile.
    async fn request(
        &mut self,
        msg: Value,
        ok_op: &str,
        timeout: Duration,
    ) -> Result<Value, String> {
        let id = self.send(msg).await?;
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(format!("timeout ({})", self.name));
            }
            let msgs = tokio::time::timeout(remaining, self.recv())
                .await
                .map_err(|_| format!("timeout ({})", self.name))??;
            let mut reply = None;
            for m in msgs {
                self.observe(&m);
                let op = m.get("op").and_then(|v| v.as_str()).unwrap_or("");
                if m.get("client-event-id").and_then(|v| v.as_str()) == Some(id.as_str())
                    && (op == ok_op || op == "error")
                {
                    reply = Some(m);
                }
            }
            if let Some(r) = reply {
                return Ok(r);
            }
        }
    }

    /// init; returns the app's current attrs from init-ok.
    async fn init(&mut self) -> Result<Value, String> {
        let app_id = self.shared.apps[self.app].id;
        let mut r = self
            .request(
                json!({"op": "init", "app-id": app_id, "versions": {"@instantdb/core": CORE_VERSION}}),
                "init-ok",
                Duration::from_secs(30),
            )
            .await?;
        if r["op"] == "error" {
            return Err(format!("init failed: {}", r["message"]));
        }
        Ok(r["attrs"].take())
    }

    async fn transact(&mut self, steps: Value, timeout: Duration) -> Result<Value, String> {
        self.request(
            json!({"op": "transact", "tx-steps": steps}),
            "transact-ok",
            timeout,
        )
        .await
    }

    /// Read until shutdown, observing refreshes.
    async fn read_loop(&mut self) {
        loop {
            let msgs = match tokio::time::timeout(Duration::from_millis(500), self.recv()).await {
                Ok(Ok(m)) => m,
                Ok(Err(_)) => return,
                Err(_) => {
                    if self.shared.shutdown.load(Ordering::Relaxed) {
                        let _ = self.ws.close(None).await;
                        return;
                    }
                    continue;
                }
            };
            for m in &msgs {
                self.observe(m);
            }
            if self.shared.shutdown.load(Ordering::Relaxed) {
                let _ = self.ws.close(None).await;
                return;
            }
        }
    }
}

fn attr_step(etype: &str, label: &str, id: Uuid) -> Value {
    json!([
        "add-attr",
        {
            "id": id,
            "forward-identity": [Uuid::new_v4(), etype, label],
            "value-type": "blob",
            "cardinality": "one",
            "unique?": label == "id",
            "index?": false,
            "isUnsynced": true,
        }
    ])
}

// ---------------------------------------------------------------------------
// setup / cleanup (per app)

/// Attr ids the app already has, by (etype, label) — a previous run that was
/// killed before its cleanup leaves its attrs behind, and the server treats
/// re-adding an existing forward name under a new id as a no-op.
fn existing_attrs(attrs: &Value) -> HashMap<(String, String), Uuid> {
    let mut out = HashMap::new();
    for a in attrs.as_array().into_iter().flatten() {
        let fwd = a.get("forward-identity").and_then(|v| v.as_array());
        let (Some(fwd), Some(id)) = (fwd, a.get("id").and_then(|v| v.as_str())) else {
            continue;
        };
        if let (Some(etype), Some(label), Ok(id)) = (
            fwd.get(1).and_then(|v| v.as_str()),
            fwd.get(2).and_then(|v| v.as_str()),
            Uuid::parse_str(id),
        ) {
            out.insert((etype.to_string(), label.to_string()), id);
        }
    }
    out
}

/// Choose the schema ids for one app: reuse whatever the app already has.
fn resolve_schema(schema: &mut Schema, existing: &HashMap<(String, String), Uuid>) -> usize {
    let mut reused = 0;
    for (etype, label, slot) in schema.slots() {
        if let Some(id) = existing.get(&(etype.to_string(), label.to_string())) {
            *slot = *id;
            reused += 1;
        }
    }
    reused
}

async fn setup_app(shared: &mut Shared, app: usize, url: &str) -> Result<Client, String> {
    let app_id = shared.apps[app].id;
    // a throwaway client: the shared state is still being built
    let probe = Arc::new(Shared::empty(vec![app_id]));
    let mut c = Client::connect(probe, format!("setup-{app}"), 0, url).await?;
    let existing = existing_attrs(&c.init().await?);
    let reused = resolve_schema(&mut shared.apps[app].schema, &existing);
    if reused > 0 {
        eprintln!("app {app_id}: reusing {reused} existing attrs (leftover from an earlier run?)");
    }
    let a = &shared.apps[app];
    let s = &a.schema;
    let mut steps = vec![];
    for (etype, label, id) in s.attrs() {
        if !existing.contains_key(&(etype.to_string(), label.to_string())) {
            steps.push(attr_step(etype, label, id));
        }
    }
    for (i, t) in a.todos.iter().enumerate() {
        steps.push(json!(["add-triple", t, s.todos_id, t]));
        steps.push(json!(["add-triple", t, s.todos_title, format!("todo {i}")]));
        steps.push(json!(["add-triple", t, s.todos_done, i % 2 == 0]));
        steps.push(json!([
            "add-triple",
            t,
            s.todos_owner,
            format!("user-{}", i % 10)
        ]));
    }
    for (i, n) in a.notes.iter().enumerate() {
        steps.push(json!(["add-triple", n, s.notes_id, n]));
        steps.push(json!(["add-triple", n, s.notes_body, format!("note {i}")]));
    }
    let r = c
        .transact(Value::Array(steps), Duration::from_secs(60))
        .await?;
    if r["op"] == "error" {
        return Err(format!("seed transact failed: {}", r["message"]));
    }
    Ok(c)
}

async fn cleanup_app(shared: &Arc<Shared>, app: usize, c: &mut Client) {
    let a = &shared.apps[app];
    let mut steps = vec![];
    for t in &a.todos {
        steps.push(json!(["delete-entity", t, "todos"]));
    }
    for n in &a.notes {
        steps.push(json!(["delete-entity", n, "notes"]));
    }
    for w in shared.writers.iter().filter(|w| w.app == app) {
        steps.push(json!(["delete-entity", w.counter, "counters"]));
    }
    for (_, _, id) in a.schema.attrs() {
        steps.push(json!(["delete-attr", id]));
    }
    match c
        .transact(Value::Array(steps), Duration::from_secs(60))
        .await
    {
        Ok(r) if r["op"] == "error" => eprintln!("cleanup failed for {}: {}", a.id, r["message"]),
        Err(e) => eprintln!("cleanup failed for {}: {e}", a.id),
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// writers

async fn writer_start(shared: &Arc<Shared>, w: usize, url: &str) -> Result<Client, String> {
    let info = &shared.writers[w];
    let mut c = Client::connect(shared.clone(), format!("writer-{w}"), info.app, url).await?;
    c.init().await?;
    let s = shared.schema(info.app);
    let r = c
        .transact(
            json!([
                ["add-triple", info.counter, s.counters_id, info.counter],
                ["add-triple", info.counter, s.counters_writer, w],
                ["add-triple", info.counter, s.counters_seq, 0],
            ]),
            Duration::from_secs(30),
        )
        .await?;
    if r["op"] == "error" {
        return Err(format!("writer counter create failed: {}", r["message"]));
    }
    Ok(c)
}

/// Closed loop with `inflight` transacts outstanding on one socket: replies
/// are matched by client-event-id, so one reader serves all in-flight sends.
async fn writer_run(shared: Arc<Shared>, w: usize, mut c: Client, until: Instant) {
    let info = &shared.writers[w];
    let s = shared.schema(info.app);
    let todos = &shared.apps[info.app].todos;
    let mut inflight: HashMap<String, (Instant, u64)> = HashMap::new();
    let send_one = |c_send_times: &Mutex<Vec<Instant>>| -> (u64, Instant, Value) {
        let mut times = c_send_times.lock().unwrap();
        let t0 = Instant::now();
        times.push(t0);
        let seq = times.len() as u64;
        let todo = todos[(seq as usize) % todos.len()];
        let steps = json!([
            ["add-triple", info.counter, s.counters_seq, seq],
            ["add-triple", todo, s.todos_title, format!("todo {seq}")],
        ]);
        (seq, t0, steps)
    };
    for _ in 0..shared.cfg.inflight {
        let (seq, t0, steps) = send_one(&info.send_times);
        match c.send(json!({"op": "transact", "tx-steps": steps})).await {
            Ok(id) => {
                inflight.insert(id, (t0, seq));
            }
            Err(e) => {
                eprintln!("{e}");
                return;
            }
        }
    }
    while !inflight.is_empty() {
        let msgs = match tokio::time::timeout(Duration::from_secs(60), c.recv()).await {
            Ok(Ok(m)) => m,
            Ok(Err(e)) => {
                eprintln!("{e}");
                return;
            }
            Err(_) => {
                eprintln!("timeout (writer-{w})");
                return;
            }
        };
        for m in msgs {
            c.observe(&m);
            let op = m.get("op").and_then(|v| v.as_str()).unwrap_or("");
            let Some(id) = m.get("client-event-id").and_then(|v| v.as_str()) else {
                continue;
            };
            if !(op == "transact-ok" || op == "error") {
                continue;
            }
            let Some((t0, _)) = inflight.remove(id) else {
                continue;
            };
            shared.tx_hist.add(t0.elapsed().as_secs_f64() * 1000.0);
            if op == "error" {
                shared.tx_err.fetch_add(1, Ordering::Relaxed);
            } else {
                shared.tx_ok.fetch_add(1, Ordering::Relaxed);
            }
            if Instant::now() < until {
                if shared.cfg.think_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(shared.cfg.think_ms)).await;
                }
                let (seq, t0, steps) = send_one(&info.send_times);
                match c.send(json!({"op": "transact", "tx-steps": steps})).await {
                    Ok(id) => {
                        inflight.insert(id, (t0, seq));
                    }
                    Err(e) => {
                        eprintln!("{e}");
                        return;
                    }
                }
            }
        }
    }
    let _ = c.ws.close(None).await;
}

// ---------------------------------------------------------------------------
// metrics

async fn scrape(url: &str) -> Option<HashMap<String, f64>> {
    let http = url.replacen("ws", "http", 1);
    let text = reqwest::get(format!("{http}/metrics"))
        .await
        .ok()?
        .error_for_status()
        .ok()?
        .text()
        .await
        .ok()?;
    let mut out = HashMap::new();
    for line in text.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some(sp) = line.rfind(' ') else { continue };
        let key = &line[..sp];
        let Ok(val) = line[sp + 1..].parse::<f64>() else {
            continue;
        };
        if !key.contains('{') || key.contains("_bucket") {
            out.insert(key.to_string(), val);
        }
    }
    Some(out)
}

/// One scrape per server url.
async fn scrape_all(urls: &[String]) -> Option<Vec<HashMap<String, f64>>> {
    let mut all = vec![];
    for u in urls {
        all.push(scrape(u).await?);
    }
    Some(all)
}

fn sum(m: &[HashMap<String, f64>], k: &str) -> f64 {
    m.iter().map(|x| x.get(k).copied().unwrap_or(0.0)).sum()
}

fn server_delta(
    a: &Option<Vec<HashMap<String, f64>>>,
    b: &Option<Vec<HashMap<String, f64>>>,
    secs: f64,
    before: &Option<Vec<HashMap<String, f64>>>,
) -> Value {
    let (Some(a), Some(b)) = (a, b) else {
        return Value::Null;
    };
    let d = |k: &str| sum(b, k) - sum(a, k);
    let di = |k: &str| (sum(b, k) - sum(a, k)) as i64;
    let per_node = |m: &[HashMap<String, f64>], k: &str| -> Vec<f64> {
        m.iter()
            .map(|x| round(x.get(k).copied().unwrap_or(0.0) / 1048576.0))
            .collect()
    };
    let mut out = json!({
        "rss_mb": per_node(b, "process_resident_memory_bytes"),
        "rss_mb_before_clients": before.as_ref().map(|m| per_node(m, "process_resident_memory_bytes")),
        "cpu_seconds": round(d("process_cpu_seconds")),
        "cpu_cores_avg": round(d("process_cpu_seconds") / secs),
        "sessions": sum(b, "instant_sessions") as i64,
        "pg_pool_size": sum(b, "instant_pg_pool_size") as i64,
        "pg_pool_max": sum(b, "instant_pg_pool_max") as i64,
        "refresh_batches": di("instant_refresh_batches_total"),
        "refresh_txs": di("instant_refresh_txs_total"),
        "queries_skipped_by_topic": di("instant_refresh_queries_skipped_total"),
        "queries_computed": di("instant_refresh_queries_computed_total"),
        "queries_deduped": di("instant_refresh_queries_deduped_total"),
        "queries_changed": di("instant_refresh_queries_changed_total"),
        "attr_cache_hits": di("instant_attr_cache_hits_total"),
        "attr_cache_misses": di("instant_attr_cache_misses_total"),
        "sessions_overflowed": di("instant_ws_sessions_overflowed_total"),
        "nodes": b.len(),
    });
    for (name, key) in [
        ("notify_lag_ms", "instant_notify_lag_seconds"),
        ("refresh_batch_ms", "instant_refresh_batch_seconds"),
        ("query_ms", "instant_query_seconds"),
        ("transact_ms", "instant_transact_seconds"),
    ] {
        let c = d(&format!("{key}_count"));
        out[format!("{name}_mean")] = if c > 0.0 {
            json!(round(1000.0 * d(&format!("{key}_sum")) / c))
        } else {
            Value::Null
        };
    }
    out
}

// ---------------------------------------------------------------------------
// report

fn fmt_lat(l: &Value) -> String {
    if l["count"].as_u64().unwrap_or(0) == 0 {
        return "n/a".into();
    }
    format!(
        "p50 {} / p95 {} / p99 {} / max {}",
        l["p50"], l["p95"], l["p99"], l["max"]
    )
}

fn fmt_list(v: &Value) -> String {
    match v.as_array() {
        Some(a) => a
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(" + "),
        None => "n/a".into(),
    }
}

fn markdown(r: &Value) -> String {
    let s = &r["server"];
    let cfg = &r["config"];
    let has = |k: &str| !s[k].is_null();
    let rows: Vec<(&str, String)> = vec![
        (
            "clients (ok/failed)",
            format!("{} / {}", r["connect"]["ok"], r["connect"]["failed"]),
        ),
        (
            "connect+init rate",
            format!(
                "{}/s ({}s)",
                r["connect"]["per_sec"], r["connect"]["seconds"]
            ),
        ),
        (
            "connect+init latency ms",
            fmt_lat(&r["connect"]["latency_ms"]),
        ),
        (
            "add-query latency ms",
            fmt_lat(&r["add_query"]["latency_ms"]),
        ),
        (
            "tx throughput",
            format!(
                "{} tx/s ({} ok, {} err)",
                r["transact"]["per_sec"], r["transact"]["ok"], r["transact"]["errors"]
            ),
        ),
        ("tx latency ms", fmt_lat(&r["transact"]["latency_ms"])),
        ("fan-out latency ms", fmt_lat(&r["fanout"]["latency_ms"])),
        (
            "fan-out delivered",
            format!(
                "{}% ({}/{}), {} clients caught up",
                r["fanout"]["delivered_pct"],
                r["fanout"]["samples"],
                r["fanout"]["expected_samples"],
                r["fanout"]["clients_caught_up"]
            ),
        ),
        (
            "refresh-ok received",
            format!(
                "{} ({} MB)",
                r["fanout"]["refresh_ok_received"],
                round(r["fanout"]["bytes_received"].as_f64().unwrap_or(0.0) / 1048576.0)
            ),
        ),
        (
            "server RSS",
            if has("rss_mb") {
                format!(
                    "{} MB (before clients: {} MB)",
                    fmt_list(&s["rss_mb"]),
                    fmt_list(&s["rss_mb_before_clients"])
                )
            } else {
                "n/a (no /metrics)".into()
            },
        ),
        (
            "server CPU",
            if has("cpu_seconds") {
                format!(
                    "{}s = {} cores avg during writes",
                    s["cpu_seconds"], s["cpu_cores_avg"]
                )
            } else {
                "n/a".into()
            },
        ),
        (
            "refresh batches / txs",
            if has("refresh_batches") {
                format!("{} / {}", s["refresh_batches"], s["refresh_txs"])
            } else {
                "n/a".into()
            },
        ),
        (
            "queries skipped / computed / deduped / changed",
            if has("queries_computed") {
                format!(
                    "{} / {} / {} / {}",
                    s["queries_skipped_by_topic"],
                    s["queries_computed"],
                    s["queries_deduped"],
                    s["queries_changed"]
                )
            } else {
                "n/a".into()
            },
        ),
        (
            "attr cache hits / misses",
            if has("attr_cache_hits") {
                format!("{} / {}", s["attr_cache_hits"], s["attr_cache_misses"])
            } else {
                "n/a".into()
            },
        ),
        (
            "server means ms (notify lag / batch / query / transact)",
            if has("query_ms_mean") {
                format!(
                    "{} / {} / {} / {}",
                    s["notify_lag_ms_mean"],
                    s["refresh_batch_ms_mean"],
                    s["query_ms_mean"],
                    s["transact_ms_mean"]
                )
            } else {
                "n/a".into()
            },
        ),
        (
            "pg pool",
            if has("pg_pool_size") {
                format!("{}/{}", s["pg_pool_size"], s["pg_pool_max"])
            } else {
                "n/a".into()
            },
        ),
    ];
    let mut md = format!(
        "### loadtest: {} clients × {} queries, {}×{} writers, {}s, {} app(s), {} node(s)\n\n| metric | value |\n|---|---|\n",
        cfg["clients"], cfg["queries"], cfg["writers"], cfg["inflight"], cfg["duration"], cfg["apps"], cfg["nodes"]
    );
    for (k, v) in rows {
        md += &format!("| {k} | {v} |\n");
    }
    md
}

// ---------------------------------------------------------------------------
// main

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let urls: Vec<String> = args
        .str("url")
        .unwrap_or("ws://localhost:8888")
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.trim_end_matches('/').to_string())
        .collect();
    let apps: Vec<Uuid> = args
        .str("apps")
        .unwrap_or("")
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| Uuid::parse_str(s).unwrap_or_else(|_| die(&format!("bad app id: {s}"))))
        .collect();
    if apps.is_empty() {
        die("usage: instant-loadtest --apps <app-id[,app-id...]> [options]");
    }
    let cfg = Config {
        urls,
        apps,
        clients: args.num("clients", 200),
        queries: args.num("queries", 3usize).clamp(1, 4),
        writers: args.num("writers", 4),
        inflight: args.num("inflight", 1usize).max(1),
        think_ms: args.num("think-ms", 0),
        duration: args.num("duration", 15),
        seed: args.num("seed", 50),
        connect_concurrency: args.num("connect-concurrency", 100usize).max(1),
        out: args.str("out").map(String::from),
        smoke: args.flag("smoke"),
        max_p99_ms: args.str("max-p99-ms").and_then(|v| v.parse().ok()),
        cleanup: args.flag("cleanup"),
    };
    let t0 = Instant::now();
    println!(
        "loadtest: {} clients × {} queries, {} writers × {} in flight, {}s, {} app(s), server {}",
        cfg.clients,
        cfg.queries,
        cfg.writers,
        cfg.inflight,
        cfg.duration,
        cfg.apps.len(),
        cfg.urls.join(",")
    );

    let apps: Vec<App> = cfg
        .apps
        .iter()
        .map(|id| App {
            id: *id,
            schema: Schema::new(),
            todos: (0..cfg.seed).map(|_| Uuid::new_v4()).collect(),
            notes: (0..cfg.seed).map(|_| Uuid::new_v4()).collect(),
        })
        .collect();
    let writers: Vec<WriterInfo> = (0..cfg.writers)
        .map(|i| WriterInfo {
            app: i % apps.len(),
            counter: Uuid::new_v4(),
            send_times: Mutex::new(Vec::new()),
        })
        .collect();
    let mut shared = Shared::new(cfg, apps, writers);
    let n_apps = shared.apps.len();
    let n_urls = shared.cfg.urls.len();

    // seed (may rebind schema ids to attrs the app already has)
    let mut setup_clients = vec![];
    for app in 0..n_apps {
        let url = shared.cfg.urls[(app / n_apps) % n_urls].clone();
        match setup_app(&mut shared, app, &url).await {
            Ok(c) => setup_clients.push(c),
            Err(e) => die(&format!(
                "setup failed for app {}: {e}",
                shared.apps[app].id
            )),
        }
    }
    shared.finalize();
    let shared = Arc::new(shared);
    let cfg = &shared.cfg;
    let n_apps = shared.apps.len();
    let n_urls = cfg.urls.len();
    let url_for = |i: usize| cfg.urls[(i / n_apps) % n_urls].as_str();
    println!(
        "seeded {} todos + {} notes per app in {:.1}s",
        cfg.seed,
        cfg.seed,
        t0.elapsed().as_secs_f64()
    );

    // writers
    let mut writer_clients = vec![];
    for w in 0..cfg.writers {
        match writer_start(&shared, w, url_for(w)).await {
            Ok(c) => writer_clients.push(c),
            Err(e) => die(&format!("writer {w}: {e}")),
        }
    }

    // clients
    let metrics_before = scrape_all(&cfg.urls).await;
    let connect_start = Instant::now();
    let sem = Arc::new(Semaphore::new(cfg.connect_concurrency));
    let (done_tx, mut done_rx) =
        mpsc::unbounded_channel::<Result<(usize, Arc<Vec<AtomicU64>>), String>>();
    let mut tasks = Vec::with_capacity(cfg.clients);
    for i in 0..cfg.clients {
        let shared = shared.clone();
        let sem = sem.clone();
        let done_tx = done_tx.clone();
        let url = url_for(i).to_string();
        tasks.push(tokio::spawn(async move {
            let app = i % shared.apps.len();
            let setup = async {
                let _permit = sem.acquire().await.unwrap();
                let s = Instant::now();
                let mut c = Client::connect(shared.clone(), format!("c{i}"), app, &url).await?;
                c.init().await?;
                shared.connect_hist.add(s.elapsed().as_secs_f64() * 1000.0);
                let mix = [
                    json!({"counters": {}}),
                    json!({"todos": {}}),
                    json!({"notes": {}}),
                    json!({"todos": {"$": {"where": {"owner": format!("user-{}", i % 10)}}}}),
                ];
                for q in mix.iter().take(shared.cfg.queries) {
                    let qs = Instant::now();
                    let r = c
                        .request(
                            json!({"op": "add-query", "q": q}),
                            "add-query-ok",
                            Duration::from_secs(30),
                        )
                        .await?;
                    if r["op"] == "error" {
                        return Err(format!("add-query failed: {}", r["message"]));
                    }
                    shared
                        .add_query_hist
                        .add(qs.elapsed().as_secs_f64() * 1000.0);
                }
                Ok::<Client, String>(c)
            };
            match setup.await {
                Ok(mut c) => {
                    let _ = done_tx.send(Ok((app, c.seen.clone())));
                    // main waits for every sender to go away before the
                    // write phase; this task lives on as a reader
                    drop(done_tx);
                    c.read_loop().await;
                }
                Err(e) => {
                    let _ = done_tx.send(Err(e));
                }
            }
        }));
    }
    drop(done_tx);
    let mut connected: Vec<(usize, Arc<Vec<AtomicU64>>)> = vec![];
    let mut connect_failures = 0usize;
    while let Some(r) = done_rx.recv().await {
        match r {
            Ok(c) => connected.push(c),
            Err(e) => {
                connect_failures += 1;
                if connect_failures <= 5 {
                    eprintln!("client failed: {e}");
                }
            }
        }
    }
    let connect_secs = connect_start.elapsed().as_secs_f64();
    println!(
        "connected {}/{} clients in {:.1}s ({} failures)",
        connected.len(),
        cfg.clients,
        connect_secs,
        connect_failures
    );

    // write phase
    let metrics_start = scrape_all(&cfg.urls).await;
    let write_start = Instant::now();
    let until = write_start + Duration::from_secs(cfg.duration);
    let mut wtasks = vec![];
    for (w, c) in writer_clients.into_iter().enumerate() {
        wtasks.push(tokio::spawn(writer_run(shared.clone(), w, c, until)));
    }
    for t in wtasks {
        let _ = t.await;
    }
    let write_secs = write_start.elapsed().as_secs_f64();
    let tx_ok = shared.tx_ok.load(Ordering::Relaxed);
    let tx_err = shared.tx_err.load(Ordering::Relaxed);
    println!("write phase done: {tx_ok} tx ok, {tx_err} errors in {write_secs:.1}s");

    // drain: wait for every client to see every writer's final seq
    let final_seq: Vec<u64> = shared
        .writers
        .iter()
        .map(|w| w.send_times.lock().unwrap().len() as u64)
        .collect();
    let drain_start = Instant::now();
    let mut caught_up = 0usize;
    while drain_start.elapsed() < Duration::from_secs(20) {
        caught_up = connected
            .iter()
            .filter(|(app, seen)| {
                shared
                    .writers
                    .iter()
                    .enumerate()
                    .filter(|(_, w)| w.app == *app)
                    .all(|(i, _)| seen[i].load(Ordering::Relaxed) >= final_seq[i])
            })
            .count();
        if caught_up == connected.len() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let drain_secs = drain_start.elapsed().as_secs_f64();
    let metrics_end = scrape_all(&cfg.urls).await;

    // expected fan-out samples: every client observes every seq of its app's writers
    let mut expected: u64 = 0;
    for (i, w) in shared.writers.iter().enumerate() {
        expected += final_seq[i] * connected.iter().filter(|(app, _)| *app == w.app).count() as u64;
    }
    let samples = shared.fanout.count();
    let delivered_pct = if expected > 0 {
        Some(round(100.0 * samples as f64 / expected as f64))
    } else {
        None
    };
    let fanout_summary = shared.fanout.summary();
    let report = json!({
        "config": {
            "url": cfg.urls.join(","), "nodes": n_urls, "apps": n_apps, "clients": cfg.clients, "queries": cfg.queries,
            "writers": cfg.writers, "inflight": cfg.inflight, "think_ms": cfg.think_ms, "duration": cfg.duration, "seed": cfg.seed,
        },
        "connect": {
            "ok": connected.len(), "failed": connect_failures, "seconds": round(connect_secs),
            "per_sec": round(connected.len() as f64 / connect_secs), "latency_ms": shared.connect_hist.summary(),
        },
        "add_query": {"latency_ms": shared.add_query_hist.summary()},
        "transact": {
            "ok": tx_ok, "errors": tx_err, "seconds": round(write_secs),
            "per_sec": round(tx_ok as f64 / write_secs), "latency_ms": shared.tx_hist.summary(),
        },
        "fanout": {
            "latency_ms": fanout_summary,
            "samples": samples,
            "expected_samples": expected,
            "delivered_pct": delivered_pct,
            "clients_caught_up": caught_up,
            "drain_seconds": round(drain_secs),
            "refresh_ok_received": shared.refresh_ok.load(Ordering::Relaxed),
            "bytes_received": shared.bytes.load(Ordering::Relaxed),
        },
        "errors": shared.errors.load(Ordering::Relaxed),
        "server": server_delta(&metrics_start, &metrics_end, write_secs, &metrics_before),
    });
    println!("\n{}", markdown(&report));
    if let Some(out) = &cfg.out {
        if let Err(e) = std::fs::write(out, serde_json::to_string_pretty(&report).unwrap()) {
            eprintln!("could not write {out}: {e}");
        }
    }

    // cleanup + orderly close
    if cfg.cleanup {
        for (app, c) in setup_clients.iter_mut().enumerate() {
            cleanup_app(&shared, app, c).await;
        }
    }
    shared.shutdown.store(true, Ordering::Relaxed);
    for c in setup_clients.iter_mut() {
        let _ = c.ws.close(None).await;
    }
    let close_all = async {
        for t in tasks {
            let _ = t.await;
        }
    };
    let _ = tokio::time::timeout(Duration::from_secs(5), close_all).await;

    let mut failed = false;
    if cfg.smoke {
        let ws_errors = shared.errors.load(Ordering::Relaxed);
        let mut checks = vec![
            (
                connected.len() == cfg.clients,
                format!(
                    "all {} clients connected ({})",
                    cfg.clients,
                    connected.len()
                ),
            ),
            (
                tx_err == 0 && ws_errors == 0,
                format!("no errors (tx {tx_err}, ws {ws_errors})"),
            ),
            (tx_ok > 0, format!("transactions committed ({tx_ok})")),
            (
                caught_up == connected.len(),
                format!("every client caught up ({caught_up}/{})", connected.len()),
            ),
            (
                delivered_pct.unwrap_or(0.0) >= 99.9,
                format!(
                    "fan-out delivered {}% (>= 99.9)",
                    delivered_pct.unwrap_or(0.0)
                ),
            ),
        ];
        if let Some(max) = cfg.max_p99_ms {
            let p99 = report["fanout"]["latency_ms"]["p99"]
                .as_f64()
                .unwrap_or(f64::INFINITY);
            checks.push((p99 <= max, format!("fan-out p99 {p99}ms <= {max}ms")));
        }
        for (ok, label) in checks {
            println!("{} {label}", if ok { "PASS" } else { "FAIL" });
            if !ok {
                failed = true;
            }
        }
    }
    println!("total {:.1}s", t0.elapsed().as_secs_f64());
    std::process::exit(if failed { 1 } else { 0 });
}
