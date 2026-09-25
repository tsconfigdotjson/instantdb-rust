//! `/metrics` in Prometheus text format (issue #11 observability). Hand-rolled
//! counters/histograms on atomics — no registry crate, no locks on the hot
//! path. Gauges (connections, pool, process stats) are sampled at scrape time.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;
use std::time::Instant;

use crate::state::AppState;

#[derive(Default)]
pub struct Counter(AtomicU64);

impl Counter {
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Latency buckets in seconds: 1ms .. 10s, roughly ×2.5 apart.
const LATENCY_BUCKETS: [f64; 14] = [
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

pub struct Histogram {
    counts: [AtomicU64; LATENCY_BUCKETS.len()],
    count: AtomicU64,
    sum_micros: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        Histogram {
            counts: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_micros: AtomicU64::new(0),
        }
    }
}

impl Histogram {
    pub fn observe(&self, secs: f64) {
        for (i, b) in LATENCY_BUCKETS.iter().enumerate() {
            if secs <= *b {
                self.counts[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_micros
            .fetch_add((secs * 1_000_000.0) as u64, Ordering::Relaxed);
    }

    pub fn observe_since(&self, start: Instant) {
        self.observe(start.elapsed().as_secs_f64());
    }

    fn render(&self, out: &mut String, name: &str, help: &str) {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} histogram");
        for (i, b) in LATENCY_BUCKETS.iter().enumerate() {
            let _ = writeln!(
                out,
                "{name}_bucket{{le=\"{b}\"}} {}",
                self.counts[i].load(Ordering::Relaxed)
            );
        }
        let count = self.count.load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {count}");
        let _ = writeln!(
            out,
            "{name}_sum {}",
            self.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0
        );
        let _ = writeln!(out, "{name}_count {count}");
    }
}

#[derive(Default)]
pub struct Metrics {
    // transport
    pub ws_connections_total: Counter,
    pub ws_messages_received_total: Counter,
    pub ws_messages_sent_total: Counter,
    pub ws_sessions_overflowed_total: Counter,
    // request paths
    pub query_seconds: Histogram,
    pub transact_seconds: Histogram,
    pub add_query_seconds: Histogram,
    // invalidation
    pub notify_lag_seconds: Histogram,
    pub refresh_batch_seconds: Histogram,
    pub refresh_batches_total: Counter,
    pub refresh_txs_total: Counter,
    pub refresh_queries_skipped_total: Counter,
    pub refresh_queries_computed_total: Counter,
    pub refresh_queries_changed_total: Counter,
    pub refresh_queries_deduped_total: Counter,
    pub refresh_queries_failed_total: Counter,
    pub refresh_ok_sent_total: Counter,
    pub refresh_errors_sent_total: Counter,
    // caches
    pub attr_cache_hits_total: Counter,
    pub attr_cache_misses_total: Counter,
    pub query_cache_hits_total: Counter,
    pub query_cache_misses_total: Counter,
}

pub static METRICS: LazyLock<Metrics> = LazyLock::new(Metrics::default);

fn counter(out: &mut String, name: &str, help: &str, v: u64) {
    let _ = writeln!(
        out,
        "# HELP {name} {help}\n# TYPE {name} counter\n{name} {v}"
    );
}

fn gauge(out: &mut String, name: &str, help: &str, v: impl std::fmt::Display) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {v}");
}

/// (rss bytes, cpu seconds) of this process: procfs on Linux, task info on
/// macOS; zeros elsewhere.
#[cfg(target_os = "linux")]
fn process_stats() -> (u64, f64) {
    let rss = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|kb| kb.parse::<u64>().ok())
                .map(|kb| kb * 1024)
        })
        .unwrap_or(0);
    // /proc/self/stat: utime + stime in USER_HZ (always 100 on Linux)
    let cpu = std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|s| {
            let rest = s.rsplit(')').next()?;
            let fields: Vec<&str> = rest.split_whitespace().collect();
            let utime: f64 = fields.get(11)?.parse().ok()?;
            let stime: f64 = fields.get(12)?.parse().ok()?;
            Some((utime + stime) / 100.0)
        })
        .unwrap_or(0.0);
    (rss, cpu)
}

#[cfg(target_os = "macos")]
fn process_stats() -> (u64, f64) {
    let mut info: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_taskinfo>() as i32;
    // SAFETY: proc_pidinfo writes at most `size` bytes into `info`, which is
    // a properly sized, zero-initialised proc_taskinfo.
    let n = unsafe {
        libc::proc_pidinfo(
            std::process::id() as i32,
            libc::PROC_PIDTASKINFO,
            0,
            &mut info as *mut _ as *mut libc::c_void,
            size,
        )
    };
    if n != size {
        return (0, 0.0);
    }
    // process cpu time straight from the kernel clock (nanoseconds)
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: plain out-parameter call.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts) };
    let cpu = if rc == 0 {
        ts.tv_sec as f64 + ts.tv_nsec as f64 / 1e9
    } else {
        0.0
    };
    (info.pti_resident_size, cpu)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn process_stats() -> (u64, f64) {
    (0, 0.0)
}

pub fn render(state: &AppState) -> String {
    let m = &*METRICS;
    let mut out = String::with_capacity(8192);

    gauge(
        &mut out,
        "instant_sessions",
        "live ws/sse sessions on this node",
        state.sessions.len(),
    );
    let _ = writeln!(
        out,
        "# HELP instant_app_sessions live sessions per app on this node\n# TYPE instant_app_sessions gauge"
    );
    for e in state.app_sessions.iter() {
        if !e.value().is_empty() {
            let _ = writeln!(
                out,
                "instant_app_sessions{{app_id=\"{}\"}} {}",
                e.key(),
                e.value().len()
            );
        }
    }
    let queued: usize = state
        .sessions
        .iter()
        .map(|s| s.queued.load(Ordering::Relaxed))
        .sum();
    gauge(
        &mut out,
        "instant_ws_queued_messages",
        "outgoing messages queued across all sessions",
        queued,
    );
    counter(
        &mut out,
        "instant_ws_connections_total",
        "sessions accepted",
        m.ws_connections_total.get(),
    );
    counter(
        &mut out,
        "instant_ws_messages_received_total",
        "client messages parsed",
        m.ws_messages_received_total.get(),
    );
    counter(
        &mut out,
        "instant_ws_messages_sent_total",
        "server messages queued to clients",
        m.ws_messages_sent_total.get(),
    );
    counter(
        &mut out,
        "instant_ws_sessions_overflowed_total",
        "sessions disconnected for exceeding the outgoing queue cap",
        m.ws_sessions_overflowed_total.get(),
    );

    m.query_seconds.render(
        &mut out,
        "instant_query_seconds",
        "instaql query + perms filter duration",
    );
    m.transact_seconds.render(
        &mut out,
        "instant_transact_seconds",
        "transact duration (parse to notify)",
    );
    m.add_query_seconds.render(
        &mut out,
        "instant_add_query_seconds",
        "add-query handling duration",
    );
    m.notify_lag_seconds.render(
        &mut out,
        "instant_notify_lag_seconds",
        "tx commit -> NOTIFY received on this node",
    );
    m.refresh_batch_seconds.render(
        &mut out,
        "instant_refresh_batch_seconds",
        "NOTIFY received -> refresh-ok queued for every session of the app",
    );
    counter(
        &mut out,
        "instant_refresh_batches_total",
        "per-app refresh batches run",
        m.refresh_batches_total.get(),
    );
    counter(
        &mut out,
        "instant_refresh_txs_total",
        "transactions covered by refresh batches (coalescing = txs/batches)",
        m.refresh_txs_total.get(),
    );
    counter(
        &mut out,
        "instant_refresh_queries_skipped_total",
        "registered queries skipped by topic matching",
        m.refresh_queries_skipped_total.get(),
    );
    counter(
        &mut out,
        "instant_refresh_queries_computed_total",
        "distinct (query, auth) recomputations run",
        m.refresh_queries_computed_total.get(),
    );
    counter(
        &mut out,
        "instant_refresh_queries_deduped_total",
        "session queries served from another session's identical recompute",
        m.refresh_queries_deduped_total.get(),
    );
    counter(
        &mut out,
        "instant_refresh_queries_failed_total",
        "recomputations that failed (rule errors, rate limits)",
        m.refresh_queries_failed_total.get(),
    );
    counter(
        &mut out,
        "instant_refresh_errors_sent_total",
        "refreshes answered with an error frame instead of refresh-ok",
        m.refresh_errors_sent_total.get(),
    );
    counter(
        &mut out,
        "instant_refresh_queries_changed_total",
        "session queries whose result hash changed (sent in refresh-ok)",
        m.refresh_queries_changed_total.get(),
    );
    counter(
        &mut out,
        "instant_refresh_ok_sent_total",
        "refresh-ok messages queued",
        m.refresh_ok_sent_total.get(),
    );
    let pending: usize = state.refresh_queues.iter().map(|q| q.pending_len()).sum();
    gauge(
        &mut out,
        "instant_refresh_pending_txs",
        "tx notifications waiting for a refresh batch",
        pending,
    );
    counter(
        &mut out,
        "instant_attr_cache_hits_total",
        "attr catalog loads served from cache",
        m.attr_cache_hits_total.get(),
    );
    counter(
        &mut out,
        "instant_attr_cache_misses_total",
        "attr catalog loads that hit Postgres",
        m.attr_cache_misses_total.get(),
    );
    counter(
        &mut out,
        "instant_query_cache_hits_total",
        "add-query results served from the shared result cache",
        m.query_cache_hits_total.get(),
    );
    counter(
        &mut out,
        "instant_query_cache_misses_total",
        "add-query results computed",
        m.query_cache_misses_total.get(),
    );
    gauge(
        &mut out,
        "instant_query_cache_entries",
        "entries in the add-query result cache",
        state.query_cache.len(),
    );

    gauge(
        &mut out,
        "instant_pg_pool_size",
        "open Postgres connections in the pool",
        state.pool.size(),
    );
    gauge(
        &mut out,
        "instant_pg_pool_idle",
        "idle Postgres connections in the pool",
        state.pool.num_idle(),
    );
    gauge(
        &mut out,
        "instant_pg_pool_max",
        "configured pool cap",
        state.cfg.pg_pool_max,
    );

    let (rss, cpu) = process_stats();
    gauge(
        &mut out,
        "process_resident_memory_bytes",
        "resident set size",
        rss,
    );
    counter(
        &mut out,
        "process_cpu_seconds_total",
        "user+system CPU time",
        cpu as u64,
    );
    gauge(
        &mut out,
        "process_cpu_seconds",
        "user+system CPU time (fractional)",
        cpu,
    );
    out
}
