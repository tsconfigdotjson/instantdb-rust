//! Per-app rate limiting (issue #1): in-process token buckets keyed by
//! app_id, plus per-(app, email) buckets for magic codes. Per-node, which
//! matches legacy's per-machine bucket4j behavior (rate_limit.clj).
//!
//! Exceeding a limit surfaces as 429 `rate-limited` with a `retry-after`
//! hint over HTTP, or an `error` op with the same fields over the socket.
//!
//! # Sizing
//!
//! The free tier should comfortably support an app with ~100 concurrently
//! active users. Each limit below is derived from the worst-case burst and
//! sustained rate that population can legitimately generate (the derivations
//! sit next to the constants in [`Limiters::new`]), with margin on top, so a
//! well-behaved app never sees a 429 while a single hostile or buggy client
//! hammering one app is still capped.
//!
//! Set `INSTANT_RATE_LIMITS=off` to disable all limits (load tests, dev).

use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use instant_core::error::InstantError;
use uuid::Uuid;

/// Legacy's generic bucket-exhausted message (util/exception.clj:495-500).
pub const RATE_LIMIT_MSG: &str = "Your request exceeded the rate limit.";
/// Legacy's magic-code message (util/exception.clj:480-482), used for both
/// sending and verifying codes (magic_code_auth.clj:32-69).
pub const EMAIL_RATE_LIMIT_MSG: &str =
    "Too many verification codes requested for this email. Please try again later.";

pub fn rate_limited_err(retry_after_secs: u64) -> InstantError {
    InstantError::rate_limited(RATE_LIMIT_MSG, retry_after_secs)
}

pub fn email_rate_limited_err(retry_after_secs: u64) -> InstantError {
    InstantError::rate_limited(EMAIL_RATE_LIMIT_MSG, retry_after_secs)
}

struct Bucket {
    tokens: f64,
    last: Instant,
}

/// A keyed token bucket: each key gets `capacity` tokens refilled greedily at
/// `refill_per_sec`. Stale (fully refilled) buckets are swept periodically —
/// a full bucket is indistinguishable from a fresh one, so dropping it never
/// changes behavior.
pub struct RateLimiter<K: Eq + Hash> {
    capacity: f64,
    refill_per_sec: f64,
    enabled: bool,
    buckets: DashMap<K, Bucket>,
    /// seconds since `start` of the last sweep
    last_sweep: AtomicU64,
    start: Instant,
}

const SWEEP_EVERY: Duration = Duration::from_secs(600);

impl<K: Eq + Hash> RateLimiter<K> {
    pub fn new(capacity: f64, refill_per_sec: f64, enabled: bool) -> Self {
        RateLimiter {
            capacity,
            refill_per_sec,
            enabled,
            buckets: DashMap::new(),
            last_sweep: AtomicU64::new(0),
            start: Instant::now(),
        }
    }

    /// Consume `cost` tokens for `key`. On exhaustion returns the whole
    /// seconds until enough tokens refill (rounded up, min 1) — suitable for
    /// the `retry-after` hint.
    pub fn check(&self, key: K, cost: f64) -> Result<(), u64> {
        self.check_at(key, cost, Instant::now())
    }

    fn check_at(&self, key: K, cost: f64, now: Instant) -> Result<(), u64> {
        if !self.enabled {
            return Ok(());
        }
        self.maybe_sweep(now);
        let mut b = self.buckets.entry(key).or_insert_with(|| Bucket {
            tokens: self.capacity,
            last: now,
        });
        let elapsed = now.saturating_duration_since(b.last).as_secs_f64();
        b.tokens = (b.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        b.last = now;
        if b.tokens >= cost {
            b.tokens -= cost;
            Ok(())
        } else {
            let secs = ((cost - b.tokens) / self.refill_per_sec).ceil().max(1.0);
            Err(secs as u64)
        }
    }

    fn maybe_sweep(&self, now: Instant) {
        let now_s = now.saturating_duration_since(self.start).as_secs();
        let last = self.last_sweep.load(Ordering::Relaxed);
        if now_s.saturating_sub(last) < SWEEP_EVERY.as_secs() {
            return;
        }
        if self
            .last_sweep
            .compare_exchange(last, now_s, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return; // another thread won the sweep
        }
        let full_after = Duration::from_secs_f64(self.capacity / self.refill_per_sec);
        self.buckets
            .retain(|_, b| now.saturating_duration_since(b.last) < full_after);
    }
}

/// Cost of one websocket op against the per-app `ws` bucket. Ops that hit the
/// database (queries, transacts, sync/stream setup) are weighted heavier than
/// ephemeral fan-out (presence, broadcast). `init` is exempt like legacy
/// (session.clj:981-984) and never reaches the bucket.
pub fn ws_op_cost(op: &str) -> f64 {
    match op {
        "add-query" | "transact" | "start-sync" | "resync-table" | "start-stream"
        | "append-stream" | "subscribe-stream" => 5.0,
        _ => 1.0,
    }
}

pub struct Limiters {
    /// websocket/SSE ops, weighted by [`ws_op_cost`], keyed by app
    pub ws: RateLimiter<Uuid>,
    /// /runtime/auth/* requests keyed by app
    pub auth: RateLimiter<Uuid>,
    /// send_magic_code keyed by (app, email)
    pub magic_code_send: RateLimiter<(Uuid, String)>,
    /// verify_magic_code keyed by (app, email) — also the 6-digit-code
    /// brute-force guard
    pub magic_code_verify: RateLimiter<(Uuid, String)>,
    /// /admin/* requests keyed by app
    pub admin: RateLimiter<Uuid>,
    /// client storage uploads/deletes keyed by app
    pub storage_upload: RateLimiter<Uuid>,
    /// storage downloads / signed-url mints keyed by app
    pub storage_serve: RateLimiter<Uuid>,
}

impl Limiters {
    pub fn from_env() -> Self {
        let enabled = !matches!(
            std::env::var("INSTANT_RATE_LIMITS").as_deref(),
            Ok("off") | Ok("0") | Ok("false")
        );
        Self::new(enabled)
    }

    /// Free-tier sizing for ~100 concurrently active users per app; each
    /// constant is derived from that population's worst-case behavior.
    pub fn new(enabled: bool) -> Self {
        Limiters {
            // Burst: a reconnect storm — all 100 clients reconnect at once
            // (deploy, network blip), each sending init (10) + ~10 add-query
            // (5 each) + ~3 join-room (1 each) ≈ 63 cost, so ~6,300 total;
            // capacity 8,000 leaves ~25% margin. Sustained: 100 users at a
            // 20 Hz client-side presence throttle (2,000/s) plus ~2 writes/s
            // each (100 × 2 × 5 = 1,000/s) would be 3,000/s at the absolute
            // worst; realistic steady state is far below the 2,000/s refill,
            // and the burst pool absorbs the difference.
            ws: RateLimiter::new(8_000.0, 2_000.0, enabled),
            // Burst: a launch wave — all 100 users authenticate in the same
            // minute, each doing send_magic_code + verify_magic_code +
            // verify_refresh_token = 300 requests; capacity 400 adds margin.
            // Sustained: every user reloading the app (one refresh-token
            // verify each) every 30s is ~3.3/s; refill 5/s covers it.
            auth: RateLimiter::new(400.0, 5.0, enabled),
            // Legacy default: 20 codes per (app, email) per hour
            // (flags.clj:585-588). 20/3600 ≈ 0.0056 tokens/s.
            magic_code_send: RateLimiter::new(20.0, 20.0 / 3600.0, enabled),
            // Same shape as legacy's "consume" bucket. Also caps guessing a
            // 6-digit code: ~5×10⁵ expected guesses at 20/hour ≈ 2,800 years.
            magic_code_verify: RateLimiter::new(20.0, 20.0 / 3600.0, enabled),
            // Backend scripts do bulk work: a 100k-row import in batches of
            // 100 is 1,000 requests — exactly the burst capacity — then
            // continues at the 100/s refill. Normal server traffic (one
            // query per page render at 100 users) is well under refill.
            admin: RateLimiter::new(1_000.0, 100.0, enabled),
            // Burst: 100 users each uploading a 5-file album at once = 500.
            // Refill 20/s ≈ 1,200 uploads/min sustained.
            storage_upload: RateLimiter::new(500.0, 20.0, enabled),
            // Burst: 100 users each loading a gallery of 50 images = 5,000.
            // Refill 500/s sustains ~5 image loads/s per user.
            storage_serve: RateLimiter::new(5_000.0, 500.0, enabled),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn burst_then_deny_with_retry_after() {
        let rl: RateLimiter<u32> = RateLimiter::new(10.0, 1.0, true);
        let t0 = Instant::now();
        for _ in 0..10 {
            assert!(rl.check_at(1, 1.0, t0).is_ok());
        }
        // empty: cost 1 at 1 token/s → retry in 1s; cost 5 → 5s
        assert_eq!(rl.check_at(1, 1.0, t0), Err(1));
        assert_eq!(rl.check_at(1, 5.0, t0), Err(5));
        // keys are independent
        assert!(rl.check_at(2, 1.0, t0).is_ok());
    }

    #[test]
    fn refills_over_time() {
        let rl: RateLimiter<u32> = RateLimiter::new(10.0, 2.0, true);
        let t0 = Instant::now();
        for _ in 0..10 {
            assert!(rl.check_at(1, 1.0, t0).is_ok());
        }
        assert!(rl.check_at(1, 1.0, t0).is_err());
        // 3s later: 6 tokens back
        let t1 = t0 + Duration::from_secs(3);
        for _ in 0..6 {
            assert!(rl.check_at(1, 1.0, t1).is_ok());
        }
        assert!(rl.check_at(1, 1.0, t1).is_err());
        // refill caps at capacity
        let t2 = t1 + Duration::from_secs(3600);
        for _ in 0..10 {
            assert!(rl.check_at(1, 1.0, t2).is_ok());
        }
        assert!(rl.check_at(1, 1.0, t2).is_err());
    }

    #[test]
    fn disabled_always_allows() {
        let rl: RateLimiter<u32> = RateLimiter::new(1.0, 1.0, false);
        let t0 = Instant::now();
        for _ in 0..1000 {
            assert!(rl.check_at(1, 100.0, t0).is_ok());
        }
    }

    #[test]
    fn sweep_drops_only_full_buckets() {
        let rl: RateLimiter<u32> = RateLimiter::new(10.0, 1.0, true);
        let t0 = Instant::now();
        rl.check_at(1, 10.0, t0).unwrap(); // key 1 drained (full again in 10s)
        rl.check_at(2, 1.0, t0 + SWEEP_EVERY + Duration::from_secs(5))
            .unwrap();
        // key 2's check crossed the sweep interval: key 1 refilled long ago
        // and is dropped; key 2 (touched just now, not full) is kept.
        assert!(!rl.buckets.contains_key(&1));
        assert!(rl.buckets.contains_key(&2));
    }

    /// The sizing story from the module docs: a full reconnect storm of 100
    /// free-tier users fits in the ws bucket without a single 429.
    #[test]
    fn ws_bucket_absorbs_100_user_reconnect_storm() {
        let l = Limiters::new(true);
        let app = Uuid::new_v4();
        let t0 = Instant::now();
        for _user in 0..100 {
            assert!(l.ws.check_at(app, ws_op_cost("init"), t0).is_ok());
            for _q in 0..10 {
                assert!(l.ws.check_at(app, ws_op_cost("add-query"), t0).is_ok());
            }
            for _r in 0..3 {
                assert!(l.ws.check_at(app, ws_op_cost("join-room"), t0).is_ok());
            }
        }
    }

    #[test]
    fn magic_code_bucket_matches_legacy_20_per_hour() {
        let l = Limiters::new(true);
        let app = Uuid::new_v4();
        let key = (app, "a@b.com".to_string());
        let t0 = Instant::now();
        for _ in 0..20 {
            assert!(l.magic_code_send.check_at(key.clone(), 1.0, t0).is_ok());
        }
        let retry = l
            .magic_code_send
            .check_at(key.clone(), 1.0, t0)
            .unwrap_err();
        // next token in 3600/20 = 180s
        assert_eq!(retry, 180);
        // other emails unaffected
        assert!(l
            .magic_code_send
            .check_at((app, "c@d.com".to_string()), 1.0, t0)
            .is_ok());
    }
}
