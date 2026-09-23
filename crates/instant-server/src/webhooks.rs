//! Webhooks (LEGACY model/webhook.clj, webhook_processor.clj,
//! webhook_sender.clj, webhook_jwt.clj; migration 109).
//!
//! Legacy produces webhook events from its logical-replication feed: each
//! WAL record is bloom-matched against the app's webhooks and a
//! `webhook_events` row (webhook_id, isn) is queued; the payload is rebuilt
//! later from the stored WAL record. This server has no WAL consumer, so the
//! same happens on the transaction path: after a commit, the touched
//! entities of the app's webhook namespaces are matched against the
//! webhooks, a snapshot of those entities plus the tx's triple changes is
//! kept in `rust_webhook_history`, and the events are queued in the legacy
//! `webhook_events` table. The ISN of a transaction is `0/<tx-id as lsn>`.
//!
//! Delivery follows legacy: workers claim events with the migration's
//! `claim_webhook_events`, POST `{payloadUrl, token}` signed with the
//! server's Ed25519 key (`Instant-Signature: t=,kid=,v1=`), record every
//! attempt on the row, retry on Stripe's schedule, and give up after 11
//! attempts, on 410, or when the webhook is disabled.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};
use instant_core::attr::{AttrMap, Cardinality, ValueType};
use instant_core::error::{InstantError, Result};
use instant_core::tx::TxReport;
use serde_json::{json, Map, Value};
use sqlx::Row;
use uuid::Uuid;

use crate::service;
use crate::state::AppState;

// ---------------------------------------------------------------------------
// ISN: legacy `instant.isn` (slot-num, pg lsn); here slot 0 and the tx id

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Isn {
    pub slot: i32,
    pub lsn: i64,
}

impl Isn {
    pub fn of_tx(tx_id: i64) -> Isn {
        Isn {
            slot: 0,
            lsn: tx_id,
        }
    }
    /// pg_lsn text: `X/Y` with the high and low 32 bits in hex
    pub fn lsn_text(&self) -> String {
        format!(
            "{:X}/{:X}",
            (self.lsn as u64) >> 32,
            (self.lsn as u64) & 0xffff_ffff
        )
    }
    /// legacy `(format "%X/%s" slot-num (.asString lsn))`
    pub fn to_string_legacy(self) -> String {
        format!("{:X}/{}", self.slot, self.lsn_text())
    }
    /// legacy `isn/of-string`
    pub fn parse(s: &str) -> Option<Isn> {
        let (slot, lsn) = s.split_once('/')?;
        let slot = i32::from_str_radix(slot, 16).ok()?;
        let (hi, lo) = lsn.split_once('/')?;
        let hi = u64::from_str_radix(hi, 16).ok()?;
        let lo = u64::from_str_radix(lo, 16).ok()?;
        Some(Isn {
            slot,
            lsn: ((hi << 32) | lo) as i64,
        })
    }
    pub fn from_row(slot: i32, lsn_text: &str) -> Option<Isn> {
        let (hi, lo) = lsn_text.split_once('/')?;
        let hi = u64::from_str_radix(hi, 16).ok()?;
        let lo = u64::from_str_radix(lo, 16).ok()?;
        Some(Isn {
            slot,
            lsn: ((hi << 32) | lo) as i64,
        })
    }
    /// legacy `isn/->bytes`: 4-byte slot + 8-byte lsn, big endian
    pub fn to_bytes(self) -> [u8; 12] {
        let mut out = [0u8; 12];
        out[..4].copy_from_slice(&self.slot.to_be_bytes());
        out[4..].copy_from_slice(&self.lsn.to_be_bytes());
        out
    }
    pub fn from_bytes(b: &[u8]) -> Option<Isn> {
        if b.len() != 12 {
            return None;
        }
        Some(Isn {
            slot: i32::from_be_bytes(b[..4].try_into().ok()?),
            lsn: i64::from_be_bytes(b[4..].try_into().ok()?),
        })
    }
}

/// `partition-bucket-for-time` (model/history.clj:67-73): 30-day buckets, 13
/// of them.
pub fn partition_bucket(t: chrono::DateTime<chrono::Utc>) -> i32 {
    ((t.timestamp() / 86400 / 30) % 13) as i32
}

/// `recent-partition-buckets`: the n most recent buckets, newest first
pub fn recent_buckets(n: i32) -> Vec<i32> {
    let cur = partition_bucket(chrono::Utc::now());
    (0..n).map(|o| (cur - o).rem_euclid(13)).collect()
}

// ---------------------------------------------------------------------------
// signing key (legacy: a Tink Ed25519 keyset; here an Ed25519 seed persisted
// in rust_server_config with a random numeric key id)

pub struct WebhookKey {
    pub signing: SigningKey,
    pub kid: String,
}

impl WebhookKey {
    pub fn verifying(&self) -> VerifyingKey {
        self.signing.verifying_key()
    }
    /// RFC 8037 OKP JWK set (crypt.clj:213-238)
    pub fn jwks(&self) -> Value {
        let x =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(self.verifying().to_bytes());
        json!({"keys": [{
            "kty": "OKP",
            "crv": "Ed25519",
            "alg": "EdDSA",
            "use": "sig",
            "kid": self.kid,
            "x": x,
        }]})
    }
}

pub async fn load_or_generate_key(pool: &sqlx::PgPool) -> Result<WebhookKey> {
    let seed: String = {
        use rand::RngCore;
        let mut bytes = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    };
    let kid: String = {
        use rand::Rng;
        rand::thread_rng()
            .gen_range(100_000_000u32..1_000_000_000u32)
            .to_string()
    };
    sqlx::query(
        "INSERT INTO rust_server_config (key, value) VALUES ('webhook_signing_key', $1), ('webhook_kid', $2)
         ON CONFLICT (key) DO NOTHING",
    )
    .bind(&seed)
    .bind(&kid)
    .execute(pool)
    .await?;
    let rows = sqlx::query(
        "SELECT key, value FROM rust_server_config WHERE key IN ('webhook_signing_key', 'webhook_kid')",
    )
    .fetch_all(pool)
    .await?;
    let mut seed_hex = String::new();
    let mut kid = String::new();
    for r in rows {
        match r.get::<String, _>("key").as_str() {
            "webhook_signing_key" => seed_hex = r.get("value"),
            "webhook_kid" => kid = r.get("value"),
            _ => {}
        }
    }
    let bytes: Vec<u8> = (0..seed_hex.len())
        .step_by(2)
        .filter_map(|i| u8::from_str_radix(&seed_hex[i..i + 2], 16).ok())
        .collect();
    let seed: [u8; 32] = bytes
        .try_into()
        .map_err(|_| InstantError::internal("corrupt webhook signing key"))?;
    Ok(WebhookKey {
        signing: SigningKey::from_bytes(&seed),
        kid,
    })
}

fn key(state: &AppState) -> Result<&WebhookKey> {
    state
        .webhook_key
        .get()
        .ok_or_else(|| InstantError::internal("webhook signing key not loaded"))
}

/// `sign-webhook` (webhook_sender.clj:25-38): `t.body` signed, hex signature
pub fn sign_body(state: &AppState, body: &[u8]) -> Result<(String, String, String)> {
    let k = key(state)?;
    let t = chrono::Utc::now().timestamp().to_string();
    let mut msg = Vec::with_capacity(t.len() + 1 + body.len());
    msg.extend_from_slice(t.as_bytes());
    msg.push(b'.');
    msg.extend_from_slice(body);
    let sig = k.signing.sign(&msg).to_bytes();
    Ok((
        t,
        k.kid.clone(),
        sig.iter().map(|b| format!("{b:02x}")).collect(),
    ))
}

fn b64url(b: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b)
}

/// `webhook-payload-jwt` (webhook_jwt.clj:59-76): EdDSA JWT with iss, sub
/// (app id), 1-hour exp, app-id / webhook-id / isn claims.
pub fn payload_jwt(state: &AppState, app_id: Uuid, webhook_id: Uuid, isn: Isn) -> Result<String> {
    let k = key(state)?;
    let header = json!({"alg": "EdDSA", "typ": "JWT", "kid": k.kid});
    let claims = json!({
        "iss": state.cfg.base_url,
        "sub": app_id.to_string(),
        "exp": chrono::Utc::now().timestamp() + 3600,
        "app-id": app_id.to_string(),
        "webhook-id": webhook_id.to_string(),
        "isn": isn.to_string_legacy(),
    });
    let signing_input = format!(
        "{}.{}",
        b64url(header.to_string().as_bytes()),
        b64url(claims.to_string().as_bytes())
    );
    let sig = k.signing.sign(signing_input.as_bytes()).to_bytes();
    Ok(format!("{signing_input}.{}", b64url(&sig)))
}

fn jwt_err(message: &str) -> InstantError {
    InstantError::validation_failed_input(
        "jwt",
        json!("<redacted token>"),
        json!([{"message": message}]),
    )
}

/// `verify-webhook-payload-jwt` (webhook_jwt.clj:80-105)
pub fn verify_payload_jwt(
    state: &AppState,
    token: &str,
    app_id: Uuid,
    webhook_id: Uuid,
    isn: Isn,
) -> Result<()> {
    let k = key(state)?;
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(jwt_err("Invalid JWT."));
    }
    let dec = |s: &str| base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(s);
    let sig_bytes = dec(parts[2]).map_err(|_| jwt_err("Invalid JWT."))?;
    let sig =
        ed25519_dalek::Signature::from_slice(&sig_bytes).map_err(|_| jwt_err("Invalid JWT."))?;
    let signing_input = format!("{}.{}", parts[0], parts[1]);
    if k.verifying()
        .verify(signing_input.as_bytes(), &sig)
        .is_err()
    {
        return Err(jwt_err("Invalid JWT."));
    }
    let claims: Value = dec(parts[1])
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .ok_or_else(|| jwt_err("Invalid JWT."))?;
    let exp = claims
        .get("exp")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| jwt_err("JWT missing required claims: [exp]"))?;
    if exp <= chrono::Utc::now().timestamp() {
        return Err(jwt_err("Expired JWT"));
    }
    let expect = |claim: &str, value: &str| -> Result<()> {
        let got = claims.get(claim).and_then(|v| v.as_str()).unwrap_or("");
        if got != value {
            return Err(jwt_err(&format!(
                "JWT {claim} claim has value {got}, must be {value}"
            )));
        }
        Ok(())
    };
    expect("iss", &state.cfg.base_url)?;
    expect("sub", &app_id.to_string())?;
    expect("webhook-id", &webhook_id.to_string())?;
    expect("app-id", &app_id.to_string())?;
    expect("isn", &isn.to_string_legacy())?;
    Ok(())
}

// ---------------------------------------------------------------------------
// url validation + SSRF guard (webhook_sender.clj, smokescreen.clj)

fn bad_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_multicast()
                || (o[0] == 100 && (64..=127).contains(&o[1])) // CGNAT 100.64/10
                || o[0] == 0
        }
        IpAddr::V6(v6) => {
            let seg = v6.segments();
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (seg[0] & 0xfe00) == 0xfc00 // unique local
                || (seg[0] & 0xffc0) == 0xfe80 // link local
                || (seg[0] == 0x2002) // 6to4
                || (seg[0] == 0x2001 && seg[1] == 0) // teredo
                || (seg[0] == 0x64 && seg[1] == 0xff9b) // NAT64
                || v6.to_ipv4_mapped().map(|v4| bad_ip(IpAddr::V4(v4))).unwrap_or(false)
        }
    }
}

fn webhook_validation(input: Value, message: &str) -> InstantError {
    InstantError::validation_failed_input("webhook", input, json!([{"message": message}]))
}

/// legacy `assert-valid-url!` + `webhook-sender/validate-url`: https, not
/// localhost, parseable, resolvable to a public address.
/// `INSTANT_WEBHOOK_ALLOW_INSECURE=1` lets test receivers on http://localhost
/// through (never set it in production).
/// Validated destination of a webhook URL: the host and every address it
/// resolved to (all public), so the delivery can be pinned to exactly those
/// (`ClientBuilder::resolve_to_addrs`) and a DNS answer that changes between
/// validation and connect can't reach a private network.
pub struct ValidatedUrl {
    pub host: String,
    pub addrs: Vec<std::net::SocketAddr>,
}

pub async fn assert_valid_url(url: &str) -> Result<ValidatedUrl> {
    let insecure_ok = matches!(
        std::env::var("INSTANT_WEBHOOK_ALLOW_INSECURE").as_deref(),
        Ok("1") | Ok("true")
    );
    let parsed = url::Url::parse(url).ok();
    let scheme = parsed.as_ref().map(|u| u.scheme()).unwrap_or("");
    let host = parsed
        .as_ref()
        .and_then(|u| u.host_str())
        .unwrap_or("")
        .to_string();
    if scheme != "https" && !insecure_ok {
        return Err(webhook_validation(
            json!({"url": url}),
            "The Webhook url must be https",
        ));
    }
    if (host == "localhost" || host == "127.0.0.1") && !insecure_ok {
        return Err(webhook_validation(
            json!({"url": url}),
            "The Webhook url must be a public url, localhost is not allowed.",
        ));
    }
    let Some(parsed) = parsed else {
        return Err(webhook_validation(json!({"url": url}), "Invalid URL."));
    };
    if !matches!(parsed.scheme(), "http" | "https") || host.is_empty() {
        return Err(webhook_validation(json!({"url": url}), "Invalid URL."));
    }
    let port = parsed.port_or_known_default().unwrap_or(443);
    if insecure_ok {
        // test receivers: no address vetting, reqwest resolves as usual
        return Ok(ValidatedUrl {
            host,
            addrs: vec![],
        });
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        if bad_ip(ip) {
            return Err(webhook_validation(
                json!({"url": url}),
                "Could not resolve URL.",
            ));
        }
        return Ok(ValidatedUrl {
            host,
            addrs: vec![std::net::SocketAddr::new(ip, port)],
        });
    }
    let resolved: Vec<std::net::SocketAddr> = match tokio::time::timeout(
        Duration::from_secs(10),
        tokio::net::lookup_host((host.as_str(), port)),
    )
    .await
    {
        Ok(Ok(addrs)) => addrs.collect(),
        _ => vec![],
    };
    // legacy's resolver (webhook_sender.clj `validate-url` / smokescreen
    // `bad-ip?`) refuses the host when any answer is private, and so does
    // the delivery client it wires the same resolver into
    if resolved.is_empty() || resolved.iter().any(|a| bad_ip(a.ip())) {
        return Err(webhook_validation(
            json!({"url": url}),
            "Could not resolve URL.",
        ));
    }
    Ok(ValidatedUrl {
        host,
        addrs: resolved,
    })
}

// ---------------------------------------------------------------------------
// model

#[derive(Clone)]
pub struct Webhook {
    pub id: Uuid,
    pub id_attr_ids: Vec<Uuid>,
    pub actions: Vec<String>,
    pub status: String,
    pub disabled_reason: Option<String>,
    pub url: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

const WEBHOOK_COLUMNS: &str =
    "id, app_id, id_attr_ids, actions::text[] AS actions, status::text AS status, disabled_reason, sink, created_at, updated_at";

fn webhook_from_row(r: &sqlx::postgres::PgRow) -> Webhook {
    let sink: Value = r.get("sink");
    Webhook {
        id: r.get("id"),
        id_attr_ids: r
            .get::<Option<Vec<Uuid>>, _>("id_attr_ids")
            .unwrap_or_default(),
        actions: r
            .get::<Option<Vec<String>>, _>("actions")
            .unwrap_or_default(),
        status: r.get("status"),
        disabled_reason: r.get("disabled_reason"),
        url: sink
            .get("url")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
    }
}

/// The webhook's namespaces: the etypes of its id attrs (legacy joins attrs
/// of the app and the system catalog).
pub fn namespaces(attrs: &AttrMap, w: &Webhook) -> Vec<String> {
    w.id_attr_ids
        .iter()
        .filter_map(|id| attrs.get(id).map(|a| a.etype.clone()))
        .collect()
}

pub fn webhook_json(attrs: &AttrMap, w: &Webhook) -> Value {
    json!({
        "id": w.id,
        "sink": {"url": w.url},
        "namespaces": namespaces(attrs, w),
        "actions": w.actions,
        "status": w.status,
        "disabled_reason": w.disabled_reason,
        "created_at": w.created_at.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        "updated_at": w.updated_at.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
    })
}

fn webhook_not_found(app_id: Uuid, webhook_id: Uuid) -> InstantError {
    InstantError::new(
        "record-not-found",
        400,
        "Record not found: webhook",
        Some(
            json!({"args": [{"app-id": app_id, "webhook-id": webhook_id}], "record-type": "webhook"}),
        ),
    )
}

pub async fn get_by_id(state: &AppState, app_id: Uuid, webhook_id: Uuid) -> Result<Webhook> {
    let row = sqlx::query(&format!(
        "SELECT {WEBHOOK_COLUMNS} FROM webhooks WHERE app_id = $1 AND id = $2"
    ))
    .bind(app_id)
    .bind(webhook_id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or_else(|| webhook_not_found(app_id, webhook_id))?;
    Ok(webhook_from_row(&row))
}

pub async fn get_all(state: &AppState, app_id: Uuid) -> Result<Vec<Webhook>> {
    let rows = sqlx::query(&format!(
        "SELECT {WEBHOOK_COLUMNS} FROM webhooks WHERE app_id = $1 ORDER BY created_at DESC"
    ))
    .bind(app_id)
    .fetch_all(&state.pool)
    .await?;
    Ok(rows.iter().map(webhook_from_row).collect())
}

/// How long a node trusts its cached view of an app's active webhooks
/// without a NOTIFY (the `webhooks` trigger evicts on every change, so this
/// is only the safety net for a missed notification).
const WEBHOOK_CACHE_TTL: Duration = Duration::from_secs(30);

/// The app's active webhooks, cached per node so the transaction path pays a
/// map lookup instead of a query (legacy: model/webhook.clj's cache fed by
/// the WAL). Management writes evict locally, the `webhooks` row trigger
/// evicts every node through `instant_webhooks`.
async fn active_webhooks(state: &AppState, app_id: Uuid) -> Result<Arc<Vec<Webhook>>> {
    if let Some(entry) = state.webhook_cache.get(&app_id) {
        if entry.0.elapsed() < WEBHOOK_CACHE_TTL {
            return Ok(entry.1.clone());
        }
    }
    let rows = sqlx::query(&format!(
        "SELECT {WEBHOOK_COLUMNS} FROM webhooks WHERE app_id = $1 AND status = 'active'"
    ))
    .bind(app_id)
    .fetch_all(&state.pool)
    .await?;
    let hooks: Arc<Vec<Webhook>> = Arc::new(rows.iter().map(webhook_from_row).collect());
    state
        .webhook_cache
        .insert(app_id, (std::time::Instant::now(), hooks.clone()));
    Ok(hooks)
}

pub fn invalidate_cache(state: &AppState, app_id: Uuid) {
    state.webhook_cache.remove(&app_id);
}

/// `maximum-active-webhooks` flag default
const MAX_ACTIVE_WEBHOOKS: i64 = 100;

/// `namespaces->id-attr-ids+topics!`: the id attr of every namespace.
pub fn id_attr_ids(attrs: &AttrMap, namespaces: &[String]) -> Result<Vec<Uuid>> {
    let mut ids: Vec<Uuid> = vec![];
    for ns in namespaces {
        let attr = attrs.id_attr_of(ns).ok_or_else(|| {
            webhook_validation(
                json!({"namespace": ns}),
                &format!("Could not find matching table for {ns}"),
            )
        })?;
        if !ids.contains(&attr.id) {
            ids.push(attr.id);
        }
    }
    if ids.is_empty() {
        return Err(webhook_validation(
            json!({"namespaces": namespaces}),
            "Webhook must have at least one table.",
        ));
    }
    Ok(ids)
}

fn webhooks_validation(app_id: Uuid, message: &str, hint: Option<Value>) -> InstantError {
    let mut err = json!({"message": message});
    if let Some(h) = hint {
        err["hint"] = h;
    }
    InstantError::validation_failed_input("webhooks", json!({"app-id": app_id}), json!([err]))
}

/// legacy history.clj bloom-bit: one bit of a 64-bit word per attr id
/// (xxHash3 in legacy; any stable hash serves the same purpose here since the
/// filter is only consulted by this server's own matching)
fn bloom_bit(id: Uuid) -> i64 {
    let (hi, lo) = id.as_u64_pair();
    let mut h = hi ^ lo.rotate_left(29);
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    1i64 << (h % 64)
}

fn topics_of(ids: &[Uuid]) -> i64 {
    ids.iter().fold(0i64, |acc, id| acc | bloom_bit(*id))
}

async fn check_limit(conn: &mut sqlx::PgConnection, app_id: Uuid) -> Result<()> {
    let n: i64 =
        sqlx::query("SELECT count(*) AS n FROM webhooks WHERE status = 'active' AND app_id = $1")
            .bind(app_id)
            .fetch_one(&mut *conn)
            .await?
            .get("n");
    if n >= MAX_ACTIVE_WEBHOOKS {
        return Err(webhooks_validation(
            app_id,
            &format!("An app may not have more than {MAX_ACTIVE_WEBHOOKS} active webhooks."),
            None,
        ));
    }
    Ok(())
}

async fn check_duplicate(
    conn: &mut sqlx::PgConnection,
    app_id: Uuid,
    url: &str,
    id_attr_ids: &[Uuid],
    actions: &[String],
    ignore: Option<Uuid>,
) -> Result<()> {
    let dup: Option<Uuid> = sqlx::query(
        "SELECT id FROM webhooks
          WHERE status = 'active' AND app_id = $1
            AND id_attr_ids @> $2 AND id_attr_ids <@ $2
            AND actions @> $3::webhook_action[] AND actions <@ $3::webhook_action[]
            AND sink = $4
            AND ($5::uuid IS NULL OR id <> $5)
          LIMIT 1",
    )
    .bind(app_id)
    .bind(id_attr_ids)
    .bind(actions)
    .bind(json!({"url": url}))
    .bind(ignore)
    .fetch_optional(&mut *conn)
    .await
    .map_err(translate_pg)?
    .map(|r| r.get("id"));
    if let Some(id) = dup {
        return Err(webhooks_validation(
            app_id,
            "A webhook already exists with all of the same properties",
            Some(json!({"webhook-id": id})),
        ));
    }
    Ok(())
}

/// An invalid `webhook_action` enum value reaches Postgres in legacy and
/// surfaces as a 500 `sql-exception`.
fn translate_pg(e: sqlx::Error) -> InstantError {
    if let sqlx::Error::Database(db) = &e {
        if db.code().as_deref() == Some("22P02") {
            return InstantError::new(
                "sql-exception",
                500,
                "SQL Exception: invalid-text-representation",
                Some(
                    json!({"table": db.table(), "condition": "invalid-text-representation", "constraint": db.constraint()}),
                ),
            );
        }
    }
    InstantError::from(e)
}

fn actions_non_empty(actions: &[String]) -> Result<()> {
    if actions.is_empty() {
        return Err(webhook_validation(
            json!({"actions": actions}),
            "Webhook must have at least one action.",
        ));
    }
    Ok(())
}

pub async fn create(
    state: &AppState,
    app_id: Uuid,
    url: &str,
    namespaces: &[String],
    actions: &[String],
) -> Result<Uuid> {
    assert_valid_url(url).await?;
    let attrs = service::load_attrs(state, app_id).await?;
    let ids = id_attr_ids(&attrs, namespaces)?;
    actions_non_empty(actions)?;
    let mut dbtx = state.pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('webhooks'), hashtext($1::text))")
        .bind(app_id.to_string())
        .execute(&mut *dbtx)
        .await?;
    check_limit(&mut dbtx, app_id).await?;
    check_duplicate(&mut dbtx, app_id, url, &ids, actions, None).await?;
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO webhooks (id, app_id, topics, id_attr_ids, actions, status, sink)
         VALUES ($1, $2, $3, $4, $5::webhook_action[], 'active', $6)",
    )
    .bind(id)
    .bind(app_id)
    .bind(topics_of(&ids))
    .bind(&ids)
    .bind(actions)
    .bind(json!({"url": url}))
    .execute(&mut *dbtx)
    .await
    .map_err(translate_pg)?;
    dbtx.commit().await?;
    invalidate_cache(state, app_id);
    Ok(id)
}

pub struct WebhookPatch {
    pub url: Option<String>,
    pub namespaces: Option<Vec<String>>,
    pub actions: Option<Vec<String>>,
}

pub async fn update(
    state: &AppState,
    app_id: Uuid,
    webhook_id: Uuid,
    patch: WebhookPatch,
) -> Result<()> {
    if let Some(url) = &patch.url {
        assert_valid_url(url).await?;
    }
    if let Some(actions) = &patch.actions {
        actions_non_empty(actions)?;
    }
    let new_ids = match &patch.namespaces {
        Some(ns) => {
            let attrs = service::load_attrs(state, app_id).await?;
            Some(id_attr_ids(&attrs, ns)?)
        }
        None => None,
    };
    let mut dbtx = state.pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('webhooks'), hashtext($1::text))")
        .bind(app_id.to_string())
        .execute(&mut *dbtx)
        .await?;
    let existing = get_by_id(state, app_id, webhook_id).await?;
    let url = patch.url.clone().unwrap_or(existing.url.clone());
    let ids = new_ids.clone().unwrap_or(existing.id_attr_ids.clone());
    let actions = patch.actions.clone().unwrap_or(existing.actions.clone());
    check_duplicate(&mut dbtx, app_id, &url, &ids, &actions, Some(webhook_id)).await?;
    sqlx::query(
        "UPDATE webhooks SET
            sink = coalesce($3, sink),
            id_attr_ids = coalesce($4, id_attr_ids),
            topics = coalesce($5, topics),
            actions = coalesce($6::webhook_action[], actions)
          WHERE app_id = $1 AND id = $2",
    )
    .bind(app_id)
    .bind(webhook_id)
    .bind(patch.url.as_ref().map(|u| json!({"url": u})))
    .bind(new_ids.clone())
    .bind(new_ids.as_ref().map(|ids| topics_of(ids)))
    .bind(patch.actions.clone())
    .execute(&mut *dbtx)
    .await
    .map_err(translate_pg)?;
    dbtx.commit().await?;
    invalidate_cache(state, app_id);
    Ok(())
}

pub async fn disable(
    state: &AppState,
    app_id: Uuid,
    webhook_id: Uuid,
    reason: Option<&str>,
) -> Result<()> {
    sqlx::query("UPDATE webhooks SET status = 'disabled', disabled_reason = $3 WHERE app_id = $1 AND id = $2")
        .bind(app_id)
        .bind(webhook_id)
        .bind(reason)
        .execute(&state.pool)
        .await?;
    invalidate_cache(state, app_id);
    Ok(())
}

pub async fn enable(state: &AppState, app_id: Uuid, webhook_id: Uuid) -> Result<()> {
    let mut dbtx = state.pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('webhooks'), hashtext($1::text))")
        .bind(app_id.to_string())
        .execute(&mut *dbtx)
        .await?;
    check_limit(&mut dbtx, app_id).await?;
    let existing = get_by_id(state, app_id, webhook_id).await?;
    check_duplicate(
        &mut dbtx,
        app_id,
        &existing.url,
        &existing.id_attr_ids,
        &existing.actions,
        Some(webhook_id),
    )
    .await?;
    sqlx::query("UPDATE webhooks SET status = 'active', disabled_reason = NULL WHERE app_id = $1 AND id = $2")
        .bind(app_id)
        .bind(webhook_id)
        .execute(&mut *dbtx)
        .await?;
    dbtx.commit().await?;
    invalidate_cache(state, app_id);
    Ok(())
}

pub async fn delete(state: &AppState, app_id: Uuid, webhook_id: Uuid) -> Result<()> {
    sqlx::query("DELETE FROM webhooks WHERE app_id = $1 AND id = $2")
        .bind(app_id)
        .bind(webhook_id)
        .execute(&state.pool)
        .await?;
    invalidate_cache(state, app_id);
    Ok(())
}

// ---------------------------------------------------------------------------
// events

pub struct WebhookEvent {
    pub isn: Isn,
    pub status: String,
    pub attempts: Value,
    pub next_attempt_after: Option<chrono::DateTime<chrono::Utc>>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

const EVENT_COLUMNS: &str = "webhook_id, app_id, (isn).slot_num AS slot_num, (isn).lsn::text AS lsn, status::text AS status,
    to_jsonb(attempts) AS attempts, coalesce(cardinality(attempts), 0) AS attempt_count,
    partition_bucket, next_attempt_after, created_at, updated_at";

fn event_from_row(r: &sqlx::postgres::PgRow) -> WebhookEvent {
    WebhookEvent {
        isn: Isn::from_row(r.get("slot_num"), &r.get::<String, _>("lsn")).unwrap_or(Isn::of_tx(0)),
        status: r.get("status"),
        attempts: r.get::<Option<Value>, _>("attempts").unwrap_or(Value::Null),
        next_attempt_after: r.get("next_attempt_after"),
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
    }
}

fn ts(t: Option<chrono::DateTime<chrono::Utc>>) -> Value {
    match t {
        Some(t) => json!(t.format("%Y-%m-%dT%H:%M:%SZ").to_string()),
        None => Value::Null,
    }
}

/// `webhook-event-row->response` with legacy's WebhookAttempt record keys
pub fn event_json(e: &WebhookEvent) -> Value {
    let attempts = match &e.attempts {
        Value::Array(a) => Value::Array(
            a.iter()
                .map(|x| {
                    json!({
                        "attempt-at": x.get("attempt_at").cloned().unwrap_or(Value::Null),
                        "duration-ms": x.get("duration_ms").cloned().unwrap_or(Value::Null),
                        "success?": x.get("success").cloned().unwrap_or(Value::Null),
                        "status-code": x.get("status_code").cloned().unwrap_or(Value::Null),
                        "response-text": x.get("response_text").cloned().unwrap_or(Value::Null),
                        "error-type": x.get("error_type").cloned().unwrap_or(Value::Null),
                        "error-message": x.get("error_message").cloned().unwrap_or(Value::Null),
                    })
                })
                .collect(),
        ),
        _ => Value::Null,
    };
    json!({
        "isn": e.isn.to_string_legacy(),
        "status": e.status,
        "attempts": attempts,
        "next_attempt_after": ts(e.next_attempt_after),
        "created_at": ts(Some(e.created_at)),
        "updated_at": ts(Some(e.updated_at)),
    })
}

/// `get-events`: newest first, cursor on (created_at, isn), 3 buckets / 60 days
pub async fn get_events(
    state: &AppState,
    app_id: Uuid,
    webhook_id: Uuid,
    after: Option<(chrono::DateTime<chrono::Utc>, Isn)>,
    limit: i64,
) -> Result<Vec<WebhookEvent>> {
    let buckets = recent_buckets(3);
    let floor = chrono::Utc::now() - chrono::Duration::days(60);
    let rows = sqlx::query(&format!(
        "SELECT {EVENT_COLUMNS} FROM webhook_events
          WHERE app_id = $1 AND webhook_id = $2 AND partition_bucket = ANY($3) AND created_at >= $4
            AND ($5::timestamptz IS NULL OR (created_at, isn) < ($5, ROW($6, $7::pg_lsn)::isn))
          ORDER BY created_at DESC, isn DESC LIMIT $8"
    ))
    .bind(app_id)
    .bind(webhook_id)
    .bind(&buckets)
    .bind(floor)
    .bind(after.map(|a| a.0))
    .bind(after.map(|a| a.1.slot).unwrap_or(0))
    .bind(
        after
            .map(|a| a.1.lsn_text())
            .unwrap_or_else(|| "0/0".into()),
    )
    .bind(limit)
    .fetch_all(&state.pool)
    .await?;
    Ok(rows.iter().map(event_from_row).collect())
}

pub async fn get_event(
    state: &AppState,
    app_id: Uuid,
    webhook_id: Uuid,
    isn: Isn,
) -> Result<Option<WebhookEvent>> {
    let buckets = recent_buckets(3);
    let row = sqlx::query(&format!(
        "SELECT {EVENT_COLUMNS} FROM webhook_events
          WHERE app_id = $1 AND webhook_id = $2 AND isn = ROW($3, $4::pg_lsn)::isn AND partition_bucket = ANY($5)"
    ))
    .bind(app_id)
    .bind(webhook_id)
    .bind(isn.slot)
    .bind(isn.lsn_text())
    .bind(&buckets)
    .fetch_optional(&state.pool)
    .await?;
    Ok(row.as_ref().map(event_from_row))
}

/// `requeue!`: back to pending unless a machine holds it; the row, or None
pub async fn requeue(
    state: &AppState,
    app_id: Uuid,
    webhook_id: Uuid,
    isn: Isn,
) -> Result<Option<WebhookEvent>> {
    let row = sqlx::query(&format!(
        "UPDATE webhook_events SET status = 'pending'
          WHERE app_id = $1 AND webhook_id = $2 AND isn = ROW($3, $4::pg_lsn)::isn AND machine_id IS NULL
          RETURNING {EVENT_COLUMNS}"
    ))
    .bind(app_id)
    .bind(webhook_id)
    .bind(isn.slot)
    .bind(isn.lsn_text())
    .fetch_optional(&state.pool)
    .await?;
    Ok(row.as_ref().map(event_from_row))
}

// ---------------------------------------------------------------------------
// event production (the transaction-path stand-in for legacy's WAL matching)

pub async fn ensure_tables(pool: &sqlx::PgPool) -> Result<()> {
    // every node caches an app's active webhooks (active_webhooks); a row
    // trigger evicts them all on any change, including psql edits
    sqlx::query(
        r#"
        CREATE OR REPLACE FUNCTION rust_notify_webhooks_changed() RETURNS trigger AS $fn$
        BEGIN
          PERFORM pg_notify('instant_webhooks',
                            json_build_object('app_id', coalesce(NEW.app_id, OLD.app_id))::text);
          RETURN NULL;
        END $fn$ LANGUAGE plpgsql
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query("DROP TRIGGER IF EXISTS rust_webhooks_changed_trigger ON webhooks")
        .execute(pool)
        .await?;
    sqlx::query(
        "CREATE TRIGGER rust_webhooks_changed_trigger AFTER INSERT OR UPDATE OR DELETE ON webhooks
         FOR EACH ROW EXECUTE FUNCTION rust_notify_webhooks_changed()",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rust_webhook_history (
           app_id uuid NOT NULL,
           tx_id bigint NOT NULL,
           entities jsonb NOT NULL,
           changes jsonb NOT NULL,
           created_at timestamptz NOT NULL DEFAULT now(),
           PRIMARY KEY (app_id, tx_id))",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Runs inside the transaction, after the steps and before commit (the
/// stand-in for legacy's WAL matching, model/webhook.clj `webhook-matches?`):
/// an event is queued for each active webhook whose namespaces had their id
/// attr's triple inserted (`create`), written again (`update`) or deleted
/// (`delete`) by this tx — never for ref / value attrs alone, so a link or
/// unlink step and the referrers of a deleted entity produce nothing, like
/// legacy. The snapshot the payload needs (cardinality-one triples of the
/// touched entities as this tx leaves them, plus the tx's triple changes)
/// is taken on the same connection, so a later tx can't leak into `after`,
/// and the history + event rows commit or roll back with the data. Returns
/// true when events were queued (the caller wakes the delivery loop after
/// the commit).
pub async fn queue_events(
    state: &AppState,
    conn: &mut sqlx::PgConnection,
    app_id: Uuid,
    attrs: &AttrMap,
    report: &TxReport,
) -> Result<bool> {
    if report.id_written.is_empty() && report.deleted.is_empty() && report.id_retracted.is_empty() {
        return Ok(false);
    }
    let hooks = active_webhooks(state, app_id).await?;
    if hooks.is_empty() {
        return Ok(false);
    }
    let created: HashSet<Uuid> = report.created.iter().map(|(e, _)| *e).collect();
    let deleted: HashSet<Uuid> = report.deleted.iter().map(|(e, _)| *e).collect();
    // (etype -> [(entity id, action)]) from the id-attr writes of this tx
    let mut by_etype: HashMap<String, Vec<(Uuid, &'static str)>> = HashMap::new();
    let mut push = |etype: &str, e: Uuid, action: &'static str| {
        let list = by_etype.entry(etype.to_string()).or_default();
        if !list.iter().any(|(x, _)| *x == e) {
            list.push((e, action));
        }
    };
    for (e, etype) in report.deleted.iter().chain(report.id_retracted.iter()) {
        push(etype, *e, "delete");
    }
    for (e, etype) in &report.id_written {
        if deleted.contains(e) {
            continue;
        }
        push(
            etype,
            *e,
            if created.contains(e) {
                "create"
            } else {
                "update"
            },
        );
    }
    let mut matched: Vec<&Webhook> = vec![];
    let mut wanted_etypes: HashSet<String> = HashSet::new();
    for w in hooks.iter() {
        let ns = namespaces(attrs, w);
        let hit = ns.iter().any(|n| {
            by_etype
                .get(n)
                .map(|ents| ents.iter().any(|(_, a)| w.actions.iter().any(|x| x == a)))
                .unwrap_or(false)
        });
        if hit {
            matched.push(w);
            wanted_etypes.extend(ns);
        }
    }
    if matched.is_empty() {
        return Ok(false);
    }
    // snapshot: the touched entities of the matched namespaces (cardinality-one
    // attrs, like legacy's entity log) plus this tx's triple changes
    let mut entity_ids: Vec<Uuid> = vec![];
    for (etype, ents) in &by_etype {
        if wanted_etypes.contains(etype) {
            entity_ids.extend(ents.iter().map(|(e, _)| *e));
        }
    }
    let one_attr_ids: Vec<Uuid> = attrs
        .iter()
        .filter(|a| a.cardinality == Cardinality::One && wanted_etypes.contains(&a.etype))
        .map(|a| a.id)
        .collect();
    let rows = sqlx::query(
        "SELECT entity_id, attr_id, value FROM triples
          WHERE app_id = $1 AND entity_id = ANY($2) AND attr_id = ANY($3) AND ea",
    )
    .bind(app_id)
    .bind(&entity_ids)
    .bind(&one_attr_ids)
    .fetch_all(&mut *conn)
    .await?;
    // {etype: {eid: {attr_id: value}}}
    let mut entities: Map<String, Value> = Map::new();
    for r in &rows {
        let e: Uuid = r.get("entity_id");
        let a: Uuid = r.get("attr_id");
        let v: Value = r.get("value");
        let Some(attr) = attrs.get(&a) else { continue };
        entities
            .entry(attr.etype.clone())
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .unwrap()
            .entry(e.to_string())
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .unwrap()
            .insert(a.to_string(), v);
    }
    // the change log is written by the triples trigger inside this tx, so
    // the rows are visible here in insertion order (the same physical order
    // legacy's `(reverse triple-changes)` walk relies on)
    let changes: Vec<Value> = sqlx::query(
        "SELECT entity_id, attr_id, value, action FROM rust_tx_changes
          WHERE app_id = $1 AND tx_id = $2 ORDER BY ctid",
    )
    .bind(app_id)
    .bind(report.tx_id)
    .fetch_all(&mut *conn)
    .await?
    .iter()
    .map(|r| {
        json!({
            "e": r.get::<Uuid, _>("entity_id"),
            "a": r.get::<Uuid, _>("attr_id"),
            "v": r.get::<Value, _>("value"),
            "action": r.get::<String, _>("action"),
        })
    })
    .collect();
    sqlx::query(
        "INSERT INTO rust_webhook_history (app_id, tx_id, entities, changes) VALUES ($1, $2, $3, $4)
         ON CONFLICT (app_id, tx_id) DO NOTHING",
    )
    .bind(app_id)
    .bind(report.tx_id)
    .bind(Value::Object(entities))
    .bind(Value::Array(changes))
    .execute(&mut *conn)
    .await?;
    let isn = Isn::of_tx(report.tx_id);
    let bucket = partition_bucket(chrono::Utc::now());
    for w in &matched {
        sqlx::query(
            "INSERT INTO webhook_events (webhook_id, isn, app_id, status, partition_bucket)
             VALUES ($1, ROW($2, $3::pg_lsn)::isn, $4, 'pending', $5)
             ON CONFLICT (webhook_id, isn, partition_bucket) DO NOTHING",
        )
        .bind(w.id)
        .bind(isn.slot)
        .bind(isn.lsn_text())
        .bind(app_id)
        .bind(bucket)
        .execute(&mut *conn)
        .await?;
    }
    Ok(true)
}

/// `rust_webhook_history` keeps the payload inputs for the events' 60-day
/// window; legacy truncates its bucketed history the same way
/// (webhooks/history.clj).
pub async fn prune_history(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::query("DELETE FROM rust_webhook_history WHERE created_at < now() - interval '60 days'")
        .execute(pool)
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// payload (webhook-data-for-wal-record)

fn uuid_from_sha(parts: &[&[u8]]) -> Uuid {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    for p in parts {
        h.update(p);
    }
    let d = h.finalize();
    Uuid::from_slice(&d[..16]).unwrap_or_default()
}

pub fn record_idempotency_key(namespace: &str, action: &str, id: Uuid, isn: Isn) -> Uuid {
    uuid_from_sha(&[
        namespace.as_bytes(),
        action.as_bytes(),
        id.as_bytes(),
        &isn.to_bytes(),
    ])
}

pub fn payload_idempotency_key(webhook_id: Uuid, isn: Isn) -> Uuid {
    uuid_from_sha(&[webhook_id.as_bytes(), &isn.to_bytes()])
}

/// `uuids->labels`: blob attrs by label
fn labels(attrs: &AttrMap, ent: &Map<String, Value>) -> Map<String, Value> {
    let mut out = Map::new();
    for (aid, v) in ent {
        if let Some(a) = Uuid::parse_str(aid).ok().and_then(|id| attrs.get(&id)) {
            if a.value_type == ValueType::Blob {
                out.insert(a.label.clone(), v.clone());
            }
        }
    }
    out
}

/// The records for one (webhook, isn): `[{namespace, action, id, before,
/// after, idempotencyKey}]`, or None when the tx is no longer in history.
pub async fn payload_records(
    state: &AppState,
    app_id: Uuid,
    webhook: &Webhook,
    isn: Isn,
) -> Result<Option<Vec<Value>>> {
    let row = sqlx::query(
        "SELECT entities, changes FROM rust_webhook_history WHERE app_id = $1 AND tx_id = $2",
    )
    .bind(app_id)
    .bind(isn.lsn)
    .fetch_optional(&state.pool)
    .await?;
    let Some(row) = row else { return Ok(None) };
    let attrs = service::load_attrs(state, app_id).await?;
    let ns: HashSet<String> = namespaces(&attrs, webhook).into_iter().collect();
    let wants = |a: &str| webhook.actions.iter().any(|x| x == a);
    let after: Map<String, Value> = row
        .get::<Value, _>("entities")
        .as_object()
        .cloned()
        .unwrap_or_default();
    let changes: Vec<Value> = row
        .get::<Value, _>("changes")
        .as_array()
        .cloned()
        .unwrap_or_default();
    // before = after with the tx's changes reversed (cardinality-one attrs only)
    let mut before: BTreeMap<String, BTreeMap<String, Map<String, Value>>> = BTreeMap::new();
    for (etype, ents) in &after {
        for (eid, ent) in ents.as_object().cloned().unwrap_or_default() {
            before
                .entry(etype.clone())
                .or_default()
                .insert(eid, ent.as_object().cloned().unwrap_or_default());
        }
    }
    let one_etype = |a: Uuid| {
        attrs
            .get(&a)
            .filter(|x| x.cardinality == Cardinality::One)
            .map(|x| x.etype.clone())
    };
    let mut removed: Vec<(String, String, String, Value)> = vec![];
    for c in &changes {
        let a = c
            .get("a")
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok());
        let e = c
            .get("e")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let (Some(a), Some(etype)) = (a, a.and_then(one_etype)) else {
            continue;
        };
        match c.get("action").and_then(|v| v.as_str()) {
            Some("added") => {
                if let Some(ent) = before.get_mut(&etype).and_then(|m| m.get_mut(&e)) {
                    ent.remove(&a.to_string());
                }
            }
            _ => removed.push((
                etype,
                e,
                a.to_string(),
                c.get("v").cloned().unwrap_or(Value::Null),
            )),
        }
    }
    for (etype, e, a, v) in removed {
        before
            .entry(etype)
            .or_default()
            .entry(e)
            .or_default()
            .insert(a, v);
    }
    // drop entities whose before-state ended up empty (created in this tx)
    for ents in before.values_mut() {
        ents.retain(|_, ent| !ent.is_empty());
    }
    let mut out = vec![];
    for (etype, ents) in &after {
        if !ns.contains(etype) {
            continue;
        }
        for (eid, ent) in ents.as_object().cloned().unwrap_or_default() {
            let ent_before = before.get(etype).and_then(|m| m.get(&eid));
            let action = if ent_before.is_some() {
                "update"
            } else {
                "create"
            };
            if !wants(action) {
                continue;
            }
            let Ok(id) = Uuid::parse_str(&eid) else {
                continue;
            };
            let after_labels = labels(&attrs, ent.as_object().unwrap_or(&Map::new()));
            let before_labels = ent_before.map(|b| labels(&attrs, b));
            if before_labels.as_ref() == Some(&after_labels) {
                continue;
            }
            out.push(finish_record(
                state,
                app_id,
                etype,
                action,
                id,
                before_labels,
                Some(after_labels),
                isn,
            ));
        }
    }
    for (etype, ents) in &before {
        if !ns.contains(etype) || !wants("delete") {
            continue;
        }
        for (eid, ent) in ents {
            let gone = after.get(etype).and_then(|m| m.get(eid)).is_none();
            if !gone {
                continue;
            }
            let Ok(id) = Uuid::parse_str(eid) else {
                continue;
            };
            out.push(finish_record(
                state,
                app_id,
                etype,
                "delete",
                id,
                Some(labels(&attrs, ent)),
                None,
                isn,
            ));
        }
    }
    Ok(Some(out))
}

#[allow(clippy::too_many_arguments)]
fn finish_record(
    state: &AppState,
    app_id: Uuid,
    namespace: &str,
    action: &str,
    id: Uuid,
    before: Option<Map<String, Value>>,
    after: Option<Map<String, Value>>,
    isn: Isn,
) -> Value {
    let add_url = |m: Option<Map<String, Value>>| -> Value {
        match m {
            None => Value::Null,
            Some(mut m) => {
                if namespace == "$files" {
                    if let Some(loc) = m
                        .get("location-id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                    {
                        m.insert(
                            "url".into(),
                            json!(crate::storage::download_url(state, app_id, &loc)),
                        );
                    }
                }
                Value::Object(m)
            }
        }
    };
    json!({
        "namespace": namespace,
        "action": action,
        "id": id,
        "before": add_url(before),
        "after": add_url(after),
        "idempotencyKey": record_idempotency_key(namespace, action, id, isn),
    })
}

// ---------------------------------------------------------------------------
// delivery (webhook_processor.clj + webhook_sender.clj)

struct Attempt {
    at: chrono::DateTime<chrono::Utc>,
    duration_ms: i32,
    success: bool,
    status_code: Option<i32>,
    response_text: Option<String>,
    error_type: Option<String>,
    error_message: Option<String>,
}

const MAX_ATTEMPTS: i64 = 11;

/// Stripe's schedule (model/webhook.clj:710-722)
fn retry_after(previous_attempts: i64) -> chrono::Duration {
    match previous_attempts {
        0 => chrono::Duration::minutes(1),
        1 => chrono::Duration::minutes(5),
        2 => chrono::Duration::minutes(30),
        3 => chrono::Duration::hours(2),
        4 => chrono::Duration::hours(5),
        5 => chrono::Duration::hours(10),
        _ => chrono::Duration::hours(12),
    }
}

/// Legacy peeks at most 256 bytes of the receiver's reply
/// (webhook_sender.clj); read no more than that plus a little slack, so a
/// hostile endpoint can't make the sender buffer an unbounded body.
const MAX_RESPONSE_BYTES: usize = 4096;

fn http_client(target: &ValidatedUrl) -> Option<reqwest::Client> {
    let mut b = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(20))
        .user_agent("InstantDB Webhook Sender");
    if !target.addrs.is_empty() {
        b = b.resolve_to_addrs(&target.host, &target.addrs);
    }
    b.build().ok()
}

async fn read_capped(resp: &mut reqwest::Response) -> String {
    let mut buf: Vec<u8> = vec![];
    while let Ok(Some(chunk)) = resp.chunk().await {
        buf.extend_from_slice(&chunk);
        if buf.len() >= MAX_RESPONSE_BYTES {
            break;
        }
    }
    buf.truncate(MAX_RESPONSE_BYTES);
    String::from_utf8_lossy(&buf).chars().take(256).collect()
}

async fn send_webhook(
    state: &AppState,
    url: &str,
    idempotency_key: Uuid,
    body: Vec<u8>,
) -> Attempt {
    let start = chrono::Utc::now();
    let started = std::time::Instant::now();
    let fail = |error_type: &str, error_message: &str| Attempt {
        at: start,
        duration_ms: started.elapsed().as_millis() as i32,
        success: false,
        status_code: None,
        response_text: None,
        error_type: Some(error_type.to_string()),
        error_message: Some(error_message.to_string()),
    };
    let target = match assert_valid_url(url).await {
        Ok(t) => t,
        Err(e) => return fail("unknown_host", &e.message),
    };
    let Ok((t, kid, sig)) = sign_body(state, &body) else {
        return fail("unknown", "Unknown error.");
    };
    let Some(client) = http_client(&target) else {
        return fail("unknown", "Unknown error.");
    };
    let res = client
        .post(url)
        .header("Content-Type", "application/json; charset=utf-8")
        .header("Instant-Signature", format!("t={t},kid={kid},v1={sig}"))
        .header("Idempotency-Key", idempotency_key.to_string())
        .body(body)
        .send()
        .await;
    match res {
        Ok(mut resp) => {
            let status = resp.status().as_u16() as i32;
            let success = resp.status().is_success();
            let text = read_capped(&mut resp).await;
            Attempt {
                at: start,
                duration_ms: started.elapsed().as_millis() as i32,
                success,
                status_code: Some(status),
                response_text: Some(text),
                error_type: None,
                error_message: None,
            }
        }
        Err(e) => {
            let (error_type, message) = if e.is_timeout() {
                ("timeout", "Request timed out.")
            } else if e.is_connect() {
                ("connect", "Could not connect.")
            } else if e.is_redirect() {
                ("redirect", "Unexpected redirect.")
            } else {
                ("network", "Network error.")
            };
            fail(error_type, message)
        }
    }
}

struct ClaimedEvent {
    webhook_id: Uuid,
    app_id: Uuid,
    isn: Isn,
    partition_bucket: i32,
    attempt_count: i64,
    base_time: chrono::DateTime<chrono::Utc>,
}

fn claimed_from_row(r: &sqlx::postgres::PgRow, base_col: &str) -> ClaimedEvent {
    ClaimedEvent {
        webhook_id: r.get("webhook_id"),
        app_id: r.get("app_id"),
        isn: Isn::from_row(r.get("slot_num"), &r.get::<String, _>("lsn")).unwrap_or(Isn::of_tx(0)),
        partition_bucket: r.get("partition_bucket"),
        attempt_count: r.get::<i32, _>("attempt_count") as i64,
        base_time: r
            .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(base_col)
            .ok()
            .flatten()
            .unwrap_or_else(chrono::Utc::now),
    }
}

async fn record_attempt(state: &AppState, ev: &ClaimedEvent, a: Attempt) -> Result<()> {
    let gone = a.status_code == Some(410);
    let status = if a.success {
        "success"
    } else if gone
        || a.error_type.as_deref() == Some("disabled")
        || ev.attempt_count + 1 >= MAX_ATTEMPTS
    {
        "failed"
    } else {
        "error"
    };
    let next = if status == "error" {
        Some(chrono::Utc::now() + retry_after(ev.attempt_count))
    } else {
        None
    };
    let res = sqlx::query(
        "UPDATE webhook_events
            SET status = $1::webhook_event_status,
                attempts = array_append(attempts, ROW($2::timestamptz, $3::int, $4::bool, $5::int, $6::text, $7::text, $8::text)::webhook_attempt),
                machine_id = NULL,
                next_attempt_after = $9
          WHERE webhook_id = $10 AND isn = ROW($11, $12::pg_lsn)::isn AND partition_bucket = $13 AND machine_id = $14",
    )
    .bind(status)
    .bind(a.at)
    .bind(a.duration_ms)
    .bind(a.success)
    .bind(a.status_code)
    .bind(&a.response_text)
    .bind(&a.error_type)
    .bind(&a.error_message)
    .bind(next)
    .bind(ev.webhook_id)
    .bind(ev.isn.slot)
    .bind(ev.isn.lsn_text())
    .bind(ev.partition_bucket)
    .bind(state.node_id)
    .execute(&state.pool)
    .await?;
    if gone && res.rows_affected() > 0 {
        disable(
            state,
            ev.app_id,
            ev.webhook_id,
            Some("Endpoint returned 410 status code."),
        )
        .await?;
    }
    Ok(())
}

async fn handle_event(state: &Arc<AppState>, ev: ClaimedEvent) -> Result<()> {
    let webhook = get_by_id(state, ev.app_id, ev.webhook_id).await?;
    if webhook.status != "active" {
        let a = Attempt {
            at: chrono::Utc::now(),
            duration_ms: 0,
            success: false,
            status_code: None,
            response_text: None,
            error_type: Some("disabled".into()),
            error_message: Some("Webhook is disabled.".into()),
        };
        return record_attempt(state, &ev, a).await;
    }
    let idempotency_key = payload_idempotency_key(ev.webhook_id, ev.isn);
    let payload_url = format!(
        "{}/webhooks/payload/{}/{}/{}",
        state.cfg.base_url,
        ev.app_id,
        ev.webhook_id,
        ev.isn.to_string_legacy()
    );
    let body = json!({
        "payloadUrl": payload_url,
        "token": payload_jwt(state, ev.app_id, ev.webhook_id, ev.isn)?,
    })
    .to_string()
    .into_bytes();
    let _latency_ms = (chrono::Utc::now() - ev.base_time).num_milliseconds();
    let attempt = send_webhook(state, &webhook.url, idempotency_key, body).await;
    record_attempt(state, &ev, attempt).await
}

async fn claim_pending(state: &AppState) -> Result<Vec<ClaimedEvent>> {
    let rows = sqlx::query(
        "SELECT webhook_id, app_id, (isn).slot_num AS slot_num, (isn).lsn::text AS lsn, partition_bucket, created_at,
                coalesce(cardinality(attempts), 0) AS attempt_count
           FROM claim_webhook_events($1, 10, 10)",
    )
    .bind(state.node_id)
    .fetch_all(&state.pool)
    .await?;
    Ok(rows
        .iter()
        .map(|r| claimed_from_row(r, "created_at"))
        .collect())
}

async fn claim_retries(state: &AppState) -> Result<Vec<ClaimedEvent>> {
    let buckets = recent_buckets(2);
    let rows = sqlx::query(
        "WITH locked AS (
           SELECT ctid, tableoid, partition_bucket, next_attempt_after FROM webhook_events
            WHERE status = 'error' AND next_attempt_after < now() AND partition_bucket = ANY($2)
            ORDER BY next_attempt_after ASC LIMIT 100 FOR UPDATE SKIP LOCKED)
         UPDATE webhook_events SET status = 'processing', machine_id = $1, next_attempt_after = NULL
           FROM locked
          WHERE webhook_events.ctid = locked.ctid AND webhook_events.tableoid = locked.tableoid
            AND webhook_events.partition_bucket = locked.partition_bucket
         RETURNING webhook_events.webhook_id, webhook_events.app_id,
                   (webhook_events.isn).slot_num AS slot_num, (webhook_events.isn).lsn::text AS lsn,
                   webhook_events.partition_bucket, locked.next_attempt_after AS base_time,
                   coalesce(cardinality(webhook_events.attempts), 0) AS attempt_count",
    )
    .bind(state.node_id)
    .bind(&buckets)
    .fetch_all(&state.pool)
    .await?;
    Ok(rows
        .iter()
        .map(|r| claimed_from_row(r, "base_time"))
        .collect())
}

async fn free_stuck(state: &AppState) -> Result<()> {
    sqlx::query(
        "UPDATE webhook_events SET status = 'pending', machine_id = NULL
          WHERE status = 'processing' AND updated_at < now() - interval '1 minute'",
    )
    .execute(&state.pool)
    .await?;
    Ok(())
}

/// How many deliveries a node runs at once, and how long one claim batch may
/// take before the rest is left for the next pass (legacy
/// webhook_processor.clj: `invokeAll` bounded to a minute, so `free_stuck`'s
/// one-minute rule can't re-hand a batch this node is still sending).
const DELIVERY_CONCURRENCY: usize = 20;
const BATCH_DEADLINE: Duration = Duration::from_secs(55);

async fn deliver_batch(state: &Arc<AppState>, events: Vec<ClaimedEvent>, what: &'static str) {
    let sem = Arc::new(tokio::sync::Semaphore::new(DELIVERY_CONCURRENCY));
    let mut tasks = vec![];
    for ev in events {
        let st = state.clone();
        let sem = sem.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = sem.acquire().await;
            if let Err(e) = handle_event(&st, ev).await {
                tracing::warn!("webhook {what} failed: {e}");
            }
        }));
    }
    let aborts: Vec<_> = tasks.iter().map(|t| t.abort_handle()).collect();
    let all = async {
        for t in tasks {
            let _ = t.await;
        }
    };
    if tokio::time::timeout(BATCH_DEADLINE, all).await.is_err() {
        // like invokeAll's timeout, cancel what's still running or waiting on
        // a permit: a detached send could otherwise land after free_stuck
        // has handed the event to another claim (a double delivery)
        for a in aborts {
            a.abort();
        }
        tracing::warn!("webhook {what} batch exceeded {BATCH_DEADLINE:?}; the rest is freed later");
    }
}

async fn work(state: &Arc<AppState>) {
    loop {
        let events = match claim_pending(state).await {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("webhook claim failed: {e}");
                return;
            }
        };
        let n = events.len();
        deliver_batch(state, events, "delivery").await;
        if n < 10 {
            break;
        }
    }
    loop {
        let events = match claim_retries(state).await {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("webhook retry claim failed: {e}");
                return;
            }
        };
        let n = events.len();
        deliver_batch(state, events, "retry").await;
        if n < 100 {
            break;
        }
    }
}

/// The delivery loop: wakes on local event production and every two minutes
/// (work queued by other nodes). A separate ticker frees stuck events and
/// prunes the payload history, so a busy node's steady stream of wake-ups
/// can't starve the sweep (legacy runs its kicker on its own schedule).
pub async fn run(state: Arc<AppState>) {
    {
        let state = state.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(120));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut n: u64 = 0;
            loop {
                tick.tick().await;
                if let Err(e) = free_stuck(&state).await {
                    tracing::warn!("webhook free-stuck failed: {e}");
                }
                n += 1;
                if n.is_multiple_of(30) {
                    if let Err(e) = prune_history(&state.pool).await {
                        tracing::warn!("webhook history prune failed: {e}");
                    }
                }
                state.webhook_notify.notify_one();
            }
        });
    }
    loop {
        state.webhook_notify.notified().await;
        work(&state).await;
    }
}
