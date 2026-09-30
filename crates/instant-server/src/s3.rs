//! Minimal S3 client for the `s3` storage backend (issue #9): AWS Signature
//! Version 4 over the existing reqwest client, no aws-sdk. Speaks exactly the
//! subset the blob store needs — PutObject / GetObject / HeadObject /
//! DeleteObject plus presigned GET URLs — against AWS S3, Cloudflare R2,
//! MinIO and other S3-compatible stores.
//!
//! Configuration (env; the names match the legacy server so an existing
//! self-hosted deployment's settings carry over unchanged):
//! - `S3_BUCKET` (required), `AWS_REGION` (default `us-east-1`)
//! - `S3_ENDPOINT`: custom endpoint (MinIO, R2, ...). Like legacy, setting it
//!   forces path-style addressing; `S3_FORCE_PATH_STYLE=0|1` overrides.
//! - `S3_PUBLIC_ENDPOINT`: endpoint browsers reach for presigned URLs
//!   (default `S3_ENDPOINT`)
//! - `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (+ `AWS_SESSION_TOKEN`);
//!   when unset, credentials come from the ECS/EKS container endpoint
//!   (`AWS_CONTAINER_CREDENTIALS_*`) or EC2 IMDSv2 and are refreshed ahead
//!   of expiry in the background.
//! - `S3_PRESIGN_ACCESS_KEY_ID` / `S3_PRESIGN_SECRET_ACCESS_KEY`: optional
//!   long-lived keys used only to presign download URLs (legacy's
//!   `presign-creds`: a role's temporary credentials cap presigned-URL
//!   lifetime at the credential lifetime, well under the 7-day URL ttl).
//!
//! Signing follows the legacy `instant.util.aws-signature` port line for
//! line so presigned URLs come out byte-identical for identical inputs.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use instant_core::error::{InstantError, Result};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, RwLock};

type HmacSha256 = Hmac<Sha256>;

pub const SIG_ALGORITHM: &str = "AWS4-HMAC-SHA256";
pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
pub const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Signed-URL lifetime (legacy `Duration/ofDays 7`).
pub const PRESIGN_EXPIRES_SECS: u64 = 7 * 86_400;
/// Query param legacy adds to every presigned GET so S3 answers with a
/// cacheable response (objects are immutable per location-id).
pub const RESPONSE_CACHE_CONTROL: &str = "public, max-age=86400, immutable";

#[derive(Debug, Clone)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
    /// None for static keys.
    pub expires_at: Option<DateTime<Utc>>,
}

impl Credentials {
    fn needs_refresh(&self) -> bool {
        match self.expires_at {
            Some(exp) => exp - Utc::now() < chrono::Duration::minutes(5),
            None => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<url::Url>,
    pub public_endpoint: Option<url::Url>,
    pub path_style: bool,
    /// Presign `$files.url` (legacy behavior) instead of proxying through
    /// `/storage/serve`.
    pub presign: bool,
}

impl S3Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let bucket = std::env::var("S3_BUCKET")
            .ok()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow::anyhow!("STORAGE_BACKEND=s3 requires S3_BUCKET"))?;
        let region = ["AWS_REGION", "AWS_DEFAULT_REGION", "S3_REGION"]
            .iter()
            .find_map(|k| std::env::var(k).ok().filter(|s| !s.is_empty()))
            .unwrap_or_else(|| "us-east-1".to_string());
        let parse = |name: &str| -> anyhow::Result<Option<url::Url>> {
            match std::env::var(name).ok().filter(|s| !s.is_empty()) {
                Some(s) => Ok(Some(
                    url::Url::parse(&s).map_err(|e| anyhow::anyhow!("{name}: {e}"))?,
                )),
                None => Ok(None),
            }
        };
        let endpoint = parse("S3_ENDPOINT")?;
        let public_endpoint = parse("S3_PUBLIC_ENDPOINT")?.or_else(|| endpoint.clone());
        let path_style = match std::env::var("S3_FORCE_PATH_STYLE").as_deref() {
            Ok("1") | Ok("true") => true,
            Ok("0") | Ok("false") => false,
            _ => endpoint.is_some(),
        };
        let presign = !matches!(
            std::env::var("S3_PRESIGN").as_deref(),
            Ok("0") | Ok("false") | Ok("off")
        );
        Ok(S3Config {
            bucket,
            region,
            endpoint,
            public_endpoint,
            path_style,
            presign,
        })
    }
}

// ---------------------------------------------------------------------------
// SigV4 primitives (port of LEGACY/server/src/instant/util/aws_signature.clj)

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn sha256_hex(data: &[u8]) -> String {
    hex(&Sha256::digest(data))
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// AWS-style percent encoding: unreserved chars `A-Z a-z 0-9 - _ . ~` pass,
/// everything else (including `/` when `encode_slash`) becomes `%XX`.
pub fn uri_encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b'/' if !encode_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Canonical URI: each path segment percent-encoded once.
pub fn canonical_path(path: &str) -> String {
    path.split('/')
        .map(|seg| uri_encode(seg, true))
        .collect::<Vec<_>>()
        .join("/")
}

/// Canonical query string: encoded pairs sorted by key then value.
pub fn canonical_query(query: &[(String, String)]) -> String {
    let mut pairs: Vec<(String, String)> = query
        .iter()
        .map(|(k, v)| (uri_encode(k, true), uri_encode(v, true)))
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

pub fn amz_date(t: DateTime<Utc>) -> String {
    t.format("%Y%m%dT%H%M%SZ").to_string()
}

pub fn amz_short_date(t: DateTime<Utc>) -> String {
    t.format("%Y%m%d").to_string()
}

pub fn credential_scope(t: DateTime<Utc>, region: &str) -> String {
    format!("{}/{}/s3/aws4_request", amz_short_date(t), region)
}

fn signing_key(secret_key: &str, t: DateTime<Utc>, region: &str) -> Vec<u8> {
    let k_date = hmac(
        format!("AWS4{secret_key}").as_bytes(),
        amz_short_date(t).as_bytes(),
    );
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, b"s3");
    hmac(&k_service, b"aws4_request")
}

/// Everything the signature depends on. `headers` must be lowercase-keyed
/// and trimmed (BTreeMap keeps them sorted, as the canonical form needs).
pub struct SigRequest<'a> {
    pub method: &'a str,
    /// Raw (unencoded) absolute path, e.g. `/bucket/app/3/loc`.
    pub path: &'a str,
    pub query: &'a [(String, String)],
    pub headers: &'a BTreeMap<String, String>,
    pub payload_hash: &'a str,
    pub signing_instant: DateTime<Utc>,
    pub region: &'a str,
}

pub fn signed_headers(headers: &BTreeMap<String, String>) -> String {
    headers.keys().cloned().collect::<Vec<_>>().join(";")
}

pub fn canonical_request(r: &SigRequest) -> String {
    let mut s = String::new();
    s.push_str(&r.method.to_uppercase());
    s.push('\n');
    s.push_str(&canonical_path(r.path));
    s.push('\n');
    s.push_str(&canonical_query(r.query));
    s.push('\n');
    for (k, v) in r.headers {
        s.push_str(k);
        s.push(':');
        s.push_str(v);
        s.push('\n');
    }
    s.push('\n');
    s.push_str(&signed_headers(r.headers));
    s.push('\n');
    s.push_str(r.payload_hash);
    s
}

pub fn string_to_sign(r: &SigRequest) -> String {
    format!(
        "{}\n{}\n{}\n{}",
        SIG_ALGORITHM,
        amz_date(r.signing_instant),
        credential_scope(r.signing_instant, r.region),
        sha256_hex(canonical_request(r).as_bytes())
    )
}

pub fn signature(r: &SigRequest, secret_key: &str) -> String {
    let key = signing_key(secret_key, r.signing_instant, r.region);
    hex(&hmac(&key, string_to_sign(r).as_bytes()))
}

/// Presigned GET URL exactly as legacy `presign-s3-url` builds it.
pub fn presign_get_url(
    creds: &Credentials,
    region: &str,
    scheme: &str,
    host: &str,
    url_path: &str,
    signing_instant: DateTime<Utc>,
    expires_secs: u64,
) -> String {
    let mut query: Vec<(String, String)> = vec![
        ("X-Amz-Algorithm".into(), SIG_ALGORITHM.into()),
        (
            "X-Amz-Credential".into(),
            format!(
                "{}/{}",
                creds.access_key,
                credential_scope(signing_instant, region)
            ),
        ),
        ("X-Amz-Date".into(), amz_date(signing_instant)),
        ("X-Amz-Expires".into(), expires_secs.to_string()),
        ("X-Amz-SignedHeaders".into(), "host".into()),
        (
            "response-cache-control".into(),
            RESPONSE_CACHE_CONTROL.into(),
        ),
    ];
    if let Some(tok) = &creds.session_token {
        query.push(("X-Amz-Security-Token".into(), tok.clone()));
    }
    let mut headers = BTreeMap::new();
    headers.insert("host".to_string(), host.to_string());
    let sig = signature(
        &SigRequest {
            method: "GET",
            path: url_path,
            query: &query,
            headers: &headers,
            payload_hash: UNSIGNED_PAYLOAD,
            signing_instant,
            region,
        },
        &creds.secret_key,
    );
    query.push(("X-Amz-Signature".into(), sig));
    format!(
        "{scheme}://{host}{}?{}",
        canonical_path(url_path),
        canonical_query(&query)
    )
}

/// Legacy `location-id->bin`: `(mod (Math/abs (.hashCode location-id)) 10)`
/// with Java's 32-bit wrapping `String.hashCode` and Clojure's floor `mod`.
/// Keeping this exact means a bucket written by the legacy server is served
/// as-is.
pub fn java_hash_bin(s: &str) -> u32 {
    let mut h: i32 = 0;
    for unit in s.encode_utf16() {
        h = h.wrapping_mul(31).wrapping_add(unit as i32);
    }
    h.wrapping_abs().rem_euclid(10) as u32
}

/// Object keys have the shape `app-id/bin/location-id` (legacy `->object-key`).
pub fn object_key(app_id: uuid::Uuid, location_id: &str) -> String {
    format!("{app_id}/{}/{location_id}", java_hash_bin(location_id))
}

// ---------------------------------------------------------------------------
// client

pub struct S3Client {
    pub cfg: S3Config,
    http: reqwest::Client,
    static_creds: Option<Credentials>,
    presign_creds: Option<Credentials>,
    dynamic: RwLock<Option<Credentials>>,
    refresh_lock: Mutex<()>,
}

fn host_header(u: &url::Url) -> String {
    let host = u.host_str().unwrap_or_default();
    match u.port() {
        Some(p) => format!("{host}:{p}"),
        None => host.to_string(),
    }
}

impl S3Client {
    pub fn from_env(cfg: S3Config) -> Self {
        let env = |k: &str| std::env::var(k).ok().filter(|s| !s.is_empty());
        let static_creds = match (env("AWS_ACCESS_KEY_ID"), env("AWS_SECRET_ACCESS_KEY")) {
            (Some(access_key), Some(secret_key)) => Some(Credentials {
                access_key,
                secret_key,
                session_token: env("AWS_SESSION_TOKEN"),
                expires_at: None,
            }),
            _ => None,
        };
        let presign_creds = match (
            env("S3_PRESIGN_ACCESS_KEY_ID"),
            env("S3_PRESIGN_SECRET_ACCESS_KEY"),
        ) {
            (Some(access_key), Some(secret_key)) => Some(Credentials {
                access_key,
                secret_key,
                session_token: None,
                expires_at: None,
            }),
            _ => None,
        };
        S3Client {
            cfg,
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(300))
                .build()
                .expect("reqwest client"),
            static_creds,
            presign_creds,
            dynamic: RwLock::new(None),
            refresh_lock: Mutex::new(()),
        }
    }

    /// Endpoint used for API calls: (scheme, host header, path prefix).
    fn api_base(&self) -> (String, String, String) {
        self.base(self.cfg.endpoint.as_ref())
    }

    fn base(&self, endpoint: Option<&url::Url>) -> (String, String, String) {
        match endpoint {
            Some(u) => {
                let prefix = u.path().trim_end_matches('/').to_string();
                (u.scheme().to_string(), host_header(u), prefix)
            }
            None => {
                let host = if self.cfg.path_style {
                    format!("s3.{}.amazonaws.com", self.cfg.region)
                } else {
                    format!("{}.s3.{}.amazonaws.com", self.cfg.bucket, self.cfg.region)
                };
                ("https".to_string(), host, String::new())
            }
        }
    }

    /// Raw absolute path of an object on the wire (bucket folded in for
    /// path-style addressing).
    fn object_path(&self, prefix: &str, key: &str) -> String {
        if self.cfg.path_style {
            format!("{prefix}/{}/{key}", self.cfg.bucket)
        } else {
            format!("{prefix}/{key}")
        }
    }

    // -- credentials --------------------------------------------------------

    /// Credentials for signing API calls, refreshed if they are about to expire.
    pub async fn credentials(&self) -> Result<Credentials> {
        if let Some(c) = &self.static_creds {
            return Ok(c.clone());
        }
        if let Some(c) = self.dynamic.read().await.as_ref() {
            if !c.needs_refresh() {
                return Ok(c.clone());
            }
        }
        let _guard = self.refresh_lock.lock().await;
        // another task may have refreshed while we waited
        if let Some(c) = self.dynamic.read().await.as_ref() {
            if !c.needs_refresh() {
                return Ok(c.clone());
            }
        }
        let fresh = fetch_dynamic_credentials(&self.http).await?;
        *self.dynamic.write().await = Some(fresh.clone());
        Ok(fresh)
    }

    /// Credentials available right now without I/O (presigning happens
    /// inside synchronous query-result shaping). None until the first
    /// refresh has landed for role-based setups.
    pub fn cached_credentials(&self) -> Option<Credentials> {
        if let Some(c) = &self.static_creds {
            return Some(c.clone());
        }
        self.dynamic.try_read().ok().and_then(|c| c.clone())
    }

    /// Warm the credential cache and keep it fresh; spawned at boot for
    /// role-based setups (static keys never expire).
    pub async fn refresh_loop(self: Arc<Self>) {
        if self.static_creds.is_some() {
            return;
        }
        loop {
            let delay = match self.credentials().await {
                Ok(c) => {
                    let until = c
                        .expires_at
                        .map(|e| (e - Utc::now()).num_seconds() - 6 * 60)
                        .unwrap_or(3600)
                        .clamp(30, 3600);
                    tokio::time::Duration::from_secs(until as u64)
                }
                Err(e) => {
                    tracing::warn!("s3 credential refresh failed: {e}");
                    tokio::time::Duration::from_secs(30)
                }
            };
            tokio::time::sleep(delay).await;
        }
    }

    // -- signed requests ----------------------------------------------------

    async fn signed(
        &self,
        method: reqwest::Method,
        key: &str,
        body: Option<Vec<u8>>,
        extra_headers: &[(&str, &str)],
    ) -> Result<reqwest::Response> {
        let creds = self.credentials().await?;
        let (scheme, host, prefix) = self.api_base();
        let path = self.object_path(&prefix, key);
        let now = Utc::now();
        let payload_hash = match &body {
            Some(b) => sha256_hex(b),
            None => EMPTY_SHA256.to_string(),
        };
        let mut headers: BTreeMap<String, String> = BTreeMap::new();
        headers.insert("host".into(), host.clone());
        headers.insert("x-amz-content-sha256".into(), payload_hash.clone());
        headers.insert("x-amz-date".into(), amz_date(now));
        if let Some(tok) = &creds.session_token {
            headers.insert("x-amz-security-token".into(), tok.clone());
        }
        for (k, v) in extra_headers {
            headers.insert(k.to_lowercase(), v.trim().to_string());
        }
        let query: Vec<(String, String)> = vec![];
        let sig = signature(
            &SigRequest {
                method: method.as_str(),
                path: &path,
                query: &query,
                headers: &headers,
                payload_hash: &payload_hash,
                signing_instant: now,
                region: &self.cfg.region,
            },
            &creds.secret_key,
        );
        let authorization = format!(
            "{} Credential={}/{}, SignedHeaders={}, Signature={}",
            SIG_ALGORITHM,
            creds.access_key,
            credential_scope(now, &self.cfg.region),
            signed_headers(&headers),
            sig
        );
        let url = format!("{scheme}://{host}{}", canonical_path(&path));
        let mut req = self.http.request(method, &url);
        for (k, v) in &headers {
            if k != "host" {
                req = req.header(k.as_str(), v.as_str());
            }
        }
        req = req.header("authorization", authorization);
        if let Some(b) = body {
            req = req.body(b);
        }
        req.send()
            .await
            .map_err(|e| InstantError::internal(format!("s3 request failed: {e}")))
    }

    async fn fail(op: &str, resp: reqwest::Response) -> InstantError {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        InstantError::internal(format!(
            "s3 {op} failed: {status} {}",
            text.chars().take(300).collect::<String>()
        ))
    }

    pub async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: &str,
        content_disposition: &str,
    ) -> Result<()> {
        let len = bytes.len().to_string();
        let resp = self
            .signed(
                reqwest::Method::PUT,
                key,
                Some(bytes),
                &[
                    ("content-type", content_type),
                    ("content-disposition", content_disposition),
                    ("content-length", &len),
                ],
            )
            .await?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(Self::fail("put", resp).await)
        }
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let resp = self.signed(reqwest::Method::GET, key, None, &[]).await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(Self::fail("get", resp).await);
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| InstantError::internal(format!("s3 get body: {e}")))?;
        Ok(Some(bytes.to_vec()))
    }

    /// Content-Length of an object, None when it does not exist.
    pub async fn head(&self, key: &str) -> Result<Option<i64>> {
        let resp = self.signed(reqwest::Method::HEAD, key, None, &[]).await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(Self::fail("head", resp).await);
        }
        Ok(Some(
            resp.headers()
                .get(reqwest::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
        ))
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let resp = self.signed(reqwest::Method::DELETE, key, None, &[]).await?;
        // 204 on success; a missing key is also 204 on S3
        if resp.status().is_success() || resp.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(Self::fail("delete", resp).await)
        }
    }

    /// Presigned GET against the public endpoint, or None when no
    /// credentials are cached yet (caller falls back to proxying).
    pub fn presigned_get(&self, key: &str, signing_instant: DateTime<Utc>) -> Option<String> {
        let creds = self
            .presign_creds
            .clone()
            .or_else(|| self.cached_credentials())?;
        let (scheme, host, prefix) = self.base(self.cfg.public_endpoint.as_ref());
        let path = self.object_path(&prefix, key);
        Some(presign_get_url(
            &creds,
            &self.cfg.region,
            &scheme,
            &host,
            &path,
            signing_instant,
            PRESIGN_EXPIRES_SECS,
        ))
    }
}

// ---------------------------------------------------------------------------
// role-based credentials: ECS/EKS container endpoint, then EC2 IMDSv2

#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct MetadataCreds {
    access_key_id: String,
    secret_access_key: String,
    token: Option<String>,
    expiration: Option<String>,
}

impl MetadataCreds {
    fn into_credentials(self) -> Credentials {
        Credentials {
            access_key: self.access_key_id,
            secret_key: self.secret_access_key,
            session_token: self.token,
            expires_at: self
                .expiration
                .as_deref()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|d| d.with_timezone(&Utc)),
        }
    }
}

async fn fetch_dynamic_credentials(http: &reqwest::Client) -> Result<Credentials> {
    let env = |k: &str| std::env::var(k).ok().filter(|s| !s.is_empty());
    let short = std::time::Duration::from_secs(3);

    // ECS task role / EKS pod identity
    let container_uri = env("AWS_CONTAINER_CREDENTIALS_FULL_URI").or_else(|| {
        env("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI").map(|p| format!("http://169.254.170.2{p}"))
    });
    if let Some(uri) = container_uri {
        let token = env("AWS_CONTAINER_AUTHORIZATION_TOKEN").or_else(|| {
            env("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE")
                .and_then(|f| std::fs::read_to_string(f).ok())
                .map(|s| s.trim().to_string())
        });
        let mut req = http.get(&uri).timeout(short);
        if let Some(t) = token {
            req = req.header("authorization", t);
        }
        let creds: MetadataCreds = req
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(|e| InstantError::internal(format!("container credentials: {e}")))?
            .json()
            .await
            .map_err(|e| InstantError::internal(format!("container credentials: {e}")))?;
        return Ok(creds.into_credentials());
    }

    if env("AWS_EC2_METADATA_DISABLED").as_deref() == Some("true") {
        return Err(InstantError::internal(
            "no S3 credentials: set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY",
        ));
    }
    let imds = env("AWS_EC2_METADATA_SERVICE_ENDPOINT")
        .unwrap_or_else(|| "http://169.254.169.254".to_string());
    let imds = imds.trim_end_matches('/').to_string();
    let token = http
        .put(format!("{imds}/latest/api/token"))
        .header("x-aws-ec2-metadata-token-ttl-seconds", "21600")
        .timeout(short)
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .map_err(|e| InstantError::internal(format!("imds token: {e}")))?
        .text()
        .await
        .map_err(|e| InstantError::internal(format!("imds token: {e}")))?;
    let base = format!("{imds}/latest/meta-data/iam/security-credentials/");
    let role = http
        .get(&base)
        .header("x-aws-ec2-metadata-token", &token)
        .timeout(short)
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .map_err(|e| InstantError::internal(format!("imds role: {e}")))?
        .text()
        .await
        .map_err(|e| InstantError::internal(format!("imds role: {e}")))?;
    let role = role.lines().next().unwrap_or_default().trim().to_string();
    let creds: MetadataCreds = http
        .get(format!("{base}{role}"))
        .header("x-aws-ec2-metadata-token", &token)
        .timeout(short)
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .map_err(|e| InstantError::internal(format!("imds credentials: {e}")))?
        .json()
        .await
        .map_err(|e| InstantError::internal(format!("imds credentials: {e}")))?;
    Ok(creds.into_credentials())
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    // Vectors from the S3 developer guide ("Authenticating Requests: Using
    // Query Parameters" / "Signature Calculations for the Authorization
    // Header: Transferring Payload in a Single Chunk"): access key
    // AKIAIOSFODNN7EXAMPLE, secret wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY,
    // bucket examplebucket, us-east-1, 20130524T000000Z.
    fn creds() -> Credentials {
        Credentials {
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            session_token: None,
            expires_at: None,
        }
    }
    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2013, 5, 24, 0, 0, 0).unwrap()
    }

    #[test]
    fn presigned_get_matches_aws_example() {
        let url = presign_get_url(
            &creds(),
            "us-east-1",
            "https",
            "examplebucket.s3.amazonaws.com",
            "/test.txt",
            t0(),
            86_400,
        );
        // legacy adds response-cache-control; strip it to compare with the
        // AWS example, which signs only the five standard params
        let mut query = vec![
            ("X-Amz-Algorithm".to_string(), SIG_ALGORITHM.to_string()),
            (
                "X-Amz-Credential".to_string(),
                "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request".to_string(),
            ),
            ("X-Amz-Date".to_string(), "20130524T000000Z".to_string()),
            ("X-Amz-Expires".to_string(), "86400".to_string()),
            ("X-Amz-SignedHeaders".to_string(), "host".to_string()),
        ];
        let mut headers = BTreeMap::new();
        headers.insert(
            "host".to_string(),
            "examplebucket.s3.amazonaws.com".to_string(),
        );
        let sig = signature(
            &SigRequest {
                method: "GET",
                path: "/test.txt",
                query: &query,
                headers: &headers,
                payload_hash: UNSIGNED_PAYLOAD,
                signing_instant: t0(),
                region: "us-east-1",
            },
            &creds().secret_key,
        );
        assert_eq!(
            sig,
            "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
        );
        query.push(("X-Amz-Signature".into(), sig));
        // and the full legacy-shaped URL carries every param, sorted
        assert!(url.starts_with("https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-Signature="));
        assert!(url.ends_with("&X-Amz-SignedHeaders=host&response-cache-control=public%2C%20max-age%3D86400%2C%20immutable"));
    }

    #[test]
    fn header_auth_get_matches_aws_example() {
        let mut headers = BTreeMap::new();
        headers.insert(
            "host".to_string(),
            "examplebucket.s3.amazonaws.com".to_string(),
        );
        headers.insert("range".to_string(), "bytes=0-9".to_string());
        headers.insert("x-amz-content-sha256".to_string(), EMPTY_SHA256.to_string());
        headers.insert("x-amz-date".to_string(), "20130524T000000Z".to_string());
        let sig = signature(
            &SigRequest {
                method: "GET",
                path: "/test.txt",
                query: &[],
                headers: &headers,
                payload_hash: EMPTY_SHA256,
                signing_instant: t0(),
                region: "us-east-1",
            },
            &creds().secret_key,
        );
        assert_eq!(
            sig,
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn header_auth_put_matches_aws_example() {
        let body = b"Welcome to Amazon S3.";
        let hash = sha256_hex(body);
        assert_eq!(
            hash,
            "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"
        );
        let mut headers = BTreeMap::new();
        headers.insert(
            "date".to_string(),
            "Fri, 24 May 2013 00:00:00 GMT".to_string(),
        );
        headers.insert(
            "host".to_string(),
            "examplebucket.s3.amazonaws.com".to_string(),
        );
        headers.insert("x-amz-content-sha256".to_string(), hash.clone());
        headers.insert("x-amz-date".to_string(), "20130524T000000Z".to_string());
        headers.insert(
            "x-amz-storage-class".to_string(),
            "REDUCED_REDUNDANCY".to_string(),
        );
        let sig = signature(
            &SigRequest {
                method: "PUT",
                path: "/test$file.text",
                query: &[],
                headers: &headers,
                payload_hash: &hash,
                signing_instant: t0(),
                region: "us-east-1",
            },
            &creds().secret_key,
        );
        assert_eq!(
            sig,
            "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
        );
    }

    #[test]
    fn uri_encoding_matches_legacy_url_encode() {
        assert_eq!(uri_encode("a b*c~d/e", false), "a%20b%2Ac~d/e");
        assert_eq!(uri_encode("a/b", true), "a%2Fb");
        assert_eq!(canonical_path("/test$file.text"), "/test%24file.text");
        assert_eq!(
            canonical_query(&[("b".into(), "2".into()), ("a".into(), "x y".into())]),
            "a=x%20y&b=2"
        );
    }

    #[test]
    fn object_key_uses_legacy_java_hash_bin() {
        // Java String.hashCode values: "hello" = 99162322, "" = 0,
        // "polygenelubricants" = Integer.MIN_VALUE (abs stays negative,
        // Clojure mod floors to 2)
        assert_eq!(java_hash_bin("hello"), 2);
        assert_eq!(java_hash_bin(""), 0);
        assert_eq!(java_hash_bin("3f2504e0-4f89-11d3-9a0c-0305e82c3301"), 9);
        assert_eq!(
            java_hash_bin("stream-3f2504e0-4f89-11d3-9a0c-0305e82c3301"),
            6
        );
        assert_eq!(java_hash_bin("polygenelubricants"), 2);
        let app = uuid::Uuid::parse_str("0e4a1c3e-6d4f-4c33-9a2a-4d9f3e2b1c00").unwrap();
        assert_eq!(
            object_key(app, "3f2504e0-4f89-11d3-9a0c-0305e82c3301"),
            "0e4a1c3e-6d4f-4c33-9a2a-4d9f3e2b1c00/9/3f2504e0-4f89-11d3-9a0c-0305e82c3301"
        );
    }
}
