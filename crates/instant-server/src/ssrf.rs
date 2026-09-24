//! SSRF guard for server-side fetches of user-supplied URLs (legacy
//! smokescreen.clj / webhook_sender.clj). A host is refused when it resolves
//! to any private, loopback, link-local or otherwise non-public address, and
//! the request is pinned to the vetted addresses so a DNS answer that changes
//! between the check and the connect can't reach an internal network.
//! Redirects are never followed.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use serde_json::Value;

/// Upper bound on a guarded JSON response (discovery documents, JWKS, token
/// and userinfo responses are all a few KB).
const MAX_JSON_BYTES: usize = 1024 * 1024;
const TIMEOUT: Duration = Duration::from_secs(10);

pub fn bad_ip(ip: IpAddr) -> bool {
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

/// A URL's host and the public addresses it resolved to. `addrs` is empty
/// when private targets are allowed (tests): the client resolves normally.
pub struct Vetted {
    pub host: String,
    pub addrs: Vec<SocketAddr>,
}

/// Resolve `url`'s host and refuse it unless every answer is public.
pub async fn vet(url: &str, allow_private: bool) -> Result<Vetted, String> {
    let parsed = url::Url::parse(url).map_err(|_| format!("Invalid URL: {url}"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(format!("Unsupported URL scheme: {url}"));
    }
    let host = parsed
        .host_str()
        .filter(|h| !h.is_empty())
        .ok_or_else(|| format!("Invalid URL: {url}"))?
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_string();
    if allow_private {
        return Ok(Vetted {
            host,
            addrs: vec![],
        });
    }
    let port = parsed.port_or_known_default().unwrap_or(443);
    let addrs: Vec<SocketAddr> = match host.parse::<IpAddr>() {
        Ok(ip) => vec![SocketAddr::new(ip, port)],
        Err(_) => {
            match tokio::time::timeout(TIMEOUT, tokio::net::lookup_host((host.as_str(), port)))
                .await
            {
                Ok(Ok(a)) => a.collect(),
                _ => vec![],
            }
        }
    };
    if addrs.is_empty() || addrs.iter().any(|a| bad_ip(a.ip())) {
        return Err(format!(
            "Refusing to fetch {url}: it does not resolve to a public address."
        ));
    }
    Ok(Vetted { host, addrs })
}

fn client(v: &Vetted) -> Result<reqwest::Client, String> {
    let mut b = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(TIMEOUT);
    if !v.addrs.is_empty() {
        b = b.resolve_to_addrs(&v.host, &v.addrs);
    }
    b.build().map_err(|e| format!("http client: {e}"))
}

async fn read_json(url: &str, mut resp: reqwest::Response) -> Result<Value, String> {
    let mut buf: Vec<u8> = vec![];
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| format!("Failed to read {url}: {e}"))?
    {
        buf.extend_from_slice(&chunk);
        if buf.len() > MAX_JSON_BYTES {
            return Err(format!("Response from {url} is too large."));
        }
    }
    serde_json::from_slice(&buf).map_err(|e| format!("Invalid JSON from {url}: {e}"))
}

/// Guarded GET of a JSON document, optionally with a bearer token.
pub async fn get_json(
    url: &str,
    bearer: Option<&str>,
    allow_private: bool,
) -> Result<Value, String> {
    let v = vet(url, allow_private).await?;
    let mut req = client(&v)?.get(url);
    if let Some(token) = bearer {
        req = req.bearer_auth(token);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| format!("Failed to fetch {url}: {e}"))?;
    read_json(url, resp).await
}

/// Guarded form POST returning JSON (OAuth token exchange).
pub async fn post_form_json(
    url: &str,
    form: &[(&str, &str)],
    allow_private: bool,
) -> Result<Value, String> {
    let v = vet(url, allow_private).await?;
    let resp = client(&v)?
        .post(url)
        .form(form)
        .send()
        .await
        .map_err(|e| format!("Failed to fetch {url}: {e}"))?;
    read_json(url, resp).await
}
