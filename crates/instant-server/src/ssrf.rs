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
                || (o[0] == 192 && o[1] == 0 && o[2] == 0) // IETF protocol assignments 192.0.0/24
                || (o[0] == 198 && (o[1] & 0xfe) == 18) // benchmarking 198.18/15
                || o[0] >= 240 // reserved 240/4
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
                || seg[..6] == [0; 6] // IPv4-compatible ::/96 (deprecated), e.g. ::127.0.0.1
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

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap_or_else(|_| panic!("bad test address {s}"))
    }

    #[test]
    fn bad_ip_refuses_non_public_addresses() {
        for s in [
            // loopback
            "127.0.0.1",
            "127.255.255.254",
            "::1",
            // RFC 1918
            "10.0.0.1",
            "10.255.255.255",
            "172.16.0.0",
            "172.31.255.255",
            "192.168.0.1",
            "192.168.255.255",
            // link-local, incl. cloud metadata
            "169.254.0.1",
            "169.254.169.254",
            // unspecified and 0/8
            "0.0.0.0",
            "0.1.2.3",
            // broadcast, multicast
            "255.255.255.255",
            "224.0.0.1",
            "ff02::1",
            // CGNAT 100.64/10
            "100.64.0.0",
            "100.127.255.255",
            // IPv6 unique-local, link-local, unspecified
            "fc00::1",
            "fd00::1",
            "fe80::1",
            "::",
            // IPv4-embedding prefixes: 6to4, Teredo, NAT64
            "2002::1",
            "2002:c0a8:0101::1",
            "2001:0::1",
            "2001:0:ffff::1",
            "64:ff9b::1",
            "64:ff9b::808:808",
            // IPv4-mapped private
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.1",
            "::ffff:169.254.169.254",
        ] {
            assert!(bad_ip(ip(s)), "{s} should be refused");
        }
    }

    #[test]
    fn bad_ip_allows_public_addresses() {
        for s in [
            "8.8.8.8",
            "1.1.1.1",
            "2606:4700:4700::1111",
            "2001:4860:4860::8888",
            // just outside 172.16/12
            "172.15.255.255",
            "172.32.0.0",
            // just outside CGNAT 100.64/10
            "100.63.255.255",
            "100.128.0.0",
            // just outside 198.18/15 and 192.0.0/24
            "198.17.255.255",
            "198.20.0.0",
            "192.0.1.0",
            "191.255.255.255",
            // just below multicast 224/4 (and so below reserved 240/4)
            "223.255.255.255",
            "::ffff:8.8.8.8",
            // Teredo is 2001:0::/32 only
            "2001:1::1",
        ] {
            assert!(!bad_ip(ip(s)), "{s} should be allowed");
        }
    }

    /// Ranges `bad_ip` used to let through. Each is refused now; see the
    /// comments in `bad_ip`.
    #[test]
    fn bad_ip_regressions() {
        for s in [
            // IPv4-compatible (::a.b.c.d), not covered by to_ipv4_mapped
            "::127.0.0.1",
            "::10.0.0.1",
            "::169.254.169.254",
            "::8.8.8.8",
            // benchmarking 198.18/15 (also used by fake-IP DNS proxies)
            "198.18.0.0",
            "198.19.255.255",
            // reserved 240/4 (used as private pod ranges in some clusters)
            "240.0.0.0",
            "250.1.2.3",
            "255.255.255.254",
            // IETF protocol assignments 192.0.0/24 (DS-Lite, OCI metadata 192.0.0.192)
            "192.0.0.0",
            "192.0.0.8",
            "192.0.0.192",
            "192.0.0.255",
        ] {
            assert!(bad_ip(ip(s)), "{s} should be refused");
        }
    }

    /// Documentation prefix 2001:db8::/32 is never routed, so a fetch to it
    /// can't reach anything; left allowed deliberately (only `bad_ip` gaps
    /// that can reach internal hosts were closed).
    #[test]
    fn bad_ip_documentation_prefix_is_not_special_cased() {
        assert!(!bad_ip(ip("2001:db8::1")));
    }

    async fn refused(url: &str) -> String {
        match vet(url, false).await {
            Ok(v) => panic!("{url} was accepted with addrs {:?}", v.addrs),
            Err(e) => e,
        }
    }

    #[tokio::test]
    async fn vet_rejects_invalid_urls() {
        for url in ["not a url", "", "http://", "https://"] {
            let e = refused(url).await;
            assert_eq!(e, format!("Invalid URL: {url}"));
        }
    }

    #[tokio::test]
    async fn vet_rejects_non_http_schemes() {
        for url in [
            "file:///etc/passwd",
            "ftp://8.8.8.8/x",
            "gopher://8.8.8.8:70/_x",
            "ws://8.8.8.8/",
        ] {
            let e = refused(url).await;
            assert_eq!(e, format!("Unsupported URL scheme: {url}"));
            // allow_private never relaxes the scheme check
            assert!(vet(url, true).await.is_err(), "{url} accepted");
        }
    }

    #[tokio::test]
    async fn vet_refuses_private_literal_ips() {
        for url in [
            "http://127.0.0.1/",
            "http://[::1]:8080/",
            "http://169.254.169.254/latest/meta-data",
            "http://[::ffff:127.0.0.1]/",
            "https://10.0.0.1/token",
            "http://[fd00::1]/",
            "http://0.0.0.0:3000/",
            "http://[::127.0.0.1]/",
            "http://192.0.0.192/",
        ] {
            let e = refused(url).await;
            assert_eq!(
                e,
                format!("Refusing to fetch {url}: it does not resolve to a public address.")
            );
        }
    }

    #[tokio::test]
    async fn vet_pins_public_literal_ips() {
        for (url, host, addr) in [
            ("http://8.8.8.8/", "8.8.8.8", "8.8.8.8:80"),
            (
                "https://8.8.8.8/.well-known/jwks.json",
                "8.8.8.8",
                "8.8.8.8:443",
            ),
            ("http://8.8.8.8:8443/x", "8.8.8.8", "8.8.8.8:8443"),
            ("https://1.1.1.1:443/", "1.1.1.1", "1.1.1.1:443"),
            (
                "https://[2606:4700:4700::1111]/",
                "2606:4700:4700::1111",
                "[2606:4700:4700::1111]:443",
            ),
        ] {
            let v = vet(url, false).await.unwrap();
            assert_eq!(v.host, host, "{url}");
            assert_eq!(v.addrs, vec![addr.parse::<SocketAddr>().unwrap()], "{url}");
        }
    }

    #[tokio::test]
    async fn vet_allow_private_skips_resolution() {
        for (url, host) in [
            ("http://127.0.0.1:8080/", "127.0.0.1"),
            ("http://[::1]:8080/cb", "::1"),
            ("https://[fd00::1]/", "fd00::1"),
            ("http://169.254.169.254/latest/meta-data", "169.254.169.254"),
        ] {
            let v = vet(url, true).await.unwrap();
            assert_eq!(v.host, host, "{url}");
            assert!(v.addrs.is_empty(), "{url}");
        }
    }
}
