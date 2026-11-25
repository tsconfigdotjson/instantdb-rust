//! Outbound HTTP, the same on a native node and inside a Cloudflare Worker.
//!
//! Natively this is reqwest as usual. On wasm reqwest goes through the
//! runtime's `fetch`, where its futures and responses hold JavaScript values
//! and are not `Send`, which axum handlers and `tokio::spawn` require. There
//! [`send`] calls `fetch` itself and reads the whole reply before returning,
//! so nothing from JavaScript outlives the call. A client timeout and pinned
//! addresses have no `fetch` equivalent and are skipped; redirects are never
//! followed.

use std::time::Duration;

pub use imp::{read_capped, send, Error, Response};

/// A client builder with the request timeout set where the platform has one.
pub fn builder(timeout: Duration) -> reqwest::ClientBuilder {
    let b = reqwest::Client::builder();
    #[cfg(not(target_arch = "wasm32"))]
    let b = b.timeout(timeout);
    #[cfg(target_arch = "wasm32")]
    let _ = timeout;
    b
}

/// Never follow redirects, and connect only to `addrs` for `host` (the
/// addresses the caller vetted) when there are any.
pub fn guarded(
    b: reqwest::ClientBuilder,
    host: &str,
    addrs: &[std::net::SocketAddr],
) -> reqwest::ClientBuilder {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let b = b.redirect(reqwest::redirect::Policy::none());
        if addrs.is_empty() {
            b
        } else {
            b.resolve_to_addrs(host, addrs)
        }
    }
    #[cfg(target_arch = "wasm32")]
    {
        let _ = (host, addrs);
        b
    }
}

/// Whether the request failed before reaching the server.
pub fn is_connect(e: &Error) -> bool {
    e.is_connect()
}

#[cfg(not(target_arch = "wasm32"))]
mod imp {
    pub type Response = reqwest::Response;
    pub type Error = reqwest::Error;

    pub async fn send(req: reqwest::RequestBuilder) -> reqwest::Result<Response> {
        req.send().await
    }

    /// Read the body, stopping once more than `max` bytes have arrived (the
    /// caller decides what an over-long body means).
    pub async fn read_capped(resp: &mut Response, max: usize) -> reqwest::Result<Vec<u8>> {
        let mut buf: Vec<u8> = vec![];
        while let Some(chunk) = resp.chunk().await? {
            buf.extend_from_slice(&chunk);
            if buf.len() > max {
                break;
            }
        }
        Ok(buf)
    }
}

#[cfg(target_arch = "wasm32")]
mod imp {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use js_sys::{Array, Function, Object, Promise, Reflect, Uint8Array};
    use wasm_bindgen::{JsCast, JsValue};
    use wasm_bindgen_futures::JsFuture;

    /// Why a request failed. `fetch` reports one kind of failure, so the
    /// finer questions reqwest's error answers are all "no" here.
    #[derive(Debug)]
    pub struct Error(String);

    impl Error {
        pub fn is_timeout(&self) -> bool {
            false
        }
        pub fn is_redirect(&self) -> bool {
            false
        }
        pub(super) fn is_connect(&self) -> bool {
            false
        }
    }

    impl std::fmt::Display for Error {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(&self.0)
        }
    }

    impl std::error::Error for Error {}

    fn js_err(e: JsValue) -> Error {
        Error(
            e.dyn_ref::<js_sys::Error>()
                .map(|e| String::from(e.message()))
                .or_else(|| e.as_string())
                .unwrap_or_else(|| "fetch failed".to_string()),
        )
    }

    /// A reply read to the end: status, headers and body.
    pub struct Response {
        status: reqwest::StatusCode,
        headers: reqwest::header::HeaderMap,
        body: axum::body::Bytes,
    }

    impl Response {
        pub fn status(&self) -> reqwest::StatusCode {
            self.status
        }
        pub fn headers(&self) -> &reqwest::header::HeaderMap {
            &self.headers
        }
        pub async fn bytes(self) -> Result<axum::body::Bytes, Error> {
            Ok(self.body)
        }
        pub async fn text(self) -> Result<String, Error> {
            Ok(String::from_utf8_lossy(&self.body).into_owned())
        }
    }

    /// The Worker runs on one thread, so a future that holds JavaScript
    /// values can be handed to APIs that ask for `Send`.
    struct OneThread<F>(F);
    // SAFETY: wasm32 here has no threads; the future is never moved to or
    // polled from another one.
    unsafe impl<F> Send for OneThread<F> {}
    impl<F: Future> Future for OneThread<F> {
        type Output = F::Output;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
            // SAFETY: structural pinning of the only field.
            unsafe { self.map_unchecked_mut(|s| &mut s.0) }.poll(cx)
        }
    }

    /// reqwest only builds the request here. Sending goes straight to the
    /// runtime's `fetch`: reqwest's own wasm client imports a global named
    /// `fetch`, which collides with the Worker's exported `fetch` handler.
    /// Redirects are never followed (`redirect: "manual"`).
    pub async fn send(req: reqwest::RequestBuilder) -> Result<Response, Error> {
        let req = req.build().map_err(|e| Error(e.to_string()))?;
        OneThread(fetch(req)).await
    }

    async fn fetch(req: reqwest::Request) -> Result<Response, Error> {
        let set = |obj: &Object, key: &str, value: &JsValue| {
            Reflect::set(obj, &JsValue::from_str(key), value)
                .map(|_| ())
                .map_err(js_err)
        };
        let headers = Array::new();
        for (name, value) in req.headers() {
            let value = value
                .to_str()
                .map_err(|_| Error(format!("header {name} is not text")))?;
            headers.push(&Array::of2(&name.as_str().into(), &value.into()));
        }
        let init = Object::new();
        set(&init, "method", &req.method().as_str().into())?;
        set(&init, "headers", &headers)?;
        set(&init, "redirect", &"manual".into())?;
        if let Some(body) = req.body().and_then(|b| b.as_bytes()) {
            set(&init, "body", &Uint8Array::from(body))?;
        }

        let global = js_sys::global();
        let fetch: Function = Reflect::get(&global, &"fetch".into())
            .map_err(js_err)?
            .dyn_into()
            .map_err(|_| Error("no fetch in this runtime".to_string()))?;
        let promise: Promise = fetch
            .call2(&global, &req.url().as_str().into(), &init)
            .map_err(js_err)?
            .dyn_into()
            .map_err(|_| Error("fetch did not return a promise".to_string()))?;
        let resp = JsFuture::from(promise).await.map_err(js_err)?;

        let status = Reflect::get(&resp, &"status".into())
            .ok()
            .and_then(|v| v.as_f64())
            .and_then(|n| reqwest::StatusCode::from_u16(n as u16).ok())
            .ok_or_else(|| Error("fetch reply has no status".to_string()))?;
        let mut out = reqwest::header::HeaderMap::new();
        let pairs = Array::from(&Reflect::get(&resp, &"headers".into()).map_err(js_err)?);
        for pair in pairs.iter() {
            let pair: Array = pair.unchecked_into();
            if let (Some(name), Some(value)) = (pair.get(0).as_string(), pair.get(1).as_string()) {
                if let (Ok(name), Ok(value)) = (
                    reqwest::header::HeaderName::from_bytes(name.as_bytes()),
                    reqwest::header::HeaderValue::from_str(&value),
                ) {
                    out.append(name, value);
                }
            }
        }
        let array_buffer: Function = Reflect::get(&resp, &"arrayBuffer".into())
            .map_err(js_err)?
            .dyn_into()
            .map_err(|_| Error("fetch reply has no body reader".to_string()))?;
        let promise: Promise = array_buffer
            .call0(&resp)
            .map_err(js_err)?
            .dyn_into()
            .map_err(|_| Error("arrayBuffer did not return a promise".to_string()))?;
        let buf = JsFuture::from(promise).await.map_err(js_err)?;
        Ok(Response {
            status,
            headers: out,
            body: Uint8Array::new(&buf).to_vec().into(),
        })
    }

    pub async fn read_capped(resp: &mut Response, max: usize) -> Result<Vec<u8>, Error> {
        let _ = max;
        Ok(std::mem::take(&mut resp.body).to_vec())
    }
}
