//! The Instant sync server inside a Cloudflare Durable Object.
//!
//! EXPERIMENTAL: it targets workers-rs' Emscripten + Tokio preview
//! (`worker-build --emscripten`) and is not built in CI. See README.md.
//!
//! One Durable Object is one server node. It holds what a native node holds
//! in memory (sessions, subscriptions, caches) and runs the same axum router
//! and background loops; Postgres is reached through Hyperdrive. Hyperdrive
//! has no `LISTEN/NOTIFY`, so the node runs with `INSTANT_BUS=local`.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures::StreamExt;
use instant_server::ws::{Incoming, Outbound};
use instant_server::{AppState, Config};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::{mpsc, OnceCell};
use tower_service::Service;
use worker::send::SendFuture;
use worker::wasm_bindgen::JsCast;
use worker::*;

// worker-build's Emscripten mode links a bin target.
fn main() {}

/// The Worker: hand every request to the node.
#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    env.durable_object("NODE")?
        .get_by_name("node")?
        .fetch_with_request(req)
        .await
}

struct Server {
    state: Arc<AppState>,
    router: axum::Router,
}

#[durable_object]
pub struct Node {
    env: Env,
    // the durable_object macro requires the struct to be unwind-safe
    server: AssertUnwindSafe<OnceCell<Server>>,
}

impl DurableObject for Node {
    fn new(_state: State, env: Env) -> Self {
        Self {
            env,
            server: AssertUnwindSafe(OnceCell::new()),
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        let server = self.server.get_or_try_init(|| boot(&self.env)).await?;

        let mut req: HttpRequest = req.try_into()?;
        // Cloudflare's client address, where the server looks for it.
        if let Some(ip) = req.headers().get("cf-connecting-ip").cloned() {
            req.headers_mut().insert("x-forwarded-for", ip);
        }

        let wants_websocket = req
            .headers()
            .get("upgrade")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
        if wants_websocket && req.uri().path() == "/runtime/session" {
            return open_session(server.state.clone(), req.headers());
        }

        let Ok(res) = server.router.clone().call(req).await;
        Response::try_from(res)
    }
}

/// First request after a cold start: connect, bootstrap, start the loops.
async fn boot(env: &Env) -> Result<Server> {
    export_bindings(env);
    let hyperdrive = env.hyperdrive("HYPERDRIVE")?;
    std::env::set_var("DATABASE_URL", hyperdrive.connection_string());
    // Hyperdrive carries no LISTEN/NOTIFY; this object is the only node.
    std::env::set_var("INSTANT_BUS", "local");

    instant_server::set_jwt_verifier(verify_id_token);

    let cfg = Config::from_env();
    let pool = PgPoolOptions::new()
        .max_connections(cfg.pg_pool_max)
        .min_connections(0)
        .connect(&cfg.database_url)
        .await
        .map_err(|e| Error::RustError(format!("postgres: {e}")))?;
    let state = instant_server::bootstrap(cfg, pool)
        .await
        .map_err(|e| Error::RustError(format!("bootstrap: {e}")))?;
    instant_server::spawn_background(&state);
    let router = instant_server::router(state.clone());
    Ok(Server { state, router })
}

/// The server reads its settings from the process environment
/// (docs/CONFIGURATION.md); copy the Worker's string bindings (vars and
/// secrets) there.
fn export_bindings(env: &Env) {
    let entries = js_sys::Object::entries(env.unchecked_ref::<js_sys::Object>());
    for entry in entries.iter() {
        let pair: js_sys::Array = entry.unchecked_into();
        if let (Some(name), Some(value)) = (pair.get(0).as_string(), pair.get(1).as_string()) {
            std::env::set_var(name, value);
        }
    }
}

/// Accept a client websocket and run a sync session on it. The socket is a
/// JavaScript object, so one task pumps it and talks to the session (which
/// runs on the Tokio runtime like any other) over channels.
fn open_session(state: Arc<AppState>, headers: &axum::http::HeaderMap) -> Result<Response> {
    let request = instant_server::ws::request_ctx_from_headers(headers);
    let pair = WebSocketPair::new()?;
    let socket = pair.server;
    socket.accept()?;

    let (in_tx, in_rx) = mpsc::unbounded_channel::<Incoming<String>>();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Outbound>();

    tokio::spawn(SendFuture::new(async move {
        let Ok(mut events) = socket.events() else {
            return;
        };
        loop {
            tokio::select! {
                event = events.next() => match event {
                    Some(Ok(WebsocketEvent::Message(msg))) => {
                        let frame = match msg.text() {
                            Some(text) => Incoming::Text(text),
                            None => Incoming::Other,
                        };
                        if in_tx.send(frame).is_err() {
                            break;
                        }
                    }
                    // closed by the client, or the socket failed
                    _ => {
                        let _ = in_tx.send(Incoming::Close);
                        break;
                    }
                },
                frame = out_rx.recv() => match frame {
                    Some(Outbound::Text(text)) => {
                        if socket.send_with_str(text).is_err() {
                            let _ = in_tx.send(Incoming::Close);
                            break;
                        }
                    }
                    // the runtime answers protocol pings itself
                    Some(Outbound::Ping) => {}
                    // the session ended, or asked to drop a slow client
                    Some(Outbound::Close) | None => break,
                },
            }
        }
        drop(events);
        let _ = socket.close::<&str>(None, None);
    }));
    tokio::spawn(instant_server::ws::serve_channels(
        state, request, in_rx, out_tx,
    ));

    Response::from_websocket(pair.client)
}

/// The server's id_token check (routes/oauth.rs `verify_jwt`), with the same
/// rules and messages, on this crate's JWT library.
fn verify_id_token(
    disc: &serde_json::Value,
    jwks: &serde_json::Value,
    jwt: &str,
) -> std::result::Result<serde_json::Value, &'static str> {
    use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
    let header = decode_header(jwt).map_err(|_| "Error validating JWT. Malformed token.")?;
    let keys = jwks
        .get("keys")
        .and_then(|k| k.as_array())
        .cloned()
        .unwrap_or_default();
    let jwk = keys
        .iter()
        .find(|k| {
            header
                .kid
                .as_deref()
                .map(|kid| k.get("kid").and_then(|v| v.as_str()) == Some(kid))
                .unwrap_or(true)
        })
        .ok_or("Error validating JWT. No matching key.")?;
    let part = |name: &str| jwk.get(name).and_then(|v| v.as_str()).unwrap_or_default();
    let key = match jwk.get("kty").and_then(|v| v.as_str()) {
        Some("RSA") => DecodingKey::from_rsa_components(part("n"), part("e"))
            .map_err(|_| "Error validating JWT. Bad RSA key.")?,
        Some("EC") => DecodingKey::from_ec_components(part("x"), part("y"))
            .map_err(|_| "Error validating JWT. Bad EC key.")?,
        _ => return Err("Error validating JWT. Unsupported key type."),
    };
    let alg = match header.alg {
        Algorithm::RS256 => Algorithm::RS256,
        Algorithm::ES256 => Algorithm::ES256,
        _ => return Err("The id_token used an unsupported algorithm."),
    };
    if let Some(supported) = disc
        .get("id_token_signing_alg_values_supported")
        .and_then(|v| v.as_array())
    {
        let name = format!("{alg:?}");
        if !supported.iter().any(|v| v.as_str() == Some(name.as_str())) {
            return Err("The id_token used an unsupported algorithm.");
        }
    }
    let mut validation = Validation::new(alg);
    validation.validate_aud = false;
    validation.validate_exp = true;
    decode::<serde_json::Value>(jwt, &key, &validation)
        .map(|data| data.claims)
        .map_err(|_| "Error validating JWT. Signature is invalid.")
}
