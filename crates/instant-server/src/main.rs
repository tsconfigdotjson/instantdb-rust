mod auth;
mod invalidator;
mod presence;
mod service;
mod state;
mod ws;

use std::sync::Arc;

use axum::routing::get;
use axum::Router;
use sqlx::postgres::PgPoolOptions;
use state::{AppState, Config};
use tower_http::cors::CorsLayer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sqlx=warn".into()),
        )
        .init();

    let cfg = Config::from_env();
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&cfg.database_url)
        .await?;

    instant_core::system_catalog::ensure_system_catalog(&pool)
        .await
        .map_err(|e| anyhow::anyhow!("system catalog bootstrap failed: {e}"))?;
    service::ensure_server_tables(&pool)
        .await
        .map_err(|e| anyhow::anyhow!("server tables bootstrap failed: {e}"))?;

    let state = AppState::new(cfg.clone(), pool);

    tokio::spawn(invalidator::run(state.clone()));
    tokio::spawn(presence::heartbeat_loop(state.clone()));

    let app = Router::new()
        .route("/", get(|| async { "instant-server" }))
        .route("/health", get(|| async { "ok" }))
        .route("/runtime/session", get(ws::handler))
        .layer(CorsLayer::very_permissive())
        .with_state(state);

    let addr = format!("0.0.0.0:{}", cfg.port);
    tracing::info!("listening on {addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
