mod auth;
mod email;
mod invalidator;
mod presence;
mod rate_limit;
mod routes;
mod service;
mod state;
mod storage;
mod streams;
mod sync_table;
mod ws;

use axum::routing::{delete, get, post, put};
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

    // Serialize bootstrap DDL across concurrently-starting nodes.
    {
        use sqlx::Executor;
        let mut boot_conn = pool.acquire().await?;
        boot_conn
            .execute("SELECT pg_advisory_lock(772677321)")
            .await?;
        let result: anyhow::Result<()> = async {
            instant_core::system_catalog::ensure_system_catalog(&pool)
                .await
                .map_err(|e| anyhow::anyhow!("system catalog bootstrap failed: {e}"))?;
            service::ensure_server_tables(&pool)
                .await
                .map_err(|e| anyhow::anyhow!("server tables bootstrap failed: {e}"))?;
            storage::ensure_blob_table(&pool)
                .await
                .map_err(|e| anyhow::anyhow!("blob table bootstrap failed: {e}"))?;
            Ok(())
        }
        .await;
        boot_conn
            .execute("SELECT pg_advisory_unlock(772677321)")
            .await
            .ok();
        result?;
    }

    let state = AppState::new(cfg.clone(), pool);

    tokio::spawn(invalidator::run(state.clone()));
    tokio::spawn(presence::heartbeat_loop(state.clone()));

    let app = Router::new()
        .route("/", get(|| async { "instant-server" }))
        .route("/health", get(|| async { "ok" }))
        .route("/runtime/session", get(ws::handler))
        .route(
            "/runtime/sse",
            get(routes::sse::stream).post(routes::sse::push),
        )
        // runtime auth
        .route(
            "/runtime/auth/send_magic_code",
            post(routes::runtime::send_magic_code),
        )
        .route(
            "/runtime/auth/verify_magic_code",
            post(routes::runtime::verify_magic_code),
        )
        .route(
            "/runtime/auth/verify_refresh_token",
            post(routes::runtime::verify_refresh_token),
        )
        .route(
            "/runtime/auth/sign_in_guest",
            post(routes::runtime::sign_in_guest),
        )
        .route("/runtime/signout", post(routes::runtime::signout))
        // oauth
        .route("/runtime/oauth/start", get(routes::oauth::start))
        .route(
            "/runtime/{app_id}/oauth/start",
            get(routes::oauth::start_with_app),
        )
        .route(
            "/runtime/oauth/callback",
            get(routes::oauth::callback_get).post(routes::oauth::callback_post),
        )
        .route("/runtime/oauth/token", post(routes::oauth::token))
        .route(
            "/runtime/{app_id}/oauth/token",
            post(routes::oauth::token_with_app),
        )
        .route("/runtime/oauth/id_token", post(routes::oauth::id_token))
        .route(
            "/runtime/{app_id}/.well-known/openid-configuration",
            get(routes::oauth::well_known),
        )
        // admin
        .route("/admin/query", post(routes::admin::query))
        .route("/admin/transact", post(routes::admin::transact))
        .route("/admin/refresh_tokens", post(routes::admin::refresh_tokens))
        .route("/admin/sign_out", post(routes::admin::sign_out))
        .route(
            "/admin/users",
            get(routes::admin::get_user).delete(routes::admin::delete_user),
        )
        .route("/admin/magic_code", post(routes::admin::magic_code))
        .route("/admin/send_magic_code", post(routes::admin::magic_code))
        .route(
            "/admin/verify_magic_code",
            post(routes::admin::admin_verify_magic_code),
        )
        .route(
            "/admin/sign_in_guest",
            post(routes::admin::admin_sign_in_guest),
        )
        .route("/admin/rooms/presence", get(routes::admin::presence))
        .route(
            "/admin/query_perms_check",
            post(routes::admin::query_perms_check),
        )
        .route(
            "/admin/transact_perms_check",
            post(routes::admin::transact_perms_check),
        )
        // storage
        .route("/admin/storage/upload", put(routes::admin::storage_upload))
        .route(
            "/admin/storage/files",
            delete(routes::admin::storage_delete),
        )
        .route("/storage/serve/{app_id}/{location_id}", get(storage::serve))
        .route("/storage/upload", put(routes::admin::client_storage_upload))
        .route(
            "/storage/files",
            delete(routes::admin::client_storage_delete),
        )
        .route(
            "/storage/signed-download-url",
            get(routes::admin::client_signed_download_url),
        )
        .layer(CorsLayer::very_permissive())
        .layer(axum::extract::DefaultBodyLimit::max(100 * 1024 * 1024))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", cfg.port);
    tracing::info!("listening on {addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
