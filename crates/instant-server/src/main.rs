mod auth;
mod email;
mod indexing_jobs;
mod invalidator;
mod metrics;
mod presence;
mod rate_limit;
mod routes;
mod s3;
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

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Prometheus text exposition of the node's counters/histograms/gauges.
async fn metrics_handler(
    axum::extract::State(state): axum::extract::State<std::sync::Arc<AppState>>,
) -> impl axum::response::IntoResponse {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        metrics::render(&state),
    )
}

/// Request-body cap for JSON/API endpoints (transacts, admin queries, sse).
const JSON_BODY_LIMIT: usize = 10 * 1024 * 1024;
/// Request-body cap for storage uploads (matches the legacy 100MB limit).
const STORAGE_BODY_LIMIT: usize = 100 * 1024 * 1024;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sqlx=warn".into()),
        )
        .init();

    raise_fd_limit();
    let mut cfg = Config::from_env();
    let pool = PgPoolOptions::new()
        .max_connections(cfg.pg_pool_max)
        .min_connections(cfg.pg_pool_min.min(cfg.pg_pool_max))
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
            if cfg.secret.is_empty() {
                cfg.secret = service::load_or_generate_secret(&pool)
                    .await
                    .map_err(|e| anyhow::anyhow!("server secret bootstrap failed: {e}"))?;
                tracing::info!(
                    "SERVER_SECRET not set; using a generated secret persisted in Postgres"
                );
            }
            Ok(())
        }
        .await;
        boot_conn
            .execute("SELECT pg_advisory_unlock(772677321)")
            .await
            .ok();
        result?;
    }

    if cfg.secret.len() < 16 {
        tracing::warn!("SERVER_SECRET is very short; use a long random value in production");
    }

    storage::init_from_env().map_err(|e| anyhow::anyhow!("storage backend: {e}"))?;

    let state = AppState::new(cfg.clone(), pool);

    tokio::spawn(invalidator::run(state.clone()));
    tokio::spawn(presence::heartbeat_loop(state.clone()));
    tokio::spawn(indexing_jobs::sweep_loop(state.clone()));

    // Storage uploads keep the larger body cap on their own router.
    let storage_uploads = Router::new()
        .route("/admin/storage/upload", put(routes::admin::storage_upload))
        .route("/storage/upload", put(routes::admin::client_storage_upload))
        .route(
            "/storage/{upload_id}/consume-upload-url",
            put(routes::admin::consume_upload_url),
        )
        .layer(axum::extract::DefaultBodyLimit::max(STORAGE_BODY_LIMIT));

    let app = Router::new()
        .route("/", get(|| async { "instant-server" }))
        .route("/health", get(|| async { "ok" }))
        .route("/metrics", get(metrics_handler))
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
        .route(
            "/admin/subscribe-query",
            post(routes::sse::admin_subscribe_query),
        )
        .route("/admin/sse", post(routes::sse::admin_stream))
        .route("/admin/sse/push", post(routes::sse::admin_push))
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
        // dashboard routes used by instant-cli (docs/ADMIN.md §6)
        .route(
            "/dash/apps/{app_id}/schema/pull",
            get(routes::dash::schema_pull),
        )
        .route(
            "/dash/apps/{app_id}/schema/steps/apply",
            post(routes::dash::schema_steps_apply),
        )
        .route(
            "/dash/apps/{app_id}/schema/push/plan",
            post(routes::dash::schema_push_plan),
        )
        .route(
            "/dash/apps/{app_id}/schema/push/apply",
            post(routes::dash::schema_push_apply),
        )
        .route(
            "/dash/apps/{app_id}/perms/pull",
            get(routes::dash::perms_pull),
        )
        .route("/dash/apps/{app_id}/rules", post(routes::dash::rules_post))
        .route(
            "/dash/apps/{app_id}/indexing-jobs",
            post(routes::dash::indexing_job_post),
        )
        .route(
            "/dash/apps/{app_id}/indexing-jobs/group/{group_id}",
            get(routes::dash::indexing_jobs_group),
        )
        .route(
            "/dash/apps/{app_id}/indexing-jobs/{job_id}",
            get(routes::dash::indexing_job_get),
        )
        .route("/dash/cli/version", get(routes::dash::cli_version))
        .route(
            "/dash/cli/auth/register",
            post(routes::dash::cli_auth_unsupported),
        )
        .route(
            "/dash/cli/auth/check",
            post(routes::dash::cli_auth_unsupported),
        )
        .route(
            "/dash/cli/auth/claim",
            post(routes::dash::cli_auth_unsupported),
        )
        .route(
            "/dash/cli/auth/void",
            post(routes::dash::cli_auth_unsupported),
        )
        .route(
            "/admin/query_perms_check",
            post(routes::admin::query_perms_check),
        )
        .route(
            "/admin/transact_perms_check",
            post(routes::admin::transact_perms_check),
        )
        // storage (non-upload)
        .route(
            "/admin/storage/files",
            delete(routes::admin::storage_delete).get(routes::admin::storage_list),
        )
        .route(
            "/admin/storage/files/delete",
            post(routes::admin::storage_delete_many),
        )
        .route(
            "/admin/storage/signed-upload-url",
            post(routes::admin::admin_signed_upload_url),
        )
        .route(
            "/admin/storage/signed-download-url",
            get(routes::admin::admin_signed_download_url),
        )
        .route(
            "/storage/signed-upload-url",
            post(routes::admin::client_signed_upload_url),
        )
        .route("/storage/serve/{app_id}/{location_id}", get(storage::serve))
        .route(
            "/storage/files",
            delete(routes::admin::client_storage_delete),
        )
        .route(
            "/storage/signed-download-url",
            get(routes::admin::client_signed_download_url),
        )
        // JSON endpoints: transacts and admin batches are at most a few MB;
        // keep them an order of magnitude below the storage-upload cap so a
        // single request can't buffer 100MB of JSON.
        .layer(axum::extract::DefaultBodyLimit::max(JSON_BODY_LIMIT))
        .merge(storage_uploads)
        // Wildcard CORS without credential reflection: every browser-facing
        // route authenticates via bearer headers, never cookies, so the
        // origin-reflection + allow-credentials of very_permissive() is
        // unnecessary exposure. The oauth __session cookie is SameSite=Lax
        // and only read on top-level navigations, which CORS doesn't govern.
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = format!("0.0.0.0:{}", cfg.port);
    tracing::info!("listening on {addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

/// Raise the open-file soft limit to the hard limit: every websocket session
/// is a file descriptor and the usual 1024 default caps a node at ~1k
/// clients. Best effort; a failure just leaves the inherited limit.
#[cfg(unix)]
fn raise_fd_limit() {
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: plain out-parameter calls on a properly sized struct.
    unsafe {
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) != 0 {
            return;
        }
        if lim.rlim_cur >= lim.rlim_max {
            return;
        }
        let want = lim.rlim_max;
        lim.rlim_cur = want;
        if libc::setrlimit(libc::RLIMIT_NOFILE, &lim) != 0 {
            // macOS rejects RLIM_INFINITY for NOFILE; settle for the kernel cap
            #[cfg(target_os = "macos")]
            {
                lim.rlim_cur = 10240.min(want);
                libc::setrlimit(libc::RLIMIT_NOFILE, &lim);
            }
        }
    }
    tracing::info!(open_files = lim.rlim_cur, "fd limit");
}

#[cfg(not(unix))]
fn raise_fd_limit() {}
