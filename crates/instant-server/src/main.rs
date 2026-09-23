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
mod webhooks;
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

    webhooks::ensure_tables(&state.pool)
        .await
        .map_err(|e| anyhow::anyhow!("webhook tables: {e}"))?;
    let webhook_key = webhooks::load_or_generate_key(&state.pool)
        .await
        .map_err(|e| anyhow::anyhow!("webhook signing key: {e}"))?;
    let _ = state.webhook_key.set(webhook_key);
    tokio::spawn(webhooks::run(state.clone()));
    tokio::spawn(invalidator::run(state.clone()));
    tokio::spawn(presence::heartbeat_loop(state.clone()));
    tokio::spawn(indexing_jobs::sweep_loop(state.clone()));

    // Storage uploads keep the larger body cap on their own router.
    let storage_uploads = Router::new()
        .route("/admin/storage/upload", put(routes::admin::storage_upload))
        .route("/storage/upload", put(routes::admin::client_storage_upload))
        .route(
            "/dash/apps/{app_id}/storage/upload",
            put(routes::dash_manage::storage_upload),
        )
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
        // @instantdb/core FrameworkClient (SSR): runtime/routes.clj:728-743
        .route(
            "/runtime/framework/query",
            post(routes::runtime::framework_query),
        )
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
        .route(
            "/admin/send_magic_code",
            post(routes::admin::admin_send_magic_code),
        )
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
        // app management / OAuth config / email templates / orgs (issue #29)
        .route("/dash", get(routes::dash_apps::dash_get))
        .route("/dash/me", get(routes::dash_apps::me_get))
        .route("/dash/apps", post(routes::dash_apps::apps_post))
        .route(
            "/dash/apps/ephemeral",
            post(routes::dash_apps::ephemeral_post),
        )
        .route(
            "/dash/apps/ephemeral/{app_id}",
            get(routes::dash_apps::ephemeral_get),
        )
        .route(
            "/dash/apps/ephemeral/{app_id}/claim",
            post(routes::dash_apps::claim_post),
        )
        .route(
            "/dash/apps/{app_id}",
            get(routes::dash_apps::apps_get).delete(routes::dash_apps::apps_delete),
        )
        .route(
            "/dash/apps/{app_id}/claim",
            post(routes::dash_apps::claim_post),
        )
        .route("/dash/apps/{app_id}/auth", get(routes::dash_apps::auth_get))
        .route(
            "/dash/apps/{app_id}/oauth_service_providers",
            post(routes::dash_apps::providers_post),
        )
        .route(
            "/dash/apps/{app_id}/oauth_clients",
            post(routes::dash_apps::clients_post),
        )
        .route(
            "/dash/apps/{app_id}/oauth_clients/{id}",
            post(routes::dash_apps::clients_update).delete(routes::dash_apps::clients_delete),
        )
        .route(
            "/dash/apps/{app_id}/authorized_redirect_origins",
            post(routes::dash_apps::origins_post),
        )
        .route(
            "/dash/apps/{app_id}/authorized_redirect_origins/{id}",
            delete(routes::dash_apps::origins_delete),
        )
        .route(
            "/dash/apps/{app_id}/email_status",
            get(routes::dash_apps::email_status),
        )
        .route(
            "/dash/apps/{app_id}/email_templates",
            post(routes::dash_apps::email_template_post),
        )
        .route(
            "/dash/apps/{app_id}/email_templates/{id}",
            delete(routes::dash_apps::email_template_delete),
        )
        .route(
            "/dash/default-email-template",
            get(routes::dash_apps::default_email_template),
        )
        .route("/dash/orgs", post(routes::dash_apps::orgs_post))
        .route(
            "/dash/orgs/{org_id}",
            get(routes::dash_apps::org_get).delete(routes::dash_apps::org_delete),
        )
        .route("/dash/cli/version", get(routes::dash::cli_version))
        // app management + account routes (dash_manage)
        .route("/admin/schema", get(routes::dash_manage::admin_schema))
        .route(
            "/admin/soft_deleted_attrs",
            get(routes::dash_manage::admin_soft_deleted_attrs),
        )
        .route(
            "/dash/apps/{app_id}/soft_deleted_attrs",
            get(routes::dash_manage::dash_soft_deleted_attrs),
        )
        .route(
            "/dash/apps/{app_id}/rename",
            post(routes::dash_manage::app_rename),
        )
        .route(
            "/dash/apps/{app_id}/clear",
            post(routes::dash_manage::app_clear),
        )
        .route(
            "/dash/apps/{app_id}/status",
            post(routes::dash_manage::app_status_post),
        )
        .route(
            "/dash/apps/{app_id}/tokens",
            post(routes::dash_manage::app_tokens_post),
        )
        .route(
            "/dash/apps/{app_id}/set-magic-code-expiry",
            post(routes::dash_manage::app_set_magic_code_expiry),
        )
        .route(
            "/dash/apps/{app_id}/rule-versions",
            get(routes::dash_manage::rule_versions_get),
        )
        .route(
            "/dash/apps/{app_id}/test_users",
            get(routes::dash_manage::test_users_get)
                .post(routes::dash_manage::test_users_post)
                .delete(routes::dash_manage::test_users_delete),
        )
        .route(
            "/dash/apps/{app_id}/stats",
            get(routes::dash_manage::app_stats_get),
        )
        .route(
            "/dash/apps/{app_id}/storage/files/delete",
            post(routes::dash_manage::storage_files_delete),
        )
        .route(
            "/dash/apps/{app_id}/send-test-email",
            post(routes::dash_manage::send_test_email),
        )
        // teams (dash_members)
        .route(
            "/dash/apps/{app_id}/invite/send",
            post(routes::dash_members::app_invite_send),
        )
        .route(
            "/dash/apps/{app_id}/invite/revoke",
            delete(routes::dash_members::app_invite_revoke),
        )
        .route(
            "/dash/orgs/{org_id}/invite/send",
            post(routes::dash_members::org_invite_send),
        )
        .route(
            "/dash/orgs/{org_id}/invite/revoke",
            delete(routes::dash_members::org_invite_revoke),
        )
        .route(
            "/dash/invites/accept",
            post(routes::dash_members::invites_accept),
        )
        .route(
            "/dash/invites/decline",
            post(routes::dash_members::invites_decline),
        )
        .route(
            "/dash/apps/{app_id}/members/update",
            post(routes::dash_members::app_members_update),
        )
        .route(
            "/dash/apps/{app_id}/members/remove",
            delete(routes::dash_members::app_members_remove),
        )
        .route(
            "/dash/orgs/{org_id}/members/update",
            post(routes::dash_members::org_members_update),
        )
        .route(
            "/dash/orgs/{org_id}/members/remove",
            delete(routes::dash_members::org_members_remove),
        )
        .route(
            "/dash/orgs/{org_id}/rename",
            post(routes::dash_members::org_rename),
        )
        .route(
            "/dash/apps/{app_id}/transfer_to_org/{org_id}",
            post(routes::dash_members::app_transfer_to_org),
        )
        .route(
            "/dash/apps/ephemeral/{app_id}/status",
            post(routes::dash_members::ephemeral_status_post),
        )
        .route(
            "/dash/apps/get_a_db/{app_id}",
            get(routes::dash_members::get_a_db_get),
        )
        .route(
            "/dash/apps/get_a_db",
            post(routes::dash_login::get_a_db_post),
        )
        .route(
            "/dash/apps/{app_id}/track-import",
            post(routes::dash_login::track_import),
        )
        // the dashboard's Google login + node stats
        .route("/dash/oauth/start", get(routes::dash_login::oauth_start))
        .route(
            "/dash/oauth/callback",
            get(routes::dash_login::oauth_callback),
        )
        .route("/dash/oauth/token", post(routes::dash_login::oauth_token))
        .route(
            "/dash/stats/active_sessions",
            get(routes::dash_login::active_sessions),
        )
        // platform API (superadmin) + platform OAuth provider + OAuth-app management
        .route(
            "/superadmin/apps",
            get(routes::superadmin::apps_list).post(routes::superadmin::apps_create),
        )
        .route("/superadmin/orgs", get(routes::superadmin::orgs_list))
        .route(
            "/superadmin/orgs/{org_id}/apps",
            get(routes::superadmin::org_apps_list),
        )
        .route(
            "/superadmin/apps/{app_id}",
            get(routes::superadmin::app_details)
                .post(routes::superadmin::app_update)
                .delete(routes::superadmin::app_delete),
        )
        .route(
            "/superadmin/apps/{app_id}/transfers/send",
            post(routes::superadmin::transfer_send),
        )
        .route(
            "/superadmin/apps/{app_id}/transfers/revoke",
            post(routes::superadmin::transfer_revoke),
        )
        .route(
            "/superadmin/apps/{app_id}/schema",
            get(routes::superadmin::schema_get),
        )
        .route(
            "/superadmin/apps/{app_id}/schema/push/plan",
            post(routes::superadmin::schema_plan),
        )
        .route(
            "/superadmin/apps/{app_id}/schema/push/apply",
            post(routes::superadmin::schema_apply),
        )
        .route(
            "/superadmin/apps/{app_id}/perms",
            get(routes::superadmin::perms_get).post(routes::superadmin::perms_post),
        )
        .route("/platform/oauth/start", get(routes::platform_oauth::start))
        .route("/platform/oauth/claim", post(routes::platform_oauth::claim))
        .route("/platform/oauth/grant", post(routes::platform_oauth::grant))
        .route("/platform/oauth/deny", post(routes::platform_oauth::deny))
        .route("/platform/oauth/token", post(routes::platform_oauth::token))
        .route(
            "/platform/oauth/token-info",
            get(routes::platform_oauth::token_info),
        )
        .route(
            "/platform/oauth/revoke",
            post(routes::platform_oauth::revoke),
        )
        .route(
            "/dash/apps/{app_id}/oauth-apps",
            get(routes::platform_oauth::oauth_apps_get)
                .post(routes::platform_oauth::oauth_apps_post),
        )
        .route(
            "/dash/apps/{app_id}/oauth-apps/{oauth_app_id}",
            post(routes::platform_oauth::oauth_app_update)
                .delete(routes::platform_oauth::oauth_app_delete),
        )
        .route(
            "/dash/apps/{app_id}/oauth-apps/{oauth_app_id}/clients",
            post(routes::platform_oauth::oauth_clients_post),
        )
        .route(
            "/dash/apps/{app_id}/oauth-app-clients/{client_id}",
            post(routes::platform_oauth::oauth_client_update)
                .delete(routes::platform_oauth::oauth_client_delete),
        )
        .route(
            "/dash/apps/{app_id}/oauth-app-clients/{client_id}/client-secrets",
            post(routes::platform_oauth::oauth_client_secrets_post),
        )
        .route(
            "/dash/apps/{app_id}/oauth-app-client-secrets/{client_secret_id}",
            delete(routes::platform_oauth::oauth_client_secret_delete),
        )
        .route(
            "/dash/user/oauth_apps",
            get(routes::platform_oauth::user_oauth_apps_get),
        )
        .route(
            "/dash/user/oauth_apps/revoke_access",
            post(routes::platform_oauth::user_oauth_apps_revoke),
        )
        // webhooks (dashboard management + receiver-facing routes)
        .route(
            "/dash/apps/{app_id}/webhooks",
            get(routes::webhooks::list).post(routes::webhooks::create),
        )
        .route(
            "/dash/apps/{app_id}/webhooks/{webhook_id}",
            post(routes::webhooks::update).delete(routes::webhooks::delete),
        )
        .route(
            "/dash/apps/{app_id}/webhooks/{webhook_id}/enable",
            post(routes::webhooks::enable),
        )
        .route(
            "/dash/apps/{app_id}/webhooks/{webhook_id}/disable",
            post(routes::webhooks::disable),
        )
        .route(
            "/dash/apps/{app_id}/webhooks/{webhook_id}/events",
            get(routes::webhooks::events),
        )
        .route(
            "/dash/apps/{app_id}/webhooks/{webhook_id}/events/{*isn}",
            get(routes::webhooks::event).post(routes::webhooks::resend),
        )
        .route(
            "/.well-known/webhooks/jwks.json",
            get(routes::webhooks::jwks),
        )
        .route(
            "/webhooks/payload/{app_id}/{webhook_id}/{*isn}",
            get(routes::webhooks::payload),
        )
        .route("/dash/profiles", post(routes::dash_manage::profiles_post))
        .route("/dash/signout", post(routes::dash_manage::signout))
        .route("/dash/check-admin", get(routes::dash_manage::check_admin))
        .route(
            "/dash/auth/send_magic_code",
            post(routes::dash_manage::auth_send_magic_code),
        )
        .route(
            "/dash/auth/verify_magic_code",
            post(routes::dash_manage::auth_verify_magic_code),
        )
        .route(
            "/dash/personal_access_tokens",
            get(routes::dash_manage::personal_access_tokens_get)
                .post(routes::dash_manage::personal_access_tokens_post),
        )
        .route(
            "/dash/personal_access_tokens/{id}",
            delete(routes::dash_manage::personal_access_tokens_delete),
        )
        // instant-cli login: register a ticket, the dashboard login claims it
        .route(
            "/dash/cli/auth/register",
            post(routes::dash_login::cli_auth_register),
        )
        .route(
            "/dash/cli/auth/check",
            post(routes::dash_login::cli_auth_check),
        )
        .route(
            "/dash/cli/auth/claim",
            post(routes::dash_login::cli_auth_claim),
        )
        .route(
            "/dash/cli/auth/void",
            post(routes::dash_login::cli_auth_void),
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
        // legacy core.clj:189-190: unmatched paths (and, since compojure
        // falls through, wrong methods) answer a JSON 404 the CLI can parse
        .fallback(route_not_found)
        .method_not_allowed_fallback(route_not_found)
        .with_state(state);

    let addr = format!("0.0.0.0:{}", cfg.port);
    tracing::info!("listening on {addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn route_not_found() -> axum::response::Response {
    use axum::response::IntoResponse;
    (
        axum::http::StatusCode::NOT_FOUND,
        axum::Json(serde_json::json!({"message": "Oops! We couldn't match this route."})),
    )
        .into_response()
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
