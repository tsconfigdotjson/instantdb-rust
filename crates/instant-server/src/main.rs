use instant_server::Config;
use sqlx::postgres::PgPoolOptions;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sqlx=warn".into()),
        )
        .init();

    raise_fd_limit();
    let cfg = Config::from_env();
    let port = cfg.port;
    let pool = PgPoolOptions::new()
        .max_connections(cfg.pg_pool_max)
        .min_connections(cfg.pg_pool_min.min(cfg.pg_pool_max))
        .connect(&cfg.database_url)
        .await?;

    let state = instant_server::bootstrap(cfg, pool).await?;
    instant_server::spawn_background(&state);
    let app = instant_server::router(state);

    let addr = format!("0.0.0.0:{port}");
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
