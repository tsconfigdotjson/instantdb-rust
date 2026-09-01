//! Shared query/transact services used by the ws session and HTTP routes.

use std::sync::Arc;

use instant_core::attr::AttrMap;
use instant_core::error::{InstantError, Result};
use instant_core::instaql::{self, QueryCtx, QueryResult};
use instant_core::perms::Rules;
use instant_core::topics::QueryTopics;
use instant_core::tx::{self, TxOptions, TxReport};
use serde_json::{json, Value};
use sqlx::Row;
use uuid::Uuid;

use crate::state::AppState;

pub struct AppRow {
    pub id: Uuid,
    pub title: String,
    pub status: String,
}

pub async fn get_app(state: &AppState, app_id: Uuid) -> Result<AppRow> {
    let row = sqlx::query(
        "SELECT id, title, status FROM apps WHERE id = $1 AND deletion_marked_at IS NULL",
    )
    .bind(app_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?
    .ok_or_else(|| InstantError::record_not_found("app", "Could not find app."))?;
    Ok(AppRow {
        id: row.get("id"),
        title: row.get("title"),
        status: row.get("status"),
    })
}

/// Attr cache safety net: entries older than this are reloaded even without
/// an invalidation, so a missed NOTIFY (listener reconnect) self-heals.
const ATTR_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(60);

/// App attr catalog, served from the per-app cache (issue #11). Every query,
/// transact and refresh used to hit Postgres for this; now only the first
/// load per app and reloads after an `attrs_changed` transaction do.
///
/// Concurrency: a load that started before an invalidation must not be
/// cached after it (it may have read the pre-change catalog), so each entry
/// records the invalidation generation observed before its SELECT and is
/// only installed if that generation is still current.
pub async fn load_attrs(state: &AppState, app_id: Uuid) -> Result<Arc<AttrMap>> {
    if let Some(entry) = state.attr_cache.get(&app_id) {
        if entry.loaded_at.elapsed() < ATTR_CACHE_TTL {
            crate::metrics::METRICS.attr_cache_hits_total.inc();
            return Ok(entry.attrs.clone());
        }
    }
    crate::metrics::METRICS.attr_cache_misses_total.inc();
    let generation = attr_generation(state, app_id);
    let attrs = Arc::new(instant_core::attr::get_by_app_id(&state.pool, app_id).await?);
    if attr_generation(state, app_id) == generation {
        state.attr_cache.insert(
            app_id,
            crate::state::AttrCacheEntry {
                attrs: attrs.clone(),
                loaded_at: std::time::Instant::now(),
            },
        );
    }
    Ok(attrs)
}

fn attr_generation(state: &AppState, app_id: Uuid) -> u64 {
    state.attr_gen.get(&app_id).map(|g| *g).unwrap_or(0)
}

/// Drop the cached catalog for an app (its attrs changed, locally or on
/// another node).
pub fn invalidate_attrs(state: &AppState, app_id: Uuid) {
    *state.attr_gen.entry(app_id).or_insert(0) += 1;
    state.attr_cache.remove(&app_id);
}

pub async fn max_tx_id(state: &AppState, app_id: Uuid) -> Result<i64> {
    let row = sqlx::query("SELECT coalesce(max(id), 0) AS n FROM transactions WHERE app_id = $1")
        .bind(app_id)
        .fetch_one(&state.pool)
        .await
        .map_err(InstantError::from)?;
    Ok(row.get("n"))
}

/// Current ISN in the legacy wire format `"{slotNumHex}/{pgLSN}"` (see
/// LEGACY server instant/isn.clj — slot-num is a failover counter, always 0
/// here; the LSN part is the Postgres WAL position). Clients never read it,
/// but legacy attaches it to transact-ok / add-query-ok / refresh-ok, and the
/// monotonicity invariant (refresh isn >= transact isn) holds because the WAL
/// LSN only grows.
pub async fn current_isn(state: &AppState) -> Value {
    match sqlx::query("SELECT '0/' || pg_current_wal_lsn()::text AS isn")
        .fetch_one(&state.pool)
        .await
    {
        Ok(row) => Value::String(row.get("isn")),
        Err(_) => Value::Null,
    }
}

/// Auth context for permission checks.
#[derive(Debug, Clone, Default)]
pub struct PermsCtx {
    pub admin: bool,
    pub user_id: Option<Uuid>,
    pub user_map: Option<Value>,
    pub rule_params: Option<Value>,
}

pub async fn run_query(
    state: &AppState,
    app_id: Uuid,
    attrs: &AttrMap,
    perms: &PermsCtx,
    q: &Value,
) -> Result<QueryResult> {
    Ok(run_query_full(state, app_id, attrs, perms, q, None)
        .await?
        .result)
}

/// A query result plus the invalidation topics derived from it.
pub struct QueryOutcome {
    pub result: QueryResult,
    pub topics: QueryTopics,
}

/// Runs a query and derives its invalidation topics (instant_core::topics).
/// `rules` lets a refresh batch load the app's rules once instead of once
/// per query; None loads them here for non-admin contexts.
pub async fn run_query_full(
    state: &AppState,
    app_id: Uuid,
    attrs: &AttrMap,
    perms: &PermsCtx,
    q: &Value,
    rules: Option<&Rules>,
) -> Result<QueryOutcome> {
    let started = std::time::Instant::now();
    let rule_params = q.get("$$ruleParams").cloned();
    let ctx = QueryCtx {
        app_id,
        attrs,
        admin: perms.admin,
    };
    let mut conn = state.pool.acquire().await.map_err(InstantError::from)?;
    let mut result = instaql::query(&mut conn, &ctx, q).await?;
    // Topics come from the pre-permissions result: an entity hidden by a
    // view rule is still tracked, so a write that makes it visible refreshes.
    let mut topics = match instaql::parse_query(q) {
        Ok(forms) => instant_core::topics::query_topics(attrs, &forms, &result),
        Err(_) => QueryTopics::catch_all(),
    };
    if !perms.admin {
        let loaded;
        let rules = match rules {
            Some(r) => r,
            None => {
                loaded = Rules::load(&mut conn, app_id).await?;
                &loaded
            }
        };
        if rules_reference_other_entities(rules) {
            // data.ref / auth.ref walk links the topic derivation doesn't
            // see; fall back to refreshing on every tx for this query.
            topics = QueryTopics::catch_all();
        }
        let auth = instant_core::perms::AuthCtx {
            user_id: perms.user_id,
            user_map: perms.user_map.clone(),
        };
        let filter = instant_core::perms::PermsFilter {
            rules,
            auth: &auth,
            rule_params: rule_params
                .or(perms.rule_params.clone())
                .unwrap_or(json!({})),
        };
        filter.filter(&mut conn, app_id, attrs, &mut result).await?;
    }
    drop(conn);
    inject_file_urls(state, app_id, attrs, q, &mut result);
    crate::metrics::METRICS.query_seconds.observe_since(started);
    Ok(QueryOutcome { result, topics })
}

/// True when any rule expression uses `.ref(`, i.e. permission results
/// depend on linked entities beyond the queried ones.
fn rules_reference_other_entities(rules: &Rules) -> bool {
    !rules.code.is_null() && rules.code.to_string().contains(".ref(")
}

/// $files entities get a synthetic `url` triple; `location-id` triples are
/// hidden unless explicitly requested via $.fields (legacy transform-$files-result).
fn inject_file_urls(
    state: &AppState,
    app_id: Uuid,
    attrs: &AttrMap,
    q: &Value,
    result: &mut QueryResult,
) {
    let loc_attr = instant_core::system_catalog::attr_id("$files", "location-id");
    let url_attr = instant_core::system_catalog::attr_id("$files", "url");
    let _ = q;
    fn walk(
        node: &mut instant_core::instaql::EntityNode,
        state: &AppState,
        app_id: Uuid,
        loc_attr: Uuid,
        url_attr: Uuid,
    ) {
        if node.etype == "$files" {
            let loc = node
                .triples
                .iter()
                .find(|t| t.a == loc_attr)
                .and_then(|t| t.v.as_str().map(|s| s.to_string()));
            if let Some(loc) = loc {
                let t0 = node.triples.first().map(|t| t.t).unwrap_or(0);
                let url = crate::storage::download_url(state, app_id, &loc);
                node.triples.push(instant_core::instaql::TripleOut {
                    e: node.eid,
                    a: url_attr,
                    v: serde_json::Value::String(url),
                    t: t0,
                });
            }
            node.triples.retain(|t| t.a != loc_attr);
        }
        for c in &mut node.children {
            for e in &mut c.entities {
                walk(e, state, app_id, loc_attr, url_attr);
            }
        }
    }
    let _ = attrs;
    for form in &mut result.forms {
        for e in &mut form.entities {
            walk(e, state, app_id, loc_attr, url_attr);
        }
    }
}

/// Runs tx-steps for an app: perms checks (unless admin), commit, notify.
pub async fn run_transact(
    state: &Arc<AppState>,
    app_id: Uuid,
    perms: &PermsCtx,
    tx_steps: &Value,
) -> Result<TxReport> {
    let started = std::time::Instant::now();
    let steps = tx::parse_tx_steps(tx_steps)?;
    let mut attrs = (*load_attrs(state, app_id).await?).clone();
    let mut dbtx = state.pool.begin().await.map_err(InstantError::from)?;
    tx::assert_write_allowed(&mut dbtx, app_id).await?;

    let report = if perms.admin {
        tx::transact(&mut dbtx, app_id, &mut attrs, steps, &TxOptions::default()).await?
    } else {
        let rules = instant_core::perms::Rules::load(&mut dbtx, app_id).await?;
        let auth = instant_core::perms::AuthCtx {
            user_id: perms.user_id,
            user_map: perms.user_map.clone(),
        };
        instant_core::perms::permissioned_transact(
            &mut dbtx,
            app_id,
            &mut attrs,
            steps,
            &rules,
            &auth,
            perms.rule_params.as_ref().unwrap_or(&json!({})),
        )
        .await?
    };

    dbtx.commit().await.map_err(InstantError::from)?;
    notify_tx(state, app_id, report.tx_id, report.attrs_changed).await;
    crate::metrics::METRICS
        .transact_seconds
        .observe_since(started);
    Ok(report)
}

/// Server-internal transact (system catalog writes for auth flows).
pub async fn run_system_transact(
    state: &AppState,
    app_id: Uuid,
    tx_steps: &Value,
) -> Result<TxReport> {
    let steps = tx::parse_tx_steps(tx_steps)?;
    let mut attrs = (*load_attrs(state, app_id).await?).clone();
    let mut dbtx = state.pool.begin().await.map_err(InstantError::from)?;
    let report = tx::transact(
        &mut dbtx,
        app_id,
        &mut attrs,
        steps,
        &TxOptions {
            allow_system_catalog_writes: true,
        },
    )
    .await?;
    dbtx.commit().await.map_err(InstantError::from)?;
    notify_tx(state, app_id, report.tx_id, report.attrs_changed).await;
    Ok(report)
}

/// Announce a committed tx to every node (including this one). The payload
/// carries `attrs_changed` so nodes can drop their attr cache before the
/// refresh runs, and the commit timestamp (ms) for NOTIFY-lag metrics.
pub async fn notify_tx(state: &AppState, app_id: Uuid, tx_id: i64, attrs_changed: bool) {
    if attrs_changed {
        // Local requests racing the NOTIFY must not serve the old catalog.
        invalidate_attrs(state, app_id);
    }
    let payload = json!({
        "app_id": app_id,
        "tx_id": tx_id,
        "attrs_changed": attrs_changed,
        "ts": chrono::Utc::now().timestamp_millis(),
    })
    .to_string();
    let _ = sqlx::query("SELECT pg_notify('instant_tx', $1)")
        .bind(payload)
        .execute(&state.pool)
        .await;
}

/// Publish to a channel, spilling large payloads into a table.
pub async fn notify_json(state: &AppState, channel: &str, payload: Value) {
    let text = payload.to_string();
    let text = if text.len() > 7000 {
        let id = Uuid::new_v4();
        let ok = sqlx::query("INSERT INTO rust_spill (id, payload) VALUES ($1, $2)")
            .bind(id)
            .bind(&payload)
            .execute(&state.pool)
            .await
            .is_ok();
        if !ok {
            return;
        }
        json!({"spill": id}).to_string()
    } else {
        text
    };
    let _ = sqlx::query("SELECT pg_notify($1, $2)")
        .bind(channel)
        .bind(text)
        .execute(&state.pool)
        .await;
}

pub async fn resolve_spill(state: &AppState, payload: Value) -> Option<Value> {
    if let Some(id) = payload.get("spill").and_then(|s| s.as_str()) {
        let id = Uuid::parse_str(id).ok()?;
        let row = sqlx::query("SELECT payload FROM rust_spill WHERE id = $1")
            .bind(id)
            .fetch_optional(&state.pool)
            .await
            .ok()??;
        Some(row.get("payload"))
    } else {
        Some(payload)
    }
}

/// One-time schema bootstrap for the server's own coordination tables.
/// Resolve the signing secret when SERVER_SECRET isn't set: load the one
/// persisted in Postgres, or generate a random one and persist it. Storing it
/// in the shared database keeps every node signing identically and keeps
/// previously issued download URLs valid across restarts. Called under the
/// bootstrap advisory lock, so concurrent first boots don't race; the
/// ON CONFLICT + re-select is belt-and-braces on top of that.
pub async fn load_or_generate_secret(pool: &sqlx::PgPool) -> Result<String> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rust_server_config (
           key text PRIMARY KEY,
           value text NOT NULL,
           created_at timestamptz NOT NULL DEFAULT now())",
    )
    .execute(pool)
    .await
    .map_err(InstantError::from)?;
    let secret: String = {
        use rand::RngCore;
        let mut bytes = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    };
    sqlx::query(
        "INSERT INTO rust_server_config (key, value) VALUES ('server_secret', $1)
         ON CONFLICT (key) DO NOTHING",
    )
    .bind(&secret)
    .execute(pool)
    .await
    .map_err(InstantError::from)?;
    let row = sqlx::query("SELECT value FROM rust_server_config WHERE key = 'server_secret'")
        .fetch_one(pool)
        .await
        .map_err(InstantError::from)?;
    Ok(row.get::<String, _>("value"))
}

pub async fn ensure_server_tables(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE UNLOGGED TABLE IF NOT EXISTS rust_presence (
          app_id uuid NOT NULL,
          room_id text NOT NULL,
          session_id uuid NOT NULL,
          user_json jsonb,
          data jsonb,
          node_id uuid NOT NULL,
          updated_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (app_id, room_id, session_id)
        )"#,
    )
    .execute(pool)
    .await
    .map_err(InstantError::from)?;
    sqlx::query(
        "CREATE UNLOGGED TABLE IF NOT EXISTS rust_nodes (
           node_id uuid PRIMARY KEY,
           heartbeat_at timestamptz NOT NULL DEFAULT now())",
    )
    .execute(pool)
    .await
    .map_err(InstantError::from)?;
    sqlx::query(
        "CREATE UNLOGGED TABLE IF NOT EXISTS rust_spill (
           id uuid PRIMARY KEY,
           payload jsonb NOT NULL,
           created_at timestamptz NOT NULL DEFAULT now())",
    )
    .execute(pool)
    .await
    .map_err(InstantError::from)?;
    // per-tx triple change log for sync tables (replaces the legacy WAL feed)
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rust_tx_changes (
           tx_id bigint NOT NULL,
           app_id uuid NOT NULL,
           entity_id uuid NOT NULL,
           attr_id uuid NOT NULL,
           value jsonb NOT NULL,
           created_at bigint,
           action text NOT NULL,
           logged_at timestamptz NOT NULL DEFAULT now())",
    )
    .execute(pool)
    .await
    .map_err(InstantError::from)?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS rust_tx_changes_app_tx_idx
         ON rust_tx_changes (app_id, tx_id)",
    )
    .execute(pool)
    .await
    .map_err(InstantError::from)?;
    sqlx::query(
        r#"
        CREATE OR REPLACE FUNCTION rust_capture_triple_change() RETURNS trigger AS $fn$
        DECLARE txid_setting text;
        BEGIN
          txid_setting := current_setting('instant.rust_tx_id', true);
          IF txid_setting IS NULL OR txid_setting = '' THEN
            RETURN NULL;
          END IF;
          IF TG_OP = 'INSERT' THEN
            INSERT INTO rust_tx_changes (tx_id, app_id, entity_id, attr_id, value, created_at, action)
            VALUES (txid_setting::bigint, NEW.app_id, NEW.entity_id, NEW.attr_id, NEW.value, NEW.created_at, 'added');
          ELSIF TG_OP = 'DELETE' THEN
            INSERT INTO rust_tx_changes (tx_id, app_id, entity_id, attr_id, value, created_at, action)
            VALUES (txid_setting::bigint, OLD.app_id, OLD.entity_id, OLD.attr_id, OLD.value, OLD.created_at, 'removed');
          ELSIF TG_OP = 'UPDATE' AND NEW.value IS DISTINCT FROM OLD.value THEN
            INSERT INTO rust_tx_changes (tx_id, app_id, entity_id, attr_id, value, created_at, action)
            VALUES (txid_setting::bigint, OLD.app_id, OLD.entity_id, OLD.attr_id, OLD.value, OLD.created_at, 'removed'),
                   (txid_setting::bigint, NEW.app_id, NEW.entity_id, NEW.attr_id, NEW.value, NEW.created_at, 'added');
          END IF;
          RETURN NULL;
        END $fn$ LANGUAGE plpgsql
        "#,
    )
    .execute(pool)
    .await
    .map_err(InstantError::from)?;
    sqlx::query("DROP TRIGGER IF EXISTS rust_capture_trigger ON triples")
        .execute(pool)
        .await
        .map_err(InstantError::from)?;
    sqlx::query(
        "CREATE TRIGGER rust_capture_trigger
         AFTER INSERT OR UPDATE OR DELETE ON triples
         FOR EACH ROW EXECUTE FUNCTION rust_capture_triple_change()",
    )
    .execute(pool)
    .await
    .map_err(InstantError::from)?;
    Ok(())
}
