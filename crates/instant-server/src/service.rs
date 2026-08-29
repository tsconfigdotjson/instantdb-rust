//! Shared query/transact services used by the ws session and HTTP routes.

use std::sync::Arc;

use instant_core::attr::AttrMap;
use instant_core::error::{InstantError, Result};
use instant_core::instaql::{self, QueryCtx, QueryResult};
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
    let row = sqlx::query("SELECT id, title, status FROM apps WHERE id = $1 AND deletion_marked_at IS NULL")
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

pub async fn load_attrs(state: &AppState, app_id: Uuid) -> Result<AttrMap> {
    instant_core::attr::get_by_app_id(&state.pool, app_id).await
}

pub async fn max_tx_id(state: &AppState, app_id: Uuid) -> Result<i64> {
    let row = sqlx::query("SELECT coalesce(max(id), 0) AS n FROM transactions WHERE app_id = $1")
        .bind(app_id)
        .fetch_one(&state.pool)
        .await
        .map_err(InstantError::from)?;
    Ok(row.get("n"))
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
    let rule_params = q.get("$$ruleParams").cloned();
    let ctx = QueryCtx { app_id, attrs, admin: perms.admin };
    let mut conn = state.pool.acquire().await.map_err(InstantError::from)?;
    let mut result = instaql::query(&mut conn, &ctx, q).await?;
    if !perms.admin {
        let rules = instant_core::perms::Rules::load(&mut conn, app_id).await?;
        let auth = instant_core::perms::AuthCtx {
            user_id: perms.user_id,
            user_map: perms.user_map.clone(),
        };
        let filter = instant_core::perms::PermsFilter {
            rules: &rules,
            auth: &auth,
            rule_params: rule_params.or(perms.rule_params.clone()).unwrap_or(json!({})),
        };
        filter.filter(&mut conn, app_id, attrs, &mut result).await?;
    }
    inject_file_urls(state, app_id, attrs, q, &mut result);
    Ok(result)
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
    let steps = tx::parse_tx_steps(tx_steps)?;
    let mut attrs = load_attrs(state, app_id).await?;
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
    notify_tx(state, app_id, report.tx_id).await;
    Ok(report)
}

/// Server-internal transact (system catalog writes for auth flows).
pub async fn run_system_transact(
    state: &AppState,
    app_id: Uuid,
    tx_steps: &Value,
) -> Result<TxReport> {
    let steps = tx::parse_tx_steps(tx_steps)?;
    let mut attrs = load_attrs(state, app_id).await?;
    let mut dbtx = state.pool.begin().await.map_err(InstantError::from)?;
    let report = tx::transact(
        &mut dbtx,
        app_id,
        &mut attrs,
        steps,
        &TxOptions { allow_system_catalog_writes: true },
    )
    .await?;
    dbtx.commit().await.map_err(InstantError::from)?;
    notify_tx(state, app_id, report.tx_id).await;
    Ok(report)
}

pub async fn notify_tx(state: &AppState, app_id: Uuid, tx_id: i64) {
    let payload = json!({"app_id": app_id, "tx_id": tx_id}).to_string();
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
    Ok(())
}
