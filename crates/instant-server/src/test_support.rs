//! Database fixtures for the unit tests that need the server's own tables and
//! triggers (the `rust_tx_changes` capture log), against `DATABASE_URL` like
//! the instant-core integration tests.

use std::sync::Arc;

use serde_json::{json, Value};
use sqlx::Row;
use uuid::Uuid;

use crate::state::{AppState, Config};

/// The advisory lock `main` holds around the bootstrap DDL, so tests running
/// in parallel (and a server booting on the same database) never race it.
const BOOTSTRAP_LOCK: i64 = 772677321;

/// Test-only trigger function that moves a tx's `removed` log rows
/// physically past its `added` rows (issue #129).
const SCRAMBLE_SQL: &str = include_str!("../../../scripts/differential/scramble-tx-changes.sql");

/// An `AppState` on a database bootstrapped the way `main` does it.
pub async fn bootstrapped_state() -> Arc<AppState> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://instant:instant@localhost:5432/instant".into());
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect postgres");
    let mut lock = pool.acquire().await.expect("acquire");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(BOOTSTRAP_LOCK)
        .execute(&mut *lock)
        .await
        .expect("bootstrap lock");
    let res = async {
        instant_core::system_catalog::ensure_system_catalog(&pool).await?;
        crate::service::ensure_server_tables(&pool).await?;
        crate::webhooks::ensure_tables(&pool).await?;
        sqlx::query(SCRAMBLE_SQL).execute(&pool).await?;
        Ok::<_, instant_core::error::InstantError>(())
    }
    .await;
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(BOOTSTRAP_LOCK)
        .execute(&mut *lock)
        .await
        .expect("bootstrap unlock");
    drop(lock);
    res.expect("bootstrap");
    AppState::new(Config::from_env(), pool)
}

/// A fresh app (and creator) for test isolation.
pub async fn mk_app(pool: &sqlx::PgPool) -> Uuid {
    let user_id = Uuid::new_v4();
    let app_id = Uuid::new_v4();
    sqlx::query("INSERT INTO instant_users (id, email) VALUES ($1, $2)")
        .bind(user_id)
        .bind(format!("test-{user_id}@example.com"))
        .execute(pool)
        .await
        .expect("insert user");
    sqlx::query("INSERT INTO apps (id, creator_id, title) VALUES ($1, $2, 'test app')")
        .bind(app_id)
        .bind(user_id)
        .execute(pool)
        .await
        .expect("insert app");
    app_id
}

/// Attr ids of the `items` namespace [`items_app`] creates.
pub struct Items {
    pub id: Uuid,
    pub rank: Uuid,
}

/// A fresh app with `items.id` and a cardinality-one `items.rank`.
pub async fn items_app(state: &AppState) -> (Uuid, Items) {
    let app_id = mk_app(&state.pool).await;
    let items = Items {
        id: Uuid::new_v4(),
        rank: Uuid::new_v4(),
    };
    let steps = json!([
        ["add-attr", {"id": items.id, "forward-identity": [Uuid::new_v4(), "items", "id"],
          "value-type": "blob", "cardinality": "one", "unique?": true, "index?": false}],
        ["add-attr", {"id": items.rank, "forward-identity": [Uuid::new_v4(), "items", "rank"],
          "value-type": "blob", "cardinality": "one", "unique?": false, "index?": false}]
    ]);
    transact(state, app_id, steps).await;
    (app_id, items)
}

/// Commit tx-steps through the server's own transact path (trigger capture,
/// webhook queueing) and return the tx id.
pub async fn transact(state: &AppState, app_id: Uuid, steps: Value) -> i64 {
    crate::service::run_system_transact(state, app_id, &steps)
        .await
        .expect("transact")
        .tx_id
}

fn scramble_trigger(app_id: Uuid) -> String {
    format!("rust_test_scramble_{}", app_id.simple())
}

/// From now on, every insert into this app's change log moves the tx's
/// `removed` rows physically past its `added` rows, the placement a busy
/// table's free-space map can produce on its own (issue #129). Undo with
/// [`unscramble`].
pub async fn scramble(pool: &sqlx::PgPool, app_id: Uuid) {
    sqlx::query(&format!(
        "CREATE TRIGGER {} AFTER INSERT ON rust_tx_changes
         REFERENCING NEW TABLE AS scrambled_new_rows
         FOR EACH STATEMENT EXECUTE FUNCTION rust_test_scramble_tx_changes('{app_id}')",
        scramble_trigger(app_id)
    ))
    .execute(pool)
    .await
    .expect("create scramble trigger");
}

pub async fn unscramble(pool: &sqlx::PgPool, app_id: Uuid) {
    sqlx::query(&format!(
        "DROP TRIGGER IF EXISTS {} ON rust_tx_changes",
        scramble_trigger(app_id)
    ))
    .execute(pool)
    .await
    .expect("drop scramble trigger");
}

/// A tx's change rows on one attr as `(action, value)`, in physical (ctid)
/// order: what a reader relying on placement would see.
pub async fn physical_order(
    pool: &sqlx::PgPool,
    app_id: Uuid,
    tx_id: i64,
    attr_id: Uuid,
) -> Vec<(String, Value)> {
    sqlx::query(
        "SELECT action, value FROM rust_tx_changes
          WHERE app_id = $1 AND tx_id = $2 AND attr_id = $3 ORDER BY ctid",
    )
    .bind(app_id)
    .bind(tx_id)
    .bind(attr_id)
    .fetch_all(pool)
    .await
    .expect("read change log")
    .iter()
    .map(|r| (r.get("action"), r.get("value")))
    .collect()
}
