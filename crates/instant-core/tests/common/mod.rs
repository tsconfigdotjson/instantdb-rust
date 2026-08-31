// Shared across the integration-test binaries; each binary uses a subset.
#![allow(dead_code)]

use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use instant_core::attr::AttrMap;
use instant_core::error::Result;
use instant_core::system_catalog;
use instant_core::tx::{self, TxOptions, TxReport};

pub fn db_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://instant:instant@localhost:5432/instant".to_string())
}

pub async fn pool() -> PgPool {
    let pool = PgPool::connect(&db_url()).await.expect("connect postgres");
    system_catalog::ensure_system_catalog(&pool)
        .await
        .expect("system catalog");
    pool
}

/// Creates a fresh app (with a fresh creator user) for test isolation.
pub async fn mk_app(pool: &PgPool) -> Uuid {
    let user_id = Uuid::new_v4();
    let app_id = Uuid::new_v4();
    sqlx::query("INSERT INTO instant_users (id, email) VALUES ($1, $2)")
        .bind(user_id)
        .bind(format!("test-{user_id}@example.com"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO apps (id, creator_id, title) VALUES ($1, $2, 'test app')")
        .bind(app_id)
        .bind(user_id)
        .execute(pool)
        .await
        .unwrap();
    app_id
}

/// Run wire-format tx-steps in a fresh transaction, committing on success.
pub async fn transact_json(pool: &PgPool, app_id: Uuid, steps: Value) -> Result<TxReport> {
    let parsed = tx::parse_tx_steps(&steps)?;
    let mut attrs = instant_core::attr::get_by_app_id(pool, app_id).await?;
    let mut dbtx = pool
        .begin()
        .await
        .map_err(instant_core::error::InstantError::from)?;
    let report = tx::transact(&mut dbtx, app_id, &mut attrs, parsed, &TxOptions::default()).await?;
    dbtx.commit()
        .await
        .map_err(instant_core::error::InstantError::from)?;
    Ok(report)
}

pub async fn attrs_of(pool: &PgPool, app_id: Uuid) -> AttrMap {
    instant_core::attr::get_by_app_id(pool, app_id)
        .await
        .unwrap()
}

/// All triples of an entity as (attr_label, value) pairs, for assertions.
pub async fn entity_triples(pool: &PgPool, app_id: Uuid, eid: Uuid) -> Vec<(String, Value)> {
    let attrs = attrs_of(pool, app_id).await;
    let rows: Vec<(Uuid, Value)> = sqlx::query_as(
        "SELECT attr_id, value FROM triples WHERE app_id = $1 AND entity_id = $2 ORDER BY attr_id",
    )
    .bind(app_id)
    .bind(eid)
    .fetch_all(pool)
    .await
    .unwrap();
    rows.into_iter()
        .map(|(attr_id, value)| {
            let label = attrs
                .get(&attr_id)
                .map(|a| format!("{}.{}", a.etype, a.label))
                .unwrap_or_else(|| attr_id.to_string());
            (label, value)
        })
        .collect()
}

/// Convenience: create blob + ref attrs for a simple todos/owners schema.
/// Returns tx-steps that create the attrs (schemaless client style).
pub fn todo_schema_steps() -> (Value, SchemaIds) {
    let ids = SchemaIds {
        todos_id: Uuid::new_v4(),
        todos_title: Uuid::new_v4(),
        todos_done: Uuid::new_v4(),
        owners_id: Uuid::new_v4(),
        owners_name: Uuid::new_v4(),
        todos_owner: Uuid::new_v4(),
    };
    let steps = json!([
        ["add-attr", {"id": ids.todos_id, "forward-identity": [Uuid::new_v4(), "todos", "id"],
          "value-type": "blob", "cardinality": "one", "unique?": true, "index?": false}],
        ["add-attr", {"id": ids.todos_title, "forward-identity": [Uuid::new_v4(), "todos", "title"],
          "value-type": "blob", "cardinality": "one", "unique?": false, "index?": false}],
        ["add-attr", {"id": ids.todos_done, "forward-identity": [Uuid::new_v4(), "todos", "done"],
          "value-type": "blob", "cardinality": "one", "unique?": false, "index?": false}],
        ["add-attr", {"id": ids.owners_id, "forward-identity": [Uuid::new_v4(), "owners", "id"],
          "value-type": "blob", "cardinality": "one", "unique?": true, "index?": false}],
        ["add-attr", {"id": ids.owners_name, "forward-identity": [Uuid::new_v4(), "owners", "name"],
          "value-type": "blob", "cardinality": "one", "unique?": true, "index?": true}],
        ["add-attr", {"id": ids.todos_owner, "forward-identity": [Uuid::new_v4(), "todos", "owner"],
          "reverse-identity": [Uuid::new_v4(), "owners", "todos"],
          "value-type": "ref", "cardinality": "one", "unique?": false, "index?": false}]
    ]);
    (steps, ids)
}

#[derive(Debug, Clone, Copy)]
pub struct SchemaIds {
    pub todos_id: Uuid,
    pub todos_title: Uuid,
    pub todos_done: Uuid,
    pub owners_id: Uuid,
    pub owners_name: Uuid,
    pub todos_owner: Uuid,
}
