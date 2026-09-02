// In-flight indexing-job markers (issue #5): while a background job is
// rewriting an attr's triples the attr carries `indexing?` /
// `checking-data-type?` / `setting-unique?` and the query planner must treat
// it as not (yet) indexed / typed / unique. Port of the legacy `indexing?`
// and `uniqueing?` scenarios (instaql_test.clj:3843, :3913) plus the
// order-by / comparator guards (instaql.clj:951-969, attr_pat.clj:189-224).
mod common;

use common::*;
use instant_core::instaql::{query, QueryCtx};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

struct Fx {
    app: Uuid,
    age: Uuid,
    handle: Uuid,
}

/// users: id, handle (plain string), age (plain number). One user.
async fn fixture(pool: &PgPool) -> Fx {
    let app = mk_app(pool).await;
    let id = Uuid::new_v4();
    let handle = Uuid::new_v4();
    let age = Uuid::new_v4();
    let eid = Uuid::new_v4();
    transact_json(
        pool,
        app,
        json!([
            ["add-attr", {"id": id, "forward-identity": [Uuid::new_v4(), "users", "id"],
                          "value-type": "blob", "cardinality": "one", "unique?": true, "index?": true}],
            ["add-attr", {"id": handle, "forward-identity": [Uuid::new_v4(), "users", "handle"],
                          "value-type": "blob", "cardinality": "one", "unique?": false, "index?": false}],
            ["add-attr", {"id": age, "forward-identity": [Uuid::new_v4(), "users", "age"],
                          "value-type": "blob", "cardinality": "one", "unique?": false, "index?": false}],
            ["add-triple", eid, id, eid],
            ["add-triple", eid, handle, "dww"],
            ["add-triple", eid, age, 30]
        ]),
    )
    .await
    .unwrap();
    Fx { app, age, handle }
}

async fn set_attr(pool: &PgPool, attr: Uuid, set: &str) {
    sqlx::query(&format!("UPDATE attrs SET {set} WHERE id = $1"))
        .bind(attr)
        .execute(pool)
        .await
        .unwrap();
}

async fn count(pool: &PgPool, app: Uuid, q: Value) -> usize {
    let attrs = attrs_of(pool, app).await;
    let ctx = QueryCtx {
        app_id: app,
        attrs: &attrs,
        admin: true,
    };
    let mut conn = pool.acquire().await.unwrap();
    let res = query(&mut conn, &ctx, &q).await.unwrap();
    res.forms[0].entities.len()
}

async fn query_err(pool: &PgPool, app: Uuid, q: Value) -> instant_core::error::InstantError {
    let attrs = attrs_of(pool, app).await;
    let ctx = QueryCtx {
        app_id: app,
        attrs: &attrs,
        admin: true,
    };
    let mut conn = pool.acquire().await.unwrap();
    query(&mut conn, &ctx, &q).await.unwrap_err()
}

fn messages(e: &instant_core::error::InstantError) -> Vec<String> {
    e.hint
        .as_ref()
        .and_then(|h| h.get("errors"))
        .and_then(|v| v.as_array())
        .map(|errs| {
            errs.iter()
                .filter_map(|x| x.get("message").and_then(|m| m.as_str()))
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// legacy `indexing?`: the attr row says indexed + typed before its triples
/// are flagged; the typed index path would miss the row, the in-flight
/// marker sends the planner down the plain `ea` scan instead.
#[tokio::test]
async fn queries_ignore_indexes_while_still_indexing() {
    let pool = pool().await;
    let fx = fixture(&pool).await;
    let q = json!({"users": {"$": {"where": {"age": 30}}}});
    assert_eq!(count(&pool, fx.app, q.clone()).await, 1);

    // what a running index / check-data-type job looks like after its
    // update-attr-start stage (triples not flagged yet)
    set_attr(
        &pool,
        fx.age,
        "is_indexed = true, checked_data_type = 'number'",
    )
    .await;
    assert_eq!(
        count(&pool, fx.app, q.clone()).await,
        0,
        "incorrect indexes would break the query"
    );

    set_attr(&pool, fx.age, "indexing = true").await;
    assert_eq!(
        count(&pool, fx.app, q.clone()).await,
        1,
        "indexing? in progress saves the query"
    );

    set_attr(&pool, fx.age, "indexing = false, checking_data_type = true").await;
    assert_eq!(
        count(&pool, fx.app, q.clone()).await,
        1,
        "checking-data-type? in progress saves the query"
    );

    // $not on an in-flight attr: the isNull branch treats it as unindexed
    let q_not = json!({"users": {"$": {"where": {"age": {"$not": 31}}}}});
    assert_eq!(count(&pool, fx.app, q_not).await, 1);
}

#[tokio::test]
async fn attrs_report_in_flight_markers_on_the_wire() {
    let pool = pool().await;
    let fx = fixture(&pool).await;
    let wire = |attrs: &instant_core::attr::AttrMap, id: Uuid| attrs.get(&id).unwrap().to_wire();

    let attrs = attrs_of(&pool, fx.app).await;
    let a = wire(&attrs, fx.age);
    for k in ["indexing?", "checking-data-type?", "setting-unique?"] {
        assert!(a.get(k).is_none(), "{k} must be absent when false");
    }

    set_attr(&pool, fx.age, "is_indexed = true, indexing = true").await;
    set_attr(
        &pool,
        fx.handle,
        "is_unique = true, setting_unique = true, checked_data_type = 'string', checking_data_type = true",
    )
    .await;
    let attrs = attrs_of(&pool, fx.app).await;
    let a = wire(&attrs, fx.age);
    assert_eq!(a["index?"], json!(true));
    assert_eq!(a["indexing?"], json!(true));
    assert!(a.get("setting-unique?").is_none());
    let h = wire(&attrs, fx.handle);
    assert_eq!(h["unique?"], json!(true));
    assert_eq!(h["setting-unique?"], json!(true));
    assert_eq!(h["checked-data-type"], json!("string"));
    assert_eq!(h["checking-data-type?"], json!(true));

    let age = attrs.get(&fx.age).unwrap();
    assert!(!age.indexed_for_query());
    let handle = attrs.get(&fx.handle).unwrap();
    assert!(!handle.unique_for_query());
    assert!(handle.checked_type_for_query().is_none());
}

#[tokio::test]
async fn order_and_comparators_reject_in_flight_attrs() {
    let pool = pool().await;
    let fx = fixture(&pool).await;
    set_attr(
        &pool,
        fx.age,
        "is_indexed = true, checked_data_type = 'number', indexing = true",
    )
    .await;

    let e = query_err(
        &pool,
        fx.app,
        json!({"users": {"$": {"order": {"age": "asc"}}}}),
    )
    .await;
    assert_eq!(e.error_type, "validation-failed");
    assert_eq!(
        messages(&e),
        vec!["The `users.age` attribute is still in the process of indexing. It must finish before ordering by the attribute."]
    );

    let e = query_err(
        &pool,
        fx.app,
        json!({"users": {"$": {"where": {"age": {"$gt": 1}}}}}),
    )
    .await;
    assert_eq!(e.error_type, "validation-failed");
    assert!(
        e.message.contains("The `users.age` attribute is still in the process of indexing. It must finish before using comparison operators."),
        "{}",
        e.message
    );

    set_attr(&pool, fx.age, "indexing = false, checking_data_type = true").await;
    let e = query_err(
        &pool,
        fx.app,
        json!({"users": {"$": {"order": {"age": "desc"}}}}),
    )
    .await;
    assert_eq!(
        messages(&e),
        vec!["The `users.age` attribute is still in the process of validating its type. It must finish before ordering by the attribute."]
    );
    let e = query_err(
        &pool,
        fx.app,
        json!({"users": {"$": {"where": {"age": {"$lt": 100}}}}}),
    )
    .await;
    assert!(
        e.message.contains("still in the process of checking its data type. It must finish before using comparison operators."),
        "{}",
        e.message
    );

    // once the job is done the same queries work
    set_attr(&pool, fx.age, "checking_data_type = false").await;
    sqlx::query("UPDATE triples SET ave = true, checked_data_type = 'number' WHERE attr_id = $1")
        .bind(fx.age)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        count(
            &pool,
            fx.app,
            json!({"users": {"$": {"order": {"age": "asc"}}}})
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &pool,
            fx.app,
            json!({"users": {"$": {"where": {"age": {"$gt": 1}}}}})
        )
        .await,
        1
    );
}
