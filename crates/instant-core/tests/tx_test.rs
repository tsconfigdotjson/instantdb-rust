mod common;

use common::*;
use serde_json::{json, Value};
use uuid::Uuid;

#[tokio::test]
async fn creates_attrs_and_triples() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();

    let attrs = attrs_of(&pool, app).await;
    let title = attrs.by_fwd_name("todos", "title").unwrap();
    assert_eq!(title.id, ids.todos_title);
    assert!(attrs.by_rev_name("owners", "todos").is_some());

    let eid = Uuid::new_v4();
    let report = transact_json(
        &pool,
        app,
        json!([
            ["add-triple", eid, ids.todos_id, eid],
            ["add-triple", eid, ids.todos_title, "buy milk"],
            ["add-triple", eid, ids.todos_done, false]
        ]),
    )
    .await
    .unwrap();
    assert!(report.tx_id > 0);
    assert_eq!(report.created.len(), 1);

    let triples = entity_triples(&pool, app, eid).await;
    assert!(triples.contains(&("todos.title".to_string(), json!("buy milk"))));
    assert!(triples.contains(&("todos.done".to_string(), json!(false))));
}

#[tokio::test]
async fn cardinality_one_upsert_replaces() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();

    let eid = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", eid, ids.todos_id, eid],
            ["add-triple", eid, ids.todos_title, "v1"]
        ]),
    )
    .await
    .unwrap();
    transact_json(&pool, app, json!([["add-triple", eid, ids.todos_title, "v2"]]))
        .await
        .unwrap();

    let titles: Vec<(String, Value)> = entity_triples(&pool, app, eid)
        .await
        .into_iter()
        .filter(|(l, _)| l == "todos.title")
        .collect();
    assert_eq!(titles.len(), 1);
    assert_eq!(titles[0].1, json!("v2"));
}

#[tokio::test]
async fn many_cardinality_refs_accumulate() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    // A many-cardinality ref: todos.tags
    let tag_attr = Uuid::new_v4();
    let todos_id = Uuid::new_v4();
    let tags_id = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([
            ["add-attr", {"id": todos_id, "forward-identity": [Uuid::new_v4(), "todos", "id"],
              "value-type": "blob", "cardinality": "one", "unique?": true, "index?": false}],
            ["add-attr", {"id": tags_id, "forward-identity": [Uuid::new_v4(), "tags", "id"],
              "value-type": "blob", "cardinality": "one", "unique?": true, "index?": false}],
            ["add-attr", {"id": tag_attr, "forward-identity": [Uuid::new_v4(), "todos", "tags"],
              "reverse-identity": [Uuid::new_v4(), "tags", "todos"],
              "value-type": "ref", "cardinality": "many", "unique?": false, "index?": false}]
        ]),
    )
    .await
    .unwrap();

    let todo = Uuid::new_v4();
    let (t1, t2) = (Uuid::new_v4(), Uuid::new_v4());
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", todo, todos_id, todo],
            ["add-triple", t1, tags_id, t1],
            ["add-triple", t2, tags_id, t2],
            ["add-triple", todo, tag_attr, t1],
            ["add-triple", todo, tag_attr, t2],
            ["add-triple", todo, tag_attr, t1]
        ]),
    )
    .await
    .unwrap();

    let tags: Vec<_> = entity_triples(&pool, app, todo)
        .await
        .into_iter()
        .filter(|(l, _)| l == "todos.tags")
        .collect();
    assert_eq!(tags.len(), 2);

    // retract one
    transact_json(&pool, app, json!([["retract-triple", todo, tag_attr, t1]]))
        .await
        .unwrap();
    let tags: Vec<_> = entity_triples(&pool, app, todo)
        .await
        .into_iter()
        .filter(|(l, _)| l == "todos.tags")
        .collect();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].1, json!(t2));
}

#[tokio::test]
async fn unique_violation_errors() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();

    let (o1, o2) = (Uuid::new_v4(), Uuid::new_v4());
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", o1, ids.owners_id, o1],
            ["add-triple", o1, ids.owners_name, "alice"]
        ]),
    )
    .await
    .unwrap();
    let err = transact_json(
        &pool,
        app,
        json!([
            ["add-triple", o2, ids.owners_id, o2],
            ["add-triple", o2, ids.owners_name, "alice"]
        ]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "record-not-unique");
}

#[tokio::test]
async fn lookup_refs_upsert_and_resolve() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();

    // Create by lookup: owners with unique name "bob"
    let lookup = json!([ids.owners_name, "bob"]);
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", lookup, ids.owners_id, lookup],
        ]),
    )
    .await
    .unwrap();

    // Find bob's eid
    let rows: Vec<(Uuid,)> = sqlx::query_as(
        "SELECT entity_id FROM triples WHERE app_id = $1 AND attr_id = $2 AND value = '\"bob\"'::jsonb",
    )
    .bind(app)
    .bind(ids.owners_name)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    let bob = rows[0].0;

    // Entity should have an id triple whose value equals its entity id
    let triples = entity_triples(&pool, app, bob).await;
    assert!(triples.contains(&("owners.id".to_string(), json!(bob))));

    // Update via the same lookup adds data to the same entity
    let todo = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", todo, ids.todos_id, todo],
            ["add-triple", todo, ids.todos_owner, [ids.owners_name, "bob"]]
        ]),
    )
    .await
    .unwrap();
    let triples = entity_triples(&pool, app, todo).await;
    assert!(triples.contains(&("todos.owner".to_string(), json!(bob))));
}

#[tokio::test]
async fn deep_merge_merges_and_deletes_keys() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();

    let eid = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", eid, ids.todos_id, eid],
            ["add-triple", eid, ids.todos_title, {"a": {"b": 1}, "keep": true}]
        ]),
    )
    .await
    .unwrap();
    transact_json(
        &pool,
        app,
        json!([
            ["deep-merge-triple", eid, ids.todos_title, {"a": {"c": 2}, "keep": null}]
        ]),
    )
    .await
    .unwrap();
    let title: Value = entity_triples(&pool, app, eid)
        .await
        .into_iter()
        .find(|(l, _)| l == "todos.title")
        .unwrap()
        .1;
    assert_eq!(title, json!({"a": {"b": 1, "c": 2}}));
}

#[tokio::test]
async fn delete_entity_removes_triples_and_reverse_refs() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();

    let owner = Uuid::new_v4();
    let todo = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", owner, ids.owners_id, owner],
            ["add-triple", owner, ids.owners_name, "carol"],
            ["add-triple", todo, ids.todos_id, todo],
            ["add-triple", todo, ids.todos_owner, owner]
        ]),
    )
    .await
    .unwrap();

    // Deleting the owner removes the ref triple on todo, but not todo itself.
    transact_json(&pool, app, json!([["delete-entity", owner, "owners"]]))
        .await
        .unwrap();
    assert!(entity_triples(&pool, app, owner).await.is_empty());
    let todo_triples = entity_triples(&pool, app, todo).await;
    assert!(!todo_triples.iter().any(|(l, _)| l == "todos.owner"));
    assert!(todo_triples.iter().any(|(l, _)| l == "todos.id"));
}

#[tokio::test]
async fn cascade_delete_follows_on_delete() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    // comments belong to posts with on-delete cascade (deleting post deletes comments)
    let posts_id = Uuid::new_v4();
    let comments_id = Uuid::new_v4();
    let comment_post = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([
            ["add-attr", {"id": posts_id, "forward-identity": [Uuid::new_v4(), "posts", "id"],
              "value-type": "blob", "cardinality": "one", "unique?": true, "index?": false}],
            ["add-attr", {"id": comments_id, "forward-identity": [Uuid::new_v4(), "comments", "id"],
              "value-type": "blob", "cardinality": "one", "unique?": true, "index?": false}],
            ["add-attr", {"id": comment_post, "forward-identity": [Uuid::new_v4(), "comments", "post"],
              "reverse-identity": [Uuid::new_v4(), "posts", "comments"],
              "value-type": "ref", "cardinality": "one", "unique?": false, "index?": false,
              "on-delete": "cascade"}]
        ]),
    )
    .await
    .unwrap();

    let post = Uuid::new_v4();
    let (c1, c2) = (Uuid::new_v4(), Uuid::new_v4());
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", post, posts_id, post],
            ["add-triple", c1, comments_id, c1],
            ["add-triple", c2, comments_id, c2],
            ["add-triple", c1, comment_post, post],
            ["add-triple", c2, comment_post, post]
        ]),
    )
    .await
    .unwrap();

    let report = transact_json(&pool, app, json!([["delete-entity", post, "posts"]]))
        .await
        .unwrap();
    assert_eq!(report.deleted.len(), 3);
    assert!(entity_triples(&pool, app, c1).await.is_empty());
    assert!(entity_triples(&pool, app, c2).await.is_empty());
}

#[tokio::test]
async fn create_and_update_modes() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();

    let eid = Uuid::new_v4();
    // update mode on a nonexistent entity fails
    let err = transact_json(
        &pool,
        app,
        json!([["add-triple", eid, ids.todos_title, "x", {"mode": "update"}]]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "validation-failed");

    // create works
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", eid, ids.todos_id, eid, {"mode": "create"}],
            ["add-triple", eid, ids.todos_title, "x", {"mode": "create"}]
        ]),
    )
    .await
    .unwrap();

    // create again fails
    let err = transact_json(
        &pool,
        app,
        json!([["add-triple", eid, ids.todos_id, eid, {"mode": "create"}]]),
    )
    .await
    .unwrap_err();
    assert!(err.message.contains("Creating entities that exist"));
}

#[tokio::test]
async fn indexed_attr_backfills_nulls() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();

    // owners.name is indexed; creating an owner without a name should
    // backfill a null row so ave scans can find it.
    let owner = Uuid::new_v4();
    transact_json(&pool, app, json!([["add-triple", owner, ids.owners_id, owner]]))
        .await
        .unwrap();
    let triples = entity_triples(&pool, app, owner).await;
    assert!(triples.contains(&("owners.name".to_string(), Value::Null)));
}

#[tokio::test]
async fn system_catalog_writes_blocked() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let attrs = attrs_of(&pool, app).await;
    let email_attr = attrs.by_fwd_name("$users", "email").unwrap().id;
    let eid = Uuid::new_v4();
    let err = transact_json(
        &pool,
        app,
        json!([["add-triple", eid, email_attr, "evil@example.com"]]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "validation-failed");
}
