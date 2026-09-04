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
    transact_json(
        &pool,
        app,
        json!([["add-triple", eid, ids.todos_title, "v2"]]),
    )
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
        json!([["add-triple", lookup, ids.owners_id, lookup],]),
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
            [
                "add-triple",
                todo,
                ids.todos_owner,
                [ids.owners_name, "bob"]
            ]
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

/// Legacy `validate-mode` (transaction.clj:283-358) is one pre-pass over the
/// pre-tx state: existence is any triple of the etype, offenders are joined
/// into one message (in step order, with the offending steps as `input`),
/// lookups are checked as written, and steps earlier in the same tx do not
/// change the verdict.
#[tokio::test]
async fn mode_prepass_matches_legacy() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();

    // an entity with only a `title` triple (no id triple) still exists
    let e1 = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([["add-triple", e1, ids.todos_title, "t1"]]),
    )
    .await
    .unwrap();
    let err = transact_json(
        &pool,
        app,
        json!([["add-triple", e1, ids.todos_id, e1, {"mode": "create"}]]),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.message,
        format!("Validation failed for tx-step: Creating entities that exist: {e1}")
    );
    assert_eq!(err.hint.as_ref().unwrap()["data-type"], json!("tx-step"));
    assert_eq!(
        err.hint.as_ref().unwrap()["input"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    // the same uuid under another etype does not exist there (PR #1555)
    transact_json(
        &pool,
        app,
        json!([["add-triple", e1, ids.owners_id, e1, {"mode": "create"}]]),
    )
    .await
    .unwrap();

    // create-then-update in one tx: the update sees the pre-tx state
    let e2 = Uuid::new_v4();
    let err = transact_json(
        &pool,
        app,
        json!([
            ["add-triple", e2, ids.todos_id, e2, {"mode": "create"}],
            ["add-triple", e2, ids.todos_title, "x", {"mode": "update"}]
        ]),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.message,
        format!("Validation failed for tx-step: Updating entities that don't exist: {e2}")
    );

    // create-after-delete in one tx still sees the pre-tx entity
    let err = transact_json(
        &pool,
        app,
        json!([
            ["delete-entity", e1, "todos"],
            ["add-triple", e1, ids.todos_title, "again", {"mode": "create"}]
        ]),
    )
    .await
    .unwrap_err();
    assert!(err.message.contains("Creating entities that exist"));

    // every offender in one message, in step order, duplicates included
    let e3 = Uuid::new_v4();
    let err = transact_json(
        &pool,
        app,
        json!([
            ["add-triple", e3, ids.todos_title, "a", {"mode": "update"}],
            ["add-triple", e1, ids.todos_title, "b", {"mode": "update"}],
            ["add-triple", e3, ids.todos_done, true, {"mode": "update"}]
        ]),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.message,
        format!("Validation failed for tx-step: Updating entities that don't exist: {e3}, {e3}")
    );

    // an update-mode lookup that misses is a mode error naming the lookup,
    // printed the way Clojure prints the coerced vector
    let err = transact_json(
        &pool,
        app,
        json!([[
            "add-triple",
            [ids.owners_name, "nobody"],
            ids.owners_name,
            "nobody",
            {"mode": "update"}
        ]]),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.message,
        format!(
            "Validation failed for tx-step: Updating entities that don't exist: [#uuid \"{}\" \"nobody\"]",
            ids.owners_name
        )
    );
    // a create-mode lookup that resolves is an error on the admin path too
    let o = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", o, ids.owners_id, o],
            ["add-triple", o, ids.owners_name, "ann"]
        ]),
    )
    .await
    .unwrap();
    let err = transact_json(
        &pool,
        app,
        json!([[
            "add-triple",
            [ids.owners_name, "ann"],
            ids.owners_name,
            "ann",
            {"mode": "create"}
        ]]),
    )
    .await
    .unwrap_err();
    // (the non-admin path prints the resolved uuid, like legacy's
    // resolve-lookups-tx-steps rewrite; the admin path prints the vector)
    assert_eq!(
        err.message,
        format!("Validation failed for tx-step: Creating entities that exist: {o}")
    );
    // valid creates and updates mixed in one tx pass
    let e4 = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", e4, ids.todos_id, e4, {"mode": "create"}],
            ["add-triple", e2, ids.todos_title, "y", {"mode": "upsert"}],
            ["deep-merge-triple", o, ids.owners_name, "ann2", {"mode": "update"}]
        ]),
    )
    .await
    .unwrap();
}

/// Legacy `::tx-steps` specs (transaction.clj:25-69): an unknown `mode`, a
/// non-map `opts`, a non-string etype, non-map rule-params, a missing value
/// and trailing elements all fail with the bare "Validation failed for
/// tx-steps"; a bad entity id gets the friendly message.
#[tokio::test]
async fn step_shape_specs() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    let e = Uuid::new_v4();
    let pool_ref = &pool;
    let bare = move |steps: Value| async move {
        let err = transact_json(pool_ref, app, steps).await.unwrap_err();
        assert_eq!(err.error_type, "validation-failed");
        assert_eq!(err.message, "Validation failed for tx-steps");
        let hint = err.hint.unwrap();
        assert_eq!(hint["data-type"], json!("tx-steps"));
        assert!(hint["input"].is_array());
        assert!(hint["errors"][0].get("in").is_some());
    };
    bare(json!([["add-triple", e, ids.todos_title, "x", {"mode": "replace"}]])).await;
    bare(json!([["add-triple", e, ids.todos_title, "x", "create"]])).await;
    bare(json!([["add-triple", e, ids.todos_title]])).await;
    bare(json!([["retract-triple", e, ids.todos_title, "x", {"mode": "create"}]])).await;
    bare(json!([["delete-entity", e, 42]])).await;
    bare(json!([["rule-params", e, "todos", "not-a-map"]])).await;
    bare(json!([["rule-params", e]])).await;
    bare(json!([["delete-attr", ids.todos_done, "extra"]])).await;
    // `upsert` is a legal mode; a null opts slot is fine
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", e, ids.todos_id, e, {"mode": "upsert"}],
            ["add-triple", e, ids.todos_title, "x", null]
        ]),
    )
    .await
    .unwrap();
    let err = transact_json(
        &pool,
        app,
        json!([["add-triple", "not-an-id", ids.todos_title, "x"]]),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.message,
        "Validation failed for tx-steps: Invalid entity ID 'not-an-id'. Entity IDs must be UUIDs. Use id() or lookup() to generate a valid UUID."
    );
    assert_eq!(err.hint.unwrap()["errors"][0]["in"], json!([0, 1]));
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
    transact_json(
        &pool,
        app,
        json!([["add-triple", owner, ids.owners_id, owner]]),
    )
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
