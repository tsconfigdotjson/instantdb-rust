mod common;

use common::*;
use instant_core::instaql::{query, QueryCtx};
use instant_core::perms::{self, AuthCtx, PermsFilter, Rules};
use instant_core::tx;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

async fn set_rules(pool: &PgPool, app: Uuid, code: Value) {
    sqlx::query(
        "INSERT INTO rules (app_id, code) VALUES ($1, $2)
         ON CONFLICT (app_id) DO UPDATE SET code = $2",
    )
    .bind(app)
    .bind(code)
    .execute(pool)
    .await
    .unwrap();
}

async fn run_filtered(
    pool: &PgPool,
    app: Uuid,
    auth: &AuthCtx,
    q: Value,
) -> instant_core::instaql::QueryResult {
    let attrs = attrs_of(pool, app).await;
    let ctx = QueryCtx { app_id: app, attrs: &attrs, admin: true };
    let mut conn = pool.acquire().await.unwrap();
    let mut result = query(&mut conn, &ctx, &q).await.unwrap();
    let rules = Rules::load(&mut conn, app).await.unwrap();
    let rule_params = q.get("$$ruleParams").cloned().unwrap_or(json!({}));
    let filter = PermsFilter { rules: &rules, auth, rule_params };
    filter.filter(&mut conn, app, &attrs, &mut result).await.unwrap();
    result
}

async fn transact_with_perms(
    pool: &PgPool,
    app: Uuid,
    auth: &AuthCtx,
    steps: Value,
) -> instant_core::error::Result<instant_core::tx::TxReport> {
    let parsed = tx::parse_tx_steps(&steps)?;
    let mut attrs = attrs_of(pool, app).await;
    let mut dbtx = pool.begin().await.unwrap();
    let rules = Rules::load(&mut dbtx, app).await?;
    let report = perms::permissioned_transact(
        &mut dbtx, app, &mut attrs, parsed, &rules, auth, &json!({}),
    )
    .await?;
    dbtx.commit().await.unwrap();
    Ok(report)
}

/// creates a $users row directly (system transact path)
async fn mk_user(pool: &PgPool, app: Uuid, email: &str) -> Uuid {
    use instant_core::system_catalog as sc;
    use instant_core::tx::TxOptions;
    let uid = Uuid::new_v4();
    let steps = json!([
        ["add-triple", uid, sc::attr_id("$users", "id"), uid],
        ["add-triple", uid, sc::attr_id("$users", "email"), email]
    ]);
    let parsed = tx::parse_tx_steps(&steps).unwrap();
    let mut attrs = attrs_of(pool, app).await;
    let mut dbtx = pool.begin().await.unwrap();
    tx::transact(
        &mut dbtx,
        app,
        &mut attrs,
        parsed,
        &TxOptions { allow_system_catalog_writes: true },
    )
    .await
    .unwrap();
    dbtx.commit().await.unwrap();
    uid
}

#[tokio::test]
async fn default_rules_allow_user_namespaces() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    let e = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([["add-triple", e, ids.todos_id, e], ["add-triple", e, ids.todos_title, "t"]]),
    )
    .await
    .unwrap();

    // anonymous can view with no rules
    let res = run_filtered(&pool, app, &AuthCtx::default(), json!({"todos": {}})).await;
    assert_eq!(res.forms[0].entities.len(), 1);

    // anonymous can write with no rules
    let e2 = Uuid::new_v4();
    transact_with_perms(
        &pool,
        app,
        &AuthCtx::default(),
        json!([["add-triple", e2, ids.todos_id, e2]]),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn view_rule_filters_entities() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    set_rules(
        &pool,
        app,
        json!({"todos": {"allow": {"view": "data.done == true"}}}),
    )
    .await;

    let (e1, e2) = (Uuid::new_v4(), Uuid::new_v4());
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", e1, ids.todos_id, e1],
            ["add-triple", e1, ids.todos_done, true],
            ["add-triple", e2, ids.todos_id, e2],
            ["add-triple", e2, ids.todos_done, false]
        ]),
    )
    .await
    .unwrap();

    let res = run_filtered(&pool, app, &AuthCtx::default(), json!({"todos": {}})).await;
    assert_eq!(res.forms[0].entities.len(), 1);
    assert_eq!(res.forms[0].entities[0].eid, e1);
}

#[tokio::test]
async fn auth_binding_and_owner_rule() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    let alice = mk_user(&pool, app, "alice@example.com").await;
    let bob = mk_user(&pool, app, "bob@example.com").await;

    // todos.creatorId stores the owner's id as a string
    let creator_attr = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([["add-attr", {"id": creator_attr,
            "forward-identity": [Uuid::new_v4(), "todos", "creatorId"],
            "value-type": "blob", "cardinality": "one", "unique?": false, "index?": false}]]),
    )
    .await
    .unwrap();
    set_rules(
        &pool,
        app,
        json!({"todos": {"allow": {
            "view": "auth.id != null && data.creatorId == auth.id",
            "update": "data.creatorId == auth.id",
            "delete": "data.creatorId == auth.id"
        }}}),
    )
    .await;

    let e = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", e, ids.todos_id, e],
            ["add-triple", e, creator_attr, alice]
        ]),
    )
    .await
    .unwrap();

    let alice_auth = AuthCtx { user_id: Some(alice), user_map: None };
    let bob_auth = AuthCtx { user_id: Some(bob), user_map: None };

    let res = run_filtered(&pool, app, &alice_auth, json!({"todos": {}})).await;
    assert_eq!(res.forms[0].entities.len(), 1);
    let res = run_filtered(&pool, app, &bob_auth, json!({"todos": {}})).await;
    assert_eq!(res.forms[0].entities.len(), 0);
    let res = run_filtered(&pool, app, &AuthCtx::default(), json!({"todos": {}})).await;
    assert_eq!(res.forms[0].entities.len(), 0);

    // update by bob denied, by alice allowed
    let err = transact_with_perms(
        &pool,
        app,
        &bob_auth,
        json!([["add-triple", e, ids.todos_title, "hacked"]]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "permission-denied");

    transact_with_perms(
        &pool,
        app,
        &alice_auth,
        json!([["add-triple", e, ids.todos_title, "mine"]]),
    )
    .await
    .unwrap();

    // delete by bob denied
    let err = transact_with_perms(
        &pool,
        app,
        &bob_auth,
        json!([["delete-entity", e, "todos"]]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "permission-denied");
}

#[tokio::test]
async fn create_rule_checks_new_data() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    set_rules(
        &pool,
        app,
        json!({"todos": {"allow": {"create": "newData.title != 'forbidden'"}}}),
    )
    .await;

    let e = Uuid::new_v4();
    let err = transact_with_perms(
        &pool,
        app,
        &AuthCtx::default(),
        json!([
            ["add-triple", e, ids.todos_id, e],
            ["add-triple", e, ids.todos_title, "forbidden"]
        ]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "permission-denied");

    let e2 = Uuid::new_v4();
    transact_with_perms(
        &pool,
        app,
        &AuthCtx::default(),
        json!([
            ["add-triple", e2, ids.todos_id, e2],
            ["add-triple", e2, ids.todos_title, "allowed"]
        ]),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn data_ref_rule() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    // view todos only when the owner's name is 'alice' (via data.ref)
    set_rules(
        &pool,
        app,
        json!({"todos": {"allow": {"view": "'alice' in data.ref('owner.name')"}}}),
    )
    .await;

    let (owner_a, owner_b) = (Uuid::new_v4(), Uuid::new_v4());
    let (t1, t2) = (Uuid::new_v4(), Uuid::new_v4());
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", owner_a, ids.owners_id, owner_a],
            ["add-triple", owner_a, ids.owners_name, "alice"],
            ["add-triple", owner_b, ids.owners_id, owner_b],
            ["add-triple", owner_b, ids.owners_name, "bob"],
            ["add-triple", t1, ids.todos_id, t1],
            ["add-triple", t1, ids.todos_owner, owner_a],
            ["add-triple", t2, ids.todos_id, t2],
            ["add-triple", t2, ids.todos_owner, owner_b]
        ]),
    )
    .await
    .unwrap();

    let res = run_filtered(&pool, app, &AuthCtx::default(), json!({"todos": {}})).await;
    assert_eq!(res.forms[0].entities.len(), 1);
    assert_eq!(res.forms[0].entities[0].eid, t1);
}

#[tokio::test]
async fn bind_expansion() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    set_rules(
        &pool,
        app,
        json!({"todos": {
            "allow": {"view": "isDone"},
            "bind": ["isDone", "data.done == true"]
        }}),
    )
    .await;
    let (e1, e2) = (Uuid::new_v4(), Uuid::new_v4());
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", e1, ids.todos_id, e1],
            ["add-triple", e1, ids.todos_done, true],
            ["add-triple", e2, ids.todos_id, e2],
            ["add-triple", e2, ids.todos_done, false]
        ]),
    )
    .await
    .unwrap();
    let res = run_filtered(&pool, app, &AuthCtx::default(), json!({"todos": {}})).await;
    assert_eq!(res.forms[0].entities.len(), 1);
    assert_eq!(res.forms[0].entities[0].eid, e1);
}

#[tokio::test]
async fn rule_params_flow() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    set_rules(
        &pool,
        app,
        json!({"todos": {"allow": {"view": "data.title == ruleParams.secret"}}}),
    )
    .await;
    let e = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", e, ids.todos_id, e],
            ["add-triple", e, ids.todos_title, "opensesame"]
        ]),
    )
    .await
    .unwrap();

    let res = run_filtered(
        &pool,
        app,
        &AuthCtx::default(),
        json!({"todos": {}, "$$ruleParams": {"secret": "opensesame"}}),
    )
    .await;
    assert_eq!(res.forms[0].entities.len(), 1);

    let res = run_filtered(
        &pool,
        app,
        &AuthCtx::default(),
        json!({"todos": {}, "$$ruleParams": {"secret": "wrong"}}),
    )
    .await;
    assert_eq!(res.forms[0].entities.len(), 0);
}

#[tokio::test]
async fn users_default_rules() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let alice = mk_user(&pool, app, "alice2@example.com").await;
    let bob = mk_user(&pool, app, "bob2@example.com").await;

    // alice sees only herself
    let alice_auth = AuthCtx { user_id: Some(alice), user_map: None };
    let res = run_filtered(&pool, app, &alice_auth, json!({"$users": {}})).await;
    assert_eq!(res.forms[0].entities.len(), 1);
    assert_eq!(res.forms[0].entities[0].eid, alice);

    // anonymous sees nobody
    let res = run_filtered(&pool, app, &AuthCtx::default(), json!({"$users": {}})).await;
    assert_eq!(res.forms[0].entities.len(), 0);

    // bob can't delete alice
    let bob_auth = AuthCtx { user_id: Some(bob), user_map: None };
    let err = transact_with_perms(
        &pool,
        app,
        &bob_auth,
        json!([["delete-entity", alice, "$users"]]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "permission-denied");
}

#[tokio::test]
async fn default_namespace_rule_applies() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    set_rules(&pool, app, json!({"$default": {"allow": {"view": "false"}}})).await;
    let e = Uuid::new_v4();
    transact_json(&pool, app, json!([["add-triple", e, ids.todos_id, e]]))
        .await
        .unwrap();
    let res = run_filtered(&pool, app, &AuthCtx::default(), json!({"todos": {}})).await;
    assert_eq!(res.forms[0].entities.len(), 0);
}
