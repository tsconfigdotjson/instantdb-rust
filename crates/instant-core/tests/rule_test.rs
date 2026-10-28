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
    let ctx = QueryCtx {
        app_id: app,
        attrs: &attrs,
        admin: true,
    };
    let mut conn = pool.acquire().await.unwrap();
    let mut result = query(&mut conn, &ctx, &q).await.unwrap();
    let rules = Rules::load(&mut conn, app).await.unwrap();
    let rule_params = q.get("$$ruleParams").cloned().unwrap_or(json!({}));
    let filter = PermsFilter {
        rules: &rules,
        auth,
        rule_params,
    };
    filter
        .filter(&mut conn, app, &attrs, &mut result)
        .await
        .unwrap();
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
    let report =
        perms::permissioned_transact(&mut dbtx, app, &mut attrs, parsed, &rules, auth, &json!({}))
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
        &TxOptions {
            allow_system_catalog_writes: true,
            ..TxOptions::default()
        },
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
        json!([
            ["add-triple", e, ids.todos_id, e],
            ["add-triple", e, ids.todos_title, "t"]
        ]),
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
async fn type_function_in_rules() {
    // cel-java declares the type identifiers in every rule environment, so
    // `type(x) == string` is a valid rule on the legacy server, not an
    // undeclared reference
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    set_rules(
        &pool,
        app,
        json!({"todos": {"allow": {"view": "type(data.done) == bool && type(data.title) != int"}}}),
    )
    .await;

    let (e1, e2, e3) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", e1, ids.todos_id, e1],
            ["add-triple", e1, ids.todos_done, true],
            ["add-triple", e1, ids.todos_title, "a"],
            ["add-triple", e2, ids.todos_id, e2],
            ["add-triple", e2, ids.todos_done, "yes"],
            ["add-triple", e3, ids.todos_id, e3],
            ["add-triple", e3, ids.todos_done, false],
            ["add-triple", e3, ids.todos_title, 7]
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

    let alice_auth = AuthCtx {
        user_id: Some(alice),
        user_map: None,
        request: Default::default(),
    };
    let bob_auth = AuthCtx {
        user_id: Some(bob),
        user_map: None,
        request: Default::default(),
    };

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
    let alice_auth = AuthCtx {
        user_id: Some(alice),
        user_map: None,
        request: Default::default(),
    };
    let res = run_filtered(&pool, app, &alice_auth, json!({"$users": {}})).await;
    assert_eq!(res.forms[0].entities.len(), 1);
    assert_eq!(res.forms[0].entities[0].eid, alice);

    // anonymous sees nobody
    let res = run_filtered(&pool, app, &AuthCtx::default(), json!({"$users": {}})).await;
    assert_eq!(res.forms[0].entities.len(), 0);

    // bob can't delete alice: legacy validate-system-delete-entity! rejects
    // non-admin $users deletes before any rule runs
    let bob_auth = AuthCtx {
        user_id: Some(bob),
        user_map: None,
        request: Default::default(),
    };
    let err = transact_with_perms(
        &pool,
        app,
        &bob_auth,
        json!([["delete-entity", alice, "$users"]]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "validation-failed");
    assert_eq!(
        err.message,
        "Validation failed for tx-step: $users is a system entity. You aren't allowed to delete this directly."
    );

    // an upgraded guest row (linkedPrimaryUser -> alice) is visible to alice
    // through the default rule's linkedPrimaryUser clause (rule.clj:198-210)
    let guest = mk_user(&pool, app, "guest2@example.com").await;
    {
        use instant_core::system_catalog as sc;
        use instant_core::tx::TxOptions;
        let steps = json!([[
            "add-triple",
            guest,
            sc::attr_id("$users", "linkedPrimaryUser"),
            alice
        ]]);
        let parsed = tx::parse_tx_steps(&steps).unwrap();
        let mut attrs = attrs_of(&pool, app).await;
        let mut dbtx = pool.begin().await.unwrap();
        tx::transact(
            &mut dbtx,
            app,
            &mut attrs,
            parsed,
            &TxOptions {
                allow_system_catalog_writes: true,
                ..TxOptions::default()
            },
        )
        .await
        .unwrap();
        dbtx.commit().await.unwrap();
    }
    let res = run_filtered(&pool, app, &alice_auth, json!({"$users": {}})).await;
    let mut seen: Vec<Uuid> = res.forms[0].entities.iter().map(|e| e.eid).collect();
    seen.sort();
    let mut want = vec![alice, guest];
    want.sort();
    assert_eq!(seen, want);
    let res = run_filtered(&pool, app, &bob_auth, json!({"$users": {}})).await;
    assert_eq!(res.forms[0].entities.len(), 1);
}

#[tokio::test]
async fn default_namespace_rule_applies() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    set_rules(
        &pool,
        app,
        json!({"$default": {"allow": {"view": "false"}}}),
    )
    .await;
    let e = Uuid::new_v4();
    transact_json(&pool, app, json!([["add-triple", e, ids.todos_id, e]]))
        .await
        .unwrap();
    let res = run_filtered(&pool, app, &AuthCtx::default(), json!({"todos": {}})).await;
    assert_eq!(res.forms[0].entities.len(), 0);
}

#[tokio::test]
async fn explicit_link_rules() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    // linking a todo to an owner is allowed only when linkedData.name == 'boss'
    set_rules(
        &pool,
        app,
        json!({"todos": {"allow": {"link": {"owner": "linkedData.name == 'boss'"}}}}),
    )
    .await;

    let (boss, peon) = (Uuid::new_v4(), Uuid::new_v4());
    let (t1, t2) = (Uuid::new_v4(), Uuid::new_v4());
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", boss, ids.owners_id, boss],
            ["add-triple", boss, ids.owners_name, "boss"],
            ["add-triple", peon, ids.owners_id, peon],
            ["add-triple", peon, ids.owners_name, "peon"],
            ["add-triple", t1, ids.todos_id, t1],
            ["add-triple", t2, ids.todos_id, t2]
        ]),
    )
    .await
    .unwrap();

    // link to boss: allowed
    transact_with_perms(
        &pool,
        app,
        &AuthCtx::default(),
        json!([["add-triple", t1, ids.todos_owner, boss]]),
    )
    .await
    .unwrap();

    // link to peon: denied by the explicit link rule
    let err = transact_with_perms(
        &pool,
        app,
        &AuthCtx::default(),
        json!([["add-triple", t2, ids.todos_owner, peon]]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "permission-denied");
}

#[tokio::test]
async fn explicit_unlink_rules() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    set_rules(
        &pool,
        app,
        json!({"todos": {"allow": {"unlink": {"$default": "false"}}}}),
    )
    .await;
    let (owner, t1) = (Uuid::new_v4(), Uuid::new_v4());
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", owner, ids.owners_id, owner],
            ["add-triple", t1, ids.todos_id, t1],
            ["add-triple", t1, ids.todos_owner, owner]
        ]),
    )
    .await
    .unwrap();
    let err = transact_with_perms(
        &pool,
        app,
        &AuthCtx::default(),
        json!([["retract-triple", t1, ids.todos_owner, owner]]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "permission-denied");
}

#[tokio::test]
async fn missing_keys_are_null_safe() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    let e = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", e, ids.todos_id, e],
            ["add-triple", e, ids.todos_title, "t"]
        ]),
    )
    .await
    .unwrap();

    // keys absent from the schema read as null instead of erroring to deny
    set_rules(
        &pool,
        app,
        json!({"todos": {"allow": {"view": "data.someTypo == null && ruleParams.unknown == null && auth.missing == null"}}}),
    )
    .await;
    let res = run_filtered(&pool, app, &AuthCtx::default(), json!({"todos": {}})).await;
    assert_eq!(res.forms[0].entities.len(), 1);

    // a typo'd key used as the whole rule is a null result: deny, not error
    set_rules(
        &pool,
        app,
        json!({"todos": {"allow": {"view": "data.someTypo"}}}),
    )
    .await;
    let res = run_filtered(&pool, app, &AuthCtx::default(), json!({"todos": {}})).await;
    assert_eq!(res.forms[0].entities.len(), 0);

    // same on the transact path: update rule over a missing key denies
    // cleanly instead of erroring
    set_rules(
        &pool,
        app,
        json!({"todos": {"allow": {"update": "newData.someTypo == 'x'"}}}),
    )
    .await;
    let err = transact_with_perms(
        &pool,
        app,
        &AuthCtx::default(),
        json!([["add-triple", e, ids.todos_title, "edited"]]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "permission-denied");

    // and allows when the rule expects null
    set_rules(
        &pool,
        app,
        json!({"todos": {"allow": {"update": "newData.someTypo == null"}}}),
    )
    .await;
    transact_with_perms(
        &pool,
        app,
        &AuthCtx::default(),
        json!([["add-triple", e, ids.todos_title, "edited"]]),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn field_rules_filter_columns() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    // title only visible when done == true
    set_rules(
        &pool,
        app,
        json!({"todos": {"fields": {"title": "data.done == true"}}}),
    )
    .await;
    let (e1, e2) = (Uuid::new_v4(), Uuid::new_v4());
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", e1, ids.todos_id, e1],
            ["add-triple", e1, ids.todos_title, "visible"],
            ["add-triple", e1, ids.todos_done, true],
            ["add-triple", e2, ids.todos_id, e2],
            ["add-triple", e2, ids.todos_title, "hidden"],
            ["add-triple", e2, ids.todos_done, false]
        ]),
    )
    .await
    .unwrap();
    let res = run_filtered(&pool, app, &AuthCtx::default(), json!({"todos": {}})).await;
    assert_eq!(res.forms[0].entities.len(), 2);
    let titles: Vec<Option<&str>> = res.forms[0]
        .entities
        .iter()
        .map(|e| {
            e.triples
                .iter()
                .find(|t| t.a == ids.todos_title)
                .and_then(|t| t.v.as_str())
        })
        .collect();
    assert!(titles.contains(&Some("visible")));
    assert!(!titles.contains(&Some("hidden")));
}

/// Legacy evaluates view and field rules on the whole entity even when the
/// query projected `fields` (instaql.clj:1956-2007 preload-entity-maps).
#[tokio::test]
async fn view_rules_see_the_whole_entity_under_a_fields_projection() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    set_rules(
        &pool,
        app,
        json!({"todos": {
            "allow": {"view": "data.done == true"},
            "fields": {"title": "data.done == true"}
        }}),
    )
    .await;
    let (t1, t2) = (Uuid::new_v4(), Uuid::new_v4());
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", t1, ids.todos_id, t1],
            ["add-triple", t1, ids.todos_title, "shown"],
            ["add-triple", t1, ids.todos_done, true],
            ["add-triple", t2, ids.todos_id, t2],
            ["add-triple", t2, ids.todos_title, "hidden"],
            ["add-triple", t2, ids.todos_done, false]
        ]),
    )
    .await
    .unwrap();
    let attrs = attrs_of(&pool, app).await;
    let q = json!({"todos": {"$": {"fields": ["title"]}}});
    let ctx = QueryCtx {
        app_id: app,
        attrs: &attrs,
        admin: true,
    };
    let mut conn = pool.acquire().await.unwrap();
    let mut result = query(&mut conn, &ctx, &q).await.unwrap();
    let rules = Rules::load(&mut conn, app).await.unwrap();
    let auth = AuthCtx::default();
    let filter = PermsFilter {
        rules: &rules,
        auth: &auth,
        rule_params: json!({}),
    };
    let forms = instant_core::instaql::parse_query(&q).unwrap();
    filter
        .filter_with_forms(&mut conn, app, &attrs, &mut result, &forms)
        .await
        .unwrap();
    let entities = &result.forms[0].entities;
    assert_eq!(entities.len(), 1);
    assert_eq!(entities[0].eid, t1);
    // the projected result still carries only id + title, and the field rule
    // kept `title` because it saw `done` on the full entity
    let labels: Vec<String> = entities[0]
        .triples
        .iter()
        .map(|t| attrs.get(&t.a).unwrap().label.clone())
        .collect();
    assert!(labels.contains(&"title".to_string()));
    assert!(!labels.contains(&"done".to_string()));
}

/// Link rules: `actions` is bound, `linkedData.ref` resolves, and a link in
/// the same tx that creates the entity runs the link rule with
/// `actions.data == "create"` (permissioned_transaction.clj:528-560).
#[tokio::test]
async fn link_rule_bindings() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    set_rules(
        &pool,
        app,
        json!({"todos": {"allow": {
            "create": "false",
            "link": {"owner": "actions.data == 'create' && actions.linkedData == 'update' && 'boss' in linkedData.ref('name') && linkedData.name == 'boss'"}
        }}}),
    )
    .await;
    let boss = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", boss, ids.owners_id, boss],
            ["add-triple", boss, ids.owners_name, "boss"]
        ]),
    )
    .await
    .unwrap();
    // a todo brought into being by the link step alone: the link rule runs
    // (with actions.data == create), not the `create: false` rule
    let t1 = Uuid::new_v4();
    transact_with_perms(
        &pool,
        app,
        &AuthCtx::default(),
        json!([["add-triple", t1, ids.todos_owner, boss]]),
    )
    .await
    .unwrap();
    // an existing todo linking later has actions.data == update: denied here
    let t2 = Uuid::new_v4();
    transact_json(&pool, app, json!([["add-triple", t2, ids.todos_id, t2]]))
        .await
        .unwrap();
    let err = transact_with_perms(
        &pool,
        app,
        &AuthCtx::default(),
        json!([["add-triple", t2, ids.todos_owner, boss]]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "permission-denied");
    // legacy hint: [etype scope]
    assert_eq!(err.hint.unwrap()["input"], json!(["todos", "object"]));
}

/// Update / delete rules read `data.ref` against the pre-tx graph
/// (legacy runs them as pre-checks, permissioned_transaction.clj:697-715).
#[tokio::test]
async fn update_and_delete_refs_are_pre_tx() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    set_rules(
        &pool,
        app,
        json!({"todos": {"allow": {
            "delete": "'ann' in data.ref('owner.name')",
            "update": "'ann' in data.ref('owner.name')"
        }}}),
    )
    .await;
    let (ann, t1, t2) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", ann, ids.owners_id, ann],
            ["add-triple", ann, ids.owners_name, "ann"],
            ["add-triple", t1, ids.todos_id, t1],
            ["add-triple", t1, ids.todos_owner, ann],
            ["add-triple", t2, ids.todos_id, t2],
            ["add-triple", t2, ids.todos_owner, ann]
        ]),
    )
    .await
    .unwrap();
    // deleting the todo severs the link; the rule still sees ann
    transact_with_perms(
        &pool,
        app,
        &AuthCtx::default(),
        json!([["delete-entity", t1, "todos"]]),
    )
    .await
    .unwrap();
    // unlinking in the same tx as an update: the update rule sees the old link
    transact_with_perms(
        &pool,
        app,
        &AuthCtx::default(),
        json!([
            ["retract-triple", t2, ids.todos_owner, ann],
            ["add-triple", t2, ids.todos_title, "renamed"]
        ]),
    )
    .await
    .unwrap();
    // and now that the link is gone, the update is denied
    let err = transact_with_perms(
        &pool,
        app,
        &AuthCtx::default(),
        json!([["add-triple", t2, ids.todos_title, "again"]]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "permission-denied");
}

/// A delete-entity step for an entity that does not exist still runs its
/// delete pre-check, but with a nil data map and the pre-tx ref graph
/// (permissioned_transaction.clj pre-checks + cel.clj ref-impl). The check
/// must not see links the same tx then writes, or a delete-then-create tx
/// would be denied by `size(data.ref('owner.id')) == 0`.
#[tokio::test]
async fn delete_of_missing_entity_does_not_see_same_tx_links() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    set_rules(
        &pool,
        app,
        json!({"todos": {"allow": {
            "create": "true",
            "update": "false",
            "delete": "size(data.ref('owner.id')) == 0"
        }}}),
    )
    .await;
    let owner = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", owner, ids.owners_id, owner],
            ["add-triple", owner, ids.owners_name, "ann"]
        ]),
    )
    .await
    .unwrap();
    // the entity is absent pre-tx; the link is written after the delete step
    let t = Uuid::new_v4();
    transact_with_perms(
        &pool,
        app,
        &AuthCtx::default(),
        json!([
            ["delete-entity", t, "todos"],
            ["add-triple", t, ids.todos_owner, owner],
            ["add-triple", t, ids.todos_id, t]
        ]),
    )
    .await
    .unwrap();
}

/// `attrs.allow.create` (falling back to `$default`) governs schemaless
/// add-attr steps (permissioned_transaction.clj:519-527); `data` is the attr.
#[tokio::test]
async fn attrs_create_rule_gates_inline_add_attr() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    let new_attr = |label: &str| {
        json!(["add-attr", {"id": Uuid::new_v4(), "forward-identity": [Uuid::new_v4(), "todos", label],
            "value-type": "blob", "cardinality": "one", "unique?": false, "index?": false}])
    };
    // default: allowed
    transact_with_perms(&pool, app, &AuthCtx::default(), json!([new_attr("a")]))
        .await
        .unwrap();
    set_rules(
        &pool,
        app,
        json!({"attrs": {"allow": {"create": "data['forward-identity'][2].startsWith('ok')"}}}),
    )
    .await;
    transact_with_perms(&pool, app, &AuthCtx::default(), json!([new_attr("okay")]))
        .await
        .unwrap();
    let err = transact_with_perms(&pool, app, &AuthCtx::default(), json!([new_attr("nope")]))
        .await
        .unwrap_err();
    assert_eq!(err.error_type, "permission-denied");
    assert_eq!(err.hint.unwrap()["input"], json!(["attrs", "attr"]));
    // $default.allow.create applies too
    set_rules(
        &pool,
        app,
        json!({"$default": {"allow": {"create": "false"}}}),
    )
    .await;
    let err = transact_with_perms(&pool, app, &AuthCtx::default(), json!([new_attr("later")]))
        .await
        .unwrap_err();
    assert_eq!(err.error_type, "permission-denied");
    let _ = ids;
}

/// `auth.ref(...)` in update / delete / link / unlink checks reads the graph
/// as it was before the steps ran (legacy pre-checks bind `auth` on the tx
/// conn before `transact!`, permissioned_transaction.clj:697-715): a tx
/// can't grant itself a role and use it in the same transaction.
#[tokio::test]
async fn auth_refs_are_pre_tx() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    // a custom $users attr the user may write through the default self rule
    let role_attr = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([["add-attr", {"id": role_attr,
            "forward-identity": [Uuid::new_v4(), "$users", "role"],
            "value-type": "blob", "cardinality": "one", "unique?": false, "index?": false}]]),
    )
    .await
    .unwrap();
    let mallory = mk_user(&pool, app, "mallory@example.com").await;
    set_rules(
        &pool,
        app,
        json!({"todos": {"allow": {"update": "'admin' in auth.ref('$user.role')"}}}),
    )
    .await;
    let t = Uuid::new_v4();
    transact_json(&pool, app, json!([["add-triple", t, ids.todos_id, t]]))
        .await
        .unwrap();
    let auth = AuthCtx {
        user_id: Some(mallory),
        user_map: None,
        request: Default::default(),
    };
    // grant-and-use in one tx: the update check sees the pre-tx role (none)
    let err = transact_with_perms(
        &pool,
        app,
        &auth,
        json!([
            ["add-triple", mallory, role_attr, "admin"],
            ["add-triple", t, ids.todos_title, "owned"]
        ]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "permission-denied");
    // the self-write alone is allowed by the $users default rules ...
    transact_with_perms(
        &pool,
        app,
        &auth,
        json!([["add-triple", mallory, role_attr, "admin"]]),
    )
    .await
    .unwrap();
    // ... and a later tx sees the role
    transact_with_perms(
        &pool,
        app,
        &auth,
        json!([["add-triple", t, ids.todos_title, "owned"]]),
    )
    .await
    .unwrap();
}

/// A link whose value is a lookup ref (`[attr, value]`) runs the link rule
/// on the resolved entity like legacy's `resolve-lookups-tx-steps`
/// (permissioned_transaction.clj:173-190) — it can't skip the rule.
#[tokio::test]
async fn link_rules_run_for_lookup_values() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    set_rules(
        &pool,
        app,
        json!({"todos": {"allow": {"link": {"owner": "linkedData.name == 'boss'"}}}}),
    )
    .await;
    let (boss, peon, t1, t2) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", boss, ids.owners_id, boss],
            ["add-triple", boss, ids.owners_name, "boss"],
            ["add-triple", peon, ids.owners_id, peon],
            ["add-triple", peon, ids.owners_name, "peon"],
            ["add-triple", t1, ids.todos_id, t1],
            ["add-triple", t2, ids.todos_id, t2]
        ]),
    )
    .await
    .unwrap();
    transact_with_perms(
        &pool,
        app,
        &AuthCtx::default(),
        json!([["add-triple", t1, ids.todos_owner, [ids.owners_name, "boss"]]]),
    )
    .await
    .unwrap();
    let err = transact_with_perms(
        &pool,
        app,
        &AuthCtx::default(),
        json!([["add-triple", t2, ids.todos_owner, [ids.owners_name, "peon"]]]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "permission-denied");
}

/// Issue #45: permissions drop whole join rows (instaql.clj
/// permissioned-node). A where row through a hidden entity is dropped, a
/// form left without a viewable top-level row loses every entity, and a
/// forward link triple to a hidden entity stays (its viewability is the
/// parent's) while the hidden entity does not.
#[tokio::test]
async fn permissions_drop_whole_join_rows() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    set_rules(
        &pool,
        app,
        json!({"owners": {"allow": {"view": "data.name != 'hidden'"}}}),
    )
    .await;
    let (alice, hidden) = (Uuid::new_v4(), Uuid::new_v4());
    let (t1, t2) = (Uuid::new_v4(), Uuid::new_v4());
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", alice, ids.owners_id, alice],
            ["add-triple", alice, ids.owners_name, "alice"],
            ["add-triple", hidden, ids.owners_id, hidden],
            ["add-triple", hidden, ids.owners_name, "hidden"],
            ["add-triple", t1, ids.todos_id, t1],
            ["add-triple", t1, ids.todos_owner, alice],
            ["add-triple", t2, ids.todos_id, t2],
            ["add-triple", t2, ids.todos_owner, hidden]
        ]),
    )
    .await
    .unwrap();
    let auth = AuthCtx::default();

    let res = run_filtered(
        &pool,
        app,
        &auth,
        json!({"todos": {"$": {"where": {"owner.name": "alice"}}}}),
    )
    .await;
    assert_eq!(res.forms[0].entities.len(), 1);
    assert!(res.forms[0]
        .where_rows
        .iter()
        .flatten()
        .any(|t| t.e == alice && t.a == ids.owners_name));

    // the only row runs through the hidden owner: no entity survives
    let res = run_filtered(
        &pool,
        app,
        &auth,
        json!({"todos": {"$": {"where": {"owner.name": "hidden"}}}}),
    )
    .await;
    assert!(res.forms[0].entities.is_empty());
    assert!(res.forms[0].where_rows.is_empty());

    let res = run_filtered(&pool, app, &auth, json!({"todos": {"owner": {}}})).await;
    let t2_node = res.forms[0].entities.iter().find(|n| n.eid == t2).unwrap();
    assert_eq!(t2_node.children[0].link_triples.len(), 1);
    assert!(t2_node.children[0].entities.is_empty());
    let t1_node = res.forms[0].entities.iter().find(|n| n.eid == t1).unwrap();
    assert_eq!(t1_node.children[0].entities.len(), 1);
}

/// A paginated form's join rows carry the entity's order triple (datalog.clj
/// add-page-info), so a field rule on the order attr that fails drops the
/// row; when no row is left the page is empty, with null cursors and the
/// unfiltered has-next-page (instaql.clj permissioned-node).
#[tokio::test]
async fn field_rule_on_the_order_attr_drops_the_page_row() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    let rank = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([["add-attr", {"id": rank, "forward-identity": [Uuid::new_v4(), "todos", "rank"],
          "value-type": "blob", "cardinality": "one", "unique?": false, "index?": true,
          "checked-data-type": "number"}]]),
    )
    .await
    .unwrap();
    set_rules(
        &pool,
        app,
        json!({"todos": {"fields": {"rank": "data.title == 'mine'"}}}),
    )
    .await;
    let (a, b, c, d) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", d, ids.todos_id, d],
            ["add-triple", d, ids.todos_title, "theirs"],
            ["add-triple", d, rank, 0],
            ["add-triple", a, ids.todos_id, a],
            ["add-triple", a, ids.todos_title, "theirs"],
            ["add-triple", a, rank, 1],
            ["add-triple", b, ids.todos_id, b],
            ["add-triple", b, ids.todos_title, "theirs"],
            ["add-triple", b, rank, 2],
            ["add-triple", c, ids.todos_id, c],
            ["add-triple", c, ids.todos_title, "mine"],
            ["add-triple", c, rank, 3]
        ]),
    )
    .await
    .unwrap();
    let auth = AuthCtx::default();

    // the first page holds only entities whose rank is hidden
    for q in [
        json!({"todos": {"$": {"order": {"rank": "asc"}, "limit": 2}}}),
        json!({"todos": {"$": {"where": {"title": "theirs"}, "order": {"rank": "asc"}, "limit": 2}}}),
    ] {
        let res = run_filtered(&pool, app, &auth, q.clone()).await;
        let form = &res.forms[0];
        assert!(form.entities.is_empty(), "{q}");
        assert!(form.where_rows.is_empty(), "{q}");
        let pi = form.page_info.as_ref().unwrap();
        assert!(pi.start_cursor.is_none() && pi.end_cursor.is_none(), "{q}");
        assert!(pi.has_next_page, "{q}");
    }

    // a page with one viewable order triple keeps every entity
    let res = run_filtered(
        &pool,
        app,
        &auth,
        json!({"todos": {"$": {"order": {"rank": "desc"}, "limit": 2}}}),
    )
    .await;
    let eids: Vec<Uuid> = res.forms[0].entities.iter().map(|n| n.eid).collect();
    assert_eq!(eids.len(), 2);
    assert!(eids.contains(&c) && eids.contains(&b));
}

/// Legacy coerce-value-uuids runs over every step before the pre-checks
/// (permissioned_transaction.clj:684-687): a link to a non-uuid is a
/// validation error even in a tx whose attr steps would be denied.
#[tokio::test]
async fn bad_link_value_fails_before_attr_scope_checks() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    let t = Uuid::new_v4();
    for steps in [
        json!([["update-attr", {"id": Uuid::new_v4(), "index?": true}],
               ["add-triple", t, ids.todos_owner, "not-a-uuid"]]),
        json!([
            ["add-triple", t, ids.todos_owner, "not-a-uuid"],
            ["delete-attr", Uuid::new_v4()]
        ]),
    ] {
        let err = transact_with_perms(&pool, app, &AuthCtx::default(), steps)
            .await
            .unwrap_err();
        assert_eq!(err.error_type, "validation-failed");
    }
    // the attr step alone is still denied
    let err = transact_with_perms(
        &pool,
        app,
        &AuthCtx::default(),
        json!([["delete-attr", Uuid::new_v4()]]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "permission-denied");
}

/// A paginated top-level `or` keeps one where row per entity, the first
/// matching branch's (datalog.clj or-gather-cte + add-page-info's
/// DISTINCT ON); an unpaginated one keeps every branch's rows.
#[tokio::test]
async fn paginated_or_keeps_the_first_matching_branch() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    let (both, second) = (Uuid::new_v4(), Uuid::new_v4());
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", both, ids.todos_id, both],
            ["add-triple", both, ids.todos_title, "x"],
            ["add-triple", both, ids.todos_done, true],
            ["add-triple", second, ids.todos_id, second],
            ["add-triple", second, ids.todos_title, "y"],
            ["add-triple", second, ids.todos_done, true]
        ]),
    )
    .await
    .unwrap();
    let auth = AuthCtx::default();
    let where_ = json!({"or": [{"title": "x"}, {"done": true}]});
    let rows_of = |res: &instant_core::instaql::QueryResult, e: Uuid| -> Vec<Uuid> {
        res.forms[0]
            .where_rows
            .iter()
            .flatten()
            .filter(|t| t.e == e)
            .map(|t| t.a)
            .collect()
    };

    let res = run_filtered(
        &pool,
        app,
        &auth,
        json!({"todos": {"$": {"where": where_.clone(), "limit": 5}}}),
    )
    .await;
    assert_eq!(rows_of(&res, both), vec![ids.todos_title]);
    assert_eq!(rows_of(&res, second), vec![ids.todos_done]);

    let res = run_filtered(
        &pool,
        app,
        &auth,
        json!({"todos": {"$": {"where": where_}}}),
    )
    .await;
    let mut both_rows = rows_of(&res, both);
    both_rows.sort();
    let mut want = vec![ids.todos_title, ids.todos_done];
    want.sort();
    assert_eq!(both_rows, want);
}

/// An add step on an entity with no pre-tx data runs the create rule even
/// when it writes no id triple, e.g. a link step alone
/// (permissioned_transaction.clj post-create-checks `create?`).
#[tokio::test]
async fn a_link_alone_creates_its_entity() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    set_rules(&pool, app, json!({"todos": {"allow": {"create": "false"}}})).await;
    let (o, existing, missing) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", o, ids.owners_id, o],
            ["add-triple", existing, ids.todos_id, existing]
        ]),
    )
    .await
    .unwrap();
    let auth = AuthCtx::default();
    let err = transact_with_perms(
        &pool,
        app,
        &auth,
        json!([["add-triple", missing, ids.todos_owner, o]]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "permission-denied");
    let err = transact_with_perms(
        &pool,
        app,
        &auth,
        json!([["add-triple", missing, ids.todos_title, "t"]]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "permission-denied");
    transact_with_perms(
        &pool,
        app,
        &auth,
        json!([["add-triple", existing, ids.todos_owner, o]]),
    )
    .await
    .unwrap();
}

/// The link / unlink fallback's `view` check on a target that exists before
/// the tx is a pre-check (permissioned_transaction.clj pre-checks): it
/// fails the tx with permission-denied even when a step would error.
#[tokio::test]
async fn unlink_fallback_views_the_target_before_the_steps_run() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    set_rules(&pool, app, json!({"owners": {"allow": {"view": "false"}}})).await;
    let (o, t) = (Uuid::new_v4(), Uuid::new_v4());
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", o, ids.owners_id, o],
            ["add-triple", t, ids.todos_id, t],
            ["add-triple", t, ids.todos_owner, o]
        ]),
    )
    .await
    .unwrap();
    // `mode: create` on an existing entity fails while the steps run
    let err = transact_with_perms(
        &pool,
        app,
        &AuthCtx::default(),
        json!([
            ["retract-triple", t, ids.todos_owner, o],
            ["add-triple", t, ids.todos_id, t, {"mode": "create"}]
        ]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "permission-denied");
}

/// Create checks bind legacy's locally approximated post-tx entity
/// (update-entities-map applies the add / merge / retract steps and ignores
/// delete-entity), so an entity the tx writes and then deletes still shows
/// the written fields to its create rule.
#[tokio::test]
async fn create_check_sees_the_written_fields_of_a_deleted_entity() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    set_rules(
        &pool,
        app,
        json!({"todos": {"allow": {"create": "newData.title == 'ok'"}}}),
    )
    .await;
    let auth = AuthCtx::default();
    let t = Uuid::new_v4();
    transact_with_perms(
        &pool,
        app,
        &auth,
        json!([
            ["add-triple", t, ids.todos_id, t],
            ["add-triple", t, ids.todos_title, "ok"],
            ["delete-entity", t, "todos"]
        ]),
    )
    .await
    .unwrap();
    let t2 = Uuid::new_v4();
    let err = transact_with_perms(
        &pool,
        app,
        &auth,
        json!([
            ["add-triple", t2, ids.todos_id, t2],
            ["add-triple", t2, ids.todos_title, "no"],
            ["delete-entity", t2, "todos"]
        ]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "permission-denied");
}

/// Runs a query through the permission filter and returns its invalidation
/// topics the way the server derives them (service::run_query_full): the
/// query's own topics plus the lookups its permission checks ran.
async fn filtered_topics(
    pool: &PgPool,
    app: Uuid,
    auth: &AuthCtx,
    q: Value,
) -> (
    instant_core::instaql::QueryResult,
    instant_core::topics::QueryTopics,
) {
    let attrs = attrs_of(pool, app).await;
    let ctx = QueryCtx {
        app_id: app,
        attrs: &attrs,
        admin: true,
    };
    let mut conn = pool.acquire().await.unwrap();
    let mut result = query(&mut conn, &ctx, &q).await.unwrap();
    let forms = instant_core::instaql::parse_query(&q).unwrap();
    let mut topics = instant_core::topics::query_topics(&attrs, &forms, &result);
    let rules = Rules::load(&mut conn, app).await.unwrap();
    let filter = PermsFilter {
        rules: &rules,
        auth,
        rule_params: json!({}),
    };
    let mut lookups = instant_core::topics::LookupTopics::default();
    filter
        .filter_with_forms_tracked(
            &mut conn,
            app,
            &attrs,
            &mut result,
            &forms,
            Some(&mut lookups),
        )
        .await
        .unwrap();
    topics.merge_lookups(lookups);
    (result, topics)
}

/// The topics a tx would log for these changed triples (rust_tx_changes).
fn tx_of(changes: &[(Uuid, Uuid, Value)]) -> instant_core::topics::TxTopics {
    instant_core::topics::TxTopics::from_changes(changes.iter().map(|(e, a, v)| {
        instant_core::topics::TxChange {
            e: *e,
            a: *a,
            v: v.clone(),
        }
    }))
}

/// Issue #59: a query whose view rule reads `data.ref` / `auth.ref` is
/// invalidated by writes to what its hops read, and not by writes anywhere
/// else (it used to be catch-all for the whole app).
#[tokio::test]
async fn ref_rule_topics_narrow_to_ref_targets() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    let (teams_id, teams_name, owner_team) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let (notes_id, notes_body) = (Uuid::new_v4(), Uuid::new_v4());
    let (members_id, member_user) = (Uuid::new_v4(), Uuid::new_v4());
    transact_json(
        &pool,
        app,
        json!([
            ["add-attr", {"id": teams_id, "forward-identity": [Uuid::new_v4(), "teams", "id"],
              "value-type": "blob", "cardinality": "one", "unique?": true, "index?": false}],
            ["add-attr", {"id": teams_name, "forward-identity": [Uuid::new_v4(), "teams", "name"],
              "value-type": "blob", "cardinality": "one", "unique?": false, "index?": false}],
            ["add-attr", {"id": owner_team, "forward-identity": [Uuid::new_v4(), "owners", "team"],
              "reverse-identity": [Uuid::new_v4(), "teams", "owners"],
              "value-type": "ref", "cardinality": "one", "unique?": false, "index?": false}],
            ["add-attr", {"id": notes_id, "forward-identity": [Uuid::new_v4(), "notes", "id"],
              "value-type": "blob", "cardinality": "one", "unique?": true, "index?": false}],
            ["add-attr", {"id": notes_body, "forward-identity": [Uuid::new_v4(), "notes", "body"],
              "value-type": "blob", "cardinality": "one", "unique?": false, "index?": false}],
            ["add-attr", {"id": members_id, "forward-identity": [Uuid::new_v4(), "members", "id"],
              "value-type": "blob", "cardinality": "one", "unique?": true, "index?": false}],
            ["add-attr", {"id": member_user, "forward-identity": [Uuid::new_v4(), "members", "user"],
              "reverse-identity": [Uuid::new_v4(), "$users", "members"],
              "value-type": "ref", "cardinality": "one", "unique?": false, "index?": false}],
            ["add-attr", {"id": Uuid::new_v4(), "forward-identity": [Uuid::new_v4(), "members", "team"],
              "reverse-identity": [Uuid::new_v4(), "teams", "members"],
              "value-type": "ref", "cardinality": "one", "unique?": false, "index?": false}]
        ]),
    )
    .await
    .unwrap();
    set_rules(
        &pool,
        app,
        json!({
            // one hop, two hops
            "todos": {"allow": {"view": "'alice' in data.ref('owner.name') || 'red' in data.ref('owner.team.name')"}},
            // a reverse hop to a terminal id
            "owners": {"allow": {"view": "size(data.ref('todos.id')) > 0"}},
            // auth.ref through a reverse link from $users
            "notes": {"allow": {"view": "'red' in auth.ref('$user.members.team.name')"}}
        }),
    )
    .await;
    let (owner_a, owner_b, owner_c) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let (t1, t2, red, blue) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    let note = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", owner_a, ids.owners_id, owner_a],
            ["add-triple", owner_a, ids.owners_name, "alice"],
            ["add-triple", owner_b, ids.owners_id, owner_b],
            ["add-triple", owner_b, ids.owners_name, "bob"],
            ["add-triple", owner_b, owner_team, blue],
            ["add-triple", owner_c, ids.owners_id, owner_c],
            ["add-triple", owner_c, ids.owners_name, "carl"],
            ["add-triple", red, teams_id, red],
            ["add-triple", red, teams_name, "red"],
            ["add-triple", blue, teams_id, blue],
            ["add-triple", blue, teams_name, "blue"],
            ["add-triple", t1, ids.todos_id, t1],
            ["add-triple", t1, ids.todos_owner, owner_a],
            ["add-triple", t2, ids.todos_id, t2],
            ["add-triple", t2, ids.todos_owner, owner_b],
            ["add-triple", note, notes_id, note],
            ["add-triple", note, notes_body, "hi"]
        ]),
    )
    .await
    .unwrap();
    let anon = AuthCtx::default();

    // todos: t1 visible through owner.name, t2 hidden (bob, team blue)
    let (res, qt) = filtered_topics(&pool, app, &anon, json!({"todos": {}})).await;
    assert_eq!(res.forms[0].entities.len(), 1);
    assert!(
        !qt.catch_all,
        "ref rules no longer make the query catch-all"
    );
    // unrelated namespace: no refresh
    assert!(!tx_of(&[(Uuid::new_v4(), notes_body, json!("x"))]).matches(&qt));
    assert!(!tx_of(&[(note, notes_body, json!("x"))]).matches(&qt));
    // an owner no result todo links to: no refresh
    assert!(!tx_of(&[(owner_c, ids.owners_name, json!("alice"))]).matches(&qt));
    // a team no owner of a result todo is on: no refresh
    assert!(!tx_of(&[(red, teams_name, json!("blue"))]).matches(&qt));
    // hop 1 target renamed (t2's owner becomes alice): refresh
    assert!(tx_of(&[(owner_b, ids.owners_name, json!("alice"))]).matches(&qt));
    // hop 2 link moved (bob joins red) and hop 2 target renamed: refresh
    assert!(tx_of(&[(owner_b, owner_team, json!(red.to_string()))]).matches(&qt));
    assert!(tx_of(&[(blue, teams_name, json!("red"))]).matches(&qt));
    // the queried entity relinked: refresh
    assert!(tx_of(&[(t2, ids.todos_owner, json!(owner_c.to_string()))]).matches(&qt));

    // owners: reverse hop todos.owner -> owner; owner_c has no todo
    let (res, qt) = filtered_topics(&pool, app, &anon, json!({"owners": {}})).await;
    assert_eq!(res.forms[0].entities.len(), 2);
    assert!(!qt.catch_all);
    let t3 = Uuid::new_v4();
    // a todo linked to owner_c: owner_c becomes visible
    assert!(tx_of(&[(t3, ids.todos_owner, json!(owner_c.to_string()))]).matches(&qt));
    // a todo linked to an owner outside the result: no refresh
    assert!(!tx_of(&[(t3, ids.todos_owner, json!(Uuid::new_v4().to_string()))]).matches(&qt));
    // t1's id triple (the terminal `id` read): refresh
    assert!(tx_of(&[(t1, ids.todos_id, json!(t1.to_string()))]).matches(&qt));
    assert!(!tx_of(&[(note, notes_body, json!("x"))]).matches(&qt));

    // notes: auth.ref('$user.members.team.name') for a signed-in user
    let uid = mk_user(&pool, app, "u@example.com").await;
    let auth = AuthCtx {
        user_id: Some(uid),
        ..AuthCtx::default()
    };
    let (res, qt) = filtered_topics(&pool, app, &auth, json!({"notes": {}})).await;
    assert_eq!(res.forms[0].entities.len(), 0);
    assert!(!qt.catch_all);
    let attrs = attrs_of(&pool, app).await;
    let member_team = attrs.by_fwd_name("members", "team").unwrap().id;
    let m1 = Uuid::new_v4();
    // a membership for this user: refresh (reverse hop $users <- members.user)
    assert!(tx_of(&[(m1, member_user, json!(uid.to_string()))]).matches(&qt));
    // a membership for someone else: no refresh
    assert!(!tx_of(&[(m1, member_user, json!(Uuid::new_v4().to_string()))]).matches(&qt));
    // an unrelated membership's team: no refresh (the user has no members yet)
    assert!(!tx_of(&[(m1, member_team, json!(red.to_string()))]).matches(&qt));
    assert!(!tx_of(&[(t1, ids.todos_owner, json!(owner_b.to_string()))]).matches(&qt));
}

/// Field rules prefetch `data.ref` like view rules: legacy's
/// eval-programs! resolves the refs of every program it runs
/// (cel.clj:679-727), field programs included.
#[tokio::test]
async fn data_ref_in_field_rule() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    set_rules(
        &pool,
        app,
        json!({"todos": {"fields": {"title": "'alice' in data.ref('owner.name')"}}}),
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
            ["add-triple", t1, ids.todos_title, "one"],
            ["add-triple", t1, ids.todos_owner, owner_a],
            ["add-triple", t2, ids.todos_id, t2],
            ["add-triple", t2, ids.todos_title, "two"],
            ["add-triple", t2, ids.todos_owner, owner_b]
        ]),
    )
    .await
    .unwrap();
    let (res, qt) = filtered_topics(&pool, app, &AuthCtx::default(), json!({"todos": {}})).await;
    let titled: Vec<Uuid> = res.forms[0]
        .entities
        .iter()
        .filter(|n| n.triples.iter().any(|t| t.a == ids.todos_title))
        .map(|n| n.eid)
        .collect();
    assert_eq!(titled, vec![t1]);
    // the field rule's hop is tracked too
    assert!(tx_of(&[(owner_b, ids.owners_name, json!("alice"))]).matches(&qt));
}
