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
    set_rules(&pool, app, json!({"$default": {"allow": {"create": "false"}}})).await;
    let err = transact_with_perms(&pool, app, &AuthCtx::default(), json!([new_attr("later")]))
        .await
        .unwrap_err();
    assert_eq!(err.error_type, "permission-denied");
    let _ = ids;
}
