// Coverage for the 2026-09-04 audit follow-ups (docs/PARITY.md "Open items"
// section lists what is still deferred): lookup / attr / query validation,
// system-entity guards, and rule-evaluation semantics. Each test names the
// legacy source it mirrors.
mod common;

use common::*;
use instant_core::instaql::{query, QueryCtx};
use instant_core::perms::{self, AuthCtx, PermsFilter, Rules};
use instant_core::system_catalog as sc;
use instant_core::tx::{self, TxOptions, TxReport};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

async fn transact_opts(
    pool: &PgPool,
    app: Uuid,
    steps: Value,
    opts: TxOptions,
) -> instant_core::error::Result<TxReport> {
    let parsed = tx::parse_tx_steps(&steps)?;
    let mut attrs = attrs_of(pool, app).await;
    let mut dbtx = pool.begin().await.unwrap();
    let report = tx::transact(&mut dbtx, app, &mut attrs, parsed, &opts).await?;
    dbtx.commit().await.unwrap();
    Ok(report)
}

fn blob(id: Uuid, etype: &str, label: &str, unique: bool, index: bool, extra: Value) -> Value {
    let mut attr = json!({
        "id": id,
        "forward-identity": [Uuid::new_v4(), etype, label],
        "value-type": "blob", "cardinality": "one",
        "unique?": unique, "index?": index
    });
    if let Value::Object(m) = extra {
        for (k, v) in m {
            attr[k] = v;
        }
    }
    json!(["add-attr", attr])
}

fn link(
    id: Uuid,
    etype: &str,
    label: &str,
    rev_etype: &str,
    rev_label: &str,
    extra: Value,
) -> Value {
    let mut attr = json!({
        "id": id,
        "forward-identity": [Uuid::new_v4(), etype, label],
        "reverse-identity": [Uuid::new_v4(), rev_etype, rev_label],
        "value-type": "ref", "cardinality": "one",
        "unique?": false, "index?": false
    });
    if let Value::Object(m) = extra {
        for (k, v) in m {
            attr[k] = v;
        }
    }
    json!(["add-attr", attr])
}

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

async fn filtered(
    pool: &PgPool,
    app: Uuid,
    auth: &AuthCtx,
    q: Value,
) -> instant_core::error::Result<instant_core::instaql::QueryResult> {
    let attrs = attrs_of(pool, app).await;
    let ctx = QueryCtx {
        app_id: app,
        attrs: &attrs,
        admin: true,
    };
    let mut conn = pool.acquire().await.unwrap();
    let mut result = query(&mut conn, &ctx, &q).await?;
    let rules = Rules::load(&mut conn, app).await?;
    let filter = PermsFilter {
        rules: &rules,
        auth,
        rule_params: json!({}),
    };
    filter.filter(&mut conn, app, &attrs, &mut result).await?;
    Ok(result)
}

async fn transact_with_perms(
    pool: &PgPool,
    app: Uuid,
    auth: &AuthCtx,
    steps: Value,
) -> instant_core::error::Result<TxReport> {
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

async fn mk_user(pool: &PgPool, app: Uuid, email: &str) -> Uuid {
    let uid = Uuid::new_v4();
    transact_opts(
        pool,
        app,
        json!([
            ["add-triple", uid, sc::attr_id("$users", "id"), uid],
            ["add-triple", uid, sc::attr_id("$users", "email"), email]
        ]),
        TxOptions {
            allow_system_catalog_writes: true,
            ..TxOptions::default()
        },
    )
    .await
    .unwrap();
    uid
}

fn user_auth(uid: Uuid) -> AuthCtx {
    AuthCtx {
        user_id: Some(uid),
        user_map: None,
        request: Default::default(),
    }
}

async fn count_triples(pool: &PgPool, app: Uuid, attr: Uuid, value: Value) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM triples WHERE app_id = $1 AND attr_id = $2 AND value = $3",
    )
    .bind(app)
    .bind(attr)
    .bind(value)
    .fetch_one(pool)
    .await
    .unwrap()
}

// ---------------------------------------------------------------------------
// transactions

/// triple.clj:885-899: a value-position lookup raises `missing-lookup-value`
/// instead of creating the target.
#[tokio::test]
async fn value_lookup_never_creates_a_phantom_entity() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    let todo = Uuid::new_v4();
    let err = transact_json(
        &pool,
        app,
        json!([
            ["add-triple", todo, ids.todos_id, todo],
            [
                "add-triple",
                todo,
                ids.todos_owner,
                [ids.owners_name, "ghost"]
            ]
        ]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "validation-failed");
    assert_eq!(
        err.message,
        "Validation failed for lookup: The entity for the lookup does not exist."
    );
    let hint = err.hint.unwrap();
    assert_eq!(hint["data-type"], json!("lookup"));
    assert_eq!(hint["errors"][0]["namespace"], json!("owners"));
    assert_eq!(hint["errors"][0]["label"], json!("name"));
    assert_eq!(
        count_triples(&pool, app, ids.owners_name, json!("ghost")).await,
        0,
        "no owners row was minted"
    );
    // an eid-position lookup still upserts (legacy lookup semantics)
    transact_json(
        &pool,
        app,
        json!([[
            "add-triple",
            [ids.owners_name, "real"],
            ids.owners_id,
            [ids.owners_name, "real"]
        ]]),
    )
    .await
    .unwrap();
    assert_eq!(
        count_triples(&pool, app, ids.owners_name, json!("real")).await,
        1
    );
}

/// transaction.clj:532-556 + permissioned_transaction.clj:124-140 run in the
/// non-admin pre-processing branch only (:683-687).
#[tokio::test]
async fn lookup_namespaces_validated_for_non_admins_only() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();

    // eid lookup on owners.name writing a todos attr
    let err = transact_json(
        &pool,
        app,
        json!([["add-triple", [ids.owners_name, "bob"], ids.todos_title, "x"]]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "validation-failed");
    assert!(
        err.message
            .contains("The namespace in the lookup attribute is different"),
        "{}",
        err.message
    );
    // value lookup through an attr outside the link's reverse namespace
    let todo = Uuid::new_v4();
    let err = transact_json(
        &pool,
        app,
        json!([
            ["add-triple", todo, ids.todos_id, todo],
            [
                "add-triple",
                todo,
                ids.todos_owner,
                [ids.todos_id, Uuid::new_v4()]
            ]
        ]),
    )
    .await
    .unwrap_err();
    assert!(
        err.message
            .contains("The namespace in the lookup attribute is different"),
        "{}",
        err.message
    );
    // admins skip the namespace checks and fall through to the lookup miss
    let err = transact_opts(
        &pool,
        app,
        json!([
            ["add-triple", todo, ids.todos_id, todo],
            [
                "add-triple",
                todo,
                ids.todos_owner,
                [ids.todos_id, Uuid::new_v4()]
            ]
        ]),
        TxOptions {
            admin: true,
            ..TxOptions::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.message,
        "Validation failed for lookup: The entity for the lookup does not exist."
    );
}

/// exception.clj:227-240: `app_ident_uq` on idents.
#[tokio::test]
async fn duplicate_attr_name_is_record_not_unique() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, _ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    let err = transact_json(
        &pool,
        app,
        json!([blob(
            Uuid::new_v4(),
            "todos",
            "title",
            false,
            false,
            json!({})
        )]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "record-not-unique");
    assert_eq!(err.message, "`title` already exists on `todos`");
    assert_eq!(err.hint.unwrap()["record-type"], json!("ident"));
}

/// attr.clj:334-350 (`validate-add-required!`) and :533-580
/// (`validate-update-required!`).
#[tokio::test]
async fn required_attr_guards() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    let todo = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([["add-triple", todo, ids.todos_id, todo]]),
    )
    .await
    .unwrap();

    let priority = Uuid::new_v4();
    let err = transact_json(
        &pool,
        app,
        json!([blob(
            priority,
            "todos",
            "priority",
            false,
            false,
            json!({"required?": true})
        )]),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.message,
        "Validation failed for attributes: Can't create attribute `priority` as required because `todos` already have entities"
    );
    transact_json(
        &pool,
        app,
        json!([blob(priority, "todos", "priority", false, false, json!({}))]),
    )
    .await
    .unwrap();
    let err = transact_json(
        &pool,
        app,
        json!([["update-attr", {"id": priority, "required?": true}]]),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.message,
        "Validation failed for attributes: Can't update attribute `priority` to required because `todos` already have entities without it"
    );
    assert!(
        !attrs_of(&pool, app)
            .await
            .get(&priority)
            .unwrap()
            .is_required
    );
    // once every entity carries a value the flip goes through
    transact_json(&pool, app, json!([["add-triple", todo, priority, "high"]]))
        .await
        .unwrap();
    transact_json(
        &pool,
        app,
        json!([["update-attr", {"id": priority, "required?": true}]]),
    )
    .await
    .unwrap();
    assert!(
        attrs_of(&pool, app)
            .await
            .get(&priority)
            .unwrap()
            .is_required
    );
}

/// transaction.clj:617-623 feeds the deleted reverse rows to
/// `validate-required!`: deleting a link target leaves the referrer invalid.
#[tokio::test]
async fn deleting_a_link_target_revalidates_required_referrers() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (articles_id, remarks_id, remarks_article) =
        (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    transact_json(
        &pool,
        app,
        json!([
            blob(articles_id, "articles", "id", true, false, json!({})),
            blob(remarks_id, "remarks", "id", true, false, json!({})),
            link(
                remarks_article,
                "remarks",
                "article",
                "articles",
                "remarks",
                json!({"required?": true})
            ),
        ]),
    )
    .await
    .unwrap();
    let (a, r) = (Uuid::new_v4(), Uuid::new_v4());
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", a, articles_id, a],
            ["add-triple", r, remarks_id, r],
            ["add-triple", r, remarks_article, a]
        ]),
    )
    .await
    .unwrap();
    let err = transact_json(&pool, app, json!([["delete-entity", a, "articles"]]))
        .await
        .unwrap_err();
    assert_eq!(err.error_type, "validation-failed");
    assert_eq!(
        err.message,
        format!("Missing required attribute `remarks/article`: {r}")
    );
    // the failed tx rolled back: the article is still there
    assert_eq!(count_triples(&pool, app, articles_id, json!(a)).await, 1);
    // deleting the referrer first makes the target deletable
    transact_json(&pool, app, json!([["delete-entity", r, "remarks"]]))
        .await
        .unwrap();
    transact_json(&pool, app, json!([["delete-entity", a, "articles"]]))
        .await
        .unwrap();
}

/// permissioned_transaction.clj:72-79: stream-backed file paths are locked
/// for everyone, admins included.
#[tokio::test]
async fn stream_file_paths_are_locked() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let f = Uuid::new_v4();
    for admin in [false, true] {
        let err = transact_opts(
            &pool,
            app,
            json!([
                ["add-triple", f, sc::attr_id("$files", "id"), f],
                [
                    "add-triple",
                    f,
                    sc::attr_id("$files", "path"),
                    "$stream/abc"
                ]
            ]),
            TxOptions {
                admin,
                ..TxOptions::default()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(
            err.message,
            "Validation failed for tx-step: The path for stream files can't be edited."
        );
    }
    // an ordinary path is fine ($files.size is a required catalog attr, so
    // the row is written the way the upload route writes it)
    transact_opts(
        &pool,
        app,
        json!([
            ["add-triple", f, sc::attr_id("$files", "id"), f],
            ["add-triple", f, sc::attr_id("$files", "path"), "docs/a.txt"],
            ["add-triple", f, sc::attr_id("$files", "size"), 3],
            ["add-triple", f, sc::attr_id("$files", "location-id"), f]
        ]),
        TxOptions {
            admin: true,
            allow_system_catalog_writes: true,
        },
    )
    .await
    .unwrap();
}

/// permissioned_transaction.clj:589-596.
#[tokio::test]
async fn non_admins_cannot_mint_users() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let u = Uuid::new_v4();
    let err = transact_json(
        &pool,
        app,
        json!([["add-triple", u, sc::attr_id("$users", "id"), u]]),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.message,
        "Validation failed for tx-step: $users is a system entity. You aren't allowed to create this directly."
    );
    transact_opts(
        &pool,
        app,
        json!([["add-triple", u, sc::attr_id("$users", "id"), u]]),
        TxOptions {
            admin: true,
            ..TxOptions::default()
        },
    )
    .await
    .unwrap();
}

/// attr.clj:313-332 (`validate-system-ident-names!`) on both identities, on
/// add and on rename (:585).
#[tokio::test]
async fn reverse_identities_are_validated() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let err = transact_json(
        &pool,
        app,
        json!([link(
            Uuid::new_v4(),
            "things",
            "code",
            "$magicCodes",
            "things",
            json!({})
        )]),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.message,
        "Validation failed for attributes: $ is reserved for system tables. You can't create $magicCodes"
    );
    let err = transact_json(
        &pool,
        app,
        json!([link(
            Uuid::new_v4(),
            "things",
            "owner",
            "$users",
            "email",
            json!({})
        )]),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.message,
        "Validation failed for attributes: $users.email is a system column and it already exists."
    );
    // a reverse identity into an editable namespace is fine
    let owner = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([link(
            owner,
            "things",
            "owner",
            "$users",
            "things",
            json!({})
        )]),
    )
    .await
    .unwrap();
    // renaming onto a catalog ident is refused too
    let name = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([blob(name, "things", "name", false, false, json!({}))]),
    )
    .await
    .unwrap();
    let err = transact_json(
        &pool,
        app,
        json!([[
            "update-attr",
            {"id": name, "forward-identity": [Uuid::new_v4(), "$users", "email"]}
        ]]),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.message,
        "Validation failed for attributes: $users.email is a system column and it already exists."
    );
    let err = transact_json(
        &pool,
        app,
        json!([[
            "update-attr",
            {"id": owner, "reverse-identity": [Uuid::new_v4(), "$oauthCodes", "things"]}
        ]]),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.message,
        "Validation failed for attributes: $ is reserved for system tables. You can't create $oauthCodes"
    );
}

// ---------------------------------------------------------------------------
// queries

struct Things {
    app: Uuid,
    score: Uuid,
    rows: [Uuid; 3],
}

/// things: score (idx number), name (idx string), nick (unindexed string),
/// when (idx date), owner -> people
async fn things(pool: &PgPool) -> Things {
    let app = mk_app(pool).await;
    let (id, score, name, nick, when, owner, people_id) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    transact_json(
        pool,
        app,
        json!([
            blob(id, "things", "id", true, false, json!({})),
            blob(
                score,
                "things",
                "score",
                false,
                true,
                json!({"checked-data-type": "number"})
            ),
            blob(
                name,
                "things",
                "name",
                false,
                true,
                json!({"checked-data-type": "string"})
            ),
            blob(
                nick,
                "things",
                "nick",
                false,
                false,
                json!({"checked-data-type": "string"})
            ),
            blob(
                when,
                "things",
                "when",
                false,
                true,
                json!({"checked-data-type": "date"})
            ),
            blob(people_id, "people", "id", true, false, json!({})),
            link(owner, "things", "owner", "people", "things", json!({})),
        ]),
    )
    .await
    .unwrap();
    let rows = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    transact_json(
        pool,
        app,
        json!([
            ["add-triple", rows[0], id, rows[0]],
            ["add-triple", rows[0], score, 1],
            ["add-triple", rows[0], name, "a"],
            ["add-triple", rows[0], nick, "aa"],
            ["add-triple", rows[0], when, "2024-01-01T00:00:00Z"],
            ["add-triple", rows[1], id, rows[1]],
            ["add-triple", rows[1], score, 2],
            ["add-triple", rows[1], name, "b"],
            ["add-triple", rows[2], id, rows[2]],
            ["add-triple", rows[2], score, 3],
        ]),
    )
    .await
    .unwrap();
    Things { app, score, rows }
}

async fn q(
    pool: &PgPool,
    app: Uuid,
    q: Value,
) -> instant_core::error::Result<instant_core::instaql::QueryResult> {
    let attrs = attrs_of(pool, app).await;
    let ctx = QueryCtx {
        app_id: app,
        attrs: &attrs,
        admin: false,
    };
    let mut conn = pool.acquire().await.unwrap();
    query(&mut conn, &ctx, &q).await
}

fn eids(res: &instant_core::instaql::QueryResult) -> Vec<Uuid> {
    res.forms[0].entities.iter().map(|e| e.eid).collect()
}

/// attr_pat.clj:228-295, :307-317, :366-372, :410-419; instaql.clj:313-315,
/// :669-675, :1171-1172.
#[tokio::test]
async fn query_value_validation_matches_legacy() {
    let pool = pool().await;
    let t = things(&pool).await;
    let app = t.app;
    async fn err_msg(pool: &PgPool, app: Uuid, v: Value) -> String {
        q(pool, app, v).await.unwrap_err().message
    }
    let msg = |v: Value| err_msg(&pool, app, v);

    assert_eq!(
        msg(json!({"things": {"$": {"where": {"name": 5}}}})).await,
        "Validation failed for query: The data type of `things.name` is `string`, but the query got the value `5` of type `number`."
    );
    // unindexed checked attrs are validated too
    assert_eq!(
        msg(json!({"things": {"$": {"where": {"nick": 5}}}})).await,
        "Validation failed for query: The data type of `things.nick` is `string`, but the query got the value `5` of type `number`."
    );
    assert_eq!(
        msg(json!({"things": {"$": {"where": {"score": "2"}}}})).await,
        "Validation failed for query: The data type of `things.score` is `number`, but the query got the value `\"2\"` of type `string`."
    );
    assert_eq!(
        msg(json!({"things": {"$": {"where": {"when": "now"}}}})).await,
        "Validation failed for query: The data type of `things.when` is `date`, but the query got value `\"now\"` of type `string`."
    );
    assert_eq!(
        msg(json!({"things": {"$": {"where": {"when": {"$gt": "not-a-date"}}}}})).await,
        "Validation failed for query: The data type of `things.when` is `date`, but the query got value `\"not-a-date\"` of type `string`."
    );
    assert_eq!(
        msg(json!({"things": {"$": {"where": {"owner": {"$not": "nope"}}}}})).await,
        "Validation failed for query: Expected owner to be a uuid, got {\"$not\":\"nope\"}"
    );
    assert_eq!(
        msg(json!({"things": {"$": {"where": {"name": {"$like": 5}}}}})).await,
        "Validation failed for query: The $like value for `things.name` must be a string, but the query got the value `5` of type `number`."
    );

    // comparison parses relative keywords (every row is in 2024)
    let res = q(
        &pool,
        app,
        json!({"things": {"$": {"where": {"when": {"$gt": "now"}}}}}),
    )
    .await
    .unwrap();
    assert!(eids(&res).is_empty());
    // only the first operator of an args map applies
    let res = q(
        &pool,
        app,
        json!({"things": {"$": {"where": {"score": {"$gt": 0, "$lt": 2}}}}}),
    )
    .await
    .unwrap();
    assert_eq!(eids(&res).len(), 3);
    // empty order is a no-op
    let res = q(&pool, app, json!({"things": {"$": {"order": {}}}}))
        .await
        .unwrap();
    assert_eq!(eids(&res).len(), 3);
    // no page-info for an unknown namespace / a missing attr
    let res = q(&pool, app, json!({"nothing": {"$": {"limit": 2}}}))
        .await
        .unwrap();
    assert!(res.forms[0].page_info.is_none());
    let res = q(
        &pool,
        app,
        json!({"things": {"$": {"where": {"missing": "x"}, "limit": 2}}}),
    )
    .await
    .unwrap();
    assert!(eids(&res).is_empty());
    assert!(res.forms[0].page_info.is_none());
}

/// Rows predating the indexed-null backfill have no triple for the order
/// attr; legacy leaves them out of an ordered query (verified live in
/// differential step 25) and the query must not break on them.
#[tokio::test]
async fn ordering_by_an_attr_some_rows_lack() {
    let pool = pool().await;
    let t = things(&pool).await;
    sqlx::query("DELETE FROM triples WHERE app_id = $1 AND attr_id = $2 AND entity_id = $3")
        .bind(t.app)
        .bind(t.score)
        .bind(t.rows[2])
        .execute(&pool)
        .await
        .unwrap();
    let res = q(
        &pool,
        t.app,
        json!({"things": {"$": {"order": {"score": "asc"}}}}),
    )
    .await
    .unwrap();
    assert_eq!(eids(&res), vec![t.rows[0], t.rows[1]]);
    let res = q(
        &pool,
        t.app,
        json!({"things": {"$": {"order": {"score": "desc"}, "limit": 2}}}),
    )
    .await
    .unwrap();
    assert_eq!(eids(&res), vec![t.rows[1], t.rows[0]]);
    // an explicit null (the backfilled kind) still sorts first
    let res = q(
        &pool,
        t.app,
        json!({"things": {"$": {"order": {"when": "asc"}}}}),
    )
    .await
    .unwrap();
    assert_eq!(eids(&res).len(), 3);
    assert_ne!(eids(&res)[0], t.rows[0]);
}

// ---------------------------------------------------------------------------
// rules

/// rule.clj:100-111, :139-142: `$default.bind` ++ `<etype>.bind`, array or
/// object form.
#[tokio::test]
async fn default_and_object_form_binds_apply() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    let admin = mk_user(&pool, app, "admin@x.example").await;
    let other = mk_user(&pool, app, "other@x.example").await;
    set_rules(
        &pool,
        app,
        json!({
            "$default": {"bind": {"isAdmin": "auth.email == 'admin@x.example'"}},
            "todos": {
                "bind": ["hasTitle", "data.title != null"],
                "allow": {"view": "isAdmin && hasTitle"}
            }
        }),
    )
    .await;
    let (e1, e2) = (Uuid::new_v4(), Uuid::new_v4());
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", e1, ids.todos_id, e1],
            ["add-triple", e1, ids.todos_title, "titled"],
            ["add-triple", e2, ids.todos_id, e2]
        ]),
    )
    .await
    .unwrap();
    let res = filtered(&pool, app, &user_auth(admin), json!({"todos": {}}))
        .await
        .unwrap();
    assert_eq!(eids(&res), vec![e1]);
    let res = filtered(&pool, app, &user_auth(other), json!({"todos": {}}))
        .await
        .unwrap();
    assert!(eids(&res).is_empty());
}

/// exception.clj:291-297: Clojure truthiness — a string result passes.
#[tokio::test]
async fn non_boolean_rule_results_are_truthy() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    set_rules(
        &pool,
        app,
        json!({"todos": {"allow": {"view": "data.title"}}}),
    )
    .await;
    let (e1, e2) = (Uuid::new_v4(), Uuid::new_v4());
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", e1, ids.todos_id, e1],
            ["add-triple", e1, ids.todos_title, "titled"],
            ["add-triple", e2, ids.todos_id, e2]
        ]),
    )
    .await
    .unwrap();
    let res = filtered(&pool, app, &AuthCtx::default(), json!({"todos": {}}))
        .await
        .unwrap();
    assert_eq!(eids(&res), vec![e1]);
}

/// exception.clj:299-323: an evaluation error is surfaced, never a silent deny.
#[tokio::test]
async fn evaluation_errors_are_permission_evaluation_failed() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (schema, ids) = todo_schema_steps();
    transact_json(&pool, app, schema).await.unwrap();
    let e1 = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([
            ["add-triple", e1, ids.todos_id, e1],
            ["add-triple", e1, ids.todos_title, "titled"]
        ]),
    )
    .await
    .unwrap();
    set_rules(
        &pool,
        app,
        json!({"todos": {"allow": {"view": "data.title.foo", "update": "data.title.foo"}}}),
    )
    .await;
    let err = filtered(&pool, app, &AuthCtx::default(), json!({"todos": {}}))
        .await
        .unwrap_err();
    assert_eq!(err.error_type, "permission-evaluation-failed");
    assert_eq!(
        err.message,
        "Could not evaluate permission rule for `todos.view`. You may have a typo. Debug this in the sandbox and then update your permission rules."
    );
    assert_eq!(err.hint.unwrap()["rule"], json!(["todos", "view"]));
    let err = transact_with_perms(
        &pool,
        app,
        &AuthCtx::default(),
        json!([["add-triple", e1, ids.todos_title, "renamed"]]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "permission-evaluation-failed");
    assert_eq!(err.hint.unwrap()["rule"], json!(["todos", "update"]));
}
