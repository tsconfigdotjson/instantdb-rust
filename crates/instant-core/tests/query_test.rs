mod common;

use common::*;
use instant_core::instaql::{query, QueryCtx};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

struct Fixture {
    app: Uuid,
    users_handle: Uuid,
    #[allow(dead_code)]
    posts_id: Uuid,
    users: std::collections::HashMap<&'static str, Uuid>,
    posts: std::collections::HashMap<&'static str, Uuid>,
}

/// users: handle (unique idx string), age (idx number), active (idx boolean),
/// joined (idx date), nickname (plain); posts: title (plain), author (ref one
/// -> users.posts), tags (ref many -> tags.posts)
async fn fixture(pool: &PgPool) -> Fixture {
    let app = mk_app(pool).await;
    let users_id = Uuid::new_v4();
    let users_handle = Uuid::new_v4();
    let users_age = Uuid::new_v4();
    let users_active = Uuid::new_v4();
    let users_joined = Uuid::new_v4();
    let users_nickname = Uuid::new_v4();
    let posts_id = Uuid::new_v4();
    let posts_title = Uuid::new_v4();
    let posts_author = Uuid::new_v4();
    let tags_id = Uuid::new_v4();
    let tags_name = Uuid::new_v4();
    let posts_tags = Uuid::new_v4();

    let mk_blob =
        |id: Uuid, etype: &str, label: &str, unique: bool, index: bool, cdt: Option<&str>| {
            let mut attr = json!({
                "id": id,
                "forward-identity": [Uuid::new_v4(), etype, label],
                "value-type": "blob", "cardinality": "one",
                "unique?": unique, "index?": index
            });
            if let Some(c) = cdt {
                attr["checked-data-type"] = json!(c);
            }
            json!(["add-attr", attr])
        };

    let steps = json!([
        mk_blob(users_id, "users", "id", true, false, None),
        mk_blob(users_handle, "users", "handle", true, true, Some("string")),
        mk_blob(users_age, "users", "age", false, true, Some("number")),
        mk_blob(users_active, "users", "active", false, true, Some("boolean")),
        mk_blob(users_joined, "users", "joined", false, true, Some("date")),
        mk_blob(users_nickname, "users", "nickname", false, false, None),
        mk_blob(posts_id, "posts", "id", true, false, None),
        mk_blob(posts_title, "posts", "title", false, false, None),
        mk_blob(tags_id, "tags", "id", true, false, None),
        mk_blob(tags_name, "tags", "name", false, false, None),
        ["add-attr", {
            "id": posts_author,
            "forward-identity": [Uuid::new_v4(), "posts", "author"],
            "reverse-identity": [Uuid::new_v4(), "users", "posts"],
            "value-type": "ref", "cardinality": "one", "unique?": false, "index?": false
        }],
        ["add-attr", {
            "id": posts_tags,
            "forward-identity": [Uuid::new_v4(), "posts", "tags"],
            "reverse-identity": [Uuid::new_v4(), "tags", "posts"],
            "value-type": "ref", "cardinality": "many", "unique?": false, "index?": false
        }]
    ]);
    transact_json(pool, app, steps).await.unwrap();

    let mut users = std::collections::HashMap::new();
    let mut posts = std::collections::HashMap::new();
    let data: Vec<(&str, i64, bool, &str, Option<&str>)> = vec![
        ("alice", 30, true, "2023-01-15T00:00:00Z", Some("al")),
        ("bob", 25, false, "2023-06-01T00:00:00Z", None),
        ("carol", 35, true, "2024-02-20T00:00:00Z", Some("cc")),
    ];
    for (handle, age, active, joined, nickname) in data {
        let eid = Uuid::new_v4();
        users.insert(
            match handle {
                "alice" => "alice",
                "bob" => "bob",
                _ => "carol",
            },
            eid,
        );
        let mut steps = vec![
            json!(["add-triple", eid, users_id, eid]),
            json!(["add-triple", eid, users_handle, handle]),
            json!(["add-triple", eid, users_age, age]),
            json!(["add-triple", eid, users_active, active]),
            json!(["add-triple", eid, users_joined, joined]),
        ];
        if let Some(n) = nickname {
            steps.push(json!(["add-triple", eid, users_nickname, n]));
        }
        transact_json(pool, app, Value::Array(steps)).await.unwrap();
    }
    let post_data: Vec<(&str, &str)> = vec![("p1", "alice"), ("p2", "alice"), ("p3", "bob")];
    for (title, author) in post_data {
        let eid = Uuid::new_v4();
        posts.insert(
            match title {
                "p1" => "p1",
                "p2" => "p2",
                _ => "p3",
            },
            eid,
        );
        transact_json(
            pool,
            app,
            json!([
                ["add-triple", eid, posts_id, eid],
                ["add-triple", eid, posts_title, title],
                ["add-triple", eid, posts_author, users[author]]
            ]),
        )
        .await
        .unwrap();
    }
    Fixture {
        app,
        users_handle,
        posts_id,
        users,
        posts,
    }
}

async fn run_query(pool: &PgPool, app: Uuid, q: Value) -> instant_core::instaql::QueryResult {
    let attrs = attrs_of(pool, app).await;
    let ctx = QueryCtx {
        app_id: app,
        attrs: &attrs,
        admin: true,
    };
    let mut conn = pool.acquire().await.unwrap();
    query(&mut conn, &ctx, &q).await.unwrap()
}

async fn run_query_err(pool: &PgPool, app: Uuid, q: Value) -> instant_core::error::InstantError {
    let attrs = attrs_of(pool, app).await;
    let ctx = QueryCtx {
        app_id: app,
        attrs: &attrs,
        admin: false,
    };
    let mut conn = pool.acquire().await.unwrap();
    query(&mut conn, &ctx, &q).await.unwrap_err()
}

/// Extract the label->value map of each returned entity for a form.
fn entity_maps(
    res: &instant_core::instaql::QueryResult,
    attrs: &instant_core::attr::AttrMap,
    k: &str,
) -> Vec<serde_json::Map<String, Value>> {
    let form = res.forms.iter().find(|f| f.k == k).unwrap();
    form.entities
        .iter()
        .map(|e| {
            let mut m = serde_json::Map::new();
            for t in &e.triples {
                if let Some(a) = attrs.get(&t.a) {
                    m.insert(a.label.clone(), t.v.clone());
                }
            }
            m
        })
        .collect()
}

fn handles(maps: &[serde_json::Map<String, Value>]) -> Vec<String> {
    maps.iter()
        .map(|m| {
            m.get("handle")
                .and_then(|v| v.as_str())
                .unwrap_or("?")
                .to_string()
        })
        .collect()
}

#[tokio::test]
async fn flat_where() {
    let pool = pool().await;
    let fx = fixture(&pool).await;
    let attrs = attrs_of(&pool, fx.app).await;

    // all users
    let res = run_query(&pool, fx.app, json!({"users": {}})).await;
    assert_eq!(res.forms[0].entities.len(), 3);

    // where by attr
    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"where": {"handle": "alice"}}}}),
    )
    .await;
    let maps = entity_maps(&res, &attrs, "users");
    assert_eq!(handles(&maps), vec!["alice"]);

    // where by id
    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"where": {"id": fx.users["bob"]}}}}),
    )
    .await;
    assert_eq!(handles(&entity_maps(&res, &attrs, "users")), vec!["bob"]);
}

#[tokio::test]
async fn deep_where_through_links() {
    let pool = pool().await;
    let fx = fixture(&pool).await;
    let attrs = attrs_of(&pool, fx.app).await;

    // posts whose author's handle is alice (dot path through fwd link)
    let res = run_query(
        &pool,
        fx.app,
        json!({"posts": {"$": {"where": {"author.handle": "alice"}}}}),
    )
    .await;
    let titles: Vec<_> = entity_maps(&res, &attrs, "posts")
        .iter()
        .map(|m| m["title"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(titles.len(), 2);
    assert!(titles.contains(&"p1".to_string()) && titles.contains(&"p2".to_string()));

    // users who have a post titled p3 (reverse link path)
    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"where": {"posts.title": "p3"}}}}),
    )
    .await;
    assert_eq!(handles(&entity_maps(&res, &attrs, "users")), vec!["bob"]);
}

#[tokio::test]
async fn where_link_eid_relations() {
    let pool = pool().await;
    let fx = fixture(&pool).await;
    let attrs = attrs_of(&pool, fx.app).await;

    // forward: posts where author = uuid
    let res = run_query(
        &pool,
        fx.app,
        json!({"posts": {"$": {"where": {"author": fx.users["bob"]}}}}),
    )
    .await;
    assert_eq!(res.forms[0].entities.len(), 1);

    // reverse: users where posts = post uuid
    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"where": {"posts": fx.posts["p1"]}}}}),
    )
    .await;
    assert_eq!(handles(&entity_maps(&res, &attrs, "users")), vec!["alice"]);
}

#[tokio::test]
async fn where_in_not_ne() {
    let pool = pool().await;
    let fx = fixture(&pool).await;
    let attrs = attrs_of(&pool, fx.app).await;

    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"where": {"handle": {"$in": ["alice", "carol"]}}}}}),
    )
    .await;
    let mut h = handles(&entity_maps(&res, &attrs, "users"));
    h.sort();
    assert_eq!(h, vec!["alice", "carol"]);

    // $ne matches entities where attr != value INCLUDING missing/null
    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"where": {"nickname": {"$ne": "al"}}}}}),
    )
    .await;
    let mut h = handles(&entity_maps(&res, &attrs, "users"));
    h.sort();
    // bob has no nickname, carol has "cc" -> both match
    assert_eq!(h, vec!["bob", "carol"]);
}

#[tokio::test]
async fn where_is_null() {
    let pool = pool().await;
    let fx = fixture(&pool).await;
    let attrs = attrs_of(&pool, fx.app).await;

    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"where": {"nickname": {"$isNull": true}}}}}),
    )
    .await;
    assert_eq!(handles(&entity_maps(&res, &attrs, "users")), vec!["bob"]);

    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"where": {"nickname": {"$isNull": false}}}}}),
    )
    .await;
    let mut h = handles(&entity_maps(&res, &attrs, "users"));
    h.sort();
    assert_eq!(h, vec!["alice", "carol"]);

    // link isNull: users with no posts
    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"where": {"posts": {"$isNull": true}}}}}),
    )
    .await;
    assert_eq!(handles(&entity_maps(&res, &attrs, "users")), vec!["carol"]);
}

#[tokio::test]
async fn where_like_and_comparators() {
    let pool = pool().await;
    let fx = fixture(&pool).await;
    let attrs = attrs_of(&pool, fx.app).await;

    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"where": {"handle": {"$like": "%aro%"}}}}}),
    )
    .await;
    assert_eq!(handles(&entity_maps(&res, &attrs, "users")), vec!["carol"]);

    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"where": {"handle": {"$ilike": "ALICE"}}}}}),
    )
    .await;
    assert_eq!(handles(&entity_maps(&res, &attrs, "users")), vec!["alice"]);

    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"where": {"age": {"$gt": 26}}}}}),
    )
    .await;
    let mut h = handles(&entity_maps(&res, &attrs, "users"));
    h.sort();
    assert_eq!(h, vec!["alice", "carol"]);

    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"where": {"joined": {"$lt": "2023-07-01T00:00:00Z"}}}}}),
    )
    .await;
    let mut h = handles(&entity_maps(&res, &attrs, "users"));
    h.sort();
    assert_eq!(h, vec!["alice", "bob"]);

    // number value against date epoch-ms
    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"where": {"joined": {"$gte": 1704067200000i64}}}}}),
    )
    .await;
    assert_eq!(handles(&entity_maps(&res, &attrs, "users")), vec!["carol"]);

    // comparator on unindexed attr -> validation error
    let err = run_query_err(
        &pool,
        fx.app,
        json!({"users": {"$": {"where": {"nickname": {"$gt": "a"}}}}}),
    )
    .await;
    assert_eq!(err.error_type, "validation-failed");
}

#[tokio::test]
async fn where_or_and() {
    let pool = pool().await;
    let fx = fixture(&pool).await;
    let attrs = attrs_of(&pool, fx.app).await;

    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"where": {
            "or": [{"handle": "alice"}, {"age": {"$gt": 33}}]
        }}}}),
    )
    .await;
    let mut h = handles(&entity_maps(&res, &attrs, "users"));
    h.sort();
    assert_eq!(h, vec!["alice", "carol"]);

    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"where": {
            "and": [{"active": true}, {"age": {"$lt": 33}}]
        }}}}),
    )
    .await;
    assert_eq!(handles(&entity_maps(&res, &attrs, "users")), vec!["alice"]);

    // nested or inside and, with deep path
    let res = run_query(
        &pool,
        fx.app,
        json!({"posts": {"$": {"where": {
            "and": [{"or": [{"author.handle": "bob"}, {"title": "p1"}]}]
        }}}}),
    )
    .await;
    assert_eq!(res.forms[0].entities.len(), 2);

    // empty or -> validation error
    let err = run_query_err(
        &pool,
        fx.app,
        json!({"users": {"$": {"where": {"or": []}}}}),
    )
    .await;
    assert_eq!(err.error_type, "validation-failed");
}

#[tokio::test]
async fn child_forms_nesting() {
    let pool = pool().await;
    let fx = fixture(&pool).await;

    let res = run_query(&pool, fx.app, json!({"users": {"posts": {}}})).await;
    let form = &res.forms[0];
    assert_eq!(form.entities.len(), 3);
    let alice = form
        .entities
        .iter()
        .find(|e| e.eid == fx.users["alice"])
        .unwrap();
    let posts = &alice.children[0];
    assert_eq!(posts.k, "posts");
    assert_eq!(posts.entities.len(), 2);
    assert_eq!(posts.link_triples.len(), 2);
    // link triples keep stored orientation: [post, author-attr, user]
    for t in &posts.link_triples {
        assert_eq!(t.v, json!(fx.users["alice"]));
    }
    let carol = form
        .entities
        .iter()
        .find(|e| e.eid == fx.users["carol"])
        .unwrap();
    assert_eq!(carol.children[0].entities.len(), 0);

    // child where filters children only
    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"posts": {"$": {"where": {"title": "p1"}}}}}),
    )
    .await;
    let form = &res.forms[0];
    assert_eq!(form.entities.len(), 3);
    let alice = form
        .entities
        .iter()
        .find(|e| e.eid == fx.users["alice"])
        .unwrap();
    assert_eq!(alice.children[0].entities.len(), 1);

    // grandchildren: posts -> author -> posts
    let res = run_query(&pool, fx.app, json!({"posts": {"author": {"posts": {}}}})).await;
    let p1 = res.forms[0]
        .entities
        .iter()
        .find(|e| e.eid == fx.posts["p1"])
        .unwrap();
    let author = &p1.children[0];
    assert_eq!(author.entities.len(), 1);
    assert_eq!(author.entities[0].eid, fx.users["alice"]);
    assert_eq!(author.entities[0].children[0].entities.len(), 2);
}

#[tokio::test]
async fn missing_attrs_return_empty() {
    let pool = pool().await;
    let fx = fixture(&pool).await;

    // unknown namespace
    let res = run_query(&pool, fx.app, json!({"nonexistent": {}})).await;
    assert_eq!(res.forms[0].entities.len(), 0);

    // unknown where attr
    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"where": {"nope": "x"}}}}),
    )
    .await;
    assert_eq!(res.forms[0].entities.len(), 0);

    // unknown child link: parents still returned with empty children
    let res = run_query(&pool, fx.app, json!({"users": {"nothing": {}}})).await;
    assert_eq!(res.forms[0].entities.len(), 3);
    assert_eq!(res.forms[0].entities[0].children[0].entities.len(), 0);
}

#[tokio::test]
async fn pagination_and_ordering() {
    let pool = pool().await;
    let fx = fixture(&pool).await;
    let attrs = attrs_of(&pool, fx.app).await;

    // default order: serverCreatedAt asc == insertion order
    let res = run_query(&pool, fx.app, json!({"users": {"$": {"limit": 2}}})).await;
    let maps = entity_maps(&res, &attrs, "users");
    assert_eq!(handles(&maps), vec!["alice", "bob"]);
    let pi = res.forms[0].page_info.as_ref().unwrap();
    assert!(pi.has_next_page);
    assert!(!pi.has_previous_page);

    // after cursor continues
    let end = pi.end_cursor.clone().unwrap();
    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"limit": 2, "after": end}}}),
    )
    .await;
    assert_eq!(handles(&entity_maps(&res, &attrs, "users")), vec!["carol"]);
    let pi = res.forms[0].page_info.as_ref().unwrap();
    assert!(!pi.has_next_page);
    assert!(pi.has_previous_page);

    // order by typed attr desc
    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"order": {"age": "desc"}}}}),
    )
    .await;
    assert_eq!(
        handles(&entity_maps(&res, &attrs, "users")),
        vec!["carol", "alice", "bob"]
    );

    // last 2 with asc order = the 2 biggest, displayed ascending
    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"last": 2, "order": {"age": "asc"}}}}),
    )
    .await;
    assert_eq!(
        handles(&entity_maps(&res, &attrs, "users")),
        vec!["alice", "carol"]
    );
    let pi = res.forms[0].page_info.as_ref().unwrap();
    assert!(pi.has_previous_page);
    assert!(!pi.has_next_page);

    // offset
    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"offset": 1, "order": {"age": "asc"}}}}),
    )
    .await;
    assert_eq!(
        handles(&entity_maps(&res, &attrs, "users")),
        vec!["alice", "carol"]
    );
    assert!(res.forms[0].page_info.as_ref().unwrap().has_previous_page);

    // order by string attr
    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"order": {"handle": "desc"}}}}),
    )
    .await;
    assert_eq!(
        handles(&entity_maps(&res, &attrs, "users")),
        vec!["carol", "bob", "alice"]
    );

    // ordering by unindexed attr errors
    let err = run_query_err(
        &pool,
        fx.app,
        json!({"users": {"$": {"order": {"nickname": "asc"}}}}),
    )
    .await;
    assert_eq!(err.error_type, "validation-failed");

    // pagination on nested form errors
    let err = run_query_err(
        &pool,
        fx.app,
        json!({"users": {"posts": {"$": {"offset": 1}}}}),
    )
    .await;
    assert_eq!(err.error_type, "validation-failed");
}

#[tokio::test]
async fn pagination_null_order_values() {
    let pool = pool().await;
    let fx = fixture(&pool).await;
    let attrs = attrs_of(&pool, fx.app).await;

    // Add a user without age (indexed -> explicit null backfill)
    let users_id_attr = attrs.by_fwd_name("users", "id").unwrap().id;
    let handle_attr = fx.users_handle;
    let eid = Uuid::new_v4();
    transact_json(
        &pool,
        fx.app,
        json!([
            ["add-triple", eid, users_id_attr, eid],
            ["add-triple", eid, handle_attr, "dave"]
        ]),
    )
    .await
    .unwrap();

    // asc: nulls first
    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"order": {"age": "asc"}}}}),
    )
    .await;
    assert_eq!(
        handles(&entity_maps(&res, &attrs, "users")),
        vec!["dave", "bob", "alice", "carol"]
    );

    // desc: nulls last
    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"order": {"age": "desc"}}}}),
    )
    .await;
    assert_eq!(
        handles(&entity_maps(&res, &attrs, "users")),
        vec!["carol", "alice", "bob", "dave"]
    );

    // paginate across the null: limit 2 asc, then after
    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"order": {"age": "asc"}, "limit": 2}}}),
    )
    .await;
    assert_eq!(
        handles(&entity_maps(&res, &attrs, "users")),
        vec!["dave", "bob"]
    );
    let end = res.forms[0]
        .page_info
        .as_ref()
        .unwrap()
        .end_cursor
        .clone()
        .unwrap();
    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"order": {"age": "asc"}, "limit": 2, "after": end}}}),
    )
    .await;
    assert_eq!(
        handles(&entity_maps(&res, &attrs, "users")),
        vec!["alice", "carol"]
    );
}

#[tokio::test]
async fn aggregate_count() {
    let pool = pool().await;
    let fx = fixture(&pool).await;

    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"aggregate": "count"}}}),
    )
    .await;
    assert_eq!(res.forms[0].aggregate, Some(3));
    assert_eq!(res.forms[0].entities.len(), 0);

    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"aggregate": "count", "where": {"active": true}}}}),
    )
    .await;
    assert_eq!(res.forms[0].aggregate, Some(2));

    // non-admin -> error
    let err = run_query_err(
        &pool,
        fx.app,
        json!({"users": {"$": {"aggregate": "count"}}}),
    )
    .await;
    assert_eq!(err.error_type, "validation-failed");
}

#[tokio::test]
async fn fields_projection() {
    let pool = pool().await;
    let fx = fixture(&pool).await;
    let attrs = attrs_of(&pool, fx.app).await;

    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"fields": ["handle"], "where": {"handle": "alice"}}}}),
    )
    .await;
    let maps = entity_maps(&res, &attrs, "users");
    assert_eq!(maps.len(), 1);
    // only id + handle
    let keys: Vec<_> = maps[0].keys().cloned().collect();
    assert!(keys.contains(&"id".to_string()));
    assert!(keys.contains(&"handle".to_string()));
    assert!(!keys.contains(&"age".to_string()));
}

#[tokio::test]
async fn ws_result_shape() {
    let pool = pool().await;
    let fx = fixture(&pool).await;

    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"limit": 2}, "posts": {}}}),
    )
    .await;
    let ws = res.to_ws_result();
    let arr = ws.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    let data = &arr[0]["data"];
    let join_rows = data["datalog-result"]["join-rows"].as_array().unwrap();
    assert_eq!(join_rows.len(), 1);
    let triples = join_rows[0].as_array().unwrap();
    assert!(triples.len() > 5);
    for t in triples {
        let t = t.as_array().unwrap();
        assert_eq!(t.len(), 4);
        assert!(t[3].is_i64());
    }
    let pi = &data["page-info"]["users"];
    assert!(pi["start-cursor"].is_array());
    assert!(pi["has-next-page?"].is_boolean());
    assert!(arr[0]["child-nodes"].as_array().unwrap().is_empty());
}

/// (e, a, v) of every triple in the flattened ws result.
fn ws_triples(res: &instant_core::instaql::QueryResult) -> Vec<(Uuid, Uuid, Value)> {
    let ws = res.to_ws_result();
    ws[0]["data"]["datalog-result"]["join-rows"][0]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| {
            (
                Uuid::parse_str(t[0].as_str().unwrap()).unwrap(),
                Uuid::parse_str(t[1].as_str().unwrap()).unwrap(),
                t[2].clone(),
            )
        })
        .collect()
}

/// Issue #45: legacy's where patterns are part of the form's datalog query,
/// so the triples they matched ship with the result even when `fields`
/// leaves the attr out or they belong to a linked entity.
#[tokio::test]
async fn where_pattern_triples_ship_with_the_result() {
    let pool = pool().await;
    let fx = fixture(&pool).await;
    let attrs = attrs_of(&pool, fx.app).await;
    let age = attrs.by_fwd_name("users", "age").unwrap().id;
    let author = attrs.by_fwd_name("posts", "author").unwrap().id;
    let (alice, bob, carol) = (fx.users["alice"], fx.users["bob"], fx.users["carol"]);

    // a comparison on an attr the projection leaves out
    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"fields": ["handle"], "where": {"age": {"$gt": 26}}}}}),
    )
    .await;
    let ts = ws_triples(&res);
    assert!(ts.contains(&(alice, age, json!(30))));
    assert!(ts.contains(&(carol, age, json!(35))));
    assert!(!ts.iter().any(|t| t.0 == bob));
    // the entity nodes themselves stay projected
    let maps = entity_maps(&res, &attrs, "users");
    assert!(maps.iter().all(|m| !m.contains_key("age")));

    // a dotted path: the link triple and the linked entity's matched triple
    let res = run_query(
        &pool,
        fx.app,
        json!({"posts": {"$": {"fields": ["title"], "where": {"author.handle": "bob"}}}}),
    )
    .await;
    let ts = ws_triples(&res);
    let p3 = fx.posts["p3"];
    assert!(ts.contains(&(p3, author, json!(bob.to_string()))));
    assert!(ts.contains(&(bob, fx.users_handle, json!("bob"))));
    assert!(!ts.iter().any(|t| t.0 == alice));

    // `or`: only the branches an entity satisfies contribute
    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"fields": ["nickname"], "where": {"or": [{"age": {"$gt": 33}}, {"handle": "bob"}]}}}}),
    )
    .await;
    let ts = ws_triples(&res);
    assert!(ts.contains(&(carol, age, json!(35))));
    assert!(ts.contains(&(bob, fx.users_handle, json!("bob"))));
    assert!(!ts.contains(&(bob, age, json!(25))));
    assert!(!ts.contains(&(carol, fx.users_handle, json!("carol"))));

    // a child form's where rows come with its link triples
    let res = run_query(
        &pool,
        fx.app,
        json!({"users": {"$": {"fields": ["handle"]}, "posts": {"$": {"fields": ["id"], "where": {"author.age": {"$lt": 28}}}}}}),
    )
    .await;
    let ts = ws_triples(&res);
    assert!(ts.contains(&(bob, age, json!(25))));
    assert!(ts.contains(&(p3, author, json!(bob.to_string()))));
}
