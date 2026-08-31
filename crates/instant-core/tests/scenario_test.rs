//! Scenario tests modeled on the legacy instaql/transaction test suites
//! (zeneca books/users/bookshelves style fixtures).

mod common;

use common::*;
use instant_core::instaql::{query, QueryCtx};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::collections::HashMap;
use uuid::Uuid;

struct Zeneca {
    app: Uuid,
    attrs: HashMap<&'static str, Uuid>,
    users: HashMap<&'static str, Uuid>,
    books: HashMap<&'static str, Uuid>,
    shelves: HashMap<&'static str, Uuid>,
}

/// users(handle uniq idx str, fullName, createdAt idx date)
/// bookshelves(name, order idx number) -users.bookshelves-> users
/// books(title uniq idx str, pageCount idx number) <-bookshelves.books->
async fn zeneca(pool: &PgPool) -> Zeneca {
    let app = mk_app(pool).await;
    let mut attrs = HashMap::new();
    for name in [
        "users.id",
        "users.handle",
        "users.fullName",
        "users.createdAt",
        "bookshelves.id",
        "bookshelves.name",
        "bookshelves.order",
        "books.id",
        "books.title",
        "books.pageCount",
        "users.bookshelves",
        "bookshelves.books",
    ] {
        attrs.insert(
            match name {
                "users.id" => "users.id",
                "users.handle" => "users.handle",
                "users.fullName" => "users.fullName",
                "users.createdAt" => "users.createdAt",
                "bookshelves.id" => "bookshelves.id",
                "bookshelves.name" => "bookshelves.name",
                "bookshelves.order" => "bookshelves.order",
                "books.id" => "books.id",
                "books.title" => "books.title",
                "books.pageCount" => "books.pageCount",
                "users.bookshelves" => "users.bookshelves",
                _ => "bookshelves.books",
            },
            Uuid::new_v4(),
        );
    }
    let a = |k: &str| attrs[k];
    let blob = |id: Uuid, etype: &str, label: &str, uniq: bool, idx: bool, cdt: Option<&str>| {
        let mut attr = json!({
            "id": id, "forward-identity": [Uuid::new_v4(), etype, label],
            "value-type": "blob", "cardinality": "one", "unique?": uniq, "index?": idx});
        if let Some(c) = cdt {
            attr["checked-data-type"] = json!(c);
        }
        json!(["add-attr", attr])
    };
    let steps = json!([
        blob(a("users.id"), "users", "id", true, false, None),
        blob(a("users.handle"), "users", "handle", true, true, Some("string")),
        blob(a("users.fullName"), "users", "fullName", false, false, None),
        blob(a("users.createdAt"), "users", "createdAt", false, true, Some("date")),
        blob(a("bookshelves.id"), "bookshelves", "id", true, false, None),
        blob(a("bookshelves.name"), "bookshelves", "name", false, false, None),
        blob(a("bookshelves.order"), "bookshelves", "order", false, true, Some("number")),
        blob(a("books.id"), "books", "id", true, false, None),
        blob(a("books.title"), "books", "title", true, true, Some("string")),
        blob(a("books.pageCount"), "books", "pageCount", false, true, Some("number")),
        ["add-attr", {
            "id": a("users.bookshelves"),
            "forward-identity": [Uuid::new_v4(), "users", "bookshelves"],
            "reverse-identity": [Uuid::new_v4(), "bookshelves", "users"],
            "value-type": "ref", "cardinality": "many", "unique?": false, "index?": false}],
        ["add-attr", {
            "id": a("bookshelves.books"),
            "forward-identity": [Uuid::new_v4(), "bookshelves", "books"],
            "reverse-identity": [Uuid::new_v4(), "books", "bookshelves"],
            "value-type": "ref", "cardinality": "many", "unique?": false, "index?": false}]
    ]);
    transact_json(pool, app, steps).await.unwrap();

    let mut users = HashMap::new();
    let mut books = HashMap::new();
    let mut shelves = HashMap::new();

    for (key, handle, full, created) in [
        ("alex", "alex", "Alex", "2021-01-01T00:00:00Z"),
        ("stopa", "stopa", "Stepan", "2021-06-01T00:00:00Z"),
        ("joe", "joe", "Joe", "2022-03-01T00:00:00Z"),
        ("nicolegf", "nicolegf", "Nicole", "2023-01-01T00:00:00Z"),
    ] {
        let e = Uuid::new_v4();
        users.insert(key, e);
        transact_json(
            pool,
            app,
            json!([
                ["add-triple", e, a("users.id"), e],
                ["add-triple", e, a("users.handle"), handle],
                ["add-triple", e, a("users.fullName"), full],
                ["add-triple", e, a("users.createdAt"), created]
            ]),
        )
        .await
        .unwrap();
    }
    for (key, title, pages) in [
        ("musashi", "Musashi", 984),
        ("antifragile", "Antifragile", 519),
        ("atomic", "Atomic Habits", 320),
        ("sapiens", "Sapiens", 443),
    ] {
        let e = Uuid::new_v4();
        books.insert(key, e);
        transact_json(
            pool,
            app,
            json!([
                ["add-triple", e, a("books.id"), e],
                ["add-triple", e, a("books.title"), title],
                ["add-triple", e, a("books.pageCount"), pages]
            ]),
        )
        .await
        .unwrap();
    }
    // stopa: shelf "Currently Reading" [musashi, antifragile]; shelf "Done" [atomic]
    // alex: shelf "Nonfiction" [sapiens]
    for (key, owner, name, order, book_keys) in [
        (
            "s1",
            "stopa",
            "Currently Reading",
            0,
            vec!["musashi", "antifragile"],
        ),
        ("s2", "stopa", "Done", 1, vec!["atomic"]),
        ("s3", "alex", "Nonfiction", 0, vec!["sapiens"]),
    ] {
        let e = Uuid::new_v4();
        shelves.insert(key, e);
        let mut steps = vec![
            json!(["add-triple", e, a("bookshelves.id"), e]),
            json!(["add-triple", e, a("bookshelves.name"), name]),
            json!(["add-triple", e, a("bookshelves.order"), order]),
            json!(["add-triple", users[owner], a("users.bookshelves"), e]),
        ];
        for bk in book_keys {
            steps.push(json!(["add-triple", e, a("bookshelves.books"), books[bk]]));
        }
        transact_json(pool, app, Value::Array(steps)).await.unwrap();
    }
    Zeneca {
        app,
        attrs,
        users,
        books,
        shelves,
    }
}

async fn q(pool: &PgPool, app: Uuid, q_json: Value) -> instant_core::instaql::QueryResult {
    let attrs = attrs_of(pool, app).await;
    let ctx = QueryCtx {
        app_id: app,
        attrs: &attrs,
        admin: true,
    };
    let mut conn = pool.acquire().await.unwrap();
    query(&mut conn, &ctx, &q_json).await.unwrap()
}

fn eids(res: &instant_core::instaql::QueryResult, k: &str) -> Vec<Uuid> {
    res.forms
        .iter()
        .find(|f| f.k == k)
        .map(|f| f.entities.iter().map(|e| e.eid).collect())
        .unwrap_or_default()
}

#[tokio::test]
async fn deep_where_two_hops() {
    let pool = pool().await;
    let z = zeneca(&pool).await;
    // users who have a bookshelf containing Musashi
    let res = q(
        &pool,
        z.app,
        json!({"users": {"$": {"where": {"bookshelves.books.title": "Musashi"}}}}),
    )
    .await;
    assert_eq!(eids(&res, "users"), vec![z.users["stopa"]]);

    // books on shelves owned by stopa (reverse two hops)
    let res = q(
        &pool,
        z.app,
        json!({"books": {"$": {"where": {"bookshelves.users.handle": "stopa"}}}}),
    )
    .await;
    let got = eids(&res, "books");
    assert_eq!(got.len(), 3);
    assert!(got.contains(&z.books["musashi"]));
    assert!(got.contains(&z.books["antifragile"]));
    assert!(got.contains(&z.books["atomic"]));
}

#[tokio::test]
async fn where_and_with_multiple_link_conds() {
    let pool = pool().await;
    let z = zeneca(&pool).await;
    // stopa has both books on the same shelf
    let res = q(
        &pool,
        z.app,
        json!({"users": {"$": {"where": {"and": [
            {"bookshelves.books.title": "Musashi"},
            {"bookshelves.books.title": "Antifragile"}
        ]}}}}),
    )
    .await;
    assert_eq!(eids(&res, "users"), vec![z.users["stopa"]]);

    // no user has Musashi and Sapiens
    let res = q(
        &pool,
        z.app,
        json!({"users": {"$": {"where": {"and": [
            {"bookshelves.books.title": "Musashi"},
            {"bookshelves.books.title": "Sapiens"}
        ]}}}}),
    )
    .await;
    assert!(eids(&res, "users").is_empty());
}

#[tokio::test]
async fn where_or_across_links_and_values() {
    let pool = pool().await;
    let z = zeneca(&pool).await;
    let res = q(
        &pool,
        z.app,
        json!({"users": {"$": {"where": {"or": [
            {"handle": "joe"},
            {"bookshelves.books.title": "Sapiens"}
        ]}}}}),
    )
    .await;
    let got = eids(&res, "users");
    assert_eq!(got.len(), 2);
    assert!(got.contains(&z.users["joe"]));
    assert!(got.contains(&z.users["alex"]));
}

#[tokio::test]
async fn not_with_refs_and_null_links() {
    let pool = pool().await;
    let z = zeneca(&pool).await;
    // users whose bookshelves link is NOT s1 -> includes users with no shelves
    let res = q(
        &pool,
        z.app,
        json!({"users": {"$": {"where": {"bookshelves": {"$not": z.shelves["s1"]}}}}}),
    )
    .await;
    let got = eids(&res, "users");
    // stopa also matches ($not matches his other shelf s2); everyone matches
    assert_eq!(got.len(), 4);

    // users with no bookshelves at all
    let res = q(
        &pool,
        z.app,
        json!({"users": {"$": {"where": {"bookshelves": {"$isNull": true}}}}}),
    )
    .await;
    let got = eids(&res, "users");
    assert_eq!(got.len(), 2);
    assert!(got.contains(&z.users["joe"]));
    assert!(got.contains(&z.users["nicolegf"]));
}

#[tokio::test]
async fn is_null_through_link_path() {
    let pool = pool().await;
    let z = zeneca(&pool).await;
    // Add a shelf with no name (indexed? name is unindexed blob -> no null row).
    let a = |k: &str| z.attrs[k];
    let empty_shelf = Uuid::new_v4();
    transact_json(
        &pool,
        z.app,
        json!([
            ["add-triple", empty_shelf, a("bookshelves.id"), empty_shelf],
            [
                "add-triple",
                z.users["joe"],
                a("users.bookshelves"),
                empty_shelf
            ]
        ]),
    )
    .await
    .unwrap();
    // users whose shelf name is null (joe's unnamed shelf; users w/o shelves match too)
    let res = q(
        &pool,
        z.app,
        json!({"users": {"$": {"where": {"bookshelves.name": {"$isNull": true}}}}}),
    )
    .await;
    let got = eids(&res, "users");
    assert!(got.contains(&z.users["joe"]));
    assert!(got.contains(&z.users["nicolegf"])); // no shelves -> prefix isNull matches
    assert!(!got.contains(&z.users["stopa"]));
}

#[tokio::test]
async fn pagination_by_checked_number_field() {
    let pool = pool().await;
    let z = zeneca(&pool).await;
    let res = q(
        &pool,
        z.app,
        json!({"books": {"$": {"order": {"pageCount": "desc"}, "limit": 2}}}),
    )
    .await;
    assert_eq!(
        eids(&res, "books"),
        vec![z.books["musashi"], z.books["antifragile"]]
    );
    let pi = res.forms[0].page_info.as_ref().unwrap();
    assert!(pi.has_next_page);
    let end = pi.end_cursor.clone().unwrap();

    let res = q(
        &pool,
        z.app,
        json!({"books": {"$": {"order": {"pageCount": "desc"}, "limit": 2, "after": end}}}),
    )
    .await;
    assert_eq!(
        eids(&res, "books"),
        vec![z.books["sapiens"], z.books["atomic"]]
    );
    let pi = res.forms[0].page_info.as_ref().unwrap();
    assert!(!pi.has_next_page);
    assert!(pi.has_previous_page);

    // walk backward with before
    let start = pi.start_cursor.clone().unwrap();
    let res = q(
        &pool,
        z.app,
        json!({"books": {"$": {"order": {"pageCount": "desc"}, "last": 2, "before": start}}}),
    )
    .await;
    assert_eq!(
        eids(&res, "books"),
        vec![z.books["musashi"], z.books["antifragile"]]
    );
}

#[tokio::test]
async fn date_comparisons() {
    let pool = pool().await;
    let z = zeneca(&pool).await;
    let res = q(
        &pool,
        z.app,
        json!({"users": {"$": {"where": {"createdAt": {"$gt": "2021-12-31T00:00:00Z"}}}}}),
    )
    .await;
    let got = eids(&res, "users");
    assert_eq!(got.len(), 2);
    assert!(got.contains(&z.users["joe"]));
    assert!(got.contains(&z.users["nicolegf"]));

    // epoch-ms number against date attr
    let res = q(
        &pool,
        z.app,
        json!({"users": {"$": {"where": {"createdAt": {"$lte": 1622505600000i64}}}}}),
    )
    .await;
    let got = eids(&res, "users");
    assert_eq!(got.len(), 2); // alex + stopa
}

#[tokio::test]
async fn same_eid_in_two_namespaces_stays_separate() {
    let pool = pool().await;
    let app = mk_app(&pool).await;
    let (a1, a2, b1, b2) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    let shared = Uuid::new_v4();
    transact_json(
        &pool,
        app,
        json!([
            ["add-attr", {"id": a1, "forward-identity": [Uuid::new_v4(), "cats", "id"],
                "value-type": "blob", "cardinality": "one", "unique?": true, "index?": false}],
            ["add-attr", {"id": a2, "forward-identity": [Uuid::new_v4(), "cats", "name"],
                "value-type": "blob", "cardinality": "one", "unique?": false, "index?": false}],
            ["add-attr", {"id": b1, "forward-identity": [Uuid::new_v4(), "dogs", "id"],
                "value-type": "blob", "cardinality": "one", "unique?": true, "index?": false}],
            ["add-attr", {"id": b2, "forward-identity": [Uuid::new_v4(), "dogs", "name"],
                "value-type": "blob", "cardinality": "one", "unique?": false, "index?": false}],
            ["add-triple", shared, a1, shared],
            ["add-triple", shared, a2, "felix"],
            ["add-triple", shared, b1, shared],
            ["add-triple", shared, b2, "rex"]
        ]),
    )
    .await
    .unwrap();
    let res = q(&pool, app, json!({"cats": {}, "dogs": {}})).await;
    let cats = res.forms.iter().find(|f| f.k == "cats").unwrap();
    let dogs = res.forms.iter().find(|f| f.k == "dogs").unwrap();
    let cat_labels: Vec<&Value> = cats.entities[0].triples.iter().map(|t| &t.v).collect();
    assert!(cat_labels.contains(&&json!("felix")));
    assert!(!cat_labels.contains(&&json!("rex")));
    let dog_labels: Vec<&Value> = dogs.entities[0].triples.iter().map(|t| &t.v).collect();
    assert!(dog_labels.contains(&&json!("rex")));
    assert!(!dog_labels.contains(&&json!("felix")));
}

#[tokio::test]
async fn update_attr_toggles_unique_and_rewrites_flags() {
    let pool = pool().await;
    let z = zeneca(&pool).await;
    let name_attr = z.attrs["bookshelves.name"];
    // make bookshelves.name unique
    transact_json(
        &pool,
        z.app,
        json!([["update-attr", {"id": name_attr, "unique?": true}]]),
    )
    .await
    .unwrap();
    // existing rows now have av flag
    let (av_count,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM triples WHERE app_id = $1 AND attr_id = $2 AND av")
            .bind(z.app)
            .bind(name_attr)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(av_count >= 3);
    // duplicate name now errors
    let e = Uuid::new_v4();
    let err = transact_json(
        &pool,
        z.app,
        json!([
            ["add-triple", e, z.attrs["bookshelves.id"], e],
            ["add-triple", e, name_attr, "Done"]
        ]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error_type, "record-not-unique");
}

#[tokio::test]
async fn delete_attr_soft_deletes() {
    let pool = pool().await;
    let z = zeneca(&pool).await;
    let full_name = z.attrs["users.fullName"];
    transact_json(&pool, z.app, json!([["delete-attr", full_name]]))
        .await
        .unwrap();
    // attr no longer visible
    let attrs = attrs_of(&pool, z.app).await;
    assert!(attrs.get(&full_name).is_none());
    assert!(attrs.by_fwd_name("users", "fullName").is_none());
    // a new attr can take the name
    let new_attr = Uuid::new_v4();
    transact_json(
        &pool,
        z.app,
        json!([["add-attr", {"id": new_attr,
            "forward-identity": [Uuid::new_v4(), "users", "fullName"],
            "value-type": "blob", "cardinality": "one", "unique?": false, "index?": false}]]),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn retract_link_via_lookup_value() {
    let pool = pool().await;
    let z = zeneca(&pool).await;
    let a = |k: &str| z.attrs[k];
    // unlink musashi from s1 using a lookup ref for the book
    transact_json(
        &pool,
        z.app,
        json!([[
            "retract-triple",
            z.shelves["s1"],
            a("bookshelves.books"),
            [a("books.title"), "Musashi"]
        ]]),
    )
    .await
    .unwrap();
    let res = q(
        &pool,
        z.app,
        json!({"books": {"$": {"where": {"bookshelves": z.shelves["s1"]}}}}),
    )
    .await;
    assert_eq!(eids(&res, "books"), vec![z.books["antifragile"]]);
}

#[tokio::test]
async fn nested_children_with_where_and_grandchildren() {
    let pool = pool().await;
    let z = zeneca(&pool).await;
    let res = q(
        &pool,
        z.app,
        json!({"users": {
            "$": {"where": {"handle": "stopa"}},
            "bookshelves": {
                "$": {"where": {"name": "Currently Reading"}},
                "books": {}
            }
        }}),
    )
    .await;
    let user = &res.forms[0].entities[0];
    let shelves = &user.children[0];
    assert_eq!(shelves.entities.len(), 1);
    let books = &shelves.entities[0].children[0];
    assert_eq!(books.entities.len(), 2);
}

#[tokio::test]
async fn where_in_on_links() {
    let pool = pool().await;
    let z = zeneca(&pool).await;
    let res = q(
        &pool,
        z.app,
        json!({"books": {"$": {"where": {"bookshelves": {
            "$in": [z.shelves["s1"], z.shelves["s3"]]}}}}}),
    )
    .await;
    let got = eids(&res, "books");
    assert_eq!(got.len(), 3);
    assert!(got.contains(&z.books["sapiens"]));
}
