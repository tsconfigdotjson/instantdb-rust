//! Topic-based invalidation narrowing (docs/QUERY.md §6).
//!
//! A registered query produces a set of coarse *topics*: `[e-part, attr-set,
//! v-part]` triples where each part is either a wildcard or a set of concrete
//! values. A transaction produces one topic per changed triple (from the
//! `rust_tx_changes` log). A query only needs recomputing when some tx change
//! matches some query topic; result-hash suppression stays as the correctness
//! backstop on top of this.
//!
//! The shapes mirror legacy `datalog.clj` topics with result substitution for
//! the entity-fetch pattern: the ea fetch is keyed by the (pre-permissions)
//! result entity ids, so a write to an unrelated entity of the same etype does
//! not refresh the query, while the "which entities" patterns (id attr, where
//! attrs, order attr, link attrs) stay wildcard on `e` so newly matching
//! entities are picked up. Anything the derivation cannot resolve (missing
//! attrs/etypes, attr-catalog changes) degrades to a catch-all that refreshes
//! everything — never a missed refresh.

use std::collections::{HashMap, HashSet};

use serde_json::Value;
use uuid::Uuid;

use crate::attr::{Attr, AttrMap, Cardinality, CheckedDataType, ValueType};
use crate::instaql::{EntityNode, Form, QueryResult, WhereCond, WhereOp};

/// One part of a topic: wildcard, or a set of concrete values. An empty set
/// matches nothing (legacy `#{}` semantics for empty bound results).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Part<T: Eq + std::hash::Hash> {
    Any,
    Set(HashSet<T>),
}

impl<T: Eq + std::hash::Hash> Part<T> {
    fn contains(&self, v: &T) -> bool {
        match self {
            Part::Any => true,
            Part::Set(s) => s.contains(v),
        }
    }
}

/// `[e-part, attr-set, v-part]`. The v-part uses [`value_key`] strings so
/// json values from the query and from the change log compare the same way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topic {
    pub e: Part<Uuid>,
    pub a: HashSet<Uuid>,
    pub v: Part<String>,
}

/// Topics for one registered query.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryTopics {
    pub topics: Vec<Topic>,
    /// true when the query must be recomputed on every tx (unresolvable
    /// shapes; matches legacy's `[#{:ea} _ _ _]` catch-all).
    pub catch_all: bool,
}

impl QueryTopics {
    pub fn catch_all() -> Self {
        QueryTopics {
            topics: vec![],
            catch_all: true,
        }
    }

    fn push(&mut self, e: Part<Uuid>, a: impl IntoIterator<Item = Uuid>, v: Part<String>) {
        self.topics.push(Topic {
            e,
            a: a.into_iter().collect(),
            v,
        });
    }
}

/// One changed triple from a transaction.
#[derive(Debug, Clone)]
pub struct TxChange {
    pub e: Uuid,
    pub a: Uuid,
    pub v: Value,
}

/// Topics of one or more transactions, indexed by attr for matching.
#[derive(Debug, Clone, Default)]
pub struct TxTopics {
    by_attr: HashMap<Uuid, Vec<(Uuid, String)>>,
    /// true when the tx changed the attr catalog (or its changes are unknown):
    /// every query matches.
    pub catch_all: bool,
}

impl TxTopics {
    pub fn catch_all() -> Self {
        TxTopics {
            by_attr: HashMap::new(),
            catch_all: true,
        }
    }

    pub fn from_changes(changes: impl IntoIterator<Item = TxChange>) -> Self {
        let mut by_attr: HashMap<Uuid, Vec<(Uuid, String)>> = HashMap::new();
        for c in changes {
            by_attr.entry(c.a).or_default().push((c.e, value_key(&c.v)));
        }
        TxTopics {
            by_attr,
            catch_all: false,
        }
    }

    pub fn is_empty(&self) -> bool {
        !self.catch_all && self.by_attr.is_empty()
    }

    /// Does any change in this tx set match any topic of the query?
    pub fn matches(&self, q: &QueryTopics) -> bool {
        if self.catch_all || q.catch_all {
            return true;
        }
        for t in &q.topics {
            for a in &t.a {
                let Some(rows) = self.by_attr.get(a) else {
                    continue;
                };
                if rows.iter().any(|(e, v)| t.e.contains(e) && t.v.contains(v)) {
                    return true;
                }
            }
        }
        false
    }
}

/// Canonical comparison key for a triple value: its JSON encoding, with
/// uuid-shaped strings normalized to lowercase hyphenated form so ids written
/// by different clients compare equal.
pub fn value_key(v: &Value) -> String {
    if let Value::String(s) = v {
        if let Ok(u) = Uuid::parse_str(s) {
            return Value::String(u.hyphenated().to_string()).to_string();
        }
    }
    v.to_string()
}

/// Derive the topics of a query from its parsed forms and its
/// (pre-permissions) result.
pub fn query_topics(attrs: &AttrMap, forms: &[Form], result: &QueryResult) -> QueryTopics {
    let mut out = QueryTopics::default();
    for (i, form) in forms.iter().enumerate() {
        let entities: Vec<&EntityNode> = result
            .forms
            .get(i)
            .map(|f| f.entities.iter().collect())
            .unwrap_or_default();
        form_topics(attrs, form, &form.etype, &entities, &mut out);
        if out.catch_all {
            return QueryTopics::catch_all();
        }
    }
    out
}

fn form_topics(
    attrs: &AttrMap,
    form: &Form,
    etype: &str,
    entities: &[&EntityNode],
    out: &mut QueryTopics,
) {
    let Some(id_attr) = attrs.id_attr_of(etype) else {
        // missing etype: legacy emits the catch-all topics (datalog.clj:2899)
        out.catch_all = true;
        return;
    };
    // membership: entities of this etype appearing/disappearing
    out.push(Part::Any, [id_attr.id], Part::Any);
    if let Some(order) = &form.opts.order {
        if order.key != "serverCreatedAt" {
            match attrs.by_fwd_name(etype, &order.key) {
                Some(a) => out.push(Part::Any, [a.id], Part::Any),
                None => {
                    out.catch_all = true;
                    return;
                }
            }
        }
    }
    if let Some(w) = &form.opts.where_conds {
        where_topics(attrs, etype, w, out);
        if out.catch_all {
            return;
        }
    }
    if form.opts.aggregate {
        // counts only depend on membership + where attrs (coarse topics)
        return;
    }
    // entity fetch: result entities × cardinality-one attrs of the etype
    // (a superset when `fields` narrows the projection — harmless)
    let eids: HashSet<Uuid> = entities.iter().map(|e| e.eid).collect();
    let ea_attrs: Vec<Uuid> = attrs
        .attrs_of_etype(etype)
        .filter(|a| a.cardinality == Cardinality::One)
        .map(|a| a.id)
        .collect();
    out.push(Part::Set(eids.clone()), ea_attrs, Part::Any);

    for child in &form.children {
        let (link, forward, child_etype) = match attrs.by_fwd_name(etype, &child.k) {
            Some(a) if a.value_type == ValueType::Ref => {
                (a, true, a.reverse_etype.clone().unwrap_or_default())
            }
            _ => match attrs.by_rev_name(etype, &child.k) {
                Some(a) => (a, false, a.etype.clone()),
                None => {
                    out.catch_all = true;
                    return;
                }
            },
        };
        if forward {
            // link triples are stored (parent, link, child-id)
            out.push(Part::Set(eids.clone()), [link.id], Part::Any);
        } else {
            // stored (child, link, parent-id): the parent is the value
            let parent_keys: HashSet<String> = eids
                .iter()
                .map(|e| value_key(&Value::String(e.to_string())))
                .collect();
            out.push(Part::Any, [link.id], Part::Set(parent_keys));
        }
        let child_entities: Vec<&EntityNode> = entities
            .iter()
            .flat_map(|p| p.children.iter().filter(|c| c.k == child.k))
            .flat_map(|c| c.entities.iter())
            .collect();
        form_topics(attrs, child, &child_etype, &child_entities, out);
        if out.catch_all {
            return;
        }
    }
}

enum Step<'a> {
    Forward(&'a Attr),
    Reverse(&'a Attr),
}

fn resolve_seg<'a>(attrs: &'a AttrMap, etype: &str, seg: &str) -> Option<Step<'a>> {
    if seg == "$entityIdStartsWith" {
        return attrs.id_attr_of(etype).map(Step::Forward);
    }
    if let Some(a) = attrs.by_fwd_name(etype, seg) {
        return Some(Step::Forward(a));
    }
    attrs.by_rev_name(etype, seg).map(Step::Reverse)
}

fn where_topics(attrs: &AttrMap, etype: &str, cond: &WhereCond, out: &mut QueryTopics) {
    match cond {
        WhereCond::And(cs) | WhereCond::Or(cs) => {
            for c in cs {
                where_topics(attrs, etype, c, out);
                if out.catch_all {
                    return;
                }
            }
        }
        WhereCond::Cond { path, op } => {
            let mut cur = etype.to_string();
            let n = path.len();
            for (i, seg) in path.iter().enumerate() {
                let Some(step) = resolve_seg(attrs, &cur, seg) else {
                    out.catch_all = true;
                    return;
                };
                let (attr, next) = match step {
                    Step::Forward(a) => (a, a.reverse_etype.clone().unwrap_or_default()),
                    Step::Reverse(a) => (a, a.etype.clone()),
                };
                let v = if i + 1 == n {
                    value_part(attr, op)
                } else {
                    Part::Any
                };
                out.push(Part::Any, [attr.id], v);
                cur = next;
            }
        }
    }
}

/// v-part for a where operator: exact string/boolean equality narrows to the
/// value set; everything else (comparators, like, not, null checks, numbers
/// and dates whose stored encoding may differ from the query literal) stays
/// wildcard.
fn value_part(attr: &Attr, op: &WhereOp) -> Part<String> {
    let keys: Option<Vec<String>> = match op {
        WhereOp::Eq(v) => literal_key(attr, v).map(|k| vec![k]),
        WhereOp::In(vs) => vs.iter().map(|v| literal_key(attr, v)).collect(),
        _ => None,
    };
    match keys {
        Some(keys) => Part::Set(keys.into_iter().collect()),
        None => Part::Any,
    }
}

fn literal_key(attr: &Attr, v: &Value) -> Option<String> {
    match (v, attr.checked_data_type) {
        (Value::String(_), None | Some(CheckedDataType::String)) => Some(value_key(v)),
        (Value::Bool(_), None | Some(CheckedDataType::Boolean)) => Some(value_key(v)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instaql::{parse_query, ChildResult, FormOut};
    use serde_json::json;

    fn attr(etype: &str, label: &str, value_type: ValueType, rev: Option<(&str, &str)>) -> Attr {
        Attr {
            id: Uuid::new_v4(),
            value_type,
            cardinality: Cardinality::One,
            forward_ident: Uuid::new_v4(),
            etype: etype.into(),
            label: label.into(),
            reverse_ident: rev.map(|_| Uuid::new_v4()),
            reverse_etype: rev.map(|(e, _)| e.to_string()),
            reverse_label: rev.map(|(_, l)| l.to_string()),
            is_unique: label == "id",
            is_indexed: false,
            is_required: false,
            checked_data_type: None,
            on_delete_cascade: false,
            on_delete_reverse_cascade: false,
            is_system: false,
            indexing: false,
            checking_data_type: false,
            setting_unique: false,
        }
    }

    struct Fx {
        attrs: AttrMap,
        todos_id: Uuid,
        todos_title: Uuid,
        todos_done: Uuid,
        todos_owner: Uuid,
        users_name: Uuid,
    }

    fn fixture() -> Fx {
        let mut attrs = AttrMap::default();
        let a_id = attr("todos", "id", ValueType::Blob, None);
        let a_title = attr("todos", "title", ValueType::Blob, None);
        let a_done = attr("todos", "done", ValueType::Blob, None);
        let a_owner = attr("todos", "owner", ValueType::Ref, Some(("users", "todos")));
        let u_id = attr("users", "id", ValueType::Blob, None);
        let u_name = attr("users", "name", ValueType::Blob, None);
        let fx = Fx {
            todos_id: a_id.id,
            todos_title: a_title.id,
            todos_done: a_done.id,
            todos_owner: a_owner.id,
            users_name: u_name.id,
            attrs: AttrMap::default(),
        };
        for a in [a_id, a_title, a_done, a_owner, u_id, u_name] {
            attrs.insert(a);
        }
        Fx { attrs, ..fx }
    }

    fn node(eid: Uuid, etype: &str) -> EntityNode {
        EntityNode {
            eid,
            etype: etype.into(),
            triples: vec![],
            children: vec![],
        }
    }

    fn result(forms: Vec<(&str, Vec<EntityNode>)>) -> QueryResult {
        QueryResult {
            forms: forms
                .into_iter()
                .map(|(k, entities)| FormOut {
                    k: k.into(),
                    etype: k.into(),
                    entities,
                    page_info: None,
                    aggregate: None,
                })
                .collect(),
        }
    }

    fn tx(changes: &[(Uuid, Uuid, Value)]) -> TxTopics {
        TxTopics::from_changes(changes.iter().map(|(e, a, v)| TxChange {
            e: *e,
            a: *a,
            v: v.clone(),
        }))
    }

    #[test]
    fn unfiltered_query_refreshes_on_membership_and_result_writes_only() {
        let fx = fixture();
        let e1 = Uuid::new_v4();
        let forms = parse_query(&json!({"todos": {}})).unwrap();
        let qt = query_topics(
            &fx.attrs,
            &forms,
            &result(vec![("todos", vec![node(e1, "todos")])]),
        );
        assert!(!qt.catch_all);
        // new todo created
        let e2 = Uuid::new_v4();
        assert!(tx(&[(e2, fx.todos_id, json!(e2.to_string()))]).matches(&qt));
        // result entity edited
        assert!(tx(&[(e1, fx.todos_title, json!("x"))]).matches(&qt));
        // a non-result todo's title changed: not visible (it isn't in the result)
        // — only possible with perms filtering; membership is via the id attr
        assert!(!tx(&[(e2, fx.todos_title, json!("x"))]).matches(&qt));
        // writes to another etype never match
        assert!(!tx(&[(e2, fx.users_name, json!("bob"))]).matches(&qt));
        // empty tx never matches; catch-all tx always does
        assert!(!tx(&[]).matches(&qt));
        assert!(TxTopics::catch_all().matches(&qt));
    }

    #[test]
    fn where_eq_narrows_by_value() {
        let fx = fixture();
        let e1 = Uuid::new_v4();
        let e2 = Uuid::new_v4();
        let forms = parse_query(&json!({"todos": {"$": {"where": {"title": "a"}}}})).unwrap();
        let qt = query_topics(
            &fx.attrs,
            &forms,
            &result(vec![("todos", vec![node(e1, "todos")])]),
        );
        // another todo gets title "b": irrelevant
        assert!(!tx(&[(e2, fx.todos_title, json!("b"))]).matches(&qt));
        // another todo gets title "a": now matches the where
        assert!(tx(&[(e2, fx.todos_title, json!("a"))]).matches(&qt));
        // the result entity changes any attr
        assert!(tx(&[(e1, fx.todos_done, json!(true))]).matches(&qt));
        // comparators stay attr-level
        let forms =
            parse_query(&json!({"todos": {"$": {"where": {"title": {"$like": "a%"}}}}})).unwrap();
        let qt = query_topics(&fx.attrs, &forms, &result(vec![("todos", vec![])]));
        assert!(tx(&[(e2, fx.todos_title, json!("zzz"))]).matches(&qt));
    }

    #[test]
    fn links_and_children() {
        let fx = fixture();
        let t1 = Uuid::new_v4();
        let u1 = Uuid::new_v4();
        let forms = parse_query(&json!({"todos": {"owner": {}}})).unwrap();
        let mut parent = node(t1, "todos");
        parent.children.push(ChildResult {
            k: "owner".into(),
            etype: "users".into(),
            link_triples: vec![],
            entities: vec![node(u1, "users")],
        });
        let qt = query_topics(&fx.attrs, &forms, &result(vec![("todos", vec![parent])]));
        // owner renamed -> child fetch topic
        assert!(tx(&[(u1, fx.users_name, json!("new"))]).matches(&qt));
        // some other user renamed -> no
        assert!(!tx(&[(Uuid::new_v4(), fx.users_name, json!("new"))]).matches(&qt));
        // link changed on the result todo -> yes
        assert!(tx(&[(t1, fx.todos_owner, json!(u1.to_string()))]).matches(&qt));
        // link changed on an unrelated todo -> no
        assert!(!tx(&[(Uuid::new_v4(), fx.todos_owner, json!(u1.to_string()))]).matches(&qt));

        // reverse direction: users { todos {} } — link triple value is the parent
        let forms = parse_query(&json!({"users": {"todos": {}}})).unwrap();
        let mut user = node(u1, "users");
        user.children.push(ChildResult {
            k: "todos".into(),
            etype: "todos".into(),
            link_triples: vec![],
            entities: vec![],
        });
        let qt = query_topics(&fx.attrs, &forms, &result(vec![("users", vec![user])]));
        assert!(tx(&[(
            Uuid::new_v4(),
            fx.todos_owner,
            json!(u1.to_string().to_uppercase())
        )])
        .matches(&qt));
        assert!(!tx(&[(
            Uuid::new_v4(),
            fx.todos_owner,
            json!(Uuid::new_v4().to_string())
        )])
        .matches(&qt));
        // where over a link path narrows on the terminal attr
        let forms =
            parse_query(&json!({"todos": {"$": {"where": {"owner.name": "bob"}}}})).unwrap();
        let qt = query_topics(&fx.attrs, &forms, &result(vec![("todos", vec![])]));
        assert!(tx(&[(u1, fx.users_name, json!("bob"))]).matches(&qt));
        assert!(!tx(&[(u1, fx.users_name, json!("alice"))]).matches(&qt));
        assert!(tx(&[(t1, fx.todos_owner, json!(u1.to_string()))]).matches(&qt));
    }

    #[test]
    fn unresolvable_shapes_are_catch_all() {
        let fx = fixture();
        let forms = parse_query(&json!({"nope": {}})).unwrap();
        let qt = query_topics(&fx.attrs, &forms, &result(vec![("nope", vec![])]));
        assert!(qt.catch_all);
        assert!(tx(&[]).matches(&qt));
        let forms = parse_query(&json!({"todos": {"$": {"where": {"missing": 1}}}})).unwrap();
        let qt = query_topics(&fx.attrs, &forms, &result(vec![("todos", vec![])]));
        assert!(qt.catch_all);
        let forms = parse_query(&json!({"todos": {"ghost": {}}})).unwrap();
        let qt = query_topics(&fx.attrs, &forms, &result(vec![("todos", vec![])]));
        assert!(qt.catch_all);
    }

    #[test]
    fn value_keys_normalize_uuids() {
        let u = Uuid::new_v4();
        assert_eq!(
            value_key(&json!(u.to_string().to_uppercase())),
            value_key(&json!(u.to_string()))
        );
        assert_ne!(value_key(&json!("true")), value_key(&json!(true)));
    }
}
