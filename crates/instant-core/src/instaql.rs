//! InstaQL query engine over the triples schema.
//! Port of LEGACY db/instaql.clj + db/datalog.clj observable behavior
//! (see docs/QUERY.md). Produces per-entity node trees; the ws layer flattens
//! them to join-rows, the admin layer builds object trees.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::{json, Map, Value};
use sqlx::{PgConnection, Postgres, QueryBuilder, Row};
use uuid::Uuid;

use crate::attr::{Attr, AttrMap, CheckedDataType, ValueType};
use crate::error::{InstantError, Result};

// ---------------------------------------------------------------------------
// Query AST

#[derive(Debug, Clone)]
pub enum WhereCond {
    And(Vec<WhereCond>),
    Or(Vec<WhereCond>),
    Cond { path: Vec<String>, op: WhereOp },
}

#[derive(Debug, Clone)]
pub enum WhereOp {
    Eq(Value),
    In(Vec<Value>),
    /// $not / $ne — matches rows != v plus missing/null (expanded at build)
    Not(Value),
    IsNull(bool),
    Cmp(&'static str, Value), // $gt $gte $lt $lte
    Like(String, bool),       // pattern, case-insensitive?
    EntityIdStartsWith(String),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Dir {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub struct OrderSpec {
    pub key: String, // "serverCreatedAt" or attr label
    pub dir: Dir,
}

#[derive(Debug, Clone, Default)]
pub struct Opts {
    pub where_conds: Option<WhereCond>,
    pub order: Option<OrderSpec>,
    pub limit: Option<i64>,
    pub first: Option<i64>,
    pub last: Option<i64>,
    pub offset: Option<i64>,
    pub before: Option<Cursor>,
    pub after: Option<Cursor>,
    pub before_inclusive: bool,
    pub after_inclusive: bool,
    pub aggregate: bool,
    pub fields: Option<Vec<String>>,
}

impl Opts {
    pub fn is_paginated(&self) -> bool {
        self.limit.is_some()
            || self.first.is_some()
            || self.last.is_some()
            || self.offset.is_some()
            || self.before.is_some()
            || self.after.is_some()
            || self.order.is_some()
    }
    pub fn effective_limit(&self) -> Option<i64> {
        self.limit.or(self.first).or(self.last)
    }
}

#[derive(Debug, Clone)]
pub struct Cursor {
    pub e: Option<Uuid>,
    pub a: Uuid,
    pub v: Value,
    pub t: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct Form {
    pub k: String,
    pub etype: String,
    pub opts: Opts,
    pub children: Vec<Form>,
}

// ---------------------------------------------------------------------------
// Results

#[derive(Debug, Clone, PartialEq)]
pub struct TripleOut {
    pub e: Uuid,
    pub a: Uuid,
    pub v: Value,
    pub t: i64,
}

impl TripleOut {
    pub fn to_json(&self) -> Value {
        json!([self.e, self.a, self.v, self.t])
    }
}

#[derive(Debug, Clone)]
pub struct EntityNode {
    pub eid: Uuid,
    pub etype: String,
    /// cardinality-one triples of the entity (the "ea fetch")
    pub triples: Vec<TripleOut>,
    /// child form key -> per-child result
    pub children: Vec<ChildResult>,
}

#[derive(Debug, Clone)]
pub struct ChildResult {
    pub k: String,
    pub etype: String,
    /// link triples connecting this parent to the children (stored orientation)
    pub link_triples: Vec<TripleOut>,
    pub entities: Vec<EntityNode>,
}

#[derive(Debug, Clone)]
pub struct PageInfoOut {
    pub start_cursor: Option<Value>,
    pub end_cursor: Option<Value>,
    pub has_next_page: bool,
    pub has_previous_page: bool,
}

#[derive(Debug, Clone)]
pub struct FormOut {
    pub k: String,
    pub etype: String,
    pub entities: Vec<EntityNode>,
    pub page_info: Option<PageInfoOut>,
    pub aggregate: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub forms: Vec<FormOut>,
}

impl QueryResult {
    /// Flattened ws-protocol result: one node with all triples in one
    /// join-row, page-info/aggregate keyed by top-level form key.
    pub fn to_ws_result(&self) -> Value {
        let mut triples: Vec<Value> = vec![];
        let mut seen: HashSet<(Uuid, Uuid, String)> = HashSet::new();
        fn walk(
            node: &EntityNode,
            triples: &mut Vec<Value>,
            seen: &mut HashSet<(Uuid, Uuid, String)>,
        ) {
            for t in &node.triples {
                if seen.insert((t.e, t.a, t.v.to_string())) {
                    triples.push(t.to_json());
                }
            }
            for c in &node.children {
                for t in &c.link_triples {
                    if seen.insert((t.e, t.a, t.v.to_string())) {
                        triples.push(t.to_json());
                    }
                }
                for e in &c.entities {
                    walk(e, triples, seen);
                }
            }
        }
        let mut page_info = Map::new();
        let mut aggregate = Map::new();
        for form in &self.forms {
            for e in &form.entities {
                walk(e, &mut triples, &mut seen);
            }
            if let Some(pi) = &form.page_info {
                page_info.insert(
                    form.k.clone(),
                    json!({
                        "start-cursor": pi.start_cursor,
                        "end-cursor": pi.end_cursor,
                        "has-next-page?": pi.has_next_page,
                        "has-previous-page?": pi.has_previous_page,
                    }),
                );
            }
            if let Some(count) = form.aggregate {
                aggregate.insert(form.k.clone(), json!({"count": count}));
            }
        }
        let mut data = Map::new();
        data.insert("datalog-result".into(), json!({"join-rows": [triples]}));
        if !page_info.is_empty() {
            data.insert("page-info".into(), Value::Object(page_info));
        }
        if !aggregate.is_empty() {
            data.insert("aggregate".into(), Value::Object(aggregate));
        }
        json!([{"data": Value::Object(data), "child-nodes": []}])
    }
}

// ---------------------------------------------------------------------------
// Parsing

fn verr(message: impl Into<String>) -> InstantError {
    InstantError::validation_failed("query", message, json!([]))
}

pub fn parse_query(q: &Value) -> Result<Vec<Form>> {
    let obj = q
        .as_object()
        .ok_or_else(|| verr("InstaQL queries must be objects."))?;
    let mut forms = vec![];
    for (k, v) in obj {
        if k == "$$ruleParams" {
            continue;
        }
        forms.push(parse_form(k, k, v, 0)?);
    }
    Ok(forms)
}

fn parse_form(k: &str, etype: &str, v: &Value, level: usize) -> Result<Form> {
    let obj = v
        .as_object()
        .ok_or_else(|| verr(format!("Expected an object for `{k}`.")))?;
    let mut opts = Opts::default();
    let mut children = vec![];
    for (ck, cv) in obj {
        if ck == "$" {
            opts = parse_opts(cv, level)?;
        } else {
            // etype is resolved later against attrs; store the key for now
            children.push(parse_form(ck, ck, cv, level + 1)?);
        }
    }
    if opts.aggregate && !children.is_empty() {
        return Err(verr(
            "You can not combine aggregates with child queries at this time.",
        ));
    }
    Ok(Form {
        k: k.to_string(),
        etype: etype.to_string(),
        opts,
        children,
    })
}

fn parse_opts(v: &Value, level: usize) -> Result<Opts> {
    let obj = v
        .as_object()
        .ok_or_else(|| verr("`$` must be an object."))?;
    let mut opts = Opts::default();
    for (k, v) in obj {
        match k.as_str() {
            "where" => opts.where_conds = Some(parse_where(v)?),
            "order" => {
                let m = v.as_object().ok_or_else(|| verr("`order` must be an object."))?;
                if m.is_empty() {
                    // legacy `(case (count order-map) 0 nil ...)`
                    continue;
                }
                if m.len() != 1 {
                    return Err(verr("`order` must have exactly one key."));
                }
                let (key, dir) = m.iter().next().unwrap();
                let dir = match dir.as_str() {
                    Some("asc") => Dir::Asc,
                    Some("desc") => Dir::Desc,
                    _ => return Err(verr("order direction must be \"asc\" or \"desc\".")),
                };
                opts.order = Some(OrderSpec { key: key.clone(), dir });
            }
            "limit" => opts.limit = Some(pos_int(v, "limit")?),
            "first" => opts.first = Some(pos_int(v, "first")?),
            "last" => opts.last = Some(pos_int(v, "last")?),
            "offset" => {
                let n = v.as_i64().filter(|n| *n >= 0).ok_or_else(|| {
                    verr("`offset` must be a non-negative integer.")
                })?;
                opts.offset = Some(n);
            }
            "before" => opts.before = Some(parse_cursor(v)?),
            "after" => opts.after = Some(parse_cursor(v)?),
            "beforeInclusive" => {
                opts.before_inclusive = v.as_bool().unwrap_or(false);
            }
            "afterInclusive" => {
                opts.after_inclusive = v.as_bool().unwrap_or(false);
            }
            "aggregate" => {
                if v.as_str() != Some("count") {
                    return Err(verr("only `count` aggregates are supported."));
                }
                opts.aggregate = true;
            }
            "fields" => {
                let arr = v.as_array().ok_or_else(|| verr("`fields` must be an array."))?;
                let mut fields = vec![];
                for f in arr {
                    fields.push(
                        f.as_str()
                            .ok_or_else(|| verr("`fields` must be strings."))?
                            .to_string(),
                    );
                }
                opts.fields = Some(fields);
            }
            other => {
                return Err(verr(format!(
                    "We only support `where`, `order`, `limit`, `offset`, `before`, and `after` clauses. Got `{other}`."
                )))
            }
        }
    }
    if [opts.limit, opts.first, opts.last]
        .iter()
        .filter(|x| x.is_some())
        .count()
        > 1
    {
        return Err(verr(
            "Only one of `limit`, `first`, or `last` can be provided.",
        ));
    }
    if level > 0 && (opts.offset.is_some() || opts.before.is_some() || opts.after.is_some()) {
        return Err(verr(
            "We currently only support `offset`, `before`, and `after` clauses on the top-level field.",
        ));
    }
    Ok(opts)
}

fn pos_int(v: &Value, name: &str) -> Result<i64> {
    v.as_i64()
        .filter(|n| *n > 0)
        .ok_or_else(|| verr(format!("`{name}` must be a positive integer.")))
}

fn parse_cursor(v: &Value) -> Result<Cursor> {
    let arr = v
        .as_array()
        .filter(|a| a.len() == 3 || a.len() == 4)
        .ok_or_else(|| verr("cursors must be [e, a, v, t] join rows."))?;
    let a = arr[1]
        .as_str()
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| verr("cursor attr must be a uuid."))?;
    let e = arr[0].as_str().and_then(|s| Uuid::parse_str(s).ok());
    let t = arr.get(3).and_then(|t| t.as_i64());
    Ok(Cursor {
        e,
        a,
        v: arr[2].clone(),
        t,
    })
}

fn parse_where(v: &Value) -> Result<WhereCond> {
    let obj = v
        .as_object()
        .ok_or_else(|| verr("`where` must be an object."))?;
    let mut conds = vec![];
    for (k, val) in obj {
        match k.as_str() {
            "or" => {
                let arr = val
                    .as_array()
                    .ok_or_else(|| verr("`or` must be an array."))?;
                if arr.is_empty() {
                    return Err(verr("The `or` operation expects a non-empty list."));
                }
                conds.push(WhereCond::Or(
                    arr.iter().map(parse_where).collect::<Result<Vec<_>>>()?,
                ));
            }
            "and" => {
                let arr = val
                    .as_array()
                    .ok_or_else(|| verr("`and` must be an array."))?;
                if arr.is_empty() {
                    return Err(verr("The `and` operation expects a non-empty list."));
                }
                conds.push(WhereCond::And(
                    arr.iter().map(parse_where).collect::<Result<Vec<_>>>()?,
                ));
            }
            path => {
                let segs: Vec<String> = path.split('.').map(|s| s.to_string()).collect();
                let ops = parse_where_value(val)?;
                for op in ops {
                    conds.push(WhereCond::Cond {
                        path: segs.clone(),
                        op,
                    });
                }
            }
        }
    }
    Ok(if conds.len() == 1 {
        conds.pop().unwrap()
    } else {
        WhereCond::And(conds)
    })
}

fn parse_where_value(v: &Value) -> Result<Vec<WhereOp>> {
    match v {
        Value::Object(m) => {
            let mut ops = vec![];
            for (k, val) in m {
                let op = match k.as_str() {
                    "$in" | "in" => WhereOp::In(
                        val.as_array()
                            .ok_or_else(|| verr("`$in` expects an array."))?
                            .clone(),
                    ),
                    "$not" | "$ne" => WhereOp::Not(val.clone()),
                    "$isNull" => WhereOp::IsNull(
                        val.as_bool()
                            .ok_or_else(|| verr("`$isNull` expects a boolean."))?,
                    ),
                    "$gt" => WhereOp::Cmp(">", val.clone()),
                    "$gte" => WhereOp::Cmp(">=", val.clone()),
                    "$lt" => WhereOp::Cmp("<", val.clone()),
                    "$lte" => WhereOp::Cmp("<=", val.clone()),
                    "$like" | "$ilike" => WhereOp::Like(
                        val.as_str()
                            .ok_or_else(|| {
                                verr(format!(
                                    "The {} value must be a string, but the query got the value `{}` of type `{}`.",
                                    k, val, json_type_name(val)
                                ))
                            })?
                            .to_string(),
                        k == "$ilike",
                    ),
                    other => return Err(verr(format!("Unsupported where operator `{other}`."))),
                };
                ops.push(op);
            }
            if ops.is_empty() {
                return Err(verr("Empty where args map."));
            }
            // legacy `(let [[func args-map-val] (first v-value)] ...)`
            // (instaql.clj:669-675): only the first operator of an args map
            // is applied; the rest are dropped
            ops.truncate(1);
            Ok(ops)
        }
        Value::String(_) | Value::Number(_) | Value::Bool(_) => Ok(vec![WhereOp::Eq(v.clone())]),
        Value::Null => Ok(vec![WhereOp::IsNull(true)]),
        _ => Err(verr("Invalid where value.")),
    }
}

// ---------------------------------------------------------------------------
// SQL building

struct SqlCtx<'a> {
    app_id: Uuid,
    attrs: &'a AttrMap,
}

#[derive(Debug)]
struct MissingAttr;

enum PathStep<'a> {
    Forward(&'a Attr),
    Reverse(&'a Attr),
}

/// Resolve one path segment on an etype: forward attr, or reverse link.
fn resolve_seg<'a>(
    attrs: &'a AttrMap,
    etype: &str,
    seg: &str,
) -> std::result::Result<PathStep<'a>, MissingAttr> {
    if seg == "$entityIdStartsWith" {
        // handled by the caller; treat as forward id attr
        return attrs
            .id_attr_of(etype)
            .map(PathStep::Forward)
            .ok_or(MissingAttr);
    }
    if let Some(a) = attrs.by_fwd_name(etype, seg) {
        return Ok(PathStep::Forward(a));
    }
    if let Some(a) = attrs.by_rev_name(etype, seg) {
        return Ok(PathStep::Reverse(a));
    }
    Err(MissingAttr)
}

fn next_etype(step: &PathStep) -> String {
    match step {
        PathStep::Forward(a) => a.reverse_etype.clone().unwrap_or_default(),
        PathStep::Reverse(a) => a.etype.clone(),
    }
}

fn extract_fn(t: CheckedDataType) -> &'static str {
    match t {
        CheckedDataType::String => "triples_extract_string_value",
        CheckedDataType::Number => "triples_extract_number_value",
        CheckedDataType::Boolean => "triples_extract_boolean_value",
        CheckedDataType::Date => "triples_extract_date_value",
    }
}

/// Coerce a query value for a typed comparison; returns the SQL-bindable text
/// and validates types roughly like attr_pat.clj.
/// Legacy `json-type-of-clj`.
fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Legacy `throw-invalid-data-value!` / `throw-invalid-date-string!`
/// (attr_pat.clj:228-262): the value's type must match the attr's checked
/// type. `op` distinguishes the equality path (`coerce-value-data-value!`,
/// which refuses the relative date keywords) from the comparison path
/// (`coerced-type-comparison-value!`, which parses them).
fn coerce_typed(attr: &Attr, t: CheckedDataType, v: &Value, op: &str) -> Result<Value> {
    let bad = || {
        verr(format!(
            "The data type of `{}.{}` is `{}`, but the query got the value `{}` of type `{}`.",
            attr.etype,
            attr.label,
            t.as_str(),
            v,
            json_type_name(v)
        ))
    };
    let bad_date = || {
        verr(format!(
            "The data type of `{}.{}` is `date`, but the query got value `{}` of type `{}`.",
            attr.etype,
            attr.label,
            v,
            json_type_name(v)
        ))
    };
    match t {
        CheckedDataType::Number => {
            if !v.is_number() {
                return Err(bad());
            }
            Ok(v.clone())
        }
        CheckedDataType::Boolean => {
            if !v.is_boolean() {
                return Err(bad());
            }
            Ok(v.clone())
        }
        CheckedDataType::String => {
            if !v.is_string() {
                return Err(bad());
            }
            Ok(v.clone())
        }
        CheckedDataType::Date => match v {
            Value::Number(_) => Ok(v.clone()),
            Value::String(s) => {
                // equality refuses the relative keywords legacy can parse but
                // the client can't agree on (attr_pat.clj:286-293); comparison
                // parses them (verified live: `{$gt: "now"}` is a valid
                // query). Anything the parser rejects is a 400 either way.
                let keyword = matches!(s.trim(), "now" | "today" | "tomorrow" | "yesterday");
                if s.trim().is_empty()
                    || (keyword && matches!(op, "$eq" | "$not"))
                    || (!keyword && !looks_like_date(s))
                {
                    return Err(bad_date());
                }
                Ok(v.clone())
            }
            _ => Err(bad_date()),
        },
    }
}

/// Cheap pre-check standing in for legacy `parse-date-value`
/// (triple.clj:1630-1640), which accepts a wide family of formats (ISO,
/// RFC 2822, `M/D/YYYY`, `Date.toString()`, quoted JSON strings, ...).
/// Postgres does the real parse; this only keeps digit-less garbage like
/// `not-a-date` from reaching it as a 500. Postgres' own special strings
/// (`epoch`, `infinity`, `allballs`) stay accepted.
fn looks_like_date(s: &str) -> bool {
    let s = s.trim().trim_matches('"');
    s.chars().any(|c| c.is_ascii_digit())
        || matches!(s, "epoch" | "infinity" | "-infinity" | "allballs")
}

/// Push SQL for a date/number/boolean/string extract-value expression of `v`.
fn push_typed_value(qb: &mut QueryBuilder<Postgres>, t: CheckedDataType, v: &Value) {
    match t {
        CheckedDataType::Number => {
            qb.push_bind(v.as_f64().unwrap_or(0.0));
        }
        CheckedDataType::Boolean => {
            qb.push_bind(v.as_bool().unwrap_or(false));
        }
        CheckedDataType::String => {
            qb.push_bind(v.as_str().unwrap_or("").to_string());
        }
        CheckedDataType::Date => match v {
            Value::Number(n) => {
                qb.push("to_timestamp(");
                qb.push_bind(n.as_f64().unwrap_or(0.0));
                qb.push(" / 1000.0)");
            }
            _ => {
                qb.push("(");
                qb.push_bind(v.as_str().unwrap_or("").to_string());
                qb.push(")::timestamptz");
            }
        },
    }
}

impl<'a> SqlCtx<'a> {
    /// Push SQL boolean expression testing `cond` for the entity id expression
    /// `ent` (SQL text) of type `etype`.
    fn push_cond(
        &self,
        qb: &mut QueryBuilder<Postgres>,
        etype: &str,
        ent: &str,
        cond: &WhereCond,
        depth: usize,
    ) -> std::result::Result<Result<()>, MissingAttr> {
        match cond {
            WhereCond::And(cs) => {
                qb.push("(");
                for (i, c) in cs.iter().enumerate() {
                    if i > 0 {
                        qb.push(" AND ");
                    }
                    match self.push_cond(qb, etype, ent, c, depth) {
                        Ok(Ok(())) => {}
                        other => return other,
                    }
                }
                if cs.is_empty() {
                    qb.push("TRUE");
                }
                qb.push(")");
                Ok(Ok(()))
            }
            WhereCond::Or(cs) => {
                qb.push("(");
                for (i, c) in cs.iter().enumerate() {
                    if i > 0 {
                        qb.push(" OR ");
                    }
                    match self.push_cond(qb, etype, ent, c, depth) {
                        Ok(Ok(())) => {}
                        other => return other,
                    }
                }
                if cs.is_empty() {
                    qb.push("FALSE");
                }
                qb.push(")");
                Ok(Ok(()))
            }
            WhereCond::Cond { path, op } => self.push_leaf(qb, etype, ent, path, op, depth),
        }
    }

    /// Expand $not / multi-segment $isNull like the legacy coercion, then emit.
    fn push_leaf(
        &self,
        qb: &mut QueryBuilder<Postgres>,
        etype: &str,
        ent: &str,
        path: &[String],
        op: &WhereOp,
        depth: usize,
    ) -> std::result::Result<Result<()>, MissingAttr> {
        // $entityIdStartsWith special label
        if path.len() == 1 && path[0] == "$entityIdStartsWith" {
            if let WhereOp::Eq(Value::String(prefix)) = op {
                let clean: String = prefix
                    .chars()
                    .filter(|c| c.is_ascii_hexdigit() || *c == '-')
                    .collect();
                let lo = pad_uuid(&clean, '0');
                let hi = pad_uuid(&clean, 'f');
                match (lo, hi) {
                    (Some(lo), Some(hi)) => {
                        qb.push("(");
                        qb.push(ent);
                        qb.push(" BETWEEN ");
                        qb.push_bind(lo);
                        qb.push(" AND ");
                        qb.push_bind(hi);
                        qb.push(")");
                        return Ok(Ok(()));
                    }
                    _ => return Ok(Err(verr("Invalid $entityIdStartsWith prefix."))),
                }
            }
            return Ok(Err(verr("$entityIdStartsWith expects a string.")));
        }

        match op {
            WhereOp::Not(v) => {
                // {or [[path {not-raw v}] [p1 isNull] [p1.p2 isNull] ... ]}
                qb.push("(");
                match self.push_path(qb, etype, ent, path, &LeafEmit::NotRaw(v.clone()), depth) {
                    Ok(Ok(())) => {}
                    other => return other,
                }
                // isNull prefixes: every proper prefix; the full path too unless
                // its final attr is indexed.
                let mut prefixes: Vec<Vec<String>> = vec![];
                for i in 1..path.len() {
                    prefixes.push(path[..i].to_vec());
                }
                let full_indexed = self.final_attr_indexed(etype, path);
                if !full_indexed {
                    prefixes.push(path.to_vec());
                }
                for p in prefixes {
                    qb.push(" OR ");
                    match self.push_path(qb, etype, ent, &p, &LeafEmit::IsNull(true), depth) {
                        Ok(Ok(())) => {}
                        other => return other,
                    }
                }
                qb.push(")");
                Ok(Ok(()))
            }
            WhereOp::IsNull(true) if path.len() > 1 => {
                qb.push("(");
                for i in 1..=path.len() {
                    if i > 1 {
                        qb.push(" OR ");
                    }
                    match self.push_path(qb, etype, ent, &path[..i], &LeafEmit::IsNull(true), depth)
                    {
                        Ok(Ok(())) => {}
                        other => return other,
                    }
                }
                qb.push(")");
                Ok(Ok(()))
            }
            WhereOp::IsNull(b) => {
                self.push_path(qb, etype, ent, path, &LeafEmit::IsNull(*b), depth)
            }
            WhereOp::Eq(v) => {
                self.push_path(qb, etype, ent, path, &LeafEmit::Eq(vec![v.clone()]), depth)
            }
            WhereOp::In(vs) => {
                self.push_path(qb, etype, ent, path, &LeafEmit::Eq(vs.clone()), depth)
            }
            WhereOp::Cmp(op, v) => {
                self.push_path(qb, etype, ent, path, &LeafEmit::Cmp(op, v.clone()), depth)
            }
            WhereOp::Like(p, ci) => {
                self.push_path(qb, etype, ent, path, &LeafEmit::Like(p.clone(), *ci), depth)
            }
            WhereOp::EntityIdStartsWith(_) => Ok(Err(verr("unexpected"))),
        }
    }

    fn final_attr_indexed(&self, etype: &str, path: &[String]) -> bool {
        let mut etype = etype.to_string();
        for (i, seg) in path.iter().enumerate() {
            match resolve_seg(self.attrs, &etype, seg) {
                Ok(step) => {
                    if i == path.len() - 1 {
                        return match step {
                            PathStep::Forward(a) => a.indexed_for_query(),
                            PathStep::Reverse(_) => false,
                        };
                    }
                    etype = next_etype(&step);
                }
                Err(_) => return false,
            }
        }
        false
    }

    /// Walk `path` from `ent` emitting nested EXISTS; the innermost emit
    /// compares the final attr.
    fn push_path(
        &self,
        qb: &mut QueryBuilder<Postgres>,
        etype: &str,
        ent: &str,
        path: &[String],
        emit: &LeafEmit,
        depth: usize,
    ) -> std::result::Result<Result<()>, MissingAttr> {
        let seg = &path[0];
        let step = resolve_seg(self.attrs, etype, seg)?;
        let alias = format!("w{depth}");
        if path.len() == 1 {
            // Final segment
            match step {
                PathStep::Forward(attr) if attr.value_type == ValueType::Blob => {
                    return Ok(self.push_blob_leaf(qb, attr, ent, emit, &alias));
                }
                // Link leaf: compare linked entity ids
                step => return Ok(self.push_link_leaf(qb, &step, ent, emit, &alias)),
            }
        }
        // Traverse a link hop; the rest of the path applies to the next entity.
        let (attr, next_ent_expr, anchor) = match &step {
            PathStep::Forward(a) => {
                if a.value_type != ValueType::Ref {
                    return Ok(Err(verr(format!(
                        "`{}` on `{}` is not a link, but the query tries to traverse it.",
                        seg, etype
                    ))));
                }
                (
                    *a,
                    format!("json_uuid_to_uuid({alias}.value)"),
                    format!("{alias}.entity_id = {ent} AND {alias}.eav"),
                )
            }
            PathStep::Reverse(a) => (
                *a,
                format!("{alias}.entity_id"),
                format!("json_uuid_to_uuid({alias}.value) = {ent} AND {alias}.vae"),
            ),
        };
        let netype = next_etype(&step);
        qb.push("EXISTS (SELECT 1 FROM triples ");
        qb.push(alias.clone());
        qb.push(" WHERE ");
        qb.push(alias.clone());
        qb.push(".app_id = ");
        qb.push_bind(self.app_id);
        qb.push(" AND ");
        qb.push(alias.clone());
        qb.push(".attr_id = ");
        qb.push_bind(attr.id);
        qb.push(" AND ");
        qb.push(anchor);
        qb.push(" AND ");
        match self.push_path(qb, &netype, &next_ent_expr, &path[1..], emit, depth + 1) {
            Ok(Ok(())) => {}
            other => return other,
        }
        qb.push(")");
        Ok(Ok(()))
    }

    fn push_blob_leaf(
        &self,
        qb: &mut QueryBuilder<Postgres>,
        attr: &Attr,
        ent: &str,
        emit: &LeafEmit,
        alias: &str,
    ) -> Result<()> {
        // legacy best-index: the typed `ave` path needs index? AND
        // checked-data-type with neither job still in flight (a running
        // index / check-data-type job has only flagged part of the triples)
        let typed = if attr.indexed_for_query() {
            attr.checked_type_for_query()
        } else {
            None
        };
        // legacy coerces against the checked type whether or not the attr is
        // indexed (attr_pat.clj:366-372: the coerced value is only *used*
        // when indexed, but a mismatch always throws)
        if let Some(t) = attr.checked_type_for_query() {
            match emit {
                LeafEmit::Eq(vals) => {
                    for v in vals.iter().filter(|v| !v.is_null()) {
                        coerce_typed(attr, t, v, "$eq")?;
                    }
                }
                LeafEmit::NotRaw(v) if !v.is_null() => {
                    coerce_typed(attr, t, v, "$not")?;
                }
                _ => {}
            }
        }
        let base = |qb: &mut QueryBuilder<Postgres>, negate: bool| {
            if negate {
                qb.push("NOT ");
            }
            qb.push("EXISTS (SELECT 1 FROM triples ");
            qb.push(alias.to_string());
            qb.push(" WHERE ");
            qb.push(alias.to_string());
            qb.push(".app_id = ");
            qb.push_bind(self.app_id);
            qb.push(" AND ");
            qb.push(alias.to_string());
            qb.push(".attr_id = ");
            qb.push_bind(attr.id);
            qb.push(" AND ");
            qb.push(alias.to_string());
            qb.push(".entity_id = ");
            qb.push(ent.to_string());
            qb.push(" AND ");
        };
        match emit {
            LeafEmit::Eq(vals) => {
                base(qb, false);
                qb.push("(");
                let mut first = true;
                for v in vals {
                    if !first {
                        qb.push(" OR ");
                    }
                    first = false;
                    match typed {
                        Some(t) if t != CheckedDataType::String && !v.is_null() => {
                            coerce_typed(attr, t, v, "$eq")?;
                            qb.push(format!("({}({}.value) = ", extract_fn(t), alias));
                            push_typed_value(qb, t, v);
                            qb.push(format!(
                                " AND {}.checked_data_type = '{}'::checked_data_type)",
                                alias,
                                t.as_str()
                            ));
                        }
                        _ => {
                            qb.push(format!("{}.value = ", alias));
                            qb.push_bind(v.to_string());
                            qb.push("::jsonb");
                        }
                    }
                }
                if vals.is_empty() {
                    qb.push("FALSE");
                }
                qb.push("))");
                Ok(())
            }
            LeafEmit::NotRaw(v) => {
                base(qb, false);
                match typed {
                    Some(t) if t != CheckedDataType::String => {
                        qb.push(format!(
                            "{}.checked_data_type = '{}'::checked_data_type AND {}({}.value) IS DISTINCT FROM ",
                            alias,
                            t.as_str(),
                            extract_fn(t),
                            alias
                        ));
                        push_typed_value(qb, t, v);
                    }
                    _ => {
                        qb.push(format!("{}.value != ", alias));
                        qb.push_bind(v.to_string());
                        qb.push("::jsonb");
                    }
                }
                qb.push(")");
                Ok(())
            }
            LeafEmit::IsNull(is_null) => {
                base(qb, *is_null);
                qb.push(format!("{}.value != 'null'::jsonb)", alias));
                Ok(())
            }
            LeafEmit::Cmp(op, v) => {
                let t = self.require_indexed_checked(attr)?;
                let v = coerce_typed(attr, t, v, op)?;
                base(qb, false);
                qb.push(format!("{}({}.value) {} ", extract_fn(t), alias, op));
                push_typed_value(qb, t, &v);
                qb.push(format!(
                    " AND {}.checked_data_type = '{}'::checked_data_type)",
                    alias,
                    t.as_str()
                ));
                Ok(())
            }
            LeafEmit::Like(pattern, ci) => {
                let t = self.require_indexed_checked(attr)?;
                if t != CheckedDataType::String {
                    return Err(verr(format!(
                        "$like requires a string-typed attribute, but `{}.{}` is {}.",
                        attr.etype,
                        attr.label,
                        t.as_str()
                    )));
                }
                base(qb, false);
                qb.push(format!(
                    "triples_extract_string_value({}.value) {} ",
                    alias,
                    if *ci { "ILIKE" } else { "LIKE" }
                ));
                qb.push_bind(pattern.clone());
                qb.push(format!(
                    " AND {}.checked_data_type = 'string'::checked_data_type)",
                    alias
                ));
                Ok(())
            }
        }
    }

    fn require_indexed_checked(&self, attr: &Attr) -> Result<CheckedDataType> {
        // legacy assert-checked-attr-data-type! (attr_pat.clj), in its order
        if attr.checking_data_type {
            return Err(verr(format!(
                "The `{}.{}` attribute is still in the process of checking its data type. It must finish before using comparison operators.",
                attr.etype, attr.label
            )));
        }
        if attr.indexing {
            return Err(verr(format!(
                "The `{}.{}` attribute is still in the process of indexing. It must finish before using comparison operators.",
                attr.etype, attr.label
            )));
        }
        if !attr.is_indexed {
            return Err(verr(format!(
                "The `{}.{}` attribute must be indexed to use comparison operators.",
                attr.etype, attr.label
            )));
        }
        attr.checked_data_type.ok_or_else(|| {
            verr(format!(
                "The `{}.{}` attribute must have an enforced type to use comparison operators.",
                attr.etype, attr.label
            ))
        })
    }

    /// Leaf on a link label: compare linked entity ids (uuid values), or isNull.
    fn push_link_leaf(
        &self,
        qb: &mut QueryBuilder<Postgres>,
        step: &PathStep,
        ent: &str,
        emit: &LeafEmit,
        alias: &str,
    ) -> Result<()> {
        let (attr, anchor, other_expr) = match step {
            PathStep::Forward(a) => (
                *a,
                format!("{alias}.entity_id = {ent} AND {alias}.eav"),
                format!("json_uuid_to_uuid({alias}.value)"),
            ),
            PathStep::Reverse(a) => (
                *a,
                format!("json_uuid_to_uuid({alias}.value) = {ent} AND {alias}.vae"),
                format!("{alias}.entity_id"),
            ),
        };
        let base = |qb: &mut QueryBuilder<Postgres>, negate: bool| {
            if negate {
                qb.push("NOT ");
            }
            qb.push("EXISTS (SELECT 1 FROM triples ");
            qb.push(alias.to_string());
            qb.push(" WHERE ");
            qb.push(alias.to_string());
            qb.push(".app_id = ");
            qb.push_bind(self.app_id);
            qb.push(" AND ");
            qb.push(alias.to_string());
            qb.push(".attr_id = ");
            qb.push_bind(attr.id);
            qb.push(" AND ");
            qb.push(anchor.clone());
        };
        match emit {
            LeafEmit::Eq(vals) => {
                let mut uuids = vec![];
                for v in vals {
                    let u = v
                        .as_str()
                        .and_then(|s| Uuid::parse_str(s).ok())
                        .ok_or_else(|| {
                            verr(format!(
                                "Query value for link `{}` must be a uuid.",
                                attr.label
                            ))
                        })?;
                    uuids.push(u);
                }
                base(qb, false);
                qb.push(" AND ");
                qb.push(other_expr);
                qb.push(" = ANY(");
                qb.push_bind(uuids);
                qb.push("))");
                Ok(())
            }
            LeafEmit::NotRaw(v) => {
                // legacy coerce-value-uuid (attr_pat.clj:410-419)
                let u = v
                    .as_str()
                    .and_then(|s| Uuid::parse_str(s).ok())
                    .ok_or_else(|| {
                        verr(format!(
                            "Expected {} to be a uuid, got {{\"$not\":{}}}",
                            attr.label, v
                        ))
                    })?;
                base(qb, false);
                qb.push(" AND ");
                qb.push(other_expr);
                qb.push(" != ");
                qb.push_bind(u);
                qb.push(")");
                Ok(())
            }
            LeafEmit::IsNull(is_null) => {
                base(qb, *is_null);
                qb.push(")");
                Ok(())
            }
            LeafEmit::Cmp(..) | LeafEmit::Like(..) => {
                Err(verr("Comparison operators are not supported on links."))
            }
        }
    }
}

enum LeafEmit {
    Eq(Vec<Value>),
    NotRaw(Value),
    IsNull(bool),
    Cmp(&'static str, Value),
    Like(String, bool),
}

fn pad_uuid(prefix: &str, fill: char) -> Option<Uuid> {
    let hex: String = prefix.chars().filter(|c| *c != '-').collect();
    if hex.len() > 32 {
        return None;
    }
    let padded: String = hex
        .chars()
        .chain(std::iter::repeat(fill))
        .take(32)
        .collect();
    Uuid::parse_str(&padded).ok()
}

// ---------------------------------------------------------------------------
// Execution

pub struct QueryCtx<'a> {
    pub app_id: Uuid,
    pub attrs: &'a AttrMap,
    pub admin: bool,
}

pub async fn query(conn: &mut PgConnection, ctx: &QueryCtx<'_>, q: &Value) -> Result<QueryResult> {
    let forms = parse_query(q)?;
    let mut out = vec![];
    for form in &forms {
        out.push(run_top_form(conn, ctx, form).await?);
    }
    Ok(QueryResult { forms: out })
}

struct MatchedRow {
    eid: Uuid,
    order_v: Value,
    order_t: i64,
}

async fn run_top_form(conn: &mut PgConnection, ctx: &QueryCtx<'_>, form: &Form) -> Result<FormOut> {
    if form.opts.aggregate && !ctx.admin {
        return Err(verr(
            "Aggregates are currently only available for admin queries.",
        ));
    }
    let sql_ctx = SqlCtx {
        app_id: ctx.app_id,
        attrs: ctx.attrs,
    };
    let id_attr = match ctx.attrs.id_attr_of(&form.etype) {
        Some(a) => a.clone(),
        None => {
            return Ok(FormOut {
                k: form.k.clone(),
                etype: form.etype.clone(),
                entities: vec![],
                page_info: None,
                // legacy: no aggregate key for an unknown namespace
                aggregate: None,
            });
        }
    };

    if form.opts.aggregate {
        let mut qb = QueryBuilder::new("SELECT count(*) AS n FROM triples idt WHERE idt.app_id = ");
        qb.push_bind(ctx.app_id);
        qb.push(" AND idt.attr_id = ");
        qb.push_bind(id_attr.id);
        qb.push(" AND idt.ea");
        if let Some(w) = &form.opts.where_conds {
            qb.push(" AND ");
            match sql_ctx.push_cond(&mut qb, &form.etype, "idt.entity_id", w, 0) {
                Ok(r) => r?,
                Err(MissingAttr) => {
                    return Ok(FormOut {
                        k: form.k.clone(),
                        etype: form.etype.clone(),
                        entities: vec![],
                        page_info: None,
                        aggregate: Some(0),
                    })
                }
            }
        }
        let row = qb.build().fetch_one(&mut *conn).await?;
        let n: i64 = row.get("n");
        return Ok(FormOut {
            k: form.k.clone(),
            etype: form.etype.clone(),
            entities: vec![],
            page_info: None,
            aggregate: Some(n),
        });
    }

    // ---- matching + ordering + pagination ----
    let paginated = form.opts.is_paginated();
    let order = form.opts.order.clone().unwrap_or(OrderSpec {
        key: "serverCreatedAt".to_string(),
        dir: Dir::Asc,
    });

    // Resolve order attr
    let (order_attr, order_type): (Attr, Option<CheckedDataType>) = if order.key
        == "serverCreatedAt"
    {
        (id_attr.clone(), None)
    } else {
        let a = ctx
            .attrs
            .by_fwd_name(&form.etype, &order.key)
            .cloned()
            .ok_or_else(|| {
                // legacy message (LEGACY instaql.clj:945-949)
                verr(format!(
                    "There is no `{}` attribute for {}.",
                    order.key, form.etype
                ))
            })?;
        // legacy per-condition messages (LEGACY instaql.clj:951-969)
        let name = format!("{}.{}", form.etype, order.key);
        let mut order_errors: Vec<String> = vec![];
        if a.checking_data_type {
            order_errors.push(format!(
                    "The `{name}` attribute is still in the process of validating its type. It must finish before ordering by the attribute."
                ));
        }
        if a.indexing {
            order_errors.push(format!(
                    "The `{name}` attribute is still in the process of indexing. It must finish before ordering by the attribute."
                ));
        }
        if !a.is_indexed {
            order_errors.push(format!(
                    "The `{name}` attribute is not indexed. Only indexed and typed attributes can be used to order by."
                ));
        }
        if a.checked_data_type.is_none() {
            order_errors.push(format!(
                    "The `{name}` attribute is not typed. Only typed and indexed attributes can be used to order by."
                ));
        }
        if a.cardinality != crate::attr::Cardinality::One {
            order_errors.push(format!(
                    "The `{name}` attribute has cardinality `many`. Only attributes with cardinality `one` can be used to order by."
                ));
        }
        if !order_errors.is_empty() {
            let errors: Vec<Value> = order_errors
                .iter()
                .map(|m| {
                    json!({
                        "expected": "supported-order?",
                        "in": [form.k, "$", "order"],
                        "message": m,
                    })
                })
                .collect();
            return Err(InstantError::validation_failed(
                "query",
                order_errors.join(", "),
                Value::Array(errors),
            ));
        }
        let t = a.checked_data_type.unwrap();
        (a, Some(t))
    };

    // Validate cursors reference the order attr — legacy message format
    // (LEGACY instaql.clj:911-922, 979-997): an attr's order label is its
    // fwd label except `id`, which reads as `serverCreatedAt`.
    let order_label = |attr: &Attr| -> String {
        if attr.label == "id" {
            "serverCreatedAt".to_string()
        } else {
            attr.label.clone()
        }
    };
    for (cursor, which) in [(&form.opts.before, "before"), (&form.opts.after, "after")] {
        let Some(cursor) = cursor else { continue };
        if cursor.a != order_attr.id {
            let tail = match ctx.attrs.get(&cursor.a) {
                Some(ca) => format!(
                    "The query orders by `{}`, but the query that returned the cursor orders by `{}`.",
                    order_label(&order_attr),
                    order_label(ca)
                ),
                None => format!(
                    "The query orders by `{}`, but the query that returned the cursor orders by a missing attribute.",
                    order_label(&order_attr)
                ),
            };
            return Err(verr(format!("Invalid {which} cursor. {tail}")));
        }
    }

    let last = form.opts.last.is_some();
    // SQL direction: reversed for `last`
    let sql_dir = match (order.dir, last) {
        (Dir::Asc, false) | (Dir::Desc, true) => Dir::Asc,
        _ => Dir::Desc,
    };
    let limit = form.opts.effective_limit();
    let offset = form.opts.offset.unwrap_or(0);

    let mut qb = QueryBuilder::new("SELECT idt.entity_id AS eid, ");
    // order value + t exprs
    let by_created = order.key == "serverCreatedAt";
    if by_created {
        qb.push("idt.value AS order_v, idt.created_at AS order_t, idt.created_at AS sort_t, NULL::float8 AS sort_n, NULL::text AS sort_s, NULL::boolean AS sort_b, NULL::timestamptz AS sort_d");
    } else {
        let t = order_type.unwrap();
        qb.push("ord.value AS order_v, coalesce(ord.created_at, idt.created_at) AS order_t, NULL::bigint AS sort_t, ");
        for st in [
            CheckedDataType::Number,
            CheckedDataType::String,
            CheckedDataType::Boolean,
            CheckedDataType::Date,
        ] {
            let col = match st {
                CheckedDataType::Number => "sort_n",
                CheckedDataType::String => "sort_s",
                CheckedDataType::Boolean => "sort_b",
                CheckedDataType::Date => "sort_d",
            };
            if st == t {
                qb.push(format!("{}(ord.value) AS {col}", extract_fn(t)));
            } else {
                let sql_t = match st {
                    CheckedDataType::Number => "float8",
                    CheckedDataType::String => "text",
                    CheckedDataType::Boolean => "boolean",
                    CheckedDataType::Date => "timestamptz",
                };
                qb.push(format!("NULL::{sql_t} AS {col}"));
            }
            if st != CheckedDataType::Date {
                qb.push(", ");
            }
        }
    }
    qb.push(" FROM triples idt ");
    if !by_created {
        qb.push("LEFT JOIN triples ord ON ord.app_id = ");
        qb.push_bind(ctx.app_id);
        qb.push(" AND ord.entity_id = idt.entity_id AND ord.attr_id = ");
        qb.push_bind(order_attr.id);
        qb.push(" AND ord.ea ");
    }
    qb.push("WHERE idt.app_id = ");
    qb.push_bind(ctx.app_id);
    qb.push(" AND idt.attr_id = ");
    qb.push_bind(id_attr.id);
    qb.push(" AND idt.ea");
    let mut missing = false;
    if let Some(w) = &form.opts.where_conds {
        qb.push(" AND ");
        match sql_ctx.push_cond(&mut qb, &form.etype, "idt.entity_id", w, 0) {
            Ok(r) => r?,
            Err(MissingAttr) => missing = true,
        }
    }
    if missing {
        return Ok(FormOut {
            k: form.k.clone(),
            etype: form.etype.clone(),
            entities: vec![],
            // legacy emits neither page-info nor aggregate for a form whose
            // attrs don't exist (instaql.clj:1171-1172)
            page_info: None,
            aggregate: None,
        });
    }

    // Cursor comparisons
    let sort_col = if by_created {
        "idt.created_at"
    } else {
        match order_type.unwrap() {
            CheckedDataType::Number => "triples_extract_number_value(ord.value)",
            CheckedDataType::String => "triples_extract_string_value(ord.value)",
            CheckedDataType::Boolean => "triples_extract_boolean_value(ord.value)",
            CheckedDataType::Date => "triples_extract_date_value(ord.value)",
        }
    };
    for (cursor, is_after) in [(&form.opts.after, true), (&form.opts.before, false)] {
        let Some(cursor) = cursor else { continue };
        let inclusive = if is_after {
            form.opts.after_inclusive
        } else {
            form.opts.before_inclusive
        };
        // after = rows later in display order; before = rows earlier
        let forward = is_after;
        push_cursor_filter(
            &mut qb, sort_col, by_created, order_type, cursor, order.dir, forward, inclusive,
        )?;
    }

    // ORDER BY: nulls first under asc, last under desc (of the DISPLAY order),
    // applied to the SQL direction accordingly.
    let dir_sql = match sql_dir {
        Dir::Asc => "ASC",
        Dir::Desc => "DESC",
    };
    // display-nulls-first == asc display; when sql_dir is reversed (last),
    // nulls must flip too so the un-reversed output keeps nulls-first.
    let nulls = match (order.dir, sql_dir) {
        (Dir::Asc, Dir::Asc) => "NULLS FIRST",
        (Dir::Asc, Dir::Desc) => "NULLS LAST",
        (Dir::Desc, Dir::Desc) => "NULLS LAST",
        (Dir::Desc, Dir::Asc) => "NULLS FIRST",
    };
    qb.push(format!(
        " ORDER BY {sort_col} {dir_sql} {nulls}, idt.entity_id {dir_sql}"
    ));
    if offset > 0 {
        qb.push(" OFFSET ");
        qb.push_bind(offset);
    }
    if let Some(l) = limit {
        qb.push(" LIMIT ");
        qb.push_bind(l + 1);
    }

    let rows = qb.build().fetch_all(&mut *conn).await?;
    let mut matched: Vec<MatchedRow> = rows
        .iter()
        .map(|r| MatchedRow {
            eid: r.get("eid"),
            order_v: r.try_get::<Value, _>("order_v").unwrap_or(Value::Null),
            order_t: r.get::<i64, _>("order_t"),
        })
        .collect();

    let mut has_extra = false;
    if let Some(l) = limit {
        if matched.len() as i64 > l {
            matched.truncate(l as usize);
            has_extra = true;
        }
    }
    if last {
        matched.reverse();
    }

    // page-info
    let page_info = if paginated {
        let mk_cursor = |row: &MatchedRow| -> Value {
            json!([row.eid, order_attr.id, row.order_v, row.order_t])
        };
        let start = matched.first().map(mk_cursor);
        let end = matched.last().map(mk_cursor);
        // has-next / has-prev relative to display order
        let (has_next, has_prev);
        if matched.is_empty() {
            has_next = false;
            has_prev = offset > 0 || form.opts.after.is_some();
        } else if last {
            // SQL was reversed: extra row lies before the displayed start
            has_prev = has_extra;
            has_next = cursor_row_exists(
                conn,
                ctx,
                &sql_ctx,
                form,
                &id_attr,
                &order_attr,
                order_type,
                by_created,
                &order,
                matched.last().unwrap(),
                true,
            )
            .await?;
        } else {
            has_next = has_extra
                || cursor_row_exists(
                    conn,
                    ctx,
                    &sql_ctx,
                    form,
                    &id_attr,
                    &order_attr,
                    order_type,
                    by_created,
                    &order,
                    matched.last().unwrap(),
                    true,
                )
                .await?;
            has_prev = offset > 0
                || cursor_row_exists(
                    conn,
                    ctx,
                    &sql_ctx,
                    form,
                    &id_attr,
                    &order_attr,
                    order_type,
                    by_created,
                    &order,
                    matched.first().unwrap(),
                    false,
                )
                .await?;
        }
        Some(PageInfoOut {
            start_cursor: start,
            end_cursor: end,
            has_next_page: has_next,
            has_previous_page: has_prev,
        })
    } else {
        None
    };

    // ---- entity fetch + children ----
    let eids: Vec<Uuid> = matched.iter().map(|m| m.eid).collect();
    let entities = fetch_entities(conn, ctx, form, &eids).await?;

    Ok(FormOut {
        k: form.k.clone(),
        etype: form.etype.clone(),
        entities,
        page_info,
        aggregate: None,
    })
}

#[allow(clippy::too_many_arguments)]
fn push_cursor_filter(
    qb: &mut QueryBuilder<Postgres>,
    sort_col: &str,
    by_created: bool,
    order_type: Option<CheckedDataType>,
    cursor: &Cursor,
    display_dir: Dir,
    forward: bool,
    inclusive: bool,
) -> Result<()> {
    // forward means "rows after the cursor in display order".
    // Display order: asc => bigger later, nulls first; desc => smaller later, nulls last.
    let cmp = match (display_dir, forward) {
        (Dir::Asc, true) | (Dir::Desc, false) => ">",
        _ => "<",
    };
    let e_cmp = if inclusive {
        format!("{cmp}=")
    } else {
        cmp.to_string()
    };
    let v_is_null = cursor.v.is_null();
    qb.push(" AND (");
    if v_is_null && !by_created {
        // Cursor at a null order value. Nulls sort first (asc display).
        // forward+asc: (null AND e > ce) OR NOT NULL; backward+asc: (null AND e < ce)
        let null_first = display_dir == Dir::Asc;
        let beyond_nulls = forward == null_first;
        qb.push(format!("({sort_col} IS NULL AND idt.entity_id {e_cmp} "));
        qb.push_bind(cursor.e.unwrap_or_default());
        qb.push(")");
        if beyond_nulls {
            qb.push(format!(" OR {sort_col} IS NOT NULL"));
        }
    } else {
        qb.push(format!("({sort_col} {cmp} "));
        push_cursor_value(qb, by_created, order_type, cursor)?;
        qb.push(format!(") OR ({sort_col} = "));
        push_cursor_value(qb, by_created, order_type, cursor)?;
        qb.push(format!(" AND idt.entity_id {e_cmp} "));
        qb.push_bind(cursor.e.unwrap_or_default());
        qb.push(")");
        // moving backward from a non-null cursor never reaches nulls under
        // nulls-first-asc when forward, but backward it does:
        let null_first = display_dir == Dir::Asc;
        if forward != null_first && !by_created {
            qb.push(format!(" OR {sort_col} IS NULL"));
        }
    }
    qb.push(")");
    Ok(())
}

fn push_cursor_value(
    qb: &mut QueryBuilder<Postgres>,
    by_created: bool,
    order_type: Option<CheckedDataType>,
    cursor: &Cursor,
) -> Result<()> {
    if by_created {
        qb.push_bind(cursor.t.unwrap_or(0));
        return Ok(());
    }
    let t = order_type.unwrap();
    push_typed_value(qb, t, &cursor.v);
    Ok(())
}

/// Does a row exist beyond (forward=true) / before (forward=false) this row?
#[allow(clippy::too_many_arguments)]
async fn cursor_row_exists(
    conn: &mut PgConnection,
    ctx: &QueryCtx<'_>,
    sql_ctx: &SqlCtx<'_>,
    form: &Form,
    id_attr: &Attr,
    order_attr: &Attr,
    order_type: Option<CheckedDataType>,
    by_created: bool,
    order: &OrderSpec,
    row: &MatchedRow,
    forward: bool,
) -> Result<bool> {
    let cursor = Cursor {
        e: Some(row.eid),
        a: order_attr.id,
        v: row.order_v.clone(),
        t: Some(row.order_t),
    };
    let mut qb = QueryBuilder::new("SELECT 1 AS x FROM triples idt ");
    if !by_created {
        qb.push("LEFT JOIN triples ord ON ord.app_id = ");
        qb.push_bind(ctx.app_id);
        qb.push(" AND ord.entity_id = idt.entity_id AND ord.attr_id = ");
        qb.push_bind(order_attr.id);
        qb.push(" AND ord.ea ");
    }
    qb.push("WHERE idt.app_id = ");
    qb.push_bind(ctx.app_id);
    qb.push(" AND idt.attr_id = ");
    qb.push_bind(id_attr.id);
    qb.push(" AND idt.ea");
    if let Some(w) = &form.opts.where_conds {
        qb.push(" AND ");
        match sql_ctx.push_cond(&mut qb, &form.etype, "idt.entity_id", w, 0) {
            Ok(r) => r?,
            Err(MissingAttr) => return Ok(false),
        }
    }
    let sort_col = if by_created {
        "idt.created_at"
    } else {
        match order_type.unwrap() {
            CheckedDataType::Number => "triples_extract_number_value(ord.value)",
            CheckedDataType::String => "triples_extract_string_value(ord.value)",
            CheckedDataType::Boolean => "triples_extract_boolean_value(ord.value)",
            CheckedDataType::Date => "triples_extract_date_value(ord.value)",
        }
    };
    push_cursor_filter(
        &mut qb, sort_col, by_created, order_type, &cursor, order.dir, forward, false,
    )?;
    qb.push(" LIMIT 1");
    let row = qb.build().fetch_optional(&mut *conn).await?;
    Ok(row.is_some())
}

/// Fetch entity nodes (ea triples) + children recursively for a set of eids.
async fn fetch_entities(
    conn: &mut PgConnection,
    ctx: &QueryCtx<'_>,
    form: &Form,
    eids: &[Uuid],
) -> Result<Vec<EntityNode>> {
    if eids.is_empty() {
        return Ok(vec![]);
    }
    // ea attr ids: cardinality-one attrs of the etype (fields restrict)
    let mut ea_ids: Vec<Uuid> = vec![];
    for a in ctx.attrs.attrs_of_etype(&form.etype) {
        if a.cardinality != crate::attr::Cardinality::One {
            continue;
        }
        if let Some(fields) = &form.opts.fields {
            let keep = a.label == "id"
                || fields.contains(&a.label)
                || (form.etype == "$files"
                    && a.label == "location-id"
                    && fields.contains(&"url".to_string()));
            if !keep {
                continue;
            }
        }
        ea_ids.push(a.id);
    }

    let rows = sqlx::query(
        "SELECT entity_id, attr_id, value, created_at FROM triples
         WHERE app_id = $1 AND entity_id = ANY($2) AND attr_id = ANY($3) AND ea",
    )
    .bind(ctx.app_id)
    .bind(eids)
    .bind(&ea_ids)
    .fetch_all(&mut *conn)
    .await?;

    let mut by_eid: BTreeMap<Uuid, Vec<TripleOut>> = BTreeMap::new();
    for row in rows {
        let t = TripleOut {
            e: row.get("entity_id"),
            a: row.get("attr_id"),
            v: row.get("value"),
            t: row.get::<Option<i64>, _>("created_at").unwrap_or(0),
        };
        by_eid.entry(t.e).or_default().push(t);
    }

    let mut nodes: Vec<EntityNode> = eids
        .iter()
        .map(|eid| EntityNode {
            eid: *eid,
            etype: form.etype.clone(),
            triples: by_eid.remove(eid).unwrap_or_default(),
            children: vec![],
        })
        .collect();

    // children
    for child in &form.children {
        attach_children(conn, ctx, form, child, &mut nodes).await?;
    }
    Ok(nodes)
}

fn attach_children<'a>(
    conn: &'a mut PgConnection,
    ctx: &'a QueryCtx<'_>,
    parent_form: &'a Form,
    child_form: &'a Form,
    parents: &'a mut [EntityNode],
) -> futures::future::BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        let parent_ids: Vec<Uuid> = parents.iter().map(|p| p.eid).collect();
        // Resolve link
        let fwd = ctx
            .attrs
            .by_fwd_name(&parent_form.etype, &child_form.k)
            .cloned();
        let (link_attr, forward, child_etype) = match fwd {
            Some(a) if a.value_type == ValueType::Ref => {
                let child_etype = a.reverse_etype.clone().unwrap_or_default();
                (Some(a), true, child_etype)
            }
            _ => match ctx
                .attrs
                .by_rev_name(&parent_form.etype, &child_form.k)
                .cloned()
            {
                Some(a) => {
                    let child_etype = a.etype.clone();
                    (Some(a), false, child_etype)
                }
                None => (None, true, String::new()),
            },
        };
        let Some(link_attr) = link_attr else {
            // missing link attr: empty child results per parent
            for p in parents.iter_mut() {
                p.children.push(ChildResult {
                    k: child_form.k.clone(),
                    etype: child_form.k.clone(),
                    link_triples: vec![],
                    entities: vec![],
                });
            }
            return Ok(());
        };

        // Fetch link triples for all parents
        let rows = if forward {
            sqlx::query(
                "SELECT entity_id, attr_id, value, created_at FROM triples
                 WHERE app_id = $1 AND attr_id = $2 AND eav AND entity_id = ANY($3)",
            )
            .bind(ctx.app_id)
            .bind(link_attr.id)
            .bind(&parent_ids)
            .fetch_all(&mut *conn)
            .await?
        } else {
            sqlx::query(
                "SELECT entity_id, attr_id, value, created_at FROM triples
                 WHERE app_id = $1 AND attr_id = $2 AND vae AND json_uuid_to_uuid(value) = ANY($3)",
            )
            .bind(ctx.app_id)
            .bind(link_attr.id)
            .bind(&parent_ids)
            .fetch_all(&mut *conn)
            .await?
        };

        // parent -> (link triples, child ids)
        let mut per_parent: HashMap<Uuid, Vec<(TripleOut, Uuid)>> = HashMap::new();
        let mut all_children: HashSet<Uuid> = HashSet::new();
        for row in rows {
            let t = TripleOut {
                e: row.get("entity_id"),
                a: row.get("attr_id"),
                v: row.get("value"),
                t: row.get::<Option<i64>, _>("created_at").unwrap_or(0),
            };
            let (parent, child) = if forward {
                let child =
                    t.v.as_str()
                        .and_then(|s| Uuid::parse_str(s).ok())
                        .unwrap_or_default();
                (t.e, child)
            } else {
                let parent =
                    t.v.as_str()
                        .and_then(|s| Uuid::parse_str(s).ok())
                        .unwrap_or_default();
                (parent, t.e)
            };
            all_children.insert(child);
            per_parent.entry(parent).or_default().push((t, child));
        }

        // Apply the child's where filter over candidate children
        let kept: HashSet<Uuid> = if let Some(w) = &child_form.opts.where_conds {
            let sql_ctx = SqlCtx {
                app_id: ctx.app_id,
                attrs: ctx.attrs,
            };
            let candidates: Vec<Uuid> = all_children.iter().cloned().collect();
            if candidates.is_empty() {
                HashSet::new()
            } else {
                let mut qb = QueryBuilder::new(
                    "SELECT DISTINCT t.entity_id AS eid FROM triples t WHERE t.app_id = ",
                );
                qb.push_bind(ctx.app_id);
                qb.push(" AND t.entity_id = ANY(");
                qb.push_bind(candidates);
                qb.push(") AND t.attr_id = ");
                let child_id_attr = ctx
                    .attrs
                    .id_attr_of(&child_etype)
                    .ok_or_else(|| verr(format!("no id attr for {child_etype}")))?;
                qb.push_bind(child_id_attr.id);
                qb.push(" AND ");
                match sql_ctx.push_cond(&mut qb, &child_etype, "t.entity_id", w, 0) {
                    Ok(r) => r?,
                    Err(MissingAttr) => {
                        for p in parents.iter_mut() {
                            p.children.push(ChildResult {
                                k: child_form.k.clone(),
                                etype: child_etype.clone(),
                                link_triples: vec![],
                                entities: vec![],
                            });
                        }
                        return Ok(());
                    }
                }
                let rows = qb.build().fetch_all(&mut *conn).await?;
                rows.iter().map(|r| r.get::<Uuid, _>("eid")).collect()
            }
        } else {
            all_children.clone()
        };

        // Recurse: fetch child entity nodes for all kept children at once
        let kept_vec: Vec<Uuid> = {
            let mut v: Vec<Uuid> = kept.iter().cloned().collect();
            v.sort();
            v
        };
        let child_form_resolved = Form {
            k: child_form.k.clone(),
            etype: child_etype.clone(),
            opts: child_form.opts.clone(),
            children: child_form.children.clone(),
        };
        let child_nodes = fetch_entities(conn, ctx, &child_form_resolved, &kept_vec).await?;
        let node_by_id: HashMap<Uuid, EntityNode> =
            child_nodes.into_iter().map(|n| (n.eid, n)).collect();

        for p in parents.iter_mut() {
            let pairs = per_parent.remove(&p.eid).unwrap_or_default();
            let mut link_triples = vec![];
            let mut entities = vec![];
            let mut seen = HashSet::new();
            for (t, child) in pairs {
                if !kept.contains(&child) {
                    continue;
                }
                link_triples.push(t);
                if seen.insert(child) {
                    if let Some(n) = node_by_id.get(&child) {
                        entities.push(n.clone());
                    }
                }
            }
            p.children.push(ChildResult {
                k: child_form.k.clone(),
                etype: child_etype.clone(),
                link_triples,
                entities,
            });
        }
        Ok(())
    })
}
