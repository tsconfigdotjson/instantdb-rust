//! CEL permission rules. Port of LEGACY model/rule.clj + db/cel.clj +
//! db/permissioned_transaction.clj semantics (see docs/PERMS.md).

use std::collections::{HashMap, HashSet};

use serde_json::{json, Map, Value};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::attr::{AttrMap, Cardinality, ValueType};
use crate::error::{InstantError, Result};
use crate::instaql::QueryResult;
use crate::triple::EidRef;
use crate::tx::{self, TxOptions, TxReport, TxStep};

#[derive(Debug, Clone, Default)]
pub struct Rules {
    pub code: Value,
}

impl Rules {
    pub async fn load(conn: &mut PgConnection, app_id: Uuid) -> Result<Rules> {
        let row = sqlx::query("SELECT code FROM rules WHERE app_id = $1")
            .bind(app_id)
            .fetch_optional(&mut *conn)
            .await?;
        Ok(Rules {
            code: row
                .map(|r| r.get::<Value, _>("code"))
                .unwrap_or(Value::Null),
        })
    }

    /// Lookup chain: [etype allow action] -> [etype allow $default]
    /// -> [$default allow action] -> [$default allow $default] -> system default.
    /// Returns (expr, binds) or None = allow (user etypes) / system default.
    fn rule_source(&self, etype: &str, action: &str) -> Option<(String, Vec<(String, String)>)> {
        for (et, act) in [
            (etype, action),
            (etype, "$default"),
            ("$default", action),
            ("$default", "$default"),
        ] {
            let ns = self.code.get(et);
            let Some(ns) = ns else { continue };
            let allow = ns.get("allow").and_then(|a| a.get(act));
            let Some(expr) = allow else { continue };
            let expr_str = match expr {
                Value::String(s) => s.clone(),
                Value::Bool(b) => b.to_string(),
                _ => continue,
            };
            // legacy `with-binds` (rule.clj:139-142): every rule sees
            // `$default.bind` followed by the *requested* etype's binds,
            // whichever namespace the allow expression came from
            return Some((expr_str, self.binds_of(etype)));
        }
        None
    }

    /// `$default.bind` ++ `<etype>.bind`, each in array (`[k v k v]`) or
    /// object (`{k: v}`) form (rule.clj:100-111 `normalize-bind`).
    fn binds_of(&self, etype: &str) -> Vec<(String, String)> {
        let mut binds = vec![];
        for ns in ["$default", etype] {
            let flat = normalize_bind(self.code.get(ns).and_then(|n| n.get("bind")));
            let mut i = 0;
            while i + 1 < flat.len() {
                if let (Some(name), Some(expr)) = (flat[i].as_str(), flat[i + 1].as_str()) {
                    binds.push((name.to_string(), expr.to_string()));
                }
                i += 2;
            }
        }
        binds
    }

    /// Explicit link/unlink rule for a link label on an etype:
    /// [etype allow link <label>] -> [etype allow link $default]. None = no
    /// explicit rule (use the update+view fallback).
    pub fn link_program(&self, etype: &str, action: &str, label: &str) -> Option<Program> {
        let rules = self.code.get(etype)?.get("allow")?.get(action)?;
        for key in [label, "$default"] {
            if let Some(expr) = rules.get(key) {
                let expr = match expr {
                    Value::String(s) => s.clone(),
                    Value::Bool(b) => b.to_string(),
                    _ => continue,
                };
                return Some(Program {
                    rule: None,
                    expr,
                    binds: self.binds_of(etype),
                });
            }
        }
        None
    }

    /// Field-level view rule [etype fields <field>]. None = field always visible.
    pub fn field_program(&self, etype: &str, field: &str) -> Option<Program> {
        let expr = self.code.get(etype)?.get("fields")?.get(field)?;
        let expr = match expr {
            Value::String(s) => s.clone(),
            Value::Bool(b) => b.to_string(),
            _ => return None,
        };
        Some(Program {
            rule: None,
            expr,
            binds: self.binds_of(etype),
        })
    }

    /// Does this etype have any field rules at all? (fast path)
    pub fn has_field_rules(&self, etype: &str) -> bool {
        self.code
            .get(etype)
            .and_then(|ns| ns.get("fields"))
            .map(|f| f.is_object())
            .unwrap_or(false)
    }

    /// Effective program for etype+action: falls back to system defaults.
    pub fn program(&self, etype: &str, action: &str) -> Program {
        if let Some((expr, binds)) = self.rule_source(etype, action) {
            return Program {
                expr,
                binds,
                rule: Some((etype.to_string(), action.to_string())),
            };
        }
        // System defaults
        if etype == "$users" {
            let expr = match action {
                "create" => "true",
                // rule.clj:198-210: upgraded guests keep access to their
                // guest rows through the linkedPrimaryUser link
                "view" | "update" => {
                    "auth.id == data.id || (data.linkedPrimaryUser != null && auth.id == data.linkedPrimaryUser)"
                }
                _ => "false",
            };
            return Program {
                expr: expr.to_string(),
                binds: vec![],
                rule: Some((etype.to_string(), action.to_string())),
            };
        }
        if etype.starts_with('$') {
            return Program {
                expr: "false".to_string(),
                binds: vec![],
                rule: Some((etype.to_string(), action.to_string())),
            };
        }
        Program {
            expr: "true".to_string(),
            binds: vec![],
            rule: Some((etype.to_string(), action.to_string())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Program {
    pub expr: String,
    pub binds: Vec<(String, String)>,
    /// `[etype action]` for legacy's `permission-evaluation-failed` hint;
    /// None for ad-hoc programs.
    pub rule: Option<(String, String)>,
}

impl Program {
    pub fn is_true(&self) -> bool {
        self.expr.trim() == "true" && self.binds.is_empty()
    }
    pub fn is_false(&self) -> bool {
        self.expr.trim() == "false"
    }
    fn all_sources(&self) -> Vec<&str> {
        let mut v = vec![self.expr.as_str()];
        for (_, b) in &self.binds {
            v.push(b.as_str());
        }
        v
    }
}

/// Authenticated user context for rule evaluation.
#[derive(Debug, Clone, Default)]
pub struct AuthCtx {
    pub user_id: Option<Uuid>,
    /// $users entity map (id, email, ...)
    pub user_map: Option<Value>,
    /// per-request facts for the `request` / `rateLimit` bindings
    pub request: RequestCtx,
}

// ---------------------------------------------------------------------------
// Request context (legacy `*request-info*`, util/request.clj)

/// Per-request facts bound as `request.ip` / `request.origin`, plus the pool
/// that backs `rateLimit` bucket state.
#[derive(Debug, Clone, Default)]
pub struct RequestCtx {
    pub ip: Option<String>,
    pub origin: Option<String>,
    /// Bucket state lives in `rust_rate_limit_buckets` and is consumed on a
    /// pool connection, so tokens spent by a transaction that later rolls
    /// back stay spent (legacy buckets are in-memory and never roll back).
    /// None skips consumption (unit tests).
    pub pool: Option<sqlx::PgPool>,
}

impl RequestCtx {
    /// Legacy util/http.clj:84-118 and reactive/store.clj:497-508: `origin` is
    /// the Origin header; `ip` is the SECOND-TO-LAST `x-forwarded-for` hop
    /// (the last one is the load balancer's), so a single-hop header yields
    /// no ip at all.
    pub fn from_headers(origin: Option<&str>, x_forwarded_for: Option<&str>) -> Self {
        let ip = x_forwarded_for.and_then(|h| {
            let parts: Vec<&str> = h.split(',').collect();
            (parts.len() >= 2).then(|| parts[parts.len() - 2].trim().to_string())
        });
        RequestCtx {
            ip,
            origin: origin.map(|s| s.to_string()),
            pool: None,
        }
    }

    pub fn with_pool(mut self, pool: sqlx::PgPool) -> Self {
        self.pool = Some(pool);
        self
    }
}

/// Everything one rule evaluation needs beyond its data bindings.
pub struct EvalEnv<'a> {
    pub app_id: Uuid,
    /// the rules document, for `$rateLimits` bucket configs
    pub rules: &'a Rules,
    pub request: &'a RequestCtx,
    /// `request.modifiedFields`: labels written to the checked entity by this
    /// tx (create/update checks only; empty everywhere else)
    pub modified_fields: Vec<String>,
}

impl<'a> EvalEnv<'a> {
    pub fn new(app_id: Uuid, rules: &'a Rules, request: &'a RequestCtx) -> Self {
        EvalEnv {
            app_id,
            rules,
            request,
            modified_fields: vec![],
        }
    }

    pub fn with_modified_fields(mut self, fields: Vec<String>) -> Self {
        self.modified_fields = fields;
        self
    }
}

/// Legacy `get-modified-fields-for-eid` (permissioned_transaction.clj:262-279):
/// the forward labels of every add-triple / deep-merge-triple step aimed at
/// `eid` anywhere in the tx, in step order, distinct, without `id`;
/// retractions contribute nothing and forward link labels are included.
pub fn modified_fields_for(writes: &[(Uuid, Uuid)], attrs: &AttrMap, eid: Uuid) -> Vec<String> {
    let mut out: Vec<String> = vec![];
    for (e, attr_id) in writes {
        if *e != eid {
            continue;
        }
        let Some(attr) = attrs.get(attr_id) else {
            continue;
        };
        if attr.label == "id" || out.contains(&attr.label) {
            continue;
        }
        out.push(attr.label.clone());
    }
    out
}

// ---------------------------------------------------------------------------
// Ref path prefetch

fn extract_ref_paths(sources: &[&str], receiver: &str) -> Vec<String> {
    // matches `receiver.ref("...")` / `receiver.ref('...')`
    let mut out = vec![];
    for src in sources {
        let needle = format!("{receiver}.ref(");
        let mut rest = *src;
        while let Some(pos) = rest.find(&needle) {
            rest = &rest[pos + needle.len()..];
            let trimmed = rest.trim_start();
            if let Some(stripped) = trimmed.strip_prefix('"').or(trimmed.strip_prefix('\'')) {
                let quote = trimmed.chars().next().unwrap();
                if let Some(end) = stripped.find(quote) {
                    out.push(stripped[..end].to_string());
                }
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Resolve a data.ref path from an entity: walk link segments, collect the
/// terminal attr's values as a list.
async fn resolve_ref_path(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &AttrMap,
    etype: &str,
    eid: Uuid,
    path: &str,
) -> Result<Value> {
    let segs: Vec<&str> = path.split('.').collect();
    let mut current: Vec<Uuid> = vec![eid];
    let mut current_etype = etype.to_string();
    for (i, seg) in segs.iter().enumerate() {
        let is_last = i == segs.len() - 1;
        if is_last {
            // terminal: blob attr (or id) -> collect values
            if *seg == "id" {
                return Ok(json!(current
                    .iter()
                    .map(|u| u.to_string())
                    .collect::<Vec<_>>()));
            }
            if let Some(attr) = attrs.by_fwd_name(&current_etype, seg) {
                if attr.value_type == ValueType::Blob {
                    if current.is_empty() {
                        return Ok(json!([]));
                    }
                    let rows = sqlx::query(
                        "SELECT value FROM triples
                         WHERE app_id = $1 AND attr_id = $2 AND entity_id = ANY($3)
                           AND value != 'null'::jsonb",
                    )
                    .bind(app_id)
                    .bind(attr.id)
                    .bind(&current)
                    .fetch_all(&mut *conn)
                    .await?;
                    return Ok(Value::Array(
                        rows.iter().map(|r| r.get::<Value, _>("value")).collect(),
                    ));
                }
            }
        }
        // link hop (also allowed as terminal -> ids)
        let fwd = attrs.by_fwd_name(&current_etype, seg).cloned();
        let (next, next_etype): (Vec<Uuid>, String) = match fwd {
            Some(a) if a.value_type == ValueType::Ref => {
                if current.is_empty() {
                    (vec![], a.reverse_etype.clone().unwrap_or_default())
                } else {
                    let rows = sqlx::query(
                        "SELECT json_uuid_to_uuid(value) AS t FROM triples
                         WHERE app_id = $1 AND attr_id = $2 AND entity_id = ANY($3) AND eav",
                    )
                    .bind(app_id)
                    .bind(a.id)
                    .bind(&current)
                    .fetch_all(&mut *conn)
                    .await?;
                    (
                        rows.iter()
                            .filter_map(|r| r.get::<Option<Uuid>, _>("t"))
                            .collect(),
                        a.reverse_etype.clone().unwrap_or_default(),
                    )
                }
            }
            _ => match attrs.by_rev_name(&current_etype, seg).cloned() {
                Some(a) => {
                    if current.is_empty() {
                        (vec![], a.etype.clone())
                    } else {
                        let rows = sqlx::query(
                            "SELECT entity_id FROM triples
                             WHERE app_id = $1 AND attr_id = $2 AND vae
                               AND json_uuid_to_uuid(value) = ANY($3)",
                        )
                        .bind(app_id)
                        .bind(a.id)
                        .bind(&current)
                        .fetch_all(&mut *conn)
                        .await?;
                        (
                            rows.iter().map(|r| r.get::<Uuid, _>("entity_id")).collect(),
                            a.etype.clone(),
                        )
                    }
                }
                None => return Ok(json!([])),
            },
        };
        if is_last {
            return Ok(json!(next
                .iter()
                .map(|u| u.to_string())
                .collect::<Vec<_>>()));
        }
        current = next;
        current_etype = next_etype;
    }
    Ok(json!([]))
}

// ---------------------------------------------------------------------------
// CEL evaluation

fn json_to_cel(v: &Value) -> cel::Value {
    match v {
        Value::Null => cel::Value::Null,
        Value::Bool(b) => cel::Value::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                cel::Value::Int(i)
            } else {
                cel::Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        Value::String(s) => cel::Value::String(std::sync::Arc::new(s.clone())),
        Value::Array(a) => {
            cel::Value::List(std::sync::Arc::new(a.iter().map(json_to_cel).collect()))
        }
        Value::Object(o) => {
            let mut m: HashMap<cel::objects::Key, cel::Value> = HashMap::new();
            for (k, v) in o {
                m.insert(
                    cel::objects::Key::String(std::sync::Arc::new(k.clone())),
                    json_to_cel(v),
                );
            }
            cel::Value::Map(cel::objects::Map {
                map: std::sync::Arc::new(m),
            })
        }
    }
}

/// Collect every map key a compiled CEL expression can statically mention:
/// select fields (`x.foo`, `has(x.foo)`) and string literals (covering
/// `x['k']` and `'k' in x`).
///
/// Legacy's CelMap answers null for any missing key and its `containsKey`
/// always returns true (docs/PERMS.md §2), while the cel crate errors on
/// missing keys. Pre-inserting the collected keys as null into the rule-scope
/// maps (see [`null_safe_augment`]) reproduces the legacy semantics for every
/// key a rule can reference. Keys computed at runtime (`x[someVar]`) can't be
/// known statically; those still error and deny, like any CEL error.
fn collect_static_keys(expr: &cel::IdedExpr, out: &mut HashSet<String>) {
    use cel::common::ast::{EntryExpr, Expr, LiteralValue};
    match &expr.expr {
        Expr::Unspecified | Expr::Ident(_) => {}
        Expr::Literal(v) => {
            if let LiteralValue::String(s) = v {
                out.insert(s.inner().to_string());
            }
        }
        Expr::Call(c) => {
            if let Some(t) = &c.target {
                collect_static_keys(t, out);
            }
            for a in &c.args {
                collect_static_keys(a, out);
            }
        }
        Expr::Comprehension(c) => {
            for e in [
                &c.iter_range,
                &c.accu_init,
                &c.loop_cond,
                &c.loop_step,
                &c.result,
            ] {
                collect_static_keys(e, out);
            }
        }
        Expr::List(l) => {
            for e in &l.elements {
                collect_static_keys(e, out);
            }
        }
        Expr::Map(m) => {
            for e in &m.entries {
                if let EntryExpr::MapEntry(me) = &e.expr {
                    collect_static_keys(&me.key, out);
                    collect_static_keys(&me.value, out);
                }
            }
        }
        Expr::Select(s) => {
            out.insert(s.field.clone());
            collect_static_keys(&s.operand, out);
        }
        Expr::Struct(st) => {
            for e in &st.entries {
                match &e.expr {
                    EntryExpr::StructField(f) => collect_static_keys(&f.value, out),
                    EntryExpr::MapEntry(me) => {
                        collect_static_keys(&me.key, out);
                        collect_static_keys(&me.value, out);
                    }
                }
            }
        }
    }
}

/// Insert `keys` as null into every nested object of `v` (skipping the
/// internal "_refs" maps, which are keyed by ref paths and consulted by the
/// `ref` custom function).
fn null_safe_augment(v: &mut Value, keys: &HashSet<String>) {
    match v {
        Value::Object(m) => {
            for k in keys {
                if !m.contains_key(k.as_str()) {
                    m.insert(k.clone(), Value::Null);
                }
            }
            for (k, child) in m.iter_mut() {
                if k != "_refs" {
                    null_safe_augment(child, keys);
                }
            }
        }
        Value::Array(a) => {
            for child in a {
                null_safe_augment(child, keys);
            }
        }
        _ => {}
    }
}

/// Evaluate one program with the given bindings. `data`/`auth` objects carry
/// prefetched ref results under "_refs". Consumes any `rateLimit` tokens the
/// rule charged (see [`eval_program_pure`]).
pub async fn eval_program(
    program: &Program,
    data: &Value,
    new_data: Option<&Value>,
    auth: &Value,
    rule_params: &Value,
    env: &EvalEnv<'_>,
) -> Result<bool> {
    eval_program_full(program, data, new_data, auth, rule_params, None, None, env).await
}

/// eval_program with the link-check `linkedData` / `actions` bindings
/// (cel.clj:459-497: `link` programs see both, `unlink` programs only
/// `linkedData`).
#[allow(clippy::too_many_arguments)]
pub async fn eval_program_full(
    program: &Program,
    data: &Value,
    new_data: Option<&Value>,
    auth: &Value,
    rule_params: &Value,
    linked_data: Option<&Value>,
    actions: Option<&Value>,
    env: &EvalEnv<'_>,
) -> Result<bool> {
    let (ok, calls) = eval_program_pure(
        program,
        data,
        new_data,
        auth,
        rule_params,
        linked_data,
        actions,
        env,
    )?;
    // Legacy consumes at the `limit()` call and throws on exhaustion; here
    // the calls are charged right after the (synchronous) evaluation, which
    // is observably the same: a call only happens when CEL reached it, the
    // charge happens whether or not the rule then passed, and exhaustion
    // wins over the rule's own verdict. Legacy does exactly this on its
    // rule-wheres path (instaql.clj check-rate-limits-for-rule-wheres).
    consume_rate_limits(env, &calls).await?;
    Ok(ok)
}

/// Legacy `throw-permission-evaluation-failed!` (util/exception.clj:299-323)
/// for an app admin session (`show-cel-errors?`): the CEL message is echoed.
pub fn permission_evaluation_failed(program: &Program, cause: &str) -> InstantError {
    let (etype, action) = program
        .rule
        .clone()
        .unwrap_or_else(|| ("?".to_string(), "?".to_string()));
    let message = format!(
        "Could not evaluate permission rule for `{etype}.{action}`. {cause}. Debug this in the sandbox and then update your permission rules."
    );
    InstantError::new(
        "permission-evaluation-failed",
        400,
        message.clone(),
        Some(json!({
            "rule": [etype, action],
            "error": {"type": "evaluation-error", "message": message, "hint": cause},
        })),
    )
}

/// A `rateLimit.<name>.limit(key[, tokens])` call recorded during evaluation.
#[derive(Debug, Clone, PartialEq)]
pub struct RateLimitCall {
    pub bucket: String,
    pub key: Value,
    pub tokens: i64,
}

/// Fields of legacy's `request` proto struct (db/proto.clj:7-44).
pub const REQUEST_FIELDS: [&str; 4] = ["modifiedFields", "time", "ip", "origin"];

fn walk_expr(expr: &cel::IdedExpr, f: &mut dyn FnMut(&cel::IdedExpr)) {
    use cel::common::ast::{EntryExpr, Expr};
    f(expr);
    match &expr.expr {
        Expr::Unspecified | Expr::Ident(_) | Expr::Literal(_) => {}
        Expr::Call(c) => {
            if let Some(t) = &c.target {
                walk_expr(t, f);
            }
            for a in &c.args {
                walk_expr(a, f);
            }
        }
        Expr::Comprehension(c) => {
            for e in [
                &c.iter_range,
                &c.accu_init,
                &c.loop_cond,
                &c.loop_step,
                &c.result,
            ] {
                walk_expr(e, f);
            }
        }
        Expr::List(l) => {
            for e in &l.elements {
                walk_expr(e, f);
            }
        }
        Expr::Map(m) => {
            for e in &m.entries {
                if let EntryExpr::MapEntry(me) = &e.expr {
                    walk_expr(&me.key, f);
                    walk_expr(&me.value, f);
                }
            }
        }
        Expr::Select(s) => walk_expr(&s.operand, f),
        Expr::Struct(st) => {
            for e in &st.entries {
                match &e.expr {
                    EntryExpr::StructField(fl) => walk_expr(&fl.value, f),
                    EntryExpr::MapEntry(me) => {
                        walk_expr(&me.key, f);
                        walk_expr(&me.value, f);
                    }
                }
            }
        }
    }
}

fn is_ident(expr: &cel::IdedExpr, name: &str) -> bool {
    matches!(&expr.expr, cel::common::ast::Expr::Ident(i) if i == name)
}

/// `request` is a typed proto struct in legacy, so selecting an unknown
/// field fails at compile time ("undefined field 'x'", cel_test.clj:70-99).
pub fn undefined_request_field(expr: &cel::IdedExpr) -> Option<String> {
    let mut found = None;
    walk_expr(expr, &mut |e| {
        if let cel::common::ast::Expr::Select(s) = &e.expr {
            if is_ident(&s.operand, "request") && !REQUEST_FIELDS.contains(&s.field.as_str()) {
                found.get_or_insert(s.field.clone());
            }
        }
    });
    found
}

/// Names used as `rateLimit.<name>` / `rateLimit['<name>']` in an expression
/// (legacy cel.clj:1850-1872 rate-limit-validator).
pub fn rate_limit_names(expr: &cel::IdedExpr) -> Vec<String> {
    use cel::common::ast::{Expr, LiteralValue};
    let mut out = vec![];
    walk_expr(expr, &mut |e| match &e.expr {
        Expr::Select(s) if is_ident(&s.operand, "rateLimit") => out.push(s.field.clone()),
        Expr::Call(c)
            if c.func_name == "_[_]" && c.args.len() == 2 && is_ident(&c.args[0], "rateLimit") =>
        {
            if let Expr::Literal(LiteralValue::String(s)) = &c.args[1].expr {
                out.push(s.inner().to_string());
            }
        }
        _ => {}
    });
    out
}

/// Proto semantics for `has(request.<field>)`: an unset singular field is
/// "absent" for `has()` but still reads as its default (`""`). A plain CEL
/// map can't do both, so `has(request.x)` tests are folded to literals before
/// evaluation and the map always carries the defaults.
fn fold_request_has(expr: &mut cel::IdedExpr, present: &dyn Fn(&str) -> bool) {
    use cel::common::ast::{EntryExpr, Expr, LiteralValue};
    if let Expr::Select(s) = &expr.expr {
        if s.test && is_ident(&s.operand, "request") {
            let v = present(&s.field);
            expr.expr = Expr::Literal(LiteralValue::Boolean(cel::common::types::CelBool::from(v)));
            return;
        }
    }
    match &mut expr.expr {
        Expr::Unspecified | Expr::Ident(_) | Expr::Literal(_) => {}
        Expr::Call(c) => {
            if let Some(t) = &mut c.target {
                fold_request_has(t, present);
            }
            for a in &mut c.args {
                fold_request_has(a, present);
            }
        }
        Expr::Comprehension(c) => {
            fold_request_has(&mut c.iter_range, present);
            fold_request_has(&mut c.accu_init, present);
            fold_request_has(&mut c.loop_cond, present);
            fold_request_has(&mut c.loop_step, present);
            fold_request_has(&mut c.result, present);
        }
        Expr::List(l) => {
            for e in &mut l.elements {
                fold_request_has(e, present);
            }
        }
        Expr::Map(m) => {
            for e in &mut m.entries {
                if let EntryExpr::MapEntry(me) = &mut e.expr {
                    fold_request_has(&mut me.key, present);
                    fold_request_has(&mut me.value, present);
                }
            }
        }
        Expr::Select(s) => fold_request_has(&mut s.operand, present),
        Expr::Struct(st) => {
            for e in &mut st.entries {
                match &mut e.expr {
                    EntryExpr::StructField(fl) => fold_request_has(&mut fl.value, present),
                    EntryExpr::MapEntry(me) => {
                        fold_request_has(&mut me.key, present);
                        fold_request_has(&mut me.value, present);
                    }
                }
            }
        }
    }
}

fn cel_key(s: &str) -> cel::objects::Key {
    cel::objects::Key::String(std::sync::Arc::new(s.to_string()))
}

fn cel_map(entries: Vec<(&str, cel::Value)>) -> cel::Value {
    let mut m: HashMap<cel::objects::Key, cel::Value> = HashMap::new();
    for (k, v) in entries {
        m.insert(cel_key(k), v);
    }
    cel::Value::Map(cel::objects::Map {
        map: std::sync::Arc::new(m),
    })
}

/// The `request` binding (proto.clj create-request-proto): `time` is a CEL
/// timestamp taken now, per evaluation; unset ip/origin read as `""`.
fn request_binding(env: &EvalEnv<'_>) -> cel::Value {
    let fields: Vec<cel::Value> = env
        .modified_fields
        .iter()
        .map(|f| cel::Value::String(std::sync::Arc::new(f.clone())))
        .collect();
    cel_map(vec![
        (
            "modifiedFields",
            cel::Value::List(std::sync::Arc::new(fields)),
        ),
        (
            "time",
            cel::Value::Timestamp(chrono::Utc::now().fixed_offset()),
        ),
        (
            "ip",
            cel::Value::String(std::sync::Arc::new(
                env.request.ip.clone().unwrap_or_default(),
            )),
        ),
        (
            "origin",
            cel::Value::String(std::sync::Arc::new(
                env.request.origin.clone().unwrap_or_default(),
            )),
        ),
    ])
}

const RATE_LIMIT_MARKER: &str = "__instantRateLimitBucket";

/// The `rateLimit` binding (cel.clj create-rate-limit-obj): one bucket object
/// per `$rateLimits` entry, exposing `.limit(key)` / `.limit(key, tokens)`.
fn rate_limit_binding(env: &EvalEnv<'_>) -> cel::Value {
    let mut entries: Vec<(&str, cel::Value)> = vec![];
    if let Some(Value::Object(limits)) = env.rules.code.get("$rateLimits") {
        for name in limits.keys() {
            entries.push((
                name.as_str(),
                cel_map(vec![(
                    RATE_LIMIT_MARKER,
                    cel::Value::String(std::sync::Arc::new(name.clone())),
                )]),
            ));
        }
    }
    cel_map(entries)
}

fn cel_to_json(v: &cel::Value) -> Value {
    match v {
        cel::Value::Null => Value::Null,
        cel::Value::Bool(b) => json!(b),
        cel::Value::Int(i) => json!(i),
        cel::Value::UInt(u) => json!(u),
        cel::Value::Float(f) => json!(f),
        cel::Value::String(s) => json!(s.as_str()),
        cel::Value::Bytes(b) => json!(b.as_ref()),
        cel::Value::List(l) => Value::Array(l.iter().map(cel_to_json).collect()),
        cel::Value::Map(m) => {
            let mut out = Map::new();
            for (k, v) in m.map.iter() {
                out.insert(format!("{k:?}"), cel_to_json(v));
            }
            Value::Object(out)
        }
        cel::Value::Timestamp(t) => json!(t.to_rfc3339()),
        other => json!(format!("{other:?}")),
    }
}

/// Synchronous evaluation: returns the verdict plus the `rateLimit` calls the
/// rule made (to be charged by the caller). Unbound / erroring expressions
/// deny, like any CEL error; an unknown `request` field is a validation
/// error like legacy's compile-time check.
#[allow(clippy::too_many_arguments)]
pub fn eval_program_pure(
    program: &Program,
    data: &Value,
    new_data: Option<&Value>,
    auth: &Value,
    rule_params: &Value,
    linked_data: Option<&Value>,
    actions: Option<&Value>,
    env: &EvalEnv<'_>,
) -> Result<(bool, Vec<RateLimitCall>)> {
    if program.is_true() {
        return Ok((true, vec![]));
    }
    if program.is_false() && program.binds.is_empty() {
        return Ok((false, vec![]));
    }
    let compiled = cel::Program::compile(&program.expr).map_err(|e| {
        InstantError::permission_denied(json!([]), format!("Invalid permission rule: {e}"))
    })?;
    let compiled_binds: Vec<(String, Option<cel::Program>)> = program
        .binds
        .iter()
        .map(|(name, expr)| (name.clone(), cel::Program::compile(expr).ok()))
        .collect();
    for p in std::iter::once(&compiled).chain(compiled_binds.iter().filter_map(|(_, p)| p.as_ref()))
    {
        if let Some(field) = undefined_request_field(p.expression()) {
            let message = format!("undefined field '{field}'");
            return Err(InstantError::validation_failed(
                "permission",
                message.clone(),
                json!([{"message": message}]),
            ));
        }
    }

    // legacy CelMap null-safety: pre-insert every statically-mentioned key
    let mut static_keys = HashSet::new();
    collect_static_keys(compiled.expression(), &mut static_keys);
    for (_, p) in &compiled_binds {
        if let Some(p) = p {
            collect_static_keys(p.expression(), &mut static_keys);
        }
    }
    let mut data = data.clone();
    null_safe_augment(&mut data, &static_keys);
    // legacy: a nil user binds an empty map, so `auth.id == null` holds
    let mut auth = match auth {
        Value::Null => Value::Object(Map::new()),
        v => v.clone(),
    };
    null_safe_augment(&mut auth, &static_keys);
    let mut rule_params = rule_params.clone();
    null_safe_augment(&mut rule_params, &static_keys);
    let mut new_data = new_data.cloned();
    if let Some(nd) = new_data.as_mut() {
        null_safe_augment(nd, &static_keys);
    }
    let mut linked_data = linked_data.cloned();
    if let Some(ld) = linked_data.as_mut() {
        null_safe_augment(ld, &static_keys);
    }

    let calls: std::sync::Arc<std::sync::Mutex<Vec<RateLimitCall>>> = Default::default();
    let mut ctx = cel::Context::default();
    // legacy registers cel-java's strings + math extensions and its own
    // getTime / timestamp overloads (cel.clj:387-414, :439-454, :479-488)
    crate::cel_ext::register(&mut ctx);
    ctx.add_function(
        "ref",
        |cel::extractors::This(this): cel::extractors::This<cel::Value>,
         path: std::sync::Arc<String>|
         -> std::result::Result<cel::Value, cel::ExecutionError> {
            if let cel::Value::Map(m) = &this {
                let refs_key = cel::objects::Key::String(std::sync::Arc::new("_refs".to_string()));
                if let Some(cel::Value::Map(refs)) = m.map.get(&refs_key) {
                    let path_key = cel::objects::Key::String(std::sync::Arc::new(path.to_string()));
                    if let Some(v) = refs.map.get(&path_key) {
                        return Ok(v.clone());
                    }
                }
            }
            Ok(cel::Value::List(std::sync::Arc::new(vec![])))
        },
    );
    {
        let calls = calls.clone();
        ctx.add_function(
            "limit",
            move |cel::extractors::This(this): cel::extractors::This<cel::Value>,
                  cel::extractors::Arguments(args): cel::extractors::Arguments|
                  -> std::result::Result<cel::Value, cel::ExecutionError> {
                let bucket = match &this {
                    cel::Value::Map(m) => match m.map.get(&cel_key(RATE_LIMIT_MARKER)) {
                        Some(cel::Value::String(s)) => s.to_string(),
                        _ => {
                            return Err(cel::ExecutionError::function_error(
                                "limit",
                                "no matching overload for 'limit'",
                            ))
                        }
                    },
                    _ => {
                        return Err(cel::ExecutionError::function_error(
                            "limit",
                            "no matching overload for 'limit'",
                        ))
                    }
                };
                let key = args.first().map(cel_to_json).unwrap_or(Value::Null);
                // legacy fails to evaluate `limit(null)` (an anonymous
                // `auth.id` key); a CEL error here denies like any other
                if key.is_null() {
                    return Err(cel::ExecutionError::function_error(
                        "limit",
                        "rate limit key must not be null",
                    ));
                }
                let tokens = match args.get(1) {
                    None => 1,
                    Some(cel::Value::Int(i)) => *i,
                    Some(cel::Value::UInt(u)) => *u as i64,
                    Some(_) => {
                        return Err(cel::ExecutionError::function_error(
                            "limit",
                            "no matching overload for 'limit'",
                        ))
                    }
                };
                calls.lock().unwrap().push(RateLimitCall {
                    bucket,
                    key,
                    tokens,
                });
                Ok(cel::Value::Bool(true))
            },
        );
    }
    ctx.add_variable_from_value("data", json_to_cel(&data));
    ctx.add_variable_from_value("auth", json_to_cel(&auth));
    ctx.add_variable_from_value("ruleParams", json_to_cel(&rule_params));
    match &new_data {
        Some(nd) => ctx.add_variable_from_value("newData", json_to_cel(nd)),
        None => ctx.add_variable_from_value("newData", json_to_cel(&data)),
    }
    if let Some(ld) = &linked_data {
        ctx.add_variable_from_value("linkedData", json_to_cel(ld));
    }
    // `actions` is bound for link checks only (cel.clj:556-580):
    // {"data": "create"|"update", "linkedData": "create"|"update"}
    if let Some(a) = actions {
        ctx.add_variable_from_value("actions", json_to_cel(a));
    }
    ctx.add_variable_from_value("request", request_binding(env));
    ctx.add_variable_from_value("rateLimit", rate_limit_binding(env));

    let present = |field: &str| match field {
        "ip" => env.request.ip.is_some(),
        "origin" => env.request.origin.is_some(),
        // repeated / always-set fields
        _ => true,
    };
    let fold = |p: &cel::Program| {
        let mut e = p.expression().clone();
        fold_request_has(&mut e, &present);
        crate::cel_ext::rewrite_timestamp_calls(&mut e);
        e
    };

    // binds: evaluate in order, retrying to tolerate forward references
    let mut pending: Vec<(String, cel::IdedExpr)> = vec![];
    let mut unresolved: Vec<String> = vec![];
    for (name, p) in &compiled_binds {
        match p {
            Some(p) => pending.push((name.clone(), fold(p))),
            None => unresolved.push(name.clone()),
        }
    }
    let mut rounds = 0;
    while !pending.is_empty() && rounds < 5 {
        rounds += 1;
        let mut next = vec![];
        for (name, e) in pending {
            match cel::Value::resolve(&e, &ctx) {
                Ok(v) => {
                    ctx.add_variable_from_value(name.as_str(), v);
                }
                Err(_) => next.push((name, e)),
            }
        }
        pending = next;
    }
    // unresolved binds evaluate to null so has()-style checks stay sane
    unresolved.extend(pending.into_iter().map(|(name, _)| name));
    for name in &unresolved {
        ctx.add_variable_from_value(name.as_str(), cel::Value::Null);
    }

    let main = fold(&compiled);
    // legacy `assert-permitted!` (util/exception.clj:291-297) tests the CEL
    // result with Clojure truthiness: only false/null deny. An evaluation
    // error is `permission-evaluation-failed` (exception.clj:299-323), not a
    // silent deny, so a typo in a rule surfaces instead of hiding data.
    let verdict = match cel::Value::resolve(&main, &ctx) {
        Ok(cel::Value::Bool(b)) => b,
        Ok(cel::Value::Null) => false,
        Ok(_) => true,
        Err(e) => {
            // legacy shows the CEL message only with `show-cel-errors?`
            // (plain admin HTTP calls, which never evaluate rules); every
            // session that reaches here sees "You may have a typo"
            tracing::debug!(rule = ?program.rule, error = %e, "permission rule evaluation failed");
            return Err(permission_evaluation_failed(program, "You may have a typo"));
        }
    };
    let calls = std::mem::take(&mut *calls.lock().unwrap());
    Ok((verdict, calls))
}

// ---------------------------------------------------------------------------
// Rate-limit buckets (legacy rate_limit.clj: bucket4j token buckets keyed by
// app + bucket name + config + caller key). State is a row per bandwidth in
// `rust_rate_limit_buckets`, so every node sees the same bucket.

/// One bandwidth of a `$rateLimits` config (rate_limit.clj:224-275).
#[derive(Debug, Clone, PartialEq)]
pub struct Bandwidth {
    pub capacity: i64,
    pub refill_amount: i64,
    pub period_secs: f64,
    /// `greedy` refills continuously; `interval` adds `refill_amount` once per
    /// whole period
    pub greedy: bool,
}

/// Parse a validated `$rateLimits` entry (defaults: period "1 hour", greedy,
/// amount = capacity). Invalid configs (which rule validation rejects at
/// save time) yield no bandwidths, i.e. an unlimited bucket.
pub fn parse_rate_limit_config(config: &Value) -> Vec<Bandwidth> {
    let Some(limits) = config.get("limits").and_then(|l| l.as_array()) else {
        return vec![];
    };
    let mut out = vec![];
    for limit in limits {
        let Some(capacity) = pos_int(limit.get("capacity")) else {
            continue;
        };
        let refill = limit.get("refill").cloned().unwrap_or(json!({}));
        let amount = pos_int(refill.get("amount")).unwrap_or(capacity);
        let greedy = refill.get("type").and_then(|t| t.as_str()) != Some("interval");
        let period_secs = refill
            .get("period")
            .and_then(|p| p.as_str())
            .and_then(parse_interval_secs)
            .unwrap_or(3600.0);
        out.push(Bandwidth {
            capacity,
            refill_amount: amount,
            period_secs,
            greedy,
        });
    }
    out
}

/// Legacy `user-key-hash`: the bucket identity covers the config, so editing
/// a limit in the rules starts fresh buckets.
fn bucket_key(app_id: Uuid, name: &str, config: &Value, key: &Value) -> Uuid {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(b"user");
    h.update(app_id.as_bytes());
    h.update(name.as_bytes());
    h.update(config.to_string().as_bytes());
    h.update(key.to_string().as_bytes());
    let digest = h.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

/// Legacy `throw-permission-rate-limited!` (util/exception.clj:495-500).
pub fn rate_limited_error(retry_at: chrono::DateTime<chrono::Utc>, remaining: i64) -> InstantError {
    let now = chrono::Utc::now();
    let retry_after = ((retry_at - now).num_milliseconds() as f64 / 1000.0)
        .ceil()
        .max(0.0) as i64;
    InstantError::new(
        "rate-limited",
        429,
        "Your request exceeded the rate limit.",
        Some(json!({
            "retry-at": retry_at.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
            "retry-after": retry_after,
            "remaining-tokens": remaining,
        })),
    )
}

/// Charge the recorded `limit()` calls. Buckets start full; consumption is
/// all-or-nothing across a config's bandwidths, and on exhaustion nothing is
/// taken and the request fails with `rate-limited` (retry-at from the
/// slowest bandwidth, remaining-tokens from the emptiest — bucket4j's probe).
pub async fn consume_rate_limits(env: &EvalEnv<'_>, calls: &[RateLimitCall]) -> Result<()> {
    if calls.is_empty() {
        return Ok(());
    }
    let Some(pool) = &env.request.pool else {
        return Ok(());
    };
    // coalesce repeated calls on the same bucket+key
    let mut merged: Vec<(String, Value, i64)> = vec![];
    for c in calls {
        match merged
            .iter_mut()
            .find(|(b, k, _)| *b == c.bucket && *k == c.key)
        {
            Some(m) => m.2 += c.tokens,
            None => merged.push((c.bucket.clone(), c.key.clone(), c.tokens)),
        }
    }
    let configs = env
        .rules
        .code
        .get("$rateLimits")
        .cloned()
        .unwrap_or(json!({}));
    for (bucket, key, tokens) in merged {
        let Some(config) = configs.get(&bucket) else {
            continue;
        };
        let bandwidths = parse_rate_limit_config(config);
        if bandwidths.is_empty() {
            continue;
        }
        let bkey = bucket_key(env.app_id, &bucket, config, &key);
        let mut tx = pool.begin().await?;
        consume_bucket(&mut tx, bkey, &bandwidths, tokens).await?;
        tx.commit().await?;
    }
    Ok(())
}

async fn consume_bucket(
    conn: &mut PgConnection,
    key: Uuid,
    bandwidths: &[Bandwidth],
    tokens: i64,
) -> Result<()> {
    let now = chrono::Utc::now();
    let rows = sqlx::query(
        "SELECT idx, tokens, refilled_at FROM rust_rate_limit_buckets WHERE key = $1 FOR UPDATE",
    )
    .bind(key)
    .fetch_all(&mut *conn)
    .await?;
    let mut states: Vec<(f64, chrono::DateTime<chrono::Utc>)> = bandwidths
        .iter()
        .map(|b| (b.capacity as f64, now))
        .collect();
    for row in &rows {
        let idx: i32 = row.get("idx");
        if let Some(s) = states.get_mut(idx as usize) {
            *s = (row.get("tokens"), row.get("refilled_at"));
        }
    }
    // refill
    for (b, (tok, refilled_at)) in bandwidths.iter().zip(states.iter_mut()) {
        let elapsed = (now - *refilled_at).num_milliseconds().max(0) as f64 / 1000.0;
        if b.greedy {
            *tok = (*tok + elapsed * b.refill_amount as f64 / b.period_secs).min(b.capacity as f64);
            *refilled_at = now;
        } else {
            let periods = (elapsed / b.period_secs).floor();
            if periods >= 1.0 {
                *tok = (*tok + periods * b.refill_amount as f64).min(b.capacity as f64);
                *refilled_at +=
                    chrono::Duration::milliseconds((periods * b.period_secs * 1000.0) as i64);
            }
        }
    }
    let need = tokens as f64;
    let short = states.iter().any(|(tok, _)| *tok < need);
    if short {
        let mut wait_secs = 0.0f64;
        let mut remaining = i64::MAX;
        for (b, (tok, refilled_at)) in bandwidths.iter().zip(states.iter()) {
            remaining = remaining.min(tok.floor() as i64);
            if *tok >= need {
                continue;
            }
            let deficit = need - *tok;
            let w = if b.greedy {
                deficit * b.period_secs / b.refill_amount as f64
            } else {
                let periods = (deficit / b.refill_amount as f64).ceil();
                let next = *refilled_at
                    + chrono::Duration::milliseconds((periods * b.period_secs * 1000.0) as i64);
                (next - now).num_milliseconds().max(0) as f64 / 1000.0
            };
            wait_secs = wait_secs.max(w);
        }
        let retry_at = now + chrono::Duration::milliseconds((wait_secs * 1000.0).ceil() as i64);
        return Err(rate_limited_error(retry_at, remaining.max(0)));
    }
    for (idx, (tok, refilled_at)) in states.iter().enumerate() {
        sqlx::query(
            "INSERT INTO rust_rate_limit_buckets (key, idx, tokens, refilled_at, updated_at)
             VALUES ($1, $2, $3, $4, now())
             ON CONFLICT (key, idx) DO UPDATE
               SET tokens = EXCLUDED.tokens, refilled_at = EXCLUDED.refilled_at, updated_at = now()",
        )
        .bind(key)
        .bind(idx as i32)
        .bind(*tok - need)
        .bind(*refilled_at)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Entity maps

/// Build an entity map {label: value} for rule eval; includes id and defaults
/// every cardinality-one label of the etype to null. Legacy's `data` map is
/// built from `ea-ids-for-etype` (attr.clj:954-958), which keeps every
/// cardinality-one attr — forward links included, as the linked id string —
/// and drops cardinality-many ones.
pub fn base_entity_map(attrs: &AttrMap, etype: &str, eid: Uuid) -> Map<String, Value> {
    let mut m = Map::new();
    for a in attrs.attrs_of_etype(etype) {
        if a.cardinality == Cardinality::One {
            m.insert(a.label.clone(), Value::Null);
        }
    }
    m.insert("id".to_string(), json!(eid));
    m
}

pub async fn fetch_entity_map(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &AttrMap,
    etype: &str,
    eid: Uuid,
) -> Result<Option<Map<String, Value>>> {
    let attr_ids: Vec<Uuid> = attrs.attrs_of_etype(etype).map(|a| a.id).collect();
    let rows = sqlx::query(
        "SELECT attr_id, value FROM triples
         WHERE app_id = $1 AND entity_id = $2 AND attr_id = ANY($3)",
    )
    .bind(app_id)
    .bind(eid)
    .bind(&attr_ids)
    .fetch_all(&mut *conn)
    .await?;
    if rows.is_empty() {
        return Ok(None);
    }
    let mut m = base_entity_map(attrs, etype, eid);
    for row in rows {
        let attr_id: Uuid = row.get("attr_id");
        if let Some(a) = attrs.get(&attr_id) {
            if a.cardinality == Cardinality::One {
                m.insert(a.label.clone(), row.get("value"));
            }
        }
    }
    Ok(Some(m))
}

/// Attach prefetched _refs for the given paths.
async fn attach_refs(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &AttrMap,
    etype: &str,
    eid: Uuid,
    paths: &[String],
    map: &mut Map<String, Value>,
) -> Result<()> {
    if paths.is_empty() {
        return Ok(());
    }
    let mut refs = match map.remove("_refs") {
        Some(Value::Object(existing)) => existing,
        _ => Map::new(),
    };
    for p in paths {
        // a path already snapshotted (pre-tx, for update / delete / link
        // checks) keeps its snapshot
        if refs.contains_key(p) {
            continue;
        }
        let v = resolve_ref_path(conn, app_id, attrs, etype, eid, p).await?;
        refs.insert(p.clone(), v);
    }
    map.insert("_refs".to_string(), Value::Object(refs));
    Ok(())
}

/// Build the `auth` value (with auth.ref prefetch, paths start with "$user.").
/// Legacy binds `auth` as an `AuthCelMap` for every rule evaluation
/// (cel.clj:279-293, :556-580), so `$files` / `$streams` checks and the
/// debug routes go through here too.
pub async fn build_auth_value(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &AttrMap,
    auth: &AuthCtx,
    programs: &[&Program],
) -> Result<Value> {
    let Some(uid) = auth.user_id else {
        return Ok(Value::Null);
    };
    let mut m = match &auth.user_map {
        Some(Value::Object(m)) => m.clone(),
        _ => match fetch_entity_map(conn, app_id, attrs, "$users", uid).await? {
            Some(m) => m,
            None => base_entity_map(attrs, "$users", uid),
        },
    };
    let sources: Vec<&str> = programs.iter().flat_map(|p| p.all_sources()).collect();
    let paths = extract_ref_paths(&sources, "auth");
    let stripped: Vec<String> = paths
        .iter()
        .filter_map(|p| p.strip_prefix("$user.").map(|s| s.to_string()))
        .collect();
    if !stripped.is_empty() {
        let mut refs = Map::new();
        for (orig, path) in paths.iter().zip(stripped.iter()) {
            let v = resolve_ref_path(conn, app_id, attrs, "$users", uid, path).await?;
            refs.insert(orig.clone(), v);
        }
        m.insert("_refs".to_string(), Value::Object(refs));
    }
    Ok(Value::Object(m))
}

// ---------------------------------------------------------------------------
// Query filtering

pub struct PermsFilter<'a> {
    pub rules: &'a Rules,
    pub auth: &'a AuthCtx,
    pub rule_params: Value,
}

impl<'a> PermsFilter<'a> {
    /// Filters a query result in place: entities whose `view` rule fails are
    /// removed (children of removed parents go with them).
    pub async fn filter(
        &self,
        conn: &mut PgConnection,
        app_id: Uuid,
        attrs: &AttrMap,
        result: &mut QueryResult,
    ) -> Result<()> {
        self.filter_with_forms(conn, app_id, attrs, result, &[])
            .await
    }

    /// Like [`PermsFilter::filter`], with the parsed query the result came
    /// from. Legacy evaluates `view` and field rules on the whole entity even
    /// when the query projected `fields` (instaql.clj:1956-2007
    /// `preload-entity-maps` re-fetches every checked entity in full), so a
    /// projected node is re-read before its rules run.
    pub async fn filter_with_forms(
        &self,
        conn: &mut PgConnection,
        app_id: Uuid,
        attrs: &AttrMap,
        result: &mut QueryResult,
        forms: &[crate::instaql::Form],
    ) -> Result<()> {
        for form in &mut result.forms {
            let parsed = forms.iter().find(|f| f.k == form.k);
            let mut kept = vec![];
            let entities = std::mem::take(&mut form.entities);
            for mut node in entities {
                if self
                    .check_view_node(conn, app_id, attrs, &mut node, parsed)
                    .await?
                {
                    kept.push(node);
                }
            }
            form.entities = kept;
        }
        Ok(())
    }

    fn check_view_node<'b>(
        &'b self,
        conn: &'b mut PgConnection,
        app_id: Uuid,
        attrs: &'b AttrMap,
        node: &'b mut crate::instaql::EntityNode,
        form: Option<&'b crate::instaql::Form>,
    ) -> futures::future::BoxFuture<'b, Result<bool>> {
        Box::pin(async move {
            let projected = form.map(|f| f.opts.fields.is_some()).unwrap_or(false);
            let program = self.rules.program(&node.etype, "view");
            let has_field_rules = self.rules.has_field_rules(&node.etype);
            // the `data` binding: the whole entity (re-fetched when the
            // query projected it)
            let entity_map = if program.is_true() && !has_field_rules {
                None
            } else if projected {
                Some(
                    fetch_entity_map(conn, app_id, attrs, &node.etype, node.eid)
                        .await?
                        .unwrap_or_else(|| base_entity_map(attrs, &node.etype, node.eid)),
                )
            } else {
                let mut data = base_entity_map(attrs, &node.etype, node.eid);
                for t in &node.triples {
                    if let Some(a) = attrs.get(&t.a) {
                        if a.cardinality == Cardinality::One {
                            data.insert(a.label.clone(), t.v.clone());
                        }
                    }
                }
                Some(data)
            };
            let ok = if program.is_true() {
                true
            } else {
                let mut data = entity_map.clone().unwrap_or_default();
                let sources: Vec<&str> = program.all_sources();
                let data_paths = extract_ref_paths(&sources, "data");
                attach_refs(
                    conn,
                    app_id,
                    attrs,
                    &node.etype,
                    node.eid,
                    &data_paths,
                    &mut data,
                )
                .await?;
                let auth_val =
                    build_auth_value(conn, app_id, attrs, self.auth, &[&program]).await?;
                let env = EvalEnv::new(app_id, self.rules, &self.auth.request);
                eval_program(
                    &program,
                    &Value::Object(data),
                    None,
                    &auth_val,
                    &self.rule_params,
                    &env,
                )
                .await?
            };
            if !ok {
                return Ok(false);
            }
            // field-level rules: drop triples whose field program fails
            if has_field_rules {
                let data_val = Value::Object(entity_map.unwrap_or_default());
                let mut keep = Vec::with_capacity(node.triples.len());
                for t in std::mem::take(&mut node.triples) {
                    let label = attrs.get(&t.a).map(|a| a.label.clone());
                    let allowed = match label.as_deref() {
                        Some("id") | None => true,
                        Some(label) => match self.rules.field_program(&node.etype, label) {
                            None => true,
                            Some(program) => {
                                let auth_val =
                                    build_auth_value(conn, app_id, attrs, self.auth, &[&program])
                                        .await?;
                                let env = EvalEnv::new(app_id, self.rules, &self.auth.request);
                                eval_program(
                                    &program,
                                    &data_val,
                                    None,
                                    &auth_val,
                                    &self.rule_params,
                                    &env,
                                )
                                .await?
                            }
                        },
                    };
                    if allowed {
                        keep.push(t);
                    }
                }
                node.triples = keep;
            }
            for child in &mut node.children {
                let child_form = form.and_then(|f| f.children.iter().find(|cf| cf.k == child.k));
                let mut kept = vec![];
                let entities = std::mem::take(&mut child.entities);
                let mut kept_ids = HashSet::new();
                for mut n in entities {
                    if self
                        .check_view_node(conn, app_id, attrs, &mut n, child_form)
                        .await?
                    {
                        kept_ids.insert(n.eid);
                        kept.push(n);
                    }
                }
                child.link_triples.retain(|t| {
                    let child_id = if t.e == node.eid {
                        t.v.as_str().and_then(|s| Uuid::parse_str(s).ok())
                    } else {
                        Some(t.e)
                    };
                    child_id.map(|c| kept_ids.contains(&c)).unwrap_or(false)
                });
                child.entities = kept;
            }
            Ok(true)
        })
    }
}

// ---------------------------------------------------------------------------
// Permissioned transact

enum Check {
    Create {
        etype: String,
        eid: Uuid,
    },
    Update {
        etype: String,
        eid: Uuid,
        old: Map<String, Value>,
    },
    Delete {
        etype: String,
        eid: Uuid,
        old: Map<String, Value>,
    },
    ViewLinked {
        etype: String,
        eid: Uuid,
    },
    /// explicit [etype allow link/unlink <label>] rule on one link side
    LinkRule {
        action: &'static str, // "link" | "unlink"
        etype: String,
        eid: Uuid,
        /// pre-tx entity map (with its `data.ref` snapshot); None when this
        /// tx creates the entity
        old: Option<Map<String, Value>>,
        linked_etype: String,
        linked_eid: Uuid,
        /// pre-tx map of the linked entity (with its `linkedData.ref`
        /// snapshot); None when this tx creates it
        linked_old: Option<Map<String, Value>>,
        program: Program,
    },
}

/// A link step with an explicit rule, noted in the pre-pass and turned into
/// a [`Check::LinkRule`] once the pre-tx snapshots are complete.
struct LinkSpec {
    action: &'static str,
    etype: String,
    eid: Uuid,
    linked_etype: String,
    linked_eid: Uuid,
    program: Program,
}

/// `hint.input` of a failed object check: legacy `run-checks!`
/// (permissioned_transaction.clj:613-631) asserts with `[etype scope]`, and
/// the scope of an entity check is always `object`.
fn object_denied(etype: &str) -> InstantError {
    InstantError::permission_denied(
        json!([etype, "object"]),
        "Permission denied: not perms-pass?",
    )
}

/// A binding as the debug routes echo it: the internal `_refs` prefetch is
/// not part of the wire shape.
fn binding_value(v: &Value) -> Value {
    match v {
        Value::Object(m) => {
            let mut out = m.clone();
            out.remove("_refs");
            Value::Object(out)
        }
        other => other.clone(),
    }
}

/// Run tx-steps with permission checks. Must be called inside an open DB tx;
/// returns Err (aborting) when a check fails.
pub async fn permissioned_transact(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &mut AttrMap,
    steps: Vec<TxStep>,
    rules: &Rules,
    auth: &AuthCtx,
    global_rule_params: &Value,
) -> Result<TxReport> {
    let (report, _checks) = permissioned_transact_checked(
        conn,
        app_id,
        attrs,
        steps,
        rules,
        auth,
        global_rule_params,
        true,
    )
    .await?;
    Ok(report)
}

/// Like permissioned_transact but returns per-check results; with
/// `fail_fast` false, failing checks are recorded instead of aborting
/// (used by /admin/transact_perms_check dry runs).
///
/// Order follows legacy `transact!` (permissioned_transaction.clj:625-745):
/// the pre-tx entity maps and every `data.ref` / `linkedData.ref` an update,
/// delete, link or unlink rule can read are snapshotted before the steps
/// run, so those checks see the pre-tx link graph; create checks (and the
/// `attrs` create check for inline add-attr steps) run against the post-tx
/// state.
#[allow(clippy::too_many_arguments)]
pub async fn permissioned_transact_checked(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &mut AttrMap,
    steps: Vec<TxStep>,
    rules: &Rules,
    auth: &AuthCtx,
    global_rule_params: &Value,
    fail_fast: bool,
) -> Result<(TxReport, Vec<Value>)> {
    // ---- pre-pass: snapshot old entity data + note link targets ----
    let mut old_maps: HashMap<(Uuid, String), Option<Map<String, Value>>> = HashMap::new();
    let mut link_targets: Vec<(Uuid, String)> = vec![]; // entities linked-to (view check)
    let mut delete_seeds: Vec<(Uuid, String)> = vec![];
    // explicit link/unlink checks; (eid, etype) pairs whose only touches are
    // explicitly-ruled ref steps skip the generic create / update fallback
    let mut link_specs: Vec<LinkSpec> = vec![];
    let mut explicit_ref_touch: HashSet<(Uuid, String)> = HashSet::new();
    let mut other_touch: HashSet<(Uuid, String)> = HashSet::new();

    // resolve an EidRef without creating
    async fn peek_eid(
        conn: &mut PgConnection,
        app_id: Uuid,
        attrs: &AttrMap,
        eid: &EidRef,
    ) -> Result<Option<Uuid>> {
        match eid {
            EidRef::Id(id) => Ok(Some(*id)),
            EidRef::Lookup(attr_id, v) => {
                let row = sqlx::query(
                    "SELECT entity_id FROM triples
                     WHERE app_id = $1 AND attr_id = $2 AND av
                       AND json_null_to_null(value) IS NOT DISTINCT FROM json_null_to_null($3::jsonb)",
                )
                .bind(app_id)
                .bind(attr_id)
                .bind(&v.0)
                .fetch_optional(&mut *conn)
                .await?;
                let _ = attrs;
                Ok(row.map(|r| r.get("entity_id")))
            }
        }
    }

    for step in &steps {
        match step {
            TxStep::AddTriple {
                eid,
                attr_id,
                value,
                ..
            }
            | TxStep::DeepMergeTriple {
                eid,
                attr_id,
                value,
                ..
            }
            | TxStep::RetractTriple {
                eid,
                attr_id,
                value,
            } => {
                let Some(attr) = attrs.get(attr_id).cloned() else {
                    continue;
                };
                if let Some(e) = peek_eid(conn, app_id, attrs, eid).await? {
                    let key = (e, attr.etype.clone());
                    if let std::collections::hash_map::Entry::Vacant(slot) = old_maps.entry(key) {
                        let m = fetch_entity_map(conn, app_id, attrs, &attr.etype, e).await?;
                        slot.insert(m);
                    }
                }
                if attr.value_type == ValueType::Ref {
                    let action = if matches!(step, TxStep::RetractTriple { .. }) {
                        "unlink"
                    } else {
                        "link"
                    };
                    let fwd_eid = peek_eid(conn, app_id, attrs, eid).await?;
                    let target = value.as_str().and_then(|s| Uuid::parse_str(s).ok());
                    let retype = attr.reverse_etype.clone().unwrap_or_default();
                    let rlabel = attr.reverse_label.clone().unwrap_or_default();
                    let fwd_prog = rules.link_program(&attr.etype, action, &attr.label);
                    let rev_prog = rules.link_program(&retype, action, &rlabel);
                    if fwd_prog.is_some() || rev_prog.is_some() {
                        // explicit rule on at least one side: replaces fallback
                        if let (Some(e), Some(t)) = (fwd_eid, target) {
                            explicit_ref_touch.insert((e, attr.etype.clone()));
                            // snapshot the linked side too
                            let tkey = (t, retype.clone());
                            if !old_maps.contains_key(&tkey) {
                                let m = fetch_entity_map(conn, app_id, attrs, &retype, t).await?;
                                old_maps.insert(tkey.clone(), m);
                            }
                            if let Some(p) = fwd_prog {
                                link_specs.push(LinkSpec {
                                    action,
                                    etype: attr.etype.clone(),
                                    eid: e,
                                    linked_etype: retype.clone(),
                                    linked_eid: t,
                                    program: p,
                                });
                            }
                            if let Some(p) = rev_prog {
                                link_specs.push(LinkSpec {
                                    action,
                                    etype: retype.clone(),
                                    eid: t,
                                    linked_etype: attr.etype.clone(),
                                    linked_eid: e,
                                    program: p,
                                });
                            }
                        }
                    } else if let Some(t) = target {
                        if !retype.is_empty() {
                            link_targets.push((t, retype.clone()));
                        }
                        if let Some(e) = fwd_eid {
                            other_touch.insert((e, attr.etype.clone()));
                        }
                    }
                } else if let Some(e) = peek_eid(conn, app_id, attrs, eid).await? {
                    other_touch.insert((e, attr.etype.clone()));
                }
            }
            TxStep::DeleteEntity { eid, etype } => {
                if let Some(e) = peek_eid(conn, app_id, attrs, eid).await? {
                    match etype {
                        Some(et) => delete_seeds.push((e, et.clone())),
                        None => {
                            for et in crate::triple::resolve_etypes_for_delete(
                                &mut *conn, app_id, attrs, e,
                            )
                            .await?
                            {
                                delete_seeds.push((e, et));
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    // expand cascades pre-tx and snapshot delete targets
    let delete_set =
        crate::triple::expand_delete_cascade(&mut *conn, app_id, attrs, &delete_seeds).await?;
    for (e, et) in &delete_set {
        let key = (*e, et.clone());
        if let std::collections::hash_map::Entry::Vacant(slot) = old_maps.entry(key) {
            let m = fetch_entity_map(conn, app_id, attrs, et, *e).await?;
            slot.insert(m);
        }
    }

    // ---- pre-tx ref snapshots (permissioned_transaction.clj:697-715) ----
    // update / delete / link / unlink checks read `data.ref(...)` (and
    // `linkedData.ref(...)`) against the graph as it was before the steps
    // ran; the paths are resolved now and kept on the snapshot maps
    {
        let mut wanted: Vec<((Uuid, String), Vec<String>)> = vec![];
        for key in &other_touch {
            let program = rules.program(&key.1, "update");
            let paths = extract_ref_paths(&program.all_sources(), "data");
            if !paths.is_empty() {
                wanted.push((key.clone(), paths));
            }
        }
        for (e, et) in &delete_set {
            let program = rules.program(et, "delete");
            let paths = extract_ref_paths(&program.all_sources(), "data");
            if !paths.is_empty() {
                wanted.push(((*e, et.clone()), paths));
            }
        }
        for spec in &link_specs {
            let sources = spec.program.all_sources();
            let data_paths = extract_ref_paths(&sources, "data");
            if !data_paths.is_empty() {
                wanted.push(((spec.eid, spec.etype.clone()), data_paths));
            }
            let linked_paths = extract_ref_paths(&sources, "linkedData");
            if !linked_paths.is_empty() {
                wanted.push(((spec.linked_eid, spec.linked_etype.clone()), linked_paths));
            }
        }
        for (key, paths) in wanted {
            if let Some(Some(map)) = old_maps.get_mut(&key) {
                attach_refs(conn, app_id, attrs, &key.1, key.0, &paths, map).await?;
            }
        }
    }

    // attr-scope checks (permissioned_transaction.clj:338-352): update /
    // delete / restore-attr carry `{:result admin?}`, and this path is only
    // taken for non-admins, so they always fail
    let mut check_results: Vec<Value> = vec![];
    for step in &steps {
        let action = match step {
            TxStep::UpdateAttr(_) => "update",
            TxStep::DeleteAttr(_) => "delete",
            TxStep::RestoreAttr(_) => "restore",
            _ => continue,
        };
        check_results.push(json!({
            "scope": "attr",
            "etype": "attrs",
            "action": action,
            "check-result": false,
            "check-pass?": false,
            "program": {"result": false},
        }));
        if fail_fast {
            return Err(InstantError::permission_denied(
                json!(["attrs", "attr"]),
                "Permission denied: not perms-pass?",
            ));
        }
    }

    // request.modifiedFields inputs: (eid ref, attr) of every add / deep-merge
    // step; lookup eids resolve after the tx ran
    let write_refs: Vec<(EidRef, Uuid)> = steps
        .iter()
        .filter_map(|s| match s {
            TxStep::AddTriple { eid, attr_id, .. }
            | TxStep::DeepMergeTriple { eid, attr_id, .. } => Some((eid.clone(), *attr_id)),
            _ => None,
        })
        .collect();
    // inline add-attr steps: checked post-tx against `attrs.allow.create`
    // (permissioned_transaction.clj:519-527)
    let added_attrs: Vec<Value> = steps
        .iter()
        .filter_map(|s| match s {
            TxStep::AddAttr(a) => Some(a.to_wire()),
            _ => None,
        })
        .collect();

    // ---- execute ----
    let report = tx::transact(conn, app_id, attrs, steps, &TxOptions::default()).await?;
    let writes: Vec<(Uuid, Uuid)> = write_refs
        .into_iter()
        .filter_map(|(eid, attr_id)| match eid {
            EidRef::Id(id) => Some((id, attr_id)),
            EidRef::Lookup(a, v) => report
                .resolved_lookups
                .get(&(a, v))
                .map(|id| (*id, attr_id)),
        })
        .collect();

    // ---- collect checks ----
    let mut checks: Vec<Check> = vec![];
    let created: HashSet<(Uuid, String)> = report.created.iter().cloned().collect();
    for (e, et) in &report.created {
        let key = (*e, et.clone());
        if explicit_ref_touch.contains(&key) && !other_touch.contains(&key) {
            // an entity brought into being by a link step alone runs the
            // link rule (with actions.data == "create"), not the create rule
            continue;
        }
        checks.push(Check::Create {
            etype: et.clone(),
            eid: *e,
        });
    }
    let deleted: HashSet<(Uuid, String)> = report.deleted.iter().cloned().collect();
    for (e, et) in &report.deleted {
        let old = old_maps
            .get(&(*e, et.clone()))
            .cloned()
            .flatten()
            .unwrap_or_else(|| base_entity_map(attrs, et, *e));
        checks.push(Check::Delete {
            etype: et.clone(),
            eid: *e,
            old,
        });
    }
    let mut update_seen = HashSet::new();
    for (e, et) in &report.touched {
        let key = (*e, et.clone());
        if created.contains(&key) || deleted.contains(&key) || !update_seen.insert(key.clone()) {
            continue;
        }
        if explicit_ref_touch.contains(&key) && !other_touch.contains(&key) {
            continue; // link rule replaces the update fallback for this entity
        }
        let old = old_maps
            .get(&key)
            .cloned()
            .flatten()
            .unwrap_or_else(|| base_entity_map(attrs, et, *e));
        checks.push(Check::Update {
            etype: et.clone(),
            eid: *e,
            old,
        });
    }
    for spec in link_specs {
        let old = old_maps
            .get(&(spec.eid, spec.etype.clone()))
            .cloned()
            .flatten();
        let linked_old = old_maps
            .get(&(spec.linked_eid, spec.linked_etype.clone()))
            .cloned()
            .flatten();
        checks.push(Check::LinkRule {
            action: spec.action,
            etype: spec.etype,
            eid: spec.eid,
            old,
            linked_etype: spec.linked_etype,
            linked_eid: spec.linked_eid,
            linked_old,
            program: spec.program,
        });
    }
    let mut linked_seen = HashSet::new();
    for (e, et) in link_targets {
        let key = (e, et.clone());
        if created.contains(&key) || !linked_seen.insert(key.clone()) {
            continue;
        }
        checks.push(Check::ViewLinked { etype: et, eid: e });
    }

    // merged rule params for an entity: step-level overrides global
    let rule_params_for = |eid: Uuid, etype: &str| -> Value {
        let mut rp = match global_rule_params {
            Value::Object(m) => m.clone(),
            _ => Map::new(),
        };
        if let Some(Value::Object(step_rp)) = report.rule_params.get(&(eid, etype.to_string())) {
            for (k, v) in step_rp {
                rp.insert(k.clone(), v.clone());
            }
        }
        Value::Object(rp)
    };

    // ---- evaluate ----
    for check in checks {
        let (action, etype, eid, data, new_data) = match &check {
            Check::Create { etype, eid } => {
                let new = fetch_entity_map(conn, app_id, attrs, etype, *eid)
                    .await?
                    .unwrap_or_else(|| base_entity_map(attrs, etype, *eid));
                ("create", etype.clone(), *eid, Value::Object(new), None)
            }
            Check::Update { etype, eid, old } => {
                let new = fetch_entity_map(conn, app_id, attrs, etype, *eid)
                    .await?
                    .unwrap_or_else(|| base_entity_map(attrs, etype, *eid));
                (
                    "update",
                    etype.clone(),
                    *eid,
                    Value::Object(old.clone()),
                    Some(Value::Object(new)),
                )
            }
            Check::Delete { etype, eid, old } => (
                "delete",
                etype.clone(),
                *eid,
                Value::Object(old.clone()),
                None,
            ),
            Check::ViewLinked { etype, eid } => {
                let m = fetch_entity_map(conn, app_id, attrs, etype, *eid)
                    .await?
                    .unwrap_or_else(|| base_entity_map(attrs, etype, *eid));
                ("view", etype.clone(), *eid, Value::Object(m), None)
            }
            Check::LinkRule {
                action,
                etype,
                eid,
                old,
                linked_etype,
                linked_eid,
                linked_old,
                program,
            } => {
                let created_here = old.is_none();
                if created_here && *action == "unlink" {
                    // nothing to unlink from an entity that did not exist
                    continue;
                }
                let sources = program.all_sources();
                let data_paths = extract_ref_paths(&sources, "data");
                let linked_paths = extract_ref_paths(&sources, "linkedData");
                let new = fetch_entity_map(conn, app_id, attrs, etype, *eid)
                    .await?
                    .unwrap_or_else(|| base_entity_map(attrs, etype, *eid));
                // pre-checks (:344-374) bind the pre-tx entity as `data`;
                // post-create checks (:528-560) bind the created entity
                let mut data = match old {
                    Some(o) => o.clone(),
                    None => new.clone(),
                };
                attach_refs(conn, app_id, attrs, etype, *eid, &data_paths, &mut data).await?;
                let mut linked = match linked_old {
                    Some(l) => l.clone(),
                    None => fetch_entity_map(conn, app_id, attrs, linked_etype, *linked_eid)
                        .await?
                        .unwrap_or_else(|| base_entity_map(attrs, linked_etype, *linked_eid)),
                };
                attach_refs(
                    conn,
                    app_id,
                    attrs,
                    linked_etype,
                    *linked_eid,
                    &linked_paths,
                    &mut linked,
                )
                .await?;
                let actions = (*action == "link").then(|| {
                    json!({
                        "data": if created_here { "create" } else { "update" },
                        "linkedData": if linked_old.is_some() { "update" } else { "create" },
                    })
                });
                let data = Value::Object(data);
                let new_data = Value::Object(new);
                let linked = Value::Object(linked);
                let auth_val = build_auth_value(conn, app_id, attrs, auth, &[program]).await?;
                // legacy merges the linked side's rule-params under the
                // entity's own (:360, :372)
                let mut rp = match rule_params_for(*linked_eid, linked_etype) {
                    Value::Object(m) => m,
                    _ => Map::new(),
                };
                if let Value::Object(own) = rule_params_for(*eid, etype) {
                    for (k, v) in own {
                        rp.insert(k, v);
                    }
                }
                let rp = Value::Object(rp);
                let env = EvalEnv::new(app_id, rules, &auth.request);
                let ok = eval_program_full(
                    program,
                    &data,
                    Some(&new_data),
                    &auth_val,
                    &rp,
                    Some(&linked),
                    actions.as_ref(),
                    &env,
                )
                .await?;
                let mut bindings = json!({
                    "data": binding_value(&data),
                    "new-data": binding_value(&new_data),
                    "linked-data": binding_value(&linked),
                    "linked-etype": linked_etype,
                    "rule-params": rp,
                });
                if let Some(a) = &actions {
                    bindings["actions"] = a.clone();
                }
                check_results.push(json!({
                    "scope": "object",
                    "etype": etype,
                    "action": action,
                    "eid": eid,
                    "bindings": bindings,
                    "check-result": ok,
                    "check-pass?": ok,
                    "program": {
                        "etype": etype,
                        "action": action,
                        "code": program.expr,
                        "display-code": program.expr,
                    },
                }));
                if !ok && fail_fast {
                    return Err(object_denied(etype));
                }
                continue;
            }
        };
        let program = rules.program(&etype, action);
        // legacy passes modified-fields only to create / update checks
        let modified_fields = if matches!(action, "create" | "update") {
            Some(modified_fields_for(&writes, attrs, eid))
        } else {
            None
        };
        let rp = rule_params_for(eid, &etype);
        let mut bindings = json!({
            "data": binding_value(&data),
            "rule-params": rp,
        });
        match action {
            "create" => {
                bindings["new-data"] = binding_value(&data);
            }
            "update" => {
                bindings["new-data"] = new_data.as_ref().map(binding_value).unwrap_or(Value::Null);
            }
            _ => {}
        }
        if let Some(mf) = &modified_fields {
            bindings["modified-fields"] = json!(mf);
        }
        if program.is_true() {
            check_results.push(json!({
                "scope": "object",
                "etype": etype,
                "action": action,
                "eid": eid,
                "bindings": bindings,
                "check-result": true,
                "check-pass?": true,
                "program": {
                    "etype": etype,
                    "action": action,
                    "code": program.expr,
                    "display-code": program.expr,
                },
            }));
            continue;
        }
        // data refs prefetch (update / delete snapshots already carry their
        // pre-tx paths; create / view read the post-tx graph)
        let mut data = data;
        if let Value::Object(ref mut m) = data {
            let sources = program.all_sources();
            let paths = extract_ref_paths(&sources, "data");
            attach_refs(conn, app_id, attrs, &etype, eid, &paths, m).await?;
        }
        let auth_val = build_auth_value(conn, app_id, attrs, auth, &[&program]).await?;
        let env = EvalEnv::new(app_id, rules, &auth.request)
            .with_modified_fields(modified_fields.unwrap_or_default());
        let ok = eval_program(&program, &data, new_data.as_ref(), &auth_val, &rp, &env).await?;
        check_results.push(json!({
            "scope": "object",
            "etype": etype,
            "action": action,
            "eid": eid,
            "bindings": bindings,
            "check-result": ok,
            "check-pass?": ok,
            "program": {
                "etype": etype,
                "action": action,
                "code": program.expr,
                "display-code": program.expr,
            },
        }));
        if !ok && fail_fast {
            return Err(object_denied(&etype));
        }
    }

    // ---- attrs.allow.create for inline add-attr steps ----
    // (permissioned_transaction.clj:519-527: `data` is the attr map itself;
    // rule.clj:278-290 walks attrs.allow.create -> attrs.allow.$default ->
    // $default.allow.create -> $default.allow.$default, else allow)
    for attr_map in added_attrs {
        let program = rules.program("attrs", "create");
        let ok = if program.is_true() {
            true
        } else {
            let auth_val = build_auth_value(conn, app_id, attrs, auth, &[&program]).await?;
            let env = EvalEnv::new(app_id, rules, &auth.request);
            eval_program(
                &program,
                &attr_map,
                None,
                &auth_val,
                global_rule_params,
                &env,
            )
            .await?
        };
        check_results.push(json!({
            "scope": "attr",
            "etype": "attrs",
            "action": "create",
            "bindings": {"data": attr_map},
            "check-result": ok,
            "check-pass?": ok,
            "program": {
                "etype": "attrs",
                "action": "create",
                "code": program.expr,
                "display-code": program.expr,
            },
        }));
        if !ok && fail_fast {
            return Err(InstantError::permission_denied(
                json!(["attrs", "attr"]),
                "Permission denied: not perms-pass?",
            ));
        }
    }

    Ok((report, check_results))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eval(expr: &str, data: Value, auth: Value, rule_params: Value) -> bool {
        let program = Program {
            expr: expr.to_string(),
            binds: vec![],
            rule: None,
        };
        let rules = Rules { code: json!({}) };
        let request = RequestCtx::default();
        let env = EvalEnv::new(Uuid::nil(), &rules, &request);
        eval_program_pure(&program, &data, None, &auth, &rule_params, None, None, &env)
            .unwrap()
            .0
    }

    #[test]
    fn missing_keys_resolve_to_null() {
        assert!(eval(
            "data.someTypo == null",
            json!({"id": "1", "title": "t"}),
            Value::Null,
            json!({})
        ));
        assert!(eval(
            "ruleParams.unknown == null",
            json!({}),
            Value::Null,
            json!({})
        ));
        assert!(eval(
            "data['someKey'] == null",
            json!({}),
            Value::Null,
            json!({})
        ));
    }

    #[test]
    fn typoed_key_denies_by_false_not_error() {
        // rule evaluates to null -> non-boolean -> deny, not an error
        assert!(!eval(
            "data.someTypo",
            json!({"id": "1"}),
            Value::Null,
            json!({})
        ));
    }

    #[test]
    fn has_and_in_lie_like_legacy() {
        assert!(eval(
            "has(data.notThere)",
            json!({}),
            Value::Null,
            json!({})
        ));
        assert!(eval(
            "'notThere' in data",
            json!({}),
            Value::Null,
            json!({})
        ));
    }

    #[test]
    fn nested_maps_are_null_safe() {
        assert!(eval(
            "data.profile.missing == null",
            json!({"profile": {"name": "a"}}),
            Value::Null,
            json!({})
        ));
        // chained access through a null value is an evaluation error, like
        // legacy's permission-evaluation-failed (never a silent deny)
        let program = Program {
            expr: "data.missing.b == null".to_string(),
            binds: vec![],
            rule: Some(("t".to_string(), "view".to_string())),
        };
        let rules = Rules { code: json!({}) };
        let request = RequestCtx::default();
        let env = EvalEnv::new(Uuid::nil(), &rules, &request);
        let err = eval_program_pure(
            &program,
            &json!({}),
            None,
            &Value::Null,
            &json!({}),
            None,
            None,
            &env,
        )
        .unwrap_err();
        assert_eq!(err.error_type, "permission-evaluation-failed");
    }

    #[test]
    fn anonymous_auth_reads_as_empty_map() {
        assert!(eval("auth.id == null", json!({}), Value::Null, json!({})));
        assert!(!eval(
            "auth.id == null",
            json!({}),
            json!({"id": "u1"}),
            json!({})
        ));
    }

    #[test]
    fn actions_binding_and_extension_functions() {
        // link rules read `actions` (cel.clj:459-497); unlink rules never see it
        let program = Program {
            expr: "actions.data == 'create' && actions.linkedData == 'update'".to_string(),
            binds: vec![],
            rule: None,
        };
        let rules = Rules { code: json!({}) };
        let request = RequestCtx::default();
        let env = EvalEnv::new(Uuid::nil(), &rules, &request);
        let ok = eval_program_pure(
            &program,
            &json!({"id": "1"}),
            None,
            &Value::Null,
            &json!({}),
            Some(&json!({"id": "2"})),
            Some(&json!({"data": "create", "linkedData": "update"})),
            &env,
        )
        .unwrap()
        .0;
        assert!(ok);
        let err = eval_program_pure(
            &program,
            &json!({"id": "1"}),
            None,
            &Value::Null,
            &json!({}),
            Some(&json!({"id": "2"})),
            None,
            &env,
        )
        .unwrap_err();
        assert_eq!(err.error_type, "permission-evaluation-failed");
        // cel-java string / math extensions and the Instant timestamp overloads
        assert!(eval(
            "data.email.lowerAscii().endsWith('@example.com') && data.title.substring(0, 2) == 'Hi'",
            json!({"email": "A@Example.com", "title": "Hi there"}),
            Value::Null,
            json!({})
        ));
        assert!(eval(
            "math.greatest(data.a, data.b) == 3 && request.time.getTime() > timestamp(data.created).getTime()",
            json!({"a": 1, "b": 3, "created": 0}),
            Value::Null,
            json!({})
        ));
        assert!(eval(
            "timestamp('2020-01-01') < timestamp(data.when) && data.tags.join(',') == 'a,b'",
            json!({"when": "2021-06-01T00:00:00Z", "tags": ["a", "b"]}),
            Value::Null,
            json!({})
        ));
    }

    #[test]
    fn binds_see_null_safe_maps() {
        let program = Program {
            expr: "isOwner".to_string(),
            binds: vec![(
                "isOwner".to_string(),
                "data.creatorTypo == auth.id".to_string(),
            )],
            rule: None,
        };
        let rules = Rules { code: json!({}) };
        let request = RequestCtx::default();
        let env = EvalEnv::new(Uuid::nil(), &rules, &request);
        let ok = eval_program_pure(
            &program,
            &json!({"id": "1"}),
            None,
            &json!({"id": "u1"}),
            &json!({}),
            None,
            None,
            &env,
        )
        .unwrap()
        .0;
        assert!(!ok);
        let ok = eval_program_pure(
            &program,
            &json!({"id": "1"}),
            None,
            &Value::Null,
            &json!({}),
            None,
            None,
            &env,
        )
        .unwrap()
        .0;
        // both sides null -> equal -> allow, as on hosted Instant
        assert!(ok);
    }
}

// ---------------------------------------------------------------------------
// Rule validation (model/rule.clj validation-errors), used by the dashboard
// `POST /dash/apps/:app_id/rules` endpoint that `instant-cli push perms` hits.

fn get_in<'a>(v: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut cur = v;
    for k in path {
        cur = cur.get(*k)?;
    }
    Some(cur)
}

/// normalize-bind: `[k1 v1 k2 v2]` or `{k1: v1, k2: v2}` → flat sequence.
fn normalize_bind(bind: Option<&Value>) -> Vec<Value> {
    match bind {
        Some(Value::Array(a)) => a.clone(),
        Some(Value::Object(m)) => m
            .iter()
            .flat_map(|(k, v)| [Value::String(k.clone()), v.clone()])
            .collect(),
        _ => vec![],
    }
}

fn collect_idents(expr: &cel::IdedExpr, out: &mut HashSet<String>) {
    use cel::common::ast::{EntryExpr, Expr};
    match &expr.expr {
        Expr::Unspecified | Expr::Literal(_) => {}
        Expr::Ident(name) => {
            out.insert(name.clone());
        }
        Expr::Call(c) => {
            if let Some(t) = &c.target {
                collect_idents(t, out);
            }
            for a in &c.args {
                collect_idents(a, out);
            }
        }
        Expr::Comprehension(c) => {
            for e in [
                &c.iter_range,
                &c.accu_init,
                &c.loop_cond,
                &c.loop_step,
                &c.result,
            ] {
                collect_idents(e, out);
            }
        }
        Expr::List(l) => {
            for e in &l.elements {
                collect_idents(e, out);
            }
        }
        Expr::Map(m) => {
            for e in &m.entries {
                if let EntryExpr::MapEntry(me) = &e.expr {
                    collect_idents(&me.key, out);
                    collect_idents(&me.value, out);
                }
            }
        }
        Expr::Select(s) => collect_idents(&s.operand, out),
        Expr::Struct(st) => {
            for e in &st.entries {
                match &e.expr {
                    EntryExpr::StructField(f) => collect_idents(&f.value, out),
                    EntryExpr::MapEntry(me) => {
                        collect_idents(&me.key, out);
                        collect_idents(&me.value, out);
                    }
                }
            }
        }
    }
}

const UNEXPECTED_RULE_ERROR: &str = "There was an unexpected error evaluating the rules";

/// The cel crate decorates parse errors with position and a source excerpt
/// (`ERROR: <input>:1:11: Syntax error: <msg>\n| ...`); legacy reports the
/// bare ANTLR message, which is the same text.
fn cel_error_message(e: &cel::ParseErrors) -> String {
    let full = e.to_string();
    let core = full
        .split_once("Syntax error: ")
        .map(|(_, rest)| rest)
        .unwrap_or(full.as_str());
    core.lines().next().unwrap_or_default().trim().to_string()
}

/// expr-validation-errors + with-binds: compile the rule at `path` (with the
/// binds it references, transitively) and report compile problems.
fn expr_validation_errors(rules: &Value, etype: &str, path: &[&str]) -> Vec<Value> {
    let err = |message: String| vec![json!({"message": message, "in": path})];
    let Some(raw) = get_in(rules, path) else {
        return vec![];
    };
    let code = match raw {
        Value::Null => return vec![],
        Value::Bool(b) => b.to_string(),
        Value::String(s) => s.clone(),
        _ => return err(UNEXPECTED_RULE_ERROR.to_string()),
    };
    let mut binds = normalize_bind(get_in(rules, &["$default", "bind"]));
    binds.extend(normalize_bind(get_in(rules, &[etype, "bind"])));
    if !binds.len().is_multiple_of(2) {
        return err("bind should have an even number of elements".to_string());
    }
    let mut bind_map: HashMap<String, Value> = HashMap::new();
    for pair in binds.chunks(2) {
        let name = match &pair[0] {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        bind_map.insert(name, pair[1].clone());
    }
    let compiled = match cel::Program::compile(&code) {
        Ok(p) => p,
        Err(e) => return err(cel_error_message(&e)),
    };
    // legacy type-checks `request` as a proto struct (cel_test.clj:70-99)
    if let Some(field) = undefined_request_field(compiled.expression()) {
        return err(format!("undefined field '{field}'"));
    }
    // legacy rate-limit-validator (cel.clj:1850-1872)
    let rate_limit_keys: HashSet<String> = rules
        .get("$rateLimits")
        .and_then(|v| v.as_object())
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default();
    for name in rate_limit_names(compiled.expression()) {
        if !rate_limit_keys.contains(&name) {
            return err(format!(
                "`{name}` is not a valid rate limit config. It should be defined in the `$rateLimits` key."
            ));
        }
    }
    // Walk the binds the expression references (and the binds those
    // reference), compiling each; cycles are an error like legacy sort-binds.
    let mut queue: Vec<(String, Vec<String>)> = vec![];
    let mut idents = HashSet::new();
    collect_idents(compiled.expression(), &mut idents);
    for name in idents {
        if bind_map.contains_key(&name) {
            queue.push((name.clone(), vec![name]));
        }
    }
    let mut seen: HashSet<String> = HashSet::new();
    while let Some((name, chain)) = queue.pop() {
        if !seen.insert(name.clone()) {
            continue;
        }
        let bind_code = match &bind_map[&name] {
            Value::Bool(b) => b.to_string(),
            Value::String(s) => s.clone(),
            _ => return err(UNEXPECTED_RULE_ERROR.to_string()),
        };
        let compiled = match cel::Program::compile(&bind_code) {
            Ok(p) => p,
            Err(e) => return err(cel_error_message(&e)),
        };
        let mut refs = HashSet::new();
        collect_idents(compiled.expression(), &mut refs);
        for r in refs {
            if !bind_map.contains_key(&r) {
                continue;
            }
            if chain.contains(&r) {
                let mut cycle = chain.clone();
                cycle.push(r);
                return err(format!(
                    "The binds have a cyclic dependency {}",
                    cycle.join(" -> ")
                ));
            }
            let mut next = chain.clone();
            next.push(r.clone());
            queue.push((r, next));
        }
    }
    vec![]
}

/// Postgres-interval-ish duration parser (legacy uses PGInterval):
/// `"1 hour"`, `"30 minutes"`, `"2 days 4 hours"`, `"01:30:00"`.
fn parse_interval_secs(s: &str) -> Option<f64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let mut total = 0.0;
    let mut tokens = s.split_whitespace().peekable();
    while let Some(tok) = tokens.next() {
        if tok.contains(':') {
            let parts: Vec<&str> = tok.split(':').collect();
            if parts.len() < 2 || parts.len() > 3 {
                return None;
            }
            let h: f64 = parts[0].parse().ok()?;
            let m: f64 = parts[1].parse().ok()?;
            let sec: f64 = parts.get(2).map(|x| x.parse().ok()).unwrap_or(Some(0.0))?;
            total += h * 3600.0 + m * 60.0 + sec;
            continue;
        }
        let n: f64 = tok.parse().ok()?;
        let unit = tokens.next()?.to_lowercase();
        let mult = match unit.trim_end_matches('s') {
            "year" | "yr" | "y" => 365.0 * 86400.0,
            "month" | "mon" => 30.0 * 86400.0,
            "week" | "w" => 7.0 * 86400.0,
            "day" | "d" => 86400.0,
            "hour" | "hr" | "h" => 3600.0,
            "minute" | "min" | "m" => 60.0,
            "second" | "sec" | "" => 1.0,
            _ => return None,
        };
        total += n * mult;
    }
    Some(total)
}

fn pos_int(v: Option<&Value>) -> Option<i64> {
    v.and_then(|v| v.as_i64()).filter(|n| *n > 0)
}

/// rate_limit.clj rules-rate-limit-config->bucket-config, validation only.
/// Returns the legacy error message on failure.
fn rate_limit_config_error(config: &Value) -> Option<String> {
    let Some(limits) = config.get("limits") else {
        return Some("Missing parameter: [\"limits\"]".to_string());
    };
    let Some(limits) = limits.as_array().filter(|a| !a.is_empty()) else {
        return Some(
            "Validation failed for rules: The rate limit config must have at least one limit in the `limits` array.".to_string(),
        );
    };
    for limit in limits {
        if limit.get("capacity").is_none() {
            return Some("Missing parameter: [\"capacity\"]".to_string());
        }
        let Some(capacity) = pos_int(limit.get("capacity")) else {
            return Some("Malformed parameter: [\"capacity\"]".to_string());
        };
        let refill = limit.get("refill").cloned().unwrap_or(json!({}));
        let amount = refill.get("amount").cloned().unwrap_or(json!(capacity));
        if pos_int(Some(&amount)).is_none() {
            return Some("Malformed parameter: [\"refill\" \"amount\"]".to_string());
        }
        let rtype = refill.get("type").cloned().unwrap_or(json!("greedy"));
        if !matches!(rtype.as_str(), Some("interval") | Some("greedy")) {
            return Some("Malformed parameter: [\"refill\" \"type\"]".to_string());
        }
        let period = refill.get("period").cloned().unwrap_or(json!("1 hour"));
        let Some(secs) = period.as_str().and_then(parse_interval_secs) else {
            return Some("Malformed parameter: [\"refill\" \"period\"]".to_string());
        };
        if secs.floor() < 1.0 {
            return Some(
                "Validation failed for rules: The refill period must be longer than a second."
                    .to_string(),
            );
        }
        if secs > 24.0 * 3600.0 {
            return Some(
                "Validation failed for rules: The refill period can't be longer than a day."
                    .to_string(),
            );
        }
    }
    None
}

/// Every problem with a rules document, as `{message, in}` entries
/// (rule.clj validation-errors: binds, per-action rules, field rules,
/// $rateLimits). An empty result means the rules can be saved.
pub fn validation_errors(code: &Value) -> Vec<Value> {
    let mut errors = vec![];
    let Some(rules) = code.as_object() else {
        errors.push(json!({"message": UNEXPECTED_RULE_ERROR, "in": []}));
        return errors;
    };
    // bind-validation-errors
    for (etype, ns) in rules {
        let bind = normalize_bind(ns.get("bind"));
        if !bind.len().is_multiple_of(2) {
            errors.push(json!({
                "message": "bind should have an even number of elements",
                "in": [etype, "bind"],
            }));
            continue;
        }
        let mut seen = HashSet::new();
        for pair in bind.chunks(2) {
            let key = pair[0].to_string();
            if !seen.insert(key) {
                errors.push(json!({
                    "message": "bind should only contain a given variable name once",
                    "in": [etype, "bind", pair[0]],
                }));
                break;
            }
        }
    }
    // rule-validation-errors
    for etype in rules.keys() {
        if etype == "$rateLimits" {
            continue;
        }
        for action in ["view", "create", "update", "delete"] {
            let path = [etype.as_str(), "allow", action];
            let users_errors = if etype == "$users" && action == "delete" {
                match get_in(code, &path) {
                    Some(v) if v != &Value::String("false".into()) => Some(vec![json!({
                        "message": "The $users namespace doesn't support permissions for delete. Set `$users.allow.delete` to `\"false\"`.",
                        "in": path,
                    })]),
                    _ => None,
                }
            } else {
                None
            };
            let system_errors = if etype.starts_with('$')
                && !matches!(
                    etype.as_str(),
                    "$users" | "$files" | "$default" | "$streams" | "$rateLimits"
                ) {
                Some(vec![json!({
                    "message": format!("The {etype} namespace is a reserved internal namespace that does not yet support rules."),
                    "in": path,
                })])
            } else {
                None
            };
            let errs = users_errors
                .or(system_errors)
                .unwrap_or_else(|| expr_validation_errors(code, etype, &path));
            errors.extend(errs);
        }
    }
    // field-validation-errors
    for etype in rules.keys() {
        let Some(fields) = get_in(code, &[etype, "fields"]).and_then(|f| f.as_object()) else {
            continue;
        };
        for field in fields.keys() {
            if field == "id" {
                errors.push(json!({
                    "in": [etype, "fields"],
                    "message": format!("You cannot set field rules for `id`. Use {etype} -> allow -> view instead"),
                }));
            } else {
                errors.extend(expr_validation_errors(
                    code,
                    etype,
                    &[etype, "fields", field],
                ));
            }
        }
    }
    // rate-limit-validation-errors
    if let Some(rl) = rules.get("$rateLimits") {
        match rl.as_object() {
            None => errors
                .push(json!({"message": "$rateLimits must be an object", "in": ["$rateLimits"]})),
            Some(configs) => {
                for (name, config) in configs {
                    if let Some(message) = rate_limit_config_error(config) {
                        errors.push(json!({"message": message, "in": ["$rateLimits", name]}));
                    }
                }
            }
        }
    }
    errors
}

#[cfg(test)]
mod rule_validation_tests {
    use super::*;

    fn msgs(code: Value) -> Vec<(String, Value)> {
        validation_errors(&code)
            .into_iter()
            .map(|e| (e["message"].as_str().unwrap().to_string(), e["in"].clone()))
            .collect()
    }

    #[test]
    fn valid_rules_have_no_errors() {
        let code = json!({
            "docs": {
                "bind": ["isOwner", "auth.id != null && auth.id == data.ownerId"],
                "allow": {"view": "isOwner", "create": true, "update": "isOwner", "delete": "false"},
                "fields": {"secret": "isOwner"},
            },
            "$users": {"allow": {"view": "auth.id == data.id", "delete": "false"}},
            "$default": {"bind": {"isAdmin": "auth.email == 'admin@example.com'"}, "allow": {"$default": "isAdmin"}},
            "$rateLimits": {"burst": {"limits": [{"capacity": 10, "refill": {"period": "1 hour", "amount": 10, "type": "interval"}}]}},
        });
        assert_eq!(msgs(code), vec![]);
    }

    #[test]
    fn bind_shape_errors() {
        let code = json!({"docs": {"bind": ["a", "true", "b"]}});
        let m = msgs(code);
        assert_eq!(m[0].0, "bind should have an even number of elements");
        assert_eq!(m[0].1, json!(["docs", "bind"]));
        let code = json!({"docs": {"bind": ["a", "true", "a", "false"]}});
        let m = msgs(code);
        assert_eq!(
            m[0].0,
            "bind should only contain a given variable name once"
        );
        assert_eq!(m[0].1, json!(["docs", "bind", "a"]));
    }

    #[test]
    fn reserved_namespaces_and_users_delete() {
        let m = msgs(json!({"$magicCodes": {"allow": {"view": "true"}}}));
        assert_eq!(m.len(), 4);
        assert!(m[0]
            .0
            .contains("$magicCodes namespace is a reserved internal namespace"));
        assert_eq!(m[0].1, json!(["$magicCodes", "allow", "view"]));
        let m = msgs(json!({"$users": {"allow": {"delete": "auth.id == data.id"}}}));
        assert_eq!(m.len(), 1);
        assert!(m[0].0.contains("doesn't support permissions for delete"));
    }

    #[test]
    fn cel_compile_errors_are_reported_with_paths() {
        let m = msgs(json!({"docs": {"allow": {"view": "auth.id =="}}}));
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].1, json!(["docs", "allow", "view"]));
        // byte-identical to the legacy (cel-java) message for this input
        assert_eq!(
            m[0].0,
            "mismatched input '<EOF>' expecting {'[', '{', '(', '.', '-', '!', 'true', 'false', 'null', NUM_FLOAT, NUM_INT, NUM_UINT, STRING, BYTES, IDENTIFIER}"
        );
        let m = msgs(json!({"docs": {"allow": {"view": 42}}}));
        assert_eq!(m[0].0, UNEXPECTED_RULE_ERROR);
        let m = msgs(json!({"docs": {"fields": {"id": "true"}}}));
        assert!(m[0].0.starts_with("You cannot set field rules for `id`"));
        assert_eq!(m[0].1, json!(["docs", "fields"]));
    }

    #[test]
    fn bind_references_are_checked_transitively() {
        // unreferenced broken bind is fine (legacy only compiles used binds)
        let m = msgs(
            json!({"docs": {"bind": ["broken", "auth.id ==", "ok", "true"], "allow": {"view": "ok"}}}),
        );
        assert_eq!(m, vec![]);
        let m = msgs(json!({"docs": {"bind": ["a", "b", "b", "a"], "allow": {"view": "a"}}}));
        assert_eq!(m.len(), 1);
        assert!(m[0].0.starts_with("The binds have a cyclic dependency"));
        let m = msgs(
            json!({"docs": {"bind": ["a", "b && x", "b", "auth.id =="], "allow": {"view": "a"}}}),
        );
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].1, json!(["docs", "allow", "view"]));
    }

    #[test]
    fn rate_limit_configs() {
        let m = msgs(json!({"$rateLimits": "nope"}));
        assert_eq!(m[0].0, "$rateLimits must be an object");
        let m = msgs(json!({"$rateLimits": {"a": {}}}));
        assert_eq!(m[0].0, "Missing parameter: [\"limits\"]");
        assert_eq!(m[0].1, json!(["$rateLimits", "a"]));
        let m = msgs(json!({"$rateLimits": {"a": {"limits": []}}}));
        assert!(m[0].0.contains("at least one limit"));
        let m = msgs(json!({"$rateLimits": {"a": {"limits": [{"capacity": 0}]}}}));
        assert_eq!(m[0].0, "Malformed parameter: [\"capacity\"]");
        let m = msgs(
            json!({"$rateLimits": {"a": {"limits": [{"capacity": 5, "refill": {"period": "2 days"}}]}}}),
        );
        assert_eq!(
            m[0].0,
            "Validation failed for rules: The refill period can't be longer than a day."
        );
        let m = msgs(
            json!({"$rateLimits": {"a": {"limits": [{"capacity": 5, "refill": {"period": "500 milliseconds"}}]}}}),
        );
        assert_eq!(m[0].0, "Malformed parameter: [\"refill\" \"period\"]");
        assert_eq!(parse_interval_secs("01:30:00"), Some(5400.0));
        assert_eq!(parse_interval_secs("2 hours 30 minutes"), Some(9000.0));
    }
}
