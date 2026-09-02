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
            let mut binds = vec![];
            if let Some(Value::Array(b)) = ns.get("bind") {
                let mut i = 0;
                while i + 1 < b.len() {
                    if let (Some(name), Some(expr)) = (b[i].as_str(), b[i + 1].as_str()) {
                        binds.push((name.to_string(), expr.to_string()));
                    }
                    i += 2;
                }
            }
            return Some((expr_str, binds));
        }
        None
    }

    fn binds_of(&self, etype: &str) -> Vec<(String, String)> {
        let mut binds = vec![];
        if let Some(Value::Array(b)) = self.code.get(etype).and_then(|ns| ns.get("bind")) {
            let mut i = 0;
            while i + 1 < b.len() {
                if let (Some(name), Some(expr)) = (b[i].as_str(), b[i + 1].as_str()) {
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
            return Program { expr, binds };
        }
        // System defaults
        if etype == "$users" {
            let expr = match action {
                "create" => "true",
                "view" | "update" => "auth.id == data.id",
                _ => "false",
            };
            return Program {
                expr: expr.to_string(),
                binds: vec![],
            };
        }
        if etype.starts_with('$') {
            return Program {
                expr: "false".to_string(),
                binds: vec![],
            };
        }
        Program {
            expr: "true".to_string(),
            binds: vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub struct Program {
    pub expr: String,
    pub binds: Vec<(String, String)>,
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
/// prefetched ref results under "_refs".
pub fn eval_program(
    program: &Program,
    data: &Value,
    new_data: Option<&Value>,
    auth: &Value,
    rule_params: &Value,
) -> Result<bool> {
    eval_program_full(program, data, new_data, auth, rule_params, None)
}

/// eval_program with the link-check `linkedData` binding.
pub fn eval_program_full(
    program: &Program,
    data: &Value,
    new_data: Option<&Value>,
    auth: &Value,
    rule_params: &Value,
    linked_data: Option<&Value>,
) -> Result<bool> {
    if program.is_true() {
        return Ok(true);
    }
    if program.is_false() && program.binds.is_empty() {
        return Ok(false);
    }
    let compiled = cel::Program::compile(&program.expr).map_err(|e| {
        InstantError::permission_denied(json!([]), format!("Invalid permission rule: {e}"))
    })?;
    let compiled_binds: Vec<(String, Option<cel::Program>)> = program
        .binds
        .iter()
        .map(|(name, expr)| (name.clone(), cel::Program::compile(expr).ok()))
        .collect();

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

    let mut ctx = cel::Context::default();
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

    // binds: evaluate in order, retrying to tolerate forward references
    let mut pending: Vec<(String, cel::Program)> = vec![];
    let mut unresolved: Vec<String> = vec![];
    for (name, p) in compiled_binds {
        match p {
            Some(p) => pending.push((name, p)),
            None => unresolved.push(name),
        }
    }
    let mut rounds = 0;
    while !pending.is_empty() && rounds < 5 {
        rounds += 1;
        let mut next = vec![];
        for (name, p) in pending {
            match p.execute(&ctx) {
                Ok(v) => {
                    ctx.add_variable_from_value(name.as_str(), v);
                }
                Err(_) => next.push((name, p)),
            }
        }
        pending = next;
    }
    // unresolved binds evaluate to null so has()-style checks stay sane
    unresolved.extend(pending.into_iter().map(|(name, _)| name));
    for name in &unresolved {
        ctx.add_variable_from_value(name.as_str(), cel::Value::Null);
    }

    match compiled.execute(&ctx) {
        Ok(cel::Value::Bool(b)) => Ok(b),
        Ok(_) => Ok(false),
        Err(_) => Ok(false),
    }
}

// ---------------------------------------------------------------------------
// Entity maps

/// Build an entity map {label: value} for rule eval; includes id and defaults
/// all blob labels of the etype to null.
pub fn base_entity_map(attrs: &AttrMap, etype: &str, eid: Uuid) -> Map<String, Value> {
    let mut m = Map::new();
    for a in attrs.attrs_of_etype(etype) {
        if a.value_type == ValueType::Blob {
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
            if a.value_type == ValueType::Blob && a.cardinality == Cardinality::One {
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
    let mut refs = Map::new();
    for p in paths {
        let v = resolve_ref_path(conn, app_id, attrs, etype, eid, p).await?;
        refs.insert(p.clone(), v);
    }
    map.insert("_refs".to_string(), Value::Object(refs));
    Ok(())
}

/// Build the `auth` value (with auth.ref prefetch, paths start with "$user.").
async fn build_auth_value(
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
        for form in &mut result.forms {
            let mut kept = vec![];
            let entities = std::mem::take(&mut form.entities);
            for mut node in entities {
                if self.check_view_node(conn, app_id, attrs, &mut node).await? {
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
    ) -> futures::future::BoxFuture<'b, Result<bool>> {
        Box::pin(async move {
            let program = self.rules.program(&node.etype, "view");
            let ok = if program.is_true() {
                true
            } else {
                let mut data = base_entity_map(attrs, &node.etype, node.eid);
                for t in &node.triples {
                    if let Some(a) = attrs.get(&t.a) {
                        if a.value_type == ValueType::Blob {
                            data.insert(a.label.clone(), t.v.clone());
                        }
                    }
                }
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
                eval_program(
                    &program,
                    &Value::Object(data),
                    None,
                    &auth_val,
                    &self.rule_params,
                )?
            };
            if !ok {
                return Ok(false);
            }
            // field-level rules: drop triples whose field program fails
            if self.rules.has_field_rules(&node.etype) {
                let mut data = base_entity_map(attrs, &node.etype, node.eid);
                for t in &node.triples {
                    if let Some(a) = attrs.get(&t.a) {
                        if a.value_type == ValueType::Blob {
                            data.insert(a.label.clone(), t.v.clone());
                        }
                    }
                }
                let data_val = Value::Object(data);
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
                                eval_program(
                                    &program,
                                    &data_val,
                                    None,
                                    &auth_val,
                                    &self.rule_params,
                                )?
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
                let mut kept = vec![];
                let entities = std::mem::take(&mut child.entities);
                let mut kept_ids = HashSet::new();
                for mut n in entities {
                    if self.check_view_node(conn, app_id, attrs, &mut n).await? {
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
        old: Option<Map<String, Value>>,
        linked_etype: String,
        linked_eid: Uuid,
        program: Program,
    },
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
    // explicitly-ruled ref steps skip the generic update fallback
    let mut link_checks: Vec<Check> = vec![];
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
                                link_checks.push(Check::LinkRule {
                                    action,
                                    etype: attr.etype.clone(),
                                    eid: e,
                                    old: old_maps.get(&(e, attr.etype.clone())).cloned().flatten(),
                                    linked_etype: retype.clone(),
                                    linked_eid: t,
                                    program: p,
                                });
                            }
                            if let Some(p) = rev_prog {
                                link_checks.push(Check::LinkRule {
                                    action,
                                    etype: retype.clone(),
                                    eid: t,
                                    old: old_maps.get(&(t, retype.clone())).cloned().flatten(),
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

    // ---- execute ----
    let report = tx::transact(conn, app_id, attrs, steps, &TxOptions::default()).await?;

    // ---- collect checks ----
    let mut checks: Vec<Check> = vec![];
    let created: HashSet<(Uuid, String)> = report.created.iter().cloned().collect();
    for (e, et) in &report.created {
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
    checks.extend(link_checks);
    let mut linked_seen = HashSet::new();
    for (e, et) in link_targets {
        let key = (e, et.clone());
        if created.contains(&key) || !linked_seen.insert(key.clone()) {
            continue;
        }
        checks.push(Check::ViewLinked { etype: et, eid: e });
    }

    // ---- evaluate ----
    let mut check_results: Vec<Value> = vec![];
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
                program,
            } => {
                // legacy: check runs only when the entity existed pre-tx
                let Some(old) = old else { continue };
                let new = fetch_entity_map(conn, app_id, attrs, etype, *eid)
                    .await?
                    .unwrap_or_else(|| base_entity_map(attrs, etype, *eid));
                let linked = fetch_entity_map(conn, app_id, attrs, linked_etype, *linked_eid)
                    .await?
                    .map(Value::Object)
                    .unwrap_or(Value::Null);
                let mut data = Value::Object(old.clone());
                if let Value::Object(ref mut m) = data {
                    let sources = program.all_sources();
                    let paths = extract_ref_paths(&sources, "data");
                    attach_refs(conn, app_id, attrs, etype, *eid, &paths, m).await?;
                }
                let auth_val = build_auth_value(conn, app_id, attrs, auth, &[program]).await?;
                let mut rp = match global_rule_params {
                    Value::Object(m) => m.clone(),
                    _ => Map::new(),
                };
                if let Some(Value::Object(step_rp)) = report.rule_params.get(&(*eid, etype.clone()))
                {
                    for (k, v) in step_rp {
                        rp.insert(k.clone(), v.clone());
                    }
                }
                let ok = eval_program_full(
                    program,
                    &data,
                    Some(&Value::Object(new)),
                    &auth_val,
                    &Value::Object(rp),
                    Some(&linked),
                )?;
                check_results.push(json!({
                    "scope": "object",
                    "etype": etype,
                    "action": action,
                    "eid": eid,
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
                    return Err(InstantError::permission_denied(
                        json!([etype, action]),
                        "Permission denied: not perms-pass?",
                    ));
                }
                continue;
            }
        };
        let program = rules.program(&etype, action);
        if program.is_true() {
            check_results.push(json!({
                "scope": "object",
                "etype": etype,
                "action": action,
                "eid": eid,
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
        // data refs prefetch
        let mut data = data;
        if let Value::Object(ref mut m) = data {
            let sources = program.all_sources();
            let paths = extract_ref_paths(&sources, "data");
            attach_refs(conn, app_id, attrs, &etype, eid, &paths, m).await?;
        }
        let auth_val = build_auth_value(conn, app_id, attrs, auth, &[&program]).await?;
        // merge rule params: step-level overrides global
        let mut rp = match global_rule_params {
            Value::Object(m) => m.clone(),
            _ => Map::new(),
        };
        if let Some(Value::Object(step_rp)) = report.rule_params.get(&(eid, etype.clone())) {
            for (k, v) in step_rp {
                rp.insert(k.clone(), v.clone());
            }
        }
        let ok = eval_program(
            &program,
            &data,
            new_data.as_ref(),
            &auth_val,
            &Value::Object(rp),
        )?;
        check_results.push(json!({
            "scope": "object",
            "etype": etype,
            "action": action,
            "eid": eid,
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
            return Err(InstantError::permission_denied(
                json!([etype, action]),
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
        };
        eval_program(&program, &data, None, &auth, &rule_params).unwrap()
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
        // chained access through a null value still denies (legacy errors too)
        assert!(!eval(
            "data.missing.b == null",
            json!({}),
            Value::Null,
            json!({})
        ));
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
    fn binds_see_null_safe_maps() {
        let program = Program {
            expr: "isOwner".to_string(),
            binds: vec![(
                "isOwner".to_string(),
                "data.creatorTypo == auth.id".to_string(),
            )],
        };
        let ok = eval_program(
            &program,
            &json!({"id": "1"}),
            None,
            &json!({"id": "u1"}),
            &json!({}),
        )
        .unwrap();
        assert!(!ok);
        let ok = eval_program(
            &program,
            &json!({"id": "1"}),
            None,
            &Value::Null,
            &json!({}),
        )
        .unwrap();
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
