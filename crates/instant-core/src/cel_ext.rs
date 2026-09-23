//! CEL extension functions the legacy rule runtime registers beyond the CEL
//! standard library (LEGACY db/cel.clj:387-414, :426-429, :439-454, :479-488):
//!
//! - cel-java 0.11's `CelExtensions/strings`: `charAt`, `indexOf`,
//!   `lastIndexOf`, `lowerAscii`, `upperAscii`, `replace`, `split`,
//!   `substring`, `trim`, `join` (the newer `reverse`, `strings.quote` and
//!   `format` are undeclared references on the legacy server, verified by
//!   differential step 29, so they are not registered);
//! - cel-java `CelExtensions/math`: `math.greatest`, `math.least`, `math.abs`,
//!   `math.sign`, `math.ceil`, `math.floor`, `math.round` (half to even, like
//!   cel-java), `math.trunc`,
//!   `math.sqrt`, `math.isInf`, `math.isNaN`, `math.isFinite`, `math.bitAnd`,
//!   `math.bitOr`, `math.bitXor`, `math.bitNot`, `math.bitShiftLeft`,
//!   `math.bitShiftRight`;
//! - Instant's own overloads: `timestamp.getTime()` (epoch **milliseconds**,
//!   `proto/timestamp->epoch-seconds` is misnamed: it calls
//!   `Timestamps/toMillis`), `timestamp(int)` (epoch milliseconds) and
//!   `timestamp(string)` through the same lenient date parser the `date`
//!   checked type uses (`triple-model/parse-date-value`).
//!
//! - cel-java `CelExtensions/bindings` (cel.clj:489-491): the
//!   `cel.bind(var, init, body)` macro, expanded like cel-java into a
//!   comprehension over an empty list whose accumulator is `var`
//!   ([`expand_bind_macros`]).
//!
//! The `cel` crate dispatches to its own standard-library overloads before
//! any `Context` function, so `timestamp(string)` can only be widened by
//! renaming the call in the AST before evaluation ([`rewrite_timestamp_calls`]).
//! [`rewrite`] applies both AST rewrites.

use std::sync::Arc;

use cel::extractors::{Arguments, This};
use cel::{ExecutionError, Value};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, TimeZone, Utc};

type R = std::result::Result<Value, ExecutionError>;

/// Name the AST rewrite gives `timestamp(...)` calls so the standard
/// `timestamp(string)` overload (strict RFC 3339) no longer shadows the
/// lenient Instant one.
pub const TIMESTAMP_FN: &str = "__instantTimestamp";

fn no_overload(name: &str) -> ExecutionError {
    ExecutionError::function_error(name, format!("no matching overload for '{name}'"))
}

fn ferr(name: &str, msg: impl Into<String>) -> ExecutionError {
    let msg: String = msg.into();
    ExecutionError::function_error(name, msg)
}

fn as_str(name: &str, v: &Value) -> std::result::Result<Arc<String>, ExecutionError> {
    match v {
        Value::String(s) => Ok(s.clone()),
        _ => Err(no_overload(name)),
    }
}

fn as_int(name: &str, v: &Value) -> std::result::Result<i64, ExecutionError> {
    match v {
        Value::Int(i) => Ok(*i),
        Value::UInt(u) => Ok(*u as i64),
        _ => Err(no_overload(name)),
    }
}

fn as_f64(name: &str, v: &Value) -> std::result::Result<f64, ExecutionError> {
    match v {
        Value::Int(i) => Ok(*i as f64),
        Value::UInt(u) => Ok(*u as f64),
        Value::Float(f) => Ok(*f),
        _ => Err(no_overload(name)),
    }
}

fn string(s: String) -> Value {
    Value::String(Arc::new(s))
}

/// Code-point index → byte offset (cel-java indexes by code point).
fn byte_offset(s: &str, cp: usize) -> Option<usize> {
    if cp == 0 {
        return Some(0);
    }
    let mut count = 0;
    for (i, _) in s.char_indices() {
        if count == cp {
            return Some(i);
        }
        count += 1;
    }
    if count == cp {
        Some(s.len())
    } else {
        None
    }
}

fn char_count(s: &str) -> usize {
    s.chars().count()
}

/// Code-point index of the first `needle` match at or after code point `from`.
fn index_of(hay: &str, needle: &str, from: usize) -> Option<i64> {
    let start = byte_offset(hay, from)?;
    hay[start..]
        .find(needle)
        .map(|b| char_count(&hay[..start + b]) as i64)
}

fn last_index_of(hay: &str, needle: &str, from: usize) -> Option<i64> {
    // cel-java: the match must START at or before `from`
    let limit = byte_offset(hay, from)?;
    let mut best: Option<usize> = None;
    for (b, _) in hay.match_indices(needle) {
        if b <= limit {
            best = Some(b);
        } else {
            break;
        }
    }
    if needle.is_empty() {
        best = Some(limit);
    }
    best.map(|b| char_count(&hay[..b]) as i64)
}

fn numeric_cmp(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => Some(x.cmp(y)),
        (Value::UInt(x), Value::UInt(y)) => Some(x.cmp(y)),
        _ => {
            let x = as_f64("math", a).ok()?;
            let y = as_f64("math", b).ok()?;
            x.partial_cmp(&y)
        }
    }
}

fn is_numeric(v: &Value) -> bool {
    matches!(v, Value::Int(_) | Value::UInt(_) | Value::Float(_))
}

/// `math.greatest` / `math.least`: variadic numeric arguments or one list.
fn extreme(name: &str, args: &[Value], greatest: bool) -> R {
    let items: Vec<Value> = match args {
        [Value::List(l)] => l.iter().cloned().collect(),
        [] => {
            return Err(ferr(
                name,
                format!("{name}() requires at least one argument"),
            ))
        }
        other => other.to_vec(),
    };
    if items.is_empty() {
        return Err(ferr(name, format!("{name}() invoked with empty list")));
    }
    let mut best = items[0].clone();
    if !is_numeric(&best) {
        return Err(no_overload(name));
    }
    for v in &items[1..] {
        if !is_numeric(v) {
            return Err(no_overload(name));
        }
        let ord = numeric_cmp(v, &best).ok_or_else(|| no_overload(name))?;
        let replace = if greatest {
            ord == std::cmp::Ordering::Greater
        } else {
            ord == std::cmp::Ordering::Less
        };
        if replace {
            best = v.clone();
        }
    }
    Ok(best)
}

/// Legacy `triple-model/parse-date-value` for strings (db/model/triple.clj
/// :1590-1642): ISO zoned / local date-time / local date, a handful of
/// human formats, a JSON-quoted date string, and a trimmed retry.
pub fn parse_date_string(raw: &str) -> Option<DateTime<FixedOffset>> {
    fn try_formats(s: &str) -> Option<DateTime<FixedOffset>> {
        if let Ok(t) = DateTime::parse_from_rfc3339(s) {
            return Some(t);
        }
        // zoned without the `T` / with a space, or offsets like `-08`
        for f in [
            "%Y-%m-%dT%H:%M:%S%.f%:z",
            "%Y-%m-%dT%H:%M:%S%:z",
            "%Y-%m-%dT%H:%M:%S%.f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S%.f%:z",
            "%Y-%m-%d %H:%M:%S%:z",
            "%Y-%m-%d %H:%M:%S%.f%z",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S%.f%#z",
            "%Y-%m-%dT%H:%M:%S%#z",
            "%Y-%m-%d %H:%M:%S%.f%#z",
            "%Y-%m-%d %H:%M:%S%#z",
        ] {
            if let Ok(t) = DateTime::parse_from_str(s, f) {
                return Some(t);
            }
        }
        if let Ok(t) = DateTime::parse_from_rfc2822(s) {
            return Some(t);
        }
        // local date-times read as UTC
        for f in [
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M",
        ] {
            if let Ok(t) = NaiveDateTime::parse_from_str(s, f) {
                return Some(Utc.from_utc_datetime(&t).fixed_offset());
            }
        }
        // `Z`-suffixed local date-time without seconds precision handled above;
        // trailing `Z` on a date-time chrono's rfc3339 already accepts.
        for f in [
            "%Y-%m-%d",
            "%m-%d-%Y",
            "%m/%d/%Y",
            "%a %b %d %Y",
            "%b %d %Y",
            "%B %d %Y",
        ] {
            if let Ok(d) = NaiveDate::parse_from_str(s, f) {
                let t = d.and_hms_opt(0, 0, 0)?;
                return Some(Utc.from_utc_datetime(&t).fixed_offset());
            }
        }
        // `Date.toString()`: "Tue Jan 02 2024 10:20:30 GMT+0000 (Coordinated Universal Time)"
        if let Some(idx) = s.find(" GMT") {
            let head = &s[..idx];
            let tail = &s[idx + 4..];
            let off: String = tail
                .chars()
                .take_while(|c| *c == '+' || *c == '-' || c.is_ascii_digit())
                .collect();
            let candidate = format!("{head} {off}");
            if let Ok(t) = DateTime::parse_from_str(&candidate, "%a %b %d %Y %H:%M:%S %z") {
                return Some(t);
            }
        }
        None
    }
    if let Some(t) = try_formats(raw) {
        return Some(t);
    }
    // JSON-encoded string: "\"2025-01-02T00:00:00-08\""
    if let Ok(serde_json::Value::String(inner)) = serde_json::from_str::<serde_json::Value>(raw) {
        if let Some(t) = try_formats(&inner) {
            return Some(t);
        }
    }
    let trimmed = raw.trim();
    if trimmed != raw {
        if let Some(t) = try_formats(trimmed) {
            return Some(t);
        }
    }
    None
}

fn millis_to_timestamp(ms: i64) -> Option<DateTime<FixedOffset>> {
    Utc.timestamp_millis_opt(ms)
        .single()
        .map(|t| t.fixed_offset())
}

/// Every AST rewrite the legacy compiler's extensions imply: `cel.bind`
/// expansion, then the `timestamp(...)` rename.
pub fn rewrite(expr: &mut cel::IdedExpr) {
    expand_bind_macros(expr);
    rewrite_timestamp_calls(expr);
}

/// Expand `cel.bind(name, init, body)` the way cel-java's bindings macro
/// does: `Comprehension{iter_range: [], iter_var: "#unused", accu_var: name,
/// accu_init: init, loop_cond: false, loop_step: name, result: body}`. The
/// empty range never iterates, so `body` is evaluated once with `name`
/// bound to `init`. A call whose first argument isn't a simple identifier
/// is left alone (cel-java rejects it at compile time too).
pub fn expand_bind_macros(expr: &mut cel::IdedExpr) {
    use cel::common::ast::{CallExpr, ComprehensionExpr, EntryExpr, Expr, ListExpr, LiteralValue};
    if let Expr::Call(CallExpr {
        func_name,
        target: Some(target),
        args,
    }) = &mut expr.expr
    {
        let is_bind = func_name == "bind"
            && matches!(&target.expr, Expr::Ident(t) if t == "cel")
            && args.len() == 3
            && matches!(&args[0].expr, Expr::Ident(_));
        if is_bind {
            let mut args = std::mem::take(args);
            let body = args.pop().expect("three args");
            let init = args.pop().expect("three args");
            let Expr::Ident(name) = args.pop().expect("three args").expr else {
                unreachable!("checked above")
            };
            let id = expr.id;
            let lit = |v: LiteralValue| cel::IdedExpr {
                id,
                expr: Expr::Literal(v),
            };
            expr.expr = Expr::Comprehension(Box::new(ComprehensionExpr {
                iter_range: cel::IdedExpr {
                    id,
                    expr: Expr::List(ListExpr {
                        elements: vec![],
                        optional_indices: vec![],
                    }),
                },
                iter_var: "#unused".to_string(),
                iter_var2: None,
                accu_var: name.clone(),
                accu_init: init,
                loop_cond: lit(LiteralValue::Boolean(false.into())),
                loop_step: cel::IdedExpr {
                    id,
                    expr: Expr::Ident(name),
                },
                result: body,
            }));
        }
    }
    match &mut expr.expr {
        Expr::Unspecified | Expr::Ident(_) | Expr::Literal(_) => {}
        Expr::Call(c) => {
            if let Some(t) = &mut c.target {
                expand_bind_macros(t);
            }
            for a in &mut c.args {
                expand_bind_macros(a);
            }
        }
        Expr::Comprehension(c) => {
            expand_bind_macros(&mut c.iter_range);
            expand_bind_macros(&mut c.accu_init);
            expand_bind_macros(&mut c.loop_cond);
            expand_bind_macros(&mut c.loop_step);
            expand_bind_macros(&mut c.result);
        }
        Expr::List(l) => {
            for e in &mut l.elements {
                expand_bind_macros(e);
            }
        }
        Expr::Map(m) => {
            for e in &mut m.entries {
                if let EntryExpr::MapEntry(me) = &mut e.expr {
                    expand_bind_macros(&mut me.key);
                    expand_bind_macros(&mut me.value);
                }
            }
        }
        Expr::Select(s) => expand_bind_macros(&mut s.operand),
        Expr::Struct(st) => {
            for e in &mut st.entries {
                match &mut e.expr {
                    EntryExpr::StructField(fl) => expand_bind_macros(&mut fl.value),
                    EntryExpr::MapEntry(me) => {
                        expand_bind_macros(&mut me.key);
                        expand_bind_macros(&mut me.value);
                    }
                }
            }
        }
    }
}

/// Rewrite every `timestamp(x)` call (global form only) to
/// [`TIMESTAMP_FN`] so the Instant overloads win over the standard library.
pub fn rewrite_timestamp_calls(expr: &mut cel::IdedExpr) {
    use cel::common::ast::{EntryExpr, Expr};
    match &mut expr.expr {
        Expr::Unspecified | Expr::Ident(_) | Expr::Literal(_) => {}
        Expr::Call(c) => {
            if c.target.is_none() && c.func_name == "timestamp" && c.args.len() == 1 {
                c.func_name = TIMESTAMP_FN.to_string();
            }
            if let Some(t) = &mut c.target {
                rewrite_timestamp_calls(t);
            }
            for a in &mut c.args {
                rewrite_timestamp_calls(a);
            }
        }
        Expr::Comprehension(c) => {
            rewrite_timestamp_calls(&mut c.iter_range);
            rewrite_timestamp_calls(&mut c.accu_init);
            rewrite_timestamp_calls(&mut c.loop_cond);
            rewrite_timestamp_calls(&mut c.loop_step);
            rewrite_timestamp_calls(&mut c.result);
        }
        Expr::List(l) => {
            for e in &mut l.elements {
                rewrite_timestamp_calls(e);
            }
        }
        Expr::Map(m) => {
            for e in &mut m.entries {
                if let EntryExpr::MapEntry(me) = &mut e.expr {
                    rewrite_timestamp_calls(&mut me.key);
                    rewrite_timestamp_calls(&mut me.value);
                }
            }
        }
        Expr::Select(s) => rewrite_timestamp_calls(&mut s.operand),
        Expr::Struct(st) => {
            for e in &mut st.entries {
                match &mut e.expr {
                    EntryExpr::StructField(fl) => rewrite_timestamp_calls(&mut fl.value),
                    EntryExpr::MapEntry(me) => {
                        rewrite_timestamp_calls(&mut me.key);
                        rewrite_timestamp_calls(&mut me.value);
                    }
                }
            }
        }
    }
}

/// Register every extension function on a context.
pub fn register(ctx: &mut cel::Context) {
    // ---- strings ---------------------------------------------------------
    ctx.add_function(
        "charAt",
        |This(this): This<Value>, Arguments(args): Arguments| -> R {
            let s = as_str("charAt", &this)?;
            let [i] = args.as_slice() else {
                return Err(no_overload("charAt"));
            };
            let i = as_int("charAt", i)?;
            let n = char_count(&s) as i64;
            if !(0..=n).contains(&i) {
                return Err(ferr("charAt", format!("index out of range: {i}")));
            }
            Ok(string(
                s.chars()
                    .nth(i as usize)
                    .map(|c| c.to_string())
                    .unwrap_or_default(),
            ))
        },
    );
    ctx.add_function(
        "indexOf",
        |This(this): This<Value>, Arguments(args): Arguments| -> R {
            let s = as_str("indexOf", &this)?;
            let (needle, from) = match args.as_slice() {
                [n] => (as_str("indexOf", n)?, 0i64),
                [n, f] => (as_str("indexOf", n)?, as_int("indexOf", f)?),
                _ => return Err(no_overload("indexOf")),
            };
            let len = char_count(&s) as i64;
            if !(0..=len).contains(&from) {
                return Err(ferr("indexOf", format!("index out of range: {from}")));
            }
            Ok(Value::Int(
                index_of(&s, &needle, from as usize).unwrap_or(-1),
            ))
        },
    );
    ctx.add_function(
        "lastIndexOf",
        |This(this): This<Value>, Arguments(args): Arguments| -> R {
            let s = as_str("lastIndexOf", &this)?;
            let len = char_count(&s) as i64;
            let (needle, from) = match args.as_slice() {
                [n] => (as_str("lastIndexOf", n)?, len),
                [n, f] => (as_str("lastIndexOf", n)?, as_int("lastIndexOf", f)?),
                _ => return Err(no_overload("lastIndexOf")),
            };
            if !(0..=len).contains(&from) {
                return Err(ferr("lastIndexOf", format!("index out of range: {from}")));
            }
            Ok(Value::Int(
                last_index_of(&s, &needle, from as usize).unwrap_or(-1),
            ))
        },
    );
    ctx.add_function(
        "lowerAscii",
        |This(this): This<Value>, Arguments(args): Arguments| -> R {
            if !args.is_empty() {
                return Err(no_overload("lowerAscii"));
            }
            let s = as_str("lowerAscii", &this)?;
            Ok(string(s.to_ascii_lowercase()))
        },
    );
    ctx.add_function(
        "upperAscii",
        |This(this): This<Value>, Arguments(args): Arguments| -> R {
            if !args.is_empty() {
                return Err(no_overload("upperAscii"));
            }
            let s = as_str("upperAscii", &this)?;
            Ok(string(s.to_ascii_uppercase()))
        },
    );
    ctx.add_function(
        "replace",
        |This(this): This<Value>, Arguments(args): Arguments| -> R {
            let s = as_str("replace", &this)?;
            let (from, to, limit) = match args.as_slice() {
                [a, b] => (as_str("replace", a)?, as_str("replace", b)?, -1i64),
                [a, b, n] => (
                    as_str("replace", a)?,
                    as_str("replace", b)?,
                    as_int("replace", n)?,
                ),
                _ => return Err(no_overload("replace")),
            };
            let out = if limit < 0 {
                s.replace(from.as_str(), to.as_str())
            } else {
                s.replacen(from.as_str(), to.as_str(), limit as usize)
            };
            Ok(string(out))
        },
    );
    ctx.add_function(
        "split",
        |This(this): This<Value>, Arguments(args): Arguments| -> R {
            let s = as_str("split", &this)?;
            let (sep, limit) = match args.as_slice() {
                [a] => (as_str("split", a)?, -1i64),
                [a, n] => (as_str("split", a)?, as_int("split", n)?),
                _ => return Err(no_overload("split")),
            };
            let parts: Vec<Value> = match limit {
                0 => vec![],
                1 => vec![string(s.to_string())],
                n if n < 0 => s
                    .split(sep.as_str())
                    .map(|p| string(p.to_string()))
                    .collect(),
                n => s
                    .splitn(n as usize, sep.as_str())
                    .map(|p| string(p.to_string()))
                    .collect(),
            };
            Ok(Value::List(Arc::new(parts)))
        },
    );
    ctx.add_function(
        "substring",
        |This(this): This<Value>, Arguments(args): Arguments| -> R {
            let s = as_str("substring", &this)?;
            let len = char_count(&s) as i64;
            let (start, end) = match args.as_slice() {
                [a] => (as_int("substring", a)?, len),
                [a, b] => (as_int("substring", a)?, as_int("substring", b)?),
                _ => return Err(no_overload("substring")),
            };
            if !(0..=len).contains(&start) || !(0..=len).contains(&end) {
                return Err(ferr("substring", format!("index out of range: {start}")));
            }
            if start > end {
                return Err(ferr(
                    "substring",
                    format!("invalid substring range. start: {start}, end: {end}"),
                ));
            }
            let b0 = byte_offset(&s, start as usize).unwrap_or(s.len());
            let b1 = byte_offset(&s, end as usize).unwrap_or(s.len());
            Ok(string(s[b0..b1].to_string()))
        },
    );
    ctx.add_function(
        "trim",
        |This(this): This<Value>, Arguments(args): Arguments| -> R {
            if !args.is_empty() {
                return Err(no_overload("trim"));
            }
            let s = as_str("trim", &this)?;
            Ok(string(s.trim().to_string()))
        },
    );
    ctx.add_function(
        "join",
        |This(this): This<Value>, Arguments(args): Arguments| -> R {
            let list = match &this {
                Value::List(l) => l.clone(),
                _ => return Err(no_overload("join")),
            };
            let sep = match args.as_slice() {
                [] => Arc::new(String::new()),
                [a] => as_str("join", a)?,
                _ => return Err(no_overload("join")),
            };
            let mut parts: Vec<String> = Vec::with_capacity(list.len());
            for v in list.iter() {
                parts.push(as_str("join", v)?.to_string());
            }
            Ok(string(parts.join(sep.as_str())))
        },
    );
    // ---- math ------------------------------------------------------------
    ctx.add_function("math.greatest", |Arguments(args): Arguments| -> R {
        extreme("math.greatest", &args, true)
    });
    ctx.add_function("math.least", |Arguments(args): Arguments| -> R {
        extreme("math.least", &args, false)
    });
    ctx.add_function("math.abs", |Arguments(args): Arguments| -> R {
        match args.as_slice() {
            [Value::Int(i)] => i
                .checked_abs()
                .map(Value::Int)
                .ok_or_else(|| ferr("math.abs", "integer overflow")),
            [Value::UInt(u)] => Ok(Value::UInt(*u)),
            [Value::Float(f)] => Ok(Value::Float(f.abs())),
            _ => Err(no_overload("math.abs")),
        }
    });
    ctx.add_function("math.sign", |Arguments(args): Arguments| -> R {
        match args.as_slice() {
            [Value::Int(i)] => Ok(Value::Int(i.signum())),
            [Value::UInt(u)] => Ok(Value::UInt(if *u == 0 { 0 } else { 1 })),
            [Value::Float(f)] => Ok(Value::Float(if f.is_nan() {
                f64::NAN
            } else if *f == 0.0 {
                0.0
            } else {
                f.signum()
            })),
            _ => Err(no_overload("math.sign")),
        }
    });
    for (name, f) in [
        ("math.ceil", f64::ceil as fn(f64) -> f64),
        ("math.floor", f64::floor),
        ("math.trunc", f64::trunc),
        ("math.sqrt", f64::sqrt),
    ] {
        ctx.add_function(name, move |Arguments(args): Arguments| -> R {
            match args.as_slice() {
                [Value::Float(x)] => Ok(Value::Float(f(*x))),
                [Value::Int(x)] if name == "math.sqrt" => Ok(Value::Float(f(*x as f64))),
                [Value::UInt(x)] if name == "math.sqrt" => Ok(Value::Float(f(*x as f64))),
                _ => Err(no_overload(name)),
            }
        });
    }
    ctx.add_function("math.round", |Arguments(args): Arguments| -> R {
        match args.as_slice() {
            // cel-java's MathExtension rounds half to even (Math.rint):
            // math.round(2.5) == 2.0, verified against the legacy server
            [Value::Float(x)] => Ok(Value::Float(x.round_ties_even())),
            _ => Err(no_overload("math.round")),
        }
    });
    for (name, f) in [
        ("math.isInf", (|x: f64| x.is_infinite()) as fn(f64) -> bool),
        ("math.isNaN", |x: f64| x.is_nan()),
        ("math.isFinite", |x: f64| x.is_finite()),
    ] {
        ctx.add_function(name, move |Arguments(args): Arguments| -> R {
            match args.as_slice() {
                [Value::Float(x)] => Ok(Value::Bool(f(*x))),
                _ => Err(no_overload(name)),
            }
        });
    }
    for (name, f) in [
        (
            "math.bitAnd",
            (|a: i64, b: i64| a & b) as fn(i64, i64) -> i64,
        ),
        ("math.bitOr", |a: i64, b: i64| a | b),
        ("math.bitXor", |a: i64, b: i64| a ^ b),
    ] {
        ctx.add_function(name, move |Arguments(args): Arguments| -> R {
            match args.as_slice() {
                [Value::Int(a), Value::Int(b)] => Ok(Value::Int(f(*a, *b))),
                [Value::UInt(a), Value::UInt(b)] => Ok(Value::UInt(f(*a as i64, *b as i64) as u64)),
                _ => Err(no_overload(name)),
            }
        });
    }
    ctx.add_function("math.bitNot", |Arguments(args): Arguments| -> R {
        match args.as_slice() {
            [Value::Int(a)] => Ok(Value::Int(!a)),
            [Value::UInt(a)] => Ok(Value::UInt(!a)),
            _ => Err(no_overload("math.bitNot")),
        }
    });
    ctx.add_function("math.bitShiftLeft", |Arguments(args): Arguments| -> R {
        let shift = |n: &Value| -> std::result::Result<u32, ExecutionError> {
            let n = as_int("math.bitShiftLeft", n)?;
            if n < 0 {
                return Err(ferr("math.bitShiftLeft", "negative shift count"));
            }
            Ok(n.min(64) as u32)
        };
        match args.as_slice() {
            [Value::Int(a), n] => Ok(Value::Int(a.checked_shl(shift(n)?).unwrap_or(0))),
            [Value::UInt(a), n] => Ok(Value::UInt(a.checked_shl(shift(n)?).unwrap_or(0))),
            _ => Err(no_overload("math.bitShiftLeft")),
        }
    });
    ctx.add_function("math.bitShiftRight", |Arguments(args): Arguments| -> R {
        let shift = |n: &Value| -> std::result::Result<u32, ExecutionError> {
            let n = as_int("math.bitShiftRight", n)?;
            if n < 0 {
                return Err(ferr("math.bitShiftRight", "negative shift count"));
            }
            Ok(n.min(64) as u32)
        };
        match args.as_slice() {
            // cel-java shifts ints logically (as unsigned)
            [Value::Int(a), n] => Ok(Value::Int(
                ((*a as u64).checked_shr(shift(n)?).unwrap_or(0)) as i64,
            )),
            [Value::UInt(a), n] => Ok(Value::UInt(a.checked_shr(shift(n)?).unwrap_or(0))),
            _ => Err(no_overload("math.bitShiftRight")),
        }
    });

    // ---- Instant's timestamp overloads ------------------------------------
    ctx.add_function(
        "getTime",
        |This(this): This<Value>, Arguments(args): Arguments| -> R {
            if !args.is_empty() {
                return Err(no_overload("getTime"));
            }
            match this {
                Value::Timestamp(t) => Ok(Value::Int(t.timestamp_millis())),
                _ => Err(no_overload("getTime")),
            }
        },
    );
    ctx.add_function(TIMESTAMP_FN, |Arguments(args): Arguments| -> R {
        let [v] = args.as_slice() else {
            return Err(no_overload("timestamp"));
        };
        match v {
            Value::Timestamp(t) => Ok(Value::Timestamp(*t)),
            Value::Int(ms) => millis_to_timestamp(*ms)
                .map(Value::Timestamp)
                .ok_or_else(|| ferr("timestamp", format!("timestamp out of range: {ms}"))),
            Value::UInt(ms) => millis_to_timestamp(*ms as i64)
                .map(Value::Timestamp)
                .ok_or_else(|| ferr("timestamp", format!("timestamp out of range: {ms}"))),
            Value::String(s) => parse_date_string(s)
                .map(Value::Timestamp)
                .ok_or_else(|| ferr("timestamp", format!("Unable to parse date string {s}"))),
            _ => Err(no_overload("timestamp")),
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eval(expr: &str) -> Value {
        let program = cel::Program::compile(expr).unwrap();
        let mut e = program.expression().clone();
        rewrite(&mut e);
        let mut ctx = cel::Context::default();
        register(&mut ctx);
        Value::resolve(&e, &ctx).unwrap()
    }

    fn eval_err(expr: &str) -> ExecutionError {
        let program = cel::Program::compile(expr).unwrap();
        let mut e = program.expression().clone();
        rewrite(&mut e);
        let mut ctx = cel::Context::default();
        register(&mut ctx);
        Value::resolve(&e, &ctx).unwrap_err()
    }

    fn s(v: &str) -> Value {
        string(v.to_string())
    }

    #[test]
    fn string_extensions() {
        assert_eq!(eval("'hello'.charAt(1)"), s("e"));
        assert_eq!(eval("'hello'.charAt(5)"), s(""));
        assert_eq!(eval("'hello mellow'.indexOf('ello')"), Value::Int(1));
        assert_eq!(eval("'hello mellow'.indexOf('ello', 2)"), Value::Int(7));
        assert_eq!(eval("'hello mellow'.indexOf('jello')"), Value::Int(-1));
        assert_eq!(eval("'hello mellow'.lastIndexOf('ello')"), Value::Int(7));
        assert_eq!(eval("'hello mellow'.lastIndexOf('ello', 6)"), Value::Int(1));
        assert_eq!(eval("'TacoCat'.lowerAscii()"), s("tacocat"));
        assert_eq!(eval("'TacoCÆt'.upperAscii()"), s("TACOCÆT"));
        assert_eq!(eval("'hello hello'.replace('he', 'we')"), s("wello wello"));
        assert_eq!(
            eval("'hello hello'.replace('he', 'we', 1)"),
            s("wello hello")
        );
        assert_eq!(
            eval("'hello hello'.replace('he', 'we', 0)"),
            s("hello hello")
        );
        assert_eq!(
            eval("'hello hello hello'.split(' ')"),
            Value::List(Arc::new(vec![s("hello"), s("hello"), s("hello")]))
        );
        assert_eq!(
            eval("'hello hello hello'.split(' ', 2)"),
            Value::List(Arc::new(vec![s("hello"), s("hello hello")]))
        );
        assert_eq!(eval("'tacocat'.substring(4)"), s("cat"));
        assert_eq!(eval("'tacocat'.substring(0, 4)"), s("taco"));
        assert_eq!(eval("'  \\ttrim\\n    '.trim()"), s("trim"));
        assert_eq!(eval("['x', 'y'].join()"), s("xy"));
        assert_eq!(eval("['x', 'y'].join('-')"), s("x-y"));
        // out-of-range indexes are evaluation errors like cel-java
        assert!(matches!(
            eval_err("'abc'.charAt(7)"),
            ExecutionError::FunctionError { .. }
        ));
        assert!(matches!(
            eval_err("'abc'.substring(2, 1)"),
            ExecutionError::FunctionError { .. }
        ));
        // the crate's own string functions still resolve first
        assert_eq!(
            eval("'hello'.startsWith('he') && 'hello'.contains('ell')"),
            Value::Bool(true)
        );
    }

    #[test]
    fn math_extensions() {
        assert_eq!(eval("math.greatest(1, 3, 2)"), Value::Int(3));
        assert_eq!(eval("math.least([1, 3, 2])"), Value::Int(1));
        assert_eq!(eval("math.greatest(1, 2.5)"), Value::Float(2.5));
        assert_eq!(eval("math.abs(-3)"), Value::Int(3));
        assert_eq!(eval("math.sign(-2.5)"), Value::Float(-1.0));
        assert_eq!(eval("math.ceil(1.2)"), Value::Float(2.0));
        assert_eq!(eval("math.floor(1.8)"), Value::Float(1.0));
        assert_eq!(eval("math.round(1.5)"), Value::Float(2.0));
        assert_eq!(eval("math.round(2.5)"), Value::Float(2.0));
        assert_eq!(eval("math.round(-2.5)"), Value::Float(-2.0));
        assert_eq!(eval("math.trunc(-1.8)"), Value::Float(-1.0));
        assert_eq!(eval("math.sqrt(16.0)"), Value::Float(4.0));
        assert_eq!(eval("math.isNaN(0.0 / 0.0)"), Value::Bool(true));
        assert_eq!(eval("math.isInf(1.0 / 0.0)"), Value::Bool(true));
        assert_eq!(eval("math.isFinite(1.0)"), Value::Bool(true));
        assert_eq!(eval("math.bitAnd(6, 3)"), Value::Int(2));
        assert_eq!(eval("math.bitOr(6, 3)"), Value::Int(7));
        assert_eq!(eval("math.bitXor(6, 3)"), Value::Int(5));
        assert_eq!(eval("math.bitNot(0)"), Value::Int(-1));
        assert_eq!(eval("math.bitShiftLeft(1, 3)"), Value::Int(8));
        assert_eq!(eval("math.bitShiftRight(8, 3)"), Value::Int(1));
        assert!(matches!(
            eval_err("math.greatest([])"),
            ExecutionError::FunctionError { .. }
        ));
        assert!(matches!(
            eval_err("math.greatest('a', 'b')"),
            ExecutionError::FunctionError { .. }
        ));
    }

    #[test]
    fn bind_macro() {
        assert_eq!(eval("cel.bind(x, 2, x * x + x)"), Value::Int(6));
        assert_eq!(
            eval("cel.bind(s, 'Hello', s.lowerAscii() + s.upperAscii())"),
            s("helloHELLO")
        );
        // nested, and shadowing an outer binding
        assert_eq!(
            eval("cel.bind(a, 1, cel.bind(b, a + 1, cel.bind(a, 10, a + b)))"),
            Value::Int(12)
        );
        // inside a macro body
        assert_eq!(
            eval("[1, 2, 3].all(i, cel.bind(d, i * 2, d > i))"),
            Value::Bool(true)
        );
    }

    #[test]
    fn timestamp_overloads() {
        // getTime is epoch milliseconds (proto.clj:56-57 calls Timestamps/toMillis)
        assert_eq!(
            eval("timestamp('2020-01-01T00:00:00Z').getTime()"),
            Value::Int(1577836800000)
        );
        // timestamp(int) is epoch milliseconds (Instant/ofEpochMilli)
        assert_eq!(
            eval("timestamp(1577836800000).getFullYear()"),
            Value::Int(2020)
        );
        assert_eq!(
            eval("timestamp(0) < timestamp('1970-01-02T00:00:00Z')"),
            Value::Bool(true)
        );
        // the lenient string parser accepts what the `date` checked type accepts
        assert_eq!(
            eval("timestamp('2020-01-01').getTime()"),
            Value::Int(1577836800000)
        );
        assert_eq!(
            eval("timestamp('2020-01-01 10:00:00').getHours()"),
            Value::Int(10)
        );
        assert_eq!(
            eval("timestamp('2025-01-02T00:00:00-08').getDate()"),
            Value::Int(2)
        );
        assert_eq!(
            eval("timestamp('\"2020-01-01T00:00:00Z\"').getTime()"),
            Value::Int(1577836800000)
        );
        assert_eq!(
            eval("timestamp(' 2020-01-01T00:00:00Z ').getTime()"),
            Value::Int(1577836800000)
        );
        assert_eq!(eval("timestamp('01/02/2020').getMonth()"), Value::Int(0));
        assert_eq!(
            eval("timestamp('Tue Jan 02 2024').getFullYear()"),
            Value::Int(2024)
        );
        assert!(matches!(
            eval_err("timestamp('not a date')"),
            ExecutionError::FunctionError { .. }
        ));
        // request.time style comparisons compose with the standard overloads
        assert_eq!(
            eval("timestamp(0) + duration('1h') == timestamp(3600000)"),
            Value::Bool(true)
        );
    }
}
