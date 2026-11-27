//! Schema planning for the dashboard/CLI schema endpoints. Port of
//! LEGACY model/schema.clj: the `{blobs, refs}` schema shape the CLI pulls,
//! client-defs → schema conversion, schema diffing into tx-steps, and the
//! plan-level validation errors.

use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Index;

use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::attr::{Attr, AttrMap, CheckedDataType, ValueType};
use crate::system_catalog;

/// A map that iterates in insertion order with ordered-tree lookups. Legacy
/// parses the client's schema into Clojure array-maps, which keep the JSON
/// key order, and plans by walking them: a plan's steps follow the order the
/// client wrote its entities, attrs and links in (for maps of up to eight
/// keys; past that Clojure switches to hash maps, whose order is arbitrary).
#[derive(Debug, Clone)]
pub struct OrderedMap<K: Ord + Clone, V> {
    map: BTreeMap<K, V>,
    order: Vec<K>,
}

impl<K: Ord + Clone, V> Default for OrderedMap<K, V> {
    fn default() -> Self {
        OrderedMap {
            map: BTreeMap::new(),
            order: Vec::new(),
        }
    }
}

impl<K: Ord + Clone, V> OrderedMap<K, V> {
    /// Insert or replace; a replaced key keeps its first position (what
    /// `assoc` does to an existing key of a Clojure array-map).
    pub fn insert(&mut self, k: K, v: V) {
        if self.map.insert(k.clone(), v).is_none() {
            self.order.push(k);
        }
    }

    pub fn get<Q: Ord + ?Sized>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
    {
        self.map.get(k)
    }

    pub fn contains_key<Q: Ord + ?Sized>(&self, k: &Q) -> bool
    where
        K: Borrow<Q>,
    {
        self.map.contains_key(k)
    }

    pub fn entry_or_default(&mut self, k: K) -> &mut V
    where
        V: Default,
    {
        if !self.map.contains_key(&k) {
            self.order.push(k.clone());
        }
        self.map.entry(k).or_default()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> + '_ {
        self.order.iter().map(move |k| (k, &self.map[k]))
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> + '_ {
        self.order.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }
}

impl<K, V, Q> Index<&Q> for OrderedMap<K, V>
where
    K: Ord + Clone + Borrow<Q>,
    Q: Ord + ?Sized,
{
    type Output = V;
    fn index(&self, k: &Q) -> &V {
        &self.map[k]
    }
}

impl<'a, K: Ord + Clone, V> IntoIterator for &'a OrderedMap<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = Box<dyn Iterator<Item = (&'a K, &'a V)> + 'a>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

/// One attribute as the planner sees it (either an existing attr or one
/// described by client schema defs). `wire` is the JSON map the legacy
/// server emits for it in `current-schema` / `new-schema`.
#[derive(Debug, Clone)]
pub struct SchemaAttr {
    pub id: Option<Uuid>,
    pub value_type: ValueType,
    /// `"one"` / `"many"` for stored attrs. For a link from client defs it is
    /// legacy's `(keyword (:has forward))`: whatever string the client sent
    /// (`"few"` included), or null when `has` is missing or not a string.
    pub cardinality: Value,
    pub fwd_etype: String,
    pub fwd_label: String,
    pub rev_etype: Option<String>,
    pub rev_label: Option<String>,
    pub unique: bool,
    pub index: bool,
    pub required: bool,
    pub checked_data_type: Option<CheckedDataType>,
    /// `on-delete` / `on-delete-reverse`: `"cascade"` or none for stored
    /// attrs; for client links legacy's `(some-> onDelete keyword)`, i.e. any
    /// string the client sent (validated only when the plan is applied).
    pub on_delete: Option<String>,
    pub on_delete_reverse: Option<String>,
    pub is_system: bool,
    pub wire: Value,
}

impl SchemaAttr {
    fn from_attr(attr: &Attr) -> SchemaAttr {
        let mut wire = attr.to_wire();
        // $files.url is derived (never stored) but always returned, so it is
        // presented as required (schema.clj transform-$files-url-attr).
        if attr.is_system && attr.etype == "$files" && attr.label == "url" {
            wire["required?"] = json!(true);
        }
        SchemaAttr {
            id: Some(attr.id),
            value_type: attr.value_type,
            cardinality: json!(attr.cardinality.as_str()),
            fwd_etype: attr.etype.clone(),
            fwd_label: attr.label.clone(),
            rev_etype: attr.reverse_etype.clone(),
            rev_label: attr.reverse_label.clone(),
            unique: attr.is_unique,
            index: attr.is_indexed,
            required: attr.is_required,
            checked_data_type: attr.checked_data_type,
            on_delete: attr.on_delete_cascade.then(|| "cascade".to_string()),
            on_delete_reverse: attr
                .on_delete_reverse_cascade
                .then(|| "cascade".to_string()),
            is_system: attr.is_system,
            wire,
        }
    }

    fn fwd_name(&self) -> (String, String) {
        (self.fwd_etype.clone(), self.fwd_label.clone())
    }

    fn rev_name(&self) -> Option<(String, String)> {
        match (&self.rev_etype, &self.rev_label) {
            (Some(e), Some(l)) => Some((e.clone(), l.clone())),
            _ => None,
        }
    }
}

/// `{blobs: {etype: {label: attr}}, refs: {[fwd-etype fwd-label rev-etype rev-label]: attr}}`
#[derive(Debug, Clone, Default)]
pub struct Schema {
    pub blobs: OrderedMap<String, OrderedMap<String, SchemaAttr>>,
    pub refs: OrderedMap<[String; 4], SchemaAttr>,
}

/// Legacy serializes the vector keys of the refs map with Clojure `str`:
/// `["posts" "tags" "tags" "posts"]`.
pub fn ref_key_string(k: &[String; 4]) -> String {
    format!(
        "[{}]",
        k.iter()
            .map(|s| format!("{s:?}"))
            .collect::<Vec<_>>()
            .join(" ")
    )
}

impl Schema {
    pub fn to_wire(&self) -> Value {
        let mut blobs = Map::new();
        for (etype, attrs) in &self.blobs {
            let mut m = Map::new();
            for (label, a) in attrs {
                m.insert(label.clone(), a.wire.clone());
            }
            blobs.insert(etype.clone(), Value::Object(m));
        }
        let mut refs = Map::new();
        for (k, a) in &self.refs {
            refs.insert(ref_key_string(k), a.wire.clone());
        }
        json!({"refs": Value::Object(refs), "blobs": Value::Object(blobs)})
    }
}

/// Hidden system attrs (attr.clj remove-hidden): system-catalog attrs other
/// than the user-facing $users/$files/$streams, plus a few internal labels.
fn is_hidden(is_system: bool, etype: &str, label: &str) -> bool {
    if is_system && !matches!(etype, "$users" | "$files" | "$streams") {
        return true;
    }
    if etype == "$files"
        && matches!(
            label,
            "content-type" | "content-disposition" | "size" | "location-id" | "key-version"
        )
    {
        return true;
    }
    if etype == "$streams" && matches!(label, "machineId" | "hashedReconnectToken") {
        return true;
    }
    false
}

/// attrs->schema: the current catalog as `{blobs, refs}` with hidden system
/// attrs removed.
pub fn attrs_to_schema(attrs: &AttrMap) -> Schema {
    let mut schema = Schema::default();
    for attr in attrs.iter() {
        if is_hidden(attr.is_system, &attr.etype, &attr.label) {
            continue;
        }
        let sa = SchemaAttr::from_attr(attr);
        match attr.value_type {
            ValueType::Ref => {
                let key = [
                    attr.etype.clone(),
                    attr.label.clone(),
                    attr.reverse_etype.clone().unwrap_or_default(),
                    attr.reverse_label.clone().unwrap_or_default(),
                ];
                schema.refs.insert(key, sa);
            }
            ValueType::Blob => {
                schema
                    .blobs
                    .entry_or_default(attr.etype.clone())
                    .insert(attr.label.clone(), sa);
            }
        }
    }
    schema
}

fn str_of<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(|x| x.as_str())
}

/// defs->schema: client schema defs (`{entities: {etype: {attrs: {label:
/// {valueType, config: {unique, indexed}, required}}}}, links: {name:
/// {forward: {on, label, has, required?, onDelete?}, reverse: {...}}}}`)
/// as a `Schema`, with hidden system attrs dropped.
pub fn defs_to_schema(defs: &Value) -> Schema {
    let mut schema = Schema::default();
    if let Some(entities) = defs.get("entities").and_then(|e| e.as_object()) {
        for (etype, def) in entities {
            let is_system = etype.starts_with('$');
            let catalog = if is_system { "system" } else { "user" };
            let mut out = OrderedMap::default();
            if let Some(attrs) = def.get("attrs").and_then(|a| a.as_object()) {
                for (label, adef) in attrs {
                    if is_hidden(is_system, etype, label) {
                        continue;
                    }
                    let unique = adef
                        .get("config")
                        .and_then(|c| c.get("unique"))
                        .map(truthy)
                        .unwrap_or(false);
                    let index = adef
                        .get("config")
                        .and_then(|c| c.get("indexed"))
                        .map(truthy)
                        .unwrap_or(false);
                    let required = adef.get("required") == Some(&Value::Bool(true));
                    let checked_data_type =
                        str_of(adef, "valueType").and_then(|t| CheckedDataType::parse(t).ok());
                    let wire = json!({
                        "id": null,
                        "value-type": "blob",
                        "cardinality": "one",
                        "forward-identity": [null, etype, label],
                        "unique?": unique,
                        "index?": index,
                        "required?": required,
                        "checked-data-type": checked_data_type.map(|c| c.as_str()),
                        "catalog": catalog,
                    });
                    out.insert(
                        label.clone(),
                        SchemaAttr {
                            id: None,
                            value_type: ValueType::Blob,
                            cardinality: json!("one"),
                            fwd_etype: etype.clone(),
                            fwd_label: label.clone(),
                            rev_etype: None,
                            rev_label: None,
                            unique,
                            index,
                            required,
                            checked_data_type,
                            on_delete: None,
                            on_delete_reverse: None,
                            is_system,
                            wire,
                        },
                    );
                }
            }
            if !out.is_empty() {
                schema.blobs.insert(etype.clone(), out);
            }
        }
    }
    if let Some(links) = defs.get("links").and_then(|l| l.as_object()) {
        for link in links.values() {
            let fwd = link.get("forward").cloned().unwrap_or(Value::Null);
            let rev = link.get("reverse").cloned().unwrap_or(Value::Null);
            let fwd_on = str_of(&fwd, "on").unwrap_or_default().to_string();
            let fwd_label = str_of(&fwd, "label").unwrap_or_default().to_string();
            let rev_on = str_of(&rev, "on").unwrap_or_default().to_string();
            let rev_label = str_of(&rev, "label").unwrap_or_default().to_string();
            let on_delete = str_of(&fwd, "onDelete").map(|s| s.to_string());
            let on_delete_reverse = str_of(&rev, "onDelete").map(|s| s.to_string());
            // (keyword (:has forward)): any string is kept as is, anything
            // else becomes nil
            let cardinality = str_of(&fwd, "has").map_or(Value::Null, |h| json!(h));
            let unique = str_of(&rev, "has") == Some("one");
            let required = fwd.get("required") == Some(&Value::Bool(true));
            let wire = json!({
                "id": null,
                "value-type": "ref",
                "index?": false,
                "on-delete": on_delete,
                "on-delete-reverse": on_delete_reverse,
                "forward-identity": [null, fwd_on, fwd_label],
                "reverse-identity": [null, rev_on, rev_label],
                "cardinality": cardinality,
                "unique?": unique,
                "required?": required,
            });
            schema.refs.insert(
                [
                    fwd_on.clone(),
                    fwd_label.clone(),
                    rev_on.clone(),
                    rev_label.clone(),
                ],
                SchemaAttr {
                    id: None,
                    value_type: ValueType::Ref,
                    cardinality,
                    fwd_etype: fwd_on,
                    fwd_label,
                    rev_etype: Some(rev_on),
                    rev_label: Some(rev_label),
                    unique,
                    index: false,
                    required,
                    checked_data_type: None,
                    on_delete,
                    on_delete_reverse,
                    is_system: false,
                    wire,
                },
            );
        }
    }
    schema
}

fn truthy(v: &Value) -> bool {
    !matches!(v, Value::Null | Value::Bool(false))
}

/// remove-system-namespaces: strip system-catalog attrs and links from client
/// defs before planning (the CLI includes them in the schema it sends).
pub fn remove_system_namespaces(defs: &Value) -> Value {
    let reserved: HashSet<(String, String)> = system_catalog::all_attrs()
        .iter()
        .flat_map(|a| {
            let mut v = vec![(a.etype.clone(), a.label.clone())];
            if let (Some(e), Some(l)) = (&a.reverse_etype, &a.reverse_label) {
                v.push((e.clone(), l.clone()));
            }
            v
        })
        .collect();
    // schema.clj system-catalog-links: a link is dropped only when the
    // client's link map *equals* a catalog link's `{forward {on has label}
    // reverse {on has label}}`. A pulled schema writes `onDelete: "cascade"`
    // on $usersLinkedPrimaryUser, so that link is not dropped: it stays in
    // `new-schema` and is diffed against the stored system attr.
    let system_links: Vec<Value> = system_catalog::all_attrs()
        .iter()
        .filter(|a| a.value_type == ValueType::Ref)
        .map(|a| {
            json!({
                "forward": {"on": a.etype, "has": a.cardinality.as_str(), "label": a.label},
                "reverse": {"on": a.reverse_etype, "has": if a.is_unique {"one"} else {"many"}, "label": a.reverse_label},
            })
        })
        .collect();
    let mut out = defs.clone();
    if let Some(entities) = out.get_mut("entities").and_then(|e| e.as_object_mut()) {
        for (etype, def) in entities.iter_mut() {
            if let Some(attrs) = def.get_mut("attrs").and_then(|a| a.as_object_mut()) {
                // ucoll/filter-keys `dissoc!`s each reserved key from a
                // transient array-map, which moves the map's last entry into
                // the hole: the remaining attrs (and so the plan's steps)
                // come out in that order. swap_remove does the same.
                let drop: Vec<String> = attrs
                    .keys()
                    .filter(|label| reserved.contains(&(etype.clone(), (*label).clone())))
                    .cloned()
                    .collect();
                for label in drop {
                    attrs.swap_remove(&label);
                }
            }
        }
    }
    if let Some(links) = out.get_mut("links").and_then(|l| l.as_object_mut()) {
        links.retain(|_, link| !system_links.iter().any(|sys| sys == link));
    }
    out
}

/// A client schema whose shape the planner cannot read, as the
/// `param-malformed` path and offending value. Legacy answers these with a
/// typeless 500 (a non-map `schema`, `entities`, entity or `attrs` throws in
/// remove-system-namespaces) or plans a link with nil identities whose apply
/// then fails in SQL (a non-map link / `forward` / `reverse`, a missing or
/// non-string `on` / `label`). Null stands for "absent" everywhere, as in
/// legacy; an attr def that is not a map is read as an empty one (legacy
/// does the same).
pub fn malformed_defs(defs: &Value) -> Option<(Vec<String>, Value)> {
    fn path(parts: &[&str]) -> Vec<String> {
        ["body", "schema"]
            .iter()
            .chain(parts.iter())
            .map(|s| s.to_string())
            .collect()
    }
    let defs = match defs {
        Value::Null => return None,
        Value::Object(m) => m,
        other => return Some((path(&[]), other.clone())),
    };
    match defs.get("entities") {
        None | Some(Value::Null) => {}
        Some(Value::Object(entities)) => {
            for (etype, def) in entities {
                match def {
                    Value::Null => {}
                    Value::Object(d) => match d.get("attrs") {
                        None | Some(Value::Null) | Some(Value::Object(_)) => {}
                        Some(other) => {
                            return Some((path(&["entities", etype, "attrs"]), other.clone()))
                        }
                    },
                    other => return Some((path(&["entities", etype]), other.clone())),
                }
            }
        }
        Some(other) => return Some((path(&["entities"]), other.clone())),
    }
    match defs.get("links") {
        None | Some(Value::Null) => {}
        Some(Value::Object(links)) => {
            for (name, link) in links {
                let Value::Object(link) = link else {
                    return Some((path(&["links", name]), link.clone()));
                };
                for side in ["forward", "reverse"] {
                    let v = link.get(side).cloned().unwrap_or(Value::Null);
                    let Value::Object(side_map) = &v else {
                        return Some((path(&["links", name, side]), v));
                    };
                    for key in ["on", "label"] {
                        let k = side_map.get(key).cloned().unwrap_or(Value::Null);
                        if !k.is_string() {
                            return Some((path(&["links", name, side, key]), k));
                        }
                    }
                }
            }
        }
        Some(other) => return Some((path(&["links"]), other.clone())),
    }
    None
}

/// A link `has` other than one / many (`"few"`), which a plan carries
/// verbatim, refused when the plan is applied (plan-errors shape). Legacy
/// stores it as the attr's cardinality, leaving a link no client
/// understands. A missing `has` and an unknown `onDelete` are refused by the
/// transaction itself, at the point legacy's SQL fails on them
/// (attr::validate_link_values).
pub fn apply_errors(steps: &[Value]) -> Vec<Value> {
    let mut errors = vec![];
    for step in steps {
        let op = step.get(0).and_then(|v| v.as_str()).unwrap_or_default();
        if op != "add-attr" && op != "update-attr" {
            continue;
        }
        let attr = step.get(1).cloned().unwrap_or(Value::Null);
        if str_of(&attr, "value-type") != Some("ref") {
            continue;
        }
        let (fwd_name, _) = step_names(&attr);
        if let (Some(Value::String(c)), Some((etype, label))) = (attr.get("cardinality"), &fwd_name)
        {
            if c != "one" && c != "many" {
                errors.push(json!({"in": ["schema"], "message": format!(
                    "{etype}->{label}: Invalid link cardinality {}. A link's `has` must be 'one' or 'many'.",
                    json!(c)
                )}));
            }
        }
    }
    errors
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PlanOpts {
    pub check_types: bool,
    pub background_updates: bool,
}

fn job_step(op: &str, current: &SchemaAttr) -> Value {
    json!([op, {
        "attr-id": current.id,
        "forward-identity": current.wire.get("forward-identity").cloned().unwrap_or(Value::Null),
    }])
}

/// schemas->ops: diff `current` against `new` into tx-steps (attr steps plus,
/// with `background_updates`, indexing-job steps).
pub fn schemas_to_ops(opts: PlanOpts, current: &Schema, new: &Schema) -> Vec<Value> {
    let mut steps = vec![];
    // id attrs for new namespaces
    for ns in new.blobs.keys() {
        if !current.blobs.contains_key(ns) {
            steps.push(json!(["add-attr", {
                "value-type": "blob",
                "cardinality": "one",
                "id": Uuid::new_v4(),
                "forward-identity": [Uuid::new_v4(), ns, "id"],
                "unique?": true,
                "index?": false,
            }]));
        }
    }
    for (ns, attrs) in &new.blobs {
        for (label, new_attr) in attrs {
            if label == "id" {
                continue;
            }
            let current_attr = current.blobs.get(ns).and_then(|m| m.get(label));
            let Some(cur) = current_attr else {
                let mut attr = json!({
                    "value-type": "blob",
                    "cardinality": "one",
                    "id": Uuid::new_v4(),
                    "forward-identity": [Uuid::new_v4(), ns, label],
                    "unique?": new_attr.unique,
                    "index?": new_attr.index,
                    "required?": new_attr.required,
                });
                if opts.check_types {
                    if let Some(cdt) = new_attr.checked_data_type {
                        attr["checked-data-type"] = json!(cdt.as_str());
                    }
                }
                steps.push(json!(["add-attr", attr]));
                continue;
            };
            let changed_type =
                opts.check_types && new_attr.checked_data_type != cur.checked_data_type;
            let changed_unique = new_attr.unique != cur.unique;
            let changed_index = new_attr.index != cur.index;
            let changed_required = new_attr.required != cur.required;
            let attr_changed = changed_unique || changed_index || changed_required;
            if attr_changed && !opts.background_updates {
                steps.push(json!(["update-attr", {
                    "value-type": "blob",
                    "cardinality": "one",
                    "id": cur.id,
                    "forward-identity": cur.wire.get("forward-identity").cloned().unwrap_or(Value::Null),
                    "unique?": new_attr.unique,
                    "index?": new_attr.index,
                    "required?": new_attr.required,
                }]));
            }
            if changed_index && opts.background_updates {
                steps.push(job_step(
                    if new_attr.index {
                        "index"
                    } else {
                        "remove-index"
                    },
                    cur,
                ));
            }
            if changed_unique && opts.background_updates {
                steps.push(job_step(
                    if new_attr.unique {
                        "unique"
                    } else {
                        "remove-unique"
                    },
                    cur,
                ));
            }
            if changed_required && opts.background_updates {
                steps.push(job_step(
                    if new_attr.required {
                        "required"
                    } else {
                        "remove-required"
                    },
                    cur,
                ));
            }
            if changed_type && !cur.is_system {
                match new_attr.checked_data_type {
                    Some(cdt) => steps.push(json!(["check-data-type", {
                        "attr-id": cur.id,
                        "checked-data-type": cdt.as_str(),
                        "forward-identity": cur.wire.get("forward-identity").cloned().unwrap_or(Value::Null),
                    }])),
                    None => steps.push(job_step("remove-data-type", cur)),
                }
            }
        }
    }
    for (key, new_attr) in &new.refs {
        let cur = current.refs.get(key);
        let Some(cur) = cur else {
            steps.push(json!(["add-attr", {
                "value-type": "ref",
                "id": Uuid::new_v4(),
                "forward-identity": [Uuid::new_v4(), key[0], key[1]],
                "reverse-identity": [Uuid::new_v4(), key[2], key[3]],
                "cardinality": new_attr.cardinality,
                "unique?": new_attr.unique,
                "index?": new_attr.index,
                "required?": new_attr.required,
                "on-delete": new_attr.on_delete,
                "on-delete-reverse": new_attr.on_delete_reverse,
            }]));
            continue;
        };
        let changed_required = new_attr.required != cur.required;
        let changed_updatable = new_attr.cardinality != cur.cardinality
            || new_attr.unique != cur.unique
            || new_attr.on_delete != cur.on_delete
            || new_attr.on_delete_reverse != cur.on_delete_reverse;
        if !changed_required && !changed_updatable {
            continue;
        }
        if changed_required && opts.background_updates {
            steps.push(job_step(
                if new_attr.required {
                    "required"
                } else {
                    "remove-required"
                },
                cur,
            ));
        }
        if changed_updatable || !opts.background_updates {
            steps.push(json!(["update-attr", {
                "value-type": "ref",
                "id": cur.id,
                "forward-identity": cur.wire.get("forward-identity").cloned().unwrap_or(Value::Null),
                "reverse-identity": cur.wire.get("reverse-identity").cloned().unwrap_or(Value::Null),
                "cardinality": new_attr.cardinality,
                "unique?": new_attr.unique,
                "index?": new_attr.index,
                "required?": new_attr.required,
                "on-delete": new_attr.on_delete,
                "on-delete-reverse": new_attr.on_delete_reverse,
            }]));
        }
    }
    steps
}

fn dup_message((etype, label): &(String, String)) -> String {
    format!(
        "{etype}->{label}: Duplicate entry found for attribute. \
         Check your schema file for duplicate link definitions. \
         If it's not in the schema file, it may have been generated by the backend. \
         Check your full schema in the dashboard: \
         https://www.instantdb.com/dash?s=main&t=explorer"
    )
}

fn backwards_link_message((etype, label): &(String, String)) -> String {
    format!(
        "{etype}->{label}: Conflicting link found for attribute. \
         It's possible that you already have a link with the same label names, but in the reverse direction. \
         We cannot automatically swap the direction of the link. \
         To fix this, can: a) swap the `forward` and `reverse` parameters for this link in your schema file, or b) delete the existing link in the dashboard.\
         Check your full schema in the dashboard for a link with the same label names: \
         https://www.instantdb.com/dash?s=main&t=explorer"
    )
}

fn cascade_message((etype, label): &(String, String)) -> String {
    format!(
        "{etype}->{label}: Cascade delete is only possible on links with `has: 'one'`. \
         Check your full schema in the dashboard: \
         https://www.instantdb.com/dash?s=main&t=explorer"
    )
}

type IdentName = (String, String);

/// Identity names of a step's attr map: (fwd [etype label], rev [etype label]).
fn step_names(attr: &Value) -> (Option<IdentName>, Option<IdentName>) {
    let name = |key: &str| {
        attr.get(key).and_then(|v| v.as_array()).and_then(|a| {
            if a.len() >= 3 {
                Some((
                    a[1].as_str().unwrap_or_default().to_string(),
                    a[2].as_str().unwrap_or_default().to_string(),
                ))
            } else {
                None
            }
        })
    };
    (name("forward-identity"), name("reverse-identity"))
}

/// plan-errors: conflicts between planned add-attr steps and the existing
/// catalog, plus invalid cascade settings. Each error is `{in: ["schema"],
/// message}`.
pub fn plan_errors(current_attrs: &AttrMap, steps: &[Value]) -> Vec<Value> {
    let mut fwd_map: HashMap<(String, String), Option<(String, String)>> = HashMap::new();
    let mut rev_map: HashMap<(String, String), (String, String)> = HashMap::new();
    let mut blob_idents: BTreeSet<(String, String)> = BTreeSet::new();
    for attr in current_attrs.iter() {
        let sa = SchemaAttr::from_attr(attr);
        match attr.value_type {
            ValueType::Ref => {
                fwd_map.insert(sa.fwd_name(), sa.rev_name());
                if let Some(rev) = sa.rev_name() {
                    rev_map.insert(rev, sa.fwd_name());
                }
            }
            ValueType::Blob => {
                blob_idents.insert(sa.fwd_name());
            }
        }
    }
    let mut errors = vec![];
    for step in steps {
        let op = step.get(0).and_then(|v| v.as_str()).unwrap_or_default();
        let attr = step.get(1).cloned().unwrap_or(Value::Null);
        if op != "add-attr" {
            continue;
        }
        let (fwd_name, rev_name) = step_names(&attr);
        let Some(fwd_name) = fwd_name else { continue };
        let current_rev_name = rev_map.get(&fwd_name).cloned();
        let message = if !(rev_name.is_none() && current_rev_name.is_none())
            && rev_name == current_rev_name
        {
            Some(backwards_link_message(&fwd_name))
        } else if fwd_map.contains_key(&fwd_name) || rev_map.contains_key(&fwd_name) {
            Some(dup_message(&fwd_name))
        } else if let Some(rev) = rev_name
            .as_ref()
            .filter(|r| fwd_map.contains_key(*r) || rev_map.contains_key(*r))
        {
            Some(dup_message(rev))
        } else if blob_idents.contains(&fwd_name) {
            Some(dup_message(&fwd_name))
        } else {
            None
        };
        if let Some(message) = message {
            errors.push(json!({"in": ["schema"], "message": message}));
        }
    }
    for step in steps {
        let op = step.get(0).and_then(|v| v.as_str()).unwrap_or_default();
        let attr = step.get(1).cloned().unwrap_or(Value::Null);
        if op != "add-attr" && op != "update-attr" {
            continue;
        }
        let (fwd_name, rev_name) = step_names(&attr);
        let is_ref = str_of(&attr, "value-type") == Some("ref");
        let many = str_of(&attr, "cardinality") == Some("many");
        let unique = attr.get("unique?") == Some(&Value::Bool(true));
        let cascade = str_of(&attr, "on-delete") == Some("cascade");
        let cascade_rev = str_of(&attr, "on-delete-reverse") == Some("cascade");
        let message = if is_ref && many && cascade {
            fwd_name.as_ref().map(cascade_message)
        } else if is_ref && !unique && cascade_rev {
            rev_name.as_ref().map(cascade_message)
        } else {
            None
        };
        if let Some(message) = message {
            errors.push(json!({"in": ["schema"], "message": message}));
        }
    }
    errors
}

/// Job-type steps understood by the indexing-job runner (indexing_jobs.clj
/// `jobs`), with their serial keys.
pub fn job_serial_key(op: &str) -> Option<&'static str> {
    Some(match op {
        "check-data-type" | "remove-data-type" => "data-type",
        "index" | "remove-index" => "index",
        "unique" | "remove-unique" => "unique",
        "required" | "remove-required" => "required",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attr::Cardinality;

    fn attr(etype: &str, label: &str, unique: bool) -> Attr {
        Attr {
            id: Uuid::new_v4(),
            value_type: ValueType::Blob,
            cardinality: Cardinality::One,
            forward_ident: Uuid::new_v4(),
            etype: etype.into(),
            label: label.into(),
            reverse_ident: None,
            reverse_etype: None,
            reverse_label: None,
            is_unique: unique,
            is_indexed: false,
            is_required: false,
            checked_data_type: None,
            on_delete_cascade: false,
            on_delete_reverse_cascade: false,
            is_system: false,
            indexing: false,
            checking_data_type: false,
            setting_unique: false,
            inferred_types: None,
            metadata: None,
        }
    }

    fn link(fwd: (&str, &str), rev: (&str, &str), many: bool) -> Attr {
        let mut a = attr(fwd.0, fwd.1, false);
        a.value_type = ValueType::Ref;
        a.cardinality = if many {
            Cardinality::Many
        } else {
            Cardinality::One
        };
        a.reverse_ident = Some(Uuid::new_v4());
        a.reverse_etype = Some(rev.0.into());
        a.reverse_label = Some(rev.1.into());
        a
    }

    fn attr_map(attrs: Vec<Attr>) -> AttrMap {
        let mut m = AttrMap::default();
        for a in attrs {
            m.insert(a);
        }
        m
    }

    #[test]
    fn ref_keys_use_clojure_vector_printing() {
        let k = ["posts".into(), "tags".into(), "tags".into(), "posts".into()];
        assert_eq!(ref_key_string(&k), r#"["posts" "tags" "tags" "posts"]"#);
    }

    #[test]
    fn attrs_to_schema_hides_internal_system_attrs() {
        let mut attrs = attr_map(vec![
            attr("posts", "id", true),
            attr("posts", "title", false),
        ]);
        for a in system_catalog::all_attrs() {
            attrs.insert(a);
        }
        let schema = attrs_to_schema(&attrs);
        assert!(schema.blobs.contains_key("posts"));
        assert!(schema.blobs.contains_key("$users"));
        assert!(!schema.blobs.contains_key("$magicCodes"));
        let files = &schema.blobs["$files"];
        assert!(!files.contains_key("location-id"));
        assert_eq!(files["url"].wire["required?"], json!(true));
        assert_eq!(files["url"].wire["catalog"], json!("system"));
    }

    #[test]
    fn plan_adds_namespace_and_attrs() {
        let current = attrs_to_schema(&attr_map(vec![]));
        let defs = json!({
            "entities": {"posts": {"attrs": {
                "title": {"valueType": "string", "config": {"unique": false, "indexed": true}, "required": true},
                "views": {"valueType": "number", "config": {"unique": false, "indexed": false}},
            }}},
            "links": {"postsAuthor": {
                "forward": {"on": "posts", "has": "one", "label": "author"},
                "reverse": {"on": "users", "has": "many", "label": "posts"},
            }},
        });
        let new = defs_to_schema(&defs);
        let steps = schemas_to_ops(
            PlanOpts {
                check_types: true,
                background_updates: false,
            },
            &current,
            &new,
        );
        let ops: Vec<(String, String)> = steps
            .iter()
            .map(|s| {
                let a = &s[1];
                (
                    s[0].as_str().unwrap().to_string(),
                    format!(
                        "{}.{}",
                        a["forward-identity"][1].as_str().unwrap(),
                        a["forward-identity"][2].as_str().unwrap()
                    ),
                )
            })
            .collect();
        assert!(ops.contains(&("add-attr".into(), "posts.id".into())));
        assert!(ops.contains(&("add-attr".into(), "posts.title".into())));
        assert!(ops.contains(&("add-attr".into(), "posts.views".into())));
        assert!(ops.contains(&("add-attr".into(), "posts.author".into())));
        assert_eq!(ops.len(), 4);
        let title = steps
            .iter()
            .find(|s| s[1]["forward-identity"][2] == "title")
            .unwrap();
        assert_eq!(title[1]["checked-data-type"], json!("string"));
        assert_eq!(title[1]["required?"], json!(true));
        assert_eq!(title[1]["index?"], json!(true));
        let author = steps
            .iter()
            .find(|s| s[1]["forward-identity"][2] == "author")
            .unwrap();
        assert_eq!(author[1]["value-type"], json!("ref"));
        assert_eq!(author[1]["cardinality"], json!("one"));
        assert_eq!(author[1]["unique?"], json!(false));
        assert_eq!(author[1]["reverse-identity"][1], json!("users"));
    }

    #[test]
    fn plan_flag_changes_use_jobs_with_background_updates() {
        let mut title = attr("posts", "title", false);
        title.checked_data_type = Some(CheckedDataType::String);
        let existing = attr_map(vec![attr("posts", "id", true), title]);
        let current = attrs_to_schema(&existing);
        let defs = json!({"entities": {"posts": {"attrs": {
            "title": {"valueType": "number", "config": {"unique": true, "indexed": true}, "required": true},
        }}}, "links": {}});
        let new = defs_to_schema(&defs);
        let bg = schemas_to_ops(
            PlanOpts {
                check_types: true,
                background_updates: true,
            },
            &current,
            &new,
        );
        let ops: Vec<&str> = bg.iter().map(|s| s[0].as_str().unwrap()).collect();
        assert_eq!(ops, vec!["index", "unique", "required", "check-data-type"]);
        assert_eq!(bg[3][1]["checked-data-type"], json!("number"));
        let sync = schemas_to_ops(
            PlanOpts {
                check_types: false,
                background_updates: false,
            },
            &current,
            &new,
        );
        let ops: Vec<&str> = sync.iter().map(|s| s[0].as_str().unwrap()).collect();
        assert_eq!(ops, vec!["update-attr"]);
        assert_eq!(sync[0][1]["unique?"], json!(true));
    }

    #[test]
    fn plan_errors_detect_duplicates_and_backwards_links() {
        let existing = attr_map(vec![
            attr("posts", "name", false),
            link(("posts", "tags"), ("tags", "posts"), true),
        ]);
        let mk = |fwd: (&str, &str), rev: Option<(&str, &str)>| {
            json!(["add-attr", {
                "value-type": if rev.is_some() {"ref"} else {"blob"},
                "id": Uuid::new_v4(),
                "forward-identity": [Uuid::new_v4(), fwd.0, fwd.1],
                "reverse-identity": rev.map(|r| json!([Uuid::new_v4(), r.0, r.1])),
                "cardinality": "many",
                "unique?": false,
                "index?": false,
            }])
        };
        let steps = vec![
            mk(("tags", "posts"), Some(("posts", "tags"))), // backwards
            mk(("posts", "tags2"), Some(("tags", "posts"))), // dup rev
            mk(("tags", "posts2"), Some(("posts", "tags"))), // dup fwd (as rev)
            mk(("posts", "name"), None),                    // dup blob
            mk(("posts", "content"), None),                 // ok
        ];
        let errors = plan_errors(&existing, &steps);
        assert_eq!(errors.len(), 4, "{errors:?}");
        assert!(errors[0]["message"]
            .as_str()
            .unwrap()
            .starts_with("tags->posts: Conflicting link"));
        assert!(errors[1]["message"]
            .as_str()
            .unwrap()
            .starts_with("tags->posts: Duplicate"));
        assert!(errors[2]["message"]
            .as_str()
            .unwrap()
            .starts_with("posts->tags: Duplicate"));
        assert!(errors[3]["message"]
            .as_str()
            .unwrap()
            .starts_with("posts->name: Duplicate"));
    }

    #[test]
    fn plan_errors_reject_cascade_on_many() {
        let existing = attr_map(vec![]);
        let steps = vec![json!(["add-attr", {
            "value-type": "ref",
            "id": Uuid::new_v4(),
            "forward-identity": [Uuid::new_v4(), "posts", "comments"],
            "reverse-identity": [Uuid::new_v4(), "comments", "post"],
            "cardinality": "many",
            "unique?": false,
            "index?": false,
            "on-delete": "cascade",
        }])];
        let errors = plan_errors(&existing, &steps);
        assert_eq!(errors.len(), 1);
        assert!(errors[0]["message"]
            .as_str()
            .unwrap()
            .starts_with("posts->comments: Cascade delete"));
    }

    #[test]
    fn remove_system_namespaces_strips_catalog_attrs() {
        let sys_link = system_catalog::all_attrs()
            .into_iter()
            .find(|a| a.value_type == ValueType::Ref)
            .unwrap();
        let defs = json!({
            "entities": {
                "$users": {"attrs": {"email": {"valueType": "string"}, "nickname": {"valueType": "string"}}},
                "posts": {"attrs": {"title": {"valueType": "string"}}},
            },
            "links": {
                "systemLink": {
                    "forward": {"on": sys_link.etype, "has": sys_link.cardinality.as_str(), "label": sys_link.label},
                    "reverse": {"on": sys_link.reverse_etype, "has": if sys_link.is_unique {"one"} else {"many"}, "label": sys_link.reverse_label},
                },
                "postsAuthor": {
                    "forward": {"on": "posts", "has": "one", "label": "author"},
                    "reverse": {"on": "$users", "has": "many", "label": "posts"},
                },
            },
        });
        let out = remove_system_namespaces(&defs);
        assert!(out["entities"]["$users"]["attrs"].get("email").is_none());
        assert!(out["entities"]["$users"]["attrs"].get("nickname").is_some());
        assert!(out["links"].get("postsAuthor").is_some());
        assert!(out["links"].get("systemLink").is_none());
    }

    #[test]
    fn remove_system_namespaces_reorders_like_a_transient_dissoc() {
        let a = json!({"valueType": "string"});
        let defs = json!({"entities": {
            "$files": {"attrs": {"a": a, "url": a, "b": a, "c": a}},
            "$users": {"attrs": {"email": a, "x": a, "y": a, "z": a}},
        }});
        let out = remove_system_namespaces(&defs);
        let keys = |e: &str| -> Vec<String> {
            out["entities"][e]["attrs"]
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect()
        };
        // legacy: (dissoc! url) moves the last entry, c, into url's slot
        assert_eq!(keys("$files"), vec!["a", "c", "b"]);
        assert_eq!(keys("$users"), vec!["z", "x", "y"]);
    }

    #[test]
    fn remove_system_namespaces_keeps_system_links_with_extra_keys() {
        let sys_link = system_catalog::all_attrs()
            .into_iter()
            .find(|a| a.value_type == ValueType::Ref)
            .unwrap();
        let fwd = json!({"on": sys_link.etype, "has": sys_link.cardinality.as_str(), "label": sys_link.label});
        let rev = json!({"on": sys_link.reverse_etype, "has": if sys_link.is_unique {"one"} else {"many"}, "label": sys_link.reverse_label});
        let mut fwd_cascade = fwd.clone();
        fwd_cascade["onDelete"] = json!("cascade");
        let defs = json!({"links": {
            // a pulled schema's $usersLinkedPrimaryUser carries onDelete
            "withOnDelete": {"forward": fwd_cascade, "reverse": rev},
            "withExtraKey": {"forward": fwd, "reverse": rev, "extra": 1},
            "exact": {"reverse": rev, "forward": fwd},
        }});
        let out = remove_system_namespaces(&defs);
        assert!(out["links"].get("withOnDelete").is_some());
        assert!(out["links"].get("withExtraKey").is_some());
        assert!(out["links"].get("exact").is_none());
    }

    fn step_idents(steps: &[Value]) -> Vec<String> {
        steps
            .iter()
            .map(|s| {
                let a = &s[1];
                format!(
                    "{} {}.{}",
                    s[0].as_str().unwrap(),
                    a["forward-identity"][1].as_str().unwrap(),
                    a["forward-identity"][2].as_str().unwrap()
                )
            })
            .collect()
    }

    #[test]
    fn plan_follows_client_key_order() {
        let current = attrs_to_schema(&attr_map(vec![]));
        let defs = json!({
            "entities": {
                "zebras": {"attrs": {"stripes": {"valueType": "number"}, "age": {"valueType": "number"}}},
                "apes": {"attrs": {"name": {"valueType": "string"}}},
            },
            "links": {
                "zLink": {
                    "forward": {"on": "zebras", "has": "one", "label": "keeper"},
                    "reverse": {"on": "apes", "has": "many", "label": "zebras"},
                },
                "aLink": {
                    "forward": {"on": "apes", "has": "many", "label": "friends"},
                    "reverse": {"on": "zebras", "has": "many", "label": "apeFriends"},
                },
            },
        });
        let new = defs_to_schema(&defs);
        let steps = schemas_to_ops(PlanOpts::default(), &current, &new);
        assert_eq!(
            step_idents(&steps),
            vec![
                "add-attr zebras.id",
                "add-attr apes.id",
                "add-attr zebras.stripes",
                "add-attr zebras.age",
                "add-attr apes.name",
                "add-attr zebras.keeper",
                "add-attr apes.friends",
            ]
        );
        let keys: Vec<&String> = new.blobs.keys().collect();
        assert_eq!(keys, vec!["zebras", "apes"]);
    }

    #[test]
    fn plan_keeps_raw_link_cardinality_and_on_delete() {
        let existing = attr_map(vec![link(("posts", "tags"), ("tags", "posts"), true)]);
        let current = attrs_to_schema(&existing);
        let defs = json!({"links": {
            "postsTags": {
                "forward": {"on": "posts", "label": "tags"},
                "reverse": {"on": "tags", "has": "many", "label": "posts", "onDelete": "restrict"},
            },
            "postsAuthor": {
                "forward": {"on": "posts", "has": "few", "label": "author"},
                "reverse": {"on": "users", "has": "many", "label": "posts"},
            },
        }});
        let new = defs_to_schema(&defs);
        let steps = schemas_to_ops(
            PlanOpts {
                check_types: true,
                background_updates: true,
            },
            &current,
            &new,
        );
        assert_eq!(
            step_idents(&steps),
            vec!["update-attr posts.tags", "add-attr posts.author"]
        );
        assert_eq!(steps[0][1]["cardinality"], Value::Null);
        assert_eq!(steps[0][1]["on-delete-reverse"], json!("restrict"));
        assert_eq!(steps[1][1]["cardinality"], json!("few"));
        assert_eq!(
            new.to_wire()["refs"][r#"["posts" "author" "users" "posts"]"#]["cardinality"],
            json!("few")
        );
        let errors = apply_errors(&steps);
        let messages: Vec<&str> = errors
            .iter()
            .map(|e| e["message"].as_str().unwrap())
            .collect();
        assert_eq!(
            messages,
            vec!["posts->author: Invalid link cardinality \"few\". A link's `has` must be 'one' or 'many'."]
        );
        // the transaction refuses the others where legacy's SQL fails
        let restrict = &steps[0][1];
        let err = crate::attr::validate_link_values(restrict, false).unwrap_err();
        assert_eq!(
            err.message,
            "Validation failed for attributes: tags->posts: Invalid onDelete \"restrict\". The only supported onDelete is 'cascade'."
        );
        // a null cardinality is skipped by update-attr, refused by add-attr
        let missing = json!({
            "value-type": "ref",
            "forward-identity": [Uuid::new_v4(), "a", "b"],
            "reverse-identity": [Uuid::new_v4(), "c", "d"],
            "cardinality": null,
        });
        assert!(crate::attr::validate_link_values(&missing, false).is_ok());
        assert_eq!(
            crate::attr::validate_link_values(&missing, true).unwrap_err().message,
            "Validation failed for attributes: a->b: Missing link cardinality. A link's `has` must be 'one' or 'many'."
        );
    }

    #[test]
    fn malformed_defs_names_the_unreadable_part() {
        let path = |v: Value| malformed_defs(&v).map(|(p, _)| p.join("."));
        assert_eq!(path(json!(null)), None);
        assert_eq!(path(json!({})), None);
        assert_eq!(path(json!({"entities": null, "links": null})), None);
        assert_eq!(
            path(
                json!({"entities": {"a": null, "b": {"attrs": null}, "c": {"attrs": {"x": "str"}}}})
            ),
            None
        );
        assert_eq!(path(json!("x")), Some("body.schema".into()));
        assert_eq!(
            path(json!({"entities": [1]})),
            Some("body.schema.entities".into())
        );
        assert_eq!(
            path(json!({"entities": {"p": "str"}})),
            Some("body.schema.entities.p".into())
        );
        assert_eq!(
            path(json!({"entities": {"p": {"attrs": []}}})),
            Some("body.schema.entities.p.attrs".into())
        );
        assert_eq!(path(json!({"links": []})), Some("body.schema.links".into()));
        assert_eq!(
            path(json!({"links": {"l": "x"}})),
            Some("body.schema.links.l".into())
        );
        assert_eq!(
            path(json!({"links": {"l": {"reverse": {"on": "a", "label": "b"}}}})),
            Some("body.schema.links.l.forward".into())
        );
        assert_eq!(
            path(
                json!({"links": {"l": {"forward": {"on": "a", "label": "b"}, "reverse": {"on": "c", "label": 3}}}})
            ),
            Some("body.schema.links.l.reverse.label".into())
        );
    }
}
