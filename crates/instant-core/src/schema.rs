//! Schema planning for the dashboard/CLI schema endpoints. Port of
//! LEGACY model/schema.clj: the `{blobs, refs}` schema shape the CLI pulls,
//! client-defs → schema conversion, schema diffing into tx-steps, and the
//! plan-level validation errors.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::attr::{Attr, AttrMap, Cardinality, CheckedDataType, ValueType};
use crate::system_catalog;

/// One attribute as the planner sees it (either an existing attr or one
/// described by client schema defs). `wire` is the JSON map the legacy
/// server emits for it in `current-schema` / `new-schema`.
#[derive(Debug, Clone)]
pub struct SchemaAttr {
    pub id: Option<Uuid>,
    pub value_type: ValueType,
    pub cardinality: Cardinality,
    pub fwd_etype: String,
    pub fwd_label: String,
    pub rev_etype: Option<String>,
    pub rev_label: Option<String>,
    pub unique: bool,
    pub index: bool,
    pub required: bool,
    pub checked_data_type: Option<CheckedDataType>,
    pub on_delete_cascade: bool,
    pub on_delete_reverse_cascade: bool,
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
            cardinality: attr.cardinality,
            fwd_etype: attr.etype.clone(),
            fwd_label: attr.label.clone(),
            rev_etype: attr.reverse_etype.clone(),
            rev_label: attr.reverse_label.clone(),
            unique: attr.is_unique,
            index: attr.is_indexed,
            required: attr.is_required,
            checked_data_type: attr.checked_data_type,
            on_delete_cascade: attr.on_delete_cascade,
            on_delete_reverse_cascade: attr.on_delete_reverse_cascade,
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
    pub blobs: BTreeMap<String, BTreeMap<String, SchemaAttr>>,
    pub refs: BTreeMap<[String; 4], SchemaAttr>,
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
                    .entry(attr.etype.clone())
                    .or_default()
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
            let mut out = BTreeMap::new();
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
                            cardinality: Cardinality::One,
                            fwd_etype: etype.clone(),
                            fwd_label: label.clone(),
                            rev_etype: None,
                            rev_label: None,
                            unique,
                            index,
                            required,
                            checked_data_type,
                            on_delete_cascade: false,
                            on_delete_reverse_cascade: false,
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
            let cardinality = match str_of(&fwd, "has") {
                Some("many") => Cardinality::Many,
                _ => Cardinality::One,
            };
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
                "cardinality": cardinality.as_str(),
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
                    on_delete_cascade: on_delete.as_deref() == Some("cascade"),
                    on_delete_reverse_cascade: on_delete_reverse.as_deref() == Some("cascade"),
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
    let system_links: HashSet<Value> = system_catalog::all_attrs()
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
                attrs.retain(|label, _| !reserved.contains(&(etype.clone(), label.clone())));
            }
        }
    }
    if let Some(links) = out.get_mut("links").and_then(|l| l.as_object_mut()) {
        links.retain(|_, link| {
            let key = json!({
                "forward": {
                    "on": link.get("forward").and_then(|f| f.get("on")),
                    "has": link.get("forward").and_then(|f| f.get("has")),
                    "label": link.get("forward").and_then(|f| f.get("label")),
                },
                "reverse": {
                    "on": link.get("reverse").and_then(|f| f.get("on")),
                    "has": link.get("reverse").and_then(|f| f.get("has")),
                    "label": link.get("reverse").and_then(|f| f.get("label")),
                },
            });
            !system_links.contains(&key)
        });
    }
    out
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
                "cardinality": new_attr.cardinality.as_str(),
                "unique?": new_attr.unique,
                "index?": new_attr.index,
                "required?": new_attr.required,
                "on-delete": new_attr.on_delete_cascade.then_some("cascade"),
                "on-delete-reverse": new_attr.on_delete_reverse_cascade.then_some("cascade"),
            }]));
            continue;
        };
        let changed_required = new_attr.required != cur.required;
        let changed_updatable = new_attr.cardinality != cur.cardinality
            || new_attr.unique != cur.unique
            || new_attr.on_delete_cascade != cur.on_delete_cascade
            || new_attr.on_delete_reverse_cascade != cur.on_delete_reverse_cascade;
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
                "cardinality": new_attr.cardinality.as_str(),
                "unique?": new_attr.unique,
                "index?": new_attr.index,
                "required?": new_attr.required,
                "on-delete": new_attr.on_delete_cascade.then_some("cascade"),
                "on-delete-reverse": new_attr.on_delete_reverse_cascade.then_some("cascade"),
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
}
