//! Per-session op scheduling, legacy's receive queue (reactive/session.clj
//! `group-key` / `combine`, :1463-1553). Every client op gets a group key;
//! ops with the same key run one at a time in arrival order, ops with
//! different keys run concurrently, so a slow transact no longer holds up
//! a presence update or a query on the same session. While an op waits
//! behind its key, a newer `set-presence` replaces it, consecutive
//! `append-stream`s merge, and a transact that rewrites the same
//! cardinality-one values takes over the one before it, like legacy's
//! `combine` methods.
//!
//! Each queued op holds one of the session's in-flight permits; the socket
//! reader waits for a permit before reading on, so a client that floods a
//! session is back-pressured instead of queueing without bound. Ops that
//! have not started when the session closes are dropped (legacy cancels a
//! closed session's pending handlers).

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use futures::FutureExt;
use serde_json::{json, Value};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use instant_core::attr::{AttrMap, Cardinality};
use uuid::Uuid;

use crate::state::{AppState, Session};

/// Key of a combined transact that lists the events it took over; they are
/// answered with its result (legacy `:redundant-events`). Removed before the
/// handler sees the event.
pub(crate) const REDUNDANT: &str = "__redundant-events";

/// Ops of one session queued or running at once.
const MAX_IN_FLIGHT: usize = 256;

pub struct Scheduler {
    /// group key -> ops waiting behind the running one; a key is present
    /// exactly while its lane has a worker
    lanes: std::sync::Mutex<HashMap<String, VecDeque<Queued>>>,
    permits: Arc<Semaphore>,
    closed: AtomicBool,
    /// ungrouped ops each get a lane of their own
    solo: AtomicU64,
}

struct Queued {
    msg: Value,
    _permit: OwnedSemaphorePermit,
}

impl Default for Scheduler {
    fn default() -> Self {
        Scheduler {
            lanes: Default::default(),
            permits: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
            closed: AtomicBool::new(false),
            solo: AtomicU64::new(0),
        }
    }
}

impl Scheduler {
    /// Drop the ops that have not started; the running ones finish.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.lanes
            .lock()
            .unwrap()
            .values_mut()
            .for_each(VecDeque::clear);
    }

    /// `close`, then wait for the running ops (each bounded by the
    /// handle-receive timeout), so session cleanup can't race a handler
    /// that is still joining a room or registering a query.
    pub async fn shutdown(&self) {
        self.close();
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            self.permits.acquire_many(MAX_IN_FLIGHT as u32),
        )
        .await;
    }
}

/// Queue `msg` on its lane; returns once it holds an in-flight permit.
pub async fn dispatch(state: &Arc<AppState>, session: &Arc<Session>, msg: Value) {
    let sched = &session.scheduler;
    let Ok(permit) = sched.permits.clone().acquire_owned().await else {
        return;
    };
    if sched.closed.load(Ordering::Relaxed) {
        return;
    }
    let key = group_key(sched, &msg);
    // legacy's matching-steps? needs the attrs; without a cached copy
    // nothing combines (its get-attrs is nil then too)
    let attrs = if msg.get("op").and_then(Value::as_str) == Some("transact") {
        let app_id = session.state.lock().await.app_id;
        app_id.and_then(|id| state.attr_cache.get(&id).map(|e| e.attrs.clone()))
    } else {
        None
    };
    let queued = Queued {
        msg,
        _permit: permit,
    };
    {
        let mut lanes = sched.lanes.lock().unwrap();
        if let Some(waiting) = lanes.get_mut(&key) {
            if let Some(last) = waiting.back_mut() {
                if let Some(merged) = combine(&last.msg, &queued.msg, attrs.as_deref()) {
                    // the older op's permit goes with it
                    *last = Queued {
                        msg: merged,
                        _permit: queued._permit,
                    };
                    return;
                }
            }
            waiting.push_back(queued);
            return;
        }
        lanes.insert(key.clone(), VecDeque::new());
    }
    tokio::spawn(run_lane(state.clone(), session.clone(), key, queued));
}

async fn run_lane(state: Arc<AppState>, session: Arc<Session>, key: String, first: Queued) {
    let mut next = Some(first);
    while let Some(op) = next.take() {
        if !session.scheduler.closed.load(Ordering::Relaxed) {
            // a panicking handler must not wedge its lane
            let run = crate::ws::handle_message(&state, &session, op.msg);
            if std::panic::AssertUnwindSafe(run)
                .catch_unwind()
                .await
                .is_err()
            {
                tracing::error!("session {} op handler panicked (lane {key})", session.id);
            }
        }
        drop(op._permit);
        let mut lanes = session.scheduler.lanes.lock().unwrap();
        match lanes.get_mut(&key).and_then(VecDeque::pop_front) {
            Some(op) => next = Some(op),
            None => {
                lanes.remove(&key);
            }
        }
    }
}

/// Legacy `group-key` (session.clj:1463-1512) for the ops a client sends.
fn group_key(sched: &Scheduler, msg: &Value) -> String {
    let op = msg.get("op").and_then(Value::as_str).unwrap_or("");
    let field = |k: &str| msg.get(k).cloned().unwrap_or(Value::Null);
    let key = match op {
        "transact" => json!(["transact"]),
        "join-room" | "leave-room" | "set-presence" | "client-broadcast" => {
            json!(["room", field("room-id")])
        }
        "add-query" | "remove-query" => json!(["query", field("q")]),
        "append-stream" => json!(["append-stream", field("stream-id")]),
        "init" => json!(["init"]),
        "start-sync" => json!(["sync", field("q")]),
        "remove-sync" | "resync-table" => json!(["modify-sync", field("subscription-id")]),
        "start-stream" => json!(["start-stream", field("client-id")]),
        "subscribe-stream" => {
            let id = msg
                .get("stream-id")
                .filter(|v| !v.is_null())
                .cloned()
                .unwrap_or_else(|| field("client-id"));
            json!(["subscribe-stream", id])
        }
        "unsubscribe-stream" => json!(["unsubscribe-stream", field("subscribe-event-id")]),
        _ => json!(["solo", sched.solo.fetch_add(1, Ordering::Relaxed)]),
    };
    key.to_string()
}

/// Legacy `combine` (session.clj:1514-1553) for two client ops of one lane,
/// the second arriving while the first still waits: `Some(merged)` replaces
/// the first.
fn combine(first: &Value, second: &Value, attrs: Option<&AttrMap>) -> Option<Value> {
    let op = |m: &Value| m.get("op").and_then(Value::as_str).map(str::to_string);
    match (op(first)?.as_str(), op(second)?.as_str()) {
        // combine-transact (:1452-1461, `combine-transacts?` defaults on)
        ("transact", "transact") if matching_steps(first, second, attrs?) => {
            let mut earlier = first.clone();
            let mut redundant = match earlier.as_object_mut()?.remove(REDUNDANT) {
                Some(Value::Array(v)) => v,
                _ => vec![],
            };
            redundant.push(earlier);
            let mut merged = second.clone();
            merged[REDUNDANT] = Value::Array(redundant);
            Some(merged)
        }
        ("set-presence", "set-presence") => Some(second.clone()),
        ("append-stream", "append-stream") => {
            let mut merged = second.clone();
            let mut chunks = first
                .get("chunks")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            chunks.extend(
                second
                    .get("chunks")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default(),
            );
            merged["chunks"] = Value::Array(chunks);
            merged["offset"] = first.get("offset").cloned().unwrap_or(Value::Null);
            Some(merged)
        }
        _ => None,
    }
}

/// Legacy `matching-steps?` (:1404-1450): the same number of steps, each
/// pair an `add-triple` with a mode (5 elements) on the same entity, the
/// same cardinality-one attr and the same mode, so the later transact
/// overwrites everything the earlier one writes.
fn matching_steps(first: &Value, second: &Value, attrs: &AttrMap) -> bool {
    let (Some(a), Some(b)) = (
        first.get("tx-steps").and_then(Value::as_array),
        second.get("tx-steps").and_then(Value::as_array),
    ) else {
        return false;
    };
    a.len() == b.len()
        && a.iter().zip(b).all(|(s1, s2)| {
            let (Some(s1), Some(s2)) = (s1.as_array(), s2.as_array()) else {
                return false;
            };
            s1.len() == 5
                && s2.len() == 5
                && s1[0] == "add-triple"
                && s2[0] == "add-triple"
                && s1[1] == s2[1]
                && s1[2] == s2[2]
                && s1[4] == s2[4]
                && s1[2]
                    .as_str()
                    .and_then(|id| Uuid::parse_str(id).ok())
                    .and_then(|id| attrs.get(&id))
                    .is_some_and(|a| a.cardinality == Cardinality::One)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_follow_legacy_groups() {
        let s = Scheduler::default();
        let k = |m: Value| group_key(&s, &m);
        assert_eq!(
            k(json!({"op": "transact"})),
            k(json!({"op": "transact", "tx-steps": []}))
        );
        assert_eq!(
            k(json!({"op": "join-room", "room-id": "r"})),
            k(json!({"op": "set-presence", "room-id": "r"}))
        );
        assert_ne!(
            k(json!({"op": "set-presence", "room-id": "r"})),
            k(json!({"op": "set-presence", "room-id": "s"}))
        );
        assert_eq!(
            k(json!({"op": "add-query", "q": {"a": {}}})),
            k(json!({"op": "remove-query", "q": {"a": {}}}))
        );
        assert_ne!(k(json!({"op": "bogus"})), k(json!({"op": "bogus"})));
    }

    #[test]
    fn combines_presence_and_stream_appends() {
        let p1 = json!({"op": "set-presence", "data": 1});
        let p2 = json!({"op": "set-presence", "data": 2});
        assert_eq!(combine(&p1, &p2, None), Some(p2.clone()));
        let a1 = json!({"op": "append-stream", "chunks": ["a"], "offset": 0, "done": false});
        let a2 = json!({"op": "append-stream", "chunks": ["b"], "offset": 1, "done": true});
        assert_eq!(
            combine(&a1, &a2, None),
            Some(json!({"op": "append-stream", "chunks": ["a", "b"], "offset": 0, "done": true}))
        );
        // no attrs, no transact combining
        assert_eq!(
            combine(&json!({"op": "transact"}), &json!({"op": "transact"}), None),
            None
        );
    }

    fn attr(id: Uuid, cardinality: Cardinality) -> instant_core::attr::Attr {
        instant_core::attr::Attr {
            id,
            value_type: instant_core::attr::ValueType::Blob,
            cardinality,
            forward_ident: Uuid::new_v4(),
            etype: "e".into(),
            label: id.to_string(),
            reverse_ident: None,
            reverse_etype: None,
            reverse_label: None,
            is_unique: false,
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

    #[test]
    fn combines_overwriting_transacts() {
        let (one, many) = (Uuid::new_v4(), Uuid::new_v4());
        let mut attrs = AttrMap::default();
        attrs.insert(attr(one, Cardinality::One));
        attrs.insert(attr(many, Cardinality::Many));
        let tx = |ceid: &str, attr: Uuid, v: i64| {
            json!({"op": "transact", "client-event-id": ceid,
                   "tx-steps": [["add-triple", "e1", attr.to_string(), v, {"mode": "update"}]]})
        };
        let merged = combine(&tx("a", one, 1), &tx("b", one, 2), Some(&attrs)).unwrap();
        assert_eq!(merged["client-event-id"], "b");
        assert_eq!(merged["tx-steps"][0][3], 2);
        assert_eq!(merged[REDUNDANT][0]["client-event-id"], "a");
        // a third one carries both earlier events, oldest first
        let merged = combine(&merged, &tx("c", one, 3), Some(&attrs)).unwrap();
        let ceids: Vec<_> = merged[REDUNDANT]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["client-event-id"].clone())
            .collect();
        assert_eq!(ceids, vec![json!("a"), json!("b")]);
        assert!(merged[REDUNDANT][1].get(REDUNDANT).is_none());
        // many-cardinality attrs, other attrs, and steps without a mode stay apart
        assert_eq!(
            combine(&tx("a", many, 1), &tx("b", many, 2), Some(&attrs)),
            None
        );
        assert_eq!(
            combine(&tx("a", one, 1), &tx("b", many, 2), Some(&attrs)),
            None
        );
        let no_mode =
            json!({"op": "transact", "tx-steps": [["add-triple", "e1", one.to_string(), 1]]});
        assert_eq!(combine(&no_mode, &no_mode, Some(&attrs)), None);
    }
}
