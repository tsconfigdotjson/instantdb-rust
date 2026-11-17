//! Sync tables: full-table replication of one namespace to the client with
//! per-tx incremental updates (client `SyncTable.ts`). Change capture comes
//! from the rust_tx_changes trigger log; subscriptions persist in the legacy
//! sync_subs table so resync works across sessions and nodes.

use std::sync::Arc;

use instant_core::attr::{Attr, AttrMap, Cardinality, CheckedDataType};
use instant_core::error::{InstantError, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::Row;
use uuid::Uuid;

use crate::service;
use crate::state::{AppState, Session, SyncSub};

fn verr(msg: &str) -> InstantError {
    InstantError::validation_failed("query", msg, json!([{"message": msg}]))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyncDir {
    Asc,
    Desc,
}

impl SyncDir {
    fn sql(self) -> &'static str {
        match self {
            SyncDir::Asc => "ASC",
            SyncDir::Desc => "DESC",
        }
    }

    fn desc(self) -> bool {
        self == SyncDir::Desc
    }
}

#[derive(Debug, Clone)]
enum SyncOrder {
    ServerCreatedAt {
        dir: SyncDir,
    },
    Attr {
        attr: Box<Attr>,
        dir: SyncDir,
        checked: CheckedDataType,
    },
}

#[derive(Debug, Clone)]
struct SyncQuery {
    etype: String,
    order: SyncOrder,
}

fn order_verr(msg: String) -> InstantError {
    verr(&msg)
}

/// Parse + validate a sync query: single namespace, no links, only $.order.
/// The order field follows legacy `create-sync-process` (db/sync_table.clj):
/// an indexed, not-currently-indexing attr with a checked data type, ordered
/// by the same typed extraction function as a regular InstaQL order.
fn parse_sync_query(attrs: &AttrMap, q: &Value) -> Result<SyncQuery> {
    let obj = q
        .as_object()
        .ok_or_else(|| verr("Query must be an object."))?;
    let mut keys = obj.keys().filter(|k| *k != "$$ruleParams");
    let etype = keys.next().ok_or_else(|| verr("Query is empty."))?.clone();
    if keys.next().is_some() {
        return Err(verr("Query can only fetch a single namespace"));
    }
    if attrs.id_attr_of(&etype).is_none() {
        return Err(verr("No matching table."));
    }

    let mut order_value: Option<&Value> = None;
    if let Some(form) = obj.get(&etype).and_then(|f| f.as_object()) {
        for (k, v) in form {
            if k == "$" {
                if let Some(opts) = v.as_object() {
                    for (opt, value) in opts {
                        if opt != "order" {
                            return Err(verr("Only order is currently supported."));
                        }
                        order_value = Some(value);
                    }
                }
            } else {
                return Err(verr("Links are not yet supported."));
            }
        }
    }

    let (order_key, dir) = match order_value {
        None => ("serverCreatedAt".to_string(), SyncDir::Asc),
        Some(v) => {
            let m = v
                .as_object()
                .ok_or_else(|| order_verr("`order` must be an object.".into()))?;
            if m.is_empty() {
                ("serverCreatedAt".to_string(), SyncDir::Asc)
            } else if m.len() != 1 {
                let keys = m.keys().cloned().collect::<Vec<_>>().join(",");
                return Err(order_verr(format!(
                    "We only support a single key in the `order` clause. Got {keys}."
                )));
            } else {
                let (key, direction) = m.iter().next().unwrap();
                let dir = match direction.as_str() {
                    Some("asc") => SyncDir::Asc,
                    Some("desc") => SyncDir::Desc,
                    _ => {
                        return Err(order_verr(format!(
                            "We only support \"asc\" or \"desc\" in the `order` clause. Got {}.",
                            direction
                        )))
                    }
                };
                (key.clone(), dir)
            }
        }
    };

    let order = if order_key == "serverCreatedAt" {
        SyncOrder::ServerCreatedAt { dir }
    } else {
        let attr = attrs.by_fwd_name(&etype, &order_key).ok_or_else(|| {
            // legacy validates the query through instaql-query->patterns
            // before create-sync-process looks at the order attr
            order_verr(format!("There is no `{order_key}` attribute for {etype}."))
        })?;
        let checked = if attr.is_indexed && !attr.indexing {
            attr.checked_data_type
        } else {
            None
        }
        .ok_or_else(|| {
            order_verr("Order field must be indexed with a checked data type.".into())
        })?;
        SyncOrder::Attr {
            attr: Box::new(attr.clone()),
            dir,
            checked,
        }
    };

    Ok(SyncQuery { etype, order })
}

/// Legacy `sync_sub_topics` persistence: `[#{:ea} _ attr-ids _]`.
fn topic_attrs(attr_ids: &[Uuid]) -> Vec<Uuid> {
    attr_ids.to_vec()
}

fn ea_attr_ids(attrs: &AttrMap, etype: &str) -> Vec<Uuid> {
    attrs
        .attrs_of_etype(etype)
        .filter(|a| a.cardinality == Cardinality::One)
        .map(|a| a.id)
        .collect()
}

fn hash_token(token: Uuid) -> Vec<u8> {
    let mut h = Sha256::new();
    h.update(token.as_bytes());
    h.finalize().to_vec()
}

pub async fn handle_start_sync(
    state: &Arc<AppState>,
    session: &Arc<Session>,
    msg: &Value,
) -> std::result::Result<(), InstantError> {
    let (app_id, admin, user_id) = {
        let st = session.state.lock().await;
        let app_id = st
            .app_id
            .ok_or_else(|| crate::ws::not_initialized(session.id))?;
        (app_id, st.admin, st.user.as_ref().map(|u| u.id))
    };
    let q = msg
        .get("q")
        .cloned()
        .filter(|q| !q.is_null())
        .ok_or_else(|| {
            InstantError::validation_failed_input(
                "start-sync",
                json!({"q": null}),
                json!([{"message": "Query can not be null."}]),
            )
        })?;
    if !admin {
        // legacy gates sync tables to admin sessions (session.clj:281-284)
        return Err(InstantError::validation_failed_input(
            "start-sync",
            json!({"q": q}),
            json!([{"message": "start-sync is currently supported for admins only."}]),
        ));
    }
    let attrs = service::load_attrs(state, app_id).await?;
    let syncq = parse_sync_query(&attrs, &q)?;

    let sub_id = Uuid::new_v4();
    let token = Uuid::new_v4();

    session.send(json!({
        "op": "start-sync-ok",
        "client-event-id": msg.get("client-event-id"),
        "subscription-id": sub_id,
        "q": q,
        "token": token,
    }));

    let tx_id = initial_load(state, session, app_id, &attrs, &syncq, sub_id).await?;
    let attr_ids = ea_attr_ids(&attrs, &syncq.etype);
    insert_sync_sub(state, app_id, &q, sub_id, token, admin, user_id, &attr_ids).await?;
    {
        let mut st = session.state.lock().await;
        st.sync_subs.insert(
            sub_id,
            SyncSub {
                etype: syncq.etype.clone(),
                last_tx: tx_id,
                attr_ids: attr_ids.clone(),
            },
        );
    }
    session.send(json!({
        "op": "sync-init-finish",
        "subscription-id": sub_id,
        "tx-id": tx_id,
    }));

    // legacy session.clj on-init-finish requeues refresh-sync-table after the
    // initial snapshot so transactions handled while the load ran are caught
    // up. Without this a tx that commits after the snapshot but before the
    // session's sub is registered is never sent.
    let latest = service::max_tx_id(state, app_id).await?;
    push_updates(state, session, app_id, latest).await;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_sync_sub(
    state: &AppState,
    app_id: Uuid,
    q: &Value,
    sub_id: Uuid,
    token: Uuid,
    admin: bool,
    user_id: Option<Uuid>,
    attr_ids: &[Uuid],
) -> Result<()> {
    let mut tx = state.pool.begin().await.map_err(InstantError::from)?;
    sqlx::query(
        "INSERT INTO sync_subs (id, app_id, query, token_hash, is_admin, user_id)
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(sub_id)
    .bind(app_id)
    .bind(q.to_string())
    .bind(hash_token(token))
    .bind(admin)
    .bind(user_id)
    .execute(&mut *tx)
    .await
    .map_err(InstantError::from)?;
    // legacy sync-sub-model/create! stores the coarse [ea _ attrs _] topic;
    // resync reads those attrs back so an attr added later stays out of the
    // subscription until a new start-sync, just like legacy.
    sqlx::query(
        "INSERT INTO sync_sub_topics (sync_sub_id, topic_num, idx, e, a, v)
         VALUES ($1, 0, ARRAY['ea']::topics_idx[],
                 ARRAY[decode('00', 'hex')],
                 ARRAY(SELECT uuid_send(u) FROM unnest($2::uuid[]) AS t(u)),
                 ARRAY['{}'::jsonb])",
    )
    .bind(sub_id)
    .bind(topic_attrs(attr_ids))
    .execute(&mut *tx)
    .await
    .map_err(InstantError::from)?;
    tx.commit().await.map_err(InstantError::from)?;
    Ok(())
}

async fn load_sync_attr_ids(state: &AppState, sub_id: Uuid) -> Result<Vec<Uuid>> {
    let rows = sqlx::query(
        "SELECT array(SELECT encode(u, 'hex') FROM unnest(a) AS t(u)) AS attrs
         FROM sync_sub_topics WHERE sync_sub_id = $1 ORDER BY topic_num",
    )
    .bind(sub_id)
    .fetch_all(&state.pool)
    .await
    .map_err(InstantError::from)?;
    let mut ids = vec![];
    for row in rows {
        let attrs: Vec<String> = row.get("attrs");
        for hex in attrs {
            if let Ok(id) = Uuid::parse_str(&hex) {
                if !ids.contains(&id) {
                    ids.push(id);
                }
            }
        }
    }
    Ok(ids)
}

/// Streams the current table contents as sync-load-batch messages; returns the
/// snapshot tx-id.
async fn initial_load(
    state: &AppState,
    session: &Arc<Session>,
    app_id: Uuid,
    attrs: &AttrMap,
    syncq: &SyncQuery,
    sub_id: Uuid,
) -> Result<i64> {
    let id_attr = attrs.id_attr_of(&syncq.etype).unwrap().id;
    let ea_ids = ea_attr_ids(attrs, &syncq.etype);

    let mut dbtx = state.pool.begin().await.map_err(InstantError::from)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *dbtx)
        .await?;
    let row = sqlx::query("SELECT coalesce(max(id), 0) AS id FROM transactions WHERE app_id = $1")
        .bind(app_id)
        .fetch_one(&mut *dbtx)
        .await?;
    let tx_id: i64 = row.get("id");

    // Legacy create-sync-process orders the initial rows by the query's order
    // (db/sync_table.clj:65-111): serverCreatedAt uses the id triple's
    // created_at, a custom field uses the same typed extraction function as
    // InstaQL, with nulls first for asc / last for desc (plain jsonb order,
    // no NULLS clause, for strings). SyncTable.ts pushes these batches as-is.
    let dir = match &syncq.order {
        SyncOrder::ServerCreatedAt { dir } | SyncOrder::Attr { dir, .. } => *dir,
    };
    let dir_sql = dir.sql();
    let (where_sql, order_sql, ea_param) = match &syncq.order {
        SyncOrder::ServerCreatedAt { .. } => (
            "t.attr_id = $2".to_string(),
            format!("t.created_at {dir_sql}, t.entity_id {dir_sql}"),
            3,
        ),
        SyncOrder::Attr {
            attr: _, checked, ..
        } => {
            let (expr, nulls) = match checked {
                CheckedDataType::Date => (
                    "triples_extract_date_value(t.value)",
                    Some(if dir.desc() {
                        " NULLS LAST"
                    } else {
                        " NULLS FIRST"
                    }),
                ),
                CheckedDataType::Number => (
                    "triples_extract_number_value(t.value)",
                    Some(if dir.desc() {
                        " NULLS LAST"
                    } else {
                        " NULLS FIRST"
                    }),
                ),
                CheckedDataType::Boolean => (
                    "triples_extract_boolean_value(t.value)",
                    Some(if dir.desc() {
                        " NULLS LAST"
                    } else {
                        " NULLS FIRST"
                    }),
                ),
                // legacy's extract-value-fn is nil for strings: raw jsonb order.
                CheckedDataType::String => ("t.value", None),
            };
            (
                "t.attr_id = $2 AND t.ave AND t.checked_data_type = $3::checked_data_type"
                    .to_string(),
                format!(
                    "{expr} {dir_sql}{}, t.entity_id {dir_sql}",
                    nulls.unwrap_or("")
                ),
                4,
            )
        }
    };

    let sql = format!(
        r#"
        SELECT t.entity_id AS eid,
               (SELECT json_agg(json_build_array(t2.entity_id, t2.attr_id, t2.value, t2.created_at))
                FROM triples t2
                WHERE t2.app_id = $1 AND t2.entity_id = t.entity_id
                  AND t2.ea AND t2.attr_id = ANY(${ea_param})) AS join_rows
        FROM triples t
        WHERE t.app_id = $1 AND {where_sql}
        ORDER BY {order_sql}
        "#
    );
    let mut query = sqlx::query(&sql).bind(app_id);
    match &syncq.order {
        SyncOrder::ServerCreatedAt { .. } => {
            query = query.bind(id_attr);
        }
        SyncOrder::Attr { attr, checked, .. } => {
            query = query.bind(attr.id).bind(checked.as_str());
        }
    }
    let rows = query.bind(&ea_ids).fetch_all(&mut *dbtx).await?;
    dbtx.rollback().await.ok();

    for chunk in rows.chunks(100) {
        let join_rows: Vec<Value> = chunk
            .iter()
            .filter_map(|r| r.get::<Option<Value>, _>("join_rows"))
            .collect();
        if join_rows.is_empty() {
            continue;
        }
        session.send(json!({
            "op": "sync-load-batch",
            "subscription-id": sub_id,
            "join-rows": join_rows,
        }));
    }
    Ok(tx_id)
}

pub async fn handle_resync_table(
    state: &Arc<AppState>,
    session: &Arc<Session>,
    msg: &Value,
) -> std::result::Result<(), InstantError> {
    let (app_id, admin, user_id) = {
        let st = session.state.lock().await;
        let app_id = st
            .app_id
            .ok_or_else(|| crate::ws::not_initialized(session.id))?;
        (app_id, st.admin, st.user.as_ref().map(|u| u.id))
    };
    // legacy handle-resync-table! reads params in this order: tx-id,
    // subscription-id, token (session.clj:407-415).
    let from_tx = tx_id_param(msg)?;
    let sub_id = subscription_id_param(msg)?;
    let token = msg
        .get("token")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s.trim()).ok())
        .ok_or_else(|| crate::ws::param_missing_at(&["token"]))?;

    // legacy get-by-id-with-topics! (model/sync_sub.clj:170-195): the token
    // hash, the admin-ness and the user of the session must all match the
    // subscription's
    let row = sqlx::query(
        "SELECT query, token_hash, is_admin, user_id FROM sync_subs WHERE id = $1 AND app_id = $2",
    )
    .bind(sub_id)
    .bind(app_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?
    .ok_or_else(|| {
        InstantError::new(
            "record-not-found",
            400,
            "Record not found: subscription",
            Some(json!({"subscription-id": sub_id, "record-type": "subscription"})),
        )
    })?;
    let sub_err = |input: Value, message: &str| {
        InstantError::new(
            "validation-failed",
            400,
            format!("Validation failed for subscription: {message}"),
            Some(json!({
                "data-type": "subscription",
                "input": input,
                "errors": [{"message": message}],
            })),
        )
    };
    let stored_hash: Option<Vec<u8>> = row.get("token_hash");
    if stored_hash.as_deref() != Some(hash_token(token).as_slice()) {
        return Err(sub_err(json!({"token": token}), "Invalid token."));
    }
    let sub_admin: bool = row.try_get("is_admin").unwrap_or(false);
    if sub_admin != admin {
        return Err(sub_err(
            json!({"admin?": admin}),
            if admin {
                "Subscription was not created by an admin, but the session is an admin session."
            } else {
                "Subscription was created as an admin, but the session is not an admin session."
            },
        ));
    }
    let sub_user: Option<Uuid> = row.try_get("user_id").unwrap_or(None);
    if sub_user != user_id {
        return Err(sub_err(
            json!({"user-id": user_id}),
            "Subscription was created by a different user.",
        ));
    }
    let q: Value = serde_json::from_str(&row.get::<String, _>("query"))
        .map_err(|_| InstantError::internal("corrupt sync sub"))?;
    let attrs = service::load_attrs(state, app_id).await?;
    let syncq = parse_sync_query(&attrs, &q)?;

    // if the change log no longer reaches back to from_tx, force a restart.
    // Only transactions old enough for the hourly `rust_tx_changes` cleanup
    // can have been pruned: a recent transaction with no log rows simply had
    // no triple changes (e.g. an add-attr), which legacy replays as nothing.
    let has_older = sqlx::query(
        "SELECT 1 AS x FROM transactions t
         WHERE t.app_id = $1 AND t.id > $2
           AND t.created_at < now() - interval '1 hour'
           AND t.id NOT IN (SELECT DISTINCT tx_id FROM rust_tx_changes WHERE app_id = $1)
         LIMIT 1",
    )
    .bind(app_id)
    .bind(from_tx)
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    if has_older.is_some() {
        // some txes after from_tx are missing from the log (pruned or from a
        // writer without capture) -- client must resync from scratch
        return Err(InstantError::record_not_found(
            "sync-sub",
            "Subscription is too far behind.",
        ));
    }

    let stored_attrs = load_sync_attr_ids(state, sub_id).await?;
    let attr_ids = if stored_attrs.is_empty() {
        // a row created before topics were persisted: fall back to the
        // current catalog rather than dropping its updates entirely
        ea_attr_ids(&attrs, &syncq.etype)
    } else {
        stored_attrs
    };
    {
        let mut st = session.state.lock().await;
        st.sync_subs.insert(
            sub_id,
            SyncSub {
                etype: syncq.etype.clone(),
                last_tx: from_tx,
                attr_ids,
            },
        );
    }
    // replay the backlog
    let latest = service::max_tx_id(state, app_id).await?;
    push_updates(state, session, app_id, latest).await;
    Ok(())
}

/// `ex/get-param! event [:tx-id] (fn [v] (try (long v) ...))`
fn tx_id_param(msg: &Value) -> std::result::Result<i64, InstantError> {
    let raw = msg
        .get("tx-id")
        .ok_or_else(|| crate::ws::param_missing_at(&["tx-id"]))?;
    if raw.is_null() || raw == &Value::Bool(false) {
        return Err(crate::ws::param_missing_at(&["tx-id"]));
    }
    raw.as_i64()
        .or_else(|| raw.as_str().and_then(|s| s.trim().parse::<i64>().ok()))
        .ok_or_else(|| crate::ws::param_malformed_at(&["tx-id"], raw))
}

/// `ex/get-param! event [:subscription-id] uuid-util/coerce`
fn subscription_id_param(msg: &Value) -> std::result::Result<Uuid, InstantError> {
    let raw = msg
        .get("subscription-id")
        .filter(|v| !v.is_null())
        .ok_or_else(|| crate::ws::param_missing_at(&["subscription-id"]))?;
    raw.as_str()
        .and_then(|s| Uuid::parse_str(s.trim()).ok())
        .ok_or_else(|| crate::ws::param_malformed_at(&["subscription-id"], raw))
}

pub async fn handle_remove_sync(
    state: &Arc<AppState>,
    session: &Arc<Session>,
    msg: &Value,
) -> std::result::Result<(), InstantError> {
    let sub_id = subscription_id_param(msg)?;
    let keep = msg
        .get("keep-subscription")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    {
        let mut st = session.state.lock().await;
        st.app_id
            .ok_or_else(|| crate::ws::not_initialized(session.id))?;
        st.sync_subs.remove(&sub_id);
    }
    // legacy (session.clj:373-381) drops the in-memory query and, without
    // `keep-subscription`, calls `sync-sub-model/delete!` with
    // `(:sync/subscription-id sync-ent)` -- an attribute nothing ever sets, so
    // the row survives and a later `resync-table` on the same id still
    // resumes it. Matched on the wire: the row is kept either way.
    let _ = (keep, state);
    // legacy sends no reply to remove-sync (session.clj handle-remove-sync!)
    Ok(())
}

/// Push sync-update-triples for all of a session's subs up to `latest`.
/// Called from the invalidator on each tx notification.
pub async fn push_updates(state: &AppState, session: &Arc<Session>, app_id: Uuid, latest: i64) {
    let _serial = session.sync_push.lock().await;
    let subs: Vec<(Uuid, SyncSub)> = {
        let st = session.state.lock().await;
        if st.app_id != Some(app_id) {
            return;
        }
        st.sync_subs.iter().map(|(k, v)| (*k, v.clone())).collect()
    };
    if subs.is_empty() {
        return;
    }
    for (sub_id, sub) in subs {
        if sub.last_tx >= latest {
            continue;
        }
        let ea_ids = sub.attr_ids.clone();
        let rows = sqlx::query(
            "SELECT tx_id, entity_id, attr_id, value, created_at, action
             FROM rust_tx_changes
             WHERE app_id = $1 AND tx_id > $2 AND tx_id <= $3 AND attr_id = ANY($4)
             ORDER BY tx_id ASC, logged_at ASC",
        )
        .bind(app_id)
        .bind(sub.last_tx)
        .bind(latest)
        .bind(&ea_ids)
        .fetch_all(&state.pool)
        .await
        .unwrap_or_default();

        let mut txes: Vec<Value> = vec![];
        let mut cur_tx: Option<i64> = None;
        let mut changes: Vec<Value> = vec![];
        for row in &rows {
            let tx_id: i64 = row.get("tx_id");
            if cur_tx != Some(tx_id) {
                if let Some(prev) = cur_tx {
                    txes.push(json!({"tx-id": prev, "changes": std::mem::take(&mut changes)}));
                }
                cur_tx = Some(tx_id);
            }
            let action: String = row.get("action");
            changes.push(json!({
                "action": action,
                "triple": [
                    row.get::<Uuid, _>("entity_id"),
                    row.get::<Uuid, _>("attr_id"),
                    row.get::<Value, _>("value"),
                    row.get::<Option<i64>, _>("created_at"),
                ],
            }));
        }
        if let Some(prev) = cur_tx {
            txes.push(json!({"tx-id": prev, "changes": changes}));
        }
        for chunk in chunk_txes(txes) {
            let Some(last_tx) = chunk
                .last()
                .and_then(|t| t.get("tx-id"))
                .and_then(|v| v.as_i64())
            else {
                continue;
            };
            session.send(json!({
                "op": "sync-update-triples",
                "subscription-id": sub_id,
                "txes": chunk,
            }));
            let mut st = session.state.lock().await;
            if let Some(s) = st.sync_subs.get_mut(&sub_id) {
                s.last_tx = last_tx;
            }
        }
    }
}

/// Legacy `handle-refresh-sync-table!` sends at most 100 txes per websocket
/// frame and requeues the rest (session.clj:383-405).
fn chunk_txes(txes: Vec<Value>) -> Vec<Vec<Value>> {
    txes.chunks(100).map(|c| c.to_vec()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_updates_are_sent_in_legacy_100_tx_chunks() {
        let txes = (0..205)
            .map(|i| json!({"tx-id": i, "changes": []}))
            .collect();
        let chunks = chunk_txes(txes);
        assert_eq!(
            chunks.iter().map(|c| c.len()).collect::<Vec<_>>(),
            vec![100, 100, 5]
        );
        assert_eq!(chunks[0][0]["tx-id"], 0);
        assert_eq!(chunks[1][0]["tx-id"], 100);
        assert_eq!(chunks[2][4]["tx-id"], 204);
    }
}
