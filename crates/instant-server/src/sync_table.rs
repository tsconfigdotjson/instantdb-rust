//! Sync tables: full-table replication of one namespace to the client with
//! per-tx incremental updates (PROTOCOL.md §2.9/§3.13). Change capture comes
//! from the rust_tx_changes trigger log; subscriptions persist in the legacy
//! sync_subs table so resync works across sessions and nodes.

use std::sync::Arc;

use instant_core::attr::{AttrMap, Cardinality};
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

/// Parse + validate a sync query: single namespace, no links, only $.order.
fn parse_sync_query(attrs: &AttrMap, q: &Value) -> Result<String> {
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
    let form = obj.get(&etype).and_then(|f| f.as_object());
    if let Some(form) = form {
        for (k, v) in form {
            if k == "$" {
                if let Some(opts) = v.as_object() {
                    for opt in opts.keys() {
                        if opt != "order" {
                            return Err(verr("Only order is currently supported."));
                        }
                    }
                }
            } else {
                return Err(verr("Links are not yet supported."));
            }
        }
    }
    Ok(etype)
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
        let app_id = st.app_id.ok_or_else(|| crate::ws::not_initialized(session.id))?;
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
    let etype = parse_sync_query(&attrs, &q)?;

    let sub_id = Uuid::new_v4();
    let token = Uuid::new_v4();
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
    .execute(&state.pool)
    .await
    .map_err(InstantError::from)?;

    session.send(json!({
        "op": "start-sync-ok",
        "client-event-id": msg.get("client-event-id"),
        "subscription-id": sub_id,
        "q": q,
        "token": token,
    }));

    let tx_id = initial_load(state, session, app_id, &attrs, &etype, sub_id).await?;
    let attr_ids = ea_attr_ids(&attrs, &etype);
    {
        let mut st = session.state.lock().await;
        st.sync_subs.insert(
            sub_id,
            SyncSub {
                etype,
                last_tx: tx_id,
                attr_ids,
            },
        );
    }
    session.send(json!({
        "op": "sync-init-finish",
        "subscription-id": sub_id,
        "tx-id": tx_id,
    }));
    Ok(())
}

/// Streams the current table contents as sync-load-batch messages; returns the
/// snapshot tx-id.
async fn initial_load(
    state: &AppState,
    session: &Arc<Session>,
    app_id: Uuid,
    attrs: &AttrMap,
    etype: &str,
    sub_id: Uuid,
) -> Result<i64> {
    let id_attr = attrs.id_attr_of(etype).unwrap().id;
    let ea_ids = ea_attr_ids(attrs, etype);

    let mut dbtx = state.pool.begin().await.map_err(InstantError::from)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *dbtx)
        .await?;
    let row = sqlx::query("SELECT coalesce(max(id), 0) AS id FROM transactions WHERE app_id = $1")
        .bind(app_id)
        .fetch_one(&mut *dbtx)
        .await?;
    let tx_id: i64 = row.get("id");

    // entities in serverCreatedAt order, each with its ea triples
    let rows = sqlx::query(
        r#"
        SELECT t.entity_id AS eid,
               (SELECT json_agg(json_build_array(t2.entity_id, t2.attr_id, t2.value, t2.created_at))
                FROM triples t2
                WHERE t2.app_id = $1 AND t2.entity_id = t.entity_id
                  AND t2.ea AND t2.attr_id = ANY($3)) AS join_rows
        FROM triples t
        WHERE t.app_id = $1 AND t.attr_id = $2 AND t.ea
        ORDER BY t.created_at ASC, t.entity_id ASC
        "#,
    )
    .bind(app_id)
    .bind(id_attr)
    .bind(&ea_ids)
    .fetch_all(&mut *dbtx)
    .await?;
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
        let app_id = st.app_id.ok_or_else(|| crate::ws::not_initialized(session.id))?;
        (app_id, st.admin, st.user.as_ref().map(|u| u.id))
    };
    let sub_id = msg
        .get("subscription-id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| InstantError::param_missing("missing subscription-id"))?;
    let from_tx = msg
        .get("tx-id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| InstantError::param_missing("missing tx-id"))?;
    let token = msg
        .get("token")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| InstantError::param_missing("missing token"))?;

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
    let etype = parse_sync_query(&attrs, &q)?;

    // if the change log no longer reaches back to from_tx, force a restart
    let has_older = sqlx::query(
        "SELECT 1 AS x FROM transactions
         WHERE app_id = $1 AND id > $2
           AND id NOT IN (SELECT DISTINCT tx_id FROM rust_tx_changes WHERE app_id = $1)
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

    {
        let mut st = session.state.lock().await;
        st.sync_subs.insert(
            sub_id,
            SyncSub {
                etype: etype.clone(),
                last_tx: from_tx,
                attr_ids: ea_attr_ids(&attrs, &etype),
            },
        );
    }
    // replay the backlog
    let latest = service::max_tx_id(state, app_id).await?;
    push_updates(state, session, app_id, latest).await;
    Ok(())
}

pub async fn handle_remove_sync(
    state: &Arc<AppState>,
    session: &Arc<Session>,
    msg: &Value,
) -> std::result::Result<(), InstantError> {
    let sub_id = msg
        .get("subscription-id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| InstantError::param_missing("missing subscription-id"))?;
    let keep = msg
        .get("keep-subscription")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let (app_id, owned) = {
        let mut st = session.state.lock().await;
        let app_id = st.app_id.ok_or_else(|| crate::ws::not_initialized(session.id))?;
        (app_id, st.sync_subs.remove(&sub_id).is_some())
    };
    // legacy deletes only `{:id ... :app-id app-id}` and only for a sub this
    // session holds; a foreign id is a silent no-op
    if !keep && owned {
        let _ = sqlx::query("DELETE FROM sync_subs WHERE id = $1 AND app_id = $2")
            .bind(sub_id)
            .bind(app_id)
            .execute(&state.pool)
            .await;
    }
    // legacy sends no reply to remove-sync (session.clj handle-remove-sync!)
    Ok(())
}

/// Push sync-update-triples for all of a session's subs up to `latest`.
/// Called from the invalidator on each tx notification.
pub async fn push_updates(state: &AppState, session: &Arc<Session>, app_id: Uuid, latest: i64) {
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
        if !txes.is_empty() {
            session.send(json!({
                "op": "sync-update-triples",
                "subscription-id": sub_id,
                "txes": txes,
            }));
        }
        let mut st = session.state.lock().await;
        if let Some(s) = st.sync_subs.get_mut(&sub_id) {
            s.last_tx = latest;
        }
    }
}
