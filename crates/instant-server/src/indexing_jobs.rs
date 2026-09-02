//! Indexing jobs: the background schema changes behind `instant-cli push
//! schema` (index / unique / required / check-data-type and their removals).
//! Port of LEGACY db/indexing_jobs.clj on the legacy `indexing_jobs` table,
//! so the CLI's job polling (`GET /dash/apps/:app_id/indexing-jobs/group/:id`)
//! sees the same rows and error codes.
//!
//! Each job rewrites its attr in ONE database transaction (flags, triple
//! index flags, null backfill) and is run by the node that accepted the
//! request. Batched, resumable rewrites for very large attrs are issue #5;
//! job rows, statuses and error reporting here already match what the CLI
//! and the dashboard read.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use instant_core::attr::{Attr, AttrMap, ValueType};
use instant_core::error::{InstantError, Result};
use serde_json::{json, Map, Value};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::service;
use crate::state::AppState;

pub const INVALID_TRIPLE_ERROR: &str = "invalid-triple-error";
pub const TRIPLE_TOO_LARGE_ERROR: &str = "triple-too-large-error";
pub const TRIPLE_NOT_UNIQUE_ERROR: &str = "triple-not-unique-error";
pub const MISSING_REQUIRED_ERROR: &str = "missing-required-error";
pub const INVALID_ATTR_STATE_ERROR: &str = "invalid-attr-state-error";
pub const UNEXPECTED_ERROR: &str = "unexpected-error";

/// (serial key, first stage, final stage) per job type (indexing_jobs.clj
/// `jobs` + the `*--stages` vectors).
pub fn job_spec(job_type: &str) -> Option<(&'static str, &'static str, &'static str)> {
    Some(match job_type {
        "check-data-type" => ("data-type", "estimate-work", "update-attr-done"),
        "remove-data-type" => ("data-type", "update-attr-start", "update-attr-done"),
        "index" => ("index", "update-attr-start", "update-attr-done"),
        "remove-index" => ("index", "update-attr-start", "update-attr-done"),
        "unique" => ("unique", "update-attr-start", "update-attr-done"),
        "remove-unique" => ("unique", "update-attr-start", "update-attr-done"),
        "required" => ("required", "estimate-work", "revalidate"),
        "remove-required" => ("required", "update-attr", "update-attr"),
        _ => return None,
    })
}

pub struct NewJob {
    pub attr_id: Uuid,
    pub job_type: String,
    pub checked_data_type: Option<String>,
}

/// Insert a `waiting` job row. Returns the job id.
pub async fn create_job(
    conn: &mut PgConnection,
    app_id: Uuid,
    group_id: Uuid,
    job: &NewJob,
) -> Result<Uuid> {
    let (serial_key, stage, _) = job_spec(&job.job_type).ok_or_else(|| {
        InstantError::validation_failed(
            "indexing-job",
            format!("Unexpected job type: {}", job.job_type),
            json!([]),
        )
    })?;
    if job.job_type == "check-data-type" && job.checked_data_type.is_none() {
        return Err(InstantError::param_missing(
            "Missing parameter: [\"checked-data-type\"]",
        ));
    }
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO indexing_jobs (id, group_id, app_id, attr_id, job_serial_key, job_type,
                                    job_stage, job_status, checked_data_type)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'waiting', $8::checked_data_type)",
    )
    .bind(id)
    .bind(group_id)
    .bind(app_id)
    .bind(job.attr_id)
    .bind(serial_key)
    .bind(&job.job_type)
    .bind(stage)
    .bind(job.checked_data_type.as_deref())
    .execute(conn)
    .await
    .map_err(|e| match &e {
        sqlx::Error::Database(db) if db.code().as_deref() == Some("23503") => {
            InstantError::record_not_found(
                "attr",
                format!("Record not found: attr {}", job.attr_id),
            )
        }
        _ => InstantError::from(e),
    })?;
    Ok(id)
}

fn ts(v: Option<DateTime<Utc>>) -> Value {
    match v {
        Some(t) => Value::String(t.format("%Y-%m-%dT%H:%M:%SZ").to_string()),
        None => Value::Null,
    }
}

/// Columns of a job row as the legacy `job->client-format` select-keys
/// emits them.
fn row_to_client(row: &sqlx::postgres::PgRow, full: bool) -> Value {
    let mut m = Map::new();
    m.insert("id".into(), json!(row.get::<Uuid, _>("id")));
    m.insert("app_id".into(), json!(row.get::<Uuid, _>("app_id")));
    m.insert(
        "group_id".into(),
        json!(row.get::<Option<Uuid>, _>("group_id")),
    );
    m.insert("attr_id".into(), json!(row.get::<Uuid, _>("attr_id")));
    if full {
        m.insert(
            "attr_name".into(),
            json!(row.get::<Option<String>, _>("attr_name")),
        );
    }
    m.insert("job_type".into(), json!(row.get::<String, _>("job_type")));
    m.insert(
        "job_status".into(),
        json!(row.get::<String, _>("job_status")),
    );
    m.insert(
        "job_stage".into(),
        json!(row.get::<Option<String>, _>("job_stage")),
    );
    m.insert(
        "work_estimate".into(),
        json!(row.get::<Option<i32>, _>("work_estimate")),
    );
    m.insert(
        "work_completed".into(),
        json!(row.get::<Option<i32>, _>("work_completed")),
    );
    m.insert("error".into(), json!(row.get::<Option<String>, _>("error")));
    m.insert(
        "checked_data_type".into(),
        json!(row.get::<Option<String>, _>("checked_data_type")),
    );
    m.insert(
        "created_at".into(),
        ts(Some(row.get::<DateTime<Utc>, _>("created_at"))),
    );
    m.insert(
        "updated_at".into(),
        ts(Some(row.get::<DateTime<Utc>, _>("updated_at"))),
    );
    m.insert(
        "done_at".into(),
        ts(row.get::<Option<DateTime<Utc>>, _>("done_at")),
    );
    m.insert(
        "invalid_unique_value".into(),
        row.get::<Option<Value>, _>("invalid_unique_value")
            .unwrap_or(Value::Null),
    );
    if full {
        m.insert(
            "invalid_triples_sample".into(),
            row.get::<Option<Value>, _>("invalid_triples_sample")
                .unwrap_or(Value::Null),
        );
    }
    m.insert(
        "error_data".into(),
        row.get::<Option<Value>, _>("error_data")
            .unwrap_or(Value::Null),
    );
    Value::Object(m)
}

const CLIENT_SELECT: &str = r#"
SELECT j.id, j.app_id, j.group_id, j.attr_id, j.job_type, j.job_status, j.job_stage,
       j.work_estimate, j.work_completed, j.error, j.checked_data_type::text AS checked_data_type,
       j.created_at, j.updated_at, j.done_at, j.invalid_unique_value, j.error_data,
       (SELECT idents.etype || '.' || idents.label
          FROM attrs JOIN idents ON attrs.forward_ident = idents.id
         WHERE attrs.id = j.attr_id) AS attr_name,
       CASE j.error
         WHEN 'invalid-triple-error' THEN
           CASE j.job_type WHEN 'check-data-type' THEN
             (SELECT json_agg(t) FROM (
                SELECT t.entity_id, t.value, jsonb_typeof(t.value) AS json_type
                  FROM triples t
                 WHERE t.app_id = j.app_id AND t.attr_id = j.attr_id
                   AND j.checked_data_type IS NOT NULL
                   AND NOT triples_valid_value(j.checked_data_type, t.value)
                 LIMIT 10) t)
           END
         WHEN 'triple-not-unique-error' THEN
           (SELECT json_agg(t) FROM (
              SELECT t.entity_id, t.value, jsonb_typeof(t.value) AS json_type
                FROM triples t
               WHERE t.app_id = j.app_id AND t.attr_id = j.attr_id
                 AND t.value = j.invalid_unique_value
               LIMIT 10) t)
         WHEN 'triple-too-large-error' THEN
           (SELECT json_agg(t) FROM (
              SELECT t.entity_id, t.value, jsonb_typeof(t.value) AS json_type
                FROM triples t
               WHERE t.app_id = j.app_id AND t.attr_id = j.attr_id
                 AND t.entity_id = j.invalid_entity_id
               LIMIT 10) t)
         WHEN 'missing-required-error' THEN
           (SELECT json_agg(t) FROM (
              SELECT t.entity_id, NULL::jsonb AS value, 'null' AS json_type
                FROM triples t
               WHERE t.app_id = j.app_id
                 AND t.attr_id = (SELECT id FROM attrs
                                   WHERE app_id = j.app_id AND label = 'id'
                                     AND etype = (SELECT etype FROM attrs
                                                   WHERE app_id = j.app_id AND id = j.attr_id))
                 AND t.entity_id IN (SELECT (json_array_elements_text((j.error_data->'entity-ids')::json))::uuid)
               LIMIT 10) t)
       END AS invalid_triples_sample
  FROM indexing_jobs j
 WHERE j.app_id = $1
"#;

pub async fn get_by_group_for_client(
    state: &AppState,
    app_id: Uuid,
    group_id: Uuid,
) -> Result<Vec<Value>> {
    let rows = sqlx::query(&format!(
        "{CLIENT_SELECT} AND j.group_id = $2 ORDER BY j.created_at, j.id"
    ))
    .bind(app_id)
    .bind(group_id)
    .fetch_all(&state.pool)
    .await?;
    Ok(rows.iter().map(|r| row_to_client(r, true)).collect())
}

pub async fn get_by_id_for_client(
    state: &AppState,
    app_id: Uuid,
    job_id: Uuid,
) -> Result<Option<Value>> {
    let row = sqlx::query(&format!("{CLIENT_SELECT} AND j.id = $2"))
        .bind(app_id)
        .bind(job_id)
        .fetch_optional(&state.pool)
        .await?;
    Ok(row.map(|r| row_to_client(&r, true)))
}

/// The freshly inserted row exactly as legacy's `create-job!` (INSERT ...
/// RETURNING *) hands it back in the apply response: every table column, no
/// derived attr_name / invalid_triples_sample.
pub async fn get_basic(conn: &mut PgConnection, job_id: Uuid) -> Result<Value> {
    let row = sqlx::query(
        "SELECT id, app_id, group_id, attr_id, job_type, job_status, job_stage, work_estimate,
                work_completed, error, checked_data_type::text AS checked_data_type, created_at,
                updated_at, done_at, invalid_unique_value, error_data,
                job_serial_key, job_dependency, worker_id, invalid_entity_id, error_detail, context
           FROM indexing_jobs WHERE id = $1",
    )
    .bind(job_id)
    .fetch_one(conn)
    .await?;
    let mut v = row_to_client(&row, false);
    if let Some(m) = v.as_object_mut() {
        m.insert(
            "job_serial_key".into(),
            json!(row.get::<String, _>("job_serial_key")),
        );
        m.insert(
            "job_dependency".into(),
            json!(row.get::<Option<Uuid>, _>("job_dependency")),
        );
        m.insert(
            "worker_id".into(),
            json!(row.get::<Option<String>, _>("worker_id")),
        );
        m.insert(
            "invalid_entity_id".into(),
            json!(row.get::<Option<Uuid>, _>("invalid_entity_id")),
        );
        m.insert(
            "error_detail".into(),
            json!(row.get::<Option<String>, _>("error_detail")),
        );
        m.insert(
            "context".into(),
            row.get::<Option<Value>, _>("context")
                .unwrap_or(Value::Null),
        );
    }
    Ok(v)
}

// ---------------------------------------------------------------------------
// runner

struct JobRow {
    id: Uuid,
    app_id: Uuid,
    attr_id: Uuid,
    job_type: String,
    checked_data_type: Option<String>,
}

/// Process every waiting job of a group, in creation order, on this node.
pub fn spawn_group(state: Arc<AppState>, app_id: Uuid, group_id: Uuid) {
    tokio::spawn(async move {
        if let Err(e) = run_group(&state, app_id, group_id).await {
            tracing::warn!(%app_id, %group_id, error = %e, "indexing job group failed");
        }
    });
}

async fn run_group(state: &Arc<AppState>, app_id: Uuid, group_id: Uuid) -> Result<()> {
    let rows = sqlx::query(
        "SELECT id, app_id, attr_id, job_type, checked_data_type::text AS checked_data_type
           FROM indexing_jobs
          WHERE app_id = $1 AND group_id = $2 AND job_status = 'waiting'
          ORDER BY created_at, id",
    )
    .bind(app_id)
    .bind(group_id)
    .fetch_all(&state.pool)
    .await?;
    for row in rows {
        let job = JobRow {
            id: row.get("id"),
            app_id: row.get("app_id"),
            attr_id: row.get("attr_id"),
            job_type: row.get("job_type"),
            checked_data_type: row.get("checked_data_type"),
        };
        run_job(state, &job).await;
    }
    Ok(())
}

/// Outcome of a job's rewrite transaction.
enum Outcome {
    Completed,
    Errored {
        error: &'static str,
        invalid_unique_value: Option<Value>,
        error_data: Option<Value>,
    },
}

async fn run_job(state: &Arc<AppState>, job: &JobRow) {
    // grab: waiting -> processing (the partial unique index on
    // (app_id, attr_id, job_serial_key) WHERE processing serializes
    // conflicting jobs across nodes)
    let grabbed = sqlx::query(
        "UPDATE indexing_jobs SET job_status = 'processing', worker_id = $2
          WHERE id = $1 AND job_status = 'waiting'",
    )
    .bind(job.id)
    .bind(state.node_id.to_string())
    .execute(&state.pool)
    .await;
    match grabbed {
        Ok(r) if r.rows_affected() == 1 => {}
        Ok(_) => return,
        Err(e) => {
            tracing::warn!(job = %job.id, error = %e, "could not grab indexing job");
            return;
        }
    }
    let (_, _, final_stage) = job_spec(&job.job_type).unwrap_or(("", "", ""));
    // the stage a job's validation fails in (indexing_jobs.clj *--stages)
    let error_stage = match job.job_type.as_str() {
        "check-data-type" | "required" => "validate",
        _ => "update-triples",
    };
    let outcome = match execute(state, job).await {
        Ok(o) => o,
        Err(e) => {
            tracing::warn!(job = %job.id, job_type = %job.job_type, error = %e, "indexing job failed");
            Outcome::Errored {
                error: classify_error(&e),
                invalid_unique_value: None,
                error_data: None,
            }
        }
    };
    let res = match outcome {
        Outcome::Completed => {
            sqlx::query(
                "UPDATE indexing_jobs
                    SET job_status = 'completed', job_stage = $2, done_at = now(), worker_id = NULL,
                        work_completed = coalesce(work_estimate, 0)
                  WHERE id = $1",
            )
            .bind(job.id)
            .bind(final_stage)
            .execute(&state.pool)
            .await
        }
        Outcome::Errored {
            error,
            invalid_unique_value,
            error_data,
        } => {
            sqlx::query(
                // legacy mark-error! leaves done_at unset
                "UPDATE indexing_jobs
                    SET job_status = 'errored', error = $2, invalid_unique_value = $3,
                        error_data = $4, job_stage = $5, worker_id = NULL
                  WHERE id = $1",
            )
            .bind(job.id)
            .bind(error)
            .bind(invalid_unique_value)
            .bind(error_data)
            .bind(error_stage)
            .execute(&state.pool)
            .await
        }
    };
    if let Err(e) = res {
        tracing::warn!(job = %job.id, error = %e, "could not finalize indexing job");
    }
}

fn classify_error(e: &InstantError) -> &'static str {
    if e.error_type == "record-not-unique" {
        TRIPLE_NOT_UNIQUE_ERROR
    } else if e.message.contains("index row size") || e.message.contains("exceeds") {
        TRIPLE_TOO_LARGE_ERROR
    } else if e.error_type == "record-not-found" {
        INVALID_ATTR_STATE_ERROR
    } else {
        UNEXPECTED_ERROR
    }
}

/// Run the job's rewrite in one transaction. A `transactions` row is created
/// first (like legacy update-attr!) so the change reaches the invalidator and
/// clients' attrs refresh.
async fn execute(state: &Arc<AppState>, job: &JobRow) -> Result<Outcome> {
    let app_id = job.app_id;
    let mut dbtx = state.pool.begin().await?;
    let attrs = instant_core::attr::get_by_app_id(&mut *dbtx, app_id).await?;
    let Some(attr) = attrs.get(&job.attr_id).cloned() else {
        return Err(InstantError::record_not_found(
            "attr",
            format!("attr {} not found", job.attr_id),
        ));
    };
    // work estimate: the attr's triple count
    let estimate: i64 =
        sqlx::query("SELECT count(*) AS n FROM triples WHERE app_id = $1 AND attr_id = $2")
            .bind(app_id)
            .bind(job.attr_id)
            .fetch_one(&mut *dbtx)
            .await?
            .get("n");
    sqlx::query("UPDATE indexing_jobs SET work_estimate = $2 WHERE id = $1")
        .bind(job.id)
        .bind(estimate.min(i32::MAX as i64) as i32)
        .execute(&mut *dbtx)
        .await?;

    let row = sqlx::query(
        "WITH t AS (INSERT INTO transactions (app_id) VALUES ($1) RETURNING id)
         SELECT id, set_config('instant.rust_tx_id', id::text, true) AS tag FROM t",
    )
    .bind(app_id)
    .fetch_one(&mut *dbtx)
    .await?;
    let tx_id: i64 = row.get("id");

    let outcome = match job.job_type.as_str() {
        "index" => {
            instant_core::attr::update(&mut *dbtx, app_id, &attr, &json!({"index?": true})).await?;
            insert_nulls(&mut dbtx, app_id, &attrs, &attr).await?;
            Outcome::Completed
        }
        "remove-index" => {
            instant_core::attr::update(&mut *dbtx, app_id, &attr, &json!({"index?": false}))
                .await?;
            Outcome::Completed
        }
        "unique" => match duplicate_value(&mut dbtx, app_id, job.attr_id).await? {
            Some(value) => Outcome::Errored {
                error: TRIPLE_NOT_UNIQUE_ERROR,
                invalid_unique_value: Some(value),
                error_data: None,
            },
            None => {
                instant_core::attr::update(&mut *dbtx, app_id, &attr, &json!({"unique?": true}))
                    .await?;
                Outcome::Completed
            }
        },
        "remove-unique" => {
            instant_core::attr::update(&mut *dbtx, app_id, &attr, &json!({"unique?": false}))
                .await?;
            Outcome::Completed
        }
        "required" => match missing_required(&mut dbtx, app_id, &attrs, &attr).await? {
            Some(error_data) => Outcome::Errored {
                error: MISSING_REQUIRED_ERROR,
                invalid_unique_value: None,
                error_data: Some(error_data),
            },
            None => {
                instant_core::attr::update(&mut *dbtx, app_id, &attr, &json!({"required?": true}))
                    .await?;
                Outcome::Completed
            }
        },
        "remove-required" => {
            instant_core::attr::update(&mut *dbtx, app_id, &attr, &json!({"required?": false}))
                .await?;
            Outcome::Completed
        }
        "check-data-type" => {
            let cdt = job.checked_data_type.clone().ok_or_else(|| {
                InstantError::validation_failed(
                    "indexing-job",
                    "missing checked-data-type",
                    json!([]),
                )
            })?;
            if has_invalid_values(&mut dbtx, app_id, job.attr_id, &cdt).await? {
                Outcome::Errored {
                    error: INVALID_TRIPLE_ERROR,
                    invalid_unique_value: None,
                    error_data: None,
                }
            } else {
                instant_core::attr::update(
                    &mut *dbtx,
                    app_id,
                    &attr,
                    &json!({"checked-data-type": cdt}),
                )
                .await?;
                Outcome::Completed
            }
        }
        "remove-data-type" => {
            instant_core::attr::update(
                &mut *dbtx,
                app_id,
                &attr,
                &json!({"checked-data-type": null}),
            )
            .await?;
            Outcome::Completed
        }
        other => {
            return Err(InstantError::validation_failed(
                "indexing-job",
                format!("Unexpected job type: {other}"),
                json!([]),
            ))
        }
    };
    dbtx.commit().await?;
    service::notify_tx(state, app_id, tx_id, true).await;
    Ok(outcome)
}

/// index--insert-nulls: every entity of the etype gets a `null` triple for the
/// newly indexed blob attr, so ordered/indexed scans see it.
async fn insert_nulls(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &AttrMap,
    attr: &Attr,
) -> Result<()> {
    if attr.value_type != ValueType::Blob {
        return Ok(());
    }
    let Some(id_attr) = attrs.id_attr_of(&attr.etype) else {
        return Ok(());
    };
    let mut indexed = attr.clone();
    indexed.is_indexed = true;
    let flags = indexed.flags();
    sqlx::query(
        r#"
        INSERT INTO triples (app_id, entity_id, attr_id, value, value_md5,
                             ea, eav, av, ave, vae, checked_data_type)
        SELECT $1, ids.entity_id, $2, 'null'::jsonb, $3, $4, $5, $6, $7, $8, $9::checked_data_type
          FROM triples ids
         WHERE ids.app_id = $1 AND ids.attr_id = $10
           AND NOT EXISTS (SELECT 1 FROM triples t
                            WHERE t.app_id = $1 AND t.attr_id = $2 AND t.entity_id = ids.entity_id)
        "#,
    )
    .bind(app_id)
    .bind(attr.id)
    .bind(instant_core::triple::JSON_NULL_MD5)
    .bind(flags.ea)
    .bind(flags.eav)
    .bind(flags.av)
    .bind(flags.ave)
    .bind(flags.vae)
    .bind(attr.checked_data_type.map(|c| c.as_str()))
    .bind(id_attr.id)
    .execute(conn)
    .await?;
    Ok(())
}

/// A value held by more than one triple of the attr (nulls never collide:
/// av_ignore_nulls_index).
async fn duplicate_value(
    conn: &mut PgConnection,
    app_id: Uuid,
    attr_id: Uuid,
) -> Result<Option<Value>> {
    let row = sqlx::query(
        "SELECT value FROM triples
          WHERE app_id = $1 AND attr_id = $2 AND value <> 'null'::jsonb
          GROUP BY value HAVING count(*) > 1
          ORDER BY min(created_at) LIMIT 1",
    )
    .bind(app_id)
    .bind(attr_id)
    .fetch_optional(conn)
    .await?;
    Ok(row.map(|r| r.get::<Value, _>("value")))
}

/// required--validate: entities of the attr's etype lacking a non-null value.
/// Returns the legacy error-data on failure.
async fn missing_required(
    conn: &mut PgConnection,
    app_id: Uuid,
    attrs: &AttrMap,
    attr: &Attr,
) -> Result<Option<Value>> {
    let Some(id_attr) = attrs.id_attr_of(&attr.etype) else {
        return Err(InstantError::record_not_found(
            "attr",
            "Could not find id attribute for entity.",
        ));
    };
    let rows = sqlx::query(
        "SELECT ids.entity_id FROM triples ids
          WHERE ids.app_id = $1 AND ids.attr_id = $2
            AND NOT EXISTS (SELECT 1 FROM triples t
                             WHERE t.app_id = $1 AND t.attr_id = $3 AND t.entity_id = ids.entity_id
                               AND t.value <> 'null'::jsonb)
          ORDER BY ids.created_at, ids.entity_id",
    )
    .bind(app_id)
    .bind(id_attr.id)
    .bind(attr.id)
    .fetch_all(conn)
    .await?;
    if rows.is_empty() {
        return Ok(None);
    }
    let ids: Vec<Uuid> = rows.iter().map(|r| r.get::<Uuid, _>("entity_id")).collect();
    Ok(Some(json!({
        "count": ids.len(),
        "etype": attr.etype,
        "label": attr.label,
        "entity-ids": ids.iter().take(10).collect::<Vec<_>>(),
    })))
}

async fn has_invalid_values(
    conn: &mut PgConnection,
    app_id: Uuid,
    attr_id: Uuid,
    checked_data_type: &str,
) -> Result<bool> {
    let row = sqlx::query(
        "SELECT EXISTS (SELECT 1 FROM triples
                         WHERE app_id = $1 AND attr_id = $2
                           AND NOT triples_valid_value($3::checked_data_type, value)) AS bad",
    )
    .bind(app_id)
    .bind(attr_id)
    .bind(checked_data_type)
    .fetch_one(conn)
    .await?;
    Ok(row.get::<bool, _>("bad"))
}
