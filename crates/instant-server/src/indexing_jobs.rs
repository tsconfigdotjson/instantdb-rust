//! Indexing jobs: the background schema changes behind `instant-cli push
//! schema` (index / unique / required / check-data-type and their removals).
//! Port of LEGACY db/indexing_jobs.clj on the legacy `indexing_jobs` table,
//! so the CLI's job polling (`GET /dash/apps/:app_id/indexing-jobs/group/:id`)
//! sees the same rows and error codes.
//!
//! Jobs run as the legacy stage machine (issue #5): the attr is flagged
//! in-flight first (`indexing` / `setting_unique` / `checking_data_type`, which
//! `Attr::to_wire` reports as `indexing?` etc. and the query planner treats as
//! "not yet indexed"), the triples are then rewritten in batches of
//! `INSTANT_INDEXING_BATCH_SIZE` rows resumed from a cursor stored in the job
//! row, progress lands in `work_estimate` / `work_completed`, and the in-flight
//! flag is cleared at the end. The job is released between steps, so any node
//! can continue it and orphaned jobs are swept up (`sweep_loop`).

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
pub const UNEXPECTED_ERROR: &str = "unexpected-error";
/// legacy `invalid-attr-state-error` (indexing_jobs.clj:469-486): the attr's
/// flags changed underneath the job, so the guarded `update-attr-done`
/// matched no row.
pub const INVALID_ATTR_STATE_ERROR: &str = "invalid-attr-state-error";

/// (serial key, first stage, final stage) per job type (indexing_jobs.clj
/// `jobs` + the `*--stages` vectors).
pub fn job_spec(job_type: &str) -> Option<(&'static str, &'static str, &'static str)> {
    let serial_key = match job_type {
        "check-data-type" | "remove-data-type" => "data-type",
        "index" | "remove-index" => "index",
        "unique" | "remove-unique" => "unique",
        "required" | "remove-required" => "required",
        _ => return None,
    };
    let stages = stages(job_type)?;
    Some((serial_key, stages[0], stages[stages.len() - 1]))
}

pub struct NewJob {
    pub attr_id: Uuid,
    pub job_type: String,
    pub checked_data_type: Option<String>,
}

/// Insert a `waiting` job row. Returns the job id. `group_id` is None for a
/// directly created job (`POST /dash/apps/:id/indexing-jobs`), like legacy's
/// `create-job!` without a `:group-id`.
pub async fn create_job(
    conn: &mut PgConnection,
    app_id: Uuid,
    group_id: Option<Uuid>,
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
    // legacy create-job! (indexing_jobs.clj:225-227) only stores the checked
    // type for check-data-type jobs
    let checked_data_type = if job.job_type == "check-data-type" {
        job.checked_data_type.as_deref()
    } else {
        None
    };
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
    .bind(checked_data_type)
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

/// The freshly inserted row through legacy's `job->client-format`
/// (indexing_jobs.clj:71-89) as `POST /dash/apps/:id/indexing-jobs` answers:
/// only the client columns, and neither `attr_name` nor
/// `invalid_triples_sample` (which the GET routes compute).
pub async fn get_client_format(conn: &mut PgConnection, job_id: Uuid) -> Result<Value> {
    let row = sqlx::query(
        "SELECT id, app_id, group_id, attr_id, job_type, job_status, job_stage, work_estimate,
                work_completed, error, checked_data_type::text AS checked_data_type, created_at,
                updated_at, done_at, invalid_unique_value, error_data
           FROM indexing_jobs WHERE id = $1",
    )
    .bind(job_id)
    .fetch_one(conn)
    .await?;
    Ok(row_to_client(&row, false))
}

/// Start one directly created (group-less) job on this node.
pub fn spawn_job(state: Arc<AppState>, job_id: Uuid) {
    tokio::spawn(async move {
        process_job(&state, job_id, CONFLICT_RETRIES).await;
    });
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
//
// A port of the legacy stage machine (indexing_jobs.clj): every job type is
// a list of stages; a worker grabs the job (worker_id + processing), runs ONE
// step of the current stage — for the triple-rewriting stages one batch of
// `indexing_batch_size` rows, resumed from a cursor kept in
// `context[job_stage]` — records progress, releases the job again and loops.
// Nothing is held across steps, so a job can migrate between nodes and a
// job orphaned by a crashed node is picked up by the periodic sweep
// (`sweep_loop`, the legacy grab-forgotten-jobs!).

/// A job row as the runner sees it.
#[derive(Debug, Clone)]
pub struct Job {
    pub id: Uuid,
    pub app_id: Uuid,
    pub attr_id: Uuid,
    pub job_type: String,
    pub job_stage: String,
    pub checked_data_type: Option<String>,
    pub context: Option<Value>,
}

/// Resume point of a batched scan over an attr's triples (legacy `after`
/// cursor: created_at, entity_id and — for cardinality-many attrs — value_md5).
#[derive(Debug, Clone, Default)]
struct Cursor {
    t: Option<i64>,
    e: Option<Uuid>,
    md5: Option<String>,
}

impl Job {
    /// The cursor stored for the current stage, if any.
    fn cursor(&self) -> Cursor {
        let after = self
            .context
            .as_ref()
            .and_then(|c| c.get(&self.job_stage))
            .and_then(|c| c.get("after"));
        let Some(after) = after else {
            return Cursor::default();
        };
        Cursor {
            t: after.get("t").and_then(|v| v.as_i64()),
            e: after
                .get("e")
                .and_then(|v| v.as_str())
                .and_then(|s| Uuid::parse_str(s).ok()),
            md5: after
                .get("md5")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        }
    }
}

/// The ordered stages of each job type (indexing_jobs.clj `*--stages`).
pub fn stages(job_type: &str) -> Option<&'static [&'static str]> {
    Some(match job_type {
        "check-data-type" => &[
            "estimate-work",
            "validate",
            "update-attr-start",
            "revalidate",
            "update-triples",
            "update-attr-done",
        ],
        "remove-data-type" => &[
            "update-attr-start",
            "estimate-work",
            "update-triples",
            "update-attr-done",
        ],
        "index" => &[
            "update-attr-start",
            "estimate-work",
            "estimate-work-with-undefineds",
            "update-triples",
            "insert-nulls",
            "update-attr-done",
        ],
        "remove-index" | "unique" | "remove-unique" => &[
            "update-attr-start",
            "estimate-work",
            "update-triples",
            "update-attr-done",
        ],
        "required" => &["estimate-work", "validate", "update-attr", "revalidate"],
        "remove-required" => &["update-attr"],
        _ => return None,
    })
}

const JOB_TYPES: [&str; 8] = [
    "check-data-type",
    "remove-data-type",
    "index",
    "remove-index",
    "unique",
    "remove-unique",
    "required",
    "remove-required",
];

/// What a stage step decided.
enum Step {
    /// stage finished: advance (or complete the job)
    Next,
    /// run the same stage again, optionally with a new cursor context
    Repeat(Option<Value>),
    /// validation failed with a plain error code (legacy ::error)
    Error(&'static str),
    /// failed with details (legacy ::exception → mark-error-from-ex-info!)
    Failed(JobError),
}

#[derive(Debug, Default)]
struct JobError {
    error: &'static str,
    invalid_unique_value: Option<Value>,
    invalid_entity_id: Option<Uuid>,
    error_data: Option<Value>,
    error_detail: Option<String>,
}

fn worker_id(state: &AppState) -> String {
    state.node_id.to_string()
}

fn batch_size(state: &AppState) -> i64 {
    state.cfg.indexing_batch_size.max(1) as i64
}

/// Start every waiting job of a freshly created group on this node. Jobs
/// run concurrently (bounded), like legacy's worker pool; two jobs on the
/// same attr + serial key are serialized by the partial unique index on
/// indexing_jobs and the loser retries.
pub fn spawn_group(state: Arc<AppState>, app_id: Uuid, group_id: Uuid) {
    tokio::spawn(async move {
        let rows = sqlx::query(
            "SELECT id FROM indexing_jobs
              WHERE app_id = $1 AND group_id = $2 AND job_status = 'waiting'
              ORDER BY created_at, id",
        )
        .bind(app_id)
        .bind(group_id)
        .fetch_all(&state.pool)
        .await;
        let rows = match rows {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(%app_id, %group_id, error = %e, "could not list indexing job group");
                return;
            }
        };
        let sem = Arc::new(tokio::sync::Semaphore::new(GROUP_CONCURRENCY));
        let mut handles = vec![];
        for row in rows {
            let job_id: Uuid = row.get("id");
            let state = state.clone();
            let sem = sem.clone();
            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await;
                process_job(&state, job_id, CONFLICT_RETRIES).await;
            }));
        }
        for h in handles {
            let _ = h.await;
        }
    });
}

const GROUP_CONCURRENCY: usize = 4;
/// How long a job waits (in 500ms polls) for a conflicting job on the same
/// attr + serial key before leaving itself to the sweep.
const CONFLICT_RETRIES: usize = 600;

enum Grab {
    Grabbed(Job),
    /// taken by another worker, done, or dependency not met
    Unavailable,
    /// another job with the same (app, attr, serial key) is processing
    Conflict,
}

/// legacy grab-job!: waiting/processing, unowned, dependency satisfied.
async fn grab_job(state: &AppState, job_id: Uuid) -> Result<Grab> {
    let res = sqlx::query(
        "UPDATE indexing_jobs SET worker_id = $2, job_status = 'processing'
          WHERE id = $1 AND worker_id IS NULL
            AND job_status IN ('waiting', 'processing')
            AND job_type = ANY($3)
            AND (job_dependency IS NULL
                 OR (SELECT d.job_status FROM indexing_jobs d WHERE d.id = indexing_jobs.job_dependency)
                    = 'completed')
          RETURNING id, app_id, attr_id, job_type, job_stage,
                    checked_data_type::text AS checked_data_type, context",
    )
    .bind(job_id)
    .bind(worker_id(state))
    .bind(JOB_TYPES.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    .fetch_optional(&state.pool)
    .await;
    match res {
        Ok(Some(row)) => Ok(Grab::Grabbed(Job {
            id: row.get("id"),
            app_id: row.get("app_id"),
            attr_id: row.get("attr_id"),
            job_type: row.get("job_type"),
            job_stage: row
                .get::<Option<String>, _>("job_stage")
                .unwrap_or_default(),
            checked_data_type: row.get("checked_data_type"),
            context: row.get("context"),
        })),
        Ok(None) => Ok(Grab::Unavailable),
        Err(sqlx::Error::Database(db)) if db.code().as_deref() == Some("23505") => {
            Ok(Grab::Conflict)
        }
        Err(e) => Err(e.into()),
    }
}

/// Drive one job to its end (or until another worker takes it over):
/// grab → one stage step → release, repeated.
pub async fn process_job(state: &Arc<AppState>, job_id: Uuid, mut conflict_retries: usize) {
    loop {
        let job = match grab_job(state, job_id).await {
            Ok(Grab::Grabbed(job)) => job,
            Ok(Grab::Unavailable) => return,
            Ok(Grab::Conflict) => {
                if conflict_retries == 0 {
                    tracing::info!(job = %job_id, "indexing job blocked by a conflicting job; leaving it to the sweep");
                    return;
                }
                conflict_retries -= 1;
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }
            Err(e) => {
                tracing::warn!(job = %job_id, error = %e, "could not grab indexing job");
                return;
            }
        };
        let still_processing = match run_next_stage(state, &job).await {
            Ok(v) => v,
            Err(e) => {
                // legacy handle-process: anything unexpected errors the job
                tracing::warn!(job = %job.id, job_type = %job.job_type, stage = %job.job_stage, error = %e, "indexing job failed");
                let _ = mark_error(
                    state,
                    &job,
                    &JobError {
                        error: UNEXPECTED_ERROR,
                        error_detail: Some(e.message.clone()),
                        ..Default::default()
                    },
                )
                .await;
                return;
            }
        };
        if !still_processing {
            return;
        }
        if let Err(e) = release_job(state, &job).await {
            tracing::warn!(job = %job.id, error = %e, "could not release indexing job");
            return;
        }
        tokio::task::yield_now().await;
    }
}

/// legacy run-next-stage: one step of the current stage, then bookkeeping.
/// Returns whether the job is still processing.
async fn run_next_stage(state: &Arc<AppState>, job: &Job) -> Result<bool> {
    let stages = stages(&job.job_type).ok_or_else(|| {
        InstantError::validation_failed(
            "indexing-job",
            format!("Unexpected job type: {}", job.job_type),
            json!([]),
        )
    })?;
    let idx = stages
        .iter()
        .position(|s| *s == job.job_stage)
        .ok_or_else(|| {
            InstantError::validation_failed(
                "indexing-job",
                format!("Unknown stage: {} {}", job.job_type, job.job_stage),
                json!([]),
            )
        })?;
    match run_stage(state, job, stages[idx]).await? {
        Step::Error(code) => {
            mark_error(
                state,
                job,
                &JobError {
                    error: code,
                    ..Default::default()
                },
            )
            .await?;
            Ok(false)
        }
        Step::Failed(err) => {
            mark_error(state, job, &err).await?;
            Ok(false)
        }
        Step::Repeat(ctx) => {
            if let Some(ctx) = ctx {
                set_context(state, job, &ctx).await?;
            }
            Ok(true)
        }
        Step::Next => {
            if idx + 1 == stages.len() {
                mark_completed(state, job).await?;
                Ok(false)
            } else {
                set_stage(state, job, stages[idx + 1]).await?;
                Ok(true)
            }
        }
    }
}

async fn run_stage(state: &Arc<AppState>, job: &Job, stage: &str) -> Result<Step> {
    let cdt = job.checked_data_type.as_deref();
    match (job.job_type.as_str(), stage) {
        // check-data-type
        ("check-data-type", "estimate-work") => estimate_work(state, job).await,
        ("check-data-type", "validate") | ("check-data-type", "revalidate") => {
            check_data_type_validate(state, job).await
        }
        ("check-data-type", "update-attr-start") => {
            update_attr(
                state,
                job,
                "checked_data_type = $3::checked_data_type, checking_data_type = true",
                "",
                cdt,
            )
            .await?;
            Ok(Step::Next)
        }
        ("check-data-type", "update-triples") => set_checked_data_type_batch(state, job, cdt).await,
        ("check-data-type", "update-attr-done") => {
            let matched = update_attr(
                state,
                job,
                "checking_data_type = false",
                "AND checked_data_type = $3::checked_data_type AND checking_data_type = true",
                cdt,
            )
            .await?;
            Ok(guarded(matched))
        }
        // remove-data-type
        ("remove-data-type", "update-attr-start") => {
            update_attr(
                state,
                job,
                "checked_data_type = NULL, checking_data_type = true",
                "",
                None,
            )
            .await?;
            Ok(Step::Next)
        }
        ("remove-data-type", "estimate-work") => estimate_work(state, job).await,
        ("remove-data-type", "update-triples") => {
            set_checked_data_type_batch(state, job, None).await
        }
        ("remove-data-type", "update-attr-done") => {
            let matched = update_attr(
                state,
                job,
                "checking_data_type = false",
                "AND checked_data_type IS NULL AND checking_data_type = true",
                None,
            )
            .await?;
            Ok(guarded(matched))
        }
        // index
        ("index", "update-attr-start") => {
            let _ = update_attr(state, job, "is_indexed = true, indexing = true", "", None).await?;
            Ok(Step::Next)
        }
        ("index", "estimate-work") => estimate_work(state, job).await,
        ("index", "estimate-work-with-undefineds") => estimate_insert_nulls(state, job).await,
        ("index", "update-triples") => match set_flag_batch(state, job, "ave").await {
            Ok(step) => Ok(step),
            Err(e) => {
                abort_index(state, job).await?;
                Ok(Step::Failed(rewrite_failure(state, job, &e).await))
            }
        },
        ("index", "insert-nulls") => match insert_nulls_batch(state, job).await {
            Ok(step) => Ok(step),
            Err(e) => {
                abort_index(state, job).await?;
                Ok(Step::Failed(rewrite_failure(state, job, &e).await))
            }
        },
        ("index", "update-attr-done") => {
            let matched = update_attr(
                state,
                job,
                "indexing = false",
                "AND is_indexed = true AND indexing = true",
                None,
            )
            .await?;
            Ok(guarded(matched))
        }
        // remove-index
        ("remove-index", "update-attr-start") => {
            let _ =
                update_attr(state, job, "is_indexed = false, indexing = true", "", None).await?;
            Ok(Step::Next)
        }
        ("remove-index", "estimate-work") => estimate_work(state, job).await,
        ("remove-index", "update-triples") => clear_flag_batch(state, job, "ave").await,
        ("remove-index", "update-attr-done") => {
            let matched = update_attr(
                state,
                job,
                "indexing = false",
                "AND is_indexed = false AND indexing = true",
                None,
            )
            .await?;
            Ok(guarded(matched))
        }
        // unique
        ("unique", "update-attr-start") => {
            update_attr(
                state,
                job,
                "is_unique = true, setting_unique = true",
                "",
                None,
            )
            .await?;
            Ok(Step::Next)
        }
        ("unique", "estimate-work") => estimate_work(state, job).await,
        ("unique", "update-triples") => match set_flag_batch(state, job, "av").await {
            Ok(step) => Ok(step),
            Err(e) => {
                abort_unique(state, job).await?;
                Ok(Step::Failed(rewrite_failure(state, job, &e).await))
            }
        },
        ("unique", "update-attr-done") => {
            let matched = update_attr(
                state,
                job,
                "setting_unique = false",
                "AND is_unique = true AND setting_unique = true",
                None,
            )
            .await?;
            Ok(guarded(matched))
        }
        // remove-unique
        ("remove-unique", "update-attr-start") => {
            update_attr(
                state,
                job,
                "is_unique = false, setting_unique = true",
                "",
                None,
            )
            .await?;
            Ok(Step::Next)
        }
        ("remove-unique", "estimate-work") => estimate_work(state, job).await,
        ("remove-unique", "update-triples") => clear_flag_batch(state, job, "av").await,
        ("remove-unique", "update-attr-done") => {
            let matched = update_attr(
                state,
                job,
                "setting_unique = false",
                "AND is_unique = false AND setting_unique = true",
                None,
            )
            .await?;
            Ok(guarded(matched))
        }
        // required
        ("required", "estimate-work") => estimate_required(state, job).await,
        ("required", "validate") | ("required", "revalidate") => {
            required_validate(state, job).await
        }
        ("required", "update-attr") => {
            let _ = update_attr(state, job, "is_required = true", "", None).await?;
            Ok(Step::Next)
        }
        // remove-required
        ("remove-required", "update-attr") => {
            let _ = update_attr(state, job, "is_required = false", "", None).await?;
            Ok(Step::Next)
        }
        (t, s) => Err(InstantError::validation_failed(
            "indexing-job",
            format!("Unknown stage: {t} {s}"),
            json!([]),
        )),
    }
}

// ---------------------------------------------------------------------------
// job bookkeeping (all guarded like legacy job-update-wheres: only the
// worker that holds the job may change it)

const OWNED: &str = "WHERE id = $1 AND worker_id = $2 AND job_status = 'processing'";

async fn release_job(state: &AppState, job: &Job) -> Result<()> {
    sqlx::query(&format!(
        "UPDATE indexing_jobs SET worker_id = NULL {OWNED}"
    ))
    .bind(job.id)
    .bind(worker_id(state))
    .execute(&state.pool)
    .await?;
    Ok(())
}

async fn set_stage(state: &AppState, job: &Job, stage: &str) -> Result<()> {
    sqlx::query(&format!("UPDATE indexing_jobs SET job_stage = $3 {OWNED}"))
        .bind(job.id)
        .bind(worker_id(state))
        .bind(stage)
        .execute(&state.pool)
        .await?;
    Ok(())
}

/// legacy set-context!: `context[job_stage] = ctx`.
async fn set_context(state: &AppState, job: &Job, ctx: &Value) -> Result<()> {
    let res = sqlx::query(&format!(
        "UPDATE indexing_jobs
            SET context = jsonb_set(coalesce(context, '{{}}'::jsonb), ARRAY[$3::text], $4::jsonb)
          {OWNED}"
    ))
    .bind(job.id)
    .bind(worker_id(state))
    .bind(&job.job_stage)
    .bind(ctx)
    .execute(&state.pool)
    .await?;
    if res.rows_affected() == 0 {
        return Err(InstantError::validation_failed(
            "indexing-jobs",
            "Unable to update context for indexing job.",
            json!([]),
        ));
    }
    Ok(())
}

async fn mark_completed(state: &AppState, job: &Job) -> Result<()> {
    // like legacy mark-job-completed!: job_stage stays at the final stage
    sqlx::query(&format!(
        "UPDATE indexing_jobs SET job_status = 'completed', done_at = now() {OWNED}"
    ))
    .bind(job.id)
    .bind(worker_id(state))
    .execute(&state.pool)
    .await?;
    Ok(())
}

async fn mark_error(state: &AppState, job: &Job, err: &JobError) -> Result<()> {
    // legacy mark-error!: status + the error fields; done_at stays unset
    sqlx::query(&format!(
        "UPDATE indexing_jobs
            SET job_status = 'errored', error = $3, invalid_unique_value = $4,
                invalid_entity_id = $5, error_data = $6, error_detail = $7
          {OWNED}"
    ))
    .bind(job.id)
    .bind(worker_id(state))
    .bind(err.error)
    .bind(&err.invalid_unique_value)
    .bind(err.invalid_entity_id)
    .bind(&err.error_data)
    .bind(&err.error_detail)
    .execute(&state.pool)
    .await?;
    Ok(())
}

async fn set_work_estimate(state: &AppState, job: &Job, estimate: i64, add: bool) -> Result<()> {
    let estimate = estimate.clamp(0, i32::MAX as i64) as i32;
    let sql = if add {
        format!("UPDATE indexing_jobs SET work_estimate = coalesce(work_estimate, 0) + $3 {OWNED}")
    } else {
        format!("UPDATE indexing_jobs SET work_estimate = $3 {OWNED}")
    };
    sqlx::query(&sql)
        .bind(job.id)
        .bind(worker_id(state))
        .bind(estimate)
        .execute(&state.pool)
        .await?;
    Ok(())
}

async fn add_work_completed(state: &AppState, job: &Job, n: i64) -> Result<()> {
    if n <= 0 {
        return Ok(());
    }
    sqlx::query(&format!(
        "UPDATE indexing_jobs SET work_completed = coalesce(work_completed, 0) + $3 {OWNED}"
    ))
    .bind(job.id)
    .bind(worker_id(state))
    .bind(n.min(i32::MAX as i64) as i32)
    .execute(&state.pool)
    .await?;
    Ok(())
}

/// legacy update-attr!: the attrs UPDATE together with a `transactions` row,
/// so the invalidator refreshes every session's attrs (clients see
/// `indexing?` appear and disappear). `set` / `where_` may reference
/// `$3::checked_data_type` when `cdt` is given. Returns whether the guarded
/// update matched the attr; legacy answers `[::error
/// invalid-attr-state-error]` when it did not (indexing_jobs.clj:485-486).
async fn update_attr(
    state: &AppState,
    job: &Job,
    set: &str,
    where_: &str,
    cdt: Option<&str>,
) -> Result<bool> {
    let mut dbtx = state.pool.begin().await?;
    let tx_id: i64 = sqlx::query("INSERT INTO transactions (app_id) VALUES ($1) RETURNING id")
        .bind(job.app_id)
        .fetch_one(&mut *dbtx)
        .await?
        .get("id");
    let sql = format!("UPDATE attrs SET {set} WHERE app_id = $1 AND id = $2 {where_}");
    let q = sqlx::query(&sql).bind(job.app_id).bind(job.attr_id);
    let q = if let Some(cdt) = cdt { q.bind(cdt) } else { q };
    let matched = q.execute(&mut *dbtx).await?.rows_affected() > 0;
    dbtx.commit().await?;
    // an attr flag flip evicts attr caches and, like legacy's attrs-row
    // topic, refreshes the sessions whose queries mention the attr; it is
    // not a schema change (only attr inserts / deletes and ident changes
    // refresh every session)
    service::notify_tx(
        state,
        job.app_id,
        &service::TxNotice {
            tx_id,
            attrs_changed: true,
            schema_changed: false,
            requery_all: false,
            changed_attrs: vec![job.attr_id],
        },
    )
    .await;
    Ok(matched)
}

/// `update-attr-done` and the abort paths: a guard that no longer matches
/// errors the job with `invalid-attr-state-error`.
fn guarded(matched: bool) -> Step {
    if matched {
        Step::Next
    } else {
        Step::Error(INVALID_ATTR_STATE_ERROR)
    }
}

/// The attr map straight from Postgres (bypassing the per-node cache) and
/// the job's attr.
async fn load_attr(state: &AppState, job: &Job) -> Result<(AttrMap, Attr)> {
    let attrs = instant_core::attr::get_by_app_id(&state.pool, job.app_id).await?;
    let attr = attrs.get(&job.attr_id).cloned().ok_or_else(|| {
        InstantError::record_not_found("attr", format!("attr {} not found", job.attr_id))
    })?;
    Ok((attrs, attr))
}

// ---------------------------------------------------------------------------
// work estimates (legacy reads a count-min sketch; we count — exact, and the
// CLI only turns these into a progress percentage)

/// legacy update-work-estimate!: the attr's triple count (×3 for
/// check-data-type: validate, revalidate and update each walk them), +5%.
async fn estimate_work(state: &AppState, job: &Job) -> Result<Step> {
    let n: i64 =
        sqlx::query("SELECT count(*) AS n FROM triples WHERE app_id = $1 AND attr_id = $2")
            .bind(job.app_id)
            .bind(job.attr_id)
            .fetch_one(&state.pool)
            .await?
            .get("n");
    let mult = if job.job_type == "check-data-type" {
        3
    } else {
        1
    };
    let estimate = (1.05 * (n * mult) as f64) as i64;
    set_work_estimate(state, job, estimate, false).await?;
    Ok(Step::Next)
}

/// legacy update-insert-nulls-work-estimate!: + the etype's entity count
/// (blob attrs only — refs get no null rows).
async fn estimate_insert_nulls(state: &AppState, job: &Job) -> Result<Step> {
    let (attrs, attr) = load_attr(state, job).await?;
    if attr.value_type != ValueType::Blob {
        return Ok(Step::Next);
    }
    let id_attr = attrs.id_attr_of(&attr.etype).ok_or_else(|| {
        InstantError::validation_failed(
            "attr",
            format!("{} has no id attribute", attr.etype),
            json!([]),
        )
    })?;
    let n: i64 =
        sqlx::query("SELECT count(*) AS n FROM triples WHERE app_id = $1 AND attr_id = $2")
            .bind(job.app_id)
            .bind(id_attr.id)
            .fetch_one(&state.pool)
            .await?
            .get("n");
    set_work_estimate(state, job, n.max(1), true).await?;
    Ok(Step::Next)
}

/// legacy validate-required-work-estimate!: entities × 2 (validate and
/// revalidate), +5%.
async fn estimate_required(state: &AppState, job: &Job) -> Result<Step> {
    let (attrs, attr) = load_attr(state, job).await?;
    let id_attr = attrs.id_attr_of(&attr.etype).ok_or_else(|| {
        InstantError::validation_failed(
            "attr",
            format!("{} has no id attribute", attr.etype),
            json!([]),
        )
    })?;
    let n: i64 =
        sqlx::query("SELECT count(*) AS n FROM triples WHERE app_id = $1 AND attr_id = $2")
            .bind(job.app_id)
            .bind(id_attr.id)
            .fetch_one(&state.pool)
            .await?
            .get("n");
    let estimate = (1.05 * (n.max(1) * 2) as f64) as i64;
    set_work_estimate(state, job, estimate, false).await?;
    Ok(Step::Next)
}

// ---------------------------------------------------------------------------
// batched scans over the attr's triples
//
// Every attr-triple batch query takes ($1 app_id, $2 attr_id, $3 cursor t,
// $4 cursor e, $5 cursor md5, $6 limit, ...); the cursor is a strict
// row-value lower bound in (created_at, entity_id, value_md5) order, which
// the (app_id, attr_id, created_at) index serves.

const CURSOR_WHERE: &str = "($3::bigint IS NULL OR (t.created_at, t.entity_id, t.value_md5) > ($3::bigint, $4::uuid, $5::text))";
const CURSOR_ORDER: &str = "t.created_at, t.entity_id, t.value_md5";
const END_CURSOR: &str = "(SELECT json_build_object('t', created_at, 'e', entity_id, 'md5', value_md5)
                            FROM next_batch ORDER BY created_at DESC, entity_id DESC, value_md5 DESC LIMIT 1)";

fn bind_cursor<'q>(
    q: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    job: &'q Job,
    cursor: &'q Cursor,
    limit: i64,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    q.bind(job.app_id)
        .bind(job.attr_id)
        .bind(cursor.t)
        .bind(cursor.e)
        .bind(cursor.md5.as_deref())
        .bind(limit)
}

/// Fold a batch result: progress, then repeat from the end cursor or move on.
async fn after_batch(
    state: &AppState,
    job: &Job,
    work_completed: i64,
    end_cursor: Option<Value>,
) -> Result<Step> {
    add_work_completed(state, job, work_completed).await?;
    Ok(match end_cursor {
        Some(c) => Step::Repeat(Some(json!({ "after": c }))),
        None => Step::Next,
    })
}

/// check-data-type--validate: does any triple in the next batch fail the
/// type check? (A larger batch than the writes: it is only a read.)
async fn check_data_type_validate(state: &AppState, job: &Job) -> Result<Step> {
    let cdt = job.checked_data_type.as_deref().ok_or_else(|| {
        InstantError::validation_failed("indexing-job", "missing checked-data-type", json!([]))
    })?;
    let cursor = job.cursor();
    let sql = format!(
        "WITH next_batch AS (
           SELECT t.created_at, t.entity_id, t.value_md5, t.checked_data_type, t.value
             FROM triples t
            WHERE t.app_id = $1 AND t.attr_id = $2 AND {CURSOR_WHERE}
            ORDER BY {CURSOR_ORDER} LIMIT $6),
         bad AS (
           SELECT 1 FROM next_batch b
            WHERE b.checked_data_type IS DISTINCT FROM $7::checked_data_type
              AND NOT triples_valid_value($7::checked_data_type, b.value))
         SELECT (SELECT count(*) FROM next_batch) AS work_completed,
                EXISTS (SELECT 1 FROM bad) AS has_invalid,
                {END_CURSOR} AS end_cursor"
    );
    let row = bind_cursor(sqlx::query(&sql), job, &cursor, batch_size(state) * 10)
        .bind(cdt)
        .fetch_one(&state.pool)
        .await?;
    let work_completed: i64 = row.get("work_completed");
    add_work_completed(state, job, work_completed).await?;
    if row.get::<bool, _>("has_invalid") {
        // undo update-attr-start if we are past it (no-op in `validate`)
        update_attr(
            state,
            job,
            "checking_data_type = false, checked_data_type = NULL",
            "AND checked_data_type = $3::checked_data_type AND checking_data_type = true",
            Some(cdt),
        )
        .await?;
        return Ok(Step::Error(INVALID_TRIPLE_ERROR));
    }
    Ok(match row.get::<Option<Value>, _>("end_cursor") {
        Some(c) => Step::Repeat(Some(json!({ "after": c }))),
        None => Step::Next,
    })
}

/// check-data-type / remove-data-type --update-triples: stamp the next batch
/// with the type (or clear it).
async fn set_checked_data_type_batch(
    state: &AppState,
    job: &Job,
    cdt: Option<&str>,
) -> Result<Step> {
    let cursor = job.cursor();
    let sql = format!(
        "WITH next_batch AS (
           SELECT t.app_id, t.entity_id, t.attr_id, t.value_md5, t.created_at
             FROM triples t
            WHERE t.app_id = $1 AND t.attr_id = $2 AND {CURSOR_WHERE}
            ORDER BY {CURSOR_ORDER} LIMIT $6 FOR UPDATE),
         updates AS (
           UPDATE triples u SET checked_data_type = $7::checked_data_type
             FROM next_batch n
            WHERE n.app_id = u.app_id AND n.entity_id = u.entity_id
              AND n.attr_id = u.attr_id AND n.value_md5 = u.value_md5
              AND u.checked_data_type IS DISTINCT FROM $7::checked_data_type
            RETURNING 1)
         SELECT (SELECT count(*) FROM updates) AS updated,
                (SELECT count(*) FROM next_batch) AS work_completed,
                {END_CURSOR} AS end_cursor"
    );
    let row = bind_cursor(sqlx::query(&sql), job, &cursor, batch_size(state))
        .bind(cdt)
        .fetch_one(&state.pool)
        .await?;
    after_batch(state, job, row.get("work_completed"), row.get("end_cursor")).await
}

/// index / unique --update-triples: set `ave` / `av` on the next batch of
/// unflagged triples. The partial unique index (`av`) and the size check
/// constraint surface bad data as errors here.
async fn set_flag_batch(state: &AppState, job: &Job, flag: &str) -> Result<Step> {
    let cursor = job.cursor();
    let sql = format!(
        "WITH next_batch AS (
           SELECT t.app_id, t.entity_id, t.attr_id, t.value_md5, t.created_at
             FROM triples t
            WHERE t.app_id = $1 AND t.attr_id = $2 AND NOT t.{flag} AND {CURSOR_WHERE}
            ORDER BY {CURSOR_ORDER} LIMIT $6 FOR UPDATE),
         updates AS (
           UPDATE triples u SET {flag} = true
             FROM next_batch n
            WHERE n.app_id = u.app_id AND n.entity_id = u.entity_id
              AND n.attr_id = u.attr_id AND n.value_md5 = u.value_md5
            RETURNING 1)
         SELECT (SELECT count(*) FROM updates) AS updated,
                (SELECT count(*) FROM next_batch) AS work_completed,
                {END_CURSOR} AS end_cursor"
    );
    let row = bind_cursor(sqlx::query(&sql), job, &cursor, batch_size(state))
        .fetch_one(&state.pool)
        .await?;
    after_batch(state, job, row.get("work_completed"), row.get("end_cursor")).await
}

/// remove-index / remove-unique --update-triples: clear the flag on a batch;
/// done when a batch comes up short.
async fn clear_flag_batch(state: &AppState, job: &Job, flag: &str) -> Result<Step> {
    let batch = batch_size(state);
    let res = sqlx::query(&format!(
        "UPDATE triples SET {flag} = false
          WHERE ctid IN (SELECT ctid FROM triples
                          WHERE app_id = $1 AND attr_id = $2 AND {flag}
                          LIMIT $3 FOR UPDATE)"
    ))
    .bind(job.app_id)
    .bind(job.attr_id)
    .bind(batch)
    .execute(&state.pool)
    .await?;
    let n = res.rows_affected() as i64;
    add_work_completed(state, job, n).await?;
    Ok(if n >= batch {
        Step::Repeat(None)
    } else {
        Step::Next
    })
}

/// index--insert-nulls: every entity of the etype gets a `null` triple for
/// the newly indexed blob attr, a batch of entities at a time.
async fn insert_nulls_batch(state: &AppState, job: &Job) -> Result<Step> {
    let (attrs, attr) = load_attr(state, job).await?;
    if attr.value_type != ValueType::Blob {
        return Ok(Step::Next);
    }
    let id_attr = attrs.id_attr_of(&attr.etype).ok_or_else(|| {
        InstantError::validation_failed(
            "attr",
            format!("{} has no id attribute", attr.etype),
            json!([]),
        )
    })?;
    let cursor = job.cursor();
    // twice the batch: we may insert fewer than we scan
    let limit = batch_size(state) * 2;
    let row = sqlx::query(
        "WITH attr AS (
           SELECT cardinality, value_type, is_unique, is_indexed, checked_data_type
             FROM attrs WHERE app_id = $1 AND id = $2),
         ids_to_check AS (
           SELECT t.app_id, t.entity_id, t.created_at
             FROM triples t
            WHERE t.app_id = $1 AND t.attr_id = $3
              AND ($4::bigint IS NULL OR (t.created_at, t.entity_id) > ($4::bigint, $5::uuid))
            ORDER BY t.created_at, t.entity_id LIMIT $6 FOR UPDATE),
         inserts AS (
           INSERT INTO triples (app_id, entity_id, attr_id, value, value_md5,
                                ea, eav, av, ave, vae, checked_data_type)
           SELECT i.app_id, i.entity_id, $2, 'null'::jsonb, $7,
                  (SELECT cardinality = 'one' FROM attr),
                  (SELECT value_type = 'ref' FROM attr),
                  (SELECT is_unique FROM attr),
                  (SELECT is_indexed FROM attr),
                  (SELECT value_type = 'ref' FROM attr),
                  (SELECT checked_data_type FROM attr)
             FROM ids_to_check i
            WHERE NOT EXISTS (SELECT 1 FROM triples a
                               WHERE a.app_id = $1 AND a.attr_id = $2 AND a.entity_id = i.entity_id)
           RETURNING 1)
         SELECT (SELECT count(*) FROM inserts) AS updated,
                (SELECT count(*) FROM ids_to_check) AS work_completed,
                (SELECT json_build_object('t', created_at, 'e', entity_id)
                   FROM ids_to_check ORDER BY created_at DESC, entity_id DESC LIMIT 1) AS end_cursor",
    )
    .bind(job.app_id)
    .bind(job.attr_id)
    .bind(id_attr.id)
    .bind(cursor.t)
    .bind(cursor.e)
    .bind(limit)
    .bind(instant_core::triple::JSON_NULL_MD5)
    .fetch_one(&state.pool)
    .await?;
    after_batch(state, job, row.get("work_completed"), row.get("end_cursor")).await
}

/// required--validate: entities of the etype (a batch at a time) that lack a
/// non-null value for the attr. Any → the attr is un-required and the job
/// errors with the legacy `missing-required-error` data.
async fn required_validate(state: &AppState, job: &Job) -> Result<Step> {
    let (attrs, attr) = load_attr(state, job).await?;
    let Some(id_attr) = attrs.id_attr_of(&attr.etype) else {
        let _ = update_attr(state, job, "is_required = false", "", None).await?;
        return Ok(Step::Failed(JobError {
            error: MISSING_REQUIRED_ERROR,
            error_data: Some(json!({"attr-id": job.attr_id, "etype": attr.etype})),
            ..Default::default()
        }));
    };
    let cursor = job.cursor();
    let limit = batch_size(state) * 10;
    let row = sqlx::query(
        "WITH ids_to_check AS (
           SELECT t.entity_id, t.created_at
             FROM triples t
            WHERE t.app_id = $1 AND t.attr_id = $3
              AND ($4::bigint IS NULL OR (t.created_at, t.entity_id) > ($4::bigint, $5::uuid))
            ORDER BY t.created_at, t.entity_id LIMIT $6),
         bad AS (
           SELECT i.entity_id FROM ids_to_check i
            WHERE NOT EXISTS (SELECT 1 FROM triples a
                               WHERE a.app_id = $1 AND a.attr_id = $2 AND a.entity_id = i.entity_id
                                 AND a.value <> 'null'::jsonb))
         SELECT (SELECT json_agg(s.entity_id) FROM (SELECT entity_id FROM bad LIMIT 10) s) AS invalid_ids,
                (SELECT count(*) FROM bad) AS invalid_count,
                (SELECT count(*) FROM ids_to_check) AS work_completed,
                (SELECT json_build_object('t', created_at, 'e', entity_id)
                   FROM ids_to_check ORDER BY created_at DESC, entity_id DESC LIMIT 1) AS end_cursor",
    )
    .bind(job.app_id)
    .bind(job.attr_id)
    .bind(id_attr.id)
    .bind(cursor.t)
    .bind(cursor.e)
    .bind(limit)
    .fetch_one(&state.pool)
    .await?;
    let work_completed: i64 = row.get("work_completed");
    add_work_completed(state, job, work_completed).await?;
    let invalid_ids: Option<Value> = row.get("invalid_ids");
    let invalid_count: i64 = row.get("invalid_count");
    if let Some(ids) = invalid_ids.filter(|v| v.as_array().is_some_and(|a| !a.is_empty())) {
        let _ = update_attr(state, job, "is_required = false", "", None).await?;
        return Ok(Step::Failed(JobError {
            error: MISSING_REQUIRED_ERROR,
            error_data: Some(json!({
                "count": invalid_count,
                "etype": attr.etype,
                "label": attr.label,
                "entity-ids": ids,
            })),
            ..Default::default()
        }));
    }
    Ok(match row.get::<Option<Value>, _>("end_cursor") {
        Some(c) => Step::Repeat(Some(json!({ "after": c }))),
        None => Step::Next,
    })
}

// ---------------------------------------------------------------------------
// failure handling for the flag rewrites

/// legacy abort-index!: un-index the attr and drop the flags set so far.
async fn abort_index(state: &AppState, job: &Job) -> Result<()> {
    update_attr(
        state,
        job,
        "indexing = false, is_indexed = false",
        "AND is_indexed = true AND indexing = true",
        None,
    )
    .await?;
    sqlx::query("UPDATE triples SET ave = false WHERE app_id = $1 AND attr_id = $2 AND ave")
        .bind(job.app_id)
        .bind(job.attr_id)
        .execute(&state.pool)
        .await?;
    Ok(())
}

async fn abort_unique(state: &AppState, job: &Job) -> Result<()> {
    update_attr(
        state,
        job,
        "setting_unique = false, is_unique = false",
        "AND is_unique = true AND setting_unique = true",
        None,
    )
    .await?;
    sqlx::query("UPDATE triples SET av = false WHERE app_id = $1 AND attr_id = $2 AND av")
        .bind(job.app_id)
        .bind(job.attr_id)
        .execute(&state.pool)
        .await?;
    Ok(())
}

/// legacy mark-error-from-ex-info!: classify a failed rewrite. A unique
/// violation → `triple-not-unique-error` with the colliding value; the
/// 1024-byte size check → `triple-too-large-error` with the offending
/// entity; anything else is unexpected.
async fn rewrite_failure(state: &AppState, job: &Job, e: &InstantError) -> JobError {
    let msg = e.message.as_str();
    if msg.contains("indexed_values_are_constrained")
        || e.error_type == "validation-failed" && msg.contains("too large")
    {
        let entity_id = sqlx::query(
            "SELECT entity_id FROM triples
              WHERE app_id = $1 AND attr_id = $2 AND pg_column_size(value) > 1024
              ORDER BY created_at, entity_id LIMIT 1",
        )
        .bind(job.app_id)
        .bind(job.attr_id)
        .fetch_optional(&state.pool)
        .await
        .ok()
        .flatten()
        .map(|r| r.get::<Uuid, _>("entity_id"));
        return JobError {
            error: TRIPLE_TOO_LARGE_ERROR,
            invalid_entity_id: entity_id,
            ..Default::default()
        };
    }
    if e.error_type == "record-not-unique"
        || msg.contains("av_index")
        || msg.contains("duplicate key")
    {
        let value = duplicate_value(state, job).await.ok().flatten();
        return JobError {
            error: TRIPLE_NOT_UNIQUE_ERROR,
            invalid_unique_value: value,
            ..Default::default()
        };
    }
    JobError {
        error: UNEXPECTED_ERROR,
        error_detail: Some(e.message.clone()),
        ..Default::default()
    }
}

/// A value held by more than one triple of the attr (nulls never collide:
/// av_index ignores them).
async fn duplicate_value(state: &AppState, job: &Job) -> Result<Option<Value>> {
    let row = sqlx::query(
        "SELECT value FROM triples
          WHERE app_id = $1 AND attr_id = $2 AND value <> 'null'::jsonb
          GROUP BY value HAVING count(*) > 1
          ORDER BY min(created_at) LIMIT 1",
    )
    .bind(job.app_id)
    .bind(job.attr_id)
    .fetch_optional(&state.pool)
    .await?;
    Ok(row.map(|r| r.get::<Value, _>("value")))
}

// ---------------------------------------------------------------------------
// sweep (legacy grab-forgotten-jobs!): jobs nobody is driving — created by
// a node that died, released and never re-grabbed, or blocked earlier by a
// serial-key conflict — are picked up here on every node.

pub async fn sweep_loop(state: Arc<AppState>) {
    let every = std::time::Duration::from_secs(state.cfg.indexing_sweep_secs.max(1));
    // first pass soon after boot, to resume anything a previous process left
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    loop {
        if let Err(e) = sweep_once(&state).await {
            tracing::warn!(error = %e, "indexing job sweep failed");
        }
        tokio::time::sleep(every).await;
    }
}

pub async fn sweep_once(state: &Arc<AppState>) -> Result<()> {
    // A worker that vanished mid-step leaves worker_id set forever (legacy
    // only warns about these). Every step bumps updated_at, so a job with no
    // progress for `indexing_stale_secs` is reclaimed.
    let stale = state.cfg.indexing_stale_secs.max(1) as f64;
    let reclaimed = sqlx::query(
        "UPDATE indexing_jobs SET worker_id = NULL
          WHERE job_status = 'processing' AND worker_id IS NOT NULL
            AND updated_at < now() - make_interval(secs => $1)
          RETURNING id",
    )
    .bind(stale)
    .fetch_all(&state.pool)
    .await?;
    for r in &reclaimed {
        tracing::warn!(job = %r.get::<Uuid, _>("id"), "reclaimed a stale indexing job");
    }
    let rows = sqlx::query(
        "SELECT id FROM indexing_jobs
          WHERE worker_id IS NULL AND job_status IN ('waiting', 'processing')
            AND job_type = ANY($1)
            AND (job_dependency IS NULL
                 OR (SELECT d.job_status FROM indexing_jobs d WHERE d.id = indexing_jobs.job_dependency)
                    = 'completed')
          ORDER BY created_at, id LIMIT 100",
    )
    .bind(JOB_TYPES.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    .fetch_all(&state.pool)
    .await?;
    if rows.is_empty() {
        return Ok(());
    }
    tracing::info!(count = rows.len(), "picking up unowned indexing jobs");
    let sem = Arc::new(tokio::sync::Semaphore::new(GROUP_CONCURRENCY));
    for row in rows {
        let job_id: Uuid = row.get("id");
        let state = state.clone();
        let sem = sem.clone();
        tokio::spawn(async move {
            let _permit = sem.acquire().await;
            // no conflict waiting here: the next sweep retries
            process_job(&state, job_id, 0).await;
        });
    }
    Ok(())
}
