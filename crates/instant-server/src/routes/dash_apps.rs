//! The rest of the `/dash/*` surface `instant-cli` uses (issue #29 item 6):
//! app management (`app`, `info`, `claim`, `init`), OAuth configuration
//! (`auth client|origin`), email templates (`auth email`) and orgs. Port of
//! the corresponding handlers in LEGACY dash/routes.clj, dash/ephemeral_app.clj
//! and the models they call.
//!
//! Auth follows legacy: routes without an `:app_id` (`/dash`, `/dash/me`,
//! `POST /dash/apps`, `/dash/orgs/*`, `claim`) take a dashboard refresh
//! token only (`req->auth-user!`); per-app routes accept the app's admin
//! token or a refresh token of a member with the required role
//! (`req->app-accepting-superadmin-or-ref-token!`).

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::Response;
use instant_core::error::{InstantError, Result};
use instant_core::system_catalog as sc;
use serde_json::{json, Map, Value};
use sqlx::Row;
use uuid::Uuid;

use crate::routes::dash::{
    coerce_non_blank_str, dash_authed_with_role, dash_user, param_malformed, param_missing,
    parse_body, DashRole,
};
use crate::routes::runtime::json_or_err;
use crate::routes::superadmin::Scope;
use crate::service;
use crate::state::AppState;

/// legacy `ephemeral-creator-email` (dash/ephemeral_app.clj:25-27) for the
/// prod / dev environments, both seeded by migration 62, and the getadb
/// creator (dash/get_a_db.clj:16, migration 105). Apps owned by one of them
/// are claimable.
const EPHEMERAL_CREATOR_EMAILS: [&str; 2] = [
    "hello+ephemeralapps@instantdb.com",
    "hello+ephemeralappsdev@instantdb.com",
];
pub(crate) const GET_A_DB_CREATOR_EMAIL: &str = "hello+getadbapps@instantdb.com";
/// legacy `expiration-days`
const EPHEMERAL_EXPIRATION_DAYS: i64 = 14;

pub(crate) fn ts_naive(t: Option<chrono::NaiveDateTime>) -> Value {
    match t {
        Some(t) => json!(t.format("%Y-%m-%dT%H:%M:%SZ").to_string()),
        None => Value::Null,
    }
}

pub(crate) fn ts_tz(t: Option<chrono::DateTime<chrono::Utc>>) -> Value {
    match t {
        Some(t) => json!(t.format("%Y-%m-%dT%H:%M:%SZ").to_string()),
        None => Value::Null,
    }
}

fn ts_ms(ms: Option<i64>) -> Value {
    use chrono::TimeZone;
    match ms.and_then(|ms| chrono::Utc.timestamp_millis_opt(ms).single()) {
        Some(t) => json!(t.format("%Y-%m-%dT%H:%M:%SZ").to_string()),
        None => Value::Null,
    }
}

/// `ex/get-param!` with `uuid-util/coerce`.
pub(crate) fn body_uuid(body: &Value, key: &str) -> Result<Uuid> {
    let v = body
        .get(key)
        .filter(|v| !v.is_null())
        .ok_or_else(|| param_missing(&["body", key]))?;
    v.as_str()
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| param_malformed(&["body", key], v.clone()))
}

/// `ex/get-param!` with `string-util/coerce-non-blank-str`.
pub(crate) fn body_str(body: &Value, key: &str) -> Result<String> {
    let v = body
        .get(key)
        .filter(|v| !v.is_null())
        .ok_or_else(|| param_missing(&["body", key]))?;
    coerce_non_blank_str(v).ok_or_else(|| param_malformed(&["body", key], v.clone()))
}

/// `ex/get-optional-param!` with `string-util/coerce-non-blank-str`: absent
/// or null is nothing, present but blank is malformed.
fn body_opt_str(body: &Value, key: &str) -> Result<Option<String>> {
    match body.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => coerce_non_blank_str(v)
            .map(Some)
            .ok_or_else(|| param_malformed(&["body", key], v.clone())),
    }
}

pub(crate) fn path_uuid(raw: &str, name: &str) -> Result<Uuid> {
    Uuid::parse_str(raw).map_err(|_| param_malformed(&["params", name], json!(raw)))
}

pub(crate) fn record_not_found(record_type: &str, hint: Value) -> InstantError {
    let mut h = hint;
    if let Some(m) = h.as_object_mut() {
        m.insert("record-type".into(), json!(record_type));
    }
    InstantError::new(
        "record-not-found",
        400,
        format!("Record not found: {record_type}"),
        Some(h),
    )
}

/// `ex/assert-valid!` with plain string errors: the message stays bare.
fn assert_valid(data_type: &str, input: Value, errors: Vec<String>) -> Result<()> {
    if errors.is_empty() {
        return Ok(());
    }
    Err(InstantError::new(
        "validation-failed",
        400,
        format!("Validation failed for {data_type}"),
        Some(json!({"data-type": data_type, "input": input, "errors": errors})),
    ))
}

// ---------------------------------------------------------------------------
// apps

/// The `apps` row as legacy returns it (`apps.*`).
pub(crate) async fn app_row(state: &AppState, app_id: Uuid) -> Result<Option<Value>> {
    let row = sqlx::query(
        "SELECT id, creator_id, org_id, title, created_at, status, deletion_marked_at,
                subscription_id, magic_code_expiry_minutes, connection_string
           FROM apps WHERE id = $1",
    )
    .bind(app_id)
    .fetch_optional(&state.pool)
    .await?;
    Ok(row.map(|r| app_row_json(&r)))
}

pub(crate) fn app_row_json(r: &sqlx::postgres::PgRow) -> Value {
    json!({
        "id": r.get::<Uuid, _>("id"),
        "creator_id": r.get::<Option<Uuid>, _>("creator_id"),
        "org_id": r.get::<Option<Uuid>, _>("org_id"),
        "title": r.get::<String, _>("title"),
        "created_at": ts_naive(r.get::<Option<chrono::NaiveDateTime>, _>("created_at")),
        "status": r.get::<String, _>("status"),
        "deletion_marked_at": ts_tz(r.get::<Option<chrono::DateTime<chrono::Utc>>, _>("deletion_marked_at")),
        "subscription_id": r.get::<Option<Uuid>, _>("subscription_id"),
        "magic_code_expiry_minutes": r.get::<Option<i32>, _>("magic_code_expiry_minutes"),
        "connection_string": r.get::<Option<Vec<u8>>, _>("connection_string").map(|b| json!(b)).unwrap_or(Value::Null),
    })
}

/// legacy `app-model/get-by-id!`: live apps only.
pub(crate) async fn live_app_row(state: &AppState, app_id: Uuid) -> Result<Value> {
    match app_row(state, app_id).await? {
        Some(app)
            if app
                .get("deletion_marked_at")
                .map(|v| v.is_null())
                .unwrap_or(true) =>
        {
            Ok(app)
        }
        _ => Err(record_not_found("app", json!({"args": [{"id": app_id}]}))),
    }
}

/// The app rows of legacy `make-apps-q` (model/app.clj:193-271) for a user:
/// `apps.*` plus the admin token, rules, members, invites and the caller's
/// role. Plans and webhooks don't exist here (`pro` false, `webhooks` null).
async fn apps_for_user(state: &AppState, user_id: Uuid) -> Result<Vec<Value>> {
    let rows = sqlx::query(
        "SELECT a.id, a.creator_id, a.org_id, a.title, a.created_at, a.status,
                a.deletion_marked_at, a.subscription_id, a.magic_code_expiry_minutes,
                a.connection_string,
                at.token AS admin_token, r.code AS rules, r.version AS rules_version,
                CASE WHEN a.creator_id = $1 THEN 'owner'
                     ELSE (SELECT m.member_role FROM app_members m WHERE m.app_id = a.id AND m.user_id = $1)
                END AS user_app_role,
                coalesce((SELECT json_agg(json_build_object('id', m.id, 'email', u.email, 'role', m.member_role))
                   FROM app_members m JOIN instant_users u ON u.id = m.user_id
                  WHERE m.app_id = a.id), '[]'::json) AS members,
                coalesce((SELECT json_agg(json_build_object('id', i.id, 'email', i.invitee_email, 'role', i.invitee_role,
                                                   'status', i.status, 'sent_at', i.sent_at,
                                                   'expired', i.sent_at < now() - interval '3 days'))
                   FROM app_member_invites i WHERE i.app_id = a.id), '[]'::json) AS invites
           FROM apps a
           LEFT JOIN app_admin_tokens at ON at.app_id = a.id
           LEFT JOIN rules r ON r.app_id = a.id
          WHERE a.deletion_marked_at IS NULL AND a.org_id IS NULL
            AND (a.creator_id = $1
                 OR EXISTS (SELECT 1 FROM app_members m WHERE m.app_id = a.id AND m.user_id = $1
                             AND ($2 OR m.created_at < $3 OR m.member_role = 'owner'
                                  -- all-for-user-q (app.clj:290-296) admits Pro (2) only
                                  OR EXISTS (SELECT 1 FROM instant_subscriptions sub
                                              WHERE sub.id = a.subscription_id
                                                AND sub.subscription_type_id = 2))))
          ORDER BY a.created_at, a.id",
    )
    .bind(user_id)
    .bind(state.cfg.paid_features_free)
    .bind(free_teams_cutoff())
    .fetch_all(&state.pool)
    .await?;
    Ok(rows
        .iter()
        .map(|r| {
            let mut app = app_row_json(r);
            if let Some(m) = app.as_object_mut() {
                m.insert(
                    "admin_token".into(),
                    json!(r.get::<Option<Uuid>, _>("admin_token")),
                );
                m.insert(
                    "rules".into(),
                    r.get::<Option<Value>, _>("rules").unwrap_or(Value::Null),
                );
                m.insert(
                    "rules_version".into(),
                    json!(r.get::<Option<i32>, _>("rules_version")),
                );
                m.insert("org".into(), Value::Null);
                m.insert("pro".into(), json!(false));
                m.insert(
                    "user_app_role".into(),
                    json!(r.get::<Option<String>, _>("user_app_role")),
                );
                m.insert(
                    "members".into(),
                    r.get::<Option<Value>, _>("members").unwrap_or(json!([])),
                );
                m.insert(
                    "invites".into(),
                    r.get::<Option<Value>, _>("invites").unwrap_or(json!([])),
                );
                m.insert("webhooks".into(), json!([]));
                // with-effective-status: no sunset stage here
                m.insert(
                    "effective_status".into(),
                    json!(r.get::<String, _>("status")),
                );
            }
            app
        })
        .collect())
}

/// GET /dash/me
pub async fn me_get(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let r = async {
        let user = dash_user(&state, &headers).await?;
        Ok(json!({"user": user.to_json()}))
    }
    .await;
    json_or_err(r)
}

/// GET /dash — legacy dash-get (dash/routes.clj:533-557); `instant-cli app
/// list` / `app delete` read `apps[].{id,title,user_app_role}`.
pub async fn dash_get(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let r = async {
        let user = dash_user(&state, &headers).await?;
        let apps = apps_for_user(&state, user.id).await?;
        let orgs = orgs_for_user(&state, user.id).await?;
        let profile: Option<Value> = sqlx::query(
            "SELECT id, meta, created_at FROM instant_profiles WHERE id = $1",
        )
        .bind(user.id)
        .fetch_optional(&state.pool)
        .await?
        .map(|r| {
            json!({
                "id": r.get::<Uuid, _>("id"),
                "meta": r.get::<Value, _>("meta"),
                "created_at": ts_naive(r.get::<Option<chrono::NaiveDateTime>, _>("created_at")),
            })
        });
        let invites: Vec<Value> = sqlx::query(
            "SELECT i.id, i.app_id, a.title AS app_title, i.invitee_role, i.status, i.sent_at,
                    u.email AS inviter_email
               FROM app_member_invites i
               JOIN apps a ON a.id = i.app_id
               JOIN instant_users u ON u.id = i.inviter_id
              WHERE i.invitee_email = $1 AND i.status = 'pending'",
        )
        .bind(&user.email)
        .fetch_all(&state.pool)
        .await?
        .iter()
        .map(|r| {
            json!({
                "id": r.get::<Uuid, _>("id"),
                "app_id": r.get::<Uuid, _>("app_id"),
                "app_title": r.get::<String, _>("app_title"),
                "invitee_role": r.get::<String, _>("invitee_role"),
                "status": r.get::<String, _>("status"),
                "sent_at": ts_naive(r.get::<Option<chrono::NaiveDateTime>, _>("sent_at")),
                "inviter_email": r.get::<String, _>("inviter_email"),
            })
        })
        .collect();
        Ok(json!({
            "apps": apps,
            "superuser": false,
            "orgs": orgs,
            "profile": profile,
            "invites": invites,
            "user": {"id": user.id, "email": user.email},
            "sunset": {
                "stage": "none",
                "app-creation-allowed": true,
                "billing-closed": false,
                "paid-features-free": false,
            },
        }))
    }
    .await;
    json_or_err(r)
}

/// Create an app with its admin token and return `apps.* + admin-token`
/// (legacy `app-model/create!`, model/app.clj:66-87).
pub(crate) async fn create_app(
    state: &AppState,
    id: Uuid,
    title: &str,
    creator_id: Option<Uuid>,
    org_id: Option<Uuid>,
    admin_token: Uuid,
) -> Result<Value> {
    let mut dbtx = state.pool.begin().await?;
    let inserted = sqlx::query(
        "INSERT INTO apps (id, title, creator_id, org_id) VALUES ($1, $2, $3, $4)
         RETURNING id, creator_id, org_id, title, created_at, status, deletion_marked_at,
                   subscription_id, magic_code_expiry_minutes, connection_string",
    )
    .bind(id)
    .bind(title)
    .bind(creator_id)
    .bind(org_id)
    .fetch_one(&mut *dbtx)
    .await
    .map_err(|e| match &e {
        sqlx::Error::Database(db) if db.code().as_deref() == Some("23505") => InstantError::new(
            "record-not-unique",
            400,
            "Record not unique: app",
            Some(json!({"record-type": "app"})),
        ),
        _ => InstantError::from(e),
    })?;
    sqlx::query("INSERT INTO app_admin_tokens (app_id, token) VALUES ($1, $2)")
        .bind(id)
        .bind(admin_token)
        .execute(&mut *dbtx)
        .await?;
    dbtx.commit().await?;
    let mut app = app_row_json(&inserted);
    if let Some(m) = app.as_object_mut() {
        m.insert("admin-token".into(), json!(admin_token));
    }
    Ok(app)
}

/// Optional `rules.code` on app creation: validated, then stored.
fn rules_code_of(body: &Value) -> Result<Option<Value>> {
    match body.get("rules").and_then(|r| r.get("code")) {
        None | Some(Value::Null) => Ok(None),
        Some(code) => {
            let errors = instant_core::perms::validation_errors(code);
            if !errors.is_empty() {
                let msgs: Vec<&str> = errors
                    .iter()
                    .filter_map(|e| e.get("message").and_then(|m| m.as_str()))
                    .collect();
                return Err(InstantError::new(
                    "validation-failed",
                    400,
                    format!("Validation failed for rule: {}", msgs.join(", ")),
                    Some(json!({"data-type": "rule", "input": code, "errors": errors})),
                ));
            }
            Ok(Some(code.clone()))
        }
    }
}

async fn put_rules(state: &AppState, app_id: Uuid, code: &Value) -> Result<()> {
    sqlx::query(
        "INSERT INTO rules (app_id, code) VALUES ($1, $2)
         ON CONFLICT (app_id) DO UPDATE SET code = excluded.code, version = rules.version + 1
         WHERE rules.code IS DISTINCT FROM excluded.code",
    )
    .bind(app_id)
    .bind(code)
    .execute(&state.pool)
    .await?;
    Ok(())
}

/// Optional `schema` on app creation: planned with type checks and applied
/// synchronously (schema-model/plan! + apply-plan!).
async fn apply_initial_schema(state: &Arc<AppState>, app_id: Uuid, body: &Value) -> Result<()> {
    let Some(schema) = body.get("schema").filter(|s| !s.is_null()) else {
        return Ok(());
    };
    let plan_body =
        json!({"schema": schema, "check_types": true, "supports_background_updates": false});
    crate::routes::dash::plan_and_apply(state, app_id, &plan_body).await?;
    Ok(())
}

/// POST /dash/apps — legacy apps-post (dash/routes.clj:577-628). Body:
/// `{title, id, admin_token, org_id?, schema?, rules?: {code}}`.
pub async fn apps_post(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let user = dash_user(&state, &headers).await?;
        let body = parse_body(&body)?;
        let title = body_str(&body, "title")?;
        let id = body_uuid(&body, "id")?;
        let token = body_uuid(&body, "admin_token")?;
        let org_id = match body.get("org_id") {
            None | Some(Value::Null) => None,
            Some(v) => Some(
                v.as_str()
                    .and_then(|s| Uuid::parse_str(s).ok())
                    .ok_or_else(|| param_malformed(&["body", "org_id"], v.clone()))?,
            ),
        };
        let rules_code = rules_code_of(&body)?;
        let (creator_id, org_id) = match org_id {
            Some(org_id) => {
                org_role_for_user(&state, org_id, user.id, DashRole::Collaborator).await?;
                (None, Some(org_id))
            }
            None => (Some(user.id), None),
        };
        let app = create_app(&state, id, &title, creator_id, org_id, token).await?;
        if let Some(code) = &rules_code {
            put_rules(&state, id, code).await?;
        }
        apply_initial_schema(&state, id, &body).await?;
        Ok(json!({"app": app}))
    }
    .await;
    json_or_err(r)
}

/// GET /dash/apps/:app_id — legacy apps-get: the raw `apps` row.
pub async fn apps_get(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let app = dash_authed_with_role(
            &state,
            &headers,
            &app_id,
            DashRole::Collaborator,
            Scope::AppsRead,
        )
        .await?;
        let row = live_app_row(&state, app.id).await?;
        Ok(json!({"app": row}))
    }
    .await;
    json_or_err(r)
}

/// DELETE /dash/apps/:app_id — legacy apps-delete (dash/routes.clj:635-649):
/// dashboard-user only, admin role, and a personal app needs its owner.
pub async fn apps_delete(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let user = dash_user(&state, &headers).await?;
        let app_id = path_uuid(&app_id, "app_id")?;
        let row = live_app_row(&state, app_id).await?;
        let access = app_role_for_user(&state, &row, user.id).await?;
        assert_app_access(DashRole::Admin, access)?;
        let creator: Option<Uuid> = row
            .get("creator_id")
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok());
        if let Some(creator) = creator {
            if creator != user.id {
                return Err(InstantError::new(
                    "permission-denied",
                    400,
                    "Permission denied: not allowed-member-role?",
                    Some(json!({"input": "owner", "expected": "allowed-member-role?"})),
                ));
            }
        }
        sqlx::query("UPDATE apps SET deletion_marked_at = NOW() WHERE id = $1")
            .bind(app_id)
            .execute(&state.pool)
            .await?;
        service::invalidate_attrs(&state, app_id);
        Ok(json!({"ok": true}))
    }
    .await;
    json_or_err(r)
}

/// legacy `config/free-teams-cutoff`: 2026-03-01 in Etc/GMT+12 (UTC-12);
/// members created before it are grandfathered into team features.
pub(crate) fn free_teams_cutoff() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc)
}

/// One membership path of legacy `get-app-with-role!`: the role and whether
/// the plan lets it count (owner, grandfathered, or paid features free).
#[derive(Clone, Copy)]
pub(crate) struct RolePath {
    pub role: DashRole,
    pub plan_ok: bool,
}

/// Both membership paths of `get-app-with-role!` (util/roles.clj:75-125).
#[derive(Clone, Copy, Default)]
pub(crate) struct AppAccess {
    pub app: Option<RolePath>,
    pub org: Option<RolePath>,
}

impl AppAccess {
    pub(crate) fn any_role(&self) -> Option<DashRole> {
        match (self.app, self.org) {
            (Some(a), Some(o)) => Some(a.role.max(o.role)),
            (Some(a), None) => Some(a.role),
            (None, Some(o)) => Some(o.role),
            (None, None) => None,
        }
    }
}

/// The caller's access to an app (`get-app-with-role!`): creator is owner,
/// else the `app_members` row, plus the org membership.
pub(crate) async fn app_role_for_user(
    state: &AppState,
    app: &Value,
    user_id: Uuid,
) -> Result<AppAccess> {
    let creator: Option<Uuid> = app
        .get("creator_id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok());
    let app_id: Uuid = app
        .get("id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .unwrap_or_default();
    let cutoff = free_teams_cutoff();
    let free = state.cfg.paid_features_free;
    let org_id: Option<Uuid> = app
        .get("org_id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok());
    // legacy `plan-supports-members?` on the app's latest subscription and on
    // the org's (instant_subscription.clj:132-138, roles.clj:88-103): a Pro
    // (2) or Startup (3) plan admits members whatever their join date
    let app_plan = plan_supports_members(
        sqlx::query_scalar::<_, Option<i32>>(
            "SELECT subscription_type_id FROM instant_subscriptions
              WHERE app_id = $1 ORDER BY created_at DESC LIMIT 1",
        )
        .bind(app_id)
        .fetch_optional(&state.pool)
        .await?
        .flatten(),
    );
    let org_plan = match org_id {
        Some(org_id) => plan_supports_members(
            sqlx::query_scalar::<_, Option<i32>>(
                "SELECT s.subscription_type_id FROM orgs o
                   JOIN instant_subscriptions s ON s.id = o.subscription_id
                  WHERE o.id = $1",
            )
            .bind(org_id)
            .fetch_optional(&state.pool)
            .await?
            .flatten(),
        ),
        None => false,
    };
    let mut access = AppAccess::default();
    if creator == Some(user_id) {
        access.app = Some(RolePath {
            role: DashRole::Owner,
            plan_ok: true,
        });
    } else if let Some(r) = sqlx::query(
        "SELECT member_role, created_at FROM app_members WHERE app_id = $1 AND user_id = $2",
    )
    .bind(app_id)
    .bind(user_id)
    .fetch_optional(&state.pool)
    .await?
    {
        if let Some(role) = DashRole::parse(&r.get::<String, _>("member_role")) {
            let created: chrono::DateTime<chrono::Utc> = r.get("created_at");
            access.app = Some(RolePath {
                role,
                plan_ok: role == DashRole::Owner
                    || created < cutoff
                    || free
                    || app_plan
                    || org_plan,
            });
        }
    }
    if let Some(org_id) = org_id {
        if let Some(r) = sqlx::query(
            "SELECT role, created_at FROM org_members WHERE org_id = $1 AND user_id = $2",
        )
        .bind(org_id)
        .bind(user_id)
        .fetch_optional(&state.pool)
        .await?
        {
            if let Some(role) = DashRole::parse(&r.get::<String, _>("role")) {
                let created: chrono::DateTime<chrono::Utc> = r.get("created_at");
                access.org = Some(RolePath {
                    role,
                    plan_ok: role == DashRole::Owner || created < cutoff || free || org_plan,
                });
            }
        }
    }
    Ok(access)
}

/// legacy `plan-supports-members?` (model/instant_subscription.clj:132-138):
/// Pro (2) and Startup (3) subscription types; Free (1) and none don't.
pub(crate) fn plan_supports_members(subscription_type_id: Option<i32>) -> bool {
    matches!(subscription_type_id, Some(2) | Some(3))
}

/// legacy `throw-insufficient-plan!` (util/exception.clj)
pub(crate) fn insufficient_plan() -> InstantError {
    InstantError::new(
        "permission-denied",
        400,
        "The plan for your app or organization does not support multiple members.",
        None,
    )
}

/// The decision of `get-app-with-role!` (util/roles.clj:98-125): a path
/// with a good enough role whose plan allows it wins; else missing role /
/// insufficient role / insufficient plan, in that order. Returns the
/// caller's effective role (the max over both paths).
pub(crate) fn assert_app_access(least: DashRole, access: AppAccess) -> Result<DashRole> {
    let good = |p: Option<RolePath>| p.map(|p| p.role >= least).unwrap_or(false);
    let ok = |p: Option<RolePath>| p.map(|p| p.role >= least && p.plan_ok).unwrap_or(false);
    if ok(access.app) || ok(access.org) {
        return Ok(access.any_role().unwrap_or(least));
    }
    if access.app.is_none() && access.org.is_none() {
        return assert_least_privilege(least, None).map(|_| least);
    }
    if !good(access.app) && !good(access.org) {
        return assert_least_privilege(least, access.any_role()).map(|_| least);
    }
    Err(insufficient_plan())
}

/// legacy `assert-least-privilege!` (util/roles.clj:43-57).
pub(crate) fn assert_least_privilege(least: DashRole, role: Option<DashRole>) -> Result<()> {
    let Some(role) = role else {
        let message = format!("User is missing role {}.", least.as_str());
        return Err(InstantError::new(
            "validation-failed",
            400,
            format!("Validation failed for user-role: {message}"),
            Some(
                json!({"data-type": "user-role", "input": null, "errors": [{"message": message}]}),
            ),
        ));
    };
    if role < least {
        return Err(InstantError::new(
            "permission-denied",
            400,
            "Permission denied: not allowed-member-role?",
            Some(json!({"input": role.as_str(), "expected": "allowed-member-role?"})),
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// ephemeral apps + claim

async fn user_id_by_email(state: &AppState, email: &str) -> Result<Option<Uuid>> {
    Ok(sqlx::query("SELECT id FROM instant_users WHERE email = $1")
        .bind(email)
        .fetch_optional(&state.pool)
        .await?
        .map(|r| r.get("id")))
}

/// The ephemeral creator (seeded by migration 62; created here if a
/// migrated database lacks it).
async fn ephemeral_creator(state: &AppState) -> Result<Uuid> {
    for email in EPHEMERAL_CREATOR_EMAILS {
        if let Some(id) = user_id_by_email(state, email).await? {
            return Ok(id);
        }
    }
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO instant_users (id, email) VALUES ($1, $2) ON CONFLICT DO NOTHING")
        .bind(id)
        .bind(EPHEMERAL_CREATOR_EMAILS[0])
        .execute(&state.pool)
        .await?;
    user_id_by_email(state, EPHEMERAL_CREATOR_EMAILS[0])
        .await?
        .ok_or_else(|| InstantError::internal("could not seed the ephemeral app creator"))
}

/// Is this app owned by one of the claimable-app creators?
async fn claimable(state: &AppState, creator_id: Option<Uuid>) -> Result<bool> {
    let Some(creator_id) = creator_id else {
        return Ok(false);
    };
    let row = sqlx::query("SELECT email FROM instant_users WHERE id = $1")
        .bind(creator_id)
        .fetch_optional(&state.pool)
        .await?;
    let email: Option<String> = row.map(|r| r.get("email"));
    Ok(email
        .map(|e| EPHEMERAL_CREATOR_EMAILS.contains(&e.as_str()) || e == GET_A_DB_CREATOR_EMAIL)
        .unwrap_or(false))
}

fn app_expires_ms(app: &Value) -> Value {
    let created = app
        .get("created_at")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%SZ").ok());
    match created {
        Some(t) => {
            use chrono::TimeZone;
            let expires = t + chrono::Duration::days(EPHEMERAL_EXPIRATION_DAYS);
            json!(chrono::Utc.from_utc_datetime(&expires).timestamp_millis())
        }
        None => Value::Null,
    }
}

/// POST /dash/apps/ephemeral — legacy ephemeral_app.clj:60-80: no auth, a
/// throwaway app (14-day expiry) owned by the ephemeral creator; optional
/// `schema` / `rules.code` like `POST /dash/apps`.
pub async fn ephemeral_post(State(state): State<Arc<AppState>>, body: Bytes) -> Response {
    let r = async {
        let body = parse_body(&body)?;
        let title = body_str(&body, "title")?;
        let rules_code = rules_code_of(&body)?;
        let creator = ephemeral_creator(&state).await?;
        let id = Uuid::new_v4();
        let app = create_app(&state, id, &title, Some(creator), None, Uuid::new_v4()).await?;
        if let Some(code) = &rules_code {
            put_rules(&state, id, code).await?;
        }
        apply_initial_schema(&state, id, &body).await?;
        let expires = app_expires_ms(&app);
        Ok(json!({"app": app, "expires_ms": expires}))
    }
    .await;
    json_or_err(r)
}

/// GET /dash/apps/ephemeral/:app_id — legacy ephemeral_app.clj:82-90.
pub async fn ephemeral_get(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
) -> Response {
    let r = async {
        let app_id = path_uuid(&app_id, "app_id")?;
        let app = live_app_row(&state, app_id).await?;
        let creator = app
            .get("creator_id")
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok());
        let ephemeral_ids = ephemeral_creator_ids(&state).await?;
        if !creator.map(|c| ephemeral_ids.contains(&c)).unwrap_or(false) {
            return Err(InstantError::new(
                "permission-denied",
                400,
                "Permission denied: not ephemeral-app?",
                Some(json!({"input": app_id, "expected": "ephemeral-app?"})),
            ));
        }
        let expires = app_expires_ms(&app);
        Ok(json!({"app": app, "expires_ms": expires}))
    }
    .await;
    json_or_err(r)
}

pub(crate) async fn ephemeral_creator_ids(state: &AppState) -> Result<Vec<Uuid>> {
    let mut out = vec![];
    for email in EPHEMERAL_CREATOR_EMAILS {
        if let Some(id) = user_id_by_email(state, email).await? {
            out.push(id);
        }
    }
    Ok(out)
}

/// POST /dash/apps/:app_id/claim and /dash/apps/ephemeral/:app_id/claim —
/// legacy claim-app-post (dash/routes.clj:857-891): a dashboard user with
/// the app's admin token takes over an ephemeral / getadb app.
pub async fn claim_post(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let app_id = path_uuid(&app_id, "app_id")?;
        let body = parse_body(&body)?;
        let token = body_uuid(&body, "token")?;
        let user = dash_user(&state, &headers).await?;
        let app = live_app_row(&state, app_id).await?;
        let creator = app
            .get("creator_id")
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok());
        if !claimable(&state, creator).await? {
            return Err(InstantError::new(
                "permission-denied",
                400,
                "Permission denied: not claimable-app?",
                Some(json!({"input": app_id, "expected": "claimable-app?"})),
            ));
        }
        // the request must carry a valid admin token (app-admin-token-model/fetch!)
        let ok =
            sqlx::query("SELECT 1 AS x FROM app_admin_tokens WHERE app_id = $1 AND token = $2")
                .bind(app_id)
                .bind(token)
                .fetch_optional(&state.pool)
                .await?
                .is_some();
        if !ok {
            // app-admin-token-model/fetch! carries an extra hint message
            return Err(record_not_found(
                "app-admin-token",
                json!({
                    "args": [{"app-id": app_id, "token": token}],
                    "message": "This admin token may be expired or invalid. Or you may have provided an incorrect app ID.",
                }),
            ));
        }
        sqlx::query("UPDATE apps SET creator_id = $1 WHERE id = $2")
            .bind(user.id)
            .bind(app_id)
            .execute(&state.pool)
            .await?;
        Ok(json!({}))
    }
    .await;
    json_or_err(r)
}

// ---------------------------------------------------------------------------
// orgs

pub(crate) async fn orgs_for_user(state: &AppState, user_id: Uuid) -> Result<Vec<Value>> {
    let rows = sqlx::query(
        "SELECT o.id, o.title, o.created_at, o.updated_at, m.role
           FROM orgs o JOIN org_members m ON m.org_id = o.id
          WHERE m.user_id = $1
          ORDER BY o.created_at, o.id",
    )
    .bind(user_id)
    .fetch_all(&state.pool)
    .await?;
    Ok(rows
        .iter()
        .map(|r| {
            json!({
                "id": r.get::<Uuid, _>("id"),
                "title": r.get::<String, _>("title"),
                "created_at": ts_tz(r.get::<Option<chrono::DateTime<chrono::Utc>>, _>("created_at")),
                "updated_at": ts_tz(r.get::<Option<chrono::DateTime<chrono::Utc>>, _>("updated_at")),
                "role": r.get::<String, _>("role"),
                "paid": false,
            })
        })
        .collect())
}

/// legacy `org-model/get-org-for-user!` + `assert-least-privilege!`
/// (util/roles.clj:144-156 `org-with-role-for-user!`).
pub(crate) async fn org_role_for_user(
    state: &AppState,
    org_id: Uuid,
    user_id: Uuid,
    least: DashRole,
) -> Result<Value> {
    let row = sqlx::query(
        "SELECT o.id, o.title, o.created_at, o.updated_at, m.role, m.created_at AS member_created_at
           FROM orgs o JOIN org_members m ON m.org_id = o.id
          WHERE o.id = $1 AND m.user_id = $2",
    )
    .bind(org_id)
    .bind(user_id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or_else(|| {
        record_not_found(
            "org",
            json!({"args": [{"user-id": user_id, "org-id": org_id}]}),
        )
    })?;
    let role: String = row.get("role");
    // org-with-role-for-user! (util/roles.clj:144-156) checks the role only;
    // the plan gate applies to app access through an org membership
    assert_least_privilege(least, DashRole::parse(&role))?;
    Ok(json!({
        "id": row.get::<Uuid, _>("id"),
        "title": row.get::<String, _>("title"),
        "created_at": ts_tz(row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("created_at")),
        "updated_at": ts_tz(row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("updated_at")),
        "role": role,
        "paid": false,
    }))
}

/// POST /dash/orgs — legacy orgs-post (dash/routes.clj:1217-1221).
pub async fn orgs_post(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let body = parse_body(&body)?;
        let title = body_str(&body, "title")?;
        let user = dash_user(&state, &headers).await?;
        let org_id = Uuid::new_v4();
        let mut dbtx = state.pool.begin().await?;
        let row = sqlx::query(
            "INSERT INTO orgs (id, title) VALUES ($1, $2) RETURNING id, title, created_at, updated_at",
        )
        .bind(org_id)
        .bind(title)
        .fetch_one(&mut *dbtx)
        .await?;
        sqlx::query("INSERT INTO org_members (id, org_id, user_id, role) VALUES ($1, $2, $3, 'owner')")
            .bind(Uuid::new_v4())
            .bind(org_id)
            .bind(user.id)
            .execute(&mut *dbtx)
            .await?;
        dbtx.commit().await?;
        Ok(json!({"org": {
            "id": row.get::<Uuid, _>("id"),
            "title": row.get::<String, _>("title"),
            "created_at": ts_tz(row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("created_at")),
            "updated_at": ts_tz(row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("updated_at")),
        }}))
    }
    .await;
    json_or_err(r)
}

/// GET /dash/orgs/:org_id — legacy org-get (dash/routes.clj:1229-1244);
/// `instant-cli`'s app picker reads `apps`.
pub async fn org_get(
    State(state): State<Arc<AppState>>,
    Path(org_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let user = dash_user(&state, &headers).await?;
        let org_id = path_uuid(&org_id, "org_id")?;
        let org = org_role_for_user(&state, org_id, user.id, DashRole::Collaborator).await?;
        let apps = apps_for_org(&state, org_id, user.id, &org).await?;
        let members: Vec<Value> = sqlx::query(
            "SELECT m.id, u.email, m.role FROM org_members m JOIN instant_users u ON u.id = m.user_id
              WHERE m.org_id = $1 ORDER BY m.created_at, m.id",
        )
        .bind(org_id)
        .fetch_all(&state.pool)
        .await?
        .iter()
        .map(|r| {
            json!({
                "id": r.get::<Uuid, _>("id"),
                "email": r.get::<String, _>("email"),
                "role": r.get::<String, _>("role"),
            })
        })
        .collect();
        let invites: Vec<Value> = sqlx::query(
            "SELECT id, invitee_email, invitee_role, status, sent_at FROM org_member_invites
              WHERE org_id = $1",
        )
        .bind(org_id)
        .fetch_all(&state.pool)
        .await
        .map(|rows| {
            rows.iter()
                .map(|r| {
                    json!({
                        "id": r.get::<Uuid, _>("id"),
                        "email": r.get::<String, _>("invitee_email"),
                        "role": r.get::<String, _>("invitee_role"),
                        "status": r.get::<String, _>("status"),
                        "sent_at": ts_tz(r.get::<Option<chrono::DateTime<chrono::Utc>>, _>("sent_at")),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
        Ok(json!({"org": org, "apps": apps, "members": members, "invites": invites}))
    }
    .await;
    json_or_err(r)
}

/// The org's live apps in the dashboard app shape (legacy `apps-for-org`,
/// model/org.clj:89-123 over `make-apps-q`).
pub(crate) async fn apps_for_org(
    state: &AppState,
    org_id: Uuid,
    user_id: Uuid,
    org: &Value,
) -> Result<Vec<Value>> {
    let user = user_id;
    let apps: Vec<Value> = sqlx::query(
        "SELECT a.id, a.creator_id, a.org_id, a.title, a.created_at, a.status,
                a.deletion_marked_at, a.subscription_id, a.magic_code_expiry_minutes,
                a.connection_string, at.token AS admin_token, r.code AS rules,
                r.version AS rules_version,
                (SELECT m.member_role FROM app_members m WHERE m.app_id = a.id AND m.user_id = $2) AS user_app_role
           FROM apps a
           LEFT JOIN app_admin_tokens at ON at.app_id = a.id
           LEFT JOIN rules r ON r.app_id = a.id
          WHERE a.org_id = $1 AND a.deletion_marked_at IS NULL
          ORDER BY a.created_at, a.id",
    )
    .bind(org_id)
    .bind(user)
    .fetch_all(&state.pool)
    .await?
    .iter()
    .map(|r| {
        let mut app = app_row_json(r);
        if let Some(m) = app.as_object_mut() {
            m.insert("admin_token".into(), json!(r.get::<Option<Uuid>, _>("admin_token")));
            m.insert("rules".into(), r.get::<Option<Value>, _>("rules").unwrap_or(Value::Null));
            m.insert("rules_version".into(), json!(r.get::<Option<i32>, _>("rules_version")));
            m.insert("org".into(), json!({"id": org["id"], "title": org["title"]}));
            m.insert("pro".into(), json!(false));
            m.insert("user_app_role".into(), json!(r.get::<Option<String>, _>("user_app_role")));
            m.insert("members".into(), json!([]));
            m.insert("invites".into(), json!([]));
            m.insert("webhooks".into(), json!([]));
            m.insert("effective_status".into(), json!(r.get::<String, _>("status")));
        }
        app
    })
    .collect();
    Ok(apps)
}

/// DELETE /dash/orgs/:org_id — legacy orgs-delete: owner only.
pub async fn org_delete(
    State(state): State<Arc<AppState>>,
    Path(org_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let org_id = path_uuid(&org_id, "org_id")?;
        let user = dash_user(&state, &headers).await?;
        org_role_for_user(&state, org_id, user.id, DashRole::Owner).await?;
        sqlx::query("DELETE FROM orgs WHERE id = $1")
            .bind(org_id)
            .execute(&state.pool)
            .await
            .map_err(|e| match &e {
                // legacy translate-and-throw-psql-exception! (util/exception.clj:633-649)
                sqlx::Error::Database(db) if db.code().as_deref() == Some("23503") => {
                    let constraint = db.constraint().unwrap_or_default().to_string();
                    let message = if constraint == "apps_org_id_fkey" {
                        "The org can't be deleted while it still has apps.".to_string()
                    } else {
                        "Foreign Key Invalid: foreign-key-violation".to_string()
                    };
                    InstantError::new(
                        "record-foreign-key-invalid",
                        400,
                        message,
                        Some(json!({
                            "table": db.table(),
                            "condition": "foreign-key-violation",
                            "constraint": constraint,
                        })),
                    )
                }
                _ => InstantError::from(e),
            })?;
        Ok(json!({"ok": true}))
    }
    .await;
    json_or_err(r)
}

// ---------------------------------------------------------------------------
// OAuth configuration ($oauthProviders / $oauthClients triples +
// app_authorized_redirect_origins)

/// Every entity of a system etype as `{label: value}` plus `created_at`
/// from the `id` triple (system_catalog_ops.clj triples->db-format).
async fn system_entities(
    state: &AppState,
    app_id: Uuid,
    etype: &str,
) -> Result<Vec<Map<String, Value>>> {
    let attrs = service::load_attrs(state, app_id).await?;
    let attr_ids: Vec<Uuid> = attrs.attrs_of_etype(etype).map(|a| a.id).collect();
    let rows = sqlx::query(
        "SELECT entity_id, attr_id, value, created_at FROM triples
          WHERE app_id = $1 AND attr_id = ANY($2)
          ORDER BY created_at, entity_id",
    )
    .bind(app_id)
    .bind(attr_ids)
    .fetch_all(&state.pool)
    .await?;
    let mut order: Vec<Uuid> = vec![];
    let mut ents: std::collections::HashMap<Uuid, Map<String, Value>> = Default::default();
    for r in rows {
        let eid: Uuid = r.get("entity_id");
        let attr_id: Uuid = r.get("attr_id");
        let v: Value = r.get("value");
        let Some(attr) = attrs.get(&attr_id) else {
            continue;
        };
        let ent = ents.entry(eid).or_insert_with(|| {
            order.push(eid);
            Map::new()
        });
        if attr.label == "id" {
            ent.insert(
                "created_at".into(),
                ts_ms(r.get::<Option<i64>, _>("created_at")),
            );
        }
        ent.insert(attr.label.clone(), v);
    }
    Ok(order.into_iter().filter_map(|e| ents.remove(&e)).collect())
}

async fn system_entity(
    state: &AppState,
    app_id: Uuid,
    etype: &str,
    id: Uuid,
) -> Result<Option<Map<String, Value>>> {
    Ok(system_entities(state, app_id, etype)
        .await?
        .into_iter()
        .find(|e| e.get("id").and_then(|v| v.as_str()) == Some(id.to_string().as_str())))
}

fn provider_view(p: &Map<String, Value>) -> Value {
    json!({
        "id": p.get("id").cloned().unwrap_or(Value::Null),
        "provider_name": p.get("name").cloned().unwrap_or(Value::Null),
        "created_at": p.get("created_at").cloned().unwrap_or(Value::Null),
    })
}

fn client_view(c: &Map<String, Value>, keys: &[&str]) -> Value {
    let full = json!({
        "id": c.get("id").cloned().unwrap_or(Value::Null),
        "client_name": c.get("name").cloned().unwrap_or(Value::Null),
        "client_id": c.get("clientId").cloned().unwrap_or(Value::Null),
        "provider_id": c.get("$oauthProvider").cloned().unwrap_or(Value::Null),
        "meta": c.get("meta").cloned().unwrap_or(Value::Null),
        "discovery_endpoint": c.get("discoveryEndpoint").cloned().unwrap_or(Value::Null),
        "created_at": c.get("created_at").cloned().unwrap_or(Value::Null),
        "redirect_to": c.get("redirectTo").cloned().unwrap_or(Value::Null),
        "use_shared_credentials": c.get("useSharedCredentials").and_then(|v| v.as_bool()).unwrap_or(false),
    });
    let mut out = Map::new();
    for k in keys {
        out.insert(
            (*k).to_string(),
            full.get(*k).cloned().unwrap_or(Value::Null),
        );
    }
    Value::Object(out)
}

async fn redirect_origins(state: &AppState, app_id: Uuid) -> Result<Vec<Value>> {
    let rows = sqlx::query(
        "SELECT id, service, params, created_at FROM app_authorized_redirect_origins
          WHERE app_id = $1 ORDER BY created_at DESC",
    )
    .bind(app_id)
    .fetch_all(&state.pool)
    .await?;
    Ok(rows.iter().map(origin_view).collect())
}

fn origin_view(r: &sqlx::postgres::PgRow) -> Value {
    json!({
        "id": r.get::<Uuid, _>("id"),
        "service": r.get::<String, _>("service"),
        "params": r.get::<Vec<String>, _>("params"),
        "created_at": ts_tz(r.get::<Option<chrono::DateTime<chrono::Utc>>, _>("created_at")),
    })
}

/// GET /dash/apps/:app_id/auth — legacy dash-apps-auth-get
/// (model/app_auth_data.clj:8-61).
pub async fn auth_get(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let app = dash_authed_with_role(&state, &headers, &app_id, DashRole::Collaborator, Scope::AppsRead).await?;
        let providers: Vec<Value> = system_entities(&state, app.id, "$oauthProviders")
            .await?
            .iter()
            .map(provider_view)
            .collect();
        let clients: Vec<Value> = system_entities(&state, app.id, "$oauthClients")
            .await?
            .iter()
            .map(|c| {
                client_view(
                    c,
                    &[
                        "id",
                        "client_name",
                        "client_id",
                        "provider_id",
                        "meta",
                        "discovery_endpoint",
                        "created_at",
                        "redirect_to",
                        "use_shared_credentials",
                    ],
                )
            })
            .collect();
        let origins = redirect_origins(&state, app.id).await?;
        Ok(json!({
            "oauth_service_providers": providers,
            "oauth_clients": clients,
            "authorized_redirect_origins": if origins.is_empty() { Value::Null } else { json!(origins) },
            "default_sender_email": state.email.default_sender_email,
        }))
    }
    .await;
    json_or_err(r)
}

/// POST /dash/apps/:app_id/oauth_service_providers — legacy
/// oauth-service-providers-post (dash/routes.clj:748-756).
pub async fn providers_post(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let app = dash_authed_with_role(
            &state,
            &headers,
            &app_id,
            DashRole::Collaborator,
            Scope::AppsWrite,
        )
        .await?;
        let body = parse_body(&body)?;
        let name = body_str(&body, "provider_name")?;
        let id = Uuid::new_v4();
        let steps = json!([
            ["add-triple", id, sc::attr_id("$oauthProviders", "id"), id],
            [
                "add-triple",
                id,
                sc::attr_id("$oauthProviders", "name"),
                name
            ],
        ]);
        service::run_system_transact(&state, app.id, &steps).await?;
        let provider = system_entity(&state, app.id, "$oauthProviders", id)
            .await?
            .unwrap_or_default();
        Ok(json!({"provider": provider_view(&provider)}))
    }
    .await;
    json_or_err(r)
}

/// legacy `validate-discovery-endpoint!` (model/app_oauth_client.clj:17-32):
/// the document must load and carry a string `issuer`.
async fn validate_discovery_endpoint(endpoint: &str) -> Result<()> {
    let fail = || {
        InstantError::new(
            "validation-failed",
            400,
            "Validation failed for discovery-endpoint: Could not validate discovery endpoint.",
            Some(json!({
                "data-type": "discovery-endpoint",
                "input": endpoint,
                "errors": [{"message": "Could not validate discovery endpoint."}],
            })),
        )
    };
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|_| fail())?;
    let doc: Value = client
        .get(endpoint)
        .send()
        .await
        .map_err(|_| fail())?
        .json()
        .await
        .map_err(|_| fail())?;
    if doc.get("issuer").and_then(|v| v.as_str()).is_none() {
        return Err(fail());
    }
    Ok(())
}

/// legacy `url-util/redirect-url-validation-errors` (util/url.clj:15-34)
/// with `allow-localhost? true`.
fn redirect_url_validation_errors(raw: &str) -> Vec<String> {
    redirect_url_validation_errors_opt(raw, true)
}

/// legacy `url/redirect-url-validation-errors` with `:allow-localhost?`
/// (util/url.clj:15-34)
pub(crate) fn redirect_url_validation_errors_opt(raw: &str, allow_localhost: bool) -> Vec<String> {
    let mut errors = vec![];
    let Ok(parsed) = url::Url::parse(raw) else {
        errors.push("redirect uri must use the HTTPS scheme".to_string());
        return errors;
    };
    let host = parsed.host_str().unwrap_or_default();
    let localhost = host == "localhost" || host == "127.0.0.1";
    if !parsed.username().is_empty() || parsed.password().is_some() {
        errors.push("redirect uri may not contain user or password".to_string());
    }
    let path = parsed.path();
    if path.contains("/..") || path.contains("\\.") {
        errors.push("redirect uri may not contain a path traversal".to_string());
    }
    if parsed.fragment().is_some() {
        errors.push("redirect uri may not contain the fragment component".to_string());
    }
    if localhost && !allow_localhost {
        errors.push("redirect uri may not be localhost".to_string());
    }
    if !localhost && parsed.scheme() != "https" {
        errors.push("redirect uri must use the HTTPS scheme".to_string());
    }
    if localhost && !matches!(parsed.scheme(), "http" | "https") {
        errors.push("redirect uri must use either the HTTP or HTTPS scheme".to_string());
    }
    errors
}

/// POST /dash/apps/:app_id/oauth_clients — legacy oauth-clients-post
/// (dash/routes.clj:758-803). Secrets are stored as given (this server
/// reads `encryptedClientSecret` verbatim, like `scripts/create-oauth-client.sh`).
pub async fn clients_post(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let app = dash_authed_with_role(&state, &headers, &app_id, DashRole::Collaborator, Scope::AppsWrite).await?;
        let body = parse_body(&body)?;
        let provider_id = body_uuid(&body, "provider_id")?;
        let client_name = body_str(&body, "client_name")?;
        let client_id = body_opt_str(&body, "client_id")?;
        let client_secret = body_opt_str(&body, "client_secret")?;
        let meta = match body.get("meta") {
            None | Some(Value::Null) => None,
            Some(Value::Object(m)) => Some(Value::Object(m.clone())),
            Some(v) => return Err(param_malformed(&["body", "meta"], v.clone())),
        };
        let use_shared = body
            .get("use_shared_credentials")
            .map(|v| !matches!(v, Value::Null | Value::Bool(false)))
            .unwrap_or(false);
        let redirect_to = body
            .get("redirect_to")
            .and_then(coerce_non_blank_str);
        if let Some(rt) = &redirect_to {
            assert_valid("redirect_to", json!(rt), redirect_url_validation_errors(rt))?;
        }
        let provider = system_entity(&state, app.id, "$oauthProviders", provider_id)
            .await?
            .ok_or_else(|| {
                record_not_found("oauth-service-provider", json!({"provider-id": provider_id}))
            })?;
        let provider_name = provider.get("name").and_then(|v| v.as_str()).unwrap_or_default();
        // GitHub doesn't need discovery endpoints; OIDC providers do
        let discovery_endpoint = if provider_name == "github" {
            None
        } else {
            Some(body_str(&body, "discovery_endpoint")?)
        };
        if use_shared {
            // shared Instant credentials are a hosted-service feature
            return Err(InstantError::new(
                "record-not-found",
                400,
                "Record not found: shared-oauth-client",
                Some(json!({"record-type": "shared-oauth-client", "provider-name": provider_name})),
            ));
        }
        if let Some(d) = &discovery_endpoint {
            validate_discovery_endpoint(d).await?;
        }
        let id = Uuid::new_v4();
        let mut steps = vec![
            json!(["add-triple", id, sc::attr_id("$oauthClients", "id"), id]),
            json!(["add-triple", id, sc::attr_id("$oauthClients", "$oauthProvider"), provider_id]),
            json!(["add-triple", id, sc::attr_id("$oauthClients", "name"), client_name]),
            json!(["add-triple", id, sc::attr_id("$oauthClients", "useSharedCredentials"), false]),
        ];
        if let Some(v) = &client_id {
            steps.push(json!(["add-triple", id, sc::attr_id("$oauthClients", "clientId"), v]));
        }
        if let Some(v) = &client_secret {
            steps.push(json!(["add-triple", id, sc::attr_id("$oauthClients", "encryptedClientSecret"), v]));
        }
        if let Some(v) = &discovery_endpoint {
            steps.push(json!(["add-triple", id, sc::attr_id("$oauthClients", "discoveryEndpoint"), v]));
        }
        if let Some(v) = &meta {
            steps.push(json!(["add-triple", id, sc::attr_id("$oauthClients", "meta"), v]));
        }
        if let Some(v) = &redirect_to {
            steps.push(json!(["add-triple", id, sc::attr_id("$oauthClients", "redirectTo"), v]));
        }
        service::run_system_transact(&state, app.id, &Value::Array(steps)).await?;
        let client = system_entity(&state, app.id, "$oauthClients", id)
            .await?
            .unwrap_or_default();
        Ok(json!({"client": client_view(&client, &["id", "provider_id", "client_name", "client_id", "created_at", "meta", "discovery_endpoint", "use_shared_credentials"])}))
    }
    .await;
    json_or_err(r)
}

/// POST /dash/apps/:app_id/oauth_clients/:id — legacy update-oauth-client
/// (dash/routes.clj:805-848): present-vs-null is distinguished per key, and
/// `meta` deep-merges.
pub async fn clients_update(
    State(state): State<Arc<AppState>>,
    Path((app_id, id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let app = dash_authed_with_role(&state, &headers, &app_id, DashRole::Collaborator, Scope::AppsWrite).await?;
        let id = path_uuid(&id, "id")?;
        let body = parse_body(&body)?;
        let existing = system_entity(&state, app.id, "$oauthClients", id)
            .await?
            .ok_or_else(|| record_not_found("app-oauth-client", json!({"app-id": app.id, "id": id})))?;
        let redirect_to = body.get("redirect_to").and_then(coerce_non_blank_str);
        if let Some(rt) = &redirect_to {
            assert_valid("redirect_to", json!(rt), redirect_url_validation_errors(rt))?;
        }
        let client_id = body_opt_str(&body, "client_id")?;
        let client_secret = body_opt_str(&body, "client_secret")?;
        let discovery_endpoint = body_opt_str(&body, "discovery_endpoint")?;
        let has = |k: &str| body.get(k).is_some();
        let use_shared = body
            .get("use_shared_credentials")
            .filter(|v| !v.is_null())
            .map(|v| !matches!(v, Value::Bool(false)));
        if use_shared == Some(true) {
            return Err(InstantError::new(
                "record-not-found",
                400,
                "Record not found: shared-oauth-client",
                Some(json!({"record-type": "shared-oauth-client"})),
            ));
        }
        if has("discovery_endpoint") {
            if let Some(d) = &discovery_endpoint {
                validate_discovery_endpoint(d).await?;
            }
        }
        let mut steps = vec![json!(["add-triple", id, sc::attr_id("$oauthClients", "id"), id])];
        let set = |steps: &mut Vec<Value>, label: &str, v: Value| {
            steps.push(json!(["add-triple", id, sc::attr_id("$oauthClients", label), v]));
        };
        if has("meta") {
            match body.get("meta") {
                Some(Value::Object(m)) => steps.push(json!([
                    "deep-merge-triple",
                    id,
                    sc::attr_id("$oauthClients", "meta"),
                    Value::Object(m.clone())
                ])),
                Some(Value::Null) | None => steps.push(json!([
                    "deep-merge-triple",
                    id,
                    sc::attr_id("$oauthClients", "meta"),
                    Value::Null
                ])),
                Some(v) => return Err(param_malformed(&["body", "meta"], v.clone())),
            }
        }
        if has("redirect_to") {
            set(&mut steps, "redirectTo", json!(redirect_to));
        }
        if has("client_id") {
            set(&mut steps, "clientId", json!(client_id));
        }
        if has("client_secret") {
            set(&mut steps, "encryptedClientSecret", json!(client_secret));
        }
        if has("discovery_endpoint") {
            set(&mut steps, "discoveryEndpoint", json!(discovery_endpoint));
        }
        if has("use_shared_credentials") {
            set(&mut steps, "useSharedCredentials", json!(use_shared.unwrap_or(false)));
        }
        let _ = existing;
        service::run_system_transact(&state, app.id, &Value::Array(steps)).await?;
        let client = system_entity(&state, app.id, "$oauthClients", id)
            .await?
            .unwrap_or_default();
        Ok(json!({"client": client_view(&client, &["id", "provider_id", "client_name", "client_id", "created_at", "meta", "discovery_endpoint", "redirect_to", "use_shared_credentials"])}))
    }
    .await;
    json_or_err(r)
}

/// DELETE /dash/apps/:app_id/oauth_clients/:id — legacy oauth-clients-delete.
pub async fn clients_delete(
    State(state): State<Arc<AppState>>,
    Path((app_id, id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let app = dash_authed_with_role(&state, &headers, &app_id, DashRole::Collaborator, Scope::AppsWrite).await?;
        let id = path_uuid(&id, "id")?;
        let client = system_entity(&state, app.id, "$oauthClients", id)
            .await?
            .ok_or_else(|| {
                record_not_found("app-oauth-client", json!({"args": [{"id": id, "app-id": app.id}]}))
            })?;
        service::run_system_transact(
            &state,
            app.id,
            &json!([["delete-entity", id, "$oauthClients"]]),
        )
        .await?;
        Ok(json!({"client": client_view(&client, &["id", "provider_id", "client_name", "client_id", "created_at"])}))
    }
    .await;
    json_or_err(r)
}

/// legacy `app-authorized-redirect-origin-model/validation-error`.
fn origin_validation_error(service: &str, params: &[Value]) -> Option<String> {
    let all_strings = params.iter().all(|p| p.is_string());
    match service {
        "netlify" => {
            if params.len() != 1 {
                Some("Netlify should have only the site name param.".into())
            } else if !all_strings {
                Some("Netlify site-name should be a string.".into())
            } else {
                None
            }
        }
        "vercel" => {
            if params.len() != 2 {
                Some("Vercel should have deployment suffix and project name params.".into())
            } else if !all_strings {
                Some("Vercel deployment suffix and project name should both be strings.".into())
            } else {
                None
            }
        }
        "generic" => {
            if params.len() != 1 {
                Some("Host should be the only parameter.".into())
            } else if !all_strings {
                Some("Host should be a string.".into())
            } else {
                None
            }
        }
        "custom-scheme" => {
            const RESERVED: [&str; 12] = [
                "http",
                "https",
                "ftp",
                "file",
                "mailto",
                "tel",
                "sms",
                "data",
                "javascript",
                "ws",
                "wss",
                "blob",
            ];
            if params.len() != 1 {
                Some("Custom scheme should have only one parameter.".into())
            } else if !all_strings {
                Some("Custom scheme should be a string.".into())
            } else {
                let scheme = params[0].as_str().unwrap_or_default();
                if RESERVED.contains(&scheme) {
                    Some(format!("The scheme `{scheme}` is not allowed."))
                } else {
                    None
                }
            }
        }
        other => Some(format!("Unrecognized service {other}")),
    }
}

/// POST /dash/apps/:app_id/authorized_redirect_origins — legacy
/// authorized-redirect-origins-post (dash/routes.clj:725-736).
pub async fn origins_post(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let app = dash_authed_with_role(
            &state,
            &headers,
            &app_id,
            DashRole::Collaborator,
            Scope::AppsWrite,
        )
        .await?;
        let body = parse_body(&body)?;
        let service_name = body_str(&body, "service")?;
        let params_v = body
            .get("params")
            .filter(|v| !v.is_null())
            .ok_or_else(|| param_missing(&["body", "params"]))?;
        let params = params_v
            .as_array()
            .cloned()
            .ok_or_else(|| param_malformed(&["body", "params"], params_v.clone()))?;
        let origin_req = json!({"app-id": app.id, "service": service_name, "params": params});
        if let Some(err) = origin_validation_error(&service_name, &params) {
            assert_valid("origin-request", origin_req, vec![err])?;
        }
        let strings: Vec<String> = params
            .iter()
            .map(|p| p.as_str().unwrap_or_default().to_string())
            .collect();
        let id = Uuid::new_v4();
        let row = sqlx::query(
            "INSERT INTO app_authorized_redirect_origins (id, app_id, service, params)
             VALUES ($1, $2, $3, $4) RETURNING id, service, params, created_at",
        )
        .bind(id)
        .bind(app.id)
        .bind(service_name)
        .bind(strings)
        .fetch_one(&state.pool)
        .await
        .map_err(|e| match &e {
            sqlx::Error::Database(db) if db.code().as_deref() == Some("23505") => {
                InstantError::new(
                    "record-not-unique",
                    400,
                    "Record not unique: app-authorized-redirect-origin",
                    Some(json!({"record-type": "app-authorized-redirect-origin"})),
                )
            }
            _ => InstantError::from(e),
        })?;
        Ok(json!({"origin": origin_view(&row)}))
    }
    .await;
    json_or_err(r)
}

/// DELETE /dash/apps/:app_id/authorized_redirect_origins/:id
pub async fn origins_delete(
    State(state): State<Arc<AppState>>,
    Path((app_id, id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let app = dash_authed_with_role(
            &state,
            &headers,
            &app_id,
            DashRole::Collaborator,
            Scope::AppsWrite,
        )
        .await?;
        let id = path_uuid(&id, "id")?;
        let row = sqlx::query(
            "DELETE FROM app_authorized_redirect_origins WHERE id = $1 AND app_id = $2
             RETURNING id, service, params, created_at",
        )
        .bind(id)
        .bind(app.id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| {
            record_not_found(
                "app-authorized-redirect-origin",
                json!({"args": [{"id": id, "app-id": app.id}]}),
            )
        })?;
        Ok(json!({"origin": origin_view(&row)}))
    }
    .await;
    json_or_err(r)
}

// ---------------------------------------------------------------------------
// email templates

/// GET /dash/default-email-template — legacy default-email-template-get.
pub async fn default_email_template(State(state): State<Arc<AppState>>) -> Response {
    json_or_err(Ok(json!({
        "email-type": "magic-code",
        "sender-email": state.email.default_sender_email,
        "subject": "{code} is your verification code for {app_title}",
        "body": crate::email::default_template_body(),
    })))
}

async fn email_template_info(state: &AppState, app_id: Uuid) -> Result<Value> {
    let row = sqlx::query(
        "SELECT t.id, t.app_id, t.email_type, t.body, t.sender_id, t.name, t.subject,
                s.email, s.postmark_id, v.id AS verification_id, v.verified AS verification_verified
           FROM app_email_templates t
           LEFT JOIN app_email_senders s ON t.sender_id = s.id
           LEFT JOIN app_email_verifications v ON t.sender_id = v.sender_id AND v.app_id = t.app_id
          WHERE t.app_id = $1 AND t.email_type = 'magic-code'",
    )
    .bind(app_id)
    .fetch_optional(&state.pool)
    .await?;
    Ok(match row {
        None => Value::Null,
        Some(r) => json!({
            "id": r.get::<Uuid, _>("id"),
            "app_id": r.get::<Uuid, _>("app_id"),
            "email_type": r.get::<String, _>("email_type"),
            "body": r.get::<String, _>("body"),
            "sender_id": r.get::<Option<Uuid>, _>("sender_id"),
            "name": r.get::<Option<String>, _>("name"),
            "subject": r.get::<String, _>("subject"),
            "email": r.get::<Option<String>, _>("email"),
            "postmark_id": r.get::<Option<i32>, _>("postmark_id"),
            "verification_id": r.get::<Option<Uuid>, _>("verification_id"),
            "verification_verified": r.get::<Option<bool>, _>("verification_verified"),
        }),
    })
}

/// GET /dash/apps/:app_id/email_status — legacy email-status-get.
pub async fn email_status(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let app =
            dash_authed_with_role(&state, &headers, &app_id, DashRole::Admin, Scope::AppsRead)
                .await?;
        Ok(json!({"info": email_template_info(&state, app.id).await?}))
    }
    .await;
    json_or_err(r)
}

/// POST /dash/apps/:app_id/email_templates — legacy email-template-post
/// (dash/routes.clj:1574-1602). Body: `{email-type, subject, body,
/// sender-email?, sender-name?}`; subject and body must mention `{code}`.
/// A sender email is recorded (`app_email_senders`) but, without Postmark,
/// never verified, so the default sender keeps delivering.
pub async fn email_template_post(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let app = dash_authed_with_role(&state, &headers, &app_id, DashRole::Admin, Scope::AppsRead).await?;
        let body = parse_body(&body)?;
        let email_type = body_str(&body, "email-type")?;
        let subject = body_str(&body, "subject")?;
        if !subject.contains("{code}") {
            assert_valid_msgs(
                "subject",
                json!(subject),
                "Subject does not contain template variable: '{code}'",
            )?;
        }
        let template_body = body_str(&body, "body")?;
        if !template_body.contains("{code}") {
            assert_valid_msgs(
                "body",
                json!(template_body),
                "Body does not contain template variable: '{code}'",
            )?;
        }
        let sender_email = body
            .get("sender-email")
            .and_then(|v| v.as_str())
            .map(|e| e.trim().to_lowercase())
            .filter(|e| crate::routes::runtime::valid_email(e));
        let sender_name = body
            .get("sender-name")
            .and_then(coerce_non_blank_str)
            .unwrap_or_else(|| app.title.clone());
        let sender_id: Option<Uuid> = match &sender_email {
            Some(email) => {
                let existing: Option<Uuid> =
                    sqlx::query("SELECT id FROM app_email_senders WHERE email = $1")
                        .bind(email)
                        .fetch_optional(&state.pool)
                        .await?
                        .map(|r| r.get("id"));
                match existing {
                    Some(id) => {
                        sqlx::query("UPDATE app_email_senders SET name = $2 WHERE id = $1")
                            .bind(id)
                            .bind(&sender_name)
                            .execute(&state.pool)
                            .await?;
                        Some(id)
                    }
                    None => {
                        let id = Uuid::new_v4();
                        sqlx::query(
                            "INSERT INTO app_email_senders (id, postmark_id, email, name) VALUES ($1, 0, $2, $3)",
                        )
                        .bind(id)
                        .bind(email)
                        .bind(&sender_name)
                        .execute(&state.pool)
                        .await?;
                        Some(id)
                    }
                }
            }
            None => None,
        };
        let row = sqlx::query(
            "INSERT INTO app_email_templates (id, app_id, sender_id, email_type, subject, name, body)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (app_id, email_type) DO UPDATE SET
               sender_id = EXCLUDED.sender_id, subject = EXCLUDED.subject,
               name = EXCLUDED.name, body = EXCLUDED.body
             RETURNING id",
        )
        .bind(Uuid::new_v4())
        .bind(app.id)
        .bind(sender_id)
        .bind(email_type)
        .bind(subject)
        .bind(sender_name)
        .bind(template_body)
        .fetch_one(&state.pool)
        .await?;
        Ok(json!({"id": row.get::<Uuid, _>("id")}))
    }
    .await;
    json_or_err(r)
}

fn assert_valid_msgs(data_type: &str, input: Value, message: &str) -> Result<()> {
    Err(InstantError::new(
        "validation-failed",
        400,
        format!("Validation failed for {data_type}: {message}"),
        Some(json!({"data-type": data_type, "input": input, "errors": [{"message": message}]})),
    ))
}

/// DELETE /dash/apps/:app_id/email_templates/:id — legacy email-template-delete.
pub async fn email_template_delete(
    State(state): State<Arc<AppState>>,
    Path((app_id, id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let app =
            dash_authed_with_role(&state, &headers, &app_id, DashRole::Admin, Scope::AppsWrite)
                .await?;
        let id = path_uuid(&id, "id")?;
        sqlx::query("DELETE FROM app_email_templates WHERE id = $1 AND app_id = $2")
            .bind(id)
            .bind(app.id)
            .execute(&state.pool)
            .await?;
        Ok(json!({}))
    }
    .await;
    json_or_err(r)
}
