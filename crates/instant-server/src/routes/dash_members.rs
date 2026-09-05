//! Team routes of LEGACY dash/routes.clj: member invites (send / revoke /
//! accept / decline), member role updates and removal for apps and orgs,
//! org rename, app transfer to an org, the ephemeral-app status toggle and
//! the get-a-db app lookup. Models: model/member_invites.clj,
//! model/app_members.clj, model/org_members.clj, model/org.clj.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::Response;
use instant_core::error::{InstantError, Result};
use serde_json::{json, Value};
use sqlx::Row;
use uuid::Uuid;

use crate::routes::dash::{dash_user, param_malformed, param_missing, parse_body, DashRole};
use crate::routes::dash_apps::{
    assert_least_privilege, body_str, body_uuid, ephemeral_creator_ids, live_app_row,
    org_role_for_user, path_uuid, record_not_found,
};
use crate::routes::dash_manage::app_and_user;
use crate::routes::runtime::{coerce_email_pub, json_or_err};
use crate::service;
use crate::state::AppState;

/// app or org side of the shared handlers (`(tbl :app)` / `(tbl :org)`)
#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    App,
    Org,
}

impl Side {
    fn invites_table(self) -> &'static str {
        match self {
            Side::App => "app_member_invites",
            Side::Org => "org_member_invites",
        }
    }
    fn fk(self) -> &'static str {
        match self {
            Side::App => "app_id",
            Side::Org => "org_id",
        }
    }
    fn members_table(self) -> &'static str {
        match self {
            Side::App => "app_members",
            Side::Org => "org_members",
        }
    }
    fn role_column(self) -> &'static str {
        match self {
            Side::App => "member_role",
            Side::Org => "role",
        }
    }
    fn member_record(self) -> &'static str {
        match self {
            Side::App => "app-member",
            Side::Org => "org-member",
        }
    }
}

/// legacy `assert-valid-member-role!` (util/roles.clj:19-23): a bare-string
/// error, so the message stays "Validation failed for role".
fn assert_valid_member_role(role: &str) -> Result<DashRole> {
    DashRole::parse(role).ok_or_else(|| {
        InstantError::new(
            "validation-failed",
            400,
            "Validation failed for role",
            Some(json!({"data-type": "role", "input": role, "errors": ["Invalid role"]})),
        )
    })
}

fn permission_denied(perm: &str, input: Value) -> InstantError {
    InstantError::new(
        "permission-denied",
        400,
        format!("Permission denied: not {perm}"),
        Some(json!({"input": input, "expected": perm})),
    )
}

/// The `raise exception` messages of the org_members triggers (migration
/// 86) and the orgs title check, as legacy translates them
/// (util/exception.clj:698-793).
fn translate_db_error(e: sqlx::Error) -> InstantError {
    if let sqlx::Error::Database(db) = &e {
        let msg = db.message().to_string();
        match db.code().as_deref() {
            Some("P0001") if msg.contains("modify_org_id_on_org_member") => {
                return InstantError::new(
                    "validation-failed",
                    400,
                    "Org members can not move between orgs.",
                    None,
                )
            }
            Some("P0001") if msg.contains("remove_last_org_owner") => {
                return InstantError::new(
                    "validation-failed",
                    400,
                    "There must be at least one member of the org that is an owner.",
                    None,
                )
            }
            Some("23514") if db.constraint() == Some("orgs_title_check") => {
                return InstantError::new(
                    "validation-failed",
                    400,
                    "The title for the org is too long. The maximum length is 140 characters.",
                    Some(json!({
                        "table": db.table(),
                        "condition": "check-violation",
                        "constraint": "orgs_title_check",
                    })),
                )
            }
            Some("23505") => {
                let record = match db.table() {
                    Some("app_members") => "app-member",
                    Some("org_members") => "org-member",
                    Some("app_member_invites") => "app-member-invite",
                    Some("org_member_invites") => "org-member-invite",
                    other => other.unwrap_or("record"),
                };
                return InstantError::new(
                    "record-not-unique",
                    400,
                    format!("Record not unique: {record}"),
                    Some(json!({"record-type": record})),
                );
            }
            _ => {}
        }
    }
    InstantError::from(e)
}

/// Who the caller is on the app or org named by the path
/// (`req->app-and-user!` / `req->org-and-user!` with `least`).
struct Actor {
    user_id: Uuid,
    foreign_key: Uuid,
    role: DashRole,
}

async fn actor(
    state: &AppState,
    headers: &HeaderMap,
    side: Side,
    id_raw: &str,
    least: DashRole,
) -> Result<Actor> {
    match side {
        Side::App => {
            let (user, app, role) = app_and_user(state, headers, id_raw, least).await?;
            let foreign_key = app
                .get("id")
                .and_then(|v| v.as_str())
                .and_then(|s| Uuid::parse_str(s).ok())
                .unwrap_or_default();
            Ok(Actor {
                user_id: user.id,
                foreign_key,
                role,
            })
        }
        Side::Org => {
            let org_id = path_uuid(id_raw, "org_id")?;
            let user = dash_user(state, headers).await?;
            let org = org_role_for_user(state, org_id, user.id, least).await?;
            let role = org
                .get("role")
                .and_then(|v| v.as_str())
                .and_then(DashRole::parse)
                .unwrap_or(DashRole::Collaborator);
            Ok(Actor {
                user_id: user.id,
                foreign_key: org_id,
                role,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// invites

/// legacy team-member-invite-send-post (dash/routes.clj:1312-1343): admin
/// on the app / org; the invite row is upserted per (fk, email); the invite
/// email is log-only here.
async fn invite_send(
    state: &AppState,
    headers: &HeaderMap,
    side: Side,
    id_raw: &str,
    body: &Bytes,
) -> Result<Value> {
    let a = actor(state, headers, side, id_raw, DashRole::Admin).await?;
    let body = parse_body(body)?;
    let raw_email = body
        .get("invitee-email")
        .filter(|v| !v.is_null())
        .ok_or_else(|| param_missing(&["body", "invitee-email"]))?;
    let email = raw_email
        .as_str()
        .and_then(|s| coerce_email_pub(s).ok())
        .ok_or_else(|| param_malformed(&["body", "invitee-email"], raw_email.clone()))?;
    let role = body_str(&body, "role")?;
    assert_valid_member_role(&role)?;
    let sql = format!(
        "INSERT INTO {t} (id, {fk}, inviter_id, invitee_email, invitee_role, status, sent_at)
         VALUES ($1, $2, $3, $4, $5, 'pending', now())
         ON CONFLICT ({fk}, invitee_email)
         DO UPDATE SET status = 'pending', sent_at = now(), invitee_role = excluded.invitee_role",
        t = side.invites_table(),
        fk = side.fk()
    );
    sqlx::query(&sql)
        .bind(Uuid::new_v4())
        .bind(a.foreign_key)
        .bind(a.user_id)
        .bind(&email)
        .bind(&role)
        .execute(&state.pool)
        .await
        .map_err(translate_db_error)?;
    tracing::info!(
        "team invite (log-only mail): {email} invited as {role} to {} {}",
        side.fk(),
        a.foreign_key
    );
    Ok(json!({}))
}

pub async fn app_invite_send(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    json_or_err(invite_send(&state, &headers, Side::App, &app_id, &body).await)
}

pub async fn org_invite_send(
    State(state): State<Arc<AppState>>,
    Path(org_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    json_or_err(invite_send(&state, &headers, Side::Org, &org_id, &body).await)
}

/// legacy team-member-invite-revoke-delete (:1396-1416): the invite id is
/// read before auth; admin; the pending invite of *this* app / org is
/// revoked (legacy's query drops the foreign-key predicate by mistake and
/// revokes any pending invite by id; scoping it is deliberate here).
async fn invite_revoke(
    state: &AppState,
    headers: &HeaderMap,
    side: Side,
    id_raw: &str,
    body: &Bytes,
) -> Result<Value> {
    let body = parse_body(body)?;
    let invite_id = body_uuid(&body, "invite-id")?;
    let a = actor(state, headers, side, id_raw, DashRole::Admin).await?;
    let sql = format!(
        "UPDATE {t} SET status = 'revoked' WHERE id = $1 AND {fk} = $2 AND status = 'pending'",
        t = side.invites_table(),
        fk = side.fk()
    );
    sqlx::query(&sql)
        .bind(invite_id)
        .bind(a.foreign_key)
        .execute(&state.pool)
        .await?;
    Ok(json!({}))
}

pub async fn app_invite_revoke(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    json_or_err(invite_revoke(&state, &headers, Side::App, &app_id, &body).await)
}

pub async fn org_invite_revoke(
    State(state): State<Arc<AppState>>,
    Path(org_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    json_or_err(invite_revoke(&state, &headers, Side::Org, &org_id, &body).await)
}

struct Invite {
    side: Side,
    foreign_key: Uuid,
    invitee_email: String,
    invitee_role: String,
    status: String,
}

/// legacy `member-invites-model/get-by-id!`: the app table first, then the
/// org table; `record-not-found` for `member-invite` otherwise.
async fn invite_by_id(state: &AppState, id: Uuid) -> Result<Invite> {
    for side in [Side::App, Side::Org] {
        let sql = format!(
            "SELECT {fk} AS fk, invitee_email, invitee_role, status FROM {t} WHERE id = $1",
            fk = side.fk(),
            t = side.invites_table()
        );
        if let Some(r) = sqlx::query(&sql)
            .bind(id)
            .fetch_optional(&state.pool)
            .await?
        {
            return Ok(Invite {
                side,
                foreign_key: r.get("fk"),
                invitee_email: r.get("invitee_email"),
                invitee_role: r.get("invitee_role"),
                status: r.get("status"),
            });
        }
    }
    Err(record_not_found(
        "member-invite",
        json!({"args": [{"id": id}]}),
    ))
}

/// POST /dash/invites/accept — legacy team-member-invite-accept-post
/// (:1345-1368): any signed-in user whose email the invite names; the
/// invite must not be revoked; then the membership (or, for the `creator`
/// role, the app's creator) is written.
pub async fn invites_accept(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let user = dash_user(&state, &headers).await?;
        let body = parse_body(&body)?;
        let invite_id = body_uuid(&body, "invite-id")?;
        let invite = invite_by_id(&state, invite_id).await?;
        if invite.invitee_email != user.email {
            return Err(permission_denied("invitee?", json!(invite.invitee_email)));
        }
        if invite.status == "revoked" {
            return Err(permission_denied("acceptable?", json!(invite_id)));
        }
        let mut dbtx = state.pool.begin().await?;
        // accept-by-id! (member_invites.clj:115-134): pending and sent within
        // 3 days; legacy asserts the statement result, which is a map even
        // for zero rows, so the membership is written either way
        let sql = format!(
            "UPDATE {t} SET status = 'accepted'
              WHERE id = $1 AND status = 'pending' AND sent_at >= now() - interval '3 days'",
            t = invite.side.invites_table()
        );
        sqlx::query(&sql).bind(invite_id).execute(&mut *dbtx).await?;
        match invite.side {
            Side::App => {
                if invite.invitee_role == "creator" {
                    sqlx::query("UPDATE apps SET creator_id = $1 WHERE id = $2")
                        .bind(user.id)
                        .bind(invite.foreign_key)
                        .execute(&mut *dbtx)
                        .await?;
                } else {
                    sqlx::query(
                        "INSERT INTO app_members (id, app_id, user_id, member_role) VALUES ($1, $2, $3, $4)",
                    )
                    .bind(Uuid::new_v4())
                    .bind(invite.foreign_key)
                    .bind(user.id)
                    .bind(&invite.invitee_role)
                    .execute(&mut *dbtx)
                    .await
                    .map_err(translate_db_error)?;
                }
            }
            Side::Org => {
                sqlx::query(
                    "INSERT INTO org_members (id, org_id, user_id, role) VALUES ($1, $2, $3, $4)",
                )
                .bind(Uuid::new_v4())
                .bind(invite.foreign_key)
                .bind(user.id)
                .bind(&invite.invitee_role)
                .execute(&mut *dbtx)
                .await
                .map_err(translate_db_error)?;
            }
        }
        dbtx.commit().await?;
        Ok(json!({}))
    }
    .await;
    json_or_err(r)
}

/// POST /dash/invites/decline — legacy team-member-invite-decline-post
/// (:1388-1394).
pub async fn invites_decline(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let user = dash_user(&state, &headers).await?;
        let body = parse_body(&body)?;
        let invite_id = body_uuid(&body, "invite-id")?;
        let invite = invite_by_id(&state, invite_id).await?;
        if invite.invitee_email != user.email {
            return Err(permission_denied("declinable?", json!(invite_id)));
        }
        let sql = format!(
            "UPDATE {t} SET status = 'revoked' WHERE id = $1 AND status = 'pending'",
            t = invite.side.invites_table()
        );
        sqlx::query(&sql)
            .bind(invite_id)
            .execute(&state.pool)
            .await?;
        Ok(json!({}))
    }
    .await;
    json_or_err(r)
}

// ---------------------------------------------------------------------------
// members

struct Member {
    id: Uuid,
    role: DashRole,
}

/// `instant-app-members/get-by-id` + `assert-record! :app-member {:params {:id}}`
async fn member_by_id(state: &AppState, side: Side, foreign_key: Uuid, id: Uuid) -> Result<Member> {
    let sql = format!(
        "SELECT id, {role} AS role FROM {t} WHERE {fk} = $1 AND id = $2",
        role = side.role_column(),
        t = side.members_table(),
        fk = side.fk()
    );
    let row = sqlx::query(&sql)
        .bind(foreign_key)
        .bind(id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| record_not_found(side.member_record(), json!({"params": {"id": id}})))?;
    let role: String = row.get("role");
    Ok(Member {
        id: row.get("id"),
        role: DashRole::parse(&role).unwrap_or(DashRole::Collaborator),
    })
}

/// legacy team-member-update-post (:1465-1502): `id` + `role` before auth;
/// admin; the new role must not exceed the caller's, nor may the member's
/// current role.
async fn members_update(
    state: &AppState,
    headers: &HeaderMap,
    side: Side,
    id_raw: &str,
    body: &Bytes,
) -> Result<Value> {
    let body = parse_body(body)?;
    let member_id = body_uuid(&body, "id")?;
    let role_param = body_str(&body, "role")?;
    let new_role = assert_valid_member_role(&role_param)?;
    let a = actor(state, headers, side, id_raw, DashRole::Admin).await?;
    let member = member_by_id(state, side, a.foreign_key, member_id).await?;
    assert_least_privilege(new_role, Some(a.role))?;
    assert_least_privilege(member.role, Some(a.role))?;
    let sql = format!(
        "UPDATE {t} SET {role} = $1 WHERE id = $2 AND {fk} = $3",
        t = side.members_table(),
        role = side.role_column(),
        fk = side.fk()
    );
    sqlx::query(&sql)
        .bind(&role_param)
        .bind(member.id)
        .bind(a.foreign_key)
        .execute(&state.pool)
        .await
        .map_err(translate_db_error)?;
    Ok(json!({}))
}

pub async fn app_members_update(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    json_or_err(members_update(&state, &headers, Side::App, &app_id, &body).await)
}

pub async fn org_members_update(
    State(state): State<Arc<AppState>>,
    Path(org_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    json_or_err(members_update(&state, &headers, Side::Org, &org_id, &body).await)
}

/// legacy team-member-remove-delete (:1425-1456): `id` before auth;
/// collaborator; the member's role must not exceed the caller's.
async fn members_remove(
    state: &AppState,
    headers: &HeaderMap,
    side: Side,
    id_raw: &str,
    body: &Bytes,
) -> Result<Value> {
    let body = parse_body(body)?;
    let member_id = body_uuid(&body, "id")?;
    let a = actor(state, headers, side, id_raw, DashRole::Collaborator).await?;
    let member = member_by_id(state, side, a.foreign_key, member_id).await?;
    assert_least_privilege(member.role, Some(a.role))?;
    let sql = format!(
        "DELETE FROM {t} WHERE id = $1 AND {fk} = $2",
        t = side.members_table(),
        fk = side.fk()
    );
    sqlx::query(&sql)
        .bind(member.id)
        .bind(a.foreign_key)
        .execute(&state.pool)
        .await
        .map_err(translate_db_error)?;
    Ok(json!({}))
}

pub async fn app_members_remove(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    json_or_err(members_remove(&state, &headers, Side::App, &app_id, &body).await)
}

pub async fn org_members_remove(
    State(state): State<Arc<AppState>>,
    Path(org_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    json_or_err(members_remove(&state, &headers, Side::Org, &org_id, &body).await)
}

// ---------------------------------------------------------------------------
// orgs

/// POST /dash/orgs/:org_id/rename — legacy org-rename-post (:1264-1269), admin.
pub async fn org_rename(
    State(state): State<Arc<AppState>>,
    Path(org_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let a = actor(&state, &headers, Side::Org, &org_id, DashRole::Admin).await?;
        let body = parse_body(&body)?;
        let title = body_str(&body, "title")?;
        sqlx::query("UPDATE orgs SET title = $1 WHERE id = $2")
            .bind(title)
            .bind(a.foreign_key)
            .execute(&state.pool)
            .await
            .map_err(translate_db_error)?;
        Ok(json!({}))
    }
    .await;
    json_or_err(r)
}

/// POST /dash/apps/:app_id/transfer_to_org/:org_id — legacy
/// app-transfer-to-org (:1702-1711) + `org-model/transfer-app-to-org!`
/// (model/org.clj:322-405): owner of the app and admin of the org; app
/// members already on a paid org at an equal-or-better role are dropped;
/// the app moves under the org. No billing here, so `credit` is null.
pub async fn app_transfer_to_org(
    State(state): State<Arc<AppState>>,
    Path((app_id, org_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let app = actor(&state, &headers, Side::App, &app_id, DashRole::Owner).await?;
        let org = actor(&state, &headers, Side::Org, &org_id, DashRole::Admin).await?;
        let row = sqlx::query(
            r#"
            WITH app_members_already_on_paid_org AS (
              SELECT am.* FROM app_members am
                JOIN org_members om ON am.user_id = om.user_id
                JOIN orgs o ON om.org_id = o.id
                JOIN instant_subscriptions org_s ON o.subscription_id = org_s.id
               WHERE am.app_id = $1 AND om.org_id = $2
                 AND org_s.subscription_type_id = 3
                 AND (CASE om.role WHEN 'owner' THEN 1 WHEN 'admin' THEN 2 WHEN 'collaborator' THEN 3 ELSE 999 END)
                  <= (CASE am.member_role WHEN 'owner' THEN 1 WHEN 'admin' THEN 2 WHEN 'collaborator' THEN 3 ELSE 999 END)
            ), app_member_dedupe_deletes AS (
              DELETE FROM app_members WHERE id IN (SELECT id FROM app_members_already_on_paid_org) RETURNING *
            ), app_update AS (
              UPDATE apps a SET creator_id = NULL, org_id = $2 WHERE a.id = $1 RETURNING *
            )
            SELECT (SELECT json_agg(json_build_object('id', m.id,
                       'email', (SELECT email FROM instant_users WHERE instant_users.id = m.user_id),
                       'role', m.member_role))
                      FROM app_member_dedupe_deletes m) AS removed
              FROM app_update
            "#,
        )
        .bind(app.foreign_key)
        .bind(org.foreign_key)
        .fetch_one(&state.pool)
        .await?;
        let removed: Vec<Value> = row
            .get::<Option<Value>, _>("removed")
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default();
        service::invalidate_attrs(&state, app.foreign_key);
        Ok(json!({
            "credit": null,
            "app_member_changes": {
                "removed": removed.into_iter().map(|m| json!({"member": m, "reason": "user_is_member_of_org"})).collect::<Vec<_>>(),
            }
        }))
    }
    .await;
    json_or_err(r)
}

// ---------------------------------------------------------------------------
// ephemeral status + get-a-db lookup

/// POST /dash/apps/ephemeral/:app_id/status — legacy ephemeral_app.clj
/// http-status-post-handler (:104-119): no dashboard auth; the app must
/// belong to the ephemeral creator and the body must carry its admin token.
pub async fn ephemeral_status_post(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    body: Bytes,
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
            return Err(permission_denied("ephemeral-app?", json!(app_id)));
        }
        let body = parse_body(&body)?;
        let token = body_uuid(&body, "admin-token")?;
        let ok = sqlx::query("SELECT 1 AS x FROM app_admin_tokens WHERE app_id = $1 AND token = $2")
            .bind(app_id)
            .bind(token)
            .fetch_optional(&state.pool)
            .await?
            .is_some();
        if !ok {
            return Err(record_not_found(
                "app-admin-token",
                json!({
                    "args": [{"app-id": app_id, "token": token}],
                    "message": "This admin token may be expired or invalid. Or you may have provided an incorrect app ID.",
                }),
            ));
        }
        let raw = body
            .get("status")
            .filter(|v| !v.is_null())
            .ok_or_else(|| param_missing(&["body", "status"]))?;
        let status = match raw.as_str() {
            Some("active") => "active",
            Some("read-only") => "read-only",
            Some("disabled") => "disabled",
            _ => return Err(param_malformed(&["body", "status"], raw.clone())),
        };
        sqlx::query("UPDATE apps SET status = $1 WHERE id = $2")
            .bind(status)
            .bind(app_id)
            .execute(&state.pool)
            .await?;
        service::set_app_status(&state, app_id, status.to_string());
        Ok(json!({"status": status}))
    }
    .await;
    json_or_err(r)
}

/// GET /dash/apps/get_a_db/:app_id — legacy get_a_db.clj http-get-handler
/// (:60-67): unauthenticated; the app must belong to the get-a-db creator.
pub async fn get_a_db_get(
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
        let getadb: Option<Uuid> = sqlx::query("SELECT id FROM instant_users WHERE email = $1")
            .bind(crate::routes::dash_apps::GET_A_DB_CREATOR_EMAIL)
            .fetch_optional(&state.pool)
            .await?
            .map(|r| r.get("id"));
        if creator.is_none() || creator != getadb {
            return Err(permission_denied("claimable-app?", json!(app_id)));
        }
        Ok(json!({"app": app}))
    }
    .await;
    json_or_err(r)
}
