//! App-level end-user auth: refresh tokens, users (stored as triples in the
//! $users / $userRefreshTokens system namespaces).

use instant_core::error::{InstantError, Result};
use instant_core::system_catalog as sc;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use sqlx::Row;
use uuid::Uuid;

use crate::state::AppState;

/// hex(sha256(uuid-16-bytes)) — matches legacy refresh-token hashing.
pub fn hash_token(token: Uuid) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

/// hex(sha256(utf8 string)) — for magic codes and oauth state hashes.
pub fn hash_string(s: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(s.as_bytes());
    hex::encode(hasher.finalize())
}

mod hex {
    pub fn encode(bytes: impl AsRef<[u8]>) -> String {
        bytes.as_ref().iter().map(|b| format!("{b:02x}")).collect()
    }
}

#[derive(Debug, Clone)]
pub struct AppUser {
    pub id: Uuid,
    pub email: Option<String>,
}

/// Look up the user for a refresh token (token = plaintext uuid).
pub async fn user_by_refresh_token(
    state: &AppState,
    app_id: Uuid,
    token: &str,
) -> Result<Option<AppUser>> {
    let Ok(token) = Uuid::parse_str(token) else {
        return Ok(None);
    };
    let hashed = hash_token(token);
    let hashed_attr = sc::attr_id("$userRefreshTokens", "hashedToken");
    let user_attr = sc::attr_id("$userRefreshTokens", "$user");
    let email_attr = sc::attr_id("$users", "email");
    let row = sqlx::query(
        r#"
        SELECT json_uuid_to_uuid(u.value) AS user_id, e.value AS email
        FROM triples t
        JOIN triples u ON u.app_id = t.app_id AND u.entity_id = t.entity_id AND u.attr_id = $3 AND u.eav
        LEFT JOIN triples e ON e.app_id = t.app_id
              AND e.entity_id = json_uuid_to_uuid(u.value) AND e.attr_id = $4
        WHERE t.app_id = $1 AND t.attr_id = $2 AND t.av AND t.value = to_jsonb($5::text)
        LIMIT 1
        "#,
    )
    .bind(app_id)
    .bind(hashed_attr)
    .bind(user_attr)
    .bind(email_attr)
    .bind(&hashed)
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    Ok(row.and_then(|r| {
        let user_id: Option<Uuid> = r.get("user_id");
        let email: Option<Value> = r.try_get("email").ok();
        user_id.map(|id| AppUser {
            id,
            email: email.and_then(|v| v.as_str().map(|s| s.to_string())),
        })
    }))
}

/// Find a user by email.
pub async fn user_by_email(state: &AppState, app_id: Uuid, email: &str) -> Result<Option<AppUser>> {
    let email_attr = sc::attr_id("$users", "email");
    let row = sqlx::query(
        "SELECT entity_id FROM triples
         WHERE app_id = $1 AND attr_id = $2 AND av AND value = to_jsonb($3::text) LIMIT 1",
    )
    .bind(app_id)
    .bind(email_attr)
    .bind(email)
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    Ok(row.map(|r| AppUser {
        id: r.get("entity_id"),
        email: Some(email.to_string()),
    }))
}

pub async fn user_by_id(state: &AppState, app_id: Uuid, id: Uuid) -> Result<Option<AppUser>> {
    let id_attr = sc::attr_id("$users", "id");
    let email_attr = sc::attr_id("$users", "email");
    let row = sqlx::query(
        "SELECT e.value AS email FROM triples t
         LEFT JOIN triples e ON e.app_id = t.app_id AND e.entity_id = t.entity_id AND e.attr_id = $3
         WHERE t.app_id = $1 AND t.attr_id = $2 AND t.entity_id = $4 LIMIT 1",
    )
    .bind(app_id)
    .bind(id_attr)
    .bind(email_attr)
    .bind(id)
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    Ok(row.map(|r| {
        let email: Option<Value> = r.try_get("email").ok();
        AppUser {
            id,
            email: email.and_then(|v| v.as_str().map(|s| s.to_string())),
        }
    }))
}

pub async fn mint_refresh_token(state: &AppState, app_id: Uuid, user_id: Uuid) -> Result<Uuid> {
    let token = Uuid::new_v4();
    let entity = Uuid::new_v4();
    let steps = json!([
        [
            "add-triple",
            entity,
            sc::attr_id("$userRefreshTokens", "id"),
            entity
        ],
        [
            "add-triple",
            entity,
            sc::attr_id("$userRefreshTokens", "hashedToken"),
            hash_token(token)
        ],
        [
            "add-triple",
            entity,
            sc::attr_id("$userRefreshTokens", "$user"),
            user_id
        ]
    ]);
    crate::service::run_system_transact(state, app_id, &steps).await?;
    Ok(token)
}

/// Delete all refresh tokens for a user (sign out) or one token.
pub async fn sign_out(
    state: &AppState,
    app_id: Uuid,
    user_id: Option<Uuid>,
    token: Option<&str>,
) -> Result<()> {
    let mut entity_ids: Vec<Uuid> = vec![];
    if let Some(token) = token.and_then(|t| Uuid::parse_str(t).ok()) {
        let hashed = hash_token(token);
        let rows = sqlx::query(
            "SELECT entity_id FROM triples
             WHERE app_id = $1 AND attr_id = $2 AND av AND value = to_jsonb($3::text)",
        )
        .bind(app_id)
        .bind(sc::attr_id("$userRefreshTokens", "hashedToken"))
        .bind(&hashed)
        .fetch_all(&state.pool)
        .await
        .map_err(InstantError::from)?;
        entity_ids.extend(rows.iter().map(|r| r.get::<Uuid, _>("entity_id")));
    }
    if let Some(uid) = user_id {
        let rows = sqlx::query(
            "SELECT entity_id FROM triples
             WHERE app_id = $1 AND attr_id = $2 AND eav AND json_uuid_to_uuid(value) = $3",
        )
        .bind(app_id)
        .bind(sc::attr_id("$userRefreshTokens", "$user"))
        .bind(uid)
        .fetch_all(&state.pool)
        .await
        .map_err(InstantError::from)?;
        entity_ids.extend(rows.iter().map(|r| r.get::<Uuid, _>("entity_id")));
    }
    if entity_ids.is_empty() {
        return Ok(());
    }
    let steps: Vec<Value> = entity_ids
        .iter()
        .map(|e| json!(["delete-entity", e, "$userRefreshTokens"]))
        .collect();
    crate::service::run_system_transact(state, app_id, &Value::Array(steps)).await?;
    Ok(())
}

/// `$users.type` of a user, when set.
pub async fn user_type(state: &AppState, app_id: Uuid, user_id: Uuid) -> Result<Option<String>> {
    let row = sqlx::query(
        "SELECT value FROM triples WHERE app_id = $1 AND entity_id = $2 AND attr_id = $3 LIMIT 1",
    )
    .bind(app_id)
    .bind(user_id)
    .bind(sc::attr_id("$users", "type"))
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    Ok(row.and_then(|r| r.get::<Value, _>("value").as_str().map(|s| s.to_string())))
}

/// A guest user for a refresh token, if the token belongs to one
/// (legacy runtime/routes.clj:99-104 `(when (= "guest" (:type user)) user)`).
pub async fn guest_by_refresh_token(
    state: &AppState,
    app_id: Uuid,
    token: &str,
) -> Result<Option<AppUser>> {
    let Some(user) = user_by_refresh_token(state, app_id, token).await? else {
        return Ok(None);
    };
    Ok((user_type(state, app_id, user.id).await?.as_deref() == Some("guest")).then_some(user))
}

/// Legacy `assert-signup!` / `assert-create-permission!` (model/app_user.clj:51-85):
/// when the app defines `$users.allow.create` (that exact path — no
/// `$default` fallback), a new user must pass it. `data`, `newData` and
/// `auth` are all the prospective user `{id, email}`.
/// Legacy `validate-extra-fields!` (model/app_user.clj:22-41): every
/// signup `extra-fields` key must be a `$users` attr and not a system one.
pub fn validate_extra_fields(
    attrs: &instant_core::attr::AttrMap,
    extra_fields: Option<&Map<String, Value>>,
) -> Result<()> {
    let Some(extra) = extra_fields else {
        return Ok(());
    };
    let err = |message: String| {
        InstantError::new(
            "validation-failed",
            400,
            format!("Validation failed for extra-fields: {message}"),
            Some(json!({
                "data-type": "extra-fields",
                "input": extra,
                "errors": [{"message": message}],
            })),
        )
    };
    for k in extra.keys() {
        match attrs.by_fwd_name("$users", k) {
            None => {
                return Err(err(format!(
                    "Unknown field: {k}. It must be defined in your $users schema."
                )))
            }
            Some(a) if a.is_system => {
                return Err(err(format!("Cannot set system field: {k}")));
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// The `add-triple` steps that write signup `extra-fields` onto a new
/// `$users` row (app_user.clj:88-109 `create!`).
pub fn extra_field_steps(
    attrs: &instant_core::attr::AttrMap,
    user_id: Uuid,
    extra_fields: Option<&Map<String, Value>>,
) -> Vec<Value> {
    let mut steps = vec![];
    if let Some(extra) = extra_fields {
        for (k, v) in extra {
            if let Some(a) = attrs.by_fwd_name("$users", k) {
                steps.push(json!(["add-triple", user_id, a.id, v]));
            }
        }
    }
    steps
}

/// Legacy `assert-signup!` (model/app_user.clj:43-86): validates
/// `extra-fields` against the `$users` schema, then — unless the caller is
/// an admin flow (`skip_perm_check`) — checks `$users.allow.create` with the
/// prospective user (id, email and the extra fields) bound as `data`,
/// `newData` and `auth`. Extra fields without an explicit create rule are
/// denied.
pub async fn assert_signup(
    state: &AppState,
    app_id: Uuid,
    user_id: Uuid,
    email: Option<&str>,
    extra_fields: Option<&Map<String, Value>>,
    skip_perm_check: bool,
) -> Result<()> {
    let attrs = crate::service::load_attrs(state, app_id).await?;
    validate_extra_fields(&attrs, extra_fields)?;
    if skip_perm_check {
        return Ok(());
    }
    let has_extra = extra_fields.map(|m| !m.is_empty()).unwrap_or(false);
    let mut conn = state.pool.acquire().await.map_err(InstantError::from)?;
    let rules = instant_core::perms::Rules::load(&mut conn, app_id).await?;
    let defined = rules
        .code
        .get("$users")
        .and_then(|ns| ns.get("allow"))
        .and_then(|a| a.get("create"))
        .is_some();
    if !defined {
        if has_extra {
            // app_user.clj:70-75: extra fields need an explicit create rule
            return Err(InstantError::permission_denied(
                json!(["$users", "create"]),
                "Permission denied: not perms-pass?",
            ));
        }
        return Ok(());
    }
    let program = rules.program("$users", "create");
    let mut user_data = json!({"id": user_id});
    if let Some(email) = email {
        user_data["email"] = json!(email);
    }
    if let Some(extra) = extra_fields {
        for (k, v) in extra {
            user_data[k.as_str()] = v.clone();
        }
    }
    let request = instant_core::perms::RequestCtx::default().with_pool(state.pool.clone());
    let env = instant_core::perms::EvalEnv::new(app_id, &rules, &request);
    let ok = instant_core::perms::eval_program(
        &program,
        &user_data,
        Some(&user_data),
        &user_data,
        &json!({}),
        &env,
    )
    .await?;
    if !ok {
        return Err(InstantError::permission_denied(
            json!(["$users", "create"]),
            "Permission denied: not perms-pass?",
        ));
    }
    Ok(())
}

/// `extra-fields` / `extra_fields` from a request body: a JSON object, or
/// nothing.
pub fn extra_fields_of<'a>(body: &'a Value, key: &str) -> Option<&'a Map<String, Value>> {
    body.get(key).and_then(|v| v.as_object())
}

/// Verify an admin token for an app.
pub async fn check_admin_token(state: &AppState, app_id: Uuid, token: &str) -> Result<bool> {
    let Ok(token) = Uuid::parse_str(token) else {
        return Ok(false);
    };
    let row = sqlx::query("SELECT 1 AS x FROM app_admin_tokens WHERE app_id = $1 AND token = $2")
        .bind(app_id)
        .bind(token)
        .fetch_optional(&state.pool)
        .await
        .map_err(InstantError::from)?;
    Ok(row.is_some())
}
