//! Magic-code email delivery (docs/AUTH.md §4, legacy magic_code_auth.clj).
//!
//! Delivery is a provider seam selected by `EMAIL_PROVIDER`:
//!   - `log` (default): print the code to the server log, as before.
//!   - `cloudflare`: send via the Cloudflare Email Service REST API
//!     (POST /accounts/{account_id}/email/sending/send), configured with
//!     `CLOUDFLARE_ACCOUNT_ID` + `CLOUDFLARE_API_TOKEN`.
//!
//! The send path honors per-app `app_email_templates` (email_type
//! 'magic-code', placeholders {code} {app_title} {user_email} {expiration})
//! and `app_email_senders` (custom sender only when verified), falling back
//! to the legacy default subject/body. Delivery is fire-and-forget: the
//! HTTP response is `{"sent": true}` no matter what happens here.

use std::sync::Arc;

use sqlx::Row;
use uuid::Uuid;

use crate::state::AppState;

pub enum EmailProvider {
    Log,
    Cloudflare {
        account_id: String,
        api_token: String,
        client: reqwest::Client,
    },
}

pub struct EmailConfig {
    pub provider: EmailProvider,
    /// Default (always-verified) sender address, same env var as legacy.
    pub default_sender_email: String,
}

impl EmailConfig {
    pub fn from_env() -> Self {
        let default_sender_email = std::env::var("INSTANT_APP_EMAIL_SENDER_EMAIL")
            .unwrap_or_else(|_| "verify@auth-pm.instantdb.com".into());
        let provider = match std::env::var("EMAIL_PROVIDER").as_deref() {
            Ok("cloudflare") => {
                let account_id = std::env::var("CLOUDFLARE_ACCOUNT_ID").unwrap_or_default();
                let api_token = std::env::var("CLOUDFLARE_API_TOKEN").unwrap_or_default();
                if account_id.is_empty() || api_token.is_empty() {
                    tracing::warn!(
                        "EMAIL_PROVIDER=cloudflare but CLOUDFLARE_ACCOUNT_ID or \
                         CLOUDFLARE_API_TOKEN is unset; falling back to log-only delivery"
                    );
                    EmailProvider::Log
                } else {
                    EmailProvider::Cloudflare {
                        account_id,
                        api_token,
                        client: reqwest::Client::new(),
                    }
                }
            }
            Ok(other) if other != "log" => {
                tracing::warn!("Unknown EMAIL_PROVIDER {other:?}; using log-only delivery");
                EmailProvider::Log
            }
            _ => EmailProvider::Log,
        };
        EmailConfig {
            provider,
            default_sender_email,
        }
    }
}

struct RenderedEmail {
    sender_name: String,
    sender_email: String,
    to: String,
    subject: String,
    html: String,
}

/// Kick off delivery of a magic-code email. Never blocks the caller on the
/// network and never fails the request: template lookup + send run in a
/// spawned task and errors are only logged.
pub fn deliver_magic_code(
    state: &Arc<AppState>,
    app_id: Uuid,
    app_title: &str,
    email: &str,
    code: &str,
) {
    // Log-only mode keeps the exact legacy-stub behavior, synchronously.
    if matches!(state.email.provider, EmailProvider::Log) {
        tracing::info!("magic code for {email} (app {app_id}): {code}");
        println!("MAGIC CODE for {email}: {code}");
        return;
    }
    let state = state.clone();
    let app_title = app_title.to_string();
    let email = email.to_string();
    let code = code.to_string();
    tokio::spawn(async move {
        if let Err(e) = send_magic_code_email(&state, app_id, &app_title, &email, &code).await {
            tracing::warn!("magic-code email to {email} (app {app_id}) failed: {e}");
        }
    });
}

async fn send_magic_code_email(
    state: &AppState,
    app_id: Uuid,
    app_title: &str,
    email: &str,
    code: &str,
) -> Result<(), String> {
    let msg = render_magic_code_email(state, app_id, app_title, email, code)
        .await
        .map_err(|e| format!("render failed: {e}"))?;
    let default_sender = &state.email.default_sender_email;
    match send(state, &msg).await {
        Ok(()) => Ok(()),
        // Legacy falls back to the default sender when the custom sender is
        // rejected by the provider; retry once the same way.
        Err(e) if msg.sender_email != *default_sender => {
            tracing::warn!(
                "magic-code email via sender {} failed ({e}); retrying with default sender",
                msg.sender_email
            );
            send(
                state,
                &RenderedEmail {
                    sender_email: default_sender.clone(),
                    sender_name: msg.sender_name.clone(),
                    to: msg.to.clone(),
                    subject: msg.subject.clone(),
                    html: msg.html.clone(),
                },
            )
            .await
        }
        Err(e) => Err(e),
    }
}

async fn render_magic_code_email(
    state: &AppState,
    app_id: Uuid,
    app_title: &str,
    email: &str,
    code: &str,
) -> Result<RenderedEmail, sqlx::Error> {
    let expiration = friendly_expiration(magic_code_expiry_minutes(state, app_id).await?);
    let template = sqlx::query(
        "SELECT t.subject, t.body, t.name,
                s.email AS sender_email,
                coalesce(v.verified, false) AS verified
         FROM app_email_templates t
         LEFT JOIN app_email_senders s ON t.sender_id = s.id
         LEFT JOIN app_email_verifications v
           ON v.sender_id = t.sender_id AND v.app_id = t.app_id
         WHERE t.app_id = $1 AND t.email_type = 'magic-code'",
    )
    .bind(app_id)
    .fetch_optional(&state.pool)
    .await?;

    let params: [(&str, &str); 4] = [
        ("code", code),
        ("app_title", app_title),
        ("user_email", email),
        ("expiration", &expiration),
    ];
    let default_sender = &state.email.default_sender_email;

    let (sender_name, sender_email, subject, html) = match template {
        Some(t) => {
            let custom_email: Option<String> = t.get("sender_email");
            let verified: bool = t.get("verified");
            let sender_email = match custom_email {
                Some(e) if !e.is_empty() && verified => e,
                _ => default_sender.clone(),
            };
            let name: Option<String> = t.get("name");
            (
                name.unwrap_or_else(|| app_title.to_string()),
                sender_email,
                template_replace(t.get::<String, _>("subject").as_str(), &params, false),
                template_replace(t.get::<String, _>("body").as_str(), &params, true),
            )
        }
        None => (
            app_title.to_string(),
            default_sender.clone(),
            format!("{code} is your verification code for {app_title}"),
            default_body(&params),
        ),
    };
    Ok(RenderedEmail {
        sender_name,
        sender_email,
        to: email.to_string(),
        subject,
        html,
    })
}

async fn magic_code_expiry_minutes(state: &AppState, app_id: Uuid) -> Result<i64, sqlx::Error> {
    let row = sqlx::query(
        "SELECT coalesce(magic_code_expiry_minutes, 1440) AS m FROM apps WHERE id = $1",
    )
    .bind(app_id)
    .fetch_optional(&state.pool)
    .await?;
    Ok(row.map(|r| r.get::<i32, _>("m") as i64).unwrap_or(1440))
}

/// "<n> hour(s)" when >= 60 minutes, else "<n> minute(s)" (legacy
/// friendly-expiration).
fn friendly_expiration(minutes: i64) -> String {
    if minutes >= 60 {
        let hours = minutes / 60;
        format!("{hours} hour{}", if hours > 1 { "s" } else { "" })
    } else {
        format!("{minutes} minute{}", if minutes > 1 { "s" } else { "" })
    }
}

fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

/// Replace `{key}` placeholders; values are html-escaped for bodies
/// (legacy template-replace with escape?).
fn template_replace(template: &str, params: &[(&str, &str)], escape: bool) -> String {
    let mut out = template.to_string();
    for (k, v) in params {
        let val = if escape {
            html_escape(v)
        } else {
            (*v).to_string()
        };
        out = out.replace(&format!("{{{k}}}"), &val);
    }
    out
}

/// Legacy default magic-code body (magic_code_auth.clj default-body inside
/// util.email standard-body).
fn default_body(params: &[(&str, &str)]) -> String {
    let inner = template_replace(
        "<p><strong>Welcome,</strong></p>\
         <p>You asked to join {app_title}. To complete your registration, use this \
         verification code:</p>\
         <h2 style=\"text-align: center\"><strong>{code}</strong></h2>\
         <p>Copy and paste this into the confirmation box, and you'll be on your way.</p>\
         <p>Note: This code will expire in {expiration}, and can only be used once. If you \
         didn't request this code, please reply to this email.</p>",
        params,
        true,
    );
    format!(
        "<div style='background:#f6f6f6;font-family:Helvetica,Arial,sans-serif;\
         line-height:1.6;font-size:18px'>\
         <div style='max-width:650px;margin:0 auto;background:white;padding:20px'>{inner}</div></div>"
    )
}

async fn send(state: &AppState, msg: &RenderedEmail) -> Result<(), String> {
    match &state.email.provider {
        EmailProvider::Log => {
            tracing::info!("email (log provider) to {}: {}", msg.to, msg.subject);
            Ok(())
        }
        EmailProvider::Cloudflare {
            account_id,
            api_token,
            client,
        } => {
            let url = format!(
                "https://api.cloudflare.com/client/v4/accounts/{account_id}/email/sending/send"
            );
            let body = serde_json::json!({
                "from": {"address": msg.sender_email, "name": msg.sender_name},
                "to": {"address": msg.to},
                "reply_to": msg.sender_email,
                "subject": msg.subject,
                "html": msg.html,
            });
            let resp = client
                .post(&url)
                .bearer_auth(api_token)
                .json(&body)
                .send()
                .await
                .map_err(|e| format!("cloudflare request error: {e}"))?;
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            if !status.is_success() {
                return Err(format!("cloudflare send failed ({status}): {text}"));
            }
            // Cloudflare wraps errors in a standard envelope; success:false
            // can in principle ride a 200, so check it too.
            let ok = serde_json::from_str::<serde_json::Value>(&text)
                .ok()
                .and_then(|v| v.get("success").and_then(|s| s.as_bool()))
                .unwrap_or(true);
            if !ok {
                return Err(format!("cloudflare send failed: {text}"));
            }
            tracing::info!("magic-code email sent to {} via cloudflare", msg.to);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn friendly_expiration_matches_legacy() {
        assert_eq!(friendly_expiration(1), "1 minute");
        assert_eq!(friendly_expiration(15), "15 minutes");
        assert_eq!(friendly_expiration(60), "1 hour");
        assert_eq!(friendly_expiration(90), "1 hour");
        assert_eq!(friendly_expiration(120), "2 hours");
        assert_eq!(friendly_expiration(1440), "24 hours");
    }

    #[test]
    fn template_replace_escapes_body_params() {
        let params = [("code", "123456"), ("app_title", "A <b>& B")];
        assert_eq!(
            template_replace("{code} for {app_title}", &params, false),
            "123456 for A <b>& B"
        );
        assert_eq!(
            template_replace("{code} for {app_title}", &params, true),
            "123456 for A &lt;b&gt;&amp; B"
        );
    }

    #[test]
    fn default_body_renders_all_params() {
        let params = [
            ("code", "123456"),
            ("app_title", "My App"),
            ("user_email", "a@b.com"),
            ("expiration", "24 hours"),
        ];
        let body = default_body(&params);
        assert!(body.contains("<strong>123456</strong>"));
        assert!(body.contains("You asked to join My App."));
        assert!(body.contains("expire in 24 hours"));
    }
}
