use serde_json::{json, Value};

/// Instant-style error carrying the wire fields `type`, `message`, `hint`.
/// See LEGACY util/exception.clj — clients pattern-match on `type` and
/// `hint.record-type`, so shapes here must stay stable.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{message}")]
pub struct InstantError {
    pub error_type: String,
    pub message: String,
    pub hint: Option<Value>,
    pub status: u16,
}

impl InstantError {
    pub fn new(
        error_type: &str,
        status: u16,
        message: impl Into<String>,
        hint: Option<Value>,
    ) -> Self {
        Self {
            error_type: error_type.to_string(),
            message: message.into(),
            hint,
            status,
        }
    }

    pub fn validation_failed(input_type: &str, message: impl Into<String>, errors: Value) -> Self {
        let message = message.into();
        Self::new(
            "validation-failed",
            400,
            format!("Validation failed for {}: {}", input_type, message),
            Some(json!({"data-type": input_type, "errors": errors})),
        )
    }

    /// Legacy `throw-validation-err!` (util/exception.clj:391-405) with the
    /// offending `input` in the hint: the message is the bare
    /// "Validation failed for <type>" when no error carries a `message`,
    /// else that plus the messages joined with ", ".
    pub fn validation_failed_input(input_type: &str, input: Value, errors: Value) -> Self {
        let msgs: Vec<&str> = errors
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|e| e.get("message").and_then(|m| m.as_str()))
                    .collect()
            })
            .unwrap_or_default();
        let message = if msgs.is_empty() {
            format!("Validation failed for {input_type}")
        } else {
            format!("Validation failed for {input_type}: {}", msgs.join(", "))
        };
        Self::new(
            "validation-failed",
            400,
            message,
            Some(json!({"data-type": input_type, "input": input, "errors": errors})),
        )
    }

    /// Legacy `assert-record!` (util/exception.clj:125-135): the lookup
    /// args ride in the hint next to `record-type`.
    pub fn record_not_found_args(record_type: &str, args: Value) -> Self {
        Self::new(
            "record-not-found",
            400,
            format!("Record not found: {record_type}"),
            Some(json!({"args": [args], "record-type": record_type})),
        )
    }

    pub fn record_not_found(record_type: &str, message: impl Into<String>) -> Self {
        Self::new(
            "record-not-found",
            400,
            message,
            Some(json!({"record-type": record_type})),
        )
    }

    pub fn record_not_unique(
        etype: &str,
        label: &str,
        attr_id: Option<&str>,
        value: Option<&Value>,
    ) -> Self {
        Self::new(
            "record-not-unique",
            400,
            format!(
                "`{label}` is a unique attribute on `{etype}` and an entity already exists with `{etype}.{label}` = {}",
                value.map(|v| v.to_string()).unwrap_or_else(|| "<value>".to_string())
            ),
            Some(json!({
                "record-type": "triples",
                "attr-id": attr_id,
                "etype": etype,
                "label": label,
                "value": value,
            })),
        )
    }

    pub fn permission_denied(input: Value, message: impl Into<String>) -> Self {
        Self::new(
            "permission-denied",
            400,
            message,
            Some(json!({"input": input, "expected": "perms-pass?"})),
        )
    }

    pub fn param_missing(message: impl Into<String>) -> Self {
        Self::new("param-missing", 400, message, None)
    }

    pub fn param_malformed(message: impl Into<String>) -> Self {
        Self::new("param-malformed", 400, message, None)
    }

    /// Errors legacy surfaces as Postgres RAISEs (util/exception.clj:770-775):
    /// type `sql-raise`, status 400, message "Raised Exception: ...", hint
    /// carrying the raised text as `constraint`.
    pub fn sql_raise(server_message: impl Into<String>) -> Self {
        let m = server_message.into();
        Self::new(
            "sql-raise",
            400,
            format!("Raised Exception: {m}"),
            Some(json!({"table": null, "condition": "raise-exception", "constraint": m})),
        )
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new("internal-error", 500, message, None)
    }

    /// 429 rate-limited (util/exception.clj:484-509): hint carries a
    /// machine-readable `retry-after` in whole seconds, rounded up.
    pub fn rate_limited(message: impl Into<String>, retry_after_secs: u64) -> Self {
        Self::new(
            "rate-limited",
            429,
            message,
            Some(json!({"retry-after": retry_after_secs})),
        )
    }

    /// JSON body for HTTP error responses / fields for ws `error` op.
    pub fn to_body(&self) -> Value {
        let mut body = json!({"type": self.error_type, "message": self.message});
        if let Some(h) = &self.hint {
            body["hint"] = h.clone();
        }
        body
    }
}

impl From<sqlx::Error> for InstantError {
    fn from(e: sqlx::Error) -> Self {
        if let sqlx::Error::Database(db) = &e {
            if db.code().as_deref() == Some("23505") {
                // Unique violations are translated by callers that know the attr;
                // this is the generic fallback.
                return InstantError::new(
                    "record-not-unique",
                    400,
                    "Record not unique",
                    Some(json!({"record-type": "triples", "constraint": db.constraint()})),
                );
            }
        }
        InstantError::internal(format!("database error: {e}"))
    }
}

pub type Result<T> = std::result::Result<T, InstantError>;
