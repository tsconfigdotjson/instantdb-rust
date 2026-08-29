//! /runtime/sse — SSE fallback transport (PROTOCOL.md §1.3). The stream opens
//! with an `sse-init` event; the client POSTs its messages back to the same
//! URL with the machine/session/token envelope.

use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::state::{AppState, Session};

pub async fn stream(State(state): State<Arc<AppState>>) -> Response {
    let session_id = Uuid::new_v4();
    let sse_token = Uuid::new_v4();
    let (tx, mut rx) = mpsc::unbounded_channel::<Value>();
    let session = Arc::new(Session {
        id: session_id,
        tx,
        state: Default::default(),
        refresh_lock: Default::default(),
    });
    {
        let mut st = session.state.lock().await;
        st.sse_token = Some(sse_token);
    }
    state.sessions.insert(session_id, session.clone());

    session.send(json!({
        "op": "sse-init",
        "machine-id": state.node_id,
        "session-id": session_id,
        "sse-token": sse_token,
    }));

    let guard = RxGuard { rx: Some(rx), state: state.clone(), session_id };
    let event_stream = futures::stream::unfold(guard, move |mut guard| async move {
        match guard.rx().recv().await {
            Some(msg) => {
                let event = Event::default().data(msg.to_string());
                Some((Ok::<Event, Infallible>(event), guard))
            }
            None => None,
        }
    });

    Sse::new(event_stream)
        .keep_alive(KeepAlive::new().interval(std::time::Duration::from_secs(15)))
        .into_response()
}

/// Holds the receiver and cleans up the session when the SSE stream drops.
struct RxGuard {
    rx: Option<mpsc::UnboundedReceiver<Value>>,
    state: Arc<AppState>,
    session_id: Uuid,
}

impl RxGuard {
    fn rx(&mut self) -> &mut mpsc::UnboundedReceiver<Value> {
        self.rx.as_mut().unwrap()
    }
}

impl Drop for RxGuard {
    fn drop(&mut self) {
        let state = self.state.clone();
        let session_id = self.session_id;
        tokio::spawn(async move {
            state.drop_session(session_id);
            crate::presence::leave_all(&state, session_id).await;
        });
    }
}

pub async fn push(State(state): State<Arc<AppState>>, Json(body): Json<Value>) -> Response {
    let session_id = body
        .get("session_id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok());
    let sse_token = body
        .get("sse_token")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok());
    let (Some(session_id), Some(sse_token)) = (session_id, sse_token) else {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({"type": "param-missing", "message": "Missing session_id/sse_token"})),
        )
            .into_response();
    };
    let Some(session) = state.sessions.get(&session_id).map(|s| s.clone()) else {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({"type": "record-not-found", "message": "Unknown session"})),
        )
            .into_response();
    };
    {
        let st = session.state.lock().await;
        if st.sse_token != Some(sse_token) {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(json!({"type": "record-not-found", "message": "Invalid sse token"})),
            )
                .into_response();
        }
    }
    if let Some(messages) = body.get("messages").and_then(|m| m.as_array()) {
        for msg in messages {
            crate::ws::handle_message(&state, &session, msg.clone()).await;
        }
    }
    Json(json!({})).into_response()
}
