use crate::cluster::{
    ClientCommand, ClientCommandResult, ClientQuery, ClusterHandle, ClusterSnapshot, QueryResult,
};
use crate::errors::AppError;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, put};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use storage::Command;
use tokio::sync::{mpsc, oneshot, watch};

#[derive(Clone)]
pub struct AppState {
    pub command_tx: mpsc::UnboundedSender<ClientCommand>,
    pub query_tx: mpsc::UnboundedSender<ClientQuery>,
    pub snapshot_rx: watch::Receiver<ClusterSnapshot>,
    /// Upper bound on how long one request waits for the cluster to answer.
    pub request_timeout: Duration,
}

impl AppState {
    pub fn from_handle(handle: ClusterHandle, request_timeout: Duration) -> Self {
        Self {
            command_tx: handle.command_tx,
            query_tx: handle.query_tx,
            snapshot_rx: handle.snapshot_rx,
            request_timeout,
        }
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/kv/{key}", get(get_key))
        .route("/kv/{key}", put(put_key))
        .route("/kv/{key}", delete(delete_key))
        .route("/ws", get(ws_handler))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.snapshot_rx.borrow().clone())
}

/// Waits for the cluster's answer, but never longer than `timeout`.
///
/// Without a deadline, a request to a leader that cannot reach a majority would
/// sit here until the client's own connection timeout fired, and the client
/// would learn nothing. `may_have_applied` says whether giving up here leaves
/// the outcome genuinely unknown (true for writes, false for reads).
async fn await_reply<T>(
    receiver: oneshot::Receiver<T>,
    timeout: Duration,
    may_have_applied: bool,
) -> Result<T, AppError> {
    match tokio::time::timeout(timeout, receiver).await {
        Ok(Ok(reply)) => Ok(reply),
        Ok(Err(_)) => Err(AppError::ChannelClosed),
        Err(_) => Err(AppError::Timeout { may_have_applied }),
    }
}

/// `is_leader` is the field clients must check first - `value: None` alone is
/// ambiguous (could mean "key doesn't exist" on a real leader, or "you hit a
/// follower with no idea who the leader is"). `is_leader` disambiguates that
/// before the client ever looks at `value`.
#[derive(Serialize)]
struct GetResponse {
    is_leader: bool,
    value: Option<String>,
    leader_hint: Option<u32>,
}

async fn get_key(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Json<GetResponse>, AppError> {
    let (respond_to, receiver) = oneshot::channel();
    state
        .query_tx
        .send(ClientQuery { key, respond_to })
        .map_err(|_| AppError::ChannelClosed)?;

    match await_reply(receiver, state.request_timeout, false).await? {
        QueryResult::Value(v) => Ok(Json(GetResponse {
            is_leader: true,
            value: v.map(|bytes| String::from_utf8_lossy(&bytes).to_string()),
            leader_hint: None,
        })),
        QueryResult::NotLeader { leader_hint } => Ok(Json(GetResponse {
            is_leader: false,
            value: None,
            leader_hint,
        })),
    }
}

#[derive(Deserialize)]
struct PutBody {
    value: String,
}

#[derive(Serialize)]
struct WriteResponse {
    is_leader: bool,
    applied: bool,
    leader_hint: Option<u32>,
}

/// Submits a write and waits for it to be committed and applied. Shared by PUT
/// and DELETE so both have identical redirect, timeout and leadership-loss
/// behaviour.
async fn submit_write(state: &AppState, command: Command) -> Result<Json<WriteResponse>, AppError> {
    let (respond_to, receiver) = oneshot::channel();
    state
        .command_tx
        .send(ClientCommand { command, respond_to })
        .map_err(|_| AppError::ChannelClosed)?;

    match await_reply(receiver, state.request_timeout, true).await? {
        ClientCommandResult::Applied(_) => {
            Ok(Json(WriteResponse { is_leader: true, applied: true, leader_hint: None }))
        }
        ClientCommandResult::NotLeader { leader_hint } => {
            Ok(Json(WriteResponse { is_leader: false, applied: false, leader_hint }))
        }
        ClientCommandResult::LeadershipLost => Err(AppError::LeadershipLost),
    }
}

async fn put_key(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<PutBody>,
) -> Result<Json<WriteResponse>, AppError> {
    submit_write(&state, Command::Put { key, value: body.value.into_bytes() }).await
}

async fn delete_key(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Json<WriteResponse>, AppError> {
    submit_write(&state, Command::Delete { key }).await
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(move |socket| stream_snapshots(socket, state.snapshot_rx))
}

async fn stream_snapshots(mut socket: WebSocket, mut snapshot_rx: watch::Receiver<ClusterSnapshot>) {
    let initial = snapshot_rx.borrow().clone();
    if send_snapshot(&mut socket, &initial).await.is_err() {
        return;
    }

    loop {
        if snapshot_rx.changed().await.is_err() {
            return;
        }
        let snapshot = snapshot_rx.borrow().clone();
        if send_snapshot(&mut socket, &snapshot).await.is_err() {
            return;
        }
    }
}

async fn send_snapshot(socket: &mut WebSocket, snapshot: &ClusterSnapshot) -> Result<(), axum::Error> {
    let payload = serde_json::to_string(snapshot).unwrap_or_default();
    socket.send(Message::Text(payload.into())).await
}