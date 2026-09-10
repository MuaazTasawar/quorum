use crate::cluster::{ClientCommand, ClientCommandResult, ClientQuery, ClusterHandle, ClusterSnapshot, QueryResult};
use crate::errors::AppError;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, put};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use storage::Command;
use tokio::sync::{mpsc, oneshot, watch};

#[derive(Clone)]
pub struct AppState {
    pub command_tx: mpsc::UnboundedSender<ClientCommand>,
    pub query_tx: mpsc::UnboundedSender<ClientQuery>,
    pub snapshot_rx: watch::Receiver<ClusterSnapshot>,
}

impl AppState {
    pub fn from_handle(handle: ClusterHandle) -> Self {
        Self {
            command_tx: handle.command_tx,
            query_tx: handle.query_tx,
            snapshot_rx: handle.snapshot_rx,
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

#[derive(Serialize)]
struct GetResponse {
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

    match receiver.await.map_err(|_| AppError::ChannelClosed)? {
        // Values are stored as raw bytes; base64 would be more correct for
        // arbitrary binary data, but lossy UTF-8 keeps this endpoint simple
        // for the demo/dashboard use case (text values).
        QueryResult::Value(v) => Ok(Json(GetResponse {
            value: v.map(|bytes| String::from_utf8_lossy(&bytes).to_string()),
            leader_hint: None,
        })),
        QueryResult::NotLeader { leader_hint } => Ok(Json(GetResponse {
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
    applied: bool,
    leader_hint: Option<u32>,
}

async fn put_key(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<PutBody>,
) -> Result<Json<WriteResponse>, AppError> {
    let (respond_to, receiver) = oneshot::channel();
    state
        .command_tx
        .send(ClientCommand {
            command: Command::Put { key, value: body.value.into_bytes() },
            respond_to,
        })
        .map_err(|_| AppError::ChannelClosed)?;

    match receiver.await.map_err(|_| AppError::ChannelClosed)? {
        ClientCommandResult::Applied(_) => Ok(Json(WriteResponse { applied: true, leader_hint: None })),
        ClientCommandResult::NotLeader { leader_hint } => {
            Ok(Json(WriteResponse { applied: false, leader_hint }))
        }
    }
}

async fn delete_key(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Json<WriteResponse>, AppError> {
    let (respond_to, receiver) = oneshot::channel();
    state
        .command_tx
        .send(ClientCommand { command: Command::Delete { key }, respond_to })
        .map_err(|_| AppError::ChannelClosed)?;

    match receiver.await.map_err(|_| AppError::ChannelClosed)? {
        ClientCommandResult::Applied(_) => Ok(Json(WriteResponse { applied: true, leader_hint: None })),
        ClientCommandResult::NotLeader { leader_hint } => {
            Ok(Json(WriteResponse { applied: false, leader_hint }))
        }
    }
}

/// WebSocket endpoint the dashboard connects to for a live cluster-state
/// stream. This is what makes the "kill the leader, watch election happen"
/// demo possible - pushes a fresh ClusterSnapshot every time the cluster's
/// watch channel changes, no polling.
async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(move |socket| stream_snapshots(socket, state.snapshot_rx))
}

async fn stream_snapshots(mut socket: WebSocket, mut snapshot_rx: watch::Receiver<ClusterSnapshot>) {
    // Send the current state immediately on connect, don't make the client
    // wait for the next change to see anything.
    let initial = snapshot_rx.borrow().clone();
    if send_snapshot(&mut socket, &initial).await.is_err() {
        return;
    }

    loop {
        if snapshot_rx.changed().await.is_err() {
            return; // sender side dropped - node shutting down
        }
        let snapshot = snapshot_rx.borrow().clone();
        if send_snapshot(&mut socket, &snapshot).await.is_err() {
            return; // client disconnected
        }
    }
}

async fn send_snapshot(socket: &mut WebSocket, snapshot: &ClusterSnapshot) -> Result<(), axum::Error> {
    let payload = serde_json::to_string(snapshot).unwrap_or_default();
    socket.send(Message::Text(payload.into())).await
}