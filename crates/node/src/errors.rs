use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

/// Errors the HTTP layer can return. Kept deliberately small - most "errors"
/// a client sees (like NotLeader) aren't failures of the API itself, they're
/// normal cluster states, so they get 200-with-redirect-info instead of a
/// 4xx/5xx here. This type is for actual API-level problems.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("request channel closed - node is shutting down")]
    ChannelClosed,
    #[error("invalid key: {0}")]
    InvalidKey(String),
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match &self {
            AppError::ChannelClosed => StatusCode::SERVICE_UNAVAILABLE,
            AppError::InvalidKey(_) => StatusCode::BAD_REQUEST,
        };
        (status, Json(ErrorBody { error: self.to_string() })).into_response()
    }
}