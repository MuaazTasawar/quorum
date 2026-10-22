use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

/// Errors the HTTP layer can return. Most "errors" a client sees (like
/// NotLeader) aren't failures of the API itself, they're normal cluster states,
/// so they get 200-with-redirect-info instead of a 4xx/5xx here. This type is
/// for the cases where the request genuinely could not be completed.
///
/// The two cases that matter most for writes are `LeadershipLost` and
/// `Timeout`: in both, the client does NOT know whether its write took effect.
/// The entry may already have reached a majority and will be committed by the
/// next leader, or it may be discarded. The response says so explicitly
/// (`outcome_unknown: true`) so a client never mistakes "no answer" for "not applied".
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("request channel closed - node is shutting down")]
    ChannelClosed,
    #[error("invalid key: {0}")]
    InvalidKey(String),
    #[error(
        "this node lost leadership before the write was confirmed; \
         it may or may not have been applied"
    )]
    LeadershipLost,
    #[error("{}", timeout_message(*.may_have_applied))]
    Timeout { may_have_applied: bool },
}

fn timeout_message(may_have_applied: bool) -> &'static str {
    if may_have_applied {
        "timed out waiting for the cluster to confirm the write; \
         it may or may not have been applied"
    } else {
        "timed out waiting for the cluster to answer"
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
    /// True when a write may have taken effect even though this request failed.
    outcome_unknown: bool,
}

impl AppError {
    fn status(&self) -> StatusCode {
        match self {
            AppError::ChannelClosed => StatusCode::SERVICE_UNAVAILABLE,
            AppError::InvalidKey(_) => StatusCode::BAD_REQUEST,
            AppError::LeadershipLost => StatusCode::SERVICE_UNAVAILABLE,
            AppError::Timeout { .. } => StatusCode::GATEWAY_TIMEOUT,
        }
    }

    fn outcome_unknown(&self) -> bool {
        match self {
            AppError::LeadershipLost => true,
            AppError::Timeout { may_have_applied } => *may_have_applied,
            AppError::ChannelClosed | AppError::InvalidKey(_) => false,
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.status();
        let body = ErrorBody { error: self.to_string(), outcome_unknown: self.outcome_unknown() };
        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn render(error: AppError) -> (StatusCode, String) {
        let response = error.into_response();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn losing_leadership_is_503_and_says_the_outcome_is_unknown() {
        let (status, body) = render(AppError::LeadershipLost).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.contains("\"outcome_unknown\":true"), "{body}");
    }

    #[tokio::test]
    async fn a_write_timeout_is_504_and_says_the_outcome_is_unknown() {
        let (status, body) = render(AppError::Timeout { may_have_applied: true }).await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert!(body.contains("\"outcome_unknown\":true"), "{body}");
    }

    #[tokio::test]
    async fn a_read_timeout_is_504_but_has_no_unknown_outcome() {
        let (status, body) = render(AppError::Timeout { may_have_applied: false }).await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert!(body.contains("\"outcome_unknown\":false"), "{body}");
    }

    #[tokio::test]
    async fn a_closed_channel_is_503_and_not_an_unknown_outcome() {
        let (status, body) = render(AppError::ChannelClosed).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.contains("\"outcome_unknown\":false"), "{body}");
    }

    #[tokio::test]
    async fn an_invalid_key_is_400() {
        let (status, _) = render(AppError::InvalidKey("empty".into())).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}