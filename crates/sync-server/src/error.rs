use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use sync_protocol::{ApiError, ErrorCode};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("credential expired")]
    CredentialExpired,
    #[error("credential revoked")]
    CredentialRevoked,
    #[error("invalid request: {message}")]
    Request {
        status: StatusCode,
        code: ErrorCode,
        message: String,
    },
    #[error("service unavailable")]
    Unavailable(#[source] sqlx::Error),
    #[error("internal server error")]
    Internal(#[source] anyhow::Error),
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let (status, code, public_message) = match &self {
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                ErrorCode::Unauthorized,
                "invalid credential",
            ),
            Self::CredentialExpired => (
                StatusCode::UNAUTHORIZED,
                ErrorCode::CredentialExpired,
                "credential expired",
            ),
            Self::CredentialRevoked => (
                StatusCode::UNAUTHORIZED,
                ErrorCode::CredentialRevoked,
                "credential revoked",
            ),
            Self::Request {
                status,
                code,
                message,
            } => (*status, code.clone(), message.as_str()),
            Self::Unavailable(_) => (
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorCode::Unavailable,
                "service unavailable",
            ),
            Self::Internal(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::Internal,
                "internal server error",
            ),
        };
        if status.is_server_error() {
            tracing::error!(error = %self, "request failed");
        }
        (
            status,
            Json(ApiError {
                code,
                message: public_message.to_owned(),
                request_id: None,
            }),
        )
            .into_response()
    }
}

impl ServerError {
    #[must_use]
    pub fn bad_request(code: ErrorCode, message: impl Into<String>) -> Self {
        Self::Request {
            status: StatusCode::BAD_REQUEST,
            code,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn payload_too_large(message: impl Into<String>) -> Self {
        Self::Request {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            code: ErrorCode::RequestTooLarge,
            message: message.into(),
        }
    }
}

impl From<sqlx::Error> for ServerError {
    fn from(value: sqlx::Error) -> Self {
        Self::Unavailable(value)
    }
}
