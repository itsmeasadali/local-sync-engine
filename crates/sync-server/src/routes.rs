use axum::{
    extract::{
        rejection::{JsonRejection, QueryRejection},
        DefaultBodyLimit, Query, State,
    },
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use sync_protocol::{ErrorCode, PullResponse, PushRequest, PushResponse, PushStatus};
use tower_http::{
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    timeout::TimeoutLayer,
    trace::TraceLayer,
};

use crate::{Config, Metrics, Repository, ServerError};

#[derive(Clone)]
pub struct AppState {
    pub repository: Repository,
    pub config: Config,
    pub metrics: Metrics,
}

pub fn router(state: AppState) -> Router {
    let body_limit = state.config.max_body_bytes;
    let timeout = state.config.request_timeout;
    Router::new()
        .route("/v1/sync/push", post(push))
        .route("/v1/sync/pull", get(pull))
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .with_state(state)
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &axum::http::Request<_>| {
                let request_id = request
                    .headers()
                    .get("x-request-id")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("invalid");
                tracing::info_span!(
                    "http_request",
                    method = %request.method(),
                    uri = %request.uri(),
                    request_id
                )
            }),
        )
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            timeout,
        ))
        .layer(DefaultBodyLimit::max(body_limit))
}

async fn push(
    State(state): State<AppState>,
    headers: HeaderMap,
    payload: Result<Json<PushRequest>, JsonRejection>,
) -> Result<Json<PushResponse>, ServerError> {
    let Json(request) = payload.map_err(|error| {
        if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
            ServerError::payload_too_large("request body exceeds the configured limit")
        } else {
            ServerError::bad_request(ErrorCode::InvalidMutation, "invalid JSON request body")
        }
    })?;
    if request.mutations.len() > state.config.max_batch_size {
        return Err(ServerError::bad_request(
            ErrorCode::BatchTooLarge,
            format!("batch exceeds {} mutations", state.config.max_batch_size),
        ));
    }
    let auth = authenticate(&state, &headers).await?;
    let results = state
        .repository
        .push(&auth, &request.mutations, state.config.max_future_skew)
        .await?;
    for result in &results {
        match result.status {
            PushStatus::Accepted => state.metrics.accepted(),
            PushStatus::Rejected => state.metrics.rejected(),
            PushStatus::AlreadyAccepted => {}
        }
    }
    state.metrics.request("push", 200);
    Ok(Json(PushResponse { results }))
}

#[derive(Deserialize)]
struct PullQuery {
    #[serde(default)]
    after: u64,
    limit: Option<u32>,
}

async fn pull(
    State(state): State<AppState>,
    headers: HeaderMap,
    query: Result<Query<PullQuery>, QueryRejection>,
) -> Result<Json<PullResponse>, ServerError> {
    let Query(query) = query.map_err(|_| {
        ServerError::bad_request(ErrorCode::InvalidCursor, "invalid pull query parameters")
    })?;
    let limit = query.limit.unwrap_or(100);
    if limit == 0 || limit > state.config.max_pull_limit {
        return Err(ServerError::bad_request(
            ErrorCode::InvalidLimit,
            format!(
                "limit must be between 1 and {}",
                state.config.max_pull_limit
            ),
        ));
    }
    let auth = authenticate(&state, &headers).await?;
    let response = state
        .repository
        .pull(&auth.workspace_id, query.after, limit)
        .await?;
    state.metrics.request("pull", 200);
    Ok(Json(response))
}

async fn authenticate(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<crate::AuthContext, ServerError> {
    let value = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or(ServerError::Unauthorized)?;
    let bearer = value
        .strip_prefix("Bearer ")
        .ok_or(ServerError::Unauthorized)?;
    state.repository.authenticate(bearer).await
}

async fn live() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({"status":"ok"})))
}

async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    match sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(state.repository.pool())
        .await
    {
        Ok(_) => {
            state.metrics.set_ready(true);
            (StatusCode::OK, Json(serde_json::json!({"status":"ready"})))
        }
        Err(error) => {
            state.metrics.set_ready(false);
            tracing::warn!(%error, "readiness check failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"status":"unavailable", "code": ErrorCode::Unavailable})),
            )
        }
    }
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    match state.metrics.encode() {
        Ok(body) => (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )],
            body,
        )
            .into_response(),
        Err(error) => {
            tracing::error!(%error, "metrics encoding failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
