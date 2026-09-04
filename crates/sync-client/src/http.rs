use std::time::Duration;

use async_trait::async_trait;
use reqwest::{StatusCode, Url};
use sync_protocol::{ApiError, PullResponse, PushRequest, PushResponse};

use crate::{ClientError, Transport};

#[derive(Clone)]
pub struct HttpTransport {
    client: reqwest::Client,
    base_url: Url,
    bearer: String,
}

impl HttpTransport {
    /// Creates a bounded HTTP transport for a v1 sync server.
    ///
    /// # Errors
    ///
    /// Returns an error if the URL or HTTP client configuration is invalid.
    pub fn new(
        base_url: &str,
        bearer: impl Into<String>,
        timeout: Duration,
    ) -> Result<Self, ClientError> {
        let base_url =
            Url::parse(base_url).map_err(|error| ClientError::Transport(error.to_string()))?;
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        Ok(Self {
            client,
            base_url,
            bearer: bearer.into(),
        })
    }

    fn url(&self, path: &str) -> Result<Url, ClientError> {
        self.base_url
            .join(path)
            .map_err(|error| ClientError::Transport(error.to_string()))
    }
}

#[async_trait]
impl Transport for HttpTransport {
    async fn push(&self, request: PushRequest) -> Result<PushResponse, ClientError> {
        let response = self
            .client
            .post(self.url("v1/sync/push")?)
            .bearer_auth(&self.bearer)
            .json(&request)
            .send()
            .await
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        decode(response).await
    }

    async fn pull(&self, after: u64, limit: u32) -> Result<PullResponse, ClientError> {
        let response = self
            .client
            .get(self.url("v1/sync/pull")?)
            .bearer_auth(&self.bearer)
            .query(&[("after", after), ("limit", u64::from(limit))])
            .send()
            .await
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        decode(response).await
    }
}

async fn decode<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, ClientError> {
    let status = response.status();
    if status == StatusCode::OK {
        return response
            .json()
            .await
            .map_err(|error| ClientError::Transport(error.to_string()));
    }
    let (code, message) = match response.json::<ApiError>().await {
        Ok(error) => (error.code, error.message),
        Err(error) => (
            sync_protocol::ErrorCode::Unknown,
            format!("invalid error body: {error}"),
        ),
    };
    Err(ClientError::Remote {
        status: status.as_u16(),
        code,
        message,
    })
}
