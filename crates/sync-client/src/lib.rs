#![forbid(unsafe_code)]
//! Offline-first JSON record storage and synchronization.
//!
//! Most applications start with [`SyncDatabase`]. [`SyncClient`] and [`Transport`]
//! form the advanced seam for custom transport adapters and deterministic testing.

mod background;
mod database;
mod http;
mod store;

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
pub use background::{BackgroundSync, BackgroundSyncOptions, BackgroundSyncStatus};
pub use database::{DeviceCredentials, SyncDatabase, SyncDatabaseConfig};
pub use http::HttpTransport;
pub use store::{LocalStore, RejectedMutation};
use sync_protocol::{
    ActorId, ApiError, ErrorCode, Mutation, PullResponse, PushResponse, PushStatus, WorkspaceId,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("clock error: {0}")]
    Clock(#[from] sync_core::ClockError),
    #[error("invalid mutation: {0}")]
    Validation(#[from] sync_core::ValidationError),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("server returned HTTP {status} ({code:?}): {message}")]
    Remote {
        status: u16,
        code: ErrorCode,
        message: String,
    },
    #[error("server rejected mutation {mutation_id}: {reason}")]
    Rejected {
        mutation_id: uuid::Uuid,
        reason: String,
    },
    #[error("invalid server response: {0}")]
    InvalidResponse(String),
    #[error("system clock is before Unix epoch")]
    InvalidSystemClock,
    #[error("stored integer is outside the supported range")]
    IntegerRange,
    #[error("rejected mutation {0} was not found")]
    RejectedMutationNotFound(uuid::Uuid),
    #[error("record patch must be a JSON object")]
    InvalidRecordPatch,
}

impl ClientError {
    /// Reports whether repeating the same operation later can reasonably succeed.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(_) | Self::Sqlite(_) => true,
            Self::Remote { status, .. } => *status == 408 || *status == 429 || *status >= 500,
            Self::Serialization(_)
            | Self::Clock(_)
            | Self::Validation(_)
            | Self::Rejected { .. }
            | Self::InvalidResponse(_)
            | Self::InvalidSystemClock
            | Self::IntegerRange
            | Self::RejectedMutationNotFound(_)
            | Self::InvalidRecordPatch => false,
        }
    }
}

#[async_trait]
pub trait Transport: Send + Sync {
    async fn push(&self, request: sync_protocol::PushRequest) -> Result<PushResponse, ClientError>;
    async fn pull(&self, after: u64, limit: u32) -> Result<PullResponse, ClientError>;
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SyncReport {
    pub pushed: usize,
    pub pulled: usize,
    pub pages: usize,
    pub rejected: usize,
    pub quarantined: usize,
}

#[derive(Clone)]
pub struct SyncClient<T> {
    store: LocalStore,
    transport: T,
    workspace_id: WorkspaceId,
    actor_id: ActorId,
    push_batch: usize,
    pull_limit: u32,
    sync_lock: Arc<tokio::sync::Mutex<()>>,
}

impl<T: Transport> SyncClient<T> {
    #[must_use]
    pub fn new(
        store: LocalStore,
        transport: T,
        workspace_id: WorkspaceId,
        actor_id: ActorId,
    ) -> Self {
        Self {
            store,
            transport,
            workspace_id,
            actor_id,
            push_batch: 100,
            pull_limit: 100,
            sync_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    #[must_use]
    pub fn with_limits(mut self, push_batch: usize, pull_limit: u32) -> Self {
        self.push_batch = push_batch.max(1);
        self.pull_limit = pull_limit.max(1);
        self
    }

    /// Creates an immediately visible, durably queued local mutation.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, system time, clock overflow, or `SQLite` failure.
    pub fn mutate(
        &self,
        collection: &str,
        record_id: &str,
        fields: std::collections::BTreeMap<String, serde_json::Value>,
        delete: bool,
    ) -> Result<Mutation, ClientError> {
        let wall_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ClientError::InvalidSystemClock)?
            .as_millis();
        let wall_ms = u64::try_from(wall_ms).map_err(|_| ClientError::IntegerRange)?;
        self.store.local_mutation(
            &self.workspace_id,
            &self.actor_id,
            collection,
            record_id,
            fields,
            delete,
            wall_ms,
        )
    }

    /// Pushes all pending mutations, then pulls pages until caught up.
    ///
    /// # Errors
    ///
    /// Returns an error on transport, rejection, invalid server response, or local persistence failure.
    pub async fn sync(&self) -> Result<SyncReport, ClientError> {
        let _sync_guard = self.sync_lock.lock().await;
        let mut report = SyncReport::default();
        loop {
            let pending = self.store.pending(&self.workspace_id, self.push_batch)?;
            if pending.is_empty() {
                break;
            }
            let response = self
                .transport
                .push(sync_protocol::PushRequest {
                    mutations: pending.clone(),
                })
                .await?;
            if response.results.len() != pending.len() {
                return Err(ClientError::InvalidResponse(
                    "push result count differs from request".into(),
                ));
            }
            let mut acknowledged = Vec::with_capacity(pending.len());
            let mut rejected = Vec::new();
            for (expected, result) in pending.iter().zip(response.results) {
                if result.mutation_id != expected.mutation_id {
                    return Err(ClientError::InvalidResponse(
                        "push results are not in request order".into(),
                    ));
                }
                match result.status {
                    PushStatus::Accepted | PushStatus::AlreadyAccepted => {
                        acknowledged.push(result.mutation_id);
                    }
                    PushStatus::Rejected => {
                        let error = result.error.unwrap_or_else(|| ApiError {
                            code: ErrorCode::Unknown,
                            message: "server rejected the mutation without an error".into(),
                            request_id: None,
                        });
                        rejected.push((expected.clone(), error));
                    }
                }
            }
            self.store.complete_push(&acknowledged, &rejected)?;
            report.pushed += acknowledged.len();
            report.rejected += rejected.len();
        }
        loop {
            let after = self.store.cursor(&self.workspace_id)?;
            let response = self.transport.pull(after, self.pull_limit).await?;
            validate_page(&response, after)?;
            let count = response.mutations.len();
            let has_more = response.has_more;
            self.store
                .apply_page(&self.workspace_id, &self.actor_id, &response)?;
            report.pulled += count;
            report.pages += 1;
            if !has_more {
                break;
            }
        }
        report.quarantined = self.store.rejected_len(&self.workspace_id)?;
        Ok(report)
    }

    #[must_use]
    pub const fn store(&self) -> &LocalStore {
        &self.store
    }
}

impl<T> SyncClient<T>
where
    T: Transport + Clone + 'static,
{
    /// Starts an observable background loop with bounded exponential retry.
    #[must_use]
    pub fn start_background_sync(&self, options: BackgroundSyncOptions) -> BackgroundSync {
        background::spawn(self.clone(), options)
    }
}

fn validate_page(page: &PullResponse, after: u64) -> Result<(), ClientError> {
    let mut prior = after;
    for item in &page.mutations {
        if item.sequence <= prior {
            return Err(ClientError::InvalidResponse(
                "pull sequences are not strictly increasing".into(),
            ));
        }
        prior = item.sequence;
    }
    if page.cursor != prior {
        return Err(ClientError::InvalidResponse(
            "pull cursor does not match the final mutation".into(),
        ));
    }
    if page.has_more && page.mutations.is_empty() {
        return Err(ClientError::InvalidResponse(
            "empty pull page reports more data".into(),
        ));
    }
    Ok(())
}
