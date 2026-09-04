use std::{collections::BTreeMap, fmt, path::PathBuf, time::Duration};

use serde_json::Value;
use sync_protocol::{ActorId, WorkspaceId};
use uuid::Uuid;

use crate::{
    BackgroundSync, BackgroundSyncOptions, ClientError, HttpTransport, LocalStore,
    RejectedMutation, SyncClient, SyncReport,
};

#[derive(Clone)]
pub struct DeviceCredentials {
    workspace_id: WorkspaceId,
    actor_id: ActorId,
    bearer_token: String,
}

impl DeviceCredentials {
    #[must_use]
    pub fn new(
        workspace_id: impl Into<String>,
        actor_id: impl Into<String>,
        bearer_token: impl Into<String>,
    ) -> Self {
        Self {
            workspace_id: WorkspaceId::new(workspace_id),
            actor_id: ActorId::new(actor_id),
            bearer_token: bearer_token.into(),
        }
    }
}

impl fmt::Debug for DeviceCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeviceCredentials")
            .field("workspace_id", &self.workspace_id)
            .field("actor_id", &self.actor_id)
            .field("bearer_token", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct SyncDatabaseConfig {
    pub database_path: PathBuf,
    pub server_url: String,
    pub credentials: DeviceCredentials,
    pub request_timeout: Duration,
}

impl SyncDatabaseConfig {
    #[must_use]
    pub fn new(
        database_path: impl Into<PathBuf>,
        server_url: impl Into<String>,
        credentials: DeviceCredentials,
    ) -> Self {
        Self {
            database_path: database_path.into(),
            server_url: server_url.into(),
            credentials,
            request_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Clone)]
pub struct SyncDatabase {
    client: SyncClient<HttpTransport>,
}

impl SyncDatabase {
    /// Opens a local database and configures its remote synchronization adapter.
    ///
    /// # Errors
    ///
    /// Returns an error when `SQLite` initialization, URL parsing, or HTTP client setup fails.
    pub fn open(config: SyncDatabaseConfig) -> Result<Self, ClientError> {
        let store = LocalStore::open(config.database_path)?;
        let transport = HttpTransport::new(
            &config.server_url,
            config.credentials.bearer_token,
            config.request_timeout,
        )?;
        Ok(Self {
            client: SyncClient::new(
                store,
                transport,
                config.credentials.workspace_id,
                config.credentials.actor_id,
            ),
        })
    }

    /// Applies a partial record update locally and queues it for synchronization.
    ///
    /// # Errors
    ///
    /// Returns an error for a non-object patch, invalid identifiers, clock failure, or `SQLite` failure.
    pub fn patch(
        &self,
        collection: &str,
        record_id: &str,
        fields: Value,
    ) -> Result<Uuid, ClientError> {
        let Value::Object(fields) = fields else {
            return Err(ClientError::InvalidRecordPatch);
        };
        self.client
            .mutate(collection, record_id, fields.into_iter().collect(), false)
            .map(|mutation| mutation.mutation_id)
    }

    /// Reads a record immediately from the local materialized view.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt local state or an `SQLite` failure.
    pub fn get(&self, collection: &str, record_id: &str) -> Result<Option<Value>, ClientError> {
        self.client
            .store
            .record(&self.client.workspace_id, collection, record_id)
    }

    /// Creates a local deletion tombstone and queues it for synchronization.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identifiers, clock failure, or `SQLite` failure.
    pub fn delete(&self, collection: &str, record_id: &str) -> Result<Uuid, ClientError> {
        self.client
            .mutate(collection, record_id, BTreeMap::new(), true)
            .map(|mutation| mutation.mutation_id)
    }

    /// Synchronizes pending writes and remote changes once.
    ///
    /// # Errors
    ///
    /// Returns an error on transport, invalid server response, or persistence failure.
    pub async fn sync(&self) -> Result<SyncReport, ClientError> {
        self.client.sync().await
    }

    #[must_use]
    pub fn start_background_sync(&self, options: BackgroundSyncOptions) -> BackgroundSync {
        self.client.start_background_sync(options)
    }

    /// Lists explicitly rejected mutations for operator or user resolution.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt local state or an `SQLite` failure.
    pub fn rejected(&self) -> Result<Vec<RejectedMutation>, ClientError> {
        self.client.store.rejected(&self.client.workspace_id)
    }

    /// Requeues a rejected mutation after its cause has been corrected.
    ///
    /// # Errors
    ///
    /// Returns an error if it is absent or the `SQLite` transaction fails.
    pub fn retry_rejected(&self, mutation_id: uuid::Uuid) -> Result<(), ClientError> {
        self.client
            .store
            .retry_rejected(&self.client.workspace_id, mutation_id)
    }

    /// Permanently discards a rejected mutation.
    ///
    /// # Errors
    ///
    /// Returns an error if it is absent or `SQLite` cannot delete it.
    pub fn discard_rejected(&self, mutation_id: uuid::Uuid) -> Result<(), ClientError> {
        self.client
            .store
            .discard_rejected(&self.client.workspace_id, mutation_id)
    }
}
