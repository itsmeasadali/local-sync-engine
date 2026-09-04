use std::{fmt, time::Duration};

use rand::{rngs::OsRng, RngCore};
use sqlx::{PgPool, Row};
use sync_protocol::{ActorId, WorkspaceId};
use thiserror::Error;
use uuid::Uuid;

use crate::hash_secret;

#[derive(Debug, Error)]
pub enum AdminError {
    #[error("{kind} must contain between 1 and 255 bytes")]
    InvalidIdentifier { kind: &'static str },
    #[error("workspace already exists")]
    WorkspaceExists,
    #[error("workspace was not found")]
    WorkspaceNotFound,
    #[error("actor already exists")]
    ActorExists,
    #[error("actor was not found")]
    ActorNotFound,
    #[error("credential expiry exceeds the supported range")]
    ExpiryRange,
    #[error("credential hashing failed: {0}")]
    Hash(#[source] crate::ServerError),
    #[error("database operation failed: {0}")]
    Database(#[from] sqlx::Error),
}

pub struct IssuedCredential {
    pub workspace_id: WorkspaceId,
    pub actor_id: ActorId,
    pub credential_id: Uuid,
    bearer_token: String,
}

impl IssuedCredential {
    #[must_use]
    pub fn bearer_token(&self) -> &str {
        &self.bearer_token
    }
}

impl fmt::Debug for IssuedCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IssuedCredential")
            .field("workspace_id", &self.workspace_id)
            .field("actor_id", &self.actor_id)
            .field("credential_id", &self.credential_id)
            .field("bearer_token", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActorSummary {
    pub actor_id: ActorId,
    pub credential_id: Uuid,
    pub expires_at: Option<String>,
    pub revoked: bool,
}

#[derive(Clone)]
pub struct Admin {
    pool: PgPool,
}

impl Admin {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Creates a tenant workspace.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid ID, duplicate workspace, or database failure.
    pub async fn create_workspace(&self, workspace_id: &str) -> Result<(), AdminError> {
        validate_identifier("workspace_id", workspace_id)?;
        let inserted =
            sqlx::query("INSERT INTO workspaces (workspace_id) VALUES ($1) ON CONFLICT DO NOTHING")
                .bind(workspace_id)
                .execute(&self.pool)
                .await?
                .rows_affected();
        if inserted == 0 {
            return Err(AdminError::WorkspaceExists);
        }
        Ok(())
    }

    /// Creates an actor and returns its bearer credential exactly once.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid IDs, absent workspace, duplicate actor, expiry overflow,
    /// credential hashing failure, or database failure.
    pub async fn create_actor(
        &self,
        workspace_id: &str,
        actor_id: &str,
        expires_in: Option<Duration>,
    ) -> Result<IssuedCredential, AdminError> {
        self.issue(workspace_id, actor_id, expires_in, false).await
    }

    /// Invalidates the previous actor credential and returns a replacement.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid IDs, absent actor, expiry overflow, hashing failure, or
    /// database failure.
    pub async fn rotate_actor(
        &self,
        workspace_id: &str,
        actor_id: &str,
        expires_in: Option<Duration>,
    ) -> Result<IssuedCredential, AdminError> {
        self.issue(workspace_id, actor_id, expires_in, true).await
    }

    /// Revokes an actor without deleting its immutable mutation history.
    ///
    /// # Errors
    ///
    /// Returns an error when the actor is absent or the database update fails.
    pub async fn revoke_actor(&self, workspace_id: &str, actor_id: &str) -> Result<(), AdminError> {
        let updated =
            sqlx::query("UPDATE actors SET revoked_at=now() WHERE workspace_id=$1 AND actor_id=$2")
                .bind(workspace_id)
                .bind(actor_id)
                .execute(&self.pool)
                .await?
                .rows_affected();
        if updated == 0 {
            return Err(AdminError::ActorNotFound);
        }
        Ok(())
    }

    /// Lists actors without exposing credential hashes or secrets.
    ///
    /// # Errors
    ///
    /// Returns an error when the database query fails.
    pub async fn actors(&self, workspace_id: &str) -> Result<Vec<ActorSummary>, AdminError> {
        let rows = sqlx::query(
            "SELECT actor_id,credential_id,expires_at::text,revoked_at IS NOT NULL AS revoked FROM actors WHERE workspace_id=$1 ORDER BY actor_id",
        )
        .bind(workspace_id)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok(ActorSummary {
                    actor_id: ActorId::new(row.try_get::<String, _>("actor_id")?),
                    credential_id: row.try_get("credential_id")?,
                    expires_at: row.try_get("expires_at")?,
                    revoked: row.try_get("revoked")?,
                })
            })
            .collect()
    }

    async fn issue(
        &self,
        workspace_id: &str,
        actor_id: &str,
        expires_in: Option<Duration>,
        rotate: bool,
    ) -> Result<IssuedCredential, AdminError> {
        validate_identifier("workspace_id", workspace_id)?;
        validate_identifier("actor_id", actor_id)?;
        let credential_id = Uuid::new_v4();
        let mut secret_bytes = [0_u8; 32];
        OsRng.fill_bytes(&mut secret_bytes);
        let secret = hex::encode(secret_bytes);
        let credential_hash = hash_secret(&secret).map_err(AdminError::Hash)?;
        let expiry_seconds = expires_in
            .map(|duration| i64::try_from(duration.as_secs()).map_err(|_| AdminError::ExpiryRange))
            .transpose()?;
        if rotate {
            let updated = sqlx::query(
                "UPDATE actors SET credential_id=$3,credential_hash=$4,expires_at=CASE WHEN $5::bigint IS NULL THEN NULL ELSE now()+($5*interval '1 second') END,revoked_at=NULL WHERE workspace_id=$1 AND actor_id=$2",
            )
            .bind(workspace_id)
            .bind(actor_id)
            .bind(credential_id)
            .bind(&credential_hash)
            .bind(expiry_seconds)
            .execute(&self.pool)
            .await?
            .rows_affected();
            if updated == 0 {
                return Err(AdminError::ActorNotFound);
            }
        } else {
            let workspace_exists = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM workspaces WHERE workspace_id=$1)",
            )
            .bind(workspace_id)
            .fetch_one(&self.pool)
            .await?;
            if !workspace_exists {
                return Err(AdminError::WorkspaceNotFound);
            }
            let inserted = sqlx::query(
                "INSERT INTO actors (workspace_id,actor_id,credential_id,credential_hash,expires_at) VALUES ($1,$2,$3,$4,CASE WHEN $5::bigint IS NULL THEN NULL ELSE now()+($5*interval '1 second') END) ON CONFLICT (workspace_id,actor_id) DO NOTHING",
            )
            .bind(workspace_id)
            .bind(actor_id)
            .bind(credential_id)
            .bind(&credential_hash)
            .bind(expiry_seconds)
            .execute(&self.pool)
            .await?
            .rows_affected();
            if inserted == 0 {
                return Err(AdminError::ActorExists);
            }
        }
        Ok(IssuedCredential {
            workspace_id: WorkspaceId::new(workspace_id),
            actor_id: ActorId::new(actor_id),
            credential_id,
            bearer_token: format!("{credential_id}.{secret}"),
        })
    }
}

fn validate_identifier(kind: &'static str, value: &str) -> Result<(), AdminError> {
    if value.is_empty() || value.len() > 255 {
        Err(AdminError::InvalidIdentifier { kind })
    } else {
        Ok(())
    }
}
