use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value;
use sqlx::{postgres::PgRow, PgPool, Postgres, Row, Transaction};
use sync_core::{validate_mutation, ValidationLimits};
use sync_protocol::{
    ActorId, ApiError, ErrorCode, Mutation, PullResponse, PushResult, PushStatus,
    SequencedMutation, WorkspaceId,
};
use uuid::Uuid;

use crate::{auth::verify_secret, AuthContext, ServerError};

#[derive(Clone)]
pub struct Repository {
    pool: PgPool,
}

impl Repository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Verifies a bearer token and returns its immutable tenant context.
    ///
    /// # Errors
    ///
    /// Returns an authentication, credential-expiry, hashing, or database error.
    pub async fn authenticate(&self, bearer: &str) -> Result<AuthContext, ServerError> {
        let (credential_id, secret) = bearer.split_once('.').ok_or(ServerError::Unauthorized)?;
        if secret.is_empty() {
            return Err(ServerError::Unauthorized);
        }
        let credential_id =
            Uuid::parse_str(credential_id).map_err(|_| ServerError::Unauthorized)?;
        let row = sqlx::query(
            "SELECT workspace_id, actor_id, credential_hash, expires_at IS NOT NULL AND expires_at <= now() AS expired, revoked_at IS NOT NULL AS revoked FROM actors WHERE credential_id = $1",
        )
        .bind(credential_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            // Keep unknown credential IDs on the same expensive Argon2 path as
            // known IDs so the selector cannot be enumerated by timing alone.
            let _unused_hash = crate::hash_secret(secret)?;
            return Err(ServerError::Unauthorized);
        };
        let encoded: String = row.try_get("credential_hash")?;
        if !verify_secret(secret, &encoded)? {
            return Err(ServerError::Unauthorized);
        }
        if row.try_get::<bool, _>("expired")? {
            return Err(ServerError::CredentialExpired);
        }
        if row.try_get::<bool, _>("revoked")? {
            return Err(ServerError::CredentialRevoked);
        }
        Ok(AuthContext {
            workspace_id: WorkspaceId::new(row.try_get::<String, _>("workspace_id")?),
            actor_id: ActorId::new(row.try_get::<String, _>("actor_id")?),
        })
    }

    /// Validates and atomically accepts or deduplicates an ordered mutation batch.
    ///
    /// # Errors
    ///
    /// Returns an error if time calculation, serialization, or `PostgreSQL` fails.
    pub async fn push(
        &self,
        auth: &AuthContext,
        mutations: &[Mutation],
        future_skew: Duration,
    ) -> Result<Vec<PushResult>, ServerError> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| ServerError::Internal(error.into()))?
            .as_millis();
        let latest_ms = now_ms.saturating_add(future_skew.as_millis());
        let mut transaction = self.pool.begin().await?;
        let mut results = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            let rejection = if mutation.workspace_id != auth.workspace_id {
                Some((
                    ErrorCode::WorkspaceMismatch,
                    "mutation workspace does not match credential",
                ))
            } else if mutation.actor_id != auth.actor_id {
                Some((
                    ErrorCode::ActorMismatch,
                    "mutation actor does not match credential",
                ))
            } else if u128::from(mutation.version.timestamp.physical_ms) > latest_ms {
                Some((
                    ErrorCode::FutureClock,
                    "mutation clock is too far in the future",
                ))
            } else if mutation.version.timestamp.physical_ms > i64::MAX as u64 {
                Some((
                    ErrorCode::InvalidMutation,
                    "mutation clock cannot be represented by the server",
                ))
            } else if let Err(error) = validate_mutation(mutation, ValidationLimits::default()) {
                results.push(rejected(
                    mutation,
                    ErrorCode::InvalidMutation,
                    error.to_string(),
                ));
                continue;
            } else {
                None
            };
            if let Some((code, message)) = rejection {
                results.push(rejected(mutation, code, message));
                continue;
            }
            results.push(self.insert_one(&mut transaction, mutation).await?);
        }
        transaction.commit().await?;
        Ok(results)
    }

    async fn insert_one(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        mutation: &Mutation,
    ) -> Result<PushResult, ServerError> {
        let payload =
            serde_json::to_value(mutation).map_err(|error| ServerError::Internal(error.into()))?;
        // Workspace serialization gives gap-free sequences and closes the race
        // between concurrent checks for the same mutation ID.
        sqlx::query("SELECT current_sequence FROM workspaces WHERE workspace_id = $1 FOR UPDATE")
            .bind(mutation.workspace_id.as_str())
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or_else(|| {
                ServerError::bad_request(ErrorCode::WorkspaceMismatch, "workspace does not exist")
            })?;
        if let Some(row) = sqlx::query(
            "SELECT sequence, payload FROM mutations WHERE workspace_id = $1 AND mutation_id = $2",
        )
        .bind(mutation.workspace_id.as_str())
        .bind(mutation.mutation_id)
        .fetch_optional(&mut **transaction)
        .await?
        {
            let sequence: i64 = row.try_get("sequence")?;
            let previous: Value = row.try_get("payload")?;
            if previous != payload {
                return Ok(rejected(
                    mutation,
                    ErrorCode::InvalidMutation,
                    "mutation_id was already used with a different payload",
                ));
            }
            return Ok(PushResult {
                mutation_id: mutation.mutation_id,
                status: PushStatus::AlreadyAccepted,
                sequence: Some(sequence_from_db(sequence)?),
                error: None,
            });
        }
        let row = sqlx::query("UPDATE workspaces SET current_sequence = current_sequence + 1 WHERE workspace_id = $1 RETURNING current_sequence")
            .bind(mutation.workspace_id.as_str()).fetch_optional(&mut **transaction).await?
            .ok_or_else(|| ServerError::Internal(anyhow::anyhow!("locked workspace disappeared")))?;
        let sequence: i64 = row.try_get("current_sequence")?;
        let physical_ms = i64::try_from(mutation.version.timestamp.physical_ms)
            .map_err(|error| ServerError::Internal(error.into()))?;
        sqlx::query(
            "INSERT INTO mutations (workspace_id, mutation_id, sequence, actor_id, collection, record_id, hlc_physical_ms, hlc_logical, payload, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,to_timestamp($7::double precision / 1000.0))",
        )
        .bind(mutation.workspace_id.as_str()).bind(mutation.mutation_id).bind(sequence)
        .bind(mutation.actor_id.as_str()).bind(mutation.collection.as_str()).bind(mutation.record_id.as_str())
        .bind(physical_ms).bind(i64::from(mutation.version.timestamp.logical)).bind(payload)
        .execute(&mut **transaction).await?;
        Ok(PushResult {
            mutation_id: mutation.mutation_id,
            status: PushStatus::Accepted,
            sequence: Some(sequence_from_db(sequence)?),
            error: None,
        })
    }

    /// Reads a stable, ordered page strictly after `after` for one workspace.
    ///
    /// # Errors
    ///
    /// Returns an error for an out-of-range cursor, corrupt payload, or database failure.
    pub async fn pull(
        &self,
        workspace_id: &WorkspaceId,
        after: u64,
        limit: u32,
    ) -> Result<PullResponse, ServerError> {
        let database_after = i64::try_from(after).map_err(|_| {
            ServerError::bad_request(ErrorCode::InvalidCursor, "cursor is too large")
        })?;
        let rows = sqlx::query("SELECT sequence, payload FROM mutations WHERE workspace_id = $1 AND sequence > $2 ORDER BY sequence ASC LIMIT $3")
            .bind(workspace_id.as_str()).bind(database_after).bind(i64::from(limit) + 1)
            .fetch_all(&self.pool).await?;
        let has_more = rows.len() > limit as usize;
        let mutations = rows
            .into_iter()
            .take(limit as usize)
            .map(|row| decode_mutation(&row))
            .collect::<Result<Vec<_>, _>>()?;
        let cursor = mutations.last().map_or(after, |item| item.sequence);
        Ok(PullResponse {
            mutations,
            cursor,
            has_more,
        })
    }
}

fn decode_mutation(row: &PgRow) -> Result<SequencedMutation, ServerError> {
    let sequence: i64 = row.try_get("sequence")?;
    let payload: Value = row.try_get("payload")?;
    let mutation =
        serde_json::from_value(payload).map_err(|error| ServerError::Internal(error.into()))?;
    Ok(SequencedMutation {
        sequence: sequence_from_db(sequence)?,
        mutation,
    })
}

fn sequence_from_db(sequence: i64) -> Result<u64, ServerError> {
    u64::try_from(sequence).map_err(|error| ServerError::Internal(error.into()))
}

fn rejected(mutation: &Mutation, code: ErrorCode, message: impl Into<String>) -> PushResult {
    PushResult {
        mutation_id: mutation.mutation_id,
        status: PushStatus::Rejected,
        sequence: None,
        error: Some(ApiError {
            code,
            message: message.into(),
            request_id: None,
        }),
    }
}
