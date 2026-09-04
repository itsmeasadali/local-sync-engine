use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use sync_protocol::{ActorId, WorkspaceId};

use crate::ServerError;

#[derive(Clone, Debug)]
pub struct AuthContext {
    pub workspace_id: WorkspaceId,
    pub actor_id: ActorId,
}

/// Hashes a bearer secret using Argon2id with a random salt.
///
/// # Errors
///
/// Returns an internal error if the password hashing backend fails.
pub fn hash_secret(secret: &str) -> Result<String, ServerError> {
    Argon2::default()
        .hash_password(secret.as_bytes(), &SaltString::generate(&mut OsRng))
        .map(|hash| hash.to_string())
        .map_err(|error| ServerError::Internal(anyhow::anyhow!(error)))
}

pub fn verify_secret(secret: &str, encoded: &str) -> Result<bool, ServerError> {
    let hash = PasswordHash::new(encoded)
        .map_err(|error| ServerError::Internal(anyhow::anyhow!(error)))?;
    Ok(Argon2::default()
        .verify_password(secret.as_bytes(), &hash)
        .is_ok())
}
