use sync_protocol::Mutation;
use thiserror::Error;

#[derive(Clone, Copy, Debug)]
pub struct ValidationLimits {
    pub max_identifier_bytes: usize,
    pub max_fields: usize,
    pub max_field_name_bytes: usize,
    pub max_mutation_bytes: usize,
}

impl Default for ValidationLimits {
    fn default() -> Self {
        Self {
            max_identifier_bytes: 255,
            max_fields: 256,
            max_field_name_bytes: 255,
            max_mutation_bytes: 1_048_576,
        }
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ValidationError {
    #[error("{0} must not be empty")]
    EmptyIdentifier(&'static str),
    #[error("{0} exceeds the identifier size limit")]
    IdentifierTooLong(&'static str),
    #[error("mutation must change at least one field or set delete")]
    EmptyMutation,
    #[error("mutation exceeds the field count limit")]
    TooManyFields,
    #[error("field name is empty or too long")]
    InvalidFieldName,
    #[error("mutation exceeds the encoded size limit")]
    MutationTooLarge,
    #[error("version actor does not match mutation actor")]
    VersionActorMismatch,
}

/// Checks protocol invariants and transport-independent resource bounds.
///
/// # Errors
///
/// Returns the first [`ValidationError`] encountered.
pub fn validate_mutation(
    mutation: &Mutation,
    limits: ValidationLimits,
) -> Result<(), ValidationError> {
    for (label, value) in [
        ("workspace_id", mutation.workspace_id.as_str()),
        ("actor_id", mutation.actor_id.as_str()),
        ("collection", mutation.collection.as_str()),
        ("record_id", mutation.record_id.as_str()),
    ] {
        if value.is_empty() {
            return Err(ValidationError::EmptyIdentifier(label));
        }
        if value.len() > limits.max_identifier_bytes {
            return Err(ValidationError::IdentifierTooLong(label));
        }
    }
    if mutation.fields.is_empty() && !mutation.delete {
        return Err(ValidationError::EmptyMutation);
    }
    if mutation.fields.len() > limits.max_fields {
        return Err(ValidationError::TooManyFields);
    }
    if mutation
        .fields
        .keys()
        .any(|key| key.is_empty() || key.len() > limits.max_field_name_bytes)
    {
        return Err(ValidationError::InvalidFieldName);
    }
    if mutation.actor_id != mutation.version.actor_id {
        return Err(ValidationError::VersionActorMismatch);
    }
    if serde_json::to_vec(mutation).map_or(true, |bytes| bytes.len() > limits.max_mutation_bytes) {
        return Err(ValidationError::MutationTooLarge);
    }
    Ok(())
}
