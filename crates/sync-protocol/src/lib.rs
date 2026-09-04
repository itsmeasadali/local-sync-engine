#![forbid(unsafe_code)]
//! Stable serialized types for Local Sync Engine protocol v1.

use std::{collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            #[must_use]
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

string_id!(WorkspaceId);
string_id!(ActorId);
string_id!(CollectionName);
string_id!(RecordId);

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct HlcTimestamp {
    pub physical_ms: u64,
    pub logical: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Version {
    pub timestamp: HlcTimestamp,
    pub actor_id: ActorId,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FieldChange {
    pub value: Value,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Mutation {
    pub mutation_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub actor_id: ActorId,
    pub collection: CollectionName,
    pub record_id: RecordId,
    #[serde(default)]
    pub fields: BTreeMap<String, FieldChange>,
    #[serde(default)]
    pub delete: bool,
    pub version: Version,
}

pub type Cursor = u64;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PushRequest {
    pub mutations: Vec<Mutation>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PushStatus {
    Accepted,
    AlreadyAccepted,
    Rejected,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PushResult {
    pub mutation_id: Uuid,
    pub status: PushStatus,
    pub sequence: Option<Cursor>,
    pub error: Option<ApiError>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PushResponse {
    pub results: Vec<PushResult>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SequencedMutation {
    pub sequence: Cursor,
    pub mutation: Mutation,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PullResponse {
    pub mutations: Vec<SequencedMutation>,
    /// Sequence of the last item in this page, or the input cursor for an empty page.
    pub cursor: Cursor,
    pub has_more: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    Unauthorized,
    CredentialExpired,
    CredentialRevoked,
    WorkspaceMismatch,
    ActorMismatch,
    InvalidMutation,
    FutureClock,
    BatchTooLarge,
    InvalidCursor,
    InvalidLimit,
    RequestTooLarge,
    Internal,
    Unavailable,
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ApiError {
    pub code: ErrorCode,
    pub message: String,
    pub request_id: Option<String>,
}
