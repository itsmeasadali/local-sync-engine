use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use serde_json::json;
use sync_client::{ClientError, LocalStore, SyncClient, Transport};
use sync_protocol::{
    ActorId, PullResponse, PushRequest, PushResponse, PushResult, PushStatus, SequencedMutation,
    WorkspaceId,
};

#[derive(Clone, Default)]
struct MemoryServer {
    inner: Arc<Mutex<ServerState>>,
}

#[derive(Default)]
struct ServerState {
    log: Vec<SequencedMutation>,
    seen: HashMap<uuid::Uuid, u64>,
}

#[async_trait]
impl Transport for MemoryServer {
    async fn push(&self, request: PushRequest) -> Result<PushResponse, ClientError> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| ClientError::Transport("memory server lock poisoned".into()))?;
        let mut results = Vec::new();
        for mutation in request.mutations {
            if let Some(sequence) = state.seen.get(&mutation.mutation_id).copied() {
                results.push(PushResult {
                    mutation_id: mutation.mutation_id,
                    status: PushStatus::AlreadyAccepted,
                    sequence: Some(sequence),
                    error: None,
                });
            } else {
                let sequence = state.log.len() as u64 + 1;
                state.seen.insert(mutation.mutation_id, sequence);
                let mutation_id = mutation.mutation_id;
                state.log.push(SequencedMutation { sequence, mutation });
                results.push(PushResult {
                    mutation_id,
                    status: PushStatus::Accepted,
                    sequence: Some(sequence),
                    error: None,
                });
            }
        }
        Ok(PushResponse { results })
    }

    async fn pull(&self, after: u64, limit: u32) -> Result<PullResponse, ClientError> {
        let state = self
            .inner
            .lock()
            .map_err(|_| ClientError::Transport("memory server lock poisoned".into()))?;
        let available: Vec<_> = state
            .log
            .iter()
            .filter(|entry| entry.sequence > after)
            .take(limit as usize)
            .cloned()
            .collect();
        let cursor = available.last().map_or(after, |entry| entry.sequence);
        let has_more = state.log.iter().any(|entry| entry.sequence > cursor);
        Ok(PullResponse {
            mutations: available,
            cursor,
            has_more,
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), ClientError> {
    let workspace = WorkspaceId::new("demo");
    let server = MemoryServer::default();
    let alice_store = LocalStore::open_in_memory()?;
    let bob_store = LocalStore::open_in_memory()?;

    alice_store.local_mutation(
        &workspace,
        &ActorId::new("alice"),
        "todos",
        "first",
        BTreeMap::from([
            ("title".into(), json!("buy milk")),
            ("priority".into(), json!(1)),
        ]),
        false,
        1_000,
    )?;
    bob_store.local_mutation(
        &workspace,
        &ActorId::new("bob"),
        "todos",
        "first",
        BTreeMap::from([
            ("title".into(), json!("buy oat milk")),
            ("done".into(), json!(true)),
        ]),
        false,
        1_000,
    )?;

    let alice = SyncClient::new(
        alice_store,
        server.clone(),
        workspace.clone(),
        ActorId::new("alice"),
    );
    let bob = SyncClient::new(bob_store, server, workspace.clone(), ActorId::new("bob"));
    alice.sync().await?;
    bob.sync().await?;
    alice.sync().await?;

    let expected = json!({"done": true, "priority": 1, "title": "buy oat milk"});
    assert_eq!(
        alice.store().record(&workspace, "todos", "first")?,
        Some(expected.clone())
    );
    assert_eq!(
        bob.store().record(&workspace, "todos", "first")?,
        Some(expected.clone())
    );
    println!(
        "Both offline clients converged to:\n{}",
        serde_json::to_string_pretty(&expected)?
    );
    Ok(())
}
