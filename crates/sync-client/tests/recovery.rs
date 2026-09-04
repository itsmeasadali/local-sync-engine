use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use async_trait::async_trait;
use serde_json::json;
use sync_client::{ClientError, LocalStore, SyncClient, Transport};
use sync_protocol::{
    ActorId, ApiError, ErrorCode, PullResponse, PushRequest, PushResponse, PushResult, PushStatus,
    SequencedMutation, WorkspaceId,
};

#[derive(Clone, Default)]
struct Server {
    state: Arc<Mutex<State>>,
    fail_after_accept_once: Arc<AtomicBool>,
    reject_field: Option<String>,
}
#[derive(Default)]
struct State {
    log: Vec<SequencedMutation>,
    ids: HashMap<uuid::Uuid, u64>,
}

#[derive(Clone)]
struct UnauthorizedTransport;

#[async_trait]
impl Transport for UnauthorizedTransport {
    async fn push(&self, _request: PushRequest) -> Result<PushResponse, ClientError> {
        Err(unauthorized())
    }

    async fn pull(&self, _after: u64, _limit: u32) -> Result<PullResponse, ClientError> {
        Err(unauthorized())
    }
}

fn unauthorized() -> ClientError {
    ClientError::Remote {
        status: 401,
        code: ErrorCode::Unauthorized,
        message: "invalid credential".into(),
    }
}

#[async_trait]
impl Transport for Server {
    async fn push(&self, request: PushRequest) -> Result<PushResponse, ClientError> {
        let mut state = self.state.lock().unwrap();
        let mut results = Vec::new();
        for mutation in request.mutations {
            if self
                .reject_field
                .as_ref()
                .is_some_and(|field| mutation.fields.contains_key(field))
            {
                results.push(PushResult {
                    mutation_id: mutation.mutation_id,
                    status: PushStatus::Rejected,
                    sequence: None,
                    error: Some(ApiError {
                        code: ErrorCode::InvalidMutation,
                        message: "rejected by test server".into(),
                        request_id: None,
                    }),
                });
                continue;
            }
            let (status, sequence) =
                if let Some(sequence) = state.ids.get(&mutation.mutation_id).copied() {
                    (PushStatus::AlreadyAccepted, sequence)
                } else {
                    let sequence = state.log.len() as u64 + 1;
                    state.ids.insert(mutation.mutation_id, sequence);
                    state.log.push(SequencedMutation {
                        sequence,
                        mutation: mutation.clone(),
                    });
                    (PushStatus::Accepted, sequence)
                };
            results.push(PushResult {
                mutation_id: mutation.mutation_id,
                status,
                sequence: Some(sequence),
                error: None,
            });
        }
        drop(state);
        if self.fail_after_accept_once.swap(false, Ordering::SeqCst) {
            return Err(ClientError::Transport(
                "connection dropped after commit".into(),
            ));
        }
        Ok(PushResponse { results })
    }
    async fn pull(&self, after: u64, limit: u32) -> Result<PullResponse, ClientError> {
        let state = self.state.lock().unwrap();
        let mutations: Vec<_> = state
            .log
            .iter()
            .filter(|item| item.sequence > after)
            .take(limit as usize)
            .cloned()
            .collect();
        let cursor = mutations.last().map_or(after, |item| item.sequence);
        let has_more = state.log.iter().any(|item| item.sequence > cursor);
        Ok(PullResponse {
            mutations,
            cursor,
            has_more,
        })
    }
}

fn write(
    store: &LocalStore,
    workspace: &WorkspaceId,
    actor: &str,
    field: &str,
    value: serde_json::Value,
    time: u64,
) {
    store
        .local_mutation(
            workspace,
            &ActorId::new(actor),
            "todos",
            "r",
            BTreeMap::from([(field.into(), value)]),
            false,
            time,
        )
        .unwrap();
}

#[tokio::test]
async fn interrupted_push_retries_without_duplication() {
    let workspace = WorkspaceId::new("w");
    let store = LocalStore::open_in_memory().unwrap();
    write(&store, &workspace, "a", "title", json!("one"), 1);
    let server = Server::default();
    server.fail_after_accept_once.store(true, Ordering::SeqCst);
    let client = SyncClient::new(store, server.clone(), workspace.clone(), ActorId::new("a"));
    assert!(client.sync().await.is_err());
    assert_eq!(client.store().outbox_len(&workspace).unwrap(), 1);
    client.sync().await.unwrap();
    assert_eq!(client.store().outbox_len(&workspace).unwrap(), 0);
    assert_eq!(server.state.lock().unwrap().log.len(), 1);
}

#[tokio::test]
async fn three_clients_converge_across_multiple_pages() {
    let workspace = WorkspaceId::new("w");
    let server = Server::default();
    let stores = [
        LocalStore::open_in_memory().unwrap(),
        LocalStore::open_in_memory().unwrap(),
        LocalStore::open_in_memory().unwrap(),
    ];
    write(&stores[0], &workspace, "a", "one", json!(1), 10);
    write(&stores[1], &workspace, "b", "two", json!(2), 10);
    write(&stores[2], &workspace, "c", "one", json!(3), 10);
    let clients: Vec<_> = stores
        .into_iter()
        .zip(["a", "b", "c"])
        .map(|(store, actor)| {
            SyncClient::new(
                store,
                server.clone(),
                workspace.clone(),
                ActorId::new(actor),
            )
            .with_limits(1, 1)
        })
        .collect();
    for client in &clients {
        client.sync().await.unwrap();
    }
    for client in &clients {
        client.sync().await.unwrap();
    }
    let expected = Some(json!({"one": 3, "two": 2}));
    for client in &clients {
        assert_eq!(
            client.store().record(&workspace, "todos", "r").unwrap(),
            expected
        );
    }
}

#[test]
fn restart_preserves_pending_outbox() {
    let path = std::env::temp_dir().join(format!("sync-client-{}.db", uuid::Uuid::new_v4()));
    let workspace = WorkspaceId::new("w");
    {
        let store = LocalStore::open(&path).unwrap();
        write(&store, &workspace, "a", "title", json!("pending"), 1);
    }
    let reopened = LocalStore::open(&path).unwrap();
    assert_eq!(reopened.outbox_len(&workspace).unwrap(), 1);
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn invalid_pull_page_rolls_back_records_and_cursor() {
    let workspace = WorkspaceId::new("w");
    let source = LocalStore::open_in_memory().unwrap();
    let valid = source
        .local_mutation(
            &workspace,
            &ActorId::new("remote"),
            "todos",
            "r",
            BTreeMap::from([("title".into(), json!("should roll back"))]),
            false,
            1,
        )
        .unwrap();
    let mut invalid = valid.clone();
    invalid.mutation_id = uuid::Uuid::new_v4();
    invalid.workspace_id = WorkspaceId::new("other");
    let page = PullResponse {
        mutations: vec![
            SequencedMutation {
                sequence: 1,
                mutation: valid,
            },
            SequencedMutation {
                sequence: 2,
                mutation: invalid,
            },
        ],
        cursor: 2,
        has_more: false,
    };
    let destination = LocalStore::open_in_memory().unwrap();
    assert!(destination
        .apply_page(&workspace, &ActorId::new("local"), &page)
        .is_err());
    assert_eq!(destination.cursor(&workspace).unwrap(), 0);
    assert_eq!(destination.record(&workspace, "todos", "r").unwrap(), None);
}

#[tokio::test]
async fn rejected_mutation_is_quarantined_without_blocking_the_outbox() {
    let workspace = WorkspaceId::new("w");
    let store = LocalStore::open_in_memory().unwrap();
    write(&store, &workspace, "a", "reject", json!(true), 1);
    write(&store, &workspace, "a", "accepted", json!(true), 2);
    let server = Server {
        reject_field: Some("reject".into()),
        ..Server::default()
    };
    let client = SyncClient::new(store, server, workspace.clone(), ActorId::new("a"));
    let report = client.sync().await.unwrap();
    assert_eq!(report.pushed, 1);
    assert_eq!(report.rejected, 1);
    assert_eq!(report.quarantined, 1);
    assert_eq!(client.store().outbox_len(&workspace).unwrap(), 0);
    let rejected = client.store().rejected(&workspace).unwrap();
    assert_eq!(rejected.len(), 1);
    let mutation_id = rejected[0].mutation.mutation_id;
    client
        .store()
        .retry_rejected(&workspace, mutation_id)
        .unwrap();
    assert_eq!(client.store().outbox_len(&workspace).unwrap(), 1);
    client.sync().await.unwrap();
    client
        .store()
        .discard_rejected(&workspace, mutation_id)
        .unwrap();
    assert!(client.store().rejected(&workspace).unwrap().is_empty());
}

#[tokio::test]
async fn background_sync_exposes_failure_retry_and_success() {
    let workspace = WorkspaceId::new("w");
    let store = LocalStore::open_in_memory().unwrap();
    write(&store, &workspace, "a", "title", json!("eventual"), 1);
    let server = Server::default();
    server.fail_after_accept_once.store(true, Ordering::SeqCst);
    let client = SyncClient::new(store, server, workspace, ActorId::new("a"));
    let mut background = client.start_background_sync(sync_client::BackgroundSyncOptions {
        interval: std::time::Duration::from_secs(60),
        initial_retry: std::time::Duration::from_millis(25),
        max_retry: std::time::Duration::from_millis(25),
    });
    let first = tokio::time::timeout(std::time::Duration::from_secs(1), background.changed())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        first,
        sync_client::BackgroundSyncStatus::Retrying { .. }
    ));
    let second = tokio::time::timeout(std::time::Duration::from_secs(1), background.changed())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        second,
        sync_client::BackgroundSyncStatus::Succeeded(_)
    ));
    background.shutdown().await;
}

#[tokio::test]
async fn background_sync_stops_retrying_permanent_failures() {
    let client = SyncClient::new(
        LocalStore::open_in_memory().unwrap(),
        UnauthorizedTransport,
        WorkspaceId::new("w"),
        ActorId::new("a"),
    );
    let mut background =
        client.start_background_sync(sync_client::BackgroundSyncOptions::default());
    let status = tokio::time::timeout(std::time::Duration::from_secs(1), background.changed())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        status,
        sync_client::BackgroundSyncStatus::Blocked { .. }
    ));
    background.shutdown().await;
}
