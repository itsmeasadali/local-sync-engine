# Local Sync Engine

[![MSRV](https://img.shields.io/badge/rustc-1.88%2B-blue.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Local Sync Engine is an open-source Rust synchronization engine for applications that must keep working offline. Writes commit immediately to SQLite, then synchronize through an immutable PostgreSQL operation log when connectivity returns.

> Project status: **beta**. The correctness and recovery paths are tested, but operators should read the documented limitations before a public multi-tenant deployment.

## Why use it?

- Local reads and writes do not wait for a network.
- Concurrent changes to different JSON fields are preserved.
- Same-field conflicts resolve deterministically with HLC and actor versions.
- Outbox delivery, server pushes, pulls, and cursor advancement are retry-safe.
- The protocol and merge engine are independent of HTTP and database adapters.

It is designed for native desktop, mobile, CLI, and server applications. It is not a collaborative-text engine, peer-to-peer database, relational query synchronizer, or end-to-end encrypted store.

## Is it a good fit?

Use Local Sync Engine when each device needs an embedded SQLite database, users must write without connectivity, and your data can be represented as JSON records with deterministic field-level conflict resolution.

Choose a different system when you need peer-to-peer sync, collaborative rich-text CRDTs, arbitrary relational replication, end-to-end encryption supplied by the sync layer, or a hosted service with no operations work.

## How it works

```text
Application
    |
    v
SyncDatabase <---- local transaction ----> SQLite + durable outbox
    |
    | HTTP v1 push / pull
    v
Axum sync server ------------------------> PostgreSQL operation log
```

The client owns the offline experience: local mutations and their outbox entries commit atomically. The server authenticates actors, assigns gap-free workspace sequence numbers, and stores immutable operations. Every client pulls from its durable cursor and applies the same deterministic merge rules, so retries are safe and replicas converge.

## Try it

Prerequisites: Rust 1.88+, Docker, and Docker Compose.

```sh
docker compose up -d
```

The development stack creates workspace `demo`, actor `alice`, and a development-only credential. In another shell:

```sh
cargo run -p syncctl -- \
  --database alice.db \
  --workspace demo \
  --actor alice \
  set todos first title '"Buy milk"'

cargo run -p syncctl -- \
  --database alice.db \
  --server http://127.0.0.1:8080 \
  --token 00000000-0000-0000-0000-000000000001.local-development-secret \
  --workspace demo \
  --actor alice \
  sync
```

The first command succeeds without the server. The second flushes the durable outbox and downloads remote changes. Run the in-memory two-client demonstration with:

```sh
cargo run -p todo-example
```

## Embed it

Most applications should use the deep `SyncDatabase` module and let it hide clocks, mutation IDs, cursors, and outbox transactions:

```rust,no_run
use serde_json::json;
use sync_client::{
    BackgroundSyncOptions, DeviceCredentials, SyncDatabase, SyncDatabaseConfig,
};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let credentials = DeviceCredentials::new("acme", "device-123", bearer_token);
let config = SyncDatabaseConfig::new("app.db", "https://sync.example.com/", credentials);
let database = SyncDatabase::open(config)?;

// Immediately visible and durably queued, even while offline.
database.patch("todos", "todo-1", json!({
    "title": "Buy milk",
    "done": false
}))?;

let local_record = database.get("todos", "todo-1")?;

// Starts immediately, retries transient failures, and exposes every status.
let mut background = database.start_background_sync(BackgroundSyncOptions::default());
while let Some(status) = background.changed().await {
    println!("sync status: {status:?}");
}
# Ok(())
# }
```

`patch` updates only the supplied top-level fields. JSON `null` is a value, not field removal. `delete` creates a versioned record tombstone. Advanced integrations can use `SyncClient<T>` with their own transport adapter.

Only one sync runs per client at a time. Transient background failures remain observable and retry with bounded exponential backoff plus jitter. Permanent configuration, authentication, validation, or protocol failures publish `Blocked` and stop retrying; create a corrected client before restarting. Dropping or shutting down the handle stops the loop.

## Handle rejected writes

Server rejection never silently deletes a mutation and no longer blocks later writes. It moves the mutation into a local quarantine. `SyncReport::rejected` reports newly rejected writes, while `SyncReport::quarantined` keeps reporting unresolved writes on every sync.

```sh
syncctl ... rejected
syncctl ... retry <mutation-id>
syncctl ... discard <mutation-id>
```

Retry only after correcting the cause. Discard is permanent, but it affects only the rejected outgoing mutation; its already-visible local value remains until another mutation changes it.

## Administer credentials

Set `SYNC_DATABASE_URL`, then use the server's offline administration commands:

```sh
cargo run -p sync-server -- workspace-create acme
cargo run -p sync-server -- actor-create acme device-123 --expires-in-days 90
cargo run -p sync-server -- actor-list acme
cargo run -p sync-server -- actor-rotate acme device-123 --expires-in-days 90
cargo run -p sync-server -- actor-revoke acme device-123
```

Create and rotate print a bearer token once. Store it securely; PostgreSQL retains only its Argon2id hash. Rotation invalidates the old credential, and revocation preserves mutation history.

## Workspace

- `sync-protocol`: stable v1 wire and identifier types.
- `sync-core`: HLC, validation, field registers, tombstones, and property tests.
- `sync-client`: the high-level database interface, SQLite adapter, HTTP adapter, recovery, and background synchronization.
- `sync-server`: Axum routes, PostgreSQL adapter, authentication, administration, limits, health, and metrics.
- `syncctl`: local writes, reads, synchronization, and quarantine recovery.
- `examples/todo`: deterministic offline conflict and convergence demonstration.

Architecture and decisions are in [docs/architecture.md](docs/architecture.md) and [docs/adr](docs/adr). See [docs/operations.md](docs/operations.md) before deployment, [docs/protocol-v1.md](docs/protocol-v1.md) for wire semantics, and [docs/threat-model.md](docs/threat-model.md) for security assumptions.

## Verify

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Set `TEST_DATABASE_URL` to an isolated PostgreSQL database to run the real repository, credential, isolation, pagination, and idempotency tests. CI runs the Rust 1.88 minimum-version check separately.

## Known limitations

- Same-field LWW can discard concurrent intent and is unsuitable for collaborative text.
- A client clock within the accepted skew window can influence conflict outcomes.
- Mutation logs and tombstones are not compacted in v1.
- Workspace writes are serialized to guarantee gap-free sequence assignment.
- TLS, edge rate limiting, PostgreSQL high availability, and secret delivery belong to the deployment environment.
- SQLite uses one mutex-protected connection per `LocalStore`.

Contributions are welcome; read [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), and the [compatibility policy](docs/compatibility.md). Licensed under Apache-2.0.
