# Architecture

The client owns a SQLite materialized view, HLC state, durable pull cursor, applied-mutation set, and outbox. A local write ticks the HLC, applies the mutation, persists the HLC, and inserts the outbox row in one transaction. Reads never depend on the network.

The server authenticates a credential to exactly one `(workspace, actor)`, validates each mutation, assigns accepted mutations a gap-free monotonically increasing sequence while holding the workspace row lock, and stores the mutation once. It does not materialize records; conflict resolution happens identically in every client through `sync-core`.

Sync first pushes a bounded snapshot of the outbox. Accepted and already-accepted items are removed transactionally. It then pulls immutable pages after the durable cursor. Applying all page mutations, recording their IDs, observing their HLCs, and advancing the cursor is one SQLite transaction. A crash before commit repeats the page; a crash after commit continues after it.

An explicitly rejected mutation is atomically moved from the outbox to `rejected_mutations` with the complete server error. This quarantine prevents one permanent error from blocking later writes while preserving the original mutation for inspection or retry. Discard requires an explicit caller action.

`SyncDatabase` is the primary application interface. It hides workspace scoping, actor versions, mutation construction, transport wiring, sync serialization, and recovery. `SyncClient<T>` remains the advanced seam: the HTTP adapter is used in production and in-memory adapters exercise identical orchestration in tests. Background synchronization is observable through status changes and uses bounded exponential retry with jitter.

## Invariants

- A version is `(physical milliseconds, logical counter, actor ID)` in lexicographic order.
- Per-field state and the record tombstone are max-registers under that total order.
- A field is visible only when its version is strictly newer than the tombstone.
- `mutation_id` is unique per workspace; duplicate payloads return their original sequence.
- A duplicate ID with a different payload is rejected rather than silently accepted.
- Pull uses `sequence > after ORDER BY sequence LIMIT limit + 1`; the returned cursor is exactly the last returned sequence.
- Tenant identity comes only from the verified credential, never from a query parameter.
- Credential rotation replaces the selector and hash atomically; revocation never deletes mutation history.

See the ADRs for consequences and alternatives.
