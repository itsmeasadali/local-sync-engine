# Operations

## Configure and run

Required: `SYNC_DATABASE_URL`. Defaults listen on `127.0.0.1:8080`, allow 100 mutations per push, 500 per pull, 2 MiB bodies, five minutes future skew, and 30-second requests. All defaults can be overridden with the `SYNC_*` variables documented in `sync-server/src/config.rs`.

For first-run development only, set `SYNC_BOOTSTRAP_WORKSPACE`, `SYNC_BOOTSTRAP_ACTOR`, and `SYNC_BOOTSTRAP_TOKEN=<uuid>.<secret>` together. Startup creates or rotates that actor credential. Do not use bootstrap credentials in production.

Production operators can provision without handling hashes directly:

```sh
sync-server workspace-create <workspace-id>
sync-server actor-create <workspace-id> <actor-id> --expires-in-days 90
sync-server actor-list <workspace-id>
sync-server actor-rotate <workspace-id> <actor-id> --expires-in-days 90
sync-server actor-revoke <workspace-id> <actor-id>
```

Create and rotate print the bearer token once. Deliver it over a secure channel and do not retain it in logs or shell history. Rotation invalidates the previous token. Revocation takes effect on the next request and preserves immutable mutation history.

The server applies checked-in migrations at startup. Deploy schema changes before older binaries are removed and preserve v1 compatibility.

## Observe

- `/health/live` reports process liveness without touching dependencies.
- `/health/ready` executes `SELECT 1` and returns 503 when PostgreSQL is unavailable.
- `/metrics` exposes OpenMetrics counters and readiness.
- JSON logs should be collected with `RUST_LOG=info`; request tracing deliberately omits bodies and authorization headers.

Clients should alert when background status remains `Retrying`, outbox age grows, or `SyncReport::rejected` is nonzero. Inspect quarantined mutations before retrying or discarding them.

## Back up and restore

Back up PostgreSQL with `pg_dump --format=custom --dbname "$SYNC_DATABASE_URL" --file sync.dump`. Restore into an empty compatible server using `pg_restore --clean --if-exists --dbname "$RESTORE_DATABASE_URL" sync.dump`, verify workspace row counts and maximum sequences, then point stopped application instances at the restored database.

SQLite databases are client caches but contain unsent writes. Copy them only after stopping the client or use SQLite's online backup API. Never discard a client database with a non-empty outbox. After restore, clients safely resume from their durable cursor; restoring an older server snapshot can require resetting affected client cursors or restoring the missing mutation log.

## Shutdown and upgrades

Send SIGTERM. The listener stops accepting work and waits for active requests. PostgreSQL transactions either commit fully or roll back. Roll back binaries only when their protocol and migration compatibility matches the deployed schema.
