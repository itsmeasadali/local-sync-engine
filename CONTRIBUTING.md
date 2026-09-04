# Contributing

Thank you for improving Local Sync Engine. Start with an issue for protocol or merge-semantic changes so compatibility can be discussed before implementation. Small bug fixes and documentation improvements may go directly to a pull request.

## Development

Install Rust 1.88 or newer and PostgreSQL 16. Then run:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Database tests run when `TEST_DATABASE_URL` points at an isolated PostgreSQL database. Never point tests at production data.

```sh
createdb local_sync_engine_test
TEST_DATABASE_URL=postgresql://localhost/local_sync_engine_test cargo test --workspace
```

Changes to synchronization semantics require property or convergence tests, an ADR under `docs/adr/`, and a protocol compatibility assessment. Changes to HTTP types require a matching `openapi.yaml` update.

Use focused commits, explain observable behavior in the pull request, and do not include credentials or production record contents in fixtures or logs.
