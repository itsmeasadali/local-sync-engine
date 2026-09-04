# Changelog

This project follows semantic versioning for Rust crates and separately preserves compatibility within HTTP protocol `/v1`.

## Unreleased

- Added the initial field-level LWW synchronization protocol and merge engine.
- Added restart-safe SQLite outbox and cursor persistence.
- Added the PostgreSQL/Axum sync server with tenant-scoped credentials.
- Added a high-level `SyncDatabase` interface and observable background synchronization.
- Added rejected-mutation quarantine with inspect, retry, and discard flows.
- Added workspace and actor credential administration, rotation, expiry, and revocation.
