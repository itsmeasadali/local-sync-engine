# Compatibility policy

Protocol `/v1` preserves meanings and serialized forms of existing fields. Readers must tolerate new optional response fields and unknown machine-readable error codes. Writers must not send fields introduced by a later capability until negotiated in a future extension. Breaking wire or merge-semantic changes use a new endpoint version.

SQLite and PostgreSQL migrations are forward-only. Before upgrading, back up server state and any client with pending outbox entries. Rust's public library API follows semantic versioning independently of the wire protocol.
