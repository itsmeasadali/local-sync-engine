# Threat model

## Protected assets

Workspace mutation contents, tenant boundaries, credential secrets, sequence integrity, and availability are protected. PostgreSQL administrators and the server process are trusted; clients, networks before TLS termination, and all request input are untrusted.

## Controls

- Argon2id hashes protect stored bearer secrets; credential IDs are selectors, not secrets.
- Every mutation workspace and actor must match authenticated identity. Every log query includes workspace scope and foreign keys preserve actor ownership.
- Body, batch, identifier, field, pull, timeout, and future-clock limits constrain abuse.
- UUID deduplication plus payload comparison prevents mutation-ID equivocation.
- Rotation replaces the stored credential selector and hash; revocation is checked on every request without erasing actor history.
- Structured logs include metadata but never bearer values or mutation bodies.
- TLS, database encryption, rate limiting, and denial-of-service controls belong at the deployment edge.

## Residual risks

An actor credential grants read access to the complete workspace log. LWW exposes client-selected time within the skew window and may discard concurrent same-field intent. PostgreSQL compromise exposes record content. v1 does not provide end-to-end encryption, fine-grained collection authorization, log compaction, a persistent credential-administration audit log, or replay-window limits.
