# Protocol v1

All bodies are JSON and endpoints are under `/v1`. Authentication is `Authorization: Bearer <credential-id>.<secret>`.

`POST /v1/sync/push` accepts `{ "mutations": [...] }`. Every input receives a result with `accepted`, `already_accepted`, or `rejected`, its original sequence when known, and a machine-readable error for rejection. The server may reject the HTTP request itself for authentication, body, or batch-level failures.

`GET /v1/sync/pull?after=0&limit=100` returns `{ "mutations": [{"sequence": 1, "mutation": {...}}], "cursor": 1, "has_more": false }`. Clients must persist application and the returned cursor atomically. Limits are positive and bounded by server configuration.

Credentials may be active, expired, rotated, or revoked. Expired credentials return `credential_expired`; explicitly revoked credentials return `credential_revoked`; rotation causes the old selector to return `unauthorized`. These authentication failures do not alter queued client mutations.

Changing the meaning or JSON representation of an existing v1 field is forbidden. Optional response fields and new error codes may be added. A breaking change requires `/v2`.
