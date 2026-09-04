# ADR 0005: Server trust boundary

Status: accepted.

Clients choose mutation IDs and HLC timestamps but are not trusted for tenancy, actor identity, size, shape, or reasonable time. Bearer credentials use `credential_id.secret`; the ID locates an actor and the secret is verified against an Argon2id hash. Expiry, workspace, actor, batch, body, identifier, JSON, and future-clock limits are checked before acceptance. Logs never include credentials or record payloads.

The server is trusted to assign ordered sequences and retain accepted mutations. TLS is expected at the deployment boundary. A compromised workspace credential can read that workspace's mutation history and write as its actor; rotate it by replacing the actor hash and expiry.
