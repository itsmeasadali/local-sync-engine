CREATE TABLE workspaces (
    workspace_id TEXT PRIMARY KEY CHECK (length(workspace_id) BETWEEN 1 AND 255),
    current_sequence BIGINT NOT NULL DEFAULT 0 CHECK (current_sequence >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE actors (
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    actor_id TEXT NOT NULL CHECK (length(actor_id) BETWEEN 1 AND 255),
    credential_id UUID NOT NULL UNIQUE,
    credential_hash TEXT NOT NULL,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, actor_id)
);

CREATE TABLE mutations (
    workspace_id TEXT NOT NULL,
    mutation_id UUID NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    actor_id TEXT NOT NULL,
    collection TEXT NOT NULL CHECK (length(collection) BETWEEN 1 AND 255),
    record_id TEXT NOT NULL CHECK (length(record_id) BETWEEN 1 AND 255),
    hlc_physical_ms BIGINT NOT NULL CHECK (hlc_physical_ms >= 0),
    hlc_logical BIGINT NOT NULL CHECK (hlc_logical BETWEEN 0 AND 4294967295),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, mutation_id),
    UNIQUE (workspace_id, sequence),
    FOREIGN KEY (workspace_id, actor_id) REFERENCES actors(workspace_id, actor_id)
);

CREATE INDEX mutations_pull_idx ON mutations (workspace_id, sequence);
