PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS records (
    workspace_id TEXT NOT NULL,
    collection_name TEXT NOT NULL,
    record_id TEXT NOT NULL,
    tombstone_physical_ms INTEGER,
    tombstone_logical INTEGER,
    tombstone_actor_id TEXT,
    PRIMARY KEY (workspace_id, collection_name, record_id),
    CHECK ((tombstone_physical_ms IS NULL) = (tombstone_logical IS NULL)),
    CHECK ((tombstone_physical_ms IS NULL) = (tombstone_actor_id IS NULL))
);
CREATE TABLE IF NOT EXISTS record_fields (
    workspace_id TEXT NOT NULL,
    collection_name TEXT NOT NULL,
    record_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    value_json TEXT NOT NULL CHECK (json_valid(value_json)),
    version_physical_ms INTEGER NOT NULL CHECK (version_physical_ms >= 0),
    version_logical INTEGER NOT NULL CHECK (version_logical >= 0),
    version_actor_id TEXT NOT NULL,
    PRIMARY KEY (workspace_id, collection_name, record_id, field_name),
    FOREIGN KEY (workspace_id, collection_name, record_id)
      REFERENCES records(workspace_id, collection_name, record_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS outbox (
    mutation_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    mutation_json TEXT NOT NULL CHECK (json_valid(mutation_json)),
    created_order INTEGER NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS outbox_workspace_order_idx ON outbox(workspace_id, created_order);

CREATE TABLE IF NOT EXISTS applied_mutations (
    workspace_id TEXT NOT NULL,
    mutation_id TEXT NOT NULL,
    PRIMARY KEY (workspace_id, mutation_id)
);

CREATE TABLE IF NOT EXISTS pull_cursors (
    workspace_id TEXT PRIMARY KEY,
    cursor INTEGER NOT NULL CHECK (cursor >= 0)
);

CREATE TABLE IF NOT EXISTS hlc_state (
    workspace_id TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    physical_ms INTEGER NOT NULL CHECK (physical_ms >= 0),
    logical INTEGER NOT NULL CHECK (logical >= 0),
    PRIMARY KEY (workspace_id, actor_id)
);
