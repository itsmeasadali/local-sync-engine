CREATE TABLE IF NOT EXISTS rejected_mutations (
    mutation_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    mutation_json TEXT NOT NULL CHECK (json_valid(mutation_json)),
    error_json TEXT NOT NULL CHECK (json_valid(error_json)),
    rejected_at_ms INTEGER NOT NULL CHECK (rejected_at_ms >= 0)
);

CREATE INDEX IF NOT EXISTS rejected_mutations_workspace_idx
    ON rejected_mutations(workspace_id, rejected_at_ms);
