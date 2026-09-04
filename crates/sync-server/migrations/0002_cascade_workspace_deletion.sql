ALTER TABLE mutations
    DROP CONSTRAINT mutations_workspace_id_actor_id_fkey,
    ADD CONSTRAINT mutations_workspace_id_actor_id_fkey
        FOREIGN KEY (workspace_id, actor_id)
        REFERENCES actors(workspace_id, actor_id)
        ON DELETE CASCADE;
