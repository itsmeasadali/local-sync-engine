use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
};

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde_json::Value;
use sync_core::{validate_mutation, FieldState, HybridLogicalClock, RecordState, ValidationLimits};
use sync_protocol::{
    ActorId, ApiError, CollectionName, FieldChange, HlcTimestamp, Mutation, PullResponse, RecordId,
    Version, WorkspaceId,
};
use uuid::Uuid;

use crate::ClientError;

const MIGRATIONS: &[&str] = &[
    include_str!("../migrations/0001_initial.sql"),
    include_str!("../migrations/0002_rejected_mutations.sql"),
];

#[derive(Clone, Debug)]
pub struct RejectedMutation {
    pub mutation: Mutation,
    pub error: ApiError,
    pub rejected_at_ms: u64,
}

#[derive(Clone)]
pub struct LocalStore {
    connection: Arc<Mutex<Connection>>,
}

impl LocalStore {
    /// Opens or creates a migrated `SQLite` database.
    ///
    /// # Errors
    ///
    /// Returns an error when `SQLite` cannot open, configure, or migrate the file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ClientError> {
        let connection = Connection::open(path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "foreign_keys", true)?;
        migrate(&connection)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    /// Creates a migrated in-memory database, primarily for tests and ephemeral tools.
    ///
    /// # Errors
    ///
    /// Returns an error when `SQLite` initialization fails.
    pub fn open_in_memory() -> Result<Self, ClientError> {
        let connection = Connection::open_in_memory()?;
        connection.pragma_update(None, "foreign_keys", true)?;
        migrate(&connection)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    /// Applies and enqueues one local mutation in a single transaction.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, HLC overflow, encoding, or `SQLite` failure.
    #[allow(clippy::too_many_arguments)]
    pub fn local_mutation(
        &self,
        workspace: &WorkspaceId,
        actor: &ActorId,
        collection: &str,
        record_id: &str,
        fields: BTreeMap<String, Value>,
        delete: bool,
        wall_ms: u64,
    ) -> Result<Mutation, ClientError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction()?;
        let last = load_hlc(&transaction, workspace, actor)?.unwrap_or_default();
        let mut clock = HybridLogicalClock::new(last);
        let timestamp = clock.tick(wall_ms)?;
        let mutation = Mutation {
            mutation_id: Uuid::new_v4(),
            workspace_id: workspace.clone(),
            actor_id: actor.clone(),
            collection: CollectionName::new(collection),
            record_id: RecordId::new(record_id),
            fields: fields
                .into_iter()
                .map(|(name, value)| (name, FieldChange { value }))
                .collect(),
            delete,
            version: Version {
                timestamp,
                actor_id: actor.clone(),
            },
        };
        validate_mutation(&mutation, ValidationLimits::default())?;
        apply_mutation(&transaction, &mutation)?;
        save_hlc(&transaction, workspace, actor, timestamp)?;
        transaction.execute("INSERT INTO outbox (mutation_id, workspace_id, mutation_json, created_order) VALUES (?1,?2,?3,COALESCE((SELECT MAX(created_order)+1 FROM outbox),1))", params![mutation.mutation_id.to_string(), workspace.as_str(), serde_json::to_string(&mutation)?])?;
        transaction.execute(
            "INSERT OR IGNORE INTO applied_mutations (workspace_id, mutation_id) VALUES (?1,?2)",
            params![workspace.as_str(), mutation.mutation_id.to_string()],
        )?;
        transaction.commit()?;
        Ok(mutation)
    }

    /// Returns at most `limit` pending mutations in durable insertion order.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt stored JSON or an `SQLite` failure.
    pub fn pending(
        &self,
        workspace: &WorkspaceId,
        limit: usize,
    ) -> Result<Vec<Mutation>, ClientError> {
        let connection = self.lock()?;
        let mut statement = connection.prepare("SELECT mutation_json FROM outbox WHERE workspace_id = ?1 ORDER BY created_order LIMIT ?2")?;
        let sql_limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = statement.query_map(params![workspace.as_str(), sql_limit], |row| {
            row.get::<_, String>(0)
        })?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    /// Removes server-acknowledged mutations atomically.
    ///
    /// # Errors
    ///
    /// Returns an error if the `SQLite` transaction fails.
    pub fn acknowledge(&self, mutation_ids: &[Uuid]) -> Result<(), ClientError> {
        self.complete_push(mutation_ids, &[])
    }

    pub(crate) fn complete_push(
        &self,
        acknowledged: &[Uuid],
        rejected: &[(Mutation, ApiError)],
    ) -> Result<(), ClientError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction()?;
        for id in acknowledged {
            transaction.execute(
                "DELETE FROM outbox WHERE mutation_id = ?1",
                [id.to_string()],
            )?;
        }
        let rejected_at_ms = to_i64(current_time_ms()?)?;
        for (mutation, error) in rejected {
            transaction.execute(
                "INSERT INTO rejected_mutations (mutation_id,workspace_id,mutation_json,error_json,rejected_at_ms) VALUES (?1,?2,?3,?4,?5) ON CONFLICT(mutation_id) DO UPDATE SET error_json=excluded.error_json,rejected_at_ms=excluded.rejected_at_ms",
                params![
                    mutation.mutation_id.to_string(),
                    mutation.workspace_id.as_str(),
                    serde_json::to_string(mutation)?,
                    serde_json::to_string(error)?,
                    rejected_at_ms,
                ],
            )?;
            transaction.execute(
                "DELETE FROM outbox WHERE mutation_id = ?1",
                [mutation.mutation_id.to_string()],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Lists mutations quarantined after an explicit server rejection.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt stored data or an `SQLite` failure.
    pub fn rejected(&self, workspace: &WorkspaceId) -> Result<Vec<RejectedMutation>, ClientError> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(
            "SELECT mutation_json,error_json,rejected_at_ms FROM rejected_mutations WHERE workspace_id=?1 ORDER BY rejected_at_ms,mutation_id",
        )?;
        let rows = statement.query_map([workspace.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;
        rows.map(|row| {
            let (mutation, error, rejected_at_ms) = row?;
            Ok(RejectedMutation {
                mutation: serde_json::from_str(&mutation)?,
                error: serde_json::from_str(&error)?,
                rejected_at_ms: u64::try_from(rejected_at_ms)
                    .map_err(|_| ClientError::IntegerRange)?,
            })
        })
        .collect()
    }

    /// Counts currently quarantined mutations for a workspace.
    ///
    /// # Errors
    ///
    /// Returns an error if `SQLite` cannot execute the query.
    pub fn rejected_len(&self, workspace: &WorkspaceId) -> Result<usize, ClientError> {
        let connection = self.lock()?;
        let count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM rejected_mutations WHERE workspace_id=?1",
            [workspace.as_str()],
            |row| row.get(0),
        )?;
        usize::try_from(count).map_err(|_| ClientError::IntegerRange)
    }

    /// Moves one quarantined mutation back to the end of the outbox.
    ///
    /// # Errors
    ///
    /// Returns an error if the mutation is absent or `SQLite` cannot commit the move.
    pub fn retry_rejected(
        &self,
        workspace: &WorkspaceId,
        mutation_id: Uuid,
    ) -> Result<(), ClientError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction()?;
        let inserted = transaction.execute(
            "INSERT INTO outbox (mutation_id,workspace_id,mutation_json,created_order) SELECT mutation_id,workspace_id,mutation_json,COALESCE((SELECT MAX(created_order)+1 FROM outbox),1) FROM rejected_mutations WHERE workspace_id=?1 AND mutation_id=?2",
            params![workspace.as_str(), mutation_id.to_string()],
        )?;
        if inserted == 0 {
            return Err(ClientError::RejectedMutationNotFound(mutation_id));
        }
        transaction.execute(
            "DELETE FROM rejected_mutations WHERE workspace_id=?1 AND mutation_id=?2",
            params![workspace.as_str(), mutation_id.to_string()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Permanently discards one quarantined mutation.
    ///
    /// # Errors
    ///
    /// Returns an error if the mutation is absent or `SQLite` cannot commit the deletion.
    pub fn discard_rejected(
        &self,
        workspace: &WorkspaceId,
        mutation_id: Uuid,
    ) -> Result<(), ClientError> {
        let connection = self.lock()?;
        let deleted = connection.execute(
            "DELETE FROM rejected_mutations WHERE workspace_id=?1 AND mutation_id=?2",
            params![workspace.as_str(), mutation_id.to_string()],
        )?;
        if deleted == 0 {
            return Err(ClientError::RejectedMutationNotFound(mutation_id));
        }
        Ok(())
    }

    /// Reads the durable pull cursor, returning zero for a new workspace.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid stored data or an `SQLite` failure.
    pub fn cursor(&self, workspace: &WorkspaceId) -> Result<u64, ClientError> {
        let connection = self.lock()?;
        let value = connection
            .query_row(
                "SELECT cursor FROM pull_cursors WHERE workspace_id = ?1",
                [workspace.as_str()],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .unwrap_or(0);
        u64::try_from(value).map_err(|_| ClientError::IntegerRange)
    }

    /// Applies a validated pull page and advances its cursor in one transaction.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid mutations, corrupt state, HLC overflow, or `SQLite` failure.
    pub fn apply_page(
        &self,
        workspace: &WorkspaceId,
        local_actor: &ActorId,
        page: &PullResponse,
    ) -> Result<(), ClientError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction()?;
        let mut clock = HybridLogicalClock::new(
            load_hlc(&transaction, workspace, local_actor)?.unwrap_or_default(),
        );
        let wall_ms = current_time_ms()?;
        for item in &page.mutations {
            if item.mutation.workspace_id != *workspace {
                return Err(ClientError::InvalidResponse(
                    "server returned a mutation for another workspace".into(),
                ));
            }
            let exists = transaction
                .query_row(
                    "SELECT 1 FROM applied_mutations WHERE workspace_id=?1 AND mutation_id=?2",
                    params![workspace.as_str(), item.mutation.mutation_id.to_string()],
                    |_| Ok(()),
                )
                .optional()?
                .is_some();
            if !exists {
                validate_mutation(&item.mutation, ValidationLimits::default())?;
                apply_mutation(&transaction, &item.mutation)?;
                transaction.execute(
                    "INSERT INTO applied_mutations (workspace_id, mutation_id) VALUES (?1,?2)",
                    params![workspace.as_str(), item.mutation.mutation_id.to_string()],
                )?;
            }
            clock.observe(item.mutation.version.timestamp, wall_ms)?;
        }
        save_hlc(&transaction, workspace, local_actor, clock.last())?;
        let cursor = i64::try_from(page.cursor).map_err(|_| ClientError::IntegerRange)?;
        transaction.execute("INSERT INTO pull_cursors (workspace_id,cursor) VALUES (?1,?2) ON CONFLICT(workspace_id) DO UPDATE SET cursor=excluded.cursor WHERE excluded.cursor >= pull_cursors.cursor", params![workspace.as_str(), cursor])?;
        transaction.commit()?;
        Ok(())
    }

    /// Materializes the currently visible JSON object for one record.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt state or an `SQLite` failure.
    pub fn record(
        &self,
        workspace: &WorkspaceId,
        collection: &str,
        record_id: &str,
    ) -> Result<Option<Value>, ClientError> {
        let connection = self.lock()?;
        Ok(load_record(&connection, workspace, collection, record_id)?
            .and_then(|state| state.materialize()))
    }

    /// Counts pending mutations for a workspace.
    ///
    /// # Errors
    ///
    /// Returns an error if `SQLite` cannot execute the query.
    pub fn outbox_len(&self, workspace: &WorkspaceId) -> Result<usize, ClientError> {
        let connection = self.lock()?;
        let count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM outbox WHERE workspace_id=?1",
            [workspace.as_str()],
            |row| row.get(0),
        )?;
        usize::try_from(count).map_err(|_| ClientError::IntegerRange)
    }

    fn lock(&self) -> Result<MutexGuard<'_, Connection>, ClientError> {
        self.connection
            .lock()
            .map_err(|_| ClientError::Transport("SQLite connection mutex was poisoned".into()))
    }
}

fn migrate(connection: &Connection) -> Result<(), ClientError> {
    for migration in MIGRATIONS {
        connection.execute_batch(migration)?;
    }
    Ok(())
}

fn apply_mutation(transaction: &Transaction<'_>, mutation: &Mutation) -> Result<(), ClientError> {
    let mut state = load_record(
        transaction,
        &mutation.workspace_id,
        mutation.collection.as_str(),
        mutation.record_id.as_str(),
    )?
    .unwrap_or_default();
    state.apply(mutation);
    transaction.execute("INSERT INTO records (workspace_id,collection_name,record_id) VALUES (?1,?2,?3) ON CONFLICT DO NOTHING", params![mutation.workspace_id.as_str(), mutation.collection.as_str(), mutation.record_id.as_str()])?;
    if let Some(tombstone) = &state.tombstone {
        transaction.execute("UPDATE records SET tombstone_physical_ms=?4,tombstone_logical=?5,tombstone_actor_id=?6 WHERE workspace_id=?1 AND collection_name=?2 AND record_id=?3", params![mutation.workspace_id.as_str(), mutation.collection.as_str(), mutation.record_id.as_str(), to_i64(tombstone.timestamp.physical_ms)?, i64::from(tombstone.timestamp.logical), tombstone.actor_id.as_str()])?;
    }
    for (name, field) in state.fields {
        transaction.execute("INSERT INTO record_fields (workspace_id,collection_name,record_id,field_name,value_json,version_physical_ms,version_logical,version_actor_id) VALUES (?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(workspace_id,collection_name,record_id,field_name) DO UPDATE SET value_json=excluded.value_json,version_physical_ms=excluded.version_physical_ms,version_logical=excluded.version_logical,version_actor_id=excluded.version_actor_id", params![mutation.workspace_id.as_str(), mutation.collection.as_str(), mutation.record_id.as_str(), name, serde_json::to_string(&field.value)?, to_i64(field.version.timestamp.physical_ms)?, i64::from(field.version.timestamp.logical), field.version.actor_id.as_str()])?;
    }
    Ok(())
}

fn load_record(
    connection: &Connection,
    workspace: &WorkspaceId,
    collection: &str,
    record_id: &str,
) -> Result<Option<RecordState>, ClientError> {
    let tombstone = connection.query_row("SELECT tombstone_physical_ms,tombstone_logical,tombstone_actor_id FROM records WHERE workspace_id=?1 AND collection_name=?2 AND record_id=?3", params![workspace.as_str(), collection, record_id], |row| {
        let physical: Option<i64> = row.get(0)?; let logical: Option<i64> = row.get(1)?; let actor: Option<String> = row.get(2)?;
        Ok(match (physical, logical, actor) { (Some(p), Some(l), Some(a)) => Some((p,l,a)), _ => None })
    }).optional()?;
    let Some(tombstone) = tombstone else {
        return Ok(None);
    };
    let tombstone = tombstone
        .map(|(p, l, a)| version_from_db(p, l, a))
        .transpose()?;
    let mut statement = connection.prepare("SELECT field_name,value_json,version_physical_ms,version_logical,version_actor_id FROM record_fields WHERE workspace_id=?1 AND collection_name=?2 AND record_id=?3")?;
    let rows = statement.query_map(params![workspace.as_str(), collection, record_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, String>(4)?,
        ))
    })?;
    let mut fields = BTreeMap::new();
    for row in rows {
        let (name, value, p, l, actor) = row?;
        fields.insert(
            name,
            FieldState {
                value: serde_json::from_str(&value)?,
                version: version_from_db(p, l, actor)?,
            },
        );
    }
    Ok(Some(RecordState { fields, tombstone }))
}

fn load_hlc(
    connection: &Connection,
    workspace: &WorkspaceId,
    actor: &ActorId,
) -> Result<Option<HlcTimestamp>, ClientError> {
    connection
        .query_row(
            "SELECT physical_ms,logical FROM hlc_state WHERE workspace_id=?1 AND actor_id=?2",
            params![workspace.as_str(), actor.as_str()],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?
        .map(|(p, l)| timestamp_from_db(p, l))
        .transpose()
}

fn save_hlc(
    transaction: &Transaction<'_>,
    workspace: &WorkspaceId,
    actor: &ActorId,
    timestamp: HlcTimestamp,
) -> Result<(), ClientError> {
    transaction.execute("INSERT INTO hlc_state (workspace_id,actor_id,physical_ms,logical) VALUES (?1,?2,?3,?4) ON CONFLICT(workspace_id,actor_id) DO UPDATE SET physical_ms=excluded.physical_ms,logical=excluded.logical", params![workspace.as_str(), actor.as_str(), to_i64(timestamp.physical_ms)?, i64::from(timestamp.logical)])?;
    Ok(())
}

fn version_from_db(physical: i64, logical: i64, actor: String) -> Result<Version, ClientError> {
    Ok(Version {
        timestamp: timestamp_from_db(physical, logical)?,
        actor_id: ActorId::new(actor),
    })
}
fn timestamp_from_db(physical: i64, logical: i64) -> Result<HlcTimestamp, ClientError> {
    Ok(HlcTimestamp {
        physical_ms: u64::try_from(physical).map_err(|_| ClientError::IntegerRange)?,
        logical: u32::try_from(logical).map_err(|_| ClientError::IntegerRange)?,
    })
}
fn to_i64(value: u64) -> Result<i64, ClientError> {
    i64::try_from(value).map_err(|_| ClientError::IntegerRange)
}
fn current_time_ms() -> Result<u64, ClientError> {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| ClientError::InvalidSystemClock)?
            .as_millis(),
    )
    .map_err(|_| ClientError::IntegerRange)
}
