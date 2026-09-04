use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sync_protocol::{Mutation, Version};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FieldState {
    pub value: Value,
    pub version: Version,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct RecordState {
    pub fields: BTreeMap<String, FieldState>,
    pub tombstone: Option<Version>,
}

impl RecordState {
    /// Joins a mutation into this state. Versions form a total order through the actor tie-breaker.
    pub fn apply(&mut self, mutation: &Mutation) {
        if mutation.delete
            && self
                .tombstone
                .as_ref()
                .is_none_or(|current| mutation.version > *current)
        {
            self.tombstone = Some(mutation.version.clone());
        }
        for (name, change) in &mutation.fields {
            let replace = self
                .fields
                .get(name)
                .is_none_or(|current| mutation.version > current.version);
            if replace {
                self.fields.insert(
                    name.clone(),
                    FieldState {
                        value: change.value.clone(),
                        version: mutation.version.clone(),
                    },
                );
            }
        }
    }

    /// State-based join used for repair and tests. It is associative, commutative and idempotent.
    #[must_use]
    pub fn merge(&self, other: &Self) -> Self {
        let mut merged = self.clone();
        if let Some(version) = &other.tombstone {
            if merged.tombstone.as_ref().is_none_or(|v| version > v) {
                merged.tombstone = Some(version.clone());
            }
        }
        for (name, field) in &other.fields {
            if merged
                .fields
                .get(name)
                .is_none_or(|v| field.version > v.version)
            {
                merged.fields.insert(name.clone(), field.clone());
            }
        }
        merged
    }

    #[must_use]
    pub fn materialize(&self) -> Option<Value> {
        let visible: Map<String, Value> = self
            .fields
            .iter()
            .filter(|(_, field)| {
                self.tombstone
                    .as_ref()
                    .is_none_or(|deleted_at| field.version > *deleted_at)
            })
            .map(|(name, field)| (name.clone(), field.value.clone()))
            .collect();
        if visible.is_empty() && self.tombstone.is_some() {
            None
        } else {
            Some(Value::Object(visible))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;
    use serde_json::json;
    use sync_protocol::{
        ActorId, CollectionName, FieldChange, HlcTimestamp, RecordId, WorkspaceId,
    };
    use uuid::Uuid;

    use super::*;

    fn mutation(actor: &str, physical_ms: u64, logical: u32, field: &str, value: i64) -> Mutation {
        Mutation {
            mutation_id: Uuid::new_v4(),
            workspace_id: WorkspaceId::new("w"),
            actor_id: ActorId::new(actor),
            collection: CollectionName::new("c"),
            record_id: RecordId::new("r"),
            fields: BTreeMap::from([(
                field.to_owned(),
                FieldChange {
                    value: json!(value),
                },
            )]),
            delete: false,
            version: Version {
                timestamp: HlcTimestamp {
                    physical_ms,
                    logical,
                },
                actor_id: ActorId::new(actor),
            },
        }
    }

    fn state(items: &[(u64, u32, u8, u8, i64, bool)]) -> RecordState {
        let mut result = RecordState::default();
        for &(physical, logical, actor, field, value, delete) in items {
            let mut operation = mutation(
                &format!("a{actor}"),
                physical,
                logical,
                &format!("f{field}"),
                value,
            );
            if delete {
                operation.fields.clear();
                operation.delete = true;
            }
            result.apply(&operation);
        }
        result
    }

    prop_compose! {
        fn state_strategy()(items in prop::collection::vec((0_u64..1000, 0_u32..20, 0_u8..5, 0_u8..5, any::<i64>(), any::<bool>()), 0..30)) -> RecordState {
            state(&items)
        }
    }

    proptest! {
        #[test]
        fn merge_is_idempotent(a in state_strategy()) { prop_assert_eq!(a.merge(&a), a); }
        #[test]
        fn merge_is_commutative(a in state_strategy(), b in state_strategy()) { prop_assert_eq!(a.merge(&b), b.merge(&a)); }
        #[test]
        fn merge_is_associative(a in state_strategy(), b in state_strategy(), c in state_strategy()) {
            prop_assert_eq!(a.merge(&b).merge(&c), a.merge(&b.merge(&c)));
        }
        #[test]
        fn delivery_order_is_deterministic(mut items in prop::collection::vec((0_u64..1000, 0_u32..20, 0_u8..5, 0_u8..5, any::<i64>(), any::<bool>()), 0..30)) {
            let forward = state(&items);
            items.reverse();
            prop_assert_eq!(forward, state(&items));
        }
    }

    #[test]
    fn different_fields_survive_and_actor_breaks_ties() {
        let mut record = RecordState::default();
        record.apply(&mutation("a", 1, 0, "left", 1));
        record.apply(&mutation("b", 1, 0, "right", 2));
        record.apply(&mutation("b", 1, 0, "left", 3));
        assert_eq!(record.materialize(), Some(json!({"left": 3, "right": 2})));
    }

    #[test]
    fn stale_update_cannot_resurrect_delete() {
        let mut record = RecordState::default();
        record.apply(&mutation("a", 1, 0, "title", 1));
        let mut delete = mutation("a", 3, 0, "unused", 0);
        delete.fields.clear();
        delete.delete = true;
        record.apply(&delete);
        record.apply(&mutation("z", 2, 0, "title", 2));
        assert_eq!(record.materialize(), None);
        record.apply(&mutation("a", 4, 0, "title", 4));
        assert_eq!(record.materialize(), Some(json!({"title": 4})));
    }
}
