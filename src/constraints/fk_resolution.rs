//! Foreign key resolution with tombstone policy.
//!
//! ## Global FK Policy: Tombstone
//!
//! When a parent row is deleted while a child row references it:
//! - Parent row becomes tombstoned (not fully removed from storage)
//! - Child row survives with its FK logically valid (parent still in storage)
//! - Parent is invisible to normal SELECT queries
//! - Parent is visible to FK validation and snapshot hashing
//!
//! ## Concurrent Delete + Child Insert (CRDT Edge Case)
//!
//! Peer A deletes user u1.
//! Peer B inserts order o1 referencing u1 (offline).
//!
//! After sync with tombstone policy:
//! - u1 is tombstoned
//! - o1 survives
//! - o1.user_id still references u1 (FK is logically valid against tombstoned row)
//!
//! This is deterministic: the tombstone policy is applied globally at merge time
//! regardless of which peer you are.
//!
//! ## CRDT Properties
//! - **Commutativity**: tombstone + child-insert merge order doesn't affect outcome
//! - **Idempotence**: re-applying a delete produces the same tombstone
//! - **Determinism**: outcome depends only on the operations, not delivery order

use crate::storage::row_store::RowStore;
use crate::crdt::clocks::HlcTimestamp;

/// Global FK policy (configured once at engine startup).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FkPolicy {
    /// Parent becomes tombstoned; child survives with FK to tombstoned parent
    Tombstone,
    /// Child is deleted when parent is deleted (cascading)
    Cascade,
    /// Child survives with FK pointing to a deleted/missing parent (orphan)
    Orphan,
}

impl Default for FkPolicy {
    fn default() -> Self { FkPolicy::Tombstone }
}

/// FK resolution engine.
pub struct FkResolver {
    pub policy: FkPolicy,
}

impl FkResolver {
    pub fn new(policy: FkPolicy) -> Self {
        Self { policy }
    }

    /// Called after a merge or delete operation to enforce FK policy.
    ///
    /// For tombstone policy: ensures parent deletions tombstone rather than
    /// hard-delete, preserving FK integrity.
    ///
    /// Returns a list of (table, row_id) pairs that were affected.
    pub fn enforce_on_delete(
        &self,
        store: &mut RowStore,
        parent_table: &str,
        parent_row_id: &str,
        hlc: &HlcTimestamp,
    ) -> Vec<(String, String)> {
        match self.policy {
            FkPolicy::Tombstone => {
                // Tombstone is handled at delete time by setting fk_tombstone=true
                // No additional action needed — the OR-Map already tombstones
                vec![]
            }
            FkPolicy::Cascade => {
                self.cascade_delete(store, parent_table, parent_row_id, hlc)
            }
            FkPolicy::Orphan => {
                // No action: child survives with potentially invalid FK
                vec![]
            }
        }
    }

    /// Cascade delete: find all child rows referencing this parent and delete them.
    fn cascade_delete(
        &self,
        store: &mut RowStore,
        parent_table: &str,
        parent_row_id: &str,
        hlc: &HlcTimestamp,
    ) -> Vec<(String, String)> {
        let mut deleted = Vec::new();

        // Find all tables that have FKs referencing parent_table
        let tables: Vec<_> = store.tables.keys().cloned().collect();
        for child_table_name in tables {
            let fk_columns: Vec<(String, String)> = {
                let child_table = match store.tables.get(&child_table_name) {
                    Some(t) => t,
                    None => continue,
                };
                child_table.schema.foreign_keys.iter()
                    .filter(|fk| fk.ref_table == parent_table)
                    .map(|fk| (fk.column.clone(), fk.ref_column.clone()))
                    .collect()
            };

            if fk_columns.is_empty() { continue; }

            // Find child rows whose FK column value == parent_row_id
            let child_ids: Vec<String> = {
                let child_table = store.tables.get(&child_table_name).unwrap();
                child_table.visible_rows()
                    .iter()
                    .filter_map(|(row_id, row)| {
                        for (fk_col, _) in &fk_columns {
                            if row.read_cell(fk_col) == Some(parent_row_id.as_bytes()) {
                                return Some(row_id.to_string());
                            }
                        }
                        None
                    })
                    .collect()
            };

            for child_id in child_ids {
                let child_table = store.tables.get_mut(&child_table_name).unwrap();
                child_table.delete_row(&child_id, hlc, false);
                deleted.push((child_table_name.clone(), child_id));
            }
        }

        deleted
    }

    /// Validate a child insert against parent FK.
    /// Returns Ok if valid, Err if parent does not exist in any form.
    pub fn validate_insert(
        &self,
        store: &RowStore,
        child_fk_value: &str,
        parent_table: &str,
    ) -> Result<(), String> {
        let parent = store.get_table(parent_table)
            .ok_or_else(|| format!("Referenced table '{}' does not exist", parent_table))?;

        // Under tombstone policy: tombstoned parents are still FK-valid
        if parent.exists_for_fk(child_fk_value) {
            return Ok(());
        }

        match self.policy {
            FkPolicy::Tombstone | FkPolicy::Cascade => {
                Err(format!(
                    "FK violation: parent '{}' not found in '{}'",
                    child_fk_value, parent_table
                ))
            }
            FkPolicy::Orphan => Ok(()), // Orphan allows missing parents
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::{clocks::HlcTimestamp, merge::{TableSchema, ColumnDef, ColumnType, ForeignKeyDef}};
    use crate::storage::row_store::RowStore;

    fn ts(wall: u64, peer: &str) -> HlcTimestamp { HlcTimestamp::new(wall, 0, peer) }
    fn cell(col: &str, val: &str) -> (String, Option<Vec<u8>>) {
        (col.to_string(), Some(val.as_bytes().to_vec()))
    }

    fn users_schema() -> TableSchema {
        TableSchema {
            name: "users".into(),
            columns: vec![
                ColumnDef { name: "id".into(), col_type: ColumnType::Text, not_null: true, default_value: None, primary_key: true, unique: false },
                ColumnDef { name: "name".into(), col_type: ColumnType::Text, not_null: false, default_value: None, primary_key: false, unique: false },
            ],
            foreign_keys: vec![],
            indexes: vec![],
            composite_unique_constraints: vec![],
        }
    }

    fn orders_schema() -> TableSchema {
        TableSchema {
            name: "orders".into(),
            columns: vec![
                ColumnDef { name: "id".into(), col_type: ColumnType::Text, not_null: true, default_value: None, primary_key: true, unique: false },
                ColumnDef { name: "user_id".into(), col_type: ColumnType::Text, not_null: true, default_value: None, primary_key: false, unique: false },
                ColumnDef { name: "status".into(), col_type: ColumnType::Text, not_null: true, default_value: None, primary_key: false, unique: false },
            ],
            foreign_keys: vec![
                ForeignKeyDef { column: "user_id".into(), ref_table: "users".into(), ref_column: "id".into() },
            ],
            indexes: vec![],
            composite_unique_constraints: vec![],
        }
    }

    #[test]
    fn tombstone_parent_still_fk_valid() {
        let resolver = FkResolver::new(FkPolicy::Tombstone);
        let mut store = RowStore::new();
        store.create_table(users_schema());
        store.tables.get_mut("users").unwrap()
            .insert_row("u1", &ts(100, "A"), vec![cell("name", "Alice")]);
        store.tables.get_mut("users").unwrap()
            .delete_row("u1", &ts(200, "A"), true); // tombstoned

        // FK validation: u1 is tombstoned but still FK-valid
        let result = resolver.validate_insert(&store, "u1", "users");
        assert!(result.is_ok(), "Tombstoned parent is still FK-valid");
    }

    #[test]
    fn orphan_policy_allows_missing_parent() {
        let resolver = FkResolver::new(FkPolicy::Orphan);
        let mut store = RowStore::new();
        store.create_table(users_schema());

        let result = resolver.validate_insert(&store, "u999", "users");
        assert!(result.is_ok(), "Orphan policy allows missing parents");
    }

    #[test]
    fn cascade_deletes_children() {
        let resolver = FkResolver::new(FkPolicy::Cascade);
        let mut store = RowStore::new();
        store.create_table(users_schema());
        store.create_table(orders_schema());

        let ut = store.tables.get_mut("users").unwrap();
        ut.insert_row("u1", &ts(100, "A"), vec![cell("name", "Alice")]);
        let ot = store.tables.get_mut("orders").unwrap();
        ot.insert_row("o1", &ts(101, "A"), vec![cell("user_id", "u1"), cell("status", "pending")]);

        // Cascade delete u1
        let deleted = resolver.enforce_on_delete(&mut store, "users", "u1", &ts(200, "A"));
        assert!(deleted.contains(&("orders".to_string(), "o1".to_string())));
        assert_eq!(store.tables.get("orders").unwrap().visible_rows().len(), 0);
    }
}
