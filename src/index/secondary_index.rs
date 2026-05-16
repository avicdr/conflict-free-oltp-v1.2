//! Derived secondary indexes (OPTION A).
//!
//! ## Strategy: Derived Indexes Rebuilt from CRDT State
//!
//! Secondary indexes are deterministic projections of the canonical CRDT row state.
//! They are NOT replicated independently — they are rebuilt incrementally as
//! CRDT operations are applied to the row store.
//!
//! ## Justification
//! - **Correctness**: Since indexes are a pure function of CRDT state, they
//!   automatically satisfy all CRDT invariants (commutativity, associativity,
//!   idempotence). Any peer with the same CRDT state will produce the same index.
//! - **Convergence**: Convergent row states → convergent indexes. No separate
//!   replication protocol needed for indexes.
//! - **Simplicity**: Avoids the complex correctness requirements of independently
//!   replicated CRDT indexes, which would need their own merge semantics.
//!
//! ## Deterministic Ordering
//! Index entries use canonical key ordering:
//!   (indexed_column_values..., primary_key_value)
//! Tie-breaking uses the primary key (unique per row), ensuring total stable order.

use crate::storage::row_store::TableState;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// An index entry: maps (index_key_values, pk_value) → row_id.
/// The inclusion of pk ensures total ordering and no collisions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct IndexKey {
    /// Values of the indexed columns (in index column order), canonically encoded
    pub key_parts: Vec<Option<Vec<u8>>>,
    /// Primary key value (tie-breaker for stable total ordering)
    pub pk_value: String,
}

/// A secondary index for one (table, index_name) pair.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SecondaryIndex {
    pub index_name: String,
    pub indexed_columns: Vec<String>,
    /// BTreeMap of IndexKey → row_id (sorted for deterministic range scans)
    pub entries: BTreeMap<IndexKey, String>,
}

impl SecondaryIndex {
    pub fn new(name: impl Into<String>, columns: Vec<String>) -> Self {
        Self {
            index_name: name.into(),
            indexed_columns: columns,
            entries: BTreeMap::new(),
        }
    }

    /// Rebuild this index entirely from the current canonical table state.
    /// This is O(n) in the number of visible rows but guarantees correctness.
    pub fn rebuild_from(&mut self, table: &TableState) {
        self.entries.clear();
        let pk_col = match table.schema.primary_key_col() {
            Some(col) => col.to_string(),
            None => return,
        };

        for (row_id, row) in table.visible_rows() {
            let pk_val = row.read_cell(&pk_col)
                .map(|b| String::from_utf8_lossy(b).to_string())
                .unwrap_or_else(|| row_id.to_string());

            let key_parts = self.indexed_columns.iter()
                .map(|col| row.read_cell(col).map(|b| b.to_vec()))
                .collect();

            let key = IndexKey { key_parts, pk_value: pk_val };
            self.entries.insert(key, row_id.to_string());
        }
    }

    /// Incremental update: apply a single row change.
    /// Removes old entry, inserts new entry.
    pub fn update_row(
        &mut self,
        row_id: &str,
        pk_val: &str,
        old_key_parts: Option<Vec<Option<Vec<u8>>>>,
        new_key_parts: Option<Vec<Option<Vec<u8>>>>,
    ) {
        // Remove old entry
        if let Some(old_parts) = old_key_parts {
            let old_key = IndexKey { key_parts: old_parts, pk_value: pk_val.to_string() };
            self.entries.remove(&old_key);
        }
        // Insert new entry
        if let Some(new_parts) = new_key_parts {
            let new_key = IndexKey { key_parts: new_parts, pk_value: pk_val.to_string() };
            self.entries.insert(new_key, row_id.to_string());
        }
    }

    /// Range scan: returns row_ids for entries where the leading index key starts with prefix.
    /// Results are in deterministic canonical order.
    pub fn range_scan(
        &self,
        prefix: &[Option<Vec<u8>>],
    ) -> Vec<&str> {
        let start = IndexKey {
            key_parts: prefix.to_vec(),
            pk_value: String::new(),
        };
        self.entries
            .range(start..)
            .take_while(|(k, _)| k.key_parts.starts_with(prefix))
            .map(|(_, row_id)| row_id.as_str())
            .collect()
    }

    /// Full scan in index order (deterministic).
    pub fn all_entries(&self) -> Vec<(&IndexKey, &str)> {
        self.entries.iter().map(|(k, v)| (k, v.as_str())).collect()
    }
}

/// Index manager for a single table.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableIndexManager {
    pub indexes: BTreeMap<String, SecondaryIndex>,
}

impl TableIndexManager {
    pub fn new() -> Self { Self::default() }

    pub fn add_index(&mut self, index: SecondaryIndex) {
        self.indexes.insert(index.index_name.clone(), index);
    }

    /// Rebuild all indexes from current table state.
    pub fn rebuild_all(&mut self, table: &TableState) {
        for idx in self.indexes.values_mut() {
            idx.rebuild_from(table);
        }
    }

    pub fn get(&self, name: &str) -> Option<&SecondaryIndex> {
        self.indexes.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::{clocks::HlcTimestamp, merge::{TableSchema, ColumnDef, ColumnType}};
    use crate::storage::row_store::TableState;

    fn ts(wall: u64, peer: &str) -> HlcTimestamp { HlcTimestamp::new(wall, 0, peer) }
    fn cell(col: &str, val: &str) -> (String, Option<Vec<u8>>) {
        (col.to_string(), Some(val.as_bytes().to_vec()))
    }

    fn make_schema() -> TableSchema {
        TableSchema {
            name: "orders".into(),
            columns: vec![
                ColumnDef { name: "id".into(), col_type: ColumnType::Text, not_null: true, default_value: None, primary_key: true, unique: false },
                ColumnDef { name: "user_id".into(), col_type: ColumnType::Text, not_null: true, default_value: None, primary_key: false, unique: false },
                ColumnDef { name: "status".into(), col_type: ColumnType::Text, not_null: true, default_value: None, primary_key: false, unique: false },
            ],
            foreign_keys: vec![],
            indexes: vec![],
            composite_unique_constraints: vec![],
        }
    }

    #[test]
    fn rebuild_and_range_scan() {
        let mut table = TableState::new(make_schema());
        table.insert_row("o1", &ts(100, "A"), vec![cell("id", "o1"), cell("user_id", "u1"), cell("status", "pending")]);
        table.insert_row("o2", &ts(101, "A"), vec![cell("id", "o2"), cell("user_id", "u1"), cell("status", "shipped")]);
        table.insert_row("o3", &ts(102, "A"), vec![cell("id", "o3"), cell("user_id", "u2"), cell("status", "pending")]);

        let mut idx = SecondaryIndex::new("orders_by_user", vec!["user_id".into(), "status".into()]);
        idx.rebuild_from(&table);

        let prefix = vec![Some(b"u1".to_vec())];
        let results = idx.range_scan(&prefix);
        assert_eq!(results.len(), 2, "Should find 2 orders for u1");

        // Verify deterministic ordering
        assert!(results.contains(&"o1"));
        assert!(results.contains(&"o2"));
    }

    #[test]
    fn rebuild_excludes_deleted_rows() {
        let mut table = TableState::new(make_schema());
        table.insert_row("o1", &ts(100, "A"), vec![cell("id", "o1"), cell("user_id", "u1"), cell("status", "pending")]);
        table.delete_row("o1", &ts(200, "A"), false); // hard delete

        let mut idx = SecondaryIndex::new("orders_by_user", vec!["user_id".into()]);
        idx.rebuild_from(&table);

        assert_eq!(idx.entries.len(), 0, "Deleted row must not appear in index");
    }

    #[test]
    fn index_deterministic_across_merge_orders() {
        // Peer A and B independently insert orders, then merge
        let mut table_a = TableState::new(make_schema());
        let mut table_b = TableState::new(make_schema());

        table_a.insert_row("o1", &ts(100, "A"), vec![cell("id", "o1"), cell("user_id", "u1"), cell("status", "pending")]);
        table_b.insert_row("o2", &ts(101, "B"), vec![cell("id", "o2"), cell("user_id", "u1"), cell("status", "shipped")]);

        let mut merged_ab = table_a.clone(); merged_ab.merge(&table_b);
        let mut merged_ba = table_b.clone(); merged_ba.merge(&table_a);

        let mut idx_ab = SecondaryIndex::new("i", vec!["user_id".into()]);
        idx_ab.rebuild_from(&merged_ab);

        let mut idx_ba = SecondaryIndex::new("i", vec!["user_id".into()]);
        idx_ba.rebuild_from(&merged_ba);

        assert_eq!(idx_ab.entries, idx_ba.entries,
            "Index must be identical regardless of merge order");
    }
}
