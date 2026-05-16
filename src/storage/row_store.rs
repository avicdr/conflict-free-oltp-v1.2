//! Row store: materialized CRDT state indexed for fast relational queries.
//!
//! The row store is an OR-Map of rows, where each row is an OR-Map entry and
//! each cell is an MV-Register. This is the "materialized view" of the op-log.
//!
//! ## Delete vs Update Semantics (OPTION B implemented)
//! Updates attach to tombstoned rows. If a row is deleted and concurrently updated:
//! - The update is applied to the tombstoned row's cells
//! - The row remains tombstoned (not visible to SELECT)
//! - The updated data is preserved for audit/resurrection
//! - This is deterministic: the delete and update both survive; visibility is
//!   determined solely by the OR-Map tombstone state, not by timestamp ordering

use crate::crdt::{
    clocks::HlcTimestamp,
    merge::{CrdtOp, TableSchema},
    mv_register::MvRegister,
    orset::OrMap,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A fully materialized row with per-column MV-Registers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CrdtRow {
    /// Column name → MV-Register
    pub cells: BTreeMap<String, MvRegister>,
}

impl CrdtRow {
    pub fn new() -> Self { Self::default() }

    /// Apply a cell update to this row.
    pub fn write_cell(&mut self, column: &str, hlc: HlcTimestamp, value: Option<Vec<u8>>) {
        self.cells
            .entry(column.to_string())
            .or_insert_with(MvRegister::new)
            .write(hlc, value);
    }

    /// Read the canonical value of a cell (deterministic via HLC total order).
    pub fn read_cell(&self, column: &str) -> Option<&[u8]> {
        self.cells.get(column)?.read()
    }

    /// Merge another row's cell registers into this one (cell-level CRDT merge).
    pub fn merge(&mut self, other: &CrdtRow) {
        for (col, other_reg) in &other.cells {
            self.cells
                .entry(col.clone())
                .or_insert_with(MvRegister::new)
                .merge(other_reg);
        }
    }
}

/// A table's full CRDT state: membership (OR-Map) + cell data (rows of MV-Registers).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableState {
    pub schema: TableSchema,
    /// Row membership: OR-Map tracking live/tombstoned rows
    pub membership: OrMap,
    /// Row data: row_id → CrdtRow (cells)
    pub rows: BTreeMap<String, CrdtRow>,
}

impl TableState {
    pub fn new(schema: TableSchema) -> Self {
        Self {
            schema,
            membership: OrMap::new(),
            rows: BTreeMap::new(),
        }
    }

    /// Insert a row with given cell values.
    pub fn insert_row(
        &mut self,
        row_id: &str,
        hlc: &HlcTimestamp,
        cells: Vec<(String, Option<Vec<u8>>)>,
    ) {
        self.membership.insert(row_id, hlc);
        let row = self.rows.entry(row_id.to_string()).or_insert_with(CrdtRow::new);
        for (col, val) in cells {
            row.write_cell(&col, hlc.clone(), val);
        }
    }

    /// Update cells of a row.
    /// OPTION B: Updates apply even to tombstoned rows (data is preserved).
    pub fn update_row(
        &mut self,
        row_id: &str,
        hlc: &HlcTimestamp,
        cells: Vec<(String, Option<Vec<u8>>)>,
    ) {
        let row = self.rows.entry(row_id.to_string()).or_insert_with(CrdtRow::new);
        for (col, val) in cells {
            row.write_cell(&col, hlc.clone(), val);
        }
    }

    /// Delete a row. With fk_tombstone=true, tombstones instead of fully removing.
    pub fn delete_row(&mut self, row_id: &str, hlc: &HlcTimestamp, fk_tombstone: bool) {
        self.membership.delete(row_id, hlc, fk_tombstone);
    }

    /// Merge another TableState into this one.
    /// This is the core CRDT merge for tables.
    pub fn merge(&mut self, other: &TableState) {
        // Merge membership (OR-Map merge: commutative, assoc, idempotent)
        self.membership.merge(&other.membership);

        // Merge cell data (per-cell MV-Register merge)
        for (row_id, other_row) in &other.rows {
            self.rows
                .entry(row_id.clone())
                .or_insert_with(CrdtRow::new)
                .merge(other_row);
        }
    }

    /// Return all visible rows as (row_id, CrdtRow) pairs, sorted by row_id.
    pub fn visible_rows(&self) -> Vec<(&str, &CrdtRow)> {
        self.membership
            .visible_rows()
            .into_iter()
            .filter_map(|id| self.rows.get(id).map(|row| (id, row)))
            .collect()
    }

    /// Return all rows including tombstoned (for snapshot hashing).
    pub fn all_rows_for_snapshot(&self) -> Vec<(&str, &CrdtRow, bool)> {
        let mut result: Vec<_> = self.rows.iter()
            .map(|(id, row)| {
                let tombstoned = self.membership.rows.get(id)
                    .map(|e| e.is_tombstoned())
                    .unwrap_or(false);
                (id.as_str(), row, tombstoned)
            })
            .collect();
        result.sort_by_key(|(id, _, _)| *id);
        result
    }

    /// Check FK: does this table contain row_id in any state (for FK validation)?
    pub fn exists_for_fk(&self, row_id: &str) -> bool {
        self.membership.exists_for_fk(row_id)
    }
}

/// The full database row store: all tables.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RowStore {
    pub tables: BTreeMap<String, TableState>,
}

impl RowStore {
    pub fn new() -> Self { Self::default() }

    pub fn create_table(&mut self, schema: TableSchema) {
        self.tables
            .entry(schema.name.clone())
            .or_insert_with(|| TableState::new(schema));
    }

    pub fn apply_op(&mut self, op: &CrdtOp) {
        match op {
            CrdtOp::CreateTable { schema, .. } => {
                self.create_table(schema.clone());
            }
            CrdtOp::InsertRow { table, row_id, hlc, cells } => {
                if let Some(t) = self.tables.get_mut(table) {
                    t.insert_row(row_id, hlc, cells.clone());
                }
            }
            CrdtOp::UpdateCells { table, row_id, hlc, cells } => {
                if let Some(t) = self.tables.get_mut(table) {
                    t.update_row(row_id, hlc, cells.clone());
                }
            }
            CrdtOp::DeleteRow { table, row_id, hlc, fk_tombstone } => {
                if let Some(t) = self.tables.get_mut(table) {
                    t.delete_row(row_id, hlc, *fk_tombstone);
                }
            }
            CrdtOp::ReserveUnique { .. } => {
                // Handled by the uniqueness protocol, not directly by row store
            }
        }
    }

    /// Merge another RowStore into this one.
    pub fn merge(&mut self, other: &RowStore) {
        for (name, other_table) in &other.tables {
            self.tables
                .entry(name.clone())
                .or_insert_with(|| TableState::new(other_table.schema.clone()))
                .merge(other_table);
        }
    }

    pub fn get_table(&self, name: &str) -> Option<&TableState> {
        self.tables.get(name)
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut TableState> {
        self.tables.get_mut(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::merge::{ColumnDef, ColumnType};

    fn make_schema(name: &str) -> TableSchema {
        TableSchema {
            name: name.to_string(),
            columns: vec![
                ColumnDef { name: "id".into(), col_type: ColumnType::Text, not_null: true, default_value: None, primary_key: true, unique: false },
                ColumnDef { name: "name".into(), col_type: ColumnType::Text, not_null: false, default_value: None, primary_key: false, unique: false },
                ColumnDef { name: "email".into(), col_type: ColumnType::Text, not_null: false, default_value: None, primary_key: false, unique: true },
            ],
            foreign_keys: vec![],
            indexes: vec![],
            composite_unique_constraints: vec![],
        }
    }

    fn ts(wall: u64, peer: &str) -> HlcTimestamp { HlcTimestamp::new(wall, 0, peer) }
    fn cell(col: &str, val: &str) -> (String, Option<Vec<u8>>) {
        (col.to_string(), Some(val.as_bytes().to_vec()))
    }

    #[test]
    fn insert_visible() {
        let mut t = TableState::new(make_schema("users"));
        t.insert_row("u1", &ts(100, "A"), vec![cell("name", "Alice"), cell("email", "alice@x.com")]);
        let rows = t.visible_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "u1");
    }

    #[test]
    fn delete_tombstone_invisible_but_fk_valid() {
        let mut t = TableState::new(make_schema("users"));
        t.insert_row("u1", &ts(100, "A"), vec![cell("name", "Alice")]);
        t.delete_row("u1", &ts(200, "A"), true);
        assert_eq!(t.visible_rows().len(), 0, "Tombstoned row not in SELECT");
        assert!(t.exists_for_fk("u1"), "Tombstoned row valid for FK");
    }

    #[test]
    fn option_b_update_survives_delete() {
        // Peer A deletes u1, Peer B updates u1's name concurrently
        let mut store_a = TableState::new(make_schema("users"));
        let mut store_b = TableState::new(make_schema("users"));

        // Both start with u1
        store_a.insert_row("u1", &ts(100, "A"), vec![cell("name", "Alice")]);
        store_b.insert_row("u1", &ts(100, "A"), vec![cell("name", "Alice")]);

        // A deletes u1
        store_a.delete_row("u1", &ts(200, "A"), true);

        // B updates u1's name concurrently
        store_b.update_row("u1", &ts(200, "B"), vec![cell("name", "Alice Cooper")]);

        // Merge both directions
        store_a.merge(&store_b);
        store_b.merge(&store_a);

        // After merge: row is tombstoned but update is preserved
        let row_a = store_a.rows.get("u1").unwrap();
        let row_b = store_b.rows.get("u1").unwrap();
        assert_eq!(
            row_a.read_cell("name"),
            Some(b"Alice Cooper".as_ref()),
            "OPTION B: update must survive delete"
        );
        assert_eq!(row_a, row_b, "States must converge");
    }

    #[test]
    fn cell_level_concurrent_update() {
        // Peer A updates name, Peer B updates email on the same row
        let mut a = TableState::new(make_schema("users"));
        let mut b = TableState::new(make_schema("users"));

        a.insert_row("u1", &ts(100, "A"), vec![cell("name", "Alice"), cell("email", "a@x.com")]);
        b.insert_row("u1", &ts(100, "A"), vec![cell("name", "Alice"), cell("email", "a@x.com")]);

        a.update_row("u1", &ts(200, "A"), vec![cell("name", "Alice Cooper")]);
        b.update_row("u1", &ts(200, "B"), vec![cell("email", "alice@ex.org")]);

        a.merge(&b);
        b.merge(&a);

        let row_a = a.rows.get("u1").unwrap();
        let row_b = b.rows.get("u1").unwrap();

        assert_eq!(row_a.read_cell("name"), Some(b"Alice Cooper".as_ref()), "name must be updated");
        assert_eq!(row_a.read_cell("email"), Some(b"alice@ex.org".as_ref()), "email must be updated");
        assert_eq!(row_a, row_b, "Both peers must converge");
    }
}
