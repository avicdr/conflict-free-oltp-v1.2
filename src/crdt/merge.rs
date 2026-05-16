//! CRDT merge trait definitions and merge utilities.

use crate::crdt::{clocks::HlcTimestamp, mv_register::MvRegister, orset::OrMap};
use serde::{Deserialize, Serialize};

/// Core CRDT merge trait.
/// Any type implementing this is a CRDT join-semilattice.
pub trait CrdtMerge: Clone {
    /// Merge `other` into `self`. Must be commutative, associative, idempotent.
    fn merge_from(&mut self, other: &Self);

    /// Return a new merged value without mutating self.
    fn merged(&self, other: &Self) -> Self {
        let mut result = self.clone();
        result.merge_from(other);
        result
    }
}

impl CrdtMerge for MvRegister {
    fn merge_from(&mut self, other: &Self) {
        self.merge(other);
    }
}

impl CrdtMerge for OrMap {
    fn merge_from(&mut self, other: &Self) {
        self.merge(other);
    }
}

/// A raw CRDT operation as recorded in the op-log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CrdtOp {
    /// Insert a new row (or re-insert / resurrect)
    InsertRow {
        table: String,
        row_id: String,
        hlc: HlcTimestamp,
        cells: Vec<(String, Option<Vec<u8>>)>, // (column_name, value)
    },
    /// Update one or more cells of an existing row
    UpdateCells {
        table: String,
        row_id: String,
        hlc: HlcTimestamp,
        cells: Vec<(String, Option<Vec<u8>>)>,
    },
    /// Delete a row (with FK tombstone policy)
    DeleteRow {
        table: String,
        row_id: String,
        hlc: HlcTimestamp,
        fk_tombstone: bool,
    },
    /// Reserve a unique value (uniqueness protocol)
    ReserveUnique {
        table: String,
        column: String,
        value: Vec<u8>,
        row_id: String,
        hlc: HlcTimestamp,
    },
    /// Create a table (DDL)
    CreateTable {
        table: String,
        schema: TableSchema,
        hlc: HlcTimestamp,
    },
}

impl CrdtOp {
    pub fn hlc(&self) -> &HlcTimestamp {
        match self {
            CrdtOp::InsertRow { hlc, .. } => hlc,
            CrdtOp::UpdateCells { hlc, .. } => hlc,
            CrdtOp::DeleteRow { hlc, .. } => hlc,
            CrdtOp::ReserveUnique { hlc, .. } => hlc,
            CrdtOp::CreateTable { hlc, .. } => hlc,
        }
    }

    pub fn table(&self) -> &str {
        match self {
            CrdtOp::InsertRow { table, .. } => table,
            CrdtOp::UpdateCells { table, .. } => table,
            CrdtOp::DeleteRow { table, .. } => table,
            CrdtOp::ReserveUnique { table, .. } => table,
            CrdtOp::CreateTable { table, .. } => table,
        }
    }
}

/// Column data type for schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnType {
    Text,
    Integer,
    Real,
    Blob,
    Boolean,
}

/// Column definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub col_type: ColumnType,
    pub not_null: bool,
    pub default_value: Option<Vec<u8>>,
    pub primary_key: bool,
    pub unique: bool,
}

/// Foreign key reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignKeyDef {
    pub column: String,
    pub ref_table: String,
    pub ref_column: String,
}

/// Index definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDef {
    pub name: String,
    pub columns: Vec<String>,
}

/// Table schema definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub foreign_keys: Vec<ForeignKeyDef>,
    pub indexes: Vec<IndexDef>,
    /// Table-level composite UNIQUE constraints, e.g. UNIQUE(user_id, team_id)
    /// Each entry is an ordered list of column names forming a composite key.
    #[serde(default)]
    pub composite_unique_constraints: Vec<Vec<String>>,
}

impl TableSchema {
    pub fn primary_key_col(&self) -> Option<&str> {
        self.columns
            .iter()
            .find(|c| c.primary_key)
            .map(|c| c.name.as_str())
    }

    pub fn unique_columns(&self) -> Vec<&str> {
        self.columns
            .iter()
            .filter(|c| c.unique && !c.primary_key)
            .map(|c| c.name.as_str())
            .collect()
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Compute the canonical composite key name for a set of columns.
    /// Used as the "column" name in ReserveUnique for composite constraints.
    pub fn composite_key_name(cols: &[String]) -> String {
        format!("__composite__{}__", cols.join(","))
    }

    /// Compute the composite key value bytes by concatenating cell values null-delimited.
    /// Returns None if any required column is missing.
    pub fn composite_key_value(
        cols: &[String],
        cells: &[(String, Option<Vec<u8>>)],
    ) -> Option<Vec<u8>> {
        let mut parts: Vec<Vec<u8>> = Vec::new();
        for col in cols {
            let val = cells.iter()
                .find(|(c, _)| c == col)
                .and_then(|(_, v)| v.as_ref().cloned());
            parts.push(val.unwrap_or_default());
        }
        // Concatenate with null byte separator for deterministic ordering
        let mut result = Vec::new();
        for (i, part) in parts.iter().enumerate() {
            if i > 0 { result.push(0u8); }
            result.extend_from_slice(part);
        }
        Some(result)
    }
}
