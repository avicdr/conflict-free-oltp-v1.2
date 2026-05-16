//! SQL planner: translates ParsedStatements into CRDT operations.

use crate::constraints::fk_resolution::{FkPolicy, FkResolver};
use crate::crdt::{
    clocks::HlcClock,
    merge::CrdtOp,
};
use crate::sql::parser::{ParsedStatement, SqlValue};
use crate::storage::row_store::RowStore;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PlanError {
    #[error("Table '{0}' not found")]
    TableNotFound(String),
    #[error("Column '{0}' not found in table '{1}'")]
    ColumnNotFound(String, String),
    #[error("FK violation: {0}")]
    FkViolation(String),
    #[error("Row '{0}' not found")]
    RowNotFound(String),
    #[error("No primary key column defined for table '{0}'")]
    NoPrimaryKey(String),
    #[error("Missing value for NOT NULL column '{0}'")]
    NullConstraint(String),
}

/// Plans SQL statements into sequences of CRDT operations.
pub struct Planner {
    pub clock: Arc<HlcClock>,
    pub fk_resolver: FkResolver,
}

impl Planner {
    pub fn new(clock: Arc<HlcClock>, fk_policy: FkPolicy) -> Self {
        Self {
            clock,
            fk_resolver: FkResolver::new(fk_policy),
        }
    }

    /// Plan a parsed statement into CRDT operations.
    pub fn plan(
        &self,
        stmt: ParsedStatement,
        store: &RowStore,
        params: &[SqlValue],
    ) -> Result<Vec<CrdtOp>, PlanError> {
        match stmt {
            ParsedStatement::CreateTable { schema } => {
                let hlc = self.clock.tick();
                Ok(vec![CrdtOp::CreateTable { table: schema.name.clone(), schema, hlc }])
            }

            ParsedStatement::CreateIndex { index_name: _, table_name: _, columns: _ } => {
                // Indexes are derived — CreateIndex is a no-op at the CRDT layer
                // (the index manager will rebuild from state)
                Ok(vec![])
            }

            ParsedStatement::Insert { table, columns, values } => {
                self.plan_insert(&table, columns, values, store, params)
            }

            ParsedStatement::Update { table, assignments, filter } => {
                self.plan_update(&table, assignments, filter, store, params)
            }

            ParsedStatement::Delete { table, filter } => {
                self.plan_delete(&table, filter, store, params)
            }

            ParsedStatement::Select { .. } => {
                // SELECT is handled by the executor, not the planner
                Ok(vec![])
            }
        }
    }

    fn plan_insert(
        &self,
        table_name: &str,
        columns: Vec<String>,
        values: Vec<Vec<SqlValue>>,
        store: &RowStore,
        params: &[SqlValue],
    ) -> Result<Vec<CrdtOp>, PlanError> {
        let table_state = store.get_table(table_name)
            .ok_or_else(|| PlanError::TableNotFound(table_name.to_string()))?;
        let schema = table_state.schema.clone();

        let pk_col = schema.primary_key_col()
            .ok_or_else(|| PlanError::NoPrimaryKey(table_name.to_string()))?
            .to_string();

        let mut ops = Vec::new();

        for row_vals in values {
            let hlc = self.clock.tick();

            // Build column → value map
            let col_val_pairs: Vec<(String, SqlValue)> = if columns.is_empty() {
                // Positional: match schema column order
                schema.columns.iter().zip(row_vals.iter())
                    .map(|(col, val)| (col.name.clone(), val.clone()))
                    .collect()
            } else {
                columns.iter().zip(row_vals.iter())
                    .map(|(col, val)| (col.clone(), val.clone()))
                    .collect()
            };

            // Resolve parameterized values
            let cells: Vec<(String, Option<Vec<u8>>)> = col_val_pairs.iter()
                .map(|(col, val)| {
                    let bytes = val.to_bytes_with_params(params);
                    (col.clone(), bytes)
                })
                .collect();

            // Get PK value
            let pk_val = cells.iter()
                .find(|(col, _)| col == &pk_col)
                .and_then(|(_, v)| v.as_ref())
                .map(|v| String::from_utf8_lossy(v).to_string())
                .ok_or_else(|| PlanError::ColumnNotFound(pk_col.clone(), table_name.to_string()))?;

            // FK validation
            for fk in &schema.foreign_keys {
                if let Some(fk_val_bytes) = cells.iter()
                    .find(|(col, _)| col == &fk.column)
                    .and_then(|(_, v)| v.as_ref())
                {
                    let fk_val = String::from_utf8_lossy(fk_val_bytes);
                    self.fk_resolver
                        .validate_insert(store, &fk_val, &fk.ref_table)
                        .map_err(PlanError::FkViolation)?;
                }
            }

            // NOT NULL checks
            for col_def in &schema.columns {
                if col_def.not_null && !col_def.primary_key {
                    let has_val = cells.iter().any(|(c, v)| c == &col_def.name && v.is_some());
                    let has_default = col_def.default_value.is_some();
                    if !has_val && !has_default {
                        return Err(PlanError::NullConstraint(col_def.name.clone()));
                    }
                }
            }

            // Emit InsertRow op
            ops.push(CrdtOp::InsertRow {
                table: table_name.to_string(),
                row_id: pk_val.clone(),
                hlc: hlc.clone(),
                cells: cells.clone(),
            });

            // Emit ReserveUnique for each UNIQUE column
            for col_def in schema.columns.iter().filter(|c| c.unique) {
                if let Some(val_bytes) = cells.iter()
                    .find(|(c, _)| c == &col_def.name)
                    .and_then(|(_, v)| v.as_ref())
                {
                    ops.push(CrdtOp::ReserveUnique {
                        table: table_name.to_string(),
                        column: col_def.name.clone(),
                        value: val_bytes.clone(),
                        row_id: pk_val.clone(),
                        hlc: hlc.clone(),
                    });
                }
            }

            // Emit ReserveUnique for composite UNIQUE constraints
            for composite_cols in &schema.composite_unique_constraints {
                if let Some(key_bytes) = crate::crdt::merge::TableSchema::composite_key_value(
                    composite_cols, &cells
                ) {
                    let key_col = crate::crdt::merge::TableSchema::composite_key_name(composite_cols);
                    ops.push(CrdtOp::ReserveUnique {
                        table: table_name.to_string(),
                        column: key_col,
                        value: key_bytes,
                        row_id: pk_val.clone(),
                        hlc: hlc.clone(),
                    });
                }
            }
        }

        Ok(ops)
    }

    fn plan_update(
        &self,
        table_name: &str,
        assignments: Vec<(String, SqlValue)>,
        filter: Option<crate::sql::parser::SqlExpr>,
        store: &RowStore,
        params: &[SqlValue],
    ) -> Result<Vec<CrdtOp>, PlanError> {
        let table_state = store.get_table(table_name)
            .ok_or_else(|| PlanError::TableNotFound(table_name.to_string()))?;

        let schema = table_state.schema.clone();
        let _pk_col = schema.primary_key_col()
            .ok_or_else(|| PlanError::NoPrimaryKey(table_name.to_string()))?
            .to_string();

        // Find matching rows (visible only — OPTION B: updates also go to tombstoned rows,
        // but SQL UPDATE semantics only apply to visible rows)
        let matching_rows: Vec<String> = table_state.visible_rows()
            .iter()
            .filter(|(_, row)| {
                filter.as_ref().map_or(true, |f| {
                    f.evaluate(&|col: &str| row.read_cell(col).map(|b| b.to_vec()))
                })
            })
            .map(|(id, _)| id.to_string())
            .collect();

        let cells: Vec<(String, Option<Vec<u8>>)> = assignments.iter()
            .map(|(col, val)| (col.clone(), val.to_bytes_with_params(params)))
            .collect();

        let mut ops = Vec::new();
        for row_id in matching_rows {
            let hlc = self.clock.tick();
            ops.push(CrdtOp::UpdateCells {
                table: table_name.to_string(),
                row_id: row_id.clone(),
                hlc: hlc.clone(),
                cells: cells.clone(),
            });

            // Emit ReserveUnique for any UNIQUE columns being updated
            for col_def in schema.columns.iter().filter(|c| c.unique) {
                if let Some(new_val_bytes) = cells.iter()
                    .find(|(c, _)| c == &col_def.name)
                    .and_then(|(_, v)| v.as_ref())
                {
                    ops.push(CrdtOp::ReserveUnique {
                        table: table_name.to_string(),
                        column: col_def.name.clone(),
                        value: new_val_bytes.clone(),
                        row_id: row_id.clone(),
                        hlc: hlc.clone(),
                    });
                }
            }
        }

        Ok(ops)
    }


    fn plan_delete(
        &self,
        table_name: &str,
        filter: Option<crate::sql::parser::SqlExpr>,
        store: &RowStore,
        _params: &[SqlValue],
    ) -> Result<Vec<CrdtOp>, PlanError> {
        let table_state = store.get_table(table_name)
            .ok_or_else(|| PlanError::TableNotFound(table_name.to_string()))?;

        let fk_tombstone = matches!(self.fk_resolver.policy, FkPolicy::Tombstone);

        let matching_rows: Vec<String> = table_state.visible_rows()
            .iter()
            .filter(|(_, row)| {
                filter.as_ref().map_or(true, |f| {
                    f.evaluate(&|col: &str| row.read_cell(col).map(|b| b.to_vec()))
                })
            })
            .map(|(id, _)| id.to_string())
            .collect();

        let mut ops = Vec::new();
        for row_id in matching_rows {
            let hlc = self.clock.tick();
            ops.push(CrdtOp::DeleteRow {
                table: table_name.to_string(),
                row_id,
                hlc,
                fk_tombstone,
            });
        }

        Ok(ops)
    }
}
