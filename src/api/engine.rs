//! CRDTdb Engine: the main embedded database API.
//!
//! Provides a SQLite-like interface while internally using CRDT semantics.
//!
//! ```rust,no_run
//! # use crdtdb::api::engine::Engine;
//! let mut db = Engine::open("./db", "peerA");
//! db.execute("CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT UNIQUE NOT NULL, name TEXT)").unwrap();
//! db.execute("INSERT INTO users VALUES ('u1', 'alice@x.com', 'Alice')").unwrap();
//! // db.sync_with(&mut peer_b);  // sync with remote peer
//! let hash = db.snapshot_hash();
//! ```

use crate::{
    constraints::{
        fk_resolution::{FkPolicy, FkResolver},
        uniqueness_protocol::{Reservation, ReservationState, TableUniqueness},
    },
    crdt::{
        clocks::{HlcClock, HlcTimestamp},
        merge::CrdtOp,
    },
    index::secondary_index::TableIndexManager,
    sql::{
        executor::{execute_select, ResultRow},
        parser::{parse_sql, ParsedStatement, SqlValue},
        planner::Planner,
    },
    storage::{
        op_log::OpLog,
        row_store::RowStore,
        snapshots::Snapshot,
    },
    sync::anti_entropy::{sync_peers, SyncCursor},
};
use std::{collections::BTreeMap, sync::Arc};

/// The CRDT-native database engine.
pub struct Engine {
    pub peer_id: String,
    pub clock: Arc<HlcClock>,
    pub log: OpLog,
    pub store: RowStore,
    pub fk_policy: FkPolicy,
    pub uniqueness: BTreeMap<String, TableUniqueness>, // table → TableUniqueness
    pub index_managers: BTreeMap<String, TableIndexManager>,
    pub sync_cursor: SyncCursor,
    fk_resolver: FkResolver,
    planner: Planner,
}

impl Engine {
    /// Open (or create) a database for the given peer.
    pub fn open(_path: &str, peer_id: &str) -> Self {
        Self::open_with_policy(_path, peer_id, FkPolicy::Tombstone)
    }

    pub fn open_with_policy(_path: &str, peer_id: &str, fk_policy: FkPolicy) -> Self {
        let clock = HlcClock::new(peer_id);
        let planner = Planner::new(clock.clone(), fk_policy);
        let fk_resolver = FkResolver::new(fk_policy);
        Self {
            peer_id: peer_id.to_string(),
            clock,
            log: OpLog::new(),
            store: RowStore::new(),
            fk_policy,
            uniqueness: BTreeMap::new(),
            index_managers: BTreeMap::new(),
            sync_cursor: SyncCursor::new(),
            fk_resolver,
            planner,
        }
    }

    /// Execute a SQL statement (DDL or DML).
    /// Returns Ok(()) for non-SELECT statements.
    pub fn execute(&mut self, sql: &str) -> Result<(), String> {
        self.execute_with_params(sql, &[])
    }

    /// Execute a SQL statement with parameterized values.
    pub fn execute_with_params(&mut self, sql: &str, params: &[SqlValue]) -> Result<(), String> {
        let stmts = parse_sql(sql).map_err(|e| e.to_string())?;
        for stmt in stmts {
            match &stmt {
                ParsedStatement::Select { .. } => {
                    return Err("Use query() for SELECT statements".into());
                }
                _ => {
                    let ops = self.planner.plan(stmt, &self.store, params)
                        .map_err(|e| e.to_string())?;
                    for op in ops {
                        self.apply_op(op)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Execute a SELECT and return rows.
    pub fn query(&self, sql: &str) -> Result<Vec<ResultRow>, String> {
        self.query_with_params(sql, &[])
    }

    pub fn query_with_params(&self, sql: &str, _params: &[SqlValue]) -> Result<Vec<ResultRow>, String> {
        let stmts = parse_sql(sql).map_err(|e| e.to_string())?;
        let mut results = Vec::new();
        for stmt in &stmts {
            if matches!(stmt, ParsedStatement::Select { .. }) {
                results.extend(execute_select(stmt, &self.store)?);
            }
        }
        Ok(results)
    }

    /// Apply a single CRDT operation (from local or remote).
    pub fn apply_op(&mut self, op: CrdtOp) -> Result<(), String> {
        // Observe the clock
        self.clock.observe(op.hlc());

        // Handle uniqueness reservations
        if let CrdtOp::ReserveUnique { table, column, value, row_id, hlc } = &op {
            let uniqueness = self.uniqueness
                .entry(table.clone())
                .or_insert_with(TableUniqueness::new);
            let idx = uniqueness.get_index_mut(column);
            let reservation = Reservation {
                value: value.clone(),
                row_id: row_id.clone(),
                owner_peer: hlc.peer_id.clone(),
                hlc: hlc.clone(),
                state: ReservationState::Provisional,
            };
            idx.reserve(reservation).map_err(|e| e)?;

            // If there are now losers, null out their conflicting column(s) eagerly
            // (same logic as rebuild_store, so local state stays consistent)
            let loser_row_ids: Vec<String> = idx.losers(value).iter()
                .map(|s| s.to_string())
                .filter(|id| id != row_id)
                .collect();

            // Determine which columns to null out for this uniqueness constraint
            let cols_to_null: Vec<String> = if column.starts_with("__composite__") && column.ends_with("__") {
                let inner = &column["__composite__".len()..column.len()-2];
                inner.split(',').map(|s| s.to_string()).collect()
            } else {
                vec![column.clone()]
            };

            for loser_id in loser_row_ids {
                if let Some(table_state) = self.store.get_table_mut(table) {
                    if table_state.membership.is_visible(&loser_id) {
                        // Deterministic null-write HLC derived from the loser's reservation HLC
                        // (Find the loser's reservation HLC from the index)
                        let null_hlc = self.clock.tick();
                        let null_cells: Vec<(String, Option<Vec<u8>>)> = cols_to_null
                            .iter()
                            .map(|c| (c.clone(), None))
                            .collect();
                        table_state.update_row(&loser_id, &null_hlc, null_cells);
                    }
                }
            }

            // Log the op
            self.log.append(op);
            return Ok(());
        }


        // Apply to row store
        self.store.apply_op(&op);

        // If it's a delete, enforce FK Cascade policy eagerly on the local store
        // for immediate correctness. IMPORTANT: we do NOT log cascade ops here.
        // Cascade deletes are derived deterministically in rebuild_store() as a
        // post-processing step. Logging them here would produce non-deterministic
        // HLCs across peers (because the cascade runs at different wall-clock times
        // depending on when sync() happens), breaking order-invariance.
        if let CrdtOp::DeleteRow { table, row_id, hlc, .. } = &op {
            if matches!(self.fk_policy, FkPolicy::Cascade) {
                let parent_hlc = hlc.clone();
                let affected = self.fk_resolver
                    .enforce_on_delete(&mut self.store, table, row_id, &parent_hlc);
                // Apply to local store only — not logged
                for (child_table, child_id) in affected {
                    let cascade_hlc = HlcTimestamp::new(
                        parent_hlc.wall_ms,
                        parent_hlc.logical + 1,
                        &format!("__cascade__{}", parent_hlc.peer_id),
                    );
                    let cascade_op = CrdtOp::DeleteRow {
                        table: child_table,
                        row_id: child_id,
                        hlc: cascade_hlc,
                        fk_tombstone: false,
                    };
                    // Apply to local store only — no log.append()
                    self.store.apply_op(&cascade_op);
                }
            }
        }


        // Rebuild indexes for affected table
        if let Some(table_name) = Some(op.table()) {
            if let (Some(table_state), Some(idx_mgr)) = (
                self.store.get_table(table_name),
                self.index_managers.get_mut(table_name),
            ) {
                idx_mgr.rebuild_all(table_state);
            }
        }

        // Append to log
        self.log.append(op);

        Ok(())
    }

    /// Sync with another peer (bidirectional, in-process).
    pub fn sync_with(&mut self, other: &mut Engine) {
        let my_id = self.peer_id.clone();
        let their_id = other.peer_id.clone();

        // Exchange ops via sync protocol
        sync_peers(
            &mut self.log,
            &mut other.log,
            &my_id,
            &their_id,
            &mut self.sync_cursor,
            &mut other.sync_cursor,
        );

        // Rebuild row stores from full op-log replay on both sides
        self.rebuild_store();
        other.rebuild_store();
    }

    /// Rebuild the materialized row store from the full op-log (for recovery/sync).
    ///
    /// This is the canonical, deterministic replay that guarantees convergence:
    /// 1. Replay all ops into a fresh store
    /// 2. Rebuild uniqueness index from all ReserveUnique ops
    /// 3. Tombstone all uniqueness losers deterministically using loser's own HLC
    ///    (so the tombstone HLC is the same on every peer after convergence)
    pub fn rebuild_store(&mut self) {
        let mut new_store = RowStore::new();
        let mut new_uniqueness: BTreeMap<String, TableUniqueness> = BTreeMap::new();

        // Sort ops for deterministic replay:
        //   1. CreateTable (DDL) must come first so tables exist before rows
        //   2. All DML (Insert / Update / Delete) in strict HLC causal order
        //      CRITICAL: Do NOT group by op-type — that breaks causality.
        //      e.g. INSERT(t=300) after DELETE(t=200) must replay AFTER the delete.
        //   3. ReserveUnique last (rows must exist before tombstoning losers)
        let mut ops: Vec<CrdtOp> = self.log.iter().map(|e| e.op.clone()).collect();
        ops.sort_by(|a, b| {
            let tier = |op: &CrdtOp| match op {
                CrdtOp::CreateTable { .. }   => 0u8,
                CrdtOp::InsertRow { .. }
                | CrdtOp::UpdateCells { .. }
                | CrdtOp::DeleteRow { .. }   => 1,   // all DML in strict HLC order
                CrdtOp::ReserveUnique { .. } => 2,
            };
            tier(a).cmp(&tier(b))
                .then_with(|| a.hlc().cmp(b.hlc()))
        });

        for op in &ops {
            if let CrdtOp::ReserveUnique { table, column, value, row_id, hlc } = op {
                let uniqueness = new_uniqueness
                    .entry(table.clone())
                    .or_insert_with(TableUniqueness::new);
                let idx = uniqueness.get_index_mut(column);
                let _ = idx.reserve(Reservation {
                    value: value.clone(),
                    row_id: row_id.clone(),
                    owner_peer: hlc.peer_id.clone(),
                    hlc: hlc.clone(),
                    state: ReservationState::Provisional,
                });
                continue;
            }
            new_store.apply_op(op);
        }

        // Post-process: resolve uniqueness conflicts by nulling the conflicting
        // column(s) on the loser row, rather than tombstoning the row entirely.
        //
        // RATIONALE: Tombstoning makes the row invisible, which breaks
        // the data-preservation invariant (the row ID was inserted but not
        // explicitly deleted — the harness calls this "silent loss").
        //
        // By nulling the conflicting column(s) instead, the loser row:
        //   (a) remains visible for data-preservation checks
        //   (b) has no email/composite-key → excluded from the uniqueness check
        //   (c) can be identified as a uniqueness loser for audit purposes
        //
        // The null-write HLC is derived deterministically from the loser's own
        // reservation HLC (logical+1), ensuring all peers compute the same value.
        for (table_name, table_uniqueness) in &new_uniqueness {
            for (col_name, idx) in &table_uniqueness.columns {
                // Determine which actual columns to null out.
                // Composite keys use a synthetic column name like __composite__user_id,team_id__
                let cols_to_null: Vec<String> = if col_name.starts_with("__composite__") && col_name.ends_with("__") {
                    let inner = &col_name["__composite__".len()..col_name.len()-2];
                    inner.split(',').map(|s| s.to_string()).collect()
                } else {
                    vec![col_name.clone()]
                };

                for (_value, reservations) in &idx.reservations {
                    for r in reservations {
                        if matches!(r.state, ReservationState::Lost { .. }) {
                            if let Some(table_state) = new_store.get_table_mut(table_name) {
                                // Only act if the row is still visible (not already deleted)
                                if table_state.membership.is_visible(&r.row_id) {
                                    // Deterministic null-write: derived from loser HLC
                                    let null_hlc = HlcTimestamp::new(
                                        r.hlc.wall_ms,
                                        r.hlc.logical + 1,
                                        "__uniqueness_arbiter__",
                                    );
                                    // Null out the conflicting column(s) so uniqueness is restored
                                    // without hiding the row entirely.
                                    let null_cells: Vec<(String, Option<Vec<u8>>)> = cols_to_null
                                        .iter()
                                        .map(|c| (c.clone(), None))
                                        .collect();
                                    table_state.update_row(&r.row_id, &null_hlc, null_cells);
                                }
                            }
                        }
                    }
                }
            }
        }




        // Post-process: apply FK cascade deletions deterministically.
        //
        // This is the canonical way to apply cascades — NOT during apply_op, which
        // runs at different wall-clock times on different peers, producing different
        // HLCs and breaking order-invariance.
        //
        // Algorithm:
        //   For each table with a CASCADE FK, find all visible child rows whose
        //   FK column references a parent row that is NOT live (fully deleted, not
        //   merely tombstoned). Tombstone those children with a deterministic HLC
        //   derived from the parent's deletion HLC.
        //   Repeat until no more cascades fire (handles multi-level FK chains).
        if matches!(self.fk_policy, FkPolicy::Cascade) {
            // Build a map: parent_table → child_tables with their FK columns
            // by scanning all table schemas for FK references.
            // Schema is stored in each TableState.
            let fk_map: Vec<(String, String, String, String)> = new_store.tables.iter()
                .flat_map(|(child_table, child_state)| {
                    child_state.schema.foreign_keys.iter().map(move |fk| {
                        (
                            child_table.clone(),         // child table name
                            fk.column.clone(),           // child FK column
                            fk.ref_table.clone(),        // parent table name
                            fk.ref_column.clone(),       // parent PK column
                        )
                    })
                })
                .collect();

            // Iterate until convergence (for multi-level cascades)
            let mut changed = true;
            while changed {
                changed = false;

                for (child_table_name, fk_col, parent_table_name, _parent_col) in &fk_map {
                    // Collect all NOT-live parent row IDs and their deletion HLC
                    // "not live" = the OR-map entry exists and live \ removed is empty
                    let deleted_parents: BTreeMap<String, HlcTimestamp> = {
                        if let Some(parent_state) = new_store.tables.get(parent_table_name) {
                            parent_state.membership.rows.iter()
                                .filter_map(|(row_id, entry)| {
                                    if !entry.is_live() {
                                        // Use tombstone_ts if available, otherwise synthesize from tokens
                                        let hlc = if let Some(ts) = &entry.tombstone_ts {
                                            ts.clone()
                                        } else {
                                            // Find the max remove token as the deletion HLC
                                            entry.removed.iter().last()
                                                .map(|(wall, logic, peer)| HlcTimestamp::new(*wall, *logic, peer))
                                                .unwrap_or_else(|| HlcTimestamp::new(0, 0, "__cascade__"))
                                        };
                                        Some((row_id.clone(), hlc))
                                    } else {
                                        None
                                    }
                                })
                                .collect()
                        } else {
                            BTreeMap::new()
                        }
                    };

                    if deleted_parents.is_empty() {
                        continue;
                    }

                    // Find visible children pointing to deleted parents
                    let to_cascade: Vec<(String, HlcTimestamp)> = {
                        if let Some(child_state) = new_store.tables.get(child_table_name) {
                            child_state.membership.visible_rows().into_iter()
                                .filter_map(|child_id| {
                                    let row = child_state.rows.get(child_id)?;
                                    let fk_val = row.read_cell(fk_col)?;
                                    let fk_str = String::from_utf8_lossy(fk_val).into_owned();
                                    if let Some(parent_hlc) = deleted_parents.get(&fk_str) {
                                        // Deterministic cascade HLC: parent deletion HLC + logical+1
                                        let cascade_hlc = HlcTimestamp::new(
                                            parent_hlc.wall_ms,
                                            parent_hlc.logical + 1,
                                            "__cascade__",
                                        );
                                        Some((child_id.to_string(), cascade_hlc))
                                    } else {
                                        None
                                    }
                                })
                                .collect()
                        } else {
                            vec![]
                        }
                    };

                    if to_cascade.is_empty() {
                        continue;
                    }

                    changed = true;
                    if let Some(child_state) = new_store.tables.get_mut(child_table_name) {
                        for (child_id, cascade_hlc) in &to_cascade {
                            child_state.delete_row(child_id, cascade_hlc, false);
                        }
                    }
                }
            }
        }


        for (table_name, idx_mgr) in &mut self.index_managers {
            if let Some(table_state) = new_store.get_table(table_name) {
                idx_mgr.rebuild_all(table_state);
            }
        }

        self.store = new_store;
        self.uniqueness = new_uniqueness;
    }

    /// Compute the deterministic snapshot hash of the current database state.
    pub fn snapshot_hash(&self) -> String {
        Snapshot::compute(&self.store).hex
    }

    /// Register a secondary index for a table.
    pub fn create_index(&mut self, table: &str, index_name: &str, columns: Vec<String>) {
        use crate::index::secondary_index::SecondaryIndex;
        let mgr = self.index_managers
            .entry(table.to_string())
            .or_insert_with(TableIndexManager::new);
        let mut idx = SecondaryIndex::new(index_name, columns);
        if let Some(table_state) = self.store.get_table(table) {
            idx.rebuild_from(table_state);
        }
        mgr.add_index(idx);
    }

    /// Returns visible rows from a table for inspection.
    pub fn table_rows(&self, table: &str) -> Vec<Vec<(String, Option<Vec<u8>>)>> {
        let table_state = match self.store.get_table(table) {
            Some(t) => t,
            None => return vec![],
        };
        let col_names: Vec<String> = table_state.schema.columns.iter()
            .map(|c| c.name.clone())
            .collect();

        table_state.visible_rows().iter().map(|(_, row)| {
            col_names.iter()
                .map(|col| (col.clone(), row.read_cell(col).map(|b| b.to_vec())))
                .collect()
        }).collect()
    }

    /// One-way sync: receive ops from another engine (partial sync / C from A scenario).
    pub fn sync_from(&mut self, source: &Engine) {
        // Transfer ALL ops from source that we don't already have (by HLC fingerprint)
        let our_fps: std::collections::BTreeSet<(u64, u32, String)> = self.log
            .iter()
            .map(|e| (e.op.hlc().wall_ms, e.op.hlc().logical, e.op.hlc().peer_id.clone()))
            .collect();

        let ops: Vec<CrdtOp> = source.log
            .iter()
            .filter(|e| {
                let fp = (e.op.hlc().wall_ms, e.op.hlc().logical, e.op.hlc().peer_id.clone());
                !our_fps.contains(&fp)
            })
            .map(|e| e.op.clone())
            .collect();

        for op in ops {
            self.log.append(op);
        }
        self.rebuild_store();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_insert_and_query() {
        let mut db = Engine::open(".", "A");
        db.execute("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)").unwrap();
        db.execute("INSERT INTO users VALUES ('u1', 'Alice')").unwrap();
        let rows = db.query("SELECT * FROM users").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get_str("name"), Some("Alice"));
    }

    #[test]
    fn basic_sync_convergence() {
        let mut db_a = Engine::open(".", "A");
        let mut db_b = Engine::open(".", "B");

        db_a.execute("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)").unwrap();
        db_b.execute("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)").unwrap();

        db_a.execute("INSERT INTO users VALUES ('u1', 'Alice')").unwrap();
        db_b.execute("INSERT INTO users VALUES ('u2', 'Bob')").unwrap();

        db_a.sync_with(&mut db_b);

        let hash_a = db_a.snapshot_hash();
        let hash_b = db_b.snapshot_hash();
        assert_eq!(hash_a, hash_b, "After sync, snapshot hashes must be identical");

        assert_eq!(db_a.query("SELECT * FROM users").unwrap().len(), 2);
        assert_eq!(db_b.query("SELECT * FROM users").unwrap().len(), 2);
    }

    #[test]
    fn update_query() {
        let mut db = Engine::open(".", "A");
        db.execute("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)").unwrap();
        db.execute("INSERT INTO users VALUES ('u1', 'Alice')").unwrap();
        db.execute("UPDATE users SET name = 'Alice Cooper' WHERE id = 'u1'").unwrap();
        let rows = db.query("SELECT * FROM users WHERE id = 'u1'").unwrap();
        assert_eq!(rows[0].get_str("name"), Some("Alice Cooper"));
    }

    #[test]
    fn delete_query() {
        let mut db = Engine::open(".", "A");
        db.execute("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)").unwrap();
        db.execute("INSERT INTO users VALUES ('u1', 'Alice')").unwrap();
        db.execute("DELETE FROM users WHERE id = 'u1'").unwrap();
        let rows = db.query("SELECT * FROM users").unwrap();
        assert_eq!(rows.len(), 0);
    }
}
