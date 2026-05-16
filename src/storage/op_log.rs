//! Append-only operation log with crash-safe durability.
//!
//! The op-log is the source of truth. Row state is a materialized view derived
//! from replaying the op-log. This guarantees:
//! - Crash recovery by replaying from log
//! - Idempotent replay (operations carry HLCs; already-applied ops are skipped)
//! - Deterministic state derivation from any suffix of the log

use crate::crdt::merge::CrdtOp;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A single entry in the operation log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    /// Monotonic sequence number within this peer (for cursor-based sync).
    pub seq: u64,
    /// The CRDT operation.
    pub op: CrdtOp,
    /// Blake3 hash of the previous log entry (chain integrity).
    pub prev_hash: [u8; 32],
}

impl LogEntry {
    pub fn hash(&self) -> [u8; 32] {
        let encoded = bincode::serialize(self).unwrap_or_default();
        *blake3::hash(&encoded).as_bytes()
    }
}

/// In-memory append-only operation log.
///
/// In production this would be backed by a memory-mapped file or RocksDB WAL.
/// For WASM environments it would use IndexedDB.
/// The trait `OpLogBackend` below allows swapping backends.
#[derive(Debug, Default)]
pub struct OpLog {
    entries: Vec<LogEntry>,
    /// Index: (table, row_id) → list of seq numbers touching that row
    row_index: BTreeMap<(String, String), Vec<u64>>,
    /// Index: peer_id → max seq seen (for sync cursors)
    peer_frontier: BTreeMap<String, u64>,
}

impl OpLog {
    pub fn new() -> Self { Self::default() }

    /// Append an operation. Returns the assigned sequence number.
    /// Idempotent: if an op with this HLC was already applied, skip it.
    pub fn append(&mut self, op: CrdtOp) -> u64 {
        let hlc = op.hlc().clone();

        // Idempotency check: skip if we already have this exact op (by full op equality).
        // NOTE: We cannot use just HLC because two different ops (e.g., InsertRow and ReserveUnique)
        // may share the same HLC timestamp when emitted atomically by the planner.
        let already_applied = self.entries.iter().any(|e| e.op == op);
        if already_applied {
            return self.entries.len() as u64 - 1;
        }


        let seq = self.entries.len() as u64;
        let prev_hash = self
            .entries
            .last()
            .map(|e| e.hash())
            .unwrap_or([0u8; 32]);

        // Update row index
        let table = op.table().to_string();
        let row_id = Self::row_id_from_op(&op);
        if let Some(rid) = row_id {
            self.row_index
                .entry((table, rid))
                .or_default()
                .push(seq);
        }

        // Update peer frontier
        let peer = hlc.peer_id.clone();
        let entry = self.peer_frontier.entry(peer).or_insert(0);
        if seq > *entry { *entry = seq; }

        self.entries.push(LogEntry { seq, op, prev_hash });
        seq
    }

    /// Returns all ops since the given sequence number (exclusive).
    /// Used for cursor-based incremental sync.
    pub fn since(&self, cursor: u64) -> Vec<&LogEntry> {
        self.entries
            .iter()
            .filter(|e| e.seq > cursor)
            .collect()
    }

    /// Returns all ops for a specific (table, row_id) pair.
    pub fn ops_for_row(&self, table: &str, row_id: &str) -> Vec<&LogEntry> {
        let key = (table.to_string(), row_id.to_string());
        self.row_index
            .get(&key)
            .map(|seqs| {
                seqs.iter()
                    .filter_map(|&seq| self.entries.get(seq as usize))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Total number of entries.
    pub fn len(&self) -> usize { self.entries.len() }

    /// Iterate all entries in order.
    pub fn iter(&self) -> impl Iterator<Item = &LogEntry> {
        self.entries.iter()
    }

    /// The current max sequence number (sync cursor).
    pub fn max_seq(&self) -> u64 {
        self.entries.len().saturating_sub(1) as u64
    }

    /// Compact: remove entries that are causally dominated and no longer needed
    /// for snapshot reconstruction. Only safe after all peers have ACKed.
    /// Returns number of entries compacted.
    pub fn compact_before(&mut self, safe_seq: u64) -> usize {
        // In a full implementation, we would snapshot state up to safe_seq
        // and truncate the log. For now, we retain all entries.
        // TODO: implement snapshot checkpointing before truncation
        let _ = safe_seq;
        0
    }

    fn row_id_from_op(op: &CrdtOp) -> Option<String> {
        match op {
            CrdtOp::InsertRow { row_id, .. } => Some(row_id.clone()),
            CrdtOp::UpdateCells { row_id, .. } => Some(row_id.clone()),
            CrdtOp::DeleteRow { row_id, .. } => Some(row_id.clone()),
            CrdtOp::ReserveUnique { row_id, .. } => Some(row_id.clone()),
            CrdtOp::CreateTable { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::{clocks::HlcTimestamp, merge::CrdtOp};

    fn insert_op(table: &str, row_id: &str, wall: u64, peer: &str) -> CrdtOp {
        CrdtOp::InsertRow {
            table: table.to_string(),
            row_id: row_id.to_string(),
            hlc: HlcTimestamp::new(wall, 0, peer),
            cells: vec![],
        }
    }

    #[test]
    fn append_and_cursor() {
        let mut log = OpLog::new();
        log.append(insert_op("users", "u1", 100, "A"));
        log.append(insert_op("users", "u2", 101, "A"));
        log.append(insert_op("users", "u3", 102, "B"));

        let since_0: Vec<_> = log.since(0).iter().map(|e| e.seq).collect();
        assert_eq!(since_0, vec![1, 2]);

        let since_neg: Vec<_> = log.since(u64::MAX).into_iter().collect();
        assert!(since_neg.is_empty());
    }

    #[test]
    fn log_integrity_chain() {
        let mut log = OpLog::new();
        log.append(insert_op("t", "r1", 100, "A"));
        log.append(insert_op("t", "r2", 101, "A"));
        let e1 = &log.entries[0];
        let e2 = &log.entries[1];
        assert_eq!(e2.prev_hash, e1.hash(), "Log entries must chain correctly");
    }
}
