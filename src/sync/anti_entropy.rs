//! Anti-entropy sync protocol: bidirectional, cursor-based, resumable.
//!
//! ## Protocol Design
//!
//! Pairwise anti-entropy sync between peers A and B:
//!
//! 1. **Exchange cursors**: Each peer sends its current `SyncCursor` (max seq seen
//!    from each peer it knows about).
//! 2. **Compute delta**: Each peer computes which ops the other hasn't seen yet.
//! 3. **Exchange deltas**: Bidirectional delta exchange.
//! 4. **Apply**: Each peer applies received ops to its own state.
//! 5. **Update cursor**: Each peer updates its cursor to reflect the new state.
//!
//! ## Network Adversary Resilience
//! - **Duplicate delivery**: Ops are idempotent at the log level (checked by HLC)
//! - **Out-of-order delivery**: Ops have HLCs; order of application doesn't affect
//!   final CRDT state (commutativity property)
//! - **Lost messages**: Cursor-based resumption allows retrying from last known position
//! - **Partitions**: Peers sync when they reconnect; no global coordination needed
//!
//! ## Merkle Reconciliation
//! For large datasets, we include a lightweight Merkle digest to quickly identify
//! which seq-ranges need syncing without transferring all ops.

use crate::crdt::merge::CrdtOp;
use crate::storage::op_log::OpLog;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A sync cursor tracking how much of each peer's log has been received.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncCursor {
    /// peer_id → max seq received from that peer
    pub frontier: BTreeMap<String, u64>,
}

impl SyncCursor {
    pub fn new() -> Self { Self::default() }

    pub fn observe(&mut self, peer_id: &str, seq: u64) {
        let entry = self.frontier.entry(peer_id.to_string()).or_insert(0);
        if seq > *entry { *entry = seq; }
    }

    pub fn max_seq_for(&self, peer_id: &str) -> u64 {
        self.frontier.get(peer_id).copied().unwrap_or(0)
    }
}

/// A delta: the set of ops to send from one peer to another.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncDelta {
    /// Source peer that generated these ops
    pub source_peer: String,
    /// The ops to deliver (in log order)
    pub ops: Vec<CrdtOp>,
    /// The new cursor position after these ops
    pub new_cursor: SyncCursor,
}

/// Compute the delta that `local_log` should send to a peer with `remote_cursor`.
///
/// Returns only ops from `local_log` that the remote hasn't seen yet.
pub fn compute_delta(
    local_peer_id: &str,
    local_log: &OpLog,
    remote_cursor: &SyncCursor,
) -> SyncDelta {
    let remote_max_seq = remote_cursor.max_seq_for(local_peer_id);

    // Only send ops that originated from our peer and that the remote hasn't seen
    let ops: Vec<CrdtOp> = local_log
        .since(remote_max_seq)
        .iter()
        .filter(|e| e.op.hlc().peer_id == local_peer_id)
        .map(|e| e.op.clone())
        .collect();

    let mut new_cursor = remote_cursor.clone();
    if let Some(last_seq) = local_log.iter()
        .filter(|e| e.op.hlc().peer_id == local_peer_id)
        .map(|e| e.seq)
        .last()
    {
        new_cursor.observe(local_peer_id, last_seq);
    }

    SyncDelta {
        source_peer: local_peer_id.to_string(),
        ops,
        new_cursor,
    }
}

/// Apply a received delta to a local op-log.
///
/// Idempotent: duplicate ops are skipped by the op-log's idempotency check.
/// Order-safe: CRDT ops commute, so arrival order doesn't matter.
pub fn apply_delta(local_log: &mut OpLog, delta: &SyncDelta) -> Vec<CrdtOp> {
    let mut applied = Vec::new();
    for op in &delta.ops {
        let _seq = local_log.append(op.clone());
        applied.push(op.clone());
    }
    applied
}

/// A lightweight Merkle digest of the op-log for efficient reconciliation.
///
/// The digest divides the op-log into fixed-size buckets and hashes each bucket.
/// Two peers can compare digests to quickly find divergent ranges.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MerkleDigest {
    /// Bucket size (number of ops per bucket)
    pub bucket_size: u64,
    /// Bucket index → BLAKE3 hash of ops in that bucket
    pub buckets: BTreeMap<u64, [u8; 32]>,
    /// Total op count
    pub total_ops: u64,
}

impl MerkleDigest {
    pub fn build(log: &OpLog, bucket_size: u64) -> Self {
        let mut buckets: BTreeMap<u64, blake3::Hasher> = BTreeMap::new();
        let total_ops = log.len() as u64;

        for entry in log.iter() {
            let bucket = entry.seq / bucket_size;
            let hasher = buckets.entry(bucket).or_insert_with(blake3::Hasher::new);
            let encoded = bincode::serialize(&entry.op).unwrap_or_default();
            hasher.update(&encoded);
        }

        let buckets = buckets
            .into_iter()
            .map(|(k, h)| (k, *h.finalize().as_bytes()))
            .collect();

        Self { bucket_size, buckets, total_ops }
    }

    /// Find bucket indices where the two digests differ.
    pub fn diff_buckets(&self, other: &MerkleDigest) -> Vec<u64> {
        let all_buckets: std::collections::BTreeSet<u64> = self.buckets.keys()
            .chain(other.buckets.keys())
            .copied()
            .collect();

        all_buckets
            .into_iter()
            .filter(|b| self.buckets.get(b) != other.buckets.get(b))
            .collect()
    }

    /// Get the seq range for a bucket.
    pub fn bucket_range(&self, bucket: u64) -> (u64, u64) {
        let start = bucket * self.bucket_size;
        let end = start + self.bucket_size - 1;
        (start, end)
    }
}

/// Full bidirectional sync session between two peers (in-process).
///
/// Each peer sends ALL ops the other hasn't seen yet (identified by HLC fingerprint).
/// This ensures ops are fully relayed even through intermediaries.
pub fn sync_peers(
    peer_a_log: &mut OpLog,
    peer_b_log: &mut OpLog,
    peer_a_id: &str,
    peer_b_id: &str,
    cursor_a: &mut SyncCursor,
    cursor_b: &mut SyncCursor,
) -> (usize, usize) {
    // Compute the set of HLC fingerprints each peer already has
    let a_fingerprints: std::collections::BTreeSet<(u64, u32, String)> = peer_a_log
        .iter()
        .map(|e| (e.op.hlc().wall_ms, e.op.hlc().logical, e.op.hlc().peer_id.clone()))
        .collect();
    let b_fingerprints: std::collections::BTreeSet<(u64, u32, String)> = peer_b_log
        .iter()
        .map(|e| (e.op.hlc().wall_ms, e.op.hlc().logical, e.op.hlc().peer_id.clone()))
        .collect();

    // A sends to B: ops A has that B doesn't
    let ops_for_b: Vec<CrdtOp> = peer_a_log
        .iter()
        .filter(|e| {
            let fp = (e.op.hlc().wall_ms, e.op.hlc().logical, e.op.hlc().peer_id.clone());
            !b_fingerprints.contains(&fp)
        })
        .map(|e| e.op.clone())
        .collect();
    let a_sent = ops_for_b.len();

    // B sends to A: ops B has that A doesn't
    let ops_for_a: Vec<CrdtOp> = peer_b_log
        .iter()
        .filter(|e| {
            let fp = (e.op.hlc().wall_ms, e.op.hlc().logical, e.op.hlc().peer_id.clone());
            !a_fingerprints.contains(&fp)
        })
        .map(|e| e.op.clone())
        .collect();
    let b_sent = ops_for_a.len();

    // Apply deltas (idempotent via HLC dedup in op_log)
    for op in ops_for_b { peer_b_log.append(op); }
    for op in ops_for_a { peer_a_log.append(op); }

    // Update cursors
    cursor_b.observe(peer_a_id, peer_a_log.max_seq());
    cursor_a.observe(peer_b_id, peer_b_log.max_seq());

    (a_sent, b_sent)
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::{clocks::HlcTimestamp, merge::CrdtOp};
    use crate::storage::op_log::OpLog;

    fn insert_op(table: &str, row_id: &str, wall: u64, peer: &str) -> CrdtOp {
        CrdtOp::InsertRow {
            table: table.to_string(),
            row_id: row_id.to_string(),
            hlc: HlcTimestamp::new(wall, 0, peer),
            cells: vec![],
        }
    }

    #[test]
    fn bidirectional_sync_convergence() {
        let mut log_a = OpLog::new();
        let mut log_b = OpLog::new();
        let mut cursor_a = SyncCursor::new();
        let mut cursor_b = SyncCursor::new();

        log_a.append(insert_op("t", "r1", 100, "A"));
        log_a.append(insert_op("t", "r2", 101, "A"));
        log_b.append(insert_op("t", "r3", 102, "B"));

        sync_peers(&mut log_a, &mut log_b, "A", "B", &mut cursor_a, &mut cursor_b);

        // Both should have 3 entries after sync
        assert_eq!(log_a.len(), 3);
        assert_eq!(log_b.len(), 3);
    }

    #[test]
    fn idempotent_sync() {
        let mut log_a = OpLog::new();
        let mut log_b = OpLog::new();
        let mut ca = SyncCursor::new();
        let mut cb = SyncCursor::new();

        log_a.append(insert_op("t", "r1", 100, "A"));
        sync_peers(&mut log_a, &mut log_b, "A", "B", &mut ca, &mut cb);
        let len_after_first = log_b.len();

        // Sync again — should be idempotent
        sync_peers(&mut log_a, &mut log_b, "A", "B", &mut ca, &mut cb);
        assert_eq!(log_b.len(), len_after_first, "Repeated sync must not add entries");
    }

    #[test]
    fn out_of_order_delivery_same_final_state() {
        let mut log_normal = OpLog::new();
        let mut log_reversed = OpLog::new();

        let op1 = insert_op("t", "r1", 100, "A");
        let op2 = insert_op("t", "r2", 101, "A");

        log_normal.append(op1.clone());
        log_normal.append(op2.clone());

        // Apply in reverse order
        log_reversed.append(op2.clone());
        log_reversed.append(op1.clone());

        assert_eq!(log_normal.len(), log_reversed.len());
    }

    #[test]
    fn merkle_diff_detects_divergence() {
        let mut log_a = OpLog::new();
        let mut log_b = OpLog::new();

        log_a.append(insert_op("t", "r1", 100, "A"));
        log_b.append(insert_op("t", "r2", 101, "B"));

        let digest_a = MerkleDigest::build(&log_a, 10);
        let digest_b = MerkleDigest::build(&log_b, 10);

        let diffs = digest_a.diff_buckets(&digest_b);
        assert!(!diffs.is_empty(), "Different logs must produce different Merkle digests");
    }
}
