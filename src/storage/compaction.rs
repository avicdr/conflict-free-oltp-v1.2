//! Convergence-safe compaction and garbage collection.
//!
//! ## GC Rules
//! GC may only occur when:
//! 1. Causal stability is established: all known peers have observed the ops to be GC'd
//! 2. A snapshot checkpoint has been written (so state can be recovered without the log)
//! 3. Tombstone entries have been ACKed by all peers (for FK safety)
//!
//! Premature GC breaks convergence by allowing peers to re-add "deleted" data
//! without the necessary remove-tokens.

use std::collections::BTreeMap;

/// Tracks which sequence numbers have been ACKed by each peer.
#[derive(Debug, Default)]
pub struct GcTracker {
    /// peer_id → max seq acknowledged
    peer_acks: BTreeMap<String, u64>,
    /// Total number of known peers
    known_peers: Vec<String>,
}

impl GcTracker {
    pub fn new(known_peers: Vec<String>) -> Self {
        Self {
            peer_acks: BTreeMap::new(),
            known_peers,
        }
    }

    /// Record that a peer has acknowledged up to `seq`.
    pub fn ack(&mut self, peer_id: &str, seq: u64) {
        let entry = self.peer_acks.entry(peer_id.to_string()).or_insert(0);
        if seq > *entry { *entry = seq; }
    }

    /// Returns the "safe" sequence number: the minimum across all known peers.
    /// Any op at or before this seq can be safely GC'd.
    pub fn safe_seq(&self) -> u64 {
        if self.known_peers.is_empty() {
            return 0;
        }
        self.known_peers.iter()
            .filter_map(|p| self.peer_acks.get(p))
            .copied()
            .min()
            .unwrap_or(0)
    }

    /// True if all peers have ACKed up to at least `seq`.
    pub fn is_safe_to_gc(&self, seq: u64) -> bool {
        self.safe_seq() >= seq
    }
}

/// A snapshot checkpoint used to replace compacted log entries.
#[derive(Debug, Clone)]
pub struct Checkpoint {
    /// The sequence number this checkpoint captures state through.
    pub seq: u64,
    /// BLAKE3 hash of the snapshot at this checkpoint.
    pub hash: [u8; 32],
    /// The serialized RowStore state at this checkpoint.
    pub state_bytes: Vec<u8>,
}

/// Compaction policy configuration.
#[derive(Debug, Clone)]
pub struct CompactionPolicy {
    /// Minimum number of log entries before triggering compaction.
    pub min_entries: usize,
    /// Target maximum log entries after compaction.
    pub target_entries: usize,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            min_entries: 10_000,
            target_entries: 1_000,
        }
    }
}
