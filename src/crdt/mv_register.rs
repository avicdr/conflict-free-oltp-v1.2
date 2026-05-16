//! Multi-Value (MV) Register with causal compaction.
//!
//! ## CRDT Invariants
//! - **Commutativity**: merge(A,B) == merge(B,A) — union + dominance prune is symmetric
//! - **Associativity**: merge(merge(A,B),C) == merge(A,merge(B,C)) — union is associative
//! - **Idempotence**: merge(A,A) == A — duplicate entries pruned by dominance
//!
//! ## Causal Compaction
//! Entry X dominates entry Y iff X.peer_id == Y.peer_id AND X.hlc >= Y.hlc.
//! Only truly concurrent entries (different peers, incomparable clocks) coexist.

use crate::crdt::clocks::HlcTimestamp;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvEntry {
    pub hlc: HlcTimestamp,
    pub value: Option<Vec<u8>>,
}

impl MvEntry {
    /// Entry A dominates B if same peer and A.hlc >= B.hlc
    pub fn dominates(&self, other: &MvEntry) -> bool {
        self.hlc.peer_id == other.hlc.peer_id && self.hlc >= other.hlc
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MvRegister {
    /// Keyed by (wall_ms, logical, peer_id) for stable BTree ordering
    entries: BTreeMap<(u64, u32, String), MvEntry>,
}

impl MvRegister {
    pub fn new() -> Self { Self::default() }

    /// Write a new value. Removes prior entries from the same peer (causal compaction).
    pub fn write(&mut self, hlc: HlcTimestamp, value: Option<Vec<u8>>) {
        let peer = hlc.peer_id.clone();
        // Remove all prior same-peer entries (they are dominated by this write)
        self.entries.retain(|_, e| e.hlc.peer_id != peer);
        let key = (hlc.wall_ms, hlc.logical, hlc.peer_id.clone());
        self.entries.insert(key, MvEntry { hlc, value });
    }

    /// Merge two MV-registers: union entries, then compact dominated ones.
    pub fn merge(&mut self, other: &MvRegister) {
        for (key, entry) in &other.entries {
            self.entries.entry(key.clone()).or_insert_with(|| entry.clone());
        }
        self.compact();
    }

    /// Remove entries dominated by another entry in the set.
    fn compact(&mut self) {
        let entries: Vec<_> = self.entries.values().cloned().collect();
        self.entries.retain(|_, candidate| {
            !entries.iter().any(|e| e.dominates(candidate) && e.hlc != candidate.hlc)
        });
    }

    /// All concurrent values (full MV set).
    pub fn values(&self) -> impl Iterator<Item = &MvEntry> {
        self.entries.values()
    }

    /// Canonical winner: max by (wall_ms, logical, peer_id) — deterministic total order.
    pub fn canonical_value(&self) -> Option<&MvEntry> {
        self.entries.values().last()
    }

    pub fn read(&self) -> Option<&[u8]> {
        self.canonical_value().and_then(|e| e.value.as_deref())
    }

    pub fn is_empty(&self) -> bool { self.entries.is_empty() }

    pub fn is_deleted(&self) -> bool {
        match self.canonical_value() {
            Some(e) => e.value.is_none(),
            None => true,
        }
    }

    pub fn concurrent_count(&self) -> usize { self.entries.len() }

    pub fn entries_snapshot(&self) -> &BTreeMap<(u64, u32, String), MvEntry> {
        &self.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ts(wall: u64, logical: u32, peer: &str) -> HlcTimestamp {
        HlcTimestamp::new(wall, logical, peer)
    }
    fn bytes(s: &str) -> Option<Vec<u8>> { Some(s.as_bytes().to_vec()) }

    #[test]
    fn single_write_and_read() {
        let mut reg = MvRegister::new();
        reg.write(ts(100, 0, "A"), bytes("Alice"));
        assert_eq!(reg.read(), Some(b"Alice".as_ref()));
    }

    #[test]
    fn later_write_same_peer_dominates() {
        let mut reg = MvRegister::new();
        reg.write(ts(100, 0, "A"), bytes("Alice"));
        reg.write(ts(200, 0, "A"), bytes("Alice Cooper"));
        assert_eq!(reg.concurrent_count(), 1);
        assert_eq!(reg.read(), Some(b"Alice Cooper".as_ref()));
    }

    #[test]
    fn concurrent_writes_both_survive() {
        let mut a = MvRegister::new();
        let mut b = MvRegister::new();
        a.write(ts(100, 0, "A"), bytes("Alice Cooper"));
        b.write(ts(100, 0, "B"), bytes("alice@ex.org"));
        a.merge(&b);
        assert_eq!(a.concurrent_count(), 2);
    }

    #[test]
    fn merge_commutative() {
        let mut a = MvRegister::new();
        let mut b = MvRegister::new();
        a.write(ts(100, 0, "A"), bytes("v1"));
        b.write(ts(100, 0, "B"), bytes("v2"));
        let mut ab = a.clone(); ab.merge(&b);
        let mut ba = b.clone(); ba.merge(&a);
        assert_eq!(ab, ba);
    }

    #[test]
    fn merge_idempotent() {
        let mut reg = MvRegister::new();
        reg.write(ts(100, 0, "A"), bytes("v1"));
        let orig = reg.clone();
        reg.merge(&orig);
        assert_eq!(reg, orig);
    }

    #[test]
    fn merge_associative() {
        let mut a = MvRegister::new();
        let mut b = MvRegister::new();
        let mut c = MvRegister::new();
        a.write(ts(100, 0, "A"), bytes("va"));
        b.write(ts(101, 0, "B"), bytes("vb"));
        c.write(ts(102, 0, "C"), bytes("vc"));
        let mut ab_c = a.clone(); ab_c.merge(&b); ab_c.merge(&c);
        let mut bc = b.clone(); bc.merge(&c);
        let mut a_bc = a.clone(); a_bc.merge(&bc);
        assert_eq!(ab_c, a_bc);
    }
}
