//! Observed-Remove Map (OR-Map) for row membership.
//!
//! An OR-Map supports concurrent insert and delete with deterministic semantics:
//! - Each insertion is tagged with a unique token (HLC + peer_id)
//! - A delete removes specific observed tokens
//! - "Add wins" if a concurrent insert is not yet observed by the deleter
//!
//! This is the standard "add-wins" OR-Set semantics extended to a map.
//!
//! ## CRDT Invariants
//! - **Commutativity**: union of entries + intersection of removes is symmetric
//! - **Associativity**: set-union operations are associative
//! - **Idempotence**: re-adding/removing same-token entry is a no-op

use crate::crdt::clocks::HlcTimestamp;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// A token uniquely identifying one "insertion" of a row.
/// Format: (wall_ms, logical, peer_id)
pub type Token = (u64, u32, String);

fn token(hlc: &HlcTimestamp) -> Token {
    (hlc.wall_ms, hlc.logical, hlc.peer_id.clone())
}

/// The state of a row in the OR-Map.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RowState {
    /// Row is alive with its insertion token set.
    Alive(BTreeSet<Token>),
    /// Row is tombstoned (for FK policy compliance).
    Tombstoned {
        tokens: BTreeSet<Token>,
        deleted_at: HlcTimestamp,
    },
}

impl RowState {
    pub fn is_alive(&self) -> bool {
        matches!(self, RowState::Alive(t) if !t.is_empty())
    }

    pub fn is_tombstoned(&self) -> bool {
        matches!(self, RowState::Tombstoned { .. })
    }

    pub fn tokens(&self) -> &BTreeSet<Token> {
        match self {
            RowState::Alive(t) => t,
            RowState::Tombstoned { tokens, .. } => tokens,
        }
    }
}

/// OR-Map: maps row IDs to their CRDT membership state.
///
/// Each entry carries:
/// - A set of live insertion tokens (add-wins semantics)
/// - A set of remove tokens (observed tokens at delete time)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct OrMap {
    /// row_id → (live_tokens, removed_tokens, optional tombstone_ts)
    pub rows: BTreeMap<String, OrMapEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct OrMapEntry {
    /// Tokens for live insertions of this row
    pub live: BTreeSet<Token>,
    /// Tokens that have been observed-removed
    pub removed: BTreeSet<Token>,
    /// If tombstoned, the HLC of the delete operation
    pub tombstone_ts: Option<HlcTimestamp>,
}

impl OrMapEntry {
    /// A row is "live" if live \ removed is non-empty
    pub fn is_live(&self) -> bool {
        self.live.difference(&self.removed).next().is_some()
    }

    pub fn is_tombstoned(&self) -> bool {
        self.tombstone_ts.is_some()
    }

    /// Visible to normal SELECT: live and not tombstoned
    pub fn is_visible(&self) -> bool {
        self.is_live() && !self.is_tombstoned()
    }
}

impl OrMap {
    pub fn new() -> Self { Self::default() }

    /// Insert a row with a given HLC token.
    pub fn insert(&mut self, row_id: &str, hlc: &HlcTimestamp) {
        let entry = self.rows.entry(row_id.to_string()).or_default();
        entry.live.insert(token(hlc));
        // Inserting clears tombstone (resurrection)
        entry.tombstone_ts = None;
    }

    /// Delete a row: move all currently-live tokens to removed.
    /// If fk_tombstone is true, mark as tombstoned rather than fully removing.
    pub fn delete(&mut self, row_id: &str, hlc: &HlcTimestamp, fk_tombstone: bool) {
        if let Some(entry) = self.rows.get_mut(row_id) {
            // Observed-remove: remove all currently-live tokens
            for t in entry.live.clone() {
                entry.removed.insert(t);
            }
            if fk_tombstone {
                entry.tombstone_ts = Some(hlc.clone());
            }
        }
    }

    /// Merge two OR-Maps.
    /// Union live sets, union removed sets, take max tombstone.
    /// CRDT merge: commutative, associative, idempotent.
    pub fn merge(&mut self, other: &OrMap) {
        for (row_id, other_entry) in &other.rows {
            let entry = self.rows.entry(row_id.clone()).or_default();

            // Union live tokens
            for t in &other_entry.live {
                entry.live.insert(t.clone());
            }
            // Union removed tokens
            for t in &other_entry.removed {
                entry.removed.insert(t.clone());
            }
            // Take the "later" tombstone
            match (&entry.tombstone_ts, &other_entry.tombstone_ts) {
                (None, Some(ts)) => entry.tombstone_ts = Some(ts.clone()),
                (Some(a), Some(b)) if b > a => entry.tombstone_ts = Some(b.clone()),
                _ => {}
            }
        }
    }

    /// Check if a row is visible for normal SELECT queries.
    pub fn is_visible(&self, row_id: &str) -> bool {
        self.rows.get(row_id).map(|e| e.is_visible()).unwrap_or(false)
    }

    /// Check if a row exists in any form (alive or tombstoned) — for FK validation.
    pub fn exists_for_fk(&self, row_id: &str) -> bool {
        self.rows.get(row_id).map(|e| e.is_live() || e.is_tombstoned()).unwrap_or(false)
    }

    /// List all visible (live, non-tombstoned) row IDs in sorted order.
    pub fn visible_rows(&self) -> Vec<&str> {
        let mut ids: Vec<&str> = self
            .rows
            .iter()
            .filter(|(_, e)| e.is_visible())
            .map(|(id, _)| id.as_str())
            .collect();
        ids.sort();
        ids
    }

    /// List ALL rows including tombstoned (for snapshot hashing and FK checks).
    pub fn all_rows(&self) -> Vec<(&str, &OrMapEntry)> {
        self.rows.iter().map(|(id, e)| (id.as_str(), e)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(wall: u64, peer: &str) -> HlcTimestamp {
        HlcTimestamp::new(wall, 0, peer)
    }

    #[test]
    fn insert_and_visible() {
        let mut m = OrMap::new();
        m.insert("r1", &ts(100, "A"));
        assert!(m.is_visible("r1"));
    }

    #[test]
    fn delete_makes_invisible() {
        let mut m = OrMap::new();
        m.insert("r1", &ts(100, "A"));
        m.delete("r1", &ts(200, "A"), false);
        assert!(!m.is_visible("r1"));
    }

    #[test]
    fn tombstone_invisible_but_fk_valid() {
        let mut m = OrMap::new();
        m.insert("r1", &ts(100, "A"));
        m.delete("r1", &ts(200, "A"), true);
        assert!(!m.is_visible("r1"), "Tombstoned row not visible to SELECT");
        assert!(m.exists_for_fk("r1"), "Tombstoned row valid for FK");
    }

    #[test]
    fn concurrent_insert_add_wins() {
        // A inserts r1, B deletes r1 concurrently (B hasn't seen A's insert token)
        let mut map_a = OrMap::new();
        let mut map_b = OrMap::new();

        map_a.insert("r1", &ts(100, "A")); // A inserts

        // B's delete only removes tokens B has seen (none from A yet)
        map_b.insert("r1", &ts(100, "A")); // B synced then
        map_b.delete("r1", &ts(101, "B"), false);

        // A later inserts with a new token (concurrent with B's delete)
        map_a.insert("r1", &ts(102, "A"));

        // Merge
        map_a.merge(&map_b);

        // A's second insert token (102, "A") is NOT in B's removed set — add wins
        assert!(map_a.is_visible("r1"), "Add-wins: concurrent insert survives delete");
    }

    #[test]
    fn merge_commutative() {
        let mut a = OrMap::new();
        let mut b = OrMap::new();
        a.insert("r1", &ts(100, "A"));
        b.delete("r1", &ts(200, "B"), false);

        let mut ab = a.clone(); ab.merge(&b);
        let mut ba = b.clone(); ba.merge(&a);
        assert_eq!(ab, ba);
    }

    #[test]
    fn merge_idempotent() {
        let mut m = OrMap::new();
        m.insert("r1", &ts(100, "A"));
        let orig = m.clone();
        m.merge(&orig);
        assert_eq!(m, orig);
    }
}
