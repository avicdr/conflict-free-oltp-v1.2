//! Reservation-based uniqueness coordination protocol.
//!
//! ## Problem
//! Pure CRDTs cannot enforce uniqueness because two peers can independently
//! claim the same unique value while offline, with no way to resolve the conflict
//! without coordination.
//!
//! ## Protocol Design
//! We implement a "reservation CRDT" — a replicated data structure where:
//! 1. Each unique-value claim is a `Reservation` object
//! 2. Reservations replicate via the op-log like any other CRDT op
//! 3. When two reservations for the same value arrive, we elect a winner
//!    deterministically using (HLC, PeerID, RowID) total ordering
//! 4. The loser is preserved in `conflict_log` — NOT silently deleted
//! 5. The winner is the canonical row in the relational table
//!
//! ## Winner Election
//! Given two reservations R1 and R2 for value V:
//!   winner = max(R1, R2) by (HLC, PeerID, RowID) lexicographic ordering
//!
//! This is deterministic regardless of sync order: any peer that has observed
//! both reservations will compute the same winner. Peers that have only seen
//! one reservation provisionally accept it until the other arrives.
//!
//! ## Invariant Proofs
//! - **Commutativity**: winner(R1, R2) == winner(R2, R1) — max is commutative
//! - **Associativity**: winner(winner(R1,R2), R3) == winner(R1, winner(R2,R3)) — max is associative  
//! - **Idempotence**: winner(R, R) == R — trivially
//! - **Determinism**: outcome depends only on (HLC, PeerID, RowID) ordering,
//!   never on arrival order or sync topology

use crate::crdt::clocks::HlcTimestamp;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// State of a unique-value reservation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReservationState {
    /// Provisionally claimed (offline, not yet resolved)
    Provisional,
    /// Won the conflict election — this row is canonical
    Won,
    /// Lost the conflict election — row moved to conflict state
    Lost { winner_row_id: String, winner_peer: String },
}

/// A single reservation for a unique value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reservation {
    /// The unique value being reserved (e.g., email = "alice@x.com")
    pub value: Vec<u8>,
    /// The row that owns this reservation
    pub row_id: String,
    /// The peer that created this reservation
    pub owner_peer: String,
    /// HLC timestamp of the reservation creation
    pub hlc: HlcTimestamp,
    /// Current state of this reservation
    pub state: ReservationState,
}

impl Reservation {
    /// Sort key for deterministic winner election: (HLC, PeerID, RowID)
    #[allow(dead_code)]
    fn election_key(&self) -> (&HlcTimestamp, &str, &str) {
        (&self.hlc, &self.owner_peer, &self.row_id)
    }

    /// Comparison for winner election: higher = wins
    #[allow(dead_code)]
    fn beats(&self, other: &Reservation) -> bool {
        // Total order: compare HLC (wall, logical, peer), then row_id
        let self_key = (self.hlc.wall_ms, self.hlc.logical, &self.hlc.peer_id, &self.row_id);
        let other_key = (other.hlc.wall_ms, other.hlc.logical, &other.hlc.peer_id, &other.row_id);
        self_key > other_key
    }
}

/// A conflict record stored for audit/recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictRecord {
    pub unique_column: String,
    pub value: Vec<u8>,
    pub loser_row_id: String,
    pub loser_peer: String,
    pub loser_hlc: HlcTimestamp,
    pub winner_row_id: String,
    pub winner_peer: String,
    pub winner_hlc: HlcTimestamp,
}

/// The uniqueness protocol state for a single table+column.
///
/// Maps: unique_value → Vec<Reservation>
/// After resolution: exactly one Won reservation, others Lost.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UniqueIndex {
    /// All reservations, including won and lost (never silently deleted)
    pub reservations: BTreeMap<Vec<u8>, Vec<Reservation>>,
    /// Conflict log: preserved for audit and recovery
    pub conflict_log: Vec<ConflictRecord>,
}

impl UniqueIndex {
    pub fn new() -> Self { Self::default() }

    /// Register a reservation for a unique value.
    /// Returns Ok(()) if provisionally accepted, Err if the value is already won by another row.
    pub fn reserve(&mut self, reservation: Reservation) -> Result<(), String> {
        let value = reservation.value.clone();
        let entry = self.reservations.entry(value.clone()).or_default();

        // Check for duplicate (idempotency)
        let already_exists = entry.iter().any(|r| {
            r.row_id == reservation.row_id && r.owner_peer == reservation.owner_peer
        });
        if already_exists {
            return Ok(());
        }

        entry.push(reservation);

        // Run election if there are multiple reservations
        if entry.len() > 1 {
            self.resolve_conflicts_for_value(&value.clone());
        }

        Ok(())
    }

    /// Resolve conflicts for a specific unique value.
    /// Elects exactly one winner; marks others as Lost.
    fn resolve_conflicts_for_value(&mut self, value: &[u8]) {
        let entry = match self.reservations.get_mut(value) {
            Some(e) => e,
            None => return,
        };

        if entry.len() <= 1 {
            return;
        }

        // Find winner: max by (HLC, PeerID, RowID)
        let winner_idx = entry
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| {
                let ka = (a.hlc.wall_ms, a.hlc.logical, a.hlc.peer_id.as_str(), a.row_id.as_str());
                let kb = (b.hlc.wall_ms, b.hlc.logical, b.hlc.peer_id.as_str(), b.row_id.as_str());
                ka.cmp(&kb)
            })
            .map(|(i, _)| i)
            .unwrap_or(0);

        let winner = entry[winner_idx].clone();

        // Mark all others as Lost, record conflicts
        for (i, r) in entry.iter_mut().enumerate() {
            if i == winner_idx {
                r.state = ReservationState::Won;
            } else {
                let conflict = ConflictRecord {
                    unique_column: String::new(), // filled by caller
                    value: r.value.clone(),
                    loser_row_id: r.row_id.clone(),
                    loser_peer: r.owner_peer.clone(),
                    loser_hlc: r.hlc.clone(),
                    winner_row_id: winner.row_id.clone(),
                    winner_peer: winner.owner_peer.clone(),
                    winner_hlc: winner.hlc.clone(),
                };
                self.conflict_log.push(conflict);
                r.state = ReservationState::Lost {
                    winner_row_id: winner.row_id.clone(),
                    winner_peer: winner.owner_peer.clone(),
                };
            }
        }
    }

    /// Returns the canonical winning row_id for a unique value, if resolved.
    pub fn canonical_owner(&self, value: &[u8]) -> Option<&str> {
        self.reservations.get(value)?.iter()
            .find(|r| matches!(r.state, ReservationState::Won | ReservationState::Provisional))
            .map(|r| r.row_id.as_str())
    }

    /// Check if a unique value is available (no Won/Provisional reservation exists for a different row).
    pub fn is_available_for(&self, value: &[u8], row_id: &str) -> bool {
        match self.reservations.get(value) {
            None => true,
            Some(entries) => entries.iter().all(|r| {
                r.row_id == row_id ||
                matches!(r.state, ReservationState::Lost { .. })
            }),
        }
    }

    /// Merge two UniqueIndex states.
    /// CRDT merge: union all reservations, re-run elections.
    pub fn merge(&mut self, other: &UniqueIndex) {
        for (value, other_reservations) in &other.reservations {
            let entry = self.reservations.entry(value.clone()).or_default();
            for r in other_reservations {
                let exists = entry.iter().any(|e| {
                    e.row_id == r.row_id && e.owner_peer == r.owner_peer
                });
                if !exists {
                    entry.push(r.clone());
                }
            }
            if entry.len() > 1 {
                self.resolve_conflicts_for_value(value);
            }
        }

        // Merge conflict logs (deduplicate by loser+winner pair)
        for conflict in &other.conflict_log {
            let exists = self.conflict_log.iter().any(|c| {
                c.loser_row_id == conflict.loser_row_id &&
                c.winner_row_id == conflict.winner_row_id
            });
            if !exists {
                self.conflict_log.push(conflict.clone());
            }
        }
    }

    /// Returns all losing row_ids for a given unique value (for FK/audit queries).
    pub fn losers(&self, value: &[u8]) -> Vec<&str> {
        self.reservations.get(value)
            .map(|entries| {
                entries.iter()
                    .filter(|r| matches!(r.state, ReservationState::Lost { .. }))
                    .map(|r| r.row_id.as_str())
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Per-table uniqueness manager (one UniqueIndex per UNIQUE column).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableUniqueness {
    /// column_name → UniqueIndex
    pub columns: BTreeMap<String, UniqueIndex>,
}

impl TableUniqueness {
    pub fn new() -> Self { Self::default() }

    pub fn get_index(&self, column: &str) -> Option<&UniqueIndex> {
        self.columns.get(column)
    }

    pub fn get_index_mut(&mut self, column: &str) -> &mut UniqueIndex {
        self.columns.entry(column.to_string()).or_insert_with(UniqueIndex::new)
    }

    pub fn merge(&mut self, other: &TableUniqueness) {
        for (col, other_idx) in &other.columns {
            self.columns
                .entry(col.clone())
                .or_insert_with(UniqueIndex::new)
                .merge(other_idx);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(wall: u64, peer: &str) -> HlcTimestamp { HlcTimestamp::new(wall, 0, peer) }

    fn reservation(value: &str, row_id: &str, peer: &str, wall: u64) -> Reservation {
        Reservation {
            value: value.as_bytes().to_vec(),
            row_id: row_id.to_string(),
            owner_peer: peer.to_string(),
            hlc: ts(wall, peer),
            state: ReservationState::Provisional,
        }
    }

    #[test]
    fn single_reservation_provisional() {
        let mut idx = UniqueIndex::new();
        idx.reserve(reservation("alice@x.com", "u1", "A", 100)).unwrap();
        assert_eq!(idx.canonical_owner(b"alice@x.com"), Some("u1"));
    }

    #[test]
    fn conflict_deterministic_winner() {
        // A and B both insert alice@x.com
        let mut idx_a = UniqueIndex::new();
        let mut idx_b = UniqueIndex::new();

        idx_a.reserve(reservation("alice@x.com", "u1", "A", 100)).unwrap();
        idx_b.reserve(reservation("alice@x.com", "u3", "B", 101)).unwrap(); // higher HLC

        idx_a.merge(&idx_b);
        idx_b.merge(&idx_a);

        // B has higher HLC (101 > 100), so u3@B wins
        assert_eq!(idx_a.canonical_owner(b"alice@x.com"), Some("u3"), "A must agree on winner");
        assert_eq!(idx_b.canonical_owner(b"alice@x.com"), Some("u3"), "B must agree on winner");

        // u1 is in loser list
        assert!(idx_a.losers(b"alice@x.com").contains(&"u1"), "Loser must be preserved");
    }

    #[test]
    fn merge_commutative() {
        let mut a = UniqueIndex::new();
        let mut b = UniqueIndex::new();
        a.reserve(reservation("v@x.com", "r1", "A", 100)).unwrap();
        b.reserve(reservation("v@x.com", "r2", "B", 101)).unwrap();

        let mut ab = a.clone(); ab.merge(&b);
        let mut ba = b.clone(); ba.merge(&a);

        assert_eq!(ab.canonical_owner(b"v@x.com"), ba.canonical_owner(b"v@x.com"),
            "Uniqueness merge must be commutative");
    }

    #[test]
    fn merge_idempotent() {
        let mut a = UniqueIndex::new();
        a.reserve(reservation("v@x.com", "r1", "A", 100)).unwrap();
        let copy = a.clone();
        a.merge(&copy);
        assert_eq!(a.canonical_owner(b"v@x.com"), Some("r1"));
    }

    #[test]
    fn loser_preserved_not_silently_deleted() {
        let mut a = UniqueIndex::new();
        let mut b = UniqueIndex::new();
        a.reserve(reservation("e@x.com", "u1", "A", 100)).unwrap();
        b.reserve(reservation("e@x.com", "u2", "B", 200)).unwrap();
        a.merge(&b);

        let losers = a.losers(b"e@x.com");
        assert!(!losers.is_empty(), "Loser must be preserved, not silently deleted");
        assert!(!a.conflict_log.is_empty(), "Conflict must be logged for audit");
    }
}
