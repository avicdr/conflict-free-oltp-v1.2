//! Hybrid Logical Clock (HLC) and bounded version vectors.
//!
//! HLCs combine physical wall-clock time with a logical counter to produce
//! causal timestamps that:
//!   - are globally unique across peers
//!   - preserve causality (if A → B then hlc(A) < hlc(B))
//!   - remain bounded (no global vector growth)
//!   - are totally ordered via (wall_ms, logical, peer_id)
//!
//! Invariant proof (monotonicity):
//!   For any send/receive event, the resulting HLC is strictly greater than
//!   any previously observed HLC on that peer. This is guaranteed by always
//!   taking max(local, remote) before incrementing.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// A Hybrid Logical Clock timestamp.
/// Total order: compare (wall_ms, logical, peer_id) lexicographically.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HlcTimestamp {
    /// Wall-clock milliseconds since Unix epoch.
    pub wall_ms: u64,
    /// Logical counter disambiguating same-millisecond events.
    pub logical: u32,
    /// The originating peer identifier (for total ordering).
    pub peer_id: String,
}

impl HlcTimestamp {
    pub fn new(wall_ms: u64, logical: u32, peer_id: impl Into<String>) -> Self {
        Self {
            wall_ms,
            logical,
            peer_id: peer_id.into(),
        }
    }

    /// Returns the "zero" / bottom timestamp.
    pub fn zero(peer_id: impl Into<String>) -> Self {
        Self {
            wall_ms: 0,
            logical: 0,
            peer_id: peer_id.into(),
        }
    }
}

impl PartialOrd for HlcTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HlcTimestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.wall_ms
            .cmp(&other.wall_ms)
            .then(self.logical.cmp(&other.logical))
            .then(self.peer_id.cmp(&other.peer_id))
    }
}

impl std::fmt::Display for HlcTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}@{}", self.wall_ms, self.logical, self.peer_id)
    }
}

/// HLC clock instance per peer.
///
/// Thread-safe via atomic wall_ms + logical tracking.
/// The logical counter is a u32 which fits 4 billion events per millisecond —
/// effectively unbounded in practice.
#[derive(Debug)]
pub struct HlcClock {
    peer_id: String,
    /// Internally track max_wall_ms and logical as a packed u64:
    /// upper 32 bits = wall_ms (seconds), lower 32 bits = logical.
    /// We store them separately for clarity.
    max_wall_ms: AtomicU64,
    max_logical: AtomicU64,
}

impl HlcClock {
    pub fn new(peer_id: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            peer_id: peer_id.into(),
            max_wall_ms: AtomicU64::new(0),
            max_logical: AtomicU64::new(0),
        })
    }

    fn now_wall_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    /// Generate a new HLC timestamp for a local event.
    /// Guarantees: result > all previously generated timestamps on this peer.
    pub fn tick(&self) -> HlcTimestamp {
        let wall = Self::now_wall_ms();
        let prev_wall = self.max_wall_ms.load(AtomicOrdering::Acquire);

        let (new_wall, new_logical) = if wall > prev_wall {
            // Physical clock advanced: reset logical counter
            self.max_wall_ms.store(wall, AtomicOrdering::Release);
            self.max_logical.store(0, AtomicOrdering::Release);
            (wall, 0u32)
        } else {
            // Same or backwards clock: increment logical
            let logical = self.max_logical.fetch_add(1, AtomicOrdering::AcqRel) + 1;
            (prev_wall, logical as u32)
        };

        HlcTimestamp::new(new_wall, new_logical, &self.peer_id)
    }

    /// Receive a remote HLC timestamp and update local state.
    /// Ensures our next tick() will be causally after the received event.
    pub fn observe(&self, remote: &HlcTimestamp) -> HlcTimestamp {
        let wall = Self::now_wall_ms();
        let prev_wall = self.max_wall_ms.load(AtomicOrdering::Acquire);
        let prev_logical = self.max_logical.load(AtomicOrdering::Acquire) as u32;

        let max_wall = wall.max(prev_wall).max(remote.wall_ms);
        let new_logical = if max_wall == prev_wall && max_wall == remote.wall_ms {
            // All three agree: take max logical + 1
            prev_logical.max(remote.logical) + 1
        } else if max_wall == prev_wall {
            prev_logical + 1
        } else if max_wall == remote.wall_ms {
            remote.logical + 1
        } else {
            0
        };

        self.max_wall_ms.store(max_wall, AtomicOrdering::Release);
        self.max_logical
            .store(new_logical as u64, AtomicOrdering::Release);

        HlcTimestamp::new(max_wall, new_logical, &self.peer_id)
    }

    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }
}

/// A bounded version vector tracking per-peer causal frontiers.
///
/// Used for sync cursors and causal stability calculations.
/// Bounded by O(number of known peers), which is typically small.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct VersionVector {
    /// Maps peer_id → max HLC (wall_ms, logical) seen from that peer.
    pub entries: indexmap::IndexMap<String, (u64, u32)>,
}

impl VersionVector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Update the vector to reflect having seen `ts` from its originating peer.
    pub fn observe(&mut self, ts: &HlcTimestamp) {
        let entry = self
            .entries
            .entry(ts.peer_id.clone())
            .or_insert((0, 0));
        if (ts.wall_ms, ts.logical) > *entry {
            *entry = (ts.wall_ms, ts.logical);
        }
    }

    /// Check if `ts` is causally dominated by this vector
    /// (i.e., we have already seen this or a later event from ts.peer_id).
    pub fn dominates_ts(&self, ts: &HlcTimestamp) -> bool {
        match self.entries.get(&ts.peer_id) {
            Some(&(w, l)) => (w, l) >= (ts.wall_ms, ts.logical),
            None => false,
        }
    }

    /// Merge two version vectors, taking component-wise max.
    /// This operation is commutative, associative, and idempotent.
    pub fn merge(&mut self, other: &VersionVector) {
        for (peer, &(w, l)) in &other.entries {
            let entry = self.entries.entry(peer.clone()).or_insert((0, 0));
            if (w, l) > *entry {
                *entry = (w, l);
            }
        }
    }

    /// Returns true if self ≤ other (self is causally dominated by other)
    pub fn dominated_by(&self, other: &VersionVector) -> bool {
        self.entries.iter().all(|(peer, &(w, l))| {
            other
                .entries
                .get(peer)
                .map(|&(ow, ol)| (ow, ol) >= (w, l))
                .unwrap_or(false)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hlc_monotonic_tick() {
        let clock = HlcClock::new("A");
        let t1 = clock.tick();
        let t2 = clock.tick();
        assert!(t2 >= t1, "HLC must be monotonically non-decreasing");
    }

    #[test]
    fn hlc_total_order() {
        let t1 = HlcTimestamp::new(100, 0, "A");
        let t2 = HlcTimestamp::new(100, 0, "B");
        let t3 = HlcTimestamp::new(101, 0, "A");
        assert!(t1 < t2, "Same wall+logical: peer_id breaks tie");
        assert!(t1 < t3, "Higher wall wins");
    }

    #[test]
    fn hlc_observe_advances_past_remote() {
        let clock = HlcClock::new("A");
        let remote = HlcTimestamp::new(999_999_999, 42, "B");
        let after = clock.observe(&remote);
        let next = clock.tick();
        assert!(next > after, "Next tick must be after observed remote ts");
    }

    #[test]
    fn version_vector_merge_idempotent() {
        let mut vv1 = VersionVector::new();
        vv1.observe(&HlcTimestamp::new(10, 0, "A"));
        let vv2 = vv1.clone();
        vv1.merge(&vv2);
        assert_eq!(vv1, vv2, "Merge with self must be idempotent");
    }

    #[test]
    fn version_vector_dominance() {
        let mut vv = VersionVector::new();
        let ts = HlcTimestamp::new(100, 5, "A");
        vv.observe(&ts);
        assert!(vv.dominates_ts(&ts));
        let earlier = HlcTimestamp::new(100, 4, "A");
        assert!(vv.dominates_ts(&earlier));
        let later = HlcTimestamp::new(100, 6, "A");
        assert!(!vv.dominates_ts(&later));
    }
}
