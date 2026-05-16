//! CRDTdb: A CRDT-native distributed relational database engine.
//!
//! # Architecture Overview
//!
//! CRDTdb is a fully distributed, offline-first relational OLTP engine whose
//! external interface behaves like SQLite while its internal representation is
//! CRDT-native all the way down.
//!
//! ## Key Design Decisions
//!
//! ### Row Storage: OR-Map of MV-Registers
//! - **Row membership**: Observed-Remove Map (add-wins semantics)
//! - **Cell storage**: Multi-Value Registers (per-column, causal compaction)
//! - **Concurrent column updates**: Both survive (commutative merge)
//!
//! ### Delete vs Update: OPTION B (Updates attach to tombstones)
//! - Concurrent deletes and updates are both preserved
//! - The row becomes tombstoned, but cell data reflects updates
//! - Deterministic: outcome depends only on operations, not delivery order
//!
//! ### Uniqueness: Reservation-Based Protocol
//! - Unique values go through a CRDT reservation phase
//! - Conflicts resolved deterministically by (HLC, PeerID, RowID)
//! - Losers preserved in conflict_log, never silently deleted
//!
//! ### Secondary Indexes: Derived (OPTION A)
//! - Indexes rebuilt from canonical CRDT state
//! - Guarantees correctness: any peer with same state produces same index
//! - No separate replication protocol needed
//!
//! ### FK Policy: Tombstone (global, deterministic)
//! - Parent deletions tombstone the row, preserving FK validity
//! - Tombstoned rows excluded from SELECT but included in hashing
//!
//! ### Snapshot Hashing: Deterministic BLAKE3
//! - Tables lexicographically ordered
//! - Rows sorted by PK
//! - Columns in schema order
//! - All MV-register entries (not just canonical) included
//! - Bit-identical across all peers, merge orders, architectures

pub mod api;
pub mod constraints;
pub mod crdt;
pub mod index;
pub mod sql;
pub mod storage;
pub mod sync;
pub mod testing;

// Re-export the main Engine for convenient access
pub use api::engine::Engine;
pub use constraints::fk_resolution::FkPolicy;
