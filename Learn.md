# CRDTdb Architecture & Comprehensive Viva Prep Guide

This document is your definitive study guide for the CRDTdb viva. It provides an exhaustive, low-level breakdown of the codebase, explaining the algorithms, distributed systems theory, and design decisions behind every major file. 

---

## 1. The Core Storage Engine (CRDTs)
*The fundamental building blocks that allow leaderless replication and conflict-free eventual consistency.*

### `src/crdt/mv_register.rs` (Multi-Value Register)
- **What it is:** The storage mechanism for a single table cell (e.g., a specific user's email). It holds a set of concurrent values rather than a single overwriting value.
- **Why it's used:** It explicitly prevents the **Row-Level LWW (Last Writer Wins) anti-pattern**. If Alice edits `name` and Bob edits `email` on the same row concurrently, a traditional LWW system would overwrite one person's edit. By using an MVR *per column*, both edits survive seamlessly.
- **Algorithm - Causal Compaction:** To prevent **Unbounded Metadata** (another fatal anti-pattern), the MVR prunes dominated entries. An entry $X$ dominates $Y$ if $X.peer\_id == Y.peer\_id$ and $X.hlc \ge Y.hlc$. When a peer makes a new write, all of its previous timestamps for that cell are garbage collected.
- **Deterministic Resolution:** When multiple concurrent writes exist from different peers, the MVR deterministically picks a "canonical winner" by ordering the entries by their `HlcTimestamp` (Highest `wall_ms`, then `logical`, then `peer_id`).

### `src/crdt/orset.rs` (Observed-Remove Set)
- **What it is:** Manages row-level existence using Add-Wins semantics.
- **Why it's used:** It tracks whether a row is active or deleted. If Peer A deletes a row, but Peer B concurrently updates a column in that row, the Add-Wins semantics ensure the row becomes a "Tombstone" (hidden from `SELECT`), but Peer B's updates are mathematically preserved inside the underlying `MvRegisters`. This ensures that data is never silently destroyed due to network delivery order.

### `src/crdt/clocks.rs` (Hybrid Logical Clocks - HLC)
- **What it is:** Generates distributed timestamps `(wall_clock_ms, logical_counter, peer_id)`.
- **Why it's used:** Pure physical clocks suffer from drift (NTP isn't perfect), and pure logical clocks (Lamport) don't map to real-world time. HLCs combine both. They track real time, but if events happen faster than the physical clock ticks, the `logical_counter` increments. This guarantees a strict, deterministic total ordering of all events across the entire cluster without a coordinator.

---

## 2. SQL Interface & Execution
*Bridging the gap between the end-user and the CRDT backend.*

### `src/sql/parser.rs`
- **What it is:** A parser integration using the `sqlparser` crate. It translates raw SQL strings (`INSERT`, `UPDATE`, `CREATE TABLE`) into strongly typed Rust Abstract Syntax Trees (ASTs) via the `ParsedStatement` enum.
- **Why it's used:** The engine provides a standard relational interface. The parser validates syntax and maps SQL concepts to CRDT operations.

### `src/sql/executor.rs`
- **What it is:** The execution engine that applies `ParsedStatement` instructions to the CRDT structures.
- **How it works:** 
  - **`UPDATE`:** The executor evaluates the `WHERE` clause, finds the matching rows in the `OrSet`, locates the specific column's `MvRegister`, and calls `.write()` using a freshly generated HLC timestamp.
  - **`DELETE`:** Modifies the row's state in the `OrSet` to be a tombstone, without actually erasing the underlying cell vectors.

---

## 3. Distributed Constraints & Integrity
*Enforcing ACID-like properties in an AP (Available/Partition-tolerant) system.*

### `src/constraints/uniqueness.rs`
- **What it is:** Handles `UNIQUE` constraints across the cluster.
- **The Problem:** Pure CRDTs cannot natively enforce uniqueness (e.g., ensuring two users can't register the same email while partitioned) without a coordinator.
- **The Solution (Reservation Protocol):** The engine uses a deterministic escrow/reservation system. During a partition, both peers might successfully write the same unique value locally. Upon reconnection and sync, the system detects the "Uniqueness Storm". It mathematically agrees on exactly *one* winner by comparing the HLC timestamps. The loser's row is preserved in a conflict log but suspended/hidden from active queries.

### `src/constraints/fk_resolution.rs`
- **What it is:** Enforces Foreign Key relationships (e.g., Orders must belong to a valid User).
- **The Solution (Tombstone Policy):** If Peer A deletes a User, and Peer B concurrently creates an Order for that User, standard databases would throw a constraint violation or silently drop the Order. CRDTdb implements a strict global `Tombstone` policy. The User row is marked as a tombstone (hidden from `SELECT`), but technically still exists in the CRDT layer, allowing Peer B's Order to merge cleanly and remain mathematically valid.

---

## 4. Synchronization & Cluster Topology
*How peers communicate and converge.*

### `src/sync/`
- **What it is:** The pairwise replication protocol.
- **How it works:** It allows any two `Engine` instances to exchange state. They exchange their `OrSet` and `MvRegister` histories. Because all operations are **Commutative**, **Associative**, and **Idempotent**, the order in which peers sync does not matter. 
- **Convergence Guarantee:** To prove convergence, the system generates a `BLAKE3` hash of the entire database state. After a full sync, Peer A and Peer B are mathematically guaranteed to have the exact same hash.

---

## 5. The Observability Dashboard
*Simulating the physical network.*

### `src/bin/dashboard_server.rs`
- **What it is:** The Rust Axum REST & WebSocket server.
- **Why it's used:** It orchestrates the simulation. It holds $N$ isolated `Engine` instances in memory wrapped in thread-safe `Arc<Mutex<Engine>>` structures. 
- **Features:** It provides API endpoints to trigger SQL execution, partition network connections, dynamically add/remove peers, and inject "Chaos" (synthetic network drops). It emits WebSocket events containing the exact conflict resolution payloads so the UI can explain the outcomes.

### `dashboard/` (Next.js + React Flow)
- **What it is:** The interactive React frontend.
- **Why it's used:** Uses React Flow to draw the peer graph. Uses Zustand for state management. It visually proves that offline peers drift out of sync, and then immediately converge upon reconnection.

---

## 🎓 Viva Question Bank (Deep Dive)

### 1. How do you prevent Last-Writer-Wins (LWW) data loss?
> "We explicitly avoid Row-level LWW. We implement Multi-Value Registers (MVR) at the **cell** level. If Alice updates `name` and Bob updates `email` on the same row concurrently, both edits survive because they mutate different MVRs. If they edit the exact same cell, the MVR keeps both versions mathematically, and uses the HLC timestamp to deterministically pick a canonical winner for `SELECT` queries."

### 2. How do you prevent metadata from growing infinitely as writers are added?
> "Vector clocks and raw histories grow unbounded, which is an anti-pattern. We solve this using **Causal Compaction**. Within an MVR, if Peer A writes a new value, the algorithm immediately prunes any of Peer A's older entries for that specific cell. Dominance is defined as `X.peer_id == Y.peer_id && X.hlc >= Y.hlc`. This ensures the metadata size is bounded by the number of concurrent writers, not the total number of historical edits."

### 3. How does the system handle offline uniqueness conflicts without a central coordinator?
> "Pure CRDTs cannot guarantee global uniqueness. We address this using a deterministic reservation protocol. If two partitioned peers insert the same unique email, both accept it locally. When they reconnect, the system detects the violation. It uses the Hybrid Logical Clock to deterministically pick one canonical winner. The loser is not silently deleted; it is suspended and logged, preserving the data but enforcing the constraint."

### 4. Why use a Tombstone policy for Foreign Keys instead of Cascading Deletes?
> "In an offline-first system, cascading deletes can cause catastrophic data loss if network packets arrive out of order. If we physically delete a parent row, a concurrent child insertion from another peer would be orphaned and lost. By using a Tombstone policy, a deleted parent is hidden from user queries but mathematically preserved in the CRDT layer. This guarantees that concurrent child inserts can still attach to the parent, ensuring mathematical convergence when the partition heals."

### 5. What proves that your deterministic reads are actually working?
> "We generate a deterministic `BLAKE3` hash of the entire database state. The hash lexicographically orders the tables, sorts rows by Primary Key, and includes every single MVR entry (not just the canonical winners). We can prove that regardless of the order updates were applied, or how many network partitions occurred, once all peers sync, their BLAKE3 hashes are bit-for-bit identical."

### 6. Are you using a Server-Authoritative fallback for conflicts?
> "Absolutely not. That is an auto-disqualifying anti-pattern. There is no 'master' node or central source of truth. Every single peer in the cluster runs its own `Engine` and resolves conflicts completely locally using pure, commutative CRDT mathematics. The dashboard server merely holds the instances in memory to simulate a physical network, it does not orchestrate conflict resolution."
