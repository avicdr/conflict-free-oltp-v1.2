# CRDTdb Final Architecture & Comprehensive Tech Stack Guide

This document is the definitive, exhaustive breakdown of the entire CRDTdb project. It details exactly **what** technologies we used, **why** we used them, **how** they interconnect, and the deep architectural **significance** of every single file in the codebase.

---

## 1. The Technology Stack & Design Philosophy

### Backend & Core Database Engine (Rust)
* **Rust (The Foundation):** We built the core database engine entirely in Rust. 
  * *Why:* A database requires absolute control over memory, predictable performance (no garbage collection pauses), and extreme safety. Rust’s strict compiler and borrow checker mathematically guarantee that our highly concurrent, deeply threaded CRDT merge operations are free of data races.
* **Axum & Tokio (Network Layer):**
  * *Why:* We needed a server capable of handling massive concurrency to simulate a distributed network. Axum (a web framework) runs on top of Tokio (Rust’s async runtime). It allowed us to expose standard REST APIs for the dashboard while simultaneously holding open stateful WebSockets to stream real-time cluster convergence events directly to the UI.
* **sqlparser (Translation Layer):**
  * *Why:* To make the database usable, it needs a standard interface. Instead of inventing a proprietary querying language, we used the `sqlparser` crate to take standard SQL strings (`INSERT`, `UPDATE`, `SELECT`) and parse them into strongly-typed Abstract Syntax Trees (ASTs).
* **BLAKE3 (Cryptographic Proofs):**
  * *Why:* BLAKE3 is a highly parallelized, extremely fast cryptographic hash function. We use it to compute the "Snapshot Hash" of the entire database state. This gives us mathematical proof of convergence: if Peer A and Peer B yield the exact same BLAKE3 hash, we know their CRDT states are bit-for-bit identical.

### Frontend Observability Dashboard (Next.js & TypeScript)
* **Next.js (App Router) & React 19:** 
  * *Why:* Provides a robust, highly optimized, and modern architecture for building a complex Single Page Application that needs to react to rapid WebSocket streams.
* **React Flow (Topology Visualization):**
  * *Why:* Distributed systems are notoriously difficult to visualize. React Flow allows us to draw a dynamic, interactive graph of the database cluster, showing which nodes are online, offline, and actively syncing.
* **Zustand (Global State Management):**
  * *Why:* Zustand is a lightweight, hook-based state manager. As our Rust backend pumps thousands of events through the WebSocket, Zustand instantly digests these payloads, updating the state of `TableExplorer`, the `ConflictPanel`, and the `Timeline` simultaneously without the heavy boilerplate of Redux.
* **Framer Motion & Tailwind CSS (UI/UX):**
  * *Why:* We aimed for a production-grade, "wow-factor" observability tool. Framer Motion provides physics-based animations (like rows smoothly sliding into the data table upon convergence), while Tailwind CSS ensures a pristine, responsive dark-mode aesthetic.

---

## 2. Exhaustive File-by-File Breakdown

### 📂 `src/crdt/` (The Mathematical Core)
*This directory contains the Conflict-Free Replicated Data Type primitives. It is the engine that guarantees cluster convergence without ever needing a central leader node.*

* **`clocks.rs` (Hybrid Logical Clocks & Version Vectors)**
  * *What it does:* Implements `HlcTimestamp` which tracks `(wall_ms, logical_counter, peer_id)`. Also implements `VersionVector` which tracks the causal history a peer has seen.
  * *How it works:* Whenever a peer creates an event, it "ticks" its clock. If multiple events happen in the same millisecond, the `logical_counter` increments.
  * *Significance:* Pure physical clocks suffer from drift. Pure logical clocks don't track real time. HLCs solve both. Furthermore, the `peer_id` field acts as a 100% deterministic, arbitrary tie-breaker. This file prevents the database from ever deadlocking over a conflict.
* **`mv_register.rs` (Multi-Value Register)**
  * *What it does:* The storage structure for a single table cell (e.g., an email field). 
  * *How it works:* It uses a `BTreeMap` sorted by HLC. When concurrent offline edits happen, it does **not** overwrite the loser. Instead, it mathematically preserves both edits in the map.
  * *Significance:* This eliminates the fatal "Row-Level Last-Writer-Wins" anti-pattern. Data is never silently destroyed. It uses "Causal Compaction" to garbage collect old edits from the *same* peer, keeping memory usage strictly bounded.
* **`orset.rs` (Observed-Remove Set)**
  * *What it does:* Manages row existence. 
  * *How it works:* Uses Add-Wins semantics. It tracks insertions and deletions via unique tags. 
  * *Significance:* If `Peer A` deletes a row while `Peer B` concurrently updates a cell in that row, the Add-Wins logic guarantees the row correctly transitions into a "Tombstone" (hidden from `SELECT`) without deleting the underlying cell data.
* **`merge.rs` (Convergence Logic)**
  * *What it does:* The trait definitions for merging two CRDT objects.
  * *Significance:* Enforces that all merge operations are Commutative ($A+B = B+A$), Associative ($(A+B)+C = A+(B+C)$), and Idempotent ($A+A = A$). This guarantees that network packets can arrive in any order, or be duplicated, without corrupting the database.

### 📂 `src/sql/` (The Relational Bridge)
*This directory bridges the gap between traditional SQL developers and the underlying NoSQL CRDT mathematics.*

* **`parser.rs`**
  * *What it does:* Wraps the `sqlparser` crate. Takes a raw string like `UPDATE users SET name='Alice'` and parses it.
  * *Significance:* Validates syntax and ensures that malformed queries are rejected before they ever touch the execution engine.
* **`planner.rs`**
  * *What it does:* Takes the AST from the parser and maps it to specific tables, rows, and columns.
  * *Significance:* It optimizes how the engine will traverse the internal `OrSet` and `MvRegister` structures to fulfill the query.
* **`executor.rs`**
  * *What it does:* The actual execution engine. It loops through the planned operations, generates fresh HLC timestamps, and calls `.write()` on the specific cell registers or updates the Tombstone status in the `OrSet`.
  * *Significance:* This is the exact point where SQL is translated into immutable, timestamped CRDT events.

### 📂 `src/constraints/` (Distributed ACID Compliance)
*Solving the hardest problems in distributed computing: enforcing global relational rules without a central coordinator.*

* **`uniqueness_protocol.rs`**
  * *What it does:* Handles `UNIQUE` constraints across the cluster.
  * *How it works:* It implements a reservation protocol. If two disconnected peers both accept the same unique email locally, this protocol detects the "Uniqueness Storm" upon reconnection. It mathematically compares the HLC timestamps, declares one the canonical winner, and moves the loser to a conflict log.
  * *Significance:* Pure CRDTs cannot guarantee global uniqueness. This file provides a deterministic, safe fallback that prevents the database from exploding when constraints are violated offline.
* **`fk_resolution.rs`**
  * *What it does:* Enforces Foreign Key (FK) relationships.
  * *Significance:* Employs a strict **Tombstone Policy**. If a parent row is deleted, it is never physically erased from memory; it is merely tombstoned. This ensures that if a delayed network packet arrives containing a child row that references the deleted parent, the child can still securely attach to the parent's tombstone without causing a fatal "Orphaned Data" panic.

### 📂 `src/storage/` (Durability & State Hashing)
*Ensuring data survives sudden power loss.*

* **`op_log.rs` (Write-Ahead Log)**
  * *What it does:* Durably appends every SQL mutation to the hard drive *before* updating RAM.
  * *Significance:* If the server crashes mid-query, the engine reads this log on reboot to perfectly reconstruct the in-memory CRDT state.
* **`row_store.rs`**
  * *What it does:* The physical memory layout mapping Table Names $\rightarrow$ Row IDs $\rightarrow$ `OrSet` & `MvRegisters`.
* **`snapshots.rs` (BLAKE3 Hashing)**
  * *What it does:* Periodically compresses the `op_log` by taking a snapshot of the current state.
  * *Significance:* Also contains the critical `compute_snapshot_hash` function. It iterates through tables alphabetically, rows by primary key, and hashes *every single concurrent entry* in the MVR. This deterministic serialization is what proves cluster convergence.

### 📂 `src/sync/` (The Network Layer)
*How peers actually share data.*

* **`anti_entropy.rs`**
  * *What it does:* Implements a pairwise gossip protocol.
  * *How it works:* When `Peer A` connects to `Peer B`, they exchange their `VersionVectors`. `Peer A` looks at `B`'s vector, identifies exactly which causal events `B` is missing, and transmits only those specific CRDT deltas.
  * *Significance:* Makes network synchronization highly efficient. Nodes don't have to send their entire database to each other; they only send the delta of what happened while they were disconnected.

### 📂 `src/bin/` & `dashboard/` (The User Interface)
*The interactive simulation environment.*

* **`src/bin/dashboard_server.rs` (The Orchestrator)**
  * *What it does:* A specialized Axum server holding multiple isolated `Engine` instances in memory (simulating physical machines). 
  * *Significance:* Exposes endpoints like `/api/peer/:id/partition` to simulate severed network cables, and `/api/scenario` to trigger complex offline race conditions. It broadcasts all actions over a WebSocket.
* **`dashboard/components/TableExplorer.tsx`**
  * *What it does:* The visual SQL table viewer in the UI.
  * *Significance:* It is deeply reactive. We specifically tied its auto-refresh hook to the mathematical `op_count` of the CRDT state inside Zustand. When a severed node reconnects and syncs, the `op_count` changes, and the Table Explorer instantly slides in the correct, converged data.
* **`dashboard/components/ConflictPanel.tsx`**
  * *What it does:* Tracks race conditions.
  * *Significance:* When the backend detects a scenario like a "Delete vs Update Race" or a "Uniqueness Storm", it pushes a rich payload to the frontend. This panel visually explains to the user *why* a specific value was chosen as the canonical winner, making the opaque CRDT math completely transparent.
