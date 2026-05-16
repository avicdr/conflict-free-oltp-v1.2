# CRDTdb Observability Dashboard

A production-grade web dashboard for observing, simulating, and interacting with a CRDT-native distributed relational database system. The dashboard visually demonstrates offline-first replication, conflict resolution, convergence, and distributed synchronization between peers in real-time.

## Features

- **Interactive Peer Cluster Graph:** Visualizes active peers, their sync status, and replication pulses flowing between nodes using React Flow.
- **Dynamic Peer Scaling:** Add and remove simulated database peers dynamically on the fly to test varying cluster topologies.
- **Chaos Engineering Controls:** Introduce synthetic latency and packet drops to simulate adversarial network conditions and observe eventual convergence.
- **Detailed Conflict Resolution Panel:** Tracks and visualizes concurrent edits, the policies applied to resolve them, and the exact final value persisted by the CRDT engine.
- **Live Event Timeline:** A real-time stream of cluster events like database synchronizations, schema updates, network partitions, and scenario milestones.

## Tech Stack

- **Backend:** Rust, Axum, Tokio (WebSocket & REST API)
- **Frontend:** Next.js (App Router), React 19, TypeScript
- **Styling & Animations:** Tailwind CSS v4, Framer Motion
- **State Management:** Zustand
- **Visualization:** React Flow

## Prerequisites

To run this project locally, ensure you have the following installed:

1. **Rust Toolchain:** (Includes `cargo` and `rustc`)
   - Install via [rustup](https://rustup.rs/): `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
2. **Node.js:** (v18+ recommended)
   - Install from [nodejs.org](https://nodejs.org/) or use a version manager like `nvm`.
3. **npm:** (Comes bundled with Node.js)

## Installation & Setup

1. **Clone the repository** (if you haven't already):
   ```bash
   git clone <repository-url>
   cd oltp
   ```

2. **Install Frontend Dependencies:**
   The backend dependencies are managed automatically via Cargo, but the Next.js frontend dependencies must be installed via npm.
   ```bash
   cd dashboard
   npm install
   cd ..
   ```

## Running the Application

A convenient bash script is provided to simultaneously compile and launch both the Rust backend and the Next.js frontend.

1. **Make the start script executable:**
   ```bash
   chmod +x start-dashboard.sh
   ```

2. **Run the cluster:**
   ```bash
   ./start-dashboard.sh
   ```

3. **Open the Dashboard:**
   Open your browser and navigate to: **[http://localhost:3000](http://localhost:3000)**

> **Note:** The Rust backend will be served on `http://localhost:8888` to provide the API and WebSocket connections. If you prefer to run them separately, you can run `cargo run --bin dashboard-server` in the root directory, and `npm run dev` in the `dashboard/` directory.

## Usage Guide

- **Syncing Peers:** Click the **Sync All Peers** button to force a full-mesh synchronization between all currently online peers.
- **Partitioning / Simulating Offline:** Click the **Offline** button on any peer card to drop it from the network. It will stop receiving sync updates. Click **Reconnect** to bring it back online.
- **Running Scenarios:** Use the scenarios dropdown on the left side of the UI to trigger specific database edge cases. The predefined scenarios include:
  - **Concurrent Update:** Simulates two disconnected peers updating the exact same cell concurrently. Demonstrates the Multi-Value Register (MVR) maintaining both edits and using the Hybrid Logical Clock (HLC) to pick a deterministic winner upon syncing.
  - **Delete vs Update:** Simulates one peer deleting a row while a disconnected peer updates a column on that same row. Demonstrates the strict Tombstone policy—the row is hidden from queries, but the concurrent cell update is preserved in the underlying CRDT layer.
  - **Uniqueness Storm:** Simulates a split-brain scenario where multiple partitioned peers insert the same `UNIQUE` value (e.g., an email). Upon reconnect, the reservation protocol detects the violation and deterministically picks one canonical winner without destroying the losers' data.
  - **Multi-Hop Sync:** Demonstrates transitive replication by passing data sequentially through a chain of peers (e.g., P0 → P1 → P2 → P3) without a central server.
  - **Offline Reconnect:** Drops a peer from the network, simulates cluster activity, and then brings the peer back online to observe immediate mathematical convergence.
  - **Chaos Test:** Introduces synthetic latency and packet drops across the cluster to prove that the CRDT engine always achieves eventual consistency despite adverse network conditions.
- **Scaling the Cluster:** Use the **+ Add Peer** button to spin up a new database engine instance on the fly.
