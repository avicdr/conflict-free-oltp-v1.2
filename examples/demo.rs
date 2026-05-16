//! Demo runner: illustrates the full CRDT-native database API.
//!
//! Run with: cargo run --example demo

use crdtdb::{Engine, FkPolicy};

fn separator(label: &str) {
    println!("\n{}", "═".repeat(60));
    println!("  {}", label);
    println!("{}", "═".repeat(60));
}

fn print_table(engine: &Engine, table: &str) {
    println!("\n  [{}]", table.to_uppercase());
    match engine.query(&format!("SELECT * FROM {}", table)) {
        Ok(rows) => {
            if rows.is_empty() {
                println!("  (empty)");
            }
            for row in &rows {
                let cells: Vec<String> = row.columns.iter().zip(row.values.iter())
                    .map(|(col, val)| {
                        let v = val.as_ref()
                            .map(|b| String::from_utf8_lossy(b).to_string())
                            .unwrap_or_else(|| "NULL".to_string());
                        format!("{}={}", col, v)
                    })
                    .collect();
                println!("  → {}", cells.join(", "));
            }
        }
        Err(e) => println!("  Query error: {}", e),
    }
}

fn main() {
    println!("\n╔══════════════════════════════════════════════════════════╗");
    println!("║         CRDTdb — Distributed Relational Engine          ║");
    println!("║         CRDT-Native · Offline-First · SQLite API        ║");
    println!("╚══════════════════════════════════════════════════════════╝\n");

    // ================================================================
    // SETUP: 3 Peers
    // ================================================================
    separator("SETUP: Initialize 3 offline peers");

    let schema = "
        CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT NOT NULL, name TEXT);
        CREATE TABLE orders (id TEXT PRIMARY KEY, user_id TEXT NOT NULL, status TEXT NOT NULL, total_cents INTEGER NOT NULL);
    ";

    let mut peer_a = Engine::open_with_policy(".", "A", FkPolicy::Tombstone);
    let mut peer_b = Engine::open_with_policy(".", "B", FkPolicy::Tombstone);
    let mut peer_c = Engine::open_with_policy(".", "C", FkPolicy::Tombstone);

    for (name, peer) in [("A", &mut peer_a), ("B", &mut peer_b), ("C", &mut peer_c)] {
        peer.execute(schema).unwrap();
        println!("  Peer {} initialized", name);
    }

    // ================================================================
    // STEP 1: A inserts two users
    // ================================================================
    separator("STEP 1: Peer A inserts u1 and u2 (offline)");
    peer_a.execute("INSERT INTO users VALUES ('u1', 'alice@x.com', 'Alice')").unwrap();
    peer_a.execute("INSERT INTO users VALUES ('u2', 'bob@x.com', 'Bob')").unwrap();
    println!("  A hash: {}", &peer_a.snapshot_hash()[..16]);
    print_table(&peer_a, "users");

    // ================================================================
    // STEP 2: B creates UNIQUENESS CONFLICT
    // ================================================================
    separator("STEP 2: Peer B inserts u3 with same email as u1 (CONFLICT!)");
    peer_b.execute("INSERT INTO users VALUES ('u3', 'alice@x.com', 'Alice Prime')").unwrap();
    println!("  B hash: {}", &peer_b.snapshot_hash()[..16]);
    print_table(&peer_b, "users");

    // ================================================================
    // STEP 3: C one-way syncs from A
    // ================================================================
    separator("STEP 3: Peer C one-way syncs from A");
    peer_c.sync_from(&peer_a);
    println!("  C hash: {}", &peer_c.snapshot_hash()[..16]);
    print_table(&peer_c, "users");

    // ================================================================
    // STEP 4: C deletes u1
    // ================================================================
    separator("STEP 4: Peer C deletes u1 (tombstone policy)");
    peer_c.execute("DELETE FROM users WHERE id = 'u1'").unwrap();
    println!("  C hash: {}", &peer_c.snapshot_hash()[..16]);
    print_table(&peer_c, "users");

    // ================================================================
    // STEP 5: A inserts order referencing u1 (FK conflict!)
    // ================================================================
    separator("STEP 5: Peer A inserts order o1 → u1 (parent deleted by C concurrently!)");
    peer_a.execute("INSERT INTO orders VALUES ('o1', 'u1', 'pending', 1200)").unwrap();
    print_table(&peer_a, "orders");

    // ================================================================
    // STEP 6: A updates u1's name
    // ================================================================
    separator("STEP 6: Peer A updates u1's name (concurrent with C's delete)");
    peer_a.execute("UPDATE users SET name = 'Alice Cooper' WHERE id = 'u1'").unwrap();

    // ================================================================
    // STEP 7: B updates u1's email (cross-column concurrent update)
    // ================================================================
    separator("STEP 7: Peer B updates u1's email (cross-column concurrent update)");
    peer_b.execute("UPDATE users SET email = 'alice@ex.org' WHERE id = 'u1'").unwrap();

    // ================================================================
    // STEP 8: Randomized pairwise sync until quiescence
    // ================================================================
    separator("STEP 8: Randomized pairwise sync until quiescence");
    println!("  Syncing A↔B, B↔C, A↔C (multiple rounds)...");

    for round in 1..=5 {
        peer_a.sync_with(&mut peer_b);
        peer_b.sync_with(&mut peer_c);
        peer_a.sync_with(&mut peer_c);
        peer_b.sync_with(&mut peer_a);
        peer_c.sync_with(&mut peer_b);
        peer_c.sync_with(&mut peer_a);

        let ha = peer_a.snapshot_hash();
        let hb = peer_b.snapshot_hash();
        let hc = peer_c.snapshot_hash();

        if ha == hb && hb == hc {
            println!("  ✓ Quiescence reached at round {}", round);
            break;
        }
    }

    // ================================================================
    // FINAL STATE
    // ================================================================
    separator("FINAL STATE: All peers converged");

    let hash_a = peer_a.snapshot_hash();
    let hash_b = peer_b.snapshot_hash();
    let hash_c = peer_c.snapshot_hash();

    println!("\n  Snapshot Hashes:");
    println!("    A: {}", hash_a);
    println!("    B: {}", hash_b);
    println!("    C: {}", hash_c);

    let converged = hash_a == hash_b && hash_b == hash_c;
    if converged {
        println!("\n  ✓ ALL HASHES IDENTICAL — Deterministic convergence achieved!");
    } else {
        println!("\n  ✗ Hashes differ — convergence failed");
    }

    println!("\n  Peer A — Final Tables:");
    print_table(&peer_a, "users");
    print_table(&peer_a, "orders");

    // ================================================================
    // ASSERTIONS
    // ================================================================
    separator("ASSERTIONS");

    assert_eq!(hash_a, hash_b, "A and B must have identical hashes");
    assert_eq!(hash_b, hash_c, "B and C must have identical hashes");
    println!("  ✓ Snapshot hashes identical");

    let users_a = peer_a.query("SELECT * FROM users").unwrap();
    let users_b = peer_b.query("SELECT * FROM users").unwrap();
    assert_eq!(users_a, users_b, "User tables must be identical");
    println!("  ✓ Table contents identical");

    let emails: Vec<_> = users_a.iter().filter_map(|r| r.get_str("email")).collect();
    let unique_emails: std::collections::BTreeSet<_> = emails.iter().collect();
    assert_eq!(emails.len(), unique_emails.len(), "UNIQUE(email) violated!");
    println!("  ✓ UNIQUE(email) invariant preserved ({} unique emails)", unique_emails.len());

    let orders_a = peer_a.query("SELECT * FROM orders").unwrap();
    let orders_b = peer_b.query("SELECT * FROM orders").unwrap();
    assert_eq!(orders_a, orders_b, "Orders tables must be identical");
    println!("  ✓ FK policy consistently enforced ({} visible orders)", orders_a.len());

    println!("\n╔══════════════════════════════════════════════════════════╗");
    println!("║  ALL ASSERTIONS PASSED — CRDTdb demo complete! 🎉       ║");
    println!("╚══════════════════════════════════════════════════════════╝\n");
}
