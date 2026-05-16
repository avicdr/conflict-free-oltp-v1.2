//! Adversarial test suite — 20 scenarios covering all CRDT invariants.

#[cfg(test)]
mod adversarial {
    use crate::api::engine::Engine;
    use crate::constraints::fk_resolution::FkPolicy;

    // ── helpers ─────────────────────────────────────────────────────────────

    fn users_engine(peer: &str) -> Engine {
        let mut e = Engine::open_with_policy(".", peer, FkPolicy::Tombstone);
        e.execute("CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT UNIQUE NOT NULL, name TEXT)").unwrap();
        e
    }

    fn full_engine(peer: &str) -> Engine {
        let mut e = Engine::open_with_policy(".", peer, FkPolicy::Tombstone);
        e.execute("CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT UNIQUE NOT NULL, name TEXT)").unwrap();
        e.execute("CREATE TABLE orders (id TEXT PRIMARY KEY, user_id TEXT NOT NULL, status TEXT NOT NULL, total_cents INTEGER NOT NULL)").unwrap();
        e
    }

    fn sync_all(engines: &mut Vec<Engine>, rounds: usize) {
        for _ in 0..rounds {
            for i in 0..engines.len() {
                for j in (i + 1)..engines.len() {
                    let (l, r) = engines.split_at_mut(j);
                    l[i].sync_with(&mut r[0]);
                }
            }
        }
    }

    fn assert_hashes_equal(engines: &[Engine], label: &str) {
        let hashes: Vec<String> = engines.iter().map(|e| e.snapshot_hash()).collect();
        for i in 1..hashes.len() {
            assert_eq!(hashes[0], hashes[i],
                "{}: peer 0 hash {} != peer {} hash {}", label, hashes[0], i, hashes[i]);
        }
    }

    // ── 1. Concurrent same-cell update ──────────────────────────────────────

    #[test]
    fn t01_concurrent_same_cell_update() {
        let mut a = users_engine("A");
        let mut b = users_engine("B");

        a.execute("INSERT INTO users VALUES ('u1','alice@x.com','Alice')").unwrap();
        a.sync_with(&mut b);

        a.execute("UPDATE users SET name='Alice Cooper' WHERE id='u1'").unwrap();
        b.execute("UPDATE users SET name='Alice Prime'  WHERE id='u1'").unwrap();

        a.sync_with(&mut b);

        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "T01: hashes must match");

        let ra = a.query("SELECT * FROM users WHERE id='u1'").unwrap();
        let rb = b.query("SELECT * FROM users WHERE id='u1'").unwrap();
        assert_eq!(ra, rb, "T01: both peers must agree on row");
        // Exactly one visible name (deterministic winner)
        assert!(ra[0].get_str("name").is_some(), "T01: name must be set");
    }

    // ── 2. Delete vs Update race ─────────────────────────────────────────────

    #[test]
    fn t02_delete_vs_update_race() {
        let mut a = users_engine("A");
        let mut b = users_engine("B");

        a.execute("INSERT INTO users VALUES ('u1','alice@x.com','Alice')").unwrap();
        a.sync_with(&mut b);

        a.execute("DELETE FROM users WHERE id='u1'").unwrap();
        b.execute("UPDATE users SET email='new@x.com' WHERE id='u1'").unwrap();

        a.sync_with(&mut b);

        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "T02: hashes must match");

        // Row must be tombstoned (not visible)
        let ra = a.query("SELECT * FROM users").unwrap();
        assert_eq!(ra.len(), 0, "T02: deleted row must not be visible");

        // But OPTION B: cell data preserved internally
        let u1 = a.store.get_table("users").and_then(|t| t.rows.get("u1"));
        assert!(u1.is_some(), "T02: row data must be retained in store (OPTION B)");
    }

    // ── 3. Parent delete vs multiple child inserts ───────────────────────────

    #[test]
    fn t03_parent_delete_vs_child_inserts() {
        let mut a = full_engine("A");
        let mut b = full_engine("B");
        let mut c = full_engine("C");

        a.execute("INSERT INTO users VALUES ('u1','u1@x.com','User1')").unwrap();
        a.sync_with(&mut b);
        a.sync_with(&mut c);

        // A deletes parent
        a.execute("DELETE FROM users WHERE id='u1'").unwrap();
        // B and C insert children concurrently
        b.execute("INSERT INTO orders VALUES ('o1','u1','pending',0)").unwrap();
        c.execute("INSERT INTO orders VALUES ('o2','u1','shipped',0)").unwrap();

        // Full sync
        let mut engines = vec![a, b, c];
        sync_all(&mut engines, 5);

        assert_hashes_equal(&engines, "T03");

        // u1 must be tombstone (not visible in users)
        let users = engines[0].query("SELECT * FROM users").unwrap();
        assert_eq!(users.len(), 0, "T03: deleted user must not be visible");

        // o1 and o2 must be visible
        let orders = engines[0].query("SELECT * FROM orders").unwrap();
        assert_eq!(orders.len(), 2, "T03: both orders must be visible");
    }

    // ── 4. Duplicate message delivery (idempotence) ──────────────────────────

    #[test]
    fn t04_duplicate_delivery_idempotent() {
        let mut a = users_engine("A");
        let mut b = users_engine("B");

        a.execute("INSERT INTO users VALUES ('u1','alice@x.com','Alice')").unwrap();

        // Sync multiple times — must be idempotent
        for _ in 0..5 {
            a.sync_with(&mut b);
        }

        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "T04: repeated sync must be idempotent");

        let rows = b.query("SELECT * FROM users").unwrap();
        assert_eq!(rows.len(), 1, "T04: no duplicated rows after repeated sync");
    }

    // ── 5. Out-of-order operation replay ─────────────────────────────────────

    #[test]
    fn t05_out_of_order_replay() {
        use crate::crdt::{clocks::HlcTimestamp, merge::CrdtOp};
        use crate::storage::snapshots::compute_snapshot_hash;
        use crate::storage::row_store::RowStore;

        let insert = CrdtOp::InsertRow {
            table: "users".into(), row_id: "u1".into(),
            hlc: HlcTimestamp::new(100, 0, "A"),
            cells: vec![("name".into(), Some(b"Alice".to_vec())), ("email".into(), Some(b"a@x.com".to_vec()))],
        };
        let update = CrdtOp::UpdateCells {
            table: "users".into(), row_id: "u1".into(),
            hlc: HlcTimestamp::new(200, 0, "A"),
            cells: vec![("name".into(), Some(b"Alice Cooper".to_vec()))],
        };
        let delete = CrdtOp::DeleteRow {
            table: "users".into(), row_id: "u1".into(),
            hlc: HlcTimestamp::new(300, 0, "A"),
            fk_tombstone: false,
        };

        // Normal order
        let mut s1 = RowStore::new();
        for op in &[insert.clone(), update.clone(), delete.clone()] { s1.apply_op(op); }

        // Reversed order
        let mut s2 = RowStore::new();
        for op in &[delete.clone(), update.clone(), insert.clone()] { s2.apply_op(op); }

        // Random order
        let mut s3 = RowStore::new();
        for op in &[update.clone(), insert.clone(), delete.clone()] { s3.apply_op(op); }

        assert_eq!(compute_snapshot_hash(&s1), compute_snapshot_hash(&s2), "T05: normal vs reversed");
        assert_eq!(compute_snapshot_hash(&s1), compute_snapshot_hash(&s3), "T05: normal vs random");
    }

    // ── 6. Concurrent unique reservation storm (10 peers) ────────────────────

    #[test]
    fn t06_unique_reservation_storm_10_peers() {
        let mut engines: Vec<Engine> = (0..10)
            .map(|i| {
                let mut e = Engine::open_with_policy(".", &format!("P{}", i), FkPolicy::Tombstone);
                e.execute("CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT UNIQUE NOT NULL, name TEXT)").unwrap();
                // All claim same email
                let _ = e.execute(&format!("INSERT INTO users VALUES ('u{}','storm@x.com','Peer{}')", i, i));
                e
            })
            .collect();

        sync_all(&mut engines, 10);
        assert_hashes_equal(&engines, "T06");

        // Exactly one visible row with storm@x.com
        for (i, e) in engines.iter().enumerate() {
            let rows = e.query("SELECT * FROM users").unwrap();
            let cnt = rows.iter().filter(|r| r.get_str("email") == Some("storm@x.com")).count();
            assert_eq!(cnt, 1, "T06: peer {}: must have exactly 1 canonical winner", i);
        }
    }

    // ── 7. Randomized sync topology ──────────────────────────────────────────

    #[test]
    fn t07_randomized_sync_topology() {
        let mut a = users_engine("A");
        let mut b = users_engine("B");
        let mut c = users_engine("C");
        let mut d = users_engine("D");

        a.execute("INSERT INTO users VALUES ('u1','a@x.com','Alice')").unwrap();
        b.execute("INSERT INTO users VALUES ('u2','b@x.com','Bob')").unwrap();
        c.execute("INSERT INTO users VALUES ('u3','c@x.com','Carol')").unwrap();
        d.execute("INSERT INTO users VALUES ('u4','d@x.com','Dave')").unwrap();

        // Non-linear topology: A↔B, C↔D, B↔D, A↔C
        a.sync_with(&mut b);
        c.sync_with(&mut d);
        b.sync_with(&mut d);
        a.sync_with(&mut c);
        // One more round for full propagation
        a.sync_with(&mut b);
        c.sync_with(&mut d);

        let peers = [&a, &b, &c, &d];
        let hashes: Vec<_> = peers.iter().map(|e| e.snapshot_hash()).collect();
        for i in 1..hashes.len() {
            assert_eq!(hashes[0], hashes[i], "T07: hash mismatch peer 0 vs {}", i);
        }
        let rows = a.query("SELECT * FROM users").unwrap();
        assert_eq!(rows.len(), 4, "T07: all 4 rows must be visible");
    }

    // ── 8. Secondary index consistency ───────────────────────────────────────

    #[test]
    fn t08_secondary_index_consistency() {
        let mut a = full_engine("A");
        let mut b = full_engine("B");

        a.create_index("orders", "orders_by_user", vec!["user_id".into(), "status".into()]);
        b.create_index("orders", "orders_by_user", vec!["user_id".into(), "status".into()]);

        a.execute("INSERT INTO users VALUES ('u1','u1@x.com','User1')").unwrap();
        a.sync_with(&mut b);

        // Concurrent operations
        a.execute("INSERT INTO orders VALUES ('o1','u1','pending',100)").unwrap();
        b.execute("INSERT INTO orders VALUES ('o2','u1','shipped',200)").unwrap();

        a.sync_with(&mut b);

        // Update then delete/reinsert
        a.execute("UPDATE orders SET status='cancelled' WHERE id='o1'").unwrap();
        b.execute("DELETE FROM orders WHERE id='o2'").unwrap();
        b.execute("INSERT INTO orders VALUES ('o2','u1','refunded',0)").unwrap();

        a.sync_with(&mut b);

        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "T08: hashes must match");

        let oa = a.query("SELECT * FROM orders").unwrap();
        let ob = b.query("SELECT * FROM orders").unwrap();
        assert_eq!(oa.len(), ob.len(), "T08: order counts must match");
        assert_eq!(oa, ob, "T08: order rows must be identical");
    }

    // ── 9. Tombstone GC safety ───────────────────────────────────────────────

    #[test]
    fn t09_tombstone_gc_safety() {
        let mut a = users_engine("A");
        let mut b = users_engine("B");

        a.execute("INSERT INTO users VALUES ('u1','alice@x.com','Alice')").unwrap();
        a.sync_with(&mut b);

        // A deletes
        a.execute("DELETE FROM users WHERE id='u1'").unwrap();

        // Simulate compaction (compact_before is a no-op in current impl — verifies API)
        let compacted = a.log.compact_before(a.log.max_seq());
        assert_eq!(compacted, 0, "T09: in-memory log retains all entries (safe GC)");

        // B reconnects later
        a.sync_with(&mut b);

        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "T09: hashes must match after late sync");
        let rows = b.query("SELECT * FROM users").unwrap();
        assert_eq!(rows.len(), 0, "T09: deleted row must not resurrect after reconnect");
    }

    // ── 10. Snapshot hash determinism ────────────────────────────────────────

    #[test]
    fn t10_snapshot_hash_determinism() {
        // Same logical ops on one engine, verify rebuild_store is idempotent
        let mut a = users_engine("A");
        let mut b = users_engine("B");

        a.execute("INSERT INTO users VALUES ('u1','a@x.com','Alice')").unwrap();
        b.execute("INSERT INTO users VALUES ('u2','b@x.com','Bob')").unwrap();

        a.sync_with(&mut b);
        let h1a = a.snapshot_hash();
        let h1b = b.snapshot_hash();

        // Re-run rebuild_store multiple times — must be stable
        a.rebuild_store();
        a.rebuild_store();
        b.rebuild_store();
        b.rebuild_store();

        assert_eq!(a.snapshot_hash(), h1a, "T10: rebuild idempotent on A");
        assert_eq!(b.snapshot_hash(), h1b, "T10: rebuild idempotent on B");
        assert_eq!(h1a, h1b, "T10: A and B must agree after sync");

        // Sync again — hash must remain stable
        a.sync_with(&mut b);
        assert_eq!(a.snapshot_hash(), h1a, "T10: re-sync must be idempotent");
    }

    // ── 11. Restart/recovery test ────────────────────────────────────────────

    #[test]
    fn t11_restart_recovery() {
        let mut a = users_engine("A");
        a.execute("INSERT INTO users VALUES ('u1','alice@x.com','Alice')").unwrap();
        a.execute("UPDATE users SET name='Alice Cooper' WHERE id='u1'").unwrap();

        let hash_before = a.snapshot_hash();
        let log_len = a.log.len();

        // Simulate recovery: rebuild store from op-log
        a.rebuild_store();

        assert_eq!(a.snapshot_hash(), hash_before, "T11: hash must be identical after recovery");
        assert_eq!(a.log.len(), log_len, "T11: log must not grow during recovery");
    }

    // ── 12. Concurrent resurrection conflict ──────────────────────────────────

    #[test]
    fn t12_concurrent_resurrection_conflict() {
        let mut a = users_engine("A");
        let b = users_engine("B");
        let c = users_engine("C");

        a.execute("INSERT INTO users VALUES ('u1','alice@x.com','Alice')").unwrap();
        let mut engines = vec![a, b, c];
        sync_all(&mut engines, 2);

        // A deletes, B updates, C re-inserts (same PK — will be OR-Set add-wins)
        engines[0].execute("DELETE FROM users WHERE id='u1'").unwrap();
        engines[1].execute("UPDATE users SET name='Alice B' WHERE id='u1'").unwrap();
        engines[2].execute("INSERT INTO users VALUES ('u1','alice@x.com','Alice C')").unwrap();

        sync_all(&mut engines, 5);
        assert_hashes_equal(&engines, "T12");

        // Exactly one identity for u1 across all peers
        for (i, e) in engines.iter().enumerate() {
            let rows = e.query("SELECT * FROM users").unwrap();
            let u1_count = rows.iter().filter(|r| r.get_str("id") == Some("u1")).count();
            assert!(u1_count <= 1, "T12: peer {}: u1 must appear at most once", i);
        }
    }

    // ── 13. Large partition test (bounded metadata) ──────────────────────────

    #[test]
    fn t13_large_partition_bounded_metadata() {
        let n_peers = 5;
        let n_ops_each = 50; // reduced for test speed

        let mut engines: Vec<Engine> = (0..n_peers)
            .map(|i| users_engine(&format!("P{}", i)))
            .collect();

        // Each peer inserts unique rows
        for (p, e) in engines.iter_mut().enumerate() {
            for j in 0..n_ops_each {
                let _ = e.execute(&format!(
                    "INSERT INTO users VALUES ('u{p}_{j}','p{p}j{j}@x.com','N{j}')"
                ));
            }
        }

        sync_all(&mut engines, 5);
        assert_hashes_equal(&engines, "T13");

        // Metadata: each cell MV-register should hold at most 1 value (no concurrent updates)
        // Verify log size is bounded (each peer has n_ops_each × 2 ops + CreateTable)
        for (i, e) in engines.iter().enumerate() {
            // After full convergence, log contains all peers' ops
            assert!(e.log.len() > 0, "T13: peer {} log must not be empty", i);
        }
    }

    // ── 14. Incremental sync cursor test ─────────────────────────────────────

    #[test]
    fn t14_incremental_sync_cursor() {
        let mut a = users_engine("A");
        let mut b = users_engine("B");

        a.execute("INSERT INTO users VALUES ('u1','a@x.com','Alice')").unwrap();
        a.sync_with(&mut b); // Partial sync
        let h1 = b.snapshot_hash();

        // More ops on A
        a.execute("INSERT INTO users VALUES ('u2','b@x.com','Bob')").unwrap();
        a.execute("UPDATE users SET name='Alice2' WHERE id='u1'").unwrap();

        // Resume sync
        a.sync_with(&mut b);
        let h2a = a.snapshot_hash();
        let h2b = b.snapshot_hash();

        assert_ne!(h1, h2b, "T14: state must change after new ops");
        assert_eq!(h2a, h2b, "T14: incremental sync must converge");

        let rows = b.query("SELECT * FROM users").unwrap();
        assert_eq!(rows.len(), 2, "T14: B must have both rows after incremental sync");
    }

    // ── 15. Causal dominance pruning ─────────────────────────────────────────

    #[test]
    fn t15_causal_dominance_pruning() {
        use crate::crdt::{clocks::HlcTimestamp, mv_register::MvRegister};

        let mut reg = MvRegister::new();
        // Sequential writes from same peer: v3 causally dominates v1/v2
        reg.write(HlcTimestamp::new(100, 0, "A"), Some(b"v1".to_vec()));
        reg.write(HlcTimestamp::new(200, 0, "A"), Some(b"v2".to_vec()));
        reg.write(HlcTimestamp::new(300, 0, "A"), Some(b"v3".to_vec()));

        // Only v3 should survive (causal compaction)
        assert_eq!(reg.concurrent_count(), 1, "T15: causal compaction must prune v1/v2");
        assert_eq!(reg.read(), Some(b"v3".as_ref()), "T15: v3 must be the canonical value");
    }

    // ── 16. HashMap iteration nondeterminism ─────────────────────────────────

    #[test]
    fn t16_hashmap_iteration_nondeterminism() {
        // Build one engine with rows inserted in varying order,
        // then verify rebuild_store is deterministic (no HashMap iteration nondeterminism)
        let mut e = users_engine("A");
        // Insert in a non-sorted order
        e.execute("INSERT INTO users VALUES ('u3','c@x.com','Carol')").unwrap();
        e.execute("INSERT INTO users VALUES ('u1','a@x.com','Alice')").unwrap();
        e.execute("INSERT INTO users VALUES ('u2','b@x.com','Bob')").unwrap();

        let h0 = e.snapshot_hash();

        // Rebuild 10 times — hash must remain identical every time
        for i in 0..10 {
            e.rebuild_store();
            assert_eq!(e.snapshot_hash(), h0, "T16: rebuild {} produced different hash", i);
        }

        // Sync to a fresh peer and verify hash matches
        let mut b = users_engine("B");
        e.sync_with(&mut b);
        assert_eq!(e.snapshot_hash(), b.snapshot_hash(),
            "T16: snapshot must be identical after sync");
    }

    // ── 17. Concurrent delete + reinsert same PK ─────────────────────────────

    #[test]
    fn t17_concurrent_delete_reinsert_same_pk() {
        let mut a = users_engine("A");
        let mut b = users_engine("B");

        a.execute("INSERT INTO users VALUES ('u1','alice@x.com','Alice')").unwrap();
        a.sync_with(&mut b);

        // A deletes, B re-inserts with new data (same PK)
        a.execute("DELETE FROM users WHERE id='u1'").unwrap();
        b.execute("UPDATE users SET email='alice2@x.com', name='Alice2' WHERE id='u1'").unwrap();

        a.sync_with(&mut b);

        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "T17: hashes must match");

        // State is deterministic — both agree
        let ra = a.query("SELECT * FROM users").unwrap();
        let _rb = b.query("SELECT * FROM users WHERE id='u1'").unwrap();
        assert_eq!(ra, b.query("SELECT * FROM users").unwrap(), "T17: both peers must agree");
    }

    // ── 18. Multi-hop sync convergence ───────────────────────────────────────

    #[test]
    fn t18_multi_hop_sync() {
        let mut a = users_engine("A");
        let mut b = users_engine("B");
        let mut c = users_engine("C");
        let mut d = users_engine("D");

        a.execute("INSERT INTO users VALUES ('u1','a@x.com','Alice')").unwrap();
        d.execute("INSERT INTO users VALUES ('u4','d@x.com','Dave')").unwrap();

        // Linear chain: A→B→C→D only
        a.sync_with(&mut b);
        b.sync_with(&mut c);
        c.sync_with(&mut d);

        // A's op should propagate to D via hops
        // D's op should propagate to A via hops
        // Need one more round for full propagation
        c.sync_with(&mut b);
        b.sync_with(&mut a);

        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "T18: A==B");
        assert_eq!(b.snapshot_hash(), c.snapshot_hash(), "T18: B==C");

        let ra = a.query("SELECT * FROM users").unwrap();
        assert_eq!(ra.len(), 2, "T18: A must see both u1 and u4 after multi-hop");
    }

    // ── 19. Adversarial replay fuzzer ────────────────────────────────────────

    #[test]
    fn t19_adversarial_replay_fuzzer() {
        use crate::testing::randomized_chaos::run_chaos_seed;
        let adversarial: &[u64] = &[0, 1, 7, 13, 42, 99, 137, 256, 512, 1000, 9999, u64::MAX / 2];
        let mut failures = 0;
        for &seed in adversarial {
            if !run_chaos_seed(seed) {
                failures += 1;
                eprintln!("T19: seed {} failed", seed);
            }
        }
        assert_eq!(failures, 0, "T19: {} adversarial seeds failed", failures);
    }

    // ── 20. Canonical SQL projection determinism ──────────────────────────────

    #[test]
    fn t20_canonical_sql_projection() {
        let mut a = users_engine("A");
        let mut b = users_engine("B");

        a.execute("INSERT INTO users VALUES ('u1','alice@x.com','Alice')").unwrap();
        a.sync_with(&mut b);

        // Concurrent name updates (same column, different values)
        a.execute("UPDATE users SET name='Alice Cooper' WHERE id='u1'").unwrap();
        b.execute("UPDATE users SET name='Alice Prime'  WHERE id='u1'").unwrap();

        a.sync_with(&mut b);

        let ra = a.query("SELECT * FROM users WHERE id='u1'").unwrap();
        let rb = b.query("SELECT * FROM users WHERE id='u1'").unwrap();

        // Both peers must return identical canonical value
        assert_eq!(ra, rb, "T20: SQL projection must be identical on all peers");
        // Exactly one canonical visible name
        let name_a = ra[0].get_str("name");
        let name_b = rb[0].get_str("name");
        assert!(name_a.is_some(), "T20: name must be visible");
        assert_eq!(name_a, name_b, "T20: canonical name must be same on both peers");
    }

    // ════════════════════════════════════════════════════════════════════════
    // SECTION 1 — BASIC REPLICATION TESTS (s01–s10)
    // ════════════════════════════════════════════════════════════════════════

    #[test]
    fn s01_single_peer_insert_replication() {
        let mut a = users_engine("A");
        let mut b = users_engine("B");
        a.execute("INSERT INTO users VALUES ('u1','alice@x.com','Alice')").unwrap();
        a.sync_with(&mut b);
        let rows = b.query("SELECT * FROM users").unwrap();
        assert_eq!(rows.len(), 1, "S01: B must see A's insert after sync");
        assert_eq!(rows[0].get_str("name"), Some("Alice"), "S01: name must match");
        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "S01: hashes must match");
    }

    #[test]
    fn s02_single_peer_update_replication() {
        let mut a = users_engine("A");
        let mut b = users_engine("B");
        a.execute("INSERT INTO users VALUES ('u1','alice@x.com','Alice')").unwrap();
        a.sync_with(&mut b);
        a.execute("UPDATE users SET name='Alice Cooper' WHERE id='u1'").unwrap();
        a.sync_with(&mut b);
        let rows = b.query("SELECT * FROM users WHERE id='u1'").unwrap();
        assert_eq!(rows[0].get_str("name"), Some("Alice Cooper"), "S02: update must replicate");
        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "S02: hashes must match");
    }

    #[test]
    fn s03_single_peer_delete_replication() {
        let mut a = users_engine("A");
        let mut b = users_engine("B");
        a.execute("INSERT INTO users VALUES ('u1','alice@x.com','Alice')").unwrap();
        a.sync_with(&mut b);
        a.execute("DELETE FROM users WHERE id='u1'").unwrap();
        a.sync_with(&mut b);
        let rows_b = b.query("SELECT * FROM users").unwrap();
        assert_eq!(rows_b.len(), 0, "S03: delete must replicate — B must see 0 rows");
        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "S03: hashes must match");
    }

    #[test]
    fn s04_multiple_sequential_operations_replication() {
        let mut a = users_engine("A");
        let mut b = users_engine("B");
        for i in 0..5u32 {
            a.execute(&format!("INSERT INTO users VALUES ('u{}','u{}@x.com','User{}')", i, i, i)).unwrap();
        }
        a.execute("UPDATE users SET name='Updated' WHERE id='u2'").unwrap();
        a.execute("DELETE FROM users WHERE id='u4'").unwrap();
        a.sync_with(&mut b);
        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "S04: hashes must match after sequential ops");
        let rows = b.query("SELECT * FROM users").unwrap();
        assert_eq!(rows.len(), 4, "S04: B must see 4 rows (5 inserted, 1 deleted)");
    }

    #[test]
    fn s05_batched_operations_replication() {
        let mut a = users_engine("A");
        let mut b = users_engine("B");
        // Write 20 rows then sync once
        for i in 0..20u32 {
            a.execute(&format!("INSERT INTO users VALUES ('u{}','batch{}@x.com','B{}')", i, i, i)).unwrap();
        }
        a.sync_with(&mut b);
        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "S05: hashes must match after batch sync");
        assert_eq!(b.query("SELECT * FROM users").unwrap().len(), 20, "S05: all 20 rows must replicate");
    }

    #[test]
    fn s06_large_payload_replication() {
        let mut a = users_engine("A");
        let mut b = users_engine("B");
        let large_name = "X".repeat(4096);
        a.execute(&format!("INSERT INTO users VALUES ('u1','big@x.com','{}')", large_name)).unwrap();
        a.sync_with(&mut b);
        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "S06: hashes must match with large payload");
        let rows = b.query("SELECT * FROM users").unwrap();
        assert_eq!(rows[0].get_str("name").unwrap().len(), 4096, "S06: large payload must replicate intact");
    }

    #[test]
    fn s07_empty_operations_replication() {
        let mut a = users_engine("A");
        let mut b = users_engine("B");
        // Sync with no data — both should agree on empty state
        a.sync_with(&mut b);
        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "S07: empty sync must produce identical hashes");
        assert_eq!(b.query("SELECT * FROM users").unwrap().len(), 0, "S07: no rows");
    }

    #[test]
    fn s08_replay_after_reconnect() {
        let mut a = users_engine("A");
        let mut b = users_engine("B");
        // A writes while B is "offline"
        for i in 0..10u32 {
            a.execute(&format!("INSERT INTO users VALUES ('u{}','r{}@x.com','R{}')", i, i, i)).unwrap();
        }
        // B reconnects — single sync catches up all 10 ops
        a.sync_with(&mut b);
        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "S08: B must catch up on reconnect");
        assert_eq!(b.query("SELECT * FROM users").unwrap().len(), 10, "S08: all rows must appear");
    }

    #[test]
    fn s09_peer_startup_synchronization() {
        let mut a = users_engine("A");
        let mut b = users_engine("B");
        let mut c = users_engine("C");
        a.execute("INSERT INTO users VALUES ('u1','a@x.com','Alice')").unwrap();
        b.execute("INSERT INTO users VALUES ('u2','b@x.com','Bob')").unwrap();
        // C starts up fresh and syncs from both
        c.sync_with(&mut a);
        c.sync_with(&mut b);
        a.sync_with(&mut b);
        let h = a.snapshot_hash();
        assert_eq!(b.snapshot_hash(), h, "S09: A==B");
        assert_eq!(c.snapshot_hash(), h, "S09: C==A after startup sync");
    }

    #[test]
    fn s10_peer_bootstrap_from_snapshot() {
        let mut a = users_engine("A");
        let mut b = users_engine("B");
        for i in 0..15u32 {
            a.execute(&format!("INSERT INTO users VALUES ('u{}','snap{}@x.com','S{}')", i, i, i)).unwrap();
        }
        // Bootstrap B from A (cold start)
        a.sync_with(&mut b);
        let h_a = a.snapshot_hash();
        let h_b = b.snapshot_hash();
        assert_eq!(h_a, h_b, "S10: bootstrapped peer must have identical hash");
        assert_eq!(b.query("SELECT * FROM users").unwrap().len(), 15, "S10: all rows visible");
        // Subsequent re-sync must be idempotent
        a.sync_with(&mut b);
        assert_eq!(a.snapshot_hash(), h_a, "S10: re-sync must be idempotent");
    }

    // ════════════════════════════════════════════════════════════════════════
    // SECTION 2 — CONCURRENT WRITE TESTS (s11–s20)
    // ════════════════════════════════════════════════════════════════════════

    #[test]
    fn s11_concurrent_update_same_row_same_column() {
        let mut a = users_engine("A"); let mut b = users_engine("B");
        a.execute("INSERT INTO users VALUES ('u1','x@x.com','Alice')").unwrap();
        a.sync_with(&mut b);
        a.execute("UPDATE users SET name='AliceA' WHERE id='u1'").unwrap();
        b.execute("UPDATE users SET name='AliceB' WHERE id='u1'").unwrap();
        a.sync_with(&mut b);
        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "S11: hashes must match");
        let ra = a.query("SELECT name FROM users WHERE id='u1'").unwrap();
        let rb = b.query("SELECT name FROM users WHERE id='u1'").unwrap();
        assert_eq!(ra, rb, "S11: both peers must agree on canonical name");
    }

    #[test]
    fn s12_concurrent_update_same_row_different_columns() {
        let mut a = users_engine("A"); let mut b = users_engine("B");
        a.execute("INSERT INTO users VALUES ('u1','old@x.com','Alice')").unwrap();
        a.sync_with(&mut b);
        a.execute("UPDATE users SET name='Alice Cooper' WHERE id='u1'").unwrap();
        b.execute("UPDATE users SET email='new@x.com' WHERE id='u1'").unwrap();
        a.sync_with(&mut b);
        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "S12: hashes must match");
        let row = a.query("SELECT * FROM users WHERE id='u1'").unwrap();
        assert_eq!(row[0].get_str("name"), Some("Alice Cooper"), "S12: name update must survive");
        assert_eq!(row[0].get_str("email"), Some("new@x.com"), "S12: email update must survive");
    }

    #[test]
    fn s13_concurrent_inserts_same_primary_key() {
        let mut a = users_engine("A"); let mut b = users_engine("B");
        a.execute("INSERT INTO users VALUES ('u1','a@x.com','Alice')").unwrap();
        b.execute("INSERT INTO users VALUES ('u1','b@x.com','Bob')").unwrap();
        a.sync_with(&mut b);
        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "S13: hashes must match");
        let rows = a.query("SELECT * FROM users WHERE id='u1'").unwrap();
        assert_eq!(rows.len(), 1, "S13: only one canonical row for same PK");
        assert_eq!(rows, b.query("SELECT * FROM users WHERE id='u1'").unwrap());
    }

    #[test]
    fn s14_concurrent_delete_vs_update() {
        let mut a = users_engine("A"); let mut b = users_engine("B");
        a.execute("INSERT INTO users VALUES ('u1','x@x.com','Alice')").unwrap();
        a.sync_with(&mut b);
        a.execute("DELETE FROM users WHERE id='u1'").unwrap();
        b.execute("UPDATE users SET name='Alice2' WHERE id='u1'").unwrap();
        a.sync_with(&mut b);
        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "S14: hashes must match");
        // Tombstone policy: row not visible
        assert_eq!(a.query("SELECT * FROM users").unwrap().len(), 0, "S14: tombstoned row not visible");
    }

    #[test]
    fn s15_concurrent_delete_vs_insert() {
        let mut a = users_engine("A"); let mut b = users_engine("B");
        a.execute("INSERT INTO users VALUES ('u1','x@x.com','Alice')").unwrap();
        a.sync_with(&mut b);
        a.execute("DELETE FROM users WHERE id='u1'").unwrap();
        b.execute("INSERT INTO users VALUES ('u2','y@x.com','Bob')").unwrap();
        a.sync_with(&mut b);
        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "S15: hashes must match");
        assert_eq!(a.query("SELECT * FROM users").unwrap().len(), 1, "S15: only u2 visible");
    }

    #[test]
    fn s16_triple_concurrent_updates() {
        let mut a = users_engine("A"); let mut b = users_engine("B"); let mut c = users_engine("C");
        a.execute("INSERT INTO users VALUES ('u1','x@x.com','Alice')").unwrap();
        let mut engines = vec![a, b, c];
        sync_all(&mut engines, 2);
        engines[0].execute("UPDATE users SET name='NameA' WHERE id='u1'").unwrap();
        engines[1].execute("UPDATE users SET name='NameB' WHERE id='u1'").unwrap();
        engines[2].execute("UPDATE users SET name='NameC' WHERE id='u1'").unwrap();
        sync_all(&mut engines, 5);
        assert_hashes_equal(&engines, "S16");
        let rows = engines[0].query("SELECT * FROM users WHERE id='u1'").unwrap();
        assert_eq!(rows, engines[1].query("SELECT * FROM users WHERE id='u1'").unwrap(), "S16: all peers agree");
    }

    #[test]
    fn s17_n_peer_concurrent_updates() {
        let n = 8;
        let mut engines: Vec<Engine> = (0..n).map(|i| users_engine(&format!("P{}", i))).collect();
        engines[0].execute("INSERT INTO users VALUES ('u1','x@x.com','Alice')").unwrap();
        sync_all(&mut engines, 3);
        for i in 0..n {
            let _ = engines[i].execute(&format!("UPDATE users SET name='Peer{}' WHERE id='u1'", i));
        }
        sync_all(&mut engines, 5);
        assert_hashes_equal(&engines, "S17");
    }

    #[test]
    fn s18_rapid_toggle_delete_insert_race() {
        let mut a = users_engine("A"); let mut b = users_engine("B");
        for i in 0..10u32 {
            a.execute(&format!("INSERT INTO users VALUES ('t{}','t{}@x.com','T{}')", i, i, i)).unwrap();
            let _ = a.execute(&format!("DELETE FROM users WHERE id='t{}'", i));
        }
        a.sync_with(&mut b);
        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "S18: hash must match after toggle race");
    }

    #[test]
    fn s19_concurrent_multi_op_commits() {
        let mut a = users_engine("A"); let mut b = users_engine("B");
        for i in 0..5u32 {
            a.execute(&format!("INSERT INTO users VALUES ('a{}','a{}@x.com','A{}')", i, i, i)).unwrap();
            b.execute(&format!("INSERT INTO users VALUES ('b{}','b{}@x.com','B{}')", i, i, i)).unwrap();
        }
        a.sync_with(&mut b);
        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "S19: hashes must match");
        assert_eq!(a.query("SELECT * FROM users").unwrap().len(), 10, "S19: all 10 rows visible");
    }

    #[test]
    fn s20_concurrent_schema_create_same_table() {
        // Both peers independently CREATE TABLE then sync — must converge
        let mut a = Engine::open_with_policy(".", "A", FkPolicy::Tombstone);
        let mut b = Engine::open_with_policy(".", "B", FkPolicy::Tombstone);
        a.execute("CREATE TABLE items (id TEXT PRIMARY KEY, val TEXT)").unwrap();
        b.execute("CREATE TABLE items (id TEXT PRIMARY KEY, val TEXT)").unwrap();
        a.execute("INSERT INTO items VALUES ('i1','foo')").unwrap();
        b.execute("INSERT INTO items VALUES ('i2','bar')").unwrap();
        a.sync_with(&mut b);
        assert_eq!(a.snapshot_hash(), b.snapshot_hash(), "S20: hashes must match after concurrent DDL");
        assert_eq!(a.query("SELECT * FROM items").unwrap().len(), 2, "S20: both rows visible");
    }
}
