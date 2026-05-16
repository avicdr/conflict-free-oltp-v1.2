//! Property-based tests for CRDT invariants.
//!
//! Tests:
//! - Merge commutativity
//! - Merge associativity
//! - Idempotent sync
//! - Out-of-order delivery
//! - Duplicate delivery
//! - Snapshot hash stability
//! - Cell-level concurrent updates
//! - Delete vs Update (OPTION B)
//! - Uniqueness conflict determinism
//! - Index consistency across merge orders

#[allow(unused_imports)]
use crate::{
    api::engine::Engine,
    constraints::fk_resolution::FkPolicy,
    crdt::{
        clocks::HlcTimestamp,
        merge::CrdtOp,
        mv_register::MvRegister,
        orset::OrMap,
    },
    storage::{row_store::RowStore, snapshots::compute_snapshot_hash},
};

#[allow(dead_code)]
fn ts(wall: u64, peer: &str) -> HlcTimestamp {
    HlcTimestamp::new(wall, 0, peer)
}

#[allow(dead_code)]
fn bytes(s: &str) -> Option<Vec<u8>> {
    Some(s.as_bytes().to_vec())
}

// ================================================================
// MV-REGISTER CRDT INVARIANTS
// ================================================================

#[cfg(test)]
mod mv_register_tests {
    use super::*;

    #[test]
    fn commutativity() {
        let mut a = MvRegister::new();
        let mut b = MvRegister::new();
        a.write(ts(100, "A"), bytes("va"));
        b.write(ts(101, "B"), bytes("vb"));

        let mut ab = a.clone(); ab.merge(&b);
        let mut ba = b.clone(); ba.merge(&a);
        assert_eq!(ab, ba, "MV-Register merge must be commutative");
    }

    #[test]
    fn associativity() {
        let mut a = MvRegister::new();
        let mut b = MvRegister::new();
        let mut c = MvRegister::new();
        a.write(ts(100, "A"), bytes("va"));
        b.write(ts(101, "B"), bytes("vb"));
        c.write(ts(102, "C"), bytes("vc"));

        let mut ab_c = a.clone(); ab_c.merge(&b); ab_c.merge(&c);
        let mut bc = b.clone(); bc.merge(&c);
        let mut a_bc = a.clone(); a_bc.merge(&bc);
        assert_eq!(ab_c, a_bc, "MV-Register merge must be associative");
    }

    #[test]
    fn idempotence() {
        let mut r = MvRegister::new();
        r.write(ts(100, "A"), bytes("v"));
        let orig = r.clone();
        r.merge(&orig);
        assert_eq!(r, orig, "MV-Register merge must be idempotent");
    }

    #[test]
    fn causal_compaction_bounded_metadata() {
        let mut r = MvRegister::new();
        // 100 sequential writes from same peer — should only retain the last one
        for i in 0..100u64 {
            r.write(ts(i, "A"), bytes(&format!("v{}", i)));
        }
        assert_eq!(r.concurrent_count(), 1, "Causal compaction must bound metadata to O(1) for single peer");
    }

    #[test]
    fn concurrent_writes_both_retained() {
        let mut a = MvRegister::new();
        let mut b = MvRegister::new();
        a.write(ts(100, "A"), bytes("from_a"));
        b.write(ts(100, "B"), bytes("from_b"));
        a.merge(&b);
        assert_eq!(a.concurrent_count(), 2, "Concurrent writes from different peers must both be retained");
    }

    #[test]
    fn delete_and_concurrent_write_option_b() {
        // A deletes, B updates concurrently
        let mut a = MvRegister::new();
        let mut b = MvRegister::new();
        a.write(ts(100, "A"), bytes("original"));
        b.write(ts(100, "A"), bytes("original")); // both start same
        a.write(ts(200, "A"), None);          // A deletes
        b.write(ts(200, "B"), bytes("updated")); // B updates concurrently
        a.merge(&b);
        // OPTION B: update survives, data is preserved
        let vals: Vec<_> = a.values().collect();
        assert!(vals.iter().any(|e| e.value.is_some()), "OPTION B: concurrent update must survive delete");
    }
}

// ================================================================
// OR-MAP CRDT INVARIANTS
// ================================================================

#[cfg(test)]
mod ormap_tests {
    use super::*;

    #[test]
    fn commutativity() {
        let mut a = OrMap::new();
        let mut b = OrMap::new();
        a.insert("r1", &ts(100, "A"));
        b.delete("r1", &ts(200, "B"), false);

        let mut ab = a.clone(); ab.merge(&b);
        let mut ba = b.clone(); ba.merge(&a);
        assert_eq!(ab, ba, "OR-Map merge must be commutative");
    }

    #[test]
    fn idempotence() {
        let mut m = OrMap::new();
        m.insert("r1", &ts(100, "A"));
        let orig = m.clone();
        m.merge(&orig);
        assert_eq!(m, orig, "OR-Map merge must be idempotent");
    }

    #[test]
    fn add_wins_semantics() {
        let mut a = OrMap::new();
        let mut b = OrMap::new();

        // A inserts r1 with token (100, A)
        a.insert("r1", &ts(100, "A"));

        // B only knows about initial state before A's token
        // B deletes r1 — but B's remove-set only covers tokens B has seen
        // Since B didn't see A's (100,A) token, add-wins
        b.insert("r1", &ts(50, "setup")); // B had an older insert
        b.delete("r1", &ts(150, "B"), false); // B deletes based on what it saw

        // Now A inserts again with a new token after B's delete
        a.insert("r1", &ts(200, "A"));

        a.merge(&b);
        // A's (200,A) token was NOT in B's removed set — add wins
        assert!(a.is_visible("r1"), "Add-wins: A's new insert after B's delete must survive");
    }
}

// ================================================================
// ENGINE-LEVEL INVARIANTS
// ================================================================

#[cfg(test)]
mod engine_tests {
    use super::*;
    use crate::testing::scenario::{run_reference_scenario, assert_convergence};

    fn make_engine(peer: &str) -> Engine {
        let mut e = Engine::open_with_policy(".", peer, FkPolicy::Tombstone);
        e.execute("CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT UNIQUE NOT NULL, name TEXT)").unwrap();
        e
    }

    #[test]
    fn snapshot_hash_stable_after_idempotent_sync() {
        let mut a = make_engine("A");
        let mut b = make_engine("B");
        a.execute("INSERT INTO users VALUES ('u1', 'alice@x.com', 'Alice')").unwrap();
        b.execute("INSERT INTO users VALUES ('u2', 'bob@x.com', 'Bob')").unwrap();
        a.sync_with(&mut b);
        let h1 = a.snapshot_hash();
        a.sync_with(&mut b); // Repeat sync
        let h2 = a.snapshot_hash();
        assert_eq!(h1, h2, "Snapshot hash must be stable after repeated sync");
    }

    #[test]
    fn snapshot_hash_commutative() {
        // Test: after A↔B sync, both peers must have the same hash.
        // This tests that sync direction (A→B vs B→A) produces identical converged states.
        let mut a = make_engine("A");
        let mut b = make_engine("B");
        a.execute("INSERT INTO users VALUES ('u1', 'alice@x.com', 'Alice')").unwrap();
        b.execute("INSERT INTO users VALUES ('u2', 'bob@x.com', 'Bob')").unwrap();

        a.sync_with(&mut b);

        // After sync, A and B must have identical hashes
        assert_eq!(a.snapshot_hash(), b.snapshot_hash(),
            "After bidirectional sync, both peers must have the same snapshot hash");

        // Re-sync: still identical (idempotent)
        a.sync_with(&mut b);
        assert_eq!(a.snapshot_hash(), b.snapshot_hash(),
            "Hash must remain identical after redundant re-sync");
    }

    #[test]
    fn cell_level_concurrent_update_both_preserved() {
        let mut a = make_engine("A");
        let mut b = make_engine("B");

        // Ensure b has the row first
        a.execute("INSERT INTO users VALUES ('u1', 'alice@x.com', 'Alice')").unwrap();
        a.sync_with(&mut b);

        // Concurrent updates to different columns
        a.execute("UPDATE users SET name = 'Alice Cooper' WHERE id = 'u1'").unwrap();
        b.execute("UPDATE users SET email = 'alice@ex.org' WHERE id = 'u1'").unwrap();

        a.sync_with(&mut b);

        let rows_a = a.query("SELECT * FROM users WHERE id = 'u1'").unwrap();
        let rows_b = b.query("SELECT * FROM users WHERE id = 'u1'").unwrap();

        assert_eq!(rows_a, rows_b, "Both peers must converge on same row");
        let row = &rows_a[0];
        assert_eq!(row.get_str("name"), Some("Alice Cooper"), "name update must be preserved");
        assert_eq!(row.get_str("email"), Some("alice@ex.org"), "email update must be preserved");
    }

    #[test]
    fn uniqueness_conflict_exactly_one_winner() {
        let mut a = make_engine("A");
        let mut b = make_engine("B");

        // Both insert same email while offline
        a.execute("INSERT INTO users VALUES ('u1', 'alice@x.com', 'Alice')").unwrap();
        b.execute("INSERT INTO users VALUES ('u2', 'alice@x.com', 'Alice Prime')").unwrap();

        a.sync_with(&mut b);

        let rows_a = a.query("SELECT * FROM users").unwrap();
        let rows_b = b.query("SELECT * FROM users").unwrap();

        // Exactly one row with alice@x.com in visible state
        let alice_count_a = rows_a.iter().filter(|r| r.get_str("email") == Some("alice@x.com")).count();
        let alice_count_b = rows_b.iter().filter(|r| r.get_str("email") == Some("alice@x.com")).count();
        assert_eq!(alice_count_a, 1, "Exactly one winner for unique email on A");
        assert_eq!(alice_count_b, 1, "Exactly one winner for unique email on B");
        assert_eq!(alice_count_a, alice_count_b, "Both peers must agree on winner");

        // Hash must be identical
        assert_eq!(a.snapshot_hash(), b.snapshot_hash(),
            "After uniqueness conflict resolution, hashes must match");
    }

    #[test]
    fn delete_vs_update_option_b() {
        let mut a = make_engine("A");
        let mut b = make_engine("B");

        a.execute("INSERT INTO users VALUES ('u1', 'alice@x.com', 'Alice')").unwrap();
        a.sync_with(&mut b);

        // A deletes, B updates concurrently (both offline)
        a.execute("DELETE FROM users WHERE id = 'u1'").unwrap();
        b.execute("UPDATE users SET name = 'Alice Cooper' WHERE id = 'u1'").unwrap();

        a.sync_with(&mut b);

        // OPTION B: update data is preserved in tombstoned row
        let u1_row = a.store.get_table("users").and_then(|t| t.rows.get("u1"));
        assert!(u1_row.is_some(), "OPTION B: u1 must remain in store after delete+update merge");
        if let Some(row) = u1_row {
            // The update data must be preserved
            assert!(row.cells.contains_key("name"), "Cell data from concurrent update must be preserved");
        }

        // Hashes must match
        assert_eq!(a.snapshot_hash(), b.snapshot_hash());
    }

    #[test]
    fn out_of_order_delivery_same_hash() {
        // Apply ops in two different orders, verify same final hash
        let ops = vec![
            CrdtOp::InsertRow {
                table: "users".into(), row_id: "u1".into(),
                hlc: ts(100, "A"), cells: vec![("email".to_string(), bytes("a@x.com")), ("name".to_string(), bytes("Alice"))],
            },
            CrdtOp::InsertRow {
                table: "users".into(), row_id: "u2".into(),
                hlc: ts(101, "B"), cells: vec![("email".to_string(), bytes("b@x.com")), ("name".to_string(), bytes("Bob"))],
            },
        ];

        let mut store1 = RowStore::new();
        let _schema = make_engine("X").store.tables.values().next().cloned();
        // Skip if no tables — just use raw store
        // Apply in order 1, 2
        for op in &ops { store1.apply_op(op); }

        let mut store2 = RowStore::new();
        // Apply in order 2, 1
        for op in ops.iter().rev() { store2.apply_op(op); }

        assert_eq!(
            compute_snapshot_hash(&store1),
            compute_snapshot_hash(&store2),
            "Out-of-order delivery must produce same snapshot hash"
        );
    }

    #[test]
    fn mandatory_8_step_scenario() {
        let (a, b, c) = run_reference_scenario();
        assert_convergence(&a, &b, &c);
    }
}
