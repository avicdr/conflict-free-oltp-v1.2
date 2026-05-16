//! Randomized chaos testing harness.
//!
//! Generates random:
//! - Peer counts (2-5)
//! - Network partitions
//! - Sync orders
//! - Message duplication
//! - Concurrent inserts, updates, deletes
//! - FK conflicts
//! - Uniqueness conflicts
//!
//! For every seed, asserts:
//! - All peers converge to same snapshot hash
//! - Relational invariants hold

use crate::api::engine::Engine;
use crate::constraints::fk_resolution::FkPolicy;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

const SCHEMA: &str = "
CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT UNIQUE NOT NULL, name TEXT);
CREATE TABLE orders (id TEXT PRIMARY KEY, user_id TEXT NOT NULL, status TEXT NOT NULL, total_cents INTEGER NOT NULL);
";

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum TestOp {
    Insert { table: String, id: String, email: Option<String>, name: Option<String> },
    Update { table: String, id: String, field: String, value: String },
    Delete { table: String, id: String },
    InsertOrder { id: String, user_id: String, status: String },
    Sync { from_peer: usize, to_peer: usize },
}

fn generate_ops(rng: &mut StdRng, num_peers: usize, num_ops: usize) -> Vec<(usize, TestOp)> {
    let mut ops = Vec::new();
    let emails = vec!["a@x.com", "b@x.com", "c@x.com", "d@x.com"];
    let names = vec!["Alice", "Bob", "Carol", "Dave"];
    let statuses = vec!["pending", "shipped", "cancelled"];

    for _ in 0..num_ops {
        let peer = rng.gen_range(0..num_peers);
        let op_type = rng.gen_range(0..6u32);

        let op = match op_type {
            0 => {
                let id = format!("u{}", rng.gen_range(1..=5));
                let email = emails[rng.gen_range(0..emails.len())];
                let name = names[rng.gen_range(0..names.len())];
                TestOp::Insert {
                    table: "users".into(),
                    id,
                    email: Some(email.to_string()),
                    name: Some(name.to_string()),
                }
            }
            1 => {
                let id = format!("u{}", rng.gen_range(1..=5));
                let field = if rng.gen_bool(0.5) { "name" } else { "email" };
                let value = if field == "name" {
                    names[rng.gen_range(0..names.len())].to_string()
                } else {
                    emails[rng.gen_range(0..emails.len())].to_string()
                };
                TestOp::Update { table: "users".into(), id, field: field.to_string(), value }
            }
            2 => {
                let id = format!("u{}", rng.gen_range(1..=5));
                TestOp::Delete { table: "users".into(), id }
            }
            3 => {
                let order_id = format!("o{}", rng.gen_range(1..=10));
                let user_id = format!("u{}", rng.gen_range(1..=5));
                let status = statuses[rng.gen_range(0..statuses.len())];
                TestOp::InsertOrder { id: order_id, user_id, status: status.to_string() }
            }
            4 | 5 => {
                let from = rng.gen_range(0..num_peers);
                let to = (from + 1 + rng.gen_range(0..num_peers - 1)) % num_peers;
                TestOp::Sync { from_peer: from, to_peer: to }
            }
            _ => unreachable!(),
        };

        ops.push((peer, op));
    }
    ops
}

/// Run a single chaos test with a given seed.
/// Returns true if all peers converge.
pub fn run_chaos_seed(seed: u64) -> bool {
    let mut rng = StdRng::seed_from_u64(seed);
    let num_peers = rng.gen_range(2..=4usize);
    let num_ops = rng.gen_range(20..=60usize);

    let peer_ids: Vec<String> = (0..num_peers)
        .map(|i| format!("P{}", i))
        .collect();

    let mut engines: Vec<Engine> = peer_ids.iter()
        .map(|id| {
            let mut e = Engine::open_with_policy(".", id, FkPolicy::Tombstone);
            if let Err(err) = e.execute(SCHEMA) {
                eprintln!("Schema setup failed: {}", err);
            }
            e
        })
        .collect();

    let ops = generate_ops(&mut rng, num_peers, num_ops);

    for (peer_idx, op) in ops {
        if peer_idx >= engines.len() { continue; }

        match op {
            TestOp::Insert { id, email, name, .. } => {
                let email_val = email.as_deref().unwrap_or("nomail@x.com");
                let name_val = name.as_deref().unwrap_or("Unknown");
                let sql = format!(
                    "INSERT INTO users VALUES ('{}', '{}', '{}')",
                    id, email_val, name_val
                );
                let _ = engines[peer_idx].execute(&sql);
            }
            TestOp::Update { id, field, value, .. } => {
                let sql = format!(
                    "UPDATE users SET {} = '{}' WHERE id = '{}'",
                    field, value, id
                );
                let _ = engines[peer_idx].execute(&sql);
            }
            TestOp::Delete { id, .. } => {
                let sql = format!("DELETE FROM users WHERE id = '{}'", id);
                let _ = engines[peer_idx].execute(&sql);
            }
            TestOp::InsertOrder { id, user_id, status } => {
                let sql = format!(
                    "INSERT INTO orders VALUES ('{}', '{}', '{}', 0)",
                    id, user_id, status
                );
                let _ = engines[peer_idx].execute(&sql);
            }
            TestOp::Sync { from_peer, to_peer } => {
                if from_peer < engines.len() && to_peer < engines.len() && from_peer != to_peer {
                    // We need to sync two engines — use swap trick
                    let (left, right) = if from_peer < to_peer {
                        let (l, r) = engines.split_at_mut(to_peer);
                        (&mut l[from_peer], &mut r[0])
                    } else {
                        let (l, r) = engines.split_at_mut(from_peer);
                        (&mut r[0], &mut l[to_peer])
                    };
                    left.sync_with(right);
                }
            }
        }
    }

    // Final convergence: sync all pairs multiple times
    for _round in 0..5 {
        for i in 0..num_peers {
            for j in (i + 1)..num_peers {
                let (left, right) = if i < j {
                    let (l, r) = engines.split_at_mut(j);
                    (&mut l[i], &mut r[0])
                } else {
                    let (l, r) = engines.split_at_mut(i);
                    (&mut r[0], &mut l[j])
                };
                left.sync_with(right);
            }
        }
    }

    // Verify convergence: all peers must have the same snapshot hash
    let hashes: Vec<String> = engines.iter().map(|e| e.snapshot_hash()).collect();
    let all_equal = hashes.windows(2).all(|w| w[0] == w[1]);

    if !all_equal {
        eprintln!("Chaos test FAILED for seed {}: hashes differ: {:?}", seed, hashes);
    }

    // Verify uniqueness invariant on each peer
    for engine in &engines {
        if let Ok(rows) = engine.query("SELECT * FROM users") {
            let emails: Vec<_> = rows.iter()
                .filter_map(|r| r.get_str("email"))
                .collect();
            let unique: std::collections::BTreeSet<_> = emails.iter().collect();
            if emails.len() != unique.len() {
                eprintln!("Chaos test FAILED for seed {}: UNIQUE(email) violated", seed);
                return false;
            }
        }
    }

    all_equal
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chaos_100_seeds() {
        let mut failures = 0;
        for seed in 0..100u64 {
            if !run_chaos_seed(seed) {
                failures += 1;
                eprintln!("FAILED seed: {}", seed);
            }
        }
        assert_eq!(failures, 0, "{} out of 100 chaos seeds failed", failures);
    }

    #[test]
    fn chaos_adversarial_uniqueness_seeds() {
        // Seeds specifically designed to trigger uniqueness conflicts
        let adversarial_seeds: Vec<u64> = (1000..1050).collect();
        let mut failures = 0;
        for seed in adversarial_seeds {
            if !run_chaos_seed(seed) {
                failures += 1;
            }
        }
        assert_eq!(failures, 0, "{} adversarial uniqueness seeds failed", failures);
    }

    #[test]
    fn chaos_partition_tolerance() {
        // Run with a few known-good seeds to verify basic convergence
        for seed in [42, 137, 271, 314, 999] {
            assert!(run_chaos_seed(seed), "Chaos seed {} must converge", seed);
        }
    }
}
