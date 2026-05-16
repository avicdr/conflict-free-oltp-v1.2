//! Deterministic canonical snapshot serialization and BLAKE3 hashing.
//!
//! ## Canonical Serialization Rules
//! 1. Tables serialized in lexicographic name order
//! 2. Rows serialized in primary-key order
//! 3. Columns serialized in schema-defined order
//! 4. Tombstoned rows INCLUDED with a tombstone marker
//! 5. CRDT metadata INCLUDED (all MV-Register entries, not just canonical value)
//! 6. Nulls encoded as a distinct 0x00 byte prefix
//! 7. Values encoded as 0x01 prefix + little-endian bincode bytes
//! 8. Integers: always 8-byte little-endian i64
//! 9. Floats: IEEE 754 bits reinterpreted as u64 (NaN canonicalized to 0x7FF8000000000000)
//!
//! These rules guarantee bit-identical hashes across:
//! - merge orders
//! - sync orders
//! - operating systems
//! - architectures (via explicit endianness)

use crate::storage::row_store::RowStore;
use crate::crdt::mv_register::MvRegister;

/// Compute a deterministic BLAKE3 snapshot hash of the entire database state.
///
/// The hash covers: all tables, all rows (including tombstoned), all cell MV-entries.
pub fn compute_snapshot_hash(store: &RowStore) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();

    // Tables in lexicographic order
    for (table_name, table_state) in &store.tables {
        // Table separator
        hasher.update(b"T:");
        hasher.update(table_name.as_bytes());
        hasher.update(b"|");

        // All rows (including tombstoned) in primary-key order
        let mut rows: Vec<_> = table_state.rows.iter().collect();
        rows.sort_by_key(|(id, _)| id.as_str());

        for (row_id, row) in &rows {
            // Row membership state
            let is_tombstoned = table_state.membership.rows.get(*row_id)
                .map(|e| e.is_tombstoned())
                .unwrap_or(false);
            let is_live = table_state.membership.is_visible(row_id);

            hasher.update(b"R:");
            hasher.update(row_id.as_bytes());
            hasher.update(if is_tombstoned { b"T" } else if is_live { b"L" } else { b"D" });
            hasher.update(b"|");

            // Columns in schema-defined order
            for col_def in &table_state.schema.columns {
                hasher.update(b"C:");
                hasher.update(col_def.name.as_bytes());
                hasher.update(b":");

                if let Some(reg) = row.cells.get(&col_def.name) {
                    hash_mv_register(&mut hasher, reg);
                } else {
                    hasher.update(b"EMPTY");
                }
                hasher.update(b"|");
            }
        }
    }

    *hasher.finalize().as_bytes()
}

/// Hash all entries in an MV-Register deterministically.
/// We hash ALL entries (not just canonical) to capture the full CRDT state.
fn hash_mv_register(hasher: &mut blake3::Hasher, reg: &MvRegister) {
    // Entries are already in BTreeMap order: deterministic
    for ((wall, logical, peer), entry) in reg.entries_snapshot() {
        hasher.update(b"E:");
        hasher.update(&wall.to_le_bytes());
        hasher.update(&logical.to_le_bytes());
        hasher.update(peer.as_bytes());
        hasher.update(b":");

        match &entry.value {
            None => { hasher.update(b"\x00"); }  // NULL
            Some(v) => {
                hasher.update(b"\x01");            // Non-null
                hasher.update(&(v.len() as u64).to_le_bytes());
                hasher.update(v);
            }
        }
        hasher.update(b"|");
    }
}

/// A snapshot record for transmission/comparison.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub hash: [u8; 32],
    pub hex: String,
}

impl Snapshot {
    pub fn compute(store: &RowStore) -> Self {
        let hash = compute_snapshot_hash(store);
        let hex = hex::encode(hash);
        Self { hash, hex }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::{clocks::HlcTimestamp, merge::{TableSchema, ColumnDef, ColumnType}};
    use crate::storage::row_store::RowStore;

    fn make_schema() -> TableSchema {
        TableSchema {
            name: "users".into(),
            columns: vec![
                ColumnDef { name: "id".into(), col_type: ColumnType::Text, not_null: true, default_value: None, primary_key: true, unique: false },
                ColumnDef { name: "name".into(), col_type: ColumnType::Text, not_null: false, default_value: None, primary_key: false, unique: false },
            ],
            foreign_keys: vec![],
            indexes: vec![],
            composite_unique_constraints: vec![],
        }
    }

    fn ts(wall: u64, peer: &str) -> HlcTimestamp { HlcTimestamp::new(wall, 0, peer) }
    fn cell(col: &str, val: &str) -> (String, Option<Vec<u8>>) {
        (col.to_string(), Some(val.as_bytes().to_vec()))
    }

    #[test]
    fn hash_stable_after_idempotent_merge() {
        let mut store = RowStore::new();
        store.create_table(make_schema());
        let t = store.tables.get_mut("users").unwrap();
        t.insert_row("u1", &ts(100, "A"), vec![cell("name", "Alice")]);

        let h1 = compute_snapshot_hash(&store);

        // Merging with self should not change hash
        let copy = store.clone();
        store.merge(&copy);

        let h2 = compute_snapshot_hash(&store);
        assert_eq!(h1, h2, "Idempotent merge must not change snapshot hash");
    }

    #[test]
    fn hash_commutative_merge() {
        let mut a = RowStore::new();
        let mut b = RowStore::new();
        a.create_table(make_schema());
        b.create_table(make_schema());

        a.tables.get_mut("users").unwrap()
            .insert_row("u1", &ts(100, "A"), vec![cell("name", "Alice")]);
        b.tables.get_mut("users").unwrap()
            .insert_row("u2", &ts(101, "B"), vec![cell("name", "Bob")]);

        let mut ab = a.clone(); ab.merge(&b);
        let mut ba = b.clone(); ba.merge(&a);

        assert_eq!(
            compute_snapshot_hash(&ab),
            compute_snapshot_hash(&ba),
            "Commutative merge must yield identical snapshot hash"
        );
    }
}
