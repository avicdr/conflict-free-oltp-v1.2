//! Benchmark harness for CRDTdb engine operations.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use crdtdb::{Engine, FkPolicy};

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");

    for row_count in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(row_count as u64));
        group.bench_with_input(BenchmarkId::new("sequential", row_count), &row_count, |b, &n| {
            b.iter(|| {
                let mut engine = Engine::open_with_policy(".", "bench", FkPolicy::Tombstone);
                engine.execute("CREATE TABLE t (id TEXT PRIMARY KEY, val TEXT)").unwrap();
                for i in 0..n {
                    engine.execute(&format!("INSERT INTO t VALUES ('r{}', 'v{}')", i, i)).unwrap();
                }
                engine.snapshot_hash()
            });
        });
    }
    group.finish();
}

fn bench_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync");

    for row_count in [100, 1_000] {
        group.throughput(Throughput::Elements(row_count as u64));
        group.bench_with_input(BenchmarkId::new("bidirectional", row_count), &row_count, |b, &n| {
            b.iter(|| {
                let mut a = Engine::open_with_policy(".", "A", FkPolicy::Tombstone);
                let mut b_engine = Engine::open_with_policy(".", "B", FkPolicy::Tombstone);
                let schema = "CREATE TABLE t (id TEXT PRIMARY KEY, val TEXT)";
                a.execute(schema).unwrap();
                b_engine.execute(schema).unwrap();

                for i in 0..n {
                    a.execute(&format!("INSERT INTO t VALUES ('a{}', 'va{}')", i, i)).unwrap();
                    b_engine.execute(&format!("INSERT INTO t VALUES ('b{}', 'vb{}')", i, i)).unwrap();
                }

                a.sync_with(&mut b_engine);
                (a.snapshot_hash(), b_engine.snapshot_hash())
            });
        });
    }
    group.finish();
}

fn bench_snapshot_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_hash");

    for row_count in [100, 1_000, 10_000] {
        group.bench_with_input(BenchmarkId::new("hash", row_count), &row_count, |b, &n| {
            let mut engine = Engine::open_with_policy(".", "bench", FkPolicy::Tombstone);
            engine.execute("CREATE TABLE t (id TEXT PRIMARY KEY, val TEXT, extra TEXT)").unwrap();
            for i in 0..n {
                engine.execute(&format!("INSERT INTO t VALUES ('r{}', 'v{}', 'e{}')", i, i, i)).unwrap();
            }
            b.iter(|| engine.snapshot_hash());
        });
    }
    group.finish();
}

criterion_group!(benches, bench_insert, bench_sync, bench_snapshot_hash);
criterion_main!(benches);
