// benches/throughput.rs — 50K ops/second benchmark on a 3-node cluster.
//
// HOW TO RUN:
//   cargo bench --bench throughput
//
// WHAT THIS MEASURES:
//   - Throughput (ops/sec) of SET operations through the Raft cluster.
//   - Latency distribution (p50, p99) of a single operation.
//
// HOW TO INTERPRET:
//   Criterion reports mean time per iteration.
//   ops/sec = 1 / mean_time_per_iteration
//   Target: 50,000 ops/sec = 20µs per op.
//
// TUNING HINTS if you're below target:
//   - Batch AppendEntries: send multiple log entries per RPC instead of one.
//   - Pipeline: don't wait for disk fsync on every entry (risk: durability trade-off).
//   - Use larger write batches in RocksDB.
//   - Reduce lock contention in RaftState (consider dashmap, or fine-grained locks).

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

fn bench_set_throughput(c: &mut Criterion) {
    // TODO:
    // 1. Start a 3-node in-process cluster (same setup as chaos_test).
    // 2. Get a RaftKvClient connected to the leader.
    //
    // let rt = tokio::runtime::Runtime::new().unwrap();
    // let mut client = rt.block_on(async { setup_cluster_and_client().await });
    //
    // 3. Benchmark a single SET:
    let mut group = c.benchmark_group("raft-kv-throughput");
    group.throughput(Throughput::Elements(1));

    group.bench_function(BenchmarkId::new("SET", "single-key"), |b| {
        b.iter(|| {
            // TODO: rt.block_on(client.set("bench-key", b"bench-val".to_vec()))
            todo!("benchmark single SET through 3-node cluster")
        });
    });

    group.finish();
}

criterion_group!(benches, bench_set_throughput);
criterion_main!(benches);
