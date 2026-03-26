use criterion::{criterion_group, criterion_main, Criterion};
use hawk_engine::math::jsd;

fn bench_jsd_100_bins(c: &mut Criterion) {
    let p = vec![10_u64; 100];
    let mut q = vec![10_u64; 100];
    q[0] = 30;
    c.bench_function("jsd_100", |b| b.iter(|| jsd(&p, &q, 1000, 1020)));
}

fn bench_jsd_1000_bins(c: &mut Criterion) {
    let p = vec![10_u64; 1000];
    let mut q = vec![10_u64; 1000];
    q[0] = 30;
    c.bench_function("jsd_1000", |b| b.iter(|| jsd(&p, &q, 10_000, 10_020)));
}

criterion_group!(benches, bench_jsd_100_bins, bench_jsd_1000_bins);
criterion_main!(benches);
