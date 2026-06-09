# Reproducible Benchmarks

This document describes the benchmark path used for Hawk performance and compression claims. Treat benchmark numbers as environment-specific unless the hardware, OS, Rust version, dataset, and command line are reported with the result.

## Environment Template

Record this block with every published benchmark snapshot:

| Field | Value |
|---|---|
| CPU | `<model and core count>` |
| RAM | `<amount and speed if known>` |
| OS | `<name and version>` |
| Rust | `rustc --version` |
| Hawk commit | `git rev-parse --short HEAD` |
| Dataset rows | `<row count>` |
| Dataset seed | `<seed>` |
| Raw input size | `wc -c <csv>` |
| Hawk DB size | `du -sk <db_dir>` |

## Benchmark Types

Microbenchmarks measure isolated math routines:

```bash
cargo bench -p hawk-engine --bench bench_jsd
```

End-to-end benchmarks measure deterministic in-memory ingest and query paths over 10,000 synthetic rows:

```bash
cargo bench -p hawk-engine --bench bench_end_to_end
```

The end-to-end benchmark includes:

| Benchmark | Measures |
|---|---|
| `e2e_ingest_10k_rows` | Batch ingestion into a fresh database |
| `e2e_compare_10k_rows` | `COMPARE` latency over yearly distributions |
| `e2e_track_10k_rows` | `TRACK` latency over the time dimension |
| `e2e_mi_10k_rows` | Mutual information latency for `political_leaning` x `topic` |
| `e2e_cmi_10k_rows` | Conditional MI latency given `time` |

Criterion writes JSON estimates under `target/criterion/<benchmark>/new/estimates.json`. Convert those estimates into a Markdown table with:

```bash
python scripts/criterion_to_markdown.py target/criterion
```

## Deterministic Dataset Generation

Generate a small smoke dataset:

```bash
python tests/generate_test_data.py \
  --rows 1000 \
  --seed 42 \
  --output tests/fixtures/community_notes_1k.csv
```

Generate the medium dataset used for local reproducibility checks:

```bash
python tests/generate_test_data.py \
  --rows 10000 \
  --seed 42 \
  --output tests/fixtures/community_notes_10k.csv
```

The generator is deterministic for a fixed `--seed`, `--rows`, and Python version. It creates Community Notes-style rows with:

| Column | Meaning |
|---|---|
| `sentiment_score` | Continuous value from -1.0 to 1.0 |
| `political_leaning` | Categorical variable |
| `topic_label` | Topic dimension candidate |
| `created_at` | Date dimension candidate |

## Compression Snapshot Procedure

Use this procedure when publishing storage-size claims:

```bash
python tests/generate_test_data.py --rows 10000 --seed 42 --output /tmp/hawk_10k.csv
cargo run -p hawk-engine --example benchmark_snapshot -- /tmp/hawk_10k.csv /tmp/hawk_bench_db
```

Report:

| Metric | How to compute |
|---|---|
| Raw input size | `wc -c /tmp/hawk_10k.csv` |
| Hawk DB size | `du -sk /tmp/hawk_bench_db` |
| Compression ratio | `raw_input_bytes / hawk_db_bytes` |
| Ingest duration | Wall-clock command duration or benchmark result |
| Query latency | Criterion estimates from `bench_end_to_end` |

## Large Dataset Reproduction

Do not commit large datasets to the repository. For large-claim reproduction, publish:

- dataset source URL and retrieval date
- exact row count after filtering
- preprocessing command or script
- SHA-256 of the raw input file
- benchmark environment template values
- generated Criterion Markdown table

## CI Smoke

CI runs Criterion in smoke mode:

```bash
cargo bench -p hawk-engine --bench bench_jsd -- --test
cargo bench -p hawk-engine --bench bench_end_to_end -- --test
```

This confirms benchmarks compile and execute without making CI depend on noisy performance thresholds.
