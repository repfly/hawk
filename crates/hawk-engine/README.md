# hawk-engine

A distribution-native analytics engine. Instead of storing raw rows, Hawk digests data into compact probability distributions and lets you query them directly — compare, explain, track drift, and discover correlations, all through an information-theoretic lens.

**209,527 rows of news articles compress to 5KB. Queries return in microseconds.**

## Install

```toml
[dependencies]
hawk-engine = "0.1"
```

## Quick start

```rust
use hawk_engine::core::{VariableDefinition, VariableType, DimensionDefinition};
use hawk_engine::storage::{Database, OpenMode};
use hawk_engine::ingest::{IngestionPipeline, IngestOptions, InferConfig};
use hawk_engine::query::QueryEngine;

// Create a database and auto-ingest a CSV
let mut db = Database::create("my.db").unwrap();
IngestionPipeline::ingest_file_auto(
    &mut db,
    "data.csv",
    InferConfig::default(),
    IngestOptions::default(),
).unwrap();

// Query
let engine = QueryEngine::default();
let result = engine.compare(&db, "time:2023", "time:2024", None).unwrap();
println!("JSD = {:.6}", result.jsd);

// Or use the SQL-like DSL
let output = hawk_engine::sql::query(&db, &engine, "COMPARE category BETWEEN time:2023 AND time:2024").unwrap();
println!("{}", output);
```

## Modules

| Module | What it does |
|--------|-------------|
| `core` | Base types: `DistributionObject`, `Schema`, `VariableType`, `DimensionKey` |
| `math` | Entropy, JSD, KL, PSI, Hellinger, MI, NMI, Cramer's V, Wasserstein |
| `storage` | Binary format (HAWK magic + zstd), mmap reads, file locking |
| `ingest` | CSV/JSON/Parquet ingestion, rayon parallelism, schema inference |
| `query` | Compare, explain, track, mutual info, pairwise, correlation discovery |
| `sql` | SQL-like DSL: tokenizer, recursive-descent parser, executor |

## Metrics

| Metric | Range | Measures |
|--------|-------|----------|
| Entropy | [0, log k] | Distribution uncertainty |
| JSD | [0, 1] | Symmetric divergence |
| KL divergence | [0, inf) | Directional divergence |
| PSI | [0, inf) | Population stability |
| Hellinger | [0, 1] | Bounded symmetric distance |
| Mutual Information | [0, inf) | Shared information between variables |
| NMI | [0, 1] | Normalized association strength |
| Cramer's V | [0, 1] | Effect size for categorical association |
| Wasserstein | [0, inf) | Earth mover's distance (histograms only) |

## Query language

```sql
COMPARE category BETWEEN time:2013 AND time:2022
COMPARE category ACROSS time WHERE region:US
EXPLAIN time:2013 VS time:2022
TRACK category FROM time:2012 GRANULARITY yearly
SHOW category AT time:2022 TOP 5
RANK category BY ENTROPY OVER time
MI author, category AT time:2016
CMI author, category GIVEN time
CORRELATIONS OVER time LIMIT 10
PAIRWISE time ON category USING jsd
NEAREST time:2022 ON time LIMIT 3
EXPORT STATS AS JSON
STATS
SCHEMA
DIMENSIONS time
```

## Feature flags

- `json` — Enable JSON Lines ingestion
- `parquet` — Enable Parquet ingestion

## License

MIT
