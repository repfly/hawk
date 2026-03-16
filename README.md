# Hawk

A distribution-native analytics engine. Instead of storing raw rows, Hawk digests data into compact probability distributions and lets you query them directly -- compare, explain, track drift, and discover correlations, all through an information-theoretic lens.

**209,527 rows of news articles compress to 5KB. Queries return in microseconds.**

```
hawk> COMPARE category BETWEEN time:2013 AND time:2022

Metric              Value
──────────────────  ──────────────────────────────────────────
JSD                 0.684139
PSI                 36.357643
Hellinger           0.782895
Entropy(A)          3.6248 bits
Entropy(B)          3.1460 bits
Samples             34583 vs 1398

--- Top Movers ---
POLITICS            +0.2854  (0.000 → 0.285)  contrib=0.1427
WELLNESS            -0.2150  (0.232 → 0.017)  contrib=0.0796
U.S. NEWS           +0.1724  (0.000 → 0.172)  contrib=0.0862
```

## How it works

1. **Define** variables (categorical or continuous) and dimensions (e.g., time)
2. **Ingest** data from CSV, JSON, or Parquet -- Hawk builds histograms and contingency tables
3. **Query** the distributions directly using a SQL-like language, Python API, or web UI

The database stores only the distribution summaries, not the raw data. Everything is built on entropy and information theory: JSD for comparison, mutual information for association, KL divergence for directionality.

## Quick start

```bash
# Build
cargo build --release

# Start the web UI
cargo run --release --bin hawk-server -- my_database.db 3000

# Or use the CLI
cargo run --release --bin hawk -- my_database.db
```

### Python

```python
import hawk as hdb

# Create a database and define schema
db = hdb.create("my_db")
db.define_variable("category", "categorical", categories=["A", "B", "C"])
db.define_dimension("time", "date", granularity="yearly")

# Ingest data
db.ingest("data.csv", {
    "variables": {"category_col": "category"},
    "dimensions": {"date_col": "time"},
})

# Query
result = db.sql("COMPARE category BETWEEN time:2023 AND time:2024")
print(result["text"])

# Or get a DataFrame
df = db.sql_df("COMPARE category ACROSS time")
```

Auto-ingest without manual schema definition:

```python
db = hdb.create("my_db")
db.ingest_auto("data.json")  # infers variable types from the data
```

## Query language

```sql
-- Compare two distribution slices
COMPARE category BETWEEN time:2013 AND time:2022

-- With dimension filters
COMPARE category BETWEEN time:2013 AND time:2022 WHERE region:US

-- Compare all pairs across a dimension
COMPARE category ACROSS time

-- What drives the divergence?
EXPLAIN time:2013 VS time:2022

-- Track drift over time
TRACK category FROM time:2012 GRANULARITY yearly

-- Show a distribution (top 5 categories)
SHOW category AT time:2022 TOP 5

-- Entropy ranking
RANK category BY ENTROPY OVER time

-- Mutual information between variables
MI author, category AT time:2016

-- Conditional MI (controlling for time)
CMI author, category GIVEN time

-- Find strongest associations
CORRELATIONS OVER time LIMIT 10

-- Pairwise distance matrix
PAIRWISE time ON category USING jsd

-- Nearest distributions
NEAREST time:2022 ON time LIMIT 3 USING hellinger

-- Export results
EXPORT STATS AS JSON
EXPORT COMPARE category ACROSS time AS CSV

-- Metadata
STATS
SCHEMA
DIMENSIONS time
```

### Example outputs

**Drift tracking:**
```
hawk> TRACK category FROM time:2012 GRANULARITY yearly

Time  Entropy  Drift (JSD)
────  ───────  ────────────────
2012  3.6310   0.0314
2013  3.6248   0.3571 <- shift
2014  4.8237   0.1656 <- shift
2015  4.4118   0.0561 <- shift
2018  3.3050   0.1775 <- shift
2020  3.0430   0.0372
2021  2.9053   0.0286
2022  3.1460   0.0000
```

**Explain divergence:**
```
hawk> EXPLAIN time:2013 VS time:2022

Variable          JSD       Fraction
────────────────  ────────  ──────────────
TOTAL             0.830323  100.0%
category          0.684139  82.4%
  POLITICS        +0.2854   contrib=0.1427
  WELLNESS        -0.2150   contrib=0.0796
  U.S. NEWS       +0.1724   contrib=0.0862
author            0.146184  17.6%
  Mary Papenfuss  +0.0715   contrib=0.0358
```

**Association strength:**
```
hawk> MI author, category AT time:2016

Metric       Value
───────────  ───────────
MI           1.7794 bits
NMI          0.5537
Cramer's V   0.5186
Samples      5688
Strength     strong
```

## Metrics

All metrics are rooted in information theory:

| Metric | Formula | Range | What it measures |
|--------|---------|-------|------------------|
| **Entropy** | H(X) = -Σ p_i log p_i | [0, log k] | Distribution uncertainty |
| **JSD** | H(M) - ½H(P) - ½H(Q) | [0, 1] | Symmetric divergence |
| **KL divergence** | Σ p_i log(p_i/q_i) | [0, ∞) | Directional divergence |
| **PSI** | KL(P\|\|Q) + KL(Q\|\|P) | [0, ∞) | Population stability (<0.1 stable, >0.2 significant) |
| **Hellinger** | (1/√2)√(Σ(√p-√q)²) | [0, 1] | Bounded symmetric distance |
| **MI** | H(X)+H(Y)-H(X,Y) | [0, ∞) | Shared information between variables |
| **NMI** | MI / min(H(X),H(Y)) | [0, 1] | Normalized association strength |
| **Cramer's V** | √(χ²/(n·min(r-1,c-1))) | [0, 1] | Effect size for categorical association |
| **Wasserstein** | Σ\|CDF_P - CDF_Q\|·Δx | [0, ∞) | Earth mover's distance (histograms only) |

## Web UI

```bash
cargo run --release --bin hawk-server -- my_database.db 3000
# Open http://localhost:3000
```

Features:
- Interactive query input with htmx (no page reloads)
- SVG charts: diverging bar charts for COMPARE, entropy timelines for TRACK, distribution bars for SHOW, heatmaps for PAIRWISE
- Clickable schema sidebar
- Query history (persisted in localStorage)
- Streaming ingestion endpoint: `POST /ingest` with JSON body

## Streaming ingestion

The web server accepts live data via HTTP:

```bash
# Single record
curl -X POST http://localhost:3000/ingest \
  -H 'Content-Type: application/json' \
  -d '{"category": "TECH", "date": "2024-01-15"}'

# Batch
curl -X POST http://localhost:3000/ingest \
  -H 'Content-Type: application/json' \
  -d '[{"category": "TECH", "date": "2024-01-15"}, {"category": "SPORTS", "date": "2024-01-16"}]'
```

## Python plotting

```python
import hawk as hdb

db = hdb.open("my_db")

# Diverging bar chart of what changed
hdb.plot_compare(db, "time:2013", "time:2022", "category")

# Distribution bar chart
hdb.plot_distribution(db, "time:2022", "category")

# Entropy timeline
hdb.plot_track(db, "time:2012", "yearly")
```

## Storage format

Hawk uses a custom binary format with zstd compression:

```
[4 bytes] "HAWK" magic
[4 bytes] format version (u32 LE)
[rest]    zstd-compressed bincode payload
```

A database that digests 209K news articles (42 categories, 20 authors, 11 years) occupies **~6KB on disk**.

| File | Contents |
|------|----------|
| `meta.edb` | Schema, counters, config |
| `distributions.edb` | All marginal distributions + joint contingency tables |
| `dist_index.edb` | Lookup index for (variable, dimension_key) -> distribution |
| `snapshots.edb` | Historical distribution snapshots |

Backward compatible: reads older JSON (v1) and uncompressed bincode (v2) formats automatically.

## Architecture

```
hawk-core        Types: Distribution, Joint, Schema, DimensionKey
hawk-math        Entropy, JSD, KL, PSI, Hellinger, MI, NMI, Cramer's V, Wasserstein
hawk-storage     Binary file storage, zstd compression, mmap reads, locking
hawk-ingest      CSV/JSON/Parquet ingestion, rayon parallelism, schema inference
hawk-query       Query engine: compare, explain, track, pairwise, correlations
hawk-sql         SQL-like DSL: tokenizer, recursive descent parser, executor
hawk-server      Web UI: axum + htmx, SVG charts, streaming ingestion endpoint
hawk-python      PyO3 bindings, pandas/matplotlib integration
```

## Building

```bash
# Rust (CLI + server)
cargo build --release

# Python bindings (requires maturin)
pip install maturin
maturin develop --manifest-path crates/hawk-python/Cargo.toml

# Run tests
cargo test
```

Requirements: Rust 1.75+, Python 3.9+ (for bindings).

## License

MIT
