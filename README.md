# Hawk

[![CI](https://github.com/repfly/hawk/actions/workflows/ci.yml/badge.svg)](https://github.com/repfly/hawk/actions/workflows/ci.yml)
[![Audit](https://github.com/repfly/hawk/actions/workflows/audit.yml/badge.svg)](https://github.com/repfly/hawk/actions/workflows/audit.yml)

Hawk is a Rust database for storing distribution summaries and querying changes between them. It is intended for drift analysis, association analysis, and sharing aggregate statistics without keeping raw rows by default.

Hawk stores marginal and joint distributions for categorical and continuous variables, optionally grouped by dimensions such as time or region. Its query language includes `COMPARE`, `TRACK`, `EXPLAIN`, `SHOW`, `MI`, `CORRELATIONS`, and related commands.

The project is pre-1.0. The query language, file format, and public APIs may change.

## Quick start

Requirements: Rust 1.75 or newer.

```bash
cargo build --release
cargo test
```

Start the local web server:

```bash
cargo run --release --bin hawk-server -- my_database.db 3000
```

Then open <http://127.0.0.1:3000>. Use the CLI instead with `cargo run --release --bin hawk -- my_database.db`.

Runnable examples are listed in [docs/examples/README.md](docs/examples/README.md).

## Query examples

```sql
-- Compare a variable in two slices
COMPARE category BETWEEN time:2024 AND time:2025

-- Track a variable over time
TRACK category FROM time:2024 GRANULARITY monthly

-- Explain which variables contribute to a difference
EXPLAIN time:2024 VS time:2025

-- Measure association between two variables
MI author, category AT time:2024

-- Inspect a distribution
SHOW category AT time:2025 TOP 10
```

See [docs/surprise.md](docs/surprise.md) and [docs/structure.md](docs/structure.md) for query semantics and structural comparisons.

## Metrics and exports

The engine implements entropy, Jensen–Shannon divergence, KL divergence, PSI, Hellinger distance, Wasserstein distance, mutual information, normalized mutual information, Cramér's V, and conditional mutual information.

Query results can be exported as JSON or CSV:

```sql
EXPORT COMPARE category ACROSS time AS CSV
```

## Ingestion and server mode

The server accepts JSON records at `/ingest` when ingestion is enabled:

```bash
curl -X POST http://localhost:3000/ingest \
  -H 'Content-Type: application/json' \
  -d '{"category":"TECH","date":"2024-01-15"}'
```

Use `--readonly` or `--disable-ingest` to disable writes. Set `--auth-token` or `HAWK_SERVER_TOKEN` when the endpoint needs authentication. The server binds to `127.0.0.1` by default.

Raw-log retention is optional. Distribution summaries are not a formal privacy or anonymization guarantee; treat retained raw records and sensitive aggregates accordingly.

## Use as a Rust library

```toml
[dependencies]
hawk-engine = "0.1"
```

```rust
use hawk_engine::query::QueryEngine;
use hawk_engine::storage::{Database, OpenMode};

let db = Database::open("my.db", OpenMode::ReadOnly).unwrap();
let engine = QueryEngine::default();
let result = engine.compare(&db, "time:2024", "time:2025", None).unwrap();
println!("JSD = {:.6}", result.jsd);
```

The crate is published on [crates.io](https://crates.io/crates/hawk-engine).

## Python and MCP

Build the Python bindings with:

```bash
maturin develop -m crates/hawk-python/Cargo.toml --release
```

Python documentation is in [docs/python.md](docs/python.md). To expose a read-only database over MCP:

```bash
cargo run -p hawk-mcp -- --db ./my_hawk_db --readonly
```

See [docs/mcp.md](docs/mcp.md) for configuration and limitations.

## Project layout

- `crates/hawk-engine` — core types, ingestion, storage, metrics, and query engine
- `crates/hawk-server` — web UI and HTTP ingestion endpoint
- `crates/hawk-python` — Python bindings
- `crates/hawk-mcp` — MCP server
- `docs` — concepts, examples, compatibility, and development notes

## Contributing and license

See [CONTRIBUTING.md](CONTRIBUTING.md) for development guidance. Compatibility notes are in [docs/compatibility.md](docs/compatibility.md), and release notes are in [CHANGELOG.md](CHANGELOG.md).

Hawk is licensed under the MIT license.
