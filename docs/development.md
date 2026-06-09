# Development Guide

## Workspace Layout

| Path | Responsibility |
|---|---|
| `crates/hawk-engine` | Core types, storage, ingest, math, query engine, SQL DSL, CLI |
| `crates/hawk-server` | Axum web UI and HTTP ingest surface |
| `crates/hawk-mcp` | MCP server wrapper around Hawk queries and state |
| `crates/hawk-python` | PyO3 Python bindings |
| `tests` | Shared test fixtures and deterministic data generation |
| `docs` | Maintainer and benchmark documentation |

## Storage Files

| File | Contents | When it changes |
|---|---|---|
| `meta.edb` | Schema, counters, high-water mark, raw-log config | schema changes, ingestion counters |
| `distributions.edb` | Marginal and joint distributions | ingestion and updates |
| `dist_index.edb` | Persistent lookup index | distribution creation |
| `snapshots.edb` | Historical distribution snapshots | update paths that snapshot |
| `raw/` | Optional raw record log | raw-log enabled ingestion |

The storage layer may build additional in-memory indexes on open. Keep persisted format changes deliberate and documented in `docs/file-format.md`.

## Ingest Pipeline

```text
reader -> schema inference or explicit mapping -> row mapping -> batch updater -> storage flush
```

Important modules:

- `ingest/csv_reader.rs`, `json_reader.rs`, `parquet_reader.rs`
- `ingest/schema_inference.rs`
- `ingest/column_mapper.rs`
- `ingest/batch_updater.rs`

## Query Pipeline

```text
SQL text -> tokenizer -> parser -> executor -> QueryEngine -> storage/math
```

Direct API callers can skip SQL and use `QueryEngine`.

## Wrappers

The web server, MCP server, and Python package should stay thin over `hawk-engine`.

- Put durable behavior in `hawk-engine`.
- Keep wrapper-specific validation close to the wrapper boundary.
- Add engine tests for shared behavior instead of duplicating logic in wrappers.

## Local Checks

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
