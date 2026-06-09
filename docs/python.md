# Python

Hawk ships first-class Python bindings (`hawk_engine`) built with
[PyO3](https://pyo3.rs) and [Maturin](https://www.maturin.rs). A Python user can
ingest data and run the full query language without touching Rust.

## Install

### From a local checkout (development)

```bash
# Build the extension into the current virtualenv, in place
maturin develop -m crates/hawk-python/Cargo.toml --release

# Verify
python -c "import hawk_engine; print('hawk_engine ok')"
```

### Build a wheel

```bash
maturin build -m crates/hawk-python/Cargo.toml --release --out dist
python -m pip install dist/*.whl
```

The package name on import is `hawk_engine`. Requires Python 3.9+.

## Quickstart

```python
import hawk_engine

# Create a fresh database (read-write)
db = hawk_engine.HawkDB.create("./demo_db")

# Ingest a file — schema (variables, dimensions) is inferred
report = db.ingest("data.csv")
print(report)  # IngestReport(processed=..., skipped=..., ...)

# Query with the Hawk DSL
result = db.query("COMPARE category BETWEEN time:2024 AND time:2025")
print(result)              # pretty table
print(result.header)       # ['Metric', 'Value']
print(result.rows)         # [['JSD', '0.68'], ...]
print(result.to_dicts())   # [{'Metric': 'JSD', 'Value': '0.68'}, ...]

db.close()                 # flushes to disk
```

`HawkDB` is also a context manager, which closes (and flushes) on exit:

```python
with hawk_engine.HawkDB.open("./demo_db", mode="read_only") as db:
    print(db.stats())
    print(db.query("SCHEMA"))
```

## API reference

### `HawkDB`

| Method | Signature | Notes |
|---|---|---|
| `HawkDB.create` | `(path: str) -> HawkDB` | Create a new database (read-write). |
| `HawkDB.open` | `(path: str, mode: str = "read_write") -> HawkDB` | `mode` is `"read_write"`/`"rw"` or `"read_only"`/`"ro"`. |
| `db.query` | `(sql: str) -> QueryResult` | Run any Hawk DSL statement. |
| `db.ingest` | `(path, max_categories=50, date_columns=None, date_granularity="yearly") -> IngestReport` | Ingest CSV/JSON/Parquet (file-format support depends on enabled features). |
| `db.ingest_records` | `(records: list[dict]) -> IngestReport` | Ingest in-memory rows against the existing schema; flushes. |
| `db.stats` | `() -> DatabaseStats` | Counts of distributions/samples/variables/dimensions. |
| `db.schema` | `() -> dict` | Schema as a plain Python dict (parsed JSON). |
| `db.flush` | `() -> None` | Persist pending writes. |
| `db.close` | `() -> None` | Flush and release; safe to call once. |

`HawkDB` supports `with` (calls `close()` on exit).

### `QueryResult`

| Member | Type | Notes |
|---|---|---|
| `.header` | `list[str]` | Column names. |
| `.rows` | `list[list[str]]` | Row cells as strings. |
| `.to_dicts()` | `list[dict]` | Rows zipped with the header. |
| `.to_csv()` | `str` | CSV serialization. |
| `.to_json()` | `str` | JSON serialization. |
| `len(result)` | `int` | Number of rows. |
| `str(result)` | `str` | Pretty-printed table. |

### `DatabaseStats`

Read-only attributes: `distributions`, `total_samples`, `variables`, `dimensions`.

### `IngestReport`

Read-only attributes: `total_rows`, `processed_rows`, `skipped_rows`,
`distributions_updated`, `elapsed_ms`.

## Errors

All engine failures raise `hawk_engine.HawkError` (a subclass of `Exception`)
with a readable message. Operating on a closed database raises `RuntimeError`;
an invalid `mode` raises `ValueError`.

```python
try:
    db.query("NONSENSE")
except hawk_engine.HawkError as e:
    print("query failed:", e)
```

## Example

A runnable end-to-end script lives at
[`examples/python/basic_usage.py`](../examples/python/basic_usage.py):

```bash
maturin develop -m crates/hawk-python/Cargo.toml
python examples/python/basic_usage.py
```

## Parity with the Rust engine

The Python bindings call the same `hawk_engine` query path as the CLI, so query
output matches the Rust engine for the same database. `db.query(...).to_json()`
returns the same structure the CLI's `EXPORT ... AS JSON` and the MCP `query`
tool return.
