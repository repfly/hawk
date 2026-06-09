# Positioning

Hawk is a **distribution-native analytics engine**: it digests rows into compact
probability distributions and queries those distributions directly using
information-theoretic metrics. It is not a general-purpose database, and it does
not try to be one.

> Hawk queries drift, association, and statistical change without keeping raw
> rows by default.

## What Hawk is

- A compact distribution store (histograms, categoricals, and joint contingency tables).
- A drift and divergence query engine (JSD, PSI, KL, Hellinger, Wasserstein).
- A privacy-safer summary-exchange format — share a database, not the rows.
- A local/embedded analytics engine that runs in-process with no server.
- A safe statistical-context provider for LLM agents over MCP.
- A low-level engine other tools can build dashboards, reports, and monitors on.

## What Hawk is not (yet)

- A general-purpose DuckDB/ClickHouse replacement.
- A full ML-observability platform.
- A BI dashboard product.
- A row-level query engine — you cannot `SELECT *` raw records back out.
- A multi-tenant production SaaS.

## When to use Hawk

- You care about **how a distribution changed**, not about individual rows.
- You want drift/stability monitoring (PSI, JSD) embedded in an existing system.
- You want to share analytical summaries without shipping raw data.
- You want association/dependency discovery (MI, Cramér's V) over categoricals.
- You want to give an LLM agent statistical context instead of table access.
- You need a tiny, file-based artifact (kilobytes) instead of a warehouse.

## When **not** to use Hawk

- You need to retrieve or join individual rows.
- You need arbitrary SQL aggregations, windows, or ad-hoc group-bys over raw data.
- You need transactional writes or strict consistency across concurrent writers.
- You need a formal privacy/anonymization guarantee — Hawk reduces exposure by
  storing distributions, but it is **not** differential privacy. Small or
  high-cardinality slices can still be revealing, and opt-in raw-log retention
  stores original records. See [file-format.md](file-format.md#raw-logs).

## Pivot angles

These are the use-case narratives the project leans into. Each maps to a runnable
example under `crates/hawk-engine/examples` or `examples/`.

### Embedded drift engine

Drift detection inside existing systems without adopting a full observability
stack. PSI/JSD comparisons and time-window tracking are first-class queries.

```sql
COMPARE feature_x BETWEEN time:2024-01 AND time:2024-06
TRACK   feature_x FROM time:2024-01 GRANULARITY monthly
```

See `cargo run -p hawk-engine --example drift_analysis`.

### Privacy-safer analytical sharing

Ingest raw CSV → Hawk database → recipients query distributions without ever
seeing the rows. Be honest about the limits: this is reduced exposure, not
anonymization (see "When not to use Hawk" above).

See `cargo run -p hawk-engine --example privacy_safe_sharing`.

### Agent-safe analytics through MCP

LLM agents inspect distributions and statistical summaries instead of raw,
sensitive data. The MCP server exposes `query`, `schema`, `stats`, and friends
with descriptions tuned for tool-using models. See [mcp.md](mcp.md).

### Model-risk / regulatory reporting

Reproducible, decomposable drift artifacts for model governance (SR 11-7,
Basel III). `EXPLAIN` decomposes total divergence across variables, and results
export to JSON/CSV/Markdown for auditable reports.

```sql
EXPLAIN time:2023Q4 VS time:2024Q4
EXPORT COMPARE category ACROSS time AS CSV
```

## Comparison

| | Hawk | whylogs | Evidently | DuckDB | DataSketches |
|---|---|---|---|---|---|
| Persists distributions, not rows | yes | yes (profiles) | no | no | yes (sketches) |
| Query language for distributions | yes (DSL) | no | no | SQL over rows | no |
| JSD / KL / PSI / MI as queries | yes | partial (API) | via Python | no | no |
| Joint distributions first-class | yes | no | no | no | no |
| Temporal drift as a query | `TRACK` | code | code/dashboard | time-range query | no |
| Embeddable in-process | Rust/Python | Python/Java | Python | yes | C++/Java |
| Goal | distribution analytics | ML data logging | ML monitoring UI | general OLAP | streaming estimates |

## Maturity

Hawk is **pre-1.0**. The query language, storage format, and APIs may change
between minor versions. The storage format is versioned (see
[compatibility.md](compatibility.md)); breaking format changes bump the version
and are noted in the [CHANGELOG](../CHANGELOG.md).
