# MCP

Hawk ships an [MCP](https://modelcontextprotocol.io) server (`hawk-mcp`) so an
LLM agent can ask statistical questions over **distribution summaries** instead
of raw rows. This is the "agent-safe statistical context" surface: the agent
sees JSD, PSI, entropy, top movers, and associations — not individual records.

## Why MCP matters for Hawk

Giving an agent direct table/database access exposes every row to the model and
to its context window. Hawk's MCP server exposes only aggregate distribution
queries, so an agent can investigate drift and association without the raw data
ever leaving the engine. Typical workflow: the agent detects drift with
`COMPARE`/`TRACK`, explains the top contributors with `EXPLAIN`, and suggests
where to investigate — all over summaries.

This reduces exposure; it is **not** a privacy guarantee. See the warning below.

## Run the server

The server speaks MCP over stdio. Point it at a Hawk database directory:

```bash
cargo run -p hawk-mcp -- --db ./my_hawk_db --readonly
```

| Flag | Meaning |
|---|---|
| `--db <path>` | Database directory to open at startup (optional; the agent can also call `open_database`). |
| `--readonly` | Open read-only. Recommended for analysis-only agents. |
| `--min-cell-count <k>` | Small-cell suppression: categories with fewer than `k` samples fold into `__unknown__` on every query result; metrics are computed after folding. Storage untouched. Default off. |
| `--bit-budget <bits>` | Session disclosure budget. Each successful query is charged in bits to the information ledger; an over-budget query returns a structured JSON refusal instead of results. Default off. See [information-ledger.md](information-ledger.md). |

Logs go to stderr (stdout is reserved for the MCP protocol). For an
analysis-only agent, prefer `--readonly` so the session cannot ingest or mutate.

### Example client config

```json
{
  "mcpServers": {
    "hawk": {
      "command": "cargo",
      "args": ["run", "-p", "hawk-mcp", "--", "--db", "./my_hawk_db", "--readonly"]
    }
  }
}
```

Use a built binary path (e.g. `target/release/hawk-mcp`) instead of `cargo run`
in production so startup is not a build step.

## Available tools

| Tool | Arguments | Returns |
|---|---|---|
| `help` | — | The Hawk SQL syntax reference (all query types + examples). |
| `query` | `sql: string` | Query result as JSON. Charged to the information ledger; over-budget queries return `{"refused": true, ...}`. |
| `ledger` | — | Session information ledger: bits revealed per variable, total, budget, remaining. Free to call. |
| `suggest` | `limit?: number` | Ranked next queries by expected information gain, deduplicated against the session history: `[{query, rationale, expected_bits, cost_bits, fits_budget}]`. Free to call. |
| `profile` | — | One-call dataset card: variables (type, entropy, samples), dimensions, top associations by MI, biggest recent drift, storage summary. Charged one flat scalar per variable, once per session. |
| `schema` | — | Variables (with types), dimensions, joint definitions. |
| `stats` | — | `distributions`, `total_samples`, `variables`, `dimensions`. |
| `list_dimensions` | `dimension: string` | Unique values for a dimension. |
| `open_database` | `path: string`, `readonly?: bool` | Opens a database (closes the current one). |
| `create_database` | `path: string` | Creates an empty database (closes the current one). |
| `ingest_file` | `file_path: string`, `max_categories?`, `date_columns?`, `date_granularity?` | Ingest report as JSON. |

Tool descriptions are written for tool-using models; an agent should call `help`
first to discover query syntax, then `schema`/`stats` to orient, then `query`.

## Example prompts

- "Show me the schema of this Hawk database."
- "Compare category drift between 2024 and 2025."
- "Track entropy over time and identify the largest shift."
- "Which variables have the strongest association?"
- "Explain the top contributors to divergence between two time windows."

## Expected tool output

`query` returns compact JSON, e.g. for a `COMPARE`:

```json
{
  "header": ["Metric", "Value"],
  "rows": [["JSD", "0.684139"], ["PSI", "36.357643"], ["Hellinger", "0.782895"]]
}
```

The agent receives aggregate values — JSD/PSI/Hellinger/entropy, top movers,
MI/NMI/Cramér's V, and time-series drift points — never raw rows.

## Surprisal scoring (`SURPRISE`)

`SURPRISE <dim:val> UNDER <dim:val> [ON <var>]` reports, in bits, how
surprising slice A's data is under slice B's stored model: cross-entropy,
excess bits (KL), an unseen-mass warning, and a per-bucket "Top Surprises"
table. Without `ON <var>` it scores every variable and ranks by excess bits —
a useful one-call anomaly check for an agent ("which variable moved the
most?"). It also works via `EXPORT SURPRISE ... AS JSON`. See
[surprise.md](surprise.md) for semantics and edge cases.

## Dependency structure (`STRUCTURE`)

`STRUCTURE AT <dim:val>` returns the Chow-Liu dependency tree over the
variables at a slice — edge list ranked by MI plus total retained information
in bits, with pairs lacking a stored joint listed as unknown.
`COMPARE STRUCTURE BETWEEN <dim:val> AND <dim:val>` diffs the two trees:
added / dropped / re-weighted edges, an MI-weighted rewiring score, and a
"retained information changed by X bits" headline — how relationships rewired
even when marginals barely move. Both work via `EXPORT ... AS JSON|CSV`. See
[structure.md](structure.md) for semantics and edge cases.

## Max-entropy estimation (`ESTIMATE`)

`ESTIMATE <var_a>, <var_b> AT <dim:val>` reconstructs the joint distribution
of two variables from their stored marginals when no joint was ever stored —
maximum entropy via iterative proportional fitting, with per-cell Fréchet
bounds, an MI upper bound, and `missing_information_bits` (the bits about the
dependency the database does not know), all under an explicit
`ESTIMATED — not observed` banner. If a stored joint exists at the slice it
is preferred and bannered `OBSERVED — stored joint`. `MI` on an unstored pair
falls back to this estimate with an explicit warning instead of erroring.
Full grids via `EXPORT ESTIMATE ... AS JSON|CSV`. See
[estimate.md](estimate.md) for the derivation and edge cases.

## Storage audit (`AUDIT STORAGE`)

`AUDIT STORAGE` is an advisory, read-only MDL report over everything stored:
per object the approximate on-disk cost, the information it retains in bits,
and a recommendation (rebin an over-resolved histogram, fold ~0-bit
categories, drop a joint whose dependency information cannot pay for its
bytes — with how closely `ESTIMATE` would recover it), plus a summary footer
with total size, total candidate savings, and how many snapshots are
redundant to their temporal neighbors. It never modifies anything. Works via
`EXPORT AUDIT STORAGE AS JSON|CSV`. See [mdl-storage.md](mdl-storage.md) for
the cost model, opt-in MDL auto-binning, and snapshot compaction.

## Guided exploration (`SUGGEST` + `suggest`/`profile` tools)

`SUGGEST [LIMIT <n>]` ranks candidate next queries by expected information
gain in bits: the highest-entropy variables not yet explored (`SHOW`, score
= H(var)), the dimension that explains the most about a variable
(`COMPARE ... ACROSS`, score = MI(var; dim) from stored per-slice marginals),
and unstored variable pairs with the widest dependency uncertainty
(`ESTIMATE`, score = min(H(A), H(B)) missing bits). The MCP `suggest` tool
feeds the ranking with the session ledger's history (already-executed
queries and released variables drop out) and annotates every suggestion with
its ledger cost and whether it fits the remaining bit budget. The `profile`
tool is the one-call dataset card to run first: variables with entropies,
dimensions, top associations, biggest recent drift, and a storage summary.
Together they close the loop from Checkpoint 6 — an agent orients with
`profile`, then follows `suggest` under a bit budget. Both `SUGGEST` and the
`suggest` tool are free on the ledger; `profile` charges one flat scalar per
variable, once per session. See [suggest.md](suggest.md) for the scoring,
dedup, and determinism rules.

## Guardrails: suppression and the information ledger

Two opt-in guardrails make agent sessions self-limiting:

- **Small-cell suppression** (`--min-cell-count <k>`): categories with fewer
  than `k` samples fold into `__unknown__` on every read path — `SHOW`,
  `COMPARE` top movers, `EXPLAIN`, `SURPRISE`, `ESTIMATE` grids, `EXPORT` —
  and every released metric (entropy, JSD, MI, …) is computed **after**
  folding, so numbers stay consistent with what was shown. Stored data is
  never modified.
- **Information ledger** (`--bit-budget <bits>`): each successful `query` is
  charged in bits — a released distribution costs its entropy, a scalar
  metric a small flat charge, metadata is free, identical queries charge
  once. A query that would exceed the budget gets a structured refusal
  (`{"refused": true, "reason": ..., "query_cost_bits": ..., "spent_bits":
  ..., "budget_bits": ..., "remaining_bits": ...}`) the agent can re-plan
  around; nothing is charged. The `ledger` tool reports spend at any time.

Charge semantics, dedup rules, and — importantly — the limitations of this
accounting are defined in [information-ledger.md](information-ledger.md).
It is a spend meter, not a formal privacy guarantee.

## Privacy warning

The MCP surface is agent-**safer**, not private:

- Do not point the server at a database with **raw-log retention enabled**
  unless the agent is trusted to see original records — raw logs can contain
  the source rows (see [file-format.md](file-format.md#raw-logs)).
- Small or high-cardinality slices can still be revealing.
- Prefer `--readonly` so an agent cannot ingest new data or mutate the database.

There is a runnable agent-demo walkthrough at
[`examples/mcp/README.md`](../examples/mcp/README.md).
