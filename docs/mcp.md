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
| `query` | `sql: string` | Query result as JSON. |
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

## Privacy warning

The MCP surface is agent-**safer**, not private:

- Do not point the server at a database with **raw-log retention enabled**
  unless the agent is trusted to see original records — raw logs can contain
  the source rows (see [file-format.md](file-format.md#raw-logs)).
- Small or high-cardinality slices can still be revealing.
- Prefer `--readonly` so an agent cannot ingest new data or mutate the database.

There is a runnable agent-demo walkthrough at
[`examples/mcp/README.md`](../examples/mcp/README.md).
