# MCP Agent Demo

Hawk's MCP server lets an agent ask statistical questions over distribution summaries instead of raw rows.

## Start the Server

Create or choose a Hawk database, then start MCP over stdio:

```bash
cargo run -p hawk-mcp -- --db ./my_hawk_db --readonly
```

Use `--readonly` when the agent only needs analysis. This keeps the session focused on querying existing summaries.

## Example Agent Prompts

- "Show me the schema of this Hawk database."
- "Compare category drift between 2024 and 2025."
- "Track entropy over time and identify the largest shift."
- "Which variables have the strongest association?"
- "Explain the top contributors to divergence between two time windows."

## Expected Tool Results

The agent should receive compact statistical outputs such as:

- available variables and dimensions
- JSD, PSI, Hellinger, and entropy values
- top category movers
- MI, normalized MI, and Cramer's V
- time-series drift points

## Why This Is Agent-Safe

Hawk can provide aggregate distribution context without sending every raw row to the agent. This is useful for trend analysis, drift monitoring, and summarization workflows.

This is not a privacy guarantee by itself. Avoid exposing raw-log enabled databases or small/high-cardinality slices unless the agent is trusted to see that data.
