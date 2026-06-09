# Examples

These examples are designed to run quickly from a fresh clone.

## Drift Analysis

Run:

```bash
cargo run -p hawk-engine --example drift_analysis
```

What it shows:

- builds two time windows
- runs `COMPARE category BETWEEN time:before AND time:after`
- runs `TRACK category FROM time:before`
- demonstrates a distribution shift where `login` becomes dominant

Expected output includes a `COMPARE` table with JSD, entropy values, and top movers, followed by a `TRACK` table.

## Privacy-Safe Sharing

Run:

```bash
cargo run -p hawk-engine --example privacy_safe_sharing
```

What it shows:

- creates raw rows in memory
- ingests them with raw-log retention disabled
- queries only distribution summaries with `SHOW`
- explains that raw-log mode changes the privacy properties

Hawk databases can be useful for sharing aggregate statistical context, but distribution summaries are not a formal anonymization guarantee. Small sample sizes and high-cardinality dimensions can still leak information.

## Association Discovery

Run:

```bash
cargo run -p hawk-engine --example association_discovery
```

What it shows:

- creates a dataset with a known relationship between `plan` and `outcome`
- runs `MI`
- runs `CMI`
- runs `CORRELATIONS`

Expected output highlights a strong association between paid plans and retention in the generated data.

## Python

See [../../examples/python/basic_usage.py](../../examples/python/basic_usage.py).

## MCP

See [../../examples/mcp/README.md](../../examples/mcp/README.md).
