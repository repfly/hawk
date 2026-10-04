# Information Ledger

The MCP server (`hawk-mcp`) keeps a per-session **information ledger**: a
running total of how many bits each query has revealed, per variable. With an
optional budget (`--bit-budget <bits>`) the session becomes self-limiting — a
query that would push the session over budget gets a structured refusal the
agent can read and reason about, instead of an answer.

This page defines what "bits revealed" means for each result type. The
semantics are deliberately simple; they are the accounting contract the
implementation follows.

## What this is — and is not

This is **accounting, not a formal privacy guarantee**. In the same spirit as
[positioning.md](positioning.md): Hawk reduces exposure, it does not provide
differential privacy. The ledger measures the *size* of what was released (the
entropy of released distributions, the precision of released scalars); it does
not model what an adversary can *infer* by combining answers, and it does not
add noise. Known limitations are listed at the bottom. Treat the budget as a
spend meter and a forcing function for query discipline, not as an
anonymization mechanism.

## Charge model

Every successful `query` tool call is classified by its statement type and
charged to one or more **ledger keys** — usually the variable whose
information was released. Charges are in bits.

### 1. Distribution releases — charged at released entropy

Revealing the distribution of a variable `V` at a slice releases at most
`H(V at slice)` bits: the Shannon entropy of the **released** distribution at
the **released** resolution. If small-cell suppression (`--min-cell-count`)
is on, entropy is computed *after* folding, so the charge matches what the
agent actually saw.

| Statement | Charge |
|---|---|
| `SHOW v AT d` / `EXPORT DISTRIBUTION v AT d` | `H(v at d)` → key `v` |
| `COMPARE v BETWEEN a AND b` | `H(v at a) + H(v at b)` → key `v` (top movers reveal both per-category vectors) |
| `SURPRISE a UNDER b ON v` | `H(v at a) + H(v at b)` → key `v` (per-bucket contributions reveal both) |
| `EXPLAIN a VS b` | `H(v at a) + H(v at b)` for **every** schema variable `v` → each key `v` |
| `ESTIMATE x, y AT d` | `H(joint grid)` → pair key `x×y` (canonical variable order) |

Notes: `SHOW ... TOP n` is charged the full distribution entropy — the
truncation is a display convenience, not a release guarantee, so the charge is
conservative. A joint release is charged once to the pair key `x×y`, not
double-charged to each variable.

### 2. Scalar releases — flat charge per statement

A scalar metric (JSD, PSI, MI value, an entropy number, a rank ordering)
reveals far less than a distribution. Each scalar-only statement is charged a
flat

```text
SCALAR_RELEASE_BITS = log2(10^4) ≈ 13.288 bits
```

— the log of the number of distinguishable values at the reported 4-decimal
precision over a unit-scale range. This is a deliberate simplification: it
over-charges narrow-range metrics and under-charges statements that emit many
scalars (e.g. `PAIRWISE` emits a matrix), but it is defensible, monotone in
query count, and trivially auditable. Refine later if it matters.

| Statement | Key charged |
|---|---|
| `MI x, y AT d`, `CMI x, y GIVEN d` | pair key `x×y` |
| `TRACK v`, `RANK v`, `COMPARE ALL v`, `PAIRWISE d ON v`, `ALERT ... ON v` | `v` |
| `SURPRISE a UNDER b` (no `ON`, scalar table per variable) | each schema variable |
| `STRUCTURE`, `COMPARE STRUCTURE`, `CORRELATIONS`, `NEAREST` | `__database__` |
| `profile` tool (dataset card: per-variable entropy scalars + MI/drift scalars) | one flat scalar per schema variable → each key `v`; charged once per session |

`__database__` is the reserved key for scalar releases not attributable to a
single variable.

### 3. Free — metadata

`STATS`, `SCHEMA`, `DIMENSIONS`, and the `help`/`schema`/`stats`/
`list_dimensions` tools are uncharged. They reveal structure (names, types,
cardinalities), not data. This is a scoping choice, documented here so it is
not mistaken for an oversight: schema knowledge is assumed public within the
session.

`SUGGEST` and the `suggest` tool are also free: they release ranked query
text and advisory scores (expected bits, ledger cost, budget fit), not
distributions — the information they surface is of the same class as schema
metadata. Executing a suggested query is charged as usual.

### `EXPORT` wrappers

`EXPORT <stmt> AS JSON|CSV` is charged exactly as `<stmt>` — same information,
different serialization.

## Rules

- **Charge on success only.** A query that errors (parse error, missing
  distribution) charges nothing.
- **Identical queries are charged once.** The query fingerprint is the query
  text with whitespace collapsed; a fingerprint already charged in this
  session re-runs for free (the information was already released). Trivially
  different spellings of the same question (reordered filters, case changes
  in keywords) count as new queries — the dedup is conservative in the
  cheap direction.
- **Refusal before release.** If the would-be charge pushes the session over
  budget, the tool returns a structured JSON refusal
  (`refused: true`, with `query_cost_bits`, `spent_bits`, `budget_bits`,
  `remaining_bits`) instead of the result, and nothing is charged. This is a
  normal tool result, not a protocol error, so the agent can re-plan.
- **The `ledger` tool is free** and returns the session's spend per key,
  total, budget, and remaining bits.

## Limitations (read this)

- Entropy of the released distribution is an *upper bound on new information
  only for the first release*; correlated queries (overlapping slices,
  marginals of an already-released joint) are charged independently and can
  double-count — the ledger over-charges there, which is the safe direction
  for a budget, but do not read the total as "exact bits an adversary
  learned".
- Conversely, an adversary combining many scalar answers can reconstruct
  more than the flat scalar charges suggest (e.g. many `NEAREST`/`PAIRWISE`
  calls triangulating a distribution). The flat charge bounds honest use,
  not adaptive attacks.
- The ledger is per **session** (per server process). Restarting the server
  resets it. Opening a different database does not reset it.
- No noise is added anywhere. Small or high-cardinality slices remain
  revealing; pair the budget with `--min-cell-count` and `--readonly`.
