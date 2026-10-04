# SUGGEST + profile: information-gain-guided exploration

`SUGGEST` inverts the query direction: instead of you asking the database,
the database ranks what is worth asking next, scored by expected information
gain in bits. Paired with the MCP `profile` tool (a one-call dataset card),
an agent connecting to an unknown database can orient itself and then follow
the suggestions — the self-guiding loop from the roadmap's Checkpoint 6.

```
SUGGEST [LIMIT <n>]
EXPORT SUGGEST [LIMIT <n>] AS JSON|CSV
```

Default limit is 10. Every suggestion carries a ready-to-run query, a
rationale, and an `expected_bits` score.

## Candidate generators

All scores are computed from what is already stored — suggesting never reads
raw data, and only scalar scores are released (suggestions are free on the
information ledger).

1. **Unexplored entropy** — for each variable not yet explored this session:

   ```
   SHOW <var> AT <slice>        score = H(var)
   ```

   `H(var)` is the variable's entropy pooled over all stored slices. The
   default slice is the first schema dimension at its greatest value (for
   time dimensions: the latest slice).

2. **Explanatory dimension** (channel-capacity proxy) — for each
   (variable, dimension) pair with at least two populated slices:

   ```
   COMPARE <var> ACROSS <dim>   score = MI(var; dim)
   ```

   computed as the entropy drop between the pooled distribution and the
   sample-weighted average of per-slice entropies,
   `H(pooled) − Σ (n_s / N) · H(var | slice s)` — how many of the variable's
   bits the dimension explains, from stored per-slice marginals alone.
   Rationale reads like: *"category has 3.6 bits of entropy; time explains
   1.2 of them"*.

3. **Widest dependency uncertainty** — for each variable pair with **no
   stored joint** anywhere:

   ```
   ESTIMATE <a>, <b> AT <slice> score = min(H(A), H(B))
   ```

   the maximum mutual information the marginals permit — exactly the
   `missing_information_bits` that [ESTIMATE](estimate.md) reports. The
   database is pointing at the largest hole in its own knowledge.

## Deduplication and determinism

- A suggested query that was **already executed** this session is filtered
  out (matching is on whitespace-collapsed query text, the same fingerprint
  the [information ledger](information-ledger.md) uses).
- A variable already **released** (its ledger key was charged) is no longer
  a `SHOW` candidate; a released pair key drops its `ESTIMATE` candidate.
- Ordering is deterministic: score descending, ties broken lexicographically
  by query text. Two calls with the same history return the same list.

The DSL/CLI path carries no session history (`SUGGEST` from the REPL always
ranks from scratch); history-aware ranking lives in the MCP tool below.

## MCP: the `suggest` tool

The MCP server exposes `suggest` (`limit` optional, default 10). It feeds the
ranking with the session ledger's history — charged query fingerprints plus
released variable/pair keys — and annotates every suggestion with its cost
under the ledger's charge model:

```json
[
  {
    "query": "SHOW category AT time:2025-02",
    "rationale": "category carries 3.61 bits of entropy and has not been explored this session",
    "expected_bits": 3.61,
    "cost_bits": 3.61,
    "fits_budget": true
  }
]
```

`expected_bits` is the information-gain score; `cost_bits` is what the query
would charge to the ledger (see [information-ledger.md](information-ledger.md));
`fits_budget` says whether that charge fits the remaining `--bit-budget`.
Calling `suggest` itself charges nothing.

## MCP: the `profile` tool

`profile` is the one-call dataset card an agent needs before `suggest` is
useful — structured JSON with:

| Field | Meaning |
|---|---|
| `variables` | name, type, entropy in bits (pooled over all slices), sample count |
| `dimensions` | name + distinct value count |
| `top_associations` | strongest stored pairs by MI (from stored joints; top 10) |
| `unknown_pairs` | pairs with no stored joint — unknown, never reported as zero |
| `biggest_drift` | largest latest-vs-previous time-slice shift (variable, slices, JSD); `null` without a time dimension |
| `stored_distributions` / `stored_joints` / `total_samples` / `approx_bytes` | storage summary |

Ledger charge: the card releases one entropy scalar per variable plus a
handful of MI/drift scalars, so it is charged **one flat scalar
(`≈ 13.29` bits) per schema variable**, once per session; over budget it
returns the standard `{"refused": true, ...}` refusal and charges nothing.

## The loop

```
profile          → orient: what exists, what is known, what moved
suggest          → ranked next questions, budget-annotated
query <top>      → charged to the ledger as usual
suggest          → the answered question drops out; the next one surfaces
```

Runnable CLI-side demo: `cargo run -p hawk-engine --example
guided_exploration` — five rounds of take-the-top-suggestion, execute,
re-rank.

## Edge cases

- No dimensions with values → generators (1) and (3) have no slice to
  reference and emit nothing; an empty result is reported, not an error.
- A variable with no stored distributions is skipped by every generator.
- A dimension with fewer than two populated slices cannot explain anything
  and is skipped by generator (2); negative rounding artifacts clamp to 0.
- Scores are computed from stored (pre-suppression) distributions; with
  `--min-cell-count` the released *answers* remain suppressed as usual.
