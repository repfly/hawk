# ESTIMATE: max-entropy answers with honesty in bits

`ESTIMATE` answers a joint question that was never stored: "how do these two
variables co-vary here?" — reconstructed from their stored marginals by
maximum entropy, reported with hard per-cell bounds and an explicit account,
in bits, of what the database does *not* know.

```
ESTIMATE <var_a>, <var_b> AT <dim:val>
EXPORT ESTIMATE <var_a>, <var_b> AT <dim:val> AS JSON|CSV
```

## How the estimate is built

1. **Marginals.** Both variables' stored distributions are resolved at the
   slice (exact dimension key first, otherwise aggregated over all stored
   slices containing the requested dimensions). Continuous variables
   participate via their histogram bins — each bin label is a category.
2. **Iterative proportional fitting (IPF).** Starting from a uniform table,
   rows and columns are alternately rescaled to the target marginals until
   the table changes by less than an L1 tolerance (1e-10, capped at 1000
   sweeps). With only two marginals as constraints the max-entropy solution
   is exactly the independence product `p_a ⊗ p_b`, so IPF converges in one
   sweep — the general loop exists so stored joints can join the constraint
   set later.
3. **Fréchet bounds.** Whatever the true joint is, each cell must satisfy
   `max(0, p_i + q_j − 1) ≤ P(i, j) ≤ min(p_i, q_j)`. These bounds are
   reported per cell; when both marginals are heavy the lower bound is
   strictly positive — some co-occurrence is *forced* by the marginals alone.

## Missing information, in bits

The number that makes ESTIMATE honest:

```
missing_information_bits = min(H(A), H(B))
```

Derivation: the max-ent joint is the independence product, so its entropy is
`H(A) + H(B)`. Any joint with these marginals has entropy between
`max(H(A), H(B))` (one variable fully determined by the other) and
`H(A) + H(B)` (independence). The gap between the max-ent entropy and the
lowest achievable joint entropy is therefore

```
H(A) + H(B) − max(H(A), H(B)) = min(H(A), H(B))
```

— which is exactly the **maximum mutual information the marginals permit**:
the bits about the dependency that the database does not know. Relatedly, the
MI of the estimate itself is 0 (independence), and the true MI is bounded by
`0 ≤ MI ≤ min(H(A), H(B))` (a valid, if not always tight, upper bound).

| Field | Meaning |
|---|---|
| Banner | `ESTIMATED — not observed` or `OBSERVED — stored joint`. |
| `Missing Information` | Bits about the dependency the DB does not know; 0 when observed. |
| `MI` | MI of the reported table, with lower/upper bound columns `[0, min(H(A), H(B))]`. |
| `Joint Entropy` | Entropy of the reported table; `H(A)+H(B)` for a pure estimate. |
| Cells | `a × b` with probability and Fréchet `[lower, upper]`, ranked by probability. |
| `IPF` | Sweeps to convergence (estimate path only). |

## Stored joints are preferred

If a stored joint exists for the pair at the slice (exact key, or aggregated
across matching slices, as in `STRUCTURE`), the executor reports **it** —
bannered `OBSERVED — stored joint`, with `missing_information_bits = 0` and
MI computed from the observed table. Fréchet bounds are still shown for
reference (the observed cells always sit inside them). Nothing is ever
silently substituted: the banner states which world you are in.

## MI fallback

`MI a, b AT <dim:val>` on a pair with **no stored joint** used to be an
error. By default it now falls back to the max-ent estimate and says so:

```
MI                    0.0000 bits
Estimated             true
Warning               estimate from marginals — MI lower bound 0; true MI ≤ 1.0000 bits
Missing Information   1.0000 bits
```

The fallback never silently returns 0 — the warning and bound always ride
along. Disable it with `QueryEngine::with_mi_estimate_fallback(false)` to get
the hard error back.

## Display vs export

The interactive table caps the cell listing at the top 20 cells by
probability (grids can be large); `EXPORT ESTIMATE ... AS JSON|CSV` emits the
full grid.

## Edge cases

- **Zero-mass marginals** (no samples at the slice) are an error, not a
  fabricated uniform answer.
- **Mismatched category sets** cannot arise inside one database (the schema
  fixes each variable's categories); the `__unknown__` bucket participates as
  a regular category and simply carries 0 mass when unused.
- **Categorical × continuous** and **continuous × continuous** pairs work via
  histogram bin labels.
- Nothing is persisted; the estimate is computed at query time.

See `crates/hawk-engine/examples/estimate_reconstruction.rs` for the
accuracy + honesty demo: a stored joint's MI versus the estimate built from
marginals alone, with the missing bits called out.
