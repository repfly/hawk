# STRUCTURE: dependency trees and structural drift

`STRUCTURE` answers "how do the variables depend on each other here?" — as a
Chow-Liu tree, the best tree-shaped dependency model, built entirely from
stored joint distributions. `COMPARE STRUCTURE` then answers "how did those
relationships *rewire*?" — drift that marginal-level metrics cannot see.

```
STRUCTURE AT <dim:val>
COMPARE STRUCTURE BETWEEN <dim:val> AND <dim:val>
```

## How the tree is built

1. **All-pairs MI.** For every unordered pair of schema variables, Hawk looks
   up the stored joint distribution at the slice and computes mutual
   information (bits). Pairs with **no stored joint are reported as unknown**
   — listed separately, never silently treated as zero MI.
2. **Maximum spanning tree.** Kruskal over the MI graph selects the tree that
   retains the most pairwise information. Tie-breaking is deterministic
   (equal-MI edges are taken in lexicographic edge-name order), so trees
   built from different slices are directly comparable.
3. **Forests.** When missing joints disconnect the graph, the result is a
   forest; the component count is reported rather than hidden.

| Field | Meaning |
|---|---|
| `Edges` | Selected tree edges, ranked by MI descending. |
| `Retained Information` | Σ edge MI, in bits — how much of the joint structure the tree captures. |
| `Shape` | `tree (connected)` or `forest (N components — joints missing)`. |
| `Unknown Pairs` | Variable pairs with no stored joint at the slice. |

## Structural diff

`COMPARE STRUCTURE BETWEEN a AND b` compares the two trees edge-by-edge:

| Field | Meaning |
|---|---|
| `Headline` | "retained information changed by X bits" — retained(B) − retained(A). |
| `Added Edges` | In B's tree but not A's. |
| `Dropped Edges` | In A's tree but not B's. |
| `Re-weighted Edges` | Same edge in both trees; MI delta, sorted by magnitude. |
| `Rewiring Score` | MI-weighted symmetric difference: (Σ MI of dropped + Σ MI of added) / (Σ MI of A's edges + Σ MI of B's edges), in [0, 1]. 0 = no positive-MI mass rewired, 1 = all positive-MI mass rewired. If both trees retain zero bits, the score is 0. |

The point: two slices can have near-identical marginals while an association
flips from one variable pair to another. `COMPARE churned BETWEEN ...` shows
JSD ≈ 0; `COMPARE STRUCTURE BETWEEN ...` lights up.

```
STRUCTURE AT time:2025-Q1
COMPARE STRUCTURE BETWEEN time:2025-Q1 AND time:2025-Q2
EXPORT STRUCTURE AT time:2025-Q1 AS JSON
EXPORT COMPARE STRUCTURE BETWEEN time:2025-Q1 AND time:2025-Q2 AS CSV
```

## Requirements and edge cases

- Joints must be defined (`define_joint()`) and populated for a pair to be
  known; everything else lands in `Unknown Pairs`.
- Zero-MI edges are legitimate tree edges — independent variables still get
  connected (contributing 0 bits) as in standard Chow-Liu.
- At least 2 schema variables are required; with all pairs unknown the
  result is a forest of singletons with every pair listed as unknown.
- Nothing is persisted; both statements are computed from stored joints at
  query time.

## Run the checkpoint demo

```sh
cargo run -p hawk-engine --example structural_drift
```

The controlled example uses two 80-row slices with exactly identical marginals
(JSD = 0 for channel, plan, and churned). In Q1, churn follows plan; in Q2 it
follows channel. Retained information changes from 1.0000 to 0.5310 bits,
with a rewiring score of 0.6532. The example asserts the unchanged marginals
and structural change.

Zero-MI edges still participate in deterministic tie-breaking. Consequently,
channel–churned already exists as a zero-weight edge in Q1 and appears as a
reweighted edge in the diff; churned–plan is dropped. Read added/dropped edges
alongside reweighted edges to see the full change.

Both verbs support JSON and CSV export of the same metric table, including
unknown-pair warnings. JSON is an array of objects with `Metric` and `Value`
keys; values are formatted strings, including units. CSV uses those two
columns. Tree persistence (T2.6) remains deferred; no file-format change is
needed.
