# SURPRISE: surprisal scoring

`SURPRISE` answers "how surprising is this data under that model?" — in bits,
computed entirely from two stored distributions. No raw rows, no format change.

```
SURPRISE <dim:val> UNDER <dim:val> [ON <variable>]
```

Slice A (before `UNDER`) supplies the *data*; slice B supplies the *model*.
Hawk reports the cross-entropy

> H(A, B) = −Σ p_A · log₂ q_B

the average number of bits per sample needed to encode A's data with a code
built for B. Alongside it:

| Field | Meaning |
|---|---|
| `Cross-Entropy H(A,B)` | Bits per sample to encode A under B's model. |
| `Total Bits` | Cross-entropy × A's sample count. |
| `Excess Bits KL(A‖B)` | H(A,B) − H(A): the bits *wasted* by using B's model instead of A's own. Zero iff the distributions match. |
| `Entropy(A)` / `Entropy(B)` | A's own entropy and the baseline model entropy. |
| `Unseen Mass` | Probability mass of A falling on buckets with zero count in B. |
| `Top Surprises` | Per-category / per-bin contributions, ranked by excess bits. |

Without `ON <variable>`, every variable in the schema is scored and ranked by
excess bits — a one-query "what changed the most" check.

```
SURPRISE time:2025-06 UNDER time:2025-05
SURPRISE time:2025-06 UNDER time:2025-05 ON category
EXPORT SURPRISE time:2025-06 UNDER time:2025-05 ON category AS JSON
```

## Edge-case semantics

- **Unseen categories.** Buckets present in A but with zero count in B get
  additive smoothing `q = (c + ε) / (total + k·ε)` with `ε = 1e-10` (the same
  epsilon as `KL` in `COMPARE`), so surprisal stays finite. The mass A places
  on such buckets is reported separately as an unseen-mass warning.
- **Histogram range mismatch.** Both histograms are rebinned to a common
  range and bin count before scoring (same alignment as `COMPARE`).
- **`__unknown__` buckets** participate like any other category.
- **Empty A** yields a zero report; **empty B** acts as a know-nothing model
  (smoothing makes it uniform, so H(A,B) ≈ log₂ k).

Note: this excess-bits definition is KL(A‖B) = H(A,B) − H(A); the number is
cross-checked against `math/kl_divergence.rs` in the unit tests.

## Ingest-time scoring

Batch ingest can score the arriving data against the pre-batch stored model —
the database reacting to data as it arrives:

```rust
let report = IngestionPipeline::ingest_file(
    &mut db,
    path,
    &mapping,
    IngestOptions { surprisal_report: true, ..IngestOptions::default() },
)?;
for s in &report.surprisal {
    println!("{} @ {}: {:.3} excess bits", s.variable, s.dimension_key, s.result.excess_bits);
}
```

The report is per updated `(variable, dimension slice)`, ranked by excess bits,
and is not persisted. Slices with no pre-batch model are skipped. Default off.

## Alerting

`surprisal` is an alert metric: for each consecutive pair of time slices it
measures the excess bits of the newer slice's data under the previous slice's
model.

```
ALERT WHEN surprisal > 0.5 ON category FROM time:2025-01
```

See `crates/hawk-engine/examples/surprise_scoring.rs` for a runnable demo.
