# MDL storage: compression as schema

Hawk treats representation choices as information decisions: every stored
object should earn its bytes with the bits of information it retains. The
minimum-description-length (MDL) machinery makes that trade explicit in
three places — an advisory `AUDIT STORAGE` report, opt-in MDL auto-binning
at schema inference, and an explicit snapshot GC by information distance.

```
AUDIT STORAGE
EXPORT AUDIT STORAGE AS JSON|CSV
```

## The cost model

All scores compare **storage bits** (description-length cost) against
**information bits** (what the bytes buy):

| Object | Storage cost | Information retained |
|---|---|---|
| Histogram bin | 64 bits (one u64 count) | Entropy captured at that bin resolution, bits/sample |
| Categorical entry | 64-bit length prefix + label bytes + 64-bit count | The category's entropy contribution `n × (−p·log₂ p)`, total bits |
| Joint distribution | Serialized size (bincode serialize-and-measure) | Dependency information `n × MI`, total bits |
| Snapshot | Its share of the snapshot file | Its JSD distance (bits) to its temporal neighbors |

The pure scoring functions live in `crates/hawk-engine/src/math/mdl.rs` and
touch no storage; sizes are measured at the call site.

## AUDIT STORAGE (advisory only)

`AUDIT STORAGE` walks every stored object and reports approximate on-disk
cost, information retained in bits, and a recommendation. It is strictly
read-only: it never rebins, folds, drops, or compacts anything.

- **Histograms** — coarser rebinnings (successive halvings, via
  `math/rebin.rs`) are scored with the two-part description length
  `k × 64 + n × (H_current − H_k)`. When a coarser resolution wins, the
  report suggests it: `rebin 64 → 16 bins: saves ~384 B, loses 0.0021
  bits/sample`. Otherwise: `resolution earns its bytes`.
- **Categoricals** — categories with tiny probability mass (≤ 1%) whose
  entropy contribution in total bits is below their label+count storage cost
  are listed as fold candidates for `__unknown__`. Dominant categories are
  never candidates even when the variable is near-deterministic — folding
  the majority label would destroy the released distribution's meaning.
- **Joints** — a stored joint earns its bytes iff its dependency information
  `n × MI` exceeds its serialized size in bits. Both numbers are reported.
  A failing joint is flagged `candidate to drop; ESTIMATE would recover it
  within X bits/sample`, where X is `KL(stored ‖ max-ent reconstruction from
  its marginals)` — computed with Epic 3's IPF, and equal to the joint's MI
  up to numerics, because the max-ent joint of two marginals is their
  independence product.
- **Snapshots** — one line reports how many snapshots are redundant at the
  default epsilon (`N snapshots redundant at eps=0.01 bits`), i.e. what
  `compact_snapshots` would reclaim.

The footer sums total size and total candidate savings. Savings are
estimates of the serialized payload affected, not exact file deltas (the
files are zstd-compressed).

## MDL auto-binning at schema inference (opt-in)

`InferConfig::mdl_binning` (default **off**) replaces the fixed 20-bin
default for inferred continuous variables with an MDL choice over the
candidate set `{4, 8, 16, 32, 64, 128}` (powers of two, so every candidate
is an exact coarsening of the finer ones):

```
choose k minimizing  k × 64 + n × (H_ref − H_k)
```

where `H_k` is the sample entropy at k equal-width bins and `H_ref` the
entropy at the finest candidate. The scan is ascending and a candidate must
beat the incumbent by more than 1e-9 bits, so ties break toward fewer bins.
The choice is deterministic for a given sample. Strongly bimodal data gets
enough bins to separate its modes; near-constant data gets the minimum
candidate. With the flag off, inference is byte-identical to previous
behavior.

```rust
let config = InferConfig { mdl_binning: true, ..InferConfig::default() };
IngestionPipeline::ingest_file_auto(&mut db, path, config, options)?;
```

## Snapshot GC by information distance (explicit, opt-in)

```rust
let removed = db.compact_snapshots(epsilon_bits)?; // e.g. 0.01
db.flush()?; // or close()
```

Walks each variable/slice's snapshot sequence in time order and drops a
snapshot when its Jensen–Shannon divergence (base 2, so at most 1 bit) to
**both** temporal neighbors — the previously kept snapshot and the next
original one — is below `epsilon_bits`. Such a snapshot is reconstructible
to within epsilon from either neighbor.

Invariants:

- The **first and last** snapshot of every sequence are always kept.
- Snapshots whose shape differs from a neighbor's are never dropped.
- Deletion removes entries from the existing structure only; the persisted
  format is unchanged (no format-version bump), and a compacted database
  reopens on any build that reads the current format.

Compaction is never wired into ingest paths — it runs only when explicitly
called. Changes persist on the next `flush()`/`close()`.

## Measuring the effect

See the "MDL audit" subsection of [benchmarks.md](benchmarks.md) for the
procedure to measure database size before/after compaction on a real
dataset.
