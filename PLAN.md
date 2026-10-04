# Hawk: Bits-Native Roadmap

Vision: a row database answers *what is in the data*; Hawk answers *what the data
knows* — how much, about what, and how that is changing — with every answer
denominated in bits. Ingest is scored in bits, answers carry their missing
information in bits, storage is managed in bits, and access is metered in bits.

Epics are ordered by leverage. Each epic ships independently and ends with an
iteration checkpoint: run the demo, decide whether to continue, reorder, or cut.

> **Status (2026-07-03): all six epics implemented** (T2.6 stretch and T6.1(d)
> optional generator skipped by design; PROFILE is MCP-only per plan). Suppression
> config is runtime-only, not in meta.edb — positional bincode makes additive
> serde fields break old files, so persisting it needs a format-version bump.

Verb-pipeline touchpoints (every new DSL verb touches all of these):
`sql/tokenizer.rs` → `Statement` in `sql/parser.rs` → `sql/executor.rs` →
`sql/formatter.rs` → `hawk-mcp/src/help_text.rs` → `docs/` → an example.

---

## Epic 1 — SURPRISE: surprisal scoring (no format change)

Goal: `SURPRISE <dim:val> UNDER <dim:val> [ON <variable>]` — how surprising is
slice A's data under slice B's stored model, in bits. Computed entirely from two
stored distributions (cross-entropy H(A,B) = -Σ p_A·log₂ p_B), so no storage or
format changes. Entropy collapse and surprisal spikes become one-query checks.
Excess bits = KL(A‖B) = H(A,B) − H(A) (identity: H(A,B) = H(A) + KL(A‖B)).

- [x] **T1.1** `math/surprisal.rs`: cross-entropy between two `DistributionRepr`s.
      Handle: categories in A unseen in B (smoothing policy — additive with
      documented epsilon, and report the unseen-mass separately), histogram
      range mismatch (reuse `math/rebin.rs`), `__unknown__` buckets, empty
      distributions. Unit tests for each edge case. Also return per-category
      surprisal contributions (drives the "top surprises" table).
- [x] **T1.2** Result type in `query/result_types.rs`: total bits, bits/sample,
      baseline entropy H(B), excess bits (cross-entropy − H(A) = KL(A‖B), reuse
      `math/kl_divergence.rs` for cross-check in tests), top contributors,
      sample counts, unseen-mass warning.
- [x] **T1.3** DSL wiring: `SURPRISE` keyword in tokenizer, `Statement::Surprise
      { ref_a, ref_b, variable: Option<String> }` in parser (no variable = score
      all variables, ranked by excess bits). Parser tests alongside the existing
      statement tests.
- [x] **T1.4** Executor + formatter: table output mirroring COMPARE's style
      (metric rows + "Top Surprises" section). `EXPORT SURPRISE ... AS JSON|CSV`
      must work via the existing `Export` wrapper.
- [x] **T1.5** Ingest hook (opt-in flag on batch ingest in `ingest/pipeline.rs`):
      after building the batch's distributions, score them under the current
      stored model and emit the surprisal report to the ingest result — the
      "database reacts to data as it arrives" moment. No persistence yet.
- [x] **T1.6** Surface: `hawk-mcp` help_text + a dedicated `surprise` note in
      `docs/mcp.md`; docs section; `crates/hawk-engine/examples/surprise_scoring.rs`
      using the existing news dataset fixture.
- [x] **T1.7** `ALERT WHEN surprisal > <bits> ON <var>` — extend the existing
      Alert statement's metric set so surprisal plugs into the alerting path.

**Checkpoint 1**: demo = ingest the news dataset month by month, watch surprisal
per batch; inject a corrupted batch and see it light up. Decide: proceed to
STRUCTURE or iterate on SURPRISE ergonomics.

---

## Epic 2 — STRUCTURE: dependency trees and structural drift

Goal: `STRUCTURE AT <dim:val>` returns the Chow-Liu tree (best tree-shaped
dependency model) built from pairwise MI; `COMPARE STRUCTURE BETWEEN a AND b`
reports how variable *relationships* rewired — drift nobody else can query.

- [ ] **T2.1** `math/mi_matrix.rs`: all-pairs MI at a dimension slice from
      *stored* joints (`JointRepr`); pairs without a stored joint are reported
      as unknown, not silently zero. Reuses `math/mutual_info.rs`.
- [ ] **T2.2** `math/chow_liu.rs`: maximum spanning tree (Kruskal) over the MI
      matrix → tree as edge list with MI weights + total retained information
      (Σ edge MI, in bits). Deterministic tie-breaking so trees are comparable.
- [ ] **T2.3** `Statement::Structure { reference }` through the verb pipeline;
      formatter renders the edge list ranked by MI (ASCII tree optional, later).
- [ ] **T2.4** Structural diff: edge-set comparison between two trees — added /
      dropped / re-weighted edges, MI-weighted rewiring score, plus a plain
      "retained information changed by X bits" headline.
      `Statement::CompareStructure { ref_a, ref_b }`.
- [ ] **T2.5** Surface: MCP help_text, docs, example
      (`examples/structural_drift.rs`), EXPORT support.
- [ ] **T2.6** (stretch) Persist the tree per snapshot in `snapshot_store.rs` so
      `TRACK STRUCTURE` becomes possible later. Format-version bump + reopen
      test per `docs/file-format.md` rules — only if cheap; otherwise defer.

**Checkpoint 2**: demo = two time slices of the news dataset where marginals
barely move but an association flips. If the story lands, this is the flagship
README example.

---

## Epic 3 — ESTIMATE: max-entropy answers with honesty in bits

Goal: `ESTIMATE <var_a>, <var_b> AT <dim:val>` — answer joint questions that
were never stored, via maximum-entropy reconstruction from stored marginals and
joints, with explicit missing-information and bounds. The moat epic.

- [ ] **T3.1** `math/ipf.rs`: iterative proportional fitting over the two
      marginals (+ any stored joints sharing a variable, later). Convergence
      criteria, iteration cap, degenerate-input handling. Property tests: when
      the true joint IS stored, IPF over its marginals must match the max-ent
      solution, and stored-joint answers must be preferred by the planner.
- [ ] **T3.2** `math/frechet.rs`: Fréchet bounds per cell → bounds on derived
      metrics (at minimum: bounds on MI estimate).
- [ ] **T3.3** Missing-information accounting: H(maxent joint) − lower bound
      from available info; expose as `missing_information_bits` on every
      ESTIMATE result. This number is the product.
- [ ] **T3.4** Verb pipeline + formatter (estimate table, bounds, missing bits,
      and a clear `ESTIMATED — not observed` banner), MCP, docs, example.
- [ ] **T3.5** Planner integration in `query/planner.rs`: `MI` on an unstored
      pair falls back to ESTIMATE with a warning instead of erroring (behind a
      setting).

**Checkpoint 3**: demo = delete a stored joint, ESTIMATE it, compare against the
real one. Accuracy + honesty story in one plot.

---

## Epic 4 — Guardrails & the information ledger

Goal: make the privacy/agent story native — small-cell suppression and
per-session disclosure metering in bits.

- [ ] **T4.1** Small-cell suppression: configurable min sample count; categories
      below it fold into `__unknown__` on query/export/MCP paths (storage
      untouched). Setting lives in `meta.edb` config; default off, documented.
- [ ] **T4.2** Disclosure accounting model: define and document what "bits
      revealed" means per result type (start simple: entropy of the released
      distribution at released resolution; refine later). Write
      `docs/information-ledger.md` first — the semantics are the hard part.
- [ ] **T4.3** Ledger in `hawk-mcp/src/state.rs`: per-session cumulative bits
      per variable; `--bit-budget <bits>` flag; over-budget queries return a
      structured refusal the agent can reason about.
- [ ] **T4.4** `LEDGER` MCP tool + docs: agent can inspect its own remaining
      budget.

**Checkpoint 4**: demo = an MCP agent explores the news DB on a 40-bit budget
and must choose its queries; pairs with Epic 6.

---

## Epic 5 — MDL storage: compression as schema

Goal: representation choices become information decisions.

- [ ] **T5.1** MDL scoring: description-length cost vs. information retained for
      (a) histogram bin count, (b) category folding, (c) keeping a joint vs.
      relying on ESTIMATE. Pure functions + report first (`AUDIT STORAGE`
      verb: "joint X costs 2.1 KB, saves 0.03 bits — candidate to drop").
- [ ] **T5.2** Advisory → enforcement: opt-in auto-binning at schema inference
      (`ingest/schema_inference.rs`) using MDL.
- [ ] **T5.3** Snapshot GC by information distance: drop snapshots whose JSD to
      neighbors < ε bits (`storage/snapshot_store.rs`); keep-first/keep-last
      invariants; reopen tests + format-version discipline.

**Checkpoint 5**: measure DB size before/after on the news dataset with zero
measurable metric change — the "stores exactly the information, nothing else"
claim, now a benchmark in `docs/benchmarks.md`.

---

## Epic 6 — SUGGEST: information-gain-guided exploration

Goal: the database tells you what to ask next. MCP-first.

- [ ] **T6.1** Expected-information-gain ranking: given session history (which
      slices/variables were viewed), rank candidate queries — highest-entropy
      unexplained variable, dimension with max channel capacity onto it,
      unstored joint with widest Fréchet bounds. Reuses everything above.
- [ ] **T6.2** `suggest` MCP tool returning ranked next queries with rationale
      ("category has 3.6 bits of entropy; time explains 1.2 of them; ask
      COMPARE category ACROSS time").
- [ ] **T6.3** `SUGGEST` DSL verb for CLI/REPL parity.
- [ ] **T6.4** Composite `profile` MCP tool (dataset card: variables, entropies,
      top associations, biggest recent drifts) — the one-call orientation an
      agent needs before SUGGEST is useful.

**Checkpoint 6**: demo = agent connects to an unknown DB, calls `profile`, then
follows `SUGGEST` for five rounds under a Epic-4 bit budget — the self-guiding,
self-limiting statistical oracle, end to end.

---

## Cross-cutting rules

- Every epic: unit tests for math edge cases, parser tests, an executor
  integration test, docs, and an example. Reopen tests whenever a persisted
  payload changes (`docs/file-format.md` rules; bump format version).
- No epic before Epic 5 may change the file format except optional T2.6.
- README gets rewritten only after Checkpoint 2, when SURPRISE + STRUCTURE make
  the bits-native identity demonstrable rather than aspirational.
