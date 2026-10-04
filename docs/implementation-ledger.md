# Bits-native implementation ledger

The six phases follow `PLAN.md`. The initial implementation exists in commit
`f0f00aa`; this pass verifies acceptance, completes gaps, and commits each phase.
No repository agent profiles were found. Planner, phase implementers, batch
reviewers, and architect use synthesized role-specific instructions.

| Phase | Outcome / owned subsystem | Dependencies | Acceptance checks | Status | Review batch |
| --- | --- | --- | --- | --- | --- |
| 1 | SURPRISE math, query, ingest, alerts, docs and example | Existing distributions | Math/DSL/export/ingest tests; corrupt-batch demo | Implemented | 1 |
| 2 | STRUCTURE MI matrix, trees, structural diff, surfaces | 1 checkpoint | Unknown joints, deterministic trees, export, rewiring demo | Implemented | 1 |
| 3 | ESTIMATE IPF, bounds, missing bits, planner | 2 checkpoint | Degenerate/property tests; observed preference; fallback/export demo | Pending | 1 |
| 4 | Suppression and MCP disclosure ledger | 3 | Query/export suppression; budget/refusal/session tests; docs/demo | Pending | 1 |
| 5 | MDL advisory, auto-binning, snapshot compaction | 3, 4 checkpoint | MDL tests; reopen/invariants; measured storage demo | Pending | 1 |
| 6 | SUGGEST and PROFILE exploration | 3, 4, 5 checkpoint | Ranking/history/tool tests; profile plus five budgeted rounds | Pending | 2 |

Commits proceed in phase order. Disjoint implementation work can overlap; shared
SQL and MCP surfaces are assigned sequentially.
Each phase has a distinct implementation subagent. Batch 1 reviews phases 1–5;
batch 2 reviews phase 6. All major findings must be fixed before a fresh final
architect review.

The plan's explicit exclusions remain: optional tree persistence and optional
suggestion generator are deferred; suppression settings remain runtime-only;
PROFILE remains MCP-only. No persisted payload changes are planned.

## Findings

| Severity | Owner | Evidence / criterion | Status / review provenance |
| --- | --- | --- | --- |
| Major | Phase 1 | Original example bypasses ingest-time scoring; checkpoint 1 needs stable/corrupted batches | Resolved by fixture-based ingest demo and regression; batch review pending |
| Major | Phase 4 | `state.rs` swap/mutation retains release fingerprints; CLI accepts invalid budgets | Open, guardrails implementer audit |
| Major | Phase 5 | Storage benchmark documents procedure without measured before/after results | Open, planner |
| Major | Phase 6 | Guided example omits PROFILE and actual MCP disclosure budget | Open, planner |

## Verification and final gate

Baseline: `cargo test --workspace --all-features` passed (228 tests); strict
workspace/all-target Clippy passed. Formatting had existing failures, assigned
to relevant phases. Architect verdict: pending.

### Phase 1

Completed actual news-fixture ingest demo and stable/corrupted batch assertions;
clarified pre-call baseline and new-slice behavior. Added empty/disjoint math and
batch/export/alert regression coverage. No persisted change. Tests: 12 surprisal
unit tests and one batch integration test passed; example passed with 35.804
excess bits/sample and 100% unseen mass for the corrupted batch.

### Phase 2

Verified reversed joint lookup, partial aggregation, disconnected unknown forest,
and JSON/CSV metric roundtrips in two standalone integration tests. Existing
structure/parser/tokenizer (11), Chow–Liu (7), MI matrix (4), and 10k structure
integration checks passed. Asserted demo confirms unchanged marginals and a
0.6532 rewiring score; README now demonstrates this result. T2.6 stays deferred.
