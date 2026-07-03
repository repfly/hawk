pub const HAWK_SQL_HELP: &str = r#"Hawk SQL Query Reference
=========================

COMPARE <var> BETWEEN <dim:val> AND <dim:val>
  Compare a variable's distribution between two dimension values.
  Returns JSD, KL divergence, Hellinger, PSI, Wasserstein, entropy, and top movers.
  Example: COMPARE price BETWEEN region:US AND region:EU

COMPARE ALL <var> OVER <dim>
  Compare a variable across all values of a dimension.
  Example: COMPARE ALL sentiment OVER topic

EXPLAIN <dim:val> VS <dim:val>
  Decompose divergence between two references across all variables.
  Shows which variables contribute most to the difference.
  Example: EXPLAIN time:2023 VS time:2024

SURPRISE <dim:val> UNDER <dim:val> [ON <var>]
  How surprising is slice A's data under slice B's stored model, in bits.
  Returns cross-entropy H(A,B), excess bits KL(A||B), baseline entropy H(B),
  unseen-mass warning, and a "Top Surprises" table of per-bucket contributions.
  Without ON <var>, scores every variable and ranks them by excess bits.
  Also works wrapped: EXPORT SURPRISE ... AS JSON|CSV.
  Example: SURPRISE time:2025-06 UNDER time:2025-05 ON category

STRUCTURE AT <dim:val>
  Chow-Liu dependency tree over all variables at a dimension slice, built
  from pairwise mutual information of stored joints only. Returns the edge
  list ranked by MI plus total retained information (sum of edge MI, bits).
  Pairs with no stored joint are listed separately as unknown, never treated
  as zero; missing joints can make the result a forest (reported as such).
  Also works wrapped: EXPORT STRUCTURE ... AS JSON|CSV.
  Example: STRUCTURE AT time:2024

COMPARE STRUCTURE BETWEEN <dim:val> AND <dim:val>
  Structural diff of the dependency trees at two slices: added edges,
  dropped edges, re-weighted edges (same edge, MI delta), an MI-weighted
  rewiring score in [0,1], and a "retained information changed by X bits"
  headline. Shows how variable relationships rewired even when marginals
  barely move.
  Example: COMPARE STRUCTURE BETWEEN time:2023 AND time:2024

ESTIMATE <var_a>, <var_b> AT <dim:val>
  Reconstruct the joint distribution of two variables at a slice by maximum
  entropy from their stored marginals (iterative proportional fitting), even
  when that joint was never stored. Returns the estimated table (top cells by
  probability), per-cell Frechet bounds [max(0, p+q-1), min(p, q)], the MI of
  the estimate with an upper bound min(H(A), H(B)), and
  missing_information_bits — the bits about the dependency the database does
  NOT know — under a prominent "ESTIMATED — not observed" banner. If a stored
  joint exists at the slice it is preferred and bannered
  "OBSERVED — stored joint" (missing information 0). Interactive output caps
  the cell listing; EXPORT ESTIMATE ... AS JSON|CSV emits the full grid.
  Example: ESTIMATE plan, churned AT time:2025-Q1

TRACK <var> FROM <dim:val> [GRANULARITY <g>]
  Track distribution drift over a dimension with entropy timeline.
  Example: TRACK price FROM region:US GRANULARITY monthly

SHOW <var> AT <dim:val> [TOP <n>] [BOTTOM <n>]
  Show the distribution of a variable at a specific reference.
  Example: SHOW category AT time:2024 TOP 10

RANK <var> BY ENTROPY OVER <dim>
  Rank dimension values by entropy for a variable.
  Example: RANK sentiment BY ENTROPY OVER topic

MI <var_a>, <var_b> AT <dim:val>
  Mutual information between two variables at a reference. When no stored
  joint exists for the pair, MI falls back to the max-entropy ESTIMATE and
  the result carries an explicit estimated:true warning ("estimate from
  marginals — MI lower bound 0; true MI <= X bits") instead of erroring.
  Example: MI price, category AT region:US

CMI <var_a>, <var_b> GIVEN <dim>
  Conditional mutual information given a dimension.
  Example: CMI price, sentiment GIVEN region

CORRELATIONS [OVER <dim>] [LIMIT <n>]
  Find the most correlated variable pairs.
  Example: CORRELATIONS OVER topic LIMIT 20

PAIRWISE <dim> ON <var> [USING jsd|hellinger|psi]
  Pairwise distance matrix between dimension values.
  Example: PAIRWISE region ON price USING hellinger

NEAREST <dim:val> ON <dim> [LIMIT <n>] [USING jsd|hellinger|psi]
  Find nearest neighbors to a reference.
  Example: NEAREST topic:politics ON topic LIMIT 5

AUDIT STORAGE
  Advisory MDL (minimum description length) report over everything stored:
  per object (marginal, joint, snapshot store) the approximate on-disk cost,
  the information it retains in bits, and a recommendation — e.g. a joint
  whose dependency information (samples x MI) is smaller than its serialized
  size is flagged "candidate to drop; ESTIMATE would recover it within X
  bits/sample"; histograms get a cheaper rebinning suggestion when coarser
  bins lose ~no entropy; categories carrying ~0 bits are fold candidates.
  Footer: total size, total candidate savings, and how many snapshots are
  redundant (JSD to both temporal neighbors below epsilon). Read-only —
  it never modifies anything. Also works wrapped:
  EXPORT AUDIT STORAGE AS JSON|CSV.
  Example: AUDIT STORAGE

SUGGEST [LIMIT <n>]
  Rank candidate next queries by expected information gain, in bits
  (default limit 10). Candidates: the highest-entropy variables not yet
  explored (SHOW, score = H(var)); the dimension that explains the most
  about a variable (COMPARE ... ACROSS, score = the entropy drop between the
  pooled distribution and the sample-weighted per-slice entropies, i.e.
  MI(var; dim)); and unstored variable pairs with the widest dependency
  uncertainty (ESTIMATE, score = min(H(A), H(B)) missing bits). Ordering is
  deterministic (score descending, ties lexicographic). Free — it releases
  ranked query text and scores, not data. Prefer the dedicated 'suggest'
  MCP tool: it deduplicates against this session's history and annotates
  each suggestion with its ledger cost and whether it fits the remaining
  bit budget. Pair it with the 'profile' tool (one-call dataset card) to
  orient before exploring. Also works wrapped: EXPORT SUGGEST ... AS JSON|CSV.
  Example: SUGGEST LIMIT 5

STATS
  Show database statistics (distribution count, samples, variables, dimensions).

SCHEMA
  Show the database schema (variables with types, dimensions, joints).

DIMENSIONS [<name>]
  List dimension values. Optionally filter by dimension name.

Guardrails
----------
Small-cell suppression: when the server runs with --min-cell-count <k>,
categories with fewer than k samples are folded into __unknown__ on every
query result, and all metrics are computed after folding. Storage is
untouched.

Information ledger: every successful query is charged in bits to a
per-session ledger (revealing a distribution costs its entropy; a scalar
metric costs a small flat charge; STATS/SCHEMA/DIMENSIONS are free;
identical queries are charged once). Call the 'ledger' tool to see spend
per variable, total, budget, and remaining bits. When the server runs with
--bit-budget <bits>, a query that would exceed the budget returns a JSON
refusal ({"refused": true, ...}) instead of results — ask a cheaper
question (scalar metrics cost less than full distributions).

Guided exploration: the 'profile' tool returns a one-call dataset card
(variables with entropies, dimensions, top associations by MI, biggest
recent drift, storage summary; charged one flat scalar per variable, once
per session), and the 'suggest' tool ranks the next queries to run by
expected information gain, deduplicated against this session's history and
annotated with ledger cost and budget fit. Orientation loop: profile →
suggest → query → suggest → ...
"#;
