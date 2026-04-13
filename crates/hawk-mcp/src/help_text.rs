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
  Mutual information between two variables at a reference.
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

STATS
  Show database statistics (distribution count, samples, variables, dimensions).

SCHEMA
  Show the database schema (variables with types, dimensions, joints).

DIMENSIONS [<name>]
  List dimension values. Optionally filter by dimension name.
"#;
