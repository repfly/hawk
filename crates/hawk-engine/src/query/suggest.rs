use std::collections::{HashMap, HashSet};

use anyhow::Result;

use crate::math::entropy;
use crate::storage::Database;

use crate::query::planner::resolve_distribution;
use crate::query::result_types::Suggestion;
use crate::query::structure::resolve_joint_counts;

pub const DEFAULT_SUGGEST_LIMIT: usize = 10;

/// Session history that suggestions are deduplicated against. The MCP layer
/// feeds it from the session Ledger (charged fingerprints + released ledger
/// keys); the CLI/DSL path passes an empty default.
#[derive(Debug, Clone, Default)]
pub struct SuggestContext {
    /// Normalized (whitespace-collapsed) query texts already executed.
    executed: HashSet<String>,
    /// Ledger keys already released: variable names and canonical pair keys.
    explored: HashSet<String>,
}

impl SuggestContext {
    pub fn new(
        executed_queries: impl IntoIterator<Item = String>,
        released_keys: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            executed: executed_queries
                .into_iter()
                .map(|q| normalize_query(&q))
                .collect(),
            explored: released_keys.into_iter().collect(),
        }
    }

    pub fn record_query(&mut self, query: &str) {
        self.executed.insert(normalize_query(query));
    }

    /// Mark a ledger key (variable name or `pair_key`) as already released.
    pub fn record_released_key(&mut self, key: impl Into<String>) {
        self.explored.insert(key.into());
    }
}

/// Query text with whitespace collapsed — matches the MCP ledger fingerprint.
pub fn normalize_query(query: &str) -> String {
    query.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Canonical ledger key for a variable pair — matches the MCP ledger's key.
pub fn pair_key(var_a: &str, var_b: &str) -> String {
    if var_a <= var_b {
        format!("{}×{}", var_a, var_b)
    } else {
        format!("{}×{}", var_b, var_a)
    }
}

/// Rank candidate next queries by expected information gain, in bits.
///
/// Generators:
/// (a) highest-entropy variables not yet explored → `SHOW <var> AT <slice>`
///     (score = pooled H(var));
/// (b) the dimension that explains the most about a variable — channel
///     capacity proxy MI(var; dim) = H(pooled) − Σ (n_s/N)·H(var | slice s),
///     from stored per-slice marginals → `COMPARE <var> ACROSS <dim>`;
/// (c) unstored variable pairs with the widest dependency uncertainty —
///     missing_information_bits = min(H(A), H(B)) → `ESTIMATE a, b AT <slice>`.
///
/// Scores use stored (pre-suppression) distributions — only scalar scores are
/// released. Suggestions whose query was already executed are filtered;
/// ordering is deterministic (score desc, then lexicographic query).
pub fn execute_suggest(
    db: &Database,
    ctx: &SuggestContext,
    limit: usize,
) -> Result<Vec<Suggestion>> {
    let schema = db.schema();
    let slice = default_slice(db);

    // Pooled entropy per variable, shared by generators (a) and (c).
    let mut pooled: HashMap<String, f64> = HashMap::new();
    for var in &schema.variables {
        if let Ok(dist) = resolve_distribution(db, &var.name, &HashMap::new(), None) {
            if dist.sample_count > 0 {
                pooled.insert(var.name.clone(), dist.entropy);
            }
        }
    }

    let mut out: Vec<Suggestion> = Vec::new();

    // (a) highest-entropy variables not yet explored this session.
    if let Some((dim, val)) = &slice {
        for var in &schema.variables {
            if ctx.explored.contains(&var.name) {
                continue;
            }
            let Some(&h) = pooled.get(&var.name) else {
                continue;
            };
            out.push(Suggestion {
                query: format!("SHOW {} AT {}:{}", var.name, dim, val),
                rationale: format!(
                    "{} carries {:.2} bits of entropy and has not been explored this session",
                    var.name, h
                ),
                expected_bits: h,
            });
        }
    }

    // (b) dimension with the largest entropy drop onto a variable.
    for var in &schema.variables {
        for dim in &schema.dimensions {
            let Some((pooled_h, mi)) = dimension_mi(db, &var.name, &dim.name) else {
                continue;
            };
            out.push(Suggestion {
                query: format!("COMPARE {} ACROSS {}", var.name, dim.name),
                rationale: format!(
                    "{} has {:.2} bits of entropy; {} explains {:.2} of them",
                    var.name, pooled_h, dim.name, mi
                ),
                expected_bits: mi,
            });
        }
    }

    // (c) unstored pairs: the dependency the database knows nothing about.
    if let Some((dim, val)) = &slice {
        let names: Vec<&str> = schema.variables.iter().map(|v| v.name.as_str()).collect();
        for i in 0..names.len() {
            for j in (i + 1)..names.len() {
                let (a, b) = if names[i] <= names[j] {
                    (names[i], names[j])
                } else {
                    (names[j], names[i])
                };
                if ctx.explored.contains(&pair_key(a, b)) {
                    continue;
                }
                if resolve_joint_counts(db, a, b, &HashMap::new()).is_some() {
                    continue;
                }
                let (Some(&h_a), Some(&h_b)) = (pooled.get(a), pooled.get(b)) else {
                    continue;
                };
                let missing = h_a.min(h_b);
                out.push(Suggestion {
                    query: format!("ESTIMATE {}, {} AT {}:{}", a, b, dim, val),
                    rationale: format!(
                        "no stored joint for {} × {}; up to {:.2} bits of dependency \
                         information are unknown (min(H(A), H(B)))",
                        a, b, missing
                    ),
                    expected_bits: missing,
                });
            }
        }
    }

    // Dedup against history, then deterministic ordering.
    out.retain(|s| !ctx.executed.contains(&normalize_query(&s.query)));
    out.sort_by(|x, y| {
        y.expected_bits
            .total_cmp(&x.expected_bits)
            .then_with(|| x.query.cmp(&y.query))
    });
    out.truncate(limit);
    Ok(out)
}

/// Default slice for generated queries: the first schema dimension that has
/// values, at its greatest value (for time dimensions: the latest slice).
fn default_slice(db: &Database) -> Option<(String, String)> {
    db.schema().dimensions.iter().find_map(|d| {
        db.dimension_values(&d.name)
            .into_iter()
            .next_back()
            .map(|v| (d.name.clone(), v))
    })
}

/// (pooled entropy, MI(variable; dimension)) from stored per-slice marginals:
/// the entropy drop between the pooled distribution and the sample-weighted
/// average of per-slice entropies. None with fewer than 2 populated slices.
fn dimension_mi(db: &Database, variable: &str, dimension: &str) -> Option<(f64, f64)> {
    let mut slices: Vec<(Vec<u64>, u64)> = Vec::new();
    for value in db.dimension_values(dimension) {
        let mut dims = HashMap::new();
        dims.insert(dimension.to_owned(), value);
        let Ok(dist) = resolve_distribution(db, variable, &dims, None) else {
            continue;
        };
        if dist.sample_count > 0 {
            slices.push((dist.repr.value_count_vector(), dist.sample_count));
        }
    }
    if slices.len() < 2 {
        return None;
    }
    // Schema fixes the vector shape per variable; skip anything odd.
    let width = slices[0].0.len();
    if slices.iter().any(|(c, _)| c.len() != width) {
        return None;
    }

    let total: u64 = slices.iter().map(|(_, n)| n).sum();
    let mut pooled = vec![0u64; width];
    let mut conditional_h = 0.0;
    for (counts, n) in &slices {
        for (slot, c) in pooled.iter_mut().zip(counts) {
            *slot += c;
        }
        conditional_h += (*n as f64 / total as f64) * entropy(counts, *n);
    }
    let pooled_h = entropy(&pooled, total);
    Some((pooled_h, (pooled_h - conditional_h).max(0.0)))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use serde_json::Value;

    use crate::core::{DimensionDefinition, VariableDefinition, VariableType};
    use crate::ingest::batch_updater::apply_batch;
    use crate::ingest::column_mapper::MappedRow;
    use crate::query::QueryEngine;
    use crate::storage::Database;

    use super::{execute_suggest, pair_key, SuggestContext};

    fn temp_db(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("hawk-suggest-test-{}-{}", name, std::process::id()))
    }

    fn categorical(name: &str, categories: &[&str]) -> VariableDefinition {
        VariableDefinition {
            name: name.to_owned(),
            var_type: VariableType::Categorical {
                categories: categories.iter().map(|c| (*c).to_owned()).collect(),
                allow_unknown: false,
            },
        }
    }

    fn row(month: &str, values: &[(&str, &str)]) -> MappedRow {
        let mut variable_values = HashMap::new();
        for (var, val) in values {
            variable_values.insert((*var).to_owned(), Value::from(*val));
        }
        let mut dimension_values = HashMap::new();
        dimension_values.insert("time".to_owned(), month.to_owned());
        MappedRow {
            variable_values,
            dimension_values,
        }
    }

    /// Three variables over two months:
    /// - `wide`: 4 uniform categories (H = 2 bits), stable over time;
    /// - `narrow`: heavily skewed 2 categories (H « 1 bit), stable;
    /// - `driven`: flips completely between months (time explains ~1 bit).
    ///
    /// One stored joint (narrow × wide); pairs with `driven` are unstored.
    fn build_db(root: &std::path::Path) -> Database {
        let _ = std::fs::remove_dir_all(root);
        let mut db = Database::create_with_options(root, false).expect("create db");

        db.define_variable(categorical("wide", &["a", "b", "c", "d"]))
            .unwrap();
        db.define_variable(categorical("narrow", &["x", "y"]))
            .unwrap();
        db.define_variable(categorical("driven", &["up", "down"]))
            .unwrap();
        db.define_dimension(DimensionDefinition {
            name: "time".to_owned(),
            source_column: "time".to_owned(),
            granularity: None,
        })
        .unwrap();
        db.define_joint("narrow", "wide").unwrap();

        let mut rows = Vec::new();
        for month in ["2025-01", "2025-02"] {
            let driven = if month == "2025-01" { "up" } else { "down" };
            for (i, wide) in ["a", "b", "c", "d"].iter().enumerate() {
                for k in 0..8 {
                    let narrow = if i == 0 && k == 0 { "y" } else { "x" };
                    rows.push(row(
                        month,
                        &[("wide", wide), ("narrow", narrow), ("driven", driven)],
                    ));
                }
            }
        }
        let schema = db.schema().clone();
        apply_batch(&mut db, &schema, &rows).expect("apply batch");
        db
    }

    #[test]
    fn show_suggestions_rank_high_entropy_unexplored_first() {
        let root = temp_db("show-rank");
        let db = build_db(&root);

        let suggestions = execute_suggest(&db, &SuggestContext::default(), 100).unwrap();
        let shows: Vec<&str> = suggestions
            .iter()
            .filter(|s| s.query.starts_with("SHOW"))
            .map(|s| s.query.as_str())
            .collect();
        // Default slice = last value of the first dimension (latest month).
        assert_eq!(shows[0], "SHOW wide AT time:2025-02");
        let wide = suggestions
            .iter()
            .find(|s| s.query == "SHOW wide AT time:2025-02")
            .unwrap();
        assert!(
            (wide.expected_bits - 2.0).abs() < 0.1,
            "{}",
            wide.expected_bits
        );
        let narrow = suggestions
            .iter()
            .find(|s| s.query == "SHOW narrow AT time:2025-02")
            .unwrap();
        assert!(wide.expected_bits > narrow.expected_bits);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn explored_variables_are_not_suggested_for_show() {
        let root = temp_db("explored");
        let db = build_db(&root);

        let mut ctx = SuggestContext::default();
        ctx.record_released_key("wide");
        let suggestions = execute_suggest(&db, &ctx, 100).unwrap();
        assert!(!suggestions.iter().any(|s| s.query.starts_with("SHOW wide")));
        // Other generators for `wide` are unaffected.
        assert!(suggestions
            .iter()
            .any(|s| s.query == "COMPARE wide ACROSS time"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn dimension_mi_generator_scores_entropy_drop() {
        let root = temp_db("dim-mi");
        let db = build_db(&root);

        let suggestions = execute_suggest(&db, &SuggestContext::default(), 100).unwrap();
        // `driven` flips with time: pooled H = 1 bit, per-slice H = 0.
        let driven = suggestions
            .iter()
            .find(|s| s.query == "COMPARE driven ACROSS time")
            .unwrap();
        assert!((driven.expected_bits - 1.0).abs() < 1e-9);
        assert!(driven.rationale.contains("time explains"));
        // `wide` is stable over time: time explains ~0 bits of it.
        let wide = suggestions
            .iter()
            .find(|s| s.query == "COMPARE wide ACROSS time")
            .unwrap();
        assert!(wide.expected_bits.abs() < 1e-9);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn unstored_pairs_suggest_estimate_with_missing_bits() {
        let root = temp_db("unstored");
        let db = build_db(&root);

        let suggestions = execute_suggest(&db, &SuggestContext::default(), 100).unwrap();
        // The stored pair is never suggested for ESTIMATE.
        assert!(!suggestions
            .iter()
            .any(|s| s.query.starts_with("ESTIMATE narrow, wide")));
        // Unstored pairs are, scored min(H(A), H(B)) — H(driven) = 1 bit.
        let est = suggestions
            .iter()
            .find(|s| s.query == "ESTIMATE driven, wide AT time:2025-02")
            .unwrap();
        assert!((est.expected_bits - 1.0).abs() < 1e-9);
        assert!(est.rationale.contains("no stored joint"));
        assert!(suggestions
            .iter()
            .any(|s| s.query == "ESTIMATE driven, narrow AT time:2025-02"));

        // A released pair key drops the ESTIMATE suggestion.
        let mut ctx = SuggestContext::default();
        ctx.record_released_key(pair_key("wide", "driven"));
        let after = execute_suggest(&db, &ctx, 100).unwrap();
        assert!(!after
            .iter()
            .any(|s| s.query == "ESTIMATE driven, wide AT time:2025-02"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn executed_queries_are_filtered() {
        let root = temp_db("dedup");
        let db = build_db(&root);

        let first = execute_suggest(&db, &SuggestContext::default(), 100).unwrap();
        let top = first[0].query.clone();
        let mut ctx = SuggestContext::default();
        // Whitespace differences do not defeat the dedup.
        ctx.record_query(&top.replace(' ', "   "));
        let second = execute_suggest(&db, &ctx, 100).unwrap();
        assert!(!second.iter().any(|s| s.query == top));
        assert_eq!(second.len(), first.len() - 1);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn ordering_is_deterministic_and_limit_applies() {
        let root = temp_db("determinism");
        let db = build_db(&root);

        let a = execute_suggest(&db, &SuggestContext::default(), 100).unwrap();
        let b = execute_suggest(&db, &SuggestContext::default(), 100).unwrap();
        let queries_a: Vec<&str> = a.iter().map(|s| s.query.as_str()).collect();
        let queries_b: Vec<&str> = b.iter().map(|s| s.query.as_str()).collect();
        assert_eq!(queries_a, queries_b);
        // Score descending; ties broken lexicographically by query.
        for w in a.windows(2) {
            assert!(
                w[0].expected_bits > w[1].expected_bits
                    || (w[0].expected_bits == w[1].expected_bits && w[0].query < w[1].query)
            );
        }
        let capped = execute_suggest(&db, &SuggestContext::default(), 3).unwrap();
        assert_eq!(capped.len(), 3);
        assert_eq!(capped[0].query, a[0].query);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn suggest_verb_runs_through_the_sql_pipeline() {
        let root = temp_db("sql-verb");
        let db = build_db(&root);
        let engine = QueryEngine::default();

        let out = crate::sql::query(&db, &engine, "SUGGEST LIMIT 2").unwrap();
        assert_eq!(
            out.header,
            vec!["Suggested Query", "Expected Bits", "Rationale"]
        );
        assert_eq!(out.rows.len(), 2);
        assert!(out.rows[0][0].starts_with("SHOW wide"));

        let export = crate::sql::query(&db, &engine, "EXPORT SUGGEST LIMIT 2 AS JSON").unwrap();
        assert!(export.rows[0][0].contains("Suggested Query"));
        let _ = std::fs::remove_dir_all(root);
    }
}
