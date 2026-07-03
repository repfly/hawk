use std::collections::HashMap;
use std::path::PathBuf;

use hawk_engine::core::{DimensionDefinition, VariableDefinition, VariableType};
use hawk_engine::ingest::batch_updater::apply_batch;
use hawk_engine::ingest::column_mapper::MappedRow;
use hawk_engine::query::QueryEngine;
use hawk_engine::storage::Database;
use serde_json::Value;

fn temp_db(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("hawk-estimate-{}-{}", name, std::process::id()))
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

fn row(quarter: &str, channel: &str, plan: &str, churned: &str, score: f64) -> MappedRow {
    let mut variable_values = HashMap::new();
    variable_values.insert("channel".to_owned(), Value::from(channel));
    variable_values.insert("plan".to_owned(), Value::from(plan));
    variable_values.insert("churned".to_owned(), Value::from(churned));
    variable_values.insert("score".to_owned(), Value::from(score));

    let mut dimension_values = HashMap::new();
    dimension_values.insert("time".to_owned(), quarter.to_owned());

    MappedRow {
        variable_values,
        dimension_values,
    }
}

/// 80 rows per quarter: plan determines churn exactly (free → yes, paid →
/// no); channel is independent of everything; score is uniform. Only
/// plan×churned has a stored joint.
fn build_db(root: &std::path::Path) -> Database {
    let _ = std::fs::remove_dir_all(root);
    let mut db = Database::create_with_options(root, false).expect("create db");

    db.define_variable(categorical("channel", &["mobile", "desktop"]))
        .unwrap();
    db.define_variable(categorical("plan", &["free", "paid"]))
        .unwrap();
    db.define_variable(categorical("churned", &["yes", "no"]))
        .unwrap();
    db.define_variable(VariableDefinition {
        name: "score".to_owned(),
        var_type: VariableType::Continuous {
            bins: 4,
            range: Some((0.0, 1.0)),
        },
    })
    .unwrap();
    db.define_dimension(DimensionDefinition {
        name: "time".to_owned(),
        source_column: "time".to_owned(),
        granularity: None,
    })
    .unwrap();
    db.define_joint("plan", "churned").unwrap();

    let mut rows = Vec::new();
    for channel in ["mobile", "desktop"] {
        for plan in ["free", "paid"] {
            for i in 0..20 {
                let churned = if plan == "free" { "yes" } else { "no" };
                let score = (i % 4) as f64 * 0.25 + 0.1;
                rows.push(row("2025-Q1", channel, plan, churned, score));
            }
        }
    }
    let schema = db.schema().clone();
    apply_batch(&mut db, &schema, &rows).expect("apply batch");
    db
}

#[test]
fn stored_joint_is_preferred_and_flagged_observed() {
    let root = temp_db("observed");
    let db = build_db(&root);
    let qe = QueryEngine::default();

    let est = qe
        .estimate(&db, "plan", "churned", "time:2025-Q1")
        .expect("estimate");
    assert!(est.observed);
    // Canonical order.
    assert_eq!(est.var_a, "churned");
    assert_eq!(est.var_b, "plan");
    assert_eq!(est.missing_information_bits, 0.0);
    // Deterministic dependency with 50/50 marginals: 1 bit of MI.
    assert!((est.mi - 1.0).abs() < 1e-9);
    assert!((est.mi_upper_bound - 1.0).abs() < 1e-9);
    assert_eq!(est.ipf_iterations, 0);

    // Fréchet bounds are still reported and contain every cell.
    assert_eq!(est.cells.len(), 4);
    for c in &est.cells {
        assert!(c.lower_bound <= c.probability + 1e-12);
        assert!(c.probability <= c.upper_bound + 1e-12);
    }
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn unstored_pair_is_estimated_with_missing_bits() {
    let root = temp_db("estimated");
    let db = build_db(&root);
    let qe = QueryEngine::default();

    let est = qe
        .estimate(&db, "channel", "churned", "time:2025-Q1")
        .expect("estimate");
    assert!(!est.observed);
    assert!(est.ipf_converged);
    // Max-ent estimate of two marginals is the independence product: MI 0.
    assert!(est.mi.abs() < 1e-9);
    // Both marginals are 50/50 → 1 bit of missing dependency information.
    assert!((est.missing_information_bits - 1.0).abs() < 1e-9);
    assert!((est.mi_upper_bound - est.missing_information_bits).abs() < 1e-12);
    assert!((est.joint_entropy - 2.0).abs() < 1e-9);

    // The unknown bucket rides along for categoricals: 3×3 grid.
    assert_eq!(est.cells.len(), 9);
    let mass: f64 = est.cells.iter().map(|c| c.probability).sum();
    assert!((mass - 1.0).abs() < 1e-9);
    for c in &est.cells {
        assert!(c.lower_bound <= c.probability + 1e-12);
        assert!(c.probability <= c.upper_bound + 1e-12);
    }
    // Top cells are the four real combinations at 0.25 each.
    assert!((est.cells[0].probability - 0.25).abs() < 1e-9);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn estimate_from_marginals_vs_stored_joint_is_the_honesty_story() {
    let root = temp_db("honesty");
    let db = build_db(&root);
    let qe = QueryEngine::default();

    let stored = qe
        .estimate(&db, "plan", "churned", "time:2025-Q1")
        .expect("stored");
    let est = qe
        .estimate_from_marginals(&db, "plan", "churned", "time:2025-Q1")
        .expect("estimate");

    // Same marginals, but the estimate cannot know the dependency...
    assert!(est.mi.abs() < 1e-9);
    assert!((stored.mi - 1.0).abs() < 1e-9);
    // ...and says exactly how much it does not know.
    assert!((est.missing_information_bits - stored.mi).abs() < 1e-9);
    // The true joint sits inside the estimate's Fréchet bounds.
    for sc in &stored.cells {
        let ec = est
            .cells
            .iter()
            .find(|c| c.label_a == sc.label_a && c.label_b == sc.label_b)
            .expect("matching cell");
        assert!(ec.lower_bound <= sc.probability + 1e-9);
        assert!(sc.probability <= ec.upper_bound + 1e-9);
    }
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn continuous_variables_estimate_over_histogram_bins() {
    let root = temp_db("continuous");
    let db = build_db(&root);
    let qe = QueryEngine::default();

    let est = qe
        .estimate(&db, "score", "churned", "time:2025-Q1")
        .expect("estimate");
    assert!(!est.observed);
    // churned × score: 3 categorical buckets (incl. unknown) × 4 bins.
    assert_eq!(est.var_a, "churned");
    assert_eq!(est.cells.len(), 12);
    assert!(est.cells.iter().any(|c| c.label_b.starts_with('[')));
    let mass: f64 = est.cells.iter().map(|c| c.probability).sum();
    assert!((mass - 1.0).abs() < 1e-9);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn sql_wiring_banner_and_export() {
    let root = temp_db("sql");
    let db = build_db(&root);
    let qe = QueryEngine::default();

    let out = hawk_engine::sql::query(&db, &qe, "ESTIMATE channel, churned AT time:2025-Q1")
        .expect("sql estimate");
    let text = out.to_string();
    assert!(text.contains("ESTIMATED — not observed"));
    assert!(text.contains("Missing Information"));
    assert!(text.contains("Frechet Lower"));

    let out = hawk_engine::sql::query(&db, &qe, "ESTIMATE plan, churned AT time:2025-Q1")
        .expect("sql estimate observed");
    assert!(out.to_string().contains("OBSERVED — stored joint"));

    let exported = hawk_engine::sql::query(
        &db,
        &qe,
        "EXPORT ESTIMATE channel, churned AT time:2025-Q1 AS JSON",
    )
    .expect("export estimate");
    let json = &exported.rows[0][0];
    assert!(json.starts_with('['));
    // Full grid: every cell of the 3×3 categorical grid is present.
    for a in ["mobile", "desktop", "__unknown__"] {
        for b in ["yes", "no", "__unknown__"] {
            assert!(
                json.contains(&format!("{} × {}", a, b)),
                "missing cell {} × {}",
                a,
                b
            );
        }
    }

    let csv = hawk_engine::sql::query(
        &db,
        &qe,
        "EXPORT ESTIMATE channel, churned AT time:2025-Q1 AS CSV",
    )
    .expect("export estimate csv");
    assert!(csv.rows[0][0].starts_with("Metric / Cell,Value,Frechet Lower,Frechet Upper"));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn mi_falls_back_to_estimate_with_warning() {
    let root = temp_db("fallback");
    let db = build_db(&root);

    // Default: fallback on. No stored joint for channel×churned.
    let qe = QueryEngine::default();
    let out = hawk_engine::sql::query(&db, &qe, "MI channel, churned AT time:2025-Q1")
        .expect("mi fallback");
    let text = out.to_string();
    assert!(text.contains("Estimated"));
    assert!(text.contains("estimate from marginals — MI lower bound 0; true MI ≤"));
    assert!(!text.contains("Strength"), "must not pose as a stored MI");

    // A stored pair is untouched by the fallback path.
    let out = hawk_engine::sql::query(&db, &qe, "MI plan, churned AT time:2025-Q1")
        .expect("mi stored");
    assert!(out.to_string().contains("Strength"));

    // Fallback off: the original error surfaces.
    let strict = QueryEngine::default().with_mi_estimate_fallback(false);
    let err = hawk_engine::sql::query(&db, &strict, "MI channel, churned AT time:2025-Q1")
        .expect_err("must error without fallback");
    assert!(err.to_string().contains("no joint distribution defined"));
    let _ = std::fs::remove_dir_all(root);
}
