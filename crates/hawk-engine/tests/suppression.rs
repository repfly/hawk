use std::collections::HashMap;
use std::path::PathBuf;

use hawk_engine::core::{
    dimension_key_from_pairs, DimensionDefinition, VariableDefinition, VariableType,
};
use hawk_engine::ingest::batch_updater::apply_batch;
use hawk_engine::ingest::column_mapper::MappedRow;
use hawk_engine::query::QueryEngine;
use hawk_engine::storage::Database;
use serde_json::Value;

const K: u64 = 5;

fn temp_db(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("hawk-suppression-{}-{}", name, std::process::id()))
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

fn row(month: &str, category: &str, flag: &str, channel: &str) -> MappedRow {
    let mut variable_values = HashMap::new();
    variable_values.insert("category".to_owned(), Value::from(category));
    variable_values.insert("flag".to_owned(), Value::from(flag));
    variable_values.insert("channel".to_owned(), Value::from(channel));

    let mut dimension_values = HashMap::new();
    dimension_values.insert("time".to_owned(), month.to_owned());

    MappedRow {
        variable_values,
        dimension_values,
    }
}

/// Two months of data where "niche" stays below K samples in every slice.
/// category×flag has a stored joint; category×channel does not.
fn build_db(root: &std::path::Path) -> Database {
    let _ = std::fs::remove_dir_all(root);
    let mut db = Database::create_with_options(root, false).expect("create db");

    db.define_variable(categorical(
        "category",
        &["news", "sports", "niche", "tiny"],
    ))
    .unwrap();
    db.define_variable(categorical("flag", &["yes", "no"]))
        .unwrap();
    db.define_variable(categorical("channel", &["mobile", "desktop"]))
        .unwrap();
    db.define_dimension(DimensionDefinition {
        name: "time".to_owned(),
        source_column: "time".to_owned(),
        granularity: None,
    })
    .unwrap();
    db.define_joint("category", "flag").unwrap();

    let mut rows = Vec::new();
    let mut push = |month: &str, category: &str, n: usize| {
        for i in 0..n {
            let flag = if i % 2 == 0 { "yes" } else { "no" };
            let channel = if i % 3 == 0 { "mobile" } else { "desktop" };
            rows.push(row(month, category, flag, channel));
        }
    };
    push("2025-01", "news", 50);
    push("2025-01", "sports", 45);
    push("2025-01", "niche", 2);
    push("2025-01", "tiny", 3);
    push("2025-02", "news", 40);
    push("2025-02", "sports", 55);
    push("2025-02", "niche", 3);
    push("2025-02", "tiny", 1);

    let schema = db.schema().clone();
    apply_batch(&mut db, &schema, &rows).expect("apply batch");
    db
}

fn suppressed_engine() -> QueryEngine {
    QueryEngine::default().with_min_cell_count(Some(K))
}

fn run(db: &Database, engine: &QueryEngine, sql: &str) -> String {
    hawk_engine::sql::query(db, engine, sql)
        .unwrap_or_else(|e| panic!("query '{}' failed: {}", sql, e))
        .to_string()
}

#[test]
fn disabled_by_default_nothing_changes() {
    let root = temp_db("default-off");
    let db = build_db(&root);
    let engine = QueryEngine::default();

    assert_eq!(engine.min_cell_count(), None);
    for sql in [
        "SHOW category AT time:2025-01",
        "COMPARE category BETWEEN time:2025-01 AND time:2025-02",
        "EXPLAIN time:2025-01 VS time:2025-02",
        "SURPRISE time:2025-02 UNDER time:2025-01 ON category",
        "ESTIMATE category, flag AT time:2025-01",
        "ESTIMATE category, channel AT time:2025-01",
        "EXPORT DISTRIBUTION category AT time:2025-01 AS JSON",
    ] {
        let out = run(&db, &engine, sql);
        assert!(out.contains("niche"), "'{}' must show niche when off", sql);
    }
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn suppressed_category_never_appears_in_any_output_path() {
    let root = temp_db("all-paths");
    let db = build_db(&root);
    let engine = suppressed_engine();

    for sql in [
        "SHOW category AT time:2025-01",
        "SHOW category AT time:2025-01 TOP 10",
        "COMPARE category BETWEEN time:2025-01 AND time:2025-02",
        "EXPLAIN time:2025-01 VS time:2025-02",
        "SURPRISE time:2025-02 UNDER time:2025-01 ON category",
        "SURPRISE time:2025-02 UNDER time:2025-01",
        // Observed grid (stored joint) and max-ent grid (no stored joint).
        "ESTIMATE category, flag AT time:2025-01",
        "ESTIMATE category, channel AT time:2025-01",
        "EXPORT DISTRIBUTION category AT time:2025-01 AS JSON",
        "EXPORT SHOW category AT time:2025-01 AS JSON",
        "EXPORT COMPARE category BETWEEN time:2025-01 AND time:2025-02 AS CSV",
        "EXPORT ESTIMATE category, flag AT time:2025-01 AS JSON",
    ] {
        let out = run(&db, &engine, sql);
        assert!(!out.contains("niche"), "'{}' leaked niche:\n{}", sql, out);
    }

    // The folded mass lands in the unknown bucket.
    let show = run(&db, &engine, "SHOW category AT time:2025-01");
    assert!(show.contains("__unknown__"));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn released_metrics_are_computed_after_folding() {
    let root = temp_db("metrics");
    let db = build_db(&root);
    let engine = suppressed_engine();

    // SHOW's entropy equals the entropy of the folded distribution.
    let key = dimension_key_from_pairs([("time", "2025-01")]);
    let mut released = db
        .get_distribution("category", &key)
        .expect("stored distribution")
        .clone();
    released.suppress_small_cells(K);
    let out = hawk_engine::sql::query(&db, &engine, "SHOW category AT time:2025-01").unwrap();
    assert_eq!(out.rows[0][1], format!("{:.4} bits", released.entropy));

    // Divergence metrics move once the rare categories fold together.
    let unfolded = QueryEngine::default()
        .compare(&db, "time:2025-01", "time:2025-02", Some("category"))
        .unwrap();
    let folded = engine
        .compare(&db, "time:2025-01", "time:2025-02", Some("category"))
        .unwrap();
    assert_ne!(folded.jsd, unfolded.jsd);
    assert_ne!(folded.entropy_a, unfolded.entropy_a);

    // The observed ESTIMATE grid folds the rare row and keeps total mass.
    let est = engine
        .estimate(&db, "category", "flag", "time:2025-01")
        .unwrap();
    assert!(est.observed);
    assert!(est.cells.iter().all(|c| c.label_a != "niche"));
    assert!(est.cells.iter().any(|c| c.label_a == "__unknown__"));
    let mass: f64 = est.cells.iter().map(|c| c.probability).sum();
    assert!((mass - 1.0).abs() < 1e-9);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn storage_is_never_touched_by_suppression() {
    let root = temp_db("storage");
    let db = build_db(&root);
    let engine = suppressed_engine();

    let _ = run(&db, &engine, "SHOW category AT time:2025-01");
    let _ = run(&db, &engine, "ESTIMATE category, flag AT time:2025-01");

    let key = dimension_key_from_pairs([("time", "2025-01")]);
    let stored = db
        .get_distribution("category", &key)
        .expect("stored distribution");
    let labels = stored
        .repr
        .categorical_labels_with_unknown()
        .expect("categorical");
    assert!(labels.iter().any(|l| l == "niche"));
    let _ = std::fs::remove_dir_all(root);
}
