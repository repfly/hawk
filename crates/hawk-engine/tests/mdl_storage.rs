use std::path::PathBuf;

use hawk_engine::core::{
    dimension_key_from_pairs, DimensionDefinition, DistributionRepr, JointRepr,
    VariableDefinition, VariableType,
};
use hawk_engine::query::QueryEngine;
use hawk_engine::sql::{executor, parser};
use hawk_engine::storage::{Database, OpenMode};

fn temp_db_dir(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("hawk-mdl-{}-{}", name, std::process::id()))
}

fn build_db(root: &PathBuf) -> Database {
    if root.exists() {
        std::fs::remove_dir_all(root).expect("remove existing db dir");
    }
    let mut db = Database::create(root).expect("create db");

    db.define_variable(VariableDefinition {
        name: "category".to_owned(),
        var_type: VariableType::Categorical {
            categories: vec!["big".to_owned(), "rare".to_owned()],
            allow_unknown: true,
        },
    })
    .expect("define category");
    db.define_variable(VariableDefinition {
        name: "sentiment".to_owned(),
        var_type: VariableType::Continuous {
            bins: 16,
            range: Some((0.0, 1.0)),
        },
    })
    .expect("define sentiment");
    db.define_dimension(DimensionDefinition {
        name: "time".to_owned(),
        source_column: "time".to_owned(),
        granularity: None,
    })
    .expect("define time");
    db.define_joint("category", "sentiment").expect("define joint");

    let key = dimension_key_from_pairs([("time", "2024")]);
    db.update_distribution("category", &key, |dist| {
        dist.repr.increment_categorical(Some(0), 5_000).unwrap();
        dist.repr.increment_categorical(Some(1), 1).unwrap();
    })
    .expect("update category");
    db.update_distribution("sentiment", &key, |dist| {
        // Single spike: coarser bins lose no entropy.
        dist.repr.increment_histogram(3, 1_000);
    })
    .expect("update sentiment");

    // Near-independent joint with few samples: MI bits cannot pay for bytes.
    db.ensure_joint_distribution("category", "sentiment", &key)
        .expect("ensure joint");
    let joint = db
        .get_joint_distribution_mut("category", "sentiment", &key)
        .expect("joint exists");
    if let JointRepr::ConditionalHistograms {
        histograms,
        total_count,
        ..
    } = &mut joint.repr
    {
        for hist in histograms.iter_mut() {
            hist.increment_histogram(3, 10);
        }
        *total_count = histograms
            .iter()
            .map(DistributionRepr::total_count)
            .sum();
    }

    db
}

#[test]
fn audit_storage_reports_every_object_and_mutates_nothing() {
    let root = temp_db_dir("audit");
    let db = build_db(&root);
    let engine = QueryEngine::default();

    let key = dimension_key_from_pairs([("time", "2024")]);
    let versions_before: Vec<u64> = ["category", "sentiment"]
        .iter()
        .map(|v| db.get_distribution(v, &key).unwrap().version)
        .collect();
    let snapshots_before = db.snapshots_for("category", &key).len();

    let stmt = parser::parse("AUDIT STORAGE").expect("parse audit");
    let result = executor::execute(&db, &engine, &stmt).expect("execute audit");

    assert_eq!(
        result.header,
        vec!["Object", "Size", "Information", "Recommendation"]
    );
    let text = result.to_string();
    assert!(text.contains("category @ time:2024"));
    assert!(text.contains("sentiment @ time:2024"));
    assert!(text.contains("joint category×sentiment @ time:2024"));
    assert!(text.contains("snapshots"));
    assert!(text.contains("Total size"));
    assert!(text.contains("Candidate savings"));
    // Rare category carries ~0 bits; spiky histogram over-resolves; tiny
    // joint cannot pay for its bytes.
    assert!(text.contains("fold candidates"));
    assert!(text.contains("rebin 16 → "));
    assert!(text.contains("candidate to drop; ESTIMATE would recover it within"));

    // Advisory only: nothing changed.
    let versions_after: Vec<u64> = ["category", "sentiment"]
        .iter()
        .map(|v| db.get_distribution(v, &key).unwrap().version)
        .collect();
    assert_eq!(versions_before, versions_after);
    assert_eq!(db.snapshots_for("category", &key).len(), snapshots_before);
}

#[test]
fn audit_storage_works_read_only_and_exports() {
    let root = temp_db_dir("audit-ro");
    let mut db = build_db(&root);
    db.close().expect("close");
    drop(db);

    let db = Database::open(&root, OpenMode::ReadOnly).expect("reopen read-only");
    let engine = QueryEngine::default();

    let stmt = parser::parse("EXPORT AUDIT STORAGE AS JSON").expect("parse export audit");
    let result = executor::execute(&db, &engine, &stmt).expect("execute export audit");
    let json = &result.rows[0][0];
    assert!(json.contains("\"Object\""));
    assert!(json.contains("Total size"));

    let stmt = parser::parse("EXPORT AUDIT STORAGE AS CSV").expect("parse export audit csv");
    let result = executor::execute(&db, &engine, &stmt).expect("execute export audit csv");
    assert!(result.rows[0][0].starts_with("Object,Size,Information,Recommendation"));
}

#[test]
fn compact_snapshots_survives_reopen_with_invariants() {
    let root = temp_db_dir("compact");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove existing db dir");
    }
    let mut db = Database::create(&root).expect("create db");
    db.define_variable(VariableDefinition {
        name: "category".to_owned(),
        var_type: VariableType::Categorical {
            categories: vec!["a".to_owned(), "b".to_owned()],
            allow_unknown: true,
        },
    })
    .expect("define category");
    db.define_dimension(DimensionDefinition {
        name: "time".to_owned(),
        source_column: "time".to_owned(),
        granularity: None,
    })
    .expect("define time");

    let key = dimension_key_from_pairs([("time", "2024")]);
    // Each update snapshots the pre-update state: the sequence is
    // [empty, [50,50], [51,50], [51,51]] — only [51,50] is redundant
    // (near-identical to both temporal neighbors).
    db.update_distribution("category", &key, |dist| {
        dist.repr.increment_categorical(Some(0), 50).unwrap();
        dist.repr.increment_categorical(Some(1), 50).unwrap();
    })
    .expect("seed category");
    let increments: [(usize, u64); 3] = [(0, 1), (1, 1), (0, 1)];
    for (idx, by) in increments {
        db.update_distribution("category", &key, |dist| {
            dist.repr.increment_categorical(Some(idx), by).unwrap();
        })
        .expect("update category");
    }
    // Sequence check before compaction.
    let before = db.snapshots_for("category", &key);
    assert_eq!(before.len(), 4);

    let removed = db.compact_snapshots(0.01).expect("compact");
    assert_eq!(removed, 1);

    db.close().expect("close");
    drop(db);

    let reopened = Database::open(&root, OpenMode::ReadWrite).expect("reopen");
    let after = reopened.snapshots_for("category", &key);
    assert_eq!(after.len(), 3);
    // First and last snapshots always survive.
    assert_eq!(after.first().unwrap().version, before.first().unwrap().version);
    assert_eq!(after.last().unwrap().version, before.last().unwrap().version);
    // Live distribution untouched.
    let dist = reopened
        .get_distribution("category", &key)
        .expect("distribution survives");
    assert_eq!(dist.sample_count, 103);

    // Idempotent at the same epsilon.
    let mut reopened = reopened;
    assert_eq!(reopened.compact_snapshots(0.01).expect("recompact"), 0);
}

#[test]
fn compact_snapshots_requires_write_mode() {
    let root = temp_db_dir("compact-ro");
    let mut db = build_db(&root);
    db.close().expect("close");
    drop(db);

    let mut db = Database::open(&root, OpenMode::ReadOnly).expect("reopen read-only");
    assert!(db.compact_snapshots(0.01).is_err());
}
