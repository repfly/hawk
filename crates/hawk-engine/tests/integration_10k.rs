use std::path::PathBuf;

use hawk_engine::core::{DimensionDefinition, VariableDefinition, VariableType};
use hawk_engine::ingest::{IngestMapping, IngestOptions, IngestionPipeline};
use hawk_engine::query::QueryEngine;
use hawk_engine::storage::Database;

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/community_notes_10k.csv")
}

fn temp_db(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("hawk-int10k-{}-{}", name, std::process::id()))
}

fn create_test_db(path: &std::path::Path) -> Database {
    if path.exists() {
        std::fs::remove_dir_all(path).unwrap();
    }
    let mut db = Database::create(path).expect("create db");

    db.define_variable(VariableDefinition {
        name: "sentiment".into(),
        var_type: VariableType::Continuous {
            bins: 50,
            range: Some((-1.0, 1.0)),
        },
    })
    .unwrap();

    db.define_variable(VariableDefinition {
        name: "leaning".into(),
        var_type: VariableType::Categorical {
            categories: vec!["left".into(), "center".into(), "right".into()],
            allow_unknown: true,
        },
    })
    .unwrap();

    db.define_dimension(DimensionDefinition {
        name: "topic".into(),
        source_column: "topic_label".into(),
        granularity: None,
    })
    .unwrap();

    db.define_dimension(DimensionDefinition {
        name: "time".into(),
        source_column: "created_at".into(),
        granularity: Some("monthly".into()),
    })
    .unwrap();

    db.define_joint("sentiment", "leaning").unwrap();

    db
}

fn ingest(db: &mut Database) {
    let mut mapping = IngestMapping::default();
    mapping.variables.insert("sentiment_score".into(), "sentiment".into());
    mapping.variables.insert("political_leaning".into(), "leaning".into());
    mapping.dimensions.insert("topic_label".into(), "topic".into());
    mapping.dimensions.insert("created_at".into(), "time".into());

    let report = IngestionPipeline::ingest_file(
        db,
        fixture_path(),
        &mapping,
        IngestOptions {
            batch_size: 1_000,
            show_progress: false,
        },
    )
    .expect("ingest");

    assert_eq!(report.total_rows, 10_000);
    assert_eq!(report.processed_rows, 10_000);
    println!(
        "Ingested {} rows, {} distributions updated in {}ms",
        report.processed_rows, report.distributions_updated, report.elapsed_ms
    );
}

#[test]
fn ingest_10k_rows() {
    let root = temp_db("ingest");
    let mut db = create_test_db(&root);
    ingest(&mut db);

    let stats = db.stats();
    assert!(stats.distributions > 0);
    assert!(stats.total_samples > 0);
    println!("Stats: {} distributions, {} total samples", stats.distributions, stats.total_samples);
}

#[test]
fn compare_topics() {
    let root = temp_db("compare");
    let mut db = create_test_db(&root);
    ingest(&mut db);

    let qe = QueryEngine::default();

    let cmp = qe
        .compare(&db, "topic:russia-ukraine/variable:sentiment", "topic:climate-change/variable:sentiment", None)
        .expect("compare sentiment");

    println!("russia-ukraine vs climate-change (sentiment):");
    println!("  JSD = {:.6}", cmp.jsd);
    println!("  KL(A->B) = {:.6}, KL(B->A) = {:.6}", cmp.kl_a_to_b, cmp.kl_b_to_a);
    println!("  Entropy A = {:.4}, B = {:.4}", cmp.entropy_a, cmp.entropy_b);
    println!("  Wasserstein = {:.6}", cmp.wasserstein.unwrap_or(0.0));
    println!("  Samples: A={}, B={}", cmp.sample_count_a, cmp.sample_count_b);

    assert!(cmp.jsd >= 0.0 && cmp.jsd <= 1.0);
    assert!(cmp.sample_count_a > 100);
    assert!(cmp.sample_count_b > 100);

    let cmp_leaning = qe
        .compare(&db, "topic:us-elections/variable:leaning", "topic:ai-regulation/variable:leaning", None)
        .expect("compare leaning");

    println!("\nus-elections vs ai-regulation (leaning):");
    println!("  JSD = {:.6}", cmp_leaning.jsd);
    assert!(cmp_leaning.jsd >= 0.0);
}

#[test]
fn explain_divergence() {
    let root = temp_db("explain");
    let mut db = create_test_db(&root);
    ingest(&mut db);

    let qe = QueryEngine::default();
    let explained = qe
        .explain(&db, "topic:russia-ukraine", "topic:immigration")
        .expect("explain");

    println!("Explain russia-ukraine vs immigration:");
    println!("  Total divergence = {:.6}", explained.total_divergence);
    for c in &explained.contributions {
        println!("  {} — JSD={:.6} fraction={:.2}%", c.variable, c.jsd, c.fraction * 100.0);
    }

    assert!(explained.total_divergence >= 0.0);
    assert_eq!(explained.contributions.len(), 2);
    let frac_sum: f64 = explained.contributions.iter().map(|c| c.fraction).sum();
    assert!((frac_sum - 1.0).abs() < 1e-9 || explained.total_divergence == 0.0);
}

#[test]
fn track_over_time() {
    let root = temp_db("track");
    let mut db = create_test_db(&root);
    ingest(&mut db);

    let qe = QueryEngine::default();
    let track = qe
        .track(&db, "topic:russia-ukraine/variable:sentiment", Some("2023-01"), Some("2025-06"), Some("monthly"))
        .expect("track");

    println!("Track russia-ukraine/sentiment:");
    println!("  Time points: {}", track.time_points.len());
    println!("  Drift events: {}", track.drift_events.len());
    for (t, e) in track.time_points.iter().zip(track.entropy_series.iter()) {
        println!("  {} — entropy={:.4}", t, e);
    }

    assert!(track.time_points.len() > 5);
    assert_eq!(track.time_points.len(), track.entropy_series.len());
}

#[test]
fn pairwise_matrix() {
    let root = temp_db("pairwise");
    let mut db = create_test_db(&root);
    ingest(&mut db);

    let qe = QueryEngine::default();
    let (labels, matrix) = qe
        .pairwise(&db, "topic", "sentiment", "jsd")
        .expect("pairwise");

    println!("Pairwise JSD matrix (sentiment by topic):");
    print!("{:>20}", "");
    for l in &labels {
        print!("{:>18}", l);
    }
    println!();
    for (i, row) in matrix.iter().enumerate() {
        print!("{:>20}", labels[i]);
        for val in row {
            print!("{:>18.6}", val);
        }
        println!();
    }

    assert_eq!(labels.len(), matrix.len());
    for i in 0..labels.len() {
        assert!((matrix[i][i]).abs() < 1e-12, "diagonal should be 0");
        for j in 0..labels.len() {
            assert!((matrix[i][j] - matrix[j][i]).abs() < 1e-12, "should be symmetric");
        }
    }
}
