use std::path::PathBuf;

use hawk_engine::core::{DimensionDefinition, VariableDefinition, VariableType};
use hawk_engine::ingest::{IngestMapping, IngestOptions, IngestionPipeline};
use hawk_engine::query::QueryEngine;
use hawk_engine::storage::Database;

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/community_notes_10k.csv")
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
    mapping
        .variables
        .insert("sentiment_score".into(), "sentiment".into());
    mapping
        .variables
        .insert("political_leaning".into(), "leaning".into());
    mapping
        .dimensions
        .insert("topic_label".into(), "topic".into());
    mapping
        .dimensions
        .insert("created_at".into(), "time".into());

    let report = IngestionPipeline::ingest_file(
        db,
        fixture_path(),
        &mapping,
        IngestOptions {
            batch_size: 1_000,
            ..IngestOptions::default()
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
    println!(
        "Stats: {} distributions, {} total samples",
        stats.distributions, stats.total_samples
    );
}

#[test]
fn compare_topics() {
    let root = temp_db("compare");
    let mut db = create_test_db(&root);
    ingest(&mut db);

    let qe = QueryEngine::default();

    let cmp = qe
        .compare(
            &db,
            "topic:russia-ukraine/variable:sentiment",
            "topic:climate-change/variable:sentiment",
            None,
        )
        .expect("compare sentiment");

    println!("russia-ukraine vs climate-change (sentiment):");
    println!("  JSD = {:.6}", cmp.jsd);
    println!(
        "  KL(A->B) = {:.6}, KL(B->A) = {:.6}",
        cmp.kl_a_to_b, cmp.kl_b_to_a
    );
    println!(
        "  Entropy A = {:.4}, B = {:.4}",
        cmp.entropy_a, cmp.entropy_b
    );
    println!("  Wasserstein = {:.6}", cmp.wasserstein.unwrap_or(0.0));
    println!(
        "  Samples: A={}, B={}",
        cmp.sample_count_a, cmp.sample_count_b
    );

    assert!(cmp.jsd >= 0.0 && cmp.jsd <= 1.0);
    assert!(cmp.sample_count_a > 100);
    assert!(cmp.sample_count_b > 100);

    let cmp_leaning = qe
        .compare(
            &db,
            "topic:us-elections/variable:leaning",
            "topic:ai-regulation/variable:leaning",
            None,
        )
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
        println!(
            "  {} — JSD={:.6} fraction={:.2}%",
            c.variable,
            c.jsd,
            c.fraction * 100.0
        );
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
        .track(
            &db,
            "topic:russia-ukraine/variable:sentiment",
            Some("2023-01"),
            Some("2025-06"),
            Some("monthly"),
        )
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
fn surprise_between_topics() {
    let root = temp_db("surprise");
    let mut db = create_test_db(&root);
    ingest(&mut db);

    let qe = QueryEngine::default();

    let results = qe
        .surprise(
            &db,
            "topic:russia-ukraine",
            "topic:climate-change",
            Some("sentiment"),
        )
        .expect("surprise");
    assert_eq!(results.len(), 1);
    let r = &results[0];
    println!(
        "Surprise russia-ukraine under climate-change (sentiment): H(A,B)={:.4}, excess={:.4}",
        r.bits_per_sample, r.excess_bits
    );
    assert!(r.bits_per_sample >= r.entropy_a - 1e-9);
    assert!(r.excess_bits >= -1e-9);
    assert!((r.excess_bits - (r.bits_per_sample - r.entropy_a)).abs() < 1e-9);

    // No variable: all variables scored, ranked by excess bits.
    let all = qe
        .surprise(&db, "topic:russia-ukraine", "topic:climate-change", None)
        .expect("surprise all");
    assert_eq!(all.len(), 2);
    assert!(all[0].excess_bits >= all[1].excess_bits);

    // Self-surprise is just the entropy: zero excess.
    let self_r = &qe
        .surprise(
            &db,
            "topic:russia-ukraine",
            "topic:russia-ukraine",
            Some("leaning"),
        )
        .expect("self surprise")[0];
    assert!(self_r.excess_bits.abs() < 1e-6);

    // SQL wiring, EXPORT wrapper, and ALERT metric.
    let out = hawk_engine::sql::query(
        &db,
        &qe,
        "SURPRISE topic:russia-ukraine UNDER topic:climate-change ON leaning",
    )
    .expect("sql surprise");
    assert!(out.to_string().contains("Excess Bits"));

    let ranked = hawk_engine::sql::query(
        &db,
        &qe,
        "SURPRISE topic:russia-ukraine UNDER topic:climate-change",
    )
    .expect("sql surprise ranked");
    assert_eq!(ranked.rows.len(), 2);

    let exported = hawk_engine::sql::query(
        &db,
        &qe,
        "EXPORT SURPRISE topic:russia-ukraine UNDER topic:climate-change ON leaning AS JSON",
    )
    .expect("export surprise");
    assert!(exported.rows[0][0].starts_with('['));

    let alert = hawk_engine::sql::query(
        &db,
        &qe,
        "ALERT WHEN surprisal > 0.0001 ON sentiment FROM time:2023-01",
    )
    .expect("alert surprisal");
    assert!(
        alert.rows.iter().all(|r| r[0] != "No alerts triggered"),
        "expected surprisal alert hits: {:?}",
        alert.rows
    );
}

#[test]
fn structure_and_structural_diff() {
    let root = temp_db("structure");
    let mut db = create_test_db(&root);
    ingest(&mut db);

    let qe = QueryEngine::default();

    // Only sentiment×leaning has a stored joint; both variables exist, so the
    // tree is a single edge with no unknown pairs.
    let s = qe
        .structure(&db, "topic:russia-ukraine")
        .expect("structure");
    assert_eq!(s.variables, vec!["leaning".to_owned(), "sentiment".to_owned()]);
    assert_eq!(s.edges.len(), 1);
    assert_eq!(s.components, 1);
    assert!(!s.is_forest());
    assert!(s.unknown_pairs.is_empty());
    assert!(s.retained_information >= 0.0);
    assert!(
        (s.retained_information - s.edges.iter().map(|e| e.mi).sum::<f64>()).abs() < 1e-12
    );

    // Diff of a slice against itself: nothing rewired, delta zero.
    let self_diff = qe
        .compare_structure(&db, "topic:russia-ukraine", "topic:russia-ukraine")
        .expect("self diff");
    assert!(self_diff.added_edges.is_empty());
    assert!(self_diff.dropped_edges.is_empty());
    assert_eq!(self_diff.rewiring_score, 0.0);
    assert!(self_diff.retained_information_delta.abs() < 1e-12);

    // Diff between two topics: same single edge, possibly re-weighted.
    let diff = qe
        .compare_structure(&db, "topic:russia-ukraine", "topic:climate-change")
        .expect("diff");
    assert_eq!(diff.reweighted_edges.len(), 1);
    assert!(
        (diff.retained_information_delta
            - (diff.structure_b.retained_information - diff.structure_a.retained_information))
            .abs()
            < 1e-12
    );

    // SQL wiring for both statements plus the EXPORT wrapper.
    let out = hawk_engine::sql::query(&db, &qe, "STRUCTURE AT topic:russia-ukraine")
        .expect("sql structure");
    assert!(out.to_string().contains("Retained Information"));
    assert!(out.to_string().contains("leaning — sentiment"));

    let out = hawk_engine::sql::query(
        &db,
        &qe,
        "COMPARE STRUCTURE BETWEEN topic:russia-ukraine AND topic:climate-change",
    )
    .expect("sql compare structure");
    assert!(out.to_string().contains("retained information changed by"));

    // COMPARE <var> BETWEEN must keep working end-to-end.
    let out = hawk_engine::sql::query(
        &db,
        &qe,
        "COMPARE leaning BETWEEN topic:russia-ukraine AND topic:climate-change",
    )
    .expect("sql compare variable");
    assert!(out.to_string().contains("JSD"));

    let exported = hawk_engine::sql::query(
        &db,
        &qe,
        "EXPORT STRUCTURE AT topic:russia-ukraine AS JSON",
    )
    .expect("export structure");
    assert!(exported.rows[0][0].starts_with('['));

    let exported = hawk_engine::sql::query(
        &db,
        &qe,
        "EXPORT COMPARE STRUCTURE BETWEEN topic:russia-ukraine AND topic:climate-change AS CSV",
    )
    .expect("export compare structure");
    assert!(exported.rows[0][0].starts_with("Metric,Value"));
}

#[test]
fn ingest_surprisal_hook() {
    let root = temp_db("surprisal-hook");
    let mut db = create_test_db(&root);
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/community_notes_small.csv");

    let mut mapping = IngestMapping::default();
    mapping
        .variables
        .insert("sentiment_score".into(), "sentiment".into());
    mapping
        .variables
        .insert("political_leaning".into(), "leaning".into());
    mapping
        .dimensions
        .insert("topic_label".into(), "topic".into());
    mapping
        .dimensions
        .insert("created_at".into(), "time".into());

    // First ingest builds the model; no pre-batch model exists, so even with
    // the flag on there is nothing to score.
    let first = IngestionPipeline::ingest_file(
        &mut db,
        fixture.clone(),
        &mapping,
        IngestOptions {
            surprisal_report: true,
            ..IngestOptions::default()
        },
    )
    .expect("first ingest");
    assert!(first.surprisal.is_empty());

    // Second ingest of the same data: batch matches the model, low excess bits.
    let second = IngestionPipeline::ingest_file(
        &mut db,
        fixture.clone(),
        &mapping,
        IngestOptions {
            surprisal_report: true,
            ..IngestOptions::default()
        },
    )
    .expect("second ingest");
    assert!(!second.surprisal.is_empty());
    for s in &second.surprisal {
        assert!(s.result.excess_bits < 0.01, "identical batch: {:?}", s);
    }
    // Ranked descending.
    for w in second.surprisal.windows(2) {
        assert!(w[0].result.excess_bits >= w[1].result.excess_bits);
    }

    // Default: opt-out, no report.
    let third = IngestionPipeline::ingest_file(
        &mut db,
        fixture.clone(),
        &mapping,
        IngestOptions::default(),
    )
    .expect("third ingest");
    assert!(third.surprisal.is_empty());
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
    for (i, row) in matrix.iter().enumerate().take(labels.len()) {
        assert!((row[i]).abs() < 1e-12, "diagonal should be 0");
        for (j, value) in row.iter().enumerate().take(labels.len()) {
            assert!((value - matrix[j][i]).abs() < 1e-12, "should be symmetric");
        }
    }
}
