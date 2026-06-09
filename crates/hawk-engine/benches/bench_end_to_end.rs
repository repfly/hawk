use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use hawk_engine::core::{DimensionDefinition, VariableDefinition, VariableType};
use hawk_engine::ingest::batch_updater::apply_batch;
use hawk_engine::ingest::column_mapper::MappedRow;
use hawk_engine::query::QueryEngine;
use hawk_engine::storage::Database;
use serde_json::Value;

const ROW_COUNT: usize = 10_000;
const LEANINGS: [&str; 3] = ["left", "center", "right"];
const TOPICS: [&str; 5] = [
    "russia-ukraine",
    "climate-change",
    "us-elections",
    "ai-regulation",
    "immigration",
];
const YEARS: [&str; 3] = ["2023", "2024", "2025"];

fn bench_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("hawk-bench-{label}-{}-{nanos}", std::process::id()))
}

fn synthetic_rows(row_count: usize) -> Vec<MappedRow> {
    (0..row_count)
        .map(|i| {
            let leaning = LEANINGS[(i * 7 + i / 11) % LEANINGS.len()];
            let topic = TOPICS[(i * 5 + i / 17) % TOPICS.len()];
            let year = YEARS[(i * 3 + i / 101) % YEARS.len()];
            let sentiment = (((i * 37) % 200) as f64 / 100.0) - 1.0;

            let mut variable_values = HashMap::new();
            variable_values.insert("sentiment_score".to_owned(), Value::from(sentiment));
            variable_values.insert("political_leaning".to_owned(), Value::from(leaning));
            variable_values.insert("topic".to_owned(), Value::from(topic));

            let mut dimension_values = HashMap::new();
            dimension_values.insert("time".to_owned(), year.to_owned());

            MappedRow {
                variable_values,
                dimension_values,
            }
        })
        .collect()
}

fn create_empty_db(label: &str) -> (PathBuf, Database) {
    let root = bench_dir(label);
    let mut db = Database::create_with_options(&root, false).expect("create benchmark db");
    db.define_variable(VariableDefinition {
        name: "sentiment_score".to_owned(),
        var_type: VariableType::Continuous {
            bins: 50,
            range: Some((-1.0, 1.0)),
        },
    })
    .expect("define sentiment");
    db.define_variable(VariableDefinition {
        name: "political_leaning".to_owned(),
        var_type: VariableType::Categorical {
            categories: LEANINGS.iter().map(|s| s.to_string()).collect(),
            allow_unknown: false,
        },
    })
    .expect("define leaning");
    db.define_variable(VariableDefinition {
        name: "topic".to_owned(),
        var_type: VariableType::Categorical {
            categories: TOPICS.iter().map(|s| s.to_string()).collect(),
            allow_unknown: false,
        },
    })
    .expect("define topic");
    db.define_dimension(DimensionDefinition {
        name: "time".to_owned(),
        source_column: "created_year".to_owned(),
        granularity: None,
    })
    .expect("define time");
    db.define_joint("political_leaning", "topic")
        .expect("define joint");
    (root, db)
}

fn create_populated_db(label: &str) -> (PathBuf, Database) {
    let rows = synthetic_rows(ROW_COUNT);
    let (root, mut db) = create_empty_db(label);
    let schema = db.schema().clone();
    apply_batch(&mut db, &schema, &rows).expect("apply benchmark rows");
    (root, db)
}

fn remove_db(root: PathBuf) {
    let _ = std::fs::remove_dir_all(root);
}

fn bench_ingest(c: &mut Criterion) {
    let rows = synthetic_rows(ROW_COUNT);
    c.bench_function("e2e_ingest_10k_rows", |b| {
        b.iter_batched(
            || {
                let (root, db) = create_empty_db("ingest");
                (root, db, rows.clone())
            },
            |(root, mut db, rows)| {
                let schema = db.schema().clone();
                let report =
                    apply_batch(&mut db, &schema, black_box(&rows)).expect("apply benchmark rows");
                black_box(report.processed);
                remove_db(root);
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_queries(c: &mut Criterion) {
    let (root, db) = create_populated_db("queries");
    let engine = QueryEngine::default();

    c.bench_function("e2e_compare_10k_rows", |b| {
        b.iter(|| {
            black_box(
                engine
                    .compare(&db, "time:2023", "time:2024", Some("political_leaning"))
                    .expect("compare"),
            )
        })
    });

    c.bench_function("e2e_track_10k_rows", |b| {
        b.iter(|| {
            black_box(
                engine
                    .track(&db, "time:2023", None, None, None)
                    .expect("track"),
            )
        })
    });

    c.bench_function("e2e_mi_10k_rows", |b| {
        b.iter(|| {
            black_box(
                engine
                    .mutual_info(&db, "political_leaning", "topic", "time:2023")
                    .expect("mi"),
            )
        })
    });

    c.bench_function("e2e_cmi_10k_rows", |b| {
        b.iter(|| {
            black_box(
                engine
                    .conditional_mutual_info(&db, "political_leaning", "topic", "time", None)
                    .expect("cmi"),
            )
        })
    });

    remove_db(root);
}

criterion_group!(benches, bench_ingest, bench_queries);
criterion_main!(benches);
