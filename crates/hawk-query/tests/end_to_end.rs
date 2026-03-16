use std::path::PathBuf;

use hawk_core::{DimensionDefinition, VariableDefinition, VariableType};
use hawk_ingest::{IngestMapping, IngestOptions, IngestionPipeline};
use hawk_query::QueryEngine;
use hawk_storage::{Database, OpenMode};

fn temp_db_dir() -> PathBuf {
    std::env::temp_dir().join(format!("hawk-e2e-{}", std::process::id()))
}

#[test]
fn create_define_ingest_compare_reopen_compare() {
    let root = temp_db_dir();
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove existing db dir");
    }

    let mut db = Database::create(&root).expect("create db");
    db.define_variable(VariableDefinition {
        name: "sentiment".to_owned(),
        var_type: VariableType::Continuous {
            bins: 10,
            range: Some((-1.0, 1.0)),
        },
    })
    .expect("define sentiment");
    db.define_variable(VariableDefinition {
        name: "leaning".to_owned(),
        var_type: VariableType::Categorical {
            categories: vec!["left".to_owned(), "center".to_owned(), "right".to_owned()],
            allow_unknown: true,
        },
    })
    .expect("define leaning");

    db.define_dimension(DimensionDefinition {
        name: "topic".to_owned(),
        source_column: "topic_label".to_owned(),
        granularity: None,
    })
    .expect("define topic");

    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/community_notes_small.csv");

    let mut mapping = IngestMapping::default();
    mapping
        .variables
        .insert("sentiment_score".to_owned(), "sentiment".to_owned());
    mapping
        .variables
        .insert("political_leaning".to_owned(), "leaning".to_owned());
    mapping
        .dimensions
        .insert("topic_label".to_owned(), "topic".to_owned());

    let report = IngestionPipeline::ingest_file(
        &mut db,
        fixture,
        &mapping,
        IngestOptions {
            batch_size: 2,
            show_progress: false,
        },
    )
    .expect("ingest csv");

    assert_eq!(report.processed_rows, 6);

    let query = QueryEngine::default();
    let before = query
        .compare(
            &db,
            "topic:russia-ukraine/variable:sentiment",
            "topic:climate-change/variable:sentiment",
            None,
        )
        .expect("compare before reopen");

    assert!(before.jsd >= 0.0);

    db.flush().expect("flush before reopen");
    drop(db);

    let reopened = Database::open(&root, OpenMode::ReadOnly).expect("reopen db");
    let after = query
        .compare(
            &reopened,
            "topic:russia-ukraine/variable:sentiment",
            "topic:climate-change/variable:sentiment",
            None,
        )
        .expect("compare after reopen");

    assert!((before.jsd - after.jsd).abs() < 1e-12);
}
