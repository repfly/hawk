//! Reproducible storage checkpoint using repeated batches of the news fixture.
use std::path::Path;

use hawk_engine::core::{DimensionDefinition, VariableDefinition, VariableType};
use hawk_engine::ingest::{IngestMapping, IngestOptions, IngestionPipeline};
use hawk_engine::query::QueryEngine;
use hawk_engine::sql::{executor, parser};
use hawk_engine::storage::{Database, OpenMode};

fn bytes(path: &Path) -> std::io::Result<u64> {
    std::fs::read_dir(path)?.try_fold(0, |total, entry| {
        let entry = entry?;
        Ok(total
            + if entry.file_type()?.is_dir() {
                bytes(&entry.path())?
            } else {
                entry.metadata()?.len()
            })
    })
}

fn metrics(db: &Database) -> anyhow::Result<Vec<String>> {
    let engine = QueryEngine::default();
    [
        "EXPORT COMPARE leaning BETWEEN time:2024-01 AND time:2024-02 AS JSON",
        "EXPORT STRUCTURE AT time:2024-01 AS JSON",
    ]
    .iter()
    .map(|sql| Ok(executor::execute(db, &engine, &parser::parse(sql)?)?.to_string()))
    .collect()
}

fn main() -> anyhow::Result<()> {
    let root = std::env::temp_dir().join(format!("hawk-mdl-demo-{}", std::process::id()));
    // A fresh directory avoids destroying a user's database.
    anyhow::ensure!(
        !root.exists(),
        "demo directory already exists: {}",
        root.display()
    );
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/community_notes_small.csv");
    let mut db = Database::create_with_options(&root, false)?;
    for (name, categories) in [
        ("leaning", vec!["left", "center", "right"]),
        ("topic", vec!["russia-ukraine", "climate-change"]),
    ] {
        db.define_variable(VariableDefinition {
            name: name.into(),
            var_type: VariableType::Categorical {
                categories: categories.into_iter().map(String::from).collect(),
                allow_unknown: true,
            },
        })?;
    }
    db.define_dimension(DimensionDefinition {
        name: "time".into(),
        source_column: "created_at".into(),
        granularity: Some("monthly".into()),
    })?;
    db.define_joint("leaning", "topic")?;
    let mapping = IngestMapping {
        variables: [
            ("political_leaning".into(), "leaning".into()),
            ("topic_label".into(), "topic".into()),
        ]
        .into(),
        dimensions: [("created_at".into(), "time".into())].into(),
    };
    let mut rows = 0;
    for _ in 0..100 {
        rows +=
            IngestionPipeline::ingest_file(&mut db, &fixture, &mapping, IngestOptions::default())?
                .processed_rows;
    }
    db.flush()?;
    let before = bytes(&root)?;
    let before_audit = db.audit_snapshots(0.01);
    let reference = metrics(&db)?;
    let audit = executor::execute(
        &db,
        &QueryEngine::default(),
        &parser::parse("EXPORT AUDIT STORAGE AS JSON")?,
    )?;
    // Persist report outside the measured DB directory.
    let audit_path = root.with_extension("audit.json");
    std::fs::write(&audit_path, &audit.rows[0][0])?;
    let removed = db.compact_snapshots(0.01)?;
    db.close()?;
    drop(db);
    let after = bytes(&root)?;
    let db = Database::open(&root, OpenMode::ReadOnly)?;
    anyhow::ensure!(
        reference == metrics(&db)?,
        "live query metrics changed after reopen"
    );
    println!("rows,epsilon_bits,db_bytes_before,db_bytes_after,snapshots_before,snapshots_removed,snapshots_after,live_query_output_changed");
    println!(
        "{rows},0.01,{before},{after},{},{removed},{},false",
        before_audit.entries,
        db.audit_snapshots(0.01).entries
    );
    println!(
        "database: {}\naudit: {}",
        root.display(),
        audit_path.display()
    );
    Ok(())
}
