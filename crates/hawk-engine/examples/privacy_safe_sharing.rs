use std::collections::HashMap;

use hawk_engine::core::{DimensionDefinition, VariableDefinition, VariableType};
use hawk_engine::ingest::batch_updater::apply_batch;
use hawk_engine::ingest::column_mapper::MappedRow;
use hawk_engine::query::QueryEngine;
use hawk_engine::sql;
use hawk_engine::storage::Database;
use serde_json::Value;

fn row(time: &str, segment: &str) -> MappedRow {
    let mut variable_values = HashMap::new();
    variable_values.insert("segment".to_owned(), Value::from(segment));

    let mut dimension_values = HashMap::new();
    dimension_values.insert("time".to_owned(), time.to_owned());

    MappedRow {
        variable_values,
        dimension_values,
    }
}

fn main() -> anyhow::Result<()> {
    let root = std::env::temp_dir().join(format!("hawk-privacy-demo-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);

    // Raw-log retention is disabled, so the database stores distribution summaries only.
    let mut db = Database::create_with_options(&root, false)?;
    db.define_variable(VariableDefinition {
        name: "segment".to_owned(),
        var_type: VariableType::Categorical {
            categories: vec!["free".into(), "team".into(), "enterprise".into()],
            allow_unknown: false,
        },
    })?;
    db.define_dimension(DimensionDefinition {
        name: "time".to_owned(),
        source_column: "time".to_owned(),
        granularity: None,
    })?;

    let raw_rows = vec![
        row("2024", "free"),
        row("2024", "team"),
        row("2024", "team"),
        row("2025", "team"),
        row("2025", "enterprise"),
        row("2025", "enterprise"),
    ];
    let schema = db.schema().clone();
    apply_batch(&mut db, &schema, &raw_rows)?;
    db.flush()?;

    let engine = QueryEngine::default();
    println!("{}", sql::query(&db, &engine, "SHOW segment AT time:2025")?);
    println!(
        "Share the database directory for aggregate distribution analysis; do not enable raw logs for sensitive rows."
    );

    let _ = std::fs::remove_dir_all(root);
    Ok(())
}
