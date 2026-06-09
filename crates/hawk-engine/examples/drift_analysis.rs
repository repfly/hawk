use std::collections::HashMap;

use hawk_engine::core::{DimensionDefinition, VariableDefinition, VariableType};
use hawk_engine::ingest::batch_updater::apply_batch;
use hawk_engine::ingest::column_mapper::MappedRow;
use hawk_engine::query::QueryEngine;
use hawk_engine::sql;
use hawk_engine::storage::Database;
use serde_json::Value;

fn row(time: &str, category: &str) -> MappedRow {
    let mut variable_values = HashMap::new();
    variable_values.insert("category".to_owned(), Value::from(category));

    let mut dimension_values = HashMap::new();
    dimension_values.insert("time".to_owned(), time.to_owned());

    MappedRow {
        variable_values,
        dimension_values,
    }
}

fn main() -> anyhow::Result<()> {
    let root = std::env::temp_dir().join(format!("hawk-drift-demo-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let mut db = Database::create_with_options(&root, false)?;

    db.define_variable(VariableDefinition {
        name: "category".to_owned(),
        var_type: VariableType::Categorical {
            categories: vec!["billing".into(), "login".into(), "search".into()],
            allow_unknown: false,
        },
    })?;
    db.define_dimension(DimensionDefinition {
        name: "time".to_owned(),
        source_column: "time".to_owned(),
        granularity: None,
    })?;

    let rows = vec![
        row("before", "billing"),
        row("before", "billing"),
        row("before", "login"),
        row("before", "search"),
        row("after", "login"),
        row("after", "login"),
        row("after", "login"),
        row("after", "search"),
    ];
    let schema = db.schema().clone();
    apply_batch(&mut db, &schema, &rows)?;

    let engine = QueryEngine::default();
    println!("--- COMPARE ---");
    println!(
        "{}",
        sql::query(
            &db,
            &engine,
            "COMPARE category BETWEEN time:before AND time:after"
        )?
    );

    println!("--- TRACK ---");
    println!(
        "{}",
        sql::query(&db, &engine, "TRACK category FROM time:before")?
    );

    println!("Login issues became dominant after the change window.");
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}
