use std::collections::HashMap;

use hawk_engine::core::{DimensionDefinition, VariableDefinition, VariableType};
use hawk_engine::ingest::batch_updater::apply_batch;
use hawk_engine::ingest::column_mapper::MappedRow;
use hawk_engine::query::QueryEngine;
use hawk_engine::sql;
use hawk_engine::storage::Database;
use serde_json::Value;

fn row(time: &str, plan: &str, outcome: &str) -> MappedRow {
    let mut variable_values = HashMap::new();
    variable_values.insert("plan".to_owned(), Value::from(plan));
    variable_values.insert("outcome".to_owned(), Value::from(outcome));

    let mut dimension_values = HashMap::new();
    dimension_values.insert("time".to_owned(), time.to_owned());

    MappedRow {
        variable_values,
        dimension_values,
    }
}

fn main() -> anyhow::Result<()> {
    let root = std::env::temp_dir().join(format!("hawk-association-demo-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let mut db = Database::create_with_options(&root, false)?;

    db.define_variable(VariableDefinition {
        name: "plan".to_owned(),
        var_type: VariableType::Categorical {
            categories: vec!["free".into(), "paid".into()],
            allow_unknown: false,
        },
    })?;
    db.define_variable(VariableDefinition {
        name: "outcome".to_owned(),
        var_type: VariableType::Categorical {
            categories: vec!["retained".into(), "churned".into()],
            allow_unknown: false,
        },
    })?;
    db.define_dimension(DimensionDefinition {
        name: "time".to_owned(),
        source_column: "time".to_owned(),
        granularity: None,
    })?;
    db.define_joint("plan", "outcome")?;

    let rows = vec![
        row("2024", "free", "churned"),
        row("2024", "free", "churned"),
        row("2024", "paid", "retained"),
        row("2024", "paid", "retained"),
        row("2025", "free", "churned"),
        row("2025", "paid", "retained"),
        row("2025", "paid", "retained"),
        row("2025", "paid", "retained"),
    ];
    let schema = db.schema().clone();
    apply_batch(&mut db, &schema, &rows)?;

    let engine = QueryEngine::default();
    println!("--- MI ---");
    println!(
        "{}",
        sql::query(&db, &engine, "MI plan, outcome AT time:2025")?
    );
    println!("--- CMI ---");
    println!(
        "{}",
        sql::query(&db, &engine, "CMI plan, outcome GIVEN time")?
    );
    println!("--- CORRELATIONS ---");
    println!(
        "{}",
        sql::query(&db, &engine, "CORRELATIONS OVER time LIMIT 5")?
    );

    let _ = std::fs::remove_dir_all(root);
    Ok(())
}
