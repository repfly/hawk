use std::collections::HashMap;

use hawk_engine::core::{DimensionDefinition, VariableDefinition, VariableType};
use hawk_engine::ingest::batch_updater::apply_batch;
use hawk_engine::ingest::column_mapper::MappedRow;
use hawk_engine::query::QueryEngine;
use hawk_engine::sql;
use hawk_engine::storage::Database;
use serde_json::Value;

fn row(month: &str, category: &str) -> MappedRow {
    let mut variable_values = HashMap::new();
    variable_values.insert("category".to_owned(), Value::from(category));

    let mut dimension_values = HashMap::new();
    dimension_values.insert("time".to_owned(), month.to_owned());

    MappedRow {
        variable_values,
        dimension_values,
    }
}

fn month(month: &str, counts: &[(&str, usize)]) -> Vec<MappedRow> {
    counts
        .iter()
        .flat_map(|(cat, n)| (0..*n).map(|_| row(month, cat)))
        .collect()
}

fn main() -> anyhow::Result<()> {
    let root = std::env::temp_dir().join(format!("hawk-surprise-demo-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let mut db = Database::create_with_options(&root, false)?;

    db.define_variable(VariableDefinition {
        name: "category".to_owned(),
        var_type: VariableType::Categorical {
            categories: vec![
                "politics".into(),
                "business".into(),
                "sports".into(),
                "science".into(),
            ],
            allow_unknown: false,
        },
    })?;
    db.define_dimension(DimensionDefinition {
        name: "time".to_owned(),
        source_column: "time".to_owned(),
        granularity: None,
    })?;

    // A stable month, then a corrupted batch where "science" floods the feed.
    let mut rows = month(
        "2025-05",
        &[
            ("politics", 40),
            ("business", 30),
            ("sports", 25),
            ("science", 5),
        ],
    );
    rows.extend(month(
        "2025-06",
        &[
            ("politics", 10),
            ("business", 5),
            ("sports", 5),
            ("science", 80),
        ],
    ));
    let schema = db.schema().clone();
    apply_batch(&mut db, &schema, &rows)?;

    let engine = QueryEngine::default();

    println!("--- SURPRISE: June's data under May's model ---");
    println!(
        "{}",
        sql::query(
            &db,
            &engine,
            "SURPRISE time:2025-06 UNDER time:2025-05 ON category"
        )?
    );

    println!("--- All variables, ranked by excess bits ---");
    println!(
        "{}",
        sql::query(&db, &engine, "SURPRISE time:2025-06 UNDER time:2025-05")?
    );

    println!("--- ALERT on surprisal spikes ---");
    println!(
        "{}",
        sql::query(
            &db,
            &engine,
            "ALERT WHEN surprisal > 0.5 ON category FROM time:2025-05"
        )?
    );

    println!("June pays heavily in bits under May's model: the feed changed.");
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}
