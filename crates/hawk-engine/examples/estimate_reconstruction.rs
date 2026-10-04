use std::collections::HashMap;

use hawk_engine::core::{DimensionDefinition, VariableDefinition, VariableType};
use hawk_engine::ingest::batch_updater::apply_batch;
use hawk_engine::ingest::column_mapper::MappedRow;
use hawk_engine::query::QueryEngine;
use hawk_engine::sql;
use hawk_engine::storage::Database;
use serde_json::Value;

fn row(quarter: &str, channel: &str, plan: &str, churned: &str) -> MappedRow {
    let mut variable_values = HashMap::new();
    variable_values.insert("channel".to_owned(), Value::from(channel));
    variable_values.insert("plan".to_owned(), Value::from(plan));
    variable_values.insert("churned".to_owned(), Value::from(churned));

    let mut dimension_values = HashMap::new();
    dimension_values.insert("time".to_owned(), quarter.to_owned());

    MappedRow {
        variable_values,
        dimension_values,
    }
}

fn categorical(name: &str, categories: &[&str]) -> VariableDefinition {
    VariableDefinition {
        name: name.to_owned(),
        var_type: VariableType::Categorical {
            categories: categories.iter().map(|c| (*c).to_owned()).collect(),
            allow_unknown: false,
        },
    }
}

fn main() -> anyhow::Result<()> {
    let root = std::env::temp_dir().join(format!("hawk-estimate-demo-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let mut db = Database::create_with_options(&root, false)?;

    db.define_variable(categorical("channel", &["mobile", "desktop"]))?;
    db.define_variable(categorical("plan", &["free", "paid"]))?;
    db.define_variable(categorical("churned", &["yes", "no"]))?;
    db.define_dimension(DimensionDefinition {
        name: "time".to_owned(),
        source_column: "time".to_owned(),
        granularity: None,
    })?;
    // Only plan×churned gets a stored joint. channel×churned is a joint
    // question the database was never told to remember.
    db.define_joint("plan", "churned")?;

    // 80 rows: churn is REALLY driven by plan (18/20 free churn, 2/20 paid
    // churn); channel is independent. Every marginal is a clean 50/50.
    let mut rows = Vec::new();
    for channel in ["mobile", "desktop"] {
        for plan in ["free", "paid"] {
            let churn_yes = if plan == "free" { 18 } else { 2 };
            for i in 0..20 {
                let churned = if i < churn_yes { "yes" } else { "no" };
                rows.push(row("2025-Q1", channel, plan, churned));
            }
        }
    }
    let schema = db.schema().clone();
    apply_batch(&mut db, &schema, &rows)?;

    let engine = QueryEngine::default();

    println!("=== 1. The stored joint knows the dependency ===");
    println!(
        "{}",
        sql::query(&db, &engine, "MI plan, churned AT time:2025-Q1")?
    );

    println!("=== 2. ESTIMATE prefers the stored joint and says so ===");
    println!(
        "{}",
        sql::query(&db, &engine, "ESTIMATE plan, churned AT time:2025-Q1")?
    );

    // Accuracy + honesty: rebuild the same pair from marginals ALONE,
    // bypassing the stored joint, and compare against the truth.
    println!("=== 3. Same pair from marginals alone vs the stored truth ===");
    let stored = engine.estimate(&db, "plan", "churned", "time:2025-Q1")?;
    let est = engine.estimate_from_marginals(&db, "plan", "churned", "time:2025-Q1")?;
    println!(
        "  stored joint:   MI = {:.4} bits   missing = {:.4} bits",
        stored.mi, stored.missing_information_bits
    );
    println!(
        "  estimate only:  MI = {:.4} bits   missing = {:.4} bits (upper bound on unknown dependency bits)",
        est.mi, est.missing_information_bits
    );
    println!("  cell-by-cell (estimate vs truth, truth always inside the Frechet bounds):");
    for ec in est.cells.iter().filter(|c| c.probability > 0.0) {
        let truth = stored
            .cells
            .iter()
            .find(|c| c.label_a == ec.label_a && c.label_b == ec.label_b)
            .map(|c| c.probability)
            .unwrap_or(0.0);
        println!(
            "    {:>4} × {:<5}  est {:.4}  truth {:.4}  bounds [{:.4}, {:.4}]",
            ec.label_a, ec.label_b, ec.probability, truth, ec.lower_bound, ec.upper_bound
        );
    }
    println!();

    println!("=== 4. A joint that was never stored: honest reconstruction ===");
    println!(
        "{}",
        sql::query(&db, &engine, "ESTIMATE channel, churned AT time:2025-Q1")?
    );

    println!("=== 5. MI on the unstored pair falls back, with a warning ===");
    println!(
        "{}",
        sql::query(&db, &engine, "MI channel, churned AT time:2025-Q1")?
    );

    println!("The estimate is exactly as accurate as independence allows, and it");
    println!("reports the gap — missing_information_bits — instead of hiding it.");
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}
