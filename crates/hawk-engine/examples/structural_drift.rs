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
    let root = std::env::temp_dir().join(format!("hawk-structure-demo-{}", std::process::id()));
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
    db.define_joint("channel", "plan")?;
    db.define_joint("channel", "churned")?;
    db.define_joint("plan", "churned")?;

    // Both quarters: 80 rows, channel and plan independent (20 per combo),
    // every marginal exactly 50/50. The marginals do not move at all.
    //
    // Q1: churn is driven by PLAN — free users churn, paid users stay.
    // Q2: churn is driven by CHANNEL — mobile users churn (9 in 10), plan
    //     no longer matters. Same marginals, flipped association.
    let mut rows = Vec::new();
    for channel in ["mobile", "desktop"] {
        for plan in ["free", "paid"] {
            for _ in 0..20 {
                let churned = if plan == "free" { "yes" } else { "no" };
                rows.push(row("2025-Q1", channel, plan, churned));
            }
            let churn_yes = if channel == "mobile" { 18 } else { 2 };
            for i in 0..20 {
                let churned = if i < churn_yes { "yes" } else { "no" };
                rows.push(row("2025-Q2", channel, plan, churned));
            }
        }
    }
    let schema = db.schema().clone();
    apply_batch(&mut db, &schema, &rows)?;

    let engine = QueryEngine::default();

    println!("--- Marginals do not move: COMPARE sees nothing ---");
    for var in ["channel", "plan", "churned"] {
        let comparison = engine.compare(&db, "time:2025-Q1", "time:2025-Q2", Some(var))?;
        assert!(comparison.jsd.abs() < 1e-12, "{var} marginal moved");
        let out = sql::query(
            &db,
            &engine,
            &format!("COMPARE {} BETWEEN time:2025-Q1 AND time:2025-Q2", var),
        )?;
        let jsd = out
            .rows
            .iter()
            .find(|r| r[0] == "JSD")
            .map(|r| r[1].clone())
            .unwrap_or_default();
        println!("  {:8}  JSD = {}", var, jsd);
    }
    println!();

    println!("--- STRUCTURE AT time:2025-Q1 (churn follows plan) ---");
    println!("{}", sql::query(&db, &engine, "STRUCTURE AT time:2025-Q1")?);

    println!("--- STRUCTURE AT time:2025-Q2 (churn follows channel) ---");
    println!("{}", sql::query(&db, &engine, "STRUCTURE AT time:2025-Q2")?);

    println!("--- COMPARE STRUCTURE: the association flipped ---");
    println!(
        "{}",
        sql::query(
            &db,
            &engine,
            "COMPARE STRUCTURE BETWEEN time:2025-Q1 AND time:2025-Q2"
        )?
    );

    let diff = engine.compare_structure(&db, "time:2025-Q1", "time:2025-Q2")?;
    assert_eq!(diff.added_edges.len(), 1);
    assert_eq!(diff.dropped_edges.len(), 1);
    assert!((diff.rewiring_score - 0.6532).abs() < 0.0001);
    assert!(diff
        .reweighted_edges
        .iter()
        .any(|edge| edge.var_a == "channel" && edge.var_b == "churned" && edge.delta > 0.5));

    println!("No marginal moved, yet the dependency tree rewired: churn detached");
    println!("from plan and attached to channel. Only the joints can see that.");
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}
