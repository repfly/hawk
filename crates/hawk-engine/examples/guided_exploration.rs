//! The self-guiding demo: the database tells you what to ask next.
//!
//! Builds a small subscriptions DB, then loops five rounds: take the top
//! SUGGEST candidate, execute it, add it to the session history, and watch
//! the suggestions evolve as knowledge accumulates. This is the CLI-side
//! twin of the MCP `profile` → `suggest` → `query` loop.

use std::collections::HashMap;

use hawk_engine::core::{DimensionDefinition, VariableDefinition, VariableType};
use hawk_engine::ingest::batch_updater::apply_batch;
use hawk_engine::ingest::column_mapper::MappedRow;
use hawk_engine::query::suggest::{pair_key, SuggestContext};
use hawk_engine::query::QueryEngine;
use hawk_engine::sql;
use hawk_engine::storage::Database;
use serde_json::Value;

fn row(month: &str, channel: &str, plan: &str, churned: &str) -> MappedRow {
    let mut variable_values = HashMap::new();
    variable_values.insert("channel".to_owned(), Value::from(channel));
    variable_values.insert("plan".to_owned(), Value::from(plan));
    variable_values.insert("churned".to_owned(), Value::from(churned));

    let mut dimension_values = HashMap::new();
    dimension_values.insert("time".to_owned(), month.to_owned());

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

/// Mirror what the MCP ledger would record for an executed query, so the
/// suggestion engine can dedup: the query fingerprint plus the released key.
fn record_history(ctx: &mut SuggestContext, query: &str) {
    ctx.record_query(query);
    let words: Vec<&str> = query.split_whitespace().collect();
    match words.as_slice() {
        ["SHOW", var, ..] | ["COMPARE", var, "ACROSS", ..] => {
            ctx.record_released_key(*var);
        }
        ["ESTIMATE", var_a, var_b, ..] => {
            ctx.record_released_key(pair_key(var_a.trim_end_matches(','), var_b));
        }
        _ => {}
    }
}

fn main() -> anyhow::Result<()> {
    let root = std::env::temp_dir().join(format!("hawk-guided-demo-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let mut db = Database::create_with_options(&root, false)?;

    db.define_variable(categorical(
        "channel",
        &["mobile", "desktop", "tablet", "tv"],
    ))?;
    db.define_variable(categorical("plan", &["free", "paid"]))?;
    db.define_variable(categorical("churned", &["yes", "no"]))?;
    db.define_dimension(DimensionDefinition {
        name: "time".to_owned(),
        source_column: "time".to_owned(),
        granularity: None,
    })?;
    // One stored joint; the other pairs stay unknown → ESTIMATE candidates.
    db.define_joint("channel", "plan")?;

    // channel: uniform 4-way (2 bits). plan: 50/50, but flips its meaning
    // over time — free-heavy in Q1, paid-heavy in Q2 (time explains it).
    // churned: follows plan, so its dependency on plan is worth estimating.
    let mut rows = Vec::new();
    for (quarter, free_weight) in [("2025-Q1", 3), ("2025-Q2", 1)] {
        for channel in ["mobile", "desktop", "tablet", "tv"] {
            for i in 0..8 {
                let plan = if i < 2 * free_weight { "free" } else { "paid" };
                let churned = if plan == "free" { "yes" } else { "no" };
                rows.push(row(quarter, channel, plan, churned));
            }
        }
    }
    let schema = db.schema().clone();
    apply_batch(&mut db, &schema, &rows)?;

    let engine = QueryEngine::default();
    let mut ctx = SuggestContext::default();

    println!("=== Guided exploration: 5 rounds of \"what should I ask next?\" ===\n");

    for round in 1..=5 {
        let suggestions = engine.suggest(&db, &ctx, 5)?;
        let Some(top) = suggestions.first() else {
            println!(
                "Round {}: nothing left to suggest — exploration complete.",
                round
            );
            break;
        };

        println!("--- Round {} ---", round);
        println!("Suggestions:");
        for s in &suggestions {
            println!("  [{:6.2} bits]  {}", s.expected_bits, s.query);
            println!("                 ({})", s.rationale);
        }
        println!("\nExecuting top suggestion: {}\n", top.query);
        match sql::query(&db, &engine, &top.query) {
            Ok(result) => println!("{}", result),
            Err(e) => println!("  error: {}\n", e),
        }
        record_history(&mut ctx, &top.query);
    }

    println!("Each executed query drops out of the ranking and the next most");
    println!("informative question surfaces — the database guides its own reading.");
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}
