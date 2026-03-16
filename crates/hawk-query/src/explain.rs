use std::collections::HashMap;

use anyhow::Result;

use hawk_storage::Database;

use crate::{
    cache::QueryCache,
    compare::execute_compare,
    result_types::{ExplainResult, VariableContribution},
};

pub fn execute_explain(
    db: &Database,
    cache: &QueryCache,
    dims_a: &HashMap<String, String>,
    dims_b: &HashMap<String, String>,
) -> Result<ExplainResult> {
    let mut contributions = Vec::new();

    for variable in &db.schema().variables {
        let cmp = execute_compare(db, cache, &variable.name, dims_a, dims_b)?;
        contributions.push(VariableContribution {
            variable: variable.name.clone(),
            jsd: cmp.jsd,
            fraction: 0.0,
            entropy_a: cmp.entropy_a,
            entropy_b: cmp.entropy_b,
            top_movers: cmp.top_movers,
        });
    }

    let total = contributions.iter().map(|c| c.jsd).sum::<f64>();
    for item in &mut contributions {
        item.fraction = if total > 0.0 { item.jsd / total } else { 0.0 };
    }
    contributions.sort_by(|a, b| b.jsd.total_cmp(&a.jsd));

    Ok(ExplainResult {
        total_divergence: total,
        contributions,
    })
}
