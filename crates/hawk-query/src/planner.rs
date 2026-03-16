use std::collections::HashMap;

use anyhow::{anyhow, Result};

use hawk_core::{dimension_key_from_pairs, DistributionObject};
use hawk_math::entropy;
use hawk_storage::Database;

pub fn resolve_distribution(
    db: &Database,
    variable: &str,
    dimensions: &HashMap<String, String>,
) -> Result<DistributionObject> {
    let key = dimension_key_from_pairs(dimensions.iter().map(|(k, v)| (k.clone(), v.clone())));

    if let Some(exact) = db.get_distribution(variable, &key) {
        return Ok(exact.clone());
    }

    let matches: Vec<&DistributionObject> = db
        .distributions_for_variable(variable)
        .into_iter()
        .filter(|d| {
            dimensions
                .iter()
                .all(|(k, v)| d.dimension_key.get(k) == Some(v))
        })
        .collect();

    if matches.is_empty() {
        return Err(anyhow!(
            "distribution not found for variable '{}' and dimensions {:?}",
            variable,
            dimensions
        ));
    }

    let mut aggregated = matches[0].clone();
    for other in &matches[1..] {
        aggregated.repr.merge_from(&other.repr);
    }
    aggregated.sample_count = aggregated.repr.total_count();
    let counts = aggregated.repr.value_count_vector();
    aggregated.entropy = entropy(&counts, aggregated.sample_count);
    Ok(aggregated)
}
