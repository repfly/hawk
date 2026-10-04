use std::collections::HashMap;

use anyhow::Result;

use crate::math::surprisal::{surprisal, SurprisalReport, SMOOTHING_EPSILON};
use crate::storage::Database;

use crate::query::planner::resolve_distribution;
use crate::query::result_types::{SurpriseContributor, SurpriseResult};

pub fn execute_surprise(
    db: &Database,
    variable: &str,
    dims_a: &HashMap<String, String>,
    dims_b: &HashMap<String, String>,
    min_cell_count: Option<u64>,
) -> Result<SurpriseResult> {
    let dist_a = resolve_distribution(db, variable, dims_a, min_cell_count)?;
    let dist_b = resolve_distribution(db, variable, dims_b, min_cell_count)?;

    let report = surprisal(&dist_a.repr, &dist_b.repr).map_err(anyhow::Error::from)?;
    Ok(surprise_result_from_report(variable, report))
}

pub fn surprise_result_from_report(variable: &str, report: SurprisalReport) -> SurpriseResult {
    let unseen_mass_warning = (report.unseen_mass > 0.0).then(|| {
        format!(
            "{:.1}% of A's mass falls on buckets unseen in B (smoothed with ε={:e})",
            report.unseen_mass * 100.0,
            SMOOTHING_EPSILON
        )
    });

    SurpriseResult {
        variable: variable.to_owned(),
        total_bits: report.cross_entropy * report.sample_count_a as f64,
        bits_per_sample: report.cross_entropy,
        entropy_a: report.entropy_a,
        baseline_entropy: report.entropy_b,
        excess_bits: report.excess_bits,
        sample_count_a: report.sample_count_a,
        sample_count_b: report.sample_count_b,
        unseen_mass: report.unseen_mass,
        unseen_mass_warning,
        top_contributors: report
            .contributions
            .into_iter()
            .map(|c| SurpriseContributor {
                label: c.label,
                prob_a: c.prob_a,
                prob_b: c.prob_b,
                bits: c.bits,
                excess_bits: c.excess_bits,
                unseen_in_b: c.unseen_in_b,
            })
            .collect(),
    }
}
