use std::collections::HashMap;

use anyhow::{anyhow, Result};

use crate::core::{
    DistributionRepr, JointDistributionObject, JointRepr, VariableType, UNKNOWN_CATEGORY_LABEL,
};
use crate::math::{
    entropy_from_probs, frechet_bounds, maxent_joint, mi_upper_bound, mutual_information,
    mutual_information_from_probs,
};
use crate::storage::Database;

use crate::query::planner::resolve_distribution;
use crate::query::result_types::{EstimateCell, EstimateResult};
use crate::query::structure::resolve_joint_counts;

/// Reconstruct the joint of two variables at a slice. A stored joint at the
/// slice is always preferred (reported as observed, missing information 0);
/// otherwise the max-entropy joint is built from the stored marginals alone.
pub fn execute_estimate(
    db: &Database,
    var_a: &str,
    var_b: &str,
    reference: &str,
    dims: &HashMap<String, String>,
    min_cell_count: Option<u64>,
) -> Result<EstimateResult> {
    let (var_a, var_b) = canonical_pair(var_a, var_b);

    if let Some(result) = observed_estimate(db, var_a, var_b, reference, dims, min_cell_count)? {
        return Ok(result);
    }
    estimate_from_marginals(db, var_a, var_b, reference, dims, min_cell_count)
}

/// The max-entropy reconstruction from stored marginals alone, bypassing any
/// stored joint — used for the honesty comparison against observed joints.
pub fn estimate_from_marginals(
    db: &Database,
    var_a: &str,
    var_b: &str,
    reference: &str,
    dims: &HashMap<String, String>,
    min_cell_count: Option<u64>,
) -> Result<EstimateResult> {
    let (var_a, var_b) = canonical_pair(var_a, var_b);

    let dist_a = resolve_distribution(db, var_a, dims, min_cell_count)?;
    let dist_b = resolve_distribution(db, var_b, dims, min_cell_count)?;
    let (labels_a, probs_a) = labels_and_probs(&dist_a.repr);
    let (labels_b, probs_b) = labels_and_probs(&dist_b.repr);

    let ipf = maxent_joint(&probs_a, &probs_b).map_err(|e| {
        anyhow!(
            "cannot estimate '{}' × '{}' at {}: {}",
            var_a,
            var_b,
            reference,
            e
        )
    })?;

    Ok(build_result(
        var_a,
        var_b,
        reference,
        false,
        &labels_a,
        &labels_b,
        &probs_a,
        &probs_b,
        &ipf.joint,
        mutual_information_from_probs(&ipf.joint),
        dist_a.sample_count,
        dist_b.sample_count,
        ipf.iterations,
        ipf.converged,
    ))
}

/// Report the stored joint (exact key, or partial-key aggregation as in
/// STRUCTURE) if one exists with data at this slice.
fn observed_estimate(
    db: &Database,
    var_a: &str,
    var_b: &str,
    reference: &str,
    dims: &HashMap<String, String>,
    min_cell_count: Option<u64>,
) -> Result<Option<EstimateResult>> {
    let Some((counts, total)) = resolve_joint_counts(db, var_a, var_b, dims) else {
        return Ok(None);
    };
    if total == 0 {
        return Ok(None);
    }

    let representative = db
        .joints_for_pair(var_a, var_b)
        .into_iter()
        .find(|j| dims.iter().all(|(k, v)| j.dimension_key.get(k) == Some(v)))
        .ok_or_else(|| anyhow!("stored joint disappeared while estimating"))?;
    let (row_labels, col_labels, rows_follow_a) = joint_axis_labels(db, var_a, representative);

    // Orient the grid so rows always follow var_a.
    let (mut counts, mut labels_a, mut labels_b) = if rows_follow_a {
        (counts, row_labels, col_labels)
    } else {
        (transpose(&counts), col_labels, row_labels)
    };

    // Small-cell suppression on the released grid: fold categorical rows /
    // columns whose marginal count is below k into __unknown__, then compute
    // every metric from the folded grid.
    if let Some(k) = min_cell_count {
        if is_categorical(db, var_a) {
            (counts, labels_a) = fold_grid_rows(counts, labels_a, k);
        }
        if is_categorical(db, var_b) {
            let (folded, folded_labels) = fold_grid_rows(transpose(&counts), labels_b, k);
            counts = transpose(&folded);
            labels_b = folded_labels;
        }
    }

    let probs: Vec<Vec<f64>> = counts
        .iter()
        .map(|row| row.iter().map(|c| *c as f64 / total as f64).collect())
        .collect();
    let marginal_a: Vec<f64> = probs.iter().map(|row| row.iter().sum()).collect();
    let marginal_b: Vec<f64> = (0..probs[0].len())
        .map(|j| probs.iter().map(|row| row[j]).sum())
        .collect();

    labels_a.resize(marginal_a.len(), String::new());
    labels_b.resize(marginal_b.len(), String::new());

    Ok(Some(build_result(
        var_a,
        var_b,
        reference,
        true,
        &labels_a,
        &labels_b,
        &marginal_a,
        &marginal_b,
        &probs,
        mutual_information(&counts, total),
        total,
        total,
        0,
        true,
    )))
}

#[allow(clippy::too_many_arguments)]
fn build_result(
    var_a: &str,
    var_b: &str,
    reference: &str,
    observed: bool,
    labels_a: &[String],
    labels_b: &[String],
    marginal_a: &[f64],
    marginal_b: &[f64],
    joint: &[Vec<f64>],
    mi: f64,
    sample_count_a: u64,
    sample_count_b: u64,
    ipf_iterations: usize,
    ipf_converged: bool,
) -> EstimateResult {
    let bounds = frechet_bounds(marginal_a, marginal_b);
    let flat: Vec<f64> = joint.iter().flatten().copied().collect();
    let mi_upper = mi_upper_bound(marginal_a, marginal_b);

    let mut cells = Vec::with_capacity(labels_a.len() * labels_b.len());
    for (i, label_a) in labels_a.iter().enumerate() {
        for (j, label_b) in labels_b.iter().enumerate() {
            cells.push(EstimateCell {
                label_a: label_a.clone(),
                label_b: label_b.clone(),
                probability: joint[i][j],
                lower_bound: bounds.lower[i][j],
                upper_bound: bounds.upper[i][j],
            });
        }
    }
    cells.sort_by(|x, y| {
        y.probability
            .total_cmp(&x.probability)
            .then_with(|| (&x.label_a, &x.label_b).cmp(&(&y.label_a, &y.label_b)))
    });

    EstimateResult {
        var_a: var_a.to_owned(),
        var_b: var_b.to_owned(),
        reference: reference.to_owned(),
        observed,
        entropy_a: entropy_from_probs(marginal_a),
        entropy_b: entropy_from_probs(marginal_b),
        joint_entropy: entropy_from_probs(&flat),
        mi,
        mi_upper_bound: mi_upper,
        // See EstimateResult: min(H(A), H(B)) is the gap between the max-ent
        // joint's entropy and the lowest joint entropy the marginals allow —
        // the maximum MI the marginals permit. Known exactly when observed.
        missing_information_bits: if observed { 0.0 } else { mi_upper },
        sample_count_a,
        sample_count_b,
        cells,
        ipf_iterations,
        ipf_converged,
    }
}

fn canonical_pair<'a>(var_a: &'a str, var_b: &'a str) -> (&'a str, &'a str) {
    if var_a <= var_b {
        (var_a, var_b)
    } else {
        (var_b, var_a)
    }
}

/// Labels and probabilities of a marginal; histogram bins become categories.
fn labels_and_probs(repr: &DistributionRepr) -> (Vec<String>, Vec<f64>) {
    let labels = match repr {
        DistributionRepr::Categorical { .. } => repr
            .categorical_labels_with_unknown()
            .expect("categorical labels expected"),
        DistributionRepr::Histogram {
            min,
            max,
            bin_counts,
            ..
        } => bin_labels(*min, *max, bin_counts.len()),
    };
    (labels, repr.as_probability_vector())
}

fn bin_labels(min: f64, max: f64, bins: usize) -> Vec<String> {
    let width = (max - min) / bins.max(1) as f64;
    (0..bins)
        .map(|i| {
            let lo = min + i as f64 * width;
            format!("[{:.2}, {:.2})", lo, lo + width)
        })
        .collect()
}

fn is_categorical(db: &Database, variable: &str) -> bool {
    db.schema()
        .variables
        .iter()
        .find(|v| v.name == variable)
        .map(|v| matches!(v.var_type, VariableType::Categorical { .. }))
        .unwrap_or(false)
}

/// Fold grid rows whose marginal count is below `min_count` into a single
/// `__unknown__` row (merging with an existing one). Column marginals are
/// unaffected. Fold columns by transposing around this.
fn fold_grid_rows(
    counts: Vec<Vec<u64>>,
    labels: Vec<String>,
    min_count: u64,
) -> (Vec<Vec<u64>>, Vec<String>) {
    let cols = counts.first().map(Vec::len).unwrap_or(0);
    let needs_fold = counts
        .iter()
        .zip(labels.iter())
        .any(|(row, label)| label != UNKNOWN_CATEGORY_LABEL && row.iter().sum::<u64>() < min_count);
    if !needs_fold {
        return (counts, labels);
    }

    let mut kept_rows = Vec::with_capacity(counts.len());
    let mut kept_labels = Vec::with_capacity(labels.len());
    let mut folded = vec![0u64; cols];
    for (row, label) in counts.into_iter().zip(labels) {
        if label == UNKNOWN_CATEGORY_LABEL || row.iter().sum::<u64>() < min_count {
            for (slot, c) in folded.iter_mut().zip(&row) {
                *slot += c;
            }
        } else {
            kept_rows.push(row);
            kept_labels.push(label);
        }
    }
    kept_rows.push(folded);
    kept_labels.push(UNKNOWN_CATEGORY_LABEL.to_owned());
    (kept_rows, kept_labels)
}

fn transpose(grid: &[Vec<u64>]) -> Vec<Vec<u64>> {
    let cols = grid.first().map(Vec::len).unwrap_or(0);
    (0..cols)
        .map(|j| grid.iter().map(|row| row[j]).collect())
        .collect()
}

/// Row and column labels of a stored joint's grid, plus whether the row axis
/// follows the canonical `var_a`. ContingencyTable and HistogramGrid store
/// pair.0 (= var_a) on the row axis; ConditionalHistograms store the
/// categorical variable there regardless of pair order.
fn joint_axis_labels(
    db: &Database,
    var_a: &str,
    joint: &JointDistributionObject,
) -> (Vec<String>, Vec<String>, bool) {
    match &joint.repr {
        JointRepr::ContingencyTable {
            x_categories,
            y_categories,
            ..
        } => (x_categories.clone(), y_categories.clone(), true),
        JointRepr::HistogramGrid {
            x_min,
            x_max,
            x_bins,
            y_min,
            y_max,
            y_bins,
            ..
        } => (
            bin_labels(*x_min, *x_max, *x_bins as usize),
            bin_labels(*y_min, *y_max, *y_bins as usize),
            true,
        ),
        JointRepr::ConditionalHistograms {
            condition_categories,
            histograms,
            ..
        } => {
            let bins = histograms
                .first()
                .map(|h| match h {
                    DistributionRepr::Histogram {
                        min,
                        max,
                        bin_counts,
                        ..
                    } => bin_labels(*min, *max, bin_counts.len()),
                    DistributionRepr::Categorical { .. } => labels_and_probs(h).0,
                })
                .unwrap_or_default();
            let a_is_categorical = db
                .schema()
                .variables
                .iter()
                .find(|v| v.name == var_a)
                .map(|v| matches!(v.var_type, VariableType::Categorical { .. }))
                .unwrap_or(true);
            (condition_categories.clone(), bins, a_is_categorical)
        }
    }
}
