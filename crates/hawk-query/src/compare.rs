use std::collections::HashMap;

use anyhow::{anyhow, Result};

use hawk_core::DistributionRepr;
use hawk_math::{
    asymptotic_jsd_confidence, entropy, hellinger, jsd, kl_divergence, psi, rebin_histogram,
    wasserstein_1,
};
use hawk_storage::Database;

use crate::{
    cache::{CompareCacheKey, QueryCache},
    planner::resolve_distribution,
    result_types::{CategoryShift, CompareResult},
};

pub fn execute_compare(
    db: &Database,
    cache: &QueryCache,
    variable: &str,
    dims_a: &HashMap<String, String>,
    dims_b: &HashMap<String, String>,
) -> Result<CompareResult> {
    let dist_a = resolve_distribution(db, variable, dims_a)?;
    let dist_b = resolve_distribution(db, variable, dims_b)?;

    let cache_key = CompareCacheKey {
        dist_id_a: dist_a.id,
        version_a: dist_a.version,
        dist_id_b: dist_b.id,
        version_b: dist_b.version,
    };

    if let Some(cached) = cache.get_compare(&cache_key) {
        return Ok(cached);
    }

    let result = match (&dist_a.repr, &dist_b.repr) {
        (
            DistributionRepr::Histogram {
                min: a_min,
                max: a_max,
                bin_counts: a_bins,
                total_count: _a_total,
            },
            DistributionRepr::Histogram {
                min: b_min,
                max: b_max,
                bin_counts: b_bins,
                total_count: _b_total,
            },
        ) => {
            let common_min = a_min.min(*b_min);
            let common_max = a_max.max(*b_max);
            let common_bins = a_bins.len().max(b_bins.len());

            let aligned_a = rebin_histogram(&dist_a.repr, common_min, common_max, common_bins)
                .ok_or_else(|| anyhow!("failed to rebin A"))?;
            let aligned_b = rebin_histogram(&dist_b.repr, common_min, common_max, common_bins)
                .ok_or_else(|| anyhow!("failed to rebin B"))?;

            let DistributionRepr::Histogram {
                bin_counts: a_bins,
                total_count: a_total,
                ..
            } = aligned_a
            else {
                unreachable!("rebin output histogram")
            };

            let DistributionRepr::Histogram {
                bin_counts: b_bins,
                total_count: b_total,
                ..
            } = aligned_b
            else {
                unreachable!("rebin output histogram")
            };

            let jsd_value = jsd(&a_bins, &b_bins, a_total, b_total);
            let result = CompareResult {
                jsd: jsd_value,
                kl_a_to_b: kl_divergence(&a_bins, &b_bins, a_total, b_total),
                kl_b_to_a: kl_divergence(&b_bins, &a_bins, b_total, a_total),
                entropy_a: entropy(&a_bins, a_total),
                entropy_b: entropy(&b_bins, b_total),
                wasserstein: Some(wasserstein_1(
                    &a_bins,
                    &b_bins,
                    a_total,
                    b_total,
                    (common_max - common_min) / common_bins as f64,
                )),
                hellinger: hellinger(&a_bins, &b_bins, a_total, b_total),
                psi: psi(&a_bins, &b_bins, a_total, b_total),
                sample_count_a: a_total,
                sample_count_b: b_total,
                confidence: asymptotic_jsd_confidence(jsd_value, a_total, b_total),
                top_movers: Vec::new(),
            };
            result
        }
        (
            DistributionRepr::Categorical {
                categories: a_categories,
                counts: a_counts,
                total_count: a_total,
                ..
            },
            DistributionRepr::Categorical {
                categories: b_categories,
                counts: b_counts,
                total_count: b_total,
                ..
            },
        ) => {
            let (cats, a_aligned, b_aligned) = hawk_math::align_categorical(
                a_categories.as_slice(),
                a_counts.as_slice(),
                b_categories.as_slice(),
                b_counts.as_slice(),
            );
            let jsd_value = jsd(&a_aligned, &b_aligned, *a_total, *b_total);

            // Compute per-category shifts
            let top_movers = compute_category_shifts(&cats, &a_aligned, *a_total, &b_aligned, *b_total);

            CompareResult {
                jsd: jsd_value,
                kl_a_to_b: kl_divergence(&a_aligned, &b_aligned, *a_total, *b_total),
                kl_b_to_a: kl_divergence(&b_aligned, &a_aligned, *b_total, *a_total),
                entropy_a: entropy(&a_aligned, *a_total),
                entropy_b: entropy(&b_aligned, *b_total),
                wasserstein: None,
                hellinger: hellinger(&a_aligned, &b_aligned, *a_total, *b_total),
                psi: psi(&a_aligned, &b_aligned, *a_total, *b_total),
                sample_count_a: *a_total,
                sample_count_b: *b_total,
                confidence: asymptotic_jsd_confidence(jsd_value, *a_total, *b_total),
                top_movers,
            }
        }
        _ => return Err(anyhow!("type mismatch for compare")),
    };

    cache.put_compare(cache_key, result.clone());
    Ok(result)
}

/// Compute per-category probability shifts and their JSD contribution.
fn compute_category_shifts(
    categories: &[String],
    a_counts: &[u64],
    a_total: u64,
    b_counts: &[u64],
    b_total: u64,
) -> Vec<CategoryShift> {
    if a_total == 0 && b_total == 0 {
        return Vec::new();
    }

    let mut shifts: Vec<CategoryShift> = categories
        .iter()
        .enumerate()
        .map(|(i, cat)| {
            let p = if a_total > 0 { a_counts[i] as f64 / a_total as f64 } else { 0.0 };
            let q = if b_total > 0 { b_counts[i] as f64 / b_total as f64 } else { 0.0 };
            let m = (p + q) * 0.5;
            // Per-category JSD contribution
            let contrib = if m > 0.0 {
                let kl_p = if p > 0.0 { 0.5 * p * (p / m).log2() } else { 0.0 };
                let kl_q = if q > 0.0 { 0.5 * q * (q / m).log2() } else { 0.0 };
                kl_p + kl_q
            } else {
                0.0
            };
            CategoryShift {
                category: cat.clone(),
                prob_a: p,
                prob_b: q,
                delta: q - p,
                contribution: contrib,
            }
        })
        .collect();

    shifts.sort_by(|a, b| b.delta.abs().total_cmp(&a.delta.abs()));
    shifts
}
