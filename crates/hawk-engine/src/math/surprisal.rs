use crate::core::{DistributionRepr, HawkError, Result};
use crate::math::{entropy, rebin_histogram};

/// Additive smoothing applied to the model (B) probabilities so buckets unseen
/// in B get finite surprisal: q_i = (c_i + ε) / (total + k·ε). Matches the
/// epsilon used by `math::kl_divergence` so excess bits cross-check against it.
pub const SMOOTHING_EPSILON: f64 = 1e-10;

#[derive(Debug, Clone)]
pub struct SurprisalContribution {
    pub label: String,
    pub prob_a: f64,
    pub prob_b: f64,
    /// p_A · −log₂ q_B — this bucket's share of the cross-entropy.
    pub bits: f64,
    /// p_A · log₂(p_A / q_B) — this bucket's share of the excess bits.
    pub excess_bits: f64,
    pub unseen_in_b: bool,
}

#[derive(Debug, Clone, Default)]
pub struct SurprisalReport {
    /// H(A, B) = −Σ p_A·log₂ q_B, in bits per sample.
    pub cross_entropy: f64,
    pub entropy_a: f64,
    pub entropy_b: f64,
    /// KL(A‖B) = H(A, B) − H(A) — bits paid per sample for using B's model on A's data.
    pub excess_bits: f64,
    /// Probability mass of A on buckets with zero count in B (smoothed, reported separately).
    pub unseen_mass: f64,
    pub sample_count_a: u64,
    pub sample_count_b: u64,
    /// Per-bucket contributions, sorted by excess bits descending.
    pub contributions: Vec<SurprisalContribution>,
}

/// Cross-entropy surprisal of distribution A's data under distribution B's model.
pub fn surprisal(a: &DistributionRepr, b: &DistributionRepr) -> Result<SurprisalReport> {
    match (a, b) {
        (DistributionRepr::Categorical { .. }, DistributionRepr::Categorical { .. }) => {
            let a_labels = a
                .categorical_labels_with_unknown()
                .expect("categorical labels expected");
            let b_labels = b
                .categorical_labels_with_unknown()
                .expect("categorical labels expected");
            let (labels, a_counts, b_counts) = crate::math::align_categorical(
                &a_labels,
                &a.value_count_vector(),
                &b_labels,
                &b.value_count_vector(),
            );
            Ok(surprisal_from_counts(
                &labels,
                &a_counts,
                a.total_count(),
                &b_counts,
                b.total_count(),
            ))
        }
        (
            DistributionRepr::Histogram {
                min: a_min,
                max: a_max,
                bin_counts: a_bins,
                ..
            },
            DistributionRepr::Histogram {
                min: b_min,
                max: b_max,
                bin_counts: b_bins,
                ..
            },
        ) => {
            let common_min = a_min.min(*b_min);
            let common_max = a_max.max(*b_max);
            let common_bins = a_bins.len().max(b_bins.len()).max(1);

            let aligned_a = rebin_histogram(a, common_min, common_max, common_bins)
                .ok_or_else(|| HawkError::TypeMismatch("failed to rebin A".to_owned()))?;
            let aligned_b = rebin_histogram(b, common_min, common_max, common_bins)
                .ok_or_else(|| HawkError::TypeMismatch("failed to rebin B".to_owned()))?;

            let width = (common_max - common_min) / common_bins as f64;
            let labels = (0..common_bins)
                .map(|i| {
                    let lo = common_min + i as f64 * width;
                    format!("[{:.2}, {:.2})", lo, lo + width)
                })
                .collect::<Vec<_>>();

            Ok(surprisal_from_counts(
                &labels,
                &aligned_a.value_count_vector(),
                aligned_a.total_count(),
                &aligned_b.value_count_vector(),
                aligned_b.total_count(),
            ))
        }
        _ => Err(HawkError::TypeMismatch(
            "cannot compute surprisal between categorical and histogram distributions".to_owned(),
        )),
    }
}

/// Surprisal over pre-aligned count vectors. Empty A yields a zero report;
/// empty B is treated as a know-nothing model (smoothing makes it uniform).
pub fn surprisal_from_counts(
    labels: &[String],
    a_counts: &[u64],
    a_total: u64,
    b_counts: &[u64],
    b_total: u64,
) -> SurprisalReport {
    assert_eq!(a_counts.len(), b_counts.len(), "vector lengths must match");

    let mut report = SurprisalReport {
        entropy_a: entropy(a_counts, a_total),
        entropy_b: entropy(b_counts, b_total),
        sample_count_a: a_total,
        sample_count_b: b_total,
        ..SurprisalReport::default()
    };

    if a_total == 0 {
        return report;
    }

    let k = a_counts.len() as f64;

    for (i, (a, b)) in a_counts.iter().zip(b_counts).enumerate() {
        if *a == 0 {
            continue;
        }
        let p = *a as f64 / a_total as f64;
        let q = (*b as f64 + SMOOTHING_EPSILON) / (b_total as f64 + k * SMOOTHING_EPSILON);
        let bits = -p * q.log2();
        let excess = p * (p / q).log2();
        let unseen = *b == 0;
        if unseen {
            report.unseen_mass += p;
        }
        report.cross_entropy += bits;
        report.contributions.push(SurprisalContribution {
            label: labels.get(i).cloned().unwrap_or_default(),
            prob_a: p,
            prob_b: if b_total > 0 {
                *b as f64 / b_total as f64
            } else {
                0.0
            },
            bits,
            excess_bits: excess,
            unseen_in_b: unseen,
        });
    }

    report.excess_bits = report.cross_entropy - report.entropy_a;
    report
        .contributions
        .sort_by(|a, b| b.excess_bits.total_cmp(&a.excess_bits));
    report
}

#[cfg(test)]
mod tests {
    use crate::core::DistributionRepr;
    use crate::math::kl_divergence;

    use super::{surprisal, surprisal_from_counts};

    fn categorical(categories: &[&str], counts: &[u64], unknown: u64) -> DistributionRepr {
        DistributionRepr::Categorical {
            categories: categories.iter().map(|c| (*c).to_owned()).collect(),
            counts: counts.to_vec(),
            unknown_count: unknown,
            total_count: counts.iter().sum::<u64>() + unknown,
        }
    }

    fn histogram(min: f64, max: f64, bin_counts: &[u64]) -> DistributionRepr {
        DistributionRepr::Histogram {
            min,
            max,
            bin_counts: bin_counts.to_vec(),
            total_count: bin_counts.iter().sum(),
        }
    }

    #[test]
    fn identical_distributions_have_zero_excess() {
        let a = categorical(&["x", "y"], &[5, 5], 0);
        let report = surprisal(&a, &a).expect("surprisal");
        assert!((report.cross_entropy - 1.0).abs() < 1e-6);
        assert!(report.excess_bits.abs() < 1e-6);
        assert!(report.unseen_mass.abs() < 1e-12);
    }

    #[test]
    fn excess_bits_matches_kl_divergence() {
        let report = surprisal_from_counts(
            &["a".into(), "b".into(), "c".into()],
            &[7, 2, 1],
            10,
            &[3, 3, 4],
            10,
        );
        let kl = kl_divergence(&[7, 2, 1], &[3, 3, 4], 10, 10);
        assert!((report.excess_bits - kl).abs() < 1e-9);
        let contrib_sum: f64 = report.contributions.iter().map(|c| c.excess_bits).sum();
        assert!((report.excess_bits - contrib_sum).abs() < 1e-9);
    }

    #[test]
    fn unseen_category_is_smoothed_and_reported() {
        let a = categorical(&["x", "y", "z"], &[4, 4, 2], 0);
        let b = categorical(&["x", "y", "z"], &[5, 5, 0], 0);
        let report = surprisal(&a, &b).expect("surprisal");
        assert!((report.unseen_mass - 0.2).abs() < 1e-12);
        assert!(report.cross_entropy.is_finite());
        assert!(report.excess_bits > 1.0, "unseen mass should dominate");
        let top = &report.contributions[0];
        assert_eq!(top.label, "z");
        assert!(top.unseen_in_b);
    }

    #[test]
    fn categories_align_across_different_label_sets() {
        let a = categorical(&["x", "w"], &[5, 5], 0);
        let b = categorical(&["x", "y"], &[5, 5], 0);
        let report = surprisal(&a, &b).expect("surprisal");
        assert!((report.unseen_mass - 0.5).abs() < 1e-12);
    }

    #[test]
    fn unknown_bucket_participates() {
        let a = categorical(&["x"], &[5], 5);
        let b = categorical(&["x"], &[10], 0);
        let report = surprisal(&a, &b).expect("surprisal");
        assert!((report.unseen_mass - 0.5).abs() < 1e-12);
        assert!(report
            .contributions
            .iter()
            .any(|c| c.label == crate::core::UNKNOWN_CATEGORY_LABEL && c.unseen_in_b));
    }

    #[test]
    fn empty_a_yields_zero_report() {
        let a = categorical(&["x", "y"], &[0, 0], 0);
        let b = categorical(&["x", "y"], &[5, 5], 0);
        let report = surprisal(&a, &b).expect("surprisal");
        assert_eq!(report.cross_entropy, 0.0);
        assert_eq!(report.excess_bits, 0.0);
        assert!(report.contributions.is_empty());
    }

    #[test]
    fn empty_b_is_uniform_ignorance() {
        let a = categorical(&["x", "y", "z"], &[6, 3, 3], 0);
        let b = categorical(&["x", "y", "z"], &[0, 0, 0], 0);
        let report = surprisal(&a, &b).expect("surprisal");
        assert!((report.unseen_mass - 1.0).abs() < 1e-12);
        // Smoothing makes an empty model uniform over k buckets (incl. __unknown__).
        assert!((report.cross_entropy - 2.0).abs() < 1e-6);
    }

    #[test]
    fn histogram_range_mismatch_is_rebinned() {
        let a = histogram(0.0, 10.0, &[10, 20, 30, 40]);
        let b = histogram(0.0, 20.0, &[40, 30, 20, 10]);
        let report = surprisal(&a, &b).expect("surprisal");
        assert!(report.cross_entropy.is_finite());
        assert!(report.excess_bits > -1e-9);
        assert_eq!(report.contributions[0].label.chars().next(), Some('['));
    }

    #[test]
    fn identical_histograms_have_zero_excess() {
        let a = histogram(0.0, 10.0, &[10, 20, 30, 40]);
        let report = surprisal(&a, &a).expect("surprisal");
        assert!(report.excess_bits.abs() < 1e-6);
    }

    #[test]
    fn both_empty_distributions_are_finite() {
        for empty in [categorical(&[], &[], 0), histogram(0.0, 1.0, &[0, 0])] {
            let report = surprisal(&empty, &empty).expect("empty surprisal");
            assert_eq!(report.cross_entropy, 0.0);
            assert_eq!(report.excess_bits, 0.0);
            assert_eq!(report.unseen_mass, 0.0);
            assert!(report.contributions.is_empty());
        }
    }

    #[test]
    fn disjoint_histograms_report_unseen_mass_and_consistent_contributions() {
        let a = histogram(0.0, 1.0, &[3, 7]);
        let b = histogram(1.0, 2.0, &[6, 4]);
        let report = surprisal(&a, &b).expect("disjoint histograms");
        assert_eq!(report.unseen_mass, 1.0);
        assert!(report.cross_entropy.is_finite());
        assert!(report.excess_bits > 10.0);
        assert!(
            (report.contributions.iter().map(|c| c.bits).sum::<f64>() - report.cross_entropy).abs()
                < 1e-9
        );
        assert!(
            (report
                .contributions
                .iter()
                .map(|c| c.excess_bits)
                .sum::<f64>()
                - report.excess_bits)
                .abs()
                < 1e-9
        );
    }

    #[test]
    fn type_mismatch_is_an_error() {
        let a = categorical(&["x"], &[5], 0);
        let b = histogram(0.0, 1.0, &[5]);
        assert!(surprisal(&a, &b).is_err());
    }
}
