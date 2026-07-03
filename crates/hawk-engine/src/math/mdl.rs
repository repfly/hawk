use crate::core::{DistributionRepr, HawkError, Result, UNKNOWN_CATEGORY_LABEL};
use crate::math::{entropy, maxent_joint, mutual_information, rebin_histogram};

/// Storage-cost model for the description-length side of every MDL score,
/// mirroring the bincode encoding: a histogram bin is one u64 count, a
/// categorical entry is a length-prefixed label plus one u64 count.
pub const HISTOGRAM_BIN_BITS: f64 = 64.0;
const LENGTH_PREFIX_BITS: f64 = 64.0;
const COUNT_BITS: f64 = 64.0;

/// Candidate bin counts for MDL auto-binning. Powers of two so every
/// candidate is an exact coarsening of the finer ones.
pub const MDL_BIN_CANDIDATES: [usize; 6] = [4, 8, 16, 32, 64, 128];

/// One candidate resolution for a histogram: what k bins cost to store and
/// how much of the reference entropy they retain.
#[derive(Debug, Clone)]
pub struct BinCountScore {
    pub bins: usize,
    /// Cost of persisting k bins, in bits (k × 64).
    pub storage_bits: f64,
    /// Entropy captured at this resolution, in bits per sample.
    pub entropy_bits: f64,
    /// Two-part description length: storage_bits + n × (H_ref − H_k), where
    /// H_ref is the entropy at the finest scored resolution. Lower is better.
    pub description_length: f64,
}

fn category_storage_bits(label: &str) -> f64 {
    LENGTH_PREFIX_BITS + 8.0 * label.len() as f64 + COUNT_BITS
}

fn bin_description_length(bins: usize, samples: u64, entropy_lost: f64) -> f64 {
    bins as f64 * HISTOGRAM_BIN_BITS + samples as f64 * entropy_lost.max(0.0)
}

/// Score coarser rebinnings of a stored histogram. Candidates larger than the
/// current bin count are ignored; the current resolution is always scored and
/// serves as the entropy reference.
pub fn score_histogram_bins(
    hist: &DistributionRepr,
    candidates: &[usize],
) -> Result<Vec<BinCountScore>> {
    let DistributionRepr::Histogram {
        min,
        max,
        bin_counts,
        total_count,
    } = hist
    else {
        return Err(HawkError::TypeMismatch(
            "MDL bin scoring requires a histogram distribution".to_owned(),
        ));
    };

    let current = bin_counts.len();
    let h_ref = entropy(bin_counts, *total_count);

    let mut ks: Vec<usize> = candidates
        .iter()
        .copied()
        .filter(|k| *k > 0 && *k < current)
        .collect();
    ks.push(current);
    ks.sort_unstable();
    ks.dedup();

    let mut scores = Vec::with_capacity(ks.len());
    for k in ks {
        let entropy_bits = if k == current {
            h_ref
        } else {
            let rebinned = rebin_histogram(hist, *min, *max, k).ok_or_else(|| {
                HawkError::TypeMismatch(format!("failed to rebin histogram to {} bins", k))
            })?;
            entropy(&rebinned.value_count_vector(), rebinned.total_count())
        };
        scores.push(BinCountScore {
            bins: k,
            storage_bits: k as f64 * HISTOGRAM_BIN_BITS,
            entropy_bits,
            description_length: bin_description_length(k, *total_count, h_ref - entropy_bits),
        });
    }
    Ok(scores)
}

/// The candidate minimizing description length. Scores are scanned in
/// ascending bin order and a candidate must beat the incumbent by more than
/// 1e-9 bits, so ties (and near-ties) break toward fewer bins.
pub fn choose_bin_count(scores: &[BinCountScore]) -> Option<usize> {
    let mut best: Option<&BinCountScore> = None;
    for score in scores {
        match best {
            Some(b) if score.description_length >= b.description_length - 1e-9 => {}
            _ => best = Some(score),
        }
    }
    best.map(|b| b.bins)
}

/// Pick a bin count for raw continuous values by MDL over
/// [`MDL_BIN_CANDIDATES`]: minimize k × 64 + n × (H_ref − H_k), where H_k is
/// the entropy of the values binned into k equal-width bins over [min, max)
/// and H_ref is the entropy at the finest candidate. Deterministic; ties
/// break toward fewer bins.
pub fn mdl_bin_count(values: &[f64], min: f64, max: f64) -> usize {
    let fallback = MDL_BIN_CANDIDATES[0];
    if values.is_empty() || max <= min || !min.is_finite() || !max.is_finite() {
        return fallback;
    }

    let n = values.len() as u64;
    let entropies: Vec<(usize, f64)> = MDL_BIN_CANDIDATES
        .iter()
        .map(|&k| (k, entropy(&bin_values(values, min, max, k), n)))
        .collect();
    let h_ref = entropies.last().map(|(_, h)| *h).unwrap_or(0.0);

    let scores: Vec<BinCountScore> = entropies
        .into_iter()
        .map(|(k, h_k)| BinCountScore {
            bins: k,
            storage_bits: k as f64 * HISTOGRAM_BIN_BITS,
            entropy_bits: h_k,
            description_length: bin_description_length(k, n, h_ref - h_k),
        })
        .collect();
    choose_bin_count(&scores).unwrap_or(fallback)
}

fn bin_values(values: &[f64], min: f64, max: f64, bins: usize) -> Vec<u64> {
    let width = (max - min) / bins as f64;
    let mut counts = vec![0u64; bins];
    for v in values {
        let idx = (((v - min) / width).floor() as isize).clamp(0, bins as isize - 1) as usize;
        counts[idx] += 1;
    }
    counts
}

/// Mass guard for fold candidates: a category above this probability is
/// never a candidate, even when its entropy contribution is small — folding
/// a dominant label would destroy the released distribution's meaning.
pub const FOLD_MASS_THRESHOLD: f64 = 0.01;

/// A category whose distinction from `__unknown__` carries fewer bits than
/// its storage costs — folding it loses ~nothing.
#[derive(Debug, Clone)]
pub struct FoldCandidate {
    pub category: String,
    pub count: u64,
    /// Total bits of information the distinction carries: n × (−p·log₂ p),
    /// the category's contribution to the distribution's entropy.
    pub information_bits: f64,
    /// Bits spent storing the label + count entry.
    pub storage_bits: f64,
}

/// Categories of a categorical distribution whose distinction does not pay
/// for its storage: probability mass at most [`FOLD_MASS_THRESHOLD`] and
/// entropy contribution (in total bits) below the entry's storage cost.
/// Sorted by information carried ascending. Histograms yield an empty list.
pub fn category_fold_candidates(repr: &DistributionRepr) -> Vec<FoldCandidate> {
    let DistributionRepr::Categorical {
        categories,
        counts,
        total_count,
        ..
    } = repr
    else {
        return Vec::new();
    };
    if *total_count == 0 {
        // No data: every stored category is pure storage cost.
        let mut out: Vec<FoldCandidate> = categories
            .iter()
            .map(|c| FoldCandidate {
                category: c.clone(),
                count: 0,
                information_bits: 0.0,
                storage_bits: category_storage_bits(c),
            })
            .collect();
        out.sort_by(|a, b| a.category.cmp(&b.category));
        return out;
    }

    let n = *total_count as f64;
    let term = |count: u64| {
        if count == 0 {
            0.0
        } else {
            let p = count as f64 / n;
            -p * p.log2()
        }
    };

    let mut out = Vec::new();
    for (category, &count) in categories.iter().zip(counts.iter()) {
        if category == UNKNOWN_CATEGORY_LABEL {
            continue;
        }
        if count as f64 / n > FOLD_MASS_THRESHOLD {
            continue;
        }
        let information_bits = n * term(count);
        let storage_bits = category_storage_bits(category);
        if information_bits < storage_bits {
            out.push(FoldCandidate {
                category: category.clone(),
                count,
                information_bits,
                storage_bits,
            });
        }
    }
    out.sort_by(|a, b| {
        a.information_bits
            .total_cmp(&b.information_bits)
            .then_with(|| a.category.cmp(&b.category))
    });
    out
}

/// MDL verdict on a stored joint: it earns its bytes iff the dependency
/// information it carries (n × MI) exceeds its serialized size.
#[derive(Debug, Clone)]
pub struct JointMdlScore {
    /// MI of the stored joint, in bits per sample.
    pub mi_bits_per_sample: f64,
    /// Total dependency information: sample_count × MI, in bits.
    pub dependency_bits: f64,
    /// Serialized size, in bits.
    pub storage_bits: f64,
    /// KL(stored ‖ max-ent estimate from its marginals), bits per sample —
    /// how far the Epic-3 ESTIMATE reconstruction would land from the stored
    /// joint. Equals MI up to IPF numerics.
    pub estimate_gap_bits: f64,
    pub worth_keeping: bool,
}

/// Score keeping a joint vs. dropping it and relying on the max-ent ESTIMATE.
pub fn score_joint(counts: &[Vec<u64>], total: u64, serialized_bytes: u64) -> JointMdlScore {
    let mi = mutual_information(counts, total);
    let dependency_bits = total as f64 * mi;
    let storage_bits = serialized_bytes as f64 * 8.0;
    let estimate_gap_bits = estimate_gap(counts, total).unwrap_or(mi);
    JointMdlScore {
        mi_bits_per_sample: mi,
        dependency_bits,
        storage_bits,
        estimate_gap_bits,
        worth_keeping: dependency_bits >= storage_bits,
    }
}

/// KL(observed joint ‖ IPF max-ent joint from the observed marginals), bits
/// per sample. None on degenerate input.
fn estimate_gap(counts: &[Vec<u64>], total: u64) -> Option<f64> {
    if total == 0 || counts.is_empty() || counts[0].is_empty() {
        return Some(0.0);
    }
    let n = total as f64;
    let probs: Vec<Vec<f64>> = counts
        .iter()
        .map(|row| row.iter().map(|c| *c as f64 / n).collect())
        .collect();
    let marginal_a: Vec<f64> = probs.iter().map(|row| row.iter().sum()).collect();
    let marginal_b: Vec<f64> = (0..probs[0].len())
        .map(|j| probs.iter().map(|row| row[j]).sum())
        .collect();

    let maxent = maxent_joint(&marginal_a, &marginal_b).ok()?.joint;
    let mut kl = 0.0;
    for (p_row, q_row) in probs.iter().zip(&maxent) {
        for (&p, &q) in p_row.iter().zip(q_row) {
            if p > 0.0 && q > 0.0 {
                kl += p * (p / q).log2();
            }
        }
    }
    Some(kl.max(0.0))
}

#[cfg(test)]
mod tests {
    use crate::core::DistributionRepr;
    use crate::math::mutual_information;

    use super::{
        category_fold_candidates, choose_bin_count, mdl_bin_count, score_histogram_bins,
        score_joint, MDL_BIN_CANDIDATES,
    };

    fn histogram(min: f64, max: f64, bin_counts: &[u64]) -> DistributionRepr {
        DistributionRepr::Histogram {
            min,
            max,
            bin_counts: bin_counts.to_vec(),
            total_count: bin_counts.iter().sum(),
        }
    }

    #[test]
    fn uniform_histogram_keeps_its_resolution() {
        // 8 uniform bins with many samples: any coarsening loses log2 bits
        // per sample, far more than 64 bits per bin saved.
        let hist = histogram(0.0, 8.0, &[100; 8]);
        let scores = score_histogram_bins(&hist, &[1, 2, 4]).expect("histogram");
        assert_eq!(choose_bin_count(&scores), Some(8));
    }

    #[test]
    fn single_spike_histogram_prefers_fewest_bins() {
        let mut bins = vec![0u64; 64];
        bins[10] = 1000;
        let hist = histogram(0.0, 64.0, &bins);
        let scores = score_histogram_bins(&hist, &[2, 4, 8, 16, 32]).expect("histogram");
        // All resolutions capture zero entropy; ties break toward fewer bins.
        assert_eq!(choose_bin_count(&scores), Some(2));
    }

    #[test]
    fn candidates_above_current_resolution_are_ignored() {
        let hist = histogram(0.0, 4.0, &[10, 10, 10, 10]);
        let scores = score_histogram_bins(&hist, &[2, 8, 128]).expect("histogram");
        assert!(scores.iter().all(|s| s.bins <= 4));
    }

    #[test]
    fn bin_scoring_rejects_categoricals() {
        let cat = DistributionRepr::Categorical {
            categories: vec!["a".into()],
            counts: vec![1],
            unknown_count: 0,
            total_count: 1,
        };
        assert!(score_histogram_bins(&cat, &[2]).is_err());
    }

    /// Two tight modes at 0.4 and 0.45 (which coarse binning merges) plus a
    /// few outliers at the range ends so the modes sit mid-range.
    fn bimodal_values() -> Vec<f64> {
        let mut values = Vec::with_capacity(1000);
        values.extend(std::iter::repeat_n(0.4, 490));
        values.extend(std::iter::repeat_n(0.45, 490));
        values.extend(std::iter::repeat_n(0.0, 10));
        values.extend(std::iter::repeat_n(1.0, 10));
        values
    }

    #[test]
    fn mdl_bin_count_bimodal_separates_modes() {
        let values = bimodal_values();
        let k = mdl_bin_count(&values, 0.0, 1.0005);

        // At the chosen resolution the two modes must land in distinct bins;
        // coarse candidates (4, 8 bins) merge them.
        let width = 1.0005 / k as f64;
        assert!(
            (0.4 / width) as usize != (0.45 / width) as usize,
            "chosen k={} does not separate the modes",
            k
        );
        assert!(k >= 16);
    }

    #[test]
    fn mdl_bin_count_near_constant_gets_few_bins() {
        // One dominant value plus a handful of outliers.
        let mut values = vec![0.5; 990];
        values.extend(std::iter::repeat_n(1.0, 10));
        let k = mdl_bin_count(&values, 0.5, 1.0005);
        assert_eq!(k, MDL_BIN_CANDIDATES[0]);
    }

    #[test]
    fn mdl_bin_count_bimodal_beats_near_constant() {
        let bimodal = bimodal_values();
        let constant = vec![0.5; 1000];

        let k_bimodal = mdl_bin_count(&bimodal, 0.0, 1.0005);
        let k_constant = mdl_bin_count(&constant, 0.5, 1.5);
        assert!(k_bimodal > k_constant);
        // Deterministic across calls.
        assert_eq!(k_bimodal, mdl_bin_count(&bimodal, 0.0, 1.0005));
    }

    #[test]
    fn tiny_category_is_a_fold_candidate() {
        let cat = DistributionRepr::Categorical {
            categories: vec!["big".into(), "rare".into()],
            counts: vec![10_000, 1],
            unknown_count: 0,
            total_count: 10_001,
        };
        let folds = category_fold_candidates(&cat);
        assert_eq!(folds.len(), 1);
        assert_eq!(folds[0].category, "rare");
        assert!(folds[0].information_bits < folds[0].storage_bits);
    }

    #[test]
    fn heavy_category_is_not_a_fold_candidate() {
        let cat = DistributionRepr::Categorical {
            categories: vec!["a".into(), "b".into()],
            counts: vec![5_000, 5_000],
            unknown_count: 0,
            total_count: 10_000,
        };
        assert!(category_fold_candidates(&cat).is_empty());
    }

    #[test]
    fn empty_category_is_pure_storage_waste() {
        let cat = DistributionRepr::Categorical {
            categories: vec!["a".into(), "never-seen".into()],
            counts: vec![10_000, 0],
            unknown_count: 0,
            total_count: 10_000,
        };
        let folds = category_fold_candidates(&cat);
        assert_eq!(folds.len(), 1);
        assert_eq!(folds[0].category, "never-seen");
        assert_eq!(folds[0].information_bits, 0.0);
    }

    #[test]
    fn histograms_have_no_fold_candidates() {
        let hist = histogram(0.0, 1.0, &[1, 2, 3]);
        assert!(category_fold_candidates(&hist).is_empty());
    }

    #[test]
    fn independent_joint_is_a_drop_candidate() {
        // Product of uniform marginals: MI = 0, so any storage cost loses.
        let counts = vec![vec![250u64, 250], vec![250, 250]];
        let score = score_joint(&counts, 1000, 100);
        assert!(score.mi_bits_per_sample.abs() < 1e-9);
        assert!(!score.worth_keeping);
        assert!(score.estimate_gap_bits < 1e-6);
    }

    #[test]
    fn strongly_dependent_joint_earns_its_bytes() {
        // Diagonal joint: MI = 1 bit/sample; 10_000 samples dwarf 100 bytes.
        let counts = vec![vec![5_000u64, 0], vec![0, 5_000]];
        let score = score_joint(&counts, 10_000, 100);
        assert!((score.mi_bits_per_sample - 1.0).abs() < 1e-9);
        assert!(score.worth_keeping);
    }

    #[test]
    fn estimate_gap_matches_mi() {
        let counts = vec![vec![400u64, 100], vec![100, 400]];
        let total = 1000;
        let score = score_joint(&counts, total, 64);
        let mi = mutual_information(&counts, total);
        assert!((score.estimate_gap_bits - mi).abs() < 1e-6);
    }

    #[test]
    fn empty_joint_is_a_drop_candidate() {
        let counts = vec![vec![0u64, 0], vec![0, 0]];
        let score = score_joint(&counts, 0, 64);
        assert_eq!(score.dependency_bits, 0.0);
        assert!(!score.worth_keeping);
    }
}
