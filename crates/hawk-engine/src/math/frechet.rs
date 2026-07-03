use crate::math::entropy_from_probs;

/// Per-cell Fréchet bounds on a joint distribution given only its marginals:
/// any joint with marginals p, q satisfies
/// max(0, p_i + q_j − 1) ≤ P(i, j) ≤ min(p_i, q_j).
#[derive(Debug, Clone)]
pub struct FrechetBounds {
    pub lower: Vec<Vec<f64>>,
    pub upper: Vec<Vec<f64>>,
}

pub fn frechet_bounds(marginal_a: &[f64], marginal_b: &[f64]) -> FrechetBounds {
    let lower = marginal_a
        .iter()
        .map(|p| marginal_b.iter().map(|q| (p + q - 1.0).max(0.0)).collect())
        .collect();
    let upper = marginal_a
        .iter()
        .map(|p| marginal_b.iter().map(|q| p.min(*q)).collect())
        .collect();
    FrechetBounds { lower, upper }
}

/// Upper bound on the mutual information any joint with these marginals can
/// have: MI(A;B) ≤ min(H(A), H(B)), in bits. Not always tight (mismatched
/// marginal shapes may not admit a joint that reaches it), but it is a valid,
/// simple bound — documented as such wherever it is reported.
pub fn mi_upper_bound(marginal_a: &[f64], marginal_b: &[f64]) -> f64 {
    entropy_from_probs(marginal_a).min(entropy_from_probs(marginal_b))
}

#[cfg(test)]
mod tests {
    use super::{frechet_bounds, mi_upper_bound};

    #[test]
    fn bounds_contain_independence_product() {
        let a = [0.7, 0.3];
        let b = [0.4, 0.6];
        let bounds = frechet_bounds(&a, &b);
        for (i, p) in a.iter().enumerate() {
            for (j, q) in b.iter().enumerate() {
                let indep = p * q;
                assert!(bounds.lower[i][j] <= indep + 1e-12);
                assert!(bounds.upper[i][j] >= indep - 1e-12);
                assert!(bounds.lower[i][j] <= bounds.upper[i][j]);
            }
        }
    }

    #[test]
    fn heavy_marginals_force_positive_lower_bound() {
        // p=0.9, q=0.9 → the cell must carry at least 0.8 mass.
        let bounds = frechet_bounds(&[0.9, 0.1], &[0.9, 0.1]);
        assert!((bounds.lower[0][0] - 0.8).abs() < 1e-12);
        assert!((bounds.upper[0][0] - 0.9).abs() < 1e-12);
        // Light cells stay at zero.
        assert_eq!(bounds.lower[1][1], 0.0);
        assert!((bounds.upper[1][1] - 0.1).abs() < 1e-12);
    }

    #[test]
    fn uniform_marginals_have_zero_lower_bounds() {
        let bounds = frechet_bounds(&[0.25; 4], &[0.25; 4]);
        assert!(bounds.lower.iter().flatten().all(|c| *c == 0.0));
        assert!(bounds.upper.iter().flatten().all(|c| (c - 0.25).abs() < 1e-12));
    }

    #[test]
    fn mi_upper_bound_is_min_marginal_entropy() {
        // H = 1 bit vs 2 bits → bound is 1 bit.
        let bound = mi_upper_bound(&[0.5, 0.5], &[0.25, 0.25, 0.25, 0.25]);
        assert!((bound - 1.0).abs() < 1e-12);
    }

    #[test]
    fn deterministic_marginal_permits_no_mi() {
        let bound = mi_upper_bound(&[1.0, 0.0], &[0.5, 0.5]);
        assert!(bound.abs() < 1e-12);
    }
}
