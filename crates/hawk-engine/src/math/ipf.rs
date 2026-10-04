use crate::core::{HawkError, Result};

/// L1 convergence tolerance for each fitted marginal against its target.
pub const IPF_TOLERANCE: f64 = 1e-10;
pub const IPF_MAX_ITERATIONS: usize = 1_000;

#[derive(Debug, Clone)]
pub struct IpfResult {
    /// Fitted joint probabilities; rows follow marginal A, columns marginal B.
    pub joint: Vec<Vec<f64>>,
    pub iterations: usize,
    pub converged: bool,
    /// L1 change of the final sweep.
    pub l1_change: f64,
}

/// Maximum-entropy joint of two marginals: IPF from a uniform initialization,
/// which with only the two marginals as constraints is exactly the
/// independence product p_a ⊗ p_b (IPF converges in one sweep).
pub fn maxent_joint(marginal_a: &[f64], marginal_b: &[f64]) -> Result<IpfResult> {
    let uniform = vec![vec![1.0; marginal_b.len()]; marginal_a.len()];
    ipf(
        &uniform,
        marginal_a,
        marginal_b,
        IPF_TOLERANCE,
        IPF_MAX_ITERATIONS,
    )
}

/// Iterative proportional fitting: alternately rescale rows and columns of
/// `initial` until both marginals match the targets. Targets and the initial
/// table are normalized internally, so any non-negative weights are accepted.
pub fn ipf(
    initial: &[Vec<f64>],
    target_a: &[f64],
    target_b: &[f64],
    tolerance: f64,
    max_iterations: usize,
) -> Result<IpfResult> {
    if !tolerance.is_finite() || tolerance <= 0.0 {
        return Err(HawkError::TypeMismatch(
            "IPF tolerance must be finite and positive".to_owned(),
        ));
    }
    let p_a = normalize_marginal(target_a, "A")?;
    let p_b = normalize_marginal(target_b, "B")?;

    if initial.len() != p_a.len() || initial.iter().any(|row| row.len() != p_b.len()) {
        return Err(HawkError::TypeMismatch(format!(
            "IPF initial table must be {}×{} to match the target marginals",
            p_a.len(),
            p_b.len()
        )));
    }
    if initial
        .iter()
        .any(|row| row.iter().any(|c| !c.is_finite() || *c < 0.0))
    {
        return Err(HawkError::TypeMismatch(
            "IPF initial table must contain finite non-negative values".to_owned(),
        ));
    }

    let scale = initial.iter().flatten().copied().fold(0.0, f64::max);
    if scale == 0.0 {
        return Err(HawkError::TypeMismatch(
            "IPF initial table has zero total mass".to_owned(),
        ));
    }
    // Scale before summation so finite weights cannot overflow their total.
    let total: f64 = initial.iter().flatten().map(|c| c / scale).sum();
    let mut joint: Vec<Vec<f64>> = initial
        .iter()
        .map(|row| row.iter().map(|c| (c / scale) / total).collect())
        .collect();

    let mut iterations = 0;
    let mut l1_change = f64::INFINITY;
    let mut converged = false;

    while iterations < max_iterations {
        let previous = joint.clone();

        // Row scaling toward marginal A.
        for (row, &target) in joint.iter_mut().zip(&p_a) {
            let sum: f64 = row.iter().sum();
            if sum > 0.0 {
                for cell in row.iter_mut() {
                    *cell = (*cell / sum) * target;
                }
            } else if target > 0.0 {
                // Structural zeros in `initial` make this marginal unreachable.
                return Err(HawkError::TypeMismatch(
                    "IPF cannot fit: initial table has zero mass on a row whose target marginal is positive".to_owned(),
                ));
            }
        }

        // Column scaling toward marginal B.
        for (j, &target) in p_b.iter().enumerate() {
            let sum: f64 = joint.iter().map(|row| row[j]).sum();
            if sum > 0.0 {
                for row in joint.iter_mut() {
                    row[j] = (row[j] / sum) * target;
                }
            } else if target > 0.0 {
                return Err(HawkError::TypeMismatch(
                    "IPF cannot fit: initial table has zero mass on a column whose target marginal is positive".to_owned(),
                ));
            }
        }

        iterations += 1;
        l1_change = joint
            .iter()
            .flatten()
            .zip(previous.iter().flatten())
            .map(|(a, b)| (a - b).abs())
            .sum();
        // A stationary sweep does not imply feasible marginals: structural
        // zeros can cause row and column scaling to undo one another forever.
        let row_error: f64 = joint
            .iter()
            .zip(&p_a)
            .map(|(row, target)| (row.iter().sum::<f64>() - target).abs())
            .sum();
        let col_error: f64 = p_b
            .iter()
            .enumerate()
            .map(|(j, target)| (joint.iter().map(|row| row[j]).sum::<f64>() - target).abs())
            .sum();
        if row_error <= tolerance && col_error <= tolerance {
            converged = true;
            break;
        }
    }

    Ok(IpfResult {
        joint,
        iterations,
        converged,
        l1_change,
    })
}

fn normalize_marginal(target: &[f64], name: &str) -> Result<Vec<f64>> {
    if target.is_empty() {
        return Err(HawkError::TypeMismatch(format!(
            "IPF target marginal {} is empty",
            name
        )));
    }
    if target.iter().any(|p| !p.is_finite() || *p < 0.0) {
        return Err(HawkError::TypeMismatch(format!(
            "IPF target marginal {} must contain finite non-negative values",
            name
        )));
    }
    let scale = target.iter().copied().fold(0.0, f64::max);
    if scale == 0.0 {
        return Err(HawkError::TypeMismatch(format!(
            "IPF target marginal {} has zero mass",
            name
        )));
    }
    let total: f64 = target.iter().map(|p| p / scale).sum();
    Ok(target.iter().map(|p| (p / scale) / total).collect())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{ipf, maxent_joint, IPF_MAX_ITERATIONS, IPF_TOLERANCE};

    fn marginals(joint: &[Vec<f64>]) -> (Vec<f64>, Vec<f64>) {
        let rows: Vec<f64> = joint.iter().map(|r| r.iter().sum()).collect();
        let cols: Vec<f64> = (0..joint[0].len())
            .map(|j| joint.iter().map(|r| r[j]).sum())
            .collect();
        (rows, cols)
    }

    fn positive_marginal(len: std::ops::Range<usize>) -> impl Strategy<Value = Vec<f64>> {
        proptest::collection::vec(0.01f64..1.0, len)
    }

    proptest! {
        // (a) Uniform initialization yields exactly the independence product.
        #[test]
        fn uniform_init_gives_independence_product(
            a in positive_marginal(2..6),
            b in positive_marginal(2..6),
        ) {
            let ta: f64 = a.iter().sum();
            let tb: f64 = b.iter().sum();
            let result = maxent_joint(&a, &b).expect("ipf");
            prop_assert!(result.converged);
            for (i, row) in result.joint.iter().enumerate() {
                for (j, cell) in row.iter().enumerate() {
                    let expected = (a[i] / ta) * (b[j] / tb);
                    prop_assert!((cell - expected).abs() < 1e-9);
                }
            }
        }

        // (b) Fitted row and column sums match the target marginals.
        #[test]
        fn fitted_marginals_match_targets(
            a in positive_marginal(2..5),
            b in positive_marginal(2..5),
            seed in proptest::collection::vec(0.01f64..1.0, 4..25),
        ) {
            let initial: Vec<Vec<f64>> = (0..a.len())
                .map(|i| (0..b.len()).map(|j| seed[(i * b.len() + j) % seed.len()]).collect())
                .collect();
            let result = ipf(&initial, &a, &b, IPF_TOLERANCE, IPF_MAX_ITERATIONS).expect("ipf");
            prop_assert!(result.converged);
            let ta: f64 = a.iter().sum();
            let tb: f64 = b.iter().sum();
            let (rows, cols) = marginals(&result.joint);
            for (got, want) in rows.iter().zip(&a) {
                prop_assert!((got - want / ta).abs() < 1e-8);
            }
            for (got, want) in cols.iter().zip(&b) {
                prop_assert!((got - want / tb).abs() < 1e-8);
            }
        }

        // (c) A stored joint is a fixed point of IPF over its own marginals.
        #[test]
        fn stored_joint_is_a_fixed_point(
            cells in proptest::collection::vec(0.01f64..1.0, 9),
        ) {
            let joint: Vec<Vec<f64>> = cells.chunks(3).map(<[f64]>::to_vec).collect();
            let total: f64 = cells.iter().sum();
            let (rows, cols) = marginals(&joint);
            let result = ipf(&joint, &rows, &cols, IPF_TOLERANCE, IPF_MAX_ITERATIONS).expect("ipf");
            prop_assert!(result.converged);
            for (fitted_row, original_row) in result.joint.iter().zip(&joint) {
                for (fitted, original) in fitted_row.iter().zip(original_row) {
                    prop_assert!((fitted - original / total).abs() < 1e-9);
                }
            }
        }
    }

    #[test]
    fn maxent_of_uniform_marginals_is_uniform() {
        let result = maxent_joint(&[0.5, 0.5], &[0.25, 0.25, 0.25, 0.25]).expect("ipf");
        assert!(result.converged);
        for row in &result.joint {
            for cell in row {
                assert!((cell - 0.125).abs() < 1e-12);
            }
        }
    }

    #[test]
    fn zero_target_category_gets_zero_mass() {
        let result = maxent_joint(&[0.5, 0.5, 0.0], &[1.0, 1.0]).expect("ipf");
        assert!(result.converged);
        assert!(result.joint[2].iter().all(|c| c.abs() < 1e-12));
        let total: f64 = result.joint.iter().flatten().sum();
        assert!((total - 1.0).abs() < 1e-9);
    }

    #[test]
    fn zero_mass_marginal_is_an_error() {
        assert!(maxent_joint(&[0.0, 0.0], &[0.5, 0.5]).is_err());
        assert!(maxent_joint(&[0.5, 0.5], &[]).is_err());
    }

    #[test]
    fn mismatched_shape_is_an_error() {
        let initial = vec![vec![1.0, 1.0]; 2];
        assert!(ipf(&initial, &[0.5, 0.5, 0.0], &[0.5, 0.5], 1e-10, 10).is_err());
    }

    #[test]
    fn structural_zero_row_with_positive_target_is_an_error() {
        let initial = vec![vec![0.0, 0.0], vec![1.0, 1.0]];
        assert!(ipf(&initial, &[0.5, 0.5], &[0.5, 0.5], 1e-10, 10).is_err());
    }

    #[test]
    fn incompatible_structural_support_does_not_report_convergence() {
        let initial = vec![vec![1.0, 0.0], vec![0.0, 1.0]];
        let result = ipf(&initial, &[0.8, 0.2], &[0.2, 0.8], 1e-10, 10).unwrap();
        assert_eq!(result.l1_change, 0.0);
        assert!(!result.converged);
        assert_eq!(result.iterations, 10);
    }

    #[test]
    fn finite_weights_with_overflowing_sums_are_normalized() {
        let initial = vec![vec![f64::MAX; 2]; 2];
        let target = [f64::MAX; 2];
        let result = ipf(&initial, &target, &target, IPF_TOLERANCE, 10).unwrap();
        assert!(result.converged);
        assert!(result
            .joint
            .iter()
            .flatten()
            .all(|p| (*p - 0.25).abs() < 1e-12));
    }

    #[test]
    fn rescaling_tiny_positive_support_stays_finite() {
        let initial = vec![vec![1e-310, 1e-310], vec![0.5, 0.5]];
        let result = ipf(&initial, &[0.5, 0.5], &[0.5, 0.5], IPF_TOLERANCE, 10).unwrap();
        assert!(result.converged);
        assert!(result
            .joint
            .iter()
            .flatten()
            .all(|p| (*p - 0.25).abs() < 1e-12));
    }

    #[test]
    fn invalid_tolerance_is_rejected() {
        for tolerance in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(ipf(&[vec![1.0]], &[1.0], &[1.0], tolerance, 10).is_err());
        }
    }

    #[test]
    fn invalid_weights_are_rejected() {
        for value in [-1.0, f64::NAN, f64::INFINITY] {
            assert!(maxent_joint(&[value, 1.0], &[1.0]).is_err());
            assert!(ipf(&[vec![value]], &[1.0], &[1.0], IPF_TOLERANCE, 10).is_err());
        }
    }

    #[test]
    fn iteration_cap_is_respected() {
        // One sweep is not enough only for a genuinely constrained fit; with a
        // cap of 0 sweeps the loop must exit unconverged rather than hang.
        let initial = vec![vec![0.9, 0.1], vec![0.1, 0.9]];
        let result = ipf(&initial, &[0.3, 0.7], &[0.6, 0.4], 1e-15, 0).expect("ipf");
        assert!(!result.converged);
        assert_eq!(result.iterations, 0);
    }
}
