pub fn mutual_information(joint_counts: &[Vec<u64>], total: u64) -> f64 {
    if total == 0 || joint_counts.is_empty() || joint_counts[0].is_empty() {
        return 0.0;
    }

    let nx = joint_counts.len();
    let ny = joint_counts[0].len();

    let mut marginal_x = vec![0_u64; nx];
    let mut marginal_y = vec![0_u64; ny];

    for (i, row) in joint_counts.iter().enumerate() {
        for (j, value) in row.iter().enumerate() {
            marginal_x[i] += *value;
            marginal_y[j] += *value;
        }
    }

    let mut mi = 0.0;
    for i in 0..nx {
        for j in 0..ny {
            let n = joint_counts[i][j];
            if n == 0 {
                continue;
            }

            let p_xy = n as f64 / total as f64;
            let p_x = marginal_x[i] as f64 / total as f64;
            let p_y = marginal_y[j] as f64 / total as f64;
            mi += p_xy * (p_xy / (p_x * p_y)).log2();
        }
    }

    mi
}

/// Conditional mutual information: MI(X; Y | Z) = Σ_z P(z) * MI(X; Y | Z=z)
///
/// Takes a slice of (joint_counts, total) pairs, one per conditioning value z.
/// Each `joint_counts` is the joint count table for X×Y restricted to Z=z.
/// Returns the weighted average MI across all conditioning values.
pub fn conditional_mutual_information(slices: &[(Vec<Vec<u64>>, u64)]) -> f64 {
    let grand_total: u64 = slices.iter().map(|(_, t)| t).sum();
    if grand_total == 0 {
        return 0.0;
    }

    let mut cmi = 0.0;
    for (joint_counts, slice_total) in slices {
        if *slice_total == 0 {
            continue;
        }
        let p_z = *slice_total as f64 / grand_total as f64;
        let mi_given_z = mutual_information(joint_counts, *slice_total);
        cmi += p_z * mi_given_z;
    }
    cmi
}

/// Normalized mutual information: MI(X;Y) / min(H(X), H(Y)), in [0, 1].
/// Returns 0 if either marginal has zero entropy.
pub fn normalized_mutual_information(joint_counts: &[Vec<u64>], total: u64) -> f64 {
    if total == 0 || joint_counts.is_empty() || joint_counts[0].is_empty() {
        return 0.0;
    }

    let mi = mutual_information(joint_counts, total);

    let nx = joint_counts.len();
    let ny = joint_counts[0].len();
    let mut marginal_x = vec![0_u64; nx];
    let mut marginal_y = vec![0_u64; ny];
    for (i, row) in joint_counts.iter().enumerate() {
        for (j, value) in row.iter().enumerate() {
            marginal_x[i] += *value;
            marginal_y[j] += *value;
        }
    }

    let h_x = crate::entropy::entropy(&marginal_x, total);
    let h_y = crate::entropy::entropy(&marginal_y, total);
    let min_h = h_x.min(h_y);
    if min_h <= 0.0 {
        return 0.0;
    }
    (mi / min_h).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::mutual_information;

    #[test]
    fn mi_zero_for_independent_distribution() {
        let joint = vec![vec![25, 25], vec![25, 25]];
        let mi = mutual_information(&joint, 100);
        assert!(mi.abs() < 1e-10);
    }

    #[test]
    fn mi_positive_for_correlated_distribution() {
        let joint = vec![vec![45, 5], vec![5, 45]];
        let mi = mutual_information(&joint, 100);
        assert!(mi > 0.0);
    }

    #[test]
    fn cmi_equals_mi_with_single_slice() {
        use super::conditional_mutual_information;
        let joint = vec![vec![45, 5], vec![5, 45]];
        let mi = mutual_information(&joint, 100);
        let cmi = conditional_mutual_information(&[(joint, 100)]);
        assert!((mi - cmi).abs() < 1e-10);
    }

    #[test]
    fn cmi_zero_for_independent_slices() {
        use super::conditional_mutual_information;
        let indep = vec![vec![25, 25], vec![25, 25]];
        let cmi = conditional_mutual_information(&[
            (indep.clone(), 100),
            (indep, 100),
        ]);
        assert!(cmi.abs() < 1e-10);
    }

    #[test]
    fn normalized_mi_is_bounded() {
        use super::normalized_mutual_information;
        let joint = vec![vec![45, 5], vec![5, 45]];
        let nmi = normalized_mutual_information(&joint, 100);
        assert!(nmi > 0.0 && nmi <= 1.0);
    }
}
