/// Cramér's V: effect size for categorical association.
///
/// V = √(χ² / (n × min(r-1, c-1)))
///
/// where χ² = Σ (O_ij - E_ij)² / E_ij
///
/// Bounded in [0, 1]. 0 = no association, 1 = perfect association.
/// More interpretable than MI for tables of different dimensions.
pub fn cramers_v(joint_counts: &[Vec<u64>], total: u64) -> f64 {
    if total == 0 || joint_counts.is_empty() || joint_counts[0].is_empty() {
        return 0.0;
    }

    let r = joint_counts.len();
    let c = joint_counts[0].len();
    let min_dim = (r - 1).min(c - 1);
    if min_dim == 0 {
        return 0.0;
    }

    // Compute marginals
    let mut row_sums = vec![0_u64; r];
    let mut col_sums = vec![0_u64; c];
    for (i, row) in joint_counts.iter().enumerate() {
        for (j, &count) in row.iter().enumerate() {
            row_sums[i] += count;
            col_sums[j] += count;
        }
    }

    // Compute chi-squared statistic
    let n = total as f64;
    let mut chi2 = 0.0;
    for (i, row) in joint_counts.iter().enumerate() {
        for (j, &observed) in row.iter().enumerate() {
            let expected = (row_sums[i] as f64 * col_sums[j] as f64) / n;
            if expected > 0.0 {
                let diff = observed as f64 - expected;
                chi2 += diff * diff / expected;
            }
        }
    }

    (chi2 / (n * min_dim as f64)).sqrt().clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::cramers_v;

    #[test]
    fn cramers_v_independent_is_zero() {
        // Perfectly independent: all cells proportional
        let joint = vec![vec![25, 25], vec![25, 25]];
        let v = cramers_v(&joint, 100);
        assert!(v.abs() < 1e-10);
    }

    #[test]
    fn cramers_v_perfect_association() {
        // Perfect association: diagonal only
        let joint = vec![vec![50, 0], vec![0, 50]];
        let v = cramers_v(&joint, 100);
        assert!((v - 1.0).abs() < 1e-10);
    }

    #[test]
    fn cramers_v_in_bounds() {
        let joint = vec![vec![40, 10], vec![15, 35]];
        let v = cramers_v(&joint, 100);
        assert!(v >= 0.0 && v <= 1.0);
    }

    #[test]
    fn cramers_v_rectangular_table() {
        // 2x3 table
        let joint = vec![vec![30, 10, 10], vec![5, 20, 25]];
        let v = cramers_v(&joint, 100);
        assert!(v > 0.0 && v <= 1.0);
    }
}
