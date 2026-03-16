/// Hellinger distance between two discrete distributions.
///
/// H(P,Q) = (1/√2) × √( Σ (√p_i - √q_i)² )
///
/// Bounded in [0, 1]. Symmetric. Related to Rényi divergence of order α=½.
pub fn hellinger(p_counts: &[u64], q_counts: &[u64], p_total: u64, q_total: u64) -> f64 {
    assert_eq!(p_counts.len(), q_counts.len(), "vector lengths must match");
    if p_total == 0 && q_total == 0 {
        return 0.0;
    }
    if p_total == 0 || q_total == 0 {
        return 1.0;
    }

    let sum_sq: f64 = p_counts
        .iter()
        .zip(q_counts)
        .map(|(p, q)| {
            let sp = (*p as f64 / p_total as f64).sqrt();
            let sq = (*q as f64 / q_total as f64).sqrt();
            (sp - sq).powi(2)
        })
        .sum();

    (sum_sq / 2.0).sqrt()
}

#[cfg(test)]
mod tests {
    use super::hellinger;

    #[test]
    fn hellinger_identical_is_zero() {
        let h = hellinger(&[5, 5], &[5, 5], 10, 10);
        assert!(h.abs() < 1e-12);
    }

    #[test]
    fn hellinger_symmetric() {
        let a = hellinger(&[9, 1], &[1, 9], 10, 10);
        let b = hellinger(&[1, 9], &[9, 1], 10, 10);
        assert!((a - b).abs() < 1e-12);
    }

    #[test]
    fn hellinger_in_bounds() {
        let h = hellinger(&[9, 1], &[1, 9], 10, 10);
        assert!(h >= 0.0 && h <= 1.0);
    }

    #[test]
    fn hellinger_disjoint_is_one() {
        let h = hellinger(&[10, 0], &[0, 10], 10, 10);
        assert!((h - 1.0).abs() < 1e-12);
    }
}
