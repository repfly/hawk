pub fn wasserstein_1(
    p_counts: &[u64],
    q_counts: &[u64],
    p_total: u64,
    q_total: u64,
    bin_width: f64,
) -> f64 {
    assert_eq!(p_counts.len(), q_counts.len(), "vector lengths must match");
    if p_total == 0 || q_total == 0 {
        return 0.0;
    }

    let mut cdf_p = 0.0;
    let mut cdf_q = 0.0;
    let mut w = 0.0;

    for i in 0..p_counts.len() {
        cdf_p += p_counts[i] as f64 / p_total as f64;
        cdf_q += q_counts[i] as f64 / q_total as f64;
        w += (cdf_p - cdf_q).abs() * bin_width;
    }

    w
}

#[cfg(test)]
mod tests {
    use super::wasserstein_1;

    #[test]
    fn wasserstein_identical_zero() {
        let w = wasserstein_1(&[1, 3, 1], &[1, 3, 1], 5, 5, 1.0);
        assert!(w.abs() < 1e-12);
    }
}
