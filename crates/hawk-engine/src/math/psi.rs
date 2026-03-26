use crate::math::kl_divergence::kl_divergence;

/// Population Stability Index: symmetric KL divergence.
/// PSI = KL(P||Q) + KL(Q||P)
///
/// Interpretation:
///   < 0.1  → stable
///   0.1–0.2 → moderate shift
///   > 0.2  → significant shift
pub fn psi(p_counts: &[u64], q_counts: &[u64], p_total: u64, q_total: u64) -> f64 {
    kl_divergence(p_counts, q_counts, p_total, q_total)
        + kl_divergence(q_counts, p_counts, q_total, p_total)
}

#[cfg(test)]
mod tests {
    use super::psi;

    #[test]
    fn psi_identical_is_zero() {
        let v = psi(&[5, 5], &[5, 5], 10, 10);
        assert!(v.abs() < 1e-8);
    }

    #[test]
    fn psi_symmetric() {
        let a = psi(&[9, 1], &[1, 9], 10, 10);
        let b = psi(&[1, 9], &[9, 1], 10, 10);
        assert!((a - b).abs() < 1e-12);
    }

    #[test]
    fn psi_non_negative() {
        let v = psi(&[8, 2], &[3, 7], 10, 10);
        assert!(v >= 0.0);
    }
}
