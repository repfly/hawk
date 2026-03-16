pub fn kl_divergence(p_counts: &[u64], q_counts: &[u64], p_total: u64, q_total: u64) -> f64 {
    assert_eq!(p_counts.len(), q_counts.len(), "vector lengths must match");
    if p_total == 0 || q_total == 0 {
        return 0.0;
    }

    let epsilon = 1e-10;
    let k = p_counts.len() as f64;

    p_counts
        .iter()
        .zip(q_counts)
        .filter_map(|(p, q)| {
            if *p == 0 {
                return None;
            }
            let p_i = *p as f64 / p_total as f64;
            let q_i = (*q as f64 + epsilon) / (q_total as f64 + k * epsilon);
            Some(p_i * (p_i / q_i).log2())
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::kl_divergence;

    #[test]
    fn kl_identical_is_zero() {
        let kl = kl_divergence(&[5, 5], &[5, 5], 10, 10);
        assert!(kl.abs() < 1e-10);
    }

    #[test]
    fn kl_non_negative() {
        let kl = kl_divergence(&[9, 1], &[1, 9], 10, 10);
        assert!(kl >= 0.0);
    }
}
