pub fn entropy(counts: &[u64], total: u64) -> f64 {
    if total == 0 {
        return 0.0;
    }

    counts
        .iter()
        .filter(|&&c| c > 0)
        .map(|&c| {
            let p = c as f64 / total as f64;
            -p * p.log2()
        })
        .sum()
}

/// Shannon entropy of a probability vector, in bits. Mass is used as given;
/// callers are expected to pass a normalized vector.
pub fn entropy_from_probs(probs: &[f64]) -> f64 {
    probs
        .iter()
        .filter(|&&p| p > 0.0)
        .map(|&p| -p * p.log2())
        .sum()
}

#[cfg(test)]
mod tests {
    use super::{entropy, entropy_from_probs};

    #[test]
    fn entropy_of_uniform_four_bins() {
        let h = entropy(&[1, 1, 1, 1], 4);
        assert!((h - 2.0).abs() < 1e-12);
    }

    #[test]
    fn entropy_of_delta_is_zero() {
        let h = entropy(&[10, 0, 0], 10);
        assert!(h.abs() < 1e-12);
    }

    #[test]
    fn entropy_from_probs_matches_counts() {
        let h = entropy_from_probs(&[0.25, 0.25, 0.5]);
        assert!((h - entropy(&[1, 1, 2], 4)).abs() < 1e-12);
    }
}
