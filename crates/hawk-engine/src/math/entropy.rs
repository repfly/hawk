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

#[cfg(test)]
mod tests {
    use super::entropy;

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
}
