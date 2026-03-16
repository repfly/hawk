fn normalize(counts: &[u64], total: u64) -> Vec<f64> {
    if total == 0 {
        return vec![0.0; counts.len()];
    }
    counts.iter().map(|c| *c as f64 / total as f64).collect()
}

pub fn jsd(p_counts: &[u64], q_counts: &[u64], p_total: u64, q_total: u64) -> f64 {
    assert_eq!(p_counts.len(), q_counts.len(), "vector lengths must match");
    if p_total == 0 && q_total == 0 {
        return 0.0;
    }

    let p = normalize(p_counts, p_total);
    let q = normalize(q_counts, q_total);
    let m = p
        .iter()
        .zip(&q)
        .map(|(a, b)| (a + b) * 0.5)
        .collect::<Vec<_>>();

    let kl_to_m = |src: &[f64]| {
        src.iter()
            .zip(&m)
            .filter_map(|(s, mm)| {
                if *s > 0.0 && *mm > 0.0 {
                    Some(*s * (*s / *mm).log2())
                } else {
                    None
                }
            })
            .sum::<f64>()
    };

    let out = 0.5 * kl_to_m(&p) + 0.5 * kl_to_m(&q);
    out.clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::jsd;

    #[test]
    fn jsd_identical_zero() {
        let d = jsd(&[5, 5], &[5, 5], 10, 10);
        assert!(d.abs() < 1e-12);
    }

    #[test]
    fn jsd_symmetric() {
        let a = jsd(&[9, 1], &[1, 9], 10, 10);
        let b = jsd(&[1, 9], &[9, 1], 10, 10);
        assert!((a - b).abs() < 1e-12);
    }

    #[test]
    fn jsd_in_bounds() {
        let d = jsd(&[9, 1], &[1, 9], 10, 10);
        assert!((0.0..=1.0).contains(&d));
    }
}
