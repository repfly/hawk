use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConfidenceInfo {
    pub jsd_ci_lower: f64,
    pub jsd_ci_upper: f64,
    pub sufficient_samples: bool,
}

pub fn asymptotic_jsd_confidence(jsd_value: f64, sample_count_a: u64, sample_count_b: u64) -> ConfidenceInfo {
    let sufficient_samples = sample_count_a >= 30 && sample_count_b >= 30;
    let n_eff = ((sample_count_a as f64) * (sample_count_b as f64))
        / ((sample_count_a + sample_count_b).max(1) as f64);

    let se = if n_eff > 0.0 {
        (jsd_value.max(1e-12) * (1.0 - jsd_value).max(1e-12) / n_eff).sqrt()
    } else {
        0.0
    };

    let margin = 1.96 * se;
    ConfidenceInfo {
        jsd_ci_lower: (jsd_value - margin).max(0.0),
        jsd_ci_upper: (jsd_value + margin).min(1.0),
        sufficient_samples,
    }
}

#[cfg(test)]
mod tests {
    use super::asymptotic_jsd_confidence;

    #[test]
    fn confidence_bounds_are_valid() {
        let ci = asymptotic_jsd_confidence(0.2, 100, 100);
        assert!(ci.jsd_ci_lower >= 0.0);
        assert!(ci.jsd_ci_upper <= 1.0);
        assert!(ci.jsd_ci_lower <= ci.jsd_ci_upper);
        assert!(ci.sufficient_samples);
    }
}
