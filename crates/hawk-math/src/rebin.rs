use std::collections::{BTreeMap, BTreeSet};

use hawk_core::DistributionRepr;

pub fn rebin_histogram(
    source: &DistributionRepr,
    target_min: f64,
    target_max: f64,
    target_bins: usize,
) -> Option<DistributionRepr> {
    let DistributionRepr::Histogram {
        min,
        max,
        bin_counts,
        ..
    } = source
    else {
        return None;
    };

    if target_bins == 0 {
        return None;
    }

    let target_bin_width = (target_max - target_min) / target_bins as f64;
    let source_bin_width = (max - min) / bin_counts.len() as f64;

    let mut new_counts = vec![0_u64; target_bins];

    for (src_idx, src_count) in bin_counts.iter().enumerate() {
        if *src_count == 0 {
            continue;
        }

        let src_left = min + src_idx as f64 * source_bin_width;
        let src_right = src_left + source_bin_width;

        let tgt_start = (((src_left - target_min) / target_bin_width).floor() as isize)
            .clamp(0, target_bins as isize - 1) as usize;
        let tgt_end = (((src_right - target_min) / target_bin_width).floor() as isize)
            .clamp(0, target_bins as isize - 1) as usize;

        for tgt_idx in tgt_start..=tgt_end {
            let tgt_left = target_min + tgt_idx as f64 * target_bin_width;
            let tgt_right = tgt_left + target_bin_width;
            let overlap =
                (src_right.min(tgt_right) - src_left.max(tgt_left)).max(0.0) / source_bin_width;
            if overlap <= 0.0 {
                continue;
            }
            new_counts[tgt_idx] += (*src_count as f64 * overlap).round() as u64;
        }
    }

    let total_count = new_counts.iter().sum();
    Some(DistributionRepr::Histogram {
        min: target_min,
        max: target_max,
        bin_counts: new_counts,
        total_count,
    })
}

pub fn align_categorical(
    left_categories: &[String],
    left_counts: &[u64],
    right_categories: &[String],
    right_counts: &[u64],
) -> (Vec<String>, Vec<u64>, Vec<u64>) {
    let mut category_set = BTreeSet::new();
    for c in left_categories {
        category_set.insert(c.clone());
    }
    for c in right_categories {
        category_set.insert(c.clone());
    }

    let mut left_map = BTreeMap::new();
    for (idx, c) in left_categories.iter().enumerate() {
        left_map.insert(c.clone(), left_counts.get(idx).copied().unwrap_or(0));
    }

    let mut right_map = BTreeMap::new();
    for (idx, c) in right_categories.iter().enumerate() {
        right_map.insert(c.clone(), right_counts.get(idx).copied().unwrap_or(0));
    }

    let categories = category_set.into_iter().collect::<Vec<_>>();
    let left = categories
        .iter()
        .map(|c| *left_map.get(c).unwrap_or(&0))
        .collect::<Vec<_>>();
    let right = categories
        .iter()
        .map(|c| *right_map.get(c).unwrap_or(&0))
        .collect::<Vec<_>>();

    (categories, left, right)
}

#[cfg(test)]
mod tests {
    use hawk_core::DistributionRepr;

    use super::{align_categorical, rebin_histogram};

    #[test]
    fn categorical_union_alignment() {
        let (cats, l, r) = align_categorical(
            &["a".into(), "b".into()],
            &[1, 2],
            &["b".into(), "c".into()],
            &[3, 4],
        );
        assert_eq!(cats, vec!["a", "b", "c"]);
        assert_eq!(l, vec![1, 2, 0]);
        assert_eq!(r, vec![0, 3, 4]);
    }

    #[test]
    fn rebin_preserves_mass_approximately() {
        let src = DistributionRepr::Histogram {
            min: 0.0,
            max: 10.0,
            bin_counts: vec![10, 20, 30, 40],
            total_count: 100,
        };

        let rebinned = rebin_histogram(&src, 0.0, 10.0, 5).expect("histogram expected");
        let DistributionRepr::Histogram { total_count, .. } = rebinned else {
            panic!("must stay histogram")
        };

        assert!((total_count as i64 - 100).abs() <= 1);
    }
}
