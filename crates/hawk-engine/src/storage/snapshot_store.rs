use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::core::{canonical_dimension_key, DimensionKey, DistributionObject, DistributionRepr};
use crate::math::jsd;

/// Epsilon used by the AUDIT STORAGE advisory line. JSD (base 2) is at most
/// 1 bit, so this flags near-duplicate neighbors only.
pub const DEFAULT_SNAPSHOT_EPSILON_BITS: f64 = 0.01;

/// Advisory summary of the snapshot store, for AUDIT STORAGE.
#[derive(Debug, Clone)]
pub struct SnapshotAudit {
    pub entries: usize,
    /// Entries droppable by `compact` at the given epsilon.
    pub redundant: usize,
    pub serialized_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub version: u64,
    pub timestamp: u64,
    pub distribution: DistributionObject,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SnapshotStore {
    pub entries: HashMap<String, Vec<SnapshotEntry>>,
}

impl SnapshotStore {
    fn key(variable: &str, dimension_key: &DimensionKey) -> String {
        format!("{variable}|{}", canonical_dimension_key(dimension_key))
    }

    pub fn push_snapshot(&mut self, dist: &DistributionObject) {
        let key = Self::key(&dist.variable, &dist.dimension_key);
        self.entries.entry(key).or_default().push(SnapshotEntry {
            version: dist.version,
            timestamp: dist.last_updated,
            distribution: dist.clone(),
        });
    }

    pub fn get_snapshots(
        &self,
        variable: &str,
        dimension_key: &DimensionKey,
    ) -> Vec<SnapshotEntry> {
        self.entries
            .get(&Self::key(variable, dimension_key))
            .cloned()
            .unwrap_or_default()
    }

    pub fn total_entries(&self) -> usize {
        self.entries.values().map(Vec::len).sum()
    }

    /// Entries `compact` would drop at this epsilon, without dropping them.
    pub fn count_redundant(&self, epsilon_bits: f64) -> usize {
        self.entries
            .values()
            .map(|seq| redundant_indices(seq, epsilon_bits).len())
            .sum()
    }

    /// Drop snapshots whose JSD to both temporal neighbors is below
    /// `epsilon_bits` — they are reconstructible (to within epsilon) from
    /// either neighbor. The first and last snapshot of every sequence are
    /// always kept. Deletes entries only; the persisted shape is unchanged.
    /// Returns the number of entries removed.
    pub fn compact(&mut self, epsilon_bits: f64) -> usize {
        let mut removed = 0;
        for seq in self.entries.values_mut() {
            let drop = redundant_indices(seq, epsilon_bits);
            if drop.is_empty() {
                continue;
            }
            removed += drop.len();
            let mut idx = 0;
            seq.retain(|_| {
                let dropped = drop.contains(&idx);
                idx += 1;
                !dropped
            });
        }
        removed
    }
}

/// Indices droppable from one time-ordered sequence: an interior snapshot is
/// redundant when its JSD to the previously kept snapshot AND to the next
/// original snapshot are both below epsilon. Endpoints are never dropped;
/// shape mismatches are never dropped.
fn redundant_indices(seq: &[SnapshotEntry], epsilon_bits: f64) -> Vec<usize> {
    let mut drop = Vec::new();
    if seq.len() < 3 || !epsilon_bits.is_finite() || epsilon_bits <= 0.0 {
        return drop;
    }

    let mut last_kept = 0;
    for i in 1..seq.len() - 1 {
        let redundant = snapshot_jsd(&seq[last_kept], &seq[i])
            .zip(snapshot_jsd(&seq[i], &seq[i + 1]))
            .is_some_and(|(to_prev, to_next)| to_prev < epsilon_bits && to_next < epsilon_bits);
        if redundant {
            drop.push(i);
        } else {
            last_kept = i;
        }
    }
    drop
}

fn snapshot_jsd(a: &SnapshotEntry, b: &SnapshotEntry) -> Option<f64> {
    // Equal vector lengths do not imply equal events: a count at index zero
    // can name a different category or cover a different numeric interval.
    // Conservatively retain changed schemas instead of inventing a rebinning.
    let compatible = match (&a.distribution.repr, &b.distribution.repr) {
        (
            DistributionRepr::Categorical {
                categories: ac,
                counts: av,
                ..
            },
            DistributionRepr::Categorical {
                categories: bc,
                counts: bv,
                ..
            },
        ) => ac == bc && ac.len() == av.len() && bc.len() == bv.len(),
        (
            DistributionRepr::Histogram {
                min: amin,
                max: amax,
                bin_counts: ac,
                ..
            },
            DistributionRepr::Histogram {
                min: bmin,
                max: bmax,
                bin_counts: bc,
                ..
            },
        ) => amin == bmin && amax == bmax && ac.len() == bc.len(),
        _ => false,
    };
    if !compatible {
        return None;
    }
    let a_counts = a.distribution.repr.value_count_vector();
    let b_counts = b.distribution.repr.value_count_vector();
    if a_counts.len() != b_counts.len() {
        return None;
    }
    Some(jsd(
        &a_counts,
        &b_counts,
        a.distribution.repr.total_count(),
        b.distribution.repr.total_count(),
    ))
}

#[cfg(test)]
mod tests {
    use crate::core::{dimension_key_from_pairs, DistributionObject, DistributionRepr};

    use super::SnapshotStore;

    fn snapshot_store_with(counts: &[[u64; 2]]) -> SnapshotStore {
        let key = dimension_key_from_pairs([("time", "2024")]);
        let mut store = SnapshotStore::default();
        for (i, c) in counts.iter().enumerate() {
            let repr = DistributionRepr::Categorical {
                categories: vec!["a".into(), "b".into()],
                counts: c.to_vec(),
                unknown_count: 0,
                total_count: c.iter().sum(),
            };
            let mut dist = DistributionObject::new(1, "var", key.clone(), repr);
            dist.version = i as u64 + 1;
            store.push_snapshot(&dist);
        }
        store
    }

    #[test]
    fn invalid_thresholds_do_not_discard_history() {
        for epsilon in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0, 0.0] {
            let mut store = snapshot_store_with(&[[50, 50]; 3]);
            assert_eq!(store.count_redundant(epsilon), 0);
            assert_eq!(store.compact(epsilon), 0);
            assert_eq!(store.total_entries(), 3);
        }
    }

    #[test]
    fn equal_length_vectors_with_different_meaning_are_preserved() {
        let category = |labels: &[&str]| DistributionRepr::Categorical {
            categories: labels.iter().map(|s| (*s).into()).collect(),
            counts: vec![100, 0],
            unknown_count: 0,
            total_count: 100,
        };
        let histogram = |min, max, counts: Vec<u64>| DistributionRepr::Histogram {
            min,
            max,
            total_count: counts.iter().sum(),
            bin_counts: counts,
        };
        for (a, b) in [
            (category(&["a", "b"]), category(&["b", "a"])),
            (category(&["a", "b"]), category(&["c", "d"])),
            (
                histogram(0.0, 1.0, vec![100, 0]),
                histogram(1.0, 2.0, vec![100, 0]),
            ),
            (category(&["a", "b"]), histogram(0.0, 1.0, vec![100, 0, 0])),
        ] {
            let mut store = SnapshotStore::default();
            for (i, repr) in [a.clone(), b, a].into_iter().enumerate() {
                let mut dist = DistributionObject::new(1, "var", Default::default(), repr);
                dist.version = i as u64;
                store.push_snapshot(&dist);
            }
            assert_eq!(store.count_redundant(0.01), 0);
            assert_eq!(store.compact(0.01), 0);
            assert_eq!(store.total_entries(), 3);
        }
    }

    #[test]
    fn compact_drops_redundant_interior_snapshots() {
        // Snapshots 2 and 3 are near-identical to both neighbors; snapshot 4
        // precedes a jump, so it is kept as the last pre-jump state.
        let mut store = snapshot_store_with(&[[50, 50], [50, 51], [51, 50], [50, 50], [90, 10]]);
        let removed = store.compact(0.01);
        assert_eq!(removed, 2);

        let key = dimension_key_from_pairs([("time", "2024")]);
        let remaining = store.get_snapshots("var", &key);
        let versions: Vec<u64> = remaining.iter().map(|e| e.version).collect();
        assert_eq!(versions, vec![1, 4, 5]);
    }

    #[test]
    fn compact_always_keeps_first_and_last() {
        let mut store = snapshot_store_with(&[[50, 50], [50, 50], [50, 50]]);
        store.compact(1.0);
        let key = dimension_key_from_pairs([("time", "2024")]);
        let remaining = store.get_snapshots("var", &key);
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].version, 1);
        assert_eq!(remaining[1].version, 3);
    }

    #[test]
    fn compact_keeps_snapshots_that_differ_from_a_neighbor() {
        // Middle snapshot is far from both neighbors; nothing droppable.
        let mut store = snapshot_store_with(&[[100, 0], [0, 100], [100, 0]]);
        assert_eq!(store.compact(0.01), 0);
        assert_eq!(store.total_entries(), 3);
    }

    #[test]
    fn short_sequences_are_untouched() {
        let mut store = snapshot_store_with(&[[50, 50], [50, 50]]);
        assert_eq!(store.compact(1.0), 0);
        assert_eq!(store.total_entries(), 2);
    }

    #[test]
    fn count_redundant_matches_compact_without_mutating() {
        let mut store = snapshot_store_with(&[[50, 50], [50, 51], [51, 50], [50, 50], [90, 10]]);
        let advisory = store.count_redundant(0.01);
        assert_eq!(store.total_entries(), 5, "advisory count must not mutate");
        assert_eq!(advisory, store.compact(0.01));
    }
}
