use std::collections::HashMap;

use anyhow::{anyhow, Result};

use crate::core::dimension_key_from_pairs;
use crate::math::{chow_liu_tree, mi_matrix};
use crate::storage::Database;

use crate::query::result_types::{
    ReweightedEdge, StructureDiffResult, StructureEdge, StructureResult,
};

/// Build the Chow-Liu dependency tree over all schema variables at a
/// dimension slice, from stored joints only. Pairs without a stored joint
/// are reported as unknown; missing joints can make the result a forest.
pub fn execute_structure(
    db: &Database,
    reference: &str,
    dims: &HashMap<String, String>,
) -> Result<StructureResult> {
    let variables: Vec<String> = db
        .schema()
        .variables
        .iter()
        .map(|v| v.name.clone())
        .collect();
    if variables.len() < 2 {
        return Err(anyhow!(
            "STRUCTURE needs at least 2 variables in the schema, found {}",
            variables.len()
        ));
    }

    let matrix = mi_matrix(&variables, |a, b| resolve_joint_counts(db, a, b, dims));
    let tree = chow_liu_tree(&matrix.variables, &matrix.edges);

    let sample_count_of = |a: &str, b: &str| {
        matrix
            .edges
            .iter()
            .find(|e| e.var_a == a && e.var_b == b)
            .map(|e| e.sample_count)
            .unwrap_or(0)
    };

    Ok(StructureResult {
        reference: reference.to_owned(),
        variables: tree.variables.clone(),
        edges: tree
            .edges
            .iter()
            .map(|e| StructureEdge {
                var_a: e.var_a.clone(),
                var_b: e.var_b.clone(),
                mi: e.mi,
                sample_count: sample_count_of(&e.var_a, &e.var_b),
            })
            .collect(),
        retained_information: tree.retained_information,
        components: tree.components,
        unknown_pairs: matrix.unknown_pairs,
    })
}

/// Structural diff of two Chow-Liu trees: added / dropped / re-weighted
/// edges, an MI-weighted rewiring score, and the retained-information delta.
pub fn diff_structures(a: &StructureResult, b: &StructureResult) -> StructureDiffResult {
    let in_a = |e: &StructureEdge| {
        a.edges
            .iter()
            .find(|x| x.var_a == e.var_a && x.var_b == e.var_b)
    };
    let in_b = |e: &StructureEdge| {
        b.edges
            .iter()
            .find(|x| x.var_a == e.var_a && x.var_b == e.var_b)
    };

    let added_edges: Vec<StructureEdge> = b
        .edges
        .iter()
        .filter(|e| in_a(e).is_none())
        .cloned()
        .collect();
    let dropped_edges: Vec<StructureEdge> = a
        .edges
        .iter()
        .filter(|e| in_b(e).is_none())
        .cloned()
        .collect();

    let mut reweighted_edges: Vec<ReweightedEdge> = a
        .edges
        .iter()
        .filter_map(|ea| {
            in_b(ea).map(|eb| ReweightedEdge {
                var_a: ea.var_a.clone(),
                var_b: ea.var_b.clone(),
                mi_a: ea.mi,
                mi_b: eb.mi,
                delta: eb.mi - ea.mi,
            })
        })
        .collect();
    reweighted_edges.sort_by(|x, y| y.delta.abs().total_cmp(&x.delta.abs()));

    let rewired_mass: f64 = dropped_edges.iter().map(|e| e.mi).sum::<f64>()
        + added_edges.iter().map(|e| e.mi).sum::<f64>();
    let total_mass: f64 =
        a.edges.iter().map(|e| e.mi).sum::<f64>() + b.edges.iter().map(|e| e.mi).sum::<f64>();
    let rewiring_score = if total_mass > 0.0 {
        (rewired_mass / total_mass).clamp(0.0, 1.0)
    } else {
        0.0
    };

    StructureDiffResult {
        retained_information_delta: b.retained_information - a.retained_information,
        structure_a: a.clone(),
        structure_b: b.clone(),
        added_edges,
        dropped_edges,
        reweighted_edges,
        rewiring_score,
    }
}

/// Locate the stored joint counts for a pair at a slice: exact dimension-key
/// match first, otherwise aggregate all stored joints whose key contains the
/// requested dimensions — the same resolution `resolve_distribution` uses
/// for marginals. `None` means the pair is unknown at this slice.
pub(crate) fn resolve_joint_counts(
    db: &Database,
    var_a: &str,
    var_b: &str,
    dims: &HashMap<String, String>,
) -> Option<(Vec<Vec<u64>>, u64)> {
    let dim_key = dimension_key_from_pairs(dims.iter().map(|(k, v)| (k.clone(), v.clone())));
    if let Some(joint) = db.get_joint_distribution(var_a, var_b, &dim_key) {
        return Some(extract_joint_counts(joint));
    }

    let mut merged: Option<(Vec<Vec<u64>>, u64)> = None;
    for joint in db.joints_for_pair(var_a, var_b) {
        let matches = dims
            .iter()
            .all(|(k, v)| joint.dimension_key.get(k) == Some(v));
        if !matches {
            continue;
        }
        let (counts, total) = extract_joint_counts(joint);
        match &mut merged {
            None => merged = Some((counts, total)),
            Some((acc, acc_total)) => {
                // Schema fixes the grid shape per pair; skip anything odd.
                if acc.len() == counts.len()
                    && acc.iter().zip(&counts).all(|(r, s)| r.len() == s.len())
                {
                    for (row, other) in acc.iter_mut().zip(&counts) {
                        for (cell, v) in row.iter_mut().zip(other) {
                            *cell += v;
                        }
                    }
                    *acc_total += total;
                }
            }
        }
    }
    merged
}

fn extract_joint_counts(joint: &crate::core::JointDistributionObject) -> (Vec<Vec<u64>>, u64) {
    use crate::core::JointRepr;
    match &joint.repr {
        JointRepr::HistogramGrid {
            counts,
            total_count,
            ..
        } => (counts.clone(), *total_count),
        JointRepr::ContingencyTable {
            counts,
            total_count,
            ..
        } => (counts.clone(), *total_count),
        JointRepr::ConditionalHistograms {
            histograms,
            total_count,
            ..
        } => {
            let grid: Vec<Vec<u64>> = histograms.iter().map(|h| h.value_count_vector()).collect();
            (grid, *total_count)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::diff_structures;
    use crate::query::result_types::{StructureEdge, StructureResult};

    fn edge(a: &str, b: &str, mi: f64) -> StructureEdge {
        StructureEdge {
            var_a: a.to_owned(),
            var_b: b.to_owned(),
            mi,
            sample_count: 100,
        }
    }

    fn structure(reference: &str, edges: Vec<StructureEdge>) -> StructureResult {
        let retained_information = edges.iter().map(|e| e.mi).sum();
        StructureResult {
            reference: reference.to_owned(),
            variables: vec!["a".into(), "b".into(), "c".into()],
            edges,
            retained_information,
            components: 1,
            unknown_pairs: vec![],
        }
    }

    #[test]
    fn identical_trees_have_zero_rewiring() {
        let a = structure("t:1", vec![edge("a", "b", 0.8), edge("b", "c", 0.4)]);
        let diff = diff_structures(&a, &a);
        assert!(diff.added_edges.is_empty());
        assert!(diff.dropped_edges.is_empty());
        assert_eq!(diff.reweighted_edges.len(), 2);
        assert_eq!(diff.rewiring_score, 0.0);
        assert_eq!(diff.retained_information_delta, 0.0);
    }

    #[test]
    fn flipped_association_is_added_and_dropped() {
        let a = structure("t:1", vec![edge("a", "b", 0.8), edge("b", "c", 0.1)]);
        let b = structure("t:2", vec![edge("a", "b", 0.8), edge("a", "c", 0.7)]);
        let diff = diff_structures(&a, &b);
        assert_eq!(diff.added_edges.len(), 1);
        assert_eq!(diff.added_edges[0].var_b, "c");
        assert_eq!(diff.dropped_edges.len(), 1);
        assert_eq!(diff.dropped_edges[0].var_a, "b");
        // (0.1 + 0.7) / (0.9 + 1.5)
        assert!((diff.rewiring_score - 0.8 / 2.4).abs() < 1e-12);
        assert!((diff.retained_information_delta - 0.6).abs() < 1e-12);
    }

    #[test]
    fn reweighted_edges_sorted_by_abs_delta() {
        let a = structure("t:1", vec![edge("a", "b", 0.8), edge("b", "c", 0.4)]);
        let b = structure("t:2", vec![edge("a", "b", 0.7), edge("b", "c", 0.9)]);
        let diff = diff_structures(&a, &b);
        assert_eq!(diff.reweighted_edges.len(), 2);
        assert_eq!(diff.reweighted_edges[0].var_a, "b");
        assert!((diff.reweighted_edges[0].delta - 0.5).abs() < 1e-12);
        assert!((diff.reweighted_edges[1].delta + 0.1).abs() < 1e-12);
    }

    #[test]
    fn completely_disjoint_trees_score_one() {
        let a = structure("t:1", vec![edge("a", "b", 0.5)]);
        let b = structure("t:2", vec![edge("b", "c", 0.5)]);
        let diff = diff_structures(&a, &b);
        assert_eq!(diff.rewiring_score, 1.0);
    }

    #[test]
    fn empty_trees_score_zero_not_nan() {
        let a = structure("t:1", vec![]);
        let diff = diff_structures(&a, &a);
        assert_eq!(diff.rewiring_score, 0.0);
    }
}
