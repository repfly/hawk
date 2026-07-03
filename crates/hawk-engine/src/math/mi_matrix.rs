use crate::math::mutual_information;

/// One known pairwise MI entry, canonically ordered (`var_a < var_b`).
#[derive(Debug, Clone, PartialEq)]
pub struct MiEdge {
    pub var_a: String,
    pub var_b: String,
    pub mi: f64,
    pub sample_count: u64,
}

/// All-pairs MI over a variable set. Pairs without a stored joint are listed
/// in `unknown_pairs` — never silently treated as zero MI.
#[derive(Debug, Clone, Default)]
pub struct MiMatrix {
    /// Variables considered, sorted lexicographically.
    pub variables: Vec<String>,
    /// Known pairs, sorted by MI descending (ties: lexicographic edge name).
    pub edges: Vec<MiEdge>,
    /// Pairs with no stored joint, canonically ordered and sorted.
    pub unknown_pairs: Vec<(String, String)>,
}

/// Build the all-pairs MI matrix for `variables`. `lookup` resolves a
/// canonical pair to its stored joint counts `(counts, total)` at the slice
/// under consideration; `None` marks the pair unknown.
pub fn mi_matrix<F>(variables: &[String], lookup: F) -> MiMatrix
where
    F: Fn(&str, &str) -> Option<(Vec<Vec<u64>>, u64)>,
{
    let mut vars: Vec<String> = variables.to_vec();
    vars.sort();
    vars.dedup();

    let mut edges = Vec::new();
    let mut unknown_pairs = Vec::new();

    for i in 0..vars.len() {
        for j in (i + 1)..vars.len() {
            let (a, b) = (&vars[i], &vars[j]);
            match lookup(a, b) {
                Some((counts, total)) => edges.push(MiEdge {
                    var_a: a.clone(),
                    var_b: b.clone(),
                    mi: mutual_information(&counts, total),
                    sample_count: total,
                }),
                None => unknown_pairs.push((a.clone(), b.clone())),
            }
        }
    }

    edges.sort_by(|x, y| {
        y.mi.total_cmp(&x.mi)
            .then_with(|| (&x.var_a, &x.var_b).cmp(&(&y.var_a, &y.var_b)))
    });

    MiMatrix {
        variables: vars,
        edges,
        unknown_pairs,
    }
}

#[cfg(test)]
mod tests {
    use super::mi_matrix;

    fn vars(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| (*n).to_owned()).collect()
    }

    #[test]
    fn all_pairs_known_produces_full_matrix() {
        let correlated = (vec![vec![45, 5], vec![5, 45]], 100u64);
        let m = mi_matrix(&vars(&["a", "b", "c"]), |_, _| Some(correlated.clone()));
        assert_eq!(m.edges.len(), 3);
        assert!(m.unknown_pairs.is_empty());
        assert!(m.edges.iter().all(|e| e.mi > 0.0));
    }

    #[test]
    fn missing_joint_is_reported_unknown_not_zero() {
        let m = mi_matrix(&vars(&["a", "b", "c"]), |x, y| {
            if (x, y) == ("a", "b") {
                Some((vec![vec![25, 25], vec![25, 25]], 100))
            } else {
                None
            }
        });
        assert_eq!(m.edges.len(), 1);
        assert_eq!(
            m.unknown_pairs,
            vec![
                ("a".to_owned(), "c".to_owned()),
                ("b".to_owned(), "c".to_owned())
            ]
        );
        // The known-but-independent pair reports MI 0 explicitly.
        assert!(m.edges[0].mi.abs() < 1e-12);
    }

    #[test]
    fn variable_order_is_canonical_regardless_of_input() {
        let m = mi_matrix(&vars(&["c", "a", "b", "a"]), |_, _| None);
        assert_eq!(m.variables, vars(&["a", "b", "c"]));
        assert_eq!(m.unknown_pairs.len(), 3);
    }

    #[test]
    fn edges_sorted_by_mi_then_name() {
        let strong = vec![vec![50u64, 0], vec![0, 50]];
        let indep = vec![vec![25u64, 25], vec![25, 25]];
        let m = mi_matrix(&vars(&["a", "b", "c"]), |x, y| {
            Some(if (x, y) == ("b", "c") {
                (strong.clone(), 100)
            } else {
                (indep.clone(), 100)
            })
        });
        assert_eq!((m.edges[0].var_a.as_str(), m.edges[0].var_b.as_str()), ("b", "c"));
        // Tied zero-MI edges fall back to lexicographic order.
        assert_eq!((m.edges[1].var_a.as_str(), m.edges[1].var_b.as_str()), ("a", "b"));
        assert_eq!((m.edges[2].var_a.as_str(), m.edges[2].var_b.as_str()), ("a", "c"));
    }
}
