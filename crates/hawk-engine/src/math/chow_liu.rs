use crate::math::mi_matrix::MiEdge;

#[derive(Debug, Clone, PartialEq)]
pub struct TreeEdge {
    pub var_a: String,
    pub var_b: String,
    pub mi: f64,
}

/// Chow-Liu dependency tree: the maximum spanning tree over the pairwise MI
/// graph. When joints are missing the graph can be disconnected, in which
/// case the result is a forest (`components > 1`) — reported, not hidden.
#[derive(Debug, Clone, Default)]
pub struct ChowLiuTree {
    /// Variables spanned, sorted lexicographically.
    pub variables: Vec<String>,
    /// Selected edges, sorted by MI descending (ties: lexicographic).
    pub edges: Vec<TreeEdge>,
    /// Σ edge MI, in bits — the information the tree retains about the joint.
    pub retained_information: f64,
    /// Number of connected components; 1 means a proper spanning tree.
    pub components: usize,
}

impl ChowLiuTree {
    pub fn is_forest(&self) -> bool {
        self.components > 1
    }

    pub fn contains_edge(&self, var_a: &str, var_b: &str) -> Option<&TreeEdge> {
        let (a, b) = if var_a <= var_b {
            (var_a, var_b)
        } else {
            (var_b, var_a)
        };
        self.edges.iter().find(|e| e.var_a == a && e.var_b == b)
    }
}

struct UnionFind {
    parent: Vec<usize>,
}

impl UnionFind {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
        }
    }

    fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            self.parent[x] = self.parent[self.parent[x]];
            x = self.parent[x];
        }
        x
    }

    fn union(&mut self, a: usize, b: usize) -> bool {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra == rb {
            return false;
        }
        self.parent[ra] = rb;
        true
    }
}

/// Kruskal maximum spanning tree over the MI edges. Tie-breaking is
/// deterministic — equal-MI edges are taken in lexicographic (var_a, var_b)
/// order — so trees built from different slices are comparable.
pub fn chow_liu_tree(variables: &[String], edges: &[MiEdge]) -> ChowLiuTree {
    let mut vars: Vec<String> = variables.to_vec();
    vars.sort();
    vars.dedup();

    let index_of = |name: &str| vars.binary_search_by(|v| v.as_str().cmp(name)).ok();

    let mut sorted: Vec<&MiEdge> = edges.iter().collect();
    sorted.sort_by(|x, y| {
        y.mi.total_cmp(&x.mi)
            .then_with(|| (&x.var_a, &x.var_b).cmp(&(&y.var_a, &y.var_b)))
    });

    let mut uf = UnionFind::new(vars.len());
    let mut tree_edges = Vec::new();

    for edge in sorted {
        let (Some(i), Some(j)) = (index_of(&edge.var_a), index_of(&edge.var_b)) else {
            continue;
        };
        if i == j {
            continue;
        }
        if uf.union(i, j) {
            tree_edges.push(TreeEdge {
                var_a: edge.var_a.clone(),
                var_b: edge.var_b.clone(),
                mi: edge.mi,
            });
        }
    }

    tree_edges.sort_by(|x, y| {
        y.mi.total_cmp(&x.mi)
            .then_with(|| (&x.var_a, &x.var_b).cmp(&(&y.var_a, &y.var_b)))
    });

    let retained_information = tree_edges.iter().map(|e| e.mi).sum();
    let components = if vars.is_empty() {
        0
    } else {
        vars.len() - tree_edges.len()
    };

    ChowLiuTree {
        variables: vars,
        edges: tree_edges,
        retained_information,
        components,
    }
}

#[cfg(test)]
mod tests {
    use super::chow_liu_tree;
    use crate::math::mi_matrix::MiEdge;

    fn vars(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| (*n).to_owned()).collect()
    }

    fn edge(a: &str, b: &str, mi: f64) -> MiEdge {
        MiEdge {
            var_a: a.to_owned(),
            var_b: b.to_owned(),
            mi,
            sample_count: 100,
        }
    }

    #[test]
    fn picks_maximum_weight_spanning_tree() {
        let edges = vec![
            edge("a", "b", 0.9),
            edge("b", "c", 0.8),
            edge("a", "c", 0.1),
        ];
        let tree = chow_liu_tree(&vars(&["a", "b", "c"]), &edges);
        assert_eq!(tree.components, 1);
        assert_eq!(tree.edges.len(), 2);
        assert!(tree.contains_edge("a", "b").is_some());
        assert!(tree.contains_edge("b", "c").is_some());
        assert!(tree.contains_edge("a", "c").is_none());
        assert!((tree.retained_information - 1.7).abs() < 1e-12);
    }

    #[test]
    fn tie_breaking_is_deterministic_across_input_orders() {
        // A 4-cycle of identical weights: which two edges win is defined by
        // the lexicographic tie-break, not the input order.
        let mut edges = vec![
            edge("a", "b", 0.5),
            edge("b", "c", 0.5),
            edge("c", "d", 0.5),
            edge("a", "d", 0.5),
        ];
        let tree1 = chow_liu_tree(&vars(&["a", "b", "c", "d"]), &edges);
        edges.reverse();
        let tree2 = chow_liu_tree(&vars(&["d", "c", "b", "a"]), &edges);
        assert_eq!(tree1.edges, tree2.edges);
        // Lexicographically first edges win: (a,b), (a,d), (b,c).
        assert!(tree1.contains_edge("a", "b").is_some());
        assert!(tree1.contains_edge("a", "d").is_some());
        assert!(tree1.contains_edge("b", "c").is_some());
        assert!(tree1.contains_edge("c", "d").is_none());
    }

    #[test]
    fn disconnected_graph_yields_forest() {
        // {a,b} and {c,d} islands — no edge bridges them.
        let edges = vec![edge("a", "b", 0.6), edge("c", "d", 0.4)];
        let tree = chow_liu_tree(&vars(&["a", "b", "c", "d"]), &edges);
        assert_eq!(tree.components, 2);
        assert!(tree.is_forest());
        assert_eq!(tree.edges.len(), 2);
        assert!((tree.retained_information - 1.0).abs() < 1e-12);
    }

    #[test]
    fn isolated_variable_counts_as_component() {
        let edges = vec![edge("a", "b", 0.6)];
        let tree = chow_liu_tree(&vars(&["a", "b", "c"]), &edges);
        assert_eq!(tree.components, 2);
        assert!(tree.is_forest());
    }

    #[test]
    fn no_edges_yields_all_singletons() {
        let tree = chow_liu_tree(&vars(&["a", "b", "c"]), &[]);
        assert_eq!(tree.components, 3);
        assert!(tree.edges.is_empty());
        assert_eq!(tree.retained_information, 0.0);
    }

    #[test]
    fn zero_mi_edges_still_connect_the_tree() {
        let edges = vec![edge("a", "b", 0.0), edge("b", "c", 0.0)];
        let tree = chow_liu_tree(&vars(&["a", "b", "c"]), &edges);
        assert_eq!(tree.components, 1);
        assert_eq!(tree.retained_information, 0.0);
    }

    #[test]
    fn edges_referencing_unlisted_variables_are_ignored() {
        let edges = vec![edge("a", "b", 0.9), edge("a", "zz", 0.8)];
        let tree = chow_liu_tree(&vars(&["a", "b"]), &edges);
        assert_eq!(tree.edges.len(), 1);
        assert_eq!(tree.components, 1);
    }
}
