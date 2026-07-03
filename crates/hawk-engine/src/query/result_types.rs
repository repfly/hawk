use serde::{Deserialize, Serialize};

use crate::math::ConfidenceInfo;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryShift {
    pub category: String,
    pub prob_a: f64,
    pub prob_b: f64,
    pub delta: f64,
    pub contribution: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompareResult {
    pub jsd: f64,
    pub kl_a_to_b: f64,
    pub kl_b_to_a: f64,
    pub entropy_a: f64,
    pub entropy_b: f64,
    pub wasserstein: Option<f64>,
    pub hellinger: f64,
    pub psi: f64,
    pub sample_count_a: u64,
    pub sample_count_b: u64,
    pub confidence: ConfidenceInfo,
    /// Per-category probability shifts, sorted by absolute delta descending.
    /// Empty for histogram distributions.
    pub top_movers: Vec<CategoryShift>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariableContribution {
    pub variable: String,
    pub jsd: f64,
    pub fraction: f64,
    pub entropy_a: f64,
    pub entropy_b: f64,
    /// Per-category shifts for this variable (empty for histograms).
    pub top_movers: Vec<CategoryShift>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainResult {
    pub total_divergence: f64,
    pub contributions: Vec<VariableContribution>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributionSummary {
    pub reference: String,
    pub sample_count: u64,
    pub entropy: f64,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftEvent {
    pub time_from: String,
    pub time_to: String,
    pub jsd: f64,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackResult {
    pub time_points: Vec<String>,
    pub entropy_series: Vec<f64>,
    pub drift_series: Vec<f64>,
    pub drift_events: Vec<DriftEvent>,
    pub snapshots: Vec<DistributionSummary>,
}

// --- Surprisal ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SurpriseContributor {
    pub label: String,
    pub prob_a: f64,
    pub prob_b: f64,
    /// This bucket's share of the cross-entropy, in bits.
    pub bits: f64,
    /// This bucket's share of the excess bits (KL contribution).
    pub excess_bits: f64,
    pub unseen_in_b: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SurpriseResult {
    pub variable: String,
    /// Cross-entropy × A's sample count.
    pub total_bits: f64,
    /// Cross-entropy H(A, B) in bits per sample.
    pub bits_per_sample: f64,
    pub entropy_a: f64,
    /// Baseline model entropy H(B).
    pub baseline_entropy: f64,
    /// KL(A‖B) = H(A, B) − H(A).
    pub excess_bits: f64,
    pub sample_count_a: u64,
    pub sample_count_b: u64,
    /// A's probability mass on buckets unseen in B.
    pub unseen_mass: f64,
    pub unseen_mass_warning: Option<String>,
    /// Sorted by excess bits descending.
    pub top_contributors: Vec<SurpriseContributor>,
}

// --- Alerting ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertHit {
    pub time_from: String,
    pub time_to: String,
    pub metric_value: f64,
    pub threshold: f64,
    pub metric: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertResult {
    pub variable: String,
    pub metric: String,
    pub threshold: f64,
    pub hits: Vec<AlertHit>,
}

// --- Conditional MI ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimensionMI {
    pub value: String,
    pub mi: f64,
    pub nmi: f64,
    pub cramers_v: f64,
    pub sample_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CondMutualInfoResult {
    pub cmi: f64,
    pub total_samples: u64,
    pub conditioning_dimension: String,
    pub per_value: Vec<DimensionMI>,
}

// --- Structure (Chow-Liu dependency trees) ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructureEdge {
    pub var_a: String,
    pub var_b: String,
    pub mi: f64,
    pub sample_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructureResult {
    pub reference: String,
    /// Variables considered, sorted lexicographically.
    pub variables: Vec<String>,
    /// Tree edges ranked by MI descending.
    pub edges: Vec<StructureEdge>,
    /// Σ edge MI, in bits.
    pub retained_information: f64,
    /// Connected components; > 1 means missing joints forced a forest.
    pub components: usize,
    /// Pairs with no stored joint at the slice — reported, never zeroed.
    pub unknown_pairs: Vec<(String, String)>,
}

impl StructureResult {
    pub fn is_forest(&self) -> bool {
        self.components > 1
    }
}

/// Same edge in both trees but with a different MI weight.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReweightedEdge {
    pub var_a: String,
    pub var_b: String,
    pub mi_a: f64,
    pub mi_b: f64,
    /// mi_b − mi_a, in bits.
    pub delta: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructureDiffResult {
    pub structure_a: StructureResult,
    pub structure_b: StructureResult,
    /// Edges in B's tree but not A's.
    pub added_edges: Vec<StructureEdge>,
    /// Edges in A's tree but not B's.
    pub dropped_edges: Vec<StructureEdge>,
    /// Edges in both trees, sorted by |delta| descending.
    pub reweighted_edges: Vec<ReweightedEdge>,
    /// MI-weighted symmetric difference: (Σ MI dropped + Σ MI added) /
    /// (Σ MI of A's edges + Σ MI of B's edges), in [0, 1].
    pub rewiring_score: f64,
    /// retained(B) − retained(A), in bits.
    pub retained_information_delta: f64,
}

// --- Estimate (max-entropy reconstruction) ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EstimateCell {
    pub label_a: String,
    pub label_b: String,
    pub probability: f64,
    /// Fréchet lower bound: max(0, p_a + p_b − 1).
    pub lower_bound: f64,
    /// Fréchet upper bound: min(p_a, p_b).
    pub upper_bound: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EstimateResult {
    /// Canonically ordered (var_a < var_b); rows of the grid follow var_a.
    pub var_a: String,
    pub var_b: String,
    pub reference: String,
    /// true = a stored joint exists at this slice and is reported instead of
    /// the reconstruction; the estimate machinery was bypassed.
    pub observed: bool,
    pub entropy_a: f64,
    pub entropy_b: f64,
    /// Entropy of the reported joint table, in bits.
    pub joint_entropy: f64,
    /// MI of the reported table — 0 for the pure independence estimate.
    pub mi: f64,
    /// Upper bound on the true MI given only the marginals: min(H(A), H(B)).
    pub mi_upper_bound: f64,
    /// Bits about the dependency that the database does NOT know.
    ///
    /// Derivation: the max-ent joint is the independence product, so
    /// H(maxent) = H(A) + H(B). Any joint with these marginals has entropy in
    /// [max(H(A), H(B)), H(A) + H(B)], so the gap between the max-ent entropy
    /// and the lowest achievable joint entropy is
    /// H(A) + H(B) − max(H(A), H(B)) = min(H(A), H(B)) — exactly the maximum
    /// MI the marginals permit. 0 when the joint is observed (stored).
    pub missing_information_bits: f64,
    pub sample_count_a: u64,
    pub sample_count_b: u64,
    /// Full grid, sorted by probability descending (ties: label order).
    pub cells: Vec<EstimateCell>,
    /// IPF diagnostics; 0 iterations when the stored joint was used.
    pub ipf_iterations: usize,
    pub ipf_converged: bool,
}

impl EstimateResult {
    pub fn banner(&self) -> &'static str {
        if self.observed {
            "OBSERVED — stored joint"
        } else {
            "ESTIMATED — not observed"
        }
    }
}

// --- Suggest (information-gain-guided exploration) ---

/// One ranked next-query candidate. `expected_bits` is the generator's
/// expected-information score, not a ledger charge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Suggestion {
    /// Ready-to-run Hawk SQL.
    pub query: String,
    pub rationale: String,
    pub expected_bits: f64,
}

// --- Profile (one-call dataset card) ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileVariable {
    pub name: String,
    /// "categorical" or "continuous".
    pub var_type: String,
    /// Entropy of the variable pooled over all stored slices, in bits.
    pub entropy: f64,
    pub sample_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileDimension {
    pub name: String,
    pub value_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileAssociation {
    pub var_a: String,
    pub var_b: String,
    pub mi: f64,
    pub sample_count: u64,
}

/// Largest latest-vs-previous time-slice shift across all variables.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileDrift {
    pub variable: String,
    pub time_from: String,
    pub time_to: String,
    pub jsd: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileResult {
    pub variables: Vec<ProfileVariable>,
    pub dimensions: Vec<ProfileDimension>,
    /// Strongest stored associations, ranked by MI descending.
    pub top_associations: Vec<ProfileAssociation>,
    /// Variable pairs with no stored joint anywhere — unknown, never zero.
    pub unknown_pairs: Vec<(String, String)>,
    /// None when no time-like dimension (or fewer than 2 time slices) exists.
    pub biggest_drift: Option<ProfileDrift>,
    pub stored_distributions: usize,
    pub stored_joints: usize,
    pub total_samples: u64,
    /// Approximate serialized size of stored marginals + joints, in bytes.
    pub approx_bytes: u64,
}

// --- Correlation Discovery ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariablePairCorrelation {
    pub var_a: String,
    pub var_b: String,
    pub mi: f64,
    pub nmi: f64,
    pub cramers_v: f64,
    pub sample_count: u64,
    pub dimension_value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrelationReport {
    pub pairs: Vec<VariablePairCorrelation>,
    pub dimension: Option<String>,
    pub total_pairs_scanned: usize,
}
