use serde::{Deserialize, Serialize};

use hawk_math::ConfidenceInfo;

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
