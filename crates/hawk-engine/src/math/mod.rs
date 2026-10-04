pub mod chow_liu;
pub mod confidence;
pub mod cramers_v;
pub mod entropy;
pub mod frechet;
pub mod hellinger;
pub mod ipf;
pub mod jsd;
pub mod kl_divergence;
pub mod mdl;
pub mod mi_matrix;
pub mod mutual_info;
pub mod psi;
pub mod rebin;
pub mod surprisal;
pub mod wasserstein;

pub use chow_liu::{chow_liu_tree, ChowLiuTree, TreeEdge};
pub use confidence::{asymptotic_jsd_confidence, ConfidenceInfo};
pub use cramers_v::cramers_v;
pub use entropy::{entropy, entropy_from_probs};
pub use frechet::{frechet_bounds, mi_upper_bound, FrechetBounds};
pub use hellinger::hellinger;
pub use ipf::{ipf, maxent_joint, IpfResult, IPF_MAX_ITERATIONS, IPF_TOLERANCE};
pub use jsd::jsd;
pub use kl_divergence::kl_divergence;
pub use mdl::{
    category_fold_candidates, choose_bin_count, mdl_bin_count, score_histogram_bins, score_joint,
    BinCountScore, FoldCandidate, JointMdlScore, FOLD_MASS_THRESHOLD, MDL_BIN_CANDIDATES,
};
pub use mi_matrix::{mi_matrix, MiEdge, MiMatrix};
pub use mutual_info::{
    conditional_mutual_information, mutual_information, mutual_information_from_probs,
    normalized_mutual_information,
};
pub use psi::psi;
pub use rebin::{align_categorical, rebin_histogram};
pub use surprisal::{surprisal, SurprisalContribution, SurprisalReport, SMOOTHING_EPSILON};
pub use wasserstein::wasserstein_1;
