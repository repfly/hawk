use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::{dimension_key::DimensionKey, schema::VariableType};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DistributionRepr {
    Histogram {
        min: f64,
        max: f64,
        bin_counts: Vec<u64>,
        total_count: u64,
    },
    Categorical {
        categories: Vec<String>,
        counts: Vec<u64>,
        unknown_count: u64,
        total_count: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum JointRepr {
    HistogramGrid {
        x_min: f64,
        x_max: f64,
        x_bins: u32,
        y_min: f64,
        y_max: f64,
        y_bins: u32,
        counts: Vec<Vec<u64>>,
        total_count: u64,
    },
    ContingencyTable {
        x_categories: Vec<String>,
        y_categories: Vec<String>,
        counts: Vec<Vec<u64>>,
        total_count: u64,
    },
    ConditionalHistograms {
        condition_categories: Vec<String>,
        histograms: Vec<DistributionRepr>,
        total_count: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DistributionObject {
    pub id: u64,
    pub variable: String,
    pub dimension_key: DimensionKey,
    pub repr: DistributionRepr,
    pub sample_count: u64,
    pub entropy: f64,
    pub last_updated: u64,
    pub version: u64,
    pub raw_record_range: (u64, u64),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JointDistributionObject {
    pub id: u64,
    pub variables: (String, String),
    pub dimension_key: DimensionKey,
    pub repr: JointRepr,
    pub sample_count: u64,
    pub last_updated: u64,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TrackPoint {
    pub time_label: String,
    pub entropy: f64,
    pub sample_count: u64,
    pub version: u64,
}

impl DistributionObject {
    pub fn new(id: u64, variable: &str, dimension_key: DimensionKey, repr: DistributionRepr) -> Self {
        let now = Utc::now().timestamp() as u64;
        let sample_count = repr.total_count();
        Self {
            id,
            variable: variable.to_owned(),
            dimension_key,
            repr,
            sample_count,
            entropy: 0.0,
            last_updated: now,
            version: 1,
            raw_record_range: (0, 0),
        }
    }

    pub fn bump_version(&mut self) {
        self.version += 1;
        self.last_updated = Utc::now().timestamp() as u64;
        self.sample_count = self.repr.total_count();
    }
}

impl DistributionRepr {
    pub fn from_variable(var_type: &VariableType) -> Self {
        match var_type {
            VariableType::Continuous { bins, range } => {
                let (min, max) = range.unwrap_or((0.0, 1.0));
                Self::Histogram {
                    min,
                    max,
                    bin_counts: vec![0; *bins as usize],
                    total_count: 0,
                }
            }
            VariableType::Categorical {
                categories,
                allow_unknown: _,
            } => Self::Categorical {
                categories: categories.clone(),
                counts: vec![0; categories.len()],
                unknown_count: 0,
                total_count: 0,
            },
        }
    }

    pub fn total_count(&self) -> u64 {
        match self {
            Self::Histogram { total_count, .. } | Self::Categorical { total_count, .. } => *total_count,
        }
    }

    pub fn as_probability_vector(&self) -> Vec<f64> {
        match self {
            Self::Histogram {
                bin_counts,
                total_count,
                ..
            } => counts_to_probs(bin_counts, *total_count),
            Self::Categorical {
                counts,
                total_count,
                ..
            } => counts_to_probs(counts, *total_count),
        }
    }

    pub fn value_count_vector(&self) -> Vec<u64> {
        match self {
            Self::Histogram { bin_counts, .. } => bin_counts.clone(),
            Self::Categorical { counts, .. } => counts.clone(),
        }
    }

    pub fn increment_histogram(&mut self, index: usize, by: u64) {
        if let Self::Histogram {
            bin_counts,
            total_count,
            ..
        } = self
        {
            if let Some(slot) = bin_counts.get_mut(index) {
                *slot += by;
                *total_count += by;
            }
        }
    }

    pub fn merge_from(&mut self, other: &DistributionRepr) {
        match (self, other) {
            (
                Self::Histogram {
                    bin_counts: a_bins,
                    total_count: a_total,
                    ..
                },
                Self::Histogram {
                    bin_counts: b_bins,
                    total_count: b_total,
                    ..
                },
            ) => {
                for (a, b) in a_bins.iter_mut().zip(b_bins.iter()) {
                    *a += b;
                }
                *a_total += b_total;
            }
            (
                Self::Categorical {
                    counts: a_counts,
                    unknown_count: a_unknown,
                    total_count: a_total,
                    ..
                },
                Self::Categorical {
                    counts: b_counts,
                    unknown_count: b_unknown,
                    total_count: b_total,
                    ..
                },
            ) => {
                for (a, b) in a_counts.iter_mut().zip(b_counts.iter()) {
                    *a += b;
                }
                *a_unknown += b_unknown;
                *a_total += b_total;
            }
            _ => {}
        }
    }

    pub fn increment_categorical(&mut self, index: Option<usize>, by: u64) {
        if let Self::Categorical {
            counts,
            unknown_count,
            total_count,
            ..
        } = self
        {
            match index {
                Some(i) => {
                    if let Some(slot) = counts.get_mut(i) {
                        *slot += by;
                    }
                }
                None => *unknown_count += by,
            }
            *total_count += by;
        }
    }
}

fn counts_to_probs(counts: &[u64], total: u64) -> Vec<f64> {
    if total == 0 {
        return vec![0.0; counts.len()];
    }
    counts
        .iter()
        .map(|c| *c as f64 / total as f64)
        .collect::<Vec<_>>()
}
