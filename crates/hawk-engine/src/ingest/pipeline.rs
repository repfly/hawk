use std::collections::HashMap;
use std::{path::Path, time::Instant};

use anyhow::{anyhow, Result};

use crate::core::{canonical_dimension_key, DimensionKey, DistributionRepr, Schema};
use crate::math::surprisal::surprisal;
use crate::query::result_types::SurpriseResult;
use crate::query::surprise::surprise_result_from_report;
use crate::storage::Database;

use crate::ingest::batch_updater::{apply_batch, BatchReport};
use crate::ingest::column_mapper::{map_row, validate_mapping, IngestMapping};
use crate::ingest::csv_reader::read_csv_rows;
use crate::ingest::schema_inference::{identity_mapping, infer_schema, InferConfig};

#[derive(Debug, Clone)]
pub struct IngestOptions {
    pub batch_size: usize,
    pub show_progress: bool,
    /// Score the ingested batch's data against the pre-batch stored model and
    /// attach a surprisal report to the ingest result. Default off.
    pub surprisal_report: bool,
}

impl Default for IngestOptions {
    fn default() -> Self {
        Self {
            batch_size: 10_000,
            show_progress: false,
            surprisal_report: false,
        }
    }
}

/// Surprisal of one updated distribution's batch data under the pre-batch model.
#[derive(Debug, Clone)]
pub struct BatchSurprisal {
    pub variable: String,
    pub dimension_key: String,
    pub result: SurpriseResult,
}

#[derive(Debug, Clone, Default)]
pub struct IngestReport {
    pub total_rows: usize,
    pub processed_rows: usize,
    pub skipped_rows: usize,
    pub distributions_updated: usize,
    pub elapsed_ms: u128,
    /// Populated when `IngestOptions::surprisal_report` is set; sorted by
    /// excess bits descending. Not persisted.
    pub surprisal: Vec<BatchSurprisal>,
}

pub struct IngestionPipeline;

impl IngestionPipeline {
    pub fn ingest_file(
        db: &mut Database,
        path: impl AsRef<Path>,
        mapping: &IngestMapping,
        options: IngestOptions,
    ) -> Result<IngestReport> {
        let schema = db.schema().clone();
        validate_mapping(&schema, mapping)?;

        let raw_rows = Self::read_rows(path.as_ref())?;

        Self::ingest_rows_internal(db, &raw_rows, mapping, options)
    }

    /// Infer the schema from the file, define all variables/dimensions in the
    /// database, then ingest all rows.
    pub fn ingest_file_auto(
        db: &mut Database,
        path: impl AsRef<Path>,
        config: InferConfig,
        options: IngestOptions,
    ) -> Result<IngestReport> {
        let raw_rows = Self::read_rows(path.as_ref())?;

        let sample_size = config.sample_size.min(raw_rows.len());
        let sample = &raw_rows[..sample_size];
        let inferred = infer_schema(sample, &config);

        // Define inferred variables and dimensions in the database.
        for var in &inferred.variables {
            // Skip if already defined.
            if db.schema().variables.iter().any(|v| v.name == var.name) {
                continue;
            }
            db.define_variable(var.clone())?;
        }
        for dim in &inferred.dimensions {
            if db.schema().dimensions.iter().any(|d| d.name == dim.name) {
                continue;
            }
            db.define_dimension(dim.clone())?;
        }

        let mapping = identity_mapping(db.schema());

        Self::ingest_rows_internal(db, &raw_rows, &mapping, options)
    }

    /// Delta ingestion: skip rows that have already been processed (based on
    /// the database high-water mark) and only ingest new rows.  After
    /// successful ingestion the high-water mark is advanced.
    pub fn ingest_file_delta(
        db: &mut Database,
        path: impl AsRef<Path>,
        mapping: &IngestMapping,
        options: IngestOptions,
    ) -> Result<IngestReport> {
        let schema = db.schema().clone();
        validate_mapping(&schema, mapping)?;

        let raw_rows = Self::read_rows(path.as_ref())?;

        let hwm = db.get_high_water_mark() as usize;
        if hwm >= raw_rows.len() {
            return Ok(IngestReport {
                total_rows: 0,
                ..IngestReport::default()
            });
        }

        let new_rows = &raw_rows[hwm..];
        let report = Self::ingest_rows_internal(db, new_rows, mapping, options)?;

        db.set_high_water_mark((hwm + report.processed_rows) as u64)?;

        Ok(report)
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    fn read_rows(path: &Path) -> Result<Vec<serde_json::Map<String, serde_json::Value>>> {
        let extension = path
            .extension()
            .and_then(|x| x.to_str())
            .map(|x| x.to_ascii_lowercase())
            .unwrap_or_default();

        match extension.as_str() {
            "csv" => read_csv_rows(path),
            "json" | "jsonl" => {
                #[cfg(feature = "json")]
                {
                    crate::ingest::json_reader::read_json_lines_rows(path)
                }
                #[cfg(not(feature = "json"))]
                {
                    Err(anyhow!(
                        "json ingestion requires enabling the 'json' feature"
                    ))
                }
            }
            "parquet" => {
                #[cfg(feature = "parquet")]
                {
                    crate::ingest::parquet_reader::read_parquet_rows(path)
                }
                #[cfg(not(feature = "parquet"))]
                {
                    Err(anyhow!(
                        "parquet ingestion requires enabling the 'parquet' feature"
                    ))
                }
            }
            other => Err(anyhow!(
                "unsupported ingestion format '{}'; expected csv/json/parquet",
                other
            )),
        }
    }

    fn ingest_rows_internal(
        db: &mut Database,
        raw_rows: &[serde_json::Map<String, serde_json::Value>],
        mapping: &IngestMapping,
        options: IngestOptions,
    ) -> Result<IngestReport> {
        let schema = db.schema().clone();

        let pre_model = options
            .surprisal_report
            .then(|| snapshot_distributions(db, &schema));

        let mut mapped_rows = Vec::with_capacity(raw_rows.len());
        for row in raw_rows {
            if let Some(mapped) = map_row(row, mapping) {
                mapped_rows.push(mapped);
            }
        }

        let start = Instant::now();
        let mut report = IngestReport {
            total_rows: mapped_rows.len(),
            ..IngestReport::default()
        };

        for chunk in mapped_rows.chunks(options.batch_size.max(1)) {
            let BatchReport {
                processed,
                skipped,
                distributions_updated,
            } = apply_batch(db, &schema, chunk)?;

            report.processed_rows += processed;
            report.skipped_rows += skipped;
            report.distributions_updated += distributions_updated;

            if options.show_progress && report.total_rows > 0 {
                let pct = (report.processed_rows as f64 / report.total_rows as f64) * 100.0;
                println!(
                    "Ingesting: {} / {} rows ({:.1}%) — {} distributions updated",
                    report.processed_rows, report.total_rows, pct, report.distributions_updated
                );
            }
        }

        db.flush()?;

        if let Some(pre_model) = pre_model {
            report.surprisal = score_batch_surprisal(db, &schema, &pre_model);
        }

        report.elapsed_ms = start.elapsed().as_millis();
        Ok(report)
    }
}

type ModelSnapshot = HashMap<(String, DimensionKey), DistributionRepr>;

fn snapshot_distributions(db: &Database, schema: &Schema) -> ModelSnapshot {
    let mut snapshot = HashMap::new();
    for var in &schema.variables {
        for dist in db.distributions_for_variable(&var.name) {
            snapshot.insert(
                (var.name.clone(), dist.dimension_key.clone()),
                dist.repr.clone(),
            );
        }
    }
    snapshot
}

/// Score each updated distribution's batch-only counts (post − pre) against
/// the pre-batch stored model. Slices with no pre-batch model are skipped.
fn score_batch_surprisal(
    db: &Database,
    schema: &Schema,
    pre_model: &ModelSnapshot,
) -> Vec<BatchSurprisal> {
    let mut scores = Vec::new();
    for var in &schema.variables {
        for dist in db.distributions_for_variable(&var.name) {
            let key = (var.name.clone(), dist.dimension_key.clone());
            let Some(pre) = pre_model.get(&key) else {
                continue;
            };
            if pre.total_count() == 0 {
                continue;
            }
            let Some(batch) = subtract_counts(&dist.repr, pre) else {
                continue;
            };
            if batch.total_count() == 0 {
                continue;
            }
            let Ok(report) = surprisal(&batch, pre) else {
                continue;
            };
            scores.push(BatchSurprisal {
                variable: var.name.clone(),
                dimension_key: canonical_dimension_key(&dist.dimension_key),
                result: surprise_result_from_report(&var.name, report),
            });
        }
    }
    scores.sort_by(|a, b| b.result.excess_bits.total_cmp(&a.result.excess_bits));
    scores
}

/// Element-wise `post − pre`, i.e. the counts contributed by the batch alone.
fn subtract_counts(post: &DistributionRepr, pre: &DistributionRepr) -> Option<DistributionRepr> {
    match (post, pre) {
        (
            DistributionRepr::Histogram {
                min,
                max,
                bin_counts: post_bins,
                total_count: post_total,
            },
            DistributionRepr::Histogram {
                bin_counts: pre_bins,
                total_count: pre_total,
                ..
            },
        ) if post_bins.len() == pre_bins.len() => Some(DistributionRepr::Histogram {
            min: *min,
            max: *max,
            bin_counts: post_bins
                .iter()
                .zip(pre_bins)
                .map(|(a, b)| a.saturating_sub(*b))
                .collect(),
            total_count: post_total.saturating_sub(*pre_total),
        }),
        (
            DistributionRepr::Categorical {
                categories,
                counts: post_counts,
                unknown_count: post_unknown,
                total_count: post_total,
            },
            DistributionRepr::Categorical {
                counts: pre_counts,
                unknown_count: pre_unknown,
                total_count: pre_total,
                ..
            },
        ) if post_counts.len() == pre_counts.len() => Some(DistributionRepr::Categorical {
            categories: categories.clone(),
            counts: post_counts
                .iter()
                .zip(pre_counts)
                .map(|(a, b)| a.saturating_sub(*b))
                .collect(),
            unknown_count: post_unknown.saturating_sub(*pre_unknown),
            total_count: post_total.saturating_sub(*pre_total),
        }),
        _ => None,
    }
}
