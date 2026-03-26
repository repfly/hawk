use std::{path::Path, time::Instant};

use anyhow::{anyhow, Result};

use crate::storage::Database;

use crate::ingest::batch_updater::{apply_batch, BatchReport};
use crate::ingest::column_mapper::{map_row, validate_mapping, IngestMapping};
use crate::ingest::csv_reader::read_csv_rows;
use crate::ingest::schema_inference::{identity_mapping, infer_schema, InferConfig};

#[derive(Debug, Clone)]
pub struct IngestOptions {
    pub batch_size: usize,
    pub show_progress: bool,
}

impl Default for IngestOptions {
    fn default() -> Self {
        Self {
            batch_size: 10_000,
            show_progress: false,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct IngestReport {
    pub total_rows: usize,
    pub processed_rows: usize,
    pub skipped_rows: usize,
    pub distributions_updated: usize,
    pub elapsed_ms: u128,
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
                    Err(anyhow!("json ingestion requires enabling the 'json' feature"))
                }
            }
            "parquet" => {
                #[cfg(feature = "parquet")]
                {
                    crate::ingest::parquet_reader::read_parquet_rows(path)
                }
                #[cfg(not(feature = "parquet"))]
                {
                    Err(anyhow!("parquet ingestion requires enabling the 'parquet' feature"))
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

        report.elapsed_ms = start.elapsed().as_millis();
        Ok(report)
    }
}
