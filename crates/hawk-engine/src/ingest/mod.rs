pub mod batch_updater;
pub mod column_mapper;
pub mod csv_reader;
#[cfg(feature = "json")]
pub mod json_reader;
#[cfg(feature = "parquet")]
pub mod parquet_reader;
pub mod pipeline;
pub mod schema_inference;

pub use column_mapper::IngestMapping;
pub use pipeline::{IngestOptions, IngestReport, IngestionPipeline};
pub use schema_inference::{InferConfig, infer_schema};
