use std::sync::Arc;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Content};
use rmcp::schemars;
use rmcp::{tool, tool_router};
use serde::Deserialize;

use hawk_engine::ingest::pipeline::{IngestOptions, IngestionPipeline};
use hawk_engine::ingest::schema_inference::InferConfig;
use hawk_engine::storage::{Database, OpenMode};

use crate::help_text::HAWK_SQL_HELP;
use crate::state::AppState;

#[derive(Clone)]
pub struct HawkMcp {
    pub state: Arc<AppState>,
}

// --- Parameter structs ---

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct QueryParams {
    #[schemars(description = "Hawk SQL query string. Use the 'help' tool to see available syntax.")]
    pub sql: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct OpenDatabaseParams {
    #[schemars(description = "Path to the database directory")]
    pub path: String,
    #[schemars(description = "Open in read-only mode (default: false)")]
    pub readonly: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CreateDatabaseParams {
    #[schemars(description = "Path for the new database directory")]
    pub path: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct IngestFileParams {
    #[schemars(description = "Path to the file to ingest (CSV, JSON, or Parquet)")]
    pub file_path: String,
    #[schemars(
        description = "Max unique string values before treating a column as a dimension instead of categorical variable (default: 50)"
    )]
    pub max_categories: Option<usize>,
    #[schemars(description = "Column names to treat as date dimensions")]
    pub date_columns: Option<Vec<String>>,
    #[schemars(
        description = "Date granularity: 'daily', 'monthly', 'yearly' (default: 'daily')"
    )]
    pub date_granularity: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListDimensionsParams {
    #[schemars(description = "Name of the dimension to list values for")]
    pub dimension: String,
}

// --- Tool implementations ---

#[tool_router(server_handler)]
impl HawkMcp {
    #[tool(description = "Return the Hawk SQL syntax reference with all available query types and examples.")]
    fn help(&self) -> String {
        HAWK_SQL_HELP.to_string()
    }

    #[tool(
        description = "Execute a Hawk SQL query against the open database. Returns results as JSON. Use the 'help' tool first to see available query syntax."
    )]
    fn query(
        &self,
        Parameters(QueryParams { sql }): Parameters<QueryParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        self.state.with_db(|db, engine| {
            match hawk_engine::sql::query(db, engine, &sql) {
                Ok(result) => Ok(CallToolResult::success(vec![Content::text(result.to_json())])),
                Err(e) => Ok(CallToolResult::error(vec![Content::text(e.to_string())])),
            }
        })
    }

    #[tool(description = "Get the database schema: variables (with types), dimensions, and joint definitions.")]
    fn schema(&self) -> Result<CallToolResult, rmcp::ErrorData> {
        self.state.with_db(|db, _engine| {
            let schema = db.schema();
            let json = serde_json::to_string_pretty(schema).map_err(|e| {
                rmcp::ErrorData::internal_error(format!("serialization error: {}", e), None)
            })?;
            Ok(CallToolResult::success(vec![Content::text(json)]))
        })
    }

    #[tool(description = "Get database statistics: number of distributions, total samples, variable count, dimension count.")]
    fn stats(&self) -> Result<CallToolResult, rmcp::ErrorData> {
        self.state.with_db(|db, _engine| {
            let stats = db.stats();
            let json = format!(
                r#"{{"distributions": {}, "total_samples": {}, "variables": {}, "dimensions": {}}}"#,
                stats.distributions, stats.total_samples, stats.variables, stats.dimensions
            );
            Ok(CallToolResult::success(vec![Content::text(json)]))
        })
    }

    #[tool(description = "Open an existing Hawk database at the given path. Closes any currently open database.")]
    fn open_database(
        &self,
        Parameters(OpenDatabaseParams { path, readonly }): Parameters<OpenDatabaseParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let mode = if readonly.unwrap_or(false) {
            OpenMode::ReadOnly
        } else {
            OpenMode::ReadWrite
        };
        let db = Database::open(&path, mode).map_err(|e| {
            rmcp::ErrorData::internal_error(format!("failed to open database: {}", e), None)
        })?;
        let stats = db.stats();
        let schema = db.schema();
        let summary = format!(
            "Opened database at '{}'. {} variables, {} dimensions, {} distributions, {} total samples.",
            path, schema.variables.len(), schema.dimensions.len(), stats.distributions, stats.total_samples
        );
        self.state.swap_db(db, path)?;
        Ok(CallToolResult::success(vec![Content::text(summary)]))
    }

    #[tool(description = "Create a new empty Hawk database at the given path. Closes any currently open database.")]
    fn create_database(
        &self,
        Parameters(CreateDatabaseParams { path }): Parameters<CreateDatabaseParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let db = Database::create(&path).map_err(|e| {
            rmcp::ErrorData::internal_error(format!("failed to create database: {}", e), None)
        })?;
        self.state.swap_db(db, path.clone())?;
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Created new database at '{}'.",
            path
        ))]))
    }

    #[tool(
        description = "Ingest a CSV, JSON, or Parquet file into the open database. Automatically infers schema (variables, dimensions) from the data."
    )]
    fn ingest_file(
        &self,
        Parameters(params): Parameters<IngestFileParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let mut config = InferConfig::default();
        if let Some(max) = params.max_categories {
            config.max_categories = max;
        }
        if let Some(cols) = params.date_columns {
            config.date_columns = cols;
        }
        if let Some(gran) = params.date_granularity {
            config.date_granularity = gran;
        }

        self.state.with_db_mut(|db| {
            let report = IngestionPipeline::ingest_file_auto(
                db,
                &params.file_path,
                config,
                IngestOptions::default(),
            )
            .map_err(|e| {
                rmcp::ErrorData::internal_error(format!("ingestion failed: {}", e), None)
            })?;

            db.flush().map_err(|e| {
                rmcp::ErrorData::internal_error(format!("flush failed: {}", e), None)
            })?;

            let json = format!(
                r#"{{"total_rows": {}, "processed_rows": {}, "skipped_rows": {}, "distributions_updated": {}, "elapsed_ms": {}}}"#,
                report.total_rows,
                report.processed_rows,
                report.skipped_rows,
                report.distributions_updated,
                report.elapsed_ms
            );
            Ok(CallToolResult::success(vec![Content::text(json)]))
        })
    }

    #[tool(description = "List all unique values for a given dimension in the database.")]
    fn list_dimensions(
        &self,
        Parameters(ListDimensionsParams { dimension }): Parameters<ListDimensionsParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        self.state.with_db(|db, _engine| {
            let values = db.dimension_values(&dimension);
            let json = serde_json::to_string_pretty(&values).map_err(|e| {
                rmcp::ErrorData::internal_error(format!("serialization error: {}", e), None)
            })?;
            Ok(CallToolResult::success(vec![Content::text(json)]))
        })
    }
}
