use std::sync::Arc;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Content};
use rmcp::schemars;
use rmcp::{tool, tool_router};
use serde::Deserialize;

use hawk_engine::ingest::pipeline::{IngestOptions, IngestionPipeline};
use hawk_engine::ingest::schema_inference::InferConfig;
use hawk_engine::storage::{Database, OpenMode};

use hawk_engine::query::suggest::SuggestContext;

use crate::help_text::HAWK_SQL_HELP;
use crate::state::{
    charges_for_profile, charges_for_statement, query_fingerprint, AppState, Ledger,
};

#[derive(Clone)]
pub struct HawkMcp {
    pub state: Arc<AppState>,
}

// --- Parameter structs ---

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct QueryParams {
    #[schemars(
        description = "Hawk SQL query string. Use the 'help' tool to see available syntax."
    )]
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
    #[schemars(description = "Date granularity: 'daily', 'monthly', 'yearly' (default: 'daily')")]
    pub date_granularity: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListDimensionsParams {
    #[schemars(description = "Name of the dimension to list values for")]
    pub dimension: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SuggestParams {
    #[schemars(description = "Maximum number of suggestions to return (default: 10)")]
    pub limit: Option<usize>,
}

// --- Tool implementations ---

#[tool_router(server_handler)]
impl HawkMcp {
    #[tool(
        description = "Return the Hawk SQL syntax reference with all available query types and examples."
    )]
    fn help(&self) -> String {
        HAWK_SQL_HELP.to_string()
    }

    #[tool(
        description = "Execute a Hawk SQL query against the open database. Returns results as JSON. Use the 'help' tool first to see available query syntax. Successful queries are charged to the session's information ledger; when a bit budget is set, an over-budget query returns a JSON refusal with refused:true instead of results (check the 'ledger' tool)."
    )]
    fn query(
        &self,
        Parameters(QueryParams { sql }): Parameters<QueryParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        self.state.with_db(|db, engine| {
            let stmt = match hawk_engine::sql::parser::parse(&sql) {
                Ok(stmt) => stmt,
                Err(e) => {
                    return Ok(CallToolResult::error(vec![Content::text(format!(
                        "parse error: {}",
                        e
                    ))]))
                }
            };
            let result = match hawk_engine::sql::executor::execute(db, engine, &stmt) {
                Ok(result) => result,
                Err(e) => return Ok(CallToolResult::error(vec![Content::text(e.to_string())])),
            };

            // Disclosure accounting (docs/information-ledger.md): charge on
            // success only; identical queries are never re-charged; a query
            // that would exceed the budget is refused before release.
            let fingerprint = query_fingerprint(&sql);
            let mut ledger = self.state.ledger.lock().map_err(|e| {
                rmcp::ErrorData::internal_error(format!("ledger lock poisoned: {}", e), None)
            })?;
            if !ledger.is_charged(&fingerprint) {
                let charges = charges_for_statement(db, engine, &stmt);
                let cost_bits: f64 = charges.iter().map(|(_, bits)| bits).sum();
                if ledger.would_exceed(cost_bits) {
                    return Ok(CallToolResult::success(vec![Content::text(
                        budget_refusal_json(&ledger, cost_bits),
                    )]));
                }
                ledger.charge(fingerprint, charges);
            }

            Ok(CallToolResult::success(vec![Content::text(
                result.to_json(),
            )]))
        })
    }

    #[tool(
        description = "Session information ledger: cumulative bits revealed per variable, total spend, budget, and remaining bits. Free to call. See docs/information-ledger.md for what each query type is charged."
    )]
    fn ledger(&self) -> Result<CallToolResult, rmcp::ErrorData> {
        let ledger = self.state.ledger.lock().map_err(|e| {
            rmcp::ErrorData::internal_error(format!("ledger lock poisoned: {}", e), None)
        })?;
        let json = serde_json::json!({
            "spent_bits_per_variable": ledger.spent_per_variable(),
            "total_spent_bits": ledger.total_spent_bits(),
            "budget_bits": ledger.budget_bits(),
            "remaining_bits": ledger.remaining_bits(),
            "charged_queries": ledger.charged_query_count(),
        });
        let text = serde_json::to_string_pretty(&json).map_err(|e| {
            rmcp::ErrorData::internal_error(format!("serialization error: {}", e), None)
        })?;
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(
        description = "Rank candidate next queries by expected information gain in bits, deduplicated against this session's history (queries already run, variables/pairs already released). Returns [{query, rationale, expected_bits, cost_bits, fits_budget}] where cost_bits is what the query would charge to the information ledger and fits_budget says whether it fits the remaining bit budget. Free to call — suggestions release ranked query text and scores, not data."
    )]
    fn suggest(
        &self,
        Parameters(SuggestParams { limit }): Parameters<SuggestParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        self.state.with_db(|db, engine| {
            let ledger = self.state.ledger.lock().map_err(|e| {
                rmcp::ErrorData::internal_error(format!("ledger lock poisoned: {}", e), None)
            })?;
            // Session history: executed fingerprints + released ledger keys.
            let ctx = SuggestContext::new(
                ledger.charged_fingerprints().iter().cloned(),
                ledger.spent_per_variable().keys().cloned(),
            );
            let suggestions = engine
                .suggest(db, &ctx, limit.unwrap_or(10))
                .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

            let items: Vec<serde_json::Value> = suggestions
                .iter()
                .map(|s| {
                    // What the suggested query would charge under the ledger's
                    // model — distinct from its expected information gain.
                    let cost_bits = hawk_engine::sql::parser::parse(&s.query)
                        .ok()
                        .map(|stmt| {
                            charges_for_statement(db, engine, &stmt)
                                .iter()
                                .map(|(_, bits)| bits)
                                .sum::<f64>()
                        });
                    serde_json::json!({
                        "query": s.query,
                        "rationale": s.rationale,
                        "expected_bits": s.expected_bits,
                        "cost_bits": cost_bits,
                        "fits_budget": cost_bits.is_none_or(|c| !ledger.would_exceed(c)),
                    })
                })
                .collect();
            let text = serde_json::to_string_pretty(&items).map_err(|e| {
                rmcp::ErrorData::internal_error(format!("serialization error: {}", e), None)
            })?;
            Ok(CallToolResult::success(vec![Content::text(text)]))
        })
    }

    #[tool(
        description = "One-call dataset card for agent orientation: variables (name, type, entropy in bits, sample count), dimensions with value counts, top associations by mutual information (pairs without a stored joint listed as unknown), the biggest recent drift (latest vs previous time slice by JSD, when a time dimension exists), and a stored-objects/size summary. Charged one flat scalar per variable to the information ledger, once per session; over budget it returns a JSON refusal."
    )]
    fn profile(&self) -> Result<CallToolResult, rmcp::ErrorData> {
        self.state.with_db(|db, engine| {
            let result = engine
                .profile(db)
                .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

            // Same accounting discipline as `query`: charge on success only,
            // once per session, refusal before release.
            let fingerprint = "__tool:profile".to_owned();
            let mut ledger = self.state.ledger.lock().map_err(|e| {
                rmcp::ErrorData::internal_error(format!("ledger lock poisoned: {}", e), None)
            })?;
            if !ledger.is_charged(&fingerprint) {
                let charges = charges_for_profile(db);
                let cost_bits: f64 = charges.iter().map(|(_, bits)| bits).sum();
                if ledger.would_exceed(cost_bits) {
                    return Ok(CallToolResult::success(vec![Content::text(
                        budget_refusal_json(&ledger, cost_bits),
                    )]));
                }
                ledger.charge(fingerprint, charges);
            }

            let text = serde_json::to_string_pretty(&result).map_err(|e| {
                rmcp::ErrorData::internal_error(format!("serialization error: {}", e), None)
            })?;
            Ok(CallToolResult::success(vec![Content::text(text)]))
        })
    }

    #[tool(
        description = "Get the database schema: variables (with types), dimensions, and joint definitions."
    )]
    fn schema(&self) -> Result<CallToolResult, rmcp::ErrorData> {
        self.state.with_db(|db, _engine| {
            let schema = db.schema();
            let json = serde_json::to_string_pretty(schema).map_err(|e| {
                rmcp::ErrorData::internal_error(format!("serialization error: {}", e), None)
            })?;
            Ok(CallToolResult::success(vec![Content::text(json)]))
        })
    }

    #[tool(
        description = "Get database statistics: number of distributions, total samples, variable count, dimension count."
    )]
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

    #[tool(
        description = "Open an existing Hawk database at the given path. Closes any currently open database."
    )]
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

    #[tool(
        description = "Create a new empty Hawk database at the given path. Closes any currently open database."
    )]
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

/// Structured over-budget refusal: a normal tool result the agent can parse
/// and re-plan around, not a protocol error. Nothing is charged.
fn budget_refusal_json(ledger: &Ledger, cost_bits: f64) -> String {
    serde_json::json!({
        "refused": true,
        "reason": "query would exceed the session bit budget; nothing was charged — ask a cheaper question (scalar metrics cost less than full distributions) or inspect spend with the 'ledger' tool",
        "query_cost_bits": cost_bits,
        "spent_bits": ledger.total_spent_bits(),
        "budget_bits": ledger.budget_bits(),
        "remaining_bits": ledger.remaining_bits(),
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    use hawk_engine::core::{DimensionDefinition, VariableDefinition, VariableType};
    use hawk_engine::ingest::batch_updater::apply_batch;
    use hawk_engine::ingest::column_mapper::MappedRow;
    use hawk_engine::storage::Database;
    use serde_json::Value;

    use super::*;

    fn temp_db(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("hawk-mcp-test-{}-{}", name, std::process::id()))
    }

    /// category has a rare label ("niche", 2 samples per month) and two months
    /// of data for COMPARE-style queries.
    fn build_db(root: &std::path::Path) -> Database {
        let _ = std::fs::remove_dir_all(root);
        let mut db = Database::create_with_options(root, false).expect("create db");

        db.define_variable(VariableDefinition {
            name: "category".to_owned(),
            var_type: VariableType::Categorical {
                categories: vec!["news".to_owned(), "sports".to_owned(), "niche".to_owned()],
                allow_unknown: false,
            },
        })
        .unwrap();
        db.define_dimension(DimensionDefinition {
            name: "time".to_owned(),
            source_column: "time".to_owned(),
            granularity: None,
        })
        .unwrap();

        let mut rows = Vec::new();
        for (month, category, n) in [
            ("2025-01", "news", 30),
            ("2025-01", "sports", 20),
            ("2025-01", "niche", 2),
            ("2025-02", "news", 25),
            ("2025-02", "sports", 26),
            ("2025-02", "niche", 2),
        ] {
            for _ in 0..n {
                let mut variable_values = HashMap::new();
                variable_values.insert("category".to_owned(), Value::from(category));
                let mut dimension_values = HashMap::new();
                dimension_values.insert("time".to_owned(), month.to_owned());
                rows.push(MappedRow {
                    variable_values,
                    dimension_values,
                });
            }
        }
        let schema = db.schema().clone();
        apply_batch(&mut db, &schema, &rows).expect("apply batch");
        db
    }

    fn server(
        root: &std::path::Path,
        min_cell_count: Option<u64>,
        bit_budget: Option<f64>,
    ) -> HawkMcp {
        let db = build_db(root);
        HawkMcp {
            state: Arc::new(AppState::new(Some(db), None, min_cell_count, bit_budget)),
        }
    }

    fn text_of(result: &CallToolResult) -> String {
        result
            .content
            .iter()
            .filter_map(|c| c.as_text().map(|t| t.text.clone()))
            .collect()
    }

    fn run_query(server: &HawkMcp, sql: &str) -> String {
        let result = server
            .query(Parameters(QueryParams {
                sql: sql.to_owned(),
            }))
            .expect("query tool");
        text_of(&result)
    }

    fn ledger_json(server: &HawkMcp) -> Value {
        let result = server.ledger().expect("ledger tool");
        serde_json::from_str(&text_of(&result)).expect("ledger json")
    }

    #[test]
    fn min_cell_count_suppresses_rare_categories_in_query_results() {
        let root = temp_db("suppress");
        let server = server(&root, Some(5), None);

        let out = run_query(&server, "SHOW category AT time:2025-01");
        assert!(!out.contains("niche"), "leaked: {}", out);
        assert!(out.contains("__unknown__"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn queries_are_charged_once_and_metadata_is_free() {
        let root = temp_db("charging");
        let server = server(&root, None, None);

        let ledger = ledger_json(&server);
        assert_eq!(ledger["total_spent_bits"], 0.0);
        assert_eq!(ledger["budget_bits"], Value::Null);
        assert_eq!(ledger["remaining_bits"], Value::Null);

        run_query(&server, "SHOW category AT time:2025-01");
        let after_show = ledger_json(&server);
        let spent = after_show["total_spent_bits"].as_f64().unwrap();
        assert!(spent > 0.0);
        assert!(
            after_show["spent_bits_per_variable"]["category"]
                .as_f64()
                .unwrap()
                .abs()
                > 0.0
        );
        assert_eq!(after_show["charged_queries"], 1);

        // Identical query (modulo whitespace) is not re-charged.
        run_query(&server, "SHOW   category AT time:2025-01");
        assert_eq!(
            ledger_json(&server)["total_spent_bits"].as_f64().unwrap(),
            spent
        );

        // Metadata is free; a failing query charges nothing.
        run_query(&server, "STATS");
        run_query(&server, "SHOW category AT time:2099-01");
        assert_eq!(
            ledger_json(&server)["total_spent_bits"].as_f64().unwrap(),
            spent
        );

        // A scalar release charges the flat amount.
        run_query(&server, "RANK category BY ENTROPY OVER time");
        let total = ledger_json(&server)["total_spent_bits"].as_f64().unwrap();
        assert!((total - spent - crate::state::scalar_release_bits()).abs() < 1e-9);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn over_budget_queries_get_a_structured_refusal_and_charge_nothing() {
        let root = temp_db("budget");
        let server = server(&root, None, Some(0.5));

        let out = run_query(&server, "SHOW category AT time:2025-01");
        let refusal: Value = serde_json::from_str(&out).expect("refusal json");
        assert_eq!(refusal["refused"], true);
        assert!(refusal["query_cost_bits"].as_f64().unwrap() > 0.5);
        assert_eq!(refusal["spent_bits"], 0.0);
        assert_eq!(refusal["budget_bits"], 0.5);
        assert_eq!(refusal["remaining_bits"], 0.5);
        assert!(refusal["reason"].as_str().unwrap().contains("bit budget"));
        assert!(
            !out.contains("news"),
            "refusal must not leak the result: {}",
            out
        );

        let ledger = ledger_json(&server);
        assert_eq!(ledger["total_spent_bits"], 0.0);
        assert_eq!(ledger["charged_queries"], 0);

        // Metadata stays free even when over budget.
        let stats = run_query(&server, "STATS");
        assert!(stats.contains("Distributions"));
        let _ = std::fs::remove_dir_all(root);
    }

    fn suggest_json(server: &HawkMcp, limit: Option<usize>) -> Vec<Value> {
        let result = server
            .suggest(Parameters(SuggestParams { limit }))
            .expect("suggest tool");
        serde_json::from_str(&text_of(&result)).expect("suggest json")
    }

    #[test]
    fn suggest_tool_returns_ranked_suggestions_and_charges_nothing() {
        let root = temp_db("suggest-free");
        let server = server(&root, None, None);

        let items = suggest_json(&server, None);
        assert!(!items.is_empty());
        // Top suggestion: the unexplored variable at the latest slice.
        assert_eq!(items[0]["query"], "SHOW category AT time:2025-02");
        assert!(items[0]["expected_bits"].as_f64().unwrap() > 0.0);
        assert!(items[0]["rationale"]
            .as_str()
            .unwrap()
            .contains("not been explored"));
        // Suggestions themselves are free.
        assert_eq!(ledger_json(&server)["total_spent_bits"], 0.0);

        // Limit applies.
        assert_eq!(suggest_json(&server, Some(1)).len(), 1);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn suggest_dedups_against_the_session_ledger() {
        let root = temp_db("suggest-dedup");
        let server = server(&root, None, None);

        run_query(&server, "SHOW  category AT time:2025-02");
        let items = suggest_json(&server, None);
        // The released variable is no longer a SHOW candidate at all.
        assert!(
            !items
                .iter()
                .any(|i| i["query"].as_str().unwrap().starts_with("SHOW category")),
            "{:?}",
            items
        );
        // Other generators for it survive.
        assert!(items
            .iter()
            .any(|i| i["query"] == "COMPARE category ACROSS time"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn suggest_annotates_whether_each_query_fits_the_budget() {
        let root = temp_db("suggest-budget");

        let unbudgeted = server(&root, None, None);
        for item in suggest_json(&unbudgeted, None) {
            assert!(item["cost_bits"].as_f64().unwrap() > 0.0);
            assert_eq!(item["fits_budget"], true);
        }

        let tight = server(&root, None, Some(0.5));
        for item in suggest_json(&tight, None) {
            assert_eq!(item["fits_budget"], false, "{:?}", item);
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn profile_tool_returns_dataset_card_and_charges_per_variable_once() {
        let root = temp_db("profile");
        let server = server(&root, None, None);

        let result = server.profile().expect("profile tool");
        let card: Value = serde_json::from_str(&text_of(&result)).expect("profile json");

        assert_eq!(card["variables"][0]["name"], "category");
        assert_eq!(card["variables"][0]["var_type"], "categorical");
        assert!(card["variables"][0]["entropy"].as_f64().unwrap() > 0.0);
        assert_eq!(card["dimensions"][0]["name"], "time");
        assert_eq!(card["dimensions"][0]["value_count"], 2);
        let drift = &card["biggest_drift"];
        assert_eq!(drift["variable"], "category");
        assert_eq!(drift["time_from"], "2025-01");
        assert_eq!(drift["time_to"], "2025-02");
        assert!(card["total_samples"].as_u64().unwrap() > 0);
        assert!(card["approx_bytes"].as_u64().unwrap() > 0);

        // One flat scalar per variable, charged once per session.
        let expected = crate::state::scalar_release_bits();
        let ledger = ledger_json(&server);
        assert!((ledger["total_spent_bits"].as_f64().unwrap() - expected).abs() < 1e-9);
        assert!(
            (ledger["spent_bits_per_variable"]["category"].as_f64().unwrap() - expected).abs()
                < 1e-9
        );
        let _ = server.profile().expect("profile tool again");
        assert!(
            (ledger_json(&server)["total_spent_bits"].as_f64().unwrap() - expected).abs() < 1e-9
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn profile_tool_respects_the_bit_budget() {
        let root = temp_db("profile-budget");
        let server = server(&root, None, Some(0.5));

        let result = server.profile().expect("profile tool");
        let refusal: Value = serde_json::from_str(&text_of(&result)).expect("refusal json");
        assert_eq!(refusal["refused"], true);
        assert!(refusal["query_cost_bits"].as_f64().unwrap() > 0.5);
        assert_eq!(ledger_json(&server)["total_spent_bits"], 0.0);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn within_budget_queries_succeed_and_spend_accumulates() {
        let root = temp_db("within-budget");
        let server = server(&root, None, Some(100.0));

        let out = run_query(&server, "SHOW category AT time:2025-01");
        assert!(out.contains("news"));
        let ledger = ledger_json(&server);
        let spent = ledger["total_spent_bits"].as_f64().unwrap();
        assert!(spent > 0.0);
        assert!((ledger["remaining_bits"].as_f64().unwrap() - (100.0 - spent)).abs() < 1e-9);
        let _ = std::fs::remove_dir_all(root);
    }
}
