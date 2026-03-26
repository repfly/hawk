use std::sync::{Arc, Mutex};

use axum::{
    extract::{Query, State},
    response::Html,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use hawk_engine::ingest::batch_updater::apply_batch;
use hawk_engine::ingest::column_mapper::map_row;
use hawk_engine::query::QueryEngine;
use hawk_engine::storage::{Database, OpenMode};

mod templates;
mod charts;

struct AppState {
    db: Mutex<Database>,
    engine: QueryEngine,
    db_path: String,
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: hawk-server <database_path> [port] [--readonly]");
        std::process::exit(1);
    }

    let db_path = &args[1];
    let port = args.get(2).and_then(|p| p.parse::<u16>().ok()).unwrap_or(3000);

    let readonly = args.iter().any(|a| a == "--readonly");
    let mode = if readonly {
        OpenMode::ReadOnly
    } else {
        OpenMode::ReadWrite
    };

    let db = Database::open(db_path, mode).unwrap_or_else(|e| {
        eprintln!("Error opening database '{}': {}", db_path, e);
        std::process::exit(1);
    });

    let state = Arc::new(AppState {
        db: Mutex::new(db),
        engine: QueryEngine::default(),
        db_path: db_path.to_string(),
    });

    let app = Router::new()
        .route("/", get(index_page))
        .route("/query", get(handle_query))
        .route("/overview", get(overview_fragment))
        .route("/ingest", post(handle_ingest))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    println!("Hawk server running at http://localhost:{}", port);
    println!("Database: {} (mode: {})", db_path, if readonly { "read-only" } else { "read-write" });

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn index_page(State(state): State<Arc<AppState>>) -> Html<String> {
    let db = state.db.lock().unwrap();
    let stats = db.stats();
    let schema = db.schema().clone();
    drop(db);

    Html(templates::index_page(&state.db_path, &stats, &schema))
}

#[derive(Deserialize)]
struct QueryParams {
    q: String,
}

async fn handle_query(
    State(state): State<Arc<AppState>>,
    Query(params): Query<QueryParams>,
) -> Html<String> {
    let q = params.q.trim().to_string();
    if q.is_empty() {
        return Html("<div class=\"error\">Empty query</div>".into());
    }

    let db = state.db.lock().unwrap();
    match hawk_engine::sql::query(&db, &state.engine, &q) {
        Ok(result) => {
            let chart_html = charts::maybe_chart(&q, &db, &state.engine);
            drop(db);
            Html(templates::query_result(&q, &result, &chart_html))
        }
        Err(e) => {
            drop(db);
            Html(templates::query_error(&q, &e.to_string()))
        }
    }
}

async fn overview_fragment(State(state): State<Arc<AppState>>) -> Html<String> {
    let db = state.db.lock().unwrap();

    let schema = db.schema().clone();
    let first_var = schema.first_variable_name().map(ToOwned::to_owned);
    let dims: Vec<String> = schema.dimensions.iter().map(|d| d.name.clone()).collect();

    let mut overview_parts = Vec::new();

    if let (Some(var), Some(dim)) = (&first_var, dims.first()) {
        let mut ranked: Vec<_> = db
            .distributions_for_variable(var)
            .into_iter()
            .filter_map(|d| {
                d.dimension_key
                    .get(dim)
                    .map(|v| (v.clone(), d.entropy, d.sample_count))
            })
            .collect();
        ranked.sort_by(|a, b| a.0.cmp(&b.0));

        if !ranked.is_empty() {
            overview_parts.push(charts::entropy_timeline_svg(&ranked, var, dim));
        }
    }

    drop(db);
    Html(overview_parts.join("\n"))
}

#[derive(Serialize)]
struct IngestResponse {
    processed: usize,
    skipped: usize,
    distributions_updated: usize,
}

#[derive(Serialize)]
struct IngestErrorResponse {
    error: String,
}

async fn handle_ingest(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Value>,
) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    let records: Vec<Map<String, Value>> = match body {
        Value::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                if let Value::Object(map) = item {
                    out.push(map);
                }
            }
            out
        }
        Value::Object(map) => vec![map],
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(IngestErrorResponse {
                    error: "request body must be a JSON object or array of objects".into(),
                }),
            )
                .into_response();
        }
    };

    let mut db = state.db.lock().unwrap();
    let schema = db.schema().clone();

    let mapping = hawk_engine::ingest::schema_inference::identity_mapping(&schema);

    let mut mapped_rows = Vec::with_capacity(records.len());
    for record in &records {
        if let Some(mapped) = map_row(record, &mapping) {
            mapped_rows.push(mapped);
        }
    }

    match apply_batch(&mut db, &schema, &mapped_rows) {
        Ok(report) => {
            if let Err(e) = db.flush() {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(IngestErrorResponse {
                        error: format!("flush failed: {}", e),
                    }),
                )
                    .into_response();
            }

            (
                StatusCode::OK,
                Json(IngestResponse {
                    processed: report.processed,
                    skipped: report.skipped,
                    distributions_updated: report.distributions_updated,
                }),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(IngestErrorResponse {
                error: format!("ingestion failed: {}", e),
            }),
        )
            .into_response(),
    }
}
