use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{
    extract::{DefaultBodyLimit, Query, State},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use hawk_engine::ingest::batch_updater::apply_batch;
use hawk_engine::ingest::column_mapper::{map_row, MappedRow};
use hawk_engine::query::QueryEngine;
use hawk_engine::storage::{Database, OpenMode};

mod charts;
mod templates;

struct IngestBuffer {
    rows: Vec<MappedRow>,
    capacity: usize,
}

impl IngestBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            rows: Vec::new(),
            capacity,
        }
    }
}

struct AppState {
    db: Mutex<Database>,
    engine: QueryEngine,
    db_path: String,
    buffer: Mutex<IngestBuffer>,
    config: ServerConfig,
}

#[derive(Debug, Clone)]
struct ServerConfig {
    db_path: String,
    bind_addr: String,
    port: u16,
    readonly: bool,
    ingest_enabled: bool,
    max_request_bytes: usize,
    max_batch_size: usize,
    auth_token: Option<String>,
}

impl ServerConfig {
    fn from_args(args: &[String]) -> Result<Self, String> {
        if args.len() < 2 {
            return Err(
                "Usage: hawk-server <database_path> [port] [--readonly] [--bind <addr>] [--max-body-bytes <n>] [--max-batch-size <n>] [--auth-token <token>] [--disable-ingest]".into(),
            );
        }

        let mut config = Self {
            db_path: args[1].clone(),
            bind_addr: "127.0.0.1".to_owned(),
            port: 3000,
            readonly: false,
            ingest_enabled: true,
            max_request_bytes: 1_048_576,
            max_batch_size: 1_000,
            auth_token: std::env::var("HAWK_SERVER_TOKEN").ok(),
        };

        let mut i = 2;
        if let Some(port) = args
            .get(i)
            .and_then(|value| (!value.starts_with("--")).then_some(value))
        {
            config.port = port
                .parse()
                .map_err(|_| format!("invalid port '{}'", port))?;
            i += 1;
        }

        while i < args.len() {
            match args[i].as_str() {
                "--readonly" => {
                    config.readonly = true;
                    i += 1;
                }
                "--disable-ingest" => {
                    config.ingest_enabled = false;
                    i += 1;
                }
                "--bind" => {
                    config.bind_addr = next_arg(args, i, "--bind")?.to_owned();
                    i += 2;
                }
                "--max-body-bytes" => {
                    let value = next_arg(args, i, "--max-body-bytes")?;
                    config.max_request_bytes = value
                        .parse()
                        .map_err(|_| format!("invalid --max-body-bytes '{}'", value))?;
                    i += 2;
                }
                "--max-batch-size" => {
                    let value = next_arg(args, i, "--max-batch-size")?;
                    config.max_batch_size = value
                        .parse()
                        .map_err(|_| format!("invalid --max-batch-size '{}'", value))?;
                    i += 2;
                }
                "--auth-token" => {
                    config.auth_token = Some(next_arg(args, i, "--auth-token")?.to_owned());
                    i += 2;
                }
                other => return Err(format!("unknown argument '{}'", other)),
            }
        }

        if config.max_batch_size == 0 {
            return Err("--max-batch-size must be greater than zero".into());
        }
        if config.max_request_bytes == 0 {
            return Err("--max-body-bytes must be greater than zero".into());
        }

        Ok(config)
    }

    fn socket_addr(&self) -> Result<SocketAddr, String> {
        format!("{}:{}", self.bind_addr, self.port)
            .parse()
            .map_err(|_| "invalid bind address or port".to_owned())
    }
}

fn next_arg<'a>(args: &'a [String], index: usize, flag: &str) -> Result<&'a str, String> {
    args.get(index + 1)
        .map(|value| value.as_str())
        .ok_or_else(|| format!("{} requires a value", flag))
}

/// Drain the buffer and flush to the database.
/// Lock order: buffer first, then db.
fn flush_buffer(state: &AppState) -> Result<FlushReport, String> {
    let rows = {
        let mut buf = state.buffer.lock().unwrap();
        if buf.rows.is_empty() {
            return Ok(FlushReport {
                flushed: 0,
                distributions_updated: 0,
            });
        }
        std::mem::take(&mut buf.rows)
    };

    let mut db = state.db.lock().unwrap();
    let schema = db.schema().clone();

    match apply_batch(&mut db, &schema, &rows) {
        Ok(report) => {
            db.flush().map_err(|e| format!("flush failed: {}", e))?;
            Ok(FlushReport {
                flushed: report.processed,
                distributions_updated: report.distributions_updated,
            })
        }
        Err(e) => Err(format!("batch apply failed: {}", e)),
    }
}

#[derive(Serialize)]
struct FlushReport {
    flushed: usize,
    distributions_updated: usize,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Serialize)]
struct VersionResponse {
    name: &'static str,
    version: &'static str,
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let config = ServerConfig::from_args(&args).unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(1);
    });
    let mode = if config.readonly {
        OpenMode::ReadOnly
    } else {
        OpenMode::ReadWrite
    };

    let db = Database::open(&config.db_path, mode).unwrap_or_else(|e| {
        eprintln!("Error opening database: {}", e);
        std::process::exit(1);
    });

    let state = Arc::new(AppState {
        db: Mutex::new(db),
        engine: QueryEngine::default(),
        db_path: config.db_path.clone(),
        buffer: Mutex::new(IngestBuffer::new(config.max_batch_size)),
        config: config.clone(),
    });

    // Background flush timer
    if !config.readonly && config.ingest_enabled {
        let state_clone = Arc::clone(&state);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                let _ = flush_buffer(&state_clone);
            }
        });
    }

    let app = Router::new()
        .route("/", get(index_page))
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/query", get(handle_query))
        .route("/overview", get(overview_fragment))
        .route("/ingest", post(handle_ingest))
        .route("/flush", post(handle_flush))
        .layer(DefaultBodyLimit::max(config.max_request_bytes))
        .with_state(state);

    let addr = config.socket_addr().unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(1);
    });
    println!("Hawk server running at http://{}", addr);
    println!(
        "Database: {} (mode: {})",
        config.db_path,
        if config.readonly {
            "read-only"
        } else {
            "read-write"
        }
    );
    println!(
        "Ingest: {} (max body: {} bytes, max batch: {}, auth: {})",
        if config.ingest_enabled {
            "enabled"
        } else {
            "disabled"
        },
        config.max_request_bytes,
        config.max_batch_size,
        if config.auth_token.is_some() {
            "required"
        } else {
            "disabled"
        }
    );

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn version() -> Json<VersionResponse> {
    Json(VersionResponse {
        name: "hawk-server",
        version: env!("CARGO_PKG_VERSION"),
    })
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
    accepted: usize,
    flushed: bool,
    buffer_size: usize,
}

#[derive(Serialize)]
struct IngestErrorResponse {
    error: String,
}

fn authorized(headers: &HeaderMap, token: Option<&str>) -> bool {
    let Some(token) = token else {
        return true;
    };

    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        == Some(token)
}

fn parse_ingest_records(
    body: Value,
    max_batch_size: usize,
) -> Result<Vec<Map<String, Value>>, (StatusCode, String)> {
    let records = match body {
        Value::Array(arr) => {
            if arr.len() > max_batch_size {
                return Err((
                    StatusCode::PAYLOAD_TOO_LARGE,
                    format!("batch size exceeds maximum of {}", max_batch_size),
                ));
            }
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                match item {
                    Value::Object(map) => out.push(map),
                    _ => {
                        return Err((
                            StatusCode::BAD_REQUEST,
                            "array body must contain only JSON objects".into(),
                        ))
                    }
                }
            }
            out
        }
        Value::Object(map) => vec![map],
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                "request body must be a JSON object or array of objects".into(),
            ))
        }
    };

    Ok(records)
}

fn error_response(status: StatusCode, message: impl Into<String>) -> axum::response::Response {
    (
        status,
        Json(IngestErrorResponse {
            error: message.into(),
        }),
    )
        .into_response()
}

async fn handle_ingest(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> axum::response::Response {
    if state.config.readonly || !state.config.ingest_enabled {
        return error_response(StatusCode::FORBIDDEN, "ingest is disabled");
    }

    if !authorized(&headers, state.config.auth_token.as_deref()) {
        return error_response(StatusCode::UNAUTHORIZED, "missing or invalid bearer token");
    }

    let records = match parse_ingest_records(body, state.config.max_batch_size) {
        Ok(records) => records,
        Err((status, message)) => return error_response(status, message),
    };

    // Map rows using current schema
    let mapped_rows = {
        let db = state.db.lock().unwrap();
        let schema = db.schema().clone();
        let mapping = hawk_engine::ingest::schema_inference::identity_mapping(&schema);
        records
            .iter()
            .filter_map(|record| map_row(record, &mapping))
            .collect::<Vec<_>>()
    };

    let accepted = mapped_rows.len();

    // Buffer the rows
    let should_flush = {
        let mut buf = state.buffer.lock().unwrap();
        buf.rows.extend(mapped_rows);
        buf.rows.len() >= buf.capacity
    };

    if should_flush {
        match flush_buffer(&state) {
            Ok(_) => (
                StatusCode::OK,
                Json(IngestResponse {
                    accepted,
                    flushed: true,
                    buffer_size: 0,
                }),
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(IngestErrorResponse {
                    error: format!("flush failed: {}", e),
                }),
            )
                .into_response(),
        }
    } else {
        let buffer_size = state.buffer.lock().unwrap().rows.len();
        (
            StatusCode::OK,
            Json(IngestResponse {
                accepted,
                flushed: false,
                buffer_size,
            }),
        )
            .into_response()
    }
}

async fn handle_flush(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> axum::response::Response {
    if state.config.readonly || !state.config.ingest_enabled {
        return error_response(StatusCode::FORBIDDEN, "ingest is disabled");
    }

    if !authorized(&headers, state.config.auth_token.as_deref()) {
        return error_response(StatusCode::UNAUTHORIZED, "missing or invalid bearer token");
    }

    match flush_buffer(&state) {
        Ok(report) => (StatusCode::OK, Json(report)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(IngestErrorResponse {
                error: format!("flush failed: {}", e),
            }),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::header::AUTHORIZATION;

    #[test]
    fn config_defaults_to_local_safe_bind() {
        let args = vec!["hawk-server".to_owned(), "/tmp/hawk-db".to_owned()];
        let config = ServerConfig::from_args(&args).expect("config");

        assert_eq!(config.bind_addr, "127.0.0.1");
        assert_eq!(config.port, 3000);
        assert!(config.ingest_enabled);
        assert!(!config.readonly);
        assert_eq!(config.max_batch_size, 1_000);
    }

    #[test]
    fn config_parses_hardening_flags() {
        let args = vec![
            "hawk-server".to_owned(),
            "/tmp/hawk-db".to_owned(),
            "4000".to_owned(),
            "--readonly".to_owned(),
            "--bind".to_owned(),
            "0.0.0.0".to_owned(),
            "--max-body-bytes".to_owned(),
            "2048".to_owned(),
            "--max-batch-size".to_owned(),
            "5".to_owned(),
            "--auth-token".to_owned(),
            "secret".to_owned(),
            "--disable-ingest".to_owned(),
        ];

        let config = ServerConfig::from_args(&args).expect("config");
        assert_eq!(config.bind_addr, "0.0.0.0");
        assert_eq!(config.port, 4000);
        assert!(config.readonly);
        assert!(!config.ingest_enabled);
        assert_eq!(config.max_request_bytes, 2048);
        assert_eq!(config.max_batch_size, 5);
        assert_eq!(config.auth_token.as_deref(), Some("secret"));
    }

    #[test]
    fn auth_requires_bearer_token_when_configured() {
        let mut headers = HeaderMap::new();
        assert!(authorized(&headers, None));
        assert!(!authorized(&headers, Some("secret")));

        headers.insert(AUTHORIZATION, "Bearer secret".parse().unwrap());
        assert!(authorized(&headers, Some("secret")));
        assert!(!authorized(&headers, Some("other")));
    }

    #[test]
    fn ingest_parser_rejects_invalid_payloads() {
        let err = parse_ingest_records(Value::String("bad".into()), 10).unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);

        let err =
            parse_ingest_records(Value::Array(vec![Value::String("bad".into())]), 10).unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn ingest_parser_rejects_oversized_batches() {
        let body = Value::Array(vec![Value::Object(Map::new()), Value::Object(Map::new())]);
        let err = parse_ingest_records(body, 1).unwrap_err();
        assert_eq!(err.0, StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn ingest_parser_accepts_object_and_object_array() {
        assert_eq!(
            parse_ingest_records(Value::Object(Map::new()), 10)
                .expect("object")
                .len(),
            1
        );
        assert_eq!(
            parse_ingest_records(Value::Array(vec![Value::Object(Map::new())]), 10)
                .expect("array")
                .len(),
            1
        );
    }
}
