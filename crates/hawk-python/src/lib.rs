use std::path::PathBuf;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList};

use ::hawk_engine::ingest::batch_updater::apply_batch;
use ::hawk_engine::ingest::column_mapper::map_row;
use ::hawk_engine::ingest::schema_inference::identity_mapping;
use ::hawk_engine::ingest::{InferConfig, IngestOptions, IngestionPipeline};
use ::hawk_engine::query::QueryEngine;
use ::hawk_engine::sql;
use ::hawk_engine::storage::{Database, OpenMode};

// ---------------------------------------------------------------------------
// Exception
// ---------------------------------------------------------------------------

pyo3::create_exception!(hawk_engine, HawkError, pyo3::exceptions::PyException);

fn to_py_err(e: anyhow::Error) -> PyErr {
    HawkError::new_err(format!("{:#}", e))
}

// ---------------------------------------------------------------------------
// QueryResult
// ---------------------------------------------------------------------------

#[pyclass(frozen, skip_from_py_object)]
#[derive(Clone)]
struct QueryResult {
    inner: ::hawk_engine::sql::formatter::QueryResult,
}

#[pymethods]
impl QueryResult {
    #[getter]
    fn header(&self) -> Vec<String> {
        self.inner.header.clone()
    }

    #[getter]
    fn rows(&self) -> Vec<Vec<String>> {
        self.inner.rows.clone()
    }

    fn to_csv(&self) -> String {
        self.inner.to_csv()
    }

    fn to_json(&self) -> String {
        self.inner.to_json()
    }

    fn to_dicts<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty(py);
        for row in &self.inner.rows {
            let dict = PyDict::new(py);
            for (i, cell) in row.iter().enumerate() {
                if let Some(key) = self.inner.header.get(i) {
                    dict.set_item(key, cell)?;
                }
            }
            list.append(dict)?;
        }
        Ok(list)
    }

    fn __repr__(&self) -> String {
        format!("{}", self.inner)
    }

    fn __len__(&self) -> usize {
        self.inner.rows.len()
    }
}

// ---------------------------------------------------------------------------
// DatabaseStats
// ---------------------------------------------------------------------------

#[pyclass(frozen, skip_from_py_object)]
#[derive(Clone)]
struct DatabaseStats {
    #[pyo3(get)]
    distributions: usize,
    #[pyo3(get)]
    total_samples: u64,
    #[pyo3(get)]
    variables: usize,
    #[pyo3(get)]
    dimensions: usize,
}

#[pymethods]
impl DatabaseStats {
    fn __repr__(&self) -> String {
        format!(
            "DatabaseStats(distributions={}, total_samples={}, variables={}, dimensions={})",
            self.distributions, self.total_samples, self.variables, self.dimensions
        )
    }
}

// ---------------------------------------------------------------------------
// IngestReport
// ---------------------------------------------------------------------------

#[pyclass(frozen, skip_from_py_object)]
#[derive(Clone)]
struct IngestReport {
    #[pyo3(get)]
    total_rows: usize,
    #[pyo3(get)]
    processed_rows: usize,
    #[pyo3(get)]
    skipped_rows: usize,
    #[pyo3(get)]
    distributions_updated: usize,
    #[pyo3(get)]
    elapsed_ms: u128,
}

#[pymethods]
impl IngestReport {
    fn __repr__(&self) -> String {
        format!(
            "IngestReport(processed={}, skipped={}, distributions_updated={}, elapsed_ms={})",
            self.processed_rows, self.skipped_rows, self.distributions_updated, self.elapsed_ms
        )
    }
}

// ---------------------------------------------------------------------------
// HawkDB
// ---------------------------------------------------------------------------

#[pyclass(unsendable)]
struct HawkDB {
    inner: Option<(Database, QueryEngine)>,
}

impl HawkDB {
    fn db_ref(&self) -> PyResult<(&Database, &QueryEngine)> {
        match &self.inner {
            Some((db, engine)) => Ok((db, engine)),
            None => Err(PyRuntimeError::new_err("database is closed")),
        }
    }

    fn db_mut(&mut self) -> PyResult<(&mut Database, &QueryEngine)> {
        match &mut self.inner {
            Some((db, engine)) => Ok((db, engine)),
            None => Err(PyRuntimeError::new_err("database is closed")),
        }
    }
}

#[pymethods]
impl HawkDB {
    // --- Constructors ---

    #[staticmethod]
    #[pyo3(signature = (path, mode=None))]
    fn open(path: &str, mode: Option<&str>) -> PyResult<Self> {
        let open_mode = match mode.unwrap_or("read_write") {
            "read_only" | "ro" => OpenMode::ReadOnly,
            "read_write" | "rw" => OpenMode::ReadWrite,
            other => {
                return Err(PyValueError::new_err(format!(
                    "unknown mode '{}'; use 'read_only' or 'read_write'",
                    other
                )))
            }
        };
        let db = Database::open(PathBuf::from(path), open_mode).map_err(to_py_err)?;
        Ok(Self {
            inner: Some((db, QueryEngine::default())),
        })
    }

    #[staticmethod]
    fn create(path: &str) -> PyResult<Self> {
        let db = Database::create(PathBuf::from(path)).map_err(to_py_err)?;
        Ok(Self {
            inner: Some((db, QueryEngine::default())),
        })
    }

    // --- Lifecycle ---

    fn flush(&self) -> PyResult<()> {
        let (db, _) = self.db_ref()?;
        db.flush().map_err(to_py_err)
    }

    fn close(&mut self) -> PyResult<()> {
        if let Some((db, _)) = self.inner.take() {
            db.flush().map_err(to_py_err)?;
        }
        Ok(())
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &mut self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.close()?;
        Ok(false)
    }

    // --- Introspection ---

    fn stats(&self) -> PyResult<DatabaseStats> {
        let (db, _) = self.db_ref()?;
        let s = db.stats();
        Ok(DatabaseStats {
            distributions: s.distributions,
            total_samples: s.total_samples,
            variables: s.variables,
            dimensions: s.dimensions,
        })
    }

    fn schema<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let (db, _) = self.db_ref()?;
        let schema = db.schema();
        let json_str =
            serde_json::to_string(schema).map_err(|e| to_py_err(anyhow::anyhow!(e)))?;
        let json_mod = py.import("json")?;
        let result = json_mod.call_method1("loads", (json_str,))?;
        Ok(result)
    }

    // --- Query ---

    fn query(&self, sql_str: &str) -> PyResult<QueryResult> {
        let (db, engine) = self.db_ref()?;
        let result = sql::query(db, engine, sql_str).map_err(to_py_err)?;
        Ok(QueryResult { inner: result })
    }

    // --- Ingestion ---

    #[pyo3(signature = (path, max_categories=None, date_columns=None, date_granularity=None))]
    fn ingest(
        &mut self,
        path: &str,
        max_categories: Option<usize>,
        date_columns: Option<Vec<String>>,
        date_granularity: Option<&str>,
    ) -> PyResult<IngestReport> {
        let (db, _) = self.db_mut()?;
        let config = InferConfig {
            max_categories: max_categories.unwrap_or(50),
            date_columns: date_columns.unwrap_or_default(),
            date_granularity: date_granularity.unwrap_or("yearly").to_owned(),
            ..InferConfig::default()
        };
        let report =
            IngestionPipeline::ingest_file_auto(db, PathBuf::from(path), config, IngestOptions::default())
                .map_err(to_py_err)?;
        Ok(IngestReport {
            total_rows: report.total_rows,
            processed_rows: report.processed_rows,
            skipped_rows: report.skipped_rows,
            distributions_updated: report.distributions_updated,
            elapsed_ms: report.elapsed_ms,
        })
    }

    fn ingest_records(&mut self, records: &Bound<'_, PyList>) -> PyResult<IngestReport> {
        let (db, _) = self.db_mut()?;
        let schema = db.schema().clone();
        let mapping = identity_mapping(&schema);

        let mut json_rows: Vec<serde_json::Map<String, serde_json::Value>> =
            Vec::with_capacity(records.len());
        for item in records.iter() {
            let dict: &Bound<'_, PyDict> = item.cast::<PyDict>()?;
            let map = py_dict_to_json_map(dict)?;
            json_rows.push(map);
        }

        let mut mapped = Vec::with_capacity(json_rows.len());
        for row in &json_rows {
            if let Some(m) = map_row(row, &mapping) {
                mapped.push(m);
            }
        }

        let report = apply_batch(db, &schema, &mapped).map_err(to_py_err)?;
        db.flush().map_err(to_py_err)?;

        Ok(IngestReport {
            total_rows: json_rows.len(),
            processed_rows: report.processed,
            skipped_rows: report.skipped,
            distributions_updated: report.distributions_updated,
            elapsed_ms: 0,
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn py_dict_to_json_map(
    dict: &Bound<'_, PyDict>,
) -> PyResult<serde_json::Map<String, serde_json::Value>> {
    let mut map = serde_json::Map::new();
    for (key, value) in dict.iter() {
        let k: String = key.extract()?;
        let v = py_to_json_value(&value)?;
        map.insert(k, v);
    }
    Ok(map)
}

fn py_to_json_value(obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    if obj.is_none() {
        Ok(serde_json::Value::Null)
    } else if let Ok(b) = obj.extract::<bool>() {
        // Check bool before i64 — Python bool is a subclass of int
        Ok(serde_json::Value::Bool(b))
    } else if let Ok(i) = obj.extract::<i64>() {
        Ok(serde_json::Value::Number(i.into()))
    } else if let Ok(f) = obj.extract::<f64>() {
        Ok(serde_json::json!(f))
    } else if let Ok(s) = obj.extract::<String>() {
        Ok(serde_json::Value::String(s))
    } else {
        let s = obj.str()?.to_string();
        Ok(serde_json::Value::String(s))
    }
}

// ---------------------------------------------------------------------------
// Module
// ---------------------------------------------------------------------------

#[pymodule]
fn hawk_engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<HawkDB>()?;
    m.add_class::<QueryResult>()?;
    m.add_class::<DatabaseStats>()?;
    m.add_class::<IngestReport>()?;
    m.add("HawkError", m.py().get_type::<HawkError>())?;
    Ok(())
}
