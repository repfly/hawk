use std::sync::Mutex;

use hawk_engine::query::QueryEngine;
use hawk_engine::storage::Database;
use rmcp::ErrorData;

pub struct AppState {
    pub db: Mutex<Option<Database>>,
    pub engine: QueryEngine,
    pub db_path: Mutex<Option<String>>,
}

impl AppState {
    pub fn new(db: Option<Database>, path: Option<String>) -> Self {
        Self {
            db: Mutex::new(db),
            engine: QueryEngine::default(),
            db_path: Mutex::new(path),
        }
    }

    pub fn with_db<F, T>(&self, f: F) -> Result<T, ErrorData>
    where
        F: FnOnce(&Database, &QueryEngine) -> Result<T, ErrorData>,
    {
        let guard = self.db.lock().map_err(|e| {
            ErrorData::internal_error(format!("database lock poisoned: {}", e), None)
        })?;
        let db = guard.as_ref().ok_or_else(|| {
            ErrorData::invalid_params(
                "no database open — use open_database or create_database first",
                None,
            )
        })?;
        f(db, &self.engine)
    }

    pub fn with_db_mut<F, T>(&self, f: F) -> Result<T, ErrorData>
    where
        F: FnOnce(&mut Database) -> Result<T, ErrorData>,
    {
        let mut guard = self.db.lock().map_err(|e| {
            ErrorData::internal_error(format!("database lock poisoned: {}", e), None)
        })?;
        let db = guard.as_mut().ok_or_else(|| {
            ErrorData::invalid_params(
                "no database open — use open_database or create_database first",
                None,
            )
        })?;
        f(db)
    }

    pub fn swap_db(&self, new_db: Database, path: String) -> Result<(), ErrorData> {
        let mut db_guard = self.db.lock().map_err(|e| {
            ErrorData::internal_error(format!("lock poisoned: {}", e), None)
        })?;
        if let Some(ref mut old_db) = *db_guard {
            let _ = old_db.flush();
        }
        *db_guard = Some(new_db);
        drop(db_guard);

        let mut path_guard = self.db_path.lock().map_err(|e| {
            ErrorData::internal_error(format!("lock poisoned: {}", e), None)
        })?;
        *path_guard = Some(path);
        Ok(())
    }
}
