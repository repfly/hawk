use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;

use hawk_engine::query::planner::resolve_distribution;
use hawk_engine::query::QueryEngine;
use hawk_engine::sql::parser::{DimRef, Statement};
use hawk_engine::storage::Database;
use rmcp::ErrorData;

pub struct AppState {
    pub db: Mutex<Option<Database>>,
    pub engine: QueryEngine,
    pub db_path: Mutex<Option<String>>,
    pub ledger: Mutex<Ledger>,
}

impl AppState {
    pub fn new(
        db: Option<Database>,
        path: Option<String>,
        min_cell_count: Option<u64>,
        bit_budget: Option<f64>,
    ) -> Self {
        Self {
            db: Mutex::new(db),
            engine: QueryEngine::default().with_min_cell_count(min_cell_count),
            db_path: Mutex::new(path),
            ledger: Mutex::new(Ledger::new(bit_budget)),
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
        let mut db_guard = self
            .db
            .lock()
            .map_err(|e| ErrorData::internal_error(format!("lock poisoned: {}", e), None))?;
        if let Some(ref mut old_db) = *db_guard {
            let _ = old_db.flush();
        }
        *db_guard = Some(new_db);
        drop(db_guard);

        let mut path_guard = self
            .db_path
            .lock()
            .map_err(|e| ErrorData::internal_error(format!("lock poisoned: {}", e), None))?;
        *path_guard = Some(path);
        Ok(())
    }
}

// --- Information ledger (docs/information-ledger.md) ---

/// Flat charge for a scalar-only release: log2 of the number of
/// distinguishable values at the reported 4-decimal precision.
pub fn scalar_release_bits() -> f64 {
    1e4_f64.log2()
}

/// Reserved key for scalar releases not attributable to a single variable.
pub const DATABASE_LEDGER_KEY: &str = "__database__";

/// Per-session disclosure accounting: cumulative bits revealed per variable,
/// with an optional global budget. Charges happen on success only; identical
/// queries (by fingerprint) are charged once.
pub struct Ledger {
    budget_bits: Option<f64>,
    spent: BTreeMap<String, f64>,
    charged_fingerprints: HashSet<String>,
}

impl Ledger {
    pub fn new(budget_bits: Option<f64>) -> Self {
        Self {
            budget_bits,
            spent: BTreeMap::new(),
            charged_fingerprints: HashSet::new(),
        }
    }

    pub fn budget_bits(&self) -> Option<f64> {
        self.budget_bits
    }

    pub fn total_spent_bits(&self) -> f64 {
        self.spent.values().sum()
    }

    pub fn remaining_bits(&self) -> Option<f64> {
        self.budget_bits
            .map(|b| (b - self.total_spent_bits()).max(0.0))
    }

    pub fn spent_per_variable(&self) -> &BTreeMap<String, f64> {
        &self.spent
    }

    pub fn charged_query_count(&self) -> usize {
        self.charged_fingerprints.len()
    }

    pub fn is_charged(&self, fingerprint: &str) -> bool {
        self.charged_fingerprints.contains(fingerprint)
    }

    /// Fingerprints already charged this session — the executed-query history
    /// fed to the `suggest` tool's dedup.
    pub fn charged_fingerprints(&self) -> &HashSet<String> {
        &self.charged_fingerprints
    }

    pub fn would_exceed(&self, cost_bits: f64) -> bool {
        match self.budget_bits {
            Some(budget) => self.total_spent_bits() + cost_bits > budget,
            None => false,
        }
    }

    pub fn charge(&mut self, fingerprint: String, charges: Vec<(String, f64)>) {
        for (key, bits) in charges {
            *self.spent.entry(key).or_insert(0.0) += bits;
        }
        self.charged_fingerprints.insert(fingerprint);
    }
}

/// Query text with whitespace collapsed — identical queries charge once.
/// Delegates to the engine's normalization so the `suggest` dedup and the
/// ledger fingerprints can never drift apart.
pub fn query_fingerprint(sql: &str) -> String {
    hawk_engine::query::suggest::normalize_query(sql)
}

/// "Bits revealed" by a successful statement, per docs/information-ledger.md:
/// distribution releases charge the entropy of the released (post-suppression)
/// distribution to the variable; joint releases charge the joint entropy to
/// the pair key; scalar-only releases charge a flat amount; metadata is free.
pub fn charges_for_statement(
    db: &Database,
    engine: &QueryEngine,
    stmt: &Statement,
) -> Vec<(String, f64)> {
    let scalar = scalar_release_bits();
    match stmt {
        Statement::Show {
            variable,
            reference,
            filters,
            ..
        } => vec![(
            variable.clone(),
            released_entropy(db, engine, variable, reference, filters),
        )],
        Statement::ExportDistribution {
            variable,
            reference,
        } => vec![(
            variable.clone(),
            released_entropy(db, engine, variable, reference, &[]),
        )],
        Statement::Compare {
            variable,
            ref_a,
            ref_b,
            filters,
        } => vec![(
            variable.clone(),
            released_entropy(db, engine, variable, ref_a, filters)
                + released_entropy(db, engine, variable, ref_b, filters),
        )],
        Statement::Surprise {
            ref_a,
            ref_b,
            variable: Some(variable),
        } => vec![(
            variable.clone(),
            released_entropy(db, engine, variable, ref_a, &[])
                + released_entropy(db, engine, variable, ref_b, &[]),
        )],
        // Scalar table, one row per variable.
        Statement::Surprise { variable: None, .. } => db
            .schema()
            .variables
            .iter()
            .map(|v| (v.name.clone(), scalar))
            .collect(),
        Statement::Explain { ref_a, ref_b } => db
            .schema()
            .variables
            .iter()
            .map(|v| {
                (
                    v.name.clone(),
                    released_entropy(db, engine, &v.name, ref_a, &[])
                        + released_entropy(db, engine, &v.name, ref_b, &[]),
                )
            })
            .collect(),
        Statement::Estimate {
            var_a,
            var_b,
            reference,
        } => {
            let bits = engine
                .estimate(db, var_a, var_b, &reference.to_ref_string())
                .map(|est| est.joint_entropy)
                .unwrap_or(scalar);
            vec![(pair_key(var_a, var_b), bits)]
        }
        Statement::MutualInfo { var_a, var_b, .. }
        | Statement::ConditionalMI { var_a, var_b, .. } => vec![(pair_key(var_a, var_b), scalar)],
        Statement::Track { variable, .. }
        | Statement::Rank { variable, .. }
        | Statement::CompareAll { variable, .. }
        | Statement::Pairwise { variable, .. }
        | Statement::Alert { variable, .. } => vec![(variable.clone(), scalar)],
        Statement::Nearest { .. }
        | Statement::Structure { .. }
        | Statement::CompareStructure { .. }
        | Statement::Correlations { .. }
        // Advisory storage audit: scalar metrics per stored object.
        | Statement::AuditStorage => vec![(DATABASE_LEDGER_KEY.to_owned(), scalar)],
        // Same information, different serialization.
        Statement::Export { inner, .. } => charges_for_statement(db, engine, inner),
        // Metadata is free. SUGGEST is too: it releases ranked query text and
        // advisory scores, not distributions (SCHEMA-class by design).
        Statement::Stats
        | Statement::Schema
        | Statement::Dimensions { .. }
        | Statement::Suggest { .. } => Vec::new(),
    }
}

/// Charge for the composite `profile` tool (docs/information-ledger.md):
/// the dataset card releases one entropy scalar per variable plus a handful
/// of MI/drift scalars, so it is charged one flat scalar per schema variable.
pub fn charges_for_profile(db: &Database) -> Vec<(String, f64)> {
    db.schema()
        .variables
        .iter()
        .map(|v| (v.name.clone(), scalar_release_bits()))
        .collect()
}

/// Entropy of the distribution as released (after suppression); falls back to
/// the flat scalar charge if the slice cannot be resolved.
fn released_entropy(
    db: &Database,
    engine: &QueryEngine,
    variable: &str,
    reference: &DimRef,
    filters: &[DimRef],
) -> f64 {
    let mut dims: HashMap<String, String> = HashMap::new();
    dims.insert(reference.dimension.clone(), reference.value.clone());
    for f in filters {
        dims.insert(f.dimension.clone(), f.value.clone());
    }
    resolve_distribution(db, variable, &dims, engine.min_cell_count())
        .map(|d| d.entropy)
        .unwrap_or_else(|_| scalar_release_bits())
}

/// Canonical pair key — shared with the engine so `suggest` dedup matches.
fn pair_key(var_a: &str, var_b: &str) -> String {
    hawk_engine::query::suggest::pair_key(var_a, var_b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_collapses_whitespace_only() {
        assert_eq!(
            query_fingerprint("SHOW  category\n AT time:2024"),
            query_fingerprint("SHOW category AT time:2024")
        );
        assert_ne!(
            query_fingerprint("SHOW category AT time:2024"),
            query_fingerprint("show category at time:2024")
        );
    }

    #[test]
    fn ledger_budget_accounting() {
        let mut ledger = Ledger::new(Some(10.0));
        assert!(!ledger.would_exceed(10.0));
        assert!(ledger.would_exceed(10.1));

        ledger.charge(
            "q1".to_owned(),
            vec![("a".to_owned(), 4.0), ("b".to_owned(), 2.0)],
        );
        assert_eq!(ledger.total_spent_bits(), 6.0);
        assert_eq!(ledger.remaining_bits(), Some(4.0));
        assert!(ledger.is_charged("q1"));
        assert!(!ledger.is_charged("q2"));
        assert!(ledger.would_exceed(4.5));
        assert!(!ledger.would_exceed(4.0));
        assert_eq!(ledger.charged_query_count(), 1);
    }

    #[test]
    fn unbudgeted_ledger_never_refuses() {
        let ledger = Ledger::new(None);
        assert!(!ledger.would_exceed(f64::MAX));
        assert_eq!(ledger.remaining_bits(), None);
    }
}
