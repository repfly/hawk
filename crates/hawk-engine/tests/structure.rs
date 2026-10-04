use std::collections::HashMap;

use hawk_engine::core::{DimensionDefinition, VariableDefinition, VariableType};
use hawk_engine::ingest::{batch_updater::apply_batch, column_mapper::MappedRow};
use hawk_engine::{query::QueryEngine, sql, storage::Database};
use serde_json::Value;

fn database(name: &str) -> (Database, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!("hawk-structure-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    let mut db = Database::create_with_options(&path, false).unwrap();
    for name in ["a", "b", "c"] {
        db.define_variable(VariableDefinition {
            name: name.into(),
            var_type: VariableType::Categorical {
                categories: vec!["0".into(), "1".into()],
                allow_unknown: false,
            },
        })
        .unwrap();
    }
    for name in ["time", "region"] {
        db.define_dimension(DimensionDefinition {
            name: name.into(),
            source_column: name.into(),
            granularity: None,
        })
        .unwrap();
    }
    (db, path)
}

fn row(time: &str, region: &str, a: usize, b: usize, c: usize) -> MappedRow {
    MappedRow {
        variable_values: [("a", a), ("b", b), ("c", c)]
            .into_iter()
            .map(|(k, v)| (k.into(), Value::from(v.to_string())))
            .collect(),
        dimension_values: [
            ("time".into(), time.into()),
            ("region".into(), region.into()),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>(),
    }
}

fn assert_exports(db: &Database, engine: &QueryEngine, statement: &str) {
    let expected = sql::query(db, engine, statement).unwrap();
    let json = sql::query(db, engine, &format!("EXPORT {statement} AS JSON")).unwrap();
    let decoded: Vec<HashMap<String, String>> = serde_json::from_str(&json.rows[0][0]).unwrap();
    assert_eq!(decoded.len(), expected.rows.len());
    for (actual, row) in decoded.iter().zip(&expected.rows) {
        for (column, value) in expected.header.iter().zip(row) {
            assert_eq!(&actual[column], value);
        }
    }
    let csv = sql::query(db, engine, &format!("EXPORT {statement} AS CSV")).unwrap();
    let mut reader = csv::Reader::from_reader(csv.rows[0][0].as_bytes());
    assert_eq!(
        reader.headers().unwrap().iter().collect::<Vec<_>>(),
        expected.header
    );
    let rows = reader
        .records()
        .map(|r| r.unwrap().iter().map(str::to_owned).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(rows, expected.rows);
}

#[test]
fn reversed_joint_and_partial_slice_preserve_unknown_forest() {
    let (mut db, path) = database("forest");
    db.define_joint("b", "a").unwrap();
    let rows = vec![
        row("before", "east", 0, 0, 0),
        row("before", "west", 1, 1, 1),
    ];
    let schema = db.schema().clone();
    apply_batch(&mut db, &schema, &rows).unwrap();
    let engine = QueryEngine::default();
    let result = engine.structure(&db, "time:before").unwrap();
    assert_eq!(result.components, 2);
    assert_eq!(result.edges.len(), 1);
    assert_eq!(
        (&*result.edges[0].var_a, &*result.edges[0].var_b),
        ("a", "b")
    );
    assert_eq!(result.edges[0].sample_count, 2);
    assert!((result.retained_information - 1.0).abs() < 1e-12);
    assert_eq!(
        result.unknown_pairs,
        vec![("a".into(), "c".into()), ("b".into(), "c".into())]
    );
    assert_exports(&db, &engine, "STRUCTURE AT time:before");
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn unchanged_marginals_can_rewire_and_both_verbs_export_losslessly() {
    let (mut db, path) = database("rewire");
    for (a, b) in [("b", "a"), ("c", "a"), ("c", "b")] {
        db.define_joint(a, b).unwrap();
    }
    let mut rows = Vec::new();
    for a in 0..2 {
        for b in 0..2 {
            rows.push(row("before", "east", a, b, b));
            rows.push(row("after", "east", a, b, a));
        }
    }
    let schema = db.schema().clone();
    apply_batch(&mut db, &schema, &rows).unwrap();
    let engine = QueryEngine::default();
    for variable in ["a", "b", "c"] {
        assert!(
            engine
                .compare(&db, "time:before", "time:after", Some(variable))
                .unwrap()
                .jsd
                .abs()
                < 1e-12
        );
    }
    let diff = engine
        .compare_structure(&db, "time:before", "time:after")
        .unwrap();
    assert_eq!(diff.added_edges.len(), 1);
    assert_eq!(
        (&*diff.added_edges[0].var_a, &*diff.added_edges[0].var_b),
        ("a", "c")
    );
    assert_eq!(diff.dropped_edges.len(), 1);
    assert_eq!(
        (&*diff.dropped_edges[0].var_a, &*diff.dropped_edges[0].var_b),
        ("b", "c")
    );
    assert!((diff.rewiring_score - 1.0).abs() < 1e-12);
    assert!(diff.retained_information_delta.abs() < 1e-12);
    assert_exports(&db, &engine, "STRUCTURE AT time:before");
    assert_exports(
        &db,
        &engine,
        "COMPARE STRUCTURE BETWEEN time:before AND time:after",
    );
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}
