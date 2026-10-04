use hawk_engine::core::{DimensionDefinition, VariableDefinition, VariableType};
use hawk_engine::ingest::{IngestMapping, IngestOptions, IngestionPipeline};
use hawk_engine::query::QueryEngine;
use hawk_engine::storage::Database;

#[test]
fn scores_arriving_counts_against_pre_call_model_and_exports_alerts() {
    let root =
        std::env::temp_dir().join(format!("hawk-surprise-regression-{}", std::process::id()));
    if root.exists() {
        std::fs::remove_dir_all(&root).unwrap();
    }
    std::fs::create_dir_all(&root).unwrap();
    let mut db = Database::create_with_options(root.join("db"), false).unwrap();
    db.define_variable(VariableDefinition {
        name: "category".into(),
        var_type: VariableType::Categorical {
            categories: vec!["normal".into(), "rare".into()],
            allow_unknown: true,
        },
    })
    .unwrap();
    db.define_dimension(DimensionDefinition {
        name: "time".into(),
        source_column: "time".into(),
        granularity: None,
    })
    .unwrap();
    let mapping = IngestMapping {
        variables: [("category".into(), "category".into())].into(),
        dimensions: [("time".into(), "time".into())].into(),
    };
    let path = root.join("batch.csv");
    let options = || IngestOptions {
        batch_size: 1,
        surprisal_report: true,
        ..IngestOptions::default()
    };
    std::fs::write(
        &path,
        "category,time\nnormal,2025-01\nnormal,2025-01\nnormal,2025-01\nrare,2025-01\n",
    )
    .unwrap();
    let first = IngestionPipeline::ingest_file(&mut db, &path, &mapping, options()).unwrap();
    assert!(first.surprisal.is_empty());
    let stable = IngestionPipeline::ingest_file(&mut db, &path, &mapping, options()).unwrap();
    let s = &stable.surprisal[0].result;
    assert_eq!((s.sample_count_a, s.sample_count_b), (4, 4));
    assert!(s.excess_bits.abs() < 1e-8);

    // Unknown categories remain represented, and all chunks use the original baseline.
    std::fs::write(
        &path,
        "category,time\nbroken,2025-01\nbroken,2025-01\nbroken,2025-01\n",
    )
    .unwrap();
    let corrupt = IngestionPipeline::ingest_file(&mut db, &path, &mapping, options()).unwrap();
    assert_eq!(corrupt.surprisal.len(), 1);
    let s = &corrupt.surprisal[0].result;
    assert_eq!((s.sample_count_a, s.sample_count_b), (3, 8));
    assert_eq!(s.unseen_mass, 1.0);
    assert!(s.unseen_mass_warning.is_some());
    let expected = -((1e-10_f64) / (8.0 + 3.0e-10)).log2();
    assert!((s.bits_per_sample - expected).abs() < 1e-9);
    assert!((s.total_bits - 3.0 * expected).abs() < 1e-9);
    assert!(
        (s.top_contributors.iter().map(|c| c.bits).sum::<f64>() - s.bits_per_sample).abs() < 1e-9
    );
    assert!(
        (s.top_contributors
            .iter()
            .map(|c| c.excess_bits)
            .sum::<f64>()
            - s.excess_bits)
            .abs()
            < 1e-9
    );

    // A fresh month is skipped by ingest scoring, but query/alert can compare it.
    std::fs::write(&path, "category,time\nrare,2025-02\nrare,2025-02\n").unwrap();
    let new_month = IngestionPipeline::ingest_file(&mut db, &path, &mapping, options()).unwrap();
    assert!(new_month.surprisal.is_empty());
    let engine = QueryEngine::default();
    let query = "SURPRISE time:2025-02 UNDER time:2025-01 ON category";
    let result = engine
        .surprise(&db, "time:2025-02", "time:2025-01", Some("category"))
        .unwrap();
    let json = hawk_engine::sql::query(&db, &engine, &format!("EXPORT {query} AS JSON")).unwrap();
    let values: serde_json::Value = serde_json::from_str(&json.rows[0][0]).unwrap();
    assert!(values
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v["Metric"] == "Total Bits"));
    let csv = hawk_engine::sql::query(&db, &engine, &format!("EXPORT {query} AS CSV")).unwrap();
    let mut reader = csv::Reader::from_reader(csv.rows[0][0].as_bytes());
    assert_eq!(
        reader.headers().unwrap().iter().collect::<Vec<_>>(),
        ["Metric", "Value"]
    );
    assert!(reader
        .records()
        .any(|r| r.unwrap().get(0) == Some("Total Bits")));
    for (threshold, should_trigger) in [
        (result[0].excess_bits - 0.01, true),
        (result[0].excess_bits + 0.01, false),
    ] {
        let alert = hawk_engine::sql::query(
            &db,
            &engine,
            &format!("ALERT WHEN surprisal > {threshold} ON category FROM time:2025-01"),
        )
        .unwrap();
        assert_eq!(
            alert.to_string().contains("No alerts triggered"),
            !should_trigger
        );
    }
    let off =
        IngestionPipeline::ingest_file(&mut db, &path, &mapping, IngestOptions::default()).unwrap();
    assert!(off.surprisal.is_empty());
    drop(db);
    std::fs::remove_dir_all(root).unwrap();
}
