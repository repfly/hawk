use std::collections::BTreeMap;
use std::path::Path;

use hawk_engine::core::{DimensionDefinition, VariableDefinition, VariableType};
use hawk_engine::ingest::{IngestMapping, IngestOptions, IngestReport, IngestionPipeline};
use hawk_engine::query::QueryEngine;
use hawk_engine::sql;
use hawk_engine::storage::Database;

fn ingest(db: &mut Database, path: &Path, mapping: &IngestMapping) -> anyhow::Result<IngestReport> {
    IngestionPipeline::ingest_file(
        db,
        path,
        mapping,
        IngestOptions {
            // Multiple internal chunks still score against the model before this call.
            batch_size: 2,
            surprisal_report: true,
            ..IngestOptions::default()
        },
    )
}

fn show(label: &str, report: &IngestReport) {
    for score in &report.surprisal {
        println!(
            "{label}: {} @ {}: {:.3} excess bits/sample, {:.3} bits/sample, unseen mass {:.0}% ({} arriving / {} baseline samples)",
            score.variable,
            score.dimension_key,
            score.result.excess_bits,
            score.result.bits_per_sample,
            score.result.unseen_mass * 100.0,
            score.result.sample_count_a,
            score.result.sample_count_b,
        );
    }
}

fn main() -> anyhow::Result<()> {
    let root = std::env::temp_dir().join(format!("hawk-surprise-demo-{}", std::process::id()));
    std::fs::create_dir_all(&root)?;
    let db_path = root.join("db");
    let mut db = Database::create_with_options(&db_path, false)?;
    db.define_variable(VariableDefinition {
        name: "leaning".into(),
        var_type: VariableType::Categorical {
            categories: vec!["left".into(), "center".into(), "right".into()],
            allow_unknown: true,
        },
    })?;
    db.define_dimension(DimensionDefinition {
        name: "time".into(),
        source_column: "created_at".into(),
        granularity: Some("monthly".into()),
    })?;
    let mapping = IngestMapping {
        variables: [("political_leaning".into(), "leaning".into())].into(),
        dimensions: [("created_at".into(), "time".into())].into(),
    };

    // The existing news fixture has two months with the same leaning distribution.
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/community_notes_small.csv");
    let mut reader = csv::Reader::from_path(fixture)?;
    let headers = reader.headers()?.clone();
    let mut months: BTreeMap<String, Vec<csv::StringRecord>> = BTreeMap::new();
    for record in reader.records() {
        let record = record?;
        months
            .entry(record[3][..7].into())
            .or_default()
            .push(record);
    }
    let batch_path = root.join("batch.csv");
    for (month, rows) in &months {
        let mut writer = csv::Writer::from_path(&batch_path)?;
        writer.write_record(&headers)?;
        for row in rows {
            writer.write_record(row)?;
        }
        writer.flush()?;
        let initial = ingest(&mut db, &batch_path, &mapping)?;
        assert!(initial.surprisal.is_empty());
        println!(
            "{month}: initialized model with {} news rows",
            initial.processed_rows
        );

        // Replay as a controlled stable follow-up batch for this month's slice.
        // A brand-new month has no same-slice baseline and is intentionally skipped.
        let stable = ingest(&mut db, &batch_path, &mapping)?;
        assert_eq!(stable.surprisal.len(), 1);
        assert!(stable.surprisal[0].result.excess_bits.abs() < 1e-8);
        show("stable follow-up", &stable);
    }

    // Inject a broken category into the latest month; it folds into __unknown__.
    let latest = months.keys().next_back().expect("fixture contains months");
    let mut writer = csv::Writer::from_path(&batch_path)?;
    writer.write_record(&headers)?;
    for _ in 0..12 {
        writer.write_record(["0", "CORRUPTED", "climate-change", &format!("{latest}-25")])?;
    }
    writer.flush()?;
    let corrupt = ingest(&mut db, &batch_path, &mapping)?;
    assert_eq!(corrupt.surprisal.len(), 1);
    assert!(corrupt.surprisal[0].result.excess_bits > 10.0);
    assert_eq!(corrupt.surprisal[0].result.unseen_mass, 1.0);
    show("corrupted batch", &corrupt);

    let first = months.keys().next().expect("fixture contains months");
    let engine = QueryEngine::default();
    println!(
        "{}",
        sql::query(
            &db,
            &engine,
            &format!("SURPRISE time:{latest} UNDER time:{first} ON leaning")
        )?
    );
    println!(
        "{}",
        sql::query(
            &db,
            &engine,
            &format!("ALERT WHEN surprisal > 0.5 ON leaning FROM time:{first}")
        )?
    );
    drop(db);
    std::fs::remove_dir_all(root)?;
    Ok(())
}
