use std::fs;
use std::path::Path;

use hawk_engine::ingest::{InferConfig, IngestOptions, IngestionPipeline};
use hawk_engine::storage::Database;

fn dir_size(path: &Path) -> std::io::Result<u64> {
    let mut total = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total += dir_size(&entry.path())?;
        } else {
            total += metadata.len();
        }
    }
    Ok(total)
}

fn main() -> anyhow::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 3 {
        eprintln!("Usage: benchmark_snapshot <input.csv> <db_dir>");
        std::process::exit(2);
    }

    let input = Path::new(&args[1]);
    let db_dir = Path::new(&args[2]);
    if db_dir.exists() {
        fs::remove_dir_all(db_dir)?;
    }

    let raw_bytes = fs::metadata(input)?.len();
    let mut db = Database::create_with_options(db_dir, false)?;
    let report = IngestionPipeline::ingest_file_auto(
        &mut db,
        input,
        InferConfig::default(),
        IngestOptions::default(),
    )?;
    db.flush()?;

    let db_bytes = dir_size(db_dir)?;
    let ratio = if db_bytes > 0 {
        raw_bytes as f64 / db_bytes as f64
    } else {
        0.0
    };

    println!("raw_input_bytes,hawk_db_bytes,compression_ratio,processed_rows,ingest_ms");
    println!(
        "{raw_bytes},{db_bytes},{ratio:.2},{},{}",
        report.processed_rows, report.elapsed_ms
    );

    Ok(())
}
