use std::io::{self, BufRead, Write};

use hawk_query::QueryEngine;
use hawk_storage::{Database, OpenMode};

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: hawk-sql <database_path> [query]");
        eprintln!();
        eprintln!("Interactive mode:  hawk-sql ./my_db");
        eprintln!("One-shot mode:     hawk-sql ./my_db \"COMPARE category BETWEEN time:2013 AND time:2022\"");
        eprintln!();
        eprintln!("Available queries:");
        eprintln!("  COMPARE <var> BETWEEN <dim:val> AND <dim:val>");
        eprintln!("  EXPLAIN <dim:val> VS <dim:val>");
        eprintln!("  TRACK <var> FROM <dim:val> [GRANULARITY <g>]");
        eprintln!("  SHOW <var> AT <dim:val>");
        eprintln!("  RANK <var> BY ENTROPY OVER <dim>");
        eprintln!("  MI <var_a>, <var_b> AT <dim:val>");
        eprintln!("  CMI <var_a>, <var_b> GIVEN <dim>");
        eprintln!("  CORRELATIONS [OVER <dim>] [LIMIT <n>]");
        eprintln!("  PAIRWISE <dim> ON <var> [USING jsd|hellinger|psi]");
        eprintln!("  NEAREST <dim:val> ON <dim> [LIMIT <n>] [USING jsd|hellinger|psi]");
        eprintln!("  STATS");
        eprintln!("  SCHEMA");
        eprintln!("  DIMENSIONS [<name>]");
        std::process::exit(1);
    }

    let db_path = &args[1];
    let db = match Database::open(db_path, OpenMode::ReadOnly) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Error opening database '{}': {}", db_path, e);
            std::process::exit(1);
        }
    };
    let engine = QueryEngine::default();

    // One-shot mode
    if args.len() > 2 {
        let query = args[2..].join(" ");
        match hawk_sql::query(&db, &engine, &query) {
            Ok(result) => print!("{}", result),
            Err(e) => {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        return;
    }

    // Interactive REPL
    println!("Hawk — interactive mode");
    println!("Database: {}", db_path);
    println!("Type a query or 'quit' to exit.\n");

    let stdin = io::stdin();
    let mut stdout = io::stdout();

    loop {
        print!("hawk> ");
        stdout.flush().unwrap();

        let mut line = String::new();
        if stdin.lock().read_line(&mut line).unwrap() == 0 {
            break; // EOF
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.eq_ignore_ascii_case("quit") || trimmed.eq_ignore_ascii_case("exit") {
            break;
        }

        match hawk_sql::query(&db, &engine, trimmed) {
            Ok(result) => println!("{}", result),
            Err(e) => eprintln!("Error: {}\n", e),
        }
    }
}
