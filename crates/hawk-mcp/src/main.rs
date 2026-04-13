use std::sync::Arc;

use clap::Parser;
use rmcp::ServiceExt;

mod help_text;
mod state;
mod tools;

#[derive(Parser)]
#[command(name = "hawk-mcp", about = "MCP server for Hawk distribution analytics")]
struct Cli {
    /// Path to a Hawk database to open at startup (optional)
    #[arg(long)]
    db: Option<String>,

    /// Open database in read-only mode
    #[arg(long)]
    readonly: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    let cli = Cli::parse();

    let (db, path) = if let Some(ref db_path) = cli.db {
        let mode = if cli.readonly {
            hawk_engine::storage::OpenMode::ReadOnly
        } else {
            hawk_engine::storage::OpenMode::ReadWrite
        };
        let db = hawk_engine::storage::Database::open(db_path, mode)?;
        (Some(db), Some(db_path.clone()))
    } else {
        (None, None)
    };

    let state = Arc::new(state::AppState::new(db, path));
    let server = tools::HawkMcp { state };

    let service = server.serve(rmcp::transport::stdio()).await?;
    service.waiting().await?;

    Ok(())
}
