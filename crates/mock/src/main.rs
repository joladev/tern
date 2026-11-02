mod fixture;
mod server;

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Serve {
        #[arg(long)]
        fixture: PathBuf,
        #[arg(long)]
        backfill_fixture: Option<PathBuf>,
        #[arg(long, default_value = "0.0.0.0:3001")]
        bind: SocketAddr,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();
    match Cli::parse().command {
        Command::Serve {
            fixture,
            backfill_fixture,
            bind,
        } => server::run(&fixture, backfill_fixture.as_deref(), bind).await,
    }
}
