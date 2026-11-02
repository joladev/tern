mod backfill;
mod http;
mod ingest;
mod lookup;
mod refresh;
mod score;
mod telemetry;
mod writer;

use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use clap::{Args, Parser, Subcommand};
use jiff::SignedDuration;
use store::Store;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use backfill::Backfill;
use ingest::Ingest;
use lookup::Lookups;
use refresh::Refresh;
use score::Reach;
use telemetry::Exporter;
use writer::{Message, Writer, record_backfill};

#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const WRITER_QUEUE_SIZE: usize = 1024;
const JETSTREAM_QUEUE_SIZE: usize = 10_000;
const LOOKUP_QUEUE_SIZE: usize = 64;
const COMMIT_INTERVAL: Duration = Duration::from_secs(1);
const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Parser)]
#[command(about)]
struct Cli {
    #[arg(long, env = "TERN_DATA_DIR", default_value = "/data")]
    data_dir: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Serve(Box<ServeArgs>),
    Rebuild,
    Health {
        #[arg(long, env = "TERN_BIND", default_value = "0.0.0.0:3000")]
        bind: SocketAddr,
    },
}

#[derive(Args)]
struct ServeArgs {
    #[arg(long, env = "TERN_BIND", default_value = "0.0.0.0:3000")]
    bind: SocketAddr,
    #[arg(long, env = "TERN_METRICS_BIND", default_value = "0.0.0.0:9000")]
    metrics_bind: SocketAddr,
    #[arg(
        long,
        env = "TERN_JETSTREAM_URL",
        default_value = "wss://jetstream.us-east.bsky.network"
    )]
    jetstream_url: String,
    #[arg(
        long,
        env = "TERN_SLINGSHOT_URL",
        default_value = "https://slingshot.cove.town"
    )]
    slingshot_url: String,
    #[arg(
        long,
        env = "TERN_USER_AGENT",
        default_value = "tern (https://tangled.org/jola.dev/tern)"
    )]
    user_agent: String,
    #[arg(long, env = "TERN_BACKFILL")]
    backfill: bool,
    #[arg(
        long,
        env = "TERN_HUBBLE_URL",
        default_value = "https://hubble.microcosm.blue"
    )]
    hubble_url: String,
    #[arg(
        long,
        env = "TERN_APPVIEW_URL",
        default_value = "https://public.api.bsky.app"
    )]
    appview_url: String,
    #[arg(
        long,
        env = "TERN_APPVIEW_CONCURRENCY",
        default_value_t = 16,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    appview_concurrency: u64,
    #[arg(long, env = "TERN_REFRESH_AFTER", default_value = "168h")]
    refresh_after: SignedDuration,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Serve(args) => serve(&cli.data_dir, *args).await,
        Command::Rebuild => rebuild(&cli.data_dir),
        Command::Health { bind } => health(bind).await,
    }
}

async fn serve(data_dir: &Path, args: ServeArgs) -> anyhow::Result<()> {
    let store = open_store(data_dir)?;
    let (reader, index) = open_index(data_dir)?;
    let cursor = index.cursor();
    tracing::info!(?cursor, "starting");

    let exporter = Arc::new(Exporter::install()?);

    let http_client = reqwest::Client::builder()
        .user_agent(&args.user_agent)
        .connect_timeout(HTTP_CONNECT_TIMEOUT)
        .timeout(HTTP_TIMEOUT)
        .build()
        .context("failed to create the HTTP client")?;
    let (writer_queue, inbox) = mpsc::channel(WRITER_QUEUE_SIZE);
    let writer = Writer::new(Arc::clone(&store), index, Box::new(Reach));
    let writer_task = tokio::task::spawn_blocking(move || writer.run(inbox));

    let slingshot = slingshot::Client::new(http_client.clone(), args.slingshot_url);
    let appview = appview::Client::new(http_client.clone(), args.appview_url);

    let (lookup_queue, lookups) = mpsc::channel(LOOKUP_QUEUE_SIZE);
    let ingest = Ingest {
        subscription: jetstream::subscribe(args.jetstream_url, cursor, JETSTREAM_QUEUE_SIZE),
        resolver: slingshot,
        store: Arc::clone(&store),
        writer: writer_queue.clone(),
        lookups: lookup_queue.clone(),
    };
    let consumer = Lookups {
        appview,
        queue: lookups,
        writer: writer_queue.clone(),
        concurrency: args.appview_concurrency as usize,
    };

    let listener = tokio::net::TcpListener::bind(args.bind)
        .await
        .with_context(|| format!("failed to bind {}", args.bind))?;
    let metrics_listener = tokio::net::TcpListener::bind(args.metrics_bind)
        .await
        .with_context(|| format!("failed to bind {}", args.metrics_bind))?;
    tracing::info!(
        bind = %args.bind,
        metrics_bind = %args.metrics_bind,
        "listening"
    );

    let cancel = CancellationToken::new();
    let metrics_server = axum::serve(metrics_listener, http::metrics_router(exporter))
        .with_graceful_shutdown(cancel.clone().cancelled_owned());
    let shutdown = cancel.clone().cancelled_owned();
    let server = axum::serve(listener, http::router(reader)).with_graceful_shutdown(shutdown);

    let mut tasks = JoinSet::new();
    spawn_until_cancelled(&mut tasks, &cancel, ingest.run());
    let backfill_state = store
        .backfill_state()
        .context("failed to read the backfill state")?;
    record_backfill(&backfill_state);
    if args.backfill && !backfill_state.progress.finished {
        let backfill = Backfill {
            hubble: hubble::Client::new(http_client, args.hubble_url),
            store: Arc::clone(&store),
            queue: lookup_queue.clone(),
        };
        spawn_finite(&mut tasks, &cancel, backfill.run());
    }
    let refresh = Refresh {
        store: Arc::clone(&store),
        queue: lookup_queue,
        stale_after: args.refresh_after,
    };
    spawn_until_cancelled(&mut tasks, &cancel, refresh.run());
    spawn_until_cancelled(&mut tasks, &cancel, consumer.run());
    spawn_until_cancelled(&mut tasks, &cancel, tick(writer_queue));
    spawn_until_cancelled(&mut tasks, &cancel, async move {
        server.await.context("the HTTP server failed")
    });
    spawn_until_cancelled(&mut tasks, &cancel, async move {
        metrics_server.await.context("the metrics server failed")
    });

    tokio::select! {
        result = shutdown_signal() => result?,
        () = cancel.cancelled() => {}
    }
    cancel.cancel();

    let mut first_error = None;
    while let Some(result) = tasks.join_next().await {
        if let Err(err) = result.context("a task panicked")? {
            first_error.get_or_insert(err);
        }
    }
    writer_task.await.context("the writer panicked")??;

    first_error.map_or(Ok(()), Err)
}

fn rebuild(data_dir: &Path) -> anyhow::Result<()> {
    let store = open_store(data_dir)?;
    let (_reader, index) = open_index(data_dir)?;
    Writer::new(store, index, Box::new(Reach)).rebuild()
}

async fn health(bind: SocketAddr) -> anyhow::Result<()> {
    let url = format!("http://{}/xrpc/_health", reachable(bind));
    let response = reqwest::Client::new()
        .get(url)
        .timeout(HTTP_TIMEOUT)
        .send()
        .await?;
    response.error_for_status()?;
    Ok(())
}

fn reachable(bind: SocketAddr) -> SocketAddr {
    if !bind.ip().is_unspecified() {
        return bind;
    }
    let loopback = match bind {
        SocketAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
        SocketAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
    };
    SocketAddr::new(loopback, bind.port())
}

fn open_store(data_dir: &Path) -> anyhow::Result<Arc<Store>> {
    let store = Store::open(&data_dir.join("store")).context("failed to open the account store")?;
    Ok(Arc::new(store))
}

fn open_index(data_dir: &Path) -> anyhow::Result<(search::Reader, search::Writer)> {
    search::open(&data_dir.join("index")).context("failed to open the search index")
}

fn spawn_until_cancelled(
    tasks: &mut JoinSet<anyhow::Result<()>>,
    cancel: &CancellationToken,
    task: impl Future<Output = anyhow::Result<()>> + Send + 'static,
) {
    let cancel = cancel.clone();
    tasks.spawn(async move {
        let _stop_others = cancel.clone().drop_guard();
        cancel.run_until_cancelled(task).await.unwrap_or(Ok(()))
    });
}

fn spawn_finite(
    tasks: &mut JoinSet<anyhow::Result<()>>,
    cancel: &CancellationToken,
    task: impl Future<Output = anyhow::Result<()>> + Send + 'static,
) {
    let cancel = cancel.clone();
    tasks.spawn(async move {
        let result = cancel.run_until_cancelled(task).await.unwrap_or(Ok(()));
        if result.is_err() {
            cancel.cancel();
        }
        result
    });
}

async fn tick(writer: mpsc::Sender<Message>) -> anyhow::Result<()> {
    let mut interval = tokio::time::interval(COMMIT_INTERVAL);
    loop {
        interval.tick().await;
        writer
            .send(Message::Commit)
            .await
            .context("the writer stopped")?;
    }
}

async fn shutdown_signal() -> anyhow::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate()).context("failed to listen for SIGTERM")?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result.context("failed to listen for Ctrl-C")
        }
        _ = terminate.recv() => Ok(()),
    }
}
