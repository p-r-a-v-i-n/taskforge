use clap::Parser;
use taskforge_worker::{run_worker, WorkerConfig};

#[derive(Parser, Debug)]
#[command(name = "taskforge-worker")]
#[command(about = "Taskforge worker (Redis Streams)", long_about = None)]
struct Cli {
    #[arg(long)]
    broker_url: String,
    #[arg(long, default_value = "taskforge.tasks")]
    stream: String,
    #[arg(long, default_value = "$")]
    last_id: String,
    #[arg(long, default_value = "5000")]
    block_ms: u64,
    #[arg(long, default_value = "10")]
    prefetch: u64,
    #[arg(long, default_value = "8")]
    concurrency: usize,
    #[arg(long, default_value = "taskforge:result:")]
    result_prefix: String,
    #[arg(long, default_value = "86400")]
    result_ttl_seconds: u64,
}

impl From<Cli> for WorkerConfig {
    fn from(cli: Cli) -> Self {
        WorkerConfig {
            broker_url: cli.broker_url,
            stream: cli.stream,
            last_id: cli.last_id,
            block_ms: cli.block_ms,
            prefetch: cli.prefetch,
            concurrency: cli.concurrency,
            result_prefix: cli.result_prefix,
            result_ttl_seconds: cli.result_ttl_seconds,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    run_worker(cli.into()).await
}
