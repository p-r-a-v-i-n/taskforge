use chrono::Utc;
use clap::{Parser, Subcommand};
use taskforge_core::{RetryPolicy, TaskSpec};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "taskforge-control")]
#[command(about = "Control plane CLI for taskforge", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: CommandsCli,
}

#[derive(Subcommand, Debug)]
enum CommandsCli {
    Enqueue {
        #[arg(long)]
        name: String,
        #[arg(long, default_value = "default")]
        queue: String,
        #[arg(long, default_value = "[]")]
        args: String,
        #[arg(long, default_value = "{}")]
        kwargs: String,
        #[arg(long, default_value = "3")]
        max_attempts: u32,
        #[arg(long, default_value = "0")]
        attempt: u32,
        #[arg(long, default_value = "5")]
        backoff_seconds: u64,
        #[arg(long)]
        broker_url: String,
        #[arg(long, default_value = "taskforge.tasks")]
        stream: String,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        CommandsCli::Enqueue {
            name,
            queue,
            args,
            kwargs,
            max_attempts,
            attempt,
            backoff_seconds,
            broker_url,
            stream,
        } => {
            let args_value: serde_json::Value = serde_json::from_str(&args)
                .map_err(|e| anyhow::anyhow!("Invalid --args JSON: {e}"))?;
            if !args_value.is_array() {
                return Err(anyhow::anyhow!("--args must be a JSON array"));
            }

            let kwargs_value: serde_json::Value = serde_json::from_str(&kwargs)
                .map_err(|e| anyhow::anyhow!("Invalid --kwargs JSON: {e}"))?;
            if !kwargs_value.is_object() {
                return Err(anyhow::anyhow!("--kwargs must be a JSON object"));
            }

            let task = TaskSpec {
                id: Uuid::new_v4(),
                name,
                args: args_value,
                kwargs: kwargs_value,
                queue,
                retry: RetryPolicy {
                    max_attempts,
                    attempt,
                    backoff_seconds,
                },
                eta: None,
                timeout_seconds: None,
                idempotency_key: None,
                created_at: Utc::now(),
            };

            let payload = serde_json::to_string(&task)?;
            let mut conn = redis::Client::open(broker_url)?.get_connection()?;

            let _id: String = redis::cmd("XADD")
                .arg(&stream)
                .arg("*")
                .arg("payload")
                .arg(payload)
                .query(&mut conn)?;

            println!("Enqueued task {} to stream {}", task.id, stream);
        }
    }

    Ok(())
}
