use chrono::Utc;
use clap::{Parser, Subcommand};
use redis::Commands;
use rustly_dispatch_core::{RetryPolicy, TaskResult, TaskSpec, TaskStatus};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "rustly-dispatch-control")]
#[command(about = "Control plane CLI for Rustly Dispatch", long_about = None)]
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
        timeout_seconds: Option<u64>,
        #[arg(long)]
        broker_url: String,
        #[arg(long, default_value = "rustly-dispatch.tasks")]
        stream: String,
        #[arg(long, default_value = "rustly-dispatch:result:")]
        result_prefix: String,
        #[arg(long, default_value = "86400")]
        result_ttl_seconds: u64,
    },
    Result {
        #[arg(long)]
        id: String,
        #[arg(long)]
        broker_url: String,
        #[arg(long, default_value = "rustly-dispatch:result:")]
        result_prefix: String,
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
            timeout_seconds,
            broker_url,
            stream,
            result_prefix,
            result_ttl_seconds,
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
                timeout_seconds,
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

            let result = TaskResult {
                id: task.id,
                status: TaskStatus::Queued,
                started_at: None,
                finished_at: None,
                output: None,
                error: None,
            };
            let result_key = format!("{}{}", result_prefix, task.id);
            let result_payload = serde_json::to_string(&result)?;
            let _: String = conn.set(&result_key, result_payload)?;
            if result_ttl_seconds > 0 {
                let _: i32 = conn.expire(&result_key, result_ttl_seconds as i64)?;
            }

            println!("Enqueued task {} to stream {}", task.id, stream);
        }
        CommandsCli::Result {
            id,
            broker_url,
            result_prefix,
        } => {
            let mut conn = redis::Client::open(broker_url)?.get_connection()?;
            let key = format!("{}{}", result_prefix, id);
            let payload: Option<String> = conn.get(key)?;
            match payload {
                Some(value) => {
                    let result: TaskResult = serde_json::from_str(&value)?;
                    println!("{}", serde_json::to_string_pretty(&result)?);
                }
                None => {
                    println!("Result not found");
                }
            }
        }
    }

    Ok(())
}
