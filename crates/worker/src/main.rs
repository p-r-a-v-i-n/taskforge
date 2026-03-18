use anyhow::Context;
use chrono::Utc;
use clap::Parser;
use futures::future::BoxFuture;
use futures::FutureExt;
use redis::AsyncCommands;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use taskforge_core::{TaskResult, TaskSpec, TaskStatus};
use tokio::sync::Semaphore;

#[derive(Parser, Debug, Clone)]
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let client = redis::Client::open(cli.broker_url.clone())?;
    let mut conn = redis::aio::ConnectionManager::new(client)
        .await
        .context("Failed to create Redis connection manager")?;
    let mut last_id = cli.last_id.clone();

    let registry = build_registry();
    let semaphore = Arc::new(Semaphore::new(cli.concurrency));

    loop {
        let reply: redis::Value = redis::cmd("XREAD")
            .arg("BLOCK")
            .arg(cli.block_ms)
            .arg("COUNT")
            .arg(cli.prefetch)
            .arg("STREAMS")
            .arg(&cli.stream)
            .arg(&last_id)
            .query_async(&mut conn)
            .await?;

        for (id, payload) in extract_payloads(reply) {
            last_id = id;
            let task: TaskSpec = match serde_json::from_str(&payload) {
                Ok(task) => task,
                Err(err) => {
                    eprintln!("Invalid task payload JSON: {}", err);
                    continue;
                }
            };

            let permit = semaphore.clone().acquire_owned().await?;
            let cli_clone = cli.clone();
            let registry_clone = registry.clone();
            let mut task_conn = conn.clone();

            tokio::spawn(async move {
                let _permit = permit;
                if let Err(err) =
                    handle_task(task, &cli_clone, &registry_clone, &mut task_conn).await
                {
                    eprintln!("Task handling error: {}", err);
                }
            });
        }
    }
}

#[derive(Debug)]
enum TaskFailure {
    UnknownTask(String),
    Execution(String),
    Timeout(u64),
}

type TaskHandler =
    Arc<dyn Fn(TaskSpec) -> BoxFuture<'static, Result<serde_json::Value, String>> + Send + Sync>;

async fn handle_task(
    task: TaskSpec,
    cli: &Cli,
    registry: &HashMap<String, TaskHandler>,
    conn: &mut redis::aio::ConnectionManager,
) -> anyhow::Result<()> {
    let result_key = format!("{}{}", cli.result_prefix, task.id);
    let running = TaskResult {
        id: task.id,
        status: TaskStatus::Running,
        started_at: Some(Utc::now()),
        finished_at: None,
        output: None,
        error: None,
    };
    write_result(conn, &result_key, &running, cli.result_ttl_seconds).await?;

    println!("Received task {} name={}", task.id, task.name);

    let execution = execute_task_with_timeout(&task, registry).await;
    let finished_at = Some(Utc::now());

    match execution {
        Ok(output) => {
            let result = TaskResult {
                id: task.id,
                status: TaskStatus::Succeeded,
                started_at: running.started_at,
                finished_at,
                output: Some(output),
                error: None,
            };
            write_result(conn, &result_key, &result, cli.result_ttl_seconds).await?;
        }
        Err(TaskFailure::UnknownTask(name)) => {
            let result = TaskResult {
                id: task.id,
                status: TaskStatus::Failed,
                started_at: running.started_at,
                finished_at,
                output: None,
                error: Some(format!("Unknown task: {}", name)),
            };
            write_result(conn, &result_key, &result, cli.result_ttl_seconds).await?;
        }
        Err(TaskFailure::Execution(error)) => {
            handle_retry(
                &task,
                cli,
                conn,
                TaskFailure::Execution(error),
                running.started_at,
                finished_at,
            )
            .await?;
        }
        Err(TaskFailure::Timeout(secs)) => {
            handle_retry(
                &task,
                cli,
                conn,
                TaskFailure::Timeout(secs),
                running.started_at,
                finished_at,
            )
            .await?;
        }
    }

    Ok(())
}

async fn handle_retry(
    task: &TaskSpec,
    cli: &Cli,
    conn: &mut redis::aio::ConnectionManager,
    failure: TaskFailure,
    started_at: Option<chrono::DateTime<Utc>>,
    finished_at: Option<chrono::DateTime<Utc>>,
) -> anyhow::Result<()> {
    let should_retry = task.retry.attempt < task.retry.max_attempts;
    let error = match failure {
        TaskFailure::Execution(msg) => msg,
        TaskFailure::Timeout(secs) => format!("Task timed out after {}s", secs),
        TaskFailure::UnknownTask(name) => format!("Unknown task: {}", name),
    };

    let result_key = format!("{}{}", cli.result_prefix, task.id);

    if should_retry {
        let result = TaskResult {
            id: task.id,
            status: TaskStatus::Retrying,
            started_at,
            finished_at,
            output: None,
            error: Some(error.clone()),
        };
        write_result(conn, &result_key, &result, cli.result_ttl_seconds).await?;

        let backoff = task.retry.backoff_seconds * (task.retry.attempt as u64 + 1);
        if backoff > 0 {
            tokio::time::sleep(Duration::from_secs(backoff)).await;
        }

        let mut retry_task = task.clone();
        retry_task.retry.attempt += 1;
        retry_task.created_at = Utc::now();
        retry_task.eta = None;
        let retry_payload = serde_json::to_string(&retry_task)?;

        let _id: String = redis::cmd("XADD")
            .arg(&cli.stream)
            .arg("*")
            .arg("payload")
            .arg(retry_payload)
            .query_async(conn)
            .await?;
    } else {
        let result = TaskResult {
            id: task.id,
            status: TaskStatus::Failed,
            started_at,
            finished_at,
            output: None,
            error: Some(error),
        };
        write_result(conn, &result_key, &result, cli.result_ttl_seconds).await?;
    }

    Ok(())
}

async fn write_result(
    conn: &mut redis::aio::ConnectionManager,
    key: &str,
    result: &TaskResult,
    ttl_seconds: u64,
) -> anyhow::Result<()> {
    let result_payload = serde_json::to_string(result)?;
    let _: String = conn.set(key, result_payload).await?;
    if ttl_seconds > 0 {
        let _: i32 = conn.expire(key, ttl_seconds as i64).await?;
    }
    Ok(())
}

async fn execute_task(
    task: &TaskSpec,
    registry: &HashMap<String, TaskHandler>,
) -> Result<serde_json::Value, TaskFailure> {
    match registry.get(&task.name) {
        Some(handler) => handler(task.clone())
            .await
            .map_err(TaskFailure::Execution),
        None => Err(TaskFailure::UnknownTask(task.name.clone())),
    }
}

async fn execute_task_with_timeout(
    task: &TaskSpec,
    registry: &HashMap<String, TaskHandler>,
) -> Result<serde_json::Value, TaskFailure> {
    match task.timeout_seconds {
        Some(timeout) => {
            match tokio::time::timeout(Duration::from_secs(timeout), execute_task(task, registry))
                .await
            {
                Ok(result) => result,
                Err(_) => Err(TaskFailure::Timeout(timeout)),
            }
        }
        None => execute_task(task, registry).await,
    }
}

fn build_registry() -> HashMap<String, TaskHandler> {
    let mut registry: HashMap<String, TaskHandler> = HashMap::new();
    registry.insert(
        "echo".to_string(),
        Arc::new(|task| async move { task_echo(task).await }.boxed()),
    );
    registry.insert(
        "add".to_string(),
        Arc::new(|task| async move { task_add(task).await }.boxed()),
    );
    registry.insert(
        "sleep".to_string(),
        Arc::new(|task| async move { task_sleep(task).await }.boxed()),
    );
    registry
}

async fn task_echo(task: TaskSpec) -> Result<serde_json::Value, String> {
    Ok(json!({
        "args": task.args,
        "kwargs": task.kwargs,
    }))
}

async fn task_add(task: TaskSpec) -> Result<serde_json::Value, String> {
    let args = task
        .args
        .as_array()
        .ok_or_else(|| "args must be an array".to_string())?;
    if args.len() < 2 {
        return Err("add requires at least 2 numeric args".to_string());
    }
    let mut sum = 0.0f64;
    for value in args {
        let number = value
            .as_f64()
            .ok_or_else(|| "add args must be numbers".to_string())?;
        sum += number;
    }
    Ok(json!({ "sum": sum }))
}

async fn task_sleep(task: TaskSpec) -> Result<serde_json::Value, String> {
    let kwargs = task
        .kwargs
        .as_object()
        .ok_or_else(|| "kwargs must be an object".to_string())?;
    let seconds = kwargs
        .get("seconds")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| "sleep requires kwargs.seconds (u64)".to_string())?;
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    Ok(json!({ "slept_seconds": seconds }))
}

fn extract_payloads(reply: redis::Value) -> Vec<(String, String)> {
    let streams = match reply {
        redis::Value::Bulk(items) => items,
        _ => return Vec::new(),
    };

    let first_stream = match streams.get(0) {
        Some(stream) => stream,
        None => return Vec::new(),
    };
    let stream_items = match first_stream {
        redis::Value::Bulk(items) => items,
        _ => return Vec::new(),
    };

    if stream_items.len() < 2 {
        return Vec::new();
    }

    let entries = match &stream_items[1] {
        redis::Value::Bulk(items) => items,
        _ => return Vec::new(),
    };

    let mut results = Vec::new();
    for entry in entries {
        let entry_items = match entry {
            redis::Value::Bulk(items) => items,
            _ => continue,
        };
        if entry_items.len() < 2 {
            continue;
        }
        let id = match &entry_items[0] {
            redis::Value::Data(bytes) => String::from_utf8_lossy(bytes).to_string(),
            _ => continue,
        };
        let fields = match &entry_items[1] {
            redis::Value::Bulk(items) => items,
            _ => continue,
        };
        let mut i = 0;
        while i + 1 < fields.len() {
            let key = match &fields[i] {
                redis::Value::Data(bytes) => String::from_utf8_lossy(bytes).to_string(),
                _ => break,
            };
            let value = match &fields[i + 1] {
                redis::Value::Data(bytes) => String::from_utf8_lossy(bytes).to_string(),
                _ => break,
            };
            if key == "payload" {
                results.push((id.clone(), value));
                break;
            }
            i += 2;
        }
    }

    results
}
