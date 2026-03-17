use chrono::Utc;
use clap::Parser;
use redis::Commands;
use serde_json::json;
use std::collections::HashMap;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use taskforge_core::{TaskResult, TaskSpec, TaskStatus};

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
    #[arg(long, default_value = "taskforge:result:")]
    result_prefix: String,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let mut conn = redis::Client::open(cli.broker_url)?.get_connection()?;
    let mut last_id = cli.last_id;

    let registry = build_registry();

    loop {
        let reply: redis::Value = redis::cmd("XREAD")
            .arg("BLOCK")
            .arg(cli.block_ms)
            .arg("COUNT")
            .arg(1)
            .arg("STREAMS")
            .arg(&cli.stream)
            .arg(&last_id)
            .query(&mut conn)?;

        if let Some((id, payload)) = extract_payload(reply) {
            last_id = id.clone();
            let task: TaskSpec = serde_json::from_str(&payload)
                .map_err(|e| anyhow::anyhow!("Invalid task payload JSON: {e}"))?;
            let result_key = format!("{}{}", cli.result_prefix, task.id);
            let running = TaskResult {
                id: task.id,
                status: TaskStatus::Running,
                started_at: Some(Utc::now()),
                finished_at: None,
                output: None,
                error: None,
            };
            let running_payload = serde_json::to_string(&running)?;
            let _: String = conn.set(&result_key, running_payload)?;

            println!("Received task {} name={}", task.id, task.name);

            let execution = execute_task_with_timeout(&task, &registry);

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
                    let result_payload = serde_json::to_string(&result)?;
                    let _: String = conn.set(result_key, result_payload)?;
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
                    let result_payload = serde_json::to_string(&result)?;
                    let _: String = conn.set(result_key, result_payload)?;
                }
                Err(TaskFailure::Execution(error)) => {
                    let should_retry = task.retry.attempt < task.retry.max_attempts;
                    if should_retry {
                        let result = TaskResult {
                            id: task.id,
                            status: TaskStatus::Retrying,
                            started_at: running.started_at,
                            finished_at,
                            output: None,
                            error: Some(error.clone()),
                        };
                        let result_payload = serde_json::to_string(&result)?;
                        let _: String = conn.set(&result_key, result_payload)?;

                        let backoff = task.retry.backoff_seconds * (task.retry.attempt as u64 + 1);
                        if backoff > 0 {
                            thread::sleep(Duration::from_secs(backoff));
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
                            .query(&mut conn)?;
                    } else {
                        let result = TaskResult {
                            id: task.id,
                            status: TaskStatus::Failed,
                            started_at: running.started_at,
                            finished_at,
                            output: None,
                            error: Some(error),
                        };
                        let result_payload = serde_json::to_string(&result)?;
                        let _: String = conn.set(result_key, result_payload)?;
                    }
                }
                Err(TaskFailure::Timeout(secs)) => {
                    let error = format!("Task timed out after {}s", secs);
                    let should_retry = task.retry.attempt < task.retry.max_attempts;
                    if should_retry {
                        let result = TaskResult {
                            id: task.id,
                            status: TaskStatus::Retrying,
                            started_at: running.started_at,
                            finished_at,
                            output: None,
                            error: Some(error.clone()),
                        };
                        let result_payload = serde_json::to_string(&result)?;
                        let _: String = conn.set(&result_key, result_payload)?;

                        let backoff = task.retry.backoff_seconds * (task.retry.attempt as u64 + 1);
                        if backoff > 0 {
                            thread::sleep(Duration::from_secs(backoff));
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
                            .query(&mut conn)?;
                    } else {
                        let result = TaskResult {
                            id: task.id,
                            status: TaskStatus::Failed,
                            started_at: running.started_at,
                            finished_at,
                            output: None,
                            error: Some(error),
                        };
                        let result_payload = serde_json::to_string(&result)?;
                        let _: String = conn.set(result_key, result_payload)?;
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
enum TaskFailure {
    UnknownTask(String),
    Execution(String),
    Timeout(u64),
}

fn execute_task(
    task: &TaskSpec,
    registry: &HashMap<String, fn(&TaskSpec) -> Result<serde_json::Value, String>>,
) -> Result<serde_json::Value, TaskFailure> {
    match registry.get(&task.name) {
        Some(handler) => handler(task).map_err(TaskFailure::Execution),
        None => Err(TaskFailure::UnknownTask(task.name.clone())),
    }
}

fn execute_task_with_timeout(
    task: &TaskSpec,
    registry: &HashMap<String, fn(&TaskSpec) -> Result<serde_json::Value, String>>,
) -> Result<serde_json::Value, TaskFailure> {
    match task.timeout_seconds {
        Some(timeout) => {
            let (tx, rx) = mpsc::channel();
            let task_clone = task.clone();
            let registry_clone = registry.clone();

            thread::spawn(move || {
                let result = execute_task(&task_clone, &registry_clone);
                let _ = tx.send(result);
            });

            match rx.recv_timeout(Duration::from_secs(timeout)) {
                Ok(result) => result,
                Err(mpsc::RecvTimeoutError::Timeout) => Err(TaskFailure::Timeout(timeout)),
                Err(_) => Err(TaskFailure::Execution(
                    "Worker execution channel closed".to_string(),
                )),
            }
        }
        None => execute_task(task, registry),
    }
}

fn build_registry() -> HashMap<String, fn(&TaskSpec) -> Result<serde_json::Value, String>> {
    let mut registry: HashMap<String, fn(&TaskSpec) -> Result<serde_json::Value, String>> =
        HashMap::new();
    registry.insert("echo".to_string(), task_echo);
    registry.insert("add".to_string(), task_add);
    registry.insert("sleep".to_string(), task_sleep);
    registry
}

fn task_echo(task: &TaskSpec) -> Result<serde_json::Value, String> {
    Ok(json!({
        "args": task.args,
        "kwargs": task.kwargs,
    }))
}

fn task_add(task: &TaskSpec) -> Result<serde_json::Value, String> {
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

fn task_sleep(task: &TaskSpec) -> Result<serde_json::Value, String> {
    let kwargs = task
        .kwargs
        .as_object()
        .ok_or_else(|| "kwargs must be an object".to_string())?;
    let seconds = kwargs
        .get("seconds")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| "sleep requires kwargs.seconds (u64)".to_string())?;
    std::thread::sleep(std::time::Duration::from_secs(seconds));
    Ok(json!({ "slept_seconds": seconds }))
}

fn extract_payload(reply: redis::Value) -> Option<(String, String)> {
    let streams = match reply {
        redis::Value::Bulk(items) => items,
        _ => return None,
    };

    let first_stream = streams.get(0)?;
    let stream_items = match first_stream {
        redis::Value::Bulk(items) => items,
        _ => return None,
    };

    if stream_items.len() < 2 {
        return None;
    }

    let entries = match &stream_items[1] {
        redis::Value::Bulk(items) => items,
        _ => return None,
    };

    let first_entry = entries.get(0)?;
    let entry_items = match first_entry {
        redis::Value::Bulk(items) => items,
        _ => return None,
    };

    if entry_items.len() < 2 {
        return None;
    }

    let id = match &entry_items[0] {
        redis::Value::Data(bytes) => String::from_utf8_lossy(bytes).to_string(),
        _ => return None,
    };

    let fields = match &entry_items[1] {
        redis::Value::Bulk(items) => items,
        _ => return None,
    };

    let mut i = 0;
    while i + 1 < fields.len() {
        let key = match &fields[i] {
            redis::Value::Data(bytes) => String::from_utf8_lossy(bytes).to_string(),
            _ => return None,
        };
        let value = match &fields[i + 1] {
            redis::Value::Data(bytes) => String::from_utf8_lossy(bytes).to_string(),
            _ => return None,
        };
        if key == "payload" {
            return Some((id, value));
        }
        i += 2;
    }

    None
}
