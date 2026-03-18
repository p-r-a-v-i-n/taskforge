use anyhow::Context;
use chrono::Utc;
use futures::future::BoxFuture;
use futures::FutureExt;
use redis::AsyncCommands;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use taskforge_core::{TaskResult, TaskSpec, TaskStatus};
use tokio::sync::Semaphore;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub broker_url: String,
    pub stream: String,
    pub last_id: String,
    pub block_ms: u64,
    pub prefetch: u64,
    pub concurrency: usize,
    pub result_prefix: String,
    pub result_ttl_seconds: u64,
}

#[derive(Debug)]
enum TaskFailure {
    UnknownTask(String),
    Execution(String),
    Timeout(u64),
}

pub type TaskHandler =
    Arc<dyn Fn(TaskSpec) -> BoxFuture<'static, Result<serde_json::Value, String>> + Send + Sync>;

pub async fn run_worker(config: WorkerConfig) -> anyhow::Result<()> {
    let client = redis::Client::open(config.broker_url.clone())?;
    let mut conn = redis::aio::ConnectionManager::new(client)
        .await
        .context("Failed to create Redis connection manager")?;
    let mut last_id = config.last_id.clone();

    let registry = default_registry();
    let semaphore = Arc::new(Semaphore::new(config.concurrency));

    loop {
        let payloads = read_payloads(&mut conn, &config, &last_id).await?;
        for (id, payload) in payloads {
            last_id = id;
            let task: TaskSpec = match serde_json::from_str(&payload) {
                Ok(task) => task,
                Err(err) => {
                    eprintln!("Invalid task payload JSON: {}", err);
                    continue;
                }
            };

            let permit = semaphore.clone().acquire_owned().await?;
            let config_clone = config.clone();
            let registry_clone = registry.clone();
            let mut task_conn = conn.clone();

            tokio::spawn(async move {
                let _permit = permit;
                if let Err(err) =
                    handle_task(task, &config_clone, &registry_clone, &mut task_conn).await
                {
                    eprintln!("Task handling error: {}", err);
                }
            });
        }
    }
}

pub async fn run_worker_once(
    conn: &mut redis::aio::ConnectionManager,
    config: &WorkerConfig,
    registry: &HashMap<String, TaskHandler>,
    last_id: &mut String,
) -> anyhow::Result<usize> {
    let payloads = read_payloads(conn, config, last_id).await?;
    let mut processed = 0;
    for (id, payload) in payloads {
        *last_id = id;
        let task: TaskSpec = serde_json::from_str(&payload)
            .map_err(|e| anyhow::anyhow!("Invalid task payload JSON: {e}"))?;
        handle_task(task, config, registry, conn).await?;
        processed += 1;
    }
    Ok(processed)
}

async fn read_payloads(
    conn: &mut redis::aio::ConnectionManager,
    config: &WorkerConfig,
    last_id: &str,
) -> anyhow::Result<Vec<(String, String)>> {
    let reply: redis::Value = redis::cmd("XREAD")
        .arg("BLOCK")
        .arg(config.block_ms)
        .arg("COUNT")
        .arg(config.prefetch)
        .arg("STREAMS")
        .arg(&config.stream)
        .arg(last_id)
        .query_async(conn)
        .await?;

    Ok(extract_payloads(reply))
}

async fn handle_task(
    task: TaskSpec,
    config: &WorkerConfig,
    registry: &HashMap<String, TaskHandler>,
    conn: &mut redis::aio::ConnectionManager,
) -> anyhow::Result<()> {
    let result_key = format!("{}{}", config.result_prefix, task.id);
    let running = TaskResult {
        id: task.id,
        status: TaskStatus::Running,
        started_at: Some(Utc::now()),
        finished_at: None,
        output: None,
        error: None,
    };
    write_result(conn, &result_key, &running, config.result_ttl_seconds).await?;

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
            write_result(conn, &result_key, &result, config.result_ttl_seconds).await?;
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
            write_result(conn, &result_key, &result, config.result_ttl_seconds).await?;
        }
        Err(TaskFailure::Execution(error)) => {
            handle_retry(
                &task,
                config,
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
                config,
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
    config: &WorkerConfig,
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

    let result_key = format!("{}{}", config.result_prefix, task.id);

    if should_retry {
        let result = TaskResult {
            id: task.id,
            status: TaskStatus::Retrying,
            started_at,
            finished_at,
            output: None,
            error: Some(error.clone()),
        };
        write_result(conn, &result_key, &result, config.result_ttl_seconds).await?;

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
            .arg(&config.stream)
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
        write_result(conn, &result_key, &result, config.result_ttl_seconds).await?;
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

pub fn default_registry() -> HashMap<String, TaskHandler> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use taskforge_core::RetryPolicy;
    use uuid::Uuid;

    fn make_task(name: &str) -> TaskSpec {
        TaskSpec {
            id: Uuid::new_v4(),
            name: name.to_string(),
            args: serde_json::Value::Array(vec![]),
            kwargs: serde_json::Value::Object(serde_json::Map::new()),
            queue: "default".to_string(),
            retry: RetryPolicy {
                max_attempts: 1,
                attempt: 0,
                backoff_seconds: 1,
            },
            eta: None,
            timeout_seconds: None,
            idempotency_key: None,
            created_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn task_echo_returns_args_kwargs() {
        let mut task = make_task("echo");
        task.args = serde_json::json!(["hello"]);
        task.kwargs = serde_json::json!({"from": "taskforge"});

        let result = task_echo(task).await.unwrap();
        assert_eq!(result["args"][0], "hello");
        assert_eq!(result["kwargs"]["from"], "taskforge");
    }

    #[tokio::test]
    async fn task_add_sums_numbers() {
        let mut task = make_task("add");
        task.args = serde_json::json!([1, 2.5, 3]);

        let result = task_add(task).await.unwrap();
        assert_eq!(result["sum"], 6.5);
    }

    #[tokio::test]
    async fn task_add_rejects_non_numbers() {
        let mut task = make_task("add");
        task.args = serde_json::json!(["oops", 2]);

        let err = task_add(task).await.unwrap_err();
        assert!(err.contains("numbers"));
    }

    #[tokio::test]
    async fn execute_task_unknown_task_is_non_retryable() {
        let task = make_task("unknown_task");
        let registry = default_registry();

        let err = execute_task(&task, &registry).await.unwrap_err();
        match err {
            TaskFailure::UnknownTask(name) => assert_eq!(name, "unknown_task"),
            _ => panic!("Expected UnknownTask"),
        }
    }

    #[tokio::test]
    async fn execute_task_times_out() {
        let mut task = make_task("sleep");
        task.timeout_seconds = Some(1);
        task.kwargs = serde_json::json!({"seconds": 2});

        let registry = default_registry();
        let err = execute_task_with_timeout(&task, &registry).await.unwrap_err();
        match err {
            TaskFailure::Timeout(secs) => assert_eq!(secs, 1),
            _ => panic!("Expected Timeout"),
        }
    }
}
