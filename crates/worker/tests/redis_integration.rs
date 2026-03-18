use redis::AsyncCommands;
use taskforge_core::{RetryPolicy, TaskResult, TaskSpec, TaskStatus};
use taskforge_worker::{default_registry, run_worker_once, WorkerConfig};
use testcontainers::core::ContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::GenericImage;
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
            backoff_seconds: 0,
        },
        eta: None,
        timeout_seconds: None,
        idempotency_key: None,
        created_at: chrono::Utc::now(),
    }
}

#[tokio::test]
async fn redis_echo_task_succeeds() -> anyhow::Result<()> {
    let container = GenericImage::new("redis", "7")
        .with_exposed_port(ContainerPort::Tcp(6379))
        .start()
        .await?;
    let port = container.get_host_port_ipv4(6379).await?;
    let redis_url = format!("redis://127.0.0.1:{port}");

    let client = redis::Client::open(redis_url.clone())?;
    let mut conn = redis::aio::ConnectionManager::new(client).await?;

    let config = WorkerConfig {
        broker_url: redis_url,
        stream: "taskforge.tasks".to_string(),
        last_id: "0".to_string(),
        block_ms: 100,
        prefetch: 10,
        concurrency: 1,
        result_prefix: "taskforge:result:".to_string(),
        result_ttl_seconds: 60,
    };

    let registry = default_registry();
    let mut last_id = config.last_id.clone();

    let mut task = make_task("echo");
    task.args = serde_json::json!(["hello"]);
    task.kwargs = serde_json::json!({"from": "taskforge"});

    let payload = serde_json::to_string(&task)?;
    let _: String = redis::cmd("XADD")
        .arg(&config.stream)
        .arg("*")
        .arg("payload")
        .arg(payload)
        .query_async(&mut conn)
        .await?;

    let processed = run_worker_once(&mut conn, &config, &registry, &mut last_id).await?;
    assert_eq!(processed, 1);

    let result_key = format!("{}{}", config.result_prefix, task.id);
    let result_json: String = conn.get(result_key).await?;
    let result: TaskResult = serde_json::from_str(&result_json)?;
    assert!(matches!(result.status, TaskStatus::Succeeded));
    assert_eq!(result.output.unwrap()["args"][0], "hello");

    Ok(())
}

#[tokio::test]
async fn redis_add_task_retries_then_fails() -> anyhow::Result<()> {
    let container = GenericImage::new("redis", "7")
        .with_exposed_port(ContainerPort::Tcp(6379))
        .start()
        .await?;
    let port = container.get_host_port_ipv4(6379).await?;
    let redis_url = format!("redis://127.0.0.1:{port}");

    let client = redis::Client::open(redis_url.clone())?;
    let mut conn = redis::aio::ConnectionManager::new(client).await?;

    let config = WorkerConfig {
        broker_url: redis_url,
        stream: "taskforge.tasks".to_string(),
        last_id: "0".to_string(),
        block_ms: 100,
        prefetch: 10,
        concurrency: 1,
        result_prefix: "taskforge:result:".to_string(),
        result_ttl_seconds: 60,
    };

    let registry = default_registry();
    let mut last_id = config.last_id.clone();

    let mut task = make_task("add");
    task.args = serde_json::json!(["oops", 2]);
    task.retry.max_attempts = 1;
    task.retry.backoff_seconds = 0;

    let payload = serde_json::to_string(&task)?;
    let _: String = redis::cmd("XADD")
        .arg(&config.stream)
        .arg("*")
        .arg("payload")
        .arg(payload)
        .query_async(&mut conn)
        .await?;

    let processed = run_worker_once(&mut conn, &config, &registry, &mut last_id).await?;
    assert_eq!(processed, 1);

    let processed = run_worker_once(&mut conn, &config, &registry, &mut last_id).await?;
    assert_eq!(processed, 1);

    let result_key = format!("{}{}", config.result_prefix, task.id);
    let result_json: String = conn.get(result_key).await?;
    let result: TaskResult = serde_json::from_str(&result_json)?;
    assert!(matches!(result.status, TaskStatus::Failed));

    Ok(())
}
