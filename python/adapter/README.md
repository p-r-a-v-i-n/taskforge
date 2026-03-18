# Rustly Dispatch Python Adapter

A minimal Celery-like API that publishes tasks to Redis Streams using the
Rustly Dispatch task schema.

## Install (editable)

```bash
pip install -e python/adapter
```

## Quick Start

```python
from rustly_dispatch import RetryPolicy, RustlyDispatchApp

app = RustlyDispatchApp(
    broker_url="redis://127.0.0.1:6379",
    default_retry_policy=RetryPolicy(max_attempts=3, backoff_seconds=2),
    default_timeout_seconds=10,
)

@app.task()
def send_email(to, subject):
    ...

handle = send_email.delay("user@example.com", "Hi")
print(handle.id)
```

## Notes
- This is an MVP adapter (publish-only). Workers still run in Rust.
- The default stream is `rustly-dispatch.tasks`.

## Retry Policy Override

```python
handle = send_email.apply_async(
    args=["user@example.com", "Hi"],
    retry_policy=RetryPolicy(max_attempts=5, backoff_seconds=3),
)
```

## Demo Script

```bash
python python/adapter/examples/enqueue_demo.py
```
