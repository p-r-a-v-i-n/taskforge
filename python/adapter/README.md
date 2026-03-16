# Taskforge Python Adapter

A minimal Celery-like API that publishes tasks to Redis Streams using the
Taskforge task schema.

## Install (editable)

```bash
pip install -e python/adapter
```

## Quick Start

```python
from taskforge import TaskforgeApp

app = TaskforgeApp(broker_url="redis://127.0.0.1:6379")

@app.task()
def send_email(to, subject):
    ...

handle = send_email.delay("user@example.com", "Hi")
print(handle.id)
```

## Notes
- This is an MVP adapter (publish-only). Workers still run in Rust.
- The default stream is `taskforge.tasks`.

## Demo Script

```bash
python python/adapter/examples/enqueue_demo.py
```
