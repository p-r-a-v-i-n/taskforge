from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Union

from .client import RetryPolicy, TaskforgeClient, TaskHandle


@dataclass
class TaskDefinition:
    name: str
    client: TaskforgeClient
    default_queue: str = "default"
    default_retry_policy: Optional[RetryPolicy] = None
    default_timeout_seconds: Optional[int] = None

    def delay(self, *args: Any, **kwargs: Any) -> TaskHandle:
        return self.client.send_task(
            self.name,
            list(args),
            kwargs,
            queue=self.default_queue,
            retry_policy=self.default_retry_policy,
            timeout_seconds=self.default_timeout_seconds,
        )

    def apply_async(
        self,
        args: Optional[list] = None,
        kwargs: Optional[dict] = None,
        **options: Any,
    ) -> TaskHandle:
        retry_policy = _build_retry_policy(
            options.get("retry_policy"),
            options.get("max_attempts"),
            options.get("attempt"),
            options.get("backoff_seconds"),
            self.default_retry_policy,
        )
        return self.client.send_task(
            self.name,
            args=args or [],
            kwargs=kwargs or {},
            queue=options.get("queue", self.default_queue),
            retry_policy=retry_policy,
            eta=options.get("eta"),
            timeout_seconds=options.get("timeout_seconds", self.default_timeout_seconds),
            idempotency_key=options.get("idempotency_key"),
        )


class TaskforgeApp:
    def __init__(
        self,
        broker_url: str,
        stream: str = "taskforge.tasks",
        default_queue: str = "default",
        default_retry_policy: Optional[RetryPolicy] = None,
        default_timeout_seconds: Optional[int] = None,
    ) -> None:
        self.client = TaskforgeClient(broker_url, stream=stream)
        self.default_queue = default_queue
        self.default_retry_policy = default_retry_policy
        self.default_timeout_seconds = default_timeout_seconds

    def task(self, name: Optional[str] = None) -> Callable[[Callable[..., Any]], TaskDefinition]:
        def decorator(func: Callable[..., Any]) -> TaskDefinition:
            task_name = name or func.__name__
            return TaskDefinition(
                name=task_name,
                client=self.client,
                default_queue=self.default_queue,
                default_retry_policy=self.default_retry_policy,
                default_timeout_seconds=self.default_timeout_seconds,
            )

        return decorator


def _build_retry_policy(
    override: Optional[Union[RetryPolicy, Dict[str, Any]]],
    max_attempts: Optional[int],
    attempt: Optional[int],
    backoff_seconds: Optional[int],
    default: Optional[RetryPolicy],
) -> Optional[Union[RetryPolicy, Dict[str, Any]]]:
    if override is not None:
        return override
    if max_attempts is None and attempt is None and backoff_seconds is None:
        return default
    return RetryPolicy(
        max_attempts=max_attempts if max_attempts is not None else 3,
        attempt=attempt if attempt is not None else 0,
        backoff_seconds=backoff_seconds if backoff_seconds is not None else 5,
    )
