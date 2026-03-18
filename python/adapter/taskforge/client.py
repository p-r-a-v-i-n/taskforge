import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

import redis


@dataclass(frozen=True)
class TaskHandle:
    id: str


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    attempt: int = 0
    backoff_seconds: int = 5

    def to_dict(self) -> Dict[str, int]:
        return {
            "max_attempts": self.max_attempts,
            "attempt": self.attempt,
            "backoff_seconds": self.backoff_seconds,
        }


class TaskforgeClient:
    def __init__(self, broker_url: str, stream: str = "taskforge.tasks") -> None:
        self._redis = redis.Redis.from_url(broker_url, decode_responses=True)
        self._stream = stream

    def send_task(
        self,
        name: str,
        args: Optional[List[Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        queue: str = "default",
        retry_policy: Optional[Union["RetryPolicy", Dict[str, Any]]] = None,
        eta: Optional[datetime] = None,
        timeout_seconds: Optional[int] = None,
        idempotency_key: Optional[str] = None,
    ) -> TaskHandle:
        args = args or []
        kwargs = kwargs or {}
        if retry_policy is None:
            retry_policy_dict: Dict[str, Any] = RetryPolicy().to_dict()
        elif isinstance(retry_policy, RetryPolicy):
            retry_policy_dict = retry_policy.to_dict()
        elif isinstance(retry_policy, dict):
            retry_policy_dict = retry_policy
        else:
            raise TypeError("retry_policy must be a dict or RetryPolicy")

        if not isinstance(args, list):
            raise TypeError("args must be a list")
        if not isinstance(kwargs, dict):
            raise TypeError("kwargs must be a dict")

        payload = {
            "id": str(uuid.uuid4()),
            "name": name,
            "args": args,
            "kwargs": kwargs,
            "queue": queue,
            "retry": retry_policy_dict,
            "eta": eta.astimezone(timezone.utc).isoformat() if eta else None,
            "timeout_seconds": timeout_seconds,
            "idempotency_key": idempotency_key,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        self._redis.xadd(self._stream, {"payload": json.dumps(payload)})
        return TaskHandle(id=payload["id"])
