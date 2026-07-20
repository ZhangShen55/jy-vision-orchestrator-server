import json
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import List, Optional, Protocol

from app.infrastructure.compat import StrEnum
from app.infrastructure.worker_control import WorkerDesiredState


class WorkerRuntimeState(StrEnum):
    REGISTERING = "REGISTERING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    DRAINING = "DRAINING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class WorkerStatus:
    worker_id: str
    actual_state: WorkerRuntimeState
    desired_state: WorkerDesiredState
    topic: str
    consumer_group: str
    assigned_partitions: List[int]
    current_task_id: Optional[str]
    current_partition: Optional[int]
    current_offset: Optional[int]
    processed_count: int
    failed_count: int
    last_error: Optional[str]
    started_at: str
    last_heartbeat_at: str
    expires_at: float = field(default_factory=lambda: time.time() + 30)

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["actual_state"] = self.actual_state.value
        payload["desired_state"] = self.desired_state.value
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_payload(cls, payload: dict) -> "WorkerStatus":
        return cls(
            worker_id=str(payload["worker_id"]),
            actual_state=WorkerRuntimeState(str(payload.get("actual_state") or WorkerRuntimeState.REGISTERING)),
            desired_state=WorkerDesiredState(str(payload.get("desired_state") or WorkerDesiredState.PAUSED)),
            topic=str(payload.get("topic") or ""),
            consumer_group=str(payload.get("consumer_group") or ""),
            assigned_partitions=[int(item) for item in (payload.get("assigned_partitions") or [])],
            current_task_id=payload.get("current_task_id"),
            current_partition=_optional_int(payload.get("current_partition")),
            current_offset=_optional_int(payload.get("current_offset")),
            processed_count=max(0, int(payload.get("processed_count") or 0)),
            failed_count=max(0, int(payload.get("failed_count") or 0)),
            last_error=payload.get("last_error"),
            started_at=str(payload.get("started_at") or _now_iso()),
            last_heartbeat_at=str(payload.get("last_heartbeat_at") or _now_iso()),
            expires_at=float(payload.get("expires_at") or time.time() + 30),
        )

    @classmethod
    def from_json(cls, value: str) -> "WorkerStatus":
        return cls.from_payload(json.loads(value))

    def is_expired(self, now: Optional[float] = None) -> bool:
        return (now if now is not None else time.time()) >= self.expires_at


class WorkerRegistry(Protocol):
    def upsert(self, status: WorkerStatus, ttl_seconds: Optional[int] = None) -> None:
        ...

    def list_workers(self) -> List[WorkerStatus]:
        ...

    def get_worker(self, worker_id: str) -> Optional[WorkerStatus]:
        ...


class InMemoryWorkerRegistry:
    def __init__(self, default_ttl_seconds: int = 30):
        self.default_ttl_seconds = int(default_ttl_seconds)
        self._workers: dict[str, WorkerStatus] = {}

    def upsert(self, status: WorkerStatus, ttl_seconds: Optional[int] = None) -> None:
        ttl = self.default_ttl_seconds if ttl_seconds is None else int(ttl_seconds)
        self._workers[status.worker_id] = _with_expiry(status, ttl)

    def list_workers(self) -> List[WorkerStatus]:
        self._remove_expired()
        return list(self._workers.values())

    def get_worker(self, worker_id: str) -> Optional[WorkerStatus]:
        self._remove_expired()
        return self._workers.get(worker_id)

    def _remove_expired(self) -> None:
        now = time.time()
        expired = [worker_id for worker_id, status in self._workers.items() if status.is_expired(now)]
        for worker_id in expired:
            self._workers.pop(worker_id, None)


class RedisWorkerRegistry:
    def __init__(
            self,
            redis_url: str,
            key_prefix: str = "vision_orchestrator",
            default_ttl_seconds: int = 30):
        try:
            import redis
        except ModuleNotFoundError as exc:
            raise RuntimeError("缺少 redis 依赖，请安装 app/requirements.txt") from exc
        self.client = redis.Redis.from_url(redis_url, decode_responses=True)
        self.key_prefix = key_prefix.rstrip(":")
        self.default_ttl_seconds = int(default_ttl_seconds)

    @property
    def workers_key(self) -> str:
        return f"{self.key_prefix}:workers"

    def worker_key(self, worker_id: str) -> str:
        return f"{self.key_prefix}:worker:{worker_id}"

    def upsert(self, status: WorkerStatus, ttl_seconds: Optional[int] = None) -> None:
        ttl = self.default_ttl_seconds if ttl_seconds is None else int(ttl_seconds)
        status = _with_expiry(status, ttl)
        pipe = self.client.pipeline()
        pipe.sadd(self.workers_key, status.worker_id)
        pipe.set(self.worker_key(status.worker_id), status.to_json(), ex=max(1, ttl))
        pipe.execute()

    def list_workers(self) -> List[WorkerStatus]:
        worker_ids = self.client.smembers(self.workers_key)
        workers = []
        for worker_id in worker_ids:
            status = self.get_worker(worker_id)
            if status is not None:
                workers.append(status)
        return workers

    def get_worker(self, worker_id: str) -> Optional[WorkerStatus]:
        raw = self.client.get(self.worker_key(worker_id))
        if not raw:
            self.client.srem(self.workers_key, worker_id)
            return None
        status = WorkerStatus.from_json(raw)
        if status.is_expired():
            self.client.srem(self.workers_key, worker_id)
            self.client.delete(self.worker_key(worker_id))
            return None
        return status


def build_worker_status(
        worker_id: str,
        actual_state: WorkerRuntimeState,
        desired_state: WorkerDesiredState,
        topic: str,
        consumer_group: str,
        started_at: str,
        assigned_partitions: Optional[List[int]] = None,
        current_task_id: Optional[str] = None,
        current_partition: Optional[int] = None,
        current_offset: Optional[int] = None,
        processed_count: int = 0,
        failed_count: int = 0,
        last_error: Optional[str] = None) -> WorkerStatus:
    return WorkerStatus(
        worker_id=worker_id,
        actual_state=actual_state,
        desired_state=desired_state,
        topic=topic,
        consumer_group=consumer_group,
        assigned_partitions=assigned_partitions or [],
        current_task_id=current_task_id,
        current_partition=current_partition,
        current_offset=current_offset,
        processed_count=processed_count,
        failed_count=failed_count,
        last_error=last_error,
        started_at=started_at,
        last_heartbeat_at=_now_iso(),
    )


def _with_expiry(status: WorkerStatus, ttl_seconds: int) -> WorkerStatus:
    payload = status.to_dict()
    payload["expires_at"] = time.time() + ttl_seconds
    payload["last_heartbeat_at"] = _now_iso()
    return WorkerStatus.from_payload(payload)


def _optional_int(value) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
