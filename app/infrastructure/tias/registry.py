import json
import time
from dataclasses import asdict, dataclass, field
from typing import Iterable, List, Optional, Protocol


@dataclass(frozen=True)
class TiasInstanceStatus:
    instance_id: str
    base_url: str
    capabilities: List[str]
    max_concurrent_batches: int
    running_batches: int
    queued_batches: int
    max_queue_size: int
    status: str
    available_slots: int = 0
    avg_latency_ms: Optional[float] = None
    p95_latency_ms: Optional[float] = None
    success_count: int = 0
    failure_count: int = 0
    recent_failure_count: int = 0
    last_error: Optional[str] = None
    service_version: Optional[str] = None
    model_version: Optional[str] = None
    last_heartbeat_at: Optional[float] = None
    expires_at: float = field(default_factory=lambda: time.time() + 15)

    @classmethod
    def from_payload(cls, payload: dict, default_ttl_seconds: int = 15) -> "TiasInstanceStatus":
        now = time.time()
        expires_at = payload.get("expires_at")
        if expires_at is None:
            expires_at = now + int(payload.get("heartbeat_timeout_seconds") or default_ttl_seconds)
        return cls(
            instance_id=str(payload["instance_id"]),
            base_url=str(payload["base_url"]).rstrip("/"),
            capabilities=list(payload.get("capabilities") or []),
            max_concurrent_batches=max(1, int(payload.get("max_concurrent_batches") or 1)),
            running_batches=max(0, int(payload.get("running_batches") or 0)),
            queued_batches=max(0, int(payload.get("queued_batches") or 0)),
            max_queue_size=max(0, int(payload.get("max_queue_size") or 0)),
            status=str(payload.get("status") or "UP"),
            available_slots=max(0, int(payload.get("available_slots") or 0)),
            avg_latency_ms=_optional_float(payload.get("avg_latency_ms")),
            p95_latency_ms=_optional_float(payload.get("p95_latency_ms")),
            success_count=max(0, int(payload.get("success_count") or 0)),
            failure_count=max(0, int(payload.get("failure_count") or 0)),
            recent_failure_count=max(0, int(payload.get("recent_failure_count") or 0)),
            last_error=payload.get("last_error"),
            service_version=payload.get("service_version"),
            model_version=payload.get("model_version"),
            last_heartbeat_at=_optional_float(payload.get("last_heartbeat_at")) or now,
            expires_at=float(expires_at),
        )

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json(cls, value: str) -> "TiasInstanceStatus":
        return cls.from_payload(json.loads(value))

    def is_expired(self, now: Optional[float] = None) -> bool:
        return (now if now is not None else time.time()) >= self.expires_at


class TiasRegistry(Protocol):
    def upsert(self, instance: TiasInstanceStatus, ttl_seconds: Optional[int] = None) -> None:
        ...

    def unregister(self, instance_id: str) -> None:
        ...

    def list_instances(self) -> List[TiasInstanceStatus]:
        ...


class InMemoryTiasRegistry:
    def __init__(self, key_prefix: str = "vision_orchestrator:tias", default_ttl_seconds: int = 15):
        self.key_prefix = key_prefix.rstrip(":")
        self.default_ttl_seconds = int(default_ttl_seconds)
        self._instances: dict[str, TiasInstanceStatus] = {}

    def upsert(self, instance: TiasInstanceStatus, ttl_seconds: Optional[int] = None) -> None:
        ttl = self.default_ttl_seconds if ttl_seconds is None else int(ttl_seconds)
        self._instances[instance.instance_id] = _with_expiry(instance, ttl)

    def unregister(self, instance_id: str) -> None:
        self._instances.pop(instance_id, None)

    def list_instances(self) -> List[TiasInstanceStatus]:
        now = time.time()
        expired = [instance_id for instance_id, instance in self._instances.items() if instance.is_expired(now)]
        for instance_id in expired:
            self._instances.pop(instance_id, None)
        return list(self._instances.values())


class RedisTiasRegistry:
    def __init__(self, redis_url: str, key_prefix: str = "vision_orchestrator:tias", default_ttl_seconds: int = 15):
        try:
            import redis
        except ModuleNotFoundError as exc:
            raise RuntimeError("缺少 redis 依赖，请安装 app/requirements.txt 或使用本地内存注册表测试") from exc
        self.client = redis.Redis.from_url(redis_url, decode_responses=True)
        self.key_prefix = key_prefix.rstrip(":")
        self.default_ttl_seconds = int(default_ttl_seconds)

    @property
    def instances_key(self) -> str:
        return f"{self.key_prefix}:instances"

    def instance_key(self, instance_id: str) -> str:
        return f"{self.key_prefix}:instance:{instance_id}"

    def upsert(self, instance: TiasInstanceStatus, ttl_seconds: Optional[int] = None) -> None:
        ttl = self.default_ttl_seconds if ttl_seconds is None else int(ttl_seconds)
        instance = _with_expiry(instance, ttl)
        pipe = self.client.pipeline()
        pipe.sadd(self.instances_key, instance.instance_id)
        pipe.set(self.instance_key(instance.instance_id), instance.to_json(), ex=max(1, ttl))
        pipe.execute()

    def unregister(self, instance_id: str) -> None:
        pipe = self.client.pipeline()
        pipe.srem(self.instances_key, instance_id)
        pipe.delete(self.instance_key(instance_id))
        pipe.execute()

    def list_instances(self) -> List[TiasInstanceStatus]:
        instance_ids = self.client.smembers(self.instances_key)
        instances = []
        for instance_id in instance_ids:
            raw = self.client.get(self.instance_key(instance_id))
            if not raw:
                self.client.srem(self.instances_key, instance_id)
                continue
            instance = TiasInstanceStatus.from_json(raw)
            if instance.is_expired():
                self.unregister(instance_id)
                continue
            instances.append(instance)
        return instances


def _optional_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _with_expiry(instance: TiasInstanceStatus, ttl_seconds: int) -> TiasInstanceStatus:
    payload = asdict(instance)
    payload["expires_at"] = time.time() + ttl_seconds
    payload["last_heartbeat_at"] = time.time()
    return TiasInstanceStatus(**payload)
