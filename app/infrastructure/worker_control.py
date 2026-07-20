import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional, Protocol

from app.infrastructure.compat import StrEnum


class WorkerDesiredState(StrEnum):
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    DRAINING = "DRAINING"
    STOPPED = "STOPPED"


@dataclass(frozen=True)
class WorkerControlState:
    desired_state: WorkerDesiredState
    version: int
    updated_at: str
    updated_by: str = "system"
    reason: Optional[str] = None

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["desired_state"] = self.desired_state.value
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_payload(cls, payload: dict) -> "WorkerControlState":
        return cls(
            desired_state=WorkerDesiredState(str(payload.get("desired_state") or WorkerDesiredState.PAUSED)),
            version=int(payload.get("version") or 0),
            updated_at=str(payload.get("updated_at") or _now_iso()),
            updated_by=str(payload.get("updated_by") or "system"),
            reason=payload.get("reason"),
        )

    @classmethod
    def from_json(cls, value: str) -> "WorkerControlState":
        return cls.from_payload(json.loads(value))


class WorkerControlStateRepository(Protocol):
    def get_state(self) -> WorkerControlState:
        ...

    def set_desired_state(
            self,
            desired_state: WorkerDesiredState,
            updated_by: str = "system",
            reason: Optional[str] = None) -> WorkerControlState:
        ...


class InMemoryWorkerControlStateRepository:
    def __init__(self, default_state: WorkerDesiredState = WorkerDesiredState.PAUSED):
        self._state = WorkerControlState(
            desired_state=default_state,
            version=0,
            updated_at=_now_iso(),
            updated_by="system",
            reason="default",
        )

    def get_state(self) -> WorkerControlState:
        return self._state

    def set_desired_state(
            self,
            desired_state: WorkerDesiredState,
            updated_by: str = "system",
            reason: Optional[str] = None) -> WorkerControlState:
        self._state = WorkerControlState(
            desired_state=WorkerDesiredState(desired_state),
            version=self._state.version + 1,
            updated_at=_now_iso(),
            updated_by=updated_by or "system",
            reason=reason,
        )
        return self._state


class RedisWorkerControlStateRepository:
    def __init__(
            self,
            redis_url: str,
            state_key: str = "vision_orchestrator:worker_control:state",
            default_state: WorkerDesiredState = WorkerDesiredState.PAUSED):
        try:
            import redis
        except ModuleNotFoundError as exc:
            raise RuntimeError("缺少 redis 依赖，请安装 app/requirements.txt") from exc
        self.client = redis.Redis.from_url(redis_url, decode_responses=True)
        self.state_key = state_key
        self.default_state = default_state

    def get_state(self) -> WorkerControlState:
        raw = self.client.get(self.state_key)
        if not raw:
            return WorkerControlState(
                desired_state=self.default_state,
                version=0,
                updated_at=_now_iso(),
                updated_by="system",
                reason="default",
            )
        return WorkerControlState.from_json(raw)

    def set_desired_state(
            self,
            desired_state: WorkerDesiredState,
            updated_by: str = "system",
            reason: Optional[str] = None) -> WorkerControlState:
        current = self.get_state()
        next_state = WorkerControlState(
            desired_state=WorkerDesiredState(desired_state),
            version=current.version + 1,
            updated_at=_now_iso(),
            updated_by=updated_by or "system",
            reason=reason,
        )
        self.client.set(self.state_key, next_state.to_json())
        return next_state


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
