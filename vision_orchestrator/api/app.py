import logging
from typing import Any, Mapping

from fastapi import Depends, FastAPI, Header, HTTPException, Request

from vision_orchestrator.config import VisionOrchestratorConfig
from vision_orchestrator.infrastructure.tias.registry import TiasInstanceStatus, TiasRegistry
from vision_orchestrator.infrastructure.worker_control import (
    WorkerControlStateRepository,
    WorkerDesiredState,
)
from vision_orchestrator.infrastructure.worker_registry import WorkerRegistry


logger = logging.getLogger(__name__)


class TiasInstanceApi:
    def __init__(self, registry: TiasRegistry, heartbeat_interval_seconds: int = 5, default_ttl_seconds: int = 15):
        self.registry = registry
        self.heartbeat_interval_seconds = int(heartbeat_interval_seconds)
        self.default_ttl_seconds = int(default_ttl_seconds)

    def register(self, payload: Mapping[str, Any]) -> dict:
        instance = self._instance_from_payload(payload)
        ttl_seconds = int(payload.get("heartbeat_timeout_seconds") or self.default_ttl_seconds)
        self.registry.upsert(instance, ttl_seconds=ttl_seconds)
        logger.info(
            "TIAS 注册 instance_id=%s base_url=%s max_concurrent_batches=%s max_queue_size=%s",
            instance.instance_id,
            instance.base_url,
            instance.max_concurrent_batches,
            instance.max_queue_size,
        )
        return {
            "status": "ok",
            "instance_id": instance.instance_id,
            "heartbeat_interval_seconds": self.heartbeat_interval_seconds,
        }

    def heartbeat(self, payload: Mapping[str, Any]) -> dict:
        instance = self._instance_from_payload(payload)
        ttl_seconds = int(payload.get("heartbeat_timeout_seconds") or self.default_ttl_seconds)
        self.registry.upsert(instance, ttl_seconds=ttl_seconds)
        logger.info(
            "TIAS 心跳 instance_id=%s status=%s running_batches=%s queued_batches=%s",
            instance.instance_id,
            instance.status,
            instance.running_batches,
            instance.queued_batches,
        )
        return {
            "status": "ok",
            "instance_id": instance.instance_id,
            "heartbeat_interval_seconds": self.heartbeat_interval_seconds,
        }

    def unregister(self, payload: Mapping[str, Any]) -> dict:
        instance_id = str(payload.get("instance_id") or "").strip()
        if not instance_id:
            raise ValueError("instance_id is required")
        self.registry.unregister(instance_id)
        logger.info("TIAS 注销 instance_id=%s", instance_id)
        return {
            "status": "ok",
            "instance_id": instance_id,
        }

    def _instance_from_payload(self, payload: Mapping[str, Any]) -> TiasInstanceStatus:
        required_fields = ["instance_id", "base_url", "max_concurrent_batches", "max_queue_size"]
        missing = [field for field in required_fields if payload.get(field) in (None, "")]
        if missing:
            raise ValueError("缺少 TIAS 注册字段: " + ",".join(missing))
        return TiasInstanceStatus.from_payload(dict(payload), default_ttl_seconds=self.default_ttl_seconds)


def create_api_app(
        config: VisionOrchestratorConfig,
        tias_registry: TiasRegistry,
        worker_control_repository: WorkerControlStateRepository,
        worker_registry: WorkerRegistry) -> FastAPI:
    app = FastAPI(title="AI课堂质量调度服务", version="6.0")
    app.state.config = config
    app.state.tias_registry = tias_registry
    app.state.worker_control_repository = worker_control_repository
    app.state.worker_registry = worker_registry
    tias_api = TiasInstanceApi(
        tias_registry,
        heartbeat_interval_seconds=5,
        default_ttl_seconds=config.tias_heartbeat_timeout_seconds,
    )

    @app.get("/api/health")
    async def health():
        redis_status = "skipped"
        redis_error = None
        if config.health_check_redis:
            try:
                worker_control_repository.get_state()
                redis_status = "ok"
            except Exception as exc:  # pragma: no cover - depends on external Redis
                redis_status = "error"
                redis_error = str(exc)
        body = {
            "status": "ok" if redis_status != "error" else "error",
            "service": "vision_orchestrator-api",
            "version": "6.0",
            "redis_key_prefix": config.redis_key_prefix,
            "worker_registry_key_prefix": config.worker_registry_key_prefix,
            "redis_status": redis_status,
        }
        if redis_error:
            body["error"] = redis_error
        return body

    @app.get("/api/tias/instances")
    async def list_tias_instances():
        return {"items": [_to_dict(item) for item in tias_registry.list_instances()]}

    @app.get("/api/tias/instances/{instance_id}")
    async def get_tias_instance(instance_id: str):
        for instance in tias_registry.list_instances():
            if instance.instance_id == instance_id:
                return _to_dict(instance)
        raise HTTPException(status_code=404, detail="TIAS 实例不存在或心跳已过期")

    @app.post("/api/tias/instances/register")
    async def register_tias(payload: dict):
        try:
            return tias_api.register(payload)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    @app.post("/api/tias/instances/heartbeat")
    async def heartbeat_tias(payload: dict):
        try:
            return tias_api.heartbeat(payload)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    @app.post("/api/tias/instances/unregister")
    async def unregister_tias(payload: dict):
        try:
            return tias_api.unregister(payload)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    @app.post("/api/worker-control/resume")
    async def resume(payload: dict | None = None, _: None = Depends(_require_control_key)):
        return _set_worker_state(WorkerDesiredState.RUNNING, payload)

    @app.post("/api/worker-control/pause")
    async def pause(payload: dict | None = None, _: None = Depends(_require_control_key)):
        return _set_worker_state(WorkerDesiredState.PAUSED, payload)

    @app.post("/api/worker-control/drain")
    async def drain(payload: dict | None = None, _: None = Depends(_require_control_key)):
        return _set_worker_state(WorkerDesiredState.DRAINING, payload)

    @app.get("/api/worker-control/state")
    async def worker_control_state():
        return worker_control_repository.get_state().to_dict()

    @app.get("/api/workers")
    async def list_workers():
        return {"items": [_to_dict(item) for item in worker_registry.list_workers()]}

    @app.get("/api/workers/{worker_id}")
    async def get_worker(worker_id: str):
        worker = worker_registry.get_worker(worker_id)
        if worker is None:
            raise HTTPException(status_code=404, detail="Worker 不存在或心跳已过期")
        return _to_dict(worker)

    def _set_worker_state(desired_state: WorkerDesiredState, payload: dict | None) -> dict:
        payload = payload or {}
        state = worker_control_repository.set_desired_state(
            desired_state,
            updated_by=str(payload.get("updated_by") or "api"),
            reason=payload.get("reason"),
        )
        return state.to_dict()

    return app


def _to_dict(value) -> dict:
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if hasattr(value, "__dict__"):
        return dict(value.__dict__)
    return dict(value)


async def _require_control_key(request: Request, x_vision_orchestrator_key: str | None = Header(default=None)) -> None:
    config: VisionOrchestratorConfig = request.app.state.config
    if not config.worker_control_enabled:
        raise HTTPException(status_code=403, detail="Worker 控制接口未启用")
    header_name = config.worker_control_header_name.lower()
    provided = x_vision_orchestrator_key
    if header_name != "x-vision-orchestrator-key":
        provided = request.headers.get(config.worker_control_header_name)
    if not provided or provided != config.worker_control_key:
        raise HTTPException(status_code=403, detail="Worker 控制 key 无效")
