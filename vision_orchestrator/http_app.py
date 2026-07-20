import logging

from vision_orchestrator.api.app import TiasInstanceApi, create_api_app
from vision_orchestrator.config import VisionOrchestratorConfig
from vision_orchestrator.infrastructure.tias.registry import InMemoryTiasRegistry, RedisTiasRegistry, TiasRegistry
from vision_orchestrator.infrastructure.worker_control import (
    InMemoryWorkerControlStateRepository,
    RedisWorkerControlStateRepository,
    WorkerDesiredState,
)
from vision_orchestrator.infrastructure.worker_registry import InMemoryWorkerRegistry, RedisWorkerRegistry


logger = logging.getLogger(__name__)


def create_app(registry: TiasRegistry, config: VisionOrchestratorConfig | None = None):
    config = config or VisionOrchestratorConfig()
    return create_api_app(
        config=config,
        tias_registry=registry,
        worker_control_repository=InMemoryWorkerControlStateRepository(
            default_state=WorkerDesiredState(config.worker_default_desired_state),
        ),
        worker_registry=InMemoryWorkerRegistry(default_ttl_seconds=config.worker_heartbeat_timeout_seconds),
    )


def create_app_from_config(config: VisionOrchestratorConfig):
    registry = RedisTiasRegistry(
        redis_url=config.redis_url,
        key_prefix=config.redis_key_prefix,
        default_ttl_seconds=config.tias_heartbeat_timeout_seconds,
    )
    try:
        control_repository = RedisWorkerControlStateRepository(
            redis_url=config.redis_url,
            state_key=config.worker_control_state_key,
            default_state=WorkerDesiredState(config.worker_default_desired_state),
        )
        worker_registry = RedisWorkerRegistry(
            redis_url=config.redis_url,
            key_prefix=config.worker_registry_key_prefix,
            default_ttl_seconds=config.worker_heartbeat_timeout_seconds,
        )
    except RuntimeError:
        logger.warning("Redis Worker 控制依赖不可用，HTTP 服务使用内存控制仓储")
        control_repository = InMemoryWorkerControlStateRepository(
            default_state=WorkerDesiredState(config.worker_default_desired_state),
        )
        worker_registry = InMemoryWorkerRegistry(default_ttl_seconds=config.worker_heartbeat_timeout_seconds)
    return create_api_app(config, registry, control_repository, worker_registry)


__all__ = [
    "TiasInstanceApi",
    "InMemoryTiasRegistry",
    "create_app",
    "create_app_from_config",
]
