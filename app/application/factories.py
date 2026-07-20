from app.application.worker import VisualAnalysisWorker
from app.config import load_vision_orchestrator_config
from app.infrastructure.db.connection import create_mysql_connection
from app.infrastructure.db.repositories import VisionOrchestratorRepository
from app.infrastructure.media.snapshot_storage import SnapshotStorage
from app.infrastructure.tias.registry import InMemoryTiasRegistry, RedisTiasRegistry, TiasInstanceStatus
from app.infrastructure.tias.scheduler import TiasScheduler
from app.infrastructure.vision.remote_frame_analyzer import RemoteFrameAnalyzer


def build_worker(config_path: str) -> VisualAnalysisWorker:
    config = load_vision_orchestrator_config(config_path)
    config.ensure_runtime_dependencies()
    storage = SnapshotStorage(config.snapshot_mount_root, config.snapshot_relative_prefix, config.snapshot_scale)
    storage.ensure_writable()
    connection = create_mysql_connection(config)
    repository = VisionOrchestratorRepository(connection)
    return VisualAnalysisWorker(
        config=config,
        repository=repository,
        snapshot_storage=storage,
        frame_analyzer=build_frame_analyzer(config),
    )


def build_frame_analyzer(config):
    if config.tias_inference_mode != "remote":
        raise RuntimeError("vision_orchestrator 独立服务仅支持 TiasInferenceMode=remote")
    try:
        registry = RedisTiasRegistry(
            redis_url=config.redis_url,
            key_prefix=config.redis_key_prefix,
            default_ttl_seconds=config.tias_heartbeat_timeout_seconds,
        )
    except RuntimeError:
        if not config.tias_fallback_instances:
            raise
        registry = InMemoryTiasRegistry(
            key_prefix=config.redis_key_prefix,
            default_ttl_seconds=config.tias_heartbeat_timeout_seconds,
        )
        for index, base_url in enumerate(config.tias_fallback_instances, start=1):
            registry.upsert(TiasInstanceStatus(
                instance_id=f"fallback-tias-{index}",
                base_url=base_url,
                capabilities=["student_behavior", "teacher_behavior", "teacher_head_pose"],
                max_concurrent_batches=1,
                running_batches=0,
                queued_batches=0,
                max_queue_size=0,
                status="UP",
            ))
    scheduler = TiasScheduler(
        registry,
        circuit_breaker_failure_threshold=config.tias_circuit_breaker_failure_threshold,
        circuit_breaker_cooldown_seconds=config.tias_circuit_breaker_cooldown_seconds,
    )
    return RemoteFrameAnalyzer(config, scheduler)
