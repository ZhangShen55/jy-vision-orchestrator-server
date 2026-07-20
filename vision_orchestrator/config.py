import importlib.util
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

from vision_orchestrator.config_loader import load_config


class DependencyCheckError(RuntimeError):
    """运行 Worker 所需依赖缺失。"""


def _get_value(config: Mapping[str, object], key: str, default):
    return config.get(key, default)


@dataclass(frozen=True)
class VisionOrchestratorConfig:
    kafka_bootstrap_servers: str = "127.0.0.1:9092"
    kafka_topic: str = "classroom_cv_task"
    kafka_group_id: str = "cv-analysis-service"
    kafka_auto_offset_reset: str = "earliest"
    kafka_max_poll_interval_ms: int = 7200000
    kafka_max_poll_records: int = 1
    http_host: str = "0.0.0.0"
    http_port: int = 9000
    redis_url: str = "redis://127.0.0.1:6379/0"
    redis_key_prefix: str = "vision_orchestrator:tias"
    health_check_redis: bool = False
    worker_control_enabled: bool = True
    worker_control_key: str = "change-me"
    worker_control_header_name: str = "X-VISION-ORCHESTRATOR-KEY"
    worker_control_state_key: str = "vision_orchestrator:worker_control:state"
    worker_registry_key_prefix: str = "vision_orchestrator"
    worker_id: str = ""
    worker_controlled_by_redis: bool = True
    worker_default_desired_state: str = "PAUSED"
    worker_heartbeat_interval_seconds: int = 5
    worker_heartbeat_timeout_seconds: int = 30
    worker_poll_when_paused_seconds: int = 5
    worker_stop_exits: bool = False
    tias_inference_mode: str = "remote"
    tias_batch_size: int = 8
    tias_request_timeout_seconds: int = 60
    tias_max_retry_per_batch: int = 3
    tias_busy_retry_delay_seconds: int = 5
    tias_circuit_breaker_failure_threshold: int = 3
    tias_circuit_breaker_cooldown_seconds: int = 30
    tias_heartbeat_timeout_seconds: int = 15
    tias_fallback_instances: tuple[str, ...] = ()
    db_host: str = "127.0.0.1"
    db_port: int = 3306
    db_user: str = "root"
    db_password: str = ""
    db_name: str = "ai_quality"
    snapshot_mount_root: Path = Path("/mnt")
    snapshot_relative_prefix: str = "cv"
    snapshot_scale: float = 0.25
    snapshot_max_total: int = 30
    snapshot_head_up_top_k: int = 3
    snapshot_head_up_min_rate: float = 0.70
    snapshot_read_top_k: int = 3
    snapshot_read_min_rate: float = 0.30
    snapshot_sleep_min_count: int = 2
    snapshot_sleep_min_rate: float = 0.05
    snapshot_phone_min_count: int = 2
    snapshot_phone_min_rate: float = 0.05
    snapshot_teacher_alert_consecutive_frames: int = 3
    snapshot_same_type_min_interval_seconds: int = 90
    behavior_stat_start_minute: int = 3
    behavior_stat_peak_max_segments: int = 5
    temp_root: Path = Path("/tmp/vision-orchestrator")
    local_video_base_root: Path | None = None
    frame_interval_seconds: int = 30
    max_task_retries: int = 3
    worker_concurrency: int = 1
    default_student_count: int = 50
    max_frames_per_video: int | None = None

    def check_required_modules(self, module_names: Iterable[str] = ()) -> None:
        missing = [
            module_name
            for module_name in module_names
            if importlib.util.find_spec(module_name) is None
        ]
        if missing:
            raise DependencyCheckError("缺少运行依赖: " + ", ".join(missing))

    def ensure_runtime_dependencies(self) -> None:
        modules = ["cv2", "requests", "pymysql", "kafka"]
        if self.tias_inference_mode == "remote" and not self.tias_fallback_instances:
            modules.append("redis")
        self.check_required_modules(modules)


def load_vision_orchestrator_config(config_path: str) -> VisionOrchestratorConfig:
    raw_config = load_config(config_path)
    section = raw_config.get("Vision_Orchestrator")
    if section is None:
        section = raw_config.get("AI_Quality", {})
    if not isinstance(section, Mapping):
        section = {}

    fallback_instances = _get_value(section, "TiasFallbackInstances", VisionOrchestratorConfig.tias_fallback_instances)
    if isinstance(fallback_instances, str):
        fallback_instances = tuple(
            item.strip()
            for item in fallback_instances.split(",")
            if item.strip()
        )
    else:
        fallback_instances = tuple(str(item).strip() for item in fallback_instances if str(item).strip())

    return VisionOrchestratorConfig(
        kafka_bootstrap_servers=str(_get_value(section, "KafkaBootstrapServers", VisionOrchestratorConfig.kafka_bootstrap_servers)),
        kafka_topic=str(_get_value(section, "KafkaTopic", VisionOrchestratorConfig.kafka_topic)),
        kafka_group_id=str(_get_value(section, "KafkaGroupId", VisionOrchestratorConfig.kafka_group_id)),
        kafka_auto_offset_reset=str(_get_value(
            section,
            "KafkaAutoOffsetReset",
            VisionOrchestratorConfig.kafka_auto_offset_reset,
        )),
        kafka_max_poll_interval_ms=int(_get_value(
            section,
            "KafkaMaxPollIntervalMs",
            VisionOrchestratorConfig.kafka_max_poll_interval_ms,
        )),
        kafka_max_poll_records=int(_get_value(
            section,
            "KafkaMaxPollRecords",
            VisionOrchestratorConfig.kafka_max_poll_records,
        )),
        http_host=str(_get_value(section, "HttpHost", VisionOrchestratorConfig.http_host)),
        http_port=int(_get_value(section, "HttpPort", VisionOrchestratorConfig.http_port)),
        redis_url=str(_get_value(section, "RedisUrl", VisionOrchestratorConfig.redis_url)),
        redis_key_prefix=str(_get_value(section, "RedisKeyPrefix", VisionOrchestratorConfig.redis_key_prefix)),
        health_check_redis=_to_bool(_get_value(section, "HealthCheckRedis", VisionOrchestratorConfig.health_check_redis)),
        worker_control_enabled=_to_bool(_get_value(
            section,
            "WorkerControlEnabled",
            VisionOrchestratorConfig.worker_control_enabled,
        )),
        worker_control_key=str(_get_value(section, "WorkerControlKey", VisionOrchestratorConfig.worker_control_key)),
        worker_control_header_name=str(_get_value(
            section,
            "WorkerControlHeaderName",
            VisionOrchestratorConfig.worker_control_header_name,
        )),
        worker_control_state_key=str(_get_value(
            section,
            "WorkerControlStateKey",
            VisionOrchestratorConfig.worker_control_state_key,
        )),
        worker_registry_key_prefix=str(_get_value(
            section,
            "WorkerRegistryKeyPrefix",
            VisionOrchestratorConfig.worker_registry_key_prefix,
        )),
        worker_id=str(_get_value(section, "WorkerId", VisionOrchestratorConfig.worker_id)),
        worker_controlled_by_redis=_to_bool(_get_value(
            section,
            "WorkerControlledByRedis",
            VisionOrchestratorConfig.worker_controlled_by_redis,
        )),
        worker_default_desired_state=str(_get_value(
            section,
            "WorkerDefaultDesiredState",
            VisionOrchestratorConfig.worker_default_desired_state,
        )),
        worker_heartbeat_interval_seconds=int(_get_value(
            section,
            "WorkerHeartbeatIntervalSeconds",
            VisionOrchestratorConfig.worker_heartbeat_interval_seconds,
        )),
        worker_heartbeat_timeout_seconds=int(_get_value(
            section,
            "WorkerHeartbeatTimeoutSeconds",
            VisionOrchestratorConfig.worker_heartbeat_timeout_seconds,
        )),
        worker_poll_when_paused_seconds=int(_get_value(
            section,
            "WorkerPollWhenPausedSeconds",
            VisionOrchestratorConfig.worker_poll_when_paused_seconds,
        )),
        worker_stop_exits=_to_bool(_get_value(section, "WorkerStopExits", VisionOrchestratorConfig.worker_stop_exits)),
        tias_inference_mode=str(_get_value(section, "TiasInferenceMode", VisionOrchestratorConfig.tias_inference_mode)),
        tias_batch_size=int(_get_value(section, "TiasBatchSize", VisionOrchestratorConfig.tias_batch_size)),
        tias_request_timeout_seconds=int(_get_value(
            section,
            "TiasRequestTimeoutSeconds",
            VisionOrchestratorConfig.tias_request_timeout_seconds,
        )),
        tias_max_retry_per_batch=int(_get_value(section, "TiasMaxRetryPerBatch", VisionOrchestratorConfig.tias_max_retry_per_batch)),
        tias_busy_retry_delay_seconds=int(_get_value(
            section,
            "TiasBusyRetryDelaySeconds",
            VisionOrchestratorConfig.tias_busy_retry_delay_seconds,
        )),
        tias_circuit_breaker_failure_threshold=int(_get_value(
            section,
            "TiasCircuitBreakerFailureThreshold",
            VisionOrchestratorConfig.tias_circuit_breaker_failure_threshold,
        )),
        tias_circuit_breaker_cooldown_seconds=int(_get_value(
            section,
            "TiasCircuitBreakerCooldownSeconds",
            VisionOrchestratorConfig.tias_circuit_breaker_cooldown_seconds,
        )),
        tias_heartbeat_timeout_seconds=int(_get_value(
            section,
            "TiasHeartbeatTimeoutSeconds",
            VisionOrchestratorConfig.tias_heartbeat_timeout_seconds,
        )),
        tias_fallback_instances=fallback_instances,
        db_host=str(_get_value(section, "DBHost", VisionOrchestratorConfig.db_host)),
        db_port=int(_get_value(section, "DBPort", VisionOrchestratorConfig.db_port)),
        db_user=str(_get_value(section, "DBUser", VisionOrchestratorConfig.db_user)),
        db_password=str(_get_value(section, "DBPassword", VisionOrchestratorConfig.db_password)),
        db_name=str(_get_value(section, "DBName", VisionOrchestratorConfig.db_name)),
        snapshot_mount_root=Path(str(_get_value(section, "SnapshotMountRoot", VisionOrchestratorConfig.snapshot_mount_root))),
        snapshot_relative_prefix=str(_get_value(section, "SnapshotRelativePrefix", VisionOrchestratorConfig.snapshot_relative_prefix)),
        snapshot_scale=float(_get_value(section, "SnapshotScale", VisionOrchestratorConfig.snapshot_scale)),
        snapshot_max_total=int(_get_value(section, "SnapshotMaxTotal", VisionOrchestratorConfig.snapshot_max_total)),
        snapshot_head_up_top_k=int(_get_value(section, "SnapshotHeadUpTopK", VisionOrchestratorConfig.snapshot_head_up_top_k)),
        snapshot_head_up_min_rate=float(_get_value(section, "SnapshotHeadUpMinRate", VisionOrchestratorConfig.snapshot_head_up_min_rate)),
        snapshot_read_top_k=int(_get_value(section, "SnapshotReadTopK", VisionOrchestratorConfig.snapshot_read_top_k)),
        snapshot_read_min_rate=float(_get_value(section, "SnapshotReadMinRate", VisionOrchestratorConfig.snapshot_read_min_rate)),
        snapshot_sleep_min_count=int(_get_value(section, "SnapshotSleepMinCount", VisionOrchestratorConfig.snapshot_sleep_min_count)),
        snapshot_sleep_min_rate=float(_get_value(section, "SnapshotSleepMinRate", VisionOrchestratorConfig.snapshot_sleep_min_rate)),
        snapshot_phone_min_count=int(_get_value(section, "SnapshotPhoneMinCount", VisionOrchestratorConfig.snapshot_phone_min_count)),
        snapshot_phone_min_rate=float(_get_value(section, "SnapshotPhoneMinRate", VisionOrchestratorConfig.snapshot_phone_min_rate)),
        snapshot_teacher_alert_consecutive_frames=int(_get_value(
            section,
            "SnapshotTeacherAlertConsecutiveFrames",
            VisionOrchestratorConfig.snapshot_teacher_alert_consecutive_frames,
        )),
        snapshot_same_type_min_interval_seconds=int(_get_value(
            section,
            "SnapshotSameTypeMinIntervalSeconds",
            VisionOrchestratorConfig.snapshot_same_type_min_interval_seconds,
        )),
        behavior_stat_start_minute=int(_get_value(
            section,
            "BehaviorStatStartMinute",
            VisionOrchestratorConfig.behavior_stat_start_minute,
        )),
        behavior_stat_peak_max_segments=int(_get_value(
            section,
            "BehaviorStatPeakMaxSegments",
            VisionOrchestratorConfig.behavior_stat_peak_max_segments,
        )),
        temp_root=Path(str(_get_value(section, "TempRoot", VisionOrchestratorConfig.temp_root))),
        local_video_base_root=_optional_path(_get_value(
            section,
            "LocalVideoBaseRoot",
            VisionOrchestratorConfig.local_video_base_root,
        )),
        frame_interval_seconds=int(_get_value(section, "FrameIntervalSeconds", VisionOrchestratorConfig.frame_interval_seconds)),
        max_task_retries=int(_get_value(section, "MaxTaskRetries", VisionOrchestratorConfig.max_task_retries)),
        worker_concurrency=int(_get_value(section, "WorkerConcurrency", VisionOrchestratorConfig.worker_concurrency)),
        default_student_count=int(_get_value(section, "DefaultStudentCount", VisionOrchestratorConfig.default_student_count)),
        max_frames_per_video=(
            int(section["MaxFramesPerVideo"])
            if section.get("MaxFramesPerVideo") not in (None, "", 0, "0")
            else None
        ),
    )


def _to_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _optional_path(value) -> Path | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return Path(text)
