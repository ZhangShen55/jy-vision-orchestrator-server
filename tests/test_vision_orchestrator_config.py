import tempfile
import unittest
from pathlib import Path

from app.core.config import VisionOrchestratorConfig, DependencyCheckError, load_vision_orchestrator_config


class VisionOrchestratorConfigTest(unittest.TestCase):
    def test_load_vision_orchestrator_config_from_toml_section(self):
        with tempfile.NamedTemporaryFile("w", suffix=".toml", delete=False) as config_file:
            config_file.write(
                """
[Vision_Orchestrator]
KafkaBootstrapServers = "192.0.2.10:9092"
KafkaTopic = "classroom_cv_task"
KafkaGroupId = "cv-analysis-service"
KafkaAutoOffsetReset = "latest"
KafkaMaxPollIntervalMs = 7200000
KafkaMaxPollRecords = 1
HttpHost = "0.0.0.0"
HttpPort = 9000
RedisUrl = "redis://127.0.0.1:6379/0"
RedisKeyPrefix = "vision_orchestrator:tias"
HealthCheckRedis = true
WorkerControlEnabled = true
WorkerControlKey = "secret"
WorkerControlHeaderName = "X-VISION-ORCHESTRATOR-KEY"
WorkerControlStateKey = "vision_orchestrator:worker_control:state"
WorkerRegistryKeyPrefix = "vision_orchestrator"
WorkerId = "worker-test"
WorkerControlledByRedis = true
WorkerDefaultDesiredState = "PAUSED"
WorkerHeartbeatIntervalSeconds = 5
WorkerHeartbeatTimeoutSeconds = 30
WorkerPollWhenPausedSeconds = 5
WorkerStopExits = false
TiasInferenceMode = "remote"
TiasBatchSize = 8
TiasRequestTimeoutSeconds = 60
TiasMaxRetryPerBatch = 3
TiasBusyRetryDelaySeconds = 5
TiasCircuitBreakerFailureThreshold = 3
TiasCircuitBreakerCooldownSeconds = 30
TiasHeartbeatTimeoutSeconds = 15
TiasFallbackInstances = ["http://127.0.0.1:8981"]
DBHost = "192.0.2.20"
DBPort = 23308
DBUser = "root"
DBPassword = "test-password"
DBName = "ai_quality"
WriteSnapshotSelectionMode = false
SnapshotMountRoot = "/tmp"
SnapshotRelativePrefix = "cv"
SnapshotScale = 0.25
SnapshotMaxTotal = 30
SnapshotHeadUpTopK = 3
SnapshotHeadUpMinRate = 0.7
SnapshotReadTopK = 3
SnapshotReadMinRate = 0.3
SnapshotSleepMinCount = 2
SnapshotSleepMinRate = 0.05
SnapshotPhoneMinCount = 2
SnapshotPhoneMinRate = 0.05
SnapshotTeacherAlertConsecutiveFrames = 3
SnapshotSameTypeMinIntervalSeconds = 90
BehaviorStatStartMinute = 3
BehaviorStatPeakMaxSegments = 5
TempRoot = "/tmp/vision-orchestrator"
LocalVideoBaseRoot = "/data/course-videos"
FrameIntervalSeconds = 30
MaxTaskRetries = 3
WorkerConcurrency = 1
DefaultStudentCount = 50
MaxFramesPerVideo = 1
"""
            )

        config = load_vision_orchestrator_config(config_file.name)
        Path(config_file.name).unlink()

        self.assertEqual(config.kafka_bootstrap_servers, "192.0.2.10:9092")
        self.assertEqual(config.kafka_topic, "classroom_cv_task")
        self.assertEqual(config.kafka_auto_offset_reset, "latest")
        self.assertEqual(config.kafka_max_poll_interval_ms, 7200000)
        self.assertEqual(config.kafka_max_poll_records, 1)
        self.assertEqual(config.http_host, "0.0.0.0")
        self.assertEqual(config.http_port, 9000)
        self.assertEqual(config.redis_url, "redis://127.0.0.1:6379/0")
        self.assertEqual(config.redis_key_prefix, "vision_orchestrator:tias")
        self.assertTrue(config.health_check_redis)
        self.assertTrue(config.worker_control_enabled)
        self.assertEqual(config.worker_control_key, "secret")
        self.assertEqual(config.worker_control_header_name, "X-VISION-ORCHESTRATOR-KEY")
        self.assertEqual(config.worker_control_state_key, "vision_orchestrator:worker_control:state")
        self.assertEqual(config.worker_registry_key_prefix, "vision_orchestrator")
        self.assertEqual(config.worker_id, "worker-test")
        self.assertTrue(config.worker_controlled_by_redis)
        self.assertEqual(config.worker_default_desired_state, "PAUSED")
        self.assertEqual(config.worker_heartbeat_interval_seconds, 5)
        self.assertEqual(config.worker_heartbeat_timeout_seconds, 30)
        self.assertEqual(config.worker_poll_when_paused_seconds, 5)
        self.assertFalse(config.worker_stop_exits)
        self.assertEqual(config.tias_inference_mode, "remote")
        self.assertEqual(config.tias_batch_size, 8)
        self.assertEqual(config.tias_request_timeout_seconds, 60)
        self.assertEqual(config.tias_max_retry_per_batch, 3)
        self.assertEqual(config.tias_busy_retry_delay_seconds, 5)
        self.assertEqual(config.tias_circuit_breaker_failure_threshold, 3)
        self.assertEqual(config.tias_circuit_breaker_cooldown_seconds, 30)
        self.assertEqual(config.tias_heartbeat_timeout_seconds, 15)
        self.assertEqual(config.tias_fallback_instances, ("http://127.0.0.1:8981",))
        self.assertEqual(config.db_port, 23308)
        self.assertFalse(config.write_snapshot_selection_mode)
        self.assertEqual(config.snapshot_mount_root, Path("/tmp"))
        self.assertEqual(config.snapshot_relative_prefix, "cv")
        self.assertEqual(config.snapshot_max_total, 30)
        self.assertEqual(config.snapshot_head_up_top_k, 3)
        self.assertEqual(config.snapshot_head_up_min_rate, 0.7)
        self.assertEqual(config.snapshot_read_top_k, 3)
        self.assertEqual(config.snapshot_read_min_rate, 0.3)
        self.assertEqual(config.snapshot_sleep_min_count, 2)
        self.assertEqual(config.snapshot_sleep_min_rate, 0.05)
        self.assertEqual(config.snapshot_phone_min_count, 2)
        self.assertEqual(config.snapshot_phone_min_rate, 0.05)
        self.assertEqual(config.snapshot_teacher_alert_consecutive_frames, 3)
        self.assertEqual(config.snapshot_same_type_min_interval_seconds, 90)
        self.assertEqual(config.behavior_stat_start_minute, 3)
        self.assertEqual(config.behavior_stat_peak_max_segments, 5)
        self.assertEqual(config.local_video_base_root, Path("/data/course-videos"))
        self.assertEqual(config.default_student_count, 50)
        self.assertEqual(config.max_frames_per_video, 1)

    def test_config_defaults_are_usable_for_local_development(self):
        config = VisionOrchestratorConfig()

        self.assertEqual(config.kafka_topic, "classroom_cv_task")
        self.assertEqual(config.kafka_group_id, "cv-analysis-service")
        self.assertEqual(config.kafka_auto_offset_reset, "earliest")
        self.assertEqual(config.kafka_max_poll_interval_ms, 7200000)
        self.assertEqual(config.kafka_max_poll_records, 1)
        self.assertEqual(config.http_port, 9000)
        self.assertEqual(config.tias_inference_mode, "remote")
        self.assertEqual(config.tias_batch_size, 8)
        self.assertEqual(config.redis_key_prefix, "vision_orchestrator:tias")
        self.assertFalse(config.health_check_redis)
        self.assertTrue(config.worker_control_enabled)
        self.assertEqual(config.worker_control_key, "change-me")
        self.assertEqual(config.worker_control_header_name, "X-VISION-ORCHESTRATOR-KEY")
        self.assertEqual(config.worker_default_desired_state, "PAUSED")
        self.assertEqual(config.worker_heartbeat_timeout_seconds, 30)
        self.assertEqual(config.db_name, "ai_quality")
        self.assertTrue(config.write_snapshot_selection_mode)
        self.assertEqual(config.frame_interval_seconds, 30)
        self.assertIsNone(config.local_video_base_root)
        self.assertEqual(config.max_task_retries, 3)
        self.assertEqual(config.worker_concurrency, 1)
        self.assertIsNone(config.max_frames_per_video)
        self.assertEqual(config.snapshot_max_total, 30)
        self.assertEqual(config.snapshot_head_up_min_rate, 0.70)
        self.assertEqual(config.snapshot_sleep_min_count, 2)
        self.assertEqual(config.snapshot_phone_min_count, 2)
        self.assertEqual(config.behavior_stat_start_minute, 3)
        self.assertEqual(config.behavior_stat_peak_max_segments, 5)

    def test_dependency_check_reports_missing_module(self):
        with self.assertRaises(DependencyCheckError) as ctx:
            VisionOrchestratorConfig().check_required_modules(["definitely_missing_vision_orchestrator_module"])

        self.assertIn("definitely_missing_vision_orchestrator_module", str(ctx.exception))

    def test_vision_orchestrator_config_does_not_import_tias_config_loader(self):
        config_source = Path(__file__).resolve().parents[1] / "app" / "core" / "config.py"

        self.assertNotIn("tias.core.config_loader", config_source.read_text(encoding="utf-8"))

    def test_vision_orchestrator_dockerfile_keeps_service_boundary(self):
        dockerfile = Path(__file__).resolve().parents[1] / "app" / "docker" / "Dockerfile"
        source = dockerfile.read_text(encoding="utf-8")

        self.assertNotIn("COPY ./tias", source)
        self.assertIn("--keep-source app/api/app.py", source)
        self.assertNotIn("frame_analyzer.py", source)

    def test_source_tree_does_not_depend_on_tias_python_package(self):
        package_root = Path(__file__).resolve().parents[1] / "app"
        local_analyzer = package_root / "infrastructure" / "vision" / "frame_analyzer.py"

        self.assertFalse(local_analyzer.exists())
        for source_path in package_root.rglob("*.py"):
            source = source_path.read_text(encoding="utf-8")
            self.assertNotIn("from tias", source, source_path.as_posix())
            self.assertNotIn("import tias", source, source_path.as_posix())

    def test_loads_legacy_ai_quality_section(self):
        with tempfile.NamedTemporaryFile("w", suffix=".toml", delete=False) as config_file:
            config_file.write(
                """
[AI_Quality]
KafkaTopic = "legacy-topic"
DBName = "ai_quality"
"""
            )

        try:
            config = load_vision_orchestrator_config(config_file.name)
        finally:
            Path(config_file.name).unlink()

        self.assertEqual(config.kafka_topic, "legacy-topic")
        self.assertEqual(config.db_name, "ai_quality")

    def test_loads_toml_example_file(self):
        config_path = Path(__file__).resolve().parents[1] / "app" / "config.toml.example"

        config = load_vision_orchestrator_config(str(config_path))

        self.assertEqual(config.kafka_topic, "classroom_cv_task")
        self.assertEqual(config.snapshot_mount_root, Path("/mnt"))
        self.assertIsNone(config.local_video_base_root)


if __name__ == "__main__":
    unittest.main()
