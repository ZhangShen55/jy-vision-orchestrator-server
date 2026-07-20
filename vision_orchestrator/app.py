import argparse
import json
import logging
import os
import socket
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

from vision_orchestrator.config import load_vision_orchestrator_config
from vision_orchestrator.http_app import create_app_from_config
from vision_orchestrator.infrastructure.kafka.controlled_consumer import ControlledVisionOrchestratorKafkaConsumer
from vision_orchestrator.infrastructure.kafka.consumer import VisionOrchestratorKafkaConsumer, create_kafka_consumer
from vision_orchestrator.infrastructure.kafka.message import VisualTaskMessage
from vision_orchestrator.infrastructure.worker_control import RedisWorkerControlStateRepository, WorkerDesiredState
from vision_orchestrator.infrastructure.worker_registry import RedisWorkerRegistry


if TYPE_CHECKING:
    from vision_orchestrator.application.worker import VisualAnalysisWorker


logger = logging.getLogger(__name__)


def load_message_from_json_arg(value: str) -> VisualTaskMessage:
    candidate_path = Path(value)
    if candidate_path.exists():
        payload = json.loads(candidate_path.read_text(encoding="utf-8"))
    else:
        payload = json.loads(value)
    return VisualTaskMessage.from_payload(payload)


def run_single_json(config_path: str, message_json: str) -> None:
    from vision_orchestrator.application.factories import build_worker

    worker = build_worker(config_path)
    message = load_message_from_json_arg(message_json)
    worker.process_task(message)


def consume(config_path: str) -> None:
    from vision_orchestrator.application.factories import build_worker

    config = load_vision_orchestrator_config(config_path)
    config.ensure_runtime_dependencies()
    worker = build_worker(config_path)
    kafka_consumer = VisionOrchestratorKafkaConsumer(
        create_kafka_consumer(config),
        max_retries=config.max_task_retries,
    )
    kafka_consumer.consume(
        worker.process_task,
        invalid_message_handler=lambda payload, error: handle_invalid_message(worker, payload, error),
    )


def worker(config_path: str) -> None:
    from vision_orchestrator.application.factories import build_worker

    config = load_vision_orchestrator_config(config_path)
    config.ensure_runtime_dependencies()
    worker_id = resolve_worker_id(config.worker_id)
    visual_worker = build_worker(config_path)
    visual_worker.set_worker_id(worker_id)
    kafka_consumer = ControlledVisionOrchestratorKafkaConsumer(
        create_kafka_consumer(config),
        worker_id=worker_id,
        control_repository=RedisWorkerControlStateRepository(
            redis_url=config.redis_url,
            state_key=config.worker_control_state_key,
            default_state=WorkerDesiredState(config.worker_default_desired_state),
        ),
        worker_registry=RedisWorkerRegistry(
            redis_url=config.redis_url,
            key_prefix=config.worker_registry_key_prefix,
            default_ttl_seconds=config.worker_heartbeat_timeout_seconds,
        ),
        topic=config.kafka_topic,
        consumer_group=config.kafka_group_id,
        max_retries=config.max_task_retries,
        heartbeat_ttl_seconds=config.worker_heartbeat_timeout_seconds,
        poll_timeout_ms=max(1000, config.worker_heartbeat_interval_seconds * 1000),
        stop_exits=config.worker_stop_exits,
    )
    logger.info(
        "vision_orchestrator Worker 启动 worker_id=%s topic=%s group=%s default_state=%s",
        worker_id,
        config.kafka_topic,
        config.kafka_group_id,
        config.worker_default_desired_state,
    )
    kafka_consumer.run_forever(
        visual_worker.process_task,
        sleep_seconds=config.worker_poll_when_paused_seconds,
    )


def serve(config_path: str) -> None:
    import uvicorn

    config = load_vision_orchestrator_config(config_path)
    app = create_app_from_config(config)
    uvicorn.run(app, host=config.http_host, port=config.http_port)


def handle_invalid_message(worker: "VisualAnalysisWorker", payload, error: Exception) -> None:
    if not isinstance(payload, dict):
        return
    task_id = payload.get("task_id") or payload.get("taskId") or payload.get("taskID")
    if not task_id:
        return
    error_msg = str(error)
    worker.repository.mark_workflow_failed(str(task_id), error_msg)


def resolve_worker_id(configured_worker_id: str | None = None) -> str:
    configured_worker_id = (configured_worker_id or "").strip()
    if configured_worker_id:
        return configured_worker_id
    env_worker_id = os.getenv("VISION_ORCHESTRATOR_WORKER_ID", "").strip()
    if env_worker_id:
        return env_worker_id
    legacy_worker_id = os.getenv("AI_QUALITY_WORKER_ID", "").strip()
    if legacy_worker_id:
        return legacy_worker_id
    return f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"


def main() -> None:
    parser = argparse.ArgumentParser(description="课堂视觉任务编排服务")
    parser.add_argument(
        "--config",
        default=os.getenv("CONFIG_PATH", "vision_orchestrator/config.toml"),
        help="配置文件路径，默认读取 CONFIG_PATH 或 vision_orchestrator/config.toml",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    consume_parser = subparsers.add_parser("consume", help="从 Kafka 消费视觉分析任务")
    consume_parser.set_defaults(func=lambda args: consume(args.config))

    worker_parser = subparsers.add_parser("worker", help="启动受 Redis 控制的 vision_orchestrator Kafka Worker")
    worker_parser.set_defaults(func=lambda args: worker(args.config))

    serve_parser = subparsers.add_parser("serve", help="启动 vision_orchestrator HTTP 注册和心跳服务")
    serve_parser.set_defaults(func=lambda args: serve(args.config))

    run_json_parser = subparsers.add_parser("run-json", help="使用 JSON 字符串或 JSON 文件模拟一条 Kafka 消息")
    run_json_parser.add_argument("message_json", help="JSON 字符串或 JSON 文件路径")
    run_json_parser.set_defaults(func=lambda args: run_single_json(args.config, args.message_json))

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args.func(args)


if __name__ == "__main__":
    main()
