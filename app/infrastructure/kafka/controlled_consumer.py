import logging
import inspect
from datetime import datetime, timezone
from typing import Callable, Optional

from app.infrastructure.kafka.consumer import VisionOrchestratorKafkaConsumer, _message_attr
from app.infrastructure.kafka.message import VisualTaskMessage
from app.infrastructure.worker_control import (
    WorkerControlState,
    WorkerControlStateRepository,
    WorkerDesiredState,
)
from app.infrastructure.worker_registry import (
    WorkerRegistry,
    WorkerRuntimeState,
    build_worker_status,
)


logger = logging.getLogger(__name__)


class ControlledVisionOrchestratorKafkaConsumer:
    def __init__(
            self,
            consumer,
            worker_id: str,
            control_repository: WorkerControlStateRepository,
            worker_registry: WorkerRegistry,
            topic: str,
            consumer_group: str,
            max_retries: int = 3,
            heartbeat_ttl_seconds: int = 30,
            poll_timeout_ms: int = 1000,
            stop_exits: bool = False):
        self.consumer = consumer
        self.worker_id = worker_id
        self.control_repository = control_repository
        self.worker_registry = worker_registry
        self.topic = topic
        self.consumer_group = consumer_group
        self.max_retries = max(1, int(max_retries))
        self.heartbeat_ttl_seconds = int(heartbeat_ttl_seconds)
        self.poll_timeout_ms = max(1, int(poll_timeout_ms))
        self.stop_exits = bool(stop_exits)
        self.started_at = _now_iso()
        self.processed_count = 0
        self.failed_count = 0
        self.last_error: Optional[str] = None
        self._last_seen_control_version: Optional[int] = None
        self._last_actual_state: Optional[WorkerRuntimeState] = None

    def run_once(self, handler: Callable[[VisualTaskMessage], None]) -> WorkerRuntimeState:
        control_state = self.control_repository.get_state()
        desired_state = control_state.desired_state
        if desired_state == WorkerDesiredState.STOPPED:
            self._log_control_state(control_state, WorkerRuntimeState.STOPPED, None)
            self._heartbeat(WorkerRuntimeState.STOPPED, desired_state)
            return WorkerRuntimeState.STOPPED
        if desired_state in (WorkerDesiredState.PAUSED, WorkerDesiredState.DRAINING):
            self._log_control_state(control_state, WorkerRuntimeState.PAUSED, None)
            self._heartbeat(WorkerRuntimeState.PAUSED, desired_state)
            return WorkerRuntimeState.PAUSED

        self._log_control_state(control_state, WorkerRuntimeState.RUNNING, None)
        self._heartbeat(WorkerRuntimeState.RUNNING, desired_state)
        processed = self._consume_one(handler)
        next_control_state = self.control_repository.get_state()
        next_desired_state = next_control_state.desired_state
        if next_desired_state == WorkerDesiredState.DRAINING:
            self._log_control_state(next_control_state, WorkerRuntimeState.PAUSED, None)
            self._heartbeat(WorkerRuntimeState.PAUSED, next_desired_state)
            return WorkerRuntimeState.PAUSED
        if next_desired_state == WorkerDesiredState.STOPPED:
            self._log_control_state(next_control_state, WorkerRuntimeState.STOPPED, None)
            self._heartbeat(WorkerRuntimeState.STOPPED, next_desired_state)
            return WorkerRuntimeState.STOPPED
        runtime_state = WorkerRuntimeState.RUNNING if processed else WorkerRuntimeState.RUNNING
        self._log_control_state(next_control_state, runtime_state, None)
        self._heartbeat(runtime_state, next_desired_state)
        return runtime_state

    def run_forever(self, handler: Callable[[VisualTaskMessage], None], sleep_seconds: int = 5) -> None:
        import time

        while True:
            state = self.run_once(handler)
            if state == WorkerRuntimeState.STOPPED and self.stop_exits:
                return
            if state in (WorkerRuntimeState.PAUSED, WorkerRuntimeState.STOPPED):
                time.sleep(max(1, int(sleep_seconds)))

    def _consume_one(self, handler: Callable[[VisualTaskMessage], None]) -> bool:
        for raw_message in self._iter_messages_once():
            task_message = VisionOrchestratorKafkaConsumer._parse_message(raw_message)
            self._heartbeat(
                WorkerRuntimeState.RUNNING,
                self.control_repository.get_state().desired_state,
                current_task_id=task_message.task_id,
                current_partition=_message_attr(raw_message, "partition"),
                current_offset=_message_attr(raw_message, "offset"),
            )
            final_status = "failed"
            for attempt in range(1, self.max_retries + 1):
                try:
                    logger.info(
                        "消费 Kafka 任务 worker_id=%s task_id=%s course_id=%s student_count=%s topic=%s partition=%s offset=%s",
                        self.worker_id,
                        task_message.task_id,
                        task_message.course_id,
                        task_message.student_count,
                        _message_attr(raw_message, "topic"),
                        _message_attr(raw_message, "partition"),
                        _message_attr(raw_message, "offset"),
                    )
                    self._call_handler(
                        handler,
                        task_message,
                        lambda: self._heartbeat(
                            WorkerRuntimeState.RUNNING,
                            self.control_repository.get_state().desired_state,
                            current_task_id=task_message.task_id,
                            current_partition=_message_attr(raw_message, "partition"),
                            current_offset=_message_attr(raw_message, "offset"),
                        ),
                    )
                    self.processed_count += 1
                    self.last_error = None
                    final_status = "success"
                    break
                except Exception as exc:
                    self.failed_count += 1
                    self.last_error = str(exc)
                    logger.warning(
                        "视觉分析任务失败 worker_id=%s task_id=%s attempt=%s/%s reason=%s",
                        self.worker_id,
                        task_message.task_id,
                        attempt,
                        self.max_retries,
                        exc,
                    )
                    if attempt >= self.max_retries:
                        break
            self.consumer.commit()
            logger.info(
                "Kafka offset 已提交 worker_id=%s task_id=%s status=%s topic=%s partition=%s offset=%s",
                self.worker_id,
                task_message.task_id,
                final_status,
                _message_attr(raw_message, "topic"),
                _message_attr(raw_message, "partition"),
                _message_attr(raw_message, "offset"),
            )
            self._heartbeat(
                WorkerRuntimeState.RUNNING,
                self.control_repository.get_state().desired_state,
            )
            return True
        return False

    def _iter_messages_once(self):
        if hasattr(self.consumer, "poll"):
            records = self.consumer.poll(timeout_ms=self.poll_timeout_ms, max_records=1)
            if not records:
                return []
            messages = []
            for partition_records in records.values():
                messages.extend(partition_records)
            return messages[:1]
        for raw_message in self.consumer:
            return [raw_message]
        return []

    @staticmethod
    def _call_handler(handler: Callable, task_message: VisualTaskMessage, heartbeat: Callable[[], None]) -> None:
        try:
            signature = inspect.signature(handler)
            if len(signature.parameters) >= 2:
                handler(task_message, heartbeat)
                return
        except (TypeError, ValueError):
            pass
        handler(task_message)

    def _heartbeat(
            self,
            actual_state: WorkerRuntimeState,
            desired_state: WorkerDesiredState,
            current_task_id: Optional[str] = None,
            current_partition: Optional[int] = None,
            current_offset: Optional[int] = None) -> None:
        self.worker_registry.upsert(
            build_worker_status(
                worker_id=self.worker_id,
                actual_state=actual_state,
                desired_state=desired_state,
                topic=self.topic,
                consumer_group=self.consumer_group,
                started_at=self.started_at,
                assigned_partitions=self._assigned_partitions(),
                current_task_id=current_task_id,
                current_partition=current_partition,
                current_offset=current_offset,
                processed_count=self.processed_count,
                failed_count=self.failed_count,
                last_error=self.last_error,
            ),
            ttl_seconds=self.heartbeat_ttl_seconds,
        )

    def _assigned_partitions(self) -> list[int]:
        if not hasattr(self.consumer, "assignment"):
            return []
        try:
            return sorted(int(item.partition) for item in self.consumer.assignment())
        except Exception:
            return []

    def _log_control_state(
            self,
            control_state: WorkerControlState,
            actual_state: WorkerRuntimeState,
            current_task_id: Optional[str]) -> None:
        if self._last_seen_control_version == control_state.version:
            self._last_actual_state = actual_state
            return
        logger.info(
            "Worker 控制状态变化 worker_id=%s old_actual_state=%s desired_state=%s version=%s current_task_id=%s",
            self.worker_id,
            self._last_actual_state.value if self._last_actual_state else "-",
            control_state.desired_state.value,
            control_state.version,
            current_task_id or "-",
        )
        self._last_seen_control_version = control_state.version
        self._last_actual_state = actual_state


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
