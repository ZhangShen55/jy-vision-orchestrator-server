import json
import logging
from typing import Callable, Optional

from app.config import VisionOrchestratorConfig
from app.infrastructure.kafka.message import InvalidTaskMessage, VisualTaskMessage

try:
    from kafka import KafkaConsumer
except ModuleNotFoundError:
    KafkaConsumer = None


logger = logging.getLogger(__name__)


class VisionOrchestratorKafkaConsumer:
    def __init__(self, consumer, max_retries: int = 3):
        self.consumer = consumer
        self.max_retries = max(1, int(max_retries))

    def consume(
            self,
            handler: Callable[[VisualTaskMessage], None],
            invalid_message_handler: Optional[Callable[[object, Exception], None]] = None,
            limit: Optional[int] = None) -> None:
        processed = 0
        for raw_message in self.consumer:
            try:
                task_message = self._parse_message(raw_message)
            except InvalidTaskMessage as exc:
                logger.error(
                    "Kafka 消息不可处理，提交 offset topic=%s partition=%s offset=%s reason=%s",
                    _message_attr(raw_message, "topic"),
                    _message_attr(raw_message, "partition"),
                    _message_attr(raw_message, "offset"),
                    exc,
                )
                if invalid_message_handler is not None:
                    invalid_message_handler(self._raw_value(raw_message), exc)
                self.consumer.commit()
                logger.info(
                    "Kafka offset 已提交 task_id=- status=invalid topic=%s partition=%s offset=%s",
                    _message_attr(raw_message, "topic"),
                    _message_attr(raw_message, "partition"),
                    _message_attr(raw_message, "offset"),
                )
                processed += 1
                if limit is not None and processed >= limit:
                    break
                continue

            logger.info(
                "消费 Kafka 任务 task_id=%s course_id=%s student_count=%s topic=%s partition=%s offset=%s",
                task_message.task_id,
                task_message.course_id,
                task_message.student_count,
                _message_attr(raw_message, "topic"),
                _message_attr(raw_message, "partition"),
                _message_attr(raw_message, "offset"),
            )
            final_status = "failed"
            for attempt in range(1, self.max_retries + 1):
                try:
                    handler(task_message)
                    final_status = "success"
                    break
                except Exception as exc:
                    logger.warning(
                        "视觉分析任务失败 task_id=%s attempt=%s/%s: %s",
                        task_message.task_id,
                        attempt,
                        self.max_retries,
                        exc,
                    )
                    if attempt >= self.max_retries:
                        break
            self.consumer.commit()
            logger.info(
                "Kafka offset 已提交 task_id=%s status=%s topic=%s partition=%s offset=%s",
                task_message.task_id,
                final_status,
                _message_attr(raw_message, "topic"),
                _message_attr(raw_message, "partition"),
                _message_attr(raw_message, "offset"),
            )
            processed += 1
            if limit is not None and processed >= limit:
                break

    @staticmethod
    def _parse_message(raw_message) -> VisualTaskMessage:
        value = VisionOrchestratorKafkaConsumer._raw_value(raw_message)
        if isinstance(value, str):
            value = json.loads(value)
        if not isinstance(value, dict):
            raise InvalidTaskMessage("Kafka message value must be a JSON object")
        return VisualTaskMessage.from_payload(value)

    @staticmethod
    def _raw_value(raw_message):
        value = raw_message.value
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        return value


def create_kafka_consumer(config: VisionOrchestratorConfig):
    if KafkaConsumer is None:
        raise RuntimeError("缺少 kafka-python 依赖，请安装 app/requirements.txt")

    return KafkaConsumer(
        config.kafka_topic,
        bootstrap_servers=config.kafka_bootstrap_servers,
        group_id=config.kafka_group_id,
        enable_auto_commit=False,
        auto_offset_reset=config.kafka_auto_offset_reset,
        max_poll_interval_ms=config.kafka_max_poll_interval_ms,
        max_poll_records=config.kafka_max_poll_records,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def _message_attr(raw_message, name: str):
    return getattr(raw_message, name, None)
