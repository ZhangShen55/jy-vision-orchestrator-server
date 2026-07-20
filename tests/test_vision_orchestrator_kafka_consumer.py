import unittest
from unittest import mock

from app.config import VisionOrchestratorConfig
from app.infrastructure.kafka.consumer import VisionOrchestratorKafkaConsumer
from app.infrastructure.kafka.consumer import create_kafka_consumer


class FakeKafkaConsumer:
    def __init__(self, messages):
        self.messages = messages
        self.commits = 0

    def __iter__(self):
        return iter(self.messages)

    def commit(self):
        self.commits += 1


class FakeMessage:
    def __init__(self, value):
        self.value = value


class VisionOrchestratorKafkaConsumerTest(unittest.TestCase):
    def test_commits_after_successful_handler(self):
        kafka = FakeKafkaConsumer([FakeMessage({"task_id": "task-1", "teacher_video_path": "https://e/t.mp4", "student_video_path": "https://e/s.mp4"})])
        handled = []
        consumer = VisionOrchestratorKafkaConsumer(kafka, max_retries=3)

        consumer.consume(lambda message: handled.append(message.task_id), limit=1)

        self.assertEqual(handled, ["task-1"])
        self.assertEqual(kafka.commits, 1)

    def test_retries_then_commits_after_final_failure(self):
        kafka = FakeKafkaConsumer([FakeMessage({"task_id": "task-1", "teacher_video_path": "https://e/t.mp4", "student_video_path": "https://e/s.mp4"})])
        attempts = []
        consumer = VisionOrchestratorKafkaConsumer(kafka, max_retries=3)

        consumer.consume(lambda message: (_ for _ in ()).throw(RuntimeError(attempts.append(message.task_id))), limit=1)

        self.assertEqual(attempts, ["task-1", "task-1", "task-1"])
        self.assertEqual(kafka.commits, 1)

    def test_invalid_message_is_committed_without_handler(self):
        kafka = FakeKafkaConsumer([FakeMessage({"task_id": "task-1"})])
        invalid_messages = []
        consumer = VisionOrchestratorKafkaConsumer(kafka, max_retries=3)

        consumer.consume(
            lambda message: self.fail("handler should not run"),
            invalid_message_handler=lambda payload, error: invalid_messages.append((payload, str(error))),
            limit=1,
        )

        self.assertEqual(kafka.commits, 1)
        self.assertEqual(invalid_messages[0][0]["task_id"], "task-1")
        self.assertIn("teacher_video", invalid_messages[0][1])

    def test_create_kafka_consumer_uses_long_running_task_poll_settings(self):
        config = VisionOrchestratorConfig(
            kafka_topic="classroom_cv_task",
            kafka_bootstrap_servers="localhost:9092",
            kafka_group_id="cv-analysis-service",
            kafka_auto_offset_reset="latest",
            kafka_max_poll_interval_ms=7200000,
            kafka_max_poll_records=1,
        )

        with mock.patch("app.infrastructure.kafka.consumer.KafkaConsumer") as kafka_consumer:
            create_kafka_consumer(config)

        kafka_consumer.assert_called_once_with(
            "classroom_cv_task",
            bootstrap_servers="localhost:9092",
            group_id="cv-analysis-service",
            enable_auto_commit=False,
            auto_offset_reset="latest",
            max_poll_interval_ms=7200000,
            max_poll_records=1,
            value_deserializer=mock.ANY,
        )


if __name__ == "__main__":
    unittest.main()
