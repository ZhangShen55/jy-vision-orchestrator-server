import unittest
from unittest import mock

from app.infrastructure.kafka.controlled_consumer import ControlledVisionOrchestratorKafkaConsumer
from app.infrastructure.worker_control import (
    InMemoryWorkerControlStateRepository,
    WorkerDesiredState,
)
from app.infrastructure.worker_registry import InMemoryWorkerRegistry, WorkerRuntimeState


class FakeKafkaConsumer:
    def __init__(self, messages):
        self.messages = list(messages)
        self.iterations = 0
        self.commits = 0

    def __iter__(self):
        self.iterations += 1
        return iter(self.messages)

    def commit(self):
        self.commits += 1


class FakeMessage:
    topic = "classroom_cv_task"
    partition = 0
    offset = 12

    def __init__(self, value):
        self.value = value


class PollableFakeKafkaConsumer:
    def __init__(self, batches):
        self.batches = list(batches)
        self.poll_calls = 0
        self.commits = 0

    def poll(self, timeout_ms=0, max_records=1):
        self.poll_calls += 1
        if not self.batches:
            return {}
        return self.batches.pop(0)

    def commit(self):
        self.commits += 1


class ControlledVisionOrchestratorKafkaConsumerTest(unittest.TestCase):
    def test_paused_state_does_not_poll_kafka_and_keeps_heartbeat(self):
        kafka = FakeKafkaConsumer([FakeMessage({
            "task_id": "task-1",
            "teacher_video_path": "https://e/t.mp4",
            "student_video_path": "https://e/s.mp4",
        })])
        control_repository = InMemoryWorkerControlStateRepository(default_state=WorkerDesiredState.PAUSED)
        worker_registry = InMemoryWorkerRegistry()
        consumer = ControlledVisionOrchestratorKafkaConsumer(
            consumer=kafka,
            worker_id="worker-a",
            control_repository=control_repository,
            worker_registry=worker_registry,
            topic="classroom_cv_task",
            consumer_group="cv-analysis-service",
        )

        result = consumer.run_once(lambda message: self.fail("handler should not run"))

        self.assertEqual(result, WorkerRuntimeState.PAUSED)
        self.assertEqual(kafka.iterations, 0)
        self.assertEqual(kafka.commits, 0)
        self.assertEqual(worker_registry.get_worker("worker-a").actual_state, WorkerRuntimeState.PAUSED)

    def test_running_state_processes_one_message_and_commits_offset(self):
        kafka = FakeKafkaConsumer([FakeMessage({
            "task_id": "task-1",
            "teacher_video_path": "https://e/t.mp4",
            "student_video_path": "https://e/s.mp4",
            "course_id": "course-1",
        })])
        control_repository = InMemoryWorkerControlStateRepository(default_state=WorkerDesiredState.RUNNING)
        worker_registry = InMemoryWorkerRegistry()
        handled = []
        consumer = ControlledVisionOrchestratorKafkaConsumer(
            consumer=kafka,
            worker_id="worker-a",
            control_repository=control_repository,
            worker_registry=worker_registry,
            topic="classroom_cv_task",
            consumer_group="cv-analysis-service",
        )

        result = consumer.run_once(lambda message: handled.append(message.task_id))

        self.assertEqual(result, WorkerRuntimeState.RUNNING)
        self.assertEqual(handled, ["task-1"])
        self.assertEqual(kafka.commits, 1)
        self.assertIsNone(worker_registry.get_worker("worker-a").current_task_id)
        self.assertEqual(worker_registry.get_worker("worker-a").processed_count, 1)

    def test_drain_during_current_task_finishes_then_pauses(self):
        kafka = FakeKafkaConsumer([FakeMessage({
            "task_id": "task-1",
            "teacher_video_path": "https://e/t.mp4",
            "student_video_path": "https://e/s.mp4",
        })])
        control_repository = InMemoryWorkerControlStateRepository(default_state=WorkerDesiredState.RUNNING)
        worker_registry = InMemoryWorkerRegistry()
        consumer = ControlledVisionOrchestratorKafkaConsumer(
            consumer=kafka,
            worker_id="worker-a",
            control_repository=control_repository,
            worker_registry=worker_registry,
            topic="classroom_cv_task",
            consumer_group="cv-analysis-service",
        )

        def handler(_message):
            control_repository.set_desired_state(WorkerDesiredState.DRAINING, updated_by="tester")

        result = consumer.run_once(handler)

        self.assertEqual(result, WorkerRuntimeState.PAUSED)
        self.assertEqual(kafka.commits, 1)
        self.assertEqual(worker_registry.get_worker("worker-a").actual_state, WorkerRuntimeState.PAUSED)
        self.assertIsNone(worker_registry.get_worker("worker-a").current_task_id)

    def test_handler_can_refresh_heartbeat_during_long_task(self):
        kafka = FakeKafkaConsumer([FakeMessage({
            "task_id": "task-1",
            "teacher_video_path": "https://e/t.mp4",
            "student_video_path": "https://e/s.mp4",
        })])
        control_repository = InMemoryWorkerControlStateRepository(default_state=WorkerDesiredState.RUNNING)
        worker_registry = InMemoryWorkerRegistry()
        consumer = ControlledVisionOrchestratorKafkaConsumer(
            consumer=kafka,
            worker_id="worker-a",
            control_repository=control_repository,
            worker_registry=worker_registry,
            topic="classroom_cv_task",
            consumer_group="cv-analysis-service",
        )

        def handler(_message, heartbeat):
            heartbeat()
            self.assertEqual(worker_registry.get_worker("worker-a").current_task_id, "task-1")

        consumer.run_once(handler)

        self.assertEqual(worker_registry.get_worker("worker-a").processed_count, 1)

    def test_running_state_poll_timeout_returns_to_control_loop(self):
        kafka = PollableFakeKafkaConsumer([{}])
        control_repository = InMemoryWorkerControlStateRepository(default_state=WorkerDesiredState.RUNNING)
        worker_registry = InMemoryWorkerRegistry()
        consumer = ControlledVisionOrchestratorKafkaConsumer(
            consumer=kafka,
            worker_id="worker-a",
            control_repository=control_repository,
            worker_registry=worker_registry,
            topic="classroom_cv_task",
            consumer_group="cv-analysis-service",
            poll_timeout_ms=10,
        )

        result = consumer.run_once(lambda message: self.fail("handler should not run"))

        self.assertEqual(result, WorkerRuntimeState.RUNNING)
        self.assertEqual(kafka.poll_calls, 1)
        self.assertEqual(kafka.commits, 0)
        self.assertEqual(worker_registry.get_worker("worker-a").actual_state, WorkerRuntimeState.RUNNING)

    def test_stopped_state_can_keep_process_alive_without_polling(self):
        kafka = PollableFakeKafkaConsumer([{}])
        control_repository = InMemoryWorkerControlStateRepository(default_state=WorkerDesiredState.STOPPED)
        worker_registry = InMemoryWorkerRegistry()
        consumer = ControlledVisionOrchestratorKafkaConsumer(
            consumer=kafka,
            worker_id="worker-a",
            control_repository=control_repository,
            worker_registry=worker_registry,
            topic="classroom_cv_task",
            consumer_group="cv-analysis-service",
            stop_exits=False,
        )
        calls = {"count": 0}

        def run_once(_handler):
            calls["count"] += 1
            if calls["count"] >= 2:
                raise KeyboardInterrupt()
            return WorkerRuntimeState.STOPPED

        with mock.patch.object(consumer, "run_once", side_effect=run_once), \
                mock.patch("time.sleep", return_value=None):
            with self.assertRaises(KeyboardInterrupt):
                consumer.run_forever(lambda message: self.fail("handler should not run"), sleep_seconds=1)

        self.assertEqual(kafka.poll_calls, 0)

    def test_stopped_state_exits_when_configured(self):
        kafka = PollableFakeKafkaConsumer([{}])
        control_repository = InMemoryWorkerControlStateRepository(default_state=WorkerDesiredState.STOPPED)
        worker_registry = InMemoryWorkerRegistry()
        consumer = ControlledVisionOrchestratorKafkaConsumer(
            consumer=kafka,
            worker_id="worker-a",
            control_repository=control_repository,
            worker_registry=worker_registry,
            topic="classroom_cv_task",
            consumer_group="cv-analysis-service",
            stop_exits=True,
        )

        consumer.run_forever(lambda message: self.fail("handler should not run"), sleep_seconds=1)

        self.assertEqual(kafka.poll_calls, 0)
        self.assertEqual(worker_registry.get_worker("worker-a").actual_state, WorkerRuntimeState.STOPPED)


if __name__ == "__main__":
    unittest.main()
