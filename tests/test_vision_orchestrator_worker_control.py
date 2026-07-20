import time
import unittest

from vision_orchestrator.infrastructure.worker_control import (
    InMemoryWorkerControlStateRepository,
    WorkerDesiredState,
)
from vision_orchestrator.infrastructure.worker_registry import (
    InMemoryWorkerRegistry,
    WorkerRuntimeState,
    WorkerStatus,
)


class WorkerControlStateTest(unittest.TestCase):
    def test_default_state_is_paused_and_set_increments_version(self):
        repository = InMemoryWorkerControlStateRepository()

        initial = repository.get_state()
        self.assertEqual(initial.desired_state, WorkerDesiredState.PAUSED)
        self.assertEqual(initial.version, 0)

        updated = repository.set_desired_state(
            WorkerDesiredState.RUNNING,
            updated_by="tester",
            reason="resume for test",
        )

        self.assertEqual(updated.desired_state, WorkerDesiredState.RUNNING)
        self.assertEqual(updated.version, 1)
        self.assertEqual(updated.updated_by, "tester")
        self.assertEqual(updated.reason, "resume for test")


class WorkerRegistryTest(unittest.TestCase):
    def test_registers_heartbeats_and_filters_expired_workers(self):
        registry = InMemoryWorkerRegistry(default_ttl_seconds=1)
        status = WorkerStatus(
            worker_id="worker-a",
            actual_state=WorkerRuntimeState.RUNNING,
            desired_state=WorkerDesiredState.RUNNING,
            topic="classroom_cv_task",
            consumer_group="cv-analysis-service",
            assigned_partitions=[0],
            current_task_id="task-1",
            current_partition=0,
            current_offset=10,
            processed_count=2,
            failed_count=0,
            last_error=None,
            started_at="2026-07-01T10:00:00+08:00",
            last_heartbeat_at="2026-07-01T10:00:01+08:00",
            expires_at=time.time() + 30,
        )

        registry.upsert(status, ttl_seconds=30)

        self.assertEqual(registry.get_worker("worker-a").current_task_id, "task-1")
        self.assertEqual([item.worker_id for item in registry.list_workers()], ["worker-a"])

        registry.upsert(status, ttl_seconds=-1)

        self.assertIsNone(registry.get_worker("worker-a"))
        self.assertEqual(registry.list_workers(), [])


if __name__ == "__main__":
    unittest.main()
