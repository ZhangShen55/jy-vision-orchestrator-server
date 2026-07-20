import time
import unittest

from fastapi.testclient import TestClient

from vision_orchestrator.api.app import create_api_app
from vision_orchestrator.config import VisionOrchestratorConfig
from vision_orchestrator.infrastructure.tias.registry import InMemoryTiasRegistry, TiasInstanceStatus
from vision_orchestrator.infrastructure.worker_control import (
    InMemoryWorkerControlStateRepository,
    WorkerDesiredState,
)
from vision_orchestrator.infrastructure.worker_registry import (
    InMemoryWorkerRegistry,
    WorkerRuntimeState,
    WorkerStatus,
)


class VisionOrchestratorApiServiceTest(unittest.TestCase):
    def build_client(self):
        config = VisionOrchestratorConfig(
            worker_control_enabled=True,
            worker_control_key="secret",
            worker_control_header_name="X-VISION-ORCHESTRATOR-KEY",
            health_check_redis=False,
        )
        tias_registry = InMemoryTiasRegistry(default_ttl_seconds=30)
        control_repository = InMemoryWorkerControlStateRepository()
        worker_registry = InMemoryWorkerRegistry(default_ttl_seconds=30)
        app = create_api_app(
            config=config,
            tias_registry=tias_registry,
            worker_control_repository=control_repository,
            worker_registry=worker_registry,
        )
        return TestClient(app), tias_registry, control_repository, worker_registry

    def test_app_exposes_health_tias_and_worker_routes(self):
        client, _, _, _ = self.build_client()

        response = client.get("/api/health")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["status"], "ok")
        self.assertEqual(body["service"], "vision_orchestrator-api")
        self.assertIn("redis_key_prefix", body)

    def test_tias_instances_can_be_queried(self):
        client, tias_registry, _, _ = self.build_client()
        tias_registry.upsert(TiasInstanceStatus(
            instance_id="tias-a",
            base_url="http://127.0.0.1:8981",
            capabilities=["student_behavior"],
            max_concurrent_batches=2,
            running_batches=1,
            queued_batches=0,
            max_queue_size=0,
            status="UP",
            expires_at=time.time() + 30,
        ))

        list_response = client.get("/api/tias/instances")
        single_response = client.get("/api/tias/instances/tias-a")

        self.assertEqual(list_response.status_code, 200)
        self.assertEqual(list_response.json()["items"][0]["instance_id"], "tias-a")
        self.assertEqual(single_response.status_code, 200)
        self.assertEqual(single_response.json()["running_batches"], 1)
        self.assertEqual(client.get("/api/tias/instances/missing").status_code, 404)

    def test_worker_control_requires_key_and_updates_shared_state(self):
        client, _, control_repository, _ = self.build_client()

        denied = client.post("/api/worker-control/resume", json={"updated_by": "tester"})
        allowed = client.post(
            "/api/worker-control/resume",
            headers={"X-VISION-ORCHESTRATOR-KEY": "secret"},
            json={"updated_by": "tester", "reason": "manual"},
        )

        self.assertIn(denied.status_code, (401, 403))
        self.assertEqual(allowed.status_code, 200)
        self.assertEqual(allowed.json()["desired_state"], "RUNNING")
        self.assertEqual(allowed.json()["version"], 1)
        self.assertEqual(control_repository.get_state().desired_state, WorkerDesiredState.RUNNING)

    def test_worker_registry_can_be_queried(self):
        client, _, _, worker_registry = self.build_client()
        worker_registry.upsert(WorkerStatus(
            worker_id="worker-a",
            actual_state=WorkerRuntimeState.PAUSED,
            desired_state=WorkerDesiredState.PAUSED,
            topic="classroom_cv_task",
            consumer_group="cv-analysis-service",
            assigned_partitions=[],
            current_task_id=None,
            current_partition=None,
            current_offset=None,
            processed_count=0,
            failed_count=0,
            last_error=None,
            started_at="2026-07-01T10:00:00+08:00",
            last_heartbeat_at="2026-07-01T10:00:01+08:00",
            expires_at=time.time() + 30,
        ))

        list_response = client.get("/api/workers")
        single_response = client.get("/api/workers/worker-a")

        self.assertEqual(list_response.status_code, 200)
        self.assertEqual(list_response.json()["items"][0]["worker_id"], "worker-a")
        self.assertEqual(single_response.status_code, 200)
        self.assertEqual(single_response.json()["actual_state"], "PAUSED")
        self.assertEqual(client.get("/api/workers/missing").status_code, 404)


if __name__ == "__main__":
    unittest.main()
