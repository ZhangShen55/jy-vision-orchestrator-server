import unittest

from app.core.bootstrap import TiasInstanceApi, create_app
from app.infrastructure.tias.registry import InMemoryTiasRegistry


class VisionOrchestratorHttpApiTest(unittest.TestCase):
    def test_create_app_exposes_tias_routes(self):
        app = create_app(InMemoryTiasRegistry())
        paths = {route.path for route in app.routes}

        self.assertIn("/api/tias/instances/register", paths)
        self.assertIn("/api/tias/instances/heartbeat", paths)
        self.assertIn("/api/tias/instances/unregister", paths)

    def test_tias_register_heartbeat_and_unregister(self):
        registry = InMemoryTiasRegistry(default_ttl_seconds=15)
        api = TiasInstanceApi(registry)
        payload = {
            "instance_id": "tias-a",
            "base_url": "http://127.0.0.1:8981",
            "capabilities": ["student_behavior"],
            "max_concurrent_batches": 1,
            "running_batches": 0,
            "queued_batches": 0,
            "max_queue_size": 0,
            "status": "UP",
            "heartbeat_timeout_seconds": 15,
        }

        register_response = api.register(payload)
        self.assertEqual(register_response["status"], "ok")
        self.assertEqual(len(registry.list_instances()), 1)

        payload["running_batches"] = 1
        heartbeat_response = api.heartbeat(payload)
        self.assertEqual(heartbeat_response["status"], "ok")
        self.assertEqual(registry.list_instances()[0].running_batches, 1)

        unregister_response = api.unregister({
            "instance_id": "tias-a",
            "base_url": "http://127.0.0.1:8981",
            "status": "DOWN",
        })
        self.assertEqual(unregister_response["status"], "ok")
        self.assertEqual(registry.list_instances(), [])


if __name__ == "__main__":
    unittest.main()
