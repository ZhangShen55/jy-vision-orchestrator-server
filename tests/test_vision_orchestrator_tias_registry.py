import time
import unittest

from app.infrastructure.tias.registry import InMemoryTiasRegistry, TiasInstanceStatus


class TiasRegistryTest(unittest.TestCase):
    def test_upserts_registration_and_filters_expired_instances(self):
        registry = InMemoryTiasRegistry(key_prefix="test:tias", default_ttl_seconds=1)
        instance = TiasInstanceStatus(
            instance_id="tias-a",
            base_url="http://127.0.0.1:8981",
            capabilities=["student_behavior"],
            max_concurrent_batches=1,
            running_batches=0,
            queued_batches=0,
            max_queue_size=0,
            status="UP",
            expires_at=time.time() + 30,
        )

        registry.upsert(instance, ttl_seconds=30)

        self.assertEqual([item.instance_id for item in registry.list_instances()], ["tias-a"])

        registry.upsert(instance, ttl_seconds=-1)

        self.assertEqual(registry.list_instances(), [])

    def test_unregister_removes_schedulable_instance(self):
        registry = InMemoryTiasRegistry(key_prefix="test:tias", default_ttl_seconds=15)
        registry.upsert(TiasInstanceStatus(
            instance_id="tias-a",
            base_url="http://127.0.0.1:8981",
            capabilities=["student_behavior"],
            max_concurrent_batches=1,
            running_batches=0,
            queued_batches=0,
            max_queue_size=0,
            status="UP",
            expires_at=time.time() + 15,
        ))

        registry.unregister("tias-a")

        self.assertEqual(registry.list_instances(), [])


if __name__ == "__main__":
    unittest.main()
