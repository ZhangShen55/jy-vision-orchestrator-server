import time
import unittest

from app.infrastructure.tias.registry import InMemoryTiasRegistry, TiasInstanceStatus
from app.infrastructure.tias.scheduler import NoAvailableTiasInstance, TiasScheduler


def _instance(
        instance_id,
        running,
        avg_latency=100,
        p95_latency=150,
        capabilities=None,
        status="UP",
        recent_failure_count=0):
    return TiasInstanceStatus(
        instance_id=instance_id,
        base_url=f"http://127.0.0.1:{8980 + running}",
        capabilities=capabilities or ["student_behavior", "teacher_behavior", "teacher_head_pose"],
        max_concurrent_batches=2,
        running_batches=running,
        queued_batches=0,
        max_queue_size=0,
        status=status,
        avg_latency_ms=avg_latency,
        p95_latency_ms=p95_latency,
        recent_failure_count=recent_failure_count,
        expires_at=time.time() + 60,
    )


class TiasSchedulerTest(unittest.TestCase):
    def test_selects_lowest_running_then_latency_instance(self):
        registry = InMemoryTiasRegistry(default_ttl_seconds=60)
        registry.upsert(_instance("tias-slow", running=0, avg_latency=200))
        registry.upsert(_instance("tias-fast", running=0, avg_latency=80))
        scheduler = TiasScheduler(registry)

        selected, reason = scheduler.select_instance("student_behavior")

        self.assertEqual(selected.instance_id, "tias-fast")
        self.assertIn("running_batches=0", reason)

    def test_rotates_idle_instances_before_reusing_low_latency_instance(self):
        registry = InMemoryTiasRegistry(default_ttl_seconds=60)
        registry.upsert(_instance("tias-fast", running=0, avg_latency=80))
        registry.upsert(_instance("tias-cold", running=0, avg_latency=None))
        scheduler = TiasScheduler(registry)

        first, _ = scheduler.select_instance("student_behavior")
        scheduler.release_reservation(first.instance_id)
        second, _ = scheduler.select_instance("student_behavior")

        self.assertEqual(first.instance_id, "tias-fast")
        self.assertEqual(second.instance_id, "tias-cold")

    def test_filters_missing_capability_and_full_instances(self):
        registry = InMemoryTiasRegistry(default_ttl_seconds=60)
        registry.upsert(_instance("tias-teacher-only", running=0, capabilities=["teacher_behavior"]))
        registry.upsert(_instance("tias-full", running=2, capabilities=["student_behavior"]))
        registry.upsert(_instance("tias-ok", running=1, capabilities=["student_behavior"]))
        scheduler = TiasScheduler(registry)

        selected, _ = scheduler.select_instance("student_behavior")

        self.assertEqual(selected.instance_id, "tias-ok")

    def test_raises_when_all_instances_busy(self):
        registry = InMemoryTiasRegistry(default_ttl_seconds=60)
        registry.upsert(_instance("tias-full", running=2, capabilities=["student_behavior"]))
        scheduler = TiasScheduler(registry)

        with self.assertRaises(NoAvailableTiasInstance):
            scheduler.select_instance("student_behavior")

    def test_circuit_breaker_skips_failed_instance_until_cooldown(self):
        registry = InMemoryTiasRegistry(default_ttl_seconds=60)
        registry.upsert(_instance("tias-a", running=0))
        registry.upsert(_instance("tias-b", running=1))
        scheduler = TiasScheduler(
            registry,
            circuit_breaker_failure_threshold=1,
            circuit_breaker_cooldown_seconds=60,
        )

        scheduler.record_failure("tias-a")
        selected, _ = scheduler.select_instance("student_behavior")

        self.assertEqual(selected.instance_id, "tias-b")

    def test_select_instance_can_exclude_retry_failed_instance(self):
        registry = InMemoryTiasRegistry(default_ttl_seconds=60)
        registry.upsert(_instance("tias-a", running=0))
        registry.upsert(_instance("tias-b", running=0, avg_latency=120))
        scheduler = TiasScheduler(registry)

        selected, _ = scheduler.select_instance("student_behavior")
        scheduler.release_reservation(selected.instance_id)
        retry_selected, _ = scheduler.select_instance(
            "student_behavior",
            excluded_instance_ids={selected.instance_id},
        )

        self.assertNotEqual(retry_selected.instance_id, selected.instance_id)


if __name__ == "__main__":
    unittest.main()
