import time
from dataclasses import dataclass
from typing import Dict, Optional, Set, Tuple

from .registry import TiasInstanceStatus, TiasRegistry


class NoAvailableTiasInstance(RuntimeError):
    """当前没有可调度的 TIAS 实例。"""


@dataclass
class _CircuitState:
    failure_count: int = 0
    opened_at: Optional[float] = None


class TiasScheduler:
    def __init__(
            self,
            registry: TiasRegistry,
            circuit_breaker_failure_threshold: int = 3,
            circuit_breaker_cooldown_seconds: int = 30):
        self.registry = registry
        self.circuit_breaker_failure_threshold = max(1, int(circuit_breaker_failure_threshold))
        self.circuit_breaker_cooldown_seconds = max(1, int(circuit_breaker_cooldown_seconds))
        self._circuits: Dict[str, _CircuitState] = {}
        self._reservations: Dict[str, int] = {}
        self._selection_counts: Dict[str, int] = {}

    def select_instance(
            self,
            required_capability: str,
            excluded_instance_ids: Optional[Set[str]] = None) -> Tuple[TiasInstanceStatus, str]:
        excluded_instance_ids = excluded_instance_ids or set()
        candidates = [
            instance
            for instance in self.registry.list_instances()
            if instance.instance_id not in excluded_instance_ids and self._is_schedulable(instance, required_capability)
        ]
        if not candidates:
            raise NoAvailableTiasInstance(f"没有可用 TIAS 实例 capability={required_capability}")
        candidates.sort(key=self._sort_key)
        selected = candidates[0]
        self._reservations[selected.instance_id] = self._reservations.get(selected.instance_id, 0) + 1
        self._selection_counts[selected.instance_id] = self._selection_counts.get(selected.instance_id, 0) + 1
        reason = (
            f"running_batches={selected.running_batches}, "
            f"local_selection_count={self._selection_counts[selected.instance_id]}, "
            f"avg_latency_ms={selected.avg_latency_ms}, "
            f"p95_latency_ms={selected.p95_latency_ms}, "
            f"queued_batches={selected.queued_batches}, "
            f"recent_failure_count={selected.recent_failure_count}"
        )
        return selected, reason

    def release_reservation(self, instance_id: str) -> None:
        current = self._reservations.get(instance_id, 0)
        if current <= 1:
            self._reservations.pop(instance_id, None)
        else:
            self._reservations[instance_id] = current - 1

    def record_success(self, instance_id: str) -> None:
        self._circuits.pop(instance_id, None)

    def record_failure(self, instance_id: str) -> None:
        state = self._circuits.setdefault(instance_id, _CircuitState())
        state.failure_count += 1
        if state.failure_count >= self.circuit_breaker_failure_threshold:
            state.opened_at = time.time()

    def _is_schedulable(self, instance: TiasInstanceStatus, required_capability: str) -> bool:
        if instance.is_expired():
            return False
        if required_capability not in instance.capabilities:
            return False
        if instance.status not in {"UP", "BUSY"}:
            return False
        if self._is_circuit_open(instance.instance_id):
            return False
        reserved = self._reservations.get(instance.instance_id, 0)
        if instance.running_batches + reserved >= instance.max_concurrent_batches:
            return False
        return True

    def _sort_key(self, instance: TiasInstanceStatus):
        reserved = self._reservations.get(instance.instance_id, 0)
        return (
            instance.running_batches + reserved,
            self._selection_counts.get(instance.instance_id, 0),
            _none_last(instance.avg_latency_ms),
            _none_last(instance.p95_latency_ms),
            instance.queued_batches,
            instance.recent_failure_count,
            instance.instance_id,
        )

    def _is_circuit_open(self, instance_id: str) -> bool:
        state = self._circuits.get(instance_id)
        if state is None or state.opened_at is None:
            return False
        if time.time() - state.opened_at >= self.circuit_breaker_cooldown_seconds:
            self._circuits.pop(instance_id, None)
            return False
        return True


def _none_last(value: Optional[float]) -> float:
    return float(value) if value is not None else 1_000_000_000.0
