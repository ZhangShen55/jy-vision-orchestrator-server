from dataclasses import dataclass
from typing import Iterable, List, Sequence

from app.core.config import VisionOrchestratorConfig
from app.domain.metrics import StudentFrameMetric, TeacherFrameMetric


@dataclass(frozen=True)
class StudentFrameSnapshotInput:
    frame_index: int
    timestamp_seconds: float
    image: object
    metric: StudentFrameMetric


@dataclass(frozen=True)
class TeacherFrameSnapshotInput:
    frame_index: int
    timestamp_seconds: float
    image: object
    metric: TeacherFrameMetric


@dataclass(frozen=True)
class SnapshotEventCandidate:
    target_type: int
    record_type: int
    behavior_type: int
    capture_second: int
    confidence_score: float
    image_id: str
    image: object
    priority: int


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return max(0.0, min(1.0, numerator / denominator))


def _student_event(
        frame: StudentFrameSnapshotInput,
        record_type: int,
        behavior_type: int,
        confidence_score: float,
        priority: int) -> SnapshotEventCandidate:
    return SnapshotEventCandidate(
        target_type=2,
        record_type=record_type,
        behavior_type=behavior_type,
        capture_second=int(frame.timestamp_seconds),
        confidence_score=round(confidence_score, 4),
        image_id=f"student-{behavior_type}-{frame.frame_index:04d}",
        image=frame.image,
        priority=priority,
    )


def _teacher_event(
        frame: TeacherFrameSnapshotInput,
        confidence_score: float,
        priority: int) -> SnapshotEventCandidate:
    return SnapshotEventCandidate(
        target_type=1,
        record_type=1,
        behavior_type=1,
        capture_second=int(frame.timestamp_seconds),
        confidence_score=round(confidence_score, 4),
        image_id=f"teacher-1-{frame.frame_index:04d}",
        image=frame.image,
        priority=priority,
    )


def _top_k_student_events(
        frames: Sequence[StudentFrameSnapshotInput],
        *,
        count_getter,
        top_k: int,
        min_rate: float,
        behavior_type: int,
        priority: int) -> List[SnapshotEventCandidate]:
    if top_k <= 0:
        return []
    scored = []
    for frame in frames:
        metric = frame.metric
        if metric.present_count <= 0:
            continue
        rate = _ratio(count_getter(metric), metric.present_count)
        scored.append((rate, int(frame.timestamp_seconds), frame))
    scored.sort(key=lambda item: (-item[0], item[1]))
    return [
        _student_event(frame, record_type=2, behavior_type=behavior_type, confidence_score=rate, priority=priority)
        for rate, _, frame in scored[:top_k]
        if rate >= min_rate
    ]


def _threshold_student_events(
        frames: Sequence[StudentFrameSnapshotInput],
        *,
        count_getter,
        min_count: int,
        min_rate: float,
        behavior_type: int,
        priority: int) -> List[SnapshotEventCandidate]:
    events = []
    for frame in frames:
        metric = frame.metric
        if metric.present_count <= 0:
            continue
        count = count_getter(metric)
        rate = _ratio(count, metric.present_count)
        if count >= min_count or rate >= min_rate:
            events.append(_student_event(
                frame,
                record_type=1,
                behavior_type=behavior_type,
                confidence_score=rate,
                priority=priority,
            ))
    return sorted(events, key=lambda event: (-event.confidence_score, event.capture_second))


def _is_teacher_alert_frame(frame: TeacherFrameSnapshotInput) -> bool:
    metric = frame.metric
    if not metric.valid_head_pose:
        return False
    return metric.face_direction != "front" or metric.is_looking_down


def _teacher_alert_events(
        frames: Sequence[TeacherFrameSnapshotInput],
        consecutive_frames: int) -> List[SnapshotEventCandidate]:
    if consecutive_frames <= 0:
        return []
    events = []
    window: List[TeacherFrameSnapshotInput] = []
    for frame in frames:
        if _is_teacher_alert_frame(frame):
            window.append(frame)
        else:
            window = []
        if len(window) >= consecutive_frames:
            chosen = window[-1]
            events.append(_teacher_event(chosen, confidence_score=1.0, priority=30))
            window = []
    return events


def _dedupe_same_type(
        events: Iterable[SnapshotEventCandidate],
        min_interval_seconds: int) -> List[SnapshotEventCandidate]:
    if min_interval_seconds <= 0:
        return list(events)
    ordered = sorted(
        events,
        key=lambda event: (
            event.target_type,
            event.behavior_type,
            event.capture_second,
            -event.confidence_score,
        ),
    )
    kept: List[SnapshotEventCandidate] = []
    for event in ordered:
        conflict_index = next((
            index
            for index, kept_event in enumerate(kept)
            if kept_event.target_type == event.target_type
            and kept_event.behavior_type == event.behavior_type
            and abs(kept_event.capture_second - event.capture_second) < min_interval_seconds
        ), None)
        if conflict_index is None:
            kept.append(event)
            continue
        existing = kept[conflict_index]
        if (event.confidence_score, -event.capture_second) > (existing.confidence_score, -existing.capture_second):
            kept[conflict_index] = event
    return kept


def build_snapshot_events(
        task_id: str,
        config: VisionOrchestratorConfig,
        student_frames: Sequence[StudentFrameSnapshotInput],
        teacher_frames: Sequence[TeacherFrameSnapshotInput]) -> List[SnapshotEventCandidate]:
    del task_id
    candidates: List[SnapshotEventCandidate] = []
    candidates.extend(_top_k_student_events(
        student_frames,
        count_getter=lambda metric: metric.face_count,
        top_k=config.snapshot_head_up_top_k,
        min_rate=config.snapshot_head_up_min_rate,
        behavior_type=2,
        priority=10,
    ))
    candidates.extend(_top_k_student_events(
        student_frames,
        count_getter=lambda metric: metric.read_count,
        top_k=config.snapshot_read_top_k,
        min_rate=config.snapshot_read_min_rate,
        behavior_type=3,
        priority=20,
    ))
    candidates.extend(_threshold_student_events(
        student_frames,
        count_getter=lambda metric: metric.sleep_count,
        min_count=config.snapshot_sleep_min_count,
        min_rate=config.snapshot_sleep_min_rate,
        behavior_type=4,
        priority=40,
    ))
    candidates.extend(_threshold_student_events(
        student_frames,
        count_getter=lambda metric: metric.phone_count,
        min_count=config.snapshot_phone_min_count,
        min_rate=config.snapshot_phone_min_rate,
        behavior_type=5,
        priority=50,
    ))
    candidates.extend(_teacher_alert_events(
        teacher_frames,
        config.snapshot_teacher_alert_consecutive_frames,
    ))

    deduped = _dedupe_same_type(candidates, config.snapshot_same_type_min_interval_seconds)
    ranked = sorted(deduped, key=lambda event: (event.priority, -event.confidence_score, event.capture_second))
    if config.snapshot_max_total <= 0:
        return []
    return ranked[:config.snapshot_max_total]
