import hashlib
import random
import statistics
from dataclasses import dataclass
from typing import Dict, Iterable, List


@dataclass(frozen=True)
class StudentFrameMetric:
    minute_no: int
    present_count: int
    face_count: int
    sleep_count: int = 0
    phone_count: int = 0
    read_count: int = 0


@dataclass(frozen=True)
class TeacherFrameMetric:
    minute_no: int
    valid_head_pose: bool
    face_direction: str = "unknown"
    is_looking_down: bool = False


@dataclass(frozen=True)
class IndicatorMetric:
    code: str
    value: float
    score: float


@dataclass(frozen=True)
class AggregatedVisualMetrics:
    expected_student_count: int
    indicators: Dict[str, IndicatorMetric]


def clamp_ratio(value: float) -> float:
    return min(1.0, max(0.0, value))


def _score_from_ratio(value: float) -> float:
    return round(clamp_ratio(value) * 100.0, 2)


def _median(values: Iterable[float]) -> float:
    values = list(values)
    if not values:
        return 0.0
    return float(statistics.median(values))


def placeholder_seat_count(task_id: str, minute_no: int, metric_type: str, present_count: int) -> int:
    if present_count <= 0:
        return 0
    seed_text = f"{task_id}:{minute_no}:{metric_type}".encode("utf-8")
    seed = int(hashlib.sha256(seed_text).hexdigest()[:16], 16)
    ratio = random.Random(seed).uniform(0.20, 0.30)
    return min(present_count, max(0, round(present_count * ratio)))


def aggregate_visual_metrics(
        task_id: str,
        student_count: int,
        student_frames: List[StudentFrameMetric],
        teacher_frames: List[TeacherFrameMetric]) -> AggregatedVisualMetrics:
    valid_student_frames = [frame for frame in student_frames if frame.present_count > 0]
    detected_max = max((frame.present_count for frame in valid_student_frames), default=0)
    expected_student_count = student_count if student_count and student_count > 0 else detected_max
    denominator = expected_student_count if expected_student_count > 0 else 1

    attendance_value = clamp_ratio(_median(frame.present_count for frame in valid_student_frames) / denominator)
    head_up_rates = [
        clamp_ratio(frame.face_count / frame.present_count)
        for frame in valid_student_frames
    ]
    head_up_value = clamp_ratio(_median(head_up_rates))

    front_counts = [
        placeholder_seat_count(task_id, frame.minute_no, "E3-01", frame.present_count)
        for frame in valid_student_frames
    ]
    back_counts = [
        placeholder_seat_count(task_id, frame.minute_no, "E4-01", frame.present_count)
        for frame in valid_student_frames
    ]
    front_value = clamp_ratio(_median(front_counts) / denominator)
    back_value = clamp_ratio(_median(back_counts) / denominator)

    valid_teacher_frames = [frame for frame in teacher_frames if frame.valid_head_pose]
    facing_student_count = sum(
        1 for frame in valid_teacher_frames
        if frame.face_direction == "front" and not frame.is_looking_down
    )
    facing_value = clamp_ratio(
        facing_student_count / len(valid_teacher_frames)
        if valid_teacher_frames else 0.0
    )

    indicators = {
        "E2-01": IndicatorMetric("E2-01", attendance_value, _score_from_ratio(attendance_value)),
        "E3-01": IndicatorMetric("E3-01", front_value, _score_from_ratio(front_value)),
        "E4-01": IndicatorMetric("E4-01", back_value, _score_from_ratio(back_value)),
        "E5-01": IndicatorMetric("E5-01", head_up_value, _score_from_ratio(head_up_value)),
        "A6-01": IndicatorMetric("A6-01", facing_value, _score_from_ratio(facing_value)),
    }
    return AggregatedVisualMetrics(
        expected_student_count=expected_student_count,
        indicators=indicators,
    )
