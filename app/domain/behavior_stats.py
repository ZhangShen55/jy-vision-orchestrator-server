from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Tuple

from app.domain.metrics import StudentFrameMetric


PHONE_BEHAVIOR_TYPE = 1
SLEEP_BEHAVIOR_TYPE = 3
DEFAULT_CONFIDENCE_LEVEL = 2


@dataclass(frozen=True)
class StudentBehaviorStat:
    behavior_type: int
    detect_count: int
    peak_period_desc: str
    confidence_level: int = DEFAULT_CONFIDENCE_LEVEL


@dataclass(frozen=True)
class _Segment:
    start_minute: int
    end_minute: int
    total: int
    peak: int

    @property
    def length(self) -> int:
        return self.end_minute - self.start_minute + 1


def build_student_behavior_stats(
        frames: Iterable[StudentFrameMetric],
        start_minute: int = 3,
        peak_max_segments: int = 5) -> List[StudentBehaviorStat]:
    frame_list = [
        frame for frame in frames
        if frame.present_count > 0 and frame.minute_no >= start_minute
    ]
    stats: List[StudentBehaviorStat] = []
    for behavior_type, extractor in (
            (PHONE_BEHAVIOR_TYPE, lambda frame: frame.phone_count),
            (SLEEP_BEHAVIOR_TYPE, lambda frame: frame.sleep_count)):
        stat = _build_single_behavior_stat(frame_list, behavior_type, extractor, peak_max_segments)
        if stat is not None:
            stats.append(stat)
    return stats


def _build_single_behavior_stat(
        frames: Iterable[StudentFrameMetric],
        behavior_type: int,
        count_extractor: Callable[[StudentFrameMetric], int],
        peak_max_segments: int) -> StudentBehaviorStat | None:
    minute_counts = _aggregate_minute_counts(frames, count_extractor)
    detect_count = sum(minute_counts.values())
    if detect_count <= 0:
        return None
    segments = _build_segments(minute_counts)
    peak_period_desc = _format_peak_period_desc(segments, peak_max_segments)
    return StudentBehaviorStat(
        behavior_type=behavior_type,
        detect_count=detect_count,
        peak_period_desc=peak_period_desc,
    )


def _aggregate_minute_counts(
        frames: Iterable[StudentFrameMetric],
        count_extractor: Callable[[StudentFrameMetric], int]) -> Dict[int, int]:
    minute_counts: Dict[int, int] = {}
    for frame in frames:
        count = max(0, int(count_extractor(frame)))
        if count <= 0:
            continue
        minute_counts[frame.minute_no] = minute_counts.get(frame.minute_no, 0) + count
    return minute_counts


def _build_segments(minute_counts: Dict[int, int]) -> List[_Segment]:
    segments: List[_Segment] = []
    current_minutes: List[Tuple[int, int]] = []
    previous_minute: int | None = None
    for minute in sorted(minute_counts):
        count = minute_counts[minute]
        if previous_minute is None or minute == previous_minute + 1:
            current_minutes.append((minute, count))
        else:
            segments.append(_segment_from_minutes(current_minutes))
            current_minutes = [(minute, count)]
        previous_minute = minute
    if current_minutes:
        segments.append(_segment_from_minutes(current_minutes))
    return segments


def _segment_from_minutes(minutes: List[Tuple[int, int]]) -> _Segment:
    return _Segment(
        start_minute=minutes[0][0],
        end_minute=minutes[-1][0],
        total=sum(count for _, count in minutes),
        peak=max(count for _, count in minutes),
    )


def _format_peak_period_desc(segments: List[_Segment], peak_max_segments: int) -> str:
    if peak_max_segments <= 0:
        return ""
    selected = sorted(
        segments,
        key=lambda segment: (
            -segment.total,
            -segment.peak,
            -segment.length,
            segment.start_minute,
        ),
    )[:peak_max_segments]
    selected = sorted(selected, key=lambda segment: segment.start_minute)
    return "、".join(_format_segment(segment) for segment in selected)


def _format_segment(segment: _Segment) -> str:
    if segment.start_minute == segment.end_minute:
        return f"{segment.start_minute}′"
    return f"{segment.start_minute}′–{segment.end_minute}′"
