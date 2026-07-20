import inspect
import logging
import shutil
from pathlib import Path
from typing import List, Optional

from app.application.constants import INDICATOR_CODES
from app.core.config import VisionOrchestratorConfig
from app.domain.behavior_stats import build_student_behavior_stats
from app.domain.metrics import StudentFrameMetric, TeacherFrameMetric, aggregate_visual_metrics
from app.domain.scoring import score_indicator
from app.domain.snapshots import (
    StudentFrameSnapshotInput,
    TeacherFrameSnapshotInput,
    build_snapshot_events,
)
from app.infrastructure.db.repositories import VisionOrchestratorRepository
from app.infrastructure.kafka.message import VisualTaskMessage
from app.infrastructure.media.snapshot_storage import SnapshotStorage
from app.infrastructure.media.video import extract_frames, prepare_video_source, validate_video_source


logger = logging.getLogger(__name__)


class VisualAnalysisWorker:
    def __init__(
            self,
            config: VisionOrchestratorConfig,
            repository: VisionOrchestratorRepository,
            frame_analyzer: Optional[object] = None,
            snapshot_storage: Optional[SnapshotStorage] = None):
        self.config = config
        self.repository = repository
        self.frame_analyzer = frame_analyzer or self._default_frame_analyzer()
        self.snapshot_storage = snapshot_storage or SnapshotStorage(
            config.snapshot_mount_root,
            config.snapshot_relative_prefix,
            config.snapshot_scale,
        )
        self.worker_id = ""

    def set_worker_id(self, worker_id: str) -> None:
        self.worker_id = worker_id
        if hasattr(self.frame_analyzer, "set_worker_id"):
            self.frame_analyzer.set_worker_id(worker_id)

    def process_task(self, message: VisualTaskMessage, heartbeat=None) -> None:
        task_dir = self.config.temp_root / message.task_id
        try:
            self.repository.mark_workflow_running(message.task_id)
            self.repository.clear_previous_results(message.task_id)
            task_dir.mkdir(parents=True, exist_ok=True)
            self._heartbeat(heartbeat)

            if message.slides_video_path:
                slides_source = validate_video_source(
                    message.slides_video_path,
                    local_base_root=self.config.local_video_base_root,
                )
                logger.info(
                    "课件视频来源校验完成 task_id=%s source_type=%s source=%s",
                    message.task_id,
                    slides_source.source_type,
                    slides_source.path or slides_source.source,
                )

            student_video = prepare_video_source(
                message.student_video_path,
                task_dir / "student.mp4",
                local_base_root=self.config.local_video_base_root,
                progress_callback=lambda: self._heartbeat(heartbeat),
            )
            teacher_video = prepare_video_source(
                message.teacher_video_path,
                task_dir / "teacher.mp4",
                local_base_root=self.config.local_video_base_root,
                progress_callback=lambda: self._heartbeat(heartbeat),
            )
            logger.info(
                "视频资源准备完成 task_id=%s student_source_type=%s student_path=%s teacher_source_type=%s teacher_path=%s",
                message.task_id,
                student_video.source_type,
                student_video.path,
                teacher_video.source_type,
                teacher_video.path,
            )
            student_frames = extract_frames(
                student_video.path,
                self.config.frame_interval_seconds,
                progress_callback=lambda: self._heartbeat(heartbeat),
            )
            teacher_frames = extract_frames(
                teacher_video.path,
                self.config.frame_interval_seconds,
                progress_callback=lambda: self._heartbeat(heartbeat),
            )
            if self.config.max_frames_per_video is not None:
                student_frames = student_frames[:self.config.max_frames_per_video]
                teacher_frames = teacher_frames[:self.config.max_frames_per_video]
            self._heartbeat(heartbeat)

            student_metrics: List[StudentFrameMetric] = []
            teacher_metrics: List[TeacherFrameMetric] = []
            student_snapshot_inputs: List[StudentFrameSnapshotInput] = []
            teacher_snapshot_inputs: List[TeacherFrameSnapshotInput] = []
            timeline_rows = []
            snapshot_rows = []

            if hasattr(self.frame_analyzer, "analyze_student_frames"):
                student_metrics_result = self._call_batch_analyzer(
                    self.frame_analyzer.analyze_student_frames,
                    message.task_id,
                    student_frames,
                    heartbeat,
                )
            else:
                student_metrics_result = [
                    self.frame_analyzer.analyze_student_frame(frame.point.minute_no, frame.image)
                    for frame in student_frames
                ]
            student_metrics = list(student_metrics_result)
            self._heartbeat(heartbeat)
            for frame, metric in zip(student_frames, student_metrics):
                student_snapshot_inputs.append(StudentFrameSnapshotInput(
                    frame_index=frame.point.frame_index,
                    timestamp_seconds=frame.point.timestamp_seconds,
                    image=frame.image,
                    metric=metric,
                ))
                if metric.present_count > 0:
                    timeline_rows.append({
                        "metric_type": 3,
                        "minute_no": metric.minute_no,
                        "metric_value": round(metric.face_count / metric.present_count * 100, 2),
                    })

            if hasattr(self.frame_analyzer, "analyze_teacher_frames"):
                teacher_metrics_result = self._call_batch_analyzer(
                    self.frame_analyzer.analyze_teacher_frames,
                    message.task_id,
                    teacher_frames,
                    heartbeat,
                )
            else:
                teacher_metrics_result = [
                    self.frame_analyzer.analyze_teacher_frame(frame.point.minute_no, frame.image)
                    for frame in teacher_frames
                ]
            teacher_metrics = list(teacher_metrics_result)
            self._heartbeat(heartbeat)
            for frame, metric in zip(teacher_frames, teacher_metrics):
                teacher_snapshot_inputs.append(TeacherFrameSnapshotInput(
                    frame_index=frame.point.frame_index,
                    timestamp_seconds=frame.point.timestamp_seconds,
                    image=frame.image,
                    metric=metric,
                ))

            for event in build_snapshot_events(
                    message.task_id,
                    self.config,
                    student_snapshot_inputs,
                    teacher_snapshot_inputs):
                snapshot = self.snapshot_storage.save_snapshot(message.task_id, event.image_id, event.image)
                snapshot_rows.append({
                    "target_type": event.target_type,
                    "record_type": event.record_type,
                    "behavior_type": event.behavior_type,
                    "capture_second": event.capture_second,
                    "confidence_score": event.confidence_score,
                    "image_url": snapshot.relative_path,
                })

            aggregated = aggregate_visual_metrics(
                task_id=message.task_id,
                student_count=message.student_count,
                student_frames=student_metrics,
                teacher_frames=teacher_metrics,
            )
            student_behavior_stats = build_student_behavior_stats(
                student_metrics,
                start_minute=self.config.behavior_stat_start_minute,
                peak_max_segments=self.config.behavior_stat_peak_max_segments,
            )
            indicator_definitions = self.repository.load_indicator_definitions(INDICATOR_CODES)
            rescored_indicators = {}
            for code, metric in aggregated.indicators.items():
                definition = indicator_definitions.get(code)
                rule = definition.score_rule if definition else None
                rescored_indicators[code] = type(metric)(
                    code=metric.code,
                    value=metric.value,
                    score=score_indicator(metric.value, rule),
                )

            self.repository.insert_timeline_rows(message.task_id, timeline_rows)
            self.repository.insert_snapshot_events(message.task_id, snapshot_rows)
            self.repository.upsert_student_behavior_stats(message.task_id, student_behavior_stats)
            self.repository.upsert_indicator_results(
                message.task_id,
                rescored_indicators,
                indicator_definitions,
            )
            self.repository.mark_workflow_success(message.task_id)
            self._heartbeat(heartbeat)
        except Exception as exc:
            try:
                self.repository.mark_workflow_failed(message.task_id, str(exc))
            except Exception as status_exc:
                logger.exception(
                    "任务失败状态写入失败 task_id=%s original_error_type=%s original_reason=%s status_error_type=%s status_reason=%s",
                    message.task_id,
                    type(exc).__name__,
                    exc,
                    type(status_exc).__name__,
                    status_exc,
                )
            raise
        finally:
            shutil.rmtree(task_dir, ignore_errors=True)

    @staticmethod
    def _heartbeat(heartbeat) -> None:
        if heartbeat is not None:
            heartbeat()

    @staticmethod
    def _call_batch_analyzer(analyzer, task_id: str, frames, heartbeat):
        try:
            signature = inspect.signature(analyzer)
            if "heartbeat" in signature.parameters:
                return analyzer(task_id, frames, heartbeat=heartbeat)
        except (TypeError, ValueError):
            pass
        return analyzer(task_id, frames)

    @staticmethod
    def _default_frame_analyzer():
        raise RuntimeError("必须显式提供远程 TIAS 帧分析器")
