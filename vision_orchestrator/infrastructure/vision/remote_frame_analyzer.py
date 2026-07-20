import base64
import logging
import time
from typing import Dict, Iterable, List

import cv2

from vision_orchestrator.config import VisionOrchestratorConfig
from vision_orchestrator.domain.metrics import StudentFrameMetric, TeacherFrameMetric
from vision_orchestrator.infrastructure.media.video import ExtractedFrame
from vision_orchestrator.infrastructure.tias.client import TiasHttpClient, TiasHttpError
from vision_orchestrator.infrastructure.tias.scheduler import NoAvailableTiasInstance, TiasScheduler


logger = logging.getLogger(__name__)

STUDENT_OBJECT_TYPES = {
    100: "present_count",
    101: "face_count",
    201: "phone_count",
    202: "sleep_count",
    205: "read_count",
}


class RemoteFrameAnalyzer:
    def __init__(
            self,
            config: VisionOrchestratorConfig,
            scheduler: TiasScheduler,
            client: TiasHttpClient | None = None):
        self.config = config
        self.scheduler = scheduler
        self.client = client or TiasHttpClient(config.tias_request_timeout_seconds)
        self.worker_id = ""

    def set_worker_id(self, worker_id: str) -> None:
        self.worker_id = worker_id

    def analyze_student_frames(
            self,
            task_id: str,
            frames: List[ExtractedFrame],
            heartbeat=None) -> List[StudentFrameMetric]:
        results: Dict[int, StudentFrameMetric] = {}
        for batch_index, batch in enumerate(_chunks(frames, self.config.tias_batch_size), start=1):
            batch_id = f"{task_id}-student-{batch_index:04d}"
            payload = self._build_payload(task_id, batch_id, "student", batch)
            _heartbeat(heartbeat)
            response = self._dispatch("student_behavior", task_id, batch_id, "student", payload)
            _heartbeat(heartbeat)
            for item in response.get("DataList", []):
                frame_index = _frame_index_from_image_id(item.get("StatusObject", {}).get("ImageId"))
                if frame_index is None:
                    raise RuntimeError(f"TIAS 学生响应缺少帧身份 batch_id={batch_id}")
                counts = self._student_counts(item)
                source_frame = _find_frame(batch, frame_index)
                results[frame_index] = StudentFrameMetric(
                    minute_no=source_frame.point.minute_no,
                    present_count=counts.get("present_count", 0),
                    face_count=counts.get("face_count", 0),
                    sleep_count=counts.get("sleep_count", 0),
                    phone_count=counts.get("phone_count", 0),
                    read_count=counts.get("read_count", 0),
                )
            self._ensure_complete(batch, results, batch_id)
        return [results[frame.point.frame_index] for frame in sorted(frames, key=lambda item: item.point.frame_index)]

    def analyze_teacher_frames(
            self,
            task_id: str,
            frames: List[ExtractedFrame],
            heartbeat=None) -> List[TeacherFrameMetric]:
        results: Dict[int, TeacherFrameMetric] = {}
        for batch_index, batch in enumerate(_chunks(frames, self.config.tias_batch_size), start=1):
            batch_id = f"{task_id}-teacher-{batch_index:04d}"
            payload = self._build_payload(task_id, batch_id, "teacher", batch)
            payload["ReturnHeadPose"] = True
            _heartbeat(heartbeat)
            response = self._dispatch("teacher_behavior", task_id, batch_id, "teacher", payload)
            _heartbeat(heartbeat)
            for item in response.get("DataList", []):
                frame_index = _frame_index_from_image_id(item.get("StatusObject", {}).get("ImageId"))
                if frame_index is None:
                    raise RuntimeError(f"TIAS 教师响应缺少帧身份 batch_id={batch_id}")
                source_frame = _find_frame(batch, frame_index)
                head_pose = item.get("HeadPoseResult") or {}
                valid = head_pose.get("Status") == "success"
                results[frame_index] = TeacherFrameMetric(
                    minute_no=source_frame.point.minute_no,
                    valid_head_pose=valid,
                    face_direction=head_pose.get("FaceDirection") or "unknown",
                    is_looking_down=bool(head_pose.get("IsLookingDown", False)),
                )
            self._ensure_complete(batch, results, batch_id)
        return [results[frame.point.frame_index] for frame in sorted(frames, key=lambda item: item.point.frame_index)]

    def _dispatch(self, capability: str, task_id: str, batch_id: str, stream_type: str, payload: dict) -> dict:
        last_error: Exception | None = None
        excluded_instance_ids: set[str] = set()
        for attempt in range(1, self.config.tias_max_retry_per_batch + 1):
            instance = None
            started_at = time.monotonic()
            try:
                instance, reason = self.scheduler.select_instance(
                    capability,
                    excluded_instance_ids=excluded_instance_ids,
                )
                logger.info(
                    "选择 TIAS 实例 worker_id=%s task_id=%s batch_id=%s stream_type=%s instance_id=%s reason=%s",
                    self.worker_id or "-",
                    task_id,
                    batch_id,
                    stream_type,
                    instance.instance_id,
                    reason,
                )
                if stream_type == "student":
                    response = self.client.infer_student(instance, payload)
                else:
                    response = self.client.infer_teacher(instance, payload)
                duration_ms = int((time.monotonic() - started_at) * 1000)
                logger.info(
                    "TIAS 批次调用完成 worker_id=%s task_id=%s batch_id=%s stream_type=%s instance_id=%s duration_ms=%s",
                    self.worker_id or "-",
                    task_id,
                    batch_id,
                    stream_type,
                    instance.instance_id,
                    duration_ms,
                )
                self.scheduler.record_success(instance.instance_id)
                return response
            except NoAvailableTiasInstance:
                raise
            except TiasHttpError as exc:
                last_error = exc
                if instance is not None:
                    self.scheduler.record_failure(instance.instance_id)
                    if exc.retryable:
                        excluded_instance_ids.add(instance.instance_id)
                logger.warning(
                    "TIAS 批次调用失败 worker_id=%s task_id=%s batch_id=%s stream_type=%s attempt=%s/%s retryable=%s duration_ms=%s reason=%s",
                    self.worker_id or "-",
                    task_id,
                    batch_id,
                    stream_type,
                    attempt,
                    self.config.tias_max_retry_per_batch,
                    exc.retryable,
                    int((time.monotonic() - started_at) * 1000),
                    exc,
                )
                if not exc.retryable:
                    raise
            finally:
                if instance is not None:
                    self.scheduler.release_reservation(instance.instance_id)
        raise RuntimeError(f"TIAS 批次重试耗尽 batch_id={batch_id}: {last_error}")

    def _build_payload(self, task_id: str, batch_id: str, stream_type: str, frames: List[ExtractedFrame]) -> dict:
        return {
            "task_id": task_id,
            "batch_id": batch_id,
            "stream_type": stream_type,
            "ImageList": [
                {
                    "ImageId": f"{stream_type}-{frame.point.frame_index}",
                    "StoragePath": _encode_image(frame.image),
                    "frame_index": frame.point.frame_index,
                    "timestamp_seconds": frame.point.timestamp_seconds,
                    "frame_id": f"{stream_type}-{frame.point.frame_index}",
                }
                for frame in frames
            ],
        }

    @staticmethod
    def _student_counts(item: dict) -> Dict[str, int]:
        counts = {name: 0 for name in STUDENT_OBJECT_TYPES.values()}
        for result in item.get("ResultList", []):
            name = STUDENT_OBJECT_TYPES.get(int(result.get("ObjectType", -1)))
            if name:
                counts[name] = int(result.get("ObjectCount") or 0)
        return counts

    @staticmethod
    def _ensure_complete(frames: List[ExtractedFrame], results: Dict[int, object], batch_id: str) -> None:
        missing = [
            frame.point.frame_index
            for frame in frames
            if frame.point.frame_index not in results
        ]
        if missing:
            raise RuntimeError(f"TIAS 批次结果缺帧 batch_id={batch_id} missing={missing}")


def _chunks(items: List[ExtractedFrame], size: int) -> Iterable[List[ExtractedFrame]]:
    size = max(1, int(size))
    for index in range(0, len(items), size):
        yield items[index:index + size]


def _encode_image(image) -> str:
    ok, encoded = cv2.imencode(".jpg", image)
    if not ok:
        raise RuntimeError("帧图片编码失败")
    return "data:image/jpeg;base64," + base64.b64encode(encoded.tobytes()).decode("ascii")


def _frame_index_from_image_id(image_id) -> int | None:
    if not image_id:
        return None
    text = str(image_id)
    try:
        return int(text.rsplit("-", 1)[-1])
    except ValueError:
        return None


def _find_frame(frames: List[ExtractedFrame], frame_index: int) -> ExtractedFrame:
    for frame in frames:
        if frame.point.frame_index == frame_index:
            return frame
    raise RuntimeError(f"未找到帧 frame_index={frame_index}")


def _heartbeat(heartbeat) -> None:
    if heartbeat is not None:
        heartbeat()
