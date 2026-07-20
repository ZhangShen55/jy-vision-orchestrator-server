from dataclasses import dataclass
import os
from pathlib import Path
from typing import Callable, List, Optional
from urllib.parse import urlparse

import cv2
import requests


class VideoProcessingError(RuntimeError):
    """视频下载、读取或抽帧失败。"""


@dataclass(frozen=True)
class VideoSourceInfo:
    source: str
    source_type: str
    path: Optional[Path] = None


@dataclass(frozen=True)
class PreparedVideoSource:
    source: str
    source_type: str
    path: Path
    owned_by_task: bool


@dataclass(frozen=True)
class FramePoint:
    timestamp_seconds: float
    minute_no: int
    frame_index: int


@dataclass(frozen=True)
class ExtractedFrame:
    point: FramePoint
    image: object


def build_frame_points(duration_seconds: float, interval_seconds: int = 30) -> List[FramePoint]:
    if duration_seconds <= 0 or interval_seconds <= 0:
        return []
    points: List[FramePoint] = []
    timestamp = interval_seconds / 2.0
    frame_index = 0
    while timestamp < duration_seconds:
        points.append(FramePoint(
            timestamp_seconds=float(timestamp),
            minute_no=int(timestamp // 60),
            frame_index=frame_index,
        ))
        timestamp += interval_seconds
        frame_index += 1
    return points


def download_video(
        url: str,
        destination: Path,
        timeout_seconds: int = 60,
        progress_callback: Optional[Callable[[], None]] = None) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with requests.get(url, stream=True, timeout=timeout_seconds) as response:
            response.raise_for_status()
            with destination.open("wb") as output:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        output.write(chunk)
                        _notify_progress(progress_callback)
    except Exception as exc:
        raise VideoProcessingError(f"下载视频失败: {url}: {exc}") from exc
    if destination.stat().st_size <= 0:
        raise VideoProcessingError(f"下载视频为空: {url}")
    return destination


def validate_video_source(source: str, local_base_root: Optional[Path] = None) -> VideoSourceInfo:
    if not isinstance(source, str) or not source.strip():
        raise VideoProcessingError("视频 path 不能为空")
    source = source.strip()
    parsed = urlparse(source)
    if parsed.scheme in {"http", "https"}:
        if not parsed.netloc:
            raise VideoProcessingError(f"视频 URL 不完整: {source}")
        return VideoSourceInfo(source=source, source_type="url")
    if parsed.scheme:
        raise VideoProcessingError(f"不支持的视频 path 类型: {parsed.scheme}: {source}")

    path = Path(source).expanduser()
    if not path.is_absolute() and local_base_root is not None:
        path = local_base_root / path
    path = path.resolve()
    if not path.exists():
        raise VideoProcessingError(f"本地视频文件不存在: {path}")
    if not path.is_file():
        raise VideoProcessingError(f"本地视频路径不是文件: {path}")
    if not os.access(path, os.R_OK):
        raise VideoProcessingError(f"本地视频文件不可读: {path}")
    return VideoSourceInfo(source=source, source_type="local_file", path=path)


def prepare_video_source(
        source: str,
        destination: Path,
        local_base_root: Optional[Path] = None,
        timeout_seconds: int = 60,
        progress_callback: Optional[Callable[[], None]] = None) -> PreparedVideoSource:
    source_info = validate_video_source(source, local_base_root=local_base_root)
    if source_info.source_type == "url":
        return PreparedVideoSource(
            source=source_info.source,
            source_type=source_info.source_type,
            path=download_video(
                source_info.source,
                destination,
                timeout_seconds=timeout_seconds,
                progress_callback=progress_callback,
            ),
            owned_by_task=True,
        )
    if source_info.path is None:
        raise VideoProcessingError(f"本地视频路径解析失败: {source}")
    return PreparedVideoSource(
        source=source_info.source,
        source_type=source_info.source_type,
        path=source_info.path,
        owned_by_task=False,
    )


def get_video_duration_seconds(video_path: Path) -> float:
    capture = cv2.VideoCapture(str(video_path))
    try:
        if not capture.isOpened():
            raise VideoProcessingError(f"无法打开视频: {video_path}")
        fps = capture.get(cv2.CAP_PROP_FPS)
        frame_count = capture.get(cv2.CAP_PROP_FRAME_COUNT)
        if fps and fps > 0 and frame_count and frame_count > 0:
            return float(frame_count / fps)
        milliseconds = capture.get(cv2.CAP_PROP_POS_MSEC)
        if milliseconds and milliseconds > 0:
            return float(milliseconds / 1000.0)
    finally:
        capture.release()
    raise VideoProcessingError(f"无法读取视频时长: {video_path}")


def extract_frame_at(video_path: Path, point: FramePoint):
    capture = cv2.VideoCapture(str(video_path))
    try:
        if not capture.isOpened():
            raise VideoProcessingError(f"无法打开视频: {video_path}")
        capture.set(cv2.CAP_PROP_POS_MSEC, point.timestamp_seconds * 1000.0)
        ok, frame = capture.read()
        if not ok or frame is None:
            raise VideoProcessingError(f"抽帧失败: {video_path} at {point.timestamp_seconds}s")
        return frame
    finally:
        capture.release()


def extract_frames(
        video_path: Path,
        interval_seconds: int = 30,
        progress_callback: Optional[Callable[[], None]] = None) -> List[ExtractedFrame]:
    duration = get_video_duration_seconds(video_path)
    frames = []
    for point in build_frame_points(duration, interval_seconds):
        frames.append(ExtractedFrame(point=point, image=extract_frame_at(video_path, point)))
        _notify_progress(progress_callback)
    if not frames:
        raise VideoProcessingError(f"视频无有效抽帧点: {video_path}")
    return frames


def _notify_progress(progress_callback: Optional[Callable[[], None]]) -> None:
    if progress_callback is not None:
        progress_callback()
