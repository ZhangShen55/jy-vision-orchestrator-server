import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np

from app.infrastructure.media.snapshot_storage import SnapshotStorage
from app.infrastructure.media.video import (
    VideoProcessingError,
    build_frame_points,
    prepare_video_source,
    validate_video_source,
)


class FramePointTest(unittest.TestCase):
    def test_build_30_second_midpoint_frame_points(self):
        points = build_frame_points(duration_seconds=100.0, interval_seconds=30)

        self.assertEqual([point.timestamp_seconds for point in points], [15.0, 45.0, 75.0])
        self.assertEqual([point.minute_no for point in points], [0, 0, 1])
        self.assertEqual([point.frame_index for point in points], [0, 1, 2])

    def test_skip_midpoint_outside_short_video(self):
        self.assertEqual(build_frame_points(duration_seconds=10.0, interval_seconds=30), [])


class SnapshotStorageTest(unittest.TestCase):
    def test_save_snapshot_uses_relative_path_and_scales_to_quarter(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = SnapshotStorage(Path(tmpdir), relative_prefix="cv", scale=0.25)
            image = np.zeros((80, 120, 3), dtype=np.uint8)
            image[:, :] = (10, 20, 30)

            result = storage.save_snapshot("lesson-1", "img-1", image)

            self.assertEqual(result.relative_path, "cv/lesson-1/img-1.png")
            self.assertEqual(result.absolute_path, Path(tmpdir) / "cv" / "lesson-1" / "img-1.png")
            self.assertTrue(result.absolute_path.exists())
            saved = storage.read_image(result.absolute_path)
            self.assertEqual(saved.shape[:2], (20, 30))

    def test_check_writable_rejects_missing_mount_root(self):
        storage = SnapshotStorage(Path("/path/not/exist/for/vision-orchestrator-test"))

        with self.assertRaises(FileNotFoundError):
            storage.ensure_writable()


class VideoSourceTest(unittest.TestCase):
    def test_validate_http_video_source(self):
        source = validate_video_source("https://example.com/video.mp4")

        self.assertEqual(source.source_type, "url")
        self.assertEqual(source.source, "https://example.com/video.mp4")
        self.assertIsNone(source.path)

    def test_prepare_local_video_source_uses_existing_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            video_path = Path(tmpdir) / "teacher.mp4"
            video_path.write_bytes(b"video")

            prepared = prepare_video_source(str(video_path), Path(tmpdir) / "downloaded.mp4")

            self.assertEqual(prepared.source_type, "local_file")
            self.assertEqual(prepared.path, video_path.resolve())
            self.assertFalse(prepared.owned_by_task)
            self.assertTrue(video_path.exists())

    def test_prepare_relative_local_video_source_uses_base_root(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base_root = Path(tmpdir) / "videos"
            base_root.mkdir()
            video_path = base_root / "student.mp4"
            video_path.write_bytes(b"video")

            prepared = prepare_video_source("student.mp4", Path(tmpdir) / "downloaded.mp4", local_base_root=base_root)

            self.assertEqual(prepared.source_type, "local_file")
            self.assertEqual(prepared.path, video_path.resolve())

    def test_reject_missing_local_video_source(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(VideoProcessingError) as ctx:
                validate_video_source(str(Path(tmpdir) / "missing.mp4"))

        self.assertIn("不存在", str(ctx.exception))

    def test_reject_directory_local_video_source(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(VideoProcessingError) as ctx:
                validate_video_source(tmpdir)

        self.assertIn("不是文件", str(ctx.exception))

    def test_reject_unreadable_local_video_source(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            video_path = Path(tmpdir) / "unreadable.mp4"
            video_path.write_bytes(b"video")
            with mock.patch("app.infrastructure.media.video.os.access", return_value=False):
                with self.assertRaises(VideoProcessingError) as ctx:
                    validate_video_source(str(video_path))

        self.assertIn("不可读", str(ctx.exception))

    def test_reject_unsupported_source_scheme(self):
        with self.assertRaises(VideoProcessingError) as ctx:
            validate_video_source("file:///tmp/video.mp4")

        self.assertIn("不支持", str(ctx.exception))

    def test_prepare_url_video_source_downloads_to_destination(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir) / "video.mp4"
            with mock.patch("app.infrastructure.media.video.download_video", return_value=destination) as download:
                prepared = prepare_video_source("http://example.com/video.mp4", destination)

        self.assertEqual(prepared.source_type, "url")
        self.assertEqual(prepared.path, destination)
        self.assertTrue(prepared.owned_by_task)
        download.assert_called_once()


if __name__ == "__main__":
    unittest.main()
