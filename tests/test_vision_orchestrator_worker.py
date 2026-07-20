import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np

from vision_orchestrator.application.worker import VisualAnalysisWorker
from vision_orchestrator.config import VisionOrchestratorConfig
from vision_orchestrator.domain.metrics import IndicatorMetric, StudentFrameMetric, TeacherFrameMetric
from vision_orchestrator.infrastructure.db.repositories import IndicatorDefinition
from vision_orchestrator.infrastructure.kafka.message import VisualTaskMessage
from vision_orchestrator.infrastructure.media.video import PreparedVideoSource


class FakeRepository:
    def __init__(self):
        self.calls = []

    def mark_workflow_running(self, task_id):
        self.calls.append(("workflow_running", task_id))

    def mark_job_running(self, task_id):
        self.calls.append(("job_running", task_id))

    def clear_previous_results(self, task_id):
        self.calls.append(("clear", task_id))

    def insert_timeline_rows(self, task_id, rows):
        self.calls.append(("timeline", task_id, list(rows)))

    def insert_snapshot_events(self, task_id, rows):
        self.calls.append(("snapshots", task_id, list(rows)))

    def upsert_student_behavior_stats(self, task_id, rows):
        self.calls.append(("student_behavior_stats", task_id, list(rows)))

    def load_indicator_definitions(self, codes):
        self.calls.append(("load_indicators", tuple(codes)))
        return {
            code: IndicatorDefinition(
                indicator_id=f"id-{code}",
                indicator_code=code,
                indicator_name=code,
                unit="%",
                score_rule=None,
            )
            for code in codes
        }

    def upsert_indicator_results(self, task_id, indicators, definitions):
        self.calls.append(("indicators", task_id, indicators, definitions))

    def mark_workflow_success(self, task_id):
        self.calls.append(("workflow_success", task_id))

    def mark_job_success(self, task_id):
        self.calls.append(("job_success", task_id))

    def mark_workflow_failed(self, task_id, error_msg):
        self.calls.append(("workflow_failed", task_id, error_msg))

    def mark_job_failed(self, task_id, error_msg):
        self.calls.append(("job_failed", task_id, error_msg))


class FakeFrameAnalyzer:
    def analyze_student_frame(self, minute_no, image):
        return StudentFrameMetric(minute_no=minute_no, present_count=20, face_count=10)

    def analyze_teacher_frame(self, minute_no, image):
        return TeacherFrameMetric(minute_no=minute_no, valid_head_pose=True, face_direction="front")


class FakeBatchFrameAnalyzer:
    def __init__(self):
        self.student_batches = []
        self.teacher_batches = []

    def analyze_student_frames(self, task_id, frames):
        self.student_batches.append((task_id, len(frames)))
        return [
            StudentFrameMetric(minute_no=frame.point.minute_no, present_count=20, face_count=10)
            for frame in frames
        ]

    def analyze_teacher_frames(self, task_id, frames):
        self.teacher_batches.append((task_id, len(frames)))
        return [
            TeacherFrameMetric(minute_no=frame.point.minute_no, valid_head_pose=True, face_direction="front")
            for frame in frames
        ]


def fake_prepare_video_source(source, dest, local_base_root=None, timeout_seconds=60, **kwargs):
    return PreparedVideoSource(
        source=source,
        source_type="url",
        path=dest,
        owned_by_task=True,
    )


class VisualAnalysisWorkerTest(unittest.TestCase):
    def test_process_task_runs_status_without_saving_non_event_frames(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = VisionOrchestratorConfig(temp_root=Path(tmpdir) / "tmp", snapshot_mount_root=Path(tmpdir))
            repository = FakeRepository()
            worker = VisualAnalysisWorker(
                config=config,
                repository=repository,
                frame_analyzer=FakeFrameAnalyzer(),
            )
            message = VisualTaskMessage.from_payload({
                "task_id": "task-1",
                "teacher_video_path": "https://example.com/t.mp4",
                "student_video_path": "https://example.com/s.mp4",
                "student_count": 40,
            })

            fake_frame = np.zeros((40, 80, 3), dtype=np.uint8)
            with mock.patch("vision_orchestrator.application.worker.prepare_video_source", side_effect=fake_prepare_video_source), \
                    mock.patch("vision_orchestrator.application.worker.extract_frames") as extract:
                extract.return_value = [
                    mock.Mock(point=mock.Mock(minute_no=0, timestamp_seconds=15.0, frame_index=0), image=fake_frame)
                ]

                worker.process_task(message)

        call_names = [call[0] for call in repository.calls]
        self.assertEqual(call_names[:2], ["workflow_running", "clear"])
        self.assertIn("timeline", call_names)
        self.assertIn("snapshots", call_names)
        self.assertIn("student_behavior_stats", call_names)
        self.assertIn("indicators", call_names)
        self.assertEqual(call_names[-1], "workflow_success")
        snapshot_call = next(call for call in repository.calls if call[0] == "snapshots")
        self.assertEqual(snapshot_call[2], [])

    def test_process_task_does_not_write_lesson_ai_job(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = VisionOrchestratorConfig(temp_root=Path(tmpdir) / "tmp", snapshot_mount_root=Path(tmpdir))
            repository = FakeRepository()
            worker = VisualAnalysisWorker(
                config=config,
                repository=repository,
                frame_analyzer=FakeFrameAnalyzer(),
            )
            message = VisualTaskMessage.from_payload({
                "task_id": "task-no-job",
                "teacher_video_path": "https://example.com/t.mp4",
                "student_video_path": "https://example.com/s.mp4",
                "student_count": 40,
            })

            fake_frame = np.zeros((40, 80, 3), dtype=np.uint8)
            with mock.patch("vision_orchestrator.application.worker.prepare_video_source", side_effect=fake_prepare_video_source), \
                    mock.patch("vision_orchestrator.application.worker.extract_frames") as extract:
                extract.return_value = [
                    mock.Mock(point=mock.Mock(minute_no=0, timestamp_seconds=15.0, frame_index=0), image=fake_frame)
                ]

                worker.process_task(message)

        call_names = [call[0] for call in repository.calls]
        self.assertNotIn("job_running", call_names)
        self.assertNotIn("job_success", call_names)
        self.assertNotIn("job_failed", call_names)

    def test_process_task_uses_batch_frame_analyzer_when_available(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = VisionOrchestratorConfig(temp_root=Path(tmpdir) / "tmp", snapshot_mount_root=Path(tmpdir))
            repository = FakeRepository()
            frame_analyzer = FakeBatchFrameAnalyzer()
            worker = VisualAnalysisWorker(
                config=config,
                repository=repository,
                frame_analyzer=frame_analyzer,
            )
            message = VisualTaskMessage.from_payload({
                "task_id": "task-batch",
                "teacher_video_path": "https://example.com/t.mp4",
                "student_video_path": "https://example.com/s.mp4",
                "student_count": 40,
            })
            fake_frame = np.zeros((40, 80, 3), dtype=np.uint8)
            frames = [
                mock.Mock(point=mock.Mock(minute_no=0, timestamp_seconds=15.0, frame_index=0), image=fake_frame),
                mock.Mock(point=mock.Mock(minute_no=0, timestamp_seconds=45.0, frame_index=1), image=fake_frame),
            ]
            with mock.patch("vision_orchestrator.application.worker.prepare_video_source", side_effect=fake_prepare_video_source), \
                    mock.patch("vision_orchestrator.application.worker.extract_frames", return_value=frames):
                worker.process_task(message)

        self.assertEqual(frame_analyzer.student_batches, [("task-batch", 2)])
        self.assertEqual(frame_analyzer.teacher_batches, [("task-batch", 2)])

    def test_process_task_writes_student_behavior_stats_after_start_minute(self):
        class BehaviorStatFrameAnalyzer:
            def __init__(self):
                self.student_calls = 0

            def analyze_student_frame(self, minute_no, image):
                self.student_calls += 1
                if minute_no < 3:
                    return StudentFrameMetric(minute_no=minute_no, present_count=20, face_count=10, phone_count=5)
                if minute_no == 3:
                    return StudentFrameMetric(minute_no=minute_no, present_count=20, face_count=10, phone_count=2)
                return StudentFrameMetric(minute_no=minute_no, present_count=20, face_count=10, sleep_count=1)

            def analyze_teacher_frame(self, minute_no, image):
                return TeacherFrameMetric(minute_no=minute_no, valid_head_pose=True, face_direction="front")

        with tempfile.TemporaryDirectory() as tmpdir:
            config = VisionOrchestratorConfig(
                temp_root=Path(tmpdir) / "tmp",
                snapshot_mount_root=Path(tmpdir),
                behavior_stat_start_minute=3,
                behavior_stat_peak_max_segments=5,
                snapshot_head_up_top_k=0,
                snapshot_read_top_k=0,
                snapshot_phone_min_count=99,
                snapshot_sleep_min_count=99,
            )
            repository = FakeRepository()
            worker = VisualAnalysisWorker(
                config=config,
                repository=repository,
                frame_analyzer=BehaviorStatFrameAnalyzer(),
            )
            message = VisualTaskMessage.from_payload({
                "task_id": "task-behavior-stat",
                "teacher_video_path": "https://example.com/t.mp4",
                "student_video_path": "https://example.com/s.mp4",
                "student_count": 40,
            })

            fake_frame = np.zeros((40, 80, 3), dtype=np.uint8)
            frames = [
                mock.Mock(point=mock.Mock(minute_no=2, timestamp_seconds=165.0, frame_index=0), image=fake_frame),
                mock.Mock(point=mock.Mock(minute_no=3, timestamp_seconds=195.0, frame_index=1), image=fake_frame),
                mock.Mock(point=mock.Mock(minute_no=4, timestamp_seconds=255.0, frame_index=2), image=fake_frame),
            ]
            with mock.patch("vision_orchestrator.application.worker.prepare_video_source", side_effect=fake_prepare_video_source), \
                    mock.patch("vision_orchestrator.application.worker.extract_frames", return_value=frames):
                worker.process_task(message)

        stat_call = next(call for call in repository.calls if call[0] == "student_behavior_stats")
        stats = {stat.behavior_type: stat for stat in stat_call[2]}
        self.assertEqual(set(stats), {1, 3})
        self.assertEqual(stats[1].detect_count, 2)
        self.assertEqual(stats[1].peak_period_desc, "3′")
        self.assertEqual(stats[3].detect_count, 1)
        self.assertEqual(stats[3].peak_period_desc, "4′")

    def test_process_task_saves_only_configured_snapshot_events(self):
        class EventFrameAnalyzer:
            def __init__(self):
                self.student_calls = 0
                self.teacher_calls = 0

            def analyze_student_frame(self, minute_no, image):
                self.student_calls += 1
                if self.student_calls == 1:
                    return StudentFrameMetric(minute_no=minute_no, present_count=20, face_count=18, read_count=1)
                return StudentFrameMetric(minute_no=minute_no, present_count=20, face_count=5, read_count=0, phone_count=2)

            def analyze_teacher_frame(self, minute_no, image):
                self.teacher_calls += 1
                return TeacherFrameMetric(minute_no=minute_no, valid_head_pose=True, face_direction="front")

        with tempfile.TemporaryDirectory() as tmpdir:
            config = VisionOrchestratorConfig(
                temp_root=Path(tmpdir) / "tmp",
                snapshot_mount_root=Path(tmpdir),
                snapshot_head_up_top_k=3,
                snapshot_head_up_min_rate=0.7,
                snapshot_read_top_k=0,
                snapshot_phone_min_count=2,
                snapshot_same_type_min_interval_seconds=0,
            )
            repository = FakeRepository()
            worker = VisualAnalysisWorker(
                config=config,
                repository=repository,
                frame_analyzer=EventFrameAnalyzer(),
            )
            message = VisualTaskMessage.from_payload({
                "task_id": "task-events",
                "teacher_video_path": "https://example.com/t.mp4",
                "student_video_path": "https://example.com/s.mp4",
                "student_count": 40,
            })

            fake_frame = np.zeros((40, 80, 3), dtype=np.uint8)
            frames = [
                mock.Mock(point=mock.Mock(minute_no=0, timestamp_seconds=15.0, frame_index=0), image=fake_frame),
                mock.Mock(point=mock.Mock(minute_no=0, timestamp_seconds=45.0, frame_index=1), image=fake_frame),
            ]
            with mock.patch("vision_orchestrator.application.worker.prepare_video_source", side_effect=fake_prepare_video_source), \
                    mock.patch("vision_orchestrator.application.worker.extract_frames", return_value=frames):
                worker.process_task(message)

        snapshot_call = next(call for call in repository.calls if call[0] == "snapshots")
        rows = snapshot_call[2]
        self.assertEqual(
            [(row["behavior_type"], row["capture_second"]) for row in rows],
            [(2, 15), (5, 45)],
        )

    def test_process_task_passes_heartbeat_to_media_steps(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = VisionOrchestratorConfig(temp_root=Path(tmpdir) / "tmp", snapshot_mount_root=Path(tmpdir))
            repository = FakeRepository()
            worker = VisualAnalysisWorker(
                config=config,
                repository=repository,
                frame_analyzer=FakeFrameAnalyzer(),
            )
            message = VisualTaskMessage.from_payload({
                "task_id": "task-heartbeat",
                "teacher_video_path": "https://example.com/t.mp4",
                "student_video_path": "https://example.com/s.mp4",
                "student_count": 40,
            })
            fake_frame = np.zeros((40, 80, 3), dtype=np.uint8)
            frame = mock.Mock(point=mock.Mock(minute_no=0, timestamp_seconds=15.0, frame_index=0), image=fake_frame)
            heartbeat = mock.Mock()

            with mock.patch("vision_orchestrator.application.worker.prepare_video_source", side_effect=fake_prepare_video_source) as prepare, \
                    mock.patch("vision_orchestrator.application.worker.extract_frames", return_value=[frame]) as extract:
                worker.process_task(message, heartbeat=heartbeat)

        self.assertEqual(prepare.call_count, 2)
        self.assertEqual(extract.call_count, 2)
        for call in prepare.call_args_list:
            self.assertIn("progress_callback", call.kwargs)
            self.assertTrue(call.kwargs["progress_callback"])
        for call in extract.call_args_list:
            self.assertIn("progress_callback", call.kwargs)
            self.assertTrue(call.kwargs["progress_callback"])

    def test_process_task_keeps_local_source_video_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            source_root = tmp_path / "source"
            source_root.mkdir()
            student_source = source_root / "student.mp4"
            teacher_source = source_root / "teacher.mp4"
            slides_source = source_root / "PPT.mp4"
            student_source.write_bytes(b"student-video")
            teacher_source.write_bytes(b"teacher-video")
            slides_source.write_bytes(b"slides-video")
            config = VisionOrchestratorConfig(temp_root=tmp_path / "tmp", snapshot_mount_root=tmp_path)
            repository = FakeRepository()
            worker = VisualAnalysisWorker(
                config=config,
                repository=repository,
                frame_analyzer=FakeFrameAnalyzer(),
            )
            message = VisualTaskMessage.from_payload({
                "task_id": "task-local-source",
                "teacher_video_path": str(teacher_source),
                "student_video_path": str(student_source),
                "slides_video_path": str(slides_source),
                "student_count": 40,
            })
            fake_frame = np.zeros((40, 80, 3), dtype=np.uint8)
            frame = mock.Mock(point=mock.Mock(minute_no=0, timestamp_seconds=15.0, frame_index=0), image=fake_frame)

            with mock.patch("vision_orchestrator.application.worker.extract_frames", return_value=[frame]) as extract:
                worker.process_task(message)

            self.assertTrue(student_source.exists())
            self.assertTrue(teacher_source.exists())
            self.assertTrue(slides_source.exists())
            extracted_paths = [call.args[0] for call in extract.call_args_list]
            self.assertEqual(extracted_paths, [student_source.resolve(), teacher_source.resolve()])

    def test_process_task_marks_failure_on_exception(self):
        config = VisionOrchestratorConfig()
        repository = FakeRepository()
        worker = VisualAnalysisWorker(
            config=config,
            repository=repository,
            frame_analyzer=FakeFrameAnalyzer(),
        )
        message = VisualTaskMessage.from_payload({
            "task_id": "task-fail",
            "teacher_video_path": "https://example.com/t.mp4",
            "student_video_path": "https://example.com/s.mp4",
        })

        with mock.patch("vision_orchestrator.application.worker.prepare_video_source", side_effect=RuntimeError("download failed")):
            with self.assertRaises(RuntimeError):
                worker.process_task(message)

        call_names = [call[0] for call in repository.calls]
        self.assertIn("workflow_failed", call_names)
        self.assertNotIn("job_failed", call_names)

    def test_process_task_preserves_original_error_when_failure_status_write_fails(self):
        class FailingStatusRepository(FakeRepository):
            def mark_workflow_running(self, task_id):
                raise RuntimeError("original task failure")

            def mark_workflow_failed(self, task_id, error_msg):
                raise RuntimeError("failure status write failed")

        worker = VisualAnalysisWorker(
            config=VisionOrchestratorConfig(),
            repository=FailingStatusRepository(),
            frame_analyzer=FakeFrameAnalyzer(),
        )
        message = VisualTaskMessage.from_payload({
            "task_id": "task-original-error",
            "teacher_video_path": "https://example.com/t.mp4",
            "student_video_path": "https://example.com/s.mp4",
        })

        with self.assertRaisesRegex(RuntimeError, "original task failure"):
            worker.process_task(message)


if __name__ == "__main__":
    unittest.main()
