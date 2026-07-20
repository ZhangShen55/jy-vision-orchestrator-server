import unittest

from vision_orchestrator.config import VisionOrchestratorConfig
from vision_orchestrator.domain.metrics import StudentFrameMetric, TeacherFrameMetric
from vision_orchestrator.domain.snapshots import (
    StudentFrameSnapshotInput,
    TeacherFrameSnapshotInput,
    build_snapshot_events,
)


class VisionOrchestratorSnapshotPolicyTest(unittest.TestCase):
    def test_head_up_and_read_snapshots_use_top_k_then_threshold(self):
        config = VisionOrchestratorConfig(
            snapshot_head_up_top_k=3,
            snapshot_head_up_min_rate=0.70,
            snapshot_read_top_k=2,
            snapshot_read_min_rate=0.40,
            snapshot_same_type_min_interval_seconds=0,
        )
        student_frames = [
            StudentFrameSnapshotInput(
                frame_index=0,
                timestamp_seconds=15,
                image=object(),
                metric=StudentFrameMetric(minute_no=0, present_count=20, face_count=13, read_count=2),
            ),
            StudentFrameSnapshotInput(
                frame_index=1,
                timestamp_seconds=45,
                image=object(),
                metric=StudentFrameMetric(minute_no=0, present_count=20, face_count=14, read_count=8),
            ),
            StudentFrameSnapshotInput(
                frame_index=2,
                timestamp_seconds=75,
                image=object(),
                metric=StudentFrameMetric(minute_no=1, present_count=20, face_count=15, read_count=9),
            ),
        ]

        events = build_snapshot_events("task-1", config, student_frames, [])

        self.assertCountEqual(
            [(event.behavior_type, event.capture_second) for event in events],
            [(2, 45), (2, 75), (3, 75), (3, 45)],
        )

    def test_sleep_phone_teacher_alert_and_total_limit_are_configurable(self):
        config = VisionOrchestratorConfig(
            snapshot_max_total=3,
            snapshot_head_up_top_k=0,
            snapshot_read_top_k=0,
            snapshot_sleep_min_count=2,
            snapshot_sleep_min_rate=0.05,
            snapshot_phone_min_count=2,
            snapshot_phone_min_rate=0.05,
            snapshot_teacher_alert_consecutive_frames=3,
            snapshot_same_type_min_interval_seconds=0,
        )
        student_frames = [
            StudentFrameSnapshotInput(
                frame_index=0,
                timestamp_seconds=15,
                image=object(),
                metric=StudentFrameMetric(minute_no=0, present_count=40, face_count=10, sleep_count=1, phone_count=1),
            ),
            StudentFrameSnapshotInput(
                frame_index=1,
                timestamp_seconds=45,
                image=object(),
                metric=StudentFrameMetric(minute_no=0, present_count=40, face_count=10, sleep_count=2, phone_count=0),
            ),
            StudentFrameSnapshotInput(
                frame_index=2,
                timestamp_seconds=75,
                image=object(),
                metric=StudentFrameMetric(minute_no=1, present_count=40, face_count=10, sleep_count=0, phone_count=3),
            ),
        ]
        teacher_frames = [
            TeacherFrameSnapshotInput(
                frame_index=0,
                timestamp_seconds=15,
                image=object(),
                metric=TeacherFrameMetric(minute_no=0, valid_head_pose=True, face_direction="left"),
            ),
            TeacherFrameSnapshotInput(
                frame_index=1,
                timestamp_seconds=45,
                image=object(),
                metric=TeacherFrameMetric(minute_no=0, valid_head_pose=True, face_direction="right"),
            ),
            TeacherFrameSnapshotInput(
                frame_index=2,
                timestamp_seconds=75,
                image=object(),
                metric=TeacherFrameMetric(minute_no=1, valid_head_pose=True, face_direction="front", is_looking_down=True),
            ),
        ]

        events = build_snapshot_events("task-2", config, student_frames, teacher_frames)

        self.assertEqual(len(events), 3)
        self.assertEqual(
            {(event.target_type, event.record_type, event.behavior_type) for event in events},
            {(2, 1, 4), (2, 1, 5), (1, 1, 1)},
        )

    def test_same_type_interval_keeps_stronger_candidate(self):
        config = VisionOrchestratorConfig(
            snapshot_max_total=10,
            snapshot_head_up_top_k=0,
            snapshot_read_top_k=0,
            snapshot_sleep_min_count=1,
            snapshot_sleep_min_rate=0.01,
            snapshot_same_type_min_interval_seconds=90,
        )
        student_frames = [
            StudentFrameSnapshotInput(
                frame_index=0,
                timestamp_seconds=15,
                image=object(),
                metric=StudentFrameMetric(minute_no=0, present_count=40, face_count=10, sleep_count=1),
            ),
            StudentFrameSnapshotInput(
                frame_index=1,
                timestamp_seconds=45,
                image=object(),
                metric=StudentFrameMetric(minute_no=0, present_count=40, face_count=10, sleep_count=3),
            ),
        ]

        events = build_snapshot_events("task-3", config, student_frames, [])

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].capture_second, 45)
        self.assertEqual(events[0].confidence_score, 3 / 40)


if __name__ == "__main__":
    unittest.main()
