import unittest

from vision_orchestrator.domain.metrics import (
    StudentFrameMetric,
    TeacherFrameMetric,
    aggregate_visual_metrics,
    placeholder_seat_count,
)


class VisionOrchestratorAggregatorTest(unittest.TestCase):
    def test_aggregate_metrics_skips_zero_present_count_and_uses_median(self):
        result = aggregate_visual_metrics(
            task_id="task-1",
            student_count=40,
            student_frames=[
                StudentFrameMetric(minute_no=0, present_count=30, face_count=15),
                StudentFrameMetric(minute_no=1, present_count=0, face_count=0),
                StudentFrameMetric(minute_no=2, present_count=34, face_count=17),
                StudentFrameMetric(minute_no=3, present_count=38, face_count=19),
            ],
            teacher_frames=[
                TeacherFrameMetric(minute_no=0, valid_head_pose=True, face_direction="front", is_looking_down=False),
                TeacherFrameMetric(minute_no=1, valid_head_pose=True, face_direction="left", is_looking_down=False),
                TeacherFrameMetric(minute_no=2, valid_head_pose=False, face_direction="front", is_looking_down=False),
            ],
        )

        self.assertEqual(result.expected_student_count, 40)
        self.assertAlmostEqual(result.indicators["E2-01"].value, 34 / 40)
        self.assertAlmostEqual(result.indicators["E5-01"].value, 0.5)
        self.assertAlmostEqual(result.indicators["A6-01"].value, 0.5)
        self.assertGreaterEqual(result.indicators["E3-01"].value, 0.0)
        self.assertGreaterEqual(result.indicators["E4-01"].value, 0.0)

    def test_student_count_falls_back_to_detected_max(self):
        result = aggregate_visual_metrics(
            task_id="task-2",
            student_count=0,
            student_frames=[
                StudentFrameMetric(minute_no=0, present_count=20, face_count=10),
                StudentFrameMetric(minute_no=1, present_count=30, face_count=15),
            ],
            teacher_frames=[],
        )

        self.assertEqual(result.expected_student_count, 30)
        self.assertAlmostEqual(result.indicators["E2-01"].value, 25 / 30)

    def test_placeholder_seat_count_is_stable_and_within_required_ratio(self):
        first = placeholder_seat_count("task-3", minute_no=2, metric_type="E3-01", present_count=50)
        second = placeholder_seat_count("task-3", minute_no=2, metric_type="E3-01", present_count=50)
        other_metric = placeholder_seat_count("task-3", minute_no=2, metric_type="E4-01", present_count=50)

        self.assertEqual(first, second)
        self.assertGreaterEqual(first, 10)
        self.assertLessEqual(first, 15)
        self.assertGreaterEqual(other_metric, 10)
        self.assertLessEqual(other_metric, 15)


if __name__ == "__main__":
    unittest.main()
