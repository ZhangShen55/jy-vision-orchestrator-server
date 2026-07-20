import unittest

from vision_orchestrator.domain.behavior_stats import build_student_behavior_stats
from vision_orchestrator.domain.metrics import StudentFrameMetric


class StudentBehaviorStatTest(unittest.TestCase):
    def test_build_stats_skips_first_three_minutes_and_sorts_candidate_segments(self):
        stats = build_student_behavior_stats(
            [
                StudentFrameMetric(minute_no=0, present_count=30, face_count=20, phone_count=9, sleep_count=9),
                StudentFrameMetric(minute_no=3, present_count=30, face_count=20, phone_count=1),
                StudentFrameMetric(minute_no=4, present_count=30, face_count=20, phone_count=3),
                StudentFrameMetric(minute_no=5, present_count=30, face_count=20, phone_count=1),
                StudentFrameMetric(minute_no=6, present_count=30, face_count=20, sleep_count=1),
                StudentFrameMetric(minute_no=8, present_count=30, face_count=20, sleep_count=2),
                StudentFrameMetric(minute_no=10, present_count=30, face_count=20, phone_count=5),
                StudentFrameMetric(minute_no=11, present_count=30, face_count=20, phone_count=5),
                StudentFrameMetric(minute_no=20, present_count=0, face_count=0, phone_count=99, sleep_count=99),
            ],
            start_minute=3,
            peak_max_segments=2,
        )

        by_type = {stat.behavior_type: stat for stat in stats}

        self.assertEqual(set(by_type), {1, 3})
        self.assertEqual(by_type[1].detect_count, 15)
        self.assertEqual(by_type[1].peak_period_desc, "3′–5′、10′–11′")
        self.assertEqual(by_type[1].confidence_level, 2)
        self.assertEqual(by_type[3].detect_count, 3)
        self.assertEqual(by_type[3].peak_period_desc, "6′、8′")

    def test_peak_period_desc_keeps_only_configured_top_segments(self):
        stats = build_student_behavior_stats(
            [
                StudentFrameMetric(minute_no=3, present_count=30, face_count=20, phone_count=1),
                StudentFrameMetric(minute_no=5, present_count=30, face_count=20, phone_count=9),
                StudentFrameMetric(minute_no=7, present_count=30, face_count=20, phone_count=8),
                StudentFrameMetric(minute_no=9, present_count=30, face_count=20, phone_count=7),
                StudentFrameMetric(minute_no=11, present_count=30, face_count=20, phone_count=6),
                StudentFrameMetric(minute_no=13, present_count=30, face_count=20, phone_count=5),
                StudentFrameMetric(minute_no=15, present_count=30, face_count=20, phone_count=4),
            ],
            start_minute=3,
            peak_max_segments=5,
        )

        phone_stat = stats[0]

        self.assertEqual(phone_stat.behavior_type, 1)
        self.assertEqual(phone_stat.detect_count, 40)
        self.assertEqual(phone_stat.peak_period_desc, "5′、7′、9′、11′、13′")

    def test_no_rows_are_built_when_behavior_counts_are_zero(self):
        stats = build_student_behavior_stats(
            [
                StudentFrameMetric(minute_no=3, present_count=30, face_count=20),
                StudentFrameMetric(minute_no=4, present_count=0, face_count=0, phone_count=2, sleep_count=2),
            ],
            start_minute=3,
            peak_max_segments=5,
        )

        self.assertEqual(stats, [])


if __name__ == "__main__":
    unittest.main()
