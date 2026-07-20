import json
import unittest
from pathlib import Path

from app.infrastructure.kafka.message import InvalidTaskMessage, VisualTaskMessage


class VisualTaskMessageTest(unittest.TestCase):
    def test_parse_user_provided_kafka_message(self):
        message = VisualTaskMessage.from_payload({
            "task_id": "lesson-test-asd",
            "teacher_video_path": "https://example.com/teacher.mp4",
            "student_video_path": "https://example.com/student.mp4",
            "slides_video_path": "https://example.com/PPT.mp4",
            "evaluation_mode": 1,
            "course_id": "crs-test1",
            "student_count": 38,
        })

        self.assertEqual(message.task_id, "lesson-test-asd")
        self.assertEqual(message.teacher_video_path, "https://example.com/teacher.mp4")
        self.assertEqual(message.student_video_path, "https://example.com/student.mp4")
        self.assertEqual(message.slides_video_path, "https://example.com/PPT.mp4")
        self.assertEqual(message.teacher_video_url, "https://example.com/teacher.mp4")
        self.assertEqual(message.student_video_url, "https://example.com/student.mp4")
        self.assertEqual(message.slides_video_url, "https://example.com/PPT.mp4")
        self.assertEqual(message.course_id, "crs-test1")
        self.assertEqual(message.student_count, 38)
        self.assertEqual(message.raw_payload["evaluation_mode"], 1)

    def test_parse_compatible_url_field_names_and_default_student_count(self):
        message = VisualTaskMessage.from_payload({
            "taskId": "lesson-2",
            "teacherVideoUrl": "https://example.com/t.mp4",
            "studentVideoUrl": "https://example.com/s.mp4",
        })

        self.assertEqual(message.task_id, "lesson-2")
        self.assertEqual(message.teacher_video_url, "https://example.com/t.mp4")
        self.assertEqual(message.student_video_url, "https://example.com/s.mp4")
        self.assertEqual(message.student_count, 50)

    def test_reject_missing_required_fields(self):
        with self.assertRaises(InvalidTaskMessage) as ctx:
            VisualTaskMessage.from_payload({
                "task_id": "lesson-missing-video",
                "teacher_video_path": "https://example.com/t.mp4",
            })

        self.assertIn("student_video", str(ctx.exception))

    def test_accept_local_video_paths(self):
        message = VisualTaskMessage.from_payload({
            "task_id": "lesson-local-path",
            "teacher_video_path": "/data/course/teacher.mp4",
            "student_video_path": "relative/student.mp4",
            "slides_video_path": "/data/course/PPT.mp4",
        })

        self.assertEqual(message.teacher_video_path, "/data/course/teacher.mp4")
        self.assertEqual(message.student_video_path, "relative/student.mp4")
        self.assertEqual(message.slides_video_path, "/data/course/PPT.mp4")

    def test_parse_local_video_path_fixture(self):
        fixture_path = Path(__file__).resolve().parent / "fixtures" / "vision_orchestrator_lesson_local_path.json"
        payload = json.loads(fixture_path.read_text(encoding="utf-8"))

        message = VisualTaskMessage.from_payload(payload)

        self.assertEqual(message.task_id, "lesson-test-local-path")
        self.assertTrue(message.teacher_video_path.endswith("教师1.mp4"))
        self.assertTrue(message.student_video_path.endswith("学生1.mp4"))
        self.assertTrue(message.slides_video_path.endswith("PPT.mp4"))

    def test_reject_unsupported_video_path_scheme(self):
        with self.assertRaises(InvalidTaskMessage):
            VisualTaskMessage.from_payload({
                "task_id": "lesson-invalid-scheme",
                "teacher_video_path": "ftp://example.com/t.mp4",
                "student_video_path": "https://example.com/s.mp4",
            })


if __name__ == "__main__":
    unittest.main()
