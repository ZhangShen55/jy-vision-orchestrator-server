import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from app.main import load_message_from_json_arg, resolve_worker_id


class VisionOrchestratorAppTest(unittest.TestCase):
    def test_load_message_from_inline_json(self):
        message = load_message_from_json_arg(json.dumps({
            "task_id": "task-1",
            "teacher_video_path": "https://example.com/t.mp4",
            "student_video_path": "https://example.com/s.mp4",
        }))

        self.assertEqual(message.task_id, "task-1")

    def test_load_message_from_json_file(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as json_file:
            json.dump({
                "task_id": "task-file",
                "teacher_video_path": "https://example.com/t.mp4",
                "student_video_path": "https://example.com/s.mp4",
            }, json_file)
            path = json_file.name

        try:
            message = load_message_from_json_arg(path)
        finally:
            Path(path).unlink()

        self.assertEqual(message.task_id, "task-file")

    @mock.patch.dict("os.environ", {"VISION_ORCHESTRATOR_WORKER_ID": "env-worker"}, clear=False)
    def test_resolve_worker_id_prefers_config_then_env(self):
        self.assertEqual(resolve_worker_id("configured-worker"), "configured-worker")
        self.assertEqual(resolve_worker_id(""), "env-worker")

    @mock.patch.dict(
        "os.environ",
        {"VISION_ORCHESTRATOR_WORKER_ID": "", "AI_QUALITY_WORKER_ID": "legacy-worker"},
        clear=False,
    )
    def test_resolve_worker_id_accepts_legacy_environment_variable(self):
        self.assertEqual(resolve_worker_id(""), "legacy-worker")


if __name__ == "__main__":
    unittest.main()
