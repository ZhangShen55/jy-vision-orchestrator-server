import unittest
from unittest import mock

from app.application.factories import build_frame_analyzer
from app.config import VisionOrchestratorConfig
from app.infrastructure.vision.remote_frame_analyzer import RemoteFrameAnalyzer


class VisionOrchestratorFactoriesTest(unittest.TestCase):
    def test_remote_mode_uses_remote_frame_analyzer_with_fallback_instances(self):
        analyzer = build_frame_analyzer(VisionOrchestratorConfig(
            tias_inference_mode="remote",
            tias_fallback_instances=("http://127.0.0.1:8981",),
        ))

        self.assertIsInstance(analyzer, RemoteFrameAnalyzer)

    def test_remote_mode_does_not_import_local_tias_frame_analyzer(self):
        with mock.patch.dict("sys.modules", {"app.infrastructure.vision.frame_analyzer": None}):
            analyzer = build_frame_analyzer(VisionOrchestratorConfig(
                tias_inference_mode="remote",
                tias_fallback_instances=("http://127.0.0.1:8981",),
            ))

        self.assertIsInstance(analyzer, RemoteFrameAnalyzer)

    def test_non_remote_mode_is_rejected(self):
        for inference_mode in ("local", "invalid"):
            with self.subTest(inference_mode=inference_mode):
                with self.assertRaisesRegex(RuntimeError, "仅支持 TiasInferenceMode=remote"):
                    build_frame_analyzer(VisionOrchestratorConfig(tias_inference_mode=inference_mode))


if __name__ == "__main__":
    unittest.main()
