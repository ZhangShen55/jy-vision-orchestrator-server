import unittest
from unittest import mock

from app.application.factories import build_frame_analyzer, build_worker
from app.core.config import VisionOrchestratorConfig
from app.infrastructure.vision.remote_frame_analyzer import RemoteFrameAnalyzer


class VisionOrchestratorFactoriesTest(unittest.TestCase):
    def test_build_worker_passes_selection_mode_write_setting_to_repository(self):
        config = VisionOrchestratorConfig(write_snapshot_selection_mode=False)
        connection = mock.Mock()
        repository = mock.Mock()

        with mock.patch("app.application.factories.load_vision_orchestrator_config", return_value=config), \
                mock.patch("app.application.factories.create_mysql_connection", return_value=connection), \
                mock.patch("app.application.factories.VisionOrchestratorRepository", return_value=repository) as repository_type, \
                mock.patch("app.application.factories.SnapshotStorage") as storage_type, \
                mock.patch("app.application.factories.build_frame_analyzer", return_value=mock.Mock()):
            build_worker("config.toml")

        storage_type.return_value.ensure_writable.assert_called_once_with()
        repository_type.assert_called_once_with(
            connection,
            write_snapshot_selection_mode=False,
        )

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
