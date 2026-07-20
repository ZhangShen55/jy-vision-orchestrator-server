import time
import unittest
from unittest import mock

import numpy as np

from app.core.config import VisionOrchestratorConfig
from app.infrastructure.media.video import ExtractedFrame, FramePoint
from app.infrastructure.tias.registry import InMemoryTiasRegistry, TiasInstanceStatus
from app.infrastructure.tias.scheduler import TiasScheduler
from app.infrastructure.vision.remote_frame_analyzer import RemoteFrameAnalyzer


class RemoteFrameAnalyzerTest(unittest.TestCase):
    def _registry(self):
        registry = InMemoryTiasRegistry(default_ttl_seconds=60)
        registry.upsert(TiasInstanceStatus(
            instance_id="tias-a",
            base_url="http://127.0.0.1:8981",
            capabilities=["student_behavior", "teacher_behavior", "teacher_head_pose"],
            max_concurrent_batches=2,
            running_batches=0,
            queued_batches=0,
            max_queue_size=0,
            status="UP",
            expires_at=time.time() + 60,
        ))
        return registry

    def test_analyze_student_frames_splits_batches_and_parses_counts(self):
        config = VisionOrchestratorConfig(tias_batch_size=2)
        analyzer = RemoteFrameAnalyzer(config, TiasScheduler(self._registry()))
        frames = [
            ExtractedFrame(FramePoint(15, 0, 0), np.zeros((10, 10, 3), dtype=np.uint8)),
            ExtractedFrame(FramePoint(45, 0, 1), np.zeros((10, 10, 3), dtype=np.uint8)),
            ExtractedFrame(FramePoint(75, 1, 2), np.zeros((10, 10, 3), dtype=np.uint8)),
        ]

        responses = [
            {
                "StatusObject": {"StatusCode": 0},
                "DataList": [
                    {"StatusObject": {"ImageId": "student-0"}, "ResultList": [
                        {"ObjectType": 100, "ObjectCount": 20},
                        {"ObjectType": 101, "ObjectCount": 12},
                        {"ObjectType": 202, "ObjectCount": 1},
                        {"ObjectType": 205, "ObjectCount": 3},
                    ]},
                    {"StatusObject": {"ImageId": "student-1"}, "ResultList": [
                        {"ObjectType": 100, "ObjectCount": 22},
                        {"ObjectType": 101, "ObjectCount": 15},
                        {"ObjectType": 201, "ObjectCount": 2},
                    ]},
                ],
            },
            {
                "StatusObject": {"StatusCode": 0},
                "DataList": [
                    {"StatusObject": {"ImageId": "student-2"}, "ResultList": [
                        {"ObjectType": 100, "ObjectCount": 18},
                        {"ObjectType": 101, "ObjectCount": 10},
                    ]},
                ],
            },
        ]

        with mock.patch("app.infrastructure.tias.client.requests.post") as post:
            post.side_effect = [_response(payload) for payload in responses]
            metrics = analyzer.analyze_student_frames("task-1", frames)

        self.assertEqual([metric.present_count for metric in metrics], [20, 22, 18])
        self.assertEqual([metric.face_count for metric in metrics], [12, 15, 10])
        self.assertEqual(metrics[0].sleep_count, 1)
        self.assertEqual(metrics[0].read_count, 3)
        self.assertEqual(metrics[1].phone_count, 2)
        self.assertEqual(post.call_count, 2)

    def test_analyze_teacher_frames_parses_head_pose(self):
        config = VisionOrchestratorConfig(tias_batch_size=8)
        analyzer = RemoteFrameAnalyzer(config, TiasScheduler(self._registry()))
        frames = [
            ExtractedFrame(FramePoint(15, 0, 0), np.zeros((10, 10, 3), dtype=np.uint8)),
        ]
        response_payload = {
            "StatusObject": {"StatusCode": 0},
            "DataList": [
                {
                    "StatusObject": {"ImageId": "teacher-0"},
                    "ResultList": [],
                    "HeadPoseResult": {
                        "Status": "success",
                        "FaceDirection": "front",
                        "IsLookingDown": False,
                    },
                }
            ],
        }

        with mock.patch("app.infrastructure.tias.client.requests.post") as post:
            post.return_value = _response(response_payload)
            metrics = analyzer.analyze_teacher_frames("task-1", frames)

        self.assertEqual(len(metrics), 1)
        self.assertTrue(metrics[0].valid_head_pose)
        self.assertEqual(metrics[0].face_direction, "front")
        self.assertFalse(metrics[0].is_looking_down)


def _response(payload):
    response = mock.Mock()
    response.json.return_value = payload
    response.status_code = 200
    response.raise_for_status.return_value = None
    return response


if __name__ == "__main__":
    unittest.main()
