import unittest

from scripts import produce_vision_orchestrator_kafka_message


class VisionOrchestratorProducerScriptTest(unittest.TestCase):
    def test_default_topic_is_classroom_cv_task(self):
        self.assertEqual(produce_vision_orchestrator_kafka_message.DEFAULT_TOPIC, "classroom_cv_task")

    def test_default_broker_is_localhost(self):
        self.assertEqual(produce_vision_orchestrator_kafka_message.DEFAULT_BOOTSTRAP_SERVERS, "127.0.0.1:9092")


if __name__ == "__main__":
    unittest.main()
