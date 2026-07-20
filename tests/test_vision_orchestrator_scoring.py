import unittest

from vision_orchestrator.domain.scoring import score_indicator


class VisionOrchestratorScoringTest(unittest.TestCase):
    def test_positive_rule_interpolates_percent_value(self):
        rule = {
            "type": "POSITIVE",
            "score_min": 0,
            "thresholds": [
                {"score": 0, "value": 70},
                {"score": 60, "value": 90},
                {"score": 100, "value": 100},
            ],
        }

        self.assertEqual(score_indicator(0.70, rule), 0.0)
        self.assertEqual(score_indicator(0.90, rule), 60.0)
        self.assertEqual(score_indicator(1.00, rule), 100.0)
        self.assertEqual(score_indicator(0.95, rule), 80.0)

    def test_negative_rule_interpolates_percent_value(self):
        rule = {
            "type": "NEGATIVE",
            "score_min": 0,
            "thresholds": [
                {"score": 100, "value": 0},
                {"score": 60, "value": 30},
                {"score": 0, "value": 60},
            ],
        }

        self.assertEqual(score_indicator(0.00, rule), 100.0)
        self.assertEqual(score_indicator(0.30, rule), 60.0)
        self.assertEqual(score_indicator(0.60, rule), 0.0)
        self.assertEqual(score_indicator(0.15, rule), 80.0)

    def test_missing_rule_uses_default_ratio_score(self):
        self.assertEqual(score_indicator(0.42, None), 42.0)


if __name__ == "__main__":
    unittest.main()
