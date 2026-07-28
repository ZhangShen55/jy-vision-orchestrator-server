import unittest

from app.domain.behavior_stats import StudentBehaviorStat
from app.domain.metrics import IndicatorMetric
from app.infrastructure.db.repositories import VisionOrchestratorRepository, IndicatorDefinition


class FakeCursor:
    def __init__(self):
        self.executed = []
        self.executemany_calls = []
        self.indicator_rows = [
            {
                "indicator_id": "id-e2",
                "indicator_code": "E2-01",
                "indicator_name": "到课率",
                "unit": "%",
                "score_rule": None,
            }
        ]

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def executemany(self, sql, params):
        self.executemany_calls.append((sql, list(params)))

    def fetchall(self):
        return self.indicator_rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConnection:
    def __init__(self):
        self.cursor_obj = FakeCursor()
        self.commits = 0
        self.rollbacks = 0
        self.ping_calls = []

    def cursor(self):
        return self.cursor_obj

    def ping(self, reconnect=False):
        self.ping_calls.append(reconnect)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class StaleConnection(FakeConnection):
    def __init__(self):
        super().__init__()
        self.stale = True

    def ping(self, reconnect=False):
        super().ping(reconnect=reconnect)
        if reconnect:
            self.stale = False

    def cursor(self):
        if self.stale:
            raise RuntimeError("stale database connection")
        return super().cursor()


class FailingCursor(FakeCursor):
    def execute(self, sql, params=None):
        raise RuntimeError("database write failed")


class FailingConnection(FakeConnection):
    def __init__(self):
        super().__init__()
        self.cursor_obj = FailingCursor()


class VisionOrchestratorRepositoryTest(unittest.TestCase):
    def test_mark_workflow_running_reconnects_stale_connection(self):
        conn = StaleConnection()
        repo = VisionOrchestratorRepository(conn)

        repo.mark_workflow_running("task-reconnect")

        self.assertEqual(conn.ping_calls, [True])
        self.assertEqual(conn.commits, 1)

    def test_mark_workflow_running_rolls_back_failed_transaction(self):
        conn = FailingConnection()
        repo = VisionOrchestratorRepository(conn)

        with self.assertRaisesRegex(RuntimeError, "database write failed"):
            repo.mark_workflow_running("task-rollback")

        self.assertEqual(conn.rollbacks, 1)
        self.assertEqual(conn.commits, 0)

    def test_mark_workflow_running_upserts_visual_stage(self):
        conn = FakeConnection()
        repo = VisionOrchestratorRepository(conn)

        repo.mark_workflow_running("task-1")

        sql, params = conn.cursor_obj.executed[0]
        self.assertIn("lesson_ai_workflow", sql)
        self.assertIn("ON DUPLICATE KEY UPDATE", sql)
        self.assertEqual(params["task_id"], "task-1")
        self.assertEqual(params["stage_node"], 7)
        self.assertEqual(params["status"], 2)
        self.assertEqual(conn.commits, 1)

    def test_clear_previous_results_deletes_task_details(self):
        conn = FakeConnection()
        repo = VisionOrchestratorRepository(conn)

        repo.clear_previous_results("task-1")

        executed_sql = [sql for sql, _ in conn.cursor_obj.executed]
        self.assertTrue(any("lesson_behavior_timeline" in sql for sql in executed_sql))
        self.assertTrue(any("lesson_snapshot_event" in sql for sql in executed_sql))
        self.assertTrue(any("lesson_student_behavior_stat" in sql for sql in executed_sql))
        self.assertEqual(conn.commits, 1)

    def test_load_indicator_definitions_by_codes(self):
        repo = VisionOrchestratorRepository(FakeConnection())

        definitions = repo.load_indicator_definitions(["E2-01"])

        self.assertEqual(definitions["E2-01"], IndicatorDefinition(
            indicator_id="id-e2",
            indicator_code="E2-01",
            indicator_name="到课率",
            unit="%",
            score_rule=None,
        ))

    def test_upsert_indicator_results_writes_percent_raw_value(self):
        conn = FakeConnection()
        repo = VisionOrchestratorRepository(conn)
        definition = IndicatorDefinition(
            indicator_id="id-e2",
            indicator_code="E2-01",
            indicator_name="到课率",
            unit="%",
            score_rule=None,
        )

        repo.upsert_indicator_results(
            "task-1",
            {"E2-01": IndicatorMetric(code="E2-01", value=0.875, score=87.5)},
            {"E2-01": definition},
        )

        sql, params = conn.cursor_obj.executemany_calls[0]
        self.assertIn("indicator_score_result", sql)
        self.assertIn("ON DUPLICATE KEY UPDATE", sql)
        self.assertEqual(params[0]["raw_value"], 87.5)
        self.assertEqual(params[0]["score"], 87.5)

    def test_upsert_student_behavior_stats_writes_task_behavior_rows(self):
        conn = FakeConnection()
        repo = VisionOrchestratorRepository(conn)

        repo.upsert_student_behavior_stats(
            "task-1",
            [
                StudentBehaviorStat(behavior_type=1, detect_count=12, peak_period_desc="3′–5′"),
                StudentBehaviorStat(behavior_type=3, detect_count=2, peak_period_desc="10′"),
            ],
        )

        sql, params = conn.cursor_obj.executemany_calls[0]
        self.assertIn("lesson_student_behavior_stat", sql)
        self.assertIn("ON DUPLICATE KEY UPDATE", sql)
        self.assertEqual(params[0]["task_id"], "task-1")
        self.assertEqual(params[0]["behavior_type"], 1)
        self.assertEqual(params[0]["detect_count"], 12)
        self.assertEqual(params[0]["peak_period_desc"], "3′–5′")
        self.assertEqual(params[0]["confidence_level"], 2)

    def test_insert_snapshot_events_writes_selection_mode_one(self):
        conn = FakeConnection()
        repo = VisionOrchestratorRepository(conn)

        repo.insert_snapshot_events(
            "task-1",
            [
                {
                    "target_type": 1,
                    "record_type": 2,
                    "behavior_type": 3,
                    "capture_second": 12,
                    "confidence_score": 0.9,
                    "image_url": "https://example.test/snapshot.jpg",
                }
            ],
        )

        sql, params = conn.cursor_obj.executemany_calls[0]
        self.assertIn("selection_mode", sql)
        self.assertIn("selection_mode = VALUES(selection_mode)", sql)
        self.assertEqual(params[0]["selection_mode"], 1)


if __name__ == "__main__":
    unittest.main()
