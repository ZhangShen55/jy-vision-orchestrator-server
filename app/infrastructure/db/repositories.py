from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, Iterable, Mapping, Optional

from app.domain.behavior_stats import StudentBehaviorStat
from app.domain.ids import stable_id
from app.domain.metrics import IndicatorMetric


VISUAL_STAGE_NODE = 7
STATUS_RUNNING = 2
STATUS_SUCCESS = 3
STATUS_FAILED = 4


@dataclass(frozen=True)
class IndicatorDefinition:
    indicator_id: str
    indicator_code: str
    indicator_name: Optional[str]
    unit: Optional[str]
    score_rule: Optional[object]


class VisionOrchestratorRepository:
    def __init__(self, connection):
        self.connection = connection

    @contextmanager
    def _cursor(self):
        self.connection.ping(reconnect=True)
        with self.connection.cursor() as cursor:
            yield cursor

    @contextmanager
    def _transaction_cursor(self):
        try:
            with self._cursor() as cursor:
                yield cursor
            self.connection.commit()
        except Exception:
            try:
                self.connection.rollback()
            except Exception:
                pass
            raise

    def mark_workflow_running(self, task_id: str) -> None:
        sql = """
            INSERT INTO lesson_ai_workflow
                (workflow_node_id, task_id, stage_node, status, progress, note, error_msg, started_at, completed_at, create_by, update_by)
            VALUES
                (%(workflow_node_id)s, %(task_id)s, %(stage_node)s, %(status)s, %(progress)s, %(note)s, NULL, NOW(), NULL, 'cv-worker', 'cv-worker')
            ON DUPLICATE KEY UPDATE
                status = VALUES(status),
                progress = VALUES(progress),
                note = VALUES(note),
                error_msg = NULL,
                started_at = COALESCE(started_at, NOW()),
                completed_at = NULL,
                update_by = 'cv-worker'
        """
        params = {
            "workflow_node_id": stable_id("workflow", task_id, VISUAL_STAGE_NODE),
            "task_id": task_id,
            "stage_node": VISUAL_STAGE_NODE,
            "status": STATUS_RUNNING,
            "progress": 0,
            "note": "视觉分析处理中",
        }
        with self._transaction_cursor() as cursor:
            cursor.execute(sql, params)

    def mark_workflow_success(self, task_id: str, note: str = "视觉分析完成") -> None:
        self._update_workflow_final(task_id, STATUS_SUCCESS, 100, note, None)

    def mark_workflow_failed(self, task_id: str, error_msg: str) -> None:
        self._update_workflow_final(task_id, STATUS_FAILED, 100, "视觉分析失败", error_msg[:500])

    def _update_workflow_final(
            self,
            task_id: str,
            status: int,
            progress: int,
            note: str,
            error_msg: Optional[str]) -> None:
        sql = """
            INSERT INTO lesson_ai_workflow
                (workflow_node_id, task_id, stage_node, status, progress, note, error_msg, started_at, completed_at, create_by, update_by)
            VALUES
                (%(workflow_node_id)s, %(task_id)s, %(stage_node)s, %(status)s, %(progress)s, %(note)s, %(error_msg)s, NOW(), NOW(), 'cv-worker', 'cv-worker')
            ON DUPLICATE KEY UPDATE
                status = VALUES(status),
                progress = VALUES(progress),
                note = VALUES(note),
                error_msg = VALUES(error_msg),
                completed_at = NOW(),
                update_by = 'cv-worker'
        """
        with self._transaction_cursor() as cursor:
            cursor.execute(sql, {
                "workflow_node_id": stable_id("workflow", task_id, VISUAL_STAGE_NODE),
                "task_id": task_id,
                "stage_node": VISUAL_STAGE_NODE,
                "status": status,
                "progress": progress,
                "note": note,
                "error_msg": error_msg,
            })

    def clear_previous_results(self, task_id: str) -> None:
        with self._transaction_cursor() as cursor:
            cursor.execute("DELETE FROM lesson_behavior_timeline WHERE task_id = %(task_id)s", {"task_id": task_id})
            cursor.execute("DELETE FROM lesson_snapshot_event WHERE task_id = %(task_id)s", {"task_id": task_id})
            cursor.execute("DELETE FROM lesson_student_behavior_stat WHERE task_id = %(task_id)s", {"task_id": task_id})

    def insert_timeline_rows(self, task_id: str, rows: Iterable[Mapping[str, object]]) -> None:
        params = []
        for row in rows:
            metric_type = int(row["metric_type"])
            minute_no = int(row["minute_no"])
            params.append({
                "behavior_timeline_id": stable_id("timeline", task_id, metric_type, minute_no),
                "task_id": task_id,
                "metric_type": metric_type,
                "minute_no": minute_no,
                "metric_value": row["metric_value"],
            })
        if not params:
            return
        sql = """
            INSERT INTO lesson_behavior_timeline
                (behavior_timeline_id, task_id, metric_type, minute_no, metric_value, create_by, update_by)
            VALUES
                (%(behavior_timeline_id)s, %(task_id)s, %(metric_type)s, %(minute_no)s, %(metric_value)s, 'cv-worker', 'cv-worker')
            ON DUPLICATE KEY UPDATE
                metric_value = VALUES(metric_value),
                update_by = 'cv-worker'
        """
        with self._transaction_cursor() as cursor:
            cursor.executemany(sql, params)

    def insert_snapshot_events(self, task_id: str, rows: Iterable[Mapping[str, object]]) -> None:
        params = []
        for row in rows:
            image_url = row.get("image_url")
            params.append({
                "snapshot_event_id": row.get("snapshot_event_id") or stable_id("snapshot", task_id, row["capture_second"], image_url),
                "task_id": task_id,
                "target_type": row["target_type"],
                "record_type": row["record_type"],
                "behavior_type": row.get("behavior_type"),
                "capture_second": row["capture_second"],
                "confidence_score": row.get("confidence_score", 1.0),
                "selection_mode": 1,
                "image_url": image_url,
            })
        if not params:
            return
        sql = """
            INSERT INTO lesson_snapshot_event
                (snapshot_event_id, task_id, target_type, record_type, behavior_type, capture_second, confidence_score, selection_mode, image_url, create_by, update_by)
            VALUES
                (%(snapshot_event_id)s, %(task_id)s, %(target_type)s, %(record_type)s, %(behavior_type)s, %(capture_second)s, %(confidence_score)s, %(selection_mode)s, %(image_url)s, 'cv-worker', 'cv-worker')
            ON DUPLICATE KEY UPDATE
                confidence_score = VALUES(confidence_score),
                selection_mode = VALUES(selection_mode),
                image_url = VALUES(image_url),
                update_by = 'cv-worker'
        """
        with self._transaction_cursor() as cursor:
            cursor.executemany(sql, params)

    def upsert_student_behavior_stats(self, task_id: str, rows: Iterable[StudentBehaviorStat]) -> None:
        params = []
        for row in rows:
            params.append({
                "behavior_stat_id": stable_id("student_behavior_stat", task_id, row.behavior_type),
                "task_id": task_id,
                "behavior_type": row.behavior_type,
                "detect_count": row.detect_count,
                "peak_period_desc": row.peak_period_desc,
                "confidence_level": row.confidence_level,
            })
        if not params:
            return
        sql = """
            INSERT INTO lesson_student_behavior_stat
                (behavior_stat_id, task_id, behavior_type, detect_count, peak_period_desc, confidence_level, create_by, update_by)
            VALUES
                (%(behavior_stat_id)s, %(task_id)s, %(behavior_type)s, %(detect_count)s, %(peak_period_desc)s, %(confidence_level)s, 'cv-worker', 'cv-worker')
            ON DUPLICATE KEY UPDATE
                detect_count = VALUES(detect_count),
                peak_period_desc = VALUES(peak_period_desc),
                confidence_level = VALUES(confidence_level),
                update_by = 'cv-worker'
        """
        with self._transaction_cursor() as cursor:
            cursor.executemany(sql, params)

    def load_indicator_definitions(self, indicator_codes: Iterable[str]) -> Dict[str, IndicatorDefinition]:
        codes = list(indicator_codes)
        if not codes:
            return {}
        placeholders = ", ".join(["%s"] * len(codes))
        sql = f"""
            SELECT indicator_id, indicator_code, indicator_name, unit, score_rule
            FROM indicator
            WHERE indicator_code IN ({placeholders})
        """
        with self._cursor() as cursor:
            cursor.execute(sql, codes)
            rows = cursor.fetchall()
        definitions = {}
        for row in rows:
            definitions[row["indicator_code"]] = IndicatorDefinition(
                indicator_id=row["indicator_id"],
                indicator_code=row["indicator_code"],
                indicator_name=row.get("indicator_name"),
                unit=row.get("unit"),
                score_rule=row.get("score_rule"),
            )
        return definitions

    def upsert_indicator_results(
            self,
            task_id: str,
            indicators: Mapping[str, IndicatorMetric],
            definitions: Mapping[str, IndicatorDefinition]) -> None:
        params = []
        for code, metric in indicators.items():
            definition = definitions.get(code)
            if definition is None:
                continue
            raw_value = round(metric.value * 100.0, 4) if definition.unit == "%" else metric.value
            params.append({
                "score_result_id": stable_id("indicator", task_id, definition.indicator_id),
                "task_id": task_id,
                "indicator_id": definition.indicator_id,
                "indicator_code": definition.indicator_code,
                "indicator_name": definition.indicator_name,
                "raw_value": raw_value,
                "unit": definition.unit,
                "score": metric.score,
                "reason": "视觉分析生成",
            })
        if not params:
            return
        sql = """
            INSERT INTO indicator_score_result
                (score_result_id, task_id, indicator_id, indicator_code, indicator_name, raw_value, unit, score, reason, create_by, update_by)
            VALUES
                (%(score_result_id)s, %(task_id)s, %(indicator_id)s, %(indicator_code)s, %(indicator_name)s, %(raw_value)s, %(unit)s, %(score)s, %(reason)s, 'cv-worker', 'cv-worker')
            ON DUPLICATE KEY UPDATE
                indicator_code = VALUES(indicator_code),
                indicator_name = VALUES(indicator_name),
                raw_value = VALUES(raw_value),
                unit = VALUES(unit),
                score = VALUES(score),
                reason = VALUES(reason),
                update_by = 'cv-worker'
        """
        with self._transaction_cursor() as cursor:
            cursor.executemany(sql, params)
