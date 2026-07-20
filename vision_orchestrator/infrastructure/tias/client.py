import logging
from typing import Any, Dict

import requests

from .registry import TiasInstanceStatus


logger = logging.getLogger(__name__)


class TiasHttpError(RuntimeError):
    def __init__(self, message: str, retryable: bool, status_code: int | None = None):
        super().__init__(message)
        self.retryable = retryable
        self.status_code = status_code


class TiasHttpClient:
    def __init__(self, timeout_seconds: int = 60):
        self.timeout_seconds = int(timeout_seconds)

    def infer_student(self, instance: TiasInstanceStatus, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post(instance, "/ImageDetect/student/v1.0.0", payload)

    def infer_teacher(self, instance: TiasInstanceStatus, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post(instance, "/ImageDetect/teacher/v1.0.0", payload)

    def _post(self, instance: TiasInstanceStatus, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{instance.base_url.rstrip('/')}{path}"
        try:
            response = requests.post(url, json=payload, timeout=self.timeout_seconds)
        except requests.Timeout as exc:
            raise TiasHttpError(f"TIAS 请求超时 instance_id={instance.instance_id}", retryable=True) from exc
        except requests.RequestException as exc:
            raise TiasHttpError(f"TIAS 请求失败 instance_id={instance.instance_id}: {exc}", retryable=True) from exc
        if response.status_code in {429, 503, 500, 502, 504}:
            raise TiasHttpError(
                f"TIAS 可重试失败 instance_id={instance.instance_id} status={response.status_code}",
                retryable=True,
                status_code=response.status_code,
            )
        if 400 <= response.status_code < 500:
            raise TiasHttpError(
                f"TIAS 参数失败 instance_id={instance.instance_id} status={response.status_code}",
                retryable=False,
                status_code=response.status_code,
            )
        try:
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            raise TiasHttpError(f"TIAS 响应失败 instance_id={instance.instance_id}: {exc}", retryable=True) from exc
        except ValueError as exc:
            raise TiasHttpError(f"TIAS 响应不是 JSON instance_id={instance.instance_id}", retryable=False) from exc
