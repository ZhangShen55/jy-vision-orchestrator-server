import json
from typing import Any, Mapping, Optional, Sequence


def _as_rule(rule: Optional[Any]) -> Optional[Mapping[str, Any]]:
    if rule is None:
        return None
    if isinstance(rule, str):
        if not rule.strip():
            return None
        return json.loads(rule)
    if isinstance(rule, Mapping):
        return rule
    return None


def _default_score(ratio_value: float) -> float:
    return round(min(1.0, max(0.0, ratio_value)) * 100.0, 2)


def _interpolate(value: float, left: Mapping[str, Any], right: Mapping[str, Any]) -> float:
    left_value = float(left["value"])
    right_value = float(right["value"])
    left_score = float(left["score"])
    right_score = float(right["score"])
    if right_value == left_value:
        return right_score
    ratio = (value - left_value) / (right_value - left_value)
    return left_score + ratio * (right_score - left_score)


def score_indicator(ratio_value: float, rule: Optional[Any]) -> float:
    parsed_rule = _as_rule(rule)
    if not parsed_rule:
        return _default_score(ratio_value)

    thresholds = parsed_rule.get("thresholds")
    if not isinstance(thresholds, Sequence) or len(thresholds) == 0:
        return _default_score(ratio_value)

    value = ratio_value * 100.0
    sorted_thresholds = sorted(
        thresholds,
        key=lambda threshold: float(threshold["value"]),
    )

    if value <= float(sorted_thresholds[0]["value"]):
        score = float(sorted_thresholds[0]["score"])
    elif value >= float(sorted_thresholds[-1]["value"]):
        score = float(sorted_thresholds[-1]["score"])
    else:
        score = float(sorted_thresholds[-1]["score"])
        for left, right in zip(sorted_thresholds, sorted_thresholds[1:]):
            if float(left["value"]) <= value <= float(right["value"]):
                score = _interpolate(value, left, right)
                break

    score_min = float(parsed_rule.get("score_min", 0))
    return round(min(100.0, max(score_min, score)), 2)
