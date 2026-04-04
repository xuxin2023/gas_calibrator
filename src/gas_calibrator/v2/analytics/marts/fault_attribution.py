from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any


FAULT_RULES = {
    "humidity_path": ("humidity", "h2o", "dewpoint", "moisture"),
    "pressure_path": ("pressure", "leak", "flow", "pump"),
    "communication": ("comm", "serial", "timeout", "modbus", "connection"),
    "data_quality": ("quality", "stability", "missing", "exclude", "invalid"),
    "runtime_alarm": ("alarm", "critical", "warning", "runtime_log"),
}


def _classify_tokens(*tokens: str) -> str:
    text = " ".join(token for token in tokens if token).lower()
    for category, keywords in FAULT_RULES.items():
        if any(keyword in text for keyword in keywords):
            return category
    return "uncategorized"


def build_fault_attribution(features: dict[str, Any], **_: Any) -> dict[str, Any]:
    point_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for point in features.get("points", []):
        point_groups[str(point.get("run_id"))].append(point)

    runs_output: list[dict[str, Any]] = []
    overall = Counter()
    for run in features.get("runs", []):
        categories = Counter()
        alarm_categories = run.get("alarm_categories") or {}
        for category_name, count in alarm_categories.items():
            mapped = _classify_tokens(str(category_name), "runtime_alarm")
            categories[mapped] += int(count or 0)

        for point in point_groups.get(str(run.get("run_id")), []):
            rule_names = [str(name) for name in point.get("failed_qc_rule_names") or []]
            messages = [str(message) for message in point.get("failed_qc_messages") or []]
            if not rule_names and not messages:
                continue
            mapped = _classify_tokens(" ".join(rule_names), " ".join(messages))
            categories[mapped] += max(int(point.get("qc_fail_count") or 0), 1)

        dominant_category = None
        dominant_count = 0
        if categories:
            dominant_category, dominant_count = max(categories.items(), key=lambda item: (item[1], item[0]))
        overall.update(categories)
        runs_output.append(
            {
                "run_id": run.get("run_id"),
                "status": run.get("status"),
                "dominant_category": dominant_category,
                "dominant_count": dominant_count,
                "category_counts": dict(categories),
            }
        )

    return {
        "run_count": len(runs_output),
        "overall_category_counts": dict(overall),
        "runs": runs_output,
    }
