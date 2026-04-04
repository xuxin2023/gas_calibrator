from __future__ import annotations

from statistics import mean
from typing import Any


def _average(values: list[float | int | None]) -> float | None:
    numbers = [float(item) for item in values if item is not None]
    if not numbers:
        return None
    return mean(numbers)


def _coverage(items: list[dict[str, Any]], key: str, *, truthy_values: set[str] | None = None) -> float:
    if not items:
        return 0.0
    if truthy_values is None:
        matched = sum(1 for item in items if item.get(key))
    else:
        matched = sum(1 for item in items if str(item.get(key) or "").strip().lower() in truthy_values)
    return matched / len(items)


def build_run_kpis(features: dict[str, Any], **_: Any) -> dict[str, Any]:
    runs = list(features.get("runs", []))
    completed = sum(1 for item in runs if item.get("status") == "completed")
    failed = sum(1 for item in runs if item.get("status") == "failed")
    aborted = sum(1 for item in runs if item.get("status") == "aborted")
    return {
        "run_count": len(runs),
        "completed_run_count": completed,
        "failed_run_count": failed,
        "aborted_run_count": aborted,
        "run_success_rate": 0.0 if not runs else completed / len(runs),
        "average_duration_s": _average([item.get("duration_s") for item in runs]),
        "average_points_total": _average([item.get("total_points") for item in runs]),
        "average_warning_count": _average([item.get("warnings") for item in runs]),
        "average_error_count": _average([item.get("errors") for item in runs]),
        "manifest_coverage": _coverage(runs, "raw_manifest_present"),
        "qc_enrich_coverage": _coverage(runs, "enrich_qc_status", truthy_values={"loaded", "completed"}),
        "fit_enrich_coverage": _coverage(runs, "enrich_fit_imported_results"),
        "ai_summary_coverage": _coverage(runs, "ai_summary_status", truthy_values={"loaded", "completed", "fallback"}),
        "postprocess_summary_coverage": _coverage(runs, "postprocess_summary_status", truthy_values={"loaded"}),
    }
