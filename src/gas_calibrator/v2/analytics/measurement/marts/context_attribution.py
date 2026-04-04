from __future__ import annotations

from collections import Counter
from typing import Any


def _is_status_abnormal(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    return text not in {"ok", "normal", "ready", "usable"}


def _contains_any(values: list[str], keywords: tuple[str, ...]) -> bool:
    lowered = " ".join(values).lower()
    return any(keyword in lowered for keyword in keywords)


def build_context_attribution(features: dict[str, Any], **_: Any) -> dict[str, Any]:
    affected: list[dict[str, Any]] = []
    category_counts: Counter[str] = Counter()

    for frame in features.get("frame_features", []):
        if not (
            not frame.get("frame_usable", False)
            or _is_status_abnormal(frame.get("analyzer_status"))
            or int(frame.get("qc_fail_count") or 0) > 0
        ):
            continue

        categories: list[str] = []
        expected = frame.get("analyzer_expected_count")
        usable = frame.get("analyzer_usable_count")
        if (
            (expected not in (None, 0) and usable is not None and float(usable) < float(expected))
            or frame.get("analyzer_missing_labels")
            or frame.get("analyzer_unusable_labels")
        ):
            categories.append("coverage_context")

        pressure_hpa = frame.get("pressure_hpa")
        pressure_kpa = frame.get("pressure_kpa")
        if (
            pressure_hpa is not None
            and (float(pressure_hpa) < 950.0 or float(pressure_hpa) > 1050.0)
        ) or (
            pressure_hpa is not None
            and pressure_kpa is not None
            and abs((float(pressure_kpa) * 10.0) - float(pressure_hpa)) > 25.0
        ):
            categories.append("pressure_context")

        failed_text = [str(item) for item in frame.get("failed_qc_rule_names", []) + frame.get("failed_qc_messages", [])]
        has_hgen = any(str(key).startswith("hgen_") and frame.get(key) not in (None, "") for key in frame)
        if has_hgen or _contains_any(failed_text, ("humidity", "h2o", "dew")):
            categories.append("humidity_context")

        if (frame.get("stability_time_s") or 0) and float(frame.get("stability_time_s") or 0) > 60.0:
            categories.append("timing_context")
        elif (frame.get("total_time_s") or 0) and float(frame.get("total_time_s") or 0) > 180.0:
            categories.append("timing_context")

        chamber_temp = frame.get("context_chamber_temp_c")
        if chamber_temp is not None and (float(chamber_temp) < 0.0 or float(chamber_temp) > 35.0):
            categories.append("thermal_context")

        if not categories:
            categories.append("general_context")

        affected.append(
            {
                "run_id": frame.get("run_id"),
                "point_id": frame.get("point_id"),
                "point_sequence": frame.get("point_sequence"),
                "analyzer_label": frame.get("analyzer_label"),
                "sample_index": frame.get("sample_index"),
                "sample_ts": frame.get("sample_ts"),
                "categories": categories,
            }
        )
        for category in categories:
            category_counts[category] += 1

    dominant_category = None
    if category_counts:
        dominant_category = max(category_counts.items(), key=lambda item: item[1])[0]
    return {
        "affected_frame_count": len(affected),
        "category_counts": dict(category_counts),
        "dominant_category": dominant_category,
        "frames": affected,
    }
