from __future__ import annotations

from collections import Counter
from typing import Any


def _is_status_abnormal(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    return text not in {"ok", "normal", "ready", "usable"}


def _relative_delta(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in (None, 0):
        return None
    return abs(float(current) - float(previous)) / abs(float(previous))


def build_signal_anomaly(features: dict[str, Any], **_: Any) -> dict[str, Any]:
    frames = sorted(
        features.get("frame_features", []),
        key=lambda item: (
            str(item.get("analyzer_label") or ""),
            str(item.get("sample_ts") or ""),
            int(item.get("sample_index") or 0),
        ),
    )
    anomalies: dict[tuple[Any, ...], dict[str, Any]] = {}
    category_counts: Counter[str] = Counter()

    def ensure_frame(frame: dict[str, Any]) -> dict[str, Any]:
        key = (
            frame.get("run_uuid"),
            frame.get("point_id"),
            frame.get("analyzer_label"),
            frame.get("sample_index"),
            frame.get("sample_ts"),
        )
        if key not in anomalies:
            anomalies[key] = {
                "run_id": frame.get("run_id"),
                "point_id": frame.get("point_id"),
                "point_sequence": frame.get("point_sequence"),
                "analyzer_label": frame.get("analyzer_label"),
                "sample_index": frame.get("sample_index"),
                "sample_ts": frame.get("sample_ts"),
                "categories": [],
            }
        return anomalies[key]

    previous_by_analyzer: dict[str, dict[str, Any]] = {}
    for frame in frames:
        reasons: list[str] = []
        if not frame.get("frame_has_data", False):
            reasons.append("missing_frame")
        if not frame.get("frame_usable", False):
            reasons.append("unusable_frame")
        if _is_status_abnormal(frame.get("analyzer_status")):
            reasons.append("analyzer_status")
        co2_ratio_gap = None
        if frame.get("co2_ratio_f") is not None and frame.get("co2_ratio_raw") is not None:
            co2_ratio_gap = abs(float(frame["co2_ratio_f"]) - float(frame["co2_ratio_raw"]))
        if co2_ratio_gap is not None and co2_ratio_gap > 0.01:
            reasons.append("co2_ratio_gap")
        h2o_ratio_gap = None
        if frame.get("h2o_ratio_f") is not None and frame.get("h2o_ratio_raw") is not None:
            h2o_ratio_gap = abs(float(frame["h2o_ratio_f"]) - float(frame["h2o_ratio_raw"]))
        if h2o_ratio_gap is not None and h2o_ratio_gap > 0.01:
            reasons.append("h2o_ratio_gap")
        if frame.get("pressure_hpa") is not None and frame.get("pressure_kpa") is not None:
            mismatch = abs((float(frame["pressure_kpa"]) * 10.0) - float(frame["pressure_hpa"]))
            if mismatch > 25.0:
                reasons.append("pressure_mismatch")

        previous = previous_by_analyzer.get(str(frame.get("analyzer_label") or ""))
        if previous is not None:
            for category, key in (
                ("ref_signal_jump", "ref_signal"),
                ("co2_signal_jump", "co2_signal"),
                ("h2o_signal_jump", "h2o_signal"),
            ):
                delta = _relative_delta(frame.get(key), previous.get(key))
                if delta is not None and delta > 0.10:
                    reasons.append(category)

        if reasons:
            entry = ensure_frame(frame)
            for reason in reasons:
                if reason not in entry["categories"]:
                    entry["categories"].append(reason)
                    category_counts[reason] += 1

        previous_by_analyzer[str(frame.get("analyzer_label") or "")] = frame

    analyzer_counts = Counter(str(item["analyzer_label"]) for item in anomalies.values())
    return {
        "frame_count": len(frames),
        "anomaly_count": len(anomalies),
        "category_counts": dict(category_counts),
        "analyzer_counts": dict(analyzer_counts),
        "anomalies": list(anomalies.values()),
    }
