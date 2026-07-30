"""Pure sampling contracts for offline validation."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from gas_calibrator.utils import as_float, safe_get

STANDARD_ANALYZER_ROW_FIELDS = (
    ("co2_ppm", "co2_ppm"),
    ("h2o_mmol", "h2o_mmol"),
    ("co2_ratio_f", "co2_ratio_f"),
    ("h2o_ratio_f", "h2o_ratio_f"),
    ("co2_signal", "co2_signal"),
    ("h2o_signal", "h2o_signal"),
    ("ref_signal", "ref_signal"),
    ("analyzer_chamber_temp_c", "analyzer_chamber_temp_c"),
    ("case_temp_c", "case_temp_c"),
)

__all__ = [
    "STANDARD_ANALYZER_ROW_FIELDS",
    "evaluate_sample_quality",
    "filter_samples_for_point",
    "normalize_snapshot",
    "pick_humidity_value",
    "pick_numeric",
    "pick_text",
    "sample_span",
    "sampling_result_to_row",
    "sanitize_humidity_value",
    "snapshot_has_data",
    "snapshot_retry_reason",
    "standard_analyzer_row_values",
    "summarize_analyzer_integrity",
]


def normalize_snapshot(snapshot: Any) -> dict[str, Any]:
    if not isinstance(snapshot, dict):
        return {}
    normalized: dict[str, Any] = {}
    data = safe_get(snapshot, "data", default={})
    if isinstance(data, dict):
        normalized.update(data)
    normalized.update(snapshot)
    return normalized


def pick_numeric(
    snapshot: dict[str, Any],
    *keys: str,
) -> float | None:
    for key in keys:
        value = as_float(snapshot.get(key))
        if value is not None:
            return float(value)
    return None


def pick_text(snapshot: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = snapshot.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def sanitize_humidity_value(value: float | None) -> float | None:
    if value is None:
        return None
    numeric = float(value)
    if numeric < 0.0 or numeric > 100.0:
        return None
    return numeric


def pick_humidity_value(
    snapshot: dict[str, Any],
) -> float | None:
    value = pick_numeric(
        snapshot,
        "humidity_pct",
        "rh_pct",
        "humidity",
        "Uw",
        "Ui",
    )
    return sanitize_humidity_value(value)


def snapshot_has_data(snapshot: dict[str, Any]) -> bool:
    for value in snapshot.values():
        if value is None:
            continue
        if isinstance(value, dict) and not value:
            continue
        if isinstance(value, (list, tuple, set)) and not value:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return True
    return False


def snapshot_retry_reason(
    snapshot: Any,
    *,
    required_keys: tuple[str, ...],
    retry_on_empty: bool,
) -> str | None:
    normalized = normalize_snapshot(snapshot)
    if required_keys and pick_numeric(normalized, *required_keys) is None:
        return f"missing numeric data for keys={','.join(required_keys)}"
    if retry_on_empty and not snapshot_has_data(normalized):
        return "empty snapshot"
    return None


def sample_span(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    return float(max(values) - min(values))


def evaluate_sample_quality(
    rows: list[dict[str, Any]],
    *,
    quality_config: Any,
) -> tuple[bool, dict[str, float]]:
    if not isinstance(quality_config, dict) or not quality_config.get(
        "enabled",
        False,
    ):
        return True, {}
    limits = {
        "co2_ppm": quality_config.get("max_span_co2_ppm"),
        "h2o_mmol": quality_config.get("max_span_h2o_mmol"),
        "pressure_hpa": quality_config.get("max_span_pressure_hpa"),
        "dewpoint_c": quality_config.get("max_span_dewpoint_c"),
    }
    spans: dict[str, float] = {}
    ok = True
    for key, raw_limit in limits.items():
        if raw_limit is None:
            continue
        values = [
            float(row[key])
            for row in rows
            if row.get(key) is not None
        ]
        if not values:
            continue
        span = sample_span(values)
        spans[key] = span
        if span > float(raw_limit):
            ok = False
    return ok, spans


def summarize_analyzer_integrity(
    rows: list[dict[str, Any]],
    *,
    analyzer_labels: list[str],
) -> dict[str, Any]:
    expected = len(analyzer_labels)
    with_frame: list[str] = []
    usable: list[str] = []
    missing: list[str] = []
    unusable: list[str] = []
    for label in analyzer_labels:
        prefix = str(label or "").lower().replace(" ", "_")
        has_frame = any(
            bool(row.get(f"{prefix}_frame_has_data"))
            for row in rows
        )
        has_usable = any(
            bool(row.get(f"{prefix}_frame_usable"))
            for row in rows
        )
        display = str(label or "").upper()
        if has_frame:
            with_frame.append(display)
        else:
            missing.append(display)
        if has_usable:
            usable.append(display)
        elif has_frame:
            unusable.append(display)
    usable_count = len(usable)
    with_frame_count = len(with_frame)
    coverage_text = (
        f"{usable_count}/{expected}"
        if expected
        else "0/0"
    )
    integrity = (
        "完整"
        if expected and usable_count == expected
        else "部分可用"
    )
    if expected == 0:
        integrity = "无分析仪"
    elif usable_count == 0 and with_frame_count == 0:
        integrity = "无帧"
    elif usable_count == 0:
        integrity = "仅异常帧"
    elif missing and unusable:
        integrity = "部分缺失且含异常帧"
    elif missing:
        integrity = "部分缺失"
    elif unusable:
        integrity = "含异常帧"
    return {
        "analyzer_expected_count": expected,
        "analyzer_with_frame_count": with_frame_count,
        "analyzer_usable_count": usable_count,
        "analyzer_coverage_text": coverage_text,
        "analyzer_integrity": integrity,
        "analyzer_missing_labels": ",".join(missing),
        "analyzer_unusable_labels": ",".join(unusable),
    }


def standard_analyzer_row_values(result: Any) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for row_key, attr_name in STANDARD_ANALYZER_ROW_FIELDS:
        row[row_key] = getattr(result, attr_name, None)
    return row


def sampling_result_to_row(result: Any) -> dict[str, Any]:
    return {
        "timestamp": result.timestamp.isoformat(),
        "point_index": result.point.index,
        "temperature_c": result.point.temperature_c,
        "co2_ppm": result.point.co2_ppm,
        "co2_group": result.point.co2_group,
        "cylinder_nominal_ppm": result.point.cylinder_nominal_ppm,
        "humidity_pct": result.point.humidity_pct,
        "route": result.point.route,
        "analyzer_id": result.analyzer_id,
        "sample_co2_ppm": result.co2_ppm,
        "sample_h2o_mmol": result.h2o_mmol,
        "h2o_signal": result.h2o_signal,
        "co2_signal": result.co2_signal,
        "co2_ratio_f": result.co2_ratio_f,
        "co2_ratio_raw": result.co2_ratio_raw,
        "h2o_ratio_f": result.h2o_ratio_f,
        "h2o_ratio_raw": result.h2o_ratio_raw,
        "ref_signal": result.ref_signal,
        "pressure_hpa": result.pressure_hpa,
        "pressure_gauge_hpa": result.pressure_gauge_hpa,
        "pressure_reference_status": result.pressure_reference_status,
        "thermometer_temp_c": result.thermometer_temp_c,
        "thermometer_reference_status": (
            result.thermometer_reference_status
        ),
        "dew_point_c": result.dew_point_c,
        "analyzer_pressure_kpa": result.analyzer_pressure_kpa,
        "analyzer_chamber_temp_c": result.analyzer_chamber_temp_c,
        "case_temp_c": result.case_temp_c,
        "frame_has_data": result.frame_has_data,
        "frame_usable": result.frame_usable,
        "frame_status": result.frame_status,
        "sample_index": result.sample_index,
    }


def filter_samples_for_point(
    samples: Iterable[Any],
    point: Any,
    *,
    phase: str = "",
    point_tag: str = "",
) -> list[Any]:
    resolved_tag = str(point_tag or "").strip()
    resolved_phase = str(phase or "").strip().lower()
    selected: list[Any] = []
    for result in samples:
        if (
            result.point.index != point.index
            or result.point.route != point.route
        ):
            continue
        if (
            resolved_tag
            and str(
                getattr(result, "point_tag", "") or ""
            ).strip()
            != resolved_tag
        ):
            continue
        result_phase = str(
            getattr(result, "point_phase", "") or ""
        ).strip().lower()
        if (
            resolved_phase
            and result_phase not in {"", resolved_phase}
        ):
            continue
        selected.append(result)
    return selected
