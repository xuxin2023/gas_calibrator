"""Pressure-channel validation for V1.5 calibration evidence.

This module verifies the analyzer internal pressure input ``P`` independently
from CO2/H2O component fitting. It is pure/offline and does not control
pressure hardware, routes, valves, or coefficient storage.
"""

from __future__ import annotations

from collections import Counter
import hashlib
import json
import math
import csv
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from ..senco_format import format_senco_values
from .artifact_rows import normalize_sample_row
from .common import latest_artifact, load_csv_rows
from .reporting import ValidationMetadata, write_validation_report


AMBIENT_PRESSURE_MODES = {
    "",
    "ambient",
    "ambient_open",
    "atmosphere",
    "current_atmosphere",
    "open_flow",
    "open_flow_atmosphere",
    "pressure_only_ambient",
}

_ANALYZER_PREFIX_RE = re.compile(
    r"^(?P<prefix>ga\d{2,})_"
    r"(?P<field>pressure_kpa|analyzer_device_id|device_id|id|frame_usable|mode2_contract_status)$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class PressureChannelConfig:
    min_pairs: int = 3
    mean_abs_delta_hpa: float = 2.0
    max_abs_delta_hpa: float = 3.0
    analyzer_pressure_span_hpa_max: Optional[float] = 2.0
    com22_pressure_span_hpa_max: Optional[float] = 2.0
    pace_com22_mean_abs_delta_hpa_warn: Optional[float] = 2.0
    require_atmosphere_hold_evidence: bool = True
    allowed_pressure_modes: Sequence[str] = tuple(sorted(AMBIENT_PRESSURE_MODES))


@dataclass(frozen=True)
class PressureReferenceTraceabilityResult:
    status: str
    validation_level: str
    reasons: List[str]
    device_id: str = ""
    certificate_id: str = ""
    certificate_hash: str = ""
    valid_until: str = ""
    uncertainty_hpa: Optional[float] = None


@dataclass(frozen=True)
class PressureChannelValidationResult:
    validation_mode: str
    status: str
    validation_level: str
    reason: str
    sample_count: int
    valid_pair_count: int
    rejected_pair_count: int
    analyzer_pressure_mean_hpa: Optional[float]
    com22_pressure_mean_hpa: Optional[float]
    pace_pressure_mean_hpa: Optional[float]
    analyzer_minus_com22_mean_hpa: Optional[float]
    analyzer_minus_com22_max_abs_hpa: Optional[float]
    pace_minus_com22_mean_hpa: Optional[float]
    allowed_for_co2_h2o_formal_work: bool
    traceability: Dict[str, Any]
    measurement_model: Dict[str, Any]
    analyzer_prefix: str = ""
    analyzer_device_id: str = ""


@dataclass(frozen=True)
class PressureSenco9FitConfig:
    min_pairs: int = 10
    min_distinct_pressure_points: int = 3
    min_pressure_span_hpa: float = 300.0
    point_group_resolution_hpa: float = 1.0
    discard_initial_samples_per_pressure_point: int = 0
    max_point_reference_span_hpa: float = 1.0
    max_offset_residual_mean_abs_hpa: float = 1.0
    max_offset_residual_max_abs_hpa: float = 2.0
    max_linear_residual_mean_abs_hpa: float = 0.75
    max_linear_residual_max_abs_hpa: float = 1.5
    max_slope_bias_for_offset_only: float = 0.02
    require_traceable_reference: bool = True


@dataclass(frozen=True)
class PressureSenco9FitResult:
    analyzer_prefix: str
    analyzer_device_id: str
    status: str
    recommendation: str
    reason: str
    sample_count: int
    valid_pair_count: int
    distinct_pressure_points: int
    reference_span_hpa: Optional[float]
    analyzer_span_hpa: Optional[float]
    offset_only_offset_kpa: Optional[float]
    offset_only_residual_mean_abs_hpa: Optional[float]
    offset_only_residual_max_abs_hpa: Optional[float]
    linear_intercept_kpa: Optional[float]
    linear_slope: Optional[float]
    linear_slope_bias: Optional[float]
    linear_residual_mean_abs_hpa: Optional[float]
    linear_residual_max_abs_hpa: Optional[float]
    senco9_candidate_command: str
    write_allowed: bool
    traceability: Dict[str, Any]


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on", "pass", "ok", "verified"}


def _first_value(row: Mapping[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _prefixed_value(row: Mapping[str, Any], prefix: str, key: str) -> Any:
    prefixed = f"{prefix}_{key}" if prefix else key
    value = row.get(prefixed)
    if value in (None, "") and str(prefix or "").strip().lower() in {"", "ga01"}:
        value = row.get(key)
    return value


def _analyzer_device_id(row: Mapping[str, Any], analyzer_prefix: str) -> str:
    prefix = str(analyzer_prefix or "").strip().lower()
    keys = [
        f"{analyzer_prefix}_analyzer_device_id",
        f"{analyzer_prefix}_device_id",
        f"{analyzer_prefix}_id",
        f"{analyzer_prefix}_mode2_device_id",
        f"{analyzer_prefix}_serial_number",
    ]
    if prefix in {"", "ga01"}:
        keys.extend(["analyzer_device_id", "device_id", "id"])
    value = _first_value(
        row,
        keys,
    )
    text = str(value or "").strip()
    if text:
        return text
    token_keys = [f"{analyzer_prefix}_mode2_tokens_json"]
    if prefix in {"", "ga01"}:
        token_keys.append("mode2_tokens_json")
    for key in token_keys:
        raw = str(row.get(key) or "").strip()
        if not raw:
            continue
        try:
            tokens = json.loads(raw)
        except Exception:
            continue
        if isinstance(tokens, list) and len(tokens) >= 2:
            return str(tokens[1] or "").strip()
    return ""


def detect_pressure_analyzer_prefixes(rows: Sequence[Mapping[str, Any]]) -> List[str]:
    """Detect analyzer acquisition prefixes in pressure/sample rows."""

    prefixes: set[str] = set()
    for row in rows:
        selected_prefix = str(row.get("analyzer_prefix") or "").strip().lower()
        if re.fullmatch(r"ga\d{2,}", selected_prefix):
            prefixes.add(selected_prefix)
        for key in row.keys():
            match = _ANALYZER_PREFIX_RE.match(str(key or ""))
            if match:
                prefixes.add(match.group("prefix").lower())
    if prefixes:
        return sorted(prefixes)
    if any(_safe_float(row.get("pressure_kpa")) is not None for row in rows):
        return ["ga01"]
    return ["ga01"]


def _dominant_analyzer_device_id(rows: Sequence[Mapping[str, Any]], analyzer_prefix: str) -> str:
    values = []
    for row in rows:
        selected_prefix = str(row.get("analyzer_prefix") or "").strip().lower()
        selected_id = str(row.get("analyzer_device_id") or "").strip()
        if selected_prefix == str(analyzer_prefix or "").strip().lower() and selected_id:
            values.append(selected_id)
            continue
        values.append(_analyzer_device_id(row, analyzer_prefix))
    clean = [value for value in values if value]
    if not clean:
        return ""
    return Counter(clean).most_common(1)[0][0]


def _span(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    return float(max(values) - min(values))


def _parse_date(value: Any) -> Optional[date]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except Exception:
        return None


def _today(today: Optional[date | str] = None) -> date:
    if isinstance(today, date):
        return today
    parsed = _parse_date(today)
    return parsed or date.today()


def _compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _table_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return _compact_json(value)
    return value


def _table_header(rows: Sequence[Mapping[str, Any]]) -> List[str]:
    header: List[str] = []
    for row in rows:
        for key in row.keys():
            text = str(key)
            if text not in header:
                header.append(text)
    return header


def _primary_pressure_reference(reference: Mapping[str, Any]) -> Mapping[str, Any]:
    if isinstance(reference.get("primary"), Mapping):
        return reference["primary"]  # type: ignore[index]
    if isinstance(reference.get("com22"), Mapping):
        return reference["com22"]  # type: ignore[index]
    if isinstance(reference.get("pressure_reference"), Mapping):
        return reference["pressure_reference"]  # type: ignore[index]
    return reference


def validate_pressure_reference_traceability(
    reference: Mapping[str, Any],
    *,
    today: Optional[date | str] = None,
) -> PressureReferenceTraceabilityResult:
    """Validate the COM22 pressure-reference certificate snapshot."""

    primary = _primary_pressure_reference(reference or {})
    reasons: List[str] = []
    required = (
        "device_id",
        "certificate_id",
        "certificate_uncertainty",
        "valid_until",
        "certificate_hash",
    )
    for key in required:
        if not str(primary.get(key) or "").strip():
            reasons.append(f"missing_{key}")

    uncertainty = _safe_float(primary.get("certificate_uncertainty"))
    if uncertainty is None and str(primary.get("certificate_uncertainty") or "").strip():
        reasons.append("invalid_certificate_uncertainty")

    valid_until = _parse_date(primary.get("valid_until"))
    if primary.get("valid_until") not in (None, "") and valid_until is None:
        reasons.append("invalid_valid_until")
    elif valid_until is not None and valid_until < _today(today):
        reasons.append("certificate_expired")

    status = "pass" if not reasons else "fail"
    return PressureReferenceTraceabilityResult(
        status=status,
        validation_level="formal_pressure_validation" if status == "pass" else "engineering_diagnostic",
        reasons=reasons,
        device_id=str(primary.get("device_id") or ""),
        certificate_id=str(primary.get("certificate_id") or ""),
        certificate_hash=str(primary.get("certificate_hash") or ""),
        valid_until=str(primary.get("valid_until") or ""),
        uncertainty_hpa=uncertainty,
    )


def _pressure_mode(row: Mapping[str, Any]) -> str:
    return str(_first_value(row, ("pressure_mode", "PressureMode")) or "").strip().lower()


def _pressure_target_hpa(row: Mapping[str, Any]) -> Optional[float]:
    return _safe_float(
        _first_value(
            row,
            (
                "pressure_target_hpa",
                "target_pressure_hpa",
                "PressureTarget",
                "pressure_target",
                "target_hpa",
            ),
        )
    )


def _group_pressure_hpa(value: Optional[float], resolution_hpa: float) -> str:
    if value is None:
        return "unlabeled"
    resolution = max(float(resolution_hpa or 1.0), 0.001)
    grouped = round(float(value) / resolution) * resolution
    if abs(grouped - round(grouped)) < 1e-9:
        return str(int(round(grouped)))
    return f"{grouped:.3f}".rstrip("0").rstrip(".")


def _frame_unusable(row: Mapping[str, Any], analyzer_prefix: str) -> bool:
    value = str(_prefixed_value(row, analyzer_prefix, "frame_usable") or "").strip().lower()
    return value in {"false", "0", "no"}


def _atmosphere_hold_status(row: Mapping[str, Any]) -> str:
    return str(
        _first_value(
            row,
            (
                "pressure_atmosphere_hold_status",
                "atmosphere_hold_status",
                "continuous_atmosphere_hold_status",
            ),
        )
        or ""
    ).strip().lower()


def _atmosphere_hold_active(row: Mapping[str, Any]) -> bool:
    return _truthy(
        _first_value(
            row,
            (
                "pressure_atmosphere_hold_active",
                "atmosphere_hold_active",
                "continuous_atmosphere_hold_active",
            ),
        )
    )


def pressure_pair_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    analyzer_prefix: str = "ga01",
    cfg: Optional[PressureChannelConfig] = None,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Extract analyzer-vs-COM22 pressure pairs from sample rows."""

    config = cfg or PressureChannelConfig()
    allowed_modes = {str(item or "").strip().lower() for item in config.allowed_pressure_modes}
    paired: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []

    for index, source in enumerate(rows, start=1):
        row = dict(source)
        requested_prefix = str(analyzer_prefix or "").strip().lower()
        selected_prefix = str(row.get("analyzer_prefix") or "").strip().lower()
        is_long_row = bool(
            re.fullmatch(r"ga\d{2,}", selected_prefix)
            and ("pressure_channel_row_status" in row or "verified_quantity" in row)
        )
        if is_long_row and selected_prefix != requested_prefix:
            continue
        reject: List[str] = []
        mode = _pressure_mode(row)
        if mode not in allowed_modes:
            reject.append(f"non_ambient_pressure_mode({mode or '<blank>'})")
        if _frame_unusable(row, analyzer_prefix):
            reject.append("analyzer_frame_unusable")
        if is_long_row and str(row.get("pressure_channel_row_status") or "").strip().lower() == "rejected":
            reject.extend(
                item
                for item in str(row.get("reject_reasons") or "pressure_channel_row_rejected").split(";")
                if item
            )
        hold_status = _atmosphere_hold_status(row)
        hold_active = _atmosphere_hold_active(row)
        if config.require_atmosphere_hold_evidence:
            if hold_status != "verified":
                reject.append(f"continuous_atmosphere_hold_not_verified({hold_status or '<missing>'})")
            elif not hold_active:
                reject.append("continuous_atmosphere_hold_not_active")

        analyzer_pressure_keys: List[str] = [
            f"{analyzer_prefix}_pressure_kpa",
            f"{analyzer_prefix}_mode2_pressure_kpa",
            f"{analyzer_prefix}_factory_pressure_kpa",
        ]
        normal_pressure_keys: List[str] = [
            f"{analyzer_prefix}_normal_pressure_kpa",
            f"{analyzer_prefix}_mode1_pressure_kpa",
        ]
        if is_long_row or requested_prefix in {"", "ga01"}:
            analyzer_pressure_keys.extend(
                [
                    "analyzer_pressure_kpa",
                    "pressure_kpa",
                    "mode2_pressure_kpa",
                    "factory_pressure_kpa",
                ]
            )
            normal_pressure_keys.extend(["normal_pressure_kpa", "mode1_pressure_kpa"])

        analyzer_kpa = _safe_float(_first_value(row, analyzer_pressure_keys))
        normal_kpa = _safe_float(
            _first_value(row, normal_pressure_keys)
        )
        com22_hpa = _safe_float(
            _first_value(
                row,
                (
                    "pressure_gauge_hpa",
                    "gauge_pressure",
                    "com22_pressure_hpa",
                    "pressure_reference_hpa",
                    "pressure_meter_hpa",
                ),
            )
        )
        pace_hpa = _safe_float(
            _first_value(
                row,
                (
                    "controller_pressure",
                    "pressure_hpa",
                    "pace_pressure_hpa",
                    "P",
                    "PSample",
                ),
            )
        )

        if analyzer_kpa is None and normal_kpa is None:
            reject.append("missing_analyzer_pressure_kpa")
        if com22_hpa is None:
            reject.append("missing_com22_pressure_hpa")

        analyzer_hpa = analyzer_kpa * 10.0 if analyzer_kpa is not None else None
        normal_hpa = normal_kpa * 10.0 if normal_kpa is not None else None
        analyzer_device_id = (
            str(row.get("analyzer_device_id") or "").strip()
            if is_long_row
            else _analyzer_device_id(row, analyzer_prefix)
        )
        pair = {
            "row_index": index,
            "analyzer_prefix": analyzer_prefix,
            "analyzer_device_id": analyzer_device_id,
            "sample_index": _first_value(row, ("sample_index", "row_index")) or index,
            "sample_ts": _first_value(row, ("sample_ts", "timestamp", "time")) or "",
            "pressure_mode": mode,
            "analyzer_pressure_kpa": analyzer_kpa,
            "analyzer_pressure_hpa": analyzer_hpa,
            "analyzer_normal_pressure_kpa": normal_kpa,
            "analyzer_normal_pressure_hpa": normal_hpa,
            "com22_pressure_hpa": com22_hpa,
            "pace_pressure_hpa": pace_hpa,
            "analyzer_minus_com22_hpa": (
                analyzer_hpa - com22_hpa if analyzer_hpa is not None and com22_hpa is not None else None
            ),
            "normal_minus_com22_hpa": (
                normal_hpa - com22_hpa if normal_hpa is not None and com22_hpa is not None else None
            ),
            "pace_minus_com22_hpa": (
                pace_hpa - com22_hpa if pace_hpa is not None and com22_hpa is not None else None
            ),
            "pressure_atmosphere_hold_status": hold_status,
            "pressure_atmosphere_hold_active": hold_active,
            "pressure_atmosphere_hold_strategy": _first_value(
                row,
                (
                    "pressure_atmosphere_hold_strategy",
                    "atmosphere_hold_strategy",
                    "continuous_atmosphere_hold_strategy",
                ),
            )
            or "",
            "reject_reasons": ";".join(reject),
        }
        if reject:
            rejected.append(pair)
        else:
            paired.append(pair)
    return paired, rejected


def pressure_senco9_fit_pair_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    analyzer_prefix: str = "ga01",
    cfg: Optional[PressureSenco9FitConfig] = None,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Extract analyzer-vs-reference rows for offline SENCO9 model evaluation."""

    config = cfg or PressureSenco9FitConfig()
    paired: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    requested_prefix = str(analyzer_prefix or "").strip().lower()
    group_seen_counts: Dict[str, int] = {}
    if requested_prefix in {"all", "*", "fleet"}:
        raise ValueError("pressure_senco9_fit_pair_rows requires one analyzer prefix")

    for index, source in enumerate(rows, start=1):
        row = dict(source)
        selected_prefix = str(row.get("analyzer_prefix") or "").strip().lower()
        is_long_row = bool(
            re.fullmatch(r"ga\d{2}", selected_prefix)
            and (
                "pressure_channel_row_status" in row
                or "verified_quantity" in row
                or "analyzer_pressure_kpa" in row
                or "analyzer_pressure_hpa" in row
            )
        )
        if is_long_row and selected_prefix != requested_prefix:
            continue

        reject: List[str] = []
        if _frame_unusable(row, analyzer_prefix):
            reject.append("analyzer_frame_unusable")
        if is_long_row and str(row.get("pressure_channel_row_status") or "").strip().lower() == "rejected":
            reject.append("pressure_channel_row_rejected")

        analyzer_kpa = _safe_float(
            _first_value(
                row,
                (
                    f"{analyzer_prefix}_pressure_kpa",
                    f"{analyzer_prefix}_mode2_pressure_kpa",
                    f"{analyzer_prefix}_factory_pressure_kpa",
                    "analyzer_pressure_kpa" if is_long_row or requested_prefix == "ga01" else "",
                    "pressure_kpa" if is_long_row or requested_prefix == "ga01" else "",
                    "mode2_pressure_kpa" if is_long_row or requested_prefix == "ga01" else "",
                    "factory_pressure_kpa" if is_long_row or requested_prefix == "ga01" else "",
                ),
            )
        )
        analyzer_hpa = _safe_float(
            _first_value(
                row,
                (
                    f"{analyzer_prefix}_pressure_hpa",
                    "analyzer_pressure_hpa" if is_long_row or requested_prefix == "ga01" else "",
                ),
            )
        )
        if analyzer_kpa is None and analyzer_hpa is not None:
            analyzer_kpa = analyzer_hpa / 10.0
        if analyzer_hpa is None and analyzer_kpa is not None:
            analyzer_hpa = analyzer_kpa * 10.0

        com22_hpa = _safe_float(
            _first_value(
                row,
                (
                    "pressure_gauge_hpa",
                    "gauge_pressure",
                    "com22_pressure_hpa",
                    "pressure_reference_hpa",
                    "pressure_meter_hpa",
                ),
            )
        )
        pace_hpa = _safe_float(
            _first_value(
                row,
                (
                    "controller_pressure",
                    "pressure_hpa",
                    "pace_pressure_hpa",
                    "P",
                    "PSample",
                ),
            )
        )
        target_hpa = _pressure_target_hpa(row)
        mode = _pressure_mode(row)
        point_group_hpa = target_hpa if target_hpa is not None else com22_hpa
        pressure_point_group = _group_pressure_hpa(
            point_group_hpa,
            config.point_group_resolution_hpa,
        )
        group_key = str(pressure_point_group or "unlabeled")
        group_seen_counts[group_key] = int(group_seen_counts.get(group_key, 0)) + 1
        discard_count = max(0, int(config.discard_initial_samples_per_pressure_point or 0))
        if (
            discard_count > 0
            and mode not in {"ambient_open", "open_flow", ""}
            and group_seen_counts[group_key] <= discard_count
        ):
            reject.append(
                f"pressure_transition_sample_discard<{discard_count + 1}"
            )

        if analyzer_kpa is None:
            reject.append("missing_analyzer_pressure_kpa")
        if com22_hpa is None:
            reject.append("missing_com22_pressure_hpa")

        reference_kpa = com22_hpa / 10.0 if com22_hpa is not None else None
        pair = {
            "row_index": index,
            "sample_index": _first_value(row, ("sample_index", "row_index")) or index,
            "sample_ts": _first_value(row, ("sample_ts", "timestamp", "time")) or "",
            "pressure_mode": mode,
            "pressure_target_hpa": target_hpa,
            "pressure_point_group": pressure_point_group,
            "analyzer_prefix": analyzer_prefix,
            "analyzer_device_id": (
                str(row.get("analyzer_device_id") or "").strip()
                if is_long_row
                else _analyzer_device_id(row, analyzer_prefix)
            ),
            "analyzer_pressure_kpa": analyzer_kpa,
            "analyzer_pressure_hpa": analyzer_hpa,
            "com22_pressure_hpa": com22_hpa,
            "com22_pressure_kpa": reference_kpa,
            "pace_pressure_hpa": pace_hpa,
            "analyzer_minus_com22_hpa": (
                analyzer_hpa - com22_hpa if analyzer_hpa is not None and com22_hpa is not None else None
            ),
            "reject_reasons": ";".join(reject),
        }
        if reject:
            rejected.append(pair)
        else:
            paired.append(pair)
    return paired, rejected


def _mean_abs(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    return float(mean(abs(item) for item in values))


def _max_abs(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    return float(max(abs(item) for item in values))


def _linear_fit(x_values: Sequence[float], y_values: Sequence[float]) -> tuple[Optional[float], Optional[float]]:
    if len(x_values) != len(y_values) or len(x_values) < 2:
        return None, None
    x_mean = float(mean(x_values))
    y_mean = float(mean(y_values))
    denominator = sum((x - x_mean) ** 2 for x in x_values)
    if denominator <= 1e-12:
        return None, None
    slope = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, y_values)) / denominator
    intercept = y_mean - slope * x_mean
    return float(intercept), float(slope)


def _pressure_fit_point_means(
    paired: Sequence[Mapping[str, Any]],
    *,
    cfg: PressureSenco9FitConfig,
) -> List[Dict[str, Any]]:
    by_point: Dict[str, List[Mapping[str, Any]]] = {}
    for row in paired:
        key = str(row.get("pressure_point_group") or "unlabeled")
        by_point.setdefault(key, []).append(row)

    out: List[Dict[str, Any]] = []
    for key, values in sorted(by_point.items(), key=lambda item: _safe_float(item[0]) or -1e12):
        ref_hpa = [
            float(value["com22_pressure_hpa"])
            for value in values
            if value.get("com22_pressure_hpa") not in (None, "")
        ]
        analyzer_hpa = [
            float(value["analyzer_pressure_hpa"])
            for value in values
            if value.get("analyzer_pressure_hpa") not in (None, "")
        ]
        delta_hpa = [
            float(value["analyzer_minus_com22_hpa"])
            for value in values
            if value.get("analyzer_minus_com22_hpa") not in (None, "")
        ]
        target_values = [
            float(value["pressure_target_hpa"])
            for value in values
            if value.get("pressure_target_hpa") not in (None, "")
        ]
        point_status = "pass"
        reasons: List[str] = []
        ref_span = _span(ref_hpa)
        if ref_span is not None and ref_span > float(cfg.max_point_reference_span_hpa):
            point_status = "warn"
            reasons.append(
                f"reference_span_hpa={ref_span:.3f}>limit={float(cfg.max_point_reference_span_hpa):.3f}"
            )
        out.append(
            {
                "pressure_point_group": key,
                "pressure_target_hpa": float(mean(target_values)) if target_values else "",
                "sample_count": len(values),
                "com22_pressure_mean_hpa": float(mean(ref_hpa)) if ref_hpa else "",
                "com22_pressure_span_hpa": ref_span if ref_span is not None else "",
                "analyzer_pressure_mean_hpa": float(mean(analyzer_hpa)) if analyzer_hpa else "",
                "analyzer_pressure_span_hpa": _span(analyzer_hpa) or "",
                "analyzer_minus_com22_mean_hpa": float(mean(delta_hpa)) if delta_hpa else "",
                "analyzer_minus_com22_max_abs_hpa": _max_abs(delta_hpa) or "",
                "point_status": point_status,
                "point_reason": ";".join(reasons) if reasons else "ok",
            }
        )
    return out


def evaluate_pressure_senco9_fit(
    rows: Sequence[Mapping[str, Any]],
    *,
    pressure_reference: Optional[Mapping[str, Any]] = None,
    analyzer_prefix: str = "ga01",
    cfg: Optional[PressureSenco9FitConfig] = None,
    today: Optional[date | str] = None,
) -> PressureSenco9FitResult:
    """Evaluate whether pressure data support a no-write SENCO9 offset candidate."""

    config = cfg or PressureSenco9FitConfig()
    traceability = validate_pressure_reference_traceability(pressure_reference or {}, today=today)
    paired, rejected = pressure_senco9_fit_pair_rows(rows, analyzer_prefix=analyzer_prefix, cfg=config)
    analyzer_device_id = _dominant_analyzer_device_id(
        [*paired, *rejected],
        analyzer_prefix,
    )

    x_analyzer_kpa = [
        float(row["analyzer_pressure_kpa"]) for row in paired if row.get("analyzer_pressure_kpa") not in (None, "")
    ]
    y_reference_kpa = [
        float(row["com22_pressure_kpa"]) for row in paired if row.get("com22_pressure_kpa") not in (None, "")
    ]
    reference_hpa = [
        float(row["com22_pressure_hpa"]) for row in paired if row.get("com22_pressure_hpa") not in (None, "")
    ]
    analyzer_hpa = [
        float(row["analyzer_pressure_hpa"]) for row in paired if row.get("analyzer_pressure_hpa") not in (None, "")
    ]
    point_means = _pressure_fit_point_means(paired, cfg=config)
    distinct_points = len(point_means)
    reference_span = _span(reference_hpa)
    analyzer_span = _span(analyzer_hpa)

    offset_kpa: Optional[float] = None
    offset_residual_mean_abs_hpa: Optional[float] = None
    offset_residual_max_abs_hpa: Optional[float] = None
    linear_intercept: Optional[float] = None
    linear_slope: Optional[float] = None
    linear_slope_bias: Optional[float] = None
    linear_residual_mean_abs_hpa: Optional[float] = None
    linear_residual_max_abs_hpa: Optional[float] = None

    if x_analyzer_kpa and len(x_analyzer_kpa) == len(y_reference_kpa):
        offsets = [y - x for x, y in zip(x_analyzer_kpa, y_reference_kpa)]
        offset_kpa = float(mean(offsets))
        offset_residuals_hpa = [
            (y - (x + offset_kpa)) * 10.0 for x, y in zip(x_analyzer_kpa, y_reference_kpa)
        ]
        offset_residual_mean_abs_hpa = _mean_abs(offset_residuals_hpa)
        offset_residual_max_abs_hpa = _max_abs(offset_residuals_hpa)
        linear_intercept, linear_slope = _linear_fit(x_analyzer_kpa, y_reference_kpa)
        if linear_intercept is not None and linear_slope is not None:
            linear_slope_bias = linear_slope - 1.0
            linear_residuals_hpa = [
                (y - (linear_intercept + linear_slope * x)) * 10.0
                for x, y in zip(x_analyzer_kpa, y_reference_kpa)
            ]
            linear_residual_mean_abs_hpa = _mean_abs(linear_residuals_hpa)
            linear_residual_max_abs_hpa = _max_abs(linear_residuals_hpa)

    issues: List[str] = []
    warnings: List[str] = []
    if config.require_traceable_reference and traceability.status != "pass":
        issues.append("pressure_reference_traceability_failed")
    if len(paired) < int(config.min_pairs):
        issues.append(f"valid_pair_count<{int(config.min_pairs)}")
    if distinct_points < int(config.min_distinct_pressure_points):
        issues.append(f"distinct_pressure_points<{int(config.min_distinct_pressure_points)}")
    if reference_span is None or reference_span < float(config.min_pressure_span_hpa):
        observed = 0.0 if reference_span is None else reference_span
        issues.append(
            f"reference_pressure_span_hpa={observed:.3f}<required={float(config.min_pressure_span_hpa):.3f}"
        )
    unstable_points = [
        row for row in point_means if str(row.get("point_status") or "").strip().lower() != "pass"
    ]
    if unstable_points:
        warnings.append(f"unstable_pressure_points={len(unstable_points)}")

    command = ""
    if offset_kpa is not None:
        command = "SENCO9,YGAS,FFF," + ",".join(format_senco_values((offset_kpa, 1.0, 0.0, 0.0)))

    status = "insufficient_evidence" if issues else "pass"
    recommendation = "collect_no_write_multi_point_pressure_data"
    if issues:
        reason = ";".join([*issues, *warnings])
    else:
        slope_bias_abs = abs(float(linear_slope_bias or 0.0)) if linear_slope_bias is not None else math.inf
        offset_mean_abs = float(offset_residual_mean_abs_hpa or math.inf)
        offset_max_abs = float(offset_residual_max_abs_hpa or math.inf)
        linear_mean_abs = float(linear_residual_mean_abs_hpa or math.inf)
        linear_max_abs = float(linear_residual_max_abs_hpa or math.inf)
        offset_model_ok = (
            slope_bias_abs <= float(config.max_slope_bias_for_offset_only)
            and offset_mean_abs <= float(config.max_offset_residual_mean_abs_hpa)
            and offset_max_abs <= float(config.max_offset_residual_max_abs_hpa)
        )
        linear_model_ok = (
            linear_mean_abs <= float(config.max_linear_residual_mean_abs_hpa)
            and linear_max_abs <= float(config.max_linear_residual_max_abs_hpa)
        )
        if offset_model_ok:
            recommendation = "review_senco9_offset_candidate_no_write"
            reason = "offset_only_model_supported_no_write_review_required"
        elif linear_model_ok and slope_bias_abs > float(config.max_slope_bias_for_offset_only):
            status = "fail"
            recommendation = "do_not_write_offset_only_senco9_investigate_scale_or_model"
            reason = "linear_model_fits_but_slope_bias_exceeds_offset_only_limit"
        else:
            status = "fail"
            recommendation = "do_not_write_senco9_investigate_pressure_channel"
            reason = "pressure_fit_residuals_exceed_limits"
        if warnings:
            reason = f"{reason};" + ";".join(warnings)

    return PressureSenco9FitResult(
        analyzer_prefix=analyzer_prefix,
        analyzer_device_id=analyzer_device_id,
        status=status,
        recommendation=recommendation,
        reason=reason,
        sample_count=len(rows),
        valid_pair_count=len(paired),
        distinct_pressure_points=distinct_points,
        reference_span_hpa=reference_span,
        analyzer_span_hpa=analyzer_span,
        offset_only_offset_kpa=offset_kpa,
        offset_only_residual_mean_abs_hpa=offset_residual_mean_abs_hpa,
        offset_only_residual_max_abs_hpa=offset_residual_max_abs_hpa,
        linear_intercept_kpa=linear_intercept,
        linear_slope=linear_slope,
        linear_slope_bias=linear_slope_bias,
        linear_residual_mean_abs_hpa=linear_residual_mean_abs_hpa,
        linear_residual_max_abs_hpa=linear_residual_max_abs_hpa,
        senco9_candidate_command=command,
        write_allowed=False,
        traceability=asdict(traceability),
    )


def build_pressure_senco9_fit_tables(
    rows: Sequence[Mapping[str, Any]],
    *,
    pressure_reference: Optional[Mapping[str, Any]] = None,
    analyzer_prefix: str = "all",
    cfg: Optional[PressureSenco9FitConfig] = None,
    today: Optional[date | str] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Build no-write pressure/SENCO9 fit tables for one analyzer or the fleet."""

    prefixes = (
        detect_pressure_analyzer_prefixes(rows)
        if str(analyzer_prefix or "").strip().lower() in {"all", "*", "fleet"}
        else [str(analyzer_prefix or "ga01").strip().lower()]
    )
    config = cfg or PressureSenco9FitConfig()
    summary_rows: List[Dict[str, Any]] = []
    point_rows: List[Dict[str, Any]] = []
    residual_rows: List[Dict[str, Any]] = []
    rejected_rows: List[Dict[str, Any]] = []
    trace_rows: List[Dict[str, Any]] = []

    for prefix in prefixes:
        result = evaluate_pressure_senco9_fit(
            rows,
            pressure_reference=pressure_reference,
            analyzer_prefix=prefix,
            cfg=config,
            today=today,
        )
        result_dict = asdict(result)
        trace_rows.append(
            {
                "analyzer_prefix": prefix,
                **{
                    key: _table_value(value)
                    for key, value in result_dict["traceability"].items()
                },
            }
        )
        summary_rows.append(
            {
                key: _table_value(value)
                for key, value in result_dict.items()
                if key != "traceability"
            }
        )
        paired, rejected = pressure_senco9_fit_pair_rows(rows, analyzer_prefix=prefix, cfg=config)
        offset = result.offset_only_offset_kpa
        linear_intercept = result.linear_intercept_kpa
        linear_slope = result.linear_slope
        for row in _pressure_fit_point_means(paired, cfg=config):
            point_rows.append(
                {
                    "analyzer_prefix": prefix,
                    "analyzer_device_id": result.analyzer_device_id,
                    **row,
                }
            )
        for row in paired:
            analyzer_kpa = _safe_float(row.get("analyzer_pressure_kpa"))
            ref_kpa = _safe_float(row.get("com22_pressure_kpa"))
            offset_resid_hpa = (
                (ref_kpa - (analyzer_kpa + offset)) * 10.0
                if analyzer_kpa is not None and ref_kpa is not None and offset is not None
                else None
            )
            linear_resid_hpa = (
                (ref_kpa - (linear_intercept + linear_slope * analyzer_kpa)) * 10.0
                if (
                    analyzer_kpa is not None
                    and ref_kpa is not None
                    and linear_intercept is not None
                    and linear_slope is not None
                )
                else None
            )
            residual_rows.append(
                {
                    **{str(key): _table_value(value) for key, value in row.items()},
                    "offset_model_residual_hpa": offset_resid_hpa if offset_resid_hpa is not None else "",
                    "linear_model_residual_hpa": linear_resid_hpa if linear_resid_hpa is not None else "",
                }
            )
        for row in rejected:
            rejected_rows.append({str(key): _table_value(value) for key, value in row.items()})

    return {
        "pressure_fit_summary": summary_rows,
        "pressure_fit_point_means": point_rows,
        "pressure_fit_residuals": residual_rows,
        "pressure_fit_rejected_rows": rejected_rows,
        "pressure_reference_traceability": trace_rows,
    }


def write_pressure_senco9_fit_report(
    *,
    run_dir: str | Path,
    output_dir: str | Path | None = None,
    pressure_reference: Optional[Mapping[str, Any]] = None,
    pressure_reference_path: str | Path | None = None,
    samples_csv: str | Path | None = None,
    analyzer_prefix: str = "all",
    cfg: Optional[PressureSenco9FitConfig] = None,
    today: Optional[date | str] = None,
) -> Dict[str, Path]:
    """Write a no-write pressure fit report for deciding whether SENCO9 is needed."""

    root = Path(run_dir).resolve()
    if samples_csv is None:
        # SENCO9 multi-point fitting needs the full sampled plateau rows when
        # they are present. Quick-check sidecars may coexist in the same run
        # folder and are intentionally narrower pressure-channel evidence.
        preferred_samples = latest_artifact(root, "samples_*.csv")
        rows_path, rows = load_pressure_validation_rows(root, samples_csv=preferred_samples)
    else:
        rows_path, rows = load_pressure_validation_rows(root, samples_csv=samples_csv)
    reference = dict(pressure_reference) if pressure_reference is not None else _load_pressure_reference(pressure_reference_path)
    config = cfg or PressureSenco9FitConfig()
    tables = build_pressure_senco9_fit_tables(
        rows,
        pressure_reference=reference,
        analyzer_prefix=analyzer_prefix,
        cfg=config,
        today=today,
    )
    destination = Path(output_dir).resolve() if output_dir else root / "pressure_senco9_fit_evaluation"
    summary = tables.get("pressure_fit_summary") or []
    analyzers = [
        f"{row.get('analyzer_prefix')}:{row.get('analyzer_device_id')}"
        for row in summary
        if row.get("analyzer_prefix")
    ]
    metadata = ValidationMetadata(
        tool_name="export_v1_5_pressure_senco9_evaluation",
        analyzers=analyzers,
        input_paths=[
            str(rows_path),
            str(Path(pressure_reference_path).resolve()) if pressure_reference_path else "",
        ],
        output_dir=str(destination),
        config_summary={
            "evaluation_mode": "no_write_pressure_senco9_fit",
            "analyzer_prefix": analyzer_prefix,
            "min_distinct_pressure_points": config.min_distinct_pressure_points,
            "min_pressure_span_hpa": config.min_pressure_span_hpa,
            "discard_initial_samples_per_pressure_point": config.discard_initial_samples_per_pressure_point,
            "write_allowed": False,
        },
        notes=[
            "Sidecar-only SENCO9 evaluation.",
            "This fits analyzer internal pressure P against COM22 reference pressure.",
            "It does not fit CO2/H2O and does not use sealed CO2/H2O samples as formal acceptance.",
            "No COM ports are opened and no PACE, valve, route, SENCO9, or coefficient writes are performed.",
            "Any candidate command is a no-write review artifact only.",
        ],
    )
    outputs = write_validation_report(
        destination,
        prefix="pressure_senco9_fit",
        metadata=metadata,
        tables=tables,
    )
    review_path = _write_pressure_senco9_review_report(
        destination=destination,
        metadata=metadata,
        summary_rows=summary,
        point_rows=tables.get("pressure_fit_point_means") or [],
        traceability_rows=tables.get("pressure_reference_traceability") or [],
    )
    outputs["review_report"] = review_path
    return outputs


def _write_pressure_senco9_review_report(
    *,
    destination: Path,
    metadata: ValidationMetadata,
    summary_rows: Sequence[Mapping[str, Any]],
    point_rows: Sequence[Mapping[str, Any]],
    traceability_rows: Sequence[Mapping[str, Any]],
) -> Path:
    """Write a human-readable no-write SENCO9 review artifact."""

    report_path = destination / "pressure_senco9_no_write_review.md"
    lines: List[str] = [
        "# V1.5 Pressure/SENCO9 No-Write Review",
        "",
        f"- Tool: {metadata.tool_name}",
        f"- Created at: {metadata.created_at}",
        "- Scope: analyzer internal pressure P vs COM22 reference pressure.",
        "- Boundary: no COM open, no PACE control, no route/valve control, no SENCO9 write.",
        "- CO2/H2O note: this is not component calibration evidence and is not real acceptance.",
        "",
        "## Decision Summary",
        "",
        "| Analyzer | Device ID | Status | Recommendation | Offset kPa | Max residual hPa | Slope bias | Write allowed |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for row in summary_rows:
        lines.append(
            "| {prefix} | {device_id} | {status} | {recommendation} | {offset} | {max_residual} | {slope_bias} | {write_allowed} |".format(
                prefix=str(row.get("analyzer_prefix") or ""),
                device_id=str(row.get("analyzer_device_id") or ""),
                status=str(row.get("status") or ""),
                recommendation=str(row.get("recommendation") or ""),
                offset=_format_report_number(row.get("offset_only_offset_kpa"), digits=6),
                max_residual=_format_report_number(row.get("offset_only_residual_max_abs_hpa"), digits=3),
                slope_bias=_format_report_number(row.get("linear_slope_bias"), digits=6),
                write_allowed=str(row.get("write_allowed")),
            )
        )

    lines.extend(
        [
            "",
            "## Point Means",
            "",
            "| Analyzer | Device ID | Pressure point hPa | Samples | COM22 mean hPa | Analyzer mean hPa | Analyzer-COM22 hPa | Point status |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in point_rows:
        lines.append(
            "| {prefix} | {device_id} | {point} | {samples} | {ref_mean} | {analyzer_mean} | {delta} | {status} |".format(
                prefix=str(row.get("analyzer_prefix") or ""),
                device_id=str(row.get("analyzer_device_id") or ""),
                point=_format_report_number(row.get("pressure_point_group"), digits=0),
                samples=str(row.get("sample_count") or ""),
                ref_mean=_format_report_number(row.get("com22_pressure_mean_hpa"), digits=3),
                analyzer_mean=_format_report_number(row.get("analyzer_pressure_mean_hpa"), digits=3),
                delta=_format_report_number(row.get("analyzer_minus_com22_mean_hpa"), digits=3),
                status=str(row.get("point_status") or ""),
            )
        )

    lines.extend(
        [
            "",
            "## Traceability",
            "",
            "| Analyzer | Reference status | Validation level | Device ID | Certificate ID | Valid until | Certificate hash |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in traceability_rows:
        lines.append(
            "| {prefix} | {status} | {level} | {device_id} | {cert_id} | {valid_until} | {cert_hash} |".format(
                prefix=str(row.get("analyzer_prefix") or ""),
                status=str(row.get("status") or ""),
                level=str(row.get("validation_level") or ""),
                device_id=str(row.get("device_id") or ""),
                cert_id=str(row.get("certificate_id") or ""),
                valid_until=str(row.get("valid_until") or ""),
                cert_hash=str(row.get("certificate_hash") or ""),
            )
        )

    lines.extend(
        [
            "",
            "## Physical Meaning",
            "",
            "- The offset-only fit checks whether analyzer pressure P tracks COM22 with a mostly constant bias.",
            "- A supported offset candidate means SENCO9 review may be justified, but this artifact still forbids writes.",
            "- CO2/H2O samples must remain open-flow and component-stable before any component calibration decision.",
            "",
        ]
    )
    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path


def _format_report_number(value: Any, *, digits: int) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return ""
    return f"{numeric:.{digits}f}"


def pressure_quick_check_artifact_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    analyzer_prefix: str = "ga01",
    cfg: Optional[PressureChannelConfig] = None,
) -> List[Dict[str, Any]]:
    """Build the raw quick-check rows expected by formal V1.5 evidence tools."""

    if str(analyzer_prefix or "").strip().lower() in {"all", "*", "fleet"}:
        out: List[Dict[str, Any]] = []
        for prefix in detect_pressure_analyzer_prefixes(rows):
            out.extend(pressure_quick_check_artifact_rows(rows, analyzer_prefix=prefix, cfg=cfg))
        return out

    paired, rejected = pressure_pair_rows(rows, analyzer_prefix=analyzer_prefix, cfg=cfg)
    out: List[Dict[str, Any]] = []
    for status, source_rows in (("paired", paired), ("rejected", rejected)):
        for row in source_rows:
            item = {str(key): _table_value(value) for key, value in row.items()}
            item.update(
                {
                "pressure_channel_row_status": status,
                "verified_quantity": "analyzer_internal_pressure_P",
                "analyzer_prefix": analyzer_prefix,
                "primary_reference": "COM22",
                "auxiliary_reference": "PACE",
                }
            )
            out.append(item)
    return out


def write_pressure_quick_check_csv(
    output_dir: str | Path,
    rows: Sequence[Mapping[str, Any]],
    *,
    analyzer_prefix: str = "ga01",
    run_id: str = "",
    cfg: Optional[PressureChannelConfig] = None,
) -> Path:
    """Write a dedicated pressure quick-check CSV without controlling hardware."""

    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    suffix = str(run_id or "").strip() or datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in suffix)
    static_len = len(str(root)) + len("\\pressure_channel_quick_check_") + len(".csv")
    max_suffix_len = max(16, 240 - static_len)
    if len(suffix) > max_suffix_len:
        digest = hashlib.sha1(suffix.encode("utf-8")).hexdigest()[:8]
        keep = max(1, max_suffix_len - len(digest) - 1)
        suffix = f"{suffix[:keep]}_{digest}"
    path = root / f"pressure_channel_quick_check_{suffix}.csv"
    artifact_rows = pressure_quick_check_artifact_rows(
        rows,
        analyzer_prefix=analyzer_prefix,
        cfg=cfg,
    )
    header = _table_header(artifact_rows)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(artifact_rows)
    return path


def evaluate_pressure_channel_ambient(
    rows: Sequence[Mapping[str, Any]],
    *,
    pressure_reference: Optional[Mapping[str, Any]] = None,
    analyzer_prefix: str = "ga01",
    cfg: Optional[PressureChannelConfig] = None,
    today: Optional[date | str] = None,
) -> PressureChannelValidationResult:
    """Evaluate mode A: current-atmosphere pressure-channel quick validation."""

    config = cfg or PressureChannelConfig()
    traceability = validate_pressure_reference_traceability(pressure_reference or {}, today=today)
    paired, rejected = pressure_pair_rows(rows, analyzer_prefix=analyzer_prefix, cfg=config)
    analyzer_device_id = _dominant_analyzer_device_id(
        [*paired, *rejected],
        analyzer_prefix,
    )

    analyzer_values = [
        float(row["analyzer_pressure_hpa"]) for row in paired if row.get("analyzer_pressure_hpa") not in (None, "")
    ]
    com22_values = [
        float(row["com22_pressure_hpa"]) for row in paired if row.get("com22_pressure_hpa") not in (None, "")
    ]
    pace_values = [
        float(row["pace_pressure_hpa"]) for row in paired if row.get("pace_pressure_hpa") not in (None, "")
    ]
    analyzer_deltas = [
        float(row["analyzer_minus_com22_hpa"])
        for row in paired
        if row.get("analyzer_minus_com22_hpa") not in (None, "")
    ]
    pace_deltas = [
        float(row["pace_minus_com22_hpa"])
        for row in paired
        if row.get("pace_minus_com22_hpa") not in (None, "")
    ]

    issues: List[str] = []
    warnings: List[str] = []
    if len(analyzer_deltas) < int(config.min_pairs):
        status = "insufficient_evidence"
        issues.append(f"pressure_pair_count<{int(config.min_pairs)}")
    else:
        status = "pass"
        mean_delta = abs(float(mean(analyzer_deltas)))
        max_abs_delta = float(max(abs(item) for item in analyzer_deltas))
        if mean_delta > float(config.mean_abs_delta_hpa):
            issues.append(f"mean_abs_delta_hpa={mean_delta:.3f}>limit={float(config.mean_abs_delta_hpa):.3f}")
        if max_abs_delta > float(config.max_abs_delta_hpa):
            issues.append(
                f"max_abs_delta_hpa={max_abs_delta:.3f}>limit={float(config.max_abs_delta_hpa):.3f}"
            )
        analyzer_span = _span(analyzer_values)
        if (
            config.analyzer_pressure_span_hpa_max is not None
            and analyzer_span is not None
            and analyzer_span > float(config.analyzer_pressure_span_hpa_max)
        ):
            issues.append(
                "analyzer_pressure_span_hpa="
                f"{analyzer_span:.3f}>limit={float(config.analyzer_pressure_span_hpa_max):.3f}"
            )
        com22_span = _span(com22_values)
        if (
            config.com22_pressure_span_hpa_max is not None
            and com22_span is not None
            and com22_span > float(config.com22_pressure_span_hpa_max)
        ):
            issues.append(
                f"com22_pressure_span_hpa={com22_span:.3f}>limit={float(config.com22_pressure_span_hpa_max):.3f}"
            )
        if issues:
            status = "fail"

    if pace_deltas and config.pace_com22_mean_abs_delta_hpa_warn is not None:
        pace_mean_abs = abs(float(mean(pace_deltas)))
        if pace_mean_abs > float(config.pace_com22_mean_abs_delta_hpa_warn):
            warnings.append(
                "pace_minus_com22_mean_abs_hpa="
                f"{pace_mean_abs:.3f}>warn={float(config.pace_com22_mean_abs_delta_hpa_warn):.3f}"
            )

    formal_ok = status == "pass" and traceability.status == "pass"
    level = "formal_pressure_validation" if formal_ok else "engineering_diagnostic"
    all_reasons = list(issues)
    if status == "pass":
        all_reasons.append("numeric_pressure_check_pass")
    if traceability.status != "pass":
        all_reasons.append("pressure_reference_traceability_failed")
    all_reasons.extend(warnings)

    return PressureChannelValidationResult(
        validation_mode="ambient_quick_check",
        status=status,
        validation_level=level,
        reason=";".join(all_reasons) if all_reasons else "ok",
        sample_count=len(rows),
        valid_pair_count=len(paired),
        rejected_pair_count=len(rejected),
        analyzer_pressure_mean_hpa=float(mean(analyzer_values)) if analyzer_values else None,
        com22_pressure_mean_hpa=float(mean(com22_values)) if com22_values else None,
        pace_pressure_mean_hpa=float(mean(pace_values)) if pace_values else None,
        analyzer_minus_com22_mean_hpa=float(mean(analyzer_deltas)) if analyzer_deltas else None,
        analyzer_minus_com22_max_abs_hpa=(
            float(max(abs(item) for item in analyzer_deltas)) if analyzer_deltas else None
        ),
        pace_minus_com22_mean_hpa=float(mean(pace_deltas)) if pace_deltas else None,
        allowed_for_co2_h2o_formal_work=formal_ok,
        traceability=asdict(traceability),
        measurement_model={
            "verified_quantity": "analyzer_internal_pressure_P",
            "analyzer_prefix": analyzer_prefix,
            "analyzer_device_id": analyzer_device_id,
            "analyzer_identity_source": "MODE2/device_id",
            "analyzer_field": "pressure_kpa",
            "analyzer_unit": "kPa",
            "normalization": "analyzer_pressure_hpa = pressure_kpa * 10",
            "primary_reference": "COM22 digital pressure gauge",
            "auxiliary_reference": "PACE pressure controller",
            "fit_scope": "not_co2_h2o_fit_input",
            "senco9_scope": "pressure_channel_only",
        },
        analyzer_prefix=analyzer_prefix,
        analyzer_device_id=analyzer_device_id,
    )


def evaluate_pressure_channel_fleet(
    rows: Sequence[Mapping[str, Any]],
    *,
    pressure_reference: Optional[Mapping[str, Any]] = None,
    analyzer_prefixes: Optional[Sequence[str]] = None,
    cfg: Optional[PressureChannelConfig] = None,
    today: Optional[date | str] = None,
) -> List[PressureChannelValidationResult]:
    """Evaluate current-atmosphere pressure validation for every analyzer prefix."""

    prefixes = list(analyzer_prefixes or detect_pressure_analyzer_prefixes(rows))
    return [
        evaluate_pressure_channel_ambient(
            rows,
            pressure_reference=pressure_reference,
            analyzer_prefix=prefix,
            cfg=cfg,
            today=today,
        )
        for prefix in prefixes
    ]


def _load_pressure_reference(path: str | Path | None) -> Dict[str, Any]:
    if path is None:
        return {}
    ref_path = Path(path)
    if not ref_path.exists():
        raise FileNotFoundError(f"Pressure reference JSON not found: {ref_path}")
    return json.loads(ref_path.read_text(encoding="utf-8"))


def load_pressure_validation_rows(run_dir: str | Path, samples_csv: str | Path | None = None) -> tuple[Path, List[Dict[str, Any]]]:
    if samples_csv:
        path = Path(samples_csv)
    else:
        root = Path(run_dir)
        path = latest_artifact(root, "pressure_channel_quick_check*.csv") or latest_artifact(root, "samples_*.csv")
        if path is None:
            raise FileNotFoundError(f"No pressure_channel_quick_check*.csv or samples_*.csv found under {root}")
    return path, [normalize_sample_row(row) for row in load_csv_rows(path)]


def build_pressure_channel_tables(
    rows: Sequence[Mapping[str, Any]],
    *,
    pressure_reference: Optional[Mapping[str, Any]] = None,
    analyzer_prefix: str = "ga01",
    cfg: Optional[PressureChannelConfig] = None,
    today: Optional[date | str] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    if str(analyzer_prefix or "").strip().lower() in {"all", "*", "fleet"}:
        out: Dict[str, List[Dict[str, Any]]] = {
            "pressure_validation_summary": [],
            "pressure_reference_traceability": [],
            "measurement_model": [],
            "paired_samples": [],
            "rejected_samples": [],
        }
        for prefix in detect_pressure_analyzer_prefixes(rows):
            tables = build_pressure_channel_tables(
                rows,
                pressure_reference=pressure_reference,
                analyzer_prefix=prefix,
                cfg=cfg,
                today=today,
            )
            for key, values in tables.items():
                out.setdefault(key, []).extend(values)
        return out

    result = evaluate_pressure_channel_ambient(
        rows,
        pressure_reference=pressure_reference,
        analyzer_prefix=analyzer_prefix,
        cfg=cfg,
        today=today,
    )
    paired, rejected = pressure_pair_rows(rows, analyzer_prefix=analyzer_prefix, cfg=cfg)
    result_dict = asdict(result)
    summary = {
        key: _table_value(value)
        for key, value in result_dict.items()
        if key not in {"traceability", "measurement_model"}
    }
    traceability = {
        key: _table_value(value)
        for key, value in result_dict["traceability"].items()
    }
    measurement_model = {
        key: _table_value(value)
        for key, value in result_dict["measurement_model"].items()
    }
    return {
        "pressure_validation_summary": [summary],
        "pressure_reference_traceability": [traceability],
        "measurement_model": [measurement_model],
        "paired_samples": [
            {str(key): _table_value(value) for key, value in row.items()} for row in paired
        ],
        "rejected_samples": [
            {str(key): _table_value(value) for key, value in row.items()} for row in rejected
        ],
    }


def write_pressure_channel_report(
    *,
    run_dir: str | Path,
    output_dir: str | Path | None = None,
    pressure_reference: Optional[Mapping[str, Any]] = None,
    pressure_reference_path: str | Path | None = None,
    samples_csv: str | Path | None = None,
    analyzer_prefix: str = "ga01",
    cfg: Optional[PressureChannelConfig] = None,
    today: Optional[date | str] = None,
) -> Dict[str, Path]:
    root = Path(run_dir).resolve()
    rows_path, rows = load_pressure_validation_rows(root, samples_csv=samples_csv)
    reference = dict(pressure_reference) if pressure_reference is not None else _load_pressure_reference(pressure_reference_path)
    tables = build_pressure_channel_tables(
        rows,
        pressure_reference=reference,
        analyzer_prefix=analyzer_prefix,
        cfg=cfg,
        today=today,
    )
    summary = tables.get("pressure_validation_summary") or [{}]
    analyzer_device_id = str(summary[0].get("analyzer_device_id") or "") if len(summary) == 1 else ""
    analyzers = []
    for row in summary:
        prefix = str(row.get("analyzer_prefix") or analyzer_prefix or "").strip()
        device_id = str(row.get("analyzer_device_id") or "").strip()
        if prefix and device_id:
            analyzers.append(f"{prefix}:{device_id}")
        elif prefix:
            analyzers.append(prefix)
    destination = Path(output_dir).resolve() if output_dir else root / "pressure_channel_validation"
    metadata = ValidationMetadata(
        tool_name="export_v1_5_pressure_channel_validation",
        analyzers=analyzers or [analyzer_prefix],
        input_paths=[
            str(rows_path),
            str(Path(pressure_reference_path).resolve()) if pressure_reference_path else "",
        ],
        output_dir=str(destination),
        config_summary={
            "validation_mode": "ambient_quick_check",
            "analyzer_prefix": analyzer_prefix,
            "analyzer_device_id": analyzer_device_id,
            "analyzer_identity_note": "analyzer_prefix is the acquisition channel; analyzer_device_id is the analyzer identity",
            "primary_reference": "COM22 digital pressure gauge",
            "auxiliary_reference": "PACE pressure controller",
        },
        notes=[
            "Sidecar-only pressure-channel validation.",
            "This verifies analyzer internal pressure P and does not fit CO2/H2O.",
            "No COM ports are opened and no PACE, valve, route, SENCO9, or coefficient writes are performed.",
            "If COM22 traceability is missing or expired, the result is engineering_diagnostic only.",
        ],
    )
    return write_validation_report(
        destination,
        prefix="pressure_channel_validation",
        metadata=metadata,
        tables=tables,
    )
