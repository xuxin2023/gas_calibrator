"""V1.5 open-flow candidate coefficient policy and no-write export.

The helpers in this module are offline-only. They consume already-recorded
formal open-flow evidence and produce review artifacts. They do not open COM
ports, switch water/gas routes, control PACE/valves, or write SENCO values.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np

from ..coefficients.model_metrics import compute_metrics
from .co2_firmware_contract import (
    co2_firmware_final_to_raw_ppm,
    co2_raw_to_firmware_final_ppm,
    co2_senco3_temperature_compensation_ppm,
)
from .dewpoint_flush_gate import dewpoint_to_h2o_mmol_per_mol
from .formal_calibration_package import build_formal_calibration_package_tables
from .formal_open_flow_artifacts import load_plan_snapshot, load_pressure_reference_snapshot
from .reporting import ValidationMetadata, write_validation_report


PRESSURE_TERMS = ("P", "RP", "RTP")
TEMPERATURE_TERMS = ("T", "T2", "RT")
CENTERABLE_TERMS = ("intercept", "R", "R2", "R3", "T", "T2", "RT")


@dataclass(frozen=True)
class CandidateCoefficientPolicyConfig:
    """Identifiability and verification limits for V1.5 candidate fitting."""

    min_fit_samples: int = 10
    min_distinct_targets: int = 2
    min_verification_samples: int = 1
    pressure_span_min_hpa_for_terms: float = 50.0
    temperature_span_min_c_for_terms: float = 5.0
    allow_pressure_terms: bool = False
    allow_temperature_terms: bool = False
    max_condition_number: float = 1.0e8
    verification_max_abs_error: Mapping[str, float] = field(
        default_factory=lambda: {"co2": 2.0, "h2o": 0.05}
    )
    verification_use_certificate_uncertainty: bool = True
    fit_all_eligible_samples: bool = False
    allow_uncertified_zero_co2_anchor: bool = False
    hard_bad_temperature_values_c: Tuple[float, ...] = (-40.0, 60.0)
    hard_bad_temperature_tolerance_c: float = 0.05
    exclude_device_ids: Tuple[str, ...] = ()
    enable_common_mode_fit_target_outlier_rejection: bool = True
    common_mode_outlier_abs_fraction: Mapping[str, float] = field(
        default_factory=lambda: {"co2": 0.01, "h2o": 0.02}
    )
    common_mode_outlier_min_devices: int = 3
    common_mode_outlier_min_remaining_targets: int = 4
    preserved_secondary_coefficients: Mapping[str, Any] = field(default_factory=dict)
    preserved_secondary_coefficients_source: str = ""
    co2_dry_correction_h2o_source: str = "reference_first"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "null", "None"):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _first_value(row: Mapping[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _prefixed_value(row: Mapping[str, Any], prefix: str, key: str) -> Any:
    prefixed = f"{prefix}_{key}" if prefix else key
    value = row.get(prefixed)
    if value in (None, ""):
        value = row.get(key)
    return value


def _normalized_device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _snapshot_numeric_values(value: Any) -> List[float]:
    if value in (None, ""):
        return []
    candidate = value
    if isinstance(value, str):
        try:
            candidate = json.loads(value)
        except Exception:
            candidate = value
    if isinstance(candidate, Mapping):
        out: List[float] = []
        for index in range(6):
            numeric = _safe_float(candidate.get(f"C{index}"))
            if numeric is not None:
                out.append(float(numeric))
        return out
    if isinstance(candidate, Sequence) and not isinstance(candidate, (str, bytes)):
        out = []
        for item in candidate:
            numeric = _safe_float(item)
            if numeric is not None:
                out.append(float(numeric))
        return out
    return []


def _secondary_channel_for_component(component: str) -> int:
    return 4 if str(component or "").strip().lower() == "h2o" else 3


def _preserved_secondary_coefficients_for_group(
    cfg: CandidateCoefficientPolicyConfig,
    *,
    component: str,
    prefix: str,
    device_id: str,
) -> List[float]:
    """Return preserved secondary compensation values for this analyzer.

    The accepted JSON shapes mirror the existing GETCO backup files:

    ``{"030": {"GETCO3_before_live": [...]}}`` or
    ``{"devices": [{"analyzer_device_id": "030", "GETCO3_before": [...]}]}``.
    """

    snapshot = cfg.preserved_secondary_coefficients
    if not isinstance(snapshot, Mapping):
        return []
    normalized = _normalized_device_id(device_id)
    candidates: List[Mapping[str, Any]] = []
    for key in (normalized, str(device_id or "").strip(), str(prefix or "").strip().lower()):
        value = snapshot.get(key)
        if isinstance(value, Mapping):
            candidates.append(value)
    devices = snapshot.get("devices")
    if isinstance(devices, Sequence) and not isinstance(devices, (str, bytes)):
        for item in devices:
            if not isinstance(item, Mapping):
                continue
            item_device = _normalized_device_id(
                item.get("analyzer_device_id")
                or item.get("device_id")
                or item.get("DeviceId")
                or item.get("id")
            )
            item_prefix = str(item.get("analyzer_prefix") or "").strip().lower()
            if item_device == normalized or (item_prefix and item_prefix == str(prefix or "").strip().lower()):
                candidates.append(item)

    channel = _secondary_channel_for_component(component)
    keys = (
        f"GETCO{channel}_before_live",
        f"GETCO{channel}_before_review",
        f"GETCO{channel}_before",
        f"SENCO{channel}_before_live",
        f"SENCO{channel}_before_review",
        f"SENCO{channel}_before",
        f"senco{channel}_readback",
        f"GETCO{channel}",
        f"SENCO{channel}",
        str(channel),
    )
    for candidate in candidates:
        for key in keys:
            values = _snapshot_numeric_values(candidate.get(key))
            if values:
                return values
    return []


def _preserved_secondary_compensation_value(
    component: str,
    row: Mapping[str, Any],
    coefficients: Sequence[float],
) -> float:
    if not coefficients:
        return 0.0
    if str(component or "").strip().lower() != "co2":
        return 0.0
    value = co2_senco3_temperature_compensation_ppm(
        coefficients,
        row.get("_ratio"),
        row.get("_temperature_c"),
        row.get("_pressure_hpa"),
    )
    return 0.0 if value is None else float(value)


def _fit_target_array(
    *,
    component: str,
    rows: Sequence[Mapping[str, Any]],
    preserved_secondary_coefficients: Sequence[float],
) -> np.ndarray:
    values: List[float] = []
    for row in rows:
        target = float(row["_target"])
        if str(component or "").strip().lower() == "co2":
            raw_target = co2_firmware_final_to_raw_ppm(target, row.get("_h2o_mmol"))
            if raw_target is not None:
                target = float(raw_target)
        target -= _preserved_secondary_compensation_value(component, row, preserved_secondary_coefficients)
        values.append(target)
    return np.asarray(values, dtype=float)


def _prediction_array(
    *,
    component: str,
    rows: Sequence[Mapping[str, Any]],
    terms: Sequence[str],
    coefficients: np.ndarray,
    preserved_secondary_coefficients: Sequence[float] = (),
) -> np.ndarray:
    primary = _build_matrix(rows, terms) @ coefficients
    secondary = np.asarray(
        [
            _preserved_secondary_compensation_value(component, row, preserved_secondary_coefficients)
            for row in rows
        ],
        dtype=float,
    )
    raw_prediction = primary + secondary
    if str(component or "").strip().lower() != "co2":
        return raw_prediction
    final_predictions: List[float] = []
    for raw_value, row in zip(raw_prediction, rows):
        final_value = co2_raw_to_firmware_final_ppm(raw_value, row.get("_h2o_mmol"))
        final_predictions.append(float(raw_value if final_value is None else final_value))
    return np.asarray(final_predictions, dtype=float)


def _span(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return float(max(values) - min(values))


def _distinct_count(values: Sequence[float], *, digits: int = 9) -> int:
    return len({round(float(value), digits) for value in values})


def _distinct_target_count(component: str, values: Sequence[float]) -> int:
    if component == "h2o":
        return _distinct_count(values, digits=3)
    return _distinct_count(values, digits=6)


def _distinct_fit_target_count(component: str, rows: Sequence[Mapping[str, Any]]) -> int:
    values = [float(row["_target"]) for row in rows]
    if component != "h2o":
        return _distinct_target_count(component, values)
    identities = {_point_identity(row, float(row["_target"])) for row in rows}
    physical_identities = {identity for identity in identities if not identity.startswith("target:")}
    if physical_identities:
        return len(physical_identities)
    return _distinct_target_count(component, values)


def _component_plan_targets(plan: Mapping[str, Any], component: str) -> List[float]:
    gases = plan.get("standard_gases")
    if not isinstance(gases, Sequence) or isinstance(gases, (str, bytes)):
        return []
    targets: List[float] = []
    for gas in gases:
        if not isinstance(gas, Mapping):
            continue
        if str(gas.get("component") or "").strip().lower() != component:
            continue
        value = _safe_float(gas.get("certificate_value"))
        if value is not None:
            targets.append(value)
    return targets


def _component_plan_target_uncertainties(plan: Mapping[str, Any], component: str) -> List[Tuple[float, float]]:
    gases = plan.get("standard_gases")
    if not isinstance(gases, Sequence) or isinstance(gases, (str, bytes)):
        return []
    target_uncertainties: List[Tuple[float, float]] = []
    for gas in gases:
        if not isinstance(gas, Mapping):
            continue
        if str(gas.get("component") or "").strip().lower() != component:
            continue
        target = _safe_float(gas.get("certificate_value"))
        uncertainty = _safe_float(gas.get("certificate_uncertainty"))
        if target is not None and uncertainty is not None and uncertainty >= 0.0:
            target_uncertainties.append((target, uncertainty))
    return target_uncertainties


def _target_match_tolerance(component: str, target: float) -> float:
    if component == "h2o":
        return max(0.001, abs(target) * 1.0e-4)
    return max(0.05, abs(target) * 1.0e-5)


def _target_outlier_key(component: str, target: float) -> float:
    if component == "h2o":
        return round(float(target), 3)
    return round(float(target), 6)


def _matched_certificate_uncertainty(
    component: str,
    target: float,
    plan_target_uncertainties: Sequence[Tuple[float, float]],
) -> Optional[Tuple[float, float]]:
    if not plan_target_uncertainties:
        return None
    tolerance = _target_match_tolerance(component, target)
    candidates = [
        (abs(float(plan_target) - target), float(plan_target), float(uncertainty))
        for plan_target, uncertainty in plan_target_uncertainties
        if abs(float(plan_target) - target) <= tolerance
    ]
    if not candidates:
        return None
    _, matched_target, uncertainty = min(candidates, key=lambda item: item[0])
    return matched_target, uncertainty


def _verification_error_limit(
    *,
    component: str,
    verification_rows: Sequence[Mapping[str, Any]],
    plan_target_uncertainties: Sequence[Tuple[float, float]],
    cfg: CandidateCoefficientPolicyConfig,
) -> Tuple[float, str, str]:
    fixed_limit = float(cfg.verification_max_abs_error.get(component, cfg.verification_max_abs_error.get("co2", 2.0)))
    certificate_uncertainties: List[Tuple[float, float]] = []
    if cfg.verification_use_certificate_uncertainty:
        for row in verification_rows:
            target = _safe_float(row.get("_target"))
            if target is None:
                continue
            matched = _matched_certificate_uncertainty(component, target, plan_target_uncertainties)
            if matched is not None:
                certificate_uncertainties.append(matched)
    if not certificate_uncertainties:
        return fixed_limit, f"fixed_abs_error={fixed_limit:g}", ""

    max_uncertainty = max(float(uncertainty) for _, uncertainty in certificate_uncertainties)
    matched_targets = ";".join(
        f"{target:g}:{uncertainty:g}"
        for target, uncertainty in sorted(set(certificate_uncertainties))
    )
    limit = max(fixed_limit, max_uncertainty)
    source = f"max(fixed_abs_error={fixed_limit:g},certificate_expanded_uncertainty={max_uncertainty:g})"
    return limit, source, matched_targets


def _target_value(row: Mapping[str, Any], component: str, plan_targets: Sequence[float]) -> Optional[float]:
    if component == "h2o":
        keys = (
            "target_h2o_mmol",
            "h2o_mmol_target",
            "h2o_target_mmol",
            "h2o_reference_mmol",
            "h2o_certificate_value",
            "ppm_H2O_Dew",
            "certificate_value",
        )
    else:
        keys = (
            "target_co2_ppm",
            "co2_ppm_target",
            "co2_target_ppm",
            "co2_reference_ppm",
            "co2_certificate_value",
            "ppm_CO2_Tank",
            "certificate_value",
        )
    value = _safe_float(_first_value(row, keys))
    if value is not None:
        return value
    if component == "h2o":
        derived = _h2o_target_from_dewpoint(row)
        if derived is not None:
            return derived
    if len(plan_targets) == 1:
        return float(plan_targets[0])
    return None


def _h2o_target_from_dewpoint(row: Mapping[str, Any]) -> Optional[float]:
    dewpoint = _first_value(
        row,
        (
            "dewpoint_c",
            "dewpoint_live_c",
            "dewpoint_reference_c",
            "Dew",
        ),
    )
    pressure = _first_value(
        row,
        (
            "pressure_gauge_hpa",
            "com22_pressure_hpa",
            "pressure_reference_hpa",
        ),
    )
    return dewpoint_to_h2o_mmol_per_mol(dewpoint, pressure)


def _ratio_value(row: Mapping[str, Any], component: str, prefix: str) -> Optional[float]:
    if component == "h2o":
        keys = (
            f"{prefix}_h2o_ratio_f",
            f"{prefix}_h2o_ratio_raw",
            "h2o_ratio_f",
            "h2o_ratio_raw",
            "R_H2O",
        )
    else:
        keys = (
            f"{prefix}_co2_ratio_f",
            f"{prefix}_co2_ratio_raw",
            "co2_ratio_f",
            "co2_ratio_raw",
            "R_CO2",
        )
    return _safe_float(_first_value(row, keys))


def _h2o_mmol_value(row: Mapping[str, Any], prefix: str) -> Optional[float]:
    return _safe_float(
        _first_value(
            row,
            (
                f"{prefix}_h2o_mmol",
                f"{prefix}_h2o_mmol_mol",
                "h2o_mmol",
                "h2o_mmol_mol",
                "ppm_H2O",
                "H2O_mmol",
            ),
        )
    )


def _h2o_reference_mmol_value(row: Mapping[str, Any]) -> Optional[float]:
    direct = _safe_float(
        _first_value(
            row,
            (
                "target_h2o_mmol",
                "h2o_mmol_target",
                "h2o_target_mmol",
                "h2o_reference_mmol",
                "h2o_certificate_value",
            ),
        )
    )
    if direct is not None:
        return direct
    return _h2o_target_from_dewpoint(row)


def _co2_dry_correction_h2o_value(
    row: Mapping[str, Any],
    prefix: str,
    cfg: CandidateCoefficientPolicyConfig,
) -> tuple[Optional[float], str]:
    """Choose H2O evidence for the CO2 firmware dry-basis correction layer."""

    source = str(cfg.co2_dry_correction_h2o_source or "reference_first").strip().lower()
    reference = _h2o_reference_mmol_value(row)
    analyzer = _h2o_mmol_value(row, prefix)
    if source in {"reference", "reference_only", "dewpoint", "dewpoint_reference"}:
        return reference, "reference_h2o"
    if source in {"analyzer", "analyzer_output", "firmware", "mode2"}:
        return analyzer, "analyzer_output_h2o"
    if source in {"analyzer_first", "firmware_first"} and analyzer is not None:
        return analyzer, "analyzer_output_h2o"
    if reference is not None:
        return reference, "reference_h2o"
    return analyzer, "analyzer_output_h2o_fallback"


def _temperature_c(row: Mapping[str, Any], prefix: str) -> Optional[float]:
    return _safe_float(
        _first_value(
            row,
            (
                f"{prefix}_chamber_temp_c",
                "chamber_temp_c",
                "thermometer_temp_c",
                "temp_c",
                "Temp",
                "TempSet",
            ),
        )
    )


def _is_hard_bad_temperature(value: Optional[float], cfg: CandidateCoefficientPolicyConfig) -> bool:
    if value is None:
        return False
    tolerance = float(cfg.hard_bad_temperature_tolerance_c)
    return any(abs(float(value) - float(bad_value)) <= tolerance for bad_value in cfg.hard_bad_temperature_values_c)


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "certified", "pass"}


def _co2_zero_anchor_certified(row: Mapping[str, Any]) -> bool:
    if _truthy(
        _first_value(
            row,
            (
                "co2_zero_certified",
                "zero_gas_co2_certified",
                "co2_zero_certificate_available",
                "standard_gas_co2_zero_certified",
            ),
        )
    ):
        return True
    role = str(row.get("standard_role") or "").strip().lower()
    return role in {"co2_zero_standard", "certified_co2_zero_gas"}


def _pressure_hpa(row: Mapping[str, Any], prefix: str) -> Optional[float]:
    direct = _safe_float(
        _first_value(
            row,
            (
                "pressure_gauge_hpa",
                "com22_pressure_hpa",
                "pressure_reference_hpa",
                "pressure_hpa",
                "pressure_target_hpa",
                "PressureTarget",
            ),
        )
    )
    if direct is not None:
        return direct
    kpa = _safe_float(
        _first_value(
            row,
            (
                f"{prefix}_pressure_kpa",
                "analyzer_pressure_kpa",
                "pressure_kpa",
            ),
        )
    )
    if kpa is not None:
        return kpa * 10.0
    bar = _safe_float(row.get("BAR"))
    if bar is not None:
        return bar * 1000.0
    return None


def _sample_role(row: Mapping[str, Any]) -> str:
    text = str(
        _first_value(
            row,
            (
                "sample_role",
                "formal_sample_role",
                "candidate_sample_role",
                "fit_role",
                "role",
                "point_role",
            ),
        )
        or ""
    ).strip().lower()
    if "verification" in text or "verify" in text:
        return "verification"
    if "diagnostic" in text:
        return "diagnostic"
    if "reject" in text or "exclude" in text:
        return "excluded"
    return "fit"


def _point_identity(row: Mapping[str, Any], target: Optional[float]) -> str:
    value = _first_value(
        row,
        (
            "verification_point_id",
            "point_id",
            "point_key",
            "point_tag",
            "sample_point",
            "PointTag",
            "PointRow",
            "sample_index",
        ),
    )
    if value not in (None, ""):
        return str(value)
    if target is not None:
        return f"target:{target:.12g}"
    return ""


def _group_key(row: Mapping[str, Any]) -> Tuple[str, str, str]:
    return (
        str(row.get("component") or "").strip().lower(),
        str(row.get("analyzer_prefix") or "").strip().lower() or "ga01",
        _normalized_device_id(row.get("analyzer_device_id")),
    )


def _model_terms(
    *,
    distinct_targets: int,
    pressure_span_hpa: float,
    temperature_span_c: float,
    cfg: CandidateCoefficientPolicyConfig,
) -> tuple[List[str], List[str], List[str]]:
    terms = ["intercept", "R"]
    if distinct_targets >= 3:
        terms.append("R2")
    if distinct_targets >= 4:
        terms.append("R3")

    frozen: List[str] = []
    notes: List[str] = []
    frozen.extend(PRESSURE_TERMS)
    notes.append("pressure_terms_excluded_current_atmosphere_open_flow_contract")
    if cfg.allow_pressure_terms:
        notes.append("allow_pressure_terms_ignored_current_atmosphere_open_flow_contract")

    if not cfg.allow_temperature_terms:
        frozen.extend(TEMPERATURE_TERMS)
        notes.append("temperature_terms_disabled_for_current_v1_5_scope")
    elif temperature_span_c < float(cfg.temperature_span_min_c_for_terms):
        frozen.extend(TEMPERATURE_TERMS)
        notes.append("temperature_span_insufficient_for_temperature_terms")
    else:
        terms.append("T")
        terms.append("T2")
        terms.append("RT")

    return terms, list(dict.fromkeys(frozen)), notes


def _feature_row(term: str, ratio: float, pressure_hpa: float, temperature_c: float) -> float:
    temp_k = temperature_c + 273.15
    if term == "intercept":
        return 1.0
    if term == "R":
        return ratio
    if term == "R2":
        return ratio**2
    if term == "R3":
        return ratio**3
    if term == "P":
        return pressure_hpa
    if term == "T":
        return temp_k
    if term == "RT":
        return ratio * temp_k
    if term == "RP":
        return ratio * pressure_hpa
    if term == "RTP":
        return ratio * temp_k * pressure_hpa
    if term == "T2":
        return temp_k**2
    raise ValueError(f"Unsupported candidate model term: {term}")


def _build_matrix(rows: Sequence[Mapping[str, Any]], terms: Sequence[str]) -> np.ndarray:
    matrix: List[List[float]] = []
    for row in rows:
        ratio = float(row["_ratio"])
        pressure = float(row["_pressure_hpa"])
        temp = float(row["_temperature_c"])
        matrix.append([_feature_row(term, ratio, pressure, temp) for term in terms])
    return np.asarray(matrix, dtype=float)


def _centered_feature_row(term: str, ratio: float, temperature_c: float, ratio_center: float, temp_k_center: float) -> float:
    ratio_delta = float(ratio) - float(ratio_center)
    temp_delta = (float(temperature_c) + 273.15) - float(temp_k_center)
    if term == "intercept":
        return 1.0
    if term == "R":
        return ratio_delta
    if term == "R2":
        return ratio_delta**2
    if term == "R3":
        return ratio_delta**3
    if term == "T":
        return temp_delta
    if term == "T2":
        return temp_delta**2
    if term == "RT":
        return ratio_delta * temp_delta
    raise ValueError(f"Unsupported centered candidate model term: {term}")


def _build_centered_matrix(
    rows: Sequence[Mapping[str, Any]],
    terms: Sequence[str],
    *,
    ratio_center: float,
    temp_k_center: float,
) -> np.ndarray:
    matrix: List[List[float]] = []
    for row in rows:
        ratio = float(row["_ratio"])
        temp = float(row["_temperature_c"])
        matrix.append(
            [
                _centered_feature_row(
                    term,
                    ratio,
                    temp,
                    ratio_center=ratio_center,
                    temp_k_center=temp_k_center,
                )
                for term in terms
            ]
        )
    return np.asarray(matrix, dtype=float)


def _scaled_lstsq(matrix: np.ndarray, target: np.ndarray) -> tuple[np.ndarray, int, float]:
    scales = np.linalg.norm(matrix, axis=0)
    scales = np.where(np.isfinite(scales) & (scales > 0.0), scales, 1.0)
    scaled_matrix = matrix / scales
    rank = int(np.linalg.matrix_rank(scaled_matrix))
    condition_number = float(np.linalg.cond(scaled_matrix))
    scaled_coefficients, _, _, _ = np.linalg.lstsq(scaled_matrix, target, rcond=None)
    coefficients = np.asarray(scaled_coefficients, dtype=float) / scales
    return coefficients, rank, condition_number


def _centered_to_absolute_coefficients(
    terms: Sequence[str],
    centered_coefficients: Sequence[float],
    *,
    ratio_center: float,
    temp_k_center: float,
) -> np.ndarray:
    """Convert centered R/T polynomial coefficients to firmware absolute terms.

    The solver uses centered variables only to improve numerical conditioning:
    rc = R - R0 and tc = T - T0. Firmware still evaluates absolute R, T, R*T,
    so the fitted coefficients are algebraically transformed before export.
    """

    values = {term: 0.0 for term in CENTERABLE_TERMS}
    for term, coefficient in zip(terms, centered_coefficients):
        values[term] = float(coefficient)

    r0 = float(ratio_center)
    t0 = float(temp_k_center)
    b0 = values["intercept"]
    b1 = values["R"]
    b2 = values["R2"]
    b3 = values["R3"]
    bt = values["T"]
    bt2 = values["T2"]
    brt = values["RT"]

    absolute = {
        "intercept": b0 - r0 * b1 + (r0**2) * b2 - (r0**3) * b3 - t0 * bt + (t0**2) * bt2 + r0 * t0 * brt,
        "R": b1 - 2.0 * r0 * b2 + 3.0 * (r0**2) * b3 - t0 * brt,
        "R2": b2 - 3.0 * r0 * b3,
        "R3": b3,
        "T": bt - 2.0 * t0 * bt2 - r0 * brt,
        "T2": bt2,
        "RT": brt,
    }
    return np.asarray([absolute[term] for term in terms], dtype=float)


def _fit_candidate_coefficients(
    rows: Sequence[Mapping[str, Any]],
    terms: Sequence[str],
    target: np.ndarray,
) -> tuple[np.ndarray, int, float, float, Dict[str, Any]]:
    absolute_matrix = _build_matrix(rows, terms)
    absolute_coefficients, absolute_rank, absolute_condition = _scaled_lstsq(absolute_matrix, target)
    if not set(terms).issubset(CENTERABLE_TERMS):
        return absolute_coefficients, absolute_rank, absolute_condition, absolute_condition, {
            "fit_basis": "absolute_firmware_terms",
            "ratio_center": "",
            "temperature_k_center": "",
        }

    ratio_values = [float(row["_ratio"]) for row in rows]
    temp_k_values = [float(row["_temperature_c"]) + 273.15 for row in rows]
    ratio_center = mean(ratio_values) if ratio_values else 0.0
    temp_k_center = mean(temp_k_values) if temp_k_values else 273.15
    centered_matrix = _build_centered_matrix(
        rows,
        terms,
        ratio_center=ratio_center,
        temp_k_center=temp_k_center,
    )
    centered_coefficients, centered_rank, centered_condition = _scaled_lstsq(centered_matrix, target)
    transformed = _centered_to_absolute_coefficients(
        terms,
        centered_coefficients,
        ratio_center=ratio_center,
        temp_k_center=temp_k_center,
    )
    return transformed, centered_rank, centered_condition, absolute_condition, {
        "fit_basis": "centered_R_T_transformed_to_firmware_absolute_terms",
        "ratio_center": ratio_center,
        "temperature_k_center": temp_k_center,
        "absolute_condition_number": absolute_condition,
    }


def _prepare_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    component: str,
    prefix: str,
    plan_targets: Sequence[float],
    cfg: CandidateCoefficientPolicyConfig,
    excluded_fit_target_keys: Sequence[float] = (),
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    prepared: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    excluded_targets = {float(value) for value in excluded_fit_target_keys}
    for row in rows:
        target = _target_value(row, component, plan_targets)
        ratio = _ratio_value(row, component, prefix)
        temp = _temperature_c(row, prefix)
        pressure = _pressure_hpa(row, prefix)
        reasons: List[str] = []
        if target is None:
            reasons.append("target_missing")
        elif (
            component == "co2"
            and abs(float(target)) <= 1.0e-9
            and not bool(cfg.allow_uncertified_zero_co2_anchor)
            and not _co2_zero_anchor_certified(row)
        ):
            reasons.append("co2_zero_anchor_uncertified")
        if ratio is None:
            reasons.append("ratio_missing")
        if temp is None:
            reasons.append("temperature_missing")
        elif _is_hard_bad_temperature(temp, cfg):
            reasons.append(f"temperature_hard_bad_value:{temp:g}")
        if pressure is None:
            reasons.append("pressure_missing")
        role = _sample_role(row)
        if role in {"diagnostic", "excluded"}:
            reasons.append(f"sample_role_{role}")
        if target is not None and role == "fit" and _target_outlier_key(component, float(target)) in excluded_targets:
            reasons.append("source_common_mode_target_outlier")
        item = dict(row)
        item["_source_sample_role"] = role
        item["_candidate_role"] = "fit" if role == "verification" and cfg.fit_all_eligible_samples else role
        item["_target"] = target
        item["_ratio"] = ratio
        item["_temperature_c"] = temp
        item["_pressure_hpa"] = pressure
        if component == "co2":
            h2o_mmol, h2o_source = _co2_dry_correction_h2o_value(row, prefix, cfg)
            item["_h2o_mmol"] = h2o_mmol
            item["_h2o_mmol_source"] = h2o_source
        else:
            item["_h2o_mmol"] = _h2o_mmol_value(row, prefix)
            item["_h2o_mmol_source"] = "analyzer_output_h2o"
        item["_point_identity"] = _point_identity(row, target)
        if reasons:
            item["_candidate_reject_reasons"] = ";".join(reasons)
            rejected.append(item)
            continue
        prepared.append(item)
    return prepared, rejected


def _point_mean_rows(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[str, List[Mapping[str, Any]]] = {}
    for row in rows:
        identity = str(row.get("_point_identity") or "").strip()
        if not identity:
            identity = f"target:{float(row['_target']):.12g}"
        groups.setdefault(identity, []).append(row)

    point_rows: List[Dict[str, Any]] = []
    for identity in sorted(groups):
        group = groups[identity]
        item = dict(group[-1])
        item["_point_identity"] = identity
        item["_point_sample_count"] = len(group)
        item["_source_sample_indices"] = ";".join(str(row.get("sample_index", "")) for row in group)
        for key in ("_target", "_ratio", "_temperature_c", "_pressure_hpa", "_h2o_mmol"):
            values = [_safe_float(row.get(key)) for row in group]
            numeric = [float(value) for value in values if value is not None]
            if numeric:
                item[key] = mean(numeric)
        sources = sorted(
            {
                str(row.get("_h2o_mmol_source") or "").strip()
                for row in group
                if str(row.get("_h2o_mmol_source") or "").strip()
            }
        )
        if sources:
            item["_h2o_mmol_source"] = ";".join(sources)
        item["sample_index"] = identity
        point_rows.append(item)
    return point_rows


def _temperature_group_label(row: Mapping[str, Any]) -> str:
    identity = str(row.get("_point_identity") or row.get("point_identity") or row.get("sample_index") or "")
    match = re.search(r"(?:^|_)T(?P<value>m?\d+(?:\.\d+)?)", identity)
    if match:
        return match.group("value").replace("m", "-")
    temp = _safe_float(row.get("_temperature_c") or row.get("temperature_c"))
    if temp is None:
        return "unknown"
    return f"{round(float(temp)):.0f}"


def _temperature_target_grid_summary(
    component: str,
    rows: Sequence[Mapping[str, Any]],
    terms: Sequence[str],
) -> Dict[str, Any]:
    if component != "co2" or not any(term in terms for term in TEMPERATURE_TERMS):
        return {
            "temperature_target_grid_status": "not_applicable",
            "temperature_group_count": "",
            "min_targets_per_temperature": "",
            "max_targets_per_temperature": "",
            "temperature_target_grid": "",
        }

    by_temperature: Dict[str, set[float]] = {}
    for row in rows:
        label = _temperature_group_label(row)
        target = _safe_float(row.get("_target"))
        if target is None:
            continue
        by_temperature.setdefault(label, set()).add(round(float(target), 6))

    counts = [len(targets) for targets in by_temperature.values()]
    min_count = min(counts) if counts else 0
    max_count = max(counts) if counts else 0
    status = "balanced_temperature_target_grid"
    if min_count < 4 or (max_count > 0 and min_count / max_count < 0.5):
        status = "imbalanced_temperature_target_grid_blocks_final_write"
    grid = ";".join(
        f"{label}:{','.join(f'{target:g}' for target in sorted(targets))}"
        for label, targets in sorted(by_temperature.items())
    )
    return {
        "temperature_target_grid_status": status,
        "temperature_group_count": len(by_temperature),
        "min_targets_per_temperature": min_count,
        "max_targets_per_temperature": max_count,
        "temperature_target_grid": grid,
    }


def _residual_rows(
    *,
    component: str,
    prefix: str,
    device_id: str,
    rows: Sequence[Mapping[str, Any]],
    terms: Sequence[str],
    coefficients: np.ndarray,
    residual_role: str,
    preserved_secondary_coefficients: Sequence[float] = (),
) -> List[Dict[str, Any]]:
    if not rows:
        return []
    targets = np.asarray([float(row["_target"]) for row in rows], dtype=float)
    predictions = _prediction_array(
        component=component,
        rows=rows,
        terms=terms,
        coefficients=coefficients,
        preserved_secondary_coefficients=preserved_secondary_coefficients,
    )
    out: List[Dict[str, Any]] = []
    for row, prediction, target in zip(rows, predictions, targets):
        out.append(
            {
                "component": component,
                "analyzer_prefix": prefix,
                "analyzer_device_id": device_id,
                "residual_role": residual_role,
                "sample_index": row.get("sample_index", ""),
                "point_identity": row.get("_point_identity", ""),
                "point_sample_count": row.get("_point_sample_count", ""),
                "target_value": float(target),
                "ratio": float(row["_ratio"]),
                "temperature_c": float(row["_temperature_c"]),
                "pressure_hpa": float(row["_pressure_hpa"]),
                "h2o_mmol": row.get("_h2o_mmol", ""),
                "h2o_mmol_source": row.get("_h2o_mmol_source", ""),
                "prediction": float(prediction),
                "error": float(prediction - target),
                "model_terms": ";".join(terms),
                "preserved_secondary_compensation": float(
                    _preserved_secondary_compensation_value(component, row, preserved_secondary_coefficients)
                ),
            }
        )
    return out


def _metrics_or_empty(
    rows: Sequence[Mapping[str, Any]],
    terms: Sequence[str],
    coefficients: np.ndarray,
    *,
    component: str,
    preserved_secondary_coefficients: Sequence[float] = (),
) -> Dict[str, Any]:
    if not rows:
        return {}
    truth = np.asarray([float(row["_target"]) for row in rows], dtype=float)
    prediction = _prediction_array(
        component=component,
        rows=rows,
        terms=terms,
        coefficients=coefficients,
        preserved_secondary_coefficients=preserved_secondary_coefficients,
    )
    return compute_metrics(truth, prediction)


def _fit_point_fractional_residuals(
    rows: Sequence[Mapping[str, Any]],
    terms: Sequence[str],
    *,
    component: str = "co2",
    preserved_secondary_coefficients: Sequence[float] = (),
) -> Optional[List[Dict[str, Any]]]:
    if not rows or not terms:
        return None
    target = _fit_target_array(
        component=component,
        rows=rows,
        preserved_secondary_coefficients=preserved_secondary_coefficients,
    )
    try:
        coefficients, rank, _, _, _ = _fit_candidate_coefficients(rows, terms, target)
    except Exception:
        return None
    if rank < len(terms):
        return None
    predictions = _prediction_array(
        component=component,
        rows=rows,
        terms=terms,
        coefficients=coefficients,
        preserved_secondary_coefficients=preserved_secondary_coefficients,
    )
    truth = np.asarray([float(row["_target"]) for row in rows], dtype=float)
    residuals: List[Dict[str, Any]] = []
    for row, prediction, target_value in zip(rows, predictions, truth):
        if abs(float(target_value)) <= 1.0e-12:
            continue
        error = float(prediction - target_value)
        residuals.append(
            {
                "point_identity": row.get("_point_identity", ""),
                "target_value": float(target_value),
                "target_key": _target_outlier_key("co2", float(target_value)),
                "prediction": float(prediction),
                "error": error,
                "fractional_error": error / float(target_value),
            }
        )
    return residuals


def _detect_common_mode_fit_target_outliers(
    *,
    keys: Sequence[Tuple[str, str, str]],
    groups: Mapping[Tuple[str, str, str], Sequence[Mapping[str, Any]]],
    review_by_key: Mapping[Tuple[str, str, str], Mapping[str, Any]],
    plan: Mapping[str, Any],
    cfg: CandidateCoefficientPolicyConfig,
) -> tuple[Dict[Tuple[str, str, str], List[float]], List[Dict[str, Any]]]:
    """Find target points that behave like source/route outliers, not analyzer response.

    The rule is intentionally conservative: a target is excluded only when the
    same target is an above-limit same-sign residual on several independent
    analyzer devices, and removing that target leaves each device with a
    well-behaved fit. That matches the physical interpretation of a suspect
    gas source, valve path, or purge state rather than a SENCO1/3 response term.
    """

    if not cfg.enable_common_mode_fit_target_outlier_rejection:
        return {}, []

    threshold = float(cfg.common_mode_outlier_abs_fraction.get("co2", 0.01))
    min_devices = int(cfg.common_mode_outlier_min_devices)
    min_remaining_targets = int(cfg.common_mode_outlier_min_remaining_targets)
    if threshold <= 0.0 or min_devices <= 1:
        return {}, []

    candidate_hits: Dict[Tuple[float, int], List[Dict[str, Any]]] = {}
    for key in keys:
        component, prefix, device_id = key
        if component != "co2":
            continue
        review_row = review_by_key.get(key, {})
        if str(review_row.get("candidate_review_status") or "").strip() not in {"", "ready_for_reviewer"}:
            continue
        prepared, _ = _prepare_rows(
            groups.get(key, []),
            component=component,
            prefix=prefix,
            plan_targets=_component_plan_targets(plan, component),
            cfg=cfg,
        )
        fit_rows = [row for row in prepared if row.get("_candidate_role") == "fit"]
        point_rows = _point_mean_rows(fit_rows)
        if _distinct_fit_target_count(component, point_rows) <= min_remaining_targets:
            continue
        pressure_span = _span([float(row["_pressure_hpa"]) for row in point_rows])
        temp_span = _span([float(row["_temperature_c"]) for row in point_rows])
        terms, _, _ = _model_terms(
            distinct_targets=_distinct_fit_target_count(component, point_rows),
            pressure_span_hpa=pressure_span,
            temperature_span_c=temp_span,
            cfg=cfg,
        )
        preserved_secondary = _preserved_secondary_coefficients_for_group(
            cfg,
            component=component,
            prefix=prefix,
            device_id=device_id,
        )
        full_residuals = _fit_point_fractional_residuals(
            point_rows,
            terms,
            component=component,
            preserved_secondary_coefficients=preserved_secondary,
        )
        if not full_residuals:
            continue
        for residual in full_residuals:
            frac = float(residual["fractional_error"])
            if abs(frac) <= threshold:
                continue
            target_key = float(residual["target_key"])
            remaining = [
                row
                for row in point_rows
                if _target_outlier_key(component, float(row["_target"])) != target_key
            ]
            if _distinct_fit_target_count(component, remaining) < min_remaining_targets:
                continue
            remaining_residuals = _fit_point_fractional_residuals(
                remaining,
                terms,
                component=component,
                preserved_secondary_coefficients=preserved_secondary,
            )
            if not remaining_residuals:
                continue
            remaining_max_fraction = max(abs(float(item["fractional_error"])) for item in remaining_residuals)
            if remaining_max_fraction > threshold:
                continue
            sign = 1 if frac > 0.0 else -1
            candidate_hits.setdefault((target_key, sign), []).append(
                {
                    "component": component,
                    "analyzer_prefix": prefix,
                    "analyzer_device_id": device_id,
                    "target_value": residual["target_value"],
                    "target_key": target_key,
                    "point_identity": residual["point_identity"],
                    "full_fit_error": residual["error"],
                    "full_fit_fractional_error": frac,
                    "remaining_fit_max_fractional_error": remaining_max_fraction,
                    "model_terms": ";".join(terms),
                }
            )

    exclusions: Dict[Tuple[str, str, str], List[float]] = {}
    diagnostics: List[Dict[str, Any]] = []
    for (target_key, sign), hits in sorted(candidate_hits.items()):
        device_ids = sorted({str(hit["analyzer_device_id"]) for hit in hits if hit.get("analyzer_device_id")})
        if len(device_ids) < min_devices:
            continue
        target_values = [float(hit["target_value"]) for hit in hits]
        aggregate = {
            "component": "co2",
            "target_key": target_key,
            "target_value_mean": mean(target_values),
            "direction": "positive" if sign > 0 else "negative",
            "device_count": len(device_ids),
            "devices": ";".join(device_ids),
            "threshold_fraction": threshold,
            "reason": "same_sign_multi_device_residual_improves_after_target_removal",
            "physical_interpretation": "suspect_standard_gas_or_route_state_not_analyzer_response",
            "auto_excluded_from_fit": True,
        }
        diagnostics.append(aggregate)
        for hit in hits:
            key = (
                str(hit["component"]),
                str(hit["analyzer_prefix"]),
                _normalized_device_id(hit["analyzer_device_id"]),
            )
            exclusions.setdefault(key, [])
            if target_key not in exclusions[key]:
                exclusions[key].append(target_key)
            item = dict(hit)
            item.update(
                {
                    "direction": aggregate["direction"],
                    "device_count": len(device_ids),
                    "threshold_fraction": threshold,
                    "reason": aggregate["reason"],
                    "physical_interpretation": aggregate["physical_interpretation"],
                    "auto_excluded_from_fit": True,
                }
            )
            diagnostics.append(item)
    return exclusions, diagnostics


def _candidate_for_group(
    *,
    component: str,
    prefix: str,
    device_id: str,
    rows: Sequence[Mapping[str, Any]],
    plan_targets: Sequence[float],
    plan_target_uncertainties: Sequence[Tuple[float, float]],
    review_row: Mapping[str, Any],
    cfg: CandidateCoefficientPolicyConfig,
    common_mode_outlier_target_keys: Sequence[float] = (),
) -> Dict[str, Any]:
    formal_review_blockers = str(review_row.get("blockers") or "").strip()
    formal_pressure_status = str(review_row.get("pressure_validation_status") or "").strip()
    formal_pressure_reason = str(review_row.get("pressure_validation_reason") or "").strip()
    formal_pressure_source = str(review_row.get("pressure_check_source") or "").strip()
    formal_pressure_condition_warning_count = int(
        _safe_float(review_row.get("pressure_condition_warning_count")) or 0
    )
    formal_window_report_warnings = str(review_row.get("window_report_warnings") or "").strip()
    prepared, preparation_rejected = _prepare_rows(
        rows,
        component=component,
        prefix=prefix,
        plan_targets=plan_targets,
        cfg=cfg,
        excluded_fit_target_keys=common_mode_outlier_target_keys,
    )
    fit_rows = [row for row in prepared if row.get("_candidate_role") == "fit"]
    verification_rows = [row for row in prepared if row.get("_candidate_role") == "verification"]
    source_verification_reused_for_fit_count = sum(
        1
        for row in fit_rows
        if str(row.get("_source_sample_role") or "").strip().lower() == "verification"
    )
    fit_point_rows = _point_mean_rows(fit_rows)
    verification_point_rows = _point_mean_rows(verification_rows)

    fit_targets = [float(row["_target"]) for row in fit_point_rows]
    pressure_values = [float(row["_pressure_hpa"]) for row in fit_point_rows]
    temp_values = [float(row["_temperature_c"]) for row in fit_point_rows]
    pressure_span = _span(pressure_values)
    temp_span = _span(temp_values)
    distinct_targets = _distinct_fit_target_count(component, fit_point_rows)
    terms, frozen_terms, model_notes = _model_terms(
        distinct_targets=distinct_targets,
        pressure_span_hpa=pressure_span,
        temperature_span_c=temp_span,
        cfg=cfg,
    )
    temperature_grid = _temperature_target_grid_summary(component, fit_point_rows, terms)
    preserved_secondary_coefficients = _preserved_secondary_coefficients_for_group(
        cfg,
        component=component,
        prefix=prefix,
        device_id=device_id,
    )
    preserved_secondary_channel = (
        f"SENCO{_secondary_channel_for_component(component)}" if preserved_secondary_coefficients else ""
    )

    blockers: List[str] = []
    warnings: List[str] = list(model_notes)
    if str(review_row.get("candidate_review_status") or "").strip() not in {"", "ready_for_reviewer"}:
        blockers.append("formal_candidate_review_not_ready")
    if len(fit_rows) < int(cfg.min_fit_samples):
        blockers.append(f"fit_samples<{int(cfg.min_fit_samples)}")
    if distinct_targets < int(cfg.min_distinct_targets):
        blockers.append(f"distinct_fit_targets<{int(cfg.min_distinct_targets)}")
    if len(terms) < 2:
        blockers.append("model_terms_insufficient")
    if formal_pressure_condition_warning_count > 0:
        warnings.append("wet_route_pressure_condition_warning_report_only")
    if temperature_grid.get("temperature_target_grid_status") == "imbalanced_temperature_target_grid_blocks_final_write":
        warnings.append("temperature_target_grid_imbalanced_blocks_final_write")

    fit_matrix: Optional[np.ndarray] = None
    rank = 0
    condition_number: Optional[float] = None
    absolute_condition_number: Optional[float] = None
    fit_basis_details: Dict[str, Any] = {
        "fit_basis": "",
        "ratio_center": "",
        "temperature_k_center": "",
        "absolute_condition_number": "",
    }
    coefficients: Optional[np.ndarray] = None
    fit_metrics: Dict[str, Any] = {}
    fit_residuals: List[Dict[str, Any]] = []
    verification_residuals: List[Dict[str, Any]] = []
    verification_metrics: Dict[str, Any] = {}
    verification_status = "not_evaluated"
    verification_reasons: List[str] = []
    verification_error_limit: Any = ""
    verification_error_limit_source = ""
    verification_certificate_uncertainties = ""
    if cfg.fit_all_eligible_samples and source_verification_reused_for_fit_count:
        warnings.append("source_verification_samples_reused_for_fit_requires_new_independent_verification")
    if common_mode_outlier_target_keys:
        warnings.append("source_common_mode_target_outlier_excluded_from_fit")
    if preserved_secondary_coefficients:
        warnings.append(
            "primary_fit_target_adjusted_for_preserved_secondary_temperature_compensation"
        )
    if component == "co2" and any(_safe_float(row.get("_h2o_mmol")) is not None for row in fit_point_rows):
        warnings.append("co2_fit_target_back_calculated_to_raw_senco13_before_h2o_dry_correction")
        h2o_sources = sorted(
            {
                str(row.get("_h2o_mmol_source") or "").strip()
                for row in fit_point_rows
                if str(row.get("_h2o_mmol_source") or "").strip()
            }
        )
        if h2o_sources:
            warnings.append("co2_dry_correction_h2o_source=" + "+".join(h2o_sources))

    if not blockers:
        fit_matrix = _build_matrix(fit_point_rows, terms)
        target = _fit_target_array(
            component=component,
            rows=fit_point_rows,
            preserved_secondary_coefficients=preserved_secondary_coefficients,
        )
        candidate_coefficients, rank, condition_number, absolute_condition_number, fit_basis_details = (
            _fit_candidate_coefficients(fit_point_rows, terms, target)
        )
        if fit_basis_details.get("fit_basis") == "centered_R_T_transformed_to_firmware_absolute_terms":
            warnings.append("fit_uses_centered_R_T_basis_then_transforms_to_firmware_terms")
        if rank < len(terms):
            blockers.append("model_matrix_rank_deficient")
        else:
            if not math.isfinite(condition_number) or condition_number > float(cfg.max_condition_number):
                blockers.append("model_matrix_ill_conditioned")
        if not blockers:
            coefficients = candidate_coefficients

    if not blockers and fit_matrix is not None and coefficients is not None:
        fit_metrics = _metrics_or_empty(
            fit_point_rows,
            terms,
            coefficients,
            component=component,
            preserved_secondary_coefficients=preserved_secondary_coefficients,
        )
        fit_residuals = _residual_rows(
            component=component,
            prefix=prefix,
            device_id=device_id,
            rows=fit_point_rows,
            terms=terms,
            coefficients=coefficients,
            residual_role="fit",
            preserved_secondary_coefficients=preserved_secondary_coefficients,
        )

        fit_point_ids = {str(row.get("_point_identity") or "") for row in fit_rows if row.get("_point_identity")}
        verification_point_ids = {
            str(row.get("_point_identity") or "")
            for row in verification_rows
            if row.get("_point_identity")
        }
        overlapping_points = sorted(fit_point_ids & verification_point_ids)
        if not verification_rows:
            verification_status = "missing"
            verification_reasons.append("verification_samples_missing")
            if cfg.fit_all_eligible_samples and source_verification_reused_for_fit_count:
                verification_reasons.append("requires_new_independent_verification")
        elif len(verification_rows) < int(cfg.min_verification_samples):
            verification_status = "fail"
            verification_reasons.append(f"verification_samples<{int(cfg.min_verification_samples)}")
        elif overlapping_points:
            verification_status = "fail"
            verification_reasons.append("verification_point_not_independent")
        else:
            verification_residuals = _residual_rows(
                component=component,
                prefix=prefix,
                device_id=device_id,
                rows=verification_point_rows,
                terms=terms,
                coefficients=coefficients,
                residual_role="verification",
                preserved_secondary_coefficients=preserved_secondary_coefficients,
            )
            verification_metrics = _metrics_or_empty(
                verification_point_rows,
                terms,
                coefficients,
                component=component,
                preserved_secondary_coefficients=preserved_secondary_coefficients,
            )
            limit, limit_source, matched_uncertainties = _verification_error_limit(
                component=component,
                verification_rows=verification_point_rows,
                plan_target_uncertainties=plan_target_uncertainties,
                cfg=cfg,
            )
            verification_error_limit = limit
            verification_error_limit_source = limit_source
            verification_certificate_uncertainties = matched_uncertainties
            max_error = _safe_float(verification_metrics.get("MaxError"))
            if max_error is None:
                verification_status = "fail"
                verification_reasons.append("verification_metrics_missing")
            elif max_error > limit:
                verification_status = "fail"
                verification_reasons.append(f"verification_max_error>{limit:g}")
            else:
                verification_status = "pass"
            fit_target_set = {round(float(value), 9) for value in fit_targets}
            verification_target_set = {round(float(row["_target"]), 9) for row in verification_point_rows}
            if fit_target_set & verification_target_set:
                warnings.append("verification_target_also_present_in_fit_set")

    if blockers:
        candidate_status = "blocked"
    elif verification_status == "pass":
        candidate_status = "verification_passed"
    elif verification_status == "fail":
        candidate_status = "verification_failed"
    else:
        candidate_status = "fit_ready_requires_verification"

    final_review_blockers: List[str] = []
    if temperature_grid.get("temperature_target_grid_status") == "imbalanced_temperature_target_grid_blocks_final_write":
        final_review_blockers.append("temperature_target_grid_imbalanced")

    formal_b_grade_count = int(_safe_float(review_row.get("b_grade_count")) or 0)
    if len(fit_rows) >= int(cfg.min_fit_samples) and distinct_targets < int(cfg.min_distinct_targets):
        evidence_reuse_class = "a_grade_single_target_review_only"
    elif len(fit_rows) >= int(cfg.min_fit_samples) and candidate_status == "verification_passed":
        evidence_reuse_class = "formal_candidate_review_ready"
    elif len(fit_rows) >= int(cfg.min_fit_samples) and candidate_status == "fit_ready_requires_verification":
        evidence_reuse_class = "fit_ready_requires_independent_verification"
    elif len(fit_rows) >= int(cfg.min_fit_samples):
        evidence_reuse_class = "a_grade_fit_samples_need_review"
    elif formal_b_grade_count > 0:
        evidence_reuse_class = "b_grade_review_only_not_fit"
    elif str(review_row.get("candidate_review_status") or "").strip() == "blocked":
        evidence_reuse_class = "device_or_point_blocked_independently"
    else:
        evidence_reuse_class = "not_reusable_for_formal_fit"

    coefficient_rows: List[Dict[str, Any]] = []
    if coefficients is not None:
        for term, value in zip(terms, coefficients):
            coefficient_rows.append(
                {
                    "component": component,
                    "analyzer_prefix": prefix,
                    "analyzer_device_id": device_id,
                    "candidate_status": candidate_status,
                    "term": term,
                    "coefficient": float(value),
                    "auto_write_allowed": False,
                    "requires_review": True,
                    "requires_verification": verification_status != "pass",
                }
            )

    policy_row = {
        "component": component,
        "analyzer_prefix": prefix,
        "analyzer_device_id": device_id,
        "candidate_status": candidate_status,
        "allowed_to_fit": bool(coefficients is not None),
        "allowed_for_review": candidate_status == "verification_passed" and not final_review_blockers,
        "auto_write_allowed": False,
        "blocked_reasons": ";".join(dict.fromkeys(blockers)),
        "final_review_blockers": ";".join(dict.fromkeys(final_review_blockers)),
        "warning_reasons": ";".join(dict.fromkeys(warnings)),
        "fit_all_eligible_samples": bool(cfg.fit_all_eligible_samples),
        "fit_target_outlier_rejected_count": sum(
            1
            for row in preparation_rejected
            if "source_common_mode_target_outlier" in str(row.get("_candidate_reject_reasons") or "")
        ),
        "fit_target_outlier_targets": ";".join(f"{target:g}" for target in sorted(common_mode_outlier_target_keys)),
        "source_verification_reused_for_fit_count": source_verification_reused_for_fit_count,
        "fit_sample_count": len(fit_rows),
        "verification_sample_count": len(verification_rows),
        "fit_point_count": len(fit_point_rows),
        "verification_point_count": len(verification_point_rows),
        "preparation_rejected_count": len(preparation_rejected),
        "distinct_fit_targets": distinct_targets,
        "pressure_span_hpa": pressure_span,
        "temperature_span_c": temp_span,
        "temperature_group_count": temperature_grid.get("temperature_group_count", ""),
        "min_targets_per_temperature": temperature_grid.get("min_targets_per_temperature", ""),
        "max_targets_per_temperature": temperature_grid.get("max_targets_per_temperature", ""),
        "temperature_target_grid_status": temperature_grid.get("temperature_target_grid_status", ""),
        "temperature_target_grid": temperature_grid.get("temperature_target_grid", ""),
        "selected_model_terms": ";".join(terms if coefficients is not None or not blockers else terms),
        "frozen_terms": ";".join(frozen_terms),
        "matrix_rank": rank,
        "matrix_condition_number": condition_number if condition_number is not None else "",
        "absolute_matrix_condition_number": (
            absolute_condition_number if absolute_condition_number is not None else ""
        ),
        "fit_basis": fit_basis_details.get("fit_basis", ""),
        "fit_basis_ratio_center": fit_basis_details.get("ratio_center", ""),
        "fit_basis_temperature_k_center": fit_basis_details.get("temperature_k_center", ""),
        "preserved_secondary_channel": preserved_secondary_channel,
        "preserved_secondary_coefficients_source": cfg.preserved_secondary_coefficients_source,
        "preserved_secondary_coefficients": (
            _compact_json(list(preserved_secondary_coefficients)) if preserved_secondary_coefficients else ""
        ),
        "primary_fit_target_contract": (
            "certificate_target_back_calculated_to_raw_minus_preserved_secondary_compensation"
            if preserved_secondary_coefficients
            else (
                "certificate_target_back_calculated_to_raw"
                if component == "co2"
                else "certificate_target_direct"
            )
        ),
        "fit_rmse": fit_metrics.get("RMSE", ""),
        "fit_max_error": fit_metrics.get("MaxError", ""),
        "verification_status": verification_status,
        "verification_reasons": ";".join(verification_reasons),
        "verification_rmse": verification_metrics.get("RMSE", ""),
        "verification_max_error": verification_metrics.get("MaxError", ""),
        "verification_error_limit": verification_error_limit,
        "verification_error_limit_source": verification_error_limit_source,
        "verification_certificate_uncertainties": verification_certificate_uncertainties,
        "formal_review_status": review_row.get("candidate_review_status", ""),
        "formal_review_blockers": formal_review_blockers,
        "formal_pressure_check_source": formal_pressure_source,
        "formal_pressure_validation_status": formal_pressure_status,
        "formal_pressure_validation_reason": formal_pressure_reason,
        "formal_pressure_condition_warning_count": formal_pressure_condition_warning_count,
        "formal_window_report_warnings": formal_window_report_warnings,
        "formal_a_grade_count": review_row.get("a_grade_count", ""),
        "formal_rejected_count": review_row.get("rejected_count", ""),
        "evidence_reuse_class": evidence_reuse_class,
        "per_analyzer_reuse_boundary": (
            "reuse_or_reject_this_analyzer_device_id_only;do_not_block_other_analyzers_by_fleet_status"
        ),
        "physical_scope": "current_atmosphere_open_flow_component_fit",
        "not_pressure_compensation_fit": True,
    }

    verification_summary = {
        "component": component,
        "analyzer_prefix": prefix,
        "analyzer_device_id": device_id,
        "verification_status": verification_status,
        "verification_reasons": ";".join(verification_reasons),
        "verification_sample_count": len(verification_rows),
        "verification_point_count": len(verification_point_rows),
        "verification_rmse": verification_metrics.get("RMSE", ""),
        "verification_max_error": verification_metrics.get("MaxError", ""),
        "verification_error_limit": verification_error_limit,
        "verification_error_limit_source": verification_error_limit_source,
        "verification_certificate_uncertainties": verification_certificate_uncertainties,
        "candidate_status": candidate_status,
        "auto_write_allowed": False,
    }

    return {
        "policy_row": policy_row,
        "coefficient_rows": coefficient_rows,
        "fit_residuals": fit_residuals,
        "verification_summary": verification_summary,
        "verification_residuals": verification_residuals,
        "rejected_rows": [
            {
                "component": component,
                "analyzer_prefix": prefix,
                "analyzer_device_id": device_id,
                "sample_index": row.get("sample_index", ""),
                "candidate_role": row.get("_candidate_role", ""),
                "source_sample_role": row.get("_source_sample_role", ""),
                "reject_reasons": row.get("_candidate_reject_reasons", ""),
            }
            for row in preparation_rejected
        ],
    }


def build_candidate_coefficient_tables(
    *,
    run_dir: str | Path,
    plan: Mapping[str, Any],
    pressure_reference: Mapping[str, Any],
    component: str = "both",
    analyzer_prefix: str = "ga01",
    require_quick_check_artifact: bool = True,
    pressure_check_path: str | Path | None = None,
    cfg: Optional[CandidateCoefficientPolicyConfig] = None,
    today: Any = None,
) -> tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any]]:
    """Build V1.5 no-write candidate coefficient review tables."""

    config = cfg or CandidateCoefficientPolicyConfig()
    excluded_device_ids = {_normalized_device_id(item) for item in config.exclude_device_ids}
    package_tables, package_context = build_formal_calibration_package_tables(
        run_dir=run_dir,
        plan=plan,
        pressure_reference=pressure_reference,
        component=component,
        analyzer_prefix=analyzer_prefix,
        require_quick_check_artifact=require_quick_check_artifact,
        pressure_check_path=pressure_check_path,
        today=today,
    )
    review_rows = list(package_tables.get("candidate_coefficient_review") or [])
    review_by_key = {
        (
            str(row.get("component") or "").strip().lower(),
            str(row.get("analyzer_prefix") or "").strip().lower() or "ga01",
            _normalized_device_id(row.get("analyzer_device_id")),
        ): row
        for row in review_rows
    }
    a_rows = list(package_tables.get("a_grade_samples") or [])
    groups: Dict[Tuple[str, str, str], List[Mapping[str, Any]]] = {}
    for row in a_rows:
        key = _group_key(row)
        if not key[0]:
            continue
        if key[2] in excluded_device_ids:
            continue
        groups.setdefault(key, []).append(row)

    keys = sorted(key for key in (set(review_by_key) | set(groups)) if key[2] not in excluded_device_ids)
    common_mode_outlier_exclusions, common_mode_outlier_rows = _detect_common_mode_fit_target_outliers(
        keys=keys,
        groups=groups,
        review_by_key=review_by_key,
        plan=plan,
        cfg=config,
    )
    policy_rows: List[Dict[str, Any]] = []
    coefficient_rows: List[Dict[str, Any]] = []
    fit_residuals: List[Dict[str, Any]] = []
    verification_summary: List[Dict[str, Any]] = []
    verification_residuals: List[Dict[str, Any]] = []
    rejected_rows: List[Dict[str, Any]] = []

    for comp, prefix, device_id in keys:
        result = _candidate_for_group(
            component=comp,
            prefix=prefix,
            device_id=device_id,
            rows=groups.get((comp, prefix, device_id), []),
            plan_targets=_component_plan_targets(plan, comp),
            plan_target_uncertainties=_component_plan_target_uncertainties(plan, comp),
            review_row=review_by_key.get((comp, prefix, device_id), {}),
            cfg=config,
            common_mode_outlier_target_keys=common_mode_outlier_exclusions.get((comp, prefix, device_id), []),
        )
        policy_rows.append(result["policy_row"])
        coefficient_rows.extend(result["coefficient_rows"])
        fit_residuals.extend(result["fit_residuals"])
        verification_summary.append(result["verification_summary"])
        verification_residuals.extend(result["verification_residuals"])
        rejected_rows.extend(result["rejected_rows"])

    run_status = "blocked"
    if policy_rows and all(row["candidate_status"] == "verification_passed" for row in policy_rows):
        run_status = "verification_passed"
    elif policy_rows and any(row["allowed_to_fit"] for row in policy_rows):
        run_status = "fit_ready_requires_verification"

    run_summary = [
        {
            "run_dir": str(Path(run_dir).resolve()),
            "component": component,
            "analyzer_prefix": analyzer_prefix,
            "candidate_run_status": run_status,
            "policy_row_count": len(policy_rows),
            "coefficient_row_count": len(coefficient_rows),
            "fit_residual_count": len(fit_residuals),
            "verification_residual_count": len(verification_residuals),
            "common_mode_outlier_count": len(common_mode_outlier_rows),
            "package_status": package_context.get("package_status", ""),
            "pressure_check_source": package_context.get("pressure_check_source", ""),
            "auto_write_allowed": False,
            "controls_water_or_gas_routes": False,
            "opens_com_ports": False,
            "writes_coefficients": False,
            "physical_meaning": (
                "Candidate coefficients are limited to current-atmosphere open-flow A-grade samples. "
                "Pressure P is a validated input/QC condition and is explicitly excluded from the current-atmosphere "
                "CO2/H2O fit. Pressure compensation is handled by the independent pressure-channel workflow."
                " Multi-device same-sign target outliers are treated as suspect source/route evidence and are "
                "not allowed to bend the analyzer response curve. When an existing secondary temperature "
                "compensation channel is preserved, the primary ratio fit target is the certificate value minus "
                "that preserved secondary contribution. For CO2, the certificate target is first back-calculated "
                "through the firmware H2O dry-basis output layer so the simulated firmware display, not just the "
                "raw optical layer, is aligned with the standard gas."
            ),
            "excluded_device_ids": ";".join(sorted(excluded_device_ids)),
        }
    ]
    tables = {
        "candidate_run_summary": run_summary,
        "candidate_policy_summary": policy_rows,
        "candidate_coefficients": coefficient_rows,
        "candidate_fit_residuals": fit_residuals,
        "candidate_verification_summary": verification_summary,
        "candidate_verification_residuals": verification_residuals,
        "candidate_preparation_rejected_samples": rejected_rows,
        "candidate_common_mode_target_outliers": common_mode_outlier_rows,
    }
    context = {
        "candidate_run_status": run_status,
        "package_status": package_context.get("package_status", ""),
        "pressure_check_source": package_context.get("pressure_check_source", ""),
        "analyzer_prefixes": package_context.get("analyzer_prefixes", []),
    }
    return tables, context


def _write_markdown_report(destination: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> Path:
    summary = (tables.get("candidate_run_summary") or [{}])[0]
    policies = list(tables.get("candidate_policy_summary") or [])
    common_outliers = list(tables.get("candidate_common_mode_target_outliers") or [])
    report_path = destination / "candidate_coefficients_report.md"
    lines = [
        "# V1.5 Candidate Coefficient Review",
        "",
        f"- Status: {summary.get('candidate_run_status', '')}",
        f"- Package status: {summary.get('package_status', '')}",
        f"- Pressure check source: {summary.get('pressure_check_source', '')}",
        "- Boundary: offline/no-write candidate review only; no COM ports, no PACE/valves, no water/gas route control, no SENCO write.",
        "",
        "## Policy Summary",
        "",
        "| Component | Analyzer | Device ID | Evidence Class | Status | Fit Samples | A-grade | Verification | Verification Limit | Terms | Frozen Terms | Blockers |",
        "| --- | --- | --- | --- | --- | ---: | ---: | --- | ---: | --- | --- | --- |",
    ]
    for row in policies:
        lines.append(
            "| {component} | {prefix} | {device} | {reuse_class} | {status} | {fit_count} | {a_grade} | {verification} | {verification_limit} | {terms} | {frozen} | {blockers} |".format(
                component=row.get("component", ""),
                prefix=row.get("analyzer_prefix", ""),
                device=row.get("analyzer_device_id", ""),
                reuse_class=row.get("evidence_reuse_class", ""),
                status=row.get("candidate_status", ""),
                fit_count=row.get("fit_sample_count", ""),
                a_grade=row.get("formal_a_grade_count", ""),
                verification=row.get("verification_status", ""),
                verification_limit=row.get("verification_error_limit", ""),
                terms=row.get("selected_model_terms", ""),
                frozen=row.get("frozen_terms", ""),
                blockers=";".join(
                    item
                    for item in (
                        str(row.get("blocked_reasons") or ""),
                        str(row.get("formal_review_blockers") or ""),
                    )
                    if item
                ),
            )
        )
    if common_outliers:
        lines.extend(
            [
                "",
                "## Common-Mode Target Outliers",
                "",
                "| Component | Target | Direction | Devices | Interpretation |",
                "| --- | ---: | --- | --- | --- |",
            ]
        )
        for row in common_outliers:
            if row.get("analyzer_device_id"):
                continue
            lines.append(
                "| {component} | {target} | {direction} | {devices} | {interpretation} |".format(
                    component=row.get("component", ""),
                    target=row.get("target_value_mean", row.get("target_key", "")),
                    direction=row.get("direction", ""),
                    devices=row.get("devices", ""),
                    interpretation=row.get("physical_interpretation", row.get("reason", "")),
                )
            )
    lines.extend(
        [
            "",
            "## Physical Meaning",
            "",
            "- Current-atmosphere open-flow samples must not fit pressure compensation coefficients; P/RP/RTP terms are excluded from this candidate contract.",
            "- Single-temperature samples cannot identify temperature compensation coefficients; T/T2/RT terms are frozen by default.",
            "- A single A-grade target can be reused as per-analyzer review or verification evidence, but it is not a complete formal calibration curve.",
            "- Analyzer failures are scoped to the analyzer device ID that produced the bad evidence; they do not invalidate other analyzer device IDs in the same run.",
            "- Multi-device same-sign target outliers are source/route evidence first; they must not force the SENCO1/3 response curve to bend around a suspect gas point.",
            "- Verification samples must be A-grade open-flow samples that were not used in fitting.",
            "- Verification error limits use the fixed policy floor or the matched certificate expanded uncertainty, whichever is larger.",
            "- This report does not authorize automatic coefficient writes.",
            "",
        ]
    )
    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path


def write_candidate_coefficient_report(
    *,
    run_dir: str | Path,
    output_dir: str | Path,
    plan: Optional[Mapping[str, Any]] = None,
    plan_path: str | Path | None = None,
    pressure_reference: Optional[Mapping[str, Any]] = None,
    pressure_reference_path: str | Path | None = None,
    pressure_check_path: str | Path | None = None,
    component: str = "both",
    analyzer_prefix: str = "ga01",
    require_quick_check_artifact: bool = True,
    cfg: Optional[CandidateCoefficientPolicyConfig] = None,
    today: Any = None,
) -> Dict[str, Path]:
    """Write no-write candidate coefficient CSV/XLSX/Markdown artifacts."""

    plan_data = dict(plan) if plan is not None else load_plan_snapshot(plan_path)
    reference_data = (
        dict(pressure_reference)
        if pressure_reference is not None
        else load_pressure_reference_snapshot(pressure_reference_path)
    )
    tables, context = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=plan_data,
        pressure_reference=reference_data,
        component=component,
        analyzer_prefix=analyzer_prefix,
        require_quick_check_artifact=require_quick_check_artifact,
        pressure_check_path=pressure_check_path,
        cfg=cfg,
        today=today,
    )
    destination = Path(output_dir).resolve()
    metadata = ValidationMetadata(
        tool_name="export_v1_5_candidate_coefficients",
        created_at=_now(),
        analyzers=list(context.get("analyzer_prefixes") or [analyzer_prefix]),
        input_paths=[
            str(Path(run_dir).resolve()),
            str(Path(plan_path).resolve()) if plan_path else "",
            str(Path(pressure_reference_path).resolve()) if pressure_reference_path else "",
            str(Path(pressure_check_path).resolve()) if pressure_check_path else "",
        ],
        output_dir=str(destination),
        config_summary={
            "component": component,
            "analyzer_prefix": analyzer_prefix,
            "candidate_run_status": context.get("candidate_run_status", ""),
            "package_status": context.get("package_status", ""),
            "pressure_check_source": context.get("pressure_check_source", ""),
            "auto_write_allowed": False,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "fit_all_eligible_samples": bool((cfg or CandidateCoefficientPolicyConfig()).fit_all_eligible_samples),
        },
        notes=[
            "Offline V1.5 candidate coefficient review.",
            "Pressure terms are excluded for the current-atmosphere open-flow contract. Temperature terms are included only when the data span makes them identifiable and the policy explicitly enables them.",
            "No coefficient write is performed or authorized by this export.",
        ],
    )
    outputs = write_validation_report(
        destination,
        prefix="candidate_coefficients",
        metadata=metadata,
        tables=tables,
    )
    outputs["markdown"] = _write_markdown_report(destination, tables)
    return outputs
