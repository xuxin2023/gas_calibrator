"""Offline CO2 fitting-algorithm matrix for V1.5 SENCO review.

This module evaluates fitting contracts from already-recorded open-flow
evidence. It never opens COM ports, controls gas/water routes, or writes
coefficients.
"""

from __future__ import annotations

import csv
import json
import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import numpy as np

from ..senco_format import format_senco_values


CORE_TERMS = ("intercept", "R", "R2", "R3")
TEMP_TERMS = ("T", "T2", "RT")
PRESSURE_TERMS = ("P", "RTP")
CENTERABLE_TERMS = CORE_TERMS + TEMP_TERMS


@dataclass(frozen=True)
class FitPoint:
    device_id: str
    analyzer_prefix: str
    point_identity: str
    source_role: str
    target_ppm: float
    zero_anchor_class: str
    target_uncertainty_ppm: Optional[float]
    ratio: float
    temperature_c: float
    pressure_hpa: float
    h2o_mmol: Optional[float] = None
    treatment_fit_policy: str = ""
    treatment_bridge_policy: str = ""
    treatment_review_priority: str = ""
    treatment_exclusion_basis: str = ""


@dataclass(frozen=True)
class ModelVariant:
    model_id: str
    terms: tuple[str, ...]
    pressure_unit: str = "kpa"
    preserve_existing_pressure_slots: bool = False
    use_celsius_temperature: bool = False
    apply_h2o_dry_basis_target_bridge: bool = False
    write_contract: str = "review_only"


MODEL_VARIANTS: tuple[ModelVariant, ...] = (
    ModelVariant(
        model_id="senco1_ratio_only",
        terms=CORE_TERMS,
        write_contract="not_recommended_for_multitemperature_formal_co2",
    ),
    ModelVariant(
        model_id="senco13_temperature_terms_pressure_zero",
        terms=CORE_TERMS + TEMP_TERMS,
        write_contract="preferred_current_atmosphere_no_pressure_candidate",
    ),
    ModelVariant(
        model_id="senco13_temperature_terms_pressure_zero_h2o_bridge",
        terms=CORE_TERMS + TEMP_TERMS,
        apply_h2o_dry_basis_target_bridge=True,
        write_contract="preferred_current_atmosphere_h2o_dry_bridge_no_pressure_candidate",
    ),
    ModelVariant(
        model_id="senco13_temperature_terms_preserve_existing_pressure_slots",
        terms=CORE_TERMS + TEMP_TERMS,
        preserve_existing_pressure_slots=True,
        write_contract="diagnostic_only_old_pressure_tail_preservation",
    ),
    ModelVariant(
        model_id="legacy_v1_a0_a8_full_rt_p_kpa",
        terms=CORE_TERMS + TEMP_TERMS + PRESSURE_TERMS,
        pressure_unit="kpa",
        write_contract="blocked_pressure_span_insufficient",
    ),
    ModelVariant(
        model_id="legacy_v1_a0_a8_full_rt_p_hpa_unit_diagnostic",
        terms=CORE_TERMS + TEMP_TERMS + PRESSURE_TERMS,
        pressure_unit="hpa",
        write_contract="unit_diagnostic_only_not_write_contract",
    ),
)

TREATMENT_INCLUDE_POLICIES = {
    "include_as_standard_s1s3_fit_point",
    "include_after_target_route_model_review",
    "include_as_zero_anchor_with_uncertainty",
}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_csv(path: str | Path) -> List[Dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return []
    with target.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    headers: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in headers:
                headers.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows([dict(row) for row in rows])


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    return numeric if math.isfinite(numeric) else None


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "pass", "certified"}


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _infer_source_role(row: Mapping[str, Any]) -> str:
    text = " ".join(
        str(row.get(key) or "")
        for key in ("source_role", "sample_role", "residual_role", "sample_index", "point_identity")
    ).lower()
    if "verification" in text or "verify" in text:
        return "verification"
    if "diagnostic" in text:
        return "diagnostic"
    return "fit"


def _load_treatment_plan(path: str | Path | None) -> Dict[str, Dict[str, Any]]:
    if not path:
        return {}
    rows = _read_csv(path)
    plan: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        point_identity = str(row.get("point_identity") or "").strip()
        if point_identity:
            plan[point_identity] = row
    return plan


def _role_from_treatment(original_role: str, treatment: Mapping[str, Any]) -> str:
    policy = str(treatment.get("fit_policy") or "").strip()
    if not policy:
        return original_role
    if policy in TREATMENT_INCLUDE_POLICIES:
        return "fit"
    if policy.startswith("hold_"):
        return "diagnostic"
    return original_role


def _zero_anchor_class(row: Mapping[str, Any], target_ppm: float) -> str:
    explicit = str(
        row.get("zero_anchor_class")
        or row.get("co2_zero_anchor_class")
        or row.get("zero_anchor_policy")
        or ""
    ).strip().lower()
    if explicit:
        return explicit
    if abs(float(target_ppm)) > 1.0e-9:
        return "standard_fit_point"
    certified = any(
        _truthy(row.get(key))
        for key in (
            "co2_zero_anchor_certified",
            "zero_anchor_certified",
            "certificate_trace_valid",
            "standard_gas_certificate_valid",
        )
    )
    certificate_hash = str(row.get("certificate_hash") or row.get("standard_gas_certificate_hash") or "").strip()
    if certified or certificate_hash:
        return "certified_zero_anchor"
    return "estimated_zero_anchor"


def _is_zero_or_low_anchor(point: FitPoint) -> bool:
    marker = str(point.zero_anchor_class or "").lower()
    return (
        abs(float(point.target_ppm)) <= 1.0e-9
        or "zero" in marker
        or "low_co2_anchor" in marker
    )


def _load_fit_points(
    path: str | Path,
    *,
    exclude_device_ids: Iterable[str] = (),
    treatment_plan_csv: str | Path | None = None,
) -> List[FitPoint]:
    excluded = {_device_id(item) for item in exclude_device_ids if str(item or "").strip()}
    treatment_plan = _load_treatment_plan(treatment_plan_csv)
    points: List[FitPoint] = []
    for row in _read_csv(path):
        if str(row.get("component") or "co2").strip().lower() != "co2":
            continue
        device = _device_id(row.get("analyzer_device_id") or row.get("device_id"))
        if not device or device in excluded:
            continue
        ratio = _safe_float(row.get("ratio") or row.get("co2_ratio_f_mean") or row.get("R_CO2"))
        target = _safe_float(row.get("target_value") or row.get("certificate_co2_ppm") or row.get("ppm_CO2_Tank"))
        temp = _safe_float(row.get("temperature_c") or row.get("chamber_temp_mean_c") or row.get("T1"))
        pressure = _safe_float(row.get("pressure_hpa") or row.get("pressure_gauge_hpa"))
        if pressure is None:
            kpa = _safe_float(row.get("pressure_kpa") or row.get("BAR"))
            pressure = kpa * 10.0 if kpa is not None else None
        h2o_mmol = _safe_float(
            row.get("h2o_mmol_mean")
            or row.get("h2o_mmol")
            or row.get("h2o_mmol_mol")
            or row.get("water_mmol_mol")
            or row.get("h2o_mmol_per_mol")
        )
        if ratio is None or target is None or temp is None or pressure is None:
            continue
        point_identity = str(row.get("point_identity") or row.get("sample_index") or "").strip()
        treatment = treatment_plan.get(point_identity, {})
        source_role = _role_from_treatment(_infer_source_role(row), treatment)
        target_uncertainty = _safe_float(row.get("target_uncertainty_ppm") or row.get("co2_uncertainty_ppm"))
        points.append(
            FitPoint(
                device_id=device,
                analyzer_prefix=str(row.get("analyzer_prefix") or "").strip(),
                point_identity=point_identity,
                source_role=source_role,
                target_ppm=float(target),
                zero_anchor_class=_zero_anchor_class(row, float(target)),
                target_uncertainty_ppm=target_uncertainty,
                ratio=float(ratio),
                temperature_c=float(temp),
                pressure_hpa=float(pressure),
                h2o_mmol=h2o_mmol,
                treatment_fit_policy=str(treatment.get("fit_policy") or "").strip(),
                treatment_bridge_policy=str(treatment.get("bridge_policy") or "").strip(),
                treatment_review_priority=str(treatment.get("review_priority") or "").strip(),
                treatment_exclusion_basis=str(treatment.get("exclusion_basis") or "").strip(),
            )
        )
    return points


def _load_snapshot(path: str | Path | None) -> Dict[str, Any]:
    if not path:
        return {}
    target = Path(path)
    if not target.exists():
        return {}
    return json.loads(target.read_text(encoding="utf-8-sig"))


def _snapshot_values(snapshot: Mapping[str, Any], device_id: str, key: str) -> List[float]:
    device = snapshot.get(_device_id(device_id))
    if not isinstance(device, Mapping):
        return []
    value = device.get(key) or device.get(key.replace("_live", "_review"))
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    out: List[float] = []
    for item in value:
        numeric = _safe_float(item)
        if numeric is not None:
            out.append(float(numeric))
    return out


def _temperature_value(point: FitPoint, variant: ModelVariant) -> float:
    return float(point.temperature_c) if variant.use_celsius_temperature else float(point.temperature_c) + 273.15


def _pressure_value(point: FitPoint, variant: ModelVariant) -> float:
    if variant.pressure_unit == "hpa":
        return float(point.pressure_hpa)
    return float(point.pressure_hpa) / 10.0


def _feature_value(term: str, point: FitPoint, variant: ModelVariant) -> float:
    r = float(point.ratio)
    t = _temperature_value(point, variant)
    p = _pressure_value(point, variant)
    if term == "intercept":
        return 1.0
    if term == "R":
        return r
    if term == "R2":
        return r * r
    if term == "R3":
        return r**3
    if term == "T":
        return t
    if term == "T2":
        return t * t
    if term == "RT":
        return r * t
    if term == "P":
        return p
    if term == "RTP":
        return r * t * p
    raise ValueError(f"Unsupported CO2 fit term: {term}")


def _matrix(points: Sequence[FitPoint], variant: ModelVariant) -> np.ndarray:
    return np.asarray(
        [[_feature_value(term, point, variant) for term in variant.terms] for point in points],
        dtype=float,
    )


def _centered_feature_value(
    term: str,
    point: FitPoint,
    variant: ModelVariant,
    *,
    ratio_center: float,
    temp_center: float,
) -> float:
    ratio_delta = float(point.ratio) - float(ratio_center)
    temp_delta = _temperature_value(point, variant) - float(temp_center)
    if term == "intercept":
        return 1.0
    if term == "R":
        return ratio_delta
    if term == "R2":
        return ratio_delta * ratio_delta
    if term == "R3":
        return ratio_delta**3
    if term == "T":
        return temp_delta
    if term == "T2":
        return temp_delta * temp_delta
    if term == "RT":
        return ratio_delta * temp_delta
    raise ValueError(f"Unsupported centered CO2 fit term: {term}")


def _centered_matrix(
    points: Sequence[FitPoint],
    variant: ModelVariant,
    *,
    ratio_center: float,
    temp_center: float,
) -> np.ndarray:
    return np.asarray(
        [
            [
                _centered_feature_value(
                    term,
                    point,
                    variant,
                    ratio_center=ratio_center,
                    temp_center=temp_center,
                )
                for term in variant.terms
            ]
            for point in points
        ],
        dtype=float,
    )


def _centered_to_absolute(
    terms: Sequence[str],
    centered_coefficients: Sequence[float],
    *,
    ratio_center: float,
    temp_center: float,
) -> np.ndarray:
    values = {term: 0.0 for term in CENTERABLE_TERMS}
    for term, coefficient in zip(terms, centered_coefficients):
        values[term] = float(coefficient)

    r0 = float(ratio_center)
    t0 = float(temp_center)
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


def _preserved_pressure_offset(point: FitPoint, variant: ModelVariant, old_secondary: Sequence[float]) -> float:
    if not variant.preserve_existing_pressure_slots:
        return 0.0
    pressure_coeff = float(old_secondary[3]) if len(old_secondary) > 3 else 0.0
    rtp_coeff = float(old_secondary[4]) if len(old_secondary) > 4 else 0.0
    p = _pressure_value(point, variant)
    t = _temperature_value(point, variant)
    return pressure_coeff * p + rtp_coeff * float(point.ratio) * t * p


def _h2o_dry_basis_factor(point: FitPoint, variant: ModelVariant) -> float:
    """Return the firmware dry-basis bridge factor for displayed CO2.

    Firmware final CO2 display is modeled as raw SENCO1/3 CO2 divided by
    (1 - H2O_mmol_mol / 1000). Therefore the raw SENCO1/3 fitting target is
    the certificate/display target multiplied by the same factor.
    """

    if not variant.apply_h2o_dry_basis_target_bridge:
        return 1.0
    if point.treatment_bridge_policy == "disable_h2o_bridge_for_s1s3":
        return 1.0
    if point.h2o_mmol is None:
        return 1.0
    h2o = float(point.h2o_mmol)
    if not math.isfinite(h2o) or h2o < 0.0:
        return 1.0
    factor = 1.0 - h2o / 1000.0
    if factor <= 0.05 or factor > 1.0:
        return 1.0
    return factor


def _raw_senco13_fit_target(point: FitPoint, variant: ModelVariant, old_secondary: Sequence[float]) -> float:
    return (
        float(point.target_ppm) * _h2o_dry_basis_factor(point, variant)
        - _preserved_pressure_offset(point, variant, old_secondary)
    )


def _display_prediction_from_raw(point: FitPoint, variant: ModelVariant, raw_prediction: float) -> float:
    factor = _h2o_dry_basis_factor(point, variant)
    if variant.apply_h2o_dry_basis_target_bridge and factor > 0.0:
        return float(raw_prediction) / factor
    return float(raw_prediction)


def _scaled_lstsq(matrix: np.ndarray, target: np.ndarray) -> tuple[np.ndarray, int, float]:
    scales = np.linalg.norm(matrix, axis=0)
    scales = np.where(np.isfinite(scales) & (scales > 0.0), scales, 1.0)
    scaled = matrix / scales
    rank = int(np.linalg.matrix_rank(scaled))
    condition = float(np.linalg.cond(scaled))
    fitted, *_ = np.linalg.lstsq(scaled, target, rcond=None)
    return np.asarray(fitted, dtype=float) / scales, rank, condition


def _fit_coefficients(
    fit_points: Sequence[FitPoint],
    variant: ModelVariant,
    target: np.ndarray,
) -> tuple[np.ndarray, int, float, float, str]:
    absolute = _matrix(fit_points, variant)
    absolute_coefficients, absolute_rank, absolute_condition = _scaled_lstsq(absolute, target)
    if not set(variant.terms).issubset(CENTERABLE_TERMS):
        return absolute_coefficients, absolute_rank, absolute_condition, absolute_condition, "absolute_firmware_terms"

    ratio_center = float(np.mean([point.ratio for point in fit_points])) if fit_points else 0.0
    temp_center = float(np.mean([_temperature_value(point, variant) for point in fit_points])) if fit_points else 273.15
    centered = _centered_matrix(
        fit_points,
        variant,
        ratio_center=ratio_center,
        temp_center=temp_center,
    )
    centered_coefficients, centered_rank, centered_condition = _scaled_lstsq(centered, target)
    transformed = _centered_to_absolute(
        variant.terms,
        centered_coefficients,
        ratio_center=ratio_center,
        temp_center=temp_center,
    )
    return transformed, centered_rank, centered_condition, absolute_condition, "centered_R_T_transformed_to_firmware_absolute_terms"


def _relative_error_percent(error: float, target: float) -> Optional[float]:
    if abs(float(target)) <= 1.0e-9:
        return None
    return 100.0 * float(error) / float(target)


def _metrics(
    errors: Sequence[float],
    relative_errors_percent: Sequence[float | None] = (),
) -> Dict[str, Any]:
    if not errors:
        return {
            "n": 0,
            "rmse": "",
            "max_abs_error": "",
            "mean_error": "",
            "max_abs_relative_error_percent": "",
            "mean_abs_relative_error_percent": "",
        }
    values = np.asarray(errors, dtype=float)
    relative_values = np.asarray(
        [
            float(item)
            for item in relative_errors_percent
            if item is not None and math.isfinite(float(item))
        ],
        dtype=float,
    )
    return {
        "n": int(values.size),
        "rmse": float(np.sqrt(np.mean(values**2))),
        "max_abs_error": float(np.max(np.abs(values))),
        "mean_error": float(np.mean(values)),
        "max_abs_relative_error_percent": (
            float(np.max(np.abs(relative_values))) if relative_values.size else ""
        ),
        "mean_abs_relative_error_percent": (
            float(np.mean(np.abs(relative_values))) if relative_values.size else ""
        ),
    }


def _payloads(
    coeffs: Mapping[str, float],
    *,
    old_primary: Sequence[float],
    old_secondary: Sequence[float],
    variant: ModelVariant,
) -> tuple[List[float], List[float]]:
    primary_len = max(4, len(old_primary) or 6)
    secondary_len = max(4, len(old_secondary) or 6)
    primary = [
        float(coeffs.get("intercept", 0.0)),
        float(coeffs.get("R", 0.0)),
        float(coeffs.get("R2", 0.0)),
        float(coeffs.get("R3", 0.0)),
    ]
    secondary = [
        float(coeffs.get("T", 0.0)),
        float(coeffs.get("T2", 0.0)),
        float(coeffs.get("RT", 0.0)),
        float(coeffs.get("P", old_secondary[3] if len(old_secondary) > 3 and variant.preserve_existing_pressure_slots else 0.0)),
        float(coeffs.get("RTP", old_secondary[4] if len(old_secondary) > 4 and variant.preserve_existing_pressure_slots else 0.0)),
        float(old_secondary[5] if len(old_secondary) > 5 and variant.preserve_existing_pressure_slots else 0.0),
    ]
    while len(primary) < primary_len:
        primary.append(float(old_primary[len(primary)]) if len(old_primary) > len(primary) else 0.0)
    return primary[:primary_len], secondary[:secondary_len]


def _fit_one(
    device_points: Sequence[FitPoint],
    *,
    variant: ModelVariant,
    old_secondary: Sequence[float],
) -> tuple[Dict[str, float], int, float, float, str, List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    fit_points = [point for point in device_points if point.source_role == "fit"]
    verification_points = [point for point in device_points if point.source_role == "verification"]
    if not fit_points:
        return {}, 0, float("inf"), float("inf"), "", [], _metrics([]), _metrics([])
    target = np.asarray(
        [_raw_senco13_fit_target(point, variant, old_secondary) for point in fit_points],
        dtype=float,
    )
    coeff_array, rank, condition, absolute_condition, fit_basis = _fit_coefficients(fit_points, variant, target)
    coeffs = {term: float(value) for term, value in zip(variant.terms, coeff_array)}

    prediction_rows: List[Dict[str, Any]] = []
    errors_by_role: Dict[str, List[float]] = {"fit": [], "verification": []}
    relative_errors_by_role: Dict[str, List[float | None]] = {"fit": [], "verification": []}
    for point in list(fit_points) + list(verification_points):
        raw_pred = float(_matrix([point], variant)[0] @ coeff_array)
        raw_pred += _preserved_pressure_offset(point, variant, old_secondary)
        factor = _h2o_dry_basis_factor(point, variant)
        display_pred = _display_prediction_from_raw(point, variant, raw_pred)
        raw_target = float(point.target_ppm) * factor
        error = display_pred - float(point.target_ppm)
        relative_error = _relative_error_percent(error, float(point.target_ppm))
        raw_error = raw_pred - raw_target
        errors_by_role.setdefault(point.source_role, []).append(error)
        relative_errors_by_role.setdefault(point.source_role, []).append(relative_error)
        prediction_rows.append(
            {
                "device_id": point.device_id,
                "analyzer_prefix": point.analyzer_prefix,
                "model_id": variant.model_id,
                "source_role": point.source_role,
                "point_identity": point.point_identity,
                "target_ppm": point.target_ppm,
                "zero_anchor_class": point.zero_anchor_class,
                "target_uncertainty_ppm": point.target_uncertainty_ppm if point.target_uncertainty_ppm is not None else "",
                "prediction_ppm": display_pred,
                "error_ppm": error,
                "relative_error_percent": relative_error if relative_error is not None else "",
                "raw_senco13_target_ppm": raw_target,
                "raw_senco13_prediction_ppm": raw_pred,
                "raw_senco13_error_ppm": raw_error,
                "ratio": point.ratio,
                "temperature_c": point.temperature_c,
                "pressure_hpa": point.pressure_hpa,
                "h2o_mmol": point.h2o_mmol if point.h2o_mmol is not None else "",
                "h2o_dry_basis_factor": factor,
                "h2o_dry_basis_bridge_applied": variant.apply_h2o_dry_basis_target_bridge,
                "h2o_bridge_disabled_by_treatment": point.treatment_bridge_policy == "disable_h2o_bridge_for_s1s3",
                "treatment_fit_policy": point.treatment_fit_policy,
                "treatment_bridge_policy": point.treatment_bridge_policy,
                "treatment_review_priority": point.treatment_review_priority,
                "treatment_exclusion_basis": point.treatment_exclusion_basis,
                "pressure_unit": variant.pressure_unit,
                "preserves_existing_pressure_slots": variant.preserve_existing_pressure_slots,
            }
        )
    return (
        coeffs,
        rank,
        condition,
        absolute_condition,
        fit_basis,
        prediction_rows,
        _metrics(errors_by_role["fit"], relative_errors_by_role["fit"]),
        _metrics(errors_by_role["verification"], relative_errors_by_role["verification"]),
    )


def _pressure_span_hpa(points: Sequence[FitPoint]) -> float:
    values = [point.pressure_hpa for point in points]
    return float(max(values) - min(values)) if values else 0.0


def _temperature_span_c(points: Sequence[FitPoint]) -> float:
    values = [point.temperature_c for point in points]
    return float(max(values) - min(values)) if values else 0.0


def _distinct_targets(points: Sequence[FitPoint]) -> int:
    return len({round(point.target_ppm, 6) for point in points})


def _zero_anchor_summary(points: Sequence[FitPoint]) -> Dict[str, Any]:
    anchors = [point for point in points if point.source_role == "fit" and _is_zero_or_low_anchor(point)]
    estimated = [point for point in anchors if "estimated" in point.zero_anchor_class]
    certified = [point for point in anchors if "certified" in point.zero_anchor_class]
    uncertainties = [
        float(point.target_uncertainty_ppm)
        for point in anchors
        if point.target_uncertainty_ppm is not None and math.isfinite(float(point.target_uncertainty_ppm))
    ]
    if estimated:
        status = "estimated_zero_anchor_sensitivity_only_not_formal_release"
    elif certified and len(certified) == len(anchors):
        status = "certified_zero_anchor_fit_eligible"
    elif anchors:
        status = "zero_anchor_present_needs_traceability_review"
    else:
        status = "zero_anchor_absent"
    return {
        "zero_anchor_count": len(anchors),
        "estimated_zero_anchor_count": len(estimated),
        "certified_zero_anchor_count": len(certified),
        "zero_anchor_policy_status": status,
        "zero_anchor_target_uncertainty_ppm_max": max(uncertainties) if uncertainties else "",
    }


def _identifiability_status(variant: ModelVariant, *, condition_number: float, pressure_span_hpa: float) -> str:
    if any(term in variant.terms for term in PRESSURE_TERMS) and pressure_span_hpa < 50.0:
        return "blocked_pressure_span_insufficient_for_pressure_terms"
    if not math.isfinite(condition_number) or condition_number > 1.0e8:
        return "review_matrix_ill_conditioned"
    if variant.write_contract.startswith("blocked"):
        return variant.write_contract
    if variant.write_contract.startswith("unit_diagnostic"):
        return variant.write_contract
    return "reviewable_no_write"


def build_co2_fit_algorithm_matrix_tables(
    *,
    fit_residuals_csv: str | Path,
    old_snapshot_json: str | Path | None = None,
    exclude_device_ids: Sequence[str] = (),
    fit_point_treatment_plan_csv: str | Path | None = None,
) -> Dict[str, List[Dict[str, Any]]]:
    points = _load_fit_points(
        fit_residuals_csv,
        exclude_device_ids=exclude_device_ids,
        treatment_plan_csv=fit_point_treatment_plan_csv,
    )
    snapshot = _load_snapshot(old_snapshot_json)
    by_device: Dict[str, List[FitPoint]] = {}
    for point in points:
        by_device.setdefault(point.device_id, []).append(point)
    explicit_zero_target_present = any(_is_zero_or_low_anchor(point) for point in points)
    estimated_zero_target_present = any(
        _is_zero_or_low_anchor(point) and "estimated" in point.zero_anchor_class
        for point in points
    )
    certified_zero_target_present = any(
        _is_zero_or_low_anchor(point) and "certified" in point.zero_anchor_class
        for point in points
    )

    summary_rows: List[Dict[str, Any]] = []
    coefficient_rows: List[Dict[str, Any]] = []
    prediction_rows: List[Dict[str, Any]] = []
    recommended_by_device: Dict[str, Mapping[str, Any]] = {}
    candidate_model_ids = {
        "senco13_temperature_terms_pressure_zero",
        "senco13_temperature_terms_pressure_zero_h2o_bridge",
    }

    for device_id in sorted(by_device):
        device_points = by_device[device_id]
        old_primary = _snapshot_values(snapshot, device_id, "GETCO1_before_live")
        old_secondary = _snapshot_values(snapshot, device_id, "GETCO3_before_live")
        temp_span = _temperature_span_c(device_points)
        pressure_span = _pressure_span_hpa(device_points)
        target_count = _distinct_targets(device_points)
        zero_anchor = _zero_anchor_summary(device_points)
        treatment_counts = Counter(
            point.treatment_fit_policy or "no_treatment_plan"
            for point in device_points
        )
        bridge_disabled_count = sum(
            1
            for point in device_points
            if point.treatment_bridge_policy == "disable_h2o_bridge_for_s1s3"
        )
        held_by_treatment_count = sum(1 for point in device_points if point.source_role == "diagnostic")
        candidate_rows: List[Mapping[str, Any]] = []
        for variant in MODEL_VARIANTS:
            (
                coeffs,
                rank,
                condition,
                absolute_condition,
                fit_basis,
                predictions,
                fit_metrics,
                verification_metrics,
            ) = _fit_one(
                device_points,
                variant=variant,
                old_secondary=old_secondary,
            )
            primary_payload, secondary_payload = _payloads(
                coeffs,
                old_primary=old_primary,
                old_secondary=old_secondary,
                variant=variant,
            )
            status = _identifiability_status(
                variant,
                condition_number=condition,
                pressure_span_hpa=pressure_span,
            )
            row = {
                "device_id": device_id,
                "analyzer_prefix": device_points[0].analyzer_prefix,
                "model_id": variant.model_id,
                "terms": ";".join(variant.terms),
                "write_contract": variant.write_contract,
                "recommendation_status": status,
                "fit_point_count": fit_metrics["n"],
                "verification_point_count": verification_metrics["n"],
                "fit_rmse_ppm": fit_metrics["rmse"],
                "fit_max_abs_error_ppm": fit_metrics["max_abs_error"],
                "fit_max_abs_relative_error_percent": fit_metrics["max_abs_relative_error_percent"],
                "fit_mean_abs_relative_error_percent": fit_metrics["mean_abs_relative_error_percent"],
                "verification_rmse_ppm": verification_metrics["rmse"],
                "verification_max_abs_error_ppm": verification_metrics["max_abs_error"],
                "verification_max_abs_relative_error_percent": verification_metrics["max_abs_relative_error_percent"],
                "verification_mean_abs_relative_error_percent": verification_metrics["mean_abs_relative_error_percent"],
                "fit_mean_error_ppm": fit_metrics["mean_error"],
                "verification_mean_error_ppm": verification_metrics["mean_error"],
                "matrix_rank": rank,
                "term_count": len(variant.terms),
                "condition_number_scaled": condition,
                "absolute_condition_number_scaled": absolute_condition,
                "fit_basis": fit_basis,
                "temperature_span_c": temp_span,
                "pressure_span_hpa": pressure_span,
                "distinct_target_count": target_count,
                "pressure_unit": variant.pressure_unit,
                "preserves_existing_pressure_slots": variant.preserve_existing_pressure_slots,
                "h2o_dry_basis_target_bridge": variant.apply_h2o_dry_basis_target_bridge,
                "fit_point_treatment_plan_applied": bool(fit_point_treatment_plan_csv),
                "treatment_fit_policy_counts": json.dumps(dict(treatment_counts), ensure_ascii=False, sort_keys=True),
                "h2o_bridge_disabled_by_treatment_count": bridge_disabled_count,
                "held_by_treatment_count": held_by_treatment_count,
                "zero_anchor_count": zero_anchor["zero_anchor_count"],
                "estimated_zero_anchor_count": zero_anchor["estimated_zero_anchor_count"],
                "certified_zero_anchor_count": zero_anchor["certified_zero_anchor_count"],
                "zero_anchor_policy_status": zero_anchor["zero_anchor_policy_status"],
                "zero_anchor_target_uncertainty_ppm_max": zero_anchor["zero_anchor_target_uncertainty_ppm_max"],
                "old_secondary_pressure_slots": json.dumps(
                    {
                        "C3_P": old_secondary[3] if len(old_secondary) > 3 else 0.0,
                        "C4_RTP": old_secondary[4] if len(old_secondary) > 4 else 0.0,
                        "C5_extra": old_secondary[5] if len(old_secondary) > 5 else 0.0,
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                    ),
                    "primary_payload": json.dumps(primary_payload, ensure_ascii=False, separators=(",", ":")),
                    "secondary_payload": json.dumps(secondary_payload, ensure_ascii=False, separators=(",", ":")),
                    "primary_payload_scientific": ",".join(format_senco_values(primary_payload)),
                    "secondary_payload_scientific": ",".join(format_senco_values(secondary_payload)),
                    "physical_meaning": _physical_meaning(variant, status),
                }
            summary_rows.append(row)
            prediction_rows.extend(predictions)
            for term in variant.terms:
                coefficient_rows.append(
                    {
                        "device_id": device_id,
                        "model_id": variant.model_id,
                        "term": term,
                        "coefficient": coeffs.get(term, ""),
                    }
                )
            if variant.model_id in candidate_model_ids:
                candidate_rows.append(row)
        reviewable_candidates = [
            row
            for row in candidate_rows
            if str(row.get("recommendation_status") or "") == "reviewable_no_write"
        ]
        if reviewable_candidates:
            recommended_by_device[device_id] = min(
                reviewable_candidates,
                key=lambda row: (
                    _safe_float(row.get("fit_rmse_ppm")) if _safe_float(row.get("fit_rmse_ppm")) is not None else float("inf"),
                    _safe_float(row.get("fit_max_abs_error_ppm"))
                    if _safe_float(row.get("fit_max_abs_error_ppm")) is not None
                    else float("inf"),
                ),
            )

    recommendation_rows = [
        {
            "recommendation_item": "selected_algorithm_contract",
            "recommendation": "evaluate_no_pressure_senco13_with_optional_h2o_dry_basis_bridge_per_device",
            "status": "data_driven_review_required_not_authorized_for_write",
            "physical_meaning": (
                "Use clean open-flow multi-temperature CO2 data to fit SENCO1 ratio terms and SENCO3 T/T2/RT "
                "without pressure terms. Evaluate the H2O dry-basis bridge as an explicit competing contract. "
                "Select the bridge only when its residuals improve and H2O evidence is credible; otherwise keep "
                "the no-bridge raw SENCO1/SENCO3 contract and investigate the H2O/S6 evidence separately."
            ),
        },
        {
            "recommendation_item": "fit_point_treatment_plan",
            "recommendation": (
                "apply_ratio_first_treatment_plan_to_fit_set"
                if fit_point_treatment_plan_csv
                else "not_supplied"
            ),
            "status": (
                "applied_offline_no_write"
                if fit_point_treatment_plan_csv
                else "legacy_source_role_only"
            ),
            "physical_meaning": (
                "When supplied, the treatment plan makes ratio-stable/deep-dry points fit eligible even when "
                "uncalibrated displayed CO2/H2O disagree, keeps estimated zero anchors with uncertainty review, "
                "and disables H2O bridge where analyzer H2O output disagrees with dewpoint evidence."
            ),
        },
        {
            "recommendation_item": "h2o_dry_basis_bridge",
            "recommendation": "fit_raw_senco13_target_as_display_target_times_1_minus_h2o_mmol_per_1000_when_validated",
            "status": "candidate_bridge_not_automatic",
            "physical_meaning": (
                "Firmware displayed CO2 is modeled as raw SENCO1/SENCO3 CO2 divided by "
                "(1 - H2O_mmol_mol/1000). Fitting the displayed target directly into SENCO1/SENCO3 mixes the "
                "H2O correction layer into the optical/temperature CO2 chain and can create common-mode residuals. "
                "However, if H2O_mmol comes from an untrusted S6/final-output state, applying this bridge can make "
                "the CO2 fit worse; the matrix must show that explicitly."
            ),
        },
        {
            "recommendation_item": "pressure_terms",
            "recommendation": "do_not_fit_new_P_or_RTP_from_current_data",
            "status": "blocked_until_clean_pressure_span_exists",
            "physical_meaning": (
                "Pressure is already handled by the independent SENCO9 pressure-channel workflow. The open-flow "
                "CO2 run has too little pressure span, so P/RTP cannot be fitted from these samples."
            ),
        },
        {
            "recommendation_item": "zero_gas_anchor",
            "recommendation": (
                "allow_estimated_zero_only_in_no_write_sensitivity"
                if estimated_zero_target_present
                else "exclude_unless_explicit_CO2_zero_certificate_exists"
            ),
            "status": (
                "included_as_estimated_zero_anchor_for_sensitivity_not_formal_release"
                if estimated_zero_target_present
                else "included_as_certified_zero_anchor"
                if certified_zero_target_present
                else "included_as_target_0ppm_needs_traceability_review"
                if explicit_zero_target_present
                else "blocked_without_CO2_zero_certificate"
            ),
            "physical_meaning": (
                "Dry air or O2/N2 zero gas is a useful low-CO2 physical anchor. If CO2 is not certified, V1.5 may "
                "use it only for no-write intercept sensitivity and must label it as an estimated zero anchor. It "
                "does not become formal release evidence until the CO2 content and uncertainty are traceable."
            ),
        },
        {
            "recommendation_item": "senco5_senco6",
            "recommendation": "review_as_integrated_final_output_layer_when_displayed_ppm_is_acceptance_output",
            "status": "requires_integrated_senco5_senco6_candidate_review",
            "physical_meaning": (
                "The current optical/temperature concentration chain is SENCO1+SENCO3. SENCO5/SENCO6 are final "
                "output concentration affine layers (concentration*C1+C0) and must be reviewed as part of the "
                "same released candidate package when final displayed concentration is the acceptance result."
            ),
        },
    ]
    for row in recommended_by_device.values():
        recommendation_rows.append(
            {
                "recommendation_item": f"device_{row['device_id']}_next_candidate",
                "recommendation": row["model_id"],
                "status": row["recommendation_status"],
            "physical_meaning": (
                f"verification_max_abs_error_ppm={row['verification_max_abs_error_ppm']}; "
                    f"candidate fit excludes pressure terms; target P/RTP slots are zero: {row['secondary_payload']}"
                ),
            }
        )
    return {
        "co2_fit_algorithm_matrix_summary": summary_rows,
        "co2_fit_algorithm_matrix_coefficients": coefficient_rows,
        "co2_fit_algorithm_matrix_predictions": prediction_rows,
        "co2_fit_algorithm_matrix_recommendation": recommendation_rows,
    }


def _physical_meaning(variant: ModelVariant, status: str) -> str:
    if variant.model_id == "senco1_ratio_only":
        return "Ratio-only SENCO1 can fit a single thermal state, but it discards the measured multi-temperature physics."
    if variant.model_id == "senco13_temperature_terms_pressure_zero":
        return "Fits R and T terms only; P/RTP target slots are zero because pressure is handled by the SENCO9 workflow."
    if variant.model_id == "senco13_temperature_terms_pressure_zero_h2o_bridge":
        return (
            "Fits raw SENCO1/SENCO3 R/T terms after back-calculating the displayed CO2 target through the "
            "firmware H2O dry-basis correction; P/RTP target slots remain zero."
        )
    if variant.model_id == "senco13_temperature_terms_preserve_existing_pressure_slots":
        return "Diagnostic only: fits R/T and preserves old secondary pressure slots to estimate old-tail influence."
    if variant.model_id == "legacy_v1_a0_a8_full_rt_p_kpa":
        return "Legacy full a0..a8 model with pressure in kPa; blocked because current data do not span pressure."
    return f"Diagnostic only: {status}."


def write_co2_fit_algorithm_matrix_report(
    *,
    fit_residuals_csv: str | Path,
    output_dir: str | Path,
    old_snapshot_json: str | Path | None = None,
    exclude_device_ids: Sequence[str] = (),
    fit_point_treatment_plan_csv: str | Path | None = None,
) -> Dict[str, Path]:
    destination = Path(output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    tables = build_co2_fit_algorithm_matrix_tables(
        fit_residuals_csv=fit_residuals_csv,
        old_snapshot_json=old_snapshot_json,
        exclude_device_ids=exclude_device_ids,
        fit_point_treatment_plan_csv=fit_point_treatment_plan_csv,
    )
    outputs: Dict[str, Path] = {}
    for name, rows in tables.items():
        path = destination / f"{name}.csv"
        _write_csv(path, rows)
        outputs[f"{name}_csv"] = path

    meta = {
        "tool_name": "export_v1_5_co2_fit_algorithm_matrix",
        "created_at": _now(),
        "inputs": {
            "fit_residuals_csv": str(Path(fit_residuals_csv).resolve()),
            "old_snapshot_json": str(Path(old_snapshot_json).resolve()) if old_snapshot_json else "",
            "exclude_device_ids": list(exclude_device_ids),
            "fit_point_treatment_plan_csv": (
                str(Path(fit_point_treatment_plan_csv).resolve())
                if fit_point_treatment_plan_csv
                else ""
            ),
        },
        "boundary": {
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
        },
    }
    meta_path = destination / "co2_fit_algorithm_matrix_meta.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    outputs["meta_json"] = meta_path

    report_path = destination / "co2_fit_algorithm_matrix.md"
    outputs["markdown"] = _write_markdown(report_path, tables)
    return outputs


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> Path:
    summary = list(tables.get("co2_fit_algorithm_matrix_summary") or [])
    recommendation = list(tables.get("co2_fit_algorithm_matrix_recommendation") or [])
    lines = [
        "# V1.5 CO2 Fitting Algorithm Matrix",
        "",
        "- Boundary: offline only; no COM, no route control, no SENCO write.",
        "- Selected next no-write contract: data-driven `SENCO1/SENCO3` no-pressure review with optional H2O bridge.",
        "",
        "## Recommendation",
        "",
    ]
    for row in recommendation[:4]:
        lines.append(f"- {row.get('recommendation_item')}: {row.get('recommendation')} ({row.get('status')})")
    lines.extend(
        [
            "",
            "## Matrix",
            "",
            "| Device | Model | Fit RMSE | Fit Max | Fit Max Rel % | Verify RMSE | Verify Max | Verify Max Rel % | Status | Meaning |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
        ]
    )
    for row in summary:
        lines.append(
            "| {device} | {model} | {fit_rmse} | {fit_max} | {fit_max_rel} | {verify_rmse} | {verify_max} | {verify_max_rel} | {status} | {meaning} |".format(
                device=row.get("device_id", ""),
                model=row.get("model_id", ""),
                fit_rmse=_fmt(row.get("fit_rmse_ppm")),
                fit_max=_fmt(row.get("fit_max_abs_error_ppm")),
                fit_max_rel=_fmt(row.get("fit_max_abs_relative_error_percent")),
                verify_rmse=_fmt(row.get("verification_rmse_ppm")),
                verify_max=_fmt(row.get("verification_max_abs_error_ppm")),
                verify_max_rel=_fmt(row.get("verification_max_abs_relative_error_percent")),
                status=row.get("recommendation_status", ""),
                meaning=(
                    str(row.get("physical_meaning") or "").replace("|", "/")
                    + f" zero_anchor={row.get('zero_anchor_policy_status', '')}"
                ),
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _fmt(value: Any) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return ""
    return f"{numeric:.6g}"
