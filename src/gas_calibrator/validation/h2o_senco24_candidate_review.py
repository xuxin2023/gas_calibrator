"""No-write H2O SENCO2/SENCO4 candidate review from V1.5 open-flow evidence.

The review in this module is offline-only. It consumes completed H2O
open-flow point artifacts and produces reviewer evidence for the analyzer H2O
ratio/temperature model. It never opens COM ports, controls water/gas routes,
controls PACE/valves, or writes SENCO coefficients.
"""

from __future__ import annotations

import csv
import hashlib
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
from ..senco_format import format_senco_values
from .factory_signal_health_review import (
    factory_signal_health_block_reason,
    load_factory_signal_health_summary,
)
from .reporting import ValidationMetadata, write_validation_report


H2O_TERMS: Tuple[str, ...] = ("intercept", "R", "R2", "R3", "T", "T2", "RT")
PRESSURE_TERMS: Tuple[str, ...] = ("P", "RP", "RTP")
SENCO6_SEPARATE_LAYER_CONTRACT = "senco6_separate_final_affine_layer_do_not_fold_into_senco24"
SENCO24_MAIN_CHAIN_CONTRACT = "senco24_h2o_ratio_temperature_main_chain_only"
MINIMAX_RELATIVE_MONOTONIC_OBJECTIVE = "minimax_relative_mmol_floor_monotonic"
MINIMAX_MONOTONIC_DERIVATIVE_LIMIT = -1.0e-8


@dataclass(frozen=True)
class H2OSenco24CandidateConfig:
    """Policy for H2O SENCO2/SENCO4 no-write candidate review."""

    min_points: int = 8
    min_wet_points: int = 3
    max_condition_number: float = 1.0e8
    fit_max_abs_error_mmol: float = 0.5
    design_max_relative_error_pct: float = 2.0
    relative_error_min_reference_mmol: float = 2.0
    final_output_pin_span_mmol: float = 0.001
    final_output_pin_min_reference_span_mmol: float = 2.0
    stale_side_channel_age_ms: float = 2000.0
    terms: Tuple[str, ...] = H2O_TERMS
    exclude_device_ids: Tuple[str, ...] = field(default_factory=tuple)
    manual_device_block_reasons: Mapping[str, str] = field(default_factory=dict)
    manual_point_block_reasons: Mapping[str, str] = field(default_factory=dict)
    component_snapshot: Mapping[str, Any] = field(default_factory=dict)
    postwrite_verified_device_ids: Tuple[str, ...] = field(default_factory=tuple)
    postwrite_verification_artifacts: Tuple[str, ...] = field(default_factory=tuple)
    additional_h2o_roots: Tuple[str, ...] = field(default_factory=tuple)
    dry_anchor_roots: Tuple[str, ...] = field(default_factory=tuple)
    dry_anchor_min_temp_c: Optional[float] = None
    dry_anchor_max_temp_c: Optional[float] = None
    allow_pressure_qc_failed_device_ids: Tuple[str, ...] = field(default_factory=tuple)
    fit_temperature_source: str = "analyzer_chamber"
    fit_objective: str = "absolute_mmol"
    require_component_snapshot_for_layer_review: bool = False
    factory_signal_health_summary_csv: str | Path | None = None
    state_transfer_summary_csv: str | Path | None = None
    state_transfer_max_raw_excess_shift_mmol: float = 0.1
    state_transfer_max_post_s6_relative_error_pct: float = 2.0


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        number = float(value)
    except Exception:
        return None
    if not math.isfinite(number):
        return None
    return number


def _safe_int(value: Any) -> Optional[int]:
    number = _safe_float(value)
    if number is None:
        return None
    return int(number)


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normal_device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.startswith("GA"):
        text = text[2:]
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def _resolve_run_root(path: str | Path) -> Path:
    root = Path(path).resolve()
    if any(root.glob("p*_h2o")):
        return root
    parent = root.parent
    if any(parent.glob("p*_h2o")):
        return parent
    return root


def _resolve_dry_anchor_root(path: str | Path) -> Path:
    root = Path(path).resolve()
    if root.is_dir() and any(_is_zero_gas_anchor_dir(item) for item in root.iterdir() if item.is_dir()):
        return root
    parent = root.parent
    if parent.is_dir() and any(_is_zero_gas_anchor_dir(item) for item in parent.iterdir() if item.is_dir()):
        return parent
    return root


def _latest_summary_csv(point_dir: Path) -> Optional[Path]:
    files = sorted(point_dir.glob("*水路*.csv"))
    if files:
        return files[-1]
    files = sorted(point_dir.glob("*h2o*.csv"))
    return files[-1] if files else None


def _latest_dry_anchor_summary_csv(point_dir: Path) -> Optional[Path]:
    candidates: List[Path] = []
    for pattern in ("*气路*.csv", "*姘旇矾*.csv", "*co2*.csv", "*分析仪汇总*.csv"):
        candidates.extend(sorted(point_dir.glob(pattern)))
    filtered: List[Path] = []
    for path in dict.fromkeys(candidates):
        name = path.name.lower()
        if any(
            token in name
            for token in (
                "samples",
                "points_",
                "point_",
                "io_",
                "trace",
                "pressure",
                "quality",
                "timing",
                "conclusion",
                "overview",
                "comparison",
                "alignment",
                "audit",
                "meta",
            )
        ):
            continue
        filtered.append(path)
    if not filtered:
        return None
    filtered.sort(key=lambda path: ("气路" in path.name or "汇总" in path.name, path.name))
    return filtered[-1]


def _is_zero_gas_anchor_dir(point_dir: Path) -> bool:
    name = point_dir.name.lower()
    if "h2o" in name:
        return False
    return re.search(r"(^|_)0ppm(_|$)", name) is not None


def _temp_set_from_point_name(point_name: str) -> Optional[float]:
    match = re.search(r"(?:^|_)T(?P<temp>m?\d+(?:\.\d+)?)(?:_|$)", point_name)
    if not match:
        return None
    raw = match.group("temp")
    sign = -1.0 if raw.startswith("m") else 1.0
    raw = raw[1:] if raw.startswith("m") else raw
    value = _safe_float(raw)
    return sign * value if value is not None else None


def _channel_device_id_map(point_dir: Path) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    sample_path = point_dir / "samples_machine_readable.csv"
    for row in _read_csv(sample_path):
        for key, value in row.items():
            match = re.match(r"^(ga\d+)_analyzer_device_id$", str(key or "").lower())
            if not match:
                continue
            device_id = _normal_device_id(value)
            if device_id:
                mapping.setdefault(match.group(1), device_id)
        if mapping:
            break
    return mapping


def _load_queue_manifest(root: Path) -> Dict[str, Mapping[str, Any]]:
    candidates = sorted(root.glob("h2o_*_r*/queue_manifest.csv"))
    candidates.extend(sorted(root.glob("*/queue_manifest.csv")))
    candidates.extend(sorted(root.glob("queue_manifest.csv")))
    manifest: Dict[str, Mapping[str, Any]] = {}
    for path in candidates:
        for row in _read_csv(path):
            point_run_id = str(row.get("point_run_id") or "").strip()
            if point_run_id:
                manifest[point_run_id] = row
    return manifest


def _sample_alignment_summary(point_dir: Path) -> Dict[str, Any]:
    path = point_dir / "samples_machine_readable.csv"
    rows = _read_csv(path)
    if not rows:
        return {
            "sample_file": str(path),
            "sample_alignment_status": "missing",
            "sample_alignment_ok_count": 0,
            "sample_alignment_total_count": 0,
            "sample_alignment_failure_reasons": "samples_machine_readable_missing",
        }
    ok_count = 0
    reasons: List[str] = []
    max_ages: List[float] = []
    thermometer_ages: List[float] = []
    hgen_ages: List[float] = []
    dewpoint_ages: List[float] = []
    for row in rows:
        if str(row.get("sample_alignment_ok") or "").strip().lower() in {"true", "1", "yes", "pass"}:
            ok_count += 1
        reason = str(row.get("sample_alignment_failure_reason") or "").strip()
        if reason:
            reasons.extend(item for item in reason.split(";") if item)
        for key, target in (
            ("sampling_time_alignment_max_age_ms", max_ages),
            ("thermometer_cache_age_ms", thermometer_ages),
            ("hgen_cache_age_ms", hgen_ages),
            ("dewpoint_sample_age_ms", dewpoint_ages),
        ):
            value = _safe_float(row.get(key))
            if value is not None:
                target.append(float(value))
    total = len(rows)
    return {
        "sample_file": str(path),
        "sample_file_sha256": _sha256_file(path),
        "sample_alignment_status": "pass" if ok_count == total else "warn",
        "sample_alignment_ok_count": ok_count,
        "sample_alignment_total_count": total,
        "sample_alignment_failure_reasons": ";".join(sorted(set(reasons))),
        "sampling_time_alignment_max_age_ms": max(max_ages) if max_ages else "",
        "thermometer_cache_max_age_ms": max(thermometer_ages) if thermometer_ages else "",
        "hgen_cache_max_age_ms": max(hgen_ages) if hgen_ages else "",
        "dewpoint_sample_max_age_ms": max(dewpoint_ages) if dewpoint_ages else "",
    }


def _mean_of(values: Sequence[Any]) -> Optional[float]:
    numbers = [float(value) for value in (_safe_float(item) for item in values) if value is not None]
    if not numbers:
        return None
    return float(mean(numbers))


def _sample_device_fallback_map(point_dir: Path) -> Dict[str, Dict[str, Any]]:
    """Return per-analyzer sample-window values even when summary QC blanks a row.

    The formal point summary intentionally blanks analyzers whose MODE2 frame
    failed a broad QC gate, such as the pressure-channel failure seen on ID090.
    For H2O component fitting, pressure is not a polynomial fit input, so the
    raw factory ratio/temperature values remain useful reviewer evidence when a
    device is explicitly allowed by policy.
    """

    rows = _read_csv(point_dir / "samples_machine_readable.csv")
    if not rows:
        return {}
    prefixes: set[str] = set()
    for row in rows:
        for key in row:
            match = re.match(r"^(ga\d+)_", str(key or "").lower())
            if match:
                prefixes.add(match.group(1))
    out: Dict[str, Dict[str, Any]] = {}
    for prefix in sorted(prefixes):
        device_ids = [
            _normal_device_id(row.get(f"{prefix}_analyzer_device_id"))
            for row in rows
            if _normal_device_id(row.get(f"{prefix}_analyzer_device_id"))
        ]
        out[prefix] = {
            "sample_analyzer_device_id": device_ids[0] if device_ids else "",
            "sample_analyzer_h2o_mmol": _mean_of([row.get(f"{prefix}_h2o_mmol") for row in rows]),
            "sample_h2o_ratio_f": _mean_of([row.get(f"{prefix}_h2o_ratio_f") for row in rows]),
            "sample_h2o_ratio_raw": _mean_of([row.get(f"{prefix}_h2o_ratio_raw") for row in rows]),
            "sample_chamber_temp_c": _mean_of([row.get(f"{prefix}_chamber_temp_c") for row in rows]),
            "sample_case_temp_c": _mean_of([row.get(f"{prefix}_case_temp_c") for row in rows]),
            "sample_analyzer_pressure_kpa": _mean_of([row.get(f"{prefix}_pressure_kpa") for row in rows]),
            "sample_mode2_qc_status": next(
                (str(row.get(f"{prefix}_mode2_qc_status") or "").strip() for row in rows if row.get(f"{prefix}_mode2_qc_status")),
                "",
            ),
            "sample_mode2_qc_reason": next(
                (str(row.get(f"{prefix}_mode2_qc_reason") or "").strip() for row in rows if row.get(f"{prefix}_mode2_qc_reason")),
                "",
            ),
            "sample_frame_status": next(
                (str(row.get(f"{prefix}_frame_status") or "").strip() for row in rows if row.get(f"{prefix}_frame_status")),
                "",
            ),
        }
    return out


def _point_rows(root: Path) -> List[Dict[str, Any]]:
    manifest = _load_queue_manifest(root)
    rows: List[Dict[str, Any]] = []
    point_dirs = sorted(root.glob("p*_h2o"))
    if not point_dirs and _latest_summary_csv(root) is not None:
        point_dirs = [root]
    for point_dir in point_dirs:
        summary_path = _latest_summary_csv(point_dir)
        if summary_path is None:
            continue
        point_manifest = dict(manifest.get(point_dir.name) or {})
        alignment = _sample_alignment_summary(point_dir)
        channel_device_ids = _channel_device_id_map(point_dir)
        sample_fallback = _sample_device_fallback_map(point_dir)
        for row in _read_csv(summary_path):
            analyzer = str(row.get("Analyzer") or "").strip()
            analyzer_prefix = analyzer.lower()
            fallback = sample_fallback.get(analyzer_prefix, {})
            fallback_device_id = _normal_device_id(fallback.get("sample_analyzer_device_id"))
            device_id = fallback_device_id or channel_device_ids.get(analyzer_prefix) or _normal_device_id(analyzer)
            reference = _safe_float(row.get("ppm_H2O_Dew"))
            ratio = _safe_float(row.get("R_H2O"))
            analyzer_h2o = _safe_float(row.get("ppm_H2O"))
            chamber_temp = _safe_float(row.get("T1"))
            box_temp = _safe_float(row.get("Temp"))
            pressure_hpa = _safe_float(row.get("P") or row.get("PSample"))
            bar = _safe_float(row.get("BAR"))
            if pressure_hpa is None and bar is not None:
                pressure_hpa = bar * 10.0
            item = {
                "point_run_id": point_dir.name,
                "point_id": point_manifest.get("point_id", ""),
                "sample_role": point_manifest.get("sample_role", "fit") or "fit",
                "temp_set_c": _safe_float(point_manifest.get("temp_c")),
                "hgen_temp_set_c": _safe_float(point_manifest.get("hgen_temp_c")),
                "hgen_rh_set_pct": _safe_float(point_manifest.get("hgen_rh_pct")),
                "nominal_plan_h2o_mmol": _safe_float(point_manifest.get("reference_h2o_mmol")),
                "nominal_plan_dewpoint_c": _safe_float(point_manifest.get("reference_dewpoint_c")),
                "analyzer": analyzer,
                "analyzer_prefix": analyzer_prefix,
                "analyzer_device_id": device_id,
                "reference_h2o_mmol": reference,
                "reference_source": "dewpoint_meter_plus_com22_pressure",
                "reference_dewpoint_c": _safe_float(row.get("Dew")),
                "reference_pressure_hpa": pressure_hpa,
                "analyzer_h2o_mmol": analyzer_h2o,
                "h2o_ratio_f": ratio,
                "h2o_ratio_dev": _safe_float(row.get("R_H2O_dev")),
                "chamber_temp_c": chamber_temp,
                "digital_thermometer_temp_c": box_temp,
                "case_temp_c": _safe_float(row.get("T2")),
                "analyzer_pressure_kpa": bar,
                "valid_frames": _safe_int(row.get("ValidFrames")),
                "total_frames": _safe_int(row.get("TotalFrames")),
                "frame_status": row.get("FrameStatus", ""),
                "point_integrity": row.get("PointIntegrity", ""),
                "summary_file": str(summary_path),
                "summary_file_sha256": _sha256_file(summary_path),
                "h2o_source_root": str(root),
                **fallback,
                **alignment,
            }
            rows.append(item)
    return rows


def _dry_anchor_rows(
    root: Path,
    *,
    min_temp_c: Optional[float] = None,
    max_temp_c: Optional[float] = None,
) -> List[Dict[str, Any]]:
    manifest = _load_queue_manifest(root)
    rows: List[Dict[str, Any]] = []
    for point_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        if not _is_zero_gas_anchor_dir(point_dir):
            continue
        summary_path = _latest_dry_anchor_summary_csv(point_dir)
        if summary_path is None:
            continue
        point_manifest = dict(manifest.get(point_dir.name) or {})
        alignment = _sample_alignment_summary(point_dir)
        channel_device_ids = _channel_device_id_map(point_dir)
        sample_fallback = _sample_device_fallback_map(point_dir)
        temp_set_c = _safe_float(point_manifest.get("temp_c"))
        if temp_set_c is None:
            temp_set_c = _temp_set_from_point_name(point_dir.name)
        if min_temp_c is not None and (temp_set_c is None or temp_set_c < float(min_temp_c)):
            continue
        if max_temp_c is not None and (temp_set_c is None or temp_set_c > float(max_temp_c)):
            continue
        for row in _read_csv(summary_path):
            analyzer = str(row.get("Analyzer") or "").strip()
            analyzer_prefix = analyzer.lower()
            fallback = sample_fallback.get(analyzer_prefix, {})
            fallback_device_id = _normal_device_id(fallback.get("sample_analyzer_device_id"))
            device_id = fallback_device_id or channel_device_ids.get(analyzer_prefix) or _normal_device_id(analyzer)
            reference = _safe_float(row.get("ppm_H2O_Dew"))
            ratio = _safe_float(row.get("R_H2O"))
            analyzer_h2o = _safe_float(row.get("ppm_H2O"))
            chamber_temp = _safe_float(row.get("T1"))
            pressure_hpa = _safe_float(row.get("P") or row.get("PSample"))
            bar = _safe_float(row.get("BAR"))
            if pressure_hpa is None and bar is not None:
                pressure_hpa = bar * 10.0
            item = {
                "point_run_id": point_dir.name,
                "point_id": point_manifest.get("point_id", ""),
                "sample_role": "dry_anchor",
                "temp_set_c": temp_set_c,
                "hgen_temp_set_c": "",
                "hgen_rh_set_pct": "",
                "nominal_plan_h2o_mmol": "",
                "nominal_plan_dewpoint_c": "",
                "analyzer": analyzer,
                "analyzer_prefix": analyzer_prefix,
                "analyzer_device_id": device_id,
                "reference_h2o_mmol": reference,
                "reference_source": "dewpoint_meter_plus_com22_pressure_dry_gas_anchor",
                "reference_dewpoint_c": _safe_float(row.get("Dew")),
                "reference_pressure_hpa": pressure_hpa,
                "analyzer_h2o_mmol": analyzer_h2o,
                "h2o_ratio_f": ratio,
                "h2o_ratio_dev": _safe_float(row.get("R_H2O_dev")),
                "chamber_temp_c": chamber_temp,
                "digital_thermometer_temp_c": _safe_float(row.get("Temp")),
                "case_temp_c": _safe_float(row.get("T2")),
                "analyzer_pressure_kpa": bar,
                "valid_frames": _safe_int(row.get("ValidFrames")),
                "total_frames": _safe_int(row.get("TotalFrames")),
                "frame_status": row.get("FrameStatus", ""),
                "point_integrity": row.get("PointIntegrity", ""),
                "summary_file": str(summary_path),
                "summary_file_sha256": _sha256_file(summary_path),
                "dry_anchor_source_root": str(root),
                "dry_anchor_source_point": point_dir.name,
                "dry_anchor_route": "co2_open_flow_gas_route",
                "h2o_anchor_class": "dry_gas_anchor",
                "physical_note": (
                    "Dry-gas anchor uses dewpoint/pressure-derived residual water; "
                    "the H2O target is not forced to zero."
                ),
                **fallback,
                **alignment,
            }
            rows.append(item)
    return rows


def _feature_value(term: str, ratio: float, temp_c: float) -> float:
    temp_k = temp_c + 273.15
    if term == "intercept":
        return 1.0
    if term == "R":
        return ratio
    if term == "R2":
        return ratio**2
    if term == "R3":
        return ratio**3
    if term == "T":
        return temp_k
    if term == "T2":
        return temp_k**2
    if term == "RT":
        return ratio * temp_k
    raise ValueError(f"Unsupported H2O SENCO2/4 term: {term}")


def _matrix(rows: Sequence[Mapping[str, Any]], terms: Sequence[str]) -> np.ndarray:
    return np.asarray(
        [
            [
                _feature_value(
                    term,
                    float(row["h2o_ratio_f"]),
                    float(row["chamber_temp_c"]),
                )
                for term in terms
            ]
            for row in rows
        ],
        dtype=float,
    )


def _scaled_lstsq(matrix: np.ndarray, target: np.ndarray) -> tuple[np.ndarray, int, float]:
    scales = np.linalg.norm(matrix, axis=0)
    scales = np.where(np.isfinite(scales) & (scales > 0.0), scales, 1.0)
    scaled = matrix / scales
    rank = int(np.linalg.matrix_rank(scaled))
    condition = float(np.linalg.cond(scaled))
    scaled_coeffs, _, _, _ = np.linalg.lstsq(scaled, target, rcond=None)
    return np.asarray(scaled_coeffs, dtype=float) / scales, rank, condition


def _fit_weights(target: np.ndarray, cfg: H2OSenco24CandidateConfig) -> np.ndarray:
    objective = str(cfg.fit_objective or "absolute_mmol").strip().lower()
    if objective == "absolute_mmol":
        return np.ones_like(target, dtype=float)
    floor = max(float(cfg.relative_error_min_reference_mmol), 1.0e-12)
    denominator = np.maximum(np.abs(target.astype(float)), floor)
    if objective == "sqrt_relative_mmol_floor":
        return 1.0 / np.sqrt(denominator)
    if objective == "relative_mmol_floor":
        return 1.0 / denominator
    raise ValueError(
        "Unsupported H2O fit objective: "
        f"{cfg.fit_objective!r}; expected absolute_mmol, "
        "sqrt_relative_mmol_floor, or relative_mmol_floor"
    )


def _scaled_weighted_lstsq(
    matrix: np.ndarray,
    target: np.ndarray,
    cfg: H2OSenco24CandidateConfig,
) -> tuple[np.ndarray, int, float]:
    scales = np.linalg.norm(matrix, axis=0)
    scales = np.where(np.isfinite(scales) & (scales > 0.0), scales, 1.0)
    scaled = matrix / scales
    weights = _fit_weights(target, cfg)
    weighted_matrix = scaled * weights[:, None]
    weighted_target = target * weights
    rank = int(np.linalg.matrix_rank(weighted_matrix))
    condition = float(np.linalg.cond(weighted_matrix))
    scaled_coeffs, _, _, _ = np.linalg.lstsq(weighted_matrix, weighted_target, rcond=None)
    return np.asarray(scaled_coeffs, dtype=float) / scales, rank, condition


def _centered_feature_value(
    term: str,
    ratio: float,
    temp_c: float,
    ratio_center: float,
    temp_k_center: float,
) -> float:
    ratio_delta = ratio - ratio_center
    temp_k_delta = temp_c + 273.15 - temp_k_center
    values = {
        "intercept": 1.0,
        "R": ratio_delta,
        "R2": ratio_delta**2,
        "R3": ratio_delta**3,
        "T": temp_k_delta,
        "T2": temp_k_delta**2,
        "RT": ratio_delta * temp_k_delta,
    }
    try:
        return float(values[term])
    except KeyError as exc:
        raise ValueError(f"Unsupported H2O SENCO2/4 term: {term}") from exc


def _centered_derivative_value(
    term: str,
    ratio: float,
    temp_c: float,
    ratio_center: float,
    temp_k_center: float,
) -> float:
    ratio_delta = ratio - ratio_center
    temp_k_delta = temp_c + 273.15 - temp_k_center
    values = {
        "intercept": 0.0,
        "R": 1.0,
        "R2": 2.0 * ratio_delta,
        "R3": 3.0 * ratio_delta**2,
        "T": 0.0,
        "T2": 0.0,
        "RT": temp_k_delta,
    }
    try:
        return float(values[term])
    except KeyError as exc:
        raise ValueError(f"Unsupported H2O SENCO2/4 term: {term}") from exc


def _centered_matrix(
    rows: Sequence[Mapping[str, Any]],
    terms: Sequence[str],
    ratio_center: float,
    temp_k_center: float,
) -> np.ndarray:
    return np.asarray(
        [
            [
                _centered_feature_value(
                    term,
                    float(row["h2o_ratio_f"]),
                    float(row["chamber_temp_c"]),
                    ratio_center,
                    temp_k_center,
                )
                for term in terms
            ]
            for row in rows
        ],
        dtype=float,
    )


def _centered_derivative_matrix(
    rows: Sequence[Mapping[str, Any]],
    terms: Sequence[str],
    ratio_center: float,
    temp_k_center: float,
) -> np.ndarray:
    return np.asarray(
        [
            [
                _centered_derivative_value(
                    term,
                    float(row["h2o_ratio_f"]),
                    float(row["chamber_temp_c"]),
                    ratio_center,
                    temp_k_center,
                )
                for term in terms
            ]
            for row in rows
        ],
        dtype=float,
    )


def _centered_to_absolute_coefficients(
    terms: Sequence[str],
    values: Sequence[float],
    ratio_center: float,
    temp_k_center: float,
) -> np.ndarray:
    centered = {term: 0.0 for term in H2O_TERMS}
    centered.update({term: float(value) for term, value in zip(terms, values)})
    absolute = {
        "intercept": (
            centered["intercept"]
            - ratio_center * centered["R"]
            + ratio_center**2 * centered["R2"]
            - ratio_center**3 * centered["R3"]
            - temp_k_center * centered["T"]
            + temp_k_center**2 * centered["T2"]
            + ratio_center * temp_k_center * centered["RT"]
        ),
        "R": (
            centered["R"]
            - 2.0 * ratio_center * centered["R2"]
            + 3.0 * ratio_center**2 * centered["R3"]
            - temp_k_center * centered["RT"]
        ),
        "R2": centered["R2"] - 3.0 * ratio_center * centered["R3"],
        "R3": centered["R3"],
        "T": (
            centered["T"]
            - 2.0 * temp_k_center * centered["T2"]
            - ratio_center * centered["RT"]
        ),
        "T2": centered["T2"],
        "RT": centered["RT"],
    }
    return np.asarray([absolute[term] for term in terms], dtype=float)


def _firmware_quantize_coefficients(values: Sequence[float]) -> np.ndarray:
    return np.asarray(
        [float(format_senco_values([float(value)])[0]) for value in values],
        dtype=float,
    )


def _monotonic_grid_rows(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, float]]:
    ratios = [float(row["h2o_ratio_f"]) for row in rows]
    temperatures = [float(row["chamber_temp_c"]) for row in rows]
    ratio_grid = (
        np.linspace(min(ratios), max(ratios), 31)
        if _span(ratios) > 0.0
        else np.asarray([ratios[0]])
    )
    temperature_grid = (
        np.linspace(min(temperatures), max(temperatures), 17)
        if _span(temperatures) > 0.0
        else np.asarray([temperatures[0]])
    )
    return [
        {"h2o_ratio_f": float(ratio), "chamber_temp_c": float(temperature)}
        for ratio in ratio_grid
        for temperature in temperature_grid
    ]


def _absolute_derivative(
    ratio: float,
    temp_c: float,
    coefficients: Mapping[str, float],
) -> float:
    temp_k = float(temp_c) + 273.15
    return float(
        float(coefficients.get("R", 0.0))
        + 2.0 * float(coefficients.get("R2", 0.0)) * float(ratio)
        + 3.0 * float(coefficients.get("R3", 0.0)) * float(ratio) ** 2
        + float(coefficients.get("RT", 0.0)) * temp_k
    )


def _monotonicity_audit(
    rows: Sequence[Mapping[str, Any]],
    coefficients: Mapping[str, float],
) -> Dict[str, Any]:
    grid_rows = _monotonic_grid_rows(rows)
    derivatives = [
        _absolute_derivative(
            float(row["h2o_ratio_f"]),
            float(row["chamber_temp_c"]),
            coefficients,
        )
        for row in grid_rows
    ]
    violation_count = sum(value >= 0.0 for value in derivatives)
    return {
        "monotonic_grid_point_count": len(grid_rows),
        "monotonic_derivative_violation_count": int(violation_count),
        "minimum_d_concentration_d_ratio": min(derivatives) if derivatives else "",
        "maximum_d_concentration_d_ratio": max(derivatives) if derivatives else "",
        "monotonic_decreasing_over_fit_envelope": bool(derivatives) and violation_count == 0,
    }


def _fit_coefficients(
    rows: Sequence[Mapping[str, Any]],
    terms: Sequence[str],
    target: np.ndarray,
    cfg: H2OSenco24CandidateConfig,
) -> tuple[np.ndarray, int, float, Dict[str, Any]]:
    objective = str(cfg.fit_objective or "absolute_mmol").strip().lower()
    if objective != MINIMAX_RELATIVE_MONOTONIC_OBJECTIVE:
        matrix = _matrix(rows, terms)
        coefficients, rank, condition = _scaled_weighted_lstsq(matrix, target, cfg)
        return coefficients, rank, condition, {
            "fit_solver": "scaled_weighted_least_squares",
            "fit_basis": "firmware_absolute_terms",
            "fit_objective_bound_pct_pre_firmware_quantization": "",
            "coefficients_quantized_for_firmware": False,
        }

    try:
        from scipy.optimize import linprog
    except ImportError as exc:
        raise RuntimeError(
            "The minimax H2O review objective requires scipy.optimize.linprog; "
            "install scipy before using this optional offline objective."
        ) from exc

    ratio_center = float(mean(float(row["h2o_ratio_f"]) for row in rows))
    temp_k_center = float(mean(float(row["chamber_temp_c"]) + 273.15 for row in rows))
    centered = _centered_matrix(rows, terms, ratio_center, temp_k_center)
    scales = np.linalg.norm(centered, axis=0)
    scales = np.where(np.isfinite(scales) & (scales > 0.0), scales, 1.0)
    scaled = centered / scales
    rank = int(np.linalg.matrix_rank(scaled))
    condition = float(np.linalg.cond(scaled))

    floor = max(float(cfg.relative_error_min_reference_mmol), 1.0e-12)
    denominator = np.maximum(np.abs(target.astype(float)), floor)
    constraint_rows: List[np.ndarray] = []
    constraint_limits: List[float] = []
    for design_row, expected, divisor in zip(scaled, target, denominator):
        constraint_rows.append(np.r_[design_row, -divisor])
        constraint_limits.append(float(expected))
        constraint_rows.append(np.r_[-design_row, -divisor])
        constraint_limits.append(float(-expected))

    monotonic_rows = _monotonic_grid_rows(rows)
    derivative_matrix = _centered_derivative_matrix(
        monotonic_rows,
        terms,
        ratio_center,
        temp_k_center,
    ) / scales
    for derivative_row in derivative_matrix:
        constraint_rows.append(np.r_[derivative_row, 0.0])
        constraint_limits.append(MINIMAX_MONOTONIC_DERIVATIVE_LIMIT)

    term_count = len(terms)
    result = linprog(
        np.r_[np.zeros(term_count), 1.0],
        A_ub=np.asarray(constraint_rows, dtype=float),
        b_ub=np.asarray(constraint_limits, dtype=float),
        bounds=[(None, None)] * term_count + [(0.0, None)],
        method="highs",
    )
    if not result.success:
        raise RuntimeError(f"H2O minimax solver failed: {result.message}")

    centered_coefficients = np.asarray(result.x[:term_count], dtype=float) / scales
    absolute_coefficients = _centered_to_absolute_coefficients(
        terms,
        centered_coefficients,
        ratio_center,
        temp_k_center,
    )
    quantized_coefficients = _firmware_quantize_coefficients(absolute_coefficients)
    return quantized_coefficients, rank, condition, {
        "fit_solver": "scipy_highs_linear_program",
        "fit_basis": "centered_R_T_transformed_to_firmware_absolute_terms",
        "fit_objective_bound_pct_pre_firmware_quantization": float(result.x[-1] * 100.0),
        "coefficients_quantized_for_firmware": True,
        "ratio_center": ratio_center,
        "temp_k_center": temp_k_center,
    }


def _prediction(row: Mapping[str, Any], coefficients: Mapping[str, float], terms: Sequence[str]) -> float:
    return float(
        sum(
            float(coefficients[term])
            * _feature_value(term, float(row["h2o_ratio_f"]), float(row["chamber_temp_c"]))
            for term in terms
        )
    )


def _span(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return float(max(values) - min(values))


def _relative_error_pct(
    predicted: float,
    target: float,
    *,
    min_reference_mmol: float = 0.0,
) -> Optional[float]:
    if not math.isfinite(float(target)) or abs(float(target)) <= max(float(min_reference_mmol), 1.0e-12):
        return None
    return float((float(predicted) - float(target)) / float(target) * 100.0)


def _max_abs_relative_error_pct(
    predicted: Sequence[float],
    target: Sequence[float],
    *,
    min_reference_mmol: float = 0.0,
) -> Any:
    errors = [
        abs(error)
        for error in (
            _relative_error_pct(pred, ref, min_reference_mmol=min_reference_mmol)
            for pred, ref in zip(predicted, target)
        )
        if error is not None
    ]
    return max(errors) if errors else ""


def _allowed_pressure_qc_failed_device_ids(cfg: H2OSenco24CandidateConfig) -> set[str]:
    return {
        _normal_device_id(device_id)
        for device_id in tuple(cfg.allow_pressure_qc_failed_device_ids or ())
        if _normal_device_id(device_id)
    }


def _prepare_fit_row(row: Mapping[str, Any], cfg: H2OSenco24CandidateConfig) -> Dict[str, Any]:
    """Prepare the row used by the H2O fit without mutating raw evidence.

    Some point summaries intentionally blank analyzer values after a broad
    MODE2 QC failure. For H2O component fitting, a pressure-channel failure is
    not itself a H2O-ratio failure, so an explicitly allowed device may use the
    sample-window ratio/temperature values while keeping the pressure failure
    visible in the review package.
    """

    prepared = dict(row)
    device_id = _normal_device_id(
        prepared.get("analyzer_device_id") or prepared.get("sample_analyzer_device_id")
    )
    if device_id:
        prepared["analyzer_device_id"] = device_id
    prepared.setdefault("pressure_qc_overridden_for_component_fit", False)
    prepared.setdefault("fit_input_override_reason", "")
    prepared.setdefault("temperature_source_for_fit", "")
    prepared.setdefault("temperature_source_warning", "")
    prepared.setdefault("analyzer_chamber_temp_c_raw", "")
    prepared.setdefault("temperature_source_override_for_post_temperature_calibration_review", False)

    replaced_keys: List[str] = []
    if device_id in _allowed_pressure_qc_failed_device_ids(cfg):
        for target_key, sample_key in (
            ("analyzer_h2o_mmol", "sample_analyzer_h2o_mmol"),
            ("h2o_ratio_f", "sample_h2o_ratio_f"),
            ("chamber_temp_c", "sample_chamber_temp_c"),
            ("case_temp_c", "sample_case_temp_c"),
            ("analyzer_pressure_kpa", "sample_analyzer_pressure_kpa"),
        ):
            if _safe_float(prepared.get(target_key)) is None:
                sample_value = _safe_float(prepared.get(sample_key))
                if sample_value is not None:
                    prepared[target_key] = sample_value
                    replaced_keys.append(target_key)
        if replaced_keys:
            prepared["pressure_qc_overridden_for_component_fit"] = True
            prepared["fit_input_override_reason"] = (
                "pressure_qc_failed_but_ratio_temperature_used_for_component_fit:"
                + ",".join(replaced_keys)
            )

    temperature_source = str(cfg.fit_temperature_source or "analyzer_chamber").strip().lower()
    if temperature_source == "analyzer_chamber":
        prepared["temperature_source_for_fit"] = "analyzer_chamber_temp_c"
    elif temperature_source == "digital_thermometer":
        raw_chamber_temp = _safe_float(prepared.get("chamber_temp_c"))
        digital_temp = _safe_float(prepared.get("digital_thermometer_temp_c"))
        if digital_temp is not None:
            prepared["analyzer_chamber_temp_c_raw"] = raw_chamber_temp if raw_chamber_temp is not None else ""
            prepared["chamber_temp_c"] = digital_temp
            prepared["temperature_source_for_fit"] = "digital_thermometer_temp_c"
            prepared["temperature_source_override_for_post_temperature_calibration_review"] = True
        else:
            prepared["temperature_source_for_fit"] = "analyzer_chamber_temp_c"
            prepared["temperature_source_warning"] = "digital_thermometer_missing_fallback_to_analyzer_chamber"
    else:
        raise ValueError(
            "Unsupported H2O fit temperature source: "
            f"{cfg.fit_temperature_source!r}; expected analyzer_chamber or digital_thermometer"
        )
    return prepared


def _complete_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    cfg: H2OSenco24CandidateConfig,
    manual_point_blocks: Optional[Mapping[str, str]] = None,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    complete: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    point_blocks = {str(key or "").strip(): str(reason or "").strip() for key, reason in dict(manual_point_blocks or {}).items()}
    for row in rows:
        row = _prepare_fit_row(row, cfg)
        reasons: List[str] = []
        for key in ("reference_h2o_mmol", "h2o_ratio_f", "analyzer_h2o_mmol", "chamber_temp_c"):
            if _safe_float(row.get(key)) is None:
                reasons.append(f"{key}_missing")
        if str(row.get("sample_role") or "").strip().lower() in {"diagnostic", "excluded"}:
            reasons.append("sample_role_not_fit")
        for key in (row.get("point_run_id"), row.get("point_id")):
            point_key = str(key or "").strip()
            if point_key and point_key in point_blocks:
                reasons.append(f"manual_point_block:{point_blocks[point_key]}")
        if reasons:
            rejected.append({**dict(row), "reject_reasons": ";".join(reasons)})
        else:
            complete.append(dict(row))
    return complete, rejected


def _device_status(
    *,
    blocked_reasons: Sequence[str],
    warning_reasons: Sequence[str],
    fit_max_error: Optional[float],
    fit_max_relative_error_pct: Optional[float],
    final_output_pinned: bool,
    cfg: H2OSenco24CandidateConfig,
) -> str:
    if blocked_reasons:
        return "blocked"
    if fit_max_error is not None and fit_max_error > float(cfg.fit_max_abs_error_mmol):
        return "candidate_fit_review_required"
    if (
        fit_max_relative_error_pct is not None
        and fit_max_relative_error_pct > float(cfg.design_max_relative_error_pct)
    ):
        return "candidate_fit_review_required"
    if final_output_pinned:
        return "candidate_ratio_fit_available_but_final_output_blocked"
    if warning_reasons:
        return "candidate_fit_ready_with_warnings_requires_independent_verification"
    return "candidate_fit_ready_requires_independent_verification"


def _manual_block_reasons(cfg: H2OSenco24CandidateConfig) -> Dict[str, str]:
    return {
        _normal_device_id(device_id): str(reason or "").strip()
        for device_id, reason in dict(cfg.manual_device_block_reasons or {}).items()
        if _normal_device_id(device_id) and str(reason or "").strip()
    }


def _manual_point_block_reasons(cfg: H2OSenco24CandidateConfig) -> Dict[str, Dict[str, str]]:
    blocks: Dict[str, Dict[str, str]] = {}
    for key, reason in dict(cfg.manual_point_block_reasons or {}).items():
        reason_text = str(reason or "").strip()
        if not reason_text:
            continue
        text = str(key or "").strip()
        if ":" not in text:
            continue
        device_id, point_key = text.split(":", 1)
        normal_device_id = _normal_device_id(device_id)
        point_key = point_key.strip()
        if not normal_device_id or not point_key:
            continue
        blocks.setdefault(normal_device_id, {})[point_key] = reason_text
    return blocks


def _postwrite_verified_devices(cfg: H2OSenco24CandidateConfig) -> set[str]:
    return {
        _normal_device_id(device_id)
        for device_id in tuple(cfg.postwrite_verified_device_ids or ())
        if _normal_device_id(device_id)
    }


def _component_snapshot_for_device(cfg: H2OSenco24CandidateConfig, device_id: str) -> Mapping[str, Any]:
    snapshots = dict(cfg.component_snapshot or {})
    return dict(snapshots.get(_normal_device_id(device_id)) or {})


def _float_list(value: Any) -> List[float]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return []
    if not isinstance(value, Sequence) or isinstance(value, (bytes, bytearray)):
        return []
    out: List[float] = []
    for item in value:
        number = _safe_float(item)
        if number is None:
            return []
        out.append(float(number))
    return out


def _is_linear_trim_neutral(values: Sequence[float], *, atol: float = 0.05) -> bool:
    return len(values) >= 2 and abs(float(values[0])) <= float(atol) and abs(float(values[1]) - 1.0) <= float(atol)


def _linear_trim_from_snapshot(component_snapshot: Mapping[str, Any], key: str) -> tuple[float, float, str]:
    values = _float_list(component_snapshot.get(key))
    if not values:
        return 0.0, 1.0, "missing_assume_neutral"
    if len(values) < 2 or not math.isfinite(values[0]) or not math.isfinite(values[1]) or abs(values[1]) <= 1.0e-12:
        return 0.0, 1.0, "invalid_final_affine_layer"
    if _is_linear_trim_neutral(values):
        return float(values[0]), float(values[1]), "neutral"
    if values[1] <= 0.0:
        return float(values[0]), float(values[1]), "nonmonotonic_negative_slope_high_risk_final_affine_layer"
    return float(values[0]), float(values[1]), "nonneutral_final_affine_layer_requires_separate_review"


def _first_float(row: Mapping[str, Any], keys: Sequence[str]) -> Optional[float]:
    for key in keys:
        value = _safe_float(row.get(key))
        if value is not None:
            return value
    return None


def _load_state_transfer_summary(
    path: str | Path | None,
    *,
    raw_excess_limit_mmol: float,
    post_s6_relative_limit_pct: float,
) -> Dict[str, Dict[str, Any]]:
    """Load post-write/verification state-transfer evidence by device.

    The source is expected to be an offline CSV such as
    ``h2o_s24_post_s6_state_delta_by_device_point.csv``.  It compares the
    S2/S4 raw replay movement against the live reference H2O movement between
    two physical states.  Large excess movement means the main H2O curve is
    not transferring cleanly; S6 should not be used to hide that failure.
    """

    if not path:
        return {}
    source = Path(path).resolve()
    rows = _read_csv(source)
    by_device: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        device_id = _normal_device_id(
            row.get("analyzer_device_id")
            or row.get("device_id")
            or row.get("id")
            or row.get("analyzer_id")
        )
        if not device_id:
            continue
        excess = _first_float(
            row,
            (
                "raw_replay_delta_minus_reference_delta_mmol",
                "raw_s24_replay_excess_shift_mmol",
                "raw_replay_excess_shift_mmol",
                "senco24_raw_excess_shift_mmol",
            ),
        )
        post_s6_rel = _first_float(
            row,
            (
                "post_existing_s6_abs_rel_pct",
                "post_s6_abs_relative_error_pct",
                "post_s6_rel_error_pct",
                "post_s6_max_abs_relative_error_pct",
            ),
        )
        ratio_delta = _first_float(
            row,
            (
                "h2o_ratio_f_mean_delta_post_minus_s24",
                "h2o_ratio_delta",
                "ratio_delta",
            ),
        )
        temp_delta = _first_float(
            row,
            (
                "chamber_temp_c_mean_delta_post_minus_s24",
                "chamber_temp_delta_c",
                "temperature_delta_c",
            ),
        )
        reference_delta = _first_float(
            row,
            (
                "live_reference_h2o_mmol_delta_post_minus_s24",
                "reference_h2o_delta_mmol",
            ),
        )
        summary = by_device.setdefault(
            device_id,
            {
                "source": str(source),
                "point_count": 0,
                "max_abs_raw_excess_shift_mmol": 0.0,
                "max_abs_post_s6_relative_error_pct": 0.0,
                "max_abs_ratio_delta": 0.0,
                "max_abs_chamber_temp_delta_c": 0.0,
                "max_abs_reference_h2o_delta_mmol": 0.0,
                "worst_point_id": "",
                "worst_reason": "",
                "gate": "not_evaluated",
            },
        )
        summary["point_count"] = int(summary.get("point_count") or 0) + 1
        if excess is not None:
            abs_excess = abs(float(excess))
            if abs_excess > float(summary["max_abs_raw_excess_shift_mmol"]):
                summary["max_abs_raw_excess_shift_mmol"] = abs_excess
                summary["worst_point_id"] = row.get("point_id") or row.get("point_run_id") or ""
                summary["worst_reason"] = "raw_excess_shift"
        if post_s6_rel is not None:
            abs_rel = abs(float(post_s6_rel))
            if abs_rel > float(summary["max_abs_post_s6_relative_error_pct"]):
                summary["max_abs_post_s6_relative_error_pct"] = abs_rel
                if str(summary.get("worst_reason") or "") != "raw_excess_shift":
                    summary["worst_point_id"] = row.get("point_id") or row.get("point_run_id") or ""
                    summary["worst_reason"] = "post_s6_relative_error"
        if ratio_delta is not None:
            summary["max_abs_ratio_delta"] = max(
                float(summary["max_abs_ratio_delta"]),
                abs(float(ratio_delta)),
            )
        if temp_delta is not None:
            summary["max_abs_chamber_temp_delta_c"] = max(
                float(summary["max_abs_chamber_temp_delta_c"]),
                abs(float(temp_delta)),
            )
        if reference_delta is not None:
            summary["max_abs_reference_h2o_delta_mmol"] = max(
                float(summary["max_abs_reference_h2o_delta_mmol"]),
                abs(float(reference_delta)),
            )

    for summary in by_device.values():
        raw_excess = float(summary.get("max_abs_raw_excess_shift_mmol") or 0.0)
        post_s6_rel = float(summary.get("max_abs_post_s6_relative_error_pct") or 0.0)
        failures: List[str] = []
        if raw_excess > float(raw_excess_limit_mmol):
            failures.append("senco24_raw_state_transfer_excess_shift")
        if post_s6_rel > float(post_s6_relative_limit_pct):
            failures.append("post_s6_state_transfer_relative_error_exceeds_limit")
        summary["gate"] = "fail_" + "+".join(failures) if failures else "pass_state_transfer"
    return by_device


def _output_diagnosis(
    *,
    device_id: str,
    manual_block_reason: str,
    final_output_pinned: bool,
    ratio_span: float,
    reference_span: float,
    fit_max_error: Optional[float],
    fit_max_relative_error_pct: Optional[float],
    reported_max_error: Any,
    reported_max_relative_error_pct: Any,
    cfg: H2OSenco24CandidateConfig,
    component_snapshot: Mapping[str, Any],
    state_transfer_summary: Mapping[str, Any] | None = None,
    postwrite_verified: bool = False,
) -> Dict[str, Any]:
    getco2 = _float_list(component_snapshot.get("GETCO2_before"))
    getco4 = _float_list(component_snapshot.get("GETCO4_before"))
    getco6 = _float_list(component_snapshot.get("GETCO6_before"))
    getco6_neutral = _is_linear_trim_neutral(getco6) if getco6 else ""
    getco6_missing_but_required = bool(cfg.require_component_snapshot_for_layer_review) and not getco6
    state_transfer_gate = str((state_transfer_summary or {}).get("gate") or "")
    if manual_block_reason:
        diagnosis = "manual_device_block"
        likely_cause = manual_block_reason
        next_action = "resolve_manual_block_then_regenerate_no_write_candidate_review"
        acceptance = "blocked"
    elif state_transfer_gate.startswith("fail_"):
        diagnosis = "senco24_state_transfer_failed"
        likely_cause = (
            "SENCO2/SENCO4 raw H2O response moved more than the live dewpoint/pressure "
            "reference can explain between calibration and verification states"
        )
        next_action = (
            "do_not_accept_SENCO2_SENCO4_by_training_residual_only;"
            "review_H2O_ratio_raw_ratio_ref_signal_H2O_signal_temperature_and_tubing_state;"
            "recollect_consistent_H2O_open_flow_evidence_or_repair_analyzer_before_write"
        )
        acceptance = "blocked_state_transfer_review_required"
    elif postwrite_verified and final_output_pinned:
        diagnosis = "prewrite_final_h2o_output_pinned_resolved_by_postwrite_verification"
        likely_cause = "prewrite_final_affine_or_output_layer_issue_resolved_by_controlled_write_readback_and_h2o_verification"
        next_action = "keep_postwrite_verification_artifact_attached;do_not_reclassify_old_prewrite_frames_as_current_failure"
        acceptance = "postwrite_verified_for_review_scope"
    elif (
        final_output_pinned
        and ratio_span > 0.001
        and fit_max_error is not None
        and fit_max_error <= float(cfg.fit_max_abs_error_mmol)
        and (
            fit_max_relative_error_pct is None
            or fit_max_relative_error_pct <= float(cfg.design_max_relative_error_pct)
        )
    ):
        if getco6 and bool(getco6_neutral):
            diagnosis = "final_h2o_output_pinned_with_neutral_senco6"
            likely_cause = "existing_SENCO2_SENCO4_main_H2O_coefficients_or_firmware_h2o_chain_suspect;GETCO6_neutral"
            next_action = (
                "prepare_controlled_SENCO2_SENCO4_write_review_using_candidate_payload;"
                "then_short_H2O_open_flow_verification;do_not_CLEARSENCO6"
            )
        elif getco6:
            diagnosis = "final_h2o_output_pinned_with_nonneutral_senco6"
            likely_cause = "final_affine_trim_senco6_suspect"
            next_action = (
                "review_controlled_CLEARSENCO6_or_neutral_SENCO6_first;"
                "then_short_H2O_open_flow_verification_before_SENCO2_SENCO4_write"
            )
        else:
            diagnosis = "final_h2o_output_pinned_but_ratio_model_valid"
            likely_cause = "final_affine_trim_or_firmware_output_layer_suspect;check_GETCO6_for_C1_zero_or_C0_offset"
            next_action = (
                "read_GETCO6_for_this_device_only; if non-neutral_or_C1_near_zero_then_review_controlled_CLEARSENCO6; "
                "after_readback_run_short_H2O_open_flow_verification_before_SENCO2_SENCO4_write"
            )
        acceptance = "blocked_until_final_output_layer_verified"
    elif (
        fit_max_error is not None
        and fit_max_error <= float(cfg.fit_max_abs_error_mmol)
        and (
            fit_max_relative_error_pct is None
            or fit_max_relative_error_pct <= float(cfg.design_max_relative_error_pct)
        )
    ):
        if getco6_missing_but_required or (getco6 and getco6_neutral is False):
            diagnosis = "ratio_temperature_candidate_fit_valid_with_separate_senco6_review_required"
            likely_cause = (
                "missing_GETCO6_snapshot_must_be_read_before_write"
                if getco6_missing_but_required
                else "existing_final_affine_trim_senco6_must_be_reviewed_as_separate_output_layer"
            )
            next_action = (
                "choose_one_layer_contract_before_write:"
                " read_GETCO6_snapshot,"
                " write_SENCO2_SENCO4_as_direct_main_chain,"
                " then review_SENCO6_as_independent_final_affine_trim"
            )
            acceptance = "candidate_ready_requires_layer_contract_review_and_independent_verification"
        else:
            diagnosis = "ratio_temperature_candidate_fit_valid"
            likely_cause = "existing_H2O_coefficients_or_final_trim_need_update_after_independent_verification"
            next_action = "prepare_SENCO2_SENCO4_write_review_after_old_GETCO2_GETCO4_GETCO6_snapshot"
            acceptance = "candidate_ready_requires_independent_verification"
    else:
        diagnosis = "candidate_fit_needs_review"
        likely_cause = "fit_residual_or_evidence_quality_not_sufficient"
        next_action = "review_residuals_and_evidence_before_any_write"
        acceptance = "review_required"
    return {
        "component": "h2o",
        "analyzer_device_id": device_id,
        "diagnosis": diagnosis,
        "likely_cause": likely_cause,
        "next_safe_action": next_action,
        "formal_acceptance_status": acceptance,
        "reference_h2o_span_mmol": reference_span,
        "h2o_ratio_span": ratio_span,
        "fit_max_error_mmol": fit_max_error if fit_max_error is not None else "",
        "fit_max_abs_relative_error_pct": fit_max_relative_error_pct if fit_max_relative_error_pct is not None else "",
        "reported_h2o_max_error_mmol_before_write": reported_max_error,
        "reported_h2o_max_abs_relative_error_pct_before_write": reported_max_relative_error_pct,
        "design_max_relative_error_pct": float(cfg.design_max_relative_error_pct),
        "final_output_pinned": final_output_pinned,
        "postwrite_verified": bool(postwrite_verified),
        "postwrite_verification_artifacts_json": _compact_json(tuple(cfg.postwrite_verification_artifacts or ())),
        "GETCO2_before_json": _compact_json(getco2) if getco2 else "",
        "GETCO4_before_json": _compact_json(getco4) if getco4 else "",
        "GETCO6_before_json": _compact_json(getco6) if getco6 else "",
        "GETCO6_neutral": getco6_neutral,
        "state_transfer_gate": state_transfer_gate,
        "state_transfer_max_abs_raw_excess_shift_mmol": (
            (state_transfer_summary or {}).get("max_abs_raw_excess_shift_mmol", "")
        ),
        "state_transfer_max_abs_post_s6_relative_error_pct": (
            (state_transfer_summary or {}).get("max_abs_post_s6_relative_error_pct", "")
        ),
        "auto_write_allowed": False,
    }


def _device_tables(
    rows: Sequence[Mapping[str, Any]],
    *,
    cfg: H2OSenco24CandidateConfig,
) -> tuple[
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
]:
    excluded = {_normal_device_id(item) for item in cfg.exclude_device_ids}
    manual_blocks = _manual_block_reasons(cfg)
    manual_point_blocks = _manual_point_block_reasons(cfg)
    postwrite_verified_devices = _postwrite_verified_devices(cfg)
    factory_signal_health_source = (
        str(Path(cfg.factory_signal_health_summary_csv).resolve())
        if cfg.factory_signal_health_summary_csv
        else ""
    )
    factory_signal_health_by_device = load_factory_signal_health_summary(
        cfg.factory_signal_health_summary_csv
    )
    state_transfer_source = (
        str(Path(cfg.state_transfer_summary_csv).resolve())
        if cfg.state_transfer_summary_csv
        else ""
    )
    state_transfer_by_device = _load_state_transfer_summary(
        cfg.state_transfer_summary_csv,
        raw_excess_limit_mmol=float(cfg.state_transfer_max_raw_excess_shift_mmol),
        post_s6_relative_limit_pct=float(cfg.state_transfer_max_post_s6_relative_error_pct),
    )
    by_device: Dict[str, List[Mapping[str, Any]]] = {}
    for row in rows:
        device_id = _normal_device_id(row.get("analyzer_device_id"))
        if not device_id or device_id in excluded:
            continue
        by_device.setdefault(device_id, []).append(row)

    policies: List[Dict[str, Any]] = []
    coefficients: List[Dict[str, Any]] = []
    residuals: List[Dict[str, Any]] = []
    payloads: List[Dict[str, Any]] = []
    diagnostics: List[Dict[str, Any]] = []

    for device_id in sorted(by_device):
        device_rows = [dict(row) for row in by_device[device_id]]
        complete, rejected = _complete_rows(
            device_rows,
            cfg=cfg,
            manual_point_blocks=manual_point_blocks.get(device_id),
        )
        blocked: List[str] = []
        warnings: List[str] = []
        manual_block_reason = manual_blocks.get(device_id, "")
        if rejected:
            manual_rejected = [
                row
                for row in rejected
                if "manual_point_block:" in str(row.get("reject_reasons") or "")
            ]
            if manual_rejected:
                warnings.append(f"manual_point_blocks:{len(manual_rejected)}")
        if len(complete) < int(cfg.min_points):
            blocked.append(f"complete_points<{int(cfg.min_points)}")
        pressure_override_count = sum(
            1 for row in complete if row.get("pressure_qc_overridden_for_component_fit") is True
        )
        if pressure_override_count:
            warnings.append(
                f"pressure_qc_failed_overridden_for_component_fit:{pressure_override_count}"
            )
        digital_temp_source_count = sum(
            1 for row in complete if row.get("temperature_source_for_fit") == "digital_thermometer_temp_c"
        )
        if digital_temp_source_count:
            warnings.append(
                f"fit_temperature_source_digital_thermometer:{digital_temp_source_count}"
            )
        digital_temp_missing_count = sum(
            1
            for row in complete
            if str(row.get("temperature_source_warning") or "")
            == "digital_thermometer_missing_fallback_to_analyzer_chamber"
        )
        if digital_temp_missing_count:
            warnings.append(
                f"digital_thermometer_missing_fallback_to_analyzer:{digital_temp_missing_count}"
            )

        reference_values = [float(row["reference_h2o_mmol"]) for row in complete]
        output_values = [float(row["analyzer_h2o_mmol"]) for row in complete]
        ratio_values = [float(row["h2o_ratio_f"]) for row in complete]
        temp_values = [float(row["chamber_temp_c"]) for row in complete]
        complete_dry_anchor_count = sum(
            1 for row in complete if str(row.get("sample_role") or "").strip().lower() == "dry_anchor"
        )
        complete_wet_point_count = len(complete) - complete_dry_anchor_count
        if complete_wet_point_count < int(cfg.min_wet_points):
            blocked.append(f"wet_points<{int(cfg.min_wet_points)}")
        box_temp_values = [
            float(value)
            for value in (_safe_float(row.get("digital_thermometer_temp_c")) for row in complete)
            if value is not None
        ]
        final_output_pinned = (
            _span(reference_values) >= float(cfg.final_output_pin_min_reference_span_mmol)
            and _span(output_values) <= float(cfg.final_output_pin_span_mmol)
        )
        postwrite_verified = device_id in postwrite_verified_devices
        effective_final_output_pinned = final_output_pinned and not postwrite_verified
        if final_output_pinned and postwrite_verified:
            warnings.append("prewrite_final_h2o_output_pinned_resolved_by_postwrite_verification")
        elif final_output_pinned:
            warnings.append("analyzer_final_h2o_output_pinned;ratio_fit_is_diagnostic_until_write_verification")

        side_channel_warn = any(
            (_safe_float(row.get("sampling_time_alignment_max_age_ms")) or 0.0)
            > float(cfg.stale_side_channel_age_ms)
            or str(row.get("sample_alignment_status") or "") == "warn"
            for row in complete
        )
        if side_channel_warn:
            warnings.append("side_channel_cache_age_warning_kept_as_evidence_not_fit_blocker")

        component_snapshot = _component_snapshot_for_device(cfg, device_id)
        getco6 = _float_list(component_snapshot.get("GETCO6_before"))
        h2o_final_c0, h2o_final_c1, h2o_final_layer_status = _linear_trim_from_snapshot(
            component_snapshot,
            "GETCO6_before",
        )
        if h2o_final_layer_status == "invalid_final_affine_layer":
            blocked.append("existing_GETCO6_invalid")
        elif h2o_final_layer_status not in {"missing_assume_neutral", "neutral"}:
            warnings.append(f"existing_GETCO6_{h2o_final_layer_status}_separate_layer_review_required")
        missing_required_layer_snapshot = (
            bool(cfg.require_component_snapshot_for_layer_review)
            and h2o_final_layer_status == "missing_assume_neutral"
        )
        if missing_required_layer_snapshot:
            warnings.append("GETCO6_missing_component_snapshot_separate_layer_review_required")
        state_transfer_summary = state_transfer_by_device.get(device_id, {})
        state_transfer_gate = str(state_transfer_summary.get("gate") or "")
        if state_transfer_gate.startswith("fail_"):
            blocked.append(state_transfer_gate)
        elif state_transfer_gate == "pass_state_transfer":
            warnings.append("state_transfer_evidence_passed")

        terms = tuple(cfg.terms)
        fit_metrics: Dict[str, Any] = {}
        rank: Any = ""
        condition: Any = ""
        coeff_by_term: Dict[str, float] = {}
        fit_solver_metadata: Dict[str, Any] = {}
        monotonicity: Dict[str, Any] = {}
        if not blocked:
            y_final = np.asarray(reference_values, dtype=float)
            y_raw = np.asarray(reference_values, dtype=float)
            coeffs, rank, condition, fit_solver_metadata = _fit_coefficients(
                complete,
                terms,
                y_raw,
                cfg,
            )
            if rank < len(terms):
                blocked.append("model_matrix_rank_deficient")
            elif not math.isfinite(float(condition)) or float(condition) > float(cfg.max_condition_number):
                blocked.append("model_matrix_ill_conditioned")
            else:
                coeff_by_term = {term: float(value) for term, value in zip(terms, coeffs)}
                monotonicity = _monotonicity_audit(complete, coeff_by_term)
                if (
                    str(cfg.fit_objective or "").strip().lower()
                    == MINIMAX_RELATIVE_MONOTONIC_OBJECTIVE
                    and not bool(monotonicity["monotonic_decreasing_over_fit_envelope"])
                ):
                    blocked.append("minimax_model_not_monotonic_after_firmware_quantization")
                pred_raw = np.asarray([_prediction(row, coeff_by_term, terms) for row in complete], dtype=float)
                pred = pred_raw
                fit_metrics = compute_metrics(y_final, pred)
                for row, y_pred_raw, y_pred in zip(complete, pred_raw, pred):
                    target = float(row["reference_h2o_mmol"])
                    measured = float(row["analyzer_h2o_mmol"])
                    model_error_pct = _relative_error_pct(
                        float(y_pred),
                        target,
                        min_reference_mmol=float(cfg.relative_error_min_reference_mmol),
                    )
                    reported_error_pct = _relative_error_pct(
                        measured,
                        target,
                        min_reference_mmol=float(cfg.relative_error_min_reference_mmol),
                    )
                    residuals.append(
                        {
                            "component": "h2o",
                            "analyzer_prefix": row.get("analyzer_prefix", ""),
                            "analyzer_device_id": device_id,
                            "point_run_id": row.get("point_run_id", ""),
                            "point_id": row.get("point_id", ""),
                            "reference_h2o_mmol": target,
                            "raw_fit_target_h2o_mmol": target,
                            "raw_model_pred_h2o_mmol": float(y_pred_raw),
                            "model_pred_h2o_mmol": float(y_pred),
                            "model_error_mmol": float(y_pred - target),
                            "model_error_pct": model_error_pct if model_error_pct is not None else "",
                            "analyzer_reported_h2o_mmol": measured,
                            "analyzer_reported_error_mmol": float(measured - target),
                            "analyzer_reported_error_pct": reported_error_pct if reported_error_pct is not None else "",
                            "h2o_ratio_f": row.get("h2o_ratio_f", ""),
                            "chamber_temp_c": row.get("chamber_temp_c", ""),
                            "analyzer_chamber_temp_c_raw": row.get("analyzer_chamber_temp_c_raw", ""),
                            "digital_thermometer_temp_c": row.get("digital_thermometer_temp_c", ""),
                            "temperature_source_for_fit": row.get("temperature_source_for_fit", ""),
                            "temperature_source_warning": row.get("temperature_source_warning", ""),
                            "temperature_source_override_for_post_temperature_calibration_review": row.get(
                                "temperature_source_override_for_post_temperature_calibration_review", ""
                            ),
                            "pressure_qc_overridden_for_component_fit": row.get(
                                "pressure_qc_overridden_for_component_fit", ""
                            ),
                            "fit_input_override_reason": row.get("fit_input_override_reason", ""),
                            "sample_mode2_qc_status": row.get("sample_mode2_qc_status", ""),
                            "sample_mode2_qc_reason": row.get("sample_mode2_qc_reason", ""),
                            "sample_frame_status": row.get("sample_frame_status", ""),
                            "reference_dewpoint_c": row.get("reference_dewpoint_c", ""),
                            "reference_pressure_hpa": row.get("reference_pressure_hpa", ""),
                            "nominal_plan_h2o_mmol": row.get("nominal_plan_h2o_mmol", ""),
                            "sample_role": row.get("sample_role", ""),
                            "reference_source": row.get("reference_source", ""),
                            "h2o_anchor_class": row.get("h2o_anchor_class", ""),
                            "sample_alignment_status": row.get("sample_alignment_status", ""),
                            "sample_alignment_failure_reasons": row.get("sample_alignment_failure_reasons", ""),
                            "senco24_main_chain_contract": SENCO24_MAIN_CHAIN_CONTRACT,
                            "senco6_layer_contract": SENCO6_SEPARATE_LAYER_CONTRACT,
                            "fit_objective": str(cfg.fit_objective or "absolute_mmol"),
                            "fit_solver": fit_solver_metadata.get("fit_solver", ""),
                            "fit_basis": fit_solver_metadata.get("fit_basis", ""),
                            "coefficients_quantized_for_firmware": fit_solver_metadata.get(
                                "coefficients_quantized_for_firmware", False
                            ),
                        }
                    )
                for term in terms:
                    primary_senco = "SENCO2" if term in {"intercept", "R", "R2", "R3"} else "SENCO4"
                    coefficients.append(
                        {
                            "component": "h2o",
                            "analyzer_prefix": complete[0].get("analyzer_prefix", ""),
                            "analyzer_device_id": device_id,
                            "term": term,
                            "coefficient": coeff_by_term[term],
                            "senco_channel": primary_senco,
                            "pressure_term": False,
                            "pressure_terms_frozen": True,
                            "formatted_senco_value": format_senco_values([coeff_by_term[term]])[0],
                            "fit_objective": str(cfg.fit_objective or "absolute_mmol"),
                            "fit_solver": fit_solver_metadata.get("fit_solver", ""),
                            "fit_basis": fit_solver_metadata.get("fit_basis", ""),
                            "coefficients_quantized_for_firmware": fit_solver_metadata.get(
                                "coefficients_quantized_for_firmware", False
                            ),
                        }
                    )
                primary = [coeff_by_term[term] for term in ("intercept", "R", "R2", "R3")] + [0.0, 0.0]
                secondary = [coeff_by_term[term] for term in ("T", "T2", "RT")] + [0.0, 0.0, 0.0]
                payloads.append(
                    {
                        "component": "h2o",
                        "analyzer_prefix": complete[0].get("analyzer_prefix", ""),
                        "analyzer_device_id": device_id,
                        "primary_senco": "SENCO2",
                        "secondary_senco": "SENCO4",
                        "senco2_payload_values_json": _compact_json(primary),
                        "senco4_payload_values_json": _compact_json(secondary),
                        "senco2_command_preview": "SENCO2,YGAS,FFF," + ",".join(format_senco_values(primary)),
                        "senco4_command_preview": "SENCO4,YGAS,FFF," + ",".join(format_senco_values(secondary)),
                        "auto_write_allowed": False,
                        "fit_objective": str(cfg.fit_objective or "absolute_mmol"),
                        "fit_solver": fit_solver_metadata.get("fit_solver", ""),
                        "fit_basis": fit_solver_metadata.get("fit_basis", ""),
                        "coefficients_quantized_for_firmware": fit_solver_metadata.get(
                            "coefficients_quantized_for_firmware", False
                        ),
                        "senco24_main_chain_contract": SENCO24_MAIN_CHAIN_CONTRACT,
                        "senco6_layer_contract": SENCO6_SEPARATE_LAYER_CONTRACT,
                        "write_requires": (
                            "old_GETCO2_GETCO4_backup;reviewer_approval;controlled_write;"
                            "readback;independent_H2O_verification;separate_SENCO6_review_if_non_neutral"
                        ),
                    }
                )
        fit_max_error = _safe_float(fit_metrics.get("MaxError")) if fit_metrics else None
        fit_max_relative_error_pct = None
        if coeff_by_term:
            fit_predictions = [
                _prediction(row, coeff_by_term, terms)
                for row in complete
            ]
            fit_relative = _max_abs_relative_error_pct(
                fit_predictions,
                reference_values,
                min_reference_mmol=float(cfg.relative_error_min_reference_mmol),
            )
            fit_max_relative_error_pct = _safe_float(fit_relative)
        raw_errors = [out - ref for out, ref in zip(output_values, reference_values)]
        raw_rmse = math.sqrt(mean([error * error for error in raw_errors])) if raw_errors else ""
        raw_max_error = max((abs(error) for error in raw_errors), default="")
        raw_max_relative_error = _max_abs_relative_error_pct(
            output_values,
            reference_values,
            min_reference_mmol=float(cfg.relative_error_min_reference_mmol),
        )
        if manual_block_reason:
            blocked.append(f"manual_device_block:{manual_block_reason}")
        factory_signal_health_row = (
            factory_signal_health_by_device.get(device_id)
            if factory_signal_health_source
            else None
        )
        factory_signal_health_reason = factory_signal_health_block_reason(
            factory_signal_health_row,
            summary_was_required=bool(factory_signal_health_source),
        )
        if factory_signal_health_reason:
            blocked.append(factory_signal_health_reason)
        diagnostics.append(
            _output_diagnosis(
                device_id=device_id,
                manual_block_reason=manual_block_reason,
                final_output_pinned=final_output_pinned,
                ratio_span=_span(ratio_values),
                reference_span=_span(reference_values),
                fit_max_error=fit_max_error,
                fit_max_relative_error_pct=fit_max_relative_error_pct,
                reported_max_error=raw_max_error,
                reported_max_relative_error_pct=raw_max_relative_error,
                cfg=cfg,
                component_snapshot=component_snapshot,
                state_transfer_summary=state_transfer_summary,
                postwrite_verified=postwrite_verified,
            )
        )
        policies.append(
            {
                "component": "h2o",
                "analyzer_prefix": complete[0].get("analyzer_prefix", "") if complete else "",
                "analyzer_device_id": device_id,
                "candidate_status": _device_status(
                    blocked_reasons=blocked,
                    warning_reasons=warnings,
                    fit_max_error=fit_max_error,
                    fit_max_relative_error_pct=fit_max_relative_error_pct,
                    final_output_pinned=effective_final_output_pinned,
                    cfg=cfg,
                ),
                "blocked_reasons": ";".join(dict.fromkeys(blocked)),
                "warning_reasons": ";".join(dict.fromkeys(warnings)),
                "complete_point_count": len(complete),
                "complete_wet_point_count": complete_wet_point_count,
                "min_wet_points": int(cfg.min_wet_points),
                "complete_dry_anchor_count": complete_dry_anchor_count,
                "rejected_point_count": len(rejected),
                "reference_target_source": "ppm_H2O_Dew_from_dewpoint_meter_and_COM22_pressure",
                "reference_h2o_span_mmol": _span(reference_values),
                "analyzer_reported_h2o_span_mmol": _span(output_values),
                "h2o_ratio_span": _span(ratio_values),
                "chamber_temp_span_c": _span(temp_values),
                "digital_thermometer_temp_span_c": _span(box_temp_values),
                "selected_model_terms": ";".join(terms),
                "frozen_terms": ";".join(PRESSURE_TERMS),
                "fit_temperature_source": str(cfg.fit_temperature_source or "analyzer_chamber"),
                "pressure_qc_override_allowed": device_id in _allowed_pressure_qc_failed_device_ids(cfg),
                "pressure_qc_override_point_count": pressure_override_count,
                "digital_thermometer_temperature_fit_point_count": digital_temp_source_count,
                "digital_thermometer_missing_fallback_point_count": digital_temp_missing_count,
                "fit_strategy": "direct_reference_target_fit_SENCO2_SENCO4_SENCO6_separate",
                "fit_objective": str(cfg.fit_objective or "absolute_mmol"),
                "fit_solver": fit_solver_metadata.get("fit_solver", ""),
                "fit_basis": fit_solver_metadata.get("fit_basis", ""),
                "fit_objective_bound_pct_pre_firmware_quantization": fit_solver_metadata.get(
                    "fit_objective_bound_pct_pre_firmware_quantization", ""
                ),
                "coefficients_quantized_for_firmware": fit_solver_metadata.get(
                    "coefficients_quantized_for_firmware", False
                ),
                "fit_ratio_center": fit_solver_metadata.get("ratio_center", ""),
                "fit_temp_k_center": fit_solver_metadata.get("temp_k_center", ""),
                "monotonic_grid_point_count": monotonicity.get("monotonic_grid_point_count", ""),
                "monotonic_derivative_violation_count": monotonicity.get(
                    "monotonic_derivative_violation_count", ""
                ),
                "minimum_d_concentration_d_ratio": monotonicity.get(
                    "minimum_d_concentration_d_ratio", ""
                ),
                "maximum_d_concentration_d_ratio": monotonicity.get(
                    "maximum_d_concentration_d_ratio", ""
                ),
                "monotonic_decreasing_over_fit_envelope": monotonicity.get(
                    "monotonic_decreasing_over_fit_envelope", ""
                ),
                "senco24_main_chain_contract": SENCO24_MAIN_CHAIN_CONTRACT,
                "senco6_layer_contract": SENCO6_SEPARATE_LAYER_CONTRACT,
                "senco24_write_candidate": h2o_final_layer_status == "neutral"
                or (
                    h2o_final_layer_status == "missing_assume_neutral"
                    and not missing_required_layer_snapshot
                ),
                "senco6_separate_review_required": missing_required_layer_snapshot
                or h2o_final_layer_status not in {"missing_assume_neutral", "neutral"},
                "require_component_snapshot_for_layer_review": bool(
                    cfg.require_component_snapshot_for_layer_review
                ),
                "GETCO6_C0": h2o_final_c0,
                "GETCO6_C1": h2o_final_c1,
                "GETCO6_layer_status": h2o_final_layer_status,
                "matrix_rank": rank,
                "matrix_condition_number": condition,
                "fit_rmse_mmol": fit_metrics.get("RMSE", ""),
                "fit_max_error_mmol": fit_metrics.get("MaxError", ""),
                "fit_max_abs_relative_error_pct": fit_max_relative_error_pct if fit_max_relative_error_pct is not None else "",
                "fit_bias_mmol": fit_metrics.get("Bias", ""),
                "fit_r2": fit_metrics.get("R2", ""),
                "reported_h2o_rmse_mmol_before_write": raw_rmse,
                "reported_h2o_max_error_mmol_before_write": raw_max_error,
                "reported_h2o_max_abs_relative_error_pct_before_write": raw_max_relative_error,
                "design_max_relative_error_pct": float(cfg.design_max_relative_error_pct),
                "relative_error_min_reference_mmol": float(cfg.relative_error_min_reference_mmol),
                "fit_design_qc": (
                    "pass"
                    if fit_max_relative_error_pct is not None
                    and fit_max_relative_error_pct <= float(cfg.design_max_relative_error_pct)
                    else "review"
                    if fit_max_relative_error_pct is not None
                    else ""
                ),
                "final_output_pinned": final_output_pinned,
                "postwrite_verified": postwrite_verified,
                "postwrite_verification_artifacts_json": _compact_json(tuple(cfg.postwrite_verification_artifacts or ())),
                "factory_signal_health_source": factory_signal_health_source,
                "factory_signal_health_gate": (
                    str((factory_signal_health_row or {}).get("candidate_gate") or "").strip()
                    if factory_signal_health_row is not None
                    else ("factory_signal_health_missing_device" if factory_signal_health_source else "")
                ),
                "factory_signal_health_point_count": (factory_signal_health_row or {}).get("point_count", ""),
                "factory_signal_health_review_point_count": (factory_signal_health_row or {}).get("review_point_count", ""),
                "factory_signal_health_blocking_point_count": (factory_signal_health_row or {}).get("blocking_point_count", ""),
                "factory_signal_health_high_ref_point_count": (factory_signal_health_row or {}).get("high_ref_point_count", ""),
                "state_transfer_source": state_transfer_source,
                "state_transfer_gate": state_transfer_gate,
                "state_transfer_point_count": state_transfer_summary.get("point_count", ""),
                "state_transfer_max_abs_raw_excess_shift_mmol": state_transfer_summary.get(
                    "max_abs_raw_excess_shift_mmol", ""
                ),
                "state_transfer_max_raw_excess_shift_limit_mmol": float(
                    cfg.state_transfer_max_raw_excess_shift_mmol
                ),
                "state_transfer_max_abs_post_s6_relative_error_pct": state_transfer_summary.get(
                    "max_abs_post_s6_relative_error_pct", ""
                ),
                "state_transfer_max_post_s6_relative_error_limit_pct": float(
                    cfg.state_transfer_max_post_s6_relative_error_pct
                ),
                "state_transfer_max_abs_ratio_delta": state_transfer_summary.get(
                    "max_abs_ratio_delta", ""
                ),
                "state_transfer_max_abs_chamber_temp_delta_c": state_transfer_summary.get(
                    "max_abs_chamber_temp_delta_c", ""
                ),
                "state_transfer_max_abs_reference_h2o_delta_mmol": state_transfer_summary.get(
                    "max_abs_reference_h2o_delta_mmol", ""
                ),
                "state_transfer_worst_point_id": state_transfer_summary.get("worst_point_id", ""),
                "state_transfer_worst_reason": state_transfer_summary.get("worst_reason", ""),
                "auto_write_allowed": False,
                "physical_scope": "open_flow_H2O_ratio_temperature_candidate_fit",
                "not_pressure_compensation_fit": True,
            }
        )
        for row in rejected:
            residuals.append(
                {
                    "component": "h2o",
                    "analyzer_prefix": row.get("analyzer_prefix", ""),
                    "analyzer_device_id": device_id,
                    "point_run_id": row.get("point_run_id", ""),
                    "point_id": row.get("point_id", ""),
                    "reject_reasons": row.get("reject_reasons", ""),
                    "sample_role": row.get("sample_role", ""),
                    "reference_source": row.get("reference_source", ""),
                    "h2o_anchor_class": row.get("h2o_anchor_class", ""),
                    "temperature_source_for_fit": row.get("temperature_source_for_fit", ""),
                    "pressure_qc_overridden_for_component_fit": row.get(
                        "pressure_qc_overridden_for_component_fit", ""
                    ),
                    "fit_input_override_reason": row.get("fit_input_override_reason", ""),
                    "sample_mode2_qc_status": row.get("sample_mode2_qc_status", ""),
                    "sample_mode2_qc_reason": row.get("sample_mode2_qc_reason", ""),
                    "residual_role": "rejected_input",
                }
            )
    return policies, coefficients, residuals, payloads, diagnostics


def _contract_rows() -> List[Dict[str, Any]]:
    return [
        {
            "topic": "reference_target",
            "contract": "H2O fit target is dewpoint-meter H2O derived with COM22/sample pressure, not humidity-generator nominal setpoint.",
            "physical_meaning": "The humidity generator defines the requested condition; the dewpoint meter plus pressure reference defines the actual water-vapor amount that reached the measurement chain.",
        },
        {
            "topic": "dry_anchor",
            "contract": "Optional CO2-route zero-gas dry anchors may enter H2O fitting only as low-water anchors with dewpoint/pressure-derived residual H2O; their target is never forced to zero.",
            "physical_meaning": "Wet H2O points define the humidity response, while dry-gas anchors constrain the low-water baseline/intercept needed for dry gas and CO2 dry/wet correction.",
        },
        {
            "topic": "wet_point_minimum",
            "contract": "Dry-gas anchors cannot satisfy the H2O humidity-response evidence requirement by themselves; each device needs enough true wet open-flow points before write review.",
            "physical_meaning": "A low-water baseline does not prove the analyzer response to humid gas across the water route, adsorption/desorption state, and temperature span.",
        },
        {
            "topic": "analyzer_inputs",
            "contract": "Fit uses factory-mode R_H2O and the configured temperature evidence source; pressure terms P/RP/RTP are frozen.",
            "physical_meaning": "Pressure P was handled by the independent pressure-channel workflow. A pressure-channel failure may be retained as a warning while still allowing H2O ratio/temperature evidence to support component fitting when explicitly authorized.",
        },
        {
            "topic": "temperature_evidence",
            "contract": "The default fit temperature is analyzer chamber T1; post-temperature-repair reviews may use digital thermometer evidence as the fit temperature source when analyzer T was known bad during acquisition.",
            "physical_meaning": "The analyzer formula needs temperature as an input. If old SENCO7/SENCO8 made analyzer temperature physically impossible, the traceable digital thermometer preserves the physical temperature state needed for a repair review.",
        },
        {
            "topic": "senco_mapping",
            "contract": "H2O primary model maps to SENCO2 intercept/R/R2/R3 and secondary model maps to SENCO4 T/T2/RT with pressure slots zero.",
            "physical_meaning": "SENCO2/SENCO4 describe the H2O optical ratio and temperature chain; SENCO6 is only final affine output trim and is not the main H2O calibration model.",
        },
        {
            "topic": "state_transfer_gate",
            "contract": "When post-write or reverify state-transfer evidence is provided, a SENCO2/SENCO4 candidate must remain physically transferable between calibration and verification states.",
            "physical_meaning": "If the raw S2/S4 replay movement is much larger than the live dewpoint/pressure reference movement, the main H2O response surface is not stable. SENCO6 may trim final display, but it must not hide a non-transferable main curve.",
        },
        {
            "topic": "write_boundary",
            "contract": "This package is no-write. Any write requires old GETCO2/GETCO4 backup, reviewer approval, slow controlled write, readback, and independent H2O verification.",
            "physical_meaning": "Coefficient writing changes the analyzer measurement model and cannot be accepted from offline fit alone.",
        },
    ]


def _database_sidecar_rows(policies: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    suggested: List[Dict[str, Any]] = []
    for row in policies:
        device_id = str(row.get("analyzer_device_id") or "").strip()
        status = str(row.get("candidate_status") or "")
        suggested.append(
            {
                "db_table": "coefficient_candidates",
                "record_key": f"h2o_senco2_senco4_candidate_{device_id}",
                "component": "h2o",
                "analyzer_device_id": device_id,
                "candidate_status": status,
                "auto_write_allowed": False,
                "fit_rmse": row.get("fit_rmse_mmol", ""),
                "fit_max_error": row.get("fit_max_error_mmol", ""),
                "fit_max_abs_relative_error_pct": row.get("fit_max_abs_relative_error_pct", ""),
                "design_max_relative_error_pct": row.get("design_max_relative_error_pct", ""),
                "blocked_reasons": row.get("blocked_reasons", ""),
                "warning_reasons": row.get("warning_reasons", ""),
            }
        )
        suggested.append(
            {
                "db_table": "qc_results",
                "record_key": f"h2o_senco2_senco4_qc_{device_id}",
                "component": "h2o",
                "analyzer_device_id": device_id,
                "status": (
                    "fail"
                    if status in {"blocked", "candidate_fit_review_required"}
                    or "final_output_blocked" in status
                    else "warn"
                    if "blocked" in status or "warning" in status
                    else "pass"
                ),
                "subject": "h2o_candidate_fit",
                "reason": row.get("blocked_reasons") or row.get("warning_reasons") or "fit_generated_no_write",
            }
        )
    return {
        "no_write": True,
        "opens_com_ports": False,
        "connects_postgresql": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "database_target_tables": ["coefficient_candidates", "qc_results", "reports"],
        "suggested_rows": suggested,
    }


def build_h2o_senco24_candidate_tables(
    *,
    run_dir: str | Path,
    cfg: Optional[H2OSenco24CandidateConfig] = None,
) -> tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any]]:
    """Build no-write H2O SENCO2/SENCO4 candidate review tables."""

    config = cfg or H2OSenco24CandidateConfig()
    root = _resolve_run_root(run_dir)
    h2o_roots = [root] + [_resolve_run_root(path) for path in config.additional_h2o_roots]
    wet_point_inputs: List[Dict[str, Any]] = []
    for h2o_root in h2o_roots:
        wet_point_inputs.extend(_point_rows(h2o_root))
    dry_anchor_roots = [_resolve_dry_anchor_root(path) for path in config.dry_anchor_roots]
    dry_anchor_inputs: List[Dict[str, Any]] = []
    for dry_anchor_root in dry_anchor_roots:
        dry_anchor_inputs.extend(
            _dry_anchor_rows(
                dry_anchor_root,
                min_temp_c=config.dry_anchor_min_temp_c,
                max_temp_c=config.dry_anchor_max_temp_c,
            )
        )
    point_inputs = wet_point_inputs + dry_anchor_inputs
    policies, coefficients, residuals, payloads, diagnostics = _device_tables(point_inputs, cfg=config)
    device_count = len(policies)
    blocked_count = sum(1 for row in policies if str(row.get("candidate_status")) == "blocked")
    pinned_count = sum(1 for row in policies if row.get("final_output_pinned") is True)
    ready_count = sum(
        1
        for row in policies
        if str(row.get("candidate_status") or "").startswith("candidate_fit_ready")
    )
    if blocked_count and pinned_count and ready_count:
        run_status = "fit_ready_with_blocked_and_final_output_blocked_devices_requires_review"
    elif blocked_count and ready_count:
        run_status = "fit_ready_with_blocked_devices_requires_review"
    elif blocked_count:
        run_status = "blocked"
    elif pinned_count and ready_count:
        run_status = "fit_ready_with_device_final_output_blocked_requires_review"
    elif pinned_count and ready_count == 0:
        run_status = "ratio_fit_available_but_final_output_review_required"
    else:
        run_status = "fit_ready_requires_independent_verification"
    summary = [
        {
            "component": "h2o",
            "run_status": run_status,
            "run_dir": str(root),
            "additional_h2o_roots": ";".join(str(path) for path in h2o_roots[1:]),
            "point_input_count": len(point_inputs),
            "wet_point_input_count": len(wet_point_inputs),
            "dry_anchor_input_count": len(dry_anchor_inputs),
            "dry_anchor_min_temp_c": config.dry_anchor_min_temp_c,
            "dry_anchor_max_temp_c": config.dry_anchor_max_temp_c,
            "point_count": len({row.get("point_run_id") for row in point_inputs}),
            "wet_point_count": len({row.get("point_run_id") for row in wet_point_inputs}),
            "dry_anchor_point_count": len({row.get("point_run_id") for row in dry_anchor_inputs}),
            "dry_anchor_roots": ";".join(str(path) for path in dry_anchor_roots),
            "device_count": device_count,
            "blocked_device_count": blocked_count,
            "final_output_pinned_device_count": pinned_count,
            "ready_device_count": ready_count,
            "allow_pressure_qc_failed_device_ids": ";".join(
                sorted(_allowed_pressure_qc_failed_device_ids(config))
            ),
            "fit_temperature_source": str(config.fit_temperature_source or "analyzer_chamber"),
            "fit_objective": str(config.fit_objective or "absolute_mmol"),
            "require_component_snapshot_for_layer_review": bool(
                config.require_component_snapshot_for_layer_review
            ),
            "factory_signal_health_source": (
                str(Path(config.factory_signal_health_summary_csv).resolve())
                if config.factory_signal_health_summary_csv
                else ""
            ),
            "state_transfer_source": (
                str(Path(config.state_transfer_summary_csv).resolve())
                if config.state_transfer_summary_csv
                else ""
            ),
            "state_transfer_device_count": len(
                _load_state_transfer_summary(
                    config.state_transfer_summary_csv,
                    raw_excess_limit_mmol=float(config.state_transfer_max_raw_excess_shift_mmol),
                    post_s6_relative_limit_pct=float(config.state_transfer_max_post_s6_relative_error_pct),
                )
            )
            if config.state_transfer_summary_csv
            else 0,
            "state_transfer_max_raw_excess_shift_limit_mmol": float(
                config.state_transfer_max_raw_excess_shift_mmol
            ),
            "state_transfer_max_post_s6_relative_error_limit_pct": float(
                config.state_transfer_max_post_s6_relative_error_pct
            ),
            "reference_target_contract": "dewpoint_meter_plus_COM22_pressure_ppm_H2O_Dew",
            "selected_model_terms": ";".join(config.terms),
            "frozen_terms": ";".join(PRESSURE_TERMS),
            "design_max_relative_error_pct": float(config.design_max_relative_error_pct),
            "relative_error_min_reference_mmol": float(config.relative_error_min_reference_mmol),
            "min_wet_points": int(config.min_wet_points),
            "auto_write_allowed": False,
            "opens_com_ports": False,
            "connects_postgresql": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "physical_meaning": (
                "H2O candidate fitting uses clean open-flow water-vapor evidence. Wet H2O points cover the "
                "humidity response, and dry-gas anchors constrain the low-water baseline when provided. "
                "The target is the actual dewpoint/pressure-derived H2O amount, while humidity-generator setpoints "
                "remain process evidence."
            ),
        }
    ]
    tables = {
        "h2o_senco24_review_summary": summary,
        "h2o_senco24_device_policy": policies,
        "h2o_senco24_coefficients": coefficients,
        "h2o_senco24_payload_preview": payloads,
        "h2o_senco24_residuals": residuals,
        "h2o_senco24_output_diagnostics": diagnostics,
        "h2o_senco24_point_inputs": point_inputs,
        "h2o_senco24_measurement_contract": _contract_rows(),
    }
    context = {
        "run_status": run_status,
        "run_dir": str(root),
        "additional_h2o_roots": [str(path) for path in h2o_roots[1:]],
        "device_count": device_count,
        "point_count": summary[0]["point_count"],
        "wet_point_input_count": len(wet_point_inputs),
        "dry_anchor_input_count": len(dry_anchor_inputs),
        "dry_anchor_roots": [str(path) for path in dry_anchor_roots],
        "dry_anchor_min_temp_c": config.dry_anchor_min_temp_c,
        "dry_anchor_max_temp_c": config.dry_anchor_max_temp_c,
        "allow_pressure_qc_failed_device_ids": sorted(_allowed_pressure_qc_failed_device_ids(config)),
        "fit_temperature_source": str(config.fit_temperature_source or "analyzer_chamber"),
        "fit_objective": str(config.fit_objective or "absolute_mmol"),
        "require_component_snapshot_for_layer_review": bool(
            config.require_component_snapshot_for_layer_review
        ),
        "factory_signal_health_source": (
            str(Path(config.factory_signal_health_summary_csv).resolve())
            if config.factory_signal_health_summary_csv
            else ""
        ),
        "state_transfer_source": (
            str(Path(config.state_transfer_summary_csv).resolve())
            if config.state_transfer_summary_csv
            else ""
        ),
        "state_transfer_max_raw_excess_shift_limit_mmol": float(
            config.state_transfer_max_raw_excess_shift_mmol
        ),
        "state_transfer_max_post_s6_relative_error_limit_pct": float(
            config.state_transfer_max_post_s6_relative_error_pct
        ),
    }
    return tables, context


def _write_markdown_report(destination: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> Path:
    summary = (tables.get("h2o_senco24_review_summary") or [{}])[0]
    policies = list(tables.get("h2o_senco24_device_policy") or [])
    diagnostics = list(tables.get("h2o_senco24_output_diagnostics") or [])
    report_path = destination / "h2o_senco24_candidate_review.md"
    lines = [
        "# V1.5 H2O SENCO2/SENCO4 Candidate Review",
        "",
        f"- Status: {summary.get('run_status', '')}",
        f"- Points: {summary.get('point_count', '')}",
        f"- Wet point inputs: {summary.get('wet_point_input_count', '')}",
        f"- Dry anchor inputs: {summary.get('dry_anchor_input_count', '')}",
        f"- Dry anchor temperature filter: min={summary.get('dry_anchor_min_temp_c', '')}, max={summary.get('dry_anchor_max_temp_c', '')}",
        f"- Devices: {summary.get('device_count', '')}",
        f"- Fit temperature source: {summary.get('fit_temperature_source', 'analyzer_chamber')}",
        f"- Fit objective: {summary.get('fit_objective', 'absolute_mmol')}",
        f"- Pressure-QC override devices: {summary.get('allow_pressure_qc_failed_device_ids', '')}",
        f"- Require component snapshot for S6 layer review: {summary.get('require_component_snapshot_for_layer_review', False)}",
        f"- State-transfer evidence: {summary.get('state_transfer_source', '')}",
        "- Boundary: offline/no-write review only; no COM ports, no water/gas route control, no SENCO write.",
        "",
        "## Device Summary",
        "",
        "| Device ID | Status | Fit RMSE | Fit Max Error | Fit Max Rel % | Design Limit % | State Transfer | Reported Max Error Before Write | Terms | Warnings | Blockers |",
        "| --- | --- | ---: | ---: | ---: | ---: | --- | ---: | --- | --- | --- |",
    ]
    for row in policies:
        lines.append(
            "| {device} | {status} | {rmse} | {max_error} | {max_rel} | {limit} | {transfer} | {raw_max} | {terms} | {warnings} | {blockers} |".format(
                device=row.get("analyzer_device_id", ""),
                status=row.get("candidate_status", ""),
                rmse=row.get("fit_rmse_mmol", ""),
                max_error=row.get("fit_max_error_mmol", ""),
                max_rel=row.get("fit_max_abs_relative_error_pct", ""),
                limit=row.get("design_max_relative_error_pct", ""),
                transfer=row.get("state_transfer_gate", ""),
                raw_max=row.get("reported_h2o_max_error_mmol_before_write", ""),
                terms=row.get("selected_model_terms", ""),
                warnings=row.get("warning_reasons", ""),
                blockers=row.get("blocked_reasons", ""),
            )
        )
    lines.extend(
        [
            "",
            "## Output Diagnostics",
            "",
            "| Device ID | Diagnosis | Likely Cause | Next Safe Action | Acceptance |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for row in diagnostics:
        lines.append(
            "| {device} | {diagnosis} | {cause} | {action} | {status} |".format(
                device=row.get("analyzer_device_id", ""),
                diagnosis=row.get("diagnosis", ""),
                cause=row.get("likely_cause", ""),
                action=row.get("next_safe_action", ""),
                status=row.get("formal_acceptance_status", ""),
            )
        )
    lines.extend(
        [
            "",
            "## Physical Meaning",
            "",
            "- The humidity generator setpoint is a requested physical condition; it is not the calibration target used here.",
            "- The fit target is `ppm_H2O_Dew`, the H2O amount derived from dewpoint-meter readings and pressure reference evidence.",
            "- Dry-gas anchors from CO2 zero-gas open-flow evidence constrain the low-water baseline, but their target is the measured residual water vapor rather than hard zero.",
            "- Analyzer chamber temperature is used because the analyzer formula uses internal T; digital thermometer values are retained as chamber/box evidence.",
            "- If `fit_temperature_source=digital_thermometer`, the package is a post-temperature-repair review and uses the traceable thermometer temperature as the physical T input while preserving the original analyzer T.",
            "- Explicit pressure-QC overrides mean only the H2O ratio/temperature chain is being reviewed; pressure-channel acceptance remains blocked until the independent pressure workflow passes.",
            "- Pressure terms are frozen because pressure-channel verification/calibration is a separate V1.5 prerequisite.",
            "- A device with pinned final H2O output can still show a useful optical ratio fit, but it must not be accepted until a controlled write/readback/verification proves the final output chain follows the model.",
            "- If post-write verification artifacts are attached, the historical pre-write output-layer failure is retained as evidence but no longer treated as the current device state for that verified device.",
        ]
    )
    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path


def write_h2o_senco24_candidate_report(
    *,
    run_dir: str | Path,
    output_dir: str | Path,
    cfg: Optional[H2OSenco24CandidateConfig] = None,
    database_sidecar_json: str | Path | None = None,
) -> Dict[str, Path]:
    """Write no-write H2O SENCO2/SENCO4 candidate review artifacts."""

    config = cfg or H2OSenco24CandidateConfig()
    tables, context = build_h2o_senco24_candidate_tables(run_dir=run_dir, cfg=config)
    destination = Path(output_dir).resolve()
    metadata = ValidationMetadata(
        tool_name="export_v1_5_h2o_senco24_candidate_review",
        created_at=_now(),
        analyzers=[str(row.get("analyzer_device_id")) for row in tables["h2o_senco24_device_policy"]],
        input_paths=[str(_resolve_run_root(run_dir))]
        + [str(_resolve_run_root(path)) for path in config.additional_h2o_roots]
        + [str(_resolve_dry_anchor_root(path)) for path in config.dry_anchor_roots],
        output_dir=str(destination),
        config_summary={
            "component": "h2o",
            "run_status": context.get("run_status", ""),
            "wet_point_input_count": context.get("wet_point_input_count", 0),
            "additional_h2o_roots": context.get("additional_h2o_roots", []),
            "dry_anchor_input_count": context.get("dry_anchor_input_count", 0),
            "dry_anchor_roots": context.get("dry_anchor_roots", []),
            "dry_anchor_min_temp_c": context.get("dry_anchor_min_temp_c"),
            "dry_anchor_max_temp_c": context.get("dry_anchor_max_temp_c"),
            "no_write": True,
            "opens_com_ports": False,
            "connects_postgresql": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "reference_target_contract": "dewpoint_meter_plus_COM22_pressure_ppm_H2O_Dew",
            "selected_model_terms": ";".join(config.terms),
            "frozen_terms": ";".join(PRESSURE_TERMS),
            "design_max_relative_error_pct": float(config.design_max_relative_error_pct),
            "relative_error_min_reference_mmol": float(config.relative_error_min_reference_mmol),
            "min_wet_points": int(config.min_wet_points),
            "manual_device_block_reasons": dict(config.manual_device_block_reasons or {}),
            "postwrite_verified_device_ids": list(config.postwrite_verified_device_ids or ()),
            "postwrite_verification_artifacts": list(config.postwrite_verification_artifacts or ()),
            "allow_pressure_qc_failed_device_ids": sorted(_allowed_pressure_qc_failed_device_ids(config)),
            "fit_temperature_source": str(config.fit_temperature_source or "analyzer_chamber"),
            "fit_objective": str(config.fit_objective or "absolute_mmol"),
            "require_component_snapshot_for_layer_review": bool(
                config.require_component_snapshot_for_layer_review
            ),
            "factory_signal_health_source": context.get("factory_signal_health_source", ""),
        },
        notes=[
            "Offline V1.5 H2O SENCO2/SENCO4 candidate review.",
            "Dry-gas anchors are optional low-water evidence from gas-route zero-gas points; targets remain dewpoint/pressure-derived, not zeroed.",
            "Pressure terms are excluded for the current-atmosphere open-flow contract.",
            "Pressure-QC overrides only recover ratio/temperature evidence for component fitting; they are not pressure-channel acceptance.",
            "Digital thermometer fit temperature is only for post-temperature-repair review when analyzer T was known bad during acquisition.",
            "No coefficient write is performed or authorized by this export.",
        ],
    )
    outputs = write_validation_report(
        destination,
        prefix="h2o_senco24_candidate_review",
        metadata=metadata,
        tables=tables,
    )
    outputs["markdown"] = _write_markdown_report(destination, tables)
    sidecar_path = Path(database_sidecar_json).resolve() if database_sidecar_json else destination / "h2o_senco24_database_sidecar.json"
    _write_json(sidecar_path, _database_sidecar_rows(tables["h2o_senco24_device_policy"]))
    outputs["database_sidecar"] = sidecar_path
    return outputs
