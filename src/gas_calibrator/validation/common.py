"""Common sample-analysis helpers for sidecar validation tools."""

from __future__ import annotations

import copy
import csv
import json
import math
import re
import shutil
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Mapping, Optional, Sequence

from ..coefficients.fit_ratio_poly import fit_ratio_poly_rt_p
from ..coefficients.fit_ratio_poly_evolved import fit_ratio_poly_rt_p_evolved
from ..coefficients.model_feature_policy import resolve_ratio_poly_model_features
from ..config import get as cfg_get
from ..data.points import CalibrationPoint
from ..logging_utils import RunLogger
from ..workflow.runner import CalibrationRunner


@dataclass(frozen=True)
class AnalyzerSpec:
    label: str
    prefix: str
    device_id: str = ""


def load_csv_rows(path: str | Path) -> List[Dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def latest_artifact(run_dir: str | Path, pattern: str) -> Optional[Path]:
    matches = [path for path in Path(run_dir).glob(pattern) if path.is_file()]
    if not matches:
        return None
    return max(matches, key=lambda item: item.stat().st_mtime)


def build_validation_point(
    *,
    index: int,
    temp_c: float = 20.0,
    pressure_hpa: Optional[float] = None,
    point_tag: str = "validation",
) -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temp_chamber_c=float(temp_c),
        co2_ppm=None,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=pressure_hpa,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group=None,
    )


def discover_analyzer_specs(
    cfg: Mapping[str, Any],
    sample_rows: Sequence[Mapping[str, Any]],
) -> List[AnalyzerSpec]:
    devices_cfg = cfg.get("devices", {}) if isinstance(cfg, Mapping) else {}
    discovered: List[AnalyzerSpec] = []
    gas_cfg = devices_cfg.get("gas_analyzers", []) if isinstance(devices_cfg, Mapping) else []
    if isinstance(gas_cfg, list):
        for idx, item in enumerate(gas_cfg, start=1):
            if not isinstance(item, Mapping) or not item.get("enabled", True):
                continue
            label = str(item.get("name") or f"ga{idx:02d}").upper()
            prefix = CalibrationRunner._safe_label(label)
            discovered.append(
                AnalyzerSpec(
                    label=label,
                    prefix=prefix,
                    device_id=str(item.get("device_id", "") or ""),
                )
            )
    if discovered:
        return discovered

    prefixes: List[str] = []
    for row in sample_rows:
        for key in row.keys():
            match = re.match(r"^(ga\d+)_", str(key))
            if match:
                prefix = match.group(1)
                if prefix not in prefixes:
                    prefixes.append(prefix)
    for prefix in sorted(prefixes):
        device_id = ""
        for row in sample_rows:
            value = row.get(f"{prefix}_device_id") or row.get(f"{prefix}_id")
            if value not in (None, ""):
                device_id = str(value)
                break
        discovered.append(AnalyzerSpec(label=prefix.upper(), prefix=prefix, device_id=device_id))
    return discovered


def build_validation_mode_cfg(cfg: Mapping[str, Any], *, mode: str) -> Dict[str, Any]:
    out = copy.deepcopy(dict(cfg))
    workflow_cfg = out.setdefault("workflow", {})
    summary_cfg = workflow_cfg.setdefault("summary_alignment", {})
    quality_cfg = workflow_cfg.setdefault("analyzer_frame_quality", {})
    coeff_cfg = out.setdefault("coefficients", {})
    ratio_poly_cfg = coeff_cfg.setdefault("ratio_poly_fit", {})

    if str(mode).strip().lower() == "legacy":
        summary_cfg["reference_on_aligned_rows"] = False
        ratio_poly_cfg["pressure_source_preference"] = "analyzer_only"
        quality_cfg["reject_negative_co2_ppm"] = False
        quality_cfg["reject_negative_h2o_mmol"] = False
        quality_cfg["reject_nonpositive_co2_ratio_f"] = False
        quality_cfg["reject_nonpositive_h2o_ratio_f"] = False
        quality_cfg["invalid_sentinel_values"] = []
        quality_cfg["pressure_kpa_min"] = None
        quality_cfg["pressure_kpa_max"] = None
    else:
        summary_cfg.setdefault("reference_on_aligned_rows", True)
        ratio_poly_cfg.setdefault("pressure_source_preference", "reference_first")
    return out


def _make_runner(
    cfg: Mapping[str, Any],
    *,
    run_id: str,
) -> tuple[CalibrationRunner, RunLogger, List[str], Path]:
    messages: List[str] = []
    tmp_dir = Path(tempfile.mkdtemp(prefix="gc_validate_runner_"))
    logger = RunLogger(tmp_dir, run_id=run_id, cfg=dict(cfg))
    runner = CalibrationRunner(dict(cfg), {}, logger, messages.append, lambda *_: None)
    return runner, logger, messages, tmp_dir


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


def _device_id_for_prefix(rows: Sequence[Mapping[str, Any]], prefix: str) -> str:
    for row in rows:
        for key in (f"{prefix}_device_id", f"{prefix}_id"):
            value = row.get(key)
            if value not in (None, ""):
                return str(value)
    return ""


def _point_identity(row: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("point_row", "") or ""),
        str(row.get("point_phase", "") or ""),
        str(row.get("point_tag", "") or ""),
    )


def _extract_prefixed_parsed(row: Mapping[str, Any], prefix: str) -> Optional[Dict[str, Any]]:
    parsed: Dict[str, Any] = {}
    for key in CalibrationRunner._mode2_sample_fields():
        value = RunLogger._sample_prefixed_value(dict(row), prefix, key)
        if value not in (None, ""):
            parsed[key] = value
    return parsed or None


def reassess_sample_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    cfg: Mapping[str, Any],
    analyzer_specs: Sequence[AnalyzerSpec],
    mode: str,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
    runner, logger, messages, tmp_dir = _make_runner(cfg, run_id=f"validation_{mode}")
    try:
        updated_rows: List[Dict[str, Any]] = []
        point_reason_counters: Dict[tuple[str, str, str, str], Counter[str]] = defaultdict(Counter)

        for source_row in rows:
            row = dict(source_row)
            for index, spec in enumerate(analyzer_specs):
                parsed = _extract_prefixed_parsed(row, spec.prefix)
                has_data = parsed is not None
                if parsed is None:
                    usable = False
                    status = "无帧"
                else:
                    usable, status = runner._assess_analyzer_frame(parsed)
                runner._set_sample_frame_meta(
                    row,
                    spec.prefix,
                    has_data=has_data,
                    usable=usable,
                    status=status,
                    is_primary=index == 0,
                )
                if has_data and status:
                    point_reason_counters[(*_point_identity(row), spec.label)][status] += 1
            updated_rows.append(row)

        frame_rows: List[Dict[str, Any]] = []
        point_groups: Dict[tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
        for row in updated_rows:
            point_groups[_point_identity(row)].append(row)

        for point_key, point_rows in sorted(point_groups.items()):
            point_row, point_phase, point_tag = point_key
            for spec in analyzer_specs:
                total_frames = sum(
                    1 for item in point_rows if RunLogger._sample_prefixed_has_data(item, spec.prefix)
                )
                valid_frames = sum(
                    1 for item in point_rows if bool(item.get(f"{spec.prefix}_frame_usable"))
                )
                reasons = point_reason_counters.get((*point_key, spec.label), Counter())
                reason_text = "; ".join(f"{key}={value}" for key, value in reasons.most_common(5))
                frame_rows.append(
                    {
                        "mode": mode,
                        "Analyzer": spec.label,
                        "AnalyzerId": _device_id_for_prefix(point_rows, spec.prefix) or spec.device_id,
                        "PointRow": point_row,
                        "PointPhase": point_phase,
                        "PointTag": point_tag,
                        "TotalFrames": total_frames,
                        "ValidFrames": valid_frames,
                        "ValidRatio": round(valid_frames / total_frames, 6) if total_frames else 0.0,
                        "UnusableReasonTopN": reason_text,
                    }
                )

        return updated_rows, frame_rows, messages
    finally:
        logger.close()
        shutil.rmtree(tmp_dir, ignore_errors=True)


def rebuild_analyzer_summary_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    cfg: Mapping[str, Any],
    analyzer_specs: Sequence[AnalyzerSpec],
    mode: str,
) -> tuple[List[Dict[str, Any]], List[str]]:
    runner, logger, messages, tmp_dir = _make_runner(cfg, run_id=f"summary_{mode}")
    try:
        point_groups: Dict[tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
        for row in rows:
            point_groups[_point_identity(row)].append(dict(row))

        counters: Dict[str, int] = defaultdict(int)
        summary_rows: List[Dict[str, Any]] = []
        for spec in analyzer_specs:
            for point_key in sorted(point_groups.keys()):
                point_rows = point_groups[point_key]
                if not any(
                    RunLogger._sample_prefixed_has_data(item, spec.prefix) for item in point_rows
                ):
                    continue
                counters[spec.label] += 1
                row = logger._build_analyzer_summary_row(
                    point_rows,
                    label=spec.label,
                    num=counters[spec.label],
                )
                row["Analyzer"] = spec.label
                row["AnalyzerId"] = _device_id_for_prefix(point_rows, spec.prefix) or spec.device_id
                row["Mode"] = mode
                row["ReferenceAlignedRows"] = bool(
                    cfg_get(cfg, "workflow.summary_alignment.reference_on_aligned_rows", True)
                )
                row["AlignedSampleCount"] = int(row.get("ValidFrames") or 0)
                row["SummarySampleCount"] = int(row.get("TotalFrames") or 0)
                summary_rows.append(row)
        return summary_rows, messages
    finally:
        logger.close()
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _fit_fn_for_model(model: str):
    return fit_ratio_poly_rt_p_evolved if model in {"ratio_poly_rt_p_evolved", "poly_rt_p_evolved"} else fit_ratio_poly_rt_p


def _pressure_diff_stats(dataframe: Any) -> Dict[str, Any]:
    """Compare summary pressure channels after converting both to kPa.

    The mature V1 summary contract stores the external reference pressure in
    ``P`` as hPa and the analyzer-internal pressure in ``BAR``/``P_fit`` as
    kPa.  This helper is validation-only; it must not influence production
    pressure-source selection.
    """

    columns = tuple(getattr(dataframe, "columns", ()))
    analyzer_key = "BAR" if "BAR" in columns else ("P_fit" if "P_fit" in columns else "")
    if "P" not in columns or not analyzer_key:
        return {
            "P_coverage": int(dataframe["P"].notna().sum()) if "P" in columns else 0,
            "BAR_coverage": int(dataframe[analyzer_key].notna().sum()) if analyzer_key else 0,
            "Overlap": 0,
            "reference_pressure_unit": "hPa",
            "reference_to_kpa_scale": 0.1,
            "analyzer_pressure_key": analyzer_key,
            "analyzer_pressure_unit": "kPa",
            "P_BAR_mean_abs_diff": None,
            "P_BAR_max_abs_diff": None,
            "P_BAR_mean_abs_diff_kpa": None,
            "P_BAR_max_abs_diff_kpa": None,
            "P_minus_BAR_mean_kpa": None,
        }
    pair = dataframe[["P", analyzer_key]].copy()
    for key in ("P", analyzer_key):
        pair[key] = pair[key].map(_safe_float)
    valid = pair.dropna()
    if valid.empty:
        return {
            "P_coverage": int(pair["P"].notna().sum()),
            "BAR_coverage": int(pair[analyzer_key].notna().sum()),
            "Overlap": 0,
            "reference_pressure_unit": "hPa",
            "reference_to_kpa_scale": 0.1,
            "analyzer_pressure_key": analyzer_key,
            "analyzer_pressure_unit": "kPa",
            "P_BAR_mean_abs_diff": None,
            "P_BAR_max_abs_diff": None,
            "P_BAR_mean_abs_diff_kpa": None,
            "P_BAR_max_abs_diff_kpa": None,
            "P_minus_BAR_mean_kpa": None,
        }
    signed_diff_kpa = valid["P"] * 0.1 - valid[analyzer_key]
    abs_diff_kpa = signed_diff_kpa.abs()
    return {
        "P_coverage": int(pair["P"].notna().sum()),
        "BAR_coverage": int(pair[analyzer_key].notna().sum()),
        "Overlap": int(len(valid)),
        "reference_pressure_unit": "hPa",
        "reference_to_kpa_scale": 0.1,
        "analyzer_pressure_key": analyzer_key,
        "analyzer_pressure_unit": "kPa",
        # Retain the historical column names for downstream readers, but make
        # their values physically valid and expose explicit-unit aliases.
        "P_BAR_mean_abs_diff": float(abs_diff_kpa.mean()),
        "P_BAR_max_abs_diff": float(abs_diff_kpa.max()),
        "P_BAR_mean_abs_diff_kpa": float(abs_diff_kpa.mean()),
        "P_BAR_max_abs_diff_kpa": float(abs_diff_kpa.max()),
        "P_minus_BAR_mean_kpa": float(signed_diff_kpa.mean()),
    }


def _fit_diagnostic_metrics(result: Any) -> Dict[str, Any]:
    stats = dict(getattr(result, "stats", {}) or {})
    residuals = list(getattr(result, "residuals", []) or [])
    targets = [_safe_float(item.get("target")) for item in residuals]
    errors = [_safe_float(item.get("error_simplified")) for item in residuals]
    paired = [(target, error) for target, error in zip(targets, errors) if target is not None and error is not None]
    r_squared = None
    bias = None
    if paired:
        paired_targets = [item[0] for item in paired]
        paired_errors = [item[1] for item in paired]
        bias = float(mean(paired_errors))
        target_mean = float(mean(paired_targets))
        total_sum_squares = sum((value - target_mean) ** 2 for value in paired_targets)
        if total_sum_squares > 0:
            r_squared = 1.0 - sum(error**2 for error in paired_errors) / total_sum_squares
    stability = dict(stats.get("original_coefficient_analysis", {}) or {})
    return {
        "n": int(getattr(result, "n", 0) or 0),
        "rmse": _safe_float(stats.get("rmse_simplified")),
        "mae": _safe_float(stats.get("mae_simplified")),
        "bias": bias,
        "max_abs_error": _safe_float(stats.get("max_abs_simplified")),
        "r_squared": _safe_float(r_squared),
        "condition_number": _safe_float(stability.get("condition_number")),
        "max_abs_coefficient": _safe_float(stability.get("max_abs_coefficient")),
        "min_nonzero_coefficient": _safe_float(stability.get("min_nonzero_coefficient")),
    }


def _same_frame_pressure_source_audit(
    analyzer_rows: Sequence[Mapping[str, Any]],
    *,
    fit_fn: Any,
    gas: str,
    target_key: str,
    ratio_key: str,
    temp_keys: Sequence[str],
    common_kwargs: Mapping[str, Any],
) -> Dict[str, Any]:
    """Fit both pressure sources on the exact same rows for offline diagnosis."""

    analyzer_pressure_key = ""
    if any("BAR" in row for row in analyzer_rows):
        analyzer_pressure_key = "BAR"
    elif any("P_fit" in row for row in analyzer_rows):
        analyzer_pressure_key = "P_fit"

    base: Dict[str, Any] = {
        "pressure_audit_status": "evidence_insufficient",
        "pressure_audit_reason": "",
        "pressure_audit_evidence_source": "offline_same_frame_diagnostic",
        "pressure_audit_not_real_acceptance_evidence": True,
        "pressure_audit_writes_performed": False,
        "pressure_audit_promotion_state": "blocked",
        "pressure_audit_authority_decision": "blocked_pending_traceable_reference_evidence",
        "pressure_audit_equivalence_decision": "not_assessed_no_predefined_limits",
        "pressure_audit_reference_key": "P",
        "pressure_audit_reference_unit": "hPa",
        "pressure_audit_reference_to_kpa_scale": 0.1,
        "pressure_audit_analyzer_key": analyzer_pressure_key,
        "pressure_audit_analyzer_unit": "kPa",
        "pressure_audit_reference_coverage": sum(_safe_float(row.get("P")) is not None for row in analyzer_rows),
        "pressure_audit_analyzer_coverage": (
            sum(_safe_float(row.get(analyzer_pressure_key)) is not None for row in analyzer_rows)
            if analyzer_pressure_key
            else 0
        ),
    }
    if not analyzer_pressure_key:
        base["pressure_audit_reason"] = "missing_analyzer_pressure_column_BAR_or_P_fit"
        return base

    same_frame_rows: List[Dict[str, Any]] = []
    signed_differences_kpa: List[float] = []
    for source in analyzer_rows:
        reference_hpa = _safe_float(source.get("P"))
        analyzer_kpa = _safe_float(source.get(analyzer_pressure_key))
        if reference_hpa is None or analyzer_kpa is None:
            continue
        row = dict(source)
        row["_pressure_audit_reference_kpa"] = reference_hpa * 0.1
        row["_pressure_audit_analyzer_kpa"] = analyzer_kpa
        same_frame_rows.append(row)
        signed_differences_kpa.append(reference_hpa * 0.1 - analyzer_kpa)

    base["pressure_audit_same_frame_rows"] = len(same_frame_rows)
    if signed_differences_kpa:
        absolute_differences = [abs(value) for value in signed_differences_kpa]
        base.update(
            {
                "pressure_audit_P_minus_analyzer_mean_kpa": float(mean(signed_differences_kpa)),
                "pressure_audit_mean_abs_diff_kpa": float(mean(absolute_differences)),
                "pressure_audit_max_abs_diff_kpa": float(max(absolute_differences)),
            }
        )
    if not same_frame_rows:
        base["pressure_audit_reason"] = "no_same_frame_pressure_overlap"
        return base

    fit_kwargs = dict(common_kwargs)
    try:
        reference_result = fit_fn(
            same_frame_rows,
            gas=gas,
            target_key=target_key,
            ratio_keys=(ratio_key,),
            temp_keys=tuple(temp_keys),
            pressure_keys=("_pressure_audit_reference_kpa",),
            pressure_scale=1.0,
            **fit_kwargs,
        )
        analyzer_result = fit_fn(
            same_frame_rows,
            gas=gas,
            target_key=target_key,
            ratio_keys=(ratio_key,),
            temp_keys=tuple(temp_keys),
            pressure_keys=("_pressure_audit_analyzer_kpa",),
            pressure_scale=1.0,
            **fit_kwargs,
        )
    except Exception as exc:
        base["pressure_audit_reason"] = f"same_frame_fit_failed:{exc}"
        return base

    reference_metrics = _fit_diagnostic_metrics(reference_result)
    analyzer_metrics = _fit_diagnostic_metrics(analyzer_result)
    base.update({f"pressure_audit_reference_{key}": value for key, value in reference_metrics.items()})
    base.update({f"pressure_audit_analyzer_{key}": value for key, value in analyzer_metrics.items()})
    if (
        reference_metrics["n"] <= 0
        or analyzer_metrics["n"] <= 0
        or reference_metrics["n"] != analyzer_metrics["n"]
    ):
        base["pressure_audit_reason"] = "fit_sample_count_mismatch_or_empty"
        return base

    reference_coefficients = dict(getattr(reference_result, "simplified_coefficients", {}) or {})
    analyzer_coefficients = dict(getattr(analyzer_result, "simplified_coefficients", {}) or {})
    common_names = sorted(set(reference_coefficients) & set(analyzer_coefficients))
    coefficient_deltas = {
        name: float(analyzer_coefficients[name]) - float(reference_coefficients[name])
        for name in common_names
    }
    reference_terms = dict(getattr(reference_result, "feature_terms", {}) or {})
    pressure_related_deltas = {
        str(reference_terms.get(name) or name): value
        for name, value in coefficient_deltas.items()
        if "P" in str(reference_terms.get(name) or "").upper()
    }
    base.update(
        {
            "pressure_audit_status": "diagnostic_comparable",
            "pressure_audit_reason": "same_rows_same_units_same_model_no_authority_selection",
            "pressure_audit_reference_coefficients_json": json.dumps(
                reference_coefficients,
                ensure_ascii=False,
                sort_keys=True,
            ),
            "pressure_audit_analyzer_coefficients_json": json.dumps(
                analyzer_coefficients,
                ensure_ascii=False,
                sort_keys=True,
            ),
            "pressure_audit_analyzer_minus_reference_coefficients_json": json.dumps(
                coefficient_deltas,
                ensure_ascii=False,
                sort_keys=True,
            ),
            "pressure_audit_pressure_related_coefficient_deltas_json": json.dumps(
                pressure_related_deltas,
                ensure_ascii=False,
                sort_keys=True,
            ),
            "pressure_audit_R_T_k_P_coefficient_delta": pressure_related_deltas.get("R*T_k*P"),
            "pressure_audit_max_abs_coefficient_delta": (
                max((abs(value) for value in coefficient_deltas.values()), default=None)
            ),
        }
    )
    return base


def fit_overview_rows(
    summary_rows: Sequence[Mapping[str, Any]],
    *,
    cfg: Mapping[str, Any],
    gas: str,
    mode: str,
) -> tuple[List[Dict[str, Any]], List[str]]:
    runner, logger, messages, tmp_dir = _make_runner(cfg, run_id=f"fit_{mode}_{gas}")
    try:
        coeff_cfg = dict(cfg.get("coefficients", {}) or {})
        fit_cfg = dict(coeff_cfg)
        filtered_rows = runner._filter_ratio_poly_summary_rows(list(summary_rows), gas=gas, cfg=fit_cfg)
        grouped_rows: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in filtered_rows:
            analyzer_label = str(row.get("Analyzer") or "").strip()
            if analyzer_label:
                grouped_rows[analyzer_label].append(dict(row))

        out: List[Dict[str, Any]] = []
        if not grouped_rows:
            out.append(
                {
                    "mode": mode,
                    "gas": gas,
                    "Analyzer": "",
                    "status": "no_rows",
                    "summary_rows_used": 0,
                    "pressure_audit_status": "evidence_insufficient",
                    "pressure_audit_reason": "no_fit_rows",
                    "pressure_audit_evidence_source": "offline_same_frame_diagnostic",
                    "pressure_audit_not_real_acceptance_evidence": True,
                    "pressure_audit_writes_performed": False,
                    "pressure_audit_promotion_state": "blocked",
                    "pressure_audit_authority_decision": "blocked_pending_traceable_reference_evidence",
                    "pressure_audit_equivalence_decision": "not_assessed_no_predefined_limits",
                }
            )
            return out, messages

        summary_columns = fit_cfg.get("summary_columns", {}).get(gas, {})
        target_key = summary_columns.get("target", "ppm_CO2_Tank" if gas == "co2" else "ppm_H2O_Dew")
        ratio_key = summary_columns.get("ratio", "R_CO2" if gas == "co2" else "R_H2O")
        temp_key = summary_columns.get("temperature", "thermometer_temp_c")
        temp_keys = tuple(runner._summary_temperature_keys(str(temp_key or "").strip()))
        pressure_key = summary_columns.get("pressure", "BAR")
        pressure_scale = float(summary_columns.get("pressure_scale", 1.0))
        pressure_preference = str(
            cfg_get(
                cfg,
                "coefficients.ratio_poly_fit.pressure_source_preference",
                fit_cfg.get("ratio_poly_fit", {}).get("pressure_source_preference", "reference_first"),
            )
            or "reference_first"
        ).strip().lower()
        pressure_candidates = runner._ratio_poly_pressure_candidates(pressure_key, pressure_preference)
        model = str(fit_cfg.get("model", "ratio_poly_rt_p") or "ratio_poly_rt_p").strip().lower()
        fit_fn = _fit_fn_for_model(model)
        active_model_features, model_feature_policy = resolve_ratio_poly_model_features(
            fit_cfg,
            cfg.get("workflow", {}).get("selected_pressure_points"),
        )
        common_kwargs = {
            "ratio_degree": int(fit_cfg.get("ratio_degree", 3)),
            "temperature_offset_c": float(fit_cfg.get("temperature_offset_c", 273.15)),
            "add_intercept": bool(fit_cfg.get("add_intercept", True)),
            "model_features": active_model_features,
            "simplify_coefficients": bool(fit_cfg.get("simplify_coefficients", True)),
            "simplification_method": str(fit_cfg.get("simplification_method", "column_norm") or "column_norm"),
            "target_digits": int(fit_cfg.get("target_digits", 6)),
            "min_samples": int(fit_cfg.get("min_samples", 0) or 0),
            "train_ratio": float(fit_cfg.get("train_ratio", 0.7)),
            "val_ratio": float(fit_cfg.get("val_ratio", 0.15)),
            "random_seed": int(fit_cfg.get("random_seed", 42)),
            "shuffle_dataset": bool(fit_cfg.get("shuffle_dataset", True)),
            "log_fn": messages.append,
        }
        if fit_fn is fit_ratio_poly_rt_p_evolved:
            common_kwargs.update(
                {
                    "robust_iterations": int(fit_cfg.get("robust_iterations", 8) or 0),
                    "robust_huber_delta": float(fit_cfg.get("robust_huber_delta", 1.5)),
                    "robust_min_weight": float(fit_cfg.get("robust_min_weight", 0.05)),
                    "candidate_simplification_methods": fit_cfg.get("candidate_simplification_methods"),
                }
            )

        for analyzer_label, analyzer_rows in sorted(grouped_rows.items()):
            pressure_audit = _same_frame_pressure_source_audit(
                analyzer_rows,
                fit_fn=fit_fn,
                gas=gas,
                target_key=target_key,
                ratio_key=ratio_key,
                temp_keys=temp_keys,
                common_kwargs=common_kwargs,
            )
            try:
                resolved = runner._resolve_ratio_poly_columns(
                    analyzer_rows,
                    gas=gas,
                    target_key=target_key,
                    ratio_key=ratio_key,
                    temp_keys=temp_keys,
                    pressure_keys=pressure_candidates,
                )
                dataframe = resolved["dataframe"]
                pressure_stats = _pressure_diff_stats(dataframe)
                result = fit_fn(
                    analyzer_rows,
                    gas=gas,
                    target_key=target_key,
                    ratio_keys=(ratio_key,),
                    temp_keys=temp_keys,
                    pressure_keys=pressure_candidates,
                    pressure_scale=pressure_scale,
                    **common_kwargs,
                )
                errors = [
                    _safe_float(item.get("error_simplified"))
                    for item in getattr(result, "residuals", [])
                ]
                errors = [item for item in errors if item is not None]
                out.append(
                    {
                        "mode": mode,
                        "gas": gas,
                        "Analyzer": analyzer_label,
                        "status": "fit_ok",
                        "summary_rows_used": len(analyzer_rows),
                        "target_key": str(resolved["target_column"]),
                        "ratio_key": str(resolved["ratio_column"]),
                        "temp_key": str(resolved["temp_column"]),
                        "selected_pressure_key": str(resolved["pressure_column"]),
                        "pressure_source_preference": pressure_preference,
                        "pressure_scale": pressure_scale,
                        "model_feature_policy": model_feature_policy,
                        "model_feature_tokens": ",".join(active_model_features or []),
                        "P_coverage": pressure_stats["P_coverage"],
                        "BAR_coverage": pressure_stats["BAR_coverage"],
                        "P_BAR_overlap": pressure_stats["Overlap"],
                        "P_BAR_mean_abs_diff": pressure_stats["P_BAR_mean_abs_diff"],
                        "P_BAR_max_abs_diff": pressure_stats["P_BAR_max_abs_diff"],
                        "P_BAR_mean_abs_diff_kpa": pressure_stats["P_BAR_mean_abs_diff_kpa"],
                        "P_BAR_max_abs_diff_kpa": pressure_stats["P_BAR_max_abs_diff_kpa"],
                        "P_minus_BAR_mean_kpa": pressure_stats["P_minus_BAR_mean_kpa"],
                        "fit_n": int(getattr(result, "n", 0) or 0),
                        "rmse_simplified": float(getattr(result, "stats", {}).get("rmse_simplified", 0.0)),
                        "bias_simplified": float(mean(errors)) if errors else None,
                        "max_abs_simplified": float(getattr(result, "stats", {}).get("max_abs_simplified", 0.0)),
                        **pressure_audit,
                    }
                )
            except Exception as exc:
                out.append(
                    {
                        "mode": mode,
                        "gas": gas,
                        "Analyzer": analyzer_label,
                        "status": "fit_error",
                        "summary_rows_used": len(analyzer_rows),
                        "target_key": target_key,
                        "ratio_key": ratio_key,
                        "temp_key": temp_key,
                        "selected_pressure_key": "",
                        "pressure_source_preference": pressure_preference,
                        "pressure_scale": pressure_scale,
                        "model_feature_policy": model_feature_policy,
                        "model_feature_tokens": ",".join(active_model_features or []),
                        "fit_error": str(exc),
                        **pressure_audit,
                    }
                )
        return out, messages
    finally:
        logger.close()
        shutil.rmtree(tmp_dir, ignore_errors=True)


def analyze_sample_rows(
    sample_rows: Sequence[Mapping[str, Any]],
    *,
    cfg: Mapping[str, Any],
    analyzer_filter: Optional[Sequence[str]] = None,
    gas: str = "both",
    modes: Sequence[str] = ("current",),
) -> Dict[str, List[Dict[str, Any]]]:
    analyzers = discover_analyzer_specs(cfg, sample_rows)
    if analyzer_filter:
        wanted = {str(item).strip().upper() for item in analyzer_filter if str(item).strip()}
        analyzers = [spec for spec in analyzers if spec.label.upper() in wanted or spec.device_id.upper() in wanted]

    gases = ["co2", "h2o"] if str(gas).strip().lower() == "both" else [str(gas).strip().lower()]
    tables: Dict[str, List[Dict[str, Any]]] = {
        "frame_quality_summary": [],
        "summary_alignment_check": [],
        "pressure_source_check": [],
        "pressure_source_same_frame_audit": [],
        "fit_input_overview": [],
        "per_analyzer_comparison": [],
        "conclusion_summary": [],
    }
    fit_index: Dict[tuple[str, str, str], Dict[str, Any]] = {}

    for mode in modes:
        mode_cfg = build_validation_mode_cfg(cfg, mode=mode)
        reassessed_rows, frame_rows, _messages = reassess_sample_rows(
            sample_rows,
            cfg=mode_cfg,
            analyzer_specs=analyzers,
            mode=mode,
        )
        tables["frame_quality_summary"].extend(frame_rows)
        summary_rows, _messages = rebuild_analyzer_summary_rows(
            reassessed_rows,
            cfg=mode_cfg,
            analyzer_specs=analyzers,
            mode=mode,
        )
        for row in summary_rows:
            tables["summary_alignment_check"].append(
                {
                    "mode": mode,
                    "Analyzer": row.get("Analyzer"),
                    "AnalyzerId": row.get("AnalyzerId"),
                    "PointRow": row.get("PointRow"),
                    "PointPhase": row.get("PointPhase"),
                    "PointTag": row.get("PointTag"),
                    "ReferenceAlignedRows": row.get("ReferenceAlignedRows"),
                    "AlignedSampleCount": row.get("AlignedSampleCount"),
                    "SummarySampleCount": row.get("SummarySampleCount"),
                    "Dew": row.get("Dew"),
                    "P": row.get("P"),
                    "ppm_H2O_Dew": row.get("ppm_H2O_Dew"),
                    "ValidFrames": row.get("ValidFrames"),
                    "TotalFrames": row.get("TotalFrames"),
                }
            )
        for gas_name in gases:
            fit_rows, _messages = fit_overview_rows(summary_rows, cfg=mode_cfg, gas=gas_name, mode=mode)
            for fit_row in fit_rows:
                tables["fit_input_overview"].append(fit_row)
                fit_index[(mode, gas_name, str(fit_row.get("Analyzer") or ""))] = fit_row
                tables["pressure_source_check"].append(
                    {
                        "mode": mode,
                        "gas": gas_name,
                        "Analyzer": fit_row.get("Analyzer"),
                        "status": fit_row.get("status"),
                        "selected_pressure_key": fit_row.get("selected_pressure_key"),
                        "pressure_source_preference": fit_row.get("pressure_source_preference"),
                        "P_coverage": fit_row.get("P_coverage"),
                        "BAR_coverage": fit_row.get("BAR_coverage"),
                        "P_BAR_overlap": fit_row.get("P_BAR_overlap"),
                        "P_BAR_mean_abs_diff": fit_row.get("P_BAR_mean_abs_diff"),
                        "P_BAR_max_abs_diff": fit_row.get("P_BAR_max_abs_diff"),
                        "P_BAR_mean_abs_diff_kpa": fit_row.get("P_BAR_mean_abs_diff_kpa"),
                        "P_BAR_max_abs_diff_kpa": fit_row.get("P_BAR_max_abs_diff_kpa"),
                        "P_minus_BAR_mean_kpa": fit_row.get("P_minus_BAR_mean_kpa"),
                        "summary_rows_used": fit_row.get("summary_rows_used"),
                    }
                )
                tables["pressure_source_same_frame_audit"].append(
                    {
                        "mode": mode,
                        "gas": gas_name,
                        "Analyzer": fit_row.get("Analyzer"),
                        **{
                            key: value
                            for key, value in fit_row.items()
                            if str(key).startswith("pressure_audit_")
                        },
                    }
                )

    if set(modes) >= {"legacy", "current"}:
        analyzer_names = sorted({spec.label for spec in analyzers})
        for gas_name in gases:
            for analyzer_label in analyzer_names:
                legacy_row = fit_index.get(("legacy", gas_name, analyzer_label), {})
                current_row = fit_index.get(("current", gas_name, analyzer_label), {})
                if not legacy_row and not current_row:
                    continue
                tables["per_analyzer_comparison"].append(
                    {
                        "gas": gas_name,
                        "Analyzer": analyzer_label,
                        "legacy_pressure_key": legacy_row.get("selected_pressure_key"),
                        "current_pressure_key": current_row.get("selected_pressure_key"),
                        "legacy_rows": legacy_row.get("summary_rows_used"),
                        "current_rows": current_row.get("summary_rows_used"),
                        "legacy_rmse": legacy_row.get("rmse_simplified"),
                        "current_rmse": current_row.get("rmse_simplified"),
                        "legacy_bias": legacy_row.get("bias_simplified"),
                        "current_bias": current_row.get("bias_simplified"),
                        "legacy_status": legacy_row.get("status"),
                        "current_status": current_row.get("status"),
                    }
                )

    valid_ratio_values = [
        _safe_float(row.get("ValidRatio"))
        for row in tables["frame_quality_summary"]
    ]
    valid_ratio_values = [item for item in valid_ratio_values if item is not None]
    worst_valid_ratio = min(valid_ratio_values) if valid_ratio_values else None
    fit_errors = [row for row in tables["fit_input_overview"] if row.get("status") == "fit_error"]
    risk = "pass"
    if fit_errors:
        risk = "fail"
    elif worst_valid_ratio is not None and worst_valid_ratio < 0.8:
        risk = "warn"
    tables["conclusion_summary"].append(
        {
            "risk_level": risk,
            "analyzer_count": len(analyzers),
            "worst_valid_ratio": worst_valid_ratio,
            "fit_error_count": len(fit_errors),
            "advice": (
                "检查 fit_error 和 pressure_source_check。"
                if fit_errors
                else "先看 frame_quality_summary 与 pressure_source_check，再决定是否写设备。"
            ),
        }
    )
    return tables
