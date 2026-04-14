"""V1 CO2 weighted fit advisory helpers.

This module consumes the existing V1 CO2 calibration candidate pack and
compares a few small, deterministic offline fit variants. It does not change
live measured values or introduce new live gates.
"""

from __future__ import annotations

from collections import Counter, defaultdict
import csv
import json
import math
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np

from .co2_calibration_candidate_pack import (
    build_co2_calibration_candidate_points,
)


def _as_text(value: Any) -> str:
    return str(value or "").strip()


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = _as_text(value).lower()
    return text in {"1", "true", "yes", "y", "on"}


def _as_float(value: Any) -> Optional[float]:
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _round_or_none(value: Optional[float], digits: int = 6) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), digits)


def _numeric_candidate_points(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    points = build_co2_calibration_candidate_points(rows)
    normalized: List[Dict[str, Any]] = []
    for row in points:
        payload = dict(row)
        payload["co2_ppm_target"] = _as_float(payload.get("co2_ppm_target"))
        payload["measured_value"] = _as_float(payload.get("measured_value"))
        payload["co2_calibration_weight_recommended"] = _as_float(payload.get("co2_calibration_weight_recommended")) or 0.0
        normalized.append(payload)
    return normalized


def _has_numeric_target_and_value(row: Mapping[str, Any]) -> bool:
    return row.get("co2_ppm_target") is not None and row.get("measured_value") is not None


def _recommended_eval_points(points: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return [
        dict(row)
        for row in points
        if _has_numeric_target_and_value(row)
        and _as_bool(row.get("co2_calibration_candidate_recommended"))
        and not _as_bool(row.get("co2_calibration_candidate_hard_blocked"))
    ]


def _excluded_points(points: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return [
        dict(row)
        for row in points
        if not (
            _has_numeric_target_and_value(row)
            and _as_bool(row.get("co2_calibration_candidate_recommended"))
            and not _as_bool(row.get("co2_calibration_candidate_hard_blocked"))
        )
    ]


def _excluded_reason(row: Mapping[str, Any]) -> str:
    if not _has_numeric_target_and_value(row):
        return "missing_target_or_measured"
    if _as_bool(row.get("co2_calibration_candidate_hard_blocked")):
        return "hard_blocked"
    if not _as_bool(row.get("co2_calibration_candidate_recommended")):
        return "not_recommended"
    return "excluded"


def _variant_specs() -> List[Dict[str, Any]]:
    return [
        {
            "fit_variant_name": "baseline_unweighted_all_recommended",
            "weighted": False,
            "priority": 2,
        },
        {
            "fit_variant_name": "baseline_unweighted_fit_only",
            "weighted": False,
            "priority": 1,
        },
        {
            "fit_variant_name": "weighted_fit_advisory",
            "weighted": True,
            "priority": 0,
        },
    ]


def _training_points_for_variant(
    points: Sequence[Mapping[str, Any]],
    *,
    variant_name: str,
) -> List[Dict[str, Any]]:
    if variant_name == "baseline_unweighted_all_recommended":
        return [dict(row) for row in points]
    if variant_name == "baseline_unweighted_fit_only":
        return [dict(row) for row in points if _as_text(row.get("co2_calibration_candidate_status")) == "fit"]
    if variant_name == "weighted_fit_advisory":
        return [dict(row) for row in points if float(row.get("co2_calibration_weight_recommended") or 0.0) > 0.0]
    return []


def _fit_affine_model(
    points: Sequence[Mapping[str, Any]],
    *,
    weighted: bool,
) -> Dict[str, Any]:
    if len(points) < 2:
        return {"available": False, "reason": "insufficient_points"}

    x = np.asarray([float(row["measured_value"]) for row in points], dtype=float)
    y = np.asarray([float(row["co2_ppm_target"]) for row in points], dtype=float)
    if len({round(float(value), 9) for value in x}) < 2:
        return {"available": False, "reason": "insufficient_unique_measured_values"}

    design = np.column_stack([np.ones(len(x)), x])
    if weighted:
        weights = np.asarray([max(0.0, float(row.get("co2_calibration_weight_recommended") or 0.0)) for row in points], dtype=float)
        if np.sum(weights) <= 0.0:
            return {"available": False, "reason": "zero_weight_sum"}
        sqrt_weights = np.sqrt(weights)
        beta, *_ = np.linalg.lstsq(design * sqrt_weights[:, None], y * sqrt_weights, rcond=None)
    else:
        weights = np.ones(len(points), dtype=float)
        beta, *_ = np.linalg.lstsq(design, y, rcond=None)

    intercept = float(beta[0])
    slope = float(beta[1])
    return {
        "available": True,
        "intercept": intercept,
        "slope": slope,
        "training_weight_sum": float(np.sum(weights)),
    }


def _predict_target(intercept: float, slope: float, measured_value: float) -> float:
    return float(intercept + slope * measured_value)


def _metric_bundle(
    residuals: Sequence[float],
    *,
    weights: Sequence[float],
) -> Dict[str, float]:
    residual_array = np.asarray(list(residuals), dtype=float)
    abs_array = np.abs(residual_array)
    weight_array = np.asarray(list(weights), dtype=float)
    weight_sum = float(np.sum(weight_array))
    if residual_array.size == 0:
        return {
            "rmse": 0.0,
            "mae": 0.0,
            "bias": 0.0,
            "max_abs_error": 0.0,
            "weighted_rmse": 0.0,
            "weighted_mae": 0.0,
            "weighted_bias": 0.0,
        }
    if weight_sum <= 0.0:
        weight_array = np.ones_like(residual_array)
        weight_sum = float(np.sum(weight_array))
    return {
        "rmse": float(math.sqrt(float(np.mean(np.square(residual_array))))),
        "mae": float(np.mean(abs_array)),
        "bias": float(np.mean(residual_array)),
        "max_abs_error": float(np.max(abs_array)),
        "weighted_rmse": float(math.sqrt(float(np.sum(weight_array * np.square(residual_array)) / weight_sum))),
        "weighted_mae": float(np.sum(weight_array * abs_array) / weight_sum),
        "weighted_bias": float(np.sum(weight_array * residual_array) / weight_sum),
    }


def _reason_counter_text(rows: Sequence[Mapping[str, Any]], key: str, *, top_n: int = 4) -> str:
    counter: Counter[str] = Counter()
    for row in rows:
        for token in _as_text(row.get(key)).split(";"):
            token = token.strip()
            if token:
                counter[token] += 1
    if not counter:
        return ""
    return ";".join(f"{name}:{count}" for name, count in counter.most_common(top_n))


def _evaluation_rows_for_variant(
    variant_name: str,
    coeffs: Mapping[str, Any],
    evaluation_points: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    intercept = float(coeffs["intercept"])
    slope = float(coeffs["slope"])
    for row in evaluation_points:
        measured = float(row["measured_value"])
        target = float(row["co2_ppm_target"])
        predicted = _predict_target(intercept, slope, measured)
        residual = predicted - target
        payload = dict(row)
        payload.update(
            {
                "fit_variant_name": variant_name,
                "fit_predicted_target": _round_or_none(predicted),
                "fit_residual": _round_or_none(residual),
                "fit_abs_error": _round_or_none(abs(residual)),
                "fit_training_weight": _round_or_none(_as_float(row.get("co2_calibration_weight_recommended")) or 0.0, 4),
            }
        )
        rows.append(payload)
    return rows


def _group_rows_for_variant(
    variant_name: str,
    evaluation_rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in evaluation_rows:
        grouped[_as_text(row.get("co2_calibration_group_key"))].append(row)

    result: List[Dict[str, Any]] = []
    for key, rows in sorted(grouped.items(), key=lambda item: item[0]):
        residuals = [float(row.get("fit_residual") or 0.0) for row in rows]
        weights = [float(row.get("fit_training_weight") or 0.0) for row in rows]
        measured_values = [float(row.get("measured_value") or 0.0) for row in rows]
        predicted_values = [float(row.get("fit_predicted_target") or 0.0) for row in rows]
        target_values = [float(row.get("co2_ppm_target") or 0.0) for row in rows]
        payload = {
            "fit_variant_name": variant_name,
            "calibration_group_key": key,
            "point_count_total": len(rows),
            "weight_sum": _round_or_none(sum(weights), 4),
            "target_mean": _round_or_none(mean(target_values)),
            "measured_mean": _round_or_none(mean(measured_values)),
            "predicted_target_mean": _round_or_none(mean(predicted_values)),
            "residual_bias": _round_or_none(mean(residuals)),
            "residual_mae": _round_or_none(mean(abs(item) for item in residuals)),
            "residual_max_abs_error": _round_or_none(max(abs(item) for item in residuals)),
            "candidate_status_summary": _reason_counter_text(rows, "co2_calibration_candidate_status", top_n=3),
            "group_evidence_summary": _as_text(rows[0].get("co2_point_evidence_budget_summary")),
            "group_recommended_for_fit": any(_as_bool(row.get("co2_calibration_candidate_recommended")) for row in rows),
        }
        result.append(payload)
    return result


def _variant_summary(
    spec: Mapping[str, Any],
    *,
    training_points: Sequence[Mapping[str, Any]],
    evaluation_points: Sequence[Mapping[str, Any]],
    excluded_points: Sequence[Mapping[str, Any]],
    fit_result: Mapping[str, Any],
    evaluation_rows: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    variant_name = _as_text(spec.get("fit_variant_name"))
    if not fit_result.get("available"):
        return {
            "fit_variant_name": variant_name,
            "fit_variant_status": "unavailable",
            "weighted_fit": bool(spec.get("weighted")),
            "input_point_count": len(training_points),
            "input_group_count": len({_as_text(row.get("co2_calibration_group_key")) for row in training_points}),
            "evaluation_point_count": len(evaluation_points),
            "evaluation_group_count": len({_as_text(row.get("co2_calibration_group_key")) for row in evaluation_points}),
            "excluded_point_count": len(excluded_points),
            "excluded_reason_summary": _reason_counter_text(excluded_points, "co2_calibration_reason_chain"),
            "fit_reason_chain": _as_text(fit_result.get("reason") or "fit_unavailable"),
            "recommended_fit_variant": False,
        }

    residuals = [float(row.get("fit_residual") or 0.0) for row in evaluation_rows]
    evaluation_weights = [max(0.0, float(row.get("co2_calibration_weight_recommended") or 0.0)) for row in evaluation_rows]
    metrics = _metric_bundle(residuals, weights=evaluation_weights)
    return {
        "fit_variant_name": variant_name,
        "fit_variant_status": "available",
        "weighted_fit": bool(spec.get("weighted")),
        "input_point_count": len(training_points),
        "input_group_count": len({_as_text(row.get("co2_calibration_group_key")) for row in training_points}),
        "evaluation_point_count": len(evaluation_points),
        "evaluation_group_count": len({_as_text(row.get("co2_calibration_group_key")) for row in evaluation_points}),
        "excluded_point_count": len(excluded_points),
        "excluded_reason_summary": ";".join(f"{reason}:{count}" for reason, count in Counter(_excluded_reason(row) for row in excluded_points).most_common(4)),
        "intercept": _round_or_none(_as_float(fit_result.get("intercept"))),
        "slope": _round_or_none(_as_float(fit_result.get("slope"))),
        "training_weight_sum": _round_or_none(_as_float(fit_result.get("training_weight_sum")), 4),
        "rmse": _round_or_none(metrics["rmse"]),
        "mae": _round_or_none(metrics["mae"]),
        "bias": _round_or_none(metrics["bias"]),
        "max_abs_error": _round_or_none(metrics["max_abs_error"]),
        "weighted_rmse": _round_or_none(metrics["weighted_rmse"]),
        "weighted_mae": _round_or_none(metrics["weighted_mae"]),
        "weighted_bias": _round_or_none(metrics["weighted_bias"]),
        "candidate_status_summary": ";".join(
            f"{name}:{count}" for name, count in Counter(_as_text(row.get("co2_calibration_candidate_status")) for row in training_points).most_common()
        ),
        "fit_reason_chain": (
            f"model=affine_target_from_measured;"
            f"training_points={len(training_points)};"
            f"weighted={bool(spec.get('weighted'))};"
            f"candidate_statuses={_reason_counter_text(training_points, 'co2_calibration_candidate_status', top_n=3)}"
        ),
        "recommended_fit_variant": False,
    }


def _recommend_variant(summaries: Sequence[Mapping[str, Any]]) -> Tuple[Optional[str], str]:
    available = [row for row in summaries if _as_text(row.get("fit_variant_status")) == "available"]
    if not available:
        return None, "no_available_fit_variant"

    def _rank_key(row: Mapping[str, Any]) -> Tuple[float, float, float, int, int]:
        return (
            float(row.get("weighted_rmse") or float("inf")),
            float(row.get("rmse") or float("inf")),
            float(row.get("max_abs_error") or float("inf")),
            -int(row.get("input_point_count") or 0),
            {"weighted_fit_advisory": 0, "baseline_unweighted_fit_only": 1, "baseline_unweighted_all_recommended": 2}.get(
                _as_text(row.get("fit_variant_name")),
                9,
            ),
        )

    best = min(available, key=_rank_key)
    reason = (
        f"prefer_low_weighted_rmse={best.get('weighted_rmse')};"
        f"rmse={best.get('rmse')};"
        f"max_abs_error={best.get('max_abs_error')};"
        f"input_points={best.get('input_point_count')}"
    )
    return _as_text(best.get("fit_variant_name")), reason


def build_co2_weighted_fit_advisory(rows: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    candidate_points = _numeric_candidate_points(rows)
    evaluation_points = _recommended_eval_points(candidate_points)
    excluded_points = _excluded_points(candidate_points)

    summary_rows: List[Dict[str, Any]] = []
    point_rows: List[Dict[str, Any]] = []
    group_rows: List[Dict[str, Any]] = []

    for spec in _variant_specs():
        variant_name = _as_text(spec.get("fit_variant_name"))
        training_points = _training_points_for_variant(evaluation_points, variant_name=variant_name)
        fit_result = _fit_affine_model(training_points, weighted=bool(spec.get("weighted")))
        evaluation_rows = (
            _evaluation_rows_for_variant(variant_name, fit_result, evaluation_points)
            if fit_result.get("available")
            else []
        )
        summary = _variant_summary(
            spec,
            training_points=training_points,
            evaluation_points=evaluation_points,
            excluded_points=excluded_points,
            fit_result=fit_result,
            evaluation_rows=evaluation_rows,
        )
        point_rows.extend(evaluation_rows)
        group_rows.extend(_group_rows_for_variant(variant_name, evaluation_rows))
        summary_rows.append(summary)

    recommended_variant_name, recommended_reason = _recommend_variant(summary_rows)
    for row in summary_rows:
        row["recommended_fit_variant"] = _as_text(row.get("fit_variant_name")) == _as_text(recommended_variant_name)
        if row["recommended_fit_variant"]:
            row["fit_reason_chain"] = _as_text(row.get("fit_reason_chain")) + ";" + recommended_reason

    overall_summary = {
        "point_count_total": len(candidate_points),
        "evaluation_point_count": len(evaluation_points),
        "excluded_point_count": len(excluded_points),
        "group_count_total": len({_as_text(row.get('co2_calibration_group_key')) for row in candidate_points}),
        "recommended_fit_variant": recommended_variant_name or "",
        "recommended_fit_reason": recommended_reason,
        "excluded_reason_summary": ";".join(
            f"{reason}:{count}" for reason, count in Counter(_excluded_reason(row) for row in excluded_points).most_common(4)
        ),
        "evidence_source": "replay_or_exported_v1_candidate_pack",
        "not_real_acceptance_evidence": True,
    }
    return {
        "summary": overall_summary,
        "fit_variants": summary_rows,
        "points": point_rows,
        "groups": group_rows,
    }


def render_co2_weighted_fit_advisory_report(payload: Mapping[str, Any]) -> str:
    summary = dict(payload.get("summary") or {})
    variants = list(payload.get("fit_variants") or [])
    groups = list(payload.get("groups") or [])
    lines = [
        "# V1 CO2 weighted fit advisory",
        "",
        "> replay evidence only",
        "> not real acceptance evidence",
        "",
        "## 总览",
        f"- 总点数: {summary.get('point_count_total', 0)}",
        f"- 进入拟合评估的点数: {summary.get('evaluation_point_count', 0)}",
        f"- 被排除的点数: {summary.get('excluded_point_count', 0)}",
        f"- 总组数: {summary.get('group_count_total', 0)}",
        f"- 推荐拟合变体: {summary.get('recommended_fit_variant', '') or 'none'}",
        f"- 推荐理由: {summary.get('recommended_fit_reason', '')}",
        f"- 主要排除原因: {summary.get('excluded_reason_summary', '')}",
        "",
        "## 变体对比",
    ]
    for row in variants:
        lines.append(
            "- "
            + f"{row.get('fit_variant_name')}: "
            + f"status={row.get('fit_variant_status')} "
            + f"input_points={row.get('input_point_count')} "
            + f"eval_points={row.get('evaluation_point_count')} "
            + f"rmse={row.get('rmse')} "
            + f"mae={row.get('mae')} "
            + f"bias={row.get('bias')} "
            + f"max_abs_error={row.get('max_abs_error')} "
            + f"weighted_rmse={row.get('weighted_rmse')} "
            + f"recommended={row.get('recommended_fit_variant')}"
        )
    lines.extend(["", "## 组级摘要"])
    if not groups:
        lines.append("- 无组级评估结果")
    else:
        best_groups = sorted(groups, key=lambda row: (_as_text(row.get("fit_variant_name")), float(row.get("residual_max_abs_error") or 0.0)))
        for row in best_groups[:10]:
            lines.append(
                "- "
                + f"{row.get('fit_variant_name')} / {row.get('calibration_group_key')}: "
                + f"点数={row.get('point_count_total')} "
                + f"bias={row.get('residual_bias')} "
                + f"mae={row.get('residual_mae')} "
                + f"max_abs={row.get('residual_max_abs_error')} "
                + f"recommended={row.get('group_recommended_for_fit')}"
            )
    lines.extend(
        [
            "",
            "## 说明",
            "- 这个 sidecar 只消费现有 hardened V1 CO2 candidate pack / suitability / waterfall evidence。",
            "- 这一步的主价值是直接比较 unweighted vs weighted fit，而不是改变 live measured value。",
        ]
    )
    return "\n".join(lines) + "\n"


def _csv_header(rows: Sequence[Mapping[str, Any]]) -> List[str]:
    header: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in header:
                header.append(str(key))
    return header


def write_csv_rows(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    header = _csv_header(rows)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        csv_writer = csv.DictWriter(handle, fieldnames=header)
        csv_writer.writeheader()
        for row in rows:
            csv_writer.writerow(dict(row))


def write_weighted_fit_advisory_artifacts(output_dir: Path, payload: Mapping[str, Any]) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    points_path = output_dir / "weighted_fit_points.csv"
    groups_path = output_dir / "weighted_fit_groups.csv"
    summary_csv_path = output_dir / "weighted_fit_summary.csv"
    summary_json_path = output_dir / "weighted_fit_summary.json"
    report_path = output_dir / "weighted_fit_report.md"
    coeffs_path = output_dir / "weighted_fit_coefficients.json"

    write_csv_rows(points_path, payload.get("points") or [])
    write_csv_rows(groups_path, payload.get("groups") or [])
    write_csv_rows(summary_csv_path, payload.get("fit_variants") or [])
    with summary_json_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    report_path.write_text(render_co2_weighted_fit_advisory_report(payload), encoding="utf-8")
    coefficients = [
        {
            "fit_variant_name": row.get("fit_variant_name"),
            "intercept": row.get("intercept"),
            "slope": row.get("slope"),
            "recommended_fit_variant": row.get("recommended_fit_variant"),
        }
        for row in payload.get("fit_variants") or []
        if _as_text(row.get("fit_variant_status")) == "available"
    ]
    with coeffs_path.open("w", encoding="utf-8") as handle:
        json.dump({"coefficients": coefficients}, handle, ensure_ascii=False, indent=2)
    return {
        "points_csv": points_path,
        "groups_csv": groups_path,
        "summary_csv": summary_csv_path,
        "summary_json": summary_json_path,
        "report_md": report_path,
        "coefficients_json": coeffs_path,
    }
