"""V1 CO2 weighted-fit stability / robustness audit helpers.

This layer consumes the existing grouped calibration candidate pack and the
weighted-fit advisory output. It stays sidecar-only and does not change live
measured values or live gates.
"""

from __future__ import annotations

from collections import Counter
import csv
import json
import math
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .co2_weighted_fit_advisory import (
    _evaluation_rows_for_variant,
    _excluded_points,
    _fit_affine_model,
    _metric_bundle,
    _numeric_candidate_points,
    _recommended_eval_points,
    _round_or_none,
    _training_points_for_variant,
    _variant_specs,
    _as_bool,
    _as_float,
    _as_text,
    build_co2_weighted_fit_advisory,
)


def extract_candidate_rows_from_weighted_fit_payload(payload: Mapping[str, Any]) -> List[Dict[str, Any]]:
    seen: set[Tuple[str, str, str]] = set()
    rows: List[Dict[str, Any]] = []
    for row in payload.get("points") or []:
        point_tag = _as_text(row.get("point_tag"))
        point_no = _as_text(row.get("point_no"))
        point_title = _as_text(row.get("point_title"))
        dedupe_key = (point_tag, point_no, point_title)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        payload_row = dict(row)
        payload_row.pop("fit_variant_name", None)
        payload_row.pop("fit_predicted_target", None)
        payload_row.pop("fit_residual", None)
        payload_row.pop("fit_abs_error", None)
        payload_row.pop("fit_training_weight", None)
        rows.append(payload_row)
    return rows


def _baseline_variant_map(weighted_payload: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {
        _as_text(row.get("fit_variant_name")): dict(row)
        for row in (weighted_payload.get("fit_variants") or [])
        if _as_text(row.get("fit_variant_name"))
    }


def _relative_delta(value: Optional[float], baseline: Optional[float], *, floor: float = 1.0) -> float:
    if value is None or baseline is None:
        return 0.0
    denom = max(floor, abs(float(baseline)))
    return abs(float(value) - float(baseline)) / denom


def _leave_one_group_out_rows(
    variant_name: str,
    *,
    weighted: bool,
    training_points: Sequence[Mapping[str, Any]],
    evaluation_points: Sequence[Mapping[str, Any]],
    baseline_variant: Mapping[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    groups = sorted({_as_text(row.get("co2_calibration_group_key")) for row in training_points if _as_text(row.get("co2_calibration_group_key"))})
    influence_rows: List[Dict[str, Any]] = []
    coefficient_rows: List[Dict[str, Any]] = [
        {
            "fit_variant_name": variant_name,
            "analysis_scope": "baseline",
            "group_left_out": "",
            "fit_status": _as_text(baseline_variant.get("fit_variant_status")),
            "input_point_count": int(baseline_variant.get("input_point_count") or 0),
            "input_group_count": int(baseline_variant.get("input_group_count") or 0),
            "intercept": _as_float(baseline_variant.get("intercept")),
            "slope": _as_float(baseline_variant.get("slope")),
            "rmse": _as_float(baseline_variant.get("rmse")),
            "weighted_rmse": _as_float(baseline_variant.get("weighted_rmse")),
            "mae": _as_float(baseline_variant.get("mae")),
            "bias": _as_float(baseline_variant.get("bias")),
            "max_abs_error": _as_float(baseline_variant.get("max_abs_error")),
            "fit_stability_reason_chain": "baseline_fit",
        }
    ]

    baseline_intercept = _as_float(baseline_variant.get("intercept"))
    baseline_slope = _as_float(baseline_variant.get("slope"))
    baseline_rmse = _as_float(baseline_variant.get("rmse"))
    baseline_weighted_rmse = _as_float(baseline_variant.get("weighted_rmse"))
    baseline_mae = _as_float(baseline_variant.get("mae"))
    baseline_bias = _as_float(baseline_variant.get("bias"))
    baseline_max_abs_error = _as_float(baseline_variant.get("max_abs_error"))

    for group_key in groups:
        retained = [dict(row) for row in training_points if _as_text(row.get("co2_calibration_group_key")) != group_key]
        fit_result = _fit_affine_model(retained, weighted=weighted)
        if not fit_result.get("available"):
            influence_rows.append(
                {
                    "fit_variant_name": variant_name,
                    "group_left_out": group_key,
                    "fit_status": "unavailable",
                    "retained_point_count": len(retained),
                    "retained_group_count": len({_as_text(row.get('co2_calibration_group_key')) for row in retained}),
                    "intercept_delta": None,
                    "slope_delta": None,
                    "rmse_delta": None,
                    "weighted_rmse_delta": None,
                    "mae_delta": None,
                    "bias_delta": None,
                    "max_abs_error_delta": None,
                    "group_influence_score": None,
                    "fit_stability_reason_chain": _as_text(fit_result.get("reason") or "fit_unavailable_after_leave_one_group_out"),
                }
            )
            coefficient_rows.append(
                {
                    "fit_variant_name": variant_name,
                    "analysis_scope": "leave_one_group_out",
                    "group_left_out": group_key,
                    "fit_status": "unavailable",
                    "input_point_count": len(retained),
                    "input_group_count": len({_as_text(row.get('co2_calibration_group_key')) for row in retained}),
                    "intercept": None,
                    "slope": None,
                    "rmse": None,
                    "weighted_rmse": None,
                    "mae": None,
                    "bias": None,
                    "max_abs_error": None,
                    "fit_stability_reason_chain": _as_text(fit_result.get("reason") or "fit_unavailable_after_leave_one_group_out"),
                }
            )
            continue

        evaluation_rows = _evaluation_rows_for_variant(variant_name, fit_result, evaluation_points)
        residuals = [float(row.get("fit_residual") or 0.0) for row in evaluation_rows]
        weights = [max(0.0, float(row.get("co2_calibration_weight_recommended") or 0.0)) for row in evaluation_rows]
        metrics = _metric_bundle(residuals, weights=weights)

        intercept = _as_float(fit_result.get("intercept"))
        slope = _as_float(fit_result.get("slope"))
        rmse = metrics["rmse"]
        weighted_rmse = metrics["weighted_rmse"]
        mae = metrics["mae"]
        bias = metrics["bias"]
        max_abs_error = metrics["max_abs_error"]

        intercept_delta = None if intercept is None or baseline_intercept is None else intercept - baseline_intercept
        slope_delta = None if slope is None or baseline_slope is None else slope - baseline_slope
        rmse_delta = None if baseline_rmse is None else rmse - baseline_rmse
        weighted_rmse_delta = None if baseline_weighted_rmse is None else weighted_rmse - baseline_weighted_rmse
        mae_delta = None if baseline_mae is None else mae - baseline_mae
        bias_delta = None if baseline_bias is None else bias - baseline_bias
        max_abs_error_delta = None if baseline_max_abs_error is None else max_abs_error - baseline_max_abs_error

        influence_score = round(
            _relative_delta(intercept, baseline_intercept)
            + _relative_delta(slope, baseline_slope)
            + _relative_delta(weighted_rmse, baseline_weighted_rmse)
            + _relative_delta(rmse, baseline_rmse),
            6,
        )
        reason_chain = (
            f"leave_one_group_out={group_key};"
            f"retained_points={len(retained)};"
            f"retained_groups={len({_as_text(row.get('co2_calibration_group_key')) for row in retained})};"
            f"weighted={weighted}"
        )
        influence_rows.append(
            {
                "fit_variant_name": variant_name,
                "group_left_out": group_key,
                "fit_status": "available",
                "retained_point_count": len(retained),
                "retained_group_count": len({_as_text(row.get('co2_calibration_group_key')) for row in retained}),
                "intercept_delta": _round_or_none(intercept_delta),
                "slope_delta": _round_or_none(slope_delta),
                "rmse_delta": _round_or_none(rmse_delta),
                "weighted_rmse_delta": _round_or_none(weighted_rmse_delta),
                "mae_delta": _round_or_none(mae_delta),
                "bias_delta": _round_or_none(bias_delta),
                "max_abs_error_delta": _round_or_none(max_abs_error_delta),
                "group_influence_score": influence_score,
                "fit_stability_reason_chain": reason_chain,
            }
        )
        coefficient_rows.append(
            {
                "fit_variant_name": variant_name,
                "analysis_scope": "leave_one_group_out",
                "group_left_out": group_key,
                "fit_status": "available",
                "input_point_count": len(retained),
                "input_group_count": len({_as_text(row.get('co2_calibration_group_key')) for row in retained}),
                "intercept": _round_or_none(intercept),
                "slope": _round_or_none(slope),
                "rmse": _round_or_none(rmse),
                "weighted_rmse": _round_or_none(weighted_rmse),
                "mae": _round_or_none(mae),
                "bias": _round_or_none(bias),
                "max_abs_error": _round_or_none(max_abs_error),
                "fit_stability_reason_chain": reason_chain,
            }
        )
    return influence_rows, coefficient_rows


def _summarize_variant_stability(
    baseline_variant: Mapping[str, Any],
    influence_rows: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    variant_name = _as_text(baseline_variant.get("fit_variant_name"))
    available = [row for row in influence_rows if _as_text(row.get("fit_status")) == "available"]
    unavailable = [row for row in influence_rows if _as_text(row.get("fit_status")) != "available"]

    def _abs_mean(key: str) -> Optional[float]:
        values = [abs(float(row.get(key))) for row in available if _as_float(row.get(key)) is not None]
        return round(mean(values), 6) if values else None

    def _abs_max(key: str) -> Optional[float]:
        values = [abs(float(row.get(key))) for row in available if _as_float(row.get(key)) is not None]
        return round(max(values), 6) if values else None

    influence_values = [float(row.get("group_influence_score")) for row in available if _as_float(row.get("group_influence_score")) is not None]
    max_influence_row = max(available, key=lambda row: float(row.get("group_influence_score") or -1.0), default=None)
    max_influence = round(max(influence_values), 6) if influence_values else None
    mean_influence = round(mean(influence_values), 6) if influence_values else None

    if int(baseline_variant.get("input_group_count") or 0) < 2:
        stability_recommendation = "insufficient_groups"
    elif unavailable and len(unavailable) >= max(1, len(influence_rows) // 2):
        stability_recommendation = "fragile"
    elif max_influence is None:
        stability_recommendation = "unavailable"
    elif max_influence <= 1.0:
        stability_recommendation = "robust"
    elif max_influence <= 2.5:
        stability_recommendation = "caution"
    else:
        stability_recommendation = "fragile"

    reason_chain = (
        f"baseline_weighted_rmse={baseline_variant.get('weighted_rmse')};"
        f"max_group_influence={max_influence};"
        f"unavailable_loo={len(unavailable)}"
    )
    return {
        "fit_variant_name": variant_name,
        "fit_variant_status": _as_text(baseline_variant.get("fit_variant_status")),
        "weighted_fit": _as_bool(baseline_variant.get("weighted_fit")),
        "input_point_count": int(baseline_variant.get("input_point_count") or 0),
        "candidate_pool_point_count": int(baseline_variant.get("candidate_pool_point_count") or 0),
        "input_group_count": int(baseline_variant.get("input_group_count") or 0),
        "strong_support_pool_point_count": int(baseline_variant.get("strong_support_pool_point_count") or 0),
        "weak_support_pool_point_count": int(baseline_variant.get("weak_support_pool_point_count") or 0),
        "weak_support_pool_ratio": _as_float(baseline_variant.get("weak_support_pool_ratio")),
        "pre_fit_clean_first_applied": _as_bool(baseline_variant.get("pre_fit_clean_first_applied")),
        "baseline_rmse": _as_float(baseline_variant.get("rmse")),
        "baseline_weighted_rmse": _as_float(baseline_variant.get("weighted_rmse")),
        "baseline_mae": _as_float(baseline_variant.get("mae")),
        "baseline_bias": _as_float(baseline_variant.get("bias")),
        "baseline_max_abs_error": _as_float(baseline_variant.get("max_abs_error")),
        "leave_one_group_out_count": len(influence_rows),
        "leave_one_group_out_unavailable_count": len(unavailable),
        "mean_abs_intercept_delta": _abs_mean("intercept_delta"),
        "max_abs_intercept_delta": _abs_max("intercept_delta"),
        "mean_abs_slope_delta": _abs_mean("slope_delta"),
        "max_abs_slope_delta": _abs_max("slope_delta"),
        "mean_abs_rmse_delta": _abs_mean("rmse_delta"),
        "max_abs_rmse_delta": _abs_max("rmse_delta"),
        "mean_abs_weighted_rmse_delta": _abs_mean("weighted_rmse_delta"),
        "max_abs_weighted_rmse_delta": _abs_max("weighted_rmse_delta"),
        "mean_group_influence_score": mean_influence,
        "max_group_influence_score": max_influence,
        "most_influential_group": _as_text(max_influence_row.get("group_left_out")) if max_influence_row else "",
        "stability_recommendation": stability_recommendation,
        "fit_stability_reason_chain": reason_chain,
    }


def _recommend_stability_variant(rows: Sequence[Mapping[str, Any]]) -> Tuple[Optional[str], str]:
    available = [row for row in rows if _as_text(row.get("fit_variant_status")) == "available"]
    if not available:
        return None, "no_available_fit_variant"

    stability_rank = {"robust": 0, "caution": 1, "fragile": 2, "insufficient_groups": 3, "unavailable": 4}

    def _rank_key(row: Mapping[str, Any]) -> Tuple[int, float, float, float, int]:
        return (
            stability_rank.get(_as_text(row.get("stability_recommendation")), 9),
            float(row.get("baseline_weighted_rmse") or float("inf")),
            float(row.get("max_group_influence_score") or float("inf")),
            float(row.get("max_abs_weighted_rmse_delta") or float("inf")),
            {"weighted_fit_advisory": 0, "baseline_unweighted_fit_only": 1, "baseline_unweighted_all_recommended": 2}.get(
                _as_text(row.get("fit_variant_name")),
                9,
            ),
        )

    best = min(available, key=_rank_key)
    reason = (
        f"stability={best.get('stability_recommendation')};"
        f"weighted_rmse={best.get('baseline_weighted_rmse')};"
        f"max_group_influence={best.get('max_group_influence_score')};"
        f"max_weighted_rmse_delta={best.get('max_abs_weighted_rmse_delta')}"
    )
    return _as_text(best.get("fit_variant_name")), reason


def build_co2_fit_stability_audit(rows: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    weighted_payload = build_co2_weighted_fit_advisory(rows)
    candidate_points = _numeric_candidate_points(rows)
    evaluation_points = _recommended_eval_points(candidate_points)
    excluded_points = _excluded_points(candidate_points)
    baseline_map = _baseline_variant_map(weighted_payload)

    stability_rows: List[Dict[str, Any]] = []
    influence_rows: List[Dict[str, Any]] = []
    coefficient_rows: List[Dict[str, Any]] = []

    for spec in _variant_specs():
        variant_name = _as_text(spec.get("fit_variant_name"))
        baseline_variant = baseline_map.get(variant_name)
        if not baseline_variant:
            continue
        training_points = _training_points_for_variant(evaluation_points, variant_name=variant_name)
        variant_influence, variant_coefficients = _leave_one_group_out_rows(
            variant_name,
            weighted=bool(spec.get("weighted")),
            training_points=training_points,
            evaluation_points=evaluation_points,
            baseline_variant=baseline_variant,
        )
        influence_rows.extend(variant_influence)
        coefficient_rows.extend(variant_coefficients)
        stability_rows.append(_summarize_variant_stability(baseline_variant, variant_influence))

    recommended_variant, recommended_reason = _recommend_stability_variant(stability_rows)
    for row in stability_rows:
        row["recommended_fit_variant"] = _as_text(row.get("fit_variant_name")) == _as_text(recommended_variant)
        if row["recommended_fit_variant"]:
            row["fit_stability_reason_chain"] = _as_text(row.get("fit_stability_reason_chain")) + ";" + recommended_reason

    overall_summary = {
        "point_count_total": len(candidate_points),
        "evaluation_point_count": len(evaluation_points),
        "excluded_point_count": len(excluded_points),
        "group_count_total": len({_as_text(row.get('co2_calibration_group_key')) for row in candidate_points}),
        "recommended_fit_variant": recommended_variant or "",
        "recommended_fit_reason": recommended_reason,
        "excluded_reason_summary": ";".join(
            f"{reason}:{count}" for reason, count in Counter(
                "hard_blocked" if _as_bool(row.get("co2_calibration_candidate_hard_blocked")) else "not_recommended"
                for row in excluded_points
            ).most_common(4)
        ),
        "evidence_source": "replay_or_exported_v1_weighted_fit_advisory",
        "not_real_acceptance_evidence": True,
    }
    return {
        "summary": overall_summary,
        "weighted_fit_advisory_summary": weighted_payload.get("summary") or {},
        "fit_variants": weighted_payload.get("fit_variants") or [],
        "stability_variants": stability_rows,
        "groups": influence_rows,
        "coefficients": coefficient_rows,
    }


def render_co2_fit_stability_report(payload: Mapping[str, Any]) -> str:
    summary = dict(payload.get("summary") or {})
    stability_rows = list(payload.get("stability_variants") or [])
    groups = list(payload.get("groups") or [])
    lines = [
        "# V1 CO2 weighted-fit stability audit",
        "",
        "> replay evidence only",
        "> not real acceptance evidence",
        "",
        "## 总览",
        f"- 总点数: {summary.get('point_count_total', 0)}",
        f"- 进入拟合评估的点数: {summary.get('evaluation_point_count', 0)}",
        f"- 排除点数: {summary.get('excluded_point_count', 0)}",
        f"- 总组数: {summary.get('group_count_total', 0)}",
        f"- 推荐拟合变体: {summary.get('recommended_fit_variant', '') or 'none'}",
        f"- 推荐理由: {summary.get('recommended_fit_reason', '')}",
        "",
        "## 变体稳定性摘要",
    ]
    for row in stability_rows:
        lines.append(
            "- "
            + f"{row.get('fit_variant_name')}: "
            + f"stability={row.get('stability_recommendation')} "
            + f"baseline_weighted_rmse={row.get('baseline_weighted_rmse')} "
            + f"max_group_influence={row.get('max_group_influence_score')} "
            + f"max_weighted_rmse_delta={row.get('max_abs_weighted_rmse_delta')} "
            + f"recommended={row.get('recommended_fit_variant')}"
        )
    lines.extend(["", "## 高影响组"])
    ranked = sorted(
        [row for row in groups if _as_text(row.get("fit_status")) == "available"],
        key=lambda row: float(row.get("group_influence_score") or -1.0),
        reverse=True,
    )
    if not ranked:
        lines.append("- 无可用的 leave-one-group-out 结果")
    else:
        for row in ranked[:10]:
            lines.append(
                "- "
                + f"{row.get('fit_variant_name')} / {row.get('group_left_out')}: "
                + f"influence={row.get('group_influence_score')} "
                + f"weighted_rmse_delta={row.get('weighted_rmse_delta')} "
                + f"slope_delta={row.get('slope_delta')}"
            )
    lines.extend(
        [
            "",
            "## 说明",
            "- 本 sidecar 只消费现有 grouped calibration candidate pack 与 weighted fit advisory 结果。",
            "- 这一步关注的是拟合稳定性和系数鲁棒性，而不是改变 live measured value。",
            "- 当前实现默认采用 deterministic leave-one-group-out 审计；尚未加入 bootstrap/resample。",
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
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))


def write_co2_fit_stability_artifacts(output_dir: Path, payload: Mapping[str, Any]) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    groups_path = output_dir / "fit_stability_groups.csv"
    coeffs_path = output_dir / "fit_stability_coefficients.csv"
    summary_csv_path = output_dir / "fit_stability_summary.csv"
    summary_json_path = output_dir / "fit_stability_summary.json"
    report_path = output_dir / "fit_stability_report.md"

    write_csv_rows(groups_path, payload.get("groups") or [])
    write_csv_rows(coeffs_path, payload.get("coefficients") or [])
    write_csv_rows(summary_csv_path, payload.get("stability_variants") or [])
    with summary_json_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    report_path.write_text(render_co2_fit_stability_report(payload), encoding="utf-8")
    return {
        "groups_csv": groups_path,
        "coefficients_csv": coeffs_path,
        "summary_csv": summary_csv_path,
        "summary_json": summary_json_path,
        "report_md": report_path,
    }
