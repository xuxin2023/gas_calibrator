"""V1 CO2 grouped bootstrap / repeated-resample robustness audit helpers.

This layer stays sidecar-only. It consumes the existing candidate pack,
weighted-fit advisory, and fit-stability evidence to compare fit variants under
grouped resampling without changing live measured values or live gates.
"""

from __future__ import annotations

from collections import Counter, defaultdict
import csv
import json
import math
from pathlib import Path
from random import Random
from statistics import mean
from typing import Any, DefaultDict, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .co2_fit_stability_audit import (
    build_co2_fit_stability_audit,
)
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


def _percentile(values: Sequence[float], pct: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    pct = max(0.0, min(100.0, float(pct)))
    position = (len(ordered) - 1) * pct / 100.0
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _group_training_points(
    training_points: Sequence[Mapping[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in training_points:
        grouped[_as_text(row.get("co2_calibration_group_key"))].append(dict(row))
    return dict(grouped)


def _resample_keys_bootstrap(group_keys: Sequence[str], rng: Random) -> List[str]:
    return [group_keys[rng.randrange(len(group_keys))] for _ in range(len(group_keys))]


def _resample_keys_subsample(
    group_keys: Sequence[str],
    *,
    rng: Random,
    group_fraction: float,
) -> List[str]:
    if not group_keys:
        return []
    sample_size = max(1, min(len(group_keys), int(math.ceil(len(group_keys) * group_fraction))))
    if sample_size >= len(group_keys):
        return list(group_keys)
    return sorted(rng.sample(list(group_keys), sample_size))


def _expanded_training_rows(
    grouped_points: Mapping[str, Sequence[Mapping[str, Any]]],
    sampled_keys: Sequence[str],
    *,
    resample_method: str,
    round_index: int,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for draw_index, group_key in enumerate(sampled_keys):
        for row in grouped_points.get(group_key, ()):
            payload = dict(row)
            payload["resample_method"] = resample_method
            payload["resample_round_index"] = round_index
            payload["resample_group_key"] = group_key
            payload["resample_draw_index"] = draw_index
            rows.append(payload)
    return rows


def _distribution_row(
    *,
    fit_variant_name: str,
    resample_method: str,
    baseline_variant: Mapping[str, Any],
    coefficient_rows: Sequence[Mapping[str, Any]],
    group_summary_rows: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    available = [row for row in coefficient_rows if _as_text(row.get("fit_status")) == "available"]
    unavailable_count = len(coefficient_rows) - len(available)

    def _dist_stats(key: str) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
        values = [float(row.get(key)) for row in available if _as_float(row.get(key)) is not None]
        if not values:
            return None, None, None, None
        mean_value = round(mean(values), 6)
        p50 = _round_or_none(_percentile(values, 50))
        p90 = _round_or_none(_percentile(values, 90))
        p95 = _round_or_none(_percentile(values, 95))
        return mean_value, p50, p90, p95

    intercept_mean, intercept_p50, intercept_p90, intercept_p95 = _dist_stats("intercept")
    slope_mean, slope_p50, slope_p90, slope_p95 = _dist_stats("slope")
    rmse_mean, rmse_p50, rmse_p90, rmse_p95 = _dist_stats("rmse")
    wrmse_mean, wrmse_p50, wrmse_p90, wrmse_p95 = _dist_stats("weighted_rmse")
    mae_mean, mae_p50, mae_p90, mae_p95 = _dist_stats("mae")
    bias_mean, bias_p50, bias_p90, bias_p95 = _dist_stats("bias")
    maxerr_mean, maxerr_p50, maxerr_p90, maxerr_p95 = _dist_stats("max_abs_error")

    max_group_row = max(
        group_summary_rows,
        key=lambda row: float(row.get("group_fragility_score") or -1.0),
        default=None,
    )
    max_group_fragility = _as_float(max_group_row.get("group_fragility_score")) if max_group_row else None
    most_fragile_group = _as_text(max_group_row.get("calibration_group_key")) if max_group_row else ""

    if unavailable_count >= max(1, len(coefficient_rows) // 3):
        robustness_recommendation = "fragile"
    elif (max_group_fragility or 0.0) <= 0.25:
        robustness_recommendation = "robust"
    elif (max_group_fragility or 0.0) <= 1.0:
        robustness_recommendation = "caution"
    else:
        robustness_recommendation = "fragile"

    return {
        "analysis_scope": "resample_method",
        "fit_variant_name": fit_variant_name,
        "resample_method": resample_method,
        "fit_variant_status": _as_text(baseline_variant.get("fit_variant_status")),
        "weighted_fit": _as_bool(baseline_variant.get("weighted_fit")),
        "input_point_count": int(baseline_variant.get("input_point_count") or 0),
        "candidate_pool_point_count": int(baseline_variant.get("candidate_pool_point_count") or 0),
        "input_group_count": int(baseline_variant.get("input_group_count") or 0),
        "strong_support_pool_point_count": int(baseline_variant.get("strong_support_pool_point_count") or 0),
        "weak_support_pool_point_count": int(baseline_variant.get("weak_support_pool_point_count") or 0),
        "weak_support_pool_ratio": _as_float(baseline_variant.get("weak_support_pool_ratio")),
        "pre_fit_clean_first_applied": _as_bool(baseline_variant.get("pre_fit_clean_first_applied")),
        "resample_count": len(coefficient_rows),
        "available_round_count": len(available),
        "unavailable_round_count": unavailable_count,
        "baseline_rmse": _as_float(baseline_variant.get("rmse")),
        "baseline_weighted_rmse": _as_float(baseline_variant.get("weighted_rmse")),
        "baseline_mae": _as_float(baseline_variant.get("mae")),
        "baseline_bias": _as_float(baseline_variant.get("bias")),
        "baseline_max_abs_error": _as_float(baseline_variant.get("max_abs_error")),
        "intercept_mean": intercept_mean,
        "intercept_p50": intercept_p50,
        "intercept_p90": intercept_p90,
        "intercept_p95": intercept_p95,
        "slope_mean": slope_mean,
        "slope_p50": slope_p50,
        "slope_p90": slope_p90,
        "slope_p95": slope_p95,
        "rmse_mean": rmse_mean,
        "rmse_p50": rmse_p50,
        "rmse_p90": rmse_p90,
        "rmse_p95": rmse_p95,
        "weighted_rmse_mean": wrmse_mean,
        "weighted_rmse_p50": wrmse_p50,
        "weighted_rmse_p90": wrmse_p90,
        "weighted_rmse_p95": wrmse_p95,
        "mae_mean": mae_mean,
        "mae_p50": mae_p50,
        "mae_p90": mae_p90,
        "mae_p95": mae_p95,
        "bias_mean": bias_mean,
        "bias_p50": bias_p50,
        "bias_p90": bias_p90,
        "bias_p95": bias_p95,
        "max_abs_error_mean": maxerr_mean,
        "max_abs_error_p50": maxerr_p50,
        "max_abs_error_p90": maxerr_p90,
        "max_abs_error_p95": maxerr_p95,
        "most_fragile_group": most_fragile_group,
        "max_group_fragility_score": _round_or_none(max_group_fragility),
        "robustness_recommendation": robustness_recommendation,
        "fit_robustness_reason_chain": (
            f"method={resample_method};"
            f"baseline_weighted_rmse={baseline_variant.get('weighted_rmse')};"
            f"max_group_fragility={_round_or_none(max_group_fragility)};"
            f"unavailable_rounds={unavailable_count}"
        ),
    }


def _deterministic_influence_map(
    fit_stability_payload: Mapping[str, Any],
) -> Dict[Tuple[str, str], float]:
    mapping: Dict[Tuple[str, str], float] = {}
    for row in fit_stability_payload.get("groups") or []:
        variant = _as_text(row.get("fit_variant_name"))
        group = _as_text(row.get("group_left_out"))
        score = _as_float(row.get("group_influence_score"))
        if variant and group and score is not None:
            mapping[(variant, group)] = float(score)
    return mapping


def _run_resample_method(
    *,
    fit_variant_name: str,
    weighted: bool,
    training_points: Sequence[Mapping[str, Any]],
    evaluation_points: Sequence[Mapping[str, Any]],
    baseline_variant: Mapping[str, Any],
    resample_method: str,
    rounds: int,
    rng: Random,
    group_fraction: float,
    deterministic_influence: Mapping[Tuple[str, str], float],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    grouped = _group_training_points(training_points)
    group_keys = sorted(grouped.keys())
    coefficient_rows: List[Dict[str, Any]] = []
    group_acc: Dict[str, Dict[str, Any]] = {
        group_key: {
            "included_rounds": 0,
            "total_multiplicity": 0,
            "weighted_rmse_when_included": [],
            "weighted_rmse_when_omitted": [],
            "group_weight_sum": round(
                sum(float(row.get("co2_calibration_weight_recommended") or 0.0) for row in grouped[group_key]),
                4,
            ),
            "group_candidate_status_summary": ";".join(
                f"{name}:{count}"
                for name, count in Counter(
                    _as_text(row.get("co2_calibration_candidate_status")) for row in grouped[group_key]
                ).most_common()
            ),
        }
        for group_key in group_keys
    }

    for round_index in range(rounds):
        if resample_method == "grouped_bootstrap":
            sampled_keys = _resample_keys_bootstrap(group_keys, rng)
        else:
            sampled_keys = _resample_keys_subsample(group_keys, rng=rng, group_fraction=group_fraction)
        sampled_counter = Counter(sampled_keys)
        sampled_rows = _expanded_training_rows(
            grouped,
            sampled_keys,
            resample_method=resample_method,
            round_index=round_index,
        )
        fit_result = _fit_affine_model(sampled_rows, weighted=weighted)
        if not fit_result.get("available"):
            coefficient_rows.append(
                {
                    "fit_variant_name": fit_variant_name,
                    "resample_method": resample_method,
                    "round_index": round_index,
                    "fit_status": "unavailable",
                    "sampled_group_count": len(sampled_keys),
                    "sampled_unique_group_count": len(sampled_counter),
                    "sampled_group_keys": ";".join(sampled_keys),
                    "intercept": None,
                    "slope": None,
                    "rmse": None,
                    "weighted_rmse": None,
                    "mae": None,
                    "bias": None,
                    "max_abs_error": None,
                    "fit_robustness_reason_chain": _as_text(
                        fit_result.get("reason") or "fit_unavailable_under_resample"
                    ),
                }
            )
            continue

        evaluation_rows = _evaluation_rows_for_variant(fit_variant_name, fit_result, evaluation_points)
        residuals = [float(row.get("fit_residual") or 0.0) for row in evaluation_rows]
        weights = [max(0.0, float(row.get("co2_calibration_weight_recommended") or 0.0)) for row in evaluation_rows]
        metrics = _metric_bundle(residuals, weights=weights)
        coefficient_rows.append(
            {
                "fit_variant_name": fit_variant_name,
                "resample_method": resample_method,
                "round_index": round_index,
                "fit_status": "available",
                "sampled_group_count": len(sampled_keys),
                "sampled_unique_group_count": len(sampled_counter),
                "sampled_group_keys": ";".join(sampled_keys),
                "intercept": _round_or_none(_as_float(fit_result.get("intercept"))),
                "slope": _round_or_none(_as_float(fit_result.get("slope"))),
                "rmse": _round_or_none(metrics["rmse"]),
                "weighted_rmse": _round_or_none(metrics["weighted_rmse"]),
                "mae": _round_or_none(metrics["mae"]),
                "bias": _round_or_none(metrics["bias"]),
                "max_abs_error": _round_or_none(metrics["max_abs_error"]),
                "fit_robustness_reason_chain": (
                    f"method={resample_method};round={round_index};"
                    f"sampled_unique_groups={len(sampled_counter)};weighted={weighted}"
                ),
            }
        )

        for group_key in group_keys:
            included = group_key in sampled_counter
            if included:
                group_acc[group_key]["included_rounds"] += 1
                group_acc[group_key]["total_multiplicity"] += sampled_counter[group_key]
                group_acc[group_key]["weighted_rmse_when_included"].append(metrics["weighted_rmse"])
            else:
                group_acc[group_key]["weighted_rmse_when_omitted"].append(metrics["weighted_rmse"])

    group_rows: List[Dict[str, Any]] = []
    for group_key in group_keys:
        included_values = list(group_acc[group_key]["weighted_rmse_when_included"])
        omitted_values = list(group_acc[group_key]["weighted_rmse_when_omitted"])
        included_mean = mean(included_values) if included_values else None
        omitted_mean = mean(omitted_values) if omitted_values else None
        deterministic_score = deterministic_influence.get((fit_variant_name, group_key))
        fragility_score = 0.0
        if included_mean is not None and omitted_mean is not None:
            fragility_score += abs(float(included_mean) - float(omitted_mean))
        if deterministic_score is not None:
            fragility_score += abs(float(deterministic_score))
        group_rows.append(
            {
                "fit_variant_name": fit_variant_name,
                "resample_method": resample_method,
                "calibration_group_key": group_key,
                "inclusion_frequency": _round_or_none(group_acc[group_key]["included_rounds"] / max(1, rounds)),
                "mean_group_multiplicity": _round_or_none(group_acc[group_key]["total_multiplicity"] / max(1, rounds)),
                "weighted_rmse_when_included_mean": _round_or_none(included_mean),
                "weighted_rmse_when_omitted_mean": _round_or_none(omitted_mean),
                "deterministic_group_influence_score": _round_or_none(deterministic_score),
                "group_fragility_score": _round_or_none(fragility_score),
                "group_weight_sum": group_acc[group_key]["group_weight_sum"],
                "group_candidate_status_summary": group_acc[group_key]["group_candidate_status_summary"],
            }
        )
    return coefficient_rows, group_rows


def _recommend_overall_variant(
    weighted_payload: Mapping[str, Any],
    overall_rows: Sequence[Mapping[str, Any]],
) -> Tuple[Optional[str], Optional[str], Optional[str], str]:
    best_by_score = _as_text((weighted_payload.get("summary") or {}).get("recommended_fit_variant"))
    available = [row for row in overall_rows if _as_text(row.get("fit_variant_status")) == "available"]
    if not available:
        return best_by_score or None, None, None, "no_available_fit_variant"

    recommendation_rank = {"robust": 0, "caution": 1, "fragile": 2, "unavailable": 3}

    def _stability_key(row: Mapping[str, Any]) -> Tuple[int, float, float, float]:
        return (
            recommendation_rank.get(_as_text(row.get("robustness_recommendation")), 9),
            float(row.get("max_group_fragility_score") or float("inf")),
            float(row.get("worst_method_weighted_rmse_p95") or float("inf")),
            float(row.get("baseline_weighted_rmse") or float("inf")),
        )

    def _balanced_key(row: Mapping[str, Any]) -> Tuple[int, float, float]:
        return (
            recommendation_rank.get(_as_text(row.get("robustness_recommendation")), 9),
            float(row.get("baseline_weighted_rmse") or float("inf")),
            float(row.get("max_group_fragility_score") or float("inf")),
        )

    best_stability = min(available, key=_stability_key)
    best_balanced = min(available, key=_balanced_key)
    reason = (
        f"best_by_score={best_by_score or 'none'};"
        f"best_by_stability={best_stability.get('fit_variant_name')};"
        f"best_balanced={best_balanced.get('fit_variant_name')}"
    )
    return (
        best_by_score or None,
        _as_text(best_stability.get("fit_variant_name")),
        _as_text(best_balanced.get("fit_variant_name")),
        reason,
    )


def build_co2_bootstrap_robustness_audit(
    rows: Iterable[Mapping[str, Any]],
    *,
    fit_stability_payload: Optional[Mapping[str, Any]] = None,
    bootstrap_seed: int = 17,
    bootstrap_rounds: int = 64,
    subsample_rounds: int = 64,
    subsample_group_fraction: float = 0.75,
) -> Dict[str, Any]:
    weighted_payload = build_co2_weighted_fit_advisory(rows)
    fit_stability_payload = fit_stability_payload or build_co2_fit_stability_audit(rows)
    candidate_points = _numeric_candidate_points(rows)
    evaluation_points = _recommended_eval_points(candidate_points)
    excluded_points = _excluded_points(candidate_points)
    baseline_map = {
        _as_text(row.get("fit_variant_name")): dict(row)
        for row in (weighted_payload.get("fit_variants") or [])
        if _as_text(row.get("fit_variant_name"))
    }
    deterministic_influence = _deterministic_influence_map(fit_stability_payload)

    point_rows = list(weighted_payload.get("points") or [])
    resample_summaries: List[Dict[str, Any]] = []
    overall_rows: List[Dict[str, Any]] = []
    coefficient_rows: List[Dict[str, Any]] = []
    group_rows: List[Dict[str, Any]] = []

    rng_bootstrap = Random(int(bootstrap_seed))
    rng_subsample = Random(int(bootstrap_seed) + 101)

    for spec in _variant_specs():
        variant_name = _as_text(spec.get("fit_variant_name"))
        baseline_variant = baseline_map.get(variant_name)
        if not baseline_variant:
            continue
        training_points = _training_points_for_variant(evaluation_points, variant_name=variant_name)

        bootstrap_coeffs, bootstrap_groups = _run_resample_method(
            fit_variant_name=variant_name,
            weighted=bool(spec.get("weighted")),
            training_points=training_points,
            evaluation_points=evaluation_points,
            baseline_variant=baseline_variant,
            resample_method="grouped_bootstrap",
            rounds=bootstrap_rounds,
            rng=rng_bootstrap,
            group_fraction=subsample_group_fraction,
            deterministic_influence=deterministic_influence,
        )
        subsample_coeffs, subsample_groups = _run_resample_method(
            fit_variant_name=variant_name,
            weighted=bool(spec.get("weighted")),
            training_points=training_points,
            evaluation_points=evaluation_points,
            baseline_variant=baseline_variant,
            resample_method="group_subsample",
            rounds=subsample_rounds,
            rng=rng_subsample,
            group_fraction=subsample_group_fraction,
            deterministic_influence=deterministic_influence,
        )
        coefficient_rows.extend(bootstrap_coeffs)
        coefficient_rows.extend(subsample_coeffs)
        group_rows.extend(bootstrap_groups)
        group_rows.extend(subsample_groups)

        bootstrap_summary = _distribution_row(
            fit_variant_name=variant_name,
            resample_method="grouped_bootstrap",
            baseline_variant=baseline_variant,
            coefficient_rows=bootstrap_coeffs,
            group_summary_rows=bootstrap_groups,
        )
        subsample_summary = _distribution_row(
            fit_variant_name=variant_name,
            resample_method="group_subsample",
            baseline_variant=baseline_variant,
            coefficient_rows=subsample_coeffs,
            group_summary_rows=subsample_groups,
        )
        resample_summaries.extend([bootstrap_summary, subsample_summary])

        combined = [bootstrap_summary, subsample_summary]
        max_group_row = max(
            combined,
            key=lambda row: float(row.get("max_group_fragility_score") or -1.0),
            default=None,
        )
        recommendation_rank = {"robust": 0, "caution": 1, "fragile": 2, "unavailable": 3}
        overall_recommendation = max(
            (_as_text(row.get("robustness_recommendation")) for row in combined),
            key=lambda name: recommendation_rank.get(name, 9),
        )
        overall_rows.append(
            {
                "analysis_scope": "variant_overall",
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
                "baseline_weighted_rmse": _as_float(baseline_variant.get("weighted_rmse")),
                "baseline_rmse": _as_float(baseline_variant.get("rmse")),
                "worst_method_robustness": overall_recommendation,
                "worst_method_weighted_rmse_p95": max(
                    float(row.get("weighted_rmse_p95") or 0.0) for row in combined
                ),
                "max_group_fragility_score": max(
                    float(row.get("max_group_fragility_score") or 0.0) for row in combined
                ),
                "most_fragile_group": _as_text(max_group_row.get("most_fragile_group")) if max_group_row else "",
                "robustness_recommendation": overall_recommendation,
                "fit_robustness_reason_chain": (
                    f"bootstrap={bootstrap_summary.get('robustness_recommendation')};"
                    f"subsample={subsample_summary.get('robustness_recommendation')};"
                    f"baseline_weighted_rmse={baseline_variant.get('weighted_rmse')}"
                ),
            }
        )

    best_by_score, best_by_stability, best_balanced_choice, recommendation_reason = _recommend_overall_variant(
        weighted_payload,
        overall_rows,
    )
    for row in overall_rows:
        row["best_by_score"] = _as_text(row.get("fit_variant_name")) == _as_text(best_by_score)
        row["best_by_stability"] = _as_text(row.get("fit_variant_name")) == _as_text(best_by_stability)
        row["best_balanced_choice"] = _as_text(row.get("fit_variant_name")) == _as_text(best_balanced_choice)
        if row["best_balanced_choice"]:
            row["fit_robustness_reason_chain"] = (
                _as_text(row.get("fit_robustness_reason_chain")) + ";" + recommendation_reason
            )

    grouped_rank: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in group_rows:
        grouped_rank[_as_text(row.get("fit_variant_name"))].append(row)
    for _, rows_for_variant in grouped_rank.items():
        ranked = sorted(
            rows_for_variant,
            key=lambda row: float(row.get("group_fragility_score") or -1.0),
            reverse=True,
        )
        for index, row in enumerate(ranked, start=1):
            row["fragile_group_rank"] = index

    overall_summary = {
        "point_count_total": len(candidate_points),
        "evaluation_point_count": len(evaluation_points),
        "excluded_point_count": len(excluded_points),
        "group_count_total": len({_as_text(row.get("co2_calibration_group_key")) for row in candidate_points}),
        "best_by_score": best_by_score or "",
        "best_by_stability": best_by_stability or "",
        "best_balanced_choice": best_balanced_choice or "",
        "recommendation_reason": recommendation_reason,
        "excluded_reason_summary": ";".join(
            f"{reason}:{count}"
            for reason, count in Counter(
                "hard_blocked" if _as_bool(row.get("co2_calibration_candidate_hard_blocked")) else "not_recommended"
                for row in excluded_points
            ).most_common(4)
        ),
        "bootstrap_seed": int(bootstrap_seed),
        "bootstrap_rounds": int(bootstrap_rounds),
        "subsample_rounds": int(subsample_rounds),
        "subsample_group_fraction": float(subsample_group_fraction),
        "evidence_source": "replay_or_exported_v1_weighted_fit_and_stability_sidecars",
        "not_real_acceptance_evidence": True,
    }
    return {
        "summary": overall_summary,
        "weighted_fit_advisory_summary": weighted_payload.get("summary") or {},
        "fit_stability_summary": fit_stability_payload.get("summary") or {},
        "fit_variants": weighted_payload.get("fit_variants") or [],
        "resample_summaries": resample_summaries,
        "variant_overall": overall_rows,
        "points": point_rows,
        "groups": group_rows,
        "coefficients": coefficient_rows,
    }


def render_co2_bootstrap_robustness_report(payload: Mapping[str, Any]) -> str:
    summary = dict(payload.get("summary") or {})
    overall_rows = list(payload.get("variant_overall") or [])
    group_rows = list(payload.get("groups") or [])
    lines = [
        "# V1 CO2 grouped bootstrap robustness audit",
        "",
        "> replay evidence only",
        "> not real acceptance evidence",
        "",
        "## 总览",
        f"- 总点数: {summary.get('point_count_total', 0)}",
        f"- 进入拟合评估的点数: {summary.get('evaluation_point_count', 0)}",
        f"- 排除点数: {summary.get('excluded_point_count', 0)}",
        f"- 总组数: {summary.get('group_count_total', 0)}",
        f"- best by score: {summary.get('best_by_score', '') or 'none'}",
        f"- best by stability: {summary.get('best_by_stability', '') or 'none'}",
        f"- best balanced choice: {summary.get('best_balanced_choice', '') or 'none'}",
        f"- 推荐理由: {summary.get('recommendation_reason', '')}",
        "",
        "## 变体稳健性摘要",
    ]
    for row in overall_rows:
        lines.append(
            "- "
            + f"{row.get('fit_variant_name')}: "
            + f"robustness={row.get('robustness_recommendation')} "
            + f"baseline_weighted_rmse={row.get('baseline_weighted_rmse')} "
            + f"max_group_fragility={row.get('max_group_fragility_score')} "
            + f"best_by_score={row.get('best_by_score')} "
            + f"best_by_stability={row.get('best_by_stability')} "
            + f"best_balanced={row.get('best_balanced_choice')}"
        )
    lines.extend(["", "## 高杠杆 / fragile groups"])
    ranked = sorted(
        [row for row in group_rows if _as_text(row.get("resample_method")) == "group_subsample"],
        key=lambda row: float(row.get("group_fragility_score") or -1.0),
        reverse=True,
    )
    if not ranked:
        lines.append("- 没有可用的 grouped resample 结果")
    else:
        for row in ranked[:10]:
            lines.append(
                "- "
                + f"{row.get('fit_variant_name')} / {row.get('calibration_group_key')}: "
                + f"fragility={row.get('group_fragility_score')} "
                + f"bootstrap_inclusion={row.get('inclusion_frequency')} "
                + f"deterministic_influence={row.get('deterministic_group_influence_score')}"
            )
    lines.extend(
        [
            "",
            "## 说明",
            "- 本 sidecar 只消费现有 grouped calibration candidate pack、weighted fit advisory 和 fit stability audit 证据。",
            "- 当前实现增加了 grouped bootstrap 与 repeated group subsample，但没有改变任何 live measured value。",
            "- 如果某个变体分数更优但 grouped resample 方差更大，报告会把“更优”和“更稳”明确分开。",
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


def write_co2_bootstrap_robustness_artifacts(output_dir: Path, payload: Mapping[str, Any]) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    points_path = output_dir / "bootstrap_fit_points.csv"
    groups_path = output_dir / "bootstrap_fit_groups.csv"
    coeffs_path = output_dir / "bootstrap_coefficients.csv"
    summary_csv_path = output_dir / "bootstrap_fit_summary.csv"
    summary_json_path = output_dir / "bootstrap_fit_summary.json"
    report_path = output_dir / "bootstrap_fit_report.md"

    write_csv_rows(points_path, payload.get("points") or [])
    write_csv_rows(groups_path, payload.get("groups") or [])
    write_csv_rows(coeffs_path, payload.get("coefficients") or [])
    summary_rows = list(payload.get("resample_summaries") or []) + list(payload.get("variant_overall") or [])
    write_csv_rows(summary_csv_path, summary_rows)
    with summary_json_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    report_path.write_text(render_co2_bootstrap_robustness_report(payload), encoding="utf-8")
    return {
        "points_csv": points_path,
        "groups_csv": groups_path,
        "coefficients_csv": coeffs_path,
        "summary_csv": summary_csv_path,
        "summary_json": summary_json_path,
        "report_md": report_path,
    }
