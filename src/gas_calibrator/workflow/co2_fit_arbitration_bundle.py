"""V1 CO2 fit arbitration / release-candidate advisory bundle helpers."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .co2_bootstrap_robustness_audit import build_co2_bootstrap_robustness_audit
from .co2_fit_stability_audit import (
    build_co2_fit_stability_audit,
    extract_candidate_rows_from_weighted_fit_payload,
)
from .co2_weighted_fit_advisory import (
    _WEAK_SUPPORT_WEIGHTED_RMSE_MARGIN,
    _as_bool,
    _as_float,
    _as_text,
    build_co2_weighted_fit_advisory,
)

_THIN_STRONG_SUPPORT_MIN_POINTS = 2


def _variant_map(rows: Sequence[Mapping[str, Any]], key: str = "fit_variant_name") -> Dict[str, Dict[str, Any]]:
    return {_as_text(row.get(key)): dict(row) for row in rows if _as_text(row.get(key))}


def _bootstrap_to_weighted_payload(bootstrap_payload: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "summary": dict(bootstrap_payload.get("weighted_fit_advisory_summary") or {}),
        "fit_variants": list(bootstrap_payload.get("fit_variants") or []),
        "points": list(bootstrap_payload.get("points") or []),
        "groups": [],
    }


def _source_rows(
    rows: Optional[Iterable[Mapping[str, Any]]],
    *,
    weighted_fit_payload: Optional[Mapping[str, Any]],
    bootstrap_payload: Optional[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    if rows is not None:
        return [dict(row) for row in rows]
    if weighted_fit_payload is not None:
        return extract_candidate_rows_from_weighted_fit_payload(weighted_fit_payload)
    if bootstrap_payload is not None:
        return extract_candidate_rows_from_weighted_fit_payload({"points": bootstrap_payload.get("points") or []})
    return []


def _variant_risk_list(
    *,
    weighted_row: Mapping[str, Any],
    stability_row: Mapping[str, Any],
    robustness_row: Mapping[str, Any],
    best_by_score: str,
    best_by_stability: str,
    best_balanced_choice: str,
) -> List[str]:
    risks: List[str] = []
    variant_name = _as_text(weighted_row.get("fit_variant_name"))
    if _as_text(stability_row.get("stability_recommendation")) in {"fragile", "insufficient_groups", "unavailable"}:
        risks.append(f"deterministic_stability={_as_text(stability_row.get('stability_recommendation'))}")
    if _as_text(robustness_row.get("robustness_recommendation")) == "fragile":
        risks.append("bootstrap_robustness=fragile")
    if _as_text(robustness_row.get("most_fragile_group")):
        risks.append(f"most_fragile_group={_as_text(robustness_row.get('most_fragile_group'))}")
    if _as_text(stability_row.get("most_influential_group")):
        risks.append(f"most_influential_group={_as_text(stability_row.get('most_influential_group'))}")
    if variant_name == best_by_score and variant_name != best_by_stability:
        risks.append("score_leads_stability_conflict")
    if variant_name == best_by_stability and variant_name != best_by_score:
        risks.append("stability_leads_score_conflict")
    if variant_name == best_balanced_choice and variant_name not in {best_by_score, best_by_stability}:
        risks.append("balanced_choice_differs_from_score_and_stability")
    return risks


def _thin_strong_support_guard(
    *,
    candidate: str,
    weighted_variants: Mapping[str, Mapping[str, Any]],
    best_by_score: str,
    best_by_stability: str,
) -> Tuple[str, bool, List[str], bool]:
    if candidate != "weighted_fit_advisory" or best_by_score != "weighted_fit_advisory":
        return candidate, False, [], False

    weighted_row = weighted_variants.get("weighted_fit_advisory", {})
    baseline_row = weighted_variants.get("baseline_unweighted_fit_only", {})
    if _as_text(weighted_row.get("fit_variant_status")) != "available":
        return candidate, False, [], False

    candidate_pool_point_count = int(weighted_row.get("candidate_pool_point_count") or 0)
    strong_support_pool_point_count = int(weighted_row.get("strong_support_pool_point_count") or 0)
    weak_support_pool_ratio = float(weighted_row.get("weak_support_pool_ratio") or 0.0)
    clean_first_applied = _as_bool(weighted_row.get("pre_fit_clean_first_applied"))
    thin_strong_support = (
        clean_first_applied
        and candidate_pool_point_count > strong_support_pool_point_count
        and weak_support_pool_ratio > 0.0
        and strong_support_pool_point_count <= _THIN_STRONG_SUPPORT_MIN_POINTS
    )
    if not thin_strong_support:
        return candidate, False, [], False

    weighted_weighted_rmse = _as_float(weighted_row.get("weighted_rmse"))
    baseline_weighted_rmse = _as_float(baseline_row.get("weighted_rmse"))
    if (
        weighted_weighted_rmse is None
        or baseline_weighted_rmse is None
        or (baseline_weighted_rmse - weighted_weighted_rmse) > _WEAK_SUPPORT_WEIGHTED_RMSE_MARGIN
    ):
        return candidate, False, [], False

    reasons = [
        "thin_strong_support_guard",
        f"weighted_candidate_pool_points={candidate_pool_point_count}",
        f"weighted_strong_support_points={strong_support_pool_point_count}",
        f"weighted_weak_support_pool_ratio={round(weak_support_pool_ratio, 6)}",
        f"weighted_score_edge_vs_baseline={round(baseline_weighted_rmse - weighted_weighted_rmse, 6)}",
    ]

    baseline_available = _as_text(baseline_row.get("fit_variant_status")) == "available"
    baseline_stronger_support = (
        baseline_available
        and int(baseline_row.get("strong_support_pool_point_count") or 0) > strong_support_pool_point_count
        and float(baseline_row.get("weak_support_pool_ratio") or 0.0) <= weak_support_pool_ratio
    )
    if baseline_stronger_support:
        reasons.append("prefer_baseline_unweighted_fit_only_due_to_stronger_support")
        return "baseline_unweighted_fit_only", False, reasons, best_by_stability == "baseline_unweighted_fit_only"

    reasons.append("manual_review_due_to_thin_weighted_support")
    return "", True, reasons, False


def _recommend_release_candidate(
    *,
    weighted_variants: Mapping[str, Mapping[str, Any]],
    stability_variants: Mapping[str, Mapping[str, Any]],
    robustness_variants: Mapping[str, Mapping[str, Any]],
    best_by_score: str,
    best_by_stability: str,
    best_balanced_choice: str,
) -> Tuple[str, bool, str, str]:
    candidate = best_balanced_choice or best_by_stability or best_by_score
    if not candidate or candidate not in weighted_variants:
        return "", True, "no_available_balanced_candidate", "no_balanced_candidate_available"

    weighted_row = weighted_variants.get(candidate, {})
    stability_row = stability_variants.get(candidate, {})
    robustness_row = robustness_variants.get(candidate, {})
    reasons: List[str] = []
    manual_review_required = False

    if _as_text(weighted_row.get("fit_variant_status")) != "available":
        return "", True, "selected_variant_unavailable", "selected_fit_variant_is_unavailable"

    candidate, guard_manual_review, guard_reasons, conflict_resolved_by_guard = _thin_strong_support_guard(
        candidate=candidate,
        weighted_variants=weighted_variants,
        best_by_score=best_by_score,
        best_by_stability=best_by_stability,
    )
    manual_review_required = manual_review_required or guard_manual_review
    reasons.extend(guard_reasons)
    weighted_row = weighted_variants.get(candidate, {}) if candidate else {}
    stability_row = stability_variants.get(candidate, {}) if candidate else {}
    robustness_row = robustness_variants.get(candidate, {}) if candidate else {}

    if best_by_score and best_by_stability and best_by_score != best_by_stability:
        if conflict_resolved_by_guard:
            reasons.append("score_vs_stability_conflict_resolved_by_support_guard")
        else:
            manual_review_required = True
            reasons.append("score_vs_stability_conflict")
    if candidate != best_by_score and best_by_score:
        reasons.append(f"prefer_balanced_over_score:{best_by_score}")
    if candidate != best_by_stability and best_by_stability:
        reasons.append(f"prefer_balanced_over_stability:{best_by_stability}")

    stability_status = _as_text(stability_row.get("stability_recommendation"))
    if stability_status in {"fragile", "insufficient_groups", "unavailable"}:
        manual_review_required = True
        reasons.append(f"deterministic_stability={stability_status}")

    robustness_status = _as_text(robustness_row.get("robustness_recommendation"))
    if robustness_status == "fragile":
        manual_review_required = True
        reasons.append("bootstrap_robustness=fragile")

    if _as_float(robustness_row.get("max_group_fragility_score")) is not None and float(
        robustness_row.get("max_group_fragility_score") or 0.0
    ) > 5.0:
        manual_review_required = True
        reasons.append("high_group_fragility_score")

    if not reasons:
        reasons.append("score_stability_robustness_aligned")

    arbitration_summary = (
        f"best_by_score={best_by_score or 'none'};"
        f"best_by_stability={best_by_stability or 'none'};"
        f"best_balanced_choice={best_balanced_choice or 'none'};"
        f"recommended_release_candidate={candidate or 'none'}"
    )
    return candidate, manual_review_required, ";".join(reasons), arbitration_summary


def build_co2_fit_arbitration_bundle(
    rows: Optional[Iterable[Mapping[str, Any]]] = None,
    *,
    weighted_fit_payload: Optional[Mapping[str, Any]] = None,
    fit_stability_payload: Optional[Mapping[str, Any]] = None,
    bootstrap_payload: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    source_rows = _source_rows(rows, weighted_fit_payload=weighted_fit_payload, bootstrap_payload=bootstrap_payload)
    if weighted_fit_payload is None:
        weighted_fit_payload = _bootstrap_to_weighted_payload(bootstrap_payload) if bootstrap_payload is not None else build_co2_weighted_fit_advisory(source_rows)
    if fit_stability_payload is None:
        fit_stability_payload = build_co2_fit_stability_audit(source_rows)
    if bootstrap_payload is None:
        bootstrap_payload = build_co2_bootstrap_robustness_audit(source_rows, fit_stability_payload=fit_stability_payload)

    weighted_variants = _variant_map(weighted_fit_payload.get("fit_variants") or [])
    stability_variants = _variant_map(fit_stability_payload.get("stability_variants") or [])
    robustness_variants = _variant_map(bootstrap_payload.get("variant_overall") or [])

    best_by_score = _as_text((weighted_fit_payload.get("summary") or {}).get("recommended_fit_variant")) or _as_text((bootstrap_payload.get("summary") or {}).get("best_by_score"))
    best_by_stability = _as_text((bootstrap_payload.get("summary") or {}).get("best_by_stability")) or _as_text((fit_stability_payload.get("summary") or {}).get("recommended_fit_variant"))
    best_balanced_choice = _as_text((bootstrap_payload.get("summary") or {}).get("best_balanced_choice")) or best_by_stability or best_by_score

    variant_rows: List[Dict[str, Any]] = []
    for variant_name in sorted(set(weighted_variants) | set(stability_variants) | set(robustness_variants)):
        weighted_row = weighted_variants.get(variant_name, {})
        stability_row = stability_variants.get(variant_name, {})
        robustness_row = robustness_variants.get(variant_name, {})
        risk_list = _variant_risk_list(
            weighted_row=weighted_row,
            stability_row=stability_row,
            robustness_row=robustness_row,
            best_by_score=best_by_score,
            best_by_stability=best_by_stability,
            best_balanced_choice=best_balanced_choice,
        )
        variant_rows.append(
            {
                "fit_variant_name": variant_name,
                "fit_variant_status": _as_text(weighted_row.get("fit_variant_status")),
                "weighted_fit": _as_bool(weighted_row.get("weighted_fit")),
                "candidate_point_count": int(weighted_row.get("input_point_count") or 0),
                "candidate_group_count": int(weighted_row.get("input_group_count") or 0),
                "rmse": _as_float(weighted_row.get("rmse")),
                "weighted_rmse": _as_float(weighted_row.get("weighted_rmse")),
                "mae": _as_float(weighted_row.get("mae")),
                "bias": _as_float(weighted_row.get("bias")),
                "max_abs_error": _as_float(weighted_row.get("max_abs_error")),
                "intercept": _as_float(weighted_row.get("intercept")),
                "slope": _as_float(weighted_row.get("slope")),
                "stability_recommendation": _as_text(stability_row.get("stability_recommendation")),
                "max_group_influence_score": _as_float(stability_row.get("max_group_influence_score")),
                "most_influential_group": _as_text(stability_row.get("most_influential_group")),
                "robustness_recommendation": _as_text(robustness_row.get("robustness_recommendation")),
                "max_group_fragility_score": _as_float(robustness_row.get("max_group_fragility_score")),
                "most_fragile_group": _as_text(robustness_row.get("most_fragile_group")),
                "worst_method_weighted_rmse_p95": _as_float(robustness_row.get("worst_method_weighted_rmse_p95")),
                "best_by_score": variant_name == best_by_score,
                "best_by_stability": variant_name == best_by_stability,
                "best_balanced_choice": variant_name == best_balanced_choice,
                "variant_top_risks": ";".join(risk_list),
            }
        )

    recommended_release_candidate, manual_review_required, reason_chain, arbitration_summary = _recommend_release_candidate(
        weighted_variants=weighted_variants,
        stability_variants=stability_variants,
        robustness_variants=robustness_variants,
        best_by_score=best_by_score,
        best_by_stability=best_by_stability,
        best_balanced_choice=best_balanced_choice,
    )

    selected_weighted = weighted_variants.get(recommended_release_candidate, {})
    selected_stability = stability_variants.get(recommended_release_candidate, {})
    selected_robustness = robustness_variants.get(recommended_release_candidate, {})
    bundle_status = "not_recommended" if not recommended_release_candidate else "manual_review_required" if manual_review_required else "advisory_only"
    bundle_top_risks = [
        risk
        for risk in [
            _as_text(selected_stability.get("most_influential_group")) and f"most_influential_group={_as_text(selected_stability.get('most_influential_group'))}",
            _as_text(selected_robustness.get("most_fragile_group")) and f"most_fragile_group={_as_text(selected_robustness.get('most_fragile_group'))}",
            _as_text(selected_stability.get("stability_recommendation")) and f"stability={_as_text(selected_stability.get('stability_recommendation'))}",
            _as_text(selected_robustness.get("robustness_recommendation")) and f"robustness={_as_text(selected_robustness.get('robustness_recommendation'))}",
        ]
        if risk
    ]

    recommended_bundle = {
        "fit_variant_name": recommended_release_candidate or "",
        "bundle_status": bundle_status,
        "candidate_point_count": int(selected_weighted.get("input_point_count") or 0),
        "candidate_group_count": int(selected_weighted.get("input_group_count") or 0),
        "coefficients": {
            "intercept": _as_float(selected_weighted.get("intercept")),
            "slope": _as_float(selected_weighted.get("slope")),
        },
        "score_summary": {
            "rmse": _as_float(selected_weighted.get("rmse")),
            "weighted_rmse": _as_float(selected_weighted.get("weighted_rmse")),
            "mae": _as_float(selected_weighted.get("mae")),
            "bias": _as_float(selected_weighted.get("bias")),
            "max_abs_error": _as_float(selected_weighted.get("max_abs_error")),
        },
        "stability_summary": {
            "stability_recommendation": _as_text(selected_stability.get("stability_recommendation")),
            "max_group_influence_score": _as_float(selected_stability.get("max_group_influence_score")),
            "most_influential_group": _as_text(selected_stability.get("most_influential_group")),
        },
        "robustness_summary": {
            "robustness_recommendation": _as_text(selected_robustness.get("robustness_recommendation")),
            "max_group_fragility_score": _as_float(selected_robustness.get("max_group_fragility_score")),
            "most_fragile_group": _as_text(selected_robustness.get("most_fragile_group")),
            "worst_method_weighted_rmse_p95": _as_float(selected_robustness.get("worst_method_weighted_rmse_p95")),
        },
        "top_risks": bundle_top_risks,
        "manual_review_required": manual_review_required,
        "release_candidate_reason_chain": reason_chain,
        "explicit_disclaimer": "replay evidence only; not live deployment; not real acceptance evidence",
    }

    summary = {
        "point_count_total": int((weighted_fit_payload.get("summary") or {}).get("point_count_total") or 0),
        "evaluation_point_count": int((weighted_fit_payload.get("summary") or {}).get("evaluation_point_count") or 0),
        "excluded_point_count": int((weighted_fit_payload.get("summary") or {}).get("excluded_point_count") or 0),
        "group_count_total": int((bootstrap_payload.get("summary") or {}).get("group_count_total") or 0),
        "best_by_score": best_by_score,
        "best_by_stability": best_by_stability,
        "best_balanced_choice": best_balanced_choice,
        "recommended_release_candidate": recommended_release_candidate,
        "manual_review_required": manual_review_required,
        "release_candidate_reason_chain": reason_chain,
        "arbitration_summary": arbitration_summary,
        "evidence_source": "replay_or_exported_v1_candidate_pack_weighted_fit_stability_bootstrap_sidecars",
        "not_real_acceptance_evidence": True,
    }

    return {
        "summary": summary,
        "weighted_fit_advisory_summary": dict(weighted_fit_payload.get("summary") or {}),
        "fit_stability_summary": dict(fit_stability_payload.get("summary") or {}),
        "bootstrap_summary": dict(bootstrap_payload.get("summary") or {}),
        "variants": variant_rows,
        "recommended_coefficient_bundle": recommended_bundle,
        "points": list(weighted_fit_payload.get("points") or []),
        "groups": list(bootstrap_payload.get("groups") or []),
    }


def render_co2_fit_arbitration_report(payload: Mapping[str, Any]) -> str:
    summary = dict(payload.get("summary") or {})
    variants = list(payload.get("variants") or [])
    bundle = dict(payload.get("recommended_coefficient_bundle") or {})
    lines = [
        "# V1 CO2 fit arbitration / release-candidate advisory bundle",
        "",
        "> replay evidence only",
        "> not real acceptance evidence",
        "",
        "## 总览",
        f"- best_by_score: {summary.get('best_by_score') or 'none'}",
        f"- best_by_stability: {summary.get('best_by_stability') or 'none'}",
        f"- best_balanced_choice: {summary.get('best_balanced_choice') or 'none'}",
        f"- recommended_release_candidate: {summary.get('recommended_release_candidate') or 'none'}",
        f"- manual_review_required: {summary.get('manual_review_required')}",
        f"- arbitration_summary: {summary.get('arbitration_summary')}",
        "",
        "## 变体仲裁摘要",
    ]
    for row in variants:
        lines.append(
            "- "
            + f"{row.get('fit_variant_name')}: "
            + f"weighted_rmse={row.get('weighted_rmse')} "
            + f"stability={row.get('stability_recommendation') or 'unknown'} "
            + f"robustness={row.get('robustness_recommendation') or 'unknown'} "
            + f"score={row.get('best_by_score')} "
            + f"stability_best={row.get('best_by_stability')} "
            + f"balanced={row.get('best_balanced_choice')}"
        )
    lines.extend(
        [
            "",
            "## 推荐 bundle",
            f"- bundle_status: {bundle.get('bundle_status') or 'unknown'}",
            f"- fit_variant_name: {bundle.get('fit_variant_name') or 'none'}",
            f"- manual_review_required: {bundle.get('manual_review_required')}",
            f"- top_risks: {';'.join(bundle.get('top_risks') or [])}",
            f"- reason_chain: {bundle.get('release_candidate_reason_chain') or ''}",
            "",
            "## 说明",
            "- 这只是 sidecar 建议包，不会写回 live 配置，也不会触发 writeback。",
            "- 如果 score / stability / robustness 之间冲突较大，报告会把 manual_review_required 标成 true。",
            "- recommended_coefficient_bundle 只是离线评审对象，不是下发包。",
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


def write_co2_fit_arbitration_artifacts(output_dir: Path, payload: Mapping[str, Any]) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    variants_path = output_dir / "fit_arbitration_variants.csv"
    summary_csv_path = output_dir / "fit_arbitration_summary.csv"
    summary_json_path = output_dir / "fit_arbitration_summary.json"
    report_path = output_dir / "fit_arbitration_report.md"
    bundle_json_path = output_dir / "recommended_coefficient_bundle.json"
    bundle_csv_path = output_dir / "recommended_coefficient_bundle.csv"

    write_csv_rows(variants_path, payload.get("variants") or [])
    write_csv_rows(summary_csv_path, [payload.get("summary") or {}])
    with summary_json_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    report_path.write_text(render_co2_fit_arbitration_report(payload), encoding="utf-8")

    bundle = dict(payload.get("recommended_coefficient_bundle") or {})
    with bundle_json_path.open("w", encoding="utf-8") as handle:
        json.dump(bundle, handle, ensure_ascii=False, indent=2)
    bundle_csv_row = dict(bundle)
    bundle_csv_row["top_risks"] = ";".join(bundle.get("top_risks") or [])
    bundle_csv_row["coefficients"] = json.dumps(bundle.get("coefficients") or {}, ensure_ascii=False)
    bundle_csv_row["score_summary"] = json.dumps(bundle.get("score_summary") or {}, ensure_ascii=False)
    bundle_csv_row["stability_summary"] = json.dumps(bundle.get("stability_summary") or {}, ensure_ascii=False)
    bundle_csv_row["robustness_summary"] = json.dumps(bundle.get("robustness_summary") or {}, ensure_ascii=False)
    write_csv_rows(bundle_csv_path, [bundle_csv_row])
    return {
        "variants_csv": variants_path,
        "summary_csv": summary_csv_path,
        "summary_json": summary_json_path,
        "report_md": report_path,
        "bundle_json": bundle_json_path,
        "bundle_csv": bundle_csv_path,
    }
