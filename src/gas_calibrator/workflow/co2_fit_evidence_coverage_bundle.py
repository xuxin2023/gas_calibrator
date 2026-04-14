"""V1 CO2 fit evidence coverage / point-to-fit traceability bundle.

This sidecar stays advisory-only. It merges the current fit-side bundle
evidence with the release-readiness point evidence so engineers can inspect
which points support each fit variant, how strong that support is, and whether
the current fit recommendation is strongly backed or still review-oriented.
"""

from __future__ import annotations

from collections import Counter
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .co2_calibration_candidate_pack import _as_bool, _as_float, _as_text, write_csv_rows
from .co2_fit_arbitration_bundle import build_co2_fit_arbitration_bundle
from .co2_release_readiness_bundle import build_co2_release_readiness_bundle


def _variant_names(payload: Mapping[str, Any]) -> List[str]:
    variants = [
        _as_text(row.get("fit_variant_name"))
        for row in (payload.get("variants") or [])
        if _as_text(row.get("fit_variant_name"))
    ]
    if variants:
        return variants
    summary = dict(payload.get("summary") or {})
    fallback = [
        _as_text(summary.get("best_by_score")),
        _as_text(summary.get("best_by_stability")),
        _as_text(summary.get("best_balanced_choice")),
        _as_text(summary.get("recommended_release_candidate")),
    ]
    seen: List[str] = []
    for name in fallback:
        if name and name not in seen:
            seen.append(name)
    return seen


def _variant_participation_status(point: Mapping[str, Any], candidate_name: str) -> str:
    hard_blocked = _as_bool(point.get("co2_calibration_candidate_hard_blocked"))
    recommended = _as_bool(point.get("co2_calibration_candidate_recommended"))
    candidate_status = _as_text(point.get("co2_calibration_candidate_status"))
    weight = _as_float(point.get("co2_calibration_weight_recommended")) or 0.0
    score_path_known = point.get("score_path_eligibility") not in (None, "")
    score_path_eligible = _as_bool(point.get("score_path_eligibility"))

    if hard_blocked:
        return "excluded_hard_blocked"
    if not recommended:
        return "excluded_not_recommended"
    # Release-readiness already decides whether a point may stay on the
    # score-oriented advisory path. Coverage should not count points that were
    # explicitly removed from that path as positive fit support.
    if score_path_known and not score_path_eligible:
        return "excluded_not_score_path_eligible"
    if candidate_name == "baseline_unweighted_fit_only" and candidate_status != "fit":
        return "excluded_not_fit_for_variant"
    if candidate_name == "weighted_fit_advisory" and weight <= 0.0:
        return "excluded_zero_weight"
    if candidate_status == "fit":
        return "participating_fit"
    return "participating_advisory"


def _is_participating(status: str) -> bool:
    return status.startswith("participating_")


def _is_fallback_usable(point: Mapping[str, Any]) -> bool:
    if _as_text(point.get("release_readiness_status")) == "manual_review" or _as_bool(
        point.get("manual_review_required")
    ):
        return False
    return (
        _as_text(point.get("measured_value_source")) == "co2_trailing_window_fallback"
        or _as_text(point.get("sampling_settle_status")) == "fallback_but_usable"
        or bool(_as_text(point.get("co2_source_switch_reason")))
    )


def build_co2_fit_evidence_trace_rows(
    *,
    fit_arbitration_payload: Mapping[str, Any],
    release_readiness_payload: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    variant_names = _variant_names(fit_arbitration_payload)
    fit_summary = dict(fit_arbitration_payload.get("summary") or {})
    rows: List[Dict[str, Any]] = []
    for point in release_readiness_payload.get("points") or []:
        for candidate_name in variant_names:
            fit_participation_status = _variant_participation_status(point, candidate_name)
            rows.append(
                {
                    "point_id": _as_text(point.get("point_tag"))
                    or _as_text(point.get("point_row"))
                    or _as_text(point.get("point_no"))
                    or _as_text(point.get("point_title")),
                    "point_title": point.get("point_title"),
                    "candidate_name": candidate_name,
                    "fit_participation_status": fit_participation_status,
                    "sampling_settle_status": point.get("co2_sampling_settle_status"),
                    "sampling_confidence_bucket": point.get("sampling_confidence_bucket")
                    or point.get("co2_sampling_window_confidence"),
                    "release_readiness_status": point.get("release_readiness_status"),
                    "score_path_eligibility": point.get("score_path_eligibility"),
                    "manual_review_required": point.get("manual_review_required"),
                    "blocking_reason_chain": point.get("blocking_reason_chain"),
                    "co2_calibration_candidate_status": point.get("co2_calibration_candidate_status"),
                    "co2_calibration_candidate_recommended": point.get("co2_calibration_candidate_recommended"),
                    "co2_calibration_candidate_hard_blocked": point.get("co2_calibration_candidate_hard_blocked"),
                    "co2_calibration_weight_recommended": point.get("co2_calibration_weight_recommended"),
                    "co2_point_suitability_status": point.get("co2_point_suitability_status"),
                    "measured_value_source": point.get("measured_value_source"),
                    "co2_source_selected": point.get("co2_source_selected"),
                    "co2_source_switch_reason": point.get("co2_source_switch_reason"),
                    "candidate_is_best_by_score": candidate_name == _as_text(fit_summary.get("best_by_score")),
                    "candidate_is_best_by_stability": candidate_name
                    == _as_text(fit_summary.get("best_by_stability")),
                    "candidate_is_best_balanced_choice": candidate_name
                    == _as_text(fit_summary.get("best_balanced_choice")),
                    "candidate_is_fit_recommended_release_candidate": candidate_name
                    == _as_text(fit_summary.get("recommended_release_candidate")),
                }
            )
    return rows


def _top_reason_summary(rows: Sequence[Mapping[str, Any]], key: str, *, top_n: int = 5) -> str:
    counter: Counter[str] = Counter()
    for row in rows:
        for token in _as_text(row.get(key)).split(";"):
            token = token.strip()
            if token:
                counter[token] += 1
    if not counter:
        return ""
    return ";".join(f"{name}:{count}" for name, count in counter.most_common(top_n))


def _coverage_support_status(
    *,
    participating_points_count: int,
    release_ready_points_count: int,
    score_path_eligible_points_count: int,
    manual_review_points_count: int,
    fallback_usable_points_count: int,
) -> str:
    if participating_points_count <= 0:
        return "unsupported"
    if manual_review_points_count > 0:
        return "manual_review"
    if release_ready_points_count == participating_points_count and fallback_usable_points_count == 0:
        return "strong_support"
    if release_ready_points_count == participating_points_count:
        return "release_supported"
    if score_path_eligible_points_count == participating_points_count:
        return "score_supported"
    return "manual_review"


def build_co2_fit_evidence_candidate_coverage(
    trace_rows: Iterable[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in trace_rows:
        payload = dict(row)
        grouped.setdefault(_as_text(payload.get("candidate_name")), []).append(payload)

    output: List[Dict[str, Any]] = []
    for candidate_name, items in sorted(grouped.items(), key=lambda item: item[0]):
        participating = [row for row in items if _is_participating(_as_text(row.get("fit_participation_status")))]
        participating_points_count = len(participating)
        release_ready_points_count = sum(
            1 for row in participating if _as_text(row.get("release_readiness_status")) == "release_ready"
        )
        score_path_eligible_points_count = sum(
            1 for row in participating if _as_bool(row.get("score_path_eligibility"))
        )
        fallback_usable_points_count = sum(1 for row in participating if _is_fallback_usable(row))
        manual_review_points_count = sum(
            1
            for row in participating
            if _as_text(row.get("release_readiness_status")) == "manual_review"
            or _as_bool(row.get("manual_review_required"))
        )
        excluded_points_count = len(items) - participating_points_count
        release_ready_coverage_ratio = (
            round(release_ready_points_count / participating_points_count, 6)
            if participating_points_count > 0
            else 0.0
        )
        score_path_coverage_ratio = (
            round(score_path_eligible_points_count / participating_points_count, 6)
            if participating_points_count > 0
            else 0.0
        )
        support_status = _coverage_support_status(
            participating_points_count=participating_points_count,
            release_ready_points_count=release_ready_points_count,
            score_path_eligible_points_count=score_path_eligible_points_count,
            manual_review_points_count=manual_review_points_count,
            fallback_usable_points_count=fallback_usable_points_count,
        )
        coverage_manual_review_required = support_status in {"manual_review", "unsupported"}
        reason_parts = [
            f"participating={participating_points_count}",
            f"release_ready={release_ready_points_count}",
            f"score_path_eligible={score_path_eligible_points_count}",
            f"fallback_usable={fallback_usable_points_count}",
            f"manual_review={manual_review_points_count}",
            f"excluded={excluded_points_count}",
            f"support_status={support_status}",
        ]
        top_blocking = _top_reason_summary(participating or items, "blocking_reason_chain", top_n=6)
        if top_blocking:
            reason_parts.append(f"top_blocking={top_blocking}")
        output.append(
            {
                "candidate_name": candidate_name,
                "participating_points_count": participating_points_count,
                "release_ready_points_count": release_ready_points_count,
                "score_path_eligible_points_count": score_path_eligible_points_count,
                "fallback_usable_points_count": fallback_usable_points_count,
                "manual_review_points_count": manual_review_points_count,
                "excluded_points_count": excluded_points_count,
                "release_ready_coverage_ratio": release_ready_coverage_ratio,
                "score_path_coverage_ratio": score_path_coverage_ratio,
                "coverage_support_status": support_status,
                "coverage_manual_review_required": coverage_manual_review_required,
                "coverage_reason_chain": ";".join(reason_parts),
            }
        )
    return output


def _candidate_rank(row: Mapping[str, Any]) -> tuple:
    status_order = {
        "strong_support": 0,
        "release_supported": 1,
        "score_supported": 2,
        "manual_review": 3,
        "unsupported": 4,
    }
    candidate_preference = {
        "baseline_unweighted_fit_only": 0,
        "baseline_unweighted_all_recommended": 1,
        "weighted_fit_advisory": 2,
    }
    return (
        status_order.get(_as_text(row.get("coverage_support_status")), 9),
        -float(row.get("release_ready_coverage_ratio") or 0.0),
        -float(row.get("score_path_coverage_ratio") or 0.0),
        int(row.get("manual_review_points_count") or 0),
        int(row.get("fallback_usable_points_count") or 0),
        int(row.get("excluded_points_count") or 0),
        candidate_preference.get(_as_text(row.get("candidate_name")), 9),
        _as_text(row.get("candidate_name")),
    )


def build_co2_fit_evidence_coverage_bundle(
    rows: Optional[Iterable[Mapping[str, Any]]] = None,
    *,
    fit_arbitration_payload: Optional[Mapping[str, Any]] = None,
    release_readiness_payload: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    source_rows = [dict(row) for row in rows] if rows is not None else []
    if fit_arbitration_payload is None:
        fit_arbitration_payload = build_co2_fit_arbitration_bundle(source_rows)
    if release_readiness_payload is None:
        release_readiness_payload = build_co2_release_readiness_bundle(
            source_rows,
            fit_arbitration_payload=fit_arbitration_payload,
        )

    trace_rows = build_co2_fit_evidence_trace_rows(
        fit_arbitration_payload=fit_arbitration_payload,
        release_readiness_payload=release_readiness_payload,
    )
    candidate_coverage = build_co2_fit_evidence_candidate_coverage(trace_rows)
    fit_summary = dict(fit_arbitration_payload.get("summary") or {})
    release_summary = dict(release_readiness_payload.get("summary") or {})
    coverage_map = {_as_text(row.get("candidate_name")): dict(row) for row in candidate_coverage}
    best_supported_candidate = (
        _as_text(min(candidate_coverage, key=_candidate_rank).get("candidate_name")) if candidate_coverage else ""
    )

    fit_recommended_release_candidate = _as_text(fit_summary.get("recommended_release_candidate"))
    fit_manual_review_required = _as_bool(fit_summary.get("manual_review_required"))
    best_supported_release_candidate = ""
    if fit_recommended_release_candidate and not fit_manual_review_required:
        supported_row = coverage_map.get(fit_recommended_release_candidate, {})
        if _as_text(supported_row.get("coverage_support_status")) in {"strong_support", "release_supported"} and not _as_bool(
            supported_row.get("coverage_manual_review_required")
        ):
            best_supported_release_candidate = fit_recommended_release_candidate

    summary_reasons: List[str] = []
    if _as_text(fit_summary.get("best_by_score")):
        summary_reasons.append(f"best_by_score={_as_text(fit_summary.get('best_by_score'))}")
    if _as_text(fit_summary.get("best_by_stability")):
        summary_reasons.append(f"best_by_stability={_as_text(fit_summary.get('best_by_stability'))}")
    if best_supported_candidate:
        summary_reasons.append(f"best_supported_candidate={best_supported_candidate}")
    if fit_recommended_release_candidate and fit_recommended_release_candidate != best_supported_candidate:
        summary_reasons.append("fit_recommendation_differs_from_best_supported")
    if fit_manual_review_required:
        summary_reasons.append("fit_bundle_requires_manual_review")

    if best_supported_release_candidate:
        coverage_summary_verdict = "release_supported_candidate_available"
        manual_review_required = False
    elif (
        best_supported_candidate
        and fit_recommended_release_candidate
        and fit_recommended_release_candidate != best_supported_candidate
    ):
        coverage_summary_verdict = "support_differs_from_fit_recommendation"
        manual_review_required = True
    elif best_supported_candidate and _as_text(
        coverage_map.get(best_supported_candidate, {}).get("coverage_support_status")
    ) == "score_supported":
        coverage_summary_verdict = "score_supported_only"
        manual_review_required = True
    else:
        coverage_summary_verdict = "manual_review"
        manual_review_required = True

    top_coverage_reasons = _top_reason_summary(candidate_coverage, "coverage_reason_chain", top_n=6)
    if top_coverage_reasons:
        summary_reasons.append(f"top_coverage={top_coverage_reasons}")

    summary = {
        "best_fit_candidate": _as_text(release_summary.get("best_fit_candidate"))
        or fit_recommended_release_candidate
        or _as_text(fit_summary.get("best_balanced_choice"))
        or _as_text(fit_summary.get("best_by_score")),
        "best_by_score": _as_text(fit_summary.get("best_by_score")),
        "best_by_stability": _as_text(fit_summary.get("best_by_stability")),
        "best_balanced_choice": _as_text(fit_summary.get("best_balanced_choice")),
        "recommended_release_candidate": fit_recommended_release_candidate,
        "best_supported_candidate": best_supported_candidate,
        "best_supported_release_candidate": best_supported_release_candidate,
        "release_ready_points_count": int(release_summary.get("release_ready_points_count") or 0),
        "score_path_eligible_points_count": int(release_summary.get("score_path_eligible_points_count") or 0),
        "manual_review_points_count": int(release_summary.get("manual_review_points_count") or 0),
        "excluded_points_count": int(release_summary.get("excluded_points_count") or 0),
        "coverage_summary_verdict": coverage_summary_verdict,
        "manual_review_required": manual_review_required,
        "summary_reason_chain": ";".join(summary_reasons),
        "point_count_total": len(release_readiness_payload.get("points") or []),
        "candidate_count_total": len(candidate_coverage),
        "evidence_source": "replay_or_exported_v1_fit_arbitration_and_release_readiness_sidecars",
        "not_real_acceptance_evidence": True,
    }

    return {
        "summary": summary,
        "point_traceability": trace_rows,
        "candidate_coverage": candidate_coverage,
        "fit_arbitration_summary": fit_summary,
        "release_readiness_summary": release_summary,
    }


def render_co2_fit_evidence_coverage_report(payload: Mapping[str, Any]) -> str:
    summary = dict(payload.get("summary") or {})
    candidates = list(payload.get("candidate_coverage") or [])
    lines = [
        "# V1 CO2 fit evidence coverage / point-to-fit traceability bundle",
        "",
        "> replay evidence only",
        "> not real acceptance evidence",
        "",
        "## Overview",
        f"- best_fit_candidate: {summary.get('best_fit_candidate') or 'none'}",
        f"- best_by_score / best_by_stability / best_balanced_choice: {summary.get('best_by_score') or 'none'} / {summary.get('best_by_stability') or 'none'} / {summary.get('best_balanced_choice') or 'none'}",
        f"- recommended_release_candidate: {summary.get('recommended_release_candidate') or 'none'}",
        f"- best_supported_candidate: {summary.get('best_supported_candidate') or 'none'}",
        f"- best_supported_release_candidate: {summary.get('best_supported_release_candidate') or 'none'}",
        f"- release_ready / score_path_eligible / manual_review / excluded: "
        f"{summary.get('release_ready_points_count', 0)} / "
        f"{summary.get('score_path_eligible_points_count', 0)} / "
        f"{summary.get('manual_review_points_count', 0)} / "
        f"{summary.get('excluded_points_count', 0)}",
        f"- coverage_summary_verdict: {summary.get('coverage_summary_verdict') or 'unknown'}",
        f"- manual_review_required: {summary.get('manual_review_required')}",
        f"- summary_reason_chain: {summary.get('summary_reason_chain') or ''}",
        "",
        "## Candidate Coverage",
    ]
    for row in candidates:
        lines.append(
            "- "
            + f"{row.get('candidate_name')}: "
            + f"participating={row.get('participating_points_count')} "
            + f"release_ready={row.get('release_ready_points_count')} "
            + f"score_path={row.get('score_path_eligible_points_count')} "
            + f"fallback={row.get('fallback_usable_points_count')} "
            + f"manual_review={row.get('manual_review_points_count')} "
            + f"excluded={row.get('excluded_points_count')} "
            + f"status={row.get('coverage_support_status')} "
            + f"reasons={row.get('coverage_reason_chain')}"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "- This sidecar merges fit arbitration and release readiness evidence into a point-to-fit coverage view. It does not add another live gate.",
            "- `best_supported_candidate` is an advisory-only explanation field. It does not replace the existing `recommended_release_candidate`.",
            "- fallback-but-usable points may stay on the score-oriented advisory path, but they are tracked separately from release-ready support.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_co2_fit_evidence_coverage_artifacts(output_dir: Path, payload: Mapping[str, Any]) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    point_traceability_path = output_dir / "point_fit_traceability.csv"
    candidate_coverage_path = output_dir / "candidate_coverage.csv"
    summary_csv_path = output_dir / "fit_evidence_coverage_summary.csv"
    summary_json_path = output_dir / "fit_evidence_coverage_summary.json"
    report_path = output_dir / "fit_evidence_coverage_report.md"

    write_csv_rows(point_traceability_path, payload.get("point_traceability") or [])
    write_csv_rows(candidate_coverage_path, payload.get("candidate_coverage") or [])
    write_csv_rows(summary_csv_path, [payload.get("summary") or {}])
    with summary_json_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    report_path.write_text(render_co2_fit_evidence_coverage_report(payload), encoding="utf-8")
    return {
        "point_traceability_csv": point_traceability_path,
        "candidate_coverage_csv": candidate_coverage_path,
        "summary_csv": summary_csv_path,
        "summary_json": summary_json_path,
        "report_md": report_path,
    }
