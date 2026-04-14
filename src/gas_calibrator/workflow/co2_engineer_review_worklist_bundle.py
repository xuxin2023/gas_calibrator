"""V1 CO2 engineer review worklist / decision packet bundle.

This sidecar stays advisory-only. It does not add a new verdict layer and does
not change any existing fit, release-readiness, or live-path behavior. Instead
it reorganizes the current sidecar evidence into an engineer-facing review
packet that answers: what should be reviewed first, why, and which candidate /
segment / window / point is driving that recommendation.
"""

from __future__ import annotations

from collections import Counter, defaultdict
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .co2_calibration_candidate_pack import _as_bool, _as_text, write_csv_rows
from .co2_fit_arbitration_bundle import build_co2_fit_arbitration_bundle
from .co2_fit_evidence_coverage_bundle import build_co2_fit_evidence_coverage_bundle
from .co2_point_evidence_provenance_bundle import build_co2_point_evidence_provenance_bundle
from .co2_release_readiness_bundle import build_co2_release_readiness_bundle


_PRIORITY_ORDER = {"high": 0, "medium": 1, "low": 2, "none": 3}
_SEVERITY_ORDER = {"high": 0, "medium": 1, "low": 2}


def _priority_sort_key(row: Mapping[str, Any]) -> tuple:
    return (
        _PRIORITY_ORDER.get(_as_text(row.get("review_priority")), 9),
        _SEVERITY_ORDER.get(_as_text(row.get("severity")), 9),
        _as_text(row.get("review_item_id")),
    )


def _candidate_dispersion_status(
    candidate_name: str,
    candidate_segment_support: Sequence[Mapping[str, Any]],
) -> str:
    rows = [
        dict(row)
        for row in candidate_segment_support
        if _as_text(row.get("candidate_name")) == candidate_name
        and int(row.get("participating_points_count") or 0) > 0
    ]
    if not rows:
        return "unsupported"
    total = sum(int(row.get("participating_points_count") or 0) for row in rows)
    dominant = max(int(row.get("participating_points_count") or 0) for row in rows)
    if len(rows) == 1:
        return "single_segment_only"
    if total > 0 and dominant / total >= 0.8:
        return "single_segment_dominant"
    return "distributed"


def _dominant_segment_for_candidate(
    candidate_name: str,
    candidate_segment_support: Sequence[Mapping[str, Any]],
) -> str:
    rows = [
        dict(row)
        for row in candidate_segment_support
        if _as_text(row.get("candidate_name")) == candidate_name
        and int(row.get("participating_points_count") or 0) > 0
    ]
    if not rows:
        return ""
    rows.sort(
        key=lambda row: (
            -int(row.get("release_ready_points_count") or 0),
            -int(row.get("participating_points_count") or 0),
            _as_text(row.get("source_segment_id")),
        )
    )
    return _as_text(rows[0].get("source_segment_id"))


def _candidate_top_blocking_reason(
    coverage_row: Mapping[str, Any],
    dispersion_status: str,
) -> str:
    status = _as_text(coverage_row.get("coverage_support_status"))
    if status == "unsupported":
        return "no_participating_points"
    if status == "manual_review":
        return "manual_review_points_present"
    if dispersion_status == "single_segment_only":
        return "single_segment_support"
    if dispersion_status == "single_segment_dominant":
        return "single_segment_dominant"
    if int(coverage_row.get("fallback_usable_points_count") or 0) > 0:
        return "fallback_support_present"
    if int(coverage_row.get("excluded_points_count") or 0) > 0:
        return "excluded_points_present"
    return "aligned_release_support"


def _candidate_review_priority(
    *,
    candidate_name: str,
    coverage_row: Mapping[str, Any],
    fit_summary: Mapping[str, Any],
    coverage_summary: Mapping[str, Any],
    dispersion_status: str,
) -> tuple[str, bool, str]:
    recommended_release_candidate = _as_text(
        coverage_summary.get("recommended_release_candidate")
    )
    best_supported_candidate = _as_text(
        coverage_summary.get("best_supported_candidate")
    )
    is_key_candidate = candidate_name in {
        _as_text(fit_summary.get("best_by_score")),
        _as_text(fit_summary.get("best_by_stability")),
        _as_text(fit_summary.get("best_balanced_choice")),
        recommended_release_candidate,
        best_supported_candidate,
    }
    support_status = _as_text(coverage_row.get("coverage_support_status"))
    fallback_count = int(coverage_row.get("fallback_usable_points_count") or 0)

    if (
        recommended_release_candidate
        and best_supported_candidate
        and recommended_release_candidate != best_supported_candidate
        and candidate_name in {recommended_release_candidate, best_supported_candidate}
    ):
        return "high", True, "compare_score_vs_support_before_release"
    if is_key_candidate and support_status in {"manual_review", "unsupported"}:
        return "high", True, "do_not_release_without_manual_review"
    if is_key_candidate and dispersion_status == "single_segment_only":
        return "high", True, "inspect_single_segment_support"
    if is_key_candidate and dispersion_status == "single_segment_dominant":
        return "medium", True, "inspect_single_segment_support"
    if support_status == "score_supported" or fallback_count > 0:
        return "medium", False, "review_fallback_points_before_release"
    return "low", False, "candidate_support_looks_consistent"


def _candidate_review_summary(
    coverage_row: Mapping[str, Any],
    *,
    dispersion_status: str,
    dominant_segment: str,
) -> str:
    return ";".join(
        [
            f"support_status={_as_text(coverage_row.get('coverage_support_status')) or 'unknown'}",
            f"release_ready={int(coverage_row.get('release_ready_points_count') or 0)}",
            f"score_path_eligible={int(coverage_row.get('score_path_eligible_points_count') or 0)}",
            f"fallback_usable={int(coverage_row.get('fallback_usable_points_count') or 0)}",
            f"manual_review={int(coverage_row.get('manual_review_points_count') or 0)}",
            f"excluded={int(coverage_row.get('excluded_points_count') or 0)}",
            f"support_dispersion={dispersion_status}",
            f"dominant_segment={dominant_segment or 'none'}",
            _as_text(coverage_row.get("coverage_reason_chain")),
        ]
    )


def build_co2_candidate_review_worklist(
    *,
    fit_arbitration_payload: Mapping[str, Any],
    fit_evidence_coverage_payload: Mapping[str, Any],
    provenance_payload: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    fit_summary = dict(fit_arbitration_payload.get("summary") or {})
    coverage_summary = dict(fit_evidence_coverage_payload.get("summary") or {})
    candidate_segment_support = list(provenance_payload.get("candidate_segment_support") or [])
    output: List[Dict[str, Any]] = []

    for coverage_row in fit_evidence_coverage_payload.get("candidate_coverage") or []:
        candidate_name = _as_text(coverage_row.get("candidate_name"))
        dispersion_status = _candidate_dispersion_status(
            candidate_name, candidate_segment_support
        )
        dominant_segment = _dominant_segment_for_candidate(
            candidate_name, candidate_segment_support
        )
        review_priority, manual_review_required, suggested_action = (
            _candidate_review_priority(
                candidate_name=candidate_name,
                coverage_row=coverage_row,
                fit_summary=fit_summary,
                coverage_summary=coverage_summary,
                dispersion_status=dispersion_status,
            )
        )
        output.append(
            {
                "candidate_name": candidate_name,
                "best_by_score": candidate_name
                == _as_text(fit_summary.get("best_by_score")),
                "best_by_stability": candidate_name
                == _as_text(fit_summary.get("best_by_stability")),
                "best_balanced_choice": candidate_name
                == _as_text(fit_summary.get("best_balanced_choice")),
                "recommended_release_candidate": candidate_name
                == _as_text(coverage_summary.get("recommended_release_candidate")),
                "best_supported_candidate": candidate_name
                == _as_text(coverage_summary.get("best_supported_candidate")),
                "best_supported_release_candidate": candidate_name
                == _as_text(coverage_summary.get("best_supported_release_candidate")),
                "coverage_support_status": _as_text(
                    coverage_row.get("coverage_support_status")
                ),
                "support_dispersion_status": dispersion_status,
                "dominant_support_segment": dominant_segment,
                "review_priority": review_priority,
                "manual_review_required": manual_review_required,
                "top_blocking_reason": _candidate_top_blocking_reason(
                    coverage_row, dispersion_status
                ),
                "candidate_review_summary": _candidate_review_summary(
                    coverage_row,
                    dispersion_status=dispersion_status,
                    dominant_segment=dominant_segment,
                ),
                "suggested_engineer_action": suggested_action,
            }
        )
    output.sort(
        key=lambda row: (
            _PRIORITY_ORDER.get(_as_text(row.get("review_priority")), 9),
            not _as_bool(row.get("recommended_release_candidate")),
            not _as_bool(row.get("best_supported_candidate")),
            _as_text(row.get("candidate_name")),
        )
    )
    return output


def _related_candidates_map(
    fit_evidence_coverage_payload: Mapping[str, Any],
) -> Dict[str, List[str]]:
    grouped: Dict[str, set[str]] = defaultdict(set)
    for row in fit_evidence_coverage_payload.get("point_traceability") or []:
        point_id = _as_text(row.get("point_id"))
        candidate_name = _as_text(row.get("candidate_name"))
        if (
            point_id
            and candidate_name
            and _as_text(row.get("fit_participation_status")).startswith("participating_")
        ):
            grouped[point_id].add(candidate_name)
    return {point_id: sorted(names) for point_id, names in grouped.items()}


def _add_review_item(
    items: List[Dict[str, Any]],
    *,
    review_item_id: str,
    review_item_type: str,
    severity: str,
    review_priority: str,
    issue_code: str,
    issue_summary: str,
    blocking_reason_chain: str,
    suggested_engineer_action: str,
    related_candidate_names: Sequence[str],
    point_id: str = "",
    source_segment_id: str = "",
    sampling_window_id: str = "",
) -> None:
    items.append(
        {
            "review_item_id": review_item_id,
            "review_item_type": review_item_type,
            "point_id": point_id,
            "source_segment_id": source_segment_id,
            "sampling_window_id": sampling_window_id,
            "severity": severity,
            "review_priority": review_priority,
            "issue_code": issue_code,
            "issue_summary": issue_summary,
            "blocking_reason_chain": blocking_reason_chain,
            "suggested_engineer_action": suggested_engineer_action,
            "related_candidate_names": ";".join(
                sorted({name for name in related_candidate_names if name})
            ),
        }
    )


def build_co2_engineer_review_items(
    *,
    fit_arbitration_payload: Mapping[str, Any],
    fit_evidence_coverage_payload: Mapping[str, Any],
    provenance_payload: Mapping[str, Any],
    candidate_review_worklist: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    coverage_summary = dict(fit_evidence_coverage_payload.get("summary") or {})
    related_candidates = _related_candidates_map(fit_evidence_coverage_payload)
    items: List[Dict[str, Any]] = []

    recommended_release_candidate = _as_text(
        coverage_summary.get("recommended_release_candidate")
    )
    best_supported_candidate = _as_text(
        coverage_summary.get("best_supported_candidate")
    )
    if (
        recommended_release_candidate
        and best_supported_candidate
        and recommended_release_candidate != best_supported_candidate
    ):
        _add_review_item(
            items,
            review_item_id="candidate-support-conflict",
            review_item_type="candidate",
            severity="high",
            review_priority="high",
            issue_code="fit_support_conflict",
            issue_summary="current release recommendation differs from best-supported candidate",
            blocking_reason_chain=_as_text(coverage_summary.get("summary_reason_chain")),
            suggested_engineer_action="compare_score_vs_support_before_release",
            related_candidate_names=[
                recommended_release_candidate,
                best_supported_candidate,
            ],
        )

    for row in candidate_review_worklist:
        candidate_name = _as_text(row.get("candidate_name"))
        dispersion_status = _as_text(row.get("support_dispersion_status"))
        if dispersion_status in {"single_segment_only", "single_segment_dominant"} and (
            _as_bool(row.get("best_supported_candidate"))
            or _as_bool(row.get("recommended_release_candidate"))
            or _as_bool(row.get("best_balanced_choice"))
        ):
            _add_review_item(
                items,
                review_item_id=f"candidate-dispersion-{candidate_name}",
                review_item_type="candidate",
                severity="high" if dispersion_status == "single_segment_only" else "medium",
                review_priority="high"
                if dispersion_status == "single_segment_only"
                else "medium",
                issue_code="single_segment_support",
                issue_summary=f"{candidate_name} relies on a single dominant source segment",
                blocking_reason_chain=_as_text(row.get("candidate_review_summary")),
                suggested_engineer_action="inspect_single_segment_support",
                related_candidate_names=[candidate_name],
                source_segment_id=_as_text(row.get("dominant_support_segment")),
            )

    for point in provenance_payload.get("points") or []:
        point_id = _as_text(point.get("point_id"))
        related = related_candidates.get(point_id, [])
        release_status = _as_text(point.get("release_readiness_status"))
        sampling_status = _as_text(point.get("sampling_settle_status"))
        reason_chain = _as_text(point.get("reason_chain")) or _as_text(
            point.get("provenance_reason_chain")
        )
        if release_status == "manual_review":
            suggested = "check_temporal_anomaly_points"
            if "fallback" in _as_text(point.get("sampling_window_id")):
                suggested = "review_fallback_points_before_release"
            _add_review_item(
                items,
                review_item_id=f"point-{point_id}",
                review_item_type="point",
                point_id=point_id,
                source_segment_id=_as_text(point.get("source_segment_id")),
                sampling_window_id=_as_text(point.get("sampling_window_id")),
                severity="high",
                review_priority="high",
                issue_code="temporal_or_sampling_manual_review",
                issue_summary=f"{point_id} requires manual review before release review use",
                blocking_reason_chain=reason_chain,
                suggested_engineer_action=suggested,
                related_candidate_names=related,
            )
        elif release_status == "excluded":
            suggested = "verify_untrusted_source_points"
            if "timestamp" in reason_chain or _as_text(point.get("temporal_status")) == "fail":
                suggested = "check_temporal_anomaly_points"
            _add_review_item(
                items,
                review_item_id=f"point-{point_id}",
                review_item_type="point",
                point_id=point_id,
                source_segment_id=_as_text(point.get("source_segment_id")),
                sampling_window_id=_as_text(point.get("sampling_window_id")),
                severity="high",
                review_priority="high",
                issue_code="excluded_point",
                issue_summary=f"{point_id} is excluded from release-support evidence",
                blocking_reason_chain=reason_chain,
                suggested_engineer_action=suggested,
                related_candidate_names=related,
            )
        elif sampling_status == "fallback_but_usable" and _as_bool(
            point.get("score_path_eligibility")
        ):
            _add_review_item(
                items,
                review_item_id=f"window-{_as_text(point.get('sampling_window_id'))}",
                review_item_type="window",
                point_id=point_id,
                source_segment_id=_as_text(point.get("source_segment_id")),
                sampling_window_id=_as_text(point.get("sampling_window_id")),
                severity="medium",
                review_priority="medium",
                issue_code="fallback_window_support",
                issue_summary=f"{point_id} participates through a fallback window on the score path",
                blocking_reason_chain=reason_chain,
                suggested_engineer_action="review_fallback_points_before_release",
                related_candidate_names=related,
            )

    for segment in provenance_payload.get("segment_quality_summary") or []:
        status = _as_text(segment.get("segment_quality_status"))
        source_segment_id = _as_text(segment.get("source_segment_id"))
        if status in {"manual_review", "blocked"}:
            _add_review_item(
                items,
                review_item_id=f"segment-{source_segment_id}",
                review_item_type="segment",
                source_segment_id=source_segment_id,
                severity="high" if status == "blocked" else "medium",
                review_priority="high" if status == "blocked" else "medium",
                issue_code="segment_quality_attention",
                issue_summary=f"{source_segment_id} needs engineer review",
                blocking_reason_chain=_as_text(
                    segment.get("segment_quality_reason_chain")
                ),
                suggested_engineer_action="inspect_single_segment_support"
                if status == "manual_review"
                else "verify_untrusted_source_points",
                related_candidate_names=[
                    _as_text(row.get("candidate_name"))
                    for row in provenance_payload.get("candidate_segment_support") or []
                    if _as_text(row.get("source_segment_id")) == source_segment_id
                    and int(row.get("participating_points_count") or 0) > 0
                ],
            )

    items.sort(key=_priority_sort_key)
    return items


def build_co2_engineer_review_worklist_bundle(
    rows: Optional[Iterable[Mapping[str, Any]]] = None,
    *,
    fit_arbitration_payload: Optional[Mapping[str, Any]] = None,
    fit_evidence_coverage_payload: Optional[Mapping[str, Any]] = None,
    release_readiness_payload: Optional[Mapping[str, Any]] = None,
    provenance_payload: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    source_rows = [dict(row) for row in rows] if rows is not None else []
    if fit_arbitration_payload is None:
        fit_arbitration_payload = build_co2_fit_arbitration_bundle(source_rows)
    if release_readiness_payload is None:
        release_readiness_payload = build_co2_release_readiness_bundle(
            source_rows,
            fit_arbitration_payload=fit_arbitration_payload,
        )
    if fit_evidence_coverage_payload is None:
        fit_evidence_coverage_payload = build_co2_fit_evidence_coverage_bundle(
            source_rows,
            fit_arbitration_payload=fit_arbitration_payload,
            release_readiness_payload=release_readiness_payload,
        )
    if provenance_payload is None:
        provenance_payload = build_co2_point_evidence_provenance_bundle(
            fit_evidence_coverage_payload=fit_evidence_coverage_payload,
            release_readiness_payload=release_readiness_payload,
        )

    candidate_review_worklist = build_co2_candidate_review_worklist(
        fit_arbitration_payload=fit_arbitration_payload,
        fit_evidence_coverage_payload=fit_evidence_coverage_payload,
        provenance_payload=provenance_payload,
    )
    review_items = build_co2_engineer_review_items(
        fit_arbitration_payload=fit_arbitration_payload,
        fit_evidence_coverage_payload=fit_evidence_coverage_payload,
        provenance_payload=provenance_payload,
        candidate_review_worklist=candidate_review_worklist,
    )

    fit_summary = dict(fit_arbitration_payload.get("summary") or {})
    coverage_summary = dict(fit_evidence_coverage_payload.get("summary") or {})
    provenance_summary = dict(provenance_payload.get("summary") or {})
    top_review_priority = (
        _as_text(review_items[0].get("review_priority")) if review_items else "low"
    )
    top_risk_segment = next(
        (
            _as_text(item.get("source_segment_id"))
            for item in review_items
            if _as_text(item.get("review_item_type")) == "segment"
        ),
        "",
    )
    top_risk_window = next(
        (
            _as_text(item.get("sampling_window_id"))
            for item in review_items
            if _as_text(item.get("review_item_type")) == "window"
        ),
        "",
    )
    manual_review_required = _as_bool(
        coverage_summary.get("manual_review_required")
    ) or top_review_priority == "high"
    if not _as_text(coverage_summary.get("recommended_release_candidate")):
        decision_packet_verdict = "manual_review"
    elif top_review_priority == "high":
        decision_packet_verdict = "manual_review"
    elif top_review_priority == "medium":
        decision_packet_verdict = "review_with_caution"
    else:
        decision_packet_verdict = "aligned_low_risk"

    action_counter = Counter(
        _as_text(item.get("suggested_engineer_action"))
        for item in review_items
        if _as_text(item.get("suggested_engineer_action"))
    )
    engineer_next_actions = ";".join(
        action for action, _ in action_counter.most_common(4)
    )
    summary_reason_chain = ";".join(
        part
        for part in [
            f"best_fit_candidate={_as_text(coverage_summary.get('best_fit_candidate')) or _as_text(fit_summary.get('recommended_release_candidate')) or 'none'}",
            f"best_supported_candidate={_as_text(coverage_summary.get('best_supported_candidate')) or 'none'}",
            f"recommended_release_candidate={_as_text(coverage_summary.get('recommended_release_candidate')) or 'none'}",
            f"support_dispersion_status={_as_text(provenance_summary.get('support_dispersion_status')) or 'unknown'}",
            f"top_review_priority={top_review_priority}",
            f"decision_packet_verdict={decision_packet_verdict}",
        ]
        if part
    )

    summary = {
        "decision_packet_verdict": decision_packet_verdict,
        "manual_review_required": manual_review_required,
        "top_review_priority": top_review_priority,
        "recommended_release_candidate": _as_text(
            coverage_summary.get("recommended_release_candidate")
        ),
        "best_supported_candidate": _as_text(
            coverage_summary.get("best_supported_candidate")
        ),
        "best_supported_release_candidate": _as_text(
            coverage_summary.get("best_supported_release_candidate")
        ),
        "release_ready_points_count": int(
            coverage_summary.get("release_ready_points_count") or 0
        ),
        "manual_review_points_count": int(
            coverage_summary.get("manual_review_points_count") or 0
        ),
        "excluded_points_count": int(
            coverage_summary.get("excluded_points_count") or 0
        ),
        "top_risk_segment": top_risk_segment,
        "top_risk_window": top_risk_window,
        "summary_reason_chain": summary_reason_chain,
        "engineer_next_actions": engineer_next_actions,
        "evidence_source": "replay_or_exported_v1_fit_sampling_release_provenance_sidecars",
        "not_real_acceptance_evidence": True,
    }

    return {
        "summary": summary,
        "candidate_review_worklist": candidate_review_worklist,
        "review_items": review_items,
        "fit_arbitration_summary": fit_summary,
        "fit_evidence_coverage_summary": coverage_summary,
        "release_readiness_summary": dict(release_readiness_payload.get("summary") or {}),
        "provenance_summary": provenance_summary,
    }


def render_co2_engineer_review_worklist_report(payload: Mapping[str, Any]) -> str:
    summary = dict(payload.get("summary") or {})
    candidate_rows = list(payload.get("candidate_review_worklist") or [])
    review_items = list(payload.get("review_items") or [])
    lines = [
        "# V1 CO2 engineer review worklist / decision packet bundle",
        "",
        "> replay evidence only",
        "> not real acceptance evidence",
        "",
        "## Overview",
        f"- decision_packet_verdict: {summary.get('decision_packet_verdict') or 'unknown'}",
        f"- manual_review_required: {summary.get('manual_review_required')}",
        f"- top_review_priority: {summary.get('top_review_priority') or 'unknown'}",
        f"- recommended_release_candidate: {summary.get('recommended_release_candidate') or 'none'}",
        f"- best_supported_candidate: {summary.get('best_supported_candidate') or 'none'}",
        f"- best_supported_release_candidate: {summary.get('best_supported_release_candidate') or 'none'}",
        f"- release_ready / manual_review / excluded: {summary.get('release_ready_points_count', 0)} / {summary.get('manual_review_points_count', 0)} / {summary.get('excluded_points_count', 0)}",
        f"- top_risk_segment: {summary.get('top_risk_segment') or 'none'}",
        f"- top_risk_window: {summary.get('top_risk_window') or 'none'}",
        f"- engineer_next_actions: {summary.get('engineer_next_actions') or ''}",
        f"- summary_reason_chain: {summary.get('summary_reason_chain') or ''}",
        "",
        "## Candidate Review Worklist",
    ]
    for row in candidate_rows[:12]:
        lines.append(
            "- "
            + f"{row.get('candidate_name')}: "
            + f"priority={row.get('review_priority')} "
            + f"support={row.get('coverage_support_status')} "
            + f"dispersion={row.get('support_dispersion_status')} "
            + f"manual_review={row.get('manual_review_required')} "
            + f"top_blocking={row.get('top_blocking_reason')}"
        )
    lines.extend(["", "## Top Review Items"])
    for row in review_items[:16]:
        lines.append(
            "- "
            + f"{row.get('review_item_type')}:{row.get('review_item_id')} "
            + f"priority={row.get('review_priority')} "
            + f"severity={row.get('severity')} "
            + f"issue={row.get('issue_code')} "
            + f"action={row.get('suggested_engineer_action')}"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "- This packet reorganizes existing sidecar evidence into an engineer-facing review/worklist view. It does not add a new live gate.",
            "- `review_priority` is a conservative ordering hint for what to inspect first. It does not override existing fit arbitration or release-readiness verdicts.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_co2_engineer_review_worklist_artifacts(
    output_dir: Path,
    payload: Mapping[str, Any],
) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    candidate_path = output_dir / "candidate_review_worklist.csv"
    items_path = output_dir / "review_items.csv"
    summary_csv_path = output_dir / "decision_packet_summary.csv"
    summary_json_path = output_dir / "decision_packet_summary.json"
    report_path = output_dir / "decision_packet_report.md"

    write_csv_rows(candidate_path, payload.get("candidate_review_worklist") or [])
    write_csv_rows(items_path, payload.get("review_items") or [])
    write_csv_rows(summary_csv_path, [payload.get("summary") or {}])
    with summary_json_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    report_path.write_text(
        render_co2_engineer_review_worklist_report(payload), encoding="utf-8"
    )
    return {
        "candidate_review_csv": candidate_path,
        "review_items_csv": items_path,
        "summary_csv": summary_csv_path,
        "summary_json": summary_json_path,
        "report_md": report_path,
    }
