"""V1 CO2 point evidence provenance / source-segment lineage bundle.

This sidecar stays advisory-only. It does not add any new live gate or change
the existing arbitration / release-readiness verdicts. Instead, it explains
where each point came from, how it participates in the current fit candidate
paths, and which source segments are carrying the strongest support.
"""

from __future__ import annotations

from collections import Counter
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .co2_calibration_candidate_pack import _as_bool, _as_text, write_csv_rows
from .co2_fit_evidence_coverage_bundle import build_co2_fit_evidence_coverage_bundle
from .co2_release_readiness_bundle import build_co2_release_readiness_bundle


def _point_id(point: Mapping[str, Any]) -> str:
    return (
        _as_text(point.get("point_tag"))
        or _as_text(point.get("point_row"))
        or _as_text(point.get("point_no"))
        or _as_text(point.get("point_title"))
    )


def _phase_name(point: Mapping[str, Any]) -> str:
    route = _as_text(point.get("route")).lower()
    if route in {"gas", "co2"}:
        return "co2"
    if route in {"water", "h2o"}:
        return "h2o"
    return "co2"


def _source_segment_id(point: Mapping[str, Any]) -> str:
    return (
        _as_text(point.get("co2_source_segment_selected"))
        or _as_text(point.get("co2_source_selected"))
        or "segment_unknown"
    )


def _source_segment_label(point: Mapping[str, Any]) -> str:
    source = _as_text(point.get("co2_source_selected"))
    segment_id = _source_segment_id(point)
    if source and source not in segment_id:
        return f"{source}:{segment_id}"
    return segment_id


def _sampling_window_id(point: Mapping[str, Any]) -> str:
    segment_id = _source_segment_id(point)
    measured_source = _as_text(point.get("measured_value_source"))
    if _as_bool(point.get("co2_steady_window_found")):
        return f"{segment_id}|steady_window"
    if _as_text(point.get("release_readiness_status")) == "manual_review":
        return f"{segment_id}|manual_review_window"
    if measured_source == "co2_trailing_window_fallback" or _as_text(point.get("co2_sampling_settle_status")) == "fallback_but_usable":
        return f"{segment_id}|fallback_window"
    return f"{segment_id}|window_unknown"


def _sampling_window_label(point: Mapping[str, Any]) -> str:
    window_id = _sampling_window_id(point)
    if window_id.endswith("|steady_window"):
        return "steady-state window"
    if window_id.endswith("|fallback_window"):
        return "fallback trailing window"
    if window_id.endswith("|manual_review_window"):
        return "manual-review window"
    return "window unknown"


def _window_start_hint(point: Mapping[str, Any]) -> str:
    sample_count = _as_text(point.get("co2_steady_window_sample_count"))
    if _as_bool(point.get("co2_steady_window_found")) and sample_count:
        return f"steady_window_samples={sample_count}"
    if _as_text(point.get("measured_value_source")) == "co2_trailing_window_fallback":
        return "trailing_window_start_unknown"
    return "window_start_unknown"


def _window_end_hint(point: Mapping[str, Any]) -> str:
    steady_status = _as_text(point.get("co2_steady_window_status"))
    if steady_status:
        return f"steady_status={steady_status}"
    return f"sampling_status={_as_text(point.get('co2_sampling_settle_status')) or 'unknown'}"


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


def _support_status(
    *,
    participating_points_count: int,
    release_ready_points_count: int,
    manual_review_points_count: int,
    excluded_points_count: int,
    fallback_usable_points_count: int,
) -> str:
    if participating_points_count <= 0:
        return "unsupported"
    if manual_review_points_count > 0:
        return "manual_review"
    if excluded_points_count > 0 and release_ready_points_count <= 0:
        return "weak_support"
    if release_ready_points_count == participating_points_count and fallback_usable_points_count == 0:
        return "strong_support"
    if release_ready_points_count > 0:
        return "release_supported"
    return "score_supported"


def build_co2_point_evidence_provenance_rows(
    *,
    fit_evidence_coverage_payload: Mapping[str, Any],
    release_readiness_payload: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    coverage_summary = dict(fit_evidence_coverage_payload.get("summary") or {})
    best_fit_candidate = (
        _as_text(coverage_summary.get("best_fit_candidate"))
        or _as_text(coverage_summary.get("recommended_release_candidate"))
        or _as_text(coverage_summary.get("best_by_score"))
    )
    best_supported_candidate = _as_text(coverage_summary.get("best_supported_candidate"))
    trace_rows = list(fit_evidence_coverage_payload.get("point_traceability") or [])
    trace_by_point_candidate = {
        (_as_text(row.get("point_id")), _as_text(row.get("candidate_name"))): dict(row)
        for row in trace_rows
    }

    points: List[Dict[str, Any]] = []
    for point in release_readiness_payload.get("points") or []:
        point_id = _point_id(point)
        best_fit_trace = trace_by_point_candidate.get((point_id, best_fit_candidate), {})
        best_supported_trace = trace_by_point_candidate.get((point_id, best_supported_candidate), {})
        release_status = _as_text(point.get("release_readiness_status"))
        manual_review_required = _as_bool(point.get("manual_review_required"))
        point_row = {
            "point_id": point_id,
            "point_title": point.get("point_title"),
            "phase_name": _phase_name(point),
            "source_segment_id": _source_segment_id(point),
            "source_segment_label": _source_segment_label(point),
            "sampling_window_id": _sampling_window_id(point),
            "sampling_window_label": _sampling_window_label(point),
            "window_start_hint": _window_start_hint(point),
            "window_end_hint": _window_end_hint(point),
            "best_fit_candidate": best_fit_candidate,
            "best_supported_candidate": best_supported_candidate,
            "fit_participation_status": _as_text(best_fit_trace.get("fit_participation_status")),
            "best_supported_participation_status": _as_text(best_supported_trace.get("fit_participation_status")),
            "temporal_status": _as_text(point.get("co2_temporal_contract_status")),
            "steady_state_status": _as_text(point.get("co2_steady_window_status")),
            "candidate_status": _as_text(point.get("co2_calibration_candidate_status")),
            "sampling_settle_status": _as_text(point.get("co2_sampling_settle_status")),
            "sampling_confidence_bucket": _as_text(point.get("sampling_confidence_bucket"))
            or _as_text(point.get("co2_sampling_window_confidence")),
            "release_readiness_status": release_status,
            "score_path_eligibility": _as_bool(point.get("score_path_eligibility")),
            "manual_review_required": manual_review_required,
            "excluded_from_release_support": release_status != "release_ready",
            "reason_chain": _as_text(point.get("blocking_reason_chain")),
            "provenance_reason_chain": ";".join(
                part
                for part in [
                    f"phase={_phase_name(point)}",
                    f"segment={_source_segment_id(point)}",
                    f"window={_sampling_window_id(point)}",
                    f"best_fit_participation={_as_text(best_fit_trace.get('fit_participation_status')) or 'unknown'}",
                    f"best_supported_participation={_as_text(best_supported_trace.get('fit_participation_status')) or 'unknown'}",
                    _as_text(point.get("blocking_reason_chain")),
                ]
                if part
            ),
        }
        points.append(point_row)
    return points


def build_co2_candidate_segment_support(
    *,
    fit_evidence_coverage_payload: Mapping[str, Any],
    release_readiness_payload: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    point_map = {_point_id(point): dict(point) for point in release_readiness_payload.get("points") or []}
    grouped: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for row in fit_evidence_coverage_payload.get("point_traceability") or []:
        payload = dict(row)
        point = point_map.get(_as_text(payload.get("point_id")), {})
        payload["source_segment_id"] = _source_segment_id(point)
        payload["source_segment_label"] = _source_segment_label(point)
        payload["sampling_window_id"] = _sampling_window_id(point)
        payload["sampling_window_label"] = _sampling_window_label(point)
        grouped.setdefault(
            (_as_text(payload.get("candidate_name")), _as_text(payload.get("source_segment_id"))),
            [],
        ).append(payload)

    output: List[Dict[str, Any]] = []
    for (candidate_name, source_segment_id), items in sorted(grouped.items(), key=lambda item: item[0]):
        participating = [row for row in items if _as_text(row.get("fit_participation_status")).startswith("participating_")]
        participating_points_count = len(participating)
        release_ready_points_count = sum(
            1 for row in participating if _as_text(row.get("release_readiness_status")) == "release_ready"
        )
        score_path_eligible_points_count = sum(
            1 for row in participating if _as_bool(row.get("score_path_eligibility"))
        )
        manual_review_points_count = sum(
            1
            for row in participating
            if _as_text(row.get("release_readiness_status")) == "manual_review"
            or _as_bool(row.get("manual_review_required"))
        )
        excluded_points_count = len(items) - participating_points_count
        fallback_usable_points_count = sum(
            1
            for row in participating
            if _as_text(row.get("sampling_settle_status")) == "fallback_but_usable"
            or _as_text(row.get("measured_value_source")) == "co2_trailing_window_fallback"
        )
        support_status = _support_status(
            participating_points_count=participating_points_count,
            release_ready_points_count=release_ready_points_count,
            manual_review_points_count=manual_review_points_count,
            excluded_points_count=excluded_points_count,
            fallback_usable_points_count=fallback_usable_points_count,
        )
        output.append(
            {
                "candidate_name": candidate_name,
                "source_segment_id": source_segment_id,
                "source_segment_label": _as_text(items[0].get("source_segment_label")),
                "participating_points_count": participating_points_count,
                "release_ready_points_count": release_ready_points_count,
                "score_path_eligible_points_count": score_path_eligible_points_count,
                "manual_review_points_count": manual_review_points_count,
                "excluded_points_count": excluded_points_count,
                "segment_support_status": support_status,
                "segment_reason_chain": ";".join(
                    [
                        f"participating={participating_points_count}",
                        f"release_ready={release_ready_points_count}",
                        f"score_path_eligible={score_path_eligible_points_count}",
                        f"manual_review={manual_review_points_count}",
                        f"excluded={excluded_points_count}",
                        f"top_windows={_top_reason_summary(participating or items, 'sampling_window_id', top_n=3)}",
                        f"top_blocking={_top_reason_summary(participating or items, 'blocking_reason_chain', top_n=4)}",
                    ]
                ),
            }
        )
    return output


def build_co2_segment_quality_summary(
    point_rows: Iterable[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in point_rows:
        payload = dict(row)
        grouped.setdefault(_as_text(payload.get("source_segment_id")), []).append(payload)

    output: List[Dict[str, Any]] = []
    for source_segment_id, items in sorted(grouped.items(), key=lambda item: item[0]):
        release_ready_points_count = sum(
            1 for row in items if _as_text(row.get("release_readiness_status")) == "release_ready"
        )
        fallback_usable_points_count = sum(
            1 for row in items if _as_text(row.get("sampling_settle_status")) == "fallback_but_usable"
        )
        manual_review_points_count = sum(
            1 for row in items if _as_text(row.get("release_readiness_status")) == "manual_review"
        )
        excluded_points_count = sum(
            1 for row in items if _as_text(row.get("release_readiness_status")) == "excluded"
        )
        if manual_review_points_count > 0:
            quality_status = "manual_review"
        elif excluded_points_count == len(items):
            quality_status = "blocked"
        elif release_ready_points_count == len(items) and fallback_usable_points_count == 0:
            quality_status = "strong"
        elif release_ready_points_count > 0 or fallback_usable_points_count > 0:
            quality_status = "usable"
        else:
            quality_status = "weak"
        output.append(
            {
                "source_segment_id": source_segment_id,
                "source_segment_label": _as_text(items[0].get("source_segment_label")),
                "sampling_window_count": len({_as_text(row.get("sampling_window_id")) for row in items}),
                "points_count": len(items),
                "release_ready_points_count": release_ready_points_count,
                "fallback_usable_points_count": fallback_usable_points_count,
                "manual_review_points_count": manual_review_points_count,
                "excluded_points_count": excluded_points_count,
                "segment_quality_status": quality_status,
                "segment_quality_reason_chain": ";".join(
                    [
                        f"quality_status={quality_status}",
                        f"top_windows={_top_reason_summary(items, 'sampling_window_id', top_n=3)}",
                        f"top_reasons={_top_reason_summary(items, 'reason_chain', top_n=4)}",
                    ]
                ),
            }
        )
    return output


def _dominant_support_segment(
    candidate_segment_support: Sequence[Mapping[str, Any]],
    *,
    candidate_name: str,
) -> str:
    candidate_rows = [
        dict(row)
        for row in candidate_segment_support
        if _as_text(row.get("candidate_name")) == candidate_name
        and int(row.get("participating_points_count") or 0) > 0
    ]
    if not candidate_rows:
        return ""
    ranked = sorted(
        candidate_rows,
        key=lambda row: (
            -int(row.get("release_ready_points_count") or 0),
            -int(row.get("participating_points_count") or 0),
            _as_text(row.get("source_segment_id")),
        ),
    )
    return _as_text(ranked[0].get("source_segment_id"))


def _support_dispersion_status(
    candidate_segment_support: Sequence[Mapping[str, Any]],
    *,
    candidate_name: str,
) -> str:
    candidate_rows = [
        dict(row)
        for row in candidate_segment_support
        if _as_text(row.get("candidate_name")) == candidate_name
        and int(row.get("participating_points_count") or 0) > 0
    ]
    if not candidate_rows:
        return "unsupported"
    total = sum(int(row.get("participating_points_count") or 0) for row in candidate_rows)
    dominant = max(int(row.get("participating_points_count") or 0) for row in candidate_rows)
    if len(candidate_rows) == 1:
        return "single_segment_only"
    if total > 0 and dominant / total >= 0.8:
        return "single_segment_dominant"
    return "distributed"


def build_co2_point_evidence_provenance_bundle(
    rows: Optional[Iterable[Mapping[str, Any]]] = None,
    *,
    fit_evidence_coverage_payload: Optional[Mapping[str, Any]] = None,
    release_readiness_payload: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    source_rows = [dict(row) for row in rows] if rows is not None else []
    if release_readiness_payload is None:
        release_readiness_payload = build_co2_release_readiness_bundle(source_rows)
    if fit_evidence_coverage_payload is None:
        fit_evidence_coverage_payload = build_co2_fit_evidence_coverage_bundle(
            source_rows,
            release_readiness_payload=release_readiness_payload,
        )

    point_rows = build_co2_point_evidence_provenance_rows(
        fit_evidence_coverage_payload=fit_evidence_coverage_payload,
        release_readiness_payload=release_readiness_payload,
    )
    candidate_segment_support = build_co2_candidate_segment_support(
        fit_evidence_coverage_payload=fit_evidence_coverage_payload,
        release_readiness_payload=release_readiness_payload,
    )
    segment_quality_summary = build_co2_segment_quality_summary(point_rows)
    coverage_summary = dict(fit_evidence_coverage_payload.get("summary") or {})
    dominant_support_segment = _dominant_support_segment(
        candidate_segment_support,
        candidate_name=_as_text(coverage_summary.get("best_supported_candidate")),
    )
    support_dispersion_status = _support_dispersion_status(
        candidate_segment_support,
        candidate_name=_as_text(coverage_summary.get("best_supported_candidate")),
    )

    summary = {
        "best_fit_candidate": _as_text(coverage_summary.get("best_fit_candidate")),
        "best_supported_candidate": _as_text(coverage_summary.get("best_supported_candidate")),
        "recommended_release_candidate": _as_text(coverage_summary.get("recommended_release_candidate")),
        "best_supported_release_candidate": _as_text(coverage_summary.get("best_supported_release_candidate")),
        "dominant_support_segment": dominant_support_segment,
        "support_dispersion_status": support_dispersion_status,
        "manual_review_required": _as_bool(coverage_summary.get("manual_review_required")),
        "summary_reason_chain": ";".join(
            part
            for part in [
                f"best_fit_candidate={_as_text(coverage_summary.get('best_fit_candidate')) or 'none'}",
                f"best_supported_candidate={_as_text(coverage_summary.get('best_supported_candidate')) or 'none'}",
                f"dominant_support_segment={dominant_support_segment or 'none'}",
                f"support_dispersion_status={support_dispersion_status}",
                f"coverage_summary_verdict={_as_text(coverage_summary.get('coverage_summary_verdict')) or 'unknown'}",
            ]
            if part
        ),
        "point_count_total": len(point_rows),
        "segment_count_total": len(segment_quality_summary),
        "candidate_segment_row_count": len(candidate_segment_support),
        "evidence_source": "replay_or_exported_v1_fit_evidence_coverage_and_release_readiness_sidecars",
        "not_real_acceptance_evidence": True,
    }

    return {
        "summary": summary,
        "points": point_rows,
        "candidate_segment_support": candidate_segment_support,
        "segment_quality_summary": segment_quality_summary,
        "fit_evidence_coverage_summary": coverage_summary,
        "release_readiness_summary": dict(release_readiness_payload.get("summary") or {}),
    }


def render_co2_point_evidence_provenance_report(payload: Mapping[str, Any]) -> str:
    summary = dict(payload.get("summary") or {})
    segments = list(payload.get("segment_quality_summary") or [])
    candidate_rows = list(payload.get("candidate_segment_support") or [])
    lines = [
        "# V1 CO2 point evidence provenance / source-segment lineage bundle",
        "",
        "> replay evidence only",
        "> not real acceptance evidence",
        "",
        "## Overview",
        f"- best_fit_candidate: {summary.get('best_fit_candidate') or 'none'}",
        f"- best_supported_candidate: {summary.get('best_supported_candidate') or 'none'}",
        f"- recommended_release_candidate: {summary.get('recommended_release_candidate') or 'none'}",
        f"- best_supported_release_candidate: {summary.get('best_supported_release_candidate') or 'none'}",
        f"- dominant_support_segment: {summary.get('dominant_support_segment') or 'none'}",
        f"- support_dispersion_status: {summary.get('support_dispersion_status') or 'unknown'}",
        f"- manual_review_required: {summary.get('manual_review_required')}",
        f"- summary_reason_chain: {summary.get('summary_reason_chain') or ''}",
        "",
        "## Candidate × Segment Support",
    ]
    for row in candidate_rows[:12]:
        lines.append(
            "- "
            + f"{row.get('candidate_name')} / {row.get('source_segment_id')}: "
            + f"participating={row.get('participating_points_count')} "
            + f"release_ready={row.get('release_ready_points_count')} "
            + f"score_path={row.get('score_path_eligible_points_count')} "
            + f"manual_review={row.get('manual_review_points_count')} "
            + f"excluded={row.get('excluded_points_count')} "
            + f"status={row.get('segment_support_status')}"
        )
    lines.extend(
        [
            "",
            "## Segment Quality",
        ]
    )
    for row in segments[:10]:
        lines.append(
            "- "
            + f"{row.get('source_segment_id')}: "
            + f"points={row.get('points_count')} "
            + f"release_ready={row.get('release_ready_points_count')} "
            + f"fallback={row.get('fallback_usable_points_count')} "
            + f"manual_review={row.get('manual_review_points_count')} "
            + f"excluded={row.get('excluded_points_count')} "
            + f"quality={row.get('segment_quality_status')}"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "- This provenance bundle explains where point evidence came from and how it supports the current fit candidates. It does not add another verdict layer.",
            "- `support_dispersion_status` is advisory-only. It explains whether support is distributed or concentrated, but it does not override existing fit or release recommendations.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_co2_point_evidence_provenance_artifacts(output_dir: Path, payload: Mapping[str, Any]) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    points_path = output_dir / "point_evidence_provenance.csv"
    candidate_segment_path = output_dir / "candidate_segment_support.csv"
    segment_quality_path = output_dir / "segment_quality_summary.csv"
    summary_csv_path = output_dir / "point_evidence_provenance_summary.csv"
    summary_json_path = output_dir / "point_evidence_provenance_summary.json"
    report_path = output_dir / "point_evidence_provenance_report.md"

    write_csv_rows(points_path, payload.get("points") or [])
    write_csv_rows(candidate_segment_path, payload.get("candidate_segment_support") or [])
    write_csv_rows(segment_quality_path, payload.get("segment_quality_summary") or [])
    write_csv_rows(summary_csv_path, [payload.get("summary") or {}])
    with summary_json_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    report_path.write_text(render_co2_point_evidence_provenance_report(payload), encoding="utf-8")
    return {
        "points_csv": points_path,
        "candidate_segment_csv": candidate_segment_path,
        "segment_quality_csv": segment_quality_path,
        "summary_csv": summary_csv_path,
        "summary_json": summary_json_path,
        "report_md": report_path,
    }
