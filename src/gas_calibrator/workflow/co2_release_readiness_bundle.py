"""V1 CO2 release readiness evidence merge bundle helpers.

This sidecar stays advisory-only. It merges the existing fit-side evidence
(candidate pack / weighted fit / stability / bootstrap / arbitration) with the
existing sampling-side evidence (sampling / settle readiness) into a single,
deterministic release review contract.
"""

from __future__ import annotations

from collections import Counter
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .co2_calibration_candidate_pack import _as_bool, _as_text, write_csv_rows
from .co2_fit_arbitration_bundle import build_co2_fit_arbitration_bundle
from .co2_sampling_settle_evidence import build_co2_sampling_settle_evidence


def _fit_participation_status(row: Mapping[str, Any]) -> str:
    if _as_bool(row.get("co2_calibration_candidate_hard_blocked")) or not _as_bool(
        row.get("co2_calibration_candidate_recommended")
    ):
        return "excluded"
    if _as_text(row.get("co2_calibration_candidate_status")) == "fit":
        return "fit_candidate"
    return "advisory_candidate"


def _release_point(row: Mapping[str, Any]) -> Dict[str, Any]:
    point = dict(row)
    sampling_status = _as_text(point.get("co2_sampling_settle_status"))
    confidence_bucket = _as_text(point.get("co2_sampling_window_confidence"))
    fit_participation_status = _fit_participation_status(point)
    hard_blocked = _as_bool(point.get("co2_calibration_candidate_hard_blocked"))
    candidate_recommended = _as_bool(point.get("co2_calibration_candidate_recommended"))
    sampling_manual_review = _as_bool(point.get("co2_sampling_manual_review_required"))
    sampling_score_eligible = _as_bool(point.get("co2_sampling_recommended_for_score_path"))

    score_path_eligibility = (
        candidate_recommended
        and not hard_blocked
        and sampling_score_eligible
        and sampling_status in {"ready", "fallback_but_usable", "manual_review"}
    )

    blocking_reasons: List[str] = [
        f"fit_participation={fit_participation_status}",
        f"sampling_status={sampling_status or 'unknown'}",
        f"sampling_confidence={confidence_bucket or 'unknown'}",
    ]
    for token in (
        _as_text(point.get("co2_source_switch_reason")),
        _as_text(point.get("co2_temporal_contract_reason")),
        _as_text(point.get("co2_sampling_settle_reason_chain")),
        _as_text(point.get("co2_calibration_reason_chain")),
    ):
        if token:
            blocking_reasons.append(token)

    if hard_blocked or sampling_status == "unfit" or fit_participation_status == "excluded":
        release_status = "excluded"
        manual_review_required = False
        if hard_blocked:
            blocking_reasons.append("hard_blocked")
        if sampling_status == "unfit":
            blocking_reasons.append("sampling_unfit")
        if fit_participation_status == "excluded" and not candidate_recommended:
            blocking_reasons.append("not_candidate_recommended")
    elif (
        fit_participation_status == "fit_candidate"
        and sampling_status == "ready"
        and confidence_bucket == "high"
        and not sampling_manual_review
    ):
        release_status = "release_ready"
        manual_review_required = False
        blocking_reasons.append("ready_for_release_review")
    elif sampling_status == "manual_review" or sampling_manual_review or confidence_bucket in {"low", "none"}:
        release_status = "manual_review"
        manual_review_required = True
        blocking_reasons.append("sampling_requires_manual_review")
    elif score_path_eligibility:
        release_status = "score_path_only"
        manual_review_required = False
        blocking_reasons.append("score_path_only_not_release_ready")
    else:
        release_status = "excluded"
        manual_review_required = False
        blocking_reasons.append("not_score_path_eligible")

    point.update(
        {
            "fit_participation_status": fit_participation_status,
            "release_readiness_status": release_status,
            "score_path_eligibility": score_path_eligibility,
            "manual_review_required": manual_review_required,
            "sampling_confidence_bucket": confidence_bucket,
            "sampling_confidence_score": point.get("co2_sampling_confidence_score"),
            "blocking_reason_chain": ";".join(token for token in blocking_reasons if token),
        }
    )
    return point


def build_co2_release_readiness_points(
    sampling_settle_payload: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    points = list(sampling_settle_payload.get("points") or [])
    return [_release_point(row) for row in points]


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


def build_co2_release_readiness_bundle(
    rows: Optional[Iterable[Mapping[str, Any]]] = None,
    *,
    fit_arbitration_payload: Optional[Mapping[str, Any]] = None,
    sampling_settle_payload: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    source_rows = [dict(row) for row in rows] if rows is not None else []
    if fit_arbitration_payload is None:
        fit_arbitration_payload = build_co2_fit_arbitration_bundle(source_rows)
    if sampling_settle_payload is None:
        sampling_settle_payload = build_co2_sampling_settle_evidence(source_rows)

    points = build_co2_release_readiness_points(sampling_settle_payload)
    release_counter = Counter(_as_text(row.get("release_readiness_status")) for row in points)

    fit_summary = dict(fit_arbitration_payload.get("summary") or {})
    best_fit_candidate = (
        _as_text(fit_summary.get("recommended_release_candidate"))
        or _as_text(fit_summary.get("best_balanced_choice"))
        or _as_text(fit_summary.get("best_by_score"))
    )
    fit_manual_review_required = _as_bool(fit_summary.get("manual_review_required"))
    release_ready_points_count = release_counter.get("release_ready", 0)
    score_path_eligible_points_count = sum(1 for row in points if _as_bool(row.get("score_path_eligibility")))
    manual_review_points_count = release_counter.get("manual_review", 0)
    excluded_points_count = release_counter.get("excluded", 0)
    score_path_only_points_count = release_counter.get("score_path_only", 0)

    summary_reasons: List[str] = []
    if best_fit_candidate:
        summary_reasons.append(f"best_fit_candidate={best_fit_candidate}")
    if fit_manual_review_required:
        summary_reasons.append("fit_arbitration_requires_manual_review")
    if manual_review_points_count > 0:
        summary_reasons.append(f"sampling_manual_review_points={manual_review_points_count}")
    if release_ready_points_count <= 0 and score_path_eligible_points_count > 0:
        summary_reasons.append("score_path_only_without_release_ready_points")
    if excluded_points_count > 0:
        summary_reasons.append(f"excluded_points={excluded_points_count}")
    top_blocking = _top_reason_summary(points, "blocking_reason_chain", top_n=6)
    if top_blocking:
        summary_reasons.append(f"top_blocking={top_blocking}")

    if not best_fit_candidate or score_path_eligible_points_count <= 0:
        release_readiness_verdict = "not_recommended"
        recommended_release_candidate = ""
        manual_review_required = True
    elif fit_manual_review_required or manual_review_points_count > 0:
        release_readiness_verdict = "manual_review"
        recommended_release_candidate = ""
        manual_review_required = True
    elif release_ready_points_count <= 0:
        release_readiness_verdict = "score_only"
        recommended_release_candidate = ""
        manual_review_required = True
    else:
        release_readiness_verdict = "release_ready"
        recommended_release_candidate = best_fit_candidate
        manual_review_required = False

    summary = {
        "best_fit_candidate": best_fit_candidate,
        "best_by_score": _as_text(fit_summary.get("best_by_score")),
        "best_by_stability": _as_text(fit_summary.get("best_by_stability")),
        "best_balanced_choice": _as_text(fit_summary.get("best_balanced_choice")),
        "recommended_release_candidate": recommended_release_candidate,
        "release_ready_points_count": release_ready_points_count,
        "score_path_only_points_count": score_path_only_points_count,
        "score_path_eligible_points_count": score_path_eligible_points_count,
        "manual_review_points_count": manual_review_points_count,
        "excluded_points_count": excluded_points_count,
        "release_readiness_verdict": release_readiness_verdict,
        "manual_review_required": manual_review_required,
        "summary_reason_chain": ";".join(summary_reasons),
        "evidence_source": "replay_or_exported_v1_fit_arbitration_and_sampling_settle_sidecars",
        "not_real_acceptance_evidence": True,
    }

    return {
        "summary": summary,
        "points": points,
        "fit_arbitration_summary": fit_summary,
        "sampling_settle_summary": dict(sampling_settle_payload.get("summary") or {}),
    }


def render_co2_release_readiness_report(payload: Mapping[str, Any]) -> str:
    summary = dict(payload.get("summary") or {})
    lines = [
        "# V1 CO2 release readiness evidence merge bundle",
        "",
        "> replay evidence only",
        "> not real acceptance evidence",
        "",
        "## 总览",
        f"- best_fit_candidate: {summary.get('best_fit_candidate') or 'none'}",
        f"- best_by_score / stability / balanced: {summary.get('best_by_score') or 'none'} / {summary.get('best_by_stability') or 'none'} / {summary.get('best_balanced_choice') or 'none'}",
        f"- recommended_release_candidate: {summary.get('recommended_release_candidate') or 'none'}",
        f"- release_readiness_verdict: {summary.get('release_readiness_verdict') or 'unknown'}",
        f"- manual_review_required: {summary.get('manual_review_required')}",
        f"- release_ready / score_path_only / manual_review / excluded: "
        f"{summary.get('release_ready_points_count', 0)} / "
        f"{summary.get('score_path_only_points_count', 0)} / "
        f"{summary.get('manual_review_points_count', 0)} / "
        f"{summary.get('excluded_points_count', 0)}",
        f"- summary_reason_chain: {summary.get('summary_reason_chain') or ''}",
        "",
        "## 说明",
        "- 这层 sidecar 只合并现有 fit 证据和 sampling/settle 证据，不改变 live measured_value。",
        "- `release_ready` 和 `score_path_eligible` 是两个不同层级：前者更保守，后者允许 fallback-but-usable 保留在 score-oriented advisory 路径。",
        "- 任何结论都只是 release review / advisory 合并证据，不是 live writeback，也不是 real acceptance evidence。",
        "",
    ]
    return "\n".join(lines) + "\n"


def write_co2_release_readiness_artifacts(output_dir: Path, payload: Mapping[str, Any]) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    points_path = output_dir / "release_readiness_points.csv"
    summary_csv_path = output_dir / "release_readiness_summary.csv"
    summary_json_path = output_dir / "release_readiness_summary.json"
    report_path = output_dir / "release_readiness_report.md"

    write_csv_rows(points_path, payload.get("points") or [])
    write_csv_rows(summary_csv_path, [payload.get("summary") or {}])
    with summary_json_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    report_path.write_text(render_co2_release_readiness_report(payload), encoding="utf-8")
    return {
        "points_csv": points_path,
        "summary_csv": summary_csv_path,
        "summary_json": summary_json_path,
        "report_md": report_path,
    }
