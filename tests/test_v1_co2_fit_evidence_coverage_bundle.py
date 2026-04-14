from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.tools.build_v1_co2_fit_evidence_coverage_bundle import (
    main as build_fit_evidence_main,
)
from gas_calibrator.workflow.co2_fit_evidence_coverage_bundle import (
    build_co2_fit_evidence_coverage_bundle,
)


def _fit_payload(*, recommended_release_candidate: str, manual_review_required: bool) -> dict[str, object]:
    return {
        "summary": {
            "best_by_score": "weighted_fit_advisory",
            "best_by_stability": "baseline_unweighted_fit_only",
            "best_balanced_choice": "weighted_fit_advisory",
            "recommended_release_candidate": recommended_release_candidate,
            "manual_review_required": manual_review_required,
            "not_real_acceptance_evidence": True,
        },
        "variants": [
            {"fit_variant_name": "baseline_unweighted_all_recommended"},
            {"fit_variant_name": "baseline_unweighted_fit_only"},
            {"fit_variant_name": "weighted_fit_advisory"},
        ],
    }


def _release_point(
    *,
    title: str,
    point_no: int,
    candidate_status: str,
    recommended: bool,
    hard_blocked: bool,
    measured_source: str,
    sampling_status: str,
    release_status: str,
    score_path_eligibility: bool,
    manual_review_required: bool,
    blocking_reason_chain: str,
    weight: float = 1.0,
    source_switch_reason: str = "",
) -> dict[str, object]:
    return {
        "point_title": title,
        "point_no": point_no,
        "point_tag": f"row-{point_no}",
        "point_row": str(point_no),
        "co2_calibration_candidate_status": candidate_status,
        "co2_calibration_candidate_recommended": recommended,
        "co2_calibration_candidate_hard_blocked": hard_blocked,
        "co2_calibration_weight_recommended": weight,
        "co2_point_suitability_status": candidate_status,
        "measured_value_source": measured_source,
        "co2_source_selected": "ga02" if source_switch_reason else "primary",
        "co2_source_switch_reason": source_switch_reason,
        "co2_sampling_settle_status": sampling_status,
        "sampling_confidence_bucket": (
            "high"
            if sampling_status == "ready"
            else "medium"
            if sampling_status == "fallback_but_usable"
            else "low"
        ),
        "release_readiness_status": release_status,
        "score_path_eligibility": score_path_eligibility,
        "manual_review_required": manual_review_required,
        "blocking_reason_chain": blocking_reason_chain,
    }


def _release_payload(
    *,
    best_fit_candidate: str,
    points: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "summary": {
            "best_fit_candidate": best_fit_candidate,
            "release_ready_points_count": sum(
                1 for row in points if row["release_readiness_status"] == "release_ready"
            ),
            "score_path_eligible_points_count": sum(
                1 for row in points if bool(row["score_path_eligibility"])
            ),
            "manual_review_points_count": sum(
                1 for row in points if row["release_readiness_status"] == "manual_review"
            ),
            "excluded_points_count": sum(
                1 for row in points if row["release_readiness_status"] == "excluded"
            ),
            "not_real_acceptance_evidence": True,
        },
        "points": points,
        "fit_arbitration_summary": {},
        "sampling_settle_summary": {},
    }


def test_fit_evidence_coverage_tracks_point_to_candidate_support_levels() -> None:
    fit_payload = _fit_payload(
        recommended_release_candidate="weighted_fit_advisory",
        manual_review_required=True,
    )
    points = [
        _release_point(
            title="clean-1",
            point_no=1,
            candidate_status="fit",
            recommended=True,
            hard_blocked=False,
            measured_source="co2_steady_state_window",
            sampling_status="ready",
            release_status="release_ready",
            score_path_eligibility=True,
            manual_review_required=False,
            blocking_reason_chain="ready_for_release_review",
        ),
        _release_point(
            title="clean-2",
            point_no=2,
            candidate_status="fit",
            recommended=True,
            hard_blocked=False,
            measured_source="co2_steady_state_window",
            sampling_status="ready",
            release_status="release_ready",
            score_path_eligibility=True,
            manual_review_required=False,
            blocking_reason_chain="ready_for_release_review",
        ),
        _release_point(
            title="fallback-score-only",
            point_no=3,
            candidate_status="advisory",
            recommended=True,
            hard_blocked=False,
            measured_source="co2_trailing_window_fallback",
            sampling_status="fallback_but_usable",
            release_status="score_path_only",
            score_path_eligibility=True,
            manual_review_required=False,
            blocking_reason_chain="score_path_only_not_release_ready",
            weight=0.35,
            source_switch_reason="source_fallback",
        ),
        _release_point(
            title="manual-review",
            point_no=4,
            candidate_status="advisory",
            recommended=True,
            hard_blocked=False,
            measured_source="co2_trailing_window_fallback",
            sampling_status="manual_review",
            release_status="manual_review",
            score_path_eligibility=True,
            manual_review_required=True,
            blocking_reason_chain="sampling_requires_manual_review;large_gap_detected",
            weight=0.45,
        ),
        _release_point(
            title="excluded-unfit",
            point_no=5,
            candidate_status="unfit",
            recommended=False,
            hard_blocked=True,
            measured_source="co2_no_trusted_source",
            sampling_status="unfit",
            release_status="excluded",
            score_path_eligibility=False,
            manual_review_required=False,
            blocking_reason_chain="hard_blocked;sampling_unfit;timestamp_rollback_detected",
            weight=0.0,
        ),
    ]

    payload = build_co2_fit_evidence_coverage_bundle(
        fit_arbitration_payload=fit_payload,
        release_readiness_payload=_release_payload(
            best_fit_candidate="weighted_fit_advisory",
            points=points,
        ),
    )

    coverage = {row["candidate_name"]: row for row in payload["candidate_coverage"]}
    trace = {
        (row["point_title"], row["candidate_name"]): row for row in payload["point_traceability"]
    }
    summary = payload["summary"]

    fit_only = coverage["baseline_unweighted_fit_only"]
    assert fit_only["participating_points_count"] == 2
    assert fit_only["release_ready_points_count"] == 2
    assert fit_only["coverage_support_status"] == "strong_support"

    weighted = coverage["weighted_fit_advisory"]
    assert weighted["participating_points_count"] == 4
    assert weighted["fallback_usable_points_count"] == 1
    assert weighted["manual_review_points_count"] == 1
    assert weighted["coverage_support_status"] == "manual_review"
    assert weighted["coverage_manual_review_required"] is True

    fallback_trace = trace[("fallback-score-only", "weighted_fit_advisory")]
    assert fallback_trace["fit_participation_status"] == "participating_advisory"
    assert fallback_trace["release_readiness_status"] == "score_path_only"
    assert fallback_trace["score_path_eligibility"] is True

    excluded_trace = trace[("excluded-unfit", "weighted_fit_advisory")]
    assert excluded_trace["fit_participation_status"] == "excluded_hard_blocked"

    assert summary["best_supported_candidate"] == "baseline_unweighted_fit_only"
    assert summary["best_supported_release_candidate"] == ""
    assert summary["coverage_summary_verdict"] == "support_differs_from_fit_recommendation"
    assert summary["manual_review_required"] is True
    assert summary["release_ready_points_count"] == 2
    assert summary["score_path_eligible_points_count"] == 4
    assert summary["excluded_points_count"] == 1


def test_fit_evidence_coverage_can_confirm_supported_release_candidate() -> None:
    fit_payload = _fit_payload(
        recommended_release_candidate="baseline_unweighted_fit_only",
        manual_review_required=False,
    )
    points = [
        _release_point(
            title="clean-a",
            point_no=1,
            candidate_status="fit",
            recommended=True,
            hard_blocked=False,
            measured_source="co2_steady_state_window",
            sampling_status="ready",
            release_status="release_ready",
            score_path_eligibility=True,
            manual_review_required=False,
            blocking_reason_chain="ready_for_release_review",
        ),
        _release_point(
            title="clean-b",
            point_no=2,
            candidate_status="fit",
            recommended=True,
            hard_blocked=False,
            measured_source="co2_steady_state_window",
            sampling_status="ready",
            release_status="release_ready",
            score_path_eligibility=True,
            manual_review_required=False,
            blocking_reason_chain="ready_for_release_review",
        ),
        _release_point(
            title="blocked-c",
            point_no=3,
            candidate_status="unfit",
            recommended=False,
            hard_blocked=True,
            measured_source="co2_no_trusted_source",
            sampling_status="unfit",
            release_status="excluded",
            score_path_eligibility=False,
            manual_review_required=False,
            blocking_reason_chain="hard_blocked;sampling_unfit",
            weight=0.0,
        ),
    ]

    payload = build_co2_fit_evidence_coverage_bundle(
        fit_arbitration_payload=fit_payload,
        release_readiness_payload=_release_payload(
            best_fit_candidate="baseline_unweighted_fit_only",
            points=points,
        ),
    )

    summary = payload["summary"]
    assert summary["best_supported_candidate"] == "baseline_unweighted_fit_only"
    assert summary["best_supported_release_candidate"] == "baseline_unweighted_fit_only"
    assert summary["coverage_summary_verdict"] == "release_supported_candidate_available"
    assert summary["manual_review_required"] is False


def test_fit_evidence_coverage_tool_writes_expected_artifacts(tmp_path: Path) -> None:
    fit_json = tmp_path / "fit_arbitration_summary.json"
    release_json = tmp_path / "release_readiness_summary.json"
    output_dir = tmp_path / "fit_evidence_coverage"

    fit_payload = _fit_payload(
        recommended_release_candidate="weighted_fit_advisory",
        manual_review_required=True,
    )
    release_payload = _release_payload(
        best_fit_candidate="weighted_fit_advisory",
        points=[
            _release_point(
                title="clean",
                point_no=1,
                candidate_status="fit",
                recommended=True,
                hard_blocked=False,
                measured_source="co2_steady_state_window",
                sampling_status="ready",
                release_status="release_ready",
                score_path_eligibility=True,
                manual_review_required=False,
                blocking_reason_chain="ready_for_release_review",
            ),
            _release_point(
                title="manual",
                point_no=2,
                candidate_status="advisory",
                recommended=True,
                hard_blocked=False,
                measured_source="co2_trailing_window_fallback",
                sampling_status="manual_review",
                release_status="manual_review",
                score_path_eligibility=True,
                manual_review_required=True,
                blocking_reason_chain="sampling_requires_manual_review",
                weight=0.4,
            ),
        ],
    )
    fit_json.write_text(json.dumps(fit_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    release_json.write_text(json.dumps(release_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    assert build_fit_evidence_main(
        [
            "--fit-arbitration-summary-json",
            str(fit_json),
            "--release-readiness-summary-json",
            str(release_json),
            "--output-dir",
            str(output_dir),
        ]
    ) == 0

    assert (output_dir / "point_fit_traceability.csv").exists()
    assert (output_dir / "candidate_coverage.csv").exists()
    assert (output_dir / "fit_evidence_coverage_summary.csv").exists()
    assert (output_dir / "fit_evidence_coverage_summary.json").exists()
    assert (output_dir / "fit_evidence_coverage_report.md").exists()

    payload = json.loads((output_dir / "fit_evidence_coverage_summary.json").read_text(encoding="utf-8"))
    assert payload["summary"]["best_supported_candidate"] != ""
    assert payload["summary"]["not_real_acceptance_evidence"] is True
    report_text = (output_dir / "fit_evidence_coverage_report.md").read_text(encoding="utf-8")
    assert "replay evidence only" in report_text
    assert "not real acceptance evidence" in report_text
