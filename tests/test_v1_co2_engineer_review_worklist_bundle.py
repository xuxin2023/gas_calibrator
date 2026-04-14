from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.tools.build_v1_co2_engineer_review_worklist_bundle import (
    main as build_engineer_review_main,
)
from gas_calibrator.workflow.co2_engineer_review_worklist_bundle import (
    build_co2_engineer_review_worklist_bundle,
)
from gas_calibrator.workflow.co2_point_evidence_provenance_bundle import (
    build_co2_point_evidence_provenance_bundle,
)


def _fit_payload(
    *,
    best_by_score: str,
    best_by_stability: str,
    best_balanced_choice: str,
    recommended_release_candidate: str,
    manual_review_required: bool,
) -> dict[str, object]:
    return {
        "summary": {
            "best_by_score": best_by_score,
            "best_by_stability": best_by_stability,
            "best_balanced_choice": best_balanced_choice,
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


def _trace_row(
    *,
    point_id: str,
    candidate_name: str,
    fit_participation_status: str,
    release_readiness_status: str,
    score_path_eligibility: bool,
    manual_review_required: bool,
    blocking_reason_chain: str,
) -> dict[str, object]:
    return {
        "point_id": point_id,
        "point_title": point_id,
        "candidate_name": candidate_name,
        "fit_participation_status": fit_participation_status,
        "release_readiness_status": release_readiness_status,
        "score_path_eligibility": score_path_eligibility,
        "manual_review_required": manual_review_required,
        "blocking_reason_chain": blocking_reason_chain,
    }


def _coverage_payload(
    *,
    summary: dict[str, object],
    candidate_rows: list[dict[str, object]],
    trace_rows: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "summary": summary,
        "candidate_coverage": candidate_rows,
        "point_traceability": trace_rows,
    }


def _release_point(
    *,
    point_id: str,
    source_segment_id: str,
    candidate_status: str,
    sampling_status: str,
    confidence_bucket: str,
    release_status: str,
    score_path_eligibility: bool,
    manual_review_required: bool,
    blocking_reason_chain: str,
    measured_value_source: str = "co2_steady_state_window",
    temporal_status: str = "pass",
    steady_found: bool = True,
    steady_status: str = "pass",
) -> dict[str, object]:
    return {
        "point_title": point_id,
        "point_no": 1,
        "point_tag": point_id,
        "point_row": point_id,
        "route": "gas",
        "pressure_target_label": "ambient",
        "co2_ppm_target": 500.0,
        "temp_chamber_c": 20.0,
        "measured_value_source": measured_value_source,
        "co2_source_selected": "primary" if source_segment_id.startswith("primary") else "ga02",
        "co2_source_segment_selected": source_segment_id,
        "co2_source_switch_reason": "source_fallback" if "ga02" in source_segment_id else "",
        "co2_temporal_contract_status": temporal_status,
        "co2_steady_window_status": steady_status,
        "co2_steady_window_found": steady_found,
        "co2_steady_window_sample_count": 4,
        "co2_calibration_candidate_status": candidate_status,
        "co2_sampling_settle_status": sampling_status,
        "sampling_confidence_bucket": confidence_bucket,
        "release_readiness_status": release_status,
        "score_path_eligibility": score_path_eligibility,
        "manual_review_required": manual_review_required,
        "blocking_reason_chain": blocking_reason_chain,
    }


def _release_payload(
    *,
    best_fit_candidate: str,
    points: list[dict[str, object]],
    recommended_release_candidate: str = "",
    manual_review_required: bool = False,
) -> dict[str, object]:
    return {
        "summary": {
            "best_fit_candidate": best_fit_candidate,
            "recommended_release_candidate": recommended_release_candidate,
            "release_readiness_verdict": "manual_review" if manual_review_required else "release_ready",
            "manual_review_required": manual_review_required,
            "release_ready_points_count": sum(1 for row in points if row["release_readiness_status"] == "release_ready"),
            "score_path_eligible_points_count": sum(1 for row in points if bool(row["score_path_eligibility"])),
            "manual_review_points_count": sum(1 for row in points if row["release_readiness_status"] == "manual_review"),
            "excluded_points_count": sum(1 for row in points if row["release_readiness_status"] == "excluded"),
            "not_real_acceptance_evidence": True,
        },
        "points": points,
    }


def test_engineer_review_worklist_marks_aligned_clean_candidate_as_low_priority() -> None:
    fit_payload = _fit_payload(
        best_by_score="weighted_fit_advisory",
        best_by_stability="weighted_fit_advisory",
        best_balanced_choice="weighted_fit_advisory",
        recommended_release_candidate="weighted_fit_advisory",
        manual_review_required=False,
    )
    coverage_payload = _coverage_payload(
        summary={
            "best_fit_candidate": "weighted_fit_advisory",
            "best_by_score": "weighted_fit_advisory",
            "best_by_stability": "weighted_fit_advisory",
            "best_balanced_choice": "weighted_fit_advisory",
            "recommended_release_candidate": "weighted_fit_advisory",
            "best_supported_candidate": "weighted_fit_advisory",
            "best_supported_release_candidate": "weighted_fit_advisory",
            "release_ready_points_count": 2,
            "manual_review_points_count": 0,
            "excluded_points_count": 0,
            "coverage_summary_verdict": "release_supported_candidate_available",
            "manual_review_required": False,
            "not_real_acceptance_evidence": True,
        },
        candidate_rows=[
            {
                "candidate_name": "weighted_fit_advisory",
                "participating_points_count": 2,
                "release_ready_points_count": 2,
                "score_path_eligible_points_count": 2,
                "fallback_usable_points_count": 0,
                "manual_review_points_count": 0,
                "excluded_points_count": 0,
                "coverage_support_status": "strong_support",
                "coverage_manual_review_required": False,
                "coverage_reason_chain": "participating=2;release_ready=2;support_status=strong_support",
            }
        ],
        trace_rows=[
            _trace_row(
                point_id="clean-a",
                candidate_name="weighted_fit_advisory",
                fit_participation_status="participating_fit",
                release_readiness_status="release_ready",
                score_path_eligibility=True,
                manual_review_required=False,
                blocking_reason_chain="ready_for_release_review",
            ),
            _trace_row(
                point_id="clean-b",
                candidate_name="weighted_fit_advisory",
                fit_participation_status="participating_fit",
                release_readiness_status="release_ready",
                score_path_eligibility=True,
                manual_review_required=False,
                blocking_reason_chain="ready_for_release_review",
            ),
        ],
    )
    release_payload = _release_payload(
        best_fit_candidate="weighted_fit_advisory",
        recommended_release_candidate="weighted_fit_advisory",
        points=[
            _release_point(
                point_id="clean-a",
                source_segment_id="primary#1",
                candidate_status="fit",
                sampling_status="ready",
                confidence_bucket="high",
                release_status="release_ready",
                score_path_eligibility=True,
                manual_review_required=False,
                blocking_reason_chain="ready_for_release_review",
            ),
            _release_point(
                point_id="clean-b",
                source_segment_id="primary#2",
                candidate_status="fit",
                sampling_status="ready",
                confidence_bucket="high",
                release_status="release_ready",
                score_path_eligibility=True,
                manual_review_required=False,
                blocking_reason_chain="ready_for_release_review",
            ),
        ],
    )
    provenance_payload = build_co2_point_evidence_provenance_bundle(
        fit_evidence_coverage_payload=coverage_payload,
        release_readiness_payload=release_payload,
    )

    payload = build_co2_engineer_review_worklist_bundle(
        fit_arbitration_payload=fit_payload,
        fit_evidence_coverage_payload=coverage_payload,
        release_readiness_payload=release_payload,
        provenance_payload=provenance_payload,
    )

    summary = payload["summary"]
    candidate = payload["candidate_review_worklist"][0]
    assert candidate["candidate_name"] == "weighted_fit_advisory"
    assert candidate["review_priority"] == "low"
    assert summary["decision_packet_verdict"] == "aligned_low_risk"
    assert summary["manual_review_required"] is False


def test_engineer_review_worklist_escalates_conflict_and_generates_review_items() -> None:
    fit_payload = _fit_payload(
        best_by_score="weighted_fit_advisory",
        best_by_stability="baseline_unweighted_fit_only",
        best_balanced_choice="baseline_unweighted_fit_only",
        recommended_release_candidate="weighted_fit_advisory",
        manual_review_required=True,
    )
    coverage_payload = _coverage_payload(
        summary={
            "best_fit_candidate": "weighted_fit_advisory",
            "best_by_score": "weighted_fit_advisory",
            "best_by_stability": "baseline_unweighted_fit_only",
            "best_balanced_choice": "baseline_unweighted_fit_only",
            "recommended_release_candidate": "weighted_fit_advisory",
            "best_supported_candidate": "baseline_unweighted_fit_only",
            "best_supported_release_candidate": "",
            "release_ready_points_count": 1,
            "manual_review_points_count": 1,
            "excluded_points_count": 1,
            "coverage_summary_verdict": "support_differs_from_fit_recommendation",
            "manual_review_required": True,
            "summary_reason_chain": "fit_recommendation_differs_from_best_supported",
            "not_real_acceptance_evidence": True,
        },
        candidate_rows=[
            {
                "candidate_name": "weighted_fit_advisory",
                "participating_points_count": 3,
                "release_ready_points_count": 1,
                "score_path_eligible_points_count": 3,
                "fallback_usable_points_count": 1,
                "manual_review_points_count": 1,
                "excluded_points_count": 1,
                "coverage_support_status": "manual_review",
                "coverage_manual_review_required": True,
                "coverage_reason_chain": "participating=3;release_ready=1;fallback_usable=1;manual_review=1;excluded=1;support_status=manual_review",
            },
            {
                "candidate_name": "baseline_unweighted_fit_only",
                "participating_points_count": 1,
                "release_ready_points_count": 1,
                "score_path_eligible_points_count": 1,
                "fallback_usable_points_count": 0,
                "manual_review_points_count": 0,
                "excluded_points_count": 0,
                "coverage_support_status": "strong_support",
                "coverage_manual_review_required": False,
                "coverage_reason_chain": "participating=1;release_ready=1;support_status=strong_support",
            },
        ],
        trace_rows=[
            _trace_row(
                point_id="clean-1",
                candidate_name="weighted_fit_advisory",
                fit_participation_status="participating_fit",
                release_readiness_status="release_ready",
                score_path_eligibility=True,
                manual_review_required=False,
                blocking_reason_chain="ready_for_release_review",
            ),
            _trace_row(
                point_id="clean-1",
                candidate_name="baseline_unweighted_fit_only",
                fit_participation_status="participating_fit",
                release_readiness_status="release_ready",
                score_path_eligibility=True,
                manual_review_required=False,
                blocking_reason_chain="ready_for_release_review",
            ),
            _trace_row(
                point_id="fallback-point",
                candidate_name="weighted_fit_advisory",
                fit_participation_status="participating_advisory",
                release_readiness_status="score_path_only",
                score_path_eligibility=True,
                manual_review_required=False,
                blocking_reason_chain="score_path_only_not_release_ready",
            ),
            _trace_row(
                point_id="manual-point",
                candidate_name="weighted_fit_advisory",
                fit_participation_status="participating_advisory",
                release_readiness_status="manual_review",
                score_path_eligibility=True,
                manual_review_required=True,
                blocking_reason_chain="sampling_requires_manual_review;large_gap_detected",
            ),
            _trace_row(
                point_id="blocked-point",
                candidate_name="weighted_fit_advisory",
                fit_participation_status="excluded_hard_blocked",
                release_readiness_status="excluded",
                score_path_eligibility=False,
                manual_review_required=False,
                blocking_reason_chain="hard_blocked;sampling_unfit;timestamp_rollback_detected",
            ),
        ],
    )
    release_payload = _release_payload(
        best_fit_candidate="weighted_fit_advisory",
        manual_review_required=True,
        points=[
            _release_point(
                point_id="clean-1",
                source_segment_id="primary#1",
                candidate_status="fit",
                sampling_status="ready",
                confidence_bucket="high",
                release_status="release_ready",
                score_path_eligibility=True,
                manual_review_required=False,
                blocking_reason_chain="ready_for_release_review",
            ),
            _release_point(
                point_id="fallback-point",
                source_segment_id="ga02#1",
                candidate_status="advisory",
                sampling_status="fallback_but_usable",
                confidence_bucket="medium",
                release_status="score_path_only",
                score_path_eligibility=True,
                manual_review_required=False,
                blocking_reason_chain="score_path_only_not_release_ready",
                measured_value_source="co2_trailing_window_fallback",
                steady_found=False,
                steady_status="warn",
            ),
            _release_point(
                point_id="manual-point",
                source_segment_id="primary#1",
                candidate_status="advisory",
                sampling_status="manual_review",
                confidence_bucket="low",
                release_status="manual_review",
                score_path_eligibility=True,
                manual_review_required=True,
                blocking_reason_chain="sampling_requires_manual_review;large_gap_detected",
                measured_value_source="co2_trailing_window_fallback",
                temporal_status="warn",
                steady_found=False,
                steady_status="warn",
            ),
            _release_point(
                point_id="blocked-point",
                source_segment_id="segment_unknown",
                candidate_status="unfit",
                sampling_status="unfit",
                confidence_bucket="none",
                release_status="excluded",
                score_path_eligibility=False,
                manual_review_required=False,
                blocking_reason_chain="hard_blocked;sampling_unfit;timestamp_rollback_detected",
                measured_value_source="co2_no_trusted_source",
                temporal_status="fail",
                steady_found=False,
                steady_status="warn",
            ),
        ],
    )
    provenance_payload = build_co2_point_evidence_provenance_bundle(
        fit_evidence_coverage_payload=coverage_payload,
        release_readiness_payload=release_payload,
    )

    payload = build_co2_engineer_review_worklist_bundle(
        fit_arbitration_payload=fit_payload,
        fit_evidence_coverage_payload=coverage_payload,
        release_readiness_payload=release_payload,
        provenance_payload=provenance_payload,
    )

    summary = payload["summary"]
    candidates = {row["candidate_name"]: row for row in payload["candidate_review_worklist"]}
    items = payload["review_items"]

    assert candidates["weighted_fit_advisory"]["review_priority"] == "high"
    assert candidates["baseline_unweighted_fit_only"]["review_priority"] == "high"
    assert summary["decision_packet_verdict"] == "manual_review"
    assert summary["manual_review_required"] is True
    assert summary["recommended_release_candidate"] == "weighted_fit_advisory"
    assert summary["best_supported_candidate"] == "baseline_unweighted_fit_only"
    assert summary["top_review_priority"] == "high"

    issue_codes = {row["issue_code"] for row in items}
    assert "fit_support_conflict" in issue_codes
    assert "excluded_point" in issue_codes
    assert "temporal_or_sampling_manual_review" in issue_codes
    assert "single_segment_support" in issue_codes


def test_engineer_review_worklist_tool_writes_expected_artifacts(tmp_path: Path) -> None:
    fit_payload = _fit_payload(
        best_by_score="weighted_fit_advisory",
        best_by_stability="baseline_unweighted_fit_only",
        best_balanced_choice="baseline_unweighted_fit_only",
        recommended_release_candidate="weighted_fit_advisory",
        manual_review_required=True,
    )
    coverage_payload = _coverage_payload(
        summary={
            "best_fit_candidate": "weighted_fit_advisory",
            "best_by_score": "weighted_fit_advisory",
            "best_by_stability": "baseline_unweighted_fit_only",
            "best_balanced_choice": "baseline_unweighted_fit_only",
            "recommended_release_candidate": "weighted_fit_advisory",
            "best_supported_candidate": "baseline_unweighted_fit_only",
            "best_supported_release_candidate": "",
            "release_ready_points_count": 1,
            "manual_review_points_count": 1,
            "excluded_points_count": 1,
            "coverage_summary_verdict": "support_differs_from_fit_recommendation",
            "manual_review_required": True,
            "not_real_acceptance_evidence": True,
        },
        candidate_rows=[
            {
                "candidate_name": "weighted_fit_advisory",
                "participating_points_count": 2,
                "release_ready_points_count": 1,
                "score_path_eligible_points_count": 2,
                "fallback_usable_points_count": 1,
                "manual_review_points_count": 1,
                "excluded_points_count": 0,
                "coverage_support_status": "manual_review",
                "coverage_manual_review_required": True,
                "coverage_reason_chain": "participating=2;release_ready=1;support_status=manual_review",
            }
        ],
        trace_rows=[
            _trace_row(
                point_id="clean-1",
                candidate_name="weighted_fit_advisory",
                fit_participation_status="participating_fit",
                release_readiness_status="release_ready",
                score_path_eligibility=True,
                manual_review_required=False,
                blocking_reason_chain="ready_for_release_review",
            ),
            _trace_row(
                point_id="manual-point",
                candidate_name="weighted_fit_advisory",
                fit_participation_status="participating_advisory",
                release_readiness_status="manual_review",
                score_path_eligibility=True,
                manual_review_required=True,
                blocking_reason_chain="sampling_requires_manual_review;large_gap_detected",
            ),
        ],
    )
    release_payload = _release_payload(
        best_fit_candidate="weighted_fit_advisory",
        manual_review_required=True,
        points=[
            _release_point(
                point_id="clean-1",
                source_segment_id="primary#1",
                candidate_status="fit",
                sampling_status="ready",
                confidence_bucket="high",
                release_status="release_ready",
                score_path_eligibility=True,
                manual_review_required=False,
                blocking_reason_chain="ready_for_release_review",
            ),
            _release_point(
                point_id="manual-point",
                source_segment_id="primary#1",
                candidate_status="advisory",
                sampling_status="manual_review",
                confidence_bucket="low",
                release_status="manual_review",
                score_path_eligibility=True,
                manual_review_required=True,
                blocking_reason_chain="sampling_requires_manual_review;large_gap_detected",
                measured_value_source="co2_trailing_window_fallback",
                temporal_status="warn",
                steady_found=False,
                steady_status="warn",
            ),
        ],
    )
    provenance_payload = build_co2_point_evidence_provenance_bundle(
        fit_evidence_coverage_payload=coverage_payload,
        release_readiness_payload=release_payload,
    )

    fit_json = tmp_path / "fit_arbitration_summary.json"
    coverage_json = tmp_path / "fit_evidence_coverage_summary.json"
    release_json = tmp_path / "release_readiness_summary.json"
    provenance_json = tmp_path / "point_evidence_provenance_summary.json"
    output_dir = tmp_path / "engineer_review_worklist"
    fit_json.write_text(json.dumps(fit_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    coverage_json.write_text(json.dumps(coverage_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    release_json.write_text(json.dumps(release_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    provenance_json.write_text(json.dumps(provenance_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    assert build_engineer_review_main(
        [
            "--fit-arbitration-summary-json",
            str(fit_json),
            "--fit-evidence-coverage-summary-json",
            str(coverage_json),
            "--release-readiness-summary-json",
            str(release_json),
            "--point-evidence-provenance-summary-json",
            str(provenance_json),
            "--output-dir",
            str(output_dir),
        ]
    ) == 0

    assert (output_dir / "candidate_review_worklist.csv").exists()
    assert (output_dir / "review_items.csv").exists()
    assert (output_dir / "decision_packet_summary.csv").exists()
    assert (output_dir / "decision_packet_summary.json").exists()
    assert (output_dir / "decision_packet_report.md").exists()

    payload = json.loads((output_dir / "decision_packet_summary.json").read_text(encoding="utf-8"))
    assert payload["summary"]["not_real_acceptance_evidence"] is True
    report_text = (output_dir / "decision_packet_report.md").read_text(encoding="utf-8")
    assert "replay evidence only" in report_text
    assert "not real acceptance evidence" in report_text
