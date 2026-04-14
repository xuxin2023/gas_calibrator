from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.tools.build_v1_co2_point_evidence_provenance_bundle import (
    main as build_point_provenance_main,
)
from gas_calibrator.workflow.co2_point_evidence_provenance_bundle import (
    build_co2_point_evidence_provenance_bundle,
)


def _coverage_summary() -> dict[str, object]:
    return {
        "best_fit_candidate": "weighted_fit_advisory",
        "best_by_score": "weighted_fit_advisory",
        "best_by_stability": "baseline_unweighted_fit_only",
        "best_balanced_choice": "baseline_unweighted_fit_only",
        "recommended_release_candidate": "weighted_fit_advisory",
        "best_supported_candidate": "baseline_unweighted_fit_only",
        "best_supported_release_candidate": "",
        "coverage_summary_verdict": "support_differs_from_fit_recommendation",
        "manual_review_required": True,
        "not_real_acceptance_evidence": True,
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
    measured_value_source: str = "co2_steady_state_window",
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
        "measured_value_source": measured_value_source,
    }


def _release_point(
    *,
    point_id: str,
    point_no: int,
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
    steady_status: str = "pass",
    steady_found: bool = True,
    source_selected: str = "primary",
    source_switch_reason: str = "",
) -> dict[str, object]:
    return {
        "point_title": point_id,
        "point_no": point_no,
        "point_tag": point_id,
        "point_row": str(point_no),
        "route": "gas",
        "pressure_target_label": "ambient",
        "co2_ppm_target": 500.0,
        "temp_chamber_c": 20.0,
        "measured_value_source": measured_value_source,
        "co2_source_selected": source_selected,
        "co2_source_segment_selected": source_segment_id,
        "co2_source_switch_reason": source_switch_reason,
        "co2_temporal_contract_status": temporal_status,
        "co2_steady_window_status": steady_status,
        "co2_steady_window_found": steady_found,
        "co2_steady_window_sample_count": 4,
        "co2_calibration_candidate_status": candidate_status,
        "co2_calibration_reason_chain": (
            "waterfall=pass;steady_state_window"
            if candidate_status == "fit"
            else "waterfall=warn;source_fallback"
            if candidate_status == "advisory"
            else "waterfall=fail;no_trusted_source"
        ),
        "co2_sampling_settle_status": sampling_status,
        "sampling_confidence_bucket": confidence_bucket,
        "release_readiness_status": release_status,
        "score_path_eligibility": score_path_eligibility,
        "manual_review_required": manual_review_required,
        "blocking_reason_chain": blocking_reason_chain,
    }


def _payloads() -> tuple[dict[str, object], dict[str, object]]:
    coverage_payload = {
        "summary": _coverage_summary(),
        "point_traceability": [
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
                point_id="fallback-usable",
                candidate_name="weighted_fit_advisory",
                fit_participation_status="participating_advisory",
                release_readiness_status="score_path_only",
                score_path_eligibility=True,
                manual_review_required=False,
                blocking_reason_chain="score_path_only_not_release_ready",
                measured_value_source="co2_trailing_window_fallback",
            ),
            _trace_row(
                point_id="fallback-usable",
                candidate_name="baseline_unweighted_fit_only",
                fit_participation_status="excluded_not_fit_for_variant",
                release_readiness_status="score_path_only",
                score_path_eligibility=True,
                manual_review_required=False,
                blocking_reason_chain="score_path_only_not_release_ready",
                measured_value_source="co2_trailing_window_fallback",
            ),
            _trace_row(
                point_id="manual-review",
                candidate_name="weighted_fit_advisory",
                fit_participation_status="participating_advisory",
                release_readiness_status="manual_review",
                score_path_eligibility=True,
                manual_review_required=True,
                blocking_reason_chain="sampling_requires_manual_review;large_gap_detected",
                measured_value_source="co2_trailing_window_fallback",
            ),
            _trace_row(
                point_id="excluded-unfit",
                candidate_name="weighted_fit_advisory",
                fit_participation_status="excluded_hard_blocked",
                release_readiness_status="excluded",
                score_path_eligibility=False,
                manual_review_required=False,
                blocking_reason_chain="hard_blocked;sampling_unfit;timestamp_rollback_detected",
                measured_value_source="co2_no_trusted_source",
            ),
        ],
    }
    release_payload = {
        "summary": {
            "best_fit_candidate": "weighted_fit_advisory",
            "recommended_release_candidate": "",
            "release_readiness_verdict": "manual_review",
            "manual_review_required": True,
            "not_real_acceptance_evidence": True,
        },
        "points": [
            _release_point(
                point_id="clean-1",
                point_no=1,
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
                point_id="fallback-usable",
                point_no=2,
                source_segment_id="ga02#1",
                candidate_status="advisory",
                sampling_status="fallback_but_usable",
                confidence_bucket="medium",
                release_status="score_path_only",
                score_path_eligibility=True,
                manual_review_required=False,
                blocking_reason_chain="score_path_only_not_release_ready",
                measured_value_source="co2_trailing_window_fallback",
                steady_status="warn",
                steady_found=False,
                source_selected="ga02",
                source_switch_reason="source_fallback",
            ),
            _release_point(
                point_id="manual-review",
                point_no=3,
                source_segment_id="primary#2",
                candidate_status="advisory",
                sampling_status="manual_review",
                confidence_bucket="low",
                release_status="manual_review",
                score_path_eligibility=True,
                manual_review_required=True,
                blocking_reason_chain="sampling_requires_manual_review;large_gap_detected",
                measured_value_source="co2_trailing_window_fallback",
                temporal_status="warn",
                steady_status="warn",
                steady_found=False,
            ),
            _release_point(
                point_id="excluded-unfit",
                point_no=4,
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
                steady_status="warn",
                steady_found=False,
                source_selected="",
            ),
        ],
    }
    return coverage_payload, release_payload


def test_point_evidence_provenance_tracks_point_origin_and_support_strength() -> None:
    coverage_payload, release_payload = _payloads()

    payload = build_co2_point_evidence_provenance_bundle(
        fit_evidence_coverage_payload=coverage_payload,
        release_readiness_payload=release_payload,
    )

    by_point = {row["point_id"]: row for row in payload["points"]}
    summary = payload["summary"]

    assert by_point["clean-1"]["source_segment_id"] == "primary#1"
    assert by_point["clean-1"]["sampling_window_id"] == "primary#1|steady_window"
    assert by_point["clean-1"]["release_readiness_status"] == "release_ready"
    assert "segment=primary#1" in by_point["clean-1"]["provenance_reason_chain"]

    assert by_point["fallback-usable"]["score_path_eligibility"] is True
    assert by_point["fallback-usable"]["excluded_from_release_support"] is True
    assert by_point["fallback-usable"]["sampling_window_id"] == "ga02#1|fallback_window"

    assert by_point["manual-review"]["release_readiness_status"] == "manual_review"
    assert "large_gap_detected" in by_point["manual-review"]["reason_chain"]
    assert "window=primary#2|manual_review_window" in by_point["manual-review"]["provenance_reason_chain"]

    assert by_point["excluded-unfit"]["fit_participation_status"] == "excluded_hard_blocked"
    assert by_point["excluded-unfit"]["excluded_from_release_support"] is True

    assert summary["best_fit_candidate"] == "weighted_fit_advisory"
    assert summary["best_supported_candidate"] == "baseline_unweighted_fit_only"
    assert summary["best_supported_release_candidate"] == ""
    assert summary["manual_review_required"] is True
    assert summary["not_real_acceptance_evidence"] is True


def test_point_evidence_provenance_reports_segment_concentration_without_overriding_recommendation() -> None:
    coverage_payload, release_payload = _payloads()

    payload = build_co2_point_evidence_provenance_bundle(
        fit_evidence_coverage_payload=coverage_payload,
        release_readiness_payload=release_payload,
    )

    candidate_segment = {
        (row["candidate_name"], row["source_segment_id"]): row
        for row in payload["candidate_segment_support"]
    }
    summary = payload["summary"]

    fit_only_primary = candidate_segment[("baseline_unweighted_fit_only", "primary#1")]
    assert fit_only_primary["participating_points_count"] == 1
    assert fit_only_primary["release_ready_points_count"] == 1
    assert fit_only_primary["segment_support_status"] == "strong_support"

    weighted_primary = candidate_segment[("weighted_fit_advisory", "primary#1")]
    weighted_fallback = candidate_segment[("weighted_fit_advisory", "ga02#1")]
    assert weighted_primary["participating_points_count"] == 1
    assert weighted_fallback["participating_points_count"] == 1

    assert summary["dominant_support_segment"] == "primary#1"
    assert summary["support_dispersion_status"] == "single_segment_only"
    assert summary["recommended_release_candidate"] == "weighted_fit_advisory"
    assert summary["best_supported_candidate"] == "baseline_unweighted_fit_only"


def test_point_evidence_provenance_tool_writes_expected_artifacts(tmp_path: Path) -> None:
    coverage_payload, release_payload = _payloads()
    coverage_json = tmp_path / "fit_evidence_coverage_summary.json"
    release_json = tmp_path / "release_readiness_summary.json"
    output_dir = tmp_path / "point_evidence_provenance"

    coverage_json.write_text(json.dumps(coverage_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    release_json.write_text(json.dumps(release_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    assert build_point_provenance_main(
        [
            "--fit-evidence-coverage-summary-json",
            str(coverage_json),
            "--release-readiness-summary-json",
            str(release_json),
            "--output-dir",
            str(output_dir),
        ]
    ) == 0

    assert (output_dir / "point_evidence_provenance.csv").exists()
    assert (output_dir / "candidate_segment_support.csv").exists()
    assert (output_dir / "segment_quality_summary.csv").exists()
    assert (output_dir / "point_evidence_provenance_summary.csv").exists()
    assert (output_dir / "point_evidence_provenance_summary.json").exists()
    assert (output_dir / "point_evidence_provenance_report.md").exists()

    payload = json.loads((output_dir / "point_evidence_provenance_summary.json").read_text(encoding="utf-8"))
    assert payload["summary"]["point_count_total"] == 4
    assert payload["summary"]["not_real_acceptance_evidence"] is True
    report_text = (output_dir / "point_evidence_provenance_report.md").read_text(encoding="utf-8")
    assert "replay evidence only" in report_text
    assert "not real acceptance evidence" in report_text
