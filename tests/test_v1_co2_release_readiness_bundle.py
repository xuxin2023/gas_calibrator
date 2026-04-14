from __future__ import annotations

import csv
import json
from pathlib import Path

from gas_calibrator.tools.build_v1_co2_release_readiness_bundle import (
    main as build_release_readiness_main,
)
from gas_calibrator.workflow.co2_release_readiness_bundle import (
    build_co2_release_readiness_bundle,
)


def _sampling_point(
    *,
    title: str,
    point_no: int,
    target: float,
    measured: float,
    candidate_status: str,
    recommended: bool,
    hard_blocked: bool,
    weight: float,
    measured_source: str = "co2_steady_state_window",
    sampling_status: str = "ready",
    confidence_bucket: str = "high",
    confidence_score: float = 92.0,
    score_path: bool = True,
    manual_review: bool = False,
    temporal_reason: str = "",
    route: str = "gas",
    pressure_label: str = "ambient",
    temp_c: float = 20.0,
) -> dict[str, object]:
    return {
        "point_title": title,
        "point_no": point_no,
        "point_tag": f"row-{point_no}",
        "point_row": str(point_no),
        "route": route,
        "pressure_target_label": pressure_label,
        "co2_ppm_target": target,
        "temp_chamber_c": temp_c,
        "measured_value": measured,
        "measured_value_source": measured_source,
        "co2_calibration_candidate_status": candidate_status,
        "co2_calibration_candidate_recommended": recommended,
        "co2_calibration_candidate_hard_blocked": hard_blocked,
        "co2_calibration_weight_recommended": weight,
        "co2_calibration_reason_chain": (
            "waterfall=pass;steady_state_window"
            if candidate_status == "fit"
            else "waterfall=warn;source_fallback"
            if recommended
            else "waterfall=fail;no_trusted_source"
        ),
        "co2_sampling_settle_status": sampling_status,
        "co2_sampling_window_confidence": confidence_bucket,
        "co2_sampling_confidence_score": confidence_score,
        "co2_sampling_recommended_for_score_path": score_path,
        "co2_sampling_manual_review_required": manual_review,
        "co2_sampling_settle_reason_chain": temporal_reason or sampling_status,
        "co2_temporal_contract_reason": temporal_reason,
    }


def _fit_summary(
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
        }
    }


def _sampling_payload(points: list[dict[str, object]]) -> dict[str, object]:
    return {
        "summary": {
            "point_count_total": len(points),
            "ready_count": sum(1 for row in points if row["co2_sampling_settle_status"] == "ready"),
            "fallback_but_usable_count": sum(
                1 for row in points if row["co2_sampling_settle_status"] == "fallback_but_usable"
            ),
            "manual_review_count": sum(
                1 for row in points if row["co2_sampling_settle_status"] == "manual_review"
            ),
            "unfit_count": sum(1 for row in points if row["co2_sampling_settle_status"] == "unfit"),
            "not_real_acceptance_evidence": True,
        },
        "points": points,
        "groups": [],
    }


def _rows_for_tool() -> list[dict[str, object]]:
    return [
        {
            "point_title": "clean-400",
            "point_no": 1,
            "point_tag": "row-1",
            "point_row": "1",
            "route": "gas",
            "pressure_target_label": "ambient",
            "co2_ppm_target": 400.0,
            "temp_chamber_c": 20.0,
            "measured_value": 400.0,
            "measured_value_source": "co2_steady_state_window",
            "co2_point_suitability_status": "fit",
            "co2_calibration_candidate_status": "fit",
            "co2_calibration_candidate_recommended": True,
            "co2_calibration_candidate_hard_blocked": False,
            "co2_calibration_weight_recommended": 1.0,
            "co2_calibration_reason_chain": "waterfall=pass;steady_state_window",
            "co2_evidence_score": 100.0,
            "co2_decision_waterfall_status": "pass",
            "co2_source_selected": "primary",
            "co2_source_switch_reason": "",
            "co2_temporal_contract_status": "pass",
            "co2_temporal_contract_reason": "",
            "co2_phase_excluded_count": 0,
            "co2_phase_excluded_ratio": 0.0,
            "co2_phase_reason_summary": "",
            "co2_source_segment_selected": "primary#1",
            "co2_source_segment_settle_excluded_count": 0,
            "co2_source_segment_settle_excluded_ratio": 0.0,
            "co2_source_segment_reason_summary": "",
            "co2_bad_frame_count": 0,
            "co2_bad_frame_ratio": 0.0,
            "co2_soft_warn_count": 0,
            "co2_soft_warn_ratio": 0.0,
            "co2_timestamp_monotonic": True,
            "co2_duplicate_timestamp_count": 0,
            "co2_backward_timestamp_count": 0,
            "co2_large_gap_count": 0,
            "co2_effective_dwell_seconds": 4.0,
            "co2_nominal_dt_seconds": 1.0,
            "co2_cadence_coverage_ratio": 1.0,
            "co2_temporal_fallback_reason": "",
            "co2_steady_window_found": True,
            "co2_steady_window_status": "pass",
            "co2_steady_window_reason": "",
            "co2_steady_window_sample_count": 4,
            "point_quality_status": "pass",
            "point_quality_reason": "",
        },
        {
            "point_title": "clean-500",
            "point_no": 2,
            "point_tag": "row-2",
            "point_row": "2",
            "route": "gas",
            "pressure_target_label": "ambient",
            "co2_ppm_target": 500.0,
            "temp_chamber_c": 20.0,
            "measured_value": 500.0,
            "measured_value_source": "co2_steady_state_window",
            "co2_point_suitability_status": "fit",
            "co2_calibration_candidate_status": "fit",
            "co2_calibration_candidate_recommended": True,
            "co2_calibration_candidate_hard_blocked": False,
            "co2_calibration_weight_recommended": 0.95,
            "co2_calibration_reason_chain": "waterfall=pass;steady_state_window",
            "co2_evidence_score": 97.0,
            "co2_decision_waterfall_status": "pass",
            "co2_source_selected": "primary",
            "co2_source_switch_reason": "",
            "co2_temporal_contract_status": "pass",
            "co2_temporal_contract_reason": "",
            "co2_phase_excluded_count": 0,
            "co2_phase_excluded_ratio": 0.0,
            "co2_phase_reason_summary": "",
            "co2_source_segment_selected": "primary#1",
            "co2_source_segment_settle_excluded_count": 0,
            "co2_source_segment_settle_excluded_ratio": 0.0,
            "co2_source_segment_reason_summary": "",
            "co2_bad_frame_count": 0,
            "co2_bad_frame_ratio": 0.0,
            "co2_soft_warn_count": 0,
            "co2_soft_warn_ratio": 0.0,
            "co2_timestamp_monotonic": True,
            "co2_duplicate_timestamp_count": 0,
            "co2_backward_timestamp_count": 0,
            "co2_large_gap_count": 0,
            "co2_effective_dwell_seconds": 4.0,
            "co2_nominal_dt_seconds": 1.0,
            "co2_cadence_coverage_ratio": 1.0,
            "co2_temporal_fallback_reason": "",
            "co2_steady_window_found": True,
            "co2_steady_window_status": "pass",
            "co2_steady_window_reason": "",
            "co2_steady_window_sample_count": 4,
            "point_quality_status": "pass",
            "point_quality_reason": "",
        },
        {
            "point_title": "fallback-usable",
            "point_no": 3,
            "point_tag": "row-3",
            "point_row": "3",
            "route": "gas",
            "pressure_target_label": "ambient",
            "co2_ppm_target": 500.0,
            "temp_chamber_c": 20.0,
            "measured_value": 560.0,
            "measured_value_source": "co2_trailing_window_fallback",
            "co2_point_suitability_status": "advisory",
            "co2_calibration_candidate_status": "advisory",
            "co2_calibration_candidate_recommended": True,
            "co2_calibration_candidate_hard_blocked": False,
            "co2_calibration_weight_recommended": 0.35,
            "co2_calibration_reason_chain": "waterfall=warn;source_fallback",
            "co2_evidence_score": 58.0,
            "co2_decision_waterfall_status": "warn",
            "co2_source_selected": "ga02",
            "co2_source_switch_reason": "source_fallback",
            "co2_temporal_contract_status": "pass",
            "co2_temporal_contract_reason": "",
            "co2_phase_excluded_count": 0,
            "co2_phase_excluded_ratio": 0.0,
            "co2_phase_reason_summary": "",
            "co2_source_segment_selected": "ga02#1",
            "co2_source_segment_settle_excluded_count": 1,
            "co2_source_segment_settle_excluded_ratio": 0.1,
            "co2_source_segment_reason_summary": "settle_head_rows_excluded",
            "co2_bad_frame_count": 0,
            "co2_bad_frame_ratio": 0.0,
            "co2_soft_warn_count": 0,
            "co2_soft_warn_ratio": 0.0,
            "co2_timestamp_monotonic": True,
            "co2_duplicate_timestamp_count": 0,
            "co2_backward_timestamp_count": 0,
            "co2_large_gap_count": 0,
            "co2_effective_dwell_seconds": 4.0,
            "co2_nominal_dt_seconds": 1.0,
            "co2_cadence_coverage_ratio": 1.0,
            "co2_temporal_fallback_reason": "",
            "co2_steady_window_found": False,
            "co2_steady_window_status": "warn",
            "co2_steady_window_reason": "fallback=trailing_window",
            "co2_steady_window_sample_count": 4,
            "point_quality_status": "warn",
            "point_quality_reason": "",
        },
        {
            "point_title": "clean-800",
            "point_no": 4,
            "point_tag": "row-4",
            "point_row": "4",
            "route": "gas",
            "pressure_target_label": "ambient",
            "co2_ppm_target": 800.0,
            "temp_chamber_c": 20.0,
            "measured_value": 800.0,
            "measured_value_source": "co2_steady_state_window",
            "co2_point_suitability_status": "fit",
            "co2_calibration_candidate_status": "fit",
            "co2_calibration_candidate_recommended": True,
            "co2_calibration_candidate_hard_blocked": False,
            "co2_calibration_weight_recommended": 0.92,
            "co2_calibration_reason_chain": "waterfall=pass;steady_state_window",
            "co2_evidence_score": 96.0,
            "co2_decision_waterfall_status": "pass",
            "co2_source_selected": "primary",
            "co2_source_switch_reason": "",
            "co2_temporal_contract_status": "pass",
            "co2_temporal_contract_reason": "",
            "co2_phase_excluded_count": 0,
            "co2_phase_excluded_ratio": 0.0,
            "co2_phase_reason_summary": "",
            "co2_source_segment_selected": "primary#1",
            "co2_source_segment_settle_excluded_count": 0,
            "co2_source_segment_settle_excluded_ratio": 0.0,
            "co2_source_segment_reason_summary": "",
            "co2_bad_frame_count": 0,
            "co2_bad_frame_ratio": 0.0,
            "co2_soft_warn_count": 0,
            "co2_soft_warn_ratio": 0.0,
            "co2_timestamp_monotonic": True,
            "co2_duplicate_timestamp_count": 0,
            "co2_backward_timestamp_count": 0,
            "co2_large_gap_count": 0,
            "co2_effective_dwell_seconds": 4.0,
            "co2_nominal_dt_seconds": 1.0,
            "co2_cadence_coverage_ratio": 1.0,
            "co2_temporal_fallback_reason": "",
            "co2_steady_window_found": True,
            "co2_steady_window_status": "pass",
            "co2_steady_window_reason": "",
            "co2_steady_window_sample_count": 4,
            "point_quality_status": "pass",
            "point_quality_reason": "",
        },
        {
            "point_title": "manual-review-fit",
            "point_no": 5,
            "point_tag": "row-5",
            "point_row": "5",
            "route": "gas",
            "pressure_target_label": "ambient",
            "co2_ppm_target": 900.0,
            "temp_chamber_c": 20.0,
            "measured_value": 905.0,
            "measured_value_source": "co2_steady_state_window",
            "co2_point_suitability_status": "fit",
            "co2_calibration_candidate_status": "fit",
            "co2_calibration_candidate_recommended": True,
            "co2_calibration_candidate_hard_blocked": False,
            "co2_calibration_weight_recommended": 0.88,
            "co2_calibration_reason_chain": "waterfall=pass;steady_state_window",
            "co2_evidence_score": 86.0,
            "co2_decision_waterfall_status": "warn",
            "co2_source_selected": "primary",
            "co2_source_switch_reason": "",
            "co2_temporal_contract_status": "warn",
            "co2_temporal_contract_reason": "large_gap_detected",
            "co2_phase_excluded_count": 0,
            "co2_phase_excluded_ratio": 0.0,
            "co2_phase_reason_summary": "",
            "co2_source_segment_selected": "primary#1",
            "co2_source_segment_settle_excluded_count": 0,
            "co2_source_segment_settle_excluded_ratio": 0.0,
            "co2_source_segment_reason_summary": "",
            "co2_bad_frame_count": 0,
            "co2_bad_frame_ratio": 0.0,
            "co2_soft_warn_count": 0,
            "co2_soft_warn_ratio": 0.0,
            "co2_timestamp_monotonic": True,
            "co2_duplicate_timestamp_count": 0,
            "co2_backward_timestamp_count": 0,
            "co2_large_gap_count": 1,
            "co2_effective_dwell_seconds": 2.0,
            "co2_nominal_dt_seconds": 1.0,
            "co2_cadence_coverage_ratio": 0.6,
            "co2_temporal_fallback_reason": "",
            "co2_steady_window_found": False,
            "co2_steady_window_status": "warn",
            "co2_steady_window_reason": "fallback=trailing_window",
            "co2_steady_window_sample_count": 4,
            "point_quality_status": "warn",
            "point_quality_reason": "large_gap_detected",
        },
        {
            "point_title": "blocked-no-source",
            "point_no": 6,
            "point_tag": "row-6",
            "point_row": "6",
            "route": "gas",
            "pressure_target_label": "ambient",
            "co2_ppm_target": 700.0,
            "temp_chamber_c": 20.0,
            "measured_value": 710.0,
            "measured_value_source": "co2_no_trusted_source",
            "co2_point_suitability_status": "unfit",
            "co2_calibration_candidate_status": "unfit",
            "co2_calibration_candidate_recommended": False,
            "co2_calibration_candidate_hard_blocked": True,
            "co2_calibration_weight_recommended": 0.0,
            "co2_calibration_reason_chain": "waterfall=fail;no_trusted_source",
            "co2_evidence_score": 0.0,
            "co2_decision_waterfall_status": "fail",
            "co2_source_selected": "",
            "co2_source_switch_reason": "",
            "co2_temporal_contract_status": "fail",
            "co2_temporal_contract_reason": "timestamp_rollback_detected",
            "co2_phase_excluded_count": 0,
            "co2_phase_excluded_ratio": 0.0,
            "co2_phase_reason_summary": "",
            "co2_source_segment_selected": "",
            "co2_source_segment_settle_excluded_count": 0,
            "co2_source_segment_settle_excluded_ratio": 0.0,
            "co2_source_segment_reason_summary": "",
            "co2_bad_frame_count": 0,
            "co2_bad_frame_ratio": 0.0,
            "co2_soft_warn_count": 0,
            "co2_soft_warn_ratio": 0.0,
            "co2_timestamp_monotonic": False,
            "co2_duplicate_timestamp_count": 0,
            "co2_backward_timestamp_count": 1,
            "co2_large_gap_count": 0,
            "co2_effective_dwell_seconds": 1.0,
            "co2_nominal_dt_seconds": 1.0,
            "co2_cadence_coverage_ratio": 0.4,
            "co2_temporal_fallback_reason": "",
            "co2_steady_window_found": False,
            "co2_steady_window_status": "warn",
            "co2_steady_window_reason": "fallback=trailing_window",
            "co2_steady_window_sample_count": 4,
            "point_quality_status": "fail",
            "point_quality_reason": "timestamp_rollback_detected",
        },
    ]


def test_release_readiness_merges_clean_fallback_manual_review_and_unfit() -> None:
    fit_payload = _fit_summary(
        best_by_score="weighted_fit_advisory",
        best_by_stability="baseline_unweighted_fit_only",
        best_balanced_choice="baseline_unweighted_fit_only",
        recommended_release_candidate="baseline_unweighted_fit_only",
        manual_review_required=False,
    )
    sampling_payload = _sampling_payload(
        [
            _sampling_point(
                title="clean-ready",
                point_no=1,
                target=500.0,
                measured=500.0,
                candidate_status="fit",
                recommended=True,
                hard_blocked=False,
                weight=1.0,
            ),
            _sampling_point(
                title="fallback-usable",
                point_no=2,
                target=500.0,
                measured=560.0,
                candidate_status="advisory",
                recommended=True,
                hard_blocked=False,
                weight=0.35,
                measured_source="co2_trailing_window_fallback",
                sampling_status="fallback_but_usable",
                confidence_bucket="medium",
                confidence_score=67.0,
            ),
            _sampling_point(
                title="fit-but-sampling-manual",
                point_no=3,
                target=900.0,
                measured=905.0,
                candidate_status="fit",
                recommended=True,
                hard_blocked=False,
                weight=0.88,
                sampling_status="manual_review",
                confidence_bucket="low",
                confidence_score=42.0,
                manual_review=True,
                temporal_reason="large_gap_detected",
            ),
            _sampling_point(
                title="blocked-unfit",
                point_no=4,
                target=700.0,
                measured=710.0,
                candidate_status="unfit",
                recommended=False,
                hard_blocked=True,
                weight=0.0,
                measured_source="co2_no_trusted_source",
                sampling_status="unfit",
                confidence_bucket="none",
                confidence_score=0.0,
                score_path=False,
            ),
        ]
    )

    payload = build_co2_release_readiness_bundle(
        fit_arbitration_payload=fit_payload,
        sampling_settle_payload=sampling_payload,
    )

    by_title = {row["point_title"]: row for row in payload["points"]}
    assert by_title["clean-ready"]["release_readiness_status"] == "release_ready"
    assert by_title["clean-ready"]["score_path_eligibility"] is True

    assert by_title["fallback-usable"]["release_readiness_status"] == "score_path_only"
    assert by_title["fallback-usable"]["score_path_eligibility"] is True

    assert by_title["fit-but-sampling-manual"]["release_readiness_status"] == "manual_review"
    assert by_title["fit-but-sampling-manual"]["manual_review_required"] is True
    assert "large_gap_detected" in by_title["fit-but-sampling-manual"]["blocking_reason_chain"]

    assert by_title["blocked-unfit"]["release_readiness_status"] == "excluded"
    assert by_title["blocked-unfit"]["score_path_eligibility"] is False

    summary = payload["summary"]
    assert summary["best_fit_candidate"] == "baseline_unweighted_fit_only"
    assert summary["recommended_release_candidate"] == ""
    assert summary["release_readiness_verdict"] == "manual_review"
    assert summary["manual_review_required"] is True


def test_release_readiness_can_recommend_release_when_fit_and_sampling_align() -> None:
    fit_payload = _fit_summary(
        best_by_score="weighted_fit_advisory",
        best_by_stability="weighted_fit_advisory",
        best_balanced_choice="weighted_fit_advisory",
        recommended_release_candidate="weighted_fit_advisory",
        manual_review_required=False,
    )
    sampling_payload = _sampling_payload(
        [
            _sampling_point(
                title="clean-a",
                point_no=1,
                target=400.0,
                measured=400.0,
                candidate_status="fit",
                recommended=True,
                hard_blocked=False,
                weight=1.0,
            ),
            _sampling_point(
                title="clean-b",
                point_no=2,
                target=800.0,
                measured=800.0,
                candidate_status="fit",
                recommended=True,
                hard_blocked=False,
                weight=0.95,
            ),
        ]
    )

    payload = build_co2_release_readiness_bundle(
        fit_arbitration_payload=fit_payload,
        sampling_settle_payload=sampling_payload,
    )

    assert payload["summary"]["recommended_release_candidate"] == "weighted_fit_advisory"
    assert payload["summary"]["release_readiness_verdict"] == "release_ready"
    assert payload["summary"]["manual_review_required"] is False


def test_release_readiness_tool_writes_expected_artifacts_from_points_csv(tmp_path: Path) -> None:
    points_csv = tmp_path / "points.csv"
    output_dir = tmp_path / "release"
    rows = _rows_for_tool()
    with points_csv.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    assert build_release_readiness_main(
        ["--points-csv", str(points_csv), "--output-dir", str(output_dir)]
    ) == 0

    assert (output_dir / "release_readiness_points.csv").exists()
    assert (output_dir / "release_readiness_summary.csv").exists()
    assert (output_dir / "release_readiness_summary.json").exists()
    assert (output_dir / "release_readiness_report.md").exists()

    payload = json.loads((output_dir / "release_readiness_summary.json").read_text(encoding="utf-8"))
    assert payload["summary"]["not_real_acceptance_evidence"] is True
    assert payload["summary"]["best_fit_candidate"] != ""
    assert payload["summary"]["release_ready_points_count"] >= 1
    assert payload["summary"]["excluded_points_count"] >= 1
    report_text = (output_dir / "release_readiness_report.md").read_text(encoding="utf-8")
    assert "replay evidence only" in report_text
    assert "not real acceptance evidence" in report_text
