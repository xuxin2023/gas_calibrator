from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from gas_calibrator.tools.build_v1_co2_sampling_settle_evidence import main as build_sampling_settle_main
from gas_calibrator.workflow.co2_calibration_candidate_pack import _FIELD_ALIASES
from gas_calibrator.workflow.co2_sampling_settle_evidence import (
    build_co2_sampling_settle_evidence,
)


def _row(
    *,
    title: str,
    point_no: int,
    target: float,
    measured: float,
    suitability: str,
    recommended: bool,
    hard_blocked: bool,
    weight: float,
    evidence_score: float,
    measured_source: str = "co2_steady_state_window",
    source_switch_reason: str = "",
    temporal_status: str = "pass",
    temporal_reason: str = "",
    waterfall_status: str = "pass",
    phase_ratio: float = 0.0,
    segment_ratio: float = 0.0,
    cadence_coverage: float | None = 1.0,
    effective_dwell_s: float | None = 4.0,
    backward_count: int = 0,
    duplicate_count: int = 0,
    large_gap_count: int = 0,
    steady_window_found: bool = True,
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
        "co2_point_suitability_status": suitability,
        "co2_calibration_candidate_status": suitability,
        "co2_calibration_candidate_recommended": recommended,
        "co2_calibration_candidate_hard_blocked": hard_blocked,
        "co2_calibration_weight_recommended": weight,
        "co2_calibration_reason_chain": (
            "waterfall=pass;steady_state_window"
            if suitability == "fit"
            else "waterfall=warn;source_fallback"
            if recommended
            else "waterfall=fail;no_trusted_source"
        ),
        "co2_evidence_score": evidence_score,
        "co2_decision_waterfall_status": waterfall_status,
        "co2_decision_selected_stage_path": "phase:pass>source_segment:pass>temporal:pass>quarantine:pass>source_trust:pass>steady_state:pass",
        "co2_source_selected": "primary" if not source_switch_reason else "ga02",
        "co2_source_switch_reason": source_switch_reason,
        "co2_temporal_contract_status": temporal_status,
        "co2_temporal_contract_reason": temporal_reason,
        "co2_phase_excluded_count": round(phase_ratio * 10),
        "co2_phase_excluded_ratio": phase_ratio,
        "co2_phase_reason_summary": "transition_head_delta_gt_threshold" if phase_ratio else "",
        "co2_source_segment_selected": "primary#1" if not source_switch_reason else "ga02#1",
        "co2_source_segment_settle_excluded_count": round(segment_ratio * 10),
        "co2_source_segment_settle_excluded_ratio": segment_ratio,
        "co2_source_segment_reason_summary": "settle_head_rows_excluded" if segment_ratio else "",
        "co2_bad_frame_count": 0,
        "co2_bad_frame_ratio": 0.0,
        "co2_soft_warn_count": 0,
        "co2_soft_warn_ratio": 0.0,
        "co2_timestamp_monotonic": backward_count <= 0,
        "co2_duplicate_timestamp_count": duplicate_count,
        "co2_backward_timestamp_count": backward_count,
        "co2_large_gap_count": large_gap_count,
        "co2_effective_dwell_seconds": effective_dwell_s,
        "co2_nominal_dt_seconds": 1.0 if cadence_coverage is not None else None,
        "co2_cadence_coverage_ratio": cadence_coverage,
        "co2_temporal_fallback_reason": "",
        "co2_steady_window_found": steady_window_found,
        "co2_steady_window_status": "pass" if steady_window_found else "warn",
        "co2_steady_window_reason": "" if steady_window_found else "fallback=trailing_window",
        "co2_steady_window_sample_count": 4,
        "point_quality_status": "warn" if suitability != "fit" else "pass",
        "point_quality_reason": temporal_reason,
    }


def _dataset() -> list[dict[str, object]]:
    return [
        _row(
            title="clean-ready",
            point_no=1,
            target=500.0,
            measured=500.0,
            suitability="fit",
            recommended=True,
            hard_blocked=False,
            weight=1.0,
            evidence_score=100.0,
        ),
        _row(
            title="fallback-usable",
            point_no=2,
            target=500.0,
            measured=560.0,
            suitability="advisory",
            recommended=True,
            hard_blocked=False,
            weight=0.35,
            evidence_score=58.0,
            measured_source="co2_trailing_window_fallback",
            source_switch_reason="source_fallback",
            temporal_status="pass",
        ),
        _row(
            title="temporal-manual-review",
            point_no=3,
            target=700.0,
            measured=705.0,
            suitability="advisory",
            recommended=True,
            hard_blocked=False,
            weight=0.45,
            evidence_score=54.0,
            temporal_status="warn",
            temporal_reason="large_gap_detected",
            cadence_coverage=0.60,
            effective_dwell_s=2.0,
            large_gap_count=1,
            steady_window_found=False,
            measured_source="co2_trailing_window_fallback",
        ),
        _row(
            title="blocked-unfit",
            point_no=4,
            target=800.0,
            measured=810.0,
            suitability="unfit",
            recommended=False,
            hard_blocked=True,
            weight=0.0,
            evidence_score=0.0,
            measured_source="co2_no_trusted_source",
            temporal_status="fail",
            temporal_reason="timestamp_rollback_detected",
            backward_count=1,
            cadence_coverage=0.40,
            effective_dwell_s=1.0,
            steady_window_found=False,
            waterfall_status="fail",
        ),
    ]


def test_sampling_settle_evidence_classifies_ready_fallback_manual_review_and_unfit() -> None:
    payload = build_co2_sampling_settle_evidence(_dataset())

    summary = payload["summary"]
    assert summary["point_count_total"] == 4
    assert summary["ready_count"] == 1
    assert summary["fallback_but_usable_count"] == 1
    assert summary["manual_review_count"] == 1
    assert summary["unfit_count"] == 1
    assert summary["not_real_acceptance_evidence"] is True

    by_title = {row["point_title"]: row for row in payload["points"]}
    assert by_title["clean-ready"]["co2_sampling_settle_status"] == "ready"
    assert by_title["clean-ready"]["co2_sampling_window_confidence"] == "high"
    assert by_title["clean-ready"]["measured_value"] == 500.0

    assert by_title["fallback-usable"]["co2_sampling_settle_status"] == "fallback_but_usable"
    assert by_title["fallback-usable"]["co2_sampling_manual_review_required"] is False

    assert by_title["temporal-manual-review"]["co2_sampling_settle_status"] == "manual_review"
    assert by_title["temporal-manual-review"]["co2_sampling_manual_review_required"] is True
    assert "large_gap_detected" in by_title["temporal-manual-review"]["co2_sampling_settle_reason_chain"]

    assert by_title["blocked-unfit"]["co2_sampling_settle_status"] == "unfit"
    assert by_title["blocked-unfit"]["co2_sampling_recommended_for_score_path"] is False
    assert by_title["blocked-unfit"]["co2_sampling_confidence_score"] == 0.0


def test_sampling_settle_evidence_accepts_chinese_fields_and_degrades_when_sampling_fields_missing() -> None:
    def zh(field: str) -> str:
        return _FIELD_ALIASES[field][1]

    rows = [
        {
            zh("point_title"): "中文点",
            zh("point_no"): "7",
            zh("route"): "gas",
            zh("pressure_target_label"): "ambient",
            zh("co2_ppm_target"): "400",
            zh("temp_chamber_c"): "20",
            zh("measured_value"): "401.2",
            zh("measured_value_source"): "co2_steady_state_window",
            zh("co2_point_suitability_status"): "fit",
            zh("co2_calibration_candidate_recommended"): "True",
            zh("co2_calibration_candidate_hard_blocked"): "False",
            zh("co2_calibration_weight_recommended"): "0.88",
            zh("co2_evidence_score"): "88",
            zh("co2_decision_waterfall_status"): "pass",
        }
    ]

    payload = build_co2_sampling_settle_evidence(rows)
    point = payload["points"][0]

    assert point["co2_sampling_settle_status"] == "ready"
    assert "phase_fields_missing" in point["co2_sampling_settle_fallback_reason"]
    assert "temporal_fields_missing" in point["co2_sampling_settle_fallback_reason"]


def test_sampling_settle_tool_writes_expected_artifacts_from_points_csv(tmp_path: Path) -> None:
    points_csv = tmp_path / "points.csv"
    output_dir = tmp_path / "sampling_settle"
    rows = _dataset()
    with points_csv.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    assert build_sampling_settle_main(["--points-csv", str(points_csv), "--output-dir", str(output_dir)]) == 0

    assert (output_dir / "sampling_settle_points.csv").exists()
    assert (output_dir / "sampling_settle_groups.csv").exists()
    assert (output_dir / "sampling_settle_summary.csv").exists()
    assert (output_dir / "sampling_settle_summary.json").exists()
    assert (output_dir / "sampling_settle_report.md").exists()

    payload = json.loads((output_dir / "sampling_settle_summary.json").read_text(encoding="utf-8"))
    assert payload["summary"]["ready_count"] == 1
    assert payload["summary"]["manual_review_count"] == 1
    report_text = (output_dir / "sampling_settle_report.md").read_text(encoding="utf-8")
    assert "replay evidence only" in report_text
    assert "not real acceptance evidence" in report_text


def test_sampling_settle_tool_accepts_candidate_pack_json_with_degraded_fields(tmp_path: Path) -> None:
    candidate_pack_json = tmp_path / "candidate_pack.json"
    output_dir = tmp_path / "sampling_settle"
    minimal_point = {
        "point_title": "candidate-only",
        "point_no": "9",
        "point_tag": "row-9",
        "point_row": "9",
        "route": "gas",
        "pressure_target_label": "ambient",
        "co2_ppm_target": 450.0,
        "temp_chamber_c": 20.0,
        "measured_value": 451.0,
        "measured_value_source": "co2_steady_state_window",
        "co2_point_suitability_status": "fit",
        "co2_calibration_candidate_recommended": True,
        "co2_calibration_candidate_hard_blocked": False,
        "co2_calibration_weight_recommended": 0.88,
        "co2_evidence_score": 88.0,
        "co2_decision_waterfall_status": "pass",
        "co2_decision_selected_stage_path": "phase:pass>source_segment:pass>temporal:pass>quarantine:pass>source_trust:pass>steady_state:pass",
    }
    candidate_pack_json.write_text(
        json.dumps({"summary": {"point_count_total": 1}, "points": [minimal_point], "groups": []}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    assert build_sampling_settle_main(
        ["--candidate-pack-json", str(candidate_pack_json), "--output-dir", str(output_dir)]
    ) == 0

    payload = json.loads((output_dir / "sampling_settle_summary.json").read_text(encoding="utf-8"))
    assert payload["summary"]["point_count_total"] == 1
    assert payload["summary"]["not_real_acceptance_evidence"] is True
    point = payload["points"][0]
    assert "phase_fields_missing" in point["co2_sampling_settle_fallback_reason"]
    assert "temporal_fields_missing" in point["co2_sampling_settle_fallback_reason"]
