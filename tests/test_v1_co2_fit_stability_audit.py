from __future__ import annotations

import json
from pathlib import Path

import pytest

from gas_calibrator.tools.build_v1_co2_fit_stability_audit import main as build_stability_main
from gas_calibrator.tools.build_v1_co2_weighted_fit_advisory import main as build_weighted_main
from gas_calibrator.workflow.co2_fit_stability_audit import (
    build_co2_fit_stability_audit,
    extract_candidate_rows_from_weighted_fit_payload,
)
from gas_calibrator.workflow.co2_weighted_fit_advisory import build_co2_weighted_fit_advisory


def _candidate_row(
    *,
    title: str,
    point_no: int,
    target: float,
    temp_c: float,
    pressure_label: str,
    measured: float,
    suitability: str,
    recommended: bool,
    hard_blocked: bool,
    weight: float,
    evidence_score: float,
    measured_source: str = "co2_steady_state_window",
    route: str = "gas",
    switch_reason: str = "",
    temporal_status: str = "pass",
    temporal_reason: str = "",
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
        "co2_calibration_weight_reason": (
            "trusted_steady_state"
            if suitability == "fit"
            else "fallback_but_usable"
            if recommended
            else "hard_blocked_or_untrusted"
        ),
        "co2_calibration_reason_chain": (
            "waterfall=pass;steady_state_window"
            if suitability == "fit"
            else "waterfall=warn;source_fallback"
            if recommended
            else "waterfall=fail;no_trusted_source"
        ),
        "co2_evidence_score": evidence_score,
        "co2_decision_waterfall_status": "fail" if hard_blocked else "warn" if suitability == "advisory" else "pass",
        "co2_decision_selected_stage_path": "phase:pass>source_segment:pass>temporal:pass>quarantine:pass>source_trust:pass>steady_state:pass",
        "co2_source_selected": "primary" if not switch_reason else "ga02",
        "co2_source_switch_reason": switch_reason,
        "co2_temporal_contract_status": temporal_status,
        "co2_temporal_contract_reason": temporal_reason,
        "co2_point_evidence_budget_summary": f"total={evidence_score:.1f}/100",
    }


def _dataset() -> list[dict[str, object]]:
    return [
        _candidate_row(
            title="clean-400",
            point_no=1,
            target=400.0,
            temp_c=20.0,
            pressure_label="ambient",
            measured=400.0,
            suitability="fit",
            recommended=True,
            hard_blocked=False,
            weight=1.0,
            evidence_score=100.0,
        ),
        _candidate_row(
            title="clean-500",
            point_no=2,
            target=500.0,
            temp_c=20.0,
            pressure_label="ambient",
            measured=500.0,
            suitability="fit",
            recommended=True,
            hard_blocked=False,
            weight=0.95,
            evidence_score=97.0,
        ),
        _candidate_row(
            title="fallback-500",
            point_no=3,
            target=500.0,
            temp_c=20.0,
            pressure_label="ambient",
            measured=560.0,
            suitability="advisory",
            recommended=True,
            hard_blocked=False,
            weight=0.35,
            evidence_score=58.0,
            measured_source="co2_trailing_window_fallback",
            switch_reason="source_fallback",
            temporal_status="warn",
        ),
        _candidate_row(
            title="clean-800",
            point_no=4,
            target=800.0,
            temp_c=20.0,
            pressure_label="ambient",
            measured=800.0,
            suitability="fit",
            recommended=True,
            hard_blocked=False,
            weight=0.92,
            evidence_score=96.0,
        ),
        _candidate_row(
            title="clean-1000",
            point_no=5,
            target=1000.0,
            temp_c=20.0,
            pressure_label="ambient",
            measured=1000.0,
            suitability="fit",
            recommended=True,
            hard_blocked=False,
            weight=0.9,
            evidence_score=95.0,
        ),
        _candidate_row(
            title="blocked-no-source",
            point_no=6,
            target=700.0,
            temp_c=20.0,
            pressure_label="ambient",
            measured=710.0,
            suitability="unfit",
            recommended=False,
            hard_blocked=True,
            weight=0.0,
            evidence_score=0.0,
            measured_source="co2_no_trusted_source",
        ),
        _candidate_row(
            title="blocked-temporal",
            point_no=7,
            target=600.0,
            temp_c=20.0,
            pressure_label="ambient",
            measured=610.0,
            suitability="unfit",
            recommended=False,
            hard_blocked=True,
            weight=0.0,
            evidence_score=0.0,
            measured_source="co2_no_trusted_source",
            temporal_status="fail",
            temporal_reason="timestamp_rollback_detected",
        ),
    ]


def test_fit_stability_audit_consumes_weighted_fit_payload_without_rejudging() -> None:
    weighted_payload = build_co2_weighted_fit_advisory(_dataset())
    extracted_rows = extract_candidate_rows_from_weighted_fit_payload(weighted_payload)
    payload = build_co2_fit_stability_audit(extracted_rows)

    assert payload["summary"]["not_real_acceptance_evidence"] is True
    assert payload["summary"]["evaluation_point_count"] == 5
    assert {row["fit_variant_name"] for row in payload["stability_variants"]} == {
        "baseline_unweighted_all_recommended",
        "baseline_unweighted_fit_only",
        "weighted_fit_advisory",
    }


def test_fallback_point_participates_in_weighted_stability_with_lower_weight() -> None:
    payload = build_co2_fit_stability_audit(_dataset())
    variants = {row["fit_variant_name"]: row for row in payload["fit_variants"]}
    stability_variants = {row["fit_variant_name"]: row for row in payload["stability_variants"]}

    assert variants["baseline_unweighted_all_recommended"]["input_point_count"] == 5
    assert variants["weighted_fit_advisory"]["input_point_count"] == 4
    assert variants["weighted_fit_advisory"]["training_weight_sum"] == pytest.approx(3.77, abs=1e-9)
    assert variants["baseline_unweighted_fit_only"]["input_point_count"] == 4
    assert stability_variants["weighted_fit_advisory"]["candidate_pool_point_count"] == 5
    assert stability_variants["weighted_fit_advisory"]["strong_support_pool_point_count"] == 4
    assert stability_variants["weighted_fit_advisory"]["weak_support_pool_point_count"] == 1
    assert stability_variants["weighted_fit_advisory"]["pre_fit_clean_first_applied"] is True


def test_blocked_points_do_not_pollute_stability_baseline() -> None:
    payload = build_co2_fit_stability_audit(_dataset())

    assert payload["summary"]["point_count_total"] == 7
    assert payload["summary"]["evaluation_point_count"] == 5
    assert payload["summary"]["excluded_point_count"] == 2
    assert "hard_blocked:2" in payload["summary"]["excluded_reason_summary"]


def test_fit_stability_keeps_fallback_training_points_when_strong_support_is_insufficient() -> None:
    rows = [
        _candidate_row(
            title="clean-500",
            point_no=1,
            target=500.0,
            temp_c=20.0,
            pressure_label="ambient",
            measured=500.0,
            suitability="fit",
            recommended=True,
            hard_blocked=False,
            weight=1.0,
            evidence_score=100.0,
        ),
        _candidate_row(
            title="fallback-800",
            point_no=2,
            target=800.0,
            temp_c=20.0,
            pressure_label="ambient",
            measured=810.0,
            suitability="advisory",
            recommended=True,
            hard_blocked=False,
            weight=0.4,
            evidence_score=60.0,
            measured_source="co2_trailing_window_fallback",
            switch_reason="source_fallback",
            temporal_status="warn",
        ),
    ]

    payload = build_co2_fit_stability_audit(rows)
    stability_variants = {row["fit_variant_name"]: row for row in payload["stability_variants"]}

    assert stability_variants["weighted_fit_advisory"]["input_point_count"] == 2
    assert stability_variants["weighted_fit_advisory"]["candidate_pool_point_count"] == 2
    assert stability_variants["weighted_fit_advisory"]["weak_support_pool_point_count"] == 1
    assert stability_variants["weighted_fit_advisory"]["pre_fit_clean_first_applied"] is False


def test_leave_one_group_out_reports_group_influence_and_coefficient_deltas() -> None:
    payload = build_co2_fit_stability_audit(_dataset())
    groups = [
        row
        for row in payload["groups"]
        if row["fit_variant_name"] == "weighted_fit_advisory" and row["fit_status"] == "available"
    ]

    assert len(groups) == 4
    assert any(row["group_left_out"].endswith("target=500.000|temp=20.000|pressure=ambient") for row in groups)
    assert all(row["group_influence_score"] is not None for row in groups)


def test_fit_stability_audit_accepts_chinese_candidate_rows() -> None:
    rows = [
        {
            "点位标题": "中文clean",
            "点位编号": "1",
            "采样路线": "gas",
            "压力目标标签": "ambient",
            "目标二氧化碳浓度ppm": "500",
            "温箱目标温度C": "20",
            "测量值": "500.0",
            "测量值来源": "co2_steady_state_window",
            "气路点适用性": "fit",
            "气路校准候选推荐": "True",
            "气路校准候选硬阻断": "False",
            "气路推荐校准权重": "1.0",
            "气路证据分": "100",
        },
        {
            "点位标题": "中文fallback",
            "点位编号": "2",
            "采样路线": "gas",
            "压力目标标签": "ambient",
            "目标二氧化碳浓度ppm": "800",
            "温箱目标温度C": "20",
            "测量值": "810.0",
            "测量值来源": "co2_trailing_window_fallback",
            "气路点适用性": "advisory",
            "气路校准候选推荐": "True",
            "气路校准候选硬阻断": "False",
            "气路推荐校准权重": "0.5",
            "气路证据分": "60",
        },
    ]
    payload = build_co2_fit_stability_audit(rows)

    assert payload["summary"]["evaluation_point_count"] == 2
    assert len(payload["stability_variants"]) == 3


def test_fit_stability_tool_writes_expected_artifacts_from_weighted_fit_json(tmp_path: Path) -> None:
    candidate_pack_json = tmp_path / "candidate_pack.json"
    weighted_output_dir = tmp_path / "weighted_out"
    stability_output_dir = tmp_path / "stability_out"
    candidate_pack = {
        "summary": {"point_count_total": 7},
        "points": _dataset(),
        "groups": [],
    }
    candidate_pack_json.write_text(json.dumps(candidate_pack, ensure_ascii=False, indent=2), encoding="utf-8")

    assert build_weighted_main(["--candidate-pack-json", str(candidate_pack_json), "--output-dir", str(weighted_output_dir)]) == 0
    weighted_summary_json = weighted_output_dir / "weighted_fit_summary.json"

    assert build_stability_main(["--weighted-fit-summary-json", str(weighted_summary_json), "--output-dir", str(stability_output_dir)]) == 0

    assert (stability_output_dir / "fit_stability_groups.csv").exists()
    assert (stability_output_dir / "fit_stability_coefficients.csv").exists()
    assert (stability_output_dir / "fit_stability_summary.csv").exists()
    assert (stability_output_dir / "fit_stability_summary.json").exists()
    assert (stability_output_dir / "fit_stability_report.md").exists()

    summary_payload = json.loads((stability_output_dir / "fit_stability_summary.json").read_text(encoding="utf-8"))
    assert summary_payload["summary"]["not_real_acceptance_evidence"] is True
    report_text = (stability_output_dir / "fit_stability_report.md").read_text(encoding="utf-8")
    assert "replay evidence only" in report_text
    assert "not real acceptance evidence" in report_text
