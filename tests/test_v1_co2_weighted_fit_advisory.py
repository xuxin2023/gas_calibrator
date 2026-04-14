from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from gas_calibrator.tools.build_v1_co2_weighted_fit_advisory import main as build_weighted_main
from gas_calibrator.workflow.co2_weighted_fit_advisory import (
    build_co2_weighted_fit_advisory,
)


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
            title="clean-800",
            point_no=2,
            target=800.0,
            temp_c=20.0,
            pressure_label="ambient",
            measured=800.0,
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
            title="blocked-no-source",
            point_no=4,
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
            point_no=5,
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


def test_weighted_fit_advisory_consumes_candidate_pack_without_rejudging() -> None:
    payload = build_co2_weighted_fit_advisory(_dataset())

    summary = payload["summary"]
    assert summary["point_count_total"] == 5
    assert summary["evaluation_point_count"] == 3
    assert summary["excluded_point_count"] == 2
    assert summary["not_real_acceptance_evidence"] is True

    variants = {row["fit_variant_name"]: row for row in payload["fit_variants"]}
    assert variants["baseline_unweighted_all_recommended"]["input_point_count"] == 3
    assert variants["baseline_unweighted_fit_only"]["input_point_count"] == 2
    assert variants["weighted_fit_advisory"]["input_point_count"] == 2
    assert variants["weighted_fit_advisory"]["candidate_pool_point_count"] == 3
    assert variants["weighted_fit_advisory"]["strong_support_pool_point_count"] == 2
    assert pytest.approx(float(variants["weighted_fit_advisory"]["training_weight_sum"]), abs=1e-9) == 1.95
    assert variants["weighted_fit_advisory"]["pre_fit_clean_first_applied"] is True
    assert variants["weighted_fit_advisory"]["weak_support_pool_point_count"] == 1
    assert "hard_blocked:2" in variants["weighted_fit_advisory"]["excluded_reason_summary"]
    assert payload["summary"]["recommended_fit_variant"] == "baseline_unweighted_fit_only"


def test_fallback_but_usable_point_enters_weighted_fit_with_lower_weight() -> None:
    payload = build_co2_weighted_fit_advisory(_dataset())
    weighted_points = [
        row
        for row in payload["points"]
        if row["fit_variant_name"] == "weighted_fit_advisory"
        and row["point_title"] == "fallback-500"
    ]

    assert len(weighted_points) == 1
    point = weighted_points[0]
    assert pytest.approx(float(point["fit_training_weight"]), abs=1e-9) == 0.35
    assert point["co2_calibration_candidate_status"] == "advisory"
    assert point["measured_value"] == 560.0


def test_weighted_fit_advisory_keeps_fallback_training_points_when_strong_support_is_insufficient() -> None:
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

    payload = build_co2_weighted_fit_advisory(rows)
    variants = {row["fit_variant_name"]: row for row in payload["fit_variants"]}

    assert variants["weighted_fit_advisory"]["input_point_count"] == 2
    assert variants["weighted_fit_advisory"]["candidate_pool_point_count"] == 2
    assert variants["weighted_fit_advisory"]["strong_support_pool_point_count"] == 1
    assert variants["weighted_fit_advisory"]["pre_fit_clean_first_applied"] is False
    assert variants["weighted_fit_advisory"]["weak_support_pool_point_count"] == 1


def test_temporal_and_no_trusted_source_points_do_not_enter_weighted_fit() -> None:
    payload = build_co2_weighted_fit_advisory(_dataset())
    weighted_titles = {
        row["point_title"]
        for row in payload["points"]
        if row["fit_variant_name"] == "weighted_fit_advisory"
    }

    assert "blocked-no-source" not in weighted_titles
    assert "blocked-temporal" not in weighted_titles


def test_grouped_weighted_summary_is_reported() -> None:
    payload = build_co2_weighted_fit_advisory(_dataset())
    groups = {
        (row["fit_variant_name"], row["calibration_group_key"]): row
        for row in payload["groups"]
    }

    weighted_500 = groups[("weighted_fit_advisory", "co2|route=gas|target=500.000|temp=20.000|pressure=ambient")]
    assert weighted_500["point_count_total"] == 2
    assert pytest.approx(float(weighted_500["weight_sum"]), abs=1e-9) == 1.35
    assert weighted_500["group_recommended_for_fit"] is True


def test_weighted_fit_advisory_prefers_stronger_support_when_score_gap_is_small() -> None:
    rows = [
        _candidate_row(title="clean-400", point_no=1, target=400.0, temp_c=20.0, pressure_label="ambient", measured=400.0, suitability="fit", recommended=True, hard_blocked=False, weight=1.0, evidence_score=100.0),
        _candidate_row(title="clean-500", point_no=2, target=500.0, temp_c=20.0, pressure_label="ambient", measured=500.0, suitability="fit", recommended=True, hard_blocked=False, weight=0.95, evidence_score=97.0),
        _candidate_row(title="fallback-500", point_no=3, target=500.0, temp_c=20.0, pressure_label="ambient", measured=560.0, suitability="advisory", recommended=True, hard_blocked=False, weight=0.35, evidence_score=58.0, measured_source="co2_trailing_window_fallback", switch_reason="source_fallback", temporal_status="warn"),
        _candidate_row(title="clean-800", point_no=4, target=800.0, temp_c=20.0, pressure_label="ambient", measured=800.0, suitability="fit", recommended=True, hard_blocked=False, weight=0.92, evidence_score=96.0),
        _candidate_row(title="clean-1000", point_no=5, target=1000.0, temp_c=20.0, pressure_label="ambient", measured=1000.0, suitability="fit", recommended=True, hard_blocked=False, weight=0.9, evidence_score=95.0),
        _candidate_row(title="blocked-no-source", point_no=6, target=700.0, temp_c=20.0, pressure_label="ambient", measured=710.0, suitability="unfit", recommended=False, hard_blocked=True, weight=0.0, evidence_score=0.0, measured_source="co2_no_trusted_source"),
        _candidate_row(title="blocked-temporal", point_no=7, target=600.0, temp_c=20.0, pressure_label="ambient", measured=610.0, suitability="unfit", recommended=False, hard_blocked=True, weight=0.0, evidence_score=0.0, measured_source="co2_no_trusted_source", temporal_status="fail", temporal_reason="timestamp_rollback_detected"),
    ]

    payload = build_co2_weighted_fit_advisory(rows)
    variants = {row["fit_variant_name"]: row for row in payload["fit_variants"]}

    assert payload["summary"]["recommended_fit_variant"] == "baseline_unweighted_fit_only"
    assert variants["weighted_fit_advisory"]["candidate_pool_point_count"] == 5
    assert variants["weighted_fit_advisory"]["strong_support_pool_point_count"] == 4
    assert variants["weighted_fit_advisory"]["weak_support_point_count"] == 0
    assert variants["weighted_fit_advisory"]["weak_support_pool_point_count"] == 1
    assert variants["baseline_unweighted_fit_only"]["weak_support_point_count"] == 0
    assert variants["weighted_fit_advisory"]["pre_fit_clean_first_applied"] is True
    assert "support_tie_break_within_weighted_rmse_margin" in payload["summary"]["recommended_fit_reason"]
    assert "prefer_stronger_support_over=weighted_fit_advisory" in payload["summary"]["recommended_fit_reason"]


def test_weighted_fit_advisory_accepts_chinese_candidate_rows() -> None:
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
            "气路点适用性原因链": "waterfall=pass;steady_state_window",
            "气路决策瀑布结果": "pass",
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
            "气路点适用性原因链": "waterfall=warn;source_fallback",
            "气路决策瀑布结果": "warn",
        },
    ]
    payload = build_co2_weighted_fit_advisory(rows)

    assert payload["summary"]["evaluation_point_count"] == 2
    assert {row["fit_variant_name"] for row in payload["fit_variants"]} == {
        "baseline_unweighted_all_recommended",
        "baseline_unweighted_fit_only",
        "weighted_fit_advisory",
    }


def test_weighted_fit_advisory_tool_writes_all_expected_artifacts(tmp_path: Path) -> None:
    candidate_pack_json = tmp_path / "candidate_pack.json"
    output_dir = tmp_path / "weighted_fit_out"
    candidate_pack = {
        "summary": {"point_count_total": 5},
        "points": _dataset(),
        "groups": [],
    }
    candidate_pack_json.write_text(json.dumps(candidate_pack, ensure_ascii=False, indent=2), encoding="utf-8")

    assert build_weighted_main(["--candidate-pack-json", str(candidate_pack_json), "--output-dir", str(output_dir)]) == 0

    assert (output_dir / "weighted_fit_points.csv").exists()
    assert (output_dir / "weighted_fit_groups.csv").exists()
    assert (output_dir / "weighted_fit_summary.csv").exists()
    assert (output_dir / "weighted_fit_summary.json").exists()
    assert (output_dir / "weighted_fit_report.md").exists()
    assert (output_dir / "weighted_fit_coefficients.json").exists()

    summary_payload = json.loads((output_dir / "weighted_fit_summary.json").read_text(encoding="utf-8"))
    assert summary_payload["summary"]["not_real_acceptance_evidence"] is True
    report_text = (output_dir / "weighted_fit_report.md").read_text(encoding="utf-8")
    assert "replay evidence only" in report_text
    assert "not real acceptance evidence" in report_text
