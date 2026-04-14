from __future__ import annotations

import json
from pathlib import Path

import pytest

from gas_calibrator.tools.build_v1_co2_bootstrap_robustness_audit import main as build_bootstrap_main
from gas_calibrator.tools.build_v1_co2_fit_stability_audit import main as build_stability_main
from gas_calibrator.tools.build_v1_co2_weighted_fit_advisory import main as build_weighted_main
from gas_calibrator.workflow.co2_bootstrap_robustness_audit import (
    build_co2_bootstrap_robustness_audit,
)
from gas_calibrator.workflow.co2_calibration_candidate_pack import _FIELD_ALIASES
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


def test_bootstrap_audit_consumes_weighted_fit_output_without_rejudging() -> None:
    weighted_payload = build_co2_weighted_fit_advisory(_dataset())
    fit_stability_payload = build_co2_fit_stability_audit(_dataset())
    extracted_rows = extract_candidate_rows_from_weighted_fit_payload(weighted_payload)
    payload = build_co2_bootstrap_robustness_audit(
        extracted_rows,
        fit_stability_payload=fit_stability_payload,
        bootstrap_seed=7,
        bootstrap_rounds=12,
        subsample_rounds=8,
        subsample_group_fraction=0.5,
    )

    summary = payload["summary"]
    assert summary["point_count_total"] == 5
    assert summary["evaluation_point_count"] == 5
    assert summary["excluded_point_count"] == 0
    assert summary["not_real_acceptance_evidence"] is True
    assert {row["fit_variant_name"] for row in payload["variant_overall"]} == {
        "baseline_unweighted_all_recommended",
        "baseline_unweighted_fit_only",
        "weighted_fit_advisory",
    }


def test_fallback_point_still_participates_with_lower_weight() -> None:
    payload = build_co2_bootstrap_robustness_audit(
        _dataset(),
        bootstrap_seed=7,
        bootstrap_rounds=8,
        subsample_rounds=8,
        subsample_group_fraction=0.5,
    )
    weighted_points = [
        row
        for row in payload["points"]
        if row["fit_variant_name"] == "weighted_fit_advisory" and row["point_title"] == "fallback-500"
    ]
    assert len(weighted_points) == 1
    assert pytest.approx(float(weighted_points[0]["fit_training_weight"]), abs=1e-9) == 0.35


def test_blocked_points_do_not_enter_weighted_bootstrap_baseline() -> None:
    payload = build_co2_bootstrap_robustness_audit(_dataset(), bootstrap_rounds=6, subsample_rounds=6)
    assert payload["summary"]["excluded_point_count"] == 2
    assert "hard_blocked:2" in payload["summary"]["excluded_reason_summary"]
    weighted_variant = next(
        row for row in payload["variant_overall"] if row["fit_variant_name"] == "weighted_fit_advisory"
    )
    assert weighted_variant["input_point_count"] == 4


def test_grouped_bootstrap_and_repeated_subsample_are_reported() -> None:
    payload = build_co2_bootstrap_robustness_audit(_dataset(), bootstrap_rounds=10, subsample_rounds=10)
    methods = {row["resample_method"] for row in payload["resample_summaries"]}
    assert methods == {"grouped_bootstrap", "group_subsample"}

    subgroup = [
        row
        for row in payload["groups"]
        if row["fit_variant_name"] == "weighted_fit_advisory" and row["resample_method"] == "group_subsample"
    ]
    assert subgroup
    assert all(0.0 <= float(row["inclusion_frequency"]) <= 1.0 for row in subgroup if row["inclusion_frequency"] is not None)
    assert all(int(row["fragile_group_rank"]) >= 1 for row in subgroup)


def test_bootstrap_audit_accepts_chinese_candidate_rows() -> None:
    def zh(field: str) -> str:
        return _FIELD_ALIASES[field][1]

    rows = [
        {
            zh("point_title"): "中文clean",
            zh("point_no"): "1",
            zh("route"): "gas",
            zh("pressure_target_label"): "ambient",
            zh("co2_ppm_target"): "500",
            zh("temp_chamber_c"): "20",
            zh("measured_value"): "500.0",
            zh("measured_value_source"): "co2_steady_state_window",
            zh("co2_point_suitability_status"): "fit",
            zh("co2_calibration_candidate_recommended"): "True",
            zh("co2_calibration_candidate_hard_blocked"): "False",
            zh("co2_calibration_weight_recommended"): "1.0",
            zh("co2_evidence_score"): "100",
        },
        {
            zh("point_title"): "中文fallback",
            zh("point_no"): "2",
            zh("route"): "gas",
            zh("pressure_target_label"): "ambient",
            zh("co2_ppm_target"): "800",
            zh("temp_chamber_c"): "20",
            zh("measured_value"): "810.0",
            zh("measured_value_source"): "co2_trailing_window_fallback",
            zh("co2_point_suitability_status"): "advisory",
            zh("co2_calibration_candidate_recommended"): "True",
            zh("co2_calibration_candidate_hard_blocked"): "False",
            zh("co2_calibration_weight_recommended"): "0.5",
            zh("co2_evidence_score"): "60",
        },
    ]

    payload = build_co2_bootstrap_robustness_audit(rows, bootstrap_rounds=4, subsample_rounds=4)

    assert payload["summary"]["evaluation_point_count"] == 2
    assert payload["summary"]["not_real_acceptance_evidence"] is True


def test_bootstrap_tool_writes_expected_artifacts_from_weighted_and_stability_jsons(tmp_path: Path) -> None:
    candidate_pack_json = tmp_path / "candidate_pack.json"
    weighted_output_dir = tmp_path / "weighted_out"
    stability_output_dir = tmp_path / "stability_out"
    bootstrap_output_dir = tmp_path / "bootstrap_out"
    candidate_pack = {
        "summary": {"point_count_total": 7},
        "points": _dataset(),
        "groups": [],
    }
    candidate_pack_json.write_text(json.dumps(candidate_pack, ensure_ascii=False, indent=2), encoding="utf-8")

    assert build_weighted_main(["--candidate-pack-json", str(candidate_pack_json), "--output-dir", str(weighted_output_dir)]) == 0
    weighted_summary_json = weighted_output_dir / "weighted_fit_summary.json"
    assert build_stability_main(["--weighted-fit-summary-json", str(weighted_summary_json), "--output-dir", str(stability_output_dir)]) == 0
    stability_summary_json = stability_output_dir / "fit_stability_summary.json"

    assert (
        build_bootstrap_main(
            [
                "--weighted-fit-summary-json",
                str(weighted_summary_json),
                "--fit-stability-summary-json",
                str(stability_summary_json),
                "--output-dir",
                str(bootstrap_output_dir),
                "--bootstrap-rounds",
                "12",
                "--subsample-rounds",
                "8",
                "--subsample-group-fraction",
                "0.5",
            ]
        )
        == 0
    )

    assert (bootstrap_output_dir / "bootstrap_fit_points.csv").exists()
    assert (bootstrap_output_dir / "bootstrap_fit_groups.csv").exists()
    assert (bootstrap_output_dir / "bootstrap_coefficients.csv").exists()
    assert (bootstrap_output_dir / "bootstrap_fit_summary.csv").exists()
    assert (bootstrap_output_dir / "bootstrap_fit_summary.json").exists()
    assert (bootstrap_output_dir / "bootstrap_fit_report.md").exists()

    summary_payload = json.loads((bootstrap_output_dir / "bootstrap_fit_summary.json").read_text(encoding="utf-8"))
    assert summary_payload["summary"]["not_real_acceptance_evidence"] is True
    assert summary_payload["summary"]["bootstrap_rounds"] == 12
    assert summary_payload["summary"]["subsample_rounds"] == 8

    report_text = (bootstrap_output_dir / "bootstrap_fit_report.md").read_text(encoding="utf-8")
    assert "replay evidence only" in report_text
    assert "not real acceptance evidence" in report_text
