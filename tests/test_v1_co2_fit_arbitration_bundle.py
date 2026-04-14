from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.tools.build_v1_co2_bootstrap_robustness_audit import main as build_bootstrap_main
from gas_calibrator.tools.build_v1_co2_fit_arbitration_bundle import main as build_arbitration_main
from gas_calibrator.tools.build_v1_co2_fit_stability_audit import main as build_stability_main
from gas_calibrator.tools.build_v1_co2_weighted_fit_advisory import main as build_weighted_main
from gas_calibrator.workflow.co2_bootstrap_robustness_audit import build_co2_bootstrap_robustness_audit
from gas_calibrator.workflow.co2_fit_arbitration_bundle import build_co2_fit_arbitration_bundle
from gas_calibrator.workflow.co2_fit_stability_audit import build_co2_fit_stability_audit
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
        _candidate_row(title="clean-400", point_no=1, target=400.0, temp_c=20.0, pressure_label="ambient", measured=400.0, suitability="fit", recommended=True, hard_blocked=False, weight=1.0, evidence_score=100.0),
        _candidate_row(title="clean-500", point_no=2, target=500.0, temp_c=20.0, pressure_label="ambient", measured=500.0, suitability="fit", recommended=True, hard_blocked=False, weight=0.95, evidence_score=97.0),
        _candidate_row(title="fallback-500", point_no=3, target=500.0, temp_c=20.0, pressure_label="ambient", measured=560.0, suitability="advisory", recommended=True, hard_blocked=False, weight=0.35, evidence_score=58.0, measured_source="co2_trailing_window_fallback", switch_reason="source_fallback", temporal_status="warn"),
        _candidate_row(title="clean-800", point_no=4, target=800.0, temp_c=20.0, pressure_label="ambient", measured=800.0, suitability="fit", recommended=True, hard_blocked=False, weight=0.92, evidence_score=96.0),
        _candidate_row(title="clean-1000", point_no=5, target=1000.0, temp_c=20.0, pressure_label="ambient", measured=1000.0, suitability="fit", recommended=True, hard_blocked=False, weight=0.9, evidence_score=95.0),
        _candidate_row(title="blocked-no-source", point_no=6, target=700.0, temp_c=20.0, pressure_label="ambient", measured=710.0, suitability="unfit", recommended=False, hard_blocked=True, weight=0.0, evidence_score=0.0, measured_source="co2_no_trusted_source"),
        _candidate_row(title="blocked-temporal", point_no=7, target=600.0, temp_c=20.0, pressure_label="ambient", measured=610.0, suitability="unfit", recommended=False, hard_blocked=True, weight=0.0, evidence_score=0.0, measured_source="co2_no_trusted_source", temporal_status="fail", temporal_reason="timestamp_rollback_detected"),
    ]


def test_fit_arbitration_distinguishes_score_stability_and_balanced_choice() -> None:
    rows = _dataset()
    weighted_payload = build_co2_weighted_fit_advisory(rows)
    fit_stability_payload = build_co2_fit_stability_audit(rows)
    bootstrap_payload = build_co2_bootstrap_robustness_audit(rows, fit_stability_payload=fit_stability_payload)

    payload = build_co2_fit_arbitration_bundle(
        rows,
        weighted_fit_payload=weighted_payload,
        fit_stability_payload=fit_stability_payload,
        bootstrap_payload=bootstrap_payload,
    )

    summary = payload["summary"]
    assert summary["best_by_score"] == "baseline_unweighted_fit_only"
    assert summary["best_by_stability"] == "baseline_unweighted_fit_only"
    assert summary["best_balanced_choice"] == "baseline_unweighted_fit_only"
    assert summary["recommended_release_candidate"] == "baseline_unweighted_fit_only"
    assert summary["manual_review_required"] is False


def test_fallback_point_is_not_globally_killed_by_arbitration() -> None:
    payload = build_co2_fit_arbitration_bundle(_dataset())
    variants = {row["fit_variant_name"]: row for row in payload["variants"]}

    assert variants["weighted_fit_advisory"]["best_by_score"] is False
    assert variants["weighted_fit_advisory"]["candidate_point_count"] == 4
    assert variants["weighted_fit_advisory"]["fit_variant_status"] == "available"
    assert variants["weighted_fit_advisory"]["weighted_fit"] is True


def test_untrusted_points_do_not_enter_recommended_bundle() -> None:
    payload = build_co2_fit_arbitration_bundle(_dataset())
    bundle = payload["recommended_coefficient_bundle"]

    assert bundle["fit_variant_name"] == "baseline_unweighted_fit_only"
    assert bundle["bundle_status"] == "advisory_only"
    assert bundle["candidate_point_count"] == 4
    assert bundle["manual_review_required"] is False


def test_arbitration_tool_writes_expected_artifacts(tmp_path: Path) -> None:
    candidate_pack_json = tmp_path / "candidate_pack.json"
    weighted_dir = tmp_path / "weighted"
    stability_dir = tmp_path / "stability"
    bootstrap_dir = tmp_path / "bootstrap"
    arbitration_dir = tmp_path / "arbitration"
    candidate_pack_json.write_text(
        json.dumps({"summary": {"point_count_total": 7}, "points": _dataset(), "groups": []}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    assert build_weighted_main(["--candidate-pack-json", str(candidate_pack_json), "--output-dir", str(weighted_dir)]) == 0
    assert build_stability_main(["--weighted-fit-summary-json", str(weighted_dir / "weighted_fit_summary.json"), "--output-dir", str(stability_dir)]) == 0
    assert build_bootstrap_main([
        "--weighted-fit-summary-json", str(weighted_dir / "weighted_fit_summary.json"),
        "--fit-stability-summary-json", str(stability_dir / "fit_stability_summary.json"),
        "--output-dir", str(bootstrap_dir),
    ]) == 0

    assert build_arbitration_main([
        "--bootstrap-summary-json", str(bootstrap_dir / "bootstrap_fit_summary.json"),
        "--fit-stability-summary-json", str(stability_dir / "fit_stability_summary.json"),
        "--weighted-fit-summary-json", str(weighted_dir / "weighted_fit_summary.json"),
        "--output-dir", str(arbitration_dir),
    ]) == 0

    assert (arbitration_dir / "fit_arbitration_variants.csv").exists()
    assert (arbitration_dir / "fit_arbitration_summary.csv").exists()
    assert (arbitration_dir / "fit_arbitration_summary.json").exists()
    assert (arbitration_dir / "fit_arbitration_report.md").exists()
    assert (arbitration_dir / "recommended_coefficient_bundle.json").exists()
    assert (arbitration_dir / "recommended_coefficient_bundle.csv").exists()

    payload = json.loads((arbitration_dir / "fit_arbitration_summary.json").read_text(encoding="utf-8"))
    assert payload["summary"]["not_real_acceptance_evidence"] is True
    assert payload["recommended_coefficient_bundle"]["fit_variant_name"] == "baseline_unweighted_fit_only"
    report_text = (arbitration_dir / "fit_arbitration_report.md").read_text(encoding="utf-8")
    assert "replay evidence only" in report_text
    assert "not real acceptance evidence" in report_text
