import csv
import json

from gas_calibrator.tools.export_v1_5_candidate_coefficients import main as candidate_cli
from gas_calibrator.validation.formal_candidate_coefficients import (
    CandidateCoefficientPolicyConfig,
    _detect_common_mode_fit_target_outliers,
    build_candidate_coefficient_tables,
    write_candidate_coefficient_report,
)
from gas_calibrator.validation.formal_calibration_package import build_formal_calibration_package_tables
from gas_calibrator.validation.pressure_channel import write_pressure_quick_check_csv


def _plan():
    return {
        "plan_id": "v1_5_candidate_policy_demo",
        "plan_version": "2026-05-25",
        "config_hash": "config-hash",
        "operator": "operator-a",
        "standard_gases": [
            {
                "component": "co2",
                "cylinder_id": "CO2-900",
                "certificate_value": 900.0,
                "certificate_uncertainty": 0.9,
                "valid_until": "2027-01-01",
                "supplier": "standard-lab",
                "certificate_hash": "co2-cert-hash",
            },
            {
                "component": "h2o",
                "cylinder_id": "H2O-001",
                "certificate_value": 0.5,
                "certificate_uncertainty": 0.01,
                "valid_until": "2027-01-01",
                "supplier": "standard-lab",
                "certificate_hash": "h2o-cert-hash",
            },
        ],
    }


def _pressure_reference():
    return {
        "device_id": "118288",
        "certificate_id": "FRGsz25038057",
        "certificate_uncertainty": 0.55,
        "valid_until": "2027-05-21",
        "certificate_hash": "pressure-cert-hash",
        "unit": "hPa",
    }


def _co2_row(
    index: int,
    target: float,
    *,
    role: str = "fit",
    point_tag: str = "",
    pressure_mode: str = "ambient_open",
    zero_certified: bool = True,
):
    ratio = 1.0 + target / 1000.0 + (index % 10) * 0.000001
    row = {
        "sample_index": index,
        "sample_ts": f"2026-05-25T12:00:{index % 60:02d}",
        "point_phase": "co2",
        "route": "co2",
        "point_tag": point_tag or f"co2_{target:g}",
        "sample_role": role,
        "pressure_mode": pressure_mode,
        "target_co2_ppm": target,
        "pressure_gauge_hpa": 1000.5 + (index % 5) * 0.01,
        "controller_pressure": 1000.6,
        "pressure_atmosphere_hold_status": "verified",
        "pressure_atmosphere_hold_active": "true",
        "pressure_atmosphere_hold_strategy": "legacy_hold_thread",
        "dewpoint_c": -30.0 + (index % 5) * 0.001,
        "ga01_frame_usable": "true",
        "ga01_mode2_contract_status": "pass",
        "ga01_mode2_qc_status": "pass",
        "ga01_mode2_tokens_json": json.dumps(
            ["YGAS", "023", f"{target:08.3f}", "00.500", "1768.000", "00.410"],
            separators=(",", ":"),
        ),
        "ga01_raw": "YGAS,023,...",
        "ga01_ref_signal": 3322.0,
        "ga01_co2_signal": 4356.0,
        "ga01_h2o_signal": 2631.0,
        "ga01_chamber_temp_c": 25.0 + (index % 5) * 0.001,
        "ga01_case_temp_c": 25.5,
        "ga01_pressure_kpa": 100.05 + (index % 5) * 0.001,
        "ga01_co2_ratio_f": ratio,
        "ga01_co2_ppm": target + 0.02,
        "ga01_h2o_ratio_f": 0.7,
        "ga01_h2o_mmol": 0.5,
    }
    if target == 0.0 and zero_certified:
        row["co2_zero_certified"] = "true"
    return row


def _h2o_row(index: int, target: float, *, role: str = "fit", point_tag: str = "", pressure_mode: str = "ambient_open"):
    ratio = 0.5 + target * 0.1 + (index % 10) * 0.00001
    return {
        "sample_index": index,
        "sample_ts": f"2026-05-25T12:01:{index % 60:02d}",
        "point_phase": "h2o",
        "route": "h2o",
        "point_tag": point_tag or f"h2o_{target:g}",
        "sample_role": role,
        "pressure_mode": pressure_mode,
        "target_h2o_mmol": target,
        "pressure_gauge_hpa": 1000.5 + (index % 5) * 0.01,
        "controller_pressure": 1000.6,
        "pressure_atmosphere_hold_status": "verified",
        "pressure_atmosphere_hold_active": "true",
        "pressure_atmosphere_hold_strategy": "legacy_hold_thread",
        "dewpoint_c": -20.0 + (index % 5) * 0.001,
        "ga01_frame_usable": "true",
        "ga01_mode2_contract_status": "pass",
        "ga01_mode2_qc_status": "pass",
        "ga01_mode2_tokens_json": json.dumps(
            ["YGAS", "023", "0000.000", f"{target:06.3f}", "0000.000", "00.000"],
            separators=(",", ":"),
        ),
        "ga01_raw": "YGAS,023,...",
        "ga01_ref_signal": 3322.0,
        "ga01_co2_signal": 4356.0,
        "ga01_h2o_signal": 2631.0,
        "ga01_chamber_temp_c": 25.0 + (index % 5) * 0.001,
        "ga01_case_temp_c": 25.5,
        "ga01_pressure_kpa": 100.05 + (index % 5) * 0.001,
        "ga01_co2_ratio_f": 1.0,
        "ga01_co2_ppm": 0.02,
        "ga01_h2o_ratio_f": ratio,
        "ga01_h2o_mmol": target + 0.001,
    }


def _write_csv(path, rows):
    keys = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def _co2_fit_source_row(index: int, target: float, ratio: float, *, prefix: str):
    return {
        "sample_index": index,
        "point_phase": "co2",
        "route": "co2",
        "point_tag": f"T20_{target:g}ppm",
        "sample_role": "fit",
        "target_co2_ppm": target,
        "pressure_gauge_hpa": 1013.25,
        "dewpoint_c": -35.0,
        f"{prefix}_co2_ratio_f": ratio,
        f"{prefix}_chamber_temp_c": 25.0,
        f"{prefix}_pressure_kpa": 101.325,
    }


def _read_csv(path):
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _make_run(tmp_path, rows):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _write_csv(run_dir / "samples_20260525.csv", rows)
    write_pressure_quick_check_csv(run_dir, rows[:10], run_id="20260525")
    plan_path = tmp_path / "formal_plan.json"
    plan_path.write_text(json.dumps(_plan(), ensure_ascii=False), encoding="utf-8")
    pressure_reference_path = tmp_path / "pressure_reference.json"
    pressure_reference_path.write_text(json.dumps(_pressure_reference(), ensure_ascii=False), encoding="utf-8")
    return run_dir, plan_path, pressure_reference_path


def _write_fit_input_quality(
    root,
    *,
    devices=("023",),
    components=("co2", "h2o"),
    rejected=(),
    summary_status="pass",
):
    summary_path = root / "v1_5_fit_input_quality_summary.csv"
    devices_path = root / "v1_5_fit_input_quality_devices.csv"
    _write_csv(
        summary_path,
        [
            {
                "run_status": summary_status,
                "fit_input_continuity_gate_status": "pass" if summary_status == "pass" else "blocked",
                "opens_com_ports": "False",
                "controls_water_or_gas_routes": "False",
                "writes_coefficients": "False",
            }
        ],
    )
    rejected_set = {(str(component), str(device)) for component, device in rejected}
    _write_csv(
        devices_path,
        [
            {
                "component": component,
                "device_id": device,
                "fit_input_grade": "REJECT" if (component, device) in rejected_set else "A",
                "fit_input_status": (
                    "excluded_from_candidate_fit"
                    if (component, device) in rejected_set
                    else "usable_for_candidate_fit"
                ),
                "reject_reasons": (
                    "fit_input_continuity_gate_not_ready:segmented_route_evidence"
                    if (component, device) in rejected_set
                    else ""
                ),
            }
            for device in devices
            for component in components
        ],
    )
    return summary_path, devices_path


def _co2_wide_row(index: int, target: float) -> dict:
    row = {
        "sample_index": index,
        "point_phase": "co2",
        "route": "co2",
        "point_id": f"p{index:03d}_co2_{target:g}",
        "point_tag": f"co2_{target:g}",
        "sample_role": "fit",
        "target_co2_ppm": target,
        "co2_zero_certified": "true" if target == 0.0 else "",
        "pressure_gauge_hpa": 1009.5,
        "dewpoint_live_c": -42.0,
    }
    for prefix, device_id, offset in (("ga01", "079", 0.0), ("ga02", "091", 0.001)):
        ratio = 1.0 + target / 1000.0 + offset + (index % 3) * 0.000001
        row.update(
            {
                f"{prefix}_analyzer_device_id": device_id,
                f"{prefix}_raw": f"YGAS,{device_id},{target:08.3f},00.000,0000.000,00.000",
                f"{prefix}_frame_usable": "true",
                f"{prefix}_mode2_contract_status": "pass",
                f"{prefix}_mode2_qc_status": "pass",
                f"{prefix}_mode2_tokens_json": json.dumps(
                    [
                        "YGAS",
                        device_id,
                        f"{target:08.3f}",
                        "00.000",
                        "0000.000",
                        "00.000",
                        f"{ratio:.6f}",
                        f"{ratio:.6f}",
                        "0.6000",
                        "0.6000",
                        "03000",
                        "04000",
                        "02000",
                        "25.00",
                        "25.00",
                        "100.95",
                    ],
                    separators=(",", ":"),
                ),
                f"{prefix}_co2_ratio_f": ratio,
                f"{prefix}_co2_ppm": target,
                f"{prefix}_h2o_ratio_f": 0.6,
                f"{prefix}_h2o_mmol": 0.0,
                f"{prefix}_ref_signal": 3000.0,
                f"{prefix}_co2_signal": 4000.0,
                f"{prefix}_h2o_signal": 2000.0,
                f"{prefix}_chamber_temp_c": 25.0,
                f"{prefix}_case_temp_c": 25.0,
                f"{prefix}_pressure_kpa": 100.95,
            }
        )
    return row


def test_review_only_wide_sample_fallback_expands_all_detected_analyzers(tmp_path):
    run_dir = tmp_path / "wide_run"
    run_dir.mkdir()
    rows = [_co2_wide_row(index, 0.0 if index <= 10 else 900.0) for index in range(1, 21)]
    for row in rows:
        row["pressure_mode"] = "closed_diagnostic_replay"
    _write_csv(run_dir / "samples_machine_readable.csv", rows)

    tables, context = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan={},
        pressure_reference={},
        component="co2",
        analyzer_prefix="all",
        require_quick_check_artifact=False,
        cfg=CandidateCoefficientPolicyConfig(review_only_wide_sample_fallback=True),
        today="2026-06-14",
    )

    policies = {
        (row["analyzer_prefix"], row["analyzer_device_id"]): row
        for row in tables["candidate_policy_summary"]
        if row["analyzer_device_id"] in {"079", "091"}
    }
    assert set(policies) == {("ga01", "079"), ("ga02", "091")}
    assert context["review_only_wide_sample_fallback"] is True
    assert context["review_only_fallback_source"].endswith("samples_machine_readable.csv")
    for row in policies.values():
        assert row["allowed_to_fit"] is True
        assert row["candidate_sample_source"] == "review_only_wide_sample_fallback"
        assert row["fit_sample_count"] == 20
        assert row["distinct_fit_targets"] == 2
        assert "review_only_wide_sample_fallback_used_formal_package_not_write_ready" in row[
            "warning_reasons"
        ]
        assert row["auto_write_allowed"] is False
    assert len(tables["candidate_coefficients"]) == 4


def test_candidate_fit_residual_quality_gate_blocks_large_relative_error(tmp_path):
    rows = []
    rows.extend(_co2_row(index, 100.0, point_tag="co2_100") for index in range(1, 5))
    rows.extend(_co2_row(index, 200.0, point_tag="co2_200") for index in range(5, 9))
    rows.extend(_co2_row(index, 300.0, point_tag="co2_300") for index in range(9, 12))
    # The duplicated target with a conflicting ratio is mathematically fit-able
    # only with a visible residual; the quality gate keeps it out of write review.
    conflicting_row = _co2_row(12, 300.0, point_tag="co2_300_conflict")
    conflicting_row["ga01_co2_ratio_f"] = rows[0]["ga01_co2_ratio_f"]
    rows.append(conflicting_row)
    run_dir, _, _ = _make_run(tmp_path, rows)

    tables, context = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="ga01",
        cfg=CandidateCoefficientPolicyConfig(
            min_fit_samples=1,
            max_fit_absolute_relative_error_pct_for_review={"co2": 0.01, "h2o": 10.0}
        ),
        today="2026-06-14",
    )

    policy = tables["candidate_policy_summary"][0]
    assert context["candidate_run_status"] == "fit_ready_requires_verification"
    assert policy["candidate_status"] == "blocked"
    assert policy["allowed_for_review"] is False
    assert "fit_max_absolute_relative_error_pct>0.01" in policy["blocked_reasons"]


def test_common_mode_source_outlier_is_detected_without_bending_response_curve():
    targets = [100.0, 200.0, 400.0, 500.0, 600.0, 700.0, 800.0, 900.0, 1000.0]
    keys = []
    groups = {}
    review = {}
    for device_index, (prefix, device_id) in enumerate(
        [("ga01", "030"), ("ga02", "022"), ("ga03", "033"), ("ga04", "051")]
    ):
        key = ("co2", prefix, device_id)
        keys.append(key)
        review[key] = {"candidate_review_status": "ready_for_reviewer"}
        rows = []
        for point_index, target in enumerate(targets, start=1):
            # The 600 ppm source behaves as if the true gas were about 620 ppm
            # on every independent analyzer. That is source/route evidence, not
            # a physical reason to warp the analyzer response curve.
            effective_target = 620.0 if target == 600.0 else target
            ratio = 1.5 - effective_target / 2000.0 + device_index * 0.0001
            rows.append(_co2_fit_source_row(point_index, target, ratio, prefix=prefix))
        groups[key] = rows

    exclusions, diagnostics = _detect_common_mode_fit_target_outliers(
        keys=keys,
        groups=groups,
        review_by_key=review,
        plan={"standard_gases": []},
        cfg=CandidateCoefficientPolicyConfig(min_fit_samples=1),
    )

    assert all(exclusions[key] == [600.0] for key in keys)
    aggregate = [row for row in diagnostics if not row.get("analyzer_device_id")]
    assert aggregate[0]["target_key"] == 600.0
    assert aggregate[0]["device_count"] == 4
    assert aggregate[0]["auto_excluded_from_fit"] is True
    assert aggregate[0]["physical_interpretation"] == "suspect_standard_gas_or_route_state_not_analyzer_response"


def test_candidate_policy_blocks_single_target_and_freezes_pressure_temperature_terms(tmp_path):
    rows = [_co2_row(index, 900.0, point_tag="co2_900") for index in range(1, 11)]
    run_dir, _, _ = _make_run(tmp_path, rows)

    tables, context = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="ga01",
        today="2026-05-25",
    )
    policy = tables["candidate_policy_summary"][0]

    assert context["candidate_run_status"] == "blocked"
    assert policy["candidate_status"] == "blocked"
    assert "distinct_fit_targets<2" in policy["blocked_reasons"]
    assert policy["evidence_reuse_class"] == "a_grade_single_target_review_only"
    assert "do_not_block_other_analyzers" in policy["per_analyzer_reuse_boundary"]
    assert "P" in policy["frozen_terms"]
    assert "RTP" in policy["frozen_terms"]
    assert "T" in policy["frozen_terms"]
    assert tables["candidate_coefficients"] == []


def test_candidate_policy_surfaces_formal_review_blockers(tmp_path):
    rows = [_co2_row(index, 900.0, point_tag="co2_900") for index in range(1, 11)]
    for row in rows:
        row["ga01_pressure_kpa"] = 102.0
    run_dir, _, _ = _make_run(tmp_path, rows)

    tables, context = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="ga01",
        today="2026-05-25",
    )
    policy = tables["candidate_policy_summary"][0]

    assert context["candidate_run_status"] == "blocked"
    assert policy["candidate_status"] == "blocked"
    assert "formal_candidate_review_not_ready" in policy["blocked_reasons"]
    assert "pressure_channel_quick_check_not_passed" in policy["formal_review_blockers"]
    assert policy["formal_pressure_validation_status"] == "fail"


def test_candidate_export_fits_all_a_grade_open_flow_rows_and_requires_independent_verification(tmp_path):
    rows = []
    rows.extend(_co2_row(index, 0.0, point_tag="co2_0") for index in range(1, 11))
    rows.extend(_co2_row(index, 900.0, point_tag="co2_900") for index in range(11, 21))
    rows.extend(_co2_row(index, 450.0, role="verification", point_tag="co2_450_verify") for index in range(21, 31))
    rows.extend(
        _co2_row(index, 700.0, point_tag="sealed_diagnostic", pressure_mode="sealed_controlled")
        for index in range(31, 36)
    )
    run_dir, _, _ = _make_run(tmp_path, rows)

    formal_tables, _ = build_formal_calibration_package_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="ga01",
        today="2026-05-25",
    )
    candidate_tables, context = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="ga01",
        today="2026-05-25",
    )
    policy = candidate_tables["candidate_policy_summary"][0]

    assert context["candidate_run_status"] == "fit_ready_requires_verification"
    assert policy["candidate_status"] == "fit_ready_requires_verification"
    assert policy["fit_sample_count"] == 30
    assert policy["verification_sample_count"] == 0
    assert policy["fit_point_count"] == 3
    assert policy["verification_point_count"] == 0
    assert policy["source_verification_reused_for_fit_count"] == 10
    assert "source_verification_samples_reused_for_fit_requires_new_independent_verification" in policy["warning_reasons"]
    assert "verification_samples_missing" in policy["verification_reasons"]
    assert policy["selected_model_terms"] == "intercept;R;R2"
    assert policy["auto_write_allowed"] is False
    assert {row["term"] for row in candidate_tables["candidate_coefficients"]} == {"intercept", "R", "R2"}
    assert all(row["residual_role"] == "fit" for row in candidate_tables["candidate_fit_residuals"])
    assert len(candidate_tables["candidate_fit_residuals"]) == 3
    assert len(candidate_tables["candidate_verification_residuals"]) == 0
    assert "sealed_controlled" in ";".join(row.get("formal_reject_reasons", "") for row in formal_tables["rejected_samples"])
    assert all(row["target_value"] != 700.0 for row in candidate_tables["candidate_fit_residuals"])
    assert any(row["target_value"] == 450.0 for row in candidate_tables["candidate_fit_residuals"])


def test_candidate_export_blocks_when_required_fit_input_quality_missing(tmp_path):
    rows = []
    rows.extend(_co2_row(index, 0.0, point_tag="co2_0") for index in range(1, 11))
    rows.extend(_co2_row(index, 900.0, point_tag="co2_900") for index in range(11, 21))
    rows.extend(_co2_row(index, 450.0, role="verification", point_tag="co2_450_verify") for index in range(21, 31))
    run_dir, _, _ = _make_run(tmp_path, rows)

    tables, context = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="ga01",
        cfg=CandidateCoefficientPolicyConfig(require_fit_input_quality=True),
        today="2026-05-25",
    )

    policy = tables["candidate_policy_summary"][0]
    assert context["candidate_run_status"] == "blocked"
    assert context["fit_input_quality_gate_status"] == "blocked"
    assert policy["candidate_status"] == "blocked"
    assert policy["allowed_to_fit"] is False
    assert policy["allowed_for_review"] is False
    assert "fit_input_quality_summary_csv_missing" in policy["blocked_reasons"]
    assert tables["candidate_coefficients"] == []


def test_candidate_export_blocks_only_rejected_fit_input_device_component(tmp_path):
    rows = []
    rows.extend(_co2_row(index, 0.0, point_tag="co2_0") for index in range(1, 11))
    rows.extend(_co2_row(index, 900.0, point_tag="co2_900") for index in range(11, 21))
    rows.extend(_co2_row(index, 450.0, role="verification", point_tag="co2_450_verify") for index in range(21, 31))
    run_dir, _, _ = _make_run(tmp_path, rows)
    summary_path, devices_path = _write_fit_input_quality(
        tmp_path,
        components=("co2",),
        rejected=(("co2", "023"),),
    )

    tables, context = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="ga01",
        cfg=CandidateCoefficientPolicyConfig(
            fit_input_quality_summary_csv=summary_path,
            fit_input_quality_devices_csv=devices_path,
            require_fit_input_quality=True,
        ),
        today="2026-05-25",
    )

    policy = tables["candidate_policy_summary"][0]
    assert context["fit_input_quality_gate_status"] == "pass"
    assert policy["candidate_status"] == "blocked"
    assert policy["fit_input_quality_grade"] == "REJECT"
    assert "fit_input_quality_rejected:co2:" in policy["blocked_reasons"]
    assert tables["candidate_coefficients"] == []


def test_candidate_export_blocks_write_candidate_when_factory_signal_health_blocks_device(tmp_path):
    rows = []
    rows.extend(_co2_row(index, 0.0, point_tag="co2_0") for index in range(1, 11))
    rows.extend(_co2_row(index, 450.0, point_tag="co2_450") for index in range(11, 21))
    rows.extend(_co2_row(index, 900.0, point_tag="co2_900") for index in range(21, 31))
    run_dir, _, _ = _make_run(tmp_path, rows)
    factory_summary = tmp_path / "factory_signal_health_summary.csv"
    _write_csv(
        factory_summary,
        [
            {
                "device_id": "023",
                "point_count": 9,
                "review_point_count": 3,
                "blocking_point_count": 1,
                "high_ref_point_count": 1,
                "max_abs_model_error": 42.0,
                "max_relative_model_error_pct": 8.0,
                "candidate_gate": "block_optical_reference_health_review",
            }
        ],
    )

    tables, context = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="ga01",
        cfg=CandidateCoefficientPolicyConfig(factory_signal_health_summary_csv=factory_summary),
        today="2026-05-25",
    )

    policy = tables["candidate_policy_summary"][0]
    assert context["candidate_run_status"] == "blocked"
    assert policy["candidate_status"] == "blocked"
    assert policy["allowed_to_fit"] is False
    assert policy["factory_signal_health_gate"] == "block_optical_reference_health_review"
    assert "factory_signal_health_block:block_optical_reference_health_review" in policy["blocked_reasons"]
    assert tables["candidate_coefficients"] == []


def test_co2_candidate_rejects_uncertified_zero_gas_anchor(tmp_path):
    rows = []
    rows.extend(_co2_row(index, 0.0, point_tag="co2_0", zero_certified=False) for index in range(1, 11))
    rows.extend(_co2_row(index, 900.0, point_tag="co2_900") for index in range(11, 21))
    rows.extend(_co2_row(index, 450.0, role="verification", point_tag="co2_450_verify") for index in range(21, 31))
    run_dir, _, _ = _make_run(tmp_path, rows)

    candidate_tables, context = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="ga01",
        cfg=CandidateCoefficientPolicyConfig(fit_all_eligible_samples=False),
        today="2026-05-25",
    )
    policy = candidate_tables["candidate_policy_summary"][0]
    rejected = candidate_tables["candidate_preparation_rejected_samples"]

    assert context["candidate_run_status"] == "blocked"
    assert policy["candidate_status"] == "blocked"
    assert "distinct_fit_targets<2" in policy["blocked_reasons"]
    assert policy["fit_sample_count"] == 10
    assert len(rejected) == 10
    assert all("co2_zero_anchor_uncertified" in row["reject_reasons"] for row in rejected)
    assert candidate_tables["candidate_coefficients"] == []


def test_candidate_export_can_reuse_complete_verification_rows_for_fit_but_requires_new_verification(tmp_path):
    rows = []
    rows.extend(_co2_row(index, 0.0, point_tag="co2_0") for index in range(1, 11))
    rows.extend(_co2_row(index, 900.0, point_tag="co2_900") for index in range(11, 21))
    rows.extend(_co2_row(index, 450.0, role="verification", point_tag="co2_450_verify") for index in range(21, 31))
    rows.extend(_co2_row(index, 700.0, role="verification", point_tag="co2_700_verify") for index in range(31, 41))
    run_dir, _, _ = _make_run(tmp_path, rows)

    candidate_tables, context = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="ga01",
        cfg=CandidateCoefficientPolicyConfig(fit_all_eligible_samples=True),
        today="2026-05-25",
    )
    policy = candidate_tables["candidate_policy_summary"][0]

    assert context["candidate_run_status"] == "fit_ready_requires_verification"
    assert policy["candidate_status"] == "fit_ready_requires_verification"
    assert policy["fit_all_eligible_samples"] is True
    assert policy["fit_sample_count"] == 40
    assert policy["verification_sample_count"] == 0
    assert policy["fit_point_count"] == 4
    assert policy["verification_point_count"] == 0
    assert policy["source_verification_reused_for_fit_count"] == 20
    assert policy["verification_status"] == "missing"
    assert "verification_samples_missing" in policy["verification_reasons"]
    assert "source_verification_samples_reused_for_fit_requires_new_independent_verification" in policy["warning_reasons"]
    assert policy["allowed_for_review"] is False
    assert {row["target_value"] for row in candidate_tables["candidate_fit_residuals"]} == {0.0, 450.0, 700.0, 900.0}
    assert candidate_tables["candidate_verification_residuals"] == []
    assert all(row["requires_verification"] is True for row in candidate_tables["candidate_coefficients"])


def test_co2_multitemperature_candidate_includes_t2_and_freezes_pressure_terms(tmp_path):
    rows = []
    index = 1
    for temp_c in (0.0, 20.0, 40.0):
        for target in (0.0, 300.0, 600.0, 900.0):
            for _ in range(10):
                row = _co2_row(index, target, point_tag=f"co2_T{temp_c:g}_{target:g}")
                row["ga01_chamber_temp_c"] = temp_c + (index % 5) * 0.001
                rows.append(row)
                index += 1
    run_dir, _, _ = _make_run(tmp_path, rows)

    tables, context = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="ga01",
        cfg=CandidateCoefficientPolicyConfig(
            allow_temperature_terms=True,
            fit_all_eligible_samples=True,
        ),
        today="2026-05-25",
    )

    policy = tables["candidate_policy_summary"][0]
    assert context["candidate_run_status"] == "fit_ready_requires_verification"
    assert policy["selected_model_terms"] == "intercept;R;R2;R3;T;T2;RT"
    assert policy["fit_basis"] == "centered_R_T_transformed_to_firmware_absolute_terms"
    assert float(policy["matrix_condition_number"]) < float(policy["absolute_matrix_condition_number"])
    assert "P" in policy["frozen_terms"]
    assert "RTP" in policy["frozen_terms"]
    assert policy["fit_point_count"] == 12
    assert {row["term"] for row in tables["candidate_coefficients"]} == {
        "intercept",
        "R",
        "R2",
        "R3",
        "T",
        "T2",
        "RT",
    }


def test_co2_candidate_fits_senco1_after_subtracting_preserved_senco3(tmp_path):
    rows = []
    index = 1
    for target in (100.0, 300.0, 700.0, 900.0):
        for _ in range(10):
            row = _co2_row(index, target, point_tag=f"co2_T20_{target:g}")
            row["ga01_chamber_temp_c"] = 22.0 + (index % 4) * 0.01
            row["ga01_co2_ratio_f"] = 1.55 - target / 1800.0 + (index % 5) * 0.00001
            rows.append(row)
            index += 1
    run_dir, _, _ = _make_run(tmp_path, rows)

    tables, context = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="ga01",
        cfg=CandidateCoefficientPolicyConfig(
            fit_all_eligible_samples=True,
            preserved_secondary_coefficients={
                "023": {"GETCO3_before_live": [10.0, 0.0, -5.0, 0.0, 0.0, 0.0]}
            },
            preserved_secondary_coefficients_source="unit-test-getco3-snapshot",
        ),
        today="2026-05-25",
    )

    policy = tables["candidate_policy_summary"][0]
    residuals = tables["candidate_fit_residuals"]
    coefficients = {row["term"]: float(row["coefficient"]) for row in tables["candidate_coefficients"]}

    assert context["candidate_run_status"] == "fit_ready_requires_verification"
    assert (
        policy["primary_fit_target_contract"]
        == "certificate_target_back_calculated_to_raw_minus_preserved_secondary_compensation"
    )
    assert policy["preserved_secondary_channel"] == "SENCO3"
    assert policy["preserved_secondary_coefficients_source"] == "unit-test-getco3-snapshot"
    assert "primary_fit_target_adjusted_for_preserved_secondary_temperature_compensation" in policy["warning_reasons"]
    assert "co2_fit_target_back_calculated_to_raw_senco13_before_h2o_dry_correction" in policy["warning_reasons"]
    assert max(abs(float(row["error"])) for row in residuals) < 1.0e-6
    assert max(abs(float(row["preserved_secondary_compensation"])) for row in residuals) > 100.0
    direct_tables, _ = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="ga01",
        cfg=CandidateCoefficientPolicyConfig(fit_all_eligible_samples=True),
        today="2026-05-25",
    )
    direct_coefficients = {
        row["term"]: float(row["coefficient"]) for row in direct_tables["candidate_coefficients"]
    }
    # A direct SENCO1 fit would hide the preserved SENCO3 layer in the primary
    # curve. The corrected contract keeps SENCO1 on the target-minus-SENCO3
    # raw layer instead.
    assert abs(coefficients["intercept"] - direct_coefficients["intercept"]) > 100.0


def test_co2_candidate_uses_reference_h2o_for_dry_correction_by_default(tmp_path):
    rows = []
    index = 1
    for target in (100.0, 300.0, 700.0, 900.0):
        for _ in range(10):
            row = _co2_row(index, target, point_tag=f"co2_T20_{target:g}")
            row["ga01_h2o_mmol"] = 80.0
            row["dewpoint_c"] = -30.0
            rows.append(row)
            index += 1
    run_dir, _, _ = _make_run(tmp_path, rows)

    tables, _ = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="ga01",
        cfg=CandidateCoefficientPolicyConfig(fit_all_eligible_samples=True),
        today="2026-05-25",
    )

    policy = tables["candidate_policy_summary"][0]
    residual = tables["candidate_fit_residuals"][0]

    assert "co2_dry_correction_h2o_source=reference_h2o" in policy["warning_reasons"]
    assert residual["h2o_mmol_source"] == "reference_h2o"
    assert float(residual["h2o_mmol"]) < 10.0


def test_current_atmosphere_candidate_ignores_allow_pressure_terms_and_can_exclude_bad_device(tmp_path):
    rows = []
    index = 1
    for temp_c in (0.0, 20.0, 40.0):
        for target in (0.0, 300.0, 600.0, 900.0):
            for _ in range(10):
                row = _co2_row(index, target, point_tag=f"co2_T{temp_c:g}_{target:g}")
                row["ga01_chamber_temp_c"] = temp_c + (index % 5) * 0.001
                rows.append(row)
                index += 1
    run_dir, _, _ = _make_run(tmp_path, rows)

    tables, context = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="ga01",
        cfg=CandidateCoefficientPolicyConfig(
            allow_pressure_terms=True,
            allow_temperature_terms=True,
            fit_all_eligible_samples=True,
        ),
        today="2026-05-25",
    )
    policy = tables["candidate_policy_summary"][0]

    assert context["candidate_run_status"] == "fit_ready_requires_verification"
    assert policy["selected_model_terms"] == "intercept;R;R2;R3;T;T2;RT"
    assert policy["temperature_target_grid_status"] == "balanced_temperature_target_grid"
    assert "P" not in policy["selected_model_terms"].split(";")
    assert "RTP" not in policy["selected_model_terms"].split(";")
    assert policy["frozen_terms"] == "P;RP;RTP"
    assert "allow_pressure_terms_ignored_current_atmosphere_open_flow_contract" in policy["warning_reasons"]

    excluded_tables, excluded_context = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="ga01",
        cfg=CandidateCoefficientPolicyConfig(exclude_device_ids=("023",)),
        today="2026-05-25",
    )

    assert excluded_context["candidate_run_status"] == "blocked"
    assert excluded_tables["candidate_policy_summary"] == []
    assert excluded_tables["candidate_coefficients"] == []


def test_candidate_export_rejects_hard_bad_analyzer_temperature_values(tmp_path):
    rows = []
    rows.extend(_co2_row(index, 0.0, point_tag="co2_0") for index in range(1, 11))
    rows.extend(_co2_row(index, 900.0, point_tag="co2_900") for index in range(11, 21))
    rows.extend(_co2_row(index, 450.0, role="verification", point_tag="co2_450_verify") for index in range(21, 31))
    bad_rows = [_co2_row(index, 500.0, point_tag="co2_500_bad_temp") for index in range(31, 41)]
    for row in bad_rows:
        row["ga01_chamber_temp_c"] = 60.0
    rows.extend(bad_rows)
    run_dir, _, _ = _make_run(tmp_path, rows)

    tables, context = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="ga01",
        cfg=CandidateCoefficientPolicyConfig(fit_all_eligible_samples=False),
        today="2026-05-25",
    )

    policy = tables["candidate_policy_summary"][0]
    rejected = tables["candidate_preparation_rejected_samples"]
    assert context["candidate_run_status"] == "verification_passed"
    assert policy["candidate_status"] == "verification_passed"
    assert policy["fit_sample_count"] == 20
    assert policy["preparation_rejected_count"] == 10
    assert len(rejected) == 10
    assert all("temperature_hard_bad_value:60" in row["reject_reasons"] for row in rejected)
    assert all(row["target_value"] != 500.0 for row in tables["candidate_fit_residuals"])


def test_candidate_verification_limit_uses_matching_certificate_uncertainty(tmp_path):
    plan = _plan()
    plan["standard_gases"].append(
        {
            "component": "co2",
            "cylinder_id": "CO2-500",
            "certificate_value": 500.13,
            "certificate_uncertainty": 10.0026,
            "valid_until": "2027-03-12",
            "supplier": "standard-lab",
            "certificate_hash": "co2-500-cert-hash",
        }
    )
    rows = []
    rows.extend(_co2_row(index, 0.0, point_tag="co2_0") for index in range(1, 11))
    rows.extend(_co2_row(index, 900.0, point_tag="co2_900") for index in range(11, 21))
    verification_rows = [
        _co2_row(index, 500.13, role="verification", point_tag="co2_500_13_verify")
        for index in range(21, 31)
    ]
    for row in verification_rows:
        row["ga01_co2_ratio_f"] = 1.50513
    rows.extend(verification_rows)
    run_dir, _, _ = _make_run(tmp_path, rows)

    tables, context = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=plan,
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="ga01",
        cfg=CandidateCoefficientPolicyConfig(fit_all_eligible_samples=False),
        today="2026-05-25",
    )

    policy = tables["candidate_policy_summary"][0]
    verification = tables["candidate_verification_summary"][0]
    assert context["candidate_run_status"] == "verification_passed"
    assert policy["candidate_status"] == "verification_passed"
    assert verification["verification_status"] == "pass"
    assert float(verification["verification_max_error"]) > 2.0
    assert abs(float(verification["verification_error_limit"]) - 10.0026) < 1e-9
    assert "certificate_expanded_uncertainty=10.0026" in verification["verification_error_limit_source"]
    assert verification["verification_certificate_uncertainties"] == "500.13:10.0026"


def test_candidate_verification_without_certificate_uncertainty_keeps_fixed_limit(tmp_path):
    rows = []
    rows.extend(_co2_row(index, 0.0, point_tag="co2_0") for index in range(1, 11))
    rows.extend(_co2_row(index, 900.0, point_tag="co2_900") for index in range(11, 21))
    verification_rows = [
        _co2_row(index, 500.13, role="verification", point_tag="co2_500_13_verify")
        for index in range(21, 31)
    ]
    for row in verification_rows:
        row["ga01_co2_ratio_f"] = 1.50513
    rows.extend(verification_rows)
    run_dir, _, _ = _make_run(tmp_path, rows)

    tables, context = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="ga01",
        cfg=CandidateCoefficientPolicyConfig(fit_all_eligible_samples=False),
        today="2026-05-25",
    )

    policy = tables["candidate_policy_summary"][0]
    verification = tables["candidate_verification_summary"][0]
    assert context["candidate_run_status"] == "fit_ready_requires_verification"
    assert policy["candidate_status"] == "verification_failed"
    assert verification["verification_status"] == "fail"
    assert verification["verification_error_limit"] == 2.0
    assert verification["verification_error_limit_source"] == "fixed_abs_error=2"
    assert "verification_max_error>2" in verification["verification_reasons"]


def test_candidate_policy_blocks_reused_verification_point_identity(tmp_path):
    rows = []
    rows.extend(_co2_row(index, 0.0, point_tag="co2_0") for index in range(1, 11))
    rows.extend(_co2_row(index, 900.0, point_tag="co2_900") for index in range(11, 21))
    verification_rows = [
        _co2_row(index, 450.0, role="verification", point_tag="co2_450_verify") for index in range(21, 26)
    ]
    for row in verification_rows:
        row["verification_point_id"] = "co2_900"
    rows.extend(verification_rows)
    run_dir, _, _ = _make_run(tmp_path, rows)

    tables, _ = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="ga01",
        cfg=CandidateCoefficientPolicyConfig(fit_all_eligible_samples=False),
        today="2026-05-25",
    )

    verification = tables["candidate_verification_summary"][0]
    policy = tables["candidate_policy_summary"][0]
    assert verification["verification_status"] == "fail"
    assert "verification_point_not_independent" in verification["verification_reasons"]
    assert policy["candidate_status"] == "verification_failed"


def test_candidate_export_cli_writes_no_write_artifacts(tmp_path):
    rows = []
    rows.extend(_co2_row(index, 0.0, point_tag="co2_0") for index in range(1, 11))
    rows.extend(_co2_row(index, 900.0, point_tag="co2_900") for index in range(11, 21))
    rows.extend(_co2_row(index, 450.0, role="verification", point_tag="co2_450_verify") for index in range(21, 31))
    run_dir, plan_path, pressure_reference_path = _make_run(tmp_path, rows)
    output_dir = tmp_path / "candidate"

    rc = candidate_cli(
        [
            "--run-dir",
            str(run_dir),
            "--plan-json",
            str(plan_path),
            "--pressure-reference-json",
            str(pressure_reference_path),
            "--output-dir",
            str(output_dir),
            "--component",
            "co2",
            "--today",
            "2026-05-25",
        ]
    )

    assert rc == 0
    summary = _read_csv(output_dir / "candidate_run_summary.csv")
    policy = _read_csv(output_dir / "candidate_policy_summary.csv")
    coefficients = _read_csv(output_dir / "candidate_coefficients.csv")
    assert summary[0]["candidate_run_status"] == "fit_ready_requires_verification"
    assert summary[0]["controls_water_or_gas_routes"] == "False"
    assert summary[0]["opens_com_ports"] == "False"
    assert summary[0]["writes_coefficients"] == "False"
    assert policy[0]["auto_write_allowed"] == "False"
    assert policy[0]["fit_all_eligible_samples"] == "True"
    assert policy[0]["verification_status"] == "missing"
    assert policy[0]["evidence_reuse_class"] == "fit_ready_requires_independent_verification"
    assert {row["term"] for row in coefficients} == {"intercept", "R", "R2"}
    assert (output_dir / "candidate_coefficients_report.md").exists()


def test_candidate_export_cli_can_require_passing_fit_input_quality(tmp_path):
    rows = []
    rows.extend(_co2_row(index, 0.0, point_tag="co2_0") for index in range(1, 11))
    rows.extend(_co2_row(index, 900.0, point_tag="co2_900") for index in range(11, 21))
    rows.extend(_co2_row(index, 450.0, role="verification", point_tag="co2_450_verify") for index in range(21, 31))
    run_dir, plan_path, pressure_reference_path = _make_run(tmp_path, rows)
    summary_path, devices_path = _write_fit_input_quality(tmp_path, components=("co2",))
    output_dir = tmp_path / "candidate_fit_input_gated"

    rc = candidate_cli(
        [
            "--run-dir",
            str(run_dir),
            "--plan-json",
            str(plan_path),
            "--pressure-reference-json",
            str(pressure_reference_path),
            "--fit-input-quality-summary-csv",
            str(summary_path),
            "--fit-input-quality-devices-csv",
            str(devices_path),
            "--require-fit-input-quality",
            "--output-dir",
            str(output_dir),
            "--component",
            "co2",
            "--today",
            "2026-05-25",
        ]
    )

    assert rc == 0
    summary = _read_csv(output_dir / "candidate_run_summary.csv")
    policy = _read_csv(output_dir / "candidate_policy_summary.csv")
    coefficients = _read_csv(output_dir / "candidate_coefficients.csv")
    assert summary[0]["fit_input_quality_required"] == "True"
    assert summary[0]["fit_input_quality_gate_status"] == "pass"
    assert policy[0]["fit_input_quality_grade"] == "A"
    assert policy[0]["fit_input_quality_status"] == "usable_for_candidate_fit"
    assert coefficients
    assert all(row["fit_input_quality_grade"] == "A" for row in coefficients)


def test_candidate_export_cli_can_fit_all_eligible_samples_flag(tmp_path):
    rows = []
    rows.extend(_co2_row(index, 0.0, point_tag="co2_0") for index in range(1, 11))
    rows.extend(_co2_row(index, 900.0, point_tag="co2_900") for index in range(11, 21))
    rows.extend(_co2_row(index, 450.0, role="verification", point_tag="co2_450_verify") for index in range(21, 31))
    run_dir, plan_path, pressure_reference_path = _make_run(tmp_path, rows)
    output_dir = tmp_path / "candidate_all_eligible"

    rc = candidate_cli(
        [
            "--run-dir",
            str(run_dir),
            "--plan-json",
            str(plan_path),
            "--pressure-reference-json",
            str(pressure_reference_path),
            "--output-dir",
            str(output_dir),
            "--component",
            "co2",
            "--fit-all-eligible-samples",
            "--today",
            "2026-05-25",
        ]
    )

    assert rc == 0
    summary = _read_csv(output_dir / "candidate_run_summary.csv")
    policy = _read_csv(output_dir / "candidate_policy_summary.csv")
    residuals = _read_csv(output_dir / "candidate_fit_residuals.csv")
    assert summary[0]["candidate_run_status"] == "fit_ready_requires_verification"
    assert policy[0]["fit_all_eligible_samples"] == "True"
    assert policy[0]["fit_sample_count"] == "30"
    assert policy[0]["verification_sample_count"] == "0"
    assert policy[0]["source_verification_reused_for_fit_count"] == "10"
    assert policy[0]["verification_status"] == "missing"
    assert "requires_new_independent_verification" in policy[0]["verification_reasons"]
    assert "source_verification_samples_reused_for_fit_requires_new_independent_verification" in policy[0]["warning_reasons"]
    assert policy[0]["allowed_for_review"] == "False"
    assert {float(row["target_value"]) for row in residuals} == {0.0, 450.0, 900.0}


def test_h2o_single_target_blocks_and_freezes_pressure_temperature_terms(tmp_path):
    rows = [_h2o_row(index, 0.5, point_tag="h2o_0_5") for index in range(1, 11)]
    run_dir, _, _ = _make_run(tmp_path, rows)

    tables, context = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="h2o",
        analyzer_prefix="ga01",
        today="2026-05-25",
    )
    policy = tables["candidate_policy_summary"][0]

    assert context["candidate_run_status"] == "blocked"
    assert policy["candidate_status"] == "blocked"
    assert "distinct_fit_targets<2" in policy["blocked_reasons"]
    assert "P" in policy["frozen_terms"]
    assert "RTP" in policy["frozen_terms"]
    assert "T" in policy["frozen_terms"]
    assert tables["candidate_coefficients"] == []


def test_h2o_candidate_derives_target_from_dewpoint_and_com22_pressure(tmp_path):
    rows = []
    for index in range(1, 11):
        row = _h2o_row(index, 0.5, point_tag="h2o_dewpoint_reference")
        row.pop("target_h2o_mmol", None)
        row["dewpoint_c"] = 16.46
        row["pressure_gauge_hpa"] = 1021.676 + (index % 3) * 0.01
        rows.append(row)
    run_dir, _, _ = _make_run(tmp_path, rows)
    pressure_rows = [
        {
            "sample_index": index,
            "sample_ts": f"2026-05-25T12:05:{index:02d}",
            "analyzer_prefix": "ga01",
            "analyzer_device_id": "023",
            "pressure_mode": "ambient_open",
            "analyzer_pressure_kpa": 100.05 + index * 0.0001,
            "com22_pressure_hpa": 1000.5 + index * 0.001,
            "pressure_atmosphere_hold_status": "verified",
            "pressure_atmosphere_hold_active": "true",
            "pressure_atmosphere_hold_strategy": "legacy_hold_thread",
        }
        for index in range(1, 11)
    ]
    pressure_path = tmp_path / "pressure_quick_check_external.csv"
    _write_csv(pressure_path, pressure_rows)

    tables, _ = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="h2o",
        analyzer_prefix="ga01",
        pressure_check_path=pressure_path,
        today="2026-05-25",
    )
    policy = tables["candidate_policy_summary"][0]

    assert policy["fit_sample_count"] == 10
    assert policy["fit_point_count"] == 1
    assert policy["preparation_rejected_count"] == 0
    assert policy["distinct_fit_targets"] == 1
    assert policy["evidence_reuse_class"] == "a_grade_single_target_review_only"
    assert tables["candidate_preparation_rejected_samples"] == []


def test_h2o_pressure_instability_is_report_warning_not_fit_blocker(tmp_path):
    rows = [_h2o_row(index, 0.5, point_tag="h2o_0_5") for index in range(1, 11)]
    for index, row in enumerate(rows):
        row["ga01_pressure_kpa"] = 100.0 + index * 0.05
    run_dir, _, _ = _make_run(tmp_path, rows)
    pressure_rows = [
        {
            "sample_index": index,
            "sample_ts": f"2026-05-25T12:05:{index:02d}",
            "analyzer_prefix": "ga01",
            "analyzer_device_id": "023",
            "pressure_mode": "ambient_open",
            "analyzer_pressure_kpa": 100.05 + index * 0.0001,
            "com22_pressure_hpa": 1000.5 + index * 0.001,
            "pressure_atmosphere_hold_status": "verified",
            "pressure_atmosphere_hold_active": "true",
            "pressure_atmosphere_hold_strategy": "legacy_hold_thread",
        }
        for index in range(1, 11)
    ]
    pressure_path = tmp_path / "pressure_quick_check_external.csv"
    _write_csv(pressure_path, pressure_rows)

    tables, _ = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="h2o",
        analyzer_prefix="ga01",
        pressure_check_path=pressure_path,
        today="2026-05-25",
    )
    policy = tables["candidate_policy_summary"][0]

    assert policy["formal_a_grade_count"] == 10
    assert policy["formal_pressure_condition_warning_count"] == 1
    assert "wet_route_pressure_condition_warning_report_only" in policy["warning_reasons"]
    assert policy["evidence_reuse_class"] == "a_grade_single_target_review_only"
    assert policy["candidate_status"] == "blocked"


def test_h2o_candidate_requires_independent_verification_and_keeps_no_write_boundary(tmp_path):
    rows = []
    rows.extend(_h2o_row(index, 0.5, point_tag="h2o_0_5") for index in range(1, 11))
    rows.extend(_h2o_row(index, 1.0, point_tag="h2o_1_0") for index in range(11, 21))
    rows.extend(_h2o_row(index, 0.75, role="verification", point_tag="h2o_0_75_verify") for index in range(21, 31))
    for row in rows:
        row["ga01_pressure_kpa"] = 100.0 + (int(row["sample_index"]) % 10) * 0.05
    run_dir, _, _ = _make_run(tmp_path, rows)
    pressure_rows = [
        {
            "sample_index": index,
            "sample_ts": f"2026-05-25T12:06:{index:02d}",
            "analyzer_prefix": "ga01",
            "analyzer_device_id": "023",
            "pressure_mode": "ambient_open",
            "analyzer_pressure_kpa": 100.05 + index * 0.0001,
            "com22_pressure_hpa": 1000.5 + index * 0.001,
            "pressure_atmosphere_hold_status": "verified",
            "pressure_atmosphere_hold_active": "true",
            "pressure_atmosphere_hold_strategy": "legacy_hold_thread",
        }
        for index in range(1, 11)
    ]
    pressure_path = tmp_path / "pressure_quick_check_h2o_external.csv"
    _write_csv(pressure_path, pressure_rows)

    tables, context = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="h2o",
        analyzer_prefix="ga01",
        pressure_check_path=pressure_path,
        cfg=CandidateCoefficientPolicyConfig(fit_all_eligible_samples=False),
        today="2026-05-25",
    )
    policy = tables["candidate_policy_summary"][0]

    assert context["candidate_run_status"] == "verification_passed"
    assert policy["candidate_status"] == "verification_passed"
    assert policy["formal_pressure_condition_warning_count"] == 3
    assert "wet_route_pressure_condition_warning_report_only" in policy["warning_reasons"]
    assert policy["fit_sample_count"] == 20
    assert policy["verification_sample_count"] == 10
    assert policy["fit_point_count"] == 2
    assert policy["verification_point_count"] == 1
    assert policy["selected_model_terms"] == "intercept;R"
    assert policy["auto_write_allowed"] is False
    assert {row["term"] for row in tables["candidate_coefficients"]} == {"intercept", "R"}
    assert len(tables["candidate_fit_residuals"]) == 2
    assert len(tables["candidate_verification_residuals"]) == 1
    assert all(row["residual_role"] == "verification" for row in tables["candidate_verification_residuals"])
    assert all(row["component"] == "h2o" for row in tables["candidate_fit_residuals"])
    assert max(abs(float(row["error"])) for row in tables["candidate_verification_residuals"]) < 0.05
