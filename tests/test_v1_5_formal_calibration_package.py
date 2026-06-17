import csv
import json

from gas_calibrator.tools.export_v1_5_formal_calibration_package import main as package_main
from gas_calibrator.validation.artifact_rows import load_latest_sample_rows, normalize_sample_row
from gas_calibrator.validation.formal_calibration_package import (
    build_formal_calibration_package_tables,
    write_formal_calibration_package,
)
from gas_calibrator.validation.pressure_channel import write_pressure_quick_check_csv


def _plan():
    return {
        "plan_id": "v1_5_formal_demo",
        "plan_version": "2026-05-24",
        "config_hash": "config-hash",
        "operator": "operator-a",
        "standard_gases": [
            {
                "component": "co2",
                "cylinder_id": "CO2-001",
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
        "device_id": "COM22-DPG-001",
        "certificate_id": "P-CERT-001",
        "certificate_uncertainty": 0.15,
        "valid_until": "2027-01-01",
        "certificate_hash": "pressure-cert-hash",
    }


def _row(index: int, component: str):
    return {
        "sample_index": index,
        "sample_ts": f"2026-05-24T12:00:{index:02d}",
        "point_phase": component,
        "route": component,
        "pressure_mode": "ambient_open",
        "pressure_gauge_hpa": 1000.5 + index * 0.002,
        "controller_pressure": 1000.6 + index * 0.002,
        "pressure_atmosphere_hold_status": "verified",
        "pressure_atmosphere_hold_active": "true",
        "pressure_atmosphere_hold_strategy": "legacy_hold_thread",
        "dewpoint_c": -30.0 + index * 0.001,
        "ga01_frame_usable": "true",
        "ga01_mode2_contract_status": "pass",
        "ga01_mode2_qc_status": "pass",
        "ga01_mode2_tokens_json": json.dumps(
            ["YGAS", "001", "0900.000", "00.500", "1768.000", "00.410"],
            separators=(",", ":"),
        ),
        "ga01_raw": "YGAS,001,...",
        "ga01_ref_signal": 3322.0,
        "ga01_co2_signal": 4356.0,
        "ga01_h2o_signal": 2631.0,
        "ga01_chamber_temp_c": 25.0 + index * 0.001,
        "ga01_case_temp_c": 25.5,
        "ga01_pressure_kpa": 100.05 + index * 0.0002,
        "ga01_co2_ratio_f": 1.3000 + index * 0.0001,
        "ga01_co2_ppm": 900.0 + index * 0.01,
        "ga01_h2o_ratio_f": 0.7000 + index * 0.00001,
        "ga01_h2o_mmol": 0.5 + index * 0.0001,
    }


def _multi_analyzer_row(index: int, component: str):
    row = {
        "sample_index": index,
        "sample_ts": f"2026-05-24T12:10:{index:02d}",
        "point_phase": component,
        "route": component,
        "pressure_mode": "ambient_open",
        "pressure_gauge_hpa": 1000.5 + index * 0.002,
        "controller_pressure": 1000.6 + index * 0.002,
        "pressure_atmosphere_hold_status": "verified",
        "pressure_atmosphere_hold_active": "true",
        "pressure_atmosphere_hold_strategy": "legacy_hold_thread",
        "dewpoint_c": -30.0 + index * 0.001,
    }
    for offset, (prefix, device_id, pressure_kpa) in enumerate(
        (
            ("ga01", "091", 100.05 + index * 0.0002),
            ("ga02", "033", 101.50),
            ("ga03", "001", 100.05 + index * 0.0002),
        ),
        start=1,
    ):
        row.update(
            {
                f"{prefix}_frame_usable": "true",
                f"{prefix}_mode2_contract_status": "pass",
                f"{prefix}_mode2_qc_status": "pass",
                f"{prefix}_mode2_tokens_json": json.dumps(
                    ["YGAS", device_id, "0900.000", "00.500", "1768.000", "00.410"],
                    separators=(",", ":"),
                ),
                f"{prefix}_raw": f"YGAS,{device_id},...",
                f"{prefix}_ref_signal": 3320.0 + offset,
                f"{prefix}_co2_signal": 4350.0 + offset,
                f"{prefix}_h2o_signal": 2630.0 + offset,
                f"{prefix}_chamber_temp_c": 25.0 + index * 0.001,
                f"{prefix}_case_temp_c": 25.5,
                f"{prefix}_pressure_kpa": pressure_kpa,
                f"{prefix}_co2_ratio_f": 1.3000 + index * 0.0001 + offset * 0.00001,
                f"{prefix}_co2_ppm": 900.0 + index * 0.01 + offset * 0.1,
                f"{prefix}_h2o_ratio_f": 0.7000 + index * 0.00001,
                f"{prefix}_h2o_mmol": 0.5 + index * 0.0001,
            }
        )
    return row


def _long_analyzer_row(index: int, component: str, prefix: str, device_id: str):
    return {
        "sample_index": index,
        "sample_ts": f"2026-05-24T12:30:{index:02d}",
        "analyzer_prefix": prefix,
        "analyzer_device_id": device_id,
        "point_phase": component,
        "route": component,
        "pressure_mode": "ambient_open",
        "pressure_gauge_hpa": 1000.5 + index * 0.002,
        "controller_pressure": 1000.6 + index * 0.002,
        "pressure_atmosphere_hold_status": "verified",
        "pressure_atmosphere_hold_active": "true",
        "pressure_atmosphere_hold_strategy": "legacy_hold_thread",
        "dewpoint_c": -30.0 + index * 0.001,
        "frame_usable": "true",
        "mode2_contract_status": "pass",
        "mode2_qc_status": "pass",
        "mode2_tokens_json": json.dumps(
            ["YGAS", device_id, "0900.000", "00.500", "1768.000", "00.410"],
            separators=(",", ":"),
        ),
        "raw": f"YGAS,{device_id},...",
        "ref_signal": 3320.0,
        "co2_signal": 4350.0,
        "h2o_signal": 2630.0,
        "chamber_temp_c": 25.0 + index * 0.001,
        "case_temp_c": 25.5,
        "pressure_kpa": 100.05 + index * 0.0002,
        "co2_ratio_f": 1.3000 + index * 0.0001,
        "co2_ppm": 900.0 + index * 0.01,
        "h2o_ratio_f": 0.7000 + index * 0.00001,
        "h2o_mmol": 0.5 + index * 0.0001,
    }


def _external_pressure_quick_rows():
    rows = []
    for index in range(1, 11):
        com22 = 1000.5 + index * 0.002
        for prefix, device_id, analyzer_kpa in (
            ("ga01", "091", com22 / 10.0),
            ("ga02", "033", 101.50),
            ("ga03", "001", com22 / 10.0),
        ):
            rows.append(
                {
                    "row_index": len(rows) + 1,
                    "analyzer_prefix": prefix,
                    "analyzer_device_id": device_id,
                    "sample_index": index,
                    "sample_ts": f"2026-05-24T12:20:{index:02d}",
                    "pressure_mode": "ambient_open",
                    "analyzer_pressure_kpa": analyzer_kpa,
                    "com22_pressure_hpa": com22,
                    "pace_pressure_hpa": com22 + 0.1,
                    "pressure_atmosphere_hold_status": "verified",
                    "pressure_atmosphere_hold_active": "true",
                    "pressure_atmosphere_hold_strategy": "legacy_hold_thread",
                    "pressure_channel_row_status": "paired",
                    "verified_quantity": "analyzer_internal_pressure_P",
                }
            )
    return rows


def _external_pressure_quick_rows_for(prefixes):
    rows = []
    for index in range(1, 11):
        com22 = 1000.5 + index * 0.002
        for prefix, device_id in prefixes:
            rows.append(
                {
                    "row_index": len(rows) + 1,
                    "analyzer_prefix": prefix,
                    "analyzer_device_id": device_id,
                    "sample_index": index,
                    "sample_ts": f"2026-05-24T12:40:{index:02d}",
                    "pressure_mode": "ambient_open",
                    "analyzer_pressure_kpa": com22 / 10.0,
                    "com22_pressure_hpa": com22,
                    "pace_pressure_hpa": com22 + 0.1,
                    "pressure_atmosphere_hold_status": "verified",
                    "pressure_atmosphere_hold_active": "true",
                    "pressure_atmosphere_hold_strategy": "legacy_hold_thread",
                    "pressure_channel_row_status": "paired",
                    "verified_quantity": "analyzer_internal_pressure_P",
                }
            )
    return rows


def _write_pressure_completion_package(root, device_rows):
    out_dir = root / "offline_closure" / "pressure_channel_completion_for_formal_package"
    out_dir.mkdir(parents=True)
    ready_count = sum(1 for row in device_rows if row["readiness_status"] == "pass")
    _write_csv(
        out_dir / "pressure_channel_completion_summary.csv",
        [
            {
                "overall_status": "ready_for_open_flow_main_calibration"
                if ready_count == len(device_rows)
                else "blocked",
                "completion_scope_device_ids": ",".join(row["analyzer_device_id"] for row in device_rows),
                "device_count": len(device_rows),
                "ready_device_count": ready_count,
                "blocked_device_count": len(device_rows) - ready_count,
                "pressure_reference_status": "pass",
                "pressure_reference_validation_level": "formal_pressure_validation",
                "pressure_reference_certificate_id": "P-CERT-001",
                "pressure_reference_certificate_hash": "pressure-cert-hash",
            }
        ],
    )
    normalized_rows = []
    for row in device_rows:
        status = row["readiness_status"]
        normalized_rows.append(
            {
                "analyzer_prefix": row["analyzer_prefix"],
                "analyzer_device_id": row["analyzer_device_id"],
                "readiness_status": status,
                "readiness_reasons": "" if status == "pass" else "post_write_pressure_fit_not_pass",
                "senco9_write_status": "written_readback_verified" if status == "pass" else "failed",
                "write_applied": status == "pass",
                "readback_verified": True,
                "post_write_fit_status": "pass" if status == "pass" else "fail",
                "post_write_offset_kpa": "0.01" if status == "pass" else "0.25",
                "post_write_residual_max_abs_hpa": "0.1" if status == "pass" else "3.5",
                "valid_pair_count": "56",
                "distinct_pressure_points": "7",
                "pressure_reference_certificate_id": "P-CERT-001",
                "pressure_reference_certificate_hash": "pressure-cert-hash",
                "can_enter_open_flow_main_calibration": status == "pass",
                "pressure_channel_only": True,
                "not_co2_h2o_fit_evidence": True,
            }
        )
    _write_csv(out_dir / "pressure_channel_device_readiness.csv", normalized_rows)
    _write_csv(
        out_dir / "pressure_channel_traceability.csv",
        [
            {
                "status": "pass",
                "validation_level": "formal_pressure_validation",
                "certificate_id": "P-CERT-001",
                "certificate_hash": "pressure-cert-hash",
            }
        ],
    )
    _write_csv(
        out_dir / "pressure_channel_acceptance_policy.csv",
        [
            {
                "policy_id": "pressure_channel_completion_acceptance_policy",
                "scope": "independent_pressure_input_readiness_for_open_flow_co2_h2o",
            }
        ],
    )
    return out_dir


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


def _read_csv(path):
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _make_run(tmp_path, *, quick_check: bool = True, include_sealed_diagnostic: bool = False):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    rows = [_row(i, "co2") for i in range(1, 11)] + [_row(i, "h2o") for i in range(11, 21)]
    if include_sealed_diagnostic:
        rows.extend(
            _row(
                100 + index,
                "co2",
            )
            | {
                "pressure_mode": "sealed_controlled",
                "point_tag": "sealed_pressure_diagnostic",
                "sample_index": 100 + index,
            }
            for index in range(1, 4)
        )
    _write_csv(run_dir / "samples_20260524.csv", rows)
    if quick_check:
        write_pressure_quick_check_csv(run_dir, rows[:10], run_id="20260524")
    plan_path = tmp_path / "formal_plan.json"
    plan_path.write_text(json.dumps(_plan(), ensure_ascii=False), encoding="utf-8")
    pressure_reference_path = tmp_path / "pressure_reference.json"
    pressure_reference_path.write_text(
        json.dumps(_pressure_reference(), ensure_ascii=False),
        encoding="utf-8",
    )
    return run_dir, plan_path, pressure_reference_path


def test_formal_package_ready_when_contracts_and_pressure_quick_check_pass(tmp_path):
    run_dir, _, _ = _make_run(tmp_path, quick_check=True)

    tables, context = build_formal_calibration_package_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        today="2026-05-24",
    )

    assert context["pressure_check_source"] == "pressure_quick_check_artifact"
    assert tables["package_summary"][0]["package_status"] == "ready_for_reviewer"
    assert {row["candidate_review_status"] for row in tables["candidate_coefficient_review"]} == {
        "ready_for_reviewer"
    }
    assert all(row["candidate_fit_auto_write_allowed"] is False for row in tables["candidate_coefficient_review"])
    assert len(tables["a_grade_samples"]) == 20


def test_formal_replay_prefers_machine_readable_samples_for_multi_analyzer_rows(tmp_path):
    run_dir = tmp_path / "machine_readable_run"
    run_dir.mkdir()
    _write_csv(run_dir / "samples_20260524.csv", [_row(1, "co2")])
    _write_csv(run_dir / "samples_machine_readable.csv", [_multi_analyzer_row(i, "co2") for i in range(1, 11)])

    samples_path, rows = load_latest_sample_rows(run_dir)
    assert samples_path.name == "samples_machine_readable.csv"
    assert any("ga02_co2_ratio_f" in row for row in rows)

    _write_csv(run_dir / "pressure_channel_quick_check_20260524.csv", rows)
    tables, context = build_formal_calibration_package_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="all",
        today="2026-05-24",
    )

    assert "ga02" in context["analyzer_prefixes"]
    review = {row["analyzer_device_id"]: row for row in tables["candidate_coefficient_review"]}
    assert review["091"]["candidate_review_status"] == "ready_for_reviewer"
    assert review["001"]["candidate_review_status"] == "ready_for_reviewer"


def test_sample_row_normalization_maps_chinese_analyzer_headers():
    row = normalize_sample_row({"气体分析仪2_二氧化碳比值滤波后": "1.234"})
    assert row["ga02_co2_ratio_f"] == "1.234"


def test_formal_package_loads_queue_style_aggregate_samples_and_keeps_devices_separate(tmp_path):
    run_dir = tmp_path / "queue_run"
    co2_dir = run_dir / "candidate_fit_queue" / "co2_aggregate"
    h2o_dir = run_dir / "candidate_fit_queue" / "h2o_aggregate"
    co2_dir.mkdir(parents=True)
    h2o_dir.mkdir(parents=True)
    device_prefixes = (("ga01", "091"), ("ga03", "077"))
    _write_csv(
        co2_dir / "samples_machine_readable.csv",
        [
            _long_analyzer_row(index, "co2", prefix, device_id)
            for prefix, device_id in device_prefixes
            for index in range(1, 11)
        ],
    )
    _write_csv(
        h2o_dir / "samples_machine_readable.csv",
        [
            _long_analyzer_row(index, "h2o", prefix, device_id)
            for prefix, device_id in device_prefixes
            for index in range(11, 21)
        ],
    )
    pressure_path = tmp_path / "pressure_channel_quick_check_external.csv"
    _write_csv(pressure_path, _external_pressure_quick_rows_for(device_prefixes))

    samples_path, rows = load_latest_sample_rows(run_dir)
    assert "candidate_fit_queue" in str(samples_path)
    assert len(rows) == 40
    assert {row["analyzer_device_id"] for row in rows} == {"091", "077"}

    tables, context = build_formal_calibration_package_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="both",
        analyzer_prefix="all",
        pressure_check_path=pressure_path,
        today="2026-05-24",
    )

    assert context["analyzer_prefixes"] == ["ga01", "ga03"]
    review = {
        (row["analyzer_prefix"], row["component"]): row
        for row in tables["candidate_coefficient_review"]
    }
    assert review[("ga01", "co2")]["analyzer_device_id"] == "091"
    assert review[("ga03", "co2")]["analyzer_device_id"] == "077"
    assert review[("ga01", "h2o")]["analyzer_device_id"] == "091"
    assert review[("ga03", "h2o")]["analyzer_device_id"] == "077"
    assert all(row["candidate_review_status"] == "ready_for_reviewer" for row in review.values())


def test_formal_package_blocks_when_pressure_quick_check_artifact_is_missing(tmp_path):
    run_dir, _, _ = _make_run(tmp_path, quick_check=False)

    tables, context = build_formal_calibration_package_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        today="2026-05-24",
    )

    assert context["pressure_check_source"] == "sample_rows_fallback"
    assert tables["package_summary"][0]["package_status"] == "blocked"
    blockers = ";".join(row["blockers"] for row in tables["candidate_coefficient_review"])
    assert "pressure_quick_check_artifact_missing" in blockers


def test_formal_package_accepts_pressure_channel_completion_as_pressure_precondition(tmp_path):
    run_dir, _, _ = _make_run(tmp_path, quick_check=False)
    completion_dir = _write_pressure_completion_package(
        run_dir,
        [{"analyzer_prefix": "ga01", "analyzer_device_id": "001", "readiness_status": "pass"}],
    )

    tables, context = build_formal_calibration_package_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        today="2026-05-24",
    )

    assert context["pressure_check_source"] == "pressure_channel_completion_artifact"
    assert context["pressure_check_path"].endswith("pressure_channel_completion_summary.csv")
    assert completion_dir.name in context["pressure_check_path"]
    assert tables["package_summary"][0]["package_status"] == "ready_for_reviewer"
    assert {row["candidate_review_status"] for row in tables["candidate_coefficient_review"]} == {
        "ready_for_reviewer"
    }
    assert tables["candidate_coefficient_review"][0]["pressure_validation_level"] == "formal_pressure_completion"


def test_formal_package_keeps_blocked_completion_device_blocked_without_missing_artifact(tmp_path):
    run_dir = tmp_path / "completion_multi_pressure_run"
    run_dir.mkdir()
    rows = [_multi_analyzer_row(i, "co2") for i in range(1, 11)]
    _write_csv(run_dir / "samples_20260524.csv", rows)
    _write_pressure_completion_package(
        run_dir,
        [
            {"analyzer_prefix": "ga01", "analyzer_device_id": "091", "readiness_status": "pass"},
            {"analyzer_prefix": "ga02", "analyzer_device_id": "033", "readiness_status": "blocked"},
            {"analyzer_prefix": "ga03", "analyzer_device_id": "001", "readiness_status": "pass"},
        ],
    )

    tables, context = build_formal_calibration_package_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="all",
        today="2026-05-24",
    )

    review = {row["analyzer_device_id"]: row for row in tables["candidate_coefficient_review"]}
    assert context["pressure_check_source"] == "pressure_channel_completion_artifact"
    assert review["091"]["candidate_review_status"] == "ready_for_reviewer"
    assert review["001"]["candidate_review_status"] == "ready_for_reviewer"
    assert review["033"]["candidate_review_status"] == "blocked"
    assert "pressure_channel_validation_not_formal_pass" in review["033"]["blockers"]
    assert "pressure_quick_check_artifact_missing" not in review["033"]["blockers"]


def test_formal_package_report_and_cli_write_reviewer_artifacts(tmp_path):
    run_dir, plan_path, pressure_reference_path = _make_run(tmp_path, quick_check=True)
    output_dir = tmp_path / "package"

    outputs = write_formal_calibration_package(
        run_dir=run_dir,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        output_dir=output_dir,
        today="2026-05-24",
    )
    assert outputs["workbook"].exists()
    summary = _read_csv(outputs["package_summary_csv"])
    assert summary[0]["package_status"] == "ready_for_reviewer"

    cli_dir = tmp_path / "cli_package"
    rc = package_main(
        [
            "--run-dir",
            str(run_dir),
            "--plan-json",
            str(plan_path),
            "--pressure-reference-json",
            str(pressure_reference_path),
            "--output-dir",
            str(cli_dir),
        ]
    )
    assert rc == 0
    assert (cli_dir / "candidate_coefficient_review.csv").exists()


def test_formal_package_replay_blocks_missing_pressure_reference_traceability(tmp_path):
    run_dir, _, _ = _make_run(tmp_path, quick_check=True)
    bad_reference = dict(_pressure_reference())
    bad_reference["certificate_hash"] = ""

    tables, context = build_formal_calibration_package_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=bad_reference,
        today="2026-05-24",
    )

    assert context["package_status"] == "blocked"
    assert tables["pressure_reference_traceability"][0]["status"] == "fail"
    blockers = ";".join(row["blockers"] for row in tables["candidate_coefficient_review"])
    assert "pressure_channel_validation_not_formal_pass" in blockers


def test_formal_package_replay_keeps_sealed_diagnostic_rows_out_of_a_grade_fit(tmp_path):
    run_dir, _, _ = _make_run(tmp_path, quick_check=True, include_sealed_diagnostic=True)

    tables, context = build_formal_calibration_package_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        today="2026-05-24",
    )

    assert context["package_status"] == "ready_for_reviewer"
    assert len(tables["a_grade_samples"]) == 20
    rejected_reasons = ";".join(row.get("formal_reject_reasons", "") for row in tables["rejected_samples"])
    assert "non_open_flow_pressure_mode(sealed_controlled)" in rejected_reasons


def test_formal_package_binds_pressure_quick_check_by_analyzer_device_id(tmp_path):
    run_dir = tmp_path / "multi_pressure_run"
    run_dir.mkdir()
    rows = [_multi_analyzer_row(i, "co2") for i in range(1, 11)]
    _write_csv(run_dir / "samples_20260524.csv", rows)
    _write_csv(run_dir / "pressure_channel_quick_check_20260524.csv", rows)

    tables, context = build_formal_calibration_package_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="all",
        today="2026-05-24",
    )

    review = {row["analyzer_device_id"]: row for row in tables["candidate_coefficient_review"]}
    pressure_summary = {row["analyzer_device_id"]: row for row in tables["pressure_validation_summary"]}

    assert context["pressure_check_source"] == "pressure_quick_check_artifact"
    assert review["091"]["candidate_review_status"] == "ready_for_reviewer"
    assert review["001"]["candidate_review_status"] == "ready_for_reviewer"
    assert review["033"]["candidate_review_status"] == "blocked"
    assert review["033"]["pressure_binding"] == "device_id"
    assert review["033"]["pressure_analyzer_device_id"] == "033"
    assert "pressure_channel_validation_not_formal_pass" in review["033"]["blockers"]
    assert pressure_summary["033"]["status"] == "fail"
    assert pressure_summary["091"]["status"] == "pass"
    assert pressure_summary["001"]["status"] == "pass"


def test_formal_package_accepts_external_pressure_quick_check_and_blocks_only_bad_device(tmp_path):
    run_dir = tmp_path / "external_pressure_run"
    run_dir.mkdir()
    rows = [_multi_analyzer_row(i, "co2") for i in range(1, 11)]
    _write_csv(run_dir / "samples_20260524.csv", rows)
    pressure_path = tmp_path / "pressure_channel_quick_check_external.csv"
    _write_csv(pressure_path, _external_pressure_quick_rows())

    tables, context = build_formal_calibration_package_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="all",
        pressure_check_path=pressure_path,
        today="2026-05-24",
    )

    review = {row["analyzer_device_id"]: row for row in tables["candidate_coefficient_review"]}

    assert context["pressure_check_source"] == "external_pressure_quick_check_artifact"
    assert review["091"]["candidate_review_status"] == "ready_for_reviewer"
    assert review["001"]["candidate_review_status"] == "ready_for_reviewer"
    assert review["033"]["candidate_review_status"] == "blocked"
    assert "pressure_channel_validation_not_formal_pass" in review["033"]["blockers"]


def test_formal_package_pressure_warning_keeps_analyzer_identity_and_a_grade(tmp_path):
    run_dir = tmp_path / "b_grade_identity_run"
    run_dir.mkdir()
    rows = [_multi_analyzer_row(i, "h2o") for i in range(1, 11)]
    for row in rows:
        index = int(row["sample_index"])
        row["ga02_pressure_kpa"] = 100.0 + index * 0.3
    _write_csv(run_dir / "samples_20260524.csv", rows)

    pressure_rows = _external_pressure_quick_rows()
    for row in pressure_rows:
        row["analyzer_pressure_kpa"] = float(row["com22_pressure_hpa"]) / 10.0
    pressure_path = tmp_path / "pressure_channel_quick_check_external.csv"
    _write_csv(pressure_path, pressure_rows)

    tables, _ = build_formal_calibration_package_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="h2o",
        analyzer_prefix="all",
        pressure_check_path=pressure_path,
        today="2026-05-24",
    )

    a_grade_rows = [row for row in tables["a_grade_samples"] if row["analyzer_prefix"] == "ga02"]
    assert len(a_grade_rows) == 10
    assert {row["analyzer_device_id"] for row in a_grade_rows} == {"033"}
    assert all("analyzer_pressure_hpa_span" in row["formal_report_warning_reasons"] for row in a_grade_rows)
    assert not [row for row in tables["b_grade_review_samples"] if row["analyzer_prefix"] == "ga02"]
