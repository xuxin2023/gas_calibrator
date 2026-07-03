import csv
import json

from gas_calibrator.tools.export_v1_5_formal_preflight import main as preflight_main
from gas_calibrator.validation.formal_contracts import (
    COM22_PRESSURE_REFERENCE_TEMPLATE,
    FORMAL_PLAN_TEMPLATE,
    PRESSURE_QUICK_CHECK_REQUIRED_COLUMNS,
    RELEASED_UNCERTAINTY_INPUTS_TEMPLATE,
    STANDARD_GASES_TEMPLATE,
    validate_formal_plan_contract,
    validate_pressure_quick_check_contract,
    validate_pressure_reference_contract,
    write_contract_templates,
)
from gas_calibrator.validation.formal_preflight import (
    assess_no_write_config,
    build_formal_preflight_tables,
    write_formal_preflight_report,
)
from gas_calibrator.validation.pressure_channel import write_pressure_quick_check_csv


def _plan(**overrides):
    plan = {
        "plan_id": "v1_5_formal_demo",
        "plan_version": "2026-05-24",
        "config_hash": "config-hash",
        "operator": "operator-a",
        "analyzer_id": "001",
        "allow_candidate_coefficients": True,
        "allow_device_write": False,
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
    plan.update(overrides)
    return plan


def _pressure_reference(**overrides):
    reference = {
        "device_id": "COM22-DPG-001",
        "certificate_id": "P-CERT-001",
        "certificate_uncertainty": 0.15,
        "valid_until": "2027-01-01",
        "certificate_hash": "pressure-cert-hash",
        "unit": "hPa",
    }
    reference.update(overrides)
    return reference


def _row(index: int, component: str = "co2", **overrides):
    row = {
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
    row.update(overrides)
    return row


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


def _make_run(tmp_path, *, quick_check=True, unsafe_config=False):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    rows = [_row(i, "co2") for i in range(1, 11)] + [_row(i, "h2o") for i in range(11, 21)]
    _write_csv(run_dir / "samples_20260524.csv", rows)
    if quick_check:
        write_pressure_quick_check_csv(run_dir, rows[:10], run_id="20260524")
    plan_path = tmp_path / "formal_plan.json"
    plan_path.write_text(json.dumps(_plan(), ensure_ascii=False), encoding="utf-8")
    pressure_reference_path = tmp_path / "pressure_reference.json"
    pressure_reference_path.write_text(json.dumps(_pressure_reference(), ensure_ascii=False), encoding="utf-8")
    config_path = tmp_path / "runtime_config.json"
    cfg = {
        "workflow": {
            "controlled_write": bool(unsafe_config),
            "postrun_corrected_delivery": {
                "enabled": False,
                "write_devices": False,
            },
        },
        "validation": {
            "dry_collect": {"write_coefficients": False},
            "coefficient_roundtrip": {"write_back_same": False, "allow_write_modified": False},
        },
        "sencos": {},
    }
    config_path.write_text(json.dumps(cfg, ensure_ascii=False), encoding="utf-8")
    return run_dir, plan_path, pressure_reference_path, config_path


def test_contract_templates_are_available_and_written(tmp_path):
    assert FORMAL_PLAN_TEMPLATE["allow_device_write"] is False
    assert STANDARD_GASES_TEMPLATE["allow_device_write"] is False
    assert COM22_PRESSURE_REFERENCE_TEMPLATE["unit"] == "hPa"
    assert RELEASED_UNCERTAINTY_INPUTS_TEMPLATE["released"] is False
    outputs = write_contract_templates(tmp_path / "templates")
    assert outputs["formal_plan_template"].exists()
    assert outputs["standard_gases_template"].exists()
    assert outputs["pressure_reference_template"].exists()
    assert outputs["uncertainty_inputs_template"].exists()
    uncertainty = json.loads(outputs["uncertainty_inputs_template"].read_text(encoding="utf-8"))
    assert uncertainty["released"] is False
    assert {row["status"] for row in uncertainty["inputs"]} == {"not_evaluated"}


def test_formal_plan_and_pressure_reference_contracts_validate_dates_and_writes():
    assert validate_formal_plan_contract(_plan(), today="2026-05-24").status == "pass"
    bad_plan = _plan(allow_device_write=True)
    bad_plan["standard_gases"][0]["valid_until"] = "2026-01-01"
    result = validate_formal_plan_contract(bad_plan, today="2026-05-24")
    assert result.status == "fail"
    assert "allow_device_write_must_be_false" in result.reasons
    assert "standard_gas_1_expired" in result.reasons

    assert validate_pressure_reference_contract(_pressure_reference(), today="2026-05-24").status == "pass"
    expired = validate_pressure_reference_contract(
        _pressure_reference(valid_until="2026-01-01"),
        today="2026-05-24",
    )
    assert expired.status == "fail"
    assert "certificate_expired" in expired.reasons


def test_pressure_quick_check_contract_requires_stable_columns(tmp_path):
    rows = [_row(i) for i in range(1, 6)]
    path = write_pressure_quick_check_csv(tmp_path, rows, run_id="demo")
    quick_rows = _read_csv(path)
    result = validate_pressure_quick_check_contract(quick_rows)
    assert result.status == "pass"
    assert set(PRESSURE_QUICK_CHECK_REQUIRED_COLUMNS).issubset(set(quick_rows[0]))

    bad = [dict(quick_rows[0])]
    bad[0].pop("analyzer_pressure_kpa")
    result = validate_pressure_quick_check_contract(bad)
    assert result.status == "fail"
    assert "missing_column_analyzer_pressure_kpa" in result.reasons
    assert "paired_rows<3" in result.reasons


def test_no_write_config_assessment_blocks_known_write_enablers():
    status, reasons = assess_no_write_config(
        {
            "workflow": {"controlled_write": True},
            "validation": {"dry_collect": {"write_coefficients": True}},
            "sencos": {"9": {"A": 0}},
        }
    )
    assert status == "fail"
    assert "workflow.controlled_write_enabled" in reasons
    assert "dry_collect_write_coefficients_enabled" in reasons
    assert "static_sencos_present" in reasons


def test_formal_preflight_passes_for_complete_sidecar_contracts(tmp_path):
    run_dir, plan_path, pressure_reference_path, config_path = _make_run(tmp_path)

    tables, context = build_formal_preflight_tables(
        run_dir=run_dir,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        config_path=config_path,
        today="2026-05-24",
    )

    assert context["preflight_status"] == "pass"
    assert tables["preflight_summary"][0]["opens_com_ports"] is False
    assert {row["status"] for row in tables["preflight_checks"]} == {"pass"}


def test_formal_preflight_blocks_missing_quick_check_and_unsafe_config(tmp_path):
    run_dir, plan_path, pressure_reference_path, config_path = _make_run(
        tmp_path,
        quick_check=False,
        unsafe_config=True,
    )

    tables, context = build_formal_preflight_tables(
        run_dir=run_dir,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        config_path=config_path,
        today="2026-05-24",
    )

    assert context["preflight_status"] == "fail"
    failed = {row["check"]: row["reasons"] for row in tables["preflight_checks"] if row["status"] == "fail"}
    assert "pressure_quick_check_contract" in failed
    assert "pressure_quick_check_artifact_missing" in failed["pressure_quick_check_contract"]
    assert "workflow.controlled_write_enabled" in failed["no_write_config"]


def test_formal_preflight_report_and_cli_write_artifacts(tmp_path):
    run_dir, plan_path, pressure_reference_path, config_path = _make_run(tmp_path)
    output_dir = tmp_path / "preflight"

    outputs = write_formal_preflight_report(
        run_dir=run_dir,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        config_path=config_path,
        output_dir=output_dir,
        today="2026-05-24",
    )
    assert outputs["workbook"].exists()
    assert _read_csv(outputs["preflight_summary_csv"])[0]["preflight_status"] == "pass"

    cli_dir = tmp_path / "cli_preflight"
    rc = preflight_main(
        [
            "--run-dir",
            str(run_dir),
            "--plan-json",
            str(plan_path),
            "--pressure-reference-json",
            str(pressure_reference_path),
            "--config",
            str(config_path),
            "--output-dir",
            str(cli_dir),
        ]
    )
    assert rc == 0
    assert (cli_dir / "preflight_checks.csv").exists()
