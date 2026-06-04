import csv
import json

from gas_calibrator.tools.export_v1_5_formal_readiness import main as readiness_main
from gas_calibrator.validation.formal_evidence_run import prepare_formal_evidence_run
from gas_calibrator.validation.formal_readiness import (
    build_formal_readiness_model,
    write_formal_readiness_report,
)
from gas_calibrator.validation.pressure_channel import write_pressure_quick_check_csv


def _standard_gases():
    return [
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
            "cylinder_id": "H2O-GEN-001",
            "certificate_value": 0.5,
            "certificate_uncertainty": 0.01,
            "valid_until": "2027-01-01",
            "supplier": "standard-lab",
            "certificate_hash": "h2o-cert-hash",
        },
    ]


def _pressure_reference():
    return {
        "device_id": "COM22-DPG-001",
        "certificate_id": "P-CERT-001",
        "certificate_uncertainty": 0.15,
        "valid_until": "2027-01-01",
        "certificate_hash": "pressure-cert-hash",
        "unit": "hPa",
    }


def _safe_config():
    return {
        "workflow": {
            "controlled_write": False,
            "postrun_corrected_delivery": {"enabled": False, "write_devices": False},
        },
        "validation": {
            "dry_collect": {"write_coefficients": False},
            "coefficient_roundtrip": {"write_back_same": False, "allow_write_modified": False},
        },
        "sencos": {},
    }


def _unsafe_config():
    cfg = _safe_config()
    cfg["validation"]["dry_collect"]["write_coefficients"] = True
    return cfg


def _pressure_hardware_unavailable_config():
    cfg = _safe_config()
    cfg["devices"] = {
        "pressure_controller": {"enabled": False, "present": False},
        "pressure_gauge": {"enabled": False, "present": False},
    }
    return cfg


def _row(index: int, component: str, **overrides):
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


def _make_inputs(tmp_path, *, unsafe=False):
    config_path = tmp_path / "runtime_config.json"
    config_path.write_text(json.dumps(_unsafe_config() if unsafe else _safe_config(), ensure_ascii=False), encoding="utf-8")
    gases_path = tmp_path / "standard_gases.json"
    gases_path.write_text(json.dumps({"standard_gases": _standard_gases()}, ensure_ascii=False), encoding="utf-8")
    reference_path = tmp_path / "source_pressure_reference.json"
    reference_path.write_text(json.dumps(_pressure_reference(), ensure_ascii=False), encoding="utf-8")
    outputs = prepare_formal_evidence_run(
        output_dir=tmp_path / "formal_evidence",
        operator="operator-a",
        analyzer_id="GA-001",
        run_id="v1_5_formal_demo",
        config_path=config_path,
        standard_gases_json=gases_path,
        pressure_reference_json=reference_path,
        lab="lab-a",
    )
    return config_path, outputs


def _make_inputs_with_config(tmp_path, config):
    config_path = tmp_path / "runtime_config.json"
    config_path.write_text(json.dumps(config, ensure_ascii=False), encoding="utf-8")
    gases_path = tmp_path / "standard_gases.json"
    gases_path.write_text(json.dumps({"standard_gases": _standard_gases()}, ensure_ascii=False), encoding="utf-8")
    reference_path = tmp_path / "source_pressure_reference.json"
    reference_path.write_text(json.dumps(_pressure_reference(), ensure_ascii=False), encoding="utf-8")
    outputs = prepare_formal_evidence_run(
        output_dir=tmp_path / "formal_evidence",
        operator="operator-a",
        analyzer_id="GA-001",
        run_id="v1_5_formal_demo",
        config_path=config_path,
        standard_gases_json=gases_path,
        pressure_reference_json=reference_path,
        lab="lab-a",
    )
    return config_path, outputs


def _make_inputs_with_gases(tmp_path, gases):
    config_path = tmp_path / "runtime_config.json"
    config_path.write_text(json.dumps(_safe_config(), ensure_ascii=False), encoding="utf-8")
    gases_path = tmp_path / "standard_gases.json"
    gases_path.write_text(json.dumps({"standard_gases": gases}, ensure_ascii=False), encoding="utf-8")
    reference_path = tmp_path / "source_pressure_reference.json"
    reference_path.write_text(json.dumps(_pressure_reference(), ensure_ascii=False), encoding="utf-8")
    outputs = prepare_formal_evidence_run(
        output_dir=tmp_path / "formal_evidence",
        operator="operator-a",
        analyzer_id="GA-001",
        run_id="v1_5_formal_demo",
        config_path=config_path,
        standard_gases_json=gases_path,
        pressure_reference_json=reference_path,
        lab="lab-a",
    )
    return config_path, outputs


def _make_run(tmp_path, *, quick_check=False, samples=False):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    rows = [_row(i, "co2") for i in range(1, 11)] + [_row(i, "h2o") for i in range(11, 21)]
    if samples:
        _write_csv(run_dir / "samples_20260524.csv", rows)
    if quick_check:
        write_pressure_quick_check_csv(run_dir, rows[:10], run_id="20260524")
    return run_dir


def test_readiness_blocks_when_pressure_quick_check_values_fail(tmp_path):
    config_path, outputs = _make_inputs(tmp_path)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    rows = [
        _row(i, "co2", pressure_gauge_hpa=1020.0, ga01_pressure_kpa=100.0)
        for i in range(1, 11)
    ]
    write_pressure_quick_check_csv(run_dir, rows, run_id="20260524")

    model = build_formal_readiness_model(
        run_dir=run_dir,
        plan_path=outputs["plan"],
        pressure_reference_path=outputs["pressure_reference"],
        config_path=config_path,
        today="2026-05-24",
    )

    assert model["readiness_status"] == "pressure_channel_blocked"
    pressure_check = next(row for row in model["checks"] if row["check"] == "pressure_quick_check_contract")
    assert pressure_check["status"] == "fail"
    assert "mean_abs_delta_hpa" in pressure_check["reasons"]


def test_readiness_stops_at_pressure_quick_check_authorization_when_artifacts_missing(tmp_path):
    config_path, outputs = _make_inputs(tmp_path)
    run_dir = _make_run(tmp_path, quick_check=False, samples=False)

    model = build_formal_readiness_model(
        run_dir=run_dir,
        plan_path=outputs["plan"],
        pressure_reference_path=outputs["pressure_reference"],
        config_path=config_path,
        today="2026-05-24",
    )

    assert model["readiness_status"] == "ready_for_pressure_quick_check_authorization"
    assert model["requires_real_device_authorization"] is True
    assert model["opens_com_ports"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["writes_coefficients"] is False
    assert model["real_acceptance_evidence"] is False


def test_readiness_requires_h2o_reference_when_component_scope_is_both(tmp_path):
    co2_only = [row for row in _standard_gases() if row["component"] == "co2"]
    config_path, outputs = _make_inputs_with_gases(tmp_path, co2_only)
    run_dir = _make_run(tmp_path, quick_check=False, samples=False)

    model = build_formal_readiness_model(
        run_dir=run_dir,
        plan_path=outputs["plan"],
        pressure_reference_path=outputs["pressure_reference"],
        config_path=config_path,
        component="both",
        today="2026-05-24",
    )

    assert model["readiness_status"] == "setup_blocked"
    scope_check = next(row for row in model["checks"] if row["check"] == "component_reference_scope")
    assert scope_check["status"] == "fail"
    assert "missing_h2o_standard_gas_or_reference" in scope_check["reasons"]


def test_readiness_allows_co2_only_scope_without_h2o_reference(tmp_path):
    co2_only = [row for row in _standard_gases() if row["component"] == "co2"]
    config_path, outputs = _make_inputs_with_gases(tmp_path, co2_only)
    run_dir = _make_run(tmp_path, quick_check=False, samples=False)

    model = build_formal_readiness_model(
        run_dir=run_dir,
        plan_path=outputs["plan"],
        pressure_reference_path=outputs["pressure_reference"],
        config_path=config_path,
        component="co2",
        today="2026-05-24",
    )

    assert model["readiness_status"] == "ready_for_pressure_quick_check_authorization"
    scope_check = next(row for row in model["checks"] if row["check"] == "component_reference_scope")
    assert scope_check["status"] == "pass"


def test_readiness_blocks_pressure_quick_check_when_pressure_hardware_unavailable(tmp_path):
    config_path, outputs = _make_inputs_with_config(tmp_path, _pressure_hardware_unavailable_config())
    run_dir = _make_run(tmp_path, quick_check=False, samples=False)

    model = build_formal_readiness_model(
        run_dir=run_dir,
        plan_path=outputs["plan"],
        pressure_reference_path=outputs["pressure_reference"],
        config_path=config_path,
        today="2026-05-24",
    )

    assert model["readiness_status"] == "pressure_hardware_blocked"
    assert model["requires_real_device_authorization"] is False
    assert model["pressure_hardware_status"] == "fail"
    assert model["can_run_pressure_quick_check"] is False
    hardware_check = next(row for row in model["checks"] if row["check"] == "pressure_hardware_availability")
    assert hardware_check["status"] == "fail"
    assert "pressure_controller_unavailable" in hardware_check["reasons"]
    assert "pressure_gauge_unavailable" in hardware_check["reasons"]
    quick_check = next(row for row in model["checks"] if row["check"] == "pressure_quick_check_contract")
    assert quick_check["status"] == "blocked_hardware_unavailable"


def test_readiness_advances_to_open_flow_authorization_after_pressure_quick_check(tmp_path):
    config_path, outputs = _make_inputs(tmp_path)
    run_dir = _make_run(tmp_path, quick_check=True, samples=False)

    model = build_formal_readiness_model(
        run_dir=run_dir,
        plan_path=outputs["plan"],
        pressure_reference_path=outputs["pressure_reference"],
        config_path=config_path,
        today="2026-05-24",
    )

    assert model["readiness_status"] == "ready_for_open_flow_sampling_authorization"
    assert model["requires_real_device_authorization"] is True
    assert any(row["step"] == "PRESSURE_CHANNEL_QUICK_CHECK" and row["status"] == "pass" for row in model["formal_run_order"])


def test_readiness_does_not_count_pressure_only_samples_as_open_flow(tmp_path):
    config_path, outputs = _make_inputs(tmp_path)
    run_dir = _make_run(tmp_path, quick_check=True, samples=False)
    pressure_only_rows = [
        _row(i, "co2", point_tag="pressure_only_ambient")
        for i in range(1, 6)
    ]
    _write_csv(run_dir / "samples_20260524.csv", pressure_only_rows)

    model = build_formal_readiness_model(
        run_dir=run_dir,
        plan_path=outputs["plan"],
        pressure_reference_path=outputs["pressure_reference"],
        config_path=config_path,
        today="2026-05-24",
    )

    assert model["readiness_status"] == "ready_for_open_flow_sampling_authorization"
    samples_check = next(row for row in model["checks"] if row["check"] == "open_flow_samples")
    assert samples_check["status"] == "pending_real_no_write"
    assert "open_flow_component_samples_missing" in samples_check["reasons"]


def test_readiness_selects_fleet_pressure_quick_file_and_reports_partial(tmp_path):
    config_path, outputs = _make_inputs(tmp_path)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    rows = []
    for prefix, device_id, pressure_kpa in (
        ("ga01", "023", 100.05),
        ("ga02", "033", 100.04),
        ("ga03", "001", 100.03),
        ("ga04", "027", 100.02),
    ):
        for index in range(1, 5):
            rows.append(
                _row(
                    index,
                    "co2",
                    analyzer_prefix=prefix,
                    analyzer_device_id=device_id,
                    pressure_gauge_hpa=1000.5,
                    controller_pressure=1000.6,
                    analyzer_pressure_kpa=pressure_kpa,
                    **{f"{prefix}_pressure_kpa": pressure_kpa, f"{prefix}_device_id": device_id},
                    **({f"{prefix}_frame_usable": "false"} if prefix == "ga04" else {}),
                )
            )
    write_pressure_quick_check_csv(run_dir, rows, analyzer_prefix="all", run_id="fleet")
    write_pressure_quick_check_csv(
        run_dir,
        [row for row in rows if row["analyzer_prefix"] == "ga04"],
        analyzer_prefix="ga04",
        run_id="zz_latest_single",
    )

    model = build_formal_readiness_model(
        run_dir=run_dir,
        plan_path=outputs["plan"],
        pressure_reference_path=outputs["pressure_reference"],
        config_path=config_path,
        analyzer_prefix="all",
        today="2026-05-24",
    )

    assert model["readiness_status"] == "pressure_channel_partial"
    pressure_check = next(row for row in model["checks"] if row["check"] == "pressure_quick_check_contract")
    assert pressure_check["status"] == "partial"
    assert "blocked_analyzers=ga04:" in pressure_check["reasons"]
    assert "passed_analyzers=ga01,ga02,ga03" in pressure_check["reasons"]


def test_readiness_reaches_reviewer_when_quick_check_and_samples_exist(tmp_path):
    config_path, outputs = _make_inputs(tmp_path)
    run_dir = _make_run(tmp_path, quick_check=True, samples=True)

    model = build_formal_readiness_model(
        run_dir=run_dir,
        plan_path=outputs["plan"],
        pressure_reference_path=outputs["pressure_reference"],
        config_path=config_path,
        today="2026-05-24",
    )

    assert model["readiness_status"] == "ready_for_reviewer"
    assert model["requires_real_device_authorization"] is False
    assert model["package_summary"][0]["package_status"] == "ready_for_reviewer"
    assert {row["candidate_review_status"] for row in model["candidate_coefficient_review"]} == {"ready_for_reviewer"}


def test_readiness_blocks_unsafe_write_config(tmp_path):
    config_path, outputs = _make_inputs(tmp_path, unsafe=True)
    run_dir = _make_run(tmp_path, quick_check=True, samples=True)

    model = build_formal_readiness_model(
        run_dir=run_dir,
        plan_path=outputs["plan"],
        pressure_reference_path=outputs["pressure_reference"],
        config_path=config_path,
        today="2026-05-24",
    )

    assert model["readiness_status"] == "setup_blocked"
    no_write = next(row for row in model["checks"] if row["check"] == "no_write_config")
    assert no_write["status"] == "fail"
    assert "dry_collect_write_coefficients_enabled" in no_write["reasons"]


def test_readiness_writer_and_cli(tmp_path):
    config_path, outputs = _make_inputs(tmp_path)
    run_dir = _make_run(tmp_path, quick_check=False, samples=False)

    paths = write_formal_readiness_report(
        run_dir=run_dir,
        plan_path=outputs["plan"],
        pressure_reference_path=outputs["pressure_reference"],
        config_path=config_path,
        output_dir=tmp_path / "readiness",
        today="2026-05-24",
    )
    assert paths["summary_json"].exists()
    assert paths["summary_markdown"].exists()
    assert paths["workbook"].exists()

    rc = readiness_main(
        [
            "--run-dir",
            str(run_dir),
            "--plan-json",
            str(outputs["plan"]),
            "--pressure-reference-json",
            str(outputs["pressure_reference"]),
            "--config",
            str(config_path),
            "--output-dir",
            str(tmp_path / "readiness_cli"),
            "--today",
            "2026-05-24",
        ]
    )
    assert rc == 0
    assert (tmp_path / "readiness_cli" / "formal_readiness.json").exists()
    assert (tmp_path / "readiness_cli" / "formal_readiness.xlsx").exists()
