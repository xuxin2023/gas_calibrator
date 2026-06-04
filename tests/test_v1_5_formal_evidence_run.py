import csv
import json

from gas_calibrator.tools.prepare_v1_5_formal_evidence_run import main as prepare_main
from gas_calibrator.tools.run_v1_5_formal_evidence_sidecar import main as sidecar_main
from gas_calibrator.validation.formal_evidence_run import (
    prepare_formal_evidence_run,
    run_formal_evidence_sidecar,
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


def _make_inputs(tmp_path):
    config_path = tmp_path / "runtime_config.json"
    config_path.write_text(json.dumps(_safe_config(), ensure_ascii=False), encoding="utf-8")
    gases_path = tmp_path / "standard_gases.json"
    gases_path.write_text(json.dumps({"standard_gases": _standard_gases()}, ensure_ascii=False), encoding="utf-8")
    reference_path = tmp_path / "source_pressure_reference.json"
    reference_path.write_text(json.dumps(_pressure_reference(), ensure_ascii=False), encoding="utf-8")
    evidence_dir = tmp_path / "formal_evidence"
    outputs = prepare_formal_evidence_run(
        output_dir=evidence_dir,
        operator="operator-a",
        analyzer_id="GA-001",
        run_id="v1_5_formal_demo",
        config_path=config_path,
        standard_gases_json=gases_path,
        pressure_reference_json=reference_path,
        lab="lab-a",
    )
    return config_path, outputs


def _make_run(tmp_path, *, quick_check=True):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    rows = [_row(i, "co2") for i in range(1, 11)] + [_row(i, "h2o") for i in range(11, 21)]
    _write_csv(run_dir / "samples_20260524.csv", rows)
    if quick_check:
        write_pressure_quick_check_csv(run_dir, rows[:10], run_id="20260524")
    return run_dir


def test_prepare_formal_evidence_run_writes_plan_reference_and_manifest(tmp_path):
    config_path, outputs = _make_inputs(tmp_path)
    plan = json.loads(outputs["plan"].read_text(encoding="utf-8"))
    reference = json.loads(outputs["pressure_reference"].read_text(encoding="utf-8"))
    manifest = json.loads(outputs["manifest"].read_text(encoding="utf-8"))

    assert plan["allow_device_write"] is False
    assert plan["config_hash"] != "<sha256-runtime-config>"
    assert plan["runtime_config_path"] == str(config_path.resolve())
    assert {gas["component"] for gas in plan["standard_gases"]} == {"co2", "h2o"}
    assert reference["device_id"] == "COM22-DPG-001"
    assert manifest["opens_com_ports"] is False
    assert manifest["controls_water_or_gas_routes"] is False
    assert manifest["writes_coefficients"] is False


def test_prepare_cli_writes_evidence_templates(tmp_path):
    config_path = tmp_path / "runtime_config.json"
    config_path.write_text(json.dumps(_safe_config(), ensure_ascii=False), encoding="utf-8")
    gases_path = tmp_path / "standard_gases.json"
    gases_path.write_text(json.dumps(_standard_gases(), ensure_ascii=False), encoding="utf-8")
    ref_path = tmp_path / "pressure_reference.json"
    ref_path.write_text(json.dumps(_pressure_reference(), ensure_ascii=False), encoding="utf-8")

    rc = prepare_main(
        [
            "--output-dir",
            str(tmp_path / "prepared"),
            "--operator",
            "operator-a",
            "--analyzer-id",
            "GA-001",
            "--run-id",
            "demo-run",
            "--config",
            str(config_path),
            "--standard-gases-json",
            str(gases_path),
            "--pressure-reference-json",
            str(ref_path),
        ]
    )

    assert rc == 0
    assert (tmp_path / "prepared" / "formal_plan_snapshot.json").exists()
    assert (tmp_path / "prepared" / "com22_pressure_reference.json").exists()
    assert (tmp_path / "prepared" / "evidence_run_manifest.json").exists()


def test_formal_evidence_sidecar_runs_complete_offline_chain(tmp_path):
    config_path, outputs = _make_inputs(tmp_path)
    run_dir = _make_run(tmp_path, quick_check=True)

    summary = run_formal_evidence_sidecar(
        run_dir=run_dir,
        plan_path=outputs["plan"],
        pressure_reference_path=outputs["pressure_reference"],
        config_path=config_path,
        today="2026-05-24",
    )

    assert summary["sidecar_only"] is True
    assert summary["opens_com_ports"] is False
    assert summary["controls_water_or_gas_routes"] is False
    assert summary["writes_coefficients"] is False
    assert summary["evidence_bundle"]["evidence_status"] == "ready_for_reviewer"
    assert summary["evidence_bundle_integrity"]["status"] == "pass"
    assert summary["evidence_bundle_integrity"]["failed_check_count"] == 0
    assert (run_dir / "formal_evidence_sidecar" / "formal_evidence_sidecar_summary.json").exists()
    assert (run_dir / "formal_evidence_sidecar" / "evidence_bundle.json").exists()
    assert (run_dir / "formal_evidence_sidecar" / "evidence_bundle_integrity.json").exists()


def test_formal_evidence_sidecar_blocks_missing_pressure_quick_check(tmp_path):
    config_path, outputs = _make_inputs(tmp_path)
    run_dir = _make_run(tmp_path, quick_check=False)

    summary = run_formal_evidence_sidecar(
        run_dir=run_dir,
        plan_path=outputs["plan"],
        pressure_reference_path=outputs["pressure_reference"],
        config_path=config_path,
        today="2026-05-24",
    )

    assert summary["evidence_bundle"]["evidence_status"] == "blocked"
    assert summary["evidence_bundle"]["package_status"] == "blocked"
    assert summary["evidence_bundle_integrity"]["status"] == "pass"


def test_formal_evidence_sidecar_cli_preflight_only(tmp_path):
    config_path, outputs = _make_inputs(tmp_path)
    run_dir = _make_run(tmp_path, quick_check=True)

    rc = sidecar_main(
        [
            "--run-dir",
            str(run_dir),
            "--plan-json",
            str(outputs["plan"]),
            "--pressure-reference-json",
            str(outputs["pressure_reference"]),
            "--config",
            str(config_path),
            "--stage",
            "preflight",
            "--today",
            "2026-05-24",
        ]
    )

    assert rc == 0
    assert (run_dir / "formal_evidence_sidecar" / "formal_preflight_report" / "formal_preflight.xlsx").exists()
    assert not (run_dir / "formal_evidence_sidecar" / "evidence_bundle.json").exists()
