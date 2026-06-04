import csv
import json

from gas_calibrator.tools.run_v1_5_formal_offline_review_chain import main as chain_main
from gas_calibrator.validation.formal_evidence_run import prepare_formal_evidence_run
from gas_calibrator.validation.formal_offline_review_chain import run_formal_offline_review_chain
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
        "h2o_dry_ppmv": 500.0,
        "h2o_wet_ppmv": 505.0,
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
    prepared = prepare_formal_evidence_run(
        output_dir=tmp_path / "formal_evidence",
        operator="operator-a",
        analyzer_id="GA-001",
        run_id="v1_5_formal_demo",
        config_path=config_path,
        standard_gases_json=gases_path,
        pressure_reference_json=reference_path,
        lab="lab-a",
    )
    return config_path, prepared


def _make_run(tmp_path, *, quick_check=False, samples=False):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    rows = [_row(i, "co2") for i in range(1, 11)] + [_row(i, "h2o") for i in range(11, 21)]
    if samples:
        _write_csv(run_dir / "samples_20260524.csv", rows)
    if quick_check:
        write_pressure_quick_check_csv(run_dir, rows[:10], run_id="20260524")
    return run_dir


def test_offline_review_chain_stays_pending_without_real_artifacts(tmp_path):
    config_path, prepared = _make_inputs(tmp_path)
    run_dir = _make_run(tmp_path, quick_check=False, samples=False)

    summary = run_formal_offline_review_chain(
        run_dir=run_dir,
        plan_path=prepared["plan"],
        pressure_reference_path=prepared["pressure_reference"],
        config_path=config_path,
        output_dir=tmp_path / "chain",
        today="2026-05-24",
    )

    assert summary["chain_status"] == "pending_or_blocked"
    assert summary["readiness_status"] == "ready_for_pressure_quick_check_authorization"
    assert summary["requires_real_device_authorization"] is True
    assert summary["opens_com_ports"] is False
    assert summary["controls_water_or_gas_routes"] is False
    assert summary["writes_coefficients"] is False
    assert summary["stages"]["evidence_bundle"]["status"] == "pending"
    assert "report_report_model" not in summary["outputs"]
    assert summary["artifact_manifest"]["status"] == "pass"
    assert summary["outputs"]["chain_artifact_manifest_json"].endswith("offline_review_chain_artifacts.json")
    assert (tmp_path / "chain" / "review_surface" / "v1_5_review_surface.html").exists()
    assert (tmp_path / "chain" / "offline_review_chain_artifacts.json").exists()


def test_offline_review_chain_generates_full_review_outputs_when_evidence_exists(tmp_path):
    config_path, prepared = _make_inputs(tmp_path)
    run_dir = _make_run(tmp_path, quick_check=True, samples=True)

    summary = run_formal_offline_review_chain(
        run_dir=run_dir,
        plan_path=prepared["plan"],
        pressure_reference_path=prepared["pressure_reference"],
        config_path=config_path,
        output_dir=tmp_path / "chain",
        reviewer="reviewer-a",
        approver="approver-a",
        today="2026-05-24",
    )

    assert summary["chain_status"] == "ready_for_reviewer"
    assert summary["readiness_status"] == "ready_for_reviewer"
    assert summary["requires_real_device_authorization"] is False
    assert summary["stages"]["evidence_bundle"]["status"] == "completed"
    assert summary["stages"]["evidence_bundle_integrity"]["status"] == "completed"
    assert summary["stages"]["evidence_bundle_integrity"]["reason"] == "pass"
    assert summary["stages"]["reports"]["status"] == "completed"
    assert summary["stages"]["advanced_qc"]["status"] == "completed"
    assert summary["outputs"]["evidence_bundle_json"].endswith("evidence_bundle.json")
    assert summary["outputs"]["evidence_bundle_integrity_json"].endswith("evidence_bundle_integrity.json")
    assert summary["outputs"]["report_report_model"].endswith("report_model.json")
    assert summary["outputs"]["advanced_qc_json"].endswith("advanced_qc_summary.json")
    assert summary["outputs"]["review_surface_html"].endswith("v1_5_review_surface.html")
    assert summary["artifact_manifest"]["status"] == "pass"
    assert summary["outputs"]["chain_artifact_manifest_json"].endswith("offline_review_chain_artifacts.json")
    manifest = json.loads((tmp_path / "chain" / "offline_review_chain_artifacts.json").read_text(encoding="utf-8"))
    manifest_roles = {row["artifact_role"]: row for row in manifest["artifacts"]}
    assert manifest["status"] == "pass"
    assert manifest["physical_boundaries"]["opens_com_ports"] is False
    assert manifest["physical_boundaries"]["controls_water_or_gas_routes"] is False
    assert manifest["physical_boundaries"]["writes_coefficients"] is False
    for role in (
        "evidence_bundle_json",
        "evidence_bundle_integrity_json",
        "report_formal_calibration_report_pdf",
        "workbench_html",
        "operation_console_html",
        "review_surface_html",
        "chain_summary_json",
    ):
        assert manifest_roles[role]["sha256"]
        assert manifest_roles[role]["exists"] is True
        assert manifest_roles[role]["is_file"] is True


def test_offline_review_chain_cli(tmp_path):
    config_path, prepared = _make_inputs(tmp_path)
    run_dir = _make_run(tmp_path, quick_check=False, samples=False)

    rc = chain_main(
        [
            "--run-dir",
            str(run_dir),
            "--plan-json",
            str(prepared["plan"]),
            "--pressure-reference-json",
            str(prepared["pressure_reference"]),
            "--config",
            str(config_path),
            "--output-dir",
            str(tmp_path / "chain_cli"),
            "--today",
            "2026-05-24",
        ]
    )

    assert rc == 0
    assert (tmp_path / "chain_cli" / "formal_offline_review_chain_summary.json").exists()
    assert (tmp_path / "chain_cli" / "review_surface" / "v1_5_review_surface.json").exists()
