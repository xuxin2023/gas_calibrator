import json

from gas_calibrator.tools.prepare_v1_5_formal_run_package import (
    main as package_main,
    prepare_formal_run_package,
)


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


def _reviewed_standard_gases():
    return {
        "standard_gases": [
            {
                "component": "co2",
                "cylinder_id": "CO2-900",
                "certificate_id": "CO2-CERT-001",
                "certificate_value": 900.0,
                "certificate_unit": "ppm",
                "certificate_uncertainty": 0.9,
                "valid_until": "2027-01-01",
                "supplier": "standard-lab",
                "certificate_hash": "co2-cert-hash",
            },
            {
                "component": "h2o",
                "cylinder_id": "H2O-GEN-001",
                "certificate_id": "H2O-CERT-001",
                "certificate_value": 0.5,
                "certificate_unit": "mmol/mol",
                "certificate_uncertainty": 0.01,
                "valid_until": "2027-01-01",
                "supplier": "standard-lab",
                "certificate_hash": "h2o-cert-hash",
            },
        ],
    }


def _reviewed_pressure_reference():
    return {
        "device_id": "COM22-DPG-001",
        "certificate_id": "P-CERT-001",
        "certificate_uncertainty": 0.15,
        "valid_until": "2027-01-01",
        "certificate_hash": "pressure-cert-hash",
        "supplier": "cal-lab",
        "unit": "hPa",
    }


def test_prepare_formal_run_package_writes_no_write_templates_snapshots_and_runbook(tmp_path):
    config_path = tmp_path / "runtime_config.json"
    config_path.write_text(json.dumps(_safe_config(), ensure_ascii=False), encoding="utf-8")

    outputs = prepare_formal_run_package(
        output_dir=tmp_path / "formal_run_package",
        operator="operator-a",
        analyzer_id="GA-001",
        run_id="planned-run-001",
        plan_id="plan-001",
        plan_version="2026-05-24",
        config_path=config_path,
        lab="lab-a",
        ambient_temperature_c="24.5",
        ambient_rh_pct="45",
    )

    expected_keys = {
        "formal_plan_template",
        "standard_gases_template",
        "pressure_reference_template",
        "uncertainty_inputs_template",
        "plan",
        "pressure_reference",
        "manifest",
        "runbook",
    }
    assert expected_keys.issubset(outputs)
    assert all(path.exists() for path in outputs.values())

    plan = json.loads(outputs["plan"].read_text(encoding="utf-8"))
    manifest = json.loads(outputs["manifest"].read_text(encoding="utf-8"))
    standard_gases = json.loads(outputs["standard_gases_template"].read_text(encoding="utf-8"))
    uncertainty = json.loads(outputs["uncertainty_inputs_template"].read_text(encoding="utf-8"))
    runbook = outputs["runbook"].read_text(encoding="utf-8")

    assert plan["allow_device_write"] is False
    assert plan["formal_run_id"] == "planned-run-001"
    assert plan["plan_id"] == "plan-001"
    assert plan["runtime_config_path"] == str(config_path.resolve())
    assert plan["environment"]["lab"] == "lab-a"
    assert plan["environment"]["ambient_temperature_c"] == "24.5"
    assert {gas["component"] for gas in plan["standard_gases"]} == {"co2", "h2o"}
    assert standard_gases["sidecar_only"] is True
    assert standard_gases["allow_device_write"] is False
    assert uncertainty["released"] is False
    assert {row["status"] for row in uncertainty["inputs"]} == {"not_evaluated"}
    assert manifest["sidecar_only"] is True
    assert manifest["opens_com_ports"] is False
    assert manifest["controls_water_or_gas_routes"] is False
    assert manifest["controls_valves_or_pace"] is False
    assert manifest["writes_coefficients"] is False
    assert "does not open COM ports" in runbook
    assert "python -m gas_calibrator.tools.validate_pressure_only" in runbook
    assert "--require-continuous-atmosphere-hold" in runbook
    assert "Missing released uncertainty keeps reports in `draft_only`" in runbook


def test_prepare_formal_run_package_uses_reviewed_traceability_json_when_provided(tmp_path):
    config_path = tmp_path / "runtime_config.json"
    config_path.write_text(json.dumps(_safe_config(), ensure_ascii=False), encoding="utf-8")
    gases_path = tmp_path / "standard_gases.json"
    gases_path.write_text(json.dumps(_reviewed_standard_gases(), ensure_ascii=False), encoding="utf-8")
    pressure_path = tmp_path / "com22_pressure_reference.json"
    pressure_path.write_text(json.dumps(_reviewed_pressure_reference(), ensure_ascii=False), encoding="utf-8")

    outputs = prepare_formal_run_package(
        output_dir=tmp_path / "reviewed_package",
        operator="operator-a",
        analyzer_id="GA-001",
        run_id="planned-run-reviewed",
        config_path=config_path,
        standard_gases_json=gases_path,
        pressure_reference_json=pressure_path,
    )

    plan = json.loads(outputs["plan"].read_text(encoding="utf-8"))
    pressure_reference = json.loads(outputs["pressure_reference"].read_text(encoding="utf-8"))

    assert plan["standard_gases"][0]["cylinder_id"] == "CO2-900"
    assert plan["standard_gases"][0]["certificate_value"] == 900.0
    assert plan["standard_gases"][1]["cylinder_id"] == "H2O-GEN-001"
    assert pressure_reference["certificate_id"] == "P-CERT-001"
    assert pressure_reference["reference_role"] == "primary_pressure_reference"


def test_prepare_formal_run_package_cli_writes_complete_package(tmp_path):
    config_path = tmp_path / "runtime_config.json"
    config_path.write_text(json.dumps(_safe_config(), ensure_ascii=False), encoding="utf-8")
    output_dir = tmp_path / "cli_package"

    rc = package_main(
        [
            "--output-dir",
            str(output_dir),
            "--operator",
            "operator-a",
            "--analyzer-id",
            "GA-001",
            "--run-id",
            "planned-run-cli",
            "--config",
            str(config_path),
        ]
    )

    assert rc == 0
    for filename in (
        "formal_plan_snapshot_template.json",
        "standard_gases_template.json",
        "com22_pressure_reference_template.json",
        "released_uncertainty_inputs_template.json",
        "formal_plan_snapshot.json",
        "com22_pressure_reference.json",
        "evidence_run_manifest.json",
        "v1_5_formal_no_write_runbook.md",
    ):
        assert (output_dir / filename).exists()
