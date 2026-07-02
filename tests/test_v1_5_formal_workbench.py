import csv
import json

from gas_calibrator.tools.export_v1_5_formal_workbench import main as workbench_main
from gas_calibrator.validation.formal_evidence_run import (
    prepare_formal_evidence_run,
    run_formal_evidence_sidecar,
)
from gas_calibrator.validation.formal_workbench import (
    build_formal_workbench_model,
    write_formal_workbench,
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


def _make_formal_inputs(tmp_path, *, quick_check=True):
    config_path = tmp_path / "runtime_config.json"
    config_path.write_text(json.dumps(_safe_config(), ensure_ascii=False), encoding="utf-8")
    gases_path = tmp_path / "standard_gases.json"
    gases_path.write_text(json.dumps({"standard_gases": _standard_gases()}, ensure_ascii=False), encoding="utf-8")
    reference_path = tmp_path / "pressure_reference_source.json"
    reference_path.write_text(json.dumps(_pressure_reference(), ensure_ascii=False), encoding="utf-8")
    prepared = prepare_formal_evidence_run(
        output_dir=tmp_path / "prepared",
        operator="operator-a",
        analyzer_id="GA-001",
        run_id="v1_5_formal_demo",
        config_path=config_path,
        standard_gases_json=gases_path,
        pressure_reference_json=reference_path,
        lab="lab-a",
    )
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    rows = [_row(i, "co2") for i in range(1, 11)] + [_row(i, "h2o") for i in range(11, 21)]
    _write_csv(run_dir / "samples_20260524.csv", rows)
    if quick_check:
        write_pressure_quick_check_csv(run_dir, rows[:10], run_id="20260524")
    sidecar_summary = run_formal_evidence_sidecar(
        run_dir=run_dir,
        plan_path=prepared["plan"],
        pressure_reference_path=prepared["pressure_reference"],
        config_path=config_path,
        today="2026-05-24",
    )
    sidecar_dir = run_dir / "formal_evidence_sidecar"
    return {
        "run_dir": run_dir,
        "config_path": config_path,
        "plan_path": prepared["plan"],
        "pressure_reference_path": prepared["pressure_reference"],
        "evidence_bundle_path": sidecar_dir / "evidence_bundle.json",
        "sidecar_summary_path": sidecar_dir / "formal_evidence_sidecar_summary.json",
        "sidecar_summary": sidecar_summary,
    }


def test_formal_workbench_model_is_sidecar_only_and_summarizes_ready_run(tmp_path):
    inputs = _make_formal_inputs(tmp_path, quick_check=True)

    model = build_formal_workbench_model(
        run_dir=inputs["run_dir"],
        plan_path=inputs["plan_path"],
        pressure_reference_path=inputs["pressure_reference_path"],
        config_path=inputs["config_path"],
        evidence_bundle_path=inputs["evidence_bundle_path"],
        sidecar_summary_path=inputs["sidecar_summary_path"],
        reviewer="reviewer-a",
        approver="approver-a",
        today="2026-05-24",
    )

    assert model["sidecar_only"] is True
    assert model["opens_com_ports"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["controls_valves_or_pace"] is False
    assert model["writes_coefficients"] is False
    assert model["preflight_status"] == "pass"
    assert model["package_status"] == "ready_for_reviewer"
    assert model["report_summary"]["release_status"] == "draft_only"
    assert any(card["key"] == "open_flow_samples" and "A 级 20" in card["metric"] for card in model["cards"])
    assert any(step["key"] == "PRESSURE_CHANNEL_QUICK_CHECK" for step in model["workflow_steps"])


def test_formal_workbench_blocks_when_pressure_quick_check_is_missing(tmp_path):
    inputs = _make_formal_inputs(tmp_path, quick_check=False)

    model = build_formal_workbench_model(
        run_dir=inputs["run_dir"],
        plan_path=inputs["plan_path"],
        pressure_reference_path=inputs["pressure_reference_path"],
        config_path=inputs["config_path"],
        evidence_bundle_path=inputs["evidence_bundle_path"],
        today="2026-05-24",
    )

    pressure_card = next(card for card in model["cards"] if card["key"] == "pressure_quick_check")
    candidate_card = next(card for card in model["cards"] if card["key"] == "candidate_review")
    assert pressure_card["status"] == "fail"
    assert "pressure_quick_check_artifact_missing" in pressure_card["blockers"]
    assert candidate_card["status"] == "blocked"
    assert "pressure_quick_check_artifact_missing" in candidate_card["blockers"]


def test_formal_workbench_writes_static_html_json_and_markdown(tmp_path):
    inputs = _make_formal_inputs(tmp_path, quick_check=True)
    outputs = write_formal_workbench(
        output_dir=tmp_path / "workbench",
        run_dir=inputs["run_dir"],
        plan_path=inputs["plan_path"],
        pressure_reference_path=inputs["pressure_reference_path"],
        config_path=inputs["config_path"],
        evidence_bundle_path=inputs["evidence_bundle_path"],
        sidecar_summary_path=inputs["sidecar_summary_path"],
        today="2026-05-24",
    )

    assert outputs["html"].exists()
    assert outputs["model"].exists()
    assert outputs["markdown"].exists()
    html = outputs["html"].read_text(encoding="utf-8")
    model = json.loads(outputs["model"].read_text(encoding="utf-8"))
    assert "V1.5 正式校准证据工作台" in html
    assert "不控制水路气路" in html
    assert "压力通道快速验证" in html
    assert "开放流通 CO2/H2O 样本" in html
    assert model["controls_water_or_gas_routes"] is False


def test_formal_workbench_cli_writes_outputs(tmp_path):
    inputs = _make_formal_inputs(tmp_path, quick_check=True)
    output_dir = tmp_path / "cli_workbench"

    rc = workbench_main(
        [
            "--output-dir",
            str(output_dir),
            "--run-dir",
            str(inputs["run_dir"]),
            "--plan-json",
            str(inputs["plan_path"]),
            "--pressure-reference-json",
            str(inputs["pressure_reference_path"]),
            "--config",
            str(inputs["config_path"]),
            "--evidence-bundle-json",
            str(inputs["evidence_bundle_path"]),
            "--sidecar-summary-json",
            str(inputs["sidecar_summary_path"]),
            "--today",
            "2026-05-24",
        ]
    )

    assert rc == 0
    assert (output_dir / "v1_5_formal_workbench.html").exists()
    assert (output_dir / "v1_5_formal_workbench.json").exists()
    assert (output_dir / "v1_5_formal_workbench.md").exists()
