import json

import pytest

from gas_calibrator.tools.export_v1_5_run_evidence_status import main as export_status_main
from gas_calibrator.validation.v1_5_canonical_evidence import write_canonical_v1_5_evidence_package
from gas_calibrator.validation.v1_5_run_evidence_status import (
    build_v1_5_run_evidence_status,
    render_v1_5_run_evidence_status_markdown,
)


pytestmark = pytest.mark.v1_5_formal_gate


def _write_contract(path, *, status="pass"):
    payload = {
        "schema": "v1_5_formal_flow_contract_v1",
        "status": status,
        "physical_boundaries": {
            "offline_audit_only": True,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "controls_valves_or_pace": False,
            "writes_coefficients": False,
            "not_real_acceptance_evidence": True,
        },
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _stages(status):
    return {row["stage_id"]: row for row in status["stage_statuses"]}


def test_run_evidence_status_indexes_canonical_bundle_without_touching_devices(tmp_path):
    outputs = write_canonical_v1_5_evidence_package(tmp_path / "canonical", include_reports=False)
    run_dir = outputs["root"]
    contract_path = _write_contract(run_dir / "v1_5_formal_flow_contract.json", status="pass")

    status = build_v1_5_run_evidence_status(
        run_dir=run_dir,
        contract_json=contract_path,
        evidence_bundle_json=outputs["evidence_bundle"],
    )
    stages = _stages(status)

    assert status["overall_status"] == "incomplete"
    assert status["current_stage"] == "identity_getco_epoch0"
    assert status["physical_boundaries"]["opens_com_ports"] is False
    assert status["physical_boundaries"]["controls_water_or_gas_routes"] is False
    assert status["physical_boundaries"]["writes_coefficients"] is False
    assert status["physical_boundaries"]["not_real_acceptance_evidence"] is True
    assert stages["full_flow_contract_gate"]["status"] == "pass"
    assert stages["plan_traceability"]["status"] == "pass"
    assert stages["pressure_quick_check"]["status"] == "pass"
    assert stages["co2_open_flow"]["status"] == "pass"
    assert stages["h2o_open_flow"]["status"] == "pass"
    assert stages["candidate_review"]["status"] == "pass"
    assert stages["controlled_write_events"]["status"] == "not_attempted"
    assert stages["evidence_bundle"]["status"] == "pass"
    assert status["traceability_checks"]["has_water_route_traceability"] is True
    assert any(row["role"] == "raw_samples" for row in status["artifacts"])
    assert any(row["role"] == "pressure_channel_quick_check" for row in status["artifacts"])


def test_run_evidence_status_blocks_when_contract_audit_blocks(tmp_path):
    run_dir = tmp_path / "blocked_run"
    run_dir.mkdir()
    contract_path = _write_contract(run_dir / "v1_5_formal_flow_contract.json", status="blocked")

    status = build_v1_5_run_evidence_status(run_dir=run_dir, contract_json=contract_path)
    stages = _stages(status)

    assert status["overall_status"] == "blocked"
    assert status["current_stage"] == "full_flow_contract_gate"
    assert stages["full_flow_contract_gate"]["status"] == "blocked"
    assert stages["full_flow_contract_gate"]["reason"] == "contract_status=blocked"


def test_run_evidence_status_cli_writes_json_and_markdown(tmp_path, capsys):
    outputs = write_canonical_v1_5_evidence_package(tmp_path / "canonical_cli", include_reports=False)
    contract_path = _write_contract(outputs["root"] / "v1_5_formal_flow_contract.json", status="pass")
    output_dir = tmp_path / "status_out"

    rc = export_status_main(
        [
            "--run-dir",
            str(outputs["root"]),
            "--contract-json",
            str(contract_path),
            "--evidence-bundle-json",
            str(outputs["evidence_bundle"]),
            "--output-dir",
            str(output_dir),
        ]
    )
    cli_result = json.loads(capsys.readouterr().out)
    status_path = output_dir / "v1_5_run_evidence_status.json"
    markdown_path = output_dir / "v1_5_run_evidence_status.md"
    status = json.loads(status_path.read_text(encoding="utf-8"))

    assert rc == 0
    assert cli_result["status"] == status["overall_status"]
    assert status_path.exists()
    assert markdown_path.exists()
    assert "Physical Boundaries" in markdown_path.read_text(encoding="utf-8")
    assert "CO2 open-flow evidence" in markdown_path.read_text(encoding="utf-8")
    assert status["physical_boundaries"]["opens_com_ports"] is False


def test_run_evidence_status_markdown_keeps_physical_meaning_visible(tmp_path):
    run_dir = tmp_path / "markdown_run"
    run_dir.mkdir()
    contract_path = _write_contract(run_dir / "v1_5_formal_flow_contract.json", status="blocked")

    status = build_v1_5_run_evidence_status(run_dir=run_dir, contract_json=contract_path)
    markdown = render_v1_5_run_evidence_status_markdown(status)

    assert "`full_flow_contract_gate` Full-flow contract audit gate: `blocked`" in markdown
    assert "pressure-first" in markdown
    assert "`writes_coefficients`: `False`" in markdown


def test_run_evidence_status_classifies_optical_and_status_reports_as_diagnostics(tmp_path):
    run_dir = tmp_path / "diagnostic_run"
    run_dir.mkdir()
    (run_dir / "six_device_optical_root_cause_report_zh.md").write_text("光学根因", encoding="utf-8")
    (run_dir / "status_register_and_invalid_frame_summary.csv").write_text(
        "device_id,status_register\n079,0101\n",
        encoding="utf-8",
    )

    status = build_v1_5_run_evidence_status(run_dir=run_dir)
    diagnostic_paths = [
        row["path"]
        for row in status["artifacts"]
        if row["role"] == "diagnostic_analysis"
    ]

    assert any("optical_root_cause" in path for path in diagnostic_paths)
    assert any("status_register" in path for path in diagnostic_paths)


def test_run_evidence_status_indexes_per_device_certificates_and_h2o_exclusions(tmp_path):
    run_dir = tmp_path / "certificate_run"
    cert_dir = run_dir / "reports" / "per_device_certificates"
    h2o_dir = run_dir / "h2o_open_flow" / "aborted_point"
    cert_dir.mkdir(parents=True)
    h2o_dir.mkdir(parents=True)
    (run_dir / "reports" / "per_device_certificate_manifest.json").write_text("{}", encoding="utf-8")
    (run_dir / "reports" / "per_device_certificate_artifact_hashes.csv").write_text(
        "artifact_key,sha256\nx,abc\n",
        encoding="utf-8",
    )
    (cert_dir / "device_001_calibration_certificate.docx").write_bytes(b"docx")
    (cert_dir / "device_001_verification_certificate.pdf").write_bytes(b"%PDF-")
    (h2o_dir / "queue_abort_exclusion.csv").write_text(
        "point_id,exclude_from_fit\np001,true\n",
        encoding="utf-8",
    )

    status = build_v1_5_run_evidence_status(run_dir=run_dir)
    stages = _stages(status)
    roles = {row["role"] for row in status["artifacts"]}

    assert "per_device_certificate_manifest" in roles
    assert "per_device_certificate_artifact_hashes" in roles
    assert "per_device_calibration_certificate" in roles
    assert "per_device_verification_certificate" in roles
    assert "h2o_queue_exclusion" in roles
    assert stages["per_device_certificates"]["status"] == "pass"
    assert stages["h2o_queue_exclusion"]["status"] == "pass"


def test_run_evidence_status_indexes_full_flow_closure_readiness(tmp_path):
    run_dir = tmp_path / "closure_ready_run"
    closure_dir = run_dir / "full_flow_closure_readiness"
    closure_dir.mkdir(parents=True)
    (closure_dir / "v1_5_full_flow_closure_readiness.json").write_text(
        json.dumps({"overall_status": "ready_for_controlled_write_review"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (closure_dir / "v1_5_full_flow_closure_readiness.md").write_text("闭环 readiness", encoding="utf-8")
    (closure_dir / "v1_5_full_flow_closure_gaps.csv").write_text("gap_id,status\n", encoding="utf-8")
    (closure_dir / "v1_5_full_flow_device_closure.csv").write_text(
        "device_id,status\n001,ready\n",
        encoding="utf-8",
    )

    status = build_v1_5_run_evidence_status(run_dir=run_dir)
    stages = _stages(status)
    roles = {row["role"] for row in status["artifacts"]}

    assert "full_flow_closure_readiness" in roles
    assert "full_flow_closure_gaps" in roles
    assert "full_flow_device_closure" in roles
    assert stages["full_flow_closure_readiness"]["status"] == "pass"
    assert "controlled SENCO writes" in stages["full_flow_closure_readiness"]["physical_meaning"]
