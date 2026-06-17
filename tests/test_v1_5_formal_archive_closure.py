import json

from gas_calibrator.tools.run_v1_5_formal_archive_closure import main as closure_main
from gas_calibrator.validation.v1_5_canonical_evidence import write_canonical_v1_5_evidence_package
from gas_calibrator.validation.v1_5_formal_archive_closure import build_v1_5_formal_archive_closure


def _write_contract(path):
    payload = {
        "schema": "v1_5_formal_flow_contract_v1",
        "status": "pass",
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


def _reviewed_standard_gases():
    return {
        "standard_gases": [
            {
                "component": "co2",
                "cylinder_id": "CO2-900",
                "certificate_id": "CO2-CERT-REVIEWED",
                "certificate_value": 900.0,
                "certificate_uncertainty": 0.9,
                "valid_until": "2027-01-01",
                "supplier": "reviewed-standard-lab",
                "certificate_hash": "reviewed-co2-cert-hash",
            },
            {
                "component": "h2o",
                "cylinder_id": "H2O-GEN-REVIEWED",
                "certificate_id": "H2O-CERT-REVIEWED",
                "certificate_value": 0.5,
                "certificate_uncertainty": 0.01,
                "valid_until": "2027-01-01",
                "supplier": "reviewed-standard-lab",
                "certificate_hash": "reviewed-h2o-cert-hash",
            },
        ],
    }


def test_formal_archive_closure_generates_reports_bundle_traceability_and_dry_run_db(tmp_path):
    canonical = write_canonical_v1_5_evidence_package(
        tmp_path / "canonical",
        include_reports=False,
    )
    run_dir = canonical["root"] / "run"
    closure_dir = run_dir / "formal_archive_closure"
    contract_path = _write_contract(run_dir / "v1_5_formal_flow_contract.json")

    result = build_v1_5_formal_archive_closure(
        run_dir=run_dir,
        plan_json=canonical["plan"],
        pressure_reference_json=canonical["pressure_reference"],
        contract_json=contract_path,
        output_dir=closure_dir,
        today="2026-05-24",
        report_no="RPT-CLOSURE-001",
        reviewer="reviewer-a",
        approver="approver-a",
        location="lab-a",
        calibration_date="2026-05-24",
        db_mode="dry_run",
    )

    index = result["index"]
    paths = result["paths"]
    assert index["schema"] == "v1_5_formal_archive_closure_v1"
    assert index["physical_boundaries"]["opens_com_ports"] is False
    assert index["physical_boundaries"]["controls_water_or_gas_routes"] is False
    assert index["physical_boundaries"]["controls_valves_or_pace"] is False
    assert index["physical_boundaries"]["writes_coefficients"] is False
    assert index["database"]["mode"] == "dry_run"
    assert index["database"]["database_imported"] is False
    assert index["calibration_capability"]["method_backbone_ready"] is True
    assert index["calibration_capability"]["formal_release_ready"] is False
    assert index["traceability_checks"]["has_raw_samples"] is True
    assert index["traceability_checks"]["has_run_evidence_status"] is True
    assert index["traceability_checks"]["has_water_route_traceability"] is True

    final_bundle = json.loads(paths["evidence_bundle"].read_text(encoding="utf-8"))
    report_types = {row["report_type"] for row in final_bundle["tables"]["reports"]}
    assert {"run_report", "technical_report", "formal_calibration_report", "report_model"}.issubset(
        report_types
    )
    report_roles = {
        row["artifact_role"]
        for row in final_bundle["tables"]["sample_files"]
        if str(row.get("path") or "").endswith((".md", ".docx", ".pdf", ".json"))
    }
    assert {"run_report", "technical_report", "formal_calibration_report", "report_model"}.issubset(
        report_roles
    )
    assert {"run_evidence_status", "calibration_capability", "calibration_capability_report"}.issubset(
        report_roles
    )

    traceability = json.loads(paths["traceability_summary"].read_text(encoding="utf-8"))
    assert traceability["traceability_checks"]["all_required_artifacts_have_sha256"] is True
    assert any(row["report_type"] == "formal_calibration_report" for row in traceability["reports"])
    assert paths["archive_index_json"].exists()
    assert paths["archive_index_markdown"].exists()
    assert paths["calibration_capability_json"].exists()
    assert paths["calibration_capability_markdown"].exists()
    capability = json.loads(paths["calibration_capability_json"].read_text(encoding="utf-8"))
    assert capability["physical_boundaries"]["opens_com_ports"] is False
    index_text = paths["archive_index_markdown"].read_text(encoding="utf-8")
    assert "V1.5 正式归档闭环索引" in index_text
    assert "控制气路/水路" not in index_text
    assert "controls_water_or_gas_routes" in index_text


def test_formal_archive_closure_can_bind_reviewed_standard_gas_snapshot(tmp_path):
    canonical = write_canonical_v1_5_evidence_package(
        tmp_path / "canonical_reviewed_gases",
        include_reports=False,
    )
    run_dir = canonical["root"] / "run"
    closure_dir = run_dir / "formal_archive_closure_reviewed_gases"
    contract_path = _write_contract(run_dir / "v1_5_formal_flow_contract.json")
    source_plan = json.loads(canonical["plan"].read_text(encoding="utf-8"))
    source_plan.pop("standard_gases", None)
    plan_without_gases = run_dir / "formal_plan_without_standard_gases.json"
    plan_without_gases.write_text(json.dumps(source_plan, ensure_ascii=False, indent=2), encoding="utf-8")
    gases_path = run_dir / "standard_gases_reviewed.json"
    gases_path.write_text(json.dumps(_reviewed_standard_gases(), ensure_ascii=False), encoding="utf-8")

    result = build_v1_5_formal_archive_closure(
        run_dir=run_dir,
        plan_json=plan_without_gases,
        pressure_reference_json=canonical["pressure_reference"],
        standard_gases_json=gases_path,
        contract_json=contract_path,
        output_dir=closure_dir,
        today="2026-05-24",
        report_no="RPT-CLOSURE-GASES",
        db_mode="dry_run",
    )

    index = result["index"]
    paths = result["paths"]
    traceability = json.loads(paths["traceability_summary"].read_text(encoding="utf-8"))
    final_bundle = json.loads(paths["evidence_bundle"].read_text(encoding="utf-8"))
    gases = {row["component"]: row for row in final_bundle["tables"]["standard_gases"]}

    assert index["source_plan_json"] == str(plan_without_gases.resolve())
    assert index["standard_gases_json"] == str(gases_path.resolve())
    assert paths["standard_gases_reviewed_snapshot"].exists()
    assert paths["formal_plan_with_standard_gases"].exists()
    assert traceability["traceability_checks"]["has_standard_gas_traceability"] is True
    assert traceability["traceability_checks"]["has_water_route_traceability"] is True
    assert gases["co2"]["certificate_hash"] == "reviewed-co2-cert-hash"
    assert gases["h2o"]["cylinder_id"] == "H2O-GEN-REVIEWED"


def test_formal_archive_closure_cli_keeps_output_inside_run_dir(tmp_path):
    canonical = write_canonical_v1_5_evidence_package(
        tmp_path / "canonical_cli",
        include_reports=False,
    )
    run_dir = canonical["root"] / "run"
    closure_dir = run_dir / "formal_archive_closure_cli"
    contract_path = _write_contract(run_dir / "v1_5_formal_flow_contract.json")

    rc = closure_main(
        [
            "--run-dir",
            str(run_dir),
            "--plan-json",
            str(canonical["plan"]),
            "--pressure-reference-json",
            str(canonical["pressure_reference"]),
            "--contract-json",
            str(contract_path),
            "--output-dir",
            str(closure_dir),
            "--today",
            "2026-05-24",
            "--db-mode",
            "dry-run",
        ]
    )

    assert rc == 0
    index_path = closure_dir / "v1_5_formal_archive_closure_index.json"
    bundle_path = closure_dir / "evidence_bundle.json"
    assert index_path.exists()
    assert bundle_path.exists()
    index = json.loads(index_path.read_text(encoding="utf-8"))
    assert index["database"]["database_imported"] is False
    assert index["reports"]["formal_calibration_report_markdown"].endswith(
        "formal_calibration_report.md"
    )
    assert index["calibration_capability"]["method_backbone_ready"] is True


def test_formal_archive_closure_excludes_previous_archive_closure_dirs(tmp_path):
    canonical = write_canonical_v1_5_evidence_package(
        tmp_path / "canonical_repeat",
        include_reports=False,
    )
    run_dir = canonical["root"] / "run"
    contract_path = _write_contract(run_dir / "v1_5_formal_flow_contract.json")

    first_dir = run_dir / "archive_first"
    build_v1_5_formal_archive_closure(
        run_dir=run_dir,
        plan_json=canonical["plan"],
        pressure_reference_json=canonical["pressure_reference"],
        contract_json=contract_path,
        output_dir=first_dir,
        today="2026-05-24",
        report_no="RPT-CLOSURE-FIRST",
        db_mode="dry_run",
    )

    second_dir = run_dir / "archive_second"
    result = build_v1_5_formal_archive_closure(
        run_dir=run_dir,
        plan_json=canonical["plan"],
        pressure_reference_json=canonical["pressure_reference"],
        contract_json=contract_path,
        output_dir=second_dir,
        today="2026-05-24",
        report_no="RPT-CLOSURE-SECOND",
        db_mode="dry_run",
    )

    final_bundle = json.loads(result["paths"]["evidence_bundle"].read_text(encoding="utf-8"))
    indexed_paths = [
        str(row.get("path") or "")
        for table in ("sample_files", "reports")
        for row in final_bundle["tables"][table]
    ]
    assert not any(str(first_dir) in path for path in indexed_paths)
    assert any(str(second_dir / "reports") in path for path in indexed_paths)
