import json

from gas_calibrator.tools.export_v1_5_review_surface import main as review_main
from gas_calibrator.v1_5.parameters.governance import build_parameter_surface
from gas_calibrator.v1_5.review_surface import build_review_surface_model, write_review_surface
from gas_calibrator.v1_5.ui.operation_console import build_operation_console_model


def _formal_workbench(*, package_status="ready_for_reviewer", release_status="draft_only", pressure_missing=False):
    pressure_blockers = ["pressure_quick_check_artifact_missing"] if pressure_missing else []
    cards = [
        {"key": "formal_plan", "status": "pass", "blockers": []},
        {"key": "pressure_quick_check", "status": "fail" if pressure_missing else "pass", "blockers": pressure_blockers},
        {"key": "open_flow_samples", "status": "pass", "blockers": [], "metric": "A 级 20 / 拒绝 0"},
        {"key": "qc_package", "status": package_status, "blockers": pressure_blockers},
        {
            "key": "report_release",
            "status": release_status,
            "blockers": ["uncertainty_budget_not_released"] if release_status == "draft_only" else [],
        },
    ]
    return {
        "schema_version": "v1_5_formal_workbench_v0",
        "run_dir": "D:/runs/demo",
        "preflight_status": "fail" if pressure_missing else "pass",
        "package_status": "blocked" if pressure_missing else package_status,
        "sidecar_only": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "controls_valves_or_pace": False,
        "writes_coefficients": False,
        "cards": cards,
        "report_summary": {
            "release_status": release_status,
            "formal_issue_allowed": release_status == "formal_release_ready",
            "reasons": ["uncertainty_budget_not_released"] if release_status == "draft_only" else [],
            "missing_uncertainty": [],
        },
    }


def test_review_surface_merges_workbench_console_parameters_and_qc_without_device_controls():
    formal = _formal_workbench()
    console = build_operation_console_model(workbench_model=formal, role="operator")
    model = build_review_surface_model(
        formal_workbench=formal,
        operation_console=console,
        parameter_surface=build_parameter_surface(),
        advanced_qc={"root_cause": {"status": "pass", "root_cause_codes": [], "summary": "高级 QC 未发现异常。"}},
    )

    assert model["sidecar_only"] is True
    assert model["opens_com_ports"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["controls_valves_or_pace"] is False
    assert model["writes_coefficients"] is False
    assert model["overall_status"] == "draft_only"
    assert model["parameter_summary"]["device_write_enabled"] is False
    assert model["parameter_summary"]["high_risk_parameters_hidden_by_default"] is True
    assert any("uncertainty" in action for action in model["next_actions"])


def test_review_surface_blocks_when_pressure_quick_check_is_missing():
    model = build_review_surface_model(formal_workbench=_formal_workbench(pressure_missing=True))

    assert model["overall_status"] == "blocked"
    assert "pressure_quick_check_artifact_missing" in model["evidence_summary"]["blockers"]
    assert any("压力通道快速验证" in action for action in model["next_actions"])


def test_review_surface_blocks_on_advanced_qc_root_cause():
    model = build_review_surface_model(
        formal_workbench=_formal_workbench(release_status="review_ready"),
        advanced_qc={
            "root_cause": {
                "status": "reject_point",
                "root_cause_codes": ["real_moisture_release"],
                "summary": "露点未稳定，H2O dry ppmv 持续上升。",
            }
        },
    )

    assert model["overall_status"] == "blocked"
    assert model["advanced_qc_summary"]["status"] == "reject_point"
    assert "real_moisture_release" in model["advanced_qc_summary"]["blockers"]
    assert any("气路反湿" in action for action in model["next_actions"])


def test_review_surface_writer_and_cli(tmp_path):
    formal = _formal_workbench()
    workbench_path = tmp_path / "v1_5_formal_workbench.json"
    workbench_path.write_text(json.dumps(formal, ensure_ascii=False), encoding="utf-8")

    outputs = write_review_surface(
        output_dir=tmp_path / "review",
        formal_workbench=formal,
        parameter_surface=build_parameter_surface(),
    )
    assert outputs["model"].exists()
    assert outputs["html"].exists()
    assert outputs["markdown"].exists()
    model = json.loads(outputs["model"].read_text(encoding="utf-8"))
    assert model["overall_status"] == "draft_only"

    rc = review_main(
        [
            "--output-dir",
            str(tmp_path / "cli_review"),
            "--formal-workbench-json",
            str(workbench_path),
            "--role",
            "reviewer",
        ]
    )
    assert rc == 0
    assert (tmp_path / "cli_review" / "v1_5_review_surface.html").exists()
    assert (tmp_path / "cli_review" / "v1_5_review_surface.json").exists()
