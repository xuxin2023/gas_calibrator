import json

from gas_calibrator.tools.export_v1_5_operation_console import main as console_main
from gas_calibrator.v1_5.ui.operation_console import (
    PAGE_DEFINITIONS,
    build_operation_console_model,
    write_operation_console,
)


def _workbench_model():
    return {
        "run_dir": "D:/runs/demo",
        "preflight_status": "pass",
        "package_status": "ready_for_reviewer",
        "cards": [
            {"key": "formal_plan", "status": "pass", "blockers": []},
            {"key": "pressure_quick_check", "status": "pass", "blockers": []},
            {"key": "open_flow_samples", "status": "pass", "blockers": [], "metric": "A 级 20 / 拒绝 0"},
            {"key": "qc_package", "status": "ready_for_reviewer", "blockers": []},
            {"key": "report_release", "status": "draft_only", "blockers": ["uncertainty_budget_not_released"]},
        ],
    }


def test_operation_console_has_eight_read_only_pages_and_no_device_controls():
    model = build_operation_console_model(workbench_model=_workbench_model(), role="operator")

    assert len(PAGE_DEFINITIONS) == 8
    assert [page["key"] for page in model["pages"]] == [
        "dashboard",
        "plan_select",
        "precheck",
        "pressure_channel_verify",
        "open_flow_sampling",
        "qc_review",
        "report_review",
        "approval",
    ]
    assert model["sidecar_only"] is True
    assert model["opens_com_ports"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["controls_valves_or_pace"] is False
    assert model["writes_coefficients"] is False
    assert model["role_permissions"]["write_senco"] is False
    assert all(page["read_only"] for page in model["pages"])
    assert all(page["device_control_enabled"] is False for page in model["pages"])


def test_operation_console_writer_and_cli(tmp_path):
    outputs = write_operation_console(output_dir=tmp_path / "console", workbench_model=_workbench_model())
    assert outputs["model"].exists()
    assert outputs["html"].exists()
    model = json.loads(outputs["model"].read_text(encoding="utf-8"))
    assert model["pages"][3]["key"] == "pressure_channel_verify"

    workbench_path = tmp_path / "workbench.json"
    workbench_path.write_text(json.dumps(_workbench_model(), ensure_ascii=False), encoding="utf-8")
    rc = console_main(
        [
            "--output-dir",
            str(tmp_path / "cli_console"),
            "--workbench-json",
            str(workbench_path),
            "--role",
            "engineer",
        ]
    )
    assert rc == 0
    assert (tmp_path / "cli_console" / "v1_5_operation_console.html").exists()
