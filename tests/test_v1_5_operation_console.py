import json

from gas_calibrator.tools.export_v1_5_operation_console import main as console_main
from gas_calibrator.v1_5.ui.operation_console import (
    PAGE_DEFINITIONS,
    build_operation_console_model,
    write_operation_console,
)


def _workbench_model():
    return {
        "run_id": "RUN-UI-001",
        "run_dir": "D:/runs/demo",
        "preflight_status": "pass",
        "package_status": "ready_for_reviewer",
        "cards": [
            {"key": "formal_plan", "status": "pass", "blockers": []},
            {"key": "pressure_quick_check", "status": "pass", "blockers": []},
            {"key": "open_flow_samples", "status": "pass", "blockers": [], "metric": "A 级 20 / 拒绝 0"},
            {"key": "qc_package", "status": "ready_for_reviewer", "blockers": []},
            {"key": "post_write_reverification", "status": "pass", "blockers": []},
            {"key": "report_release", "status": "draft_only", "blockers": ["uncertainty_budget_not_released"]},
        ],
    }


def _run_evidence_status():
    return {
        "run_id": "RUN-UI-001",
        "run_dir": "D:/runs/demo",
        "stage_statuses": [
            {
                "stage_id": "full_flow_contract_gate",
                "title": "V1.5 正式流程合同",
                "status": "pass",
                "reason": "physical boundaries frozen",
                "artifact_count": 1,
            },
            {
                "stage_id": "pressure_quick_check",
                "title": "压力通道快速验证",
                "status": "pass",
                "reason": "COM22 agreement evidence present",
                "artifact_count": 2,
            },
            {
                "stage_id": "co2_open_flow",
                "title": "CO2 开放流通采样",
                "status": "pass",
                "reason": "clean A-grade samples present",
                "artifact_count": 4,
            },
            {
                "stage_id": "h2o_open_flow",
                "title": "H2O 开放流通采样",
                "status": "pass",
                "reason": "dewpoint evidence present",
                "artifact_count": 4,
            },
            {
                "stage_id": "reports",
                "title": "正式报告",
                "status": "partial",
                "reason": "formal report draft exists",
                "artifact_count": 2,
            },
            {
                "stage_id": "database_import",
                "title": "数据库归档",
                "status": "not_attempted",
                "reason": "dry-run only",
                "artifact_count": 0,
            },
        ],
    }


def _calibration_capability():
    return {
        "schema": "v1_5_calibration_capability_v1",
        "status": "demonstrated_calibratable_for_verified_scope",
        "method_backbone_ready": True,
        "formal_release_ready": False,
        "verification_rollup": {
            "available": True,
            "status": "pass",
            "row_count": 8,
            "device_ids": ["022", "030", "033", "051"],
            "components": ["co2", "h2o"],
            "max_abs_error_pct": 0.83,
        },
    }


def _archive_index():
    return {
        "run_id": "RUN-UI-001",
        "run_dir": "D:/runs/demo",
        "database": {"mode": "dry_run", "database_imported": False},
    }


def test_operation_console_has_eight_read_only_pages_and_no_device_controls():
    model = build_operation_console_model(
        workbench_model=_workbench_model(),
        run_evidence_status=_run_evidence_status(),
        calibration_capability=_calibration_capability(),
        archive_index=_archive_index(),
        role="operator",
    )

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
    assert model["cannot_write_senco"] is True
    assert model["cannot_clear_senco"] is True
    assert model["role_permissions"]["write_senco"] is False
    assert model["calibration_capability_label"] == "已证明可校准范围"
    assert all(page["read_only"] for page in model["pages"])
    assert all(page["device_control_enabled"] is False for page in model["pages"])
    assert model["source_evidence"] == {
        "has_workbench_model": True,
        "has_run_evidence_status": True,
        "has_calibration_capability": True,
        "has_archive_index": True,
    }


def test_operation_console_surfaces_physical_status_and_release_blockers():
    model = build_operation_console_model(
        workbench_model=_workbench_model(),
        run_evidence_status=_run_evidence_status(),
        calibration_capability=_calibration_capability(),
        archive_index=_archive_index(),
        role="reviewer",
    )
    summary = {row["key"]: row for row in model["summary_cards"]}
    assert summary["method_backbone"]["status"] == "pass"
    assert summary["verification"]["detail"] == "最大相对误差 0.83%"
    assert summary["formal_release"]["status"] == "demonstrated_calibratable_for_verified_scope"
    assert summary["database"]["status"] == "dry_run"

    report_page = next(page for page in model["pages"] if page["key"] == "report_review")
    assert report_page["status"] == "partial"
    assert any("正式报告" in blocker for blocker in report_page["blockers"])
    assert any("uncertainty_budget_not_released" in blocker for blocker in report_page["blockers"])


def test_operation_console_writer_and_cli(tmp_path):
    outputs = write_operation_console(
        output_dir=tmp_path / "console",
        workbench_model=_workbench_model(),
        run_evidence_status=_run_evidence_status(),
        calibration_capability=_calibration_capability(),
        archive_index=_archive_index(),
    )
    assert outputs["model"].exists()
    assert outputs["html"].exists()
    model = json.loads(outputs["model"].read_text(encoding="utf-8"))
    assert model["pages"][3]["key"] == "pressure_channel_verify"
    html_text = outputs["html"].read_text(encoding="utf-8-sig")
    assert "V1.5 正式校准操作台" in html_text
    assert "压力通道验证" in html_text
    assert "开放流通采样" in html_text
    assert "不打开串口" in html_text

    workbench_path = tmp_path / "workbench.json"
    status_path = tmp_path / "run_status.json"
    capability_path = tmp_path / "capability.json"
    archive_path = tmp_path / "archive.json"
    workbench_path.write_text(json.dumps(_workbench_model(), ensure_ascii=False), encoding="utf-8")
    status_path.write_text(json.dumps(_run_evidence_status(), ensure_ascii=False), encoding="utf-8")
    capability_path.write_text(json.dumps(_calibration_capability(), ensure_ascii=False), encoding="utf-8")
    archive_path.write_text(json.dumps(_archive_index(), ensure_ascii=False), encoding="utf-8")
    rc = console_main(
        [
            "--output-dir",
            str(tmp_path / "cli_console"),
            "--workbench-json",
            str(workbench_path),
            "--run-evidence-status-json",
            str(status_path),
            "--calibration-capability-json",
            str(capability_path),
            "--archive-index-json",
            str(archive_path),
            "--role",
            "engineer",
        ]
    )
    assert rc == 0
    cli_html = tmp_path / "cli_console" / "v1_5_operation_console.html"
    assert cli_html.exists()
    assert "设备预检" in cli_html.read_text(encoding="utf-8-sig")
