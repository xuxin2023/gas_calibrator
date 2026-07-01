import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_formal_run_status import main as export_status_main
from gas_calibrator.validation.v1_5_formal_run_status import (
    build_v1_5_formal_run_status,
    render_v1_5_formal_run_status_markdown,
    write_v1_5_formal_run_status_outputs,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _seed_ready_run(root: Path) -> None:
    _write_json(
        root / "initialization" / "v1_5_initialization_readiness.json",
        {
            "schema": "v1_5_initialization_readiness_v1",
            "readiness_status": "ready_for_open_flow_main_calibration",
        },
    )
    _write_json(
        root / "identity" / "v1_5_getco_identity_readiness.json",
        {
            "schema": "v1_5_getco_identity_readiness_v1",
            "overall_status": "identity_getco_ready_for_auxiliary_neutralization",
            "traceability_review_required": False,
        },
    )
    _write_json(
        root / "pre_gas" / "v1_5_pre_gas_readiness.json",
        {
            "schema": "v1_5_pre_gas_readiness_v1",
            "overall_status": "ready_for_open_flow_from_sidecar_evidence",
        },
    )
    _write_json(
        root / "run_evidence" / "v1_5_run_evidence_status.json",
        {
            "schema": "v1_5_run_evidence_status_v1",
            "overall_status": "ready_for_reviewer",
            "stage_statuses": [
                {"stage_id": "pressure_quick_check", "status": "pass"},
                {"stage_id": "co2_open_flow", "status": "pass"},
                {"stage_id": "h2o_open_flow", "status": "pass"},
                {"stage_id": "candidate_review", "status": "pass"},
                {"stage_id": "post_run_coefficient_executor", "status": "pass"},
                {"stage_id": "post_write_reverification", "status": "pass"},
            ],
        },
    )
    _write_json(
        root / "closure" / "v1_5_full_flow_closure_readiness.json",
        {
            "schema": "v1_5_full_flow_closure_readiness_v1",
            "overall_status": "ready_for_controlled_write_review",
            "release_status": "ready_for_formal_release",
            "gaps": [],
        },
    )
    _write_json(
        root / "archive" / "v1_5_formal_archive_closure_index.json",
        {
            "schema": "v1_5_formal_archive_closure_v1",
            "overall_status": "ready",
            "package_status": "ready",
            "database": {"mode": "import", "database_imported": True},
            "identity_getco_traceability": {
                "status": "ready",
                "ready_for_archive_release": True,
                "traceability_review_required": False,
            },
        },
    )


def test_formal_run_status_reports_ready_release_without_touching_devices(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run"
    _seed_ready_run(run_dir)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gate_statuses = {row["gate_id"]: row["status"] for row in model["gates"]}

    assert model["schema"] == "v1_5_formal_run_status_v1"
    assert model["overall_status"] == "formal_release_ready"
    assert model["current_stage"] == "complete"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is True
    assert model["can_continue_physical_flow"] is True
    assert model["gaps"] == []
    assert gate_statuses["pressure_senco9_pre_open_flow"] == "ready"
    assert gate_statuses["co2_open_flow_mature_queue"] == "ready"
    assert gate_statuses["h2o_open_flow_mature_queue"] == "ready"
    assert model["physical_boundaries"] == {
        "offline_status_only": True,
        "opens_com_ports": False,
        "connects_postgresql": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "not_real_acceptance_evidence": True,
    }


def test_formal_run_status_sn_review_blocks_release_not_physical_flow(tmp_path: Path) -> None:
    run_dir = tmp_path / "sn_review"
    _seed_ready_run(run_dir)
    _write_json(
        run_dir / "identity" / "v1_5_getco_identity_readiness.json",
        {
            "schema": "v1_5_getco_identity_readiness_v1",
            "overall_status": "identity_getco_ready_for_auxiliary_neutralization",
            "traceability_review_required": True,
        },
    )

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}

    assert model["overall_status"] == "review_required"
    assert model["current_stage"] == "identity_getco_sn_traceability"
    assert model["formal_release_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert gates["identity_getco_sn_traceability"]["status"] == "review_required"
    assert gates["identity_getco_sn_traceability"]["blocks_release"] is True
    assert gates["identity_getco_sn_traceability"]["blocks_physical_flow"] is False


def test_formal_run_status_empty_run_dir_is_offline_todo_only(tmp_path: Path) -> None:
    run_dir = tmp_path / "empty"
    run_dir.mkdir()

    model = build_v1_5_formal_run_status(run_dir=run_dir)

    assert model["overall_status"] == "in_progress"
    assert model["current_stage"] == "initialization_readiness"
    assert model["formal_release_allowed"] is False
    assert model["can_continue_physical_flow"] is False
    assert model["physical_boundaries"]["opens_com_ports"] is False
    assert model["physical_boundaries"]["writes_coefficients"] is False
    assert model["physical_boundaries"]["connects_postgresql"] is False


def test_formal_run_status_writes_json_markdown_and_csv(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_for_export"
    output_dir = tmp_path / "out"
    _seed_ready_run(run_dir)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    outputs = write_v1_5_formal_run_status_outputs(model, output_dir)
    markdown = render_v1_5_formal_run_status_markdown(model)

    assert "formal_release_allowed: `True`" in markdown
    assert Path(outputs["json_path"]).exists()
    assert Path(outputs["markdown_path"]).exists()
    assert Path(outputs["gates_csv_path"]).exists()
    assert Path(outputs["gaps_csv_path"]).exists()
    exported = json.loads(Path(outputs["json_path"]).read_text(encoding="utf-8"))
    assert exported["overall_status"] == "formal_release_ready"
    with Path(outputs["gates_csv_path"]).open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert {row["gate_id"] for row in rows} >= {
        "initialization_readiness",
        "formal_archive_database_release",
    }


def test_formal_run_status_cli_exports_rollup(tmp_path: Path, capsys) -> None:
    run_dir = tmp_path / "ready_cli"
    output_dir = tmp_path / "cli_out"
    _seed_ready_run(run_dir)

    rc = export_status_main(["--run-dir", str(run_dir), "--output-dir", str(output_dir)])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert rc == 0
    assert payload["overall_status"] == "formal_release_ready"
    assert payload["physical_boundaries"]["opens_com_ports"] is False
    assert (output_dir / "v1_5_formal_run_status.json").exists()
    assert (output_dir / "v1_5_formal_run_status_gates.csv").exists()
