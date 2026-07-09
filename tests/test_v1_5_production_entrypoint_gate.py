import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_production_entrypoint_gate import main as cli_main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_production_entrypoint_gate import (
    build_v1_5_production_entrypoint_gate,
    write_v1_5_production_entrypoint_gate,
)


ROOT = Path(__file__).resolve().parents[1]


def _write_plan(tmp_path: Path, steps: list[dict]) -> Path:
    path = tmp_path / "formal_plan.json"
    path.write_text(
        json.dumps({"schema": "test_v1_5_formal_plan", "steps": steps}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return path


def _by_step(model: dict) -> dict[str, dict]:
    return {row["step_id"]: row for row in model["references"]}


def test_gate_allows_mapped_production_entrypoints(tmp_path: Path) -> None:
    plan = _write_plan(
        tmp_path,
        [
            {
                "step_id": "init",
                "entrypoint": "src/gas_calibrator/tools/run_v1_5_formal_initialization_runner.py",
            },
            {
                "step_id": "co2",
                "runner": "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue",
            },
            {
                "step_id": "h2o",
                "command": "python -m gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue --dry-run",
            },
            {
                "step_id": "status",
                "tool": "src/gas_calibrator/tools/export_v1_5_formal_run_status.py",
            },
        ],
    )

    model = build_v1_5_production_entrypoint_gate(plan_path=plan)
    rows = _by_step(model)

    assert model["manifest"]["status"] == "pass"
    assert model["manifest"]["blocker_count"] == 0
    assert model["manifest"]["review_required_count"] == 0
    assert rows["co2"]["normalized_reference"] == "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py"
    assert rows["co2"]["status"] == "pass"
    assert rows["h2o"]["status"] == "pass"
    assert model["manifest"]["opens_com_ports"] is False
    assert model["manifest"]["controls_water_or_gas_routes"] is False
    assert model["manifest"]["writes_coefficients"] is False
    assert model["manifest"]["connects_postgresql"] is False
    assert model["manifest"]["not_real_acceptance_evidence"] is True


def test_gate_blocks_handoff_root_0624_worker_diagnostic_v1_and_v2(tmp_path: Path) -> None:
    plan = _write_plan(
        tmp_path,
        [
            {"step_id": "handoff", "entrypoint": "_handoff/v1_5_formal_queue_migration_20260624/run.py"},
            {
                "step_id": "root",
                "entrypoint": "D:/gas_calibrator/src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py",
            },
            {
                "step_id": "worker",
                "entrypoint": "src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py",
            },
            {
                "step_id": "diagnostic",
                "entrypoint": "src/gas_calibrator/tools/run_v1_5_open_flow_dynamic_pressure_diagnostic.py",
            },
            {
                "step_id": "legacy_v1",
                "entrypoint": "src/gas_calibrator/tools/run_v1_corrected_autodelivery.py",
            },
            {
                "step_id": "v2",
                "entrypoint": "src/gas_calibrator/v2/core/runners/co2_route_runner.py",
            },
        ],
    )

    model = build_v1_5_production_entrypoint_gate(plan_path=plan)
    rows = _by_step(model)

    assert model["manifest"]["status"] == "blocked"
    assert model["manifest"]["blocker_count"] == 6
    assert rows["handoff"]["policy"] == "handoff_not_production_launcher"
    assert rows["root"]["policy"] == "root_migration_area_not_allowed"
    assert rows["worker"]["policy"] == "sampling_worker_not_top_level"
    assert rows["diagnostic"]["policy"] == "diagnostic_not_production_entry"
    assert rows["legacy_v1"]["policy"] == "legacy_v1_not_v1_5_production_entry"
    assert rows["v2"]["policy"] == "v2_not_v1_5_production_entry"


def test_gate_blocks_0624_queue_source_even_when_embedded_in_command(tmp_path: Path) -> None:
    plan = _write_plan(
        tmp_path,
        [
            {
                "step_id": "co2_source",
                "command": (
                    "python -m gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue "
                    "--queue-source D:/gas_calibrator/_handoff/v1_5_formal_queue_migration_20260624/"
                    "canonical_open_flow_points/co2_runner_queue.csv"
                ),
            }
        ],
    )

    model = build_v1_5_production_entrypoint_gate(plan_path=plan)
    row = model["references"][0]

    assert model["manifest"]["status"] == "blocked"
    assert row["status"] == "blocker"
    assert row["policy"] == "handoff_not_production_launcher"


def test_gate_marks_unknown_v1_5_runnable_as_review_required(tmp_path: Path) -> None:
    plan = _write_plan(
        tmp_path,
        [
            {
                "step_id": "unknown",
                "entrypoint": "src/gas_calibrator/tools/run_v1_5_unmapped_future_runner.py",
            }
        ],
    )

    model = build_v1_5_production_entrypoint_gate(plan_path=plan)
    row = model["references"][0]

    assert model["manifest"]["status"] == "review_required"
    assert model["manifest"]["blocker_count"] == 0
    assert model["manifest"]["review_required_count"] == 1
    assert row["policy"] == "not_in_production_entrypoint_map"


def test_gate_writer_and_cli(tmp_path: Path) -> None:
    plan = _write_plan(
        tmp_path,
        [
            {
                "step_id": "co2",
                "entrypoint": "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py",
            },
            {
                "step_id": "bad_worker",
                "entrypoint": "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_sampling.py",
            },
        ],
    )

    paths = write_v1_5_production_entrypoint_gate(plan_path=plan, output_dir=tmp_path / "out")
    model = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    refs_csv = paths["references"].read_text(encoding="utf-8")
    markdown = paths["markdown"].read_text(encoding="utf-8")

    assert model["manifest"]["status"] == "blocked"
    assert "sampling_worker_not_top_level" in refs_csv
    assert "V1.5 Production Entrypoint Gate" in markdown

    assert cli_main(["--plan-path", str(plan), "--output-dir", str(tmp_path / "cli")]) == 0
    assert cli_main(["--plan-path", str(plan), "--output-dir", str(tmp_path / "cli_block"), "--fail-on-blocker"]) == 2


def test_gate_exporter_is_offline_review_evidence() -> None:
    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/export_v1_5_production_entrypoint_gate.py",
        root=ROOT,
    )

    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert any("production entrypoint gate" in note for note in entry.notes)
