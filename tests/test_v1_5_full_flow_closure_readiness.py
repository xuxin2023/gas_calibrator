import csv
import json

import pytest

from gas_calibrator.tools.export_v1_5_full_flow_closure_readiness import (
    main as export_closure_main,
)
from gas_calibrator.validation.v1_5_full_flow_closure_readiness import (
    build_v1_5_full_flow_closure_readiness,
    render_v1_5_full_flow_closure_readiness_markdown,
    write_v1_5_full_flow_closure_readiness_outputs,
)


pytestmark = pytest.mark.v1_5_formal_gate


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_csv(path, rows, fieldnames=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    keys = list(fieldnames or [])
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return path


def _seed_ready_closure(root, *, devices=("077", "084")):
    _write_json(
        root / "v1_5_full_flow_plan.json",
        {"schema": "v1_5_full_calibration_flow_plan_v0", "steps": []},
    )
    _write_json(
        root / "v1_5_run_evidence_status.json",
        {"schema": "v1_5_run_evidence_status_v1", "overall_status": "ready_for_reviewer"},
    )
    write_package = []
    reverify_plan = []
    device_rows = []
    for device_id in devices:
        write_package.append(
            {
                "device_id": device_id,
                "component": "co2_senco1_senco3",
                "requires_explicit_authorization": "true",
            }
        )
        reverify_plan.append(
            {
                "device_id": device_id,
                "component": "co2",
                "route_contract": "gas route must remain open during sampling",
            }
        )
        device_rows.append(
            {
                "device_id": device_id,
                "overall_status": "ready_for_controlled_write_review",
                "pressure_status": "ready",
                "temperature_status": "ready",
                "co2_status": "candidate_ready_co2",
                "h2o_status": "candidate_ready_h2o",
                "output_trim_status": "trim_review_ready",
                "blockers": [],
                "next_action": "review controlled write package",
            }
        )
    executor = {
        "schema": "v1_5_post_run_coefficient_executor_v1",
        "overall_status": "ready_for_next_automatic_step",
        "devices": device_rows,
        "controlled_write_package": write_package,
        "post_write_reverification_plan": reverify_plan,
        "archive_gap_list": [],
    }
    executor_dir = root / "post_run_coefficient_executor"
    _write_json(executor_dir / "executor_manifest.json", executor)
    _write_csv(executor_dir / "controlled_write_package.csv", write_package)
    _write_csv(executor_dir / "post_write_reverification_plan.csv", reverify_plan)
    _write_csv(executor_dir / "archive_gap_list.csv", [], fieldnames=["scope", "item", "status"])
    _write_json(
        root / "formal_archive_closure_from_full_chain" / "v1_5_formal_archive_closure_index.json",
        {"overall_status": "ready"},
    )
    return executor_dir


def test_full_flow_closure_readiness_ready_without_touching_devices(tmp_path):
    run_dir = tmp_path / "run"
    _seed_ready_closure(run_dir)

    model = build_v1_5_full_flow_closure_readiness(run_dir=run_dir)
    stages = {row["stage_id"]: row for row in model["stage_statuses"]}

    assert model["overall_status"] == "ready_for_controlled_write_review"
    assert model["physical_boundaries"]["opens_com_ports"] is False
    assert model["physical_boundaries"]["controls_water_or_gas_routes"] is False
    assert model["physical_boundaries"]["writes_coefficients"] is False
    assert model["workflow_contract"]["sample_window_requires_route_open"] is True
    assert model["workflow_contract"]["fit_label_does_not_exclude_points_by_default"] is True
    assert model["workflow_contract"]["co2_zero_anchor_distinct_from_h2o_dry_anchor"] is True
    assert stages["controlled_write_package"]["status"] == "ready"
    assert stages["post_write_reverification_plan"]["status"] == "ready"
    assert model["gaps"] == []
    assert {row["device_id"] for row in model["devices"]} == {"077", "084"}


def test_full_flow_closure_readiness_blocks_missing_executor(tmp_path):
    run_dir = tmp_path / "blocked"
    _write_json(
        run_dir / "v1_5_full_flow_plan.json",
        {"schema": "v1_5_full_calibration_flow_plan_v0"},
    )
    _write_json(
        run_dir / "v1_5_run_evidence_status.json",
        {"overall_status": "ready_for_reviewer"},
    )

    model = build_v1_5_full_flow_closure_readiness(run_dir=run_dir)
    stages = {row["stage_id"]: row for row in model["stage_statuses"]}

    assert model["overall_status"] == "blocked"
    assert stages["post_run_coefficient_executor"]["status"] == "blocked"
    assert any(row["item"] == "post_run_coefficient_executor" for row in model["gaps"])


def test_full_flow_closure_readiness_keeps_device_blockers_per_device(tmp_path):
    run_dir = tmp_path / "partial"
    executor_dir = _seed_ready_closure(run_dir, devices=("077", "079"))
    executor = json.loads((executor_dir / "executor_manifest.json").read_text(encoding="utf-8"))
    executor["devices"][1]["overall_status"] = "blocked_or_partial"
    executor["devices"][1]["blockers"] = ["factory_signal_health_failed"]
    executor["devices"][1]["next_action"] = "repair optical signal before write"
    (executor_dir / "executor_manifest.json").write_text(
        json.dumps(executor, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    model = build_v1_5_full_flow_closure_readiness(run_dir=run_dir)
    devices = {row["device_id"]: row for row in model["devices"]}

    assert model["overall_status"] == "partial"
    assert devices["077"]["overall_status"] == "ready_for_controlled_write_review"
    assert devices["079"]["overall_status"] == "blocked_or_partial"
    assert any(row["scope"] == "device" and row["item"] == "079" for row in model["gaps"])


def test_full_flow_closure_readiness_writes_json_markdown_and_csv(tmp_path):
    run_dir = tmp_path / "run"
    out = tmp_path / "closure"
    _seed_ready_closure(run_dir, devices=("091",))

    model = build_v1_5_full_flow_closure_readiness(run_dir=run_dir)
    paths = write_v1_5_full_flow_closure_readiness_outputs(model, out)
    markdown = paths["readiness_markdown"].read_text(encoding="utf-8")

    assert paths["readiness_json"].exists()
    assert paths["gaps"].exists()
    assert paths["devices"].exists()
    assert "V1.5 全流程离线闭环验收" in markdown
    assert "采样窗口必须在阀门打开时取得" in markdown
    with paths["devices"].open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["device_id"] == "091"


def test_full_flow_closure_readiness_cli_exports_offline_review(tmp_path, capsys):
    run_dir = tmp_path / "run"
    out = tmp_path / "closure_cli"
    _seed_ready_closure(run_dir, devices=("001",))

    rc = export_closure_main(["--run-dir", str(run_dir), "--output-dir", str(out)])
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["status"] == "ready_for_controlled_write_review"
    assert payload["physical_boundaries"]["writes_coefficients"] is False
    assert (out / "v1_5_full_flow_closure_readiness.json").exists()
    assert (out / "v1_5_full_flow_device_closure.csv").exists()


def test_full_flow_closure_readiness_markdown_names_physical_boundaries(tmp_path):
    run_dir = tmp_path / "run"
    _seed_ready_closure(run_dir)

    markdown = render_v1_5_full_flow_closure_readiness_markdown(
        build_v1_5_full_flow_closure_readiness(run_dir=run_dir)
    )

    assert "`opens_com_ports`: `False`" in markdown
    assert "CO2 零气低端锚点和 H2O 干气低水锚点物理意义不同" in markdown
