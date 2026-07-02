import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_h2o_senco24_repair_plan import main as cli_main
from gas_calibrator.validation.h2o_senco24_repair_plan import (
    H2OSenco24RepairInputs,
    build_h2o_senco24_repair_plan_tables,
    write_h2o_senco24_repair_plan_report,
)


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _inputs(tmp_path: Path) -> H2OSenco24RepairInputs:
    original = tmp_path / "original.csv"
    current = tmp_path / "current.csv"
    policy = tmp_path / "policy.csv"
    payload = tmp_path / "payload.csv"
    residuals = tmp_path / "residuals.csv"
    _write_csv(
        original,
        [
            {"analyzer_device_id": "022", "getco_group": "2", "coefficient_values_json": json.dumps([1, 2, 3, 4, 0, 0])},
            {"analyzer_device_id": "022", "getco_group": "4", "coefficient_values_json": json.dumps([5, 6, 7, 0, 0, 0])},
            {"analyzer_device_id": "022", "getco_group": "6", "coefficient_values_json": json.dumps([1, 1])},
        ],
    )
    _write_csv(
        current,
        [
            {"analyzer_device_id": "022", "getco_group": "2", "coefficient_values_json": json.dumps([10, 20, 30, 40, 0, 0])},
            {"analyzer_device_id": "022", "getco_group": "4", "coefficient_values_json": json.dumps([50, 60, 70, 0, 0, 0])},
            {"analyzer_device_id": "022", "getco_group": "6", "coefficient_values_json": json.dumps([0, 1])},
        ],
    )
    _write_csv(
        policy,
        [
            {
                "analyzer_device_id": "022",
                "candidate_status": "candidate_fit_ready_with_warnings_requires_independent_verification",
                "complete_point_count": 12,
                "rejected_point_count": 0,
                "fit_rmse_mmol": 0.1,
                "fit_max_error_mmol": 0.2,
                "fit_max_abs_relative_error_pct": 0.5,
                "warning_reasons": "existing_GETCO6_nonneutral_final_affine_layer_requires_separate_review",
            }
        ],
    )
    _write_csv(
        payload,
        [
            {
                "analyzer_device_id": "022",
                "senco2_payload_values_json": json.dumps([101, 202, 303, 404, 0, 0]),
                "senco4_payload_values_json": json.dumps([11, 22, 33, 0, 0, 0]),
            }
        ],
    )
    _write_csv(
        residuals,
        [
            {
                "analyzer_device_id": "022",
                "point_run_id": "p001_T0_HG0C_50RH_h2o",
                "residual_role": "rejected_input",
                "reject_reasons": "manual_point_block:first_cold_low_h2o_anchor",
            }
        ],
    )
    return H2OSenco24RepairInputs(
        original_getco_snapshot_csv=original,
        current_getco_snapshot_csv=current,
        candidate_device_policy_csv=policy,
        candidate_payload_preview_csv=payload,
        candidate_residuals_csv=residuals,
        target_device_ids=("022",),
    )


def test_h2o_repair_plan_detects_s6_layer_change_and_target_pair(tmp_path):
    tables = build_h2o_senco24_repair_plan_tables(_inputs(tmp_path))

    history = tables["h2o_senco24_repair_history"][0]
    assert history["senco6_layer_changed_to_target"] is True
    assert history["inferred_layer_state"] == "old_final_affine_layer_removed_requires_matched_main_chain"

    plan = tables["h2o_senco24_repair_plan"][0]
    assert plan["repair_status"] == "ready_for_live_precheck_then_pair_rewrite"
    assert json.loads(plan["target_senco6"]) == [0.0, 1.0]
    assert json.loads(plan["target_senco2"]) == [101.0, 202.0, 303.0, 404.0, 0.0, 0.0]
    assert json.loads(plan["target_senco4"]) == [11.0, 22.0, 33.0, 0.0, 0.0, 0.0]
    assert "p001_T0_HG0C_50RH_h2o" in plan["rejected_points"]
    assert plan["writes_coefficients"] is False
    assert plan["controls_water_or_gas_routes"] is False
    commands = tables["h2o_senco24_repair_command_plan"]
    order = {row["operation"]: row["sequence"] for row in commands}
    assert order["write_senco2"] < order["write_senco4"] < order["align_senco6_final_affine_layer"]


def test_h2o_repair_plan_can_target_non_neutral_s6_without_mixing_layers(tmp_path):
    inputs = _inputs(tmp_path)
    inputs = H2OSenco24RepairInputs(
        original_getco_snapshot_csv=inputs.original_getco_snapshot_csv,
        current_getco_snapshot_csv=inputs.current_getco_snapshot_csv,
        candidate_device_policy_csv=inputs.candidate_device_policy_csv,
        candidate_payload_preview_csv=inputs.candidate_payload_preview_csv,
        candidate_residuals_csv=inputs.candidate_residuals_csv,
        target_senco6=(0.8, 1.04),
        target_device_ids=("022",),
    )

    tables = build_h2o_senco24_repair_plan_tables(inputs)

    plan = tables["h2o_senco24_repair_plan"][0]
    assert plan["repair_status"] == "requires_senco6_layer_alignment_then_pair_rewrite"
    assert json.loads(plan["target_senco6"]) == [0.8, 1.04]
    commands = tables["h2o_senco24_repair_command_plan"]
    align_command = next(row for row in commands if row["operation"] == "align_senco6_final_affine_layer")
    assert "SENCO6,YGAS,FFF" in align_command["command_template"]
    assert "after the SENCO2/SENCO4 main chain" in align_command["reason"]
    order = {row["operation"]: row["sequence"] for row in commands}
    assert order["write_senco2"] < order["write_senco4"] < order["align_senco6_final_affine_layer"]


def test_h2o_repair_plan_writer_and_cli_are_offline_only(tmp_path):
    inputs = _inputs(tmp_path)
    output = tmp_path / "repair"

    outputs = write_h2o_senco24_repair_plan_report(inputs=inputs, output_dir=output)
    assert outputs["markdown"].exists()
    sidecar = json.loads(outputs["database_sidecar"].read_text(encoding="utf-8"))
    assert sidecar["no_write"] is True
    assert sidecar["opens_com_ports"] is False
    assert "coefficient_candidates" in sidecar["database_target_tables"]

    cli_output = tmp_path / "repair_cli"
    rc = cli_main(
        [
            "--original-getco-snapshot-csv",
            str(inputs.original_getco_snapshot_csv),
            "--current-getco-snapshot-csv",
            str(inputs.current_getco_snapshot_csv),
            "--candidate-device-policy-csv",
            str(inputs.candidate_device_policy_csv),
            "--candidate-payload-preview-csv",
            str(inputs.candidate_payload_preview_csv),
            "--candidate-residuals-csv",
            str(inputs.candidate_residuals_csv),
            "--output-dir",
            str(cli_output),
            "--target-device-id",
            "022",
        ]
    )
    assert rc == 0
    assert (cli_output / "h2o_senco24_repair_plan.csv").exists()
