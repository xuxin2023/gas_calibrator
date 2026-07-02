import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_co2_senco13_repair_plan import main as cli_main
from gas_calibrator.validation.co2_senco13_repair_plan import (
    Senco13RepairInputs,
    build_co2_senco13_repair_plan_tables,
    write_co2_senco13_repair_plan_report,
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


def _inputs(tmp_path: Path) -> Senco13RepairInputs:
    original = tmp_path / "original.csv"
    first_pair = tmp_path / "first_pair.csv"
    latest_s1 = tmp_path / "latest_s1.csv"
    recalc = tmp_path / "recalc.csv"
    preclear_s5 = tmp_path / "preclear_s5.csv"
    postclear_s5 = tmp_path / "postclear_s5.csv"
    _write_csv(
        original,
        [
            {"analyzer_device_id": "022", "getco_group": "1", "coefficient_values_json": json.dumps([1, 2, 3, 4, 0, 0])},
            {"analyzer_device_id": "022", "getco_group": "3", "coefficient_values_json": json.dumps([5, 6, 7, 0, 0, 0])},
        ],
    )
    _write_csv(
        first_pair,
        [
            {
                "analyzer_device_id": "022",
                "senco1_readback": json.dumps([10, 20, 30, 40, 0, 0]),
                "senco3_readback": json.dumps([50, 60, 70, 0, 0, 0]),
            }
        ],
    )
    _write_csv(
        latest_s1,
        [
            {
                "analyzer_device_id": "022",
                "senco1_readback": json.dumps([100, 200, 300, 400, 0, 0]),
                "senco3_readback": json.dumps([50, 60, 70, 0, 0, 0]),
            }
        ],
    )
    _write_csv(
        recalc,
        [
            {
                "device_id": "022",
                "scenario": "force_neutral_senco5",
                "rounded_primary_payload_json": json.dumps([101, 202, 303, 404, 0, 0]),
                "rounded_secondary_payload_json": json.dumps([11, 22, 33, 0, 0, 0]),
                "rounded_rmse_ppm": 1.2,
                "rounded_max_abs_error_ppm": 2.3,
                "fit_point_count": 8,
                "fit_strategy": "absolute_replace_main_chain",
            }
        ],
    )
    _write_csv(
        preclear_s5,
        [{"analyzer_device_id": "022", "getco_group": "5", "coefficient_values_json": json.dumps([11, 0.66])}],
    )
    _write_csv(
        postclear_s5,
        [{"analyzer_device_id": "022", "getco_group": "5", "coefficient_values_json": json.dumps([0, 1])}],
    )
    return Senco13RepairInputs(
        original_getco_snapshot_csv=original,
        first_pair_write_summary_csv=first_pair,
        latest_s1_write_summary_csv=latest_s1,
        integrated_recalc_summary_csv=recalc,
        preclear_senco5_snapshot_csv=preclear_s5,
        postclear_senco5_snapshot_csv=postclear_s5,
        target_device_ids=("022",),
    )


def test_repair_plan_detects_s1_only_mixed_state_and_pair_payload(tmp_path):
    tables = build_co2_senco13_repair_plan_tables(_inputs(tmp_path))

    history = tables["co2_senco13_repair_history"][0]
    assert history["inferred_current_state"] == "mixed_s1_only_latest_plus_preserved_s3"
    assert json.loads(history["latest_s1_only_senco1"]) == [100.0, 200.0, 300.0, 400.0, 0.0, 0.0]

    plan = tables["co2_senco13_repair_plan"][0]
    assert plan["repair_status"] == "ready_for_live_precheck_then_pair_rewrite"
    assert json.loads(plan["target_senco1"]) == [101.0, 202.0, 303.0, 404.0, 0.0, 0.0]
    assert json.loads(plan["target_senco3"]) == [11.0, 22.0, 33.0, 0.0, 0.0, 0.0]
    assert plan["writes_coefficients"] is False
    assert plan["controls_water_or_gas_routes"] is False
    commands = tables["co2_senco13_repair_command_plan"]
    order = {row["operation"]: row["sequence"] for row in commands}
    assert order["write_senco1"] < order["write_senco3"] < order["align_senco5_final_affine_layer"]


def test_repair_plan_can_target_non_neutral_s5_without_mixing_layers(tmp_path):
    inputs = _inputs(tmp_path)
    inputs = Senco13RepairInputs(
        original_getco_snapshot_csv=inputs.original_getco_snapshot_csv,
        first_pair_write_summary_csv=inputs.first_pair_write_summary_csv,
        latest_s1_write_summary_csv=inputs.latest_s1_write_summary_csv,
        integrated_recalc_summary_csv=inputs.integrated_recalc_summary_csv,
        preclear_senco5_snapshot_csv=inputs.preclear_senco5_snapshot_csv,
        postclear_senco5_snapshot_csv=inputs.postclear_senco5_snapshot_csv,
        target_senco5=(-26.5, 1.01),
        target_device_ids=("022",),
    )

    tables = build_co2_senco13_repair_plan_tables(inputs)

    plan = tables["co2_senco13_repair_plan"][0]
    assert plan["repair_status"] == "requires_senco5_layer_alignment_then_pair_rewrite"
    assert json.loads(plan["target_senco5"]) == [-26.5, 1.01]
    commands = tables["co2_senco13_repair_command_plan"]
    align_command = next(row for row in commands if row["operation"] == "align_senco5_final_affine_layer")
    assert "SENCO5,YGAS,FFF" in align_command["command_template"]
    assert "after the SENCO1/SENCO3 main chain" in align_command["reason"]
    order = {row["operation"]: row["sequence"] for row in commands}
    assert order["write_senco1"] < order["write_senco3"] < order["align_senco5_final_affine_layer"]


def test_repair_plan_writer_and_cli_are_offline_only(tmp_path):
    inputs = _inputs(tmp_path)
    output = tmp_path / "repair"

    outputs = write_co2_senco13_repair_plan_report(inputs=inputs, output_dir=output)
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
            "--first-pair-write-summary-csv",
            str(inputs.first_pair_write_summary_csv),
            "--latest-s1-write-summary-csv",
            str(inputs.latest_s1_write_summary_csv),
            "--integrated-recalc-summary-csv",
            str(inputs.integrated_recalc_summary_csv),
            "--preclear-senco5-snapshot-csv",
            str(inputs.preclear_senco5_snapshot_csv),
            "--postclear-senco5-snapshot-csv",
            str(inputs.postclear_senco5_snapshot_csv),
            "--output-dir",
            str(cli_output),
            "--target-device-id",
            "022",
        ]
    )
    assert rc == 0
    assert (cli_output / "co2_senco13_repair_plan.csv").exists()
