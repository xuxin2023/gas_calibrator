import csv
from pathlib import Path

from gas_calibrator.tools.export_v1_5_h2o_special_diagnostic_queue import main as cli_main
from gas_calibrator.validation.h2o_special_diagnostic_queue import (
    H2OSpecialDiagnosticQueueInputs,
    build_h2o_special_diagnostic_queue_tables,
    write_h2o_special_diagnostic_queue,
)


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers: list[str] = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _seed_decision(tmp_path: Path) -> Path:
    decision_csv = tmp_path / "h2o_state_transfer_device_decision.csv"
    _write_csv(
        decision_csv,
        [
            {
                "device_id": "084",
                "state_transfer_decision": "blocked_h2o_write_requires_special_diagnostic",
                "blockers": "senco24_raw_state_transfer_excess_shift;post_s6_state_transfer_relative_error_exceeds_limit",
            }
        ],
    )
    return decision_csv


def test_h2o_special_diagnostic_queue_builds_no_write_runner_queue(tmp_path: Path) -> None:
    decision_csv = _seed_decision(tmp_path)

    tables = build_h2o_special_diagnostic_queue_tables(
        H2OSpecialDiagnosticQueueInputs(
            device_decision_csv=decision_csv,
            target_device_id="084",
            purge_s=420,
            sample_count=24,
        )
    )

    summary = tables["h2o_special_diagnostic_summary"][0]
    assert summary["device_id"] == "084"
    assert summary["opens_com_ports_now"] is False
    assert summary["controls_routes_now"] is False
    assert summary["writes_senco_now"] is False
    assert "--h2o-pressure-presample-policy skip" in summary["recommended_command"]
    assert "--no-control-temperature" in summary["recommended_command"]
    assert "run_v1_5_formal_h2o_open_flow_queue" in summary["recommended_command"]

    execution_plan = tables["h2o_special_diagnostic_execution_plan"]
    assert any(row["phase"] == "dry_gas_low_water_anchor" for row in execution_plan)
    assert all(row["write_senco_allowed"] is False for row in execution_plan)

    runner_queue = tables["h2o_special_diagnostic_runner_queue"]
    assert [row["diagnostic_phase"] for row in runner_queue] == [
        "low_humidity_a",
        "high_humidity",
        "low_humidity_return",
    ]
    assert all(row["component"] == "h2o" for row in runner_queue)
    assert all(row["sample_role"] == "diagnostic" for row in runner_queue)
    assert all(row["writes_senco"] is False for row in runner_queue)
    assert all(row["pressure_presample_policy"] == "skip" for row in runner_queue)
    assert runner_queue[0]["hgen_rh_pct"] == 30.0
    assert runner_queue[1]["hgen_rh_pct"] == 70.0
    assert runner_queue[2]["hgen_rh_pct"] == 30.0
    assert runner_queue[0]["reference_h2o_mmol"] > 0


def test_h2o_special_diagnostic_queue_writes_artifacts_and_cli(tmp_path: Path) -> None:
    decision_csv = _seed_decision(tmp_path)
    output_dir = tmp_path / "queue_pack"

    outputs = write_h2o_special_diagnostic_queue(
        inputs=H2OSpecialDiagnosticQueueInputs(device_decision_csv=decision_csv, target_device_id="84"),
        output_dir=output_dir,
    )

    runbook = outputs["runbook"].read_text(encoding="utf-8")
    assert "V1.5 084 水路专项 no-write 诊断队列" in runbook
    assert "不写 `SENCO2/SENCO4/SENCO6`" in runbook
    assert "干气低水锚点只约束 H2O 低端" in runbook

    queue_rows = _read_csv(output_dir / "h2o_084_special_diagnostic_runner_queue.csv")
    assert len(queue_rows) == 3
    assert {row["sample_role"] for row in queue_rows} == {"diagnostic"}

    cli_output = tmp_path / "cli_pack"
    rc = cli_main(
        [
            "--device-decision-csv",
            str(decision_csv),
            "--output-dir",
            str(cli_output),
            "--target-device-id",
            "084",
        ]
    )
    assert rc == 0
    assert (cli_output / "h2o_084_special_diagnostic_runbook_zh.md").exists()
    assert (cli_output / "h2o_084_special_diagnostic_runner_queue.csv").exists()
