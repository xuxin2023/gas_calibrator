from __future__ import annotations

import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_co2_s13_source_state_repair_fit_review import main as cli_main
from gas_calibrator.validation.co2_s13_source_state_repair_fit_review import (
    build_co2_s13_source_state_repair_fit_review,
    write_co2_s13_source_state_repair_fit_review,
)


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    headers: list[str] = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def _source_audit_dir(root: Path) -> Path:
    audit = root / "source_audit"
    _write_csv(
        audit / "co2_s13_source_state_run_summary.csv",
        [
            {
                "write_gate_status": "blocked_source_state_discontinuity",
                "write_gate_blocker_topics": "mixed_source_temperature_group;sawtooth_bias",
                "fit_row_count": 12,
            }
        ],
    )
    _write_csv(
        audit / "co2_s13_source_state_root_cause_decision.csv",
        [
            {
                "priority": "P0",
                "topic": "same-temperature mixed source",
                "finding": "T20 contains two sources",
                "action": "hold supplement points until bridge evidence exists",
            }
        ],
    )
    return audit


def _strategy_dir(root: Path, name: str, max_error: float, s5_error: float, treatment: bool) -> Path:
    strategy = root / name
    point_count = 20 if not treatment else 18
    _write_csv(
        strategy / "co2_s13_multistrategy_best_by_device.csv",
        [
            {
                "device_id": "001",
                "strategy_profile_id": "baseline",
                "objective_id": "low_end_priority_lstsq",
                "zero_offset_ppm": 0.0,
                "fit_point_count": point_count,
                "max_abs_relative_error_percent": max_error,
                "low_end_max_abs_relative_error_percent": max_error,
                "s1_payload_scientific": "1.0e0",
                "s3_payload_scientific": "2.0e0",
                "uses_pressure_terms": False,
            },
            {
                "device_id": "002",
                "strategy_profile_id": "baseline",
                "objective_id": "low_end_priority_lstsq",
                "zero_offset_ppm": 0.0,
                "fit_point_count": point_count,
                "max_abs_relative_error_percent": max_error + 0.2,
                "low_end_max_abs_relative_error_percent": max_error + 0.2,
                "s1_payload_scientific": "1.1e0",
                "s3_payload_scientific": "2.1e0",
                "uses_pressure_terms": False,
            },
        ],
    )
    _write_csv(
        strategy / "co2_s13_multistrategy_s5_best_by_device.csv",
        [
            {
                "device_id": "001",
                "s5_C0": 0.1,
                "s5_C1": 0.999,
                "s5_command_preview": "SENCO5,YGAS,FFF,0.100,0.999",
                "s5_max_abs_relative_error_percent": s5_error,
                "s5_low_end_max_abs_relative_error_percent": s5_error,
                "s5_worst_point_identity": "T20_100ppm",
            },
            {
                "device_id": "002",
                "s5_C0": 0.2,
                "s5_C1": 1.001,
                "s5_command_preview": "SENCO5,YGAS,FFF,0.200,1.001",
                "s5_max_abs_relative_error_percent": s5_error + 0.1,
                "s5_low_end_max_abs_relative_error_percent": s5_error + 0.1,
                "s5_worst_point_identity": "T20_100ppm",
            },
        ],
    )
    _write_csv(
        strategy / "co2_s13_multistrategy_best_residuals.csv",
        [
            {
                "device_id": "001",
                "point_identity": "T20_100ppm",
                "relative_error_percent": -max_error,
            },
            {
                "device_id": "002",
                "point_identity": "T20_200ppm",
                "relative_error_percent": max_error + 0.2,
            },
        ],
    )
    _write_csv(
        strategy / "co2_s13_multistrategy_s5_best_residuals.csv",
        [
            {
                "device_id": "001",
                "point_identity": "T20_100ppm",
                "s5_relative_error_percent": -s5_error,
            },
            {
                "device_id": "002",
                "point_identity": "T20_200ppm",
                "s5_relative_error_percent": s5_error + 0.1,
            },
        ],
    )
    treatment_path = ""
    if treatment:
        plan = root / f"{name}_treatment.csv"
        treatment_path = str(plan)
        _write_csv(
            plan,
            [
                {
                    "point_identity": "T20_600ppm",
                    "fit_policy": "hold_for_source_state_discontinuity_review",
                    "exclusion_basis": "pressure-state outlier",
                }
            ],
        )
    (strategy / "co2_s13_multistrategy_meta.json").write_text(
        json.dumps(
            {"inputs": {"fit_point_treatment_plan_csv": treatment_path}},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return strategy


def _enhanced_dir(root: Path) -> Path:
    enhanced = root / "enhanced"
    _write_csv(
        enhanced / "co2_s13_enhanced_capacity_best_by_device_no_s5.csv",
        [
            {
                "device_id": "001",
                "structure_id": "diagnostic_temp_group_ratio_slope",
                "objective_id": "low_end_priority_lstsq",
                "zero_offset_ppm": 0.0,
                "fit_point_count": 20,
                "max_abs_relative_error_percent": 0.8,
                "low_end_max_abs_relative_error_percent": 0.7,
                "physical_meaning": "diagnostic only",
            }
        ],
    )
    return enhanced


def test_source_state_repair_review_blocks_write_even_when_strategy_improves(tmp_path: Path) -> None:
    audit = _source_audit_dir(tmp_path)
    baseline = _strategy_dir(tmp_path, "baseline", max_error=5.0, s5_error=3.0, treatment=False)
    repaired = _strategy_dir(tmp_path, "repaired", max_error=2.0, s5_error=1.2, treatment=True)
    enhanced = _enhanced_dir(tmp_path)

    tables = build_co2_s13_source_state_repair_fit_review(
        source_state_audit_dir=audit,
        strategy_dirs=(("baseline", baseline), ("hold_state", repaired)),
        enhanced_dir=enhanced,
    )

    gate = tables["write_gate"][0]
    assert gate["status"] == "blocked_source_state_discontinuity"
    assert gate["candidate_strategy_label"] == "hold_state"
    assert gate["candidate_s5_max_abs_relative_error_percent"] == 1.3
    assert tables["run_summary"][0]["opens_com_ports"] is False
    assert tables["run_summary"][0]["controls_water_or_gas_routes"] is False
    assert tables["run_summary"][0]["writes_coefficients"] is False
    assert any(row["strategy_label"] == "enhanced_diagnostic_capacity" for row in tables["strategy_summary"])
    assert tables["point_treatment"][0]["point_identity"] == "T20_600ppm"


def test_source_state_repair_writer_and_cli_are_offline(tmp_path: Path, capsys) -> None:
    audit = _source_audit_dir(tmp_path)
    strategy = _strategy_dir(tmp_path, "strategy", max_error=2.0, s5_error=1.2, treatment=True)
    output = tmp_path / "out"

    paths = write_co2_s13_source_state_repair_fit_review(
        source_state_audit_dir=audit,
        strategy_dirs=(("strategy", strategy),),
        output_dir=output,
    )
    meta = json.loads(paths["metadata"].read_text(encoding="utf-8"))
    assert meta["boundary"]["opens_com_ports"] is False
    assert meta["boundary"]["controls_water_or_gas_routes"] is False
    assert meta["boundary"]["writes_coefficients"] is False
    assert "V1.5 CO2 S1/S3 源状态修复版拟合评审" in paths["markdown"].read_text(
        encoding="utf-8-sig"
    )

    cli_output = tmp_path / "cli"
    result = cli_main(
        [
            "--source-state-audit-dir",
            str(audit),
            "--strategy-dir",
            f"strategy={strategy}",
            "--output-dir",
            str(cli_output),
        ]
    )
    assert result == 0
    captured = capsys.readouterr()
    assert "source-state repair fit review saved" in captured.out
    assert (cli_output / "co2_s13_source_state_repair_write_gate.csv").exists()
