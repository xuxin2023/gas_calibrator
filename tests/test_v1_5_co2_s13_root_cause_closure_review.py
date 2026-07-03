from __future__ import annotations

import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_co2_s13_root_cause_closure_review import main as cli_main
from gas_calibrator.validation.co2_s13_root_cause_closure_review import (
    write_co2_s13_root_cause_closure_review,
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


def _fixture_dirs(tmp_path: Path) -> dict[str, Path]:
    source = tmp_path / "source"
    ratio = tmp_path / "ratio"
    target = tmp_path / "target"
    correction = tmp_path / "correction"
    repair = tmp_path / "repair"
    error = tmp_path / "error"
    _write_csv(
        source / "co2_s13_source_state_run_summary.csv",
        [{"write_gate_status": "blocked_source_state_discontinuity"}],
    )
    _write_csv(
        source / "co2_s13_source_state_root_cause_decision.csv",
        [
            {
                "priority": "P0",
                "topic": "同一温度组混入不同运行来源",
                "finding": "T30 contains supplement source",
            }
        ],
    )
    _write_csv(
        source / "co2_s13_point_common_bias_with_state.csv",
        [
            {
                "model_role": "current_writable_best",
                "temperature_group": "T20",
                "target_group": "400ppm",
                "source_labels": "main",
                "device_count": 6,
                "max_abs_relative_error_percent": 3.1,
                "state_flags": "shared_point_bias;ratio_A;deep_dry",
            },
            {
                "model_role": "current_writable_best",
                "temperature_group": "T30",
                "target_group": "300ppm",
                "source_labels": "supplement",
                "device_count": 6,
                "max_abs_relative_error_percent": 2.8,
                "state_flags": "shared_point_bias;ratio_A;deep_dry",
            },
            {
                "model_role": "current_writable_best",
                "temperature_group": "T10",
                "target_group": "1000ppm",
                "source_labels": "main",
                "device_count": 6,
                "max_abs_relative_error_percent": 2.0,
                "state_flags": "shared_point_bias;ratio_A;deep_dry",
            },
        ],
    )
    _write_csv(
        ratio / "co2_s13_ratio_mapping_device_summary.csv",
        [
            {
                "device_id": "058",
                "ratio_monotonic_violation_count": 0,
                "mapping_suspect_count": 0,
                "zero_anchor_assigned_value_review_count": 0,
                "recommended_action": "review_s13_low_end_model_boundary",
                "physical_reason": "ratio monotonic but low-end bias",
            }
        ],
    )
    _write_csv(
        ratio / "co2_s13_point_mapping_audit.csv",
        [
            {
                "device_id": "058",
                "temperature_group": "T20",
                "point_identity": "T20_400ppm",
                "mapping_status": "target_matches_certificate_value",
            },
            {
                "device_id": "058",
                "temperature_group": "T20",
                "point_identity": "T20_0ppm",
                "mapping_status": "zero_anchor_assigned_value_review",
            }
        ],
    )
    _write_csv(
        target / "co2_s13_target_state_bridge_root_cause_summary.csv",
        [
            {
                "device_id": "058",
                "worst_point_identity": "T20_400ppm",
                "max_abs_relative_error_percent": 3.2,
                "primary_hypothesis": "physical_state_bridge_needed_before_refit",
            }
        ],
    )
    _write_csv(
        correction / "co2_s13_bridge_correction_device_recommendations.csv",
        [
            {
                "device_id": "058",
                "best_candidate_id": "baseline_selected_s13",
                "best_max_abs_relative_error_percent": 3.2,
            }
        ],
    )
    _write_csv(
        repair / "co2_s13_source_state_repair_write_gate.csv",
        [{"status": "blocked_source_state_discontinuity"}],
    )
    _write_csv(
        repair / "co2_s13_source_state_repair_strategy_by_device.csv",
        [
            {
                "strategy_label": "hold_state_outliers",
                "device_id": "058",
                "s1s3_worst_relative_error_percent": 3.1,
                "s5_worst_relative_error_percent": 2.7,
            }
        ],
    )
    _write_csv(
        error / "co2_s13_error_common_mode_points.csv",
        [
            {
                "point_identity": "T20_400ppm",
                "temperature_group": "T20",
                "target_ppm": 399.56,
                "device_count": 6,
                "max_abs_relative_error_percent": 3.1,
                "root_cause_class": "point_common_mode_model_or_target_state_boundary",
            },
            {
                "point_identity": "T30_300ppm",
                "temperature_group": "T30",
                "target_ppm": 299.73,
                "device_count": 6,
                "max_abs_relative_error_percent": 2.8,
                "root_cause_class": "point_common_mode_model_or_target_state_boundary",
            },
            {
                "point_identity": "T10_1000ppm",
                "temperature_group": "T10",
                "target_ppm": 998.62,
                "device_count": 6,
                "max_abs_relative_error_percent": 2.0,
                "root_cause_class": "point_common_mode_model_or_target_state_boundary",
            },
        ],
    )
    return {
        "source": source,
        "ratio": ratio,
        "target": target,
        "correction": correction,
        "repair": repair,
        "error": error,
    }


def test_root_cause_closure_blocks_write_and_holds_source_state_points(tmp_path: Path) -> None:
    dirs = _fixture_dirs(tmp_path)
    output = tmp_path / "out"
    paths = write_co2_s13_root_cause_closure_review(
        source_state_dir=dirs["source"],
        ratio_mapping_dir=dirs["ratio"],
        target_state_bridge_dir=dirs["target"],
        bridge_correction_dir=dirs["correction"],
        repair_fit_dir=dirs["repair"],
        error_root_cause_dir=dirs["error"],
        output_dir=output,
    )
    run_summary = list(csv.DictReader(paths["run_summary"].open("r", encoding="utf-8-sig")))
    assert run_summary[0]["write_gate_status"] == "blocked_root_cause_not_closed"

    points = list(csv.DictReader(paths["point_decisions"].open("r", encoding="utf-8-sig")))
    by_point = {row["point_identity"]: row for row in points}
    assert by_point["T20_400ppm"]["recommended_treatment"] == "hold_as_diagnostic_until_bridge_evidence_passes"
    assert "pressure_state_outlier_review" in by_point["T20_400ppm"]["blockers"]
    assert "supplement_source_bridge_not_proven" in by_point["T30_300ppm"]["blockers"]
    assert by_point["T10_1000ppm"]["recommended_treatment"] == "keep_for_model_boundary_review_not_auto_exclude"
    assert by_point["T20_0ppm"]["recommended_treatment"] == "review_zero_anchor_value_before_fit"
    assert "mapping_suspect" not in by_point["T20_0ppm"]["blockers"]
    assert "zero_anchor_value_review" in by_point["T20_0ppm"]["blockers"]

    meta = json.loads(paths["metadata"].read_text(encoding="utf-8-sig"))
    assert meta["boundaries"]["opens_com_ports"] is False
    assert meta["boundaries"]["writes_coefficients"] is False
    assert meta["boundaries"]["not_real_acceptance_evidence"] is True


def test_root_cause_closure_cli_writes_chinese_markdown(tmp_path: Path) -> None:
    dirs = _fixture_dirs(tmp_path)
    output = tmp_path / "cli_out"
    exit_code = cli_main(
        [
            "--source-state-dir",
            str(dirs["source"]),
            "--ratio-mapping-dir",
            str(dirs["ratio"]),
            "--target-state-bridge-dir",
            str(dirs["target"]),
            "--bridge-correction-dir",
            str(dirs["correction"]),
            "--repair-fit-dir",
            str(dirs["repair"]),
            "--error-root-cause-dir",
            str(dirs["error"]),
            "--output-dir",
            str(output),
        ]
    )
    assert exit_code == 0
    markdown = (output / "co2_s13_root_cause_closure_review_zh.md").read_text(encoding="utf-8-sig")
    assert "V1.5 CO2 S1/S3 根因收敛评审" in markdown
    assert "本评审不开 COM" in markdown
