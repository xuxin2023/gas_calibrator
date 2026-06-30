from __future__ import annotations

import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_co2_s13_blocker_closure_action_review import main as cli_main
from gas_calibrator.validation.co2_s13_blocker_closure_action_review import (
    write_co2_s13_blocker_closure_action_review,
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


def _closure_fixture(tmp_path: Path) -> Path:
    root = tmp_path / "closure"
    _write_csv(
        root / "co2_s13_root_cause_closure_run_summary.csv",
        [
            {
                "write_gate_status": "blocked_root_cause_not_closed",
                "device_count": 6,
                "blocked_device_count": 6,
            }
        ],
    )
    _write_csv(
        root / "co2_s13_root_cause_closure_device_decisions.csv",
        [
            {"device_id": "058", "status": "blocked_do_not_write_s13"},
            {"device_id": "070", "status": "blocked_do_not_write_s13"},
        ],
    )
    _write_csv(
        root / "co2_s13_root_cause_closure_point_decisions.csv",
        [
            {
                "point_identity": "T20_400ppm",
                "temperature_group": "T20",
                "recommended_treatment": "hold_as_diagnostic_until_bridge_evidence_passes",
                "blockers": "common_mode_bias;pressure_state_outlier_review",
                "max_abs_relative_error_percent": 2.5,
            },
            {
                "point_identity": "T30_300ppm",
                "temperature_group": "T30",
                "recommended_treatment": "hold_as_diagnostic_until_bridge_evidence_passes",
                "blockers": "supplement_source_bridge_not_proven",
                "max_abs_relative_error_percent": 2.8,
            },
            {
                "point_identity": "T20_0ppm",
                "temperature_group": "T20",
                "recommended_treatment": "review_zero_anchor_value_before_fit",
                "blockers": "zero_anchor_value_review",
                "max_abs_relative_error_percent": 96.0,
            },
            {
                "point_identity": "T10_1000ppm",
                "temperature_group": "T10",
                "recommended_treatment": "keep_for_model_boundary_review_not_auto_exclude",
                "blockers": "common_mode_bias",
                "max_abs_relative_error_percent": 2.0,
            },
            {
                "point_identity": "T10_900ppm",
                "temperature_group": "T10",
                "recommended_treatment": "keep_for_review_not_auto_exclude",
                "blockers": "",
                "max_abs_relative_error_percent": 0.5,
            },
        ],
    )
    return root


def test_blocker_closure_action_review_classifies_physical_blockers(tmp_path: Path) -> None:
    closure = _closure_fixture(tmp_path)
    paths = write_co2_s13_blocker_closure_action_review(
        root_cause_closure_dir=closure,
        output_dir=tmp_path / "out",
    )
    actions = list(csv.DictReader(paths["point_actions"].open("r", encoding="utf-8-sig")))
    by_point = {row["point_identity"]: row for row in actions}

    assert by_point["T20_400ppm"]["closure_action"] == "hold_until_pressure_state_explained"
    assert by_point["T20_400ppm"]["next_fit_use"] == "do_not_add_pressure_term; hold point until open_flow_state_is_explained"
    assert by_point["T30_300ppm"]["closure_action"] == "hold_until_source_bridge_evidence_or_rerun_main_source"
    assert by_point["T20_0ppm"]["closure_action"] == "review_zero_gas_co2_assigned_value_and_absolute_ppm_error"
    assert by_point["T20_0ppm"]["next_fit_use"] == "candidate_low_end_anchor_after_value_review"
    assert by_point["T10_1000ppm"]["closure_action"] == "keep_for_model_boundary_review"
    assert by_point["T10_900ppm"]["closure_action"] == "keep_with_standard_qc_review"

    summary = {
        row["metric"]: row["value"]
        for row in csv.DictReader(paths["summary"].open("r", encoding="utf-8-sig"))
    }
    assert summary["hard_hold_point_count"] == "2"
    assert summary["zero_anchor_review_point_count"] == "1"
    assert summary["can_refit_for_controlled_write"] == "false"

    meta = json.loads(paths["metadata"].read_text(encoding="utf-8"))
    assert meta["boundaries"]["opens_com_ports"] is False
    assert meta["boundaries"]["writes_coefficients"] is False


def test_blocker_closure_action_cli_writes_chinese_report(tmp_path: Path) -> None:
    closure = _closure_fixture(tmp_path)
    output = tmp_path / "cli_out"
    exit_code = cli_main(
        [
            "--root-cause-closure-dir",
            str(closure),
            "--output-dir",
            str(output),
        ]
    )
    assert exit_code == 0
    markdown = (output / "co2_s13_blocker_closure_action_review_zh.md").read_text(encoding="utf-8-sig")
    assert "V1.5 CO2 S1/S3 阻断闭环行动评审" in markdown
    assert "本评审不打开 COM" in markdown
