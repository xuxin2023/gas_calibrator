import csv
from pathlib import Path

from gas_calibrator.tools.export_v1_5_h2o_state_transfer_diagnostic import main as cli_main
from gas_calibrator.validation.h2o_state_transfer_diagnostic import (
    H2OStateTransferDiagnosticInputs,
    build_h2o_state_transfer_diagnostic_tables,
    write_h2o_state_transfer_diagnostic_report,
)


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers: list[str] = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def _seed_inputs(tmp_path: Path) -> tuple[Path, Path]:
    policy_csv = tmp_path / "candidate" / "h2o_senco24_device_policy.csv"
    transfer_csv = tmp_path / "transfer" / "h2o_s24_post_s6_state_delta_by_device_point.csv"
    _write_csv(
        policy_csv,
        [
            {
                "analyzer_device_id": "084",
                "candidate_status": "candidate_fit_review_required",
            },
            {
                "analyzer_device_id": "091",
                "candidate_status": "candidate_fit_ready_with_warnings_requires_independent_verification",
            },
        ],
    )
    _write_csv(
        transfer_csv,
        [
            {
                "device_id": "084",
                "point_id": "p001_T20_HG20C_30RH_h2o",
                "point_label_s24": "T20_HG20C_30RH",
                "h2o_ratio_f_mean_delta_post_minus_s24": 0.00085,
                "chamber_temp_c_mean_delta_post_minus_s24": -0.749,
                "dewpoint_live_c_mean_delta_post_minus_s24": -0.113,
                "pressure_gauge_hpa_mean_delta_post_minus_s24": -1.2105,
                "live_reference_h2o_mmol_delta_post_minus_s24": -0.053503,
                "senco24_replay_h2o_mmol_mean_delta_post_minus_s24": -0.234472,
                "raw_replay_delta_minus_reference_delta_mmol": -0.180968,
                "post_existing_s6_error_mmol": -0.1837,
                "post_existing_s6_abs_rel_pct": 2.329472,
            },
            {
                "device_id": "084",
                "point_id": "p002_T20_HG20C_70RH_h2o",
                "point_label_s24": "T20_HG20C_70RH",
                "h2o_ratio_f_mean_delta_post_minus_s24": 0.00095,
                "chamber_temp_c_mean_delta_post_minus_s24": -0.785,
                "dewpoint_live_c_mean_delta_post_minus_s24": -0.096,
                "pressure_gauge_hpa_mean_delta_post_minus_s24": -1.4901,
                "live_reference_h2o_mmol_delta_post_minus_s24": -0.080054,
                "senco24_replay_h2o_mmol_mean_delta_post_minus_s24": -0.343608,
                "raw_replay_delta_minus_reference_delta_mmol": -0.263554,
                "post_existing_s6_error_mmol": -0.252277,
                "post_existing_s6_abs_rel_pct": 1.484691,
            },
            {
                "device_id": "091",
                "point_id": "p001_T20_HG20C_30RH_h2o",
                "point_label_s24": "T20_HG20C_30RH",
                "h2o_ratio_f_mean_delta_post_minus_s24": 0.00125,
                "chamber_temp_c_mean_delta_post_minus_s24": -1.047,
                "live_reference_h2o_mmol_delta_post_minus_s24": -0.053503,
                "senco24_replay_h2o_mmol_mean_delta_post_minus_s24": -0.04177,
                "raw_replay_delta_minus_reference_delta_mmol": 0.011733,
                "post_existing_s6_error_mmol": 0.0042,
                "post_existing_s6_abs_rel_pct": 0.053262,
            },
        ],
    )
    return policy_csv, transfer_csv


def test_h2o_state_transfer_diagnostic_blocks_084_and_keeps_boundary(tmp_path):
    policy_csv, transfer_csv = _seed_inputs(tmp_path)

    tables = build_h2o_state_transfer_diagnostic_tables(
        H2OStateTransferDiagnosticInputs(
            candidate_device_policy_csv=policy_csv,
            state_transfer_csv=transfer_csv,
            target_device_ids=("084", "091"),
        )
    )

    decisions = {row["device_id"]: row for row in tables["h2o_state_transfer_device_decision"]}
    assert decisions["084"]["state_transfer_decision"] == "blocked_h2o_write_requires_special_diagnostic"
    assert "senco24_raw_state_transfer_excess_shift" in decisions["084"]["blockers"]
    assert "post_s6_state_transfer_relative_error_exceeds_limit" in decisions["084"]["blockers"]
    assert decisions["084"]["max_abs_raw_excess_shift_mmol"] == 0.263554
    assert decisions["084"]["auto_write_allowed"] is False
    assert decisions["084"]["opens_com_ports"] is False
    assert decisions["091"]["state_transfer_decision"] == "state_transfer_passed_can_continue_normal_review"

    plan_rows = tables["h2o_state_transfer_minimal_diagnostic_plan"]
    assert any(row["phase"] == "same_point_return_check" for row in plan_rows)
    assert all(row["write_senco_allowed"] is False for row in plan_rows)


def test_h2o_state_transfer_diagnostic_writes_chinese_report_and_cli(tmp_path):
    policy_csv, transfer_csv = _seed_inputs(tmp_path)
    output_dir = tmp_path / "report"

    outputs = write_h2o_state_transfer_diagnostic_report(
        inputs=H2OStateTransferDiagnosticInputs(
            candidate_device_policy_csv=policy_csv,
            state_transfer_csv=transfer_csv,
            target_device_ids=("084",),
        ),
        output_dir=output_dir,
    )

    text = outputs["markdown"].read_text(encoding="utf-8")
    assert "V1.5 H2O 状态迁移专项诊断报告" in text
    assert "暂停084水路S2/S4/S6写入" in text
    assert "不打开 COM" in text

    cli_output = tmp_path / "cli_report"
    rc = cli_main(
        [
            "--candidate-device-policy-csv",
            str(policy_csv),
            "--state-transfer-csv",
            str(transfer_csv),
            "--output-dir",
            str(cli_output),
            "--target-device-id",
            "084",
        ]
    )
    assert rc == 0
    assert (cli_output / "h2o_state_transfer_diagnostic_zh.md").exists()
