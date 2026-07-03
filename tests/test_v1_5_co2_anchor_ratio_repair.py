from __future__ import annotations

import csv
import json
from pathlib import Path

from gas_calibrator.validation.co2_anchor_ratio_repair import (
    build_co2_anchor_ratio_repair_tables,
    write_co2_anchor_ratio_repair_report,
)


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    headers: list[str] = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def _target_from_true_ratio(ratio: float, temp_c: float) -> float:
    temp_k = temp_c + 273.15
    return 12.0 + 5100.0 * ratio - 900.0 * ratio**2 + 18.0 * temp_k + 2.0 * ratio * temp_k


def _row(
    *,
    target: float,
    ratio: float,
    temp: float,
    h2o: float,
    dewpoint: float,
    pressure_hpa: float,
    device_id: str = "100",
) -> dict[str, object]:
    return {
        "co2_ppm_target": target,
        "pressure_gauge_hpa": pressure_hpa,
        "dewpoint_live_c": dewpoint,
        "ga05_analyzer_device_id": device_id,
        "ga05_frame_usable": "true",
        "ga05_co2_ppm": target * 0.8,
        "ga05_co2_ratio_f": ratio,
        "ga05_co2_ratio_raw": ratio + 0.0001,
        "ga05_h2o_ratio_f": 0.86,
        "ga05_h2o_mmol": h2o,
        "ga05_chamber_temp_c": temp,
        "ga05_case_temp_c": temp + 0.1,
        "ga05_pressure_kpa": 100.5,
    }


def _true_ratio(nominal_ppm: float, temp_c: float) -> float:
    return 1.42 - nominal_ppm / 5000.0 + temp_c * 0.0004


def _old_ratio_from_true(true_ratio: float) -> float:
    return (true_ratio + 0.005) / 1.01


def _make_old_run(root: Path) -> None:
    for index, temp in enumerate((10.0, 20.0, 30.0), start=1):
        for nominal in (0.0, 100.0, 400.0, 800.0, 900.0, 1000.0):
            true_ratio = _true_ratio(nominal, temp)
            target = _target_from_true_ratio(true_ratio, temp)
            old_ratio = _old_ratio_from_true(true_ratio)
            point = root / f"p{index:03d}_T{int(temp)}_{int(nominal)}ppm_fit" / "samples_machine_readable.csv"
            _write_csv(
                point,
                [
                    _row(
                        target=target,
                        ratio=old_ratio,
                        temp=temp,
                        h2o=2.0 + nominal / 1000.0,
                        dewpoint=-34.0 + nominal / 1000.0,
                        pressure_hpa=1008.0 + nominal / 1000.0,
                    ),
                    _row(
                        target=target,
                        ratio=old_ratio + 0.00001,
                        temp=temp,
                        h2o=2.0 + nominal / 1000.0,
                        dewpoint=-34.0 + nominal / 1000.0,
                        pressure_hpa=1008.0 + nominal / 1000.0,
                    ),
                ],
            )


def _make_current_anchors(root: Path) -> list[str]:
    files: list[str] = []
    for nominal in (100.0, 800.0, 900.0):
        temp = 20.0
        true_ratio = _true_ratio(nominal, temp)
        target = _target_from_true_ratio(true_ratio, temp)
        path = root / f"p_T20_{int(nominal)}ppm_verification" / "samples_machine_readable.csv"
        _write_csv(
            path,
            [
                _row(
                    target=target,
                    ratio=true_ratio,
                    temp=temp,
                    h2o=0.1 + nominal / 10000.0,
                    dewpoint=-31.0 + nominal / 1200.0,
                    pressure_hpa=1004.0 + nominal / 1500.0,
                ),
                _row(
                    target=target,
                    ratio=true_ratio + 0.00001,
                    temp=temp,
                    h2o=0.1 + nominal / 10000.0,
                    dewpoint=-31.0 + nominal / 1200.0,
                    pressure_hpa=1004.0 + nominal / 1500.0,
                ),
            ],
        )
        files.append(str(path))
    return files


def test_anchor_ratio_repair_improves_current_anchor_projection(tmp_path: Path) -> None:
    old_root = tmp_path / "old"
    current_root = tmp_path / "current"
    _make_old_run(old_root)
    current_files = _make_current_anchors(current_root)

    tables = build_co2_anchor_ratio_repair_tables(
        old_run_dir=old_root,
        current_sample_files=current_files,
        target_device_id="100",
    )
    current = [
        row
        for row in tables["summary_rows"]
        if row.get("eval_set") == "current_anchor_actual_ratio"
    ]
    by_model = {row["bridge_model"]: row for row in current}

    assert float(by_model["affine_ratio_bridge"]["max_abs_error_ppm"]) < float(
        by_model["identity_no_repair"]["max_abs_error_ppm"]
    )
    assert "state_h2o_mmol_mol_delta_bridge" in by_model
    assert by_model["state_h2o_mmol_mol_delta_bridge"]["state_driver"] == "h2o_mmol_mol"
    assert {row["state_driver"] for row in tables["state_sensitivity_rows"]} >= {
        "h2o_mmol_mol",
        "dewpoint_c",
        "chamber_temp_c",
        "pressure_hpa",
    }
    assert by_model["affine_ratio_bridge"]["pressure_terms"] == "frozen_zero_independent_senco9_workflow"
    assert by_model["affine_ratio_bridge"]["writes_coefficients"] is False
    recommendation = tables["recommendation_rows"][0]
    assert recommendation["recommendation_status"] == "reviewable_no_write_candidate"
    assert recommendation["senco1_payload_scientific"]
    assert recommendation["senco3_payload_scientific"]
    assert recommendation["boundary"] == "offline_no_com_no_route_control_no_senco_write"
    contract = tables["write_review_contract_rows"][0]
    assert contract["write_gate_status"] == "blocked_three_anchor_current_state_review_only"
    assert contract["candidate_write_allowed"] is False
    assert contract["pressure_contract"] == "pressure_terms_frozen_senco9_independent"
    assert contract["ratio_evidence_contract"] == "old_factory_ratio_preserved_bridge_is_offline_review_only"
    assert contract["raw_target_no_ratio_status"] in {
        "diagnostic_failed_h2o_target_alone_not_sufficient",
        "diagnostic_passed_h2o_target_alone_explains_current_anchors",
    }


def test_anchor_ratio_repair_report_is_chinese_and_no_write(tmp_path: Path) -> None:
    old_root = tmp_path / "old"
    current_root = tmp_path / "current"
    output = tmp_path / "out"
    _make_old_run(old_root)
    current_files = _make_current_anchors(current_root)

    outputs = write_co2_anchor_ratio_repair_report(
        old_run_dir=old_root,
        current_sample_files=current_files,
        output_dir=output,
        target_device_id="100",
    )

    manifest = json.loads(outputs["manifest_json"].read_text(encoding="utf-8-sig"))
    assert manifest["boundary"] == "offline_no_com_no_route_control_no_senco_write"
    markdown = outputs["markdown"].read_text(encoding="utf-8-sig")
    assert "状态归一化与系数候选评估" in markdown
    assert "不打开 COM" in markdown
    assert "原始旧比值不被覆盖" in markdown
    assert "同气点状态变量对 ΔR_f 的解释" in markdown
    assert "no-write 推荐候选" in markdown
    assert "写入前合同门禁" in markdown
    assert "firmware_h2o_raw_target_no_ratio_repair" in markdown
    assert "state_*_delta_bridge" in markdown
    contract_rows = list(csv.DictReader(outputs["co2_anchor_ratio_repair_write_review_contract_csv"].open(encoding="utf-8-sig")))
    assert contract_rows[0]["write_gate_status"] == "blocked_three_anchor_current_state_review_only"
    assert contract_rows[0]["candidate_write_allowed"] == "False"
