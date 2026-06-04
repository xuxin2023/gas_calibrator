import csv
import json

from gas_calibrator.tools.export_v1_5_formal_open_flow_report import main as export_main
from gas_calibrator.validation.formal_open_flow_artifacts import (
    build_formal_open_flow_tables,
    normalize_sample_row,
    write_formal_open_flow_sidecar_report,
)


def _plan():
    return {
        "plan_id": "v1_5_open_flow_demo",
        "plan_version": "2026-05-24",
        "config_hash": "abc123",
        "operator": "operator-a",
        "standard_gases": [
            {
                "component": "co2",
                "cylinder_id": "CO2-001",
                "certificate_value": 900.0,
                "certificate_uncertainty": 0.9,
                "valid_until": "2027-01-01",
                "supplier": "standard-lab",
                "certificate_hash": "co2-cert-hash",
            },
            {
                "component": "h2o",
                "cylinder_id": "H2O-001",
                "certificate_value": 0.5,
                "certificate_uncertainty": 0.01,
                "valid_until": "2027-01-01",
                "supplier": "standard-lab",
                "certificate_hash": "h2o-cert-hash",
            },
        ],
    }


def _pressure_reference():
    return {
        "device_id": "COM22-DPG-001",
        "certificate_id": "P-CERT-001",
        "certificate_uncertainty": 0.15,
        "valid_until": "2027-01-01",
        "certificate_hash": "pressure-cert-hash",
    }


def _row(index: int, component: str):
    base = {
        "sample_index": index,
        "point_phase": component,
        "route": component,
        "pressure_mode": "ambient_open",
        "pressure_gauge_hpa": 1000.5 + index * 0.005,
        "dewpoint_c": -30.0 + index * 0.001,
        "ga01_frame_usable": "true",
        "ga01_mode2_contract_status": "pass",
        "ga01_mode2_qc_status": "pass",
        "ga01_mode2_tokens_json": json.dumps(
            ["YGAS", "001", "0900.000", "00.500", "1768.000", "00.410"],
            separators=(",", ":"),
        ),
        "ga01_raw": "YGAS,001,...",
        "ga01_ref_signal": 3322.0,
        "ga01_co2_signal": 4356.0,
        "ga01_h2o_signal": 2631.0,
        "ga01_chamber_temp_c": 25.0 + index * 0.001,
        "ga01_case_temp_c": 25.5,
        "ga01_pressure_kpa": 100.05 + index * 0.0005,
        "ga01_co2_ratio_f": 1.3000 + index * 0.0001,
        "ga01_co2_ppm": 900.0 + index * 0.01,
        "ga01_h2o_ratio_f": 0.7000 + index * 0.00001,
        "ga01_h2o_mmol": 0.5 + index * 0.0001,
    }
    return base


def _write_samples(path, rows):
    keys = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path):
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _multi_analyzer_row(index: int, component: str):
    row = {
        "sample_index": index,
        "point_phase": component,
        "route": component,
        "pressure_mode": "ambient_open",
        "pressure_gauge_hpa": 1000.5 + index * 0.005,
        "dewpoint_c": -30.0 + index * 0.001,
    }
    for offset, (prefix, device_id) in enumerate(
        (("ga01", "091"), ("ga02", "033"), ("ga03", "001"), ("ga04", "087")),
        start=1,
    ):
        if prefix == "ga04" and index > 8:
            continue
        row.update(
            {
                f"{prefix}_frame_usable": "true",
                f"{prefix}_mode2_contract_status": "pass",
                f"{prefix}_mode2_qc_status": "pass",
                f"{prefix}_mode2_tokens_json": json.dumps(
                    ["YGAS", device_id, "0900.000", "00.500", "1768.000", "00.410"],
                    separators=(",", ":"),
                ),
                f"{prefix}_raw": f"YGAS,{device_id},...",
                f"{prefix}_ref_signal": 3320.0 + offset,
                f"{prefix}_co2_signal": 4350.0 + offset,
                f"{prefix}_h2o_signal": 2630.0 + offset,
                f"{prefix}_chamber_temp_c": 25.0 + index * 0.001,
                f"{prefix}_case_temp_c": 25.5,
                f"{prefix}_pressure_kpa": 100.05 + index * 0.0005 + offset * 0.00001,
                f"{prefix}_co2_ratio_f": 1.3000 + index * 0.0001 + offset * 0.00001,
                f"{prefix}_co2_ppm": 900.0 + index * 0.01 + offset * 0.1,
                f"{prefix}_h2o_ratio_f": 0.7000 + index * 0.00001,
                f"{prefix}_h2o_mmol": 0.5 + index * 0.0001,
            }
        )
    return row


def _make_run(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    rows = [_row(i, "co2") for i in range(1, 11)] + [_row(i, "h2o") for i in range(11, 21)]
    _write_samples(run_dir / "samples_20260524.csv", rows)
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(_plan(), ensure_ascii=False), encoding="utf-8")
    pressure_reference_path = tmp_path / "pressure_reference.json"
    pressure_reference_path.write_text(
        json.dumps(_pressure_reference(), ensure_ascii=False),
        encoding="utf-8",
    )
    return run_dir, plan_path, pressure_reference_path


def test_sidecar_report_writes_qc_tables_without_runner_control_changes(tmp_path):
    run_dir, plan_path, pressure_reference_path = _make_run(tmp_path)

    outputs = write_formal_open_flow_sidecar_report(
        run_dir=run_dir,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        output_dir=tmp_path / "formal_report",
    )

    assert outputs["workbook"].exists()
    assert outputs["run_summary_csv"].exists()
    assert outputs["a_grade_samples_csv"].exists()
    summary = _read_csv(outputs["run_summary_csv"])
    assert {row["component"] for row in summary} == {"co2", "h2o"}
    assert all(row["candidate_fit_allowed"] == "True" for row in summary)
    assert all(row["pressure_reference_traceability_status"] == "pass" for row in summary)
    assert all(row["analyzer_prefix"] == "ga01" for row in summary)
    assert all(row["analyzer_device_id"] == "001" for row in summary)
    a_rows = _read_csv(outputs["a_grade_samples_csv"])
    assert len(a_rows) == 20
    assert {row["component"] for row in a_rows} == {"co2", "h2o"}
    assert {row["analyzer_device_id"] for row in a_rows} == {"001"}


def test_sidecar_report_can_classify_all_detected_analyzers_independently(tmp_path):
    run_dir = tmp_path / "multi_run"
    run_dir.mkdir()
    rows = [_multi_analyzer_row(i, "co2") for i in range(1, 11)]
    _write_samples(run_dir / "samples_20260524.csv", rows)

    tables, context = build_formal_open_flow_tables(
        run_dir=run_dir,
        plan=_plan(),
        component="co2",
        analyzer_prefix="all",
        pressure_reference=_pressure_reference(),
    )

    assert context["analyzer_prefixes"] == ["ga01", "ga02", "ga03", "ga04"]
    summary_by_prefix = {row["analyzer_prefix"]: row for row in tables["run_summary"]}
    assert {row["analyzer_device_id"] for row in tables["run_summary"]} == {"091", "033", "001", "087"}
    assert summary_by_prefix["ga01"]["candidate_fit_allowed"] is True
    assert summary_by_prefix["ga02"]["candidate_fit_allowed"] is True
    assert summary_by_prefix["ga03"]["candidate_fit_allowed"] is True
    assert summary_by_prefix["ga04"]["candidate_fit_allowed"] is False
    assert summary_by_prefix["ga04"]["sampling_completion_status"] == "fail"
    assert "sampling_completion_not_passed" in summary_by_prefix["ga04"]["candidate_fit_blockers"]
    completion_by_prefix = {row["analyzer_prefix"]: row for row in tables["sampling_completion"]}
    assert completion_by_prefix["ga04"]["mode2_present_count"] == 8
    assert completion_by_prefix["ga04"]["component_payload_count"] == 8
    a_rows_by_prefix = {}
    for row in tables["a_grade_samples"]:
        a_rows_by_prefix[row["analyzer_prefix"]] = a_rows_by_prefix.get(row["analyzer_prefix"], 0) + 1
    assert a_rows_by_prefix == {"ga01": 10, "ga02": 10, "ga03": 10, "ga04": 8}


def test_sidecar_report_blocks_candidate_fit_when_plan_traceability_is_missing(tmp_path):
    run_dir, _, pressure_reference_path = _make_run(tmp_path)

    tables, context = build_formal_open_flow_tables(
        run_dir=run_dir,
        plan={},
        component="co2",
        pressure_reference=_pressure_reference(),
    )

    assert context["pressure_check_source"] == "sample_rows_fallback"
    assert tables["run_summary"][0]["plan_status"] == "fail"
    assert tables["run_summary"][0]["candidate_fit_allowed"] is False
    assert "plan_traceability_failed" in tables["run_summary"][0]["candidate_fit_blockers"]
    assert pressure_reference_path.exists()


def test_sidecar_report_blocks_candidate_fit_when_pressure_reference_missing(tmp_path):
    run_dir, plan_path, _ = _make_run(tmp_path)

    tables, _ = build_formal_open_flow_tables(
        run_dir=run_dir,
        plan=_plan(),
        component="co2",
    )

    assert tables["run_summary"][0]["candidate_fit_allowed"] is False
    assert "pressure_reference_traceability_failed" in tables["run_summary"][0]["candidate_fit_blockers"]
    assert tables["pressure_reference_traceability"][0]["status"] == "fail"
    assert plan_path.exists()


def test_normalize_sample_row_maps_translated_headers_and_analyzer_prefix():
    row = normalize_sample_row(
        {
            "流程阶段": "气路",
            "压力执行模式": "ambient_open",
            "气体分析仪1_MODE2数据合同状态": "pass",
            "气体分析仪1_分析仪压力kPa": "100.05",
        }
    )

    assert row["point_phase"] == "气路"
    assert row["pressure_mode"] == "ambient_open"
    assert row["ga01_mode2_contract_status"] == "pass"
    assert row["ga01_pressure_kpa"] == "100.05"


def test_export_tool_cli_writes_report(tmp_path):
    run_dir, plan_path, pressure_reference_path = _make_run(tmp_path)
    output_dir = tmp_path / "cli_report"

    rc = export_main(
        [
            "--run-dir",
            str(run_dir),
            "--plan-json",
            str(plan_path),
            "--pressure-reference-json",
            str(pressure_reference_path),
            "--output-dir",
            str(output_dir),
            "--component",
            "co2",
        ]
    )

    assert rc == 0
    summary = _read_csv(output_dir / f"run_summary.csv")
    assert len(summary) == 1
    assert summary[0]["component"] == "co2"
