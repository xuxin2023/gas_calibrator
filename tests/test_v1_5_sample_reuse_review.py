import csv
import json

from gas_calibrator.tools.export_v1_5_sample_reuse_review import main as reuse_cli
from gas_calibrator.validation.formal_sample_reuse import build_sample_reuse_review_tables


def _plan():
    return {
        "plan_id": "v1_5_reuse_review_demo",
        "plan_version": "2026-05-27",
        "config_hash": "config-hash",
        "operator": "operator-a",
        "standard_gases": [
            {
                "component": "co2",
                "cylinder_id": "CO2-897",
                "certificate_value": 897.04,
                "certificate_uncertainty": 8.9704,
                "valid_until": "2027-03-03",
                "supplier": "Dalian Special Gases",
                "certificate_hash": "co2-cert-hash",
            }
        ],
    }


def _pressure_reference():
    return {
        "device_id": "118288",
        "certificate_id": "FRGsz25038057",
        "certificate_uncertainty": 0.55,
        "valid_until": "2027-05-21",
        "certificate_hash": "pressure-cert-hash",
        "unit": "hPa",
    }


def _write_csv(path, rows):
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


def _co2_row(index: int, target: float, *, role: str = "fit"):
    row = {
        "sample_index": index,
        "sample_ts": f"2026-05-27T10:00:{index % 60:02d}",
        "point_phase": "co2",
        "route": "co2",
        "sample_role": role,
        "point_tag": f"co2_{target:g}_{role}",
        "pressure_mode": "ambient_open",
        "co2_ppm_target": target,
        "pressure_gauge_hpa": 1000.5 + (index % 4) * 0.01,
        "dewpoint_c": -30.0 + (index % 4) * 0.001,
    }
    row.update(_analyzer_payload("ga01", "001", index, target, status="pass"))
    if target == 897.04 and role == "fit":
        row.update(_analyzer_payload("ga02", "033", index, target, status="pass"))
        bad = _analyzer_payload("ga03", "027", index, target, status="fail")
        bad["ga03_co2_ppm"] = 0.0
        bad["ga03_co2_ratio_f"] = 0.0
        row.update(bad)
    return row


def _analyzer_payload(prefix: str, device_id: str, index: int, target: float, *, status: str):
    return {
        f"{prefix}_frame_usable": "true" if status == "pass" else "false",
        f"{prefix}_mode2_contract_status": status,
        f"{prefix}_mode2_qc_status": status,
        f"{prefix}_mode2_tokens_json": json.dumps(
            ["YGAS", device_id, f"{target:08.3f}", "00.500", "1768.000", "00.410"],
            separators=(",", ":"),
        ),
        f"{prefix}_raw": f"YGAS,{device_id},...",
        f"{prefix}_ref_signal": 3322.0,
        f"{prefix}_co2_signal": 4356.0,
        f"{prefix}_h2o_signal": 2631.0,
        f"{prefix}_chamber_temp_c": 25.0 + (index % 4) * 0.001,
        f"{prefix}_case_temp_c": 25.5,
        f"{prefix}_pressure_kpa": 100.05 + (index % 4) * 0.001,
        f"{prefix}_co2_ratio_f": 1.0 + target / 1000.0 + (index % 4) * 0.000001,
        f"{prefix}_co2_ppm": target + 0.02,
        f"{prefix}_h2o_ratio_f": 0.7,
        f"{prefix}_h2o_mmol": 0.5,
    }


def _pressure_rows():
    rows = []
    for prefix, device_id in (("ga01", "001"), ("ga02", "033"), ("ga03", "027")):
        for index in range(1, 5):
            rows.append(
                {
                    "sample_index": f"{prefix}_{index}",
                    "sample_ts": f"2026-05-27T09:55:{index:02d}",
                    "analyzer_prefix": prefix,
                    "analyzer_device_id": device_id,
                    "pressure_mode": "ambient_open",
                    "analyzer_pressure_kpa": 100.05 + index * 0.0001,
                    "com22_pressure_hpa": 1000.5 + index * 0.001,
                    "pressure_atmosphere_hold_status": "verified",
                    "pressure_atmosphere_hold_active": "true",
                    "pressure_atmosphere_hold_strategy": "legacy_hold_thread",
                }
            )
    return rows


def _make_run(tmp_path):
    rows = []
    rows.extend(_co2_row(index, 0.0, role="fit") for index in range(1, 11))
    rows.extend(_co2_row(index, 897.04, role="fit") for index in range(11, 21))
    rows.extend(_co2_row(index, 450.0, role="verification") for index in range(21, 31))
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _write_csv(run_dir / "samples_20260527.csv", rows)
    pressure_path = tmp_path / "pressure_channel_quick_check.csv"
    _write_csv(pressure_path, _pressure_rows())
    plan_path = tmp_path / "formal_plan.json"
    plan_path.write_text(json.dumps(_plan(), ensure_ascii=False), encoding="utf-8")
    pressure_reference_path = tmp_path / "pressure_reference.json"
    pressure_reference_path.write_text(json.dumps(_pressure_reference(), ensure_ascii=False), encoding="utf-8")
    return run_dir, pressure_path, plan_path, pressure_reference_path


def test_sample_reuse_review_classifies_each_device_independently(tmp_path):
    run_dir, pressure_path, _, _ = _make_run(tmp_path)

    tables, context = build_sample_reuse_review_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="co2",
        analyzer_prefix="all",
        pressure_check_path=pressure_path,
        today="2026-05-27",
    )

    by_device = {row["analyzer_device_id"]: row for row in tables["sample_reuse_by_device"]}
    assert context["decision_counts"] == {"needs_verification": 2, "reject": 1}
    assert by_device["001"]["reuse_decision"] == "needs_verification"
    assert "independent_verification_missing" in by_device["001"]["reuse_reasons"]
    assert (
        "source_verification_samples_reused_for_fit_requires_new_independent_verification"
        in by_device["001"]["reuse_reasons"]
    )
    assert by_device["033"]["reuse_decision"] == "needs_verification"
    assert "a_grade_single_target_review_only" in by_device["033"]["reuse_reasons"]
    assert by_device["027"]["reuse_decision"] == "reject"
    assert "open_flow_candidate_not_allowed" in by_device["027"]["formal_review_blockers"]
    assert "this_analyzer_device_id_only" in by_device["027"]["scope_boundary"]


def test_sample_reuse_review_derives_h2o_target_from_dewpoint_for_recheck(tmp_path):
    rows = []
    for index in range(1, 11):
        row = {
            "sample_index": index,
            "sample_ts": f"2026-05-27T10:05:{index % 60:02d}",
            "point_phase": "h2o",
            "route": "h2o",
            "sample_role": "fit",
            "point_tag": "h2o_open_flow_missing_target",
            "pressure_mode": "ambient_open",
            "pressure_gauge_hpa": 1000.5,
            "dewpoint_c": 20.0,
        }
        row.update(_analyzer_payload("ga01", "001", index, 897.04, status="pass"))
        rows.append(row)
    run_dir = tmp_path / "run_missing_target"
    run_dir.mkdir()
    _write_csv(run_dir / "samples_20260527.csv", rows)
    pressure_path = tmp_path / "pressure_channel_quick_check.csv"
    _write_csv(pressure_path, _pressure_rows())

    tables, _ = build_sample_reuse_review_tables(
        run_dir=run_dir,
        plan=_plan(),
        pressure_reference=_pressure_reference(),
        component="h2o",
        analyzer_prefix="all",
        pressure_check_path=pressure_path,
        today="2026-05-27",
    )

    by_device = {row["analyzer_device_id"]: row for row in tables["sample_reuse_by_device"]}
    assert by_device["001"]["reuse_decision_cn"] == "需复验"
    assert "a_grade_single_target_review_only" in by_device["001"]["reuse_reasons"]
    assert int(by_device["001"]["fit_sample_count"]) == 10
    assert int(by_device["001"]["preparation_rejected_count"]) == 0


def test_sample_reuse_review_cli_writes_no_write_artifacts(tmp_path):
    run_dir, pressure_path, plan_path, pressure_reference_path = _make_run(tmp_path)
    output_dir = tmp_path / "reuse_review"

    rc = reuse_cli(
        [
            "--run-dir",
            str(run_dir),
            "--plan-json",
            str(plan_path),
            "--pressure-reference-json",
            str(pressure_reference_path),
            "--pressure-check-csv",
            str(pressure_path),
            "--output-dir",
            str(output_dir),
            "--component",
            "co2",
            "--analyzer-prefix",
            "all",
            "--today",
            "2026-05-27",
        ]
    )

    assert rc == 0
    summary = _read_csv(output_dir / "sample_reuse_run_summary.csv")
    rows = _read_csv(output_dir / "sample_reuse_by_device.csv")
    assert summary[0]["opens_com_ports"] == "False"
    assert summary[0]["controls_water_or_gas_routes"] == "False"
    assert summary[0]["writes_coefficients"] == "False"
    assert {row["reuse_decision"] for row in rows} == {"needs_verification", "reject"}
    assert (output_dir / "sample_reuse_review_report.md").exists()
