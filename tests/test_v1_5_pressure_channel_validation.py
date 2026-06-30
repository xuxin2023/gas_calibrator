import csv
import json

import pytest

from gas_calibrator.tools.export_v1_5_pressure_channel_validation import main as export_main
from gas_calibrator.tools.export_v1_5_pressure_senco9_evaluation import main as senco9_export_main
from gas_calibrator.validation.pressure_channel import (
    PressureSenco9FitConfig,
    build_pressure_channel_tables,
    build_pressure_senco9_fit_tables,
    detect_pressure_analyzer_prefixes,
    evaluate_pressure_channel_ambient,
    evaluate_pressure_channel_fleet,
    evaluate_pressure_senco9_fit,
    pressure_pair_rows,
    validate_pressure_reference_traceability,
    write_pressure_channel_report,
    write_pressure_quick_check_csv,
    write_pressure_senco9_fit_report,
)
from gas_calibrator.validation.reporting import _fs_path


def _reference(**overrides):
    data = {
        "device_id": "COM22-DPG-001",
        "certificate_id": "P-CERT-001",
        "certificate_uncertainty": 0.15,
        "valid_until": "2027-01-01",
        "certificate_hash": "pressure-cert-hash",
    }
    data.update(overrides)
    return data


def _row(index: int, *, analyzer_kpa: float = 100.05, com22_hpa: float = 1000.5, **overrides):
    row = {
        "sample_index": index,
        "sample_ts": f"2026-05-24T12:00:{index:02d}",
        "pressure_mode": "ambient_open",
        "ga01_frame_usable": "true",
        "ga01_pressure_kpa": analyzer_kpa + index * 0.0005,
        "pressure_gauge_hpa": com22_hpa + index * 0.005,
        "controller_pressure": com22_hpa + index * 0.005 + 0.2,
        "pressure_atmosphere_hold_status": "verified",
        "pressure_atmosphere_hold_active": "true",
        "pressure_atmosphere_hold_strategy": "legacy_hold_thread",
        "ga01_id": "010",
        "ga01_co2_ppm": 900.0,
        "ga01_h2o_mmol": 0.5,
    }
    row.update(overrides)
    return row


def _rows(count=5, **kwargs):
    return [_row(index, **kwargs) for index in range(1, count + 1)]


def _pressure_scan_rows(points=(500.0, 800.0, 1100.0), count=5, *, offset_kpa=0.7, slope=1.0):
    rows = []
    index = 1
    for target in points:
        for repeat in range(count):
            reference_hpa = float(target) + repeat * 0.02
            reference_kpa = reference_hpa / 10.0
            analyzer_kpa = (reference_kpa - offset_kpa) / slope
            rows.append(
                _row(
                    index,
                    analyzer_kpa=analyzer_kpa,
                    com22_hpa=reference_hpa,
                    pressure_mode="sealed_controlled",
                    pressure_target_hpa=target,
                    controller_pressure=reference_hpa + 0.1,
                )
            )
            index += 1
    return rows


def _fleet_rows(count=5):
    rows = _rows(count)
    for index, row in enumerate(rows, start=1):
        row.update(
            {
                "ga02_frame_usable": "true",
                "ga02_pressure_kpa": 100.04 + index * 0.0005,
                "ga02_analyzer_device_id": "029",
                "ga02_co2_ppm": 899.5,
                "ga02_h2o_mmol": 0.5,
            }
        )
    return rows


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


def test_pressure_reference_traceability_requires_valid_com22_certificate():
    result = validate_pressure_reference_traceability(_reference(), today="2026-05-24")
    assert result.status == "pass"
    assert result.validation_level == "formal_pressure_validation"

    missing = validate_pressure_reference_traceability({}, today="2026-05-24")
    assert missing.status == "fail"
    assert missing.validation_level == "engineering_diagnostic"
    assert "missing_device_id" in missing.reasons

    expired = validate_pressure_reference_traceability(
        _reference(valid_until="2026-01-01"),
        today="2026-05-24",
    )
    assert expired.status == "fail"
    assert "certificate_expired" in expired.reasons


def test_ambient_pressure_channel_validation_passes_with_traceable_com22():
    result = evaluate_pressure_channel_ambient(
        _rows(5),
        pressure_reference=_reference(),
        today="2026-05-24",
    )

    assert result.status == "pass"
    assert result.validation_level == "formal_pressure_validation"
    assert result.allowed_for_co2_h2o_formal_work is True
    assert result.valid_pair_count == 5
    assert result.analyzer_prefix == "ga01"
    assert result.analyzer_device_id == "010"
    assert abs(result.analyzer_minus_com22_mean_hpa) < 1.0
    assert result.measurement_model["fit_scope"] == "not_co2_h2o_fit_input"
    assert result.measurement_model["analyzer_identity_source"] == "MODE2/device_id"


def test_ambient_pressure_channel_validation_blocks_formal_when_com22_certificate_missing():
    result = evaluate_pressure_channel_ambient(
        _rows(5),
        pressure_reference={},
        today="2026-05-24",
    )

    assert result.status == "pass"
    assert result.validation_level == "engineering_diagnostic"
    assert result.allowed_for_co2_h2o_formal_work is False
    assert "pressure_reference_traceability_failed" in result.reason


def test_ambient_pressure_channel_validation_fails_when_pressure_bias_is_large():
    result = evaluate_pressure_channel_ambient(
        _rows(5, analyzer_kpa=101.5, com22_hpa=1000.0),
        pressure_reference=_reference(),
        today="2026-05-24",
    )

    assert result.status == "fail"
    assert result.allowed_for_co2_h2o_formal_work is False
    assert "mean_abs_delta_hpa" in result.reason


def test_ambient_pressure_channel_validation_requires_continuous_atmosphere_evidence():
    rows = _rows(5)
    for row in rows:
        row.pop("pressure_atmosphere_hold_status", None)
        row.pop("pressure_atmosphere_hold_active", None)

    paired, rejected = pressure_pair_rows(rows)
    assert len(paired) == 0
    assert len(rejected) == 5
    assert "continuous_atmosphere_hold_not_verified(<missing>)" in rejected[0]["reject_reasons"]

    result = evaluate_pressure_channel_ambient(
        rows,
        pressure_reference=_reference(),
        today="2026-05-24",
    )
    assert result.status == "insufficient_evidence"
    assert result.allowed_for_co2_h2o_formal_work is False


def test_ambient_pressure_channel_validation_rejects_non_ambient_rows_and_insufficient_pairs():
    rows = [
        _row(1, pressure_mode="sealed_controlled"),
        _row(2, ga01_pressure_kpa=""),
        _row(3),
    ]

    paired, rejected = pressure_pair_rows(rows)
    assert len(paired) == 1
    assert len(rejected) == 2
    reasons = ";".join(row["reject_reasons"] for row in rejected)
    assert "non_ambient_pressure_mode(sealed_controlled)" in reasons
    assert "missing_analyzer_pressure_kpa" in reasons

    result = evaluate_pressure_channel_ambient(
        rows,
        pressure_reference=_reference(),
        today="2026-05-24",
    )
    assert result.status == "insufficient_evidence"
    assert "pressure_pair_count<3" in result.reason


def test_pressure_channel_tables_include_summary_pairs_and_traceability():
    tables = build_pressure_channel_tables(
        _rows(5),
        pressure_reference=_reference(),
        today="2026-05-24",
    )

    assert tables["pressure_validation_summary"][0]["status"] == "pass"
    assert tables["pressure_validation_summary"][0]["analyzer_device_id"] == "010"
    assert tables["pressure_reference_traceability"][0]["device_id"] == "COM22-DPG-001"
    assert len(tables["paired_samples"]) == 5
    assert tables["paired_samples"][0]["analyzer_device_id"] == "010"
    assert tables["measurement_model"][0]["primary_reference"] == "COM22 digital pressure gauge"
    assert tables["measurement_model"][0]["analyzer_device_id"] == "010"


def test_pressure_channel_fleet_validates_every_detected_analyzer_prefix():
    rows = _fleet_rows(5)

    assert detect_pressure_analyzer_prefixes(rows) == ["ga01", "ga02"]

    results = evaluate_pressure_channel_fleet(
        rows,
        pressure_reference=_reference(),
        today="2026-05-24",
    )
    assert [result.analyzer_prefix for result in results] == ["ga01", "ga02"]
    assert [result.analyzer_device_id for result in results] == ["010", "029"]
    assert all(result.allowed_for_co2_h2o_formal_work for result in results)

    tables = build_pressure_channel_tables(
        rows,
        pressure_reference=_reference(),
        analyzer_prefix="all",
        today="2026-05-24",
    )
    assert [row["analyzer_prefix"] for row in tables["pressure_validation_summary"]] == ["ga01", "ga02"]
    assert [row["analyzer_device_id"] for row in tables["pressure_validation_summary"]] == ["010", "029"]
    assert len(tables["paired_samples"]) == 10


def test_non_primary_analyzer_does_not_reuse_primary_pressure_fields():
    rows = _rows(5)
    for row in rows:
        row["ga02_frame_usable"] = ""

    paired, rejected = pressure_pair_rows(rows, analyzer_prefix="ga02")

    assert paired == []
    assert len(rejected) == 5
    assert {row["analyzer_device_id"] for row in rejected} == {""}
    assert all(row["analyzer_pressure_kpa"] in (None, "") for row in rejected)
    assert all("missing_analyzer_pressure_kpa" in row["reject_reasons"] for row in rejected)


def test_pressure_quick_check_csv_shortens_long_run_id_under_deep_output_dir(tmp_path):
    deep = tmp_path / ("deep_path_segment_" * 6)
    path = write_pressure_quick_check_csv(
        deep,
        _rows(3),
        run_id="pressure_quick_real_no_write_20260525_134111_ga02_with_extra_context_that_would_overflow_windows_paths",
    )

    assert path.exists()
    assert len(str(path)) < 260
    assert "pressure_channel_quick_check_" in path.name


def test_pressure_channel_tables_detect_prefixes_from_quick_check_long_rows():
    rows = []
    for prefix, device_id, pressure_kpa in (
        ("ga01", "010", 100.05),
        ("ga02", "029", 100.04),
    ):
        for index in range(1, 4):
            rows.append(
                {
                    "sample_index": index,
                    "sample_ts": f"2026-05-24T12:00:{index:02d}",
                    "pressure_mode": "ambient_open",
                    "pressure_channel_row_status": "paired",
                    "analyzer_prefix": prefix,
                    "analyzer_device_id": device_id,
                    "analyzer_pressure_kpa": pressure_kpa,
                    "com22_pressure_hpa": 1000.5,
                    "pressure_atmosphere_hold_status": "verified",
                    "pressure_atmosphere_hold_active": "true",
                }
            )

    assert detect_pressure_analyzer_prefixes(rows) == ["ga01", "ga02"]
    tables = build_pressure_channel_tables(
        rows,
        pressure_reference=_reference(),
        analyzer_prefix="all",
        today="2026-05-24",
    )
    assert [row["analyzer_prefix"] for row in tables["pressure_validation_summary"]] == ["ga01", "ga02"]


def test_pressure_channel_tables_accept_device_id_prefix_from_archive_rows():
    rows = []
    for index in range(1, 4):
        rows.append(
            {
                "sample_index": index,
                "sample_ts": f"2026-05-30T12:00:{index:02d}",
                "pressure_mode": "ambient_open",
                "pressure_channel_row_status": "paired",
                "source_analyzer_prefix": "ga03",
                "analyzer_prefix": "ga022",
                "analyzer_device_id": "022",
                "analyzer_pressure_kpa": 100.05,
                "com22_pressure_hpa": 1000.5,
                "pressure_atmosphere_hold_status": "verified",
                "pressure_atmosphere_hold_active": "true",
            }
        )

    assert detect_pressure_analyzer_prefixes(rows) == ["ga022"]
    tables = build_pressure_channel_tables(
        rows,
        pressure_reference=_reference(),
        analyzer_prefix="all",
        today="2026-05-30",
    )

    summary = tables["pressure_validation_summary"][0]
    assert summary["analyzer_prefix"] == "ga022"
    assert summary["analyzer_device_id"] == "022"
    assert summary["status"] == "pass"
    assert len(tables["paired_samples"]) == 3


def test_pressure_channel_report_and_cli_write_sidecar_artifacts(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    rows = _rows(5)
    samples_path = run_dir / "pressure_channel_quick_check_20260524.csv"
    _write_csv(samples_path, rows)
    reference_path = tmp_path / "pressure_reference.json"
    reference_path.write_text(json.dumps(_reference(), ensure_ascii=False), encoding="utf-8")

    outputs = write_pressure_channel_report(
        run_dir=run_dir,
        pressure_reference_path=reference_path,
        output_dir=tmp_path / "pressure_report",
        today="2026-05-24",
    )
    assert outputs["workbook"].exists()
    summary = _read_csv(outputs["pressure_validation_summary_csv"])
    assert summary[0]["allowed_for_co2_h2o_formal_work"] == "True"

    cli_dir = tmp_path / "cli_report"
    rc = export_main(
        [
            "--run-dir",
            str(run_dir),
            "--pressure-reference-json",
            str(reference_path),
            "--output-dir",
            str(cli_dir),
        ]
    )
    assert rc == 0
    assert (cli_dir / "pressure_validation_summary.csv").exists()


def test_pressure_channel_report_writes_traceability_under_deep_output_path(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    samples_path = run_dir / "pressure_channel_quick_check_20260524.csv"
    _write_csv(samples_path, _rows(5))

    output_dir = tmp_path / "pressure_channel_validation"
    while len(str((output_dir / "pressure_reference_traceability.csv").resolve())) < 265:
        output_dir = output_dir / "pressure_senco9_no_write_collection"

    outputs = write_pressure_channel_report(
        run_dir=run_dir,
        pressure_reference_path=None,
        output_dir=output_dir,
        samples_csv=samples_path,
        today="2026-05-24",
    )

    assert len(str(outputs["pressure_reference_traceability_csv"])) >= 265
    assert _fs_path(outputs["pressure_reference_traceability_csv"]).exists()
    assert _fs_path(outputs["workbook"]).exists()


def test_pressure_senco9_fit_blocks_single_atmosphere_point_even_with_large_offset():
    result = evaluate_pressure_senco9_fit(
        _rows(12, analyzer_kpa=99.25, com22_hpa=1000.0),
        pressure_reference=_reference(),
        today="2026-05-24",
    )

    assert result.status == "insufficient_evidence"
    assert result.write_allowed is False
    assert result.recommendation == "collect_no_write_multi_point_pressure_data"
    assert "distinct_pressure_points<3" in result.reason
    assert "reference_pressure_span_hpa" in result.reason
    assert result.senco9_candidate_command.startswith("SENCO9,YGAS,FFF,")


def test_pressure_senco9_fit_supports_no_write_offset_candidate_for_multi_point_scan():
    result = evaluate_pressure_senco9_fit(
        _pressure_scan_rows(offset_kpa=0.7),
        pressure_reference=_reference(),
        today="2026-05-24",
    )

    assert result.status == "pass"
    assert result.write_allowed is False
    assert result.recommendation == "review_senco9_offset_candidate_no_write"
    assert result.offset_only_offset_kpa == pytest.approx(0.7, abs=0.002)
    assert result.linear_slope == pytest.approx(1.0, abs=0.001)
    assert result.senco9_candidate_command.startswith("SENCO9,YGAS,FFF,7.000")


def test_pressure_senco9_fit_can_discard_pressure_transition_frames():
    rows = []
    index = 1
    for target in (500.0, 800.0, 1100.0):
        for repeat in range(5):
            reference_hpa = target + repeat * 0.02
            reference_kpa = reference_hpa / 10.0
            analyzer_kpa = reference_kpa - 0.7
            if repeat == 0:
                analyzer_kpa = (reference_hpa + 100.0) / 10.0 - 0.7
            rows.append(
                _row(
                    index,
                    analyzer_kpa=analyzer_kpa,
                    com22_hpa=reference_hpa,
                    pressure_mode="pace_no_write_controlled",
                    pressure_target_hpa=target,
                    sample_index=repeat + 1,
                    controller_pressure=reference_hpa + 0.1,
                )
            )
            index += 1

    untrimmed = evaluate_pressure_senco9_fit(
        rows,
        pressure_reference=_reference(),
        today="2026-05-24",
    )
    trimmed = evaluate_pressure_senco9_fit(
        rows,
        pressure_reference=_reference(),
        cfg=PressureSenco9FitConfig(discard_initial_samples_per_pressure_point=1),
        today="2026-05-24",
    )
    tables = build_pressure_senco9_fit_tables(
        rows,
        pressure_reference=_reference(),
        cfg=PressureSenco9FitConfig(discard_initial_samples_per_pressure_point=1),
        today="2026-05-24",
    )

    assert untrimmed.status == "fail"
    assert trimmed.status == "pass"
    assert trimmed.recommendation == "review_senco9_offset_candidate_no_write"
    assert trimmed.offset_only_residual_max_abs_hpa < 0.5
    assert any(
        "pressure_transition_sample_discard" in row["reject_reasons"]
        for row in tables["pressure_fit_rejected_rows"]
    )


def test_pressure_senco9_fit_rejects_offset_only_when_slope_bias_is_large():
    result = evaluate_pressure_senco9_fit(
        _pressure_scan_rows(offset_kpa=0.2, slope=0.95),
        pressure_reference=_reference(),
        today="2026-05-24",
    )

    assert result.status == "fail"
    assert result.write_allowed is False
    assert result.recommendation == "do_not_write_offset_only_senco9_investigate_scale_or_model"
    assert abs(result.linear_slope_bias) > 0.02


def test_pressure_senco9_fit_tables_and_cli_are_no_write_sidecar_artifacts(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    rows = _pressure_scan_rows(offset_kpa=0.7)
    samples_path = run_dir / "pressure_scan.csv"
    _write_csv(samples_path, rows)
    reference_path = tmp_path / "pressure_reference.json"
    reference_path.write_text(json.dumps(_reference(), ensure_ascii=False), encoding="utf-8")

    tables = build_pressure_senco9_fit_tables(
        rows,
        pressure_reference=_reference(),
        analyzer_prefix="ga01",
        today="2026-05-24",
    )
    assert tables["pressure_fit_summary"][0]["write_allowed"] is False
    assert len(tables["pressure_fit_point_means"]) == 3
    assert tables["pressure_fit_point_means"][0]["analyzer_device_id"] == "010"
    assert tables["pressure_fit_summary"][0]["recommendation"] == "review_senco9_offset_candidate_no_write"

    outputs = write_pressure_senco9_fit_report(
        run_dir=run_dir,
        samples_csv=samples_path,
        pressure_reference_path=reference_path,
        output_dir=tmp_path / "senco9_report",
        today="2026-05-24",
    )
    assert outputs["workbook"].exists()
    assert outputs["review_report"].exists()
    summary = _read_csv(outputs["pressure_fit_summary_csv"])
    assert summary[0]["write_allowed"] == "False"
    assert summary[0]["recommendation"] == "review_senco9_offset_candidate_no_write"
    review_text = outputs["review_report"].read_text(encoding="utf-8")
    assert "No-Write Review" in review_text
    assert "ga01" in review_text
    assert "no SENCO9 write" in review_text

    cli_dir = tmp_path / "senco9_cli"
    rc = senco9_export_main(
        [
            "--run-dir",
            str(run_dir),
            "--samples-csv",
            str(samples_path),
            "--pressure-reference-json",
            str(reference_path),
            "--output-dir",
            str(cli_dir),
        ]
    )
    assert rc == 0
    assert (cli_dir / "pressure_fit_summary.csv").exists()


def test_pressure_senco9_report_prefers_full_samples_over_quick_check_sidecar(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    samples_path = run_dir / "samples_20260607_160006.csv"
    quick_check_path = run_dir / "pressure_channel_quick_check_latest.csv"
    _write_csv(samples_path, _pressure_scan_rows(offset_kpa=0.7))
    _write_csv(
        quick_check_path,
        _rows(3, analyzer_kpa=99.25, com22_hpa=1000.0),
    )
    reference_path = tmp_path / "pressure_reference.json"
    reference_path.write_text(json.dumps(_reference(), ensure_ascii=False), encoding="utf-8")

    outputs = write_pressure_senco9_fit_report(
        run_dir=run_dir,
        pressure_reference_path=reference_path,
        output_dir=tmp_path / "senco9_report",
        today="2026-06-07",
    )

    summary = _read_csv(outputs["pressure_fit_summary_csv"])
    assert summary[0]["recommendation"] == "review_senco9_offset_candidate_no_write"
    assert summary[0]["valid_pair_count"] == "15"
    assert summary[0]["distinct_pressure_points"] == "3"


def test_pressure_channel_report_cli_supports_all_analyzer_prefixes(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    samples_path = run_dir / "pressure_channel_quick_check_20260524.csv"
    _write_csv(samples_path, _fleet_rows(5))
    reference_path = tmp_path / "pressure_reference.json"
    reference_path.write_text(json.dumps(_reference(), ensure_ascii=False), encoding="utf-8")

    cli_dir = tmp_path / "cli_report"
    rc = export_main(
        [
            "--run-dir",
            str(run_dir),
            "--pressure-reference-json",
            str(reference_path),
            "--output-dir",
            str(cli_dir),
            "--analyzer-prefix",
            "all",
        ]
    )

    assert rc == 0
    summary = _read_csv(cli_dir / "pressure_validation_summary.csv")
    assert [row["analyzer_prefix"] for row in summary] == ["ga01", "ga02"]
