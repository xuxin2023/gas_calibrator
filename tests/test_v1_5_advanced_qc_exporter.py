import csv
import json

from gas_calibrator.tools.export_v1_5_advanced_qc import main as advanced_qc_main
from gas_calibrator.v1_5.qc_advanced.exporter import (
    build_advanced_qc_summary,
    write_advanced_qc_summary,
)
from gas_calibrator.v1_5.review_surface import build_review_surface_model


def _row(index: int, component: str, **overrides):
    row = {
        "sample_index": index,
        "point_phase": component,
        "route": component,
        "pressure_mode": "ambient_open",
        "pressure_gauge_hpa": 1000.0,
        "controller_pressure": 1000.1,
        "dewpoint_c": -30.0 + index * 0.001,
        "ga01_frame_usable": "true",
        "ga01_co2_ppm": 900.0 + index * 0.01,
        "ga01_h2o_mmol": 0.5 + index * 0.0001,
        "ga01_pressure_kpa": 100.0,
        "ga01_co2_ratio_f": 1.3 + index * 0.00001,
        "ga01_h2o_ratio_f": 0.7 + index * 0.00001,
        "ga01_ref_signal": 3300.0,
        "ga01_co2_signal": 4300.0,
        "ga01_h2o_signal": 2600.0,
        "ga01_chamber_temp_c": 25.0,
        "ga01_case_temp_c": 25.5,
        "h2o_dry_ppmv": 500.0,
        "h2o_wet_ppmv": 505.0,
    }
    row.update(overrides)
    return row


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


def _make_run(tmp_path, *, moisture_release=False, include_sealed=True):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    rows = []
    rows.extend(_row(i, "co2") for i in range(1, 11))
    for i in range(11, 21):
        dry = 500.0 + (i - 10) * 0.2 if moisture_release else 500.0
        rows.append(_row(i, "h2o", h2o_dry_ppmv=dry, dewpoint_c=-30.0 + (i - 10) * 0.1))
    if include_sealed:
        rows.extend(
            _row(
                100 + i,
                "co2",
                pressure_mode="sealed_controlled",
                ga01_co2_ppm=9999.0,
                point_tag="sealed_pressure_diagnostic",
            )
            for i in range(3)
        )
    _write_csv(run_dir / "samples_20260524.csv", rows)
    return run_dir


def test_advanced_qc_summary_reads_samples_and_excludes_sealed_diagnostics(tmp_path):
    run_dir = _make_run(tmp_path, moisture_release=False, include_sealed=True)

    summary = build_advanced_qc_summary(run_dir=run_dir)

    assert summary["sidecar_only"] is True
    assert summary["opens_com_ports"] is False
    assert summary["controls_water_or_gas_routes"] is False
    assert summary["controls_valves_or_pace"] is False
    assert summary["writes_coefficients"] is False
    assert summary["formal_fit_boundary"]["open_flow_rows_only"] is True
    assert summary["formal_fit_boundary"]["excluded_row_count"] == 3
    assert summary["components"]["co2"]["sample_count"] == 10
    assert summary["components"]["h2o"]["sample_count"] == 10
    assert summary["pressure_source"] == "sample_rows_fallback"


def test_advanced_qc_summary_flags_real_moisture_release_and_feeds_review_surface(tmp_path):
    run_dir = _make_run(tmp_path, moisture_release=True, include_sealed=False)

    summary = build_advanced_qc_summary(run_dir=run_dir)
    review = build_review_surface_model(advanced_qc=summary)

    assert summary["status"] == "fail"
    assert summary["components"]["h2o"]["humidity"]["classification"] == "real_moisture_release"
    assert summary["root_cause"]["status"] == "reject_point"
    assert "real_moisture_release" in summary["root_cause"]["root_cause_codes"]
    assert review["overall_status"] == "blocked"
    assert "real_moisture_release" in review["advanced_qc_summary"]["blockers"]


def test_advanced_qc_writer_and_cli(tmp_path):
    run_dir = _make_run(tmp_path, moisture_release=False, include_sealed=True)

    outputs = write_advanced_qc_summary(run_dir=run_dir, output_dir=tmp_path / "advanced")
    assert outputs["summary_json"].exists()
    assert outputs["summary_markdown"].exists()
    payload = json.loads(outputs["summary_json"].read_text(encoding="utf-8"))
    assert payload["schema_version"] == "v1_5_advanced_qc_summary_v0"

    rc = advanced_qc_main(
        [
            "--run-dir",
            str(run_dir),
            "--output-dir",
            str(tmp_path / "cli_advanced"),
        ]
    )
    assert rc == 0
    assert (tmp_path / "cli_advanced" / "advanced_qc_summary.json").exists()
    assert (tmp_path / "cli_advanced" / "advanced_qc_summary.md").exists()
