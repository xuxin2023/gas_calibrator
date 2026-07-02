import csv
import json

from gas_calibrator.tools.prepare_v1_5_multipoint_candidate_run import (
    prepare_multipoint_candidate_run,
)


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


def test_multipoint_candidate_run_combines_samples_and_applies_certificate_target_override(tmp_path):
    run_a = tmp_path / "co2_100"
    run_b = tmp_path / "co2_900"
    run_a.mkdir()
    run_b.mkdir()
    _write_csv(
        run_a / "samples_machine_readable.csv",
        [
            {
                "sample_index": "1",
                "point_phase": "co2",
                "point_tag": "open_flow_100ppm",
                "co2_ppm_target": "100.0",
                "target_value": "100.0",
                "ga01_co2_ratio_f": "1.4",
            }
        ],
    )
    _write_csv(
        run_b / "samples_machine_readable.csv",
        [
            {
                "sample_index": "1",
                "point_phase": "co2",
                "point_tag": "open_flow_900ppm",
                "co2_ppm_target": "897.04",
                "target_value": "897.04",
                "ga01_co2_ratio_f": "1.2",
            }
        ],
    )
    (run_a / "formal_open_flow_sidecar_metadata.json").write_text(
        json.dumps({"run_id": "co2_100", "co2_source_ppm": 100.0, "certificate_co2_ppm": 100.0}),
        encoding="utf-8",
    )
    (run_b / "formal_open_flow_sidecar_metadata.json").write_text(
        json.dumps({"run_id": "co2_900", "co2_source_ppm": 900.0, "certificate_co2_ppm": 897.04}),
        encoding="utf-8",
    )

    outputs = prepare_multipoint_candidate_run(
        output_dir=tmp_path / "aggregate",
        run_dirs=[run_a, run_b],
        component="co2",
        target_overrides={"open_flow_100ppm": 99.94},
        run_id="aggregate_5pt",
    )

    rows = _read_csv(outputs["samples_csv"])
    assert len(rows) == 2
    row_100 = next(row for row in rows if row["point_tag"] == "open_flow_100ppm")
    assert row_100["target_co2_ppm"] == "99.94"
    assert row_100["co2_ppm_target"] == "99.94"
    assert row_100["co2_certificate_value"] == "99.94"
    assert row_100["certificate_target_override_applied"] == "true"
    assert row_100["point_id"] == "co2_100:open_flow_100ppm"
    assert row_100["point_key"] == "co2_100:open_flow_100ppm"
    assert row_100["source_point_identity"] == "co2_100:open_flow_100ppm"
    row_900 = next(row for row in rows if row["point_tag"] == "open_flow_900ppm")
    assert row_900["co2_ppm_target"] == "897.04"
    assert row_900["certificate_target_override_applied"] == "false"
    manifest = json.loads(outputs["manifest"].read_text(encoding="utf-8"))
    assert manifest["writes_coefficients"] is False
    assert manifest["controls_water_or_gas_routes"] is False


def test_multipoint_candidate_run_marks_verification_sources(tmp_path):
    fit_run = tmp_path / "fit_500"
    verify_run = tmp_path / "verify_500"
    fit_run.mkdir()
    verify_run.mkdir()
    for run_dir in (fit_run, verify_run):
        _write_csv(
            run_dir / "samples_machine_readable.csv",
            [
                {
                    "sample_index": "1",
                    "point_phase": "co2",
                    "point_tag": "open_flow_500ppm",
                    "co2_ppm_target": "500.13",
                    "ga01_co2_ratio_f": "1.3",
                }
            ],
        )
        (run_dir / "formal_open_flow_sidecar_metadata.json").write_text(
            json.dumps({"run_id": run_dir.name, "co2_source_ppm": 500.0}),
            encoding="utf-8",
        )

    outputs = prepare_multipoint_candidate_run(
        output_dir=tmp_path / "aggregate",
        run_dirs=[fit_run],
        verification_run_dirs=[verify_run],
        component="co2",
    )

    rows = _read_csv(outputs["samples_csv"])
    assert [row["sample_role"] for row in rows] == ["fit", "verification"]
    assert rows[1]["verification_point_id"] == "verify_500:open_flow_500ppm"
    assert rows[1]["point_id"] == "verify_500:open_flow_500ppm"
