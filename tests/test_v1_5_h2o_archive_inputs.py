import csv
import json

from gas_calibrator.tools.build_v1_5_h2o_archive_inputs import build_archive_inputs


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


def test_h2o_archive_inputs_preserve_dry_and_wet_ppmv_fields(tmp_path):
    run_parent = tmp_path / "h2o_open_flow"
    point_dir = run_parent / "p001_h2o"
    point_dir.mkdir(parents=True)
    _write_csv(
        point_dir / "samples_machine_readable.csv",
        [
            {
                "sample_index": 1,
                "sample_ts": "2026-06-17T01:00:01",
                "target_h2o_mmol": "0.5",
                "dewpoint_c": "-23.4",
                "ga01_mode2_tokens_json": json.dumps(["YGAS", "001", "0900.000", "00.500"]),
                "ga01_h2o_signal": "2631.0",
                "ga01_h2o_ratio_f": "0.70001",
                "ga01_h2o_mmol": "0.501",
            }
        ],
    )
    queue_run_dir = tmp_path / "queue_run"
    queue_run_dir.mkdir()
    (queue_run_dir / "queue_summary.json").write_text("{}", encoding="utf-8")
    _write_csv(
        queue_run_dir / "queue_manifest.csv",
        [
            {
                "point_id": "p001_h2o",
                "temperature_c": "20",
                "reference_h2o_mmol": "0.5",
                "min_purge_s": "360",
                "sample_count": "10",
            }
        ],
    )
    pressure_check_csv = tmp_path / "pressure_quick_check.csv"
    _write_csv(
        pressure_check_csv,
        [
            {
                "analyzer_prefix": "ga01",
                "analyzer_device_id": "001",
                "pressure_channel_status": "pass",
                "pressure_delta_hpa": "0.1",
            }
        ],
    )
    pressure_reference_json = tmp_path / "com22_pressure_reference.json"
    pressure_reference_json.write_text(
        json.dumps(
            {
                "device_id": "118288",
                "certificate_id": "FRGsz25038057",
                "certificate_hash": "pressure-cert-hash",
                "valid_until": "2027-01-01",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    standard_reference_json = tmp_path / "standard_h2o_reference.json"
    standard_reference_json.write_text(
        json.dumps(
            {
                "standard_gases": [
                    {
                        "component": "h2o",
                        "cylinder_id": "H2O-GEN-001",
                        "certificate_value": "dynamic_h2o_targets_from_dewpoint_reference",
                        "certificate_uncertainty": "dewpoint_reference_uncertainty",
                        "valid_until": "2027-01-01",
                        "supplier": "dewpoint-reference-chain",
                        "certificate_hash": "h2o-reference-hash",
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    outputs = build_archive_inputs(
        run_parent=run_parent,
        queue_run_dir=queue_run_dir,
        output_dir=tmp_path / "archive_inputs",
        candidate_review_dir=None,
        old_component_snapshot_dir=None,
        pressure_check_csv=pressure_check_csv,
        pressure_reference_json=pressure_reference_json,
        humidity_reference_json=None,
        standard_reference_json=standard_reference_json,
        operator="operator-a",
        calibration_date="2026-06-17",
    )

    rows = _read_csv(outputs["samples_csv"])
    plan = json.loads(outputs["plan_json"].read_text(encoding="utf-8"))

    assert rows[0]["pressure_mode"] == "ambient_open"
    assert rows[0]["h2o_wet_ppmv"] == "500"
    assert abs(float(rows[0]["h2o_dry_ppmv"]) - 500.250125) < 0.0001
    assert plan["standard_gases"][0]["component"] == "h2o"
    assert plan["standard_gases"][0]["certificate_hash"] == "h2o-reference-hash"
