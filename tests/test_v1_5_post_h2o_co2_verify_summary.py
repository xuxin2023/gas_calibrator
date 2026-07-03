import csv
import json

from gas_calibrator.tools.summarize_v1_5_post_h2o_co2_verify import summarize, write_outputs


def _write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_post_h2o_co2_summary_requires_all_target_devices_within_one_percent(tmp_path):
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_summary.json").write_text(
        json.dumps(
            {
                "control_temperature": True,
                "no_write": True,
                "sealed_pressure_control": False,
                "writes_senco": False,
                "writes_device_id": False,
            }
        ),
        encoding="utf-8",
    )
    _write_csv(
        queue_dir / "queue_manifest.csv",
        [
            {
                "point_run_id": "p001_T20_900ppm_verification",
                "source_nominal_ppm": "900",
                "co2_group": "B",
                "status": "ok",
                "command": "--certificate-co2-ppm 897.04",
            }
        ],
    )
    point_dir = tmp_path / "p001_T20_900ppm_verification"
    _write_csv(
        point_dir / "分析仪汇总_20260530.csv",
        [
            {
                "Analyzer": "GA022",
                "ppm_CO2_Tank": "897.04",
                "ppm_CO2": "897.50",
                "ValidFrames": "20",
                "TotalFrames": "20",
            },
            {
                "Analyzer": "GA030",
                "ppm_CO2_Tank": "897.04",
                "ppm_CO2": "890.00",
                "ValidFrames": "20",
                "TotalFrames": "20",
            },
            {
                "Analyzer": "GA033",
                "ppm_CO2_Tank": "897.04",
                "ppm_CO2": "902.00",
                "ValidFrames": "20",
                "TotalFrames": "20",
            },
            {
                "Analyzer": "GA051",
                "ppm_CO2_Tank": "897.04",
                "ppm_CO2": "899.00",
                "ValidFrames": "20",
                "TotalFrames": "20",
            },
            {
                "Analyzer": "GA100",
                "ppm_CO2_Tank": "897.04",
                "ppm_CO2": "3000.00",
                "ValidFrames": "20",
                "TotalFrames": "20",
            },
        ],
    )

    payload = summarize(queue_run_dir=queue_dir, output_dir=tmp_path, excluded_devices=("100",))

    assert payload["summary"]["overall_pass"] is True
    assert payload["summary"]["failed_pair_count"] == 0
    rows = payload["rows"]
    assert next(row for row in rows if row["device_id"] == "100")["status"] == "excluded"


def test_post_h2o_co2_summary_uses_runtime_analyzer_device_id_mapping(tmp_path):
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_summary.json").write_text(
        json.dumps(
            {
                "control_temperature": True,
                "no_write": True,
                "sealed_pressure_control": False,
                "writes_senco": False,
                "writes_device_id": False,
            }
        ),
        encoding="utf-8",
    )
    _write_csv(
        queue_dir / "queue_manifest.csv",
        [
            {
                "point_run_id": "p001_T20_900ppm_verification",
                "source_nominal_ppm": "900",
                "co2_group": "B",
                "status": "ok",
                "command": "--certificate-co2-ppm 897.04",
            }
        ],
    )
    point_dir = tmp_path / "p001_T20_900ppm_verification"
    point_dir.mkdir()
    (point_dir / "runtime_config_snapshot.json").write_text(
        json.dumps(
            {
                "devices": {
                    "gas_analyzers": [
                        {"name": "ga01", "device_id": "002"},
                        {"name": "ga05", "device_id": "100"},
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    _write_csv(
        point_dir / "analyzer_summary_20260530.csv",
        [
            {
                "Analyzer": "GA05",
                "ppm_CO2_Tank": "897.04",
                "ppm_CO2": "897.50",
                "ValidFrames": "20",
                "TotalFrames": "20",
            },
        ],
    )

    payload = summarize(
        queue_run_dir=queue_dir,
        output_dir=tmp_path,
        target_devices=("100",),
        excluded_devices=(),
        acceptance_pct=1.0,
    )

    assert payload["summary"]["overall_pass"] is True
    assert payload["summary"]["target_devices"] == ["100"]
    assert payload["rows"][0]["device_id"] == "100"
    assert payload["rows"][0]["analyzer_label"] == "GA05"
    assert payload["rows"][0]["status"] == "pass"


def test_post_h2o_co2_summary_blocks_when_one_target_fails_or_no_temp_control(tmp_path):
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_summary.json").write_text(
        json.dumps({"control_temperature": False, "no_write": True}),
        encoding="utf-8",
    )
    _write_csv(
        queue_dir / "queue_manifest.csv",
        [
            {
                "point_run_id": "p001_T20_100ppm_verification",
                "source_nominal_ppm": "100",
                "co2_group": "B",
                "status": "ok",
                "command": "--certificate-co2-ppm 99.94",
            }
        ],
    )
    _write_csv(
        tmp_path / "p001_T20_100ppm_verification" / "分析仪汇总_20260530.csv",
        [
            {"Analyzer": "GA022", "ppm_CO2_Tank": "99.94", "ppm_CO2": "153.30"},
            {"Analyzer": "GA030", "ppm_CO2_Tank": "99.94", "ppm_CO2": "100.00"},
            {"Analyzer": "GA033", "ppm_CO2_Tank": "99.94", "ppm_CO2": "100.00"},
            {"Analyzer": "GA051", "ppm_CO2_Tank": "99.94", "ppm_CO2": "100.00"},
        ],
    )

    payload = summarize(queue_run_dir=queue_dir, output_dir=tmp_path)
    outputs = write_outputs(payload, tmp_path / "out", "summary")

    assert payload["summary"]["overall_pass"] is False
    assert payload["summary"]["failed_pair_count"] == 1
    assert outputs["markdown"].exists()
