import csv
import json

from gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue import (
    _build_point_command,
    _load_queue_rows,
    _ordered_temperature_groups,
    _parse_args,
    _point_run_id,
    _prepare_temperature_runtime_cfg,
    _select_queue_rows,
    main,
)


def _write_queue(path):
    rows = [
        {
            "point_id": "h2o_T20_HGEN20C_50RH_ambient",
            "component": "h2o",
            "temp_c": "20",
            "hgen_temp_c": "20",
            "hgen_rh_pct": "50",
            "reference_dewpoint_c": "9.26",
            "reference_h2o_mmol": "11.6416",
            "sample_role": "fit",
            "purge_s": "360",
            "sample_count": "10",
            "analyzer_acquisition": "active_stream_1hz",
        },
        {
            "point_id": "h2o_T10_HGEN10C_30RH_ambient",
            "component": "h2o",
            "temp_c": "10",
            "hgen_temp_c": "10",
            "hgen_rh_pct": "30",
            "reference_dewpoint_c": "-6.75",
            "reference_h2o_mmol": "3.647",
            "sample_role": "fit",
            "purge_s": "360",
            "sample_count": "10",
            "analyzer_acquisition": "active_stream_1hz",
        },
        {
            "point_id": "co2_T20_900ppm_ambient",
            "component": "co2",
            "temp_c": "20",
            "hgen_temp_c": "",
            "hgen_rh_pct": "",
            "reference_dewpoint_c": "",
            "reference_h2o_mmol": "",
            "sample_role": "fit",
            "purge_s": "360",
            "sample_count": "10",
            "analyzer_acquisition": "",
        },
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_h2o_queue_loader_filters_to_h2o_and_orders_temperature_asc(tmp_path):
    queue_path = tmp_path / "h2o_runner_queue.csv"
    _write_queue(queue_path)

    rows = _load_queue_rows(queue_path)
    selected = _select_queue_rows(rows, temps=None, max_points=None)
    groups = _ordered_temperature_groups(selected, order="asc")

    assert [temp for temp, _ in groups] == [10.0, 20.0]
    assert [item["hgen_rh_pct"] for item in groups[0][1]] == [30.0]
    assert len(selected) == 2


def test_h2o_point_command_keeps_no_write_open_flow_sidecar_contract(tmp_path):
    args = _parse_args(
        [
            "--config",
            "config.json",
            "--queue-csv",
            "queue.csv",
            "--output-dir",
            str(tmp_path),
            "--no-prompt",
            "--no-ftd-write",
        ]
    )
    cmd = _build_point_command(
        config_path="config.json",
        output_dir=tmp_path,
        row={
            "temp_c": 20.0,
            "hgen_temp_c": 20.0,
            "hgen_rh_pct": 70.0,
            "reference_dewpoint_c": 14.36,
            "reference_h2o_mmol": 16.3715,
            "purge_s": 360.0,
            "sample_count": 10,
            "analyzer_acquisition": "active_stream_1hz",
        },
        run_id="point_run",
        args=args,
    )

    text = " ".join(cmd)
    assert "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_sampling" in text
    assert "--co2-source-ppm" not in cmd
    assert "--pressure-target-hpa" not in cmd
    assert "--no-ftd-write" in cmd
    assert "--no-prompt" in cmd
    assert "--hgen-temp 20" in text
    assert "--hgen-rh 70" in text
    assert "--certificate-h2o-mmol 16.3715" in text
    assert "--h2o-pressure-presample-policy warn" in text


def test_h2o_point_command_uses_720s_default_when_plan_does_not_override(tmp_path):
    args = _parse_args(
        [
            "--config",
            "config.json",
            "--queue-csv",
            "queue.csv",
            "--output-dir",
            str(tmp_path),
            "--no-prompt",
        ]
    )
    cmd = _build_point_command(
        config_path="config.json",
        output_dir=tmp_path,
        row={
            "temp_c": 40.0,
            "hgen_temp_c": 30.0,
            "hgen_rh_pct": 50.0,
            "reference_dewpoint_c": 18.43,
            "reference_h2o_mmol": 21.3229,
            "sample_count": 10,
            "analyzer_acquisition": "active_stream_1hz",
        },
        run_id="point_run",
        args=args,
    )

    assert cmd[cmd.index("--purge-s") + 1] == "720"


def test_h2o_temperature_runtime_inherits_formal_soak_without_shortening(tmp_path):
    cfg = {
        "paths": {"output_dir": str(tmp_path / "logs")},
        "devices": {
            "temperature_chamber": {"enabled": True, "port": "COM19"},
            "pressure_controller": {"enabled": True},
            "pressure_gauge": {"enabled": True},
            "dewpoint_meter": {"enabled": True},
            "humidity_generator": {"enabled": True},
            "relay": {"enabled": True},
            "relay_8": {"enabled": True},
            "thermometer": {"enabled": True, "port": "COM18"},
        },
        "workflow": {
            "stability": {
                "temperature": {
                    "soak_after_reach_s": 1800,
                    "analyzer_chamber_temp_span_c": 0.08,
                }
            }
        },
    }

    runtime_cfg = _prepare_temperature_runtime_cfg(
        cfg,
        output_dir=tmp_path / "out",
        analyzer_acquisition="active_stream_1hz",
        allow_ftd_write=False,
        sample_interval_s=1.0,
        sensor_read_interval_s=5.0,
        soak_after_reach_s=None,
        tol_c=None,
        timeout_s=None,
        hard_max_wait_s=None,
        analyzer_span_c=None,
        analyzer_window_s=None,
        analyzer_timeout_s=None,
    )

    temp_cfg = runtime_cfg["workflow"]["stability"]["temperature"]
    assert temp_cfg["soak_after_reach_s"] == 1800
    assert runtime_cfg["workflow"]["skip_h2o"] is True
    assert runtime_cfg["metadata"]["writes_senco"] is False
    assert runtime_cfg["devices"]["temperature_chamber"]["enabled"] is True
    assert runtime_cfg["devices"]["pressure_controller"]["enabled"] is False
    assert runtime_cfg["devices"]["dewpoint_meter"]["enabled"] is False
    assert runtime_cfg["devices"]["humidity_generator"]["enabled"] is False
    assert runtime_cfg["devices"]["relay"]["enabled"] is False
    assert runtime_cfg["devices"]["thermometer"]["enabled"] is True


def test_h2o_point_run_id_is_short_for_windows_artifact_paths():
    run_id = _point_run_id(index=12, temp_c=40.0, hgen_temp_c=30.0, hgen_rh_pct=70.0)

    assert run_id == "p012_T40_HG30C_70RH_h2o"
    assert len(run_id) < 64


def test_h2o_queue_dry_run_writes_manifest_without_real_com(tmp_path):
    queue_path = tmp_path / "h2o_runner_queue.csv"
    _write_queue(queue_path)
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "paths": {"output_dir": str(tmp_path / "logs")},
                "devices": {"temperature_chamber": {"enabled": False, "port": "COM19", "baud": 9600, "addr": 1}},
            }
        ),
        encoding="utf-8",
    )

    rc = main(
        [
            "--config",
            str(config_path),
            "--queue-csv",
            str(queue_path),
            "--output-dir",
            str(tmp_path / "out"),
            "--run-id",
            "queue_dry",
            "--dry-run",
            "--no-prompt",
        ]
    )

    assert rc == 0
    manifest = tmp_path / "out" / "queue_dry" / "queue_manifest.csv"
    summary = tmp_path / "out" / "queue_dry" / "queue_summary.json"
    assert manifest.exists()
    assert summary.exists()
    assert "dry_run" in manifest.read_text(encoding="utf-8-sig")
    payload = json.loads(summary.read_text(encoding="utf-8"))
    assert payload["selected_points"] == 2
    assert payload["dry_run_points"] == 2
    assert payload["sealed_pressure_control"] is False
    assert payload["writes_senco"] is False
