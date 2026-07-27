import csv
import json
import types

from gas_calibrator.tools import run_v1_5_formal_h2o_open_flow_queue as h2o_queue_module
from gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue import (
    _build_point_command,
    _load_queue_rows,
    _ordered_temperature_groups,
    _parse_args,
    _point_run_id,
    _prepare_humidity_prewarm_runtime_cfg,
    _prepare_temperature_runtime_cfg,
    _queue_dir_name,
    _queue_output_dir,
    _select_queue_rows,
    _temperature_settle_run_id,
    _write_queue_exclusion_evidence,
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
            "--engineering-probe-only",
            "--operator-confirmation",
            h2o_queue_module.V1_5_ENGINEERING_PROBE_CONFIRMATION_TEXT,
            "--analyzer-gate-prefer-all-stable-grace-s",
            "75",
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
    assert "--engineering-probe-only" in cmd
    assert (
        "--operator-confirmation "
        + h2o_queue_module.V1_5_ENGINEERING_PROBE_CONFIRMATION_TEXT
    ) in text
    assert "--hgen-temp 20" in text
    assert "--hgen-rh 70" in text
    assert "--certificate-h2o-mmol 16.3715" in text
    assert "--h2o-pressure-presample-policy skip" in text
    assert "--analyzer-gate-prefer-all-stable-grace-s 75" in text
    assert "--keep-hgen-running-after-point" in cmd


def test_h2o_point_command_can_opt_into_old_safe_stop_each_point(tmp_path):
    args = _parse_args(
        [
            "--config",
            "config.json",
            "--queue-csv",
            "queue.csv",
            "--output-dir",
            str(tmp_path),
            "--safe-stop-hgen-each-point",
            "--no-prompt",
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
            "sample_count": 10,
            "analyzer_acquisition": "active_stream_1hz",
        },
        run_id="point_run",
        args=args,
    )

    assert "--keep-hgen-running-after-point" not in cmd


def test_h2o_temperature_settle_run_id_stays_short_for_windows_paths():
    assert _temperature_settle_run_id(10.0) == "T10_temp_settle"
    assert _temperature_settle_run_id(-20.0) == "Tm20_temp_settle"


def test_h2o_temperature_settle_creates_missing_output_dir_before_logging(tmp_path, monkeypatch):
    def _raise_before_real_devices(*_args, **_kwargs):
        raise RuntimeError("stop before real devices")

    monkeypatch.setattr(h2o_queue_module, "_build_devices", _raise_before_real_devices)
    output_dir = tmp_path / "missing" / "h2o_open_flow"
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

    ok = h2o_queue_module._settle_temperature_group(
        {
            "devices": {
                "temperature_chamber": {"enabled": True},
                "thermometer": {"enabled": True},
            }
        },
        temp_c=10.0,
        output_dir=output_dir,
        run_id="h2o_T10_temperature_settle",
        args=args,
    )

    assert ok is False
    assert output_dir.exists()
    assert (output_dir / "h2o_T10_temperature_settle" / "temperature_settle_summary.json").exists()


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
    assert cmd[cmd.index("--minimum-purge-s") + 1] == "720"


def test_h2o_queue_uses_900s_for_low_water_anchor_without_explicit_purge(tmp_path):
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
            "temp_c": 20.0,
            "hgen_temp_c": 20.0,
            "hgen_rh_pct": 5.0,
            "reference_dewpoint_c": -30.0,
            "reference_h2o_mmol": 0.4,
            "sample_count": 10,
            "analyzer_acquisition": "active_stream_1hz",
            "low_water_anchor": "true",
        },
        run_id="point_run",
        args=args,
    )

    assert cmd[cmd.index("--purge-s") + 1] == "900"
    assert cmd[cmd.index("--minimum-purge-s") + 1] == "900"


def test_h2o_queue_preserves_plan_purge_over_unknown_route_profile(tmp_path):
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
            "temp_c": 20.0,
            "hgen_temp_c": 20.0,
            "hgen_rh_pct": 50.0,
            "reference_dewpoint_c": 9.26,
            "reference_h2o_mmol": 11.6416,
            "purge_s": 780.0,
            "initial_state": "unknown",
            "sample_count": 10,
            "analyzer_acquisition": "active_stream_1hz",
        },
        run_id="point_run",
        args=args,
    )

    assert cmd[cmd.index("--purge-s") + 1] == "780"
    assert cmd[cmd.index("--minimum-purge-s") + 1] == "720"


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
    assert temp_cfg["temperature_truth_source"] == "in_chamber_platinum_resistance_digital_thermometer"
    assert temp_cfg["thermometer_truth_required"] is True
    assert temp_cfg["temperature_chamber_setpoint_substitution_forbidden"] is True
    assert runtime_cfg["workflow"]["skip_h2o"] is True
    assert runtime_cfg["metadata"]["writes_senco"] is False
    assert runtime_cfg["devices"]["temperature_chamber"]["enabled"] is True
    assert runtime_cfg["devices"]["pressure_controller"]["enabled"] is False
    assert runtime_cfg["devices"]["dewpoint_meter"]["enabled"] is False
    assert runtime_cfg["devices"]["humidity_generator"]["enabled"] is False
    assert runtime_cfg["devices"]["relay"]["enabled"] is False
    assert runtime_cfg["devices"]["thermometer"]["enabled"] is True


def test_h2o_humidity_prewarm_runtime_only_opens_humidity_generator(tmp_path):
    cfg = {
        "paths": {"output_dir": str(tmp_path / "logs")},
        "devices": {
            "temperature_chamber": {"enabled": True, "port": "COM19"},
            "pressure_controller": {"enabled": True},
            "pressure_gauge": {"enabled": True},
            "dewpoint_meter": {"enabled": True},
            "humidity_generator": {"enabled": False, "port": "COM16"},
            "relay": {"enabled": True},
            "relay_8": {"enabled": True},
            "thermometer": {"enabled": True},
            "gas_analyzer": {"enabled": True},
            "gas_analyzers": [{"enabled": True, "port": "COM35"}],
        },
    }

    runtime_cfg = _prepare_humidity_prewarm_runtime_cfg(cfg, output_dir=tmp_path / "out")

    devices = runtime_cfg["devices"]
    assert devices["humidity_generator"]["enabled"] is True
    for key in (
        "temperature_chamber",
        "pressure_controller",
        "pressure_gauge",
        "dewpoint_meter",
        "relay",
        "relay_8",
        "thermometer",
        "gas_analyzer",
    ):
        assert devices[key]["enabled"] is False
    assert devices["gas_analyzers"][0]["enabled"] is False
    assert runtime_cfg["metadata"]["opens_h2o_route"] is False
    assert runtime_cfg["metadata"]["writes_senco"] is False


def test_h2o_point_run_id_is_short_for_windows_artifact_paths():
    run_id = _point_run_id(index=12, temp_c=40.0, hgen_temp_c=30.0, hgen_rh_pct=70.0)

    assert run_id == "p012_T40_HG30C_70RH_h2o"
    assert len(run_id) < 64


def test_h2o_queue_output_dir_does_not_duplicate_run_id(tmp_path):
    run_id = "h2o_pw_r1"
    run_specific_dir = tmp_path / run_id

    assert _queue_output_dir(tmp_path, run_id) == tmp_path / run_id
    assert _queue_output_dir(run_specific_dir, run_id) == run_specific_dir


def test_h2o_queue_output_dir_compacts_long_reverification_run_id(tmp_path):
    output_dir = tmp_path / ("h2o_postwrite_reverify_no_temp_" + "x" * 90)
    run_id = "h2o_postwrite_reverify_T20_30RH_50RH_no_temp_control_20260610"

    name = _queue_dir_name(output_dir, run_id)
    queue_dir = _queue_output_dir(output_dir, run_id)

    assert name.startswith("h2oq_")
    assert queue_dir == output_dir / name
    assert len(str(queue_dir)) < len(str(output_dir / run_id))


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

    argv = [
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
    rc = main(argv)

    assert rc == 0
    manifest = tmp_path / "out" / "queue_dry" / "queue_manifest.csv"
    summary = tmp_path / "out" / "queue_dry" / "queue_summary.json"
    assert manifest.exists()
    assert summary.exists()
    assert "dry_run" in manifest.read_text(encoding="utf-8-sig")
    with manifest.open(encoding="utf-8-sig", newline="") as handle:
        manifest_rows = list(csv.DictReader(handle))
    assert manifest_rows[0]["resolved_purge_s"] == "360.0"
    assert manifest_rows[0]["purge_profile"] == "explicit_override"
    assert manifest_rows[0]["purge_reasons"] == "explicit_purge_s_preserved"
    payload = json.loads(summary.read_text(encoding="utf-8"))
    assert payload["selected_points"] == 2
    assert payload["dry_run_points"] == 2
    assert payload["failure_audit"]["status"] == "ok"
    assert payload["failure_audit"]["total_points"] == 2
    assert payload["sealed_pressure_control"] is False
    assert payload["writes_senco"] is False
    assert payload["hgen_point_shutdown_policy"] == "queue_managed_keep_running_between_points"
    assert payload["hgen_final_safe_stop_required"] is True
    assert (tmp_path / "out" / "queue_dry" / "queue_failure_audit" / "h2o_queue_failure_audit.json").exists()
    assert (tmp_path / "out" / "queue_dry" / "queue_failure_audit" / "queue_failure_audit.json").exists()
    summary_bytes = summary.read_bytes()
    assert main(argv) == 2
    assert summary.read_bytes() == summary_bytes


def test_h2o_queue_continues_after_point_failure_but_returns_nonzero(tmp_path, monkeypatch):
    queue_path = tmp_path / "h2o_runner_queue.csv"
    _write_queue(queue_path)
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "paths": {"output_dir": str(tmp_path / "logs")},
                "devices": {
                    "temperature_chamber": {"enabled": False},
                    "humidity_generator": {"enabled": True},
                },
            }
        ),
        encoding="utf-8",
    )
    returncodes = iter([1, 0])
    point_commands = []

    def fake_run(command, **_kwargs):
        point_commands.append(command)
        return types.SimpleNamespace(returncode=next(returncodes))

    monkeypatch.setattr(
        h2o_queue_module,
        "_prewarm_humidity_generator_for_group",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        h2o_queue_module,
        "_safe_stop_humidity_generator_after_queue",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(h2o_queue_module.subprocess, "run", fake_run)
    monkeypatch.setattr(h2o_queue_module.time, "sleep", lambda _seconds: None)

    rc = main(
        [
            "--config",
            str(config_path),
            "--queue-csv",
            str(queue_path),
            "--output-dir",
            str(tmp_path / "out"),
            "--run-id",
            "queue_mixed_result",
            "--no-control-temperature",
            "--no-prompt",
            "--engineering-probe-only",
            "--operator-confirmation",
            h2o_queue_module.V1_5_ENGINEERING_PROBE_CONFIRMATION_TEXT,
        ]
    )

    assert rc == 1
    assert len(point_commands) == 2
    summary = json.loads(
        (tmp_path / "out" / "queue_mixed_result" / "queue_summary.json").read_text(encoding="utf-8")
    )
    assert summary["ok_points"] == 1
    assert summary["failed_points"] == 1
    assert summary["hard_failure"] is False
    assert summary["hgen_final_safe_stop_ok"] is True


def test_h2o_queue_prewarms_humidity_generator_before_temperature_settle(tmp_path, monkeypatch):
    queue_path = tmp_path / "h2o_runner_queue.csv"
    _write_queue(queue_path)
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "paths": {"output_dir": str(tmp_path / "logs")},
                "devices": {
                    "temperature_chamber": {"enabled": True, "port": "COM19", "baud": 9600, "addr": 1},
                    "humidity_generator": {"enabled": True, "port": "COM16", "baud": 9600},
                },
            }
        ),
        encoding="utf-8",
    )
    events = []

    def fake_prewarm(*_args, **_kwargs):
        events.append("prewarm")
        return True

    def fake_settle(*_args, **_kwargs):
        events.append("settle")
        return True

    def fake_run(*_args, **_kwargs):
        events.append("point")
        return types.SimpleNamespace(returncode=0)

    final_stop_kwargs = {}

    def fake_final_stop(*_args, **_kwargs):
        events.append("final_stop")
        final_stop_kwargs.update(_kwargs)
        return True

    monkeypatch.setattr(h2o_queue_module, "_prewarm_humidity_generator_for_group", fake_prewarm)
    monkeypatch.setattr(h2o_queue_module, "_settle_temperature_group", fake_settle)
    monkeypatch.setattr(h2o_queue_module, "_safe_stop_humidity_generator_after_queue", fake_final_stop)
    monkeypatch.setattr(h2o_queue_module.subprocess, "run", fake_run)

    rc = main(
        [
            "--config",
            str(config_path),
            "--queue-csv",
            str(queue_path),
            "--output-dir",
            str(tmp_path / "out"),
            "--run-id",
            "queue_prewarm_order",
            "--temps",
            "10",
            "--no-prompt",
            "--engineering-probe-only",
            "--operator-confirmation",
            h2o_queue_module.V1_5_ENGINEERING_PROBE_CONFIRMATION_TEXT,
        ]
    )

    assert rc == 0
    assert events[:3] == ["prewarm", "settle", "point"]
    assert events[-1] == "final_stop"
    assert final_stop_kwargs["run_id"] == "hgen_final_safe_stop"
    summary = json.loads(
        (tmp_path / "out" / "queue_prewarm_order" / "queue_summary.json").read_text(encoding="utf-8")
    )
    assert summary["hgen_final_safe_stop_ok"] is True
    assert summary["hgen_final_safe_stop_run_id"] == "hgen_final_safe_stop"


def test_h2o_queue_humidity_prewarm_creates_run_evidence_dir(tmp_path, monkeypatch):
    cfg = {
        "paths": {"output_dir": str(tmp_path / "logs")},
        "devices": {"humidity_generator": {"enabled": True, "port": "COM16", "baud": 9600}},
    }
    lead_row = {
        "hgen_temp_c": "20",
        "hgen_rh_pct": "30",
        "reference_dewpoint_c": "1.97",
        "reference_h2o_mmol": "6.9571",
    }
    prepared = []

    class FakeRunner:
        def __init__(self, *_args, **_kwargs):
            pass

        def _prepare_humidity_generator(self, point):
            prepared.append((point.hgen_temp_c, point.hgen_rh_pct))

    monkeypatch.setattr(h2o_queue_module, "_build_devices", lambda *_args, **_kwargs: {"humidity_gen": object()})
    monkeypatch.setattr(h2o_queue_module, "CalibrationRunner", FakeRunner)
    monkeypatch.setattr(h2o_queue_module, "_read_humidity_generator_snapshot", lambda *_args, **_kwargs: {"ok": True})
    monkeypatch.setattr(h2o_queue_module, "_close_devices", lambda *_args, **_kwargs: None)

    ok = h2o_queue_module._prewarm_humidity_generator_for_group(
        cfg,
        temp_c=20.0,
        lead_row=lead_row,
        output_dir=tmp_path / "out",
        run_id="T20_temp_settle_hgen_prewarm",
    )

    run_dir = tmp_path / "out" / "T20_temp_settle_hgen_prewarm"
    assert ok is True
    assert prepared == [(20.0, 30.0)]
    assert (run_dir / "humidity_prewarm_runtime_config_snapshot.json").exists()
    summary = json.loads((run_dir / "humidity_prewarm_summary.json").read_text(encoding="utf-8"))
    assert summary["ok"] is True
    assert summary["writes_senco"] is False
    assert summary["route_opened"] is False


def test_h2o_queue_final_hgen_safe_stop_creates_evidence_dir(tmp_path, monkeypatch):
    cfg = {
        "paths": {"output_dir": str(tmp_path / "logs")},
        "devices": {"humidity_generator": {"enabled": True, "port": "COM16", "baud": 9600}},
    }

    monkeypatch.setattr(h2o_queue_module, "_build_devices", lambda *_args, **_kwargs: {"humidity_gen": object()})
    monkeypatch.setattr(
        h2o_queue_module,
        "_read_humidity_generator_snapshot",
        lambda _dev: {"raw": "ok", "data": {"Fl": 0.0}},
    )
    monkeypatch.setattr(
        h2o_queue_module,
        "_safe_stop_humidity_generator",
        lambda _devices: {"ok": True, "status": "pass"},
    )
    monkeypatch.setattr(h2o_queue_module, "_close_devices", lambda _devices: None)

    ok = h2o_queue_module._safe_stop_humidity_generator_after_queue(
        cfg,
        output_dir=tmp_path / "out",
        run_id="hgen_final_stop",
    )

    assert ok is True
    run_dir = tmp_path / "out" / "hgen_final_stop"
    assert (run_dir / "hgen_final_safe_stop_runtime_config.json").exists()
    summary = json.loads((run_dir / "humidity_generator_queue_final_safe_stop.json").read_text(encoding="utf-8"))
    assert summary["ok"] is True
    assert summary["safe_stop_status"]["ok"] is True
    assert summary["route_opened"] is False
    assert summary["writes_senco"] is False
    assert summary["writes_device_id"] is False


def test_h2o_queue_final_hgen_safe_stop_failure_is_not_marked_success(
    tmp_path,
    monkeypatch,
):
    cfg = {
        "paths": {"output_dir": str(tmp_path / "logs")},
        "devices": {"humidity_generator": {"enabled": True, "port": "COM16"}},
    }
    monkeypatch.setattr(
        h2o_queue_module,
        "_build_devices",
        lambda *_args, **_kwargs: {"humidity_gen": object()},
    )
    monkeypatch.setattr(
        h2o_queue_module,
        "_read_humidity_generator_snapshot",
        lambda _dev: {"raw": "ok"},
    )
    monkeypatch.setattr(
        h2o_queue_module,
        "_safe_stop_humidity_generator",
        lambda _devices: {
            "ok": False,
            "status": "fail",
            "error": "humidity_generator_safe_stop_failed:stuck",
        },
    )
    monkeypatch.setattr(h2o_queue_module, "_close_devices", lambda _devices: None)

    ok = h2o_queue_module._safe_stop_humidity_generator_after_queue(
        cfg,
        output_dir=tmp_path / "out",
        run_id="hgen_final_stop_failed",
    )

    assert ok is False
    summary = json.loads(
        (
            tmp_path
            / "out"
            / "hgen_final_stop_failed"
            / "humidity_generator_queue_final_safe_stop.json"
        ).read_text(encoding="utf-8")
    )
    assert summary["ok"] is False
    assert summary["safe_stop_status"]["ok"] is False
    assert "FINAL_SAFE_STOP_NOT_CONFIRMED" in summary["error"]


def test_h2o_queue_final_hgen_safe_stop_uses_short_snapshot_name_for_long_paths(tmp_path, monkeypatch):
    cfg = {
        "paths": {"output_dir": str(tmp_path / "logs")},
        "devices": {"humidity_generator": {"enabled": True, "port": "COM16", "baud": 9600}},
    }

    monkeypatch.setattr(h2o_queue_module, "_build_devices", lambda *_args, **_kwargs: {"humidity_gen": object()})
    monkeypatch.setattr(
        h2o_queue_module,
        "_read_humidity_generator_snapshot",
        lambda _dev: {"raw": "ok", "data": {"Fl": 0.0}},
    )
    monkeypatch.setattr(
        h2o_queue_module,
        "_safe_stop_humidity_generator",
        lambda _devices: {"ok": True, "status": "pass"},
    )
    monkeypatch.setattr(h2o_queue_module, "_close_devices", lambda _devices: None)

    long_dir = tmp_path / ("post_write_reverify_" + "001_077_084_091_" * 2)
    ok = h2o_queue_module._safe_stop_humidity_generator_after_queue(
        cfg,
        output_dir=long_dir,
        run_id="h2o_post_write_reverify_20260614_r2_hgen_final_safe_stop",
    )

    assert ok is True
    run_dir = long_dir / "h2o_post_write_reverify_20260614_r2_hgen_final_safe_stop"
    assert (run_dir / "hgen_final_safe_stop_runtime_config.json").exists()
    assert (run_dir / "humidity_generator_queue_final_safe_stop.json").exists()


def test_h2o_queue_exclusion_evidence_blocks_aborted_rows_from_fit(tmp_path):
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    summary = {"queue_run_id": "h2o_abort_r1"}
    manifest_rows = [
        {
            "point_run_id": "p001_T20_HG20C_30RH_h2o",
            "point_id": "h2o_T20_30RH",
            "temp_c": 20.0,
            "hgen_temp_c": 20.0,
            "hgen_rh_pct": 30.0,
            "reference_dewpoint_c": 1.97,
            "reference_h2o_mmol": 6.95,
            "sample_role": "fit",
            "status": "aborted",
            "point_log": str(queue_dir / "point.log"),
        }
    ]

    _write_queue_exclusion_evidence(
        queue_dir,
        queue_summary=summary,
        manifest_rows=manifest_rows,
        reason="operator_interrupted",
    )

    with (queue_dir / "queue_abort_exclusion.csv").open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    payload = json.loads((queue_dir / "queue_abort_exclusion.json").read_text(encoding="utf-8"))

    assert rows[0]["queue_run_id"] == "h2o_abort_r1"
    assert rows[0]["exclude_from_fit"] == "True"
    assert rows[0]["exclude_from_acceptance"] == "True"
    assert rows[0]["exclude_from_senco_review"] == "True"
    assert rows[0]["exclusion_reason"] == "operator_interrupted"
    assert payload["exclude_from_fit"] is True
    assert payload["rows"][0]["source_status"] == "aborted"
