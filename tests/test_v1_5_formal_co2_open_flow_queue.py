import csv
import json

from gas_calibrator.tools import run_v1_5_formal_co2_open_flow_queue as co2_queue_module
from gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue import (
    _build_point_command,
    _classify_point_failure_from_log,
    _resolve_formal_min_valid_analyzers,
    _load_queue_rows,
    _ordered_temperature_groups,
    _parse_args,
    _point_run_id,
    _prepare_temperature_runtime_cfg,
    _resolve_n2_prepurge_s,
    _select_queue_rows,
    _temperature_group_n2_prepurge_index,
    _temperature_settle_run_id,
    main,
)
from gas_calibrator.validation.v1_5_open_flow_purge_contract import nitrogen_prepurge_formal_role


def _write_queue(path):
    rows = [
        {
            "point_id": "co2_T20_900ppm_ambient",
            "component": "co2",
            "temp_c": "20",
            "source_nominal_ppm": "900",
            "co2_group": "B",
            "sample_role": "fit",
            "purge_s": "360",
            "sample_count": "10",
            "analyzer_acquisition": "active_stream_1hz",
        },
        {
            "point_id": "co2_T40_200ppm_ambient",
            "component": "co2",
            "temp_c": "40",
            "source_nominal_ppm": "200",
            "co2_group": "A",
            "sample_role": "verification",
            "purge_s": "360",
            "sample_count": "10",
            "analyzer_acquisition": "active_stream_1hz",
        },
        {
            "point_id": "h2o_T20",
            "component": "h2o",
            "temp_c": "20",
            "source_nominal_ppm": "",
            "co2_group": "",
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


def _write_queue_with_zero_anchor(path):
    rows = [
        {
            "point_id": "co2_T40_0ppm_ambient",
            "component": "co2",
            "temp_c": "40",
            "source_nominal_ppm": "0",
            "co2_group": "A",
            "sample_role": "fit",
            "purge_s": "360",
            "sample_count": "10",
            "analyzer_acquisition": "active_stream_1hz",
        },
        {
            "point_id": "co2_T40_400ppm_ambient",
            "component": "co2",
            "temp_c": "40",
            "source_nominal_ppm": "400",
            "co2_group": "A",
            "sample_role": "fit",
            "purge_s": "360",
            "sample_count": "10",
            "analyzer_acquisition": "active_stream_1hz",
        },
        {
            "point_id": "co2_T40_900ppm_ambient",
            "component": "co2",
            "temp_c": "40",
            "source_nominal_ppm": "900",
            "co2_group": "B",
            "sample_role": "verification",
            "purge_s": "360",
            "sample_count": "10",
            "analyzer_acquisition": "active_stream_1hz",
        },
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_queue_loader_filters_to_co2_and_orders_temperature_desc(tmp_path):
    queue_path = tmp_path / "co2_runner_queue.csv"
    _write_queue(queue_path)

    rows = _load_queue_rows(queue_path)
    selected = _select_queue_rows(rows, temps=None, roles={"fit", "verification"}, max_points=None)
    groups = _ordered_temperature_groups(selected, order="desc")

    assert [temp for temp, _ in groups] == [40.0, 20.0]
    assert [item["source_nominal_ppm"] for item in groups[0][1]] == [200.0]
    assert len(selected) == 2


def test_temperature_group_n2_prepurge_prefers_zero_anchor():
    rows = [
        {"source_nominal_ppm": 400.0},
        {"source_nominal_ppm": 0.0},
        {"source_nominal_ppm": 900.0},
    ]

    assert _temperature_group_n2_prepurge_index(rows) == 1
    assert _temperature_group_n2_prepurge_index([{"source_nominal_ppm": 400.0}]) == 0
    assert _temperature_group_n2_prepurge_index([]) is None


def test_classify_point_failure_from_log_identifies_dewpoint_rebound(tmp_path):
    log_path = tmp_path / "point.log"
    log_path.write_text(
        "CO2 route precondition failed: "
        "reason=dewpoint_tail_reference_not_dry_enough;dewpoint_rebound_detected;max_total_wait_exceeded\n",
        encoding="utf-8",
    )

    result = _classify_point_failure_from_log(log_path)

    assert result["failure_category"] == "dewpoint_rebound"
    assert "dewpoint_rebound_detected" in result["failure_reason"]


def test_classify_point_failure_from_log_identifies_mode2_startup(tmp_path):
    log_path = tmp_path / "point.log"
    log_path.write_text(
        "Analyzer startup config failed: ga05 err=MODE2 not ready\n",
        encoding="utf-8",
    )

    result = _classify_point_failure_from_log(log_path)

    assert result["failure_category"] == "analyzer_startup_mode2"
    assert "MODE2 not ready" in result["failure_reason"]


def test_queue_point_command_keeps_no_write_open_flow_sidecar_contract(tmp_path):
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
            "--n2-prepurge-s",
            "180",
            "--n2-purge-source-valve",
            "27",
            "--reference-source-catalog",
            "catalog.json",
            "--analyzer-gate-required-labels",
            "ga02,ga03",
            "--analyzer-gate-prefer-all-stable-grace-s",
            "45",
            "--co2-ratio-f-preseal-tol",
            "0.0002",
            "--co2-ratio-f-preseal-window-s",
            "120",
            "--co2-ratio-f-preseal-timeout-s",
            "900",
            "--co2-ratio-f-preseal-min-samples",
            "20",
            "--co2-ratio-f-preseal-policy",
            "warn",
        ]
    )
    cmd = _build_point_command(
        config_path="config.json",
        output_dir=tmp_path,
        row={
            "temp_c": 20.0,
            "source_nominal_ppm": 900.0,
            "certificate_co2_ppm": "897.04",
            "certificate_uncertainty_ppm": "8.9704",
            "reference_asset_id": "co2-standard-897-test",
            "co2_group": "B",
            "sample_role": "fit",
            "purge_s": 360.0,
            "sample_count": 10,
            "analyzer_acquisition": "active_stream_1hz",
        },
        run_id="point_run",
        args=args,
    )

    text = " ".join(cmd)
    assert "gas_calibrator.tools.run_v1_5_formal_open_flow_sampling" in text
    assert "--pressure-target-hpa" not in cmd
    assert "--no-ftd-write" in cmd
    assert "--no-prompt" in cmd
    assert "--n2-prepurge-s 180" in text
    assert "--n2-purge-source-valve 27" in text
    assert "--co2-source-ppm 900" in text
    assert "--certificate-co2-ppm 897.04" in text
    assert "--certificate-uncertainty-ppm 8.9704" in text
    assert "--reference-source-catalog catalog.json" in text
    assert "--reference-asset-id co2-standard-897-test" in text
    assert "--analyzer-gate-required-labels ga02,ga03" in text
    assert "--analyzer-gate-prefer-all-stable-grace-s 45" in text
    assert "--co2-ratio-f-preseal-tol 0.0002" in text
    assert "--co2-ratio-f-preseal-window-s 120" in text
    assert "--co2-ratio-f-preseal-timeout-s 900" in text
    assert "--co2-ratio-f-preseal-min-samples 20" in text
    assert "--co2-ratio-f-preseal-policy warn" in text
    assert "--min-valid-analyzers 1" in text
    assert "--gas-route-dewpoint-gate-enabled" in cmd
    assert "--gas-route-dewpoint-gate-policy reject" in text
    assert "--gas-route-dewpoint-require-dry-enough" in cmd
    assert "--gas-route-dewpoint-dry-enough-c -28" in text
    assert "--gas-route-dewpoint-gate-max-total-wait-s 1800" in text


def test_queue_point_command_preserves_numeric_zero_reference_value(tmp_path):
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
            "source_nominal_ppm": 0.0,
            "certificate_co2_ppm": 0.0,
            "co2_group": "A",
            "sample_role": "fit",
            "purge_s": 360.0,
            "sample_count": 10,
        },
        run_id="zero_point",
        args=args,
    )

    text = " ".join(cmd)
    assert "--certificate-co2-ppm 0" in text


def test_queue_point_command_uses_frozen_row_reference_catalog_when_cli_omits_it(
    tmp_path,
):
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
            "source_nominal_ppm": 600.0,
            "certificate_co2_ppm": 598.22,
            "reference_asset_id": "co2-standard-600-test",
            "reference_source_catalog": "configs/frozen_reference_catalog.json",
            "co2_group": "B",
            "sample_role": "fit",
            "purge_s": 360.0,
            "sample_count": 10,
        },
        run_id="frozen_catalog_point",
        args=args,
    )

    text = " ".join(cmd)
    assert (
        "--reference-source-catalog configs/frozen_reference_catalog.json"
        in text
    )
    assert "--reference-asset-id co2-standard-600-test" in text


def test_queue_point_command_preserves_explicit_relaxed_dewpoint_warn_policy(tmp_path):
    args = _parse_args(
        [
            "--config",
            "config.json",
            "--queue-csv",
            "queue.csv",
            "--output-dir",
            str(tmp_path),
            "--no-prompt",
            "--gas-route-dewpoint-gate-policy",
            "warn",
        ]
    )
    cmd = _build_point_command(
        config_path="config.json",
        output_dir=tmp_path,
        row={
            "temp_c": 20.0,
            "source_nominal_ppm": 900.0,
            "co2_group": "B",
            "sample_role": "fit",
            "purge_s": 360.0,
            "sample_count": 10,
            "analyzer_acquisition": "active_stream_1hz",
        },
        run_id="point_run",
        args=args,
    )

    assert "--gas-route-dewpoint-gate-policy warn" in " ".join(cmd)


def test_queue_does_not_inherit_n2_prepurge_from_runtime_config_without_explicit_request():
    cfg = {"workflow": {"nitrogen_purge": {"co2_prepurge_s": 240}}}

    assert _resolve_n2_prepurge_s(cfg, None) == 0.0
    assert _resolve_n2_prepurge_s(cfg, 0.0) == 0.0
    assert _resolve_n2_prepurge_s(cfg, 180.0) == 180.0


def test_formal_min_valid_defaults_to_per_analyzer_evidence_mode():
    cfg = {
        "devices": {
            "gas_analyzers": [
                {"name": "ga01"},
                {"name": "ga02"},
                {"name": "ga03", "enabled": False},
                {"name": "ga04"},
            ]
        }
    }

    assert _resolve_formal_min_valid_analyzers(cfg, None) == 1
    assert _resolve_formal_min_valid_analyzers(cfg, 9) == 3
    assert _resolve_formal_min_valid_analyzers(cfg, 2) == 2


def test_co2_temperature_settle_run_id_stays_short_for_windows_paths():
    assert _temperature_settle_run_id(40.0) == "T40_temp_settle"
    assert _temperature_settle_run_id(-20.0) == "Tm20_temp_settle"


def test_co2_temperature_settle_creates_missing_output_dir_before_logging(tmp_path, monkeypatch):
    def _raise_before_real_devices(*_args, **_kwargs):
        raise RuntimeError("stop before real devices")

    monkeypatch.setattr(co2_queue_module, "_build_devices", _raise_before_real_devices)
    output_dir = tmp_path / "missing" / "co2_open_flow"
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

    ok = co2_queue_module._settle_temperature_group(
        {
            "devices": {
                "temperature_chamber": {"enabled": True},
                "thermometer": {"enabled": True},
            }
        },
        temp_c=40.0,
        output_dir=output_dir,
        run_id="co2_T40_temperature_settle",
        args=args,
    )

    assert ok is False
    assert output_dir.exists()
    assert (output_dir / "co2_T40_temperature_settle" / "temperature_settle_summary.json").exists()


def test_co2_queue_uses_600s_for_unknown_route_when_plan_does_not_override(tmp_path):
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
            "source_nominal_ppm": 900.0,
            "co2_group": "B",
            "sample_role": "fit",
            "initial_state": "unknown",
            "sample_count": 10,
            "analyzer_acquisition": "active_stream_1hz",
        },
        run_id="point_run",
        args=args,
    )

    assert cmd[cmd.index("--purge-s") + 1] == "600"
    assert cmd[cmd.index("--minimum-purge-s") + 1] == "600"


def test_co2_queue_preserves_explicit_purge_over_conservative_profile(tmp_path):
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
            "source_nominal_ppm": 900.0,
            "co2_group": "B",
            "sample_role": "fit",
            "purge_s": 420.0,
            "after_wet_route": "true",
            "sample_count": 10,
            "analyzer_acquisition": "active_stream_1hz",
        },
        run_id="point_run",
        args=args,
    )

    assert cmd[cmd.index("--purge-s") + 1] == "420"
    assert cmd[cmd.index("--minimum-purge-s") + 1] == "360"


def test_co2_queue_adaptive_purge_shortens_known_route_after_first_point(tmp_path):
    args = _parse_args(
        [
            "--config",
            "config.json",
            "--queue-csv",
            "queue.csv",
            "--output-dir",
            str(tmp_path),
            "--no-prompt",
            "--co2-adaptive-purge-after-first-point",
            "--co2-subsequent-purge-s",
            "240",
        ]
    )
    row = {
        "temp_c": 10.0,
        "source_nominal_ppm": 900.0,
        "co2_group": "B",
        "sample_role": "fit",
        "purge_s": 360.0,
        "sample_count": 10,
        "analyzer_acquisition": "active_stream_1hz",
    }

    first_cmd = _build_point_command(
        config_path="config.json",
        output_dir=tmp_path,
        row=row,
        run_id="first_point",
        args=args,
        row_index_in_temperature_group=0,
    )
    next_cmd = _build_point_command(
        config_path="config.json",
        output_dir=tmp_path,
        row=row,
        run_id="next_point",
        args=args,
        row_index_in_temperature_group=1,
    )

    assert first_cmd[first_cmd.index("--purge-s") + 1] == "360"
    assert first_cmd[first_cmd.index("--minimum-purge-s") + 1] == "360"
    assert next_cmd[next_cmd.index("--purge-s") + 1] == "240"
    assert next_cmd[next_cmd.index("--minimum-purge-s") + 1] == "240"


def test_co2_queue_adaptive_purge_keeps_conservative_route_time(tmp_path):
    args = _parse_args(
        [
            "--config",
            "config.json",
            "--queue-csv",
            "queue.csv",
            "--output-dir",
            str(tmp_path),
            "--no-prompt",
            "--co2-adaptive-purge-after-first-point",
            "--co2-subsequent-purge-s",
            "240",
        ]
    )
    cmd = _build_point_command(
        config_path="config.json",
        output_dir=tmp_path,
        row={
            "temp_c": 10.0,
            "source_nominal_ppm": 900.0,
            "co2_group": "B",
            "sample_role": "fit",
            "initial_state": "unknown",
            "sample_count": 10,
            "analyzer_acquisition": "active_stream_1hz",
        },
        run_id="recovery_point",
        args=args,
        row_index_in_temperature_group=1,
    )

    assert cmd[cmd.index("--purge-s") + 1] == "600"
    assert cmd[cmd.index("--minimum-purge-s") + 1] == "600"


def test_nitrogen_prepurge_is_conditioning_not_formal_anchor():
    role = nitrogen_prepurge_formal_role()

    assert role["may_reduce_residual_co2"] is True
    assert role["may_help_dry_route"] is True
    assert role["is_formal_co2_zero_anchor"] is False
    assert role["is_formal_h2o_dry_anchor"] is False
    assert role["requires_own_reference_evidence_for_anchor_use"] is True


def test_temperature_runtime_inherits_formal_soak_without_shortening(tmp_path):
    cfg = {
        "paths": {"output_dir": str(tmp_path / "logs")},
        "devices": {
            "temperature_chamber": {"enabled": True, "port": "COM19"},
            "thermometer": {"enabled": True, "port": "COM18"},
            "pressure_controller": {"enabled": True},
            "pressure_gauge": {"enabled": True},
            "dewpoint_meter": {"enabled": True},
            "humidity_generator": {"enabled": True},
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
    assert runtime_cfg["devices"]["thermometer"]["enabled"] is True


def test_point_run_id_is_short_for_windows_artifact_paths():
    run_id = _point_run_id(index=1, temp_c=-20.0, ppm=1000.0, role="verification")

    assert run_id == "p001_Tm20_1000ppm_verification"
    assert len(run_id) < 64


def test_queue_dry_run_writes_manifest_without_real_com(tmp_path):
    queue_path = tmp_path / "co2_runner_queue.csv"
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
        "--n2-prepurge-s",
        "120",
        "--n2-purge-source-valve",
        "27",
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
    assert payload["sealed_pressure_control"] is False
    assert payload["writes_senco"] is False
    assert payload["n2_prepurge_s"] == 120.0
    assert payload["n2_prepurge_policy"] == "explicit_engineering_conditioning_once_per_temperature_group"
    assert payload["n2_purge_source_valve"] == 27
    audit = payload["failure_audit"]
    assert audit["status"] == "ok"
    assert audit["total_points"] == 2
    assert audit["status_counts"] == {"dry_run": 2}
    assert (tmp_path / "out" / "queue_dry" / "queue_failure_audit" / "queue_failure_audit.json").exists()
    assert (tmp_path / "out" / "queue_dry" / "queue_failure_audit" / "queue_failure_audit_zh.md").exists()
    summary_bytes = summary.read_bytes()
    assert main(argv) == 2
    assert summary.read_bytes() == summary_bytes


def test_queue_writes_failure_audit_when_temperature_settle_fails_before_points(tmp_path, monkeypatch):
    queue_path = tmp_path / "co2_runner_queue.csv"
    _write_queue(queue_path)
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "paths": {"output_dir": str(tmp_path / "logs")},
                "devices": {"temperature_chamber": {"enabled": True, "port": "COM19", "baud": 9600, "addr": 1}},
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(co2_queue_module, "_settle_temperature_group", lambda *_args, **_kwargs: False)

    rc = main(
        [
            "--config",
            str(config_path),
            "--queue-csv",
            str(queue_path),
            "--output-dir",
            str(tmp_path / "out"),
            "--run-id",
            "queue_temp_fail",
            "--no-prompt",
        ]
    )

    assert rc == 1
    run_dir = tmp_path / "out" / "queue_temp_fail"
    manifest = run_dir / "queue_manifest.csv"
    summary = run_dir / "queue_summary.json"
    audit_json = run_dir / "queue_failure_audit" / "queue_failure_audit.json"
    audit_md = run_dir / "queue_failure_audit" / "queue_failure_audit_zh.md"
    assert manifest.exists()
    assert audit_json.exists()
    assert audit_md.exists()

    with manifest.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows == [
        {
            "point_run_id": "T40_temp_settle",
            "temp_c": "40.0",
            "source_nominal_ppm": "",
            "co2_group": "temperature_settle",
            "sample_role": "temperature_settle",
            "started_at": "",
            "ended_at": rows[0]["ended_at"],
            "returncode": "",
            "status": "failed",
            "point_log": "",
            "command": "",
            "failure_category": "temperature_settle_failed",
            "failure_reason": "Temperature group 40C failed before 1 CO2 point(s).",
            "temperature_settle_run_id": "T40_temp_settle",
            "temperature_settle_output_dir": rows[0]["temperature_settle_output_dir"],
        }
    ]
    assert rows[0]["temperature_settle_output_dir"].endswith("T40_temp_settle")

    payload = json.loads(summary.read_text(encoding="utf-8"))
    assert payload["hard_failure"] is True
    assert payload["failure_audit"]["status"] == "ok"
    assert payload["failure_audit"]["status_counts"] == {"failed": 1}
    assert payload["failure_audit"]["failure_category_counts"] == {"temperature_settle_failed": 1}
    audit_payload = json.loads(audit_json.read_text(encoding="utf-8"))
    assert audit_payload["rows"][0]["failure_category"] == "temperature_settle_failed"


def test_queue_applies_n2_prepurge_once_per_temperature_group_zero_anchor(tmp_path):
    queue_path = tmp_path / "co2_runner_queue.csv"
    _write_queue_with_zero_anchor(queue_path)
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
            "queue_dry_zero",
            "--dry-run",
            "--no-prompt",
            "--n2-prepurge-s",
            "300",
            "--n2-purge-source-valve",
            "27",
        ]
    )

    assert rc == 0
    manifest = tmp_path / "out" / "queue_dry_zero" / "queue_manifest.csv"
    with manifest.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert [row["source_nominal_ppm"] for row in rows] == ["0.0", "400.0", "900.0"]
    assert [row["n2_prepurge_s"] for row in rows] == ["300.0", "0.0", "0.0"]
    assert "--n2-prepurge-s 300" in rows[0]["command"]
    assert "--n2-purge-source-valve 27" in rows[0]["command"]
    assert "--n2-prepurge-s" not in rows[1]["command"]
    assert "--n2-prepurge-s" not in rows[2]["command"]
