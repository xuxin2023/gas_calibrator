from __future__ import annotations

from pathlib import Path

from gas_calibrator.tools.verify_short_run import build_short_verification_config


def test_build_short_verification_config_applies_short_run_overrides() -> None:
    cfg = {
        "workflow": {
            "collect_only": False,
            "route_mode": "co2_only",
            "skip_h2o": True,
            "selected_temps_c": [0.0],
            "skip_co2_ppm": [200],
            "startup_connect_check": {"enabled": True},
            "startup_pressure_precheck": {"enabled": True},
        }
    }

    runtime_cfg = build_short_verification_config(
        cfg,
        temp_c=20.0,
        skip_co2_ppm=[100, 200, 300],
        enable_connect_check=False,
    )

    workflow_cfg = runtime_cfg["workflow"]
    assert workflow_cfg["collect_only"] is True
    assert workflow_cfg["route_mode"] == "h2o_then_co2"
    assert workflow_cfg["skip_h2o"] is False
    assert workflow_cfg["selected_temps_c"] == [20.0]
    assert workflow_cfg["skip_co2_ppm"] == [100, 200, 300]
    assert workflow_cfg["startup_connect_check"]["enabled"] is False
    assert workflow_cfg["startup_pressure_precheck"]["enabled"] is False
    stability_cfg = workflow_cfg["stability"]
    assert stability_cfg["temperature"]["analyzer_chamber_temp_timeout_s"] == 300.0
    assert stability_cfg["temperature"]["analyzer_chamber_temp_first_valid_timeout_s"] == 60.0
    assert stability_cfg["co2_route"]["preseal_soak_s"] == 0.0
    assert stability_cfg["co2_route"]["post_h2o_zero_ppm_soak_s"] == 0.0
    assert stability_cfg["gas_route_dewpoint_gate_enabled"] is False
    assert stability_cfg["sensor"]["enabled"] is False
    assert stability_cfg["sensor"]["baseline_sanity_gate"]["enabled"] is False
    assert stability_cfg["sensor"]["baseline_sanity_gate"]["policy"] == "off"
    assert stability_cfg["co2_cold_quality_gate"]["enabled"] is False
    assert stability_cfg["co2_cold_quality_gate"]["policy"] == "off"


def test_build_short_verification_config_preserves_explicit_points_matrix_when_override_is_used() -> None:
    cfg = {
        "workflow": {
            "selected_temps_c": [10.0],
            "skip_co2_ppm": [200],
        },
        "paths": {
            "points_excel": "configs/default_points.xlsx",
        },
    }

    runtime_cfg = build_short_verification_config(
        cfg,
        temp_c=20.0,
        skip_co2_ppm=[],
        enable_connect_check=True,
        points_excel_override="configs/points_tiny_short_run_20c_even500.xlsx",
    )

    workflow_cfg = runtime_cfg["workflow"]
    assert workflow_cfg["selected_temps_c"] == []
    assert workflow_cfg["skip_co2_ppm"] == []
    assert workflow_cfg["preserve_explicit_point_matrix"] is True
    assert runtime_cfg["paths"]["points_excel"] == str(
        Path("configs/points_tiny_short_run_20c_even500.xlsx").resolve()
    )


def test_build_short_verification_config_applies_actual_device_ids() -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"device_id": "000"},
            "gas_analyzers": [
                {"name": "ga01", "enabled": True, "device_id": "001"},
                {"name": "ga02", "enabled": True, "device_id": "002"},
            ],
        }
    }

    runtime_cfg = build_short_verification_config(
        cfg,
        temp_c=20.0,
        skip_co2_ppm=[],
        enable_connect_check=False,
        actual_device_ids={"GA01": "086", "GA02": "008"},
    )

    assert runtime_cfg["devices"]["gas_analyzer"]["device_id"] == "086"
    assert runtime_cfg["devices"]["gas_analyzers"][0]["device_id"] == "086"
    assert runtime_cfg["devices"]["gas_analyzers"][1]["device_id"] == "008"


def test_default_config_keeps_mode2_post_enable_wait() -> None:
    import json
    from pathlib import Path

    cfg = json.loads(Path("D:/gas_calibrator/configs/default_config.json").read_text(encoding="utf-8"))
    assert cfg["workflow"]["analyzer_mode2_init"]["post_enable_stream_wait_s"] == 2.0
    assert cfg["workflow"]["analyzer_mode2_init"]["command_gap_s"] == 0.15
    assert cfg["workflow"]["analyzer_mode2_init"]["post_enable_stream_ack_wait_s"] == 8.0
