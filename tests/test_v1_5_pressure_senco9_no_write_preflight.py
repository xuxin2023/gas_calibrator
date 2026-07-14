import json
from pathlib import Path

from gas_calibrator.config import load_config
from gas_calibrator.tools.export_v1_5_pressure_senco9_no_write_preflight import main as preflight_main
from gas_calibrator.validation.pressure_senco9_no_write_plan import (
    assess_pressure_senco9_no_write_config,
    build_pressure_senco9_no_write_plan_tables,
    normalize_pressure_points,
    write_pressure_senco9_no_write_preflight_report,
)


ROOT = Path(__file__).resolve().parents[1]


def _write_no_write_config(path, analyzers):
    payload = {
        "devices": {
            "pressure_gauge": {"enabled": True},
            "pressure_controller": {"enabled": True},
            "relay": {"enabled": False},
            "relay_8": {"enabled": False},
            "humidity_generator": {"enabled": False},
            "dewpoint_meter": {"enabled": False},
            "gas_analyzers": analyzers,
        },
        "workflow": {
            "controlled_write": False,
            "postrun_corrected_delivery": {
                "write_devices": False,
                "write_pressure_coefficients": False,
            },
            "startup_pressure_sensor_calibration": {
                "enabled": False,
                "apply_write": False,
            },
        },
        "coefficients": {"enabled": False},
        "metadata": {
            "writes_senco": False,
            "writes_device_id": False,
            "controls_water_or_gas_routes": False,
        },
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _pressure_reference(**overrides):
    data = {
        "device_id": "COM22-DPG-001",
        "certificate_id": "P-CERT-001",
        "certificate_uncertainty": 0.15,
        "valid_until": "2027-01-01",
        "certificate_hash": "pressure-cert-hash",
        "unit": "hPa",
    }
    data.update(overrides)
    return data


def _read_csv(path):
    import csv

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def test_pressure_point_normalization_defaults_to_v1_5_multi_point_matrix():
    assert normalize_pressure_points(None) == [
        "ambient",
        1100.0,
        1000.0,
        900.0,
        800.0,
        700.0,
        600.0,
        500.0,
    ]
    assert normalize_pressure_points("ambient,1100,800,500") == ["ambient", 1100.0, 800.0, 500.0]


def test_pressure_senco9_no_write_config_blocks_known_write_paths():
    status, reasons, warnings = assess_pressure_senco9_no_write_config(
        {
            "devices": {
                "pressure_gauge": {"enabled": True},
                "pressure_controller": {"enabled": True},
                "gas_analyzers": [
                    {"name": "ga01", "device_id": "023", "port": "COM35", "active_send": True}
                ],
            },
            "workflow": {
                "controlled_write": True,
                "postrun_corrected_delivery": {
                    "write_devices": True,
                    "write_pressure_coefficients": True,
                },
                "startup_pressure_sensor_calibration": {
                    "enabled": True,
                    "apply_write": True,
                },
            },
            "coefficients": {"enabled": True, "sencos": {"9": {"A": 1}}},
            "metadata": {"writes_senco": True, "writes_device_id": True},
        }
    )

    assert status == "fail"
    assert "workflow.controlled_write_enabled" in reasons
    assert "workflow.postrun_corrected_delivery.write_devices_enabled" in reasons
    assert "workflow.postrun_corrected_delivery.write_pressure_coefficients_enabled" in reasons
    assert "workflow.startup_pressure_sensor_calibration.enabled_enabled" in reasons
    assert "workflow.startup_pressure_sensor_calibration.apply_write_enabled" in reasons
    assert "coefficients.enabled_enabled" in reasons
    assert "coefficients.sencos_present" in reasons
    assert "metadata.writes_senco_enabled" in reasons
    assert "metadata.writes_device_id_enabled" in reasons
    assert "active_send_enabled_for_ga01" in warnings


def test_pressure_senco9_no_write_plan_passes_for_formal_4ch_config(tmp_path):
    cfg_path = _write_no_write_config(
        tmp_path / "site_v1_5_formal_open_flow_4ch_no_write_900ppm.json",
        [
            {"name": "ga01", "device_id": "023", "port": "COM35", "enabled": True},
            {"name": "ga02", "device_id": "033", "port": "COM36", "enabled": True},
            {"name": "ga03", "device_id": "001", "port": "COM37", "enabled": True},
            {"name": "ga04", "device_id": "091", "port": "COM38", "enabled": True},
        ],
    )
    cfg = load_config(cfg_path)

    tables, context = build_pressure_senco9_no_write_plan_tables(
        config=cfg,
        config_path=cfg_path,
        pressure_reference=_pressure_reference(),
        pressure_reference_path=ROOT / "pressure_reference.json",
        today="2026-05-24",
    )

    summary = tables["pressure_senco9_no_write_summary"][0]
    assert context["preflight_status"] == "pass"
    assert summary["controls_water_or_gas_routes"] is False
    assert summary["writes_senco9"] is False
    assert summary["writes_device_id"] is False
    assert summary["not_real_acceptance_evidence"] is True
    assert len(tables["pressure_point_plan"]) == 8
    assert [row["configured_device_id"] for row in tables["analyzer_identity_plan"]] == [
        "023",
        "033",
        "001",
        "091",
    ]
    assert "--require-continuous-atmosphere-hold" in context["collection_command"]
    assert "--control-pressure-points" in context["collection_command"]
    assert "--pressure-control-setpoint-mode absolute" in context["collection_command"]
    assert "--pressure-control-slew-mode max" in context["collection_command"]
    assert "--analyzer-active-upload-hz 1" in context["collection_command"]
    assert "export_v1_5_pressure_senco9_evaluation" in context["fit_command"]


def test_pressure_senco9_5ch_active_config_is_no_write_and_keeps_average_filter(tmp_path):
    cfg_path = _write_no_write_config(
        tmp_path / "site_v1_5_pressure_senco9_no_write_5ch_active.json",
        [
            {
                "name": f"ga{index:02d}",
                "device_id": device_id,
                "port": port,
                "enabled": True,
                "active_send": True,
                "ftd_hz": 1,
                "average_filter": 49,
                "average_co2": 1,
                "average_h2o": 1,
            }
            for index, (port, device_id) in enumerate(
                zip(
                    ["COM35", "COM36", "COM37", "COM41", "COM42"],
                    ["023", "030", "033", "001", "027"],
                ),
                start=1,
            )
        ],
    )
    cfg = load_config(cfg_path)

    analyzers = cfg["devices"]["gas_analyzers"]
    assert [item["port"] for item in analyzers] == ["COM35", "COM36", "COM37", "COM41", "COM42"]
    assert [item["device_id"] for item in analyzers] == ["023", "030", "033", "001", "027"]
    assert all(item["active_send"] is True for item in analyzers)
    assert all(item["ftd_hz"] == 1 for item in analyzers)
    assert all(item["average_filter"] == 49 for item in analyzers)
    assert all(item["average_co2"] == 1 and item["average_h2o"] == 1 for item in analyzers)

    assert cfg["devices"]["relay"]["enabled"] is False
    assert cfg["devices"]["relay_8"]["enabled"] is False
    assert cfg["devices"]["humidity_generator"]["enabled"] is False
    assert cfg["devices"]["dewpoint_meter"]["enabled"] is False
    assert cfg["workflow"]["postrun_corrected_delivery"]["write_devices"] is False
    assert cfg["workflow"]["postrun_corrected_delivery"]["write_pressure_coefficients"] is False
    assert cfg["workflow"]["startup_pressure_sensor_calibration"]["enabled"] is False
    assert cfg["workflow"]["startup_pressure_sensor_calibration"]["apply_write"] is False
    assert cfg["coefficients"]["enabled"] is False
    assert cfg["metadata"]["writes_senco"] is False
    assert cfg["metadata"]["writes_device_id"] is False
    assert cfg["metadata"]["controls_water_or_gas_routes"] is False

    tables, context = build_pressure_senco9_no_write_plan_tables(
        config=cfg,
        config_path=cfg_path,
        pressure_reference=_pressure_reference(),
        today="2026-05-24",
    )
    assert context["preflight_status"] == "pass"
    assert tables["pressure_senco9_no_write_summary"][0]["analyzer_count"] == 5


def test_pressure_senco9_mature_seven_point_matrix_does_not_require_ambient(tmp_path):
    cfg_path = _write_no_write_config(
        tmp_path / "site_v1_5_pressure_senco9_no_write_mature_7point.json",
        [{"name": "ga01", "device_id": "001", "port": "COM35", "enabled": True}],
    )
    cfg = load_config(cfg_path)

    tables, context = build_pressure_senco9_no_write_plan_tables(
        config=cfg,
        config_path=cfg_path,
        pressure_reference=_pressure_reference(),
        pressure_points="1100,1000,900,800,700,600,500",
        sample_count=10,
        today="2026-05-24",
    )

    assert context["preflight_status"] == "pass"
    checks = {row["check"]: row for row in tables["pressure_senco9_no_write_checks"]}
    ambient_check = checks["ambient_reference_point"]
    assert ambient_check["status"] == "pass"
    assert ambient_check["ambient_required"] is False
    assert ambient_check["mature_seven_point_matrix"] is True
    assert ambient_check["policy"] == "mature_seven_point_matrix_does_not_require_ambient"


def test_pressure_senco9_non_mature_matrix_without_ambient_remains_blocked(tmp_path):
    cfg_path = _write_no_write_config(
        tmp_path / "site_v1_5_pressure_senco9_no_write_non_mature.json",
        [{"name": "ga01", "device_id": "001", "port": "COM35", "enabled": True}],
    )
    cfg = load_config(cfg_path)

    tables, context = build_pressure_senco9_no_write_plan_tables(
        config=cfg,
        config_path=cfg_path,
        pressure_reference=_pressure_reference(),
        pressure_points="1100,800,500",
        sample_count=10,
        today="2026-05-24",
    )

    assert context["preflight_status"] == "fail"
    checks = {row["check"]: row for row in tables["pressure_senco9_no_write_checks"]}
    ambient_check = checks["ambient_reference_point"]
    assert ambient_check["status"] == "fail"
    assert ambient_check["reasons"] == "ambient_point_missing"
    assert ambient_check["ambient_required"] is True
    assert ambient_check["mature_seven_point_matrix"] is False
    assert ambient_check["policy"] == "ambient_required_for_non_mature_matrix"


def test_pressure_senco9_no_write_plan_blocks_short_matrix_and_missing_reference(tmp_path):
    cfg_path = _write_no_write_config(
        tmp_path / "site_v1_5_formal_open_flow_4ch_no_write_900ppm.json",
        [
            {"name": "ga01", "device_id": "023", "port": "COM35", "enabled": True},
            {"name": "ga02", "device_id": "033", "port": "COM36", "enabled": True},
            {"name": "ga03", "device_id": "001", "port": "COM37", "enabled": True},
            {"name": "ga04", "device_id": "091", "port": "COM38", "enabled": True},
        ],
    )
    cfg = load_config(cfg_path)

    tables, context = build_pressure_senco9_no_write_plan_tables(
        config=cfg,
        config_path=cfg_path,
        pressure_reference={},
        pressure_points="ambient,1000",
        sample_count=5,
        today="2026-05-24",
    )

    assert context["preflight_status"] == "fail"
    failed = {
        row["check"]: row["reasons"]
        for row in tables["pressure_senco9_no_write_checks"]
        if row["status"] == "fail"
    }
    assert "pressure_reference_traceability" in failed
    assert "pressure_point_matrix" in failed
    assert "numeric_pressure_points<3" in failed["pressure_point_matrix"]
    assert "sample_count_per_point" in failed
    assert "sample_count_per_point<10" in failed["sample_count_per_point"]


def test_pressure_senco9_no_write_preflight_report_and_cli_write_artifacts(tmp_path):
    cfg_path = _write_no_write_config(
        tmp_path / "site_v1_5_formal_open_flow_4ch_no_write_900ppm.json",
        [
            {"name": "ga01", "device_id": "023", "port": "COM35", "enabled": True},
            {"name": "ga02", "device_id": "033", "port": "COM36", "enabled": True},
            {"name": "ga03", "device_id": "001", "port": "COM37", "enabled": True},
            {"name": "ga04", "device_id": "091", "port": "COM38", "enabled": True},
        ],
    )
    cfg = load_config(cfg_path)
    reference_path = tmp_path / "pressure_reference.json"
    reference_path.write_text(json.dumps(_pressure_reference(), ensure_ascii=False), encoding="utf-8")

    outputs = write_pressure_senco9_no_write_preflight_report(
        config=cfg,
        config_path=cfg_path,
        pressure_reference_path=reference_path,
        output_dir=tmp_path / "preflight",
        today="2026-05-24",
    )
    assert outputs["workbook"].exists()
    assert outputs["runbook"].exists()
    summary = _read_csv(outputs["pressure_senco9_no_write_summary_csv"])
    assert summary[0]["preflight_status"] == "pass"
    runbook = outputs["runbook"].read_text(encoding="utf-8")
    assert "water_or_gas_route_action: none" in runbook
    assert "writes_senco9: false" in runbook

    cli_dir = tmp_path / "cli_preflight"
    rc = preflight_main(
        [
            "--config",
            str(cfg_path),
            "--pressure-reference-json",
            str(reference_path),
            "--output-dir",
            str(cli_dir),
        ]
    )
    assert rc == 0
    assert (cli_dir / "pressure_senco9_no_write_preflight.xlsx").exists()
    assert (cli_dir / "pressure_senco9_no_write_runbook.md").exists()
