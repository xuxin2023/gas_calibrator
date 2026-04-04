import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from gas_calibrator.v2.core.models import CalibrationPhase
from gas_calibrator.v2.core.run_manifest import (
    RUN_MANIFEST_SCHEMA_VERSION,
    build_run_manifest,
    write_run_manifest,
)


class DangerousDeviceConfig:
    def __init__(
        self,
        *,
        port: str,
        enabled: bool = True,
        baud: int = 9600,
        timeout: float = 1.0,
        description: str = "",
    ) -> None:
        self.port = port
        self.enabled = enabled
        self.baud = baud
        self.timeout = timeout
        self.description = description
        self.probe_calls = 0

    def fetch_all(self):  # pragma: no cover - should never be called
        self.probe_calls += 1
        raise AssertionError("manifest builder must not access live device methods")


def _fake_session():
    pressure = DangerousDeviceConfig(port="COM1", enabled=True, description="pressure controller")
    analyzer = DangerousDeviceConfig(port="COM3", enabled=True, description="analyzer")
    config = SimpleNamespace(
        workflow=SimpleNamespace(
            run_mode="auto_calibration",
            route_mode="h2o_then_co2",
            profile_name="bench_profile",
            profile_version="2.4",
            report_family="v2_product_report_family",
            report_templates={},
            analyzer_setup={
                "software_version": "v5_plus",
                "device_id_assignment_mode": "automatic",
                "start_device_id": "001",
            },
        ),
        devices=SimpleNamespace(
            pressure_controller=pressure,
            pressure_meter=None,
            dewpoint_meter=None,
            humidity_generator=None,
            temperature_chamber=None,
            relay_a=None,
            relay_b=None,
            gas_analyzers=[analyzer],
        ),
        paths=SimpleNamespace(points_excel="points.xlsx", output_dir="output"),
        features=SimpleNamespace(simulation_mode=True, debug_mode=False, use_v2=True),
        storage=SimpleNamespace(host="localhost", password="26372023"),
        ai=SimpleNamespace(enabled=True, api_key="secret-key", provider="openai"),
    )
    session = SimpleNamespace(
        run_id="run_20260320_043540",
        config=config,
        started_at=datetime(2026, 3, 20, 4, 35, 40),
        ended_at=datetime(2026, 3, 20, 4, 38, 0),
        phase=CalibrationPhase.COMPLETED,
        current_point=None,
        enabled_devices={"pressure_controller", "gas_analyzer_0"},
        output_dir=Path("output") / "run_20260320_043540",
        stop_reason="",
        warnings=[],
        errors=[],
    )
    return session, pressure, analyzer


def test_build_run_manifest_is_json_safe_and_redacts_secrets() -> None:
    session, pressure, analyzer = _fake_session()

    manifest = build_run_manifest(
        session,
        source_points_file=Path("points") / "batch_a.xlsx",
        hostname="host-a",
        git_commit="abc1234",
    )

    encoded = json.dumps(manifest, ensure_ascii=False)

    assert encoded
    assert manifest["schema_version"] == RUN_MANIFEST_SCHEMA_VERSION
    assert manifest["run_id"] == "run_20260320_043540"
    assert manifest["source_points_file"] == str(Path("points") / "batch_a.xlsx")
    assert manifest["profile_name"] == "bench_profile"
    assert manifest["profile_version"] == "2.4"
    assert manifest["software_build_id"] is not None
    assert manifest["config_version"].startswith("cfg-")
    assert manifest["points_version"].startswith("pts-")
    assert manifest["report_family"] == "v2_product_report_family"
    assert manifest["analyzer_setup"]["software_version"] == "v5_plus"
    assert manifest["report_templates"]["per_device_output"] is True
    assert {item["key"] for item in manifest["report_templates"]["templates"]} == {
        "co2_test_report",
        "co2_calibration_report",
        "h2o_test_report",
        "h2o_calibration_report",
    }
    assert set(manifest["artifacts"]["role_catalog"]["execution_summary"]) >= {
        "manifest",
        "run_summary",
        "points_readable",
        "acceptance_plan",
        "lineage_summary",
        "evidence_registry",
        "suite_summary",
        "suite_summary_markdown",
        "suite_acceptance_plan",
        "suite_evidence_registry",
    }
    assert set(manifest["artifacts"]["role_catalog"]["diagnostic_analysis"]) >= {
        "qc_report",
        "temperature_snapshots",
        "analytics_summary",
        "trend_registry",
        "suite_analytics_summary",
        "workbench_action_report_json",
        "workbench_action_report_markdown",
        "workbench_action_snapshot",
    }
    assert set(manifest["artifacts"]["role_catalog"]["formal_analysis"]) >= {
        "coefficient_report",
        "coefficient_registry",
    }
    assert manifest["config_snapshot"]["storage"]["password"] == "***REDACTED***"
    assert manifest["config_snapshot"]["ai"]["api_key"] == "***REDACTED***"
    assert manifest["device_snapshot"]["enabled_devices"] == ["gas_analyzer_0", "pressure_controller"]
    assert manifest["device_snapshot"]["configured_devices"]["pressure_controller"]["port"] == "COM1"
    assert manifest["device_snapshot"]["configured_devices"]["gas_analyzers"][0]["id"] == "gas_analyzer_0"
    assert pressure.probe_calls == 0
    assert analyzer.probe_calls == 0


def test_write_run_manifest_creates_manifest_file(tmp_path: Path) -> None:
    session, pressure, analyzer = _fake_session()

    path = write_run_manifest(tmp_path / session.run_id, session, source_points_file="points.xlsx")

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert path.name == "manifest.json"
    assert payload["run_id"] == session.run_id
    assert payload["profile_name"] == "bench_profile"
    assert payload["profile_version"] == "2.4"
    assert payload["software_build_id"] is not None
    assert "acceptance_plan" in payload["artifacts"]["role_catalog"]["execution_summary"]
    assert "suite_summary" in payload["artifacts"]["role_catalog"]["execution_summary"]
    assert "suite_summary_markdown" in payload["artifacts"]["role_catalog"]["execution_summary"]
    assert "suite_acceptance_plan" in payload["artifacts"]["role_catalog"]["execution_summary"]
    assert "suite_evidence_registry" in payload["artifacts"]["role_catalog"]["execution_summary"]
    assert "analytics_summary" in payload["artifacts"]["role_catalog"]["diagnostic_analysis"]
    assert "suite_analytics_summary" in payload["artifacts"]["role_catalog"]["diagnostic_analysis"]
    assert "coefficient_registry" in payload["artifacts"]["role_catalog"]["formal_analysis"]
    assert payload["config_snapshot"]["storage"]["password"] == "***REDACTED***"
    assert payload["environment"]["route_mode"] == "h2o_then_co2"
    assert pressure.probe_calls == 0
    assert analyzer.probe_calls == 0
