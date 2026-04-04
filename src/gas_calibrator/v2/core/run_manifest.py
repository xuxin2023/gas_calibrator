from __future__ import annotations

from dataclasses import fields, is_dataclass
from datetime import date, datetime
from enum import Enum
import json
from pathlib import Path
import socket
from typing import Any, Optional

try:  # pragma: no cover - defensive import
    from ... import __version__ as SOFTWARE_VERSION
except Exception:  # pragma: no cover - defensive
    SOFTWARE_VERSION = ""

from .acceptance_model import build_version_snapshot
from .artifact_catalog import build_default_role_catalog
from ..export.product_report_plan import build_product_report_manifest


RUN_MANIFEST_SCHEMA_VERSION = "1.0"
RUN_MANIFEST_FILENAME = "manifest.json"
_REDACTED = "***REDACTED***"
_SENSITIVE_KEY_PARTS = ("password", "api_key", "token", "secret")


def safe_serialize(value: Any, *, key: Optional[str] = None) -> Any:
    """Convert nested values into JSON-safe data while redacting secrets."""
    if key is not None and _is_sensitive_key(key):
        return _REDACTED if value not in (None, "") else value
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if is_dataclass(value):
        return {
            field.name: safe_serialize(getattr(value, field.name), key=field.name)
            for field in fields(value)
        }
    if isinstance(value, dict):
        return {
            str(item_key): safe_serialize(item_value, key=str(item_key))
            for item_key, item_value in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        return [safe_serialize(item) for item in value]
    if hasattr(value, "__dict__"):
        return {
            str(item_key): safe_serialize(item_value, key=str(item_key))
            for item_key, item_value in vars(value).items()
            if not str(item_key).startswith("_")
        }
    return str(value)


def build_device_snapshot(session: Any) -> dict[str, Any]:
    """Build a static device snapshot from config/session only."""
    config = getattr(session, "config", None)
    devices = getattr(config, "devices", None)
    single_names = (
        "pressure_controller",
        "pressure_meter",
        "dewpoint_meter",
        "humidity_generator",
        "temperature_chamber",
        "relay_a",
        "relay_b",
    )
    configured_devices: dict[str, Any] = {}
    for name in single_names:
        item = None if devices is None else getattr(devices, name, None)
        if item is not None:
            configured_devices[name] = _serialize_device_config(item)
    gas_analyzers = []
    for index, item in enumerate(getattr(devices, "gas_analyzers", []) or []):
        gas_analyzers.append(
            {
                "id": f"gas_analyzer_{index}",
                **_serialize_device_config(item),
            }
        )
    if gas_analyzers:
        configured_devices["gas_analyzers"] = gas_analyzers
    return {
        "enabled_devices": sorted(getattr(session, "enabled_devices", []) or []),
        "configured_devices": configured_devices,
    }


def build_run_manifest(
    session: Any,
    *,
    source_points_file: Optional[str | Path] = None,
    output_files: Optional[list[str]] = None,
    startup_pressure_precheck: Optional[dict[str, Any]] = None,
    software_version: Optional[str] = None,
    hostname: Optional[str] = None,
    operator: Optional[str] = None,
    environment: Optional[dict[str, Any]] = None,
    git_commit: Optional[str] = None,
    extra_sections: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Build a JSON-safe manifest payload for one run."""
    config = getattr(session, "config", None)
    points_path = source_points_file
    if points_path is None and config is not None:
        paths = getattr(config, "paths", None)
        points_path = None if paths is None else getattr(paths, "points_excel", None)

    report_templates = _report_templates_from_config(config)
    version_snapshot = build_version_snapshot(
        config_snapshot=config,
        source_points_file=points_path,
        profile_name=_profile_name_from_config(config),
        profile_version=_profile_version_from_config(config),
        software_build_id=software_version if software_version is not None else SOFTWARE_VERSION,
    )
    payload = {
        "schema_version": RUN_MANIFEST_SCHEMA_VERSION,
        "run_id": str(getattr(session, "run_id", "")),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "software_version": software_version if software_version is not None else SOFTWARE_VERSION,
        "software_build_id": version_snapshot["software_build_id"],
        "run_mode": _run_mode_from_config(config),
        "route_mode": _route_mode_from_config(config),
        "profile_name": _profile_name_from_config(config),
        "profile_version": _profile_version_from_config(config),
        "config_version": version_snapshot["config_version"],
        "points_version": version_snapshot["points_version"],
        "analyzer_setup": _analyzer_setup_from_config(config),
        "report_policy": {
            "formal_calibration_report": _run_mode_from_config(config) == "auto_calibration",
            "include_fleet_stats": _include_fleet_stats_from_config(config),
        },
        "report_templates": report_templates,
        "report_family": _report_family_from_config(config),
        "config_snapshot": safe_serialize(config),
        "device_snapshot": build_device_snapshot(session),
        "source_points_file": None if points_path is None else str(points_path),
        "artifacts": {
            "output_files": list(output_files or []),
            "role_catalog": _artifact_role_catalog(),
        },
        "hostname": hostname if hostname is not None else socket.gethostname(),
        "operator": operator,
        "environment": environment if environment is not None else _build_environment_snapshot(config),
        "git_commit": git_commit,
    }
    if startup_pressure_precheck is not None:
        payload["startup_pressure_precheck"] = safe_serialize(startup_pressure_precheck)
    if extra_sections:
        for key, value in dict(extra_sections).items():
            payload[str(key)] = safe_serialize(value)
    return safe_serialize(payload)


def write_run_manifest(
    run_dir: str | Path,
    session: Any,
    *,
    source_points_file: Optional[str | Path] = None,
    output_files: Optional[list[str]] = None,
    startup_pressure_precheck: Optional[dict[str, Any]] = None,
    software_version: Optional[str] = None,
    hostname: Optional[str] = None,
    operator: Optional[str] = None,
    environment: Optional[dict[str, Any]] = None,
    git_commit: Optional[str] = None,
    filename: str = RUN_MANIFEST_FILENAME,
    extra_sections: Optional[dict[str, Any]] = None,
) -> Path:
    """Build and persist the run manifest."""
    directory = Path(run_dir)
    directory.mkdir(parents=True, exist_ok=True)
    manifest = build_run_manifest(
        session,
        source_points_file=source_points_file,
        output_files=output_files,
        startup_pressure_precheck=startup_pressure_precheck,
        software_version=software_version,
        hostname=hostname,
        operator=operator,
        environment=environment,
        git_commit=git_commit,
        extra_sections=extra_sections,
    )
    path = directory / filename
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _serialize_device_config(item: Any) -> dict[str, Any]:
    return {
        "enabled": bool(getattr(item, "enabled", True)),
        "port": getattr(item, "port", None),
        "baud": getattr(item, "baud", None),
        "timeout": getattr(item, "timeout", None),
        "description": getattr(item, "description", None),
    }


def _build_environment_snapshot(config: Any) -> dict[str, Any]:
    features = None if config is None else getattr(config, "features", None)
    return {
        "simulation_mode": bool(getattr(features, "simulation_mode", False)),
        "debug_mode": bool(getattr(features, "debug_mode", False)),
        "use_v2": bool(getattr(features, "use_v2", False)),
        "run_mode": _run_mode_from_config(config),
        "route_mode": _route_mode_from_config(config),
    }


def _artifact_role_catalog() -> dict[str, list[str]]:
    return build_default_role_catalog()


def _run_mode_from_config(config: Any) -> str:
    workflow = None if config is None else getattr(config, "workflow", None)
    return str(getattr(workflow, "run_mode", "auto_calibration") or "auto_calibration")


def _route_mode_from_config(config: Any) -> str:
    workflow = None if config is None else getattr(config, "workflow", None)
    return str(getattr(workflow, "route_mode", "h2o_then_co2") or "h2o_then_co2")


def _profile_name_from_config(config: Any) -> Optional[str]:
    workflow = None if config is None else getattr(config, "workflow", None)
    value = None if workflow is None else getattr(workflow, "profile_name", None)
    text = str(value or "").strip()
    return text or None


def _profile_version_from_config(config: Any) -> Optional[str]:
    workflow = None if config is None else getattr(config, "workflow", None)
    value = None if workflow is None else getattr(workflow, "profile_version", None)
    text = str(value or "").strip()
    return text or None


def _analyzer_setup_from_config(config: Any) -> dict[str, Any]:
    workflow = None if config is None else getattr(config, "workflow", None)
    payload = {} if workflow is None else getattr(workflow, "analyzer_setup", {}) or {}
    return safe_serialize(dict(payload)) if isinstance(payload, dict) else {}


def _report_family_from_config(config: Any) -> str:
    workflow = None if config is None else getattr(config, "workflow", None)
    explicit = None if workflow is None else getattr(workflow, "report_family", None)
    text = str(explicit or "").strip()
    if text:
        return text
    return str(_report_templates_from_config(config).get("report_family", "") or "")


def _report_templates_from_config(config: Any) -> dict[str, Any]:
    workflow = None if config is None else getattr(config, "workflow", None)
    explicit = None if workflow is None else getattr(workflow, "report_templates", None)
    if isinstance(explicit, dict) and explicit:
        return safe_serialize(dict(explicit))
    return build_product_report_manifest(
        run_mode=_run_mode_from_config(config),
        route_mode=_route_mode_from_config(config),
    )


def _include_fleet_stats_from_config(config: Any) -> bool:
    workflow = None if config is None else getattr(config, "workflow", None)
    reporting = {} if workflow is None else getattr(workflow, "reporting", {}) or {}
    if not isinstance(reporting, dict):
        return False
    return bool(reporting.get("include_fleet_stats", False))


def _is_sensitive_key(key: str) -> bool:
    lowered = str(key or "").strip().lower()
    return any(part in lowered for part in _SENSITIVE_KEY_PARTS)
