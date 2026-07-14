"""No-write pressure/SENCO9 collection preflight for V1.5.

This module is offline-only. It turns a runtime config and pressure-point list
into a bench checklist for pressure-channel evidence collection. It never opens
COM ports, controls valves/PACE, switches water or gas routes, or writes
SENCO9.
"""

from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .pressure_channel import validate_pressure_reference_traceability
from .reporting import ValidationMetadata, write_validation_report


DEFAULT_PRESSURE_POINTS: tuple[str | float, ...] = (
    "ambient",
    1100.0,
    1000.0,
    900.0,
    800.0,
    700.0,
    600.0,
    500.0,
)

MATURE_SEVEN_POINT_PRESSURE_MATRIX_HPA: tuple[float, ...] = (
    500.0,
    600.0,
    700.0,
    800.0,
    900.0,
    1000.0,
    1100.0,
)


def _compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _table_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return _compact_json(value)
    return value


def _nested_get(mapping: Mapping[str, Any], path: str, default: Any = None) -> Any:
    current: Any = mapping
    for part in path.split("."):
        if not isinstance(current, Mapping):
            return default
        current = current.get(part)
    return default if current is None else current


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _load_json(path: str | Path | None) -> Dict[str, Any]:
    if path is None:
        return {}
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(f"JSON file not found: {source}")
    return json.loads(source.read_text(encoding="utf-8"))


def normalize_pressure_points(raw: Any = None) -> List[str | float]:
    """Normalize a pressure point specification for the no-write runbook."""

    if raw in (None, ""):
        return list(DEFAULT_PRESSURE_POINTS)
    if isinstance(raw, str):
        items = [item.strip() for item in raw.split(",") if item.strip()]
    elif isinstance(raw, Iterable):
        items = list(raw)
    else:
        items = [raw]

    out: List[str | float] = []
    for item in items:
        text = str(item).strip()
        if text.lower() in {"ambient", "atmosphere", "current_atmosphere", "ambient_open"}:
            out.append("ambient")
            continue
        numeric = _safe_float(item)
        if numeric is None:
            out.append(text)
        else:
            out.append(float(numeric))
    return out


def _numeric_pressure_points(points: Sequence[str | float]) -> List[float]:
    values: List[float] = []
    for item in points:
        numeric = _safe_float(item)
        if numeric is not None:
            values.append(float(numeric))
    return values


def _has_ambient(points: Sequence[str | float]) -> bool:
    return any(str(item).strip().lower() == "ambient" for item in points)


def _pressure_span(points: Sequence[float]) -> Optional[float]:
    if not points:
        return None
    return float(max(points) - min(points))


def _is_mature_seven_point_pressure_matrix(points: Sequence[float]) -> bool:
    unique_points = sorted(set(float(item) for item in points))
    expected_points = list(MATURE_SEVEN_POINT_PRESSURE_MATRIX_HPA)
    return len(unique_points) == len(expected_points) and all(
        math.isclose(actual, expected, rel_tol=0.0, abs_tol=1e-6)
        for actual, expected in zip(unique_points, expected_points)
    )


def _enabled_device(config: Mapping[str, Any], name: str) -> bool:
    devices = config.get("devices") if isinstance(config, Mapping) else {}
    device = devices.get(name) if isinstance(devices, Mapping) else {}
    return bool(isinstance(device, Mapping) and device.get("enabled", True))


def _enabled_analyzers(config: Mapping[str, Any]) -> List[Dict[str, Any]]:
    devices = config.get("devices") if isinstance(config, Mapping) else {}
    if not isinstance(devices, Mapping):
        return []
    analyzers = devices.get("gas_analyzers")
    if isinstance(analyzers, list) and analyzers:
        source = [item for item in analyzers if isinstance(item, Mapping)]
    else:
        single = devices.get("gas_analyzer")
        source = [single] if isinstance(single, Mapping) else []
    out: List[Dict[str, Any]] = []
    for index, item in enumerate(source, start=1):
        if not item.get("enabled", True):
            continue
        payload = dict(item)
        payload.setdefault("name", f"ga{index:02d}")
        out.append(payload)
    return out


def assess_pressure_senco9_no_write_config(config: Optional[Mapping[str, Any]]) -> tuple[str, List[str], List[str]]:
    """Assess config safety for pressure/SENCO9 no-write data collection."""

    if config is None:
        return "fail", ["config_missing"], []

    reasons: List[str] = []
    warnings: List[str] = []
    checks = {
        "workflow.controlled_write": _nested_get(config, "workflow.controlled_write", False),
        "workflow.controlled_write_enabled": _nested_get(config, "workflow.controlled_write_enabled", False),
        "workflow.postrun_corrected_delivery.write_devices": _nested_get(
            config,
            "workflow.postrun_corrected_delivery.write_devices",
            False,
        ),
        "workflow.postrun_corrected_delivery.write_pressure_coefficients": _nested_get(
            config,
            "workflow.postrun_corrected_delivery.write_pressure_coefficients",
            False,
        ),
        "workflow.startup_pressure_sensor_calibration.enabled": _nested_get(
            config,
            "workflow.startup_pressure_sensor_calibration.enabled",
            False,
        ),
        "workflow.startup_pressure_sensor_calibration.apply_write": _nested_get(
            config,
            "workflow.startup_pressure_sensor_calibration.apply_write",
            False,
        ),
        "validation.dry_collect.write_coefficients": _nested_get(
            config,
            "validation.dry_collect.write_coefficients",
            False,
        ),
        "validation.coefficient_roundtrip.write_back_same": _nested_get(
            config,
            "validation.coefficient_roundtrip.write_back_same",
            False,
        ),
        "validation.coefficient_roundtrip.allow_write_modified": _nested_get(
            config,
            "validation.coefficient_roundtrip.allow_write_modified",
            False,
        ),
        "coefficients.enabled": _nested_get(config, "coefficients.enabled", False),
        "metadata.writes_senco": _nested_get(config, "metadata.writes_senco", False),
        "metadata.writes_device_id": _nested_get(config, "metadata.writes_device_id", False),
    }
    for key, value in checks.items():
        if _truthy(value):
            reasons.append(f"{key}_enabled")

    for path in ("sencos", "coefficients.sencos"):
        value = _nested_get(config, path, {})
        if isinstance(value, Mapping) and value:
            reasons.append(f"{path}_present")

    analyzers = _enabled_analyzers(config)
    if not analyzers:
        reasons.append("gas_analyzers_missing")
    ids = [str(item.get("device_id") or "").strip() for item in analyzers if str(item.get("device_id") or "").strip()]
    if len(ids) != len(analyzers):
        warnings.append("configured_analyzer_device_id_missing")
    if len(set(ids)) != len(ids):
        reasons.append("configured_analyzer_device_id_not_unique")
    if not _enabled_device(config, "pressure_gauge"):
        reasons.append("com22_pressure_gauge_disabled")
    if not (_enabled_device(config, "pressure_controller") or _enabled_device(config, "pace")):
        warnings.append("pace_pressure_controller_not_enabled")

    devices = config.get("devices") if isinstance(config, Mapping) else {}
    if not isinstance(devices, Mapping):
        reasons.append("devices_config_missing")
        devices = {}
    for key in (
        "humidity_generator",
        "dewpoint_meter",
        "temperature_chamber",
        "thermometer",
        "relay",
        "relay_8",
    ):
        if not isinstance(devices.get(key), Mapping):
            reasons.append(f"devices.{key}_config_missing")

    controller_key = "pressure_controller" if isinstance(devices.get("pressure_controller"), Mapping) else "pace"
    controller = devices.get(controller_key)
    if isinstance(controller, Mapping) and controller.get("enabled", True):
        for key in ("port", "baud", "in_limits_pct", "in_limits_time_s"):
            if controller.get(key) in (None, ""):
                reasons.append(f"devices.{controller_key}.{key}_missing")
        pressure_queries = controller.get("pressure_queries")
        if not isinstance(pressure_queries, Sequence) or isinstance(pressure_queries, (str, bytes)):
            reasons.append(f"devices.{controller_key}.pressure_queries_missing")
        elif ":SENS:PRES:INL?" not in {str(item).strip().upper() for item in pressure_queries}:
            reasons.append(f"devices.{controller_key}.pressure_queries_missing_inl")

    pressure_gauge = devices.get("pressure_gauge")
    if isinstance(pressure_gauge, Mapping) and pressure_gauge.get("enabled", True):
        for key in ("port", "baud"):
            if pressure_gauge.get(key) in (None, ""):
                reasons.append(f"devices.pressure_gauge.{key}_missing")

    active_labels = [
        str(item.get("name") or "")
        for item in analyzers
        if _truthy(item.get("active_send", False))
    ]
    if active_labels:
        warnings.append("active_send_enabled_for_" + ",".join(active_labels))

    return ("pass" if not reasons else "fail"), reasons, warnings


def build_pressure_senco9_no_write_plan_tables(
    *,
    config: Mapping[str, Any],
    config_path: str | Path | None = None,
    pressure_reference: Optional[Mapping[str, Any]] = None,
    pressure_reference_path: str | Path | None = None,
    pressure_points: Any = None,
    sample_count: int = 12,
    interval_s: float = 1.0,
    min_numeric_points: int = 3,
    min_pressure_span_hpa: float = 300.0,
    require_ambient: Optional[bool] = None,
    require_traceable_pressure_reference: bool = True,
    today: Any = None,
) -> tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any]]:
    """Build the pressure/SENCO9 no-write preflight checklist tables."""

    points = normalize_pressure_points(pressure_points)
    numeric_points = _numeric_pressure_points(points)
    unique_numeric = sorted(set(float(item) for item in numeric_points))
    span = _pressure_span(unique_numeric)
    mature_seven_point_matrix = _is_mature_seven_point_pressure_matrix(unique_numeric)
    ambient_required = bool(require_ambient) if require_ambient is not None else not mature_seven_point_matrix
    analyzers = _enabled_analyzers(config)
    config_status, config_reasons, config_warnings = assess_pressure_senco9_no_write_config(config)
    traceability = validate_pressure_reference_traceability(pressure_reference or {}, today=today)

    checks: List[Dict[str, Any]] = []

    def add_check(name: str, status: str, reasons: Sequence[str], **extra: Any) -> None:
        checks.append(
            {
                "check": name,
                "status": status,
                "reasons": ";".join(str(item) for item in reasons if item),
                **{str(key): _table_value(value) for key, value in extra.items()},
            }
        )

    add_check(
        "no_write_config",
        config_status,
        config_reasons,
        warnings=";".join(config_warnings),
    )
    add_check(
        "pressure_reference_traceability",
        "pass" if traceability.status == "pass" or not require_traceable_pressure_reference else "fail",
        traceability.reasons if traceability.status != "pass" and require_traceable_pressure_reference else [],
        validation_level=traceability.validation_level,
        reference_device_id=traceability.device_id,
        certificate_id=traceability.certificate_id,
    )
    add_check(
        "pressure_point_matrix",
        "pass"
        if len(unique_numeric) >= int(min_numeric_points)
        and span is not None
        and span >= float(min_pressure_span_hpa)
        else "fail",
        [
            reason
            for reason in (
                f"numeric_pressure_points<{int(min_numeric_points)}"
                if len(unique_numeric) < int(min_numeric_points)
                else "",
                f"pressure_span_hpa={(span or 0.0):.3f}<required={float(min_pressure_span_hpa):.3f}"
                if span is None or span < float(min_pressure_span_hpa)
                else "",
            )
            if reason
        ],
        numeric_pressure_points=_compact_json(unique_numeric),
        pressure_span_hpa=span,
    )
    add_check(
        "ambient_reference_point",
        "pass" if _has_ambient(points) or not ambient_required else "fail",
        [] if _has_ambient(points) or not ambient_required else ["ambient_point_missing"],
        ambient_required=ambient_required,
        mature_seven_point_matrix=mature_seven_point_matrix,
        policy=(
            "explicit"
            if require_ambient is not None
            else "mature_seven_point_matrix_does_not_require_ambient"
            if mature_seven_point_matrix
            else "ambient_required_for_non_mature_matrix"
        ),
    )
    add_check(
        "sample_count_per_point",
        "pass" if int(sample_count) >= 10 else "fail",
        [] if int(sample_count) >= 10 else ["sample_count_per_point<10"],
        sample_count=sample_count,
    )
    add_check(
        "runtime_physical_boundary",
        "pass",
        [],
        opens_com_ports="only_when_user_runs_collection_tool",
        controls_water_or_gas_routes=False,
        controls_humidity_generator=False,
        writes_senco9=False,
        writes_device_id=False,
        formal_co2_h2o_fit=False,
    )

    failed = [row for row in checks if row["status"] == "fail"]
    preflight_status = "pass" if not failed else "fail"
    pressure_token = ",".join("ambient" if str(item).lower() == "ambient" else f"{float(item):g}" for item in points)
    continuous_hold_requirement = "--require-continuous-atmosphere-hold " if _has_ambient(points) else ""
    collection_command = (
        "python -m gas_calibrator.tools.validate_pressure_only "
        f"--config \"{Path(config_path).resolve() if config_path else '<config.json>'}\" "
        f"--pressure-points \"{pressure_token}\" "
        f"--count {int(sample_count)} --interval-s {float(interval_s):g} "
        f"{continuous_hold_requirement}"
        "--control-pressure-points "
        "--pressure-control-setpoint-mode absolute "
        "--pressure-control-slew-mode max "
        "--pressure-control-allow-overshoot "
        "--pressure-control-tolerance-hpa 1.0 "
        "--pressure-control-stable-s 10 "
        "--pressure-control-timeout-s 240 "
        "--pressure-control-atmosphere-release-wait-s 1.5 "
        "--pressure-control-post-stable-wait-s 5 "
        "--pressure-control-analyzer-stream-flush-s 2 "
        "--analyzer-active-upload-hz 1 "
        "--no-prompt"
    )
    fit_command = (
        "python -m gas_calibrator.tools.export_v1_5_pressure_senco9_evaluation "
        "--run-dir \"<pressure_run_dir>\" --analyzer-prefix all"
    )
    if pressure_reference_path:
        collection_command += f" --pressure-reference-json \"{Path(pressure_reference_path).resolve()}\""
        fit_command += f" --pressure-reference-json \"{Path(pressure_reference_path).resolve()}\""

    summary = [
        {
            "preflight_status": preflight_status,
            "failed_checks": ";".join(str(row["check"]) for row in failed),
            "collection_mode": "pressure_senco9_no_write_multi_point",
            "pressure_points": pressure_token,
            "sample_count_per_point": int(sample_count),
            "sample_interval_s": float(interval_s),
            "analyzer_count": len(analyzers),
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_senco9": False,
            "writes_device_id": False,
            "not_real_acceptance_evidence": True,
        }
    ]

    point_rows: List[Dict[str, Any]] = []
    for index, point in enumerate(points, start=1):
        is_ambient = str(point).strip().lower() == "ambient"
        point_rows.append(
            {
                "order": index,
                "pressure_target_hpa": "" if is_ambient else float(point),
                "pressure_mode": "ambient_open" if is_ambient else "manual_or_pace_prepared_pressure_plateau",
                "sample_count": int(sample_count),
                "interval_s": float(interval_s),
                "primary_reference": "COM22",
                "auxiliary_reference": "PACE",
                "physical_meaning": (
                    "same open-atmosphere state as V1.5 open-flow baseline"
                    if is_ambient
                    else "pressure channel only; verify analyzer internal P against COM22 at this plateau"
                ),
                "water_or_gas_route_action": "none",
                "senco9_write": "forbidden",
            }
        )

    analyzer_rows: List[Dict[str, Any]] = []
    for index, item in enumerate(analyzers, start=1):
        analyzer_rows.append(
            {
                "analyzer_prefix": str(item.get("name") or f"ga{index:02d}"),
                "configured_device_id": str(item.get("device_id") or ""),
                "port": str(item.get("port") or ""),
                "baud": item.get("baud", item.get("baudrate", "")),
                "mode": item.get("mode", ""),
                "active_send": bool(_truthy(item.get("active_send", False))),
                "ftd_hz": item.get("ftd_hz", ""),
                "average_filter": item.get("average_filter", ""),
                "average_co2": item.get("average_co2", ""),
                "average_h2o": item.get("average_h2o", ""),
                "identity_rule": "use analyzer MODE2 frame ID as identity; do not write device ID",
            }
        )

    command_rows = [
        {
            "step": 1,
            "command": collection_command,
            "purpose": "collect no-write pressure pairs for every analyzer",
            "safety_boundary": "does not switch water/gas routes and does not write coefficients",
        },
        {
            "step": 2,
            "command": fit_command,
            "purpose": "fit no-write SENCO9 offset/linear diagnostics from artifacts",
            "safety_boundary": "offline artifact read only; candidate command is review evidence only",
        },
    ]

    tables = {
        "pressure_senco9_no_write_summary": summary,
        "pressure_senco9_no_write_checks": checks,
        "pressure_point_plan": point_rows,
        "analyzer_identity_plan": analyzer_rows,
        "command_plan": command_rows,
    }
    context = {
        "preflight_status": preflight_status,
        "collection_command": collection_command,
        "fit_command": fit_command,
        "pressure_points": pressure_token,
    }
    return tables, context


def _runbook_markdown(tables: Mapping[str, Sequence[Mapping[str, Any]]], context: Mapping[str, Any]) -> str:
    summary = (tables.get("pressure_senco9_no_write_summary") or [{}])[0]
    lines = [
        "# V1.5 Pressure/SENCO9 No-Write Runbook",
        "",
        f"- preflight_status: {summary.get('preflight_status', '')}",
        f"- pressure_points: {summary.get('pressure_points', '')}",
        f"- sample_count_per_point: {summary.get('sample_count_per_point', '')}",
        "- water_or_gas_route_action: none",
        "- writes_senco9: false",
        "- writes_device_id: false",
        "- formal_co2_h2o_fit: false",
        "",
        "## Collection Command",
        "",
        "```powershell",
        str(context.get("collection_command") or ""),
        "```",
        "",
        "## Fit Command",
        "",
        "```powershell",
        str(context.get("fit_command") or ""),
        "```",
        "",
        "## Physical Meaning",
        "",
        "This run isolates analyzer internal pressure P from CO2/H2O fitting. "
        "COM22 is the primary pressure reference; PACE is auxiliary. "
        "The output can only decide whether a SENCO9 review is justified.",
    ]
    return "\n".join(lines) + "\n"


def write_pressure_senco9_no_write_preflight_report(
    *,
    config: Mapping[str, Any],
    config_path: str | Path | None = None,
    pressure_reference_path: str | Path | None = None,
    output_dir: str | Path,
    pressure_points: Any = None,
    sample_count: int = 12,
    interval_s: float = 1.0,
    require_traceable_pressure_reference: bool = True,
    today: Any = None,
) -> Dict[str, Path]:
    pressure_reference = _load_json(pressure_reference_path) if pressure_reference_path else {}
    tables, context = build_pressure_senco9_no_write_plan_tables(
        config=config,
        config_path=config_path,
        pressure_reference=pressure_reference,
        pressure_reference_path=pressure_reference_path,
        pressure_points=pressure_points,
        sample_count=sample_count,
        interval_s=interval_s,
        require_traceable_pressure_reference=require_traceable_pressure_reference,
        today=today,
    )
    destination = Path(output_dir).resolve()
    metadata = ValidationMetadata(
        tool_name="export_v1_5_pressure_senco9_no_write_preflight",
        created_at=datetime.now().isoformat(timespec="seconds"),
        analyzers=[
            str(row.get("analyzer_prefix") or "")
            for row in tables.get("analyzer_identity_plan", [])
            if row.get("analyzer_prefix")
        ],
        input_paths=[
            str(Path(config_path).resolve()) if config_path else "",
            str(Path(pressure_reference_path).resolve()) if pressure_reference_path else "",
        ],
        output_dir=str(destination),
        config_summary={
            "preflight_status": context.get("preflight_status", ""),
            "pressure_points": context.get("pressure_points", ""),
            "write_allowed": False,
        },
        notes=[
            "Offline pressure/SENCO9 no-write preflight.",
            "No COM ports are opened and no water/gas route, valve, PACE, SENCO9, or device-ID writes are performed.",
            "The generated collection command is a bench instruction and still requires operator supervision.",
        ],
    )
    outputs = write_validation_report(
        destination,
        prefix="pressure_senco9_no_write_preflight",
        metadata=metadata,
        tables=tables,
    )
    runbook_path = destination / "pressure_senco9_no_write_runbook.md"
    runbook_path.write_text(_runbook_markdown(tables, context), encoding="utf-8")
    outputs["runbook"] = runbook_path
    return outputs
