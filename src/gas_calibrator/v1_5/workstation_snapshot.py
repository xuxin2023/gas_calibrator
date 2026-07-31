"""Single read-only snapshot contract for the final V1.5 workstation.

The builder consumes already-produced workstation results and bounded local
artifact presence.  It never opens COM ports, controls routes, writes
coefficients, mutates a database, or promotes dry-run evidence.
"""

from __future__ import annotations

import csv
import io
import json
import math
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


SCHEMA_VERSION = "v1_5_workstation_snapshot_v3"
EXPECTED_POINT_COUNTS = {"co2": 45, "h2o": 13}
CONFIGURED_CHANNEL_COUNT = 8
PRODUCTION_PROFILE_ID = "legacy_ratio_production"
SHADOW_PROFILE_ID = "absorption_ratio_shadow"
RUNTIME_FRESH_SECONDS = 20
RUNTIME_STALE_SECONDS = 180
PRESSURE_PAIR_MAX_DELTA_MS = 2_000
_MATURE_IO_NAME = re.compile(r"^io_\d{8}_\d{6}\.csv$")
_MATURE_SAMPLE_NAME = re.compile(r"^samples_\d{8}_\d{6}\.csv$")
_SAMPLE_FIELD_ALIASES = {
    "sample_ts": ("sample_ts", "采样时间"),
    "thermometer_sample_ts": (
        "thermometer_sample_ts",
        "数字温度计缓存采样时间",
    ),
    "thermometer_temp_c": (
        "thermometer_temp_c",
        "数字温度计温度C",
    ),
    "pressure_gauge_sample_ts": (
        "pressure_gauge_sample_ts",
        "数字压力计采样时间",
    ),
    "pressure_gauge_hpa": (
        "pressure_gauge_hpa",
        "gauge_pressure",
        "数字压力计压力hPa",
    ),
    "pace_sample_ts": (
        "pace_sample_ts",
        "压力控制器采样时间",
    ),
    "pace_pressure_hpa": (
        "pace_pressure_hpa",
        "controller_pressure",
        "pressure_controller_hpa",
        "压力控制器压力hPa",
    ),
    "pace_output_state": (
        "pace_output_state",
        "压力控制器输出状态",
    ),
    "pace_isolation_state": (
        "pace_isolation_state",
        "压力控制器隔离状态",
    ),
    "pace_vent_status": (
        "pace_vent_status",
        "压力控制器通大气状态",
    ),
    "dewpoint_sample_ts": (
        "dewpoint_live_sample_ts",
        "dewpoint_sample_ts",
        "露点仪实时采样时间",
    ),
    "dewpoint_c": (
        "dewpoint_live_c",
        "dewpoint_c",
        "露点仪实时露点C",
        "露点仪露点C",
    ),
    "dewpoint_flow_lpm": (
        "dewpoint_flow_lpm",
        "flow_lpm",
        "露点仪实时流量(L/min)",
        "露点仪流量(L/min)",
    ),
}
ARTIFACT_ROLES = (
    "execution_rows",
    "execution_summary",
    "diagnostic_analysis",
    "formal_analysis",
)
EXPORT_STATUSES = ("ok", "skipped", "missing", "error")
REPORT_AUTHORITY = "mature_v1_5_runner_artifacts"
ARTIFACT_DEFINITIONS = (
    (
        "workstation_result_json",
        "V1.5 工作站结果 JSON",
        "v1_5_operator_workstation_dry_run.json",
        "execution_summary",
    ),
    (
        "workstation_result_markdown",
        "V1.5 工作站结果摘要",
        "V1_5_OPERATOR_WORKSTATION_DRY_RUN.md",
        "execution_summary",
    ),
)
SAFETY_FLAGS = (
    "opens_com_ports",
    "controls_water_or_gas_routes",
    "writes_coefficients",
    "writes_device_id",
)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _device_readback_configured(value: Any) -> bool:
    if not isinstance(value, Mapping) or value.get("enabled") is False:
        return False
    return bool(str(value.get("port") or "").strip())


def _sample_value(row: Mapping[str, Any], field: str) -> Any:
    for key in _SAMPLE_FIELD_ALIASES.get(field, (field,)):
        value = row.get(key)
        if value is not None and str(value).strip() != "":
            return value
    return None


def _latest_sample_row(
    rows: Iterable[Mapping[str, Any]],
    *fields: str,
    require_all: bool = False,
) -> Mapping[str, Any]:
    candidates = list(rows)
    for row in reversed(candidates):
        present = [
            _sample_value(row, field) is not None
            for field in fields
        ]
        if present and (all(present) if require_all else any(present)):
            return row
    return {}


def _timestamp_delta_ms(left: Any, right: Any) -> float | None:
    left_value = _parse_artifact_timestamp(left)
    right_value = _parse_artifact_timestamp(right)
    if left_value is None or right_value is None:
        return None
    return abs((left_value - right_value).total_seconds()) * 1_000


def _sample_age_seconds(
    row: Mapping[str, Any],
    timestamp_field: str,
    latest_artifact_timestamp: datetime | None,
) -> float | None:
    if latest_artifact_timestamp is None:
        return None
    sample_timestamp = _parse_artifact_timestamp(
        _sample_value(row, timestamp_field)
        or _sample_value(row, "sample_ts")
    )
    if sample_timestamp is None:
        return None
    return max(
        0.0,
        (latest_artifact_timestamp - sample_timestamp).total_seconds(),
    )


def _sample_freshness(
    value: Any,
    age_seconds: float | None,
    runtime_freshness: str,
) -> str:
    if value is None:
        return "unknown"
    if runtime_freshness != "fresh":
        return runtime_freshness
    if age_seconds is None:
        return "unknown"
    return "fresh" if age_seconds <= RUNTIME_FRESH_SECONDS else "stale"


def _tail_csv_rows(
    path: Path,
    *,
    row_limit: int = 800,
    byte_limit: int = 1_048_576,
) -> list[dict[str, str]]:
    """Read a bounded tail of an append-only CSV without following the file."""

    if row_limit <= 0 or not path.is_file():
        return []
    with path.open("rb") as handle:
        header = handle.readline().decode("utf-8-sig", errors="replace").rstrip(
            "\r\n"
        )
        handle.seek(0, 2)
        size = handle.tell()
        start = max(0, size - max(1, byte_limit))
        handle.seek(start)
        if start:
            handle.readline()
        tail = handle.read(max(1, byte_limit)).decode(
            "utf-8",
            errors="replace",
        )
    if not header or not tail.strip():
        return []
    rows = [
        dict(row)
        for row in csv.DictReader(io.StringIO(f"{header}\n{tail}"))
        if any(str(value or "").strip() for value in row.values())
    ]
    return rows[-row_limit:]


def _load_json_object(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _latest_matching_file(
    directory: Path,
    pattern: re.Pattern[str],
) -> Path | None:
    try:
        candidates = []
        for path in directory.iterdir():
            if not path.is_file() or not pattern.fullmatch(path.name):
                continue
            try:
                candidates.append((path.stat().st_mtime, path))
            except OSError:
                continue
    except OSError:
        return None
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def _latest_mature_run(
    output_root: str | Path | None,
) -> tuple[Path | None, Path | None]:
    """Locate a mature V1/V1.5 run without accepting V2 ``io_log.csv``."""

    if not output_root:
        return None, None
    root = Path(output_root)
    if not root.is_dir():
        return None, None
    direct_io = _latest_matching_file(root, _MATURE_IO_NAME)
    candidates: list[tuple[float, Path, Path]] = []
    if direct_io is not None:
        candidates.append((direct_io.stat().st_mtime, root, direct_io))
    try:
        run_dirs = [
            path
            for path in root.iterdir()
            if path.is_dir()
            and (path.name.startswith("run_") or path.name.startswith("rerun_"))
        ]
    except OSError:
        run_dirs = []
    for run_dir in run_dirs:
        io_path = _latest_matching_file(run_dir, _MATURE_IO_NAME)
        if io_path is not None:
            candidates.append((io_path.stat().st_mtime, run_dir, io_path))
    if not candidates:
        return None, None
    _mtime, run_dir, io_path = max(candidates, key=lambda item: item[0])
    return run_dir, io_path


def _parse_event_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    response = str(row.get("response") or "").strip()
    if not response.startswith("{"):
        return {}
    try:
        payload = json.loads(response)
    except json.JSONDecodeError:
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _parse_artifact_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _common_value(values: Iterable[Any]) -> Any:
    cleaned = [
        value
        for value in values
        if value is not None and str(value).strip() != ""
    ]
    if not cleaned:
        return None
    normalized = {str(value).strip() for value in cleaned}
    return cleaned[0] if len(normalized) == 1 else "mixed"


def _configured_analyzers(
    runtime_config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    devices = runtime_config.get("devices")
    devices = devices if isinstance(devices, Mapping) else {}
    rows = devices.get("gas_analyzers")
    output: list[dict[str, Any]] = []
    if isinstance(rows, list):
        for index, raw in enumerate(rows, start=1):
            if not isinstance(raw, Mapping) or raw.get("enabled") is False:
                continue
            output.append(
                {
                    "display_name": str(
                        raw.get("name") or f"GA{index:02d}"
                    ).upper(),
                    "port": str(raw.get("port") or "").strip().upper(),
                    "operator_confirmed": False,
                    "connected": True,
                    "powered": True,
                    "runtime_evidence": {
                        "ftd_hz": raw.get("ftd_hz"),
                        "average1": raw.get("average1")
                        or raw.get("average_co2"),
                        "average2": raw.get("average2")
                        or raw.get("average_h2o"),
                    },
                }
            )
    return output


def _site_analyzers(
    site_profile: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    profile = site_profile if isinstance(site_profile, Mapping) else {}
    rows = profile.get("candidate_analyzers")
    if not isinstance(rows, list):
        return []
    output: list[dict[str, Any]] = []
    for index, raw in enumerate(rows, start=1):
        if not isinstance(raw, Mapping):
            continue
        identity = raw.get("identity_evidence")
        identity = identity if isinstance(identity, Mapping) else {}
        runtime = raw.get("runtime_evidence")
        runtime = runtime if isinstance(runtime, Mapping) else {}
        output.append(
            {
                "display_name": str(
                    raw.get("ga_label") or f"候选 {index:02d}"
                ),
                "port": str(raw.get("port") or "").strip().upper(),
                "operator_confirmed": raw.get("operator_confirmed") is True,
                "connected": raw.get("connected") is True,
                "powered": raw.get("powered") is True,
                "identity_scope": str(identity.get("scope") or ""),
                "has_bound_identity": bool(
                    str(raw.get("protocol_device_id") or "").strip()
                    and str(raw.get("sn_code") or "").strip()
                ),
                "runtime_evidence": dict(runtime),
            }
        )
    return output


def _runtime_observation(
    *,
    runtime_output_dir: str | Path | None,
    site_profile: Mapping[str, Any] | None,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    """Normalize mature append-only artifacts into a fail-closed read model."""

    now_value = now_utc or datetime.now(timezone.utc)
    if now_value.tzinfo is None:
        now_value = now_value.replace(tzinfo=timezone.utc)
    run_dir, io_path = _latest_mature_run(runtime_output_dir)
    if run_dir is None or io_path is None:
        return {
            "status": "unavailable",
            "freshness_status": "unknown",
            "age_seconds": None,
            "run_id": "",
            "current_stage": "unknown",
            "route_group": "unknown",
            "sample_progress": "unknown",
            "latest_event": "unknown",
            "source_contract": "mature_v1_v1_5_append_only_artifacts",
            "source_files": [],
            "channel_observations": [],
            "reference_observations": {},
            "pressure_chain": {
                "status": "unavailable",
                "calibration_ready": False,
                "controller_role": "pressure_actuator_not_reference",
                "reference_role": "digital_pressure_gauge_metrological_truth",
                "fit_target": "analyzer_internal_pressure_via_SENCO9",
            },
            "contains_paths": False,
            "read_only": True,
        }

    io_mtime = io_path.stat().st_mtime
    age_seconds = max(0, int(now_value.timestamp() - io_mtime))
    if age_seconds <= RUNTIME_FRESH_SECONDS:
        freshness = "fresh"
    elif age_seconds <= RUNTIME_STALE_SECONDS:
        freshness = "aging"
    else:
        freshness = "stale"

    rows = _tail_csv_rows(io_path)
    runtime_config = _load_json_object(run_dir / "runtime_config_snapshot.json")
    analyzers = _site_analyzers(site_profile) or _configured_analyzers(
        runtime_config
    )

    latest_stage: dict[str, Any] = {}
    latest_progress: dict[str, Any] = {}
    latest_event = ""
    run_state = "observed"
    latest_rx_by_port: dict[str, tuple[Mapping[str, Any], datetime | None]] = {}
    latest_row_timestamp: datetime | None = None
    for row in rows:
        row_timestamp = _parse_artifact_timestamp(
            row.get("timestamp") or row.get("ts")
        )
        if (
            row_timestamp is not None
            and (
                latest_row_timestamp is None
                or row_timestamp > latest_row_timestamp
            )
        ):
            latest_row_timestamp = row_timestamp
        port = str(row.get("port") or "").strip().upper()
        direction = str(row.get("direction") or "").strip().upper()
        if port and direction == "RX":
            latest_rx_by_port[port] = (row, row_timestamp)
        if (
            port == "RUN"
            and str(row.get("device") or "").strip().lower() == "runner"
            and direction == "EVENT"
        ):
            command = str(row.get("command") or "").strip()
            payload = _parse_event_payload(row)
            if command == "stage" and payload:
                latest_stage = payload
            elif command == "sample-progress" and payload:
                latest_progress = payload
            if command:
                latest_event = command
            if command == "run-finished":
                run_state = "completed"
            elif command == "run-aborted":
                run_state = "aborted"
            elif command == "run-start":
                run_state = "running"

    channel_observations: list[dict[str, Any]] = []
    for index, analyzer in enumerate(analyzers, start=1):
        port = str(analyzer.get("port") or "").strip().upper()
        last_rx_record = latest_rx_by_port.get(port)
        last_rx = last_rx_record[0] if last_rx_record is not None else None
        last_rx_timestamp = (
            last_rx_record[1] if last_rx_record is not None else None
        )
        frame_age_seconds = (
            max(
                0,
                int(
                    (latest_row_timestamp - last_rx_timestamp).total_seconds()
                ),
            )
            if latest_row_timestamp is not None
            and last_rx_timestamp is not None
            else None
        )
        has_frame = bool(
            last_rx
            and str(last_rx.get("response") or "").strip()
            and not str(last_rx.get("error") or "").strip()
        )
        frame_error = bool(last_rx and str(last_rx.get("error") or "").strip())
        if (
            freshness == "fresh"
            and has_frame
            and (
                frame_age_seconds is None
                or frame_age_seconds <= RUNTIME_FRESH_SECONDS
            )
        ):
            frame_status = "fresh"
            connection_status = "recent_frame"
            health_status = "frame_observed"
        elif last_rx is not None:
            frame_status = "stale"
            connection_status = "stale_frame"
            health_status = "warning_observed" if frame_error else "stale"
        elif analyzer.get("connected") is True:
            frame_status = "missing"
            connection_status = "mapped_not_observed"
            health_status = "not_evaluated"
        else:
            frame_status = "not_evaluated"
            connection_status = "not_selected"
            health_status = "not_evaluated"
        if (
            analyzer.get("operator_confirmed") is True
            and analyzer.get("has_bound_identity") is True
        ):
            identity_status = "operator_confirmed"
        elif str(analyzer.get("identity_scope") or "").startswith(
            "historical_"
        ):
            identity_status = "historical_unconfirmed"
        else:
            identity_status = "not_evaluated"
        channel_observations.append(
            {
                "channel_id": f"CH{index:02d}",
                "display_name": str(
                    analyzer.get("display_name") or f"通道 {index:02d}"
                ),
                "connection_status": connection_status,
                "identity_status": identity_status,
                "health_status": health_status,
                "last_frame_status": frame_status,
                "last_frame_age_seconds": frame_age_seconds,
                "selected": analyzer.get("connected") is True,
                "powered": analyzer.get("powered") is True,
                "runtime_evidence": dict(
                    analyzer.get("runtime_evidence") or {}
                ),
            }
        )

    sample_path = _latest_matching_file(run_dir, _MATURE_SAMPLE_NAME)
    sample_rows = (
        _tail_csv_rows(sample_path, row_limit=200, byte_limit=524_288)
        if sample_path is not None
        else []
    )
    temperature_sample = _latest_sample_row(
        sample_rows,
        "thermometer_temp_c",
    )
    pressure_gauge_sample = _latest_sample_row(
        sample_rows,
        "pressure_gauge_hpa",
    )
    pressure_controller_sample = _latest_sample_row(
        sample_rows,
        "pace_pressure_hpa",
    )
    pressure_pair_sample = _latest_sample_row(
        sample_rows,
        "pressure_gauge_hpa",
        "pace_pressure_hpa",
        require_all=True,
    )
    dewpoint_sample = _latest_sample_row(sample_rows, "dewpoint_c")
    flow_sample = _latest_sample_row(sample_rows, "dewpoint_flow_lpm")
    reference_path = run_dir / "formal_reference_source_record.json"
    reference_record = _load_json_object(reference_path)
    concentration_reference = reference_record.get(
        "h2o_concentration_reference"
    )
    concentration_reference = (
        concentration_reference
        if isinstance(concentration_reference, Mapping)
        else {}
    )
    dewpoint_snapshot = concentration_reference.get("dewpoint_snapshot")
    dewpoint_snapshot = (
        dewpoint_snapshot
        if isinstance(dewpoint_snapshot, Mapping)
        else {}
    )
    route_flow = reference_record.get("route_flow_evidence")
    route_flow = route_flow if isinstance(route_flow, Mapping) else {}
    temperature = _as_float(
        _sample_value(temperature_sample, "thermometer_temp_c")
    )
    pressure = _as_float(
        _sample_value(pressure_gauge_sample, "pressure_gauge_hpa")
    )
    pressure_controller = _as_float(
        _sample_value(pressure_controller_sample, "pace_pressure_hpa")
    )
    dewpoint = _as_float(
        _sample_value(dewpoint_sample, "dewpoint_c")
        or dewpoint_snapshot.get("dewpoint_c")
    )
    flow = _as_float(
        _sample_value(flow_sample, "dewpoint_flow_lpm")
    )
    if (
        flow is None
        and str(route_flow.get("source") or "") == "dewpoint_meter_output"
    ):
        flow = _as_float(
            route_flow.get("dewpoint_meter_output_flow_lpm")
            or route_flow.get("observed_flow_lpm")
        )
    paired_pressure_gauge = _as_float(
        _sample_value(pressure_pair_sample, "pressure_gauge_hpa")
    )
    paired_pressure_controller = _as_float(
        _sample_value(pressure_pair_sample, "pace_pressure_hpa")
    )
    pressure_pair_delta_ms = _timestamp_delta_ms(
        _sample_value(pressure_pair_sample, "pressure_gauge_sample_ts"),
        _sample_value(pressure_pair_sample, "pace_sample_ts"),
    )
    pressure_gauge_age_seconds = _sample_age_seconds(
        pressure_gauge_sample,
        "pressure_gauge_sample_ts",
        latest_row_timestamp,
    )
    pressure_controller_age_seconds = _sample_age_seconds(
        pressure_controller_sample,
        "pace_sample_ts",
        latest_row_timestamp,
    )
    pressure_pair_age_seconds = _sample_age_seconds(
        pressure_pair_sample,
        "sample_ts",
        latest_row_timestamp,
    )
    pressure_pair_coincident = bool(
        paired_pressure_gauge is not None
        and paired_pressure_controller is not None
        and pressure_pair_delta_ms is not None
        and pressure_pair_delta_ms <= PRESSURE_PAIR_MAX_DELTA_MS
    )
    runtime_devices = runtime_config.get("devices")
    runtime_devices = (
        runtime_devices if isinstance(runtime_devices, Mapping) else {}
    )
    controller_config = runtime_devices.get("pressure_controller")
    gauge_config = runtime_devices.get("pressure_gauge")
    controller_configured = _device_readback_configured(controller_config)
    reference_configured = _device_readback_configured(gauge_config)
    pressure_pair_readback_ready = bool(
        freshness == "fresh"
        and pressure_pair_coincident
        and pressure_pair_age_seconds is not None
        and pressure_pair_age_seconds <= RUNTIME_FRESH_SECONDS
    )
    if freshness != "fresh":
        pressure_chain_status = "stale_observation"
    elif pressure is None:
        pressure_chain_status = "reference_missing"
    elif pressure_controller is None:
        pressure_chain_status = "controller_feedback_missing"
    elif not pressure_pair_sample:
        pressure_chain_status = "coincident_pair_missing"
    elif pressure_pair_delta_ms is None:
        pressure_chain_status = "pair_timestamp_missing"
    elif not pressure_pair_coincident:
        pressure_chain_status = "pair_not_coincident"
    elif pressure_pair_age_seconds is None:
        pressure_chain_status = "pair_age_unknown"
    elif pressure_pair_age_seconds > RUNTIME_FRESH_SECONDS:
        pressure_chain_status = "pair_stale"
    elif not reference_configured:
        pressure_chain_status = "reference_not_configured"
    elif not controller_configured:
        pressure_chain_status = "controller_not_configured"
    else:
        pressure_chain_status = "fresh_coincident_observation"
    reference_record_timestamp = (
        datetime.fromtimestamp(reference_path.stat().st_mtime, timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        if reference_path.is_file()
        else ""
    )
    source_files = [io_path.name]
    if (run_dir / "runtime_config_snapshot.json").is_file():
        source_files.append("runtime_config_snapshot.json")
    if sample_path is not None:
        source_files.append(sample_path.name)
    if reference_path.is_file():
        source_files.append(reference_path.name)
    return {
        "status": run_state,
        "freshness_status": freshness,
        "age_seconds": age_seconds,
        "run_id": run_dir.name,
        "current_stage": str(
            latest_stage.get("current") or "unknown"
        ),
        "route_group": str(
            latest_stage.get("route_group") or "unknown"
        ),
        "sample_progress": str(
            latest_progress.get("text") or "unknown"
        ),
        "latest_event": latest_event or "unknown",
        "source_contract": "mature_v1_v1_5_append_only_artifacts",
        "source_files": source_files,
        "pressure_chain": {
            "status": pressure_chain_status,
            "calibration_ready": (
                pressure_chain_status == "fresh_coincident_observation"
                and controller_configured
                and reference_configured
            ),
            "configuration_ready": (
                controller_configured and reference_configured
            ),
            "readback_ready": pressure_pair_readback_ready,
            "controller_configured": controller_configured,
            "reference_configured": reference_configured,
            "controller_role": "pressure_actuator_not_reference",
            "controller_is_pressure_truth": False,
            "reference_role": "digital_pressure_gauge_metrological_truth",
            "fit_target": "analyzer_internal_pressure_via_SENCO9",
            "pair_max_delta_ms": PRESSURE_PAIR_MAX_DELTA_MS,
            "pair_timestamp_delta_ms": pressure_pair_delta_ms,
            "pair_is_coincident": pressure_pair_coincident,
            "pair_age_seconds": pressure_pair_age_seconds,
            "pair_is_recent": (
                pressure_pair_age_seconds is not None
                and pressure_pair_age_seconds <= RUNTIME_FRESH_SECONDS
            ),
            "controller_minus_reference_hpa": (
                paired_pressure_controller - paired_pressure_gauge
                if paired_pressure_controller is not None
                and paired_pressure_gauge is not None
                else None
            ),
            "delta_role": "control_tracking_only_not_SENCO9_fit_residual",
        },
        "channel_observations": channel_observations,
        "reference_observations": {
            "temperature": {
                "value": temperature,
                "unit": "degC",
                "source": "digital_platinum_resistance_thermometer_in_chamber",
                "channel": "thermometer",
                "sample_timestamp": str(
                    _sample_value(
                        temperature_sample,
                        "thermometer_sample_ts",
                    )
                    or _sample_value(temperature_sample, "sample_ts")
                    or ""
                ),
                "freshness_status": freshness if temperature is not None else "unknown",
            },
            "pressure": {
                "value": pressure,
                "unit": "hPa",
                "source": "independent_pressure_reference",
                "channel": "pressure_gauge",
                "sample_timestamp": str(
                    _sample_value(
                        pressure_gauge_sample,
                        "pressure_gauge_sample_ts",
                    )
                    or _sample_value(pressure_gauge_sample, "sample_ts")
                    or ""
                ),
                "freshness_status": _sample_freshness(
                    pressure,
                    pressure_gauge_age_seconds,
                    freshness,
                ),
                "age_seconds": pressure_gauge_age_seconds,
                "role": "metrological_truth_for_pressure_calibration",
                "controller_value_is_substitute": False,
            },
            "pressure_controller": {
                "value": pressure_controller,
                "unit": "hPa",
                "source": "pressure_controller_feedback",
                "channel": "pressure_controller",
                "sample_timestamp": str(
                    _sample_value(
                        pressure_controller_sample,
                        "pace_sample_ts",
                    )
                    or _sample_value(pressure_controller_sample, "sample_ts")
                    or ""
                ),
                "freshness_status": _sample_freshness(
                    pressure_controller,
                    pressure_controller_age_seconds,
                    freshness,
                ),
                "age_seconds": pressure_controller_age_seconds,
                "role": "pressure_actuator_and_tracking_feedback_only",
                "used_as_pressure_truth": False,
                "output_state": _sample_value(
                    pressure_controller_sample,
                    "pace_output_state",
                ),
                "isolation_state": _sample_value(
                    pressure_controller_sample,
                    "pace_isolation_state",
                ),
                "vent_status": _sample_value(
                    pressure_controller_sample,
                    "pace_vent_status",
                ),
            },
            "dewpoint": {
                "value": dewpoint,
                "unit": "degC",
                "source": "dewpoint_meter_output",
                "channel": "dewpoint_meter",
                "sample_timestamp": str(
                    _sample_value(dewpoint_sample, "dewpoint_sample_ts")
                    or _sample_value(dewpoint_sample, "sample_ts")
                    or ""
                ),
                "freshness_status": freshness if dewpoint is not None else "unknown",
            },
            "flow": {
                "value": flow,
                "unit": "L/min",
                "source": "dewpoint_meter_output",
                "channel": "dewpoint_meter_flow_output",
                "sample_timestamp": str(
                    _sample_value(flow_sample, "dewpoint_sample_ts")
                    or _sample_value(flow_sample, "sample_ts")
                    or reference_record_timestamp
                    or ""
                ),
                "freshness_status": freshness if flow is not None else "unknown",
                "role": "existence_and_stability_monitoring_only",
                "used_for_concentration_fit": False,
            },
        },
        "contains_paths": False,
        "read_only": True,
    }


def _clean_list(values: Iterable[Any] | None) -> list[str]:
    if isinstance(values, (str, bytes)):
        values = [values]
    return [
        text
        for value in values or ()
        if (text := str(value or "").strip())
    ]


def _point_counts(execution: Mapping[str, Any]) -> dict[str, int]:
    provided = dict(execution.get("point_counts") or {})
    counts: dict[str, int] = {}
    for route_kind, default in EXPECTED_POINT_COUNTS.items():
        if route_kind not in provided:
            counts[route_kind] = default
            continue
        try:
            counts[route_kind] = int(provided[route_kind])
        except (TypeError, ValueError):
            counts[route_kind] = 0
    return counts


def _route_rows(execution: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in execution.get("route_results") or ():
        if not isinstance(raw, Mapping):
            continue
        route_kind = str(raw.get("route_kind") or "").strip().lower()
        if route_kind not in EXPECTED_POINT_COUNTS:
            continue
        try:
            point_count = int(
                raw.get("dry_run_points")
                or EXPECTED_POINT_COUNTS[route_kind]
            )
        except (TypeError, ValueError):
            point_count = EXPECTED_POINT_COUNTS[route_kind]
        rows.append(
            {
                "route_kind": route_kind,
                "status": str(raw.get("status") or "pending"),
                "point_count": point_count,
                "blockers": _clean_list(raw.get("blockers")),
            }
        )
    return rows


def _plan_route_rows(
    execution: Mapping[str, Any],
    *,
    route_results: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    result_statuses = {
        str(row.get("route_kind") or "").strip().lower(): str(
            row.get("status") or "pending"
        )
        for row in route_results
        if str(row.get("route_kind") or "").strip().lower()
        in EXPECTED_POINT_COUNTS
    }
    provided_routes = execution.get("routes")
    source_rows = (
        provided_routes
        if isinstance(provided_routes, (list, tuple))
        else [
            {
                "route_kind": route_kind,
                "expected_point_count": point_count,
            }
            for route_kind, point_count in EXPECTED_POINT_COUNTS.items()
        ]
    )
    rows: list[dict[str, Any]] = []
    observed: set[str] = set()
    for raw in source_rows:
        if not isinstance(raw, Mapping):
            continue
        route_kind = str(raw.get("route_kind") or "").strip().lower()
        if route_kind not in EXPECTED_POINT_COUNTS or route_kind in observed:
            continue
        observed.add(route_kind)
        try:
            point_count = int(
                raw.get("expected_point_count")
                or raw.get("point_count")
                or EXPECTED_POINT_COUNTS[route_kind]
            )
        except (TypeError, ValueError):
            point_count = EXPECTED_POINT_COUNTS[route_kind]
        rows.append(
            {
                "route_kind": route_kind,
                "point_count": point_count,
                "execution_mode": "mature_runner_dry_run",
                "status": result_statuses.get(route_kind, "planned"),
            }
        )
    for route_kind, point_count in EXPECTED_POINT_COUNTS.items():
        if route_kind not in observed:
            rows.append(
                {
                    "route_kind": route_kind,
                    "point_count": point_count,
                    "execution_mode": "mature_runner_dry_run",
                    "status": result_statuses.get(route_kind, "planned"),
                }
            )
    return rows


def _plan_summary(
    *,
    execution: Mapping[str, Any],
    execution_status: str,
    point_counts: Mapping[str, int],
    route_results: list[dict[str, Any]],
    safety: Mapping[str, Any],
    certificate: Mapping[str, Any],
    warnings: list[str],
    blockers: list[str],
) -> dict[str, Any]:
    routes = _plan_route_rows(execution, route_results=route_results)
    if safety.get("status") != "pass" or blockers:
        status = "blocked"
    elif execution_status == "pass":
        status = "executed_dry_run"
    elif execution_status in {"failed", "error"}:
        status = "failed"
    else:
        status = "planned"
    return {
        "status": status,
        "profile_id": str(
            execution.get("profile_id") or "legacy_ratio_production"
        ),
        "calibration_kernel": str(
            execution.get("calibration_kernel")
            or "v1_5_legacy_ratio_0613_0620_0621"
        ),
        "route_order": ["co2", "h2o"],
        "routes": routes,
        "point_counts": dict(point_counts),
        "total_points": sum(int(value) for value in point_counts.values()),
        "execution_mode": "mature_runner_dry_run",
        "editable": False,
        "point_table_edit_allowed": False,
        "certificate_start_gate": str(
            certificate.get("start_gate") or "non_blocking"
        ),
        "no_write": safety.get("status") == "pass",
        "warnings": list(warnings),
        "blockers": list(blockers),
        "contains_paths": False,
    }


def _qc_summary(
    *,
    execution_status: str,
    point_counts: Mapping[str, int],
    route_results: list[dict[str, Any]],
    safety: Mapping[str, Any],
    blockers: list[str],
) -> dict[str, Any]:
    point_contract_pass = dict(point_counts) == EXPECTED_POINT_COUNTS
    route_statuses = {
        str(row.get("route_kind") or ""): (
            str(row.get("status") or ""),
            int(row.get("point_count") or 0),
        )
        for row in route_results
    }
    route_closure_pass = (
        execution_status == "pass"
        and route_statuses
        == {
            route_kind: ("pass", point_count)
            for route_kind, point_count in EXPECTED_POINT_COUNTS.items()
        }
    )
    if route_closure_pass:
        route_closure_status = "pass"
    elif execution_status in {"pass", "failed", "error", "blocked"}:
        route_closure_status = "fail"
    else:
        route_closure_status = "pending"

    checks = [
        {
            "check_id": "point_count_contract",
            "status": "pass" if point_contract_pass else "fail",
            "evidence_basis": "mature_45_13_queue_contract",
        },
        {
            "check_id": "anchor_separation",
            "status": "pass",
            "evidence_basis": "co2_zero_gas_and_h2o_dry_gas_are_distinct",
        },
        {
            "check_id": "no_write_safety",
            "status": str(safety.get("status") or "blocked"),
            "evidence_basis": "workstation_safety_flags",
        },
        {
            "check_id": "route_dry_run_closure",
            "status": route_closure_status,
            "evidence_basis": "mature_runner_route_results",
        },
        {
            "check_id": "sample_stability",
            "status": "not_evaluated",
            "evidence_basis": "real_samples_not_available",
        },
        {
            "check_id": "real_device_readback",
            "status": "not_evaluated",
            "evidence_basis": "real_device_not_connected",
        },
    ]
    qc_blockers = list(blockers)
    if not point_contract_pass:
        qc_blockers.append("point_count_contract_failed")
    if safety.get("status") != "pass":
        qc_blockers.extend(str(item) for item in safety.get("violations") or ())
    if route_closure_status == "fail":
        qc_blockers.append("route_dry_run_closure_failed")
    qc_blockers = list(dict.fromkeys(qc_blockers))

    if qc_blockers or execution_status in {"failed", "error", "blocked"}:
        overall_status = "blocked"
    elif execution_status == "pass" and route_closure_pass:
        overall_status = "dry_run_pass"
    else:
        overall_status = "pending"
    return {
        "overall_status": overall_status,
        "checks": checks,
        "warnings": [
            "sample_stability_not_evaluated",
            "real_device_readback_not_evaluated",
        ],
        "blockers": qc_blockers,
        "formal_acceptance_status": "not_evaluated",
        "release_status": "not_released",
        "point_evidence_contract": {
            "status": "not_evaluated",
            "authority": "mature_v1_5_runner_artifacts",
            "artifact_roles": [
                "execution_rows",
                "execution_summary",
                "formal_analysis",
            ],
            "available_row_count": 0,
            "reason": "real_samples_not_available",
            "required_fields": [
                "point_id",
                "route_kind",
                "analyzer_id",
                "reference_temperature_c",
                "reference_pressure_hpa",
                "reference_dewpoint_or_humidity",
                "sample_count",
                "stability_status",
                "decision",
                "reason_code",
                "qc_policy_version",
                "threshold_profile_hash",
                "evidence_source",
            ],
        },
        "rule_threshold_governance": {
            "status": "runner_owned_read_only",
            "source": "reviewed_runtime_config_and_mature_runner_qc",
            "ui_edit_allowed": False,
            "policy_version_required": True,
            "threshold_profile_hash_required": True,
        },
        "reject_reason_summary": {
            "status": "not_evaluated",
            "source_artifact_role": "execution_summary",
            "reason_code_required": True,
        },
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
    }


def _device_summary(
    *,
    site_profile: Mapping[str, Any] | None,
    runtime: Mapping[str, Any],
) -> dict[str, Any]:
    site_rows = _site_analyzers(site_profile)
    runtime_channels = [
        dict(item)
        for item in runtime.get("channel_observations") or ()
        if isinstance(item, Mapping)
    ]
    if runtime_channels:
        channels = runtime_channels
    elif site_rows:
        channels = []
        for index, row in enumerate(site_rows, start=1):
            if (
                row.get("operator_confirmed") is True
                and row.get("has_bound_identity") is True
            ):
                identity_status = "operator_confirmed"
            elif str(row.get("identity_scope") or "").startswith("historical_"):
                identity_status = "historical_unconfirmed"
            else:
                identity_status = "not_evaluated"
            channels.append(
                {
                    "channel_id": f"CH{index:02d}",
                    "display_name": str(
                        row.get("display_name") or f"通道 {index:02d}"
                    ),
                    "connection_status": (
                        "mapped_not_observed"
                        if row.get("connected") is True
                        else "not_selected"
                    ),
                    "identity_status": identity_status,
                    "health_status": "not_evaluated",
                    "last_frame_status": "not_evaluated",
                    "selected": row.get("connected") is True,
                    "powered": row.get("powered") is True,
                    "runtime_evidence": dict(
                        row.get("runtime_evidence") or {}
                    ),
                }
            )
    else:
        channels = [
            {
                "channel_id": f"CH{index:02d}",
                "display_name": f"通道 {index:02d}",
                "connection_status": "not_connected",
                "identity_status": "not_evaluated",
                "health_status": "not_evaluated",
                "last_frame_status": "not_evaluated",
                "selected": False,
                "powered": False,
                "runtime_evidence": {},
            }
            for index in range(1, CONFIGURED_CHANNEL_COUNT + 1)
        ]

    runtime_available = str(runtime.get("freshness_status") or "unknown") != "unknown"
    mapped_connected = sum(bool(row.get("connected")) for row in site_rows)
    powered_count = sum(bool(row.get("powered")) for row in site_rows)
    observed_connected = sum(
        str(row.get("connection_status") or "") == "recent_frame"
        for row in channels
    )
    identity_count = sum(
        str(row.get("identity_status") or "") == "operator_confirmed"
        for row in channels
    )
    health_count = sum(
        str(row.get("health_status") or "") == "frame_observed"
        for row in channels
    )
    runtime_values = [
        dict(row.get("runtime_evidence") or {})
        for row in channels
        if row.get("powered") is True
    ]
    upload_rate = _common_value(
        row.get("ftd_hz") for row in runtime_values
    )
    average1 = _common_value(
        row.get("average1") for row in runtime_values
    )
    average2 = _common_value(
        row.get("average2") for row in runtime_values
    )
    if runtime_available:
        overall_status = (
            "runtime_artifact_fresh"
            if runtime.get("freshness_status") == "fresh"
            else "runtime_artifact_stale"
        )
        ui_mode = "mature_runtime_artifact_read_only"
    elif site_rows:
        overall_status = "site_mapping_read_only"
        ui_mode = "site_mapping_read_only"
    else:
        overall_status = "simulation_only"
        ui_mode = "read_only_configured_slots"
    for row in channels:
        row["mode"] = ui_mode
    return {
        "overall_status": overall_status,
        "ui_mode": ui_mode,
        "runtime_state_authority": "mature_v1_v1_5_artifacts_only",
        "real_device_state": (
            str(runtime.get("freshness_status") or "not_evaluated")
            if runtime_available
            else "not_evaluated"
        ),
        "connection_policy": "no_com_no_scan",
        "configured_channel_count": len(channels),
        "reported_connected_count": _as_int(
            (site_profile or {}).get("reported_connected_count")
            if isinstance(site_profile, Mapping)
            else None,
            mapped_connected,
        ),
        "reported_powered_count": _as_int(
            (site_profile or {}).get("reported_powered_count")
            if isinstance(site_profile, Mapping)
            else None,
            powered_count,
        ),
        "mapped_connected_count": mapped_connected,
        "powered_count": powered_count,
        "connected_count": observed_connected,
        "identity_evaluated_count": identity_count,
        "health_evaluated_count": health_count,
        "unknown_health_count": max(0, len(channels) - health_count),
        "channels": channels,
        "runtime_freshness": {
            "status": str(runtime.get("freshness_status") or "unknown"),
            "age_seconds": runtime.get("age_seconds"),
            "stale_after_seconds": RUNTIME_STALE_SECONDS,
            "run_id": str(runtime.get("run_id") or ""),
        },
        "device_control_actions_available": False,
        "hardware_refresh_actions_available": False,
        "simulation_preset_actions_available": False,
        "fault_injection_actions_available": False,
        "route_control_actions_available": False,
        "device_configuration_actions_available": False,
        "initialization_contract": {
            "owner": "mature_v1_5_initialization_flow",
            "runtime_mode": "MODE2",
            "upload_rate_hz": upload_rate if upload_rate is not None else 1,
            "upload_rate_scope": "calibration_upload_timebase",
            "average1": average1,
            "average2": average2,
            "averages_are_independent": True,
            "temperature_coefficients": "SENCO7_SENCO8_neutral",
            "neutralization_evidence_required": True,
            "readback_verification_required": True,
            "performed_by_read_only_workstation": False,
        },
        "contains_ports": False,
        "contains_serial_numbers": False,
        "contains_runtime_device_data": runtime_available,
        "evidence_source": (
            "mature_runner_artifact_read_only"
            if runtime_available
            else "site_mapping_read_only"
            if site_rows
            else "simulated"
        ),
        "not_real_acceptance_evidence": True,
    }


def _algorithm_summary(
    *,
    execution: Mapping[str, Any],
    point_counts: Mapping[str, int],
) -> dict[str, Any]:
    observed_profile_id = str(
        execution.get("profile_id") or PRODUCTION_PROFILE_ID
    )
    profile_matches = observed_profile_id == PRODUCTION_PROFILE_ID
    point_contract_matches = dict(point_counts) == EXPECTED_POINT_COUNTS
    blockers: list[str] = []
    if not profile_matches:
        blockers.extend(
            [
                "observed_profile_is_not_locked_production_default",
                "implicit_profile_switch_forbidden",
            ]
        )
    if not point_contract_matches:
        blockers.append("production_profile_point_count_contract_failed")
    return {
        "overall_status": (
            "locked_production_default" if not blockers else "blocked"
        ),
        "observed_profile_id": observed_profile_id,
        "production_profile": {
            "profile_id": PRODUCTION_PROFILE_ID,
            "algorithm_mode": "legacy_ratio_R",
            "production_default": True,
            "review_status": "reviewed_mature_baseline",
            "point_counts": dict(point_counts),
            "fit_inputs": {
                "co2": "R_CO2",
                "h2o": "R_H2O",
            },
        },
        "shadow_candidates": [
            {
                "profile_id": SHADOW_PROFILE_ID,
                "algorithm_mode": "absorption_ratio_A",
                "production_default": False,
                "evaluation_scope": "offline_shadow",
                "promotion_state": "blocked",
                "writeback_state": "blocked",
                "fit_equation": "A=-ln(R/R0(T))/(P_kPa/100)",
            }
        ],
        "physical_contract": {
            "pressure_sequence": "SENCO9_first",
            "temperature_coefficients": "SENCO7_SENCO8_neutral_by_default",
            "co2_anchor": "co2_zero_gas",
            "h2o_anchor": "h2o_dry_gas",
            "anchors_are_distinct": True,
            "route_behavior": "preserve_mature_v1_5_0620_0621",
        },
        "auto_select": False,
        "profile_selection_actions_available": False,
        "coefficient_write_actions_available": False,
        "warnings": [
            "shadow_candidate_is_not_the_production_default",
            "real_acceptance_not_performed",
        ],
        "blockers": blockers,
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
    }


def _certificate_summary(
    records: Iterable[Mapping[str, Any]] | None,
    *,
    error: str = "",
) -> dict[str, Any]:
    clean_records = [dict(item) for item in records or () if isinstance(item, Mapping)]
    state_counts = Counter(
        str(item.get("review_state") or "draft") for item in clean_records
    )
    return {
        "record_count": len(clean_records),
        "review_state_counts": dict(sorted(state_counts.items())),
        "load_error": str(error or ""),
        "start_gate": "non_blocking",
        "connected_to_calibration": False,
        "formal_release_requires_independent_review": True,
    }


def _artifact_summary(output_dir: str | Path | None) -> dict[str, Any]:
    root = Path(output_dir).resolve() if output_dir else None
    rows: list[dict[str, Any]] = []
    for artifact_id, display_name, filename, role in ARTIFACT_DEFINITIONS:
        present = bool(root is not None and (root / filename).is_file())
        rows.append(
            {
                "artifact_id": artifact_id,
                "display_name": display_name,
                "role": role,
                "export_status": "ok" if present else "missing",
                "present": present,
            }
        )
    return {
        "artifacts": rows,
        "artifact_count": len(rows),
        "present_count": sum(bool(row["present"]) for row in rows),
        "allowed_roles": list(ARTIFACT_ROLES),
        "allowed_export_statuses": list(EXPORT_STATUSES),
        "authority": REPORT_AUTHORITY,
        "ui_mode": "read_only_inventory",
        "export_actions_available": False,
        "formal_release_status": "not_evaluated",
        "formal_release_requires_independent_review": True,
        "formal_certificate_signing_available": False,
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "contains_paths": False,
    }


def _safety_summary(execution: Mapping[str, Any]) -> dict[str, Any]:
    observed = {
        flag: bool(execution.get(flag, False))
        for flag in SAFETY_FLAGS
    }
    violations = [f"{flag}_true" for flag, value in observed.items() if value]
    if execution.get("not_real_acceptance_evidence") is False:
        violations.append("dry_run_claimed_as_real_acceptance")
    return {
        "status": "pass" if not violations else "blocked",
        **observed,
        "not_real_acceptance_evidence": True,
        "violations": violations,
    }


def _review_summary(
    *,
    execution_status: str,
    safety: Mapping[str, Any],
    certificate: Mapping[str, Any],
    blockers: list[str],
) -> dict[str, Any]:
    if safety.get("status") != "pass" or blockers:
        status = "blocked"
    elif execution_status == "pass":
        status = "dry_run_review_ready"
    else:
        status = "pending"
    sections = [
        {
            "key": "execution",
            "status": execution_status,
            "blockers": list(blockers),
        },
        {
            "key": "physical_anchors",
            "status": "preserved",
            "co2_anchor": "co2_zero_gas",
            "h2o_anchor": "h2o_dry_gas",
        },
        {
            "key": "certificate",
            "status": (
                "load_error"
                if certificate.get("load_error")
                else "advisory"
            ),
            "blockers": [],
        },
        {
            "key": "safety",
            "status": safety.get("status"),
            "blockers": list(safety.get("violations") or []),
        },
        {
            "key": "release",
            "status": "not_real_acceptance",
            "blockers": ["real_acceptance_not_performed"],
        },
    ]
    next_actions = (
        ["复核阻断项后重新执行仿真演练。"]
        if status == "blocked"
        else ["继续人工审核；不得把 dry-run 解释为真机验收。"]
    )
    return {
        "overall_status": status,
        "sections": sections,
        "next_actions": next_actions,
        "approval_actions_available": False,
        "coefficient_write_actions_available": False,
    }


def build_workstation_snapshot(
    *,
    execution: Mapping[str, Any] | None = None,
    output_dir: str | Path | None = None,
    runtime_output_dir: str | Path | None = None,
    site_profile: Mapping[str, Any] | None = None,
    certificate_records: Iterable[Mapping[str, Any]] | None = None,
    certificate_error: str = "",
    decision_model: Mapping[str, Any] | None = None,
    decision_authority_binding: Mapping[str, Any] | None = None,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    """Build the one read model consumed by V1.5 result/review surfaces."""

    payload = dict(execution or {})
    execution_status = str(payload.get("overall_status") or "not_started")
    point_counts = _point_counts(payload)
    routes = _route_rows(payload)
    warnings = _clean_list(payload.get("warnings"))
    blockers = [
        *_clean_list(payload.get("blockers")),
        *_clean_list(payload.get("execution_blockers")),
    ]
    certificate = _certificate_summary(
        certificate_records,
        error=certificate_error,
    )
    safety = _safety_summary(payload)
    reports = _artifact_summary(output_dir)
    plan = _plan_summary(
        execution=payload,
        execution_status=execution_status,
        point_counts=point_counts,
        route_results=routes,
        safety=safety,
        certificate=certificate,
        warnings=warnings,
        blockers=blockers,
    )
    qc = _qc_summary(
        execution_status=execution_status,
        point_counts=point_counts,
        route_results=routes,
        safety=safety,
        blockers=blockers,
    )
    runtime = _runtime_observation(
        runtime_output_dir=runtime_output_dir,
        site_profile=site_profile,
        now_utc=now_utc,
    )
    devices = _device_summary(
        site_profile=site_profile,
        runtime=runtime,
    )
    runtime_public = dict(runtime)
    runtime_public.pop("channel_observations", None)
    runtime_freshness = str(
        runtime_public.get("freshness_status") or "unknown"
    )
    physical_reference = {
        "temperature_truth": {
            "source": "digital_platinum_resistance_thermometer_in_chamber",
            "chamber_controller_display_is_truth": False,
        },
        "pressure_sequence": "SENCO9_first",
        "pressure": {
            "truth_source": "digital_pressure_gauge",
            "controller_role": "pressure_actuator_not_reference",
            "controller_is_pressure_truth": False,
            "fit_target": "analyzer_internal_pressure_via_SENCO9",
            "controller_reference_delta_role": (
                "control_tracking_only_not_SENCO9_fit_residual"
            ),
        },
        "pressure_chain": dict(runtime_public.get("pressure_chain") or {}),
        "flow": {
            "source": "dewpoint_meter_output",
            "required_metadata": [
                "unit",
                "sample_timestamp",
                "channel",
                "freshness_status",
            ],
            "role": "existence_and_stability_monitoring_only",
            "used_for_concentration_fit": False,
        },
        "sampling": {
            "calibration_upload_timebase_hz": 1,
            "raw_device_internal_acquisition_rate_claimed": False,
            "average1_average2_are_independent": True,
        },
        "anchors": {
            "co2": "co2_zero_gas",
            "h2o_wet_point_count": 13,
            "h2o_dry_anchor": "h2o_dry_gas",
            "h2o_dry_anchor_is_additional": True,
            "anchors_are_distinct": True,
        },
        "observations": dict(runtime_public.get("reference_observations") or {}),
    }
    algorithm = _algorithm_summary(
        execution=payload,
        point_counts=point_counts,
    )
    review = _review_summary(
        execution_status=execution_status,
        safety=safety,
        certificate=certificate,
        blockers=blockers,
    )
    snapshot = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _now(),
        "product_name": "V1.5 气体分析仪校准工作站",
        "product_version": "V1.5",
        "display_mode": (
            "mature_runtime_artifact_read_only"
            if runtime_freshness != "unknown"
            else "simulated_read_only"
        ),
        "channel_count": int(devices["configured_channel_count"]),
        "overall_status": execution_status,
        "point_counts": point_counts,
        "runtime": runtime_public,
        "physical_reference": physical_reference,
        "run": {
            "run_id": str(payload.get("run_id") or ""),
            "status": execution_status,
            "mode": "dry_run",
            "calibration_kernel": str(
                payload.get("calibration_kernel")
                or "v1_5_legacy_ratio_0613_0620_0621"
            ),
            "profile_id": str(
                payload.get("profile_id") or "legacy_ratio_production"
            ),
            "point_counts": point_counts,
            "route_order": ["co2", "h2o"],
            "route_results": routes,
            "warnings": warnings,
            "blockers": blockers,
        },
        "results": {
            "status": execution_status,
            "point_counts": point_counts,
            "route_results": routes,
            "anchors": {
                "co2": {
                    "kind": "co2_zero_gas",
                    "independent_from_h2o": True,
                },
                "h2o": {
                    "kind": "h2o_dry_gas",
                    "independent_from_co2": True,
                },
            },
            "warnings": warnings,
            "blockers": blockers,
        },
        "plan": plan,
        "qc": qc,
        "devices": devices,
        "algorithm": algorithm,
        "reports": reports,
        "review": review,
        "decision_model": dict(decision_model or {}),
        "decision_authority_binding": dict(decision_authority_binding or {}),
        "certificate": certificate,
        "safety": safety,
        "evidence_source": (
            "mature_runner_artifact_read_only"
            if runtime_freshness != "unknown"
            else "simulated"
        ),
        "not_real_acceptance_evidence": True,
        "opens_com_ports": safety["opens_com_ports"],
        "controls_water_or_gas_routes": safety[
            "controls_water_or_gas_routes"
        ],
        "writes_coefficients": safety["writes_coefficients"],
        "writes_device_id": safety["writes_device_id"],
    }
    return snapshot


__all__ = [
    "ARTIFACT_DEFINITIONS",
    "ARTIFACT_ROLES",
    "CONFIGURED_CHANNEL_COUNT",
    "EXPORT_STATUSES",
    "EXPECTED_POINT_COUNTS",
    "PRODUCTION_PROFILE_ID",
    "REPORT_AUTHORITY",
    "RUNTIME_FRESH_SECONDS",
    "RUNTIME_STALE_SECONDS",
    "SCHEMA_VERSION",
    "SHADOW_PROFILE_ID",
    "build_workstation_snapshot",
]
