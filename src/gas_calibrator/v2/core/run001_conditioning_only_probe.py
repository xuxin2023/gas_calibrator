from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any, Mapping, Optional

from .no_write_guard import build_no_write_guard_from_raw_config
from .run001_a2_no_write import PRESSURE_READ_LATENCY_SAMPLE_FIELDS
from .services.timing_monitor_service import (
    TIMING_EVENT_FIELDS,
    WORKFLOW_TIMING_SUMMARY_FILENAME,
    WORKFLOW_TIMING_TRACE_FILENAME,
)


CONDITIONING_ONLY_SCOPE = "run001_conditioning_only_co2_skip0_no_write"
CONDITIONING_ONLY_FINAL_DECISION = "CONDITIONING_ONLY_SIMULATED_PASS"
CONDITIONING_ONLY_SCHEMA_VERSION = "v2.run001.conditioning_only_probe.1"
CONDITIONING_ONLY_GOVERNANCE_STATEMENT = (
    "simulated_no_com; not real acceptance; not A1 PASS; not A2 PASS; "
    "not V2 replacement; not real_primary_latest"
)


def _read_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _section(raw_cfg: Mapping[str, Any], name: str) -> dict[str, Any]:
    candidate = raw_cfg.get(name)
    return dict(candidate) if isinstance(candidate, Mapping) else {}


def _as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _timestamp(base: datetime, offset_s: float) -> str:
    return (base + timedelta(seconds=float(offset_s))).isoformat(timespec="milliseconds")


def _round_s(value: Any) -> float:
    return round(float(value), 3)


def _json_dump(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _governance_markers() -> dict[str, Any]:
    return {
        "execution_mode": "simulated_no_com",
        "evidence_source": "simulated_no_com",
        "not_real_acceptance_evidence": True,
        "not_a1_pass": True,
        "not_a2_pass": True,
        "not_v2_replacement": True,
        "real_primary_latest_refreshed": False,
        "not_real_primary_latest": True,
        "acceptance_level": "not_acceptance_simulated_no_com",
        "promotion_state": "blocked",
        "governance_statement": CONDITIONING_ONLY_GOVERNANCE_STATEMENT,
    }


def _resolve_config_relative(config_path: Path, value: Any) -> Path:
    candidate = Path(str(value or "")).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    return (config_path.parent / candidate).resolve()


def _default_output_dir(config_path: Path, raw_cfg: Mapping[str, Any], run_timestamp: str) -> Path:
    paths = _section(raw_cfg, "paths")
    configured_output = str(paths.get("output_dir") or "").strip()
    if configured_output:
        base = _resolve_config_relative(config_path, configured_output)
        output_root = base.parents[1] if len(base.parents) > 1 else base.parent
    else:
        output_root = (config_path.parents[1] / "output").resolve()
    return (
        output_root
        / "run001_conditioning_only"
        / "co2_only_skip0_no_write"
        / "simulated_no_com"
        / f"run_{run_timestamp}"
    )


def _pressure_config(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    workflow = _section(raw_cfg, "workflow")
    pressure = workflow.get("pressure") if isinstance(workflow.get("pressure"), Mapping) else {}
    return dict(pressure)


def _configured_real_ports(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    devices = _section(raw_cfg, "devices")
    ports: dict[str, Any] = {}
    for name in (
        "pressure_controller",
        "pressure_gauge",
        "dewpoint_meter",
        "humidity_generator",
        "temperature_chamber",
        "thermometer",
        "relay",
        "relay_8",
    ):
        device = devices.get(name)
        if isinstance(device, Mapping):
            ports[name] = {
                "enabled": _as_bool(device.get("enabled", True)),
                "port": str(device.get("port") or ""),
                "baud": device.get("baud"),
                "opened": False,
            }
    analyzers = []
    for item in devices.get("gas_analyzers") or []:
        if isinstance(item, Mapping):
            analyzers.append(
                {
                    "name": str(item.get("name") or ""),
                    "enabled": _as_bool(item.get("enabled", True)),
                    "port": str(item.get("port") or ""),
                    "device_id": str(item.get("device_id") or ""),
                    "baud": item.get("baud"),
                    "opened": False,
                }
            )
    if analyzers:
        ports["gas_analyzers"] = analyzers
    return ports


def _base_metrics(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    pressure = _pressure_config(raw_cfg)
    heartbeat_interval_s = _as_float(
        pressure.get("atmosphere_vent_heartbeat_interval_s", pressure.get("vent_hold_interval_s", 1.0)),
        1.0,
    )
    return {
        "conditioning_duration_s": 6.5,
        "vent_tick_count": 8,
        "vent_tick_avg_gap_s": 0.914,
        "vent_tick_max_gap_s": 1.0,
        "route_open_to_first_vent_s": 0.2,
        "last_vent_command_age_s": 0.1,
        "vent_heartbeat_interval_s": heartbeat_interval_s,
        "atmosphere_vent_max_gap_s": _as_float(pressure.get("atmosphere_vent_max_gap_s"), 3.0),
        "vent_heartbeat_gap_exceeded": False,
        "pressure_monitor_interval_s": _as_float(pressure.get("pressure_monitor_interval_s"), 0.5),
        "digital_gauge_latest_age_s": 0.1,
        "digital_gauge_sequence_progress": True,
        "conditioning_pressure_abort_hpa": _as_float(pressure.get("conditioning_pressure_abort_hpa"), 1150.0),
        "pressure_overlimit_seen": False,
        "pressure_overlimit_source": "",
        "pressure_overlimit_hpa": None,
        "conditioning_pressure_max_hpa": 1012.4,
        "conditioning_pressure_min_hpa": 1009.8,
        "stream_stale_seen": False,
        "fail_closed_before_vent_off": False,
        "vent_off_sent_at": None,
        "seal_command_sent": False,
        "high_pressure_mode_started": False,
        "sample_count": 0,
        "points_completed": 0,
    }


def _event(
    *,
    name: str,
    event_type: str,
    base_time: datetime,
    monotonic_s: float,
    run_start_monotonic_s: float,
    stage: str = "co2_route_conditioning_at_atmosphere",
    decision: str = "",
    route_state: Optional[Mapping[str, Any]] = None,
    pressure_hpa: Optional[float] = None,
    sample_count: Optional[int] = None,
) -> dict[str, Any]:
    record = {field: None for field in TIMING_EVENT_FIELDS}
    record.update(
        {
            "event_name": name,
            "event_type": event_type,
            "timestamp_local": _timestamp(base_time, monotonic_s - run_start_monotonic_s),
            "timestamp_monotonic_s": _round_s(monotonic_s),
            "elapsed_from_run_start_s": _round_s(monotonic_s - run_start_monotonic_s),
            "stage": stage,
            "point_index": 1,
            "target_pressure_hpa": 1100.0,
            "duration_s": None,
            "expected_max_s": None,
            "decision": decision or None,
            "route_state": dict(route_state or {}),
            "pressure_hpa": pressure_hpa,
            "sample_count": sample_count,
            "no_write_guard_active": True,
        }
    )
    return record


def _write_timing_trace(run_dir: Path, metrics: Mapping[str, Any], pressure_values: list[float]) -> list[dict[str, Any]]:
    base_time = datetime.now(timezone.utc)
    start = 100.0
    vent_times = [100.0, 100.4, 101.4, 102.4, 103.4, 104.4, 105.4, 106.4]
    events: list[dict[str, Any]] = [
        _event(
            name="co2_route_conditioning_start",
            event_type="start",
            base_time=base_time,
            monotonic_s=start,
            run_start_monotonic_s=start,
            route_state={
                "conditioning_only_probe": True,
                "atmosphere_vent_enabled": True,
                "route_open_before_atmosphere_confirmed": True,
                "vent_heartbeat_interval_s": metrics["vent_heartbeat_interval_s"],
                "atmosphere_vent_max_gap_s": metrics["atmosphere_vent_max_gap_s"],
                "pressure_monitor_interval_s": metrics["pressure_monitor_interval_s"],
                "conditioning_pressure_abort_hpa": metrics["conditioning_pressure_abort_hpa"],
            },
        ),
        _event(
            name="co2_route_open_start",
            event_type="start",
            base_time=base_time,
            monotonic_s=100.1,
            run_start_monotonic_s=start,
            stage="co2_route_open",
            route_state={"conditioning_only_probe": True, "vent_confirmed_before_route_open": True},
        ),
        _event(
            name="co2_route_open_end",
            event_type="end",
            base_time=base_time,
            monotonic_s=100.2,
            run_start_monotonic_s=start,
            stage="co2_route_open",
            decision="simulated_no_com",
            route_state={"conditioning_only_probe": True},
        ),
    ]
    for index, tick_time in enumerate(vent_times):
        pressure = pressure_values[min(index, len(pressure_values) - 1)]
        events.append(
            _event(
                name="co2_route_conditioning_vent_tick",
                event_type="tick",
                base_time=base_time,
                monotonic_s=tick_time,
                run_start_monotonic_s=start,
                decision="atmosphere_hold_confirmed",
                pressure_hpa=pressure,
                route_state={
                    "phase": "before_route_open" if index == 0 else "conditioning_hold",
                    "command_result": "simulated_no_com",
                    "output_state": 0,
                    "isolation_state": 1,
                    "vent_status_raw": 1,
                    "route_open_to_first_vent_s": metrics["route_open_to_first_vent_s"] if index == 1 else None,
                    "last_vent_command_age_s": metrics["last_vent_command_age_s"],
                    "digital_gauge_pressure_hpa": pressure,
                    "digital_gauge_latest_age_s": metrics["digital_gauge_latest_age_s"],
                    "digital_gauge_sequence_progress": True,
                    "pressure_overlimit_seen": False,
                    "stream_stale_seen": False,
                    "fail_closed_before_vent_off": False,
                },
            )
        )
    for index, pressure in enumerate(pressure_values):
        events.append(
            _event(
                name="co2_route_conditioning_pressure_sample",
                event_type="tick",
                base_time=base_time,
                monotonic_s=100.45 + index * float(metrics["pressure_monitor_interval_s"]),
                run_start_monotonic_s=start,
                decision="monitor_only_no_seal",
                pressure_hpa=pressure,
                route_state={
                    "pressure_sample_source": "digital_pressure_gauge_continuous",
                    "digital_gauge_latest_age_s": metrics["digital_gauge_latest_age_s"],
                    "digital_gauge_sequence_progress": True,
                    "pressure_overlimit_seen": False,
                    "stream_stale_seen": False,
                    "conditioning_pressure_abort_hpa": metrics["conditioning_pressure_abort_hpa"],
                },
            )
        )
    events.append(
        _event(
            name="co2_route_conditioning_end",
            event_type="end",
            base_time=base_time,
            monotonic_s=start + float(metrics["conditioning_duration_s"]),
            run_start_monotonic_s=start,
            decision="PASS",
            route_state={
                "conditioning_only_probe": True,
                "vent_heartbeat_gap_exceeded": False,
                "pressure_overlimit_seen": False,
                "stream_stale_seen": False,
                "digital_gauge_sequence_progress": True,
                "vent_off_sent_at": None,
                "seal_command_sent": False,
                "high_pressure_mode_started": False,
                "sample_count": 0,
                "points_completed": 0,
            },
        )
    )
    trace_path = run_dir / WORKFLOW_TIMING_TRACE_FILENAME
    trace_path.write_text("\n".join(json.dumps(event, ensure_ascii=False) for event in events) + "\n", encoding="utf-8")
    return events


def _route_row(
    *,
    action: str,
    result: str,
    actual: Mapping[str, Any],
    target: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    return {
        "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "action": action,
        "route": "co2",
        "point_index": 1,
        "target": dict(target or {}),
        "actual": dict(actual),
        "result": result,
    }


def _write_route_trace(run_dir: Path, metrics: Mapping[str, Any]) -> list[dict[str, Any]]:
    shared_actual = {
        "conditioning_only_probe": True,
        "real_com_opened": False,
        "vent_heartbeat_gap_exceeded": metrics["vent_heartbeat_gap_exceeded"],
        "pressure_overlimit_seen": metrics["pressure_overlimit_seen"],
        "pressure_overlimit_source": metrics["pressure_overlimit_source"],
        "conditioning_pressure_max_hpa": metrics["conditioning_pressure_max_hpa"],
        "stream_stale_seen": metrics["stream_stale_seen"],
        "digital_gauge_latest_age_s": metrics["digital_gauge_latest_age_s"],
        "digital_gauge_sequence_progress": metrics["digital_gauge_sequence_progress"],
        "vent_off_sent_at": metrics["vent_off_sent_at"],
        "seal_command_sent": metrics["seal_command_sent"],
        "high_pressure_mode_started": metrics["high_pressure_mode_started"],
        "sample_count": metrics["sample_count"],
        "points_completed": metrics["points_completed"],
    }
    rows = [
        _route_row(
            action="set_vent",
            result="simulated_no_com",
            target={"vent_on": True, "hold_atmosphere": True},
            actual={**shared_actual, "phase": "before_route_open", "vent_confirmed": True},
        ),
        _route_row(
            action="set_co2_valves",
            result="simulated_no_com",
            target={"route": "co2", "skip_co2_ppm": 0},
            actual={**shared_actual, "phase": "route_open", "route_opened": True},
        ),
        _route_row(
            action="set_vent",
            result="simulated_no_com",
            target={"vent_on": True, "hold_atmosphere": True},
            actual={
                **shared_actual,
                "phase": "after_route_open",
                "route_open_to_first_vent_s": metrics["route_open_to_first_vent_s"],
                "vent_confirmed": True,
            },
        ),
        _route_row(
            action="co2_route_conditioning_complete",
            result="PASS",
            actual={**shared_actual, "phase": "conditioning_complete"},
        ),
    ]
    (run_dir / "route_trace.jsonl").write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n",
        encoding="utf-8",
    )
    return rows


def _pressure_latency_row(
    *,
    run_id: str,
    pressure_hpa: float,
    sequence_id: int,
    pressure_port: str,
    metrics: Mapping[str, Any],
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    row = {field: "" for field in PRESSURE_READ_LATENCY_SAMPLE_FIELDS}
    row.update(
        {
            "run_id": run_id,
            "timestamp": now,
            "stage": "co2_route_conditioning_pressure_sample",
            "point_index": 1,
            "source": "digital_pressure_gauge_continuous",
            "pressure_hpa": pressure_hpa,
            "request_sent_at": now,
            "response_received_at": now,
            "request_sent_monotonic_s": 100.0 + sequence_id * float(metrics["pressure_monitor_interval_s"]),
            "response_received_monotonic_s": 100.02 + sequence_id * float(metrics["pressure_monitor_interval_s"]),
            "read_latency_s": 0.02,
            "sample_recorded_at": now,
            "sample_recorded_monotonic_s": 100.02 + sequence_id * float(metrics["pressure_monitor_interval_s"]),
            "sample_age_s": metrics["digital_gauge_latest_age_s"],
            "is_cached": True,
            "is_stale": False,
            "stale_threshold_s": 3.0,
            "serial_port": pressure_port,
            "command": "continuous_latest_frame",
            "raw_response": f"SIMULATED_P4 {pressure_hpa:.3f}",
            "parse_ok": True,
            "sequence_id": sequence_id,
            "usable_for_abort": True,
            "usable_for_ready": False,
            "usable_for_seal": False,
            "primary_pressure_source": "digital_pressure_gauge",
            "pressure_source_used_for_decision": "digital_pressure_gauge_continuous",
            "source_selection_reason": "conditioning_only_no_com_simulated_latest_frame",
            "source_disagreement_warning": False,
            "digital_gauge_mode": "continuous",
            "digital_gauge_continuous_active": True,
            "latest_frame_age_s": metrics["digital_gauge_latest_age_s"],
            "latest_frame_interval_s": metrics["pressure_monitor_interval_s"],
            "latest_frame_sequence_id": sequence_id,
            "critical_window_blocking_query_count": 0,
            "critical_window_blocking_query_total_s": 0.0,
            "critical_window_uses_latest_frame": True,
            "critical_window_uses_query": False,
            "conditioning_pressure_abort_hpa": metrics["conditioning_pressure_abort_hpa"],
            "pressure_overlimit_seen": False,
            "pressure_overlimit_source": "",
            "pressure_overlimit_hpa": "",
            "vent_heartbeat_gap_exceeded": False,
            "digital_gauge_sequence_progress": True,
            "digital_gauge_latest_age_s": metrics["digital_gauge_latest_age_s"],
            "stream_stale": False,
            "fail_closed_before_vent_off": False,
        }
    )
    return row


def _write_pressure_latency_samples(
    run_dir: Path,
    *,
    run_id: str,
    pressure_port: str,
    metrics: Mapping[str, Any],
    pressure_values: list[float],
) -> list[dict[str, Any]]:
    rows = [
        _pressure_latency_row(
            run_id=run_id,
            pressure_hpa=pressure,
            sequence_id=index + 1,
            pressure_port=pressure_port,
            metrics=metrics,
        )
        for index, pressure in enumerate(pressure_values)
    ]
    with (run_dir / "pressure_read_latency_samples.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PRESSURE_READ_LATENCY_SAMPLE_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return rows


def _no_write_artifact(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    guard = build_no_write_guard_from_raw_config(raw_cfg)
    artifact = guard.to_artifact() if guard is not None else {
        "guard_enabled": _as_bool(_section(raw_cfg, "run001_a2").get("no_write")),
        "scope": CONDITIONING_ONLY_SCOPE,
        "attempted_write_count": 0,
        "blocked_write_events": [],
        "identity_write_command_sent": False,
        "persistent_write_command_sent": False,
        "final_decision": "PASS",
    }
    artifact.update(
        {
            "conditioning_only_probe": True,
            "real_com_opened": False,
            **_governance_markers(),
            "calibration_write_command_sent": False,
            "senco_write_command_sent": False,
        }
    )
    return artifact


def write_conditioning_only_probe_artifacts(
    config_path: str | Path,
    *,
    output_dir: Optional[str | Path] = None,
    run_timestamp: Optional[str] = None,
) -> dict[str, Any]:
    resolved_config = Path(config_path).expanduser().resolve()
    raw_cfg = _read_json(resolved_config)
    timestamp = run_timestamp or datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    run_dir = Path(output_dir).expanduser().resolve() if output_dir else _default_output_dir(resolved_config, raw_cfg, timestamp)
    run_dir.mkdir(parents=True, exist_ok=True)

    run_id = f"Run-001/conditioning-only/{timestamp}"
    metrics = _base_metrics(raw_cfg)
    pressure_values = [1009.8, 1010.0, 1010.4, 1010.9, 1011.4, 1011.8, 1012.1, 1012.4]
    ports = _configured_real_ports(raw_cfg)
    pressure_port = str((_section(_section(raw_cfg, "devices"), "pressure_gauge")).get("port") or "COM30")
    no_write = _no_write_artifact(raw_cfg)

    timing_events = _write_timing_trace(run_dir, metrics, pressure_values)
    route_trace = _write_route_trace(run_dir, metrics)
    latency_rows = _write_pressure_latency_samples(
        run_dir,
        run_id=run_id,
        pressure_port=pressure_port,
        metrics=metrics,
        pressure_values=pressure_values,
    )

    artifact_paths = {
        "summary": str(run_dir / "summary.json"),
        "route_trace": str(run_dir / "route_trace.jsonl"),
        "co2_route_conditioning_evidence": str(run_dir / "co2_route_conditioning_evidence.json"),
        "pressure_read_latency_samples": str(run_dir / "pressure_read_latency_samples.csv"),
        "workflow_timing_trace": str(run_dir / WORKFLOW_TIMING_TRACE_FILENAME),
        "workflow_timing_summary": str(run_dir / WORKFLOW_TIMING_SUMMARY_FILENAME),
        "no_write_guard": str(run_dir / "no_write_guard.json"),
        "run_manifest": str(run_dir / "run_manifest.json"),
    }
    evidence = {
        "schema_version": CONDITIONING_ONLY_SCHEMA_VERSION,
        "run_id": run_id,
        "conditioning_only_probe": True,
        **_governance_markers(),
        "real_probe_executed": False,
        "real_com_opened": False,
        "route": "co2",
        "skip_co2_ppm": 0,
        "temperature_c": 20.0,
        "conditioning_decision": "PASS",
        "route_open_before_atmosphere_confirmed": True,
        "vent_command_before_route_open": True,
        "vent_command_after_route_open": True,
        "fail_closed_guard_armed": True,
        "fail_closed_blocks_vent_off_seal_high_pressure_sample": True,
        "pressure_max_during_conditioning_hpa": metrics["conditioning_pressure_max_hpa"],
        "pressure_min_during_conditioning_hpa": metrics["conditioning_pressure_min_hpa"],
        **metrics,
        "pressure_read_latency_sample_count": len(latency_rows),
        "workflow_timing_event_count": len(timing_events),
        "route_trace_event_count": len(route_trace),
    }
    timing_summary = {
        "schema_version": CONDITIONING_ONLY_SCHEMA_VERSION,
        "run_id": run_id,
        "conditioning_only_probe": True,
        **_governance_markers(),
        "co2_route_conditioning_duration_s": metrics["conditioning_duration_s"],
        "co2_route_conditioning_vent_tick_count": metrics["vent_tick_count"],
        "co2_route_conditioning_vent_tick_avg_gap_s": metrics["vent_tick_avg_gap_s"],
        "co2_route_conditioning_vent_tick_max_gap_s": metrics["vent_tick_max_gap_s"],
        "route_open_to_first_vent_s": metrics["route_open_to_first_vent_s"],
        "co2_route_conditioning_vent_heartbeat_gap_exceeded": metrics["vent_heartbeat_gap_exceeded"],
        "co2_route_conditioning_pressure_max_hpa": metrics["conditioning_pressure_max_hpa"],
        "co2_route_conditioning_pressure_min_hpa": metrics["conditioning_pressure_min_hpa"],
        "pressure_overlimit_seen": metrics["pressure_overlimit_seen"],
        "stream_stale_seen": metrics["stream_stale_seen"],
        "digital_gauge_sequence_progress": metrics["digital_gauge_sequence_progress"],
        "vent_off_sent_at": metrics["vent_off_sent_at"],
        "seal_command_sent": metrics["seal_command_sent"],
        "high_pressure_mode_started": metrics["high_pressure_mode_started"],
        "sample_count": metrics["sample_count"],
        "points_completed": metrics["points_completed"],
    }
    summary = {
        "schema_version": CONDITIONING_ONLY_SCHEMA_VERSION,
        "run_id": run_id,
        "probe_scope": CONDITIONING_ONLY_SCOPE,
        "conditioning_only_probe": True,
        "config_path": str(resolved_config),
        "output_dir": str(run_dir),
        **_governance_markers(),
        "real_probe_executed": False,
        "real_com_opened": False,
        "co2_only": True,
        "skip0": True,
        "single_route": True,
        "single_temperature": True,
        "route_open_and_conditioning_only": True,
        "h2o_enabled": False,
        "full_group_enabled": False,
        "formal_real_acceptance": False,
        "refresh_real_primary_latest": False,
        "final_decision": CONDITIONING_ONLY_FINAL_DECISION,
        "conditioning_only_decision": "PASS",
        "configured_real_ports": ports,
        "artifact_paths": artifact_paths,
        "attempted_write_count": no_write.get("attempted_write_count", 0),
        "identity_write_command_sent": bool(no_write.get("identity_write_command_sent", False)),
        "calibration_write_command_sent": False,
        "senco_write_command_sent": False,
        **metrics,
        "pressure_read_latency_sample_count": len(latency_rows),
        "route_trace_event_count": len(route_trace),
        "workflow_timing_event_count": len(timing_events),
    }
    manifest = {
        "schema_version": CONDITIONING_ONLY_SCHEMA_VERSION,
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "config_path": str(resolved_config),
        "output_dir": str(run_dir),
        "artifact_paths": artifact_paths,
        "real_probe_executed": False,
        "real_com_opened": False,
        **_governance_markers(),
        "final_decision": CONDITIONING_ONLY_FINAL_DECISION,
    }

    _json_dump(run_dir / "co2_route_conditioning_evidence.json", evidence)
    _json_dump(run_dir / WORKFLOW_TIMING_SUMMARY_FILENAME, timing_summary)
    _json_dump(run_dir / "no_write_guard.json", no_write)
    _json_dump(run_dir / "run_manifest.json", manifest)
    _json_dump(run_dir / "summary.json", summary)
    return summary
