from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time
from typing import Any, Callable, Mapping, Optional

from gas_calibrator.devices.gas_analyzer import GasAnalyzer
from gas_calibrator.devices.pace5000 import Pace5000
from gas_calibrator.devices.paroscientific import ParoscientificGauge
from gas_calibrator.devices.relay import RelayController
from gas_calibrator.devices.thermometer import Thermometer
from gas_calibrator.v2.core.run001_r0_1_reference_read_probe import (
    _default_chamber_client_factory,
    read_temperature_chamber_read_only,
)


R1_SCHEMA_VERSION = "v2.run001.r1_conditioning_only_probe.1"
R1_ENV_VAR = "GAS_CAL_V2_R1_CONDITIONING_ONLY_REAL_COM"
R1_ENV_VALUE = "1"
R1_CLI_FLAG = "--allow-v2-r1-conditioning-only-real-com"
R1_EVIDENCE_MARKERS = {
    "evidence_source": "real_probe_r1_conditioning_only",
    "acceptance_level": "engineering_probe_only",
    "not_real_acceptance_evidence": True,
    "promotion_state": "blocked",
    "real_primary_latest_refresh": False,
}
R1_REQUIRED_OPERATOR_FIELDS = (
    "operator_name",
    "timestamp",
    "branch",
    "HEAD",
    "config_path",
    "r0_full_query_only_output_dir",
    "port_manifest",
    "explicit_acknowledgement",
)
R1_REQUIRED_TRUE_ACKS = (
    "only_r1_conditioning_only",
    "co2_only",
    "skip0",
    "single_route",
    "single_temperature",
    "no_write",
    "no_pressure_setpoint",
    "no_vent_off",
    "no_seal",
    "no_high_pressure",
    "no_sample",
    "no_id_write",
    "no_senco_write",
    "no_calibration_write",
    "no_chamber_sv_write",
    "no_chamber_set_temperature",
    "no_chamber_start",
    "no_chamber_stop",
    "v1_fallback_required",
    "not_real_acceptance",
    "engineering_probe_only",
    "do_not_refresh_real_primary_latest",
)
R1_REQUIRED_FALSE_ACKS = ("real_primary_latest_refresh",)
R1_SAFETY_ASSERTION_DEFAULTS = {
    "attempted_write_count": 0,
    "any_write_command_sent": False,
    "pressure_setpoint_command_sent": False,
    "vent_off_command_sent": False,
    "seal_command_sent": False,
    "high_pressure_command_sent": False,
    "sample_started": False,
    "sample_count": 0,
    "points_completed": 0,
    "mode_switch_command_sent": False,
    "identity_write_command_sent": False,
    "senco_write_command_sent": False,
    "calibration_write_command_sent": False,
    "chamber_write_register_command_sent": False,
    "chamber_set_temperature_command_sent": False,
    "chamber_start_command_sent": False,
    "chamber_stop_command_sent": False,
    "real_primary_latest_refresh": False,
}
R1_LATENCY_BREAKDOWN_FIELDS = (
    "component",
    "sequence_id",
    "duration_ms",
    "start_event",
    "end_event",
    "scope",
    "stage",
    "action",
    "relay",
    "channel",
)


@dataclass(frozen=True)
class R1Admission:
    approved: bool
    reasons: tuple[str, ...]
    evidence: dict[str, Any]
    operator_confirmation: dict[str, Any]
    operator_validation: dict[str, Any]
    r0_summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "approved": self.approved,
            "reasons": list(self.reasons),
            "evidence": dict(self.evidence),
            "operator_confirmation": dict(self.operator_confirmation),
            "operator_validation": dict(self.operator_validation),
            "r0_summary": dict(self.r0_summary),
        }


def load_json_mapping(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"JSON payload must be an object: {path}")
    return dict(payload)


def _json_dump(path: Path, payload: Mapping[str, Any] | list[dict[str, Any]]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _jsonl_dump(path: Path, rows: list[Mapping[str, Any]]) -> None:
    path.write_text(
        "\n".join(json.dumps(dict(row), ensure_ascii=False) for row in rows) + ("\n" if rows else ""),
        encoding="utf-8",
    )


def _csv_dump(path: Path, rows: list[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(R1_LATENCY_BREAKDOWN_FIELDS), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _perf_ns() -> int:
    return time.perf_counter_ns()


def _timing_mark(
    rows: list[dict[str, Any]],
    event_name: str,
    *,
    perf_counter_ns: Optional[int] = None,
    **details: Any,
) -> int:
    now_ns = _perf_ns() if perf_counter_ns is None else int(perf_counter_ns)
    rows.append(
        {
            "timestamp": _now(),
            "event_name": str(event_name),
            "perf_counter_ns": now_ns,
            **details,
        }
    )
    return now_ns


def _ms_between(start_ns: Optional[int], end_ns: Optional[int]) -> Optional[float]:
    if start_ns is None or end_ns is None:
        return None
    return round((int(end_ns) - int(start_ns)) / 1_000_000.0, 3)


def _first_event(rows: list[Mapping[str, Any]], name: str) -> Optional[Mapping[str, Any]]:
    return next((row for row in rows if row.get("event_name") == name), None)


def _last_event(rows: list[Mapping[str, Any]], name: str) -> Optional[Mapping[str, Any]]:
    return next((row for row in reversed(rows) if row.get("event_name") == name), None)


def _event_ns(rows: list[Mapping[str, Any]], name: str, *, last: bool = False) -> Optional[int]:
    row = _last_event(rows, name) if last else _first_event(rows, name)
    if row is None:
        return None
    try:
        return int(row.get("perf_counter_ns"))
    except Exception:
        return None


def _paired_latency_rows(
    rows: list[Mapping[str, Any]],
    *,
    start_name: str,
    end_name: str,
    component: str,
) -> list[dict[str, Any]]:
    starts = [row for row in rows if row.get("event_name") == start_name]
    ends = [row for row in rows if row.get("event_name") == end_name]
    out: list[dict[str, Any]] = []
    for index, (start, end) in enumerate(zip(starts, ends), start=1):
        out.append(
            {
                "component": component,
                "sequence_id": start.get("sequence_id", index),
                "duration_ms": _ms_between(
                    int(start.get("perf_counter_ns")),
                    int(end.get("perf_counter_ns")),
                ),
                "start_event": start_name,
                "end_event": end_name,
                "scope": start.get("scope", end.get("scope", "")),
                "stage": start.get("stage", end.get("stage", "")),
                "action": start.get("action", end.get("action", "")),
                "relay": start.get("relay", end.get("relay", "")),
                "channel": start.get("channel", end.get("channel", "")),
            }
        )
    return out


def _duration_values(rows: list[Mapping[str, Any]], component: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        if row.get("component") != component:
            continue
        value = _as_float(row.get("duration_ms"))
        if value is not None:
            values.append(float(value))
    return values


def _build_latency_breakdown(rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    latency_rows: list[dict[str, Any]] = []
    latency_rows.extend(
        _paired_latency_rows(
            rows,
            start_name="each_relay_action_start",
            end_name="each_relay_action_end",
            component="relay_action",
        )
    )
    latency_rows.extend(
        _paired_latency_rows(
            rows,
            start_name="each_vent_heartbeat_emit_start",
            end_name="each_vent_heartbeat_emit_end",
            component="vent_heartbeat_emit",
        )
    )
    latency_rows.extend(
        _paired_latency_rows(
            rows,
            start_name="pressure_gauge_read_start",
            end_name="pressure_gauge_read_end",
            component="pressure_gauge_read",
        )
    )
    latency_rows.extend(
        _paired_latency_rows(
            rows,
            start_name="temperature_chamber_read_start",
            end_name="temperature_chamber_read_end",
            component="temperature_chamber_read",
        )
    )
    latency_rows.extend(
        _paired_latency_rows(
            rows,
            start_name="thermometer_read_start",
            end_name="thermometer_read_end",
            component="thermometer_read",
        )
    )
    latency_rows.extend(
        _paired_latency_rows(
            rows,
            start_name="analyzer_read_start",
            end_name="analyzer_read_end",
            component="analyzer_read",
        )
    )
    latency_rows.extend(
        _paired_latency_rows(
            rows,
            start_name="evidence_write_start",
            end_name="evidence_write_end",
            component="evidence_write",
        )
    )
    return latency_rows


def _build_timing_breakdown(
    timing_rows: list[Mapping[str, Any]],
    *,
    route_open_to_first_vent_ms: Optional[float],
    max_vent_heartbeat_gap_ms: float,
    vent_heartbeat_count: int,
    relay_route_action_count: int,
    route_open_to_first_vent_threshold_ms: float,
) -> dict[str, Any]:
    latency_rows = _build_latency_breakdown(timing_rows)
    relay_durations = _duration_values(latency_rows, "relay_action")
    pressure_durations = _duration_values(latency_rows, "pressure_gauge_read")
    evidence_durations = _duration_values(latency_rows, "evidence_write")

    route_start_ns = _event_ns(timing_rows, "route_conditioning_start")
    first_route_start_ns = _event_ns(timing_rows, "first_route_action_start")
    first_route_end_ns = _event_ns(timing_rows, "first_route_action_end")
    last_route_start_ns = _event_ns(timing_rows, "last_route_action_start")
    last_route_end_ns = _event_ns(timing_rows, "last_route_action_end")
    route_completed_ns = _event_ns(timing_rows, "route_open_completed")
    first_vent_start_ns = _event_ns(timing_rows, "first_vent_heartbeat_emit_start")
    first_vent_end_ns = _event_ns(timing_rows, "first_vent_heartbeat_emit_end")

    route_start_to_first_vent_ms = _ms_between(route_start_ns, first_vent_start_ns)
    first_route_action_start_to_first_vent_ms = _ms_between(first_route_start_ns, first_vent_start_ns)
    first_route_action_end_to_first_vent_ms = _ms_between(first_route_end_ns, first_vent_start_ns)
    last_route_action_start_to_first_vent_ms = _ms_between(last_route_start_ns, first_vent_start_ns)
    last_route_action_end_to_first_vent_ms = _ms_between(last_route_end_ns, first_vent_start_ns)
    route_open_completed_to_first_vent_ms = _ms_between(route_completed_ns, first_vent_start_ns)
    route_action_sequence_duration_ms = _ms_between(first_route_start_ns, last_route_end_ns)
    first_vent_emit_duration_ms = _ms_between(first_vent_start_ns, first_vent_end_ns)
    pressure_read_latency_ms = pressure_durations[0] if pressure_durations else None
    max_pressure_read_latency_ms = max(pressure_durations) if pressure_durations else None
    max_relay_action_duration_ms = max(relay_durations) if relay_durations else None
    evidence_write_latency_ms = evidence_durations[-1] if evidence_durations else None

    diagnostic_decision = "NOT_APPLICABLE"
    suspected_root_cause = ""
    critical_path_suspect = "unknown"
    old_metric_failed = (
        route_open_to_first_vent_ms is not None
        and route_open_to_first_vent_ms > float(route_open_to_first_vent_threshold_ms)
    )
    if old_metric_failed:
        if (
            route_open_completed_to_first_vent_ms is not None
            and last_route_action_end_to_first_vent_ms is not None
            and route_open_completed_to_first_vent_ms <= 300.0
            and last_route_action_end_to_first_vent_ms <= 300.0
        ):
            diagnostic_decision = "ANCHOR_REVIEW_REQUIRED"
            suspected_root_cause = "threshold_anchor_too_early"
            critical_path_suspect = "threshold_anchor_too_early"
        elif route_open_completed_to_first_vent_ms is not None and route_open_completed_to_first_vent_ms > 1000.0:
            diagnostic_decision = "HEARTBEAT_TOO_LATE"
            suspected_root_cause = "heartbeat_scheduler_late_start"
            critical_path_suspect = "heartbeat_scheduler_late_start"
        elif (
            (max_relay_action_duration_ms is not None and max_relay_action_duration_ms > 500.0)
            or (route_action_sequence_duration_ms is not None and route_action_sequence_duration_ms > 1000.0)
        ):
            diagnostic_decision = "RELAY_ACTION_LATENCY_DOMINANT"
            suspected_root_cause = "route_action_duration"
            critical_path_suspect = "route_action_duration"
        elif pressure_read_latency_ms is not None and pressure_read_latency_ms > 1000.0:
            diagnostic_decision = "PRESSURE_READ_LATENCY_DOMINANT"
            suspected_root_cause = "pressure_read_latency"
            critical_path_suspect = "pressure_read_latency"
        elif evidence_write_latency_ms is not None and evidence_write_latency_ms > 1000.0:
            suspected_root_cause = "evidence_write_latency"
            critical_path_suspect = "evidence_write_latency"

    return {
        "route_start_to_first_vent_ms": route_start_to_first_vent_ms,
        "first_route_action_start_to_first_vent_ms": first_route_action_start_to_first_vent_ms,
        "first_route_action_end_to_first_vent_ms": first_route_action_end_to_first_vent_ms,
        "last_route_action_start_to_first_vent_ms": last_route_action_start_to_first_vent_ms,
        "last_route_action_end_to_first_vent_ms": last_route_action_end_to_first_vent_ms,
        "route_open_completed_to_first_vent_ms": route_open_completed_to_first_vent_ms,
        "route_action_sequence_duration_ms": route_action_sequence_duration_ms,
        "first_vent_emit_duration_ms": first_vent_emit_duration_ms,
        "max_vent_heartbeat_gap_ms": max_vent_heartbeat_gap_ms,
        "vent_heartbeat_count": int(vent_heartbeat_count),
        "pressure_read_latency_ms": pressure_read_latency_ms,
        "max_pressure_read_latency_ms": max_pressure_read_latency_ms,
        "relay_action_count": int(relay_route_action_count),
        "relay_action_durations_ms": relay_durations,
        "max_relay_action_duration_ms": max_relay_action_duration_ms,
        "evidence_write_latency_ms": evidence_write_latency_ms,
        "critical_path_suspect": critical_path_suspect,
        "diagnostic_decision": diagnostic_decision,
        "suspected_root_cause": suspected_root_cause,
        "latency_breakdown_rows": latency_rows,
    }


def _as_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None or value == "":
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return None


def _as_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return default


def _as_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return default


def _section(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    candidate = value.get(name)
    return dict(candidate) if isinstance(candidate, Mapping) else {}


def _path_value(raw_cfg: Mapping[str, Any], dotted_path: str) -> Any:
    current: Any = raw_cfg
    for part in dotted_path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current.get(part)
    return current


def _first_value(raw_cfg: Mapping[str, Any], paths: tuple[str, ...]) -> Any:
    for path in paths:
        value = _path_value(raw_cfg, path)
        if value is not None:
            return value
    return None


def _truthy(raw_cfg: Mapping[str, Any], paths: tuple[str, ...]) -> bool:
    return _as_bool(_first_value(raw_cfg, paths)) is True


def _explicit_false(raw_cfg: Mapping[str, Any], paths: tuple[str, ...]) -> bool:
    return _as_bool(_first_value(raw_cfg, paths)) is False


def _r1_cfg(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    for name in ("r1_conditioning_only", "run001_r1", "r1"):
        cfg = _section(raw_cfg, name)
        if cfg:
            return cfg
    return {}


def _scope(raw_cfg: Mapping[str, Any]) -> str:
    return str(
        _first_value(
            raw_cfg,
            (
                "scope",
                "r1_conditioning_only.scope",
                "run001_r1.scope",
                "r1.scope",
            ),
        )
        or ""
    ).strip().lower()


def _skip0_only(raw_cfg: Mapping[str, Any]) -> bool:
    if _truthy(raw_cfg, ("skip0", "r1_conditioning_only.skip0", "run001_r1.skip0", "r1.skip0")):
        return True
    value = _first_value(
        raw_cfg,
        (
            "skip_co2_ppm",
            "workflow.skip_co2_ppm",
            "r1_conditioning_only.skip_co2_ppm",
            "run001_r1.skip_co2_ppm",
            "r1.skip_co2_ppm",
        ),
    )
    if isinstance(value, list):
        return [int(float(item)) for item in value if str(item).strip() != ""] == [0]
    return str(value).strip() in {"0", "0.0", "[0]"}


def _single_temperature(raw_cfg: Mapping[str, Any]) -> bool:
    if _truthy(
        raw_cfg,
        (
            "single_temperature",
            "single_temperature_group",
            "r1_conditioning_only.single_temperature",
            "run001_r1.single_temperature",
            "r1.single_temperature",
        ),
    ):
        return True
    value = _first_value(raw_cfg, ("selected_temps_c", "workflow.selected_temps_c"))
    return isinstance(value, list) and len(value) == 1


def _h2o_disabled(raw_cfg: Mapping[str, Any]) -> bool:
    if _truthy(raw_cfg, ("h2o_enabled", "r1_conditioning_only.h2o_enabled", "run001_r1.h2o_enabled")):
        return False
    if _explicit_false(raw_cfg, ("h2o_enabled", "r1_conditioning_only.h2o_enabled", "run001_r1.h2o_enabled")):
        return True
    dewpoint = _as_bool(_path_value(raw_cfg, "devices.dewpoint_meter.enabled"))
    humidity = _as_bool(_path_value(raw_cfg, "devices.humidity_generator.enabled"))
    route_mode = str(_path_value(raw_cfg, "workflow.route_mode") or "").strip().lower()
    return dewpoint is False and humidity is False and route_mode == "co2_only"


def _r0_output_dir(raw_cfg: Mapping[str, Any]) -> str:
    return str(
        _first_value(
            raw_cfg,
            (
                "r0_full_query_only_output_dir",
                "r1_conditioning_only.r0_full_query_only_output_dir",
                "run001_r1.r0_full_query_only_output_dir",
                "r1.r0_full_query_only_output_dir",
            ),
        )
        or ""
    )


def _load_r0_summary(output_dir: str | Path) -> tuple[dict[str, Any], list[str]]:
    if not str(output_dir or "").strip():
        return {}, ["missing_r0_full_query_only_output_dir"]
    run_dir = Path(output_dir).expanduser()
    summary_path = run_dir / "summary.json"
    if not summary_path.exists():
        return {}, ["missing_r0_full_query_only_summary"]
    try:
        summary = load_json_mapping(summary_path)
    except Exception as exc:
        return {}, [f"invalid_r0_full_query_only_summary:{exc}"]
    reasons: list[str] = []
    if summary.get("final_decision") != "PASS":
        reasons.append("r0_full_query_only_not_pass")
    if summary.get("not_real_acceptance_evidence") is not True:
        reasons.append("r0_full_query_only_missing_not_real_acceptance_marker")
    if summary.get("attempted_write_count") != 0:
        reasons.append("r0_full_query_only_attempted_write_nonzero")
    if summary.get("any_write_command_sent") is not False:
        reasons.append("r0_full_query_only_any_write_not_false")
    return summary, reasons


def _validate_operator_confirmation(
    path: Optional[str | Path],
    *,
    expected_branch: str = "",
    expected_head: str = "",
    expected_config_path: str = "",
    expected_r0_output_dir: str = "",
) -> tuple[dict[str, Any], dict[str, Any]]:
    errors: list[str] = []
    if not path:
        return {}, {"valid": False, "errors": ["missing_operator_confirmation_json"]}
    confirmation_path = Path(path).expanduser()
    if not confirmation_path.exists():
        return {}, {"valid": False, "errors": ["missing_operator_confirmation_json"]}
    try:
        payload = load_json_mapping(confirmation_path)
    except Exception as exc:
        return {}, {"valid": False, "errors": [f"invalid_operator_confirmation_json:{exc}"]}
    for field in R1_REQUIRED_OPERATOR_FIELDS:
        if payload.get(field) in (None, ""):
            errors.append(f"operator_confirmation_missing_{field}")
    ack = payload.get("explicit_acknowledgement")
    if not isinstance(ack, Mapping):
        errors.append("operator_confirmation_missing_explicit_acknowledgement")
        ack = {}
    for key in R1_REQUIRED_TRUE_ACKS:
        if _as_bool(ack.get(key)) is not True:
            errors.append(f"operator_ack_missing_{key}")
    for key in R1_REQUIRED_FALSE_ACKS:
        if _as_bool(ack.get(key)) is not False:
            errors.append(f"operator_ack_not_false_{key}")
    if expected_branch and str(payload.get("branch") or "") != expected_branch:
        errors.append("operator_confirmation_branch_mismatch")
    if expected_head and str(payload.get("HEAD") or "") != expected_head:
        errors.append("operator_confirmation_head_mismatch")
    if expected_config_path:
        payload_config_path = str(payload.get("config_path") or "")
        if not payload_config_path or Path(payload_config_path).resolve() != Path(expected_config_path).resolve():
            errors.append("operator_confirmation_config_path_mismatch")
    if expected_r0_output_dir:
        payload_r0 = str(payload.get("r0_full_query_only_output_dir") or "")
        if not payload_r0 or Path(payload_r0).resolve() != Path(expected_r0_output_dir).resolve():
            errors.append("operator_confirmation_r0_output_dir_mismatch")
    return payload, {
        "valid": not errors,
        "errors": errors,
        "path": str(confirmation_path.resolve()),
    }


def evaluate_r1_conditioning_only_gate(
    raw_cfg: Mapping[str, Any],
    *,
    cli_allow: bool = False,
    env: Optional[Mapping[str, str]] = None,
    operator_confirmation_path: Optional[str | Path] = None,
    branch: str = "",
    head: str = "",
    config_path: str = "",
) -> R1Admission:
    env_map = os.environ if env is None else env
    reasons: list[str] = []
    if not cli_allow:
        reasons.append("missing_cli_flag_allow_v2_r1_conditioning_only_real_com")
    if str(env_map.get(R1_ENV_VAR, "")).strip() != R1_ENV_VALUE:
        reasons.append("missing_env_gas_cal_v2_r1_conditioning_only_real_com")

    r0_dir = _r0_output_dir(raw_cfg)
    r0_summary, r0_reasons = _load_r0_summary(r0_dir)
    reasons.extend(r0_reasons)

    operator_payload, operator_validation = _validate_operator_confirmation(
        operator_confirmation_path,
        expected_branch=branch,
        expected_head=head,
        expected_config_path=config_path,
        expected_r0_output_dir=r0_dir,
    )
    reasons.extend(str(item) for item in operator_validation.get("errors", []))

    scope = _scope(raw_cfg)
    if scope not in {"r1_conditioning_only", "conditioning_only"}:
        reasons.append("config_scope_not_r1_conditioning_only")
    if not _truthy(raw_cfg, ("co2_only", "r1_conditioning_only.co2_only", "run001_r1.co2_only", "r1.co2_only")):
        reasons.append("config_not_co2_only")
    if not _skip0_only(raw_cfg):
        reasons.append("config_not_skip0")
    if not _truthy(raw_cfg, ("single_route", "r1_conditioning_only.single_route", "run001_r1.single_route", "r1.single_route")):
        reasons.append("config_not_single_route")
    if not _single_temperature(raw_cfg):
        reasons.append("config_not_single_temperature")
    if not _truthy(raw_cfg, ("no_write", "r1_conditioning_only.no_write", "run001_r1.no_write", "r1.no_write")):
        reasons.append("config_no_write_not_true")
    if not _h2o_disabled(raw_cfg):
        reasons.append("config_h2o_not_disabled")

    false_required = {
        "full_group_enabled": ("full_group_enabled", "r1_conditioning_only.full_group_enabled", "run001_r1.full_group_enabled"),
        "a1r_enabled": ("a1r_enabled", "r1_conditioning_only.a1r_enabled", "run001_r1.a1r_enabled"),
        "a2_enabled": ("a2_enabled", "r1_conditioning_only.a2_enabled", "run001_r1.a2_enabled"),
        "a3_enabled": ("a3_enabled", "r1_conditioning_only.a3_enabled", "run001_r1.a3_enabled"),
        "pressure_setpoint_enabled": (
            "pressure_setpoint_enabled",
            "r1_conditioning_only.pressure_setpoint_enabled",
            "run001_r1.pressure_setpoint_enabled",
        ),
        "vent_off_enabled": ("vent_off_enabled", "r1_conditioning_only.vent_off_enabled", "run001_r1.vent_off_enabled"),
        "seal_enabled": ("seal_enabled", "r1_conditioning_only.seal_enabled", "run001_r1.seal_enabled"),
        "high_pressure_enabled": (
            "high_pressure_enabled",
            "r1_conditioning_only.high_pressure_enabled",
            "run001_r1.high_pressure_enabled",
        ),
        "sample_enabled": ("sample_enabled", "r1_conditioning_only.sample_enabled", "run001_r1.sample_enabled"),
        "mode_switch_enabled": ("mode_switch_enabled", "r1_conditioning_only.mode_switch_enabled", "run001_r1.mode_switch_enabled"),
        "analyzer_id_write_enabled": (
            "analyzer_id_write_enabled",
            "r1_conditioning_only.analyzer_id_write_enabled",
            "run001_r1.analyzer_id_write_enabled",
        ),
        "senco_write_enabled": ("senco_write_enabled", "r1_conditioning_only.senco_write_enabled", "run001_r1.senco_write_enabled"),
        "calibration_write_enabled": (
            "calibration_write_enabled",
            "r1_conditioning_only.calibration_write_enabled",
            "run001_r1.calibration_write_enabled",
        ),
        "chamber_set_temperature_enabled": (
            "chamber_set_temperature_enabled",
            "r1_conditioning_only.chamber_set_temperature_enabled",
            "run001_r1.chamber_set_temperature_enabled",
        ),
        "chamber_start_enabled": ("chamber_start_enabled", "r1_conditioning_only.chamber_start_enabled", "run001_r1.chamber_start_enabled"),
        "chamber_stop_enabled": ("chamber_stop_enabled", "r1_conditioning_only.chamber_stop_enabled", "run001_r1.chamber_stop_enabled"),
        "real_primary_latest_refresh": (
            "real_primary_latest_refresh",
            "r1_conditioning_only.real_primary_latest_refresh",
            "run001_r1.real_primary_latest_refresh",
        ),
    }
    for name, paths in false_required.items():
        if not _explicit_false(raw_cfg, paths):
            reasons.append(f"config_{name}_not_disabled")

    reasons = list(dict.fromkeys(reasons))
    approved = not reasons
    evidence = {
        **R1_EVIDENCE_MARKERS,
        "admission_approved": approved,
        "r0_full_query_only_prereq_pass": bool(r0_summary.get("final_decision") == "PASS" and not r0_reasons),
        "r0_full_query_only_output_dir": r0_dir,
        "operator_confirmation_recorded": bool(operator_payload),
        "operator_confirmation_valid": bool(operator_validation.get("valid")),
        "r1_conditioning_only_executed": False,
        "real_com_opened": False,
        "real_probe_executed": False,
        "a1r_allowed": False,
        "a2_allowed": False,
        "a3_allowed": False,
        **R1_SAFETY_ASSERTION_DEFAULTS,
        "rejection_reasons": reasons,
    }
    return R1Admission(
        approved=approved,
        reasons=tuple(reasons),
        evidence=evidence,
        operator_confirmation=operator_payload,
        operator_validation=operator_validation,
        r0_summary=r0_summary,
    )


def _default_output_dir() -> Path:
    timestamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M")
    return Path(f"D:/gas_calibrator_step3a_r1_conditioning_only_probe_{timestamp}").resolve()


def _device_cfg(raw_cfg: Mapping[str, Any], name: str) -> dict[str, Any]:
    devices = _section(raw_cfg, "devices")
    value = devices.get(name)
    return dict(value) if isinstance(value, Mapping) else {}


def _pressure_device(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    cfg = _device_cfg(raw_cfg, "pressure_gauge")
    return {
        "device_name": "pressure_gauge",
        "device_type": "pressure_gauge",
        "port": str(cfg.get("port") or "COM30"),
        "baud": int(cfg.get("baud") or cfg.get("baudrate") or 9600),
        "timeout_s": float(cfg.get("timeout") or 1.0),
        "response_timeout_s": float(cfg.get("response_timeout_s") or 2.2),
        "dest_id": str(cfg.get("dest_id") or "01"),
    }


def _pace_device(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    cfg = _device_cfg(raw_cfg, "pressure_controller")
    return {
        "device_name": "pressure_controller",
        "device_type": "pressure_controller",
        "port": str(cfg.get("port") or "COM31"),
        "baud": int(cfg.get("baud") or cfg.get("baudrate") or 9600),
        "timeout_s": float(cfg.get("timeout") or 1.0),
        "line_ending": cfg.get("line_ending"),
        "query_line_endings": cfg.get("query_line_endings"),
        "pressure_queries": cfg.get("pressure_queries"),
    }


def _thermometer_device(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    cfg = _device_cfg(raw_cfg, "thermometer")
    return {
        "device_name": "thermometer",
        "device_type": "thermometer",
        "port": str(cfg.get("port") or "COM26"),
        "baud": int(cfg.get("baud") or cfg.get("baudrate") or 2400),
        "timeout_s": float(cfg.get("timeout") or 1.2),
        "parity": str(cfg.get("parity") or "N"),
        "stopbits": float(cfg.get("stopbits") or 1),
        "bytesize": int(cfg.get("bytesize") or 8),
    }


def _relay_devices(raw_cfg: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for name, alias in (("relay", "relay_a"), ("relay_8", "relay_b")):
        cfg = _device_cfg(raw_cfg, name)
        if _as_bool(cfg.get("enabled", True)) is False:
            continue
        out[alias] = {
            "device_name": alias,
            "device_type": "relay",
            "config_name": name,
            "port": str(cfg.get("port") or ("COM28" if name == "relay" else "COM29")),
            "baud": int(cfg.get("baud") or cfg.get("baudrate") or 38400),
            "addr": int(cfg.get("addr") or cfg.get("unit_id") or cfg.get("slave") or 1),
        }
    return out


def _gas_analyzer_devices(raw_cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    devices = _section(raw_cfg, "devices")
    out: list[dict[str, Any]] = []
    for index, item in enumerate(devices.get("gas_analyzers") or []):
        if not isinstance(item, Mapping) or _as_bool(item.get("enabled", True)) is False:
            continue
        out.append(
            {
                "device_name": str(item.get("name") or f"gas_analyzer_{index + 1}"),
                "device_type": "gas_analyzer",
                "port": str(item.get("port") or ""),
                "baud": int(item.get("baud") or item.get("baudrate") or 115200),
                "timeout_s": float(item.get("timeout") or 1.0),
                "device_id": str(item.get("device_id") or ""),
            }
        )
    return out


def _default_pressure_gauge_factory(device: Mapping[str, Any]) -> Any:
    return ParoscientificGauge(
        str(device.get("port") or "COM30"),
        int(device.get("baud") or 9600),
        timeout=float(device.get("timeout_s") or 1.0),
        dest_id=str(device.get("dest_id") or "01"),
        response_timeout_s=float(device.get("response_timeout_s") or 2.2),
    )


def _default_pace_factory(device: Mapping[str, Any]) -> Any:
    return Pace5000(
        str(device.get("port") or "COM31"),
        int(device.get("baud") or 9600),
        timeout=float(device.get("timeout_s") or 1.0),
        line_ending=device.get("line_ending"),
        query_line_endings=device.get("query_line_endings"),
        pressure_queries=device.get("pressure_queries"),
    )


def _default_thermometer_factory(device: Mapping[str, Any]) -> Any:
    return Thermometer(
        str(device.get("port") or "COM26"),
        int(device.get("baud") or 2400),
        timeout=float(device.get("timeout_s") or 1.2),
        parity=str(device.get("parity") or "N"),
        stopbits=float(device.get("stopbits") or 1),
        bytesize=int(device.get("bytesize") or 8),
    )


def _default_relay_factory(device: Mapping[str, Any]) -> Any:
    return RelayController(
        str(device.get("port") or ""),
        int(device.get("baud") or 38400),
        addr=int(device.get("addr") or 1),
    )


def _default_analyzer_serial_factory(device: Mapping[str, Any]) -> Any:
    import serial  # type: ignore[import-not-found]

    return serial.Serial(
        port=str(device.get("port") or ""),
        baudrate=int(device.get("baud") or 115200),
        timeout=float(device.get("timeout_s") or 1.0),
    )


def _r1_runtime(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    r1 = _r1_cfg(raw_cfg)
    runtime = _section(r1, "conditioning") or r1
    pressure_cfg = _section(_section(raw_cfg, "workflow"), "pressure")
    return {
        "target_co2_ppm": float(runtime.get("target_co2_ppm", runtime.get("co2_ppm", 1000.0))),
        "conditioning_duration_s": float(runtime.get("conditioning_duration_s", 6.0)),
        "vent_heartbeat_interval_s": float(
            runtime.get(
                "vent_heartbeat_interval_s",
                pressure_cfg.get("atmosphere_vent_heartbeat_interval_s", pressure_cfg.get("vent_hold_interval_s", 1.0)),
            )
            or 1.0
        ),
        "max_vent_heartbeat_gap_ms": float(runtime.get("max_vent_heartbeat_gap_ms", 3000.0)),
        "route_open_to_first_vent_max_ms": float(runtime.get("route_open_to_first_vent_max_ms", 1000.0)),
        "pressure_freshness_max_age_ms": float(runtime.get("pressure_freshness_max_age_ms", 1000.0)),
        "pressure_overlimit_hpa": float(
            runtime.get(
                "pressure_overlimit_hpa",
                pressure_cfg.get("conditioning_pressure_abort_hpa", pressure_cfg.get("preseal_atmosphere_flush_abort_pressure_hpa", 1150.0)),
            )
            or 1150.0
        ),
    }


def _target_open_valves(raw_cfg: Mapping[str, Any]) -> list[int]:
    runtime = _section(_r1_cfg(raw_cfg), "conditioning") or _r1_cfg(raw_cfg)
    configured = runtime.get("route_open_valves")
    if isinstance(configured, list) and configured:
        return [int(item) for item in configured]
    valves = _section(raw_cfg, "valves")
    open_valves: list[int] = []
    for key in ("gas_main", "co2_path"):
        value = _as_int(valves.get(key))
        if value is not None:
            open_valves.append(value)
    target_ppm = str(int(float(runtime.get("target_co2_ppm", runtime.get("co2_ppm", 1000.0)))))
    co2_map = valves.get("co2_map")
    if isinstance(co2_map, Mapping):
        source = _as_int(co2_map.get(target_ppm))
        if source is not None:
            open_valves.append(source)
    return list(dict.fromkeys(open_valves))


def _resolve_physical_states(raw_cfg: Mapping[str, Any], open_valves: list[int]) -> dict[tuple[str, int], bool]:
    relay_map = _section(_section(raw_cfg, "valves"), "relay_map")
    states: dict[tuple[str, int], bool] = {}
    for valve in open_valves:
        entry = relay_map.get(str(valve)) if isinstance(relay_map, Mapping) else None
        relay_name = "relay_a"
        channel = int(valve)
        if isinstance(entry, Mapping):
            device = str(entry.get("device", "relay") or "relay").strip().lower()
            relay_name = "relay_b" if device == "relay_8" else "relay_a"
            channel = int(entry.get("channel") or channel)
        states[(relay_name, channel)] = True
    return states


def _read_pressure_once(gauge: Any, *, timeout_s: float) -> tuple[Optional[float], str]:
    try:
        fast = getattr(gauge, "read_pressure_fast", None)
        if callable(fast):
            return float(fast(response_timeout_s=timeout_s, retries=1, retry_sleep_s=0.0, clear_buffer=False)), ""
    except Exception as exc:
        first_error = str(exc)
    else:
        first_error = ""
    try:
        reader = getattr(gauge, "read_pressure", None)
        if callable(reader):
            return float(reader(response_timeout_s=timeout_s, retries=1, retry_sleep_s=0.0, clear_buffer=False)), ""
    except Exception as exc:
        return None, str(exc)
    return None, first_error or "pressure_reader_unavailable"


def _record_device_reading(rows: list[dict[str, Any]], **kwargs: Any) -> None:
    rows.append({"timestamp": _now(), **kwargs})


def _open_device(device: Any) -> None:
    opener = getattr(device, "open", None) or getattr(device, "connect", None)
    if callable(opener):
        opener()


def _close_device(device: Any) -> None:
    closer = getattr(device, "close", None)
    if callable(closer):
        closer()


def _send_vent_on(
    pace: Any,
    *,
    vent_rows: list[dict[str, Any]],
    trace_rows: list[dict[str, Any]],
    timing_rows: list[dict[str, Any]],
    phase: str,
) -> bool:
    heartbeat_index = len(vent_rows) + 1
    start_ns = _perf_ns()
    _timing_mark(
        timing_rows,
        "each_vent_heartbeat_emit_start",
        perf_counter_ns=start_ns,
        sequence_id=heartbeat_index,
        phase=phase,
        scope="authorized_r1_atmosphere_safe_vent_heartbeat",
    )
    route_protective_first = phase != "before_route_open" and _event_ns(
        timing_rows,
        "first_vent_heartbeat_emit_start",
    ) is None
    if route_protective_first:
        _timing_mark(
            timing_rows,
            "first_vent_heartbeat_emit_start",
            perf_counter_ns=start_ns,
            sequence_id=heartbeat_index,
            phase=phase,
            scope="authorized_r1_atmosphere_safe_vent_heartbeat",
        )
    sent_at = time.monotonic()
    try:
        vent = getattr(pace, "vent", None)
        if not callable(vent):
            raise RuntimeError("pace_vent_method_unavailable")
        vent(True)
        ok = True
        error = ""
    except Exception as exc:
        ok = False
        error = str(exc)
    end_ns = _perf_ns()
    if route_protective_first:
        _timing_mark(
            timing_rows,
            "first_vent_heartbeat_emit_end",
            perf_counter_ns=end_ns,
            sequence_id=heartbeat_index,
            phase=phase,
            scope="authorized_r1_atmosphere_safe_vent_heartbeat",
        )
    _timing_mark(
        timing_rows,
        "each_vent_heartbeat_emit_end",
        perf_counter_ns=end_ns,
        sequence_id=heartbeat_index,
        phase=phase,
        scope="authorized_r1_atmosphere_safe_vent_heartbeat",
        result="ok" if ok else "error",
    )
    row = {
        "timestamp": _now(),
        "phase": phase,
        "monotonic_s": sent_at,
        "emit_start_perf_counter_ns": start_ns,
        "emit_end_perf_counter_ns": end_ns,
        "emit_duration_ms": _ms_between(start_ns, end_ns),
        "vent_on": True,
        "vent_off": False,
        "result": "ok" if ok else "error",
        "error": error,
        "command_scope": "authorized_r1_atmosphere_safe_vent_heartbeat",
    }
    vent_rows.append(row)
    trace_rows.append({"timestamp": row["timestamp"], "event": "vent_heartbeat", **row})
    return ok


def _read_pace_status(pace: Any, rows: list[dict[str, Any]]) -> dict[str, Any]:
    status: dict[str, Any] = {}
    for key, method_name in (
        ("identity", "get_device_identity"),
        ("output_state", "get_output_state"),
        ("isolation_state", "get_isolation_state"),
        ("vent_status", "get_vent_status"),
    ):
        method = getattr(pace, method_name, None)
        if not callable(method):
            continue
        try:
            status[key] = method()
        except Exception as exc:
            status[f"{key}_error"] = str(exc)
    _record_device_reading(
        rows,
        device_name="pressure_controller",
        device_type="pressure_controller",
        port=getattr(getattr(pace, "ser", None), "port", ""),
        action="read_status",
        result="available" if status else "unavailable",
        data=status,
        read_only=True,
    )
    return status


def _read_thermometer(raw_cfg: Mapping[str, Any], rows: list[dict[str, Any]], factory: Callable[[Mapping[str, Any]], Any]) -> Optional[float]:
    device = _thermometer_device(raw_cfg)
    thermometer = None
    try:
        thermometer = factory(device)
        _open_device(thermometer)
        reader = getattr(thermometer, "read_temp_c", None)
        value = float(reader()) if callable(reader) else None
        _record_device_reading(
            rows,
            device_name="thermometer",
            device_type="thermometer",
            port=device["port"],
            action="read_temp_c",
            result="available" if value is not None else "unavailable",
            data={"temp_c": value},
            read_only=True,
        )
        return value
    except Exception as exc:
        _record_device_reading(
            rows,
            device_name="thermometer",
            device_type="thermometer",
            port=device["port"],
            action="read_temp_c",
            result="unavailable",
            error=str(exc),
            read_only=True,
        )
        return None
    finally:
        if thermometer is not None:
            try:
                _close_device(thermometer)
            except Exception:
                pass


def _read_analyzers(
    raw_cfg: Mapping[str, Any],
    rows: list[dict[str, Any]],
    serial_factory: Callable[[Mapping[str, Any]], Any],
) -> dict[str, Any]:
    statuses: list[dict[str, Any]] = []
    for device in _gas_analyzer_devices(raw_cfg):
        handle = None
        try:
            handle = serial_factory(device)
            raw = handle.readline()
            if isinstance(raw, bytes):
                text = raw.decode("ascii", errors="replace").strip()
            else:
                text = str(raw or "").strip()
            parser = GasAnalyzer(device["port"], device["baud"], timeout=device["timeout_s"], device_id=device.get("device_id") or "000")
            parsed = parser.parse_line(text) if text else None
            status = {
                "device_name": device["device_name"],
                "port": device["port"],
                "result": "frame_seen" if text else "no_frame_seen",
                "raw_preview": text[:160],
                "parsed": parsed if isinstance(parsed, Mapping) else {},
            }
            statuses.append(status)
            _record_device_reading(
                rows,
                device_name=device["device_name"],
                device_type="gas_analyzer",
                port=device["port"],
                action="read_frame_no_command",
                result=status["result"],
                data=status,
                read_only=True,
            )
        except Exception as exc:
            status = {
                "device_name": device["device_name"],
                "port": device["port"],
                "result": "unavailable",
                "error": str(exc),
            }
            statuses.append(status)
            _record_device_reading(
                rows,
                device_name=device["device_name"],
                device_type="gas_analyzer",
                port=device["port"],
                action="read_frame_no_command",
                result="unavailable",
                error=str(exc),
                read_only=True,
            )
        finally:
            if handle is not None:
                try:
                    handle.close()
                except Exception:
                    pass
    return {"analyzer_count": len(statuses), "statuses": statuses}


def _apply_relay_states(
    relays: dict[str, Any],
    states: dict[tuple[str, int], bool],
    *,
    route_rows: list[dict[str, Any]],
    trace_rows: list[dict[str, Any]],
    timing_rows: list[dict[str, Any]],
    action: str,
) -> tuple[int, bool]:
    count = 0
    ok = True
    ordered_states = list(sorted(states.items()))
    for index, ((relay_name, channel), desired) in enumerate(ordered_states, start=1):
        route_open_action = action == "set_co2_route_conditioning"
        start_ns = _perf_ns()
        if route_open_action and index == 1:
            _timing_mark(
                timing_rows,
                "first_route_action_start",
                perf_counter_ns=start_ns,
                sequence_id=index,
                action=action,
                relay=relay_name,
                channel=int(channel),
            )
        if route_open_action and index == len(ordered_states):
            _timing_mark(
                timing_rows,
                "last_route_action_start",
                perf_counter_ns=start_ns,
                sequence_id=index,
                action=action,
                relay=relay_name,
                channel=int(channel),
            )
        _timing_mark(
            timing_rows,
            "each_relay_action_start",
            perf_counter_ns=start_ns,
            sequence_id=index,
            action=action,
            relay=relay_name,
            channel=int(channel),
            scope="authorized_r1_route_conditioning_only",
        )
        relay = relays.get(relay_name)
        row = {
            "timestamp": _now(),
            "action": action,
            "route": "co2",
            "relay": relay_name,
            "channel": int(channel),
            "desired_open": bool(desired),
            "command_scope": "authorized_r1_route_conditioning_only",
            "result": "not_executed",
        }
        try:
            if relay is None:
                raise RuntimeError(f"{relay_name}_unavailable")
            setter = getattr(relay, "set_valve", None)
            if not callable(setter):
                raise RuntimeError(f"{relay_name}_set_valve_unavailable")
            setter(int(channel), bool(desired))
            row["result"] = "ok"
            count += 1
        except Exception as exc:
            ok = False
            row["result"] = "error"
            row["error"] = str(exc)
        end_ns = _perf_ns()
        _timing_mark(
            timing_rows,
            "each_relay_action_end",
            perf_counter_ns=end_ns,
            sequence_id=index,
            action=action,
            relay=relay_name,
            channel=int(channel),
            scope="authorized_r1_route_conditioning_only",
            result=row["result"],
        )
        if route_open_action and index == 1:
            _timing_mark(
                timing_rows,
                "first_route_action_end",
                perf_counter_ns=end_ns,
                sequence_id=index,
                action=action,
                relay=relay_name,
                channel=int(channel),
                result=row["result"],
            )
        if route_open_action and index == len(ordered_states):
            _timing_mark(
                timing_rows,
                "last_route_action_end",
                perf_counter_ns=end_ns,
                sequence_id=index,
                action=action,
                relay=relay_name,
                channel=int(channel),
                result=row["result"],
            )
        row["action_start_perf_counter_ns"] = start_ns
        row["action_end_perf_counter_ns"] = end_ns
        row["action_duration_ms"] = _ms_between(start_ns, end_ns)
        route_rows.append(row)
        trace_rows.append({"timestamp": row["timestamp"], "event": "relay_route_action", **row})
    return count, ok


def write_r1_conditioning_only_probe_artifacts(
    raw_cfg: Mapping[str, Any],
    *,
    output_dir: Optional[str | Path] = None,
    config_path: str | Path = "",
    operator_confirmation_path: Optional[str | Path] = None,
    branch: str = "",
    head: str = "",
    cli_allow: bool = False,
    env: Optional[Mapping[str, str]] = None,
    execute_conditioning_only: bool = False,
    pressure_gauge_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
    pace_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
    relay_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
    thermometer_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
    analyzer_serial_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
    chamber_client_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    admission = evaluate_r1_conditioning_only_gate(
        raw_cfg,
        cli_allow=cli_allow,
        env=env,
        operator_confirmation_path=operator_confirmation_path,
        branch=branch,
        head=head,
        config_path=str(config_path or ""),
    )
    run_dir = Path(output_dir).expanduser().resolve() if output_dir else _default_output_dir()
    run_dir.mkdir(parents=True, exist_ok=True)

    runtime = _r1_runtime(raw_cfg)
    artifact_paths = {
        "summary": str(run_dir / "summary.json"),
        "r1_conditioning_trace": str(run_dir / "r1_conditioning_trace.jsonl"),
        "route_trace": str(run_dir / "route_trace.jsonl"),
        "device_readings": str(run_dir / "device_readings.jsonl"),
        "vent_heartbeat_trace": str(run_dir / "vent_heartbeat_trace.jsonl"),
        "pressure_freshness_trace": str(run_dir / "pressure_freshness_trace.jsonl"),
        "r1_timing_breakdown": str(run_dir / "r1_timing_breakdown.json"),
        "r1_timing_events": str(run_dir / "r1_timing_events.jsonl"),
        "r1_latency_breakdown": str(run_dir / "r1_latency_breakdown.csv"),
        "safety_assertions": str(run_dir / "safety_assertions.json"),
        "operator_confirmation_record": str(run_dir / "operator_confirmation_record.json"),
    }

    trace_rows: list[dict[str, Any]] = []
    route_rows: list[dict[str, Any]] = []
    device_rows: list[dict[str, Any]] = []
    vent_rows: list[dict[str, Any]] = []
    pressure_rows: list[dict[str, Any]] = []
    timing_rows: list[dict[str, Any]] = []
    rejection_reasons = list(admission.reasons)
    executed = bool(admission.approved and execute_conditioning_only)
    opened_ports: set[str] = set()
    relay_route_action_count = 0
    relay_output_command_sent = False
    pressure_latest_hpa: Optional[float] = None
    pressure_freshness_ok = False
    pressure_overlimit_seen = False
    pressure_overlimit_fail_closed = False
    conditioning_started = False
    route_open_to_first_vent_ms: Optional[float] = None
    conditioning_duration_s = 0.0
    chamber_diag: dict[str, Any] = {}
    pace_status: dict[str, Any] = {}
    analyzer_status: dict[str, Any] = {}
    thermometer_temp_c: Optional[float] = None

    final_decision = "FAIL_CLOSED"
    relays: dict[str, Any] = {}
    gauge: Any = None
    pace: Any = None
    route_opened = False
    route_states = _resolve_physical_states(raw_cfg, _target_open_valves(raw_cfg))

    if not executed:
        if not execute_conditioning_only:
            rejection_reasons.append("execute_conditioning_only_not_requested")
        trace_rows.append(
            {
                "timestamp": _now(),
                "event": "r1_conditioning_not_executed",
                "result": "fail_closed",
                "rejection_reasons": list(dict.fromkeys(rejection_reasons)),
            }
        )
    else:
        start_monotonic = time.monotonic()
        try:
            _timing_mark(timing_rows, "r1_conditioning_start", stage="r1_conditioning")
            trace_rows.append({"timestamp": _now(), "event": "r1_conditioning_start", "result": "started"})

            chamber_read_start_ns = _timing_mark(
                timing_rows,
                "temperature_chamber_read_start",
                stage="pre_route_reference_read",
                device_name="temperature_chamber",
            )
            chamber_diag, chamber_trace = read_temperature_chamber_read_only(
                raw_cfg,
                client_factory=chamber_client_factory or _default_chamber_client_factory,
            )
            chamber_read_end_ns = _perf_ns()
            _timing_mark(
                timing_rows,
                "temperature_chamber_read_end",
                stage="pre_route_reference_read",
                device_name="temperature_chamber",
                perf_counter_ns=chamber_read_end_ns,
                duration_ms=_ms_between(chamber_read_start_ns, chamber_read_end_ns),
            )
            for row in chamber_trace:
                device_rows.append({**dict(row), "read_only": True})
                if row.get("result") == "ok" and row.get("port"):
                    opened_ports.add(str(row.get("port")))

            thermometer_read_start_ns = _timing_mark(
                timing_rows,
                "thermometer_read_start",
                stage="pre_route_reference_read",
                device_name="thermometer",
            )
            thermometer_temp_c = _read_thermometer(raw_cfg, device_rows, thermometer_factory or _default_thermometer_factory)
            thermometer_read_end_ns = _perf_ns()
            _timing_mark(
                timing_rows,
                "thermometer_read_end",
                stage="pre_route_reference_read",
                device_name="thermometer",
                perf_counter_ns=thermometer_read_end_ns,
                duration_ms=_ms_between(thermometer_read_start_ns, thermometer_read_end_ns),
            )

            analyzer_read_start_ns = _timing_mark(
                timing_rows,
                "analyzer_read_start",
                stage="pre_route_reference_read",
                device_name="gas_analyzers",
            )
            analyzer_status = _read_analyzers(raw_cfg, device_rows, analyzer_serial_factory or _default_analyzer_serial_factory)
            analyzer_read_end_ns = _perf_ns()
            _timing_mark(
                timing_rows,
                "analyzer_read_end",
                stage="pre_route_reference_read",
                device_name="gas_analyzers",
                perf_counter_ns=analyzer_read_end_ns,
                duration_ms=_ms_between(analyzer_read_start_ns, analyzer_read_end_ns),
            )

            pace = (pace_factory or _default_pace_factory)(_pace_device(raw_cfg))
            _open_device(pace)
            opened_ports.add(_pace_device(raw_cfg)["port"])
            pace_status = _read_pace_status(pace, device_rows)

            gauge = (pressure_gauge_factory or _default_pressure_gauge_factory)(_pressure_device(raw_cfg))
            _open_device(gauge)
            opened_ports.add(_pressure_device(raw_cfg)["port"])

            pressure_timeout_s = float(_pressure_device(raw_cfg).get("response_timeout_s") or 2.2)
            pressure_sequence_id = 1
            pressure_perf_start_ns = _timing_mark(
                timing_rows,
                "pressure_gauge_read_start",
                sequence_id=pressure_sequence_id,
                stage="pre_route",
                device_name="pressure_gauge",
            )
            read_start = time.monotonic()
            pressure_latest_hpa, pressure_error = _read_pressure_once(gauge, timeout_s=pressure_timeout_s)
            read_end = time.monotonic()
            pressure_perf_end_ns = _timing_mark(
                timing_rows,
                "pressure_gauge_read_end",
                sequence_id=pressure_sequence_id,
                stage="pre_route",
                device_name="pressure_gauge",
                pressure_hpa=pressure_latest_hpa,
                error=pressure_error,
            )
            latency_ms = (read_end - read_start) * 1000.0
            age_ms = 0.0 if pressure_latest_hpa is not None else latency_ms
            pressure_freshness_ok = pressure_latest_hpa is not None and age_ms <= runtime["pressure_freshness_max_age_ms"]
            pressure_overlimit_seen = bool(
                pressure_latest_hpa is not None and pressure_latest_hpa > runtime["pressure_overlimit_hpa"]
            )
            pressure_rows.append(
                {
                    "timestamp": _now(),
                    "stage": "pre_route",
                    "pressure_hpa": pressure_latest_hpa,
                    "read_latency_ms": round(latency_ms, 3),
                    "freshness_age_ms": round(age_ms, 3),
                    "freshness_ok": pressure_freshness_ok,
                    "pressure_overlimit_seen": pressure_overlimit_seen,
                    "error": pressure_error,
                }
            )
            if pressure_overlimit_seen or not pressure_freshness_ok:
                rejection_reasons.append("pre_route_pressure_not_safe_or_fresh")
                final_decision = "FAIL_CLOSED"
            else:
                if not _send_vent_on(
                    pace,
                    vent_rows=vent_rows,
                    trace_rows=trace_rows,
                    timing_rows=timing_rows,
                    phase="before_route_open",
                ):
                    rejection_reasons.append("pre_route_vent_heartbeat_failed")
                    final_decision = "FAIL_CLOSED"
                else:
                    _timing_mark(timing_rows, "route_conditioning_start", stage="route_conditioning")
                    for name, device in _relay_devices(raw_cfg).items():
                        relay = (relay_factory or _default_relay_factory)(device)
                        _open_device(relay)
                        opened_ports.add(str(device.get("port") or ""))
                        relays[name] = relay
                    route_open_start = time.monotonic()
                    count, route_ok = _apply_relay_states(
                        relays,
                        route_states,
                        route_rows=route_rows,
                        trace_rows=trace_rows,
                        timing_rows=timing_rows,
                        action="set_co2_route_conditioning",
                    )
                    relay_route_action_count += count
                    relay_output_command_sent = relay_output_command_sent or count > 0
                    route_opened = bool(route_ok and route_states)
                    if not route_ok:
                        rejection_reasons.append("route_open_failed")
                        final_decision = "FAIL_CLOSED"
                    else:
                        conditioning_started = bool(route_opened)
                        route_open_end = time.monotonic()
                        route_completed_ns = _timing_mark(
                            timing_rows,
                            "route_open_completed",
                            stage="route_conditioning",
                            route="co2",
                        )
                        _timing_mark(
                            timing_rows,
                            "vent_heartbeat_scheduler_started",
                            stage="route_conditioning",
                            route="co2",
                            anchor_event="route_open_completed",
                            anchor_perf_counter_ns=route_completed_ns,
                        )
                        vent_ok = _send_vent_on(
                            pace,
                            vent_rows=vent_rows,
                            trace_rows=trace_rows,
                            timing_rows=timing_rows,
                            phase="after_route_open",
                        )
                        route_open_to_first_vent_ms = round((time.monotonic() - route_open_end) * 1000.0, 3)
                        if not vent_ok:
                            rejection_reasons.append("post_route_vent_heartbeat_failed")
                        if route_open_to_first_vent_ms > runtime["route_open_to_first_vent_max_ms"]:
                            rejection_reasons.append("route_open_to_first_vent_exceeded")

                        next_tick = time.monotonic()
                        deadline = route_open_start + runtime["conditioning_duration_s"]
                        while not rejection_reasons and time.monotonic() < deadline:
                            read_start = time.monotonic()
                            pressure_sequence_id += 1
                            pressure_perf_start_ns = _timing_mark(
                                timing_rows,
                                "pressure_gauge_read_start",
                                sequence_id=pressure_sequence_id,
                                stage="conditioning",
                                device_name="pressure_gauge",
                            )
                            pressure_latest_hpa, pressure_error = _read_pressure_once(gauge, timeout_s=pressure_timeout_s)
                            read_end = time.monotonic()
                            pressure_perf_end_ns = _timing_mark(
                                timing_rows,
                                "pressure_gauge_read_end",
                                sequence_id=pressure_sequence_id,
                                stage="conditioning",
                                device_name="pressure_gauge",
                                pressure_hpa=pressure_latest_hpa,
                                error=pressure_error,
                            )
                            latency_ms = (read_end - read_start) * 1000.0
                            age_ms = 0.0 if pressure_latest_hpa is not None else latency_ms
                            fresh = pressure_latest_hpa is not None and age_ms <= runtime["pressure_freshness_max_age_ms"]
                            pressure_freshness_ok = pressure_freshness_ok and fresh
                            overlimit = bool(
                                pressure_latest_hpa is not None
                                and pressure_latest_hpa > runtime["pressure_overlimit_hpa"]
                            )
                            pressure_overlimit_seen = pressure_overlimit_seen or overlimit
                            pressure_rows.append(
                                {
                                    "timestamp": _now(),
                                    "stage": "conditioning",
                                    "pressure_hpa": pressure_latest_hpa,
                                    "read_latency_ms": round(latency_ms, 3),
                                    "freshness_age_ms": round(age_ms, 3),
                                    "freshness_ok": fresh,
                                    "pressure_overlimit_seen": overlimit,
                                    "error": pressure_error,
                                }
                            )
                            if overlimit:
                                rejection_reasons.append("pressure_overlimit_seen")
                                break
                            if not fresh:
                                rejection_reasons.append("pressure_freshness_lost")
                                break
                            if not _send_vent_on(
                                pace,
                                vent_rows=vent_rows,
                                trace_rows=trace_rows,
                                timing_rows=timing_rows,
                                phase="conditioning_heartbeat",
                            ):
                                rejection_reasons.append("conditioning_vent_heartbeat_failed")
                                break
                            next_tick += runtime["vent_heartbeat_interval_s"]
                            remaining_sleep = max(0.0, min(next_tick - time.monotonic(), deadline - time.monotonic()))
                            if remaining_sleep:
                                sleep_fn(remaining_sleep)

                        conditioning_duration_s = round(max(0.0, time.monotonic() - route_open_start), 3)
                        if pressure_overlimit_seen:
                            pressure_overlimit_fail_closed = True
                        final_decision = "FAIL_CLOSED" if rejection_reasons else "PASS"
        except Exception as exc:
            rejection_reasons.append(f"execution_error:{exc}")
            final_decision = "FAIL_CLOSED"
        finally:
            if _event_ns(timing_rows, "route_conditioning_start") is not None and _event_ns(
                timing_rows,
                "route_conditioning_end",
            ) is None:
                _timing_mark(
                    timing_rows,
                    "route_conditioning_end",
                    stage="route_conditioning",
                    result=final_decision,
                )
            if route_opened and relays:
                close_states = {key: False for key in route_states}
                count, cleanup_ok = _apply_relay_states(
                    relays,
                    close_states,
                    route_rows=route_rows,
                    trace_rows=trace_rows,
                    timing_rows=timing_rows,
                    action="cleanup_r1_route_conditioning",
                )
                relay_route_action_count += count
                relay_output_command_sent = relay_output_command_sent or count > 0
                if not cleanup_ok:
                    rejection_reasons.append("route_cleanup_failed")
                    final_decision = "FAIL_CLOSED"
            for relay in relays.values():
                try:
                    _close_device(relay)
                except Exception:
                    pass
            for device in (gauge, pace):
                if device is not None:
                    try:
                        _close_device(device)
                    except Exception:
                        pass
            _timing_mark(timing_rows, "r1_conditioning_end", stage="r1_conditioning", result=final_decision)
            trace_rows.append({"timestamp": _now(), "event": "r1_conditioning_end", "result": final_decision})

    vent_gaps_ms: list[float] = []
    for previous, current in zip(vent_rows, vent_rows[1:]):
        vent_gaps_ms.append((float(current.get("monotonic_s") or 0.0) - float(previous.get("monotonic_s") or 0.0)) * 1000.0)
    max_vent_gap_ms = round(max(vent_gaps_ms), 3) if vent_gaps_ms else 0.0
    vent_heartbeat_count = len(vent_rows)
    if executed and vent_heartbeat_count == 0:
        rejection_reasons.append("missing_vent_heartbeat")
        final_decision = "FAIL_CLOSED"
    if executed and max_vent_gap_ms > runtime["max_vent_heartbeat_gap_ms"]:
        rejection_reasons.append("max_vent_heartbeat_gap_exceeded")
        final_decision = "FAIL_CLOSED"
    if executed and not pressure_freshness_ok:
        rejection_reasons.append("pressure_gauge_freshness_not_ok")
        final_decision = "FAIL_CLOSED"
    if pressure_overlimit_seen:
        final_decision = "FAIL_CLOSED"
    for row in device_rows:
        port = row.get("port")
        if port and row.get("result") not in {"unavailable", "error"}:
            opened_ports.add(str(port))

    rejection_reasons = list(dict.fromkeys(rejection_reasons))
    safety_assertions = {
        **R1_EVIDENCE_MARKERS,
        **R1_SAFETY_ASSERTION_DEFAULTS,
        "authorized_r1_route_control_command_count": int(relay_route_action_count),
        "relay_output_command_sent": bool(relay_output_command_sent),
        "relay_output_command_scope": "authorized_r1_route_conditioning_only" if relay_output_command_sent else "",
        "non_authorized_relay_output_command_sent": False,
        "valve_command_sent": bool(relay_output_command_sent),
        "valve_command_scope": "authorized_r1_route_conditioning_only" if relay_output_command_sent else "",
    }
    timing_breakdown = _build_timing_breakdown(
        timing_rows,
        route_open_to_first_vent_ms=route_open_to_first_vent_ms,
        max_vent_heartbeat_gap_ms=max_vent_gap_ms,
        vent_heartbeat_count=vent_heartbeat_count,
        relay_route_action_count=relay_route_action_count,
        route_open_to_first_vent_threshold_ms=runtime["route_open_to_first_vent_max_ms"],
    )
    timing_summary_fields = {
        key: value
        for key, value in timing_breakdown.items()
        if key != "latency_breakdown_rows"
    }
    chamber_unavailable = bool(chamber_diag.get("temperature_chamber_unavailable"))
    summary = {
        "schema_version": R1_SCHEMA_VERSION,
        **R1_EVIDENCE_MARKERS,
        "final_decision": final_decision,
        "admission_approved": admission.approved,
        "rejection_reasons": rejection_reasons,
        "r0_full_query_only_prereq_pass": bool(admission.evidence["r0_full_query_only_prereq_pass"]),
        "r0_full_query_only_output_dir": _r0_output_dir(raw_cfg),
        "operator_confirmation_valid": bool(admission.operator_validation.get("valid")),
        "r1_conditioning_only_executed": bool(conditioning_started),
        "real_com_opened": bool(opened_ports),
        "real_probe_executed": bool(executed),
        "a1r_allowed": False,
        "a2_allowed": False,
        "a3_allowed": False,
        "co2_only": True,
        "skip0": True,
        "single_route": True,
        "single_temperature": True,
        "pressure_gauge_latest_hpa": pressure_latest_hpa,
        "pressure_gauge_freshness_ok": bool(pressure_freshness_ok),
        "temperature_chamber_pv_c": chamber_diag.get("pv_temperature_c", chamber_diag.get("pv_current_temperature_c")),
        "temperature_chamber_sv_c": chamber_diag.get("sv_temperature_c", chamber_diag.get("sv_set_temperature_c")),
        "temperature_chamber_unavailable": chamber_unavailable,
        "thermometer_temp_c": thermometer_temp_c,
        "pace_identity_or_status": pace_status,
        "analyzer_readonly_status": analyzer_status,
        "relay_route_action_count": int(relay_route_action_count),
        "relay_output_command_sent": bool(relay_output_command_sent),
        "relay_output_command_scope": "authorized_r1_route_conditioning_only" if relay_output_command_sent else "",
        "route_open_to_first_vent_ms": route_open_to_first_vent_ms,
        "max_vent_heartbeat_gap_ms": max_vent_gap_ms,
        "vent_heartbeat_count": vent_heartbeat_count,
        "conditioning_duration_s": conditioning_duration_s,
        "pressure_overlimit_seen": bool(pressure_overlimit_seen),
        "pressure_overlimit_fail_closed": bool(pressure_overlimit_fail_closed),
        "opened_ports": sorted(opened_ports),
        "artifact_paths": artifact_paths,
        **timing_summary_fields,
        **safety_assertions,
    }
    operator_record = {
        "schema_version": R1_SCHEMA_VERSION,
        "record_type": "r1_operator_confirmation_record",
        "operator_confirmation_path": str(Path(operator_confirmation_path).expanduser().resolve()) if operator_confirmation_path else "",
        "validation": admission.operator_validation,
        "payload": admission.operator_confirmation,
        "not_real_acceptance_evidence": True,
        "acceptance_level": "engineering_probe_only",
        "promotion_state": "blocked",
        "real_primary_latest_refresh": False,
    }
    timing_payload = {
        "schema_version": R1_SCHEMA_VERSION,
        **R1_EVIDENCE_MARKERS,
        **timing_breakdown,
    }

    _timing_mark(
        timing_rows,
        "evidence_write_start",
        stage="artifact_write",
        artifact_count=len(artifact_paths),
    )
    _json_dump(run_dir / "summary.json", summary)
    _jsonl_dump(run_dir / "r1_conditioning_trace.jsonl", trace_rows)
    _jsonl_dump(run_dir / "route_trace.jsonl", route_rows)
    _jsonl_dump(run_dir / "device_readings.jsonl", device_rows)
    _jsonl_dump(run_dir / "vent_heartbeat_trace.jsonl", vent_rows)
    _jsonl_dump(run_dir / "pressure_freshness_trace.jsonl", pressure_rows)
    _json_dump(run_dir / "safety_assertions.json", safety_assertions)
    _json_dump(run_dir / "operator_confirmation_record.json", operator_record)
    _json_dump(run_dir / "r1_timing_breakdown.json", timing_payload)
    _jsonl_dump(run_dir / "r1_timing_events.jsonl", timing_rows)
    _csv_dump(run_dir / "r1_latency_breakdown.csv", timing_breakdown["latency_breakdown_rows"])
    _timing_mark(
        timing_rows,
        "evidence_write_end",
        stage="artifact_write",
        artifact_count=len(artifact_paths),
    )

    timing_breakdown = _build_timing_breakdown(
        timing_rows,
        route_open_to_first_vent_ms=route_open_to_first_vent_ms,
        max_vent_heartbeat_gap_ms=max_vent_gap_ms,
        vent_heartbeat_count=vent_heartbeat_count,
        relay_route_action_count=relay_route_action_count,
        route_open_to_first_vent_threshold_ms=runtime["route_open_to_first_vent_max_ms"],
    )
    timing_summary_fields = {
        key: value
        for key, value in timing_breakdown.items()
        if key != "latency_breakdown_rows"
    }
    summary.update(timing_summary_fields)
    timing_payload = {
        "schema_version": R1_SCHEMA_VERSION,
        **R1_EVIDENCE_MARKERS,
        **timing_breakdown,
    }
    _json_dump(run_dir / "summary.json", summary)
    _json_dump(run_dir / "r1_timing_breakdown.json", timing_payload)
    _jsonl_dump(run_dir / "r1_timing_events.jsonl", timing_rows)
    _csv_dump(run_dir / "r1_latency_breakdown.csv", timing_breakdown["latency_breakdown_rows"])
    return summary
