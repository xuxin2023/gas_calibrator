from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import time
from typing import Any, Mapping, Optional


WORKFLOW_TIMING_TRACE_FILENAME = "workflow_timing_trace.jsonl"
WORKFLOW_TIMING_SUMMARY_FILENAME = "workflow_timing_summary.json"
WORKFLOW_TIMING_SCHEMA_VERSION = "v2.workflow_timing.1"

TIMING_EVENT_FIELDS = (
    "event_name",
    "event_type",
    "timestamp_local",
    "timestamp_monotonic_s",
    "elapsed_from_run_start_s",
    "stage",
    "point_index",
    "target_pressure_hpa",
    "duration_s",
    "expected_max_s",
    "wait_reason",
    "blocking_condition",
    "decision",
    "route_state",
    "pressure_hpa",
    "chamber_temperature_c",
    "dewpoint_c",
    "pace_output_state",
    "pace_isolation_state",
    "pace_vent_status",
    "sample_count",
    "warning_code",
    "error_code",
    "no_write_guard_active",
)

_START_SUFFIX = "_start"
_END_SUFFIX = "_end"


def _as_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _as_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:
        return None


def _round_s(value: Any) -> Optional[float]:
    numeric = _as_float(value)
    if numeric is None:
        return None
    return round(max(0.0, float(numeric)), 3)


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _parse_ts(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def _stage_from_event_name(event_name: str) -> str:
    text = str(event_name or "").strip()
    if text.endswith(_START_SUFFIX):
        return text[: -len(_START_SUFFIX)]
    if text.endswith(_END_SUFFIX):
        return text[: -len(_END_SUFFIX)]
    if text == "pressure_ready" or text == "pressure_timeout":
        return "pressure_setpoint"
    if text == "run_fail" or text == "run_abort":
        return "run"
    return text


def _point_key(event: Mapping[str, Any]) -> str:
    point_index = event.get("point_index")
    if point_index in (None, ""):
        return ""
    return str(point_index)


def _duration_between(start_event: Mapping[str, Any], end_event: Mapping[str, Any]) -> Optional[float]:
    start = _as_float(start_event.get("timestamp_monotonic_s"))
    end = _as_float(end_event.get("timestamp_monotonic_s"))
    if start is None or end is None:
        return None
    return _round_s(end - start)


def _event_record(
    *,
    event_name: str,
    event_type: str,
    run_start_monotonic_s: Optional[float],
    timestamp_local: Optional[str] = None,
    timestamp_monotonic_s: Optional[float] = None,
    stage: str = "",
    point_index: Any = None,
    target_pressure_hpa: Any = None,
    duration_s: Any = None,
    expected_max_s: Any = None,
    wait_reason: Any = None,
    blocking_condition: Any = None,
    decision: Any = None,
    route_state: Any = None,
    pressure_hpa: Any = None,
    chamber_temperature_c: Any = None,
    dewpoint_c: Any = None,
    pace_output_state: Any = None,
    pace_isolation_state: Any = None,
    pace_vent_status: Any = None,
    sample_count: Any = None,
    warning_code: Any = None,
    error_code: Any = None,
    no_write_guard_active: Any = None,
) -> dict[str, Any]:
    now_monotonic = time.monotonic() if timestamp_monotonic_s is None else float(timestamp_monotonic_s)
    start_monotonic = now_monotonic if run_start_monotonic_s is None else float(run_start_monotonic_s)
    record = {
        "event_name": str(event_name or "").strip(),
        "event_type": str(event_type or "info").strip().lower() or "info",
        "timestamp_local": timestamp_local or datetime.now().astimezone().isoformat(timespec="milliseconds"),
        "timestamp_monotonic_s": _round_s(now_monotonic),
        "elapsed_from_run_start_s": _round_s(now_monotonic - start_monotonic),
        "stage": str(stage or _stage_from_event_name(str(event_name or ""))).strip(),
        "point_index": _as_int(point_index),
        "target_pressure_hpa": _as_float(target_pressure_hpa),
        "duration_s": _round_s(duration_s),
        "expected_max_s": _round_s(expected_max_s),
        "wait_reason": None if wait_reason in ("", None) else str(wait_reason),
        "blocking_condition": None if blocking_condition in ("", None) else str(blocking_condition),
        "decision": None if decision in ("", None) else str(decision),
        "route_state": _json_safe(route_state) if route_state is not None else None,
        "pressure_hpa": _as_float(pressure_hpa),
        "chamber_temperature_c": _as_float(chamber_temperature_c),
        "dewpoint_c": _as_float(dewpoint_c),
        "pace_output_state": pace_output_state,
        "pace_isolation_state": pace_isolation_state,
        "pace_vent_status": pace_vent_status,
        "sample_count": _as_int(sample_count),
        "warning_code": None if warning_code in ("", None) else str(warning_code),
        "error_code": None if error_code in ("", None) else str(error_code),
        "no_write_guard_active": None if no_write_guard_active is None else bool(no_write_guard_active),
    }
    return {field: record.get(field) for field in TIMING_EVENT_FIELDS}


class TimingMonitorService:
    """Append-only workflow timing monitor.

    The service never talks to devices. Callers pass snapshots they already have.
    """

    def __init__(
        self,
        run_dir: str | Path,
        *,
        run_id: str = "",
        no_write_guard_active: bool = False,
        enabled: bool = True,
    ) -> None:
        self.run_dir = Path(run_dir)
        self.run_id = str(run_id or "")
        self.no_write_guard_active = bool(no_write_guard_active)
        self.enabled = bool(enabled)
        self.trace_path = self.run_dir / WORKFLOW_TIMING_TRACE_FILENAME
        self.summary_path = self.run_dir / WORKFLOW_TIMING_SUMMARY_FILENAME
        self._run_start_monotonic_s: Optional[float] = None
        self._open_events: dict[tuple[str, str], dict[str, Any]] = {}

    def record_event(
        self,
        event_name: str,
        event_type: str = "info",
        *,
        stage: str = "",
        point_index: Any = None,
        target_pressure_hpa: Any = None,
        duration_s: Any = None,
        expected_max_s: Any = None,
        wait_reason: Any = None,
        blocking_condition: Any = None,
        decision: Any = None,
        route_state: Any = None,
        pressure_hpa: Any = None,
        chamber_temperature_c: Any = None,
        dewpoint_c: Any = None,
        pace_output_state: Any = None,
        pace_isolation_state: Any = None,
        pace_vent_status: Any = None,
        sample_count: Any = None,
        warning_code: Any = None,
        error_code: Any = None,
        no_write_guard_active: Any = None,
    ) -> dict[str, Any]:
        if not self.enabled:
            return {}
        try:
            now_monotonic = time.monotonic()
            if self._run_start_monotonic_s is None or str(event_name) == "run_start":
                self._run_start_monotonic_s = now_monotonic
            resolved_stage = stage or _stage_from_event_name(str(event_name or ""))
            if event_type == "end" and duration_s is None:
                start_key = (resolved_stage, "" if point_index in (None, "") else str(point_index))
                start_event = self._open_events.get(start_key)
                if start_event:
                    duration_s = now_monotonic - float(start_event.get("timestamp_monotonic_s", now_monotonic))
            record = _event_record(
                event_name=event_name,
                event_type=event_type,
                run_start_monotonic_s=self._run_start_monotonic_s,
                timestamp_monotonic_s=now_monotonic,
                stage=resolved_stage,
                point_index=point_index,
                target_pressure_hpa=target_pressure_hpa,
                duration_s=duration_s,
                expected_max_s=expected_max_s,
                wait_reason=wait_reason,
                blocking_condition=blocking_condition,
                decision=decision,
                route_state=route_state,
                pressure_hpa=pressure_hpa,
                chamber_temperature_c=chamber_temperature_c,
                dewpoint_c=dewpoint_c,
                pace_output_state=pace_output_state,
                pace_isolation_state=pace_isolation_state,
                pace_vent_status=pace_vent_status,
                sample_count=sample_count,
                warning_code=warning_code,
                error_code=error_code,
                no_write_guard_active=self.no_write_guard_active
                if no_write_guard_active is None
                else bool(no_write_guard_active),
            )
            if event_type == "start":
                self._open_events[(resolved_stage, "" if point_index in (None, "") else str(point_index))] = dict(record)
            if event_type in {"end", "fail", "abort"}:
                self._open_events.pop((resolved_stage, "" if point_index in (None, "") else str(point_index)), None)
            self.run_dir.mkdir(parents=True, exist_ok=True)
            with self.trace_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
            return record
        except Exception:
            return {}

    def finalize_summary(
        self,
        *,
        final_decision: str = "",
        a1_final_decision: str = "",
        a2_final_decision: str = "",
        retrospective: bool = False,
        extra_context: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        return write_workflow_timing_summary(
            self.run_dir,
            final_decision=final_decision,
            a1_final_decision=a1_final_decision,
            a2_final_decision=a2_final_decision,
            retrospective=retrospective,
            extra_context=extra_context,
        )


def load_workflow_timing_events(trace_path: str | Path) -> list[dict[str, Any]]:
    path = Path(trace_path)
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except Exception:
            continue
        if isinstance(item, Mapping):
            events.append({field: item.get(field) for field in TIMING_EVENT_FIELDS})
    return events


def write_workflow_timing_summary(
    run_dir: str | Path,
    *,
    final_decision: str = "",
    a1_final_decision: str = "",
    a2_final_decision: str = "",
    retrospective: bool = False,
    extra_context: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    directory = Path(run_dir)
    trace_path = directory / WORKFLOW_TIMING_TRACE_FILENAME
    events = load_workflow_timing_events(trace_path)
    summary = build_workflow_timing_summary(
        events,
        run_id=str((extra_context or {}).get("run_id") or ""),
        final_decision=final_decision,
        a1_final_decision=a1_final_decision,
        a2_final_decision=a2_final_decision,
        retrospective=retrospective,
        extra_context=extra_context,
    )
    directory.mkdir(parents=True, exist_ok=True)
    (directory / WORKFLOW_TIMING_SUMMARY_FILENAME).write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return summary


def ensure_workflow_timing_artifacts(
    run_dir: str | Path,
    payload: Mapping[str, Any],
    *,
    retrospective: bool = False,
) -> dict[str, Any]:
    directory = Path(run_dir)
    directory.mkdir(parents=True, exist_ok=True)
    trace_path = directory / WORKFLOW_TIMING_TRACE_FILENAME
    if not trace_path.exists() or not trace_path.read_text(encoding="utf-8").strip():
        events = synthesize_timing_events_from_route_trace(directory, payload, retrospective=retrospective)
        trace_path.write_text(
            "".join(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n" for event in events),
            encoding="utf-8",
        )
    summary = write_workflow_timing_summary(
        directory,
        final_decision=str(payload.get("final_decision") or ""),
        a1_final_decision=str(payload.get("a1_final_decision") or ""),
        a2_final_decision=str(payload.get("a2_final_decision") or ""),
        retrospective=retrospective,
        extra_context=payload,
    )
    return {
        "workflow_timing_trace": str(trace_path),
        "workflow_timing_summary": str(directory / WORKFLOW_TIMING_SUMMARY_FILENAME),
        "summary_payload": summary,
    }


def synthesize_timing_events_from_route_trace(
    run_dir: str | Path,
    payload: Mapping[str, Any],
    *,
    retrospective: bool = False,
) -> list[dict[str, Any]]:
    directory = Path(run_dir)
    route_trace_path = directory / "route_trace.jsonl"
    route_rows: list[dict[str, Any]] = []
    if route_trace_path.exists():
        for line in route_trace_path.read_text(encoding="utf-8").splitlines():
            try:
                item = json.loads(line)
            except Exception:
                continue
            if isinstance(item, Mapping):
                route_rows.append(dict(item))

    timestamps = [_parse_ts(row.get("ts") or row.get("timestamp")) for row in route_rows]
    timestamps = [item for item in timestamps if item is not None]
    first_ts = timestamps[0] if timestamps else datetime.now().astimezone()
    no_write = bool(payload.get("no_write_guard_active", payload.get("no_write", False)))
    events: list[dict[str, Any]] = []
    pressure_rise_threshold = _as_float(payload.get("pressure_rise_detection_threshold_hpa")) or 2.0
    route_open_seen = False
    preseal_flush_started = False
    route_open_pressure_hpa: Optional[float] = None
    pressure_rise_seen = False

    def mono_for(row_ts: Optional[datetime]) -> float:
        if row_ts is None:
            return float(len(events)) * 0.001
        try:
            return max(0.0, (row_ts - first_ts).total_seconds())
        except Exception:
            return float(len(events)) * 0.001

    def add(
        name: str,
        event_type: str,
        *,
        row: Optional[Mapping[str, Any]] = None,
        stage: str = "",
        point_index: Any = None,
        target_pressure_hpa: Any = None,
        pressure_hpa: Any = None,
        sample_count: Any = None,
        decision: Any = None,
        error_code: Any = None,
        wait_reason: Any = None,
    ) -> None:
        row_ts = _parse_ts((row or {}).get("ts") or (row or {}).get("timestamp"))
        timestamp = row_ts or first_ts
        actual = (row or {}).get("actual")
        actual = actual if isinstance(actual, Mapping) else {}
        target = (row or {}).get("target")
        target = target if isinstance(target, Mapping) else {}
        route_state = {
            "route": (row or {}).get("route"),
            "point_tag": (row or {}).get("point_tag"),
            "result": (row or {}).get("result"),
            "retrospective": bool(retrospective),
            "derived_from": "route_trace" if row is not None else "artifact_payload",
        }
        resolved_pressure = pressure_hpa
        if resolved_pressure is None:
            for key in ("pressure_hpa", "preseal_pressure_peak_hpa", "preseal_pressure_last_hpa"):
                resolved_pressure = actual.get(key)
                if _as_float(resolved_pressure) is not None:
                    break
        resolved_target = target_pressure_hpa
        if resolved_target is None:
            resolved_target = target.get("pressure_hpa", actual.get("target_pressure_hpa"))
        events.append(
            _event_record(
                event_name=name,
                event_type=event_type,
                timestamp_local=timestamp.astimezone().isoformat(timespec="milliseconds"),
                timestamp_monotonic_s=mono_for(row_ts),
                run_start_monotonic_s=0.0,
                stage=stage,
                point_index=point_index if point_index is not None else (row or {}).get("point_index"),
                target_pressure_hpa=resolved_target,
                wait_reason=wait_reason,
                decision=decision,
                route_state=route_state,
                pressure_hpa=resolved_pressure,
                pace_output_state=actual.get("output_state"),
                pace_isolation_state=actual.get("isolation_state"),
                pace_vent_status=actual.get("vent_status_raw", actual.get("pressure_controller_vent_status")),
                sample_count=sample_count if sample_count is not None else actual.get("sample_count"),
                error_code=error_code,
                no_write_guard_active=no_write,
            )
        )

    def detect_pressure_rise(row: Mapping[str, Any], pressure_hpa: Any) -> None:
        nonlocal route_open_pressure_hpa, pressure_rise_seen
        pressure = _as_float(pressure_hpa)
        if not route_open_seen or pressure is None or pressure_rise_seen:
            return
        if route_open_pressure_hpa is None:
            route_open_pressure_hpa = float(pressure)
            return
        if float(pressure) - float(route_open_pressure_hpa) >= float(pressure_rise_threshold):
            pressure_rise_seen = True
            add(
                "pressure_rise_detected",
                "info",
                row=row,
                stage="preseal_atmosphere_flush_hold",
                pressure_hpa=pressure,
                decision="rise_detected",
            )

    add("run_start", "start")
    if not route_rows:
        add("preflight_start", "start")
        add("preflight_end", "end")
        add("artifact_finalize_start", "start")
        add("artifact_finalize_end", "end")
        add("run_end", "end")
        return events

    failure_seen = False
    for row in route_rows:
        action = str(row.get("action") or "").strip().lower()
        result = str(row.get("result") or "").strip().lower()
        actual = row.get("actual") if isinstance(row.get("actual"), Mapping) else {}
        target = row.get("target") if isinstance(row.get("target"), Mapping) else {}
        if action == "route_baseline":
            add("route_baseline_start", "start", row=row, stage="route_baseline")
            add("route_baseline_end", "end", row=row, stage="route_baseline")
        elif action == "set_vent":
            if target.get("vent_on") is True:
                if route_open_seen and not preseal_flush_started:
                    preseal_flush_started = True
                    add(
                        "preseal_atmosphere_flush_hold_start",
                        "start",
                        row=row,
                        stage="preseal_atmosphere_flush_hold",
                        pressure_hpa=actual.get("pressure_hpa"),
                        wait_reason="continuous_atmosphere_hold",
                    )
                add("pressure_atmosphere_vent_start", "start", row=row, stage="pressure_atmosphere_vent")
                add("pressure_atmosphere_vent_end", "end", row=row, stage="pressure_atmosphere_vent")
                add("preseal_vent_hold_tick", "tick", row=row, stage="preseal_soak")
                if route_open_seen:
                    add(
                        "preseal_atmosphere_flush_pressure_check",
                        "tick",
                        row=row,
                        stage="preseal_atmosphere_flush_hold",
                        pressure_hpa=actual.get("pressure_hpa"),
                        decision="vent_hold_tick",
                    )
                    detect_pressure_rise(row, actual.get("pressure_hpa"))
            elif target.get("vent_on") is False:
                message = str(row.get("message") or actual.get("reason") or "")
                if "positive" in message.lower() and "preseal" in message.lower():
                    add(
                        "positive_preseal_vent_close_start",
                        "start",
                        row=row,
                        stage="positive_preseal_vent_close",
                        target_pressure_hpa=actual.get("target_pressure_hpa"),
                        wait_reason="close_pressure_controller_atmosphere_vent",
                    )
                    add(
                        "positive_preseal_vent_close_end" if result == "ok" else "positive_preseal_vent_close_fail",
                        "end" if result == "ok" else "fail",
                        row=row,
                        stage="positive_preseal_vent_close",
                        target_pressure_hpa=actual.get("target_pressure_hpa"),
                        decision=result or "unknown",
                        pressure_hpa=actual.get("pressure_hpa"),
                        error_code=None if result == "ok" else "preseal_vent_close_failed",
                    )
        elif action == "set_co2_valves":
            route_open_seen = True
            route_open_pressure_hpa = _as_float(actual.get("pressure_hpa"))
            add("co2_route_open_start", "start", row=row, stage="co2_route_open")
            add("co2_route_open_end", "end", row=row, stage="co2_route_open")
        elif action == "wait_route_soak":
            add("preseal_soak_end", "end", row=row, stage="preseal_soak")
        elif action == "co2_preseal_atmosphere_hold_pressure_guard":
            add(
                "preseal_atmosphere_flush_pressure_check",
                "fail" if result == "fail" else "tick",
                row=row,
                stage="preseal_atmosphere_flush_hold",
                pressure_hpa=actual.get("pressure_hpa"),
                decision=result,
                error_code=actual.get("reason") if result == "fail" else None,
            )
            detect_pressure_rise(row, actual.get("pressure_hpa"))
            if result == "fail" and preseal_flush_started:
                add(
                    "preseal_atmosphere_flush_hold_end",
                    "fail",
                    row=row,
                    stage="preseal_atmosphere_flush_hold",
                    pressure_hpa=actual.get("pressure_hpa"),
                    decision="limit_exceeded",
                    error_code=actual.get("reason"),
                )
            add(
                "preseal_pressure_check",
                "fail" if result == "fail" else "info",
                row=row,
                stage="preseal_soak",
                decision=result,
                error_code=actual.get("reason") if result == "fail" else None,
            )
        elif action == "preseal_atmosphere_flush_ready_handoff":
            add(
                "preseal_atmosphere_flush_pressure_check",
                "tick",
                row=row,
                stage="preseal_atmosphere_flush_hold",
                pressure_hpa=actual.get("pressure_hpa"),
                decision="positive_preseal_ready_handoff",
            )
            detect_pressure_rise(row, actual.get("pressure_hpa"))
            add(
                "preseal_atmosphere_flush_ready_handoff",
                "info",
                row=row,
                stage="preseal_atmosphere_flush_hold",
                pressure_hpa=actual.get("pressure_hpa"),
                decision="positive_preseal_ready_handoff",
            )
            if preseal_flush_started:
                add(
                    "preseal_atmosphere_flush_hold_end",
                    "end",
                    row=row,
                    stage="preseal_atmosphere_flush_hold",
                    pressure_hpa=actual.get("pressure_hpa"),
                    decision="positive_preseal_ready_handoff",
                )
            add(
                "preseal_pressure_check",
                "info",
                row=row,
                stage="preseal_atmosphere_flush_hold",
                decision="positive_preseal_ready_handoff",
            )
        elif action == "positive_preseal_pressurization_start":
            add(
                "positive_preseal_pressurization_start",
                "start",
                row=row,
                stage="positive_preseal_pressurization",
                target_pressure_hpa=actual.get("target_pressure_hpa"),
            )
        elif action == "positive_preseal_pressure_check":
            add(
                "positive_preseal_pressure_check",
                "tick",
                row=row,
                stage="positive_preseal_pressurization",
                target_pressure_hpa=actual.get("target_pressure_hpa"),
                pressure_hpa=actual.get("pressure_hpa"),
                wait_reason="positive_preseal_pressure_rise",
            )
        elif action == "positive_preseal_ready":
            add(
                "positive_preseal_ready",
                "info",
                row=row,
                stage="positive_preseal_pressurization",
                target_pressure_hpa=actual.get("target_pressure_hpa"),
                pressure_hpa=actual.get("seal_trigger_pressure_hpa", actual.get("pressure_hpa")),
                decision="ready",
            )
            add(
                "positive_preseal_seal_start",
                "info",
                row=row,
                stage="positive_preseal_pressurization",
                target_pressure_hpa=actual.get("target_pressure_hpa"),
                pressure_hpa=actual.get("seal_trigger_pressure_hpa", actual.get("pressure_hpa")),
                wait_reason="close_co2_route_valves",
            )
        elif action == "positive_preseal_abort":
            if str(actual.get("abort_reason") or actual.get("reason") or "") == "preseal_vent_close_failed":
                add(
                    "positive_preseal_vent_close_fail",
                    "fail",
                    row=row,
                    stage="positive_preseal_vent_close",
                    target_pressure_hpa=actual.get("target_pressure_hpa"),
                    pressure_hpa=actual.get("pressure_hpa"),
                    decision="fail",
                    error_code="preseal_vent_close_failed",
                )
            add(
                "positive_preseal_abort",
                "fail",
                row=row,
                stage="positive_preseal_pressurization",
                target_pressure_hpa=actual.get("target_pressure_hpa"),
                pressure_hpa=actual.get("pressure_hpa"),
                decision="abort",
                error_code=actual.get("abort_reason") or actual.get("reason"),
            )
        elif action == "seal_route":
            if bool(actual.get("positive_preseal_pressurization_enabled")) or str(actual.get("preseal_trigger") or "") == "positive_preseal_ready":
                add(
                    "positive_preseal_seal_end",
                    "end" if result == "ok" else "fail",
                    row=row,
                    stage="positive_preseal_pressurization",
                    target_pressure_hpa=actual.get("target_pressure_hpa"),
                    pressure_hpa=actual.get("preseal_trigger_pressure_hpa", actual.get("pressure_hpa")),
                    decision=result,
                )
            add("seal_start", "start", row=row, stage="seal")
            add("seal_end", "end", row=row, stage="seal")
        elif action == "sealed_pressure_control_start":
            add(
                "positive_preseal_abort",
                "fail",
                row=row,
                stage="positive_preseal_pressurization",
                target_pressure_hpa=actual.get("target_pressure_hpa"),
                pressure_hpa=actual.get("pressure_hpa"),
                decision="sealed_pressure_below_target",
                error_code="sealed_pressure_below_target",
            )
        elif action == "set_pressure":
            add("pressure_setpoint_start", "start", row=row, stage="pressure_setpoint")
            if result == "timeout":
                add("pressure_timeout", "warning", row=row, stage="pressure_setpoint", decision="timeout")
            else:
                add("pressure_ready", "end" if result == "ok" else "warning", row=row, stage="pressure_setpoint", decision=result)
        elif action == "wait_post_pressure":
            add("wait_gate_start", "start", row=row, stage="wait_gate", wait_reason="post_pressure_hold")
            add("wait_gate_end", "end", row=row, stage="wait_gate", wait_reason="post_pressure_hold")
        elif action == "sample_start":
            add("sample_start", "start", row=row, stage="sample")
        elif action == "sample_end":
            add("sample_end", "end" if result == "ok" else "warning", row=row, stage="sample")
        elif action.startswith("final_safe_stop") or action == "restore_baseline":
            add("safe_stop_start", "start", row=row, stage="safe_stop")
            add("safe_stop_end", "end", row=row, stage="safe_stop")
        if result in {"fail", "abort", "timeout"}:
            failure_seen = True

    if failure_seen or str(payload.get("a2_final_decision") or payload.get("final_decision") or "").upper() in {"FAIL", "ABORT"}:
        add("run_fail", "fail", row=route_rows[-1], stage="run", decision=payload.get("a2_final_decision") or payload.get("final_decision"), error_code=payload.get("a2_fail_reason"))
    add("artifact_finalize_start", "start", row=route_rows[-1], stage="artifact_finalize")
    add("artifact_finalize_end", "end", row=route_rows[-1], stage="artifact_finalize")
    add("run_end", "end", row=route_rows[-1], stage="run", decision=payload.get("a2_final_decision") or payload.get("final_decision"))
    return events


def build_workflow_timing_summary(
    events: list[dict[str, Any]],
    *,
    run_id: str = "",
    final_decision: str = "",
    a1_final_decision: str = "",
    a2_final_decision: str = "",
    retrospective: bool = False,
    extra_context: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    context = dict(extra_context or {})
    resolved_run_id = str(run_id or context.get("run_id") or "")
    stage_durations: dict[str, float] = {}
    point_durations: dict[str, float] = {}
    pressure_ready_durations: dict[str, float] = {}
    wait_gate_durations: dict[str, float] = {}
    sample_durations: dict[str, float] = {}
    missing_end_events: list[dict[str, Any]] = []
    abnormal_waits: list[dict[str, Any]] = []
    timeout_events: list[dict[str, Any]] = []
    repeated_sleep_warnings: list[dict[str, Any]] = []
    state_mismatch_warnings: list[dict[str, Any]] = []
    open_events: dict[tuple[str, str], dict[str, Any]] = {}
    pressure_ready_seen_by_point: set[str] = set()
    pressure_setpoint_started_by_point: set[str] = set()
    preseal_vent_ticks: list[dict[str, Any]] = []
    preseal_pressures: list[float] = []
    positive_preseal_ready_pressure_hpa = _as_float(context.get("positive_preseal_ready_pressure_hpa"))
    positive_preseal_abort_pressure_hpa = _as_float(context.get("positive_preseal_abort_pressure_hpa"))
    positive_preseal_seal_trigger_pressure_hpa: Optional[float] = _as_float(
        context.get("positive_preseal_seal_trigger_pressure_hpa")
    )
    ambient_reference_pressure_hpa = _as_float(context.get("ambient_reference_pressure_hpa"))
    positive_preseal_pressure_max_hpa = _as_float(context.get("positive_preseal_pressure_max_hpa"))
    positive_preseal_vent_close_status: Optional[str] = None

    run_start = next((event for event in events if event.get("event_name") == "run_start"), events[0] if events else {})
    run_end = next((event for event in reversed(events) if event.get("event_name") == "run_end"), events[-1] if events else {})
    total_duration_s = _duration_between(run_start, run_end) if run_start and run_end else None

    def add_duration(target: dict[str, float], key: str, value: Any) -> None:
        duration = _round_s(value)
        if duration is None:
            return
        target[key] = _round_s(float(target.get(key, 0.0)) + float(duration)) or 0.0

    for event in events:
        name = str(event.get("event_name") or "")
        event_type = str(event.get("event_type") or "")
        stage = str(event.get("stage") or _stage_from_event_name(name))
        point = _point_key(event)
        key = (stage, point)
        pressure = _as_float(event.get("pressure_hpa"))
        if pressure is not None and "preseal" in stage:
            preseal_pressures.append(float(pressure))
        if name == "preseal_vent_hold_tick":
            preseal_vent_ticks.append(event)
        if name == "positive_preseal_vent_close_end":
            positive_preseal_vent_close_status = "PASS"
        if name == "positive_preseal_vent_close_fail":
            positive_preseal_vent_close_status = "FAIL"
        if name == "pressure_setpoint_start":
            pressure_setpoint_started_by_point.add(point)
        if name == "pressure_ready":
            pressure_ready_seen_by_point.add(point)
        if name == "sample_start" and point and point not in pressure_ready_seen_by_point:
            state_mismatch_warnings.append(
                {
                    "warning_code": "sample_before_pressure_ready",
                    "point_index": event.get("point_index"),
                    "event_name": name,
                }
            )
        if "timeout" in name or event_type == "timeout":
            timeout_events.append(_warning_payload(event, "timeout_event"))
        if event_type == "start" or (
            name.endswith(_START_SUFFIX) and event_type not in {"info", "tick", "warning"}
        ):
            open_events[key] = event
            continue
        if name == "positive_preseal_ready":
            positive_preseal_ready_pressure_hpa = _as_float(event.get("pressure_hpa")) or positive_preseal_ready_pressure_hpa
            positive_preseal_seal_trigger_pressure_hpa = (
                _as_float(event.get("pressure_hpa")) or positive_preseal_seal_trigger_pressure_hpa
            )
        if name == "positive_preseal_seal_end":
            positive_preseal_seal_trigger_pressure_hpa = positive_preseal_seal_trigger_pressure_hpa or _as_float(
                event.get("pressure_hpa")
            )
        terminal_for_stage = event_type in {"end", "fail", "abort", "timeout"} or name.endswith(_END_SUFFIX) or name in {"pressure_ready", "pressure_timeout"}
        if not terminal_for_stage:
            continue
        start_event = open_events.pop(key, None)
        duration = event.get("duration_s")
        if start_event is not None and duration is None:
            duration = _duration_between(start_event, event)
        expected_max = event.get("expected_max_s")
        if expected_max is None and start_event is not None:
            expected_max = start_event.get("expected_max_s")
        add_duration(stage_durations, stage, duration)
        if stage == "pressure_point" and point:
            add_duration(point_durations, point, duration)
        if stage == "pressure_setpoint" and point:
            add_duration(pressure_ready_durations, point, duration)
        if stage == "wait_gate" and point:
            add_duration(wait_gate_durations, point, duration)
        if stage == "sample" and point:
            add_duration(sample_durations, point, duration)
        duration_float = _as_float(duration)
        expected_float = _as_float(expected_max)
        if duration_float is not None and expected_float is not None and duration_float > expected_float:
            abnormal_waits.append(_warning_payload(event, "stage_duration_gt_expected_max", duration_s=duration_float, expected_max_s=expected_float))
        if stage == "wait_gate" and duration_float is not None and expected_float is not None and duration_float >= expected_float * 0.8:
            abnormal_waits.append(_warning_payload(event, "wait_gate_over_80pct_timeout", duration_s=duration_float, expected_max_s=expected_float))
        if stage == "pressure_setpoint" and duration_float is not None:
            threshold = expected_float * 0.8 if expected_float is not None else 120.0
            if duration_float >= threshold:
                abnormal_waits.append(_warning_payload(event, "pressure_ready_duration_long", duration_s=duration_float, expected_max_s=expected_float))

    for (stage, point), event in sorted(open_events.items()):
        if stage == "run":
            continue
        missing_end_events.append(
            {
                "stage": stage,
                "point_index": event.get("point_index"),
                "event_name": event.get("event_name"),
                "warning_code": "missing_end_event",
            }
        )

    vent_interval = _as_float(context.get("vent_hold_interval_s"))
    for left, right in zip(preseal_vent_ticks, preseal_vent_ticks[1:]):
        gap = _duration_between(left, right)
        if vent_interval is not None and gap is not None and gap > vent_interval * 2.0:
            repeated_sleep_warnings.append(
                {
                    "warning_code": "preseal_vent_tick_gap_gt_2x_interval",
                    "gap_s": gap,
                    "vent_hold_interval_s": vent_interval,
                    "point_index": right.get("point_index"),
                }
            )

    if pressure_setpoint_started_by_point:
        missing_pressure_ready = sorted(point for point in pressure_setpoint_started_by_point if point and point not in pressure_ready_seen_by_point)
        for point in missing_pressure_ready:
            state_mismatch_warnings.append(
                {
                    "warning_code": "pressure_setpoint_without_pressure_ready",
                    "point_index": _as_int(point),
                }
            )

    final_text = str(a2_final_decision or final_decision or "").strip().upper()
    if final_text in {"FAIL", "ABORT"} and not any(str(event.get("event_name") or "") in {"run_fail", "run_abort"} for event in events):
        state_mismatch_warnings.append({"warning_code": "terminal_fail_abort_without_run_fail_or_abort"})

    def first_event(name: str) -> Optional[dict[str, Any]]:
        return next((event for event in events if str(event.get("event_name") or "") == name), None)

    def last_event(name: str) -> Optional[dict[str, Any]]:
        return next((event for event in reversed(events) if str(event.get("event_name") or "") == name), None)

    def event_time(event: Optional[Mapping[str, Any]]) -> Optional[float]:
        if not event:
            return None
        return _as_float(event.get("timestamp_monotonic_s"))

    def elapsed_between(left: Optional[Mapping[str, Any]], right: Optional[Mapping[str, Any]]) -> Optional[float]:
        left_time = event_time(left)
        right_time = event_time(right)
        if left_time is None or right_time is None:
            return None
        return _round_s(right_time - left_time)

    def pressure_of(event: Optional[Mapping[str, Any]]) -> Optional[float]:
        if not event:
            return None
        return _as_float(event.get("pressure_hpa"))

    route_open_end = first_event("co2_route_open_end")
    pressure_rise_event = first_event("pressure_rise_detected")
    positive_preseal_start = first_event("positive_preseal_pressurization_start")
    positive_preseal_ready = first_event("positive_preseal_ready")
    seal_command_event = first_event("positive_preseal_seal_start")
    seal_confirm_event = first_event("positive_preseal_seal_end")
    ready_candidates = [
        event
        for event in (first_event("preseal_atmosphere_flush_ready_handoff"), positive_preseal_ready)
        if event is not None
    ]
    ready_event = min(ready_candidates, key=lambda event: event_time(event) or float("inf")) if ready_candidates else None
    route_open_to_first_pressure_rise_s = elapsed_between(route_open_end, pressure_rise_event)
    route_open_to_ready_s = elapsed_between(route_open_end, ready_event)
    positive_preseal_start_to_ready_s = elapsed_between(positive_preseal_start, positive_preseal_ready)
    ready_to_seal_command_s = elapsed_between(positive_preseal_ready, seal_command_event)
    ready_to_seal_confirm_s = elapsed_between(positive_preseal_ready, seal_confirm_event)
    vent_pressure_events = [
        event
        for event in events
        if str(event.get("event_name") or "")
        in {"preseal_atmosphere_flush_pressure_check", "preseal_vent_hold_tick", "pressure_atmosphere_vent_end"}
        and _as_float(event.get("pressure_hpa")) is not None
    ]
    positive_pressure_events = [
        event
        for event in events
        if str(event.get("event_name") or "")
        in {"positive_preseal_pressure_check", "positive_preseal_ready", "positive_preseal_abort"}
        and _as_float(event.get("pressure_hpa")) is not None
    ]
    vent_hold_pressure_rise_rate_hpa_per_s: Optional[float] = None
    if len(vent_pressure_events) >= 2:
        start_event = vent_pressure_events[0]
        end_event = vent_pressure_events[-1]
        duration = elapsed_between(start_event, end_event)
        start_pressure = pressure_of(start_event)
        end_pressure = pressure_of(end_event)
        if duration and duration > 0 and start_pressure is not None and end_pressure is not None:
            vent_hold_pressure_rise_rate_hpa_per_s = _round_s((end_pressure - start_pressure) / duration)
    positive_preseal_pressure_rise_rate_hpa_per_s: Optional[float] = None
    if positive_preseal_start_to_ready_s and positive_preseal_start_to_ready_s > 0:
        start_pressure = pressure_of(positive_preseal_start)
        if start_pressure is None and positive_pressure_events:
            start_pressure = pressure_of(positive_pressure_events[0])
        ready_pressure = pressure_of(positive_preseal_ready)
        if start_pressure is not None and ready_pressure is not None:
            positive_preseal_pressure_rise_rate_hpa_per_s = _round_s(
                (ready_pressure - start_pressure) / positive_preseal_start_to_ready_s
            )
    pressure_at_ready = pressure_of(positive_preseal_ready)
    pressure_at_seal = pressure_of(seal_confirm_event)
    pressure_max_before_seal_hpa: Optional[float] = None
    seal_confirm_time = event_time(seal_confirm_event)
    for event in positive_pressure_events + vent_pressure_events:
        timestamp = event_time(event)
        if seal_confirm_time is not None and timestamp is not None and timestamp > seal_confirm_time:
            continue
        pressure = pressure_of(event)
        if pressure is not None:
            pressure_max_before_seal_hpa = (
                pressure if pressure_max_before_seal_hpa is None else max(float(pressure_max_before_seal_hpa), pressure)
            )
    pressure_increase_after_ready_before_seal_hpa = (
        _round_s((pressure_at_seal if pressure_at_seal is not None else pressure_max_before_seal_hpa) - pressure_at_ready)
        if pressure_at_ready is not None
        and (pressure_at_seal is not None or pressure_max_before_seal_hpa is not None)
        else None
    )
    preseal_timing_warnings: list[dict[str, Any]] = []

    def add_preseal_warning(code: str, *, actual: Any = None, expected: Any = None) -> None:
        preseal_timing_warnings.append(
            {
                "warning_code": code,
                "actual": _round_s(actual),
                "expected": _round_s(expected),
                "warning_only": bool(context.get("timing_warning_only", True)),
            }
        )

    threshold_checks = [
        (
            "route_open_to_first_pressure_rise_s_long",
            route_open_to_first_pressure_rise_s,
            context.get("expected_route_open_to_first_pressure_rise_max_s"),
        ),
        ("route_open_to_ready_s_long", route_open_to_ready_s, context.get("expected_route_open_to_ready_max_s")),
        (
            "positive_preseal_start_to_ready_s_long",
            positive_preseal_start_to_ready_s,
            context.get("expected_positive_preseal_to_ready_max_s"),
        ),
        (
            "vent_hold_pressure_rise_rate_high",
            vent_hold_pressure_rise_rate_hpa_per_s,
            context.get("expected_vent_hold_pressure_rise_rate_max_hpa_per_s"),
        ),
        ("ready_to_seal_command_s_long", ready_to_seal_command_s, context.get("expected_ready_to_seal_command_max_s")),
        ("ready_to_seal_confirm_s_long", ready_to_seal_confirm_s, context.get("expected_ready_to_seal_confirm_max_s")),
        (
            "pressure_increase_after_ready_before_seal_hpa_high",
            pressure_increase_after_ready_before_seal_hpa,
            context.get("expected_max_pressure_increase_after_ready_hpa"),
        ),
    ]
    for code, actual_value, expected_value in threshold_checks:
        actual_float = _as_float(actual_value)
        expected_float = _as_float(expected_value)
        if actual_float is not None and expected_float is not None and actual_float > expected_float:
            add_preseal_warning(code, actual=actual_float, expected=expected_float)
    if (
        vent_hold_pressure_rise_rate_hpa_per_s is not None
        and _as_float(context.get("expected_vent_hold_pressure_rise_rate_max_hpa_per_s")) is not None
        and vent_hold_pressure_rise_rate_hpa_per_s
        > float(_as_float(context.get("expected_vent_hold_pressure_rise_rate_max_hpa_per_s")))
        and ready_event is None
    ):
        add_preseal_warning(
            "vent_hold_fast_pressure_rise_without_handoff",
            actual=vent_hold_pressure_rise_rate_hpa_per_s,
            expected=context.get("expected_vent_hold_pressure_rise_rate_max_hpa_per_s"),
        )
    abort_margin = (
        positive_preseal_abort_pressure_hpa - pressure_max_before_seal_hpa
        if positive_preseal_abort_pressure_hpa is not None and pressure_max_before_seal_hpa is not None
        else None
    )
    expected_abort_margin = _as_float(context.get("expected_abort_margin_min_hpa"))
    if abort_margin is not None and expected_abort_margin is not None and abort_margin <= expected_abort_margin:
        add_preseal_warning("pressure_max_before_seal_near_abort_threshold", actual=abort_margin, expected=expected_abort_margin)
    expected_tick_interval = _as_float(context.get("expected_vent_hold_tick_interval_s"))
    if expected_tick_interval is not None and expected_tick_interval > 0 and len(preseal_vent_ticks) >= 2:
        duration = elapsed_between(preseal_vent_ticks[0], preseal_vent_ticks[-1])
        if duration is not None:
            expected_count = int(duration / expected_tick_interval) + 1
            if abs(len(preseal_vent_ticks) - expected_count) > 1:
                add_preseal_warning("vent_hold_tick_count_interval_mismatch", actual=len(preseal_vent_ticks), expected=expected_count)
    if ready_event is not None and seal_command_event is None:
        add_preseal_warning("positive_preseal_ready_without_seal_start")
    if seal_command_event is not None and seal_confirm_event is None:
        add_preseal_warning("positive_preseal_seal_start_without_seal_end")

    longest_stage = _longest_from_mapping(stage_durations)
    wait_candidates = {
        "preseal_soak": stage_durations.get("preseal_soak"),
        "positive_preseal_pressurization": stage_durations.get("positive_preseal_pressurization"),
        "safe_stop": stage_durations.get("safe_stop"),
        "artifact_finalize": stage_durations.get("artifact_finalize"),
    }
    for point, duration in pressure_ready_durations.items():
        wait_candidates[f"pressure_ready:{point}"] = duration
    for point, duration in wait_gate_durations.items():
        wait_candidates[f"wait_gate:{point}"] = duration
    longest_wait = _longest_from_mapping({key: value for key, value in wait_candidates.items() if value is not None})

    return {
        "schema_version": WORKFLOW_TIMING_SCHEMA_VERSION,
        "artifact_type": "workflow_timing_summary",
        "run_id": resolved_run_id,
        "final_decision": final_decision,
        "a1_final_decision": a1_final_decision,
        "a2_final_decision": a2_final_decision,
        "retrospective": bool(retrospective),
        "total_duration_s": total_duration_s,
        "stage_durations": stage_durations,
        "point_durations": point_durations,
        "longest_stage": longest_stage,
        "longest_wait": longest_wait,
        "abnormal_waits": abnormal_waits,
        "timeout_events": timeout_events,
        "repeated_sleep_warnings": repeated_sleep_warnings,
        "missing_end_events": missing_end_events,
        "state_mismatch_warnings": state_mismatch_warnings,
        "preseal_soak_duration_s": stage_durations.get("preseal_soak"),
        "positive_preseal_duration_s": stage_durations.get("positive_preseal_pressurization"),
        "positive_preseal_vent_close_duration_s": stage_durations.get("positive_preseal_vent_close"),
        "positive_preseal_vent_close_status": positive_preseal_vent_close_status,
        "positive_preseal_ready_pressure_hpa": positive_preseal_ready_pressure_hpa,
        "positive_preseal_seal_trigger_pressure_hpa": positive_preseal_seal_trigger_pressure_hpa,
        "positive_preseal_abort_pressure_hpa": positive_preseal_abort_pressure_hpa,
        "route_open_to_first_pressure_rise_s": route_open_to_first_pressure_rise_s,
        "route_open_to_ready_s": route_open_to_ready_s,
        "positive_preseal_start_to_ready_s": positive_preseal_start_to_ready_s,
        "vent_hold_pressure_rise_rate_hpa_per_s": vent_hold_pressure_rise_rate_hpa_per_s,
        "positive_preseal_pressure_rise_rate_hpa_per_s": positive_preseal_pressure_rise_rate_hpa_per_s,
        "ready_to_seal_command_s": ready_to_seal_command_s,
        "ready_to_seal_confirm_s": ready_to_seal_confirm_s,
        "pressure_increase_after_ready_before_seal_hpa": pressure_increase_after_ready_before_seal_hpa,
        "pressure_max_before_seal_hpa": pressure_max_before_seal_hpa,
        "preseal_timing_warnings": preseal_timing_warnings,
        "preseal_timing_warning_count": len(preseal_timing_warnings),
        "temperature_chamber_settle_duration_s": stage_durations.get("temperature_chamber_settle"),
        "analyzer_chamber_temperature_stability_duration_s": stage_durations.get(
            "analyzer_chamber_temperature_stability"
        ),
        "ambient_reference_pressure_hpa": ambient_reference_pressure_hpa,
        "positive_preseal_pressure_max_hpa": positive_preseal_pressure_max_hpa
        if positive_preseal_pressure_max_hpa is not None
        else (max(preseal_pressures) if preseal_pressures else None),
        "preseal_vent_tick_count": len(preseal_vent_ticks),
        "preseal_pressure_max_hpa": max(preseal_pressures) if preseal_pressures else None,
        "pressure_ready_durations_by_point": pressure_ready_durations,
        "wait_gate_durations_by_point": wait_gate_durations,
        "sample_durations_by_point": sample_durations,
        "safe_stop_duration_s": stage_durations.get("safe_stop"),
        "artifact_finalize_duration_s": stage_durations.get("artifact_finalize"),
        "event_count": len(events),
        "not_real_acceptance_evidence": True,
    }


def _warning_payload(
    event: Mapping[str, Any],
    warning_code: str,
    *,
    duration_s: Any = None,
    expected_max_s: Any = None,
) -> dict[str, Any]:
    return {
        "warning_code": warning_code,
        "event_name": event.get("event_name"),
        "stage": event.get("stage"),
        "point_index": event.get("point_index"),
        "duration_s": _round_s(duration_s if duration_s is not None else event.get("duration_s")),
        "expected_max_s": _round_s(expected_max_s if expected_max_s is not None else event.get("expected_max_s")),
        "wait_reason": event.get("wait_reason"),
        "blocking_condition": event.get("blocking_condition"),
    }


def _longest_from_mapping(values: Mapping[str, Any]) -> dict[str, Any]:
    candidates = [(str(key), _as_float(value)) for key, value in values.items()]
    candidates = [(key, value) for key, value in candidates if value is not None]
    if not candidates:
        return {"name": None, "duration_s": None}
    name, duration = max(candidates, key=lambda item: float(item[1]))
    return {"name": name, "duration_s": _round_s(duration)}
