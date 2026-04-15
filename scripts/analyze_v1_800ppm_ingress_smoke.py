from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


REQUIRED_PRESSURES = [1000, 800, 600, 500]
TARGET_CO2_PPM = 800
PRESSURE_ORDER = {pressure: index for index, pressure in enumerate(REQUIRED_PRESSURES)}
FORBIDDEN_TRACE_STAGES = {
    "atmosphere_enter_begin": "atmosphere refresh",
    "atmosphere_enter_verified": "atmosphere refresh",
    "route_open": "route reopen",
}
PRESAMPLE_LOCK_ACTION_MAP = {
    "vent_on": "VENT 1",
    "output_enable": "OUTP ON",
    "route_reopen": "route reopen",
    "atmosphere_refresh": "atmosphere refresh",
}
PACE_DIAG_CATEGORIES = (
    "pace_vent_in_progress_suspect",
    "pace_vent_valve_left_open_suspect",
    "pace_vent_popup_only",
    "pace_isolation_state_mismatch_suspect",
    "post_isolation_ambient_ingress_suspect",
    "sealed_path_leak_suspect",
    "dead_volume_wet_release_suspect",
    "controller_hunting_suspect",
)
POINT_SUMMARY_FIELDS = [
    "point_row",
    "round_index",
    "step_index",
    "pressure_hpa",
    "point_tag",
    "status",
    "co2_mean_ppm",
    "dewpoint_mean_c",
    "h2o_mean_mmol",
    "handoff_mode",
    "pressure_in_limits",
    "outp_state",
    "isol_state",
    "capture_hold_state",
    "post_isolation_status",
    "post_isolation_diagnosis",
    "post_isolation_pressure_drift_hpa",
    "post_isolation_dewpoint_rise_c",
    "pace_outp_state_query",
    "pace_isol_state_query",
    "pace_mode_query",
    "pace_vent_status_query",
    "pace_vent_after_valve_state_query",
    "pace_vent_popup_state_query",
    "pace_oper_cond_query",
    "pace_oper_pres_cond_query",
    "pressure_gate_result",
    "dewpoint_gate_result",
    "reject_reason",
    "forbidden_pre_sampling_actions",
]


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    number = _safe_float(value)
    if number is None:
        return None
    try:
        return int(round(number))
    except Exception:
        return None


def _safe_bool(value: Any) -> Optional[bool]:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _parse_ts(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _load_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _pick_first(row: Mapping[str, Any], candidates: Sequence[str]) -> Any:
    for key in candidates:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _latest_nonempty(rows: Sequence[Mapping[str, Any]], candidates: Sequence[str]) -> Any:
    for row in reversed(rows):
        value = _pick_first(row, candidates)
        if value not in (None, ""):
            return value
    return None


def _round_label(round_index: int) -> str:
    return f"round{int(round_index)}"


def discover_run_artifacts(run_dir: Path) -> Dict[str, Optional[Path]]:
    resolved = run_dir.resolve()
    points_candidates = sorted(
        path for path in resolved.glob("points_*.csv") if not path.name.startswith("points_readable_")
    )
    aggregate_summary = resolved / "same_gas_two_round_point_summary.csv"
    return {
        "run_dir": resolved,
        "pressure_trace": resolved / "pressure_transition_trace.csv",
        "point_timing_summary": resolved / "point_timing_summary.csv",
        "points_summary": points_candidates[-1] if points_candidates else None,
        "aggregate_summary": aggregate_summary if aggregate_summary.exists() else None,
    }


def _normalize_prebuilt_summary_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "point_row": _safe_int(row.get("point_row")),
        "round_index": _safe_int(row.get("round_index")) or 1,
        "step_index": _safe_int(row.get("step_index")) or 0,
        "pressure_hpa": _safe_int(row.get("pressure_hpa")),
        "point_tag": str(row.get("point_tag") or ""),
        "status": str(row.get("status") or ""),
        "co2_mean_ppm": _safe_float(row.get("co2_mean_ppm")),
        "dewpoint_mean_c": _safe_float(row.get("dewpoint_mean_c")),
        "h2o_mean_mmol": _safe_float(row.get("h2o_mean_mmol")),
        "handoff_mode": str(row.get("handoff_mode") or ""),
        "pressure_in_limits": _safe_bool(row.get("pressure_in_limits")) or False,
        "outp_state": _safe_int(row.get("outp_state")),
        "isol_state": _safe_int(row.get("isol_state")),
        "capture_hold_state": str(row.get("capture_hold_state") or ""),
        "post_isolation_status": str(row.get("post_isolation_status") or ""),
        "post_isolation_diagnosis": str(row.get("post_isolation_diagnosis") or ""),
        "post_isolation_pressure_drift_hpa": _safe_float(row.get("post_isolation_pressure_drift_hpa")),
        "post_isolation_dewpoint_rise_c": _safe_float(row.get("post_isolation_dewpoint_rise_c")),
        "pace_outp_state_query": _safe_int(row.get("pace_outp_state_query")),
        "pace_isol_state_query": _safe_int(row.get("pace_isol_state_query")),
        "pace_mode_query": str(row.get("pace_mode_query") or ""),
        "pace_vent_status_query": _safe_int(row.get("pace_vent_status_query")),
        "pace_vent_after_valve_state_query": str(row.get("pace_vent_after_valve_state_query") or ""),
        "pace_vent_popup_state_query": str(row.get("pace_vent_popup_state_query") or ""),
        "pace_oper_cond_query": _safe_int(row.get("pace_oper_cond_query")),
        "pace_oper_pres_cond_query": _safe_int(row.get("pace_oper_pres_cond_query")),
        "pressure_gate_result": str(row.get("pressure_gate_result") or ""),
        "dewpoint_gate_result": str(row.get("dewpoint_gate_result") or ""),
        "reject_reason": str(row.get("reject_reason") or ""),
        "forbidden_pre_sampling_actions": str(row.get("forbidden_pre_sampling_actions") or ""),
    }


def _row_key(row: Mapping[str, Any]) -> Tuple[Optional[int], str]:
    return (
        _safe_int(_pick_first(row, ("point_row", "row_index"))),
        str(_pick_first(row, ("point_phase", "phase")) or "").strip().lower(),
    )


def _group_by_point(rows: Iterable[Mapping[str, Any]]) -> Dict[Tuple[Optional[int], str], List[Mapping[str, Any]]]:
    grouped: Dict[Tuple[Optional[int], str], List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[_row_key(row)].append(row)
    return grouped


def _parse_presample_lock_action(note: str) -> str:
    match = re.search(r"blocked_action=([a-z_]+)", note or "", re.IGNORECASE)
    if match:
        key = match.group(1).strip().lower()
        return PRESAMPLE_LOCK_ACTION_MAP.get(key, key)
    lowered = str(note or "").lower()
    for key, label in PRESAMPLE_LOCK_ACTION_MAP.items():
        if key in lowered:
            return label
    if "vent 1" in lowered:
        return "VENT 1"
    if "outp on" in lowered:
        return "OUTP ON"
    return "unknown"


def _trace_forbidden_actions(
    trace_rows: Sequence[Mapping[str, Any]],
    *,
    pressure_in_limits_ts: Optional[datetime],
    sampling_begin_ts: Optional[datetime],
) -> List[str]:
    actions: List[str] = []
    for row in trace_rows:
        row_ts = _parse_ts(row.get("ts"))
        if pressure_in_limits_ts is not None and row_ts is not None and row_ts < pressure_in_limits_ts:
            continue
        if sampling_begin_ts is not None and row_ts is not None and row_ts >= sampling_begin_ts:
            continue
        stage = str(row.get("trace_stage") or "").strip()
        if stage in FORBIDDEN_TRACE_STAGES:
            actions.append(FORBIDDEN_TRACE_STAGES[stage])
        elif stage == "presample_lock_violation":
            actions.append(_parse_presample_lock_action(str(row.get("note") or "")))
        vent_status = _safe_int(row.get("pace_vent_status_query"))
        if vent_status is None:
            vent_status = _safe_int(row.get("pace_vent_status"))
        if vent_status == 1:
            actions.append("VENT 1")

    deduped: List[str] = []
    seen = set()
    for action in actions:
        if not action or action in seen:
            continue
        seen.add(action)
        deduped.append(action)
    return deduped


def _point_status(
    *,
    sampling_begin_ts: Optional[datetime],
    pressure_in_limits: bool,
    reject_reason: str,
    co2_mean_ppm: Optional[float],
    capture_hold_state: str = "",
) -> str:
    if sampling_begin_ts is not None or co2_mean_ppm is not None:
        return "sampled"
    if pressure_in_limits or reject_reason or str(capture_hold_state or "").strip().lower() == "fail":
        return "rejected_before_sampling"
    return "pressure_not_stable"


def _build_point_result(
    *,
    source_row: Optional[Mapping[str, Any]],
    timing_row: Mapping[str, Any],
    point_trace_rows: Sequence[Mapping[str, Any]],
    round_index: int,
) -> Optional[Dict[str, Any]]:
    phase = str(
        _pick_first(source_row or {}, ("point_phase", "phase"))
        or _pick_first(timing_row, ("point_phase", "phase"))
        or _latest_nonempty(point_trace_rows, ("point_phase",))
        or ""
    ).strip().lower()
    if phase and phase != "co2":
        return None

    pressure_hpa = _safe_int(
        _pick_first(source_row or {}, ("pressure_target_hpa",))
        or _pick_first(timing_row, ("pressure_target_hpa",))
        or _latest_nonempty(point_trace_rows, ("pressure_target_hpa",))
    )
    if pressure_hpa not in PRESSURE_ORDER:
        return None

    target_co2_ppm = _safe_int(
        _pick_first(source_row or {}, ("target_co2_ppm", "co2_ppm"))
        or _pick_first(timing_row, ("target_co2_ppm", "co2_ppm"))
        or _latest_nonempty(point_trace_rows, ("target_co2_ppm", "co2_ppm"))
    )
    if target_co2_ppm not in (None, TARGET_CO2_PPM):
        return None

    pressure_in_limits_ts = _parse_ts(_pick_first(timing_row, ("pressure_in_limits_ts",)))
    sampling_begin_ts = _parse_ts(_pick_first(timing_row, ("sampling_begin_ts",)))
    pressure_in_limits = bool(pressure_in_limits_ts) or any(
        str(trace_row.get("trace_stage") or "").strip() in {"pressure_in_limits", "pressure_in_limits_ready_check"}
        for trace_row in point_trace_rows
    )
    capture_hold_state = str(
        _pick_first(timing_row, ("capture_hold_status",))
        or _latest_nonempty(point_trace_rows, ("capture_hold_status",))
        or ""
    )
    reject_reason = str(
        _pick_first(timing_row, ("root_cause_reject_reason",))
        or _latest_nonempty(point_trace_rows, ("root_cause_reject_reason",))
        or _pick_first(source_row or {}, ("point_quality_reason",))
        or ""
    )

    return {
        "point_row": _safe_int(
            _pick_first(source_row or {}, ("point_row",))
            or _pick_first(timing_row, ("point_row",))
            or _latest_nonempty(point_trace_rows, ("point_row",))
        ),
        "round_index": int(round_index),
        "step_index": PRESSURE_ORDER[pressure_hpa] + 1,
        "pressure_hpa": pressure_hpa,
        "point_tag": str(
            _pick_first(source_row or {}, ("point_tag",))
            or _pick_first(timing_row, ("point_tag",))
            or _latest_nonempty(point_trace_rows, ("point_tag",))
            or ""
        ),
        "status": _point_status(
            sampling_begin_ts=sampling_begin_ts,
            pressure_in_limits=pressure_in_limits,
            reject_reason=reject_reason,
            co2_mean_ppm=_safe_float(_pick_first(source_row or {}, ("co2_mean_primary_or_first", "co2_mean"))),
            capture_hold_state=capture_hold_state,
        ),
        "co2_mean_ppm": _safe_float(_pick_first(source_row or {}, ("co2_mean_primary_or_first", "co2_mean"))),
        "dewpoint_mean_c": _safe_float(_pick_first(source_row or {}, ("dewpoint_mean_c", "dewpoint_c_snapshot"))),
        "h2o_mean_mmol": _safe_float(_pick_first(source_row or {}, ("h2o_mean_primary_or_first", "h2o_mean"))),
        "handoff_mode": str(
            _pick_first(timing_row, ("handoff_mode",))
            or _latest_nonempty(point_trace_rows, ("handoff_mode",))
            or ""
        ),
        "pressure_in_limits": pressure_in_limits,
        "outp_state": _safe_int(
            _pick_first(timing_row, ("pace_output_state",))
            or _latest_nonempty(point_trace_rows, ("pace_output_state",))
        ),
        "isol_state": _safe_int(
            _pick_first(timing_row, ("pace_isolation_state",))
            or _latest_nonempty(point_trace_rows, ("pace_isolation_state",))
        ),
        "capture_hold_state": capture_hold_state,
        "post_isolation_status": str(
            _pick_first(timing_row, ("post_isolation_status",))
            or _pick_first(source_row or {}, ("post_isolation_status",))
            or _latest_nonempty(point_trace_rows, ("post_isolation_status",))
            or ""
        ),
        "post_isolation_diagnosis": str(
            _pick_first(timing_row, ("post_isolation_diagnosis",))
            or _pick_first(source_row or {}, ("post_isolation_diagnosis",))
            or _latest_nonempty(point_trace_rows, ("post_isolation_diagnosis",))
            or ""
        ),
        "post_isolation_pressure_drift_hpa": _safe_float(
            _pick_first(timing_row, ("post_isolation_pressure_drift_hpa",))
            or _pick_first(source_row or {}, ("post_isolation_pressure_drift_hpa",))
            or _latest_nonempty(point_trace_rows, ("post_isolation_pressure_drift_hpa",))
        ),
        "post_isolation_dewpoint_rise_c": _safe_float(
            _pick_first(timing_row, ("post_isolation_dewpoint_rise_c",))
            or _pick_first(source_row or {}, ("post_isolation_dewpoint_rise_c",))
            or _latest_nonempty(point_trace_rows, ("post_isolation_dewpoint_rise_c",))
        ),
        "pace_outp_state_query": _safe_int(
            _pick_first(timing_row, ("pace_outp_state_query",))
            or _pick_first(source_row or {}, ("pace_outp_state_query",))
            or _latest_nonempty(point_trace_rows, ("pace_outp_state_query",))
        ),
        "pace_isol_state_query": _safe_int(
            _pick_first(timing_row, ("pace_isol_state_query",))
            or _pick_first(source_row or {}, ("pace_isol_state_query",))
            or _latest_nonempty(point_trace_rows, ("pace_isol_state_query",))
        ),
        "pace_mode_query": str(
            _pick_first(timing_row, ("pace_mode_query",))
            or _pick_first(source_row or {}, ("pace_mode_query",))
            or _latest_nonempty(point_trace_rows, ("pace_mode_query",))
            or ""
        ),
        "pace_vent_status_query": _safe_int(
            _pick_first(timing_row, ("pace_vent_status_query",))
            or _pick_first(source_row or {}, ("pace_vent_status_query",))
            or _latest_nonempty(point_trace_rows, ("pace_vent_status_query", "pace_vent_status"))
        ),
        "pace_vent_after_valve_state_query": str(
            _pick_first(timing_row, ("pace_vent_after_valve_state_query",))
            or _pick_first(source_row or {}, ("pace_vent_after_valve_state_query",))
            or _latest_nonempty(point_trace_rows, ("pace_vent_after_valve_state_query",))
            or ""
        ),
        "pace_vent_popup_state_query": str(
            _pick_first(timing_row, ("pace_vent_popup_state_query",))
            or _pick_first(source_row or {}, ("pace_vent_popup_state_query",))
            or _latest_nonempty(point_trace_rows, ("pace_vent_popup_state_query",))
            or ""
        ),
        "pace_oper_cond_query": _safe_int(
            _pick_first(timing_row, ("pace_oper_cond_query",))
            or _pick_first(source_row or {}, ("pace_oper_cond_query",))
            or _latest_nonempty(point_trace_rows, ("pace_oper_cond_query",))
        ),
        "pace_oper_pres_cond_query": _safe_int(
            _pick_first(timing_row, ("pace_oper_pres_cond_query",))
            or _pick_first(source_row or {}, ("pace_oper_pres_cond_query",))
            or _latest_nonempty(point_trace_rows, ("pace_oper_pres_cond_query",))
        ),
        "pressure_gate_result": str(
            _latest_nonempty(point_trace_rows, ("pressure_gate_status",))
            or _pick_first(source_row or {}, ("pressure_gate_status",))
            or ""
        ),
        "dewpoint_gate_result": str(
            _pick_first(source_row or {}, ("dewpoint_gate_result",))
            or _latest_nonempty(point_trace_rows, ("dewpoint_gate_status", "dewpoint_gate_result"))
            or ""
        ),
        "reject_reason": reject_reason,
        "forbidden_pre_sampling_actions": ",".join(
            _trace_forbidden_actions(
                point_trace_rows,
                pressure_in_limits_ts=pressure_in_limits_ts,
                sampling_begin_ts=sampling_begin_ts,
            )
        ),
    }


def load_run_point_results(run_dir: Path, *, round_index: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
    artifacts = discover_run_artifacts(run_dir)
    aggregate_summary = artifacts.get("aggregate_summary")
    if aggregate_summary is not None:
        rows = [_normalize_prebuilt_summary_row(row) for row in _load_csv_rows(aggregate_summary)]
        trace_rows = (
            _load_csv_rows(artifacts["pressure_trace"])
            if artifacts["pressure_trace"] and artifacts["pressure_trace"].exists()
            else []
        )
        for row in rows:
            row["round_index"] = int(row.get("round_index") or round_index)
        return rows, trace_rows

    points_path = artifacts.get("points_summary")
    if points_path is None or not points_path.exists():
        raise FileNotFoundError(f"points summary not found under {run_dir}")

    point_rows = _load_csv_rows(points_path)
    timing_rows = (
        _load_csv_rows(artifacts["point_timing_summary"])
        if artifacts["point_timing_summary"] and artifacts["point_timing_summary"].exists()
        else []
    )
    trace_rows = (
        _load_csv_rows(artifacts["pressure_trace"])
        if artifacts["pressure_trace"] and artifacts["pressure_trace"].exists()
        else []
    )

    timing_by_point = {_row_key(row): row for row in timing_rows}
    trace_by_point = _group_by_point(trace_rows)
    points_by_key = {_row_key(row): row for row in point_rows}

    results: List[Dict[str, Any]] = []
    keys = set(points_by_key.keys()) | set(timing_by_point.keys()) | set(trace_by_point.keys())
    for key in sorted(keys, key=lambda item: ((_safe_int(item[0]) or 0), str(item[1] or ""))):
        result = _build_point_result(
            source_row=points_by_key.get(key),
            timing_row=timing_by_point.get(key, {}),
            point_trace_rows=sorted(trace_by_point.get(key, []), key=lambda item: str(item.get("ts") or "")),
            round_index=round_index,
        )
        if result is not None:
            results.append(result)

    results.sort(key=lambda item: (int(item["round_index"]), int(item["step_index"])))
    return results, trace_rows


def summarize_presample_lock_violations(trace_rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    counter: Counter[str] = Counter()
    for row in trace_rows:
        if str(row.get("trace_stage") or "").strip() != "presample_lock_violation":
            continue
        counter[_parse_presample_lock_action(str(row.get("note") or ""))] += 1
    return [{"action": action, "count": count} for action, count in sorted(counter.items())]


def summarize_reject_reasons(point_results: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    counter: Counter[str] = Counter()
    for row in point_results:
        reason = str(row.get("reject_reason") or "").strip()
        if reason:
            counter[reason] += 1
    return [{"reject_reason": reason, "count": count} for reason, count in sorted(counter.items())]


def summarize_post_isolation_diagnoses(point_results: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    counter: Counter[str] = Counter()
    for row in point_results:
        diagnosis = str(row.get("post_isolation_diagnosis") or "").strip()
        if diagnosis and diagnosis not in {"pass", "skipped"}:
            counter[diagnosis] += 1
    return [{"post_isolation_diagnosis": reason, "count": count} for reason, count in sorted(counter.items())]


def summarize_pace_post_isolation_diagnoses(point_results: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    counter: Counter[str] = Counter()
    for row in point_results:
        reject_reason = str(row.get("reject_reason") or "").strip()
        diagnosis = str(row.get("post_isolation_diagnosis") or "").strip()
        category = ""
        if reject_reason in PACE_DIAG_CATEGORIES:
            category = reject_reason
        elif diagnosis in PACE_DIAG_CATEGORIES and diagnosis not in {"pass", "skipped"}:
            category = diagnosis
        if category:
            counter[category] += 1
    return [{"diagnosis": reason, "count": count} for reason, count in sorted(counter.items())]


def _sequence_metrics(point_results: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    by_pressure: Dict[int, Mapping[str, Any]] = {
        int(row["pressure_hpa"]): row
        for row in point_results
        if row.get("pressure_hpa") in PRESSURE_ORDER and row.get("co2_mean_ppm") is not None
    }
    co2_values = [float(by_pressure[p]["co2_mean_ppm"]) for p in REQUIRED_PRESSURES if p in by_pressure]
    dew_values = [
        float(by_pressure[p]["dewpoint_mean_c"])
        for p in REQUIRED_PRESSURES
        if p in by_pressure and by_pressure[p].get("dewpoint_mean_c") is not None
    ]
    h2o_values = [
        float(by_pressure[p]["h2o_mean_mmol"])
        for p in REQUIRED_PRESSURES
        if p in by_pressure and by_pressure[p].get("h2o_mean_mmol") is not None
    ]
    co2_drop = None
    dew_rise = None
    h2o_rise = None
    if 1000 in by_pressure and 500 in by_pressure:
        co2_drop = float(by_pressure[1000]["co2_mean_ppm"]) - float(by_pressure[500]["co2_mean_ppm"])
        if by_pressure[1000].get("dewpoint_mean_c") is not None and by_pressure[500].get("dewpoint_mean_c") is not None:
            dew_rise = float(by_pressure[500]["dewpoint_mean_c"]) - float(by_pressure[1000]["dewpoint_mean_c"])
        if by_pressure[1000].get("h2o_mean_mmol") is not None and by_pressure[500].get("h2o_mean_mmol") is not None:
            h2o_rise = float(by_pressure[500]["h2o_mean_mmol"]) - float(by_pressure[1000]["h2o_mean_mmol"])
    return {
        "sampled_count": len(by_pressure),
        "co2_monotonic_down": len(co2_values) >= 2
        and all(later <= earlier + 1e-9 for earlier, later in zip(co2_values, co2_values[1:])),
        "dew_monotonic_up": len(dew_values) >= 2
        and all(later >= earlier - 1e-9 for earlier, later in zip(dew_values, dew_values[1:])),
        "h2o_monotonic_up": len(h2o_values) >= 2
        and all(later >= earlier - 1e-9 for earlier, later in zip(h2o_values, h2o_values[1:])),
        "co2_drop_ppm": co2_drop,
        "dew_rise_c": dew_rise,
        "h2o_rise_mmol": h2o_rise,
    }


def classify_ingress_result(point_results: Sequence[Mapping[str, Any]]) -> Tuple[str, Dict[str, Any]]:
    round_groups: Dict[int, List[Mapping[str, Any]]] = defaultdict(list)
    legacy_reopen_issue_count = 0
    pace_vent_in_progress_count = 0
    pace_vent_valve_left_open_count = 0
    pace_vent_popup_only_count = 0
    pace_isolation_state_mismatch_count = 0
    post_isolation_ambient_ingress_count = 0
    sealed_path_leak_count = 0
    dead_volume_wet_release_count = 0
    controller_hunting_count = 0
    forbidden_count = 0
    for row in point_results:
        round_groups[int(row.get("round_index") or 0)].append(row)
        reject_reason = str(row.get("reject_reason") or "").strip()
        diagnosis = str(row.get("post_isolation_diagnosis") or "").strip()
        if reject_reason == "ambient_ingress_suspect":
            legacy_reopen_issue_count += 1
        if reject_reason == "pace_vent_in_progress_suspect" or diagnosis == "pace_vent_in_progress_suspect":
            pace_vent_in_progress_count += 1
        if reject_reason == "pace_vent_valve_left_open_suspect" or diagnosis == "pace_vent_valve_left_open_suspect":
            pace_vent_valve_left_open_count += 1
        if diagnosis == "pace_vent_popup_only":
            pace_vent_popup_only_count += 1
        if reject_reason == "pace_isolation_state_mismatch_suspect" or diagnosis == "pace_isolation_state_mismatch_suspect":
            pace_isolation_state_mismatch_count += 1
        if reject_reason == "post_isolation_ambient_ingress_suspect" or diagnosis == "post_isolation_ambient_ingress_suspect":
            post_isolation_ambient_ingress_count += 1
        if reject_reason == "sealed_path_leak_suspect" or diagnosis == "sealed_path_leak_suspect":
            sealed_path_leak_count += 1
        if reject_reason == "dead_volume_wet_release_suspect" or diagnosis == "dead_volume_wet_release_suspect":
            dead_volume_wet_release_count += 1
        if reject_reason == "controller_hunting_suspect" or diagnosis == "controller_hunting_suspect":
            controller_hunting_count += 1
        if str(row.get("forbidden_pre_sampling_actions") or "").strip():
            forbidden_count += 1

    round_metrics = {round_index: _sequence_metrics(rows) for round_index, rows in sorted(round_groups.items())}
    strong_issue = (
        legacy_reopen_issue_count > 0
        or forbidden_count > 0
        or pace_vent_in_progress_count > 0
        or pace_vent_valve_left_open_count > 0
        or pace_isolation_state_mismatch_count > 0
        or (post_isolation_ambient_ingress_count + sealed_path_leak_count) >= 2
    )
    mild_issue = False
    sufficient_samples = True
    for metrics in round_metrics.values():
        if metrics["sampled_count"] < 3:
            sufficient_samples = False
            mild_issue = True
            continue
        co2_drop = _safe_float(metrics.get("co2_drop_ppm")) or 0.0
        dew_rise = _safe_float(metrics.get("dew_rise_c")) or 0.0
        h2o_rise = _safe_float(metrics.get("h2o_rise_mmol")) or 0.0
        if metrics["co2_monotonic_down"] and co2_drop >= 80.0:
            strong_issue = True
        elif metrics["co2_monotonic_down"] and co2_drop >= 30.0:
            mild_issue = True
        if (metrics["dew_monotonic_up"] and dew_rise >= 1.0) or (metrics["h2o_monotonic_up"] and h2o_rise >= 0.8):
            strong_issue = True
        elif (metrics["dew_monotonic_up"] and dew_rise >= 0.3) or (metrics["h2o_monotonic_up"] and h2o_rise >= 0.2):
            mild_issue = True

    if (
        post_isolation_ambient_ingress_count > 0
        or sealed_path_leak_count > 0
        or dead_volume_wet_release_count > 0
        or controller_hunting_count > 0
        or pace_vent_popup_only_count > 0
    ):
        mild_issue = True

    if strong_issue:
        conclusion = "混气仍明显存在"
    elif mild_issue or not sufficient_samples:
        conclusion = "混气明显减轻但未完全解决"
    else:
        conclusion = "混气已基本解决"

    return conclusion, {
        "legacy_reopen_issue_count": legacy_reopen_issue_count,
        "pace_vent_in_progress_count": pace_vent_in_progress_count,
        "pace_vent_valve_left_open_count": pace_vent_valve_left_open_count,
        "pace_vent_popup_only_count": pace_vent_popup_only_count,
        "pace_isolation_state_mismatch_count": pace_isolation_state_mismatch_count,
        "post_isolation_ambient_ingress_count": post_isolation_ambient_ingress_count,
        "sealed_path_leak_count": sealed_path_leak_count,
        "dead_volume_wet_release_count": dead_volume_wet_release_count,
        "controller_hunting_count": controller_hunting_count,
        "forbidden_pre_sampling_action_point_count": forbidden_count,
        "round_metrics": round_metrics,
        "sufficient_samples": sufficient_samples,
    }


def _write_csv(path: Path, fieldnames: Sequence[str], rows: Sequence[Mapping[str, Any]]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})
    return str(path)


def _write_table_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> str:
    if rows:
        fieldnames = list(rows[0].keys())
    else:
        fieldnames = ["count"]
    return _write_csv(path, fieldnames, rows)


def _plot_round_curves(
    point_results: Sequence[Mapping[str, Any]],
    trace_rows: Sequence[Mapping[str, Any]],
    *,
    output_dir: Path,
) -> Dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    plots: Dict[str, str] = {}

    round_groups: Dict[int, List[Mapping[str, Any]]] = defaultdict(list)
    for row in point_results:
        round_groups[int(row.get("round_index") or 0)].append(row)

    def _sorted_rows(rows: Sequence[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
        return sorted(rows, key=lambda item: PRESSURE_ORDER.get(int(item.get("pressure_hpa") or 0), 999))

    co2_path = output_dir / "pressure_vs_co2_rounds.png"
    fig, ax = plt.subplots(figsize=(8, 5))
    for round_index, rows in sorted(round_groups.items()):
        sorted_rows = _sorted_rows(rows)
        xs = [int(row["pressure_hpa"]) for row in sorted_rows if row.get("co2_mean_ppm") is not None]
        ys = [float(row["co2_mean_ppm"]) for row in sorted_rows if row.get("co2_mean_ppm") is not None]
        if xs:
            ax.plot(xs, ys, marker="o", label=_round_label(round_index))
    ax.set_title("Pressure vs CO2")
    ax.set_xlabel("Pressure (hPa)")
    ax.set_ylabel("CO2 (ppm)")
    ax.invert_xaxis()
    ax.grid(True, alpha=0.3)
    handles, labels = ax.get_legend_handles_labels()
    if handles:
        ax.legend()
    fig.tight_layout()
    fig.savefig(co2_path, dpi=160)
    plt.close(fig)
    plots["co2_plot"] = str(co2_path)

    dew_path = output_dir / "pressure_vs_dewpoint_h2o_rounds.png"
    fig, ax1 = plt.subplots(figsize=(8, 5))
    ax2 = ax1.twinx()
    for round_index, rows in sorted(round_groups.items()):
        sorted_rows = _sorted_rows(rows)
        dew_points = [(int(row["pressure_hpa"]), float(row["dewpoint_mean_c"])) for row in sorted_rows if row.get("dewpoint_mean_c") is not None]
        h2o_points = [(int(row["pressure_hpa"]), float(row["h2o_mean_mmol"])) for row in sorted_rows if row.get("h2o_mean_mmol") is not None]
        if dew_points:
            ax1.plot(
                [item[0] for item in dew_points],
                [item[1] for item in dew_points],
                marker="o",
                label=f"{_round_label(round_index)} dewpoint",
            )
        if h2o_points:
            ax2.plot(
                [item[0] for item in h2o_points],
                [item[1] for item in h2o_points],
                marker="s",
                linestyle="--",
                label=f"{_round_label(round_index)} H2O",
            )
    ax1.set_title("Pressure vs Dewpoint / H2O")
    ax1.set_xlabel("Pressure (hPa)")
    ax1.set_ylabel("Dewpoint (C)")
    ax2.set_ylabel("H2O (mmol)")
    ax1.invert_xaxis()
    ax1.grid(True, alpha=0.3)
    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    if handles1 or handles2:
        ax1.legend(handles1 + handles2, labels1 + labels2, loc="best")
    fig.tight_layout()
    fig.savefig(dew_path, dpi=160)
    plt.close(fig)
    plots["dewpoint_h2o_plot"] = str(dew_path)

    drift_path = output_dir / "post_isolation_pressure_drift_vs_pressure.png"
    fig, ax = plt.subplots(figsize=(8, 5))
    for round_index, rows in sorted(round_groups.items()):
        sorted_rows = _sorted_rows(rows)
        points = [
            (int(row["pressure_hpa"]), float(row["post_isolation_pressure_drift_hpa"]))
            for row in sorted_rows
            if row.get("post_isolation_pressure_drift_hpa") is not None
        ]
        if points:
            ax.plot(
                [item[0] for item in points],
                [item[1] for item in points],
                marker="o",
                label=_round_label(round_index),
            )
    ax.set_title("Post-isolation Pressure Drift vs Pressure")
    ax.set_xlabel("Pressure (hPa)")
    ax.set_ylabel("Post-isolation drift (hPa)")
    ax.invert_xaxis()
    ax.grid(True, alpha=0.3)
    handles, labels = ax.get_legend_handles_labels()
    if handles:
        ax.legend()
    fig.tight_layout()
    fig.savefig(drift_path, dpi=160)
    plt.close(fig)
    plots["post_isolation_drift_plot"] = str(drift_path)

    dew_rise_path = output_dir / "post_isolation_dewpoint_rise_vs_pressure.png"
    fig, ax = plt.subplots(figsize=(8, 5))
    for round_index, rows in sorted(round_groups.items()):
        sorted_rows = _sorted_rows(rows)
        points = [
            (int(row["pressure_hpa"]), float(row["post_isolation_dewpoint_rise_c"]))
            for row in sorted_rows
            if row.get("post_isolation_dewpoint_rise_c") is not None
        ]
        if points:
            ax.plot(
                [item[0] for item in points],
                [item[1] for item in points],
                marker="o",
                label=_round_label(round_index),
            )
    ax.set_title("Post-isolation Dewpoint Rise vs Pressure")
    ax.set_xlabel("Pressure (hPa)")
    ax.set_ylabel("Post-isolation dewpoint rise (C)")
    ax.invert_xaxis()
    ax.grid(True, alpha=0.3)
    handles, labels = ax.get_legend_handles_labels()
    if handles:
        ax.legend()
    fig.tight_layout()
    fig.savefig(dew_rise_path, dpi=160)
    plt.close(fig)
    plots["post_isolation_dewpoint_rise_plot"] = str(dew_rise_path)

    vent_path = output_dir / "pace_vent_status_timeline.png"
    fig, ax = plt.subplots(figsize=(9, 5))
    trace_groups: Dict[int, List[Mapping[str, Any]]] = defaultdict(list)
    for row in trace_rows:
        trace_groups[int(row.get("round_index") or 0)].append(row)
    if not trace_groups:
        trace_groups[0] = list(trace_rows)
    for round_index, rows in sorted(trace_groups.items()):
        series: List[Tuple[int, int]] = []
        for idx, row in enumerate(rows):
            vent_status = _safe_int(row.get("pace_vent_status_query"))
            if vent_status is None:
                vent_status = _safe_int(row.get("pace_vent_status"))
            if vent_status is None:
                continue
            series.append((idx, int(vent_status)))
        if series:
            ax.step(
                [item[0] for item in series],
                [item[1] for item in series],
                where="post",
                label=_round_label(round_index) if round_index else "trace",
            )
    ax.set_title("PACE Vent Status Timeline")
    ax.set_xlabel("Trace sample index")
    ax.set_ylabel("PACE vent status")
    ax.grid(True, alpha=0.3)
    handles, labels = ax.get_legend_handles_labels()
    if handles:
        ax.legend()
    fig.tight_layout()
    fig.savefig(vent_path, dpi=160)
    plt.close(fig)
    plots["pace_vent_status_timeline_plot"] = str(vent_path)

    return plots


def analyze_runs(run_dirs: Sequence[Path | str], *, output_dir: Path | str) -> Dict[str, Any]:
    if not run_dirs:
        raise ValueError("at least one run directory is required")

    resolved_run_dirs = [Path(run_dir).resolve() for run_dir in run_dirs]
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    all_point_results: List[Dict[str, Any]] = []
    all_trace_rows: List[Dict[str, Any]] = []
    for round_index, run_dir in enumerate(resolved_run_dirs, start=1):
        point_results, trace_rows = load_run_point_results(run_dir, round_index=round_index)
        for row in point_results:
            row["round_index"] = round_index
            all_point_results.append(dict(row))
        for row in trace_rows:
            enriched = dict(row)
            enriched["round_index"] = round_index
            all_trace_rows.append(enriched)

    all_point_results.sort(key=lambda item: (int(item.get("round_index") or 0), int(item.get("step_index") or 0)))
    presample_lock_violations = summarize_presample_lock_violations(all_trace_rows)
    reject_reasons = summarize_reject_reasons(all_point_results)
    post_isolation_diagnoses = summarize_post_isolation_diagnoses(all_point_results)
    pace_post_isolation_diagnoses = summarize_pace_post_isolation_diagnoses(all_point_results)
    conclusion, metrics = classify_ingress_result(all_point_results)
    plots = _plot_round_curves(all_point_results, all_trace_rows, output_dir=output_dir)

    point_summary_csv = _write_csv(output_dir / "same_gas_two_round_point_summary.csv", POINT_SUMMARY_FIELDS, all_point_results)
    presample_lock_violations_csv = _write_table_csv(output_dir / "presample_lock_violations.csv", presample_lock_violations)
    reject_reason_summary_csv = _write_table_csv(output_dir / "reject_reason_summary.csv", reject_reasons)
    post_isolation_diagnosis_summary_csv = _write_table_csv(
        output_dir / "post_isolation_diagnosis_summary.csv",
        post_isolation_diagnoses,
    )
    pace_post_isolation_diagnosis_summary_csv = _write_table_csv(
        output_dir / "pace_post_isolation_diagnosis_summary.csv",
        pace_post_isolation_diagnoses,
    )

    summary_payload = {
        "run_dirs": [str(path) for path in resolved_run_dirs],
        "output_dir": str(output_dir),
        "point_summary_csv": point_summary_csv,
        "presample_lock_violations_csv": presample_lock_violations_csv,
        "reject_reason_summary_csv": reject_reason_summary_csv,
        "post_isolation_diagnosis_summary_csv": post_isolation_diagnosis_summary_csv,
        "pace_post_isolation_diagnosis_summary_csv": pace_post_isolation_diagnosis_summary_csv,
        "plots": plots,
        "point_results": all_point_results,
        "metrics": metrics,
        "conclusion": conclusion,
    }
    summary_path = output_dir / "same_gas_two_round_summary.json"
    summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    summary_payload["summary_json"] = str(summary_path)
    return summary_payload


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze V1 800 ppm ingress smoke traces.")
    parser.add_argument("run_dirs", nargs="+", help="One or more run output directories.")
    parser.add_argument("--output-dir", required=True, help="Directory to write plots and summaries.")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    summary = analyze_runs(args.run_dirs, output_dir=args.output_dir)
    print(json.dumps({"conclusion": summary["conclusion"], "summary_json": summary["summary_json"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
