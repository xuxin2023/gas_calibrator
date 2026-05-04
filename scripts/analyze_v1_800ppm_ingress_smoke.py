from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception:  # pragma: no cover - optional dependency in engineering envs
    matplotlib = None
    plt = None


REQUIRED_PRESSURES = [1000, 800, 600, 500]
TARGET_CO2_PPM = 800
BASELINE_TOLERANCE_PPM = 120.0
NEAR_AMBIENT_PRESSURE_MIN_HPA = 980
NEAR_AMBIENT_PRESSURE_MAX_HPA = 1020
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
    "controller_side_state_only_without_analyzer_side_effect",
    "pace_legacy_vent_state_3_suspect",
    "pace_atmosphere_connected_latched_state_suspect",
    "pace_vent_completed_latched_suspect",
    "pace_effort_nonzero_after_output_off_suspect",
    "pace_supply_vacuum_compensation_suspect",
    "pace_vent_after_valve_config_open_suspect",
    "pace_vent_valve_left_open_suspect",
    "pace_protective_vent_suspect",
    "pace_vent_popup_only",
    "pace_vent_popup_stale_suspect",
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
    "target_co2_ppm",
    "co2_mean_ppm",
    "dewpoint_mean_c",
    "h2o_mean_mmol",
    "handoff_mode",
    "pace_upstream_check_valve_installed",
    "post_isolation_pressure_truth_source",
    "pressure_in_limits",
    "outp_state",
    "isol_state",
    "capture_hold_state",
    "post_isolation_status",
    "post_isolation_diagnosis",
    "post_isolation_capture_mode",
    "post_isolation_fast_capture_status",
    "post_isolation_fast_capture_reason",
    "post_isolation_fast_capture_elapsed_s",
    "post_isolation_fast_capture_fallback",
    "post_isolation_pressure_drift_hpa",
    "post_isolation_dewpoint_rise_c",
    "pace_outp_state_query",
    "pace_isol_state_query",
    "pace_mode_query",
    "pace_vent_status_query",
    "pace_legacy_vent_state_3_suspect",
    "pace_vent_completed_latched",
    "pace_vent_clear_attempted",
    "pace_vent_clear_result",
    "pace_vent_after_valve_state_query",
    "pace_vent_popup_state_query",
    "pace_vent_elapsed_time_query",
    "pace_vent_orpv_state_query",
    "pace_vent_pupv_state_query",
    "pace_oper_cond_query",
    "pace_oper_pres_cond_query",
    "pace_effort_query",
    "pace_comp1_query",
    "pace_comp2_query",
    "pace_sens_pres_cont_query",
    "pace_sens_pres_bar_query",
    "pace_sens_pres_inl_query",
    "pace_sens_pres_inl_state_query",
    "pace_sens_pres_inl_time_query",
    "pace_sens_inl_query",
    "pace_sens_inl_time_query",
    "pace_sens_slew_query",
    "pace_oper_pres_even_query",
    "pace_oper_pres_vent_complete_bit",
    "pace_oper_pres_in_limits_bit",
    "baseline_sanity_gate_status",
    "baseline_sanity_gate_reason",
    "baseline_sanity_target_co2_ppm",
    "baseline_sanity_plateau_mean_ppm",
    "baseline_sanity_plateau_span_ppm",
    "baseline_sanity_plateau_count",
    "pressure_gate_result",
    "dewpoint_gate_result",
    "reject_reason",
    "forbidden_pre_sampling_actions",
]

POINT_PHASE_FIELDS = ("point_phase", "phase", "step", "gas_type", "流程阶段", "气体类型", "步骤")
POINT_ROW_FIELDS = ("point_row", "row_index", "校准点行号", "点位编号")
POINT_TAG_FIELDS = ("point_tag", "点位标签")
PRESSURE_FIELDS = ("pressure_hpa", "pressure_target_hpa", "target_pressure_hpa", "目标压力hPa")
TARGET_CO2_FIELDS = ("target_co2_ppm", "co2_ppm_target", "co2_ppm", "目标二氧化碳浓度ppm", "目标值")
CO2_MEAN_FIELDS = ("co2_mean_ppm", "co2_mean_primary_or_first", "co2_mean", "二氧化碳平均值", "测量值")
DEWPOINT_MEAN_FIELDS = ("dewpoint_mean_c", "露点仪露点C_平均值")
H2O_MEAN_FIELDS = ("h2o_mean_mmol", "h2o_mean_primary_or_first", "h2o_mean", "水平均值")


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


def _is_near_ambient_pressure(pressure_hpa: Any) -> bool:
    pressure_value = _safe_int(pressure_hpa)
    if pressure_value is None:
        return False
    return NEAR_AMBIENT_PRESSURE_MIN_HPA <= pressure_value <= NEAR_AMBIENT_PRESSURE_MAX_HPA


def _checkvalve_mode_active(row: Mapping[str, Any]) -> bool:
    return _safe_bool(row.get("pace_upstream_check_valve_installed")) is True


def _checkvalve_pressure_drift_failure(row: Mapping[str, Any]) -> bool:
    diagnosis = str(row.get("post_isolation_diagnosis") or "").strip()
    drift_hpa = _safe_float(row.get("post_isolation_pressure_drift_hpa"))
    return diagnosis in {
        "post_isolation_ambient_ingress_suspect",
        "sealed_path_leak_suspect",
        "pace_vent_valve_left_open_suspect",
        "pace_protective_vent_suspect",
    } or (drift_hpa is not None and abs(float(drift_hpa)) >= 0.18)


def _checkvalve_dewpoint_failure(row: Mapping[str, Any]) -> bool:
    diagnosis = str(row.get("post_isolation_diagnosis") or "").strip()
    dew_rise_c = _safe_float(row.get("post_isolation_dewpoint_rise_c"))
    return diagnosis == "dead_volume_wet_release_suspect" or (
        dew_rise_c is not None and float(dew_rise_c) >= 0.06
    )


def _checkvalve_controller_side_only(row: Mapping[str, Any]) -> bool:
    diagnosis = str(row.get("post_isolation_diagnosis") or "").strip()
    if diagnosis not in {
        "controller_side_state_only_without_analyzer_side_effect",
        "pace_legacy_vent_state_3_suspect",
        "pace_atmosphere_connected_latched_state_suspect",
        "pace_vent_completed_latched_suspect",
    }:
        return False
    if _checkvalve_pressure_drift_failure(row) or _checkvalve_dewpoint_failure(row):
        return False
    return str(row.get("reject_reason") or "").strip() == ""


def _co2_like_row(row: Mapping[str, Any]) -> bool:
    phase = str(_pick_first(row, POINT_PHASE_FIELDS) or "").strip().lower()
    if phase in {"co2", "carbon_dioxide"}:
        return True
    return _pick_first(row, TARGET_CO2_FIELDS) not in (None, "")


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
    normalized = {
        "point_row": _safe_int(_pick_first(row, POINT_ROW_FIELDS)),
        "round_index": _safe_int(row.get("round_index")) or 1,
        "step_index": _safe_int(row.get("step_index")) or 0,
        "pressure_hpa": _safe_int(_pick_first(row, PRESSURE_FIELDS)),
        "point_tag": str(_pick_first(row, POINT_TAG_FIELDS) or ""),
        "status": str(row.get("status") or ""),
        "target_co2_ppm": _safe_float(_pick_first(row, TARGET_CO2_FIELDS)),
        "co2_mean_ppm": _safe_float(_pick_first(row, CO2_MEAN_FIELDS)),
        "dewpoint_mean_c": _safe_float(_pick_first(row, DEWPOINT_MEAN_FIELDS)),
        "h2o_mean_mmol": _safe_float(_pick_first(row, H2O_MEAN_FIELDS)),
        "handoff_mode": str(row.get("handoff_mode") or ""),
        "pace_upstream_check_valve_installed": _safe_bool(row.get("pace_upstream_check_valve_installed")) or False,
        "post_isolation_pressure_truth_source": str(row.get("post_isolation_pressure_truth_source") or ""),
        "pressure_in_limits": _safe_bool(row.get("pressure_in_limits")) or False,
        "outp_state": _safe_int(_pick_first(row, ("outp_state", "pace_output_state", "pace_outp_state_query"))),
        "isol_state": _safe_int(_pick_first(row, ("isol_state", "pace_isolation_state", "pace_isol_state_query"))),
        "capture_hold_state": str(_pick_first(row, ("capture_hold_state", "capture_hold_status")) or ""),
        "post_isolation_status": str(row.get("post_isolation_status") or ""),
        "post_isolation_diagnosis": str(row.get("post_isolation_diagnosis") or ""),
        "post_isolation_capture_mode": str(row.get("post_isolation_capture_mode") or ""),
        "post_isolation_fast_capture_status": str(row.get("post_isolation_fast_capture_status") or ""),
        "post_isolation_fast_capture_reason": str(row.get("post_isolation_fast_capture_reason") or ""),
        "post_isolation_fast_capture_elapsed_s": _safe_float(row.get("post_isolation_fast_capture_elapsed_s")),
        "post_isolation_fast_capture_fallback": _safe_bool(row.get("post_isolation_fast_capture_fallback")) or False,
        "post_isolation_pressure_drift_hpa": _safe_float(row.get("post_isolation_pressure_drift_hpa")),
        "post_isolation_dewpoint_rise_c": _safe_float(row.get("post_isolation_dewpoint_rise_c")),
        "pace_outp_state_query": _safe_int(row.get("pace_outp_state_query")),
        "pace_isol_state_query": _safe_int(row.get("pace_isol_state_query")),
        "pace_mode_query": str(row.get("pace_mode_query") or ""),
        "pace_vent_status_query": _safe_int(row.get("pace_vent_status_query")),
        "pace_legacy_vent_state_3_suspect": _safe_bool(row.get("pace_legacy_vent_state_3_suspect")) or False,
        "pace_vent_completed_latched": _safe_bool(row.get("pace_vent_completed_latched")),
        "pace_vent_clear_attempted": _safe_bool(row.get("pace_vent_clear_attempted")) or False,
        "pace_vent_clear_result": str(row.get("pace_vent_clear_result") or ""),
        "pace_vent_after_valve_state_query": str(row.get("pace_vent_after_valve_state_query") or ""),
        "pace_vent_popup_state_query": str(row.get("pace_vent_popup_state_query") or ""),
        "pace_vent_elapsed_time_query": _safe_float(row.get("pace_vent_elapsed_time_query")),
        "pace_vent_orpv_state_query": str(row.get("pace_vent_orpv_state_query") or ""),
        "pace_vent_pupv_state_query": str(row.get("pace_vent_pupv_state_query") or ""),
        "pace_oper_cond_query": _safe_int(row.get("pace_oper_cond_query")),
        "pace_oper_pres_cond_query": _safe_int(row.get("pace_oper_pres_cond_query")),
        "pace_effort_query": _safe_float(row.get("pace_effort_query")),
        "pace_comp1_query": _safe_float(row.get("pace_comp1_query")),
        "pace_comp2_query": _safe_float(row.get("pace_comp2_query")),
        "pace_sens_pres_cont_query": _safe_float(row.get("pace_sens_pres_cont_query")),
        "pace_sens_pres_bar_query": _safe_float(row.get("pace_sens_pres_bar_query")),
        "pace_sens_pres_inl_query": _safe_float(row.get("pace_sens_pres_inl_query")),
        "pace_sens_pres_inl_state_query": _safe_int(row.get("pace_sens_pres_inl_state_query")),
        "pace_sens_pres_inl_time_query": _safe_float(row.get("pace_sens_pres_inl_time_query")),
        "pace_sens_inl_query": _safe_float(row.get("pace_sens_inl_query")),
        "pace_sens_inl_time_query": _safe_float(row.get("pace_sens_inl_time_query")),
        "pace_sens_slew_query": _safe_float(row.get("pace_sens_slew_query")),
        "pace_oper_pres_even_query": _safe_int(row.get("pace_oper_pres_even_query")),
        "pace_oper_pres_vent_complete_bit": _safe_bool(row.get("pace_oper_pres_vent_complete_bit")),
        "pace_oper_pres_in_limits_bit": _safe_bool(row.get("pace_oper_pres_in_limits_bit")),
        "baseline_sanity_gate_status": str(row.get("baseline_sanity_gate_status") or ""),
        "baseline_sanity_gate_reason": str(row.get("baseline_sanity_gate_reason") or ""),
        "baseline_sanity_target_co2_ppm": _safe_float(row.get("baseline_sanity_target_co2_ppm")),
        "baseline_sanity_plateau_mean_ppm": _safe_float(row.get("baseline_sanity_plateau_mean_ppm")),
        "baseline_sanity_plateau_span_ppm": _safe_float(row.get("baseline_sanity_plateau_span_ppm")),
        "baseline_sanity_plateau_count": _safe_int(row.get("baseline_sanity_plateau_count")),
        "pressure_gate_result": str(row.get("pressure_gate_result") or ""),
        "dewpoint_gate_result": str(row.get("dewpoint_gate_result") or ""),
        "reject_reason": str(_pick_first(row, ("reject_reason", "root_cause_reject_reason", "point_quality_reason")) or ""),
        "forbidden_pre_sampling_actions": str(row.get("forbidden_pre_sampling_actions") or ""),
    }
    if not normalized["status"]:
        normalized["status"] = _point_status(
            co2_mean_ppm=normalized["co2_mean_ppm"],
            sampling_begin_ts=_pick_first(row, ("sampling_begin_ts",)),
            capture_hold_state=normalized["capture_hold_state"],
            post_isolation_status=normalized["post_isolation_status"],
            reject_reason=normalized["reject_reason"],
            post_isolation_diagnosis=normalized["post_isolation_diagnosis"],
        )
    return normalized


def _row_key(row: Mapping[str, Any]) -> Tuple[Optional[int], str]:
    phase = str(_pick_first(row, POINT_PHASE_FIELDS) or "").strip().lower()
    if not phase and _co2_like_row(row):
        phase = "co2"
    return (_safe_int(_pick_first(row, POINT_ROW_FIELDS)), phase)


def _group_by_point(rows: Iterable[Mapping[str, Any]]) -> Dict[Tuple[Optional[int], str], List[Mapping[str, Any]]]:
    grouped: Dict[Tuple[Optional[int], str], List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        key = _row_key(row)
        if key[0] is None:
            continue
        grouped[key].append(row)
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
        if action and action not in seen:
            seen.add(action)
            deduped.append(action)
    return deduped


def _parse_round_index(point_tag: str, fallback: int) -> int:
    text = str(point_tag or "")
    match = re.search(r"\bround[_-]?(\d+)\b", text, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return int(fallback)


def _point_status(
    *,
    co2_mean_ppm: Optional[float],
    sampling_begin_ts: Any,
    capture_hold_state: str,
    post_isolation_status: str,
    reject_reason: str,
    post_isolation_diagnosis: str,
) -> str:
    if co2_mean_ppm is not None or str(sampling_begin_ts or "").strip():
        return "sampled"
    if reject_reason or capture_hold_state.lower() == "fail" or post_isolation_status.lower() == "fail":
        return "rejected_before_sampling"
    if post_isolation_diagnosis and post_isolation_diagnosis not in {"pass", "pace_vent_popup_only", "pace_vent_popup_stale_suspect"}:
        return "rejected_before_sampling"
    return "unknown"


def _build_point_result(
    *,
    sample_rows: Sequence[Mapping[str, Any]],
    timing_rows: Sequence[Mapping[str, Any]],
    trace_rows: Sequence[Mapping[str, Any]],
    round_index_fallback: int,
) -> Dict[str, Any]:
    rows = list(sample_rows) + list(timing_rows) + list(trace_rows)
    row = rows[0] if rows else {}
    point_row = _safe_int(_pick_first(row, POINT_ROW_FIELDS))
    point_tag = str(_latest_nonempty(rows, POINT_TAG_FIELDS) or "")
    pressure_hpa = _safe_int(_latest_nonempty(rows, PRESSURE_FIELDS))
    round_index = _safe_int(_latest_nonempty(rows, ("round_index",))) or _parse_round_index(point_tag, round_index_fallback)
    step_index = _safe_int(_latest_nonempty(rows, ("step_index",)))
    if step_index is None:
        step_index = (PRESSURE_ORDER.get(pressure_hpa, point_row or 0) + 1) if pressure_hpa is not None else (point_row or 0)
    pressure_in_limits_ts = _latest_nonempty(timing_rows, ("pressure_in_limits_ts",))
    sampling_begin_ts = _latest_nonempty(timing_rows, ("sampling_begin_ts",))
    if sampling_begin_ts in (None, ""):
        for trace_row in trace_rows:
            if str(trace_row.get("trace_stage") or "").strip() == "sampling_begin":
                sampling_begin_ts = trace_row.get("ts")
                break
    forbidden_actions = _trace_forbidden_actions(
        trace_rows,
        pressure_in_limits_ts=_parse_ts(pressure_in_limits_ts),
        sampling_begin_ts=_parse_ts(sampling_begin_ts),
    )
    explicit_forbidden = str(_latest_nonempty(rows, ("forbidden_pre_sampling_actions",)) or "").strip()
    if explicit_forbidden:
        for item in re.split(r"[;,|]", explicit_forbidden):
            label = item.strip()
            if label and label not in forbidden_actions:
                forbidden_actions.append(label)
    capture_hold_state = str(_latest_nonempty(rows, ("capture_hold_state", "capture_hold_status")) or "")
    post_isolation_status = str(_latest_nonempty(rows, ("post_isolation_status",)) or "")
    post_isolation_diagnosis = str(_latest_nonempty(rows, ("post_isolation_diagnosis",)) or "")
    reject_reason = str(
        _latest_nonempty(rows, ("reject_reason", "root_cause_reject_reason", "point_quality_reason")) or ""
    )
    if not reject_reason and post_isolation_diagnosis not in {"", "pass", "pace_vent_popup_only", "pace_vent_popup_stale_suspect"}:
        reject_reason = post_isolation_diagnosis
    if not reject_reason and capture_hold_state.lower() == "fail":
        reject_reason = "capture_hold_failed"
    co2_mean_ppm = _safe_float(_latest_nonempty(sample_rows, CO2_MEAN_FIELDS))
    dewpoint_mean_c = _safe_float(_latest_nonempty(sample_rows, DEWPOINT_MEAN_FIELDS))
    h2o_mean_mmol = _safe_float(_latest_nonempty(sample_rows, H2O_MEAN_FIELDS))
    status = _point_status(
        co2_mean_ppm=co2_mean_ppm,
        sampling_begin_ts=sampling_begin_ts,
        capture_hold_state=capture_hold_state,
        post_isolation_status=post_isolation_status,
        reject_reason=reject_reason,
        post_isolation_diagnosis=post_isolation_diagnosis,
    )
    return {
        "point_row": point_row,
        "round_index": round_index,
        "step_index": step_index,
        "pressure_hpa": pressure_hpa,
        "point_tag": point_tag,
        "status": status,
        "target_co2_ppm": _safe_float(_latest_nonempty(rows, TARGET_CO2_FIELDS)),
        "co2_mean_ppm": co2_mean_ppm,
        "dewpoint_mean_c": dewpoint_mean_c,
        "h2o_mean_mmol": h2o_mean_mmol,
        "handoff_mode": str(_latest_nonempty(rows, ("handoff_mode",)) or ""),
        "pace_upstream_check_valve_installed": _safe_bool(
            _latest_nonempty(rows, ("pace_upstream_check_valve_installed",))
        )
        or False,
        "post_isolation_pressure_truth_source": str(
            _latest_nonempty(rows, ("post_isolation_pressure_truth_source",)) or ""
        ),
        "pressure_in_limits": bool(str(pressure_in_limits_ts or "").strip()),
        "outp_state": _safe_int(_latest_nonempty(rows, ("outp_state", "pace_outp_state_query", "pace_output_state"))),
        "isol_state": _safe_int(_latest_nonempty(rows, ("isol_state", "pace_isol_state_query", "pace_isolation_state"))),
        "capture_hold_state": capture_hold_state,
        "post_isolation_status": post_isolation_status,
        "post_isolation_diagnosis": post_isolation_diagnosis,
        "post_isolation_capture_mode": str(_latest_nonempty(rows, ("post_isolation_capture_mode",)) or ""),
        "post_isolation_fast_capture_status": str(_latest_nonempty(rows, ("post_isolation_fast_capture_status",)) or ""),
        "post_isolation_fast_capture_reason": str(_latest_nonempty(rows, ("post_isolation_fast_capture_reason",)) or ""),
        "post_isolation_fast_capture_elapsed_s": _safe_float(_latest_nonempty(rows, ("post_isolation_fast_capture_elapsed_s",))),
        "post_isolation_fast_capture_fallback": _safe_bool(_latest_nonempty(rows, ("post_isolation_fast_capture_fallback",)))
        or False,
        "post_isolation_pressure_drift_hpa": _safe_float(_latest_nonempty(rows, ("post_isolation_pressure_drift_hpa",))),
        "post_isolation_dewpoint_rise_c": _safe_float(_latest_nonempty(rows, ("post_isolation_dewpoint_rise_c",))),
        "pace_outp_state_query": _safe_int(_latest_nonempty(rows, ("pace_outp_state_query",))),
        "pace_isol_state_query": _safe_int(_latest_nonempty(rows, ("pace_isol_state_query",))),
        "pace_mode_query": str(_latest_nonempty(rows, ("pace_mode_query",)) or ""),
        "pace_vent_status_query": _safe_int(_latest_nonempty(rows, ("pace_vent_status_query", "pace_vent_status"))),
        "pace_legacy_vent_state_3_suspect": _safe_bool(
            _latest_nonempty(rows, ("pace_legacy_vent_state_3_suspect",))
        )
        or False,
        "pace_vent_completed_latched": _safe_bool(_latest_nonempty(rows, ("pace_vent_completed_latched",))),
        "pace_vent_clear_attempted": _safe_bool(_latest_nonempty(rows, ("pace_vent_clear_attempted",))) or False,
        "pace_vent_clear_result": str(_latest_nonempty(rows, ("pace_vent_clear_result",)) or ""),
        "pace_vent_after_valve_state_query": str(_latest_nonempty(rows, ("pace_vent_after_valve_state_query",)) or ""),
        "pace_vent_popup_state_query": str(_latest_nonempty(rows, ("pace_vent_popup_state_query",)) or ""),
        "pace_vent_elapsed_time_query": _safe_float(_latest_nonempty(rows, ("pace_vent_elapsed_time_query",))),
        "pace_vent_orpv_state_query": str(_latest_nonempty(rows, ("pace_vent_orpv_state_query",)) or ""),
        "pace_vent_pupv_state_query": str(_latest_nonempty(rows, ("pace_vent_pupv_state_query",)) or ""),
        "pace_oper_cond_query": _safe_int(_latest_nonempty(rows, ("pace_oper_cond_query",))),
        "pace_oper_pres_cond_query": _safe_int(_latest_nonempty(rows, ("pace_oper_pres_cond_query",))),
        "pace_effort_query": _safe_float(_latest_nonempty(rows, ("pace_effort_query",))),
        "pace_comp1_query": _safe_float(_latest_nonempty(rows, ("pace_comp1_query",))),
        "pace_comp2_query": _safe_float(_latest_nonempty(rows, ("pace_comp2_query",))),
        "pace_sens_pres_cont_query": _safe_float(_latest_nonempty(rows, ("pace_sens_pres_cont_query",))),
        "pace_sens_pres_bar_query": _safe_float(_latest_nonempty(rows, ("pace_sens_pres_bar_query",))),
        "pace_sens_pres_inl_query": _safe_float(_latest_nonempty(rows, ("pace_sens_pres_inl_query",))),
        "pace_sens_pres_inl_state_query": _safe_int(_latest_nonempty(rows, ("pace_sens_pres_inl_state_query",))),
        "pace_sens_pres_inl_time_query": _safe_float(_latest_nonempty(rows, ("pace_sens_pres_inl_time_query",))),
        "pace_sens_inl_query": _safe_float(_latest_nonempty(rows, ("pace_sens_inl_query", "pace_sens_pres_inl_query"))),
        "pace_sens_inl_time_query": _safe_float(
            _latest_nonempty(rows, ("pace_sens_inl_time_query", "pace_sens_pres_inl_time_query"))
        ),
        "pace_sens_slew_query": _safe_float(_latest_nonempty(rows, ("pace_sens_slew_query",))),
        "pace_oper_pres_even_query": _safe_int(_latest_nonempty(rows, ("pace_oper_pres_even_query",))),
        "pace_oper_pres_vent_complete_bit": _safe_bool(_latest_nonempty(rows, ("pace_oper_pres_vent_complete_bit",))),
        "pace_oper_pres_in_limits_bit": _safe_bool(_latest_nonempty(rows, ("pace_oper_pres_in_limits_bit",))),
        "baseline_sanity_gate_status": str(_latest_nonempty(rows, ("baseline_sanity_gate_status",)) or ""),
        "baseline_sanity_gate_reason": str(_latest_nonempty(rows, ("baseline_sanity_gate_reason",)) or ""),
        "baseline_sanity_target_co2_ppm": _safe_float(_latest_nonempty(rows, ("baseline_sanity_target_co2_ppm",))),
        "baseline_sanity_plateau_mean_ppm": _safe_float(_latest_nonempty(rows, ("baseline_sanity_plateau_mean_ppm",))),
        "baseline_sanity_plateau_span_ppm": _safe_float(_latest_nonempty(rows, ("baseline_sanity_plateau_span_ppm",))),
        "baseline_sanity_plateau_count": _safe_int(_latest_nonempty(rows, ("baseline_sanity_plateau_count",))),
        "pressure_gate_result": str(_latest_nonempty(rows, ("pressure_gate_result", "pressure_gate_status")) or ""),
        "dewpoint_gate_result": str(_latest_nonempty(rows, ("dewpoint_gate_result",)) or ""),
        "reject_reason": reject_reason,
        "forbidden_pre_sampling_actions": "; ".join(forbidden_actions),
    }


def load_run_point_results(run_dir: Path, *, round_index_fallback: int = 1) -> List[Dict[str, Any]]:
    artifacts = discover_run_artifacts(run_dir)
    points_path = artifacts.get("points_summary")
    timing_path = artifacts.get("point_timing_summary")
    trace_path = artifacts.get("pressure_trace")
    if points_path and timing_path and trace_path and points_path.exists() and timing_path.exists() and trace_path.exists():
        sample_rows = [row for row in _load_csv_rows(points_path) if _co2_like_row(row)]
        timing_rows = [row for row in _load_csv_rows(timing_path) if _co2_like_row(row)]
        trace_rows = [row for row in _load_csv_rows(trace_path) if _co2_like_row(row)]
        samples_by_point = _group_by_point(sample_rows)
        timing_by_point = _group_by_point(timing_rows)
        trace_by_point = _group_by_point(trace_rows)
        point_keys = sorted(
            set(samples_by_point) | set(timing_by_point) | set(trace_by_point),
            key=lambda item: (item[0] or 0, item[1]),
        )
        results = [
            _build_point_result(
                sample_rows=samples_by_point.get(key, []),
                timing_rows=timing_by_point.get(key, []),
                trace_rows=trace_by_point.get(key, []),
                round_index_fallback=round_index_fallback,
            )
            for key in point_keys
            if key[1] == "co2"
        ]
        return sorted(results, key=lambda row: (int(row["round_index"]), PRESSURE_ORDER.get(row["pressure_hpa"], 99)))

    aggregate_path = artifacts.get("aggregate_summary")
    if aggregate_path and aggregate_path.exists():
        rows = [_normalize_prebuilt_summary_row(row) for row in _load_csv_rows(aggregate_path) if _co2_like_row(row)]
        for row in rows:
            if not row["round_index"]:
                row["round_index"] = round_index_fallback
        return sorted(rows, key=lambda row: (int(row["round_index"]), PRESSURE_ORDER.get(row["pressure_hpa"], 99)))
    raise FileNotFoundError(f"Could not locate usable artifacts under {run_dir}")


def _load_trace_rows(run_dir: Path, *, round_index_fallback: int = 1) -> List[Dict[str, Any]]:
    artifacts = discover_run_artifacts(run_dir)
    trace_path = artifacts.get("pressure_trace")
    if not trace_path or not trace_path.exists():
        return []
    rows = [dict(row) for row in _load_csv_rows(trace_path) if _co2_like_row(row)]
    for row in rows:
        point_tag = str(row.get("point_tag") or "")
        row["round_index"] = _safe_int(row.get("round_index")) or _parse_round_index(point_tag, round_index_fallback)
        row["pressure_hpa"] = _safe_int(_pick_first(row, ("pressure_hpa", "pressure_target_hpa", "target_pressure_hpa")))
    return rows


def _sequence_metrics(point_results: Sequence[Mapping[str, Any]]) -> Dict[int, Dict[str, Any]]:
    grouped: Dict[int, List[Mapping[str, Any]]] = defaultdict(list)
    for row in point_results:
        grouped[int(row.get("round_index") or 1)].append(row)
    metrics: Dict[int, Dict[str, Any]] = {}
    for round_index, rows in sorted(grouped.items()):
        ordered = sorted(rows, key=lambda item: PRESSURE_ORDER.get(_safe_int(item.get("pressure_hpa")), 99))
        co2_values = [float(row["co2_mean_ppm"]) for row in ordered if row.get("co2_mean_ppm") is not None]
        dew_values = [float(row["dewpoint_mean_c"]) for row in ordered if row.get("dewpoint_mean_c") is not None]
        h2o_values = [float(row["h2o_mean_mmol"]) for row in ordered if row.get("h2o_mean_mmol") is not None]
        metrics[round_index] = {
            "sampled_points": sum(1 for row in ordered if row.get("status") == "sampled"),
            "rejected_points": sum(1 for row in ordered if row.get("status") == "rejected_before_sampling"),
            "co2_monotonic_down": len(co2_values) >= 3 and all(nxt < cur for cur, nxt in zip(co2_values, co2_values[1:])),
            "dewpoint_monotonic_up": len(dew_values) >= 3 and all(nxt > cur for cur, nxt in zip(dew_values, dew_values[1:])),
            "h2o_monotonic_up": len(h2o_values) >= 3 and all(nxt > cur for cur, nxt in zip(h2o_values, h2o_values[1:])),
        }
    return metrics


def classify_ingress_result(point_results: Sequence[Mapping[str, Any]]) -> Tuple[str, Dict[str, Any]]:
    round_metrics = _sequence_metrics(point_results)
    diagnosis_counter: Counter[str] = Counter()
    reject_counter: Counter[str] = Counter()
    forbidden_counter: Counter[str] = Counter()
    category_point_counter: Counter[str] = Counter()
    handoff_mismatch_count = 0
    accepted_near_ambient_count = 0
    accepted_low_pressure_count = 0
    baseline_large_delta_count = 0
    baseline_wrong_plateau_count = 0
    for row in point_results:
        diagnosis = str(row.get("post_isolation_diagnosis") or "").strip()
        reject_reason = str(row.get("reject_reason") or "").strip()
        forbidden = str(row.get("forbidden_pre_sampling_actions") or "").strip()
        handoff_mode = str(row.get("handoff_mode") or "").strip()
        pressure_hpa = _safe_int(row.get("pressure_hpa"))
        target_co2_ppm = _safe_float(row.get("target_co2_ppm"))
        if target_co2_ppm is None:
            target_co2_ppm = _safe_float(row.get("baseline_sanity_target_co2_ppm"))
        if target_co2_ppm is None:
            target_co2_ppm = float(TARGET_CO2_PPM)
        co2_mean_ppm = _safe_float(row.get("co2_mean_ppm"))
        status = str(row.get("status") or "").strip().lower()
        if status == "sampled":
            if _is_near_ambient_pressure(pressure_hpa):
                accepted_near_ambient_count += 1
                if co2_mean_ppm is not None and abs(float(co2_mean_ppm) - float(target_co2_ppm)) > BASELINE_TOLERANCE_PPM:
                    baseline_large_delta_count += 1
            elif pressure_hpa is not None:
                accepted_low_pressure_count += 1
        if reject_reason == "baseline_precondition_wrong_plateau_suspect":
            baseline_wrong_plateau_count += 1
        if diagnosis:
            diagnosis_counter[diagnosis] += 1
        if reject_reason:
            reject_counter[reject_reason] += 1
        for category in {value for value in (reject_reason, diagnosis) if value}:
            category_point_counter[category] += 1
        if forbidden:
            for item in re.split(r"[;,|]", forbidden):
                label = item.strip()
                if label:
                    forbidden_counter[label] += 1
        if handoff_mode and handoff_mode not in {"same_gas_pressure_step_handoff", "same_gas_superambient_precharge_handoff"}:
            handoff_mismatch_count += 1

    old_reopen_count = handoff_mismatch_count + sum(
        count for action, count in forbidden_counter.items() if action in {"atmosphere refresh", "route reopen"}
    )
    pace_vent_in_progress_count = category_point_counter["pace_vent_in_progress_suspect"]
    pace_legacy_vent_state_3_count = (
        category_point_counter["pace_legacy_vent_state_3_suspect"]
        + category_point_counter["pace_atmosphere_connected_latched_state_suspect"]
    )
    pace_vent_completed_latched_count = category_point_counter["pace_vent_completed_latched_suspect"]
    pace_effort_nonzero_after_output_off_count = category_point_counter["pace_effort_nonzero_after_output_off_suspect"]
    pace_supply_vacuum_compensation_count = category_point_counter["pace_supply_vacuum_compensation_suspect"]
    pace_vent_after_valve_config_open_count = category_point_counter["pace_vent_after_valve_config_open_suspect"]
    pace_vent_valve_left_open_count = category_point_counter["pace_vent_valve_left_open_suspect"]
    pace_protective_vent_count = category_point_counter["pace_protective_vent_suspect"]
    pace_popup_only_count = category_point_counter["pace_vent_popup_only"]
    pace_popup_stale_count = category_point_counter["pace_vent_popup_stale_suspect"]
    post_isolation_ambient_ingress_count = category_point_counter["post_isolation_ambient_ingress_suspect"]
    sealed_path_leak_count = category_point_counter["sealed_path_leak_suspect"]
    dead_volume_wet_release_count = category_point_counter["dead_volume_wet_release_suspect"]
    controller_hunting_count = category_point_counter["controller_hunting_suspect"]
    fast5s_pass_count = sum(
        1
        for row in point_results
        if str(row.get("post_isolation_fast_capture_status") or "").strip().lower() == "pass"
        and str(row.get("post_isolation_capture_mode") or "").strip().lower() == "fast5s"
    )
    fast5s_fallback_count = sum(
        1
        for row in point_results
        if bool(row.get("post_isolation_fast_capture_fallback"))
        or str(row.get("post_isolation_capture_mode") or "").strip().lower() == "extended20s"
    )
    if pace_vent_in_progress_count:
        fast_capture_assessment = "5 秒快采失败且提示 vent 正在执行"
    elif pace_vent_completed_latched_count:
        fast_capture_assessment = "5 秒快采失败且提示 VENT=2 锁存未清"
    elif pace_supply_vacuum_compensation_count:
        fast_capture_assessment = "5 秒快采失败且提示供压/真空侧仍在补偿"
    elif pace_effort_nonzero_after_output_off_count:
        fast_capture_assessment = "5 秒快采失败且提示 OUTP OFF 后 effort 仍非零"
    elif pace_vent_valve_left_open_count or pace_protective_vent_count or pace_vent_after_valve_config_open_count:
        fast_capture_assessment = "5 秒快采失败且提示 vent-after-valve / protective vent"
    elif post_isolation_ambient_ingress_count or sealed_path_leak_count:
        fast_capture_assessment = "5 秒快采失败且提示 post-isolation ambient ingress"
    elif dead_volume_wet_release_count:
        fast_capture_assessment = "5 秒快采失败且提示 dead-volume wet release"
    elif pace_popup_only_count or pace_popup_stale_count:
        fast_capture_assessment = "5 秒快采失败但 20 秒诊断显示是 popup-only"
    elif fast5s_pass_count:
        fast_capture_assessment = "5 秒快采已足够"
    else:
        fast_capture_assessment = "未启用 5 秒快采或证据不足"

    if pace_legacy_vent_state_3_count and not any(_checkvalve_mode_active(row) for row in point_results):
        fast_capture_assessment = "5s fast capture blocked by legacy VENT=3 watchlist-only state"

    severe_physical_count = (
        old_reopen_count
        + pace_vent_in_progress_count
        + pace_legacy_vent_state_3_count
        + pace_vent_completed_latched_count
        + pace_effort_nonzero_after_output_off_count
        + pace_supply_vacuum_compensation_count
        + pace_vent_valve_left_open_count
        + pace_protective_vent_count
        + post_isolation_ambient_ingress_count
        + sealed_path_leak_count
    )
    moderate_count = pace_vent_after_valve_config_open_count + pace_popup_only_count + pace_popup_stale_count
    monotonic_pullback = any(
        item["co2_monotonic_down"] or (item["dewpoint_monotonic_up"] and item["h2o_monotonic_up"])
        for item in round_metrics.values()
    )
    if severe_physical_count > 0 or monotonic_pullback:
        conclusion = "混气仍明显存在"
    elif dead_volume_wet_release_count > 0 or moderate_count > 0 or fast5s_fallback_count > 0:
        conclusion = "混气明显减轻但未完全解决"
    else:
        conclusion = "混气已基本解决"

    baseline_source_or_stream_problem_count = (
        baseline_large_delta_count
        + baseline_wrong_plateau_count
        + category_point_counter["baseline_source_mismatch_suspect"]
        + category_point_counter["baseline_stream_mismatch_suspect"]
        + category_point_counter["baseline_precondition_wrong_plateau_suspect"]
    )
    checkvalve_mode_point_count = sum(1 for row in point_results if _checkvalve_mode_active(row))
    checkvalve_external_pressure_drift_failure_count = sum(
        1 for row in point_results if _checkvalve_mode_active(row) and _checkvalve_pressure_drift_failure(row)
    )
    checkvalve_dewpoint_rise_failure_count = sum(
        1 for row in point_results if _checkvalve_mode_active(row) and _checkvalve_dewpoint_failure(row)
    )
    controller_side_only_without_analyzer_side_effect_count = sum(
        1 for row in point_results if _checkvalve_mode_active(row) and _checkvalve_controller_side_only(row)
    )
    if checkvalve_mode_point_count > 0:
        if (
            fast5s_pass_count > 0
            and checkvalve_external_pressure_drift_failure_count == 0
            and checkvalve_dewpoint_rise_failure_count == 0
            and old_reopen_count == 0
            and pace_vent_in_progress_count == 0
            and controller_hunting_count == 0
        ):
            conclusion_code = "checkvalve_quick_smoke_pass"
            conclusion = "check-valve quick smoke passed on analyzer-side evidence"
        elif checkvalve_external_pressure_drift_failure_count > 0:
            conclusion_code = "checkvalve_quick_smoke_failed_by_external_pressure_drift"
            conclusion = "check-valve quick smoke failed on analyzer-side external-pressure drift"
        elif checkvalve_dewpoint_rise_failure_count > 0:
            conclusion_code = "checkvalve_quick_smoke_failed_by_dewpoint_rise"
            conclusion = "check-valve quick smoke failed on post-isolation dewpoint rise"
        elif controller_side_only_without_analyzer_side_effect_count > 0:
            conclusion_code = "controller_side_state_only_without_analyzer_side_effect"
            conclusion = "controller-side vent state persisted without analyzer-side effect under check-valve mode"
        elif severe_physical_count > 0 or dead_volume_wet_release_count > 0 or moderate_count > 0 or monotonic_pullback:
            conclusion_code = "post_isolation_physical_leak_or_wet_release_still_present"
            conclusion = "post-isolation physical leak or wet release still present under check-valve quick smoke"
        else:
            conclusion_code = "mixed_but_improved"
    elif pace_legacy_vent_state_3_count > 0:
        conclusion_code = "legacy_vent_state_problem"
        conclusion = "legacy VENT=3 remains watchlist-only and blocks low-pressure inference"
    elif baseline_source_or_stream_problem_count > 0:
        conclusion_code = "baseline_source_or_stream_problem"
        conclusion = "near-ambient baseline points to source/stream mismatch before low-pressure inference"
    elif severe_physical_count > 0 or monotonic_pullback:
        conclusion_code = "post_isolation_low_pressure_problem"
    elif dead_volume_wet_release_count > 0 or moderate_count > 0 or fast5s_fallback_count > 0:
        conclusion_code = "mixed_but_improved"
    else:
        conclusion_code = "cleared_or_not_observed"

    metrics = {
        "round_metrics": round_metrics,
        "old_atmosphere_reopen_problem_count": old_reopen_count,
        "handoff_mismatch_count": handoff_mismatch_count,
        "pace_vent_in_progress_count": pace_vent_in_progress_count,
        "pace_vent_completed_latched_count": pace_vent_completed_latched_count,
        "pace_effort_nonzero_after_output_off_count": pace_effort_nonzero_after_output_off_count,
        "pace_supply_vacuum_compensation_count": pace_supply_vacuum_compensation_count,
        "pace_vent_after_valve_config_open_count": pace_vent_after_valve_config_open_count,
        "pace_vent_valve_left_open_count": pace_vent_valve_left_open_count,
        "pace_protective_vent_count": pace_protective_vent_count,
        "pace_vent_popup_only_count": pace_popup_only_count,
        "pace_vent_popup_stale_count": pace_popup_stale_count,
        "post_isolation_ambient_ingress_count": post_isolation_ambient_ingress_count,
        "sealed_path_leak_count": sealed_path_leak_count,
        "dead_volume_wet_release_count": dead_volume_wet_release_count,
        "controller_hunting_count": controller_hunting_count,
        "fast5s_pass_count": fast5s_pass_count,
        "fast5s_fallback_count": fast5s_fallback_count,
        "fast_capture_assessment": fast_capture_assessment,
        "forbidden_action_counts": dict(forbidden_counter),
        "reject_reason_counts": dict(reject_counter),
        "diagnosis_counts": dict(diagnosis_counter),
    }
    metrics.update(
        {
            "conclusion_code": conclusion_code,
            "pace_legacy_vent_state_3_count": pace_legacy_vent_state_3_count,
            "accepted_near_ambient_count": accepted_near_ambient_count,
            "accepted_low_pressure_count": accepted_low_pressure_count,
            "baseline_large_delta_count": baseline_large_delta_count,
            "baseline_wrong_plateau_count": baseline_wrong_plateau_count,
            "baseline_source_or_stream_problem_count": baseline_source_or_stream_problem_count,
            "checkvalve_mode_point_count": checkvalve_mode_point_count,
            "checkvalve_external_pressure_drift_failure_count": checkvalve_external_pressure_drift_failure_count,
            "checkvalve_dewpoint_rise_failure_count": checkvalve_dewpoint_rise_failure_count,
            "controller_side_only_without_analyzer_side_effect_count": (
                controller_side_only_without_analyzer_side_effect_count
            ),
        }
    )
    return conclusion, metrics


def _write_csv_rows(path: Path, rows: Sequence[Mapping[str, Any]], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _write_count_csv(path: Path, counter: Mapping[str, int], *, key_name: str) -> None:
    rows = [{key_name: key, "count": value} for key, value in sorted(counter.items(), key=lambda item: (-item[1], item[0]))]
    _write_csv_rows(path, rows, [key_name, "count"])


def _build_standard_status_summary_rows(
    point_results: Sequence[Mapping[str, Any]],
    trace_rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    rows = list(point_results) + list(trace_rows)
    vent_counter: Counter[str] = Counter(
        str(_safe_int(_pick_first(row, ("pace_vent_status_query", "pace_vent_status"))))
        for row in rows
        if _safe_int(_pick_first(row, ("pace_vent_status_query", "pace_vent_status"))) is not None
    )
    summary_rows = [
        {"metric": "vent_status_0_count", "count": vent_counter.get("0", 0)},
        {"metric": "vent_status_1_count", "count": vent_counter.get("1", 0)},
        {"metric": "vent_status_2_count", "count": vent_counter.get("2", 0)},
        {"metric": "vent_status_3_count", "count": vent_counter.get("3", 0)},
        {
            "metric": "accepted_near_ambient_count",
            "count": sum(
                1
                for row in point_results
                if str(row.get("status") or "").strip().lower() == "sampled"
                and _is_near_ambient_pressure(row.get("pressure_hpa"))
            ),
        },
        {
            "metric": "accepted_low_pressure_count",
            "count": sum(
                1
                for row in point_results
                if str(row.get("status") or "").strip().lower() == "sampled"
                and not _is_near_ambient_pressure(row.get("pressure_hpa"))
                and _safe_int(row.get("pressure_hpa")) is not None
            ),
        },
        {
            "metric": "vent_completed_latched_points",
            "count": sum(
                1
                for row in point_results
                if _safe_bool(row.get("pace_vent_completed_latched")) is True
                or _safe_int(row.get("pace_vent_status_query")) == 2
            ),
        },
        {
            "metric": "oper_pres_vent_complete_bit_points",
            "count": sum(1 for row in point_results if _safe_bool(row.get("pace_oper_pres_vent_complete_bit")) is True),
        },
        {
            "metric": "oper_pres_in_limits_bit_points",
            "count": sum(1 for row in point_results if _safe_bool(row.get("pace_oper_pres_in_limits_bit")) is True),
        },
        {
            "metric": "effort_nonzero_after_output_off_points",
            "count": sum(
                1
                for row in point_results
                if _safe_int(row.get("pace_outp_state_query")) == 0
                and _safe_float(row.get("pace_effort_query")) is not None
                and abs(float(_safe_float(row.get("pace_effort_query")) or 0.0)) > 0.01
            ),
        },
        {
            "metric": "supply_comp_positive_points",
            "count": sum(1 for row in point_results if (_safe_float(row.get("pace_comp1_query")) or 0.0) > 0.0),
        },
        {
            "metric": "vacuum_comp_negative_points",
            "count": sum(1 for row in point_results if (_safe_float(row.get("pace_comp2_query")) or 0.0) < 0.0),
        },
    ]
    return summary_rows


def _build_baseline_plateau_rows(point_results: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in point_results:
        pressure_hpa = _safe_int(row.get("pressure_hpa"))
        if not _is_near_ambient_pressure(pressure_hpa):
            continue
        target_co2_ppm = _safe_float(row.get("target_co2_ppm"))
        if target_co2_ppm is None:
            target_co2_ppm = _safe_float(row.get("baseline_sanity_target_co2_ppm"))
        if target_co2_ppm is None:
            target_co2_ppm = float(TARGET_CO2_PPM)
        plateau_mean_ppm = _safe_float(row.get("baseline_sanity_plateau_mean_ppm"))
        co2_mean_ppm = _safe_float(row.get("co2_mean_ppm"))
        abs_delta_ppm = None
        if co2_mean_ppm is not None:
            abs_delta_ppm = abs(float(co2_mean_ppm) - float(target_co2_ppm))
        elif plateau_mean_ppm is not None:
            abs_delta_ppm = abs(float(plateau_mean_ppm) - float(target_co2_ppm))
        rows.append(
            {
                "round_index": row.get("round_index"),
                "pressure_hpa": pressure_hpa,
                "point_row": row.get("point_row"),
                "point_tag": row.get("point_tag"),
                "status": row.get("status"),
                "target_co2_ppm": target_co2_ppm,
                "co2_mean_ppm": co2_mean_ppm,
                "baseline_sanity_gate_status": row.get("baseline_sanity_gate_status"),
                "baseline_sanity_gate_reason": row.get("baseline_sanity_gate_reason"),
                "baseline_sanity_plateau_mean_ppm": plateau_mean_ppm,
                "baseline_sanity_plateau_span_ppm": _safe_float(row.get("baseline_sanity_plateau_span_ppm")),
                "baseline_sanity_plateau_count": _safe_int(row.get("baseline_sanity_plateau_count")),
                "abs_delta_ppm": abs_delta_ppm,
                "reject_reason": row.get("reject_reason"),
                "post_isolation_diagnosis": row.get("post_isolation_diagnosis"),
            }
        )
    return rows


def _write_placeholder_plot(path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")
    return str(path)


def _safe_plot_series(ax: Any, *, x_values: Sequence[int], y_values: Sequence[Optional[float]], label: str, marker: str = "o") -> None:
    cleaned = [(x, y) for x, y in zip(x_values, y_values) if y is not None]
    if not cleaned:
        return
    xs, ys = zip(*cleaned)
    ax.plot(xs, ys, marker=marker, label=label)


def _plot_round_curves(point_results: Sequence[Mapping[str, Any]], output_dir: Path) -> Dict[str, str]:
    if plt is None:
        return {
            "co2_plot": _write_placeholder_plot(output_dir / "pressure_vs_co2_rounds.png"),
            "dewpoint_h2o_plot": _write_placeholder_plot(output_dir / "pressure_vs_dewpoint_h2o_rounds.png"),
            "post_isolation_drift_plot": _write_placeholder_plot(
                output_dir / "post_isolation_pressure_drift_vs_pressure.png"
            ),
            "post_isolation_dewpoint_rise_plot": _write_placeholder_plot(
                output_dir / "post_isolation_dewpoint_rise_vs_pressure.png"
            ),
        }
    grouped: Dict[int, List[Mapping[str, Any]]] = defaultdict(list)
    for row in point_results:
        grouped[int(row.get("round_index") or 1)].append(row)

    co2_plot = output_dir / "pressure_vs_co2_rounds.png"
    fig, ax = plt.subplots(figsize=(8.5, 5.0))
    for round_index, rows in sorted(grouped.items()):
        ordered = sorted(rows, key=lambda item: PRESSURE_ORDER.get(_safe_int(item.get("pressure_hpa")), 99))
        filtered = [row for row in ordered if row.get("pressure_hpa") is not None]
        _safe_plot_series(
            ax,
            x_values=[int(row["pressure_hpa"]) for row in filtered],
            y_values=[_safe_float(row.get("co2_mean_ppm")) for row in filtered],
            label=_round_label(round_index),
        )
    ax.set_title("pressure vs CO2")
    ax.set_xlabel("pressure (hPa)")
    ax.set_ylabel("CO2 (ppm)")
    ax.grid(True, alpha=0.3)
    if grouped:
        ax.legend()
    fig.tight_layout()
    fig.savefig(co2_plot, dpi=160)
    plt.close(fig)

    dewpoint_plot = output_dir / "pressure_vs_dewpoint_h2o_rounds.png"
    fig, (ax_dew, ax_h2o) = plt.subplots(2, 1, figsize=(8.5, 8.0), sharex=True)
    for round_index, rows in sorted(grouped.items()):
        ordered = sorted(rows, key=lambda item: PRESSURE_ORDER.get(_safe_int(item.get("pressure_hpa")), 99))
        filtered = [row for row in ordered if row.get("pressure_hpa") is not None]
        x_values = [int(row["pressure_hpa"]) for row in filtered]
        _safe_plot_series(ax_dew, x_values=x_values, y_values=[_safe_float(row.get("dewpoint_mean_c")) for row in filtered], label=_round_label(round_index))
        _safe_plot_series(ax_h2o, x_values=x_values, y_values=[_safe_float(row.get("h2o_mean_mmol")) for row in filtered], label=_round_label(round_index))
    ax_dew.set_title("pressure vs dewpoint")
    ax_dew.set_ylabel("dewpoint (C)")
    ax_dew.grid(True, alpha=0.3)
    ax_h2o.set_title("pressure vs H2O")
    ax_h2o.set_xlabel("pressure (hPa)")
    ax_h2o.set_ylabel("H2O (mmol)")
    ax_h2o.grid(True, alpha=0.3)
    if grouped:
        ax_dew.legend()
        ax_h2o.legend()
    fig.tight_layout()
    fig.savefig(dewpoint_plot, dpi=160)
    plt.close(fig)

    drift_plot = output_dir / "post_isolation_pressure_drift_vs_pressure.png"
    fig, ax = plt.subplots(figsize=(8.5, 5.0))
    for round_index, rows in sorted(grouped.items()):
        ordered = sorted(rows, key=lambda item: PRESSURE_ORDER.get(_safe_int(item.get("pressure_hpa")), 99))
        filtered = [row for row in ordered if row.get("pressure_hpa") is not None]
        _safe_plot_series(ax, x_values=[int(row["pressure_hpa"]) for row in filtered], y_values=[_safe_float(row.get("post_isolation_pressure_drift_hpa")) for row in filtered], label=_round_label(round_index))
    ax.set_title("post-isolation pressure drift vs pressure")
    ax.set_xlabel("pressure (hPa)")
    ax.set_ylabel("drift (hPa)")
    ax.grid(True, alpha=0.3)
    if grouped:
        ax.legend()
    fig.tight_layout()
    fig.savefig(drift_plot, dpi=160)
    plt.close(fig)

    dew_rise_plot = output_dir / "post_isolation_dewpoint_rise_vs_pressure.png"
    fig, ax = plt.subplots(figsize=(8.5, 5.0))
    for round_index, rows in sorted(grouped.items()):
        ordered = sorted(rows, key=lambda item: PRESSURE_ORDER.get(_safe_int(item.get("pressure_hpa")), 99))
        filtered = [row for row in ordered if row.get("pressure_hpa") is not None]
        _safe_plot_series(ax, x_values=[int(row["pressure_hpa"]) for row in filtered], y_values=[_safe_float(row.get("post_isolation_dewpoint_rise_c")) for row in filtered], label=_round_label(round_index))
    ax.set_title("post-isolation dewpoint rise vs pressure")
    ax.set_xlabel("pressure (hPa)")
    ax.set_ylabel("dewpoint rise (C)")
    ax.grid(True, alpha=0.3)
    if grouped:
        ax.legend()
    fig.tight_layout()
    fig.savefig(dew_rise_plot, dpi=160)
    plt.close(fig)

    return {
        "co2_plot": str(co2_plot),
        "dewpoint_h2o_plot": str(dewpoint_plot),
        "post_isolation_drift_plot": str(drift_plot),
        "post_isolation_dewpoint_rise_plot": str(dew_rise_plot),
    }


def _plot_trace_timelines(trace_rows: Sequence[Mapping[str, Any]], output_dir: Path) -> Dict[str, str]:
    if plt is None:
        return {
            "pace_vent_state_vs_time_plot": _write_placeholder_plot(output_dir / "pace_vent_state_vs_time.png"),
            "pace_effort_vs_time_plot": _write_placeholder_plot(output_dir / "pace_effort_vs_time.png"),
            "pace_comp_supply_vacuum_vs_time_plot": _write_placeholder_plot(
                output_dir / "pace_comp_supply_vacuum_vs_time.png"
            ),
            "pace_inlimits_vs_time_plot": _write_placeholder_plot(output_dir / "pace_inlimits_vs_time.png"),
        }
    vent_plot = output_dir / "pace_vent_state_vs_time.png"
    effort_plot = output_dir / "pace_effort_vs_time.png"
    comp_plot = output_dir / "pace_comp_supply_vacuum_vs_time.png"
    inlimits_plot = output_dir / "pace_inlimits_vs_time.png"
    sorted_rows = sorted(
        trace_rows,
        key=lambda row: (
            int(row.get("round_index") or 1),
            PRESSURE_ORDER.get(_safe_int(row.get("pressure_hpa")), 99),
            _parse_ts(row.get("ts")) or datetime.min,
        ),
    )
    x_values = list(range(len(sorted_rows)))
    status_values = [_safe_float(_pick_first(row, ("pace_vent_status_query", "pace_vent_status"))) for row in sorted_rows]
    effort_values = [_safe_float(row.get("pace_effort_query")) for row in sorted_rows]
    comp1_values = [_safe_float(row.get("pace_comp1_query")) for row in sorted_rows]
    comp2_values = [_safe_float(row.get("pace_comp2_query")) for row in sorted_rows]
    inl_state_values = [
        _safe_float(_pick_first(row, ("pace_sens_pres_inl_state_query", "pace_oper_pres_in_limits_bit")))
        for row in sorted_rows
    ]
    inl_time_values = [
        _safe_float(_pick_first(row, ("pace_sens_pres_inl_time_query", "pace_sens_inl_time_query")))
        for row in sorted_rows
    ]

    fig, ax = plt.subplots(figsize=(9.0, 4.8))
    _safe_plot_series(ax, x_values=x_values, y_values=status_values, label="VENT?", marker=".")
    ax.set_title("PACE vent state vs time")
    ax.set_xlabel("trace sample index")
    ax.set_ylabel("VENT status")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(vent_plot, dpi=160)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(9.0, 4.8))
    _safe_plot_series(ax, x_values=x_values, y_values=effort_values, label="EFF?", marker=".")
    ax.set_title("PACE effort vs time")
    ax.set_xlabel("trace sample index")
    ax.set_ylabel("effort")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(effort_plot, dpi=160)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(9.0, 4.8))
    _safe_plot_series(ax, x_values=x_values, y_values=comp1_values, label="COMP1", marker=".")
    _safe_plot_series(ax, x_values=x_values, y_values=comp2_values, label="COMP2", marker=".")
    ax.set_title("PACE compensation source vs time")
    ax.set_xlabel("trace sample index")
    ax.set_ylabel("compensation pressure")
    ax.grid(True, alpha=0.3)
    if sorted_rows:
        ax.legend()
    fig.tight_layout()
    fig.savefig(comp_plot, dpi=160)
    plt.close(fig)

    fig, (ax_state, ax_time) = plt.subplots(2, 1, figsize=(9.0, 7.0), sharex=True)
    _safe_plot_series(ax_state, x_values=x_values, y_values=inl_state_values, label="INL?", marker=".")
    ax_state.set_title("PACE in-limits vs time")
    ax_state.set_ylabel("in-limits state")
    ax_state.grid(True, alpha=0.3)
    _safe_plot_series(ax_time, x_values=x_values, y_values=inl_time_values, label="INL:TIME?", marker=".")
    ax_time.set_xlabel("trace sample index")
    ax_time.set_ylabel("in-limits time (s)")
    ax_time.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(inlimits_plot, dpi=160)
    plt.close(fig)

    return {
        "pace_vent_state_vs_time_plot": str(vent_plot),
        "pace_effort_vs_time_plot": str(effort_plot),
        "pace_comp_supply_vacuum_vs_time_plot": str(comp_plot),
        "pace_inlimits_vs_time_plot": str(inlimits_plot),
    }


def analyze_runs(run_dirs: Sequence[Path | str], *, output_dir: Path | str) -> Dict[str, Any]:
    resolved_run_dirs = [Path(path).resolve() for path in run_dirs]
    output_path = Path(output_dir).resolve()
    output_path.mkdir(parents=True, exist_ok=True)

    point_results: List[Dict[str, Any]] = []
    trace_rows: List[Dict[str, Any]] = []
    for run_index, run_dir in enumerate(resolved_run_dirs, start=1):
        point_results.extend(load_run_point_results(run_dir, round_index_fallback=run_index))
        trace_rows.extend(_load_trace_rows(run_dir, round_index_fallback=run_index))

    point_results = sorted(
        point_results,
        key=lambda row: (
            int(row.get("round_index") or 1),
            PRESSURE_ORDER.get(_safe_int(row.get("pressure_hpa")), 99),
            int(row.get("point_row") or 0),
        ),
    )
    point_summary_csv = output_path / "same_gas_two_round_point_summary.csv"
    _write_csv_rows(point_summary_csv, point_results, POINT_SUMMARY_FIELDS)

    presample_lock_rows: List[Dict[str, Any]] = []
    for row in point_results:
        forbidden = str(row.get("forbidden_pre_sampling_actions") or "").strip()
        if not forbidden:
            continue
        for item in re.split(r"[;,|]", forbidden):
            label = item.strip()
            if label:
                presample_lock_rows.append(
                    {
                        "round_index": row.get("round_index"),
                        "pressure_hpa": row.get("pressure_hpa"),
                        "point_row": row.get("point_row"),
                        "point_tag": row.get("point_tag"),
                        "action": label,
                    }
                )
    presample_lock_csv = output_path / "presample_lock_violations.csv"
    _write_csv_rows(presample_lock_csv, presample_lock_rows, ["round_index", "pressure_hpa", "point_row", "point_tag", "action"])

    reject_counter = Counter(str(row.get("reject_reason") or "").strip() for row in point_results if row.get("reject_reason"))
    reject_reason_csv = output_path / "reject_reason_summary.csv"
    _write_count_csv(reject_reason_csv, reject_counter, key_name="reject_reason")

    diagnosis_counter = Counter(str(row.get("post_isolation_diagnosis") or "").strip() for row in point_results if row.get("post_isolation_diagnosis"))
    diagnosis_csv = output_path / "post_isolation_diagnosis_summary.csv"
    _write_count_csv(diagnosis_csv, diagnosis_counter, key_name="post_isolation_diagnosis")

    pace_diagnosis_counter = Counter({key: count for key, count in diagnosis_counter.items() if key in PACE_DIAG_CATEGORIES})
    pace_diagnosis_csv = output_path / "pace_post_isolation_diagnosis_summary.csv"
    _write_count_csv(pace_diagnosis_csv, pace_diagnosis_counter, key_name="pace_post_isolation_diagnosis")

    standard_status_rows = _build_standard_status_summary_rows(point_results, trace_rows)
    standard_status_csv = output_path / "pace_standard_status_summary.csv"
    _write_csv_rows(standard_status_csv, standard_status_rows, ["metric", "count"])
    baseline_plateau_rows = _build_baseline_plateau_rows(point_results)
    baseline_plateau_csv = output_path / "baseline_plateau_summary.csv"
    _write_csv_rows(
        baseline_plateau_csv,
        baseline_plateau_rows,
        [
            "round_index",
            "pressure_hpa",
            "point_row",
            "point_tag",
            "status",
            "target_co2_ppm",
            "co2_mean_ppm",
            "baseline_sanity_gate_status",
            "baseline_sanity_gate_reason",
            "baseline_sanity_plateau_mean_ppm",
            "baseline_sanity_plateau_span_ppm",
            "baseline_sanity_plateau_count",
            "abs_delta_ppm",
            "reject_reason",
            "post_isolation_diagnosis",
        ],
    )

    protective_rows = [
        {"state": "vent_orpv_enabled_points", "count": sum(1 for row in point_results if str(row.get("pace_vent_orpv_state_query") or "").upper() == "ENABLED")},
        {"state": "vent_pupv_enabled_points", "count": sum(1 for row in point_results if str(row.get("pace_vent_pupv_state_query") or "").upper() == "ENABLED")},
        {"state": "pace_protective_vent_suspect_points", "count": sum(1 for row in point_results if str(row.get("post_isolation_diagnosis") or "") == "pace_protective_vent_suspect")},
    ]
    protective_csv = output_path / "pace_protective_vent_state_summary.csv"
    _write_csv_rows(protective_csv, protective_rows, ["state", "count"])

    fast_capture_rows = [
        {
            "round_index": row.get("round_index"),
            "pressure_hpa": row.get("pressure_hpa"),
            "point_row": row.get("point_row"),
            "point_tag": row.get("point_tag"),
            "post_isolation_capture_mode": row.get("post_isolation_capture_mode"),
            "post_isolation_fast_capture_status": row.get("post_isolation_fast_capture_status"),
            "post_isolation_fast_capture_reason": row.get("post_isolation_fast_capture_reason"),
            "post_isolation_fast_capture_elapsed_s": row.get("post_isolation_fast_capture_elapsed_s"),
            "post_isolation_fast_capture_fallback": row.get("post_isolation_fast_capture_fallback"),
            "pace_effort_query": row.get("pace_effort_query"),
            "pace_comp1_query": row.get("pace_comp1_query"),
            "pace_comp2_query": row.get("pace_comp2_query"),
            "pace_sens_pres_inl_state_query": row.get("pace_sens_pres_inl_state_query"),
            "pace_sens_pres_inl_time_query": row.get("pace_sens_pres_inl_time_query"),
            "pace_sens_slew_query": row.get("pace_sens_slew_query"),
            "post_isolation_diagnosis": row.get("post_isolation_diagnosis"),
            "reject_reason": row.get("reject_reason"),
        }
        for row in point_results
    ]
    fast_capture_csv = output_path / "fast5s_vs_extended20s_with_effort_summary.csv"
    _write_csv_rows(
        fast_capture_csv,
        fast_capture_rows,
        [
            "round_index",
            "pressure_hpa",
            "point_row",
            "point_tag",
            "post_isolation_capture_mode",
            "post_isolation_fast_capture_status",
            "post_isolation_fast_capture_reason",
            "post_isolation_fast_capture_elapsed_s",
            "post_isolation_fast_capture_fallback",
            "pace_effort_query",
            "pace_comp1_query",
            "pace_comp2_query",
            "pace_sens_pres_inl_state_query",
            "pace_sens_pres_inl_time_query",
            "pace_sens_slew_query",
            "post_isolation_diagnosis",
            "reject_reason",
        ],
    )
    checkvalve_fast_smoke_rows = [
        {
            "round_index": row.get("round_index"),
            "pressure_hpa": row.get("pressure_hpa"),
            "point_row": row.get("point_row"),
            "point_tag": row.get("point_tag"),
            "pace_upstream_check_valve_installed": row.get("pace_upstream_check_valve_installed"),
            "post_isolation_pressure_truth_source": row.get("post_isolation_pressure_truth_source"),
            "post_isolation_capture_mode": row.get("post_isolation_capture_mode"),
            "post_isolation_fast_capture_status": row.get("post_isolation_fast_capture_status"),
            "post_isolation_fast_capture_reason": row.get("post_isolation_fast_capture_reason"),
            "post_isolation_fast_capture_fallback": row.get("post_isolation_fast_capture_fallback"),
            "post_isolation_pressure_drift_hpa": row.get("post_isolation_pressure_drift_hpa"),
            "post_isolation_dewpoint_rise_c": row.get("post_isolation_dewpoint_rise_c"),
            "post_isolation_diagnosis": row.get("post_isolation_diagnosis"),
            "pace_vent_status_query": row.get("pace_vent_status_query"),
            "pace_legacy_vent_state_3_suspect": row.get("pace_legacy_vent_state_3_suspect"),
            "pace_vent_completed_latched": row.get("pace_vent_completed_latched"),
            "checkvalve_row_assessment": (
                "external_pressure_drift_failure"
                if _checkvalve_pressure_drift_failure(row)
                else "dewpoint_rise_failure"
                if _checkvalve_dewpoint_failure(row)
                else "controller_side_only_without_analyzer_side_effect"
                if _checkvalve_controller_side_only(row)
                else "clean_or_not_checkvalve"
            ),
            "reject_reason": row.get("reject_reason"),
        }
        for row in point_results
    ]
    checkvalve_fast_smoke_csv = output_path / "checkvalve_fast_smoke_summary.csv"
    _write_csv_rows(
        checkvalve_fast_smoke_csv,
        checkvalve_fast_smoke_rows,
        [
            "round_index",
            "pressure_hpa",
            "point_row",
            "point_tag",
            "pace_upstream_check_valve_installed",
            "post_isolation_pressure_truth_source",
            "post_isolation_capture_mode",
            "post_isolation_fast_capture_status",
            "post_isolation_fast_capture_reason",
            "post_isolation_fast_capture_fallback",
            "post_isolation_pressure_drift_hpa",
            "post_isolation_dewpoint_rise_c",
            "post_isolation_diagnosis",
            "pace_vent_status_query",
            "pace_legacy_vent_state_3_suspect",
            "pace_vent_completed_latched",
            "checkvalve_row_assessment",
            "reject_reason",
        ],
    )

    conclusion, metrics = classify_ingress_result(point_results)
    conclusion_code = str(metrics.get("conclusion_code") or "")
    plots = {}
    plots.update(_plot_round_curves(point_results, output_path))
    plots.update(_plot_trace_timelines(trace_rows, output_path))

    summary = {
        "run_dirs": [str(path) for path in resolved_run_dirs],
        "target_co2_ppm": TARGET_CO2_PPM,
        "required_pressures_hpa": REQUIRED_PRESSURES,
        "conclusion_code": conclusion_code,
        "conclusion": conclusion,
        "metrics": metrics,
        "point_results": point_results,
        "point_summary_csv": str(point_summary_csv),
        "presample_lock_violations_csv": str(presample_lock_csv),
        "reject_reason_summary_csv": str(reject_reason_csv),
        "post_isolation_diagnosis_summary_csv": str(diagnosis_csv),
        "pace_post_isolation_diagnosis_summary_csv": str(pace_diagnosis_csv),
        "pace_standard_status_summary_csv": str(standard_status_csv),
        "baseline_plateau_summary_csv": str(baseline_plateau_csv),
        "pace_protective_vent_state_summary_csv": str(protective_csv),
        "fast5s_vs_extended20s_with_effort_summary_csv": str(fast_capture_csv),
        "checkvalve_fast_smoke_summary_csv": str(checkvalve_fast_smoke_csv),
        "plots": plots,
    }
    summary_json = output_path / "same_gas_two_round_summary.json"
    summary["summary_json"] = str(summary_json)
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Analyze V1 800 ppm ingress smoke artifacts.")
    parser.add_argument("run_dirs", nargs="+", help="One or more run directories to analyze.")
    parser.add_argument("--output-dir", required=True, help="Directory for plots and summary CSVs.")
    args = parser.parse_args(argv)
    summary = analyze_runs(args.run_dirs, output_dir=args.output_dir)
    print(f"结论: {summary['conclusion']}")
    print(f"汇总: {summary['summary_json']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
