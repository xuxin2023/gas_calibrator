"""Calibration workflow runner."""

from __future__ import annotations

import csv
import os
import threading
import time
import re
import math
import json
from collections import Counter, deque
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean, stdev
from typing import Any, Dict, List, Mapping, Optional, Tuple

from ..config import (
    V1_CO2_ONLY_H2O_NOT_SUPPORTED_MESSAGE,
    get as cfg_get,
    require_v1_h2o_zero_span_supported,
    runtime_default,
    v1_h2o_zero_span_capability,
)
from ..coefficients.fit_amt import fit_amt_eq4, save_fit_report
from ..coefficients.data_loader import records_to_dataframe, resolve_column_name
from ..coefficients.fit_ratio_poly import fit_ratio_poly_rt_p, save_ratio_poly_report
from ..coefficients.fit_ratio_poly_evolved import fit_ratio_poly_rt_p_evolved
from ..data.points import CalibrationPoint, load_points_from_excel, reorder_points, validate_points
from ..export.temperature_compensation_export import export_temperature_compensation_artifacts
from ..h2o_summary_selection import normalize_h2o_summary_selection
from ..logging_utils import RunLogger
from ..senco_format import format_senco_values, rounded_senco_values
from ..validation.dewpoint_flush_gate import (
    detect_dewpoint_rebound,
    evaluate_dewpoint_flush_gate,
    predict_pressure_scaled_dewpoint_c,
)
from .tuning import workflow_param


class StabilityWindow:
    """Sliding time window stability detector based on peak-to-peak value."""

    def __init__(self, tol: float, window_s: float):
        self.tol = tol
        self.window_s = window_s
        self.values: List[tuple[float, float]] = []

    def add(self, value: float) -> None:
        now = time.time()
        self.values.append((now, value))
        self.values = [(t, v) for t, v in self.values if now - t <= self.window_s]

    def is_stable(self) -> bool:
        if len(self.values) < 2:
            return False
        vals = [v for _, v in self.values]
        return max(vals) - min(vals) <= self.tol


def _normalized_co2_group(value: Any) -> str:
    return str(value or "").strip().upper()


def _co2_map_ppm_values(raw_map: Any, default_ppm: Tuple[int, ...]) -> List[int]:
    if not isinstance(raw_map, dict) or not raw_map:
        return list(default_ppm)

    ppm_values: set[int] = set()
    for ppm_key in raw_map.keys():
        try:
            ppm_values.add(int(ppm_key))
        except Exception:
            continue
    if not ppm_values:
        return list(default_ppm)
    return sorted(ppm_values)


def _normalized_device_id_text(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _optional_env_bool(name: str) -> Optional[bool]:
    raw = os.getenv(name)
    if raw is None:
        return None
    text = str(raw).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None


_PRESSURE_TRACE_FIELDS = [
    "ts",
    "point_title",
    "point_phase",
    "point_tag",
    "point_row",
    "route",
    "trace_stage",
    "trigger_reason",
    "pressure_target_hpa",
    "pace_pressure_hpa",
    "pressure_gauge_hpa",
    "dewpoint_c",
    "dew_temp_c",
    "dew_rh_pct",
    "pace_output_state",
    "pace_isolation_state",
    "pace_vent_status",
    "vent_after_valve_supported",
    "vent_after_valve_open",
    "vent_popup_ack_enabled",
    "atmosphere_hold_strategy",
    "fast_group_span_ms",
    "handoff_sample_to_vent_ms",
    "handoff_vent_to_safe_open_ms",
    "handoff_safe_open_to_route_open_ms",
    "handoff_total_ms",
    "atmosphere_reference_hpa",
    "handoff_safe_open_delta_hpa",
    "deferred_export_queue_len",
    "dewpoint_live_c",
    "dew_temp_live_c",
    "dew_rh_live_pct",
    "dewpoint_gate_window_s",
    "dewpoint_gate_elapsed_s",
    "dewpoint_gate_span_c",
    "dewpoint_gate_slope_c_per_s",
    "dewpoint_gate_count",
    "dewpoint_time_to_gate",
    "dewpoint_tail_span_60s",
    "dewpoint_tail_slope_60s",
    "dewpoint_rebound_detected",
    "flush_gate_status",
    "flush_gate_reason",
    "sample_lag_ms",
    "soft_control_enabled",
    "soft_control_linear_slew_hpa_per_s",
    "note",
]

_POINT_TIMING_SUMMARY_FIELDS = [
    "point_title",
    "point_row",
    "point_phase",
    "point_tag",
    "pressure_target_hpa",
    "configured_route_soak_s",
    "prev_sampling_end_ts",
    "atmosphere_enter_begin_ts",
    "atmosphere_enter_verified_ts",
    "route_open_ts",
    "soak_begin_ts",
    "soak_end_ts",
    "preseal_vent_off_begin_ts",
    "preseal_trigger_reached_ts",
    "route_sealed_ts",
    "control_prepare_begin_ts",
    "control_ready_snapshot_acquired_ts",
    "control_ready_wait_begin_ts",
    "control_ready_wait_end_ts",
    "control_ready_verified_ts",
    "control_output_on_begin_ts",
    "control_output_on_command_sent_ts",
    "control_output_verify_wait_begin_ts",
    "control_output_verify_wait_end_ts",
    "control_output_on_verified_ts",
    "pressure_in_limits_ts",
    "dewpoint_gate_begin_ts",
    "dewpoint_gate_end_ts",
    "sampling_begin_ts",
    "first_effective_sample_ts",
    "sampling_end_ts",
    "sampling_end_to_atmosphere_enter_begin_ms",
    "atmosphere_enter_begin_to_atmosphere_enter_verified_ms",
    "atmosphere_enter_verified_to_route_open_ms",
    "route_open_to_soak_begin_ms",
    "soak_begin_to_soak_end_ms",
    "soak_end_to_preseal_vent_off_begin_ms",
    "preseal_vent_off_begin_to_preseal_trigger_reached_ms",
    "preseal_trigger_reached_to_route_sealed_ms",
    "preseal_vent_off_begin_to_route_sealed_ms",
    "route_sealed_to_control_prepare_begin_ms",
    "control_prepare_begin_to_control_ready_snapshot_acquired_ms",
    "control_ready_snapshot_acquired_to_control_ready_verified_ms",
    "control_ready_wait_ms",
    "control_prepare_begin_to_control_ready_verified_ms",
    "control_output_on_begin_to_control_output_on_command_sent_ms",
    "control_output_on_command_sent_to_control_output_on_verified_ms",
    "control_output_verify_wait_ms",
    "control_output_on_begin_to_control_output_on_verified_ms",
    "control_output_on_verified_to_pressure_in_limits_ms",
    "pressure_in_limits_to_dewpoint_gate_begin_ms",
    "dewpoint_gate_begin_to_dewpoint_gate_end_ms",
    "dewpoint_gate_end_to_sampling_begin_ms",
    "pressure_in_limits_to_sampling_begin_ms",
    "sampling_begin_to_first_effective_sample_ms",
    "lead_in_nonsoak_ms",
]


def _perform_safe_stop(devices: Dict[str, Any], log_fn, cfg: Dict[str, Any]) -> Dict[str, Any]:
    from ..tools.safe_stop import perform_safe_stop_with_retries

    safe_cfg = (cfg or {}).get("workflow", {}).get("safe_stop", {})
    return perform_safe_stop_with_retries(
        devices,
        log_fn=log_fn,
        cfg=cfg,
        attempts=int(safe_cfg.get("perform_attempts", 3) or 3),
        retry_delay_s=float(safe_cfg.get("retry_delay_s", 1.5) or 1.5),
    )


class CalibrationRunner:
    """Main workflow orchestrator."""

    _AMBIENT_PRESSURE_TOKEN = "ambient"
    _AMBIENT_PRESSURE_LABEL = "当前大气压"
    _STANDARD_PRESSURE_POINTS_HPA = (1100, 1000, 900, 800, 700, 600, 500)
    _PRESEAL_TOPOFF_TARGET_HPA = 1100.0
    _DEFAULT_CO2_GROUP_A_PPM = (0, 200, 400, 600, 800, 1000)
    _DEFAULT_CO2_GROUP_B_PPM = (0, 100, 300, 500, 700, 900)
    _FULL_SWEEP_CO2_TEMPS_C = (10.0, 20.0, 30.0)

    def __init__(self, config: Dict[str, Any], devices: Dict[str, Any], logger: RunLogger, log_fn, status_fn):
        self.cfg = config
        self.devices = devices
        self.logger = logger
        self.log = log_fn
        self.set_status = status_fn

        self.stop_event = threading.Event()
        self.pause_event = threading.Event()
        self.pause_event.set()

        self._all_samples: List[Dict[str, Any]] = []
        self._last_temp_target_c: Optional[float] = None
        self._last_temp_soak_done = False
        self._last_hgen_target: Optional[Tuple[Optional[float], Optional[float]]] = None
        self._last_hgen_setpoint_ready = False
        self._last_hgen_dewpoint_ready = False
        self._h2o_pressure_prepared_target: Optional[float] = None
        self._last_pressure_atmosphere_refresh_ts = 0.0
        self._pressure_atmosphere_hold_enabled = False
        self._pressure_atmosphere_refresh_error_logged = False
        self._pressure_atmosphere_hold_strategy = "legacy_hold_thread"
        self._pace_vent_after_valve_supported: Optional[bool] = None
        self._pace_vent_after_valve_open: Optional[bool] = None
        self._pace_vent_popup_ack_enabled: Optional[bool] = None
        self._pace_vent_popup_ack_supported: Optional[bool] = None
        self._disabled_analyzers: set[str] = set()
        self._disabled_analyzer_reasons: Dict[str, str] = {}
        self._disabled_analyzer_last_reprobe_ts: Dict[str, float] = {}
        self._last_live_analyzer_snapshot_ts = 0.0
        self._live_analyzer_frame_cache: Dict[str, Dict[str, Any]] = {}
        self._live_analyzer_frame_cache_lock = threading.Lock()
        self._buffer_seq_lock = threading.Lock()
        self._live_analyzer_frame_seq = 0
        self._fast_signal_frame_seq = 0
        self._sensor_read_reject_states: Dict[Tuple[str, str, str, str, str], Dict[str, Any]] = {}
        self._preseal_dewpoint_snapshot: Optional[Dict[str, Any]] = None
        self._preseal_pressure_control_ready_state: Optional[Dict[str, Any]] = None
        self._temperature_wait_context: Optional[Dict[str, Any]] = None
        self._post_h2o_co2_zero_flush_pending = False
        self._initial_co2_zero_flush_pending = self._route_mode() == "co2_only"
        self._active_post_h2o_co2_zero_flush = False
        self._first_co2_route_soak_pending = True
        self._last_cold_co2_zero_flush_temp_c: Optional[float] = None
        self._temperature_calibration_records: List[Dict[str, Any]] = []
        self._temperature_calibration_capture_keys: set[tuple[str, str]] = set()
        self._pressure_capture_then_hold_cfg_logged = False
        self._adaptive_pressure_sampling_cfg_logged = False
        self._soft_pressure_control_cfg_logged = False
        self._pressure_gauge_sampling_freshness_warning_logged = False
        self._pressure_selection_log_signatures: set[Tuple[str, Tuple[Any, ...], Tuple[int, ...]]] = set()
        run_dir = getattr(self.logger, "run_dir", None)
        self._pressure_trace_path = (run_dir / "pressure_transition_trace.csv") if run_dir is not None else None
        self._pressure_trace_error_logged = False
        self._point_timing_summary_path = (run_dir / "point_timing_summary.csv") if run_dir is not None else None
        self._point_timing_summary_error_logged = False
        self._sampling_window_context: Optional[Dict[str, Any]] = None
        self._pressure_transition_fast_signal_context: Optional[Dict[str, Any]] = None
        self._pace_state_cache: Dict[str, Any] = {}
        self._relay_state_cache: Dict[Tuple[str, int], bool] = {}
        self._relay_state_cache_lock = threading.Lock()
        self._deferred_sample_exports: List[Dict[str, Any]] = []
        self._deferred_point_exports: List[Dict[str, Any]] = []
        self._last_sample_completion: Optional[Dict[str, Any]] = None
        self._pending_route_handoff: Optional[Dict[str, Any]] = None
        self._sample_handoff_request: Optional[Dict[str, Any]] = None
        self._sample_export_deferral_request: Optional[Dict[str, Any]] = None
        self._atmosphere_reference_hpa: Optional[float] = None
        self._point_runtime_summary: Dict[Tuple[str, int], Dict[str, Any]] = {}
        self._lead_in_transition_stage_ts: Dict[str, float] = {}
        self._last_preseal_pressure_control_ready_invalidation: Optional[Dict[str, Any]] = None
        self._active_route_requires_preseal_topoff = True

    def _log_run_event(self, command: Any = None, response: Any = None, error: Any = None) -> None:
        try:
            self.logger.log_io(
                port="RUN",
                device="runner",
                direction="EVENT",
                command=command,
                response=response,
                error=error,
            )
        except Exception:
            pass

    def _log_data_quality_effective_config(self) -> None:
        frame_cfg = dict(cfg_get(self.cfg, "workflow.analyzer_frame_quality", {}) or {})
        strict_required_keys = list(
            frame_cfg.get("strict_required_keys", ["co2_ratio_f", "h2o_ratio_f", "co2_ppm", "h2o_mmol"]) or []
        )
        relaxed_required_keys = list(
            frame_cfg.get("relaxed_required_keys", ["chamber_temp_c", "case_temp_c", "temp_c"]) or []
        )
        runtime_hard_bad_status_tokens = list(
            frame_cfg.get("runtime_hard_bad_status_tokens", ["FAIL", "INVALID", "ERROR"]) or []
        )
        runtime_soft_bad_status_tokens = list(
            frame_cfg.get("runtime_soft_bad_status_tokens", ["NO_RESPONSE", "NO_ACK"]) or []
        )
        live_snapshot_cfg = dict(cfg_get(self.cfg, "workflow.analyzer_live_snapshot", {}) or {})
        self.log(
            "Data-quality config: "
            f"workflow.summary_alignment.reference_on_aligned_rows="
            f"{bool(cfg_get(self.cfg, 'workflow.summary_alignment.reference_on_aligned_rows', True))} "
            f"workflow.sampling.quality.per_analyzer="
            f"{bool(cfg_get(self.cfg, 'workflow.sampling.quality.per_analyzer', False))} "
            f"coefficients.ratio_poly_fit.pressure_source_preference="
            f"{str(cfg_get(self.cfg, 'coefficients.ratio_poly_fit.pressure_source_preference', 'reference_first'))} "
            f"workflow.analyzer_frame_quality.min_mode2_fields="
            f"{int(frame_cfg.get('min_mode2_fields', 16) or 16)} "
            f"workflow.analyzer_frame_quality.runtime_relaxed_for_required_key="
            f"{bool(frame_cfg.get('runtime_relaxed_for_required_key', True))} "
            f"workflow.analyzer_frame_quality.strict_required_keys={strict_required_keys} "
            f"workflow.analyzer_frame_quality.relaxed_required_keys={relaxed_required_keys} "
            f"workflow.analyzer_frame_quality.runtime_hard_bad_status_tokens={runtime_hard_bad_status_tokens} "
            f"workflow.analyzer_frame_quality.runtime_soft_bad_status_tokens={runtime_soft_bad_status_tokens} "
            f"workflow.analyzer_frame_quality.reject_log_window_s="
            f"{float(frame_cfg.get('reject_log_window_s', 15.0) or 15.0):g} "
            f"workflow.analyzer_frame_quality.invalid_sentinel_values="
            f"{list(frame_cfg.get('invalid_sentinel_values', [-1001, -9999, 999999]) or [])} "
            f"workflow.analyzer_frame_quality.pressure_kpa_range="
            f"[{frame_cfg.get('pressure_kpa_min', 30.0)},{frame_cfg.get('pressure_kpa_max', 150.0)}] "
            f"workflow.analyzer_live_snapshot.enabled={bool(live_snapshot_cfg.get('enabled', True))} "
            f"workflow.analyzer_live_snapshot.interval_s={float(live_snapshot_cfg.get('interval_s', 5.0) or 5.0):g} "
            f"workflow.analyzer_live_snapshot.cache_ttl_s={float(live_snapshot_cfg.get('cache_ttl_s', 0.5) or 0.5):g}"
        )

    def _route_group_for_point(self, point: Optional[CalibrationPoint], phase: str = "") -> str:
        phase_text = str(phase or "").strip().lower()
        if point is not None and (phase_text == "h2o" or getattr(point, "is_h2o_point", False)):
            return "水路"
        if point is None:
            return "--"

        group = _normalized_co2_group(getattr(point, "co2_group", ""))
        if group == "B":
            return "第二组气路"

        ppm = self._as_int(getattr(point, "co2_ppm", None))
        if ppm in {100, 300, 500, 700, 900}:
            return "第二组气路"
        if ppm is not None:
            return "第一组气路"
        return "--"

    def _stage_label_for_point(
        self,
        point: Optional[CalibrationPoint],
        *,
        phase: str = "",
        include_pressure: bool = True,
    ) -> str:
        if point is None:
            return "--"

        phase_text = str(phase or "").strip().lower()
        if phase_text == "h2o" or getattr(point, "is_h2o_point", False):
            hgen_temp = self._as_float(getattr(point, "hgen_temp_c", None))
            hgen_rh = self._as_float(getattr(point, "hgen_rh_pct", None))
            if hgen_temp is not None or hgen_rh is not None:
                temp_text = f"{hgen_temp:g}°C" if hgen_temp is not None else "--°C"
                rh_text = f"{hgen_rh:g}%RH" if hgen_rh is not None else "--%RH"
                label = f"H2O {temp_text}/{rh_text}"
            else:
                label = f"H2O row {point.index}"
        else:
            ppm = self._as_float(getattr(point, "co2_ppm", None))
            if ppm is not None:
                label = f"CO2 {int(round(float(ppm)))}ppm"
            else:
                label = f"CO2 row {point.index}"

        pressure_label = self._pressure_target_label(point)
        if include_pressure and pressure_label:
            label += f" {pressure_label}"
        return label

    def _emit_stage_event(
        self,
        *,
        current: str,
        point: Optional[CalibrationPoint] = None,
        phase: str = "",
        point_tag: str = "",
        route_group: Optional[str] = None,
        wait_reason: Optional[str] = None,
        countdown_s: Optional[float] = None,
        detail: Optional[str] = None,
    ) -> None:
        payload: Dict[str, Any] = {"current": str(current)}
        group_text = route_group or self._route_group_for_point(point, phase=phase)
        if group_text and group_text != "--":
            payload["route_group"] = group_text
        if wait_reason:
            payload["wait_reason"] = str(wait_reason)
        if countdown_s is not None:
            payload["countdown_s"] = max(0, int(round(float(countdown_s))))
        if detail:
            payload["detail"] = str(detail)
        if point is not None:
            payload["point_row"] = int(point.index)
            phase_text = str(phase or ("h2o" if point.is_h2o_point else "co2")).strip().lower()
            payload["point_phase"] = phase_text
            pressure_hpa = self._as_float(getattr(point, "target_pressure_hpa", None))
            if pressure_hpa is not None:
                payload["pressure_hpa"] = int(round(float(pressure_hpa)))
            co2_ppm = self._as_float(getattr(point, "co2_ppm", None))
            if co2_ppm is not None:
                payload["co2_ppm"] = int(round(float(co2_ppm)))
            temp_c = self._as_float(getattr(point, "temp_chamber_c", None))
            if temp_c is not None:
                payload["temp_c"] = float(temp_c)
        if point_tag:
            payload["point_tag"] = point_tag
        self._log_run_event(
            command="stage",
            response=json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        )

    def _emit_sample_progress_event(
        self,
        current_count: int,
        total_count: int,
        *,
        point: Optional[CalibrationPoint] = None,
        phase: str = "",
        point_tag: str = "",
    ) -> None:
        target = max(1, int(total_count))
        current = max(0, min(int(current_count), target))
        payload: Dict[str, Any] = {
            "current": current,
            "total": target,
            "text": f"采样进度：{current}/{target}",
        }
        if point is not None:
            payload["point_row"] = int(point.index)
            payload["point_phase"] = str(phase or ("h2o" if point.is_h2o_point else "co2")).strip().lower()
            payload["route_group"] = self._route_group_for_point(point, phase=phase)
        if point_tag:
            payload["point_tag"] = point_tag
        self._log_run_event(
            command="sample-progress",
            response=json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        )

    def _pressure_trace_enabled(self, point: Optional[CalibrationPoint] = None) -> bool:
        return bool(self._wf("workflow.pressure.transition_trace_enabled", True))

    def _pressure_trace_poll_s(self, point: Optional[CalibrationPoint] = None) -> float:
        return max(0.05, float(self._wf("workflow.pressure.transition_trace_poll_s", 0.5)))

    @staticmethod
    def _pressure_trace_cell(value: Any) -> Any:
        return "" if value is None else value

    @staticmethod
    def _ts_from_datetime(value: datetime) -> str:
        return value.isoformat(timespec="milliseconds")

    def _pace_state_cache_snapshot(self) -> Dict[str, Any]:
        snapshot = {
            "pace_output_state": "",
            "pace_isolation_state": "",
            "pace_vent_status": "",
        }
        if not self._sampling_pace_state_cache_enabled():
            return snapshot
        cached = self._pace_state_cache if isinstance(self._pace_state_cache, dict) else {}
        for key in snapshot.keys():
            if key in cached:
                snapshot[key] = cached.get(key, "")
        return snapshot

    def _update_pace_state_cache(self, snapshot: Dict[str, Any]) -> None:
        if not self._sampling_pace_state_cache_enabled():
            return
        self._pace_state_cache = {
            "sample_ts": self._ts_from_datetime(datetime.now()),
            "timestamp": time.time(),
            "pace_output_state": snapshot.get("pace_output_state", ""),
            "pace_isolation_state": snapshot.get("pace_isolation_state", ""),
            "pace_vent_status": snapshot.get("pace_vent_status", ""),
        }

    def _pace_state_snapshot(self, pace: Any = None, *, refresh: bool = True) -> Dict[str, Any]:
        pace = pace if pace is not None else self.devices.get("pace")
        snapshot = self._pace_state_cache_snapshot()
        if not pace:
            return snapshot
        if not refresh:
            return snapshot
        refreshed = dict(snapshot)
        any_success = False
        for attr_name, key in (
            ("get_output_state", "pace_output_state"),
            ("get_isolation_state", "pace_isolation_state"),
            ("get_vent_status", "pace_vent_status"),
        ):
            getter = getattr(pace, attr_name, None)
            if not callable(getter):
                continue
            try:
                refreshed[key] = getter()
                any_success = True
            except Exception:
                refreshed[key] = snapshot.get(key, "")
        if any_success:
            self._update_pace_state_cache(refreshed)
            return refreshed
        return snapshot

    def _pressure_soft_control_trace_context(self) -> Dict[str, Any]:
        return {
            "soft_control_enabled": bool(self._wf("workflow.pressure.soft_control_enabled", False)),
            "soft_control_linear_slew_hpa_per_s": self._as_float(
                self._wf("workflow.pressure.soft_control_linear_slew_hpa_per_s", 10.0)
            ),
        }

    def _pressure_atmosphere_trace_context(self) -> Dict[str, Any]:
        return {
            "vent_after_valve_supported": self._pace_vent_after_valve_supported,
            "vent_after_valve_open": self._pace_vent_after_valve_open,
            "vent_popup_ack_enabled": self._pace_vent_popup_ack_enabled,
            "atmosphere_hold_strategy": self._pressure_atmosphere_hold_strategy,
        }

    def _pressure_trace_row(
        self,
        *,
        point: Optional[CalibrationPoint],
        route: str,
        trace_stage: str,
        point_phase: str = "",
        point_tag: str = "",
        trigger_reason: str = "",
        pressure_target_hpa: Any = None,
        pace_pressure_hpa: Any = None,
        pressure_gauge_hpa: Any = None,
        dewpoint_c: Any = None,
        dew_temp_c: Any = None,
        dew_rh_pct: Any = None,
        pace_output_state: Any = None,
        pace_isolation_state: Any = None,
        pace_vent_status: Any = None,
        vent_after_valve_supported: Any = None,
        vent_after_valve_open: Any = None,
        vent_popup_ack_enabled: Any = None,
        atmosphere_hold_strategy: Any = None,
        fast_group_span_ms: Any = None,
        handoff_sample_to_vent_ms: Any = None,
        handoff_vent_to_safe_open_ms: Any = None,
        handoff_safe_open_to_route_open_ms: Any = None,
        handoff_total_ms: Any = None,
        atmosphere_reference_hpa: Any = None,
        handoff_safe_open_delta_hpa: Any = None,
        deferred_export_queue_len: Any = None,
        dewpoint_live_c: Any = None,
        dew_temp_live_c: Any = None,
        dew_rh_live_pct: Any = None,
        dewpoint_gate_window_s: Any = None,
        dewpoint_gate_elapsed_s: Any = None,
        dewpoint_gate_span_c: Any = None,
        dewpoint_gate_slope_c_per_s: Any = None,
        dewpoint_gate_count: Any = None,
        dewpoint_time_to_gate: Any = None,
        dewpoint_tail_span_60s: Any = None,
        dewpoint_tail_slope_60s: Any = None,
        dewpoint_rebound_detected: Any = None,
        flush_gate_status: Any = None,
        flush_gate_reason: Any = None,
        sample_lag_ms: Any = None,
        soft_control_enabled: Any = None,
        soft_control_linear_slew_hpa_per_s: Any = None,
        event_ts: Optional[float] = None,
        note: str = "",
    ) -> Dict[str, Any]:
        phase_text = str(point_phase or ("h2o" if point is not None and point.is_h2o_point else "co2")).strip().lower()
        row = {field: "" for field in _PRESSURE_TRACE_FIELDS}
        soft_cfg = self._pressure_soft_control_trace_context()
        atmosphere_cfg = self._pressure_atmosphere_trace_context()
        event_dt = datetime.fromtimestamp(float(event_ts)) if event_ts is not None else datetime.now()
        row.update(
            {
                "ts": event_dt.isoformat(timespec="milliseconds"),
                "point_title": self._point_title(point, phase=phase_text, point_tag=point_tag) if point is not None else "",
                "point_phase": phase_text,
                "point_tag": str(point_tag or ""),
                "point_row": "" if point is None else int(point.index),
                "route": str(route or phase_text or "").strip().lower(),
                "trace_stage": str(trace_stage or "").strip(),
                "trigger_reason": str(trigger_reason or "").strip(),
                "pressure_target_hpa": self._pressure_trace_cell(
                    pressure_target_hpa if pressure_target_hpa is not None else getattr(point, "target_pressure_hpa", None)
                ),
                "pace_pressure_hpa": self._pressure_trace_cell(pace_pressure_hpa),
                "pressure_gauge_hpa": self._pressure_trace_cell(pressure_gauge_hpa),
                "dewpoint_c": self._pressure_trace_cell(dewpoint_c),
                "dew_temp_c": self._pressure_trace_cell(dew_temp_c),
                "dew_rh_pct": self._pressure_trace_cell(dew_rh_pct),
                "pace_output_state": self._pressure_trace_cell(pace_output_state),
                "pace_isolation_state": self._pressure_trace_cell(pace_isolation_state),
                "pace_vent_status": self._pressure_trace_cell(pace_vent_status),
                "vent_after_valve_supported": self._pressure_trace_cell(
                    atmosphere_cfg["vent_after_valve_supported"]
                    if vent_after_valve_supported is None
                    else vent_after_valve_supported
                ),
                "vent_after_valve_open": self._pressure_trace_cell(
                    atmosphere_cfg["vent_after_valve_open"]
                    if vent_after_valve_open is None
                    else vent_after_valve_open
                ),
                "vent_popup_ack_enabled": self._pressure_trace_cell(
                    atmosphere_cfg["vent_popup_ack_enabled"]
                    if vent_popup_ack_enabled is None
                    else vent_popup_ack_enabled
                ),
                "atmosphere_hold_strategy": self._pressure_trace_cell(
                    atmosphere_cfg["atmosphere_hold_strategy"]
                    if atmosphere_hold_strategy is None
                    else atmosphere_hold_strategy
                ),
                "fast_group_span_ms": self._pressure_trace_cell(fast_group_span_ms),
                "handoff_sample_to_vent_ms": self._pressure_trace_cell(handoff_sample_to_vent_ms),
                "handoff_vent_to_safe_open_ms": self._pressure_trace_cell(handoff_vent_to_safe_open_ms),
                "handoff_safe_open_to_route_open_ms": self._pressure_trace_cell(handoff_safe_open_to_route_open_ms),
                "handoff_total_ms": self._pressure_trace_cell(handoff_total_ms),
                "atmosphere_reference_hpa": self._pressure_trace_cell(atmosphere_reference_hpa),
                "handoff_safe_open_delta_hpa": self._pressure_trace_cell(handoff_safe_open_delta_hpa),
                "deferred_export_queue_len": self._pressure_trace_cell(deferred_export_queue_len),
                "dewpoint_live_c": self._pressure_trace_cell(dewpoint_live_c),
                "dew_temp_live_c": self._pressure_trace_cell(dew_temp_live_c),
                "dew_rh_live_pct": self._pressure_trace_cell(dew_rh_live_pct),
                "dewpoint_gate_window_s": self._pressure_trace_cell(dewpoint_gate_window_s),
                "dewpoint_gate_elapsed_s": self._pressure_trace_cell(dewpoint_gate_elapsed_s),
                "dewpoint_gate_span_c": self._pressure_trace_cell(dewpoint_gate_span_c),
                "dewpoint_gate_slope_c_per_s": self._pressure_trace_cell(dewpoint_gate_slope_c_per_s),
                "dewpoint_gate_count": self._pressure_trace_cell(dewpoint_gate_count),
                "dewpoint_time_to_gate": self._pressure_trace_cell(dewpoint_time_to_gate),
                "dewpoint_tail_span_60s": self._pressure_trace_cell(dewpoint_tail_span_60s),
                "dewpoint_tail_slope_60s": self._pressure_trace_cell(dewpoint_tail_slope_60s),
                "dewpoint_rebound_detected": self._pressure_trace_cell(dewpoint_rebound_detected),
                "flush_gate_status": self._pressure_trace_cell(flush_gate_status),
                "flush_gate_reason": self._pressure_trace_cell(flush_gate_reason),
                "sample_lag_ms": self._pressure_trace_cell(sample_lag_ms),
                "soft_control_enabled": self._pressure_trace_cell(
                    soft_cfg["soft_control_enabled"] if soft_control_enabled is None else soft_control_enabled
                ),
                "soft_control_linear_slew_hpa_per_s": self._pressure_trace_cell(
                    soft_cfg["soft_control_linear_slew_hpa_per_s"]
                    if soft_control_linear_slew_hpa_per_s is None
                    else soft_control_linear_slew_hpa_per_s
                ),
                "note": str(note or ""),
            }
        )
        return row

    def _capture_pressure_trace_snapshot(
        self,
        *,
        point: Optional[CalibrationPoint],
        route: str,
        point_phase: str = "",
        point_tag: str = "",
        trigger_reason: str = "",
        pressure_target_hpa: Any = None,
        pace_pressure_hpa: Any = None,
        pressure_gauge_hpa: Any = None,
        dewpoint_c: Any = None,
        dew_temp_c: Any = None,
        dew_rh_pct: Any = None,
        pace_output_state: Any = None,
        pace_isolation_state: Any = None,
        pace_vent_status: Any = None,
        vent_after_valve_supported: Any = None,
        vent_after_valve_open: Any = None,
        vent_popup_ack_enabled: Any = None,
        atmosphere_hold_strategy: Any = None,
        read_pace_pressure: bool = False,
        read_pressure_gauge: bool = False,
        read_dewpoint: bool = False,
        refresh_pace_state: bool = True,
        fast_group_span_ms: Any = None,
        handoff_sample_to_vent_ms: Any = None,
        handoff_vent_to_safe_open_ms: Any = None,
        handoff_safe_open_to_route_open_ms: Any = None,
        handoff_total_ms: Any = None,
        atmosphere_reference_hpa: Any = None,
        handoff_safe_open_delta_hpa: Any = None,
        deferred_export_queue_len: Any = None,
        dewpoint_live_c: Any = None,
        dew_temp_live_c: Any = None,
        dew_rh_live_pct: Any = None,
        dewpoint_gate_window_s: Any = None,
        dewpoint_gate_elapsed_s: Any = None,
        dewpoint_gate_span_c: Any = None,
        dewpoint_gate_slope_c_per_s: Any = None,
        dewpoint_gate_count: Any = None,
        dewpoint_time_to_gate: Any = None,
        dewpoint_tail_span_60s: Any = None,
        dewpoint_tail_slope_60s: Any = None,
        dewpoint_rebound_detected: Any = None,
        flush_gate_status: Any = None,
        flush_gate_reason: Any = None,
        sample_lag_ms: Any = None,
        soft_control_enabled: Any = None,
        soft_control_linear_slew_hpa_per_s: Any = None,
        event_ts: Optional[float] = None,
        note: str = "",
    ) -> Dict[str, Any]:
        phase_text = str(point_phase or ("h2o" if point is not None and point.is_h2o_point else "co2")).strip().lower()
        pace = self.devices.get("pace")
        if read_pace_pressure and pace_pressure_hpa is None and pace:
            reader = getattr(pace, "read_pressure", None)
            if callable(reader):
                try:
                    pace_pressure_hpa = self._as_float(reader())
                except Exception:
                    pace_pressure_hpa = None

        if read_pressure_gauge and pressure_gauge_hpa is None:
            gauge = self.devices.get("pressure_gauge")
            reader = getattr(gauge, "read_pressure", None) if gauge else None
            if callable(reader):
                try:
                    pressure_gauge_hpa = self._as_float(reader())
                except Exception:
                    pressure_gauge_hpa = None

        if phase_text == "h2o" and self._preseal_dewpoint_snapshot:
            dewpoint_c = self._preseal_dewpoint_snapshot.get("dewpoint_c") if dewpoint_c is None else dewpoint_c
            dew_temp_c = self._preseal_dewpoint_snapshot.get("temp_c") if dew_temp_c is None else dew_temp_c
            dew_rh_pct = self._preseal_dewpoint_snapshot.get("rh_pct") if dew_rh_pct is None else dew_rh_pct
        elif read_dewpoint and (dewpoint_c is None or dew_temp_c is None or dew_rh_pct is None):
            dew = self.devices.get("dewpoint")
            reader = getattr(dew, "get_current", None) if dew else None
            if callable(reader):
                try:
                    dew_data = reader()
                except Exception:
                    dew_data = {}
                if isinstance(dew_data, dict):
                    if dewpoint_c is None:
                        dewpoint_c = dew_data.get("dewpoint_c")
                    if dew_temp_c is None:
                        dew_temp_c = dew_data.get("temp_c")
                    if dew_rh_pct is None:
                        dew_rh_pct = dew_data.get("rh_pct")

        if pace_output_state is None or pace_isolation_state is None or pace_vent_status is None:
            pace_state = self._pace_state_snapshot(pace, refresh=refresh_pace_state)
            if pace_output_state is None:
                pace_output_state = pace_state["pace_output_state"]
            if pace_isolation_state is None:
                pace_isolation_state = pace_state["pace_isolation_state"]
            if pace_vent_status is None:
                pace_vent_status = pace_state["pace_vent_status"]

        return self._pressure_trace_row(
            point=point,
            route=route,
            point_phase=phase_text,
            point_tag=point_tag,
            trace_stage="",
            trigger_reason=trigger_reason,
            pressure_target_hpa=pressure_target_hpa,
            pace_pressure_hpa=pace_pressure_hpa,
            pressure_gauge_hpa=pressure_gauge_hpa,
            dewpoint_c=dewpoint_c,
            dew_temp_c=dew_temp_c,
            dew_rh_pct=dew_rh_pct,
            pace_output_state=pace_output_state,
            pace_isolation_state=pace_isolation_state,
            pace_vent_status=pace_vent_status,
            vent_after_valve_supported=vent_after_valve_supported,
            vent_after_valve_open=vent_after_valve_open,
            vent_popup_ack_enabled=vent_popup_ack_enabled,
            atmosphere_hold_strategy=atmosphere_hold_strategy,
            fast_group_span_ms=fast_group_span_ms,
            handoff_sample_to_vent_ms=handoff_sample_to_vent_ms,
            handoff_vent_to_safe_open_ms=handoff_vent_to_safe_open_ms,
            handoff_safe_open_to_route_open_ms=handoff_safe_open_to_route_open_ms,
            handoff_total_ms=handoff_total_ms,
            atmosphere_reference_hpa=atmosphere_reference_hpa,
            handoff_safe_open_delta_hpa=handoff_safe_open_delta_hpa,
            deferred_export_queue_len=deferred_export_queue_len,
            dewpoint_live_c=dewpoint_live_c,
            dew_temp_live_c=dew_temp_live_c,
            dew_rh_live_pct=dew_rh_live_pct,
            dewpoint_gate_window_s=dewpoint_gate_window_s,
            dewpoint_gate_elapsed_s=dewpoint_gate_elapsed_s,
            dewpoint_gate_span_c=dewpoint_gate_span_c,
            dewpoint_gate_slope_c_per_s=dewpoint_gate_slope_c_per_s,
            dewpoint_gate_count=dewpoint_gate_count,
            dewpoint_time_to_gate=dewpoint_time_to_gate,
            dewpoint_tail_span_60s=dewpoint_tail_span_60s,
            dewpoint_tail_slope_60s=dewpoint_tail_slope_60s,
            dewpoint_rebound_detected=dewpoint_rebound_detected,
            flush_gate_status=flush_gate_status,
            flush_gate_reason=flush_gate_reason,
            sample_lag_ms=sample_lag_ms,
            soft_control_enabled=soft_control_enabled,
            soft_control_linear_slew_hpa_per_s=soft_control_linear_slew_hpa_per_s,
            event_ts=event_ts,
            note=note,
        )

    def _append_pressure_trace_row(
        self,
        *,
        point: Optional[CalibrationPoint],
        route: str,
        trace_stage: str,
        point_phase: str = "",
        point_tag: str = "",
        trigger_reason: str = "",
        pressure_target_hpa: Any = None,
        pace_pressure_hpa: Any = None,
        pressure_gauge_hpa: Any = None,
        dewpoint_c: Any = None,
        dew_temp_c: Any = None,
        dew_rh_pct: Any = None,
        pace_output_state: Any = None,
        pace_isolation_state: Any = None,
        pace_vent_status: Any = None,
        vent_after_valve_supported: Any = None,
        vent_after_valve_open: Any = None,
        vent_popup_ack_enabled: Any = None,
        atmosphere_hold_strategy: Any = None,
        read_pace_pressure: bool = False,
        read_pressure_gauge: bool = False,
        read_dewpoint: bool = False,
        refresh_pace_state: bool = True,
        fast_group_span_ms: Any = None,
        handoff_sample_to_vent_ms: Any = None,
        handoff_vent_to_safe_open_ms: Any = None,
        handoff_safe_open_to_route_open_ms: Any = None,
        handoff_total_ms: Any = None,
        atmosphere_reference_hpa: Any = None,
        handoff_safe_open_delta_hpa: Any = None,
        deferred_export_queue_len: Any = None,
        dewpoint_live_c: Any = None,
        dew_temp_live_c: Any = None,
        dew_rh_live_pct: Any = None,
        dewpoint_gate_window_s: Any = None,
        dewpoint_gate_elapsed_s: Any = None,
        dewpoint_gate_span_c: Any = None,
        dewpoint_gate_slope_c_per_s: Any = None,
        dewpoint_gate_count: Any = None,
        dewpoint_time_to_gate: Any = None,
        dewpoint_tail_span_60s: Any = None,
        dewpoint_tail_slope_60s: Any = None,
        dewpoint_rebound_detected: Any = None,
        flush_gate_status: Any = None,
        flush_gate_reason: Any = None,
        sample_lag_ms: Any = None,
        soft_control_enabled: Any = None,
        soft_control_linear_slew_hpa_per_s: Any = None,
        event_ts: Optional[float] = None,
        note: str = "",
    ) -> None:
        if not self._pressure_trace_enabled(point):
            return
        if self._pressure_trace_path is None:
            return

        try:
            row = self._capture_pressure_trace_snapshot(
                point=point,
                route=route,
                point_phase=point_phase,
                point_tag=point_tag,
                trigger_reason=trigger_reason,
                pressure_target_hpa=pressure_target_hpa,
                pace_pressure_hpa=pace_pressure_hpa,
                pressure_gauge_hpa=pressure_gauge_hpa,
                dewpoint_c=dewpoint_c,
                dew_temp_c=dew_temp_c,
                dew_rh_pct=dew_rh_pct,
                pace_output_state=pace_output_state,
                pace_isolation_state=pace_isolation_state,
                pace_vent_status=pace_vent_status,
                vent_after_valve_supported=vent_after_valve_supported,
                vent_after_valve_open=vent_after_valve_open,
                vent_popup_ack_enabled=vent_popup_ack_enabled,
                atmosphere_hold_strategy=atmosphere_hold_strategy,
                read_pace_pressure=read_pace_pressure,
                read_pressure_gauge=read_pressure_gauge,
                read_dewpoint=read_dewpoint,
                refresh_pace_state=refresh_pace_state,
                fast_group_span_ms=fast_group_span_ms,
                handoff_sample_to_vent_ms=handoff_sample_to_vent_ms,
                handoff_vent_to_safe_open_ms=handoff_vent_to_safe_open_ms,
                handoff_safe_open_to_route_open_ms=handoff_safe_open_to_route_open_ms,
                handoff_total_ms=handoff_total_ms,
                atmosphere_reference_hpa=atmosphere_reference_hpa,
                handoff_safe_open_delta_hpa=handoff_safe_open_delta_hpa,
                deferred_export_queue_len=deferred_export_queue_len,
                dewpoint_live_c=dewpoint_live_c,
                dew_temp_live_c=dew_temp_live_c,
                dew_rh_live_pct=dew_rh_live_pct,
                dewpoint_gate_window_s=dewpoint_gate_window_s,
                dewpoint_gate_elapsed_s=dewpoint_gate_elapsed_s,
                dewpoint_gate_span_c=dewpoint_gate_span_c,
                dewpoint_gate_slope_c_per_s=dewpoint_gate_slope_c_per_s,
                dewpoint_gate_count=dewpoint_gate_count,
                dewpoint_time_to_gate=dewpoint_time_to_gate,
                dewpoint_tail_span_60s=dewpoint_tail_span_60s,
                dewpoint_tail_slope_60s=dewpoint_tail_slope_60s,
                dewpoint_rebound_detected=dewpoint_rebound_detected,
                flush_gate_status=flush_gate_status,
                flush_gate_reason=flush_gate_reason,
                sample_lag_ms=sample_lag_ms,
                soft_control_enabled=soft_control_enabled,
                soft_control_linear_slew_hpa_per_s=soft_control_linear_slew_hpa_per_s,
                event_ts=event_ts,
                note=note,
            )
            row["trace_stage"] = str(trace_stage or "").strip()

            self._pressure_trace_path.parent.mkdir(parents=True, exist_ok=True)
            needs_header = (
                not self._pressure_trace_path.exists()
                or self._pressure_trace_path.stat().st_size <= 0
            )
            with self._pressure_trace_path.open("a", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=_PRESSURE_TRACE_FIELDS, extrasaction="ignore")
                if needs_header:
                    writer.writeheader()
                writer.writerow(row)
            self._record_runtime_state_from_pressure_trace_row(row)
        except Exception as exc:
            if not self._pressure_trace_error_logged:
                self._pressure_trace_error_logged = True
                self.log(f"Pressure transition trace append failed: {exc}")

    @staticmethod
    def _point_runtime_key_from_values(*, phase: str, point_row: Any) -> Optional[Tuple[str, int]]:
        phase_text = str(phase or "").strip().lower()
        try:
            row_value = int(point_row)
        except Exception:
            return None
        if not phase_text:
            return None
        return phase_text, row_value

    def _point_runtime_key(
        self,
        point: Optional[CalibrationPoint],
        *,
        phase: str,
    ) -> Optional[Tuple[str, int]]:
        if point is None:
            return None
        return self._point_runtime_key_from_values(phase=phase, point_row=getattr(point, "index", None))

    def _point_runtime_state(
        self,
        point: Optional[CalibrationPoint] = None,
        *,
        phase: str,
        create: bool = False,
        point_key: Optional[Tuple[str, int]] = None,
    ) -> Optional[Dict[str, Any]]:
        key = point_key if point_key is not None else self._point_runtime_key(point, phase=phase)
        if key is None:
            return None
        if create:
            return self._point_runtime_summary.setdefault(key, {})
        return self._point_runtime_summary.get(key)

    def _set_point_runtime_fields(
        self,
        point: Optional[CalibrationPoint],
        *,
        phase: str,
        **fields: Any,
    ) -> None:
        state = self._point_runtime_state(point, phase=phase, create=True)
        if state is None:
            return
        for key, value in fields.items():
            state[key] = value

    @staticmethod
    def _trace_row_ts_epoch(row: Dict[str, Any]) -> Optional[float]:
        value = row.get("ts")
        if value in (None, ""):
            return None
        try:
            return datetime.fromisoformat(str(value)).timestamp()
        except Exception:
            return None

    def _record_runtime_state_from_pressure_trace_row(self, row: Dict[str, Any]) -> None:
        if not isinstance(row, dict):
            return
        stage = str(row.get("trace_stage") or "").strip()
        if not stage:
            return
        event_ts = self._trace_row_ts_epoch(row)
        if event_ts is None:
            return

        point_key = self._point_runtime_key_from_values(
            phase=str(row.get("point_phase") or ""),
            point_row=row.get("point_row"),
        )
        if point_key is None:
            if stage in {"atmosphere_enter_begin", "atmosphere_enter_verified", "route_open", "soak_begin", "soak_end"}:
                self._lead_in_transition_stage_ts[stage] = event_ts
            return

        state = self._point_runtime_state(phase=point_key[0], create=True, point_key=point_key)
        if state is None:
            return
        point_tag = str(row.get("point_tag") or "")
        if point_tag and "point_tag" not in state:
            state["point_tag"] = point_tag
        stages = state.setdefault("timing_stages", {})

        recorded_stage = stage
        if stage == "handoff_next_route_open_done":
            recorded_stage = "route_open"
        if recorded_stage == "route_open":
            prev_sample_done_ts = self._as_float((self._last_sample_completion or {}).get("sample_done_ts"))
            if prev_sample_done_ts is not None and "prev_sampling_end" not in stages:
                stages["prev_sampling_end"] = prev_sample_done_ts
            for lead_stage in ("atmosphere_enter_begin", "atmosphere_enter_verified"):
                if lead_stage in self._lead_in_transition_stage_ts and lead_stage not in stages:
                    stages[lead_stage] = self._lead_in_transition_stage_ts[lead_stage]
        elif recorded_stage in {"preseal_vent_off_begin", "preseal_trigger_reached", "route_sealed", "control_prepare_begin"}:
            if "prev_sampling_end" not in stages:
                prev_sample_done_ts = self._as_float((self._last_sample_completion or {}).get("sample_done_ts"))
                if prev_sample_done_ts is not None:
                    stages["prev_sampling_end"] = prev_sample_done_ts
            for lead_stage in (
                "atmosphere_enter_begin",
                "atmosphere_enter_verified",
                "route_open",
                "soak_begin",
                "soak_end",
            ):
                if lead_stage in self._lead_in_transition_stage_ts and lead_stage not in stages:
                    stages[lead_stage] = self._lead_in_transition_stage_ts[lead_stage]

        if recorded_stage not in stages:
            stages[recorded_stage] = event_ts
        if recorded_stage in {"route_open", "soak_begin", "soak_end"}:
            self._lead_in_transition_stage_ts[recorded_stage] = event_ts

    @staticmethod
    def _timing_ms(start_ts: Optional[float], end_ts: Optional[float]) -> Optional[float]:
        if start_ts is None or end_ts is None:
            return None
        return round((float(end_ts) - float(start_ts)) * 1000.0, 3)

    @staticmethod
    def _timing_ts_text(value: Optional[float]) -> str:
        if value is None:
            return ""
        return datetime.fromtimestamp(float(value)).isoformat(timespec="milliseconds")

    def _point_timing_delta_map(self, stages: Dict[str, Any]) -> Dict[str, Optional[float]]:
        return {
            "sampling_end_to_atmosphere_enter_begin_ms": self._timing_ms(
                self._as_float(stages.get("prev_sampling_end")),
                self._as_float(stages.get("atmosphere_enter_begin")),
            ),
            "atmosphere_enter_begin_to_atmosphere_enter_verified_ms": self._timing_ms(
                self._as_float(stages.get("atmosphere_enter_begin")),
                self._as_float(stages.get("atmosphere_enter_verified")),
            ),
            "atmosphere_enter_verified_to_route_open_ms": self._timing_ms(
                self._as_float(stages.get("atmosphere_enter_verified")),
                self._as_float(stages.get("route_open")),
            ),
            "route_open_to_soak_begin_ms": self._timing_ms(
                self._as_float(stages.get("route_open")),
                self._as_float(stages.get("soak_begin")),
            ),
            "soak_begin_to_soak_end_ms": self._timing_ms(
                self._as_float(stages.get("soak_begin")),
                self._as_float(stages.get("soak_end")),
            ),
            "soak_end_to_preseal_vent_off_begin_ms": self._timing_ms(
                self._as_float(stages.get("soak_end")),
                self._as_float(stages.get("preseal_vent_off_begin")),
            ),
            "preseal_vent_off_begin_to_preseal_trigger_reached_ms": self._timing_ms(
                self._as_float(stages.get("preseal_vent_off_begin")),
                self._as_float(stages.get("preseal_trigger_reached")),
            ),
            "preseal_trigger_reached_to_route_sealed_ms": self._timing_ms(
                self._as_float(stages.get("preseal_trigger_reached")),
                self._as_float(stages.get("route_sealed")),
            ),
            "preseal_vent_off_begin_to_route_sealed_ms": self._timing_ms(
                self._as_float(stages.get("preseal_vent_off_begin")),
                self._as_float(stages.get("route_sealed")),
            ),
            "route_sealed_to_control_prepare_begin_ms": self._timing_ms(
                self._as_float(stages.get("route_sealed")),
                self._as_float(stages.get("control_prepare_begin")),
            ),
            "control_prepare_begin_to_control_ready_snapshot_acquired_ms": self._timing_ms(
                self._as_float(stages.get("control_prepare_begin")),
                self._as_float(stages.get("control_ready_snapshot_acquired")),
            ),
            "control_ready_snapshot_acquired_to_control_ready_verified_ms": self._timing_ms(
                self._as_float(stages.get("control_ready_snapshot_acquired")),
                self._as_float(stages.get("control_ready_verified")),
            ),
            "control_ready_wait_ms": self._timing_ms(
                self._as_float(stages.get("control_ready_wait_begin")),
                self._as_float(stages.get("control_ready_wait_end")),
            ),
            "control_prepare_begin_to_control_ready_verified_ms": self._timing_ms(
                self._as_float(stages.get("control_prepare_begin")),
                self._as_float(stages.get("control_ready_verified")),
            ),
            "control_output_on_begin_to_control_output_on_command_sent_ms": self._timing_ms(
                self._as_float(stages.get("control_output_on_begin")),
                self._as_float(stages.get("control_output_on_command_sent")),
            ),
            "control_output_on_command_sent_to_control_output_on_verified_ms": self._timing_ms(
                self._as_float(stages.get("control_output_on_command_sent")),
                self._as_float(stages.get("control_output_on_verified")),
            ),
            "control_output_verify_wait_ms": self._timing_ms(
                self._as_float(stages.get("control_output_verify_wait_begin")),
                self._as_float(stages.get("control_output_verify_wait_end")),
            ),
            "control_output_on_begin_to_control_output_on_verified_ms": self._timing_ms(
                self._as_float(stages.get("control_output_on_begin")),
                self._as_float(stages.get("control_output_on_verified")),
            ),
            "control_output_on_verified_to_pressure_in_limits_ms": self._timing_ms(
                self._as_float(stages.get("control_output_on_verified")),
                self._as_float(stages.get("pressure_in_limits")),
            ),
            "pressure_in_limits_to_dewpoint_gate_begin_ms": self._timing_ms(
                self._as_float(stages.get("pressure_in_limits")),
                self._as_float(stages.get("dewpoint_gate_begin")),
            ),
            "dewpoint_gate_begin_to_dewpoint_gate_end_ms": self._timing_ms(
                self._as_float(stages.get("dewpoint_gate_begin")),
                self._as_float(stages.get("dewpoint_gate_end")),
            ),
            "dewpoint_gate_end_to_sampling_begin_ms": self._timing_ms(
                self._as_float(stages.get("dewpoint_gate_end")),
                self._as_float(stages.get("sampling_begin")),
            ),
            "pressure_in_limits_to_sampling_begin_ms": self._timing_ms(
                self._as_float(stages.get("pressure_in_limits")),
                self._as_float(stages.get("sampling_begin")),
            ),
            "sampling_begin_to_first_effective_sample_ms": self._timing_ms(
                self._as_float(stages.get("sampling_begin")),
                self._as_float(stages.get("first_effective_sample")),
            ),
        }

    def _append_point_timing_summary_row(self, row: Dict[str, Any]) -> None:
        if self._point_timing_summary_path is None:
            return
        try:
            self._point_timing_summary_path.parent.mkdir(parents=True, exist_ok=True)
            needs_header = (
                not self._point_timing_summary_path.exists()
                or self._point_timing_summary_path.stat().st_size <= 0
            )
            with self._point_timing_summary_path.open("a", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=_POINT_TIMING_SUMMARY_FIELDS, extrasaction="ignore")
                if needs_header:
                    writer.writeheader()
                writer.writerow({field: row.get(field, "") for field in _POINT_TIMING_SUMMARY_FIELDS})
        except Exception as exc:
            if not self._point_timing_summary_error_logged:
                self._point_timing_summary_error_logged = True
                self.log(f"Point timing summary append failed: {exc}")

    def _write_point_timing_summary(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        point_tag: str = "",
    ) -> None:
        state = self._point_runtime_state(point, phase=phase)
        if not isinstance(state, dict):
            return
        stages = dict(state.get("timing_stages") or {})
        if not stages:
            return

        configured_route_soak_s = self._as_float(state.get("configured_route_soak_s"))
        timing_row = {
            "point_title": self._point_title(point, phase=phase, point_tag=point_tag),
            "point_row": int(point.index),
            "point_phase": str(phase or "").strip().lower(),
            "point_tag": str(point_tag or state.get("point_tag") or ""),
            "pressure_target_hpa": self._as_float(getattr(point, "target_pressure_hpa", None)),
            "configured_route_soak_s": configured_route_soak_s,
            "prev_sampling_end_ts": self._timing_ts_text(self._as_float(stages.get("prev_sampling_end"))),
            "atmosphere_enter_begin_ts": self._timing_ts_text(self._as_float(stages.get("atmosphere_enter_begin"))),
            "atmosphere_enter_verified_ts": self._timing_ts_text(self._as_float(stages.get("atmosphere_enter_verified"))),
            "route_open_ts": self._timing_ts_text(self._as_float(stages.get("route_open"))),
            "soak_begin_ts": self._timing_ts_text(self._as_float(stages.get("soak_begin"))),
            "soak_end_ts": self._timing_ts_text(self._as_float(stages.get("soak_end"))),
            "preseal_vent_off_begin_ts": self._timing_ts_text(self._as_float(stages.get("preseal_vent_off_begin"))),
            "preseal_trigger_reached_ts": self._timing_ts_text(self._as_float(stages.get("preseal_trigger_reached"))),
            "route_sealed_ts": self._timing_ts_text(self._as_float(stages.get("route_sealed"))),
            "control_prepare_begin_ts": self._timing_ts_text(self._as_float(stages.get("control_prepare_begin"))),
            "control_ready_snapshot_acquired_ts": self._timing_ts_text(
                self._as_float(stages.get("control_ready_snapshot_acquired"))
            ),
            "control_ready_wait_begin_ts": self._timing_ts_text(self._as_float(stages.get("control_ready_wait_begin"))),
            "control_ready_wait_end_ts": self._timing_ts_text(self._as_float(stages.get("control_ready_wait_end"))),
            "control_ready_verified_ts": self._timing_ts_text(self._as_float(stages.get("control_ready_verified"))),
            "control_output_on_begin_ts": self._timing_ts_text(self._as_float(stages.get("control_output_on_begin"))),
            "control_output_on_command_sent_ts": self._timing_ts_text(
                self._as_float(stages.get("control_output_on_command_sent"))
            ),
            "control_output_verify_wait_begin_ts": self._timing_ts_text(
                self._as_float(stages.get("control_output_verify_wait_begin"))
            ),
            "control_output_verify_wait_end_ts": self._timing_ts_text(
                self._as_float(stages.get("control_output_verify_wait_end"))
            ),
            "control_output_on_verified_ts": self._timing_ts_text(
                self._as_float(stages.get("control_output_on_verified"))
            ),
            "pressure_in_limits_ts": self._timing_ts_text(self._as_float(stages.get("pressure_in_limits"))),
            "dewpoint_gate_begin_ts": self._timing_ts_text(self._as_float(stages.get("dewpoint_gate_begin"))),
            "dewpoint_gate_end_ts": self._timing_ts_text(self._as_float(stages.get("dewpoint_gate_end"))),
            "sampling_begin_ts": self._timing_ts_text(self._as_float(stages.get("sampling_begin"))),
            "first_effective_sample_ts": self._timing_ts_text(
                self._as_float(stages.get("first_effective_sample"))
            ),
            "sampling_end_ts": self._timing_ts_text(self._as_float(stages.get("sampling_end"))),
        }

        delta_map = self._point_timing_delta_map(stages)
        timing_row.update(delta_map)
        nonsoak_segments = [
            "sampling_end_to_atmosphere_enter_begin_ms",
            "atmosphere_enter_begin_to_atmosphere_enter_verified_ms",
            "atmosphere_enter_verified_to_route_open_ms",
            "route_open_to_soak_begin_ms",
            "soak_end_to_preseal_vent_off_begin_ms",
            "preseal_vent_off_begin_to_preseal_trigger_reached_ms",
            "preseal_trigger_reached_to_route_sealed_ms",
            "route_sealed_to_control_prepare_begin_ms",
            "control_prepare_begin_to_control_ready_verified_ms",
            "control_output_on_begin_to_control_output_on_verified_ms",
            "control_output_on_verified_to_pressure_in_limits_ms",
            "pressure_in_limits_to_dewpoint_gate_begin_ms",
            "dewpoint_gate_begin_to_dewpoint_gate_end_ms",
            "dewpoint_gate_end_to_sampling_begin_ms",
            "sampling_begin_to_first_effective_sample_ms",
        ]
        nonsoak_values = [delta_map[key] for key in nonsoak_segments if delta_map.get(key) is not None]
        timing_row["lead_in_nonsoak_ms"] = round(sum(nonsoak_values), 3) if nonsoak_values else ""
        self._append_point_timing_summary_row(timing_row)

    def _strict_control_ready_check_enabled(self) -> bool:
        return bool(self._wf("workflow.pressure.strict_control_ready_check", True))

    @staticmethod
    def _is_fast_preseal_vent_off_reason(reason: str = "") -> bool:
        text = str(reason or "").strip().lower()
        return text.startswith("before co2 pressure seal") or text.startswith("before h2o pressure seal")

    def _abort_on_vent_off_failure(self) -> bool:
        return bool(self._wf("workflow.pressure.abort_on_vent_off_failure", True))

    def _atmosphere_hold_strategy(self) -> str:
        text = str(self._wf("workflow.pressure.atmosphere_hold_strategy", "legacy_hold_thread") or "").strip()
        if text == "legacy_hold_thread":
            return "legacy_hold_thread"
        return "vent_valve_open_after_vent"

    def _continuous_atmosphere_hold_enabled(self) -> bool:
        return bool(self._wf("workflow.pressure.continuous_atmosphere_hold", True))

    def _vent_hold_interval_s(self) -> float:
        return max(0.1, float(self._wf("workflow.pressure.vent_hold_interval_s", 2.0) or 2.0))

    def _vent_after_valve_open_enabled(self) -> bool:
        return bool(self._wf("workflow.pressure.vent_after_valve_open", False))

    def _vent_popup_ack_override(self) -> Optional[bool]:
        disable_popup = bool(self._wf("workflow.pressure.vent_popup_ack_disable_for_automation", False))
        if disable_popup:
            return False
        return None

    def _refresh_pressure_controller_aux_state(self, pace: Any = None) -> None:
        pace = pace if pace is not None else self.devices.get("pace")
        if not pace:
            return
        support_getter = getattr(pace, "supports_vent_after_valve_open", None)
        if callable(support_getter):
            try:
                if not bool(support_getter()):
                    self._pace_vent_after_valve_supported = False
            except Exception:
                pass
        getter = getattr(pace, "get_vent_after_valve_open", None)
        if self._pace_vent_after_valve_supported is not False and callable(getter):
            try:
                self._pace_vent_after_valve_open = bool(getter())
                self._pace_vent_after_valve_supported = True
            except Exception:
                self._pace_vent_after_valve_supported = False
        popup_support_getter = getattr(pace, "supports_vent_popup_ack", None)
        if callable(popup_support_getter):
            try:
                if not bool(popup_support_getter()):
                    self._pace_vent_popup_ack_supported = False
            except Exception:
                pass
        popup_getter = getattr(pace, "get_vent_popup_ack_enabled", None)
        if self._pace_vent_popup_ack_supported is not False and callable(popup_getter):
            try:
                self._pace_vent_popup_ack_enabled = bool(popup_getter())
                self._pace_vent_popup_ack_supported = True
            except Exception:
                self._pace_vent_popup_ack_supported = False

    def _set_pressure_controller_vent_after_valve_open(
        self,
        open_after_vent: bool,
        *,
        strict: bool,
        reason: str = "",
    ) -> Optional[bool]:
        pace = self.devices.get("pace")
        if not pace:
            return None
        setter = getattr(pace, "set_vent_after_valve_open", None)
        getter = getattr(pace, "get_vent_after_valve_open", None)
        support_getter = getattr(pace, "supports_vent_after_valve_open", None)
        if callable(support_getter):
            try:
                if not bool(support_getter()):
                    self._pace_vent_after_valve_supported = False
                    if strict:
                        raise RuntimeError("VENT_AFTER_VALVE_UNSUPPORTED")
                    return None
            except RuntimeError:
                raise
            except Exception:
                pass
        if not callable(setter):
            self._pace_vent_after_valve_supported = False
            if strict:
                raise RuntimeError("VENT_AFTER_VALVE_UNSUPPORTED")
            return None
        try:
            setter(bool(open_after_vent))
            self._pace_vent_after_valve_supported = True
            self._pace_vent_after_valve_open = bool(open_after_vent)
            if callable(getter):
                actual = bool(getter())
                self._pace_vent_after_valve_open = actual
                if actual != bool(open_after_vent):
                    raise RuntimeError(
                        f"VENT_AFTER_VALVE_STATE_{'OPEN' if actual else 'CLOSED'}"
                    )
            return self._pace_vent_after_valve_open
        except Exception:
            if not callable(getter):
                self._pace_vent_after_valve_supported = False
            if strict:
                extra = f" ({reason})" if reason else ""
                raise RuntimeError(f"VENT_AFTER_VALVE_SET_FAILED{extra}")
            return self._pace_vent_after_valve_open

    def _set_pressure_controller_popup_ack(
        self,
        enabled: Optional[bool],
        *,
        strict: bool = False,
        reason: str = "",
    ) -> Optional[bool]:
        if enabled is None:
            self._refresh_pressure_controller_aux_state()
            return self._pace_vent_popup_ack_enabled
        pace = self.devices.get("pace")
        if not pace:
            return None
        setter = getattr(pace, "set_vent_popup_ack_enabled", None)
        getter = getattr(pace, "get_vent_popup_ack_enabled", None)
        support_getter = getattr(pace, "supports_vent_popup_ack", None)
        if callable(support_getter):
            try:
                if not bool(support_getter()):
                    self._pace_vent_popup_ack_supported = False
                    if strict:
                        raise RuntimeError("VENT_POPUP_ACK_UNSUPPORTED")
                    return self._pace_vent_popup_ack_enabled
            except RuntimeError:
                raise
            except Exception:
                pass
        if not callable(setter):
            self._pace_vent_popup_ack_supported = False
            if strict:
                raise RuntimeError("VENT_POPUP_ACK_UNSUPPORTED")
            return self._pace_vent_popup_ack_enabled
        try:
            setter(bool(enabled))
            self._pace_vent_popup_ack_supported = True
            self._pace_vent_popup_ack_enabled = bool(enabled)
            if callable(getter):
                self._pace_vent_popup_ack_enabled = bool(getter())
            return self._pace_vent_popup_ack_enabled
        except Exception:
            if not callable(getter):
                self._pace_vent_popup_ack_supported = False
            if strict:
                extra = f" ({reason})" if reason else ""
                raise RuntimeError(f"VENT_POPUP_ACK_SET_FAILED{extra}")
            return self._pace_vent_popup_ack_enabled

    def _handoff_fast_enabled(self) -> bool:
        return bool(self._wf("workflow.pressure.handoff_fast_enabled", False))

    def _handoff_safe_open_delta_hpa(self) -> float:
        return max(0.1, float(self._wf("workflow.pressure.handoff_safe_open_delta_hpa", 3.0) or 3.0))

    def _handoff_use_pressure_gauge(self) -> bool:
        return bool(self._wf("workflow.pressure.handoff_use_pressure_gauge", True))

    def _handoff_require_vent_completed(self) -> bool:
        return bool(self._wf("workflow.pressure.handoff_require_vent_completed", False))

    def _defer_heavy_exports_during_handoff_enabled(self) -> bool:
        return bool(self._wf("workflow.reporting.defer_heavy_exports_during_handoff", True))

    def _flush_deferred_exports_on_next_route_soak_enabled(self) -> bool:
        return bool(self._wf("workflow.reporting.flush_deferred_exports_on_next_route_soak", True))

    def _relay_bulk_write_enabled(self) -> bool:
        return bool(self._wf("workflow.relay.bulk_write_enabled", True))

    def _update_atmosphere_reference_hpa(self, *, reason: str = "") -> Optional[float]:
        pressure_now, source = self._read_preseal_pressure_gauge()
        if pressure_now is None or source != "pressure_gauge":
            return self._atmosphere_reference_hpa
        self._atmosphere_reference_hpa = float(pressure_now)
        extra = f" ({reason})" if reason else ""
        self.log(
            f"Atmosphere reference updated{extra}: "
            f"pressure_gauge_hpa={self._atmosphere_reference_hpa:.3f}"
        )
        return self._atmosphere_reference_hpa

    def _sampling_fixed_rate_enabled(self) -> bool:
        return bool(self._wf("workflow.sampling.fixed_rate_enabled", True))

    def _sampling_fast_sync_warn_span_ms(self) -> float:
        return max(0.0, float(self._wf("workflow.sampling.fast_sync_warn_span_ms", 1000.0) or 1000.0))

    def _sampling_slow_aux_cache_interval_s(self) -> float:
        return max(0.5, float(self._wf("workflow.sampling.slow_aux_cache_interval_s", 5.0) or 5.0))

    def _sampling_slow_aux_cache_enabled(self) -> bool:
        return bool(self._wf("workflow.sampling.slow_aux_cache_enabled", True))

    def _sampling_fast_signal_worker_enabled(self) -> bool:
        return bool(self._wf("workflow.sampling.fast_signal_worker_enabled", True))

    def _sampling_fast_signal_worker_interval_s(self) -> float:
        return max(0.05, float(self._wf("workflow.sampling.fast_signal_worker_interval_s", 0.1) or 0.1))

    def _sampling_fast_signal_ring_buffer_size(self) -> int:
        try:
            return max(8, int(self._wf("workflow.sampling.fast_signal_ring_buffer_size", 128) or 128))
        except Exception:
            return 128

    def _pressure_fast_gauge_response_timeout_s(self) -> float:
        return max(0.05, float(self._wf("workflow.pressure.fast_gauge_response_timeout_s", 1.0) or 1.0))

    def _pressure_transition_gauge_response_timeout_s(self) -> float:
        fast_timeout_s = self._pressure_fast_gauge_response_timeout_s()
        configured_timeout_s = float(
            self._wf("workflow.pressure.transition_gauge_response_timeout_s", 1.5) or 1.5
        )
        return max(fast_timeout_s, configured_timeout_s)

    def _pressure_fast_gauge_read_retries(self) -> int:
        try:
            return max(1, int(self._wf("workflow.pressure.fast_gauge_read_retries", 1) or 1))
        except Exception:
            return 1

    def _sampling_dewpoint_fast_timeout_s(self) -> float:
        return max(0.05, float(self._wf("workflow.sampling.dewpoint_fast_timeout_s", 0.35) or 0.35))

    def _sampling_pressure_gauge_continuous_enabled(self) -> bool:
        return bool(self._wf("workflow.sampling.pressure_gauge_continuous_enabled", False))

    def _sampling_pressure_gauge_continuous_mode(self) -> str:
        mode = str(self._wf("workflow.sampling.pressure_gauge_continuous_mode", "P4") or "P4").strip().upper()
        if mode not in {"P4", "P7"}:
            return "P4"
        return mode

    def _sampling_pressure_gauge_continuous_drain_s(self) -> float:
        return max(0.02, float(self._wf("workflow.sampling.pressure_gauge_continuous_drain_s", 0.12) or 0.12))

    def _sampling_pressure_gauge_continuous_read_timeout_s(self) -> float:
        return max(
            0.01,
            float(self._wf("workflow.sampling.pressure_gauge_continuous_read_timeout_s", 0.02) or 0.02),
        )

    def _pressure_transition_gauge_continuous_enabled(self) -> bool:
        return bool(self._wf("workflow.pressure.transition_pressure_gauge_continuous_enabled", True))

    def _pressure_transition_gauge_continuous_mode(self) -> str:
        fallback_mode = self._sampling_pressure_gauge_continuous_mode()
        mode = str(
            self._wf("workflow.pressure.transition_pressure_gauge_continuous_mode", fallback_mode) or fallback_mode
        ).strip().upper()
        if mode not in {"P4", "P7"}:
            return "P4"
        return mode

    def _pressure_transition_gauge_continuous_drain_s(self) -> float:
        fallback_drain_s = self._sampling_pressure_gauge_continuous_drain_s()
        return max(
            0.02,
            float(
                self._wf(
                    "workflow.pressure.transition_pressure_gauge_continuous_drain_s",
                    fallback_drain_s,
                )
                or fallback_drain_s
            ),
        )

    def _pressure_transition_gauge_continuous_read_timeout_s(self) -> float:
        fallback_timeout_s = self._sampling_pressure_gauge_continuous_read_timeout_s()
        return max(
            0.01,
            float(
                self._wf(
                    "workflow.pressure.transition_pressure_gauge_continuous_read_timeout_s",
                    fallback_timeout_s,
                )
                or fallback_timeout_s
            ),
        )

    def _pressure_control_wait_aux_interval_s(self) -> float:
        return max(0.2, float(self._wf("workflow.pressure.control_wait_aux_interval_s", 1.0) or 1.0))

    def _slow_fast_signal_match_defaults(self, signal_key: str) -> tuple[float, float, float]:
        key = str(signal_key or "").strip().lower()
        if key == "pace":
            interval_ms = self._sampling_fast_signal_worker_interval_s() * 1000.0
            return (
                max(350.0, interval_ms * 3.0),
                max(120.0, interval_ms * 1.5),
                max(900.0, interval_ms * 8.0),
            )
        if key == "pressure_gauge":
            timeout_ms = self._pressure_fast_gauge_response_timeout_s() * 1000.0
            return (
                max(800.0, timeout_ms + 500.0),
                400.0,
                max(2200.0, timeout_ms + 1400.0),
            )
        if key == "dewpoint":
            timeout_ms = self._sampling_dewpoint_fast_timeout_s() * 1000.0
            return (
                max(1500.0, timeout_ms + 900.0),
                500.0,
                max(3000.0, timeout_ms + 2200.0),
            )
        return (
            self._sampling_active_frame_max_anchor_delta_ms(),
            self._sampling_active_frame_right_match_max_ms(),
            self._sampling_active_frame_stale_ms(),
        )

    def _fast_signal_left_match_max_ms(self, signal_key: str = "") -> float:
        default_left_ms, _, _ = self._slow_fast_signal_match_defaults(signal_key)
        path = f"workflow.sampling.fast_signal_match.{str(signal_key or '').strip().lower()}.left_match_max_ms"
        return max(0.0, float(self._wf(path, default_left_ms) or default_left_ms))

    def _fast_signal_right_match_max_ms(self, signal_key: str = "") -> float:
        _, default_right_ms, _ = self._slow_fast_signal_match_defaults(signal_key)
        path = f"workflow.sampling.fast_signal_match.{str(signal_key or '').strip().lower()}.right_match_max_ms"
        return max(0.0, float(self._wf(path, default_right_ms) or default_right_ms))

    def _fast_signal_stale_ms(self, signal_key: str = "") -> float:
        _, _, default_stale_ms = self._slow_fast_signal_match_defaults(signal_key)
        path = f"workflow.sampling.fast_signal_match.{str(signal_key or '').strip().lower()}.stale_ms"
        return max(0.0, float(self._wf(path, default_stale_ms) or default_stale_ms))

    def _read_pressure_gauge_value(self, *, fast: bool = False, purpose: str = "sampling") -> float:
        gauge = self.devices.get("pressure_gauge")
        if gauge is None:
            raise RuntimeError("pressure_gauge unavailable")
        reader = getattr(gauge, "read_pressure", None)
        if not callable(reader):
            raise RuntimeError("pressure_gauge unsupported")
        if not fast:
            return float(reader())
        timeout_s = self._pressure_fast_gauge_response_timeout_s()
        if str(purpose or "").strip().lower() == "transition":
            timeout_s = self._pressure_transition_gauge_response_timeout_s()
        fast_reader = getattr(gauge, "read_pressure_fast", None)
        if callable(fast_reader):
            return float(
                fast_reader(
                    response_timeout_s=timeout_s,
                    retries=self._pressure_fast_gauge_read_retries(),
                    retry_sleep_s=0.0,
                    clear_buffer=False,
                )
            )
        try:
            return float(
                reader(
                    response_timeout_s=timeout_s,
                    retries=self._pressure_fast_gauge_read_retries(),
                    retry_sleep_s=0.0,
                    clear_buffer=False,
                )
            )
        except TypeError:
            return float(reader())

    def _read_pace_pressure_value(self, *, fast: bool = False) -> float:
        pace = self.devices.get("pace")
        if pace is None:
            raise RuntimeError("pace unavailable")
        reader = getattr(pace, "read_pressure", None)
        if not callable(reader):
            raise RuntimeError("pace unsupported")
        if not fast:
            return float(reader())

        query = getattr(pace, "query", None)
        parse_first_float = getattr(pace, "_parse_first_float", None)
        pressure_queries = list(getattr(pace, "pressure_queries", []) or [])
        query_line_endings = list(getattr(pace, "query_line_endings", []) or [])
        if callable(query) and callable(parse_first_float) and pressure_queries:
            cmd = str(pressure_queries[0] or "").strip()
            if cmd:
                term = query_line_endings[0] if query_line_endings else None
                try:
                    resp = query(cmd, line_ending=term) if term is not None else query(cmd)
                except TypeError:
                    resp = query(cmd)
                value = parse_first_float(resp)
                if value is not None:
                    return float(value)
                raise RuntimeError("NO_RESPONSE")

        return float(reader())

    def _sampling_pace_state_every_n_samples(self) -> int:
        try:
            return max(0, int(self._wf("workflow.sampling.pace_state_every_n_samples", 0) or 0))
        except Exception:
            return 0

    def _sampling_pace_state_cache_enabled(self) -> bool:
        return bool(self._wf("workflow.sampling.pace_state_cache_enabled", True))

    def _sampling_pace_state_strategy_text(self) -> str:
        every_n = self._sampling_pace_state_every_n_samples()
        cache_enabled = self._sampling_pace_state_cache_enabled()
        if every_n <= 0:
            return "stage_points+cached_rows" if cache_enabled else "stage_points_only"
        if every_n == 1:
            return "every_sample"
        if cache_enabled:
            return f"first_row_then_every_{every_n}_samples+cache"
        return f"first_row_then_every_{every_n}_samples"

    def _sampling_has_reusable_pace_state(self) -> bool:
        snapshot = self._pace_state_cache_snapshot()
        if snapshot.get("sample_ts"):
            return True
        completion = dict(self._last_sample_completion or {})
        for key in ("pace_output_state", "pace_isolation_state", "pace_vent_status"):
            if self._as_int(completion.get(key)) is not None:
                return True
        return False

    def _sampling_worker_interval_s(self) -> float:
        return max(
            0.05,
            float(self._wf("workflow.analyzer_live_snapshot.sampling_worker_interval_s", 0.2) or 0.2),
        )

    def _sampling_passive_round_robin_interval_s(self) -> float:
        return max(
            0.05,
            float(self._wf("workflow.analyzer_live_snapshot.passive_round_robin_interval_s", 0.25) or 0.25),
        )

    def _sampling_active_ring_buffer_size(self) -> int:
        try:
            return max(
                8,
                int(self._wf("workflow.analyzer_live_snapshot.active_ring_buffer_size", 128) or 128),
            )
        except Exception:
            return 128

    def _sampling_active_frame_max_anchor_delta_ms(self) -> float:
        return max(
            0.0,
            float(self._wf("workflow.analyzer_live_snapshot.active_frame_max_anchor_delta_ms", 250.0) or 250.0),
        )

    def _sampling_active_frame_right_match_max_ms(self) -> float:
        return max(
            0.0,
            float(self._wf("workflow.analyzer_live_snapshot.active_frame_right_match_max_ms", 120.0) or 120.0),
        )

    def _sampling_active_frame_stale_ms(self) -> float:
        return max(
            0.0,
            float(self._wf("workflow.analyzer_live_snapshot.active_frame_stale_ms", 500.0) or 500.0),
        )

    def _sampling_active_drain_poll_s(self) -> float:
        return max(0.01, float(self._wf("workflow.analyzer_live_snapshot.active_drain_poll_s", 0.05) or 0.05))

    def _sampling_pre_sample_freshness_timeout_s(self) -> float:
        raw = self._wf("workflow.sampling.pre_sample_freshness_timeout_s", 1.0)
        return max(0.0, float(1.0 if raw is None else raw))

    def _sampling_pre_sample_freshness_poll_s(self) -> float:
        raw = self._wf("workflow.sampling.pre_sample_freshness_poll_s", 0.05)
        return max(0.02, float(0.05 if raw is None else raw))

    def _sampling_pre_sample_signal_max_age_s(self) -> float:
        raw = self._wf("workflow.sampling.pre_sample_signal_max_age_s", 0.35)
        return max(
            self._sampling_fast_signal_worker_interval_s() * 2.0,
            float(0.35 if raw is None else raw),
        )

    def _sampling_pre_sample_analyzer_max_age_s(self) -> float:
        raw = self._wf("workflow.sampling.pre_sample_analyzer_max_age_s", 0.6)
        return max(
            self._sampling_worker_interval_s() * 2.0,
            float(0.6 if raw is None else raw),
        )

    def _is_co2_low_pressure_sealed_point(self, point: CalibrationPoint) -> bool:
        if point.is_h2o_point or self._is_ambient_pressure_point(point):
            return False
        target_pressure_hpa = self._as_float(getattr(point, "target_pressure_hpa", None))
        return target_pressure_hpa is not None and target_pressure_hpa < 900.0

    def _warn_pressure_gauge_sampling_freshness_if_needed(self) -> None:
        if self._pressure_gauge_sampling_freshness_warning_logged:
            return
        if not bool(self._wf("workflow.pressure.use_pressure_gauge_for_sampling_gate", True)):
            return
        if self._sampling_pressure_gauge_continuous_enabled():
            return
        self._pressure_gauge_sampling_freshness_warning_logged = True
        self.log(
            "Pressure gauge freshness risk: workflow.pressure.use_pressure_gauge_for_sampling_gate=true "
            "but workflow.sampling.pressure_gauge_continuous_enabled=false; sample rows may report "
            "pressure_gauge_error=fast_signal_stale. Prefer enabling continuous gauge mode in focused "
            "engineering configs instead of relaxing stale thresholds."
        )

    def _record_co2_preseal_snapshot_runtime_fields(self, point: CalibrationPoint, *, phase: str) -> None:
        snapshot = dict(self._preseal_dewpoint_snapshot or {})
        self._set_point_runtime_fields(
            point,
            phase=phase,
            preseal_dewpoint_c=self._as_float(snapshot.get("dewpoint_c")),
            preseal_temp_c=self._as_float(snapshot.get("temp_c")),
            preseal_rh_pct=self._as_float(snapshot.get("rh_pct")),
            preseal_pressure_hpa=self._as_float(snapshot.get("pressure_hpa")),
        )

    @staticmethod
    def _normalized_policy(raw: Any, *, allowed: set[str], default: str) -> str:
        value = str(default if raw in (None, "") else raw).strip().lower()
        return value if value in allowed else default

    def _postseal_dewpoint_gate_cfg(self, point: CalibrationPoint) -> Dict[str, Any]:
        phase = "h2o" if point.is_h2o_point else "co2"
        is_co2_low_pressure = self._is_co2_low_pressure_sealed_point(point)
        window_raw = self._wf(f"workflow.pressure.{phase}_postseal_dewpoint_window_s", 2.0)
        timeout_raw = self._wf(f"workflow.pressure.{phase}_postseal_dewpoint_timeout_s", 5.5)
        span_raw = self._wf(f"workflow.pressure.{phase}_postseal_dewpoint_span_c", 0.12)
        slope_raw = self._wf(f"workflow.pressure.{phase}_postseal_dewpoint_slope_c_per_s", 0.04)
        min_samples_raw = self._wf(f"workflow.pressure.{phase}_postseal_dewpoint_min_samples", 4)
        physical_policy = self._normalized_policy(
            self._wf("workflow.pressure.co2_postseal_physical_qc_policy", "off"),
            allowed={"off", "warn", "reject"},
            default="off",
        )
        timeout_policy = self._normalized_policy(
            self._wf("workflow.pressure.co2_postseal_timeout_policy", "pass"),
            allowed={"pass", "warn", "reject"},
            default="pass",
        )
        postsample_late_rebound_policy = self._normalized_policy(
            self._wf("workflow.pressure.co2_postsample_late_rebound_policy", "off"),
            allowed={"off", "warn", "reject"},
            default="off",
        )
        presample_long_guard_policy = self._normalized_policy(
            self._wf("workflow.pressure.co2_presample_long_guard_policy", "off"),
            allowed={"off", "warn", "reject"},
            default="off",
        )
        sampling_window_qc_policy = self._normalized_policy(
            self._wf("workflow.pressure.co2_sampling_window_qc_policy", "off"),
            allowed={"off", "warn", "reject"},
            default="off",
        )
        return {
            "phase": phase,
            "window_s": max(
                0.1,
                float(2.0 if window_raw is None else window_raw),
            ),
            "timeout_s": max(
                0.0,
                float(5.5 if timeout_raw is None else timeout_raw),
            ),
            "span_c": max(
                0.0,
                float(0.12 if span_raw is None else span_raw),
            ),
            "slope_c_per_s": max(
                0.0,
                float(0.04 if slope_raw is None else slope_raw),
            ),
            "min_samples": max(
                2,
                int(4 if min_samples_raw is None else min_samples_raw),
            ),
            "co2_low_pressure": is_co2_low_pressure,
            "rebound_guard_enabled": bool(
                is_co2_low_pressure and self._wf("workflow.pressure.co2_postseal_rebound_guard_enabled", False)
            ),
            "rebound_window_s": max(
                1.0,
                float(self._wf("workflow.pressure.co2_postseal_rebound_window_s", 8.0) or 8.0),
            ),
            "rebound_min_rise_c": max(
                0.0,
                float(self._wf("workflow.pressure.co2_postseal_rebound_min_rise_c", 0.12) or 0.12),
            ),
            "physical_qc_enabled": bool(
                is_co2_low_pressure and self._wf("workflow.pressure.co2_postseal_physical_qc_enabled", False)
            ),
            "physical_qc_max_abs_delta_c": max(
                0.0,
                float(self._wf("workflow.pressure.co2_postseal_physical_qc_max_abs_delta_c", 1.0) or 1.0),
            ),
            "physical_qc_policy": physical_policy,
            "timeout_policy": timeout_policy if is_co2_low_pressure else "pass",
            "postsample_late_rebound_guard_enabled": bool(
                is_co2_low_pressure and self._wf("workflow.pressure.co2_postsample_late_rebound_guard_enabled", False)
            ),
            "postsample_late_rebound_max_rise_c": max(
                0.0,
                float(self._wf("workflow.pressure.co2_postsample_late_rebound_max_rise_c", 0.12) or 0.12),
            ),
            "postsample_late_rebound_policy": postsample_late_rebound_policy if is_co2_low_pressure else "off",
            "presample_long_guard_enabled": bool(
                is_co2_low_pressure and self._wf("workflow.pressure.co2_presample_long_guard_enabled", False)
            ),
            "presample_long_guard_window_s": max(
                0.1,
                float(self._wf("workflow.pressure.co2_presample_long_guard_window_s", 8.0) or 8.0),
            ),
            "presample_long_guard_timeout_s": max(
                0.0,
                float(self._wf("workflow.pressure.co2_presample_long_guard_timeout_s", 20.0) or 20.0),
            ),
            "presample_long_guard_max_span_c": max(
                0.0,
                float(self._wf("workflow.pressure.co2_presample_long_guard_max_span_c", 0.15) or 0.15),
            ),
            "presample_long_guard_max_abs_slope_c_per_s": max(
                0.0,
                float(self._wf("workflow.pressure.co2_presample_long_guard_max_abs_slope_c_per_s", 0.02) or 0.02),
            ),
            "presample_long_guard_max_rise_c": max(
                0.0,
                float(self._wf("workflow.pressure.co2_presample_long_guard_max_rise_c", 0.12) or 0.12),
            ),
            "presample_long_guard_policy": presample_long_guard_policy if is_co2_low_pressure else "off",
            "sampling_window_qc_enabled": bool(
                is_co2_low_pressure and self._wf("workflow.pressure.co2_sampling_window_qc_enabled", False)
            ),
            "sampling_window_qc_max_range_c": max(
                0.0,
                float(self._wf("workflow.pressure.co2_sampling_window_qc_max_range_c", 0.20) or 0.20),
            ),
            "sampling_window_qc_max_rise_c": max(
                0.0,
                float(self._wf("workflow.pressure.co2_sampling_window_qc_max_rise_c", 0.12) or 0.12),
            ),
            "sampling_window_qc_max_abs_slope_c_per_s": max(
                0.0,
                float(self._wf("workflow.pressure.co2_sampling_window_qc_max_abs_slope_c_per_s", 0.02) or 0.02),
            ),
            "sampling_window_qc_policy": sampling_window_qc_policy if is_co2_low_pressure else "off",
        }

    def _sampling_active_anchor_match_enabled(self) -> bool:
        return bool(self._wf("workflow.analyzer_live_snapshot.anchor_match_enabled", True))

    def _evaluate_co2_postseal_physical_qc(
        self,
        point: CalibrationPoint,
        *,
        actual_dewpoint_c: Any,
        gate_cfg: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        cfg = dict(gate_cfg or self._postseal_dewpoint_gate_cfg(point))
        snapshot = dict(self._preseal_dewpoint_snapshot or {})
        result = {
            "preseal_dewpoint_c": self._as_float(snapshot.get("dewpoint_c")),
            "preseal_temp_c": self._as_float(snapshot.get("temp_c")),
            "preseal_rh_pct": self._as_float(snapshot.get("rh_pct")),
            "preseal_pressure_hpa": self._as_float(snapshot.get("pressure_hpa")),
            "postseal_expected_dewpoint_c": None,
            "postseal_actual_dewpoint_c": self._as_float(actual_dewpoint_c),
            "postseal_physical_delta_c": None,
            "postseal_physical_qc_status": "skipped",
            "postseal_physical_qc_reason": "",
        }
        if not bool(cfg.get("co2_low_pressure")):
            result["postseal_physical_qc_reason"] = "not_co2_low_pressure_point"
            return result
        if not bool(cfg.get("physical_qc_enabled")):
            result["postseal_physical_qc_reason"] = "physical_qc_disabled"
            return result

        preseal_dewpoint_c = result["preseal_dewpoint_c"]
        preseal_pressure_hpa = result["preseal_pressure_hpa"]
        target_pressure_hpa = self._as_float(getattr(point, "target_pressure_hpa", None))
        expected_dewpoint_c = predict_pressure_scaled_dewpoint_c(
            preseal_dewpoint_c,
            preseal_pressure_hpa,
            target_pressure_hpa,
        )
        result["postseal_expected_dewpoint_c"] = expected_dewpoint_c
        if preseal_dewpoint_c is None:
            result["postseal_physical_qc_reason"] = "preseal_dewpoint_missing"
            return result
        if preseal_pressure_hpa is None:
            result["postseal_physical_qc_reason"] = "preseal_pressure_missing"
            return result
        if target_pressure_hpa is None:
            result["postseal_physical_qc_reason"] = "target_pressure_missing"
            return result
        if expected_dewpoint_c is None:
            result["postseal_physical_qc_reason"] = "expected_dewpoint_unavailable"
            return result
        if result["postseal_actual_dewpoint_c"] is None:
            result["postseal_physical_qc_reason"] = "postseal_live_dewpoint_missing"
            return result

        delta_c = float(result["postseal_actual_dewpoint_c"]) - float(expected_dewpoint_c)
        result["postseal_physical_delta_c"] = round(delta_c, 6)
        max_abs_delta_c = float(cfg.get("physical_qc_max_abs_delta_c") or 0.0)
        if abs(delta_c) <= max_abs_delta_c:
            result["postseal_physical_qc_status"] = "pass"
            return result

        result["postseal_physical_qc_status"] = "fail"
        result["postseal_physical_qc_reason"] = (
            f"abs_delta_c={abs(delta_c):.3f}>max_abs_delta_c={max_abs_delta_c:.3f};"
            f"policy={cfg.get('physical_qc_policy') or 'off'}"
        )
        return result

    def _apply_co2_postseal_physical_qc_runtime_fields(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        qc_result: Optional[Dict[str, Any]],
    ) -> None:
        result = dict(qc_result or {})
        self._set_point_runtime_fields(
            point,
            phase=phase,
            preseal_dewpoint_c=result.get("preseal_dewpoint_c"),
            preseal_temp_c=result.get("preseal_temp_c"),
            preseal_rh_pct=result.get("preseal_rh_pct"),
            preseal_pressure_hpa=result.get("preseal_pressure_hpa"),
            postseal_expected_dewpoint_c=result.get("postseal_expected_dewpoint_c"),
            postseal_actual_dewpoint_c=result.get("postseal_actual_dewpoint_c"),
            postseal_physical_delta_c=result.get("postseal_physical_delta_c"),
            postseal_physical_qc_status=result.get("postseal_physical_qc_status"),
            postseal_physical_qc_reason=result.get("postseal_physical_qc_reason"),
        )

    def _set_postseal_timeout_runtime_fields(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        gate_cfg: Optional[Dict[str, Any]] = None,
        timed_out: bool,
        blocked: bool,
    ) -> None:
        cfg = dict(gate_cfg or self._postseal_dewpoint_gate_cfg(point))
        if not bool(cfg.get("co2_low_pressure")):
            return
        self._set_point_runtime_fields(
            point,
            phase=phase,
            postseal_timeout_policy=str(cfg.get("timeout_policy") or "pass"),
            postseal_timeout_blocked=bool(blocked),
            point_quality_timeout_flag=bool(timed_out),
        )

    def _evaluate_co2_postsample_late_rebound(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        first_effective_sample_dewpoint_c: Any,
        gate_cfg: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        cfg = dict(gate_cfg or self._postseal_dewpoint_gate_cfg(point))
        runtime_state = dict(self._point_runtime_state(point, phase=phase) or {})
        gate_pass_dewpoint_c = self._as_float(runtime_state.get("dewpoint_gate_pass_live_c"))
        first_effective_dewpoint_c = self._as_float(first_effective_sample_dewpoint_c)
        result = {
            "dewpoint_gate_pass_live_c": gate_pass_dewpoint_c,
            "first_effective_sample_dewpoint_c": first_effective_dewpoint_c,
            "postgate_to_first_effective_dewpoint_rise_c": None,
            "postsample_late_rebound_status": "skipped",
            "postsample_late_rebound_reason": "",
        }
        if str(phase or "").strip().lower() != "co2" or not bool(cfg.get("co2_low_pressure")):
            result["postsample_late_rebound_reason"] = "not_co2_low_pressure_point"
        elif not bool(cfg.get("postsample_late_rebound_guard_enabled")):
            result["postsample_late_rebound_reason"] = "guard_disabled"
        elif str(cfg.get("postsample_late_rebound_policy") or "off").lower() == "off":
            result["postsample_late_rebound_reason"] = "policy_off"
        elif gate_pass_dewpoint_c is None:
            result["postsample_late_rebound_reason"] = "gate_pass_dewpoint_missing"
        elif first_effective_dewpoint_c is None:
            result["postsample_late_rebound_reason"] = "first_effective_sample_dewpoint_missing"
        else:
            rise_c = float(first_effective_dewpoint_c) - float(gate_pass_dewpoint_c)
            max_rise_c = float(cfg.get("postsample_late_rebound_max_rise_c") or 0.0)
            result["postgate_to_first_effective_dewpoint_rise_c"] = round(rise_c, 6)
            if rise_c <= max_rise_c:
                result["postsample_late_rebound_status"] = "pass"
            else:
                policy = str(cfg.get("postsample_late_rebound_policy") or "off").lower()
                result["postsample_late_rebound_status"] = "fail" if policy == "reject" else "warn"
                result["postsample_late_rebound_reason"] = (
                    f"rise_c={rise_c:.3f}>max_rise_c={max_rise_c:.3f};policy={policy}"
                )
                self.log(
                    "CO2 post-sample late rebound detected: "
                    f"point={point.index} gate_pass_dewpoint_c={gate_pass_dewpoint_c:.3f} "
                    f"first_effective_dewpoint_c={first_effective_dewpoint_c:.3f} "
                    f"rise_c={rise_c:.3f} policy={policy}"
                )
        self._set_point_runtime_fields(
            point,
            phase=phase,
            dewpoint_gate_pass_live_c=result.get("dewpoint_gate_pass_live_c"),
            first_effective_sample_dewpoint_c=result.get("first_effective_sample_dewpoint_c"),
            postgate_to_first_effective_dewpoint_rise_c=result.get("postgate_to_first_effective_dewpoint_rise_c"),
            postsample_late_rebound_status=result.get("postsample_late_rebound_status"),
            postsample_late_rebound_reason=result.get("postsample_late_rebound_reason"),
        )
        return result

    def _wait_co2_presample_long_guard(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        context: Optional[Dict[str, Any]] = None,
        gate_cfg: Optional[Dict[str, Any]] = None,
    ) -> bool:
        cfg = dict(gate_cfg or self._postseal_dewpoint_gate_cfg(point))
        runtime_state = dict(self._point_runtime_state(point, phase=phase) or {})
        policy = str(cfg.get("presample_long_guard_policy") or "off").lower()
        result = {
            "presample_long_guard_status": "skipped",
            "presample_long_guard_reason": "",
            "presample_long_guard_elapsed_s": 0.0,
            "presample_long_guard_span_c": None,
            "presample_long_guard_slope_c_per_s": None,
            "presample_long_guard_rise_c": None,
        }

        def _finalize(status: str, reason: str, *, log_message: str = "") -> bool:
            result["presample_long_guard_status"] = status
            result["presample_long_guard_reason"] = reason
            self._set_point_runtime_fields(point, phase=phase, **result)
            if log_message:
                self.log(log_message)
            self._update_point_quality_summary(point, phase=phase)
            return status != "fail"

        if str(phase or "").strip().lower() != "co2" or not bool(cfg.get("co2_low_pressure")):
            result["presample_long_guard_reason"] = "not_co2_low_pressure_point"
            self._set_point_runtime_fields(point, phase=phase, **result)
            return True
        if not bool(cfg.get("presample_long_guard_enabled")):
            result["presample_long_guard_reason"] = "guard_disabled"
            self._set_point_runtime_fields(point, phase=phase, **result)
            return True
        if policy == "off":
            result["presample_long_guard_reason"] = "policy_off"
            self._set_point_runtime_fields(point, phase=phase, **result)
            return True

        gate_pass_dewpoint_c = self._as_float(runtime_state.get("dewpoint_gate_pass_live_c"))
        reference_dewpoint_c = gate_pass_dewpoint_c
        reference_source = "gate_pass" if gate_pass_dewpoint_c is not None else "first_live_pending"

        active_context = context if isinstance(context, dict) else self._pressure_transition_fast_signal_context_active()
        if not isinstance(active_context, dict):
            return _finalize(
                "fail" if policy == "reject" else "warn",
                f"fast_signal_context_missing;policy={policy}",
                log_message=(
                    f"CO2 pre-sample long guard unavailable: point={point.index} "
                    f"fast_signal_context_missing policy={policy}"
                ),
            )

        window_s = float(cfg.get("presample_long_guard_window_s") or 8.0)
        timeout_s = float(cfg.get("presample_long_guard_timeout_s") or 20.0)
        max_span_c = float(cfg.get("presample_long_guard_max_span_c") or 0.0)
        max_abs_slope_c_per_s = float(cfg.get("presample_long_guard_max_abs_slope_c_per_s") or 0.0)
        max_rise_c = float(cfg.get("presample_long_guard_max_rise_c") or 0.0)
        poll_s = min(0.1, self._pressure_transition_monitor_wait_s(point))
        start_mono = time.monotonic()
        deadline = start_mono + timeout_s
        self._append_pressure_trace_row(
            point=point,
            route=phase,
            point_phase=phase,
            trace_stage="presample_long_guard_begin",
            pressure_target_hpa=point.target_pressure_hpa,
            refresh_pace_state=False,
            note=(
                f"window_s={window_s:.3f} timeout_s={timeout_s:.3f} "
                f"max_span_c={max_span_c:.3f} max_abs_slope_c_per_s={max_abs_slope_c_per_s:.4f} "
                f"max_rise_c={max_rise_c:.3f} "
                + (
                    f"reference_dewpoint_c={reference_dewpoint_c:.3f}"
                    if reference_dewpoint_c is not None
                    else "reference_dewpoint_c=awaiting_first_live"
                )
            ),
        )

        while True:
            obs = self._recent_fast_signal_numeric_observation(
                "dewpoint",
                "dewpoint_live_c",
                context=active_context,
                window_s=window_s,
                min_recv_mono_s=start_mono,
            )
            ready_values = self._cached_ready_check_trace_values(context=active_context, point=point)
            live_dewpoint_c = self._as_float(ready_values.get("dewpoint_live_c"))
            window_min_c = self._as_float(obs.get("min_value"))
            elapsed_s = max(0.0, time.monotonic() - start_mono)
            span_c = self._as_float(obs.get("span"))
            slope_c_per_s = self._as_float(obs.get("slope_per_s"))
            if reference_dewpoint_c is None and live_dewpoint_c is not None:
                reference_dewpoint_c = float(live_dewpoint_c)
                reference_source = "first_live"
            rise_c = (
                round(float(live_dewpoint_c) - float(reference_dewpoint_c), 6)
                if live_dewpoint_c is not None and reference_dewpoint_c is not None
                else None
            )
            rebound_c = (
                float(live_dewpoint_c) - float(window_min_c)
                if live_dewpoint_c is not None and window_min_c is not None
                else None
            )
            result.update(
                {
                    "presample_long_guard_elapsed_s": round(elapsed_s, 6),
                    "presample_long_guard_span_c": span_c,
                    "presample_long_guard_slope_c_per_s": slope_c_per_s,
                    "presample_long_guard_rise_c": rise_c,
                }
            )
            enough_window = elapsed_s >= window_s
            passed = bool(
                enough_window
                and live_dewpoint_c is not None
                and int(obs.get("count") or 0) >= 2
                and span_c is not None
                and slope_c_per_s is not None
                and rebound_c is not None
                and span_c <= max_span_c
                and abs(slope_c_per_s) <= max_abs_slope_c_per_s
                and rebound_c <= max_rise_c
            )
            if passed:
                self._append_pressure_trace_row(
                    point=point,
                    route=phase,
                    point_phase=phase,
                    trace_stage="presample_long_guard_end",
                    pressure_target_hpa=point.target_pressure_hpa,
                    pressure_gauge_hpa=ready_values.get("pressure_gauge_hpa"),
                    dewpoint_live_c=live_dewpoint_c,
                    dew_temp_live_c=ready_values.get("dew_temp_live_c"),
                    dew_rh_live_pct=ready_values.get("dew_rh_live_pct"),
                    refresh_pace_state=False,
                    note=(
                        f"result=pass elapsed_s={elapsed_s:.3f} span_c={span_c:.3f} "
                        f"slope_c_per_s={slope_c_per_s:.4f} "
                        + (
                            f"rise_c={float(rise_c):.3f} "
                            if rise_c is not None
                            else ""
                        )
                        + (
                            f"window_rebound_c={float(rebound_c):.3f} reference={reference_source}"
                            if rebound_c is not None
                            else f"reference={reference_source}"
                        )
                    ),
                )
                return _finalize(
                    "pass",
                    "",
                    log_message=(
                        f"CO2 pre-sample long guard pass: point={point.index} elapsed_s={elapsed_s:.3f} "
                        f"span_c={span_c:.3f} slope_c_per_s={slope_c_per_s:.4f} "
                        + (
                            f"rise_c={float(rise_c):.3f} "
                            if rise_c is not None
                            else ""
                        )
                        + (
                            f"window_rebound_c={float(rebound_c):.3f} reference={reference_source}"
                            if rebound_c is not None
                            else f"reference={reference_source}"
                        )
                    ),
                )

            if time.monotonic() >= deadline:
                reasons: List[str] = [f"timeout_elapsed_s={elapsed_s:.3f}"]
                count = int(obs.get("count") or 0)
                if elapsed_s < window_s:
                    reasons.append(f"elapsed_s={elapsed_s:.3f}<window_s={window_s:.3f}")
                if count < 2:
                    reasons.append(f"count={count}<min_samples=2")
                if span_c is None:
                    reasons.append("span_c=NA")
                elif span_c > max_span_c:
                    reasons.append(f"span_c={span_c:.3f}>max_span_c={max_span_c:.3f}")
                if slope_c_per_s is None:
                    reasons.append("abs_slope_c_per_s=NA")
                elif abs(slope_c_per_s) > max_abs_slope_c_per_s:
                    reasons.append(
                        f"abs_slope_c_per_s={abs(slope_c_per_s):.4f}>"
                        f"max_abs_slope_c_per_s={max_abs_slope_c_per_s:.4f}"
                    )
                if rebound_c is None:
                    reasons.append("window_rebound_c=NA")
                elif rebound_c > max_rise_c:
                    reasons.append(f"window_rebound_c={rebound_c:.3f}>max_rise_c={max_rise_c:.3f}")
                if reference_dewpoint_c is None:
                    reasons.append("reference_dewpoint_c=NA")
                else:
                    reasons.append(f"reference={reference_source}")
                if rise_c is not None:
                    reasons.append(f"rise_c={rise_c:.3f}")
                reasons.append(f"policy={policy}")
                reason = ";".join(reasons)
                status = "fail" if policy == "reject" else "warn"
                self._append_pressure_trace_row(
                    point=point,
                    route=phase,
                    point_phase=phase,
                    trace_stage="presample_long_guard_end",
                    pressure_target_hpa=point.target_pressure_hpa,
                    pressure_gauge_hpa=ready_values.get("pressure_gauge_hpa"),
                    dewpoint_live_c=live_dewpoint_c,
                    dew_temp_live_c=ready_values.get("dew_temp_live_c"),
                    dew_rh_live_pct=ready_values.get("dew_rh_live_pct"),
                    refresh_pace_state=False,
                    note=f"result={status} {reason}",
                )
                return _finalize(
                    status,
                    reason,
                    log_message=(
                        f"CO2 pre-sample long guard {status}: point={point.index} {reason}"
                    ),
                )

            self._ensure_pressure_transition_fast_signal_cache(
                active_context,
                reason="co2 presample long guard",
            )
            if not self._sampling_window_wait(poll_s, stop_event=active_context.get("stop_event")):
                result["presample_long_guard_status"] = "fail" if policy == "reject" else "warn"
                result["presample_long_guard_reason"] = f"guard_interrupted;policy={policy}"
                self._set_point_runtime_fields(point, phase=phase, **result)
                self._update_point_quality_summary(point, phase=phase)
                self.log(f"CO2 pre-sample long guard interrupted: point={point.index} policy={policy}")
                return False

    def _evaluate_co2_sampling_window_qc(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        samples: List[Dict[str, Any]],
        gate_cfg: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        cfg = dict(gate_cfg or self._postseal_dewpoint_gate_cfg(point))
        policy = str(cfg.get("sampling_window_qc_policy") or "off").lower()
        result = {
            "sampling_window_dewpoint_first_c": None,
            "sampling_window_dewpoint_last_c": None,
            "sampling_window_dewpoint_range_c": None,
            "sampling_window_dewpoint_rise_c": None,
            "sampling_window_dewpoint_slope_c_per_s": None,
            "sampling_window_qc_status": "skipped",
            "sampling_window_qc_reason": "",
        }
        if str(phase or "").strip().lower() != "co2" or not bool(cfg.get("co2_low_pressure")):
            result["sampling_window_qc_reason"] = "not_co2_low_pressure_point"
        elif not bool(cfg.get("sampling_window_qc_enabled")):
            result["sampling_window_qc_reason"] = "qc_disabled"
        elif policy == "off":
            result["sampling_window_qc_reason"] = "policy_off"
        else:
            dewpoint_series: List[Tuple[float, float]] = []
            for sample_idx, row in enumerate(samples or []):
                dewpoint_live_c = self._as_float(row.get("dewpoint_live_c"))
                if dewpoint_live_c is None:
                    continue
                sample_ts = self._sample_row_wall_ts(row, key="sample_start_ts")
                if sample_ts is None:
                    sample_ts = self._sample_row_wall_ts(row, key="sample_ts")
                if sample_ts is None:
                    sample_ts = float(sample_idx)
                dewpoint_series.append((sample_ts, float(dewpoint_live_c)))
            metrics = self._numeric_series_metrics(dewpoint_series)
            if not metrics:
                result["sampling_window_qc_reason"] = "insufficient_live_dewpoint_samples"
            else:
                first_c = self._as_float(metrics.get("first_value"))
                last_c = self._as_float(metrics.get("last_value"))
                range_c = self._as_float(metrics.get("span"))
                slope_c_per_s = self._as_float(metrics.get("slope_per_s"))
                rise_c = (
                    round(float(last_c) - float(first_c), 6)
                    if first_c is not None and last_c is not None
                    else None
                )
                result.update(
                    {
                        "sampling_window_dewpoint_first_c": first_c,
                        "sampling_window_dewpoint_last_c": last_c,
                        "sampling_window_dewpoint_range_c": (
                            round(float(range_c), 6) if range_c is not None else None
                        ),
                        "sampling_window_dewpoint_rise_c": rise_c,
                        "sampling_window_dewpoint_slope_c_per_s": (
                            round(float(slope_c_per_s), 6) if slope_c_per_s is not None else None
                        ),
                    }
                )
                range_c = result["sampling_window_dewpoint_range_c"]
                slope_c_per_s = result["sampling_window_dewpoint_slope_c_per_s"]
                max_range_c = float(cfg.get("sampling_window_qc_max_range_c") or 0.0)
                max_rise_c = float(cfg.get("sampling_window_qc_max_rise_c") or 0.0)
                max_abs_slope_c_per_s = float(cfg.get("sampling_window_qc_max_abs_slope_c_per_s") or 0.0)
                passed = bool(
                    range_c is not None
                    and rise_c is not None
                    and slope_c_per_s is not None
                    and range_c <= max_range_c
                    and rise_c <= max_rise_c
                    and abs(slope_c_per_s) <= max_abs_slope_c_per_s
                )
                if passed:
                    result["sampling_window_qc_status"] = "pass"
                else:
                    reasons: List[str] = []
                    if range_c is None:
                        reasons.append("range_c=NA")
                    elif range_c > max_range_c:
                        reasons.append(f"range_c={range_c:.3f}>max_range_c={max_range_c:.3f}")
                    if rise_c is None:
                        reasons.append("rise_c=NA")
                    elif rise_c > max_rise_c:
                        reasons.append(f"rise_c={rise_c:.3f}>max_rise_c={max_rise_c:.3f}")
                    if slope_c_per_s is None:
                        reasons.append("abs_slope_c_per_s=NA")
                    elif abs(slope_c_per_s) > max_abs_slope_c_per_s:
                        reasons.append(
                            f"abs_slope_c_per_s={abs(slope_c_per_s):.4f}>"
                            f"max_abs_slope_c_per_s={max_abs_slope_c_per_s:.4f}"
                        )
                    reasons.append(f"policy={policy}")
                    result["sampling_window_qc_status"] = "fail" if policy == "reject" else "warn"
                    result["sampling_window_qc_reason"] = ";".join(reasons)
                    self.log(
                        "CO2 sampling-window dewpoint QC "
                        f"{result['sampling_window_qc_status']}: point={point.index} "
                        f"{result['sampling_window_qc_reason']}"
                    )
        self._set_point_runtime_fields(point, phase=phase, **result)
        return result

    def _update_point_quality_summary(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
    ) -> Dict[str, Any]:
        state = dict(self._point_runtime_state(point, phase=phase) or {})
        status = "pass"
        flags: List[str] = []
        reasons: List[str] = []
        blocked = False

        def _add_issue(flag: str, severity: str, reason: str) -> None:
            nonlocal status, blocked
            if flag not in flags:
                flags.append(flag)
            if reason:
                reasons.append(reason)
            if severity == "fail":
                status = "fail"
                blocked = True
            elif severity == "warn" and status != "fail":
                status = "warn"

        if self._is_co2_low_pressure_sealed_point(point):
            gate_result = str(state.get("dewpoint_gate_result") or "").strip().lower()
            timeout_policy = self._normalized_policy(
                state.get("postseal_timeout_policy"),
                allowed={"pass", "warn", "reject"},
                default="pass",
            )
            physical_policy = self._normalized_policy(
                self._wf("workflow.pressure.co2_postseal_physical_qc_policy", "off"),
                allowed={"off", "warn", "reject"},
                default="off",
            )

            if gate_result == "rebound_veto":
                _add_issue("postseal_rebound_veto", "fail", "dewpoint_gate_result=rebound_veto")

            if bool(state.get("point_quality_timeout_flag")):
                if timeout_policy == "warn":
                    _add_issue("postseal_timeout", "warn", "postseal_timeout(policy=warn)")
                elif timeout_policy == "reject":
                    _add_issue("postseal_timeout", "fail", "postseal_timeout(policy=reject)")

            if str(state.get("postseal_physical_qc_status") or "").strip().lower() == "fail":
                physical_reason = str(state.get("postseal_physical_qc_reason") or "").strip()
                if physical_policy == "warn":
                    _add_issue("postseal_physical_qc", "warn", physical_reason or "postseal_physical_qc(policy=warn)")
                elif physical_policy == "reject":
                    _add_issue("postseal_physical_qc", "fail", physical_reason or "postseal_physical_qc(policy=reject)")

            late_rebound_status = str(state.get("postsample_late_rebound_status") or "").strip().lower()
            late_rebound_reason = str(state.get("postsample_late_rebound_reason") or "").strip()
            if late_rebound_status == "warn":
                _add_issue("postsample_late_rebound", "warn", late_rebound_reason or "postsample_late_rebound")
            elif late_rebound_status == "fail":
                _add_issue("postsample_late_rebound", "fail", late_rebound_reason or "postsample_late_rebound")

            presample_long_guard_status = str(state.get("presample_long_guard_status") or "").strip().lower()
            presample_long_guard_reason = str(state.get("presample_long_guard_reason") or "").strip()
            if presample_long_guard_status == "warn":
                _add_issue(
                    "presample_long_guard",
                    "warn",
                    presample_long_guard_reason or "presample_long_guard",
                )
            elif presample_long_guard_status == "fail":
                _add_issue(
                    "presample_long_guard",
                    "fail",
                    presample_long_guard_reason or "presample_long_guard",
                )

            sampling_window_qc_status = str(state.get("sampling_window_qc_status") or "").strip().lower()
            sampling_window_qc_reason = str(state.get("sampling_window_qc_reason") or "").strip()
            if sampling_window_qc_status == "warn":
                _add_issue(
                    "sampling_window_qc",
                    "warn",
                    sampling_window_qc_reason or "sampling_window_qc",
                )
            elif sampling_window_qc_status == "fail":
                _add_issue(
                    "sampling_window_qc",
                    "fail",
                    sampling_window_qc_reason or "sampling_window_qc",
                )

        stale_ratio = self._as_float(state.get("pressure_gauge_stale_ratio"))
        stale_warn_max = self._as_float(self._wf("workflow.sampling.pressure_gauge_stale_ratio_warn_max", None))
        stale_reject_max = self._as_float(self._wf("workflow.sampling.pressure_gauge_stale_ratio_reject_max", None))
        if stale_ratio is not None:
            if stale_reject_max is not None and stale_ratio > stale_reject_max:
                _add_issue(
                    "pressure_gauge_stale_ratio",
                    "fail",
                    f"pressure_gauge_stale_ratio={stale_ratio:.3f}>reject_max={stale_reject_max:.3f}",
                )
            elif stale_warn_max is not None and stale_ratio > stale_warn_max:
                _add_issue(
                    "pressure_gauge_stale_ratio",
                    "warn",
                    f"pressure_gauge_stale_ratio={stale_ratio:.3f}>warn_max={stale_warn_max:.3f}",
                )

        overshoot_hpa = self._as_float(state.get("preseal_trigger_overshoot_hpa"))
        overshoot_warn_hpa = self._as_float(self._wf("workflow.pressure.preseal_trigger_overshoot_warn_hpa", None))
        overshoot_reject_hpa = self._as_float(self._wf("workflow.pressure.preseal_trigger_overshoot_reject_hpa", None))
        if overshoot_hpa is not None:
            if overshoot_reject_hpa is not None and overshoot_hpa > overshoot_reject_hpa:
                _add_issue(
                    "preseal_trigger_overshoot",
                    "fail",
                    f"preseal_trigger_overshoot_hpa={overshoot_hpa:.3f}>reject_hpa={overshoot_reject_hpa:.3f}",
                )
            elif overshoot_warn_hpa is not None and overshoot_hpa > overshoot_warn_hpa:
                _add_issue(
                    "preseal_trigger_overshoot",
                    "warn",
                    f"preseal_trigger_overshoot_hpa={overshoot_hpa:.3f}>warn_hpa={overshoot_warn_hpa:.3f}",
                )

        summary = {
            "point_quality_status": status,
            "point_quality_reason": ";".join(reasons),
            "point_quality_flags": ",".join(flags),
            "point_quality_blocked": blocked,
        }
        self._set_point_runtime_fields(point, phase=phase, **summary)
        return summary

    def _record_pressure_gauge_freshness_runtime_fields(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        samples: List[Dict[str, Any]],
    ) -> None:
        total_count = len(samples) if self.devices.get("pressure_gauge") is not None else 0
        stale_count = sum(
            1
            for row in samples
            if str(row.get("pressure_gauge_error") or "").strip().lower() == "fast_signal_stale"
        )
        stale_ratio = round(stale_count / total_count, 6) if total_count > 0 else None
        self._set_point_runtime_fields(
            point,
            phase=phase,
            pressure_gauge_stale_count=stale_count,
            pressure_gauge_total_count=total_count,
            pressure_gauge_stale_ratio=stale_ratio,
        )

    def _copy_point_runtime_exports_into_samples(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        samples: List[Dict[str, Any]],
    ) -> None:
        state = dict(self._point_runtime_state(point, phase=phase) or {})
        export_fields = (
            "preseal_dewpoint_c",
            "preseal_temp_c",
            "preseal_rh_pct",
            "preseal_pressure_hpa",
            "preseal_trigger_overshoot_hpa",
            "postseal_expected_dewpoint_c",
            "postseal_actual_dewpoint_c",
            "postseal_physical_delta_c",
            "postseal_physical_qc_status",
            "postseal_physical_qc_reason",
            "postseal_timeout_policy",
            "postseal_timeout_blocked",
            "point_quality_timeout_flag",
            "dewpoint_gate_pass_live_c",
            "presample_long_guard_status",
            "presample_long_guard_reason",
            "presample_long_guard_elapsed_s",
            "presample_long_guard_span_c",
            "presample_long_guard_slope_c_per_s",
            "presample_long_guard_rise_c",
            "first_effective_sample_dewpoint_c",
            "postgate_to_first_effective_dewpoint_rise_c",
            "postsample_late_rebound_status",
            "postsample_late_rebound_reason",
            "sampling_window_dewpoint_first_c",
            "sampling_window_dewpoint_last_c",
            "sampling_window_dewpoint_range_c",
            "sampling_window_dewpoint_rise_c",
            "sampling_window_dewpoint_slope_c_per_s",
            "sampling_window_qc_status",
            "sampling_window_qc_reason",
            "pressure_gauge_stale_count",
            "pressure_gauge_total_count",
            "pressure_gauge_stale_ratio",
            "point_quality_status",
            "point_quality_reason",
            "point_quality_flags",
            "point_quality_blocked",
        )
        for row in samples:
            for key in export_fields:
                if key in state:
                    row[key] = state.get(key)

    @staticmethod
    def _cache_age_ms(timestamp_s: Any, now_s: Optional[float] = None) -> Optional[float]:
        try:
            ts_value = float(timestamp_s)
        except Exception:
            return None
        ref_s = time.time() if now_s is None else float(now_s)
        return max(0.0, round((ref_s - ts_value) * 1000.0, 3))

    def _next_live_analyzer_frame_seq(self) -> int:
        with self._buffer_seq_lock:
            self._live_analyzer_frame_seq += 1
            return self._live_analyzer_frame_seq

    def _next_fast_signal_frame_seq(self) -> int:
        with self._buffer_seq_lock:
            self._fast_signal_frame_seq += 1
            return self._fast_signal_frame_seq

    def _sampling_window_wait(self, duration_s: float, *, stop_event: Optional[threading.Event] = None) -> bool:
        if duration_s <= 0:
            return not self.stop_event.is_set() and not (stop_event.is_set() if stop_event else False)
        deadline = time.monotonic() + float(duration_s)
        return self._sampling_window_wait_until(deadline, stop_event=stop_event)

    def _sampling_window_wait_until(self, deadline_monotonic: float, *, stop_event: Optional[threading.Event] = None) -> bool:
        while True:
            if self.stop_event.is_set():
                return False
            if stop_event is not None and stop_event.is_set():
                return False
            if not self.pause_event.is_set():
                time.sleep(0.05)
                continue
            remain = float(deadline_monotonic) - time.monotonic()
            if remain <= 0:
                return True
            time.sleep(min(0.1, remain))

    @staticmethod
    def _advance_sampling_due_time(
        current_due_monotonic: float,
        interval_s: float,
        completed_monotonic: float,
    ) -> float:
        interval = max(0.05, float(interval_s))
        next_due = float(current_due_monotonic) + interval
        ref = float(completed_monotonic)
        if next_due <= ref:
            skipped = int((ref - next_due) // interval) + 1
            next_due += skipped * interval
        return next_due

    def _sampling_window_analyzer_entries(self) -> Dict[str, List[Tuple[str, Any, Dict[str, Any]]]]:
        active_entries: List[Tuple[str, Any, Dict[str, Any]]] = []
        passive_entries: List[Tuple[str, Any, Dict[str, Any]]] = []
        for label, ga, analyzer_cfg in self._all_gas_analyzers():
            if label in self._disabled_analyzers:
                continue
            settings = self._gas_analyzer_runtime_settings(analyzer_cfg)
            if bool(settings["active_send"]):
                active_entries.append((label, ga, analyzer_cfg))
            else:
                passive_entries.append((label, ga, analyzer_cfg))
        return {
            "active_entries": active_entries,
            "passive_entries": passive_entries,
        }

    def _sampling_window_worker_plan(self) -> Dict[str, Any]:
        analyzer_entries = self._sampling_window_analyzer_entries()
        live_cfg = self._live_snapshot_cfg()
        slow_aux_devices = [
            name
            for name in ("temp_chamber", "thermometer", "humidity_gen")
            if self.devices.get(name) is not None
        ]
        fast_signal_devices = [
            name
            for name in ("pace", "pressure_gauge", "dewpoint")
            if self.devices.get(name) is not None
        ]
        active_enabled = bool(live_cfg.get("sampling_worker_enabled", True)) and bool(analyzer_entries["active_entries"])
        passive_enabled = bool(live_cfg.get("passive_round_robin_enabled", True)) and bool(analyzer_entries["passive_entries"])
        slow_aux_enabled = self._sampling_slow_aux_cache_enabled() and bool(slow_aux_devices)
        fast_signal_enabled = self._sampling_fast_signal_worker_enabled() and bool(fast_signal_devices)
        return {
            "active_entries": list(analyzer_entries["active_entries"]),
            "passive_entries": list(analyzer_entries["passive_entries"]),
            "active_enabled": active_enabled,
            "passive_enabled": passive_enabled,
            "analyzer_worker_enabled": bool(active_enabled or passive_enabled),
            "fast_signal_enabled": fast_signal_enabled,
            "fast_signal_devices": fast_signal_devices,
            "slow_aux_enabled": slow_aux_enabled,
            "slow_aux_devices": slow_aux_devices,
        }

    def _sampling_row_pace_state_snapshot(self, pace: Any, *, sample_idx: int) -> Dict[str, Any]:
        every_n = self._sampling_pace_state_every_n_samples()
        refresh = every_n > 0 and (sample_idx == 0 or (sample_idx % every_n) == 0)
        snapshot = self._pace_state_snapshot(pace, refresh=refresh)
        if refresh:
            return snapshot
        if any(
            self._as_int(snapshot.get(key)) is not None
            for key in ("pace_output_state", "pace_isolation_state", "pace_vent_status")
        ):
            return snapshot
        fallback = self._last_sample_completion_pace_state()
        if any(
            self._as_int(fallback.get(key)) is not None
            for key in ("pace_output_state", "pace_isolation_state", "pace_vent_status")
        ):
            return fallback
        return snapshot

    def _run_sampling_window_worker(
        self,
        context: Dict[str, Any],
        *,
        worker_key: str,
        role: str,
        target,
        target_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        worker_states = context.setdefault("worker_states", {})
        worker_state = worker_states.setdefault(str(worker_key), {})
        worker_state.update(
            {
                "role": str(role),
                "started_at": self._ts_from_datetime(datetime.now()),
                "exited": False,
                "clean_exit": False,
                "error": "",
            }
        )
        try:
            kwargs = dict(target_kwargs or {})
            target(context, **kwargs)
            worker_state["clean_exit"] = True
        except Exception as exc:
            worker_state["error"] = str(exc)
            signature = f"{role}:fatal:{exc}"
            if signature not in context["worker_errors"]:
                context["worker_errors"].add(signature)
                self.log(f"Sampling window worker fatal [{role}] err={exc}")
        finally:
            worker_state["exited"] = True
            worker_state["ended_at"] = self._ts_from_datetime(datetime.now())

    def _new_sampling_window_context(
        self,
        *,
        point: CalibrationPoint,
        phase: str,
        point_tag: str,
    ) -> Dict[str, Any]:
        fast_ring_size = self._sampling_fast_signal_ring_buffer_size()
        return {
            "point": point,
            "phase": str(phase or "").strip().lower(),
            "point_tag": str(point_tag or ""),
            "stop_event": threading.Event(),
            "lock": threading.Lock(),
            "slow_aux_cache": {},
            "fast_signal_buffers": {
                "pace": deque(maxlen=fast_ring_size),
                "pressure_gauge": deque(maxlen=fast_ring_size),
                "dewpoint": deque(maxlen=fast_ring_size),
            },
            "fast_signal_errors": {},
            "active_analyzer_buffers": {},
            "active_analyzer_ring_buffer_size": self._sampling_active_ring_buffer_size(),
            "workers": [],
            "worker_errors": set(),
            "worker_states": {},
        }

    def _copy_recent_fast_signal_frames(
        self,
        source_context: Optional[Dict[str, Any]],
        dest_context: Optional[Dict[str, Any]],
        *,
        signal_keys: tuple[str, ...] = ("pace", "pressure_gauge", "dewpoint"),
        max_frames_per_signal: int = 3,
        max_age_s: float = 3.0,
    ) -> int:
        if not isinstance(source_context, dict) or not isinstance(dest_context, dict):
            return 0

        copied = 0
        cutoff_mono_s = time.monotonic() - max(0.1, float(max_age_s))
        dest_lock = dest_context.get("lock")
        source_lock = source_context.get("lock")
        for signal_key in signal_keys:
            if source_lock is not None:
                with source_lock:
                    source_frames = list(source_context.get("fast_signal_buffers", {}).get(signal_key, []))
                    source_error = dict(source_context.get("fast_signal_errors", {}).get(signal_key) or {})
            else:
                source_frames = list(source_context.get("fast_signal_buffers", {}).get(signal_key, []))
                source_error = dict(source_context.get("fast_signal_errors", {}).get(signal_key) or {})

            recent_frames = []
            for frame in source_frames:
                if not isinstance(frame, dict):
                    continue
                recv_mono_s = self._as_float(frame.get("recv_mono_s"))
                if recv_mono_s is None or recv_mono_s < cutoff_mono_s:
                    continue
                recent_frames.append(dict(frame))
            if max_frames_per_signal > 0:
                recent_frames = recent_frames[-max_frames_per_signal:]

            if dest_lock is not None:
                with dest_lock:
                    buffer = dest_context.setdefault("fast_signal_buffers", {}).setdefault(
                        signal_key,
                        deque(maxlen=self._sampling_fast_signal_ring_buffer_size()),
                    )
                    for frame in recent_frames:
                        buffer.append(frame)
                        copied += 1
                    if source_error.get("error"):
                        dest_context.setdefault("fast_signal_errors", {})[signal_key] = source_error
            else:
                buffer = dest_context.setdefault("fast_signal_buffers", {}).setdefault(
                    signal_key,
                    deque(maxlen=self._sampling_fast_signal_ring_buffer_size()),
                )
                for frame in recent_frames:
                    buffer.append(frame)
                    copied += 1
                if source_error.get("error"):
                    dest_context.setdefault("fast_signal_errors", {})[signal_key] = source_error
        return copied

    def _bootstrap_sampling_window_context_from_transition(
        self,
        context: Optional[Dict[str, Any]],
        transition_context: Optional[Dict[str, Any]],
    ) -> int:
        return self._copy_recent_fast_signal_frames(
            transition_context,
            context,
            max_frames_per_signal=4,
            max_age_s=max(
                3.0,
                self._sampling_pre_sample_signal_max_age_s() * 4.0,
            ),
        )

    def _update_slow_aux_cache_entry(
        self,
        context: Dict[str, Any],
        key: str,
        *,
        values: Optional[Dict[str, Any]] = None,
        error: str = "",
    ) -> None:
        entry = {
            "sample_ts": self._ts_from_datetime(datetime.now()),
            "timestamp": time.time(),
            "values": dict(values or {}),
            "error": str(error or ""),
        }
        lock = context.get("lock")
        if lock is not None:
            with lock:
                context.setdefault("slow_aux_cache", {})[str(key)] = entry
            return
        context.setdefault("slow_aux_cache", {})[str(key)] = entry

    def _sampling_window_cache_entry(self, context: Dict[str, Any], key: str) -> Optional[Dict[str, Any]]:
        lock = context.get("lock")
        if lock is not None:
            with lock:
                entry = context.get("slow_aux_cache", {}).get(key)
        else:
            entry = context.get("slow_aux_cache", {}).get(key)
        if not isinstance(entry, dict):
            return None
        return dict(entry)

    def _sampling_window_fast_signal_frames(self, context: Dict[str, Any], key: str) -> List[Dict[str, Any]]:
        lock = context.get("lock")
        if lock is not None:
            with lock:
                frames = list(context.get("fast_signal_buffers", {}).get(key, []))
        else:
            frames = list(context.get("fast_signal_buffers", {}).get(key, []))
        return [dict(frame) for frame in frames if isinstance(frame, dict)]

    def _sampling_window_fast_signal_error(self, context: Dict[str, Any], key: str) -> Optional[Dict[str, Any]]:
        lock = context.get("lock")
        if lock is not None:
            with lock:
                entry = context.get("fast_signal_errors", {}).get(key)
        else:
            entry = context.get("fast_signal_errors", {}).get(key)
        if not isinstance(entry, dict):
            return None
        return dict(entry)

    def _sampling_window_active_analyzer_buffer(
        self,
        context: Dict[str, Any],
        ga: Any,
        *,
        label: Optional[str] = None,
    ) -> tuple[str, Any]:
        runtime_key = self._analyzer_runtime_key(ga, label)
        lock = context.get("lock")
        if lock is not None:
            with lock:
                buffers = context.setdefault("active_analyzer_buffers", {})
                buffer = buffers.get(runtime_key)
                if buffer is None:
                    buffer = deque(maxlen=int(context.get("active_analyzer_ring_buffer_size") or 128))
                    buffers[runtime_key] = buffer
        else:
            buffers = context.setdefault("active_analyzer_buffers", {})
            buffer = buffers.get(runtime_key)
            if buffer is None:
                buffer = deque(maxlen=int(context.get("active_analyzer_ring_buffer_size") or 128))
                buffers[runtime_key] = buffer
        return runtime_key, buffer

    def _sampling_window_active_analyzer_frames(
        self,
        context: Dict[str, Any],
        ga: Any,
        *,
        label: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        runtime_key = self._analyzer_runtime_key(ga, label)
        lock = context.get("lock")
        if lock is not None:
            with lock:
                frames = list(context.get("active_analyzer_buffers", {}).get(runtime_key, []))
        else:
            frames = list(context.get("active_analyzer_buffers", {}).get(runtime_key, []))
        return [dict(frame) for frame in frames if isinstance(frame, dict)]

    @staticmethod
    def _split_stream_frame_lines(raw: Any) -> List[str]:
        if raw is None:
            return []
        if isinstance(raw, (list, tuple)):
            lines: List[str] = []
            for item in raw:
                lines.extend(CalibrationRunner._split_stream_frame_lines(item))
            return lines
        text = str(raw or "").replace("\r", "\n")
        return [line.strip() for line in text.split("\n") if line.strip()]

    def _append_fast_signal_frame(
        self,
        context: Dict[str, Any],
        key: str,
        *,
        values: Optional[Dict[str, Any]] = None,
        source: str,
    ) -> None:
        entry = {
            "recv_wall_ts": self._ts_from_datetime(datetime.now()),
            "timestamp": time.time(),
            "recv_mono_s": time.monotonic(),
            "values": dict(values or {}),
            "source": str(source or key),
            "seq": self._next_fast_signal_frame_seq(),
        }
        lock = context.get("lock")
        if lock is not None:
            with lock:
                context.setdefault("fast_signal_buffers", {}).setdefault(
                    key,
                    deque(maxlen=self._sampling_fast_signal_ring_buffer_size()),
                ).append(entry)
                context.setdefault("fast_signal_errors", {}).pop(key, None)
            return
        context.setdefault("fast_signal_buffers", {}).setdefault(
            key,
            deque(maxlen=self._sampling_fast_signal_ring_buffer_size()),
        ).append(entry)
        context.setdefault("fast_signal_errors", {}).pop(key, None)

    def _record_fast_signal_error(self, context: Dict[str, Any], key: str, error: Any) -> None:
        entry = {
            "recv_wall_ts": self._ts_from_datetime(datetime.now()),
            "timestamp": time.time(),
            "recv_mono_s": time.monotonic(),
            "error": str(error or ""),
        }
        lock = context.get("lock")
        if lock is not None:
            with lock:
                context.setdefault("fast_signal_errors", {})[key] = entry
            return
        context.setdefault("fast_signal_errors", {})[key] = entry

    def _record_active_analyzer_frame(
        self,
        context: Dict[str, Any],
        label: str,
        ga: Any,
        *,
        line: str,
        parsed: Optional[Dict[str, Any]],
        category: str,
    ) -> None:
        seq = self._next_live_analyzer_frame_seq()
        entry = {
            "recv_wall_ts": self._ts_from_datetime(datetime.now()),
            "timestamp": time.time(),
            "recv_mono_s": time.monotonic(),
            "seq": seq,
            "line": str(line or ""),
            "parsed": dict(parsed) if isinstance(parsed, dict) else None,
            "category": str(category or ""),
            "source": "active_stream",
            "is_live": True,
        }
        _runtime_key, buffer = self._sampling_window_active_analyzer_buffer(context, ga, label=label)
        lock = context.get("lock")
        if lock is not None:
            with lock:
                buffer.append(entry)
        else:
            buffer.append(entry)
        if isinstance(parsed, dict) and parsed:
            self._cache_live_analyzer_frame(
                ga,
                line,
                parsed,
                category=category,
                label=label,
                source="active_stream",
                is_live=True,
                recv_wall_ts=entry["recv_wall_ts"],
                recv_mono_s=entry["recv_mono_s"],
                timestamp=entry["timestamp"],
                seq=seq,
            )

    def _drain_active_analyzer_lines(self, ga: Any) -> List[str]:
        poll_s = self._sampling_active_drain_poll_s()
        read_timeout_s = min(0.05, poll_s)
        drain_lines = getattr(ga, "_drain_stream_lines", None)
        if callable(drain_lines):
            try:
                return self._split_stream_frame_lines(drain_lines(drain_s=poll_s, read_timeout_s=read_timeout_s))
            except TypeError:
                return self._split_stream_frame_lines(drain_lines())

        read_latest = getattr(ga, "read_latest_data", None)
        if callable(read_latest):
            try:
                raw = read_latest(
                    prefer_stream=True,
                    drain_s=poll_s,
                    read_timeout_s=read_timeout_s,
                    allow_passive_fallback=False,
                )
            except TypeError:
                raw = read_latest()
            return self._split_stream_frame_lines(raw)
        return []

    def _refresh_fast_signal_entry(self, context: Dict[str, Any], signal_key: str, *, reason: str = "") -> None:
        key = str(signal_key or "").strip().lower()
        if key == "pace":
            pace = self.devices.get("pace")
            if pace is None:
                return
            try:
                pace_pressure_hpa = self._read_pace_pressure_value(fast=True)
                self._append_fast_signal_frame(
                    context,
                    "pace",
                    values={"pressure_hpa": pace_pressure_hpa},
                    source="pace_read_pressure",
                )
            except Exception as exc:
                self._record_fast_signal_error(context, "pace", exc)
            return

        if key == "pressure_gauge":
            gauge = self.devices.get("pressure_gauge")
            if gauge is None:
                return
            try:
                gauge_pressure_hpa: Optional[float] = None
                continuous_reader = getattr(gauge, "read_pressure_continuous_latest", None)
                continuous_active = getattr(gauge, "pressure_continuous_active", None)
                if (
                    self._sampling_pressure_gauge_continuous_enabled()
                    and callable(continuous_reader)
                    and callable(continuous_active)
                    and bool(continuous_active())
                ):
                    gauge_pressure_hpa = self._as_float(
                        continuous_reader(
                            drain_s=self._sampling_pressure_gauge_continuous_drain_s(),
                            read_timeout_s=self._sampling_pressure_gauge_continuous_read_timeout_s(),
                        )
                    )
                    if gauge_pressure_hpa is None:
                        return
                else:
                    gauge_pressure_hpa = self._read_pressure_gauge_value(fast=True)
                self._append_fast_signal_frame(
                    context,
                    "pressure_gauge",
                    values={
                        "pressure_gauge_raw": gauge_pressure_hpa,
                        "pressure_gauge_hpa": gauge_pressure_hpa,
                    },
                    source="pressure_gauge_read",
                )
            except Exception as exc:
                self._record_fast_signal_error(context, "pressure_gauge", exc)
            return

        if key == "dewpoint":
            dew = self.devices.get("dewpoint")
            if dew is None:
                return
            try:
                fast_reader = getattr(dew, "get_current_fast", None)
                if callable(fast_reader):
                    dew_data = fast_reader(timeout_s=self._sampling_dewpoint_fast_timeout_s())
                else:
                    dew_data = dew.get_current(timeout_s=self._sampling_dewpoint_fast_timeout_s(), attempts=1)
                values = {}
                if isinstance(dew_data, dict):
                    values = {
                        "dewpoint_live_c": dew_data.get("dewpoint_c"),
                        "dew_temp_live_c": dew_data.get("temp_c"),
                        "dew_rh_live_pct": dew_data.get("rh_pct"),
                    }
                self._append_fast_signal_frame(
                    context,
                    "dewpoint",
                    values=values,
                    source="dewpoint_live_read",
                )
            except Exception as exc:
                self._record_fast_signal_error(context, "dewpoint", exc)

    def _refresh_fast_signal_cache_once(self, context: Dict[str, Any], *, reason: str = "") -> None:
        for signal_key in ("pace", "pressure_gauge", "dewpoint"):
            self._refresh_fast_signal_entry(context, signal_key, reason=reason)

    def _sampling_window_fast_signal_worker(self, context: Dict[str, Any]) -> None:
        stop_event = context["stop_event"]
        interval_s = self._sampling_fast_signal_worker_interval_s()
        while not self.stop_event.is_set() and not stop_event.is_set():
            try:
                reason = "sampling window start" if not context.get("_fast_signal_worker_started") else "sampling window"
                self._refresh_fast_signal_cache_once(context, reason=reason)
                context["_fast_signal_worker_started"] = True
            except Exception as exc:
                signature = f"fast_signal:{exc}"
                if signature not in context["worker_errors"]:
                    context["worker_errors"].add(signature)
                    self.log(f"Sampling window worker warning [fast_signal] err={exc}")
            if not self._sampling_window_wait(interval_s, stop_event=stop_event):
                return

    def _sampling_window_fast_signal_device_worker(self, context: Dict[str, Any], *, signal_key: str) -> None:
        stop_event = context["stop_event"]
        interval_s = self._sampling_fast_signal_worker_interval_s()
        continuous_started = False
        gauge = self.devices.get("pressure_gauge") if signal_key == "pressure_gauge" else None
        try:
            if (
                signal_key == "pressure_gauge"
                and gauge is not None
                and self._sampling_pressure_gauge_continuous_enabled()
            ):
                starter = getattr(gauge, "start_pressure_continuous", None)
                if callable(starter):
                    try:
                        continuous_started = bool(
                            starter(
                                mode=self._sampling_pressure_gauge_continuous_mode(),
                                clear_buffer=True,
                            )
                        )
                    except Exception as exc:
                        self.log(
                            "Sampling pressure-gauge continuous mode start failed; "
                            f"fallback to query mode: {exc}"
                        )
                    else:
                        if continuous_started:
                            self.log(
                                "Sampling pressure-gauge continuous mode enabled: "
                                f"mode={self._sampling_pressure_gauge_continuous_mode()}"
                            )
            first_pass = True
            while not self.stop_event.is_set() and not stop_event.is_set():
                try:
                    reason = "sampling window start" if first_pass else "sampling window"
                    self._refresh_fast_signal_entry(context, signal_key, reason=reason)
                except Exception as exc:
                    signature = f"fast_signal:{signal_key}:{exc}"
                    if signature not in context["worker_errors"]:
                        context["worker_errors"].add(signature)
                        self.log(f"Sampling window worker warning [fast_signal:{signal_key}] err={exc}")
                first_pass = False
                if not self._sampling_window_wait(interval_s, stop_event=stop_event):
                    return
        finally:
            if continuous_started and gauge is not None:
                stopper = getattr(gauge, "stop_pressure_continuous", None)
                if callable(stopper):
                    try:
                        stopped = bool(stopper(response_timeout_s=self._pressure_fast_gauge_response_timeout_s()))
                    except Exception as exc:
                        self.log(f"Sampling pressure-gauge continuous mode stop failed: {exc}")
                    else:
                        if not stopped:
                            self.log("Sampling pressure-gauge continuous mode stop fallback did not confirm clean exit")

    def _prime_sampling_window_context(
        self,
        context: Dict[str, Any],
        *,
        worker_plan: Optional[Dict[str, Any]] = None,
        reason: str = "",
    ) -> None:
        plan = dict(worker_plan or context.get("worker_plan") or {})
        skip_fast_signal_prime = bool(plan.get("skip_fast_signal_prime", False))
        skip_pace_state_prime = bool(plan.get("skip_pace_state_prime", False))
        skip_slow_aux_prime = bool(plan.get("skip_slow_aux_prime", False))
        if plan.get("fast_signal_enabled", True) and not skip_fast_signal_prime:
            self._refresh_fast_signal_cache_once(context, reason=reason)
        if (
            self.devices.get("pace") is not None
            and self._sampling_pace_state_cache_enabled()
            and self._sampling_pace_state_every_n_samples() <= 0
            and not skip_pace_state_prime
        ):
            self._pace_state_snapshot(refresh=True)
        analyzers = list(plan.get("active_entries") or []) + list(plan.get("passive_entries") or [])
        if analyzers and not bool(plan.get("skip_analyzer_prime", False)):
            self._prime_sampling_analyzer_cache_once(
                analyzers,
                include_passive=True,
                reason=reason,
                context=context,
            )
        if not skip_slow_aux_prime and self._sampling_slow_aux_cache_enabled():
            self._refresh_slow_aux_cache_once(context, reason=reason)

    def _refresh_slow_aux_cache_once(self, context: Dict[str, Any], *, reason: str = "") -> None:
        chamber = self.devices.get("temp_chamber")
        if chamber:
            values: Dict[str, Any] = {}
            error_text = ""
            try:
                chamber_temp = chamber.read_temp_c()
                chamber_rh = chamber.read_rh_pct()
                values = {
                    "env_chamber_temp_c": chamber_temp,
                    "env_chamber_rh_pct": chamber_rh,
                    "chamber_temp_c": chamber_temp,
                    "chamber_rh_pct": chamber_rh,
                }
            except Exception as exc:
                error_text = str(exc)
            self._update_slow_aux_cache_entry(context, "chamber", values=values, error=error_text)

        thermometer = self.devices.get("thermometer")
        if thermometer:
            values = {}
            error_text = ""
            try:
                values = {"thermometer_temp_c": thermometer.read_temp_c()}
            except Exception as exc:
                error_text = str(exc)
            self._update_slow_aux_cache_entry(context, "thermometer", values=values, error=error_text)

        hgen = self.devices.get("humidity_gen")
        if hgen:
            values = {}
            error_text = ""
            try:
                snap = hgen.fetch_all()
                values["hgen_raw"] = snap.get("raw") if isinstance(snap, dict) else None
                hgen_data = snap.get("data", {}) if isinstance(snap, dict) else {}
                if isinstance(hgen_data, dict):
                    for key, value in sorted(hgen_data.items(), key=lambda kv: str(kv[0])):
                        safe_key = re.sub(r"[^0-9A-Za-z_]+", "_", str(key)).strip("_")
                        if safe_key:
                            values[f"hgen_{safe_key}"] = value
            except Exception as exc:
                error_text = str(exc)
            self._update_slow_aux_cache_entry(context, "hgen", values=values, error=error_text)

    def _refresh_sampling_analyzer_cache_entry(
        self,
        label: str,
        ga: Any,
        cfg: Dict[str, Any],
        *,
        context: Optional[Dict[str, Any]] = None,
        reason: str = "",
    ) -> None:
        try:
            setattr(ga, "_runtime_label", label)
        except Exception:
            pass
        settings = self._gas_analyzer_runtime_settings(cfg)
        active_send = bool(settings["active_send"])
        if active_send and context is not None and self._sampling_active_anchor_match_enabled():
            lines = self._drain_active_analyzer_lines(ga)
            if not lines and not callable(getattr(ga, "read_latest_data", None)):
                try:
                    line, parsed = self._read_sensor_parsed(ga, require_usable=False)
                    category = self._classify_sensor_read_line(ga, line, parsed)
                    if line or parsed:
                        self._record_active_analyzer_frame(
                            context,
                            label,
                            ga,
                            line=line,
                            parsed=parsed,
                            category=category,
                        )
                    return
                except TypeError:
                    line, parsed = self._read_sensor_parsed(ga)
                    category = self._classify_sensor_read_line(ga, line, parsed)
                    if line or parsed:
                        self._record_active_analyzer_frame(
                            context,
                            label,
                            ga,
                            line=line,
                            parsed=parsed,
                            category=category,
                        )
                    return
                except Exception:
                    return
            for line in lines:
                parsed = self._parse_sensor_line(ga, line)
                category = self._classify_sensor_read_line(ga, line, parsed)
                self._record_active_analyzer_frame(
                    context,
                    label,
                    ga,
                    line=line,
                    parsed=parsed,
                    category=category,
                )
            return

        source = "active_live_cache" if active_send else "passive_cache"
        is_live = bool(active_send)
        line = ""
        parsed: Optional[Dict[str, Any]] = None
        try:
            if active_send:
                line = self._read_runtime_sensor_line(ga)
                parsed = self._parse_sensor_line(ga, line)
            else:
                line, parsed = self._read_mode2_frame(
                    ga,
                    prefer_stream=False,
                    ftd_hz=int(settings["ftd_hz"]),
                    attempts=1,
                    retry_delay_s=0.0,
                    require_usable=False,
                )
        except Exception:
            line = ""
            parsed = None
        if not parsed:
            try:
                line, parsed = self._read_sensor_parsed(ga, require_usable=False)
            except TypeError:
                line, parsed = self._read_sensor_parsed(ga)
        category = self._classify_sensor_read_line(ga, line, parsed)
        if parsed:
            self._cache_live_analyzer_frame(
                ga,
                line,
                parsed,
                category=category,
                label=label,
                source=source,
                is_live=is_live,
            )

    def _prime_sampling_analyzer_cache_once(
        self,
        analyzers: List[Tuple[str, Any, Dict[str, Any]]],
        *,
        include_passive: bool = True,
        reason: str = "",
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        for label, ga, cfg in analyzers:
            if label in self._disabled_analyzers:
                continue
            settings = self._gas_analyzer_runtime_settings(cfg)
            if not include_passive and not bool(settings["active_send"]):
                continue
            try:
                self._refresh_sampling_analyzer_cache_entry(label, ga, cfg, context=context, reason=reason)
            except Exception as exc:
                extra = f" ({reason})" if reason else ""
                self.log(f"Analyzer sampling cache prime failed: {label}{extra} err={exc}")

    def _sampling_window_analyzer_worker(self, context: Dict[str, Any]) -> None:
        stop_event = context["stop_event"]
        plan = dict(context.get("worker_plan") or {})
        active_enabled = bool(plan.get("active_enabled", False))
        passive_enabled = bool(plan.get("passive_enabled", False))
        active_interval_s = self._sampling_worker_interval_s()
        passive_interval_s = self._sampling_passive_round_robin_interval_s()
        active_entries = list(plan.get("active_entries") or [])
        passive_entries = list(plan.get("passive_entries") or [])
        if not plan:
            entries = self._sampling_window_analyzer_entries()
            active_entries = list(entries["active_entries"])
            passive_entries = list(entries["passive_entries"])
            cfg = self._live_snapshot_cfg()
            active_enabled = bool(cfg.get("sampling_worker_enabled", True)) and bool(active_entries)
            passive_enabled = bool(cfg.get("passive_round_robin_enabled", True)) and bool(passive_entries)

        passive_idx = 0
        now = time.monotonic()
        next_active_due = now
        next_passive_due = now
        while not self.stop_event.is_set() and not stop_event.is_set():
            loop_now = time.monotonic()
            if active_enabled and active_entries and loop_now >= next_active_due:
                for label, ga, analyzer_cfg in active_entries:
                    try:
                        self._refresh_sampling_analyzer_cache_entry(
                            label,
                            ga,
                            analyzer_cfg,
                            context=context,
                            reason="sampling worker",
                        )
                    except Exception as exc:
                        signature = f"active:{label}:{exc}"
                        if signature not in context["worker_errors"]:
                            context["worker_errors"].add(signature)
                            self.log(f"Sampling window worker warning [active] {label} err={exc}")
                loop_now = time.monotonic()
                next_active_due = self._advance_sampling_due_time(next_active_due, active_interval_s, loop_now)

            if passive_enabled and passive_entries and loop_now >= next_passive_due:
                label, ga, analyzer_cfg = passive_entries[passive_idx % len(passive_entries)]
                passive_idx += 1
                try:
                    self._refresh_sampling_analyzer_cache_entry(
                        label,
                        ga,
                        analyzer_cfg,
                        context=context,
                        reason="passive round-robin",
                    )
                except Exception as exc:
                    signature = f"passive:{label}:{exc}"
                    if signature not in context["worker_errors"]:
                        context["worker_errors"].add(signature)
                        self.log(f"Sampling window worker warning [passive] {label} err={exc}")
                loop_now = time.monotonic()
                next_passive_due = self._advance_sampling_due_time(next_passive_due, passive_interval_s, loop_now)

            deadlines = []
            if active_enabled and active_entries:
                deadlines.append(next_active_due)
            if passive_enabled and passive_entries:
                deadlines.append(next_passive_due)
            if not deadlines:
                return
            wait_deadline = max(min(deadlines), time.monotonic() + 0.01)
            if not self._sampling_window_wait_until(wait_deadline, stop_event=stop_event):
                return

    def _sampling_window_slow_aux_worker(self, context: Dict[str, Any]) -> None:
        stop_event = context["stop_event"]
        interval_s = self._sampling_slow_aux_cache_interval_s()
        while not self.stop_event.is_set() and not stop_event.is_set():
            try:
                self._refresh_slow_aux_cache_once(context, reason="sampling window")
            except Exception as exc:
                signature = f"slow_aux:{exc}"
                if signature not in context["worker_errors"]:
                    context["worker_errors"].add(signature)
                    self.log(f"Sampling window worker warning [slow_aux] err={exc}")
            if not self._sampling_window_wait(interval_s, stop_event=stop_event):
                return

    def _start_sampling_window_context(
        self,
        *,
        point: CalibrationPoint,
        phase: str,
        point_tag: str,
    ) -> Dict[str, Any]:
        transition_context = self._pressure_transition_fast_signal_context_active()
        context = self._new_sampling_window_context(point=point, phase=phase, point_tag=point_tag)
        bootstrapped_frames = self._bootstrap_sampling_window_context_from_transition(context, transition_context)
        self._stop_pressure_transition_fast_signal_context(reason="sampling window start")
        plan = self._sampling_window_worker_plan()
        context["worker_plan"] = plan
        prime_plan = dict(plan)
        prime_plan["skip_analyzer_prime"] = bool(plan.get("analyzer_worker_enabled", False))
        prime_plan["skip_fast_signal_prime"] = bool(plan.get("fast_signal_enabled", False))
        prime_plan["skip_pace_state_prime"] = bool(
            plan.get("fast_signal_enabled", False) and self._sampling_has_reusable_pace_state()
        )
        prime_plan["skip_slow_aux_prime"] = True
        self._prime_sampling_window_context(context, worker_plan=prime_plan, reason="sampling window start")
        pace_cache_primed = bool(self._pace_state_cache_snapshot().get("sample_ts"))
        if plan["analyzer_worker_enabled"]:
            worker = threading.Thread(
                target=self._run_sampling_window_worker,
                kwargs={
                    "context": context,
                    "worker_key": "analyzer",
                    "role": "analyzer",
                    "target": self._sampling_window_analyzer_worker,
                },
                name="sampling-analyzer-cache",
                daemon=True,
            )
            worker.start()
            context["workers"].append(
                {"key": "analyzer", "role": "analyzer", "thread": worker}
            )
        if plan["fast_signal_enabled"]:
            for signal_key in list(plan.get("fast_signal_devices") or []):
                worker_key = f"fast_signal:{signal_key}"
                role = worker_key
                worker = threading.Thread(
                    target=self._run_sampling_window_worker,
                    kwargs={
                        "context": context,
                        "worker_key": worker_key,
                        "role": role,
                        "target": self._sampling_window_fast_signal_device_worker,
                        "target_kwargs": {"signal_key": signal_key},
                    },
                    name=f"sampling-fast-signal-{signal_key}",
                    daemon=True,
                )
                worker.start()
                context["workers"].append(
                    {"key": worker_key, "role": role, "thread": worker}
                )
        if plan["slow_aux_enabled"]:
            worker = threading.Thread(
                target=self._run_sampling_window_worker,
                kwargs={
                    "context": context,
                    "worker_key": "slow_aux",
                    "role": "slow_aux",
                    "target": self._sampling_window_slow_aux_worker,
                },
                name="sampling-slow-aux-cache",
                daemon=True,
            )
            worker.start()
            context["workers"].append(
                {"key": "slow_aux", "role": "slow_aux", "thread": worker}
            )
            try:
                self._refresh_slow_aux_cache_once(context, reason="sampling window start")
            except Exception as exc:
                self.log(f"Sampling slow-aux prime failed: {exc}")
        self.log(
            "Sampling window start: "
            f"phase={context['phase']} point={point.index} point_tag={context['point_tag'] or '--'} "
            f"fast_signal_worker_enabled={plan['fast_signal_enabled']} "
            f"fast_signal_devices={','.join(plan['fast_signal_devices']) if plan['fast_signal_devices'] else '--'} "
            f"fast_signal_interval_s={self._sampling_fast_signal_worker_interval_s():g} "
            f"active_analyzer_worker_enabled={plan['active_enabled']} "
            f"active_analyzer_count={len(plan['active_entries'])} "
            f"active_ring_buffer_size={self._sampling_active_ring_buffer_size()} "
            f"passive_round_robin_enabled={plan['passive_enabled']} "
            f"passive_analyzer_count={len(plan['passive_entries'])} "
            f"slow_aux_worker_enabled={plan['slow_aux_enabled']} "
            f"slow_aux_devices={','.join(plan['slow_aux_devices']) if plan['slow_aux_devices'] else '--'} "
            f"pace_state_strategy={self._sampling_pace_state_strategy_text()} "
            f"pace_state_cache_primed={pace_cache_primed} "
            f"transition_bootstrap_frames={bootstrapped_frames}"
        )
        return context

    def _stop_sampling_window_context(self, context: Optional[Dict[str, Any]]) -> None:
        if not isinstance(context, dict):
            return
        stop_event = context.get("stop_event")
        if isinstance(stop_event, threading.Event):
            stop_event.set()
        worker_summaries: List[str] = []
        for worker_entry in list(context.get("workers", []) or []):
            worker_key = ""
            role = ""
            worker: Optional[threading.Thread] = None
            if isinstance(worker_entry, dict):
                worker_key = str(worker_entry.get("key") or "")
                role = str(worker_entry.get("role") or worker_key or "worker")
                maybe_thread = worker_entry.get("thread")
                if isinstance(maybe_thread, threading.Thread):
                    worker = maybe_thread
            elif isinstance(worker_entry, threading.Thread):
                worker = worker_entry
                worker_key = worker.name
                role = worker.name
            if worker is None:
                continue
            join_timeout_s = 0.35 if role == "fast_signal" else 0.05
            worker.join(timeout=join_timeout_s)
            worker_state = dict(context.get("worker_states", {}).get(worker_key, {}) or {})
            worker_summaries.append(
                f"{role}:exited={bool(worker_state.get('exited'))} "
                f"clean_exit={bool(worker_state.get('clean_exit')) and (not worker.is_alive())} "
                f"alive={worker.is_alive()}"
            )
        self.log(
            "Sampling window stop: "
            f"phase={context.get('phase') or '--'} point={getattr(context.get('point'), 'index', '--')} "
            f"pace_state_strategy={self._sampling_pace_state_strategy_text()} "
            f"workers={'; '.join(worker_summaries) if worker_summaries else 'none'} "
            f"worker_warning_count={len(context.get('worker_errors', set()) or set())}"
        )

    def _pressure_transition_fast_signal_context_active(self) -> Optional[Dict[str, Any]]:
        context = self._pressure_transition_fast_signal_context
        if not isinstance(context, dict):
            return None
        stop_event = context.get("stop_event")
        if isinstance(stop_event, threading.Event) and stop_event.is_set():
            return None
        return context

    def _pressure_transition_fast_signal_ttl_s(self) -> float:
        base_ttl_s = max(0.25, float(self._sampling_fast_signal_worker_interval_s()) * 3.0)
        gauge_ttl_s = self._pressure_transition_gauge_response_timeout_s() + 0.75
        return max(base_ttl_s, gauge_ttl_s)

    def _pressure_transition_monitor_wait_s(self, point: Optional[CalibrationPoint] = None) -> float:
        base_wait_s = max(0.05, self._pressure_trace_poll_s(point))
        context = self._pressure_transition_fast_signal_context_active()
        if context is None:
            return base_wait_s
        if not list(context.get("workers", []) or []):
            return base_wait_s
        return max(0.02, min(base_wait_s, float(self._sampling_fast_signal_worker_interval_s()) / 2.0))

    def _pressure_transition_fast_signal_devices(self) -> List[str]:
        return [
            signal_key
            for signal_key in ("pace", "pressure_gauge", "dewpoint")
            if self.devices.get(signal_key) is not None
        ]

    def _pressure_transition_fast_signal_worker_devices(self) -> List[str]:
        devices = self._pressure_transition_fast_signal_devices()
        if not any(signal_key in ("pressure_gauge", "dewpoint") for signal_key in devices):
            return []
        return devices

    def _pressure_transition_fast_signal_worker_running(self, context: Optional[Dict[str, Any]]) -> bool:
        if not isinstance(context, dict):
            return False
        for worker_entry in list(context.get("workers", []) or []):
            if not isinstance(worker_entry, dict):
                continue
            worker_key = str(worker_entry.get("key") or "")
            if not worker_key.startswith("pressure_transition_fast_signal:"):
                continue
            worker = worker_entry.get("thread")
            if isinstance(worker, threading.Thread) and worker.is_alive():
                return True
        worker_states = context.get("worker_states", {})
        if not isinstance(worker_states, dict):
            return False
        for worker_key, worker_state in worker_states.items():
            if not str(worker_key or "").startswith("pressure_transition_fast_signal:"):
                continue
            if isinstance(worker_state, dict) and not bool(worker_state.get("exited")):
                return True
        return False

    def _pressure_transition_missing_fresh_fast_signals(self, context: Optional[Dict[str, Any]]) -> List[str]:
        if not isinstance(context, dict):
            return []
        missing: List[str] = []
        ttl_s = self._pressure_transition_fast_signal_ttl_s()
        for signal_key in self._pressure_transition_fast_signal_devices():
            frame = self._latest_fast_signal_frame(signal_key, context=context, max_age_s=ttl_s)
            if not self._fast_signal_frame_has_required_value(signal_key, frame):
                missing.append(signal_key)
        return missing

    def _ensure_pressure_transition_fast_signal_cache(
        self,
        context: Optional[Dict[str, Any]],
        *,
        reason: str = "",
    ) -> List[str]:
        if not isinstance(context, dict):
            return []
        missing = self._pressure_transition_missing_fresh_fast_signals(context)
        if not missing:
            return []
        if self._pressure_transition_fast_signal_worker_running(context):
            return missing
        self._refresh_pressure_transition_fast_signal_once(
            context,
            reason=reason or "pressure transition fallback refresh",
        )
        return self._pressure_transition_missing_fresh_fast_signals(context)

    def _latest_fast_signal_frame(
        self,
        key: str,
        *,
        context: Optional[Dict[str, Any]] = None,
        max_age_s: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        contexts: List[Dict[str, Any]] = []
        if isinstance(context, dict):
            contexts.append(context)
        else:
            if isinstance(self._sampling_window_context, dict):
                contexts.append(self._sampling_window_context)
            transition_context = self._pressure_transition_fast_signal_context_active()
            if isinstance(transition_context, dict) and transition_context not in contexts:
                contexts.append(transition_context)

        newest: Optional[Dict[str, Any]] = None
        newest_mono: Optional[float] = None
        for one_context in contexts:
            for frame in self._sampling_window_fast_signal_frames(one_context, key):
                if not isinstance(frame, dict):
                    continue
                try:
                    recv_mono_s = float(frame.get("recv_mono_s"))
                except Exception:
                    continue
                if newest is None or newest_mono is None or recv_mono_s > newest_mono:
                    newest = dict(frame)
                    newest["recv_mono_s"] = recv_mono_s
                    newest_mono = recv_mono_s

        if newest is None or newest_mono is None:
            return None

        ttl_s = self._pressure_transition_fast_signal_ttl_s() if max_age_s is None else max(0.0, float(max_age_s))
        if ttl_s > 0 and (time.monotonic() - newest_mono) > ttl_s:
            return None
        return newest

    def _refresh_pressure_transition_fast_signal_once(
        self,
        context: Dict[str, Any],
        *,
        reason: str = "",
        skip_keys: Optional[Iterable[str]] = None,
    ) -> None:
        skip = {str(one_key or "").strip().lower() for one_key in list(skip_keys or [])}
        for signal_key in self._pressure_transition_fast_signal_devices():
            if str(signal_key or "").strip().lower() in skip:
                continue
            self._refresh_pressure_transition_fast_signal_entry(context, signal_key, reason=reason)

    def _refresh_pressure_transition_fast_signal_entry(
        self,
        context: Dict[str, Any],
        signal_key: str,
        *,
        reason: str = "",
    ) -> None:
        key = str(signal_key or "").strip().lower()
        if key != "pressure_gauge":
            self._refresh_fast_signal_entry(context, key, reason=reason)
            return

        gauge = self.devices.get("pressure_gauge")
        if gauge is None:
            return
        try:
            gauge_pressure_hpa: Optional[float] = None
            continuous_reader = getattr(gauge, "read_pressure_continuous_latest", None)
            continuous_active = getattr(gauge, "pressure_continuous_active", None)
            if (
                callable(continuous_reader)
                and callable(continuous_active)
                and bool(continuous_active())
            ):
                gauge_pressure_hpa = self._as_float(
                    continuous_reader(
                        drain_s=self._pressure_transition_gauge_continuous_drain_s(),
                        read_timeout_s=self._pressure_transition_gauge_continuous_read_timeout_s(),
                    )
                )
                if gauge_pressure_hpa is None:
                    return
            else:
                gauge_pressure_hpa = self._read_pressure_gauge_value(
                    fast=True,
                    purpose="transition",
                )
            self._append_fast_signal_frame(
                context,
                "pressure_gauge",
                values={
                    "pressure_gauge_raw": gauge_pressure_hpa,
                    "pressure_gauge_hpa": gauge_pressure_hpa,
                },
                source="pressure_gauge_transition_read",
            )
        except Exception as exc:
            self._record_fast_signal_error(context, "pressure_gauge", exc)

    def _pressure_transition_fast_signal_worker(self, context: Dict[str, Any]) -> None:
        stop_event = context["stop_event"]
        interval_s = self._sampling_fast_signal_worker_interval_s()
        if not self._sampling_window_wait(interval_s, stop_event=stop_event):
            return
        while not self.stop_event.is_set() and not stop_event.is_set():
            self._refresh_pressure_transition_fast_signal_once(context, reason="pressure transition")
            if not self._sampling_window_wait(interval_s, stop_event=stop_event):
                return

    def _pressure_transition_fast_signal_device_worker(self, context: Dict[str, Any], *, signal_key: str) -> None:
        stop_event = context["stop_event"]
        interval_s = self._sampling_fast_signal_worker_interval_s()
        continuous_started = False
        gauge = self.devices.get("pressure_gauge") if signal_key == "pressure_gauge" else None
        try:
            if (
                signal_key == "pressure_gauge"
                and gauge is not None
                and self._pressure_transition_gauge_continuous_enabled()
            ):
                starter = getattr(gauge, "start_pressure_continuous", None)
                if callable(starter):
                    try:
                        continuous_started = bool(
                            starter(
                                mode=self._pressure_transition_gauge_continuous_mode(),
                                clear_buffer=True,
                            )
                        )
                    except Exception as exc:
                        self.log(
                            "Pressure transition pressure-gauge continuous mode start failed; "
                            f"fallback to query mode: {exc}"
                        )
                    else:
                        if continuous_started:
                            self.log(
                                "Pressure transition pressure-gauge continuous mode enabled: "
                                f"mode={self._pressure_transition_gauge_continuous_mode()}"
                            )
            if continuous_started:
                try:
                    self._refresh_pressure_transition_fast_signal_entry(
                        context,
                        signal_key,
                        reason="pressure transition start",
                    )
                except Exception as exc:
                    signature = f"pressure_transition_fast_signal:{signal_key}:{exc}"
                    if signature not in context["worker_errors"]:
                        context["worker_errors"].add(signature)
                        self.log(
                            f"Sampling window worker warning [pressure_transition_fast_signal:{signal_key}] err={exc}"
                        )
            if not self._sampling_window_wait(interval_s, stop_event=stop_event):
                return
            while not self.stop_event.is_set() and not stop_event.is_set():
                try:
                    self._refresh_pressure_transition_fast_signal_entry(
                        context,
                        signal_key,
                        reason="pressure transition",
                    )
                except Exception as exc:
                    signature = f"pressure_transition_fast_signal:{signal_key}:{exc}"
                    if signature not in context["worker_errors"]:
                        context["worker_errors"].add(signature)
                        self.log(
                            f"Sampling window worker warning [pressure_transition_fast_signal:{signal_key}] err={exc}"
                        )
                if not self._sampling_window_wait(interval_s, stop_event=stop_event):
                    return
        finally:
            if continuous_started and gauge is not None:
                stopper = getattr(gauge, "stop_pressure_continuous", None)
                if callable(stopper):
                    try:
                        stopped = bool(stopper(response_timeout_s=self._pressure_fast_gauge_response_timeout_s()))
                    except Exception as exc:
                        self.log(f"Pressure transition pressure-gauge continuous mode stop failed: {exc}")
                    else:
                        if not stopped:
                            self.log(
                                "Pressure transition pressure-gauge continuous mode stop fallback "
                                "did not confirm clean exit"
                            )

    def _start_pressure_transition_fast_signal_context(
        self,
        *,
        point: CalibrationPoint,
        phase: str,
        point_tag: str = "",
        reason: str = "",
        prime_immediately: bool = True,
    ) -> Optional[Dict[str, Any]]:
        fast_signal_devices = self._pressure_transition_fast_signal_devices()
        if not fast_signal_devices:
            return None
        worker_devices = self._pressure_transition_fast_signal_worker_devices()

        context = self._pressure_transition_fast_signal_context_active()
        if context is None:
            context = self._new_sampling_window_context(point=point, phase=phase, point_tag=point_tag)
            self._pressure_transition_fast_signal_context = context
            if prime_immediately:
                skip_keys: List[str] = []
                if (
                    self._sampling_fast_signal_worker_enabled()
                    and "pressure_gauge" in worker_devices
                    and self._pressure_transition_gauge_continuous_enabled()
                ):
                    skip_keys.append("pressure_gauge")
                self._refresh_pressure_transition_fast_signal_once(
                    context,
                    reason=reason or "pressure transition start",
                    skip_keys=skip_keys,
                )
            if self._sampling_fast_signal_worker_enabled() and worker_devices:
                for signal_key in worker_devices:
                    worker_key = f"pressure_transition_fast_signal:{signal_key}"
                    worker = threading.Thread(
                        target=self._run_sampling_window_worker,
                        kwargs={
                            "context": context,
                            "worker_key": worker_key,
                            "role": worker_key,
                            "target": self._pressure_transition_fast_signal_device_worker,
                            "target_kwargs": {"signal_key": signal_key},
                        },
                        name=f"pressure-transition-fast-signal-{signal_key}",
                        daemon=True,
                    )
                    context["workers"].append(
                        {
                            "key": worker_key,
                            "role": worker_key,
                            "thread": worker,
                        }
                    )
                    worker.start()
            extra = f" ({reason})" if reason else ""
            self.log(
                "Pressure transition fast-signal start"
                f"{extra}: phase={phase or '--'} point={point.index} "
                f"devices={','.join(fast_signal_devices)} "
                f"workers={','.join(worker_devices) if worker_devices else 'sync-only'} "
                f"interval_s={self._sampling_fast_signal_worker_interval_s():g}"
            )
            return context

        context["point"] = point
        context["phase"] = str(phase or "").strip().lower()
        context["point_tag"] = str(point_tag or "")
        return context

    def _stop_pressure_transition_fast_signal_context(self, *, reason: str = "") -> None:
        context = self._pressure_transition_fast_signal_context
        self._pressure_transition_fast_signal_context = None
        if not isinstance(context, dict):
            return
        stop_event = context.get("stop_event")
        if isinstance(stop_event, threading.Event):
            stop_event.set()
        for worker_entry in list(context.get("workers", []) or []):
            worker = worker_entry.get("thread") if isinstance(worker_entry, dict) else None
            if isinstance(worker, threading.Thread):
                worker.join(timeout=1.0)
        extra = f" ({reason})" if reason else ""
        self.log(f"Pressure transition fast-signal stop{extra}")

    def _analyzer_cache_snapshot(
        self,
        ga: Any,
        *,
        label: Optional[str] = None,
        now_s: Optional[float] = None,
    ) -> Tuple[Optional[Dict[str, Any]], bool, Optional[float]]:
        entry = self._get_live_analyzer_frame_cache(ga, label=label)
        if not isinstance(entry, dict):
            return None, False, None
        ttl_s = max(0.0, float(self._live_snapshot_cfg().get("cache_ttl_s", 0.5) or 0.5))
        age_ms = self._cache_age_ms(entry.get("timestamp"), now_s)
        fresh = bool(ttl_s > 0 and age_ms is not None and age_ms <= ttl_s * 1000.0)
        return entry, fresh, age_ms

    def _anchor_frame_candidates(
        self,
        frames: List[Dict[str, Any]],
        sample_anchor_mono: float,
        *,
        parsed_only: bool = False,
    ) -> tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        left_match: Optional[Dict[str, Any]] = None
        right_match: Optional[Dict[str, Any]] = None
        for frame in frames:
            if not isinstance(frame, dict):
                continue
            if parsed_only and not isinstance(frame.get("parsed"), dict):
                continue
            try:
                recv_mono_s = float(frame.get("recv_mono_s"))
            except Exception:
                continue
            candidate = dict(frame)
            candidate["recv_mono_s"] = recv_mono_s
            if recv_mono_s <= sample_anchor_mono:
                if left_match is None or recv_mono_s > float(left_match.get("recv_mono_s", 0.0) or 0.0):
                    left_match = candidate
            elif right_match is None or recv_mono_s < float(right_match.get("recv_mono_s", 0.0) or 0.0):
                right_match = candidate
        return left_match, right_match

    def _select_anchor_frame_match(
        self,
        frames: List[Dict[str, Any]],
        sample_anchor_mono: float,
        *,
        parsed_only: bool = False,
        signal_key: str = "",
    ) -> Dict[str, Any]:
        left_match, right_match = self._anchor_frame_candidates(
            frames,
            sample_anchor_mono,
            parsed_only=parsed_only,
        )
        max_left_ms = self._fast_signal_left_match_max_ms(signal_key)
        right_max_ms = self._fast_signal_right_match_max_ms(signal_key)
        stale_ms = self._fast_signal_stale_ms(signal_key)
        if left_match is not None:
            delta_ms = round(max(0.0, (sample_anchor_mono - float(left_match["recv_mono_s"])) * 1000.0), 3)
            if delta_ms <= max_left_ms:
                return {
                    "entry": left_match,
                    "delta_ms": delta_ms,
                    "side": "before_anchor",
                    "match_strategy": "left_match",
                    "stale": False,
                    "beyond_stale_limit": False,
                }
            if delta_ms <= stale_ms:
                return {
                    "entry": left_match,
                    "delta_ms": delta_ms,
                    "side": "stale",
                    "match_strategy": "stale_left_fallback",
                    "stale": True,
                    "beyond_stale_limit": False,
                }
            return {
                "entry": left_match,
                "delta_ms": delta_ms,
                "side": "stale",
                "match_strategy": "stale_left_far",
                "stale": True,
                "beyond_stale_limit": True,
            }
        if right_match is not None:
            delta_ms = round(max(0.0, (float(right_match["recv_mono_s"]) - sample_anchor_mono) * 1000.0), 3)
            if delta_ms <= right_max_ms:
                return {
                    "entry": right_match,
                    "delta_ms": delta_ms,
                    "side": "after_anchor",
                    "match_strategy": "right_match",
                    "stale": False,
                    "beyond_stale_limit": False,
                }
            if delta_ms <= stale_ms:
                return {
                    "entry": right_match,
                    "delta_ms": delta_ms,
                    "side": "stale",
                    "match_strategy": "stale_right_fallback",
                    "stale": True,
                    "beyond_stale_limit": False,
                }
            return {
                "entry": right_match,
                "delta_ms": delta_ms,
                "side": "stale",
                "match_strategy": "stale_right_far",
                "stale": True,
                "beyond_stale_limit": True,
            }
        return {
            "entry": None,
            "delta_ms": None,
            "side": "missing",
            "match_strategy": "missing",
            "stale": True,
            "beyond_stale_limit": True,
        }

    @staticmethod
    def _frame_category_status_text(category: str) -> str:
        mapping = {
            "empty": "空帧",
            "ack": "ACK帧",
            "parse_failed": "解析失败",
            "parsed": "已解析",
        }
        text = str(category or "").strip()
        if not text:
            return "无帧"
        if text in mapping:
            return mapping[text]
        if text.startswith("short_frame"):
            return f"短帧({text.split('(', 1)[-1].rstrip(')')})"
        return text

    def _merge_fast_signal_cache_into_sample(
        self,
        data: Dict[str, Any],
        context: Optional[Dict[str, Any]],
        *,
        sample_anchor_mono: float,
        row_time_s: float,
    ) -> None:
        if not isinstance(context, dict):
            return

        signal_specs = [
            (
                "pace",
                "pace_sample_ts",
                "pace_anchor_delta_ms",
                "pressure_error",
                ("pressure_hpa",),
            ),
            (
                "pressure_gauge",
                "pressure_gauge_sample_ts",
                "pressure_gauge_anchor_delta_ms",
                "pressure_gauge_error",
                ("pressure_gauge_raw", "pressure_gauge_hpa"),
            ),
            (
                "dewpoint",
                "dewpoint_live_sample_ts",
                "dewpoint_live_anchor_delta_ms",
                "dewpoint_live_error",
                ("dewpoint_live_c", "dew_temp_live_c", "dew_rh_live_pct"),
            ),
        ]

        for signal_key, sample_ts_key, delta_key, error_key, value_keys in signal_specs:
            match = self._select_anchor_frame_match(
                self._sampling_window_fast_signal_frames(context, signal_key),
                sample_anchor_mono,
                signal_key=signal_key,
            )
            entry = match.get("entry")
            if entry is not None:
                data[delta_key] = match.get("delta_ms")
            if entry is not None and not bool(match.get("beyond_stale_limit")):
                data[sample_ts_key] = entry.get("recv_wall_ts")
                values = dict(entry.get("values", {}) or {})
                for value_key in value_keys:
                    if value_key in values:
                        data[value_key] = values.get(value_key)
                continue

            if entry is not None and bool(match.get("beyond_stale_limit")):
                data[error_key] = "fast_signal_stale"
                continue

            error_entry = self._sampling_window_fast_signal_error(context, signal_key)
            if error_entry and error_entry.get("error"):
                data[error_key] = error_entry.get("error")

    def _active_analyzer_anchor_match(
        self,
        context: Dict[str, Any],
        ga: Any,
        *,
        label: str,
        sample_anchor_mono: float,
    ) -> Dict[str, Any]:
        frames = self._sampling_window_active_analyzer_frames(context, ga, label=label)
        parsed_match = self._select_anchor_frame_match(
            frames,
            sample_anchor_mono,
            parsed_only=True,
        )
        if parsed_match.get("entry") is not None:
            parsed_match["selection_kind"] = "parsed"
            return parsed_match
        raw_match = self._select_anchor_frame_match(
            frames,
            sample_anchor_mono,
            parsed_only=False,
        )
        raw_match["selection_kind"] = "raw"
        return raw_match

    def _merge_slow_aux_cache_into_sample(
        self,
        data: Dict[str, Any],
        context: Optional[Dict[str, Any]],
        *,
        row_time_s: float,
    ) -> None:
        if not isinstance(context, dict):
            return

        chamber_entry = self._sampling_window_cache_entry(context, "chamber")
        if chamber_entry:
            data["chamber_sample_ts"] = chamber_entry.get("sample_ts")
            data["chamber_cache_age_ms"] = self._cache_age_ms(chamber_entry.get("timestamp"), row_time_s)
            for key, value in dict(chamber_entry.get("values", {}) or {}).items():
                data[key] = value
            if chamber_entry.get("error"):
                data["chamber_error"] = chamber_entry.get("error")

        thermometer_entry = self._sampling_window_cache_entry(context, "thermometer")
        if thermometer_entry:
            data["thermometer_sample_ts"] = thermometer_entry.get("sample_ts")
            data["thermometer_cache_age_ms"] = self._cache_age_ms(thermometer_entry.get("timestamp"), row_time_s)
            for key, value in dict(thermometer_entry.get("values", {}) or {}).items():
                data[key] = value
            if thermometer_entry.get("error"):
                data["thermometer_error"] = thermometer_entry.get("error")

        hgen_entry = self._sampling_window_cache_entry(context, "hgen")
        if hgen_entry:
            data["hgen_sample_ts"] = hgen_entry.get("sample_ts")
            data["hgen_cache_age_ms"] = self._cache_age_ms(hgen_entry.get("timestamp"), row_time_s)
            for key, value in dict(hgen_entry.get("values", {}) or {}).items():
                data[key] = value
            if hgen_entry.get("error"):
                data["hgen_error"] = hgen_entry.get("error")

    def _merge_analyzer_cache_into_sample(
        self,
        data: Dict[str, Any],
        gas_analyzers: List[Tuple[str, Any, Dict[str, Any]]],
        *,
        context: Optional[Dict[str, Any]],
        sample_anchor_mono: float,
        row_time_s: float,
    ) -> Dict[str, int]:
        mode2_keys = self._mode2_sample_fields()
        frame_issue_counts: Dict[str, int] = {}
        active_anchor_match_enabled = self._sampling_active_anchor_match_enabled()
        for analyzer_idx, (label, ga, analyzer_cfg) in enumerate(gas_analyzers):
            prefix = self._safe_label(label)
            for key in mode2_keys:
                data.setdefault(f"{prefix}_{key}", None)
                if analyzer_idx == 0:
                    data.setdefault(key, None)
            data.setdefault(f"{prefix}_frame_cache_ts", None)
            data.setdefault(f"{prefix}_frame_cache_age_ms", None)
            data.setdefault(f"{prefix}_frame_rx_ts", None)
            data.setdefault(f"{prefix}_frame_rx_seq", None)
            data.setdefault(f"{prefix}_frame_anchor_delta_ms", None)
            data.setdefault(f"{prefix}_frame_anchor_side", "missing")
            data.setdefault(f"{prefix}_frame_match_strategy", "missing")
            data.setdefault(f"{prefix}_frame_stale", True)
            data.setdefault(f"{prefix}_frame_source", "missing")
            data.setdefault(f"{prefix}_frame_is_live", False)
            self._set_sample_frame_meta(
                data,
                prefix,
                has_data=False,
                usable=False,
                status="无帧",
                is_primary=analyzer_idx == 0,
            )

            if label in self._disabled_analyzers:
                data[f"{prefix}_frame_status"] = "已禁用"
                if analyzer_idx == 0:
                    data["frame_status"] = "已禁用"
                continue

            settings = self._gas_analyzer_runtime_settings(analyzer_cfg)
            active_send = bool(settings["active_send"])
            entry: Optional[Dict[str, Any]]
            fresh = False
            age_ms: Optional[float] = None
            parsed: Optional[Dict[str, Any]] = None
            match_side = "missing"
            match_strategy = "missing"
            frame_stale = True
            if active_send and active_anchor_match_enabled and isinstance(context, dict):
                match = self._active_analyzer_anchor_match(
                    context,
                    ga,
                    label=label,
                    sample_anchor_mono=sample_anchor_mono,
                )
                entry = match.get("entry")
                age_ms = self._cache_age_ms(entry.get("timestamp"), row_time_s) if isinstance(entry, dict) else None
                data[f"{prefix}_frame_anchor_delta_ms"] = match.get("delta_ms")
                match_side = str(match.get("side") or "missing")
                match_strategy = str(match.get("match_strategy") or "missing")
                frame_stale = bool(match.get("stale", True))
                fresh = entry is not None
                if isinstance(entry, dict):
                    parsed = entry.get("parsed")
            else:
                entry, fresh, age_ms = self._analyzer_cache_snapshot(ga, label=label, now_s=row_time_s)
                if isinstance(entry, dict):
                    parsed = entry.get("parsed")
                match_strategy = "passive_cache"
                match_side = "missing"
                frame_stale = not bool(fresh)

            data[f"{prefix}_frame_anchor_side"] = match_side
            data[f"{prefix}_frame_match_strategy"] = match_strategy
            data[f"{prefix}_frame_stale"] = bool(frame_stale)

            if entry:
                cache_ts = entry.get("recv_wall_ts")
                if not cache_ts and entry.get("timestamp") is not None:
                    cache_ts = datetime.fromtimestamp(float(entry.get("timestamp", 0.0) or 0.0)).isoformat(
                        timespec="milliseconds"
                    )
                data[f"{prefix}_frame_cache_ts"] = cache_ts
                data[f"{prefix}_frame_rx_ts"] = entry.get("recv_wall_ts") or cache_ts
                data[f"{prefix}_frame_rx_seq"] = entry.get("seq")
                data[f"{prefix}_frame_cache_age_ms"] = age_ms
                data[f"{prefix}_frame_is_live"] = bool(entry.get("is_live"))
                data[f"{prefix}_frame_source"] = str(
                    entry.get("source") or ("active_stream" if active_send else "passive_cache")
                )

            if not entry or not fresh:
                data[f"{prefix}_frame_source"] = "missing" if not entry else data[f"{prefix}_frame_source"]
                data[f"{prefix}_frame_is_live"] = False if not entry else bool(entry.get("is_live"))
                data[f"{prefix}_frame_status"] = "缓存缺失" if not entry else "缓存过期"
                if analyzer_idx == 0:
                    data["frame_status"] = data[f"{prefix}_frame_status"]
                continue

            if not isinstance(parsed, dict) or not parsed:
                data[f"{prefix}_frame_status"] = self._frame_category_status_text(str(entry.get("category") or ""))
                if analyzer_idx == 0:
                    data["frame_status"] = data[f"{prefix}_frame_status"]
                continue

            usable, frame_status = self._assess_analyzer_frame(parsed)
            self._set_sample_frame_meta(
                data,
                prefix,
                has_data=True,
                usable=usable,
                status=frame_status,
                is_primary=analyzer_idx == 0,
            )
            if not usable and frame_status:
                frame_issue_counts[frame_status] = frame_issue_counts.get(frame_status, 0) + 1

            if analyzer_idx == 0 and usable:
                for key, value in parsed.items():
                    if key in {"chamber_temp_c"}:
                        continue
                    data[key] = value

            for key, value in parsed.items():
                data[f"{prefix}_{key}"] = value

        return frame_issue_counts

    def _set_temperature_for_point(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        point_tag: str = "",
    ) -> bool:
        previous = self._temperature_wait_context
        self._temperature_wait_context = {
            "point": point,
            "phase": str(phase or "").strip().lower(),
            "point_tag": str(point_tag or "").strip(),
        }
        try:
            return self._set_temperature(point.temp_chamber_c)
        finally:
            self._temperature_wait_context = previous

    def _emit_analyzer_chamber_temp_stage(
        self,
        target_c: float,
        *,
        analyzer_label: Optional[str] = None,
        analyzer_temp_c: Optional[float] = None,
        countdown_s: Optional[float] = None,
        detail: Optional[str] = None,
    ) -> None:
        ctx = self._temperature_wait_context or {}
        point = ctx.get("point")
        phase = str(ctx.get("phase", "") or "").strip().lower()
        point_tag = str(ctx.get("point_tag", "") or "").strip()

        if point is not None:
            current = f"{self._stage_label_for_point(point, phase=phase, include_pressure=False)} 腔温判稳"
        else:
            current = f"温箱到位后腔温判稳 {float(target_c):g}°C"

        if analyzer_label and analyzer_temp_c is not None:
            current += f" {analyzer_label}={float(analyzer_temp_c):.2f}°C"
        elif analyzer_label:
            current += f" {analyzer_label}"

        self._emit_stage_event(
            current=current,
            point=point,
            phase=phase,
            point_tag=point_tag,
            wait_reason="分析仪腔温判稳",
            countdown_s=countdown_s,
            detail=detail,
        )

    def stop(self) -> None:
        if self.stop_event.is_set():
            return
        self.stop_event.set()
        self._log_run_event(command="stop-request", response="runner.stop() called")

    def pause(self) -> None:
        self.pause_event.clear()

    def resume(self) -> None:
        self.pause_event.set()

    def _check_pause(self) -> None:
        while not self.pause_event.is_set():
            if self.stop_event.is_set():
                return
            time.sleep(0.2)

    def _wf(self, path: str, default: Any = None) -> Any:
        return workflow_param(self.cfg, path, default)

    def _resolve_postrun_bool(
        self,
        *,
        path: str,
        env_names: Tuple[str, ...],
    ) -> Tuple[bool, str]:
        for name in env_names:
            env_value = _optional_env_bool(name)
            if env_value is not None:
                return bool(env_value), f"ENV:{name}"
        current_value = bool(cfg_get(self.cfg, path, runtime_default(path, False)))
        default_value = bool(runtime_default(path, False))
        return current_value, "default" if current_value == default_value else "config"

    def _effective_postrun_corrected_delivery_cfg(self) -> Dict[str, Any]:
        raw_cfg = self.cfg.get("workflow", {}).get("postrun_corrected_delivery", {})
        cfg = dict(raw_cfg or {}) if isinstance(raw_cfg, dict) else {}
        verify_short_run_cfg = dict(cfg.get("verify_short_run", {}) or {})

        enabled, enabled_source = self._resolve_postrun_bool(
            path="workflow.postrun_corrected_delivery.enabled",
            env_names=("GAS_CAL_POSTRUN_CORRECTED_DELIVERY_ENABLED",),
        )
        write_devices, write_source = self._resolve_postrun_bool(
            path="workflow.postrun_corrected_delivery.write_devices",
            env_names=(
                "GAS_CAL_POSTRUN_CORRECTED_DELIVERY_WRITE_DEVICES",
                "GAS_CAL_ALLOW_REAL_DEVICE_WRITE",
            ),
        )
        if not enabled:
            write_devices = False
            if write_source.startswith("ENV:"):
                write_source = f"{write_source} (suppressed_by_disabled_postrun)"
        cfg["enabled"] = bool(enabled)
        cfg["write_devices"] = bool(write_devices)
        cfg["verify_short_run"] = verify_short_run_cfg
        cfg["_resolved_sources"] = {
            "enabled": enabled_source,
            "write_devices": write_source,
            "allow_real_device_write": write_source,
        }
        return cfg

    def _log_postrun_corrected_delivery_effective_config(self) -> None:
        cfg = self._effective_postrun_corrected_delivery_cfg()
        sources = dict(cfg.get("_resolved_sources") or {})
        payload = {
            "enabled": bool(cfg.get("enabled", False)),
            "write_devices": bool(cfg.get("write_devices", False)),
            "allow_real_device_write": bool(cfg.get("enabled", False) and cfg.get("write_devices", False)),
            "enabled_source": sources.get("enabled", "unknown"),
            "write_devices_source": sources.get("write_devices", "unknown"),
            "allow_real_device_write_source": sources.get("allow_real_device_write", "unknown"),
        }
        self.log(
            "Postrun corrected delivery config: "
            f"enabled={payload['enabled']} source={payload['enabled_source']} "
            f"allow_real_device_write={payload['allow_real_device_write']} "
            f"source={payload['allow_real_device_write_source']}"
        )
        self._log_run_event(
            command="postrun-corrected-delivery-config",
            response=json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        )

    def _h2o_zero_span_capability_payload(
        self,
        points: Optional[List[CalibrationPoint]] = None,
    ) -> Dict[str, Any]:
        coeff_cfg = self.cfg.get("coefficients", {}) if isinstance(self.cfg.get("coefficients", {}), dict) else {}
        shared_payload = v1_h2o_zero_span_capability(coeff_cfg)
        point_list = list(points or [])
        has_h2o_points = any(bool(getattr(point, "is_h2o_point", False)) for point in point_list)
        fit_h2o_requested = bool(coeff_cfg.get("fit_h2o", False))
        return {
            "status": str(shared_payload.get("status") or "NOT_SUPPORTED").strip().upper(),
            "require_supported_capability": bool(shared_payload.get("require_supported_capability", False)),
            "has_h2o_points": has_h2o_points,
            "fit_h2o_requested": fit_h2o_requested,
            "note": (
                f"{V1_CO2_ONLY_H2O_NOT_SUPPORTED_MESSAGE} "
                "H2O route sampling and ratio-poly/report selection exist, "
                "but no explicit H2O zero/span business chain is wired into CalibrationRunner."
            ),
        }

    def _log_h2o_zero_span_capability(self, points: Optional[List[CalibrationPoint]] = None) -> None:
        payload = self._h2o_zero_span_capability_payload(points)
        self.log(
            "H2O zero/span capability: "
            f"status={payload['status']} "
            f"has_h2o_points={payload['has_h2o_points']} "
            f"fit_h2o_requested={payload['fit_h2o_requested']} "
            f"require_supported_capability={payload['require_supported_capability']} "
            f"note={payload['note']}"
        )
        self._log_run_event(
            command="h2o-zero-span-capability",
            response=json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        )

    def _require_supported_h2o_zero_span_if_requested(
        self,
        points: Optional[List[CalibrationPoint]] = None,
    ) -> None:
        payload = self._h2o_zero_span_capability_payload(points)
        if payload["require_supported_capability"] and payload["status"] != "SUPPORTED":
            require_v1_h2o_zero_span_supported(
                self.cfg.get("coefficients", {}) if isinstance(self.cfg.get("coefficients", {}), dict) else {},
                requested=True,
                context="CalibrationRunner",
            )

    def run(self) -> None:
        self.log("Starting calibration...")
        self._log_data_quality_effective_config()
        self._log_postrun_corrected_delivery_effective_config()
        self._log_run_event(command="run-start", response="CalibrationRunner.run entered")
        completed_normally = False
        try:
            points_path = self.cfg["paths"]["points_excel"]
            wcfg = self.cfg.get("workflow", {})
            policy = wcfg.get("missing_pressure_policy", "require")
            carry_h2o = bool(wcfg.get("h2o_carry_forward", False))
            self.set_status("初始化：加载点表")
            self._emit_stage_event(current="初始化", wait_reason="加载点表")
            self.log(f"Loading points from {points_path}")
            points = load_points_from_excel(
                points_path,
                missing_pressure_policy=policy,
                carry_forward_h2o=carry_h2o,
            )
            points = self._filter_selected_temperatures(points)
            # Fixed process rule:
            # - sub-zero temperature groups run CO2 only
            # - 0C and above always run H2O before CO2
            # - run temperature groups from hot to cold by default to reduce
            #   humidity-generator icing risk during downward sweeps
            points = reorder_points(
                points,
                0.0,
                descending_temperatures=bool(wcfg.get("temperature_descending", True)),
            )
            self.log(f"Points loaded: {len(points)}")

            issues = validate_points(points, policy)
            if issues:
                self.log("Point validation failed:")
                for issue in issues:
                    self.log("  " + issue)
                return

            self._log_h2o_zero_span_capability(points)
            self._require_supported_h2o_zero_span_if_requested(points)

            self.set_status("初始化：传感器预检查")
            self._emit_stage_event(current="初始化", wait_reason="传感器预检查")
            self._sensor_precheck()
            self.set_status("初始化：配置设备")
            self._emit_stage_event(current="初始化", wait_reason="配置设备")
            self._configure_devices()
            self.set_status("初始化：恢复初始状态")
            self._emit_stage_event(current="初始化", wait_reason="恢复初始状态")
            self._startup_preflight_reset()
            self.set_status("初始化：启动压力预检查")
            self._emit_stage_event(current="初始化", wait_reason="启动压力预检查")
            self._startup_pressure_precheck(points)
            self.set_status("初始化：压力传感器单点校准")
            self._emit_stage_event(current="初始化", wait_reason="压力传感器单点校准")
            self._startup_pressure_sensor_calibration(points)
            self.set_status("初始化完成，准备进入点位流程")
            self._emit_stage_event(current="初始化完成", wait_reason="准备进入点位流程")
            self._run_points(points)

            if self.stop_event.is_set():
                self.log("Run stopped by request.")
                self._log_run_event(command="run-stop", response="stop_event observed after point loop")
                return

            if self.cfg.get("workflow", {}).get("collect_only", False):
                self.log("Collect-only mode enabled: coefficient fitting skipped.")
            else:
                self._flush_deferred_sample_exports(reason="before coefficient fitting")
                self._flush_deferred_point_exports(reason="before coefficient fitting")
                self._maybe_write_coefficients()
            self.log("Run finished.")
            self._log_run_event(command="run-finished", response="completed normally")
            completed_normally = True
        except Exception as exc:
            self.log(f"Run aborted: {exc}")
            self._log_run_event(command="run-aborted", error=exc)
        finally:
            self._finalize_temperature_calibration_outputs()
            self._log_run_event(command="run-cleanup", response="cleanup begin")
            self._cleanup()
            if completed_normally:
                self._maybe_run_postrun_corrected_delivery()

    def _run_points(self, points: List[CalibrationPoint]) -> None:
        groups = self._group_points_by_temperature(points)
        for idx, group in enumerate(groups):
            if self.stop_event.is_set():
                return
            self._check_pause()
            next_group = groups[idx + 1] if idx + 1 < len(groups) else None
            self._run_temperature_group(group, next_group=next_group)

    def _group_points_by_temperature(self, points: List[CalibrationPoint]) -> List[List[CalibrationPoint]]:
        groups: List[List[CalibrationPoint]] = []
        current: List[CalibrationPoint] = []
        current_temp: Optional[float] = None

        for point in points:
            temp = point.temp_chamber_c
            if current and current_temp is not None and abs(float(temp) - float(current_temp)) > 1e-9:
                groups.append(current)
                current = []
            current.append(point)
            current_temp = temp

        if current:
            groups.append(current)
        return groups

    def _group_h2o_points(self, points: List[CalibrationPoint]) -> List[List[CalibrationPoint]]:
        groups: List[List[CalibrationPoint]] = []
        current: List[CalibrationPoint] = []
        current_key: Optional[Tuple[Optional[float], Optional[float]]] = None

        for point in points:
            if not point.is_h2o_point:
                continue
            key = (self._as_float(point.hgen_temp_c), self._as_float(point.hgen_rh_pct))
            if current and key != current_key:
                groups.append(current)
                current = []
            current.append(point)
            current_key = key

        if current:
            groups.append(current)
        return groups

    def _h2o_source_groups_for_temperature(self, points: List[CalibrationPoint]) -> List[List[CalibrationPoint]]:
        groups = self._group_h2o_points([point for point in points if point.is_h2o_point])
        selected = self._normalize_selected_pressure_points(
            self.cfg.get("workflow", {}).get("selected_pressure_points")
        )
        if selected is None:
            return groups

        ambient_only = self._selected_pressure_points_is_ambient_only(selected)
        filtered_groups: List[List[CalibrationPoint]] = []
        for group in groups:
            matched = [
                point
                for point in group
                if self._point_matches_selected_pressure(point, selected=selected)
            ]
            if matched:
                if ambient_only:
                    matched = [self._ambient_pressure_reference_point(point) for point in matched]
                filtered_groups.append(matched)
                continue
            if ambient_only and group:
                # Ambient-only execution should still keep the H2O route even when
                # the point matrix only defines sealed source rows for that setpoint.
                filtered_groups.append([self._ambient_pressure_reference_point(group[0])])
        return filtered_groups

    def _co2_source_points(self, points: List[CalibrationPoint]) -> List[CalibrationPoint]:
        out: List[CalibrationPoint] = []
        seen: set[Tuple[float, str]] = set()
        skip_ppm = self._co2_skip_ppm_set()
        for point in points:
            if point.is_h2o_point:
                continue
            ppm = self._as_float(point.co2_ppm)
            if ppm is None:
                continue
            ppm_rounded = int(round(float(ppm)))
            if ppm_rounded in skip_ppm:
                continue
            group = _normalized_co2_group(getattr(point, "co2_group", ""))
            key = (float(ppm), group)
            if key in seen:
                continue
            seen.add(key)
            out.append(point)
        out.sort(
            key=lambda point: (
                float(self._as_float(point.co2_ppm) or 0.0),
                1 if _normalized_co2_group(getattr(point, "co2_group", "")) == "B" else 0,
            )
        )
        if self._selected_pressure_points_is_ambient_only():
            out = [self._ambient_pressure_reference_point(point) for point in out]
        if self._preserve_explicit_point_matrix():
            return out
        if not self._should_expand_co2_sources(points):
            return out

        source_template = out[0] if out else self._co2_synthesis_template(points)
        if source_template is None:
            return out
        real_by_ppm: Dict[int, CalibrationPoint] = {}
        for point in out:
            ppm = self._as_float(point.co2_ppm)
            if ppm is None:
                continue
            ppm_key = int(round(float(ppm)))
            real_by_ppm.setdefault(ppm_key, point)

        expanded: List[CalibrationPoint] = []
        for ppm in self._full_sweep_co2_ppm_values():
            if ppm in skip_ppm:
                continue
            real_point = real_by_ppm.get(ppm)
            if real_point is not None:
                expanded.append(real_point)
                continue
            expanded.append(self._synthesized_co2_source_point(source_template, ppm))
        if self._selected_pressure_points_is_ambient_only():
            return [self._ambient_pressure_reference_point(point) for point in expanded]
        return expanded

    def _co2_synthesis_template(self, points: List[CalibrationPoint]) -> Optional[CalibrationPoint]:
        if not points:
            return None
        template = self._normalize_pressure_point(points[0])
        return CalibrationPoint(
            index=template.index,
            temp_chamber_c=template.temp_chamber_c,
            co2_ppm=None,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=None,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
            co2_group=None,
        )

    def _should_expand_co2_sources(self, points: List[CalibrationPoint]) -> bool:
        if self._preserve_explicit_point_matrix():
            return False
        if not points:
            return False
        temp_c = self._as_float(points[0].temp_chamber_c)
        if temp_c is None:
            return False
        return any(abs(float(temp_c) - target) <= 1e-9 for target in self._FULL_SWEEP_CO2_TEMPS_C)

    def _full_sweep_co2_ppm_values(self) -> List[int]:
        return sorted(
            set(self._DEFAULT_CO2_GROUP_A_PPM).union(self._DEFAULT_CO2_GROUP_B_PPM)
        )

    def _preferred_co2_group_for_ppm(self, ppm: int) -> Optional[str]:
        valves_cfg = self.cfg.get("valves", {})
        ppm_key = str(int(ppm))
        map_a = valves_cfg.get("co2_map", {})
        map_b = valves_cfg.get("co2_map_group2", {})
        in_a = isinstance(map_a, dict) and ppm_key in map_a
        in_b = isinstance(map_b, dict) and ppm_key in map_b
        if in_a and not in_b:
            return None
        if in_b and not in_a:
            return "B"
        if ppm in self._DEFAULT_CO2_GROUP_B_PPM and ppm not in self._DEFAULT_CO2_GROUP_A_PPM:
            return "B"
        return None

    def _synthesized_co2_source_point(
        self,
        template: CalibrationPoint,
        ppm: int,
    ) -> CalibrationPoint:
        return CalibrationPoint(
            index=template.index,
            temp_chamber_c=template.temp_chamber_c,
            co2_ppm=float(ppm),
            hgen_temp_c=None,
            hgen_rh_pct=None,
            # Synthetic source points represent route conditioning only.
            # They must not inherit a sealed-pressure template and leak that
            # pressure into route-open trace titles or precondition gate rows.
            target_pressure_hpa=None,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
            co2_group=self._preferred_co2_group_for_ppm(ppm),
        )

        return out

    def _co2_skip_ppm_set(self) -> set[int]:
        workflow_cfg = self.cfg.get("workflow", {})
        raw = workflow_cfg.get("skip_co2_ppm", [])
        if not isinstance(raw, list):
            return set()
        out: set[int] = set()
        for item in raw:
            iv = self._as_int(item)
            if iv is not None:
                out.add(iv)
        return out

    def _preserve_explicit_point_matrix(self) -> bool:
        return bool(self._wf("workflow.preserve_explicit_point_matrix", False))

    @staticmethod
    def _normalized_pressure_hpa_value(value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        return float(value)

    @classmethod
    def _is_ambient_pressure_selection_value(cls, value: Any) -> bool:
        if not isinstance(value, str):
            return False
        compact = re.sub(r"\s+", "", value.strip().lower())
        return compact in {
            cls._AMBIENT_PRESSURE_TOKEN,
            "当前大气压",
            "大气压",
        }

    @classmethod
    def _decorate_pressure_point(
        cls,
        point: CalibrationPoint,
        *,
        pressure_mode: str = "",
        pressure_target_label: str = "",
        pressure_selection_token: str = "",
    ) -> CalibrationPoint:
        if pressure_mode:
            setattr(point, "_pressure_mode", str(pressure_mode))
        if pressure_target_label:
            setattr(point, "_pressure_target_label", str(pressure_target_label))
        if pressure_selection_token:
            setattr(point, "_pressure_selection_token", str(pressure_selection_token))
        return point

    def _pressure_mode_for_point(self, point: Optional[CalibrationPoint]) -> str:
        if point is None:
            return ""
        mode = str(getattr(point, "_pressure_mode", "") or "").strip()
        if mode:
            return mode
        token = getattr(point, "_pressure_selection_token", "")
        if self._is_ambient_pressure_selection_value(token):
            return "ambient_open"
        if self._as_float(getattr(point, "target_pressure_hpa", None)) is not None:
            return "sealed_controlled"
        return ""

    def _is_ambient_pressure_point(self, point: Any) -> bool:
        if point is None:
            return False
        if isinstance(point, CalibrationPoint):
            return self._pressure_mode_for_point(point) == "ambient_open"
        return self._is_ambient_pressure_selection_value(point)

    def _pressure_target_label(self, point: Optional[CalibrationPoint]) -> Optional[str]:
        if point is None:
            return None
        explicit_label = str(getattr(point, "_pressure_target_label", "") or "").strip()
        if explicit_label:
            return explicit_label
        pressure_hpa = self._as_float(getattr(point, "target_pressure_hpa", None))
        if pressure_hpa is None:
            return None
        return f"{int(round(float(pressure_hpa)))}hPa"

    def _pressure_metadata_for_point(self, point: Optional[CalibrationPoint]) -> Tuple[str, Optional[str]]:
        return self._pressure_mode_for_point(point), self._pressure_target_label(point)

    def _ambient_pressure_reference_point(self, template: CalibrationPoint) -> CalibrationPoint:
        point = CalibrationPoint(
            index=int(getattr(template, "index", 0) or 0),
            temp_chamber_c=float(getattr(template, "temp_chamber_c")),
            co2_ppm=self._as_float(getattr(template, "co2_ppm", None)),
            hgen_temp_c=self._as_float(getattr(template, "hgen_temp_c", None)),
            hgen_rh_pct=self._as_float(getattr(template, "hgen_rh_pct", None)),
            target_pressure_hpa=None,
            dewpoint_c=self._as_float(getattr(template, "dewpoint_c", None)),
            h2o_mmol=self._as_float(getattr(template, "h2o_mmol", None)),
            raw_h2o=getattr(template, "raw_h2o", None),
            co2_group=getattr(template, "co2_group", None),
        )
        return self._decorate_pressure_point(
            point,
            pressure_mode="ambient_open",
            pressure_target_label=self._AMBIENT_PRESSURE_LABEL,
            pressure_selection_token=self._AMBIENT_PRESSURE_TOKEN,
        )

    def _copy_pressure_metadata(self, point: CalibrationPoint, template: Optional[CalibrationPoint] = None) -> CalibrationPoint:
        source = template or point
        mode, label = self._pressure_metadata_for_point(source)
        token = str(getattr(source, "_pressure_selection_token", "") or "").strip()
        if mode or label or token:
            return self._decorate_pressure_point(
                point,
                pressure_mode=mode,
                pressure_target_label=label or "",
                pressure_selection_token=token,
            )
        return point

    def _split_pressure_execution_points(
        self,
        points: List[CalibrationPoint],
    ) -> Tuple[List[CalibrationPoint], List[CalibrationPoint]]:
        ambient_open_refs: List[CalibrationPoint] = []
        sealed_control_refs: List[CalibrationPoint] = []
        for point in points:
            if self._is_ambient_pressure_point(point):
                ambient_open_refs.append(point)
            else:
                sealed_control_refs.append(point)
        return ambient_open_refs, sealed_control_refs

    def _sealed_pressure_points_include_target(
        self,
        points: Optional[List[CalibrationPoint]],
        target_hpa: float,
    ) -> bool:
        target_int = int(round(float(target_hpa)))
        for point in points or []:
            if self._is_ambient_pressure_point(point):
                continue
            pressure_hpa = self._as_float(getattr(point, "target_pressure_hpa", None))
            if pressure_hpa is None:
                continue
            if int(round(float(pressure_hpa))) == target_int:
                return True
        return False

    def _route_requires_preseal_topoff(self, points: Optional[List[CalibrationPoint]]) -> bool:
        if not points:
            return True
        return self._sealed_pressure_points_include_target(points, self._PRESEAL_TOPOFF_TARGET_HPA)

    def _pressurize_route_for_sealed_points(
        self,
        point: CalibrationPoint,
        *,
        route: str,
        sealed_control_refs: Optional[List[CalibrationPoint]] = None,
    ) -> bool:
        require_preseal_topoff = self._route_requires_preseal_topoff(sealed_control_refs or [point])
        route_label = str(route or ("h2o" if point.is_h2o_point else "co2")).strip().upper()
        if require_preseal_topoff:
            self.log(
                f"{route_label} sealed pressure set includes "
                f"{int(round(float(self._PRESEAL_TOPOFF_TARGET_HPA)))} hPa; "
                "keep preseal top-off before sealing"
            )
        else:
            self.log(
                f"{route_label} sealed pressure set excludes "
                f"{int(round(float(self._PRESEAL_TOPOFF_TARGET_HPA)))} hPa; "
                "skip preseal top-off and seal immediately after vent off"
            )

        previous = bool(getattr(self, "_active_route_requires_preseal_topoff", True))
        self._active_route_requires_preseal_topoff = require_preseal_topoff
        try:
            return self._pressurize_and_hold(point, route=route)
        finally:
            self._active_route_requires_preseal_topoff = previous

    def _normalize_pressure_point(self, point: CalibrationPoint) -> CalibrationPoint:
        pressure = self._as_float(point.target_pressure_hpa)
        normalized = self._normalized_pressure_hpa_value(pressure)
        if pressure is None or normalized is None or abs(float(pressure) - float(normalized)) <= 1e-9:
            return point
        return CalibrationPoint(
            index=point.index,
            temp_chamber_c=point.temp_chamber_c,
            co2_ppm=point.co2_ppm,
            hgen_temp_c=point.hgen_temp_c,
            hgen_rh_pct=point.hgen_rh_pct,
            target_pressure_hpa=normalized,
            dewpoint_c=point.dewpoint_c,
            h2o_mmol=point.h2o_mmol,
            raw_h2o=point.raw_h2o,
            co2_group=getattr(point, "co2_group", None),
        )

    def _pressure_reference_points(self, points: List[CalibrationPoint]) -> List[CalibrationPoint]:
        if self._preserve_explicit_point_matrix():
            out: List[CalibrationPoint] = []
            for point in points:
                normalized_point = self._normalize_pressure_point(point)
                pressure = self._as_float(normalized_point.target_pressure_hpa)
                if pressure is None:
                    continue
                out.append(normalized_point)
            return out
        out: List[CalibrationPoint] = []
        seen: set[float] = set()
        for point in points:
            normalized_point = self._normalize_pressure_point(point)
            pressure = self._as_float(normalized_point.target_pressure_hpa)
            if pressure is None:
                continue
            key = float(pressure)
            if key in seen:
                continue
            seen.add(key)
            out.append(normalized_point)
        return out

    def _co2_pressure_points_for_temperature(self, points: List[CalibrationPoint]) -> List[CalibrationPoint]:
        pressure_points = self._pressure_reference_points(points)
        pressure_points.sort(
            key=lambda point: float(self._as_float(point.target_pressure_hpa) or 0.0),
            reverse=True,
        )
        ambient_template = next((point for point in points if point.co2_ppm is not None), points[0] if points else None)
        return self._filter_pressure_points_by_selection(
            pressure_points,
            route_label="co2",
            ambient_template=ambient_template,
        )

    def _h2o_pressure_points_for_temperature(self, points: List[CalibrationPoint]) -> List[CalibrationPoint]:
        pressure_points = self._pressure_reference_points(points)
        pressure_points.sort(
            key=lambda point: float(self._as_float(point.target_pressure_hpa) or 0.0),
            reverse=True,
        )
        ambient_template = next((point for point in points if point.is_h2o_point), points[0] if points else None)
        return self._filter_pressure_points_by_selection(
            pressure_points,
            route_label="h2o",
            ambient_template=ambient_template,
        )

    def _build_co2_pressure_point(
        self,
        source_point: CalibrationPoint,
        pressure_point: CalibrationPoint,
    ) -> CalibrationPoint:
        built = self._normalize_pressure_point(
            CalibrationPoint(
                index=source_point.index if self._is_ambient_pressure_point(pressure_point) else pressure_point.index,
                temp_chamber_c=source_point.temp_chamber_c,
                co2_ppm=source_point.co2_ppm,
                hgen_temp_c=None,
                hgen_rh_pct=None,
                target_pressure_hpa=None if self._is_ambient_pressure_point(pressure_point) else pressure_point.target_pressure_hpa,
                dewpoint_c=None,
                h2o_mmol=None,
                raw_h2o=None,
                co2_group=source_point.co2_group,
            )
        )
        return self._copy_pressure_metadata(built, template=pressure_point)

    def _build_h2o_pressure_point(
        self,
        source_point: CalibrationPoint,
        pressure_point: CalibrationPoint,
    ) -> CalibrationPoint:
        built = self._normalize_pressure_point(
            CalibrationPoint(
                index=source_point.index if self._is_ambient_pressure_point(pressure_point) else pressure_point.index,
                temp_chamber_c=source_point.temp_chamber_c,
                co2_ppm=source_point.co2_ppm,
                hgen_temp_c=self._as_float(getattr(source_point, "hgen_temp_c", None)),
                hgen_rh_pct=self._as_float(getattr(source_point, "hgen_rh_pct", None)),
                target_pressure_hpa=None if self._is_ambient_pressure_point(pressure_point) else pressure_point.target_pressure_hpa,
                dewpoint_c=self._as_float(getattr(source_point, "dewpoint_c", None)),
                h2o_mmol=self._as_float(getattr(source_point, "h2o_mmol", None)),
                raw_h2o=getattr(source_point, "raw_h2o", None),
                co2_group=getattr(source_point, "co2_group", None),
            )
        )
        return self._copy_pressure_metadata(built, template=pressure_point)

    def _co2_point_tag(self, point: CalibrationPoint) -> str:
        ppm = self._as_float(point.co2_ppm)
        group = str(getattr(point, "co2_group", "") or "").strip().upper()
        ppm_text = f"{int(round(ppm))}ppm" if ppm is not None else "unknownppm"
        if self._is_ambient_pressure_point(point):
            pressure_text = self._AMBIENT_PRESSURE_TOKEN
        else:
            pressure = self._as_float(point.target_pressure_hpa)
            pressure_text = f"{int(round(pressure))}hpa" if pressure is not None else "unknownhpa"
        group_text = f"group{group.lower()}" if group else "groupa"
        return f"co2_{group_text}_{ppm_text}_{pressure_text}"

    def _h2o_point_tag(self, point: CalibrationPoint) -> str:
        hgen_temp = self._as_float(point.hgen_temp_c)
        hgen_rh = self._as_float(point.hgen_rh_pct)
        temp_text = f"{int(round(hgen_temp))}c" if hgen_temp is not None else "unknownc"
        rh_text = f"{int(round(hgen_rh))}rh" if hgen_rh is not None else "unknownrh"
        if self._is_ambient_pressure_point(point):
            pressure_text = self._AMBIENT_PRESSURE_TOKEN
        else:
            pressure = self._as_float(point.target_pressure_hpa)
            pressure_text = f"{int(round(pressure))}hpa" if pressure is not None else "unknownhpa"
        return f"h2o_{temp_text}_{rh_text}_{pressure_text}"

    def _route_entry_context_for_h2o_group(
        self,
        points: List[CalibrationPoint],
        *,
        pressure_points: Optional[List[CalibrationPoint]] = None,
    ) -> Optional[Dict[str, Any]]:
        if not points:
            return None
        lead = points[0]
        effective_pressure_points = pressure_points or points
        sample_point = self._build_h2o_pressure_point(lead, effective_pressure_points[0])
        return {
            "point": lead,
            "phase": "h2o",
            "point_tag": self._h2o_point_tag(sample_point),
            "open_valves": self._h2o_open_valves(lead),
        }

    def _route_entry_context_for_co2_source(
        self,
        point: CalibrationPoint,
        *,
        pressure_points: Optional[List[CalibrationPoint]] = None,
    ) -> Dict[str, Any]:
        pressure_refs = pressure_points or [point]
        sample_point = self._build_co2_pressure_point(point, pressure_refs[0])
        return {
            "point": point,
            "phase": "co2",
            "point_tag": self._co2_point_tag(sample_point),
            "open_valves": self._co2_open_valves(point, include_total_valve=True),
        }

    def _run_temperature_group(
        self,
        points: List[CalibrationPoint],
        next_group: Optional[List[CalibrationPoint]] = None,
    ) -> None:
        if not points:
            return

        self._attempt_reenable_disabled_analyzers()

        temp = points[0].temp_chamber_c
        is_subzero = temp is not None and float(temp) < 0.0
        route_mode = self._route_mode()
        skip_h2o = route_mode == "co2_only" or bool(self.cfg.get("workflow", {}).get("skip_h2o", False))
        skip_co2 = route_mode == "h2o_only"
        h2o_groups: List[List[CalibrationPoint]] = []
        h2o_pressure_points: List[CalibrationPoint] = []
        if skip_h2o:
            self.log(f"Temperature group {temp}C: workflow skip_h2o enabled, skip H2O route")
        elif is_subzero:
            self.log(f"Temperature group {temp}C: sub-zero, skip H2O route")
        else:
            h2o_pressure_points = self._h2o_pressure_points_for_temperature(points)
            h2o_groups = self._h2o_source_groups_for_temperature(points)

        gas_sources: List[CalibrationPoint] = []
        pressure_points: List[CalibrationPoint] = []
        if skip_co2:
            self.log(f"Temperature group {temp}C: workflow route_mode=h2o_only, skip CO2 route")
        else:
            # Pressure selection chooses which pressure references to execute.
            # CO2 source ppm steps still come from the whole temperature group;
            # otherwise ambient-only selection can erase the route when the point
            # table has no explicit ambient CO2 source rows.
            gas_sources = self._co2_source_points(points)
            pressure_points = self._co2_pressure_points_for_temperature(points)
        if gas_sources and pressure_points:
            pressure_text = ", ".join(self._pressure_target_label(p) or "--" for p in pressure_points)
            source_text = ", ".join(
                str(int(round(float(p.co2_ppm or 0)))) for p in gas_sources if p.co2_ppm is not None
            )
            self.log(
                f"Temperature group {temp}C CO2 sweep: sources=[{source_text}] "
                f"pressures=[{pressure_text}]"
            )
        self._precondition_next_temperature_humidity(next_group)
        route_plan: List[Dict[str, Any]] = []
        for group in h2o_groups:
            context = self._route_entry_context_for_h2o_group(group, pressure_points=h2o_pressure_points)
            if context:
                route_plan.append(
                    {
                        "kind": "h2o_group",
                        "group": group,
                        "pressure_points": h2o_pressure_points,
                        "route_context": context,
                    }
                )
        for point in gas_sources:
            route_plan.append(
                {
                    "kind": "co2_source",
                    "point": point,
                    "pressure_points": pressure_points,
                    "route_context": self._route_entry_context_for_co2_source(
                        point,
                        pressure_points=pressure_points,
                    ),
                }
            )

        for idx, route_item in enumerate(route_plan):
            if self.stop_event.is_set():
                return
            self._check_pause()
            next_route_context = None
            if idx + 1 < len(route_plan):
                next_route_context = route_plan[idx + 1].get("route_context")
            if route_item["kind"] == "h2o_group":
                self._run_h2o_group(
                    route_item["group"],
                    pressure_points=route_item["pressure_points"],
                    next_route_context=next_route_context,
                )
                continue
            point = route_item["point"]
            self.set_status(f"CO2 row {point.index}")
            self._run_co2_point(
                point,
                pressure_points=route_item["pressure_points"],
                next_route_context=next_route_context,
            )

    def _next_temperature_h2o_lead(
        self,
        next_group: Optional[List[CalibrationPoint]],
    ) -> Optional[CalibrationPoint]:
        if not next_group:
            return None
        temp = self._as_float(next_group[0].temp_chamber_c)
        if temp is not None and temp < 0.0:
            return None
        groups = self._h2o_source_groups_for_temperature(next_group)
        if not groups or not groups[0]:
            return None
        return groups[0][0]

    def _next_group_temperature_target(
        self,
        next_group: Optional[List[CalibrationPoint]],
    ) -> Optional[float]:
        if not next_group:
            return None
        return self._as_float(next_group[0].temp_chamber_c)

    def _precondition_next_temperature_chamber(self, target_c: Optional[float]) -> None:
        if target_c is None:
            return
        if not bool(self._wf("workflow.stability.temperature.precondition_next_group_enabled", False)):
            return

        chamber = self.devices.get("temp_chamber")
        if not chamber:
            return

        command_offset_c = float(self._wf("workflow.stability.temperature.command_offset_c", 0.0) or 0.0)
        command_target_c = float(target_c) + command_offset_c
        self.log(
            "Preconditioning temperature chamber for next temperature group: "
            f"target={target_c:.2f}C command={command_target_c:.2f}C"
        )
        try:
            chamber.set_temp_c(command_target_c)
            chamber.start()
        except Exception as exc:
            self.log(f"Next-group chamber precondition failed: {exc}")

    def _precondition_next_temperature_humidity(
        self,
        next_group: Optional[List[CalibrationPoint]],
    ) -> None:
        if self._route_mode() != "h2o_then_co2":
            return
        if not bool(
            self._wf(
                "workflow.stability.humidity_generator.precondition_next_group_enabled",
                True,
            )
        ):
            return
        next_lead = self._next_temperature_h2o_lead(next_group)
        if not next_lead:
            return
        self.log(
            "Preconditioning humidity generator for next temperature group: "
            f"chamber={next_lead.temp_chamber_c}C "
            f"hgen={next_lead.hgen_temp_c}C/{next_lead.hgen_rh_pct}%"
        )
        self._prepare_humidity_generator(next_lead)

    def _route_mode(self) -> str:
        workflow_cfg = self.cfg.get("workflow", {})
        route_mode = str(workflow_cfg.get("route_mode", "") or "").strip().lower()
        if route_mode in {"h2o_only", "co2_only", "h2o_then_co2"}:
            return route_mode
        if bool(workflow_cfg.get("skip_h2o", False)):
            return "co2_only"
        return "h2o_then_co2"

    def _filter_selected_temperatures(self, points: List[CalibrationPoint]) -> List[CalibrationPoint]:
        workflow_cfg = self.cfg.get("workflow", {})
        raw = workflow_cfg.get("selected_temps_c")
        if raw in (None, "", []):
            return points
        if not isinstance(raw, list):
            raw = [raw]

        selected: List[float] = []
        for item in raw:
            fv = self._as_float(item)
            if fv is not None:
                selected.append(float(fv))
        if not selected:
            self.log("Temperature filter requested but no valid temperature values parsed; keep all points")
            return points

        filtered = [
            p for p in points
            if p.temp_chamber_c is not None
            and any(abs(float(p.temp_chamber_c) - target) < 1e-9 for target in selected)
        ]
        selected_text = ",".join(f"{t:g}" for t in selected)
        self.log(f"Temperature filter: temps=[{selected_text}]C -> {len(filtered)}/{len(points)} points")
        return filtered

    def _normalize_selected_pressure_points(self, raw: Any) -> Optional[List[Any]]:
        if raw in (None, "", []):
            return None
        if not isinstance(raw, list):
            raw = [raw]

        include_ambient = False
        selected_numeric: List[int] = []
        invalid: List[Any] = []
        allowed = set(self._STANDARD_PRESSURE_POINTS_HPA)
        for item in raw:
            if self._is_ambient_pressure_selection_value(item):
                include_ambient = True
                continue
            iv = self._as_int(item)
            if iv is None:
                invalid.append(item)
                continue
            value = int(iv)
            if value not in allowed:
                invalid.append(value)
                continue
            if value not in selected_numeric:
                selected_numeric.append(value)

        if invalid:
            allowed_text = ", ".join(
                [self._AMBIENT_PRESSURE_TOKEN] + [str(value) for value in self._STANDARD_PRESSURE_POINTS_HPA]
            )
            invalid_text = ", ".join(str(value) for value in invalid)
            raise ValueError(
                f"invalid selected_pressure_points: [{invalid_text}]; "
                f"allowed values: [{allowed_text}]"
            )
        normalized: List[Any] = []
        if include_ambient:
            normalized.append(self._AMBIENT_PRESSURE_TOKEN)
        normalized.extend([value for value in self._STANDARD_PRESSURE_POINTS_HPA if value in selected_numeric])
        if not normalized:
            raise ValueError("no valid pressure points selected after filtering")
        return normalized

    def _selected_pressure_points_is_ambient_only(
        self,
        selected: Optional[List[Any]] = None,
    ) -> bool:
        normalized = (
            selected
            if selected is not None
            else self._normalize_selected_pressure_points(
                self.cfg.get("workflow", {}).get("selected_pressure_points")
            )
        )
        if normalized is None:
            return False
        include_ambient = any(self._is_ambient_pressure_selection_value(value) for value in normalized)
        include_numeric = any(not self._is_ambient_pressure_selection_value(value) for value in normalized)
        return include_ambient and not include_numeric

    def _point_matches_selected_pressure(
        self,
        point: CalibrationPoint,
        *,
        selected: Optional[List[Any]] = None,
    ) -> bool:
        normalized = (
            selected
            if selected is not None
            else self._normalize_selected_pressure_points(
                self.cfg.get("workflow", {}).get("selected_pressure_points")
            )
        )
        if normalized is None:
            return True

        include_ambient = any(self._is_ambient_pressure_selection_value(value) for value in normalized)
        selected_numeric = {
            int(value)
            for value in normalized
            if not self._is_ambient_pressure_selection_value(value)
        }
        normalized_point = self._normalize_pressure_point(point)
        pressure_hpa = self._as_float(getattr(normalized_point, "target_pressure_hpa", None))
        if pressure_hpa is None:
            return include_ambient
        return int(round(float(pressure_hpa))) in selected_numeric

    def _filter_execution_points_by_selected_pressure(
        self,
        points: List[CalibrationPoint],
    ) -> List[CalibrationPoint]:
        selected = self._normalize_selected_pressure_points(
            self.cfg.get("workflow", {}).get("selected_pressure_points")
        )
        if selected is None:
            return list(points)

        ambient_only = self._selected_pressure_points_is_ambient_only(selected)
        filtered: List[CalibrationPoint] = []
        for point in points:
            if not self._point_matches_selected_pressure(point, selected=selected):
                continue
            filtered.append(self._ambient_pressure_reference_point(point) if ambient_only else point)
        return filtered

    def _filter_pressure_points_by_selection(
        self,
        pressure_points: List[CalibrationPoint],
        *,
        route_label: str,
        ambient_template: Optional[CalibrationPoint] = None,
    ) -> List[CalibrationPoint]:
        default_values = [
            int(round(float(point.target_pressure_hpa or 0)))
            for point in pressure_points
            if self._as_float(point.target_pressure_hpa) is not None
        ]
        selected = self._normalize_selected_pressure_points(
            self.cfg.get("workflow", {}).get("selected_pressure_points")
        )
        signature = (str(route_label), tuple(selected or ()), tuple(default_values))
        if selected is None:
            if signature not in self._pressure_selection_log_signatures:
                self.log(
                    "pressure point selection not set, using default pressure points: "
                    f"{default_values}"
                )
                self._pressure_selection_log_signatures.add(signature)
            return pressure_points

        if signature not in self._pressure_selection_log_signatures:
            self.log(f"pressure point selection requested: {selected}")
            self.log(f"pressure points normalized to execution order: {selected}")
            self._pressure_selection_log_signatures.add(signature)

        filtered: List[CalibrationPoint] = []
        selected_numeric = {
            int(value)
            for value in selected
            if not self._is_ambient_pressure_selection_value(value)
        }
        include_ambient = any(self._is_ambient_pressure_selection_value(value) for value in selected)
        if include_ambient:
            template = ambient_template or (pressure_points[0] if pressure_points else None)
            if template is not None:
                filtered.append(self._ambient_pressure_reference_point(template))
        for pressure_hpa in self._STANDARD_PRESSURE_POINTS_HPA:
            if pressure_hpa not in selected_numeric:
                continue
            for point in pressure_points:
                value = self._as_float(point.target_pressure_hpa)
                if value is None:
                    continue
                if int(round(float(value))) == pressure_hpa:
                    filtered.append(point)

        if not filtered:
            available_text = ", ".join(str(value) for value in default_values) or "none"
            selected_text = ", ".join(str(value) for value in selected)
            raise ValueError(
                "no valid pressure points selected after filtering; "
                f"requested=[{selected_text}] available=[{available_text}]"
            )
        return filtered

    @staticmethod
    def _split_sensor_lines(raw: str) -> List[str]:
        text = (raw or "").replace("\r", "\n")
        return [line.strip() for line in text.split("\n") if line.strip()]

    def _sensor_precheck(self) -> None:
        pcfg = self.cfg.get("workflow", {}).get("sensor_precheck", {})
        if not pcfg or not pcfg.get("enabled", False):
            return

        analyzers = self._gas_analyzers()
        if not analyzers:
            self.log("Sensor precheck skipped: gas analyzer unavailable")
            return

        label, ga, analyzer_cfg = analyzers[0]
        mode = 2
        gas_cfg_default = self.cfg.get("devices", {}).get("gas_analyzer", {})
        active_send = bool(
            pcfg.get(
                "active_send",
                analyzer_cfg.get("active_send", gas_cfg_default.get("active_send", True)),
            )
        )
        ftd_hz = int(pcfg.get("ftd_hz", 1))
        avg_co2 = int(pcfg.get("average_co2", 1))
        avg_h2o = int(pcfg.get("average_h2o", 1))
        avg_filter = int(pcfg.get("average_filter", 49))
        duration_s = float(pcfg.get("duration_s", 8.0))
        poll_s = float(pcfg.get("poll_s", 0.2))
        min_valid = int(pcfg.get("min_valid_frames", 3))
        strict = bool(pcfg.get("strict", True))

        self.log(
            f"Sensor precheck start ({label}): mode={mode} active_send={active_send} "
            f"ftd={ftd_hz}Hz avg=({avg_co2},{avg_h2o}) filter={avg_filter}"
        )

        self._configure_gas_analyzer(
            ga,
            mode=mode,
            active_send=active_send,
            ftd_hz=ftd_hz,
            avg_co2=avg_co2,
            avg_h2o=avg_h2o,
            avg_filter=avg_filter,
            warning_phase="startup",
        )

        valid = 0
        last_valid = ""
        deadline = time.time() + max(0.5, duration_s)

        while time.time() < deadline:
            if self.stop_event.is_set():
                return
            self._check_pause()
            line, parsed = self._read_sensor_parsed(ga, frame_acceptance_mode="strict")
            if parsed:
                valid += 1
                last_valid = line
                if valid >= min_valid:
                    self.log(f"Sensor precheck passed: valid_frames={valid}")
                    return

            if poll_s > 0:
                time.sleep(poll_s)

        msg = f"Sensor precheck failed: valid_frames={valid}/{min_valid}"
        if last_valid:
            msg += f" last={last_valid[:80]}"
        if strict:
            raise RuntimeError(msg)
        self.log(msg)

    def _cleanup(self) -> None:
        self._pending_route_handoff = None
        self._sample_export_deferral_request = None
        self._clear_preseal_pressure_control_ready_state(reason="runner_cleanup")
        self._stop_pressure_transition_fast_signal_context(reason="runner cleanup")
        try:
            self._flush_deferred_sample_exports(reason="runner cleanup")
        except Exception as exc:
            self.log(f"Deferred sample export flush failed during cleanup: {exc}")
        try:
            self._flush_deferred_point_exports(reason="runner cleanup")
        except Exception as exc:
            self.log(f"Deferred heavy export flush failed during cleanup: {exc}")
        self._restore_baseline_after_run()
        self._flush_sensor_read_reject_states(force=True)

        seen = set()
        close_targets: List[Any] = []
        for dev in self.devices.values():
            if isinstance(dev, dict):
                candidates = list(dev.values())
            elif isinstance(dev, (list, tuple, set)):
                candidates = list(dev)
            else:
                candidates = [dev]
            for item in candidates:
                if not hasattr(item, "close"):
                    continue
                obj_id = id(item)
                if obj_id in seen:
                    continue
                seen.add(obj_id)
                close_targets.append(item)

        for dev in close_targets:
            try:
                dev.close()
            except Exception:
                pass
        try:
            self.logger.close()
        except Exception:
            pass

    def _restore_baseline_after_run(self) -> None:
        if not bool(self._wf("workflow.restore_baseline_on_finish", True)):
            self._log_run_event(command="baseline-restore-skip", response="disabled by config")
            return

        try:
            self._emit_stage_event(current="流程结束，恢复基线", wait_reason="恢复基线")
        except Exception:
            pass

        self._log_run_event(command="baseline-restore-begin", response="perform_safe_stop")
        try:
            result = _perform_safe_stop(self.devices, log_fn=self.log, cfg=self.cfg)
        except Exception as exc:
            self.log(f"Baseline restore failed: {exc}")
            self._log_run_event(command="baseline-restore-failed", error=exc)
            return

        summary: Dict[str, Any] = {}
        for key in ("pace_pressure_hpa", "gauge_pressure_hpa", "relay_states", "relay8_states"):
            if key in result:
                summary[key] = result[key]
        chamber = result.get("chamber")
        if isinstance(chamber, dict):
            summary["chamber"] = {
                "temp_c": chamber.get("temp_c"),
                "rh_pct": chamber.get("rh_pct"),
                "run_state": chamber.get("run_state"),
            }
        hgen_stop_check = result.get("hgen_stop_check")
        if isinstance(hgen_stop_check, dict):
            summary["hgen_stop_check"] = {
                "ok": hgen_stop_check.get("ok"),
                "flow_lpm": hgen_stop_check.get("flow_lpm"),
            }
        hgen_safe_stop = result.get("hgen_safe_stop")
        if isinstance(hgen_safe_stop, dict):
            summary["hgen_safe_stop"] = {
                "flow_off": hgen_safe_stop.get("flow_off"),
                "ctrl_off": hgen_safe_stop.get("ctrl_off"),
                "cool_off": hgen_safe_stop.get("cool_off"),
                "heat_off": hgen_safe_stop.get("heat_off"),
            }
        if "safe_stop_verified" in result:
            summary["safe_stop_verified"] = result.get("safe_stop_verified")
        issues = list(result.get("safe_stop_issues") or [])
        if issues:
            summary["safe_stop_issues"] = issues
            self.log(f"Baseline restore incomplete: {', '.join(issues)}")
        self._log_run_event(
            command="baseline-restore-done",
            response=json.dumps(summary, ensure_ascii=False, separators=(",", ":"), default=str),
        )

    def _configure_devices(self) -> None:
        pace = self.devices.get("pace")
        if pace:
            pace.set_units_hpa()
            soft_control_enabled = bool(self._wf("workflow.pressure.soft_control_enabled", False))
            if soft_control_enabled:
                self._configure_pressure_soft_control(pace)
            else:
                if not self._soft_pressure_control_cfg_logged:
                    self._soft_pressure_control_cfg_logged = True
                    self.log(
                        "Pressure soft-control disabled for this V1 run: "
                        "soft_control_enabled=False, "
                        f"configured_linear_slew_hpa_per_s={float(self._wf('workflow.pressure.soft_control_linear_slew_hpa_per_s', 10.0)):g} "
                        "is not active on the real control path"
                    )
                try:
                    set_mode_active = getattr(pace, "set_output_mode_active", None)
                    if callable(set_mode_active):
                        set_mode_active()
                except Exception as exc:
                    self.log(f"Pressure controller set active mode failed: {exc}")
            pace.set_in_limits(
                self.cfg["devices"]["pressure_controller"]["in_limits_pct"],
                self.cfg["devices"]["pressure_controller"]["in_limits_time_s"],
            )

        gas_cfg_default = self.cfg.get("devices", {}).get("gas_analyzer", {})
        analyzers = self._gas_analyzers()
        for label, ga, cfg in analyzers:
            active_send = bool(cfg.get("active_send", gas_cfg_default.get("active_send", False)))
            ftd_hz = int(cfg.get("ftd_hz", gas_cfg_default.get("ftd_hz", 1)))
            avg_co2 = int(cfg.get("average_co2", gas_cfg_default.get("average_co2", 1)))
            avg_h2o = int(cfg.get("average_h2o", gas_cfg_default.get("average_h2o", 1)))
            avg_filter = int(cfg.get("average_filter", gas_cfg_default.get("average_filter", 49)))

            # V2 workflow requires mode2 for acquisition and parsing.
            try:
                self._configure_gas_analyzer(
                    ga,
                    label=label,
                    mode=2,
                    active_send=active_send,
                    ftd_hz=ftd_hz,
                    avg_co2=avg_co2,
                    avg_h2o=avg_h2o,
                    avg_filter=avg_filter,
                    warning_phase="startup",
                )
                self._disabled_analyzers.discard(label)
                self._disabled_analyzer_reasons.pop(label, None)
                self._disabled_analyzer_last_reprobe_ts.pop(label, None)
            except Exception as exc:
                self.log(f"Analyzer startup config failed: {label} err={exc}")
                self._disable_analyzers([label], reason="startup_mode2_verify_failed")
                self._disabled_analyzer_last_reprobe_ts[label] = time.time()

        if analyzers and not self._active_gas_analyzers():
            raise RuntimeError("No gas analyzers available after startup configuration")

    @staticmethod
    def _summarize_sensor_line(line: Any, limit: int = 120) -> str:
        text = str(line or "").replace("\x00", " ").strip()
        text = re.sub(r"\s+", " ", text)
        return text[:limit]

    @staticmethod
    def _config_ack_ok(result: Any) -> bool:
        return result is not False

    def _read_mode2_frame(
        self,
        ga: Any,
        *,
        prefer_stream: bool,
        ftd_hz: int,
        attempts: int = 3,
        retry_delay_s: float = 0.1,
        require_usable: bool = False,
        frame_acceptance_mode: str = "strict",
    ) -> tuple[str, Optional[Dict[str, Any]]]:
        last_line = ""
        last_parsed: Optional[Dict[str, Any]] = None
        read_latest = getattr(ga, "read_latest_data", None)
        parse_mode2 = getattr(ga, "parse_line_mode2", None)
        drain_lines = getattr(ga, "_drain_stream_lines", None)
        drain_s = max(0.2, 2.0 / max(1, int(ftd_hz)))

        for idx in range(max(1, int(attempts))):
            if prefer_stream and callable(drain_lines):
                lines = drain_lines(drain_s=drain_s, read_timeout_s=0.05)
                if lines:
                    last_line = str(lines[-1] or "")
                    for candidate in reversed(lines):
                        text = str(candidate or "")
                        parsed = parse_mode2(text) if callable(parse_mode2) else self._parse_sensor_line(ga, text)
                        if parsed:
                            if require_usable:
                                if frame_acceptance_mode == "startup_mode2":
                                    usable, _status = self._assess_mode2_frame_for_startup(parsed)
                                else:
                                    usable, _status = self._assess_analyzer_frame(parsed)
                                if not usable:
                                    last_line = text
                                    last_parsed = None
                                    continue
                            return text, parsed
                    last_parsed = None
                if idx + 1 < max(1, int(attempts)) and retry_delay_s > 0:
                    time.sleep(max(0.01, float(retry_delay_s)))
                continue
            elif callable(read_latest):
                try:
                    line = read_latest(
                        prefer_stream=prefer_stream,
                        drain_s=drain_s,
                        allow_passive_fallback=False,
                    )
                except TypeError:
                    line = read_latest()
            else:
                line = ga.read_data_passive()
            last_line = str(line or "")
            if callable(parse_mode2):
                last_parsed = parse_mode2(last_line)
            else:
                last_parsed = self._parse_sensor_line(ga, last_line)
            if last_parsed:
                if require_usable:
                    if frame_acceptance_mode == "startup_mode2":
                        usable, _status = self._assess_mode2_frame_for_startup(last_parsed)
                    else:
                        usable, _status = self._assess_analyzer_frame(last_parsed)
                    if not usable:
                        last_parsed = None
                    else:
                        return last_line, last_parsed
                else:
                    return last_line, last_parsed
            if idx + 1 < max(1, int(attempts)) and retry_delay_s > 0:
                time.sleep(max(0.01, float(retry_delay_s)))

        return last_line, last_parsed

    def _verify_startup_mode2_ready(
        self,
        ga: Any,
        *,
        prefer_stream: bool,
        ftd_hz: int,
        attempts: int,
        retry_delay_s: float,
        consecutive_frames: int,
    ) -> tuple[str, Optional[Dict[str, Any]]]:
        need = max(1, int(consecutive_frames))
        total_attempts = max(need, int(attempts))
        ready_count = 0
        last_line = ""
        last_parsed: Optional[Dict[str, Any]] = None
        for idx in range(total_attempts):
            line, parsed = self._read_mode2_frame(
                ga,
                prefer_stream=prefer_stream,
                ftd_hz=ftd_hz,
                attempts=1,
                retry_delay_s=0.0,
                require_usable=True,
                frame_acceptance_mode="startup_mode2",
            )
            if line:
                last_line = line
            if parsed:
                last_parsed = parsed
                ready_count += 1
                if ready_count >= need:
                    return last_line, last_parsed
            else:
                ready_count = 0
                last_parsed = None
            if idx + 1 < total_attempts and retry_delay_s > 0:
                time.sleep(max(0.01, float(retry_delay_s)))
        return last_line, None

    def _configure_gas_analyzer(
        self,
        ga: Any,
        *,
        label: str = "",
        mode: int,
        active_send: bool,
        ftd_hz: int,
        avg_co2: int,
        avg_h2o: int,
        avg_filter: int,
        warning_phase: str = "startup",
    ) -> None:
        # Bench requirement: initialize with a minimal command set, do not wait for
        # replies on the setup commands, then allow the analyzer to proceed once
        # active streaming emits protocol-ready MODE2 frames. Strict frame quality
        # gates still apply later during formal sampling.
        set_warning_phase = getattr(ga, "set_warning_phase", None)
        init_retry_cfg = dict(self.cfg.get("workflow", {}).get("analyzer_mode2_init", {}) or {})
        reapply_attempts = max(1, int(init_retry_cfg.get("reapply_attempts", 4) or 4))
        stream_attempts = max(1, int(init_retry_cfg.get("stream_attempts", 10) or 10))
        passive_attempts = max(1, int(init_retry_cfg.get("passive_attempts", 4) or 4))
        ready_consecutive_frames = max(1, int(init_retry_cfg.get("ready_consecutive_frames", 2) or 2))
        retry_delay_s = max(0.01, float(init_retry_cfg.get("retry_delay_s", 0.2) or 0.2))
        reapply_delay_s = max(0.0, float(init_retry_cfg.get("reapply_delay_s", 0.35) or 0.35))
        command_gap_s = max(0.0, float(init_retry_cfg.get("command_gap_s", 0.15) or 0.15))
        last_error: Optional[Exception] = None

        try:
            if callable(set_warning_phase):
                set_warning_phase(warning_phase)
            set_comm_way = getattr(ga, "set_comm_way_with_ack", None)
            set_mode = getattr(ga, "set_mode_with_ack", None)
            set_active_freq = getattr(ga, "set_active_freq_with_ack", None)
            set_average_filter = getattr(ga, "set_average_filter_with_ack", None)
            set_average_filter_channel = getattr(ga, "set_average_filter_channel_with_ack", None)
            success_ack = getattr(ga, "_is_success_ack", None)
            post_enable_stream_wait_s = max(
                0.0,
                float(init_retry_cfg.get("post_enable_stream_wait_s", 2.0) or 2.0),
            )
            post_enable_stream_ack_wait_s = max(
                0.0,
                float(init_retry_cfg.get("post_enable_stream_ack_wait_s", 8.0) or 8.0),
            )

            for attempt_idx in range(reapply_attempts):
                if callable(set_comm_way):
                    set_comm_way(False, require_ack=False)
                else:
                    ga.set_comm_way(False)
                if command_gap_s > 0:
                    time.sleep(command_gap_s)
                if callable(set_mode):
                    set_mode(mode, require_ack=False)
                else:
                    ga.set_mode(mode)
                if command_gap_s > 0:
                    time.sleep(command_gap_s)
                if callable(set_active_freq):
                    set_active_freq(ftd_hz, require_ack=False)
                else:
                    ga.set_active_freq(ftd_hz)
                if command_gap_s > 0:
                    time.sleep(command_gap_s)
                if callable(set_average_filter_channel):
                    set_average_filter_channel(1, avg_filter, require_ack=False)
                    if command_gap_s > 0:
                        time.sleep(command_gap_s)
                    set_average_filter_channel(2, avg_filter, require_ack=False)
                else:
                    if callable(set_average_filter):
                        set_average_filter(avg_filter, require_ack=False)
                    else:
                        ga.set_average_filter(avg_filter)
                if command_gap_s > 0:
                    time.sleep(command_gap_s)

                if active_send:
                    if callable(set_comm_way):
                        set_comm_way(True, require_ack=False)
                    else:
                        ga.set_comm_way(True)
                    if post_enable_stream_wait_s > 0:
                        time.sleep(post_enable_stream_wait_s)
                    stream_line, stream_parsed = self._verify_startup_mode2_ready(
                        ga,
                        prefer_stream=True,
                        ftd_hz=ftd_hz,
                        attempts=stream_attempts,
                        retry_delay_s=retry_delay_s,
                        consecutive_frames=ready_consecutive_frames,
                    )
                    if stream_parsed:
                        return
                    if callable(success_ack) and success_ack(stream_line):
                        label_text = label or "gas_analyzer"
                        self.log(
                            "Analyzer MODE2 stream ack observed: "
                            f"{label_text} wait={post_enable_stream_ack_wait_s:.1f}s for MODE2 data"
                        )
                        if post_enable_stream_ack_wait_s > 0:
                            time.sleep(post_enable_stream_ack_wait_s)
                        ack_follow_line, ack_follow_parsed = self._verify_startup_mode2_ready(
                            ga,
                            prefer_stream=True,
                            ftd_hz=ftd_hz,
                            attempts=max(stream_attempts, 3),
                            retry_delay_s=retry_delay_s,
                            consecutive_frames=ready_consecutive_frames,
                        )
                        if ack_follow_parsed:
                            return
                        if ack_follow_line:
                            stream_line = ack_follow_line
                    last_error = RuntimeError(
                        "MODE2 not ready (stream) "
                        f"last={self._summarize_sensor_line(stream_line)}"
                    )
                else:
                    passive_line, passive_parsed = self._verify_startup_mode2_ready(
                        ga,
                        prefer_stream=False,
                        ftd_hz=ftd_hz,
                        attempts=passive_attempts,
                        retry_delay_s=max(0.01, retry_delay_s / 2.0),
                        consecutive_frames=ready_consecutive_frames,
                    )
                    if passive_parsed:
                        return
                    last_error = RuntimeError(
                        "MODE2 not ready (passive) "
                        f"last={self._summarize_sensor_line(passive_line)}"
                    )

                if attempt_idx + 1 < reapply_attempts:
                    label_text = label or "gas_analyzer"
                    self.log(
                        "Analyzer MODE2 verify retry: "
                        f"{label_text} attempt {attempt_idx + 2}/{reapply_attempts} "
                        f"reason={last_error}"
                    )
                    if reapply_delay_s > 0:
                        time.sleep(reapply_delay_s)

            if last_error is not None:
                raise last_error
        finally:
            if callable(set_warning_phase):
                set_warning_phase("")

    @staticmethod
    def _safe_label(label: str) -> str:
        text = re.sub(r"[^0-9A-Za-z_]+", "_", str(label).strip().lower())
        return text.strip("_") or "ga"

    @staticmethod
    def _mode2_sample_fields() -> List[str]:
        # Keep CSV schema stable so full MODE2 fields are always present.
        return [
            "raw",
            "id",
            "mode",
            "mode2_field_count",
            "co2_ppm",
            "h2o_mmol",
            "co2_density",
            "h2o_density",
            "co2_ratio_f",
            "co2_ratio_raw",
            "h2o_ratio_f",
            "h2o_ratio_raw",
            "ref_signal",
            "co2_signal",
            "h2o_signal",
            "chamber_temp_c",
            "case_temp_c",
            "pressure_kpa",
            "status",
        ]

    def _all_gas_analyzers(self) -> List[Tuple[str, Any, Dict[str, Any]]]:
        dcfg = self.cfg.get("devices", {})
        list_cfg = dcfg.get("gas_analyzers", [])
        prefixed = sorted(k for k in self.devices.keys() if k.startswith("gas_analyzer_"))

        entries: List[Tuple[str, Any, Dict[str, Any]]] = []
        if prefixed:
            for idx, key in enumerate(prefixed, start=1):
                item_cfg = {}
                if isinstance(list_cfg, list) and idx - 1 < len(list_cfg) and isinstance(list_cfg[idx - 1], dict):
                    item_cfg = list_cfg[idx - 1]
                label = str(item_cfg.get("name") or f"ga{idx:02d}")
                entries.append((label, self.devices[key], item_cfg))
            return entries

        ga = self.devices.get("gas_analyzer")
        if ga:
            one_cfg = dcfg.get("gas_analyzer", {})
            entries.append(("ga01", ga, one_cfg if isinstance(one_cfg, dict) else {}))
        return entries

    def _gas_analyzers(self) -> List[Tuple[str, Any, Dict[str, Any]]]:
        return self._all_gas_analyzers()

    def _active_gas_analyzers(self) -> List[Tuple[str, Any, Dict[str, Any]]]:
        return [entry for entry in self._all_gas_analyzers() if entry[0] not in self._disabled_analyzers]

    def _disable_analyzers(self, labels: List[str], reason: str) -> None:
        dropped = []
        for label in labels:
            if label in self._disabled_analyzers:
                continue
            self._disabled_analyzers.add(label)
            self._disabled_analyzer_reasons[label] = reason
            dropped.append(label)
        if dropped:
            self._log_run_event(
                command="analyzers-disabled",
                response=json.dumps({"labels": dropped, "reason": reason}, ensure_ascii=False),
            )
            self.log(f"Analyzers dropped from active set: {', '.join(dropped)} reason={reason}")

    def _gas_analyzer_runtime_settings(self, cfg: Dict[str, Any]) -> Dict[str, int | bool]:
        dcfg = self.cfg.get("devices", {})
        gas_cfg_default = dcfg.get("gas_analyzer", {})
        return {
            "mode": 2,
            "active_send": bool(cfg.get("active_send", gas_cfg_default.get("active_send", False))),
            "ftd_hz": int(cfg.get("ftd_hz", gas_cfg_default.get("ftd_hz", 1))),
            "avg_co2": int(cfg.get("average_co2", gas_cfg_default.get("average_co2", 1))),
            "avg_h2o": int(cfg.get("average_h2o", gas_cfg_default.get("average_h2o", 1))),
            "avg_filter": int(cfg.get("average_filter", gas_cfg_default.get("average_filter", 49))),
        }

    def _analyzer_reprobe_cooldown_s(self) -> float:
        return float(self._wf("workflow.analyzer_reprobe.cooldown_s", 300.0))

    def _attempt_reenable_disabled_analyzers(self) -> None:
        if not self._disabled_analyzers:
            return

        cooldown_s = max(0.0, self._analyzer_reprobe_cooldown_s())
        now = time.time()
        recovered: List[str] = []
        failed: List[str] = []
        skipped: List[str] = []
        for label, ga, cfg in self._all_gas_analyzers():
            if label not in self._disabled_analyzers:
                continue
            last_reprobe_ts = self._disabled_analyzer_last_reprobe_ts.get(label)
            if last_reprobe_ts is not None and cooldown_s > 0:
                remain = cooldown_s - (now - last_reprobe_ts)
                if remain > 0:
                    skipped.append(f"{label}({int(math.ceil(remain))}s)")
                    continue
            settings = self._gas_analyzer_runtime_settings(cfg)
            self.log(f"Analyzer re-probe start: {label}")
            try:
                self._configure_gas_analyzer(
                    ga,
                    label=label,
                    mode=int(settings["mode"]),
                    active_send=bool(settings["active_send"]),
                    ftd_hz=int(settings["ftd_hz"]),
                    avg_co2=int(settings["avg_co2"]),
                    avg_h2o=int(settings["avg_h2o"]),
                    avg_filter=int(settings["avg_filter"]),
                    warning_phase="runtime",
                )
                _, parsed = self._read_sensor_parsed(
                    ga,
                    required_key="co2_ratio_f",
                    frame_acceptance_mode="strict",
                )
                if parsed and self._pick_numeric(parsed, ["co2_ratio_f", "co2_ppm", "h2o_ratio_f", "h2o_mmol"]) is not None:
                    self._disabled_analyzers.discard(label)
                    self._disabled_analyzer_reasons.pop(label, None)
                    self._disabled_analyzer_last_reprobe_ts.pop(label, None)
                    recovered.append(label)
                    continue
            except Exception as exc:
                self.log(f"Analyzer re-probe failed: {label} err={exc}")
            self._disabled_analyzer_last_reprobe_ts[label] = now
            failed.append(label)

        if recovered:
            self._log_run_event(
                command="analyzers-restored",
                response=json.dumps({"labels": recovered}, ensure_ascii=False),
            )
            self.log(f"Analyzers restored to active set: {', '.join(recovered)}")
        if failed:
            self._log_run_event(
                command="analyzers-still-disabled",
                response=json.dumps({"labels": failed}, ensure_ascii=False),
            )
            self.log(f"Analyzers still disabled after re-probe: {', '.join(failed)}")
        if skipped:
            self.log(f"Analyzers re-probe cooldown active: {', '.join(skipped)}")

    @staticmethod
    def _parse_sensor_line(ga: Any, line: str) -> Optional[Dict[str, Any]]:
        parse_mode2 = getattr(ga, "parse_line_mode2", None)
        if callable(parse_mode2):
            return parse_mode2(line)
        parse_any = getattr(ga, "parse_line", None)
        if callable(parse_any):
            return parse_any(line)
        return None

    def _analyzer_frame_quality_cfg(self) -> Dict[str, Any]:
        return dict(self.cfg.get("workflow", {}).get("analyzer_frame_quality", {}) or {})

    def _frame_quality_sentinel_config(self) -> tuple[List[float], float]:
        cfg = self._analyzer_frame_quality_cfg()
        invalid_sentinels: List[float] = []
        for item in cfg.get("invalid_sentinel_values", [-1001.0, -9999.0, 999999.0]) or []:
            try:
                invalid_sentinels.append(float(item))
            except Exception:
                continue
        invalid_sentinel_tol = abs(float(cfg.get("invalid_sentinel_tolerance", 1e-6) or 1e-6))
        return invalid_sentinels, invalid_sentinel_tol

    @staticmethod
    def _frame_quality_key_set(raw_values: Any, default_values: List[str]) -> set[str]:
        values = raw_values if isinstance(raw_values, list) else default_values
        normalized: set[str] = set()
        for item in values:
            text = str(item or "").strip()
            if text:
                normalized.add(text)
        return normalized

    def _live_snapshot_cfg(self) -> Dict[str, Any]:
        return dict(cfg_get(self.cfg, "workflow.analyzer_live_snapshot", {}) or {})

    @staticmethod
    def _analyzer_runtime_key(ga: Any, label: Optional[str] = None) -> str:
        label_text = str(label or getattr(ga, "_runtime_label", "") or "").strip()
        port_text = str(getattr(getattr(ga, "ser", None), "port", "") or "").strip()
        device_id_text = str(getattr(ga, "device_id", "") or "").strip()
        if label_text:
            return f"{label_text}|{port_text}|{device_id_text}"
        if port_text or device_id_text:
            return f"{port_text}|{device_id_text}"
        return f"ga:{id(ga)}"

    def _cache_live_analyzer_frame(
        self,
        ga: Any,
        line: str,
        parsed: Optional[Dict[str, Any]],
        *,
        category: str,
        label: Optional[str] = None,
        source: str = "",
        is_live: Optional[bool] = None,
        recv_wall_ts: Optional[str] = None,
        recv_mono_s: Optional[float] = None,
        timestamp: Optional[float] = None,
        seq: Optional[int] = None,
    ) -> None:
        if not isinstance(parsed, dict) or not parsed:
            return
        recv_ts = time.time() if timestamp is None else float(timestamp)
        entry = {
            "line": str(line or ""),
            "parsed": dict(parsed),
            "timestamp": recv_ts,
            "recv_wall_ts": recv_wall_ts or self._ts_from_datetime(datetime.now()),
            "recv_mono_s": time.monotonic() if recv_mono_s is None else float(recv_mono_s),
            "seq": self._next_live_analyzer_frame_seq() if seq is None else int(seq),
            "category": str(category or ""),
            "source": str(source or ""),
            "is_live": bool(is_live) if is_live is not None else None,
        }
        with self._live_analyzer_frame_cache_lock:
            self._live_analyzer_frame_cache[self._analyzer_runtime_key(ga, label)] = entry

    def _get_fresh_live_analyzer_frame_cache(self, ga: Any) -> Optional[Dict[str, Any]]:
        cfg = self._live_snapshot_cfg()
        if not bool(cfg.get("enabled", True)):
            return None
        ttl_s = max(0.0, float(cfg.get("cache_ttl_s", 0.5) or 0.5))
        if ttl_s <= 0:
            return None
        with self._live_analyzer_frame_cache_lock:
            entry = self._live_analyzer_frame_cache.get(self._analyzer_runtime_key(ga))
        if not isinstance(entry, dict):
            return None
        age_s = time.time() - float(entry.get("timestamp", 0.0) or 0.0)
        if age_s > ttl_s:
            return None
        return entry

    def _get_live_analyzer_frame_cache(self, ga: Any, *, label: Optional[str] = None) -> Optional[Dict[str, Any]]:
        with self._live_analyzer_frame_cache_lock:
            entry = self._live_analyzer_frame_cache.get(self._analyzer_runtime_key(ga, label))
            if not isinstance(entry, dict):
                return None
            return dict(entry)

    def _emit_sensor_read_reject_summary(self, signature: Tuple[str, str, str, str, str], state: Dict[str, Any]) -> None:
        suppressed_count = int(state.get("suppressed_count", 0) or 0)
        if suppressed_count <= 0:
            return
        analyzer_key, required_key, acceptance_mode, category, reason = signature
        self._log_run_event(
            command="sensor-read-reject-summary",
            response=json.dumps(
                {
                    "analyzer_key": analyzer_key,
                    "required_key": required_key,
                    "acceptance_mode": acceptance_mode,
                    "category": category,
                    "reason": reason,
                    "suppressed_count": suppressed_count,
                    "window_s": float(state.get("window_s", 15.0) or 15.0),
                    "line_preview": str(state.get("line_preview") or "")[:120],
                },
                ensure_ascii=False,
            ),
        )

    def _flush_sensor_read_reject_states(
        self,
        *,
        analyzer_key: Optional[str] = None,
        exclude_signature: Optional[Tuple[str, str, str, str, str]] = None,
        expired_only: bool = False,
        force: bool = False,
    ) -> None:
        now = time.time()
        pending = list(self._sensor_read_reject_states.items())
        for signature, state in pending:
            if exclude_signature is not None and signature == exclude_signature:
                continue
            if analyzer_key is not None and state.get("analyzer_key") != analyzer_key:
                continue
            if not force and expired_only:
                detail_ts = float(state.get("detail_ts", 0.0) or 0.0)
                window_s = float(state.get("window_s", 15.0) or 15.0)
                if (now - detail_ts) < window_s:
                    continue
            self._emit_sensor_read_reject_summary(signature, state)
            self._sensor_read_reject_states.pop(signature, None)

    def _log_sensor_read_reject(
        self,
        ga: Any,
        *,
        required_key: str,
        acceptance_mode: str,
        category: str,
        reason: str,
        line_preview: str,
    ) -> None:
        cfg = self._analyzer_frame_quality_cfg()
        window_s = max(1.0, float(cfg.get("reject_log_window_s", 15.0) or 15.0))
        analyzer_key = self._analyzer_runtime_key(ga)
        signature = (
            analyzer_key,
            str(required_key or ""),
            str(acceptance_mode or ""),
            str(category or ""),
            str(reason or ""),
        )
        self._flush_sensor_read_reject_states(expired_only=True)
        self._flush_sensor_read_reject_states(analyzer_key=analyzer_key, exclude_signature=signature)

        now = time.time()
        state = self._sensor_read_reject_states.get(signature)
        if state is not None:
            detail_ts = float(state.get("detail_ts", 0.0) or 0.0)
            if (now - detail_ts) < window_s:
                state["suppressed_count"] = int(state.get("suppressed_count", 0) or 0) + 1
                state["last_ts"] = now
                state["line_preview"] = str(line_preview or state.get("line_preview") or "")[:120]
                return
            self._emit_sensor_read_reject_summary(signature, state)

        self._log_run_event(
            command="sensor-read-reject",
            response=json.dumps(
                {
                    "analyzer_key": analyzer_key,
                    "required_key": required_key,
                    "acceptance_mode": acceptance_mode,
                    "category": category,
                    "reason": reason,
                    "line_preview": str(line_preview or "")[:120],
                },
                ensure_ascii=False,
            ),
        )
        self._sensor_read_reject_states[signature] = {
            "analyzer_key": analyzer_key,
            "detail_ts": now,
            "last_ts": now,
            "suppressed_count": 0,
            "line_preview": str(line_preview or "")[:120],
            "window_s": window_s,
        }

    def _resolve_sensor_frame_acceptance_mode(
        self,
        required_key: Optional[str],
        *,
        require_usable: bool,
        explicit_mode: Optional[str] = None,
    ) -> str:
        # Runtime waits only need the requested field to be trustworthy; sampling and
        # fitting still use the strict whole-frame gate via _assess_analyzer_frame().
        if not require_usable:
            return "unfiltered"

        mode = str(explicit_mode or "").strip().lower()
        if mode in {"strict", "required_key_relaxed"}:
            return mode

        key = str(required_key or "").strip()
        if not key:
            return "strict"

        cfg = self._analyzer_frame_quality_cfg()
        strict_required_keys = self._frame_quality_key_set(
            cfg.get("strict_required_keys"),
            ["co2_ratio_f", "h2o_ratio_f", "co2_ppm", "h2o_mmol"],
        )
        if key in strict_required_keys:
            return "strict"

        relaxed_enabled = bool(cfg.get("runtime_relaxed_for_required_key", True))
        relaxed_required_keys = self._frame_quality_key_set(
            cfg.get("relaxed_required_keys"),
            ["chamber_temp_c", "case_temp_c", "temp_c"],
        )
        if relaxed_enabled and key in relaxed_required_keys:
            return "required_key_relaxed"
        return "strict"

    def _assess_required_key_value(
        self,
        parsed: Optional[Dict[str, Any]],
        required_key: Optional[str],
    ) -> tuple[bool, str]:
        key = str(required_key or "").strip()
        if not key:
            return True, "未指定字段"
        if not isinstance(parsed, dict) or not parsed:
            return False, "无帧"
        if key not in parsed:
            return False, f"{key}缺失"

        raw_value = parsed.get(key)
        if raw_value is None:
            return False, f"{key}缺失"
        if isinstance(raw_value, str) and not raw_value.strip():
            return False, f"{key}为空"

        invalid_sentinels, invalid_sentinel_tol = self._frame_quality_sentinel_config()
        if invalid_sentinels and self._matches_frame_sentinel(raw_value, invalid_sentinels, invalid_sentinel_tol):
            return False, f"{key}=sentinel({raw_value})"

        try:
            numeric = float(raw_value)
        except Exception:
            return False, f"{key}非数值({raw_value})"
        if not math.isfinite(numeric):
            return False, f"{key}非有限值({raw_value})"
        return True, f"{key}可用"

    def _assess_runtime_required_key_frame(
        self,
        parsed: Optional[Dict[str, Any]],
        required_key: Optional[str],
    ) -> tuple[bool, str]:
        if not isinstance(parsed, dict) or not parsed:
            return False, "无帧"

        cfg = self._analyzer_frame_quality_cfg()
        status_text = str(parsed.get("status", "") or "").strip().upper()
        hard_tokens = self._frame_quality_key_set(
            cfg.get("runtime_hard_bad_status_tokens"),
            ["FAIL", "INVALID", "ERROR"],
        )
        soft_tokens = self._frame_quality_key_set(
            cfg.get("runtime_soft_bad_status_tokens"),
            ["NO_RESPONSE", "NO_ACK"],
        )
        if status_text and any(token.upper() in status_text for token in hard_tokens):
            return False, f"状态异常({status_text})"

        key_ok, key_status = self._assess_required_key_value(parsed, required_key)
        if not key_ok:
            return False, key_status

        if status_text and any(token.upper() in status_text for token in soft_tokens):
            return True, f"状态告警({status_text})；{key_status}"
        return True, key_status

    def _assess_sensor_frame_for_read(
        self,
        parsed: Optional[Dict[str, Any]],
        *,
        required_key: Optional[str],
        require_usable: bool,
        acceptance_mode: str,
    ) -> tuple[bool, str]:
        if not require_usable:
            return self._assess_required_key_value(parsed, required_key)

        if acceptance_mode == "required_key_relaxed":
            return self._assess_runtime_required_key_frame(parsed, required_key)

        usable, status = self._assess_analyzer_frame(parsed)
        if not usable:
            return False, status
        key_ok, key_status = self._assess_required_key_value(parsed, required_key)
        if not key_ok:
            return False, key_status
        return True, status

    def _classify_sensor_read_line(self, ga: Any, line: str, parsed: Optional[Dict[str, Any]]) -> str:
        text = str(line or "").strip()
        if not text:
            return "empty"

        if isinstance(parsed, dict) and parsed:
            field_count = self._as_int(parsed.get("mode2_field_count"))
            min_mode2_fields = max(0, int(self._analyzer_frame_quality_cfg().get("min_mode2_fields", 16) or 16))
            if field_count is not None and field_count < min_mode2_fields:
                return f"short_frame({field_count})"
            return "parsed"

        ack_checker = getattr(type(ga), "_is_success_ack", None)
        if callable(ack_checker):
            try:
                if ack_checker(text):
                    return "ack"
            except Exception:
                pass

        return "parse_failed"

    @staticmethod
    def _sensor_read_cache_eligible_category(category: str) -> bool:
        text = str(category or "").strip().lower()
        return text in {"empty", "ack", "parse_failed"} or text.startswith("short_frame")

    @staticmethod
    def _matches_frame_sentinel(value: Any, sentinels: List[float], tolerance: float) -> bool:
        try:
            numeric = float(value)
        except Exception:
            return False
        if not math.isfinite(numeric):
            return True
        return any(abs(numeric - float(sentinel)) <= float(tolerance) for sentinel in sentinels)

    def _frame_quality_ratio_is_usable(self, value: Any, sentinels: List[float], tolerance: float) -> bool:
        numeric = self._as_float(value)
        if numeric is None or not math.isfinite(numeric) or numeric <= 0:
            return False
        return not self._matches_frame_sentinel(numeric, sentinels, tolerance)

    def _assess_mode2_frame_for_startup(self, parsed: Optional[Dict[str, Any]]) -> tuple[bool, str]:
        if not isinstance(parsed, dict) or not parsed:
            return False, "无帧"

        cfg = self._analyzer_frame_quality_cfg()
        issues: List[str] = []
        min_mode2_fields = max(0, int(cfg.get("min_mode2_fields", 16) or 16))
        field_count = self._as_int(parsed.get("mode2_field_count"))
        if field_count is not None and field_count < min_mode2_fields:
            issues.append(f"短帧({field_count})")

        status_text = str(parsed.get("status", "") or "").strip().upper()
        bad_status_tokens = self._frame_quality_key_set(
            cfg.get("bad_status_tokens"),
            ["FAIL", "INVALID", "NO_RESPONSE", "NO_ACK", "ERROR"],
        )
        if status_text and any(token.upper() in status_text for token in bad_status_tokens):
            issues.append(f"状态异常({status_text})")

        required_keys = cfg.get(
            "strict_required_keys",
            ["co2_ratio_f", "h2o_ratio_f", "co2_ppm", "h2o_mmol"],
        ) or ["co2_ratio_f", "h2o_ratio_f", "co2_ppm", "h2o_mmol"]
        seen_keys: set[str] = set()
        for item in required_keys:
            key = str(item or "").strip()
            if not key or key in seen_keys:
                continue
            seen_keys.add(key)
            ok, status = self._assess_required_key_value(parsed, key)
            if not ok:
                issues.append(status)

        if issues:
            return False, "；".join(issues)
        return True, "启动MODE2可用"

    def _assess_analyzer_frame(self, parsed: Optional[Dict[str, Any]]) -> tuple[bool, str]:
        if not isinstance(parsed, dict) or not parsed:
            return False, "无帧"

        cfg = self._analyzer_frame_quality_cfg()
        issues: List[str] = []
        marks: List[str] = []
        min_mode2_fields = max(0, int(cfg.get("min_mode2_fields", 16) or 16))
        field_count = self._as_int(parsed.get("mode2_field_count"))
        if field_count is not None and field_count < min_mode2_fields:
            issues.append(f"短帧({field_count})")

        status_text = str(parsed.get("status", "") or "").strip().upper()
        bad_status_tokens = cfg.get("bad_status_tokens", ["FAIL", "INVALID", "NO_RESPONSE", "NO_ACK", "ERROR"])
        if status_text and any(str(token).strip().upper() in status_text for token in bad_status_tokens):
            issues.append(f"状态异常({status_text})")

        suspicious_co2_ppm_min = float(cfg.get("suspicious_co2_ppm_min", 2999.0) or 2999.0)
        suspicious_h2o_mmol_min = float(cfg.get("suspicious_h2o_mmol_min", 70.0) or 70.0)
        invalid_sentinels, invalid_sentinel_tol = self._frame_quality_sentinel_config()
        co2_ppm = self._as_float(parsed.get("co2_ppm"))
        h2o_mmol = self._as_float(parsed.get("h2o_mmol"))
        co2_ratio_f = self._as_float(parsed.get("co2_ratio_f"))
        h2o_ratio_f = self._as_float(parsed.get("h2o_ratio_f"))
        pressure_kpa = self._as_float(parsed.get("pressure_kpa"))
        pressure_kpa_min = self._as_float(cfg.get("pressure_kpa_min", 30.0))
        pressure_kpa_max = self._as_float(cfg.get("pressure_kpa_max", 150.0))

        if bool(cfg.get("reject_negative_co2_ppm", True)) and co2_ppm is not None and co2_ppm < 0:
            issues.append(f"co2<0({co2_ppm:g})")
        if bool(cfg.get("reject_negative_h2o_mmol", True)) and h2o_mmol is not None and h2o_mmol < 0:
            issues.append(f"h2o<0({h2o_mmol:g})")
        if bool(cfg.get("reject_nonpositive_co2_ratio_f", True)) and co2_ratio_f is not None and co2_ratio_f <= 0:
            issues.append(f"R_CO2<=0({co2_ratio_f:g})")
        if bool(cfg.get("reject_nonpositive_h2o_ratio_f", True)) and h2o_ratio_f is not None and h2o_ratio_f <= 0:
            issues.append(f"R_H2O<=0({h2o_ratio_f:g})")

        if invalid_sentinels:
            for key in ("co2_ppm", "h2o_mmol", "co2_ratio_f", "h2o_ratio_f", "pressure_kpa"):
                raw_value = parsed.get(key)
                if self._matches_frame_sentinel(raw_value, invalid_sentinels, invalid_sentinel_tol):
                    issues.append(f"{key}=sentinel({raw_value})")

        if (
            pressure_kpa is not None
            and pressure_kpa_min is not None
            and pressure_kpa < pressure_kpa_min
        ):
            issues.append(f"P_kPa<{pressure_kpa_min:g}({pressure_kpa:g})")
        if (
            pressure_kpa is not None
            and pressure_kpa_max is not None
            and pressure_kpa > pressure_kpa_max
        ):
            issues.append(f"P_kPa>{pressure_kpa_max:g}({pressure_kpa:g})")
        if (
            co2_ppm is not None
            and h2o_mmol is not None
            and co2_ppm >= suspicious_co2_ppm_min
            and h2o_mmol >= suspicious_h2o_mmol_min
        ):
            if any(
                self._frame_quality_ratio_is_usable(parsed.get(key), invalid_sentinels, invalid_sentinel_tol)
                for key in (
                    "co2_ratio_f",
                    "co2_ratio_raw",
                    "h2o_ratio_f",
                    "h2o_ratio_raw",
                    "co2_sig",
                    "h2o_sig",
                )
            ):
                marks.append("极值已标记")
            else:
                issues.append("异常极值")

        if issues:
            return False, "；".join(issues)
        if marks:
            return True, "；".join(marks)
        return True, "可用"

    @staticmethod
    def _set_sample_frame_meta(
        data: Dict[str, Any],
        prefix: str,
        *,
        has_data: bool,
        usable: bool,
        status: str,
        is_primary: bool,
    ) -> None:
        data[f"{prefix}_frame_has_data"] = bool(has_data)
        data[f"{prefix}_frame_usable"] = bool(usable)
        data[f"{prefix}_frame_status"] = str(status or "")
        if is_primary:
            data["frame_has_data"] = bool(has_data)
            data["frame_usable"] = bool(usable)
            data["frame_status"] = str(status or "")

    def _read_runtime_sensor_line(self, ga: Any) -> str:
        read_latest = getattr(ga, "read_latest_data", None)
        if callable(read_latest):
            try:
                return str(read_latest(allow_passive_fallback=False) or "")
            except TypeError:
                return str(read_latest() or "")
        return ""

    def _read_sensor_parsed(
        self,
        ga: Any,
        *,
        required_key: Optional[str] = None,
        require_usable: bool = True,
        frame_acceptance_mode: Optional[str] = None,
    ) -> tuple[str, Optional[Dict[str, Any]]]:
        retries = int(self._wf("workflow.sensor_read_retry.retries", 1))
        retry_delay_s = float(self._wf("workflow.sensor_read_retry.delay_s", 0.05))
        acceptance_mode = self._resolve_sensor_frame_acceptance_mode(
            required_key,
            require_usable=require_usable,
            explicit_mode=frame_acceptance_mode,
        )

        last_line = ""
        last_parsed: Optional[Dict[str, Any]] = None
        last_reason = "无帧"
        last_category = "empty"
        attempts = 1 + max(0, retries)
        for idx in range(attempts):
            line = self._read_runtime_sensor_line(ga)
            parsed = self._parse_sensor_line(ga, line)
            last_line = line
            last_parsed = parsed
            last_category = self._classify_sensor_read_line(ga, line, parsed)

            if not require_usable and required_key is None:
                if parsed:
                    return line, parsed
                last_reason = "无帧" if last_category == "empty" else f"{last_category}:未解析"
            else:
                if (
                    acceptance_mode == "required_key_relaxed"
                    and self._sensor_read_cache_eligible_category(last_category)
                ):
                    cached_entry = self._get_fresh_live_analyzer_frame_cache(ga)
                    if cached_entry:
                        cached_parsed = cached_entry.get("parsed")
                        cached_accepted, cached_status = self._assess_sensor_frame_for_read(
                            cached_parsed,
                            required_key=required_key,
                            require_usable=require_usable,
                            acceptance_mode=acceptance_mode,
                        )
                        if cached_accepted:
                            return str(cached_entry.get("line") or ""), cached_parsed

                accepted, accept_status = self._assess_sensor_frame_for_read(
                    parsed,
                    required_key=required_key,
                    require_usable=require_usable,
                    acceptance_mode=acceptance_mode,
                )
                last_reason = accept_status
                if accepted:
                    return line, parsed
            if idx + 1 < attempts:
                time.sleep(max(0.01, retry_delay_s))
        if require_usable and required_key is not None:
            self._log_sensor_read_reject(
                ga,
                required_key=required_key,
                acceptance_mode=acceptance_mode,
                category=last_category,
                reason=last_reason,
                line_preview=str(last_line or "")[:120],
            )
        if require_usable:
            return last_line, None
        return last_line, last_parsed

    def _refresh_live_analyzer_snapshots(self, *, force: bool = False, reason: str = "") -> None:
        cfg = self._live_snapshot_cfg()
        if not bool(cfg.get("enabled", True)):
            return
        analyzers = self._all_gas_analyzers()
        if not analyzers:
            return
        interval_s = max(0.5, float(cfg.get("interval_s", 5.0) or 5.0))
        now = time.time()
        if not force and (now - self._last_live_analyzer_snapshot_ts) < interval_s:
            return
        self._last_live_analyzer_snapshot_ts = now
        for label, ga, _cfg in analyzers:
            try:
                try:
                    setattr(ga, "_runtime_label", label)
                except Exception:
                    pass
                line = self._read_runtime_sensor_line(ga)
                parsed = self._parse_sensor_line(ga, line)
                category = self._classify_sensor_read_line(ga, line, parsed)
                self._cache_live_analyzer_frame(
                    ga,
                    line,
                    parsed,
                    category=category,
                    label=label,
                    source="active_live_cache",
                    is_live=True,
                )
            except Exception as exc:
                extra = f" ({reason})" if reason else ""
                self.log(f"Analyzer live snapshot failed: {label}{extra} err={exc}")

    @staticmethod
    def _as_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except Exception:
            return None

    @staticmethod
    def _as_float(value: Any) -> Optional[float]:
        try:
            v = float(value)
        except Exception:
            return None
        if v <= -999:
            return None
        return v

    def _pick_numeric(self, data: Dict[str, Any], keys: List[str]) -> Optional[float]:
        for key in keys:
            if key not in data:
                continue
            v = self._as_float(data.get(key))
            if v is not None:
                return v
        return None

    def _temperature_calibration_cfg(self) -> Dict[str, Any]:
        raw = self.cfg.get("temperature_calibration", {})
        cfg = raw if isinstance(raw, dict) else {}
        plausibility_raw = cfg.get("plausibility", {})
        plausibility = plausibility_raw if isinstance(plausibility_raw, dict) else {}
        snapshot_window_s = cfg.get("snapshot_window_s", 60.0)
        poll_interval_s = cfg.get("poll_interval_s", 1.0)
        min_ref_samples = cfg.get("min_ref_samples", 3)
        env_stable_span_c = cfg.get("env_stable_span_c", 0.3)
        box_stable_span_c = cfg.get("box_stable_span_c", 0.3)
        polynomial_order = cfg.get("polynomial_order", 3)
        return {
            "enabled": bool(cfg.get("enabled", True)),
            "snapshot_window_s": max(0.0, float(60.0 if snapshot_window_s is None else snapshot_window_s)),
            "poll_interval_s": max(0.2, float(1.0 if poll_interval_s is None else poll_interval_s)),
            "min_ref_samples": max(1, int(3 if min_ref_samples is None else min_ref_samples)),
            "env_stable_span_c": max(0.0, float(0.3 if env_stable_span_c is None else env_stable_span_c)),
            "box_stable_span_c": max(0.0, float(0.3 if box_stable_span_c is None else box_stable_span_c)),
            "use_env_temp_as_ref_first": bool(cfg.get("use_env_temp_as_ref_first", True)),
            "fallback_to_box_temp": bool(cfg.get("fallback_to_box_temp", True)),
            "export_commands": bool(cfg.get("export_commands", True)),
            "polynomial_order": max(1, int(3 if polynomial_order is None else polynomial_order)),
            "plausibility_enabled": bool(plausibility.get("enabled", True)),
            "plausibility_temp_min_c": float(plausibility.get("raw_temp_min_c", -30.0)),
            "plausibility_temp_max_c": float(plausibility.get("raw_temp_max_c", 85.0)),
            "plausibility_max_abs_delta_from_ref_c": float(plausibility.get("max_abs_delta_from_ref_c", 15.0)),
            "plausibility_max_cell_shell_gap_c": float(plausibility.get("max_cell_shell_gap_c", 12.0)),
            "plausibility_hard_bad_values_c": [
                float(value)
                for value in (plausibility.get("hard_bad_values_c", [-40.0, 60.0]) or [-40.0, 60.0])
                if value is not None
            ],
            "plausibility_hard_bad_value_tolerance_c": float(plausibility.get("hard_bad_value_tolerance_c", 0.05)),
        }

    @staticmethod
    def _temp_matches_hard_bad_value(value_c: Optional[float], bad_values_c: List[float], tolerance_c: float) -> bool:
        if value_c is None:
            return False
        for candidate in bad_values_c:
            if abs(float(value_c) - float(candidate)) <= float(tolerance_c):
                return True
        return False

    def _evaluate_temperature_fit_validity(
        self,
        *,
        channel: str,
        raw_temp_c: Optional[float],
        paired_temp_c: Optional[float],
        ref_temp_c: Optional[float],
        cfg: Dict[str, Any],
    ) -> tuple[bool, str]:
        channel_key = str(channel or "").strip().lower() or "temp"
        if ref_temp_c is None:
            return False, "missing_ref"
        if raw_temp_c is None:
            return False, f"missing_{channel_key}_temp"
        if not bool(cfg.get("plausibility_enabled", True)):
            return True, ""
        if self._temp_matches_hard_bad_value(
            raw_temp_c,
            list(cfg.get("plausibility_hard_bad_values_c", []) or []),
            float(cfg.get("plausibility_hard_bad_value_tolerance_c", 0.05)),
        ):
            return False, "hard_bad_value"
        temp_min_c = float(cfg.get("plausibility_temp_min_c", -30.0))
        temp_max_c = float(cfg.get("plausibility_temp_max_c", 85.0))
        if raw_temp_c < temp_min_c or raw_temp_c > temp_max_c:
            return False, "raw_temp_out_of_range"
        max_ref_delta_c = float(cfg.get("plausibility_max_abs_delta_from_ref_c", 15.0))
        if abs(raw_temp_c - ref_temp_c) > max_ref_delta_c:
            return False, "too_far_from_ref"
        if paired_temp_c is not None:
            max_pair_gap_c = float(cfg.get("plausibility_max_cell_shell_gap_c", 12.0))
            if abs(raw_temp_c - paired_temp_c) > max_pair_gap_c:
                return False, "cell_shell_gap_too_large"
        return True, ""

    @staticmethod
    def _temperature_capture_key(point: CalibrationPoint, route_type: str) -> tuple[str, str]:
        temp_value = getattr(point, "temp_chamber_c", None)
        if temp_value is None:
            temp_text = "unknown"
        else:
            temp_text = f"{float(temp_value):.6f}"
        return temp_text, str(route_type or "").strip().lower() or "idle_before_route"

    @staticmethod
    def _mean_and_span(values: List[float]) -> tuple[Optional[float], Optional[float]]:
        cleaned = [float(value) for value in values if value is not None and math.isfinite(float(value))]
        if not cleaned:
            return None, None
        return float(mean(cleaned)), float(max(cleaned) - min(cleaned))

    def _capture_temperature_calibration_snapshot(
        self,
        point: CalibrationPoint,
        *,
        route_type: str,
    ) -> bool:
        cfg = self._temperature_calibration_cfg()
        if not cfg["enabled"]:
            return False

        capture_key = self._temperature_capture_key(point, route_type)
        if capture_key in self._temperature_calibration_capture_keys:
            return True
        self._temperature_calibration_capture_keys.add(capture_key)

        analyzers = self._all_gas_analyzers()
        if not analyzers:
            self.log("Temperature calibration snapshot warning: no analyzers available")
            return False

        window_s = float(cfg["snapshot_window_s"])
        poll_s = float(cfg["poll_interval_s"])
        end_time = time.time() + max(0.0, window_s)
        snapshot_time = datetime.now().isoformat(timespec="milliseconds")
        box_values: List[float] = []
        env_values: List[float] = []
        analyzer_samples: Dict[str, Dict[str, Any]] = {}
        warnings_seen: set[str] = set()

        self.log(
            "Temperature calibration snapshot start: "
            f"temp={getattr(point, 'temp_chamber_c', None)}C route={route_type} window={int(window_s)}s"
        )

        while True:
            if self.stop_event.is_set():
                self.log("Temperature calibration snapshot interrupted by stop request")
                break
            self._check_pause()

            thermometer = self.devices.get("thermometer")
            if thermometer:
                try:
                    env_temp = self._as_float(thermometer.read_temp_c())
                    if env_temp is not None:
                        env_values.append(float(env_temp))
                except Exception as exc:
                    warning_key = f"thermometer:{exc}"
                    if warning_key not in warnings_seen:
                        warnings_seen.add(warning_key)
                        self.log(f"Temperature calibration warning: thermometer read failed: {exc}")

            chamber = self.devices.get("temp_chamber")
            if chamber:
                try:
                    box_temp = self._as_float(chamber.read_temp_c())
                    if box_temp is not None:
                        box_values.append(float(box_temp))
                except Exception as exc:
                    warning_key = f"temp_chamber:{exc}"
                    if warning_key not in warnings_seen:
                        warnings_seen.add(warning_key)
                        self.log(f"Temperature calibration warning: chamber read failed: {exc}")

            for label, ga, analyzer_cfg in analyzers:
                sample_bucket = analyzer_samples.setdefault(
                    label.upper(),
                    {
                        "analyzer_id": label.upper(),
                        "analyzer_device_id": str(analyzer_cfg.get("device_id") or "").strip() or None,
                        "cell_values": [],
                        "shell_values": [],
                    },
                )
                try:
                    _, parsed = self._read_sensor_parsed(ga, require_usable=False)
                    if parsed:
                        parsed_id = str(parsed.get("id") or "").strip()
                        if parsed_id:
                            sample_bucket["analyzer_device_id"] = parsed_id
                        cell_temp = self._as_float(parsed.get("chamber_temp_c"))
                        if cell_temp is None:
                            cell_temp = self._as_float(parsed.get("temp_c"))
                        shell_temp = self._as_float(parsed.get("case_temp_c"))
                        if cell_temp is not None:
                            sample_bucket["cell_values"].append(float(cell_temp))
                        if shell_temp is not None:
                            sample_bucket["shell_values"].append(float(shell_temp))
                    else:
                        warning_key = f"{label}:no_frame"
                        if warning_key not in warnings_seen:
                            warnings_seen.add(warning_key)
                            self.log(f"Temperature calibration warning: {label} has no valid temperature frame")
                except Exception as exc:
                    warning_key = f"{label}:{exc}"
                    if warning_key not in warnings_seen:
                        warnings_seen.add(warning_key)
                        self.log(f"Temperature calibration warning: {label} snapshot failed: {exc}")

            if time.time() >= end_time:
                break
            time.sleep(min(poll_s, max(0.05, end_time - time.time())))

        env_mean, env_span = self._mean_and_span(env_values)
        box_mean, box_span = self._mean_and_span(box_values)
        env_valid = (
            env_mean is not None
            and env_span is not None
            and len(env_values) >= int(cfg["min_ref_samples"])
            and env_span <= float(cfg["env_stable_span_c"])
        )
        box_valid = (
            box_mean is not None
            and box_span is not None
            and len(box_values) >= int(cfg["min_ref_samples"])
            and box_span <= float(cfg["box_stable_span_c"])
        )

        added = 0
        for analyzer_id, bucket in sorted(analyzer_samples.items()):
            cell_mean, cell_span = self._mean_and_span(bucket.get("cell_values", []))
            shell_mean, shell_span = self._mean_and_span(bucket.get("shell_values", []))

            ref_temp_c: Optional[float] = None
            ref_temp_source = "none"
            if bool(cfg["use_env_temp_as_ref_first"]) and env_valid:
                ref_temp_c = env_mean
                ref_temp_source = "env"
            elif bool(cfg["fallback_to_box_temp"]) and box_valid:
                ref_temp_c = box_mean
                ref_temp_source = "box"
            cell_valid, cell_reason = self._evaluate_temperature_fit_validity(
                channel="cell",
                raw_temp_c=cell_mean,
                paired_temp_c=shell_mean,
                ref_temp_c=ref_temp_c,
                cfg=cfg,
            )
            shell_valid, shell_reason = self._evaluate_temperature_fit_validity(
                channel="shell",
                raw_temp_c=shell_mean,
                paired_temp_c=cell_mean,
                ref_temp_c=ref_temp_c,
                cfg=cfg,
            )

            record = {
                "snapshot_time": snapshot_time,
                "timestamp": snapshot_time,
                "analyzer_id": analyzer_id,
                "analyzer_device_id": bucket.get("analyzer_device_id"),
                "temp_setpoint_c": self._as_float(getattr(point, "temp_chamber_c", None)),
                "temperature_setpoint_c": self._as_float(getattr(point, "temp_chamber_c", None)),
                "chamber_temperature_box_c": box_mean,
                "chamber_temperature_env_c": env_mean,
                "ref_temp_c": ref_temp_c,
                "ref_temp_source": ref_temp_source,
                "cell_temp_raw_c": cell_mean,
                "shell_temp_raw_c": shell_mean,
                "analyzer_cell_temp_raw_c": cell_mean,
                "analyzer_shell_temp_raw_c": shell_mean,
                "route_type": str(route_type or "").strip().lower() or "idle_before_route",
                "is_temp_calibration_snapshot": True,
                "valid_for_cell_fit": cell_valid,
                "valid_for_shell_fit": shell_valid,
                "cell_fit_gate_reason": cell_reason,
                "shell_fit_gate_reason": shell_reason,
                "snapshot_window_s": window_s,
                "env_temp_span_c": env_span,
                "box_temp_span_c": box_span,
                "cell_temp_span_c": cell_span,
                "shell_temp_span_c": shell_span,
            }
            self._temperature_calibration_records.append(record)
            added += 1
            if not cell_valid or not shell_valid:
                self.log(
                    "Temperature calibration snapshot rejected for fit: "
                    f"{analyzer_id} route={route_type} "
                    f"cell_valid={cell_valid} cell_reason={cell_reason or 'ok'} "
                    f"shell_valid={shell_valid} shell_reason={shell_reason or 'ok'} "
                    f"ref={ref_temp_c} cell={cell_mean} shell={shell_mean}"
                )

        if not added:
            self.log("Temperature calibration snapshot warning: no analyzer records were captured")
            return False
        self.log(
            "Temperature calibration snapshot saved: "
            f"records={added} route={route_type} ref_source={'env' if env_valid else 'box' if box_valid else 'none'}"
        )
        return True

    def _finalize_temperature_calibration_outputs(self) -> None:
        cfg = self._temperature_calibration_cfg()
        if not cfg["enabled"]:
            return
        if not getattr(self, "logger", None) or not getattr(self.logger, "run_dir", None):
            self.log("Temperature calibration export warning: run logger unavailable")
            return
        try:
            exported = export_temperature_compensation_artifacts(
                self.logger.run_dir,
                self._temperature_calibration_records,
                polynomial_order=int(cfg["polynomial_order"]),
                export_commands=bool(cfg["export_commands"]),
            )
            results = exported.get("results", [])
            self.log(
                "Temperature compensation export saved: "
                f"observations={len(self._temperature_calibration_records)} results={len(results)}"
            )
        except Exception as exc:
            self.log(f"Temperature calibration export warning: {exc}")

    def _maybe_run_postrun_corrected_delivery(self) -> None:
        cfg = self._effective_postrun_corrected_delivery_cfg()
        if not isinstance(cfg, dict) or not cfg.get("enabled", False):
            return
        if not getattr(self, "logger", None) or not getattr(self.logger, "run_dir", None):
            self.log("Postrun corrected delivery skipped: run logger unavailable")
            return

        run_dir = Path(self.logger.run_dir)
        config_snapshot = run_dir / "runtime_config_snapshot.json"
        if not config_snapshot.exists():
            try:
                config_snapshot.write_text(json.dumps(self.cfg, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
            except Exception as exc:
                self.log(f"Postrun corrected delivery skipped: config snapshot unavailable ({exc})")
                return

        output_dir = cfg.get("output_dir")
        if output_dir:
            target_dir = Path(str(output_dir)).resolve()
        else:
            target_dir = run_dir / f"corrected_autodelivery_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            from ..tools import run_v1_corrected_autodelivery

            short_verify_cfg = cfg.get("verify_short_run", {})
            result = run_v1_corrected_autodelivery.run_from_cli(
                run_dir=str(run_dir),
                config_path=str(config_snapshot),
                output_dir=str(target_dir),
                write_devices=bool(cfg.get("write_devices", False)),
                verify_report=bool(cfg.get("verify_report", False)),
                verification_template=str(cfg.get("verification_template") or ""),
                fallback_pressure_to_controller=bool(cfg.get("fallback_pressure_to_controller", False)),
                pressure_row_source=str(cfg.get("pressure_row_source") or "startup_calibration"),
                write_pressure_coefficients=bool(cfg.get("write_pressure_coefficients", False)),
                verify_short_run_cfg=short_verify_cfg if isinstance(short_verify_cfg, dict) else {},
            )
            short_verify_outputs = result.get("short_verify_outputs") if isinstance(result, dict) else {}
            if isinstance(short_verify_outputs, dict) and short_verify_outputs.get("skipped"):
                short_verify_status = f"skipped:{short_verify_outputs.get('reason', 'unknown')}"
            elif isinstance(short_verify_outputs, dict) and short_verify_outputs:
                short_verify_status = "ok" if bool(short_verify_outputs.get("ok", False)) else "failed"
            else:
                short_verify_status = "no"
            self.log(
                "Postrun corrected delivery finished: "
                f"report={result.get('report_path')} "
                f"write={'yes' if cfg.get('write_devices', False) else 'no'} "
                f"verify={'yes' if result.get('verify_outputs') else 'no'} "
                f"short_verify={short_verify_status}"
            )
        except Exception as exc:
            if bool(cfg.get("strict", False)):
                raise
            self.log(f"Postrun corrected delivery warning: {exc}")

    def _wait_analyzer_chamber_temp_stable(self, target_c: float) -> bool:
        enabled = bool(self._wf("workflow.stability.temperature.analyzer_chamber_temp_enabled", True))
        if not enabled:
            return True

        window_s = float(self._wf("workflow.stability.temperature.analyzer_chamber_temp_window_s", 60.0))
        span_tol_c = abs(float(self._wf("workflow.stability.temperature.analyzer_chamber_temp_span_c", 0.02)))
        timeout_raw = float(self._wf("workflow.stability.temperature.analyzer_chamber_temp_timeout_s", 5400.0))
        timeout_s: Optional[float] = timeout_raw if timeout_raw > 0 else None
        first_valid_timeout_default = 120.0 if timeout_s is None else min(timeout_s, 120.0)
        first_valid_timeout_raw = float(
            self._wf(
                "workflow.stability.temperature.analyzer_chamber_temp_first_valid_timeout_s",
                first_valid_timeout_default,
            )
        )
        first_valid_timeout_s: Optional[float] = first_valid_timeout_raw if first_valid_timeout_raw > 0 else None
        poll_s = max(0.1, float(self._wf("workflow.stability.temperature.analyzer_chamber_temp_poll_s", 1.0)))

        analyzers = self._active_gas_analyzers()
        if not analyzers:
            if self._all_gas_analyzers():
                self.log("Analyzer chamber-temp wait failed: no active analyzers remain")
                return False
            return True

        start = time.time()
        last_report = 0.0
        current_label: Optional[str] = None
        window_start: Optional[float] = None
        window_values: List[float] = []
        last_value: Optional[float] = None
        self._emit_analyzer_chamber_temp_stage(target_c, countdown_s=window_s)

        while timeout_s is None or (time.time() - start) < timeout_s:
            if self.stop_event.is_set():
                return False
            self._check_pause()
            self._refresh_live_analyzer_snapshots(reason="analyzer chamber-temp stability wait")

            selected_label: Optional[str] = None
            selected_value: Optional[float] = None
            for label, ga, _cfg in self._active_gas_analyzers():
                try:
                    _, parsed = self._read_sensor_parsed(
                        ga,
                        required_key="chamber_temp_c",
                        frame_acceptance_mode="required_key_relaxed",
                    )
                except Exception as exc:
                    self.log(f"Analyzer chamber-temp read failed: {label} err={exc}")
                    continue
                if not parsed:
                    continue
                value = self._as_float(parsed.get("chamber_temp_c"))
                if value is None:
                    continue
                selected_label = label
                selected_value = float(value)
                break

            now = time.time()
            if selected_label is None or selected_value is None:
                no_value_elapsed = now - start
                no_value_remain = (
                    max(0.0, first_valid_timeout_s - no_value_elapsed) if first_valid_timeout_s is not None else None
                )
                if first_valid_timeout_s is not None and no_value_elapsed >= first_valid_timeout_s:
                    self._emit_analyzer_chamber_temp_stage(
                        target_c,
                        countdown_s=0.0,
                        detail="未收到有效腔温",
                    )
                    self.log(
                        "Analyzer chamber temp first valid timeout: "
                        f"target={target_c:.2f} timeout={int(first_valid_timeout_raw)}s"
                    )
                    return False
                if now - last_report >= 30:
                    last_report = now
                    self._emit_analyzer_chamber_temp_stage(
                        target_c,
                        countdown_s=no_value_remain,
                        detail="等待首个有效腔温",
                    )
                    if no_value_remain is None:
                        self.log("Waiting analyzer chamber temp stability... awaiting first valid chamber_temp_c")
                    else:
                        self.log(
                            "Waiting analyzer chamber temp stability... "
                            f"awaiting first valid chamber_temp_c, remain={int(no_value_remain)}s"
                        )
                time.sleep(poll_s)
                continue

            if current_label != selected_label:
                current_label = selected_label
                window_start = now
                window_values = []
                self._emit_analyzer_chamber_temp_stage(
                    target_c,
                    analyzer_label=current_label,
                    analyzer_temp_c=selected_value,
                    countdown_s=window_s,
                )
                self.log(
                    "Analyzer chamber-temp stability source selected: "
                    f"{selected_label} target={target_c:.2f}"
                )

            if window_start is None:
                window_start = now
            last_value = selected_value
            window_values.append(selected_value)

            elapsed = now - window_start
            if elapsed >= window_s:
                span = self._span(window_values)
                if span <= span_tol_c:
                    self.log(
                        "Analyzer chamber temp stable: "
                        f"{current_label} value={selected_value:.3f} span={span:.4f} "
                        f"window={int(window_s)}s tol=±{span_tol_c:.4f}"
                    )
                    return True

                self.log(
                    "Analyzer chamber temp not stable; restart window: "
                    f"{current_label} last={selected_value:.3f} span={span:.4f} "
                    f"window={int(window_s)}s tol=±{span_tol_c:.4f}"
                )
                window_start = now
                window_values = [selected_value]
                last_report = now
                self._emit_analyzer_chamber_temp_stage(
                    target_c,
                    analyzer_label=current_label,
                    analyzer_temp_c=selected_value,
                    countdown_s=window_s,
                    detail=f"span={span:.4f}",
                )
            elif now - last_report >= 30:
                last_report = now
                remain = max(0.0, window_s - elapsed)
                span = self._span(window_values)
                self._emit_analyzer_chamber_temp_stage(
                    target_c,
                    analyzer_label=current_label,
                    analyzer_temp_c=selected_value,
                    countdown_s=remain,
                    detail=f"span={span:.4f}",
                )
                self.log(
                    "Analyzer chamber temp settling... "
                    f"{current_label} value={selected_value:.3f} "
                    f"window={int(elapsed)}/{int(window_s)}s remain={int(remain)}s"
                )

            time.sleep(poll_s)

        self.log(
            "Analyzer chamber temp stability timeout: "
            f"target={target_c:.2f} label={current_label} last={last_value} "
            f"window={int(window_s)}s tol=±{span_tol_c:.4f} timeout={int(timeout_raw)}s"
        )
        return False

    @staticmethod
    def _extract_first_float(text: Any) -> Optional[float]:
        m = re.search(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", str(text or ""))
        if not m:
            return None
        try:
            return float(m.group(0))
        except Exception:
            return None

    def _pressure_snapshot(self) -> Dict[str, Any]:
        pace = self.devices.get("pace")
        if not pace:
            return {}
        query = getattr(pace, "query", None)
        if not callable(query):
            return {}

        snap: Dict[str, Any] = {}
        commands = {
            "outp_stat": ":OUTP:STAT?",
            "outp_mode": ":OUTP:MODE?",
            "setpoint": ":SOUR:PRES:LEV:IMM:AMPL?",
            "in_limits": ":SENS:PRES:INL?",
            "effort": ":SOUR:PRES:EFF?",
            "comp": ":SOUR:PRES:COMP?",
            "comp1": ":SOUR:PRES:COMP1?",
        }
        for key, cmd in commands.items():
            try:
                snap[key] = query(cmd)
            except Exception as exc:
                snap[f"{key}_err"] = str(exc)
        return snap

    def _set_h2o_path(self, is_open: bool, point: Optional[CalibrationPoint] = None) -> None:
        self._apply_valve_states(self._h2o_open_valves(point) if is_open else [])

    def _h2o_open_valves(self, point: Optional[CalibrationPoint] = None) -> List[int]:
        valves_cfg = self.cfg.get("valves", {})
        open_list: List[int] = []
        # Water-path action must only use water-related valves (relay_8 mapping).
        # Do not open any CO2 source/path valves during H2O routing.
        for key in ("h2o_path", "hold", "flow_switch"):
            iv = self._as_int(valves_cfg.get(key))
            if iv is not None:
                open_list.append(iv)
        return open_list

    def _prepare_humidity_generator(self, point: CalibrationPoint) -> None:
        hgen = self.devices.get("humidity_gen")
        if not hgen:
            return

        target_temp = self._as_float(point.hgen_temp_c)
        target_rh = self._as_float(point.hgen_rh_pct)
        target_key = (target_temp, target_rh)
        target_changed = target_key != self._last_hgen_target
        if target_changed:
            self._last_hgen_target = target_key
            self._last_hgen_setpoint_ready = False
            self._last_hgen_dewpoint_ready = False
        hcfg = self.cfg.get("workflow", {}).get("humidity_generator", {})
        activation_verify_enabled = bool(hcfg.get("activation_verify_enabled", True))
        activation_baseline_hot_temp_c: Optional[float] = None
        activation_baseline_cold_temp_c: Optional[float] = None
        fetch_all = getattr(hgen, "fetch_all", None)
        if activation_verify_enabled and callable(fetch_all):
            try:
                baseline_snapshot = fetch_all()
                baseline_data = baseline_snapshot.get("data", {}) if isinstance(baseline_snapshot, dict) else {}
                activation_baseline_hot_temp_c = self._pick_numeric(
                    baseline_data,
                    ["Tc", "TA", "Temp", "temperature"],
                )
                activation_baseline_cold_temp_c = self._pick_numeric(
                    baseline_data,
                    ["Ts", "Tc", "TA", "Temp", "temperature"],
                )
            except Exception as exc:
                self.log(f"Humidity generator activation baseline read failed: {exc}")
        prep_state: Dict[str, Any] = {
            "target_temp_sent": "skipped",
            "target_rh_sent": "skipped",
            "ctrl_on": "not_attempted",
            "heat_on": "not_attempted",
            "cool_on": "not_attempted",
            "activation_verify": "skipped",
            "cooling_verify": "skipped",
            "errors": [],
        }

        def _record_target_send(label: str, sender, value: Optional[float]) -> None:
            if value is None or not target_changed:
                return
            self.log(f"Humidity generator {label} attempt: value={value}")
            try:
                sender(float(value))
                prep_state[label] = "yes"
                self.log(f"Humidity generator {label} ok: value={value}")
            except Exception as exc:
                prep_state[label] = "failed"
                prep_state["errors"].append(f"{label}:{exc}")
                self.log(f"Humidity generator {label} failed: {exc}")
                raise

        def _record_toggle(label: str, sender, *, fail_hard: bool) -> None:
            self.log(f"Humidity generator {label} attempt")
            try:
                sender()
                prep_state[label] = "ok"
                self.log(f"Humidity generator {label} ok")
            except Exception as exc:
                prep_state[label] = "failed"
                prep_state["errors"].append(f"{label}:{exc}")
                self.log(f"Humidity generator {label} failed: {exc}")
                if fail_hard:
                    raise

        try:
            _record_target_send("target_temp_sent", hgen.set_target_temp, target_temp)
            _record_target_send("target_rh_sent", hgen.set_target_rh, target_rh)

            # Keep the existing command order: control first, then heat, then cool.
            _record_toggle("ctrl_on", lambda: hgen.enable_control(True), fail_hard=True)
            _record_toggle("heat_on", hgen.heat_on, fail_hard=False)
            _record_toggle("cool_on", hgen.cool_on, fail_hard=False)
        finally:
            state = "updated" if target_changed else "unchanged"
            summary = (
                f"Humidity generator prepared: target_state={state} "
                f"temp={target_temp}C rh={target_rh}% "
                f"target_temp_sent={prep_state['target_temp_sent']} "
                f"target_rh_sent={prep_state['target_rh_sent']} "
                f"ctrl_on={prep_state['ctrl_on']} "
                f"heat_on={prep_state['heat_on']} "
                f"cool_on={prep_state['cool_on']} "
                f"activation_verify={prep_state['activation_verify']} "
                f"cooling_verify={prep_state['cooling_verify']}"
            )
            if prep_state["errors"]:
                summary += f" errors={prep_state['errors']}"
            self.log(summary)

        if target_changed:
            verify_readback = getattr(hgen, "verify_target_readback", None)
            if callable(verify_readback):
                try:
                    readback = verify_readback(
                        target_temp_c=target_temp,
                        target_rh_pct=target_rh,
                    )
                    self.log(
                        "Humidity generator target readback: "
                        f"temp={readback.get('read_temp_c')}C/{readback.get('target_temp_c')}C "
                        f"rh={readback.get('read_rh_pct')}%/{readback.get('target_rh_pct')}% "
                        f"ok={readback.get('ok')}"
                    )
                except Exception as exc:
                    self.log(f"Humidity generator target readback failed: {exc}")

        hcfg = self.cfg.get("workflow", {}).get("humidity_generator", {})
        if hcfg.get("ensure_run"):
            try:
                res = hgen.ensure_run(
                    min_flow_lpm=float(hcfg.get("min_flow_lpm", 0.1)),
                    tries=int(hcfg.get("tries", 2)),
                    wait_s=float(hcfg.get("wait_s", 2.5)),
                    poll_s=float(hcfg.get("poll_s", 0.25)),
                )
                if not res.get("ok"):
                    self.log(f"Humidity generator ensure_run failed: {res}")
            except Exception as exc:
                self.log(f"Humidity generator ensure_run error: {exc}")

        verify_runtime_activation = getattr(hgen, "verify_runtime_activation", None)
        need_activation_verify = activation_verify_enabled and (
            target_changed or not self._last_hgen_setpoint_ready
        )
        if need_activation_verify and callable(verify_runtime_activation):
            activation_result: Dict[str, Any]
            try:
                reference_temp_c = (
                    activation_baseline_cold_temp_c
                    if activation_baseline_cold_temp_c is not None
                    else activation_baseline_hot_temp_c
                )
                expect_cooling = bool(
                    target_temp is not None
                    and reference_temp_c is not None
                    and float(target_temp) <= float(reference_temp_c) - float(
                        hcfg.get("activation_verify_expect_cooling_margin_c", 1.0)
                    )
                )
                activation_result = verify_runtime_activation(
                    min_flow_lpm=float(
                        hcfg.get(
                            "activation_verify_min_flow_lpm",
                            hcfg.get("min_flow_lpm", 0.1),
                        )
                    ),
                    timeout_s=float(hcfg.get("activation_verify_timeout_s", 30.0)),
                    poll_s=float(hcfg.get("activation_verify_poll_s", 1.0)),
                    target_temp_c=target_temp,
                    baseline_hot_temp_c=activation_baseline_hot_temp_c,
                    baseline_cold_temp_c=activation_baseline_cold_temp_c,
                    cooling_expected=expect_cooling,
                    cooling_min_drop_c=float(hcfg.get("activation_verify_cooling_min_drop_c", 0.2)),
                    cooling_min_delta_c=float(hcfg.get("activation_verify_cooling_min_delta_c", 0.5)),
                )
            except Exception as exc:
                prep_state["activation_verify"] = "error"
                prep_state["cooling_verify"] = "error"
                prep_state["errors"].append(f"activation_verify:{exc}")
                self.log(f"Humidity generator activation verify error: {exc}")
            else:
                flow_ok = bool(activation_result.get("flow_ok"))
                cooling_expected = bool(activation_result.get("cooling_expected"))
                cooling_ok = activation_result.get("cooling_ok")
                prep_state["activation_verify"] = "ok" if flow_ok else "failed"
                if not cooling_expected:
                    prep_state["cooling_verify"] = "not_required"
                elif cooling_ok is True:
                    prep_state["cooling_verify"] = "ok"
                elif cooling_ok is False:
                    prep_state["cooling_verify"] = "pending"
                else:
                    prep_state["cooling_verify"] = "unknown"
                self.log(
                    "Humidity generator activation verify: "
                    f"flow_ok={flow_ok} flow_lpm={activation_result.get('flow_lpm')} "
                    f"cooling_expected={cooling_expected} cooling_ok={cooling_ok} "
                    f"fully_confirmed={activation_result.get('fully_confirmed')} "
                    f"Tc={activation_result.get('hot_temp_c')} Ts={activation_result.get('cold_temp_c')}"
                )
                if not flow_ok:
                    raise RuntimeError(
                        "humidity generator did not enter running state "
                        f"(flow_lpm={activation_result.get('flow_lpm')})"
                    )
                if cooling_expected and cooling_ok is False:
                    self.log(
                        "Humidity generator cooling verify not yet confirmed during startup; "
                        "continuing to setpoint wait"
                    )
        elif need_activation_verify and activation_verify_enabled:
            prep_state["activation_verify"] = "unsupported"
            prep_state["cooling_verify"] = "unsupported"
            self.log("Humidity generator activation verify skipped: device does not support runtime verification")

        if need_activation_verify:
            verify_summary = (
                "Humidity generator prepare verify summary: "
                f"activation_verify={prep_state['activation_verify']} "
                f"cooling_verify={prep_state['cooling_verify']}"
            )
            if prep_state["errors"]:
                verify_summary += f" errors={prep_state['errors']}"
            self.log(verify_summary)

    def _wait_humidity_generator_stable(self, point: CalibrationPoint) -> bool:
        hgen = self.devices.get("humidity_gen")
        if not hgen:
            return True
        target_temp = self._as_float(point.hgen_temp_c)
        target_rh = self._as_float(point.hgen_rh_pct)
        if target_temp is None and target_rh is None:
            return True
        if (
            self._last_hgen_target == (target_temp, target_rh)
            and self._last_hgen_setpoint_ready
        ):
            self.log("Humidity generator setpoint already ready for current target, skip wait")
            return True

        cfg = self.cfg.get("workflow", {}).get("stability", {}).get("humidity_generator", {})
        if cfg and not cfg.get("enabled", True):
            return True

        temp_tol = float(self._wf("workflow.stability.humidity_generator.temp_tol_c", 1.0))
        rh_tol = float(self._wf("workflow.stability.humidity_generator.rh_tol_pct", 1.0))
        rh_window_s = float(
            self._wf(
                "workflow.stability.humidity_generator.rh_stable_window_s",
                self._wf("workflow.stability.humidity_generator.window_s", 60),
            )
        )
        rh_span_tol = float(self._wf("workflow.stability.humidity_generator.rh_stable_span_pct", 0.3))
        timeout_raw = float(self._wf("workflow.stability.humidity_generator.timeout_s", 1800))
        timeout_s: Optional[float] = timeout_raw if timeout_raw > 0 else None
        poll_s = float(self._wf("workflow.stability.humidity_generator.poll_s", 1.0))
        start = time.time()
        last_report = 0.0
        in_band_since: Optional[float] = None
        rh_samples: List[Tuple[float, float]] = []

        if timeout_s is None:
            self.log("Humidity generator wait timeout disabled; waiting until RH stabilizes")

        self._emit_stage_event(
            current=f"H2O 开路等待 Tc={float(target_temp or 0.0):g}°C Uw={float(target_rh or 0.0):g}%",
            point=point,
            phase="h2o",
            wait_reason="湿度发生器判稳",
        )
        while True:
            if self.stop_event.is_set():
                return False
            self._check_pause()
            self._refresh_live_analyzer_snapshots(reason="humidity generator stability wait")

            if timeout_s is not None and (time.time() - start) >= timeout_s:
                break

            snap = hgen.fetch_all()
            data = snap.get("data", {})
            temp_now = self._pick_numeric(data, ["Tc", "Ts", "TA", "Temp", "temperature"])
            rh_now = self._pick_numeric(data, ["Uw", "Ui", "Rh", "RH", "humidity", "Hum"])

            temp_ok = (
                target_temp is None
                or (temp_now is not None and abs(temp_now - target_temp) <= temp_tol)
            )
            rh_ok = (
                target_rh is None
                or (rh_now is not None and abs(rh_now - target_rh) <= rh_tol)
            )

            if temp_ok and rh_ok:
                now = time.time()
                if in_band_since is None:
                    in_band_since = now
                    rh_samples = []
                if rh_now is not None:
                    rh_samples.append((now, float(rh_now)))
                    rh_samples = [(ts, value) for ts, value in rh_samples if now - ts <= rh_window_s]

                if (
                    in_band_since is not None
                    and (now - in_band_since) >= rh_window_s
                    and rh_samples
                    and self._span([value for _, value in rh_samples]) < rh_span_tol
                ):
                    span = self._span([value for _, value in rh_samples])
                    self.log(
                        f"Humidity generator reached setpoint: temp={temp_now}C target={target_temp} "
                        f"rh={rh_now}% target={target_rh} span={span:.3f} window={int(rh_window_s)}s"
                    )
                    self._last_hgen_setpoint_ready = True
                    return True
            else:
                if in_band_since is not None:
                    self.log(
                        f"Humidity left target band: temp={temp_now}C/{target_temp} tol=±{temp_tol} "
                        f"rh={rh_now}%/{target_rh} tol=±{rh_tol}; reset stability window"
                    )
                in_band_since = None
                rh_samples = []

            if time.time() - last_report >= 30:
                last_report = time.time()
                if in_band_since is None or not rh_samples:
                    self._emit_stage_event(
                        current=f"H2O 开路等待 Tc={float(temp_now or 0.0):.1f}°C Uw={float(rh_now or 0.0):.1f}%",
                        point=point,
                        phase="h2o",
                        wait_reason="湿度发生器判稳",
                    )
                    self.log(
                        f"Humidity settling... temp={temp_now}C/{target_temp} rh={rh_now}%/{target_rh} "
                        f"window=0/{int(rh_window_s)}s"
                    )
                else:
                    remain = max(0.0, rh_window_s - (time.time() - in_band_since))
                    span = self._span([value for _, value in rh_samples])
                    self._emit_stage_event(
                        current=f"H2O 开路等待 Tc={float(temp_now or 0.0):.1f}°C Uw={float(rh_now or 0.0):.1f}%",
                        point=point,
                        phase="h2o",
                        wait_reason="湿度发生器判稳",
                        countdown_s=remain,
                        detail=f"span={span:.3f}",
                    )
                    self.log(
                        f"Humidity in target band, observing stability... temp={temp_now}C/{target_temp} "
                        f"rh={rh_now}%/{target_rh} span={span:.3f} remaining={int(remain)}s"
                    )

            time.sleep(max(0.1, poll_s))

        self.log("Humidity generator reach-setpoint timeout")
        return False

    def _wait_humidity_generator_dewpoint_stable(self) -> bool:
        hgen = self.devices.get("humidity_gen")
        if not hgen:
            return True
        if self._last_hgen_dewpoint_ready:
            self.log("Humidity generator dewpoint already stable for current target, skip wait")
            return True

        cfg = self.cfg.get("workflow", {}).get("stability", {}).get("humidity_generator", {})
        if cfg and not cfg.get("enabled", True):
            return True

        tol = float(cfg.get("dewpoint_tol_c", 0.2))
        window_s = float(cfg.get("dewpoint_window_s", cfg.get("window_s", 60)))
        timeout_raw = float(cfg.get("dewpoint_timeout_s", cfg.get("timeout_s", 1800)))
        timeout_s: Optional[float] = timeout_raw if timeout_raw > 0 else None
        poll_s = float(cfg.get("poll_s", 1.0))

        stab = StabilityWindow(tol, window_s)
        start = time.time()
        last_report = 0.0
        last_dp: Optional[float] = None

        if timeout_s is None:
            self.log("Humidity generator dewpoint timeout disabled; waiting until Td stabilizes")

        while True:
            if self.stop_event.is_set():
                return False
            self._check_pause()
            self._refresh_live_analyzer_snapshots(reason="humidity generator dewpoint wait")

            if timeout_s is not None and (time.time() - start) >= timeout_s:
                break

            snap = hgen.fetch_all()
            data = snap.get("data", {})
            dp = self._pick_numeric(data, ["Td", "DP", "dewpoint", "DewPoint"])
            if dp is not None:
                last_dp = float(dp)
                stab.add(last_dp)
                if stab.is_stable():
                    self.log(f"Humidity generator dewpoint stable: Td={last_dp}")
                    self._last_hgen_dewpoint_ready = True
                    return True

            if time.time() - last_report >= 30:
                last_report = time.time()
                self.log(f"Humidity generator dewpoint settling... Td={last_dp}")

            time.sleep(max(0.1, poll_s))

        self.log(f"Humidity generator dewpoint stability timeout (last Td={last_dp})")
        return False

    def _set_pressure_to_target(self, point: CalibrationPoint, *, recovery_attempted: bool = False) -> bool:
        pace = self.devices.get("pace")
        if not pace:
            return True
        if point.target_pressure_hpa is None:
            self.log("Missing target pressure, skipping pressure control.")
            return False

        target = float(point.target_pressure_hpa)
        phase = "h2o" if point.is_h2o_point else "co2"
        self._emit_stage_event(
            current=self._stage_label_for_point(point, phase=phase),
            point=point,
            phase=phase,
            wait_reason="控压中",
        )
        prepared = (
            (not recovery_attempted)
            and
            point.is_h2o_point
            and self._h2o_pressure_prepared_target is not None
            and abs(float(self._h2o_pressure_prepared_target) - target) <= 1e-9
        )
        preseal_ready_state: Optional[Dict[str, Any]] = None
        preseal_reuse_reject_reason = ""
        if not prepared and not recovery_attempted:
            preseal_ready_state, preseal_reuse_reject_reason = self._matching_preseal_pressure_control_ready_state(
                point,
                phase=phase,
            )

        self._append_pressure_trace_row(
            point=point,
            route=phase,
            point_phase=phase,
            trace_stage="control_prepare_begin",
            pressure_target_hpa=target,
            refresh_pace_state=False,
            note="before pressure controller ready-for-control check",
        )

        if prepared:
            self.log(f"Pressure target already prepared for H2O point: {target} hPa")
            if not self._ensure_pressure_controller_ready_for_control(
                point,
                phase=phase,
                pressure_target_hpa=target,
                note="prepared target path",
            ):
                return False
            self._append_pressure_trace_row(
                point=point,
                route=phase,
                point_phase=phase,
                trace_stage="control_output_on_begin",
                pressure_target_hpa=target,
                read_pace_pressure=True,
                read_pressure_gauge=True,
                note="before output on using prepared setpoint",
            )
            if not self._enable_pressure_controller_output(reason="using prepared setpoint"):
                self._append_pressure_trace_row(
                    point=point,
                    route=phase,
                    point_phase=phase,
                    trace_stage="control_output_on_failed",
                    pressure_target_hpa=target,
                    read_pace_pressure=True,
                    read_pressure_gauge=True,
                    note="output enable command failed",
                )
                return False
            self._append_pressure_trace_row(
                point=point,
                route=phase,
                point_phase=phase,
                trace_stage="control_output_on_command_sent",
                pressure_target_hpa=target,
                refresh_pace_state=False,
                note="output enable command sent using prepared setpoint",
            )
            if not self._verify_pressure_controller_output_on(
                point,
                phase=phase,
                pressure_target_hpa=target,
                note="after output on using prepared setpoint",
            ):
                return False
        else:
            ready_for_control = False
            reused_preseal_ready = False
            if not preseal_ready_state and preseal_reuse_reject_reason:
                self._append_pressure_trace_row(
                    point=point,
                    route=phase,
                    point_phase=phase,
                    trace_stage="control_preseal_ready_reuse_skipped",
                    pressure_target_hpa=target,
                    refresh_pace_state=False,
                    note=preseal_reuse_reject_reason,
                )
            if preseal_ready_state:
                preseal_failures = list(preseal_ready_state.get("failures") or [])
                ready_verification_pending = bool(preseal_ready_state.get("ready_verification_pending"))
                recorded_wall_ts = self._as_float(preseal_ready_state.get("recorded_wall_ts"))
                snapshot_age_s = None if recorded_wall_ts is None else max(0.0, time.time() - recorded_wall_ts)
                target_delta_hpa = None
                snapshot_target = self._as_float(preseal_ready_state.get("target_pressure_hpa"))
                if snapshot_target is not None:
                    target_delta_hpa = abs(float(target) - float(snapshot_target))
                self._append_pressure_trace_row(
                    point=point,
                    route=phase,
                    point_phase=phase,
                    trace_stage="control_preseal_ready_reuse_begin",
                    pressure_target_hpa=target,
                    refresh_pace_state=False,
                    note=(
                        (
                            f"reuse preseal vent-off state age_s={snapshot_age_s:.3f} "
                            f"target_delta_hpa={target_delta_hpa if target_delta_hpa is not None else 'NA'}"
                        )
                        + (" live_ready_check=deferred" if ready_verification_pending else "")
                        if not preseal_failures
                        else f"preseal snapshot had failures: {','.join(preseal_failures)}"
                    ),
                )
                if not preseal_failures and ready_verification_pending:
                    ready_for_control = self._ensure_pressure_controller_ready_for_control(
                        point,
                        phase=phase,
                        pressure_target_hpa=target,
                        note="reused preseal vent-off state; deferred live ready check before setpoint",
                    )
                    if not ready_for_control:
                        return False
                    reused_preseal_ready = True
                elif not preseal_failures:
                    reused_preseal_ready = True
                    ready_for_control = True
                    self._append_pressure_trace_row(
                        point=point,
                        route=phase,
                        point_phase=phase,
                        trace_stage="control_ready_verified",
                        pressure_target_hpa=target,
                        pace_output_state=preseal_ready_state.get("pace_output_state"),
                        pace_isolation_state=preseal_ready_state.get("pace_isolation_state"),
                        pace_vent_status=preseal_ready_state.get("pace_vent_status"),
                        refresh_pace_state=False,
                        note="reused preseal ready snapshot without live re-check",
                    )

            if preseal_ready_state and not ready_for_control:
                self._append_pressure_trace_row(
                    point=point,
                    route=phase,
                    point_phase=phase,
                    trace_stage="control_vent_off_begin",
                    pressure_target_hpa=target,
                    refresh_pace_state=False,
                    note="before conservative vent off recovery for control",
                )
                try:
                    vent_off_ok = self._set_pressure_controller_vent(False, reason="before setpoint control")
                except Exception as exc:
                    self._append_pressure_trace_row(
                        point=point,
                        route=phase,
                        point_phase=phase,
                        trace_stage="control_vent_off_failed",
                        pressure_target_hpa=target,
                        read_pace_pressure=True,
                        read_pressure_gauge=True,
                        note=str(exc),
                    )
                    return False
                if vent_off_ok is False:
                    self._append_pressure_trace_row(
                        point=point,
                        route=phase,
                        point_phase=phase,
                        trace_stage="control_vent_off_failed",
                        pressure_target_hpa=target,
                        read_pace_pressure=True,
                        read_pressure_gauge=True,
                        note="vent off command returned False before setpoint control",
                    )
                    return False
                if self._pressure_atmosphere_hold_strategy == "vent_valve_open_after_vent":
                    self._append_pressure_trace_row(
                        point=point,
                        route=phase,
                        point_phase=phase,
                        trace_stage="control_vent_after_valve_closed",
                        pressure_target_hpa=target,
                        read_pace_pressure=True,
                        read_pressure_gauge=True,
                        note="vent-after-valve restored to CLOSED before control",
                    )
                ready_for_control = self._ensure_pressure_controller_ready_for_control(
                    point,
                    phase=phase,
                    pressure_target_hpa=target,
                    attempt_recovery=False,
                    note="after vent off before setpoint",
                )
                if not ready_for_control:
                    return False
            elif not preseal_ready_state:
                self._append_pressure_trace_row(
                    point=point,
                    route=phase,
                    point_phase=phase,
                    trace_stage="control_vent_off_begin",
                    pressure_target_hpa=target,
                    refresh_pace_state=False,
                    note="before vent off command for control",
                )
                try:
                    vent_off_ok = self._set_pressure_controller_vent(False, reason="before setpoint control")
                except Exception as exc:
                    self._append_pressure_trace_row(
                        point=point,
                        route=phase,
                        point_phase=phase,
                        trace_stage="control_vent_off_failed",
                        pressure_target_hpa=target,
                        read_pace_pressure=True,
                        read_pressure_gauge=True,
                        note=str(exc),
                    )
                    return False
                if vent_off_ok is False:
                    self._append_pressure_trace_row(
                        point=point,
                        route=phase,
                        point_phase=phase,
                        trace_stage="control_vent_off_failed",
                        pressure_target_hpa=target,
                        read_pace_pressure=True,
                        read_pressure_gauge=True,
                        note="vent off command returned False before setpoint control",
                    )
                    return False
                if self._pressure_atmosphere_hold_strategy == "vent_valve_open_after_vent":
                    self._append_pressure_trace_row(
                        point=point,
                        route=phase,
                        point_phase=phase,
                        trace_stage="control_vent_after_valve_closed",
                        pressure_target_hpa=target,
                        read_pace_pressure=True,
                        read_pressure_gauge=True,
                        note="vent-after-valve restored to CLOSED before control",
                    )
                if not self._ensure_pressure_controller_ready_for_control(
                    point,
                    phase=phase,
                    pressure_target_hpa=target,
                    note="after vent off before setpoint",
                ):
                    return False

            self._append_pressure_trace_row(
                point=point,
                route=phase,
                point_phase=phase,
                trace_stage="control_vent_off_verified",
                pressure_target_hpa=target,
                refresh_pace_state=False,
                note=(
                    "reused preseal vent-off state; controller ready before setpoint"
                    if reused_preseal_ready
                    else "vent off command completed; controller ready before setpoint"
                ),
            )
            pace.set_setpoint(target)
            self._append_pressure_trace_row(
                point=point,
                route=phase,
                point_phase=phase,
                trace_stage="control_output_on_begin",
                pressure_target_hpa=target,
                refresh_pace_state=False,
                note="before output on after setpoint update",
            )
            if not self._enable_pressure_controller_output(reason="after setpoint update"):
                self._append_pressure_trace_row(
                    point=point,
                    route=phase,
                    point_phase=phase,
                    trace_stage="control_output_on_failed",
                    pressure_target_hpa=target,
                    read_pace_pressure=True,
                    read_pressure_gauge=True,
                    note="output enable command failed",
                )
                return False
            self._append_pressure_trace_row(
                point=point,
                route=phase,
                point_phase=phase,
                trace_stage="control_output_on_command_sent",
                pressure_target_hpa=target,
                refresh_pace_state=False,
                note="output enable command sent after setpoint update",
            )
            if not self._verify_pressure_controller_output_on(
                point,
                phase=phase,
                pressure_target_hpa=target,
                note="after output on after setpoint update",
            ):
                return False

        self._clear_preseal_pressure_control_ready_state(
            reason="control_sequence_completed",
            point=point,
            phase=phase,
        )

        timeout_s = float(self._wf("workflow.pressure.stabilize_timeout_s", 120))
        retry_count = int(self._wf("workflow.pressure.restabilize_retries", 2))
        adaptive_sampling_enabled = bool(self._wf("workflow.pressure.adaptive_pressure_sampling_enabled", False))
        retry_interval_s = float(self._wf("workflow.pressure.restabilize_retry_interval_s", 10))
        trace_poll_s = self._pressure_trace_poll_s(point)
        start = time.time()
        next_retry_at = start + retry_interval_s
        next_aux_read_at = start + self._pressure_control_wait_aux_interval_s()
        retries_done = 0
        last_pressure_now: Optional[float] = None
        closest_pressure_now: Optional[float] = None
        closest_error_hpa: Optional[float] = None
        while time.time() - start < timeout_s:
            if self.stop_event.is_set():
                return False
            self._check_pause()
            pressure_now, inl = pace.get_in_limits()
            pressure_value: Optional[float] = None
            if pressure_now is not None:
                try:
                    pressure_value = float(pressure_now)
                except Exception:
                    pressure_value = None
                if pressure_value is not None and math.isfinite(pressure_value):
                    last_pressure_now = pressure_value
                    error_hpa = abs(pressure_value - target)
                    if closest_error_hpa is None or error_hpa < closest_error_hpa:
                        closest_error_hpa = error_hpa
                        closest_pressure_now = pressure_value
            loop_now = time.time()
            read_aux_now = loop_now >= next_aux_read_at
            if read_aux_now:
                next_aux_read_at = loop_now + self._pressure_control_wait_aux_interval_s()
            self._append_pressure_trace_row(
                point=point,
                route=phase,
                point_phase=phase,
                trace_stage="pressure_control_wait",
                pressure_target_hpa=target,
                pace_pressure_hpa=pressure_value,
                read_pressure_gauge=read_aux_now,
                read_dewpoint=read_aux_now,
                note=f"pace_in_limits={inl}",
            )
            if inl == 1:
                self._emit_stage_event(
                    current=self._stage_label_for_point(point, phase=phase),
                    point=point,
                    phase=phase,
                    wait_reason="压力已达稳",
                )
                self._append_pressure_trace_row(
                    point=point,
                    route=phase,
                    point_phase=phase,
                    trace_stage="pressure_in_limits",
                    pressure_target_hpa=target,
                    pace_pressure_hpa=pressure_value,
                    read_pressure_gauge=True,
                    read_dewpoint=True,
                    note=f"pace_in_limits={inl}",
                )
                self.log(f"Pressure in-limits at target {target} hPa")
                return True
            now = loop_now
            if not adaptive_sampling_enabled and not prepared and retries_done < retry_count and now >= next_retry_at:
                retries_done += 1
                self.log(
                    f"Pressure not stable yet at {target} hPa; "
                    f"re-apply setpoint ({retries_done}/{retry_count})"
                )
                pace.set_setpoint(target)
                self._append_pressure_trace_row(
                    point=point,
                    route=phase,
                    point_phase=phase,
                    trace_stage="control_output_on_begin",
                    pressure_target_hpa=target,
                    read_pace_pressure=True,
                    read_pressure_gauge=True,
                    note="before output on after setpoint re-apply",
                )
                if not self._enable_pressure_controller_output(reason="after setpoint re-apply"):
                    self._append_pressure_trace_row(
                        point=point,
                        route=phase,
                        point_phase=phase,
                        trace_stage="control_output_on_failed",
                        pressure_target_hpa=target,
                        read_pace_pressure=True,
                        read_pressure_gauge=True,
                        note="output enable command failed",
                    )
                    return False
                if not self._verify_pressure_controller_output_on(
                    point,
                    phase=phase,
                    pressure_target_hpa=target,
                    note="after output on after setpoint re-apply",
                ):
                    return False
                next_retry_at = now + retry_interval_s
            time.sleep(trace_poll_s)
        self.log(f"Pressure stabilize timeout at target {target} hPa")
        snap = self._pressure_snapshot()
        if snap:
            self.log(
                "Pressure timeout diag: "
                f"out={snap.get('outp_stat')} mode={snap.get('outp_mode')} "
                f"set={snap.get('setpoint')} inl={snap.get('in_limits')} "
                f"eff={snap.get('effort')} comp1={snap.get('comp1')}"
            )
            comp1 = self._extract_first_float(snap.get("comp1"))
            if comp1 is not None and target > comp1 + 5.0:
                self.log(
                    f"Pressure source may be insufficient: target={target:.1f} hPa, "
                    f"source_comp1={comp1:.1f} hPa"
                )
        if closest_error_hpa is not None and closest_pressure_now is not None:
            last_text = f"{last_pressure_now:.3f} hPa" if last_pressure_now is not None else "unavailable"
            self.log(
                "Pressure timeout path: "
                f"closest={closest_pressure_now:.3f} hPa "
                f"(error={closest_error_hpa:.3f} hPa), "
                f"last={last_text}"
            )
            if (
                last_pressure_now is not None
                and closest_error_hpa <= max(2.0, target * 0.002)
                and abs(last_pressure_now - target) >= closest_error_hpa + 10.0
            ):
                self.log("Pressure timeout pattern: target vicinity reached, then drifted away while sealed")
        if not recovery_attempted and bool(self._wf("workflow.pressure.soft_recover_on_pressure_timeout", True)):
            self.log(
                f"Pressure target {target} hPa did not stabilize; "
                "attempt pressure-controller soft recovery and retry once"
            )
            if self._soft_recover_pressure_controller(reason=f"pressure timeout @ {target} hPa"):
                return self._set_pressure_to_target(point, recovery_attempted=True)
        return False

    def _soft_recover_pressure_controller(self, *, reason: str = "") -> bool:
        pace = self.devices.get("pace")
        if not pace:
            return False

        pcfg = self.cfg.get("workflow", {}).get("pressure", {})
        device_cfg = self.cfg.get("devices", {}).get("pressure_controller", {})
        transition_timeout_s = float(
            pcfg.get("vent_transition_timeout_s", max(5.0, float(pcfg.get("vent_time_s", 0) or 0)))
        )
        extra = f" ({reason})" if reason else ""
        self.log(f"Pressure controller soft recovery start{extra}")
        ok = True
        self._h2o_pressure_prepared_target = None

        try:
            stop_hold = getattr(pace, "stop_atmosphere_hold", None)
            if callable(stop_hold):
                stop_hold()
        except Exception as exc:
            ok = False
            self.log(f"Pressure controller soft recovery hold-stop failed: {exc}")

        try:
            set_output = getattr(pace, "set_output", None)
            if callable(set_output):
                set_output(False)
        except Exception as exc:
            ok = False
            self.log(f"Pressure controller soft recovery output-off failed: {exc}")

        try:
            close_fn = getattr(pace, "close", None)
            open_fn = getattr(pace, "open", None)
            if callable(close_fn) and callable(open_fn):
                close_fn()
                time.sleep(float(self._wf("workflow.pressure.soft_recover_reopen_delay_s", 1.0)))
                open_fn()
                self.log("Pressure controller soft recovery reopen ok")
        except Exception as exc:
            ok = False
            self.log(f"Pressure controller soft recovery reopen failed: {exc}")

        try:
            set_units_hpa = getattr(pace, "set_units_hpa", None)
            if callable(set_units_hpa):
                set_units_hpa()
        except Exception as exc:
            ok = False
            self.log(f"Pressure controller soft recovery set-units failed: {exc}")

        try:
            set_in_limits = getattr(pace, "set_in_limits", None)
            if callable(set_in_limits):
                set_in_limits(
                    float(device_cfg.get("in_limits_pct", 0.02)),
                    float(device_cfg.get("in_limits_time_s", 10)),
                )
        except Exception as exc:
            ok = False
            self.log(f"Pressure controller soft recovery set in-limits failed: {exc}")

        try:
            self._set_pressure_controller_vent(True, reason="soft recovery atmosphere reset")
        except Exception as exc:
            ok = False
            self.log(f"Pressure controller soft recovery atmosphere-reset failed: {exc}")

        if ok:
            self.log("Pressure controller soft recovery complete")
        else:
            self.log("Pressure controller soft recovery finished with errors")
        return ok

    def _prepare_pressure_for_h2o(self, point: CalibrationPoint) -> None:
        pace = self.devices.get("pace")
        if not pace:
            return
        try:
            self._set_pressure_controller_vent(False, reason="H2O idle precondition")
            self._h2o_pressure_prepared_target = None
            self.log("Pressure controller isolated from atmosphere while waiting for H2O route conditioning")
        except Exception as exc:
            self._h2o_pressure_prepared_target = None
            self.log(f"H2O pressure precondition failed: {exc}")

    def _pressure_controller_hold_thread_active(self, pace: Any) -> bool:
        checker = getattr(pace, "is_atmosphere_hold_active", None)
        if callable(checker):
            try:
                return bool(checker())
            except Exception:
                pass
        thread = getattr(pace, "_vent_hold_thread", None)
        return bool(thread is not None and getattr(thread, "is_alive", lambda: False)())

    def _stop_pressure_controller_atmosphere_hold(self, pace: Any, *, reason: str = "") -> bool:
        stop_hold = getattr(pace, "stop_atmosphere_hold", None)
        if callable(stop_hold):
            try:
                result = stop_hold()
                if result is False:
                    self.log(f"Pressure controller atmosphere hold stop failed ({reason or 'no reason'}): join timeout")
                    return False
            except Exception as exc:
                self.log(f"Pressure controller atmosphere hold stop failed ({reason or 'no reason'}): {exc}")
                return False
        if self._pressure_controller_hold_thread_active(pace):
            self.log(f"Pressure controller atmosphere hold still active ({reason or 'no reason'})")
            return False
        return True

    def _clear_preseal_pressure_control_ready_state(
        self,
        *,
        reason: str = "",
        point: Optional[CalibrationPoint] = None,
        phase: str = "",
    ) -> None:
        prior_state = dict(self._preseal_pressure_control_ready_state or {})
        self._preseal_pressure_control_ready_state = None
        if reason or prior_state:
            invalidation_phase = str(phase or prior_state.get("phase") or "").strip().lower()
            invalidation_point_row = self._as_int(
                getattr(point, "index", None) if point is not None else prior_state.get("point_row")
            )
            self._last_preseal_pressure_control_ready_invalidation = {
                "reason": str(reason or "cleared"),
                "phase": invalidation_phase,
                "point_row": invalidation_point_row,
                "target_pressure_hpa": self._as_float(
                    getattr(point, "target_pressure_hpa", None) if point is not None else prior_state.get("target_pressure_hpa")
                ),
                "ts": time.time(),
            }

    def _record_preseal_pressure_control_ready_state(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        defer_live_check: bool = False,
    ) -> None:
        pace = self.devices.get("pace")
        if not pace:
            self._clear_preseal_pressure_control_ready_state(
                reason="preseal_ready_snapshot_no_pace",
                point=point,
                phase=phase,
            )
            return
        if defer_live_check:
            snapshot = self._pressure_controller_ready_snapshot(
                pace,
                refresh_state=False,
                refresh_aux=False,
            )
        else:
            snapshot = self._pressure_controller_ready_snapshot(pace)
        snapshot.update(
            {
                "phase": str(phase or "").strip().lower(),
                "point_row": int(point.index),
                "target_pressure_hpa": self._as_float(getattr(point, "target_pressure_hpa", None)),
                "recorded_wall_ts": time.time(),
                "route_sealed": True,
                "atmosphere_hold_stopped": not bool(snapshot.get("hold_thread_active")),
                "ready_verification_pending": bool(defer_live_check),
            }
        )
        snapshot["failures"] = [] if defer_live_check else self._pressure_controller_ready_failures(snapshot, pace)
        self._preseal_pressure_control_ready_state = snapshot
        self._last_preseal_pressure_control_ready_invalidation = None

    def _preseal_ready_snapshot_max_age_s(self) -> float:
        return max(0.0, float(self._wf("workflow.pressure.preseal_ready_snapshot_max_age_s", 6.0) or 6.0))

    def _preseal_ready_target_tolerance_hpa(self) -> float:
        return abs(float(self._wf("workflow.pressure.preseal_ready_target_tolerance_hpa", 0.5) or 0.5))

    def _matching_preseal_pressure_control_ready_state(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
    ) -> Tuple[Optional[Dict[str, Any]], str]:
        state = dict(self._preseal_pressure_control_ready_state or {})
        if not state:
            invalidation = dict(self._last_preseal_pressure_control_ready_invalidation or {})
            same_phase = str(invalidation.get("phase") or "") == str(phase or "").strip().lower()
            same_point = self._as_int(invalidation.get("point_row")) == int(point.index)
            if invalidation and same_phase and same_point:
                return None, f"snapshot_invalidated:{invalidation.get('reason') or 'unknown'}"
            return None, "snapshot_missing"
        phase_text = str(phase or "").strip().lower()
        if str(state.get("phase") or "") != phase_text:
            return None, "phase_mismatch"
        if self._as_int(state.get("point_row")) != int(point.index):
            return None, "point_row_mismatch"
        target_pressure_hpa = self._as_float(getattr(point, "target_pressure_hpa", None))
        snapshot_target_hpa = self._as_float(state.get("target_pressure_hpa"))
        target_delta_hpa = None
        if target_pressure_hpa is not None and snapshot_target_hpa is not None:
            target_delta_hpa = abs(target_pressure_hpa - snapshot_target_hpa)
        if (
            target_delta_hpa is not None
            and target_delta_hpa > self._preseal_ready_target_tolerance_hpa()
        ):
            return None, f"target_pressure_mismatch:{target_delta_hpa:.3f}hPa"
        recorded_wall_ts = self._as_float(state.get("recorded_wall_ts"))
        age_s = None if recorded_wall_ts is None else max(0.0, time.time() - recorded_wall_ts)
        if age_s is not None and age_s > self._preseal_ready_snapshot_max_age_s():
            return None, f"snapshot_age_exceeded:{age_s:.3f}s"
        if state.get("route_sealed") is not True:
            return None, "snapshot_not_route_sealed"
        if state.get("atmosphere_hold_stopped") is not True:
            return None, "atmosphere_hold_not_stopped"
        return state, ""

    def _pressure_controller_ready_snapshot_requires_aux_refresh(self) -> bool:
        return self._pressure_atmosphere_hold_strategy == "vent_valve_open_after_vent"

    def _pressure_controller_ready_snapshot(
        self,
        pace: Any,
        *,
        refresh_state: bool = True,
        refresh_aux: Optional[bool] = None,
    ) -> Dict[str, Any]:
        snapshot = self._pace_state_snapshot(pace, refresh=refresh_state)
        snapshot["hold_thread_active"] = self._pressure_controller_hold_thread_active(pace)
        should_refresh_aux = (
            self._pressure_controller_ready_snapshot_requires_aux_refresh()
            if refresh_aux is None
            else bool(refresh_aux)
        )
        if should_refresh_aux:
            self._refresh_pressure_controller_aux_state(pace)
        snapshot["vent_after_valve_open"] = self._pace_vent_after_valve_open
        snapshot["vent_popup_ack_enabled"] = self._pace_vent_popup_ack_enabled
        snapshot["vent_after_valve_supported"] = self._pace_vent_after_valve_supported
        snapshot["atmosphere_hold_strategy"] = self._pressure_atmosphere_hold_strategy
        return snapshot

    def _pressure_control_ready_wait_timeout_s(self) -> float:
        return max(0.0, float(self._wf("workflow.pressure.control_ready_wait_timeout_s", 2.0) or 2.0))

    def _pressure_control_ready_wait_poll_s(self) -> float:
        return max(0.05, float(self._wf("workflow.pressure.control_ready_wait_poll_s", 0.1) or 0.1))

    def _pressure_output_on_verify_timeout_s(self) -> float:
        return max(0.0, float(self._wf("workflow.pressure.output_on_verify_timeout_s", 2.0) or 2.0))

    def _pressure_output_on_verify_poll_s(self) -> float:
        return max(0.05, float(self._wf("workflow.pressure.output_on_verify_poll_s", 0.1) or 0.1))

    def _pressure_output_on_recovery_retries(self) -> int:
        return max(0, int(self._wf("workflow.pressure.output_on_recovery_retries", 1) or 1))

    def _pressure_output_on_recovery_requires_trapped(self) -> bool:
        return bool(self._wf("workflow.pressure.output_on_recovery_requires_trapped", True))

    def _pace_vent_status_allows_control(self, pace: Any, vent_status: Any) -> bool:
        status_value = self._as_int(vent_status)
        if status_value is None:
            return False
        checker = getattr(pace, "vent_status_allows_control", None)
        if callable(checker):
            try:
                return bool(checker(status_value))
            except Exception:
                pass
        return status_value == 0

    def _pace_trapped_pressure_allows_control(self, pace: Any, vent_status: Any) -> bool:
        status_value = self._as_int(vent_status)
        trapped_pressure_status = self._as_int(getattr(pace, "VENT_STATUS_TRAPPED_PRESSURE", 3))
        if status_value is None or trapped_pressure_status is None or status_value != trapped_pressure_status:
            return False
        return self._pace_vent_status_allows_control(pace, status_value)

    def _pressure_controller_ready_failures(self, snapshot: Dict[str, Any], pace: Any = None) -> List[str]:
        failures: List[str] = []
        vent_status = self._as_int(snapshot.get("pace_vent_status"))
        output_state = self._as_int(snapshot.get("pace_output_state"))
        isolation_state = self._as_int(snapshot.get("pace_isolation_state"))
        hold_thread_active = bool(snapshot.get("hold_thread_active"))
        vent_after_valve_open = snapshot.get("vent_after_valve_open")
        atmosphere_hold_strategy = str(
            snapshot.get("atmosphere_hold_strategy") or self._pressure_atmosphere_hold_strategy or ""
        ).strip()
        trapped_pressure_status = self._as_int(getattr(pace, "VENT_STATUS_TRAPPED_PRESSURE", 3))
        if hold_thread_active:
            failures.append("atmosphere_hold_active")
        if atmosphere_hold_strategy == "vent_valve_open_after_vent" and vent_after_valve_open is True:
            failures.append("vent_after_valve_open")
        if vent_status is None:
            failures.append("vent_status_unavailable")
        elif (
            trapped_pressure_status is not None
            and vent_status == trapped_pressure_status
            and not self._pace_trapped_pressure_allows_control(pace, vent_status)
        ):
            failures.append(f"vent_status={vent_status}(trapped_pressure)")
        elif not self._pace_vent_status_allows_control(pace, vent_status):
            failures.append(f"vent_status={vent_status}")
        if output_state is None:
            failures.append("output_state_unavailable")
        elif output_state != 0:
            failures.append(f"output_state={output_state}")
        if isolation_state is None:
            failures.append("isolation_state_unavailable")
        elif isolation_state != 1:
            failures.append(f"isolation_state={isolation_state}")
        return failures

    def _ensure_pressure_controller_ready_for_control(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        pressure_target_hpa: Optional[float],
        attempt_recovery: bool = True,
        note: str = "",
    ) -> bool:
        pace = self.devices.get("pace")
        if not pace:
            return True

        snapshot = self._pressure_controller_ready_snapshot(pace)
        failures = self._pressure_controller_ready_failures(snapshot, pace)
        self._append_pressure_trace_row(
            point=point,
            route=phase,
            point_phase=phase,
            trace_stage="control_ready_snapshot_acquired",
            pressure_target_hpa=pressure_target_hpa,
            pace_output_state=snapshot.get("pace_output_state"),
            pace_isolation_state=snapshot.get("pace_isolation_state"),
            pace_vent_status=snapshot.get("pace_vent_status"),
            refresh_pace_state=False,
            note=note if not failures else f"{note}; failures={','.join(failures)}",
        )
        wait_timeout_s = self._pressure_control_ready_wait_timeout_s()
        wait_poll_s = self._pressure_control_ready_wait_poll_s()
        wait_iterations = 0
        if failures and wait_timeout_s > 0:
            wait_start_ts = time.time()
            self._append_pressure_trace_row(
                point=point,
                route=phase,
                point_phase=phase,
                trace_stage="control_ready_wait_begin",
                pressure_target_hpa=pressure_target_hpa,
                pace_output_state=snapshot.get("pace_output_state"),
                pace_isolation_state=snapshot.get("pace_isolation_state"),
                pace_vent_status=snapshot.get("pace_vent_status"),
                refresh_pace_state=False,
                event_ts=wait_start_ts,
                note=f"timeout_s={wait_timeout_s:.3f} poll_s={wait_poll_s:.3f} failures={','.join(failures)}",
            )
            wait_deadline = time.time() + wait_timeout_s
            while failures and time.time() < wait_deadline:
                time.sleep(wait_poll_s)
                wait_iterations += 1
                snapshot = self._pressure_controller_ready_snapshot(pace)
                failures = self._pressure_controller_ready_failures(snapshot, pace)
            self._append_pressure_trace_row(
                point=point,
                route=phase,
                point_phase=phase,
                trace_stage="control_ready_wait_end",
                pressure_target_hpa=pressure_target_hpa,
                pace_output_state=snapshot.get("pace_output_state"),
                pace_isolation_state=snapshot.get("pace_isolation_state"),
                pace_vent_status=snapshot.get("pace_vent_status"),
                refresh_pace_state=False,
                note=(
                    f"iterations={wait_iterations} result=ready"
                    if not failures
                    else f"iterations={wait_iterations} remaining_failures={','.join(failures)}"
                ),
            )
        if not failures and not self._strict_control_ready_check_enabled():
            return True
        if not failures:
            self._append_pressure_trace_row(
                point=point,
                route=phase,
                point_phase=phase,
                trace_stage="control_ready_verified",
                pressure_target_hpa=pressure_target_hpa,
                pace_output_state=snapshot.get("pace_output_state"),
                pace_isolation_state=snapshot.get("pace_isolation_state"),
                pace_vent_status=snapshot.get("pace_vent_status"),
                refresh_pace_state=False,
                note=note or "pressure controller ready for control",
            )
            return True

        if not self._strict_control_ready_check_enabled():
            self.log(
                "Pressure controller ready check warning ignored because strict_control_ready_check=false: "
                + ", ".join(failures)
            )
            return True

        if attempt_recovery:
            self.log(
                "Pressure controller ready check failed before control; "
                f"attempt one recovery: {', '.join(failures)}"
            )
            if not self._stop_pressure_controller_atmosphere_hold(pace, reason="before control recovery"):
                snapshot["hold_thread_active"] = True
            else:
                try:
                    self._set_pressure_controller_vent(False, reason="control ready recovery")
                except Exception as exc:
                    self.log(f"Pressure controller control-ready recovery failed: {exc}")
            snapshot = self._pressure_controller_ready_snapshot(pace)
            failures = self._pressure_controller_ready_failures(snapshot, pace)
            if not failures:
                self._append_pressure_trace_row(
                    point=point,
                    route=phase,
                    point_phase=phase,
                    trace_stage="control_ready_verified",
                    pressure_target_hpa=pressure_target_hpa,
                    pace_output_state=snapshot.get("pace_output_state"),
                    pace_isolation_state=snapshot.get("pace_isolation_state"),
                    pace_vent_status=snapshot.get("pace_vent_status"),
                    refresh_pace_state=False,
                    note="pressure controller ready after recovery",
                )
                return True

        failure_text = ", ".join(failures) if failures else "unknown"
        pace_pressure_now: Optional[float] = None
        pressure_gauge_now: Optional[float] = None
        try:
            pace_pressure_now = self._as_float(pace.read_pressure())
        except Exception:
            pace_pressure_now = None
        gauge = self.devices.get("pressure_gauge")
        if gauge:
            try:
                pressure_gauge_now = self._as_float(gauge.read_pressure())
            except Exception:
                pressure_gauge_now = None
        self._append_pressure_trace_row(
            point=point,
            route=phase,
            point_phase=phase,
            trace_stage="control_ready_failed",
            pressure_target_hpa=pressure_target_hpa,
            pace_output_state=snapshot.get("pace_output_state"),
            pace_isolation_state=snapshot.get("pace_isolation_state"),
            pace_vent_status=snapshot.get("pace_vent_status"),
            read_pace_pressure=True,
            read_pressure_gauge=True,
            note=f"ready failures: {failure_text}",
        )
        self.log(
            "Pressure controller not ready for control: "
            f"{failure_text}; target_hpa={pressure_target_hpa}; "
            f"pace_pressure_hpa={pace_pressure_now}; pressure_gauge_hpa={pressure_gauge_now}"
        )
        return False

    def _enter_pressure_controller_atmosphere_with_legacy_hold(self, pace: Any, *, timeout_s: float) -> None:
        enter_atmosphere = getattr(pace, "enter_atmosphere_mode", None)
        if callable(enter_atmosphere):
            try:
                enter_atmosphere(
                    timeout_s=timeout_s,
                    hold_open=True,
                    hold_interval_s=self._vent_hold_interval_s(),
                )
            except TypeError:
                try:
                    enter_atmosphere(timeout_s=timeout_s, hold_open=True)
                except TypeError:
                    try:
                        enter_atmosphere(timeout_s=timeout_s)
                    except TypeError:
                        enter_atmosphere()
        else:
            set_output = getattr(pace, "set_output", None)
            if callable(set_output):
                set_output(False)
            set_isolation_open = getattr(pace, "set_isolation_open", None)
            if callable(set_isolation_open):
                set_isolation_open(True)
            pace.vent(True)
            start_hold = getattr(pace, "start_atmosphere_hold", None)
            if callable(start_hold):
                start_hold(interval_s=self._vent_hold_interval_s())
        self._pace_vent_after_valve_supported = False
        self._pace_vent_after_valve_open = False

    def _enter_pressure_controller_atmosphere_with_open_vent_valve(self, pace: Any, *, timeout_s: float) -> None:
        popup_ack_enabled = self._vent_popup_ack_override()
        enter_atmosphere = getattr(pace, "enter_atmosphere_mode_with_open_vent_valve", None)
        if not callable(enter_atmosphere):
            raise RuntimeError("VENT_AFTER_VALVE_UNSUPPORTED")
        enter_atmosphere(
            timeout_s=timeout_s,
            popup_ack_enabled=popup_ack_enabled,
        )
        self._pace_vent_after_valve_supported = True
        self._pace_vent_after_valve_open = True
        self._refresh_pressure_controller_aux_state(pace)

    def _set_pressure_controller_vent(self, vent_on: bool, reason: str = "") -> bool:
        pace = self.devices.get("pace")
        if not pace:
            return True
        state = "ON" if vent_on else "OFF"
        fast_preseal_vent_off = (not vent_on) and self._is_fast_preseal_vent_off_reason(reason)
        vent_time_s = float(self._wf("workflow.pressure.vent_time_s", 0.0) or 0.0)
        vent_transition_timeout_s = float(
            self._wf("workflow.pressure.vent_transition_timeout_s", max(5.0, vent_time_s))
        )
        requested_strategy = self._atmosphere_hold_strategy()
        aux_restore_failed = False
        try:
            if not self._stop_pressure_controller_atmosphere_hold(
                pace,
                reason=f"before vent {state}",
            ):
                raise RuntimeError("ATMOSPHERE_HOLD_STOP_FAILED")
            if vent_on:
                self._append_pressure_trace_row(
                    point=None,
                    route="pressure",
                    trace_stage="atmosphere_enter_begin",
                    note=reason or "before enter atmosphere mode",
                    atmosphere_hold_strategy=requested_strategy,
                )
                self._append_pressure_trace_row(
                    point=None,
                    route="pressure",
                    trace_stage="atmosphere_hold_strategy_selected",
                    note=(
                        f"requested_strategy={requested_strategy} "
                        f"vent_after_valve_open={self._vent_after_valve_open_enabled()} "
                        f"popup_ack_override={self._vent_popup_ack_override()}"
                    ),
                    atmosphere_hold_strategy=requested_strategy,
                )
                strategy_used = requested_strategy
                if requested_strategy == "vent_valve_open_after_vent" and self._vent_after_valve_open_enabled():
                    try:
                        self._enter_pressure_controller_atmosphere_with_open_vent_valve(
                            pace,
                            timeout_s=vent_transition_timeout_s,
                        )
                    except Exception as exc:
                        strategy_used = "legacy_hold_thread"
                        self._pace_vent_after_valve_supported = False
                        self.log(
                            "WARNING: pressure controller atmosphere hold strategy fallback -> "
                            f"legacy hold thread ({exc})"
                        )
                        self._append_pressure_trace_row(
                            point=None,
                            route="pressure",
                            trace_stage="atmosphere_hold_legacy_fallback",
                            note=str(exc),
                            atmosphere_hold_strategy=strategy_used,
                        )
                        self._enter_pressure_controller_atmosphere_with_legacy_hold(
                            pace,
                            timeout_s=vent_transition_timeout_s,
                        )
                else:
                    if requested_strategy != "legacy_hold_thread":
                        self.log(
                            "WARNING: pressure controller atmosphere hold strategy fallback -> "
                            "legacy hold thread (vent_after_valve_open disabled)"
                        )
                        self._append_pressure_trace_row(
                            point=None,
                            route="pressure",
                            trace_stage="atmosphere_hold_legacy_fallback",
                            note="vent_after_valve_open disabled by configuration",
                            atmosphere_hold_strategy="legacy_hold_thread",
                        )
                    strategy_used = "legacy_hold_thread"
                    self._enter_pressure_controller_atmosphere_with_legacy_hold(
                        pace,
                        timeout_s=vent_transition_timeout_s,
                    )
                self._pressure_atmosphere_hold_strategy = strategy_used
                self._refresh_pressure_controller_aux_state(pace)
                self._append_pressure_trace_row(
                    point=None,
                    route="pressure",
                    trace_stage="atmosphere_enter_verified",
                    atmosphere_hold_strategy=self._pressure_atmosphere_hold_strategy,
                    note="enter atmosphere mode complete",
                    refresh_pace_state=True,
                )
            else:
                self._refresh_pressure_controller_aux_state(pace)
                need_restore_closed = (
                    self._pressure_atmosphere_hold_strategy == "vent_valve_open_after_vent"
                    and self._pace_vent_after_valve_open is True
                )
                if need_restore_closed:
                    try:
                        self._set_pressure_controller_vent_after_valve_open(
                            False,
                            strict=True,
                            reason=reason,
                        )
                        self._refresh_pressure_controller_aux_state(pace)
                    except Exception as exc:
                        aux_restore_failed = True
                        self.log(f"Pressure controller vent auxiliary restore failed ({state}): {exc}")
                        raise
                exit_atmosphere = getattr(pace, "exit_atmosphere_mode", None)
                if callable(exit_atmosphere) and not fast_preseal_vent_off:
                    exit_atmosphere(timeout_s=vent_transition_timeout_s)
                else:
                    set_output = getattr(pace, "set_output", None)
                    if callable(set_output):
                        set_output(False)
                    pace.vent(False)
                    set_isolation_open = getattr(pace, "set_isolation_open", None)
                    if callable(set_isolation_open):
                        set_isolation_open(True)
            extra = f" ({reason})" if reason else ""
            self.log(f"Pressure controller vent={state}{extra}")
            self._pressure_atmosphere_hold_enabled = bool(vent_on)
            self._pressure_atmosphere_refresh_error_logged = False
            self._last_pressure_atmosphere_refresh_ts = time.time() if vent_on else 0.0
            if vent_on:
                self._clear_preseal_pressure_control_ready_state(reason=f"vent_on:{reason or 'unspecified'}")
        except Exception as exc:
            self._pressure_atmosphere_hold_enabled = False
            self._last_pressure_atmosphere_refresh_ts = 0.0
            if vent_on:
                self._clear_preseal_pressure_control_ready_state(
                    reason=f"vent_on_failed:{reason or 'unspecified'}"
                )
            if not aux_restore_failed:
                self.log(f"Pressure controller vent command failed ({state}): {exc}")
            if vent_on or self._abort_on_vent_off_failure():
                raise RuntimeError(f"Pressure controller vent {state} failed: {exc}") from exc
            return False

        if vent_on:
            wait_s = float(self._wf("workflow.pressure.vent_time_s", 0))
            if wait_s > 0:
                time.sleep(wait_s)
            self._update_atmosphere_reference_hpa(reason=reason or "vent on")
        return True

    def _refresh_pressure_controller_atmosphere_hold(self, *, force: bool = False, reason: str = "") -> None:
        pace = self.devices.get("pace")
        if not pace:
            return

        pcfg = self.cfg.get("workflow", {}).get("pressure", {})
        interval_s = max(0.1, float(pcfg.get("vent_hold_interval_s", 2.0)))
        now = time.time()
        if not self._pressure_atmosphere_hold_enabled:
            return
        if self._pressure_atmosphere_hold_strategy != "legacy_hold_thread":
            return
        if not force and (now - self._last_pressure_atmosphere_refresh_ts) < interval_s:
            return

        try:
            set_output = getattr(pace, "set_output", None)
            if callable(set_output):
                set_output(False)
            set_isolation_open = getattr(pace, "set_isolation_open", None)
            if callable(set_isolation_open):
                set_isolation_open(True)
            pace.vent(True)
            self._last_pressure_atmosphere_refresh_ts = now
            self._pressure_atmosphere_refresh_error_logged = False
        except Exception as exc:
            if not self._pressure_atmosphere_refresh_error_logged:
                extra = f" ({reason})" if reason else ""
                self.log(f"Pressure controller atmosphere refresh failed{extra}: {exc}")
                self._pressure_atmosphere_refresh_error_logged = True

    def _enable_pressure_controller_output(self, reason: str = "") -> bool:
        pace = self.devices.get("pace")
        if not pace:
            return True
        try:
            enable_output = getattr(pace, "enable_control_output", None)
            if callable(enable_output):
                enable_output()
            else:
                set_mode_active = getattr(pace, "set_output_mode_active", None)
                if callable(set_mode_active):
                    set_mode_active()
                pace.set_output(True)
            extra = f" ({reason})" if reason else ""
            self.log(f"Pressure controller output=ON{extra}")
            return True
        except Exception as exc:
            self.log(f"Pressure controller output enable failed: {exc}")
            return False

    def _pressure_controller_output_on_failures(self, snapshot: Dict[str, Any], pace: Any = None) -> List[str]:
        failures: List[str] = []
        vent_status = self._as_int(snapshot.get("pace_vent_status"))
        output_state = self._as_int(snapshot.get("pace_output_state"))
        isolation_state = self._as_int(snapshot.get("pace_isolation_state"))
        atmosphere_hold_strategy = str(
            snapshot.get("atmosphere_hold_strategy") or self._pressure_atmosphere_hold_strategy or ""
        ).strip()
        trapped_pressure_status = self._as_int(getattr(pace, "VENT_STATUS_TRAPPED_PRESSURE", 3))
        if vent_status is None:
            failures.append("vent_status_unavailable")
        elif (
            trapped_pressure_status is not None
            and vent_status == trapped_pressure_status
            and not self._pace_trapped_pressure_allows_control(pace, vent_status)
        ):
            failures.append(f"vent_status={vent_status}(trapped_pressure)")
        elif not self._pace_vent_status_allows_control(pace, vent_status):
            failures.append(f"vent_status={vent_status}")
        if output_state is None:
            failures.append("output_state_unavailable")
        elif output_state != 1:
            failures.append(f"output_state={output_state}")
        if isolation_state is None:
            failures.append("isolation_state_unavailable")
        elif isolation_state != 1:
            failures.append(f"isolation_state={isolation_state}")
        if snapshot.get("hold_thread_active"):
            failures.append("atmosphere_hold_active")
        if atmosphere_hold_strategy == "vent_valve_open_after_vent" and snapshot.get("vent_after_valve_open") is True:
            failures.append("vent_after_valve_open")
        return failures

    def _attempt_pressure_controller_output_on_recovery(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        pressure_target_hpa: Optional[float],
        note: str = "",
    ) -> bool:
        pace = self.devices.get("pace")
        if not pace:
            return False
        retries = self._pressure_output_on_recovery_retries()
        if retries <= 0:
            return False
        trapped_pressure_status = self._as_int(getattr(pace, "VENT_STATUS_TRAPPED_PRESSURE", 3))
        for attempt_idx in range(retries):
            snapshot = self._pressure_controller_ready_snapshot(pace)
            vent_status = self._as_int(snapshot.get("pace_vent_status"))
            output_state = self._as_int(snapshot.get("pace_output_state"))
            isolation_state = self._as_int(snapshot.get("pace_isolation_state"))
            vent_ready_for_control = self._pace_vent_status_allows_control(pace, vent_status)
            if output_state == 1 and isolation_state == 1 and vent_ready_for_control:
                return True
            trapped_pressure_active = trapped_pressure_status is not None and vent_status == trapped_pressure_status
            trapped_ready_for_control = trapped_pressure_active and self._pace_trapped_pressure_allows_control(
                pace,
                vent_status,
            )
            if trapped_pressure_active and not trapped_ready_for_control:
                return False
            if self._pressure_output_on_recovery_requires_trapped():
                if not trapped_ready_for_control or isolation_state != 1:
                    return False
            elif not vent_ready_for_control or isolation_state != 1:
                return False
            self.log(
                "Pressure controller output-on recovery attempt "
                f"{attempt_idx + 1}/{retries}: vent_status={vent_status} "
                f"output_state={output_state} isolation_state={isolation_state} "
                f"target_hpa={pressure_target_hpa}"
            )
            set_output = getattr(pace, "set_output", None)
            if callable(set_output):
                try:
                    set_output(False)
                except Exception as exc:
                    self.log(f"Pressure controller output-on recovery output-off failed: {exc}")
            set_isolation_open = getattr(pace, "set_isolation_open", None)
            if callable(set_isolation_open):
                try:
                    set_isolation_open(True)
                except Exception as exc:
                    self.log(f"Pressure controller output-on recovery isolation-open failed: {exc}")
            try:
                pace.vent(False)
            except Exception as exc:
                self.log(f"Pressure controller output-on recovery vent-off failed: {exc}")
            if pressure_target_hpa is not None:
                try:
                    pace.set_setpoint(float(pressure_target_hpa))
                except Exception as exc:
                    self.log(f"Pressure controller output-on recovery setpoint failed: {exc}")
            self._append_pressure_trace_row(
                point=point,
                route=phase,
                point_phase=phase,
                trace_stage="control_output_on_recovery_begin",
                pressure_target_hpa=pressure_target_hpa,
                pace_output_state=snapshot.get("pace_output_state"),
                pace_isolation_state=snapshot.get("pace_isolation_state"),
                pace_vent_status=snapshot.get("pace_vent_status"),
                read_pace_pressure=True,
                read_pressure_gauge=True,
                note=note or f"output-on recovery attempt {attempt_idx + 1}",
            )
            if not self._enable_pressure_controller_output(
                reason=f"output-on recovery attempt {attempt_idx + 1}"
            ):
                continue
            if self._verify_pressure_controller_output_on(
                point,
                phase=phase,
                pressure_target_hpa=pressure_target_hpa,
                note=f"after output-on recovery attempt {attempt_idx + 1}",
                allow_recovery=False,
            ):
                return True
        return False

    def _verify_pressure_controller_output_on(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        pressure_target_hpa: Optional[float],
        note: str = "",
        allow_recovery: bool = True,
    ) -> bool:
        pace = self.devices.get("pace")
        if not pace:
            return True
        snapshot = self._pressure_controller_ready_snapshot(pace)
        failures = self._pressure_controller_output_on_failures(snapshot, pace)
        wait_timeout_s = self._pressure_output_on_verify_timeout_s()
        wait_poll_s = self._pressure_output_on_verify_poll_s()
        wait_iterations = 0
        if failures and wait_timeout_s > 0:
            wait_start_ts = time.time()
            self._append_pressure_trace_row(
                point=point,
                route=phase,
                point_phase=phase,
                trace_stage="control_output_verify_wait_begin",
                pressure_target_hpa=pressure_target_hpa,
                pace_output_state=snapshot.get("pace_output_state"),
                pace_isolation_state=snapshot.get("pace_isolation_state"),
                pace_vent_status=snapshot.get("pace_vent_status"),
                refresh_pace_state=False,
                event_ts=wait_start_ts,
                note=f"timeout_s={wait_timeout_s:.3f} poll_s={wait_poll_s:.3f} failures={','.join(failures)}",
            )
            wait_deadline = time.time() + wait_timeout_s
            while failures and time.time() < wait_deadline:
                time.sleep(wait_poll_s)
                wait_iterations += 1
                snapshot = self._pressure_controller_ready_snapshot(pace)
                failures = self._pressure_controller_output_on_failures(snapshot, pace)
            self._append_pressure_trace_row(
                point=point,
                route=phase,
                point_phase=phase,
                trace_stage="control_output_verify_wait_end",
                pressure_target_hpa=pressure_target_hpa,
                pace_output_state=snapshot.get("pace_output_state"),
                pace_isolation_state=snapshot.get("pace_isolation_state"),
                pace_vent_status=snapshot.get("pace_vent_status"),
                refresh_pace_state=False,
                note=(
                    f"iterations={wait_iterations} result=ready"
                    if not failures
                    else f"iterations={wait_iterations} remaining_failures={','.join(failures)}"
                ),
            )
        trace_stage = "control_output_on_verified"
        if failures:
            trace_stage = "control_output_on_failed"
        self._append_pressure_trace_row(
            point=point,
            route=phase,
            point_phase=phase,
            trace_stage=trace_stage,
            pressure_target_hpa=pressure_target_hpa,
            pace_output_state=snapshot.get("pace_output_state"),
            pace_isolation_state=snapshot.get("pace_isolation_state"),
            pace_vent_status=snapshot.get("pace_vent_status"),
            read_pace_pressure=bool(failures),
            read_pressure_gauge=bool(failures),
            refresh_pace_state=False,
            note=note if not failures else f"{note}; failures: {', '.join(failures)}",
        )
        if not failures:
            return True
        pace_pressure_now: Optional[float] = None
        pressure_gauge_now: Optional[float] = None
        try:
            pace_pressure_now = self._as_float(pace.read_pressure())
        except Exception:
            pace_pressure_now = None
        gauge = self.devices.get("pressure_gauge")
        if gauge:
            try:
                pressure_gauge_now = self._as_float(gauge.read_pressure())
            except Exception:
                pressure_gauge_now = None
        self.log(
            "Pressure controller output-on verification failed: "
            + ", ".join(failures)
            + f"; target_hpa={pressure_target_hpa}; "
            + f"pace_pressure_hpa={pace_pressure_now}; pressure_gauge_hpa={pressure_gauge_now}"
        )
        if allow_recovery and self._attempt_pressure_controller_output_on_recovery(
            point,
            phase=phase,
            pressure_target_hpa=pressure_target_hpa,
            note=note,
        ):
            return True
        set_output = getattr(pace, "set_output", None)
        if callable(set_output):
            try:
                set_output(False)
            except Exception as exc:
                self.log(f"Pressure controller output-off after verification failure failed: {exc}")
        return False

    def _wait_primary_sensor_stable(
        self,
        point: CalibrationPoint,
        *,
        value_key: Optional[str] = None,
        require_pressure_in_limits: bool = False,
        tol_override: Optional[float] = None,
        window_override: Optional[float] = None,
        timeout_override: Optional[float] = None,
        min_samples_override: Optional[int] = None,
        read_interval_override: Optional[float] = None,
        pressure_fill_override: Optional[float] = None,
        pressure_window_cfg: Optional[Dict[str, Any]] = None,
    ) -> bool:
        cfg = self.cfg.get("workflow", {}).get("stability", {}).get("sensor", {})
        if cfg and not cfg.get("enabled", True):
            return True

        analyzers = self._active_gas_analyzers()
        if not analyzers:
            if self._all_gas_analyzers():
                self.log("Sensor wait failed: no active gas analyzers remain")
                return False
            return True
        pace = self.devices.get("pace")

        key = value_key
        if not key:
            key = "h2o_ratio_f" if point.is_h2o_point else "co2_ratio_f"

        if key == "h2o_ratio_f":
            tol = float(cfg.get("h2o_ratio_f_tol", cfg.get("h2o_ratio_raw_tol", 0.001)))
        elif key == "co2_ratio_f":
            tol = float(cfg.get("co2_ratio_f_tol", cfg.get("co2_ratio_raw_tol", 0.001)))
        elif key == "h2o_ratio_raw":
            tol = float(cfg.get("h2o_ratio_raw_tol", cfg.get("h2o_ratio_f_tol", 0.001)))
        elif key == "co2_ratio_raw":
            tol = float(cfg.get("co2_ratio_raw_tol", cfg.get("co2_ratio_f_tol", 0.001)))
        elif key == "co2_ppm":
            tol = float(cfg.get("co2_tol", 2.0))
        elif key == "h2o_mmol":
            tol = float(cfg.get("h2o_tol", 0.2))
        else:
            tol = float(cfg.get("generic_tol", cfg.get("h2o_ratio_f_tol", 0.001)))

        window_s = float(cfg.get("window_s", 30))
        timeout_s = float(cfg.get("timeout_s", 300))
        poll_s = float(cfg.get("poll_s", 0.5))
        read_interval_s = float(cfg.get("read_interval_s", 1.0))
        min_samples = 2
        pressure_fill_s = 0.0

        if require_pressure_in_limits:
            if key == "h2o_ratio_f":
                tol = float(cfg.get("h2o_ratio_f_pressure_tol", tol))
                window_s = float(cfg.get("h2o_ratio_f_pressure_window_s", 15))
                min_samples = int(cfg.get("h2o_ratio_f_pressure_min_samples", 10))
                pressure_fill_s = float(cfg.get("h2o_ratio_f_pressure_fill_s", 15))
                read_interval_s = float(cfg.get("h2o_ratio_f_pressure_read_interval_s", read_interval_s))
            elif key == "co2_ratio_f":
                tol = float(cfg.get("co2_ratio_f_pressure_tol", tol))
                window_s = float(cfg.get("co2_ratio_f_pressure_window_s", 15))
                min_samples = int(cfg.get("co2_ratio_f_pressure_min_samples", 10))
                pressure_fill_s = float(cfg.get("co2_ratio_f_pressure_fill_s", 15))
                read_interval_s = float(cfg.get("co2_ratio_f_pressure_read_interval_s", read_interval_s))

        if tol_override is not None:
            tol = float(tol_override)
        if window_override is not None:
            window_s = float(window_override)
        if timeout_override is not None:
            timeout_s = float(timeout_override)
        if min_samples_override is not None:
            min_samples = int(min_samples_override)
        if read_interval_override is not None:
            read_interval_s = float(read_interval_override)
        if pressure_fill_override is not None:
            pressure_fill_s = float(pressure_fill_override)

        stabs = {label: StabilityWindow(tol, window_s) for label, _, _ in analyzers}
        start = time.time()
        last_report = 0.0
        last_values: Dict[str, Optional[float]] = {label: None for label, _, _ in analyzers}
        pressure_blocked_logged = False
        pressure_latched_logged = False
        pressure_in_limits_latched = False
        pressure_fill_started_at: Optional[float] = None
        pressure_fill_logged = False
        last_pressure_now: Optional[float] = None
        last_pressure_inl: Optional[int] = None
        pressure_window_samples: List[Tuple[float, float]] = []
        pressure_window_source = "unavailable"
        pressure_window_span: Optional[float] = None
        pressure_window_ready = False

        while time.time() - start < timeout_s:
            loop_started_at = time.time()
            if self.stop_event.is_set():
                return False
            self._check_pause()

            if require_pressure_in_limits and pace and point.target_pressure_hpa is not None:
                try:
                    p_now, inl = pace.get_in_limits()
                except Exception as exc:
                    if not pressure_blocked_logged:
                        self.log(f"Pressure check failed during sensor wait: {exc}")
                        pressure_blocked_logged = True
                    time.sleep(max(0.05, poll_s))
                    continue
                last_pressure_now = float(p_now)
                last_pressure_inl = int(inl)
                if int(inl) != 1:
                    if pressure_in_limits_latched:
                        if not pressure_latched_logged:
                            self.log(
                                "Pressure drift detected after controller in-limits latched: "
                                f"target={point.target_pressure_hpa} current={p_now}; "
                                "continue analyzer stability wait"
                            )
                            pressure_latched_logged = True
                        pressure_blocked_logged = False
                    elif not pressure_blocked_logged:
                        self.log(
                            f"Pressure not in-limits during sensor wait: "
                            f"target={point.target_pressure_hpa} current={p_now}"
                        )
                        pressure_blocked_logged = True
                        # Wait until the pressure controller itself reports in-limits
                        # before starting the analyzer stability window.
                        stabs = {label: StabilityWindow(tol, window_s) for label, _, _ in analyzers}
                        time.sleep(max(0.05, poll_s))
                        continue
                else:
                    if not pressure_in_limits_latched:
                        pressure_in_limits_latched = True
                        pressure_fill_started_at = time.time()
                        pressure_fill_logged = False
                        # Start analyzer stability timing only after the controller
                        # has first reached in-limits and the downstream volume has
                        # had time to fill.
                        stabs = {label: StabilityWindow(tol, window_s) for label, _, _ in analyzers}
                    pressure_blocked_logged = False
                    pressure_latched_logged = False
                    if pressure_fill_started_at is not None and pressure_fill_s > 0:
                        fill_elapsed = time.time() - pressure_fill_started_at
                        if fill_elapsed < pressure_fill_s:
                            if not pressure_fill_logged:
                                self.log(
                                    "Pressure in-limits latched; waiting for analyzer cavity fill: "
                                    f"{int(pressure_fill_s)}s"
                                )
                                pressure_fill_logged = True
                            time.sleep(max(0.05, poll_s))
                            continue

            if require_pressure_in_limits and pressure_window_cfg:
                pressure_value, pressure_window_source = self._read_best_pressure_for_sampling_gate(
                    bool(pressure_window_cfg.get("prefer_gauge", True))
                )
                if pressure_value is not None:
                    pressure_window_samples.append((time.time(), float(pressure_value)))
                cutoff = time.time() - float(pressure_window_cfg.get("window_s", window_s))
                pressure_window_samples = [
                    (ts, value) for ts, value in pressure_window_samples if ts >= cutoff
                ]
                pressure_values = [value for _, value in pressure_window_samples]
                pressure_window_span = self._span(pressure_values) if pressure_values else None
                pressure_window_ready = (
                    len(pressure_values) >= int(pressure_window_cfg.get("min_samples", min_samples))
                    and pressure_window_span is not None
                    and pressure_window_span <= float(pressure_window_cfg.get("pressure_span_hpa", 0.0))
                )

            all_stable = True
            stable_snapshot: Dict[str, float] = {}
            for label, ga, _ in analyzers:
                _, parsed = self._read_sensor_parsed(
                    ga,
                    required_key=key,
                    frame_acceptance_mode=self._resolve_sensor_frame_acceptance_mode(
                        key,
                        require_usable=True,
                    ),
                )
                if parsed:
                    value = self._as_float(parsed.get(key))
                    if value is not None:
                        last_values[label] = value
                        stabs[label].add(value)
                stab = stabs[label]
                if len(stab.values) >= max(2, min_samples) and stab.is_stable():
                    if last_values[label] is not None:
                        stable_snapshot[label] = float(last_values[label])
                else:
                    all_stable = False

            if pressure_window_cfg and not pressure_window_ready:
                all_stable = False

            if all_stable and len(stable_snapshot) == len(analyzers):
                summary = " ".join(
                    f"{label}={stable_snapshot[label]}" for label, _, _ in analyzers if label in stable_snapshot
                )
                msg = f"Sensor stable (all analyzers {key}: {summary})"
                if pressure_window_cfg:
                    msg += (
                        f" pressure_source={pressure_window_source} "
                        f"pressure_span={pressure_window_span} "
                        f"pressure_samples={len(pressure_window_samples)}"
                    )
                self.log(msg)
                return True

            if time.time() - last_report >= 30:
                last_report = time.time()
                parts = []
                for label, _, _ in analyzers:
                    parts.append(f"{label}={last_values.get(label)}")
                msg = f"Sensor settling... {key} " + " ".join(parts)
                if require_pressure_in_limits and pace and point.target_pressure_hpa is not None:
                    msg += f" pressure={last_pressure_now} in_limits={last_pressure_inl}"
                if pressure_window_cfg:
                    msg += (
                        f" gate_source={pressure_window_source} "
                        f"gate_span={pressure_window_span} "
                        f"gate_samples={len(pressure_window_samples)}"
                    )
                self.log(msg)

            elapsed = time.time() - loop_started_at
            sleep_s = max(0.05, poll_s, read_interval_s - elapsed)
            time.sleep(sleep_s)

        stable_labels: List[str] = []
        unstable_labels: List[str] = []
        for label, _, _ in analyzers:
            stab = stabs[label]
            if len(stab.values) >= max(2, min_samples) and stab.is_stable():
                stable_labels.append(label)
            else:
                unstable_labels.append(label)

        tail = " ".join(f"{label}={last_values.get(label)}" for label, _, _ in analyzers)
        if unstable_labels and stable_labels:
            self.log(
                f"Sensor stability timeout on active analyzers key={key}; "
                f"drop={','.join(unstable_labels)} keep={','.join(stable_labels)} last={tail}"
            )
            self._disable_analyzers(unstable_labels, reason=f"{key}_timeout")
            return self._wait_primary_sensor_stable(
                point,
                value_key=key,
                require_pressure_in_limits=require_pressure_in_limits,
                tol_override=tol_override,
                window_override=window_override,
                timeout_override=timeout_override,
                min_samples_override=min_samples_override,
                read_interval_override=read_interval_override,
                pressure_fill_override=pressure_fill_override,
                pressure_window_cfg=pressure_window_cfg,
            )

        timeout_msg = f"Sensor stability timeout on active analyzers key={key} last={tail}"
        if pressure_window_cfg:
            timeout_msg += (
                f" gate_source={pressure_window_source} "
                f"gate_span={pressure_window_span} "
                f"gate_samples={len(pressure_window_samples)}"
            )
        self.log(timeout_msg)
        return False

    def _managed_valves(self) -> List[int]:
        valves_cfg = self.cfg.get("valves", {})
        managed = set()

        for key in ("co2_path", "co2_path_group2", "gas_main", "h2o_path", "hold", "flow_switch"):
            iv = self._as_int(valves_cfg.get(key))
            if iv is not None:
                managed.add(iv)

        for map_name in ("co2_map", "co2_map_group2"):
            co2_map = valves_cfg.get(map_name, {})
            if isinstance(co2_map, dict):
                for val in co2_map.values():
                    iv = self._as_int(val)
                    if iv is not None:
                        managed.add(iv)

        relay_map = valves_cfg.get("relay_map", {})
        if isinstance(relay_map, dict):
            for key in relay_map.keys():
                iv = self._as_int(key)
                if iv is not None:
                    managed.add(iv)

        return sorted(managed)

    def _startup_preflight_reset(self) -> None:
        """Reset actuators to a known safe baseline before the run starts."""
        self.log("Startup preflight: checking and resetting device states")

        try:
            self._apply_route_baseline_valves()
            self.log("Startup preflight: baseline valve state applied")
        except Exception as exc:
            self.log(f"Startup preflight: valve close command failed: {exc}")

        for relay_name, count in (("relay", 16), ("relay_8", 8)):
            relay = self.devices.get(relay_name)
            if not relay:
                continue
            read_coils = getattr(relay, "read_coils", None)
            if not callable(read_coils):
                continue
            try:
                bits = read_coils(0, count)
                states = list(bits[:count]) if bits is not None else []
                self.log(f"Startup preflight: {relay_name} states={states}")
            except Exception as exc:
                self.log(f"Startup preflight: {relay_name} state read failed: {exc}")

        if self.devices.get("pace"):
            # Start every run from vent/open-atmosphere baseline.
            self._set_pressure_controller_vent(True, reason="startup preflight reset")
            self._h2o_pressure_prepared_target = None

    def _startup_pressure_precheck(self, points: List[CalibrationPoint]) -> None:
        cfg = self.cfg.get("workflow", {}).get("startup_pressure_precheck", {})
        if not isinstance(cfg, dict) or not cfg.get("enabled", False):
            return

        route = str(cfg.get("route", "co2") or "co2").strip().lower()
        strict = bool(cfg.get("strict", True))
        point = self._startup_pressure_precheck_point(points, route=route)
        if point is None:
            msg = f"Startup pressure precheck skipped: no usable {route.upper()} point found"
            self.log(msg)
            if strict:
                raise RuntimeError(msg)
            return

        self.log(
            "Startup pressure precheck: "
            f"route={route.upper()} row={point.index} target={point.target_pressure_hpa} "
            f"co2={point.co2_ppm} group={getattr(point, 'co2_group', None)}"
        )

        route_soak_s = max(0.0, float(cfg.get("route_soak_s", 3.0)))
        try:
            if route == "h2o":
                self._set_pressure_controller_vent(True, reason="startup pressure precheck H2O route open")
                self._set_h2o_path(True, point)
            else:
                self._set_co2_route_baseline(reason="before startup pressure precheck")
                self._set_valves_for_co2(point)

            if route_soak_s > 0:
                self.log(
                    "Startup pressure precheck: "
                    f"wait {route_soak_s:.0f}s with {route.upper()} route open before sealing"
                )
                time.sleep(route_soak_s)

            if not self._pressurize_route_for_sealed_points(point, route=route, sealed_control_refs=[point]):
                raise RuntimeError(f"Startup pressure precheck could not seal {route.upper()} route")

            if not self._set_pressure_to_target(point):
                raise RuntimeError(
                    f"Startup pressure precheck could not stabilize at {point.target_pressure_hpa} hPa"
                )

            hold_ok, detail = self._observe_startup_pressure_hold(cfg)
            msg = (
                "Startup pressure precheck hold result: "
                f"ok={hold_ok} source={detail.get('source')} start={detail.get('start_hpa')} "
                f"end={detail.get('end_hpa')} drift={detail.get('max_abs_drift_hpa')} "
                f"span={detail.get('span_hpa')} samples={detail.get('samples')}"
            )
            self.log(msg)
            if not hold_ok:
                raise RuntimeError(
                    "Startup pressure precheck failed: "
                    f"{route.upper()} route pressure drift exceeded limit "
                    f"({detail.get('max_abs_drift_hpa')} hPa > {detail.get('limit_hpa')} hPa)"
                )
        except Exception:
            if strict:
                raise
            self.log("Startup pressure precheck failed but strict=false; continue run")
        finally:
            if route == "h2o":
                self._cleanup_h2o_route(point, reason="after startup pressure precheck")
            else:
                self._cleanup_co2_route(reason="after startup pressure precheck")

    def _startup_pressure_precheck_point(
        self,
        points: List[CalibrationPoint],
        *,
        route: str,
    ) -> Optional[CalibrationPoint]:
        cfg = self.cfg.get("workflow", {}).get("startup_pressure_precheck", {})
        override_target = self._as_float(cfg.get("target_hpa")) if isinstance(cfg, dict) else None

        route_name = str(route or "co2").strip().lower()
        candidate: Optional[CalibrationPoint] = None
        if route_name == "h2o":
            for point in points:
                if point.is_h2o_point:
                    candidate = point
                    break
        else:
            gas_sources = self._co2_source_points(points)
            if gas_sources:
                candidate = gas_sources[0]
            else:
                for point in points:
                    if point.co2_ppm is not None:
                        candidate = point
                        break

        if candidate is None:
            return None

        selected_targets = self._normalize_selected_pressure_points(
            self.cfg.get("workflow", {}).get("selected_pressure_points")
        )
        selected_numeric_targets = {
            int(value)
            for value in selected_targets or []
            if not self._is_ambient_pressure_selection_value(value)
        }
        if selected_targets is not None and not selected_numeric_targets:
            return None

        targets = []
        for point in points:
            normalized_target = self._normalized_pressure_hpa_value(self._as_float(point.target_pressure_hpa))
            if normalized_target is None:
                continue
            if selected_numeric_targets and int(round(float(normalized_target))) not in selected_numeric_targets:
                continue
            targets.append(normalized_target)
        valid_targets = [float(value) for value in targets if value is not None]
        chosen_target = override_target
        if (
            chosen_target is not None
            and selected_numeric_targets
            and int(round(float(chosen_target))) not in selected_numeric_targets
        ):
            chosen_target = None
        if chosen_target is None:
            if valid_targets:
                chosen_target = max(valid_targets)
            elif selected_numeric_targets:
                return None
            else:
                chosen_target = self._as_float(candidate.target_pressure_hpa)
        if chosen_target is None:
            return None

        return self._normalize_pressure_point(
            CalibrationPoint(
                index=candidate.index,
                temp_chamber_c=candidate.temp_chamber_c,
                co2_ppm=candidate.co2_ppm,
                hgen_temp_c=candidate.hgen_temp_c,
                hgen_rh_pct=candidate.hgen_rh_pct,
                target_pressure_hpa=chosen_target,
                dewpoint_c=candidate.dewpoint_c,
                h2o_mmol=candidate.h2o_mmol,
                raw_h2o=candidate.raw_h2o,
                co2_group=getattr(candidate, "co2_group", None),
            )
        )

    def _startup_pressure_sensor_calibration_cfg(self) -> Dict[str, Any]:
        cfg = self.cfg.get("workflow", {}).get("startup_pressure_sensor_calibration", {})
        return dict(cfg) if isinstance(cfg, dict) else {}

    def _startup_pressure_sensor_calibration_point(
        self,
        points: List[CalibrationPoint],
    ) -> Optional[CalibrationPoint]:
        gas_sources = self._co2_source_points(points)
        for point in gas_sources:
            if self._is_zero_co2_point(point):
                return point
        return gas_sources[0] if gas_sources else None

    def _build_pressure_sensor_calibration_target_point(
        self,
        template: CalibrationPoint,
        *,
        target_hpa: float,
    ) -> CalibrationPoint:
        point = CalibrationPoint(
            index=int(getattr(template, "index", 0) or 0),
            temp_chamber_c=self._as_float(getattr(template, "temp_chamber_c", None)),
            co2_ppm=self._as_float(getattr(template, "co2_ppm", None)),
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=float(target_hpa),
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
            co2_group=getattr(template, "co2_group", None),
        )
        return self._decorate_pressure_point(
            point,
            pressure_mode="sealed_controlled",
            pressure_target_label=f"{int(round(float(target_hpa)))}hPa",
        )

    def _restore_startup_pressure_sensor_calibration_analyzers(self) -> None:
        gas_cfg_default = self.cfg.get("devices", {}).get("gas_analyzer", {})
        for label, ga, cfg in self._all_gas_analyzers():
            try:
                self._configure_gas_analyzer(
                    ga,
                    label=label,
                    mode=2,
                    active_send=bool(cfg.get("active_send", gas_cfg_default.get("active_send", False))),
                    ftd_hz=int(cfg.get("ftd_hz", gas_cfg_default.get("ftd_hz", 1))),
                    avg_co2=int(cfg.get("average_co2", gas_cfg_default.get("average_co2", 1))),
                    avg_h2o=int(cfg.get("average_h2o", gas_cfg_default.get("average_h2o", 1))),
                    avg_filter=int(cfg.get("average_filter", gas_cfg_default.get("average_filter", 49))),
                    warning_phase="startup",
                )
            except Exception as exc:
                self.log(f"Startup pressure calibration restore analyzer failed: {label} err={exc}")

    def _write_startup_pressure_sensor_calibration_artifacts(
        self,
        *,
        output_dir: Path,
        sample_rows: Sequence[Mapping[str, Any]],
        summary_rows: Sequence[Mapping[str, Any]],
    ) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        for name, rows in (("detail.csv", sample_rows), ("summary.csv", summary_rows)):
            header: List[str] = []
            for row in rows:
                for key in row.keys():
                    if key not in header:
                        header.append(str(key))
            with (output_dir / name).open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=header)
                writer.writeheader()
                for row in rows:
                    writer.writerow(dict(row))
        (output_dir / "summary.json").write_text(
            json.dumps({"sample_rows": list(sample_rows), "summary_rows": list(summary_rows)}, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

    def _startup_pressure_sensor_calibration(self, points: List[CalibrationPoint]) -> None:
        cfg = self._startup_pressure_sensor_calibration_cfg()
        if not bool(cfg.get("enabled", False)):
            return

        strict = bool(cfg.get("strict", True))
        target_hpa = float(cfg.get("target_hpa", 1000.0) or 1000.0)
        flush_soak_s = max(0.0, float(cfg.get("flush_soak_s", 10.0) or 10.0))
        sample_duration_s = max(1.0, float(cfg.get("sample_duration_s", 8.0) or 8.0))
        sample_interval_s = max(0.1, float(cfg.get("sample_interval_s", 1.0) or 1.0))
        min_samples = max(1, int(cfg.get("min_samples", 3) or 3))
        apply_write = bool(cfg.get("apply_write", True))
        require_pressure_gauge = bool(cfg.get("require_pressure_gauge", True))

        template_point = self._startup_pressure_sensor_calibration_point(points)
        if template_point is None:
            msg = "Startup pressure sensor calibration skipped: no usable zero-gas CO2 point found"
            self.log(msg)
            if strict:
                raise RuntimeError(msg)
            return

        analyzers = self._active_gas_analyzers()
        if not analyzers:
            msg = "Startup pressure sensor calibration skipped: no active analyzers"
            self.log(msg)
            if strict:
                raise RuntimeError(msg)
            return

        target_point = self._build_pressure_sensor_calibration_target_point(template_point, target_hpa=target_hpa)
        sample_rows: List[Dict[str, Any]] = []
        summary_rows: List[Dict[str, Any]] = []
        output_dir = self.logger.run_dir / f"startup_pressure_sensor_calibration_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            self.log(
                "Startup pressure sensor calibration: "
                f"zero-gas flush at atmosphere, then seal and control to {target_hpa:.0f} hPa"
            )
            self._set_pressure_controller_vent(True, reason="startup pressure sensor calibration flush")
            self._set_co2_route_baseline(reason="before startup pressure sensor calibration")
            self._set_valves_for_co2(template_point)
            if flush_soak_s > 0:
                time.sleep(flush_soak_s)

            self.log(
                "Startup pressure sensor calibration: flush done; "
                "close atmosphere, close all route valves, then start pressure control"
            )
            self._apply_route_baseline_valves()
            self._set_pressure_controller_vent(False, reason="startup pressure sensor calibration control")
            if not self._set_pressure_to_target(target_point):
                raise RuntimeError(f"startup pressure sensor calibration could not stabilize at {target_hpa:.1f} hPa")

            started = time.time()
            while (time.time() - started) < sample_duration_s:
                if self.stop_event.is_set():
                    raise RuntimeError("startup pressure sensor calibration interrupted")
                self._check_pause()
                reference_hpa, reference_source = self._read_preseal_pressure_gauge()
                if require_pressure_gauge and reference_source != "pressure_gauge":
                    time.sleep(sample_interval_s)
                    continue
                for label, ga, _analyzer_cfg in analyzers:
                    line, parsed = self._read_sensor_parsed(
                        ga,
                        required_key="pressure_kpa",
                        require_usable=True,
                        frame_acceptance_mode="required_key_relaxed",
                    )
                    analyzer_kpa = self._as_float(parsed.get("pressure_kpa")) if parsed else None
                    offset = None if analyzer_kpa is None or reference_hpa is None else (float(reference_hpa) / 10.0) - float(analyzer_kpa)
                    sample_rows.append(
                        {
                            "Analyzer": label,
                            "DeviceId": _normalized_device_id_text((parsed or {}).get("id")),
                            "ReferenceHpa": reference_hpa,
                            "ReferenceSource": reference_source,
                            "AnalyzerPressureKPa": analyzer_kpa,
                            "OffsetA_kPa": offset,
                            "Raw": str(line or ""),
                            "FrameOk": analyzer_kpa is not None and reference_hpa is not None,
                        }
                    )
                time.sleep(sample_interval_s)

            for label, ga, _analyzer_cfg in analyzers:
                analyzer_rows = [
                    row for row in sample_rows
                    if str(row.get("Analyzer") or "") == label and row.get("OffsetA_kPa") is not None
                ]
                if len(analyzer_rows) < min_samples:
                    summary_rows.append(
                        {
                            "Analyzer": label,
                            "DeviceId": "",
                            "Samples": len(analyzer_rows),
                            "OffsetA_kPa": None,
                            "WriteApplied": False,
                            "ReadbackOk": False,
                            "Status": "insufficient_samples",
                        }
                    )
                    continue
                device_ids = [_normalized_device_id_text(row.get("DeviceId")) for row in analyzer_rows if _normalized_device_id_text(row.get("DeviceId"))]
                device_id = Counter(device_ids).most_common(1)[0][0] if device_ids else ""
                offset = float(sum(float(row["OffsetA_kPa"]) for row in analyzer_rows) / len(analyzer_rows))
                readback_ok = False
                readback = {}
                error = ""
                if apply_write:
                    try:
                        ga.set_mode_with_ack(2, require_ack=True)
                        if not ga.set_senco(9, offset, 1.0, 0.0, 0.0):
                            raise RuntimeError("SENCO9 write ack failed")
                        readback = ga.read_coefficient_group(9)
                        expected_rounded = rounded_senco_values((offset, 1.0, 0.0, 0.0))
                        readback_c0 = self._as_float(readback.get("C0"))
                        readback_c1 = self._as_float(readback.get("C1"))
                        readback_c2 = self._as_float(readback.get("C2"))
                        readback_ok = (
                            readback_c0 is not None
                            and readback_c1 is not None
                            and readback_c2 is not None
                            and abs(float(readback_c0) - expected_rounded[0]) <= 1e-9
                            and abs(float(readback_c1) - expected_rounded[1]) <= 1e-9
                            and abs(float(readback_c2) - expected_rounded[2]) <= 1e-9
                        )
                    except Exception as exc:
                        error = str(exc)
                summary_rows.append(
                    {
                        "Analyzer": label,
                        "DeviceId": device_id,
                        "Samples": len(analyzer_rows),
                        "OffsetA_kPa": offset,
                        "WriteApplied": bool(apply_write),
                        "ReadbackOk": bool(readback_ok) if apply_write else "",
                        "Readback": json.dumps(readback, ensure_ascii=False, sort_keys=True) if readback else "",
                        "Status": "ok" if (not apply_write or readback_ok) else "write_failed",
                        "Error": error,
                    }
                )

            self._write_startup_pressure_sensor_calibration_artifacts(
                output_dir=output_dir,
                sample_rows=sample_rows,
                summary_rows=summary_rows,
            )
            failed = [row for row in summary_rows if str(row.get("Status") or "") != "ok"]
            if failed and strict:
                raise RuntimeError(f"startup pressure sensor calibration failed for {len(failed)} analyzers")
        except Exception:
            if strict:
                raise
            self.log("Startup pressure sensor calibration failed but strict=false; continue run")
        finally:
            try:
                self._cleanup_co2_route(reason="after startup pressure sensor calibration")
            except Exception as exc:
                self.log(f"Startup pressure sensor calibration cleanup failed: {exc}")
            self._restore_startup_pressure_sensor_calibration_analyzers()

    def _sample_open_route_point(self, point: CalibrationPoint, *, phase: str, point_tag: str) -> None:
        phase_text = str(phase or ("h2o" if point.is_h2o_point else "co2")).strip().lower()
        self._emit_stage_event(
            current=self._stage_label_for_point(point, phase=phase_text),
            point=point,
            phase=phase_text,
            point_tag=point_tag,
            wait_reason="准备开路采样",
        )
        self._append_pressure_trace_row(
            point=point,
            route=phase_text,
            point_phase=phase_text,
            point_tag=point_tag,
            trace_stage="sampling_begin",
            trigger_reason="ambient_open_route",
            pressure_target_hpa=None,
            refresh_pace_state=False,
            note="route_open=true pressure_control=skipped",
        )
        self._sample_and_log(point, phase=phase_text, point_tag=point_tag)

    def _observe_startup_pressure_hold(self, cfg: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        hold_s = max(0.0, float(cfg.get("hold_s", 15.0)))
        interval_s = max(0.05, float(cfg.get("sample_interval_s", 1.0)))
        limit_hpa = max(0.0, float(cfg.get("max_abs_drift_hpa", 3.0)))

        self._disable_pressure_controller_output(reason="during startup pressure hold check")
        if hold_s <= 0:
            value, source = self._read_startup_pressure_precheck_value(cfg)
            return True, {
                "source": source,
                "start_hpa": value,
                "end_hpa": value,
                "max_abs_drift_hpa": 0.0,
                "span_hpa": 0.0,
                "samples": 1 if value is not None else 0,
                "limit_hpa": limit_hpa,
            }

        readings: List[float] = []
        source = "unavailable"
        start = time.time()
        while True:
            if self.stop_event.is_set():
                return False, {
                    "source": source,
                    "start_hpa": readings[0] if readings else None,
                    "end_hpa": readings[-1] if readings else None,
                    "max_abs_drift_hpa": None,
                    "span_hpa": self._span(readings),
                    "samples": len(readings),
                    "limit_hpa": limit_hpa,
                }
            self._check_pause()

            value, source = self._read_startup_pressure_precheck_value(cfg)
            if value is not None:
                readings.append(float(value))

            elapsed = time.time() - start
            if elapsed >= hold_s:
                break
            remain = hold_s - elapsed
            time.sleep(min(interval_s, max(0.05, remain)))

        if not readings:
            return False, {
                "source": source,
                "start_hpa": None,
                "end_hpa": None,
                "max_abs_drift_hpa": None,
                "span_hpa": None,
                "samples": 0,
                "limit_hpa": limit_hpa,
            }

        baseline = readings[0]
        max_abs_drift = max(abs(one - baseline) for one in readings)
        span = self._span(readings)
        return max_abs_drift <= limit_hpa, {
            "source": source,
            "start_hpa": baseline,
            "end_hpa": readings[-1],
            "max_abs_drift_hpa": max_abs_drift,
            "span_hpa": span,
            "samples": len(readings),
            "limit_hpa": limit_hpa,
        }

    def _read_startup_pressure_precheck_value(self, cfg: Dict[str, Any]) -> Tuple[Optional[float], str]:
        prefer_gauge = bool(cfg.get("prefer_gauge", True))
        order = ["pressure_gauge", "pace"] if prefer_gauge else ["pace", "pressure_gauge"]
        for name in order:
            dev = self.devices.get(name)
            if not dev:
                continue
            reader = getattr(dev, "read_pressure", None)
            if not callable(reader):
                continue
            try:
                return float(reader()), name
            except Exception as exc:
                self.log(f"Startup pressure precheck read failed ({name}): {exc}")
        return None, "unavailable"

    def _disable_pressure_controller_output(self, reason: str = "") -> None:
        pace = self.devices.get("pace")
        if not pace:
            return
        set_output = getattr(pace, "set_output", None)
        if not callable(set_output):
            return
        try:
            set_output(False)
            extra = f" ({reason})" if reason else ""
            self.log(f"Pressure controller output=OFF{extra}")
        except Exception as exc:
            self.log(f"Pressure controller output disable failed: {exc}")

    def _configure_pressure_soft_control(self, pace: Any) -> None:
        cfg = {
            "enabled": bool(self._wf("workflow.pressure.soft_control_enabled", False)),
            "use_active_mode": bool(self._wf("workflow.pressure.soft_control_use_active_mode", True)),
            "linear_slew_hpa_per_s": float(self._wf("workflow.pressure.soft_control_linear_slew_hpa_per_s", 10.0)),
            "disallow_overshoot": bool(self._wf("workflow.pressure.soft_control_disallow_overshoot", True)),
        }
        if cfg["enabled"] and not self._soft_pressure_control_cfg_logged:
            self._soft_pressure_control_cfg_logged = True
            self.log(
                "Pressure soft-control config enabled "
                "(engineering-only / non-default; keep off the V1 UI and default production runner, "
                "use only from standalone/headless engineering paths or V2 simulation paths): "
                f"enabled={cfg['enabled']} "
                f"use_active_mode={cfg['use_active_mode']} "
                f"linear_slew_hpa_per_s={cfg['linear_slew_hpa_per_s']:g} "
                f"disallow_overshoot={cfg['disallow_overshoot']}"
            )

        commands: List[Tuple[str, Any, tuple[Any, ...]]] = []
        if cfg["use_active_mode"]:
            commands.append(("set_output_mode_active", getattr(pace, "set_output_mode_active", None), ()))
        else:
            commands.append(("set_output_mode_passive", getattr(pace, "set_output_mode_passive", None), ()))
        commands.append(("set_slew_mode_linear", getattr(pace, "set_slew_mode_linear", None), ()))
        commands.append(("set_slew_rate", getattr(pace, "set_slew_rate", None), (cfg["linear_slew_hpa_per_s"],)))
        commands.append(("set_overshoot_allowed", getattr(pace, "set_overshoot_allowed", None), (not cfg["disallow_overshoot"],)))

        for name, fn, args in commands:
            if not callable(fn):
                self.log(f"Pressure soft-control warning: {name} unsupported; keep existing controller behavior")
                continue
            try:
                fn(*args)
            except Exception as exc:
                self.log(f"Pressure soft-control warning: {name} failed: {exc}")

    def _pressure_sampling_gate_cfg(self, point: CalibrationPoint) -> Dict[str, Any]:
        cfg = {
            "enabled": bool(self._wf("workflow.pressure.adaptive_pressure_sampling_enabled", False)),
            "prefer_gauge": bool(self._wf("workflow.pressure.use_pressure_gauge_for_sampling_gate", True)),
            "poll_s": max(0.05, float(self._wf("workflow.pressure.sampling_gate_poll_s", 0.5))),
            "window_s": max(
                1.0,
                float(
                    self._wf(
                        "workflow.pressure.h2o_sampling_gate_window_s"
                        if point.is_h2o_point
                        else "workflow.pressure.co2_sampling_gate_window_s",
                        12.0 if point.is_h2o_point else 8.0,
                    )
                ),
            ),
            "pressure_span_hpa": max(
                0.0,
                float(
                    self._wf(
                        "workflow.pressure.h2o_sampling_gate_pressure_span_hpa"
                        if point.is_h2o_point
                        else "workflow.pressure.co2_sampling_gate_pressure_span_hpa",
                        0.30 if point.is_h2o_point else 0.20,
                    )
                ),
            ),
            "pressure_fill_s": max(
                0.0,
                float(
                    self._wf(
                        "workflow.pressure.h2o_sampling_gate_pressure_fill_s"
                        if point.is_h2o_point
                        else "workflow.pressure.co2_sampling_gate_pressure_fill_s",
                        8.0 if point.is_h2o_point else 5.0,
                    )
                ),
            ),
            "min_samples": max(
                2,
                int(
                    self._wf(
                        "workflow.pressure.h2o_sampling_gate_min_samples"
                        if point.is_h2o_point
                        else "workflow.pressure.co2_sampling_gate_min_samples",
                        8 if point.is_h2o_point else 6,
                    )
                ),
            ),
            "skip_fixed_post_delay": bool(self._wf("workflow.pressure.skip_fixed_post_stable_delay_when_adaptive", True)),
        }
        if cfg["enabled"] and not self._adaptive_pressure_sampling_cfg_logged:
            self._adaptive_pressure_sampling_cfg_logged = True
            self.log(
                "Adaptive pressure-sampling config enabled "
                "(engineering-only / non-default; keep off the V1 UI and default production runner, "
                "use only from standalone/headless engineering paths or V2 simulation paths): "
                f"enabled={cfg['enabled']} "
                f"prefer_gauge={cfg['prefer_gauge']} "
                f"poll_s={cfg['poll_s']:g} "
                f"co2_window_s={float(self._wf('workflow.pressure.co2_sampling_gate_window_s', 8.0)):g} "
                f"h2o_window_s={float(self._wf('workflow.pressure.h2o_sampling_gate_window_s', 12.0)):g} "
                f"co2_pressure_span_hpa={float(self._wf('workflow.pressure.co2_sampling_gate_pressure_span_hpa', 0.20)):g} "
                f"h2o_pressure_span_hpa={float(self._wf('workflow.pressure.h2o_sampling_gate_pressure_span_hpa', 0.30)):g} "
                f"co2_pressure_fill_s={float(self._wf('workflow.pressure.co2_sampling_gate_pressure_fill_s', 5.0)):g} "
                f"h2o_pressure_fill_s={float(self._wf('workflow.pressure.h2o_sampling_gate_pressure_fill_s', 8.0)):g} "
                f"co2_min_samples={int(self._wf('workflow.pressure.co2_sampling_gate_min_samples', 6))} "
                f"h2o_min_samples={int(self._wf('workflow.pressure.h2o_sampling_gate_min_samples', 8))} "
                f"skip_fixed_post_delay={cfg['skip_fixed_post_delay']}"
            )
        return cfg

    def _read_best_pressure_for_sampling_gate(self, prefer_gauge: bool) -> Tuple[Optional[float], str]:
        order = ["pressure_gauge", "pace"] if prefer_gauge else ["pace", "pressure_gauge"]
        for name in order:
            dev = self.devices.get(name)
            if not dev:
                continue
            reader = getattr(dev, "read_pressure", None)
            if not callable(reader):
                continue
            try:
                value = float(reader())
            except Exception as exc:
                self.log(f"Adaptive pressure gate read failed ({name}): {exc}")
                continue
            if math.isfinite(value):
                return value, name
        return None, "unavailable"

    def _wait_pressure_and_primary_sensor_ready(self, point: CalibrationPoint) -> bool:
        gate_cfg = self._pressure_sampling_gate_cfg(point)
        key = "h2o_ratio_f" if point.is_h2o_point else "co2_ratio_f"
        return self._wait_primary_sensor_stable(
            point,
            value_key=key,
            require_pressure_in_limits=True,
            window_override=gate_cfg["window_s"],
            min_samples_override=gate_cfg["min_samples"],
            read_interval_override=gate_cfg["poll_s"],
            pressure_fill_override=gate_cfg["pressure_fill_s"],
            pressure_window_cfg=gate_cfg,
        )

    def _pressure_output_off_hold_cfg(self, point: CalibrationPoint) -> Dict[str, Any]:
        hold_s = float(
            self._wf(
                "workflow.pressure.h2o_output_off_hold_s" if point.is_h2o_point else "workflow.pressure.co2_output_off_hold_s",
                10.0 if point.is_h2o_point else 6.0,
            )
        )
        limit_hpa = float(
            self._wf(
                "workflow.pressure.h2o_output_off_max_abs_drift_hpa"
                if point.is_h2o_point
                else "workflow.pressure.co2_output_off_max_abs_drift_hpa",
                0.40 if point.is_h2o_point else 0.25,
            )
        )
        cfg = {
            "enabled": bool(self._wf("workflow.pressure.capture_then_hold_enabled", False)),
            "disable_output_during_sampling": bool(self._wf("workflow.pressure.disable_output_during_sampling", True)),
            "prefer_gauge": bool(self._wf("workflow.pressure.output_off_prefer_gauge", True)),
            "sample_interval_s": max(0.05, float(self._wf("workflow.pressure.output_off_sample_interval_s", 0.5))),
            "retry_count": max(0, int(self._wf("workflow.pressure.output_off_retry_count", 1))),
            "hold_s": max(0.0, hold_s),
            "max_abs_drift_hpa": max(0.0, limit_hpa),
        }
        if cfg["enabled"] and not self._pressure_capture_then_hold_cfg_logged:
            self._pressure_capture_then_hold_cfg_logged = True
            self.log(
                "Pressure capture-then-hold config enabled "
                "(engineering-only / non-default; keep off the V1 UI and default production runner, "
                "use only from standalone/headless engineering paths or V2 simulation paths): "
                f"enabled={cfg['enabled']} "
                f"disable_output_during_sampling={cfg['disable_output_during_sampling']} "
                f"prefer_gauge={cfg['prefer_gauge']} "
                f"sample_interval_s={cfg['sample_interval_s']:g} "
                f"retry_count={cfg['retry_count']} "
                f"co2_hold_s={float(self._wf('workflow.pressure.co2_output_off_hold_s', 6.0)):g} "
                f"h2o_hold_s={float(self._wf('workflow.pressure.h2o_output_off_hold_s', 10.0)):g} "
                f"co2_limit_hpa={float(self._wf('workflow.pressure.co2_output_off_max_abs_drift_hpa', 0.25)):g} "
                f"h2o_limit_hpa={float(self._wf('workflow.pressure.h2o_output_off_max_abs_drift_hpa', 0.40)):g}"
            )
        return cfg

    def _read_best_pressure_for_output_off_hold(self, prefer_gauge: bool) -> Tuple[Optional[float], str]:
        order = ["pressure_gauge", "pace"] if prefer_gauge else ["pace", "pressure_gauge"]
        for name in order:
            dev = self.devices.get(name)
            if not dev:
                continue
            reader = getattr(dev, "read_pressure", None)
            if not callable(reader):
                continue
            try:
                value = float(reader())
            except Exception as exc:
                self.log(f"Output-off hold pressure read failed ({name}): {exc}")
                continue
            if math.isfinite(value):
                return value, name
        return None, "unavailable"

    def _read_preseal_pressure_gauge(self) -> Tuple[Optional[float], str]:
        cached_frame = self._latest_fast_signal_frame("pressure_gauge")
        if isinstance(cached_frame, dict):
            values = cached_frame.get("values", {})
            if isinstance(values, dict):
                gauge_value = self._as_float(
                    values.get("pressure_gauge_hpa", values.get("pressure_gauge_raw"))
                )
                if gauge_value is not None and math.isfinite(gauge_value):
                    return gauge_value, "pressure_gauge"
        transition_context = self._pressure_transition_fast_signal_context_active()
        if isinstance(transition_context, dict):
            error_entry = self._sampling_window_fast_signal_error(transition_context, "pressure_gauge")
            if isinstance(error_entry, dict):
                error_text = str(error_entry.get("error") or "").strip()
                if error_text:
                    return None, "pressure_gauge_cache_wait"
            return None, "pressure_gauge_cache_wait"
        gauge = self.devices.get("pressure_gauge")
        if not gauge:
            return None, "unavailable"
        reader = getattr(gauge, "read_pressure", None)
        if not callable(reader):
            return None, "unsupported"
        try:
            value = self._read_pressure_gauge_value(fast=True)
        except Exception as fast_exc:
            self.log(f"Pre-seal pressure-gauge fast read failed: {fast_exc}; retrying normal read")
            try:
                value = self._read_pressure_gauge_value(fast=False)
            except Exception as exc:
                self.log(f"Pre-seal pressure-gauge read failed: {exc}")
                return None, "error"
        if math.isfinite(value):
            return value, "pressure_gauge"
        self.log("Pre-seal pressure-gauge fast read returned invalid value; retrying normal read")
        try:
            value = self._read_pressure_gauge_value(fast=False)
        except Exception as exc:
            self.log(f"Pre-seal pressure-gauge read failed: {exc}")
            return None, "error"
        if math.isfinite(value):
            return value, "pressure_gauge"
        return None, "invalid"

    def _observe_pressure_hold_after_output_off(self, point: CalibrationPoint) -> Tuple[bool, Dict[str, Any]]:
        cfg = self._pressure_output_off_hold_cfg(point)
        hold_s = float(cfg["hold_s"])
        interval_s = float(cfg["sample_interval_s"])
        limit_hpa = float(cfg["max_abs_drift_hpa"])
        prefer_gauge = bool(cfg["prefer_gauge"])

        readings: List[float] = []
        source = "unavailable"
        start_ts = time.time()
        while True:
            if self.stop_event.is_set():
                return False, {
                    "source": source,
                    "start_hpa": readings[0] if readings else None,
                    "end_hpa": readings[-1] if readings else None,
                    "max_abs_drift_hpa": None,
                    "span_hpa": self._span(readings),
                    "samples": len(readings),
                    "limit_hpa": limit_hpa,
                }
            self._check_pause()

            value, source = self._read_best_pressure_for_output_off_hold(prefer_gauge)
            if value is not None:
                readings.append(float(value))

            elapsed = time.time() - start_ts
            if elapsed >= hold_s:
                break
            remain = hold_s - elapsed
            time.sleep(min(interval_s, max(0.05, remain)))

        if not readings:
            return False, {
                "source": source,
                "start_hpa": None,
                "end_hpa": None,
                "max_abs_drift_hpa": None,
                "span_hpa": None,
                "samples": 0,
                "limit_hpa": limit_hpa,
            }

        baseline = readings[0]
        max_abs_drift = max(abs(one - baseline) for one in readings)
        span = self._span(readings)
        return max_abs_drift <= limit_hpa, {
            "source": source,
            "start_hpa": baseline,
            "end_hpa": readings[-1],
            "max_abs_drift_hpa": max_abs_drift,
            "span_hpa": span,
            "samples": len(readings),
            "limit_hpa": limit_hpa,
        }

    def _resolve_valve_target(self, valve: int) -> Tuple[str, int]:
        valves_cfg = self.cfg.get("valves", {})
        relay_map = valves_cfg.get("relay_map", {})
        entry = relay_map.get(str(valve)) if isinstance(relay_map, dict) else None

        relay_name = "relay"
        channel_raw: Any = valve

        if isinstance(entry, dict):
            relay_name = str(entry.get("device", "relay") or "relay")
            channel_raw = entry.get("channel", valve)
        elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
            relay_name = str(entry[0] or "relay")
            channel_raw = entry[1]
        elif isinstance(entry, str):
            text = entry.strip()
            if ":" in text:
                left, right = text.split(":", 1)
                relay_name = left.strip() or "relay"
                channel_raw = right.strip()
            elif text:
                relay_name = text

        channel = self._as_int(channel_raw)
        if channel is None or channel <= 0:
            self.log(f"Invalid relay mapping for valve {valve}: {entry!r}, fallback to relay:{valve}")
            return "relay", valve
        return relay_name, channel

    def _set_logical_valve(self, valve: int, is_open: bool) -> None:
        relay_name, channel = self._resolve_valve_target(valve)
        relay = self.devices.get(relay_name)
        if not relay:
            self.log(f"Relay '{relay_name}' unavailable, valve {valve} (ch={channel}) skipped")
            return
        relay.set_valve(channel, is_open)
        self._relay_state_cache[(relay_name, int(channel))] = bool(is_open)

    def _write_physical_valve_states(self, physical_states: Dict[Tuple[str, int], bool]) -> None:
        grouped_updates: Dict[str, List[Tuple[int, bool]]] = {}
        for (relay_name, channel), state in sorted(physical_states.items()):
            cache_key = (str(relay_name), int(channel))
            if cache_key in self._relay_state_cache and self._relay_state_cache[cache_key] == bool(state):
                continue
            grouped_updates.setdefault(str(relay_name), []).append((int(channel), bool(state)))
        relay_tasks: List[Tuple[str, Any, List[Tuple[int, bool]]]] = []
        for relay_name, updates in sorted(grouped_updates.items()):
            relay = self.devices.get(relay_name)
            if not relay:
                for channel, _state in updates:
                    self.log(f"Relay '{relay_name}' unavailable, channel {channel} skipped")
                continue

            update_map = {int(channel): bool(state) for channel, state in updates}
            coalesced_updates: List[Tuple[int, bool]] = []
            ordered_channels = sorted(update_map.keys())
            if ordered_channels:
                lower = ordered_channels[0]
                upper = ordered_channels[-1]
                coalesced_updates = []
                for channel in range(lower, upper + 1):
                    if channel in update_map:
                        coalesced_updates.append((channel, update_map[channel]))
                        continue
                    cache_key = (str(relay_name), int(channel))
                    if cache_key not in self._relay_state_cache:
                        coalesced_updates = []
                        break
                    coalesced_updates.append((channel, bool(self._relay_state_cache[cache_key])))
            relay_tasks.append((relay_name, relay, coalesced_updates or updates))

        def _apply_one_relay(relay_name: str, relay: Any, updates: List[Tuple[int, bool]]) -> None:
            bulk_fn = getattr(relay, "set_valves_bulk", None)
            if self._relay_bulk_write_enabled() and callable(bulk_fn):
                try:
                    bulk_fn(updates)
                    with self._relay_state_cache_lock:
                        for channel, state in updates:
                            self._relay_state_cache[(relay_name, int(channel))] = bool(state)
                    return
                except Exception as exc:
                    self.log(f"Relay bulk write failed on {relay_name}; fallback to sequential writes: {exc}")

            for channel, state in updates:
                relay.set_valve(channel, state)
                with self._relay_state_cache_lock:
                    self._relay_state_cache[(relay_name, int(channel))] = bool(state)

        if len(relay_tasks) <= 1:
            for relay_name, relay, updates in relay_tasks:
                _apply_one_relay(relay_name, relay, updates)
            return

        errors: List[Tuple[str, Exception]] = []
        error_lock = threading.Lock()

        def _worker(relay_name: str, relay: Any, updates: List[Tuple[int, bool]]) -> None:
            try:
                _apply_one_relay(relay_name, relay, updates)
            except Exception as exc:
                with error_lock:
                    errors.append((relay_name, exc))

        threads: List[threading.Thread] = []
        for relay_name, relay, updates in relay_tasks:
            thread = threading.Thread(
                target=_worker,
                args=(relay_name, relay, updates),
                name=f"relay-write-{relay_name}",
                daemon=True,
            )
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()
        if errors:
            relay_name, exc = errors[0]
            raise RuntimeError(f"Relay write failed on {relay_name}: {exc}") from exc

    def _apply_valve_states(self, open_valves: List[int]) -> None:
        managed = self._managed_valves()
        if not managed:
            return

        open_set = set()
        for v in open_valves:
            iv = self._as_int(v)
            if iv is not None:
                open_set.add(iv)

        physical_states: Dict[Tuple[str, int], bool] = {}
        for valve in managed:
            relay_name, channel = self._resolve_valve_target(valve)
            desired = self._desired_valve_state(valve, open_set)
            key = (relay_name, channel)
            physical_states[key] = physical_states.get(key, False) or desired
        self._write_physical_valve_states(physical_states)

    def _route_baseline_open_valves(self) -> List[int]:
        return []

    def _apply_route_baseline_valves(self) -> None:
        self._apply_valve_states(self._route_baseline_open_valves())

    def _desired_valve_state(self, valve: int, open_set: set[int]) -> bool:
        return valve in open_set

    def _run_point(self, point: CalibrationPoint) -> None:
        self.log(
            f"Point row {point.index} T={point.temp_chamber_c} CO2={point.co2_ppm} "
            f"P={point.target_pressure_hpa}"
        )

        is_subzero = point.temp_chamber_c is not None and float(point.temp_chamber_c) < 0.0
        if point.is_h2o_point and is_subzero:
            self.log("Sub-zero point detected: force gas-path run (skip H2O path)")
        if point.is_h2o_point and not is_subzero:
            self.set_status(f"H2O row {point.index}")
            self._run_h2o_point(point)
        if point.co2_ppm is not None:
            self.set_status(f"CO2 row {point.index}")
            self._run_co2_point(point)

    def _start_chamber_with_recovery(self, chamber: Any) -> bool:
        try:
            chamber.start()
            return True
        except Exception as exc:
            if "START_STATE_MISMATCH" not in str(exc):
                self.log(f"Chamber command error: {exc}")
                return False

            run_state_reader = getattr(chamber, "read_run_state", None)
            run_state: Optional[int] = None
            if callable(run_state_reader):
                try:
                    run_state = self._as_int(run_state_reader())
                except Exception as run_exc:
                    self.log(f"Chamber run-state read failed after START_STATE_MISMATCH: {run_exc}")

            if run_state == 1:
                self.log("Chamber already running after START_STATE_MISMATCH; continue")
                return True

            self.log(
                "Chamber start state mismatch; "
                f"run_state={run_state}, continue and verify by actual temperature trend"
            )
            return True

    def _set_temperature(self, target_c: float) -> bool:
        chamber = self.devices.get("temp_chamber")
        if not chamber:
            return True
        temp_cfg_raw = self.cfg.get("workflow", {}).get("stability", {}).get("temperature", {})
        if not isinstance(temp_cfg_raw, dict):
            temp_cfg_raw = {}
        tol = float(self._wf("workflow.stability.temperature.tol", 0.2))
        timeout_s = float(self._wf("workflow.stability.temperature.timeout_s", 1800))
        soak_s = float(
            self._wf(
                "workflow.stability.temperature.soak_after_reach_s",
                self._wf("workflow.stability.temperature.wait_after_reach_s", 0),
            )
        )
        analyzer_chamber_temp_enabled = bool(
            self._wf("workflow.stability.temperature.analyzer_chamber_temp_enabled", True)
        )
        analyzer_chamber_temp_timeout_s = float(
            self._wf("workflow.stability.temperature.analyzer_chamber_temp_timeout_s", 5400.0)
        )
        command_offset_c = float(self._wf("workflow.stability.temperature.command_offset_c", 0.0) or 0.0)
        wait_for_target_before_continue = bool(
            self._wf("workflow.stability.temperature.wait_for_target_before_continue", True)
        )
        restart_on_target_change = bool(
            self._wf("workflow.stability.temperature.restart_on_target_change", False)
        )
        legacy_transition_check_window_s = self._as_float(temp_cfg_raw.get("transition_check_window_s"))
        legacy_transition_min_delta_c = self._as_float(temp_cfg_raw.get("transition_min_delta_c"))
        continue_wait_while_progress = bool(temp_cfg_raw.get("continue_wait_while_progress", True))
        progress_window_s = self._as_float(temp_cfg_raw.get("progress_window_s"))
        if progress_window_s is None or progress_window_s <= 0:
            progress_window_s = (
                float(legacy_transition_check_window_s)
                if legacy_transition_check_window_s is not None and legacy_transition_check_window_s > 0
                else 300.0
            )
        progress_min_delta_c = self._as_float(temp_cfg_raw.get("progress_min_delta_c"))
        if progress_min_delta_c is None or progress_min_delta_c <= 0:
            progress_min_delta_c = (
                abs(float(legacy_transition_min_delta_c))
                if legacy_transition_min_delta_c is not None and abs(float(legacy_transition_min_delta_c)) > 0
                else 0.05
            )
        hard_max_wait_s = max(0.0, float(temp_cfg_raw.get("hard_max_wait_s", 0.0) or 0.0))
        command_target_c = float(target_c) + command_offset_c
        setpoint_verify_tol_c = max(0.2, tol)
        target_changed = (
            self._last_temp_target_c is None
            or abs(float(self._last_temp_target_c) - float(target_c)) > 1e-9
        )
        if target_changed:
            self._last_temp_target_c = float(target_c)
            self._last_temp_soak_done = False
        need_soak = soak_s > 0 and not self._last_temp_soak_done
        reuse_running_in_tol = bool(self._wf("workflow.stability.temperature.reuse_running_in_tol_without_soak", True))

        current_temp: Optional[float] = None
        current_run_state: Optional[int] = None
        if chamber and target_changed and need_soak and reuse_running_in_tol:
            try:
                temp_reader = getattr(chamber, "read_temp_c", None)
                if callable(temp_reader):
                    temp_value = temp_reader()
                    current_temp = self._as_float(temp_value)
            except Exception as exc:
                self.log(f"Chamber pre-check temp read failed: {exc}")
            try:
                run_reader = getattr(chamber, "read_run_state", None)
                if callable(run_reader):
                    state_value = run_reader()
                    state_int = self._as_int(state_value)
                    current_run_state = state_int
            except Exception as exc:
                self.log(f"Chamber pre-check run-state read failed: {exc}")
            in_target_range = current_temp is not None and abs(current_temp - target_c) <= tol
            chamber_running = current_run_state is None or int(current_run_state) == 1
            if in_target_range and chamber_running:
                self.log(
                    "Chamber already in target range at startup; reuse current thermal state "
                    f"for soak/stability wait: temp={current_temp:.2f}, target={target_c:.2f}, "
                    f"run_state={current_run_state}"
                )
                target_changed = False
            if in_target_range and not chamber_running:
                self.log(
                    "Chamber temp is in target range but controller is not running; "
                    f"restart and wait for stability: temp={current_temp:.2f}, "
                    f"target={target_c:.2f}, run_state={current_run_state}"
                )

        if chamber and not target_changed:
            try:
                current_state: Optional[int] = None
                current_temp_same_target: Optional[float] = None
                if hasattr(chamber, "read_run_state"):
                    try:
                        current_state = self._as_int(chamber.read_run_state())
                    except Exception as exc:
                        self.log(f"Chamber run-state read failed: {exc}")
                if hasattr(chamber, "read_temp_c"):
                    try:
                        current_temp_same_target = self._as_float(chamber.read_temp_c())
                    except Exception as exc:
                        self.log(f"Chamber temp read failed: {exc}")
                if (
                    current_state == 1
                    and self._last_temp_soak_done
                    and current_temp_same_target is not None
                    and abs(float(current_temp_same_target) - float(target_c)) <= tol
                ):
                    self.log(
                        "Chamber target unchanged; keep current command and reuse current thermal state: "
                        f"target={target_c:.2f}, temp={current_temp_same_target:.2f}, run_state={current_state}"
                    )
                    return True
                if current_state == 1:
                    self.log(
                        f"Chamber target unchanged; keep current command: "
                        f"target={target_c:.2f}, run_state={current_state}"
                    )
                else:
                    self.log(
                        f"Chamber target unchanged but controller not running; "
                        f"retry start: target={target_c:.2f}, run_state={current_state}"
                    )
                    if not self._start_chamber_with_recovery(chamber):
                        return False
            except Exception as exc:
                self.log(f"Chamber command error: {exc}")
        elif chamber:
            try:
                # Keep the chamber running across temperature groups by default.
                if target_changed and restart_on_target_change and hasattr(chamber, "stop"):
                    chamber.stop()
                chamber.set_temp_c(command_target_c)
                if hasattr(chamber, "read_set_temp_c"):
                    try:
                        setpoint_readback = chamber.read_set_temp_c()
                        self.log(
                            f"Chamber setpoint readback={setpoint_readback:.2f} "
                            f"(command={command_target_c:.2f})"
                        )
                        if abs(float(setpoint_readback) - command_target_c) > setpoint_verify_tol_c:
                            self.log(
                                "Chamber setpoint readback mismatch; rewrite target once: "
                                f"readback={float(setpoint_readback):.2f}, command={command_target_c:.2f}, "
                                f"tol={setpoint_verify_tol_c:.2f}"
                            )
                            chamber.set_temp_c(command_target_c)
                            retry_readback = chamber.read_set_temp_c()
                            self.log(
                                f"Chamber setpoint readback after rewrite={retry_readback:.2f} "
                                f"(command={command_target_c:.2f})"
                            )
                    except Exception as exc:
                        self.log(f"Chamber setpoint readback failed: {exc}")
                if not self._start_chamber_with_recovery(chamber):
                    return False
                self.log(
                    f"Chamber command sent: set={command_target_c:.2f} (target={target_c:.2f}, "
                    f"offset={command_offset_c:+.2f}), run=on"
                )
                if hasattr(chamber, "read_run_state"):
                    try:
                        state = None
                        for _ in range(8):
                            state = chamber.read_run_state()
                            if int(state) == 1:
                                break
                            time.sleep(0.5)
                        self.log(f"Chamber run state={state} (0=stop,1=run)")
                        if int(state) != 1:
                            self.log("Chamber still stop after start command; retry start once")
                            try:
                                if hasattr(chamber, "stop"):
                                    chamber.stop()
                                    time.sleep(0.5)
                                if not self._start_chamber_with_recovery(chamber):
                                    return False
                                for _ in range(8):
                                    state = chamber.read_run_state()
                                    if int(state) == 1:
                                        break
                                    time.sleep(0.5)
                                self.log(f"Chamber run state after retry={state} (0=stop,1=run)")
                                if self._as_int(state) != 1:
                                    self.log(
                                        "Chamber still stop after retry start command; "
                                        "continue and verify by actual temperature trend"
                                    )
                            except Exception as retry_exc:
                                self.log(f"Chamber retry-start failed: {retry_exc}")
                                return False
                    except Exception as exc:
                        self.log(f"Chamber run-state read failed: {exc}")
            except Exception as exc:
                self.log(f"Chamber command error: {exc}")
                return False

        if not wait_for_target_before_continue:
            self.log(
                "Chamber target wait skipped by configuration: "
                f"target={target_c:.2f}, command={command_target_c:.2f}"
            )
            return True

        self.log(
            "Chamber target wait config: "
            f"target={target_c:.2f}, reach_timeout={timeout_s:.1f}s, "
            f"continue_wait_while_progress={continue_wait_while_progress}, "
            f"progress_window={float(progress_window_s):.1f}s, "
            f"progress_min_delta={float(progress_min_delta_c):.2f}C, "
            f"hard_max_wait={'disabled' if hard_max_wait_s <= 0 else f'{hard_max_wait_s:.1f}s'}"
        )

        reach_start: Optional[float] = None
        last_report_s = 0.0
        start = time.time()
        last_temp: Optional[float] = None
        best_abs_error: Optional[float] = None
        last_progress_ts = start
        last_progress_log_ts: Optional[float] = None

        while True:
            if self.stop_event.is_set():
                return False
            self._check_pause()
            self._refresh_live_analyzer_snapshots(reason="temperature target wait")

            temp = chamber.read_temp_c() if chamber else None
            now = time.time()
            if temp is not None:
                last_temp = float(temp)
                abs_error = abs(float(temp) - float(target_c))
                if best_abs_error is None or abs_error <= (best_abs_error - float(progress_min_delta_c)):
                    best_abs_error = abs_error
                    last_progress_ts = now
                in_tol = abs_error <= tol
                if in_tol and reach_start is None:
                    reach_start = now
                    if need_soak:
                        self.log(
                            f"Chamber reached target range: temp={temp:.2f}, "
                            f"start soak {int(soak_s)}s"
                        )
                    else:
                        if not self._wait_analyzer_chamber_temp_stable(target_c):
                            self.log("Analyzer chamber temp did not stabilize after chamber reached target")
                            return False
                        self._last_temp_soak_done = True
                        return True

                # User process requirement: once target range is reached, perform a fixed soak.
                # If the soak finishes while out of tolerance, restart soak after re-entering range.
                if need_soak and reach_start is not None:
                    remain = soak_s - (now - reach_start)
                    if remain <= 0:
                        if in_tol:
                            if not self._wait_analyzer_chamber_temp_stable(target_c):
                                self.log("Analyzer chamber temp did not stabilize after chamber soak")
                                return False
                            self._last_temp_soak_done = True
                            self.log(f"Chamber soak done: temp={temp:.2f}")
                            return True

                        self.log(
                            f"Chamber soak expired out of tol: temp={temp:.2f}, "
                            f"target={target_c:.2f}, tol=±{tol:.2f}; restart soak"
                        )
                        reach_start = None
                        last_report_s = 0.0
                        continue

                    # Avoid flooding logs; report approximately once per minute.
                    if now - last_report_s >= 60:
                        last_report_s = now
                        self.log(
                            f"Chamber soaking... remaining {int(remain)}s, "
                            f"temp={temp:.2f}, target={target_c:.2f}"
                        )

                if (
                    reach_start is None
                    and continue_wait_while_progress
                    and timeout_s > 0
                    and (now - start) >= timeout_s
                ):
                    last_progress_age_s = max(0.0, now - last_progress_ts)
                    if last_progress_age_s <= float(progress_window_s) and (
                        last_progress_log_ts is None or (now - last_progress_log_ts) >= 60.0
                    ):
                        last_progress_log_ts = now
                        best_error_text = f"{best_abs_error:.2f}" if best_abs_error is not None else "NA"
                        self.log(
                            "Chamber still moving toward target after reach timeout; continue waiting: "
                            f"temp={float(temp):.2f}, target={target_c:.2f}, "
                            f"best_abs_error={best_error_text}, last_progress_age_s={last_progress_age_s:.1f}"
                        )

            if reach_start is None:
                elapsed_s = max(0.0, now - start)
                last_progress_age_s = max(0.0, now - last_progress_ts)
                if hard_max_wait_s > 0 and elapsed_s >= hard_max_wait_s:
                    timeout_run_state: Optional[int] = None
                    if chamber and hasattr(chamber, "read_run_state"):
                        try:
                            timeout_run_state = self._as_int(chamber.read_run_state())
                        except Exception as exc:
                            self.log(f"Chamber timeout run-state read failed: {exc}")
                    self.log(
                        "Chamber wait stalled before reaching target: "
                        f"target={target_c:.2f}, last_temp={last_temp}, "
                        f"best_abs_error={best_abs_error}, last_progress_age_s={last_progress_age_s:.1f}, "
                        f"run_state={timeout_run_state}, reason=hard_max_wait_exceeded"
                    )
                    return False
                if (not continue_wait_while_progress) and timeout_s > 0 and elapsed_s >= timeout_s:
                    timeout_run_state = None
                    if chamber and hasattr(chamber, "read_run_state"):
                        try:
                            timeout_run_state = self._as_int(chamber.read_run_state())
                        except Exception as exc:
                            self.log(f"Chamber timeout run-state read failed: {exc}")
                    self.log(
                        "Chamber wait stalled before reaching target: "
                        f"target={target_c:.2f}, last_temp={last_temp}, "
                        f"best_abs_error={best_abs_error}, last_progress_age_s={last_progress_age_s:.1f}, "
                        f"run_state={timeout_run_state}, reason=reach_timeout"
                    )
                    return False
                if progress_window_s > 0 and last_progress_age_s >= float(progress_window_s):
                    timeout_run_state = None
                    if chamber and hasattr(chamber, "read_run_state"):
                        try:
                            timeout_run_state = self._as_int(chamber.read_run_state())
                        except Exception as exc:
                            self.log(f"Chamber timeout run-state read failed: {exc}")
                    self.log(
                        "Chamber wait stalled before reaching target: "
                        f"target={target_c:.2f}, last_temp={last_temp}, "
                        f"best_abs_error={best_abs_error}, last_progress_age_s={last_progress_age_s:.1f}, "
                        f"run_state={timeout_run_state}"
                    )
                    return False

            time.sleep(1.0)

    def _run_co2_point(
        self,
        point: CalibrationPoint,
        pressure_points: Optional[List[CalibrationPoint]] = None,
        next_route_context: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._h2o_pressure_prepared_target = None
        route_context = self._route_entry_context_for_co2_source(point, pressure_points=pressure_points)
        route_point_tag = str(route_context.get("point_tag") or self._co2_point_tag(point))
        self._apply_idle_route_isolation(reason="before CO2 chamber wait")
        self._emit_stage_event(
            current=f"{self._stage_label_for_point(point, phase='co2', include_pressure=False)} 温箱等待 {float(point.temp_chamber_c):g}°C",
            point=point,
            phase="co2",
            wait_reason="等待温度箱到位",
        )
        if not self._set_temperature_for_point(point, phase="co2"):
            self.log(f"CO2 row {point.index} skipped: chamber did not stabilize")
            self._discard_pending_route_handoff(
                point=point,
                phase="co2",
                point_tag=route_point_tag,
                reason="CO2 chamber did not stabilize before route open",
            )
            return

        self._capture_temperature_calibration_snapshot(point, route_type="co2")
        pressure_refs = pressure_points or [point]
        ambient_open_refs, sealed_control_refs = self._split_pressure_execution_points(pressure_refs)
        self.log("Pressure controller kept at atmosphere for CO2 route conditioning")
        self._open_co2_route_for_conditioning(point, point_tag=route_point_tag)
        if not self._wait_co2_route_soak_before_seal(point):
            self.log(f"CO2 row {point.index} skipped: route precondition failed before sealing")
            self._cleanup_co2_route(reason="after CO2 route soak interrupted")
            return

        if self._gas_route_dewpoint_gate_enabled():
            runtime_state = self._point_runtime_state(point, phase="co2") or {}
            gate_status = str(runtime_state.get("flush_gate_status") or "").strip().lower()
            if gate_status == "pass":
                self.log("CO2 preseal dewpoint gate passed")
            elif gate_status == "timeout":
                self.log("CO2 preseal dewpoint gate timed out; continue due to engineering policy")
            else:
                self.log("CO2 preseal dewpoint gate completed")
        else:
            self.log("CO2 preseal dewpoint gate skipped by configuration")
        if not self._wait_co2_preseal_primary_sensor_gate(point):
            self.log(f"CO2 row {point.index} skipped: analyzer precondition failed before downstream sampling/seal")
            self._cleanup_co2_route(reason="after CO2 preseal analyzer gate failure")
            return

        ambient_exports_deferred_until_seal = False

        def _flush_co2_route_seal_deferred_exports(reason: str) -> None:
            nonlocal ambient_exports_deferred_until_seal
            if not ambient_exports_deferred_until_seal:
                return
            self._flush_deferred_sample_exports(reason=reason)
            self._flush_deferred_point_exports(reason=reason)
            ambient_exports_deferred_until_seal = False

        for ambient_idx, ambient_ref in enumerate(ambient_open_refs):
            if self.stop_event.is_set():
                _flush_co2_route_seal_deferred_exports(reason="before CO2 ambient interruption cleanup")
                self._cleanup_co2_route(reason="after CO2 ambient open-route interrupted")
                return
            self._check_pause()
            sample_point = self._build_co2_pressure_point(point, ambient_ref)
            point_tag = self._co2_point_tag(sample_point)
            pressure_label = self._pressure_target_label(sample_point) or self._AMBIENT_PRESSURE_LABEL
            self.set_status(f"CO2 {int(round(float(sample_point.co2_ppm or 0)))}ppm {pressure_label}")
            defer_until_route_sealed = bool(sealed_control_refs) and ambient_idx + 1 == len(ambient_open_refs)
            if defer_until_route_sealed:
                ambient_exports_deferred_until_seal = self._request_sample_export_deferral(
                    sample_point,
                    phase="co2",
                    point_tag=point_tag,
                    mode="route_seal",
                )
            try:
                self._sample_open_route_point(sample_point, phase="co2", point_tag=point_tag)
            finally:
                if defer_until_route_sealed:
                    self._clear_requested_sample_export_deferral(
                        sample_point,
                        phase="co2",
                        point_tag=point_tag,
                    )

        if not sealed_control_refs:
            self._cleanup_co2_route(reason="after CO2 source complete")
            return

        seal_start_index: Optional[int] = None
        for idx, pressure_point in enumerate(sealed_control_refs):
            if self.stop_event.is_set():
                _flush_co2_route_seal_deferred_exports(reason="before CO2 pressure-seal interruption cleanup")
                self._cleanup_co2_route(reason="after CO2 pressure-seal interrupted")
                return
            self._check_pause()
            seal_point = self._build_co2_pressure_point(point, pressure_point)
            if self._pressurize_route_for_sealed_points(
                seal_point,
                route="co2",
                sealed_control_refs=sealed_control_refs,
            ):
                _flush_co2_route_seal_deferred_exports(
                    reason=f"after CO2 route sealed {seal_point.index}:{self._co2_point_tag(seal_point)}"
                )
                seal_start_index = idx
                break
            self.log(
                f"CO2 {seal_point.co2_ppm} ppm @ {seal_point.target_pressure_hpa} hPa skipped: "
                f"route sealing failed"
            )

        if seal_start_index is None:
            _flush_co2_route_seal_deferred_exports(reason="after CO2 pressure-seal failure")
            self.log(f"CO2 row {point.index} skipped: route sealing failed")
            self._cleanup_co2_route(reason="after CO2 pressure-seal failure")
            return

        last_sample_point: Optional[CalibrationPoint] = None
        last_point_tag = ""
        handoff_armed = False
        active_pressure_refs = sealed_control_refs[seal_start_index:]
        for pressure_idx, pressure_point in enumerate(active_pressure_refs):
            if self.stop_event.is_set():
                break
            self._check_pause()
            sample_point = self._build_co2_pressure_point(point, pressure_point)
            point_tag = self._co2_point_tag(sample_point)
            pressure_label = self._pressure_target_label(sample_point) or "--"
            self.set_status(f"CO2 {int(round(float(sample_point.co2_ppm or 0)))}ppm {pressure_label}")
            self._emit_stage_event(
                current=self._stage_label_for_point(sample_point, phase="co2"),
                point=sample_point,
                phase="co2",
                point_tag=point_tag,
                wait_reason="准备控压",
            )

            pressure_ok = self._set_pressure_to_target(sample_point)
            retry_total = self._co2_pressure_timeout_reseal_retries()
            retry_done = 0
            while not pressure_ok and retry_done < retry_total:
                retry_done += 1
                pressure_ok = self._retry_co2_pressure_point_after_timeout(
                    point,
                    sample_point,
                    attempt=retry_done,
                    total=retry_total,
                )

            if not pressure_ok:
                self.log(
                    f"CO2 {sample_point.co2_ppm} ppm @ {sample_point.target_pressure_hpa} hPa skipped: "
                    f"pressure did not stabilize"
                )
                continue

            if not self._wait_after_pressure_stable_before_sampling(sample_point):
                self.log(
                    f"CO2 {sample_point.co2_ppm} ppm @ {sample_point.target_pressure_hpa} hPa skipped: "
                    f"post-pressure hold before sampling interrupted"
                )
                continue

            is_last_pressure_point = pressure_idx + 1 == len(active_pressure_refs)
            if next_route_context and is_last_pressure_point:
                self._sample_handoff_request = {
                    "current_phase": "co2",
                    "current_point_tag": point_tag,
                    "next_point": next_route_context["point"],
                    "next_phase": next_route_context["phase"],
                    "next_point_tag": next_route_context["point_tag"],
                    "next_open_valves": list(next_route_context["open_valves"]),
                    "armed": False,
                }
            self._sample_and_log(sample_point, phase="co2", point_tag=point_tag)
            request = dict(self._sample_handoff_request or {})
            handoff_armed = handoff_armed or bool(request.get("armed"))
            self._sample_handoff_request = None
            last_sample_point = sample_point
            last_point_tag = point_tag

        if self.stop_event.is_set():
            self._cleanup_co2_route(reason="after CO2 source interrupted")
            return

        if handoff_armed:
            return

        if (
            next_route_context
            and last_sample_point is not None
            and self._begin_pending_route_handoff(
                current_point=last_sample_point,
                current_phase="co2",
                current_point_tag=last_point_tag,
                next_point=next_route_context["point"],
                next_phase=next_route_context["phase"],
                next_point_tag=next_route_context["point_tag"],
                next_open_valves=next_route_context["open_valves"],
            )
        ):
            return

        self._cleanup_co2_route(reason="after CO2 source complete")

    def _set_co2_route_baseline(self, *, reason: str = "") -> None:
        self._set_pressure_controller_vent(True, reason=reason)
        self._apply_route_baseline_valves()
        self.log("CO2 route baseline applied: gas_main=OFF flow_switch=OFF h2o_path=OFF hold=OFF")

    def _cleanup_co2_route(self, *, reason: str = "") -> None:
        self._set_co2_route_baseline(reason=reason)

    def _apply_idle_route_isolation(self, *, reason: str = "") -> None:
        self._set_pressure_controller_vent(False, reason=reason)
        self._apply_route_baseline_valves()
        extra = f" ({reason})" if str(reason or "").strip() else ""
        self.log(f"Idle route isolation applied{extra}: route valves closed and pressure controller disconnected from atmosphere")

    def _cleanup_h2o_route(self, point: CalibrationPoint, *, reason: str = "") -> None:
        self._set_pressure_controller_vent(True, reason=reason)
        self._apply_route_baseline_valves()

    def _mark_post_h2o_co2_zero_flush_pending(self) -> None:
        if self._route_mode() == "h2o_then_co2":
            self._post_h2o_co2_zero_flush_pending = True

    def _has_special_co2_zero_flush_pending(self) -> bool:
        return self._post_h2o_co2_zero_flush_pending or self._initial_co2_zero_flush_pending

    def _is_zero_co2_point(self, point: CalibrationPoint) -> bool:
        ppm_value = self._as_float(getattr(point, "co2_ppm", None))
        if ppm_value is None:
            return False
        zero_values_raw = self._wf("workflow.stability.co2_route.post_h2o_zero_ppm_values", [0])
        zero_values: set[int] = set()
        if isinstance(zero_values_raw, (list, tuple, set)):
            for value in zero_values_raw:
                iv = self._as_int(value)
                if iv is not None:
                    zero_values.add(iv)
        else:
            iv = self._as_int(zero_values_raw)
            if iv is not None:
                zero_values.add(iv)
        if not zero_values:
            zero_values = {0}
        return int(round(ppm_value)) in zero_values

    def _cold_co2_zero_flush_temp_key(self, point: CalibrationPoint) -> Optional[float]:
        temp_c = self._as_float(getattr(point, "temp_chamber_c", None))
        if temp_c is None or temp_c > 0.0:
            return None
        return round(float(temp_c), 6)

    def _should_apply_cold_co2_zero_flush(self, point: CalibrationPoint) -> bool:
        if not self._is_zero_co2_point(point):
            return False
        temp_key = self._cold_co2_zero_flush_temp_key(point)
        if temp_key is None:
            return False
        return self._last_cold_co2_zero_flush_temp_c != temp_key

    def _gas_route_dewpoint_gate_enabled(self) -> bool:
        return bool(self._wf("workflow.stability.gas_route_dewpoint_gate_enabled", False))

    def _gas_route_dewpoint_gate_policy(self) -> str:
        policy = str(self._wf("workflow.stability.gas_route_dewpoint_gate_policy", "warn") or "warn")
        normalized = policy.strip().lower()
        if normalized not in {"reject", "warn", "pass"}:
            return "warn"
        return normalized

    def _water_route_dewpoint_gate_enabled(self) -> bool:
        return bool(self._wf("workflow.stability.water_route_dewpoint_gate_enabled", False))

    def _water_route_dewpoint_gate_policy(self) -> str:
        raw = self._wf("workflow.stability.water_route_dewpoint_gate_policy", "warn")
        normalized = str(raw or "warn").strip().lower()
        if normalized not in {"reject", "warn", "pass"}:
            return "warn"
        return normalized

    def _gas_route_dewpoint_gate_cfg(self) -> Dict[str, Any]:
        return {
            "enabled": self._gas_route_dewpoint_gate_enabled(),
            "policy": self._gas_route_dewpoint_gate_policy(),
            "window_s": max(5.0, float(self._wf("workflow.stability.gas_route_dewpoint_gate_window_s", 60.0) or 60.0)),
            "max_total_wait_s": max(
                0.0,
                float(self._wf("workflow.stability.gas_route_dewpoint_gate_max_total_wait_s", 300.0) or 300.0),
            ),
            "poll_s": max(0.2, float(self._wf("workflow.stability.gas_route_dewpoint_gate_poll_s", 2.0) or 2.0)),
            "tail_span_max_c": max(
                0.0,
                float(self._wf("workflow.stability.gas_route_dewpoint_gate_tail_span_max_c", 0.45) or 0.45),
            ),
            "tail_slope_abs_max_c_per_s": max(
                0.0,
                float(
                    self._wf("workflow.stability.gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s", 0.005)
                    or 0.005
                ),
            ),
            "rebound_window_s": max(
                1.0,
                float(self._wf("workflow.stability.gas_route_dewpoint_gate_rebound_window_s", 180.0) or 180.0),
            ),
            "rebound_min_rise_c": max(
                0.0,
                float(self._wf("workflow.stability.gas_route_dewpoint_gate_rebound_min_rise_c", 1.3) or 1.3),
            ),
            "log_interval_s": max(
                1.0,
                float(self._wf("workflow.stability.gas_route_dewpoint_gate_log_interval_s", 15.0) or 15.0),
            ),
        }

    def _water_route_dewpoint_gate_cfg(self) -> Dict[str, Any]:
        return {
            "enabled": self._water_route_dewpoint_gate_enabled(),
            "policy": self._water_route_dewpoint_gate_policy(),
            "window_s": max(
                5.0,
                float(
                    self._wf(
                        "workflow.stability.water_route_dewpoint_gate_window_s",
                        self._wf("workflow.stability.gas_route_dewpoint_gate_window_s", 60.0),
                    )
                    or 60.0
                ),
            ),
            "max_total_wait_s": max(
                0.0,
                float(
                    self._wf(
                        "workflow.stability.water_route_dewpoint_gate_max_total_wait_s",
                        self._wf("workflow.stability.gas_route_dewpoint_gate_max_total_wait_s", 300.0),
                    )
                    or 300.0
                ),
            ),
            "poll_s": max(
                0.2,
                float(
                    self._wf(
                        "workflow.stability.water_route_dewpoint_gate_poll_s",
                        self._wf("workflow.stability.gas_route_dewpoint_gate_poll_s", 2.0),
                    )
                    or 2.0
                ),
            ),
            "tail_span_max_c": max(
                0.0,
                float(
                    self._wf(
                        "workflow.stability.water_route_dewpoint_gate_tail_span_max_c",
                        self._wf("workflow.stability.gas_route_dewpoint_gate_tail_span_max_c", 0.45),
                    )
                    or 0.45
                ),
            ),
            "tail_slope_abs_max_c_per_s": max(
                0.0,
                float(
                    self._wf(
                        "workflow.stability.water_route_dewpoint_gate_tail_slope_abs_max_c_per_s",
                        self._wf("workflow.stability.gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s", 0.005),
                    )
                    or 0.005
                ),
            ),
            "rebound_window_s": max(
                1.0,
                float(
                    self._wf(
                        "workflow.stability.water_route_dewpoint_gate_rebound_window_s",
                        self._wf("workflow.stability.gas_route_dewpoint_gate_rebound_window_s", 180.0),
                    )
                    or 180.0
                ),
            ),
            "rebound_min_rise_c": max(
                0.0,
                float(
                    self._wf(
                        "workflow.stability.water_route_dewpoint_gate_rebound_min_rise_c",
                        self._wf("workflow.stability.gas_route_dewpoint_gate_rebound_min_rise_c", 1.3),
                    )
                    or 1.3
                ),
            ),
            "log_interval_s": max(
                1.0,
                float(
                    self._wf(
                        "workflow.stability.water_route_dewpoint_gate_log_interval_s",
                        self._wf("workflow.stability.gas_route_dewpoint_gate_log_interval_s", 15.0),
                    )
                    or 15.0
                ),
            ),
        }

    def _read_precondition_dewpoint_gate_snapshot(self) -> Dict[str, Any]:
        dew = self.devices.get("dewpoint")
        if dew is None:
            raise RuntimeError("dewpoint_meter_unavailable")

        def _snapshot_from_payload(payload: Any) -> Optional[Dict[str, Any]]:
            if not isinstance(payload, dict):
                return None
            snapshot = {
                "dewpoint_c": self._as_float(payload.get("dewpoint_c")),
                "temp_c": self._as_float(payload.get("temp_c")),
                "rh_pct": self._as_float(payload.get("rh_pct")),
            }
            if snapshot["dewpoint_c"] is None:
                return None
            return snapshot

        fast_timeout_s = self._sampling_dewpoint_fast_timeout_s()
        fast_reader = getattr(dew, "get_current_fast", None)
        dew_data: Any = None
        if callable(fast_reader):
            try:
                dew_data = fast_reader(timeout_s=fast_timeout_s)
            except TypeError:
                dew_data = fast_reader()
            snapshot = _snapshot_from_payload(dew_data)
            if snapshot is not None:
                return snapshot
            try:
                dew_data = fast_reader(timeout_s=max(0.8, fast_timeout_s), clear_buffer=True)
            except TypeError:
                try:
                    dew_data = fast_reader(timeout_s=max(0.8, fast_timeout_s))
                except TypeError:
                    dew_data = fast_reader()
            snapshot = _snapshot_from_payload(dew_data)
            if snapshot is not None:
                return snapshot

        reader = getattr(dew, "get_current", None)
        if callable(reader):
            try:
                dew_data = reader(timeout_s=max(0.8, fast_timeout_s), attempts=1)
            except TypeError:
                try:
                    dew_data = reader(timeout_s=max(0.8, fast_timeout_s))
                except TypeError:
                    dew_data = reader()
        snapshot = _snapshot_from_payload(dew_data)
        if snapshot is None:
            raise RuntimeError("dewpoint_gate_read_missing")
        return snapshot

    def _build_co2_route_dewpoint_gate_row(
        self,
        *,
        total_elapsed_s: float,
        snapshot: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "timestamp": datetime.now().isoformat(timespec="milliseconds"),
            "phase_elapsed_s": max(0.0, float(total_elapsed_s)),
            "phase": "co2_route_precondition",
            "controller_vent_state": "VENT_ON",
            "dewpoint_c": snapshot.get("dewpoint_c"),
            "dewpoint_temp_c": snapshot.get("temp_c"),
            "dewpoint_rh_percent": snapshot.get("rh_pct"),
        }

    def _build_h2o_route_dewpoint_gate_row(
        self,
        *,
        total_elapsed_s: float,
        snapshot: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "timestamp": datetime.now().isoformat(timespec="milliseconds"),
            "phase_elapsed_s": max(0.0, float(total_elapsed_s)),
            "phase": "h2o_route_precondition",
            "controller_vent_state": "VENT_ON",
            "dewpoint_c": snapshot.get("dewpoint_c"),
            "dewpoint_temp_c": snapshot.get("temp_c"),
            "dewpoint_rh_percent": snapshot.get("rh_pct"),
        }

    def _wait_co2_route_dewpoint_gate_before_seal(
        self,
        point: CalibrationPoint,
        *,
        base_soak_s: float,
        log_context: str,
    ) -> bool:
        cfg = self._gas_route_dewpoint_gate_cfg()
        if not bool(cfg.get("enabled")):
            return True

        gate_begin_ts = time.time()
        gate_rows: List[Dict[str, Any]] = []
        last_log_ts = 0.0
        consecutive_read_missing = 0
        max_transient_read_missing = 3
        self._append_pressure_trace_row(
            point=point,
            route="co2",
            point_phase="co2",
            trace_stage="co2_precondition_dewpoint_gate_begin",
            pressure_target_hpa=point.target_pressure_hpa,
            refresh_pace_state=False,
            note=(
                f"base_soak_s={float(base_soak_s):.3f} "
                f"window_s={float(cfg['window_s']):.3f} "
                f"max_gate_wait_after_soak_s={float(cfg['max_total_wait_s']):.3f} "
                f"policy={cfg['policy']}"
            ),
        )
        while True:
            if self.stop_event.is_set():
                return False
            self._check_pause()
            try:
                snapshot = self._read_precondition_dewpoint_gate_snapshot()
            except Exception as exc:
                reason = str(exc) or "dewpoint_gate_read_failed"
                total_elapsed_s = float(base_soak_s) + max(0.0, time.time() - gate_begin_ts)
                if reason == "dewpoint_gate_read_missing":
                    consecutive_read_missing += 1
                    if consecutive_read_missing < max_transient_read_missing:
                        now_ts = time.time()
                        if (now_ts - last_log_ts) >= float(cfg["log_interval_s"]):
                            last_log_ts = now_ts
                            self.log(
                                "CO2 route precondition dewpoint gate waiting: "
                                f"row={point.index} read missing; retry "
                                f"{consecutive_read_missing}/{max_transient_read_missing - 1} "
                                f"time_to_gate={total_elapsed_s:.1f}s"
                            )
                        time.sleep(float(cfg["poll_s"]))
                        continue
                self._set_point_runtime_fields(
                    point,
                    phase="co2",
                    dewpoint_time_to_gate=round(total_elapsed_s, 3),
                    dewpoint_tail_span_60s=None,
                    dewpoint_tail_slope_60s=None,
                    dewpoint_rebound_detected=None,
                    flush_gate_status="fail",
                    flush_gate_reason=reason,
                )
                self._append_pressure_trace_row(
                    point=point,
                    route="co2",
                    point_phase="co2",
                    trace_stage="co2_precondition_dewpoint_gate_end",
                    pressure_target_hpa=point.target_pressure_hpa,
                    refresh_pace_state=False,
                    dewpoint_time_to_gate=round(total_elapsed_s, 3),
                    flush_gate_status="fail",
                    flush_gate_reason=reason,
                    note="CO2 route precondition dewpoint gate failed before seal",
                )
                self.log(
                    "CO2 route precondition dewpoint gate failed before seal: "
                    f"row={point.index} reason={reason}"
                )
                return False

            consecutive_read_missing = 0
            gate_elapsed_after_soak_s = max(0.0, time.time() - gate_begin_ts)
            total_elapsed_s = float(base_soak_s) + gate_elapsed_after_soak_s
            gate_rows.append(
                self._build_co2_route_dewpoint_gate_row(
                    total_elapsed_s=total_elapsed_s,
                    snapshot=snapshot,
                )
            )
            gate_eval = evaluate_dewpoint_flush_gate(
                gate_rows,
                min_flush_s=float(base_soak_s),
                gate_window_s=float(cfg["window_s"]),
                max_tail_span_c=float(cfg["tail_span_max_c"]),
                max_abs_tail_slope_c_per_s=float(cfg["tail_slope_abs_max_c_per_s"]),
                rebound_window_s=float(cfg["rebound_window_s"]),
                rebound_min_rise_c=float(cfg["rebound_min_rise_c"]),
                include_rebound_in_gate=True,
            )
            dewpoint_tail_span_60s = self._as_float(gate_eval.get("dewpoint_tail_span_60s"))
            dewpoint_tail_slope_60s = self._as_float(gate_eval.get("dewpoint_tail_slope_60s"))
            dewpoint_rebound_detected = bool(gate_eval.get("dewpoint_rebound_detected"))
            dewpoint_time_to_gate = self._as_float(gate_eval.get("dewpoint_time_to_gate"))
            if bool(gate_eval.get("gate_pass")):
                self._set_point_runtime_fields(
                    point,
                    phase="co2",
                    dewpoint_time_to_gate=dewpoint_time_to_gate,
                    dewpoint_tail_span_60s=dewpoint_tail_span_60s,
                    dewpoint_tail_slope_60s=dewpoint_tail_slope_60s,
                    dewpoint_rebound_detected=dewpoint_rebound_detected,
                    flush_gate_status="pass",
                    flush_gate_reason="",
                )
                self._append_pressure_trace_row(
                    point=point,
                    route="co2",
                    point_phase="co2",
                    trace_stage="co2_precondition_dewpoint_gate_end",
                    pressure_target_hpa=point.target_pressure_hpa,
                    refresh_pace_state=False,
                    dewpoint_c=snapshot.get("dewpoint_c"),
                    dew_temp_c=snapshot.get("temp_c"),
                    dew_rh_pct=snapshot.get("rh_pct"),
                    dewpoint_time_to_gate=dewpoint_time_to_gate,
                    dewpoint_tail_span_60s=dewpoint_tail_span_60s,
                    dewpoint_tail_slope_60s=dewpoint_tail_slope_60s,
                    dewpoint_rebound_detected=dewpoint_rebound_detected,
                    flush_gate_status="pass",
                    flush_gate_reason="",
                    note=f"context={log_context} result=pass",
                )
                self.log(
                    "CO2 route dewpoint gate passed after fixed precondition: "
                    f"row={point.index} time_to_gate={dewpoint_time_to_gate} "
                    f"tail_span_60s={dewpoint_tail_span_60s} "
                    f"tail_slope_60s={dewpoint_tail_slope_60s} "
                    f"rebound={dewpoint_rebound_detected}"
                )
                return True

            if float(cfg["max_total_wait_s"]) > 0 and gate_elapsed_after_soak_s >= float(cfg["max_total_wait_s"]):
                reason = str(gate_eval.get("gate_reason") or "dewpoint_gate_timeout")
                if "max_total_wait_exceeded" not in reason:
                    reason = f"{reason};max_total_wait_exceeded" if reason else "max_total_wait_exceeded"
                self._set_point_runtime_fields(
                    point,
                    phase="co2",
                    dewpoint_time_to_gate=round(total_elapsed_s, 3),
                    dewpoint_tail_span_60s=dewpoint_tail_span_60s,
                    dewpoint_tail_slope_60s=dewpoint_tail_slope_60s,
                    dewpoint_rebound_detected=dewpoint_rebound_detected,
                    flush_gate_status="timeout",
                    flush_gate_reason=reason,
                )
                self._append_pressure_trace_row(
                    point=point,
                    route="co2",
                    point_phase="co2",
                    trace_stage="co2_precondition_dewpoint_gate_end",
                    pressure_target_hpa=point.target_pressure_hpa,
                    refresh_pace_state=False,
                    dewpoint_c=snapshot.get("dewpoint_c"),
                    dew_temp_c=snapshot.get("temp_c"),
                    dew_rh_pct=snapshot.get("rh_pct"),
                    dewpoint_time_to_gate=round(total_elapsed_s, 3),
                    dewpoint_tail_span_60s=dewpoint_tail_span_60s,
                    dewpoint_tail_slope_60s=dewpoint_tail_slope_60s,
                    dewpoint_rebound_detected=dewpoint_rebound_detected,
                    flush_gate_status="timeout",
                    flush_gate_reason=reason,
                    note=f"context={log_context} result=timeout policy={cfg['policy']}",
                )
                if str(cfg["policy"]) in {"warn", "pass"}:
                    self.log(
                        "CO2 route precondition dewpoint gate timed out after fixed purge; "
                        f"continue due to policy={cfg['policy']} row={point.index} "
                        f"gate_wait_after_soak_s={gate_elapsed_after_soak_s:.1f} "
                        f"total_wait_s={total_elapsed_s:.1f} reason={reason}"
                    )
                    return True
                self.log(
                    "CO2 route precondition failed: dewpoint gate timeout after fixed purge; "
                    f"row={point.index} gate_wait_after_soak_s={gate_elapsed_after_soak_s:.1f} "
                    f"total_wait_s={total_elapsed_s:.1f} reason={reason}"
                )
                return False

            now_ts = time.time()
            if (now_ts - last_log_ts) >= float(cfg["log_interval_s"]):
                last_log_ts = now_ts
                self.log(
                    "CO2 route precondition dewpoint gate waiting: "
                    f"row={point.index} dewpoint={snapshot.get('dewpoint_c')} "
                    f"time_to_gate={total_elapsed_s:.1f}s "
                    f"tail_span_60s={dewpoint_tail_span_60s} "
                    f"tail_slope_60s={dewpoint_tail_slope_60s} "
                    f"reason={gate_eval.get('gate_reason') or 'waiting'}"
                )
            time.sleep(float(cfg["poll_s"]))

    def _wait_h2o_route_dewpoint_gate_before_sampling(
        self,
        point: CalibrationPoint,
        *,
        log_context: str,
    ) -> bool:
        cfg = self._water_route_dewpoint_gate_cfg()
        if not bool(cfg.get("enabled")):
            return True

        gate_begin_ts = time.time()
        gate_rows: List[Dict[str, Any]] = []
        last_log_ts = 0.0
        consecutive_read_missing = 0
        max_transient_read_missing = 3
        self._append_pressure_trace_row(
            point=point,
            route="h2o",
            point_phase="h2o",
            trace_stage="h2o_precondition_dewpoint_gate_begin",
            pressure_target_hpa=point.target_pressure_hpa,
            refresh_pace_state=False,
            note=(
                f"window_s={float(cfg['window_s']):.3f} "
                f"max_gate_wait_s={float(cfg['max_total_wait_s']):.3f} "
                f"policy={cfg['policy']}"
            ),
        )
        while True:
            if self.stop_event.is_set():
                return False
            self._check_pause()
            try:
                snapshot = self._read_precondition_dewpoint_gate_snapshot()
            except Exception as exc:
                reason = str(exc) or "dewpoint_gate_read_failed"
                total_elapsed_s = max(0.0, time.time() - gate_begin_ts)
                if reason == "dewpoint_gate_read_missing":
                    consecutive_read_missing += 1
                    if consecutive_read_missing < max_transient_read_missing:
                        now_ts = time.time()
                        if (now_ts - last_log_ts) >= float(cfg["log_interval_s"]):
                            last_log_ts = now_ts
                            self.log(
                                "H2O route precondition dewpoint gate waiting: "
                                f"row={point.index} read missing; retry "
                                f"{consecutive_read_missing}/{max_transient_read_missing - 1} "
                                f"time_to_gate={total_elapsed_s:.1f}s"
                            )
                        time.sleep(float(cfg["poll_s"]))
                        continue
                self._set_point_runtime_fields(
                    point,
                    phase="h2o",
                    dewpoint_time_to_gate=round(total_elapsed_s, 3),
                    dewpoint_tail_span_60s=None,
                    dewpoint_tail_slope_60s=None,
                    dewpoint_rebound_detected=None,
                    flush_gate_status="fail",
                    flush_gate_reason=reason,
                )
                self._append_pressure_trace_row(
                    point=point,
                    route="h2o",
                    point_phase="h2o",
                    trace_stage="h2o_precondition_dewpoint_gate_end",
                    pressure_target_hpa=point.target_pressure_hpa,
                    refresh_pace_state=False,
                    dewpoint_time_to_gate=round(total_elapsed_s, 3),
                    flush_gate_status="fail",
                    flush_gate_reason=reason,
                    note="H2O route precondition dewpoint gate failed before sampling",
                )
                self.log(
                    "H2O route precondition dewpoint gate failed before sampling: "
                    f"row={point.index} reason={reason}"
                )
                return False

            consecutive_read_missing = 0
            gate_elapsed_s = max(0.0, time.time() - gate_begin_ts)
            gate_rows.append(
                self._build_h2o_route_dewpoint_gate_row(
                    total_elapsed_s=gate_elapsed_s,
                    snapshot=snapshot,
                )
            )
            gate_eval = evaluate_dewpoint_flush_gate(
                gate_rows,
                min_flush_s=0.0,
                gate_window_s=float(cfg["window_s"]),
                max_tail_span_c=float(cfg["tail_span_max_c"]),
                max_abs_tail_slope_c_per_s=float(cfg["tail_slope_abs_max_c_per_s"]),
                rebound_window_s=float(cfg["rebound_window_s"]),
                rebound_min_rise_c=float(cfg["rebound_min_rise_c"]),
                include_rebound_in_gate=True,
            )
            dewpoint_tail_span_60s = self._as_float(gate_eval.get("dewpoint_tail_span_60s"))
            dewpoint_tail_slope_60s = self._as_float(gate_eval.get("dewpoint_tail_slope_60s"))
            dewpoint_rebound_detected = bool(gate_eval.get("dewpoint_rebound_detected"))
            dewpoint_time_to_gate = self._as_float(gate_eval.get("dewpoint_time_to_gate"))
            if bool(gate_eval.get("gate_pass")):
                self._set_point_runtime_fields(
                    point,
                    phase="h2o",
                    dewpoint_time_to_gate=dewpoint_time_to_gate,
                    dewpoint_tail_span_60s=dewpoint_tail_span_60s,
                    dewpoint_tail_slope_60s=dewpoint_tail_slope_60s,
                    dewpoint_rebound_detected=dewpoint_rebound_detected,
                    flush_gate_status="pass",
                    flush_gate_reason="",
                )
                self._append_pressure_trace_row(
                    point=point,
                    route="h2o",
                    point_phase="h2o",
                    trace_stage="h2o_precondition_dewpoint_gate_end",
                    pressure_target_hpa=point.target_pressure_hpa,
                    refresh_pace_state=False,
                    dewpoint_c=snapshot.get("dewpoint_c"),
                    dew_temp_c=snapshot.get("temp_c"),
                    dew_rh_pct=snapshot.get("rh_pct"),
                    dewpoint_time_to_gate=dewpoint_time_to_gate,
                    dewpoint_tail_span_60s=dewpoint_tail_span_60s,
                    dewpoint_tail_slope_60s=dewpoint_tail_slope_60s,
                    dewpoint_rebound_detected=dewpoint_rebound_detected,
                    flush_gate_status="pass",
                    flush_gate_reason="",
                    note=f"context={log_context} result=pass",
                )
                self.log(
                    "H2O route dewpoint gate passed after open-route alignment: "
                    f"row={point.index} time_to_gate={dewpoint_time_to_gate} "
                    f"tail_span_60s={dewpoint_tail_span_60s} "
                    f"tail_slope_60s={dewpoint_tail_slope_60s} "
                    f"rebound={dewpoint_rebound_detected}"
                )
                return True

            if float(cfg["max_total_wait_s"]) > 0 and gate_elapsed_s >= float(cfg["max_total_wait_s"]):
                reason = str(gate_eval.get("gate_reason") or "dewpoint_gate_timeout")
                if "max_total_wait_exceeded" not in reason:
                    reason = f"{reason};max_total_wait_exceeded" if reason else "max_total_wait_exceeded"
                self._set_point_runtime_fields(
                    point,
                    phase="h2o",
                    dewpoint_time_to_gate=round(gate_elapsed_s, 3),
                    dewpoint_tail_span_60s=dewpoint_tail_span_60s,
                    dewpoint_tail_slope_60s=dewpoint_tail_slope_60s,
                    dewpoint_rebound_detected=dewpoint_rebound_detected,
                    flush_gate_status="timeout",
                    flush_gate_reason=reason,
                )
                self._append_pressure_trace_row(
                    point=point,
                    route="h2o",
                    point_phase="h2o",
                    trace_stage="h2o_precondition_dewpoint_gate_end",
                    pressure_target_hpa=point.target_pressure_hpa,
                    refresh_pace_state=False,
                    dewpoint_c=snapshot.get("dewpoint_c"),
                    dew_temp_c=snapshot.get("temp_c"),
                    dew_rh_pct=snapshot.get("rh_pct"),
                    dewpoint_time_to_gate=round(gate_elapsed_s, 3),
                    dewpoint_tail_span_60s=dewpoint_tail_span_60s,
                    dewpoint_tail_slope_60s=dewpoint_tail_slope_60s,
                    dewpoint_rebound_detected=dewpoint_rebound_detected,
                    flush_gate_status="timeout",
                    flush_gate_reason=reason,
                    note=f"context={log_context} result=timeout policy={cfg['policy']}",
                )
                if str(cfg["policy"]) in {"warn", "pass"}:
                    self.log(
                        "H2O route precondition dewpoint gate timed out after open-route alignment; "
                        f"continue due to policy={cfg['policy']} row={point.index} "
                        f"gate_wait_s={gate_elapsed_s:.1f} reason={reason}"
                    )
                    return True
                self.log(
                    "H2O route precondition failed: dewpoint gate timeout after open-route alignment; "
                    f"row={point.index} gate_wait_s={gate_elapsed_s:.1f} reason={reason}"
                )
                return False

            now_ts = time.time()
            if (now_ts - last_log_ts) >= float(cfg["log_interval_s"]):
                last_log_ts = now_ts
                self.log(
                    "H2O route precondition dewpoint gate waiting: "
                    f"row={point.index} dewpoint={snapshot.get('dewpoint_c')} "
                    f"time_to_gate={gate_elapsed_s:.1f}s "
                    f"tail_span_60s={dewpoint_tail_span_60s} "
                    f"tail_slope_60s={dewpoint_tail_slope_60s} "
                    f"reason={gate_eval.get('gate_reason') or 'waiting'}"
                )
            time.sleep(float(cfg["poll_s"]))

    def _wait_co2_route_soak_before_seal(self, point: CalibrationPoint) -> bool:
        special_flush = self._has_special_co2_zero_flush_pending() and self._is_zero_co2_point(point)
        cold_group_flush = False
        soak_key = "workflow.stability.co2_route.preseal_soak_s"
        soak_default = 180.0
        wait_reason = "开路预通气"
        log_context = "CO2 route opened"
        if special_flush:
            soak_key = "workflow.stability.co2_route.post_h2o_zero_ppm_soak_s"
            soak_default = float(self._wf("workflow.stability.co2_route.preseal_soak_s", 180.0))
            if self._post_h2o_co2_zero_flush_pending:
                wait_reason = "水转气后首个0气清洗"
                log_context = "CO2 route opened after H2O; zero-gas flush"
            else:
                wait_reason = "纯气路首个0气清洗"
                log_context = "CO2 route opened for first zero-gas flush"
            self._active_post_h2o_co2_zero_flush = True
        elif self._first_co2_route_soak_pending:
            soak_key = "workflow.stability.co2_route.first_point_preseal_soak_s"
            soak_default = 300.0
            wait_reason = "首个气点开路预通气"
            log_context = "CO2 route opened for first gas-point flush"
        elif self._should_apply_cold_co2_zero_flush(point):
            soak_key = "workflow.stability.co2_route.cold_group_zero_ppm_soak_s"
            soak_default = float(self._wf("workflow.stability.co2_route.preseal_soak_s", 180.0))
            wait_reason = "冷组0气吹干"
            log_context = "CO2 cold-group dry flush"
            cold_group_flush = True
        else:
            self._active_post_h2o_co2_zero_flush = False

        soak_s = float(self._wf(soak_key, soak_default))
        if soak_s <= 0:
            self._first_co2_route_soak_pending = False
            return True

        self._set_point_runtime_fields(
            point,
            phase="co2",
            configured_route_soak_s=soak_s,
        )
        self._append_pressure_trace_row(
            point=point,
            route="co2",
            point_phase="co2",
            trace_stage="soak_begin",
            pressure_target_hpa=point.target_pressure_hpa,
            refresh_pace_state=False,
            note=f"configured_route_soak_s={soak_s:.3f} context={log_context}",
        )
        self._emit_stage_event(
            current=f"{self._stage_label_for_point(point, phase='co2', include_pressure=False)} 通气等待",
            point=point,
            phase="co2",
            wait_reason=wait_reason,
            countdown_s=soak_s,
        )
        self.log(
            f"{log_context}; wait {int(soak_s)}s before pressure sealing "
            f"(row {point.index})"
        )
        start = time.time()
        while time.time() - start < soak_s:
            if self.stop_event.is_set():
                return False
            self._check_pause()
            remain = soak_s - (time.time() - start)
            if int(remain) in {0, int(soak_s), 30, 60, 90, 120}:
                self._emit_stage_event(
                    current=f"{self._stage_label_for_point(point, phase='co2', include_pressure=False)} 通气等待",
                    point=point,
                    phase="co2",
                    wait_reason=wait_reason,
                    countdown_s=remain,
                )
            time.sleep(min(1.0, max(0.05, remain)))
        self._append_pressure_trace_row(
            point=point,
            route="co2",
            point_phase="co2",
            trace_stage="soak_end",
            pressure_target_hpa=point.target_pressure_hpa,
            refresh_pace_state=False,
            note=f"configured_route_soak_s={soak_s:.3f} context={log_context}",
        )
        if special_flush:
            self._post_h2o_co2_zero_flush_pending = False
            self._initial_co2_zero_flush_pending = False
        self._first_co2_route_soak_pending = False
        cold_temp_key = self._cold_co2_zero_flush_temp_key(point)
        if cold_temp_key is not None and self._is_zero_co2_point(point):
            self._last_cold_co2_zero_flush_temp_c = cold_temp_key
        return self._wait_co2_route_dewpoint_gate_before_seal(
            point,
            base_soak_s=soak_s,
            log_context=log_context,
        )

    def _wait_co2_preseal_primary_sensor_gate(self, point: CalibrationPoint) -> bool:
        sensor_cfg = self.cfg.get("workflow", {}).get("stability", {}).get("sensor", {})
        if sensor_cfg and not sensor_cfg.get("enabled", True):
            self.log("CO2 preseal analyzer stability gate skipped: sensor stability disabled by configuration")
            return True

        tol = float(sensor_cfg.get("co2_ratio_f_preseal_tol", sensor_cfg.get("co2_ratio_f_tol", 0.001)))
        window_s = float(sensor_cfg.get("co2_ratio_f_preseal_window_s", sensor_cfg.get("window_s", 30)))
        timeout_s = float(sensor_cfg.get("co2_ratio_f_preseal_timeout_s", sensor_cfg.get("timeout_s", 300)))
        min_samples = max(2, int(sensor_cfg.get("co2_ratio_f_preseal_min_samples", 10)))
        read_interval_s = float(
            sensor_cfg.get("co2_ratio_f_preseal_read_interval_s", sensor_cfg.get("read_interval_s", 1.0))
        )
        self._append_pressure_trace_row(
            point=point,
            route="co2",
            point_phase="co2",
            trace_stage="co2_precondition_analyzer_gate_begin",
            pressure_target_hpa=point.target_pressure_hpa,
            refresh_pace_state=False,
            note=(
                f"key=co2_ratio_f tol={tol:.6f} window_s={window_s:.3f} "
                f"timeout_s={timeout_s:.3f} min_samples={min_samples} "
                f"read_interval_s={read_interval_s:.3f}"
            ),
        )
        stable = self._wait_primary_sensor_stable(
            point,
            value_key="co2_ratio_f",
            require_pressure_in_limits=False,
            tol_override=tol,
            window_override=window_s,
            timeout_override=timeout_s,
            min_samples_override=min_samples,
            read_interval_override=read_interval_s,
        )
        self._append_pressure_trace_row(
            point=point,
            route="co2",
            point_phase="co2",
            trace_stage="co2_precondition_analyzer_gate_end",
            pressure_target_hpa=point.target_pressure_hpa,
            refresh_pace_state=False,
            note=(
                f"result={'pass' if stable else 'fail'} key=co2_ratio_f tol={tol:.6f} "
                f"window_s={window_s:.3f} timeout_s={timeout_s:.3f} min_samples={min_samples}"
            ),
        )
        if stable:
            self.log(
                "CO2 preseal analyzer stability gate passed: "
                f"tol={tol:g} window_s={window_s:g} min_samples={min_samples}"
            )
            return True
        self.log(
            "CO2 preseal analyzer stability gate failed before downstream sampling/seal: "
            f"tol={tol:g} window_s={window_s:g} min_samples={min_samples}"
        )
        return False

    def _wait_h2o_precondition_primary_sensor_gate(self, point: CalibrationPoint) -> bool:
        sensor_cfg = self.cfg.get("workflow", {}).get("stability", {}).get("sensor", {})
        if sensor_cfg and not sensor_cfg.get("enabled", True):
            self.log("H2O precondition analyzer stability gate skipped: sensor stability disabled by configuration")
            return True

        tol = float(sensor_cfg.get("h2o_ratio_f_preseal_tol", sensor_cfg.get("h2o_ratio_f_tol", 0.001)))
        window_s = float(sensor_cfg.get("h2o_ratio_f_preseal_window_s", sensor_cfg.get("window_s", 30)))
        timeout_s = float(sensor_cfg.get("h2o_ratio_f_preseal_timeout_s", sensor_cfg.get("timeout_s", 300)))
        min_samples = max(2, int(sensor_cfg.get("h2o_ratio_f_preseal_min_samples", 10)))
        read_interval_s = float(
            sensor_cfg.get("h2o_ratio_f_preseal_read_interval_s", sensor_cfg.get("read_interval_s", 1.0))
        )
        policy_raw = sensor_cfg.get("h2o_ratio_f_preseal_policy", "warn")
        policy = str(policy_raw or "warn").strip().lower()
        if policy not in {"reject", "warn", "pass"}:
            policy = "warn"
        self._append_pressure_trace_row(
            point=point,
            route="h2o",
            point_phase="h2o",
            trace_stage="h2o_precondition_analyzer_gate_begin",
            pressure_target_hpa=point.target_pressure_hpa,
            refresh_pace_state=False,
            note=(
                f"key=h2o_ratio_f tol={tol:.6f} window_s={window_s:.3f} "
                f"timeout_s={timeout_s:.3f} min_samples={min_samples} "
                f"read_interval_s={read_interval_s:.3f} policy={policy}"
            ),
        )
        stable = self._wait_primary_sensor_stable(
            point,
            value_key="h2o_ratio_f",
            require_pressure_in_limits=False,
            tol_override=tol,
            window_override=window_s,
            timeout_override=timeout_s,
            min_samples_override=min_samples,
            read_interval_override=read_interval_s,
        )
        self._append_pressure_trace_row(
            point=point,
            route="h2o",
            point_phase="h2o",
            trace_stage="h2o_precondition_analyzer_gate_end",
            pressure_target_hpa=point.target_pressure_hpa,
            refresh_pace_state=False,
            note=(
                f"result={'pass' if stable else 'fail'} key=h2o_ratio_f tol={tol:.6f} "
                f"window_s={window_s:.3f} timeout_s={timeout_s:.3f} min_samples={min_samples} "
                f"policy={policy}"
            ),
        )
        if stable:
            self.log(
                "H2O precondition analyzer stability gate passed: "
                f"row={point.index} tol={tol:g} window_s={window_s:g} min_samples={min_samples}"
            )
            return True
        if policy in {"warn", "pass"}:
            self.log(
                "H2O precondition analyzer stability gate timed out before open-route sampling/seal; "
                f"continue due to policy={policy} row={point.index} "
                f"tol={tol:g} window_s={window_s:g} min_samples={min_samples}"
            )
            return True
        self.log(
            "H2O precondition analyzer stability gate failed before open-route sampling/seal: "
            f"row={point.index} tol={tol:g} window_s={window_s:g} min_samples={min_samples}"
        )
        return False

    def _co2_pressure_timeout_reseal_retries(self) -> int:
        return max(0, int(self._wf("workflow.pressure.co2_reseal_retry_count", 1)))

    def _retry_co2_pressure_point_after_timeout(
        self,
        source_point: CalibrationPoint,
        sample_point: CalibrationPoint,
        *,
        attempt: int,
        total: int,
    ) -> bool:
        self.log(
            f"CO2 {sample_point.co2_ppm} ppm @ {sample_point.target_pressure_hpa} hPa timeout; "
            f"retry within sealed route {attempt}/{total}"
        )
        return self._set_pressure_to_target(sample_point)

    def _run_h2o_group(
        self,
        points: List[CalibrationPoint],
        pressure_points: Optional[List[CalibrationPoint]] = None,
        next_route_context: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not points:
            return

        self._preseal_dewpoint_snapshot = None
        lead = points[0]
        row_text = ",".join(str(point.index) for point in points)
        effective_pressure_points = pressure_points or points
        pressure_text = ", ".join(self._pressure_target_label(p) or "--" for p in effective_pressure_points)
        self.log(
            f"H2O group start: rows={row_text} chamber={lead.temp_chamber_c}C "
            f"hgen={lead.hgen_temp_c}C/{lead.hgen_rh_pct}% "
            f"pressures=[{pressure_text}]"
        )

        self._apply_idle_route_isolation(reason="before H2O group conditioning")
        self._prepare_pressure_for_h2o(lead)
        self._prepare_humidity_generator(lead)
        self._emit_stage_event(
            current=f"{self._stage_label_for_point(lead, phase='h2o', include_pressure=False)} 温箱等待 {float(lead.temp_chamber_c):g}°C",
            point=lead,
            phase="h2o",
            wait_reason="等待温度箱到位",
        )
        route_context = self._route_entry_context_for_h2o_group(points, pressure_points=effective_pressure_points) or {}
        route_point_tag = str(route_context.get("point_tag") or self._h2o_point_tag(lead))
        if not self._set_temperature_for_point(lead, phase="h2o"):
            self.log(f"H2O group rows={row_text} skipped: chamber did not stabilize")
            self._discard_pending_route_handoff(
                point=lead,
                phase="h2o",
                point_tag=route_point_tag,
                reason="H2O chamber did not stabilize before route open",
            )
            self._cleanup_h2o_route(lead, reason="after H2O chamber timeout")
            return
        if not self._wait_humidity_generator_stable(lead):
            self.log(f"H2O group rows={row_text} skipped: humidity generator did not reach setpoint")
            self._discard_pending_route_handoff(
                point=lead,
                phase="h2o",
                point_tag=route_point_tag,
                reason="H2O humidity generator did not reach setpoint before route open",
            )
            self._cleanup_h2o_route(lead, reason="after H2O humidity timeout")
            return
        self._capture_temperature_calibration_snapshot(lead, route_type="h2o")
        if not self._open_h2o_route_and_wait_ready(lead, point_tag=route_point_tag):
            self.log(f"H2O group rows={row_text} skipped: open-route stabilization failed")
            self._cleanup_h2o_route(lead, reason="after H2O route timeout")
            return
        self._mark_post_h2o_co2_zero_flush_pending()
        ambient_open_refs, sealed_control_refs = self._split_pressure_execution_points(effective_pressure_points)

        for ambient_ref in ambient_open_refs:
            if self.stop_event.is_set():
                self._cleanup_h2o_route(lead, reason="after H2O ambient open-route interrupted")
                return
            self._check_pause()
            sample_point = self._build_h2o_pressure_point(lead, ambient_ref)
            point_tag = self._h2o_point_tag(sample_point)
            pressure_label = self._pressure_target_label(sample_point) or self._AMBIENT_PRESSURE_LABEL
            self.set_status(f"H2O row {sample_point.index} {pressure_label}")
            self._sample_open_route_point(sample_point, phase="h2o", point_tag=point_tag)

        if not sealed_control_refs:
            self._cleanup_h2o_route(lead, reason="after H2O group complete")
            return

        if not self._wait_h2o_route_soak_before_seal(lead):
            self.log(f"H2O group rows={row_text} skipped: route preseal soak interrupted")
            self._cleanup_h2o_route(lead, reason="after H2O preseal soak interrupted")
            return

        if not self._pressurize_route_for_sealed_points(
            lead,
            route="h2o",
            sealed_control_refs=sealed_control_refs,
        ):
            self.log(f"H2O group rows={row_text} skipped: route sealing failed")
            self._cleanup_h2o_route(lead, reason="after H2O pressure-seal failure")
            return

        last_sample_point: Optional[CalibrationPoint] = None
        last_point_tag = ""
        handoff_armed = False
        for pressure_idx, pressure_point in enumerate(sealed_control_refs):
            if self.stop_event.is_set():
                break
            self._check_pause()
            sample_point = self._build_h2o_pressure_point(lead, pressure_point)
            point_tag = self._h2o_point_tag(sample_point)
            pressure_label = self._pressure_target_label(sample_point) or "--"
            self.set_status(f"H2O row {sample_point.index} {pressure_label}")
            self._emit_stage_event(
                current=self._stage_label_for_point(sample_point, phase="h2o"),
                point=sample_point,
                phase="h2o",
                point_tag=point_tag,
                wait_reason="准备控压",
            )
            if not self._set_pressure_to_target(sample_point):
                self.log(f"H2O row {sample_point.index} skipped: pressure did not stabilize")
                continue
            if not self._wait_after_pressure_stable_before_sampling(sample_point):
                self.log(f"H2O row {sample_point.index} skipped: post-pressure hold before sampling interrupted")
                continue
            is_last_pressure_point = pressure_idx + 1 == len(sealed_control_refs)
            if next_route_context and is_last_pressure_point:
                self._sample_handoff_request = {
                    "current_phase": "h2o",
                    "current_point_tag": point_tag,
                    "next_point": next_route_context["point"],
                    "next_phase": next_route_context["phase"],
                    "next_point_tag": next_route_context["point_tag"],
                    "next_open_valves": list(next_route_context["open_valves"]),
                    "armed": False,
                }
            self._sample_and_log(sample_point, phase="h2o", point_tag=point_tag)
            request = dict(self._sample_handoff_request or {})
            handoff_armed = handoff_armed or bool(request.get("armed"))
            self._sample_handoff_request = None
            last_sample_point = sample_point
            last_point_tag = point_tag

        if self.stop_event.is_set():
            self._cleanup_h2o_route(lead, reason="after H2O group interrupted")
            return

        if handoff_armed:
            return

        if (
            next_route_context
            and last_sample_point is not None
            and self._begin_pending_route_handoff(
                current_point=last_sample_point,
                current_phase="h2o",
                current_point_tag=last_point_tag,
                next_point=next_route_context["point"],
                next_phase=next_route_context["phase"],
                next_point_tag=next_route_context["point_tag"],
                next_open_valves=next_route_context["open_valves"],
            )
        ):
            return

        self._cleanup_h2o_route(lead, reason="after H2O group complete")

    def _run_h2o_point(self, point: CalibrationPoint, prepared: bool = False) -> None:
        self._preseal_dewpoint_snapshot = None
        self._apply_idle_route_isolation(reason="before H2O point conditioning")
        if not prepared:
            self._prepare_pressure_for_h2o(point)
            self._prepare_humidity_generator(point)
        self._emit_stage_event(
            current=f"{self._stage_label_for_point(point, phase='h2o', include_pressure=False)} 温箱等待 {float(point.temp_chamber_c):g}°C",
            point=point,
            phase="h2o",
            wait_reason="等待温度箱到位",
        )
        if not self._set_temperature_for_point(point, phase="h2o"):
            self.log(f"H2O row {point.index} skipped: chamber did not stabilize")
            self._cleanup_h2o_route(point, reason="after H2O chamber timeout")
            return
        if not self._wait_humidity_generator_stable(point):
            self.log(f"H2O row {point.index} skipped: humidity generator did not reach setpoint")
            self._cleanup_h2o_route(point, reason="after H2O humidity timeout")
            return
        self._capture_temperature_calibration_snapshot(point, route_type="h2o")
        if not self._open_h2o_route_and_wait_ready(point):
            self.log(f"H2O row {point.index} skipped: open-route stabilization failed")
            self._cleanup_h2o_route(point, reason="after H2O route timeout")
            return
        self._mark_post_h2o_co2_zero_flush_pending()
        if self._is_ambient_pressure_point(point):
            self._sample_open_route_point(point, phase="h2o", point_tag=self._h2o_point_tag(point))
            self._cleanup_h2o_route(point, reason="after H2O point complete")
            return
        if not self._wait_h2o_route_soak_before_seal(point):
            self.log(f"H2O row {point.index} skipped: route preseal soak interrupted")
            self._cleanup_h2o_route(point, reason="after H2O preseal soak interrupted")
            return
        if not self._pressurize_route_for_sealed_points(point, route="h2o", sealed_control_refs=[point]):
            self.log(f"H2O row {point.index} skipped: route sealing failed")
            self._cleanup_h2o_route(point, reason="after H2O pressure-seal failure")
            return
        if not self._set_pressure_to_target(point):
            self.log(f"H2O row {point.index} skipped: pressure did not stabilize")
            self._cleanup_h2o_route(point, reason="after H2O pressure timeout")
            return
        if not self._wait_after_pressure_stable_before_sampling(point):
            self.log(f"H2O row {point.index} skipped: post-pressure hold before sampling interrupted")
            self._cleanup_h2o_route(point, reason="after H2O analyzer timeout")
            return
        self._sample_and_log(point, phase="h2o")
        self._cleanup_h2o_route(point, reason="after H2O point complete")

    def _recent_fast_signal_numeric_observation(
        self,
        key: str,
        value_key: str,
        *,
        context: Optional[Dict[str, Any]] = None,
        window_s: float = 3.0,
        min_recv_mono_s: Optional[float] = None,
    ) -> Dict[str, Any]:
        active_context = context if isinstance(context, dict) else self._pressure_transition_fast_signal_context_active()
        if not isinstance(active_context, dict):
            return {}
        cutoff_mono_s = time.monotonic() - max(0.1, float(window_s))
        if min_recv_mono_s is not None:
            cutoff_mono_s = max(cutoff_mono_s, float(min_recv_mono_s))
        samples: List[tuple[float, float]] = []
        for frame in self._sampling_window_fast_signal_frames(active_context, key):
            if not isinstance(frame, dict):
                continue
            recv_mono_s = self._as_float(frame.get("recv_mono_s"))
            if recv_mono_s is None or recv_mono_s < cutoff_mono_s:
                continue
            values = frame.get("values", {})
            if not isinstance(values, dict):
                continue
            value = self._as_float(values.get(value_key))
            if value is None:
                continue
            samples.append((recv_mono_s, value))
        metrics = self._numeric_series_metrics(samples)
        if not metrics:
            return {}
        metrics["window_s"] = float(window_s)
        return metrics

    @staticmethod
    def _numeric_series_metrics(samples: List[Tuple[float, float]]) -> Dict[str, Any]:
        if len(samples) < 2:
            return {}
        ordered = sorted(
            (
                (float(ts), float(value))
                for ts, value in samples
                if ts is not None and value is not None
            ),
            key=lambda item: item[0],
        )
        if len(ordered) < 2:
            return {}
        values_only = [value for _ts, value in ordered]
        duration_s = max(0.0, ordered[-1][0] - ordered[0][0])
        slope_per_s = 0.0 if duration_s <= 0 else (values_only[-1] - values_only[0]) / duration_s
        return {
            "count": len(ordered),
            "duration_s": duration_s,
            "span": max(values_only) - min(values_only),
            "slope_per_s": slope_per_s,
            "first_value": values_only[0],
            "last_value": values_only[-1],
            "min_value": min(values_only),
            "max_value": max(values_only),
        }

    def _cached_ready_check_trace_values(
        self,
        *,
        context: Optional[Dict[str, Any]] = None,
        point: Optional[CalibrationPoint] = None,
    ) -> Dict[str, Any]:
        active_context = context if isinstance(context, dict) else self._pressure_transition_fast_signal_context_active()
        values: Dict[str, Any] = {}
        pace_frame = self._latest_fast_signal_frame("pace", context=active_context)
        if isinstance(pace_frame, dict):
            pace_values = pace_frame.get("values", {})
            if isinstance(pace_values, dict):
                values["pace_pressure_hpa"] = self._as_float(pace_values.get("pressure_hpa"))
        gauge_frame = self._latest_fast_signal_frame("pressure_gauge", context=active_context)
        if isinstance(gauge_frame, dict):
            gauge_values = gauge_frame.get("values", {})
            if isinstance(gauge_values, dict):
                values["pressure_gauge_hpa"] = self._as_float(
                    gauge_values.get("pressure_gauge_hpa", gauge_values.get("pressure_gauge_raw"))
                )
        dew_frame = self._latest_fast_signal_frame("dewpoint", context=active_context)
        if isinstance(dew_frame, dict):
            dew_values = dew_frame.get("values", {})
            if isinstance(dew_values, dict):
                dewpoint_live_c = self._as_float(dew_values.get("dewpoint_live_c"))
                dew_temp_live_c = self._as_float(dew_values.get("dew_temp_live_c"))
                dew_rh_live_pct = self._as_float(dew_values.get("dew_rh_live_pct"))
                values["dewpoint_live_c"] = dewpoint_live_c
                values["dew_temp_live_c"] = dew_temp_live_c
                values["dew_rh_live_pct"] = dew_rh_live_pct
        snapshot = self._preseal_dewpoint_snapshot if point is not None and point.is_h2o_point else None
        if isinstance(snapshot, dict):
            values["dewpoint_c"] = self._as_float(snapshot.get("dewpoint_c"))
            values["dew_temp_c"] = self._as_float(snapshot.get("temp_c"))
            values["dew_rh_pct"] = self._as_float(snapshot.get("rh_pct"))
        else:
            values["dewpoint_c"] = values.get("dewpoint_live_c")
            values["dew_temp_c"] = values.get("dew_temp_live_c")
            values["dew_rh_pct"] = values.get("dew_rh_live_pct")
        return values

    def _fast_signal_frame_has_required_value(
        self,
        signal_key: str,
        frame: Optional[Dict[str, Any]],
    ) -> bool:
        if not isinstance(frame, dict):
            return False
        values = frame.get("values", {})
        if not isinstance(values, dict):
            return False
        key = str(signal_key or "").strip().lower()
        if key == "pace":
            return self._as_float(values.get("pressure_hpa")) is not None
        if key == "pressure_gauge":
            return self._as_float(values.get("pressure_gauge_hpa", values.get("pressure_gauge_raw"))) is not None
        if key == "dewpoint":
            return self._as_float(values.get("dewpoint_live_c")) is not None
        return False

    def _sampling_context_missing_fresh_fast_signals(self, context: Dict[str, Any]) -> List[str]:
        missing: List[str] = []
        max_age_s = self._sampling_pre_sample_signal_max_age_s()
        for signal_key, device_key in (
            ("pace", "pace"),
            ("pressure_gauge", "pressure_gauge"),
            ("dewpoint", "dewpoint"),
        ):
            if self.devices.get(device_key) is None:
                continue
            frame = self._latest_fast_signal_frame(signal_key, context=context, max_age_s=max_age_s)
            if not self._fast_signal_frame_has_required_value(signal_key, frame):
                missing.append(signal_key)
        return missing

    def _sampling_context_missing_fresh_analyzers(self, context: Dict[str, Any]) -> List[str]:
        missing: List[str] = []
        now_s = time.time()
        now_mono_s = time.monotonic()
        max_age_s = self._sampling_pre_sample_analyzer_max_age_s()
        for label, ga, analyzer_cfg in self._all_gas_analyzers():
            if label in self._disabled_analyzers:
                continue
            settings = self._gas_analyzer_runtime_settings(analyzer_cfg)
            active_send = bool(settings["active_send"])
            if active_send and self._sampling_active_anchor_match_enabled():
                frames = self._sampling_window_active_analyzer_frames(context, ga, label=label)
                latest: Optional[Dict[str, Any]] = None
                latest_mono: Optional[float] = None
                for frame in frames:
                    recv_mono_s = self._as_float(frame.get("recv_mono_s"))
                    if recv_mono_s is None:
                        continue
                    if latest is None or latest_mono is None or recv_mono_s > latest_mono:
                        latest = dict(frame)
                        latest_mono = recv_mono_s
                if latest is None or latest_mono is None or (now_mono_s - latest_mono) > max_age_s:
                    missing.append(label)
                    continue
                parsed = latest.get("parsed")
                usable = bool(isinstance(parsed, dict) and self._assess_analyzer_frame(parsed)[0])
                if not usable:
                    missing.append(label)
                continue

            entry, fresh, _age_ms = self._analyzer_cache_snapshot(ga, label=label, now_s=now_s)
            parsed = entry.get("parsed") if isinstance(entry, dict) else None
            usable = bool(isinstance(parsed, dict) and self._assess_analyzer_frame(parsed)[0])
            if not fresh or not usable:
                missing.append(label)
        return missing

    def _wait_for_sampling_freshness_gate(
        self,
        *,
        point: CalibrationPoint,
        phase: str,
        point_tag: str,
        context: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        metrics: Dict[str, Any] = {
            "status": "skipped",
            "elapsed_s": 0.0,
            "missing": [],
            "ready_values": {},
        }
        if not isinstance(context, dict):
            return metrics
        timeout_s = self._sampling_pre_sample_freshness_timeout_s()
        if timeout_s <= 0:
            return metrics

        plan = dict(context.get("worker_plan") or {})
        analyzers = list(plan.get("active_entries") or []) + list(plan.get("passive_entries") or [])
        fast_signal_async = bool(plan.get("fast_signal_enabled", False))
        analyzer_async = bool(plan.get("analyzer_worker_enabled", False))
        if not fast_signal_async:
            self._refresh_fast_signal_cache_once(context, reason="pre-sample freshness gate")
        if analyzers and not analyzer_async:
            self._prime_sampling_analyzer_cache_once(
                analyzers,
                include_passive=True,
                reason="pre-sample freshness gate",
                context=context,
            )

        start_mono = time.monotonic()
        deadline = start_mono + timeout_s
        poll_s = self._sampling_pre_sample_freshness_poll_s()
        while True:
            missing_signals = self._sampling_context_missing_fresh_fast_signals(context)
            missing_analyzers = self._sampling_context_missing_fresh_analyzers(context)
            missing = list(missing_signals) + [f"analyzer:{label}" for label in missing_analyzers]
            elapsed_s = max(0.0, time.monotonic() - start_mono)
            ready_values = self._cached_ready_check_trace_values(context=context, point=point)
            metrics = {
                "status": "waiting",
                "elapsed_s": elapsed_s,
                "missing": list(missing),
                "ready_values": dict(ready_values),
            }
            if not missing:
                self._append_pressure_trace_row(
                    point=point,
                    route=phase,
                    point_phase=phase,
                    point_tag=point_tag,
                    trace_stage="sampling_collection_ready",
                    pressure_target_hpa=point.target_pressure_hpa,
                    pace_pressure_hpa=ready_values.get("pace_pressure_hpa"),
                    pressure_gauge_hpa=ready_values.get("pressure_gauge_hpa"),
                    dewpoint_c=ready_values.get("dewpoint_c"),
                    dew_temp_c=ready_values.get("dew_temp_c"),
                    dew_rh_pct=ready_values.get("dew_rh_pct"),
                    dewpoint_live_c=ready_values.get("dewpoint_live_c"),
                    dew_temp_live_c=ready_values.get("dew_temp_live_c"),
                    dew_rh_live_pct=ready_values.get("dew_rh_live_pct"),
                    refresh_pace_state=False,
                    note=f"elapsed_s={elapsed_s:.3f} readiness=ok",
                )
                metrics["status"] = "ready"
                self.log(
                    "Sampling freshness gate passed: "
                    f"phase={phase} point={point.index} elapsed_s={elapsed_s:.3f}"
                )
                return metrics
            if time.monotonic() >= deadline:
                self._append_pressure_trace_row(
                    point=point,
                    route=phase,
                    point_phase=phase,
                    point_tag=point_tag,
                    trace_stage="sampling_collection_timeout",
                    pressure_target_hpa=point.target_pressure_hpa,
                    pace_pressure_hpa=ready_values.get("pace_pressure_hpa"),
                    pressure_gauge_hpa=ready_values.get("pressure_gauge_hpa"),
                    dewpoint_c=ready_values.get("dewpoint_c"),
                    dew_temp_c=ready_values.get("dew_temp_c"),
                    dew_rh_pct=ready_values.get("dew_rh_pct"),
                    dewpoint_live_c=ready_values.get("dewpoint_live_c"),
                    dew_temp_live_c=ready_values.get("dew_temp_live_c"),
                    dew_rh_live_pct=ready_values.get("dew_rh_live_pct"),
                    refresh_pace_state=False,
                    note=f"elapsed_s={elapsed_s:.3f} missing={','.join(missing)}",
                )
                metrics["status"] = "timeout"
                self.log(
                    "Sampling freshness gate timeout: "
                    f"phase={phase} point={point.index} elapsed_s={elapsed_s:.3f} missing={','.join(missing)}"
                )
                return metrics
            if not fast_signal_async:
                for signal_key in missing_signals:
                    try:
                        self._refresh_fast_signal_entry(context, signal_key, reason="pre-sample freshness gate")
                    except Exception:
                        pass
            if missing_analyzers and not analyzer_async:
                missing_entries = [entry for entry in analyzers if entry[0] in set(missing_analyzers)]
                if missing_entries:
                    try:
                        self._prime_sampling_analyzer_cache_once(
                            missing_entries,
                            include_passive=True,
                            reason="pre-sample freshness gate refresh",
                            context=context,
                        )
                    except Exception:
                        pass
            if not self._sampling_window_wait(poll_s, stop_event=context.get("stop_event")):
                metrics["status"] = "interrupted"
                return metrics

    def _wait_postseal_dewpoint_gate(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        if not point.is_h2o_point:
            self._record_co2_preseal_snapshot_runtime_fields(point, phase=phase)
        dew = self.devices.get("dewpoint")
        if dew is None:
            self._set_point_runtime_fields(
                point,
                phase=phase,
                dewpoint_gate_result="skipped",
                dewpoint_gate_elapsed_s=0.0,
                dewpoint_gate_count=0,
                dewpoint_gate_span_c=None,
                dewpoint_gate_slope_c_per_s=None,
            )
            return True
        gate_cfg = self._postseal_dewpoint_gate_cfg(point)
        self._set_postseal_timeout_runtime_fields(
            point,
            phase=phase,
            gate_cfg=gate_cfg,
            timed_out=False,
            blocked=False,
        )
        active_context = context if isinstance(context, dict) else self._pressure_transition_fast_signal_context_active()
        if active_context is None:
            active_context = self._start_pressure_transition_fast_signal_context(
                point=point,
                phase=phase,
                reason="post-seal dewpoint gate",
                prime_immediately=True,
            )
        if not isinstance(active_context, dict):
            self.log(f"{phase.upper()} sealed dewpoint gate skipped: no fast-signal context")
            self._set_point_runtime_fields(
                point,
                phase=phase,
                dewpoint_gate_result="skipped",
                dewpoint_gate_elapsed_s=0.0,
                dewpoint_gate_count=0,
                dewpoint_gate_span_c=None,
                dewpoint_gate_slope_c_per_s=None,
            )
            return True

        self._ensure_pressure_transition_fast_signal_cache(
            active_context,
            reason="post-seal dewpoint gate begin",
        )
        ready_values = self._cached_ready_check_trace_values(context=active_context, point=point)
        self._append_pressure_trace_row(
            point=point,
            route=phase,
            point_phase=phase,
            trace_stage="dewpoint_gate_begin",
            pressure_target_hpa=point.target_pressure_hpa,
            pace_pressure_hpa=ready_values.get("pace_pressure_hpa"),
            pressure_gauge_hpa=ready_values.get("pressure_gauge_hpa"),
            dewpoint_c=ready_values.get("dewpoint_c"),
            dew_temp_c=ready_values.get("dew_temp_c"),
            dew_rh_pct=ready_values.get("dew_rh_pct"),
            dewpoint_live_c=ready_values.get("dewpoint_live_c"),
            dew_temp_live_c=ready_values.get("dew_temp_live_c"),
            dew_rh_live_pct=ready_values.get("dew_rh_live_pct"),
            dewpoint_gate_window_s=gate_cfg["window_s"],
            dewpoint_gate_elapsed_s=0.0,
            dewpoint_gate_span_c="",
            dewpoint_gate_slope_c_per_s="",
            dewpoint_gate_count=0,
            refresh_pace_state=False,
            note=(
                f"timeout_s={gate_cfg['timeout_s']:.3f} span_limit_c={gate_cfg['span_c']:.3f} "
                f"slope_limit_c_per_s={gate_cfg['slope_c_per_s']:.4f} min_samples={gate_cfg['min_samples']}"
            ),
        )

        start_mono = time.monotonic()
        deadline = start_mono + float(gate_cfg["timeout_s"])
        immediate_timeout = float(gate_cfg["timeout_s"]) <= 0.0
        poll_s = min(0.1, self._pressure_transition_monitor_wait_s(point))
        gate_rows: List[Dict[str, Any]] = []
        while True:
            obs = self._recent_fast_signal_numeric_observation(
                "dewpoint",
                "dewpoint_live_c",
                context=active_context,
                window_s=float(gate_cfg["window_s"]),
            )
            ready_values = self._cached_ready_check_trace_values(context=active_context, point=point)
            count = int(obs.get("count") or 0)
            span_c = self._as_float(obs.get("span"))
            slope_c_per_s = self._as_float(obs.get("slope_per_s"))
            elapsed_s = 0.0 if immediate_timeout else max(0.0, time.monotonic() - start_mono)
            live_dewpoint_c = self._as_float(ready_values.get("dewpoint_live_c"))
            if live_dewpoint_c is not None:
                gate_rows.append(
                    {
                        "timestamp": datetime.now().isoformat(timespec="milliseconds"),
                        "dewpoint_c": live_dewpoint_c,
                        "gauge_pressure_hpa": self._as_float(ready_values.get("pressure_gauge_hpa")),
                        "dewpoint_temp_c": self._as_float(
                            ready_values.get("dew_temp_live_c")
                            if ready_values.get("dew_temp_live_c") not in (None, "")
                            else ready_values.get("dew_temp_c")
                        ),
                        "dewpoint_rh_percent": self._as_float(
                            ready_values.get("dew_rh_live_pct")
                            if ready_values.get("dew_rh_live_pct") not in (None, "")
                            else ready_values.get("dew_rh_pct")
                        ),
                    }
                )
            passed = bool(
                live_dewpoint_c is not None
                and count >= int(gate_cfg["min_samples"])
                and span_c is not None
                and slope_c_per_s is not None
                and span_c <= float(gate_cfg["span_c"])
                and abs(slope_c_per_s) <= float(gate_cfg["slope_c_per_s"])
            )
            note = (
                f"elapsed_s={elapsed_s:.3f} window_s={float(gate_cfg['window_s']):.3f} "
                f"count={count} span_c={span_c if span_c is not None else 'NA'} "
                f"slope_c_per_s={slope_c_per_s if slope_c_per_s is not None else 'NA'}"
            )
            if passed:
                self._append_pressure_trace_row(
                    point=point,
                    route=phase,
                    point_phase=phase,
                    trace_stage="dewpoint_gate_stable",
                    pressure_target_hpa=point.target_pressure_hpa,
                    pace_pressure_hpa=ready_values.get("pace_pressure_hpa"),
                    pressure_gauge_hpa=ready_values.get("pressure_gauge_hpa"),
                    dewpoint_c=ready_values.get("dewpoint_c"),
                    dew_temp_c=ready_values.get("dew_temp_c"),
                    dew_rh_pct=ready_values.get("dew_rh_pct"),
                    dewpoint_live_c=live_dewpoint_c,
                    dew_temp_live_c=ready_values.get("dew_temp_live_c"),
                    dew_rh_live_pct=ready_values.get("dew_rh_live_pct"),
                    dewpoint_gate_window_s=gate_cfg["window_s"],
                    dewpoint_gate_elapsed_s=elapsed_s,
                    dewpoint_gate_span_c=span_c,
                    dewpoint_gate_slope_c_per_s=slope_c_per_s,
                    dewpoint_gate_count=count,
                    refresh_pace_state=False,
                    note=note,
                )
                physical_qc_result = None
                if bool(gate_cfg.get("rebound_guard_enabled")):
                    rebound = detect_dewpoint_rebound(
                        gate_rows,
                        rebound_window_s=float(gate_cfg["rebound_window_s"]),
                        rebound_min_rise_c=float(gate_cfg["rebound_min_rise_c"]),
                    )
                    if bool(rebound.get("dewpoint_rebound_detected")):
                        rebound_note = (
                            f"{note} rebound_rise_c={self._as_float(rebound.get('rebound_rise_c'))} "
                            f"rebound_window_s={self._as_float(rebound.get('rebound_window_s'))}"
                        )
                        physical_qc_result = self._evaluate_co2_postseal_physical_qc(
                            point,
                            actual_dewpoint_c=live_dewpoint_c,
                            gate_cfg=gate_cfg,
                        )
                        self._apply_co2_postseal_physical_qc_runtime_fields(
                            point,
                            phase=phase,
                            qc_result=physical_qc_result,
                        )
                        self._append_pressure_trace_row(
                            point=point,
                            route=phase,
                            point_phase=phase,
                            trace_stage="dewpoint_gate_rebound_veto",
                            pressure_target_hpa=point.target_pressure_hpa,
                            pace_pressure_hpa=ready_values.get("pace_pressure_hpa"),
                            pressure_gauge_hpa=ready_values.get("pressure_gauge_hpa"),
                            dewpoint_c=ready_values.get("dewpoint_c"),
                            dew_temp_c=ready_values.get("dew_temp_c"),
                            dew_rh_pct=ready_values.get("dew_rh_pct"),
                            dewpoint_live_c=live_dewpoint_c,
                            dew_temp_live_c=ready_values.get("dew_temp_live_c"),
                            dew_rh_live_pct=ready_values.get("dew_rh_live_pct"),
                            dewpoint_gate_window_s=gate_cfg["window_s"],
                            dewpoint_gate_elapsed_s=elapsed_s,
                            dewpoint_gate_span_c=span_c,
                            dewpoint_gate_slope_c_per_s=slope_c_per_s,
                            dewpoint_gate_count=count,
                            refresh_pace_state=False,
                            note=rebound_note,
                        )
                        self.log(
                            f"{phase.upper()} sealed dewpoint gate rebound veto: "
                            f"elapsed_s={elapsed_s:.3f} count={count} span_c={span_c:.4f} "
                            f"slope_c_per_s={slope_c_per_s:.5f}"
                        )
                        self._set_point_runtime_fields(
                            point,
                            phase=phase,
                            dewpoint_gate_result="rebound_veto",
                            dewpoint_gate_elapsed_s=elapsed_s,
                            dewpoint_gate_count=count,
                            dewpoint_gate_span_c=span_c,
                            dewpoint_gate_slope_c_per_s=slope_c_per_s,
                        )
                        self._set_postseal_timeout_runtime_fields(
                            point,
                            phase=phase,
                            gate_cfg=gate_cfg,
                            timed_out=False,
                            blocked=False,
                        )
                        self._update_point_quality_summary(point, phase=phase)
                        self._append_pressure_trace_row(
                            point=point,
                            route=phase,
                            point_phase=phase,
                            trace_stage="dewpoint_gate_end",
                            pressure_target_hpa=point.target_pressure_hpa,
                            pace_pressure_hpa=ready_values.get("pace_pressure_hpa"),
                            pressure_gauge_hpa=ready_values.get("pressure_gauge_hpa"),
                            dewpoint_c=ready_values.get("dewpoint_c"),
                            dew_temp_c=ready_values.get("dew_temp_c"),
                            dew_rh_pct=ready_values.get("dew_rh_pct"),
                            dewpoint_live_c=live_dewpoint_c,
                            dew_temp_live_c=ready_values.get("dew_temp_live_c"),
                            dew_rh_live_pct=ready_values.get("dew_rh_live_pct"),
                            dewpoint_gate_window_s=gate_cfg["window_s"],
                            dewpoint_gate_elapsed_s=elapsed_s,
                            dewpoint_gate_span_c=span_c,
                            dewpoint_gate_slope_c_per_s=slope_c_per_s,
                            dewpoint_gate_count=count,
                            refresh_pace_state=False,
                            note=f"result=rebound_veto {rebound_note}",
                        )
                        return False
                physical_qc_result = None
                if not point.is_h2o_point:
                    physical_qc_result = self._evaluate_co2_postseal_physical_qc(
                        point,
                        actual_dewpoint_c=live_dewpoint_c,
                        gate_cfg=gate_cfg,
                    )
                    self._apply_co2_postseal_physical_qc_runtime_fields(
                        point,
                        phase=phase,
                        qc_result=physical_qc_result,
                    )
                self._append_pressure_trace_row(
                    point=point,
                    route=phase,
                    point_phase=phase,
                    trace_stage="dewpoint_gate_pass",
                    pressure_target_hpa=point.target_pressure_hpa,
                    pace_pressure_hpa=ready_values.get("pace_pressure_hpa"),
                    pressure_gauge_hpa=ready_values.get("pressure_gauge_hpa"),
                    dewpoint_c=ready_values.get("dewpoint_c"),
                    dew_temp_c=ready_values.get("dew_temp_c"),
                    dew_rh_pct=ready_values.get("dew_rh_pct"),
                    dewpoint_live_c=live_dewpoint_c,
                    dew_temp_live_c=ready_values.get("dew_temp_live_c"),
                    dew_rh_live_pct=ready_values.get("dew_rh_live_pct"),
                    dewpoint_gate_window_s=gate_cfg["window_s"],
                    dewpoint_gate_elapsed_s=elapsed_s,
                    dewpoint_gate_span_c=span_c,
                    dewpoint_gate_slope_c_per_s=slope_c_per_s,
                    dewpoint_gate_count=count,
                    refresh_pace_state=False,
                    note=(
                        f"{note} physical_qc_status={physical_qc_result.get('postseal_physical_qc_status') or 'skipped'}"
                        if isinstance(physical_qc_result, dict)
                        else note
                    ),
                )
                self.log(
                    f"{phase.upper()} sealed dewpoint gate pass: "
                    f"elapsed_s={elapsed_s:.3f} count={count} span_c={span_c:.4f} "
                    f"slope_c_per_s={slope_c_per_s:.5f}"
                )
                self._set_point_runtime_fields(
                    point,
                    phase=phase,
                    dewpoint_gate_result="stable",
                    dewpoint_gate_elapsed_s=elapsed_s,
                    dewpoint_gate_count=count,
                    dewpoint_gate_span_c=span_c,
                    dewpoint_gate_slope_c_per_s=slope_c_per_s,
                    dewpoint_gate_pass_live_c=live_dewpoint_c,
                )
                self._set_postseal_timeout_runtime_fields(
                    point,
                    phase=phase,
                    gate_cfg=gate_cfg,
                    timed_out=False,
                    blocked=False,
                )
                physical_qc_status = (
                    str(physical_qc_result.get("postseal_physical_qc_status") or "").strip().lower()
                    if isinstance(physical_qc_result, dict)
                    else ""
                )
                physical_qc_reason = (
                    str(physical_qc_result.get("postseal_physical_qc_reason") or "").strip()
                    if isinstance(physical_qc_result, dict)
                    else ""
                )
                self._append_pressure_trace_row(
                    point=point,
                    route=phase,
                    point_phase=phase,
                    trace_stage="dewpoint_gate_end",
                    pressure_target_hpa=point.target_pressure_hpa,
                    pace_pressure_hpa=ready_values.get("pace_pressure_hpa"),
                    pressure_gauge_hpa=ready_values.get("pressure_gauge_hpa"),
                    dewpoint_c=ready_values.get("dewpoint_c"),
                    dew_temp_c=ready_values.get("dew_temp_c"),
                    dew_rh_pct=ready_values.get("dew_rh_pct"),
                    dewpoint_live_c=live_dewpoint_c,
                    dew_temp_live_c=ready_values.get("dew_temp_live_c"),
                    dew_rh_live_pct=ready_values.get("dew_rh_live_pct"),
                    dewpoint_gate_window_s=gate_cfg["window_s"],
                    dewpoint_gate_elapsed_s=elapsed_s,
                    dewpoint_gate_span_c=span_c,
                    dewpoint_gate_slope_c_per_s=slope_c_per_s,
                    dewpoint_gate_count=count,
                    refresh_pace_state=False,
                    note=(
                        f"result=stable {note}"
                        + (
                            f" physical_qc_status={physical_qc_status} physical_qc_reason={physical_qc_reason}"
                            if physical_qc_status
                            else ""
                        )
                    ),
                )
                if physical_qc_status == "fail" and str(gate_cfg.get("physical_qc_policy") or "off").lower() == "reject":
                    self.log(
                        f"{phase.upper()} post-seal physical QC reject: "
                        f"{physical_qc_reason or 'postseal_physical_qc_failed'}"
                    )
                    self._update_point_quality_summary(point, phase=phase)
                    return False
                return True
            if time.monotonic() >= deadline:
                physical_qc_result = None
                if not point.is_h2o_point:
                    physical_qc_result = self._evaluate_co2_postseal_physical_qc(
                        point,
                        actual_dewpoint_c=live_dewpoint_c,
                        gate_cfg=gate_cfg,
                    )
                    self._apply_co2_postseal_physical_qc_runtime_fields(
                        point,
                        phase=phase,
                        qc_result=physical_qc_result,
                    )
                self._append_pressure_trace_row(
                    point=point,
                    route=phase,
                    point_phase=phase,
                    trace_stage="dewpoint_gate_timeout",
                    pressure_target_hpa=point.target_pressure_hpa,
                    pace_pressure_hpa=ready_values.get("pace_pressure_hpa"),
                    pressure_gauge_hpa=ready_values.get("pressure_gauge_hpa"),
                    dewpoint_c=ready_values.get("dewpoint_c"),
                    dew_temp_c=ready_values.get("dew_temp_c"),
                    dew_rh_pct=ready_values.get("dew_rh_pct"),
                    dewpoint_live_c=live_dewpoint_c,
                    dew_temp_live_c=ready_values.get("dew_temp_live_c"),
                    dew_rh_live_pct=ready_values.get("dew_rh_live_pct"),
                    dewpoint_gate_window_s=gate_cfg["window_s"],
                    dewpoint_gate_elapsed_s=elapsed_s,
                    dewpoint_gate_span_c=span_c,
                    dewpoint_gate_slope_c_per_s=slope_c_per_s,
                    dewpoint_gate_count=count,
                    refresh_pace_state=False,
                    note=(
                        f"{note} physical_qc_status={physical_qc_result.get('postseal_physical_qc_status') or 'skipped'}"
                        if isinstance(physical_qc_result, dict)
                        else note
                    ),
                )
                self.log(
                    f"{phase.upper()} sealed dewpoint gate timeout: "
                    f"elapsed_s={elapsed_s:.3f} count={count} "
                    f"span_c={span_c if span_c is not None else 'NA'} "
                    f"slope_c_per_s={slope_c_per_s if slope_c_per_s is not None else 'NA'}"
                )
                self._set_point_runtime_fields(
                    point,
                    phase=phase,
                    dewpoint_gate_result="timeout",
                    dewpoint_gate_elapsed_s=elapsed_s,
                    dewpoint_gate_count=count,
                    dewpoint_gate_span_c=span_c,
                    dewpoint_gate_slope_c_per_s=slope_c_per_s,
                )
                timeout_blocked = bool(
                    bool(gate_cfg.get("co2_low_pressure"))
                    and str(gate_cfg.get("timeout_policy") or "pass").lower() == "reject"
                )
                self._set_postseal_timeout_runtime_fields(
                    point,
                    phase=phase,
                    gate_cfg=gate_cfg,
                    timed_out=True,
                    blocked=timeout_blocked,
                )
                self._append_pressure_trace_row(
                    point=point,
                    route=phase,
                    point_phase=phase,
                    trace_stage="dewpoint_gate_end",
                    pressure_target_hpa=point.target_pressure_hpa,
                    pace_pressure_hpa=ready_values.get("pace_pressure_hpa"),
                    pressure_gauge_hpa=ready_values.get("pressure_gauge_hpa"),
                    dewpoint_c=ready_values.get("dewpoint_c"),
                    dew_temp_c=ready_values.get("dew_temp_c"),
                    dew_rh_pct=ready_values.get("dew_rh_pct"),
                    dewpoint_live_c=live_dewpoint_c,
                    dew_temp_live_c=ready_values.get("dew_temp_live_c"),
                    dew_rh_live_pct=ready_values.get("dew_rh_live_pct"),
                    dewpoint_gate_window_s=gate_cfg["window_s"],
                    dewpoint_gate_elapsed_s=elapsed_s,
                    dewpoint_gate_span_c=span_c,
                    dewpoint_gate_slope_c_per_s=slope_c_per_s,
                    dewpoint_gate_count=count,
                    refresh_pace_state=False,
                    note=(
                        f"result=timeout {note}"
                        + (
                            " physical_qc_status="
                            + str(physical_qc_result.get("postseal_physical_qc_status") or "skipped")
                            if isinstance(physical_qc_result, dict)
                            else ""
                        )
                    ),
                )
                if timeout_blocked:
                    self.log(
                        f"{phase.upper()} sealed dewpoint gate timeout rejected by policy: "
                        f"policy={gate_cfg.get('timeout_policy')} elapsed_s={elapsed_s:.3f}"
                    )
                    self._update_point_quality_summary(point, phase=phase)
                    return False
                return True
            self._ensure_pressure_transition_fast_signal_cache(
                active_context,
                reason="post-seal dewpoint gate poll",
            )
            if not self._sampling_window_wait(poll_s, stop_event=active_context.get("stop_event")):
                return False

    def _mark_postseal_dewpoint_gate_skipped(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        context: Optional[Dict[str, Any]] = None,
        reason: str = "postseal_dewpoint_gate_disabled_in_v1",
    ) -> None:
        self._set_point_runtime_fields(
            point,
            phase=phase,
            dewpoint_gate_result="skipped",
            dewpoint_gate_elapsed_s=0.0,
            dewpoint_gate_count=0,
            dewpoint_gate_span_c=None,
            dewpoint_gate_slope_c_per_s=None,
        )
        ready_values = self._cached_ready_check_trace_values(context=context, point=point)
        self._append_pressure_trace_row(
            point=point,
            route=phase,
            point_phase=phase,
            trace_stage="dewpoint_gate_skipped",
            pressure_target_hpa=point.target_pressure_hpa,
            pace_pressure_hpa=ready_values.get("pace_pressure_hpa"),
            pressure_gauge_hpa=ready_values.get("pressure_gauge_hpa"),
            dewpoint_c=ready_values.get("dewpoint_c"),
            dew_temp_c=ready_values.get("dew_temp_c"),
            dew_rh_pct=ready_values.get("dew_rh_pct"),
            dewpoint_live_c=ready_values.get("dewpoint_live_c"),
            dew_temp_live_c=ready_values.get("dew_temp_live_c"),
            dew_rh_live_pct=ready_values.get("dew_rh_live_pct"),
            refresh_pace_state=False,
            dewpoint_gate_elapsed_s=0.0,
            dewpoint_gate_count=0,
            note=reason,
        )

    def _wait_after_pressure_stable_before_sampling(self, point: CalibrationPoint) -> bool:
        phase = "h2o" if point.is_h2o_point else "co2"
        self._warn_pressure_gauge_sampling_freshness_if_needed()
        transition_context = self._pressure_transition_fast_signal_context_active()
        if transition_context is None:
            transition_context = self._start_pressure_transition_fast_signal_context(
                point=point,
                phase=phase,
                reason="pressure in-limits ready check",
                prime_immediately=True,
            )
        missing_fast_signals: List[str] = []
        if isinstance(transition_context, dict):
            missing_fast_signals = self._ensure_pressure_transition_fast_signal_cache(
                transition_context,
                reason="pressure in-limits ready check",
            )

        ready_check_values = self._cached_ready_check_trace_values(context=transition_context, point=point)
        dewpoint_obs = self._recent_fast_signal_numeric_observation(
            "dewpoint",
            "dewpoint_live_c",
            context=transition_context,
            window_s=3.0,
        )
        ready_check_note = "cached fast confirm before sampling after pressure in-limits"
        if dewpoint_obs:
            ready_check_note += (
                f"; dewpoint_obs_window_s={float(dewpoint_obs['window_s']):.1f}"
                f"; dewpoint_span_c={float(dewpoint_obs['span']):.3f}"
                f"; dewpoint_slope_c_per_s={float(dewpoint_obs['slope_per_s']):.4f}"
                f"; dewpoint_points={int(dewpoint_obs['count'])}"
            )
            self.log(
                "Pressure in-limits ready-check dewpoint observation: "
                f"window={float(dewpoint_obs['window_s']):.1f}s "
                f"span={float(dewpoint_obs['span']):.3f}C "
                f"slope={float(dewpoint_obs['slope_per_s']):.4f}C/s "
                f"points={int(dewpoint_obs['count'])}"
            )
        elif missing_fast_signals:
            ready_check_note += f"; missing_fast_signals={','.join(missing_fast_signals)}"
        self._append_pressure_trace_row(
            point=point,
            route=phase,
            point_phase=phase,
            trace_stage="pressure_in_limits_ready_check",
            pressure_target_hpa=point.target_pressure_hpa,
            pace_pressure_hpa=ready_check_values.get("pace_pressure_hpa"),
            pressure_gauge_hpa=ready_check_values.get("pressure_gauge_hpa"),
            dewpoint_c=ready_check_values.get("dewpoint_c"),
            dew_temp_c=ready_check_values.get("dew_temp_c"),
            dew_rh_pct=ready_check_values.get("dew_rh_pct"),
            dewpoint_live_c=ready_check_values.get("dewpoint_live_c"),
            dew_temp_live_c=ready_check_values.get("dew_temp_live_c"),
            dew_rh_live_pct=ready_check_values.get("dew_rh_live_pct"),
            refresh_pace_state=False,
            note=ready_check_note,
        )

        def _record_sampling_begin(trigger_reason: str, note: str) -> None:
            sampling_begin_values = self._cached_ready_check_trace_values(context=transition_context, point=point)
            self._append_pressure_trace_row(
                point=point,
                route=phase,
                point_phase=phase,
                trace_stage="sampling_begin",
                trigger_reason=trigger_reason,
                pressure_target_hpa=point.target_pressure_hpa,
                refresh_pace_state=False,
                pace_pressure_hpa=sampling_begin_values.get("pace_pressure_hpa"),
                pressure_gauge_hpa=sampling_begin_values.get("pressure_gauge_hpa"),
                dewpoint_c=sampling_begin_values.get("dewpoint_c"),
                dew_temp_c=sampling_begin_values.get("dew_temp_c"),
                dew_rh_pct=sampling_begin_values.get("dew_rh_pct"),
                dewpoint_live_c=sampling_begin_values.get("dewpoint_live_c"),
                dew_temp_live_c=sampling_begin_values.get("dew_temp_live_c"),
                dew_rh_live_pct=sampling_begin_values.get("dew_rh_live_pct"),
                note=note,
            )
        if not self._wait_postseal_dewpoint_gate(point, phase=phase, context=transition_context):
            return False
        if not self._wait_co2_presample_long_guard(
            point,
            phase=phase,
            context=transition_context,
        ):
            return False

        adaptive_cfg = self._pressure_sampling_gate_cfg(point)
        if adaptive_cfg["enabled"]:
            adaptive_ok = self._wait_pressure_and_primary_sensor_ready(point)
            if not adaptive_ok:
                return False
            if bool(adaptive_cfg.get("skip_fixed_post_delay", True)):
                _record_sampling_begin(
                    "adaptive_pressure_sampling",
                    "adaptive sampling gate ready after post-seal dewpoint gate",
                )
                return True

        capture_then_hold_enabled = bool(self._wf("workflow.pressure.capture_then_hold_enabled", False))
        if capture_then_hold_enabled and not self._pressure_capture_then_hold_cfg_logged:
            self._pressure_capture_then_hold_cfg_logged = True
            self.log(
                "Pressure capture-then-hold is retired in V1 and will be ignored; "
                "sampling starts immediately after pressure reaches in-limits"
            )

        post_stable_delay_s = 0.0
        general_post_stable_delay_s = self._as_float(self._wf("workflow.pressure.post_stable_sample_delay_s", 0.0))
        if point.is_h2o_point:
            post_stable_delay_s = max(0.0, float(general_post_stable_delay_s or 0.0))
        else:
            co2_post_stable_delay_s = self._as_float(
                self._wf("workflow.pressure.co2_post_stable_sample_delay_s", general_post_stable_delay_s or 0.0)
            )
            post_stable_delay_s = max(0.0, float(co2_post_stable_delay_s or 0.0))
        wait_stop_event = transition_context.get("stop_event") if isinstance(transition_context, dict) else None
        pressure_label = self._pressure_target_label(point) or "sealed pressure point"
        runtime_state = self._point_runtime_state(point, phase=phase)
        stages = dict(runtime_state.get("timing_stages") or {}) if isinstance(runtime_state, dict) else {}
        pressure_in_limits_ts = self._as_float(stages.get("pressure_in_limits"))
        elapsed_since_pressure_in_limits_s: Optional[float] = None
        if pressure_in_limits_ts is not None:
            elapsed_since_pressure_in_limits_s = max(0.0, time.time() - pressure_in_limits_ts)
        remaining_post_stable_delay_s = post_stable_delay_s
        if elapsed_since_pressure_in_limits_s is not None:
            remaining_post_stable_delay_s = max(0.0, post_stable_delay_s - elapsed_since_pressure_in_limits_s)
        if post_stable_delay_s > 0:
            phase_label = "H2O" if point.is_h2o_point else "CO2"
            if remaining_post_stable_delay_s > 0:
                if elapsed_since_pressure_in_limits_s is None:
                    self.log(
                        f"{phase_label} pressure stable; post-seal dewpoint gate complete; "
                        f"waiting {remaining_post_stable_delay_s:.1f}s before sampling at {pressure_label} "
                        "(pressure_in_limits timestamp missing; use full configured minimum delay)"
                    )
                else:
                    self.log(
                        f"{phase_label} pressure stable; post-seal dewpoint gate complete; "
                        f"waiting {remaining_post_stable_delay_s:.1f}s before sampling at {pressure_label} "
                        f"(elapsed since pressure_in_limits={elapsed_since_pressure_in_limits_s:.1f}s, "
                        f"min_delay_s={post_stable_delay_s:.1f})"
                    )
                if not self._sampling_window_wait(remaining_post_stable_delay_s, stop_event=wait_stop_event):
                    return False
                _record_sampling_begin(
                    f"{phase}_post_stable_delay_elapsed",
                    "post-seal dewpoint gate complete; minimum delay from pressure_in_limits satisfied; "
                    f"configured_delay_s={post_stable_delay_s:.1f}; "
                    f"waited_remaining_s={remaining_post_stable_delay_s:.3f}; "
                    + (
                        f"elapsed_before_wait_s={elapsed_since_pressure_in_limits_s:.3f}; "
                        if elapsed_since_pressure_in_limits_s is not None
                        else "pressure_in_limits_ts_missing=true; "
                    )
                    + f"pressure_target={pressure_label}",
                )
                return True

            self.log(
                f"{phase_label} pressure stable; post-seal dewpoint gate complete; "
                f"minimum {post_stable_delay_s:.1f}s from pressure in-limits already satisfied at {pressure_label}"
            )
            _record_sampling_begin(
                f"{phase}_post_stable_delay_satisfied",
                "post-seal dewpoint gate complete; minimum delay from pressure_in_limits already satisfied; "
                f"configured_delay_s={post_stable_delay_s:.1f}; "
                f"elapsed_since_pressure_in_limits_s={elapsed_since_pressure_in_limits_s:.3f}; "
                f"pressure_target={pressure_label}",
            )
            return True

        if point.is_h2o_point:
            self.log("H2O pressure stable; post-seal dewpoint gate complete; start sampling immediately")
            _record_sampling_begin(
                "h2o_post_stable_immediate",
                "post-seal dewpoint gate complete; fixed post-stable delay disabled",
            )
            return True

        self.log("CO2 pressure stable; post-seal dewpoint gate complete; start sampling immediately")
        _record_sampling_begin(
            "co2_post_stable_immediate",
            "post-seal dewpoint gate complete; fixed post-stable delay disabled",
        )
        return True

    def _open_h2o_route_and_wait_ready(self, point: CalibrationPoint, *, point_tag: str = "") -> bool:
        open_valves = self._h2o_open_valves(point)
        fast_handoff_used = self._complete_pending_route_handoff(
            point,
            phase="h2o",
            point_tag=point_tag or self._h2o_point_tag(point),
            open_valves=open_valves,
        )
        if not fast_handoff_used:
            self._set_pressure_controller_vent(True, reason="during H2O route pre-seal preparation")
            self._set_h2o_path(True, point)
            self._append_pressure_trace_row(
                point=point,
                route="h2o",
                point_phase="h2o",
                point_tag=point_tag,
                trace_stage="route_open",
                pressure_target_hpa=point.target_pressure_hpa,
                refresh_pace_state=False,
                note=f"open_valves={open_valves}",
            )
        if not self._ensure_dewpoint_meter_ready():
            return False
        if not self._wait_dewpoint_alignment_stable(point):
            return False
        if not self._wait_h2o_route_dewpoint_gate_before_sampling(point, log_context="H2O route opened"):
            return False
        if not self._wait_h2o_precondition_primary_sensor_gate(point):
            return False
        return True

    def _open_co2_route_for_conditioning(self, point: CalibrationPoint, *, point_tag: str = "") -> None:
        open_valves = self._co2_open_valves(point, include_total_valve=True)
        if self._complete_pending_route_handoff(
            point,
            phase="co2",
            point_tag=point_tag or self._co2_point_tag(point),
            open_valves=open_valves,
        ):
            return
        self._set_co2_route_baseline(reason="before CO2 route conditioning")
        self._set_valves_for_co2(point)
        self._append_pressure_trace_row(
            point=point,
            route="co2",
            point_phase="co2",
            point_tag=point_tag,
            trace_stage="route_open",
            pressure_target_hpa=point.target_pressure_hpa,
            refresh_pace_state=False,
            note=f"open_valves={open_valves}",
        )

    def _wait_h2o_route_soak_before_seal(self, point: CalibrationPoint) -> bool:
        soak_s = float(self._wf("workflow.stability.h2o_route.preseal_soak_s", 300.0))
        if soak_s <= 0:
            return True

        self._set_point_runtime_fields(
            point,
            phase="h2o",
            configured_route_soak_s=soak_s,
        )
        self._append_pressure_trace_row(
            point=point,
            route="h2o",
            point_phase="h2o",
            trace_stage="soak_begin",
            pressure_target_hpa=point.target_pressure_hpa,
            refresh_pace_state=False,
            note=f"configured_route_soak_s={soak_s:.3f} context=h2o_route",
        )
        self._emit_stage_event(
            current=f"H2O 开路等待 Tc={float(point.hgen_temp_c or 0.0):g}°C Uw={float(point.hgen_rh_pct or 0.0):g}%",
            point=point,
            phase="h2o",
            wait_reason="开路预浸泡",
            countdown_s=soak_s,
        )
        self.log(
            f"H2O route opened; wait {int(soak_s)}s before pressure sealing "
            f"(row {point.index})"
        )
        start = time.time()
        while time.time() - start < soak_s:
            if self.stop_event.is_set():
                return False
            self._check_pause()
            remain = soak_s - (time.time() - start)
            if int(remain) in {0, int(soak_s), 30, 60, 120, 180, 240, 300}:
                self._emit_stage_event(
                    current=f"H2O 开路等待 Tc={float(point.hgen_temp_c or 0.0):g}°C Uw={float(point.hgen_rh_pct or 0.0):g}%",
                    point=point,
                    phase="h2o",
                    wait_reason="开路预浸泡",
                    countdown_s=remain,
                )
            time.sleep(min(1.0, max(0.05, remain)))
        self._append_pressure_trace_row(
            point=point,
            route="h2o",
            point_phase="h2o",
            trace_stage="soak_end",
            pressure_target_hpa=point.target_pressure_hpa,
            refresh_pace_state=False,
            note=f"configured_route_soak_s={soak_s:.3f} context=h2o_route",
        )
        return True

    def _capture_preseal_dewpoint_snapshot(self, *, prefer_cached_pressure: bool = False) -> None:
        dew = self.devices.get("dewpoint")
        if not dew:
            self._preseal_dewpoint_snapshot = None
            self.log("Dewpoint meter unavailable before pressure seal; snapshot skipped")
            return

        preseal_pressure_hpa: Optional[float] = None
        if prefer_cached_pressure:
            try:
                cached_pressure, cached_source = self._read_preseal_pressure_gauge()
            except Exception:
                cached_pressure, cached_source = None, ""
            if cached_source == "pressure_gauge" and cached_pressure is not None and math.isfinite(cached_pressure):
                preseal_pressure_hpa = cached_pressure
        if preseal_pressure_hpa is None:
            gauge = self.devices.get("pressure_gauge")
            if gauge:
                try:
                    gauge_value = gauge.read_pressure()
                    gauge_pressure = self._as_float(gauge_value)
                    if gauge_pressure is not None and math.isfinite(gauge_pressure):
                        preseal_pressure_hpa = gauge_pressure
                except Exception:
                    pass
        if preseal_pressure_hpa is None:
            pace = self.devices.get("pace")
            if pace:
                try:
                    pace_pressure = self._as_float(pace.read_pressure())
                    if pace_pressure is not None and math.isfinite(pace_pressure):
                        preseal_pressure_hpa = pace_pressure
                except Exception:
                    pass

        try:
            opener = getattr(dew, "open", None)
            if callable(opener):
                opener()
        except Exception as exc:
            self.log(f"Dewpoint meter open failed before pressure seal: {exc}")

        try:
            data = dew.get_current()
            self._preseal_dewpoint_snapshot = {
                "sample_ts": datetime.now().isoformat(timespec="milliseconds"),
                "dewpoint_c": data.get("dewpoint_c"),
                "temp_c": data.get("temp_c"),
                "rh_pct": data.get("rh_pct"),
                "pressure_hpa": preseal_pressure_hpa,
            }
            self.log(
                "Captured pre-seal dewpoint snapshot: "
                f"dewpoint={data.get('dewpoint_c')} temp={data.get('temp_c')} "
                f"rh={data.get('rh_pct')} pressure={preseal_pressure_hpa}"
            )
        except Exception as exc:
            self._preseal_dewpoint_snapshot = None
            self.log(f"Dewpoint meter pre-seal snapshot failed: {exc}")

    def _ensure_dewpoint_meter_ready(self) -> bool:
        dew = self.devices.get("dewpoint")
        if not dew:
            self.log("Dewpoint meter unavailable")
            return False

        try:
            opener = getattr(dew, "open", None)
            if callable(opener):
                opener()
        except Exception as exc:
            self.log(f"Dewpoint meter open failed: {exc}")
            return False

        try:
            data = dew.get_current()
            self.log(
                f"Dewpoint meter ready: dewpoint={data.get('dewpoint_c')} temp={data.get('temp_c')}"
            )
            return True
        except Exception as exc:
            self.log(f"Dewpoint meter initial read failed: {exc}")
            return False

    def _co2_maps_for_point(self, point: CalibrationPoint) -> List[Dict[str, Any]]:
        valves_cfg = self.cfg.get("valves", {})
        map_a = valves_cfg.get("co2_map", {})
        map_b = valves_cfg.get("co2_map_group2", {})
        group = str(getattr(point, "co2_group", "") or "").strip().upper()
        prefer_b = group in {"B", "2", "G2", "GROUP2", "SECOND"}

        maps: List[Dict[str, Any]] = []
        if prefer_b:
            if isinstance(map_b, dict):
                maps.append(map_b)
            if isinstance(map_a, dict):
                maps.append(map_a)
        else:
            if isinstance(map_a, dict):
                maps.append(map_a)
            if isinstance(map_b, dict):
                maps.append(map_b)
        return maps

    def _co2_path_for_point(self, point: CalibrationPoint) -> Optional[int]:
        co2_ppm = point.co2_ppm
        if co2_ppm is None:
            return self._as_int(self.cfg.get("valves", {}).get("co2_path"))

        ppm_key = str(int(co2_ppm))
        valves_cfg = self.cfg.get("valves", {})
        map_a = valves_cfg.get("co2_map", {}) if isinstance(valves_cfg.get("co2_map", {}), dict) else {}
        map_b = (
            valves_cfg.get("co2_map_group2", {})
            if isinstance(valves_cfg.get("co2_map_group2", {}), dict)
            else {}
        )
        in_a = ppm_key in map_a
        in_b = ppm_key in map_b
        group = str(getattr(point, "co2_group", "") or "").strip().upper()
        prefer_b = group in {"B", "2", "G2", "GROUP2", "SECOND"}

        if prefer_b and in_b:
            return self._as_int(valves_cfg.get("co2_path_group2", valves_cfg.get("co2_path")))
        if in_a:
            return self._as_int(valves_cfg.get("co2_path"))
        if in_b:
            return self._as_int(valves_cfg.get("co2_path_group2", valves_cfg.get("co2_path")))
        return self._as_int(valves_cfg.get("co2_path"))

    def _set_valves_for_co2(self, point: CalibrationPoint) -> None:
        self._apply_valve_states(self._co2_open_valves(point, include_total_valve=True))

    def _set_valves_for_h2o(self, point: CalibrationPoint) -> None:
        self._set_h2o_path(True, point)

    def _co2_open_valves(self, point: CalibrationPoint, include_total_valve: bool) -> List[int]:
        valves_cfg = self.cfg.get("valves", {})
        open_list: List[int] = []

        if include_total_valve:
            total_valve = self._as_int(valves_cfg.get("h2o_path"))
            if total_valve is not None:
                open_list.append(total_valve)
            gas_main = self._as_int(valves_cfg.get("gas_main"))
            if gas_main is not None:
                open_list.append(gas_main)

        co2_path = self._co2_path_for_point(point)
        if co2_path is not None:
            open_list.append(co2_path)

        source = self._source_valve_for_point(point)
        if source is not None:
            open_list.append(source)

        return open_list

    def _source_valve_for_point(self, point: CalibrationPoint) -> Optional[int]:
        co2_ppm = point.co2_ppm
        if co2_ppm is not None:
            ppm_key = str(int(co2_ppm))
            for one_map in self._co2_maps_for_point(point):
                try:
                    val = one_map.get(ppm_key)
                except Exception:
                    val = None
                iv = self._as_int(val)
                if iv is not None:
                    return iv

        if point.is_h2o_point:
            return self._as_int(self.cfg.get("valves", {}).get("h2o_path"))
        return None

    def _pressurize_and_hold(self, point: CalibrationPoint, route: str = "co2") -> bool:
        pace = self.devices.get("pace")
        if not pace:
            self.log("Pressure controller unavailable, cannot seal route")
            return False

        self._clear_preseal_pressure_control_ready_state(
            reason="begin_pressure_seal",
            point=point,
            phase=route,
        )
        self._set_point_runtime_fields(point, phase=route, preseal_trigger_overshoot_hpa=None)
        self._preseal_dewpoint_snapshot = None
        route_name = str(route or "co2").strip().lower()
        phase = "h2o" if route_name == "h2o" or point.is_h2o_point else "co2"
        self._start_pressure_transition_fast_signal_context(
            point=point,
            phase=phase,
            reason=f"before {route.upper()} pressure seal",
        )
        try:
            route_sealed = False
            use_preseal_topoff = bool(getattr(self, "_active_route_requires_preseal_topoff", True))

            def _seal_route_now() -> None:
                nonlocal route_sealed
                if route_sealed:
                    return
                if route_name == "h2o":
                    self._set_h2o_path(False, point)
                else:
                    # Gas-route sealing must close the total valve, current gas-route valve,
                    # and current source valve so the downstream volume is actually sealed
                    # before pressure control starts.
                    self._apply_valve_states([])
                route_sealed = True

            self._emit_stage_event(
                current=self._stage_label_for_point(point, phase=phase, include_pressure=False),
                point=point,
                phase=phase,
                wait_reason="封压准备",
            )
            if route_name == "co2":
                self._capture_preseal_dewpoint_snapshot(prefer_cached_pressure=True)
            else:
                self._capture_preseal_dewpoint_snapshot()
            self._append_pressure_trace_row(
                point=point,
                route=phase,
                point_phase=phase,
                trace_stage="preseal_vent_off_begin",
                pressure_target_hpa=point.target_pressure_hpa,
                refresh_pace_state=False,
                note="before vent off command",
            )
            self._set_pressure_controller_vent(False, reason=f"before {route.upper()} pressure seal")
            self._append_pressure_trace_row(
                point=point,
                route=phase,
                point_phase=phase,
                trace_stage="preseal_vent_off_begin",
                pressure_target_hpa=point.target_pressure_hpa,
                refresh_pace_state=False,
                note="vent off command issued; immediate pre-seal threshold monitoring begins",
            )

            pcfg = self.cfg.get("workflow", {}).get("pressure", {})
            preseal_pressure_peak: Optional[float] = None
            preseal_pressure_last: Optional[float] = None
            preseal_trigger_source = "skipped"
            preseal_trigger_pressure_hpa: Optional[float] = None
            preseal_trigger_threshold_hpa: Optional[float] = None
            valid_preseal_pressure_gauge_seen = False
            invalid_preseal_pressure_gauge_reads = 0
            valid_preseal_pressure_window: deque[tuple[float, float]] = deque()
            timeout_requires_invalid_gauge = bool(pcfg.get("preseal_timeout_requires_invalid_gauge", True))
            valid_gauge_stall_window_s = max(
                0.0,
                float(pcfg.get("preseal_valid_gauge_stall_window_s", 20.0) or 20.0),
            )
            valid_gauge_min_rise_hpa = max(
                0.0,
                float(pcfg.get("preseal_valid_gauge_min_rise_hpa", 0.5) or 0.5),
            )
            wait_after_vent_off_s = float(pcfg.get("pressurize_wait_after_vent_off_s", 5.0))
            if route_name != "h2o" and self._active_post_h2o_co2_zero_flush:
                wait_after_vent_off_s = float(pcfg.get("co2_post_h2o_vent_off_wait_s", 5.0))
            if route_name == "h2o":
                preseal_trigger_threshold_hpa = float(
                    pcfg.get(
                        "h2o_preseal_pressure_gauge_trigger_hpa",
                        pcfg.get("co2_preseal_pressure_gauge_trigger_hpa", 1110.0),
                    )
                )
            else:
                preseal_trigger_threshold_hpa = float(pcfg.get("co2_preseal_pressure_gauge_trigger_hpa", 1110.0))

            if not use_preseal_topoff:
                preseal_trigger_threshold_hpa = None
                wait_after_vent_off_s = 0.0
                no_topoff_open_wait_s = 0.0
                if route_name == "co2":
                    no_topoff_open_wait_s = max(
                        0.0,
                        float(pcfg.get("co2_no_topoff_vent_off_open_wait_s", 2.0) or 0.0),
                    )
                    wait_after_vent_off_s = no_topoff_open_wait_s
                if route_name == "co2" and no_topoff_open_wait_s > 0:
                    self.log(
                        f"{route.upper()} preseal top-off skipped: selected sealed pressures do not include "
                        f"{int(round(float(self._PRESEAL_TOPOFF_TARGET_HPA)))} hPa; "
                        f"vent OFF, keep route open for {no_topoff_open_wait_s:.1f}s, then seal before pressure control"
                    )
                else:
                    self.log(
                        f"{route.upper()} preseal top-off skipped: selected sealed pressures do not include "
                        f"{int(round(float(self._PRESEAL_TOPOFF_TARGET_HPA)))} hPa; "
                        "vent OFF and seal immediately before pressure control"
                    )
            else:
                threshold_text = f"{float(preseal_trigger_threshold_hpa or 0.0):.0f}"
                if wait_after_vent_off_s > 0:
                    self.log(
                        "Pressure controller vent OFF pre-seal wait: "
                        f"seal {route.upper()} route when pressure gauge >= {threshold_text} hPa; "
                        + (
                            f"fallback timeout {wait_after_vent_off_s:.0f}s only when pressure gauge is invalid/unavailable"
                            if timeout_requires_invalid_gauge
                            else f"fallback timeout {wait_after_vent_off_s:.0f}s if threshold is not reached"
                        )
                    )
                else:
                    self.log(
                        "Pressure controller vent OFF pre-seal wait: "
                        f"seal {route.upper()} route when pressure gauge >= {threshold_text} hPa; "
                        "no extra timeout wait configured"
                    )
            if not use_preseal_topoff and route_name == "co2" and wait_after_vent_off_s > 0:
                start = time.time()
                sample_interval_s = self._pressure_transition_monitor_wait_s(point)
                while True:
                    if self.stop_event.is_set():
                        return False
                    self._check_pause()
                    transition_context = self._pressure_transition_fast_signal_context_active()
                    if isinstance(transition_context, dict) and not list(transition_context.get("workers", []) or []):
                        self._refresh_pressure_transition_fast_signal_once(
                            transition_context,
                            reason=f"{route.upper()} preseal open wait",
                        )
                    loop_now = time.time()
                    pressure_now, pressure_source = self._read_preseal_pressure_gauge()
                    if pressure_now is not None and pressure_source == "pressure_gauge":
                        valid_preseal_pressure_gauge_seen = True
                        invalid_preseal_pressure_gauge_reads = 0
                        preseal_pressure_last = pressure_now
                        if preseal_pressure_peak is None or pressure_now > preseal_pressure_peak:
                            preseal_pressure_peak = pressure_now
                    else:
                        invalid_preseal_pressure_gauge_reads += 1
                    elapsed = loop_now - start
                    remain = wait_after_vent_off_s - elapsed
                    wait_note = f"elapsed_s={max(0.0, elapsed):.3f} open_wait_remaining_s={max(0.0, remain):.3f}"
                    if pressure_source != "pressure_gauge":
                        wait_note = (
                            f"pressure_source={pressure_source} "
                            f"open_wait_remaining_s={max(0.0, remain):.3f}"
                        )
                    self._append_pressure_trace_row(
                        point=point,
                        route=phase,
                        point_phase=phase,
                        trace_stage="preseal_wait",
                        trigger_reason=pressure_source,
                        pressure_target_hpa=point.target_pressure_hpa,
                        pressure_gauge_hpa=pressure_now,
                        refresh_pace_state=False,
                        note=wait_note,
                    )
                    if remain <= 0:
                        preseal_trigger_source = "fixed_open_wait_after_vent_off"
                        timeout_note = (
                            f"fixed open-route wait after vent off completed after {wait_after_vent_off_s:.3f}s"
                        )
                        self.log(f"{route.upper()} preseal {timeout_note}")
                        _seal_route_now()
                        trace_values = self._cached_ready_check_trace_values(point=point)
                        pace_pressure_now = self._as_float(trace_values.get("pace_pressure_hpa"))
                        if pace_pressure_now is None:
                            try:
                                pace_pressure_now = self._as_float(pace.read_pressure())
                            except Exception:
                                pace_pressure_now = None
                        dewpoint_kwargs: Dict[str, Any] = {}
                        if any(
                            trace_values.get(key) is not None
                            for key in (
                                "dewpoint_c",
                                "dew_temp_c",
                                "dew_rh_pct",
                                "dewpoint_live_c",
                                "dew_temp_live_c",
                                "dew_rh_live_pct",
                            )
                        ):
                            dewpoint_kwargs = {
                                "dewpoint_c": trace_values.get("dewpoint_c"),
                                "dew_temp_c": trace_values.get("dew_temp_c"),
                                "dew_rh_pct": trace_values.get("dew_rh_pct"),
                                "dewpoint_live_c": trace_values.get("dewpoint_live_c"),
                                "dew_temp_live_c": trace_values.get("dew_temp_live_c"),
                                "dew_rh_live_pct": trace_values.get("dew_rh_live_pct"),
                            }
                        else:
                            dewpoint_kwargs = {"read_dewpoint": True}
                        self._append_pressure_trace_row(
                            point=point,
                            route=phase,
                            point_phase=phase,
                            trace_stage="preseal_trigger_reached",
                            trigger_reason=preseal_trigger_source,
                            pressure_target_hpa=point.target_pressure_hpa,
                            pace_pressure_hpa=pace_pressure_now,
                            pressure_gauge_hpa=preseal_pressure_last,
                            refresh_pace_state=False,
                            note=timeout_note,
                            **dewpoint_kwargs,
                        )
                        break
                    sleep_s = min(sample_interval_s, max(0.02, remain))
                    time.sleep(max(0.02, sleep_s))
            elif wait_after_vent_off_s > 0 or preseal_trigger_threshold_hpa is not None:
                start = time.time()
                sample_interval_s = self._pressure_transition_monitor_wait_s(point)
                while True:
                    if self.stop_event.is_set():
                        return False
                    self._check_pause()
                    transition_context = self._pressure_transition_fast_signal_context_active()
                    if isinstance(transition_context, dict) and not list(transition_context.get("workers", []) or []):
                        self._refresh_pressure_transition_fast_signal_once(
                            transition_context,
                            reason=f"{route.upper()} preseal wait",
                        )
                    loop_now = time.time()
                    pressure_now, pressure_source = self._read_preseal_pressure_gauge()
                    if pressure_now is not None and pressure_source == "pressure_gauge":
                        valid_preseal_pressure_gauge_seen = True
                        invalid_preseal_pressure_gauge_reads = 0
                        preseal_pressure_last = pressure_now
                        if preseal_pressure_peak is None or pressure_now > preseal_pressure_peak:
                            preseal_pressure_peak = pressure_now
                        valid_preseal_pressure_window.append((loop_now, float(pressure_now)))
                        if valid_gauge_stall_window_s > 0:
                            cutoff_ts = loop_now - valid_gauge_stall_window_s
                            while valid_preseal_pressure_window and valid_preseal_pressure_window[0][0] < cutoff_ts:
                                valid_preseal_pressure_window.popleft()
                    else:
                        invalid_preseal_pressure_gauge_reads += 1
                    elapsed = loop_now - start
                    remain = wait_after_vent_off_s - elapsed
                    wait_note = f"elapsed_s={max(0.0, elapsed):.3f}"
                    if wait_after_vent_off_s > 0:
                        wait_note += f" fallback_remaining_s={max(0.0, remain):.3f}"
                    if pressure_source != "pressure_gauge":
                        wait_note = f"pressure_source={pressure_source}"
                        if wait_after_vent_off_s > 0:
                            wait_note += f" fallback_remaining_s={max(0.0, remain):.3f}"
                    self._append_pressure_trace_row(
                        point=point,
                        route=phase,
                        point_phase=phase,
                        trace_stage="preseal_wait",
                        trigger_reason=pressure_source,
                        pressure_target_hpa=point.target_pressure_hpa,
                        pressure_gauge_hpa=pressure_now,
                        refresh_pace_state=False,
                        note=wait_note,
                    )
                    if pressure_now is not None and pressure_now >= float(preseal_trigger_threshold_hpa or 0.0):
                        preseal_trigger_source = "pressure_gauge_threshold"
                        preseal_trigger_pressure_hpa = pressure_now
                        self._set_point_runtime_fields(
                            point,
                            phase=phase,
                            preseal_trigger_overshoot_hpa=max(
                                0.0,
                                float(pressure_now) - float(preseal_trigger_threshold_hpa or 0.0),
                            ),
                        )
                        self.log(
                            f"{route.upper()} preseal pressure_gauge_threshold reached: "
                            f"{pressure_now:.3f} hPa >= {float(preseal_trigger_threshold_hpa or 0.0):.3f} hPa"
                        )
                        _seal_route_now()
                        trace_values = self._cached_ready_check_trace_values(point=point)
                        pace_pressure_now = self._as_float(trace_values.get("pace_pressure_hpa"))
                        if pace_pressure_now is None:
                            try:
                                pace_pressure_now = self._as_float(pace.read_pressure())
                            except Exception:
                                pace_pressure_now = None
                        dewpoint_kwargs: Dict[str, Any] = {}
                        if any(
                            trace_values.get(key) is not None
                            for key in (
                                "dewpoint_c",
                                "dew_temp_c",
                                "dew_rh_pct",
                                "dewpoint_live_c",
                                "dew_temp_live_c",
                                "dew_rh_live_pct",
                            )
                        ):
                            dewpoint_kwargs = {
                                "dewpoint_c": trace_values.get("dewpoint_c"),
                                "dew_temp_c": trace_values.get("dew_temp_c"),
                                "dew_rh_pct": trace_values.get("dew_rh_pct"),
                                "dewpoint_live_c": trace_values.get("dewpoint_live_c"),
                                "dew_temp_live_c": trace_values.get("dew_temp_live_c"),
                                "dew_rh_live_pct": trace_values.get("dew_rh_live_pct"),
                            }
                        else:
                            dewpoint_kwargs = {"read_dewpoint": True}
                        self._append_pressure_trace_row(
                            point=point,
                            route=phase,
                            point_phase=phase,
                            trace_stage="preseal_trigger_reached",
                            trigger_reason=preseal_trigger_source,
                            pressure_target_hpa=point.target_pressure_hpa,
                            pace_pressure_hpa=pace_pressure_now,
                            pressure_gauge_hpa=pressure_now,
                            refresh_pace_state=False,
                            note=(
                                f"pressure_gauge_hpa={pressure_now:.3f} "
                                f"threshold_hpa={float(preseal_trigger_threshold_hpa or 0.0):.3f}"
                            ),
                            **dewpoint_kwargs,
                        )
                        break
                    if (
                        use_preseal_topoff
                        and valid_gauge_stall_window_s > 0
                        and pressure_now is not None
                        and pressure_source == "pressure_gauge"
                        and preseal_trigger_threshold_hpa is not None
                        and pressure_now < float(preseal_trigger_threshold_hpa)
                        and elapsed >= valid_gauge_stall_window_s
                        and len(valid_preseal_pressure_window) >= 2
                    ):
                        window_values = [value for _ts, value in valid_preseal_pressure_window]
                        window_rise_hpa = max(window_values) - min(window_values)
                        if window_rise_hpa < valid_gauge_min_rise_hpa:
                            stall_note = (
                                f"threshold_hpa={float(preseal_trigger_threshold_hpa):.3f} "
                                f"current_hpa={pressure_now:.3f} "
                                f"elapsed_s={elapsed:.3f} "
                                f"stall_window_s={valid_gauge_stall_window_s:.3f} "
                                f"window_rise_hpa={window_rise_hpa:.3f} "
                                f"min_rise_hpa={valid_gauge_min_rise_hpa:.3f}"
                            )
                            self.log(
                                f"{route.upper()} preseal valid gauge stall detected: "
                                f"{stall_note}; abort pressure seal to avoid indefinite wait"
                            )
                            self._append_pressure_trace_row(
                                point=point,
                                route=phase,
                                point_phase=phase,
                                trace_stage="preseal_fail",
                                trigger_reason="preseal_valid_gauge_stall",
                                pressure_target_hpa=point.target_pressure_hpa,
                                pressure_gauge_hpa=pressure_now,
                                refresh_pace_state=False,
                                note=stall_note,
                            )
                            return False
                    if wait_after_vent_off_s <= 0 or remain <= 0:
                        allow_timeout = (
                            (not timeout_requires_invalid_gauge)
                            or (not valid_preseal_pressure_gauge_seen)
                            or invalid_preseal_pressure_gauge_reads >= 3
                        )
                        if allow_timeout:
                            preseal_trigger_source = "timeout"
                            timeout_note = f"fallback timeout without valid pressure gauge after {wait_after_vent_off_s:.3f}s"
                            if valid_preseal_pressure_gauge_seen and invalid_preseal_pressure_gauge_reads >= 3:
                                timeout_note = (
                                    f"fallback timeout after pressure gauge became continuously invalid "
                                    f"for {invalid_preseal_pressure_gauge_reads} reads"
                                )
                            self.log(
                                f"{route.upper()} preseal {timeout_note}"
                            )
                            _seal_route_now()
                            trace_values = self._cached_ready_check_trace_values(point=point)
                            pace_pressure_now = self._as_float(trace_values.get("pace_pressure_hpa"))
                            if pace_pressure_now is None:
                                try:
                                    pace_pressure_now = self._as_float(pace.read_pressure())
                                except Exception:
                                    pace_pressure_now = None
                            dewpoint_kwargs: Dict[str, Any] = {}
                            if any(
                                trace_values.get(key) is not None
                                for key in (
                                    "dewpoint_c",
                                    "dew_temp_c",
                                    "dew_rh_pct",
                                    "dewpoint_live_c",
                                    "dew_temp_live_c",
                                    "dew_rh_live_pct",
                                )
                            ):
                                dewpoint_kwargs = {
                                    "dewpoint_c": trace_values.get("dewpoint_c"),
                                    "dew_temp_c": trace_values.get("dew_temp_c"),
                                    "dew_rh_pct": trace_values.get("dew_rh_pct"),
                                    "dewpoint_live_c": trace_values.get("dewpoint_live_c"),
                                    "dew_temp_live_c": trace_values.get("dew_temp_live_c"),
                                    "dew_rh_live_pct": trace_values.get("dew_rh_live_pct"),
                                }
                            else:
                                dewpoint_kwargs = {"read_dewpoint": True}
                            self._append_pressure_trace_row(
                                point=point,
                                route=phase,
                                point_phase=phase,
                                trace_stage="preseal_trigger_reached",
                                trigger_reason=preseal_trigger_source,
                                pressure_target_hpa=point.target_pressure_hpa,
                                pace_pressure_hpa=pace_pressure_now,
                                pressure_gauge_hpa=preseal_pressure_last,
                                refresh_pace_state=False,
                                note=timeout_note,
                                **dewpoint_kwargs,
                            )
                            break
                    sleep_s = sample_interval_s
                    if wait_after_vent_off_s > 0 and remain > 0:
                        sleep_s = min(sample_interval_s, max(0.02, remain))
                    time.sleep(max(0.02, sleep_s))
            else:
                preseal_trigger_source = "no_wait"
                _seal_route_now()
                trace_values = self._cached_ready_check_trace_values(point=point)
                dewpoint_kwargs: Dict[str, Any] = {}
                if any(
                    trace_values.get(key) is not None
                    for key in (
                        "dewpoint_c",
                        "dew_temp_c",
                        "dew_rh_pct",
                        "dewpoint_live_c",
                        "dew_temp_live_c",
                        "dew_rh_live_pct",
                    )
                ):
                    dewpoint_kwargs = {
                        "dewpoint_c": trace_values.get("dewpoint_c"),
                        "dew_temp_c": trace_values.get("dew_temp_c"),
                        "dew_rh_pct": trace_values.get("dew_rh_pct"),
                        "dewpoint_live_c": trace_values.get("dewpoint_live_c"),
                        "dew_temp_live_c": trace_values.get("dew_temp_live_c"),
                        "dew_rh_live_pct": trace_values.get("dew_rh_live_pct"),
                    }
                self._append_pressure_trace_row(
                    point=point,
                    route=phase,
                    point_phase=phase,
                    trace_stage="preseal_trigger_reached",
                    trigger_reason=preseal_trigger_source,
                    pressure_target_hpa=point.target_pressure_hpa,
                    pace_pressure_hpa=trace_values.get("pace_pressure_hpa"),
                    pressure_gauge_hpa=trace_values.get("pressure_gauge_hpa"),
                    refresh_pace_state=False,
                    note=(
                        "selected sealed pressures do not include 1100hPa; "
                        "vent off and seal immediately before pressure control"
                        if not use_preseal_topoff
                        else "vent-off settle wait disabled"
                    ),
                    **dewpoint_kwargs,
                )

            if route_name in {"co2", "h2o"}:
                route_title = route.upper()
                if preseal_pressure_peak is not None:
                    trigger_detail = ""
                    if (
                        preseal_trigger_source == "pressure_gauge_threshold"
                        and preseal_trigger_pressure_hpa is not None
                        and preseal_trigger_threshold_hpa is not None
                    ):
                        trigger_detail = (
                            f"pressure gauge trigger={preseal_trigger_pressure_hpa:.3f} hPa "
                            f">= {preseal_trigger_threshold_hpa:.3f} hPa; "
                        )
                    elif preseal_trigger_source == "timeout":
                        trigger_detail = (
                            f"fallback timeout without valid pressure gauge after {wait_after_vent_off_s:.3f}s; "
                        )
                    elif preseal_trigger_source == "fixed_open_wait_after_vent_off":
                        trigger_detail = (
                            f"kept route open for {wait_after_vent_off_s:.3f}s after vent OFF; "
                        )
                    self.log(
                        f"{route_title} route vent OFF settle complete; "
                        f"pre-seal pressure peak={preseal_pressure_peak:.3f} hPa "
                        f"last={preseal_pressure_last:.3f} hPa; "
                        f"{trigger_detail}"
                        "seal route directly before pressure control"
                    )
                else:
                    if preseal_trigger_source == "timeout":
                        self.log(
                            f"{route_title} route vent OFF settle complete; "
                            f"fallback timeout without valid pressure gauge after {wait_after_vent_off_s:.3f}s; "
                            "seal route directly before pressure control"
                        )
                    elif preseal_trigger_source == "fixed_open_wait_after_vent_off":
                        self.log(
                            f"{route_title} route vent OFF settle complete; "
                            f"kept route open for {wait_after_vent_off_s:.3f}s after vent OFF; "
                            "seal route directly before pressure control"
                        )
                    else:
                        self.log(f"{route_title} route vent OFF settle complete; seal route directly before pressure control")

            _seal_route_now()

            trace_values = self._cached_ready_check_trace_values(point=point)
            last_pressure = self._as_float(trace_values.get("pace_pressure_hpa"))
            if last_pressure is None and use_preseal_topoff:
                try:
                    last_pressure = float(pace.read_pressure())
                except Exception:
                    last_pressure = None
            if preseal_pressure_peak is not None:
                self.log(
                    f"{route.upper()} route sealed for pressure control "
                    f"(pre-seal peak={preseal_pressure_peak:.3f} hPa, "
                    f"pre-seal last={preseal_pressure_last:.3f} hPa, "
                    f"sealed pressure={last_pressure})"
                )
            else:
                self.log(f"{route.upper()} route sealed for pressure control (pressure={last_pressure})")
            self._record_preseal_pressure_control_ready_state(
                point,
                phase=phase,
                defer_live_check=(route_name == "co2") or (not use_preseal_topoff),
            )
            ready_state = dict(self._preseal_pressure_control_ready_state or {})
            ready_failures = list(ready_state.get("failures") or [])
            ready_verification_pending = bool(ready_state.get("ready_verification_pending"))
            self._append_pressure_trace_row(
                point=point,
                route=phase,
                point_phase=phase,
                trace_stage="route_sealed",
                trigger_reason=preseal_trigger_source,
                pressure_target_hpa=point.target_pressure_hpa,
                pace_pressure_hpa=last_pressure,
                pressure_gauge_hpa=preseal_trigger_pressure_hpa if preseal_trigger_pressure_hpa is not None else preseal_pressure_last,
                refresh_pace_state=False,
                note=(
                    "route sealed for pressure control; "
                    + (
                        "preseal_ready=deferred_live_check"
                        if ready_verification_pending
                        else "preseal_ready=ok"
                        if not ready_failures
                        else f"preseal_ready_failures={','.join(ready_failures)}"
                    )
                ),
            )
            return True
        finally:
            self._stop_pressure_transition_fast_signal_context(reason=f"after {route.upper()} pressure seal")
            if route_name != "h2o":
                self._active_post_h2o_co2_zero_flush = False

    def _build_point_summary_row(
        self,
        point: CalibrationPoint,
        samples: List[Dict[str, Any]],
        *,
        phase: str,
        point_tag: str,
        integrity_summary: Dict[str, Any],
    ) -> Dict[str, Any]:
        co2_vals = self._primary_or_first_usable_analyzer_series(samples, "co2_ppm")
        h2o_vals = self._primary_or_first_usable_analyzer_series(samples, "h2o_mmol")
        p_vals = self._numeric_series_for_key(samples, "pressure_hpa")
        gauge_vals = self._numeric_series_for_key(samples, "pressure_gauge_hpa")
        dewpoint_vals = self._numeric_series_for_key(samples, "dewpoint_c")
        dew_temp_vals = self._numeric_series_for_key(samples, "dew_temp_c")
        dew_rh_vals = self._numeric_series_for_key(samples, "dew_rh_pct")
        co2_mean_primary_or_first = mean(co2_vals) if co2_vals else None
        h2o_mean_primary_or_first = mean(h2o_vals) if h2o_vals else None
        include_fleet_stats = bool(cfg_get(self.cfg, "workflow.reporting.include_fleet_stats", False))
        runtime_state = dict(self._point_runtime_state(point, phase=phase) or {})
        timing_delta_map = self._point_timing_delta_map(dict(runtime_state.get("timing_stages") or {}))
        gas_type = self._point_gas_type(point, phase=phase)
        target_value = self._point_target_value(point, phase=phase)
        measured_value = h2o_mean_primary_or_first if gas_type == "H2O" else co2_mean_primary_or_first
        window_start_ts = samples[0].get("sample_ts") if samples else None
        last_sample = samples[-1] if samples else {}
        window_end_ts = last_sample.get("sample_end_ts") or last_sample.get("sample_ts")
        sample_ts = window_end_ts or window_start_ts

        row = {
            "run_id": getattr(self.logger, "run_id", ""),
            "session_id": getattr(self.logger, "run_id", ""),
            "device_id": self._point_device_id_from_samples(samples),
            "gas_type": gas_type,
            "step": phase,
            "point_no": point.index,
            "target_value": target_value,
            "measured_value": measured_value,
            "sample_ts": sample_ts,
            "window_start_ts": window_start_ts,
            "window_end_ts": window_end_ts,
            "sample_count": len(samples),
            "stable_flag": self._stable_flag_from_runtime_state(runtime_state),
            "point_title": self._point_title(point, phase=phase, point_tag=point_tag),
            "point_row": point.index,
            "point_phase": phase,
            "point_tag": point_tag,
            "pressure_mode": self._pressure_mode_for_point(point),
            "pressure_target_label": self._pressure_target_label(point),
            "temp_chamber_c": point.temp_chamber_c,
            "co2_ppm_target": point.co2_ppm,
            "hgen_temp_c": point.hgen_temp_c,
            "hgen_rh_pct": point.hgen_rh_pct,
            "pressure_target_hpa": point.target_pressure_hpa,
            "co2_mean": co2_mean_primary_or_first,
            "co2_std": stdev(co2_vals) if len(co2_vals) > 1 else None,
            "co2_valid_count": len(co2_vals),
            "h2o_mean": h2o_mean_primary_or_first,
            "h2o_std": stdev(h2o_vals) if len(h2o_vals) > 1 else None,
            "h2o_valid_count": len(h2o_vals),
            "co2_mean_primary_or_first": co2_mean_primary_or_first,
            "co2_mean_primary_or_first_valid_count": len(co2_vals),
            "h2o_mean_primary_or_first": h2o_mean_primary_or_first,
            "h2o_mean_primary_or_first_valid_count": len(h2o_vals),
            "analyzer_mean_mode": "primary_or_first_usable",
            "pressure_mean": mean(p_vals) if p_vals else None,
            "pressure_valid_count": len(p_vals),
            "controller_pressure_mean": mean(p_vals) if p_vals else None,
            "controller_pressure_valid_count": len(p_vals),
            "gauge_pressure_mean": mean(gauge_vals) if gauge_vals else None,
            "gauge_pressure_valid_count": len(gauge_vals),
            "dewpoint_c_valid_count": len(dewpoint_vals),
            "dew_temp_c_valid_count": len(dew_temp_vals),
            "dew_rh_pct_valid_count": len(dew_rh_vals),
            "dewpoint_gate_result": runtime_state.get("dewpoint_gate_result"),
            "dewpoint_gate_elapsed_s": runtime_state.get("dewpoint_gate_elapsed_s"),
            "dewpoint_gate_count": runtime_state.get("dewpoint_gate_count"),
            "dewpoint_gate_span_c": runtime_state.get("dewpoint_gate_span_c"),
            "dewpoint_gate_slope_c_per_s": runtime_state.get("dewpoint_gate_slope_c_per_s"),
            "preseal_dewpoint_c": runtime_state.get("preseal_dewpoint_c"),
            "preseal_temp_c": runtime_state.get("preseal_temp_c"),
            "preseal_rh_pct": runtime_state.get("preseal_rh_pct"),
            "preseal_pressure_hpa": runtime_state.get("preseal_pressure_hpa"),
            "preseal_trigger_overshoot_hpa": runtime_state.get("preseal_trigger_overshoot_hpa"),
            "postseal_expected_dewpoint_c": runtime_state.get("postseal_expected_dewpoint_c"),
            "postseal_actual_dewpoint_c": runtime_state.get("postseal_actual_dewpoint_c"),
            "postseal_physical_delta_c": runtime_state.get("postseal_physical_delta_c"),
            "postseal_physical_qc_status": runtime_state.get("postseal_physical_qc_status"),
            "postseal_physical_qc_reason": runtime_state.get("postseal_physical_qc_reason"),
            "postseal_timeout_policy": runtime_state.get("postseal_timeout_policy"),
            "postseal_timeout_blocked": runtime_state.get("postseal_timeout_blocked"),
            "point_quality_timeout_flag": runtime_state.get("point_quality_timeout_flag"),
            "dewpoint_gate_pass_live_c": runtime_state.get("dewpoint_gate_pass_live_c"),
            "presample_long_guard_status": runtime_state.get("presample_long_guard_status"),
            "presample_long_guard_reason": runtime_state.get("presample_long_guard_reason"),
            "presample_long_guard_elapsed_s": runtime_state.get("presample_long_guard_elapsed_s"),
            "presample_long_guard_span_c": runtime_state.get("presample_long_guard_span_c"),
            "presample_long_guard_slope_c_per_s": runtime_state.get("presample_long_guard_slope_c_per_s"),
            "presample_long_guard_rise_c": runtime_state.get("presample_long_guard_rise_c"),
            "first_effective_sample_dewpoint_c": runtime_state.get("first_effective_sample_dewpoint_c"),
            "postgate_to_first_effective_dewpoint_rise_c": runtime_state.get(
                "postgate_to_first_effective_dewpoint_rise_c"
            ),
            "postsample_late_rebound_status": runtime_state.get("postsample_late_rebound_status"),
            "postsample_late_rebound_reason": runtime_state.get("postsample_late_rebound_reason"),
            "sampling_window_dewpoint_first_c": runtime_state.get("sampling_window_dewpoint_first_c"),
            "sampling_window_dewpoint_last_c": runtime_state.get("sampling_window_dewpoint_last_c"),
            "sampling_window_dewpoint_range_c": runtime_state.get("sampling_window_dewpoint_range_c"),
            "sampling_window_dewpoint_rise_c": runtime_state.get("sampling_window_dewpoint_rise_c"),
            "sampling_window_dewpoint_slope_c_per_s": runtime_state.get("sampling_window_dewpoint_slope_c_per_s"),
            "sampling_window_qc_status": runtime_state.get("sampling_window_qc_status"),
            "sampling_window_qc_reason": runtime_state.get("sampling_window_qc_reason"),
            "dewpoint_time_to_gate": runtime_state.get("dewpoint_time_to_gate"),
            "dewpoint_tail_span_60s": runtime_state.get("dewpoint_tail_span_60s"),
            "dewpoint_tail_slope_60s": runtime_state.get("dewpoint_tail_slope_60s"),
            "dewpoint_rebound_detected": runtime_state.get("dewpoint_rebound_detected"),
            "flush_gate_status": runtime_state.get("flush_gate_status"),
            "flush_gate_reason": runtime_state.get("flush_gate_reason"),
            "pressure_gauge_stale_count": runtime_state.get("pressure_gauge_stale_count"),
            "pressure_gauge_total_count": runtime_state.get("pressure_gauge_total_count"),
            "pressure_gauge_stale_ratio": runtime_state.get("pressure_gauge_stale_ratio"),
            "point_quality_status": runtime_state.get("point_quality_status"),
            "point_quality_reason": runtime_state.get("point_quality_reason"),
            "point_quality_flags": runtime_state.get("point_quality_flags"),
            "point_quality_blocked": runtime_state.get("point_quality_blocked"),
            "preseal_vent_off_begin_to_route_sealed_ms": timing_delta_map.get(
                "preseal_vent_off_begin_to_route_sealed_ms"
            ),
            "route_sealed_to_control_prepare_begin_ms": timing_delta_map.get(
                "route_sealed_to_control_prepare_begin_ms"
            ),
            "pressure_in_limits_to_sampling_begin_ms": timing_delta_map.get(
                "pressure_in_limits_to_sampling_begin_ms"
            ),
            "first_valid_pace_ms": runtime_state.get("first_valid_pace_ms"),
            "first_valid_pressure_gauge_ms": runtime_state.get("first_valid_pressure_gauge_ms"),
            "first_valid_dewpoint_ms": runtime_state.get("first_valid_dewpoint_ms"),
            "first_valid_analyzer_ms": runtime_state.get("first_valid_analyzer_ms"),
            "effective_sample_started_on_row": runtime_state.get("effective_sample_started_on_row"),
        }
        if include_fleet_stats:
            co2_fleet_mean, co2_fleet_std = self._fleet_analyzer_point_stats(samples, "co2_ppm")
            h2o_fleet_mean, h2o_fleet_std = self._fleet_analyzer_point_stats(samples, "h2o_mmol")
            row.update(
                {
                    "co2_fleet_mean": co2_fleet_mean,
                    "co2_fleet_std": co2_fleet_std,
                    "h2o_fleet_mean": h2o_fleet_mean,
                    "h2o_fleet_std": h2o_fleet_std,
                }
            )
        row.update(integrity_summary)
        row.update(self._build_gas_field_means(samples))
        row.update(self._build_device_field_means(samples, existing_summary_keys=set(row.keys())))
        return row

    def _point_gas_type(self, point: CalibrationPoint, *, phase: str) -> str:
        phase_text = str(phase or "").strip().lower()
        if phase_text == "h2o" or bool(getattr(point, "is_h2o_point", False)):
            return "H2O"
        return "CO2"

    def _point_target_value(self, point: CalibrationPoint, *, phase: str) -> Optional[float]:
        gas_type = self._point_gas_type(point, phase=phase)
        if gas_type == "H2O":
            value = getattr(point, "h2o_mmol", None)
            if value is None:
                value = getattr(point, "raw_h2o", None)
            return self._as_float(value)
        return self._as_float(getattr(point, "co2_ppm", None))

    def _sample_device_id(self, row: Dict[str, Any]) -> str:
        direct = _normalized_device_id_text(row.get("device_id") or row.get("id"))
        if direct:
            return direct
        for label, _ga, _cfg in self._all_gas_analyzers():
            prefix = self._safe_label(label)
            candidate = _normalized_device_id_text(row.get(f"{prefix}_id"))
            if candidate:
                return candidate
        for key, value in row.items():
            if re.match(r"^ga\d+_id$", str(key or "").strip(), re.IGNORECASE):
                candidate = _normalized_device_id_text(value)
                if candidate:
                    return candidate
        return ""

    def _point_device_id_from_samples(self, samples: List[Dict[str, Any]]) -> str:
        for row in samples:
            candidate = self._sample_device_id(row)
            if candidate:
                return candidate
        return ""

    def _stable_flag_from_runtime_state(self, runtime_state: Dict[str, Any]) -> bool:
        if bool(runtime_state.get("point_quality_blocked", False)):
            return False
        for key in ("sampling_window_qc_status", "point_quality_status", "flush_gate_status"):
            status = str(runtime_state.get(key) or "").strip().lower()
            if status in {"fail", "failed", "reject", "rejected", "timeout"}:
                return False
        return True

    def _annotate_point_trace_rows(
        self,
        point: CalibrationPoint,
        samples: List[Dict[str, Any]],
        *,
        phase: str,
        point_tag: str,
    ) -> None:
        if not samples:
            return
        gas_type = self._point_gas_type(point, phase=phase)
        target_value = self._point_target_value(point, phase=phase)
        window_start_ts = samples[0].get("sample_ts")
        window_end_ts = samples[-1].get("sample_end_ts") or samples[-1].get("sample_ts")
        runtime_state = dict(self._point_runtime_state(point, phase=phase) or {})
        stable_flag = self._stable_flag_from_runtime_state(runtime_state)
        for row in samples:
            row["run_id"] = getattr(self.logger, "run_id", "")
            row["session_id"] = getattr(self.logger, "run_id", "")
            row["gas_type"] = gas_type
            row["step"] = phase
            row["point_no"] = point.index
            row["target_value"] = target_value
            row["measured_value"] = self._as_float(row.get("h2o_mmol" if gas_type == "H2O" else "co2_ppm"))
            row["window_start_ts"] = window_start_ts
            row["window_end_ts"] = window_end_ts
            row["sample_count"] = len(samples)
            row["stable_flag"] = stable_flag
            device_id = self._sample_device_id(row)
            if device_id:
                row["device_id"] = device_id

    def _perform_heavy_point_exports(
        self,
        point: CalibrationPoint,
        samples: List[Dict[str, Any]],
        *,
        phase: str,
        point_tag: str,
        analyzer_labels: List[str],
        integrity_summary: Dict[str, Any],
    ) -> None:
        try:
            analyzer_summary = self.logger.log_analyzer_summary(
                samples,
                analyzer_labels=analyzer_labels,
            )
            self.log(f"Point {point.index} analyzer summary updated: {analyzer_summary}")
        except Exception as exc:
            self.log(f"Point {point.index} analyzer summary save failed: {exc}")
        try:
            analyzer_book = self.logger.log_analyzer_workbook(
                samples,
                analyzer_labels=analyzer_labels,
                phase=phase,
                write_summary=False,
            )
            self.log(f"Point {point.index} analyzer workbook updated: {analyzer_book}")
        except Exception as exc:
            self.log(f"Point {point.index} analyzer workbook save failed: {exc}")

        row = self._build_point_summary_row(
            point,
            samples,
            phase=phase,
            point_tag=point_tag,
            integrity_summary=integrity_summary,
        )
        row["save_ts"] = self._ts_from_datetime(datetime.now())
        try:
            self.logger.log_point(row)
        except Exception as exc:
            self.log(f"Point {point.index} point export failed: {exc}")
        finally:
            point_key = self._point_runtime_key(point, phase=phase)
            if point_key is not None:
                self._point_runtime_summary.pop(point_key, None)

    def _perform_light_point_exports(
        self,
        point: CalibrationPoint,
        samples: List[Dict[str, Any]],
        *,
        phase: str,
        point_tag: str,
    ) -> None:
        stored_samples: List[Dict[str, Any]] = []
        for data in samples:
            payload = dict(data)
            payload["save_ts"] = self._ts_from_datetime(datetime.now())
            try:
                self.logger.log_sample(payload)
            except Exception as exc:
                self.log(f"Point {point.index} sample export failed: {exc}")
            stored_samples.append(payload)
            self._all_samples.append(dict(payload))
        try:
            point_csv = self.logger.log_point_samples(point.index, stored_samples, phase=phase, tag=point_tag)
            self.log(f"Point {point.index} samples saved: {point_csv}")
        except Exception as exc:
            self.log(f"Point {point.index} sample CSV save failed: {exc}")

    def _enqueue_deferred_sample_exports(
        self,
        point: CalibrationPoint,
        samples: List[Dict[str, Any]],
        *,
        phase: str,
        point_tag: str,
    ) -> None:
        self._deferred_sample_exports.append(
            {
                "point": point,
                "samples": [dict(row) for row in samples],
                "phase": str(phase or "").strip().lower(),
                "point_tag": str(point_tag or ""),
            }
        )
        self.log(
            f"Point {point.index} sample exports deferred: "
            f"phase={phase or '--'} queue_len={len(self._deferred_sample_exports)}"
        )

    def _flush_deferred_sample_exports(self, *, reason: str = "") -> None:
        if not self._deferred_sample_exports:
            return
        queue = list(self._deferred_sample_exports)
        self._deferred_sample_exports.clear()
        extra = f" ({reason})" if reason else ""
        self.log(f"Flushing deferred sample exports{extra}: count={len(queue)}")
        for item in queue:
            self._perform_light_point_exports(
                item["point"],
                item["samples"],
                phase=item["phase"],
                point_tag=item["point_tag"],
            )

    def _enqueue_deferred_point_exports(
        self,
        point: CalibrationPoint,
        samples: List[Dict[str, Any]],
        *,
        phase: str,
        point_tag: str,
        analyzer_labels: List[str],
        integrity_summary: Dict[str, Any],
    ) -> None:
        self._deferred_point_exports.append(
            {
                "point": point,
                "samples": [dict(row) for row in samples],
                "phase": str(phase or "").strip().lower(),
                "point_tag": str(point_tag or ""),
                "analyzer_labels": list(analyzer_labels),
                "integrity_summary": dict(integrity_summary),
            }
        )
        self.log(
            f"Point {point.index} heavy exports deferred: "
            f"phase={phase or '--'} queue_len={len(self._deferred_point_exports)}"
        )

    def _flush_deferred_point_exports(self, *, reason: str = "") -> None:
        if not self._deferred_point_exports:
            return
        queue = list(self._deferred_point_exports)
        self._deferred_point_exports.clear()
        extra = f" ({reason})" if reason else ""
        self.log(f"Flushing deferred heavy exports{extra}: count={len(queue)}")
        for item in queue:
            self._perform_heavy_point_exports(
                item["point"],
                item["samples"],
                phase=item["phase"],
                point_tag=item["point_tag"],
                analyzer_labels=item["analyzer_labels"],
                integrity_summary=item["integrity_summary"],
            )

    def _record_last_sample_completion(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        point_tag: str,
        sample_done_ts: float,
        last_sample: Dict[str, Any],
    ) -> None:
        self._last_sample_completion = {
            "point": point,
            "phase": str(phase or "").strip().lower(),
            "point_tag": str(point_tag or ""),
            "sample_done_ts": float(sample_done_ts),
            "pace_pressure_hpa": self._as_float(last_sample.get("pressure_hpa")),
            "pressure_gauge_hpa": self._as_float(last_sample.get("pressure_gauge_hpa")),
            "dewpoint_c": self._as_float(last_sample.get("dewpoint_c")),
            "dew_temp_c": self._as_float(last_sample.get("dew_temp_c")),
            "dew_rh_pct": self._as_float(last_sample.get("dew_rh_pct")),
            "fast_group_span_ms": self._as_float(last_sample.get("fast_group_span_ms")),
            "dewpoint_live_c": self._as_float(last_sample.get("dewpoint_live_c")),
            "dew_temp_live_c": self._as_float(last_sample.get("dew_temp_live_c")),
            "dew_rh_live_pct": self._as_float(last_sample.get("dew_rh_live_pct")),
            "sample_lag_ms": self._as_float(last_sample.get("sample_lag_ms")),
            "pace_output_state": self._as_int(last_sample.get("pace_output_state")),
            "pace_isolation_state": self._as_int(last_sample.get("pace_isolation_state")),
            "pace_vent_status": self._as_int(last_sample.get("pace_vent_status")),
        }

    @staticmethod
    def _sample_row_wall_ts(row: Dict[str, Any], key: str = "sample_end_ts") -> Optional[float]:
        value = row.get(key)
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            try:
                return float(value)
            except Exception:
                return None
        try:
            return datetime.fromisoformat(str(value)).timestamp()
        except Exception:
            return None

    def _maybe_begin_requested_sample_handoff(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        point_tag: str,
    ) -> bool:
        request = self._sample_handoff_request
        if not isinstance(request, dict):
            return False
        if str(request.get("current_phase") or "") != str(phase or ""):
            return False
        expected_point_tag = str(request.get("current_point_tag") or "")
        if expected_point_tag and expected_point_tag != str(point_tag or ""):
            return False
        armed = self._begin_pending_route_handoff(
            current_point=point,
            current_phase=phase,
            current_point_tag=point_tag,
            next_point=request["next_point"],
            next_phase=request["next_phase"],
            next_point_tag=request["next_point_tag"],
            next_open_valves=list(request.get("next_open_valves") or []),
        )
        request["armed"] = bool(armed)
        self._sample_handoff_request = request
        return bool(armed)

    def _request_sample_export_deferral(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        point_tag: str,
        mode: str,
    ) -> bool:
        if self._sample_export_deferral_request is not None:
            self.log(
                "Sample export deferral request skipped: "
                f"phase={phase or '--'} point={point.index} reason=request_already_pending"
            )
            return False
        if self._deferred_sample_exports or self._deferred_point_exports:
            self.log(
                "Sample export deferral request skipped: "
                f"phase={phase or '--'} point={point.index} reason=deferred_export_queue_busy"
            )
            return False
        self._sample_export_deferral_request = {
            "phase": str(phase or "").strip().lower(),
            "point_row": int(point.index),
            "point_tag": str(point_tag or ""),
            "mode": str(mode or "").strip().lower(),
        }
        return True

    def _consume_requested_sample_export_deferral(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        point_tag: str,
    ) -> Optional[Dict[str, Any]]:
        request = self._sample_export_deferral_request
        if not isinstance(request, dict):
            return None
        if str(request.get("phase") or "") != str(phase or ""):
            return None
        expected_point_tag = str(request.get("point_tag") or "")
        if expected_point_tag:
            if expected_point_tag != str(point_tag or ""):
                return None
        elif self._as_int(request.get("point_row")) != int(point.index):
            return None
        self._sample_export_deferral_request = None
        return dict(request)

    def _clear_requested_sample_export_deferral(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        point_tag: str,
    ) -> None:
        request = self._sample_export_deferral_request
        if not isinstance(request, dict):
            return
        if str(request.get("phase") or "") != str(phase or ""):
            return
        expected_point_tag = str(request.get("point_tag") or "")
        if expected_point_tag:
            if expected_point_tag != str(point_tag or ""):
                return
        elif self._as_int(request.get("point_row")) != int(point.index):
            return
        self._sample_export_deferral_request = None

    def _last_sample_completion_pace_state(self, completion: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        completion = dict(completion or self._last_sample_completion or {})
        cache_snapshot = self._pace_state_cache_snapshot()
        return {
            "pace_output_state": self._as_int(
                completion.get("pace_output_state", cache_snapshot.get("pace_output_state"))
            ),
            "pace_isolation_state": self._as_int(
                completion.get("pace_isolation_state", cache_snapshot.get("pace_isolation_state"))
            ),
            "pace_vent_status": self._as_int(
                completion.get("pace_vent_status", cache_snapshot.get("pace_vent_status"))
            ),
        }

    def _route_open_valves(self, point: CalibrationPoint, *, phase: str) -> List[int]:
        phase_text = str(phase or "").strip().lower()
        if phase_text == "h2o":
            return self._h2o_open_valves(point)
        return self._co2_open_valves(point, include_total_valve=True)

    def _pending_route_handoff_matches(
        self,
        *,
        point: Optional[CalibrationPoint],
        phase: str,
        point_tag: str = "",
    ) -> bool:
        handoff_state = dict(self._pending_route_handoff or {})
        if not handoff_state:
            return False
        if str(handoff_state.get("next_phase") or "") != str(phase or ""):
            return False
        expected_point_tag = str(handoff_state.get("next_point_tag") or "")
        if expected_point_tag and expected_point_tag == str(point_tag or ""):
            return True
        if point is None:
            return False
        expected_index = self._as_int(getattr(handoff_state.get("next_point"), "index", None))
        return expected_index is not None and expected_index == int(point.index)

    def _discard_pending_route_handoff(
        self,
        *,
        point: Optional[CalibrationPoint],
        phase: str,
        point_tag: str = "",
        reason: str = "",
    ) -> None:
        if not self._pending_route_handoff_matches(point=point, phase=phase, point_tag=point_tag):
            return
        extra = f": {reason}" if reason else ""
        self.log(f"Discard pending route handoff{extra}")
        self._pending_route_handoff = None
        self._stop_pressure_transition_fast_signal_context(reason="discard pending route handoff")
        try:
            self._set_pressure_controller_vent(True, reason="discard pending route handoff")
        except Exception as exc:
            self.log(f"Pending route handoff vent restore failed: {exc}")
        try:
            self._apply_route_baseline_valves()
        except Exception as exc:
            self.log(f"Pending route handoff baseline restore failed: {exc}")

    def _begin_pending_route_handoff(
        self,
        *,
        current_point: CalibrationPoint,
        current_phase: str,
        current_point_tag: str,
        next_point: CalibrationPoint,
        next_phase: str,
        next_point_tag: str,
        next_open_valves: List[int],
    ) -> bool:
        pace = self.devices.get("pace")
        if not pace or not self._handoff_fast_enabled() or not next_open_valves:
            return False
        completion = dict(self._last_sample_completion or {})
        sample_done_ts = float(completion.get("sample_done_ts") or time.time())
        begin_handoff = getattr(pace, "begin_atmosphere_handoff", None)
        try:
            if callable(begin_handoff):
                begin_handoff()
            else:
                if not self._stop_pressure_controller_atmosphere_hold(pace, reason="before atmosphere handoff"):
                    raise RuntimeError("ATMOSPHERE_HOLD_STOP_FAILED")
                set_output = getattr(pace, "set_output", None)
                if callable(set_output):
                    set_output(False)
                set_isolation_open = getattr(pace, "set_isolation_open", None)
                if callable(set_isolation_open):
                    set_isolation_open(True)
                pace.vent(True)
            self._pressure_atmosphere_hold_enabled = False
            self._pressure_atmosphere_refresh_error_logged = False
            self._last_pressure_atmosphere_refresh_ts = 0.0
        except Exception as exc:
            self.log(f"Route handoff fast-path vent start failed: {exc}")
            self._pending_route_handoff = None
            self._stop_pressure_transition_fast_signal_context(reason="route handoff vent start failed")
            return False

        vent_command_ts = time.time()
        self._start_pressure_transition_fast_signal_context(
            point=current_point,
            phase=current_phase,
            point_tag=current_point_tag,
            reason="after route handoff vent",
            prime_immediately=False,
        )
        sample_to_vent_ms = round((vent_command_ts - sample_done_ts) * 1000.0, 3)
        cached_pace_state = self._last_sample_completion_pace_state(completion)
        self._append_pressure_trace_row(
            point=current_point,
            route=current_phase,
            point_phase=current_phase,
            point_tag=current_point_tag,
            trace_stage="handoff_vent_command_sent",
            pressure_target_hpa=current_point.target_pressure_hpa,
            pace_pressure_hpa=completion.get("pace_pressure_hpa"),
            pressure_gauge_hpa=completion.get("pressure_gauge_hpa"),
            dewpoint_c=completion.get("dewpoint_c"),
            dew_temp_c=completion.get("dew_temp_c"),
            dew_rh_pct=completion.get("dew_rh_pct"),
            pace_output_state=cached_pace_state.get("pace_output_state"),
            pace_isolation_state=cached_pace_state.get("pace_isolation_state"),
            pace_vent_status=cached_pace_state.get("pace_vent_status"),
            refresh_pace_state=False,
            handoff_sample_to_vent_ms=sample_to_vent_ms,
            atmosphere_reference_hpa=self._atmosphere_reference_hpa,
            handoff_safe_open_delta_hpa=self._handoff_safe_open_delta_hpa(),
            deferred_export_queue_len=len(self._deferred_point_exports),
            event_ts=vent_command_ts,
            note=f"next_route={next_phase} next_point={next_point.index}",
        )
        self.log(
            "Route handoff fast-path armed: "
            f"from={current_phase}:{current_point.index} to={next_phase}:{next_point.index} "
            f"sample_to_vent_ms={sample_to_vent_ms:.3f}"
        )
        self._pending_route_handoff = {
            "from_point": current_point,
            "from_phase": current_phase,
            "from_point_tag": current_point_tag,
            "next_point": next_point,
            "next_phase": next_phase,
            "next_point_tag": next_point_tag,
            "next_open_valves": list(next_open_valves),
            "sample_done_ts": sample_done_ts,
            "vent_command_ts": vent_command_ts,
            "sample_completion": completion,
        }
        return True

    def _wait_until_safe_to_open_next_route(self, handoff_state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        pace = self.devices.get("pace")
        next_point = handoff_state.get("next_point")
        next_phase = str(handoff_state.get("next_phase") or "")
        next_point_tag = str(handoff_state.get("next_point_tag") or "")
        if isinstance(next_point, CalibrationPoint):
            self._start_pressure_transition_fast_signal_context(
                point=next_point,
                phase=next_phase,
                point_tag=next_point_tag,
                reason="handoff safe-open wait",
            )
        poll_s = self._pressure_transition_monitor_wait_s(
            next_point if isinstance(next_point, CalibrationPoint) else None
        )
        timeout_s = max(1.0, float(self._wf("workflow.pressure.vent_transition_timeout_s", 30.0) or 30.0))
        start = time.time()
        delta_hpa = self._handoff_safe_open_delta_hpa()
        atmosphere_reference_hpa = self._atmosphere_reference_hpa
        sample_completion = handoff_state.get("sample_completion")
        last_sample_gauge_baseline_hpa = self._as_float(
            sample_completion.get("pressure_gauge_hpa") if isinstance(sample_completion, dict) else None
        )
        baseline_hpa = last_sample_gauge_baseline_hpa
        baseline_source = "last_sample_pressure_gauge"
        if baseline_hpa is None:
            baseline_hpa = atmosphere_reference_hpa
            baseline_source = "atmosphere_reference"

        if not self._handoff_use_pressure_gauge():
            if self._handoff_require_vent_completed() and pace and hasattr(pace, "wait_for_vent_idle"):
                try:
                    pace.wait_for_vent_idle(timeout_s=timeout_s, poll_s=poll_s)
                except Exception as exc:
                    self.log(f"Route handoff wait_for_vent_idle failed: {exc}")
                    return None
            return {
                "safe_open_ts": time.time(),
                "pressure_gauge_hpa": None,
                "atmosphere_reference_hpa": atmosphere_reference_hpa,
                "safe_open_delta_hpa": delta_hpa,
                "safe_open_baseline_hpa": baseline_hpa,
                "safe_open_baseline_source": baseline_source,
            }

        if baseline_hpa is None:
            self.log("Route handoff fast-path unavailable: no safe-open baseline pressure captured")
            if self._handoff_require_vent_completed() and pace and hasattr(pace, "wait_for_vent_idle"):
                try:
                    pace.wait_for_vent_idle(timeout_s=timeout_s, poll_s=poll_s)
                except Exception as exc:
                    self.log(f"Route handoff wait_for_vent_idle failed: {exc}")
                    return None
                return {
                    "safe_open_ts": time.time(),
                    "pressure_gauge_hpa": None,
                    "atmosphere_reference_hpa": atmosphere_reference_hpa,
                    "safe_open_delta_hpa": delta_hpa,
                    "safe_open_baseline_hpa": baseline_hpa,
                    "safe_open_baseline_source": baseline_source,
                }
            return None

        while time.time() - start < timeout_s:
            if self.stop_event.is_set():
                return None
            self._check_pause()
            pressure_now, source = self._read_preseal_pressure_gauge()
            if pressure_now is not None and source == "pressure_gauge":
                pressure_now = float(pressure_now)
                if baseline_source == "last_sample_pressure_gauge":
                    safe_open_reached = abs(float(baseline_hpa) - pressure_now) >= delta_hpa
                else:
                    safe_open_reached = abs(float(baseline_hpa) - pressure_now) <= delta_hpa
                if safe_open_reached:
                    return {
                        "safe_open_ts": time.time(),
                        "pressure_gauge_hpa": pressure_now,
                        "atmosphere_reference_hpa": (
                            float(atmosphere_reference_hpa) if atmosphere_reference_hpa is not None else None
                        ),
                        "safe_open_delta_hpa": float(delta_hpa),
                        "safe_open_baseline_hpa": float(baseline_hpa),
                        "safe_open_baseline_source": baseline_source,
                    }
            time.sleep(max(0.05, poll_s))

        self.log(
            "Route handoff fast-path safe-open timeout: "
            f"baseline_source={baseline_source} baseline_hpa={baseline_hpa} delta_hpa={delta_hpa}"
        )
        if self._handoff_require_vent_completed() and pace and hasattr(pace, "wait_for_vent_idle"):
            try:
                pace.wait_for_vent_idle(timeout_s=timeout_s, poll_s=poll_s)
            except Exception as exc:
                self.log(f"Route handoff wait_for_vent_idle failed after safe-open timeout: {exc}")
                return None
            return {
                "safe_open_ts": time.time(),
                "pressure_gauge_hpa": None,
                "atmosphere_reference_hpa": atmosphere_reference_hpa,
                "safe_open_delta_hpa": delta_hpa,
                "safe_open_baseline_hpa": baseline_hpa,
                "safe_open_baseline_source": baseline_source,
            }
        return None

    def _complete_pending_route_handoff(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        point_tag: str,
        open_valves: List[int],
    ) -> bool:
        handoff_state = dict(self._pending_route_handoff or {})
        if not handoff_state:
            return False
        if str(handoff_state.get("next_phase") or "") != str(phase or ""):
            return False
        expected_point_tag = str(handoff_state.get("next_point_tag") or "")
        if expected_point_tag:
            if expected_point_tag != str(point_tag or ""):
                return False
        elif int(getattr(handoff_state.get("next_point"), "index", -1)) != int(point.index):
            return False

        safe_state = self._wait_until_safe_to_open_next_route(handoff_state)
        if not isinstance(safe_state, dict):
            self._pending_route_handoff = None
            self._stop_pressure_transition_fast_signal_context(reason="handoff safe-open unavailable")
            return False

        sample_done_ts = float(handoff_state.get("sample_done_ts") or time.time())
        vent_command_ts = float(handoff_state.get("vent_command_ts") or sample_done_ts)
        safe_open_ts = float(safe_state.get("safe_open_ts") or time.time())
        sample_to_vent_ms = round((vent_command_ts - sample_done_ts) * 1000.0, 3)
        vent_to_safe_open_ms = round((safe_open_ts - vent_command_ts) * 1000.0, 3)
        cached_pace_state = self._last_sample_completion_pace_state(
            handoff_state.get("sample_completion") if isinstance(handoff_state.get("sample_completion"), dict) else None
        )
        self._append_pressure_trace_row(
            point=point,
            route=phase,
            point_phase=phase,
            point_tag=point_tag,
            trace_stage="handoff_safe_to_open_reached",
            pressure_target_hpa=point.target_pressure_hpa,
            pressure_gauge_hpa=safe_state.get("pressure_gauge_hpa"),
            pace_output_state=cached_pace_state.get("pace_output_state"),
            pace_isolation_state=cached_pace_state.get("pace_isolation_state"),
            pace_vent_status=cached_pace_state.get("pace_vent_status"),
            refresh_pace_state=False,
            handoff_sample_to_vent_ms=sample_to_vent_ms,
            handoff_vent_to_safe_open_ms=vent_to_safe_open_ms,
            handoff_total_ms=round((safe_open_ts - sample_done_ts) * 1000.0, 3),
            atmosphere_reference_hpa=safe_state.get("atmosphere_reference_hpa"),
            handoff_safe_open_delta_hpa=safe_state.get("safe_open_delta_hpa"),
            deferred_export_queue_len=len(self._deferred_point_exports),
            event_ts=safe_open_ts,
            note=(
                "pressure gauge safe-open threshold reached "
                f"baseline_source={safe_state.get('safe_open_baseline_source')} "
                f"baseline_hpa={safe_state.get('safe_open_baseline_hpa')}"
            ),
        )

        open_begin_ts = time.time()
        self._append_pressure_trace_row(
            point=point,
            route=phase,
            point_phase=phase,
            point_tag=point_tag,
            trace_stage="handoff_next_route_open_begin",
            pressure_target_hpa=point.target_pressure_hpa,
            pressure_gauge_hpa=safe_state.get("pressure_gauge_hpa"),
            pace_output_state=cached_pace_state.get("pace_output_state"),
            pace_isolation_state=cached_pace_state.get("pace_isolation_state"),
            pace_vent_status=cached_pace_state.get("pace_vent_status"),
            refresh_pace_state=False,
            handoff_sample_to_vent_ms=sample_to_vent_ms,
            handoff_vent_to_safe_open_ms=vent_to_safe_open_ms,
            atmosphere_reference_hpa=safe_state.get("atmosphere_reference_hpa"),
            handoff_safe_open_delta_hpa=safe_state.get("safe_open_delta_hpa"),
            deferred_export_queue_len=len(self._deferred_point_exports),
            event_ts=open_begin_ts,
            note=f"open_valves={open_valves}",
        )
        self._apply_valve_states(open_valves)
        open_done_ts = time.time()
        safe_open_to_route_open_ms = round((open_done_ts - open_begin_ts) * 1000.0, 3)
        handoff_total_ms = round((open_done_ts - sample_done_ts) * 1000.0, 3)
        self._append_pressure_trace_row(
            point=point,
            route=phase,
            point_phase=phase,
            point_tag=point_tag,
            trace_stage="handoff_next_route_open_done",
            pressure_target_hpa=point.target_pressure_hpa,
            pressure_gauge_hpa=safe_state.get("pressure_gauge_hpa"),
            pace_output_state=cached_pace_state.get("pace_output_state"),
            pace_isolation_state=cached_pace_state.get("pace_isolation_state"),
            pace_vent_status=cached_pace_state.get("pace_vent_status"),
            refresh_pace_state=False,
            handoff_sample_to_vent_ms=sample_to_vent_ms,
            handoff_vent_to_safe_open_ms=vent_to_safe_open_ms,
            handoff_safe_open_to_route_open_ms=safe_open_to_route_open_ms,
            handoff_total_ms=handoff_total_ms,
            atmosphere_reference_hpa=safe_state.get("atmosphere_reference_hpa"),
            handoff_safe_open_delta_hpa=safe_state.get("safe_open_delta_hpa"),
            deferred_export_queue_len=len(self._deferred_point_exports),
            event_ts=open_done_ts,
            note="next route opened via fast handoff",
        )
        self.log(
            "Route handoff fast-path complete: "
            f"route={phase}:{point.index} sample_to_vent_ms={sample_to_vent_ms:.3f} "
            f"vent_to_safe_open_ms={vent_to_safe_open_ms:.3f} "
            f"safe_open_to_route_open_ms={safe_open_to_route_open_ms:.3f} "
            f"handoff_total_ms={handoff_total_ms:.3f}"
        )

        try:
            self._set_pressure_controller_vent(True, reason="maintain atmosphere during next route soak")
        except Exception as exc:
            self.log(f"Route handoff post-open atmosphere-hold restore failed: {exc}")

        if self._flush_deferred_exports_on_next_route_soak_enabled() and (
            self._deferred_sample_exports or self._deferred_point_exports
        ):
            queue_len = len(self._deferred_sample_exports) + len(self._deferred_point_exports)
            self._append_pressure_trace_row(
                point=point,
                route=phase,
                point_phase=phase,
                point_tag=point_tag,
                trace_stage="handoff_deferred_exports_begin",
                pressure_target_hpa=point.target_pressure_hpa,
                handoff_total_ms=handoff_total_ms,
                atmosphere_reference_hpa=safe_state.get("atmosphere_reference_hpa"),
                handoff_safe_open_delta_hpa=safe_state.get("safe_open_delta_hpa"),
                deferred_export_queue_len=queue_len,
                note="flush deferred sample/heavy exports during next-route soak",
            )
            self._flush_deferred_sample_exports(reason=f"next route soak {phase}:{point.index}")
            self._flush_deferred_point_exports(reason=f"next route soak {phase}:{point.index}")
            self._append_pressure_trace_row(
                point=point,
                route=phase,
                point_phase=phase,
                point_tag=point_tag,
                trace_stage="handoff_deferred_exports_end",
                pressure_target_hpa=point.target_pressure_hpa,
                handoff_total_ms=handoff_total_ms,
                atmosphere_reference_hpa=safe_state.get("atmosphere_reference_hpa"),
                handoff_safe_open_delta_hpa=safe_state.get("safe_open_delta_hpa"),
                deferred_export_queue_len=len(self._deferred_sample_exports) + len(self._deferred_point_exports),
                note="deferred sample/heavy exports flush complete",
            )

        self._pending_route_handoff = None
        self._stop_pressure_transition_fast_signal_context(reason="handoff complete")
        return True

    @staticmethod
    def _span(values: List[float]) -> float:
        if len(values) < 2:
            return 0.0
        return max(values) - min(values)

    @staticmethod
    def _to_numeric_for_mean(value: Any) -> Optional[float]:
        if value is None or isinstance(value, bool):
            return None
        try:
            v = float(value)
        except Exception:
            return None
        if not math.isfinite(v):
            return None
        return v

    def _mean_for_key(
        self,
        samples: List[Dict[str, Any]],
        key: str,
        *,
        usable_flag_key: Optional[str] = None,
    ) -> Optional[float]:
        vals = self._numeric_series_for_key(samples, key, usable_flag_key=usable_flag_key)
        if not vals:
            return None
        return mean(vals)

    def _numeric_series_for_key(
        self,
        samples: List[Dict[str, Any]],
        key: str,
        *,
        usable_flag_key: Optional[str] = None,
    ) -> List[float]:
        vals: List[float] = []
        for one in samples:
            if usable_flag_key is not None:
                flag_value = one.get(usable_flag_key)
                if flag_value not in (None, "") and not bool(flag_value):
                    continue
            v = self._to_numeric_for_mean(one.get(key))
            if v is not None:
                vals.append(v)
        return vals

    def _primary_or_first_usable_analyzer_series(
        self,
        samples: List[Dict[str, Any]],
        key: str,
    ) -> List[float]:
        vals = self._numeric_series_for_key(samples, key, usable_flag_key="frame_usable")
        if vals:
            return vals

        for label, _, _ in self._all_gas_analyzers():
            prefix = self._safe_label(label)
            vals = self._numeric_series_for_key(
                samples,
                f"{prefix}_{key}",
                usable_flag_key=f"{prefix}_frame_usable",
            )
            if vals:
                return vals
        return []

    def _fleet_analyzer_point_stats(
        self,
        samples: List[Dict[str, Any]],
        key: str,
    ) -> tuple[Optional[float], Optional[float]]:
        analyzer_point_means: List[float] = []
        prefixes: List[str] = []
        seen_prefixes: set[str] = set()

        for label, _, _ in self._all_gas_analyzers():
            prefix = self._safe_label(label)
            if prefix not in seen_prefixes:
                prefixes.append(prefix)
                seen_prefixes.add(prefix)

        for sample in samples:
            for sample_key in sample.keys():
                match = re.match(r"^(ga\d+)_", str(sample_key))
                if not match:
                    continue
                prefix = match.group(1)
                if prefix in seen_prefixes:
                    continue
                prefixes.append(prefix)
                seen_prefixes.add(prefix)

        for prefix in prefixes:
            vals = self._numeric_series_for_key(
                samples,
                f"{prefix}_{key}",
                usable_flag_key=f"{prefix}_frame_usable",
            )
            if vals:
                analyzer_point_means.append(mean(vals))

        if not analyzer_point_means:
            legacy_vals = self._numeric_series_for_key(samples, key, usable_flag_key="frame_usable")
            if legacy_vals:
                analyzer_point_means.append(mean(legacy_vals))

        if not analyzer_point_means:
            return None, None
        fleet_mean = mean(analyzer_point_means)
        fleet_std = stdev(analyzer_point_means) if len(analyzer_point_means) > 1 else None
        return fleet_mean, fleet_std

    def _build_gas_field_means(self, samples: List[Dict[str, Any]]) -> Dict[str, float]:
        out: Dict[str, float] = {}
        mode2_keys = self._mode2_sample_fields()

        # Primary analyzer legacy (unprefixed) fields.
        for key in mode2_keys:
            m = self._mean_for_key(samples, key, usable_flag_key="frame_usable")
            if m is not None:
                out[f"{key}_mean"] = m

        # Multi-analyzer prefixed fields.
        for label, _, _ in self._gas_analyzers():
            prefix = self._safe_label(label)
            for key in mode2_keys:
                full_key = f"{prefix}_{key}"
                m = self._mean_for_key(samples, full_key, usable_flag_key=f"{prefix}_frame_usable")
                if m is not None:
                    out[f"{full_key}_mean"] = m
        return out

    def _exclude_generic_sample_mean_key(self, key: str) -> bool:
        text = str(key or "")
        if not text:
            return True
        if text.endswith("_error"):
            return True
        if text in {
            "point_title",
            "sample_ts",
            "point_phase",
            "point_tag",
            "point_row",
            "point_is_h2o",
            "raw",
            "hgen_raw",
            "dewpoint_sample_ts",
            "co2_ppm_target",
            "h2o_mmol_target",
            "pressure_target_hpa",
            "temp_set_c",
            "temp_chamber_c",
            "hgen_temp_c",
            "hgen_rh_pct",
            "co2_ppm",
            "h2o_mmol",
            "pressure_hpa",
        }:
            return True
        if re.match(r"^ga\d+_", text):
            return True
        if text in self._mode2_sample_fields():
            return True
        return False

    def _build_device_field_means(
        self,
        samples: List[Dict[str, Any]],
        *,
        existing_summary_keys: Optional[set[str]] = None,
    ) -> Dict[str, float]:
        out: Dict[str, float] = {}
        existing = set(existing_summary_keys or set())
        keys = {str(key) for sample in samples for key in sample.keys()}
        for key in sorted(keys):
            if self._exclude_generic_sample_mean_key(key):
                continue
            mean_key = f"{key}_mean"
            if mean_key in existing:
                continue
            m = self._mean_for_key(samples, key)
            if m is not None:
                out[mean_key] = m
        return out

    def _evaluate_sample_quality(self, samples: List[Dict[str, Any]]) -> tuple[bool, Dict[str, float]]:
        qcfg = self.cfg.get("workflow", {}).get("sampling", {}).get("quality", {})
        if not qcfg or not qcfg.get("enabled", False):
            return True, {}

        per_analyzer = bool(qcfg.get("per_analyzer", False))
        limits = {
            "co2_ppm": qcfg.get("max_span_co2_ppm"),
            "h2o_mmol": qcfg.get("max_span_h2o_mmol"),
            "pressure_hpa": qcfg.get("max_span_pressure_hpa"),
            "dewpoint_c": qcfg.get("max_span_dewpoint_c"),
        }

        spans: Dict[str, float] = {}
        ok = True
        for key, raw_limit in limits.items():
            if raw_limit is None:
                continue

            if key in {"co2_ppm", "h2o_mmol"}:
                vals = self._primary_or_first_usable_analyzer_series(samples, key)
            else:
                vals = self._numeric_series_for_key(samples, key)
            if not vals:
                continue

            span = self._span(vals)
            spans[key] = span
            if span > float(raw_limit):
                ok = False

            if not per_analyzer or key not in {"co2_ppm", "h2o_mmol"}:
                continue

            prefixes = {self._safe_label(label) for label, _ga, _cfg in self._all_gas_analyzers()}
            for sample in samples:
                for sample_key in sample.keys():
                    match = re.match(r"^(ga\d+)_", str(sample_key))
                    if match:
                        prefixes.add(match.group(1))

            for prefix in sorted(prefixes):
                analyzer_vals = self._numeric_series_for_key(
                    samples,
                    f"{prefix}_{key}",
                    usable_flag_key=f"{prefix}_frame_usable",
                )
                if not analyzer_vals:
                    continue
                analyzer_span = self._span(analyzer_vals)
                spans[f"{prefix}.{key}"] = analyzer_span
                if analyzer_span > float(raw_limit):
                    ok = False
        return ok, spans

    def _summarize_analyzer_integrity(
        self,
        samples: List[Dict[str, Any]],
        *,
        analyzer_labels: List[str],
    ) -> Dict[str, Any]:
        expected = len(analyzer_labels)
        with_frame: List[str] = []
        usable: List[str] = []
        missing: List[str] = []
        unusable: List[str] = []

        for label in analyzer_labels:
            prefix = self._safe_label(label)
            has_frame = any(bool(sample.get(f"{prefix}_frame_has_data")) for sample in samples)
            has_usable = any(bool(sample.get(f"{prefix}_frame_usable")) for sample in samples)
            display = str(label or "").upper()
            if has_frame:
                with_frame.append(display)
            else:
                missing.append(display)
            if has_usable:
                usable.append(display)
            elif has_frame:
                unusable.append(display)

        usable_count = len(usable)
        with_frame_count = len(with_frame)
        coverage_text = f"{usable_count}/{expected}" if expected > 0 else "0/0"
        if expected == 0:
            integrity = "无分析仪"
        elif usable_count == expected:
            integrity = "完整"
        elif usable_count == 0 and with_frame_count == 0:
            integrity = "无帧"
        elif usable_count == 0:
            integrity = "仅异常帧"
        elif missing and unusable:
            integrity = "部分缺失且含异常帧"
        elif missing:
            integrity = "部分缺失"
        elif unusable:
            integrity = "含异常帧"
        else:
            integrity = "部分可用"

        return {
            "analyzer_expected_count": expected,
            "analyzer_with_frame_count": with_frame_count,
            "analyzer_usable_count": usable_count,
            "analyzer_coverage_text": coverage_text,
            "analyzer_integrity": integrity,
            "analyzer_missing_labels": ",".join(missing),
            "analyzer_unusable_labels": ",".join(unusable),
        }

    def _collect_samples(
        self,
        point: CalibrationPoint,
        count: int,
        interval: float,
        phase: str = "",
        point_tag: str = "",
    ) -> Optional[List[Dict[str, Any]]]:
        gas_analyzers = self._all_gas_analyzers()
        pace = self.devices.get("pace")

        samples: List[Dict[str, Any]] = []
        frame_issue_counts: Dict[str, int] = {}
        phase_text = str(phase or ("h2o" if point.is_h2o_point else "co2")).strip().lower()
        active_sampling_context = self._sampling_window_context
        sampling_context = active_sampling_context
        inline_context = False
        if sampling_context is None:
            sampling_context = self._new_sampling_window_context(point=point, phase=phase_text, point_tag=point_tag)
            sampling_context["worker_plan"] = self._sampling_window_worker_plan()
            self._prime_sampling_window_context(
                sampling_context,
                worker_plan=sampling_context["worker_plan"],
                reason="inline collect_samples",
            )
            inline_context = True

        fixed_rate_enabled = self._sampling_fixed_rate_enabled()
        fast_sync_warn_span_ms = self._sampling_fast_sync_warn_span_ms()
        start_monotonic = time.monotonic()
        start_wall = datetime.now()
        next_inline_slow_aux_refresh = start_monotonic + self._sampling_slow_aux_cache_interval_s()
        first_valid_offsets_ms: Dict[str, Optional[float]] = {
            "pace": None,
            "pressure_gauge": None,
            "dewpoint": None,
            "analyzer": None,
        }
        first_effective_recorded = False
        first_effective_row: Optional[int] = None
        late_rebound_evaluated = False
        try:
            for sample_idx in range(count):
                if self.stop_event.is_set():
                    return None
                if fixed_rate_enabled:
                    due_monotonic = start_monotonic + (sample_idx * interval)
                    due_wall = start_wall + timedelta(seconds=(sample_idx * interval))
                    while True:
                        if self.stop_event.is_set():
                            return None
                        if not self.pause_event.is_set():
                            self._check_pause()
                            continue
                        remain = due_monotonic - time.monotonic()
                        if remain <= 0:
                            break
                        time.sleep(remain)
                        break
                else:
                    due_monotonic = time.monotonic()
                    due_wall = datetime.now()
                    self._check_pause()

                if self.stop_event.is_set():
                    return None
                self._check_pause()

                row_start_monotonic = time.monotonic()
                row_start_dt = datetime.now()
                row_time_s = time.time()
                sample_anchor_mono = row_start_monotonic
                sample_lag_ms = round(max(0.0, (row_start_monotonic - due_monotonic) * 1000.0), 3)
                row_offset_ms = round(max(0.0, (row_start_monotonic - start_monotonic) * 1000.0), 3)

                if (
                    inline_context
                    and self._sampling_slow_aux_cache_enabled()
                    and sample_idx > 0
                    and row_start_monotonic >= next_inline_slow_aux_refresh
                ):
                    self._refresh_slow_aux_cache_once(sampling_context, reason="inline collect_samples refresh")
                    next_inline_slow_aux_refresh = row_start_monotonic + self._sampling_slow_aux_cache_interval_s()

                data: Dict[str, Any] = {
                    "point_title": self._point_title(point, phase=phase_text, point_tag=point_tag),
                    "sample_index": sample_idx + 1,
                    "sample_ts": self._ts_from_datetime(row_start_dt),
                    "sample_due_ts": self._ts_from_datetime(due_wall),
                    "sample_start_ts": self._ts_from_datetime(row_start_dt),
                    "sample_lag_ms": sample_lag_ms,
                    "point_phase": phase_text,
                    "point_tag": point_tag,
                    "point_row": point.index,
                    "trace_stage": "sampling_row",
                    "route": phase_text,
                    "pressure_mode": self._pressure_mode_for_point(point),
                    "pressure_target_label": self._pressure_target_label(point),
                }

                fast_group_start_dt = datetime.now()
                fast_group_start_monotonic = time.monotonic()
                data["fast_group_anchor_ts"] = self._ts_from_datetime(row_start_dt)
                data["fast_group_start_ts"] = self._ts_from_datetime(fast_group_start_dt)
                self._merge_fast_signal_cache_into_sample(
                    data,
                    sampling_context,
                    sample_anchor_mono=sample_anchor_mono,
                    row_time_s=row_time_s,
                )
                if pace:
                    pace_state = self._sampling_row_pace_state_snapshot(pace, sample_idx=sample_idx)
                    data.update(pace_state)
                fast_group_end_dt = datetime.now()
                fast_group_end_monotonic = time.monotonic()
                fast_group_span_ms = round((fast_group_end_monotonic - fast_group_start_monotonic) * 1000.0, 3)
                data["fast_group_end_ts"] = self._ts_from_datetime(fast_group_end_dt)
                data["fast_group_span_ms"] = fast_group_span_ms
                if sample_idx == 0:
                    self._append_pressure_trace_row(
                        point=point,
                        route=phase_text,
                        point_phase=phase_text,
                        point_tag=point_tag,
                        trace_stage="first_sample_begin",
                        pressure_target_hpa=point.target_pressure_hpa,
                        pace_pressure_hpa=self._as_float(data.get("pressure_hpa")),
                        pressure_gauge_hpa=self._as_float(data.get("pressure_gauge_hpa")),
                        dewpoint_c=self._as_float(data.get("dewpoint_live_c")),
                        dew_temp_c=self._as_float(data.get("dew_temp_live_c")),
                        dew_rh_pct=self._as_float(data.get("dew_rh_live_pct")),
                        pace_output_state=data.get("pace_output_state"),
                        pace_isolation_state=data.get("pace_isolation_state"),
                        pace_vent_status=data.get("pace_vent_status"),
                        dewpoint_live_c=self._as_float(data.get("dewpoint_live_c")),
                        dew_temp_live_c=self._as_float(data.get("dew_temp_live_c")),
                        dew_rh_live_pct=self._as_float(data.get("dew_rh_live_pct")),
                        fast_group_span_ms=fast_group_span_ms,
                        sample_lag_ms=sample_lag_ms,
                        refresh_pace_state=False,
                        event_ts=row_start_dt.timestamp(),
                        note=f"sample_index=1/{count}",
                    )
                if fast_sync_warn_span_ms > 0 and fast_group_span_ms > fast_sync_warn_span_ms:
                    self.log(
                        f"Sampling fast-group span warning: point={point.index} phase={phase_text} "
                        f"sample={sample_idx + 1}/{count} span_ms={fast_group_span_ms:.3f}"
                    )

                row_frame_issues = self._merge_analyzer_cache_into_sample(
                    data,
                    gas_analyzers,
                    context=sampling_context,
                    sample_anchor_mono=sample_anchor_mono,
                    row_time_s=row_time_s,
                )
                for key, value in row_frame_issues.items():
                    frame_issue_counts[key] = frame_issue_counts.get(key, 0) + value

                self._merge_slow_aux_cache_into_sample(
                    data,
                    sampling_context,
                    row_time_s=row_time_s,
                )

                snapshot = self._preseal_dewpoint_snapshot if phase_text == "h2o" else None
                if snapshot:
                    data["dewpoint_c"] = snapshot.get("dewpoint_c")
                    data["dew_temp_c"] = snapshot.get("temp_c")
                    data["dew_rh_pct"] = snapshot.get("rh_pct")
                    if snapshot.get("pressure_hpa") is not None:
                        data["dew_pressure_hpa"] = snapshot.get("pressure_hpa")
                    if snapshot.get("sample_ts"):
                        data["dewpoint_sample_ts"] = snapshot.get("sample_ts")
                else:
                    if data.get("dewpoint_live_sample_ts"):
                        data["dewpoint_sample_ts"] = data.get("dewpoint_live_sample_ts")
                    if "dewpoint_live_c" in data:
                        data["dewpoint_c"] = data.get("dewpoint_live_c")
                        data["dew_temp_c"] = data.get("dew_temp_live_c")
                        data["dew_rh_pct"] = data.get("dew_rh_live_pct")
                    if (
                        phase_text == "co2"
                        and isinstance(self._preseal_dewpoint_snapshot, dict)
                        and self._preseal_dewpoint_snapshot.get("pressure_hpa") is not None
                    ):
                        data["dew_pressure_hpa"] = self._preseal_dewpoint_snapshot.get("pressure_hpa")

                data["co2_ppm_target"] = point.co2_ppm
                data["h2o_mmol_target"] = point.h2o_mmol
                data["pressure_target_hpa"] = point.target_pressure_hpa
                data["temp_set_c"] = point.temp_chamber_c
                data["point_is_h2o"] = point.is_h2o_point
                analyzer_value_present = bool(
                    self._as_float(data.get("co2_ppm")) is not None
                    or self._as_float(data.get("h2o_mmol")) is not None
                )
                if first_valid_offsets_ms["pace"] is None and self._as_float(data.get("pressure_hpa")) is not None:
                    first_valid_offsets_ms["pace"] = row_offset_ms
                if (
                    first_valid_offsets_ms["pressure_gauge"] is None
                    and self._as_float(data.get("pressure_gauge_hpa")) is not None
                ):
                    first_valid_offsets_ms["pressure_gauge"] = row_offset_ms
                if first_valid_offsets_ms["dewpoint"] is None and self._as_float(data.get("dewpoint_live_c")) is not None:
                    first_valid_offsets_ms["dewpoint"] = row_offset_ms
                if first_valid_offsets_ms["analyzer"] is None and analyzer_value_present:
                    first_valid_offsets_ms["analyzer"] = row_offset_ms

                if not first_effective_recorded:
                    missing_effective: List[str] = []
                    if self.devices.get("pace") is not None and self._as_float(data.get("pressure_hpa")) is None:
                        missing_effective.append("pace")
                    if (
                        self.devices.get("pressure_gauge") is not None
                        and self._as_float(data.get("pressure_gauge_hpa")) is None
                    ):
                        missing_effective.append("pressure_gauge")
                    if self.devices.get("dewpoint") is not None and self._as_float(data.get("dewpoint_live_c")) is None:
                        missing_effective.append("dewpoint")
                    if gas_analyzers and not analyzer_value_present:
                        missing_effective.append("analyzer")
                    if not missing_effective:
                        effective_note = (
                            f"sample_index={sample_idx + 1}/{count} "
                            f"pace_first_valid_ms={first_valid_offsets_ms['pace']} "
                            f"pressure_gauge_first_valid_ms={first_valid_offsets_ms['pressure_gauge']} "
                            f"dewpoint_first_valid_ms={first_valid_offsets_ms['dewpoint']} "
                            f"analyzer_first_valid_ms={first_valid_offsets_ms['analyzer']}"
                        )
                        self._append_pressure_trace_row(
                            point=point,
                            route=phase_text,
                            point_phase=phase_text,
                            point_tag=point_tag,
                            trace_stage="first_effective_sample",
                            pressure_target_hpa=point.target_pressure_hpa,
                            pace_pressure_hpa=self._as_float(data.get("pressure_hpa")),
                            pressure_gauge_hpa=self._as_float(data.get("pressure_gauge_hpa")),
                            dewpoint_c=self._as_float(data.get("dewpoint_c")),
                            dew_temp_c=self._as_float(data.get("dew_temp_c")),
                            dew_rh_pct=self._as_float(data.get("dew_rh_pct")),
                            pace_output_state=data.get("pace_output_state"),
                            pace_isolation_state=data.get("pace_isolation_state"),
                            pace_vent_status=data.get("pace_vent_status"),
                            refresh_pace_state=False,
                            fast_group_span_ms=fast_group_span_ms,
                            dewpoint_live_c=self._as_float(data.get("dewpoint_live_c")),
                            dew_temp_live_c=self._as_float(data.get("dew_temp_live_c")),
                            dew_rh_live_pct=self._as_float(data.get("dew_rh_live_pct")),
                            sample_lag_ms=sample_lag_ms,
                            event_ts=row_start_dt.timestamp(),
                            note=effective_note,
                        )
                        first_effective_recorded = True
                        first_effective_row = sample_idx + 1
                        if self._is_co2_low_pressure_sealed_point(point):
                            self._evaluate_co2_postsample_late_rebound(
                                point,
                                phase=phase_text,
                                first_effective_sample_dewpoint_c=self._as_float(data.get("dewpoint_live_c")),
                            )
                            late_rebound_evaluated = True
                row_end_dt = datetime.now()
                row_end_monotonic = time.monotonic()
                data["sample_end_ts"] = self._ts_from_datetime(row_end_dt)
                data["sample_elapsed_ms"] = round((row_end_monotonic - row_start_monotonic) * 1000.0, 3)
                self._append_pressure_trace_row(
                    point=point,
                    route=phase_text,
                    point_phase=phase_text,
                    point_tag=point_tag,
                    trace_stage="sampling_row",
                    pressure_target_hpa=point.target_pressure_hpa,
                    pace_pressure_hpa=self._as_float(data.get("pressure_hpa")),
                    pressure_gauge_hpa=self._as_float(data.get("pressure_gauge_hpa")),
                    dewpoint_c=self._as_float(data.get("dewpoint_c")),
                    dew_temp_c=self._as_float(data.get("dew_temp_c")),
                    dew_rh_pct=self._as_float(data.get("dew_rh_pct")),
                    pace_output_state=data.get("pace_output_state"),
                    pace_isolation_state=data.get("pace_isolation_state"),
                    pace_vent_status=data.get("pace_vent_status"),
                    refresh_pace_state=False,
                    fast_group_span_ms=fast_group_span_ms,
                    dewpoint_live_c=self._as_float(data.get("dewpoint_live_c")),
                    dew_temp_live_c=self._as_float(data.get("dew_temp_live_c")),
                    dew_rh_live_pct=self._as_float(data.get("dew_rh_live_pct")),
                    sample_lag_ms=sample_lag_ms,
                    note=f"sample_index={sample_idx + 1}/{count}",
                )

                samples.append(data)
                self._emit_sample_progress_event(
                    sample_idx + 1,
                    count,
                    point=point,
                    phase=phase_text,
                    point_tag=point_tag,
                )
            if frame_issue_counts:
                joined = ", ".join(f"{reason}={count}" for reason, count in sorted(frame_issue_counts.items()))
                self.log(
                    f"Analyzer unusable-frame summary [{phase_text}] point={point.index} "
                    f"samples={len(samples)} {joined}"
                )
            if self._is_co2_low_pressure_sealed_point(point) and not late_rebound_evaluated:
                self._evaluate_co2_postsample_late_rebound(
                    point,
                    phase=phase_text,
                    first_effective_sample_dewpoint_c=None,
                )
            if samples and not first_effective_recorded:
                last_sample = samples[-1]
                missing_effective: List[str] = []
                if self.devices.get("pace") is not None and first_valid_offsets_ms["pace"] is None:
                    missing_effective.append("pace")
                if self.devices.get("pressure_gauge") is not None and first_valid_offsets_ms["pressure_gauge"] is None:
                    missing_effective.append("pressure_gauge")
                if self.devices.get("dewpoint") is not None and first_valid_offsets_ms["dewpoint"] is None:
                    missing_effective.append("dewpoint")
                if gas_analyzers and first_valid_offsets_ms["analyzer"] is None:
                    missing_effective.append("analyzer")
                self._append_pressure_trace_row(
                    point=point,
                    route=phase_text,
                    point_phase=phase_text,
                    point_tag=point_tag,
                    trace_stage="first_effective_sample",
                    pressure_target_hpa=point.target_pressure_hpa,
                    pace_pressure_hpa=self._as_float(last_sample.get("pressure_hpa")),
                    pressure_gauge_hpa=self._as_float(last_sample.get("pressure_gauge_hpa")),
                    dewpoint_c=self._as_float(last_sample.get("dewpoint_c")),
                    dew_temp_c=self._as_float(last_sample.get("dew_temp_c")),
                    dew_rh_pct=self._as_float(last_sample.get("dew_rh_pct")),
                    pace_output_state=last_sample.get("pace_output_state"),
                    pace_isolation_state=last_sample.get("pace_isolation_state"),
                    pace_vent_status=last_sample.get("pace_vent_status"),
                    refresh_pace_state=False,
                    fast_group_span_ms=self._as_float(last_sample.get("fast_group_span_ms")),
                    dewpoint_live_c=self._as_float(last_sample.get("dewpoint_live_c")),
                    dew_temp_live_c=self._as_float(last_sample.get("dew_temp_live_c")),
                    dew_rh_live_pct=self._as_float(last_sample.get("dew_rh_live_pct")),
                    sample_lag_ms=self._as_float(last_sample.get("sample_lag_ms")),
                    event_ts=self._sample_row_wall_ts(last_sample, key="sample_start_ts") or time.time(),
                    note=(
                        f"missing={','.join(missing_effective) if missing_effective else '--'} "
                        f"pace_first_valid_ms={first_valid_offsets_ms['pace']} "
                        f"pressure_gauge_first_valid_ms={first_valid_offsets_ms['pressure_gauge']} "
                        f"dewpoint_first_valid_ms={first_valid_offsets_ms['dewpoint']} "
                        f"analyzer_first_valid_ms={first_valid_offsets_ms['analyzer']}"
                    ),
                )
            self._set_point_runtime_fields(
                point,
                phase=phase_text,
                first_valid_pace_ms=first_valid_offsets_ms["pace"],
                first_valid_pressure_gauge_ms=first_valid_offsets_ms["pressure_gauge"],
                first_valid_dewpoint_ms=first_valid_offsets_ms["dewpoint"],
                first_valid_analyzer_ms=first_valid_offsets_ms["analyzer"],
                effective_sample_started_on_row=first_effective_row,
            )
            return samples
        finally:
            if inline_context:
                stop_event = sampling_context.get("stop_event")
                if isinstance(stop_event, threading.Event):
                    stop_event.set()

    def _sampling_params(self, phase: str = "") -> Tuple[int, float]:
        scfg = self.cfg["workflow"]["sampling"]
        count = int(scfg.get("stable_count", scfg.get("count", 10)))
        count = max(1, count)
        interval = float(scfg["interval_s"])
        if phase == "co2":
            interval = float(scfg.get("co2_interval_s", interval))
        elif phase == "h2o":
            interval = float(scfg.get("h2o_interval_s", interval))
        return count, interval

    def _finalize_sampling_prime_metrics_after_collection(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        metrics: Dict[str, Any],
        samples: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        finalized = dict(metrics or {})
        status = str(finalized.get("status") or "skipped")
        if status != "timeout":
            return finalized
        state = self._point_runtime_state(point, phase=phase, create=False) or {}
        effective_row = self._as_int(state.get("effective_sample_started_on_row"))
        if effective_row is None or effective_row < 1 or effective_row > len(samples):
            return finalized
        effective_sample = samples[effective_row - 1]
        finalized["status"] = "ready_after_start"
        finalized["original_missing"] = list(finalized.get("missing") or [])
        finalized["missing"] = []
        finalized["effective_row"] = effective_row
        finalized["event_ts"] = (
            self._sample_row_wall_ts(effective_sample, key="sample_start_ts")
            or self._sample_row_wall_ts(effective_sample)
        )
        finalized["ready_values"] = {
            "pace_pressure_hpa": self._as_float(effective_sample.get("pressure_hpa")),
            "pressure_gauge_hpa": self._as_float(effective_sample.get("pressure_gauge_hpa")),
            "dewpoint_c": self._as_float(effective_sample.get("dewpoint_c")),
            "dew_temp_c": self._as_float(effective_sample.get("dew_temp_c")),
            "dew_rh_pct": self._as_float(effective_sample.get("dew_rh_pct")),
            "dewpoint_live_c": self._as_float(effective_sample.get("dewpoint_live_c")),
            "dew_temp_live_c": self._as_float(effective_sample.get("dew_temp_live_c")),
            "dew_rh_live_pct": self._as_float(effective_sample.get("dew_rh_live_pct")),
        }
        finalized["first_valid_pace_ms"] = self._as_float(state.get("first_valid_pace_ms"))
        finalized["first_valid_pressure_gauge_ms"] = self._as_float(state.get("first_valid_pressure_gauge_ms"))
        finalized["first_valid_dewpoint_ms"] = self._as_float(state.get("first_valid_dewpoint_ms"))
        finalized["first_valid_analyzer_ms"] = self._as_float(state.get("first_valid_analyzer_ms"))
        return finalized

    def _append_sampling_prime_ready_trace(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        point_tag: str,
        metrics: Dict[str, Any],
    ) -> None:
        status = str(metrics.get("status") or "skipped")
        ready_values = dict(metrics.get("ready_values") or {})
        note = f"status={status} elapsed_s={float(metrics.get('elapsed_s') or 0.0):.3f}"
        if status == "ready_after_start":
            original_missing = list(metrics.get("original_missing") or metrics.get("missing") or [])
            if original_missing:
                note += f" originally_missing={','.join(original_missing)}"
            effective_row = self._as_int(metrics.get("effective_row"))
            if effective_row is not None:
                note += f" effective_row={effective_row}"
            for field_name, label in (
                ("first_valid_pace_ms", "pace_first_valid_ms"),
                ("first_valid_pressure_gauge_ms", "pressure_gauge_first_valid_ms"),
                ("first_valid_dewpoint_ms", "dewpoint_first_valid_ms"),
                ("first_valid_analyzer_ms", "analyzer_first_valid_ms"),
            ):
                value = metrics.get(field_name)
                if value is not None:
                    note += f" {label}={value}"
        else:
            missing = list(metrics.get("missing") or [])
            if missing:
                note += f" missing={','.join(missing)}"
        self._append_pressure_trace_row(
            point=point,
            route=phase,
            point_phase=phase,
            point_tag=point_tag,
            trace_stage="sampling_prime_ready",
            pressure_target_hpa=point.target_pressure_hpa,
            pace_pressure_hpa=ready_values.get("pace_pressure_hpa"),
            pressure_gauge_hpa=ready_values.get("pressure_gauge_hpa"),
            dewpoint_c=ready_values.get("dewpoint_c"),
            dew_temp_c=ready_values.get("dew_temp_c"),
            dew_rh_pct=ready_values.get("dew_rh_pct"),
            dewpoint_live_c=ready_values.get("dewpoint_live_c"),
            dew_temp_live_c=ready_values.get("dew_temp_live_c"),
            dew_rh_live_pct=ready_values.get("dew_rh_live_pct"),
            refresh_pace_state=False,
            event_ts=self._as_float(metrics.get("event_ts")),
            note=note,
        )

    def _sample_and_log(self, point: CalibrationPoint, phase: str = "", point_tag: str = "") -> None:
        count, interval = self._sampling_params(phase=phase)
        scfg = self.cfg["workflow"]["sampling"]
        qcfg = self.cfg.get("workflow", {}).get("sampling", {}).get("quality", {})
        retries = int(qcfg.get("retries", 0) or 0) if qcfg.get("enabled", False) else 0

        phase_text = str(phase or ("h2o" if point.is_h2o_point else "co2")).strip().lower()
        self._emit_stage_event(
            current=self._stage_label_for_point(point, phase=phase_text),
            point=point,
            phase=phase_text,
            point_tag=point_tag,
            wait_reason="采样中",
        )
        self._emit_sample_progress_event(
            0,
            count,
            point=point,
            phase=phase_text,
            point_tag=point_tag,
        )
        samples: List[Dict[str, Any]] = []
        previous_sampling_context = self._sampling_window_context
        self._append_pressure_trace_row(
            point=point,
            route=phase_text,
            point_phase=phase_text,
            point_tag=point_tag,
            trace_stage="sampling_collection_begin",
            pressure_target_hpa=point.target_pressure_hpa,
            refresh_pace_state=False,
            note=f"count={count} interval_s={interval:g}",
        )
        self._append_pressure_trace_row(
            point=point,
            route=phase_text,
            point_phase=phase_text,
            point_tag=point_tag,
            trace_stage="sampling_prime_begin",
            pressure_target_hpa=point.target_pressure_hpa,
            refresh_pace_state=False,
            note=f"count={count} interval_s={interval:g}",
        )
        sampling_context = self._start_sampling_window_context(
            point=point,
            phase=phase_text,
            point_tag=point_tag,
        )
        self._sampling_window_context = sampling_context
        soft_cfg = self._pressure_soft_control_trace_context()
        self.log(
            "Sampling window config: "
            f"phase={phase_text} count={count} interval_s={interval:g} "
            f"fixed_rate_enabled={self._sampling_fixed_rate_enabled()} "
            f"slow_aux_cache_enabled={self._sampling_slow_aux_cache_enabled()} "
            f"slow_aux_cache_interval_s={self._sampling_slow_aux_cache_interval_s():g} "
            f"pace_state_every_n_samples={self._sampling_pace_state_every_n_samples()} "
            f"pace_state_cache_enabled={self._sampling_pace_state_cache_enabled()} "
            f"pace_state_strategy={self._sampling_pace_state_strategy_text()} "
            f"soft_control_enabled={soft_cfg['soft_control_enabled']} "
            f"soft_control_linear_slew_hpa_per_s={soft_cfg['soft_control_linear_slew_hpa_per_s']}"
        )
        prime_metrics = self._wait_for_sampling_freshness_gate(
            point=point,
            phase=phase_text,
            point_tag=point_tag,
            context=sampling_context,
        )
        prime_trace_deferred = str(prime_metrics.get("status") or "skipped") == "timeout"
        prime_trace_written = False
        if not prime_trace_deferred:
            self._append_sampling_prime_ready_trace(
                point,
                phase=phase_text,
                point_tag=point_tag,
                metrics=prime_metrics,
            )
            prime_trace_written = True
        try:
            try:
                for attempt in range(retries + 1):
                    collected = self._collect_samples(point, count, interval, phase=phase, point_tag=point_tag)
                    if collected is None:
                        if prime_trace_deferred and not prime_trace_written:
                            self._append_sampling_prime_ready_trace(
                                point,
                                phase=phase_text,
                                point_tag=point_tag,
                                metrics=prime_metrics,
                            )
                            prime_trace_written = True
                        return

                    ok, spans = self._evaluate_sample_quality(collected)
                    samples = collected
                    if ok:
                        break

                    if attempt < retries:
                        self.log(f"Sample quality not met, retry {attempt + 1}/{retries}: spans={spans}")
                    else:
                        self.log(f"Sample quality not met, using last batch: spans={spans}")
            except Exception:
                if prime_trace_deferred and not prime_trace_written:
                    self._append_sampling_prime_ready_trace(
                        point,
                        phase=phase_text,
                        point_tag=point_tag,
                        metrics=prime_metrics,
                    )
                    prime_trace_written = True
                raise
        finally:
            self._sampling_window_context = previous_sampling_context
            self._stop_sampling_window_context(sampling_context)

        if prime_trace_deferred and not prime_trace_written:
            self._append_sampling_prime_ready_trace(
                point,
                phase=phase_text,
                point_tag=point_tag,
                metrics=self._finalize_sampling_prime_metrics_after_collection(
                    point,
                    phase=phase_text,
                    metrics=prime_metrics,
                    samples=samples,
                ),
            )

        last_sample = samples[-1] if samples else {}
        sample_done_ts = self._sample_row_wall_ts(last_sample) or time.time()
        self._append_pressure_trace_row(
            point=point,
            route=phase_text,
            point_phase=phase_text,
            point_tag=point_tag,
            trace_stage="sampling_end",
            pressure_target_hpa=point.target_pressure_hpa,
            pace_pressure_hpa=self._as_float(last_sample.get("pressure_hpa")),
            pressure_gauge_hpa=self._as_float(last_sample.get("pressure_gauge_hpa")),
            dewpoint_c=self._as_float(last_sample.get("dewpoint_c")),
            dew_temp_c=self._as_float(last_sample.get("dew_temp_c")),
            dew_rh_pct=self._as_float(last_sample.get("dew_rh_pct")),
            pace_output_state=self._as_int(last_sample.get("pace_output_state")),
            pace_isolation_state=self._as_int(last_sample.get("pace_isolation_state")),
            pace_vent_status=self._as_int(last_sample.get("pace_vent_status")),
            refresh_pace_state=False,
            fast_group_span_ms=self._as_float(last_sample.get("fast_group_span_ms")),
            dewpoint_live_c=self._as_float(last_sample.get("dewpoint_live_c")),
            dew_temp_live_c=self._as_float(last_sample.get("dew_temp_live_c")),
            dew_rh_live_pct=self._as_float(last_sample.get("dew_rh_live_pct")),
            sample_lag_ms=self._as_float(last_sample.get("sample_lag_ms")),
            event_ts=sample_done_ts,
            note=f"samples={len(samples)}",
        )

        self._record_last_sample_completion(
            point,
            phase=phase_text,
            point_tag=point_tag,
            sample_done_ts=sample_done_ts,
            last_sample=last_sample,
        )
        self._write_point_timing_summary(
            point,
            phase=phase_text,
            point_tag=point_tag,
        )
        self._append_pressure_trace_row(
            point=point,
            route=phase_text,
            point_phase=phase_text,
            point_tag=point_tag,
            trace_stage="handoff_last_sample_done",
            pressure_target_hpa=point.target_pressure_hpa,
            pace_pressure_hpa=self._as_float(last_sample.get("pressure_hpa")),
            pressure_gauge_hpa=self._as_float(last_sample.get("pressure_gauge_hpa")),
            dewpoint_c=self._as_float(last_sample.get("dewpoint_c")),
            dew_temp_c=self._as_float(last_sample.get("dew_temp_c")),
            dew_rh_pct=self._as_float(last_sample.get("dew_rh_pct")),
            pace_output_state=self._as_int(last_sample.get("pace_output_state")),
            pace_isolation_state=self._as_int(last_sample.get("pace_isolation_state")),
            pace_vent_status=self._as_int(last_sample.get("pace_vent_status")),
            refresh_pace_state=False,
            fast_group_span_ms=self._as_float(last_sample.get("fast_group_span_ms")),
            dewpoint_live_c=self._as_float(last_sample.get("dewpoint_live_c")),
            dew_temp_live_c=self._as_float(last_sample.get("dew_temp_live_c")),
            dew_rh_live_pct=self._as_float(last_sample.get("dew_rh_live_pct")),
            sample_lag_ms=self._as_float(last_sample.get("sample_lag_ms")),
            deferred_export_queue_len=len(self._deferred_point_exports),
            event_ts=sample_done_ts,
            note="last sample complete for route handoff timing",
        )
        handoff_armed = self._maybe_begin_requested_sample_handoff(
            point,
            phase=phase_text,
            point_tag=point_tag,
        )
        export_deferral_request = self._consume_requested_sample_export_deferral(
            point,
            phase=phase_text,
            point_tag=point_tag,
        )
        route_seal_deferral_armed = bool(export_deferral_request)

        analyzer_labels = [label for label, _, _ in self._all_gas_analyzers()]
        integrity_summary = self._summarize_analyzer_integrity(samples, analyzer_labels=analyzer_labels)
        for data in samples:
            data.update(integrity_summary)
        self._record_pressure_gauge_freshness_runtime_fields(
            point,
            phase=phase_text,
            samples=samples,
        )
        self._evaluate_co2_sampling_window_qc(
            point,
            phase=phase_text,
            samples=samples,
        )
        self._update_point_quality_summary(point, phase=phase_text)
        self._copy_point_runtime_exports_into_samples(
            point,
            phase=phase_text,
            samples=samples,
        )
        self._annotate_point_trace_rows(
            point,
            samples,
            phase=phase_text,
            point_tag=point_tag,
        )
        if handoff_armed or route_seal_deferral_armed:
            self._enqueue_deferred_sample_exports(
                point,
                samples,
                phase=phase,
                point_tag=point_tag,
            )
        else:
            self._perform_light_point_exports(
                point,
                samples,
                phase=phase,
                point_tag=point_tag,
            )
        if route_seal_deferral_armed or (handoff_armed and self._defer_heavy_exports_during_handoff_enabled()):
            self._enqueue_deferred_point_exports(
                point,
                samples,
                phase=phase_text,
                point_tag=point_tag,
                analyzer_labels=analyzer_labels,
                integrity_summary=integrity_summary,
            )
        else:
            self._perform_heavy_point_exports(
                point,
                samples,
                phase=phase_text,
                point_tag=point_tag,
                analyzer_labels=analyzer_labels,
                integrity_summary=integrity_summary,
            )
        self._emit_stage_event(
            current=self._stage_label_for_point(point, phase=phase_text),
            point=point,
            phase=phase_text,
            point_tag=point_tag,
            wait_reason="采样完成",
        )

    def _point_title(self, point: CalibrationPoint, *, phase: str = "", point_tag: str = "") -> str:
        temp_text = (
            f"{float(point.temp_chamber_c):g}°C环境"
            if point.temp_chamber_c is not None
            else "环境温度未设"
        )
        pressure_label = self._pressure_target_label(point)
        if pressure_label:
            pressure_text = pressure_label if self._is_ambient_pressure_point(point) else f"气压{pressure_label}"
        else:
            pressure_text = "气压未设"

        if point.is_h2o_point or phase == "h2o":
            parts = [temp_text]
            if point.hgen_temp_c is not None or point.hgen_rh_pct is not None:
                hgen_temp = (
                    f"{float(point.hgen_temp_c):g}°C"
                    if point.hgen_temp_c is not None
                    else "温度未设"
                )
                hgen_rh = (
                    f"{float(point.hgen_rh_pct):g}%RH"
                    if point.hgen_rh_pct is not None
                    else "湿度未设"
                )
                parts.append(f"湿度发生器{hgen_temp}/{hgen_rh}")
            parts.append(pressure_text)
            return "，".join(parts)

        parts = [temp_text]
        if point.co2_ppm is not None:
            parts.append(f"二氧化碳{float(point.co2_ppm):g}ppm")
        elif point_tag:
            parts.append(str(point_tag))
        parts.append(pressure_text)
        return "，".join(parts)

    def _read_humidity_generator_dewpoint(self) -> Optional[float]:
        hgen = self.devices.get("humidity_gen")
        if not hgen:
            return None
        try:
            snap = hgen.fetch_all()
        except Exception:
            return None
        data = snap.get("data", {}) if isinstance(snap, dict) else {}
        return self._pick_numeric(data, ["Td", "DP", "dewpoint", "DewPoint"])

    def _read_humidity_generator_temp_rh(self) -> Tuple[Optional[float], Optional[float]]:
        hgen = self.devices.get("humidity_gen")
        if not hgen:
            return None, None
        try:
            snap = hgen.fetch_all()
        except Exception:
            return None, None
        data = snap.get("data", {}) if isinstance(snap, dict) else {}
        temp_c = self._pick_numeric(data, ["Tc", "Ts", "TA", "Temp", "temperature"])
        rh_pct = self._pick_numeric(data, ["Uw", "Ui", "Rh", "RH", "humidity", "Hum"])
        return self._as_float(temp_c), self._as_float(rh_pct)

    def _wait_dewpoint_alignment_stable(self, point: Optional[CalibrationPoint] = None) -> bool:
        dew = self.devices.get("dewpoint")
        if not dew:
            return False
        cfg = self.cfg.get("workflow", {}).get("stability", {}).get("dewpoint", {})
        if cfg and not cfg.get("enabled", True):
            self.log("Dewpoint open-route alignment disabled by configuration; skip wait")
            return True

        window_s = float(self._wf("workflow.stability.dewpoint.window_s", 60))
        timeout_raw = float(self._wf("workflow.stability.dewpoint.timeout_s", 1800))
        timeout_s: Optional[float] = timeout_raw if timeout_raw > 0 else None
        poll_s = float(self._wf("workflow.stability.dewpoint.poll_s", 1.0))
        temp_match_tol_c = float(
            self._wf(
                "workflow.stability.dewpoint.temp_match_tol_c",
                self._wf("workflow.stability.dewpoint.temp_tol_c", 0.3),
            )
        )
        base_rh_match_tol_pct = float(
            self._wf(
                "workflow.stability.dewpoint.rh_match_tol_pct",
                self._wf("workflow.stability.dewpoint.rh_tol_pct", 4.0),
            )
        )
        stability_tol_c = float(self._wf("workflow.stability.dewpoint.stability_tol_c", 0.01))
        target_humidity_rh = self._as_float(getattr(point, "hgen_rh_pct", None)) if point is not None else None
        rh_match_tol_pct = base_rh_match_tol_pct

        start = time.time()
        last_report = start
        last_dew_dp: Optional[float] = None
        last_dew_temp_c: Optional[float] = None
        last_dew_rh_pct: Optional[float] = None
        last_hgen_temp_c: Optional[float] = None
        last_hgen_rh_pct: Optional[float] = None
        last_temp_diff_c: Optional[float] = None
        last_rh_diff_pct: Optional[float] = None
        min_window_samples = max(2, int(self._wf("workflow.stability.dewpoint.min_samples", 2)))
        stable_samples: List[Tuple[float, float]] = []
        matched_since: Optional[float] = None

        if timeout_s is None:
            self.log("Dewpoint meter timeout disabled; waiting until dewpoint is stable")

        self._emit_stage_event(
            current=f"H2O 开路等待 Tc={float(getattr(point, 'hgen_temp_c', 0.0) or 0.0):g}°C Uw={float(getattr(point, 'hgen_rh_pct', 0.0) or 0.0):g}%",
            point=point,
            phase="h2o",
            wait_reason="露点仪对齐",
        )
        while True:
            if self.stop_event.is_set():
                return False
            self._check_pause()
            self._refresh_live_analyzer_snapshots(reason="dewpoint alignment wait")

            if timeout_s is not None and (time.time() - start) >= timeout_s:
                break

            dew_data = dew.get_current()
            dew_dp = self._as_float(dew_data.get("dewpoint_c")) if isinstance(dew_data, dict) else None
            dew_temp_c = self._as_float(dew_data.get("temp_c")) if isinstance(dew_data, dict) else None
            dew_rh_pct = self._as_float(dew_data.get("rh_pct")) if isinstance(dew_data, dict) else None
            hgen_temp_c, hgen_rh_pct = self._read_humidity_generator_temp_rh()
            last_dew_temp_c = dew_temp_c
            last_dew_rh_pct = dew_rh_pct
            last_hgen_temp_c = hgen_temp_c
            last_hgen_rh_pct = hgen_rh_pct
            last_temp_diff_c = (
                abs(float(dew_temp_c) - float(hgen_temp_c))
                if dew_temp_c is not None and hgen_temp_c is not None
                else None
            )
            last_rh_diff_pct = (
                abs(float(dew_rh_pct) - float(hgen_rh_pct))
                if dew_rh_pct is not None and hgen_rh_pct is not None
                else None
            )
            matched = (
                last_temp_diff_c is not None
                and last_temp_diff_c <= temp_match_tol_c
                and last_rh_diff_pct is not None
                and last_rh_diff_pct <= rh_match_tol_pct
            )

            if matched and dew_dp is not None:
                now = time.time()
                last_dew_dp = float(dew_dp)
                if matched_since is None:
                    matched_since = now
                stable_samples.append((now, last_dew_dp))
                # Keep a little extra history so the window can fully mature before
                # older points are trimmed away.
                stable_samples = [(ts, value) for ts, value in stable_samples if now - ts <= (window_s + poll_s + 1.0)]
                window_samples = [(ts, value) for ts, value in stable_samples if now - ts <= window_s]
                window_span = self._span([value for _, value in window_samples]) if window_samples else float("inf")
                if len(stable_samples) == 1:
                    self.log(
                        "Dewpoint meter temp/rh matched humidity generator: "
                        f"dew_temp={last_dew_temp_c} hgen_temp={last_hgen_temp_c} diff={last_temp_diff_c} "
                        f"dew_rh={last_dew_rh_pct} hgen_rh={last_hgen_rh_pct} diff={last_rh_diff_pct}"
                    )
                    self.log(
                        f"Dewpoint meter stability window started: dewpoint={last_dew_dp} "
                        f"window={int(window_s)}s tol={stability_tol_c}"
                    )
                elif (
                    matched_since is not None
                    and len(window_samples) >= min_window_samples
                    and (now - matched_since) >= window_s
                    and window_span <= stability_tol_c
                ):
                    self.log(
                        f"Dewpoint meter stable: dewpoint={last_dew_dp} "
                        f"window={int(window_s)}s span={window_span:.4f} "
                        f"samples={len(window_samples)}"
                    )
                    return True
            else:
                if stable_samples:
                    self.log(
                        "Dewpoint meter temp/rh no longer matched humidity generator: "
                        f"dew_temp={last_dew_temp_c} hgen_temp={last_hgen_temp_c} diff={last_temp_diff_c} "
                        f"dew_rh={last_dew_rh_pct} hgen_rh={last_hgen_rh_pct} diff={last_rh_diff_pct}"
                    )
                stable_samples = []
                matched_since = None
                if dew_dp is not None:
                    last_dew_dp = float(dew_dp)

            if time.time() - last_report >= 30:
                last_report = time.time()
                if not matched:
                    self._emit_stage_event(
                        current=f"H2O 开路等待 Tc={float(last_hgen_temp_c or 0.0):.1f}°C Uw={float(last_hgen_rh_pct or 0.0):.1f}%",
                        point=point,
                        phase="h2o",
                        wait_reason="露点仪对齐",
                    )
                    self.log(
                        "Dewpoint meter matching humidity generator... "
                        f"dew_temp={last_dew_temp_c} hgen_temp={last_hgen_temp_c} diff={last_temp_diff_c} "
                        f"dew_rh={last_dew_rh_pct} hgen_rh={last_hgen_rh_pct} diff={last_rh_diff_pct} "
                        f"tol=({temp_match_tol_c}C,{rh_match_tol_pct}%) dewpoint={last_dew_dp}"
                    )
                elif not stable_samples or matched_since is None:
                    self._emit_stage_event(
                        current=f"H2O 开路等待 Tc={float(last_hgen_temp_c or 0.0):.1f}°C Uw={float(last_hgen_rh_pct or 0.0):.1f}%",
                        point=point,
                        phase="h2o",
                        wait_reason="露点仪对齐",
                    )
                    self.log(f"Dewpoint meter settling... dewpoint={last_dew_dp}")
                else:
                    now = time.time()
                    remain = max(0.0, window_s - (now - matched_since))
                    window_samples = [(ts, value) for ts, value in stable_samples if now - ts <= window_s]
                    span = self._span([value for _, value in window_samples]) if window_samples else float("inf")
                    sample_count = len(window_samples)
                    self._emit_stage_event(
                        current=f"H2O 开路等待 Tc={float(last_hgen_temp_c or 0.0):.1f}°C Uw={float(last_hgen_rh_pct or 0.0):.1f}%",
                        point=point,
                        phase="h2o",
                        wait_reason="露点仪对齐",
                        countdown_s=remain,
                        detail=f"span={span:.4f} samples={sample_count}/{min_window_samples}",
                    )
                    self.log(
                        f"Dewpoint meter observing stability... dewpoint={last_dew_dp} "
                        f"span={span:.4f} samples={sample_count}/{min_window_samples} "
                        f"remaining={int(remain)}s"
                    )

            time.sleep(max(0.1, poll_s))

        self.log(
            "Dewpoint meter stability timeout: "
            f"dewpoint={last_dew_dp} dew_temp={last_dew_temp_c} hgen_temp={last_hgen_temp_c} "
            f"temp_diff={last_temp_diff_c} dew_rh={last_dew_rh_pct} hgen_rh={last_hgen_rh_pct} "
            f"rh_diff={last_rh_diff_pct} tol=({temp_match_tol_c}C,{rh_match_tol_pct}%)"
        )
        return False

    @staticmethod
    def _dewpoint_meter_window_stable(
        values: List[float],
        *,
        fluct_tol: float = 0.09,
        decimals: int = 1,
    ) -> bool:
        if len(values) < 2:
            return False

        rounded = [round(float(v), decimals) for v in values]
        first = rounded[0]
        return all(one == first for one in rounded)

    @staticmethod
    def _dewpoint_in_target_band(
        value: float,
        *,
        target: Optional[float],
        tol: float,
    ) -> bool:
        if target is None:
            return True
        return abs(float(value) - float(target)) <= float(tol)

    def _wait_dewpoint_stable(self) -> bool:
        return self._wait_dewpoint_alignment_stable()

    @staticmethod
    def _safe_report_prefix(text: Any) -> str:
        prefix = re.sub(r"[^0-9A-Za-z_-]+", "_", str(text or "").strip())
        prefix = prefix.strip("_")
        return prefix or "analyzer"

    def _load_analyzer_summary_rows(self) -> List[Dict[str, Any]]:
        path = getattr(self.logger, "analyzer_summary_csv_path", None)
        if not path:
            return []
        try:
            with open(path, "r", encoding="utf-8-sig", newline="") as handle:
                return list(csv.DictReader(handle))
        except FileNotFoundError:
            self.log(f"Analyzer summary not found: {path}")
        except Exception as exc:
            self.log(f"Failed to read analyzer summary: {exc}")
        return []

    @staticmethod
    def _summary_phase_key(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in {"h2o", "水路"}:
            return "h2o"
        if text in {"co2", "气路"}:
            return "co2"
        return text

    @staticmethod
    def _summary_float_value(row: Dict[str, Any], *keys: str) -> Optional[float]:
        for key in keys:
            value = row.get(key)
            if value in (None, ""):
                continue
            try:
                numeric = float(value)
            except Exception:
                continue
            if math.isfinite(numeric):
                return numeric
        return None

    @staticmethod
    def _summary_temperature_keys(primary_key: Optional[str] = None) -> List[str]:
        keys: List[str] = []
        for key in (
            "thermometer_temp_c",
            primary_key,
            "Temp",
            "T1",
            "temp_chamber_c",
            "temp_c",
            "temp_set_c",
        ):
            text = str(key or "").strip()
            if text and text not in keys:
                keys.append(text)
        return keys

    @staticmethod
    def _ratio_poly_pressure_candidates(
        configured_pressure_key: Optional[str],
        preference: str,
    ) -> Tuple[str, ...]:
        analyzer_key = str(configured_pressure_key or "BAR").strip() or "BAR"
        mode = str(preference or "analyzer_only").strip().lower()
        if mode == "reference_first":
            ordered = ["P", analyzer_key]
        elif mode == "reference_only":
            ordered = ["P"]
        else:
            ordered = [analyzer_key]

        out: List[str] = []
        for key in ordered:
            text = str(key or "").strip()
            if text and text not in out:
                out.append(text)
        return tuple(out)

    def _resolve_ratio_poly_columns(
        self,
        rows: List[Dict[str, Any]],
        *,
        gas: str,
        target_key: str,
        ratio_key: str,
        temp_keys: Tuple[str, ...],
        pressure_keys: Tuple[str, ...],
    ) -> Dict[str, Any]:
        dataframe = records_to_dataframe(rows)
        target_column = resolve_column_name(dataframe, (target_key,), label=f"{gas} target")
        ratio_column = resolve_column_name(dataframe, (ratio_key,), label=f"{gas} ratio")
        temp_column = resolve_column_name(dataframe, temp_keys, label=f"{gas} temperature")
        pressure_column = resolve_column_name(dataframe, pressure_keys, label=f"{gas} pressure")
        return {
            "dataframe": dataframe,
            "target_column": target_column,
            "ratio_column": ratio_column,
            "temp_column": temp_column,
            "pressure_column": pressure_column,
        }

    def _log_ratio_poly_pressure_diagnostics(self, analyzer_label: str, dataframe: Any) -> None:
        if "P" not in getattr(dataframe, "columns", ()) or "BAR" not in getattr(dataframe, "columns", ()):
            return
        try:
            pair = dataframe[["P", "BAR"]].apply(lambda column: column.astype(float))
        except Exception:
            return
        valid = pair.dropna()
        if valid.empty:
            self.log(
                f"Ratio-poly pressure compare [{analyzer_label}]: "
                f"P_coverage={int(pair['P'].notna().sum())} BAR_coverage={int(pair['BAR'].notna().sum())} overlap=0"
            )
            return
        diff = (valid["P"] - valid["BAR"]).abs()
        self.log(
            f"Ratio-poly pressure compare [{analyzer_label}]: "
            f"P_coverage={int(pair['P'].notna().sum())} BAR_coverage={int(pair['BAR'].notna().sum())} "
            f"overlap={int(len(valid))} mean_abs_diff={float(diff.mean()):.6g} max_abs_diff={float(diff.max()):.6g}"
        )

    @staticmethod
    def _matches_any_temperature(value: Optional[float], targets: List[float], tol_c: float) -> bool:
        if value is None:
            return False
        for target in targets:
            if abs(float(value) - float(target)) <= float(tol_c):
                return True
        return False

    @staticmethod
    def _nearest_temperature_group(
        value: Optional[float],
        groups_c: List[float],
        *,
        max_distance_c: float,
    ) -> Optional[float]:
        if value is None or not groups_c:
            return None
        nearest = min(groups_c, key=lambda item: abs(float(value) - float(item)))
        if abs(float(value) - float(nearest)) <= float(max_distance_c):
            return float(nearest)
        return None

    def _filter_ratio_poly_summary_rows(
        self,
        rows: List[Dict[str, Any]],
        *,
        gas: str,
        cfg: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        gas_lower = str(gas or "").strip().lower()
        if gas_lower != "h2o":
            return list(rows)

        select_cfg = normalize_h2o_summary_selection(cfg.get("h2o_summary_selection"))
        include_h2o_phase = bool(select_cfg["include_h2o_phase"])
        include_co2_zero_rows = bool(select_cfg["include_co2_zero_ppm_rows"])
        co2_zero_target = float(select_cfg["co2_zero_ppm_target"])
        co2_zero_tol = float(select_cfg["co2_zero_ppm_tolerance"])
        temp_tol_c = float(select_cfg["temp_tolerance_c"])
        bucket_tol_c = float(select_cfg["temperature_bucket_tolerance_c"])
        bucket_points = [float(item) for item in (select_cfg.get("temperature_buckets_c") or [])]
        include_co2_temp_groups = [float(item) for item in (select_cfg.get("include_co2_temp_groups_c") or [])]
        include_co2_zero_temp_groups = [
            float(item) for item in (select_cfg.get("include_co2_zero_ppm_temp_groups_c") or [])
        ]

        filtered: List[Dict[str, Any]] = []
        h2o_phase_count = 0
        co2_temp_group_count = 0
        co2_zero_count = 0

        for row in rows:
            phase_key = self._summary_phase_key(row.get("PointPhase"))
            if include_h2o_phase and phase_key == "h2o":
                filtered.append(row)
                h2o_phase_count += 1
                continue

            if phase_key != "co2":
                continue

            co2_target = self._summary_float_value(row, "ppm_CO2_Tank", "co2_ppm_target")
            temp_set_c = self._summary_float_value(row, "TempSet", "temp_chamber_c")
            temp_measured_c = self._summary_float_value(row, *self._summary_temperature_keys())
            temp_group_c = temp_set_c
            if temp_group_c is None:
                temp_group_c = self._nearest_temperature_group(
                    temp_measured_c,
                    bucket_points,
                    max_distance_c=bucket_tol_c,
                )
            if temp_group_c is None and temp_measured_c is not None:
                temp_group_c = temp_measured_c

            if self._matches_any_temperature(temp_group_c, include_co2_temp_groups, temp_tol_c):
                filtered.append(row)
                co2_temp_group_count += 1
                continue

            if not include_co2_zero_rows:
                continue
            if co2_target is None or abs(co2_target - co2_zero_target) > co2_zero_tol:
                continue
            if not self._matches_any_temperature(temp_group_c, include_co2_zero_temp_groups, temp_tol_c):
                continue

            filtered.append(row)
            co2_zero_count += 1

        self.log(
            "H2O ratio-poly summary selection: "
            f"h2o_phase={h2o_phase_count} co2_temp_group={co2_temp_group_count} "
            f"co2_zero_temp={co2_zero_count} total={len(filtered)}"
        )
        return filtered

    def _auto_fit_ratio_poly_from_summary(self, cfg: Dict[str, Any], *, gas: str, model: str) -> None:
        summary_rows = self._load_analyzer_summary_rows()
        if not summary_rows:
            self.log("Ratio-poly auto-fit requested but no analyzer summary rows were found")
            return

        gas_lower = str(gas or "").strip().lower()
        summary_rows = self._filter_ratio_poly_summary_rows(summary_rows, gas=gas_lower, cfg=cfg)
        if not summary_rows:
            self.log(f"Ratio-poly auto-fit requested but no filtered {gas_lower.upper()} summary rows were found")
            return
        summary_columns = cfg.get("summary_columns", {}).get(gas_lower, {})
        target_key = summary_columns.get("target", "ppm_CO2_Tank" if gas_lower == "co2" else "ppm_H2O_Dew")
        ratio_key = summary_columns.get("ratio", "R_CO2" if gas_lower == "co2" else "R_H2O")
        temp_key = summary_columns.get("temperature", "thermometer_temp_c")
        temp_keys = tuple(self._summary_temperature_keys(str(temp_key or "").strip()))
        pressure_key = summary_columns.get("pressure", "BAR")
        pressure_scale = float(summary_columns.get("pressure_scale", 1.0))
        ratio_poly_fit_cfg = cfg.get("ratio_poly_fit", {}) if isinstance(cfg.get("ratio_poly_fit", {}), dict) else {}
        pressure_preference = str(
            cfg_get(
                self.cfg,
                "coefficients.ratio_poly_fit.pressure_source_preference",
                ratio_poly_fit_cfg.get("pressure_source_preference", "reference_first"),
            )
            or "reference_first"
        ).strip().lower()
        pressure_keys = self._ratio_poly_pressure_candidates(pressure_key, pressure_preference)
        self.log(
            f"Ratio-poly auto-fit setup [{gas_lower.upper()}]: rows={len(summary_rows)} "
            f"target={target_key} ratio={ratio_key} temp_candidates={list(temp_keys)} "
            f"pressure_candidates={list(pressure_keys)} pressure_scale={pressure_scale:g} "
            f"pressure_source_preference={pressure_preference} "
            "config_path=coefficients.ratio_poly_fit.pressure_source_preference"
        )

        ratio_degree = int(cfg.get("ratio_degree", 3))
        temperature_offset_c = float(cfg.get("temperature_offset_c", 273.15))
        add_intercept = bool(cfg.get("add_intercept", True))
        simplify_coefficients = bool(cfg.get("simplify_coefficients", True))
        simplification_method = str(cfg.get("simplification_method", "column_norm") or "column_norm")
        target_digits = int(cfg.get("target_digits", 6))
        min_samples = int(cfg.get("min_samples", 0) or 0)
        include_residuals = bool(cfg.get("save_residuals", True))
        train_ratio = float(cfg.get("train_ratio", 0.7))
        val_ratio = float(cfg.get("val_ratio", 0.15))
        random_seed = int(cfg.get("random_seed", 42))
        shuffle_dataset = bool(cfg.get("shuffle_dataset", True))
        evaluation_bins_cfg = cfg.get("evaluation_bins")
        evaluation_bins = None
        if isinstance(evaluation_bins_cfg, dict):
            evaluation_bins = evaluation_bins_cfg.get(gas_lower)
        elif evaluation_bins_cfg is not None:
            evaluation_bins = evaluation_bins_cfg
        robust_iterations = int(cfg.get("robust_iterations", 8) or 0)
        robust_huber_delta = float(cfg.get("robust_huber_delta", 1.5))
        robust_min_weight = float(cfg.get("robust_min_weight", 0.05))
        candidate_methods = cfg.get("candidate_simplification_methods")

        fit_fn = fit_ratio_poly_rt_p
        if model in {"ratio_poly_rt_p_evolved", "poly_rt_p_evolved"}:
            fit_fn = fit_ratio_poly_rt_p_evolved

        grouped_rows: Dict[str, List[Dict[str, Any]]] = {}
        for row in summary_rows:
            analyzer_label = str(row.get("Analyzer") or "").strip()
            if not analyzer_label:
                continue
            grouped_rows.setdefault(analyzer_label, []).append(row)

        if not grouped_rows:
            self.log("Ratio-poly auto-fit requested but no per-analyzer summary rows were found")
            return

        fitted = 0
        for analyzer_label, analyzer_rows in sorted(grouped_rows.items()):
            try:
                resolved = self._resolve_ratio_poly_columns(
                    analyzer_rows,
                    gas=gas_lower,
                    target_key=target_key,
                    ratio_key=ratio_key,
                    temp_keys=temp_keys,
                    pressure_keys=pressure_keys,
                )
                dataframe = resolved["dataframe"]
                actual_target_column = str(resolved["target_column"])
                actual_ratio_column = str(resolved["ratio_column"])
                actual_temp_column = str(resolved["temp_column"])
                actual_pressure_column = str(resolved["pressure_column"])
                self._log_ratio_poly_pressure_diagnostics(analyzer_label, dataframe)
                self.log(
                    f"Ratio-poly fit input [{gas_lower.upper()}][{analyzer_label}]: rows={len(analyzer_rows)} "
                    f"target_key={actual_target_column} ratio_key={actual_ratio_column} "
                    f"temp_key={actual_temp_column} selected_pressure_key={actual_pressure_column} "
                    "pressure_selection_mode=batch_unified "
                    f"pressure_scale={pressure_scale:g}"
                )
                if fit_fn is fit_ratio_poly_rt_p_evolved:
                    result = fit_fn(
                        analyzer_rows,
                        gas=gas_lower,
                        target_key=target_key,
                        ratio_keys=(ratio_key,),
                        temp_keys=temp_keys,
                        pressure_keys=pressure_keys,
                        pressure_scale=pressure_scale,
                        ratio_degree=ratio_degree,
                        temperature_offset_c=temperature_offset_c,
                        add_intercept=add_intercept,
                        simplify_coefficients=simplify_coefficients,
                        simplification_method=simplification_method,
                        target_digits=target_digits,
                        min_samples=min_samples,
                        train_ratio=train_ratio,
                        val_ratio=val_ratio,
                        random_seed=random_seed,
                        shuffle_dataset=shuffle_dataset,
                        evaluation_bins=evaluation_bins,
                        log_fn=self.log,
                        robust_iterations=robust_iterations,
                        robust_huber_delta=robust_huber_delta,
                        robust_min_weight=robust_min_weight,
                        candidate_simplification_methods=candidate_methods,
                    )
                else:
                    result = fit_fn(
                        analyzer_rows,
                        gas=gas_lower,
                        target_key=target_key,
                        ratio_keys=(ratio_key,),
                        temp_keys=temp_keys,
                        pressure_keys=pressure_keys,
                        pressure_scale=pressure_scale,
                        ratio_degree=ratio_degree,
                        temperature_offset_c=temperature_offset_c,
                        add_intercept=add_intercept,
                        simplify_coefficients=simplify_coefficients,
                        simplification_method=simplification_method,
                        target_digits=target_digits,
                        min_samples=min_samples,
                        train_ratio=train_ratio,
                        val_ratio=val_ratio,
                        random_seed=random_seed,
                        shuffle_dataset=shuffle_dataset,
                        evaluation_bins=evaluation_bins,
                        log_fn=self.log,
                    )
                prefix = f"{gas_lower}_{self._safe_report_prefix(analyzer_label)}_ratio_poly"
                outputs = save_ratio_poly_report(
                    result,
                    self.logger.samples_path.parent,
                    prefix=prefix,
                    include_residuals=include_residuals,
                )
                self.log(
                    f"Auto-fit {gas_lower.upper()} [{analyzer_label}] ({result.model}): n={result.n} "
                    f"rmse={result.stats['rmse_simplified']:.6g} "
                    f"max={result.stats['max_abs_simplified']:.6g}"
                )
                self.log(f"Auto-fit report saved: {outputs.get('json')}")
                fitted += 1
            except Exception as exc:
                self.log(f"Ratio-poly {gas_lower.upper()} fit failed [{analyzer_label}]: {exc}")

        if fitted == 0:
            self.log(f"Ratio-poly {gas_lower.upper()} auto-fit produced no reports")

    def _maybe_write_coefficients(self) -> None:
        cfg = self.cfg.get("coefficients", {})
        if not cfg or not cfg.get("enabled"):
            return
        ga = self.devices.get("gas_analyzer")
        if not ga:
            self.log("Coefficients enabled but gas analyzer is not available")
            return

        if cfg.get("auto_fit"):
            if not self._all_samples:
                self.log("Auto-fit requested but no samples were collected")
            else:
                model = cfg.get("model", "amt_eq4")
                if model == "amt_eq4":
                    order = int(cfg.get("order", 2))
                    p0_hpa = float(cfg.get("p0_hpa", 1013.25))
                    t0_k = float(cfg.get("t0_k", 273.15))
                    temp_keys = cfg.get("temp_keys")
                    pressure_keys = cfg.get("pressure_keys")
                    signal_keys = cfg.get("signal_keys", {}).get("co2")
                    dry_air = bool(cfg.get("dry_air_correction", True))
                    h2o_source = cfg.get("h2o_source", "target")
                    min_samples = int(cfg.get("min_samples", 0) or 0)
                    try:
                        result = fit_amt_eq4(
                            self._all_samples,
                            gas="co2",
                            target_key="co2_ppm_target",
                            signal_keys=signal_keys,
                            temp_keys=temp_keys,
                            pressure_keys=pressure_keys,
                            order=order,
                            p0_hpa=p0_hpa,
                            t0_k=t0_k,
                            dry_air_correction=dry_air,
                            h2o_source=h2o_source,
                            min_samples=min_samples,
                        )
                        outputs = save_fit_report(
                            result,
                            self.logger.samples_path.parent,
                            prefix="co2",
                            include_residuals=bool(cfg.get("save_residuals", True)),
                        )
                        self.log(
                            f"Auto-fit CO2: n={result.n} rmse={result.stats['rmse']:.3f} ppm "
                            f"max={result.stats['max_abs']:.3f} ppm"
                        )
                        self.log(f"Auto-fit report saved: {outputs.get('json')}")
                    except Exception as exc:
                        self.log(f"Auto-fit failed: {exc}")

                    if cfg.get("fit_h2o"):
                        signal_keys_h2o = cfg.get("signal_keys", {}).get("h2o")
                        try:
                            result_h2o = fit_amt_eq4(
                                self._all_samples,
                                gas="h2o",
                                target_key="h2o_mmol_target",
                                signal_keys=signal_keys_h2o,
                                temp_keys=temp_keys,
                                pressure_keys=pressure_keys,
                                order=order,
                                p0_hpa=p0_hpa,
                                t0_k=t0_k,
                                dry_air_correction=False,
                                h2o_source=h2o_source,
                                min_samples=min_samples,
                            )
                            outputs_h2o = save_fit_report(
                                result_h2o,
                                self.logger.samples_path.parent,
                                prefix="h2o",
                                include_residuals=bool(cfg.get("save_residuals", True)),
                            )
                            self.log(
                                f"Auto-fit H2O: n={result_h2o.n} rmse={result_h2o.stats['rmse']:.3f} mmol/mol "
                                f"max={result_h2o.stats['max_abs']:.3f} mmol/mol"
                            )
                            self.log(f"Auto-fit report saved: {outputs_h2o.get('json')}")
                        except Exception as exc:
                            self.log(f"H2O auto-fit failed: {exc}")
                elif model in {
                    "ratio_poly_rt_p",
                    "poly_rt_p",
                    "ratio_poly_rt_p_evolved",
                    "poly_rt_p_evolved",
                }:
                    self._auto_fit_ratio_poly_from_summary(cfg, gas="co2", model=model)
                    if cfg.get("fit_h2o"):
                        self._auto_fit_ratio_poly_from_summary(cfg, gas="h2o", model=model)
                else:
                    self.log(f"Unknown auto-fit model: {model}")

        sencos = cfg.get("sencos", {})
        if sencos:
            target_groups: Dict[int, List[float]] = {}
            for key, coeff in sencos.items():
                idx = int(key)
                target_groups[idx] = self._coerce_senco_values(coeff)

            from ..tools.run_v1_corrected_autodelivery import write_senco_groups_with_full_verification

            result = write_senco_groups_with_full_verification(
                ga,
                expected_groups=target_groups,
            )
            self._persist_coefficient_write_result(ga, result)
            for detail in list(result.get("detail_rows") or []):
                idx = int(detail.get("group"))
                target_values = [float(value) for value in list(detail.get("coeff_target") or [])]
                text = ",".join(format_senco_values(target_values))
                verify_status = str(detail.get("verify_status") or "").strip().lower()
                rollback_status = str(detail.get("rollback_status") or "").strip().lower()
                failure_reason = str(detail.get("failure_reason") or "").strip()
                if verify_status == "success":
                    self.log(
                        f"Wrote SENCO{idx} {text} "
                        f"readback_ok tolerance={float(detail.get('compare_tolerance') or 0.0):g}"
                    )
                elif rollback_status == "success":
                    self.log(
                        f"SENCO{idx} writeback failed and rolled back safely: "
                        f"{failure_reason or 'READBACK_MISMATCH'}"
                    )
                else:
                    self.log(
                        f"SENCO{idx} writeback failed: "
                        f"{failure_reason or 'unknown_failure'}"
                    )
            if not bool(result.get("ok", False)):
                failure_reason = str(result.get("failure_reason") or "coefficient writeback failed")
                if bool(result.get("unsafe", False)):
                    raise RuntimeError(f"Coefficient writeback unsafe: {failure_reason}")
                raise RuntimeError(f"Coefficient writeback failed: {failure_reason}")

    def _persist_coefficient_write_result(self, ga: Any, result: Mapping[str, Any]) -> None:
        log_write = getattr(self.logger, "log_coefficient_write", None)
        save_ts = self._ts_from_datetime(datetime.now())
        device_id = _normalized_device_id_text(getattr(ga, "device_id", "") or "")
        port_text = str(getattr(getattr(ga, "ser", None), "port", "") or "").strip()
        for detail in list(result.get("detail_rows") or []):
            row = {
                "run_id": getattr(self.logger, "run_id", ""),
                "session_id": getattr(self.logger, "run_id", ""),
                "save_ts": save_ts,
                "device_id": device_id,
                "port": port_text,
                "senco_group": detail.get("group"),
                "coeff_before": json.dumps(list(detail.get("coeff_before") or []), ensure_ascii=False),
                "coeff_target": json.dumps(list(detail.get("coeff_target") or []), ensure_ascii=False),
                "coeff_readback": json.dumps(list(detail.get("coeff_readback") or []), ensure_ascii=False),
                "coeff_rollback_target": json.dumps(list(detail.get("coeff_rollback_target") or []), ensure_ascii=False),
                "coeff_rollback_readback": json.dumps(list(detail.get("coeff_rollback_readback") or []), ensure_ascii=False),
                "mode_before": detail.get("mode_before"),
                "mode_after": detail.get("mode_after"),
                "mode_exit_attempted": bool(result.get("mode_exit_attempted", False)),
                "mode_exit_confirmed": bool(result.get("mode_exit_confirmed", False)),
                "rollback_attempted": bool(detail.get("rollback_attempted", result.get("rollback_attempted", False))),
                "rollback_confirmed": bool(detail.get("rollback_confirmed", result.get("rollback_confirmed", False))),
                "write_status": detail.get("write_status"),
                "verify_status": detail.get("verify_status"),
                "rollback_status": detail.get("rollback_status"),
                "overall_write_status": result.get("write_status"),
                "overall_verify_status": result.get("verify_status"),
                "overall_rollback_status": result.get("rollback_status"),
                "failure_reason": detail.get("failure_reason"),
                "compare_tolerance": detail.get("compare_tolerance"),
                "overall_ok": bool(result.get("ok", False)),
                "overall_unsafe": bool(result.get("unsafe", False)),
            }
            if callable(log_write):
                try:
                    log_write(row)
                except Exception as exc:
                    self.log(f"Coefficient write audit export failed: {exc}")
            self._log_run_event(
                command="coefficient-writeback",
                response=json.dumps(row, ensure_ascii=False, default=str, separators=(",", ":")),
            )

    @staticmethod
    def _coerce_senco_values(raw: Any) -> List[float]:
        if isinstance(raw, (list, tuple)):
            values = list(raw)
        elif isinstance(raw, dict):
            explicit = raw.get("values")
            if isinstance(explicit, (list, tuple)):
                values = list(explicit)
            else:
                ordered_keys = ("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L")
                values = [raw[key] for key in ordered_keys if key in raw]
                if not values:
                    lower_map = {str(key).upper(): value for key, value in raw.items()}
                    values = [lower_map[key] for key in ordered_keys if key in lower_map]
        else:
            raise ValueError("SENCO coefficients must be a dict, list, or tuple")

        if not values:
            raise ValueError("SENCO coefficient payload is empty")

        numeric: List[float] = []
        for value in values:
            numeric.append(float(value))
        if len(numeric) > 6:
            raise ValueError("SENCO coefficient payload exceeds 6 values")
        return numeric
