from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import Any, Optional

from ..models import CalibrationPoint
from ..orchestration_context import OrchestrationContext
from ..run_state import RunState
from .sampling_service import read_device_snapshot_with_retry


class DewpointAlignmentService:
    """Dewpoint readiness, pre-seal soak, and alignment helpers for H2O routes."""

    DEWPOINT_FRAME_KEYS = ("dewpoint_c", "dew_point_c", "dew_point", "Td", "temp_c", "rh_pct")

    def __init__(self, context: OrchestrationContext, run_state: RunState, *, host: Any) -> None:
        self.context = context
        self.run_state = run_state
        self.host = host
        self.last_h2o_route_ready_evidence: list[dict[str, Any]] = []

    def ensure_dewpoint_meter_ready(self) -> bool:
        dewpoint = self.host._device("dewpoint_meter")
        if dewpoint is None:
            if self.host._collect_only_fast_path_enabled():
                self.host._log("Collect-only mode: dewpoint meter unavailable, continue without readiness check")
                return True
            self.host._log("Dewpoint meter unavailable")
            return False
        opener = getattr(dewpoint, "open", None)
        if callable(opener):
            try:
                opener()
            except Exception as exc:
                if self.host._collect_only_fast_path_enabled():
                    self.host._log(f"Collect-only mode: dewpoint meter open failed but ignored: {exc}")
                    return True
                self.host._log(f"Dewpoint meter open failed: {exc}")
                return False
        try:
            snapshot = self._read_dewpoint_snapshot(
                dewpoint,
                context="dewpoint meter initial read",
                log_failures=True,
            )
            self.host._log(f"Dewpoint meter ready: dewpoint={snapshot.get('dewpoint_c')} temp={snapshot.get('temp_c')}")
            return True
        except Exception as exc:
            if self.host._collect_only_fast_path_enabled():
                self.host._log(f"Collect-only mode: dewpoint meter initial read failed but ignored: {exc}")
                return True
            self.host._log(f"Dewpoint meter initial read failed: {exc}")
            return False

    def wait_h2o_route_soak_before_seal(self, point: CalibrationPoint) -> bool:
        if self.host._collect_only_fast_path_enabled():
            self.host._log("Collect-only mode: H2O route pre-seal soak skipped")
            return True
        soak_s = float(self.host._cfg_get("workflow.stability.h2o_route.preseal_soak_s", 300.0))
        if soak_s <= 0:
            return True
        start = time.time()
        while time.time() - start < soak_s:
            self.host._check_stop()
            self._refresh_live_snapshots(reason="h2o_route_preseal_soak")
            time.sleep(min(1.0, max(0.05, soak_s - (time.time() - start))))
        return True

    def wait_dewpoint_alignment_stable(self, point: Optional[CalibrationPoint] = None) -> bool:
        dewpoint = self.host._device("dewpoint_meter")
        if dewpoint is None:
            return bool(self.host._collect_only_fast_path_enabled())
        if self.host._collect_only_fast_path_enabled():
            self.host._log("Collect-only mode: dewpoint alignment wait skipped")
            return True
        window_s = float(self.host._cfg_get("workflow.stability.dewpoint.window_s", 40.0))
        timeout_s = float(self.host._cfg_get("workflow.stability.dewpoint.timeout_s", 1800.0))
        poll_s = max(0.1, float(self.host._cfg_get("workflow.stability.dewpoint.poll_s", 1.0)))
        temp_tol = float(
            self.host._cfg_get(
                "workflow.stability.dewpoint.temp_match_tol_c",
                self.host._cfg_get("workflow.stability.dewpoint.temp_tol_c", 0.3),
            )
        )
        rh_tol = float(
            self.host._cfg_get(
                "workflow.stability.dewpoint.rh_match_tol_pct",
                self.host._cfg_get("workflow.stability.dewpoint.rh_tol_pct", 4.0),
            )
        )
        stability_tol = float(self.host._cfg_get("workflow.stability.dewpoint.stability_tol_c", 0.06))
        min_window_samples = max(2, int(self.host._cfg_get("workflow.stability.dewpoint.min_samples", 2)))
        start = time.time()
        last_report = start
        matched_since: Optional[float] = None
        stable_samples: list[tuple[float, float]] = []
        last_dew_dp: Optional[float] = None
        last_dew_temp_c: Optional[float] = None
        last_dew_rh_pct: Optional[float] = None
        last_hgen_temp_c: Optional[float] = None
        last_hgen_rh_pct: Optional[float] = None
        last_temp_diff_c: Optional[float] = None
        last_rh_diff_pct: Optional[float] = None
        while time.time() - start < timeout_s:
            self.host._check_stop()
            self._refresh_live_snapshots(reason="dewpoint_alignment_wait")
            snapshot = self._read_dewpoint_snapshot(
                dewpoint,
                context="dewpoint alignment wait",
                log_failures=False,
            )
            dew_dp = self.host._as_float(snapshot.get("dewpoint_c"))
            dew_temp = self.host._as_float(snapshot.get("temp_c"))
            dew_rh = self.host._as_float(snapshot.get("rh_pct"))
            hgen_temp, hgen_rh = self.host._read_humidity_generator_temp_rh()
            if hgen_temp is None and point is not None:
                hgen_temp = self.host._as_float(point.hgen_temp_c)
            if hgen_rh is None and point is not None:
                hgen_rh = self.host._as_float(point.hgen_rh_pct)
            last_dew_dp = dew_dp if dew_dp is None else float(dew_dp)
            last_dew_temp_c = dew_temp if dew_temp is None else float(dew_temp)
            last_dew_rh_pct = dew_rh if dew_rh is None else float(dew_rh)
            last_hgen_temp_c = hgen_temp if hgen_temp is None else float(hgen_temp)
            last_hgen_rh_pct = hgen_rh if hgen_rh is None else float(hgen_rh)
            last_temp_diff_c = (
                abs(float(dew_temp) - float(hgen_temp))
                if dew_temp is not None and hgen_temp is not None
                else None
            )
            last_rh_diff_pct = (
                abs(float(dew_rh) - float(hgen_rh))
                if dew_rh is not None and hgen_rh is not None
                else None
            )
            matched = (
                dew_dp is not None
                and last_temp_diff_c is not None
                and last_temp_diff_c <= temp_tol
                and last_rh_diff_pct is not None
                and last_rh_diff_pct <= rh_tol
            )
            if matched and dew_dp is not None:
                now = time.time()
                matched_since = matched_since or now
                stable_samples.append((now, float(dew_dp)))
                stable_samples = [(ts, value) for ts, value in stable_samples if now - ts <= (window_s + poll_s + 1.0)]
                window_samples = [(ts, value) for ts, value in stable_samples if now - ts <= window_s]
                if len(stable_samples) == 1:
                    self.host._log(
                        "Dewpoint meter temp/rh matched humidity generator: "
                        f"dew_temp={last_dew_temp_c} hgen_temp={last_hgen_temp_c} diff={last_temp_diff_c} "
                        f"dew_rh={last_dew_rh_pct} hgen_rh={last_hgen_rh_pct} diff={last_rh_diff_pct}"
                    )
                    self.host._log(
                        f"Dewpoint meter stability window started: dewpoint={last_dew_dp} "
                        f"window={int(window_s)}s tol={stability_tol}"
                    )
                elif (
                    matched_since is not None
                    and len(window_samples) >= min_window_samples
                    and (now - matched_since) >= window_s
                ):
                    span = self._span([value for _, value in window_samples]) if window_samples else float("inf")
                    if span <= stability_tol:
                        self.host._log(
                            f"Dewpoint meter stable: dewpoint={dew_dp} "
                            f"window={int(window_s)}s span={span:.4f} "
                            f"samples={len(window_samples)}"
                        )
                        return True
            else:
                if stable_samples:
                    self.host._log(
                        "Dewpoint meter temp/rh no longer matched humidity generator: "
                        f"dew_temp={last_dew_temp_c} hgen_temp={last_hgen_temp_c} diff={last_temp_diff_c} "
                        f"dew_rh={last_dew_rh_pct} hgen_rh={last_hgen_rh_pct} diff={last_rh_diff_pct}"
                    )
                matched_since = None
                stable_samples = []
            if time.time() - last_report >= 30.0:
                last_report = time.time()
                if not matched:
                    self.host._log(
                        "Dewpoint meter matching humidity generator... "
                        f"dew_temp={last_dew_temp_c} hgen_temp={last_hgen_temp_c} diff={last_temp_diff_c} "
                        f"dew_rh={last_dew_rh_pct} hgen_rh={last_hgen_rh_pct} diff={last_rh_diff_pct} "
                        f"tol=({temp_tol}C,{rh_tol}%) dewpoint={last_dew_dp}"
                    )
                elif not stable_samples or matched_since is None:
                    self.host._log(f"Dewpoint meter settling... dewpoint={last_dew_dp}")
                else:
                    now = time.time()
                    remain = max(0.0, window_s - (now - matched_since))
                    window_samples = [(ts, value) for ts, value in stable_samples if now - ts <= window_s]
                    span = self._span([value for _, value in window_samples]) if window_samples else float("inf")
                    sample_count = len(window_samples)
                    self.host._log(
                        f"Dewpoint meter observing stability... dewpoint={last_dew_dp} "
                        f"span={span:.4f} samples={sample_count}/{min_window_samples} "
                        f"remaining={int(remain)}s"
                    )
            time.sleep(poll_s)
        self.host._log(
            "Dewpoint meter stability timeout: "
            f"dewpoint={last_dew_dp} dew_temp={last_dew_temp_c} hgen_temp={last_hgen_temp_c} "
            f"temp_diff={last_temp_diff_c} dew_rh={last_dew_rh_pct} hgen_rh={last_hgen_rh_pct} "
            f"rh_diff={last_rh_diff_pct} tol=({temp_tol}C,{rh_tol}%)"
        )
        return False

    def capture_preseal_dewpoint_snapshot(self) -> None:
        dewpoint = self.host._device("dewpoint_meter")
        if dewpoint is None:
            self._set_preseal_dewpoint_snapshot(None)
            return
        pressure = None
        reader = self.host._make_pressure_reader()
        if reader is not None:
            pressure = reader()
        snapshot = self._read_dewpoint_snapshot(
            dewpoint,
            context="pre-seal dewpoint snapshot",
            log_failures=False,
        )
        payload = {
            "sample_ts": datetime.now(timezone.utc).isoformat(),
            "dewpoint_c": snapshot.get("dewpoint_c"),
            "temp_c": snapshot.get("temp_c"),
            "rh_pct": snapshot.get("rh_pct"),
            "pressure_hpa": pressure,
        }
        self._set_preseal_dewpoint_snapshot(payload)
        self.host._log(
            "Captured pre-seal dewpoint snapshot: "
            f"dewpoint={snapshot.get('dewpoint_c')} temp={snapshot.get('temp_c')} rh={snapshot.get('rh_pct')}"
        )

    def open_h2o_route_and_wait_ready(self, point: CalibrationPoint) -> bool:
        self.last_h2o_route_ready_evidence = []
        try:
            self._set_pressure_controller_vent(True, reason="during H2O route pre-seal preparation")
            self._record_h2o_route_ready_step(
                point,
                step="vent_on_before_h2o_path",
                result="ok",
                reason="pressure_controller_vent_on_requested",
            )
        except Exception as exc:
            self._record_h2o_route_ready_step(
                point,
                step="vent_on_before_h2o_path",
                result="fail",
                reason="vent_on_failed",
                error=exc,
            )
            raise
        try:
            h2o_path_opened = self._set_h2o_path(True, point)
        except Exception as exc:
            self._record_h2o_route_ready_step(
                point,
                step="set_h2o_path_open",
                result="fail",
                reason="h2o_path_open_exception",
                error=exc,
            )
            raise
        self._record_h2o_route_ready_step(
            point,
            step="set_h2o_path_open",
            result="ok" if h2o_path_opened else "fail",
            reason="h2o_path_opened" if h2o_path_opened else self._h2o_path_failure_reason(),
            details=self._h2o_path_evidence(h2o_path_opened),
        )
        if not h2o_path_opened:
            return False
        if not self._ensure_dewpoint_meter_ready_for_h2o_route(point):
            return False
        try:
            preseal_ok = self.wait_h2o_route_soak_before_seal(point)
        except Exception as exc:
            self._record_h2o_route_ready_step(
                point,
                step="preseal_soak",
                result="fail",
                reason="preseal_soak_exception",
                error=exc,
            )
            raise
        self._record_h2o_route_ready_step(
            point,
            step="preseal_soak",
            result="ok" if preseal_ok else "fail",
            reason="preseal_soak_complete" if preseal_ok else "preseal_soak_failed",
        )
        return bool(preseal_ok)

    def _ensure_dewpoint_meter_ready_for_h2o_route(self, point: CalibrationPoint) -> bool:
        dewpoint = self.host._device("dewpoint_meter")
        collect_only = self.host._collect_only_fast_path_enabled()
        if dewpoint is None:
            if collect_only:
                self._record_h2o_route_ready_step(
                    point,
                    step="dewpoint_meter_available",
                    result="skipped",
                    reason="collect_only_dewpoint_meter_unavailable_ignored",
                )
                self._record_h2o_route_ready_step(
                    point,
                    step="dewpoint_meter_open",
                    result="skipped",
                    reason="collect_only_dewpoint_meter_unavailable_ignored",
                )
                self._record_h2o_route_ready_step(
                    point,
                    step="dewpoint_initial_read",
                    result="skipped",
                    reason="collect_only_dewpoint_meter_unavailable_ignored",
                )
                self.host._log("Collect-only mode: dewpoint meter unavailable, continue without readiness check")
                return True
            self._record_h2o_route_ready_step(
                point,
                step="dewpoint_meter_available",
                result="fail",
                reason="dewpoint_meter_unavailable",
            )
            self._record_h2o_route_ready_step(
                point,
                step="dewpoint_meter_open",
                result="skipped",
                reason="dewpoint_meter_unavailable",
            )
            self._record_h2o_route_ready_step(
                point,
                step="dewpoint_initial_read",
                result="skipped",
                reason="dewpoint_meter_unavailable",
            )
            self.host._log("Dewpoint meter unavailable")
            return False
        self._record_h2o_route_ready_step(
            point,
            step="dewpoint_meter_available",
            result="ok",
            reason="dewpoint_meter_available",
        )
        opener = getattr(dewpoint, "open", None)
        if callable(opener):
            try:
                opener()
            except Exception as exc:
                if collect_only:
                    self._record_h2o_route_ready_step(
                        point,
                        step="dewpoint_meter_open",
                        result="skipped",
                        reason="collect_only_dewpoint_open_failed_ignored",
                        error=exc,
                    )
                    self._record_h2o_route_ready_step(
                        point,
                        step="dewpoint_initial_read",
                        result="skipped",
                        reason="collect_only_dewpoint_open_failed_ignored",
                    )
                    self.host._log(f"Collect-only mode: dewpoint meter open failed but ignored: {exc}")
                    return True
                self._record_h2o_route_ready_step(
                    point,
                    step="dewpoint_meter_open",
                    result="fail",
                    reason="dewpoint_open_failed",
                    error=exc,
                )
                self._record_h2o_route_ready_step(
                    point,
                    step="dewpoint_initial_read",
                    result="skipped",
                    reason="dewpoint_open_failed",
                )
                self.host._log(f"Dewpoint meter open failed: {exc}")
                return False
        self._record_h2o_route_ready_step(
            point,
            step="dewpoint_meter_open",
            result="ok",
            reason="dewpoint_meter_opened" if callable(opener) else "dewpoint_meter_open_not_required",
        )
        try:
            snapshot = self._read_dewpoint_snapshot(
                dewpoint,
                context="dewpoint meter initial read",
                log_failures=True,
            )
        except Exception as exc:
            if collect_only:
                self._record_h2o_route_ready_step(
                    point,
                    step="dewpoint_initial_read",
                    result="skipped",
                    reason="collect_only_dewpoint_initial_read_failed_ignored",
                    error=exc,
                )
                self.host._log(f"Collect-only mode: dewpoint meter initial read failed but ignored: {exc}")
                return True
            self._record_h2o_route_ready_step(
                point,
                step="dewpoint_initial_read",
                result="fail",
                reason="dewpoint_initial_read_failed",
                error=exc,
            )
            self.host._log(f"Dewpoint meter initial read failed: {exc}")
            return False
        self._record_h2o_route_ready_step(
            point,
            step="dewpoint_initial_read",
            result="ok",
            reason="dewpoint_initial_read_ok",
        )
        self.host._log(f"Dewpoint meter ready: dewpoint={snapshot.get('dewpoint_c')} temp={snapshot.get('temp_c')}")
        return True

    def _record_h2o_route_ready_step(
        self,
        point: CalibrationPoint,
        *,
        step: str,
        result: str,
        reason: str,
        error: Optional[BaseException] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        evidence: dict[str, Any] = {
            "route": "h2o",
            "point_index": getattr(point, "index", None),
            "step": str(step),
            "result": str(result),
            "reason": str(reason),
            "not_real_acceptance_evidence": True,
        }
        if details:
            evidence.update(details)
        if error is not None:
            evidence["error_class"] = type(error).__name__
            evidence["error_message"] = str(error)
        self.last_h2o_route_ready_evidence.append(evidence)
        recorder = getattr(getattr(self.host, "status_service", None), "record_route_trace", None)
        if callable(recorder):
            recorder(
                action="h2o_route_ready_step",
                route="h2o",
                point=point,
                point_index=getattr(point, "index", None),
                result=str(result),
                target={"step": str(step)},
                actual=evidence,
                message=str(reason),
            )

    def _h2o_path_evidence(self, h2o_path_opened: bool) -> dict[str, Any]:
        legacy_evidence = getattr(self.host, "_last_h2o_path_evidence", None)
        if isinstance(legacy_evidence, dict) and legacy_evidence:
            return dict(legacy_evidence)
        vrs = getattr(self.host, "valve_routing_service", None)
        evidence = getattr(vrs, "last_h2o_path_evidence", None) if vrs is not None else None
        if isinstance(evidence, dict) and evidence:
            return dict(evidence)
        return {
            "relay_command_sent": "unknown",
            "relay_command_result": "unknown",
            "h2o_path_return_value": bool(h2o_path_opened),
            "route_physical_state_match": "unknown",
            "relay_physical_mismatch": "unknown",
            "mismatched_channels": [],
            "h2o_path_open_verified": bool(h2o_path_opened),
            "h2o_path_open_failure_reason": "missing_h2o_path_evidence" if not h2o_path_opened else "",
        }

    def _h2o_path_failure_reason(self) -> str:
        evidence = self._h2o_path_evidence(False)
        reason = str(evidence.get("h2o_path_open_failure_reason") or "").strip()
        if reason:
            return reason
        if evidence.get("relay_physical_mismatch") is True:
            return "relay_physical_mismatch"
        if evidence.get("relay_command_sent") is False:
            return "relay_command_not_sent"
        return "h2o_path_open_failed"

    def _set_pressure_controller_vent(self, on: bool, *, reason: str) -> None:
        legacy = getattr(self.host, "_set_pressure_controller_vent", None)
        if callable(legacy):
            legacy(on, reason=reason)
            return
        pcs = getattr(self.host, "pressure_control_service", None)
        if pcs is not None:
            setter = getattr(pcs, "set_pressure_controller_vent", None)
            if callable(setter):
                setter(on, reason=reason)
                return
        self.host._log("DewpointAlignmentService: cannot set pressure controller vent (no adapter)")

    def _set_h2o_path(self, is_open: bool, point: CalibrationPoint) -> bool:
        legacy = getattr(self.host, "_set_h2o_path", None)
        if callable(legacy):
            result = legacy(is_open, point)
            return bool(result)
        vrs = getattr(self.host, "valve_routing_service", None)
        if vrs is not None:
            setter = getattr(vrs, "set_h2o_path", None)
            if callable(setter):
                return bool(setter(is_open, point))
        self.host._log("DewpointAlignmentService: cannot set H2O path (no adapter)")
        return False

    def _set_preseal_dewpoint_snapshot(self, value: Optional[dict[str, Any]]) -> None:
        self.run_state.humidity.preseal_dewpoint_snapshot = value
        setattr(self.host, "_preseal_dewpoint_snapshot", value)

    def _refresh_live_snapshots(self, *, force: bool = False, reason: str = "") -> bool:
        refresher = getattr(self.host, "_refresh_live_analyzer_snapshots", None)
        if callable(refresher):
            return bool(refresher(force=force, reason=reason))
        analyzer_service = getattr(self.host, "analyzer_fleet_service", None)
        refresher = getattr(analyzer_service, "refresh_live_snapshots", None)
        if callable(refresher):
            return bool(refresher(force=force, reason=reason))
        return False

    def _read_dewpoint_snapshot(
        self,
        dewpoint: Any,
        *,
        context: str,
        log_failures: bool,
    ) -> dict[str, Any]:
        return self.host._normalize_snapshot(
            read_device_snapshot_with_retry(
                dewpoint,
                host=self.host,
                context=context,
                required_keys=self.DEWPOINT_FRAME_KEYS,
                retry_on_empty=True,
                log_failures=log_failures,
            )
        )

    @staticmethod
    def _span(values: list[float]) -> float:
        if not values:
            return 0.0
        return float(max(values) - min(values))
