from __future__ import annotations

from dataclasses import dataclass, field
import time
from typing import Any, Optional

from ..models import CalibrationPoint
from ..orchestration_context import OrchestrationContext
from ..run_state import RunState
from ...utils import safe_get
from .sampling_service import read_device_snapshot_with_retry


@dataclass(frozen=True)
class HumidityWaitResult:
    ok: bool
    timed_out: bool = False
    target_temp_c: Optional[float] = None
    target_rh_pct: Optional[float] = None
    final_temp_c: Optional[float] = None
    final_rh_pct: Optional[float] = None
    stable_window_s: float = 0.0
    diagnostics: dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


class HumidityGeneratorService:
    """Humidity generator preparation and stabilization helpers."""

    HGEN_FRAME_KEYS = (
        "Tc",
        "Ts",
        "TA",
        "Temp",
        "temperature",
        "temp_c",
        "temperature_c",
        "Uw",
        "Ui",
        "Rh",
        "RH",
        "humidity",
        "Hum",
        "humidity_pct",
        "rh_pct",
    )

    def __init__(self, context: OrchestrationContext, run_state: RunState, *, host: Any) -> None:
        self.context = context
        self.run_state = run_state
        self.host = host

    def prepare_humidity_generator(self, point: CalibrationPoint) -> None:
        generator = self.host._device("humidity_generator")
        if generator is None:
            return
        target_temp = self.host._as_float(point.hgen_temp_c)
        target_rh = self.host._as_float(point.hgen_rh_pct)
        target_key = (target_temp, target_rh)
        target_changed = target_key != self.run_state.humidity.last_hgen_target
        if target_changed:
            self._set_hgen_target(target_key)
            self._set_hgen_ready(False)

        if target_temp is not None and target_changed:
            self.host._call_first(
                generator,
                ("set_target_temp", "set_temp_c", "set_temperature_c", "set_temperature"),
                target_temp,
            )
        if target_rh is not None and target_changed:
            self.host._call_first(
                generator,
                (
                    "set_relative_humidity_pct",
                    "set_rh_pct",
                    "set_humidity_pct",
                    "set_humidity",
                    "set_target_rh",
                ),
                target_rh,
            )
        for method_name, args in (("enable_control", (True,)), ("heat_on", ()), ("cool_on", ())):
            method = getattr(generator, method_name, None)
            if callable(method):
                try:
                    method(*args)
                except Exception as exc:
                    self.host._log(f"Humidity generator {method_name} failed: {exc}")
        state = "updated" if target_changed else "unchanged"
        self.host._log(
            f"Humidity generator target {state}: temp={target_temp}C rh={target_rh}% ctrl=on heat=on cool=on"
        )
        if target_changed:
            verify_readback = getattr(generator, "verify_target_readback", None)
            if callable(verify_readback):
                try:
                    readback = verify_readback(
                        target_temp_c=target_temp,
                        target_rh_pct=target_rh,
                    )
                    self.host._log(
                        "Humidity generator target readback: "
                        f"temp={readback.get('read_temp_c')}C/{readback.get('target_temp_c')}C "
                        f"rh={readback.get('read_rh_pct')}%/{readback.get('target_rh_pct')}% "
                        f"ok={readback.get('ok')}"
                    )
                except Exception as exc:
                    self.host._log(f"Humidity generator target readback failed: {exc}")
        ensure_run = getattr(generator, "ensure_run", None)
        if callable(ensure_run):
            try:
                result = ensure_run(
                    min_flow_lpm=float(self.host._cfg_get("workflow.humidity_generator.min_flow_lpm", 0.1)),
                    tries=int(self.host._cfg_get("workflow.humidity_generator.tries", 2)),
                    wait_s=float(self.host._cfg_get("workflow.humidity_generator.wait_s", 2.5)),
                    poll_s=float(self.host._cfg_get("workflow.humidity_generator.poll_s", 0.25)),
                )
                if not bool(getattr(result, "get", None) and result.get("ok")):
                    if isinstance(result, dict) and not result.get("ok", True):
                        self.host._log(f"Humidity generator ensure_run failed: {result}")
            except Exception as exc:
                self.host._log(f"Humidity generator ensure_run failed: {exc}")

    def read_humidity_generator_temp_rh(self) -> tuple[Optional[float], Optional[float]]:
        generator = self.host._device("humidity_generator")
        if generator is None:
            return None, None
        snapshot = self._read_humidity_generator_snapshot(generator, log_failures=False)
        data = safe_get(snapshot, "data", default=snapshot)
        if not isinstance(data, dict):
            data = snapshot
        return (
            self.host._pick_numeric(data, "Tc", "Ts", "TA", "Temp", "temperature", "temp_c", "temperature_c"),
            self.host._pick_numeric(data, "Uw", "Ui", "Rh", "RH", "humidity", "Hum", "humidity_pct", "rh_pct"),
        )

    def wait_humidity_generator_stable(self, point: CalibrationPoint) -> HumidityWaitResult:
        generator = self.host._device("humidity_generator")
        if generator is None:
            return HumidityWaitResult(ok=True, diagnostics={"skipped": "humidity generator unavailable"})
        if self.host._collect_only_fast_path_enabled():
            self.host._log("Collect-only mode: humidity-generator wait skipped")
            return HumidityWaitResult(ok=True, diagnostics={"skipped": "collect_only_fast_path"})
        target_temp = self.host._as_float(point.hgen_temp_c)
        target_rh = self.host._as_float(point.hgen_rh_pct)
        if target_temp is None and target_rh is None:
            return HumidityWaitResult(ok=True, target_temp_c=target_temp, target_rh_pct=target_rh)
        if (
            self.run_state.humidity.last_hgen_target == (target_temp, target_rh)
            and self.run_state.humidity.last_hgen_setpoint_ready
        ):
            self.host._log("Humidity generator setpoint already ready for current target, skip wait")
            return HumidityWaitResult(
                ok=True,
                target_temp_c=target_temp,
                target_rh_pct=target_rh,
                diagnostics={"reused_previous_stability": True},
            )

        cfg = self.host._cfg_get("workflow.stability.humidity_generator", {})
        if isinstance(cfg, dict) and not cfg.get("enabled", True):
            return HumidityWaitResult(ok=True, target_temp_c=target_temp, target_rh_pct=target_rh, diagnostics={"disabled": True})

        temp_tol = float(self.host._cfg_get("workflow.stability.humidity_generator.temp_tol_c", 1.0))
        rh_tol = float(self.host._cfg_get("workflow.stability.humidity_generator.rh_tol_pct", 1.0))
        window_s = float(
            self.host._cfg_get(
                "workflow.stability.humidity_generator.rh_stable_window_s",
                self.host._cfg_get("workflow.stability.humidity_generator.window_s", 60.0),
            )
        )
        span_tol = float(self.host._cfg_get("workflow.stability.humidity_generator.rh_stable_span_pct", 0.3))
        timeout_raw = float(self.host._cfg_get("workflow.stability.humidity_generator.timeout_s", 1800.0))
        timeout_s: Optional[float] = timeout_raw if timeout_raw > 0 else None
        poll_s = max(0.1, float(self.host._cfg_get("workflow.stability.humidity_generator.poll_s", 1.0)))
        start = time.time()
        last_report = 0.0
        in_band_since: Optional[float] = None
        rh_samples: list[tuple[float, float]] = []
        final_temp: Optional[float] = None
        final_rh: Optional[float] = None
        if timeout_s is None:
            self.host._log("Humidity generator wait timeout disabled; waiting until RH stabilizes")
        while True:
            self.host._check_stop()
            if timeout_s is not None and (time.time() - start) >= timeout_s:
                break
            self._refresh_live_snapshots(reason="humidity_generator_wait")
            temp_now, rh_now = self.read_humidity_generator_temp_rh()
            final_temp, final_rh = temp_now, rh_now
            temp_ok = target_temp is None or (temp_now is not None and abs(temp_now - target_temp) <= temp_tol)
            rh_ok = target_rh is None or (rh_now is not None and abs(rh_now - target_rh) <= rh_tol)
            if temp_ok and rh_ok:
                now = time.time()
                if in_band_since is None:
                    in_band_since = now
                    rh_samples = []
                if rh_now is not None:
                    rh_samples.append((now, float(rh_now)))
                    rh_samples = [(ts, value) for ts, value in rh_samples if now - ts <= window_s]
                if (
                    in_band_since is not None
                    and (now - in_band_since) >= window_s
                    and rh_samples
                    and self._span([value for _, value in rh_samples]) < span_tol
                ):
                    span = self._span([value for _, value in rh_samples])
                    self.host._log(
                        f"Humidity generator reached setpoint: temp={temp_now}C target={target_temp} "
                        f"rh={rh_now}% target={target_rh} span={span:.3f} window={int(window_s)}s"
                    )
                    self._set_hgen_ready(True)
                    return HumidityWaitResult(
                        ok=True,
                        target_temp_c=target_temp,
                        target_rh_pct=target_rh,
                        final_temp_c=temp_now,
                        final_rh_pct=rh_now,
                        stable_window_s=window_s,
                        diagnostics={"span": span, "temp_tol": temp_tol, "rh_tol": rh_tol, "span_tol": span_tol},
                    )
            else:
                if in_band_since is not None:
                    self.host._log(
                        f"Humidity left target band: temp={temp_now}C/{target_temp} tol=卤{temp_tol} "
                        f"rh={rh_now}%/{target_rh} tol=卤{rh_tol}; reset stability window"
                    )
                in_band_since = None
                rh_samples = []
            if time.time() - last_report >= 30.0:
                last_report = time.time()
                if in_band_since is None or not rh_samples:
                    self.host._log(
                        f"Humidity settling... temp={temp_now}C/{target_temp} rh={rh_now}%/{target_rh} "
                        f"window=0/{int(window_s)}s"
                    )
                else:
                    remain = max(0.0, window_s - (time.time() - in_band_since))
                    span = self._span([value for _, value in rh_samples])
                    self.host._log(
                        f"Humidity in target band, observing stability... temp={temp_now}C/{target_temp} "
                        f"rh={rh_now}%/{target_rh} span={span:.3f} remaining={int(remain)}s"
                    )
            time.sleep(poll_s)
        self.host._log("Humidity generator reach-setpoint timeout")
        return HumidityWaitResult(
            ok=False,
            timed_out=True,
            target_temp_c=target_temp,
            target_rh_pct=target_rh,
            final_temp_c=final_temp,
            final_rh_pct=final_rh,
            stable_window_s=window_s,
            diagnostics={"temp_tol": temp_tol, "rh_tol": rh_tol, "span_tol": span_tol},
            error="Humidity generator reach-setpoint timeout",
        )

    @staticmethod
    def _span(values: list[float]) -> float:
        if not values:
            return 0.0
        return max(values) - min(values)

    def _set_hgen_target(self, target: tuple[Optional[float], Optional[float]]) -> None:
        self.run_state.humidity.last_hgen_target = target
        setattr(self.host, "_last_hgen_target", target)

    def _set_hgen_ready(self, ready: bool) -> None:
        self.run_state.humidity.last_hgen_setpoint_ready = bool(ready)
        setattr(self.host, "_last_hgen_setpoint_ready", bool(ready))

    def _refresh_live_snapshots(self, *, force: bool = False, reason: str = "") -> bool:
        refresher = getattr(self.host, "_refresh_live_analyzer_snapshots", None)
        if callable(refresher):
            return bool(refresher(force=force, reason=reason))
        analyzer_service = getattr(self.host, "analyzer_fleet_service", None)
        refresher = getattr(analyzer_service, "refresh_live_snapshots", None)
        if callable(refresher):
            return bool(refresher(force=force, reason=reason))
        return False

    def _read_humidity_generator_snapshot(self, generator: Any, *, log_failures: bool) -> dict[str, Any]:
        return self.host._normalize_snapshot(
            read_device_snapshot_with_retry(
                generator,
                host=self.host,
                context="humidity generator temp/rh snapshot",
                required_keys=self.HGEN_FRAME_KEYS,
                retry_on_empty=True,
                log_failures=log_failures,
            )
        )
