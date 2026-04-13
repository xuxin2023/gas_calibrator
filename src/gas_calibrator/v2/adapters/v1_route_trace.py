"""V2 bridge wrapper for external V1 route tracing.

This module intentionally crosses into V1 runtime/logging code, but only as a
bridge tool. It is not part of the V2 main runtime boundary.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

from ...data.points import CalibrationPoint
from ...logging_utils import RunLogger
from ...workflow.runner import CalibrationRunner


def _as_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _as_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(round(float(value)))
    except Exception:
        return None


class V1RouteTraceWriter:
    """Best-effort JSONL trace writer for external V1 route tracing."""

    def __init__(self, logger: RunLogger) -> None:
        self.run_id = str(logger.run_id)
        self.path = Path(logger.run_dir) / "route_trace.jsonl"

    def record(
        self,
        *,
        route: str,
        point_index: Optional[int],
        point_tag: str,
        action: str,
        target: Optional[dict[str, Any]] = None,
        actual: Optional[dict[str, Any]] = None,
        relay_state: Optional[dict[str, Any]] = None,
        result: str = "ok",
        message: str = "",
    ) -> None:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "run_id": self.run_id,
            "route": str(route or "").strip().lower(),
            "point_index": point_index,
            "point_tag": str(point_tag or "").strip(),
            "action": str(action or "").strip(),
            "target": self._safe_dict(target),
            "actual": self._safe_dict(actual),
            "relay_state": self._safe_dict(relay_state),
            "result": str(result or "ok").strip().lower() or "ok",
            "message": str(message or ""),
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def _safe_dict(self, value: Optional[dict[str, Any]]) -> dict[str, Any]:
        if not isinstance(value, dict):
            return {}
        return {str(key): self._safe_value(item) for key, item in value.items()}

    def _safe_value(self, value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, dict):
            return {str(key): self._safe_value(item) for key, item in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._safe_value(item) for item in value]
        return str(value)


class TracedCalibrationRunner(CalibrationRunner):
    """External V1 runner wrapper that emits V2-aligned route trace events."""

    def __init__(self, config: dict[str, Any], devices: dict[str, Any], logger: RunLogger, log_fn, status_fn) -> None:
        super().__init__(config, devices, logger, log_fn, status_fn)
        self._trace_writer = V1RouteTraceWriter(logger)
        self._trace_route = ""
        self._trace_point: Optional[CalibrationPoint] = None

    @property
    def route_trace_path(self) -> Path:
        return self._trace_writer.path

    @contextmanager
    def _trace_context(self, route: str, point: Optional[CalibrationPoint] = None) -> Iterator[None]:
        previous_route = self._trace_route
        previous_point = self._trace_point
        if route:
            self._trace_route = str(route).strip().lower()
        if point is not None:
            self._trace_point = point
        try:
            yield
        finally:
            self._trace_route = previous_route
            self._trace_point = previous_point

    def _record_trace(
        self,
        *,
        action: str,
        route: str = "",
        point: Optional[CalibrationPoint] = None,
        point_tag: str = "",
        target: Optional[dict[str, Any]] = None,
        actual: Optional[dict[str, Any]] = None,
        result: str = "ok",
        message: str = "",
    ) -> None:
        try:
            resolved_route = str(route or self._trace_route or getattr(point or self._trace_point, "route", "") or "").strip().lower()
            resolved_point = point or self._trace_point
            resolved_index: Optional[int] = None
            if resolved_point is not None:
                try:
                    resolved_index = int(resolved_point.index)
                except Exception:
                    resolved_index = None
            resolved_tag = str(point_tag or self._point_tag_for(resolved_route, resolved_point)).strip()
            self._trace_writer.record(
                route=resolved_route,
                point_index=resolved_index,
                point_tag=resolved_tag,
                action=action,
                target=target,
                actual=actual,
                relay_state=self._relay_state_snapshot(),
                result=result,
                message=message,
            )
        except Exception:
            pass

    def _point_tag_for(self, route: str, point: Optional[CalibrationPoint]) -> str:
        if point is None:
            return ""
        route_text = str(route or getattr(point, "route", "") or "").strip().lower()
        if route_text == "h2o":
            try:
                return str(self._h2o_point_tag(point) or "").strip()
            except Exception:
                return ""
        if route_text == "co2":
            ppm = _as_int(getattr(point, "co2_ppm", None)) or 0
            pressure = _as_int(getattr(point, "target_pressure_hpa", None)) or 0
            group = str(getattr(point, "co2_group", "A") or "A").strip().upper() or "A"
            return f"co2_group{group.lower()}_{ppm}ppm_{pressure}hpa"
        return ""

    def _relay_state_snapshot(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for relay_name, count in (("relay", 16), ("relay_8", 8)):
            relay = self.devices.get(relay_name)
            if relay is None or not hasattr(relay, "read_coils"):
                continue
            try:
                bits = relay.read_coils(0, count)
                out[relay_name] = list(bits[:count]) if bits is not None else []
            except Exception as exc:
                out[f"{relay_name}_error"] = str(exc)
        return out

    def _temperature_actual(self) -> dict[str, Any]:
        actual: dict[str, Any] = {}
        chamber = self.devices.get("temp_chamber")
        thermometer = self.devices.get("thermometer")
        if chamber is not None:
            try:
                actual["temp_c"] = _as_float(chamber.read_temp_c())
            except Exception:
                pass
        if thermometer is not None:
            try:
                actual["thermometer_temp_c"] = _as_float(thermometer.read_temp_c())
            except Exception:
                pass
        return actual

    def _humidity_actual(self) -> dict[str, Any]:
        actual: dict[str, Any] = {}
        try:
            temp_c, rh_pct = self._read_humidity_generator_temp_rh()
            if temp_c is not None:
                actual["temp_c"] = temp_c
            if rh_pct is not None:
                actual["humidity_pct"] = rh_pct
        except Exception:
            pass
        return actual

    def _dewpoint_actual(self) -> dict[str, Any]:
        actual: dict[str, Any] = {}
        dew = self.devices.get("dewpoint")
        if dew is None:
            return actual
        try:
            data = dew.get_current()
        except Exception:
            return actual
        if isinstance(data, dict):
            for key in ("dewpoint_c", "temp_c", "rh_pct", "pressure_hpa"):
                value = _as_float(data.get(key))
                if value is not None:
                    actual[key] = value
        return actual

    def _pressure_actual(self) -> dict[str, Any]:
        actual: dict[str, Any] = {}
        gauge = self.devices.get("pressure_gauge")
        pace = self.devices.get("pace")
        if gauge is not None:
            try:
                actual["pressure_gauge_hpa"] = _as_float(gauge.read_pressure())
            except Exception:
                pass
        if pace is not None:
            try:
                actual["pressure_hpa"] = _as_float(pace.read_pressure())
            except Exception:
                pass
        return actual

    def _result_for_bool(self, ok: bool) -> str:
        if ok:
            return "ok"
        if self.stop_event.is_set():
            return "stopped"
        return "timeout"

    def _run_h2o_group(self, points: list[CalibrationPoint], pressure_points: Optional[list[CalibrationPoint]] = None) -> None:
        lead = points[0] if points else None
        with self._trace_context("h2o", lead):
            return super()._run_h2o_group(points, pressure_points=pressure_points)

    def _run_h2o_point(self, point: CalibrationPoint, prepared: bool = False) -> None:
        with self._trace_context("h2o", point):
            return super()._run_h2o_point(point, prepared=prepared)

    def _run_co2_point(self, point: CalibrationPoint, pressure_points: Optional[list[CalibrationPoint]] = None) -> None:
        with self._trace_context("co2", point):
            return super()._run_co2_point(point, pressure_points=pressure_points)

    def _set_h2o_path(self, is_open: bool, point: Optional[CalibrationPoint] = None) -> None:
        try:
            super()._set_h2o_path(is_open, point)
            self._record_trace(
                action="set_h2o_path",
                route="h2o",
                point=point,
                target={"open": bool(is_open)},
                actual={"open": bool(is_open)},
            )
        except Exception as exc:
            self._record_trace(
                action="set_h2o_path",
                route="h2o",
                point=point,
                target={"open": bool(is_open)},
                result="fail",
                message=str(exc),
            )
            raise

    def _set_co2_route_baseline(self, *, reason: str = "") -> None:
        try:
            super()._set_co2_route_baseline(reason=reason)
            self._record_trace(action="route_baseline", route="co2", message=reason)
        except Exception as exc:
            self._record_trace(action="route_baseline", route="co2", result="fail", message=str(exc))
            raise

    def _set_valves_for_co2(self, point: CalibrationPoint) -> None:
        try:
            super()._set_valves_for_co2(point)
            self._record_trace(
                action="set_co2_valves",
                route="co2",
                point=point,
                target={"co2_ppm": _as_float(point.co2_ppm), "pressure_hpa": _as_float(point.target_pressure_hpa)},
            )
        except Exception as exc:
            self._record_trace(action="set_co2_valves", route="co2", point=point, result="fail", message=str(exc))
            raise

    def _set_pressure_controller_vent(self, vent_on: bool, reason: str = "") -> None:
        try:
            super()._set_pressure_controller_vent(vent_on, reason=reason)
            self._record_trace(
                action="set_vent",
                target={"vent_on": bool(vent_on)},
                result="ok",
                message=reason,
                actual=self._pressure_actual(),
            )
        except Exception as exc:
            self._record_trace(
                action="set_vent",
                target={"vent_on": bool(vent_on)},
                result="fail",
                message=str(exc),
            )
            raise

    def _set_temperature_for_point(self, point: CalibrationPoint, *, phase: str, point_tag: str = "") -> bool:
        ok = super()._set_temperature_for_point(point, phase=phase, point_tag=point_tag)
        self._record_trace(
            action="wait_temperature",
            route=phase,
            point=point,
            point_tag=point_tag,
            target={"temp_c": _as_float(point.temp_chamber_c)},
            actual=self._temperature_actual(),
            result=self._result_for_bool(ok),
        )
        return ok

    def _wait_humidity_generator_stable(self, point: CalibrationPoint) -> bool:
        ok = super()._wait_humidity_generator_stable(point)
        self._record_trace(
            action="wait_humidity",
            route="h2o",
            point=point,
            target={"temp_c": _as_float(point.hgen_temp_c), "humidity_pct": _as_float(point.hgen_rh_pct)},
            actual=self._humidity_actual(),
            result=self._result_for_bool(ok),
        )
        return ok

    def _wait_humidity_generator_dewpoint_stable(self) -> bool:
        ok = super()._wait_humidity_generator_dewpoint_stable()
        self._record_trace(
            action="wait_humidity",
            route="h2o",
            actual={"dewpoint_c": self._read_humidity_generator_dewpoint()},
            result=self._result_for_bool(ok),
            message="humidity generator dewpoint stability",
        )
        return ok

    def _open_h2o_route_and_wait_ready(self, point: CalibrationPoint) -> bool:
        ok = super()._open_h2o_route_and_wait_ready(point)
        self._record_trace(
            action="wait_route_ready",
            route="h2o",
            point=point,
            target={"pressure_hpa": _as_float(point.target_pressure_hpa)},
            actual=self._dewpoint_actual(),
            result=self._result_for_bool(ok),
        )
        return ok

    def _wait_dewpoint_alignment_stable(self, point: Optional[CalibrationPoint] = None) -> bool:
        ok = super()._wait_dewpoint_alignment_stable(point)
        self._record_trace(
            action="wait_dewpoint",
            route="h2o",
            point=point,
            target={
                "temp_c": _as_float(getattr(point, "hgen_temp_c", None)),
                "humidity_pct": _as_float(getattr(point, "hgen_rh_pct", None)),
                "pressure_hpa": _as_float(getattr(point, "target_pressure_hpa", None)),
            },
            actual=self._dewpoint_actual(),
            result=self._result_for_bool(ok),
        )
        return ok

    def _wait_co2_route_soak_before_seal(self, point: CalibrationPoint) -> bool:
        ok = super()._wait_co2_route_soak_before_seal(point)
        self._record_trace(
            action="wait_route_soak",
            route="co2",
            point=point,
            target={"co2_ppm": _as_float(point.co2_ppm)},
            actual=self._pressure_actual(),
            result=self._result_for_bool(ok),
        )
        return ok

    def _pressurize_and_hold(self, point: CalibrationPoint, route: str = "co2") -> bool:
        ok = super()._pressurize_and_hold(point, route=route)
        self._record_trace(
            action="seal_route",
            route=route,
            point=point,
            target={"pressure_hpa": _as_float(point.target_pressure_hpa)},
            actual=self._pressure_actual(),
            result=self._result_for_bool(ok),
        )
        return ok

    def _set_pressure_to_target(self, point: CalibrationPoint, *, recovery_attempted: bool = False) -> bool:
        ok = super()._set_pressure_to_target(point, recovery_attempted=recovery_attempted)
        self._record_trace(
            action="set_pressure",
            point=point,
            target={"pressure_hpa": _as_float(point.target_pressure_hpa), "recovery_attempted": bool(recovery_attempted)},
            actual=self._pressure_actual(),
            result=self._result_for_bool(ok),
        )
        return ok

    def _wait_after_pressure_stable_before_sampling(self, point: CalibrationPoint) -> bool:
        ok = super()._wait_after_pressure_stable_before_sampling(point)
        self._record_trace(
            action="wait_post_pressure",
            point=point,
            target={"pressure_hpa": _as_float(point.target_pressure_hpa)},
            actual=self._pressure_actual(),
            result=self._result_for_bool(ok),
        )
        return ok

    def _sample_and_log(self, point: CalibrationPoint, phase: str = "", point_tag: str = "") -> None:
        count, interval = self._sampling_params(phase=phase)
        self._record_trace(
            action="sample_start",
            route=phase,
            point=point,
            point_tag=point_tag,
            target={"sample_count": int(count), "interval_s": float(interval)},
            actual=self._pressure_actual() | self._temperature_actual(),
        )
        try:
            super()._sample_and_log(point, phase=phase, point_tag=point_tag)
            self._record_trace(
                action="sample_end",
                route=phase,
                point=point,
                point_tag=point_tag,
                target={"sample_count": int(count)},
                actual={"sample_count": int(count)},
            )
        except Exception as exc:
            self._record_trace(
                action="sample_end",
                route=phase,
                point=point,
                point_tag=point_tag,
                target={"sample_count": int(count)},
                result="fail",
                message=str(exc),
            )
            raise

    def _cleanup_h2o_route(self, point: CalibrationPoint, *, reason: str = "") -> None:
        try:
            super()._cleanup_h2o_route(point, reason=reason)
            self._record_trace(action="cleanup", route="h2o", point=point, message=reason)
        except Exception as exc:
            self._record_trace(action="cleanup", route="h2o", point=point, result="fail", message=str(exc))
            raise

    def _cleanup_co2_route(self, *, reason: str = "") -> None:
        try:
            super()._cleanup_co2_route(reason=reason)
            self._record_trace(action="cleanup", route="co2", message=reason)
        except Exception as exc:
            self._record_trace(action="cleanup", route="co2", result="fail", message=str(exc))
            raise
