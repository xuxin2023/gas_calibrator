"""
Shared-state simulated devices for V2.

These simulation devices intentionally use one lightweight plant state so the
bench/smoke flow sees a single virtual environment instead of unrelated fake
devices.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import re
import time
from typing import Any, Dict, Optional


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, float(value)))


def _dewpoint_from_temp_rh(temp_c: float, rh_pct: float) -> float:
    # Simple, explainable approximation that is sufficient for simulation.
    return float(temp_c) - (100.0 - _clamp(rh_pct, 0.0, 100.0)) / 5.0


def _rh_from_temp_dewpoint(temp_c: float, dewpoint_c: float) -> float:
    return _clamp(100.0 - 5.0 * (float(temp_c) - float(dewpoint_c)), 0.0, 100.0)


@dataclass
class SimulationPlantState:
    temperature_c: float = 25.0
    target_temperature_c: float = 25.0
    humidity_pct: float = 35.0
    target_humidity_pct: float = 35.0
    dewpoint_c: float = 5.0
    pressure_hpa: float = 1000.0
    target_pressure_hpa: float = 1000.0
    ambient_pressure_hpa: float = 1000.0
    route: str = "ambient"
    analyzer_co2_ppm: float = 400.0
    running: bool = False
    vent_on: bool = True
    dynamic_protocol: bool = False
    relay_states: Dict[str, Dict[int, bool]] = field(default_factory=dict)
    logical_valve_states: Dict[int, bool] = field(default_factory=dict)
    last_route_command: Optional[str] = None

    def sync(self) -> None:
        if not self.dynamic_protocol:
            self.temperature_c = float(self.target_temperature_c)
            self.humidity_pct = _clamp(self.target_humidity_pct, 0.0, 100.0)
            self.pressure_hpa = float(self.ambient_pressure_hpa if self.vent_on else self.target_pressure_hpa)
        else:
            self.temperature_c = float(self.temperature_c)
            self.humidity_pct = _clamp(self.humidity_pct, 0.0, 100.0)
            self.pressure_hpa = float(self.pressure_hpa)
        self.dewpoint_c = _dewpoint_from_temp_rh(self.temperature_c, self.humidity_pct)
        self.route = self._infer_route()

    def set_temperature_target(self, value: float) -> None:
        self.target_temperature_c = float(value)
        self.sync()

    def set_humidity_target(self, value: float) -> None:
        self.target_humidity_pct = float(value)
        self.sync()

    def set_dewpoint_target(self, value: float) -> None:
        self.dewpoint_c = float(value)
        self.target_humidity_pct = _rh_from_temp_dewpoint(self.target_temperature_c, self.dewpoint_c)
        self.sync()

    def set_pressure_target(self, value: float) -> None:
        self.target_pressure_hpa = float(value)
        self.sync()

    def set_valve_state(self, channel: int, state: bool, *, device_name: str = "relay") -> None:
        relay_name = str(device_name or "relay")
        self.relay_states.setdefault(relay_name, {})[int(channel)] = bool(state)
        self.sync()

    def set_logical_valve_state(self, logical_valve: int, state: bool) -> None:
        self.logical_valve_states[int(logical_valve)] = bool(state)
        self.sync()

    def set_route_command(self, command: str) -> None:
        text = str(command or "").strip()
        self.last_route_command = text
        lowered = text.lower()
        if lowered in {"ambient", "h2o", "co2"}:
            self.route = lowered
            return

        match = re.search(r"channel\s*=\s*(\d+).*state\s*=\s*(on|off|true|false|1|0)", lowered)
        if match:
            channel = int(match.group(1))
            state = match.group(2) in {"on", "true", "1"}
            self.set_valve_state(channel, state)

    def analyzer_snapshot(self) -> Dict[str, Any]:
        self.sync()
        co2_ppm = self._co2_ppm_for_route()
        h2o_mmol = self._h2o_mmol_for_route()
        return {
            "route": self.route,
            "co2_ppm": co2_ppm,
            "co2_signal": co2_ppm,
            "co2_ratio_f": co2_ppm / 1000.0,
            "co2_ratio_raw": co2_ppm / 1000.0,
            "h2o_mmol": h2o_mmol,
            "h2o_signal": h2o_mmol,
            "h2o_ratio_f": h2o_mmol / 100.0,
            "h2o_ratio_raw": h2o_mmol / 100.0,
            "temperature_c": self.temperature_c,
            "temp_c": self.temperature_c,
            "chamber_temp_c": self.temperature_c,
            "case_temp_c": self.temperature_c,
            "cell_temp_raw_c": self.temperature_c,
            "shell_temp_raw_c": self.temperature_c,
            "pressure_hpa": self.pressure_hpa,
            "dewpoint_c": self.dewpoint_c,
            "rh_pct": self.humidity_pct,
            "humidity_pct": self.humidity_pct,
        }

    def _infer_route(self) -> str:
        open_logical_valves = {channel for channel, state in self.logical_valve_states.items() if state}
        if open_logical_valves:
            if open_logical_valves & {8, 9, 10}:
                return "h2o"
            return "co2"

        open_channels = {
            (str(device_name), int(channel))
            for device_name, channels in self.relay_states.items()
            for channel, state in channels.items()
            if state
        }
        if ("relay_8", 8) in open_channels or ("relay_8", 1) in open_channels or ("relay_8", 2) in open_channels:
            return "h2o"
        if (
            ("relay_8", 3) in open_channels
            or any(name == "relay" and channel in {1, 2, 3, 4, 5, 6, 7, 8, 15, 16} for name, channel in open_channels)
        ):
            return "co2"
        return "ambient"

    def _co2_ppm_for_route(self) -> float:
        if self.route == "co2":
            return float(self.analyzer_co2_ppm)
        if self.route == "h2o":
            return 0.0
        return float(self.analyzer_co2_ppm) * 0.1

    def _h2o_mmol_for_route(self) -> float:
        if self.route == "h2o":
            return max(0.0, self.humidity_pct * 0.2)
        if self.route == "co2":
            return max(0.0, self.humidity_pct * 0.02)
        return max(0.0, self.humidity_pct * 0.05)


class SimulatedBaseDevice:
    def __init__(self, port: str = "SIM", plant_state: Optional[SimulationPlantState] = None, **kwargs: Any) -> None:
        self.port = port
        self.kwargs = dict(kwargs)
        self.connected = False
        self.plant_state = plant_state or SimulationPlantState()

    def open(self) -> None:
        self.connected = True

    def connect(self) -> bool:
        self.connected = True
        return True

    def close(self) -> None:
        self.connected = False

    def status(self) -> Dict[str, Any]:
        return {"connected": self.connected, "port": self.port}

    def selftest(self) -> Dict[str, Any]:
        return {"ok": True, "connected": self.connected}


class SimulatedPressureController(SimulatedBaseDevice):
    def __init__(self, port: str = "SIM-PC", pressure_hpa: float = 1000.0, **kwargs: Any) -> None:
        super().__init__(port=port, **kwargs)
        self.plant_state.pressure_hpa = float(pressure_hpa)
        self.plant_state.target_pressure_hpa = float(pressure_hpa)
        self.plant_state.ambient_pressure_hpa = float(pressure_hpa)
        self.plant_state.sync()

    def enter_atmosphere_mode(self, **kwargs: Any) -> None:
        self.plant_state.vent_on = True
        self.plant_state.sync()

    def exit_atmosphere_mode(self, **kwargs: Any) -> None:
        self.plant_state.vent_on = False
        self.plant_state.sync()

    def enable_control_output(self) -> None:
        return None

    def set_output_mode_active(self) -> None:
        return None

    def set_output(self, enabled: bool) -> None:
        if not bool(enabled):
            self.plant_state.vent_on = True
            self.plant_state.sync()

    def set_isolation_open(self, is_open: bool) -> None:
        return None

    def vent(self, enabled: bool) -> None:
        self.plant_state.vent_on = bool(enabled)
        self.plant_state.sync()

    def set_pressure_hpa(self, value: float) -> None:
        self.plant_state.vent_on = False
        self.plant_state.set_pressure_target(value)

    def set_pressure(self, value: float) -> None:
        self.set_pressure_hpa(value)

    def set_setpoint(self, value: float) -> None:
        self.set_pressure_hpa(value)

    def read_pressure(self) -> float:
        self.plant_state.sync()
        return self.plant_state.pressure_hpa

    def get_in_limits(self) -> tuple[float, int]:
        self.plant_state.sync()
        current = self.plant_state.pressure_hpa
        target = self.plant_state.ambient_pressure_hpa if self.plant_state.vent_on else self.plant_state.target_pressure_hpa
        in_limits = abs(current - target) <= 0.5
        return current, 1 if in_limits else 0

    def status(self) -> Dict[str, Any]:
        payload = super().status()
        self.plant_state.sync()
        payload["pressure_hpa"] = self.plant_state.pressure_hpa
        payload["vent_on"] = self.plant_state.vent_on
        return payload


class SimulatedPressureMeter(SimulatedPressureController):
    def __init__(self, port: str = "SIM-PM", pressure_hpa: float = 1000.0, **kwargs: Any) -> None:
        super().__init__(port=port, pressure_hpa=pressure_hpa, **kwargs)
        self._continuous_pressure_active = False
        self._continuous_pressure_mode = ""
        self._continuous_pressure_sequence_id = 0

    def pressure_continuous_active(self) -> bool:
        return bool(self._continuous_pressure_active)

    def start_pressure_continuous(self, mode: str = "P4", clear_buffer: bool = True) -> bool:
        self._continuous_pressure_active = True
        self._continuous_pressure_mode = str(mode or "P4").strip().upper() or "P4"
        if clear_buffer:
            self._continuous_pressure_sequence_id = 0
        return True

    def stop_pressure_continuous(self) -> bool:
        self._continuous_pressure_active = False
        return True

    def read_pressure_continuous_latest(
        self,
        drain_s: float = 0.0,
        read_timeout_s: float = 0.0,
    ) -> Dict[str, Any]:
        self.plant_state.sync()
        self._continuous_pressure_sequence_id += 1
        pressure = float(self.plant_state.pressure_hpa)
        return {
            "pressure_hpa": pressure,
            "source": "digital_pressure_gauge_continuous",
            "monotonic_timestamp": time.monotonic(),
            "raw_line": f"{self._continuous_pressure_mode or 'P4'} {pressure:.3f}",
            "sequence_id": self._continuous_pressure_sequence_id,
        }


class SimulatedDewpointMeter(SimulatedBaseDevice):
    def __init__(
        self,
        port: str = "SIM-DP",
        dewpoint_c: float = 5.0,
        temp_c: float = 25.0,
        rh_pct: float = 35.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(port=port, **kwargs)
        self.plant_state.target_temperature_c = float(temp_c)
        self.plant_state.target_humidity_pct = float(rh_pct)
        self.plant_state.dewpoint_c = float(dewpoint_c)
        self.plant_state.sync()

    def get_current(self) -> Dict[str, Any]:
        self.plant_state.sync()
        return {
            "dewpoint_c": self.plant_state.dewpoint_c,
            "temp_c": self.plant_state.temperature_c,
            "rh_pct": self.plant_state.humidity_pct,
        }

    def status(self) -> Dict[str, Any]:
        payload = super().status()
        payload.update(self.get_current())
        return payload


class SimulatedHumidityGenerator(SimulatedBaseDevice):
    def __init__(
        self,
        port: str = "SIM-HG",
        humidity_pct: float = 35.0,
        temp_c: float = 25.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(port=port, **kwargs)
        self.plant_state.target_humidity_pct = float(humidity_pct)
        self.plant_state.target_temperature_c = float(temp_c)
        self.plant_state.sync()

    def set_relative_humidity_pct(self, value: float) -> None:
        self.plant_state.set_humidity_target(value)

    def set_rh_pct(self, value: float) -> None:
        self.set_relative_humidity_pct(value)

    def set_humidity_pct(self, value: float) -> None:
        self.set_relative_humidity_pct(value)

    def set_humidity(self, value: float) -> None:
        self.set_relative_humidity_pct(value)

    def set_target_rh(self, value: float) -> None:
        self.set_relative_humidity_pct(value)

    def set_temp_c(self, value: float) -> None:
        self.plant_state.set_temperature_target(value)

    def set_temperature_c(self, value: float) -> None:
        self.set_temp_c(value)

    def read_humidity_pct(self) -> float:
        self.plant_state.sync()
        return self.plant_state.humidity_pct

    def fetch_all(self) -> Dict[str, Any]:
        self.plant_state.sync()
        return {
            "data": {
                "humidity_pct": self.plant_state.humidity_pct,
                "rh_pct": self.plant_state.humidity_pct,
                "temp_c": self.plant_state.temperature_c,
                "dewpoint_c": self.plant_state.dewpoint_c,
                "route": self.plant_state.route,
            }
        }

    def status(self) -> Dict[str, Any]:
        payload = super().status()
        self.plant_state.sync()
        payload.update(
            {
                "humidity_pct": self.plant_state.humidity_pct,
                "temp_c": self.plant_state.temperature_c,
                "dewpoint_c": self.plant_state.dewpoint_c,
            }
        )
        return payload


class SimulatedTemperatureChamber(SimulatedBaseDevice):
    def __init__(
        self,
        port: str = "SIM-TC",
        temperature_c: float = 25.0,
        humidity_pct: float = 40.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(port=port, **kwargs)
        self.plant_state.target_temperature_c = float(temperature_c)
        self.plant_state.target_humidity_pct = float(humidity_pct)
        self.plant_state.sync()

    def set_temp_c(self, value: float) -> None:
        self.plant_state.set_temperature_target(value)

    def set_temperature_c(self, value: float) -> None:
        self.set_temp_c(value)

    def set_temperature(self, value: float) -> None:
        self.set_temp_c(value)

    def start(self) -> None:
        self.plant_state.running = True

    def stop(self) -> None:
        self.plant_state.running = False

    def read_temp_c(self) -> float:
        self.plant_state.sync()
        return self.plant_state.temperature_c

    def read_rh_pct(self) -> float:
        self.plant_state.sync()
        return self.plant_state.humidity_pct

    def status(self) -> Dict[str, Any]:
        payload = super().status()
        self.plant_state.sync()
        payload.update(
            {
                "temperature_c": self.plant_state.temperature_c,
                "humidity_pct": self.plant_state.humidity_pct,
                "running": self.plant_state.running,
            }
        )
        return payload


class SimulatedRelay(SimulatedBaseDevice):
    def __init__(self, port: str = "SIM-RLY", **kwargs: Any) -> None:
        super().__init__(port=port, **kwargs)
        self.route: Optional[str] = None

    def set_valve(self, channel: int, state: bool) -> None:
        self.plant_state.set_valve_state(int(channel), bool(state))

    def select_route(self, value: Any) -> None:
        self.route = str(value)
        self.plant_state.set_route_command(self.route)

    def set_route(self, value: Any) -> None:
        self.select_route(value)

    def switch_route(self, value: Any) -> None:
        self.select_route(value)

    def status(self) -> Dict[str, Any]:
        payload = super().status()
        payload["route"] = self.route
        payload["plant_route"] = self.plant_state.route
        return payload


class SimulatedGasAnalyzer(SimulatedBaseDevice):
    def __init__(
        self,
        port: str = "SIM-GA",
        co2_signal: float = 400.0,
        h2o_signal: float = 10.0,
        temperature_c: float = 25.0,
        chamber_temp_c: Optional[float] = None,
        case_temp_c: Optional[float] = None,
        pressure_hpa: float = 1000.0,
        dewpoint_c: float = 5.0,
        mode2_stream: str = "continuous_ok",
        active_send: bool = True,
        software_version: str = "v5_plus",
        device_id: str = "FFF",
        **kwargs: Any,
    ) -> None:
        super().__init__(port=port, **kwargs)
        self.plant_state.analyzer_co2_ppm = float(co2_signal)
        self.plant_state.target_temperature_c = float(temperature_c)
        self.plant_state.target_pressure_hpa = float(pressure_hpa)
        self.plant_state.dewpoint_c = float(dewpoint_c)
        self.sample_count = 0
        self._chamber_temp_override = chamber_temp_c
        self._case_temp_override = case_temp_c
        self.mode = 2
        self._active_send = bool(active_send)
        self._ftd_hz = 5
        self._average_filter = 1
        self._comm_way = 2
        self._mode2_stream = str(mode2_stream or "continuous_ok").strip().lower()
        self.software_version = str(software_version or "v5_plus")
        self.device_id = str(device_id or "FFF")
        self.serial = f"SIM-{self.device_id}"
        self._mode2_frame_index = 0
        self.plant_state.sync()

    def fetch_all(self) -> Dict[str, Any]:
        self.sample_count += 1
        data = self.plant_state.analyzer_snapshot()
        if self._chamber_temp_override is not None:
            data["chamber_temp_c"] = float(self._chamber_temp_override)
            data["cell_temp_raw_c"] = float(self._chamber_temp_override)
        if self._case_temp_override is not None:
            data["case_temp_c"] = float(self._case_temp_override)
            data["shell_temp_raw_c"] = float(self._case_temp_override)
        data["mode"] = 2
        data["device_id"] = self.device_id
        data["serial"] = self.serial
        data["software_version"] = self.software_version
        return {"data": data}

    def read(self) -> Dict[str, Any]:
        return self.fetch_all()

    def read_latest_data(self, *args: Any, **kwargs: Any) -> str:
        return self._mode2_frame()

    def read_data_passive(self, *args: Any, **kwargs: Any) -> str:
        return self._mode2_frame()

    def read_data_active(self, *args: Any, **kwargs: Any) -> str:
        return self._mode2_frame()

    def _drain_stream_lines(self, *args: Any, **kwargs: Any) -> list[str]:
        line = self._mode2_frame()
        return [line] if line else []

    def parse_line_mode2(self, line: str) -> Dict[str, Any]:
        text = str(line or "").strip()
        if not text:
            return {}
        data: Dict[str, Any] = {}
        for item in text.split(","):
            if "=" not in item:
                continue
            key, raw_value = item.split("=", 1)
            key = key.strip()
            value = raw_value.strip()
            if key in {"device_id", "serial", "software_version"}:
                data[key] = value
                continue
            try:
                numeric = float(value)
            except Exception:
                data[key] = value
            else:
                data[key] = int(numeric) if numeric.is_integer() else numeric
        data.setdefault("mode", 2)
        return data

    def parse_line(self, line: str) -> Dict[str, Any]:
        return self.parse_line_mode2(line)

    def set_comm_way_with_ack(self, value: Any, *_args: Any, **_kwargs: Any) -> bool:
        self._comm_way = int(value)
        return True

    def set_comm_way(self, value: Any) -> None:
        self._comm_way = int(value)

    def get_comm_way(self) -> int:
        return int(self._comm_way)

    def set_mode_with_ack(self, value: Any, *_args: Any, **_kwargs: Any) -> bool:
        self.mode = int(value)
        return True

    def set_mode(self, value: Any) -> None:
        self.mode = int(value)

    def set_active_send_with_ack(self, enabled: bool, *_args: Any, **_kwargs: Any) -> bool:
        self._active_send = bool(enabled)
        return True

    def set_active_send(self, enabled: bool) -> None:
        self._active_send = bool(enabled)

    def get_active_send(self) -> bool:
        return bool(self._active_send)

    def set_ftd_with_ack(self, value: Any, *_args: Any, **_kwargs: Any) -> bool:
        self._ftd_hz = int(value)
        return True

    def set_ftd(self, value: Any) -> None:
        self._ftd_hz = int(value)

    def set_average_filter_channel_with_ack(self, _channel: Any, value: Any, *_args: Any, **_kwargs: Any) -> bool:
        self._average_filter = int(value)
        return True

    def set_average_filter_channel(self, _channel: Any, value: Any) -> None:
        self._average_filter = int(value)

    def set_average_filter_with_ack(self, value: Any, *_args: Any, **_kwargs: Any) -> bool:
        self._average_filter = int(value)
        return True

    def set_average_filter(self, value: Any) -> None:
        self._average_filter = int(value)

    def set_warning_phase(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def get_status(self) -> Dict[str, Any]:
        return self.status()

    def status(self) -> Dict[str, Any]:
        payload = super().status()
        payload.update(self.fetch_all()["data"])
        payload["active_send"] = bool(self._active_send)
        payload["ftd_hz"] = int(self._ftd_hz)
        return payload

    def _mode2_frame(self) -> str:
        self._mode2_frame_index += 1
        stream = self._mode2_stream
        if stream == "no_response":
            return ""
        if stream == "partial_frame":
            if self._mode2_frame_index != 3:
                return f"device_id={self.device_id},mode=2,co2_ppm"
        snapshot = self.fetch_all()["data"]
        return (
            f"device_id={self.device_id},mode=2,serial={self.serial},software_version={self.software_version},"
            f"co2_ppm={float(snapshot['co2_ppm']):.3f},h2o_mmol={float(snapshot['h2o_mmol']):.3f},"
            f"temp_c={float(snapshot['temp_c']):.3f},chamber_temp_c={float(snapshot['chamber_temp_c']):.3f},"
            f"case_temp_c={float(snapshot['case_temp_c']):.3f},pressure_hpa={float(snapshot['pressure_hpa']):.3f}"
        )


class SimulatedThermometer(SimulatedBaseDevice):
    def __init__(self, port: str = "SIM-TH", temperature_c: float = 25.0, **kwargs: Any) -> None:
        super().__init__(port=port, **kwargs)
        self.plant_state.target_temperature_c = float(temperature_c)
        self.plant_state.sync()

    def read_current(self) -> Dict[str, Any]:
        self.plant_state.sync()
        return {"temp_c": self.plant_state.temperature_c, "ok": True}

    def read_temp_c(self) -> float:
        self.plant_state.sync()
        return self.plant_state.temperature_c

    def status(self) -> Dict[str, Any]:
        payload = super().status()
        payload.update(self.read_current())
        return payload
