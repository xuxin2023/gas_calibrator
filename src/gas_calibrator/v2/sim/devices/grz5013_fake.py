from __future__ import annotations

from dataclasses import dataclass
import math
import re
import time
from typing import Any, Optional


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, float(value)))


def _dewpoint_from_temp_rh(temp_c: float, rh_pct: float) -> float:
    return float(temp_c) - (100.0 - _clamp(rh_pct, 0.0, 100.0)) / 5.0


def _frostpoint_from_dewpoint(dewpoint_c: float) -> float:
    if dewpoint_c >= 0.0:
        return float(dewpoint_c)
    return float(dewpoint_c) - 1.5


def _approach(current: float, target: float, step: float) -> float:
    delta = float(target) - float(current)
    if abs(delta) <= step:
        return float(target)
    return float(current) + math.copysign(step, delta)


def _format_value(value: Any) -> str:
    if isinstance(value, bool):
        return "ON" if value else "OFF"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.3f}".rstrip("0").rstrip(".")
    return str(value)


@dataclass
class GRZ5013Status:
    mode: str
    control_enabled: bool
    current_temp_c: float
    current_rh_pct: float
    dewpoint_c: float
    frostpoint_c: float
    flow_lpm: float
    stable_seconds: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "control_enabled": self.control_enabled,
            "current_temp_c": self.current_temp_c,
            "current_rh_pct": self.current_rh_pct,
            "dewpoint_c": self.dewpoint_c,
            "frostpoint_c": self.frostpoint_c,
            "flow_lpm": self.flow_lpm,
            "stable_seconds": self.stable_seconds,
        }


class GRZ5013Fake:
    """
    Protocol-level fake for the GRZ5013 humidity generator.

    The fake accepts the same text protocol that the real driver sends:
    - 9600,8,N,1 semantics are modeled by not touching any real serial port
    - commands are case-insensitive
    - commands/queries use CRLF framing
    """

    DEFAULT_TEMP_C = 25.0
    DEFAULT_RH_PCT = 35.0
    DEFAULT_FLOW_LPM = 1.0
    DEFAULT_VERSION = "GRZ5013-SIM"
    VALID_MODES = {
        "stable",
        "temperature_only_progress",
        "humidity_static_fault",
        "timeout",
        "skipped_by_profile",
    }

    def __init__(
        self,
        port: str = "SIM-GRZ5013",
        *,
        baudrate: int = 9600,
        timeout: float = 1.0,
        io_logger: Optional[Any] = None,
        plant_state: Optional[Any] = None,
        mode: str = "stable",
        skipped_by_profile: bool = False,
        target_temp_c: float = DEFAULT_TEMP_C,
        target_rh_pct: float = DEFAULT_RH_PCT,
        target_flow_lpm: float = DEFAULT_FLOW_LPM,
        current_temp_c: float = DEFAULT_TEMP_C,
        current_rh_pct: float = DEFAULT_RH_PCT,
        temperature_step_c_per_s: float = 8.0,
        humidity_step_pct_per_s: float = 15.0,
        version: str = DEFAULT_VERSION,
        **_: Any,
    ) -> None:
        self.port = str(port or "SIM-GRZ5013")
        self.baudrate = int(baudrate or 9600)
        self.timeout = float(timeout if timeout is not None else 1.0)
        self.io_logger = io_logger
        self.plant_state = plant_state
        if self.plant_state is not None:
            setattr(self.plant_state, "dynamic_protocol", True)
        self.connected = False
        self.mode = str(mode or "stable").strip().lower()
        if self.mode not in self.VALID_MODES:
            self.mode = "stable"
        self.skipped_by_profile = bool(skipped_by_profile or self.mode == "skipped_by_profile")
        self.target_temp_c = float(target_temp_c if target_temp_c is not None else self.DEFAULT_TEMP_C)
        self.target_rh_pct = float(target_rh_pct if target_rh_pct is not None else self.DEFAULT_RH_PCT)
        self.target_flow_lpm = float(target_flow_lpm if target_flow_lpm is not None else self.DEFAULT_FLOW_LPM)
        self.current_temp_c = float(current_temp_c if current_temp_c is not None else self.DEFAULT_TEMP_C)
        self.current_rh_pct = float(current_rh_pct if current_rh_pct is not None else self.DEFAULT_RH_PCT)
        self.current_flow_lpm = 0.0
        self.control_enabled = False
        self.cool_enabled = False
        self.heat_enabled = False
        self.temperature_step_c_per_s = max(0.1, float(temperature_step_c_per_s))
        self.humidity_step_pct_per_s = max(0.1, float(humidity_step_pct_per_s))
        self.version = str(version or self.DEFAULT_VERSION)
        self._last_update_ts = time.monotonic()
        self._in_band_since_ts: Optional[float] = None
        self._last_response = ""
        self._sync_plant_state()

    def open(self) -> None:
        self.connected = True

    def connect(self) -> bool:
        self.connected = True
        return True

    def close(self) -> None:
        self.connected = False

    def write(self, data: str) -> None:
        self._last_response = self.process_command(data)

    def query(self, field: str) -> str:
        return self.process_command(f"FETC? (@{field})")

    def process_command(self, data: str) -> str:
        text = str(data or "").strip()
        if not text:
            return ""
        self._update_state()
        lowered = text.lower()
        if lowered.startswith("fetc?"):
            field = self._extract_query_field(text)
            return self._query_field(field)
        match = re.match(r"^target\s*:\s*([a-z0-9_]+)\s*=\s*(.+)$", text, flags=re.IGNORECASE)
        if not match:
            return "ERR\r\n"
        field = match.group(1).strip().lower()
        raw_value = match.group(2).strip()
        if field == "uwa":
            self.target_rh_pct = float(raw_value)
        elif field == "ta":
            self.target_temp_c = float(raw_value)
        elif field == "fa":
            self.target_flow_lpm = float(raw_value)
        elif field == "cool":
            self.cool_enabled = raw_value.strip().lower() == "on"
        elif field == "heat":
            self.heat_enabled = raw_value.strip().lower() == "on"
        elif field == "ctrl":
            self.control_enabled = raw_value.strip().lower() == "on"
        else:
            return "ERR\r\n"
        self._sync_plant_state()
        return "OK\r\n"

    def fetch_all(self) -> dict[str, Any]:
        self._update_state()
        data = self._snapshot_data()
        return {"raw": self._format_all_response(data).strip(), "data": data}

    def fetch_tag_value(self, tag: str) -> dict[str, Any]:
        self._update_state()
        normalized = str(tag or "").strip()
        data = self._snapshot_data()
        value = data.get(normalized)
        if value is None:
            value = data.get(normalized.capitalize())
        raw = _format_value(value) if value is not None else ""
        return {"tag": normalized, "raw_pick": raw, "value": value, "raw_lines": [raw] if raw else []}

    def read_target_temp_c(self) -> Optional[float]:
        return float(self.target_temp_c)

    def read_target_rh_pct(self) -> Optional[float]:
        return float(self.target_rh_pct)

    def read_flow_lpm(self) -> Optional[float]:
        self._update_state()
        return float(self.current_flow_lpm)

    def set_target_temp(self, value_c: float) -> None:
        self.process_command(f"Target:TA={value_c}")

    def set_temp_c(self, value_c: float) -> None:
        self.set_target_temp(value_c)

    def set_temperature_c(self, value_c: float) -> None:
        self.set_target_temp(value_c)

    def set_target_rh(self, value_pct: float) -> None:
        self.process_command(f"Target:UwA={value_pct}")

    def set_relative_humidity_pct(self, value_pct: float) -> None:
        self.set_target_rh(value_pct)

    def set_rh_pct(self, value_pct: float) -> None:
        self.set_target_rh(value_pct)

    def set_humidity_pct(self, value_pct: float) -> None:
        self.set_target_rh(value_pct)

    def set_humidity(self, value_pct: float) -> None:
        self.set_target_rh(value_pct)

    def set_flow_target(self, flow_lpm: float) -> None:
        self.process_command(f"Target:FA={flow_lpm}")

    def enable_control(self, on: bool) -> None:
        self.process_command(f"Target:CTRL={'ON' if on else 'OFF'}")

    def heat_on(self) -> None:
        self.process_command("Target:HEAT=ON")

    def heat_off(self) -> None:
        self.process_command("Target:HEAT=OFF")

    def cool_on(self) -> None:
        self.process_command("Target:COOL=ON")

    def cool_off(self) -> None:
        self.process_command("Target:COOL=OFF")

    def read(self) -> dict[str, Any]:
        return self.fetch_all()

    def status(self) -> dict[str, Any]:
        self._update_state()
        status = self._status()
        payload = status.to_dict()
        payload.update(
            {
                "connected": self.connected,
                "port": self.port,
                "status": "skipped_by_profile" if self.skipped_by_profile else "ready",
                "ok": True,
            }
        )
        return payload

    def selftest(self) -> dict[str, Any]:
        return {"ok": True, "connected": self.connected or self.skipped_by_profile, "mode": self.mode}

    def verify_target_readback(
        self,
        *,
        target_temp_c: Optional[float] = None,
        target_rh_pct: Optional[float] = None,
        temp_tol_c: float = 0.2,
        rh_tol_pct: float = 0.5,
    ) -> dict[str, Any]:
        read_temp_c = self.read_target_temp_c() if target_temp_c is not None else None
        read_rh_pct = self.read_target_rh_pct() if target_rh_pct is not None else None
        temp_ok = target_temp_c is None or (
            read_temp_c is not None and abs(float(read_temp_c) - float(target_temp_c)) <= float(temp_tol_c)
        )
        rh_ok = target_rh_pct is None or (
            read_rh_pct is not None and abs(float(read_rh_pct) - float(target_rh_pct)) <= float(rh_tol_pct)
        )
        return {
            "ok": bool(temp_ok and rh_ok),
            "target_temp_c": target_temp_c,
            "target_rh_pct": target_rh_pct,
            "read_temp_c": read_temp_c,
            "read_rh_pct": read_rh_pct,
            "temp_tol_c": float(temp_tol_c),
            "rh_tol_pct": float(rh_tol_pct),
        }

    def ensure_run(
        self,
        min_flow_lpm: float = 0.1,
        tries: int = 2,
        wait_s: float = 2.5,
        poll_s: float = 0.25,
    ) -> dict[str, Any]:
        if self.skipped_by_profile:
            return {"ok": True, "flow_lpm": 0.0, "tried": ["skipped_by_profile"], "skipped_by_profile": True}
        tried: list[str] = []
        for _ in range(max(1, int(tries))):
            tried.append("CTRL=ON")
            self.enable_control(True)
            deadline = time.monotonic() + max(0.1, float(wait_s))
            while time.monotonic() < deadline:
                self._update_state()
                if float(self.current_flow_lpm) >= float(min_flow_lpm):
                    return {"ok": True, "flow_lpm": float(self.current_flow_lpm), "tried": tried}
                time.sleep(max(0.01, float(poll_s)))
        return {"ok": False, "flow_lpm": float(self.current_flow_lpm), "tried": tried}

    def safe_stop(self) -> None:
        self.set_flow_target(0.0)
        self.enable_control(False)
        self.cool_off()
        self.heat_off()
        self._update_state()

    def wait_stopped(
        self,
        *,
        max_flow_lpm: float = 0.05,
        timeout_s: float = 5.0,
        poll_s: float = 0.25,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + max(0.1, float(timeout_s))
        last_flow = self.read_flow_lpm()
        while time.monotonic() < deadline:
            last_flow = self.read_flow_lpm()
            if last_flow is not None and float(last_flow) <= float(max_flow_lpm):
                return {"ok": True, "flow_lpm": last_flow, "max_flow_lpm": float(max_flow_lpm)}
            time.sleep(max(0.01, float(poll_s)))
        return {"ok": False, "flow_lpm": last_flow, "max_flow_lpm": float(max_flow_lpm)}

    def _extract_query_field(self, text: str) -> str:
        match = re.search(r"@\s*([A-Za-z0-9_]+)\s*\)", text)
        return (match.group(1) if match else "All").strip()

    def _query_field(self, field: str) -> str:
        data = self._snapshot_data()
        normalized = str(field or "").strip()
        lowered = normalized.lower()
        if lowered == "all":
            return self._format_all_response(data)
        value = data.get(normalized)
        if value is None:
            for key, item in data.items():
                if str(key).strip().lower() == lowered:
                    value = item
                    break
        if value is None:
            return "\r\n"
        return f"{_format_value(value)}\r\n"

    def _format_all_response(self, data: dict[str, Any]) -> str:
        ordered_keys = (
            "Tc",
            "Ts",
            "TA",
            "Td",
            "Tf",
            "Uw",
            "Ui",
            "UwA",
            "UiA",
            "Fl",
            "FA",
            "st",
            "CTRL",
            "COOL",
            "HEAT",
            "Ver",
        )
        items = [f"{key}={_format_value(data[key])}" for key in ordered_keys if key in data]
        return ",".join(items) + "\r\n"

    def _snapshot_data(self) -> dict[str, Any]:
        self._update_state()
        dewpoint_c = _dewpoint_from_temp_rh(self.current_temp_c, self.current_rh_pct)
        frostpoint_c = _frostpoint_from_dewpoint(dewpoint_c)
        stable_seconds = 0.0
        if self._in_band_since_ts is not None:
            stable_seconds = max(0.0, time.monotonic() - self._in_band_since_ts)
        return {
            "Tc": round(self.current_temp_c, 3),
            "Ts": round(self.target_temp_c, 3),
            "TA": round(self.target_temp_c, 3),
            "Td": round(dewpoint_c, 3),
            "Tf": round(frostpoint_c, 3),
            "Uw": round(self.current_rh_pct, 3),
            "Ui": round(self.current_rh_pct, 3),
            "UwA": round(self.target_rh_pct, 3),
            "UiA": round(self.target_rh_pct, 3),
            "Fl": round(self.current_flow_lpm, 3),
            "FA": round(self.target_flow_lpm, 3),
            "st": round(stable_seconds, 3),
            "CTRL": self.control_enabled,
            "COOL": self.cool_enabled,
            "HEAT": self.heat_enabled,
            "Ver": self.version,
        }

    def _status(self) -> GRZ5013Status:
        dewpoint_c = _dewpoint_from_temp_rh(self.current_temp_c, self.current_rh_pct)
        frostpoint_c = _frostpoint_from_dewpoint(dewpoint_c)
        stable_seconds = 0.0
        if self._in_band_since_ts is not None:
            stable_seconds = max(0.0, time.monotonic() - self._in_band_since_ts)
        return GRZ5013Status(
            mode=self.mode,
            control_enabled=bool(self.control_enabled),
            current_temp_c=float(self.current_temp_c),
            current_rh_pct=float(self.current_rh_pct),
            dewpoint_c=float(dewpoint_c),
            frostpoint_c=float(frostpoint_c),
            flow_lpm=float(self.current_flow_lpm),
            stable_seconds=float(stable_seconds),
        )

    def _update_state(self) -> None:
        now = time.monotonic()
        elapsed_s = max(0.0, now - self._last_update_ts)
        self._last_update_ts = now
        if self.skipped_by_profile:
            self.current_flow_lpm = 0.0
            self._sync_plant_state()
            return
        if elapsed_s <= 0:
            self._sync_plant_state()
            return

        if self.control_enabled:
            flow_target = max(0.0, float(self.target_flow_lpm))
            self.current_flow_lpm = _approach(self.current_flow_lpm, flow_target, max(0.05, elapsed_s * 3.0))
        else:
            self.current_flow_lpm = _approach(self.current_flow_lpm, 0.0, max(0.05, elapsed_s * 4.0))

        if self.control_enabled or self.heat_enabled or self.cool_enabled:
            self.current_temp_c = _approach(
                self.current_temp_c,
                self.target_temp_c,
                max(0.05, self.temperature_step_c_per_s * elapsed_s),
            )
            if self.mode == "stable":
                self.current_rh_pct = _approach(
                    self.current_rh_pct,
                    self.target_rh_pct,
                    max(0.1, self.humidity_step_pct_per_s * elapsed_s),
                )
            elif self.mode == "temperature_only_progress":
                self.current_rh_pct = _approach(
                    self.current_rh_pct,
                    max(0.0, min(self.target_rh_pct, self.current_rh_pct + 1.0)),
                    max(0.05, self.humidity_step_pct_per_s * 0.15 * elapsed_s),
                )
            elif self.mode == "humidity_static_fault":
                pass
            elif self.mode == "timeout":
                self.current_rh_pct = _approach(
                    self.current_rh_pct,
                    self.target_rh_pct - 5.0,
                    max(0.05, self.humidity_step_pct_per_s * 0.25 * elapsed_s),
                )

        temp_ok = abs(self.current_temp_c - self.target_temp_c) <= 0.3
        rh_ok = abs(self.current_rh_pct - self.target_rh_pct) <= 0.8
        if temp_ok and rh_ok and self.control_enabled and self.mode == "stable":
            if self._in_band_since_ts is None:
                self._in_band_since_ts = now
        else:
            self._in_band_since_ts = None
        self._sync_plant_state()

    def _sync_plant_state(self) -> None:
        if self.plant_state is None:
            return
        try:
            self.plant_state.target_temperature_c = float(self.target_temp_c)
            self.plant_state.temperature_c = float(self.current_temp_c)
            self.plant_state.target_humidity_pct = float(self.target_rh_pct)
            self.plant_state.humidity_pct = float(self.current_rh_pct)
            self.plant_state.dewpoint_c = _dewpoint_from_temp_rh(self.current_temp_c, self.current_rh_pct)
            if hasattr(self.plant_state, "sync"):
                self.plant_state.sync()
        except Exception:
            pass
