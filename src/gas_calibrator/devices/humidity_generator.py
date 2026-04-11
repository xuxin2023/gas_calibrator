"""Humidity generator serial driver."""

from __future__ import annotations

import re
import time
from typing import Any, Dict, List, Optional

from .serial_base import SerialDevice


class HumidityGenerator:
    """Humidity generator protocol wrapper."""

    def __init__(
        self,
        port: str,
        baudrate: int = 9600,
        timeout: float = 1.0,
        io_logger: Optional[Any] = None,
        serial_factory: Optional[Any] = None,
    ):
        self.ser = SerialDevice(
            port,
            baudrate=baudrate,
            timeout=timeout,
            device_name="humidity_generator",
            io_logger=io_logger,
            serial_factory=serial_factory,
        )

    def open(self) -> None:
        self.ser.open()

    def connect(self) -> None:
        self.open()

    def close(self) -> None:
        self.ser.close()

    def write(self, data: str) -> None:
        text = str(data or "")
        if text.endswith(("\r", "\n")):
            self.ser.write(text)
        else:
            self._send(text)

    def _send(self, cmd: str) -> None:
        self.ser.write(cmd + "\r\n")

    def _drain(self, drain_s: float = 0.35, read_timeout_s: float = 0.05) -> List[str]:
        try:
            return self.ser.drain_input_nonblock(drain_s=drain_s, read_timeout_s=read_timeout_s)
        except Exception:
            return []

    def _flush_input(self) -> None:
        try:
            self.ser.flush_input()
        except Exception:
            self._drain(drain_s=0.2)

    def _query_collect_lines(self, cmd: str, drain_s: float = 0.55) -> List[str]:
        self._flush_input()
        self._send(cmd)
        time.sleep(0.03)
        lines = self._drain(drain_s=drain_s, read_timeout_s=0.05)
        return [ln.strip() for ln in lines if ln and ln.strip()]

    @staticmethod
    def _as_float(value: Any) -> Optional[float]:
        try:
            return float(value)
        except Exception:
            return None

    @classmethod
    def _pick_numeric(cls, data: Dict[str, Any], keys: List[str]) -> Optional[float]:
        if not isinstance(data, dict):
            return None
        for key in keys:
            value = cls._as_float(data.get(key))
            if value is not None:
                return value
        return None

    @staticmethod
    def _parse_kv_line(line: str) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        items = [x.strip() for x in (line or "").split(",") if x.strip()]
        for it in items:
            m = re.match(r"^([A-Za-z0-9_]+)\s*=\s*(.+)$", it)
            if not m:
                continue
            k = m.group(1).strip()
            v = m.group(2).strip()
            try:
                out[k] = float(v)
            except Exception:
                out[k] = v
        return out

    def set_target_temp(self, value_c: float) -> None:
        self._send(f"Target:TA={value_c}")
        self._drain()

    def set_target_rh(self, value_pct: float) -> None:
        self._send(f"Target:UwA={value_pct}")
        self._drain()

    def set_flow_target(self, flow_lpm: float) -> None:
        self._send(f"Target:FA={flow_lpm}")
        self._drain()

    def enable_control(self, on: bool) -> None:
        self._send(f"Target:CTRL={'ON' if on else 'OFF'}")
        self._drain()

    def heat_on(self) -> None:
        self._send("Target:HEAT=ON")
        self._drain()

    def heat_off(self) -> None:
        self._send("Target:HEAT=OFF")
        self._drain()

    def cool_on(self) -> None:
        self._send("Target:COOL=ON")
        self._drain()

    def cool_off(self) -> None:
        self._send("Target:COOL=OFF")
        self._drain()

    def query(self, field: str) -> str:
        return self.ser.query(f"FETC? (@{field})\r\n")

    def fetch_all(self) -> Dict[str, Any]:
        lines = self._query_collect_lines("FETC? (@All)", drain_s=0.55)
        raw = lines[-1] if lines else ""
        data = self._parse_kv_line(raw) if raw else {}
        if "Fl" not in data and "Flux" in data:
            data["Fl"] = data["Flux"]
        return {"raw": raw, "data": data}

    def fetch_tag_value(self, tag: str) -> Dict[str, Any]:
        lines = self._query_collect_lines(f"FETC? (@{tag})", drain_s=0.40)
        picked = lines[-1] if lines else ""
        value: Any = None
        if picked:
            kv = self._parse_kv_line(picked)
            if tag in kv:
                value = kv[tag]
            else:
                try:
                    value = float(picked)
                except Exception:
                    value = picked.strip()
        return {"tag": tag, "raw_pick": picked, "value": value, "raw_lines": lines}

    def read_target_temp_c(self) -> Optional[float]:
        row = self.fetch_tag_value("TA")
        return self._as_float(row.get("value"))

    def read_target_rh_pct(self) -> Optional[float]:
        row = self.fetch_tag_value("UwA")
        return self._as_float(row.get("value"))

    def read_flow_lpm(self) -> Optional[float]:
        snap = self.fetch_all()
        data = snap.get("data", {}) if isinstance(snap, dict) else {}
        return self._as_float(data.get("Fl", data.get("Flux")))

    def verify_target_readback(
        self,
        *,
        target_temp_c: Optional[float] = None,
        target_rh_pct: Optional[float] = None,
        temp_tol_c: float = 0.2,
        rh_tol_pct: float = 0.5,
    ) -> Dict[str, Any]:
        read_temp_c = self.read_target_temp_c() if target_temp_c is not None else None
        read_rh_pct = self.read_target_rh_pct() if target_rh_pct is not None else None
        temp_ok = (
            target_temp_c is None
            or (read_temp_c is not None and abs(float(read_temp_c) - float(target_temp_c)) <= float(temp_tol_c))
        )
        rh_ok = (
            target_rh_pct is None
            or (read_rh_pct is not None and abs(float(read_rh_pct) - float(target_rh_pct)) <= float(rh_tol_pct))
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

    def read(self) -> Dict[str, Any]:
        return self.fetch_all()

    def status(self) -> Dict[str, Any]:
        row = self.fetch_all()
        data = row.get("data", {})
        return {
            "ok": bool(row.get("raw")),
            "raw": row.get("raw"),
            "flow_lpm": data.get("Fl"),
            "dewpoint_c": data.get("Td"),
            "temp_c": data.get("Tc"),
            "data": data,
        }

    def ensure_run(
        self,
        min_flow_lpm: float = 0.1,
        tries: int = 2,
        wait_s: float = 2.5,
        poll_s: float = 0.25,
    ) -> Dict[str, Any]:
        tried: List[str] = []
        for _ in range(max(1, tries)):
            tried.append("CTRL=ON")
            self.enable_control(True)
            t0 = time.time()
            while time.time() - t0 < wait_s:
                data = self.fetch_all().get("data", {})
                fl = data.get("Fl")
                try:
                    if fl is not None and float(fl) >= min_flow_lpm:
                        return {"ok": True, "flow_lpm": float(fl), "tried": tried}
                except Exception:
                    pass
                time.sleep(poll_s)
        return {"ok": False, "flow_lpm": None, "tried": tried}

    def verify_runtime_activation(
        self,
        *,
        min_flow_lpm: float = 0.5,
        timeout_s: float = 30.0,
        poll_s: float = 1.0,
        target_temp_c: Optional[float] = None,
        baseline_hot_temp_c: Optional[float] = None,
        baseline_cold_temp_c: Optional[float] = None,
        cooling_expected: Optional[bool] = None,
        cooling_min_drop_c: float = 0.2,
        cooling_min_delta_c: float = 0.5,
    ) -> Dict[str, Any]:
        deadline = time.time() + max(0.2, float(timeout_s))
        baseline_hot = self._as_float(baseline_hot_temp_c)
        baseline_cold = self._as_float(baseline_cold_temp_c)
        target_temp = self._as_float(target_temp_c)

        if cooling_expected is None:
            reference_temp = baseline_cold if baseline_cold is not None else baseline_hot
            cooling_expected = bool(
                target_temp is not None
                and reference_temp is not None
                and float(target_temp) <= float(reference_temp) - 1.0
            )
        else:
            cooling_expected = bool(cooling_expected)

        last_flow_lpm: Optional[float] = None
        last_hot_temp_c: Optional[float] = None
        last_cold_temp_c: Optional[float] = None
        last_raw = ""
        flow_ok = False
        cooling_ok: Optional[bool] = None if cooling_expected else False
        sample_count = 0

        while True:
            snap = self.fetch_all()
            data = snap.get("data", {}) if isinstance(snap, dict) else {}
            last_raw = str((snap or {}).get("raw") or "")
            sample_count += 1
            last_flow_lpm = self._pick_numeric(data, ["Fl", "Flux"])
            last_hot_temp_c = self._pick_numeric(data, ["Tc", "TA", "Temp", "temperature"])
            last_cold_temp_c = self._pick_numeric(data, ["Ts", "Tc", "TA", "Temp", "temperature"])

            flow_ok = bool(last_flow_lpm is not None and float(last_flow_lpm) >= float(min_flow_lpm))
            if cooling_expected:
                checks: List[bool] = []
                if last_cold_temp_c is not None and baseline_cold is not None:
                    checks.append(float(last_cold_temp_c) <= float(baseline_cold) - float(cooling_min_drop_c))
                if last_hot_temp_c is not None and last_cold_temp_c is not None:
                    checks.append(float(last_cold_temp_c) <= float(last_hot_temp_c) - float(cooling_min_delta_c))
                cooling_ok = any(checks) if checks else None

            fully_confirmed = bool(flow_ok and (not cooling_expected or cooling_ok is True))
            if fully_confirmed or time.time() >= deadline:
                return {
                    "ok": bool(flow_ok),
                    "fully_confirmed": fully_confirmed,
                    "flow_ok": bool(flow_ok),
                    "cooling_expected": bool(cooling_expected),
                    "cooling_ok": cooling_ok,
                    "flow_lpm": last_flow_lpm,
                    "hot_temp_c": last_hot_temp_c,
                    "cold_temp_c": last_cold_temp_c,
                    "baseline_hot_temp_c": baseline_hot,
                    "baseline_cold_temp_c": baseline_cold,
                    "target_temp_c": target_temp,
                    "timeout_s": float(timeout_s),
                    "poll_s": float(poll_s),
                    "sample_count": sample_count,
                    "raw": last_raw,
                }
            time.sleep(max(0.05, float(poll_s)))

    def safe_stop(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "flow_off": "not_attempted",
            "ctrl_off": "not_attempted",
            "cool_off": "not_attempted",
            "heat_off": "not_attempted",
        }
        try:
            self.set_flow_target(0.0)
            result["flow_off"] = "ok"
        except Exception as exc:
            result["flow_off"] = "failed"
            result["flow_off_error"] = str(exc)
        try:
            self.enable_control(False)
            result["ctrl_off"] = "ok"
        except Exception as exc:
            result["ctrl_off"] = "failed"
            result["ctrl_off_error"] = str(exc)
        try:
            self.cool_off()
            result["cool_off"] = "ok"
        except Exception as exc:
            result["cool_off"] = "failed"
            result["cool_off_error"] = str(exc)
        try:
            self.heat_off()
            result["heat_off"] = "ok"
        except Exception as exc:
            result["heat_off"] = "failed"
            result["heat_off_error"] = str(exc)
        return result

    def wait_stopped(
        self,
        *,
        max_flow_lpm: float = 0.05,
        timeout_s: float = 5.0,
        poll_s: float = 0.25,
    ) -> Dict[str, Any]:
        deadline = time.time() + max(0.2, float(timeout_s))
        last_flow_lpm: Optional[float] = None
        while time.time() < deadline:
            last_flow_lpm = self.read_flow_lpm()
            if last_flow_lpm is not None and float(last_flow_lpm) <= float(max_flow_lpm):
                return {
                    "ok": True,
                    "flow_lpm": float(last_flow_lpm),
                    "max_flow_lpm": float(max_flow_lpm),
                }
            time.sleep(max(0.05, float(poll_s)))
        return {
            "ok": False,
            "flow_lpm": last_flow_lpm,
            "max_flow_lpm": float(max_flow_lpm),
        }

    def selftest(self) -> Dict[str, Any]:
        return self.status()
