from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional


class RunMode(str, Enum):
    AUTO_CALIBRATION = "auto_calibration"
    CO2_MEASUREMENT = "co2_measurement"
    H2O_MEASUREMENT = "h2o_measurement"
    EXPERIMENT_MEASUREMENT = "experiment_measurement"


_RUN_MODE_ALIASES = {
    "": RunMode.AUTO_CALIBRATION,
    "auto": RunMode.AUTO_CALIBRATION,
    "auto_calibration": RunMode.AUTO_CALIBRATION,
    "automatic_calibration": RunMode.AUTO_CALIBRATION,
    "calibration": RunMode.AUTO_CALIBRATION,
    "co2": RunMode.CO2_MEASUREMENT,
    "co2_measurement": RunMode.CO2_MEASUREMENT,
    "co2_measure": RunMode.CO2_MEASUREMENT,
    "co2_test": RunMode.CO2_MEASUREMENT,
    "h2o": RunMode.H2O_MEASUREMENT,
    "h2o_measurement": RunMode.H2O_MEASUREMENT,
    "water": RunMode.H2O_MEASUREMENT,
    "water_measurement": RunMode.H2O_MEASUREMENT,
    "humidity_measurement": RunMode.H2O_MEASUREMENT,
    "experiment": RunMode.EXPERIMENT_MEASUREMENT,
    "experiment_measurement": RunMode.EXPERIMENT_MEASUREMENT,
    "lab": RunMode.EXPERIMENT_MEASUREMENT,
}


def normalize_run_mode(value: Any, default: RunMode = RunMode.AUTO_CALIBRATION) -> RunMode:
    if isinstance(value, RunMode):
        return value
    normalized = str(getattr(value, "value", value) or "").strip().lower()
    return _RUN_MODE_ALIASES.get(normalized, default)


def run_mode_label(value: Any) -> str:
    run_mode = normalize_run_mode(value)
    return {
        RunMode.AUTO_CALIBRATION: "自动校准",
        RunMode.CO2_MEASUREMENT: "CO2 测量",
        RunMode.H2O_MEASUREMENT: "水汽测量",
        RunMode.EXPERIMENT_MEASUREMENT: "实验测量",
    }[run_mode]


@dataclass(frozen=True)
class ModeProfile:
    run_mode: RunMode = RunMode.AUTO_CALIBRATION
    route_mode: Optional[str] = None
    formal_calibration_report: Optional[bool] = None

    @classmethod
    def from_value(cls, payload: Any = None) -> "ModeProfile":
        if isinstance(payload, ModeProfile):
            return payload

        data = dict(payload or {}) if isinstance(payload, dict) else {}
        run_mode = normalize_run_mode(data.get("run_mode", payload))
        route_mode = data.get("route_mode")
        if route_mode not in (None, ""):
            route_mode = str(route_mode).strip().lower()
        else:
            route_mode = None
        formal_calibration_report = data.get("formal_calibration_report")
        if formal_calibration_report is not None:
            formal_calibration_report = bool(formal_calibration_report)
        return cls(
            run_mode=run_mode,
            route_mode=route_mode,
            formal_calibration_report=formal_calibration_report,
        )

    def effective_route_mode(self, default: str = "h2o_then_co2") -> str:
        if self.route_mode:
            return str(self.route_mode)
        if self.run_mode == RunMode.CO2_MEASUREMENT:
            return "co2_only"
        if self.run_mode == RunMode.H2O_MEASUREMENT:
            return "h2o_only"
        return str(default or "h2o_then_co2")

    def formal_report_enabled(self) -> bool:
        if self.formal_calibration_report is not None:
            return bool(self.formal_calibration_report)
        return self.run_mode == RunMode.AUTO_CALIBRATION

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "run_mode": self.run_mode.value,
        }
        if self.route_mode:
            payload["route_mode"] = str(self.route_mode)
        if self.formal_calibration_report is not None:
            payload["formal_calibration_report"] = bool(self.formal_calibration_report)
        return payload
