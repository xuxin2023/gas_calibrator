"""Detailed calibration point used by the offline simulation runtime."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .pressure_selection import (
    effective_pressure_mode,
    normalize_pressure_selection_token,
    pressure_selection_key,
    pressure_target_label,
)


@dataclass(frozen=True)
class CalibrationPoint:
    """V2 校准点定义。"""

    index: int
    temperature_c: float
    co2_ppm: Optional[float] = None
    humidity_pct: Optional[float] = None
    pressure_hpa: Optional[float] = None
    route: str = "co2"
    humidity_generator_temp_c: Optional[float] = None
    dewpoint_c: Optional[float] = None
    h2o_mmol: Optional[float] = None
    raw_h2o: Optional[str] = None
    co2_group: Optional[str] = None
    cylinder_nominal_ppm: Optional[float] = None
    pressure_mode: str = ""
    pressure_target_label: Optional[str] = None
    pressure_selection_token: str = ""

    @property
    def temp_chamber_c(self) -> float:
        return float(self.temperature_c)

    @property
    def hgen_temp_c(self) -> Optional[float]:
        if self.humidity_generator_temp_c is not None:
            return float(self.humidity_generator_temp_c)
        if self.is_h2o_point:
            return float(self.temperature_c)
        return None

    @property
    def hgen_rh_pct(self) -> Optional[float]:
        if self.humidity_pct is None:
            return None
        return float(self.humidity_pct)

    @property
    def target_pressure_hpa(self) -> Optional[float]:
        if self.pressure_hpa is None:
            return None
        return float(self.pressure_hpa)

    @property
    def effective_pressure_mode(self) -> str:
        return effective_pressure_mode(
            pressure_hpa=self.pressure_hpa,
            pressure_mode=self.pressure_mode,
            pressure_selection_token=self.pressure_selection_token,
        )

    @property
    def pressure_selection_token_value(self) -> str:
        return normalize_pressure_selection_token(self.pressure_selection_token)

    @property
    def is_ambient_pressure_point(self) -> bool:
        return self.effective_pressure_mode == "ambient_open"

    @property
    def pressure_display_label(self) -> Optional[str]:
        return pressure_target_label(
            pressure_hpa=self.pressure_hpa,
            pressure_mode=self.pressure_mode,
            pressure_selection_token=self.pressure_selection_token,
            explicit_label=self.pressure_target_label,
        )

    @property
    def pressure_selection_key(self) -> Optional[float | str]:
        return pressure_selection_key(
            pressure_hpa=self.pressure_hpa,
            pressure_mode=self.pressure_mode,
            pressure_selection_token=self.pressure_selection_token,
        )

    @property
    def is_h2o_point(self) -> bool:
        route = str(self.route or "").strip().lower()
        return route == "h2o" or self.humidity_pct is not None or self.humidity_generator_temp_c is not None
