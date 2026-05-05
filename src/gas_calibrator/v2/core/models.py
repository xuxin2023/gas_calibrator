"""
V2 核心数据模型。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

from ..domain.pressure_selection import (
    effective_pressure_mode,
    normalize_pressure_selection_token,
    pressure_selection_key,
    pressure_target_label,
)


class CalibrationPhase(Enum):
    """校准阶段。"""

    IDLE = "idle"
    INITIALIZING = "initializing"
    PRECHECK = "precheck"
    CONDITIONING = "conditioning"
    TEMPERATURE_GROUP = "temperature_group"
    H2O_ROUTE = "h2o_route"
    CO2_ROUTE = "co2_route"
    SAMPLING = "sampling"
    FINALIZING = "finalizing"
    COMPLETED = "completed"
    ERROR = "error"
    STOPPED = "stopped"


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


@dataclass(frozen=True)
class CalibrationStatus:
    """V2 校准状态。"""

    phase: CalibrationPhase = CalibrationPhase.IDLE
    current_point: Optional[CalibrationPoint] = None
    total_points: int = 0
    completed_points: int = 0
    progress: float = 0.0
    message: str = ""
    elapsed_s: float = 0.0
    error: Optional[str] = None
    fault_code: str = ""

# ---------------------------------------------------------------------------
# 结构化故障码 (H2O 探针)
# ---------------------------------------------------------------------------
FAULT_CODES = {
    "H2O-001": "设备预检失败: 分析仪未切换到 mode2",
    "H2O-002": "湿度发生器主动控制失败: 目标温度/湿度未能在超时内达到",
    "H2O-003": "露点仪对准失败: 读数与湿度发生器不匹配",
    "H2O-004": "采样阶段失败: 传感器信号不稳定",
    "H2O-005": "过程被外部中断: 探针提前退出",
    "H2O-006": "湿度发生器过热: 初始腔温过高, 需冷却后重试",
    "H2O-007": "阀门路由失败: 继电器未响应或物理状态不匹配",
    "H2O-008": "压力控制失败: 压力未稳定在目标范围内",
    "H2O-009": "分析仪 mode2 恢复失败: selftest 重试后仍不合格",
}


@dataclass(frozen=True)
class SamplingResult:
    """单条采样结果。"""

    point: CalibrationPoint
    analyzer_id: str
    timestamp: datetime
    co2_ppm: Optional[float] = None
    h2o_mmol: Optional[float] = None
    h2o_signal: Optional[float] = None
    co2_signal: Optional[float] = None
    co2_ratio_f: Optional[float] = None
    co2_ratio_raw: Optional[float] = None
    h2o_ratio_f: Optional[float] = None
    h2o_ratio_raw: Optional[float] = None
    ref_signal: Optional[float] = None
    temperature_c: Optional[float] = None
    pressure_hpa: Optional[float] = None
    pressure_gauge_hpa: Optional[float] = None
    pressure_reference_status: str = ""
    thermometer_temp_c: Optional[float] = None
    thermometer_reference_status: str = ""
    dew_point_c: Optional[float] = None
    analyzer_pressure_kpa: Optional[float] = None
    analyzer_chamber_temp_c: Optional[float] = None
    case_temp_c: Optional[float] = None
    frame_has_data: bool = True
    frame_usable: bool = True
    frame_status: str = ""
    point_phase: str = ""
    point_tag: str = ""
    sample_index: int = 0
    stability_time_s: Optional[float] = None
    total_time_s: Optional[float] = None
