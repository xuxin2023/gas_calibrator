"""
离线建模与筛选重算配置。

该模块仅服务于离线建模链路，不参与在线自动校准运行流程。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..utils import as_bool, as_float, as_int


@dataclass
class ValueBoundsConfig:
    """基础清洗阶段的非法值边界。"""

    y_min: Optional[float] = None
    y_max: Optional[float] = None
    r_min: Optional[float] = 0.0
    r_max: Optional[float] = None
    tk_min: Optional[float] = 200.0
    tk_max: Optional[float] = 400.0
    p_min: Optional[float] = 0.0
    p_max: Optional[float] = None

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "ValueBoundsConfig":
        if not data:
            return cls()
        return cls(
            y_min=as_float(data.get("y_min")),
            y_max=as_float(data.get("y_max")),
            r_min=as_float(data.get("r_min"), default=0.0),
            r_max=as_float(data.get("r_max")),
            tk_min=as_float(data.get("tk_min"), default=200.0),
            tk_max=as_float(data.get("tk_max"), default=400.0),
            p_min=as_float(data.get("p_min"), default=0.0),
            p_max=as_float(data.get("p_max")),
        )


@dataclass
class OfflineColumnConfig:
    """离线点表字段映射。"""

    analyzer_id: str = "Analyzer"
    row_index: str = "PointRow"
    phase: str = "PointPhase"
    point_tag: str = "PointTag"
    point_title: str = "PointTitle"
    target: str = ""
    ratio: str = ""
    temperature: str = "T1"
    pressure: str = "BAR"
    tk: str = "T_k"

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "OfflineColumnConfig":
        if not data:
            return cls()
        return cls(
            analyzer_id=str(data.get("analyzer_id", "Analyzer")),
            row_index=str(data.get("row_index", "PointRow")),
            phase=str(data.get("phase", "PointPhase")),
            point_tag=str(data.get("point_tag", "PointTag")),
            point_title=str(data.get("point_title", "PointTitle")),
            target=str(data.get("target", "")),
            ratio=str(data.get("ratio", "")),
            temperature=str(data.get("temperature", "T1")),
            pressure=str(data.get("pressure", "BAR")),
            tk=str(data.get("tk", "T_k")),
        )


@dataclass
class RefitFilteringConfig:
    """二次筛选重拟合参数。"""

    enable_refit_filtering: bool = False
    temp_bin_size: float = 1.0
    press_bin_size: float = 10.0
    target_bins_co2: List[float] = field(default_factory=lambda: [0.0, 200.0, 500.0, 1000.0, 2000.0])
    target_bins_h2o: List[float] = field(default_factory=lambda: [0.0, 5.0, 10.0, 20.0, 40.0])
    mad_multiplier_group: float = 3.0
    mad_multiplier_residual: float = 3.0
    mad_min_group: float = 1e-4
    mad_min_residual: float = 1e-6
    min_group_size: int = 4
    max_remove_ratio_co2: float = 0.05
    max_remove_ratio_h2o: float = 0.03
    max_remove_per_bin: int = 2
    min_points_per_bin: int = 3
    low_range_protect_threshold_co2: float = 100.0
    low_range_extra_multiplier: float = 1.5
    temperature_offset_c: float = 273.15
    bounds: ValueBoundsConfig = field(default_factory=ValueBoundsConfig)

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "RefitFilteringConfig":
        if not data:
            return cls()
        return cls(
            enable_refit_filtering=as_bool(data.get("enable_refit_filtering"), default=False),
            temp_bin_size=as_float(data.get("temp_bin_size"), default=1.0, allow_none=False) or 1.0,
            press_bin_size=as_float(data.get("press_bin_size"), default=10.0, allow_none=False) or 10.0,
            target_bins_co2=[float(item) for item in data.get("target_bins_co2", [0.0, 200.0, 500.0, 1000.0, 2000.0])],
            target_bins_h2o=[float(item) for item in data.get("target_bins_h2o", [0.0, 5.0, 10.0, 20.0, 40.0])],
            mad_multiplier_group=as_float(data.get("mad_multiplier_group"), default=3.0, allow_none=False) or 3.0,
            mad_multiplier_residual=as_float(data.get("mad_multiplier_residual"), default=3.0, allow_none=False) or 3.0,
            mad_min_group=as_float(data.get("mad_min_group"), default=1e-4, allow_none=False) or 1e-4,
            mad_min_residual=as_float(data.get("mad_min_residual"), default=1e-6, allow_none=False) or 1e-6,
            min_group_size=as_int(data.get("min_group_size"), default=4, allow_none=False) or 4,
            max_remove_ratio_co2=as_float(data.get("max_remove_ratio_co2"), default=0.05, allow_none=False) or 0.05,
            max_remove_ratio_h2o=as_float(data.get("max_remove_ratio_h2o"), default=0.03, allow_none=False) or 0.03,
            max_remove_per_bin=as_int(data.get("max_remove_per_bin"), default=2, allow_none=False) or 2,
            min_points_per_bin=as_int(data.get("min_points_per_bin"), default=3, allow_none=False) or 3,
            low_range_protect_threshold_co2=as_float(data.get("low_range_protect_threshold_co2"), default=100.0, allow_none=False) or 100.0,
            low_range_extra_multiplier=as_float(data.get("low_range_extra_multiplier"), default=1.5, allow_none=False) or 1.5,
            temperature_offset_c=as_float(data.get("temperature_offset_c"), default=273.15, allow_none=False) or 273.15,
            bounds=ValueBoundsConfig.from_dict(data.get("bounds")),
        )


@dataclass
class SimplificationConfig:
    """系数简化配置。"""

    enabled: bool = True
    method: str = "column_norm"
    target_digits: int = 6

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "SimplificationConfig":
        if not data:
            return cls()
        return cls(
            enabled=as_bool(data.get("enabled"), default=True),
            method=str(data.get("method", "column_norm")),
            target_digits=as_int(data.get("target_digits"), default=6, allow_none=False) or 6,
        )


@dataclass
class OfflineRefitConfig:
    """离线筛选重拟合完整配置。"""

    enabled: bool = False
    gas_type: str = "co2"
    columns: OfflineColumnConfig = field(default_factory=OfflineColumnConfig)
    filtering: RefitFilteringConfig = field(default_factory=RefitFilteringConfig)
    simplification: SimplificationConfig = field(default_factory=SimplificationConfig)
    output_dir: str = "logs/v2_offline_refit"

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "OfflineRefitConfig":
        if not data:
            return cls()
        return cls(
            enabled=as_bool(data.get("enabled"), default=False),
            gas_type=str(data.get("gas_type", "co2")).strip().lower() or "co2",
            columns=OfflineColumnConfig.from_dict(data.get("columns")),
            filtering=RefitFilteringConfig.from_dict(data.get("filtering")),
            simplification=SimplificationConfig.from_dict(data.get("simplification")),
            output_dir=str(data.get("output_dir", "logs/v2_offline_refit")),
        )
