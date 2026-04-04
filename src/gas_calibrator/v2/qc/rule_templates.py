from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class RouteType(str, Enum):
    """Supported route types for QC rules."""

    CO2 = "co2"
    H2O = "h2o"
    BOTH = "both"


class ModeType(str, Enum):
    """Supported operating modes for QC rules."""

    NORMAL = "normal"
    FAST = "fast"
    VERIFY = "verify"
    SUBZERO = "subzero"


@dataclass
class SampleCountRule:
    """Sample-count QC rule."""

    min_count: int = 5
    max_missing: int = 2
    weight: float = 1.0


@dataclass
class StabilityRule:
    """Stability QC rule."""

    co2_max_std: float = 2.0
    h2o_max_std: float = 0.5
    pressure_max_std: float = 1.0
    temperature_max_std: float = 0.5
    window_s: float = 2.0
    weight: float = 1.0


@dataclass
class OutlierRule:
    """Outlier QC rule."""

    z_threshold: float = 3.0
    max_outlier_ratio: float = 0.2
    method: str = "z_score"
    weight: float = 1.0


@dataclass
class QualityThreshold:
    """Quality-score threshold rule."""

    min_score: float = 0.6
    pass_threshold: float = 0.7
    warn_threshold: float = 0.5
    reject_threshold: float = 0.3


@dataclass
class QCRuleTemplate:
    """QC rule template definition."""

    name: str
    route: RouteType = RouteType.BOTH
    mode: ModeType = ModeType.NORMAL
    sample_count: SampleCountRule = field(default_factory=SampleCountRule)
    stability: StabilityRule = field(default_factory=StabilityRule)
    outlier: OutlierRule = field(default_factory=OutlierRule)
    quality: QualityThreshold = field(default_factory=QualityThreshold)
    description: str = ""
    tags: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["route"] = self.route.value
        payload["mode"] = self.mode.value
        return payload

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QCRuleTemplate":
        return cls(
            name=str(data.get("name", "unnamed")),
            route=RouteType(str(data.get("route", RouteType.BOTH.value))),
            mode=ModeType(str(data.get("mode", ModeType.NORMAL.value))),
            sample_count=SampleCountRule(**dict(data.get("sample_count", {}))),
            stability=StabilityRule(**dict(data.get("stability", {}))),
            outlier=OutlierRule(**dict(data.get("outlier", {}))),
            quality=QualityThreshold(**dict(data.get("quality", {}))),
            description=str(data.get("description", "")),
            tags=list(data.get("tags", [])),
        )
