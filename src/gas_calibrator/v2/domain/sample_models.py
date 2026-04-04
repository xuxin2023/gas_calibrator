from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


@dataclass
class RawSample:
    """Raw sample collected from one analyzer at one timestamp."""

    timestamp: datetime
    point_index: int
    analyzer_name: str
    co2: Optional[float] = None
    h2o: Optional[float] = None
    pressure: Optional[float] = None
    temperature_c: Optional[float] = None
    dewpoint: Optional[float] = None
    chamber_temp_c: Optional[float] = None
    case_temp_c: Optional[float] = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class SampleWindow:
    """Window of raw samples collected for one point."""

    point_index: int
    started_at: datetime
    ended_at: Optional[datetime] = None
    samples: list[RawSample] = field(default_factory=list)
