from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from .enums import QCLevel
from .sample_models import RawSample

if TYPE_CHECKING:
    from ..qc.outlier_detector import OutlierResult


@dataclass
class QCDecision:
    """QC decision for one point or stage."""

    point_index: int
    level: QCLevel
    accepted: bool
    reasons: list[str] = field(default_factory=list)
    score: float = 0.0


@dataclass
class CleanedData:
    """Cleaned samples after QC filtering."""

    point_index: int
    original_count: int
    cleaned_count: int
    removed_count: int
    removed_indices: list[int] = field(default_factory=list)
    samples: list[RawSample] = field(default_factory=list)
    outlier_result: Optional["OutlierResult"] = None
