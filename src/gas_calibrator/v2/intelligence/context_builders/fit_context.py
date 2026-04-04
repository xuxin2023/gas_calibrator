from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class FitContext:
    """Normalized fit explanation payload."""

    algorithm: str
    r_squared: float
    rmse: float
    mae: float
    confidence: float
    point_count: int
    valid_points: int
    quality_score: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_fit_context(
    fit_result: Any,
    point_results: list[Any],
    quality_score: float = 0.0,
) -> FitContext:
    """Build a normalized context for fit explanation."""

    valid_points = sum(
        1
        for point in point_results
        if bool(getattr(point, "accepted", getattr(point, "valid", True)))
    )
    return FitContext(
        algorithm=str(getattr(fit_result, "algorithm_name", "")),
        r_squared=float(getattr(fit_result, "r_squared", 0.0) or 0.0),
        rmse=float(getattr(fit_result, "rmse", 0.0) or 0.0),
        mae=float(getattr(fit_result, "mae", 0.0) or 0.0),
        confidence=float(getattr(fit_result, "confidence", 0.0) or 0.0),
        point_count=len(point_results),
        valid_points=valid_points,
        quality_score=float(quality_score or 0.0),
    )
