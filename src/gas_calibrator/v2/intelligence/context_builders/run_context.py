from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class RunContext:
    """Normalized run summary payload."""

    run_id: str
    total_points: int
    valid_points: int
    invalid_points: int
    overall_score: float
    algorithm: str
    r_squared: float
    rmse: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_run_context(
    session: Any,
    fit_result: Any = None,
    quality_score: Any = None,
) -> RunContext:
    """Build a normalized context for run explanation."""

    total_points = int(
        getattr(session, "total_points", 0)
        or len(getattr(session, "points", []) or [])
        or len(getattr(session, "point_results", []) or [])
    )
    valid_points = int(
        getattr(quality_score, "valid_points", 0)
        or getattr(session, "valid_points", 0)
        or total_points
    )
    invalid_points = max(0, total_points - valid_points)
    return RunContext(
        run_id=str(getattr(session, "run_id", "")),
        total_points=total_points,
        valid_points=valid_points,
        invalid_points=invalid_points,
        overall_score=float(getattr(quality_score, "overall_score", getattr(quality_score, "score", 0.0)) or 0.0),
        algorithm=str(getattr(fit_result, "algorithm_name", "")) if fit_result is not None else "",
        r_squared=float(getattr(fit_result, "r_squared", 0.0) or 0.0) if fit_result is not None else 0.0,
        rmse=float(getattr(fit_result, "rmse", 0.0) or 0.0) if fit_result is not None else 0.0,
    )
