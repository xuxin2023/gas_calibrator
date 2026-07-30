from __future__ import annotations

from dataclasses import asdict, dataclass, field
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


@dataclass
class QCContext:
    """Normalized QC explanation payload."""

    point_index: int
    qc_score: float
    passed: bool
    reject_reasons: list[str] = field(default_factory=list)
    sample_count: int = 0
    outlier_count: int = 0
    action: str = "接受"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _normalize_reasons(validation_result: Any) -> list[str]:
    reasons = getattr(validation_result, "reasons", None)
    if isinstance(reasons, list):
        return [str(item) for item in reasons]
    reason = getattr(validation_result, "reason", "")
    if not reason:
        return []
    return [item.strip() for item in str(reason).split(",") if item.strip()]


def build_qc_context(
    point_index: int,
    validation_result: Any,
    cleaned_data: Any = None,
) -> QCContext:
    """Build a normalized context for QC explanation."""

    passed = bool(getattr(validation_result, "valid", getattr(validation_result, "passed", True)))
    score = float(getattr(validation_result, "quality_score", getattr(validation_result, "score", 0.0)) or 0.0)
    reasons = _normalize_reasons(validation_result)
    sample_count = int(getattr(validation_result, "usable_sample_count", getattr(validation_result, "sample_count", 0)) or 0)
    outlier_count = int(getattr(validation_result, "outlier_count", 0) or 0)

    if cleaned_data is not None:
        sample_count = int(getattr(cleaned_data, "cleaned_count", sample_count) or sample_count)
        outlier_count = int(getattr(cleaned_data, "removed_count", outlier_count) or outlier_count)

    return QCContext(
        point_index=point_index,
        qc_score=score,
        passed=passed,
        reject_reasons=reasons,
        sample_count=sample_count,
        outlier_count=outlier_count,
        action="接受" if passed else "剔除",
    )


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

__all__ = [
    "FitContext",
    "QCContext",
    "RunContext",
    "build_fit_context",
    "build_qc_context",
    "build_run_context",
]
