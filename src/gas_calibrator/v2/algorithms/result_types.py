from __future__ import annotations

from dataclasses import dataclass, field

from ..domain.algorithm_models import FitResult


@dataclass
class ValidationResult:
    """Back-validation result."""

    algorithm_name: str
    passed: bool
    r_squared: float
    rmse: float
    mae: float
    sample_count: int
    outliers: list[int] = field(default_factory=list)
    message: str = ""


@dataclass
class ComparisonResult:
    """Multi-algorithm comparison result."""

    best_algorithm: str
    results: dict[str, FitResult]
    ranking: list[str]
    recommendation: str
    ai_recommendation: str = ""
    metrics: dict[str, dict[str, float]] = field(default_factory=dict)
