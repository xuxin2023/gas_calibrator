from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Optional, Type


@dataclass
class AlgorithmSpec:
    """Declarative description of an algorithm plugin."""

    name: str
    display_name: str
    description: str
    supported_gases: list[str] = field(default_factory=lambda: ["co2", "h2o"])
    required_features: list[str] = field(default_factory=list)
    parameter_schema: dict[str, Any] = field(default_factory=dict)
    min_points: int = 3
    max_degree: int = 3

    def explain(self) -> str:
        return f"{self.display_name}: supports {', '.join(self.supported_gases)}"


@dataclass
class FitResult:
    """Enhanced fit result contract."""

    algorithm_name: str
    algorithm_spec: Optional[AlgorithmSpec] = None
    coefficients: dict[str, float] = field(default_factory=dict)
    coefficient_names: list[str] = field(default_factory=list)
    r_squared: float = 0.0
    adjusted_r_squared: float = 0.0
    rmse: float = 0.0
    mae: float = 0.0
    max_error: float = 0.0
    confidence: float = 0.0
    confidence_level: str = "low"
    residuals: Optional[list[float]] = None
    leverage_points: list[int] = field(default_factory=list)
    influential_points: list[int] = field(default_factory=list)
    valid: bool = True
    message: str = ""
    warnings: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)

    def __post_init__(self) -> None:
        if not self.coefficient_names and self.coefficients:
            self.coefficient_names = list(self.coefficients.keys())

    def explain(self) -> str:
        return (
            f"Algorithm: {self.algorithm_name}, R²={self.r_squared:.4f}, "
            f"Confidence={self.confidence:.2f} ({self.confidence_level})"
        )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["created_at"] = self.created_at.isoformat(timespec="seconds")
        return payload


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


from .base import AlgorithmBase  # noqa: E402


class AlgorithmRegistry:
    """Registry for algorithm classes and configured instances."""

    def __init__(self):
        self._algorithms: dict[str, Type[AlgorithmBase]] = {}
        self._instances: dict[str, AlgorithmBase] = {}

    def register(self, name: str, algorithm_class: Type[AlgorithmBase]) -> None:
        self._algorithms[name] = algorithm_class

    def get(self, name: str, config: Optional[dict] = None) -> AlgorithmBase:
        if name not in self._algorithms:
            raise ValueError(f"Algorithm not found: {name}")

        key = f"{name}_{id(config)}"
        if key not in self._instances:
            self._instances[key] = self._algorithms[name](name, config)
        return self._instances[key]

    def list_algorithms(self) -> list[str]:
        return list(self._algorithms.keys())

    def register_default_algorithms(self) -> None:
        from .amt import AMTAlgorithm
        from .linear import LinearAlgorithm
        from .polynomial import PolynomialAlgorithm
        from .robust import RobustAlgorithm

        self.register("linear", LinearAlgorithm)
        self.register("polynomial", PolynomialAlgorithm)
        self.register("amt", AMTAlgorithm)
        self.register("robust", RobustAlgorithm)


from .amt import AMTAlgorithm  # noqa: E402
from .engine import AlgorithmEngine  # noqa: E402
from .linear import LinearAlgorithm  # noqa: E402
from .polynomial import PolynomialAlgorithm  # noqa: E402
from .robust import RobustAlgorithm  # noqa: E402
from .validator import BackValidator  # noqa: E402

__all__ = [
    "AMTAlgorithm",
    "AlgorithmBase",
    "AlgorithmEngine",
    "AlgorithmRegistry",
    "AlgorithmSpec",
    "BackValidator",
    "ComparisonResult",
    "FitResult",
    "LinearAlgorithm",
    "PolynomialAlgorithm",
    "RobustAlgorithm",
    "ValidationResult",
]
