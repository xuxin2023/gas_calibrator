from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Optional

import numpy as np

from .result_models import PointResult
from .sample_models import RawSample


@dataclass
class FitPoint:
    """Single point used by fitting algorithms."""

    index: int
    target: float
    ratio: float
    temperature_c: float
    pressure_hpa: float
    humidity: Optional[float] = None
    weight: float = 1.0
    valid: bool = True
    reject_reason: str = ""

    def explain(self) -> str:
        status = "valid" if self.valid else f"invalid ({self.reject_reason or 'unspecified'})"
        return f"Point {self.index}: target={self.target}, ratio={self.ratio}, {status}"


@dataclass
class FitDataset:
    """Standard fit dataset shared by all algorithms."""

    run_id: str
    gas_type: str
    points: list[FitPoint] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_arrays(self) -> tuple[np.ndarray, np.ndarray]:
        valid_points = [point for point in self.points if point.valid]
        x_values = np.asarray([point.ratio for point in valid_points], dtype=float)
        y_values = np.asarray([point.target for point in valid_points], dtype=float)
        return x_values, y_values

    def validate(self) -> bool:
        if self.gas_type not in {"co2", "h2o"}:
            return False
        valid_points = [point for point in self.points if point.valid]
        if len(valid_points) < 2:
            return False
        return all(
            np.isfinite(point.target)
            and np.isfinite(point.ratio)
            and np.isfinite(point.temperature_c)
            and np.isfinite(point.pressure_hpa)
            and point.weight > 0
            for point in valid_points
        )

    def explain(self) -> str:
        valid_count = sum(1 for point in self.points if point.valid)
        return f"Dataset {self.run_id} ({self.gas_type}): {valid_count}/{len(self.points)} valid points"


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
class FitInput:
    """Input payload for fitting algorithms."""

    samples: list[RawSample] = field(default_factory=list)
    point_results: list[PointResult] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)
    dataset: Optional[FitDataset] = None

    def explain(self) -> str:
        if self.dataset is not None:
            return self.dataset.explain()
        return f"FitInput: {len(self.samples)} samples, {len(self.point_results)} point results"


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
class CoefficientSet:
    """Persistable coefficient set with quality metadata."""

    run_id: str
    algorithm_name: str
    coefficients: dict[str, float]
    r_squared: float = 0.0
    confidence: float = 0.0
    validation_passed: bool = True
    created_at: datetime = field(default_factory=datetime.now)
    gas_type: str = "co2"
    point_count: int = 0
    valid: bool = True
    notes: str = ""

    def explain(self) -> str:
        return (
            f"CoefficientSet {self.algorithm_name}: R²={self.r_squared:.4f}, "
            f"confidence={self.confidence:.2f}, valid={self.valid}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "algorithm_name": self.algorithm_name,
            "coefficients": dict(self.coefficients),
            "r_squared": self.r_squared,
            "confidence": self.confidence,
            "validation_passed": self.validation_passed,
            "created_at": self.created_at.isoformat(timespec="seconds"),
            "gas_type": self.gas_type,
            "point_count": self.point_count,
            "valid": self.valid,
            "notes": self.notes,
        }

    @classmethod
    def from_fit_result(cls, fit_result: FitResult, run_id: str) -> "CoefficientSet":
        gas_type = "co2"
        if fit_result.algorithm_spec and fit_result.algorithm_spec.supported_gases:
            gas_type = fit_result.algorithm_spec.supported_gases[0]
        residual_count = len(fit_result.residuals or [])
        return cls(
            run_id=run_id,
            algorithm_name=fit_result.algorithm_name,
            coefficients=dict(fit_result.coefficients),
            r_squared=fit_result.r_squared,
            confidence=fit_result.confidence,
            validation_passed=fit_result.valid,
            gas_type=gas_type,
            point_count=residual_count,
            valid=fit_result.valid,
            notes=fit_result.message,
        )
