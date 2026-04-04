from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional

from ..domain.algorithm_models import AlgorithmSpec
from .result_types import FitResult, ValidationResult


class AlgorithmBase(ABC):
    """Base class for all calibration algorithms."""

    def __init__(self, name: str, config: Optional[dict[str, Any]] = None):
        self.name = name
        self.config = config or {}

    @abstractmethod
    def fit(self, samples: list[Any], point_results: list[Any]) -> FitResult:
        ...

    @abstractmethod
    def validate(self, fit_result: FitResult, samples: list[Any]) -> ValidationResult:
        ...

    @abstractmethod
    def predict(self, coefficients: dict[str, float], inputs: dict[str, float]) -> float:
        ...

    def export_coefficients(self, fit_result: FitResult) -> dict[str, float]:
        return fit_result.coefficients

    def explain(self, fit_result: FitResult) -> str:
        return fit_result.explain()

    def get_spec(self) -> AlgorithmSpec:
        return AlgorithmSpec(
            name=self.name,
            display_name=self.name.replace("_", " ").title(),
            description=(self.__doc__ or "").strip() or f"{self.name} algorithm",
            min_points=int(self.config.get("min_points", 3)),
            max_degree=int(self.config.get("degree", 3)),
        )

    @staticmethod
    def _read_value(item: Any, *keys: str) -> Any:
        for key in keys:
            if isinstance(item, dict) and key in item:
                value = item[key]
                if value is not None:
                    return value
            value = getattr(item, key, None)
            if value is not None:
                return value
        return None

    def _extract_point_pairs(
        self,
        point_results: list[Any],
        *,
        x_keys: tuple[str, ...] = ("mean_co2", "x", "co2"),
        y_keys: tuple[str, ...] = ("mean_h2o", "y", "h2o"),
    ) -> tuple[list[float], list[float]]:
        xs: list[float] = []
        ys: list[float] = []
        for item in point_results:
            x = self._read_value(item, *x_keys)
            y = self._read_value(item, *y_keys)
            if x is None or y is None:
                continue
            xs.append(float(x))
            ys.append(float(y))
        return xs, ys

    def _extract_sample_pairs(
        self,
        samples: list[Any],
    ) -> tuple[list[float], list[float]]:
        xs: list[float] = []
        ys: list[float] = []
        for item in samples:
            extra = self._read_value(item, "extra")
            x = None
            y = None
            if isinstance(extra, dict):
                x = extra.get("x")
                y = extra.get("y")
            if x is None:
                x = self._read_value(item, "co2", "co2_signal", "x")
            if y is None:
                y = self._read_value(item, "h2o", "h2o_signal", "y")
            if x is None or y is None:
                continue
            xs.append(float(x))
            ys.append(float(y))
        return xs, ys
