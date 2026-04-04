from __future__ import annotations

from typing import Optional, Type

from .base import AlgorithmBase


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
