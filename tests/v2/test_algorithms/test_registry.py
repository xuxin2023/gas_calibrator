from gas_calibrator.v2.algorithms.linear import LinearAlgorithm
from gas_calibrator.v2.algorithms import AlgorithmRegistry, ComparisonResult, ValidationResult


def test_registry_register_and_get() -> None:
    registry = AlgorithmRegistry()
    registry.register("linear", LinearAlgorithm)

    instance = registry.get("linear", {"tolerance": 0.1})

    assert isinstance(instance, LinearAlgorithm)
    assert registry.list_algorithms() == ["linear"]


def test_registry_register_default_algorithms() -> None:
    registry = AlgorithmRegistry()
    registry.register_default_algorithms()

    names = registry.list_algorithms()
    assert "linear" in names
    assert "polynomial" in names
    assert "amt" in names


def test_algorithm_package_types_have_one_owner() -> None:
    assert AlgorithmRegistry.__module__ == "gas_calibrator.v2.algorithms"
    assert ComparisonResult.__module__ == "gas_calibrator.v2.algorithms"
    assert ValidationResult.__module__ == "gas_calibrator.v2.algorithms"
