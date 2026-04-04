from gas_calibrator.v2.algorithms.engine import AlgorithmEngine
from gas_calibrator.v2.algorithms.registry import AlgorithmRegistry
from gas_calibrator.v2.domain.result_models import PointResult


def _quadratic_points() -> list[PointResult]:
    return [
        PointResult(point_index=1, mean_co2=0.0, mean_h2o=1.0),
        PointResult(point_index=2, mean_co2=1.0, mean_h2o=6.0),
        PointResult(point_index=3, mean_co2=2.0, mean_h2o=17.0),
        PointResult(point_index=4, mean_co2=3.0, mean_h2o=34.0),
    ]


def test_engine_compare_and_auto_select() -> None:
    registry = AlgorithmRegistry()
    registry.register_default_algorithms()
    engine = AlgorithmEngine(registry)

    comparison = engine.compare(["linear", "polynomial"], [], _quadratic_points())
    selected = engine.auto_select([], _quadratic_points(), ["linear", "polynomial"])

    assert comparison.best_algorithm == "polynomial"
    assert comparison.ranking[0] == "polynomial"
    assert selected.algorithm_name == "polynomial"
