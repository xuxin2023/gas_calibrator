from datetime import datetime, timedelta

from gas_calibrator.v2.config import QCConfig
from gas_calibrator.v2.domain.point_models import CalibrationPoint
from gas_calibrator.v2.domain.sample_models import RawSample
from gas_calibrator.v2.qc.pipeline import QCPipeline
from gas_calibrator.v2.qc.rule_templates import ModeType, RouteType


def _samples(values: list[float]) -> list[RawSample]:
    base = datetime(2026, 3, 18, 9, 0, 0)
    return [
        RawSample(
            timestamp=base + timedelta(seconds=index),
            point_index=1,
            analyzer_name="ga01",
            co2=value,
            h2o=5.0 + index * 0.01,
        )
        for index, value in enumerate(values)
    ]


def test_pipeline_can_switch_to_route_rule() -> None:
    pipeline = QCPipeline(QCConfig(min_sample_count=3))
    pipeline.set_rule_for_route_mode(RouteType.CO2, ModeType.NORMAL)

    assert pipeline._current_rule is not None
    assert pipeline._current_rule.name == "co2_strict"
    assert pipeline.sample_checker.min_count == 10
    assert pipeline.outlier_detector.z_threshold == 2.5


def test_pipeline_can_switch_to_fast_mode_rule_and_process_point() -> None:
    pipeline = QCPipeline(QCConfig(min_sample_count=5))
    point = CalibrationPoint(index=1, name="P1", target_co2=10.0)
    point.mode = "fast"
    point.route = "co2"

    pipeline.set_rule_for_route_mode(RouteType.CO2, ModeType.FAST)
    cleaned, validation, score = pipeline.process_point(
        point,
        _samples([10.0, 10.1, 10.2]),
        point_index=1,
        return_cleaned=True,
    )

    assert pipeline._current_rule is not None
    assert pipeline._current_rule.name == "fast_mode"
    assert len(cleaned) == 3
    assert validation.point_index == 1
    assert 0.0 <= score <= 1.0
