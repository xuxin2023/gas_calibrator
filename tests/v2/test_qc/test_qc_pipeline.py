from datetime import datetime, timedelta, timezone

from gas_calibrator.v2.config import AIConfig, AIFeaturesConfig, QCConfig
from gas_calibrator.v2.core.models import CalibrationPoint, SamplingResult
from gas_calibrator.v2.intelligence.explainers import QCExplainer
from gas_calibrator.v2.intelligence.llm_client import LLMConfig, MockLLMClient
from gas_calibrator.v2.qc.pipeline import QCPipeline


def _samples(point: CalibrationPoint, values: list[float]) -> list[SamplingResult]:
    base = datetime(2026, 3, 18, 9, 0, 0, tzinfo=timezone.utc)
    return [
        SamplingResult(point=point, analyzer_id="ga01", timestamp=base + timedelta(seconds=index), co2_signal=value)
        for index, value in enumerate(values)
    ]


def test_qc_pipeline_processes_run() -> None:
    pipeline = QCPipeline(QCConfig(min_sample_count=3), run_id="run_qc")
    point1 = CalibrationPoint(index=1, temperature_c=25.0, route="co2")
    point2 = CalibrationPoint(index=2, temperature_c=25.0, route="h2o")

    validations, run_score, report = pipeline.process_run(
        [
            (point1, _samples(point1, [100.0, 101.0, 102.0, 103.0])),
            (point2, _samples(point2, [50.0, 80.0, 50.0, 49.0])),
        ]
    )

    assert len(validations) == 2
    assert report.run_id == "run_qc"
    assert report.total_points == 2
    assert 0.0 <= run_score.overall_score <= 1.0


def test_qc_pipeline_attaches_ai_explanation() -> None:
    ai_config = AIConfig(
        enabled=True,
        provider="mock",
        features=AIFeaturesConfig(qc_explanation=True),
    )
    pipeline = QCPipeline(
        QCConfig(min_sample_count=5),
        run_id="run_qc_ai",
        qc_explainer=QCExplainer(MockLLMClient(LLMConfig(provider="mock", model="mock")), ai_config),
        ai_config=ai_config,
    )
    point = CalibrationPoint(index=3, temperature_c=25.0, route="co2")

    _, validation, _ = pipeline.process_point(
        point,
        _samples(point, [100.0, 101.0, 102.0, 103.0]),
        point_index=point.index,
        return_cleaned=True,
    )

    assert validation.valid is False
    assert validation.failed_checks
    assert validation.ai_explanation
    assert "建议：" in validation.ai_explanation
