from types import SimpleNamespace

from gas_calibrator.v2.config import AIConfig, AIFeaturesConfig
from gas_calibrator.v2.intelligence.explainers import QCExplainer
from gas_calibrator.v2.intelligence.llm_client import LLMConfig, MockLLMClient


def _ai_config() -> AIConfig:
    return AIConfig(
        enabled=True,
        provider="mock",
        features=AIFeaturesConfig(qc_explanation=True),
    )


def test_qc_explainer_returns_structured_fallback() -> None:
    explainer = QCExplainer(MockLLMClient(LLMConfig(provider="mock", model="mock")), _ai_config())
    validation = SimpleNamespace(
        valid=False,
        quality_score=0.35,
        reason="outlier_ratio_too_high",
        failed_checks=[
            {
                "rule_name": "outlier_ratio",
                "actual": 0.5,
                "threshold": 0.2,
                "message": "Outlier ratio is above the configured limit",
            }
        ],
        usable_sample_count=2,
    )
    cleaned = SimpleNamespace(cleaned_count=2, removed_count=2)
    point = SimpleNamespace(index=1, route="co2", temperature_c=25.0)

    text = explainer.explain_failure(1, validation, cleaned_data=cleaned, point=point)

    assert 'QC 规则 "outlier_ratio" 失败' in text
    assert "建议：" in text
    assert "剔除样本 2 条" in text


def test_qc_explainer_batch() -> None:
    explainer = QCExplainer(MockLLMClient(LLMConfig(provider="mock", model="mock")), _ai_config())
    results = [
        (
            1,
            SimpleNamespace(
                valid=False,
                quality_score=0.3,
                reason="usable_sample_count_insufficient",
                failed_checks=[{"rule_name": "usable_sample_count", "actual": 2, "threshold": 5, "message": "too few"}],
                usable_sample_count=2,
            ),
            SimpleNamespace(cleaned_count=2, removed_count=0),
        ),
        (
            2,
            SimpleNamespace(
                valid=False,
                quality_score=0.2,
                reason="communication_error",
                failed_checks=[{"rule_name": "communication_error", "actual": 1, "threshold": 0, "message": "comm"}],
                usable_sample_count=0,
            ),
            SimpleNamespace(cleaned_count=0, removed_count=4),
        ),
    ]

    texts = explainer.explain_batch(results)

    assert len(texts) == 2
    assert all("建议：" in text for text in texts)
