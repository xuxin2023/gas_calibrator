from __future__ import annotations

from dataclasses import dataclass

from gas_calibrator.validation.simulation.config import AIConfig
from .advisors import AlgorithmAdvisor, AnomalyAdvisor
from .context_builders import (
    FitContext,
    QCContext,
    RunContext,
    build_fit_context,
    build_qc_context,
    build_run_context,
)
from .explainers import FitExplainer, QCExplainer, RunExplainer
from .llm_client import LLMClient, LLMConfig, MockLLMClient, OpenAIClient, create_llm_client
from .summarizer import Summarizer


@dataclass
class AIRuntime:
    """Shared AI helper bundle for one service instance."""

    config: AIConfig
    llm: object
    summarizer: Summarizer
    qc_explainer: QCExplainer
    anomaly_advisor: AnomalyAdvisor
    algorithm_advisor: AlgorithmAdvisor

    @classmethod
    def from_config(cls, config: AIConfig | None) -> "AIRuntime":
        effective = config or AIConfig()
        llm = create_llm_client(
            LLMConfig(
                provider=effective.provider,
                model=effective.model,
                api_key=effective.api_key or None,
                base_url=effective.base_url or None,
                max_tokens=effective.max_tokens,
                temperature=effective.temperature,
                timeout_s=effective.timeout_s,
                max_retries=effective.max_retries,
                fallback_to_mock=effective.fallback_to_mock,
            )
        )
        return cls(
            config=effective,
            llm=llm,
            summarizer=Summarizer(llm, effective),
            qc_explainer=QCExplainer(llm, effective),
            anomaly_advisor=AnomalyAdvisor(llm, effective),
            algorithm_advisor=AlgorithmAdvisor(llm, effective),
        )

    def feature_enabled(self, name: str) -> bool:
        return self.config.feature_enabled(name)


__all__ = [
    "AIRuntime",
    "AlgorithmAdvisor",
    "AnomalyAdvisor",
    "FitContext",
    "FitExplainer",
    "LLMClient",
    "LLMConfig",
    "MockLLMClient",
    "OpenAIClient",
    "QCContext",
    "QCExplainer",
    "RunContext",
    "RunExplainer",
    "Summarizer",
    "build_fit_context",
    "build_qc_context",
    "build_run_context",
    "create_llm_client",
]
