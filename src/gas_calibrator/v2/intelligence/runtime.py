from __future__ import annotations

from dataclasses import dataclass

from ..config import AIConfig
from .advisors import AlgorithmAdvisor, AnomalyAdvisor
from .explainers import QCExplainer
from .llm_client import LLMConfig, create_llm_client
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
