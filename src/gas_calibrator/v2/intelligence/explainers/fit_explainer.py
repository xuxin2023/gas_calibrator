from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

from ..context_builders.fit_context import build_fit_context
from ..llm_client import LLMClient, LLMConfig, MockLLMClient


def _format_range(values: list[float]) -> str:
    if not values:
        return "n/a"
    return f"{min(values):.3f} ~ {max(values):.3f}"


class FitExplainer:
    """LLM-powered fit result explainer."""

    def __init__(self, llm_client: LLMClient | None = None):
        self.llm = llm_client or MockLLMClient(LLMConfig(provider="mock", model="mock"))
        self._load_prompt()

    def _load_prompt(self) -> None:
        prompt_path = Path(__file__).parent.parent / "prompts" / "algorithm_recommend.txt"
        self.prompt_template = prompt_path.read_text(encoding="utf-8")

    def explain(self, fit_result: Any, point_results: list[Any]) -> str:
        context = build_fit_context(fit_result, point_results)
        co2_values = [float(getattr(point, "mean_co2", 0.0)) for point in point_results if getattr(point, "mean_co2", None) is not None]
        h2o_values = [float(getattr(point, "mean_h2o", 0.0)) for point in point_results if getattr(point, "mean_h2o", None) is not None]
        temp_values = [float(getattr(point, "temperature_c", 0.0)) for point in point_results if getattr(point, "temperature_c", None) is not None]
        prompt = self.prompt_template.format(
            point_count=context.point_count,
            valid_points=context.valid_points,
            quality_score=context.quality_score,
            co2_range=_format_range(co2_values),
            h2o_range=_format_range(h2o_values),
            temp_range=_format_range(temp_values),
            candidate_algorithms=context.algorithm or "n/a",
            fit_results=(
                f"algorithm={context.algorithm}, R²={context.r_squared:.4f}, "
                f"RMSE={context.rmse:.4f}, MAE={context.mae:.4f}, confidence={context.confidence:.2f}"
            ),
        )
        return self.llm.complete(prompt)
