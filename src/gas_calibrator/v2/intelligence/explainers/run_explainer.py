from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

from ..context_builders.run_context import build_run_context
from ..llm_client import LLMClient, LLMConfig, MockLLMClient


class RunExplainer:
    """LLM-powered run summary helper."""

    def __init__(self, llm_client: LLMClient | None = None):
        self.llm = llm_client or MockLLMClient(LLMConfig(provider="mock", model="mock"))
        self._load_prompt()

    def _load_prompt(self) -> None:
        prompt_path = Path(__file__).parent.parent / "prompts" / "report_summary.txt"
        self.prompt_template = prompt_path.read_text(encoding="utf-8")

    def explain(
        self,
        session: Any,
        fit_result: Any = None,
        quality_score: Any = None,
    ) -> str:
        context = build_run_context(session, fit_result, quality_score)
        prompt = self.prompt_template.format(**asdict(context))
        return self.llm.complete(prompt)
