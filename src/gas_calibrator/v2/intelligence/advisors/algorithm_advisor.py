from __future__ import annotations

import json
from typing import Any

from ...config import AIConfig
from ..llm_client import LLMClient, LLMConfig, MockLLMClient, complete_with_fallback


class AlgorithmAdvisor:
    """LLM-based algorithm recommendation helper with deterministic fallback."""

    def __init__(self, llm_client: LLMClient | None = None, config: AIConfig | None = None):
        self.config = config or AIConfig()
        self.llm = llm_client or MockLLMClient(LLMConfig(provider="mock", model="mock"))

    def recommend(
        self,
        fit_results: dict[str, Any],
        dataset_features: dict[str, Any],
    ) -> str:
        compact_results = {
            name: {
                "valid": bool(getattr(result, "valid", False)),
                "r_squared": float(getattr(result, "r_squared", 0.0) or 0.0),
                "rmse": float(getattr(result, "rmse", 0.0) or 0.0),
                "mae": float(getattr(result, "mae", 0.0) or 0.0),
                "confidence": float(getattr(result, "confidence", 0.0) or 0.0),
                "message": str(getattr(result, "message", "") or ""),
            }
            for name, result in fit_results.items()
        }
        fallback = self._build_fallback_recommendation(compact_results, dataset_features)
        prompt = json.dumps(
            {
                "task": "Recommend the best calibration fitting algorithm in concise Chinese.",
                "dataset_features": dataset_features,
                "fit_results": compact_results,
                "requirements": [
                    "Name the recommended algorithm first",
                    "Explain why it is preferred",
                    "Compare key candidate metrics",
                    "Mention data sufficiency and tradeoff between precision and complexity",
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
        return complete_with_fallback(self.llm, prompt, fallback, temperature=0.2, max_tokens=900)

    def _build_fallback_recommendation(
        self,
        fit_results: dict[str, dict[str, Any]],
        dataset_features: dict[str, Any],
    ) -> str:
        if not fit_results:
            return "当前没有可用拟合结果，无法给出算法推荐。"
        ranking = sorted(
            fit_results.items(),
            key=lambda item: (
                bool(item[1].get("valid")),
                float(item[1].get("r_squared", 0.0)),
                -float(item[1].get("rmse", 0.0)),
                float(item[1].get("confidence", 0.0)),
            ),
            reverse=True,
        )
        best_name, best = ranking[0]
        point_count = int(dataset_features.get("point_count", 0) or 0)
        valid_points = int(dataset_features.get("valid_points", point_count) or point_count)
        lines = [f"推荐算法：{best_name}"]
        lines.append("")
        lines.append("推荐理由：")
        lines.append(f"- 数据点数：{point_count} 个，可用点位 {valid_points} 个。")
        lines.append(f"- 该算法的 R²={float(best.get('r_squared', 0.0)):.4f}，RMSE={float(best.get('rmse', 0.0)):.4f}，置信度={float(best.get('confidence', 0.0)):.2f}。")
        if point_count >= 4:
            lines.append("- 数据量足以支持中高复杂度模型，能够更充分利用非线性信息。")
        else:
            lines.append("- 数据点较少，应优先选择泛化风险更低的模型。")
        lines.append("")
        lines.append("算法比较：")
        lines.append("| 算法 | 有效 | RMSE | R² | 置信度 |")
        lines.append("|------|------|------|----|--------|")
        for name, result in ranking:
            lines.append(
                f"| {name} | {'是' if result.get('valid') else '否'} | "
                f"{float(result.get('rmse', 0.0)):.4f} | {float(result.get('r_squared', 0.0)):.4f} | "
                f"{float(result.get('confidence', 0.0)):.2f} |"
            )
        lines.append("")
        lines.append("结论：优先采用综合拟合质量最高的算法；若后续验证发现残差存在系统性偏差，再回退到更简单模型。")
        return "\n".join(lines)
