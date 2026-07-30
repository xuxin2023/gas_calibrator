from __future__ import annotations

import json
from collections import Counter
from typing import Any, Iterable

from gas_calibrator.validation.simulation.config import AIConfig
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


class AnomalyAdvisor:
    """LLM-based anomaly diagnosis helper with deterministic fallback."""

    def __init__(self, llm_client: LLMClient | None = None, config: AIConfig | None = None):
        self.config = config or AIConfig()
        self.llm = llm_client or MockLLMClient(LLMConfig(provider="mock", model="mock"))

    def diagnose(
        self,
        anomaly_type: str,
        phase: str,
        device: str,
        error_message: str,
        log_excerpt: str = "",
    ) -> str:
        fallback = self._build_single_fallback(anomaly_type, phase, device, error_message)
        prompt = json.dumps(
            {
                "task": "Diagnose one calibration anomaly in concise Chinese.",
                "anomaly_type": anomaly_type,
                "phase": phase,
                "device": device,
                "error_message": error_message,
                "log_excerpt": log_excerpt[:500],
            },
            ensure_ascii=False,
            indent=2,
        )
        return complete_with_fallback(self.llm, prompt, fallback, temperature=0.2, max_tokens=600)

    def diagnose_run(
        self,
        *,
        failed_points: list[dict[str, Any]],
        device_events: Iterable[dict[str, Any]] | None = None,
        alarms: Iterable[dict[str, Any]] | None = None,
    ) -> str:
        event_counts = Counter(str(item.get("device") or item.get("device_name") or "unknown") for item in (device_events or []))
        alarm_categories = Counter(str(item.get("category") or item.get("severity") or "general") for item in (alarms or []))
        compact_failed = [
            {
                "point_index": item.get("point_index"),
                "route": item.get("route"),
                "temperature_c": item.get("temperature_c"),
                "reason": item.get("reason"),
                "ai_explanation": item.get("ai_explanation"),
            }
            for item in failed_points[:12]
        ]
        fallback = self._build_run_fallback(compact_failed, event_counts, alarm_categories)
        prompt = json.dumps(
            {
                "task": "Diagnose recurring calibration anomalies and provide actionable suggestions in Chinese.",
                "failed_points": compact_failed,
                "device_event_counts": dict(event_counts),
                "alarm_categories": dict(alarm_categories),
                "requirements": [
                    "Summarize the dominant failure pattern",
                    "Infer the most likely root causes",
                    "Give concrete next steps",
                    "Use only the aggregated statistics provided",
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
        return complete_with_fallback(self.llm, prompt, fallback, temperature=0.2, max_tokens=900)

    @staticmethod
    def _build_single_fallback(anomaly_type: str, phase: str, device: str, error_message: str) -> str:
        return (
            "诊断结论：\n"
            f"- 异常类型：{anomaly_type}\n"
            f"- 发生阶段：{phase}\n"
            f"- 涉及设备：{device or '未指明'}\n"
            f"- 现象：{error_message}\n\n"
            "建议操作：\n"
            "1. 先复核对应设备状态和通讯链路。\n"
            "2. 检查该阶段的超时参数和稳定判定条件。\n"
            "3. 如异常重复出现，建议保留日志并进行单设备复测。"
        )

    def _build_run_fallback(
        self,
        failed_points: list[dict[str, Any]],
        event_counts: Counter[str],
        alarm_categories: Counter[str],
    ) -> str:
        if not failed_points:
            return "诊断结论：本次运行未检测到明显异常模式。\n\n建议操作：继续保持当前流程，并对拟合结果做常规复核。"

        route_counts = Counter(str(item.get("route") or "unknown").lower() for item in failed_points)
        reason_counts = Counter(str(item.get("reason") or "unknown") for item in failed_points)
        dominant_route, dominant_route_count = route_counts.most_common(1)[0]
        dominant_reason, dominant_reason_count = reason_counts.most_common(1)[0]
        combined_text = " ".join(
            [
                dominant_route,
                dominant_reason,
                " ".join(reason_counts.keys()).lower(),
                " ".join(alarm_categories.keys()).lower(),
                " ".join(event_counts.keys()).lower(),
            ]
        )

        suggestions: list[str] = []
        if "h2o" in combined_text or "humidity" in combined_text or "dew" in combined_text:
            suggestions.extend(
                [
                    "检查湿度发生器温度设定、供气流量和露点仪读数。",
                    "适当增加湿度稳定等待时间，尤其是低温工况。",
                ]
            )
        if "pressure" in combined_text or "leak" in combined_text:
            suggestions.extend(
                [
                    "检查压力控制器、压力表和管路密封性。",
                    "确认切压后预留的稳定缓冲时间足够。",
                ]
            )
        if "communication" in combined_text or "frame" in combined_text or "analyzer" in combined_text:
            suggestions.extend(
                [
                    "检查分析仪通讯参数、缓存读取和串口稳定性。",
                    "必要时降低采样频率或增加读取超时。",
                ]
            )
        if "outlier" in combined_text or "span" in combined_text or "stability" in combined_text:
            suggestions.extend(
                [
                    "检查气路切换后是否已充分稳定，再开始采样。",
                    "复核稳态判定窗口和异常点剔除阈值是否过严。",
                ]
            )
        if not suggestions:
            suggestions.append("优先复核失败点位对应设备状态、稳态等待时间和采样完整性。")

        lines = [
            "诊断结论：",
            f"- 共发现 {len(failed_points)} 个失败点位。",
            f"- 主要集中在 {dominant_route.upper()} 路由，共 {dominant_route_count} 个。",
            f"- 最常见失败模式是 {dominant_reason}，共出现 {dominant_reason_count} 次。",
        ]
        if alarm_categories:
            category, count = alarm_categories.most_common(1)[0]
            lines.append(f"- 告警主要集中在 {category} 类别，共 {count} 条。")
        if event_counts:
            device, count = event_counts.most_common(1)[0]
            lines.append(f"- 设备事件最频繁的是 {device}，共记录 {count} 条。")
        lines.append("")
        lines.append("建议操作：")
        for index, item in enumerate(suggestions[:5], start=1):
            lines.append(f"{index}. {item}")
        return "\n".join(lines)

__all__ = ["AlgorithmAdvisor", "AnomalyAdvisor"]
