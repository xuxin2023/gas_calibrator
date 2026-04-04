from __future__ import annotations

import json
from collections import Counter
from typing import Any, Iterable

from ...config import AIConfig
from ..llm_client import LLMClient, LLMConfig, MockLLMClient, complete_with_fallback


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
