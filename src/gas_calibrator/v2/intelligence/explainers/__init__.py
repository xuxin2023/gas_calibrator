from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from gas_calibrator.validation.simulation.config import AIConfig
from ..context_builders import build_fit_context, build_qc_context, build_run_context
from ..llm_client import LLMClient, LLMConfig, MockLLMClient, complete_with_fallback


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


class QCExplainer:
    """LLM-powered QC explanation helper with deterministic fallback."""

    _RULE_MESSAGES = {
        "usable_sample_count": (
            "有效样本数不足，说明采样阶段可用于判定的数据不够。",
            "增加采样数量，检查通讯完整性，并确认稳定后再采样。",
        ),
        "sample_count": (
            "采样数量低于要求，统计结果不稳定。",
            "检查采样周期设置，必要时延长采样窗口。",
        ),
        "missing_count": (
            "采样过程中存在缺失帧或丢帧。",
            "检查串口通讯、缓存读取和设备返回节拍。",
        ),
        "communication_error": (
            "采样数据出现通讯异常，导致部分帧不可用。",
            "检查分析仪通讯链路、串口参数和供电状态。",
        ),
        "time_continuity": (
            "时间序列不连续，可能存在采样阻塞或设备响应抖动。",
            "检查采样调度、设备轮询间隔和主线程阻塞情况。",
        ),
        "outlier_ratio": (
            "异常点比例偏高，说明数据波动明显。",
            "检查气路稳定性、泄漏和混气均匀性，必要时重新采样。",
        ),
        "signal_span": (
            "信号跨度过大，稳定性不足。",
            "增加稳定等待时间，检查温压控制和分析仪响应。",
        ),
    }

    def __init__(self, llm_client: LLMClient | None = None, config: AIConfig | None = None):
        self.config = config or AIConfig()
        self.llm = llm_client or MockLLMClient(LLMConfig(provider="mock", model="mock"))
        self._cache: dict[str, str] = {}

    def explain(
        self,
        point_index: int,
        validation_result: Any,
        cleaned_data: Any = None,
        point: Any = None,
    ) -> str:
        return self.explain_failure(
            point_index,
            validation_result,
            cleaned_data=cleaned_data,
            point=point,
        )

    def explain_failure(
        self,
        point_index: int,
        validation_result: Any,
        *,
        cleaned_data: Any = None,
        point: Any = None,
    ) -> str:
        cache_key = self._cache_key(point_index, validation_result, cleaned_data, point)
        if cache_key in self._cache:
            return self._cache[cache_key]

        context = build_qc_context(point_index, validation_result, cleaned_data)
        fallback = self._build_fallback_explanation(point_index, validation_result, cleaned_data, point)
        prompt = json.dumps(
            {
                "task": "Explain the failed QC checks for one calibration point in concise Chinese.",
                "context": context.to_dict(),
                "point": self._point_payload(point),
                "failed_checks": list(getattr(validation_result, "failed_checks", []) or []),
                "requirements": [
                    "Use bullet points",
                    "State actual value and threshold when available",
                    "Explain likely cause",
                    "Provide practical next steps",
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
        text = complete_with_fallback(self.llm, prompt, fallback, temperature=0.2, max_tokens=700)
        self._cache[cache_key] = text
        return text

    def explain_batch(self, results: list[tuple[int, Any, Any]]) -> list[str]:
        return [self.explain(point_index, validation, cleaned) for point_index, validation, cleaned in results]

    def _build_fallback_explanation(
        self,
        point_index: int,
        validation_result: Any,
        cleaned_data: Any,
        point: Any,
    ) -> str:
        failed_checks = list(getattr(validation_result, "failed_checks", []) or [])
        point_text = self._point_label(point, point_index)
        if not failed_checks:
            reason = str(getattr(validation_result, "reason", "") or "qc_failed")
            return (
                f"QC 点位 {point_text} 未通过。\n"
                f"- 失败原因：{reason}\n"
                f"- 建议：检查采样完整性、稳定等待时间和异常点剔除策略。"
            )

        sections = [f"QC 点位 {point_text} 未通过："]
        for check in failed_checks:
            rule_name = str(check.get("rule_name") or "unknown")
            actual = check.get("actual")
            threshold = check.get("threshold")
            detail = str(check.get("message") or "")
            reason, suggestion = self._rule_reason_and_suggestion(rule_name, detail)
            actual_text = self._format_value(actual)
            threshold_text = self._format_value(threshold)
            sections.append(f'- QC 规则 "{rule_name}" 失败：')
            if actual is not None:
                sections.append(f"  实际值：{actual_text}")
            if threshold is not None:
                sections.append(f"  阈值：{threshold_text}")
            sections.append(f"  原因：{reason}")
            sections.append(f"  建议：{suggestion}")
        if cleaned_data is not None:
            cleaned_count = getattr(cleaned_data, "cleaned_count", None)
            removed_count = getattr(cleaned_data, "removed_count", None)
            if cleaned_count is not None or removed_count is not None:
                sections.append(
                    f"补充信息：保留样本 {int(cleaned_count or 0)} 条，剔除样本 {int(removed_count or 0)} 条。"
                )
        return "\n".join(sections)

    @classmethod
    def _rule_reason_and_suggestion(cls, rule_name: str, detail: str) -> tuple[str, str]:
        key = str(rule_name or "").strip().lower()
        if key in cls._RULE_MESSAGES:
            return cls._RULE_MESSAGES[key]
        lowered = detail.lower()
        if "humidity" in lowered or "h2o" in lowered or "dew" in lowered:
            return (
                "湿度相关数据不稳定，可能是湿度发生器或露点测量链路存在波动。",
                "检查湿度发生器、露点仪以及相关气路，并延长稳定等待时间。",
            )
        if "pressure" in lowered:
            return (
                "压力控制未稳定，导致点位数据波动。",
                "检查压力控制器响应和管路密封性，确认设定值附近的缓冲时间足够。",
            )
        return (
            "该 QC 检查未达到阈值要求，说明当前点位数据质量不足。",
            "复核设备状态、采样节拍和稳态判定条件后再重试。",
        )

    @staticmethod
    def _cache_key(point_index: int, validation_result: Any, cleaned_data: Any, point: Any) -> str:
        return json.dumps(
            {
                "point_index": point_index,
                "reason": getattr(validation_result, "reason", ""),
                "failed_checks": getattr(validation_result, "failed_checks", []),
                "cleaned_count": getattr(cleaned_data, "cleaned_count", None) if cleaned_data is not None else None,
                "removed_count": getattr(cleaned_data, "removed_count", None) if cleaned_data is not None else None,
                "point": QCExplainer._point_payload(point),
            },
            ensure_ascii=False,
            sort_keys=True,
        )

    @staticmethod
    def _point_payload(point: Any) -> dict[str, Any]:
        if point is None:
            return {}
        return {
            "index": getattr(point, "index", None),
            "route": getattr(point, "route", None),
            "temperature_c": getattr(point, "temperature_c", None),
            "co2_ppm": getattr(point, "co2_ppm", None),
            "humidity_pct": getattr(point, "humidity_pct", None),
            "pressure_hpa": getattr(point, "pressure_hpa", None),
        }

    @staticmethod
    def _point_label(point: Any, point_index: int) -> str:
        if point is None:
            return str(point_index)
        route = str(getattr(point, "route", "") or "").strip().upper()
        temperature = getattr(point, "temperature_c", None)
        if temperature is None:
            return f"{point_index} ({route})" if route else str(point_index)
        return f"{point_index} ({route} / {float(temperature):.1f}°C)" if route else f"{point_index} ({float(temperature):.1f}°C)"

    @staticmethod
    def _format_value(value: Any) -> str:
        if isinstance(value, float):
            return f"{value:.4f}"
        return str(value)


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


__all__ = ["FitExplainer", "QCExplainer", "RunExplainer"]
