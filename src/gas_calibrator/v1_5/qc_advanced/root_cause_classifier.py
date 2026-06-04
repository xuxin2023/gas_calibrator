"""Root-cause classification for V1.5 advanced QC outputs."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping


EXPLANATIONS = {
    "real_moisture_release": "露点/水汽摩尔分数持续上升，疑似气路反湿或样气真实变湿。",
    "wet_dilution_or_contamination_suspect": "H2O dry ppmv 上升且 CO2 下降，疑似湿气稀释或污染。",
    "pressure_effect_possible": "原始露点上升但 H2O dry ppmv 稳定，可能是压力表观效应。",
    "co2_pressure_or_temperature_compensation_suspect": "CO2 ratio 稳定但 CO2 输出漂移，疑似压力/温度补偿链路问题。",
    "h2o_pressure_or_temperature_compensation_suspect": "H2O ratio 稳定但 H2O 输出漂移，疑似压力/温度补偿链路问题。",
    "optical_reference_signal_drift": "ref_signal 漂移，提示光学参考信号不稳定。",
    "analyzer_pressure_mean_bias_exceeds_limit": "分析仪内部压力 P 与 COM22 偏差超限。",
    "analyzer_pressure_trend_drift": "分析仪压力通道偏差存在趋势漂移。",
    "standard_gas_expired": "标准气证书过期，溯源不合格。",
    "reference_certificate_expired": "参考设备证书过期，溯源不合格。",
}


def classify_root_cause(
    *,
    humidity: Mapping[str, Any] | None = None,
    factory_signal: Mapping[str, Any] | None = None,
    pressure_trend: Mapping[str, Any] | None = None,
    traceability: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    codes: List[str] = []
    humidity = humidity or {}
    factory_signal = factory_signal or {}
    pressure_trend = pressure_trend or {}
    traceability = traceability or {}

    classification = humidity.get("classification")
    if classification and classification != "humidity_stable_or_insufficient_evidence":
        codes.append(str(classification))
    codes.extend(str(item) for item in factory_signal.get("findings") or [])
    codes.extend(str(item) for item in pressure_trend.get("reasons") or [])
    codes.extend(str(item) for item in traceability.get("reasons") or [])

    deduped: List[str] = []
    for code in codes:
        if code and code not in deduped:
            deduped.append(code)
    explanations = [EXPLANATIONS.get(code, code) for code in deduped]
    severity = "pass"
    if any(code in {"standard_gas_expired", "reference_certificate_expired"} for code in deduped):
        severity = "block_formal"
    elif any(code in {"real_moisture_release", "wet_dilution_or_contamination_suspect"} for code in deduped):
        severity = "reject_point"
    elif deduped:
        severity = "review"
    return {
        "status": severity,
        "root_cause_codes": deduped,
        "explanations": explanations,
        "summary": "；".join(explanations) if explanations else "未发现高级 QC 根因异常。",
    }
