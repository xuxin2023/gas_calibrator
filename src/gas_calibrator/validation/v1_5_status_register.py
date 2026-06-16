"""V1.5 gas-analyzer status-register interpretation.

This module is offline/pure logic. It does not open COM ports or control any
hardware. The bit meanings come from ``D:\手册\气体分析仪指令.docx``:
status register range 0000-FFFF, bit 0 is normal when set to 1, and bits
1-13 are normal when set to 0.
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from typing import Any, Mapping


STATUS_REGISTER_MANUAL_SOURCE = "气体分析仪指令.docx table: 状态寄存器"


@dataclass(frozen=True)
class StatusBitDefinition:
    bit: int
    name_cn: str
    normal_value: int
    abnormal_value: int
    severity: str
    physical_meaning_cn: str

    def is_abnormal(self, value: int) -> bool:
        return int(value) == int(self.abnormal_value)

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


STATUS_BIT_DEFINITIONS: tuple[StatusBitDefinition, ...] = (
    StatusBitDefinition(
        0,
        "系统运行",
        1,
        0,
        "critical",
        "系统运行位未置位时，分析仪没有证明处于正常运行状态，当前帧不能作为正式校准证据。",
    ),
    StatusBitDefinition(
        1,
        "数据异常",
        0,
        1,
        "critical",
        "数据异常表示帧内容或内部计算链路存在异常，应拒绝该帧。",
    ),
    StatusBitDefinition(
        2,
        "电机转速异常",
        0,
        1,
        "critical",
        "电机转速异常会影响光路调制/同步条件，可能导致 ratio 或信号不可信。",
    ),
    StatusBitDefinition(
        3,
        "温度异常",
        0,
        1,
        "critical",
        "温度异常表示温度补偿输入不可信，不能把异常吸收到 CO2/H2O 系数里。",
    ),
    StatusBitDefinition(
        4,
        "光功率偏高",
        0,
        1,
        "warning",
        "光功率偏高提示光源/参考通道工作点异常，需结合 ref_signal 和信号强度判断。",
    ),
    StatusBitDefinition(
        5,
        "光功率偏低",
        0,
        1,
        "warning",
        "光功率偏低提示光源、光路污染或参考信号偏低风险。",
    ),
    StatusBitDefinition(
        6,
        "光电流异常",
        0,
        1,
        "critical",
        "光电流异常会破坏信号测量基础，应拒绝正式校准帧。",
    ),
    StatusBitDefinition(
        7,
        "脉冲不同步",
        0,
        1,
        "critical",
        "脉冲不同步会破坏采样同步关系，应拒绝正式校准帧。",
    ),
    StatusBitDefinition(
        8,
        "CO2信号超标",
        0,
        1,
        "critical",
        "CO2 信号超标表示光学 CO2 通道可能饱和或越界，不能只看浓度是否稳定。",
    ),
    StatusBitDefinition(
        9,
        "H2O信号超标",
        0,
        1,
        "critical",
        "H2O 信号超标表示水汽光学通道可能饱和或越界，不能进入正式水路拟合。",
    ),
    StatusBitDefinition(
        10,
        "CO2变化量超标",
        0,
        1,
        "warning",
        "CO2 变化量超标通常说明样气或信号仍在变化，应延长吹扫或降级该窗口。",
    ),
    StatusBitDefinition(
        11,
        "H2O变化量超标",
        0,
        1,
        "warning",
        "H2O 变化量超标通常说明露点/水汽未稳定，应延长水路稳定等待。",
    ),
    StatusBitDefinition(
        12,
        "CO2信号偏低",
        0,
        1,
        "warning",
        "CO2 信号偏低提示光学信号余量不足或光路问题，需要结合 ref_signal 排查。",
    ),
    StatusBitDefinition(
        13,
        "H2O信号偏低",
        0,
        1,
        "warning",
        "H2O 信号偏低提示水汽信号余量不足或光路问题，需要结合 H2O ratio 排查。",
    ),
)

_BIT_BY_INDEX = {item.bit: item for item in STATUS_BIT_DEFINITIONS}
_HEX_RE = re.compile(r"^[0-9A-Fa-f]{1,4}$")


def _as_register_value(raw_status: Any) -> tuple[int | None, str]:
    text = str(raw_status or "").strip()
    if not text:
        return None, ""
    if text.upper().startswith("0X"):
        try:
            return int(text, 16), text.upper()
        except ValueError:
            return None, text
    if _HEX_RE.fullmatch(text):
        return int(text, 16), text.upper().zfill(4)
    return None, text


def classify_status_register(
    raw_status: Any,
    *,
    bad_status_tokens: tuple[str, ...] | list[str] | None = None,
) -> dict[str, Any]:
    """Classify a raw analyzer status register value.

    Returns a JSON-serializable dict with QC status, reason, active bit
    explanations, and a Chinese summary that can be written into sample rows or
    reports.
    """

    text = str(raw_status or "").strip()
    if not text:
        return {
            "raw_status": "",
            "normalized_status": "",
            "numeric_value": None,
            "qc_status": "missing",
            "qc_reason": "status_empty",
            "summary_cn": "状态寄存器缺失",
            "active_bits": [],
            "unknown_bits": [],
            "manual_source": STATUS_REGISTER_MANUAL_SOURCE,
        }

    upper = text.upper()
    if upper in {"OK", "NORMAL", "PASS"}:
        return {
            "raw_status": text,
            "normalized_status": upper,
            "numeric_value": None,
            "qc_status": "pass",
            "qc_reason": "ok",
            "summary_cn": "状态寄存器正常",
            "active_bits": [],
            "unknown_bits": [],
            "manual_source": STATUS_REGISTER_MANUAL_SOURCE,
        }

    bad_tokens = tuple(bad_status_tokens or ("FAIL", "INVALID", "NO_RESPONSE", "NO_ACK", "ERROR"))
    if any(token.upper() in upper for token in bad_tokens):
        return {
            "raw_status": text,
            "normalized_status": upper,
            "numeric_value": None,
            "qc_status": "fail",
            "qc_reason": f"bad_status({upper})",
            "summary_cn": f"状态寄存器文本异常：{text}",
            "active_bits": [
                {
                    "bit": None,
                    "name_cn": "文本状态异常",
                    "observed_value": text,
                    "severity": "critical",
                    "physical_meaning_cn": "固件直接返回异常文本，不能作为正式校准帧。",
                }
            ],
            "unknown_bits": [],
            "manual_source": STATUS_REGISTER_MANUAL_SOURCE,
        }

    value, normalized = _as_register_value(text)
    if value is None:
        return {
            "raw_status": text,
            "normalized_status": normalized,
            "numeric_value": None,
            "qc_status": "fail",
            "qc_reason": f"unknown_status({text})",
            "summary_cn": f"状态寄存器格式无法解释：{text}",
            "active_bits": [
                {
                    "bit": None,
                    "name_cn": "未知状态格式",
                    "observed_value": text,
                    "severity": "critical",
                    "physical_meaning_cn": "无法按手册 0000-FFFF 状态寄存器解释，不能证明帧状态正常。",
                }
            ],
            "unknown_bits": [],
            "manual_source": STATUS_REGISTER_MANUAL_SOURCE,
        }

    active_bits: list[dict[str, Any]] = []
    for bit, definition in _BIT_BY_INDEX.items():
        bit_value = 1 if (value & (1 << bit)) else 0
        if definition.is_abnormal(bit_value):
            payload = definition.to_json()
            payload["observed_value"] = bit_value
            active_bits.append(payload)

    unknown_bits = [bit for bit in range(14, 16) if value & (1 << bit)]
    for bit in unknown_bits:
        active_bits.append(
            {
                "bit": bit,
                "name_cn": f"未定义状态位{bit}",
                "normal_value": 0,
                "abnormal_value": 1,
                "observed_value": 1,
                "severity": "warning",
                "physical_meaning_cn": "手册未定义该状态位，需作为诊断线索保留。",
            }
        )

    if active_bits:
        critical = any(str(item.get("severity")) == "critical" for item in active_bits)
        names = ",".join(str(item.get("name_cn")) for item in active_bits)
        return {
            "raw_status": text,
            "normalized_status": normalized,
            "numeric_value": value,
            "qc_status": "fail" if critical else "warning",
            "qc_reason": f"active_status_bits({names})",
            "summary_cn": f"状态寄存器异常：{names}",
            "active_bits": active_bits,
            "unknown_bits": unknown_bits,
            "manual_source": STATUS_REGISTER_MANUAL_SOURCE,
        }

    return {
        "raw_status": text,
        "normalized_status": normalized,
        "numeric_value": value,
        "qc_status": "pass",
        "qc_reason": "ok",
        "summary_cn": "状态寄存器正常",
        "active_bits": [],
        "unknown_bits": [],
        "manual_source": STATUS_REGISTER_MANUAL_SOURCE,
    }


def status_register_active_bits_json(review: Mapping[str, Any]) -> str:
    return json.dumps(review.get("active_bits") or [], ensure_ascii=False, separators=(",", ":"))
