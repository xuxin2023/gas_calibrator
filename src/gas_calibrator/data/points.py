"""
校准点解析模块。

职责：
1. 从 Excel 点表读取原始配置；
2. 解析温度、CO2、H2O、压力等字段；
3. 提供点表校验与重排工具。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pandas as pd


@dataclass
class CalibrationPoint:
    """单个校准点的数据结构。"""

    index: int
    temp_chamber_c: float
    co2_ppm: Optional[float]
    hgen_temp_c: Optional[float]
    hgen_rh_pct: Optional[float]
    target_pressure_hpa: Optional[float]
    dewpoint_c: Optional[float]
    h2o_mmol: Optional[float]
    raw_h2o: Optional[str]
    co2_group: Optional[str] = None

    @property
    def is_h2o_point(self) -> bool:
        """判断该点是否为水汽点（由湿度发生器温度+湿度共同定义）。"""
        return self.hgen_temp_c is not None and self.hgen_rh_pct is not None


_TEMP_RE = re.compile(r"([+-]?\d+(?:\.\d+)?)")


def _is_missing(val) -> bool:
    if val is None:
        return True
    try:
        if pd.isna(val):
            return True
    except Exception:
        pass
    if isinstance(val, str) and not val.strip():
        return True
    return False


def _parse_temp_cell(val) -> Optional[float]:
    """
    解析温度单元格。

    支持：
    - 数值类型（int/float）；
    - 字符串内提取第一个浮点数（如 `20℃`）。
    """
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        m = _TEMP_RE.search(val)
        if m:
            return float(m.group(1))
    return None


def _is_h2o_off_marker(val) -> bool:
    """Detect explicit non-water markers in H2O cell, e.g. '——'."""
    if not isinstance(val, str):
        return False
    text = val.strip().upper()
    if not text:
        return False
    compact = text.replace(" ", "")
    return compact in {
        "-",
        "--",
        "---",
        "—",
        "——",
        "———",
        "N/A",
        "NA",
        "NONE",
        "无",
    }


def load_points_from_excel(
    path: str | Path,
    missing_pressure_policy: str = "require",
    carry_forward_h2o: bool = False,
) -> List[CalibrationPoint]:
    """
    从 Excel 读取并解析校准点列表。

    `missing_pressure_policy`：
    - `require`：压力缺失留空，交由后续校验报错；
    - `carry_forward`：压力缺失时沿用上一条有效压力。
    """
    df = pd.read_excel(path, header=None)
    rows: List[CalibrationPoint] = []

    # 解析 H2O 列中“湿度发生器温度/湿度、露点、mmol/mol”文本。
    hum_re_temp = re.compile(r"([+-]?\d+(?:\.\d+)?)\s*\u2103\uff08\u6e7f\u5ea6\u53d1\u751f\u5668\uff09")
    hum_re_rh = re.compile(r"([+-]?\d+(?:\.\d+)?)\s*%\uff08\u6e7f\u5ea6\u53d1\u751f\u5668\uff09")
    dp_re = re.compile(r"([+-]?\d+(?:\.\d+)?)\s*\u2103\uff08\u9732\u70b9\u6e29\u5ea6\uff09")
    mmol_re = re.compile(r"([+-]?\d+(?:\.\d+)?)\s*mmol/mol")

    # 采用“向下继承”策略处理合并单元格：温度和 CO2 可能只在首行出现。
    current_temp = None
    current_co2 = None
    current_hgen_temp: Optional[float] = None
    current_hgen_rh: Optional[float] = None
    current_dewpoint: Optional[float] = None
    current_mmol: Optional[float] = None
    current_raw_h2o: Optional[str] = None
    last_pressure = None
    h2o_context_active = False

    # 默认跳过前两行（历史表格结构中通常为标题/表头区）。
    for i in range(2, len(df)):
        temp = df.iloc[i, 0]
        co2 = df.iloc[i, 1]
        h2o = df.iloc[i, 2]
        pres = df.iloc[i, 3]

        # 1) 温度列：若当前单元格为空则沿用上一条温度。
        prev_temp = current_temp
        if isinstance(temp, str) and temp.strip():
            current_temp = _parse_temp_cell(temp)
        elif isinstance(temp, (int, float)) and not _is_missing(temp):
            current_temp = float(temp)
        if current_temp != prev_temp:
            # Temperature group changed: clear H2O carry-forward context.
            current_hgen_temp = None
            current_hgen_rh = None
            current_dewpoint = None
            current_mmol = None
            current_raw_h2o = None
            h2o_context_active = False

        # 2) CO2 列：若当前单元格为空则沿用上一条 CO2。
        if not _is_missing(co2):
            try:
                parsed_co2 = float(co2)
                if not _is_missing(parsed_co2):
                    current_co2 = parsed_co2
            except Exception:
                pass

        # 3) 完全空行直接跳过。
        if _is_missing(co2) and _is_missing(h2o) and _is_missing(pres):
            # Empty separator rows end current H2O carry-forward block.
            h2o_context_active = False
            continue

        # 4) 解析 H2O 文本字段。
        parsed_hgen_temp = None
        parsed_hgen_rh = None
        parsed_dewpoint = None
        parsed_mmol = None
        parsed_raw_h2o = h2o if isinstance(h2o, str) and h2o.strip() else None
        co2_group = None
        if isinstance(h2o, str):
            m = hum_re_temp.search(h2o)
            if m:
                parsed_hgen_temp = float(m.group(1))
            m = hum_re_rh.search(h2o)
            if m:
                parsed_hgen_rh = float(m.group(1))
            m = dp_re.search(h2o)
            if m:
                parsed_dewpoint = float(m.group(1))
            m = mmol_re.search(h2o)
            if m:
                parsed_mmol = float(m.group(1))

        explicit_h2o_on = (
            parsed_hgen_temp is not None
            and parsed_hgen_rh is not None
        )
        explicit_h2o_off = _is_h2o_off_marker(h2o)
        if explicit_h2o_on:
            h2o_context_active = True
        elif explicit_h2o_off:
            h2o_context_active = False
            current_hgen_temp = None
            current_hgen_rh = None
            current_dewpoint = None
            current_mmol = None
            current_raw_h2o = None

        if carry_forward_h2o:
            if parsed_hgen_temp is not None:
                current_hgen_temp = parsed_hgen_temp
            if parsed_hgen_rh is not None:
                current_hgen_rh = parsed_hgen_rh
            if parsed_dewpoint is not None:
                current_dewpoint = parsed_dewpoint
            if parsed_mmol is not None:
                current_mmol = parsed_mmol
            if parsed_raw_h2o is not None:
                current_raw_h2o = parsed_raw_h2o

            if h2o_context_active:
                hgen_temp = current_hgen_temp
                hgen_rh = current_hgen_rh
                dewpoint = current_dewpoint
                mmol = current_mmol
                raw_h2o = current_raw_h2o
            else:
                hgen_temp = parsed_hgen_temp
                hgen_rh = parsed_hgen_rh
                dewpoint = parsed_dewpoint
                mmol = parsed_mmol
                raw_h2o = parsed_raw_h2o
        else:
            hgen_temp = parsed_hgen_temp
            hgen_rh = parsed_hgen_rh
            dewpoint = parsed_dewpoint
            mmol = parsed_mmol
            raw_h2o = parsed_raw_h2o

        # 6) 可选 CO2 组别列（第 5 列）：用于区分两组 CO2 管路（如 A/B 或 1/2）。
        if df.shape[1] > 4:
            grp = df.iloc[i, 4]
            if not _is_missing(grp):
                text = str(grp).strip()
                if text:
                    co2_group = text

        # 5) 解析压力列并按策略处理缺失。
        target_pressure = None
        if not _is_missing(pres):
            try:
                parsed_pressure = float(pres)
                target_pressure = None if _is_missing(parsed_pressure) else parsed_pressure
            except Exception:
                target_pressure = None
        elif missing_pressure_policy == "carry_forward":
            target_pressure = last_pressure

        # 无有效温度的行不构造点。
        if current_temp is None:
            continue

        # Bench rule: sub-zero chamber points are gas-path only.
        if current_temp < 0:
            hgen_temp = None
            hgen_rh = None
            dewpoint = None
            mmol = None
            raw_h2o = None

        row_co2 = current_co2
        row_co2_group = co2_group
        if hgen_temp is not None and hgen_rh is not None:
            row_co2 = None
            row_co2_group = None

        rows.append(
            CalibrationPoint(
                index=i + 1,  # Excel 行号（1-based）
                temp_chamber_c=current_temp,
                co2_ppm=row_co2,
                hgen_temp_c=hgen_temp,
                hgen_rh_pct=hgen_rh,
                target_pressure_hpa=target_pressure,
                dewpoint_c=dewpoint,
                h2o_mmol=mmol,
                raw_h2o=raw_h2o,
                co2_group=row_co2_group,
            )
        )
        if target_pressure is not None:
            last_pressure = target_pressure

    return rows


def validate_points(points: List[CalibrationPoint], missing_pressure_policy: str) -> List[str]:
    """
    校验点表并返回问题列表。

    当前规则：
    - 当策略为 `require` 时，目标压力不能为空。
    """
    issues = []
    for p in points:
        if p.target_pressure_hpa is None and missing_pressure_policy == "require":
            issues.append(f"Row {p.index}: missing target pressure")
    return issues


def reorder_points(
    points: List[CalibrationPoint],
    water_first_temp_gte: Optional[float],
    *,
    descending_temperatures: bool = True,
) -> List[CalibrationPoint]:
    """
    按温度分组重排点序。

    规则：
    - 温度 < 阈值：保持原顺序；
    - 温度 >= 阈值：同温度组内优先执行 H2O 点，再执行 CO2 点。
    """
    if water_first_temp_gte is None:
        return points

    ordered = []
    buckets = {}

    # 保持温度组首次出现的顺序，避免打乱大流程节奏。
    for p in points:
        t = p.temp_chamber_c
        if t not in buckets:
            buckets[t] = []
        buckets[t].append(p)

    temps_in_order = sorted(buckets.keys(), reverse=bool(descending_temperatures))

    for t in temps_in_order:
        group = buckets[t]
        if t >= water_first_temp_gte:
            h2o = [p for p in group if p.is_h2o_point]
            co2 = [p for p in group if not p.is_h2o_point]
            ordered.extend(h2o + co2)
        else:
            ordered.extend(group)
    return ordered
