"""校准拟合数据读取与列检查工具。"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

import pandas as pd


def load_excel_dataframe(
    path: str | Path,
    *,
    sheet_name: str | int = 0,
    required_columns: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """读取 Excel，并在入口处完成必需列检查。"""
    dataframe = pd.read_excel(path, sheet_name=sheet_name)
    validate_required_columns(dataframe, required_columns or ())
    return dataframe


def records_to_dataframe(rows: Iterable[dict[str, Any]]) -> pd.DataFrame:
    """将字典记录序列统一转换为 DataFrame。"""
    return pd.DataFrame(list(rows))


def validate_required_columns(dataframe: pd.DataFrame, required_columns: Sequence[str]) -> None:
    """检查 DataFrame 是否包含所有必需列。"""
    missing = [column for column in required_columns if column not in dataframe.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def resolve_column_name(
    dataframe: pd.DataFrame,
    candidates: Sequence[str],
    *,
    label: str,
) -> str:
    """从候选列名中解析当前数据可用的列名。"""
    for column in candidates:
        if column and column in dataframe.columns:
            return column
    raise ValueError(f"Missing {label} column. candidates={list(candidates)}")
