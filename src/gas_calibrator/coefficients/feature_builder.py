"""校准拟合特征构造模块。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


DEFAULT_MODEL_FEATURES = ["intercept", "R", "R2", "R3", "T", "T2", "RT", "P", "RTP"]
HUMIDITY_MODEL_FEATURES = {"H", "H2", "RH"}


@dataclass
class FeatureBuildResult:
    """特征构造结果。"""

    feature_matrix: np.ndarray
    target_vector: np.ndarray
    feature_names: List[str]
    feature_terms: Dict[str, str]
    feature_tokens: List[str]
    working_frame: pd.DataFrame


def default_model_features(ratio_degree: int = 3, add_intercept: bool = True) -> List[str]:
    """根据历史默认规则生成模型特征配置。"""
    features: List[str] = []
    if add_intercept:
        features.append("intercept")
    for power in range(1, ratio_degree + 1):
        features.append("R" if power == 1 else f"R{power}")
    features.extend(["T", "T2", "RT", "P", "RTP"])
    return features


def _feature_label(token: str) -> str:
    mapping = {
        "intercept": "1",
        "R": "R",
        "T": "T_k",
        "T2": "T_k^2",
        "RT": "R*T_k",
        "P": "P",
        "RTP": "R*T_k*P",
        "H": "H2O",
        "H2": "H2O^2",
        "RH": "R*H2O",
    }
    if token in mapping:
        return mapping[token]
    if token.startswith("R") and token[1:].isdigit():
        power = int(token[1:])
        return "R" if power == 1 else f"R^{power}"
    raise ValueError(f"Unsupported model feature: {token}")


def build_feature_terms(
    ratio_degree: int = 3,
    add_intercept: bool = True,
    *,
    model_features: Optional[Sequence[str]] = None,
) -> List[str]:
    """构造固定顺序的特征项定义。"""
    feature_tokens = list(model_features or default_model_features(ratio_degree, add_intercept))
    return [_feature_label(token) for token in feature_tokens]


def build_feature_names(terms: Sequence[str]) -> List[str]:
    """按固定顺序生成系数名 a0 ~ aN。"""
    return [f"a{index}" for index, _term in enumerate(terms)]


def _resolve_feature_columns(
    feature_tokens: Sequence[str],
    ratio_values: np.ndarray,
    temperature_k: np.ndarray,
    pressure_values: np.ndarray,
    humidity_values: Optional[np.ndarray] = None,
) -> List[np.ndarray]:
    """根据模型特征配置生成各列。"""
    columns: List[np.ndarray] = []
    for token in feature_tokens:
        if token == "intercept":
            columns.append(np.ones_like(ratio_values, dtype=float))
            continue
        if token == "R":
            columns.append(ratio_values)
            continue
        if token.startswith("R") and token[1:].isdigit():
            columns.append(ratio_values ** int(token[1:]))
            continue
        if token == "T":
            columns.append(temperature_k)
            continue
        if token == "T2":
            columns.append(temperature_k**2)
            continue
        if token == "RT":
            columns.append(ratio_values * temperature_k)
            continue
        if token == "P":
            columns.append(pressure_values)
            continue
        if token == "RTP":
            columns.append(ratio_values * temperature_k * pressure_values)
            continue
        if token == "H":
            if humidity_values is None:
                raise ValueError("model_features requires humidity column for token H")
            columns.append(humidity_values)
            continue
        if token == "H2":
            if humidity_values is None:
                raise ValueError("model_features requires humidity column for token H2")
            columns.append(humidity_values**2)
            continue
        if token == "RH":
            if humidity_values is None:
                raise ValueError("model_features requires humidity column for token RH")
            columns.append(ratio_values * humidity_values)
            continue
        raise ValueError(f"Unsupported model feature: {token}")
    return columns


def build_feature_matrix(
    dataframe: pd.DataFrame,
    *,
    ratio_column: str,
    temperature_column: str,
    pressure_column: str,
    humidity_column: Optional[str] = None,
    ratio_degree: int = 3,
    temperature_offset_c: float = 273.15,
    add_intercept: bool = True,
    model_features: Optional[Sequence[str]] = None,
) -> Tuple[np.ndarray, List[str]]:
    """根据模型特征配置构造 X 矩阵，并返回特征项名称。"""
    feature_tokens = list(model_features or default_model_features(ratio_degree, add_intercept))
    if len(feature_tokens) != len(set(feature_tokens)):
        raise ValueError("model_features contains duplicate feature tokens")

    working = dataframe.copy()
    working["T_k"] = working[temperature_column].astype(float) + float(temperature_offset_c)
    ratio_values = working[ratio_column].astype(float).to_numpy()
    temperature_k = working["T_k"].astype(float).to_numpy()
    pressure_values = working[pressure_column].astype(float).to_numpy()
    humidity_values = None
    if any(token in HUMIDITY_MODEL_FEATURES for token in feature_tokens):
        if not humidity_column:
            raise ValueError("model_features requires humidity_column when using H/H2/RH")
        humidity_values = working[humidity_column].astype(float).to_numpy()
    columns = _resolve_feature_columns(feature_tokens, ratio_values, temperature_k, pressure_values, humidity_values)
    x_matrix = np.column_stack(columns).astype(float, copy=False)
    return x_matrix, build_feature_terms(model_features=feature_tokens)


def build_feature_dataset(
    dataframe: pd.DataFrame,
    *,
    target_column: str,
    ratio_column: str,
    temperature_column: str,
    pressure_column: str,
    humidity_column: Optional[str] = None,
    ratio_degree: int = 3,
    temperature_offset_c: float = 273.15,
    add_intercept: bool = True,
    model_features: Optional[Sequence[str]] = None,
) -> FeatureBuildResult:
    """从清洗后的 DataFrame 中统一构造 X、Y 和特征定义。"""
    working = dataframe.copy()
    feature_tokens = list(model_features or default_model_features(ratio_degree, add_intercept))
    x_matrix, terms = build_feature_matrix(
        working,
        ratio_column=ratio_column,
        temperature_column=temperature_column,
        pressure_column=pressure_column,
        humidity_column=humidity_column,
        ratio_degree=ratio_degree,
        temperature_offset_c=temperature_offset_c,
        add_intercept=add_intercept,
        model_features=feature_tokens,
    )
    feature_names = build_feature_names(terms)
    working["T_k"] = working[temperature_column].astype(float) + float(temperature_offset_c)
    y_vector = working[target_column].astype(float).to_numpy()
    return FeatureBuildResult(
        feature_matrix=x_matrix,
        target_vector=y_vector,
        feature_names=feature_names,
        feature_terms={name: term for name, term in zip(feature_names, terms)},
        feature_tokens=feature_tokens,
        working_frame=working,
    )
