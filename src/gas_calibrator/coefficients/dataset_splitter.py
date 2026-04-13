"""数据集拆分模块。"""

from __future__ import annotations

from typing import Any, Callable, Optional

import numpy as np
import pandas as pd


_PREFERRED_GROUP_COLUMNS: tuple[str, ...] = ("PointTag", "PointRow", "PointPhase")
_FALLBACK_GROUP_COLUMNS: tuple[str, ...] = ("Analyzer",)


def _emit_log(log_fn: Optional[Callable[[str], None]], message: str) -> None:
    if log_fn is not None:
        log_fn(message)


def _normalize_group_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, np.generic):
        return value.item()
    return value


def _detect_group_columns(df: pd.DataFrame) -> list[str]:
    primary = [
        column
        for column in _PREFERRED_GROUP_COLUMNS
        if column in df.columns and df[column].notna().any()
    ]
    if primary:
        return primary
    return [
        column
        for column in _FALLBACK_GROUP_COLUMNS
        if column in df.columns and df[column].notna().any()
    ]


def _split_group_aware(
    df: pd.DataFrame,
    *,
    train_ratio: float,
    val_ratio: float,
    random_seed: int,
    shuffle: bool,
    min_train_size: int,
    log_fn: Optional[Callable[[str], None]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    group_columns = _detect_group_columns(df)
    if not group_columns:
        raise ValueError("group-aware split requires at least one group column")

    group_buckets: dict[tuple[Any, ...], list[int]] = {}
    for position, row in enumerate(df[group_columns].itertuples(index=False, name=None)):
        key = tuple(_normalize_group_value(value) for value in row)
        group_buckets.setdefault(key, []).append(position)

    ordered_groups = list(group_buckets.keys())
    if shuffle:
        rng = np.random.default_rng(int(random_seed))
        order = rng.permutation(len(ordered_groups))
        ordered_groups = [ordered_groups[index] for index in order]

    total = len(df)
    desired_train = max(int(total * train_ratio), int(min_train_size))
    desired_train = min(desired_train, total)
    desired_val = min(int(total * val_ratio), total - desired_train)

    train_positions: list[int] = []
    val_positions: list[int] = []
    test_positions: list[int] = []

    for group_key in ordered_groups:
        group_positions = group_buckets[group_key]
        if len(train_positions) < desired_train:
            train_positions.extend(group_positions)
            continue
        if len(val_positions) < desired_val:
            val_positions.extend(group_positions)
            continue
        test_positions.extend(group_positions)

    train_df = df.iloc[train_positions].copy()
    val_df = df.iloc[val_positions].copy()
    test_df = df.iloc[test_positions].copy()

    split_strategy = "group_aware"
    strategy_label = f"{split_strategy}({'+'.join(group_columns)})"
    _emit_log(log_fn, f"优先使用分组留出拆分：{strategy_label}")

    metadata = {
        "split_strategy": split_strategy,
        "split_strategy_label": strategy_label,
        "group_columns": list(group_columns),
        "group_count": int(len(group_buckets)),
    }
    return train_df, val_df, test_df, metadata


def split_dataset(
    df: pd.DataFrame,
    train_ratio: float = 0.7,
    val_ratio: float = 0.15,
    *,
    random_seed: int = 42,
    shuffle: bool = True,
    min_train_size: int = 0,
    log_fn: Optional[Callable[[str], None]] = None,
    return_metadata: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame] | tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """按 train / validation / test 三段拆分数据集。"""
    if not 0 < train_ratio < 1:
        raise ValueError("train_ratio must be within (0, 1)")
    if not 0 <= val_ratio < 1:
        raise ValueError("val_ratio must be within [0, 1)")
    if train_ratio + val_ratio >= 1:
        raise ValueError("train_ratio + val_ratio must be < 1")

    total = len(df)
    if total == 0:
        raise ValueError("Cannot split an empty dataset")

    metadata: dict[str, Any]
    group_columns = _detect_group_columns(df)
    if group_columns:
        train_df, val_df, test_df, metadata = _split_group_aware(
            df,
            train_ratio=train_ratio,
            val_ratio=val_ratio,
            random_seed=random_seed,
            shuffle=shuffle,
            min_train_size=min_train_size,
            log_fn=log_fn,
        )
    else:
        positions = np.arange(total)
        if shuffle:
            rng = np.random.default_rng(int(random_seed))
            positions = rng.permutation(positions)

        train_count = max(int(total * train_ratio), int(min_train_size))
        train_count = min(train_count, total)

        remaining = total - train_count
        val_count = int(total * val_ratio)
        val_count = min(val_count, remaining)
        test_count = total - train_count - val_count

        train_positions = positions[:train_count]
        val_positions = positions[train_count : train_count + val_count]
        test_positions = positions[train_count + val_count : train_count + val_count + test_count]

        train_df = df.iloc[train_positions].copy()
        val_df = df.iloc[val_positions].copy()
        test_df = df.iloc[test_positions].copy()
        metadata = {
            "split_strategy": "random",
            "split_strategy_label": "random",
            "group_columns": [],
            "group_count": 0,
        }

    _emit_log(log_fn, f"数据总量：{total}")
    _emit_log(log_fn, f"训练集：{len(train_df)}")
    _emit_log(log_fn, f"验证集：{len(val_df)}")
    _emit_log(log_fn, f"测试集：{len(test_df)}")

    if return_metadata:
        return train_df, val_df, test_df, metadata
    return train_df, val_df, test_df
