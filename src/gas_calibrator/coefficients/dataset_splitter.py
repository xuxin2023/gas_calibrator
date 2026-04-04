"""数据集拆分模块。"""

from __future__ import annotations

from typing import Callable, Optional

import numpy as np
import pandas as pd


def _emit_log(log_fn: Optional[Callable[[str], None]], message: str) -> None:
    if log_fn is not None:
        log_fn(message)


def split_dataset(
    df: pd.DataFrame,
    train_ratio: float = 0.7,
    val_ratio: float = 0.15,
    *,
    random_seed: int = 42,
    shuffle: bool = True,
    min_train_size: int = 0,
    log_fn: Optional[Callable[[str], None]] = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
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

    _emit_log(log_fn, f"数据总量：{total}")
    _emit_log(log_fn, f"训练集：{len(train_df)}")
    _emit_log(log_fn, f"验证集：{len(val_df)}")
    _emit_log(log_fn, f"测试集：{len(test_df)}")

    return train_df, val_df, test_df
