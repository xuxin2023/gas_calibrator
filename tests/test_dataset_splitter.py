import pandas as pd

from gas_calibrator.coefficients.dataset_splitter import split_dataset


def test_split_dataset_reports_expected_sizes() -> None:
    dataframe = pd.DataFrame({"value": list(range(20))})

    train_df, val_df, test_df = split_dataset(dataframe, train_ratio=0.7, val_ratio=0.15, random_seed=11)

    assert len(train_df) == 14
    assert len(val_df) == 3
    assert len(test_df) == 3


def test_split_dataset_respects_min_train_size() -> None:
    dataframe = pd.DataFrame({"value": list(range(12))})

    train_df, val_df, test_df = split_dataset(
        dataframe,
        train_ratio=0.7,
        val_ratio=0.15,
        random_seed=3,
        min_train_size=9,
    )

    assert len(train_df) == 9
    assert len(val_df) == 1
    assert len(test_df) == 2
