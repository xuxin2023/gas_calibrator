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


def test_split_dataset_prefers_group_aware_when_point_columns_exist() -> None:
    dataframe = pd.DataFrame(
        {
            "PointTag": ["p1", "p1", "p2", "p2", "p3", "p3"],
            "PointRow": [1, 1, 2, 2, 3, 3],
            "PointPhase": ["CO2", "CO2", "CO2", "CO2", "CO2", "CO2"],
            "Analyzer": ["GA01", "GA02", "GA01", "GA02", "GA01", "GA02"],
            "value": list(range(6)),
        }
    )

    train_df, val_df, test_df, metadata = split_dataset(
        dataframe,
        train_ratio=0.5,
        val_ratio=0.25,
        random_seed=5,
        return_metadata=True,
    )

    assert metadata["split_strategy"] == "group_aware"
    assert metadata["group_columns"] == ["PointTag", "PointRow", "PointPhase"]

    split_tags = {
        "train": set(train_df["PointTag"].tolist()),
        "validation": set(val_df["PointTag"].tolist()),
        "test": set(test_df["PointTag"].tolist()),
    }
    assert split_tags["train"].isdisjoint(split_tags["validation"])
    assert split_tags["train"].isdisjoint(split_tags["test"])
    assert split_tags["validation"].isdisjoint(split_tags["test"])
