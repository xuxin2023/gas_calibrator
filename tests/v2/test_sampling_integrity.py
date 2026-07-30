from __future__ import annotations

import ast
from pathlib import Path

import pytest

from gas_calibrator.validation.simulation.sampling_contracts import (
    summarize_analyzer_integrity,
)
from gas_calibrator.v2.core.services.sampling_service import (
    SamplingService,
)


def test_sampling_integrity_has_shared_owner_and_no_v2_import() -> None:
    assert summarize_analyzer_integrity.__module__ == (
        "gas_calibrator.validation.simulation.sampling_contracts"
    )

    path = (
        Path(__file__).resolve().parents[2]
        / "src/gas_calibrator/validation/simulation/sampling_contracts.py"
    )
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imported_modules = {
        str(node.module or "")
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    assert all(
        not module.startswith("gas_calibrator.v2")
        for module in imported_modules
    )


@pytest.mark.parametrize(
    ("rows", "labels", "status", "coverage", "missing", "unusable"),
    [
        ([], [], "无分析仪", "0/0", "", ""),
        ([{}], ["ga01", "ga02"], "无帧", "0/2", "GA01,GA02", ""),
        (
            [
                {
                    "ga01_frame_has_data": True,
                    "ga01_frame_usable": False,
                }
            ],
            ["ga01"],
            "仅异常帧",
            "0/1",
            "",
            "GA01",
        ),
        (
            [
                {
                    "ga01_frame_has_data": True,
                    "ga01_frame_usable": True,
                }
            ],
            ["ga01", "ga02"],
            "部分缺失",
            "1/2",
            "GA02",
            "",
        ),
        (
            [
                {
                    "ga01_frame_has_data": True,
                    "ga01_frame_usable": True,
                    "ga02_frame_has_data": True,
                    "ga02_frame_usable": False,
                }
            ],
            ["ga01", "ga02"],
            "含异常帧",
            "1/2",
            "",
            "GA02",
        ),
        (
            [
                {
                    "ga01_frame_has_data": True,
                    "ga01_frame_usable": True,
                    "ga02_frame_has_data": True,
                    "ga02_frame_usable": False,
                }
            ],
            ["ga01", "ga02", "ga03"],
            "部分缺失且含异常帧",
            "1/3",
            "GA03",
            "GA02",
        ),
        (
            [
                {
                    "ga01_frame_has_data": True,
                    "ga01_frame_usable": True,
                    "ga02_frame_has_data": True,
                    "ga02_frame_usable": True,
                }
            ],
            ["ga01", "ga02"],
            "完整",
            "2/2",
            "",
            "",
        ),
    ],
)
def test_sampling_integrity_preserves_all_status_branches(
    rows: list[dict[str, object]],
    labels: list[str],
    status: str,
    coverage: str,
    missing: str,
    unusable: str,
) -> None:
    summary = summarize_analyzer_integrity(
        rows,
        analyzer_labels=labels,
    )

    assert summary["analyzer_integrity"] == status
    assert summary["analyzer_coverage_text"] == coverage
    assert summary["analyzer_missing_labels"] == missing
    assert summary["analyzer_unusable_labels"] == unusable
    assert summary["analyzer_expected_count"] == len(labels)


def test_sampling_integrity_preserves_label_normalization_and_single_owner() -> None:
    rows = [
        {
            "ga_01_frame_has_data": True,
            "ga_01_frame_usable": True,
        }
    ]
    labels = ["Ga 01"]
    expected = summarize_analyzer_integrity(
        rows,
        analyzer_labels=labels,
    )

    assert expected == {
        "analyzer_expected_count": 1,
        "analyzer_with_frame_count": 1,
        "analyzer_usable_count": 1,
        "analyzer_coverage_text": "1/1",
        "analyzer_integrity": "完整",
        "analyzer_missing_labels": "",
        "analyzer_unusable_labels": "",
    }
    assert "summarize_analyzer_integrity" not in SamplingService.__dict__
