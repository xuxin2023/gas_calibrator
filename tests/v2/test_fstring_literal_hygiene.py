from __future__ import annotations

import ast
from pathlib import Path

from gas_calibrator.v2.core.human_governance_artifacts import (
    build_human_governance_artifacts,
)
from gas_calibrator.v2.core.recognition_readiness_artifacts import (
    _method_confirmation_reviewer_action,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
TARGET_FILES = (
    REPO_ROOT / "src/gas_calibrator/v2/core/human_governance_artifacts.py",
    REPO_ROOT / "src/gas_calibrator/v2/core/recognition_readiness_artifacts.py",
)


def test_target_files_have_no_fstrings_without_formatted_values() -> None:
    redundant_fstrings: list[str] = []
    for path in TARGET_FILES:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        format_specs = {
            id(node.format_spec)
            for node in ast.walk(tree)
            if isinstance(node, ast.FormattedValue) and node.format_spec is not None
        }
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.JoinedStr)
                and id(node) not in format_specs
                and not any(
                isinstance(value, ast.FormattedValue) for value in node.values
                )
            ):
                redundant_fstrings.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno}")

    assert redundant_fstrings == []


def test_human_governance_placeholder_detail_text_is_stable() -> None:
    payload = build_human_governance_artifacts()

    assert (
        payload["run_metadata_profile"]["review_surface"]["detail_lines"][-1]
        == "placeholder_mode: reviewer_note_only"
    )


def test_method_confirmation_fixed_reviewer_actions_are_stable() -> None:
    expected = {
        "reproducibility": "补 reviewer-facing reproducibility skeleton，禁止写成 formal compliance claim。",
        "temperature_effect": "把温度影响占位项与 certificate / pre-run gate / uncertainty refs 对齐。",
        "pressure_effect": "把压力影响占位项与 pressure-related reference assets / uncertainty case 对齐。",
        "freshness_check": "保留 freshness reviewer 检查项，继续禁止 real acceptance 解释。",
        "writeback_verification": "把 writeback reviewer 占位项与 golden / report pack linkage 对齐，不改 primary evidence。",
    }

    assert {
        dimension: _method_confirmation_reviewer_action("CO2", dimension)
        for dimension in expected
    } == expected
