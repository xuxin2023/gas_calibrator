from __future__ import annotations

import ast
from pathlib import Path

from gas_calibrator.v2.core.run001_a2_no_write import (
    _build_positive_preseal_pressurization_evidence,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
TARGET_SOURCES = (
    REPO_ROOT / "src" / "gas_calibrator" / "v2" / "core" / "run001_a2_no_write.py",
    REPO_ROOT / "src" / "gas_calibrator" / "v2" / "export" / "ratio_poly_report.py",
)


def test_target_sources_have_unique_literal_dictionary_keys() -> None:
    duplicates: list[tuple[str, int, str]] = []

    for path in TARGET_SOURCES:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Dict):
                continue
            seen: set[object] = set()
            for key in node.keys:
                if not isinstance(key, ast.Constant):
                    continue
                if key.value in seen:
                    duplicates.append((path.name, key.lineno, repr(key.value)))
                seen.add(key.value)

    assert duplicates == []


def test_positive_preseal_pressure_key_order_and_values_are_preserved(tmp_path: Path) -> None:
    payload = _build_positive_preseal_pressurization_evidence(
        tmp_path,
        {"run_id": "gate72-dictionary-key-hygiene"},
    )
    keys = list(payload)

    assert payload["pressure_max_hpa"] is None
    assert payload["pressure_min_hpa"] is None
    assert keys.index("pressure_samples_count") < keys.index("pressure_max_hpa")
    assert keys.index("pressure_max_hpa") < keys.index("pressure_min_hpa")
    assert keys.index("pressure_min_hpa") < keys.index("ready_reached")
