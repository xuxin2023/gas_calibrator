from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.tools.export_v2_module_disposition_inventory import (
    build_inventory,
    classify_module,
    write_inventory,
)


def _write(path: Path, text: str = "") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_classification_preserves_platform_shadow_and_compatibility_boundaries() -> None:
    assert classify_module(
        "gas_calibrator.v2.storage.coefficient_store",
        static_zero_reference=False,
    )[0] == "compatibility_wrapper"
    assert classify_module(
        "gas_calibrator.v2.storage.database",
        static_zero_reference=False,
    )[0] == "compatibility_wrapper"
    assert classify_module(
        "gas_calibrator.v2.storage.sidecar_index",
        static_zero_reference=False,
    )[0] == "compatibility_wrapper"
    assert classify_module(
        "gas_calibrator.v2.storage.queries",
        static_zero_reference=False,
    )[0] == "compatibility_wrapper"
    assert classify_module(
        "gas_calibrator.v2.storage.importer",
        static_zero_reference=False,
    )[0] == "compatibility_wrapper"
    assert classify_module(
        "gas_calibrator.v2.storage.v1_5_initialization",
        static_zero_reference=False,
    )[0] == "compatibility_wrapper"
    assert classify_module(
        "gas_calibrator.v2.storage.import_v1_5_initialization",
        static_zero_reference=False,
    )[0] == "compatibility_wrapper"
    assert classify_module(
        "gas_calibrator.v2.storage.import_v1_5_readiness_events",
        static_zero_reference=False,
    )[0] == "compatibility_wrapper"
    assert classify_module(
        "gas_calibrator.v2.storage.profile_store",
        static_zero_reference=False,
    )[0] == "platform_keep"
    assert classify_module(
        "gas_calibrator.v2.storage.exporter",
        static_zero_reference=False,
    )[0] == "platform_keep"
    assert classify_module(
        "gas_calibrator.v2.algorithms.robust",
        static_zero_reference=False,
    )[0] == "shadow_algorithm_keep"
    assert classify_module(
        "gas_calibrator.v2.ui_v2.shell",
        static_zero_reference=False,
    )[0] == "platform_keep"


def test_archive_review_requires_explicit_candidate_and_zero_static_references() -> None:
    module = "gas_calibrator.v2.core.services.conditioning_service_clean"
    assert classify_module(module, static_zero_reference=True)[0] == "archive_review"
    assert classify_module(module, static_zero_reference=False)[0] == "platform_keep"


def test_inventory_counts_relative_imports_and_blocks_v1_5_v2_imports(tmp_path: Path) -> None:
    _write(tmp_path / "src/gas_calibrator/v2/__init__.py")
    _write(tmp_path / "src/gas_calibrator/v2/storage/__init__.py")
    _write(tmp_path / "src/gas_calibrator/v2/storage/database.py", "VALUE = 1\n")
    _write(
        tmp_path / "src/gas_calibrator/v2/storage/queries.py",
        "from .database import VALUE\n",
    )
    _write(
        tmp_path / "tests/test_queries.py",
        "from gas_calibrator.v2.storage.queries import VALUE\n",
    )
    _write(
        tmp_path / "src/gas_calibrator/v1_5/runtime.py",
        "from gas_calibrator.v2.storage import queries\n",
    )

    payload = build_inventory(tmp_path)
    rows = {row["module"]: row for row in payload["modules"]}

    assert rows["gas_calibrator.v2.storage.database"]["v2_internal_reference_count"] == 1
    assert rows["gas_calibrator.v2.storage.queries"]["test_reference_count"] == 1
    assert payload["summary"]["v1_5_protected_import_violation_count"] >= 1
    assert all(row["delete_allowed"] is False for row in rows.values())


def test_write_inventory_emits_csv_json_and_markdown(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    _write(repo_root / "src/gas_calibrator/v2/__init__.py")
    _write(repo_root / "src/gas_calibrator/v2/ui_v2/shell.py", "VALUE = 1\n")
    payload = build_inventory(repo_root)

    outputs = write_inventory(payload, tmp_path / "output")

    assert Path(outputs["modules_csv"]).is_file()
    summary_path = Path(outputs["summary_json"])
    assert json.loads(summary_path.read_text(encoding="utf-8"))["schema"].endswith("_v1")
    assert "Automatic deletion permitted: `False`" in Path(
        outputs["summary_md"]
    ).read_text(encoding="utf-8")
