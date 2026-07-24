"""Export a read-only disposition inventory for the V2 Python package."""

from __future__ import annotations

import argparse
import ast
import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Sequence


SCHEMA = "v2_module_disposition_inventory_v1"
V2_PREFIX = "gas_calibrator.v2"
COMPATIBILITY_WRAPPERS = {
    "gas_calibrator.v2.storage.coefficient_store",
    "gas_calibrator.v2.storage.database",
    "gas_calibrator.v2.storage.import_v1_5_initialization",
    "gas_calibrator.v2.storage.import_v1_5_readiness_events",
    "gas_calibrator.v2.storage.importer",
    "gas_calibrator.v2.storage.models",
    "gas_calibrator.v2.storage.queries",
    "gas_calibrator.v2.storage.sidecar_index",
    "gas_calibrator.v2.storage.v1_5_initialization",
    "gas_calibrator.v2.utils",
    "gas_calibrator.v2.utils.converters",
}
EXPLICIT_ARCHIVE_REVIEW = {
    "gas_calibrator.v2.core.services.conditioning_service_clean",
}
V2_PRODUCT_STORAGE_ADAPTERS = {
    "gas_calibrator.v2.storage",
    "gas_calibrator.v2.storage.exporter",
    "gas_calibrator.v2.storage.import_run",
    "gas_calibrator.v2.storage.profile_store",
}
SHADOW_CORE_NAMES = {
    "calibration_service",
    "coefficient_service",
    "device_factory",
    "device_manager",
    "no_write_guard",
    "orchestrator",
    "plan_compiler",
    "point_parser",
    "refit_filtering",
    "route_planner",
    "sampling_service",
    "stability_checker",
}


def _module_name(path: Path, source_root: Path) -> str:
    relative = path.relative_to(source_root).with_suffix("")
    parts = list(relative.parts)
    if parts and parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _resolve_relative_import(
    *,
    current_module: str,
    current_is_package: bool,
    level: int,
    module: str | None,
) -> str:
    package_parts = current_module.split(".") if current_is_package else current_module.split(".")[:-1]
    ascend = max(0, level - 1)
    if ascend:
        package_parts = package_parts[:-ascend] if ascend < len(package_parts) else []
    if module:
        package_parts.extend(module.split("."))
    return ".".join(package_parts)


def _import_names(path: Path, source_root: Path) -> set[str]:
    try:
        text = path.read_text(encoding="utf-8-sig")
        tree = ast.parse(text, filename=str(path))
    except (OSError, SyntaxError, UnicodeError):
        return set()

    try:
        current_module = _module_name(path, source_root)
    except ValueError:
        current_module = path.stem
    current_is_package = path.name == "__init__.py"
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
            continue
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.level:
            base = _resolve_relative_import(
                current_module=current_module,
                current_is_package=current_is_package,
                level=node.level,
                module=node.module,
            )
        else:
            base = str(node.module or "")
        if base:
            imports.add(base)
        for alias in node.names:
            if alias.name != "*" and base:
                imports.add(f"{base}.{alias.name}")
    return imports


def _matching_v2_modules(import_name: str, known_modules: set[str]) -> set[str]:
    candidate = import_name
    matches: set[str] = set()
    while candidate:
        if candidate in known_modules:
            matches.add(candidate)
            break
        candidate, _, _ = candidate.rpartition(".")
    return matches


def classify_module(module: str, *, static_zero_reference: bool) -> tuple[str, str]:
    relative = module.removeprefix(f"{V2_PREFIX}.")
    parts = relative.split(".") if relative else []
    top = parts[0] if parts else ""
    leaf = parts[-1] if parts else ""

    if module in COMPATIBILITY_WRAPPERS:
        return (
            "compatibility_wrapper",
            "temporary V2 import compatibility; implementation is owned by a shared gas_calibrator namespace",
        )
    if module in V2_PRODUCT_STORAGE_ADAPTERS:
        return (
            "platform_keep",
            "V2 product adapter retained above shared storage infrastructure",
        )
    if module in EXPLICIT_ARCHIVE_REVIEW and static_zero_reference:
        return (
            "archive_review",
            "statically unreferenced duplicate-named implementation; manual evidence review required",
        )
    if top == "storage":
        return (
            "shared_migration_candidate",
            "storage and persistence are product-neutral shared infrastructure",
        )
    if top == "utils":
        return (
            "shared_migration_candidate",
            "generic utility code should be assessed for a neutral namespace",
        )
    if top in {"algorithms", "calibration"}:
        return (
            "shadow_algorithm_keep",
            "candidate algorithm code remains simulation/replay/shadow only",
        )
    if top == "core" and (
        leaf in SHADOW_CORE_NAMES
        or "runner" in parts
        or "workflow_steps" in parts
        or leaf.startswith("run001_")
        or leaf.startswith("real_com_")
    ):
        return (
            "shadow_algorithm_keep",
            "V2 execution or probe code remains isolated from the V1.5 production entry",
        )
    return (
        "platform_keep",
        "platform, governance, analytics, UI, simulation, adapter, or evidence capability",
    )


def _is_v1_5_protected_path(path: Path, repo_root: Path) -> bool:
    try:
        relative = path.relative_to(repo_root).as_posix()
    except ValueError:
        return False
    if relative.startswith("src/gas_calibrator/v1_5/"):
        return True
    if relative.startswith("src/gas_calibrator/storage/v1_5_evidence/"):
        return True
    name = path.name
    if relative.startswith("src/gas_calibrator/tools/") and name.startswith("run_v1_5"):
        return True
    return relative.startswith("src/gas_calibrator/validation/") and name.startswith("v1_5")


def _reference_kind(path: Path, *, repo_root: Path, v2_root: Path) -> str:
    if path.is_relative_to(v2_root):
        return "v2_internal"
    if path.is_relative_to(repo_root / "tests"):
        return "test"
    return "external_source"


def _iter_python_files(roots: Iterable[Path]) -> list[Path]:
    files: set[Path] = set()
    for root in roots:
        if root.exists():
            files.update(path for path in root.rglob("*.py") if "__pycache__" not in path.parts)
    return sorted(files)


def build_inventory(repo_root: Path) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    source_root = repo_root / "src"
    v2_root = source_root / "gas_calibrator" / "v2"
    if not v2_root.is_dir():
        raise FileNotFoundError(f"V2 package not found: {v2_root}")

    module_paths = {
        _module_name(path, source_root): path
        for path in _iter_python_files([v2_root])
    }
    known_modules = set(module_paths)
    references: dict[str, dict[str, set[str]]] = {
        module: {
            "v2_internal": set(),
            "external_source": set(),
            "test": set(),
        }
        for module in known_modules
    }
    protected_import_violations: list[dict[str, str]] = []

    scan_files = _iter_python_files([source_root, repo_root / "tests"])
    for path in scan_files:
        source_module = (
            _module_name(path, source_root)
            if path.is_relative_to(source_root)
            else path.relative_to(repo_root).as_posix()
        )
        imported_names = _import_names(
            path,
            source_root if path.is_relative_to(source_root) else repo_root,
        )
        if _is_v1_5_protected_path(path, repo_root):
            for imported_name in sorted(imported_names):
                if imported_name == V2_PREFIX or imported_name.startswith(f"{V2_PREFIX}."):
                    protected_import_violations.append(
                        {
                            "path": str(path.relative_to(repo_root)),
                            "import_name": imported_name,
                        }
                    )
        kind = _reference_kind(path, repo_root=repo_root, v2_root=v2_root)
        for imported_name in imported_names:
            for target in _matching_v2_modules(imported_name, known_modules):
                if target != source_module:
                    references[target][kind].add(str(path.relative_to(repo_root)))

    modules: list[dict[str, Any]] = []
    for module, path in sorted(module_paths.items()):
        module_refs = references[module]
        total_refs = sum(len(values) for values in module_refs.values())
        static_zero_reference = total_refs == 0
        disposition, reason = classify_module(
            module,
            static_zero_reference=static_zero_reference,
        )
        modules.append(
            {
                "module": module,
                "path": str(path.relative_to(repo_root)),
                "disposition": disposition,
                "disposition_reason": reason,
                "v2_internal_reference_count": len(module_refs["v2_internal"]),
                "external_source_reference_count": len(module_refs["external_source"]),
                "test_reference_count": len(module_refs["test"]),
                "total_static_reference_count": total_refs,
                "static_zero_reference": static_zero_reference,
                "delete_allowed": False,
                "manual_review_required": disposition
                in {"archive_review", "shared_migration_candidate"},
            }
        )

    disposition_counts = dict(sorted(Counter(row["disposition"] for row in modules).items()))
    summary = {
        "module_count": len(modules),
        "disposition_counts": disposition_counts,
        "static_zero_reference_count": sum(row["static_zero_reference"] for row in modules),
        "archive_review_count": disposition_counts.get("archive_review", 0),
        "shared_migration_candidate_count": disposition_counts.get(
            "shared_migration_candidate",
            0,
        ),
        "v1_5_protected_import_violation_count": len(protected_import_violations),
        "automatic_deletion_permitted": False,
    }
    return {
        "schema": SCHEMA,
        "generated_at": datetime.now().astimezone().isoformat(),
        "repo_root": str(repo_root),
        "policy": {
            "production_core": "native V1.5 mature 0613/0620/0621 route",
            "v2_role": "platform plus simulation/replay/shadow qualification",
            "archive_review_meaning": "manual review candidate only; never an automatic deletion decision",
            "default_entry_change_allowed": False,
            "automatic_deletion_permitted": False,
        },
        "summary": summary,
        "v1_5_protected_import_violations": protected_import_violations,
        "modules": modules,
    }


def _write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    fieldnames = (
        "module",
        "path",
        "disposition",
        "disposition_reason",
        "v2_internal_reference_count",
        "external_source_reference_count",
        "test_reference_count",
        "total_static_reference_count",
        "static_zero_reference",
        "delete_allowed",
        "manual_review_required",
    )
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_inventory(payload: dict[str, Any], output_dir: Path) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    modules_csv = output_dir / "v2_modules.csv"
    summary_json = output_dir / "v2_module_disposition_summary.json"
    summary_md = output_dir / "v2_module_disposition_summary.md"
    _write_csv(modules_csv, payload["modules"])
    summary_json.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    summary = payload["summary"]
    summary_md.write_text(
        "\n".join(
            [
                "# V2 Module Disposition Inventory",
                "",
                f"- Generated: `{payload['generated_at']}`",
                f"- Modules: `{summary['module_count']}`",
                f"- Static zero-reference modules: `{summary['static_zero_reference_count']}`",
                f"- V1.5 protected import violations: `{summary['v1_5_protected_import_violation_count']}`",
                f"- Automatic deletion permitted: `{summary['automatic_deletion_permitted']}`",
                "",
                "## Dispositions",
                "",
                *[
                    f"- `{name}`: {count}"
                    for name, count in summary["disposition_counts"].items()
                ],
                "",
                "## Boundary",
                "",
                "- Native mature V1.5 remains the production calibration core.",
                "- V2 remains the product platform and simulation/replay/shadow qualification layer.",
                "- Archive review is not delete approval.",
                "- No module may be deleted until references, tests, artifacts, and replacement coverage are reviewed.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return {
        "modules_csv": str(modules_csv),
        "summary_json": str(summary_json),
        "summary_md": str(summary_md),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = build_inventory(args.repo_root)
    outputs = write_inventory(payload, args.output_dir)
    print(
        json.dumps(
            {
                "status": "ok",
                "schema": SCHEMA,
                "summary": payload["summary"],
                "outputs": outputs,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 1 if payload["summary"]["v1_5_protected_import_violation_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
