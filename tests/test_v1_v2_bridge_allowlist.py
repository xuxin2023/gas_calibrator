from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"

V1_RUNTIME_FILES = {
    SRC_ROOT / "gas_calibrator/config.py",
    SRC_ROOT / "gas_calibrator/workflow/runner.py",
    SRC_ROOT / "gas_calibrator/devices/gas_analyzer.py",
    SRC_ROOT / "gas_calibrator/logging_utils.py",
    SRC_ROOT / "gas_calibrator/tools/run_headless.py",
    SRC_ROOT / "gas_calibrator/tools/run_v1_corrected_autodelivery.py",
    SRC_ROOT / "gas_calibrator/tools/run_v1_online_acceptance.py",
}
V2_RUNTIME_ROOTS = {
    SRC_ROOT / "gas_calibrator/v2/config",
    SRC_ROOT / "gas_calibrator/v2/core",
    SRC_ROOT / "gas_calibrator/v2/ui_v2",
    SRC_ROOT / "gas_calibrator/v2/storage",
    SRC_ROOT / "gas_calibrator/v2/analytics",
    SRC_ROOT / "gas_calibrator/v2/qc",
    SRC_ROOT / "gas_calibrator/v2/domain",
}
V2_RUNTIME_FILES = {
    SRC_ROOT / "gas_calibrator/v2/entry.py",
}
BRIDGE_OR_SIDECAR_FILES = {
    SRC_ROOT / "gas_calibrator/tools/run_v1_merged_calibration_sidecar.py",
    SRC_ROOT / "gas_calibrator/tools/run_v1_no500_postprocess.py",
    SRC_ROOT / "gas_calibrator/v2/adapters/legacy_runner.py",
    SRC_ROOT / "gas_calibrator/v2/adapters/v1_route_trace.py",
    SRC_ROOT / "gas_calibrator/v2/scripts/run_v1_route_trace.py",
    SRC_ROOT / "gas_calibrator/v2/sim/parity.py",
}
SHARED_CANDIDATE_FILES = {
    SRC_ROOT / "gas_calibrator/tools/_no500_filter.py",
}
APPROVED_CROSS_BOUNDARY_SOURCES = BRIDGE_OR_SIDECAR_FILES | {
    SRC_ROOT / "gas_calibrator/tools/run_v1_corrected_autodelivery.py",
}


def _module_name(path: Path) -> str:
    rel = path.relative_to(SRC_ROOT).with_suffix("")
    parts = list(rel.parts)
    if parts and parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _resolve_internal_import(path: Path, module: str, level: int) -> Path | None:
    if level:
        package_parts = _module_name(path).split(".")[:-1]
        prefix_len = len(package_parts) - level + 1
        if prefix_len < 0:
            return None
        base_parts = package_parts[:prefix_len]
        module_parts = [part for part in module.split(".") if part]
        parts = base_parts + module_parts
        resolved = ".".join(parts)
    else:
        resolved = module
    if not resolved.startswith("gas_calibrator"):
        return None
    candidate = SRC_ROOT / Path(*resolved.split("."))
    if candidate.with_suffix(".py").exists():
        return candidate.with_suffix(".py")
    if (candidate / "__init__.py").exists():
        return candidate / "__init__.py"
    return None


def _classify(path: Path) -> str | None:
    if path in V1_RUNTIME_FILES:
        return "V1_RUNTIME"
    if path in BRIDGE_OR_SIDECAR_FILES:
        return "BRIDGE_OR_SIDECAR"
    if path in SHARED_CANDIDATE_FILES:
        return "SHARED_CANDIDATE"
    if path in V2_RUNTIME_FILES or any(root in path.parents for root in V2_RUNTIME_ROOTS):
        return "V2_RUNTIME"
    return None


def _cross_boundary_edges() -> list[tuple[Path, str, Path, str, int]]:
    edges: list[tuple[Path, str, Path, str, int]] = []
    for path in sorted((SRC_ROOT / "gas_calibrator").rglob("*.py")):
        source_owner = _classify(path)
        if source_owner is None:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    target = _resolve_internal_import(path, alias.name, 0)
                    if target is None:
                        continue
                    target_owner = _classify(target)
                    if target_owner and target_owner != source_owner:
                        edges.append((path, source_owner, target, target_owner, node.lineno))
            elif isinstance(node, ast.ImportFrom):
                target = _resolve_internal_import(path, str(node.module or ""), int(node.level or 0))
                if target is None:
                    continue
                target_owner = _classify(target)
                if target_owner and target_owner != source_owner:
                    edges.append((path, source_owner, target, target_owner, node.lineno))
    return edges


def test_cross_boundary_imports_match_allowlist() -> None:
    edges = _cross_boundary_edges()
    violations: list[str] = []
    observed_sources = {source for source, *_rest in edges}

    for source, source_owner, target, target_owner, lineno in edges:
        if source_owner == "V1_RUNTIME" and target_owner in {"V2_RUNTIME", "BRIDGE_OR_SIDECAR"}:
            violations.append(f"{source}:{lineno} {source_owner}->{target_owner} via {target}")
            continue
        if source_owner == "V2_RUNTIME" and target_owner in {"V1_RUNTIME", "BRIDGE_OR_SIDECAR"}:
            violations.append(f"{source}:{lineno} {source_owner}->{target_owner} via {target}")
            continue
        if source_owner == "SHARED_CANDIDATE" and target_owner != "SHARED_CANDIDATE":
            violations.append(f"{source}:{lineno} {source_owner}->{target_owner} via {target}")
            continue
        if source not in APPROVED_CROSS_BOUNDARY_SOURCES and target_owner != "SHARED_CANDIDATE":
            violations.append(f"{source}:{lineno} unexpected cross-boundary source via {target}")

    unexpected_sources = observed_sources - APPROVED_CROSS_BOUNDARY_SOURCES - SHARED_CANDIDATE_FILES
    assert violations == [], "Unexpected runtime boundary crossings:\n" + "\n".join(violations)
    assert unexpected_sources == set(), "Unexpected cross-boundary files:\n" + "\n".join(
        str(path) for path in sorted(unexpected_sources)
    )
