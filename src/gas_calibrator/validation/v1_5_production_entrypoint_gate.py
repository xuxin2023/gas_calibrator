"""Offline gate for V1.5 production entrypoint references.

The gate reviews a formal-flow plan JSON and blocks references to surfaces that
must never start production calibration: _handoff artifacts, root migration or
0624 handoff paths, diagnostics, per-point workers, V1/V2 launchers, and other
known non-production surfaces. It does not execute the plan.
"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from .v1_5_production_entrypoint_map import (
    MATURE_FITTING_BASELINE,
    MATURE_PHYSICAL_BASELINE,
    build_v1_5_production_entrypoint_map,
)


SCHEMA = "v1_5_production_entrypoint_gate_v1"

REFERENCE_KEYS = {
    "entrypoint",
    "entrypoint_path",
    "path",
    "tool",
    "tool_path",
    "script",
    "script_path",
    "module",
    "runner",
    "command",
    "cmd",
    "queue_source",
    "source",
    "source_path",
    "working_directory",
}

WORKER_PATHS = {
    "src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py",
    "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_sampling.py",
}

DIAGNOSTIC_TOKENS = (
    "diagnostic",
    "dynamic_pressure",
    "no_outp",
    "sealed",
    "tune",
    "extended_hold",
)

ROOT_MIGRATION_TOKENS = (
    "formal_queue_migration_20260624",
    "20260624",
    "0624",
)


@dataclass(frozen=True)
class ProductionEntrypointGateReference:
    step_id: str
    key_path: str
    reference: str
    normalized_reference: str
    status: str
    severity: str
    policy: str
    reason: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _normalize_slashes(value: str) -> str:
    return value.replace("\\", "/").strip().strip("'\"")


def _module_to_path(value: str) -> str | None:
    match = re.search(r"gas_calibrator\.tools\.([A-Za-z0-9_]+)", value)
    if not match:
        return None
    return f"src/gas_calibrator/tools/{match.group(1)}.py"


def _extract_path_reference(value: str) -> str:
    text = _normalize_slashes(value)
    module_path = _module_to_path(text)
    if module_path:
        return module_path

    path_match = re.search(r"(src/gas_calibrator/[^\s'\";]+?\.py)", text)
    if path_match:
        return _normalize_slashes(path_match.group(1))

    absolute_match = re.search(r"([A-Za-z]:/[^\s'\";]+?\.py)", text)
    if absolute_match:
        return _normalize_slashes(absolute_match.group(1))

    bare_tool_match = re.search(r"((?:run|export|import|validate|probe|query|prepare)_v1_?[A-Za-z0-9_]*\.py)", text)
    if bare_tool_match:
        return f"src/gas_calibrator/tools/{bare_tool_match.group(1)}"

    return text


def _iter_references(payload: Any, *, parent: str = "$", current_step: str = "") -> Iterable[tuple[str, str, str]]:
    if isinstance(payload, dict):
        step = str(payload.get("step_id") or payload.get("id") or payload.get("stage") or current_step or "")
        for key, value in payload.items():
            key_path = f"{parent}.{key}"
            if isinstance(value, str) and key in REFERENCE_KEYS:
                yield step or "(root)", key_path, value
            elif isinstance(value, (dict, list, tuple)):
                yield from _iter_references(value, parent=key_path, current_step=step)
    elif isinstance(payload, (list, tuple)):
        for index, value in enumerate(payload):
            yield from _iter_references(value, parent=f"{parent}[{index}]", current_step=current_step)


def _allowed_paths() -> set[str]:
    model = build_v1_5_production_entrypoint_map()
    return {
        _normalize_slashes(str(row["path"])).lower()
        for row in model["production_entrypoints"]
        if isinstance(row, dict)
    }


def _status_for_reference(reference: str, allowed_paths: set[str]) -> tuple[str, str, str, str]:
    raw = _normalize_slashes(reference)
    raw_lower = raw.lower()
    normalized = _extract_path_reference(raw)
    norm_lower = normalized.lower()
    basename = Path(normalized).name.lower()

    if "_handoff" in raw_lower or "/_handoff/" in raw_lower:
        return "blocker", "blocker", "handoff_not_production_launcher", "_handoff evidence or scratch paths cannot be production entrypoints."
    if any(token in raw_lower for token in ROOT_MIGRATION_TOKENS):
        return "blocker", "blocker", "root_or_0624_migration_not_mature_baseline", "0624/root migration/handoff references cannot be treated as the 0613/0620/0621 mature baseline."
    if raw_lower.startswith("d:/gas_calibrator/") and "/_worktrees/" not in raw_lower:
        return "blocker", "blocker", "root_migration_area_not_allowed", "Absolute D:/gas_calibrator root references are treated as migration/draft surfaces for V1.5 production plans."
    if norm_lower in WORKER_PATHS:
        return "blocker", "blocker", "sampling_worker_not_top_level", "Per-point sampling workers must be called by canonical CO2/H2O queue runners, not by the formal plan."
    if basename.startswith("run_v1_") and not basename.startswith("run_v1_5_"):
        return "blocker", "blocker", "legacy_v1_not_v1_5_production_entry", "Legacy V1 launchers are references/fallbacks, not V1.5 production entrypoints."
    if "/v2/" in raw_lower or "\\v2\\" in reference.lower() or "gas_calibrator.v2." in raw_lower:
        return "blocker", "blocker", "v2_not_v1_5_production_entry", "V2 surfaces must not be used to start V1.5 production calibration."
    if any(token in basename for token in DIAGNOSTIC_TOKENS):
        return "blocker", "blocker", "diagnostic_not_production_entry", "Diagnostic/probe/tune scripts are not production launchers or formal fit evidence."
    if norm_lower in allowed_paths:
        return "pass", "none", "production_entrypoint_allowed", "Reference is listed in the V1.5 production entrypoint map."
    if basename.startswith(("export_v1_5_", "import_v1_5_")):
        return "review_required", "review", "offline_support_review_required", "Offline support/export tools may be valid sidecars but are not production route launchers by default."
    if basename.startswith("run_v1_5_") or basename.startswith("validate_"):
        return "review_required", "review", "not_in_production_entrypoint_map", "This runnable surface is not in the production entrypoint map and needs explicit review before production use."
    return "pass", "none", "non_entrypoint_reference", "Reference does not look like an executable V1.5 production surface."


def build_v1_5_production_entrypoint_gate(*, plan_path: str | Path) -> dict[str, Any]:
    plan_file = Path(plan_path)
    payload = _load_json(plan_file)
    allowed_paths = _allowed_paths()
    references = []
    for step_id, key_path, reference in _iter_references(payload):
        normalized = _extract_path_reference(reference)
        status, severity, policy, reason = _status_for_reference(reference, allowed_paths)
        references.append(
            ProductionEntrypointGateReference(
                step_id=step_id,
                key_path=key_path,
                reference=reference,
                normalized_reference=normalized,
                status=status,
                severity=severity,
                policy=policy,
                reason=reason,
            )
        )

    blocker_count = sum(1 for row in references if row.status == "blocker")
    review_required_count = sum(1 for row in references if row.status == "review_required")
    status = "blocked" if blocker_count else "review_required" if review_required_count else "pass"
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "plan_path": str(plan_file),
        "manifest": {
            "status": status,
            "blocker_count": blocker_count,
            "review_required_count": review_required_count,
            "reference_count": len(references),
            "mature_fitting_baseline": MATURE_FITTING_BASELINE,
            "mature_physical_baseline": MATURE_PHYSICAL_BASELINE,
            "opens_com_ports": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "connects_postgresql": False,
            "writes_coefficients": False,
            "writes_sn_or_device_code": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
        },
        "references": [row.to_json() for row in references],
    }


def _markdown(model: dict[str, Any]) -> str:
    manifest = model["manifest"]
    lines = [
        "# V1.5 Production Entrypoint Gate",
        "",
        f"- schema: `{model['schema']}`",
        f"- status: `{manifest['status']}`",
        f"- blocker_count: `{manifest['blocker_count']}`",
        f"- review_required_count: `{manifest['review_required_count']}`",
        f"- reference_count: `{manifest['reference_count']}`",
        f"- mature_fitting_baseline: `{manifest['mature_fitting_baseline']}`",
        f"- mature_physical_baseline: `{manifest['mature_physical_baseline']}`",
        f"- plan_path: `{model['plan_path']}`",
        "",
        "## Reviewed References",
        "",
        "| step | status | policy | normalized reference | reason |",
        "|---|---|---|---|---|",
    ]
    for row in model["references"]:
        lines.append(
            f"| `{row['step_id']}` | `{row['status']}` | `{row['policy']}` | `{row['normalized_reference']}` | {row['reason']} |"
        )
    lines.extend(
        [
            "",
            "## Non-Execution Boundary",
            "",
            "- opens_com_ports: `false`",
            "- controls_pressure: `false`",
            "- controls_water_or_gas_routes: `false`",
            "- connects_postgresql: `false`",
            "- writes_coefficients: `false`",
            "- writes_sn_or_device_code: `false`",
            "- formal_release_allowed: `false`",
            "- database_import_allowed: `false`",
            "- not_real_acceptance_evidence: `true`",
            "",
            "This gate reviews references only. It does not execute a formal plan or authorize live calibration.",
            "",
        ]
    )
    return "\n".join(lines)


def write_v1_5_production_entrypoint_gate(*, plan_path: str | Path, output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    model = build_v1_5_production_entrypoint_gate(plan_path=plan_path)
    paths = {
        "manifest": out / "v1_5_production_entrypoint_gate.json",
        "references": out / "v1_5_production_entrypoint_gate_references.csv",
        "markdown": out / "V1_5_PRODUCTION_ENTRYPOINT_GATE.md",
    }
    paths["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    with paths["references"].open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=tuple(ProductionEntrypointGateReference.__dataclass_fields__.keys()))
        writer.writeheader()
        writer.writerows(model["references"])
    paths["markdown"].write_text(_markdown(model), encoding="utf-8")
    return paths


__all__ = [
    "SCHEMA",
    "build_v1_5_production_entrypoint_gate",
    "write_v1_5_production_entrypoint_gate",
]
