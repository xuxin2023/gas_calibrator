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

SCHEMA = "v2_module_disposition_inventory_v4"
V2_PREFIX = "gas_calibrator.v2"
DYNAMIC_EXPORT_TABLE_NAMES = {"_EXPORTS", "_LAZY_EXPORTS", "_EXPORT_MAP"}
STEP3A_R0_CONTROLLED_PROBE_MODULES = {
    "gas_calibrator.v2.core.query_only_readers",
    "gas_calibrator.v2.core.run001_query_only_real_com_probe",
    "gas_calibrator.v2.scripts.query_only_com_sanity_probe",
}
INTENTIONAL_EXTERNAL_ENTRYPOINTS = {
    "gas_calibrator.v2.scripts.query_only_com_sanity_probe",
}
COMPATIBILITY_WRAPPERS: set[str] = set()
V1_5_PRODUCT_STORAGE_ADAPTERS: set[str] = set()
EXTRACTED_PAGE_SHELLS = {
    "gas_calibrator.v2.ui_v2.pages.algorithms_page",
    "gas_calibrator.v2.ui_v2.pages.devices_page",
    "gas_calibrator.v2.ui_v2.pages.qc_page",
    "gas_calibrator.v2.ui_v2.pages.reports_page",
    "gas_calibrator.v2.ui_v2.pages.results_page",
}
EXTRACTED_REDUNDANT_UI = {
    "gas_calibrator.v2.ui_v2.widgets.ai_summary_panel",
    "gas_calibrator.v2.ui_v2.widgets.algorithm_compare_table",
    "gas_calibrator.v2.ui_v2.widgets.analyzer_health_panel",
    "gas_calibrator.v2.ui_v2.widgets.artifact_list_panel",
    "gas_calibrator.v2.ui_v2.widgets.collapsible_section",
    "gas_calibrator.v2.ui_v2.widgets.device_status_table",
    "gas_calibrator.v2.ui_v2.widgets.export_bar",
    "gas_calibrator.v2.ui_v2.widgets.metric_card",
    "gas_calibrator.v2.ui_v2.widgets.qc_overview_panel",
    "gas_calibrator.v2.ui_v2.widgets.qc_reject_reason_chart",
    "gas_calibrator.v2.ui_v2.widgets.residual_chart",
    "gas_calibrator.v2.ui_v2.widgets.winner_badge",
}
RETIRED_SIMULATION_WORKBENCH = {
    "gas_calibrator.v2.ui_v2.controllers.device_workbench",
    "gas_calibrator.v2.ui_v2.widgets.device_workbench",
}
RETIRED_V2_PRODUCT_RUNTIME = {
    "gas_calibrator.v2.adapters.historical_frame_parity_audit",
    "gas_calibrator.v2.adapters.v1_sidecar_watcher",
    "gas_calibrator.v2.core.certificate_metrics_registry",
    "gas_calibrator.v2.core.certificate_operational_admission",
    "gas_calibrator.v2.core.certificate_evidence_census",
    "gas_calibrator.v2.core.cutover_candidate_worksheet",
    "gas_calibrator.v2.core.golden_dataset_registry",
    "gas_calibrator.v2.core.regression_scoreboard",
    "gas_calibrator.v2.core.real_com_probe_gate",
    "gas_calibrator.v2.core.run001_a1_analyzer_id_truth",
    "gas_calibrator.v2.core.run001_a1_analyzer_diagnostics",
    "gas_calibrator.v2.core.run001_a1_analyzer_mapping",
    "gas_calibrator.v2.core.run001_a1_analyzer_mode2_setup",
    "gas_calibrator.v2.core.run001_a1_serial_assistant_probe",
    "gas_calibrator.v2.core.run001_a1r_minimal_no_write_sampling_probe",
    "gas_calibrator.v2.core.run001_a2_co2_only_7_pressure_no_write_probe",
    "gas_calibrator.v2.core.run001_conditioning_only_probe",
    "gas_calibrator.v2.core.run001_r1_conditioning_only_probe",
    "gas_calibrator.v2.core.run001_r0_1_reference_read_probe",
    "gas_calibrator.v2.core.run001_rs485_alignment",
    "gas_calibrator.v2.scripts.audit_run001_a1_analyzer_id_truth",
    "gas_calibrator.v2.scripts.audit_historical_frame_parity",
    "gas_calibrator.v2.scripts.build_certificate_operational_admission",
    "gas_calibrator.v2.scripts.build_certificate_evidence_census",
    "gas_calibrator.v2.scripts.build_regression_scoreboard",
    "gas_calibrator.v2.scripts.build_cutover_candidate_worksheet",
    "gas_calibrator.v2.scripts.diagnose_run001_a1_analyzers",
    "gas_calibrator.v2.scripts.prepare_run001_a1_analyzers_mode2",
    "gas_calibrator.v2.scripts.probe_run001_a1_serial_assistant_equivalent",
    "gas_calibrator.v2.scripts.run_v2",
    "gas_calibrator.v2.scripts.run001_a1_no_write_dry_run",
    "gas_calibrator.v2.scripts.run001_a1r_minimal_no_write_sampling_probe",
    "gas_calibrator.v2.scripts.run001_a2_co2_only_7_pressure_no_write_probe",
    "gas_calibrator.v2.scripts.run001_a2_no_write_pressure_sweep",
    "gas_calibrator.v2.scripts.run001_conditioning_only_probe",
    "gas_calibrator.v2.scripts.run001_conditioning_only_real_com_probe",
    "gas_calibrator.v2.scripts.run001_query_only_real_com_probe",
    "gas_calibrator.v2.scripts.run001_r0_1_reference_read_probe",
    "gas_calibrator.v2.scripts.run001_r1_conditioning_only_probe",
    "gas_calibrator.v2.scripts.build_rs485_v1_v2_alignment_matrix",
    "gas_calibrator.v2.scripts.test_v2_device",
    "gas_calibrator.v2.scripts.test_v2_safe",
    "gas_calibrator.v2.scripts.v1_postprocess_gui",
    "gas_calibrator.v2.storage.coefficient_store",
    "gas_calibrator.v2.storage.database",
    "gas_calibrator.v2.storage.exporter",
    "gas_calibrator.v2.storage.import_run",
    "gas_calibrator.v2.storage.import_v1_5_initialization",
    "gas_calibrator.v2.storage.import_v1_5_readiness_events",
    "gas_calibrator.v2.storage.importer",
    "gas_calibrator.v2.storage.models",
    "gas_calibrator.v2.storage.profile_store",
    "gas_calibrator.v2.storage.queries",
    "gas_calibrator.v2.storage.sidecar_index",
    "gas_calibrator.v2.storage",
    "gas_calibrator.v2.storage.v1_5_initialization",
    "gas_calibrator.v2.utils",
    "gas_calibrator.v2.utils.converters",
    "gas_calibrator.v2.ui_v2.dialogs.about_dialog",
    "gas_calibrator.v2.ui_v2.dialogs.licenses_dialog",
    "gas_calibrator.v2.ui_v2.dialogs.preferences_dialog",
    "gas_calibrator.v2.ui_v2.dialogs.release_notes_dialog",
    "gas_calibrator.v2.ui_v2.runtime.build_info_loader",
    "gas_calibrator.v2.ui_v2.runtime.crash_recovery",
    "gas_calibrator.v2.ui_v2.runtime.recovery_store",
    "gas_calibrator.v2.ui_v2.runtime.release_notes_loader",
    "gas_calibrator.v2.ui_v2.styles",
    "gas_calibrator.v2.ui_v2.utils.app_info",
    "gas_calibrator.v2.ui_v2.utils.preferences_store",
    "gas_calibrator.v2.ui_v2.utils.recent_runs_store",
    "gas_calibrator.v2.ui_v2.utils.route_memory",
    "gas_calibrator.v2.ui_v2.utils.runtime_paths",
    "gas_calibrator.v2.ui_v2.widgets.busy_overlay",
    "gas_calibrator.v2.ui_v2.widgets.error_banner",
    "gas_calibrator.v2.ui_v2.widgets.empty_state",
    "gas_calibrator.v2.ui_v2.widgets.log_panel",
    "gas_calibrator.v2.ui_v2.widgets.notification_center",
    "gas_calibrator.v2.ui_v2.widgets.route_progress_timeline",
    "gas_calibrator.v2.ui_v2.widgets.timeseries_chart",
    "gas_calibrator.v2.ui_v2.app",
    "gas_calibrator.v2.ui_v2.shell",
    "gas_calibrator.v2.ui_v2.review_center_presenter",
    "gas_calibrator.v2.ui_v2.review_center_artifact_scope",
    "gas_calibrator.v2.ui_v2.review_center_scan_contracts",
    "gas_calibrator.v2.ui_v2.review_scope_export_index",
    "gas_calibrator.v2.ui_v2.controllers.app_facade",
    "gas_calibrator.v2.ui_v2.controllers.live_state_feed",
    "gas_calibrator.v2.ui_v2.controllers.plan_gateway",
    "gas_calibrator.v2.ui_v2.controllers.run_controller",
    "gas_calibrator.v2.ui_v2.controllers.shortcut_manager",
    "gas_calibrator.v2.ui_v2.diagnostics.diagnostic_bundle_exporter",
    "gas_calibrator.v2.ui_v2.diagnostics.redact_helpers",
    "gas_calibrator.v2.ui_v2.packaging.preflight_checks",
    "gas_calibrator.v2.ui_v2.packaging.runtime_manifest",
    "gas_calibrator.v2.ui_v2.pages.plan_editor_page",
    "gas_calibrator.v2.ui_v2.pages.run_control_page",
    "gas_calibrator.v2.ui_v2.pages.certificate_metrics_page",
    "gas_calibrator.v2.ui_v2.pages.visitor_showcase_page",
    "gas_calibrator.v2.ui_v2.theme.tokens",
    "gas_calibrator.v2.ui_v2.theme.ttk_theme",
    "gas_calibrator.v2.ui_v2.utils.screenshot",
    "gas_calibrator.v2.ui_v2.widgets.scrollable_page_frame",
    "gas_calibrator.v2.ui_v2.widgets.review_center_panel",
    "gas_calibrator.v2.ui_v2.widgets.startup_splash",
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
    "route_planner",
    "sampling_service",
    "stability_checker",
}
VALIDATION_SIMULATION_RUNTIME_MODULES = {
    "gas_calibrator.v2.core.calibration_service",
    "gas_calibrator.v2.core.device_factory",
    "gas_calibrator.v2.core.device_manager",
    "gas_calibrator.v2.core.orchestrator",
    "gas_calibrator.v2.core.plan_compiler",
    "gas_calibrator.v2.core.point_parser",
    "gas_calibrator.v2.core.route_planner",
    "gas_calibrator.v2.core.services.sampling_service",
    "gas_calibrator.v2.core.stability_checker",
    "gas_calibrator.v2.core.workflow_steps",
    "gas_calibrator.v2.entry",
}
VALIDATION_CONFIGURATION_MODULES = {
    "gas_calibrator.v2.config.models",
}
VALIDATION_ENGINEERING_PROBE_MODULES = {
    "gas_calibrator.v2.core.no_write_guard",
    "gas_calibrator.v2.core.run001_a1_dry_run",
    "gas_calibrator.v2.core.run001_a2_no_write",
}
VALIDATION_METROLOGY_MODULES: set[str] = set()
FINAL_VALIDATION_MIGRATION_MODULES = (
    VALIDATION_SIMULATION_RUNTIME_MODULES
    | VALIDATION_CONFIGURATION_MODULES
    | VALIDATION_ENGINEERING_PROBE_MODULES
    | VALIDATION_METROLOGY_MODULES
)
PROBE_OR_CUTOVER_TOKENS = {
    "cutover",
    "real_com",
    "run001_",
    "serial_assistant_probe",
}


def _target_namespace(disposition: str, module: str = "") -> str:
    if module in VALIDATION_CONFIGURATION_MODULES:
        return "gas_calibrator.validation.simulation.config"
    if module in VALIDATION_SIMULATION_RUNTIME_MODULES:
        return "gas_calibrator.validation.simulation"
    if module in VALIDATION_ENGINEERING_PROBE_MODULES:
        return "gas_calibrator.validation.engineering_probe"
    if module in VALIDATION_METROLOGY_MODULES:
        return "gas_calibrator.validation.metrology"
    return {
        "retain_step3a_r0": "gas_calibrator.v2 (temporary controlled Step 3A exception)",
        "compatibility_wrapper": "already migrated; remove wrapper after callers move",
        "migrate_to_v1_5": "gas_calibrator.v1_5",
        "migrate_to_shared": "gas_calibrator.storage or gas_calibrator.utils",
        "migrate_to_validation": "gas_calibrator.validation",
        "migrate_to_modeling": "gas_calibrator.modeling",
        "delete_after_extraction": "",
        "delete_now": "",
    }[disposition]


def _exit_phase(disposition: str, module: str = "") -> str:
    if module in VALIDATION_CONFIGURATION_MODULES:
        return "Gate 2 after closure-wide parity/nightly migration and zero V2 callers"
    if module in VALIDATION_SIMULATION_RUNTIME_MODULES:
        return "Gate 2 after closure-wide parity/nightly migration and zero V2 callers"
    if module in VALIDATION_ENGINEERING_PROBE_MODULES:
        return "Step 3A closure or Gate 2 no-write engineering-validation migration"
    if module in VALIDATION_METROLOGY_MODULES:
        return "Gate 2 after scientific review and nightly contract migration"
    return {
        "retain_step3a_r0": "Step 3A closure",
        "compatibility_wrapper": "Gate 4",
        "migrate_to_v1_5": "Gate 2",
        "migrate_to_shared": "Gate 2",
        "migrate_to_validation": "Gate 2",
        "migrate_to_modeling": "Gate 2",
        "delete_after_extraction": "Gate 3",
        "delete_now": "Gate 1",
    }[disposition]


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
    package_parts = (
        current_module.split(".")
        if current_is_package
        else current_module.split(".")[:-1]
    )
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
    for node in tree.body:
        value: ast.expr | None = None
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id in DYNAMIC_EXPORT_TABLE_NAMES
            for target in node.targets
        ):
            value = node.value
        elif (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id in DYNAMIC_EXPORT_TABLE_NAMES
        ):
            value = node.value
        if not isinstance(value, ast.Dict):
            continue
        for entry in value.values:
            if not isinstance(entry, (ast.Tuple, ast.List)) or not entry.elts:
                continue
            module_node = entry.elts[0]
            if isinstance(module_node, ast.Constant) and isinstance(
                module_node.value, str
            ):
                module_name = module_node.value
                if module_name.startswith("."):
                    level = len(module_name) - len(module_name.lstrip("."))
                    module_name = _resolve_relative_import(
                        current_module=current_module,
                        current_is_package=current_is_package,
                        level=level,
                        module=module_name[level:] or None,
                    )
                if module_name:
                    imports.add(module_name)
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

    if module in STEP3A_R0_CONTROLLED_PROBE_MODULES:
        return (
            "retain_step3a_r0",
            "temporary Step 3A R0 query-only exception; retain behind dual unlock, operator confirmation, no-write evidence, and blocked promotion until the exception is formally closed",
        )
    if module in COMPATIBILITY_WRAPPERS:
        return (
            "compatibility_wrapper",
            "temporary V2 import compatibility; implementation is owned by a final V1.5/shared namespace",
        )
    if module in V1_5_PRODUCT_STORAGE_ADAPTERS:
        return (
            "migrate_to_v1_5",
            "product-specific import/export/profile semantics belong to the final V1.5 product",
        )
    if module in VALIDATION_CONFIGURATION_MODULES:
        return (
            "migrate_to_validation",
            "active simulation configuration contract; the external sidecar is decoupled, the inert AlgorithmConfig, obsolete V2 storage schema field, obsolete entry config helpers, the unused V2 startup-connect mirror, misleading unused pressure-control knobs, the unused CO2GroupConfig/group_a/group_b/valve_mapping mirrors, the V2-only order/signal_keys coefficient mirrors, and the V2-only gas-level summary temperature/pressure mirrors are retired, while the raw V1 startup-connect dictionary, raw V1 AMT order/signal_keys, and raw V1 gas-level summary-column contracts remain production-owned; the complete active offline ratio-poly coefficient/report configuration subclosure is validation-owned, including report_temperature_key/report_pressure_key and the shared dewpoint-backed dry-anchor gate for H2O, the active nine-field valve snapshot subclosure is validation-owned, and the pure run-mode alias normalization, pure sensor-precheck normalization, and pure Step 2 port/engineering-flag risk-inventory, classification/badge/inventory-detail presentation, and hydrate/review/governance-handoff leaves are validation-owned, with the V2 path retained only as an identity-compatible AppConfig facade; the raw valve-route dictionary consumed by ValveRoutingService and its physical mapping remain frozen, while ValveConfig retains only manifest evidence fields; all SingleDeviceConfig fields and DeviceConfig roles remain active, including the reference thermometer now closed across registry, creation, sampling, and manifest evidence; simulation storage is fail-closed, adjacent storage requires explicit enablement, the common storage contract is shared-owned with a database-only schema/async extension, and the stability, sampling, precheck, pressure-tolerance, quality-control, optional-AI, offline-path, and runtime-feature config subclosures are validation-owned; migrate the remaining closure only with parity/nightly preservation and zero V2 callers",
        )
    if module in VALIDATION_SIMULATION_RUNTIME_MODULES:
        return (
            "migrate_to_validation",
            "active offline simulation execution closure; partial deletion is blocked while runtime or test consumers remain, and migration must preserve parity/nightly behavior before the V2 path is removed",
        )
    if module in VALIDATION_ENGINEERING_PROBE_MODULES:
        return (
            "migrate_to_validation",
            "no-write engineering-probe safety contract; keep promotion blocked and remove the V2 path only after Step 3A closure or an equivalent validation-owned migration",
        )
    if module in VALIDATION_METROLOGY_MODULES:
        return (
            "migrate_to_validation",
            "active nightly metrology contract; keep diagnostic-only and not-real-acceptance semantics, complete scientific review, then migrate outside the V2 product namespace",
        )
    if module in EXTRACTED_PAGE_SHELLS:
        return (
            "delete_after_extraction",
            "the required read-only product surface now exists behind the V1.5 workstation snapshot; remove this page shell with the V2 shell",
        )
    if module in EXTRACTED_REDUNDANT_UI:
        return (
            "delete_after_extraction",
            "presentation-only widget duplicates an extracted V1.5 read-only surface and owns no authoritative computation or artifact generation",
        )
    if module in RETIRED_SIMULATION_WORKBENCH:
        return (
            "delete_after_extraction",
            "simulation-only presets, fault injection, and a second device-state source are excluded from the final V1.5 product; remove after shared reviewer contracts are detached",
        )
    if module in RETIRED_V2_PRODUCT_RUNTIME:
        return (
            "delete_after_extraction",
            "obsolete V2 launcher or launcher-only desktop infrastructure; the V1.5 workstation is the sole product candidate",
        )
    if top == "tests":
        return (
            "delete_now",
            "test files must live under repository tests/ and must not ship inside the runtime package",
        )
    if top == "storage":
        return (
            "migrate_to_shared",
            "storage and persistence are product-neutral shared infrastructure",
        )
    if top == "utils":
        return (
            "migrate_to_shared",
            "generic utility code should be assessed for a neutral namespace",
        )
    if top == "algorithms":
        return (
            "migrate_to_modeling",
            "candidate algorithms and offline comparison belong to the modeling namespace",
        )
    if top == "core" and (
        leaf in SHADOW_CORE_NAMES
        or "runner" in parts
        or "workflow_steps" in parts
        or leaf.startswith("run001_")
        or leaf.startswith("real_com_")
    ):
        return (
            "delete_after_extraction",
            "parallel execution/probe code must yield any unique safety contract and then be removed",
        )
    if top in {"entry", "domain", "config", "calibration"}:
        return (
            "delete_after_extraction",
            "V2 runtime scaffolding must not survive as a parallel final-product architecture",
        )
    if top == "sim":
        return (
            "migrate_to_validation",
            "simulation, replay, parity, and resilience are V1.5 validation capabilities",
        )
    if top == "scripts":
        if any(token in leaf for token in PROBE_OR_CUTOVER_TOKENS):
            return (
                "delete_after_extraction",
                "V2 probe/cutover CLI is obsolete after V1.5 becomes the sole product",
            )
        return (
            "migrate_to_validation",
            "safe offline and suite CLIs belong to the validation/tools surface",
        )
    if top in {"ui_v2", "analytics", "qc", "export", "intelligence", "adapters"}:
        return (
            "migrate_to_v1_5",
            "useful product, review, analytics, or presentation capability belongs to V1.5",
        )
    if top == "core":
        return (
            "migrate_to_v1_5",
            "review unique governance/evidence behavior for migration; do not retain a V2 core",
        )
    if top == "exceptions":
        return (
            "migrate_to_v1_5",
            "product-facing support code belongs to the final V1.5 namespace",
        )
    return (
        "delete_after_extraction",
        "unclassified V2 runtime code requires explicit extraction review and cannot be retained by default",
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
    if relative.startswith("src/gas_calibrator/validation/"):
        return True
    name = path.name
    if relative.startswith("src/gas_calibrator/tools/") and name.startswith("run_v1_5"):
        return True
    return False


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
            files.update(
                path for path in root.rglob("*.py") if "__pycache__" not in path.parts
            )
    return sorted(files)


def build_inventory(repo_root: Path) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    source_root = repo_root / "src"
    v2_root = source_root / "gas_calibrator" / "v2"
    if not v2_root.is_dir():
        raise FileNotFoundError(f"V2 package not found: {v2_root}")

    module_paths = {
        _module_name(path, source_root): path for path in _iter_python_files([v2_root])
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
                if imported_name == V2_PREFIX or imported_name.startswith(
                    f"{V2_PREFIX}."
                ):
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
        implicit_package_initializer = path.name == "__init__.py"
        intentional_external_entrypoint = module in INTENTIONAL_EXTERNAL_ENTRYPOINTS
        explained_static_zero_reference = bool(
            static_zero_reference
            and (implicit_package_initializer or intentional_external_entrypoint)
        )
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
                "target_namespace": _target_namespace(disposition, module),
                "exit_phase": _exit_phase(disposition, module),
                "v2_internal_reference_count": len(module_refs["v2_internal"]),
                "external_source_reference_count": len(module_refs["external_source"]),
                "test_reference_count": len(module_refs["test"]),
                "total_static_reference_count": total_refs,
                "static_zero_reference": static_zero_reference,
                "implicit_package_initializer": implicit_package_initializer,
                "intentional_external_entrypoint": intentional_external_entrypoint,
                "explained_static_zero_reference": explained_static_zero_reference,
                "unexplained_static_zero_reference": bool(
                    static_zero_reference and not explained_static_zero_reference
                ),
                "delete_allowed": disposition == "delete_now" and static_zero_reference,
                "manual_review_required": disposition != "compatibility_wrapper",
            }
        )

    disposition_counts = dict(
        sorted(Counter(row["disposition"] for row in modules).items())
    )
    present_modules = {row["module"] for row in modules}
    final_validation_group_counts = {
        "simulation_config": len(
            present_modules & VALIDATION_CONFIGURATION_MODULES
        ),
        "simulation_runtime": len(
            present_modules & VALIDATION_SIMULATION_RUNTIME_MODULES
        ),
        "engineering_probe": len(
            present_modules & VALIDATION_ENGINEERING_PROBE_MODULES
        ),
        "metrology": len(present_modules & VALIDATION_METROLOGY_MODULES),
    }
    delete_after_extraction_count = disposition_counts.get(
        "delete_after_extraction", 0
    )
    deleted_page_shells = sorted(EXTRACTED_PAGE_SHELLS - known_modules)
    extracted_page_shell_audit: list[dict[str, Any]] = []
    for module in sorted(EXTRACTED_PAGE_SHELLS):
        if module not in known_modules:
            continue
        module_refs = references[module]
        runtime_callers = sorted(
            {
                *module_refs["v2_internal"],
                *module_refs["external_source"],
            }
        )
        test_callers = sorted(module_refs["test"])
        blockers: list[str] = []
        if runtime_callers:
            blockers.append("runtime_callers_present")
        if test_callers:
            blockers.append("tests_still_reference_page_shell")
        extracted_page_shell_audit.append(
            {
                "module": module,
                "runtime_callers": runtime_callers,
                "test_callers": test_callers,
                "delete_ready": not blockers,
                "blockers": blockers,
            }
        )
    summary = {
        "module_count": len(modules),
        "disposition_counts": disposition_counts,
        "static_zero_reference_count": sum(
            row["static_zero_reference"] for row in modules
        ),
        "explained_static_zero_reference_count": sum(
            row["explained_static_zero_reference"] for row in modules
        ),
        "unexplained_static_zero_reference_count": sum(
            row["unexplained_static_zero_reference"] for row in modules
        ),
        "intentional_external_entrypoint_count": sum(
            row["intentional_external_entrypoint"] for row in modules
        ),
        "delete_now_count": disposition_counts.get("delete_now", 0),
        "delete_after_extraction_count": delete_after_extraction_count,
        "direct_deletion_phase_status": (
            "stopped_no_reviewed_delete_candidates"
            if delete_after_extraction_count == 0
            else "manual_review_required"
        ),
        "final_validation_migration_count": sum(
            final_validation_group_counts.values()
        ),
        "final_validation_group_counts": final_validation_group_counts,
        "migration_candidate_count": sum(
            count
            for name, count in disposition_counts.items()
            if name.startswith("migrate_to_")
        ),
        "compatibility_wrapper_count": disposition_counts.get(
            "compatibility_wrapper", 0
        ),
        "v1_5_protected_import_violation_count": len(protected_import_violations),
        "extracted_page_shell_expected_count": len(EXTRACTED_PAGE_SHELLS),
        "extracted_page_shell_count": len(extracted_page_shell_audit),
        "extracted_page_shell_deleted_count": len(deleted_page_shells),
        "extracted_page_shell_delete_ready_count": sum(
            row["delete_ready"] for row in extracted_page_shell_audit
        ),
        "automatic_deletion_permitted": False,
    }
    return {
        "schema": SCHEMA,
        "generated_at": datetime.now().astimezone().isoformat(),
        "repo_root": str(repo_root),
        "policy": {
            "production_core": "native V1.5 mature 0613/0620/0621 route",
            "final_product": "V1.5 is the only final product and future roadmap",
            "v1_role": "frozen production fallback and historical behavior baseline",
            "v2_role": "temporary migration and deletion pool; never a product line",
            "step3a_r0_role": "temporary query-only engineering probe exception; never acceptance or product runtime",
            "default_entry_change_allowed": False,
            "automatic_deletion_permitted": False,
        },
        "summary": summary,
        "v1_5_protected_import_violations": protected_import_violations,
        "extracted_page_shell_deleted": deleted_page_shells,
        "extracted_page_shell_audit": extracted_page_shell_audit,
        "modules": modules,
    }


def _write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    fieldnames = (
        "module",
        "path",
        "disposition",
        "disposition_reason",
        "target_namespace",
        "exit_phase",
        "v2_internal_reference_count",
        "external_source_reference_count",
        "test_reference_count",
        "total_static_reference_count",
        "static_zero_reference",
        "implicit_package_initializer",
        "intentional_external_entrypoint",
        "explained_static_zero_reference",
        "unexplained_static_zero_reference",
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
                f"- Explained static zero-reference modules: `{summary['explained_static_zero_reference_count']}`",
                f"- Unexplained static zero-reference modules: `{summary['unexplained_static_zero_reference_count']}`",
                f"- Intentional external entrypoints: `{summary['intentional_external_entrypoint_count']}`",
                f"- V1.5 protected import violations: `{summary['v1_5_protected_import_violation_count']}`",
                f"- Migration candidates: `{summary['migration_candidate_count']}`",
                f"- Delete after extraction: `{summary['delete_after_extraction_count']}`",
                f"- Delete now: `{summary['delete_now_count']}`",
                f"- Direct deletion phase: `{summary['direct_deletion_phase_status']}`",
                f"- Final validation migration closure: `{summary['final_validation_migration_count']}`",
                f"- Compatibility wrappers: `{summary['compatibility_wrapper_count']}`",
                f"- Extracted page shells deleted: `{summary['extracted_page_shell_deleted_count']}` / `{summary['extracted_page_shell_expected_count']}`",
                f"- Automatic deletion permitted: `{summary['automatic_deletion_permitted']}`",
                "",
                "## Dispositions",
                "",
                *[
                    f"- `{name}`: {count}"
                    for name, count in summary["disposition_counts"].items()
                ],
                "",
                "## Final Validation Ownership Stop Boundary",
                "",
                (
                    "- Simulation config: "
                    f"`{summary['final_validation_group_counts']['simulation_config']}` -> "
                    "`gas_calibrator.validation.simulation.config`; "
                    "the external sidecar is decoupled, inert AlgorithmConfig, obsolete V2 storage schema field, obsolete entry config helpers, unused CO2GroupConfig/group_a/group_b/valve_mapping mirrors, V2-only order/signal_keys mirrors, and V2-only gas-level summary temperature/pressure mirrors are retired; raw V1 AMT and gas-level summary-column contracts remain production-owned, the active offline ratio-poly coefficient/report configuration subclosure is validation-owned with the shared dewpoint-backed dry-anchor gate for H2O, the active nine-field valve snapshot subclosure, pure run-mode alias normalization, pure sensor-precheck normalization, and pure Step 2 port/engineering-flag risk-inventory, classification/badge/inventory-detail presentation, and hydrate/review/governance-handoff leaves are validation-owned through the identity-compatible V2 AppConfig facade while the raw valve-route dictionary and physical mapping remain frozen, simulation storage is fail-closed, adjacent storage requires explicit enablement, and the common storage contract is shared-owned with a database-only schema/async extension, "
                    "and the stability, sampling, precheck, pressure-tolerance, quality-control, optional-AI, offline-path, and runtime-feature config subclosures are validation-owned; stop until closure-wide "
                    "parity/nightly migration and zero V2 callers."
                ),
                (
                    "- Simulation runtime: "
                    f"`{summary['final_validation_group_counts']['simulation_runtime']}` -> "
                    "`gas_calibrator.validation.simulation`; "
                    "stop until closure-wide parity/nightly migration and zero V2 callers."
                ),
                (
                    "- Engineering probe: "
                    f"`{summary['final_validation_group_counts']['engineering_probe']}` -> "
                    "`gas_calibrator.validation.engineering_probe`; "
                    "stop until Step 3A closure or equivalent no-write validation migration."
                ),
                (
                    "- Metrology contracts: "
                    f"`{summary['final_validation_group_counts']['metrology']}` -> "
                    "`gas_calibrator.validation.metrology`; "
                    "stop until scientific review and nightly contract migration."
                ),
                "- No module in these groups is directly deletable; automatic deletion remains prohibited.",
                "",
                "## Extracted Page Shell Readiness",
                "",
                *[
                    f"- `{module}`: deleted=`True`"
                    for module in payload["extracted_page_shell_deleted"]
                ],
                *[
                    (
                        f"- `{row['module']}`: delete_ready=`{row['delete_ready']}`; "
                        f"runtime_callers=`{len(row['runtime_callers'])}`; "
                        f"test_callers=`{len(row['test_callers'])}`; "
                        f"blockers=`{','.join(row['blockers']) or 'none'}`"
                    )
                    for row in payload["extracted_page_shell_audit"]
                ],
                "",
                "## Boundary",
                "",
                "- V1.5 is the only final product and future roadmap.",
                "- V1 remains a frozen fallback and historical behavior baseline.",
                "- V2 is a temporary migration/deletion pool, never a product platform.",
                "- Step 3A R0 remains a temporary dual-unlock query-only engineering exception, not product runtime or real acceptance.",
                "- Static zero reference is only one deletion signal; references, tests, entries, configs, artifacts, and replacement coverage still require review.",
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
