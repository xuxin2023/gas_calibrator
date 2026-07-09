"""Offline V1.5 production entrypoint map.

This map is a navigation and review contract. It identifies the entrypoints
that may start production work, the tools that are only support/worker surfaces,
and the surfaces that must not be used as production launchers. It does not
open COM ports, control routes, connect to PostgreSQL, or write analyzer state.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


SCHEMA = "v1_5_production_entrypoint_map_v1"

MATURE_FITTING_BASELINE = "0613-style V1.5 fitting method"
MATURE_PHYSICAL_BASELINE = "0620/0621 mature physical execution path"

LEGACY_CO2_POINT_COUNT = 45
LEGACY_H2O_WET_POINT_COUNT = 13
NEW_ALGORITHM_CO2_POINT_COUNT = 47
NEW_ALGORITHM_H2O_WET_POINT_COUNT = 14

PROTECTED_CORE_FILES = (
    "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py",
    "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py",
    "src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py",
    "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_sampling.py",
    "src/gas_calibrator/workflow/runner.py",
    "src/gas_calibrator/devices/gas_analyzer.py",
    "configs/default_config.json",
    "run_app.py",
)


@dataclass(frozen=True)
class ProductionEntrypointRow:
    entrypoint_id: str
    path: str
    group: str
    production_role: str
    launch_policy: str
    mature_baseline: str
    algorithm_scope: str
    opens_com_when_authorized: bool
    controls_pressure_when_authorized: bool
    controls_routes_when_authorized: bool
    writes_coefficients_when_authorized: bool
    connects_postgresql_when_authorized: bool
    point_contract: str
    required_before_use: str
    forbidden_substitutes: str
    notes: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ForbiddenSurfaceRow:
    surface_id: str
    surface: str
    examples: str
    policy: str
    reason: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EntrypointMapCheck:
    check_id: str
    status: str
    severity: str
    requirement: str
    observed: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _production_entries() -> list[ProductionEntrypointRow]:
    return [
        ProductionEntrypointRow(
            entrypoint_id="initialization_planner",
            path="src/gas_calibrator/tools/run_v1_5_formal_initialization_runner.py",
            group="01_initialization",
            production_role="formal initialization planner and readiness index",
            launch_policy="offline_plan_and_gate_only",
            mature_baseline=MATURE_PHYSICAL_BASELINE,
            algorithm_scope="legacy_and_new_algorithm",
            opens_com_when_authorized=False,
            controls_pressure_when_authorized=False,
            controls_routes_when_authorized=False,
            writes_coefficients_when_authorized=False,
            connects_postgresql_when_authorized=False,
            point_contract="not_a_route_runner",
            required_before_use="Use as the owner of the formal initialization plan; subordinate tools handle read-only COM or controlled writes.",
            forbidden_substitutes="Do not start initialization from ad hoc root scripts, _handoff evidence, or V1/V2 launchers.",
            notes="Owns SN/device_code, protocol ID alias, runtime, GETCO, neutralization, S9, CHECK, and pre-gas evidence contracts.",
        ),
        ProductionEntrypointRow(
            entrypoint_id="readonly_com_identity_getco_closeout",
            path="src/gas_calibrator/tools/run_v1_5_formal_readonly_com_minimal_executor.py",
            group="01_initialization",
            production_role="manual-authorized read-only identity, SN, GETCO, runtime, and CHECK-capable evidence",
            launch_policy="manual_authorized_read_only_com_only",
            mature_baseline=MATURE_PHYSICAL_BASELINE,
            algorithm_scope="legacy_and_new_algorithm",
            opens_com_when_authorized=True,
            controls_pressure_when_authorized=False,
            controls_routes_when_authorized=False,
            writes_coefficients_when_authorized=False,
            connects_postgresql_when_authorized=False,
            point_contract="not_a_route_runner",
            required_before_use="Requires packet validator, reviewed ports, active analyzer list, authorization packet, >=1s pacing, no-write/no-db/no-route confirmation.",
            forbidden_substitutes="Do not use raw serial scripts as production identity evidence.",
            notes="Legacy analyzers must not receive CHECK; new-algorithm/CHECK-capable analyzers may read CHECK only after the read-only plan allows it.",
        ),
        ProductionEntrypointRow(
            entrypoint_id="pressure_s9_no_write",
            path="src/gas_calibrator/tools/validate_pressure_only.py",
            group="02_pressure",
            production_role="pressure/S9 no-write acquisition and review",
            launch_policy="manual_authorized_pressure_no_write",
            mature_baseline=MATURE_PHYSICAL_BASELINE,
            algorithm_scope="legacy_and_new_algorithm",
            opens_com_when_authorized=True,
            controls_pressure_when_authorized=True,
            controls_routes_when_authorized=False,
            writes_coefficients_when_authorized=False,
            connects_postgresql_when_authorized=False,
            point_contract="PACE INL absolute pressure, S9 no-write first",
            required_before_use="Pressure reference and route-independent pressure evidence must be ready; writes remain separate controlled steps.",
            forbidden_substitutes="Do not use the wrong PACE pressure query or CO2/H2O route data as pressure readiness.",
            notes="Linear S9 exceptions must be explicit controlled exceptions with write/readback/reverify evidence.",
        ),
        ProductionEntrypointRow(
            entrypoint_id="co2_mature_legacy_queue",
            path="src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py",
            group="03_co2",
            production_role="mature legacy CO2 open-flow production queue",
            launch_policy="manual_authorized_route_runner",
            mature_baseline=MATURE_PHYSICAL_BASELINE,
            algorithm_scope="legacy_ratio_production",
            opens_com_when_authorized=True,
            controls_pressure_when_authorized=False,
            controls_routes_when_authorized=True,
            writes_coefficients_when_authorized=False,
            connects_postgresql_when_authorized=False,
            point_contract="legacy CO2 45 points",
            required_before_use="Pre-gas readiness, pressure/S9, route readiness, dewpoint/source readiness, and mature 0620/0621 physical conditions must be closed.",
            forbidden_substitutes="Do not use root migration runners, 0624 handoff queues, diagnostic probes, or _handoff scripts.",
            notes="Single-analyzer ratio instability should degrade analyzer/point quality; public physical gates decide point-level failure.",
        ),
        ProductionEntrypointRow(
            entrypoint_id="h2o_mature_legacy_queue",
            path="src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py",
            group="04_h2o",
            production_role="mature legacy H2O open-flow production queue",
            launch_policy="manual_authorized_route_runner",
            mature_baseline=MATURE_PHYSICAL_BASELINE,
            algorithm_scope="legacy_ratio_production",
            opens_com_when_authorized=True,
            controls_pressure_when_authorized=False,
            controls_routes_when_authorized=True,
            writes_coefficients_when_authorized=False,
            connects_postgresql_when_authorized=False,
            point_contract="legacy H2O 13 wet points",
            required_before_use="CO2 route evidence, pressure readiness, humidity-generator readiness, dewpoint/reference evidence, and mature 0620/0621 route behavior must be closed.",
            forbidden_substitutes="Do not use 0624 handoff H2O queue or migrated root queue as the mature baseline.",
            notes="CO2 zero gas and H2O dry/low-water anchor evidence remain physically separate.",
        ),
        ProductionEntrypointRow(
            entrypoint_id="candidate_fit_review",
            path="src/gas_calibrator/tools/export_v1_5_candidate_coefficients.py",
            group="05_fitting",
            production_role="offline candidate coefficient calculation and fit review",
            launch_policy="offline_no_write_review",
            mature_baseline=MATURE_FITTING_BASELINE,
            algorithm_scope="legacy_and_new_algorithm_profile_gated",
            opens_com_when_authorized=False,
            controls_pressure_when_authorized=False,
            controls_routes_when_authorized=False,
            writes_coefficients_when_authorized=False,
            connects_postgresql_when_authorized=False,
            point_contract="uses eligible production evidence only",
            required_before_use="Use 0613-style fitting and keep rejected/superseded/diagnostic evidence out of formal fit eligibility unless explicitly reviewed.",
            forbidden_substitutes="Do not use V1 old fitting shortcuts, smoke points, or diagnostic rows as production fit data.",
            notes="S1/S3 and S2/S4 are main chains; S5/S6 are final linear layers handled as separate review/write decisions.",
        ),
        ProductionEntrypointRow(
            entrypoint_id="controlled_coefficient_writes",
            path="src/gas_calibrator/tools/run_v1_5_co2_senco13_controlled_write.py",
            group="06_controlled_write",
            production_role="representative controlled coefficient write surface",
            launch_policy="manual_authorized_controlled_write_only",
            mature_baseline=MATURE_FITTING_BASELINE,
            algorithm_scope="legacy_and_new_algorithm_profile_gated",
            opens_com_when_authorized=True,
            controls_pressure_when_authorized=False,
            controls_routes_when_authorized=False,
            writes_coefficients_when_authorized=True,
            connects_postgresql_when_authorized=False,
            point_contract="write/readback/reverify required",
            required_before_use="Requires old GETCO snapshot, candidate review, explicit authorization, exact payload formatting, readback, and short reverify.",
            forbidden_substitutes="Do not run controlled-write tools as background evidence, replay, or report steps.",
            notes="SENCO5 writes must compose with current GETCO5 and prefer CLEARSENCO5,YGAS,FFF before writing the final layer.",
        ),
        ProductionEntrypointRow(
            entrypoint_id="archive_and_database_locked_chain",
            path="src/gas_calibrator/tools/export_v1_5_formal_run_status.py",
            group="07_archive_database",
            production_role="offline release/import status rollup",
            launch_policy="offline_status_only",
            mature_baseline="archive/release gate chain",
            algorithm_scope="legacy_and_new_algorithm",
            opens_com_when_authorized=False,
            controls_pressure_when_authorized=False,
            controls_routes_when_authorized=False,
            writes_coefficients_when_authorized=False,
            connects_postgresql_when_authorized=False,
            point_contract="not_a_route_runner",
            required_before_use="Archive closure, SN traceability, readback, reverify, PostgreSQL 18 dry-run/import gates, and reviewer authorization remain separate gates.",
            forbidden_substitutes="Do not treat status rollups, dry-run database contracts, or no-write math as real release/import evidence.",
            notes="This map does not allow release or database import.",
        ),
    ]


def _forbidden_surfaces() -> list[ForbiddenSurfaceRow]:
    return [
        ForbiddenSurfaceRow(
            surface_id="handoff_evidence",
            surface="_handoff",
            examples="_handoff/*, ad hoc run folders, copied evidence bundles",
            policy="not_a_production_launcher",
            reason="Evidence and scratch artifacts can explain a decision, but must not be used as executable production entrypoints.",
        ),
        ForbiddenSurfaceRow(
            surface_id="root_migration_area",
            surface="D:/gas_calibrator root migration drafts",
            examples="root-level migrated runners, temporary queue experiments, 0624 handoff logic",
            policy="not_a_mature_baseline",
            reason="The current production baseline is the 0613 fitting method plus 0620/0621 mature physical path, not later migration drafts.",
        ),
        ForbiddenSurfaceRow(
            surface_id="diagnostic_probes",
            surface="diagnostic/probe/tune scripts",
            examples="dynamic pressure diagnostics, no-OUTP probes, sealed-pressure tuning, extended hold experiments",
            policy="diagnostic_only",
            reason="Diagnostic rows may explain physics, but are not production fit or release evidence unless separately reviewed.",
        ),
        ForbiddenSurfaceRow(
            surface_id="sampling_workers",
            surface="per-point sampling workers",
            examples="run_v1_5_formal_open_flow_sampling.py, run_v1_5_formal_h2o_open_flow_sampling.py",
            policy="worker_not_top_level",
            reason="Workers must be called by canonical CO2/H2O queue runners so point order, route setup, and evidence indexing stay coherent.",
        ),
        ForbiddenSurfaceRow(
            surface_id="legacy_v1_or_v2_surfaces",
            surface="V1/V2 launchers and references",
            examples="run_v1_*, V2 device workbench, V2 engineering probes",
            policy="not_v1_5_production_entry",
            reason="V1 is historical/fallback reference and V2 remains outside V1.5 production entrypoint control.",
        ),
    ]


def _checks(entries: list[ProductionEntrypointRow], forbidden: list[ForbiddenSurfaceRow]) -> list[EntrypointMapCheck]:
    entries_by_id = {row.entrypoint_id: row for row in entries}
    forbidden_by_id = {row.surface_id: row for row in forbidden}
    return [
        EntrypointMapCheck(
            check_id="ENTRY-MAP-001",
            status="pass" if entries_by_id["co2_mature_legacy_queue"].point_contract == "legacy CO2 45 points" else "blocker",
            severity="blocker",
            requirement="Legacy CO2 production entry must remain the mature 45-point queue.",
            observed=entries_by_id["co2_mature_legacy_queue"].point_contract,
        ),
        EntrypointMapCheck(
            check_id="ENTRY-MAP-002",
            status="pass" if entries_by_id["h2o_mature_legacy_queue"].point_contract == "legacy H2O 13 wet points" else "blocker",
            severity="blocker",
            requirement="Legacy H2O production entry must remain the mature 13 wet-point queue.",
            observed=entries_by_id["h2o_mature_legacy_queue"].point_contract,
        ),
        EntrypointMapCheck(
            check_id="ENTRY-MAP-003",
            status="pass" if forbidden_by_id["sampling_workers"].policy == "worker_not_top_level" else "blocker",
            severity="blocker",
            requirement="Sampling workers must not be top-level production launchers.",
            observed=forbidden_by_id["sampling_workers"].policy,
        ),
        EntrypointMapCheck(
            check_id="ENTRY-MAP-004",
            status="pass" if forbidden_by_id["root_migration_area"].policy == "not_a_mature_baseline" else "blocker",
            severity="blocker",
            requirement="Root migration and 0624 handoff areas must not be treated as mature V1.5 baseline.",
            observed=forbidden_by_id["root_migration_area"].policy,
        ),
        EntrypointMapCheck(
            check_id="ENTRY-MAP-005",
            status="pass" if entries_by_id["controlled_coefficient_writes"].writes_coefficients_when_authorized else "blocker",
            severity="blocker",
            requirement="Coefficient writes are only manual-authorized controlled-write surfaces with readback/reverify.",
            observed=entries_by_id["controlled_coefficient_writes"].launch_policy,
        ),
        EntrypointMapCheck(
            check_id="ENTRY-MAP-006",
            status="pass",
            severity="blocker",
            requirement="This entrypoint map is offline documentation and must not grant release, import, COM, route, pressure, or write actions.",
            observed="offline_map_only",
        ),
    ]


def build_v1_5_production_entrypoint_map() -> dict[str, Any]:
    entries = _production_entries()
    forbidden = _forbidden_surfaces()
    checks = _checks(entries, forbidden)
    blocker_count = sum(1 for row in checks if row.status == "blocker")
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "manifest": {
            "status": "pass" if blocker_count == 0 else "blocked",
            "blocker_count": blocker_count,
            "mature_fitting_baseline": MATURE_FITTING_BASELINE,
            "mature_physical_baseline": MATURE_PHYSICAL_BASELINE,
            "legacy_point_contract": {
                "co2": LEGACY_CO2_POINT_COUNT,
                "h2o_wet": LEGACY_H2O_WET_POINT_COUNT,
            },
            "new_algorithm_profile_contract": {
                "co2": NEW_ALGORITHM_CO2_POINT_COUNT,
                "h2o_wet": NEW_ALGORITHM_H2O_WET_POINT_COUNT,
            },
            "production_entry_count": len(entries),
            "forbidden_surface_count": len(forbidden),
            "opens_com_ports": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "connects_postgresql": False,
            "writes_coefficients": False,
            "writes_sn_or_device_code": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
            "protected_core_files": list(PROTECTED_CORE_FILES),
        },
        "production_entrypoints": [row.to_json() for row in entries],
        "forbidden_surfaces": [row.to_json() for row in forbidden],
        "checks": [row.to_json() for row in checks],
    }


def _markdown(model: dict[str, Any]) -> str:
    manifest = model["manifest"]
    lines = [
        "# V1.5 Production Entrypoint Map",
        "",
        f"- schema: `{model['schema']}`",
        f"- status: `{manifest['status']}`",
        f"- blocker_count: `{manifest['blocker_count']}`",
        f"- mature_fitting_baseline: `{manifest['mature_fitting_baseline']}`",
        f"- mature_physical_baseline: `{manifest['mature_physical_baseline']}`",
        f"- legacy CO2 points: `{manifest['legacy_point_contract']['co2']}`",
        f"- legacy H2O wet points: `{manifest['legacy_point_contract']['h2o_wet']}`",
        f"- new algorithm CO2 points: `{manifest['new_algorithm_profile_contract']['co2']}`",
        f"- new algorithm H2O wet points: `{manifest['new_algorithm_profile_contract']['h2o_wet']}`",
        "",
        "## Production Entrypoints",
        "",
        "| id | group | path | launch policy | point contract |",
        "|---|---|---|---|---|",
    ]
    for row in model["production_entrypoints"]:
        lines.append(
            f"| `{row['entrypoint_id']}` | `{row['group']}` | `{row['path']}` | `{row['launch_policy']}` | `{row['point_contract']}` |"
        )
    lines.extend(
        [
            "",
            "## Forbidden As Production Launchers",
            "",
            "| surface | policy | examples | reason |",
            "|---|---|---|---|",
        ]
    )
    for row in model["forbidden_surfaces"]:
        lines.append(f"| `{row['surface_id']}` | `{row['policy']}` | {row['examples']} | {row['reason']} |")
    lines.extend(
        [
            "",
            "## Checks",
            "",
            "| check_id | status | severity | requirement |",
            "|---|---|---|---|",
        ]
    )
    for row in model["checks"]:
        lines.append(f"| `{row['check_id']}` | `{row['status']}` | `{row['severity']}` | {row['requirement']} |")
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
            "This map is a review artifact. It does not replace authorization packets, pressure readiness, route readiness, controlled writes, reverify, archive closure, or PostgreSQL import gates.",
            "",
        ]
    )
    return "\n".join(lines)


def write_v1_5_production_entrypoint_map(*, output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    model = build_v1_5_production_entrypoint_map()
    paths = {
        "manifest": out / "v1_5_production_entrypoint_map.json",
        "entrypoints": out / "v1_5_production_entrypoint_map_entrypoints.csv",
        "forbidden": out / "v1_5_production_entrypoint_map_forbidden_surfaces.csv",
        "checks": out / "v1_5_production_entrypoint_map_checks.csv",
        "markdown": out / "V1_5_PRODUCTION_ENTRYPOINT_MAP.md",
    }
    paths["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    with paths["entrypoints"].open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=tuple(ProductionEntrypointRow.__dataclass_fields__.keys()))
        writer.writeheader()
        writer.writerows(model["production_entrypoints"])
    with paths["forbidden"].open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=tuple(ForbiddenSurfaceRow.__dataclass_fields__.keys()))
        writer.writeheader()
        writer.writerows(model["forbidden_surfaces"])
    with paths["checks"].open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=tuple(EntrypointMapCheck.__dataclass_fields__.keys()))
        writer.writeheader()
        writer.writerows(model["checks"])
    paths["markdown"].write_text(_markdown(model), encoding="utf-8")
    return paths


__all__ = [
    "LEGACY_CO2_POINT_COUNT",
    "LEGACY_H2O_WET_POINT_COUNT",
    "MATURE_FITTING_BASELINE",
    "MATURE_PHYSICAL_BASELINE",
    "NEW_ALGORITHM_CO2_POINT_COUNT",
    "NEW_ALGORITHM_H2O_WET_POINT_COUNT",
    "PROTECTED_CORE_FILES",
    "SCHEMA",
    "build_v1_5_production_entrypoint_map",
    "write_v1_5_production_entrypoint_map",
]
