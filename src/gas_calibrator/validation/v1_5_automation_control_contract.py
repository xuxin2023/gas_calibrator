"""Offline V1.5 automation control contract.

This contract keeps V1.5 automation as an orchestration shell around the
0613/0620/0621 mature core. It does not open COM ports, control routes, connect
to PostgreSQL, or write analyzer coefficients.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable


SCHEMA = "v1_5_automation_control_contract_v1"

MATURE_FITTING_BASELINE = "0613-style V1.5 fitting method"
MATURE_PHYSICAL_BASELINE = "0620/0621 mature physical execution path"

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

CANONICAL_AUTOMATION_STAGES = (
    "01_initialization_identity_runtime_closeout",
    "02_pre_gas_readiness_and_pressure_s9",
    "03_mature_legacy_co2_45_route",
    "04_mature_legacy_h2o_13_route",
    "05_no_write_fit_strategy_review",
    "06_controlled_write_with_readback",
    "07_short_reverify",
    "08_archive_and_database_dry_run",
)


@dataclass(frozen=True)
class AutomationContractCheck:
    check_id: str
    status: str
    severity: str
    topic: str
    requirement: str
    forbidden_failure_mode: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _checks() -> list[AutomationContractCheck]:
    return [
        AutomationContractCheck(
            check_id="AUTO-CORE-001",
            status="pass",
            severity="blocker",
            topic="mature_core",
            requirement=(
                "V1.5 automation must call the 0613 fitting baseline and the "
                "0620/0621 mature physical execution path; automation is a shell, not a rewritten route kernel."
            ),
            forbidden_failure_mode="Do not patch migrated/root/0624 route logic until it appears equivalent.",
        ),
        AutomationContractCheck(
            check_id="AUTO-CORE-002",
            status="pass",
            severity="blocker",
            topic="protected_files",
            requirement="This contract package must not modify protected mature route, protocol, default, or app-entry files.",
            forbidden_failure_mode="Do not mix automation documentation with live runner/protocol/default_config edits.",
        ),
        AutomationContractCheck(
            check_id="AUTO-ENTRY-001",
            status="pass",
            severity="blocker",
            topic="entrypoints",
            requirement=(
                "Production launchers are initialization, pre-gas readiness, mature CO2/H2O queues, "
                "no-write fit review, controlled write, reverify, archive, and database dry-run/import gates."
            ),
            forbidden_failure_mode="Do not start production from diagnostic, replay, root migration, 0624, V1/V2, or _handoff scripts.",
        ),
        AutomationContractCheck(
            check_id="AUTO-ROUTE-001",
            status="pass",
            severity="blocker",
            topic="legacy_route",
            requirement="Legacy production remains CO2 45 points and H2O 13 wet points unless a separate reviewed profile says otherwise.",
            forbidden_failure_mode="Do not accidentally run the new-algorithm 47/14 plan for legacy analyzers.",
        ),
        AutomationContractCheck(
            check_id="AUTO-ROUTE-002",
            status="pass",
            severity="blocker",
            topic="point_quality",
            requirement=(
                "Analyzer-local ratio instability downgrades that analyzer/point quality; public physical gates "
                "such as pressure, route, dewpoint, and source failure are the point-level blockers."
            ),
            forbidden_failure_mode="Do not fail a whole point only because one analyzer misses the ratio threshold.",
        ),
        AutomationContractCheck(
            check_id="AUTO-PRESS-001",
            status="pass",
            severity="blocker",
            topic="pressure",
            requirement="Pressure uses PACE INL absolute pressure evidence before CO2/H2O route execution.",
            forbidden_failure_mode="Do not use the wrong PACE pressure query as S9 readiness evidence.",
        ),
        AutomationContractCheck(
            check_id="AUTO-FIT-001",
            status="pass",
            severity="blocker",
            topic="co2_fit",
            requirement=(
                "CO2 fitting uses S1/S3 as the main model and S5 as a final linear layer; "
                "S5 writes must account for current GETCO5 state and prefer CLEARSENCO5,YGAS,FFF before writing."
            ),
            forbidden_failure_mode="Do not treat S5 as a naive overwrite or skip live GETCO5 composition.",
        ),
        AutomationContractCheck(
            check_id="AUTO-FIT-002",
            status="pass",
            severity="blocker",
            topic="h2o_fit",
            requirement="H2O fitting uses S2/S4 as the main model and S6 as a separate final linear layer.",
            forbidden_failure_mode="Do not mix S6 from one fit strategy with incompatible S2/S4 coefficients.",
        ),
        AutomationContractCheck(
            check_id="AUTO-FIT-003",
            status="pass",
            severity="blocker",
            topic="anchors",
            requirement="Keep CO2 zero gas and H2O dry-gas or low-water anchor physically separate.",
            forbidden_failure_mode="Do not collapse CO2 zero gas into an unconditional H2O zero anchor.",
        ),
        AutomationContractCheck(
            check_id="AUTO-EVID-001",
            status="pass",
            severity="blocker",
            topic="evidence_state",
            requirement="Separate real_pass, no_write_candidate, diagnostic_only, review_required, superseded, and rejected evidence states.",
            forbidden_failure_mode="Do not use no-write math, smoke tests, or diagnostic points as production fit evidence by default.",
        ),
        AutomationContractCheck(
            check_id="AUTO-DB-001",
            status="pass",
            severity="blocker",
            topic="database",
            requirement="PostgreSQL 18 import remains after archive/release gates and SN/device_code traceability are closed.",
            forbidden_failure_mode="Do not import calibration data before final archive, readback, and reverify evidence are complete.",
        ),
        AutomationContractCheck(
            check_id="AUTO-LIVE-001",
            status="pass",
            severity="blocker",
            topic="live_actions",
            requirement="This contract is offline only and does not execute COM, route, pressure, database, or coefficient writes.",
            forbidden_failure_mode="Do not attach a live executor to this contract package.",
        ),
    ]


def build_v1_5_automation_control_contract() -> dict[str, Any]:
    checks = _checks()
    blocker_count = sum(1 for check in checks if check.status == "blocker")
    return {
        "schema": SCHEMA,
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "manifest": {
            "status": "pass" if blocker_count == 0 else "blocked",
            "blocker_count": blocker_count,
            "mature_fitting_baseline": MATURE_FITTING_BASELINE,
            "mature_physical_baseline": MATURE_PHYSICAL_BASELINE,
            "automation_model": "mature_core_with_automation_shell",
            "legacy_co2_point_count": 45,
            "legacy_h2o_wet_point_count": 13,
            "new_algorithm_profile_point_count": {"co2": 47, "h2o": 14},
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "connects_postgresql": False,
            "writes_coefficients": False,
            "writes_sn_or_device_code": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
            "protected_core_files": list(PROTECTED_CORE_FILES),
            "canonical_automation_stages": list(CANONICAL_AUTOMATION_STAGES),
        },
        "checks": [check.to_json() for check in checks],
    }


def _markdown(model: dict[str, Any]) -> str:
    manifest = model["manifest"]
    lines = [
        "# V1.5 Automation Control Contract",
        "",
        f"- schema: `{model['schema']}`",
        f"- status: `{manifest['status']}`",
        f"- blocker_count: `{manifest['blocker_count']}`",
        f"- mature_fitting_baseline: `{manifest['mature_fitting_baseline']}`",
        f"- mature_physical_baseline: `{manifest['mature_physical_baseline']}`",
        f"- automation_model: `{manifest['automation_model']}`",
        "",
        "## Principle",
        "",
        "V1.5 automation is an orchestration shell around the mature core. It may prepare inputs, enforce gates, collect evidence, and call reviewed entrypoints, but it must not reimplement the 0613 fitting method or the 0620/0621 physical CO2/H2O route kernel.",
        "",
        "## Canonical Automation Stages",
        "",
    ]
    lines.extend(f"{index}. `{stage}`" for index, stage in enumerate(manifest["canonical_automation_stages"], start=1))
    lines.extend(
        [
            "",
            "## Protected Core Files",
            "",
        ]
    )
    lines.extend(f"- `{path}`" for path in manifest["protected_core_files"])
    lines.extend(
        [
            "",
            "## Checks",
            "",
            "| check_id | status | severity | topic | requirement |",
            "|---|---:|---|---|---|",
        ]
    )
    for check in model["checks"]:
        requirement = check["requirement"].replace("|", "/")
        lines.append(
            f"| `{check['check_id']}` | `{check['status']}` | `{check['severity']}` | `{check['topic']}` | {requirement} |"
        )
    lines.extend(
        [
            "",
            "## Non-Execution Boundary",
            "",
            "- opens_com_ports: `false`",
            "- controls_water_or_gas_routes: `false`",
            "- connects_postgresql: `false`",
            "- writes_coefficients: `false`",
            "- writes_sn_or_device_code: `false`",
            "- formal_release_allowed: `false`",
            "- database_import_allowed: `false`",
            "- not_real_acceptance_evidence: `true`",
            "",
        ]
    )
    return "\n".join(lines)


def write_v1_5_automation_control_contract(*, output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    model = build_v1_5_automation_control_contract()
    paths = {
        "manifest": out / "v1_5_automation_control_contract.json",
        "checks": out / "v1_5_automation_control_contract_checks.csv",
        "markdown": out / "V1_5_AUTOMATION_CONTROL_CONTRACT.md",
    }
    paths["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    with paths["checks"].open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("check_id", "status", "severity", "topic", "requirement", "forbidden_failure_mode"),
        )
        writer.writeheader()
        writer.writerows(model["checks"])
    paths["markdown"].write_text(_markdown(model), encoding="utf-8")
    return paths


__all__ = [
    "CANONICAL_AUTOMATION_STAGES",
    "MATURE_FITTING_BASELINE",
    "MATURE_PHYSICAL_BASELINE",
    "PROTECTED_CORE_FILES",
    "SCHEMA",
    "build_v1_5_automation_control_contract",
    "write_v1_5_automation_control_contract",
]
