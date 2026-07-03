"""Offline guard for the mature V1.5 CO2/H2O route contract.

This validator freezes the production route shape that should not change when
algorithm profiles, reports, replay tools, or write contracts evolve. It reads
the V1.5 algorithm route profile and inventory constants only; it does not open
COM ports, control routes, connect to PostgreSQL, or write coefficients.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_entrypoint_inventory import CANONICAL_FORMAL_PATH, NON_START_HERE_GUARDRAILS


SCHEMA = "v1_5_mature_route_contract_v1"

EXPECTED_CO2_RUNNER = "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue"
EXPECTED_H2O_RUNNER = "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue"
EXPECTED_CO2_WORKER_PATH = "src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py"
EXPECTED_H2O_WORKER_PATH = "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_sampling.py"
EXPECTED_ROUTE_BEHAVIOR = "preserve_mature_v1_5_0620_route_timing_and_quality_gates"

EXPECTED_LEGACY_CO2_PLAN: dict[str, list[int]] = {
    "-20": [0, 400, 1000],
    "-10": [0, 400, 1000],
    "0": [0, 400, 1000],
    "10": [0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
    "20": [0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
    "30": [0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
    "40": [0, 400, 1000],
}

EXPECTED_LEGACY_H2O_PLAN: dict[str, list[str]] = {
    "0": ["HGEN0C_50RH"],
    "10": ["HGEN10C_30RH", "HGEN10C_50RH", "HGEN10C_70RH"],
    "20": ["HGEN20C_30RH", "HGEN20C_50RH", "HGEN20C_70RH"],
    "30": ["HGEN20C_30RH", "HGEN20C_50RH", "HGEN20C_70RH", "HGEN20C_90RH"],
    "40": ["HGEN30C_50RH", "HGEN30C_70RH"],
}


@dataclass(frozen=True)
class MatureRouteCheck:
    check_id: str
    title: str
    status: str
    reason: str
    expected: str
    observed: str
    physical_meaning: str
    blocks_mature_route_release: bool

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _load_profile(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError("V1.5 algorithm route profile must be a JSON object")
    return payload


def _profiles_by_id(config: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        str(profile.get("profile_id") or ""): profile
        for profile in config.get("profiles", [])
        if isinstance(profile, Mapping)
    }


def _point_count(plan: Mapping[str, Sequence[Any]]) -> int:
    return sum(len(values or []) for values in plan.values())


def _same_plan(observed: Mapping[str, Any], expected: Mapping[str, Sequence[Any]]) -> bool:
    normalized_observed = {str(key): list(values or []) for key, values in observed.items()}
    normalized_expected = {str(key): list(values or []) for key, values in expected.items()}
    return normalized_observed == normalized_expected


def _fmt(value: Any) -> str:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _check(
    *,
    check_id: str,
    title: str,
    passed: bool,
    reason: str,
    expected: Any,
    observed: Any,
    physical_meaning: str,
) -> MatureRouteCheck:
    return MatureRouteCheck(
        check_id=check_id,
        title=title,
        status="pass" if passed else "blocker",
        reason=reason,
        expected=_fmt(expected),
        observed=_fmt(observed),
        physical_meaning=physical_meaning,
        blocks_mature_route_release=not passed,
    )


def _canonical_stage_entry(stage: str) -> str:
    for item in CANONICAL_FORMAL_PATH:
        if item.get("stage") == stage:
            return str(item.get("entrypoint") or "")
    return ""


def build_v1_5_mature_route_contract(*, profile_path: str | Path) -> dict[str, Any]:
    """Return the offline mature-route guard model."""

    profile_file = Path(profile_path).resolve()
    config = _load_profile(profile_file)
    profiles = _profiles_by_id(config)
    shared = config.get("shared_route_contract", {})
    legacy = profiles.get("legacy_ratio_production", {})
    absorption = profiles.get("absorption_ratio_shadow", {})

    legacy_co2 = legacy.get("co2_route", {}) if isinstance(legacy, Mapping) else {}
    legacy_h2o = legacy.get("h2o_route", {}) if isinstance(legacy, Mapping) else {}
    absorption_co2 = absorption.get("co2_route", {}) if isinstance(absorption, Mapping) else {}
    absorption_h2o = absorption.get("h2o_route", {}) if isinstance(absorption, Mapping) else {}

    legacy_co2_plan = legacy_co2.get("temperature_plan", {})
    legacy_h2o_plan = legacy_h2o.get("temperature_plan", {})
    absorption_co2_policy = absorption_co2.get("supplement_policy", {})
    absorption_h2o_policy = absorption_h2o.get("supplement_policy", {})
    r0_contract = absorption.get("r0_write_contract", {}) if isinstance(absorption, Mapping) else {}

    checks = [
        _check(
            check_id="shared_route_behavior_0620",
            title="Shared route behavior preserves mature 0620 gates",
            passed=shared.get("route_behavior") == EXPECTED_ROUTE_BEHAVIOR,
            reason="route_behavior must stay pinned to the mature V1.5 0620 timing/QC contract",
            expected=EXPECTED_ROUTE_BEHAVIOR,
            observed=shared.get("route_behavior", ""),
            physical_meaning="Algorithm or report changes must not rewrite the mature physical gas/water route timing and QC behavior.",
        ),
        _check(
            check_id="shared_route_runner_names",
            title="CO2/H2O mature queue runners are shared",
            passed=shared.get("co2_runner") == EXPECTED_CO2_RUNNER
            and shared.get("h2o_runner") == EXPECTED_H2O_RUNNER
            and legacy_co2.get("runner") == EXPECTED_CO2_RUNNER
            and legacy_h2o.get("runner") == EXPECTED_H2O_RUNNER
            and absorption_co2.get("runner") == EXPECTED_CO2_RUNNER
            and absorption_h2o.get("runner") == EXPECTED_H2O_RUNNER,
            reason="old/new algorithm profiles must point at the same mature CO2/H2O queue runners",
            expected={"co2": EXPECTED_CO2_RUNNER, "h2o": EXPECTED_H2O_RUNNER},
            observed={
                "shared_co2": shared.get("co2_runner"),
                "shared_h2o": shared.get("h2o_runner"),
                "legacy_co2": legacy_co2.get("runner"),
                "legacy_h2o": legacy_h2o.get("runner"),
                "absorption_co2": absorption_co2.get("runner"),
                "absorption_h2o": absorption_h2o.get("runner"),
            },
            physical_meaning="New algorithm differences are allowed in fit inputs and write contracts, not in the route runner identity.",
        ),
        _check(
            check_id="legacy_default_profile",
            title="Legacy ratio remains the production default profile",
            passed=config.get("default_profile_id") == "legacy_ratio_production"
            and legacy.get("production_default") is True
            and absorption.get("production_default") is False,
            reason="legacy ratio production remains default until absorption production blockers are closed",
            expected={
                "default_profile_id": "legacy_ratio_production",
                "legacy_production_default": True,
                "absorption_production_default": False,
            },
            observed={
                "default_profile_id": config.get("default_profile_id"),
                "legacy_production_default": legacy.get("production_default"),
                "absorption_production_default": absorption.get("production_default"),
            },
            physical_meaning="A new algorithm profile may exist as a candidate without silently becoming the production route.",
        ),
        _check(
            check_id="legacy_co2_45_point_contract",
            title="Legacy CO2 mature route stays at 45 points",
            passed=legacy_co2.get("formal_point_count") == 45
            and _point_count(legacy_co2_plan) == 45
            and _same_plan(legacy_co2_plan, EXPECTED_LEGACY_CO2_PLAN),
            reason="legacy CO2 point count and temperature/ppm plan must not drift",
            expected={"formal_point_count": 45, "temperature_plan": EXPECTED_LEGACY_CO2_PLAN},
            observed={
                "formal_point_count": legacy_co2.get("formal_point_count"),
                "temperature_plan": legacy_co2_plan,
            },
            physical_meaning="CO2 production fitting uses the mature 45-point open-flow gas route, not ad hoc replay or diagnostic points.",
        ),
        _check(
            check_id="legacy_h2o_13_point_contract",
            title="Legacy H2O mature route stays at 13 wet points",
            passed=legacy_h2o.get("formal_point_count") == 13
            and _point_count(legacy_h2o_plan) == 13
            and _same_plan(legacy_h2o_plan, EXPECTED_LEGACY_H2O_PLAN),
            reason="legacy H2O wet point count and temperature/HGEN plan must not drift",
            expected={"formal_point_count": 13, "temperature_plan": EXPECTED_LEGACY_H2O_PLAN},
            observed={
                "formal_point_count": legacy_h2o.get("formal_point_count"),
                "temperature_plan": legacy_h2o_plan,
            },
            physical_meaning="H2O production fitting uses the mature 13-point wet route; dry/low-water anchors are separate evidence roles.",
        ),
        _check(
            check_id="legacy_ratio_fit_input_contract",
            title="Legacy fit inputs remain ratio based",
            passed=legacy.get("algorithm_mode") == "legacy_ratio_R"
            and legacy.get("fit_input", {}).get("co2") == "R_CO2"
            and legacy.get("fit_input", {}).get("h2o") == "R_H2O",
            reason="legacy production profile must not silently switch from ratio R to absorption A",
            expected={"algorithm_mode": "legacy_ratio_R", "co2": "R_CO2", "h2o": "R_H2O"},
            observed={
                "algorithm_mode": legacy.get("algorithm_mode"),
                "co2": legacy.get("fit_input", {}).get("co2"),
                "h2o": legacy.get("fit_input", {}).get("h2o"),
            },
            physical_meaning="Old algorithm replay/fitting should continue to interpret measured ratios directly.",
        ),
        _check(
            check_id="absorption_profile_fit_input_only",
            title="Absorption profile changes fit input, not route action",
            passed=absorption.get("algorithm_mode") == "absorption_ratio_A"
            and absorption.get("fit_input", {}).get("formula") == "A=-ln(R/R0(T))/(P_kPa/100)"
            and absorption_co2.get("formal_point_count") == 45
            and absorption_co2.get("production_candidate_point_count_with_supplements") == 47
            and absorption_h2o.get("formal_wet_point_count") == 13
            and absorption_h2o.get("production_candidate_wet_point_count_with_supplements") == 14,
            reason="new algorithm may add candidate evidence, but the mature route baseline remains 45/13",
            expected={
                "algorithm_mode": "absorption_ratio_A",
                "formula": "A=-ln(R/R0(T))/(P_kPa/100)",
                "co2_formal": 45,
                "co2_candidate": 47,
                "h2o_formal_wet": 13,
                "h2o_candidate_wet": 14,
            },
            observed={
                "algorithm_mode": absorption.get("algorithm_mode"),
                "formula": absorption.get("fit_input", {}).get("formula"),
                "co2_formal": absorption_co2.get("formal_point_count"),
                "co2_candidate": absorption_co2.get("production_candidate_point_count_with_supplements"),
                "h2o_formal_wet": absorption_h2o.get("formal_wet_point_count"),
                "h2o_candidate_wet": absorption_h2o.get("production_candidate_wet_point_count_with_supplements"),
            },
            physical_meaning="Absorption A is a fitting-layer change; it does not justify changing mature gas/water route sequencing.",
        ),
        _check(
            check_id="supplement_points_do_not_modify_legacy_queue",
            title="New algorithm supplement points cannot modify legacy queues",
            passed=absorption_co2_policy.get("supplemental_points_are_candidate_only") is True
            and absorption_co2_policy.get("must_not_modify_legacy_ratio_production_queue") is True
            and absorption_h2o_policy.get("supplemental_wet_points_are_candidate_only") is True
            and absorption_h2o_policy.get("must_not_modify_legacy_ratio_production_queue") is True,
            reason="supplement points are candidate evidence, not legacy queue edits",
            expected={
                "co2_candidate_only": True,
                "co2_must_not_modify_legacy": True,
                "h2o_candidate_only": True,
                "h2o_must_not_modify_legacy": True,
            },
            observed={
                "co2_candidate_only": absorption_co2_policy.get("supplemental_points_are_candidate_only"),
                "co2_must_not_modify_legacy": absorption_co2_policy.get(
                    "must_not_modify_legacy_ratio_production_queue"
                ),
                "h2o_candidate_only": absorption_h2o_policy.get("supplemental_wet_points_are_candidate_only"),
                "h2o_must_not_modify_legacy": absorption_h2o_policy.get(
                    "must_not_modify_legacy_ratio_production_queue"
                ),
            },
            physical_meaning="Supplemental points are run as normal gas/water points only when selected by the new algorithm profile.",
        ),
        _check(
            check_id="r0_writer_contract_blocks_absorption_release",
            title="SENCOA/SENCOB R0 writer remains a production blocker",
            passed=r0_contract.get("status") == "blocked_until_controlled_sencoa_sencob_writer_exists"
            and all(
                item.get("production_blocker") is True
                and item.get("controlled_writer_status") == "missing_controlled_writer"
                for item in r0_contract.get("components", [])
            ),
            reason="absorption profile cannot be complete production until R0 writer/readback exists",
            expected="blocked_until_controlled_sencoa_sencob_writer_exists",
            observed=r0_contract,
            physical_meaning="R0_CO2(T) and R0_H2O(T) are physical calibration models that require controlled SENCOA/SENCOB write/readback contracts.",
        ),
        _check(
            check_id="co2_zero_h2o_dry_anchor_separation",
            title="CO2 zero gas and H2O dry anchor stay separate",
            passed=shared.get("co2_zero_anchor_distinct_from_h2o_dry_anchor") is True
            and shared.get("h2o_dry_anchor_requires_dewpoint_pressure_temperature_bridge") is True
            and legacy_co2.get("anchor_policy", {}).get("h2o_dry_anchor_role") == "not_used_as_co2_anchor"
            and absorption_h2o.get("r0_policy", {}).get("must_not_use_co2_zero_gas_as_h2o_zero") is True
            and absorption_h2o.get("r0_policy", {}).get("must_not_force_residual_water_to_zero") is True,
            reason="low-end anchors must preserve their measured physical quantity",
            expected="CO2 zero gas distinct from H2O dry/low-water anchor",
            observed={
                "shared": shared,
                "legacy_co2_anchor_policy": legacy_co2.get("anchor_policy", {}),
                "absorption_h2o_r0_policy": absorption_h2o.get("r0_policy", {}),
            },
            physical_meaning="CO2 zero gas constrains CO2; H2O dry/low-water anchors require dewpoint/pressure/T evidence and are not forced to zero.",
        ),
        _check(
            check_id="canonical_entrypoint_guard",
            title="Canonical queues stay top-level; workers stay subordinate",
            passed=_canonical_stage_entry("04_co2_open_flow_sampling")
            == "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py"
            and _canonical_stage_entry("05_h2o_open_flow_sampling")
            == "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py"
            and NON_START_HERE_GUARDRAILS.get("formal_sampling_worker", {}).get("guardrail")
            == "use_via_canonical_queue_only",
            reason="sampling workers must not become top-level formal start points",
            expected={
                "co2_stage": "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py",
                "h2o_stage": "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py",
                "worker_guardrail": "use_via_canonical_queue_only",
            },
            observed={
                "co2_stage": _canonical_stage_entry("04_co2_open_flow_sampling"),
                "h2o_stage": _canonical_stage_entry("05_h2o_open_flow_sampling"),
                "worker_guardrail": NON_START_HERE_GUARDRAILS.get("formal_sampling_worker", {}).get(
                    "guardrail"
                ),
                "co2_worker": EXPECTED_CO2_WORKER_PATH,
                "h2o_worker": EXPECTED_H2O_WORKER_PATH,
            },
            physical_meaning="The queue owns route-level timing and point progression; workers only execute a queue-selected point.",
        ),
    ]

    blocker_count = sum(1 for check in checks if check.status == "blocker")
    manifest = {
        "schema": SCHEMA,
        "created_at": _now(),
        "profile_path": str(profile_file),
        "status": "pass" if blocker_count == 0 else "blocked",
        "blocker_count": blocker_count,
        "no_write": True,
        "opens_com_ports": False,
        "connects_postgresql": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "not_real_acceptance_evidence": True,
        "mature_route_contract": {
            "route_behavior": EXPECTED_ROUTE_BEHAVIOR,
            "legacy_co2_point_count": 45,
            "legacy_h2o_wet_point_count": 13,
            "co2_runner": EXPECTED_CO2_RUNNER,
            "h2o_runner": EXPECTED_H2O_RUNNER,
            "new_algorithm_difference_layer": "profile_fit_input_R0_contract_supplements_write_contract",
        },
    }
    return {
        "manifest": manifest,
        "checks": [check.to_json() for check in checks],
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows([dict(row) for row in rows])


def _render_markdown(model: Mapping[str, Any]) -> str:
    manifest = model.get("manifest", {})
    lines = [
        "# V1.5 Mature Route Contract",
        "",
        f"- schema: `{manifest.get('schema')}`",
        f"- status: `{manifest.get('status')}`",
        f"- blocker_count: `{manifest.get('blocker_count')}`",
        f"- profile_path: `{manifest.get('profile_path')}`",
        "",
        "## Physical Boundaries",
        "",
        f"- opens_com_ports: `{manifest.get('opens_com_ports')}`",
        f"- connects_postgresql: `{manifest.get('connects_postgresql')}`",
        f"- controls_pressure: `{manifest.get('controls_pressure')}`",
        f"- controls_water_or_gas_routes: `{manifest.get('controls_water_or_gas_routes')}`",
        f"- writes_coefficients: `{manifest.get('writes_coefficients')}`",
        f"- writes_device_id: `{manifest.get('writes_device_id')}`",
        f"- not_real_acceptance_evidence: `{manifest.get('not_real_acceptance_evidence')}`",
        "",
        "## Mature Route Contract",
        "",
        "| Key | Value |",
        "|---|---|",
    ]
    for key, value in (manifest.get("mature_route_contract") or {}).items():
        lines.append(f"| `{key}` | `{_fmt(value)}` |")

    lines.extend(
        [
            "",
            "## Checks",
            "",
            "| Check | Status | Reason | Physical meaning |",
            "|---|---|---|---|",
        ]
    )
    for row in model.get("checks", []):
        lines.append(
            f"| `{row.get('check_id')}` | `{row.get('status')}` | {row.get('reason')} | "
            f"{row.get('physical_meaning')} |"
        )
    return "\n".join(lines).rstrip() + "\n"


def write_v1_5_mature_route_contract(
    *,
    profile_path: str | Path,
    output_dir: str | Path,
) -> dict[str, str]:
    """Write JSON/CSV/Markdown mature route contract artifacts."""

    model = build_v1_5_mature_route_contract(profile_path=profile_path)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    outputs = {
        "manifest": out / "v1_5_mature_route_contract.json",
        "checks": out / "v1_5_mature_route_contract_checks.csv",
        "markdown": out / "V1_5_MATURE_ROUTE_CONTRACT.md",
    }
    outputs["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(outputs["checks"], model["checks"])
    outputs["markdown"].write_text(_render_markdown(model), encoding="utf-8")
    return {key: str(path) for key, path in outputs.items()}
