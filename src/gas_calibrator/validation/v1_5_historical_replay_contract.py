"""Offline guard for V1.5 historical replay interpretation.

Historical CSV/JSON replay is useful for program-level regression, but it must
not become a hidden real-acceptance path or a way to mutate the mature 0620
CO2/H2O route. This validator checks the replay contract from profile metadata
and declared replay-source families only. It does not open COM ports, connect
to PostgreSQL, control gas/water routes, or write coefficients.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_historical_replay_contract_v1"

EXPECTED_ROUTE_BEHAVIOR = "preserve_mature_v1_5_0620_route_timing_and_quality_gates"
EXPECTED_ABSORPTION_FORMULA = "A=-ln(R/R0(T))/(P_kPa/100)"

REQUIRED_REPLAY_ROLES: tuple[str, ...] = (
    "initialization_readiness",
    "pressure_senco9_review",
    "co2_open_flow_points",
    "h2o_open_flow_points",
    "point_qc_and_quality_grade",
    "fit_input_review",
    "post_write_reverify",
    "archive_status",
)


DEFAULT_REPLAY_SOURCE_FAMILIES: tuple[dict[str, Any], ...] = (
    {
        "family_id": "mature_0620_legacy_ratio",
        "algorithm_profile_id": "legacy_ratio_production",
        "source_role": "mature_path_reference",
        "required_roles": list(REQUIRED_REPLAY_ROLES),
        "co2_formal_point_count": 45,
        "h2o_formal_wet_point_count": 13,
        "co2_fit_input": "R_CO2",
        "h2o_fit_input": "R_H2O",
        "qc_policy": {
            "fit_eligibility_requires_a_grade": True,
            "rejected_points_preserved_with_reason": True,
            "sample_quality_rejects_remain_rejected": True,
        },
        "release_policy": {
            "formal_release_allowed_from_replay": False,
            "database_import_allowed_from_replay": False,
            "not_real_acceptance_evidence": True,
        },
    },
    {
        "family_id": "later_legacy_regression_runs",
        "algorithm_profile_id": "legacy_ratio_production",
        "source_role": "regression_reference",
        "required_roles": list(REQUIRED_REPLAY_ROLES),
        "co2_formal_point_count": 45,
        "h2o_formal_wet_point_count": 13,
        "co2_fit_input": "R_CO2",
        "h2o_fit_input": "R_H2O",
        "qc_policy": {
            "fit_eligibility_requires_a_grade": True,
            "rejected_points_preserved_with_reason": True,
            "sample_quality_rejects_remain_rejected": True,
        },
        "release_policy": {
            "formal_release_allowed_from_replay": False,
            "database_import_allowed_from_replay": False,
            "not_real_acceptance_evidence": True,
        },
    },
    {
        "family_id": "new_algorithm_shadow_run",
        "algorithm_profile_id": "absorption_ratio_shadow",
        "source_role": "new_algorithm_fit_input_shadow",
        "required_roles": [*REQUIRED_REPLAY_ROLES, "r0_evidence_review"],
        "co2_formal_point_count": 45,
        "h2o_formal_wet_point_count": 13,
        "co2_candidate_point_count_with_supplements": 47,
        "h2o_candidate_wet_point_count_with_supplements": 14,
        "fit_input_formula": EXPECTED_ABSORPTION_FORMULA,
        "co2_fit_input": "A_CO2_from_R_CO2_and_R0_CO2_T",
        "h2o_fit_input": "A_H2O_from_R_H2O_and_R0_H2O_T",
        "r0_evidence_required": True,
        "qc_policy": {
            "fit_eligibility_requires_a_grade": True,
            "rejected_points_preserved_with_reason": True,
            "sample_quality_rejects_remain_rejected": True,
        },
        "release_policy": {
            "formal_release_allowed_from_replay": False,
            "database_import_allowed_from_replay": False,
            "not_real_acceptance_evidence": True,
        },
    },
)


@dataclass(frozen=True)
class HistoricalReplayCheck:
    check_id: str
    title: str
    status: str
    reason: str
    expected: str
    observed: str
    physical_meaning: str
    blocks_replay_contract_release: bool

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
) -> HistoricalReplayCheck:
    return HistoricalReplayCheck(
        check_id=check_id,
        title=title,
        status="pass" if passed else "blocker",
        reason=reason,
        expected=_fmt(expected),
        observed=_fmt(observed),
        physical_meaning=physical_meaning,
        blocks_replay_contract_release=not passed,
    )


def _family_by_id(families: Sequence[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    return {str(item.get("family_id") or ""): item for item in families}


def _required_roles_present(family: Mapping[str, Any]) -> bool:
    observed = set(str(role) for role in family.get("required_roles", []))
    expected = set(REQUIRED_REPLAY_ROLES)
    if family.get("algorithm_profile_id") == "absorption_ratio_shadow":
        expected.add("r0_evidence_review")
    return expected.issubset(observed)


def _all_families_have_qc_contract(families: Sequence[Mapping[str, Any]]) -> bool:
    for family in families:
        qc = family.get("qc_policy", {})
        if not (
            qc.get("fit_eligibility_requires_a_grade") is True
            and qc.get("rejected_points_preserved_with_reason") is True
            and qc.get("sample_quality_rejects_remain_rejected") is True
        ):
            return False
    return True


def _all_families_are_no_release(families: Sequence[Mapping[str, Any]]) -> bool:
    for family in families:
        release = family.get("release_policy", {})
        if not (
            release.get("formal_release_allowed_from_replay") is False
            and release.get("database_import_allowed_from_replay") is False
            and release.get("not_real_acceptance_evidence") is True
        ):
            return False
    return True


def _r0_components_block_release(r0_contract: Mapping[str, Any]) -> bool:
    components = r0_contract.get("components", [])
    return bool(components) and all(
        item.get("production_blocker") is True
        and item.get("controlled_writer_status") == "missing_controlled_writer"
        for item in components
        if isinstance(item, Mapping)
    )


def build_v1_5_historical_replay_contract(
    *,
    profile_path: str | Path,
    replay_source_families: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Return the offline historical-replay guard model."""

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
    r0_contract = absorption.get("r0_write_contract", {}) if isinstance(absorption, Mapping) else {}

    families = tuple(replay_source_families or DEFAULT_REPLAY_SOURCE_FAMILIES)
    families_by_id = _family_by_id(families)
    legacy_0620 = families_by_id.get("mature_0620_legacy_ratio", {})
    later_legacy = families_by_id.get("later_legacy_regression_runs", {})
    absorption_shadow = families_by_id.get("new_algorithm_shadow_run", {})

    checks = [
        _check(
            check_id="historical_replay_is_offline_only",
            title="Historical replay remains offline evidence",
            passed=True,
            reason="this contract exports only JSON/CSV/Markdown and performs no hardware or database action",
            expected={
                "opens_com_ports": False,
                "connects_postgresql": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
                "not_real_acceptance_evidence": True,
            },
            observed={
                "opens_com_ports": False,
                "connects_postgresql": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
                "not_real_acceptance_evidence": True,
            },
            physical_meaning="Replay can find program regressions, but it cannot replace a physical run, post-write reverify, archive closure, or database release.",
        ),
        _check(
            check_id="replay_source_families_have_required_roles",
            title="Replay families keep the complete evidence chain roles",
            passed=all(_required_roles_present(family) for family in families),
            reason="each replay family must carry initialization, pressure, CO2, H2O, QC, fit, reverify, and archive-status roles",
            expected=REQUIRED_REPLAY_ROLES,
            observed={family.get("family_id"): family.get("required_roles", []) for family in families},
            physical_meaning="Historical data replay must preserve where each row came from and why it is or is not fit-eligible.",
        ),
        _check(
            check_id="legacy_replay_uses_mature_ratio_profile",
            title="Legacy replay uses the mature ratio profile",
            passed=shared.get("route_behavior") == EXPECTED_ROUTE_BEHAVIOR
            and config.get("default_profile_id") == "legacy_ratio_production"
            and legacy.get("algorithm_mode") == "legacy_ratio_R"
            and legacy.get("fit_input", {}).get("co2") == "R_CO2"
            and legacy.get("fit_input", {}).get("h2o") == "R_H2O"
            and legacy_0620.get("co2_fit_input") == "R_CO2"
            and legacy_0620.get("h2o_fit_input") == "R_H2O"
            and later_legacy.get("co2_fit_input") == "R_CO2"
            and later_legacy.get("h2o_fit_input") == "R_H2O",
            reason="legacy replay must not silently reinterpret mature ratio data as absorption data",
            expected={"profile": "legacy_ratio_production", "co2": "R_CO2", "h2o": "R_H2O"},
            observed={
                "route_behavior": shared.get("route_behavior"),
                "default_profile_id": config.get("default_profile_id"),
                "profile_mode": legacy.get("algorithm_mode"),
                "profile_fit_input": legacy.get("fit_input", {}),
                "mature_0620_inputs": {
                    "co2": legacy_0620.get("co2_fit_input"),
                    "h2o": legacy_0620.get("h2o_fit_input"),
                },
                "later_legacy_inputs": {
                    "co2": later_legacy.get("co2_fit_input"),
                    "h2o": later_legacy.get("h2o_fit_input"),
                },
            },
            physical_meaning="The old algorithm's replay physics is ratio R; absorption A belongs only to the new algorithm shadow profile.",
        ),
        _check(
            check_id="legacy_replay_preserves_45_13_counts",
            title="Legacy replay preserves mature CO2/H2O counts",
            passed=legacy_co2.get("formal_point_count") == 45
            and legacy_h2o.get("formal_point_count") == 13
            and legacy_0620.get("co2_formal_point_count") == 45
            and legacy_0620.get("h2o_formal_wet_point_count") == 13
            and later_legacy.get("co2_formal_point_count") == 45
            and later_legacy.get("h2o_formal_wet_point_count") == 13,
            reason="historical replay may compare runs, but it must not redefine the mature route size",
            expected={"co2_formal_point_count": 45, "h2o_formal_wet_point_count": 13},
            observed={
                "profile_co2": legacy_co2.get("formal_point_count"),
                "profile_h2o": legacy_h2o.get("formal_point_count"),
                "mature_0620": {
                    "co2": legacy_0620.get("co2_formal_point_count"),
                    "h2o": legacy_0620.get("h2o_formal_wet_point_count"),
                },
                "later_legacy": {
                    "co2": later_legacy.get("co2_formal_point_count"),
                    "h2o": later_legacy.get("h2o_formal_wet_point_count"),
                },
            },
            physical_meaning="The replay contract protects the 0620 mature point sequence from being diluted by diagnostics or retry fragments.",
        ),
        _check(
            check_id="new_algorithm_replay_uses_absorption_shadow",
            title="New algorithm replay is a fit-input shadow, not a route fork",
            passed=absorption.get("algorithm_mode") == "absorption_ratio_A"
            and absorption.get("production_default") is False
            and absorption.get("fit_input", {}).get("formula") == EXPECTED_ABSORPTION_FORMULA
            and absorption_shadow.get("fit_input_formula") == EXPECTED_ABSORPTION_FORMULA
            and absorption_co2.get("formal_point_count") == 45
            and absorption_h2o.get("formal_wet_point_count") == 13
            and absorption_co2.get("production_candidate_point_count_with_supplements") == 47
            and absorption_h2o.get("production_candidate_wet_point_count_with_supplements") == 14
            and absorption_shadow.get("co2_candidate_point_count_with_supplements") == 47
            and absorption_shadow.get("h2o_candidate_wet_point_count_with_supplements") == 14,
            reason="new algorithm replay can evaluate A and supplemental candidates, but it cannot replace mature runners",
            expected={
                "formula": EXPECTED_ABSORPTION_FORMULA,
                "formal": "45/13",
                "candidate": "47/14",
                "production_default": False,
            },
            observed={
                "profile_mode": absorption.get("algorithm_mode"),
                "production_default": absorption.get("production_default"),
                "profile_fit_input": absorption.get("fit_input", {}),
                "shadow_formula": absorption_shadow.get("fit_input_formula"),
                "profile_counts": {
                    "co2_formal": absorption_co2.get("formal_point_count"),
                    "h2o_formal": absorption_h2o.get("formal_wet_point_count"),
                    "co2_candidate": absorption_co2.get("production_candidate_point_count_with_supplements"),
                    "h2o_candidate": absorption_h2o.get(
                        "production_candidate_wet_point_count_with_supplements"
                    ),
                },
            },
            physical_meaning="The new algorithm changes concentration math through A and R0(T); it does not authorize a second physical route runner.",
        ),
        _check(
            check_id="new_algorithm_replay_requires_r0_evidence",
            title="Absorption replay requires R0 evidence and keeps R0 writer blocked",
            passed=absorption_shadow.get("r0_evidence_required") is True
            and "r0_evidence_review" in absorption_shadow.get("required_roles", [])
            and r0_contract.get("status") == "blocked_until_controlled_sencoa_sencob_writer_exists"
            and _r0_components_block_release(r0_contract),
            reason="A=-ln(R/R0)/(P/100) cannot be production-complete without R0_CO2(T)/R0_H2O(T) evidence and writer/readback contracts",
            expected="R0 evidence role present and SENCOA/SENCOB production release blocked",
            observed={
                "shadow_r0_evidence_required": absorption_shadow.get("r0_evidence_required"),
                "shadow_required_roles": absorption_shadow.get("required_roles", []),
                "r0_contract": r0_contract,
            },
            physical_meaning="R0 is not a cosmetic metadata field; it is part of the physical absorption transform and must stay traceable.",
        ),
        _check(
            check_id="replay_qc_rejections_remain_non_fit",
            title="Replay preserves QC rejection semantics",
            passed=_all_families_have_qc_contract(families),
            reason="historical rejected rows must not become fit-eligible just because replay needs more points",
            expected={
                "fit_eligibility_requires_a_grade": True,
                "rejected_points_preserved_with_reason": True,
                "sample_quality_rejects_remain_rejected": True,
            },
            observed={family.get("family_id"): family.get("qc_policy", {}) for family in families},
            physical_meaning="Replay should improve program confidence by preserving quality labels, not by washing out unstable or failed points.",
        ),
        _check(
            check_id="replay_does_not_authorize_archive_or_database_release",
            title="Replay cannot release archive or database import",
            passed=_all_families_are_no_release(families),
            reason="historical replay is regression evidence, not current-run archive or PostgreSQL 18 import evidence",
            expected={
                "formal_release_allowed_from_replay": False,
                "database_import_allowed_from_replay": False,
                "not_real_acceptance_evidence": True,
            },
            observed={family.get("family_id"): family.get("release_policy", {}) for family in families},
            physical_meaning="A replay pass can say the program still understands old evidence; it cannot say today's run is ready to release.",
        ),
        _check(
            check_id="evidence_zone_is_not_code_source",
            title="Historical evidence zones remain evidence-only",
            passed=True,
            reason="_handoff and root-draft evidence can be read as historical inputs only; formal code still comes from the clean V1.5 worktree",
            expected="read historical evidence without promoting _handoff/root drafts to formal entrypoints",
            observed={
                "clean_worktree_is_source_of_truth": True,
                "handoff_is_evidence_only": True,
                "root_draft_is_not_formal_source": True,
            },
            physical_meaning="This prevents replay from reintroducing the earlier V1/V2/root/worktree mixing problem.",
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
        "historical_replay_contract": {
            "purpose": "offline_program_level_regression_guard",
            "source_families": [family.get("family_id") for family in families],
            "required_roles": list(REQUIRED_REPLAY_ROLES),
            "legacy_replay_fit_input": "R_CO2/R_H2O",
            "new_algorithm_replay_fit_input": EXPECTED_ABSORPTION_FORMULA,
            "release_policy": "replay_pass_does_not_authorize_archive_or_database_import",
        },
    }
    return {
        "manifest": manifest,
        "replay_source_families": [dict(family) for family in families],
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
        "# V1.5 Historical Replay Contract",
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
        "## Replay Contract",
        "",
        "| Key | Value |",
        "|---|---|",
    ]
    for key, value in (manifest.get("historical_replay_contract") or {}).items():
        lines.append(f"| `{key}` | `{_fmt(value)}` |")

    lines.extend(
        [
            "",
            "## Replay Source Families",
            "",
            "| Family | Profile | Role | Fit inputs | Release from replay |",
            "|---|---|---|---|---|",
        ]
    )
    for family in model.get("replay_source_families", []):
        release = family.get("release_policy", {})
        fit_inputs = {
            "co2": family.get("co2_fit_input"),
            "h2o": family.get("h2o_fit_input"),
            "formula": family.get("fit_input_formula", ""),
        }
        lines.append(
            f"| `{family.get('family_id')}` | `{family.get('algorithm_profile_id')}` | "
            f"`{family.get('source_role')}` | `{_fmt(fit_inputs)}` | "
            f"`archive={release.get('formal_release_allowed_from_replay')}, "
            f"db={release.get('database_import_allowed_from_replay')}` |"
        )

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


def write_v1_5_historical_replay_contract(
    *,
    profile_path: str | Path,
    output_dir: str | Path,
    replay_source_families: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, str]:
    """Write JSON/CSV/Markdown historical-replay contract artifacts."""

    model = build_v1_5_historical_replay_contract(
        profile_path=profile_path,
        replay_source_families=replay_source_families,
    )
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    outputs = {
        "manifest": out / "v1_5_historical_replay_contract.json",
        "checks": out / "v1_5_historical_replay_contract_checks.csv",
        "markdown": out / "V1_5_HISTORICAL_REPLAY_CONTRACT.md",
    }
    outputs["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(outputs["checks"], model["checks"])
    outputs["markdown"].write_text(_render_markdown(model), encoding="utf-8")
    return {key: str(path) for key, path in outputs.items()}
