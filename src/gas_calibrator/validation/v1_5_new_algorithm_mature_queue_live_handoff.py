"""Freeze the V1.5 new-algorithm 47/14 handoff to mature route runners.

This module is deliberately offline. It materializes immutable profile queue
rows, binds them to the reviewed mature runner sources, and records the future
authorization requirements. It never executes a queue, opens COM, controls a
route, writes analyzer state, or connects PostgreSQL.
"""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_algorithm_mature_queue_inputs import (
    CO2_QUEUE_RUNNER,
    H2O_QUEUE_RUNNER,
    build_v1_5_algorithm_mature_queue_inputs,
)


SCHEMA = "v1_5_new_algorithm_mature_queue_live_handoff_v1"
BLOCKED_EXECUTOR_SCHEMA = (
    "v1_5_new_algorithm_mature_queue_live_handoff_blocked_executor_v1"
)
LEGACY_PROFILE_ID = "legacy_ratio_production"
NEW_PROFILE_ID = "absorption_ratio_shadow"
MATURE_ROUTE_BEHAVIOR = "preserve_mature_v1_5_0620_route_timing_and_quality_gates"
FIT_FORMULA = "A=-ln(R/R0(T))/(P_kPa/100)"
CONFIRMATION_TEXT = "AUTHORIZE_V1_5_NEW_ALGORITHM_47_14_MATURE_QUEUE_HANDOFF"
SUPPLEMENTAL_CO2_POINTS = ((-20.0, 600.0), (-10.0, 600.0))
SUPPLEMENTAL_H2O_POINTS = ((40.0, 30.0, 30.0),)
PROTECTED_ROUTE_SOURCES = {
    "co2_queue": "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py",
    "h2o_queue": "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py",
    "co2_sampling_worker": "src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py",
    "h2o_sampling_worker": "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_sampling.py",
}
FORBIDDEN_QUEUE_MARKERS = (
    "_handoff",
    "20260624",
    "0624",
    "migration",
    "diagnostic",
)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _rows_sha256(rows: Sequence[Mapping[str, Any]]) -> str:
    payload = json.dumps(
        [dict(row) for row in rows],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _portable_path(path: Path, repo_root: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return str(resolved)


def _profile(payload: Mapping[str, Any], profile_id: str) -> dict[str, Any]:
    for row in payload.get("profiles") or []:
        if isinstance(row, Mapping) and row.get("profile_id") == profile_id:
            return dict(row)
    return {}


def _co2_identities(rows: Sequence[Mapping[str, Any]]) -> list[tuple[float, float]]:
    return [
        (float(row["temp_c"]), float(row["source_nominal_ppm"]))
        for row in rows
    ]


def _h2o_identities(
    rows: Sequence[Mapping[str, Any]],
) -> list[tuple[float, float, float]]:
    return [
        (
            float(row["temp_c"]),
            float(row["hgen_temp_c"]),
            float(row["hgen_rh_pct"]),
        )
        for row in rows
    ]


def _check(
    check_id: str,
    reasons: Sequence[str],
    *,
    physical_meaning: str,
    details: Mapping[str, Any],
) -> dict[str, Any]:
    unique_reasons = list(dict.fromkeys(str(reason) for reason in reasons if reason))
    return {
        "check_id": check_id,
        "status": "pass" if not unique_reasons else "blocker",
        "reasons": unique_reasons,
        "physical_meaning": physical_meaning,
        "details": dict(details),
    }


def _authorization_requirements() -> list[dict[str, Any]]:
    return [
        {"order": 1, "requirement": "exact_profile_and_queue_hashes", "value": "profile plus CO2-47 and H2O-14 CSV SHA256", "purpose": "prevent point-plan substitution"},
        {"order": 2, "requirement": "exact_mature_runner_hashes", "value": "CO2/H2O queue and sampling-worker SHA256", "purpose": "prevent route-kernel substitution"},
        {"order": 3, "requirement": "active_analyzers", "value": "1-6 devices with unique 8-digit SN/device_code and reviewed COM/GA/protocol-ID mapping", "purpose": "bind physical devices"},
        {"order": 4, "requirement": "current_readiness", "value": "initialization, S9 pressure, route, certificates, runtime and CHECK policy pass", "purpose": "prevent stale readiness reuse"},
        {"order": 5, "requirement": "three_party_authorization", "value": "operator, reviewer, approver; reviewer != approver", "purpose": "separate operation and approval"},
        {"order": 6, "requirement": "explicit_confirmation", "value": CONFIRMATION_TEXT, "purpose": "avoid implicit live execution"},
        {"order": 7, "requirement": "serial_pacing", "value": "minimum command and retry gap >= 1.0 s", "purpose": "protect analyzer serial timing"},
        {"order": 8, "requirement": "route_order", "value": "CO2 47 first, H2O 14 second", "purpose": "preserve mature production order"},
        {"order": 9, "requirement": "fit_and_write_separation", "value": "A/R0(T) fitting and SENCOA/B writes remain outside route adapter", "purpose": "do not mix acquisition and coefficient writes"},
        {"order": 10, "requirement": "legacy_default_unchanged", "value": "legacy_ratio_production remains 45/13 default", "purpose": "protect existing production fallback"},
    ]


def _production_blockers(new_profile: Mapping[str, Any]) -> list[dict[str, str]]:
    blockers = [
        {"blocker": "separate_live_adapter_not_implemented", "next_action": "implement in a separately reviewed hardware-authorized package"},
        {"blocker": "live_authorization_packet_not_supplied", "next_action": "bind exact hashes and three-party authorization"},
        {"blocker": "active_analyzer_and_port_inventory_not_supplied", "next_action": "bind 1-6 current devices before live execution"},
        {"blocker": "current_pre_gas_pressure_route_readiness_not_supplied", "next_action": "generate fresh live readiness evidence"},
    ]
    r0_contract = new_profile.get("r0_write_contract") or {}
    if r0_contract.get("status") != "controlled_writer_implemented_and_verified":
        blockers.append(
            {
                "blocker": "sencoa_sencob_r0_writer_not_production_ready",
                "next_action": "complete controlled SENCOA/SENCOB write, readback, rollback and reverify",
            }
        )
    h2o_write_contract = (new_profile.get("h2o_route") or {}).get("write_contract") or {}
    if h2o_write_contract.get("status") != "firmware_input_scale_confirmed":
        blockers.append(
            {
                "blocker": "h2o_absorption_firmware_input_scale_not_confirmed",
                "next_action": "confirm the firmware H2O absorption input variable and scale before any production write",
            }
        )
    return blockers


def build_v1_5_new_algorithm_mature_queue_live_handoff(
    *,
    repo_root: str | Path,
    profile_path: str | Path,
    mature_route_contract_json: str | Path,
) -> dict[str, Any]:
    root = Path(repo_root).resolve()
    profile_file = Path(profile_path).resolve()
    mature_contract_file = Path(mature_route_contract_json).resolve()
    profile_payload = _read_json(profile_file)
    mature_payload = _read_json(mature_contract_file)
    legacy_profile = _profile(profile_payload, LEGACY_PROFILE_ID)
    new_profile = _profile(profile_payload, NEW_PROFILE_ID)
    checks: list[dict[str, Any]] = []

    legacy_model: dict[str, Any] = {}
    new_model: dict[str, Any] = {}
    build_reasons: list[str] = []
    try:
        legacy_model = build_v1_5_algorithm_mature_queue_inputs(
            profile_path=profile_file,
            profile_id=LEGACY_PROFILE_ID,
        )
        new_model = build_v1_5_algorithm_mature_queue_inputs(
            profile_path=profile_file,
            profile_id=NEW_PROFILE_ID,
        )
    except (KeyError, TypeError, ValueError) as exc:
        build_reasons.append(f"immutable_queue_materialization_failed:{exc}")
    checks.append(
        _check(
            "immutable_profile_queue_materialization",
            build_reasons,
            physical_meaning="The live handoff may consume only fresh profile-generated queues, never stale _handoff or 0624/migration queue files.",
            details={
                "legacy_profile": LEGACY_PROFILE_ID,
                "new_profile": NEW_PROFILE_ID,
                "profile_declared_queue_source_is_not_consumed": new_model.get(
                    "profile_declared_queue_source_is_not_consumed"
                ),
            },
        )
    )

    legacy_co2 = list(legacy_model.get("co2_rows") or [])
    legacy_h2o = list(legacy_model.get("h2o_rows") or [])
    new_co2 = list(new_model.get("co2_rows") or [])
    new_h2o = list(new_model.get("h2o_rows") or [])
    legacy_co2_ids = _co2_identities(legacy_co2) if legacy_co2 else []
    legacy_h2o_ids = _h2o_identities(legacy_h2o) if legacy_h2o else []
    new_co2_ids = _co2_identities(new_co2) if new_co2 else []
    new_h2o_ids = _h2o_identities(new_h2o) if new_h2o else []

    point_reasons: list[str] = []
    if (len(legacy_co2), len(legacy_h2o)) != (45, 13):
        point_reasons.append("legacy_point_counts_not_45_13")
    if (len(new_co2), len(new_h2o)) != (47, 14):
        point_reasons.append("new_algorithm_point_counts_not_47_14")
    if set(new_co2_ids) - set(legacy_co2_ids) != set(SUPPLEMENTAL_CO2_POINTS):
        point_reasons.append("co2_supplemental_point_set_mismatch")
    if set(new_h2o_ids) - set(legacy_h2o_ids) != set(SUPPLEMENTAL_H2O_POINTS):
        point_reasons.append("h2o_supplemental_point_set_mismatch")
    if set(legacy_co2_ids) - set(new_co2_ids):
        point_reasons.append("new_algorithm_removed_legacy_co2_points")
    if set(legacy_h2o_ids) - set(new_h2o_ids):
        point_reasons.append("new_algorithm_removed_legacy_h2o_points")
    try:
        if new_co2_ids.index((-20.0, 600.0)) != new_co2_ids.index((-20.0, 400.0)) + 1:
            point_reasons.append("minus20_600_not_inside_temperature_segment")
        if new_co2_ids.index((-10.0, 600.0)) != new_co2_ids.index((-10.0, 400.0)) + 1:
            point_reasons.append("minus10_600_not_inside_temperature_segment")
        if new_h2o_ids.index((40.0, 30.0, 30.0)) >= new_h2o_ids.index((40.0, 30.0, 50.0)):
            point_reasons.append("h2o_40_30_30_not_inside_temperature_segment")
    except ValueError:
        point_reasons.append("supplemental_point_order_not_evaluable")
    checks.append(
        _check(
            "legacy_45_13_and_new_47_14_point_contract",
            point_reasons,
            physical_meaning="New-algorithm supplemental points run inside their mature temperature segments without changing the legacy 45/13 default.",
            details={
                "legacy_counts": {"co2": len(legacy_co2), "h2o": len(legacy_h2o)},
                "new_counts": {"co2": len(new_co2), "h2o": len(new_h2o)},
                "co2_supplements": [list(row) for row in SUPPLEMENTAL_CO2_POINTS],
                "h2o_supplements": [list(row) for row in SUPPLEMENTAL_H2O_POINTS],
            },
        )
    )

    profile_reasons: list[str] = []
    if profile_payload.get("default_profile_id") != LEGACY_PROFILE_ID:
        profile_reasons.append("legacy_profile_not_default")
    if legacy_profile.get("production_default") is not True:
        profile_reasons.append("legacy_production_default_not_true")
    if new_profile.get("production_default") is not False:
        profile_reasons.append("new_algorithm_must_not_be_default")
    if new_profile.get("algorithm_mode") != "absorption_ratio_A":
        profile_reasons.append("new_algorithm_mode_not_absorption_A")
    fit_input = new_profile.get("fit_input") or {}
    if fit_input.get("formula") != FIT_FORMULA:
        profile_reasons.append("new_algorithm_fit_formula_mismatch")
    shared_contract = profile_payload.get("shared_route_contract") or {}
    if shared_contract.get("pressure_sequence") != "SENCO9_first":
        profile_reasons.append("pressure_sequence_not_SENCO9_first")
    if shared_contract.get("temperature_coefficients") != "neutral_by_default_for_both_algorithms":
        profile_reasons.append("SENCO7_SENCO8_not_neutral_by_default")
    if shared_contract.get("co2_zero_anchor_distinct_from_h2o_dry_anchor") is not True:
        profile_reasons.append("co2_zero_and_h2o_dry_anchor_not_separated")
    checks.append(
        _check(
            "algorithm_fit_input_and_physics_contract",
            profile_reasons,
            physical_meaning="The physical route stays mature; only fitting changes from R to A/R0(T), with pressure calibrated first and temperature coefficients neutral.",
            details={
                "formula": fit_input.get("formula"),
                "pressure_sequence": shared_contract.get("pressure_sequence"),
                "temperature_coefficients": shared_contract.get("temperature_coefficients"),
                "co2_zero_and_h2o_dry_anchor_are_separate": shared_contract.get(
                    "co2_zero_anchor_distinct_from_h2o_dry_anchor"
                ),
            },
        )
    )

    queue_source_reasons: list[str] = []
    queue_text = json.dumps([*new_co2, *new_h2o], ensure_ascii=False).lower()
    for marker in FORBIDDEN_QUEUE_MARKERS:
        if marker in queue_text:
            queue_source_reasons.append(f"forbidden_generated_queue_marker:{marker}")
    if new_model.get("profile_declared_queue_source_is_not_consumed") is not True:
        queue_source_reasons.append("stale_profile_queue_source_bypass_not_proven")
    if new_model.get("mature_point_execution_is_not_copied_or_modified") is not True:
        queue_source_reasons.append("mature_point_execution_protection_missing")
    if any(row.get("queue_source_contract") != "generated_from_reviewed_profile_only" for row in [*new_co2, *new_h2o]):
        queue_source_reasons.append("generated_queue_source_contract_mismatch")
    checks.append(
        _check(
            "migration_and_noncanonical_entrypoint_exclusion",
            queue_source_reasons,
            physical_meaning="The handoff must not consume _handoff, root migration, 0624, diagnostic, or worker-as-top-level queue sources.",
            details={
                "profile_declared_queue_source_is_not_consumed": new_model.get(
                    "profile_declared_queue_source_is_not_consumed"
                ),
                "top_level_runners": [CO2_QUEUE_RUNNER, H2O_QUEUE_RUNNER],
            },
        )
    )

    mature_manifest = mature_payload.get("manifest") or {}
    mature_contract = mature_manifest.get("mature_route_contract") or {}
    runner_reasons: list[str] = []
    if mature_manifest.get("status") != "pass" or int(mature_manifest.get("blocker_count") or 0) != 0:
        runner_reasons.append("mature_route_contract_not_pass")
    if mature_contract.get("route_behavior") != MATURE_ROUTE_BEHAVIOR:
        runner_reasons.append("mature_route_behavior_mismatch")
    if mature_contract.get("co2_runner") != CO2_QUEUE_RUNNER:
        runner_reasons.append("mature_co2_runner_mismatch")
    if mature_contract.get("h2o_runner") != H2O_QUEUE_RUNNER:
        runner_reasons.append("mature_h2o_runner_mismatch")
    runner_sources: list[dict[str, Any]] = []
    for role, relative_path in PROTECTED_ROUTE_SOURCES.items():
        source = root / relative_path
        exists = source.is_file()
        if not exists:
            runner_reasons.append(f"protected_route_source_missing:{role}")
        runner_sources.append(
            {
                "role": role,
                "path": relative_path,
                "sha256": _sha256(source) if exists else "",
            }
        )
    checks.append(
        _check(
            "mature_0620_0621_runner_binding",
            runner_reasons,
            physical_meaning="Both algorithms use the same reviewed 0620/0621 queue and point-worker implementation; the profile does not fork physical actions.",
            details={
                "route_behavior": mature_contract.get("route_behavior"),
                "co2_runner": mature_contract.get("co2_runner"),
                "h2o_runner": mature_contract.get("h2o_runner"),
                "protected_source_count": len(runner_sources),
            },
        )
    )

    checks.append(
        _check(
            "route_adapter_has_no_side_effects",
            (),
            physical_meaning="This package is an offline contract only and cannot open COM, control routes, write analyzers, import PostgreSQL, or release a run.",
            details={
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
                "connects_postgresql": False,
            },
        )
    )

    blocker_count = sum(row["status"] == "blocker" for row in checks)
    production_blockers = _production_blockers(new_profile)
    model = {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": (
            "offline_contract_ready_live_execution_blocked"
            if blocker_count == 0
            else "blocked_contract_review_required"
        ),
        "contract_blocker_count": blocker_count,
        "production_blocker_count": len(production_blockers),
        "offline_handoff_contract_ready": blocker_count == 0,
        "production_live_gap_closed": False,
        "profile_id": NEW_PROFILE_ID,
        "legacy_default_profile_id": LEGACY_PROFILE_ID,
        "legacy_default_preserved": profile_payload.get("default_profile_id") == LEGACY_PROFILE_ID,
        "algorithm_mode": new_profile.get("algorithm_mode"),
        "fit_input_contract": {
            "formula": FIT_FORMULA,
            "co2": "A_CO2_from_R_CO2_and_R0_CO2_T",
            "h2o": "A_H2O_from_R_H2O_and_R0_H2O_T",
            "temperature_source": "per_analyzer_chamber_T1",
            "pressure_sequence": "SENCO9_first",
            "temperature_coefficients": "SENCO7_SENCO8_neutral",
            "co2_zero_and_h2o_dry_anchor_are_separate": True,
        },
        "queue_contract": {
            "legacy_counts": {"co2": len(legacy_co2), "h2o": len(legacy_h2o)},
            "new_algorithm_counts": {"co2": len(new_co2), "h2o": len(new_h2o)},
            "co2_rows_content_sha256": _rows_sha256(new_co2) if new_co2 else "",
            "h2o_rows_content_sha256": _rows_sha256(new_h2o) if new_h2o else "",
            "co2_queue_runner": CO2_QUEUE_RUNNER,
            "h2o_queue_runner": H2O_QUEUE_RUNNER,
            "route_order": ["co2", "h2o"],
            "profile_declared_queue_source_is_not_consumed": new_model.get(
                "profile_declared_queue_source_is_not_consumed"
            ),
            "mature_point_execution_is_not_copied_or_modified": new_model.get(
                "mature_point_execution_is_not_copied_or_modified"
            ),
        },
        "runner_source_bindings": runner_sources,
        "authorization_requirements": _authorization_requirements(),
        "production_blockers": production_blockers,
        "checks": checks,
        "source_bindings": {
            "profile_path": _portable_path(profile_file, root),
            "profile_sha256": _sha256(profile_file),
            "mature_route_contract_json": _portable_path(mature_contract_file, root),
            "mature_route_contract_sha256": _sha256(mature_contract_file),
        },
        "execution_supported": False,
        "live_queue_execution_allowed": False,
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_sn_or_device_code": False,
        "writes_coefficients": False,
        "connects_postgresql": False,
        "database_written": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
        "_co2_rows": new_co2,
        "_h2o_rows": new_h2o,
    }
    return model


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(dict(row) for row in rows)


def write_v1_5_new_algorithm_mature_queue_live_handoff(
    model: Mapping[str, Any], output_dir: str | Path, *, repo_root: str | Path
) -> dict[str, Path]:
    output = Path(output_dir)
    root = Path(repo_root).resolve()
    output.mkdir(parents=True, exist_ok=True)
    co2_rows = list(model.get("_co2_rows") or [])
    h2o_rows = list(model.get("_h2o_rows") or [])
    paths = {
        "json": output / "v1_5_new_algorithm_mature_queue_live_handoff.json",
        "checks_csv": output / "v1_5_new_algorithm_mature_queue_live_handoff_checks.csv",
        "authorization_csv": output / "v1_5_new_algorithm_mature_queue_live_handoff_authorization_requirements.csv",
        "runner_hashes_csv": output / "v1_5_new_algorithm_mature_queue_runner_hashes.csv",
        "co2_queue_csv": output / "co2_new_algorithm_47_runner_queue.csv",
        "h2o_queue_csv": output / "h2o_new_algorithm_14_runner_queue.csv",
        "markdown": output / "V1_5_NEW_ALGORITHM_MATURE_QUEUE_LIVE_HANDOFF.md",
    }
    _write_csv(paths["co2_queue_csv"], co2_rows)
    _write_csv(paths["h2o_queue_csv"], h2o_rows)
    _write_csv(paths["checks_csv"], model.get("checks") or [])
    _write_csv(paths["authorization_csv"], model.get("authorization_requirements") or [])
    _write_csv(paths["runner_hashes_csv"], model.get("runner_source_bindings") or [])
    persisted = {key: value for key, value in model.items() if not key.startswith("_")}
    queue_contract = dict(persisted.get("queue_contract") or {})
    queue_contract.update(
        {
            "co2_queue_csv": _portable_path(paths["co2_queue_csv"], root),
            "h2o_queue_csv": _portable_path(paths["h2o_queue_csv"], root),
            "co2_queue_csv_sha256": _sha256(paths["co2_queue_csv"]),
            "h2o_queue_csv_sha256": _sha256(paths["h2o_queue_csv"]),
        }
    )
    persisted["queue_contract"] = queue_contract
    paths["json"].write_text(
        json.dumps(persisted, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    lines = [
        "# V1.5 New-Algorithm 47/14 Mature-Queue Live Handoff",
        "",
        "This is an offline contract and blocked live-handoff review. It does not execute the mature queues.",
        "",
        f"- overall_status: `{persisted.get('overall_status')}`",
        f"- offline_handoff_contract_ready: `{persisted.get('offline_handoff_contract_ready')}`",
        f"- production_live_gap_closed: `{persisted.get('production_live_gap_closed')}`",
        "- legacy production default: `45 CO2 / 13 H2O`",
        "- new algorithm candidate: `47 CO2 / 14 H2O`",
        f"- fitting input: `{FIT_FORMULA}`",
        f"- live_queue_execution_allowed: `{persisted.get('live_queue_execution_allowed')}`",
        "- Mature physical route: shared 0613/0620/0621 V1.5 contract; no 0624 or migration queue source is consumed.",
        "- CO2 zero gas and the H2O dry/low-water anchor remain separate physical evidence roles.",
        "",
        "## Production blockers",
        "",
    ]
    for row in persisted.get("production_blockers") or []:
        lines.append(f"- `{row.get('blocker')}`: {row.get('next_action')}")
    lines.extend(["", "## Contract checks", ""])
    for row in persisted.get("checks") or []:
        reasons = ";".join(row.get("reasons") or [])
        lines.append(f"- `{row.get('check_id')}`: `{row.get('status')}` {reasons}".rstrip())
    paths["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return paths


def build_v1_5_new_algorithm_mature_queue_live_handoff_blocked_executor(
    *, live_handoff_json: str | Path
) -> dict[str, Any]:
    recorded_source = Path(live_handoff_json)
    source = recorded_source.resolve()
    payload = _read_json(source)
    reasons: list[str] = []
    if payload.get("schema") != SCHEMA:
        reasons.append("live_handoff_schema_invalid")
    if payload.get("offline_handoff_contract_ready") is not True:
        reasons.append("offline_handoff_contract_not_ready")
    if int(payload.get("contract_blocker_count") or 0) != 0:
        reasons.append("offline_handoff_contract_has_blockers")
    if payload.get("live_queue_execution_allowed") is not False:
        reasons.append("source_live_queue_lock_not_false")
    if payload.get("production_live_gap_closed") is not False:
        reasons.append("source_live_gap_must_remain_open")
    return {
        "schema": BLOCKED_EXECUTOR_SCHEMA,
        "generated_at": _now(),
        "overall_status": (
            "blocked_live_queue_executor_not_implemented"
            if not reasons
            else "review_required_invalid_live_handoff_contract"
        ),
        "blocked_executor_ready": not reasons,
        "review_reasons": reasons,
        "source_live_handoff_json": str(recorded_source),
        "source_live_handoff_sha256": _sha256(source),
        "execution_supported": False,
        "execution_attempted": False,
        "would_execute": False,
        "live_queue_execution_allowed": False,
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_sn_or_device_code": False,
        "writes_coefficients": False,
        "connects_postgresql": False,
        "database_written": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }


def write_v1_5_new_algorithm_mature_queue_live_handoff_blocked_executor(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    paths = {
        "json": output / "v1_5_new_algorithm_mature_queue_live_handoff_blocked_executor.json",
        "markdown": output / "V1_5_NEW_ALGORITHM_MATURE_QUEUE_LIVE_HANDOFF_BLOCKED_EXECUTOR.md",
    }
    paths["json"].write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    lines = [
        "# V1.5 New-Algorithm Mature-Queue Live Handoff Blocked Executor",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- execution_supported: `{model.get('execution_supported')}`",
        f"- execution_attempted: `{model.get('execution_attempted')}`",
        f"- live_queue_execution_allowed: `{model.get('live_queue_execution_allowed')}`",
        "- No COM, route, analyzer write, PostgreSQL, release, or import action is available in this executor.",
    ]
    paths["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return paths


__all__ = [
    "BLOCKED_EXECUTOR_SCHEMA",
    "CONFIRMATION_TEXT",
    "FIT_FORMULA",
    "LEGACY_PROFILE_ID",
    "NEW_PROFILE_ID",
    "SCHEMA",
    "build_v1_5_new_algorithm_mature_queue_live_handoff",
    "build_v1_5_new_algorithm_mature_queue_live_handoff_blocked_executor",
    "write_v1_5_new_algorithm_mature_queue_live_handoff",
    "write_v1_5_new_algorithm_mature_queue_live_handoff_blocked_executor",
]
