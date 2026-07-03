"""Offline V1.5 SENCOA/SENCOB writer design review.

This module only exports reviewer-facing design artifacts. It does not open COM
ports, send commands, or write coefficients.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

from .v1_5_algorithm_route_profiles import load_v1_5_algorithm_route_profiles


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: List[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows([dict(row) for row in rows])


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")


def _profile_by_id(config: Mapping[str, Any], profile_id: str) -> Mapping[str, Any]:
    for profile in config.get("profiles", []):
        if str(profile.get("profile_id") or "") == profile_id:
            return profile
    raise ValueError(f"Profile not found: {profile_id}")


def _join_values(values: Any) -> str:
    if isinstance(values, (list, tuple)):
        return ";".join(str(item) for item in values)
    return str(values or "")


def _slot_names(width: int) -> str:
    return ";".join(f"c{idx}" for idx in range(max(0, int(width))))


def build_v1_5_sencoa_sencob_writer_design_review(
    profile_path: str | Path,
    *,
    profile_id: str = "absorption_ratio_shadow",
) -> Dict[str, Any]:
    """Build an offline no-write design review for future SENCOA/SENCOB writers."""

    config = load_v1_5_algorithm_route_profiles(profile_path)
    profile = _profile_by_id(config, profile_id)
    shared = config.get("shared_route_contract", {})
    contract = profile.get("r0_write_contract", {})
    payload_width = int(contract.get("payload_width") or 4)
    min_gap_s = max(1.0, float(contract.get("minimum_serial_command_gap_s") or 1.0))

    payload_rows: List[Dict[str, Any]] = []
    for item in contract.get("components", []):
        group = str(item.get("coefficient_group") or "").strip().upper()
        readback = str(item.get("readback_group") or "").strip().upper()
        payload_rows.append(
            {
                "profile_id": profile_id,
                "component": item.get("component"),
                "coefficient_group": group,
                "readback_group": readback,
                "physical_quantity": item.get("physical_quantity"),
                "r0_source": item.get("r0_source"),
                "payload_width": payload_width,
                "payload_slot_names": _slot_names(payload_width),
                "payload_value_format": "finite_float_coefficients_preserve_precision_scientific_or_decimal",
                "write_command_template": f"{group},YGAS,<target>," + ",".join(
                    f"<c{idx}>" for idx in range(payload_width)
                ),
                "readback_command_template": f"{readback},YGAS,<target>",
                "target_policy": "prefer_device_id_after_identity_binding; FFF only after operator-reviewed broadcast scope",
                "command_gap_min_s": min_gap_s,
                "requires_mode": "MODE2",
                "controlled_writer_status": item.get("controlled_writer_status"),
                "status": "design_only_blocked_no_real_writer",
                "fit_input_equation": item.get("fit_input_equation"),
                "payload_contract": item.get("payload_contract"),
                "notes": item.get("notes"),
            }
        )

    snapshot_rows = [
        {
            "step": "identity_binding",
            "required": True,
            "artifact": "runtime_identity_bound_config.json",
            "contents": "port;label;device_id;sn_code;algorithm_profile",
            "purpose": "bind commands to the intended analyzer before any coefficient read or write",
        },
        {
            "step": "old_r0_snapshot",
            "required": True,
            "artifact": "old_getcoa_getcob_snapshot.json",
            "contents": "GETCOA_before;GETCOB_before;raw_lines;timestamps;sha256",
            "purpose": "rollback source of truth for R0(T) coefficient groups",
        },
        {
            "step": "old_main_chain_snapshot",
            "required": True,
            "artifact": "old_main_chain_snapshot.json",
            "contents": "GETCO1;GETCO3;GETCO2;GETCO4;GETCO5;GETCO6",
            "purpose": "prove R0 writes are not mixed with concentration-chain changes",
        },
        {
            "step": "payload_review_snapshot",
            "required": True,
            "artifact": "sencoa_sencob_payload_review.csv",
            "contents": "reviewed SENCOA/SENCOB target rows and source model hashes",
            "purpose": "make the future writer consume reviewed payloads only",
        },
    ]

    rollback_rows = [
        {
            "trigger": "write_ack_missing_or_readback_mismatch",
            "action": "restore changed coefficient groups from old_getcoa_getcob_snapshot.json",
            "order": "reverse_changed_order",
            "verification": "read GETCOA/GETCOB until old values match within reviewed tolerance",
            "continue_policy": "stop_after_rollback_attempt_and_mark_device_failed",
        },
        {
            "trigger": "SENCOA_success_then_SENCOB_failure",
            "action": "restore SENCOA then verify GETCOA; preserve old GETCOB if unchanged",
            "order": "rollback_changed_group_only",
            "verification": "GETCOA readback equals pre-write snapshot",
            "continue_policy": "do_not_attempt_partial_production_acceptance",
        },
        {
            "trigger": "post_write_independent_reverification_failure",
            "action": "do not auto-rollback inside preflight; require reviewer decision using readback and route evidence",
            "order": "reviewer_controlled",
            "verification": "separate no-write CO2/H2O reverification package",
            "continue_policy": "separate write success from validation-environment failure",
        },
    ]

    preflight_rows = [
        {
            "gate": "offline_design_only",
            "status": "pass",
            "required": True,
            "evidence": "manifest no_write=true; opens_com_ports=false; writes_coefficients=false",
        },
        {
            "gate": "real_writer_exists",
            "status": "blocked",
            "required": True,
            "evidence": contract.get("writer_implementation_status", "missing_real_writer_design_only"),
        },
        {
            "gate": "serial_command_gap",
            "status": "pass",
            "required": True,
            "evidence": f"minimum_serial_command_gap_s={min_gap_s:g}",
        },
        {
            "gate": "old_snapshot_required",
            "status": "planned",
            "required": True,
            "evidence": "GETCOA/GETCOB snapshot required before future write unlock",
        },
        {
            "gate": "rollback_plan_required",
            "status": "planned",
            "required": True,
            "evidence": "rollback rows generated; future writer must implement readback-verified rollback",
        },
        {
            "gate": "route_and_sampling_untouched",
            "status": "pass",
            "required": True,
            "evidence": "design package does not modify formal CO2/H2O runners or sampling entrypoints",
        },
        {
            "gate": "independent_reverification_required",
            "status": "planned",
            "required": True,
            "evidence": "future write cannot be accepted without separate no-write CO2/H2O reverification",
        },
    ]

    manifest = {
        "schema_version": 1,
        "generated_at": _now(),
        "profile_id": profile_id,
        "design_scope": "sencoa_sencob_r0_writer_design_review",
        "no_write": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "not_real_acceptance_evidence": True,
        "writer_status": "design_only_no_real_writer",
        "production_state": "blocked",
        "minimum_serial_command_gap_s": min_gap_s,
        "mode_required": "MODE2",
        "active_upload_hz": shared.get("analyzer_runtime", {}).get("active_upload_hz"),
        "offline_writer_design_tool": contract.get("offline_writer_design_tool"),
        "write_requires": _join_values(contract.get("write_requires")),
    }

    return {
        "manifest": manifest,
        "payload_contracts": payload_rows,
        "snapshot_plan": snapshot_rows,
        "rollback_plan": rollback_rows,
        "no_write_preflight": preflight_rows,
    }


def write_v1_5_sencoa_sencob_writer_design_review(
    profile_path: str | Path,
    output_dir: str | Path,
    *,
    profile_id: str = "absorption_ratio_shadow",
) -> Dict[str, str]:
    tables = build_v1_5_sencoa_sencob_writer_design_review(
        profile_path,
        profile_id=profile_id,
    )
    out = Path(output_dir)
    outputs = {
        "manifest": out / "v1_5_sencoa_sencob_writer_design_manifest.json",
        "payload_contracts": out / "v1_5_sencoa_sencob_payload_contracts.csv",
        "snapshot_plan": out / "v1_5_sencoa_sencob_snapshot_plan.csv",
        "rollback_plan": out / "v1_5_sencoa_sencob_rollback_plan.csv",
        "no_write_preflight": out / "v1_5_sencoa_sencob_no_write_preflight.csv",
        "summary": out / "V1_5_SENCOA_SENCOB_WRITER_DESIGN_REVIEW.md",
    }
    _write_json(outputs["manifest"], tables["manifest"])
    _write_csv(outputs["payload_contracts"], tables["payload_contracts"])
    _write_csv(outputs["snapshot_plan"], tables["snapshot_plan"])
    _write_csv(outputs["rollback_plan"], tables["rollback_plan"])
    _write_csv(outputs["no_write_preflight"], tables["no_write_preflight"])
    summary = [
        "# V1.5 SENCOA/SENCOB writer design review",
        "",
        "This is an offline no-write design review. It does not implement a real writer.",
        "",
        "- SENCOA is reserved for `R0_CO2(T)` and must be read back through GETCOA.",
        "- SENCOB is reserved for `R0_H2O(T)` and must be read back through GETCOB.",
        "- Payloads are four finite float coefficients in the reviewed order.",
        "- Future real writes must use MODE2 and a serial command gap of at least 1 second.",
        "- Future real writes require old GETCOA/GETCOB snapshots, readback verification, rollback, and independent CO2/H2O reverification.",
        "- Current status remains blocked because the controlled SENCOA/SENCOB writer does not exist yet.",
    ]
    outputs["summary"].write_text("\n".join(summary) + "\n", encoding="utf-8")
    return {key: str(path) for key, path in outputs.items()}
