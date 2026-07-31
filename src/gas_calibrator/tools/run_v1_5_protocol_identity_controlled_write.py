"""Single-port controlled V1.5 analyzer protocol-ID writer.

The default path is an offline preflight. Real execution is intentionally
narrow: one FTDI-bound analyzer, complete read-only backup, two confirmations,
ACK-required ID write, two identity readbacks, and no route or coefficient
control.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional

from ..devices import GasAnalyzer
from ..v1_5.identity_authority_signature import (
    default_identity_authority_trust_store_path,
    verify_identity_authority_signature,
)
from ._analyzer_serial_pacing import (
    MIN_ANALYZER_SERIAL_COMMAND_GAP_S,
    _enforce_serial_command_gap,
)
from .run_v1_5_analyzer_runtime_setup import _read_identity_snapshot, _read_sn


AUTHORIZATION_PHRASE = "I_AUTHORIZE_V1_5_SINGLE_PORT_PROTOCOL_ID_WRITE"
ISOLATION_PHRASE = "I_CONFIRM_SINGLE_COM_ISOLATED_AND_NO_GAS_FLOW"
PLAN_SCHEMA = "v1_5_protocol_identity_normalization_plan_v1"
BACKUP_SCHEMA = "v1_5_protocol_id_prewrite_backup_v1"
RESULT_SCHEMA = "v1_5_protocol_identity_controlled_write_result_v1"
UNIQUENESS_EVIDENCE_SCHEMA = "v1_5_protocol_identity_global_uniqueness_evidence_v1"
AUTHORITY_SOURCE_TYPES = frozenset(
    {
        "formal_identity_database_readonly_export",
        "controlled_asset_registry_readonly_export",
    }
)
ANALYZER_PORTS = frozenset(f"COM{number}" for number in range(35, 43))
_ID_RE = re.compile(r"\d{3}")
_SN_RE = re.compile(r"\d{8}")


def _load_json(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"JSON object required: {path}")
    return dict(payload)


def _normalize_id(value: Any) -> str:
    text = str(value or "").strip()
    return f"{int(text):03d}" if text.isdigit() else text.upper()


def _sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _timezone_aware_iso(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def _test_fixture_source(path: Path) -> bool:
    """Return true only for a resolved path inside a tests/fixtures tree."""

    parts = [part.casefold() for part in path.parts]
    return any(
        parts[index : index + 2] == ["tests", "fixtures"]
        for index in range(len(parts) - 1)
    )


def _candidate_rows(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in plan.get("rows") or []
        if isinstance(row, Mapping)
        and str(row.get("action") or "").startswith(
            "initialize_sn_then_change_protocol_id"
        )
    ]


def _inventory_row(
    inventory: Mapping[str, Any], *, port: str, usb_serial_number: str
) -> Optional[dict[str, Any]]:
    matches = [
        dict(row)
        for row in inventory.get("analyzers") or []
        if isinstance(row, Mapping)
        and str(row.get("port") or "").upper() == port.upper()
        and str(row.get("usb_serial_number") or "").upper() == usb_serial_number.upper()
    ]
    return matches[0] if len(matches) == 1 else None


def _backup_groups(backup: Mapping[str, Any]) -> set[int]:
    raw = backup.get("getco_groups")
    if not isinstance(raw, Mapping):
        return set()
    groups: set[int] = set()
    for key, value in raw.items():
        try:
            group = int(key)
        except Exception:
            continue
        if isinstance(value, (list, tuple)) and value:
            groups.add(group)
    return groups


def _runtime_settings_complete(value: Any) -> bool:
    """Reject placeholder/null settings in a nominally complete backup."""

    if not isinstance(value, Mapping):
        return False
    if value.get("mode") not in {1, 2}:
        return False
    if not isinstance(value.get("active_send"), bool):
        return False
    for key in ("ftd_hz", "average1", "average2"):
        setting = value.get(key)
        if isinstance(setting, bool) or not isinstance(setting, int) or setting <= 0:
            return False
    return True


def build_preflight(
    plan: Mapping[str, Any],
    inventory: Mapping[str, Any],
    backup: Mapping[str, Any] | None = None,
    *,
    require_trusted_signature: bool = True,
    trust_store_path: str | Path | None = None,
) -> dict[str, Any]:
    blockers: list[str] = []
    if plan.get("schema_version") != PLAN_SCHEMA:
        blockers.append("plan_schema_invalid")
    if plan.get("execution_allowed") is not True:
        blockers.append("plan_execution_not_allowed")
    if plan.get("status") != "approved_single_device_write":
        blockers.append("plan_not_approved_single_device_write")

    approval = dict(plan.get("approval") or {})
    if approval.get("global_sn_unique") is not True:
        blockers.append("global_sn_uniqueness_not_approved")
    if approval.get("global_protocol_id_unique") is not True:
        blockers.append("global_protocol_id_uniqueness_not_approved")
    if not str(approval.get("approved_by") or "").strip():
        blockers.append("approved_by_missing")
    if not str(approval.get("approved_at") or "").strip():
        blockers.append("approved_at_missing")
    uniqueness = dict(plan.get("global_uniqueness_evidence") or {})
    if uniqueness.get("candidate_sn_absent") is not True:
        blockers.append("global_uniqueness_evidence_sn_absent_missing")
    if uniqueness.get("candidate_protocol_id_absent") is not True:
        blockers.append("global_uniqueness_evidence_protocol_id_absent_missing")
    uniqueness_source = str(uniqueness.get("source") or "").strip()
    uniqueness_sha256 = str(uniqueness.get("sha256") or "").strip()
    uniqueness_payload: dict[str, Any] = {}
    authority_records: list[dict[str, Any]] = []
    uniqueness_validation: dict[str, Any] = {
        "source": uniqueness_source,
        "declared_sha256": uniqueness_sha256,
        "source_available": False,
        "actual_sha256": "",
        "sha256_matches": False,
        "json_object": False,
        "schema_valid": False,
        "status_ready": False,
        "scope_complete": False,
        "candidate_sn_absent": False,
        "candidate_protocol_id_absent": False,
        "test_fixture_forbidden": False,
        "test_fixture_marker_explicit_false": False,
        "test_fixture_path_forbidden": False,
        "authority_valid": False,
        "authority_source_type": "",
        "authority_source_system": "",
        "authority_exported_at": "",
        "authority_read_only_export": False,
        "scope_includes_powered_devices": False,
        "scope_includes_unpowered_devices": False,
        "scope_includes_silent_ports": False,
        "authority_record_count": 0,
        "authority_records_valid": False,
        "authority_sn_codes_unique": False,
        "authority_protocol_ids_unique": False,
        "derived_candidate_sn_absent": None,
        "derived_candidate_protocol_id_absent": None,
        "trusted_authority_signature": {
            "required": bool(require_trusted_signature),
            "valid": False,
            "status": "pending",
            "blockers": [],
        },
    }
    if not uniqueness_source:
        blockers.append("global_uniqueness_evidence_source_missing")
    if not re.fullmatch(r"[0-9a-fA-F]{64}", uniqueness_sha256):
        blockers.append("global_uniqueness_evidence_sha256_invalid")
    elif uniqueness_source:
        try:
            source_path = Path(uniqueness_source).resolve(strict=True)
            if not source_path.is_file():
                raise FileNotFoundError(uniqueness_source)
        except (OSError, RuntimeError):
            blockers.append("global_uniqueness_evidence_source_unavailable")
        else:
            uniqueness_validation["source_available"] = True
            fixture_source = _test_fixture_source(source_path)
            uniqueness_validation["test_fixture_path_forbidden"] = fixture_source
            if fixture_source:
                blockers.append(
                    "global_uniqueness_evidence_test_fixture_path_forbidden"
                )
            actual_sha256 = _sha256_file(source_path).lower()
            uniqueness_validation["actual_sha256"] = actual_sha256
            uniqueness_validation["sha256_matches"] = (
                actual_sha256 == uniqueness_sha256.lower()
            )
            if not uniqueness_validation["sha256_matches"]:
                blockers.append("global_uniqueness_evidence_sha256_mismatch")
            else:
                try:
                    uniqueness_payload = _load_json(source_path)
                except (OSError, ValueError):
                    blockers.append("global_uniqueness_evidence_source_json_invalid")
                else:
                    uniqueness_validation.update(
                        {
                            "json_object": True,
                            "schema_valid": uniqueness_payload.get("schema_version")
                            == UNIQUENESS_EVIDENCE_SCHEMA,
                            "status_ready": uniqueness_payload.get("overall_status")
                            == "ready_global_scope_complete",
                            "scope_complete": uniqueness_payload.get("scope_complete")
                            is True,
                            "candidate_sn_absent": uniqueness_payload.get(
                                "candidate_sn_absent"
                            )
                            is True,
                            "candidate_protocol_id_absent": uniqueness_payload.get(
                                "candidate_protocol_id_absent"
                            )
                            is True,
                            "test_fixture_forbidden": uniqueness_payload.get(
                                "test_fixture_only"
                            )
                            is not False,
                            "test_fixture_marker_explicit_false": (
                                uniqueness_payload.get("test_fixture_only") is False
                            ),
                        }
                    )
                    if (
                        uniqueness_payload.get("schema_version")
                        != UNIQUENESS_EVIDENCE_SCHEMA
                    ):
                        blockers.append("global_uniqueness_evidence_schema_invalid")
                    if (
                        uniqueness_payload.get("overall_status")
                        != "ready_global_scope_complete"
                    ):
                        blockers.append("global_uniqueness_evidence_status_not_ready")
                    if uniqueness_payload.get("test_fixture_only") is not False:
                        blockers.append(
                            "global_uniqueness_evidence_test_fixture_forbidden"
                        )
                    if uniqueness_payload.get("scope_complete") is not True:
                        blockers.append("global_uniqueness_evidence_scope_incomplete")
                    if uniqueness_payload.get("candidate_sn_absent") is not True:
                        blockers.append(
                            "global_uniqueness_evidence_source_sn_absence_not_confirmed"
                        )
                    if (
                        uniqueness_payload.get("candidate_protocol_id_absent")
                        is not True
                    ):
                        blockers.append(
                            "global_uniqueness_evidence_source_protocol_id_absence_not_confirmed"
                        )
                    authority = uniqueness_payload.get("authority")
                    if not isinstance(authority, Mapping):
                        blockers.append("global_uniqueness_evidence_authority_missing")
                    else:
                        source_type = str(authority.get("source_type") or "").strip()
                        source_system = str(
                            authority.get("source_system") or ""
                        ).strip()
                        exported_at = str(authority.get("exported_at") or "").strip()
                        uniqueness_validation.update(
                            {
                                "authority_source_type": source_type,
                                "authority_source_system": source_system,
                                "authority_exported_at": exported_at,
                                "authority_read_only_export": authority.get(
                                    "read_only_export"
                                )
                                is True,
                            }
                        )
                        if source_type not in AUTHORITY_SOURCE_TYPES:
                            blockers.append(
                                "global_uniqueness_evidence_authority_source_type_invalid"
                            )
                        if not source_system:
                            blockers.append(
                                "global_uniqueness_evidence_authority_source_system_missing"
                            )
                        if not str(authority.get("exported_by") or "").strip():
                            blockers.append(
                                "global_uniqueness_evidence_authority_exported_by_missing"
                            )
                        if not _timezone_aware_iso(exported_at):
                            blockers.append(
                                "global_uniqueness_evidence_authority_exported_at_invalid"
                            )
                        if authority.get("read_only_export") is not True:
                            blockers.append(
                                "global_uniqueness_evidence_authority_not_read_only"
                            )
                        if authority.get("database_written") is not False:
                            blockers.append(
                                "global_uniqueness_evidence_authority_database_write_boundary_invalid"
                            )

                    scope = uniqueness_payload.get("scope")
                    if not isinstance(scope, Mapping):
                        blockers.append(
                            "global_uniqueness_evidence_scope_contract_missing"
                        )
                        scope_record_count = 0
                    else:
                        scope_record_count = scope.get("record_count")
                        uniqueness_validation.update(
                            {
                                "scope_includes_powered_devices": scope.get(
                                    "includes_powered_devices"
                                )
                                is True,
                                "scope_includes_unpowered_devices": scope.get(
                                    "includes_unpowered_devices"
                                )
                                is True,
                                "scope_includes_silent_ports": scope.get(
                                    "includes_silent_ports"
                                )
                                is True,
                            }
                        )
                        if scope.get("scope_complete") is not True:
                            blockers.append(
                                "global_uniqueness_evidence_scope_contract_incomplete"
                            )
                        for field in (
                            "includes_powered_devices",
                            "includes_unpowered_devices",
                            "includes_silent_ports",
                        ):
                            if scope.get(field) is not True:
                                blockers.append(
                                    f"global_uniqueness_evidence_scope_{field}_missing"
                                )
                        if (
                            isinstance(scope_record_count, bool)
                            or not isinstance(scope_record_count, int)
                            or scope_record_count <= 0
                        ):
                            blockers.append(
                                "global_uniqueness_evidence_scope_record_count_invalid"
                            )

                    raw_records = uniqueness_payload.get("records")
                    if not isinstance(raw_records, list) or not raw_records:
                        blockers.append("global_uniqueness_evidence_records_missing")
                    else:
                        authority_records = [
                            dict(row) for row in raw_records if isinstance(row, Mapping)
                        ]
                        uniqueness_validation["authority_record_count"] = len(
                            authority_records
                        )
                        if len(authority_records) != len(raw_records):
                            blockers.append(
                                "global_uniqueness_evidence_records_non_object_row"
                            )
                        if (
                            isinstance(scope_record_count, int)
                            and not isinstance(scope_record_count, bool)
                            and len(raw_records) != scope_record_count
                        ):
                            blockers.append(
                                "global_uniqueness_evidence_scope_record_count_mismatch"
                            )
                        asset_keys: list[str] = []
                        sn_codes: list[str] = []
                        protocol_ids: list[str] = []
                        records_valid = True
                        for row in authority_records:
                            asset_key = str(row.get("asset_key") or "").strip()
                            sn_code = str(row.get("sn_code") or "").strip()
                            protocol_id = _normalize_id(row.get("protocol_device_id"))
                            lifecycle_status = str(
                                row.get("lifecycle_status") or ""
                            ).strip()
                            asset_keys.append(asset_key)
                            sn_codes.append(sn_code)
                            protocol_ids.append(protocol_id)
                            if (
                                not asset_key
                                or not _SN_RE.fullmatch(sn_code)
                                or not _ID_RE.fullmatch(protocol_id)
                                or not lifecycle_status
                            ):
                                records_valid = False
                        if not records_valid:
                            blockers.append(
                                "global_uniqueness_evidence_records_identity_invalid"
                            )
                        if len(asset_keys) != len(set(asset_keys)):
                            blockers.append(
                                "global_uniqueness_evidence_records_duplicate_asset_key"
                            )
                        sn_codes_unique = len(sn_codes) == len(set(sn_codes))
                        protocol_ids_unique = len(protocol_ids) == len(
                            set(protocol_ids)
                        )
                        if not sn_codes_unique:
                            blockers.append(
                                "global_uniqueness_evidence_records_duplicate_sn_code"
                            )
                        if not protocol_ids_unique:
                            blockers.append(
                                "global_uniqueness_evidence_records_duplicate_protocol_device_id"
                            )
                        uniqueness_validation["authority_sn_codes_unique"] = (
                            sn_codes_unique
                        )
                        uniqueness_validation["authority_protocol_ids_unique"] = (
                            protocol_ids_unique
                        )
                        uniqueness_validation["authority_records_valid"] = (
                            records_valid
                            and len(asset_keys) == len(set(asset_keys))
                            and sn_codes_unique
                            and protocol_ids_unique
                        )

    if require_trusted_signature:
        signature_validation = verify_identity_authority_signature(
            uniqueness_payload,
            trust_store_path=(
                trust_store_path or default_identity_authority_trust_store_path()
            ),
        )
        blockers.extend(signature_validation["blockers"])
    else:
        signature_validation = {
            "required": False,
            "valid": False,
            "status": "not_required_semantic_validation_only",
            "blockers": [],
        }
    uniqueness_validation["trusted_authority_signature"] = signature_validation

    candidates = _candidate_rows(plan)
    if len(candidates) != 1:
        blockers.append(f"single_write_candidate_required:{len(candidates)}")
        candidate: dict[str, Any] = {}
    else:
        candidate = candidates[0]
    port = str(candidate.get("port") or "").strip().upper()
    usb_serial = str(candidate.get("usb_serial_number") or "").strip()
    old_id = _normalize_id(candidate.get("observed_protocol_device_id"))
    new_id = _normalize_id(candidate.get("candidate_target_protocol_device_id"))
    required_sn = str(
        candidate.get("candidate_target_sn") or candidate.get("sn_code") or ""
    ).strip()
    pre_initialization_sn = str(candidate.get("sn_code") or "").strip()
    if not port:
        blockers.append("candidate_port_missing")
    elif port not in ANALYZER_PORTS:
        blockers.append("candidate_port_outside_analyzer_bank")
    if not usb_serial:
        blockers.append("candidate_usb_serial_missing")
    if not _ID_RE.fullmatch(old_id):
        blockers.append("candidate_old_protocol_id_invalid")
    if not _ID_RE.fullmatch(new_id):
        blockers.append("candidate_new_protocol_id_invalid")
    if old_id and new_id and old_id == new_id:
        blockers.append("candidate_protocol_id_unchanged")
    if not _SN_RE.fullmatch(required_sn) or required_sn == "00000000":
        blockers.append("candidate_required_sn_invalid")
    if not _SN_RE.fullmatch(pre_initialization_sn):
        blockers.append("candidate_pre_initialization_sn_invalid")
    if uniqueness_payload:
        uniqueness_validation["candidate_sn_matches_plan"] = (
            str(uniqueness_payload.get("candidate_sn") or "").strip() == required_sn
        )
        uniqueness_validation["candidate_protocol_id_matches_plan"] = (
            _normalize_id(uniqueness_payload.get("candidate_protocol_device_id"))
            == new_id
        )
        if not uniqueness_validation["candidate_sn_matches_plan"]:
            blockers.append("global_uniqueness_evidence_candidate_sn_mismatch")
        if not uniqueness_validation["candidate_protocol_id_matches_plan"]:
            blockers.append("global_uniqueness_evidence_candidate_protocol_id_mismatch")
        if uniqueness_payload.get("candidate_sn_absent") is not uniqueness.get(
            "candidate_sn_absent"
        ):
            blockers.append("global_uniqueness_evidence_plan_source_sn_disagreement")
        if uniqueness_payload.get("candidate_protocol_id_absent") is not uniqueness.get(
            "candidate_protocol_id_absent"
        ):
            blockers.append(
                "global_uniqueness_evidence_plan_source_protocol_id_disagreement"
            )
        if authority_records:
            derived_sn_absent = all(
                str(row.get("sn_code") or "").strip() != required_sn
                for row in authority_records
            )
            derived_protocol_id_absent = all(
                _normalize_id(row.get("protocol_device_id")) != new_id
                for row in authority_records
            )
            uniqueness_validation["derived_candidate_sn_absent"] = derived_sn_absent
            uniqueness_validation["derived_candidate_protocol_id_absent"] = (
                derived_protocol_id_absent
            )
            if not derived_sn_absent:
                blockers.append(
                    "global_uniqueness_evidence_candidate_sn_present_in_authority_records"
                )
            if not derived_protocol_id_absent:
                blockers.append(
                    "global_uniqueness_evidence_candidate_protocol_id_present_in_authority_records"
                )
            if uniqueness_payload.get("candidate_sn_absent") is not derived_sn_absent:
                blockers.append(
                    "global_uniqueness_evidence_source_derived_sn_disagreement"
                )
            if (
                uniqueness_payload.get("candidate_protocol_id_absent")
                is not derived_protocol_id_absent
            ):
                blockers.append(
                    "global_uniqueness_evidence_source_derived_protocol_id_disagreement"
                )
    else:
        uniqueness_validation["candidate_sn_matches_plan"] = False
        uniqueness_validation["candidate_protocol_id_matches_plan"] = False

    other_ids: set[str] = set()
    for row in plan.get("rows") or []:
        if not isinstance(row, Mapping) or dict(row) == candidate:
            continue
        value = _normalize_id(
            row.get("target_protocol_device_id")
            or row.get("observed_protocol_device_id")
        )
        if value:
            other_ids.add(value)
    if new_id and new_id in other_ids:
        blockers.append("candidate_new_protocol_id_collides_in_plan")

    live_row = _inventory_row(inventory, port=port, usb_serial_number=usb_serial)
    if live_row is None:
        blockers.append("candidate_not_uniquely_bound_in_sn_inventory")
    elif str(live_row.get("sn_code") or "").strip() != required_sn:
        blockers.append("candidate_target_sn_not_initialized_or_mismatch")
    elif live_row.get("sn_bound_valid") is not True:
        blockers.append("candidate_sn_not_valid_bound")

    backup_payload = dict(backup or {})
    if not backup_payload:
        blockers.append("prewrite_backup_missing")
    else:
        if backup_payload.get("schema_version") != BACKUP_SCHEMA:
            blockers.append("prewrite_backup_schema_invalid")
        if str(backup_payload.get("port") or "").upper() != port.upper():
            blockers.append("prewrite_backup_port_mismatch")
        if (
            str(backup_payload.get("usb_serial_number") or "").upper()
            != usb_serial.upper()
        ):
            blockers.append("prewrite_backup_usb_serial_mismatch")
        if str(backup_payload.get("sn_code") or "").strip() != pre_initialization_sn:
            blockers.append("prewrite_backup_sn_mismatch")
        if _normalize_id(backup_payload.get("protocol_device_id")) != old_id:
            blockers.append("prewrite_backup_protocol_id_mismatch")
        if backup_payload.get("identity_verified") is not True:
            blockers.append("prewrite_backup_identity_not_verified")
        if backup_payload.get("captured_read_only") is not True:
            blockers.append("prewrite_backup_not_read_only")
        if backup_payload.get("captured_before_persistent_identity_write") is not True:
            blockers.append("prewrite_backup_not_before_identity_write")
        if _backup_groups(backup_payload) != set(range(1, 10)):
            blockers.append("prewrite_backup_getco1_9_incomplete")
        runtime_settings = backup_payload.get("runtime_settings")
        if not isinstance(runtime_settings, Mapping) or not {
            "mode",
            "active_send",
            "ftd_hz",
            "average1",
            "average2",
        }.issubset(runtime_settings):
            blockers.append("prewrite_backup_runtime_settings_missing")
        elif not _runtime_settings_complete(runtime_settings):
            blockers.append("prewrite_backup_runtime_settings_unverified")
        safety = backup_payload.get("safety")
        if not isinstance(safety, Mapping) or not (
            safety.get("query_only") is True
            and safety.get("writes_sn") is False
            and safety.get("writes_device_id") is False
            and safety.get("writes_senco") is False
        ):
            blockers.append("prewrite_backup_no_write_evidence_invalid")

    uniqueness_validation["authority_valid"] = not any(
        blocker.startswith("global_uniqueness_evidence_authority")
        or blocker.startswith("global_uniqueness_evidence_scope_")
        or blocker.startswith("global_uniqueness_evidence_records_")
        for blocker in blockers
    )
    uniqueness_validation["valid"] = not any(
        blocker.startswith("global_uniqueness_evidence") for blocker in blockers
    )
    return {
        "schema_version": "v1_5_protocol_identity_write_preflight_v1",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "status": "ready" if not blockers else "blocked",
        "blockers": blockers,
        "candidate": {
            "port": port,
            "baud": int(candidate.get("baud", 115200) or 115200),
            "usb_serial_number": usb_serial,
            "required_sn": required_sn,
            "pre_initialization_sn": pre_initialization_sn,
            "old_protocol_device_id": old_id,
            "new_protocol_device_id": new_id,
        },
        "approval": approval,
        "global_uniqueness_evidence_validation": uniqueness_validation,
        "boundary": {
            "single_port_only": True,
            "default_no_write": True,
            "requires_repository_external_trust_store": True,
            "requires_trusted_authority_signature": True,
            "writes_sn": False,
            "writes_senco": False,
            "controls_water_or_gas_routes": False,
            "opens_dewpoint_meter": False,
            "controls_pressure": False,
            "runs_calibration": False,
        },
    }


def _hardware_serial_for_port(port: str) -> str:
    try:
        from serial.tools import list_ports

        for item in list_ports.comports():
            if str(item.device or "").upper() == str(port or "").upper():
                return str(item.serial_number or "").strip()
    except Exception:
        pass
    return ""


def _default_analyzer_factory(candidate: Mapping[str, Any]) -> GasAnalyzer:
    return GasAnalyzer(
        str(candidate["port"]),
        int(candidate.get("baud", 115200) or 115200),
        timeout=1.0,
        device_id=str(candidate["old_protocol_device_id"]),
    )


def execute_controlled_write(
    preflight: Mapping[str, Any],
    *,
    execute: bool = False,
    authorization_phrase: str = "",
    isolation_phrase: str = "",
    analyzer_factory: Callable[[Mapping[str, Any]], Any] = _default_analyzer_factory,
    hardware_serial_provider: Callable[[str], str] = _hardware_serial_for_port,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    candidate = dict(preflight.get("candidate") or {})
    result: dict[str, Any] = {
        "schema_version": RESULT_SCHEMA,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "execute": bool(execute),
        "status": "dry_run_ready"
        if preflight.get("status") == "ready"
        else "dry_run_blocked",
        "preflight": dict(preflight),
        "candidate": candidate,
        "events": [],
        "device_id_write_attempted": False,
        "device_id_write_acknowledged": False,
        "rollback_attempted": False,
        "rollback_confirmed": False,
        "writes_sn": False,
        "writes_senco": False,
        "controls_water_or_gas_routes": False,
        "opens_dewpoint_meter": False,
        "not_real_acceptance_evidence": True,
        "engineering_probe_only": True,
        "promotion_state": "blocked",
    }
    if not execute:
        return result
    if preflight.get("status") != "ready":
        result["status"] = "blocked_preflight"
        return result
    uniqueness_validation = dict(
        preflight.get("global_uniqueness_evidence_validation") or {}
    )
    signature_validation = dict(
        uniqueness_validation.get("trusted_authority_signature") or {}
    )
    if not (
        signature_validation.get("required") is True
        and signature_validation.get("valid") is True
        and signature_validation.get("status") == "verified"
    ):
        result["status"] = "blocked_trusted_authority_signature"
        return result
    if authorization_phrase != AUTHORIZATION_PHRASE:
        result["status"] = "blocked_authorization_phrase"
        return result
    if isolation_phrase != ISOLATION_PHRASE:
        result["status"] = "blocked_isolation_phrase"
        return result
    result["operator_confirmation_record"] = {
        "authorization_phrase_matched": True,
        "isolation_phrase_matched": True,
        "confirmed_at": datetime.now().isoformat(timespec="seconds"),
    }

    port = str(candidate.get("port") or "")
    expected_hardware_serial = str(candidate.get("usb_serial_number") or "")
    actual_hardware_serial = str(hardware_serial_provider(port) or "")
    result["hardware_identity"] = {
        "port": port,
        "expected_usb_serial_number": expected_hardware_serial,
        "actual_usb_serial_number": actual_hardware_serial,
        "match": actual_hardware_serial.upper() == expected_hardware_serial.upper(),
    }
    if not result["hardware_identity"]["match"]:
        result["status"] = "blocked_hardware_identity_mismatch"
        return result

    old_id = str(candidate["old_protocol_device_id"])
    new_id = str(candidate["new_protocol_device_id"])
    required_sn = str(candidate["required_sn"])
    analyzer: Any = None
    pacing_events: list[dict[str, Any]] = []
    try:
        analyzer = analyzer_factory(candidate)
        analyzer.open()
        with _enforce_serial_command_gap(
            analyzer,
            MIN_ANALYZER_SERIAL_COMMAND_GAP_S,
            sleep_fn=sleep_fn,
        ) as pacing_events:
            pre_sn, pre_sn_raw = _read_sn(analyzer, timeout_s=1.2, attempts=1)
            pre_identity = _read_identity_snapshot(analyzer, prefer_stream=True) or {}
            result["prewrite_readback"] = {
                "sn_code": pre_sn or "",
                "sn_raw": pre_sn_raw,
                "protocol_device_id": _normalize_id(pre_identity.get("id")),
                "identity_raw": pre_identity.get("raw", ""),
            }
            if pre_sn != required_sn or _normalize_id(pre_identity.get("id")) != old_id:
                result["status"] = "blocked_live_prewrite_identity_mismatch"
                return result

            result["device_id_write_attempted"] = True
            acknowledged = bool(
                analyzer.set_device_id_with_ack(new_id, require_ack=True)
            )
            result["device_id_write_acknowledged"] = acknowledged
            if not acknowledged:
                result["status"] = "failed_write_ack_unknown_manual_recovery_required"
                return result

            post_identities = [
                _read_identity_snapshot(analyzer, prefer_stream=True) or {},
                _read_identity_snapshot(analyzer, prefer_stream=True) or {},
            ]
            post_ids = [_normalize_id(item.get("id")) for item in post_identities]
            post_sn, post_sn_raw = _read_sn(analyzer, timeout_s=1.2, attempts=1)
            result["postwrite_readback"] = {
                "protocol_device_ids": post_ids,
                "identity_raw": [item.get("raw", "") for item in post_identities],
                "sn_code": post_sn or "",
                "sn_raw": post_sn_raw,
            }
            if post_ids == [new_id, new_id] and post_sn == required_sn:
                result["status"] = "success"
                result["identity_change_confirmed"] = True
                return result

            new_id_observed = new_id in post_ids
            rollback_authorized = bool(
                dict(preflight.get("approval") or {}).get("rollback_authorized")
            )
            if new_id_observed and rollback_authorized:
                result["rollback_attempted"] = True
                rollback_ack = bool(
                    analyzer.set_device_id_with_ack(old_id, require_ack=True)
                )
                rollback_identities = [
                    _read_identity_snapshot(analyzer, prefer_stream=True) or {},
                    _read_identity_snapshot(analyzer, prefer_stream=True) or {},
                ]
                rollback_ids = [
                    _normalize_id(item.get("id")) for item in rollback_identities
                ]
                result["rollback_readback"] = {
                    "acknowledged": rollback_ack,
                    "protocol_device_ids": rollback_ids,
                }
                result["rollback_confirmed"] = rollback_ack and rollback_ids == [
                    old_id,
                    old_id,
                ]
                result["status"] = (
                    "failed_postwrite_verification_rolled_back"
                    if result["rollback_confirmed"]
                    else "failed_postwrite_verification_manual_recovery_required"
                )
            else:
                result["status"] = (
                    "failed_postwrite_verification_no_change_observed"
                    if not new_id_observed
                    else "failed_postwrite_verification_manual_recovery_required"
                )
            return result
    except Exception as exc:
        result["status"] = "error_manual_recovery_required"
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result
    finally:
        try:
            result["serial_command_pacing_events"] = list(pacing_events)
        except Exception:
            result["serial_command_pacing_events"] = []
        try:
            if analyzer is not None:
                analyzer.close()
        except Exception:
            pass


def _write_result(path: str | Path, result: Mapping[str, Any]) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return destination


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Single-port V1.5 controlled analyzer protocol-ID writer."
    )
    parser.add_argument("--plan-json", required=True)
    parser.add_argument("--sn-inventory-json", required=True)
    parser.add_argument("--prewrite-backup-json", default="")
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--operator-confirm", default="")
    parser.add_argument("--isolation-confirm", default="")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        plan = _load_json(args.plan_json)
        inventory = _load_json(args.sn_inventory_json)
        backup = (
            _load_json(args.prewrite_backup_json) if args.prewrite_backup_json else None
        )
        preflight = build_preflight(plan, inventory, backup)
        result = execute_controlled_write(
            preflight,
            execute=bool(args.execute),
            authorization_phrase=str(args.operator_confirm or ""),
            isolation_phrase=str(args.isolation_confirm or ""),
        )
        output = _write_result(args.output_json, result)
        print(
            json.dumps(
                {
                    "status": result.get("status"),
                    "execute": result.get("execute"),
                    "blockers": preflight.get("blockers"),
                    "output_json": str(output),
                },
                ensure_ascii=False,
                indent=2,
            ),
            flush=True,
        )
        return 0 if result.get("status") in {"dry_run_ready", "success"} else 1
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
