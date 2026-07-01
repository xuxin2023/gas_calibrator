"""Offline PostgreSQL 18 database contract for V1.5 production evidence.

This module is intentionally not a database client. It builds a reviewer-facing
schema/insert preview from the V1.5 storage models and evidence-registry
contract without opening PostgreSQL or mutating production data.
"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from sqlalchemy import Index, UniqueConstraint

from ..storage.v1_5_evidence.repository import TABLE_COLUMNS as EVIDENCE_REGISTRY_TABLE_COLUMNS
from ..storage.v1_5_evidence.repository import TABLE_NAMES as EVIDENCE_REGISTRY_TABLE_NAMES
from ..v2.storage.models import Base


SCHEMA = "v1_5_formal_database_dry_run_contract_v1"
PRODUCTION_BACKEND = "postgresql"
PRODUCTION_POSTGRESQL_MAJOR = 18
SN_RE = re.compile(r"\d{8}")
PROTOCOL_ID_RE = re.compile(r"\d{3}")
CORE_STORAGE_REQUIRED_TABLES = (
    "sensors",
    "sensor_identity_aliases",
    "runs",
    "points",
    "samples",
    "measurement_frames",
    "qc_results",
    "fit_results",
    "coefficient_versions",
    "device_events",
    "alarms_incidents",
)
CORE_REQUIRED_CONSTRAINTS = {
    "sensors": {
        "unique": {"uq_sensors_sn_code", "uq_sensors_device_code", "uq_sensors_device_key"},
        "indexes": {"ix_sensors_legacy_identity", "ix_sensors_sn_code", "ix_sensors_device_code"},
    },
    "sensor_identity_aliases": {
        "unique": {"uq_sensor_identity_alias_source"},
        "indexes": {"ix_sensor_identity_alias_lookup"},
    },
    "points": {"unique": {"uq_points_run_sequence"}, "indexes": {"ix_points_run_id"}},
    "samples": {"unique": {"uq_samples_point_analyzer_index"}, "indexes": {"ix_samples_sensor_id"}},
    "measurement_frames": {
        "unique": {"uq_measurement_frames_natural_key"},
        "indexes": {"ix_measurement_frames_sensor_id", "ix_measurement_frames_run_time"},
    },
    "fit_results": {"unique": {"uq_fit_results_run_analyzer_algorithm"}, "indexes": {"ix_fit_results_sensor_id"}},
    "coefficient_versions": {
        "unique": {"uq_coefficient_versions_analyzer_version"},
        "indexes": {"ix_coefficient_versions_sensor_id"},
    },
}
EVIDENCE_REGISTRY_REQUIRED_TABLES = (
    "runs",
    "devices",
    "run_devices",
    "standard_gases",
    "reference_certificates",
    "calibration_points",
    "sample_files",
    "qc_results",
    "coefficient_snapshots",
    "coefficient_candidates",
    "coefficient_write_events",
    "reports",
    "audit_events",
    "evidence_integrity_checks",
)


@dataclass(frozen=True)
class FormalDatabaseDryRunCheck:
    check: str
    status: str
    evidence_role: str
    reasons: tuple[str, ...]
    physical_meaning: str
    next_action: str
    details: Mapping[str, Any]

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")


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
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def _valid_sn(value: Any) -> str:
    text = str(value or "").strip()
    return text if SN_RE.fullmatch(text) and text != "00000000" else ""


def _valid_protocol_id(value: Any) -> str:
    text = str(value or "").strip()
    return text if PROTOCOL_ID_RE.fullmatch(text) else ""


def _normalize_planned_devices(values: Sequence[str | Mapping[str, Any]] | None) -> list[dict[str, str]]:
    devices: list[dict[str, str]] = []
    for index, value in enumerate(values or ()):
        if isinstance(value, Mapping):
            sn_code = _valid_sn(value.get("sn_code") or value.get("device_code"))
            device_code = _valid_sn(value.get("device_code") or sn_code)
            protocol_id = _valid_protocol_id(value.get("protocol_device_id") or value.get("device_id"))
            slot = str(value.get("slot") or value.get("slot_id") or f"GA{index + 1:02d}").strip()
            port = str(value.get("port") or "").strip()
        else:
            parts = [part.strip() for part in str(value or "").replace("=", ",").split(",")]
            sn_code = _valid_sn(parts[0] if parts else "")
            device_code = sn_code
            protocol_id = _valid_protocol_id(parts[1] if len(parts) > 1 else "")
            slot = f"GA{index + 1:02d}"
            port = ""
        devices.append(
            {
                "slot": slot,
                "sn_code": sn_code,
                "device_code": device_code,
                "protocol_device_id": protocol_id,
                "port": port,
            }
        )
    return devices


def _constraint_names(table_name: str) -> dict[str, list[str]]:
    table = Base.metadata.tables.get(table_name)
    if table is None:
        return {"unique": [], "indexes": []}
    unique = sorted(
        str(item.name)
        for item in table.constraints
        if isinstance(item, UniqueConstraint) and item.name
    )
    indexes = sorted(str(item.name) for item in table.indexes if isinstance(item, Index) and item.name)
    return {"unique": unique, "indexes": indexes}


def _core_storage_contract() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for table_name in CORE_STORAGE_REQUIRED_TABLES:
        table = Base.metadata.tables.get(table_name)
        names = _constraint_names(table_name)
        rows.append(
            {
                "database_schema": "core_storage",
                "table_name": table_name,
                "present": table is not None,
                "columns": ";".join(table.columns.keys()) if table is not None else "",
                "unique_constraints": ";".join(names["unique"]),
                "indexes": ";".join(names["indexes"]),
                "physical_role": _core_table_role(table_name),
            }
        )
    return rows


def _core_table_role(table_name: str) -> str:
    roles = {
        "sensors": "canonical analyzer identity table; sn_code/device_code are production primary identity fields",
        "sensor_identity_aliases": "compatibility aliases including protocol device ID and historical analyzer serials",
        "runs": "formal calibration/run session metadata",
        "points": "physical CO2/H2O calibration point schedule and status",
        "samples": "per-point summarized analyzer sample rows",
        "measurement_frames": "raw/parsed frame-level evidence with natural key per point/analyzer/sample timestamp",
        "qc_results": "programmatic quality gates and reject/pass reasons",
        "fit_results": "candidate fit outputs before controlled write review",
        "coefficient_versions": "versioned coefficient payloads and approval/deployment state",
        "device_events": "GETCO/SN/runtime/CHECK/writeback/archive sidecar events",
        "alarms_incidents": "operator/reviewer-visible warnings and failure records",
    }
    return roles.get(table_name, "")


def _evidence_registry_contract() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for table_name in EVIDENCE_REGISTRY_REQUIRED_TABLES:
        rows.append(
            {
                "database_schema": "v1_5_evidence",
                "table_name": table_name,
                "present": table_name in EVIDENCE_REGISTRY_TABLE_COLUMNS,
                "columns": ";".join(EVIDENCE_REGISTRY_TABLE_COLUMNS.get(table_name, ())),
                "unique_constraints": _registry_unique_contract(table_name),
                "indexes": "migration_001_v1_5_evidence_registry",
                "physical_role": _registry_table_role(table_name),
            }
        )
    return rows


def _registry_unique_contract(table_name: str) -> str:
    uniques = {
        "runs": "run_id",
        "run_devices": "run_db_id,device_id,role",
        "sample_files": "run_db_id,path",
    }
    return uniques.get(table_name, "id")


def _registry_table_role(table_name: str) -> str:
    roles = {
        "runs": "evidence package/run root and release status",
        "devices": "reference devices, gas analyzers, standards, and transport devices in the evidence bundle",
        "run_devices": "run-to-device participation and role mapping",
        "standard_gases": "standard gas certificates and cylinder traceability",
        "reference_certificates": "pressure/dewpoint/reference device certificates",
        "calibration_points": "formal point-level summary and quality counts",
        "sample_files": "raw CSV/JSON/MD/PDF artifact hash index",
        "qc_results": "formal evidence QC checks and blocker reasons",
        "coefficient_snapshots": "old GETCO/SENCO/R0 snapshots before any write",
        "coefficient_candidates": "no-write candidate coefficient review payloads",
        "coefficient_write_events": "controlled write/readback/rollback evidence",
        "reports": "formal reports and certificates",
        "audit_events": "operator/reviewer/approver events",
        "evidence_integrity_checks": "hash, identity, archive, and database-release checks",
    }
    return roles.get(table_name, "")


def _identity_contract_rows() -> list[dict[str, Any]]:
    return [
        {
            "field": "sn_code",
            "required": True,
            "format": "8 numeric digits; not 00000000",
            "table": "core_storage.sensors",
            "constraint": "uq_sensors_sn_code",
            "role": "production primary identity",
        },
        {
            "field": "device_code",
            "required": True,
            "format": "8 numeric digits; normally equals sn_code in V1.5 production",
            "table": "core_storage.sensors",
            "constraint": "uq_sensors_device_code",
            "role": "production lookup identity and future QR-code payload",
        },
        {
            "field": "protocol_device_id",
            "required": True,
            "format": "3 numeric digits",
            "table": "core_storage.sensor_identity_aliases",
            "constraint": "ix_sensor_identity_alias_lookup",
            "role": "compatibility query alias and command identity only; never primary identity",
        },
        {
            "field": "slot_or_ga_label",
            "required": True,
            "format": "GA01..GA06",
            "table": "run_devices.metadata",
            "constraint": "run-scoped metadata",
            "role": "run-local analyzer slot label only",
        },
        {
            "field": "com_port",
            "required": False,
            "format": "COMn transport endpoint",
            "table": "run_devices.metadata",
            "constraint": "run-scoped metadata",
            "role": "transport mapping only; not a device identity",
        },
    ]


def _insert_preview_rows() -> list[dict[str, Any]]:
    return [
        {
            "stage": "initialization_identity",
            "source_artifact": "v1_5_formal_initialization_db_bundle.json",
            "target_tables": "sensors;sensor_identity_aliases;runs;device_events",
            "natural_key": "sn_code/device_code + protocol_device_id alias + run_id",
            "write_mode": "dry_run_preview_only_until_explicit_db_import",
            "physical_meaning": "Bind analyzer SN/device_code, protocol ID alias, GETCO epoch-0, and initialization run metadata.",
        },
        {
            "stage": "runtime_setup",
            "source_artifact": "v1_5_analyzer_runtime_setup_result.json",
            "target_tables": "sensors;sensor_identity_aliases;runs;device_events",
            "natural_key": "sn_code + runtime setup event type + run_id",
            "write_mode": "dry_run_preview_only_until_explicit_db_import",
            "physical_meaning": "Record MODE2, 1Hz active upload, AVERAGE1/2 filter, SN readback, and CHECK-capable runtime evidence.",
        },
        {
            "stage": "pressure_temperature_pre_open_flow",
            "source_artifact": "pressure/S9 and temperature-neutral review sidecars",
            "target_tables": "device_events;evidence_integrity_checks;reference_certificates",
            "natural_key": "run_id + device + event_type",
            "write_mode": "dry_run_preview_only_until_explicit_db_import",
            "physical_meaning": "Keep pressure SENCO9 and S7/S8 neutral review separate from CO2/H2O fit rows.",
        },
        {
            "stage": "open_flow_sampling",
            "source_artifact": "CO2/H2O queue CSV/JSON evidence",
            "target_tables": "points;samples;measurement_frames;qc_results;v1_5_evidence.calibration_points;v1_5_evidence.sample_files",
            "natural_key": "run_id + point sequence/key + analyzer label + sample index/timestamp",
            "write_mode": "dry_run_preview_only_until_explicit_db_import",
            "physical_meaning": "Store point-level summaries, raw frames, sample windows, QC grades, and artifact hashes.",
        },
        {
            "stage": "fit_and_candidate_review",
            "source_artifact": "candidate coefficient review sidecars",
            "target_tables": "fit_results;v1_5_evidence.coefficient_candidates;v1_5_evidence.qc_results",
            "natural_key": "run_id + sensor/analyzer + algorithm/profile",
            "write_mode": "dry_run_preview_only_until_explicit_db_import",
            "physical_meaning": "Record no-write candidate coefficients, residuals, included/rejected points, and algorithm profile.",
        },
        {
            "stage": "controlled_write_and_readback",
            "source_artifact": "controlled write/readback/rollback sidecars",
            "target_tables": "coefficient_versions;device_events;v1_5_evidence.coefficient_write_events",
            "natural_key": "sensor_id + version or run_id + analyzer + event_type",
            "write_mode": "requires explicit controlled write and separate database import authorization",
            "physical_meaning": "Trace old coefficient snapshot, clear/write/readback status, rollback plan, and reviewer approval.",
        },
        {
            "stage": "archive_report_release",
            "source_artifact": "archive closure index, formal reports, hash manifests",
            "target_tables": "v1_5_evidence.reports;v1_5_evidence.audit_events;v1_5_evidence.evidence_integrity_checks",
            "natural_key": "run_id + artifact path/hash/report type",
            "write_mode": "dry_run_preview_only_until_archive_release_gate_passes",
            "physical_meaning": "Index immutable evidence files, reports, release gates, and SN traceability before formal database import.",
        },
    ]


def _planned_device_checks(planned_devices: Sequence[Mapping[str, str]]) -> tuple[str, list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    reasons: list[str] = []
    seen_sn: set[str] = set()
    seen_device: set[str] = set()
    seen_protocol: set[str] = set()
    if len(planned_devices) > 6:
        reasons.append("planned_device_count_gt_6")
    for index, row in enumerate(planned_devices):
        slot = row.get("slot") or f"GA{index + 1:02d}"
        sn_code = row.get("sn_code") or ""
        device_code = row.get("device_code") or ""
        protocol_id = row.get("protocol_device_id") or ""
        row_reasons: list[str] = []
        if not sn_code:
            row_reasons.append("sn_code_invalid_or_missing")
        if not device_code:
            row_reasons.append("device_code_invalid_or_missing")
        if sn_code and device_code and sn_code != device_code:
            row_reasons.append("device_code_must_match_sn_code_for_v1_5")
        if not protocol_id:
            row_reasons.append("protocol_device_id_invalid_or_missing")
        if sn_code and sn_code in seen_sn:
            row_reasons.append("duplicate_sn_code")
        if device_code and device_code in seen_device:
            row_reasons.append("duplicate_device_code")
        if protocol_id and protocol_id in seen_protocol:
            row_reasons.append("duplicate_protocol_device_id_in_same_run")
        seen_sn.add(sn_code)
        seen_device.add(device_code)
        seen_protocol.add(protocol_id)
        if row_reasons:
            reasons.extend(f"{slot}:{reason}" for reason in row_reasons)
        rows.append(
            {
                "slot": slot,
                "sn_code": sn_code,
                "device_code": device_code,
                "protocol_device_id": protocol_id,
                "port": row.get("port") or "",
                "status": "ready" if not row_reasons else "blocked",
                "reasons": ";".join(row_reasons),
                "identity_query_paths": "sn_code;device_code;protocol_device_id_alias",
                "transport_identity_role": "port_is_transport_only",
            }
        )
    status = "ready" if not reasons else "blocker"
    return status, rows, reasons


def _check(
    *,
    check: str,
    status: str,
    evidence_role: str,
    reasons: Sequence[str] = (),
    physical_meaning: str,
    next_action: str,
    details: Mapping[str, Any],
) -> FormalDatabaseDryRunCheck:
    return FormalDatabaseDryRunCheck(
        check=check,
        status=status,
        evidence_role=evidence_role,
        reasons=tuple(reasons),
        physical_meaning=physical_meaning,
        next_action=next_action,
        details=details,
    )


def build_v1_5_formal_database_dry_run_contract(
    *,
    planned_devices: Sequence[str | Mapping[str, Any]] | None = None,
    required_postgresql_major: int = PRODUCTION_POSTGRESQL_MAJOR,
) -> dict[str, Any]:
    core_rows = _core_storage_contract()
    registry_rows = _evidence_registry_contract()
    identity_rows = _identity_contract_rows()
    insert_rows = _insert_preview_rows()
    planned = _normalize_planned_devices(planned_devices)
    planned_status, planned_rows, planned_reasons = _planned_device_checks(planned)
    checks: list[FormalDatabaseDryRunCheck] = []

    backend_reasons: list[str] = []
    if required_postgresql_major != PRODUCTION_POSTGRESQL_MAJOR:
        backend_reasons.append(f"required_postgresql_major={required_postgresql_major}")
    checks.append(
        _check(
            check="postgresql18_backend_contract",
            status="ready" if not backend_reasons else "blocker",
            evidence_role="database_backend_contract",
            reasons=backend_reasons,
            physical_meaning="V1.5 production import targets PostgreSQL 18; this dry-run does not connect to it.",
            next_action="Keep this as offline schema preview until a separate PostgreSQL 18 preflight/import is authorized.",
            details={
                "production_backend": PRODUCTION_BACKEND,
                "production_postgresql_major": PRODUCTION_POSTGRESQL_MAJOR,
                "required_postgresql_major": required_postgresql_major,
                "connects_postgresql": False,
            },
        )
    )

    core_reasons: list[str] = []
    present_core = {row["table_name"] for row in core_rows if row["present"]}
    missing_core = [table for table in CORE_STORAGE_REQUIRED_TABLES if table not in present_core]
    core_reasons.extend(f"missing_core_table={table}" for table in missing_core)
    for table, required in CORE_REQUIRED_CONSTRAINTS.items():
        names = _constraint_names(table)
        for name in required.get("unique", set()) - set(names["unique"]):
            core_reasons.append(f"{table}:missing_unique={name}")
        for name in required.get("indexes", set()) - set(names["indexes"]):
            core_reasons.append(f"{table}:missing_index={name}")
    checks.append(
        _check(
            check="core_storage_schema_contract",
            status="ready" if not core_reasons else "blocker",
            evidence_role="core_storage_schema_preview",
            reasons=core_reasons,
            physical_meaning="Core storage must preserve device identity, point/sample/frame QC, fit, coefficient, and event traceability.",
            next_action="Repair storage model constraints before any production database import.",
            details={"table_count": len(core_rows), "required_tables": CORE_STORAGE_REQUIRED_TABLES},
        )
    )

    registry_reasons: list[str] = []
    registry_present = {row["table_name"] for row in registry_rows if row["present"]}
    for table in EVIDENCE_REGISTRY_REQUIRED_TABLES:
        if table not in registry_present:
            registry_reasons.append(f"missing_evidence_registry_table={table}")
    checks.append(
        _check(
            check="evidence_registry_schema_contract",
            status="ready" if not registry_reasons else "blocker",
            evidence_role="v1_5_evidence_registry_schema_preview",
            reasons=registry_reasons,
            physical_meaning="The V1.5 evidence registry indexes immutable files, reports, QC, candidates, writes, and release checks.",
            next_action="Repair evidence-registry table contract or migration before database import.",
            details={"table_count": len(registry_rows), "schema": "v1_5_evidence"},
        )
    )

    identity_reasons: list[str] = []
    if "uq_sensors_sn_code" not in _constraint_names("sensors")["unique"]:
        identity_reasons.append("sn_code_unique_constraint_missing")
    if "uq_sensors_device_code" not in _constraint_names("sensors")["unique"]:
        identity_reasons.append("device_code_unique_constraint_missing")
    if "ix_sensor_identity_alias_lookup" not in _constraint_names("sensor_identity_aliases")["indexes"]:
        identity_reasons.append("protocol_device_id_alias_lookup_index_missing")
    checks.append(
        _check(
            check="sn_device_code_identity_contract",
            status="ready" if not identity_reasons else "blocker",
            evidence_role="identity_schema_contract",
            reasons=identity_reasons,
            physical_meaning="SN/device_code is the durable production identity; protocol device ID remains a compatibility alias.",
            next_action="Do not import production evidence until identity uniqueness and alias lookup are explicit.",
            details={"identity_contract_rows": len(identity_rows), "planned_device_count": len(planned_rows)},
        )
    )

    checks.append(
        _check(
            check="planned_device_identity_preview",
            status=planned_status,
            evidence_role="planned_device_insert_preview",
            reasons=planned_reasons,
            physical_meaning="Optional 1-6 analyzer preview catches duplicate or malformed SN/device_code/protocol ID before import.",
            next_action="Fix planned device identity rows before generating a production DB insert preview.",
            details={"planned_device_count": len(planned_rows), "supports_active_device_count": "1_to_6"},
        )
    )

    insert_reasons: list[str] = []
    if not any(row["stage"] == "controlled_write_and_readback" for row in insert_rows):
        insert_reasons.append("controlled_write_stage_missing")
    if not any(row["stage"] == "archive_report_release" for row in insert_rows):
        insert_reasons.append("archive_release_stage_missing")
    checks.append(
        _check(
            check="insert_preview_contract",
            status="ready" if not insert_reasons else "blocker",
            evidence_role="database_insert_preview_contract",
            reasons=insert_reasons,
            physical_meaning="Database import must preserve each physical stage from initialization through archive without inventing release evidence.",
            next_action="Review insert-preview rows before wiring any production DB import command.",
            details={"stage_count": len(insert_rows), "database_written": False},
        )
    )

    release_reasons: list[str] = []
    checks.append(
        _check(
            check="dry_run_does_not_authorize_import_or_release",
            status="ready" if not release_reasons else "blocker",
            evidence_role="database_release_boundary",
            reasons=release_reasons,
            physical_meaning="This contract can approve schema shape, not formal archive release or database import.",
            next_action="Use archive closure and formal run status before any real PostgreSQL import.",
            details={
                "database_import_allowed": False,
                "formal_release_allowed": False,
                "not_real_acceptance_evidence": True,
            },
        )
    )

    blocker_count = sum(1 for row in checks if row.status == "blocker")
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": "blocked" if blocker_count else "ready_for_postgresql18_schema_dry_run_review",
        "blocker_count": blocker_count,
        "review_required_count": 0,
        "production_backend": PRODUCTION_BACKEND,
        "production_postgresql_major": PRODUCTION_POSTGRESQL_MAJOR,
        "connects_postgresql": False,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "database_written": False,
        "database_import_allowed": False,
        "formal_release_allowed": False,
        "not_real_acceptance_evidence": True,
        "primary_identity": "sn_code/device_code",
        "protocol_device_id_role": "compatibility_alias_and_command_identity",
        "transport_role": "COM/GA labels are run-local transport mapping only",
        "core_storage_tables": core_rows,
        "evidence_registry_tables": registry_rows,
        "identity_contract": identity_rows,
        "planned_device_preview": planned_rows,
        "insert_preview": insert_rows,
        "checks": [row.to_json() for row in checks],
        "next_action": (
            "Review schema and insert-preview evidence. Passing this dry-run does not connect to PostgreSQL, "
            "write production data, authorize archive release, or replace initialization DB preflight."
        ),
    }


def write_v1_5_formal_database_dry_run_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out / "v1_5_formal_database_dry_run.json",
        "checks_csv": out / "v1_5_formal_database_dry_run_checks.csv",
        "core_tables_csv": out / "v1_5_formal_database_core_tables.csv",
        "registry_tables_csv": out / "v1_5_formal_database_evidence_registry_tables.csv",
        "identity_contract_csv": out / "v1_5_formal_database_identity_contract.csv",
        "insert_preview_csv": out / "v1_5_formal_database_insert_preview.csv",
        "planned_device_preview_csv": out / "v1_5_formal_database_planned_device_preview.csv",
        "markdown": out / "V1_5_FORMAL_DATABASE_DRY_RUN.md",
    }
    _write_json(paths["json"], model)
    _write_csv(paths["checks_csv"], model.get("checks", []))
    _write_csv(paths["core_tables_csv"], model.get("core_storage_tables", []))
    _write_csv(paths["registry_tables_csv"], model.get("evidence_registry_tables", []))
    _write_csv(paths["identity_contract_csv"], model.get("identity_contract", []))
    _write_csv(paths["insert_preview_csv"], model.get("insert_preview", []))
    _write_csv(paths["planned_device_preview_csv"], model.get("planned_device_preview", []))
    lines = [
        "# V1.5 formal database dry-run contract",
        "",
        "This is an offline PostgreSQL 18 schema and insert-preview contract for V1.5 production evidence.",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- blocker_count: `{model.get('blocker_count')}`",
        f"- production backend: `{model.get('production_backend')}` `{model.get('production_postgresql_major')}`",
        f"- primary identity: `{model.get('primary_identity')}`",
        f"- protocol device ID role: `{model.get('protocol_device_id_role')}`",
        f"- database_import_allowed: `{model.get('database_import_allowed')}`",
        f"- formal_release_allowed: `{model.get('formal_release_allowed')}`",
        "- This dry-run does not connect PostgreSQL, open COM, control routes, write SN/device IDs, write coefficients, release archives, or import data.",
        "",
        "## Checks",
        "",
    ]
    for row in model.get("checks", []):
        reasons = ";".join(row.get("reasons") or ())
        lines.append(f"- `{row.get('check')}`: `{row.get('status')}` {reasons}".rstrip())
    paths["markdown"].parent.mkdir(parents=True, exist_ok=True)
    paths["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return paths
