"""Normalize a read-only asset export and validate it without device or DB access."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

from .run_v1_5_protocol_identity_controlled_write import (
    AUTHORITY_SOURCE_TYPES,
    UNIQUENESS_EVIDENCE_SCHEMA,
    _normalize_id,
    _sha256_file,
    _timezone_aware_iso,
)
from .verify_v1_5_authoritative_identity_export import _write_result, build_validation


NORMALIZATION_SCHEMA = "v1_5_authoritative_identity_export_normalization_v1"
CANONICAL_FIELDS = (
    "asset_key",
    "sn_code",
    "protocol_device_id",
    "lifecycle_status",
)
FIELD_ALIASES = {
    "asset_key": (
        "asset_key",
        "asset_id",
        "asset_code",
        "资产编号",
        "资产编码",
        "仪器编号",
    ),
    "sn_code": (
        "sn_code",
        "sn",
        "serial_number",
        "serial_no",
        "序列号",
        "仪器序列号",
    ),
    "protocol_device_id": (
        "protocol_device_id",
        "protocol_id",
        "device_id",
        "仪器地址",
        "设备地址",
        "协议地址",
    ),
    "lifecycle_status": (
        "lifecycle_status",
        "asset_status",
        "status",
        "生命周期状态",
        "设备状态",
    ),
}
_SN_RE = re.compile(r"\d{8}")
_ID_RE = re.compile(r"\d{3}")


def _header_key(value: Any) -> str:
    return re.sub(r"[\s_-]+", "", str(value or "").strip().lower())


_ALIAS_TO_FIELD = {
    _header_key(alias): canonical
    for canonical, aliases in FIELD_ALIASES.items()
    for alias in aliases
}


def _detect_source_format(path: Path, requested: str) -> str:
    if requested != "auto":
        return requested
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return "csv"
    if suffix == ".json":
        return "json"
    raise ValueError("source_format_cannot_be_inferred")


def _decode_source(path: Path) -> tuple[str, str]:
    raw = path.read_bytes()
    for encoding in ("utf-8-sig", "gb18030"):
        try:
            return raw.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    raise ValueError("source_text_encoding_unsupported")


def _column_mapping(headers: Iterable[Any]) -> tuple[dict[str, str], list[str]]:
    mapping: dict[str, str] = {}
    duplicate_targets: set[str] = set()
    for header in headers:
        source_name = str(header or "").strip()
        canonical = _ALIAS_TO_FIELD.get(_header_key(source_name))
        if not canonical:
            continue
        if canonical in mapping:
            duplicate_targets.add(canonical)
        else:
            mapping[canonical] = source_name
    if duplicate_targets:
        joined = ",".join(sorted(duplicate_targets))
        raise ValueError(f"ambiguous_source_columns:{joined}")
    missing = [field for field in CANONICAL_FIELDS if field not in mapping]
    return mapping, missing


def _raw_row_sha256(row: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(row), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _normalize_rows(
    raw_rows: list[Mapping[str, Any]],
    *,
    mapping: Mapping[str, str],
    source_format: str,
    source_path: Path,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_rows):
        source_row = index + 2 if source_format == "csv" else index + 1
        source_locator = (
            f"csv_row:{source_row}"
            if source_format == "csv"
            else f"json_records_index:{index}"
        )
        records.append(
            {
                "asset_key": str(raw.get(mapping.get("asset_key", ""), "") or "").strip(),
                "sn_code": str(raw.get(mapping.get("sn_code", ""), "") or "").strip(),
                "protocol_device_id": _normalize_id(
                    raw.get(mapping.get("protocol_device_id", ""), "")
                ),
                "lifecycle_status": str(
                    raw.get(mapping.get("lifecycle_status", ""), "") or ""
                ).strip(),
                "source_trace": {
                    "source_path": str(source_path),
                    "source_locator": source_locator,
                    "raw_row_sha256": _raw_row_sha256(raw),
                },
            }
        )
    return records


def _load_rows(
    path: Path, source_format: str
) -> tuple[list[Mapping[str, Any]], dict[str, str], list[str], str]:
    text, encoding = _decode_source(path)
    if source_format == "csv":
        reader = csv.DictReader(io.StringIO(text, newline=""))
        if reader.fieldnames is None:
            raise ValueError("csv_header_missing")
        mapping, missing = _column_mapping(reader.fieldnames)
        rows = [dict(row) for row in reader]
        return rows, mapping, missing, encoding

    payload = json.loads(text)
    if isinstance(payload, list):
        raw_rows = payload
    elif isinstance(payload, Mapping) and isinstance(payload.get("records"), list):
        raw_rows = payload["records"]
    else:
        raise ValueError("json_records_array_required")
    if any(not isinstance(row, Mapping) for row in raw_rows):
        raise ValueError("json_record_object_required")
    headers: list[str] = []
    for row in raw_rows:
        for key in row:
            if str(key) not in headers:
                headers.append(str(key))
    mapping, missing = _column_mapping(headers)
    return [dict(row) for row in raw_rows], mapping, missing, encoding


def _duplicate_values(records: Iterable[Mapping[str, Any]], field: str) -> list[str]:
    values = [str(row.get(field) or "").strip() for row in records]
    return sorted(value for value, count in Counter(values).items() if value and count > 1)


def _fixture_source(path: Path) -> bool:
    parts = {part.lower() for part in path.parts}
    return "tests" in parts and "fixtures" in parts


def build_normalized_export(
    *,
    source_path: str | Path,
    source_format: str,
    source_type: str,
    source_system: str,
    exported_at: str,
    exported_by: str,
    candidate_sn: str,
    candidate_protocol_id: str,
    scope_complete: bool,
    includes_powered_devices: bool,
    includes_unpowered_devices: bool,
    includes_silent_ports: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    source = Path(source_path).resolve(strict=True)
    if not source.is_file():
        raise ValueError("source_file_required")
    detected_format = _detect_source_format(source, source_format)
    raw_rows, mapping, missing_fields, encoding = _load_rows(
        source, detected_format
    )
    records = _normalize_rows(
        raw_rows,
        mapping=mapping,
        source_format=detected_format,
        source_path=source,
    )
    normalized_sn = str(candidate_sn or "").strip()
    normalized_id = _normalize_id(candidate_protocol_id)
    candidate_sn_absent = all(
        str(row.get("sn_code") or "").strip() != normalized_sn for row in records
    )
    candidate_id_absent = all(
        _normalize_id(row.get("protocol_device_id")) != normalized_id
        for row in records
    )

    blockers: list[str] = []
    blockers.extend(f"source_column_missing:{field}" for field in missing_fields)
    if not records:
        blockers.append("source_records_missing")
    invalid_rows = [
        index
        for index, row in enumerate(records, start=1)
        if not str(row.get("asset_key") or "").strip()
        or not _SN_RE.fullmatch(str(row.get("sn_code") or "").strip())
        or not _ID_RE.fullmatch(_normalize_id(row.get("protocol_device_id")))
        or not str(row.get("lifecycle_status") or "").strip()
    ]
    if invalid_rows:
        blockers.append("normalized_records_identity_invalid")
    for field, code in (
        ("asset_key", "duplicate_asset_key"),
        ("sn_code", "duplicate_sn_code"),
        ("protocol_device_id", "duplicate_protocol_device_id"),
    ):
        if _duplicate_values(records, field):
            blockers.append(code)
    if not _SN_RE.fullmatch(normalized_sn) or normalized_sn == "00000000":
        blockers.append("candidate_sn_format_invalid")
    if not _ID_RE.fullmatch(normalized_id):
        blockers.append("candidate_protocol_id_format_invalid")
    if source_type not in AUTHORITY_SOURCE_TYPES:
        blockers.append("authority_source_type_invalid")
    if not str(source_system or "").strip():
        blockers.append("authority_source_system_missing")
    if not str(exported_by or "").strip():
        blockers.append("authority_exported_by_missing")
    if not _timezone_aware_iso(exported_at):
        blockers.append("authority_exported_at_invalid")
    if not scope_complete:
        blockers.append("scope_not_attested_complete")
    if not includes_powered_devices:
        blockers.append("scope_powered_devices_not_attested")
    if not includes_unpowered_devices:
        blockers.append("scope_unpowered_devices_not_attested")
    if not includes_silent_ports:
        blockers.append("scope_silent_ports_not_attested")
    if not candidate_sn_absent:
        blockers.append("candidate_sn_present")
    if not candidate_id_absent:
        blockers.append("candidate_protocol_id_present")
    test_fixture_only = _fixture_source(source)
    if test_fixture_only:
        blockers.append("test_fixture_source_forbidden")

    source_sha256 = _sha256_file(source)
    ready = not blockers
    payload = {
        "schema_version": UNIQUENESS_EVIDENCE_SCHEMA,
        "overall_status": (
            "ready_global_scope_complete"
            if ready
            else "blocked_normalization_review_required"
        ),
        "candidate_sn": normalized_sn,
        "candidate_protocol_device_id": normalized_id,
        "candidate_sn_absent": candidate_sn_absent,
        "candidate_protocol_id_absent": candidate_id_absent,
        "scope_complete": scope_complete,
        "test_fixture_only": test_fixture_only,
        "authority": {
            "source_type": source_type,
            "source_system": str(source_system or "").strip(),
            "exported_at": str(exported_at or "").strip(),
            "exported_by": str(exported_by or "").strip(),
            "read_only_export": True,
            "database_written": False,
        },
        "scope": {
            "scope_complete": scope_complete,
            "includes_powered_devices": includes_powered_devices,
            "includes_unpowered_devices": includes_unpowered_devices,
            "includes_silent_ports": includes_silent_ports,
            "record_count": len(records),
        },
        "records": records,
        "normalization": {
            "schema_version": NORMALIZATION_SCHEMA,
            "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "source_path": str(source),
            "source_sha256": source_sha256,
            "source_format": detected_format,
            "source_encoding": encoding,
            "input_record_count": len(raw_rows),
            "output_record_count": len(records),
            "row_mapping_complete": len(raw_rows) == len(records),
            "column_mapping": mapping,
            "missing_canonical_fields": missing_fields,
            "blockers": blockers,
            "not_write_authorization": True,
            "not_real_acceptance_evidence": True,
        },
    }
    summary = {
        "status": "ready" if ready else "blocked",
        "blockers": blockers,
        "source_sha256": source_sha256,
        "source_format": detected_format,
        "source_encoding": encoding,
        "input_record_count": len(raw_rows),
        "output_record_count": len(records),
        "row_mapping_complete": len(raw_rows) == len(records),
    }
    return payload, summary


def normalize_and_validate(
    *,
    normalized_output: str | Path,
    validation_output: str | Path,
    **normalization_args: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    source = Path(normalization_args["source_path"]).resolve()
    normalized_path = Path(normalized_output).resolve()
    validation_path = Path(validation_output).resolve()
    if len({source, normalized_path, validation_path}) != 3:
        raise ValueError("source_and_output_paths_must_be_distinct")
    payload, summary = build_normalized_export(**normalization_args)
    _write_result(normalized_path, payload)
    validation = build_validation(
        evidence_path=normalized_path,
        candidate_sn=str(normalization_args["candidate_sn"]),
        candidate_protocol_id=str(normalization_args["candidate_protocol_id"]),
    )
    validation["normalization_summary"] = summary
    _write_result(validation_path, validation)
    return payload, validation


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Normalize CSV/JSON asset identities into the V1.5 read-only authority contract."
    )
    parser.add_argument("--source", required=True)
    parser.add_argument("--source-format", choices=("auto", "csv", "json"), default="auto")
    parser.add_argument("--source-type", choices=sorted(AUTHORITY_SOURCE_TYPES), required=True)
    parser.add_argument("--source-system", required=True)
    parser.add_argument("--exported-at", required=True)
    parser.add_argument("--exported-by", required=True)
    parser.add_argument("--candidate-sn", required=True)
    parser.add_argument("--candidate-protocol-id", required=True)
    parser.add_argument("--attest-scope-complete", action="store_true")
    parser.add_argument("--attest-includes-powered-devices", action="store_true")
    parser.add_argument("--attest-includes-unpowered-devices", action="store_true")
    parser.add_argument("--attest-includes-silent-ports", action="store_true")
    parser.add_argument("--normalized-output", required=True)
    parser.add_argument("--validation-output", required=True)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        normalized_path = Path(args.normalized_output).resolve()
        validation_path = Path(args.validation_output).resolve()
        _, validation = normalize_and_validate(
            source_path=args.source,
            source_format=args.source_format,
            source_type=args.source_type,
            source_system=args.source_system,
            exported_at=args.exported_at,
            exported_by=args.exported_by,
            candidate_sn=args.candidate_sn,
            candidate_protocol_id=args.candidate_protocol_id,
            scope_complete=args.attest_scope_complete,
            includes_powered_devices=args.attest_includes_powered_devices,
            includes_unpowered_devices=args.attest_includes_unpowered_devices,
            includes_silent_ports=args.attest_includes_silent_ports,
            normalized_output=normalized_path,
            validation_output=validation_path,
        )
        print(
            json.dumps(
                {
                    "status": validation["status"],
                    "blockers": validation["blockers"],
                    "normalized_output": str(normalized_path),
                    "validation_output": str(validation_path),
                    "not_write_authorization": True,
                },
                ensure_ascii=False,
                indent=2,
            ),
            flush=True,
        )
        return 0 if validation["status"] == "ready" else 1
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
