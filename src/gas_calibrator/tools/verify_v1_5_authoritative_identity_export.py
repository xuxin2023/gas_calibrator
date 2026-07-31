"""Validate an external authoritative identity export without device or DB access."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

from .run_v1_5_protocol_identity_controlled_write import (
    PLAN_SCHEMA,
    _load_json,
    _normalize_id,
    _sha256_file,
    build_preflight,
)


RESULT_SCHEMA = "v1_5_authoritative_identity_export_validation_v1"
_SN_RE = re.compile(r"\d{8}")
_ID_RE = re.compile(r"\d{3}")


def _best_effort_payload(path: Path) -> dict[str, Any]:
    try:
        return _load_json(path)
    except (OSError, ValueError):
        return {}


def build_validation(
    *,
    evidence_path: str | Path,
    candidate_sn: str,
    candidate_protocol_id: str,
    declared_sha256: str = "",
) -> dict[str, Any]:
    source = Path(evidence_path).resolve()
    normalized_sn = str(candidate_sn or "").strip()
    normalized_id = _normalize_id(candidate_protocol_id)
    format_blockers: list[str] = []
    if not _SN_RE.fullmatch(normalized_sn) or normalized_sn == "00000000":
        format_blockers.append("candidate_sn_format_invalid")
    if not _ID_RE.fullmatch(normalized_id):
        format_blockers.append("candidate_protocol_id_format_invalid")

    payload = _best_effort_payload(source)
    actual_sha256 = _sha256_file(source) if source.is_file() else ""
    declared = str(declared_sha256 or actual_sha256).strip()
    uniqueness = {
        "candidate_sn_absent": payload.get("candidate_sn_absent") is True,
        "candidate_protocol_id_absent": payload.get("candidate_protocol_id_absent")
        is True,
        "source": str(source),
        "sha256": declared,
    }
    adapter_plan = {
        "schema_version": PLAN_SCHEMA,
        "status": "authority_validation_only",
        "execution_allowed": False,
        "approval": {
            "global_sn_unique": False,
            "global_protocol_id_unique": False,
            "approved_by": "",
            "approved_at": "",
            "rollback_authorized": False,
        },
        "global_uniqueness_evidence": uniqueness,
        "rows": [
            {
                "port": "VALIDATION_ONLY",
                "usb_serial_number": "VALIDATION_ONLY",
                "sn_code": "00000000",
                "observed_protocol_device_id": "000",
                "candidate_target_sn": normalized_sn,
                "candidate_target_protocol_device_id": normalized_id,
                "action": "initialize_sn_then_change_protocol_id_after_review",
            }
        ],
    }
    preflight = build_preflight(adapter_plan, {}, None)
    evidence_blockers = [
        str(blocker)
        for blocker in preflight.get("blockers") or []
        if str(blocker).startswith("global_uniqueness_evidence")
    ]
    blockers = [*format_blockers, *evidence_blockers]
    validation = dict(
        preflight.get("global_uniqueness_evidence_validation") or {}
    )
    valid = not blockers and validation.get("valid") is True
    return {
        "schema_version": RESULT_SCHEMA,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "status": "ready" if valid else "blocked",
        "candidate": {
            "sn_code": normalized_sn,
            "protocol_device_id": normalized_id,
        },
        "source": {
            "path": str(source),
            "declared_sha256": declared,
            "actual_sha256": actual_sha256,
        },
        "blockers": blockers,
        "global_uniqueness_evidence_validation": validation,
        "not_write_authorization": True,
        "engineering_review_only": True,
        "promotion_state": "blocked",
        "not_real_acceptance_evidence": True,
        "boundary": {
            "opens_com_ports": False,
            "connects_postgresql": False,
            "database_written": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_senco": False,
            "controls_water_or_gas_routes": False,
            "opens_dewpoint_meter": False,
            "runs_calibration": False,
        },
    }


def _write_result(path: str | Path, result: Mapping[str, Any]) -> Path:
    destination = Path(path).resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    temporary_path = ""
    try:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", newline="", dir=destination.parent,
            prefix=f".{destination.name}.", suffix=".tmp", delete=False,
        ) as handle:
            temporary_path = handle.name
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, destination)
    finally:
        if temporary_path:
            Path(temporary_path).unlink(missing_ok=True)
    return destination


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Offline validation of an authoritative analyzer identity export."
    )
    parser.add_argument("--evidence-json", required=True)
    parser.add_argument("--candidate-sn", required=True)
    parser.add_argument("--candidate-protocol-id", required=True)
    parser.add_argument("--declared-sha256", default="")
    parser.add_argument("--output-json", required=True)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        result = build_validation(
            evidence_path=args.evidence_json,
            candidate_sn=args.candidate_sn,
            candidate_protocol_id=args.candidate_protocol_id,
            declared_sha256=args.declared_sha256,
        )
        output = _write_result(args.output_json, result)
        print(
            json.dumps(
                {
                    "status": result["status"],
                    "blockers": result["blockers"],
                    "output_json": str(output),
                },
                ensure_ascii=False,
                indent=2,
            ),
            flush=True,
        )
        return 0 if result["status"] == "ready" else 1
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
