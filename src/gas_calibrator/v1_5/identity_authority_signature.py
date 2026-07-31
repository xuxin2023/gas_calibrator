"""Authenticate V1.5 identity evidence against a deployment trust root."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

try:
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
except ImportError:  # pragma: no cover - exercised by fail-closed result in deployment
    InvalidSignature = Exception
    Ed25519PublicKey = None  # type: ignore[assignment]


TRUST_STORE_SCHEMA = "v1_5_identity_authority_trust_store_v1"
SIGNATURE_SCHEMA = "v1_5_identity_evidence_signature_v1"
SIGNATURE_ALGORITHM = "ed25519"
MAX_ALLOWED_SIGNATURE_AGE_S = 86_400
FUTURE_CLOCK_TOLERANCE_S = 300


def default_identity_authority_trust_store_path() -> Path:
    """Return the fixed, repository-external deployment trust-store path."""

    if os.name == "nt":
        return Path(
            r"C:\ProgramData\GasCalibrator\trust\v1_5_identity_authorities.json"
        )
    return Path("/etc/gas_calibrator/trust/v1_5_identity_authorities.json")


def canonical_evidence_bytes(payload: Mapping[str, Any]) -> bytes:
    """Canonicalize evidence without its detached signature envelope."""

    unsigned = dict(payload)
    unsigned.pop("authority_signature", None)
    return json.dumps(
        unsigned,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def signature_message_bytes(signature: Mapping[str, Any]) -> bytes:
    """Canonicalize the signed envelope metadata, excluding signature bytes."""

    message = {
        "schema_version": signature.get("schema_version"),
        "algorithm": signature.get("algorithm"),
        "key_id": signature.get("key_id"),
        "signed_at": signature.get("signed_at"),
        "payload_sha256": signature.get("payload_sha256"),
    }
    return json.dumps(
        message,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _aware_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _inside_tests_fixtures(path: Path) -> bool:
    parts = [part.casefold() for part in path.parts]
    return any(
        parts[index : index + 2] == ["tests", "fixtures"]
        for index in range(len(parts) - 1)
    )


def _decode_base64(value: Any, *, expected_bytes: int) -> bytes | None:
    try:
        decoded = base64.b64decode(str(value or ""), validate=True)
    except (ValueError, binascii.Error):
        return None
    return decoded if len(decoded) == expected_bytes else None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_identity_authority_signature(
    payload: Mapping[str, Any],
    *,
    trust_store_path: str | Path | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Verify signed evidence and return a fail-closed diagnostic result."""

    requested_path = Path(
        trust_store_path or default_identity_authority_trust_store_path()
    )
    result: dict[str, Any] = {
        "required": True,
        "valid": False,
        "status": "blocked",
        "trust_store_path": str(requested_path),
        "trust_store_available": False,
        "trust_store_sha256": "",
        "trust_store_schema_valid": False,
        "trust_store_deployment_managed": False,
        "trust_store_test_fixture_forbidden": False,
        "signature_schema_valid": False,
        "signature_algorithm": "",
        "signature_key_id": "",
        "signature_signed_at": "",
        "payload_sha256_declared": "",
        "payload_sha256_actual": "",
        "payload_digest_matches": False,
        "key_trusted": False,
        "key_active": False,
        "key_source_type_authorized": False,
        "key_source_system_authorized": False,
        "key_valid_at_signature_time": False,
        "signature_fresh": False,
        "signature_valid": False,
        "blockers": [],
    }
    blockers: list[str] = result["blockers"]

    trust_store: dict[str, Any] = {}
    try:
        resolved_path = requested_path.resolve(strict=True)
        if not resolved_path.is_file():
            raise FileNotFoundError(requested_path)
    except (OSError, RuntimeError):
        blockers.append("global_uniqueness_evidence_trust_store_missing")
        resolved_path = requested_path.resolve()
    else:
        result["trust_store_path"] = str(resolved_path)
        result["trust_store_available"] = True
        fixture_path = _inside_tests_fixtures(resolved_path)
        result["trust_store_test_fixture_forbidden"] = fixture_path
        if fixture_path:
            blockers.append(
                "global_uniqueness_evidence_trust_store_test_fixture_forbidden"
            )
        try:
            result["trust_store_sha256"] = _sha256_file(resolved_path)
            loaded = json.loads(resolved_path.read_text(encoding="utf-8-sig"))
        except OSError:
            blockers.append("global_uniqueness_evidence_trust_store_unreadable")
        except ValueError:
            blockers.append("global_uniqueness_evidence_trust_store_json_invalid")
        else:
            if not isinstance(loaded, Mapping):
                blockers.append("global_uniqueness_evidence_trust_store_json_invalid")
            else:
                trust_store = dict(loaded)
                result["trust_store_schema_valid"] = (
                    trust_store.get("schema_version") == TRUST_STORE_SCHEMA
                )
                result["trust_store_deployment_managed"] = (
                    trust_store.get("deployment_managed") is True
                )
                if not result["trust_store_schema_valid"]:
                    blockers.append(
                        "global_uniqueness_evidence_trust_store_schema_invalid"
                    )
                if not result["trust_store_deployment_managed"]:
                    blockers.append(
                        "global_uniqueness_evidence_trust_store_not_deployment_managed"
                    )
                if trust_store.get("test_fixture_only") is not False:
                    blockers.append(
                        "global_uniqueness_evidence_trust_store_test_fixture_forbidden"
                    )

    signature = payload.get("authority_signature")
    if not isinstance(signature, Mapping):
        blockers.append("global_uniqueness_evidence_signature_missing")
        signature = {}
    signature = dict(signature)
    algorithm = str(signature.get("algorithm") or "").strip().lower()
    key_id = str(signature.get("key_id") or "").strip()
    signed_at_text = str(signature.get("signed_at") or "").strip()
    declared_digest = str(signature.get("payload_sha256") or "").strip().lower()
    result.update(
        {
            "signature_schema_valid": signature.get("schema_version")
            == SIGNATURE_SCHEMA,
            "signature_algorithm": algorithm,
            "signature_key_id": key_id,
            "signature_signed_at": signed_at_text,
            "payload_sha256_declared": declared_digest,
        }
    )
    if signature.get("schema_version") != SIGNATURE_SCHEMA:
        blockers.append("global_uniqueness_evidence_signature_schema_invalid")
    if algorithm != SIGNATURE_ALGORITHM:
        blockers.append("global_uniqueness_evidence_signature_algorithm_invalid")
    if not key_id:
        blockers.append("global_uniqueness_evidence_signature_key_id_missing")

    signed_at = _aware_datetime(signed_at_text)
    if signed_at is None:
        blockers.append("global_uniqueness_evidence_signature_signed_at_invalid")
    actual_digest = hashlib.sha256(canonical_evidence_bytes(payload)).hexdigest()
    result["payload_sha256_actual"] = actual_digest
    if len(declared_digest) != 64 or any(
        char not in "0123456789abcdef" for char in declared_digest
    ):
        blockers.append("global_uniqueness_evidence_signature_payload_sha256_invalid")
    else:
        result["payload_digest_matches"] = declared_digest == actual_digest
        if not result["payload_digest_matches"]:
            blockers.append(
                "global_uniqueness_evidence_signature_payload_digest_mismatch"
            )

    signature_bytes = _decode_base64(
        signature.get("signature_base64"), expected_bytes=64
    )
    if signature_bytes is None:
        blockers.append("global_uniqueness_evidence_signature_base64_invalid")

    raw_keys = trust_store.get("keys")
    keys = [dict(item) for item in raw_keys or [] if isinstance(item, Mapping)]
    matches = [item for item in keys if str(item.get("key_id") or "") == key_id]
    trusted_key: dict[str, Any] = {}
    if len(matches) == 0:
        blockers.append("global_uniqueness_evidence_signature_key_unknown")
    elif len(matches) > 1:
        blockers.append("global_uniqueness_evidence_signature_key_duplicate")
    else:
        trusted_key = matches[0]
        result["key_trusted"] = True
        result["key_active"] = trusted_key.get("status") == "active"
        if not result["key_active"]:
            blockers.append("global_uniqueness_evidence_signature_key_inactive")
        if str(trusted_key.get("algorithm") or "").lower() != SIGNATURE_ALGORITHM:
            blockers.append(
                "global_uniqueness_evidence_signature_key_algorithm_mismatch"
            )

        authority = payload.get("authority")
        authority = dict(authority) if isinstance(authority, Mapping) else {}
        source_type = str(authority.get("source_type") or "").strip()
        source_system = str(authority.get("source_system") or "").strip()
        allowed_types = {
            str(value).strip()
            for value in trusted_key.get("allowed_source_types") or []
            if str(value).strip()
        }
        allowed_systems = {
            str(value).strip()
            for value in trusted_key.get("allowed_source_systems") or []
            if str(value).strip()
        }
        result["key_source_type_authorized"] = source_type in allowed_types
        result["key_source_system_authorized"] = source_system in allowed_systems
        if not result["key_source_type_authorized"]:
            blockers.append(
                "global_uniqueness_evidence_signature_source_type_unauthorized"
            )
        if not result["key_source_system_authorized"]:
            blockers.append(
                "global_uniqueness_evidence_signature_source_system_unauthorized"
            )

        valid_from = _aware_datetime(trusted_key.get("valid_from"))
        valid_until = _aware_datetime(trusted_key.get("valid_until"))
        if valid_from is None or valid_until is None or valid_from >= valid_until:
            blockers.append("global_uniqueness_evidence_signature_key_validity_invalid")
        elif signed_at is not None:
            result["key_valid_at_signature_time"] = (
                valid_from <= signed_at <= valid_until
            )
            if not result["key_valid_at_signature_time"]:
                blockers.append(
                    "global_uniqueness_evidence_signature_key_outside_validity"
                )

        max_age = trusted_key.get("max_signature_age_seconds")
        if (
            isinstance(max_age, bool)
            or not isinstance(max_age, int)
            or max_age <= 0
            or max_age > MAX_ALLOWED_SIGNATURE_AGE_S
        ):
            blockers.append("global_uniqueness_evidence_signature_max_age_invalid")
        elif signed_at is not None:
            current = now or datetime.now(timezone.utc)
            if current.tzinfo is None or current.utcoffset() is None:
                blockers.append(
                    "global_uniqueness_evidence_signature_verification_time_invalid"
                )
            else:
                current = current.astimezone(timezone.utc)
                if signed_at > current + timedelta(seconds=FUTURE_CLOCK_TOLERANCE_S):
                    blockers.append(
                        "global_uniqueness_evidence_signature_signed_in_future"
                    )
                else:
                    age_seconds = (current - signed_at).total_seconds()
                    result["signature_fresh"] = age_seconds <= max_age
                    if not result["signature_fresh"]:
                        blockers.append("global_uniqueness_evidence_signature_stale")

        exported_at = _aware_datetime(authority.get("exported_at"))
        if signed_at is not None and (exported_at is None or signed_at < exported_at):
            blockers.append("global_uniqueness_evidence_signature_precedes_export")

        public_key_bytes = _decode_base64(
            trusted_key.get("public_key_base64"), expected_bytes=32
        )
        if public_key_bytes is None:
            blockers.append(
                "global_uniqueness_evidence_signature_key_public_key_invalid"
            )
        elif Ed25519PublicKey is None:
            blockers.append("global_uniqueness_evidence_signature_verifier_unavailable")
        elif signature_bytes is not None:
            try:
                public_key = Ed25519PublicKey.from_public_bytes(public_key_bytes)
                public_key.verify(signature_bytes, signature_message_bytes(signature))
            except (InvalidSignature, ValueError):
                blockers.append("global_uniqueness_evidence_signature_invalid")
            else:
                result["signature_valid"] = True

    result["valid"] = not blockers
    result["status"] = "verified" if result["valid"] else "blocked"
    return result
