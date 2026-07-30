"""V1.5 operator-editable certificate metrics with an isolated audit trail.

The registry is deliberately not connected to calibration execution, fitting,
device I/O, coefficient writeback, or the frozen V1 workflow. It stores
reviewable certificate metadata for the final V1.5 product.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timezone
import json
import math
import os
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4


SCHEMA_VERSION = "certificate_metrics_registry_v1"
BOUNDARY = {
    "calibration_input_connected": False,
    "coefficient_fit_allowed": False,
    "coefficient_write_allowed": False,
    "device_io_allowed": False,
    "database_write_allowed": False,
    "real_primary_latest_refresh_allowed": False,
    "not_real_acceptance_evidence": True,
}

_NUMERIC_FIELDS = {
    "nominal_value",
    "certified_value",
    "standard_uncertainty",
    "expanded_uncertainty",
    "coverage_factor",
}
_DATE_FIELDS = {"issue_date", "valid_from", "valid_until"}
_REVIEW_STATES = {"draft", "pending_review", "reviewed", "rejected"}


def empty_certificate_metrics_registry() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "boundary": deepcopy(BOUNDARY),
        "updated_at": "",
        "records": [],
        "audit_events": [],
    }


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _optional_float(value: Any, field: str) -> float | None:
    if value is None or _clean_text(value) == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} 必须是数值") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field} 必须是有限数值")
    return number


def _validate_date(value: Any, field: str) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    try:
        date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field} 必须使用 YYYY-MM-DD") from exc
    return text


def normalize_certificate_metric_record(
    values: Mapping[str, Any],
    *,
    submit_for_review: bool = False,
) -> dict[str, Any]:
    record = {str(key): deepcopy(value) for key, value in dict(values or {}).items()}
    normalized: dict[str, Any] = {}
    for field in (
        "record_id",
        "asset_id",
        "asset_name",
        "asset_type",
        "measurand",
        "certificate_id",
        "certificate_version",
        "unit",
        "uncertainty_unit",
        "cylinder_serial_number",
        "manufacturer",
        "balance_gas",
        "gas_matrix",
        "preparation_method",
        "traceability_chain",
        "evidence_file_path",
        "evidence_file_sha256",
        "notes",
    ):
        normalized[field] = _clean_text(record.get(field))

    if not normalized["asset_id"]:
        raise ValueError("资产编号不能为空")
    if not normalized["asset_name"]:
        normalized["asset_name"] = normalized["asset_id"]
    if not normalized["record_id"]:
        normalized["record_id"] = f"certmetric-{uuid4().hex[:16]}"

    for field in _NUMERIC_FIELDS:
        normalized[field] = _optional_float(record.get(field), field)
    for field in _DATE_FIELDS:
        normalized[field] = _validate_date(record.get(field), field)

    for field in (
        "nominal_value",
        "certified_value",
        "standard_uncertainty",
        "expanded_uncertainty",
    ):
        value = normalized[field]
        if value is not None and value < 0:
            raise ValueError(f"{field} 不能小于 0")
    coverage_factor = normalized["coverage_factor"]
    if coverage_factor is not None and coverage_factor <= 0:
        raise ValueError("coverage_factor 必须大于 0")

    valid_from = normalized["valid_from"]
    valid_until = normalized["valid_until"]
    if (
        valid_from
        and valid_until
        and date.fromisoformat(valid_until) < date.fromisoformat(valid_from)
    ):
        raise ValueError("证书有效期截止日不能早于开始日")

    review_state = (
        "pending_review"
        if submit_for_review
        else _clean_text(record.get("review_state") or "draft")
    )
    if review_state not in _REVIEW_STATES:
        raise ValueError("review_state 不受支持")
    if submit_for_review:
        required = {
            "measurand": normalized["measurand"],
            "certificate_id": normalized["certificate_id"],
            "certified_value": normalized["certified_value"],
            "unit": normalized["unit"],
            "expanded_uncertainty": normalized["expanded_uncertainty"],
            "coverage_factor": normalized["coverage_factor"],
            "valid_until": normalized["valid_until"],
            "traceability_chain": normalized["traceability_chain"],
            "evidence_file_path": normalized["evidence_file_path"],
        }
        missing = [field for field, value in required.items() if value in {"", None}]
        if missing:
            raise ValueError("提交复核前仍缺少：" + "、".join(missing))
    normalized["review_state"] = review_state

    normalized["source_class"] = "operator_entered_certificate_metadata"
    normalized["calibration_input_connected"] = False
    normalized["not_real_acceptance_evidence"] = True
    return normalized


class CertificateMetricsRegistry:
    """File-backed, revisioned certificate metadata registry."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return empty_certificate_metrics_registry()
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        if payload.get("schema_version") != SCHEMA_VERSION:
            raise ValueError("证书指标注册表版本不受支持")
        if dict(payload.get("boundary") or {}) != BOUNDARY:
            raise ValueError("证书指标注册表安全边界已漂移")
        if not isinstance(payload.get("records"), list) or not isinstance(
            payload.get("audit_events"), list
        ):
            raise ValueError("证书指标注册表结构无效")
        return payload

    def list_records(self) -> list[dict[str, Any]]:
        return [deepcopy(item) for item in self.load()["records"]]

    def get_record(self, record_id: str) -> dict[str, Any] | None:
        wanted = _clean_text(record_id)
        for item in self.list_records():
            if _clean_text(item.get("record_id")) == wanted:
                return item
        return None

    def save_record(
        self,
        values: Mapping[str, Any],
        *,
        actor: str = "local_operator",
        submit_for_review: bool = False,
    ) -> dict[str, Any]:
        normalized = normalize_certificate_metric_record(
            values, submit_for_review=submit_for_review
        )
        payload = self.load()
        now = _utc_now()
        records = list(payload.get("records") or [])
        target_index: int | None = None
        previous: dict[str, Any] | None = None
        for index, item in enumerate(records):
            if _clean_text(item.get("record_id")) == normalized["record_id"]:
                target_index = index
                previous = deepcopy(item)
                break

        revision = int((previous or {}).get("revision", 0) or 0) + 1
        normalized.update(
            {
                "revision": revision,
                "created_at": _clean_text((previous or {}).get("created_at")) or now,
                "updated_at": now,
                "updated_by": _clean_text(actor) or "local_operator",
            }
        )
        history = list((previous or {}).get("revision_history") or [])
        if previous is not None:
            snapshot = {
                key: deepcopy(value)
                for key, value in previous.items()
                if key != "revision_history"
            }
            history.append(snapshot)
        normalized["revision_history"] = history

        if target_index is None:
            records.append(normalized)
            action = "created"
        else:
            records[target_index] = normalized
            action = "updated"

        payload["records"] = records
        payload["updated_at"] = now
        events = list(payload.get("audit_events") or [])
        events.append(
            {
                "event_id": f"cert-audit-{uuid4().hex[:16]}",
                "occurred_at": now,
                "actor": normalized["updated_by"],
                "action": "submitted_for_review" if submit_for_review else action,
                "record_id": normalized["record_id"],
                "asset_id": normalized["asset_id"],
                "revision": revision,
                "review_state": normalized["review_state"],
                "calibration_input_connected": False,
                "device_io_performed": False,
            }
        )
        payload["audit_events"] = events
        self._atomic_write(payload)
        return deepcopy(normalized)

    def _atomic_write(self, payload: Mapping[str, Any]) -> None:
        if dict(payload.get("boundary") or {}) != BOUNDARY:
            raise ValueError("拒绝写入越界的证书指标注册表")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.{uuid4().hex}.tmp")
        try:
            with temporary.open("w", encoding="utf-8", newline="\n") as stream:
                json.dump(payload, stream, ensure_ascii=False, indent=2)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
        finally:
            if temporary.exists():
                temporary.unlink()


__all__ = [
    "BOUNDARY",
    "CertificateMetricsRegistry",
    "SCHEMA_VERSION",
    "empty_certificate_metrics_registry",
    "normalize_certificate_metric_record",
]
