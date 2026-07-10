"""Offline V1.5 Pressure/SENCO9 readiness evidence index.

This binder normalizes pressure/S9 no-write, controlled-write readback, and
post-write pressure-only reverify evidence into one per-device readiness index.
It is deliberately offline: it does not open COM ports, control pressure,
control gas/water routes, connect PostgreSQL, or write analyzer coefficients.
"""

from __future__ import annotations

import csv
import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SCHEMA = "v1_5_pressure_s9_readiness_index_v1"

READY_STATUS = "ready_for_mature_open_flow_pressure_s9_index"
REVIEW_STATUS = "review_required"

DEFAULT_MAX_REVERIFY_RESIDUAL_HPA = 0.5
DEFAULT_MAX_LINEAR_EXCEPTION_RESIDUAL_HPA = 0.5

READY_WORDS = {
    "pass",
    "passed",
    "ready",
    "ok",
    "verified",
    "approved",
    "written_readback_verified",
    "post_write_pressure_reverify_pass",
    "ready_for_open_flow_main_calibration",
    READY_STATUS,
}

LINEAR_EXCEPTION_WORDS = {
    "linear_s9_controlled_exception",
    "linear_exception",
    "controlled_linear_exception",
    "ga04_linear_s9_controlled_exception",
}

OFFSET_ONLY_WORDS = {
    "offset_only",
    "default_offset_only",
    "mature_offset_only",
    "senco9_offset_only",
}

NO_WRITE_READY_WORDS = {
    "no_write_pressure_pass",
    "no_s9_write_needed",
    "already_pressure_ready",
}


@dataclass(frozen=True)
class PressureS9DeviceRow:
    ga_label: str
    port: str
    protocol_device_id: str
    sn_code: str
    device_code: str
    s9_action: str
    s9_model: str
    no_write_fit_ready: bool
    controlled_write_readback_ready: bool
    pressure_reverify_ready: bool
    linear_exception_authorized: bool
    ready_for_pressure_s9_index: bool
    getco9_readback_values: str
    reverify_max_abs_error_hpa: str
    reasons: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PressureS9GateRow:
    gate_id: str
    status: str
    required_before: str
    reason: str
    evidence_role: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(f"JSON file not found: {source}")
    payload = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON file must contain an object: {source}")
    return payload


def _load_csv(path: str | Path | None) -> list[dict[str, str]]:
    if not path:
        return []
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(f"CSV file not found: {source}")
    with source.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _string(value: Any) -> str:
    return str(value or "").strip()


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _string(value).lower() in {"1", "true", "yes", "y", "on", "ok", "pass", "ready", "verified", "approved"}


def _status_ready(value: Any) -> bool:
    text = _string(value).lower()
    return text in READY_WORDS or text.startswith("ready_for_") or text.endswith("_pass")


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _field(row: Mapping[str, Any], *names: str) -> str:
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return _string(value)
    return ""


def _normalize_device_id(value: Any) -> str:
    text = _string(value)
    if text.isdigit():
        return f"{int(text):03d}"
    return text.upper()


def _rows_from_json(payload: Mapping[str, Any], *keys: str) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = []
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            rows.extend(row for row in value if isinstance(row, Mapping))
    if not rows:
        for key in ("devices", "device_rows", "pressure_device_readiness", "readiness_rows"):
            value = payload.get(key)
            if isinstance(value, list):
                rows.extend(row for row in value if isinstance(row, Mapping))
    return rows


def _device_key(row: Mapping[str, Any]) -> str:
    return _normalize_device_id(
        row.get("protocol_device_id")
        or row.get("analyzer_device_id")
        or row.get("device_id")
        or row.get("identity_after")
        or row.get("runtime_device_id")
    )


def _ga_label(row: Mapping[str, Any]) -> str:
    return _field(row, "ga_label", "analyzer_prefix", "channel", "label").upper()


def _list_values(value: Any) -> list[float]:
    if isinstance(value, Mapping):
        value = value.get("values")
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            decoded = json.loads(text)
            value = decoded
        except Exception:
            tokens = re.findall(r"C\d+\s*:\s*([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)", text)
            if tokens:
                value = tokens
            else:
                value = [part.strip() for part in text.replace(";", ",").split(",") if part.strip()]
    if not isinstance(value, Sequence) or isinstance(value, (bytes, bytearray, str)):
        return []
    out: list[float] = []
    for item in value:
        numeric = _safe_float(item)
        if numeric is None:
            return []
        out.append(numeric)
    return out


def _getco9_values(row: Mapping[str, Any]) -> list[float]:
    getco = row.get("getco")
    if isinstance(getco, Mapping):
        values = _list_values(getco.get("GETCO9") or getco.get("getco9"))
        if values:
            return values
    for name in (
        "getco9_readback_values",
        "GETCO9_after",
        "GETCO9",
        "getco9_after",
        "readback_getco9",
        "old_getco9",
        "senco9_readback",
        "senco9_values",
    ):
        values = _list_values(row.get(name))
        if values:
            return values
    c_values: list[float] = []
    for idx in range(4):
        numeric = _safe_float(row.get(f"C{idx}") or row.get(f"c{idx}"))
        if numeric is None:
            break
        c_values.append(numeric)
    return c_values


def _format_values(values: Sequence[float]) -> str:
    if not values:
        return ""
    return ",".join(f"{value:.9g}" for value in values)


def _model_from_rows(*rows: Mapping[str, Any]) -> str:
    tokens: list[str] = []
    for row in rows:
        for name in (
            "s9_model",
            "s9_action",
            "model_type",
            "senco9_model",
            "candidate_model",
            "exception_type",
            "recommendation",
            "reason",
        ):
            value = _field(row, name).lower()
            if value:
                tokens.append(value)
    text = " ".join(tokens)
    if any(word in text for word in LINEAR_EXCEPTION_WORDS):
        return "linear_s9_controlled_exception"
    if any(word in text for word in NO_WRITE_READY_WORDS):
        return "no_write_pressure_pass"
    if "linear" in text and "exception" in text:
        return "linear_s9_controlled_exception"
    if "linear" in text and "offset" not in text:
        return "linear_without_exception"
    if any(word in text for word in OFFSET_ONLY_WORDS):
        return "offset_only"
    return "offset_only"


def _fit_ready(row: Mapping[str, Any], *, model: str) -> tuple[bool, str]:
    if not row:
        return False, "s9_no_write_fit_evidence_missing"
    status = row.get("status") or row.get("fit_status") or row.get("no_write_status") or row.get("recommendation")
    if not _status_ready(status):
        return False, f"s9_no_write_fit_status={_string(status) or 'missing'}"
    recommendation = _field(row, "recommendation", "reason").lower()
    if model == "linear_s9_controlled_exception":
        linear_residual = _safe_float(
            row.get("linear_residual_max_abs_hpa")
            or row.get("linear_max_abs_error_hpa")
            or row.get("linear_residual_max_hpa")
        )
        if linear_residual is not None and linear_residual > DEFAULT_MAX_LINEAR_EXCEPTION_RESIDUAL_HPA:
            return False, f"linear_no_write_residual_hpa={linear_residual:.3f}>limit={DEFAULT_MAX_LINEAR_EXCEPTION_RESIDUAL_HPA:.3f}"
        return True, ""
    if model == "linear_without_exception":
        return False, "linear_s9_model_requires_explicit_controlled_exception"
    if "do_not_write" in recommendation or "investigate" in recommendation:
        return False, f"s9_no_write_recommendation={recommendation}"
    return True, ""


def _linear_exception_authorized(row: Mapping[str, Any], *, model: str) -> bool:
    if model != "linear_s9_controlled_exception":
        return False
    text = " ".join(
        _field(row, name).lower()
        for name in (
            "exception_type",
            "s9_model",
            "s9_action",
            "authorization_status",
            "exception_review_status",
            "review_status",
            "notes",
        )
    )
    return any(word in text for word in LINEAR_EXCEPTION_WORDS) and (
        _truthy(row.get("linear_exception_authorized"))
        or _truthy(row.get("exception_authorized"))
        or _status_ready(row.get("exception_review_status"))
        or _status_ready(row.get("review_status"))
        or _status_ready(row.get("authorization_status"))
    )


def _readback_ready(row: Mapping[str, Any], *, model: str) -> tuple[bool, str, list[float]]:
    if model == "no_write_pressure_pass":
        values = _getco9_values(row)
        return bool(values), "" if values else "getco9_snapshot_missing_for_no_write_pressure_pass", values
    if not row:
        return False, "senco9_controlled_write_readback_missing", []
    status = (
        row.get("readback_status")
        or row.get("senco9_write_status")
        or row.get("write_status")
        or row.get("status")
    )
    if not _status_ready(status):
        return False, f"senco9_readback_status={_string(status) or 'missing'}", _getco9_values(row)
    values = _getco9_values(row)
    if len(values) < 4:
        return False, "getco9_readback_values_missing_or_short", values
    if abs(values[2]) > 1e-6 or abs(values[3]) > 1e-6:
        return False, "getco9_c2_c3_must_remain_zero", values
    if model == "offset_only" and abs(values[1] - 1.0) > 1e-6:
        return False, "offset_only_getco9_c1_must_equal_1", values
    if model == "linear_s9_controlled_exception" and abs(values[1] - 1.0) <= 1e-6:
        return False, "linear_exception_getco9_c1_should_not_be_default_1", values
    if model == "linear_without_exception":
        return False, "linear_s9_model_requires_explicit_controlled_exception", values
    return True, "", values


def _reverify_ready(row: Mapping[str, Any], *, model: str) -> tuple[bool, str, str]:
    if not row:
        return False, "post_write_pressure_reverify_missing", ""
    status = (
        row.get("pressure_reverify_status")
        or row.get("post_write_reverify_status")
        or row.get("readiness_status")
        or row.get("pressure_status")
        or row.get("status")
    )
    if not _status_ready(status):
        return False, f"pressure_reverify_status={_string(status) or 'missing'}", ""
    residual = _safe_float(
        row.get("max_abs_diff_hpa")
        or row.get("max_abs_delta_hpa")
        or row.get("post_write_max_abs_error_hpa")
        or row.get("pressure_reverify_max_abs_hpa")
        or row.get("max_residual_hpa")
    )
    if residual is None:
        return False, "pressure_reverify_max_abs_error_missing", ""
    limit = DEFAULT_MAX_LINEAR_EXCEPTION_RESIDUAL_HPA if model == "linear_s9_controlled_exception" else DEFAULT_MAX_REVERIFY_RESIDUAL_HPA
    if residual > limit:
        return False, f"pressure_reverify_max_abs_error_hpa={residual:.3f}>limit={limit:.3f}", f"{residual:.6g}"
    return True, "", f"{residual:.6g}"


def _merge_rows(
    fit_rows: Sequence[Mapping[str, Any]],
    write_rows: Sequence[Mapping[str, Any]],
    reverify_rows: Sequence[Mapping[str, Any]],
) -> list[PressureS9DeviceRow]:
    devices = sorted({_device_key(row) for row in (*fit_rows, *write_rows, *reverify_rows) if _device_key(row)})
    fit_by_device = {_device_key(row): row for row in fit_rows if _device_key(row)}
    write_by_device = {_device_key(row): row for row in write_rows if _device_key(row)}
    reverify_by_device = {_device_key(row): row for row in reverify_rows if _device_key(row)}
    output: list[PressureS9DeviceRow] = []
    for device in devices:
        fit_row = fit_by_device.get(device, {})
        write_row = write_by_device.get(device, {})
        reverify_row = reverify_by_device.get(device, {})
        model = _model_from_rows(fit_row, write_row, reverify_row)
        reasons: list[str] = []

        no_write_ready, reason = _fit_ready(fit_row, model=model)
        if reason:
            reasons.append(reason)

        readback_ready, reason, getco9_values = _readback_ready(write_row or reverify_row, model=model)
        if reason:
            reasons.append(reason)

        reverify_ready, reason, residual = _reverify_ready(reverify_row, model=model)
        if reason:
            reasons.append(reason)

        linear_authorized = _linear_exception_authorized(write_row or fit_row or reverify_row, model=model)
        if model == "linear_s9_controlled_exception" and not linear_authorized:
            reasons.append("linear_s9_controlled_exception_authorization_missing")
        if model == "linear_without_exception":
            reasons.append("linear_s9_model_requires_explicit_controlled_exception")

        row = next((item for item in (write_row, reverify_row, fit_row) if item), {})
        ready = (
            no_write_ready
            and readback_ready
            and reverify_ready
            and model != "linear_without_exception"
            and (model != "linear_s9_controlled_exception" or linear_authorized)
        )
        output.append(
            PressureS9DeviceRow(
                ga_label=_ga_label(row),
                port=_field(row, "port", "com_port"),
                protocol_device_id=device,
                sn_code=_field(row, "sn_code", "device_code"),
                device_code=_field(row, "device_code", "sn_code"),
                s9_action=_field(row, "s9_action", "action") or ("no_write_pressure_pass" if model == "no_write_pressure_pass" else "controlled_senco9_write_readback_reverify"),
                s9_model=model,
                no_write_fit_ready=no_write_ready,
                controlled_write_readback_ready=readback_ready,
                pressure_reverify_ready=reverify_ready,
                linear_exception_authorized=linear_authorized,
                ready_for_pressure_s9_index=ready,
                getco9_readback_values=_format_values(getco9_values),
                reverify_max_abs_error_hpa=residual,
                reasons=";".join(reasons),
            )
        )
    return output


def _gate(
    gate_id: str,
    ready: bool,
    *,
    required_before: str,
    reason: str,
    evidence_role: str,
) -> PressureS9GateRow:
    return PressureS9GateRow(
        gate_id=gate_id,
        status="pass" if ready else "review_required",
        required_before=required_before,
        reason="" if ready else reason,
        evidence_role=evidence_role,
    )


def _collect_rows(
    *,
    csv_path: str | Path | None,
    json_path: str | Path | None,
    json_keys: Sequence[str],
) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = []
    rows.extend(_load_csv(csv_path))
    payload = _load_json(json_path)
    rows.extend(_rows_from_json(payload, *json_keys))
    return rows


def build_v1_5_pressure_s9_readiness_index(
    *,
    no_write_fit_summary_csv: str | Path | None = None,
    no_write_fit_summary_json: str | Path | None = None,
    senco9_write_readback_csv: str | Path | None = None,
    senco9_write_readback_json: str | Path | None = None,
    pressure_reverify_csv: str | Path | None = None,
    pressure_reverify_json: str | Path | None = None,
) -> dict[str, Any]:
    fit_rows = _collect_rows(
        csv_path=no_write_fit_summary_csv,
        json_path=no_write_fit_summary_json,
        json_keys=("pressure_fit_summary", "fit_summary", "fit_rows", "devices"),
    )
    write_rows = _collect_rows(
        csv_path=senco9_write_readback_csv,
        json_path=senco9_write_readback_json,
        json_keys=("senco9_write_readback", "write_readback_rows", "pressure_senco9_write_summary", "devices"),
    )
    reverify_rows = _collect_rows(
        csv_path=pressure_reverify_csv,
        json_path=pressure_reverify_json,
        json_keys=("pressure_reverify_rows", "pressure_device_readiness", "readiness_rows", "devices"),
    )
    device_rows = _merge_rows(fit_rows, write_rows, reverify_rows)
    device_count_ready = 1 <= len(device_rows) <= 6
    all_devices_ready = device_count_ready and all(row.ready_for_pressure_s9_index for row in device_rows)
    linear_exception_rows = [row for row in device_rows if row.s9_model == "linear_s9_controlled_exception"]
    unauthorized_linear_rows = [row for row in linear_exception_rows if not row.linear_exception_authorized]

    gate_rows = [
        _gate(
            "pressure_s9_device_count_1_to_6",
            device_count_ready,
            required_before="batch_initialization_closeout_pre_gas_index",
            reason=f"active_pressure_s9_device_count={len(device_rows)}_outside_1_to_6",
            evidence_role="batch pressure/S9 scope",
        ),
        _gate(
            "pressure_s9_no_write_fit_basis",
            bool(device_rows) and all(row.no_write_fit_ready for row in device_rows),
            required_before="senco9_controlled_write_readback",
            reason="one_or_more_devices_missing_no_write_fit_basis",
            evidence_role="pressure-only no-write fit/review",
        ),
        _gate(
            "pressure_s9_write_readback",
            bool(device_rows) and all(row.controlled_write_readback_ready for row in device_rows),
            required_before="post_write_pressure_reverify",
            reason="one_or_more_devices_missing_senco9_readback",
            evidence_role="SENCO9 write/readback or explicit no-write snapshot",
        ),
        _gate(
            "linear_s9_controlled_exception_scope",
            not unauthorized_linear_rows,
            required_before="post_write_pressure_reverify",
            reason="linear_s9_exception_without_explicit_authorization",
            evidence_role="explicit linear S9 exception review",
        ),
        _gate(
            "pressure_s9_post_write_reverify",
            bool(device_rows) and all(row.pressure_reverify_ready for row in device_rows),
            required_before="mature_open_flow_route",
            reason="one_or_more_devices_missing_post_write_pressure_reverify",
            evidence_role="pressure-only no-write reverify after S9 state",
        ),
    ]
    review_reasons = [row.reason for row in gate_rows if row.reason]
    review_reasons.extend(row.reasons for row in device_rows if row.reasons)
    overall_status = READY_STATUS if all_devices_ready and all(row.status == "pass" for row in gate_rows) else REVIEW_STATUS
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": overall_status,
        "ready_for_mature_open_flow_pressure_s9_index": overall_status == READY_STATUS,
        "device_count": len(device_rows),
        "device_ready_count": sum(1 for row in device_rows if row.ready_for_pressure_s9_index),
        "linear_exception_count": len(linear_exception_rows),
        "review_reasons": review_reasons,
        "no_write_fit_summary_csv": str(no_write_fit_summary_csv or ""),
        "no_write_fit_summary_json": str(no_write_fit_summary_json or ""),
        "senco9_write_readback_csv": str(senco9_write_readback_csv or ""),
        "senco9_write_readback_json": str(senco9_write_readback_json or ""),
        "pressure_reverify_csv": str(pressure_reverify_csv or ""),
        "pressure_reverify_json": str(pressure_reverify_json or ""),
        "mature_route_baseline": "0620/0621 clean worktree mature physical route",
        "mature_fitting_baseline": "0613 V1.5 fitting path",
        "pressure_s9_policy": {
            "default_model": "offset_only",
            "linear_s9_allowed_only_as": "explicit controlled exception with write/readback/reverify evidence",
            "post_write_reverify_max_abs_error_hpa": DEFAULT_MAX_REVERIFY_RESIDUAL_HPA,
            "linear_exception_reverify_max_abs_error_hpa": DEFAULT_MAX_LINEAR_EXCEPTION_RESIDUAL_HPA,
        },
        "full_production_auto_allowed": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "opens_com_ports": False,
        "read_only_real_com_execution_allowed": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "connects_postgresql": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "writes_senco9": False,
        "database_written": False,
        "not_real_acceptance_evidence": True,
        "device_rows": [row.to_json() for row in device_rows],
        "gate_rows": [row.to_json() for row in gate_rows],
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    keys: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in keys:
                keys.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys or ["empty"])
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))


def _markdown(model: Mapping[str, Any]) -> str:
    lines = [
        "# V1.5 Pressure/SENCO9 Readiness Index",
        "",
        f"- schema: `{model['schema']}`",
        f"- overall_status: `{model['overall_status']}`",
        f"- ready_for_mature_open_flow_pressure_s9_index: `{str(model['ready_for_mature_open_flow_pressure_s9_index']).lower()}`",
        f"- device_count: `{model['device_count']}`",
        f"- device_ready_count: `{model['device_ready_count']}`",
        f"- linear_exception_count: `{model['linear_exception_count']}`",
        f"- mature_route_baseline: `{model['mature_route_baseline']}`",
        "",
        "## Meaning",
        "",
        "This artifact explains pressure/SENCO9 readiness before mature CO2/H2O open-flow routes. It separates no-write fit evidence, controlled write/readback, and pressure-only reverify evidence.",
        "",
        "## Gates",
        "",
        "| gate_id | status | required_before | reason |",
        "|---|---|---|---|",
    ]
    for row in model["gate_rows"]:
        lines.append(
            "| `{gate}` | `{status}` | `{required}` | {reason} |".format(
                gate=row["gate_id"],
                status=row["status"],
                required=row["required_before"],
                reason=row.get("reason") or "",
            )
        )
    lines.extend(
        [
            "",
            "## Devices",
            "",
            "| GA | port | protocol_id | SN | S9 model | ready | residual hPa | reasons |",
            "|---|---|---|---|---|---|---:|---|",
        ]
    )
    for row in model["device_rows"]:
        lines.append(
            "| {ga} | `{port}` | `{pid}` | `{sn}` | `{model}` | `{ready}` | `{residual}` | {reasons} |".format(
                ga=row["ga_label"],
                port=row["port"],
                pid=row["protocol_device_id"],
                sn=row["sn_code"],
                model=row["s9_model"],
                ready=str(row["ready_for_pressure_s9_index"]).lower(),
                residual=row.get("reverify_max_abs_error_hpa") or "",
                reasons=row.get("reasons") or "",
            )
        )
    if model.get("review_reasons"):
        lines.extend(["", "## Review Reasons", ""])
        for reason in model["review_reasons"]:
            lines.append(f"- `{reason}`")
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- Default mature S9 model is `offset_only`.",
            "- Linear S9 is allowed only as an explicit controlled exception with write/readback/reverify evidence.",
            "- This artifact is not a SENCO9 writer and not pressure hardware control.",
            "",
            "## Non-Execution Boundary",
            "",
            "- opens_com_ports: `false`",
            "- controls_pressure: `false`",
            "- controls_water_or_gas_routes: `false`",
            "- connects_postgresql: `false`",
            "- writes_sn: `false`",
            "- writes_device_id: `false`",
            "- writes_coefficients: `false`",
            "- writes_senco9: `false`",
            "- formal_release_allowed: `false`",
            "- database_import_allowed: `false`",
            "- not_real_acceptance_evidence: `true`",
            "",
        ]
    )
    return "\n".join(lines)


def write_v1_5_pressure_s9_readiness_index(
    *,
    output_dir: str | Path,
    no_write_fit_summary_csv: str | Path | None = None,
    no_write_fit_summary_json: str | Path | None = None,
    senco9_write_readback_csv: str | Path | None = None,
    senco9_write_readback_json: str | Path | None = None,
    pressure_reverify_csv: str | Path | None = None,
    pressure_reverify_json: str | Path | None = None,
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    model = build_v1_5_pressure_s9_readiness_index(
        no_write_fit_summary_csv=no_write_fit_summary_csv,
        no_write_fit_summary_json=no_write_fit_summary_json,
        senco9_write_readback_csv=senco9_write_readback_csv,
        senco9_write_readback_json=senco9_write_readback_json,
        pressure_reverify_csv=pressure_reverify_csv,
        pressure_reverify_json=pressure_reverify_json,
    )
    paths = {
        "manifest": out / "v1_5_pressure_s9_readiness_index.json",
        "devices": out / "v1_5_pressure_s9_readiness_index_devices.csv",
        "gates": out / "v1_5_pressure_s9_readiness_index_gates.csv",
        "markdown": out / "V1_5_PRESSURE_S9_READINESS_INDEX.md",
    }
    paths["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(paths["devices"], model["device_rows"])
    _write_csv(paths["gates"], model["gate_rows"])
    paths["markdown"].write_text(_markdown(model), encoding="utf-8")
    return paths


__all__ = [
    "READY_STATUS",
    "REVIEW_STATUS",
    "SCHEMA",
    "build_v1_5_pressure_s9_readiness_index",
    "write_v1_5_pressure_s9_readiness_index",
]
