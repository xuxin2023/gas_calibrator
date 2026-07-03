"""Offline V1.5 H2O formal closure review.

This module is deliberately evidence-only. It never opens COM ports, controls
the water/gas route, controls the humidity generator, or writes SENCO
coefficients. Its job is to connect the completed H2O evidence chain:

* SENCO2/SENCO4 main H2O ratio-temperature candidate review.
* Dry-gas anchors used only when dewpoint/pressure evidence gives the residual
  water amount.
* SENCO6 final firmware output affine trim, kept separate from the main model.
* Optional controlled write/readback events.

The review is meant to prevent the next formal run from repeating the same
class of mistakes: folding SENCO6 into SENCO2/SENCO4, using dry gas as hard
zero water, writing malformed decimal commands, or reporting without enough
traceability.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Sequence, Tuple


SENCO24_CONTRACT = "senco24_h2o_ratio_temperature_main_chain_only"
SENCO6_CONTRACT = "senco6_separate_final_affine_layer_do_not_fold_into_senco24"
DRY_ANCHOR_CONTRACT = (
    "dry_gas_anchor_target_is_dewpoint_pressure_derived_residual_h2o_not_zero"
)


@dataclass(frozen=True)
class H2OFormalClosureConfig:
    """Policy for the sidecar H2O closure review."""

    target_device_ids: Tuple[str, ...] = field(default_factory=tuple)
    dry_anchor_required: bool = False
    require_senco6_review: bool = True
    require_verified_writes: bool = False
    fit_temperature_source: str = "digital_thermometer"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe_float(value: Any) -> float | None:
    if value in (None, "", "None", "null"):
        return None
    try:
        number = float(value)
    except Exception:
        return None
    if not math.isfinite(number):
        return None
    return number


def _normal_device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.startswith("GA"):
        text = text[2:]
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _device_from_row(row: Mapping[str, Any]) -> str:
    for key in (
        "device_id",
        "analyzer_device_id",
        "analyzer_id",
        "AnalyzerDeviceId",
        "sensor_id",
        "id",
    ):
        value = row.get(key)
        device_id = _normal_device_id(value)
        if device_id:
            return device_id
    return ""


def _read_csv(path: str | Path | None) -> List[Dict[str, Any]]:
    if not path:
        return []
    csv_path = Path(path)
    if not csv_path.exists():
        return []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    header: List[str] = []
    for row in rows:
        for key in row:
            if key not in header:
                header.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8-sig")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8-sig")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _index_latest_by_device(rows: Iterable[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    indexed: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        device_id = _device_from_row(row)
        if device_id:
            indexed[device_id] = dict(row)
    return indexed


def _rows_by_device(rows: Iterable[Mapping[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        device_id = _device_from_row(row)
        if device_id:
            grouped.setdefault(device_id, []).append(dict(row))
    return grouped


def _status_value(row: Mapping[str, Any]) -> str:
    for key in (
        "candidate_status",
        "review_status",
        "write_status",
        "status",
        "formal_acceptance_status",
    ):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _is_ready_status(value: str) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    if any(token in text for token in ("blocked", "failed", "error", "reject")):
        return False
    return any(token in text for token in ("review_ready", "ready", "pass", "verified"))


def _is_verified_write(row: Mapping[str, Any]) -> bool:
    text = " ".join(
        str(row.get(key) or "")
        for key in ("status", "write_status", "readback_status", "verification_status")
    ).lower()
    return "written_readback_verified" in text or (
        "readback" in text and "verified" in text and "failed" not in text
    )


def _best_metric(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> str:
    values: List[float] = []
    for row in rows:
        for key in keys:
            value = _safe_float(row.get(key))
            if value is not None:
                values.append(abs(value))
                break
    if not values:
        return ""
    return f"{max(values):.6g}"


def _has_bare_decimal_senco6_command(command: str) -> bool:
    text = str(command or "").strip()
    if not text:
        return False
    parts = [part.strip() for part in text.split(",")]
    if len(parts) < 5:
        return False
    for value in parts[3:5]:
        if re.match(r"^[+-]?\.\d+", value):
            return True
    return False


def _senco6_command(row: Mapping[str, Any]) -> str:
    for key in ("command_preview", "command", "senco6_command", "write_command"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    c0 = row.get("command_C0", row.get("candidate_C0", row.get("C0", "")))
    c1 = row.get("command_C1", row.get("candidate_C1", row.get("C1", "")))
    if c0 != "" and c1 != "":
        return f"SENCO6,YGAS,FFF,{c0},{c1}"
    return ""


def _senco6_source_contract(row: Mapping[str, Any]) -> str:
    for key in (
        "input_source_contract",
        "senco6_input_contract",
        "source",
        "verification_source",
        "measured_h2o_source",
    ):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _senco6_source_is_independent(source: str) -> bool:
    text = str(source or "").lower()
    if not text:
        return False
    if any(token in text for token in ("model_pred", "model_prediction", "senco24_model", "not_current_firmware")):
        return False
    return any(
        token in text
        for token in (
            "firmware",
            "postwrite",
            "post_write",
            "reported_output",
            "analyzer_reported",
            "live_output",
        )
    )


def _dry_anchor_contract(rows: Sequence[Mapping[str, Any]]) -> Tuple[bool, str, str]:
    if not rows:
        return False, "missing", ""
    blockers: List[str] = []
    warnings: List[str] = []
    for row in rows:
        role_text = " ".join(
            str(row.get(key) or "")
            for key in ("sample_role", "role", "point_role", "residual_role")
        ).lower()
        if "dry" not in role_text and "anchor" not in role_text:
            continue
        source_text = " ".join(
            str(row.get(key) or "")
            for key in ("reference_source", "source", "target_source", "sample_source_contract")
        ).lower()
        if "dewpoint" not in source_text or ("pressure" not in source_text and "com22" not in source_text):
            blockers.append("dry_anchor_missing_dewpoint_pressure_reference")
        target = None
        for key in (
            "reference_h2o_mmol",
            "target_h2o_mmol",
            "ppm_H2O_Dew",
            "reference_value",
            "target_value",
        ):
            target = _safe_float(row.get(key))
            if target is not None:
                break
        if target is None:
            blockers.append("dry_anchor_missing_residual_h2o_target")
        elif abs(target) <= 1.0e-12:
            blockers.append("dry_anchor_target_was_forced_to_zero")
        elif target < 0.0:
            warnings.append("dry_anchor_negative_residual_h2o_target_review_required")
    if blockers:
        return True, "blocked", ";".join(sorted(set(blockers)))
    if warnings:
        return True, "warn", ";".join(sorted(set(warnings)))
    return True, "pass", ""


def _extract_device_ids(*row_sets: Iterable[Mapping[str, Any]], target_ids: Sequence[str]) -> List[str]:
    device_ids = {_normal_device_id(value) for value in target_ids if _normal_device_id(value)}
    for rows in row_sets:
        for row in rows:
            device_id = _device_from_row(row)
            if device_id:
                device_ids.add(device_id)
    return sorted(device_ids)


def build_h2o_formal_closure_review(
    *,
    senco24_candidate_csv: str | Path,
    senco24_residuals_csv: str | Path | None = None,
    senco24_point_inputs_csv: str | Path | None = None,
    senco6_candidate_csv: str | Path | None = None,
    senco6_residuals_csv: str | Path | None = None,
    senco24_write_events_csv: str | Path | None = None,
    senco6_write_events_csv: str | Path | None = None,
    config: H2OFormalClosureConfig | None = None,
) -> Dict[str, Any]:
    """Build offline device closure status from already-generated artifacts."""

    cfg = config or H2OFormalClosureConfig()
    s24_candidates = _read_csv(senco24_candidate_csv)
    s24_residuals = _read_csv(senco24_residuals_csv)
    s24_inputs = _read_csv(senco24_point_inputs_csv)
    s6_candidates = _read_csv(senco6_candidate_csv)
    s6_residuals = _read_csv(senco6_residuals_csv)
    s24_writes = _read_csv(senco24_write_events_csv)
    s6_writes = _read_csv(senco6_write_events_csv)

    s24_by_device = _index_latest_by_device(s24_candidates)
    s6_by_device = _index_latest_by_device(s6_candidates)
    s24_residuals_by_device = _rows_by_device(s24_residuals)
    s6_residuals_by_device = _rows_by_device(s6_residuals)
    s24_inputs_by_device = _rows_by_device(s24_inputs)
    s24_writes_by_device = _index_latest_by_device(s24_writes)
    s6_writes_by_device = _index_latest_by_device(s6_writes)

    device_ids = _extract_device_ids(
        s24_candidates,
        s24_residuals,
        s24_inputs,
        s6_candidates,
        s6_residuals,
        s24_writes,
        s6_writes,
        target_ids=cfg.target_device_ids,
    )

    device_rows: List[Dict[str, Any]] = []
    for device_id in device_ids:
        blockers: List[str] = []
        warnings: List[str] = []

        s24_row = s24_by_device.get(device_id, {})
        s24_status = _status_value(s24_row)
        s24_ready = _is_ready_status(s24_status)
        if not s24_row:
            blockers.append("missing_senco24_candidate")
        elif not s24_ready:
            blockers.append("senco24_candidate_not_ready")

        if cfg.fit_temperature_source != "digital_thermometer":
            warnings.append("fit_temperature_source_not_digital_thermometer_review_required")

        dry_present, dry_status, dry_detail = _dry_anchor_contract(s24_inputs_by_device.get(device_id, []))
        if cfg.dry_anchor_required and not dry_present:
            blockers.append("missing_required_h2o_dry_gas_anchor")
        if dry_status == "blocked":
            blockers.append(dry_detail)
        elif dry_status == "warn" and dry_detail:
            warnings.append(dry_detail)

        s6_row = s6_by_device.get(device_id, {})
        s6_status = _status_value(s6_row)
        s6_ready = False
        s6_command = _senco6_command(s6_row)
        s6_source = _senco6_source_contract(s6_row)
        if cfg.require_senco6_review:
            if not s6_row:
                blockers.append("missing_senco6_final_layer_review")
            else:
                s6_ready = _is_ready_status(s6_status)
                if not s6_ready:
                    blockers.append("senco6_review_not_ready")
                if not _senco6_source_is_independent(s6_source):
                    blockers.append("senco6_input_not_independent_firmware_output")
                if _has_bare_decimal_senco6_command(s6_command):
                    blockers.append("senco6_command_uses_bare_decimal_without_leading_zero")
        elif s6_row:
            s6_ready = _is_ready_status(s6_status)

        s24_write_verified = _is_verified_write(s24_writes_by_device.get(device_id, {}))
        s6_write_verified = _is_verified_write(s6_writes_by_device.get(device_id, {}))
        if cfg.require_verified_writes:
            if not s24_write_verified:
                blockers.append("senco24_write_readback_not_verified")
            if cfg.require_senco6_review and not s6_write_verified:
                blockers.append("senco6_write_readback_not_verified")

        if blockers:
            closure_status = "blocked"
        elif cfg.require_verified_writes:
            closure_status = "ready_for_formal_report"
        elif s24_ready and (s6_ready or not cfg.require_senco6_review):
            closure_status = "ready_for_controlled_write_or_report_review"
        elif s24_ready:
            closure_status = "ready_for_senco6_review"
        else:
            closure_status = "needs_review"

        s24_res = s24_residuals_by_device.get(device_id, [])
        s6_res = s6_residuals_by_device.get(device_id, [])
        device_rows.append(
            {
                "device_id": device_id,
                "closure_status": closure_status,
                "senco24_status": s24_status,
                "senco6_status": s6_status,
                "senco24_write_verified": s24_write_verified,
                "senco6_write_verified": s6_write_verified,
                "fit_temperature_source": cfg.fit_temperature_source,
                "dry_anchor_status": dry_status,
                "senco6_command_preview": s6_command,
                "senco6_input_source_contract": s6_source,
                "senco24_max_abs_error": _best_metric(
                    s24_res,
                    (
                        "abs_error_mmol",
                        "error_mmol",
                        "h2o_error_mmol",
                        "max_abs_error_mmol",
                    ),
                ),
                "senco24_max_relative_error_pct": _best_metric(
                    s24_res,
                    (
                        "abs_relative_error_pct",
                        "relative_error_pct",
                        "error_pct",
                        "max_abs_relative_error_pct",
                    ),
                ),
                "senco6_max_abs_error": _best_metric(
                    s6_res,
                    (
                        "corrected_abs_error_mmol",
                        "abs_error_mmol",
                        "error_mmol",
                        "max_abs_error_mmol",
                    ),
                ),
                "senco6_max_relative_error_pct": _best_metric(
                    s6_res,
                    (
                        "corrected_abs_relative_error_pct",
                        "abs_relative_error_pct",
                        "relative_error_pct",
                        "max_abs_relative_error_pct",
                    ),
                ),
                "warnings": ";".join(sorted(set(warnings))),
                "blocked_reasons": ";".join(sorted(set(reason for reason in blockers if reason))),
                "physical_contracts": ";".join(
                    (SENCO24_CONTRACT, SENCO6_CONTRACT, DRY_ANCHOR_CONTRACT)
                ),
            }
        )

    run_status = "ready" if device_rows and all(row["closure_status"] != "blocked" for row in device_rows) else "blocked"
    if not device_rows:
        run_status = "blocked"

    return {
        "summary": {
            "created_at": _now(),
            "run_status": run_status,
            "device_count": len(device_rows),
            "blocked_count": sum(1 for row in device_rows if row["closure_status"] == "blocked"),
            "opens_com_ports": False,
            "controls_water_route": False,
            "writes_coefficients": False,
            "senco24_contract": SENCO24_CONTRACT,
            "senco6_contract": SENCO6_CONTRACT,
            "dry_anchor_contract": DRY_ANCHOR_CONTRACT,
            "fit_temperature_source": cfg.fit_temperature_source,
        },
        "device_status": device_rows,
    }


def _markdown(tables: Mapping[str, Any]) -> str:
    summary = dict(tables.get("summary") or {})
    rows = list(tables.get("device_status") or [])
    lines = [
        "# V1.5 H2O 正式闭环评审",
        "",
        "## 总览",
        "",
        f"- 运行状态：`{summary.get('run_status', '')}`",
        f"- 设备数量：`{summary.get('device_count', 0)}`",
        f"- 阻断数量：`{summary.get('blocked_count', 0)}`",
        "- 本工具只读证据，不打开串口，不控制水路，不写 SENCO。",
        "",
        "## 物理合同",
        "",
        "- SENCO2/SENCO4：只用于 H2O 光学比值与温度主链路拟合。",
        "- SENCO6：只作为固件最终 H2O 输出层线性修正，不能混入 SENCO2/SENCO4。",
        "- 干气锚点：只能在露点仪和压力参考给出残余水含量时参与 H2O 低端约束，不能把干气硬当作 0 水。",
        "- 温度输入：优先使用数字测温仪或已验证的温度通道，避免把温度通道错误吸收到 H2O 系数里。",
        "",
        "## 设备结论",
        "",
        "| 设备ID | 闭环状态 | S2/S4 | S6 | S2/S4最大相对误差% | S6最大相对误差% | 阻断原因 |",
        "| --- | --- | --- | --- | ---: | ---: | --- |",
    ]
    for row in rows:
        lines.append(
            "| {device} | {closure} | {s24} | {s6} | {s24err} | {s6err} | {block} |".format(
                device=row.get("device_id", ""),
                closure=row.get("closure_status", ""),
                s24=row.get("senco24_status", ""),
                s6=row.get("senco6_status", ""),
                s24err=row.get("senco24_max_relative_error_pct", ""),
                s6err=row.get("senco6_max_relative_error_pct", ""),
                block=row.get("blocked_reasons", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 下一步判据",
            "",
            "- 若闭环状态为 `ready_for_controlled_write_or_report_review`：可以进入受控写入评审或报告评审。",
            "- 若闭环状态为 `blocked`：先处理阻断原因，不允许把该设备纳入正式报告结论。",
            "- 若 S6 命令出现 `.947` 这类无前导 0 的小数，必须改为 `0.947` 后再写入。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_h2o_formal_closure_review(
    *,
    output_dir: str | Path,
    senco24_candidate_csv: str | Path,
    senco24_residuals_csv: str | Path | None = None,
    senco24_point_inputs_csv: str | Path | None = None,
    senco6_candidate_csv: str | Path | None = None,
    senco6_residuals_csv: str | Path | None = None,
    senco24_write_events_csv: str | Path | None = None,
    senco6_write_events_csv: str | Path | None = None,
    config: H2OFormalClosureConfig | None = None,
) -> Dict[str, Path]:
    output = Path(output_dir).resolve()
    tables = build_h2o_formal_closure_review(
        senco24_candidate_csv=senco24_candidate_csv,
        senco24_residuals_csv=senco24_residuals_csv,
        senco24_point_inputs_csv=senco24_point_inputs_csv,
        senco6_candidate_csv=senco6_candidate_csv,
        senco6_residuals_csv=senco6_residuals_csv,
        senco24_write_events_csv=senco24_write_events_csv,
        senco6_write_events_csv=senco6_write_events_csv,
        config=config,
    )
    device_csv = output / "h2o_formal_closure_device_status.csv"
    manifest_json = output / "h2o_formal_closure_manifest.json"
    markdown_path = output / "h2o_formal_closure_review.md"
    _write_csv(device_csv, tables["device_status"])
    _write_text(markdown_path, _markdown(tables))

    input_paths = [
        path
        for path in (
            senco24_candidate_csv,
            senco24_residuals_csv,
            senco24_point_inputs_csv,
            senco6_candidate_csv,
            senco6_residuals_csv,
            senco24_write_events_csv,
            senco6_write_events_csv,
        )
        if path
    ]
    manifest: MutableMapping[str, Any] = {
        "created_at": _now(),
        "tool": "export_v1_5_h2o_formal_closure_review",
        "summary": tables["summary"],
        "input_paths": [str(Path(path).resolve()) for path in input_paths],
        "outputs": {
            "device_status_csv": str(device_csv),
            "markdown": str(markdown_path),
        },
        "artifact_sha256": {
            "device_status_csv": _sha256(device_csv),
            "markdown": _sha256(markdown_path),
        },
    }
    _write_json(manifest_json, manifest)
    return {
        "device_status_csv": device_csv,
        "manifest_json": manifest_json,
        "markdown": markdown_path,
    }
