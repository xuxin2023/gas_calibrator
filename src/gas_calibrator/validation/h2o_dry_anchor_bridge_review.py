"""Offline H2O dry-anchor bridge review for V1.5.

This review answers a narrow no-COM question: after fitting the H2O main
ratio/temperature chain from water-route wet points, do gas-route dry points
agree with that physical model once their dewpoint/pressure-derived H2O target
and traceable temperature evidence are used?

The output is diagnostic reviewer evidence only. It does not control gas or
water routes, open COM ports, or write SENCO coefficients.
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .h2o_senco24_candidate_review import (
    H2OSenco24CandidateConfig,
    _complete_rows,
    _dry_anchor_rows,
    _normal_device_id,
    _prediction,
    _resolve_dry_anchor_root,
    build_h2o_senco24_candidate_tables,
)


@dataclass(frozen=True)
class H2ODryAnchorBridgeConfig:
    """Policy for H2O dry-anchor bridge evaluation."""

    min_points: int = 8
    min_wet_points: int = 3
    fit_temperature_source: str = "digital_thermometer"
    fit_objective: str = "relative_mmol_floor"
    relative_error_min_reference_mmol: float = 2.0
    wet_fit_max_abs_error_mmol: float = 0.5
    design_max_relative_error_pct: float = 2.0
    bridge_max_abs_error_mmol: float = 0.25
    bridge_max_relative_error_pct: float = 2.0
    bridge_relative_error_min_reference_mmol: float = 2.0
    allow_pressure_qc_failed_device_ids: Tuple[str, ...] = field(default_factory=tuple)
    component_snapshot: Mapping[str, Any] = field(default_factory=dict)
    require_component_snapshot_for_layer_review: bool = True
    dry_anchor_min_temp_c: Optional[float] = None
    dry_anchor_max_temp_c: Optional[float] = None


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    return numeric if math.isfinite(numeric) else None


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers: List[str] = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows([dict(row) for row in rows])


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")


def _coefficients_by_device(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Dict[str, float]]:
    coeffs: Dict[str, Dict[str, float]] = {}
    for row in rows:
        device_id = _normal_device_id(row.get("analyzer_device_id"))
        term = str(row.get("term") or "").strip()
        value = _safe_float(row.get("coefficient"))
        if not device_id or not term or value is None:
            continue
        coeffs.setdefault(device_id, {})[term] = value
    return coeffs


def _policy_by_device(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Mapping[str, Any]]:
    return {
        _normal_device_id(row.get("analyzer_device_id")): row
        for row in rows
        if _normal_device_id(row.get("analyzer_device_id"))
    }


def _candidate_config(cfg: H2ODryAnchorBridgeConfig) -> H2OSenco24CandidateConfig:
    return H2OSenco24CandidateConfig(
        min_points=int(cfg.min_points),
        min_wet_points=int(cfg.min_wet_points),
        fit_max_abs_error_mmol=float(cfg.wet_fit_max_abs_error_mmol),
        design_max_relative_error_pct=float(cfg.design_max_relative_error_pct),
        relative_error_min_reference_mmol=float(cfg.relative_error_min_reference_mmol),
        allow_pressure_qc_failed_device_ids=tuple(cfg.allow_pressure_qc_failed_device_ids or ()),
        fit_temperature_source=str(cfg.fit_temperature_source or "digital_thermometer"),
        fit_objective=str(cfg.fit_objective or "relative_mmol_floor"),
        component_snapshot=dict(cfg.component_snapshot or {}),
        require_component_snapshot_for_layer_review=bool(cfg.require_component_snapshot_for_layer_review),
    )


def _relative_error_pct(
    error_mmol: float,
    target_mmol: float,
    *,
    min_reference_mmol: float,
) -> float | str:
    if abs(float(target_mmol)) < float(min_reference_mmol):
        return ""
    return float(error_mmol) / float(target_mmol) * 100.0


def _bridge_status(
    *,
    abs_error_mmol: float,
    rel_error_pct: float | str,
    cfg: H2ODryAnchorBridgeConfig,
) -> str:
    abs_ok = abs(float(abs_error_mmol)) <= float(cfg.bridge_max_abs_error_mmol)
    rel_ok = rel_error_pct == "" or abs(float(rel_error_pct)) <= float(cfg.bridge_max_relative_error_pct)
    if abs_ok and rel_ok:
        return "bridge_fit_compatible"
    if abs_ok:
        return "bridge_qc_only_relative_limit"
    return "bridge_qc_only_model_mismatch"


def _device_recommendation(
    *,
    wet_policy: Mapping[str, Any],
    dry_count: int,
    compatible_count: int,
    qc_only_count: int,
    cfg: H2ODryAnchorBridgeConfig,
) -> str:
    if dry_count == 0:
        return "no_dry_anchor_evidence"
    wet_rel = _safe_float(wet_policy.get("fit_max_abs_relative_error_pct"))
    wet_abs = _safe_float(wet_policy.get("fit_max_error_mmol"))
    wet_ok = (
        wet_rel is not None
        and wet_abs is not None
        and wet_rel <= float(cfg.design_max_relative_error_pct)
        and wet_abs <= float(cfg.wet_fit_max_abs_error_mmol)
    )
    if compatible_count == dry_count:
        return "dry_anchors_can_enter_low_end_fit_review"
    if compatible_count > 0:
        return "compatible_dry_anchor_subset_can_enter_low_end_review"
    if wet_ok and qc_only_count:
        return "wet_points_main_fit_keep_dry_anchors_as_qc"
    return "collect_new_formal_dry_h2o_anchor_evidence"


def _strategy_cfg(
    cfg: H2ODryAnchorBridgeConfig,
    *,
    dry_anchor_roots: Tuple[str, ...],
    dry_anchor_max_temp_c: Optional[float],
    fit_objective: str,
) -> H2OSenco24CandidateConfig:
    base = _candidate_config(cfg)
    return H2OSenco24CandidateConfig(
        min_points=base.min_points,
        min_wet_points=base.min_wet_points,
        fit_max_abs_error_mmol=999.0,
        design_max_relative_error_pct=999.0,
        relative_error_min_reference_mmol=base.relative_error_min_reference_mmol,
        allow_pressure_qc_failed_device_ids=base.allow_pressure_qc_failed_device_ids,
        fit_temperature_source=base.fit_temperature_source,
        fit_objective=fit_objective,
        component_snapshot=base.component_snapshot,
        require_component_snapshot_for_layer_review=base.require_component_snapshot_for_layer_review,
        dry_anchor_roots=dry_anchor_roots,
        dry_anchor_max_temp_c=dry_anchor_max_temp_c,
    )


def _strategy_rows(
    *,
    wet_run_dir: str | Path,
    dry_anchor_roots: Tuple[str, ...],
    cfg: H2ODryAnchorBridgeConfig,
) -> List[Dict[str, Any]]:
    strategies = [
        ("wet_only_relative", (), None, "relative_mmol_floor"),
        ("dry_all_absolute", dry_anchor_roots, None, "absolute_mmol"),
        ("dry_all_relative", dry_anchor_roots, None, "relative_mmol_floor"),
        ("dry_le_0_relative", dry_anchor_roots, 0.0, "relative_mmol_floor"),
        ("dry_le_minus10_relative", dry_anchor_roots, -10.0, "relative_mmol_floor"),
    ]
    rows: List[Dict[str, Any]] = []
    for strategy_id, roots, max_temp_c, objective in strategies:
        tables, _ = build_h2o_senco24_candidate_tables(
            run_dir=wet_run_dir,
            cfg=_strategy_cfg(
                cfg,
                dry_anchor_roots=roots,
                dry_anchor_max_temp_c=max_temp_c,
                fit_objective=objective,
            ),
        )
        policy_rows = tables["h2o_senco24_device_policy"]
        if not policy_rows:
            rows.append(
                {
                    "strategy_id": strategy_id,
                    "fit_objective": objective,
                    "dry_anchor_max_temp_c": "" if max_temp_c is None else max_temp_c,
                    "dry_anchor_roots_used": bool(roots),
                    "worst_device_id": "",
                    "worst_max_relative_error_pct": "",
                    "mean_device_max_relative_error_pct": "",
                    "sum_device_max_abs_error_mmol": "",
                    "physical_meaning": _strategy_physical_meaning(strategy_id),
                    "strategy_status": "no_fit_devices",
                }
            )
            continue
        rel_values = [
            _safe_float(row.get("fit_max_abs_relative_error_pct")) or 0.0 for row in policy_rows
        ]
        abs_values = [_safe_float(row.get("fit_max_error_mmol")) or 0.0 for row in policy_rows]
        worst = max(
            policy_rows,
            key=lambda row: _safe_float(row.get("fit_max_abs_relative_error_pct")) or 0.0,
        )
        rows.append(
            {
                "strategy_id": strategy_id,
                "fit_objective": objective,
                "dry_anchor_max_temp_c": "" if max_temp_c is None else max_temp_c,
                "dry_anchor_roots_used": bool(roots),
                "worst_device_id": worst.get("analyzer_device_id", ""),
                "worst_max_relative_error_pct": max(rel_values) if rel_values else "",
                "mean_device_max_relative_error_pct": sum(rel_values) / len(rel_values)
                if rel_values
                else "",
                "sum_device_max_abs_error_mmol": sum(abs_values),
                "physical_meaning": _strategy_physical_meaning(strategy_id),
                "strategy_status": "evaluated",
            }
        )
    return rows


def _strategy_physical_meaning(strategy_id: str) -> str:
    if strategy_id == "wet_only_relative":
        return "Main H2O wet-route response only; dry anchors retained for independent bridge QC."
    if strategy_id == "dry_all_absolute":
        return "Historical baseline; all dry anchors enter absolute-error fit, regardless of state equivalence."
    if strategy_id == "dry_all_relative":
        return "All dry anchors enter a relative-error fit; useful to detect whether dry anchors conflict globally."
    if strategy_id == "dry_le_0_relative":
        return "Only 0 C and below dry anchors enter relative-error fit."
    return "Only subzero dry anchors enter relative-error fit."


def build_h2o_dry_anchor_bridge_tables(
    *,
    wet_run_dir: str | Path,
    dry_anchor_run_dirs: Sequence[str | Path],
    cfg: H2ODryAnchorBridgeConfig = H2ODryAnchorBridgeConfig(),
) -> Dict[str, List[Dict[str, Any]] | Dict[str, Any]]:
    """Build no-write H2O dry-anchor bridge review tables."""

    wet_cfg = _candidate_config(cfg)
    wet_tables, wet_context = build_h2o_senco24_candidate_tables(run_dir=wet_run_dir, cfg=wet_cfg)
    coeffs_by_device = _coefficients_by_device(wet_tables["h2o_senco24_coefficients"])
    policies_by_device = _policy_by_device(wet_tables["h2o_senco24_device_policy"])

    dry_roots = tuple(str(_resolve_dry_anchor_root(path)) for path in dry_anchor_run_dirs)
    raw_dry_rows: List[Dict[str, Any]] = []
    for root in dry_roots:
        raw_dry_rows.extend(
            _dry_anchor_rows(
                Path(root),
                min_temp_c=cfg.dry_anchor_min_temp_c,
                max_temp_c=cfg.dry_anchor_max_temp_c,
            )
        )

    complete_dry_rows, rejected_dry_rows = _complete_rows(raw_dry_rows, cfg=wet_cfg)
    dry_predictions: List[Dict[str, Any]] = []
    rejected_predictions: List[Dict[str, Any]] = []
    for row in complete_dry_rows:
        device_id = _normal_device_id(row.get("analyzer_device_id"))
        coefficients = coeffs_by_device.get(device_id)
        if not coefficients:
            rejected_predictions.append(
                {**dict(row), "bridge_reject_reason": "missing_wet_fit_coefficients"}
            )
            continue
        target = float(row["reference_h2o_mmol"])
        predicted = _prediction(row, coefficients, wet_cfg.terms)
        error = predicted - target
        rel_error = _relative_error_pct(
            error,
            target,
            min_reference_mmol=float(cfg.bridge_relative_error_min_reference_mmol),
        )
        status = _bridge_status(abs_error_mmol=error, rel_error_pct=rel_error, cfg=cfg)
        dry_predictions.append(
            {
                "component": "h2o",
                "analyzer_device_id": device_id,
                "analyzer_prefix": row.get("analyzer_prefix", ""),
                "point_run_id": row.get("point_run_id", ""),
                "temp_set_c": row.get("temp_set_c", ""),
                "reference_h2o_mmol": target,
                "wet_model_pred_h2o_mmol": predicted,
                "bridge_error_mmol": error,
                "bridge_error_pct": rel_error,
                "bridge_status": status,
                "reference_dewpoint_c": row.get("reference_dewpoint_c", ""),
                "reference_pressure_hpa": row.get("reference_pressure_hpa", ""),
                "h2o_ratio_f": row.get("h2o_ratio_f", ""),
                "h2o_ratio_dev": row.get("h2o_ratio_dev", ""),
                "chamber_temp_c": row.get("chamber_temp_c", ""),
                "analyzer_chamber_temp_c_raw": row.get("analyzer_chamber_temp_c_raw", ""),
                "digital_thermometer_temp_c": row.get("digital_thermometer_temp_c", ""),
                "temperature_source_for_fit": row.get("temperature_source_for_fit", ""),
                "sample_alignment_status": row.get("sample_alignment_status", ""),
                "sample_alignment_failure_reasons": row.get("sample_alignment_failure_reasons", ""),
                "reference_source": row.get("reference_source", ""),
                "physical_role": "dry_anchor_bridge_qc",
            }
        )

    for row in rejected_dry_rows:
        rejected_predictions.append({**dict(row), "bridge_reject_reason": row.get("reject_reasons", "")})

    device_summary: List[Dict[str, Any]] = []
    device_ids = sorted(set(policies_by_device) | {_normal_device_id(row.get("analyzer_device_id")) for row in raw_dry_rows})
    for device_id in device_ids:
        if not device_id:
            continue
        wet_policy = policies_by_device.get(device_id, {})
        dry_rows = [row for row in dry_predictions if row.get("analyzer_device_id") == device_id]
        compatible = [row for row in dry_rows if row.get("bridge_status") == "bridge_fit_compatible"]
        qc_only = [row for row in dry_rows if str(row.get("bridge_status") or "").startswith("bridge_qc_only")]
        rel_values = [
            float(row["bridge_error_pct"])
            for row in dry_rows
            if row.get("bridge_error_pct") not in ("", None)
        ]
        abs_values = [abs(float(row["bridge_error_mmol"])) for row in dry_rows]
        device_summary.append(
            {
                "component": "h2o",
                "analyzer_device_id": device_id,
                "wet_fit_status": wet_policy.get("candidate_status", ""),
                "wet_fit_max_abs_error_mmol": wet_policy.get("fit_max_error_mmol", ""),
                "wet_fit_max_abs_relative_error_pct": wet_policy.get(
                    "fit_max_abs_relative_error_pct", ""
                ),
                "wet_fit_complete_wet_point_count": wet_policy.get("complete_wet_point_count", ""),
                "dry_anchor_count": len(dry_rows),
                "dry_anchor_compatible_count": len(compatible),
                "dry_anchor_qc_only_count": len(qc_only),
                "dry_anchor_rejected_count": len(
                    [
                        row
                        for row in rejected_predictions
                        if _normal_device_id(row.get("analyzer_device_id")) == device_id
                    ]
                ),
                "dry_bridge_max_abs_error_mmol": max(abs_values) if abs_values else "",
                "dry_bridge_max_abs_relative_error_pct": max(abs(value) for value in rel_values)
                if rel_values
                else "",
                "recommendation": _device_recommendation(
                    wet_policy=wet_policy,
                    dry_count=len(dry_rows),
                    compatible_count=len(compatible),
                    qc_only_count=len(qc_only),
                    cfg=cfg,
                ),
                "physical_meaning": (
                    "Dry anchors are evaluated against the wet-route H2O main model. "
                    "A mismatch means the dry point remains QC evidence, not an automatic "
                    "main-fit anchor."
                ),
            }
        )

    strategy_rows = _strategy_rows(wet_run_dir=wet_run_dir, dry_anchor_roots=dry_roots, cfg=cfg)
    manifest = {
        "tool_name": "export_v1_5_h2o_dry_anchor_bridge_review",
        "created_at": _now(),
        "no_write": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "wet_run_dir": str(Path(wet_run_dir).resolve()),
        "dry_anchor_run_dirs": list(dry_roots),
        "wet_fit_contract": "SENCO2/SENCO4 main chain fitted from H2O wet-route points only",
        "dry_anchor_bridge_contract": (
            "Gas-route dry anchors use dewpoint/pressure-derived H2O targets and are "
            "evaluated as low-water bridge evidence before being allowed into any main fit."
        ),
        "fit_temperature_source": cfg.fit_temperature_source,
        "fit_objective": cfg.fit_objective,
        "wet_context": wet_context,
    }
    return {
        "manifest": manifest,
        "device_summary": device_summary,
        "dry_anchor_predictions": dry_predictions,
        "dry_anchor_rejected": rejected_predictions,
        "strategy_comparison": strategy_rows,
    }


def _markdown_report(tables: Mapping[str, Any]) -> str:
    manifest = tables["manifest"]
    summary = tables["device_summary"]
    strategy = tables["strategy_comparison"]
    lines = [
        "# V1.5 H2O 干气锚点桥接评审",
        "",
        f"- 生成时间：`{manifest.get('created_at')}`",
        "- 打开串口：`False`",
        "- 写入系数：`False`",
        "- 控制水路/气路：`False`",
        f"- 湿点主拟合合同：`{manifest.get('wet_fit_contract')}`",
        f"- 干点桥接合同：`{manifest.get('dry_anchor_bridge_contract')}`",
        "",
        "## 设备摘要",
        "",
        "| 设备 ID | 湿点最大相对误差 % | 干点最大相对误差 % | 干点最大绝对误差 mmol/mol | 可兼容干点数 | 仅 QC 干点数 | 建议 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in summary:
        lines.append(
            "| {device} | {wet_rel} | {dry_rel} | {dry_abs} | {ok} | {qc} | {rec} |".format(
                device=row.get("analyzer_device_id", ""),
                wet_rel=row.get("wet_fit_max_abs_relative_error_pct", ""),
                dry_rel=row.get("dry_bridge_max_abs_relative_error_pct", ""),
                dry_abs=row.get("dry_bridge_max_abs_error_mmol", ""),
                ok=row.get("dry_anchor_compatible_count", ""),
                qc=row.get("dry_anchor_qc_only_count", ""),
                rec=row.get("recommendation", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 策略对比",
            "",
            "| 策略 | 最差设备 | 最大相对误差 % | 平均最大相对误差 % | 物理意义 |",
            "| --- | --- | ---: | ---: | --- |",
        ]
    )
    for row in strategy:
        lines.append(
            "| {strategy} | {device} | {worst} | {mean} | {meaning} |".format(
                strategy=row.get("strategy_id", ""),
                device=row.get("worst_device_id", ""),
                worst=row.get("worst_max_relative_error_pct", ""),
                mean=row.get("mean_device_max_relative_error_pct", ""),
                meaning=row.get("physical_meaning", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 物理解释",
            "",
            "- CO2 零气点不等于 H2O 零点；H2O 目标值必须来自露点和压力证据，而不能强制设为 0。",
            "- 水路湿点定义 H2O 主响应面，是 SENCO2/SENCO4 主链路拟合的核心证据。",
            "- 干气锚点只有在经过露点、压力、温度等状态归一化后与湿点模型一致，才适合进入低水端拟合评审。",
            "- 与湿点模型不一致的干气点仍然有 QC 价值，但不能静默混入 SENCO2/SENCO4 主拟合。",
        ]
    )
    return "\n".join(lines)


def write_h2o_dry_anchor_bridge_review(
    *,
    wet_run_dir: str | Path,
    dry_anchor_run_dirs: Sequence[str | Path],
    output_dir: str | Path,
    cfg: H2ODryAnchorBridgeConfig = H2ODryAnchorBridgeConfig(),
) -> Dict[str, str]:
    """Write no-write H2O dry-anchor bridge review artifacts."""

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    tables = build_h2o_dry_anchor_bridge_tables(
        wet_run_dir=wet_run_dir,
        dry_anchor_run_dirs=dry_anchor_run_dirs,
        cfg=cfg,
    )
    paths = {
        "manifest": output / "h2o_dry_anchor_bridge_manifest.json",
        "device_summary": output / "h2o_dry_anchor_bridge_device_summary.csv",
        "dry_anchor_predictions": output / "h2o_dry_anchor_bridge_predictions.csv",
        "dry_anchor_rejected": output / "h2o_dry_anchor_bridge_rejected.csv",
        "strategy_comparison": output / "h2o_dry_anchor_bridge_strategy_comparison.csv",
        "markdown": output / "h2o_dry_anchor_bridge_review.md",
    }
    _write_json(paths["manifest"], tables["manifest"])
    _write_csv(paths["device_summary"], tables["device_summary"])
    _write_csv(paths["dry_anchor_predictions"], tables["dry_anchor_predictions"])
    _write_csv(paths["dry_anchor_rejected"], tables["dry_anchor_rejected"])
    _write_csv(paths["strategy_comparison"], tables["strategy_comparison"])
    paths["markdown"].write_text(_markdown_report(tables), encoding="utf-8-sig")
    return {key: str(path) for key, path in paths.items()}
