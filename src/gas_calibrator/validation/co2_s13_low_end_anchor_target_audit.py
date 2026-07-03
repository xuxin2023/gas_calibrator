"""Low-end CO2 S1/S3 anchor and target-state audit.

This module audits already-recorded V1.5 CO2 open-flow evidence. It checks
whether low-end residuals come from target assignment, zero-gas assumptions,
run segmentation, or common-mode physical state rather than silently deleting
points or using S5 to hide a main-model problem.

It never opens COM ports, controls routes, or writes coefficients.
"""

from __future__ import annotations

import csv
import json
import math
import tempfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .co2_fit_algorithm_matrix import _device_id, _safe_float
from .co2_s13_model_structure_review import (
    DEFAULT_STRUCTURE_OBJECTIVES,
    DEFAULT_STRUCTURES,
    build_co2_s13_model_structure_review,
)
from .co2_zero_s5_sensitivity_review import DEFAULT_ZERO_OFFSETS_PPM


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_csv(path: str | Path) -> List[Dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return []
    with target.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    headers: List[str] = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in headers})


def _point_identity(row: Mapping[str, Any]) -> str:
    return str(row.get("point_identity") or row.get("sample_index") or "").strip()


def _temperature_group(row: Mapping[str, Any]) -> str:
    identity = _point_identity(row)
    if identity.startswith("T") and "_" in identity:
        return identity.split("_", 1)[0]
    value = _safe_float(row.get("temp_set_c") or row.get("temperature_c"))
    return f"T{value:g}" if value is not None else "T_unknown"


def _target_ppm(row: Mapping[str, Any]) -> Optional[float]:
    return _safe_float(row.get("target_value") or row.get("certificate_co2_ppm") or row.get("ppm_CO2_Tank"))


def _source_label(row: Mapping[str, Any]) -> str:
    explicit = str(row.get("source_label") or "").strip()
    if explicit:
        return explicit
    path = str(row.get("source_sample_path") or "").replace("\\", "/")
    for marker in (
        "T30_low_end_supplement",
        "co2_t20_600_holdout",
        "T0_to_m20",
        "T30_T40",
        "T10",
        "T20",
    ):
        if marker in path:
            return marker
    return "unknown_source"


def _target_group(row: Mapping[str, Any]) -> str:
    target = _target_ppm(row)
    if target is None:
        identity = _point_identity(row)
        return identity.split("_", 1)[1] if "_" in identity else "unknown_target"
    return f"{target:g}ppm"


def _unique_join(values: Iterable[Any], limit: int = 20) -> str:
    out: List[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in out:
            out.append(text)
    if len(out) > limit:
        return ";".join(out[:limit] + [f"...(+{len(out) - limit})"])
    return ";".join(out)


def _finite_values(rows: Sequence[Mapping[str, Any]], *keys: str) -> List[float]:
    values: List[float] = []
    for row in rows:
        value = None
        for key in keys:
            value = _safe_float(row.get(key))
            if value is not None:
                break
        if value is not None:
            values.append(float(value))
    return values


def _mean(values: Sequence[float]) -> float | str:
    return sum(values) / len(values) if values else ""


def _minimum(values: Sequence[float]) -> float | str:
    return min(values) if values else ""


def _maximum(values: Sequence[float]) -> float | str:
    return max(values) if values else ""


def _std(values: Sequence[float]) -> float | str:
    if len(values) < 2:
        return ""
    avg = sum(values) / len(values)
    return math.sqrt(sum((value - avg) ** 2 for value in values) / (len(values) - 1))


def _counts(values: Iterable[Any]) -> str:
    counter: Dict[str, int] = defaultdict(int)
    for value in values:
        text = str(value or "unknown").strip() or "unknown"
        counter[text] += 1
    return ";".join(f"{key}:{counter[key]}" for key in sorted(counter))


def _run_partition_summary(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[_source_label(row)].append(row)

    out: List[Dict[str, Any]] = []
    for source, items in sorted(groups.items()):
        targets = sorted({_target_group(row) for row in items})
        temps = sorted({_temperature_group(row) for row in items})
        identities = sorted({_point_identity(row) for row in items if _point_identity(row)})
        devices = sorted({_device_id(row.get("analyzer_device_id") or row.get("device_id")) for row in items})
        out.append(
            {
                "source_label": source,
                "row_count": len(items),
                "device_count": len([item for item in devices if item]),
                "temperature_groups": ";".join(temps),
                "target_groups": ";".join(targets),
                "point_identities": ";".join(identities),
                "fit_inclusion_status_counts": _counts(row.get("fit_inclusion_status") for row in items),
                "sample_path_roots": _unique_join(
                    Path(str(row.get("source_sample_path") or "")).parts[-4]
                    if str(row.get("source_sample_path") or "").strip()
                    else ""
                    for row in items
                ),
                "physical_interpretation": (
                    "同一 CO2 拟合包来自该采样批次；跨批次点位要重点检查露点、ratio 稳定性和目标映射是否一致。"
                ),
            }
        )
    return out


def _target_state_audit(rows: Sequence[Mapping[str, Any]], *, low_end_target_ppm: float) -> List[Dict[str, Any]]:
    groups: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        identity = _point_identity(row)
        if identity:
            groups[identity].append(row)

    out: List[Dict[str, Any]] = []
    for identity, items in sorted(groups.items()):
        targets = _finite_values(items, "target_value", "certificate_co2_ppm", "ppm_CO2_Tank")
        nominal = _finite_values(items, "source_nominal_ppm")
        ratio_values = _finite_values(items, "ratio", "co2_ratio_f_mean")
        ratio_std_values = _finite_values(items, "co2_ratio_f_std", "ratio_std")
        dewpoints = _finite_values(items, "dewpoint_mean_c", "dewpoint_c_mean", "dewpoint_c")
        pressures = _finite_values(items, "pressure_hpa", "pressure_gauge_mean_hpa")
        target = targets[0] if targets else None
        low_end = target is not None and 0.0 < float(target) <= float(low_end_target_ppm)
        target_values_disagree = bool(targets) and any(
            abs(value - targets[0]) > 1.0e-6 for value in targets
        )
        nominal_differs_from_certificate = bool(
            targets and nominal and abs(nominal[0] - targets[0]) > 1.0e-6
        )
        if target_values_disagree:
            note = "target_values_disagree_review_certificate_and_point_table"
        elif nominal_differs_from_certificate:
            note = "certificate_value_differs_from_nominal_label_confirm_mapping"
        elif target is not None and abs(float(target)) <= 1.0e-9:
            note = "zero_anchor_assigned_value_review_needed"
        elif low_end:
            note = "low_end_standard_point_review_common_mode_and_route_state"
        else:
            note = "standard_point_mapping_consistent"
        out.append(
            {
                "point_identity": identity,
                "device_count": len({_device_id(row.get("analyzer_device_id") or row.get("device_id")) for row in items}),
                "source_labels": _unique_join(_source_label(row) for row in items),
                "temperature_group": _temperature_group(items[0]),
                "target_values_ppm": _unique_join(f"{value:g}" for value in targets),
                "source_nominal_values_ppm": _unique_join(f"{value:g}" for value in nominal),
                "target_uncertainty_ppm_values": _unique_join(row.get("target_uncertainty_ppm") for row in items),
                "ratio_mean": _mean(ratio_values),
                "ratio_std_of_device_means": _std(ratio_values),
                "ratio_window_std_max": _maximum(ratio_std_values),
                "dewpoint_mean_c": _mean(dewpoints),
                "dewpoint_min_c": _minimum(dewpoints),
                "dewpoint_max_c": _maximum(dewpoints),
                "pressure_mean_hpa": _mean(pressures),
                "pressure_min_hpa": _minimum(pressures),
                "pressure_max_hpa": _maximum(pressures),
                "target_state_note": note,
                "auto_exclude_allowed": False,
            }
        )
    return out


def _zero_anchor_assignment_audit(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for row in _target_state_audit(rows, low_end_target_ppm=300.0):
        target = _safe_float(row.get("target_values_ppm").split(";")[0] if row.get("target_values_ppm") else "")
        if target is None or abs(float(target)) > 1.0e-9:
            continue
        out.append(
            {
                **row,
                "zero_anchor_physical_meaning": (
                    "该点约束 CO2 截距；即使气体名为 0ppm，也应保留估算值和不确定度，不能和 H2O 干气锚点混为一谈。"
                ),
                "recommended_action": "keep_zero_anchor_but_review_assigned_co2_content_and_uncertainty",
            }
        )
    return out


def _selected_map(tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> Dict[str, Mapping[str, Any]]:
    return {
        _device_id(row.get("device_id")): row
        for row in tables.get("selected_structure_candidates", [])
        if _device_id(row.get("device_id"))
    }


def _selected_metric(row: Mapping[str, Any], key: str) -> Optional[float]:
    return _safe_float(row.get(f"best_{key}") or row.get(key))


def _write_temp_fit_points(rows: Sequence[Mapping[str, Any]], directory: Path, name: str) -> Path:
    path = directory / name
    _write_csv(path, rows)
    return path


def _common_mode_candidates(tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    for row in tables.get("selected_low_end_common_mode_patterns", []):
        status = str(row.get("common_mode_status") or "")
        device_count = int(_safe_float(row.get("device_count")) or 0)
        positive = int(_safe_float(row.get("positive_error_count")) or 0)
        negative = int(_safe_float(row.get("negative_error_count")) or 0)
        if status != "common_mode_suspect":
            continue
        direction = ""
        if device_count and positive == device_count:
            direction = "all_positive"
        elif device_count and negative == device_count:
            direction = "all_negative"
        elif positive > negative:
            direction = "mostly_positive"
        else:
            direction = "mostly_negative"
        candidates.append({**dict(row), "bias_direction": direction})
    return candidates


def _point_exclusion_sensitivity(
    *,
    raw_rows: Sequence[Mapping[str, Any]],
    baseline_tables: Mapping[str, Sequence[Mapping[str, Any]]],
    structures: Sequence[str],
    objectives: Sequence[str],
    zero_offsets_ppm: Sequence[float],
    min_relative_target_ppm: float,
    low_end_target_ppm: float,
    low_end_multiplier: float,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    baseline = _selected_map(baseline_tables)
    candidates = _common_mode_candidates(baseline_tables)
    summary_rows: List[Dict[str, Any]] = []
    detail_rows: List[Dict[str, Any]] = []
    if not candidates:
        return summary_rows, detail_rows

    with tempfile.TemporaryDirectory(prefix="co2_s13_low_end_exclusion_") as temp_dir:
        scratch = Path(temp_dir)
        for candidate in candidates:
            identity = str(candidate.get("point_identity") or "")
            if not identity:
                continue
            filtered = [row for row in raw_rows if _point_identity(row) != identity]
            filtered_path = _write_temp_fit_points(filtered, scratch, f"without_{identity.replace('-', 'm').replace('/', '_')}.csv")
            new_tables = build_co2_s13_model_structure_review(
                fit_points_csv=filtered_path,
                structures=structures,
                objectives=objectives,
                zero_offsets_ppm=zero_offsets_ppm,
                min_relative_target_ppm=min_relative_target_ppm,
                low_end_target_ppm=low_end_target_ppm,
                low_end_multiplier=low_end_multiplier,
            )
            new_selected = _selected_map(new_tables)
            improvements: List[float] = []
            low_improvements: List[float] = []
            worsened = 0
            improved = 0
            comparable = 0
            for device_id, base_row in sorted(baseline.items()):
                new_row = new_selected.get(device_id)
                if not new_row:
                    continue
                base_max = _selected_metric(base_row, "max_abs_relative_error_percent")
                new_max = _selected_metric(new_row, "max_abs_relative_error_percent")
                base_low = _selected_metric(base_row, "low_end_max_abs_relative_error_percent")
                new_low = _selected_metric(new_row, "low_end_max_abs_relative_error_percent")
                if base_max is None or new_max is None:
                    continue
                comparable += 1
                delta = float(base_max) - float(new_max)
                improvements.append(delta)
                if delta > 0:
                    improved += 1
                elif delta < 0:
                    worsened += 1
                low_delta = ""
                if base_low is not None and new_low is not None:
                    low_delta = float(base_low) - float(new_low)
                    low_improvements.append(float(low_delta))
                detail_rows.append(
                    {
                        "excluded_point_identity": identity,
                        "bias_direction": candidate.get("bias_direction", ""),
                        "device_id": device_id,
                        "baseline_best_structure_id": base_row.get("best_structure_id", ""),
                        "baseline_best_objective_id": base_row.get("best_objective_id", ""),
                        "baseline_best_zero_offset_ppm": base_row.get("best_zero_offset_ppm", ""),
                        "baseline_max_abs_relative_error_percent": base_max,
                        "without_point_best_structure_id": new_row.get("best_structure_id", ""),
                        "without_point_best_objective_id": new_row.get("best_objective_id", ""),
                        "without_point_best_zero_offset_ppm": new_row.get("best_zero_offset_ppm", ""),
                        "without_point_max_abs_relative_error_percent": new_max,
                        "max_relative_error_improvement_percent_points": delta,
                        "low_end_relative_error_improvement_percent_points": low_delta,
                        "physical_warning": (
                            "该试验只证明点位影响模型，不等于允许删除；若 ratio/露点证据良好，应优先审查目标值、气瓶/阀路状态和低端模型。"
                        ),
                    }
                )
            summary_rows.append(
                {
                    "excluded_point_identity": identity,
                    "bias_direction": candidate.get("bias_direction", ""),
                    "target_ppm": candidate.get("target_ppm", ""),
                    "temperature_c": candidate.get("temperature_c", ""),
                    "baseline_common_mean_error_ppm": candidate.get("mean_error_ppm", ""),
                    "baseline_common_max_abs_relative_error_percent": candidate.get("max_abs_relative_error_percent", ""),
                    "comparable_device_count": comparable,
                    "improved_device_count": improved,
                    "worsened_device_count": worsened,
                    "mean_max_relative_error_improvement_percent_points": _mean(improvements),
                    "best_device_improvement_percent_points": _maximum(improvements),
                    "worst_device_improvement_percent_points": _minimum(improvements),
                    "mean_low_end_improvement_percent_points": _mean(low_improvements),
                    "exclusion_interpretation": _exclusion_interpretation(improved, worsened, comparable),
                    "auto_exclude_allowed": False,
                }
            )
    summary_rows.sort(
        key=lambda row: float(row.get("mean_max_relative_error_improvement_percent_points") or -1.0e9),
        reverse=True,
    )
    detail_rows.sort(
        key=lambda row: (
            str(row.get("excluded_point_identity") or ""),
            str(row.get("device_id") or ""),
        )
    )
    return summary_rows, detail_rows


def _exclusion_interpretation(improved: int, worsened: int, comparable: int) -> str:
    if comparable == 0:
        return "no_comparable_devices"
    if improved == comparable:
        return "deleting_this_common_mode_point_improves_all_devices_but_needs_physical_cause_before_exclusion"
    if improved > worsened:
        return "deleting_this_point_often_improves_fit_but_not_uniformly"
    if worsened > improved:
        return "deleting_this_point_worsens_more_devices_not_a_good_exclusion_candidate"
    return "mixed_effect_not_a_clean_exclusion_candidate"


def _zero_offset_selection_summary(tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> List[Dict[str, Any]]:
    groups: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in tables.get("structure_summary", []):
        if row.get("structure_id") != "core_plus_full_temp":
            continue
        offset = str(row.get("zero_offset_ppm") or "0")
        groups[offset].append(row)
    out: List[Dict[str, Any]] = []
    for offset, rows in sorted(groups.items(), key=lambda item: float(_safe_float(item[0]) or 0.0)):
        max_values = [
            float(value)
            for value in (_safe_float(row.get("max_abs_relative_error_percent")) for row in rows)
            if value is not None
        ]
        low_values = [
            float(value)
            for value in (_safe_float(row.get("low_end_max_abs_relative_error_percent")) for row in rows)
            if value is not None
        ]
        out.append(
            {
                "zero_offset_ppm": offset,
                "candidate_row_count": len(rows),
                "mean_max_abs_relative_error_percent": _mean(max_values),
                "best_max_abs_relative_error_percent": _minimum(max_values),
                "worst_max_abs_relative_error_percent": _maximum(max_values),
                "mean_low_end_max_abs_relative_error_percent": _mean(low_values),
                "physical_interpretation": (
                    "用于估计零气 CO2 指定值对 S1/S3 截距的影响；不是把零气任意当成某个值，而是给未认证零气留不确定度。"
                ),
            }
        )
    return out


def build_co2_s13_low_end_anchor_target_audit(
    *,
    fit_points_csv: str | Path,
    structures: Sequence[str] = DEFAULT_STRUCTURES,
    objectives: Sequence[str] = DEFAULT_STRUCTURE_OBJECTIVES,
    zero_offsets_ppm: Sequence[float] = DEFAULT_ZERO_OFFSETS_PPM,
    min_relative_target_ppm: float = 50.0,
    low_end_target_ppm: float = 300.0,
    low_end_multiplier: float = 3.0,
) -> Dict[str, List[Dict[str, Any]]]:
    raw_rows = _read_csv(fit_points_csv)
    baseline_tables = build_co2_s13_model_structure_review(
        fit_points_csv=fit_points_csv,
        structures=structures,
        objectives=objectives,
        zero_offsets_ppm=zero_offsets_ppm,
        min_relative_target_ppm=min_relative_target_ppm,
        low_end_target_ppm=low_end_target_ppm,
        low_end_multiplier=low_end_multiplier,
    )
    exclusion_summary, exclusion_detail = _point_exclusion_sensitivity(
        raw_rows=raw_rows,
        baseline_tables=baseline_tables,
        structures=structures,
        objectives=objectives,
        zero_offsets_ppm=zero_offsets_ppm,
        min_relative_target_ppm=min_relative_target_ppm,
        low_end_target_ppm=low_end_target_ppm,
        low_end_multiplier=low_end_multiplier,
    )
    run_summary = [
        {
            "created_at": _now(),
            "fit_points_csv": str(Path(fit_points_csv).resolve()),
            "row_count": len(raw_rows),
            "source_partition_count": len({_source_label(row) for row in raw_rows}),
            "structures": ";".join(structures),
            "objectives": ";".join(objectives),
            "zero_offsets_ppm": ";".join(f"{float(value):g}" for value in zero_offsets_ppm),
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "uses_pressure_terms": False,
            "uses_s5_output_trim": False,
            "not_real_acceptance_evidence": True,
        }
    ]
    return {
        "run_summary": run_summary,
        "run_partition_summary": _run_partition_summary(raw_rows),
        "target_state_audit": _target_state_audit(raw_rows, low_end_target_ppm=low_end_target_ppm),
        "zero_anchor_assignment_audit": _zero_anchor_assignment_audit(raw_rows),
        "low_end_common_mode_audit": list(baseline_tables.get("selected_low_end_common_mode_patterns", [])),
        "zero_offset_selection_summary": _zero_offset_selection_summary(baseline_tables),
        "point_exclusion_sensitivity": exclusion_summary,
        "point_exclusion_sensitivity_by_device": exclusion_detail,
    }


def write_co2_s13_low_end_anchor_target_audit(
    *,
    fit_points_csv: str | Path,
    output_dir: str | Path,
    structures: Sequence[str] = DEFAULT_STRUCTURES,
    objectives: Sequence[str] = DEFAULT_STRUCTURE_OBJECTIVES,
    zero_offsets_ppm: Sequence[float] = DEFAULT_ZERO_OFFSETS_PPM,
    min_relative_target_ppm: float = 50.0,
    low_end_target_ppm: float = 300.0,
    low_end_multiplier: float = 3.0,
) -> Dict[str, Path]:
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    tables = build_co2_s13_low_end_anchor_target_audit(
        fit_points_csv=fit_points_csv,
        structures=structures,
        objectives=objectives,
        zero_offsets_ppm=zero_offsets_ppm,
        min_relative_target_ppm=min_relative_target_ppm,
        low_end_target_ppm=low_end_target_ppm,
        low_end_multiplier=low_end_multiplier,
    )
    outputs = {
        "run_summary": output / "co2_s13_low_end_anchor_target_run_summary.csv",
        "run_partition_summary": output / "co2_s13_run_partition_summary.csv",
        "target_state_audit": output / "co2_s13_target_state_audit.csv",
        "zero_anchor_assignment_audit": output / "co2_s13_zero_anchor_assignment_audit.csv",
        "low_end_common_mode_audit": output / "co2_s13_low_end_common_mode_audit.csv",
        "zero_offset_selection_summary": output / "co2_s13_zero_offset_selection_summary.csv",
        "point_exclusion_sensitivity": output / "co2_s13_point_exclusion_sensitivity.csv",
        "point_exclusion_sensitivity_by_device": output / "co2_s13_point_exclusion_sensitivity_by_device.csv",
        "metadata": output / "co2_s13_low_end_anchor_target_meta.json",
        "markdown": output / "co2_s13_low_end_anchor_target_audit_zh.md",
    }
    for key, path in outputs.items():
        if key in {"metadata", "markdown"}:
            continue
        _write_csv(path, tables.get(key, []))
    outputs["metadata"].write_text(
        json.dumps(
            {
                "created_at": _now(),
                "fit_points_csv": str(Path(fit_points_csv).resolve()),
                "boundary": {
                    "opens_com_ports": False,
                    "controls_water_or_gas_routes": False,
                    "writes_coefficients": False,
                    "uses_pressure_terms": False,
                    "uses_s5_output_trim": False,
                    "not_real_acceptance_evidence": True,
                },
                "outputs": {key: str(path) for key, path in outputs.items() if key != "metadata"},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    _write_markdown(outputs["markdown"], tables)
    return outputs


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    partitions = list(tables.get("run_partition_summary") or [])
    targets = list(tables.get("target_state_audit") or [])
    zero = list(tables.get("zero_anchor_assignment_audit") or [])
    common = list(tables.get("low_end_common_mode_audit") or [])
    exclusion = list(tables.get("point_exclusion_sensitivity") or [])
    zero_offsets = list(tables.get("zero_offset_selection_summary") or [])
    lines = [
        "# V1.5 CO2 S1/S3 低端锚点与目标状态审计",
        "",
        "边界：本报告只使用既有 CSV 证据；不打开串口、不控制气路/水路、不写 SENCO；S5 输出层线性修正不参与本轮 S1/S3 主链路判断。",
        "",
        "## 1. 气路采样批次",
    ]
    for row in partitions:
        lines.append(
            "- {source}: {rows} 行，温度组 {temps}，气点 {targets}".format(
                source=row.get("source_label", ""),
                rows=row.get("row_count", ""),
                temps=row.get("temperature_groups", ""),
                targets=row.get("target_groups", ""),
            )
        )
    lines.extend(["", "## 2. 低端目标值与零气锚点"])
    for row in targets:
        target = _safe_float(str(row.get("target_values_ppm") or "").split(";")[0])
        if target is not None and (target <= 300.0 or abs(target) <= 1.0e-9):
            lines.append(
                "- {pid}: 目标 {target} ppm，来源 {source}，露点均值 {dew} °C，结论 {note}".format(
                    pid=row.get("point_identity", ""),
                    target=row.get("target_values_ppm", ""),
                    source=row.get("source_labels", ""),
                    dew=row.get("dewpoint_mean_c", ""),
                    note=row.get("target_state_note", ""),
                )
            )
    if zero:
        lines.append("")
        lines.append("零气锚点保留为 CO2 截距约束，但必须带不确定度；它不是 H2O 干气锚点。")
    lines.extend(["", "## 3. 零气指定值敏感性"])
    for row in zero_offsets:
        lines.append(
            "- 零气 {zero} ppm：平均最大相对误差 {mean}%，最优 {best}%，最差 {worst}%".format(
                zero=row.get("zero_offset_ppm", ""),
                mean=row.get("mean_max_abs_relative_error_percent", ""),
                best=row.get("best_max_abs_relative_error_percent", ""),
                worst=row.get("worst_max_abs_relative_error_percent", ""),
            )
        )
    lines.extend(["", "## 4. 共同偏差点与删点敏感性"])
    for row in common[:10]:
        lines.append(
            "- {pid}: 均值误差 {err} ppm，正偏 {pos} 台，负偏 {neg} 台，状态 {status}".format(
                pid=row.get("point_identity", ""),
                err=row.get("mean_error_ppm", ""),
                pos=row.get("positive_error_count", ""),
                neg=row.get("negative_error_count", ""),
                status=row.get("common_mode_status", ""),
            )
        )
    lines.append("")
    for row in exclusion:
        lines.append(
            "- 试删 {pid} ({direction})：{improved}/{count} 台改善，{worsened} 台变差，平均改善 {mean} 个百分点。结论：{note}".format(
                pid=row.get("excluded_point_identity", ""),
                direction=row.get("bias_direction", ""),
                improved=row.get("improved_device_count", ""),
                count=row.get("comparable_device_count", ""),
                worsened=row.get("worsened_device_count", ""),
                mean=row.get("mean_max_relative_error_improvement_percent_points", ""),
                note=row.get("exclusion_interpretation", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 5. 审计结论",
            "",
            "如果某个低端点删除后显著改善，也只能说明它是高影响点，不能自动证明它应被剔除。若该点 ratio A 级、露点深干、阀路和证书目标可追溯，优先解释为低端目标状态、零气指定值或 S1/S3 模型边界问题。",
            "",
            "S5 可以在 S1/S3 主链路评审完成后作为输出层修正，但不应先用来掩盖低端共同模式残差。",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
