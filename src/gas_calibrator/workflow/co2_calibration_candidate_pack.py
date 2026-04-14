"""V1 CO2 grouped calibration candidate pack helpers.

This layer does not add any new live gate. It only consumes the existing
point-level hardened evidence and reorganizes it into calibration-oriented
candidate points and deterministic group summaries.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
import csv
import json
import math
from statistics import mean, pstdev
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


_FIELD_ALIASES: Dict[str, Tuple[str, ...]] = {
    "point_title": ("point_title", "点位标题"),
    "point_no": ("point_no", "点位编号"),
    "point_tag": ("point_tag", "点位标签"),
    "point_row": ("point_row", "校准点行号", "标准点行号"),
    "route": ("route", "采样路线"),
    "pressure_target_label": ("pressure_target_label", "压力目标标签"),
    "pressure_mode": ("pressure_mode", "压力执行模式"),
    "pressure_target_hpa": ("pressure_target_hpa", "目标压力hPa"),
    "temp_chamber_c": ("temp_chamber_c", "温箱目标温度C"),
    "temp_set_c": ("temp_set_c", "温箱设定温度C"),
    "co2_ppm_target": ("co2_ppm_target", "目标二氧化碳浓度ppm"),
    "target_value": ("target_value", "目标值"),
    "measured_value": ("measured_value", "测量值"),
    "measured_value_source": ("measured_value_source", "测量值来源"),
    "co2_point_suitability_status": ("co2_point_suitability_status", "气路点适用性"),
    "co2_calibration_candidate_recommended": ("co2_calibration_candidate_recommended", "气路校准候选推荐"),
    "co2_calibration_candidate_hard_blocked": ("co2_calibration_candidate_hard_blocked", "气路校准候选硬阻断"),
    "co2_calibration_weight_recommended": ("co2_calibration_weight_recommended", "气路推荐校准权重"),
    "co2_evidence_score": ("co2_evidence_score", "气路证据分"),
    "co2_point_evidence_budget_reason": ("co2_point_evidence_budget_reason", "气路证据预算原因"),
    "co2_point_evidence_budget_summary": ("co2_point_evidence_budget_summary", "气路证据预算摘要"),
    "co2_point_suitability_reason_chain": ("co2_point_suitability_reason_chain", "气路点适用性原因链"),
    "co2_decision_waterfall_status": ("co2_decision_waterfall_status", "气路决策瀑布结果"),
    "co2_decision_selected_stage_path": ("co2_decision_selected_stage_path", "气路决策阶段路径"),
    "co2_decision_stage_summary": ("co2_decision_stage_summary", "气路决策阶段摘要"),
    "co2_source_selected": ("co2_source_selected", "气路选中来源"),
    "co2_source_switch_reason": ("co2_source_switch_reason", "气路来源切换原因"),
    "co2_source_trust_reason": ("co2_source_trust_reason", "气路来源可信度说明"),
    "co2_temporal_contract_status": ("co2_temporal_contract_status", "气路时间契约结果"),
    "co2_temporal_contract_reason": ("co2_temporal_contract_reason", "气路时间契约原因"),
    "co2_steady_window_status": ("co2_steady_window_status", "气路稳态窗结果"),
    "co2_steady_window_reason": ("co2_steady_window_reason", "气路稳态窗原因"),
}


def _as_text(value: Any) -> str:
    return str(value or "").strip()


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = _as_text(value).lower()
    return text in {"1", "true", "yes", "y", "on"}


def _as_float(value: Any) -> Optional[float]:
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _first_available(row: Mapping[str, Any], field: str) -> Any:
    for alias in _FIELD_ALIASES.get(field, (field,)):
        if alias in row and row.get(alias) not in (None, ""):
            return row.get(alias)
    return None


def _format_optional_float(value: Optional[float], *, digits: int = 3) -> str:
    if value is None:
        return "unknown"
    return f"{value:.{digits}f}"


def _default_candidate_status(
    suitability_status: str,
    recommended: bool,
    hard_blocked: bool,
) -> str:
    if hard_blocked or suitability_status == "unfit":
        return "unfit"
    if suitability_status == "fit":
        return "fit"
    if recommended:
        return "advisory"
    return "unfit"


def _default_weight_reason(row: Mapping[str, Any], candidate_status: str) -> str:
    if candidate_status == "unfit":
        return "hard_blocked_or_untrusted"
    if _as_text(_first_available(row, "measured_value_source")) == "co2_steady_state_window" and not _as_text(
        _first_available(row, "co2_source_switch_reason")
    ):
        return "trusted_steady_state"
    if _as_text(_first_available(row, "co2_source_switch_reason")):
        return "fallback_but_usable"
    if _as_text(_first_available(row, "co2_temporal_contract_status")).lower() in {"warn", "degraded", "fallback_row_count"}:
        return "temporal_limited"
    return "advisory_weight"


def build_co2_calibration_candidate_point(row: Mapping[str, Any]) -> Dict[str, Any]:
    suitability_status = _as_text(_first_available(row, "co2_point_suitability_status")) or "advisory"
    recommended = _as_bool(_first_available(row, "co2_calibration_candidate_recommended"))
    hard_blocked = _as_bool(_first_available(row, "co2_calibration_candidate_hard_blocked"))
    weight = _as_float(_first_available(row, "co2_calibration_weight_recommended"))
    if weight is None:
        weight = 0.0 if hard_blocked else 0.5

    candidate_status = _default_candidate_status(suitability_status, recommended, hard_blocked)
    weight_reason = _as_text(_first_available(row, "co2_calibration_weight_reason")) or _default_weight_reason(row, candidate_status)
    reason_chain = (
        _as_text(_first_available(row, "co2_calibration_reason_chain"))
        or _as_text(_first_available(row, "co2_point_suitability_reason_chain"))
        or _as_text(_first_available(row, "co2_decision_stage_summary"))
    )

    target_value = _as_float(_first_available(row, "co2_ppm_target"))
    if target_value is None:
        target_value = _as_float(_first_available(row, "target_value"))
    temp_value = _as_float(_first_available(row, "temp_chamber_c"))
    if temp_value is None:
        temp_value = _as_float(_first_available(row, "temp_set_c"))
    pressure_label = _as_text(_first_available(row, "pressure_target_label"))
    if not pressure_label:
        pressure_value = _as_float(_first_available(row, "pressure_target_hpa"))
        pressure_label = _format_optional_float(pressure_value, digits=1)
    route = _as_text(_first_available(row, "route")) or "unknown"

    group_key = "|".join(
        [
            "co2",
            f"route={route or 'unknown'}",
            f"target={_format_optional_float(target_value)}",
            f"temp={_format_optional_float(temp_value)}",
            f"pressure={pressure_label or 'unknown'}",
        ]
    )

    return {
        "point_title": _as_text(_first_available(row, "point_title")),
        "point_no": _as_text(_first_available(row, "point_no")),
        "point_tag": _as_text(_first_available(row, "point_tag")),
        "point_row": _as_text(_first_available(row, "point_row")),
        "route": route,
        "pressure_target_label": pressure_label,
        "co2_ppm_target": target_value,
        "temp_chamber_c": temp_value,
        "measured_value": _as_float(_first_available(row, "measured_value")),
        "measured_value_source": _as_text(_first_available(row, "measured_value_source")),
        "co2_point_suitability_status": suitability_status,
        "co2_evidence_score": _as_float(_first_available(row, "co2_evidence_score")) or 0.0,
        "co2_decision_waterfall_status": _as_text(_first_available(row, "co2_decision_waterfall_status")),
        "co2_decision_selected_stage_path": _as_text(_first_available(row, "co2_decision_selected_stage_path")),
        "co2_source_selected": _as_text(_first_available(row, "co2_source_selected")),
        "co2_source_switch_reason": _as_text(_first_available(row, "co2_source_switch_reason")),
        "co2_temporal_contract_status": _as_text(_first_available(row, "co2_temporal_contract_status")),
        "co2_temporal_contract_reason": _as_text(_first_available(row, "co2_temporal_contract_reason")),
        "co2_calibration_candidate_status": candidate_status,
        "co2_calibration_candidate_recommended": recommended,
        "co2_calibration_candidate_hard_blocked": hard_blocked,
        "co2_calibration_weight_recommended": float(weight),
        "co2_calibration_weight_reason": weight_reason,
        "co2_calibration_reason_chain": reason_chain,
        "co2_calibration_group_key": group_key,
        "co2_point_evidence_budget_summary": _as_text(_first_available(row, "co2_point_evidence_budget_summary")),
    }


def build_co2_calibration_candidate_points(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return [build_co2_calibration_candidate_point(row) for row in rows]


def _group_reason_summary(rows: Sequence[Mapping[str, Any]]) -> str:
    counter: Counter[str] = Counter()
    for row in rows:
        for token in _as_text(row.get("co2_calibration_weight_reason")).split(";"):
            token = token.strip()
            if token:
                counter[token] += 1
    if not counter:
        return ""
    top = [f"{key}:{count}" for key, count in counter.most_common(3)]
    return ";".join(top)


def build_co2_calibration_candidate_groups(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        payload = dict(row)
        grouped.setdefault(_as_text(payload.get("co2_calibration_group_key")), []).append(payload)

    output: List[Dict[str, Any]] = []
    for group_key, items in sorted(grouped.items(), key=lambda item: item[0]):
        measured_values = [float(v) for v in (row.get("measured_value") for row in items) if _as_float(v) is not None]
        weighted_pairs = [
            (float(row.get("co2_calibration_weight_recommended") or 0.0), float(row.get("measured_value")))
            for row in items
            if _as_float(row.get("measured_value")) is not None
            and float(row.get("co2_calibration_weight_recommended") or 0.0) > 0.0
        ]
        weight_sum = round(sum(weight for weight, _ in weighted_pairs), 4)
        weighted_mean = (
            round(sum(weight * value for weight, value in weighted_pairs) / weight_sum, 6)
            if weight_sum > 0.0
            else None
        )
        unweighted_mean = round(mean(measured_values), 6) if measured_values else None
        within_std = round(pstdev(measured_values), 6) if len(measured_values) >= 2 else None
        within_range = round(max(measured_values) - min(measured_values), 6) if len(measured_values) >= 2 else None
        fit_count = sum(1 for row in items if _as_text(row.get("co2_calibration_candidate_status")) == "fit")
        advisory_count = sum(1 for row in items if _as_text(row.get("co2_calibration_candidate_status")) == "advisory")
        unfit_count = sum(1 for row in items if _as_text(row.get("co2_calibration_candidate_status")) == "unfit")
        recommended_count = sum(1 for row in items if _as_bool(row.get("co2_calibration_candidate_recommended")))
        hard_blocked_count = sum(1 for row in items if _as_bool(row.get("co2_calibration_candidate_hard_blocked")))
        mean_weight = round(weight_sum / max(1, len(items)), 4)
        if weight_sum <= 0.0 or recommended_count <= 0:
            group_recommended_for_fit = False
        else:
            group_recommended_for_fit = True

        group_evidence_summary = (
            f"fit={fit_count};advisory={advisory_count};unfit={unfit_count};"
            f"recommended={recommended_count};hard_blocked={hard_blocked_count};"
            f"mean_weight={mean_weight:.3f};top_reasons={_group_reason_summary(items)}"
        )

        exemplar = items[0]
        output.append(
            {
                "calibration_group_key": group_key,
                "route": exemplar.get("route"),
                "pressure_target_label": exemplar.get("pressure_target_label"),
                "co2_ppm_target": exemplar.get("co2_ppm_target"),
                "temp_chamber_c": exemplar.get("temp_chamber_c"),
                "point_count_total": len(items),
                "point_count_fit": fit_count,
                "point_count_advisory": advisory_count,
                "point_count_unfit": unfit_count,
                "weight_sum": weight_sum,
                "weighted_mean_measured_value": weighted_mean,
                "unweighted_mean_measured_value": unweighted_mean,
                "within_group_std": within_std,
                "within_group_range": within_range,
                "group_evidence_summary": group_evidence_summary,
                "group_recommended_for_fit": group_recommended_for_fit,
            }
        )
    return output


def build_co2_calibration_candidate_pack(rows: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    points = build_co2_calibration_candidate_points(rows)
    groups = build_co2_calibration_candidate_groups(points)
    status_counter: Counter[str] = Counter(_as_text(row.get("co2_calibration_candidate_status")) for row in points)
    recommended_count = sum(1 for row in points if _as_bool(row.get("co2_calibration_candidate_recommended")))
    hard_blocked_count = sum(1 for row in points if _as_bool(row.get("co2_calibration_candidate_hard_blocked")))
    weights = [float(row.get("co2_calibration_weight_recommended") or 0.0) for row in points]
    summary = {
        "point_count_total": len(points),
        "point_count_fit": status_counter.get("fit", 0),
        "point_count_advisory": status_counter.get("advisory", 0),
        "point_count_unfit": status_counter.get("unfit", 0),
        "point_count_recommended": recommended_count,
        "point_count_hard_blocked": hard_blocked_count,
        "weight_min": round(min(weights), 4) if weights else 0.0,
        "weight_max": round(max(weights), 4) if weights else 0.0,
        "weight_mean": round(mean(weights), 4) if weights else 0.0,
        "group_count_total": len(groups),
        "group_count_recommended": sum(1 for row in groups if _as_bool(row.get("group_recommended_for_fit"))),
        "evidence_source": "replay_or_exported_v1_points",
        "not_real_acceptance_evidence": True,
    }
    return {"summary": summary, "points": points, "groups": groups}


def render_co2_calibration_candidate_report(pack: Mapping[str, Any]) -> str:
    summary = dict(pack.get("summary") or {})
    groups = list(pack.get("groups") or [])
    lines = [
        "# V1 CO2 grouped calibration candidate pack",
        "",
        "> replay evidence only",
        "> not real acceptance evidence",
        "",
        "## 点位摘要",
        f"- 总点数: {summary.get('point_count_total', 0)}",
        f"- fit / advisory / unfit: {summary.get('point_count_fit', 0)} / {summary.get('point_count_advisory', 0)} / {summary.get('point_count_unfit', 0)}",
        f"- recommended / hard-blocked: {summary.get('point_count_recommended', 0)} / {summary.get('point_count_hard_blocked', 0)}",
        f"- 权重范围: {summary.get('weight_min', 0.0)} ~ {summary.get('weight_max', 0.0)}",
        f"- 平均权重: {summary.get('weight_mean', 0.0)}",
        "",
        "## 分组建议",
    ]
    if not groups:
        lines.extend(["- 无可分组点位", ""])
    else:
        for group in groups:
            lines.append(
                "- "
                + f"{group.get('calibration_group_key')}: "
                + f"点数={group.get('point_count_total')} "
                + f"fit/advisory/unfit={group.get('point_count_fit')}/{group.get('point_count_advisory')}/{group.get('point_count_unfit')} "
                + f"weight_sum={group.get('weight_sum')} "
                + f"recommended={group.get('group_recommended_for_fit')} "
                + f"summary={group.get('group_evidence_summary')}"
            )
        lines.append("")
    lines.extend(
        [
            "## 说明",
            "- 这份 pack 只消费现有 hardened V1 CO2 evidence，不新增 live gate。",
            "- 目标是让后续离线拟合或人工审查更容易区分推荐点、降权点和排除点。",
        ]
    )
    return "\n".join(lines) + "\n"


def read_csv_rows(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv_rows(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    header: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in header:
                header.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))


def write_pack_artifacts(output_dir: Path, pack: Mapping[str, Any]) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    points_path = output_dir / "calibration_candidate_points.csv"
    groups_path = output_dir / "calibration_candidate_groups.csv"
    json_path = output_dir / "calibration_candidate_pack.json"
    report_path = output_dir / "report.md"

    write_csv_rows(points_path, pack.get("points") or [])
    write_csv_rows(groups_path, pack.get("groups") or [])
    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(pack, handle, ensure_ascii=False, indent=2)
    report_path.write_text(render_co2_calibration_candidate_report(pack), encoding="utf-8")
    return {
        "points_csv": points_path,
        "groups_csv": groups_path,
        "pack_json": json_path,
        "report_md": report_path,
    }
