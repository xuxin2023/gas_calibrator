"""Build an offline CO2 minimal resampling run list.

The input is the point summary from the CO2 state-bridge closure review. The
output is a no-write, no-COM run-list draft: it names only the points that need
minimal repeat sampling before another S1/S3/S5 write review can be trusted.
"""

from __future__ import annotations

import csv
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


DEFAULT_MAX_POINTS = 5
DEFAULT_ACCEPTANCE_PERCENT = 1.0
DEFAULT_EXCLUDED_TEMPERATURES_C = (-20.0,)
DEFAULT_PURGE_S = 360.0
DEFAULT_SAMPLE_COUNT = 10
DEFAULT_ANALYZER_ACQUISITION = "active_stream_1hz"
DEFAULT_LEGACY_PRESSURE_TARGETS_EXCLUDED_HPA = "1100,1000,900,800,700,600,550"


CANONICAL_CO2_QUEUE_HEADERS = [
    "point_id",
    "component",
    "temp_c",
    "source_nominal_ppm",
    "co2_group",
    "sample_role",
    "fit_eligible",
    "verification_eligible",
    "zero_gas_required",
    "standard_role",
    "certificate_required",
    "pressure_mode",
    "target_pressure_hpa",
    "pressure_reference_required",
    "pressure_channel_precheck_required",
    "legacy_pressure_targets_excluded_hpa",
    "purge_s",
    "sample_count",
    "analyzer_acquisition",
    "runner",
    "runner_args",
    "physical_meaning",
    "minimal_resampling_sequence",
    "minimal_resampling_reason",
    "not_real_acceptance_evidence",
    "no_write",
]


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        number = float(value)
    except Exception:
        return None
    return number if math.isfinite(number) else None


def _read_csv(path: str | Path) -> List[Dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return []
    with target.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _read_optional_csv(path: str | Path | None) -> List[Dict[str, Any]]:
    if path in (None, ""):
        return []
    return _read_csv(path)


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


def _write_csv_with_headers(path: Path, rows: Sequence[Mapping[str, Any]], headers: Sequence[str]) -> None:
    output_headers = list(headers)
    for row in rows:
        for key in row:
            if key not in output_headers:
                output_headers.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=output_headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in output_headers})


def _temperature_is_excluded(value: Any, excluded_temperatures_c: Iterable[float]) -> bool:
    temp = _safe_float(value)
    if temp is None:
        return False
    return any(abs(temp - float(excluded)) < 1e-9 for excluded in excluded_temperatures_c)


def _priority(row: Mapping[str, Any]) -> tuple[int, float, float, int]:
    return (
        int(_safe_float(row.get("bridged_over_acceptance_count")) or 0),
        float(_safe_float(row.get("bridged_max_abs_relative_error_percent")) or 0.0),
        float(_safe_float(row.get("base_max_abs_relative_error_percent")) or 0.0),
        int(_safe_float(row.get("base_over_acceptance_count")) or 0),
    )


def _needs_resampling(row: Mapping[str, Any]) -> bool:
    action = str(row.get("recommended_action") or "").strip()
    if action in {"minimal_resample_this_point", "insufficient_bridge_support_resample"}:
        return True
    bridge_closed = str(row.get("bridge_closes_point_to_acceptance") or "").strip().lower()
    if bridge_closed in {"true", "1", "yes"}:
        return False
    over_after = int(_safe_float(row.get("bridged_over_acceptance_count")) or 0)
    return over_after > 0


def _point_key(temp_c: Any, ppm: Any) -> tuple[Optional[float], Optional[float]]:
    temp = _safe_float(temp_c)
    gas = _safe_float(ppm)
    if temp is None or gas is None:
        return (None, None)
    return (round(float(temp), 6), round(float(gas), 6))


def _format_compact_number(value: Any) -> str:
    number = _safe_float(value)
    if number is None:
        return str(value or "")
    if abs(number - round(number)) < 1e-9:
        return str(int(round(number)))
    return f"{number:g}"


def _fallback_point_id(temp_c: Any, ppm: Any) -> str:
    return f"co2_T{_format_compact_number(temp_c)}_{_format_compact_number(ppm)}ppm_ambient"


def _co2_standard_role(ppm: Any) -> str:
    gas = _safe_float(ppm)
    if gas is not None and abs(float(gas)) <= 1e-9:
        return "zero_air"
    return "co2_standard_gas"


def _co2_runner_args(row: Mapping[str, Any]) -> str:
    return (
        f"--temp {_format_compact_number(row.get('temp_c'))} "
        f"--co2-source-ppm {_format_compact_number(row.get('source_nominal_ppm'))} "
        f"--co2-group {row.get('co2_group') or 'A'} "
        f"--purge-s {_format_compact_number(row.get('purge_s') or DEFAULT_PURGE_S)} "
        f"--sample-count {int(_safe_float(row.get('sample_count')) or DEFAULT_SAMPLE_COUNT)} "
        f"--analyzer-acquisition {row.get('analyzer_acquisition') or DEFAULT_ANALYZER_ACQUISITION}"
    )


def _template_by_point(rows: Sequence[Mapping[str, Any]]) -> Dict[tuple[Optional[float], Optional[float]], Dict[str, Any]]:
    indexed: Dict[tuple[Optional[float], Optional[float]], Dict[str, Any]] = {}
    for row in rows:
        if str(row.get("component") or "").strip().lower() != "co2":
            continue
        key = _point_key(row.get("temp_c"), row.get("source_nominal_ppm"))
        if key == (None, None):
            continue
        indexed[key] = dict(row)
    return indexed


def _build_canonical_queue(
    runlist: Sequence[Mapping[str, Any]],
    *,
    template_queue_csv: str | Path | None = None,
) -> List[Dict[str, Any]]:
    template_rows = _template_by_point(_read_optional_csv(template_queue_csv))
    canonical_rows: List[Dict[str, Any]] = []
    for row in runlist:
        temp_c = row.get("temperature_c")
        ppm = row.get("gas_ppm")
        key = _point_key(temp_c, ppm)
        template = dict(template_rows.get(key) or {})
        queue_row: Dict[str, Any] = {
            **template,
            "point_id": template.get("point_id") or _fallback_point_id(temp_c, ppm),
            "component": "co2",
            "temp_c": _safe_float(temp_c) if _safe_float(temp_c) is not None else temp_c,
            "source_nominal_ppm": _safe_float(ppm) if _safe_float(ppm) is not None else ppm,
            "co2_group": template.get("co2_group") or "A",
            "sample_role": "fit",
            "fit_eligible": True,
            "verification_eligible": False,
            "zero_gas_required": _co2_standard_role(ppm) == "zero_air",
            "standard_role": _co2_standard_role(ppm),
            "certificate_required": True,
            "pressure_mode": "ambient_open",
            "target_pressure_hpa": "",
            "pressure_reference_required": True,
            "pressure_channel_precheck_required": True,
            "legacy_pressure_targets_excluded_hpa": template.get("legacy_pressure_targets_excluded_hpa")
            or DEFAULT_LEGACY_PRESSURE_TARGETS_EXCLUDED_HPA,
            "purge_s": template.get("purge_s") or DEFAULT_PURGE_S,
            "sample_count": template.get("sample_count") or DEFAULT_SAMPLE_COUNT,
            "analyzer_acquisition": template.get("analyzer_acquisition") or DEFAULT_ANALYZER_ACQUISITION,
            "runner": "run_v1_5_formal_open_flow_sampling",
            "physical_meaning": template.get("physical_meaning")
            or (
                "Minimal no-write CO2 open-flow resampling point. The gas valve must stay open through "
                "purge, stability gating, and sample-window collection; pressure is recorded as an input "
                "diagnostic and is not a sealed-pressure fitting term."
            ),
            "minimal_resampling_sequence": row.get("sequence", ""),
            "minimal_resampling_reason": row.get("reason", ""),
            "not_real_acceptance_evidence": True,
            "no_write": True,
        }
        queue_row["runner_args"] = _co2_runner_args(queue_row)
        canonical_rows.append(queue_row)
    return canonical_rows


def _physical_gates() -> str:
    return "; ".join(
        [
            "气阀在吹扫、判稳、采样期间必须保持打开",
            "开放流通路径保持连续刷新",
            "露点达到深干或露点斜率稳定门限",
            "CO2 filtered ratio 达到 A 级判稳目标",
            "每台分析仪独立判稳和分级",
            "状态寄存器无正式校准阻断项",
            "开放流通压力仅作为诊断记录，不作硬阻断",
        ]
    )


def build_co2_s13_minimal_resampling_runlist(
    *,
    point_summary_csv: str | Path,
    template_queue_csv: str | Path | None = None,
    max_points: int = DEFAULT_MAX_POINTS,
    acceptance_percent: float = DEFAULT_ACCEPTANCE_PERCENT,
    excluded_temperatures_c: Sequence[float] = DEFAULT_EXCLUDED_TEMPERATURES_C,
) -> Dict[str, Any]:
    source_rows = _read_csv(point_summary_csv)
    excluded_rows: List[Dict[str, Any]] = []
    candidate_rows: List[Dict[str, Any]] = []
    monitor_rows: List[Dict[str, Any]] = []

    for row in source_rows:
        item = dict(row)
        if _temperature_is_excluded(item.get("temperature_c"), excluded_temperatures_c):
            item["exclusion_reason"] = "temperature_group_excluded_for_runtime_cost"
            item["recommended_action_after_exclusion"] = "do_not_resample_this_round"
            excluded_rows.append(item)
            continue
        if _needs_resampling(item):
            candidate_rows.append(item)
        else:
            monitor_rows.append(item)

    candidate_rows.sort(key=_priority, reverse=True)
    selected = candidate_rows if int(max_points) <= 0 else candidate_rows[: int(max_points)]

    runlist: List[Dict[str, Any]] = []
    for index, row in enumerate(selected, start=1):
        bridge_over = int(_safe_float(row.get("bridged_over_acceptance_count")) or 0)
        bridge_max = _safe_float(row.get("bridged_max_abs_relative_error_percent"))
        base_max = _safe_float(row.get("base_max_abs_relative_error_percent"))
        runlist.append(
            {
                "sequence": index,
                "point_identity": row.get("point_identity", ""),
                "temperature_c": row.get("temperature_c", ""),
                "gas_ppm": row.get("gas_ppm", ""),
                "route": "CO2_open_flow",
                "run_mode": "minimal_resample_no_write",
                "reason": (
                    "state_bridge_failed_to_close_point"
                    if bridge_over > 0
                    else "bridge_support_or_point_state_needs_refresh"
                ),
                "device_count": row.get("device_count", ""),
                "base_over_acceptance_count": row.get("base_over_acceptance_count", ""),
                "bridged_over_acceptance_count": row.get("bridged_over_acceptance_count", ""),
                "base_max_abs_relative_error_percent": base_max if base_max is not None else "",
                "bridged_max_abs_relative_error_percent": bridge_max if bridge_max is not None else "",
                "mean_common_error_ppm": row.get("mean_common_error_ppm", ""),
                "acceptance_percent": float(acceptance_percent),
                "required_physical_gates": _physical_gates(),
                "sample_window_rule": "select_best_stable_dry_window_while_gas_valve_is_open",
                "per_analyzer_rule": "independent_grade_each_analyzer_do_not_fail_all_for_one_device",
                "not_to_rerun_full_flow": True,
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
                "not_real_acceptance_evidence": True,
            }
        )

    queue_manifest = [
        {
            "sequence": row["sequence"],
            "phase": "CO2_minimal_resampling",
            "temperature_c": row["temperature_c"],
            "gas_ppm": row["gas_ppm"],
            "point_identity": row["point_identity"],
            "route": row["route"],
            "operator_note": "执行时只补该点；不要重跑全温度全气点。",
            "precondition": "设备初始化、压力/温度输入量可信；气路露点仪在线。",
            "no_write": True,
        }
        for row in runlist
    ]
    canonical_queue = _build_canonical_queue(runlist, template_queue_csv=template_queue_csv)

    return {
        "run_summary": [
            {
                "created_at": _now(),
                "point_summary_csv": str(Path(point_summary_csv).resolve()),
                "template_queue_csv": (
                    str(Path(template_queue_csv).resolve()) if template_queue_csv not in (None, "") else ""
                ),
                "source_point_count": len(source_rows),
                "candidate_point_count_after_exclusions": len(candidate_rows),
                "selected_point_count": len(runlist),
                "monitor_only_point_count": len(monitor_rows),
                "excluded_point_count": len(excluded_rows),
                "excluded_temperatures_c": ";".join(str(value) for value in excluded_temperatures_c),
                "max_points": int(max_points),
                "acceptance_percent": float(acceptance_percent),
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
                "not_real_acceptance_evidence": True,
            }
        ],
        "runlist": runlist,
        "canonical_co2_queue": canonical_queue,
        "queue_manifest": queue_manifest,
        "excluded_points": excluded_rows,
        "monitor_only_points": monitor_rows,
    }


def _fmt(value: Any, digits: int = 3) -> str:
    number = _safe_float(value)
    if number is None:
        return ""
    return f"{number:.{digits}f}"


def write_co2_s13_minimal_resampling_runlist(
    *,
    point_summary_csv: str | Path,
    output_dir: str | Path,
    template_queue_csv: str | Path | None = None,
    max_points: int = DEFAULT_MAX_POINTS,
    acceptance_percent: float = DEFAULT_ACCEPTANCE_PERCENT,
    excluded_temperatures_c: Sequence[float] = DEFAULT_EXCLUDED_TEMPERATURES_C,
) -> Dict[str, str]:
    output = Path(output_dir)
    tables = build_co2_s13_minimal_resampling_runlist(
        point_summary_csv=point_summary_csv,
        template_queue_csv=template_queue_csv,
        max_points=max_points,
        acceptance_percent=acceptance_percent,
        excluded_temperatures_c=excluded_temperatures_c,
    )
    paths = {
        "run_summary": output / "co2_s13_minimal_resampling_run_summary.csv",
        "runlist": output / "co2_s13_minimal_resampling_runlist.csv",
        "canonical_co2_queue": output / "co2_s13_minimal_resampling_canonical_co2_queue.csv",
        "queue_manifest": output / "co2_s13_minimal_resampling_queue_manifest.csv",
        "excluded_points": output / "co2_s13_minimal_resampling_excluded_points.csv",
        "monitor_only_points": output / "co2_s13_minimal_resampling_monitor_only_points.csv",
        "metadata": output / "co2_s13_minimal_resampling_meta.json",
        "markdown": output / "co2_s13_minimal_resampling_runlist_zh.md",
    }
    _write_csv(paths["run_summary"], tables["run_summary"])
    _write_csv(paths["runlist"], tables["runlist"])
    _write_csv_with_headers(paths["canonical_co2_queue"], tables["canonical_co2_queue"], CANONICAL_CO2_QUEUE_HEADERS)
    _write_csv(paths["queue_manifest"], tables["queue_manifest"])
    _write_csv(paths["excluded_points"], tables["excluded_points"])
    _write_csv(paths["monitor_only_points"], tables["monitor_only_points"])
    output.mkdir(parents=True, exist_ok=True)
    with paths["metadata"].open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "tool": "co2_s13_minimal_resampling_runlist",
                "created_at": _now(),
                "inputs": {
                    "point_summary_csv": str(Path(point_summary_csv).resolve()),
                    "template_queue_csv": (
                        str(Path(template_queue_csv).resolve()) if template_queue_csv not in (None, "") else ""
                    ),
                    "max_points": max_points,
                    "acceptance_percent": acceptance_percent,
                    "excluded_temperatures_c": list(excluded_temperatures_c),
                },
                "boundary": {
                    "opens_com_ports": False,
                    "controls_water_or_gas_routes": False,
                    "writes_coefficients": False,
                    "not_real_acceptance_evidence": True,
                },
                "outputs": {key: str(value.resolve()) for key, value in paths.items()},
            },
            handle,
            ensure_ascii=False,
            indent=2,
        )
    with paths["markdown"].open("w", encoding="utf-8") as handle:
        handle.write(render_markdown(tables))
    return {key: str(value) for key, value in paths.items()}


# Single UTF-8 Chinese renderer used by report exports.
def render_markdown(tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> str:
    summary = list(tables.get("run_summary") or [{}])[0]
    runlist = list(tables.get("runlist") or [])
    excluded = list(tables.get("excluded_points") or [])
    lines = [
        "# CO2 最小补采运行清单",
        "",
        "本清单由 V1.5 CO2 state bridge closure 结果离线生成，只用于指导下一轮最小补采；它不打开 COM、不控制气路/水路、不写 SENCO，也不是 real acceptance 证据。",
        "",
        "## 本轮边界",
        "",
        f"- 排除温度组：`{summary.get('excluded_temperatures_c', '')}`。",
        "- 用户明确要求：`-20°C` 太慢，本轮不补 `-20°C/0ppm`，也不补 `-20°C` 其它气点。",
        f"- 选中补采点数：`{summary.get('selected_point_count', 0)}`。",
        f"- 排除点数：`{summary.get('excluded_point_count', 0)}`。",
        "- 目标：用少量点确认共同源状态、管路状态是否导致残差，而不是重跑全温度全气点。",
        "",
        "## 建议补采顺序",
        "",
        "| 顺序 | 点位 | 温度(°C) | CO2(ppm) | 桥接后超差设备数 | 桥接后最大相对误差(%) | 原因 |",
        "| ---: | --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in runlist:
        lines.append(
            "| {seq} | {point} | {temp} | {ppm} | {over} | {err} | {reason} |".format(
                seq=row.get("sequence", ""),
                point=row.get("point_identity", ""),
                temp=_fmt(row.get("temperature_c"), 1),
                ppm=_fmt(row.get("gas_ppm"), 1),
                over=row.get("bridged_over_acceptance_count", ""),
                err=_fmt(row.get("bridged_max_abs_relative_error_percent")),
                reason=row.get("reason", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 采样物理门禁",
            "",
            "- 气阀必须在吹扫、判稳、采样窗口期间保持打开；采样结束后再关阀。",
            "- 采用开放流通，确认管路持续刷新；露点达到深干或露点斜率稳定后，再看 CO2 filtered ratio。",
            "- ratio 尽量达到 A 级判稳目标；每台分析仪独立判稳、独立分级，不能因为一台不稳拖死全部设备。",
            "- 状态寄存器异常帧、野值、失鲜帧必须保留并给出拒绝原因。",
            "- 开放流通压力波动只作为诊断证据，不作为气路采样硬阻断。",
            "",
            "## 不补采点说明",
            "",
        ]
    )
    if excluded:
        lines.extend(
            [
                "| 点位 | 温度(°C) | CO2(ppm) | 排除原因 |",
                "| --- | ---: | ---: | --- |",
            ]
        )
        for row in excluded:
            lines.append(
                "| {point} | {temp} | {ppm} | {reason} |".format(
                    point=row.get("point_identity", ""),
                    temp=_fmt(row.get("temperature_c"), 1),
                    ppm=_fmt(row.get("gas_ppm"), 1),
                    reason=row.get("exclusion_reason", ""),
                )
            )
    else:
        lines.append("- 本轮没有按温度排除的点。")
    lines.extend(
        [
            "",
            "## 物理意义",
            "",
            "- 最小补采不是重新校准全流程，而是只补对当前残差贡献最大的共同点位，确认这些点是否由源状态、管路湿度、露点、ratio 稳态或目标映射造成。",
            "- 排除 `-20°C` 是运行成本约束：低温温箱等待时间长，且本轮目标是快速闭合主模型残差；如后续证据显示低温边界仍是主因，再单独安排低温专题补采。",
            "- 新补采数据应进入后续 S1/S3 主模型评审；S5 只能作为输出层线性修正，不能替代主模型对温度、低端、共同点位残差的解释。",
        ]
    )
    return "\n".join(lines) + "\n"
