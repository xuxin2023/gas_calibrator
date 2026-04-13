"""Offline V1 CO2 threshold replay matrix and sensitivity audit.

This tool is evidence-only. It replays completed V1 sample/point exports or
fixture CSVs against:

- a legacy whole-window baseline
- the current hardened V1 CO2 steady-state + bad-frame path
- a small engineering threshold matrix

It must not open devices or alter live runtime paths.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from ..config import load_config
from ..data.points import CalibrationPoint
from ..logging_utils import _field_label
from ..workflow.runner import CalibrationRunner


DEFAULT_TINY_DELTA_PPM = 0.5
DEFAULT_MEANINGFUL_DELTA_PPM = 2.0


def _no_op(*_args: Any, **_kwargs: Any) -> None:
    return None


def _read_csv_rows(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
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


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "null", "None"):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _safe_int(value: Any) -> Optional[int]:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    try:
        return int(round(numeric))
    except Exception:
        return None


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _latest_matching(run_dir: Path, pattern: str) -> Optional[Path]:
    matches = [path for path in run_dir.glob(pattern) if path.is_file()]
    if not matches:
        return None
    return max(matches, key=lambda item: item.stat().st_mtime)


def _deep_merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = dict(base)
    for key, value in dict(override or {}).items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), Mapping):
            merged[key] = _deep_merge(dict(merged.get(key) or {}), value)
        else:
            merged[key] = value
    return merged


def _field_aliases() -> Dict[str, str]:
    aliases: Dict[str, str] = {}
    canonical_keys = [
        "sample_ts",
        "sample_start_ts",
        "sample_end_ts",
        "point_row",
        "point_phase",
        "point_tag",
        "point_title",
        "temp_chamber_c",
        "co2_ppm_target",
        "pressure_target_hpa",
        "pressure_mode",
        "pressure_target_label",
        "co2_ppm",
        "frame_usable",
        "frame_status",
        "chamber_temp_c",
        "case_temp_c",
    ]
    for key in canonical_keys:
        aliases[key] = key
        aliases[_field_label(key)] = key
    explicit = {
        "PointRow": "point_row",
        "PointPhase": "point_phase",
        "PointTag": "point_tag",
        "PointTitle": "point_title",
        "TempSet": "temp_chamber_c",
        "PressureTarget": "pressure_target_hpa",
        "PressureMode": "pressure_mode",
        "PressureTargetLabel": "pressure_target_label",
        "ppm_CO2_Tank": "co2_ppm_target",
    }
    for key, canonical in explicit.items():
        aliases[key] = canonical
    for idx in range(1, 9):
        prefix = f"ga{idx:02d}"
        for suffix in ("co2_ppm", "frame_usable", "frame_status", "chamber_temp_c", "case_temp_c"):
            key = f"{prefix}_{suffix}"
            aliases[key] = key
            aliases[_field_label(key)] = key
    return aliases


FIELD_ALIASES = _field_aliases()


def _canonicalize_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, value in dict(row or {}).items():
        canonical = FIELD_ALIASES.get(str(key), str(key))
        if canonical not in out:
            out[canonical] = value
    return out


def _normalize_phase(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"co2", "气路"}:
        return "co2"
    if text in {"h2o", "水路"}:
        return "h2o"
    return text


def _mean(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    return float(sum(values) / len(values))


def _median(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    return float(statistics.median(values))


def _ratio(num: int, den: int) -> float:
    if den <= 0:
        return 0.0
    return round(float(num) / float(den), 6)


def _resolve_samples_points_from_run(run_dir: Path) -> Tuple[Path, Optional[Path], Dict[str, Any]]:
    samples_path = _latest_matching(run_dir, "samples_*.csv")
    if samples_path is None:
        raise FileNotFoundError(f"未找到 samples_*.csv: {run_dir}")
    points_path = _latest_matching(run_dir, "points_readable_*.csv") or _latest_matching(run_dir, "points_*.csv")
    runtime_snapshot = run_dir / "runtime_config_snapshot.json"
    runtime_cfg: Dict[str, Any] = {}
    if runtime_snapshot.exists():
        try:
            runtime_cfg = json.loads(runtime_snapshot.read_text(encoding="utf-8"))
        except Exception:
            runtime_cfg = {}
    return samples_path, points_path, runtime_cfg


def _build_runtime_cfg(
    *,
    config_path: Optional[str],
    runtime_cfg: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    base_cfg = load_config(config_path or "configs/default_config.json")
    if isinstance(runtime_cfg, Mapping) and runtime_cfg:
        return _deep_merge(base_cfg, runtime_cfg)
    return base_cfg


def _quality_cfg_from_runtime(runtime_cfg: Mapping[str, Any]) -> Dict[str, Any]:
    return dict(
        ((runtime_cfg or {}).get("workflow", {}) or {}).get("sampling", {}) or {}
    ).get("quality", {}) or {}


def _matrix_design_notes(base_quality_cfg: Mapping[str, Any]) -> List[str]:
    notes = [
        "矩阵采用工程可跑完的设计：legacy baseline + current hardened baseline + 单维偏移 + 少量交叉组合。",
        "本次只审计当前 V1 live 路径已存在或直接相邻的阈值，不人为引入 V2 runtime 配置。",
        "如果某些期望维度在当前 V1 live 路径中并不存在独立配置项，则在报告中显式说明为固定项，而不伪造并行阈值体系。",
    ]
    if "co2_steady_state_min_samples" not in base_quality_cfg:
        notes.append("当前配置里缺少 co2_steady_state_min_samples，工具将回退到 runner 默认值。")
    return notes


@dataclass(frozen=True)
class ThresholdScenario:
    name: str
    kind: str
    description: str
    overrides: Dict[str, Any]


@dataclass
class ReplayPoint:
    point_key: str
    point_row: Optional[int]
    point_phase: str
    point_tag: str
    point_title: str
    temp_chamber_c: Optional[float]
    co2_ppm_target: Optional[float]
    pressure_target_hpa: Optional[float]
    pressure_mode: str
    samples: List[Dict[str, Any]]


def _build_threshold_matrix(base_runtime_cfg: Mapping[str, Any]) -> List[ThresholdScenario]:
    quality = _quality_cfg_from_runtime(base_runtime_cfg)
    min_samples = int(quality.get("co2_steady_state_min_samples", 4) or 4)
    max_std = float(quality.get("co2_steady_state_max_std_ppm", 3.0) or 3.0)
    max_range = float(quality.get("co2_steady_state_max_range_ppm", 8.0) or 8.0)
    max_slope = float(quality.get("co2_steady_state_max_abs_slope_ppm_per_s", 1.0) or 1.0)
    spike_delta = float(quality.get("co2_bad_frame_isolated_spike_delta_ppm", 50.0) or 50.0)
    neighbor_delta = float(quality.get("co2_bad_frame_neighbor_match_max_delta_ppm", 8.0) or 8.0)
    return [
        ThresholdScenario(
            name="current_hardened_baseline",
            kind="current_hardened",
            description="当前 hardened baseline：沿用 #3 + #5 默认阈值。",
            overrides={},
        ),
        ThresholdScenario(
            name="steady_looser",
            kind="candidate",
            description="放宽 steady-state 统计阈值，观察干净点零扰动与脏点放行风险。",
            overrides={
                "workflow": {
                    "sampling": {
                        "quality": {
                            "co2_steady_state_min_samples": max(3, min_samples - 1),
                            "co2_steady_state_max_std_ppm": max_std * 1.5,
                            "co2_steady_state_max_range_ppm": max_range * 1.5,
                            "co2_steady_state_max_abs_slope_ppm_per_s": max_slope * 1.5,
                        }
                    }
                }
            },
        ),
        ThresholdScenario(
            name="steady_tighter",
            kind="candidate",
            description="收紧 steady-state 统计阈值，观察 fail/warn/degraded 是否明显上升。",
            overrides={
                "workflow": {
                    "sampling": {
                        "quality": {
                            "co2_steady_state_min_samples": min_samples + 1,
                            "co2_steady_state_max_std_ppm": max(0.25, max_std * 0.6),
                            "co2_steady_state_max_range_ppm": max(0.5, max_range * 0.6),
                            "co2_steady_state_max_abs_slope_ppm_per_s": max(0.05, max_slope * 0.6),
                        }
                    }
                }
            },
        ),
        ThresholdScenario(
            name="quarantine_disabled",
            kind="candidate",
            description="关闭坏帧隔离，量化明显坏帧对代表值的污染回归。",
            overrides={
                "workflow": {
                    "sampling": {
                        "quality": {
                            "co2_bad_frame_quarantine_enabled": False,
                        }
                    }
                }
            },
        ),
        ThresholdScenario(
            name="spike_more_sensitive",
            kind="candidate",
            description="让 isolated spike 更敏感，检查脏点恢复与误杀干净点之间的平衡。",
            overrides={
                "workflow": {
                    "sampling": {
                        "quality": {
                            "co2_bad_frame_isolated_spike_delta_ppm": max(10.0, spike_delta * 0.5),
                            "co2_bad_frame_neighbor_match_max_delta_ppm": max(2.0, neighbor_delta * 0.5),
                        }
                    }
                }
            },
        ),
        ThresholdScenario(
            name="spike_more_permissive",
            kind="candidate",
            description="放宽 isolated spike 阈值，检查坏帧污染是否重新放大。",
            overrides={
                "workflow": {
                    "sampling": {
                        "quality": {
                            "co2_bad_frame_isolated_spike_delta_ppm": spike_delta * 1.6,
                            "co2_bad_frame_neighbor_match_max_delta_ppm": neighbor_delta * 1.5,
                        }
                    }
                }
            },
        ),
        ThresholdScenario(
            name="source_trust_disabled",
            kind="candidate",
            description="关闭 source trust，仅保留 legacy 选源语义，观察 source fallback 是否退化。",
            overrides={
                "workflow": {
                    "sampling": {
                        "quality": {
                            "co2_source_trust_enabled": False,
                        }
                    }
                }
            },
        ),
        ThresholdScenario(
            name="balanced_guardrail",
            kind="candidate",
            description="保守交叉组合：轻微收紧 steady-state，并适度收紧 spike 检测。",
            overrides={
                "workflow": {
                    "sampling": {
                        "quality": {
                            "co2_steady_state_min_samples": min_samples,
                            "co2_steady_state_max_std_ppm": max(0.25, max_std * 0.8),
                            "co2_steady_state_max_range_ppm": max(0.5, max_range * 0.8),
                            "co2_steady_state_max_abs_slope_ppm_per_s": max(0.05, max_slope * 0.8),
                            "co2_bad_frame_isolated_spike_delta_ppm": max(10.0, spike_delta * 0.75),
                            "co2_bad_frame_neighbor_match_max_delta_ppm": max(2.0, neighbor_delta * 0.75),
                        }
                    }
                }
            },
        ),
    ]


def _canonicalize_samples(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return [_canonicalize_row(row) for row in rows or []]


def _load_points_map(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    points_map: Dict[str, Dict[str, Any]] = {}
    for raw_row in rows or []:
        row = _canonicalize_row(raw_row)
        point_row = _safe_int(row.get("point_row"))
        point_tag = _safe_text(row.get("point_tag"))
        point_key = str(point_row) if point_row is not None else point_tag
        if not point_key:
            continue
        points_map[point_key] = row
    return points_map


def _group_samples(rows: Sequence[Mapping[str, Any]], points_map: Mapping[str, Mapping[str, Any]]) -> List[ReplayPoint]:
    grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    first_meta: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for raw_row in rows or []:
        row = _canonicalize_row(raw_row)
        phase = _normalize_phase(row.get("point_phase"))
        point_row = _safe_int(row.get("point_row"))
        point_tag = _safe_text(row.get("point_tag"))
        point_key = str(point_row) if point_row is not None else point_tag
        if not point_key:
            point_key = _safe_text(row.get("point_title")) or f"rowless_{len(grouped)+1}"
        group_key = (point_key, phase)
        grouped.setdefault(group_key, []).append(row)
        first_meta.setdefault(group_key, row)

    bundles: List[ReplayPoint] = []
    for (point_key, phase), sample_rows in grouped.items():
        if _normalize_phase(phase) != "co2":
            continue
        point_row = _safe_int(first_meta[(point_key, phase)].get("point_row"))
        point_meta = dict(points_map.get(point_key) or first_meta[(point_key, phase)] or {})
        bundles.append(
            ReplayPoint(
                point_key=point_key,
                point_row=point_row,
                point_phase="co2",
                point_tag=_safe_text(point_meta.get("point_tag")),
                point_title=_safe_text(point_meta.get("point_title")),
                temp_chamber_c=_safe_float(point_meta.get("temp_chamber_c")),
                co2_ppm_target=_safe_float(point_meta.get("co2_ppm_target")),
                pressure_target_hpa=_safe_float(point_meta.get("pressure_target_hpa")),
                pressure_mode=_safe_text(point_meta.get("pressure_mode")),
                samples=sample_rows,
            )
        )
    bundles.sort(key=lambda item: (item.point_row is None, item.point_row or 0, item.point_tag, item.point_key))
    return bundles


def _build_point(bundle: ReplayPoint) -> CalibrationPoint:
    return CalibrationPoint(
        index=int(bundle.point_row or 0),
        temp_chamber_c=float(bundle.temp_chamber_c or 0.0),
        co2_ppm=bundle.co2_ppm_target,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=bundle.pressure_target_hpa,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group="A",
    )


def _prime_runner_prefixed_sources(
    runner: CalibrationRunner,
    samples: Sequence[Mapping[str, Any]],
) -> None:
    if any(str(key).startswith("gas_analyzer_") for key in runner.devices.keys()):
        return
    configured = list((((runner.cfg or {}).get("devices", {}) or {}).get("gas_analyzers", []) or []))
    configured_count = len(configured)
    detected: set[int] = set()
    for row in samples or []:
        for key in dict(row or {}).keys():
            text = str(key)
            if len(text) >= 5 and text.startswith("ga") and text[2:4].isdigit() and text[4] == "_":
                detected.add(int(text[2:4]))
    total = max(configured_count, max(detected) if detected else 0)
    for idx in range(1, total + 1):
        runner.devices.setdefault(f"gas_analyzer_{idx}", object())


def _legacy_baseline_from_samples(runner: CalibrationRunner, samples: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    _prime_runner_prefixed_sources(runner, samples)
    info = runner._primary_or_first_usable_analyzer_window_series(list(samples), "co2_ppm")
    series = list(info.get("series") or [])
    values = [float(entry.get("value")) for entry in series if _safe_float(entry.get("value")) is not None]
    measured = _mean(values)
    return {
        "legacy_source": str(info.get("analyzer_source") or ""),
        "legacy_value_key": str(info.get("value_key") or ""),
        "legacy_rows": len(series),
        "legacy_measured_value": measured,
        "legacy_measured_value_source": "legacy_primary_or_first_usable_full_window_mean" if measured is not None else "legacy_no_usable_value",
    }


def _evaluate_hardened_point(
    *,
    bundle: ReplayPoint,
    runtime_cfg: Mapping[str, Any],
    scenario: ThresholdScenario,
) -> Dict[str, Any]:
    runner = CalibrationRunner(deepcopy(dict(runtime_cfg or {})), {}, None, _no_op, _no_op)
    point = _build_point(bundle)
    sample_rows = [dict(row) for row in bundle.samples]
    _prime_runner_prefixed_sources(runner, sample_rows)
    result = runner._evaluate_co2_steady_state_window_qc(point, phase="co2", samples=sample_rows)
    quality = runner._update_point_quality_summary(point, phase="co2")
    measured_value = _safe_float(result.get("co2_representative_value"))
    return {
        "scenario": scenario.name,
        "scenario_kind": scenario.kind,
        "scenario_description": scenario.description,
        "point_key": bundle.point_key,
        "point_row": bundle.point_row,
        "point_phase": bundle.point_phase,
        "point_tag": bundle.point_tag,
        "point_title": bundle.point_title,
        "temp_chamber_c": bundle.temp_chamber_c,
        "co2_ppm_target": bundle.co2_ppm_target,
        "pressure_target_hpa": bundle.pressure_target_hpa,
        "pressure_mode": bundle.pressure_mode,
        "measured_value": measured_value,
        "measured_value_source": _safe_text(result.get("measured_value_source")),
        "co2_steady_window_found": bool(result.get("co2_steady_window_found")) if result.get("co2_steady_window_found") is not None else None,
        "co2_steady_window_status": _safe_text(result.get("co2_steady_window_status")),
        "co2_steady_window_reason": _safe_text(result.get("co2_steady_window_reason")),
        "co2_steady_window_start_sample_index": _safe_int(result.get("co2_steady_window_start_sample_index")),
        "co2_steady_window_end_sample_index": _safe_int(result.get("co2_steady_window_end_sample_index")),
        "co2_steady_window_sample_count": _safe_int(result.get("co2_steady_window_sample_count")),
        "co2_steady_window_mean_ppm": _safe_float(result.get("co2_steady_window_mean_ppm")),
        "co2_steady_window_std_ppm": _safe_float(result.get("co2_steady_window_std_ppm")),
        "co2_steady_window_range_ppm": _safe_float(result.get("co2_steady_window_range_ppm")),
        "co2_steady_window_slope_ppm_per_s": _safe_float(result.get("co2_steady_window_slope_ppm_per_s")),
        "co2_bad_frame_count": _safe_int(result.get("co2_bad_frame_count")) or 0,
        "co2_bad_frame_ratio": _safe_float(result.get("co2_bad_frame_ratio")) or 0.0,
        "co2_soft_warn_count": _safe_int(result.get("co2_soft_warn_count")) or 0,
        "co2_soft_warn_ratio": _safe_float(result.get("co2_soft_warn_ratio")) or 0.0,
        "co2_rows_before_quarantine": _safe_int(result.get("co2_rows_before_quarantine")) or 0,
        "co2_rows_after_quarantine": _safe_int(result.get("co2_rows_after_quarantine")) or 0,
        "co2_source_selected": _safe_text(result.get("co2_source_selected")),
        "co2_source_candidates": _safe_text(result.get("co2_source_candidates")),
        "co2_source_switch_reason": _safe_text(result.get("co2_source_switch_reason")),
        "co2_source_trust_reason": _safe_text(result.get("co2_source_trust_reason")),
        "co2_quarantine_reason_summary": _safe_text(result.get("co2_quarantine_reason_summary")),
        "point_quality_status": _safe_text(quality.get("point_quality_status")),
        "point_quality_reason": _safe_text(quality.get("point_quality_reason")),
        "point_quality_flags": _safe_text(quality.get("point_quality_flags")),
        "point_quality_blocked": bool(quality.get("point_quality_blocked")),
    }


def _scenario_runtime_cfg(base_runtime_cfg: Mapping[str, Any], scenario: ThresholdScenario) -> Dict[str, Any]:
    return _deep_merge(deepcopy(dict(base_runtime_cfg or {})), scenario.overrides)


def _delta(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None:
        return None
    return float(a - b)


def _abs_delta(a: Optional[float], b: Optional[float]) -> Optional[float]:
    diff = _delta(a, b)
    return None if diff is None else abs(diff)


def _abs_error(value: Optional[float], target: Optional[float]) -> Optional[float]:
    if value is None or target is None:
        return None
    return abs(float(value) - float(target))


def _is_clean_stable(result: Mapping[str, Any]) -> bool:
    return (
        (_safe_int(result.get("co2_bad_frame_count")) or 0) == 0
        and (_safe_int(result.get("co2_soft_warn_count")) or 0) == 0
        and _safe_text(result.get("co2_source_selected")) in {"", "primary"}
        and _safe_text(result.get("co2_source_switch_reason")) == ""
        and _safe_text(result.get("measured_value_source")) == "co2_steady_state_window"
        and _safe_text(result.get("co2_steady_window_status")) == "pass"
    )


def _is_dirty_or_recovered_candidate(result: Mapping[str, Any]) -> bool:
    return (
        (_safe_int(result.get("co2_bad_frame_count")) or 0) > 0
        or _safe_text(result.get("co2_source_switch_reason")) != ""
        or _safe_text(result.get("measured_value_source")) != "co2_steady_state_window"
        or _safe_text(result.get("co2_steady_window_status")) != "pass"
    )


def _degraded_bucket(result: Mapping[str, Any]) -> bool:
    source = _safe_text(result.get("measured_value_source"))
    return "fallback" in source or _safe_text(result.get("co2_source_switch_reason")) != ""


def _reason_top_counts(rows: Sequence[Mapping[str, Any]], key: str, limit: int = 5) -> str:
    counts: Dict[str, int] = {}
    for row in rows:
        text = _safe_text(row.get(key))
        if not text:
            continue
        counts[text] = counts.get(text, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return "; ".join(f"{reason}:{count}" for reason, count in ranked[:limit])


def _bucket_key(value: Optional[float]) -> str:
    if value is None:
        return "missing"
    if abs(value - round(value)) <= 1e-9:
        return str(int(round(value)))
    return f"{value:.3f}"


def _aggregate_scenario(
    *,
    scenario: ThresholdScenario,
    rows: Sequence[Mapping[str, Any]],
    current_baseline_rows: Mapping[str, Mapping[str, Any]],
    tiny_delta_ppm: float,
    meaningful_delta_ppm: float,
) -> Dict[str, Any]:
    total = len(rows)
    pass_count = sum(1 for row in rows if _safe_text(row.get("point_quality_status")) == "pass")
    warn_count = sum(1 for row in rows if _safe_text(row.get("point_quality_status")) == "warn")
    fail_count = sum(1 for row in rows if _safe_text(row.get("point_quality_status")) == "fail")
    degraded_count = sum(1 for row in rows if _degraded_bucket(row))
    steady_window_hit_count = sum(1 for row in rows if row.get("co2_steady_window_found") is True)
    trailing_fallback_count = sum(1 for row in rows if "fallback" in _safe_text(row.get("measured_value_source")))
    source_switch_count = sum(1 for row in rows if _safe_text(row.get("co2_source_switch_reason")) != "")
    all_sources_rejected_count = sum(1 for row in rows if _safe_text(row.get("measured_value_source")) == "co2_no_trusted_source")
    rows_insufficient_count = sum(
        1
        for row in rows
        if "insufficient" in _safe_text(row.get("co2_source_trust_reason"))
        or "insufficient" in _safe_text(row.get("co2_steady_window_reason"))
    )
    no_steady_window_count = sum(
        1 for row in rows if "no_qualified_steady_state_window" in _safe_text(row.get("co2_steady_window_reason"))
    )

    deltas_vs_legacy = [_safe_float(row.get("delta_abs_vs_legacy")) for row in rows]
    deltas_vs_legacy = [value for value in deltas_vs_legacy if value is not None]
    deltas_vs_current = [_safe_float(row.get("delta_abs_vs_current")) for row in rows]
    deltas_vs_current = [value for value in deltas_vs_current if value is not None]

    clean_rows: List[Mapping[str, Any]] = []
    for row in rows:
        baseline = current_baseline_rows.get(str(row.get("point_key")))
        if baseline and _is_clean_stable(baseline):
            clean_rows.append(row)
    unchanged_count = sum(1 for row in clean_rows if (_safe_float(row.get("delta_abs_vs_current")) or 0.0) <= 1e-9)
    tiny_delta_count = sum(
        1
        for row in clean_rows
        if 1e-9 < (_safe_float(row.get("delta_abs_vs_current")) or 0.0) <= tiny_delta_ppm
    )
    meaningful_delta_count = sum(
        1 for row in clean_rows if (_safe_float(row.get("delta_abs_vs_current")) or 0.0) > meaningful_delta_ppm
    )

    dirty_rows: List[Mapping[str, Any]] = []
    for row in rows:
        baseline = current_baseline_rows.get(str(row.get("point_key")))
        if baseline and _is_dirty_or_recovered_candidate(baseline):
            dirty_rows.append(row)

    recovered_vs_legacy_count = 0
    improvement_vs_legacy: List[float] = []
    improvement_vs_current: List[float] = []
    for row in dirty_rows:
        legacy_error = _safe_float(row.get("abs_error_vs_target_legacy"))
        current_error = _safe_float(row.get("abs_error_vs_target_current"))
        candidate_error = _safe_float(row.get("abs_error_vs_target"))
        if legacy_error is not None and candidate_error is not None:
            delta = legacy_error - candidate_error
            improvement_vs_legacy.append(delta)
            if delta > tiny_delta_ppm:
                recovered_vs_legacy_count += 1
        if current_error is not None and candidate_error is not None:
            improvement_vs_current.append(current_error - candidate_error)

    ppm_buckets: Dict[str, List[float]] = {}
    pressure_buckets: Dict[str, List[float]] = {}
    temp_buckets: Dict[str, List[float]] = {}
    route_buckets: Dict[str, List[float]] = {}
    for row in rows:
        delta_abs = _safe_float(row.get("delta_abs_vs_current"))
        if delta_abs is None:
            continue
        ppm_buckets.setdefault(_bucket_key(_safe_float(row.get("co2_ppm_target"))), []).append(delta_abs)
        pressure_buckets.setdefault(_bucket_key(_safe_float(row.get("pressure_target_hpa"))), []).append(delta_abs)
        temp_buckets.setdefault(_bucket_key(_safe_float(row.get("temp_chamber_c"))), []).append(delta_abs)
        route_buckets.setdefault(_safe_text(row.get("pressure_mode")) or "missing", []).append(delta_abs)

    return {
        "scenario": scenario.name,
        "scenario_kind": scenario.kind,
        "description": scenario.description,
        "total_points": total,
        "pass_count": pass_count,
        "warn_count": warn_count,
        "fail_count": fail_count,
        "degraded_count": degraded_count,
        "steady_window_hit_count": steady_window_hit_count,
        "steady_window_hit_ratio": _ratio(steady_window_hit_count, total),
        "trailing_fallback_count": trailing_fallback_count,
        "trailing_fallback_ratio": _ratio(trailing_fallback_count, total),
        "source_switch_count": source_switch_count,
        "source_switch_ratio": _ratio(source_switch_count, total),
        "all_sources_rejected_count": all_sources_rejected_count,
        "rows_insufficient_count": rows_insufficient_count,
        "no_steady_window_count": no_steady_window_count,
        "max_abs_delta_vs_legacy": max(deltas_vs_legacy) if deltas_vs_legacy else None,
        "mean_abs_delta_vs_legacy": _mean(deltas_vs_legacy),
        "median_abs_delta_vs_legacy": _median(deltas_vs_legacy),
        "max_abs_delta_vs_current": max(deltas_vs_current) if deltas_vs_current else None,
        "mean_abs_delta_vs_current": _mean(deltas_vs_current),
        "median_abs_delta_vs_current": _median(deltas_vs_current),
        "clean_points_total": len(clean_rows),
        "clean_unchanged_count": unchanged_count,
        "clean_unchanged_ratio": _ratio(unchanged_count, len(clean_rows)),
        "clean_tiny_delta_count": tiny_delta_count,
        "clean_tiny_delta_ratio": _ratio(tiny_delta_count, len(clean_rows)),
        "clean_meaningful_delta_count": meaningful_delta_count,
        "clean_meaningful_delta_ratio": _ratio(meaningful_delta_count, len(clean_rows)),
        "dirty_points_total": len(dirty_rows),
        "dirty_recovered_vs_legacy_count": recovered_vs_legacy_count,
        "dirty_recovered_vs_legacy_ratio": _ratio(recovered_vs_legacy_count, len(dirty_rows)),
        "dirty_median_improvement_vs_legacy": _median(improvement_vs_legacy),
        "dirty_max_improvement_vs_legacy": max(improvement_vs_legacy) if improvement_vs_legacy else None,
        "dirty_median_improvement_vs_current": _median(improvement_vs_current),
        "dirty_max_improvement_vs_current": max(improvement_vs_current) if improvement_vs_current else None,
        "primary_kept_count": sum(1 for row in rows if _safe_text(row.get("co2_source_selected")) in {"", "primary"}),
        "primary_lost_to_fallback_count": sum(
            1 for row in rows if _safe_text(row.get("co2_source_switch_reason")).startswith("primary_lost_to=")
        ),
        "source_switch_reason_top": _reason_top_counts(rows, "co2_source_switch_reason"),
        "trust_reason_top": _reason_top_counts(rows, "co2_source_trust_reason"),
        "ppm_bucket_mean_abs_delta_vs_current": {
            key: round(_mean(values) or 0.0, 6) for key, values in sorted(ppm_buckets.items())
        },
        "pressure_bucket_mean_abs_delta_vs_current": {
            key: round(_mean(values) or 0.0, 6) for key, values in sorted(pressure_buckets.items())
        },
        "temp_bucket_mean_abs_delta_vs_current": {
            key: round(_mean(values) or 0.0, 6) for key, values in sorted(temp_buckets.items())
        },
        "route_bucket_mean_abs_delta_vs_current": {
            key: round(_mean(values) or 0.0, 6) for key, values in sorted(route_buckets.items())
        },
    }


def _recommend_default(
    summaries: Sequence[Mapping[str, Any]],
    scenarios: Sequence[ThresholdScenario],
) -> Tuple[Optional[str], str, Dict[str, Tuple[Any, Any]]]:
    current = next((row for row in summaries if row.get("scenario") == "current_hardened_baseline"), None)
    if current is None:
        return None, "未找到 current_hardened_baseline，无法生成建议默认阈值。", {}

    candidates = [row for row in summaries if row.get("scenario_kind") == "candidate"]
    current_fail_ratio = _ratio(int(current.get("fail_count") or 0), int(current.get("total_points") or 0))
    viable: List[Mapping[str, Any]] = []
    for row in candidates:
        clean_meaningful = _safe_float(row.get("clean_meaningful_delta_ratio")) or 0.0
        fail_ratio = _ratio(int(row.get("fail_count") or 0), int(row.get("total_points") or 0))
        if clean_meaningful <= 0.05 and fail_ratio <= current_fail_ratio + 0.05:
            viable.append(row)

    picked: Mapping[str, Any] = current
    if viable:
        def _score(row: Mapping[str, Any]) -> Tuple[Any, ...]:
            return (
                _safe_int(row.get("dirty_recovered_vs_legacy_count")) or 0,
                _safe_float(row.get("dirty_median_improvement_vs_legacy")) or 0.0,
                -(_safe_float(row.get("clean_meaningful_delta_ratio")) or 0.0),
                -(_safe_float(row.get("mean_abs_delta_vs_current")) or 0.0),
                -(_ratio(int(row.get("fail_count") or 0), int(row.get("total_points") or 0))),
            )

        best_candidate = max(viable, key=_score)
        current_score = (
            _safe_int(current.get("dirty_recovered_vs_legacy_count")) or 0,
            _safe_float(current.get("dirty_median_improvement_vs_legacy")) or 0.0,
            -(_safe_float(current.get("clean_meaningful_delta_ratio")) or 0.0),
            -(_safe_float(current.get("mean_abs_delta_vs_current")) or 0.0),
            -(_ratio(int(current.get("fail_count") or 0), int(current.get("total_points") or 0))),
        )
        if _score(best_candidate) > current_score:
            picked = best_candidate

    scenario_lookup = {scenario.name: scenario for scenario in scenarios}
    acceptable = [current]
    acceptable.extend(viable)
    acceptable_names = {str(row.get("scenario")) for row in acceptable}
    ranges: Dict[str, Tuple[Any, Any]] = {}
    parameter_keys = [
        "co2_steady_state_min_samples",
        "co2_steady_state_max_std_ppm",
        "co2_steady_state_max_range_ppm",
        "co2_steady_state_max_abs_slope_ppm_per_s",
        "co2_bad_frame_quarantine_enabled",
        "co2_source_trust_enabled",
        "co2_bad_frame_isolated_spike_delta_ppm",
        "co2_bad_frame_neighbor_match_max_delta_ppm",
    ]
    for param in parameter_keys:
        values: List[Any] = []
        for scenario_name in acceptable_names:
            scenario = scenario_lookup.get(scenario_name)
            if scenario is None:
                continue
            current_value = (((scenario.overrides.get("workflow", {}) or {}).get("sampling", {}) or {}).get("quality", {}) or {}).get(param)
            if current_value is not None:
                values.append(current_value)
        if not values:
            continue
        try:
            numeric_values = [float(value) for value in values]
        except Exception:
            unique_values = sorted({str(value) for value in values})
            ranges[param] = (unique_values[0], unique_values[-1])
        else:
            ranges[param] = (min(numeric_values), max(numeric_values))

    reason = (
        f"建议默认阈值选择 {picked.get('scenario')}："
        f"在当前 replay/fixture 覆盖下，干净点扰动={_safe_float(picked.get('clean_meaningful_delta_ratio')) or 0.0:.3f}，"
        f"dirty 恢复数={_safe_int(picked.get('dirty_recovered_vs_legacy_count')) or 0}，"
        f"fail 占比={_ratio(int(picked.get('fail_count') or 0), int(picked.get('total_points') or 0)):.3f}。"
    )
    return str(picked.get("scenario")), reason, ranges


def _format_markdown_report(
    *,
    inputs: Sequence[str],
    matrix_notes: Sequence[str],
    summary_rows: Sequence[Mapping[str, Any]],
    detail_rows: Sequence[Mapping[str, Any]],
    recommended_scenario: Optional[str],
    recommendation_reason: str,
    acceptable_ranges: Mapping[str, Tuple[Any, Any]],
) -> str:
    lines: List[str] = []
    lines.append("# V1 CO2 阈值回放矩阵 / 敏感性审计报告")
    lines.append("")
    lines.append("> replay evidence only")
    lines.append("> not real acceptance evidence")
    lines.append("")
    lines.append("## 覆盖输入")
    for item in inputs:
        lines.append(f"- `{item}`")
    lines.append("")
    lines.append("## 基线定义")
    lines.append("- legacy baseline：`primary_or_first_usable_full_window_mean`，代表旧口径 whole-window/旧选源语义。")
    lines.append("- current hardened baseline：当前 #3 + #5 默认阈值下的 V1 hardened CO2 结果。")
    lines.append("- candidate thresholds：在 current hardened baseline 基础上做单维或少量交叉组合。")
    lines.append("")
    lines.append("## 矩阵设计原则")
    for note in matrix_notes:
        lines.append(f"- {note}")
    lines.append("")
    lines.append("## 情景摘要")
    lines.append("")
    lines.append("| 情景 | 类型 | 总点数 | pass | warn | fail | degraded | 稳态窗命中率 | trailing fallback 比例 | 平均绝对变化(对 current) | clean meaningful-delta 比例 | dirty 恢复数(对 legacy) |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for row in summary_rows:
        lines.append(
            "| "
            f"{row.get('scenario')} | {row.get('scenario_kind')} | {row.get('total_points')} | "
            f"{row.get('pass_count')} | {row.get('warn_count')} | {row.get('fail_count')} | "
            f"{row.get('degraded_count')} | {(_safe_float(row.get('steady_window_hit_ratio')) or 0.0):.3f} | "
            f"{(_safe_float(row.get('trailing_fallback_ratio')) or 0.0):.3f} | "
            f"{(_safe_float(row.get('mean_abs_delta_vs_current')) or 0.0):.3f} | "
            f"{(_safe_float(row.get('clean_meaningful_delta_ratio')) or 0.0):.3f} | "
            f"{row.get('dirty_recovered_vs_legacy_count') or 0} |"
        )
    lines.append("")

    lines.append("## 变化最大的点 (按 candidate 对 current 的绝对变化)")
    top_changes = sorted(
        [row for row in detail_rows if _safe_text(row.get("scenario_kind")) == "candidate"],
        key=lambda item: _safe_float(item.get("delta_abs_vs_current")) or 0.0,
        reverse=True,
    )[:10]
    if not top_changes:
        lines.append("- 无")
    else:
        for row in top_changes:
            lines.append(
                "- "
                f"{row.get('scenario')} / row={row.get('point_row')} / tag={row.get('point_tag') or row.get('point_key')} / "
                f"delta_abs_vs_current={(_safe_float(row.get('delta_abs_vs_current')) or 0.0):.3f} / "
                f"source={row.get('co2_source_selected') or 'missing'} / "
                f"mv_source={row.get('measured_value_source') or 'missing'} / "
                f"reason={row.get('co2_steady_window_reason') or row.get('co2_source_trust_reason') or 'n/a'}"
            )
    lines.append("")

    lines.append("## 失败/降级原因 Top")
    all_candidate_rows = [row for row in detail_rows if _safe_text(row.get("scenario_kind")) == "candidate"]
    lines.append(f"- source_switch_reason: {_reason_top_counts(all_candidate_rows, 'co2_source_switch_reason') or '无'}")
    lines.append(f"- trust_reason: {_reason_top_counts(all_candidate_rows, 'co2_source_trust_reason') or '无'}")
    lines.append(f"- steady_window_reason: {_reason_top_counts(all_candidate_rows, 'co2_steady_window_reason') or '无'}")
    lines.append("")

    lines.append("## 建议默认阈值")
    lines.append(f"- 建议情景：`{recommended_scenario or 'current_hardened_baseline'}`")
    lines.append(f"- 理由：{recommendation_reason}")
    if acceptable_ranges:
        lines.append("- 保守可接受区间：")
        for key, (lower, upper) in sorted(acceptable_ranges.items()):
            lines.append(f"  - `{key}`: `{lower}` ~ `{upper}`")
    else:
        lines.append("- 保守可接受区间：当前 replay 覆盖不足，仅建议先保持 current hardened baseline。")
    lines.append("")
    lines.append("## 还需要的 replay 数据")
    lines.append("- 更多包含过渡段/漂移段的 CO2 replay，而不是只有稳态夹具。")
    lines.append("- 更多 primary 失格并 fallback 到次级 source 的历史点。")
    lines.append("- 更多带中文列名且缺少部分字段的旧工件，用于验证 resilience 降级路径。")
    lines.append("")
    return "\n".join(lines)


def _details_rows_for_scenarios(
    bundles: Sequence[ReplayPoint],
    scenarios: Sequence[ThresholdScenario],
    base_runtime_cfg: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    detail_rows: List[Dict[str, Any]] = []
    legacy_by_point: Dict[str, Dict[str, Any]] = {}
    current_by_point: Dict[str, Dict[str, Any]] = {}

    legacy_runner = CalibrationRunner(deepcopy(dict(base_runtime_cfg or {})), {}, None, _no_op, _no_op)
    for bundle in bundles:
        legacy_result = _legacy_baseline_from_samples(legacy_runner, bundle.samples)
        legacy_row = {
            "scenario": "legacy_baseline",
            "scenario_kind": "legacy",
            "scenario_description": "旧口径 primary_or_first_usable_full_window_mean",
            "point_key": bundle.point_key,
            "point_row": bundle.point_row,
            "point_phase": bundle.point_phase,
            "point_tag": bundle.point_tag,
            "point_title": bundle.point_title,
            "temp_chamber_c": bundle.temp_chamber_c,
            "co2_ppm_target": bundle.co2_ppm_target,
            "pressure_target_hpa": bundle.pressure_target_hpa,
            "pressure_mode": bundle.pressure_mode,
            "measured_value": legacy_result.get("legacy_measured_value"),
            "measured_value_source": legacy_result.get("legacy_measured_value_source"),
            "co2_source_selected": legacy_result.get("legacy_source"),
            "co2_source_selected_for_value": legacy_result.get("legacy_source"),
            "co2_rows_before_quarantine": legacy_result.get("legacy_rows"),
            "co2_rows_after_quarantine": legacy_result.get("legacy_rows"),
            "co2_bad_frame_count": 0,
            "co2_bad_frame_ratio": 0.0,
            "co2_soft_warn_count": 0,
            "co2_soft_warn_ratio": 0.0,
            "co2_steady_window_found": None,
            "co2_steady_window_status": "legacy",
            "co2_steady_window_reason": "legacy_whole_window_mean",
            "co2_source_switch_reason": "",
            "co2_source_trust_reason": "legacy_primary_or_first_usable",
            "co2_quarantine_reason_summary": "",
            "point_quality_status": "legacy",
            "point_quality_reason": "",
            "point_quality_flags": "",
            "point_quality_blocked": False,
        }
        legacy_row["abs_error_vs_target"] = _abs_error(
            _safe_float(legacy_row.get("measured_value")),
            bundle.co2_ppm_target,
        )
        detail_rows.append(legacy_row)
        legacy_by_point[str(bundle.point_key)] = legacy_row

    for scenario in scenarios:
        runtime_cfg = _scenario_runtime_cfg(base_runtime_cfg, scenario)
        for bundle in bundles:
            row = _evaluate_hardened_point(bundle=bundle, runtime_cfg=runtime_cfg, scenario=scenario)
            legacy = legacy_by_point.get(str(bundle.point_key), {})
            row["delta_signed_vs_legacy"] = _delta(_safe_float(row.get("measured_value")), _safe_float(legacy.get("measured_value")))
            row["delta_abs_vs_legacy"] = _abs_delta(_safe_float(row.get("measured_value")), _safe_float(legacy.get("measured_value")))
            row["abs_error_vs_target"] = _abs_error(_safe_float(row.get("measured_value")), bundle.co2_ppm_target)
            row["abs_error_vs_target_legacy"] = _safe_float(legacy.get("abs_error_vs_target"))
            detail_rows.append(row)
            if scenario.name == "current_hardened_baseline":
                current_by_point[str(bundle.point_key)] = row

    for row in detail_rows:
        current = current_by_point.get(str(row.get("point_key")))
        row["delta_signed_vs_current"] = _delta(_safe_float(row.get("measured_value")), _safe_float((current or {}).get("measured_value")))
        row["delta_abs_vs_current"] = _abs_delta(_safe_float(row.get("measured_value")), _safe_float((current or {}).get("measured_value")))
        row["abs_error_vs_target_current"] = _safe_float((current or {}).get("abs_error_vs_target"))

    return detail_rows


def run_threshold_matrix_audit(
    *,
    samples_csvs: Sequence[Path],
    points_csvs: Sequence[Optional[Path]],
    runtime_cfgs: Sequence[Optional[Mapping[str, Any]]],
    output_dir: Path,
    config_path: Optional[str] = None,
    tiny_delta_ppm: float = DEFAULT_TINY_DELTA_PPM,
    meaningful_delta_ppm: float = DEFAULT_MEANINGFUL_DELTA_PPM,
) -> Dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    all_bundles: List[ReplayPoint] = []
    input_descriptions: List[str] = []
    base_runtime_cfg: Optional[Dict[str, Any]] = None

    for idx, samples_csv in enumerate(samples_csvs):
        runtime_cfg = _build_runtime_cfg(config_path=config_path, runtime_cfg=runtime_cfgs[idx] if idx < len(runtime_cfgs) else None)
        if base_runtime_cfg is None:
            base_runtime_cfg = runtime_cfg
        points_csv = points_csvs[idx] if idx < len(points_csvs) else None
        sample_rows = _canonicalize_samples(_read_csv_rows(samples_csv))
        point_rows = _read_csv_rows(points_csv) if points_csv else []
        bundles = _group_samples(sample_rows, _load_points_map(point_rows))
        all_bundles.extend(bundles)
        input_descriptions.append(str(samples_csv))
        if points_csv:
            input_descriptions.append(str(points_csv))

    base_runtime_cfg = base_runtime_cfg or load_config("configs/default_config.json")
    scenarios = _build_threshold_matrix(base_runtime_cfg)
    matrix_notes = _matrix_design_notes(_quality_cfg_from_runtime(base_runtime_cfg))
    detail_rows = _details_rows_for_scenarios(all_bundles, scenarios, base_runtime_cfg)

    current_rows = {str(row.get("point_key")): row for row in detail_rows if row.get("scenario") == "current_hardened_baseline"}
    summary_rows: List[Dict[str, Any]] = []
    all_scenarios = [ThresholdScenario("legacy_baseline", "legacy", "旧口径 primary_or_first_usable_full_window_mean", {})]
    all_scenarios.extend(scenarios)
    for scenario in all_scenarios:
        scenario_rows = [row for row in detail_rows if row.get("scenario") == scenario.name]
        summary_rows.append(
            _aggregate_scenario(
                scenario=scenario,
                rows=scenario_rows,
                current_baseline_rows=current_rows,
                tiny_delta_ppm=tiny_delta_ppm,
                meaningful_delta_ppm=meaningful_delta_ppm,
            )
        )

    recommended_scenario, recommendation_reason, acceptable_ranges = _recommend_default(summary_rows, scenarios)
    report_text = _format_markdown_report(
        inputs=input_descriptions,
        matrix_notes=matrix_notes,
        summary_rows=summary_rows,
        detail_rows=detail_rows,
        recommended_scenario=recommended_scenario,
        recommendation_reason=recommendation_reason,
        acceptable_ranges=acceptable_ranges,
    )

    summary_csv = output_dir / "summary.csv"
    details_csv = output_dir / "details.csv"
    summary_json = output_dir / "summary.json"
    report_md = output_dir / "report.md"
    _write_csv(summary_csv, summary_rows)
    _write_csv(details_csv, detail_rows)
    summary_json.write_text(
        json.dumps(
            {
                "tool": "audit_v1_co2_threshold_matrix",
                "evidence_source": "replay",
                "not_real_acceptance_evidence": True,
                "inputs": input_descriptions,
                "matrix_notes": matrix_notes,
                "summary": summary_rows,
                "recommended_scenario": recommended_scenario,
                "recommendation_reason": recommendation_reason,
                "acceptable_ranges": {key: [value[0], value[1]] for key, value in acceptable_ranges.items()},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    report_md.write_text(report_text, encoding="utf-8")
    return {
        "output_dir": output_dir,
        "summary_csv": summary_csv,
        "summary_json": summary_json,
        "report_md": report_md,
        "details_csv": details_csv,
        "recommended_scenario": recommended_scenario,
        "recommendation_reason": recommendation_reason,
    }


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="V1 CO2 阈值回放矩阵 / 敏感性审计（replay evidence only）",
    )
    parser.add_argument("--run-dir", action="append", default=[], help="completed V1 run directory")
    parser.add_argument("--samples-csv", action="append", default=[], help="explicit V1 samples.csv path")
    parser.add_argument("--points-csv", action="append", default=[], help="optional V1 points.csv / points_readable.csv path")
    parser.add_argument("--config", default=None, help="optional config path for fallback runtime config loading")
    parser.add_argument("--output-dir", default=None, help="audit output directory")
    parser.add_argument("--tiny-delta-ppm", type=float, default=DEFAULT_TINY_DELTA_PPM)
    parser.add_argument("--meaningful-delta-ppm", type=float, default=DEFAULT_MEANINGFUL_DELTA_PPM)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if not args.run_dir and not args.samples_csv:
        parser.error("at least one --run-dir or --samples-csv is required")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    samples_csvs: List[Path] = []
    points_csvs: List[Optional[Path]] = []
    runtime_cfgs: List[Optional[Mapping[str, Any]]] = []

    for run_dir_text in args.run_dir:
        run_dir = Path(run_dir_text)
        samples_csv, points_csv, runtime_cfg = _resolve_samples_points_from_run(run_dir)
        samples_csvs.append(samples_csv)
        points_csvs.append(points_csv)
        runtime_cfgs.append(runtime_cfg)

    explicit_points = [Path(path) for path in (args.points_csv or [])]
    for idx, sample_text in enumerate(args.samples_csv):
        samples_csvs.append(Path(sample_text))
        points_csvs.append(explicit_points[idx] if idx < len(explicit_points) else None)
        runtime_cfgs.append(None)

    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else Path("audit") / f"v1_co2_threshold_matrix_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    run_threshold_matrix_audit(
        samples_csvs=samples_csvs,
        points_csvs=points_csvs,
        runtime_cfgs=runtime_cfgs,
        output_dir=output_dir,
        config_path=args.config,
        tiny_delta_ppm=float(args.tiny_delta_ppm),
        meaningful_delta_ppm=float(args.meaningful_delta_ppm),
    )
    print(output_dir, flush=True)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
