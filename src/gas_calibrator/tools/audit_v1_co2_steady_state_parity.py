"""Replay/parity audit for the V1 CO2 steady-state representative-value change.

This is an offline evidence tool. It does not touch live devices, does not
change V1 runtime behavior, and only replays completed V1 exports to quantify
the difference between:

- legacy baseline: whole-window primary-or-first-usable mean
- new baseline: steady-state window representative value with trailing fallback
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from ..logging_utils import _field_label
from ..workflow.co2_steady_state_qc import (
    DEFAULT_CO2_STEADY_STATE_QC,
    evaluate_co2_steady_state_window_qc,
    legacy_co2_representative,
    normalize_co2_steady_state_qc_cfg,
)


REPORT_SUBDIR = Path("audit") / "v1_co2_steady_state_parity"
SUMMARY_CSV_NAME = "summary.csv"
SUMMARY_JSON_NAME = "summary.json"
REPORT_MD_NAME = "report.md"
TOP_N_DEFAULT = 10
MAX_ANALYZERS = 8


def _safe_float(value: Any) -> Optional[float]:
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _safe_int(value: Any) -> Optional[int]:
    try:
        return int(float(value))
    except Exception:
        return None


def _safe_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text in {"1", "true", "yes", "y", "on", "ok"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _safe_median(values: Sequence[float]) -> Optional[float]:
    vals = [float(item) for item in values if _safe_float(item) is not None]
    if not vals:
        return None
    return float(statistics.median(vals))


def _safe_mean(values: Sequence[float]) -> Optional[float]:
    vals = [float(item) for item in values if _safe_float(item) is not None]
    if not vals:
        return None
    return float(statistics.mean(vals))


def _latest_artifact(run_dir: Path, pattern: str) -> Optional[Path]:
    matches = [path for path in run_dir.glob(pattern) if path.is_file()]
    if not matches:
        return None
    return max(matches, key=lambda item: item.stat().st_mtime)


def _read_csv_rows(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    header: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in header:
                header.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(text), encoding="utf-8")


def _canonical_aliases(key: str) -> List[str]:
    aliases = [str(key)]
    translated = _field_label(str(key))
    if translated not in aliases:
        aliases.append(translated)
    return aliases


def _first_present(row: Mapping[str, Any], aliases: Sequence[str]) -> Any:
    for key in aliases:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _canonicalize_sample_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    base_keys = [
        "sample_ts",
        "sample_start_ts",
        "sample_end_ts",
        "sample_index",
        "point_row",
        "point_phase",
        "point_tag",
        "point_title",
        "co2_ppm",
        "co2_ppm_target",
        "pressure_target_hpa",
        "temp_chamber_c",
        "pressure_mode",
        "route",
        "frame_usable",
        "frame_status",
        "id",
    ]
    out: Dict[str, Any] = {}
    for key in base_keys:
        value = _first_present(row, _canonical_aliases(key))
        if value not in (None, ""):
            out[key] = value

    for idx in range(1, MAX_ANALYZERS + 1):
        prefix = f"ga{idx:02d}"
        for suffix in ("co2_ppm", "frame_usable", "frame_status", "id"):
            key = f"{prefix}_{suffix}"
            value = _first_present(row, _canonical_aliases(key))
            if value not in (None, ""):
                out[key] = value
    return out


def _canonicalize_point_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    keys = [
        "point_row",
        "point_phase",
        "point_tag",
        "point_title",
        "co2_ppm_target",
        "pressure_target_hpa",
        "temp_chamber_c",
        "pressure_mode",
        "route",
        "measured_value",
        "stable_flag",
        "device_id",
    ]
    out: Dict[str, Any] = {}
    for key in keys:
        value = _first_present(row, _canonical_aliases(key))
        if value not in (None, ""):
            out[key] = value
    return out


def _parse_point_sample_filename(path: Path) -> Tuple[Optional[int], str, str]:
    name = path.name
    match = re.match(r"^point_(\d+)(?:_([^_]+)(?:_(.*))?)?_samples\.csv$", name)
    if not match:
        return None, "", ""
    point_row = _safe_int(match.group(1))
    phase = str(match.group(2) or "").strip().lower()
    point_tag = str(match.group(3) or "").strip()
    return point_row, phase, point_tag


def _point_key(
    point_row: Optional[int],
    point_phase: Any,
    point_tag: Any,
) -> Tuple[Optional[int], str, str]:
    return (
        point_row,
        str(point_phase or "").strip().lower(),
        str(point_tag or "").strip(),
    )


def _load_point_metadata(run_dir: Path) -> Dict[Tuple[Optional[int], str, str], Dict[str, Any]]:
    path = _latest_artifact(run_dir, "points_readable_*.csv") or _latest_artifact(run_dir, "points_*.csv")
    if path is None:
        return {}
    rows = _read_csv_rows(path)
    out: Dict[Tuple[Optional[int], str, str], Dict[str, Any]] = {}
    for row in rows:
        payload = _canonicalize_point_row(row)
        key = _point_key(
            _safe_int(payload.get("point_row")),
            payload.get("point_phase"),
            payload.get("point_tag"),
        )
        out[key] = payload
        point_row = key[0]
        if point_row is not None:
            out.setdefault((point_row, key[1], ""), payload)
            out.setdefault((point_row, "", ""), payload)
    return out


def _load_point_sample_groups(run_dir: Path) -> Dict[Tuple[Optional[int], str, str], Dict[str, Any]]:
    groups: Dict[Tuple[Optional[int], str, str], Dict[str, Any]] = {}
    point_sample_files = sorted(run_dir.glob("point_*_samples.csv"))
    if point_sample_files:
        for path in point_sample_files:
            point_row, phase, point_tag = _parse_point_sample_filename(path)
            rows = [_canonicalize_sample_row(row) for row in _read_csv_rows(path)]
            if rows:
                for idx, row in enumerate(rows, start=1):
                    row.setdefault("sample_index", idx)
                    row.setdefault("point_row", point_row)
                    row.setdefault("point_phase", phase)
                    row.setdefault("point_tag", point_tag)
            groups[_point_key(point_row, phase, point_tag)] = {
                "sample_path": path,
                "samples": rows,
            }
        return groups

    samples_path = _latest_artifact(run_dir, "samples_*.csv")
    if samples_path is None:
        return {}
    grouped_rows: Dict[Tuple[Optional[int], str, str], List[Dict[str, Any]]] = defaultdict(list)
    for row in _read_csv_rows(samples_path):
        payload = _canonicalize_sample_row(row)
        key = _point_key(
            _safe_int(payload.get("point_row")),
            payload.get("point_phase"),
            payload.get("point_tag"),
        )
        grouped_rows[key].append(payload)
    for key, rows in grouped_rows.items():
        for idx, row in enumerate(rows, start=1):
            row.setdefault("sample_index", idx)
        groups[key] = {
            "sample_path": samples_path,
            "samples": rows,
        }
    return groups


def _choose_metadata(
    metadata_by_key: Mapping[Tuple[Optional[int], str, str], Dict[str, Any]],
    *,
    point_row: Optional[int],
    point_phase: str,
    point_tag: str,
) -> Dict[str, Any]:
    for candidate in (
        (point_row, point_phase, point_tag),
        (point_row, point_phase, ""),
        (point_row, "", ""),
    ):
        if candidate in metadata_by_key:
            return dict(metadata_by_key[candidate])
    return {}


def _summarize_reason(reason: str) -> str:
    text = str(reason or "").strip()
    if not text:
        return ""
    return text.split(";")[0].strip()


def _route_bucket(payload: Mapping[str, Any]) -> str:
    route = str(payload.get("route") or "").strip()
    if route:
        return route
    phase = str(payload.get("point_phase") or "").strip().lower()
    if phase:
        return phase
    return "unknown"


def _point_type_bucket(payload: Mapping[str, Any]) -> str:
    tag = str(payload.get("point_tag") or "").strip()
    if not tag:
        return "unknown"
    if "ambient" in tag:
        return "ambient"
    if "hpa" in tag:
        return "pressurized"
    return "other"


def _build_point_summary_row(
    run_dir: Path,
    *,
    point_key_value: Tuple[Optional[int], str, str],
    sample_path: Path,
    samples: Sequence[Mapping[str, Any]],
    metadata: Mapping[str, Any],
    qc_cfg: Mapping[str, Any],
) -> Dict[str, Any]:
    point_row, point_phase, point_tag = point_key_value
    legacy = legacy_co2_representative(samples)
    steady = evaluate_co2_steady_state_window_qc(samples, phase=point_phase or "co2", qc_cfg=qc_cfg)

    degraded_notes: List[str] = []
    if str(steady.get("co2_steady_window_timestamp_strategy") or "") == "row_index_fallback":
        degraded_notes.append("sample_ts_missing=row_index_fallback")
    if not metadata:
        degraded_notes.append("point_metadata_missing")

    delta_signed = None
    delta_abs = None
    legacy_value = _safe_float(legacy.get("legacy_representative_value"))
    new_value = _safe_float(steady.get("co2_representative_value"))
    if legacy_value is not None and new_value is not None:
        delta_signed = round(new_value - legacy_value, 6)
        delta_abs = round(abs(delta_signed), 6)

    steady_status = str(steady.get("co2_steady_window_status") or "").strip().lower()
    measured_value_source = str(steady.get("measured_value_source") or "").strip()
    if legacy_value is None or new_value is None:
        audit_status = "fail"
    elif measured_value_source == "co2_trailing_window_fallback":
        audit_status = "degraded"
    elif degraded_notes:
        audit_status = "warn"
    elif steady_status == "pass":
        audit_status = "pass"
    elif steady_status:
        audit_status = steady_status
    else:
        audit_status = "warn"

    point_title = str(metadata.get("point_title") or "").strip()
    if not point_title and samples:
        point_title = str(samples[0].get("point_title") or "").strip()

    row = {
        "run_dir": str(run_dir),
        "sample_file": str(sample_path),
        "point_row": point_row,
        "point_phase": point_phase,
        "point_tag": point_tag,
        "point_title": point_title,
        "co2_ppm_target": _safe_float(metadata.get("co2_ppm_target") or (samples[0].get("co2_ppm_target") if samples else None)),
        "pressure_target_hpa": _safe_float(metadata.get("pressure_target_hpa") or (samples[0].get("pressure_target_hpa") if samples else None)),
        "temp_chamber_c": _safe_float(metadata.get("temp_chamber_c") or (samples[0].get("temp_chamber_c") if samples else None)),
        "pressure_mode": str(metadata.get("pressure_mode") or "").strip(),
        "route": _route_bucket({"route": metadata.get("route"), "point_phase": point_phase}),
        "point_type": _point_type_bucket({"point_tag": point_tag}),
        "legacy_representative_value": legacy_value,
        "legacy_value_source": legacy.get("legacy_value_source"),
        "legacy_analyzer_source": legacy.get("legacy_analyzer_source"),
        "legacy_value_key": legacy.get("legacy_value_key"),
        "legacy_sample_count": legacy.get("legacy_sample_count"),
        "new_representative_value": new_value,
        "delta_signed": delta_signed,
        "delta_abs": delta_abs,
        "measured_value_source": measured_value_source,
        "co2_steady_window_found": steady.get("co2_steady_window_found"),
        "co2_steady_window_status": steady.get("co2_steady_window_status"),
        "co2_steady_window_reason": steady.get("co2_steady_window_reason"),
        "co2_steady_window_analyzer_source": steady.get("co2_steady_window_analyzer_source"),
        "co2_steady_window_value_key": steady.get("co2_steady_window_value_key"),
        "co2_steady_window_candidate_count": steady.get("co2_steady_window_candidate_count"),
        "co2_steady_window_start_sample_index": steady.get("co2_steady_window_start_sample_index"),
        "co2_steady_window_end_sample_index": steady.get("co2_steady_window_end_sample_index"),
        "co2_steady_window_start_ts": steady.get("co2_steady_window_start_ts"),
        "co2_steady_window_end_ts": steady.get("co2_steady_window_end_ts"),
        "co2_steady_window_sample_count": steady.get("co2_steady_window_sample_count"),
        "co2_steady_window_mean_ppm": steady.get("co2_steady_window_mean_ppm"),
        "co2_steady_window_std_ppm": steady.get("co2_steady_window_std_ppm"),
        "co2_steady_window_range_ppm": steady.get("co2_steady_window_range_ppm"),
        "co2_steady_window_slope_ppm_per_s": steady.get("co2_steady_window_slope_ppm_per_s"),
        "co2_steady_window_timestamp_strategy": steady.get("co2_steady_window_timestamp_strategy"),
        "audit_status": audit_status,
        "degraded_reason": ";".join(degraded_notes),
        "primary_reason": _summarize_reason(str(steady.get("co2_steady_window_reason") or "")),
        "evidence_source": "replay",
        "not_real_acceptance_evidence": True,
    }
    return row


def _bucket_summary(rows: Sequence[Mapping[str, Any]], key: str) -> List[Dict[str, Any]]:
    buckets: Dict[str, List[float]] = defaultdict(list)
    for row in rows:
        bucket = row.get(key)
        if bucket in (None, ""):
            continue
        delta_abs = _safe_float(row.get("delta_abs"))
        if delta_abs is None:
            continue
        buckets[str(bucket)].append(delta_abs)
    summary_rows: List[Dict[str, Any]] = []
    for bucket, deltas in sorted(buckets.items(), key=lambda item: item[0]):
        summary_rows.append(
            {
                "bucket": bucket,
                "count": len(deltas),
                "avg_abs_delta": round(_safe_mean(deltas) or 0.0, 6),
                "median_abs_delta": round(_safe_median(deltas) or 0.0, 6),
                "max_abs_delta": round(max(deltas), 6),
            }
        )
    return summary_rows


def _aggregate_summary(
    point_rows: Sequence[Mapping[str, Any]],
    *,
    qc_cfg: Mapping[str, Any],
    top_n: int,
) -> Dict[str, Any]:
    total_points = len(point_rows)
    status_counts = Counter(str(row.get("audit_status") or "unknown") for row in point_rows)
    steady_found = sum(1 for row in point_rows if row.get("co2_steady_window_found") is True)
    fallback_count = sum(1 for row in point_rows if row.get("measured_value_source") == "co2_trailing_window_fallback")
    delta_values = [
        float(row["delta_abs"])
        for row in point_rows
        if _safe_float(row.get("delta_abs")) is not None
    ]
    top_rows = sorted(
        point_rows,
        key=lambda row: (_safe_float(row.get("delta_abs")) or -1.0, _safe_int(row.get("point_row")) or 0),
        reverse=True,
    )[: max(1, int(top_n))]
    reason_counts = Counter(
        str(row.get("primary_reason") or row.get("degraded_reason") or "none")
        for row in point_rows
    )
    return {
        "tool": "v1_co2_steady_state_parity_audit",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "evidence_source": "replay",
        "not_real_acceptance_evidence": True,
        "legacy_value_basis": "primary_or_first_usable_full_window_mean",
        "steady_state_value_basis": "steady_state_window_or_trailing_fallback",
        "qc_config": dict(qc_cfg),
        "total_points": total_points,
        "status_counts": dict(status_counts),
        "steady_state_window_hit_rate": (steady_found / total_points) if total_points else 0.0,
        "trailing_fallback_ratio": (fallback_count / total_points) if total_points else 0.0,
        "max_abs_delta": max(delta_values) if delta_values else None,
        "avg_abs_delta": _safe_mean(delta_values),
        "median_abs_delta": _safe_median(delta_values),
        "top_changed_points": [dict(row) for row in top_rows],
        "reason_counts": dict(reason_counts),
        "bucket_summaries": {
            "co2_ppm_target": _bucket_summary(point_rows, "co2_ppm_target"),
            "pressure_target_hpa": _bucket_summary(point_rows, "pressure_target_hpa"),
            "temp_chamber_c": _bucket_summary(point_rows, "temp_chamber_c"),
            "analyzer_source": _bucket_summary(point_rows, "co2_steady_window_analyzer_source"),
            "point_type": _bucket_summary(point_rows, "point_type"),
            "route": _bucket_summary(point_rows, "route"),
        },
    }


def _markdown_table(rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> str:
    if not rows:
        return "_无数据_"
    header = "| " + " | ".join(columns) + " |"
    sep = "| " + " | ".join("---" for _ in columns) + " |"
    body = []
    for row in rows:
        body.append("| " + " | ".join(str(row.get(col, "")) for col in columns) + " |")
    return "\n".join([header, sep, *body])


def _render_report(summary: Mapping[str, Any]) -> str:
    top_rows = list(summary.get("top_changed_points") or [])
    status_counts = dict(summary.get("status_counts") or {})
    reason_counts = dict(summary.get("reason_counts") or {})
    lines = [
        "# V1 CO2 稳态窗 replay/parity 审计报告",
        "",
        "- replay evidence only",
        "- not real acceptance evidence",
        f"- 旧口径基线：`{summary.get('legacy_value_basis')}`",
        f"- 新口径基线：`{summary.get('steady_state_value_basis')}`",
        "",
        "## 总览",
        "",
        f"- 总点数：`{summary.get('total_points', 0)}`",
        f"- 稳态窗命中率：`{float(summary.get('steady_state_window_hit_rate') or 0.0):.2%}`",
        f"- trailing fallback 比例：`{float(summary.get('trailing_fallback_ratio') or 0.0):.2%}`",
        f"- 最大绝对变化：`{summary.get('max_abs_delta')}`",
        f"- 平均绝对变化：`{summary.get('avg_abs_delta')}`",
        f"- 中位数绝对变化：`{summary.get('median_abs_delta')}`",
        "",
        "## 状态分布",
        "",
        _markdown_table(
            [{"status": key, "count": value} for key, value in sorted(status_counts.items())],
            ["status", "count"],
        ),
        "",
        "## 变化最大的点",
        "",
        _markdown_table(
            top_rows,
            [
                "run_dir",
                "point_row",
                "point_tag",
                "co2_ppm_target",
                "legacy_representative_value",
                "new_representative_value",
                "delta_abs",
                "measured_value_source",
                "co2_steady_window_status",
            ],
        ),
        "",
        "## 失败/降级原因计数",
        "",
        _markdown_table(
            [{"reason": key, "count": value} for key, value in sorted(reason_counts.items())],
            ["reason", "count"],
        ),
    ]
    bucket_summaries = dict(summary.get("bucket_summaries") or {})
    for bucket_name, rows in bucket_summaries.items():
        lines.extend(
            [
                "",
                f"## 分桶差异：{bucket_name}",
                "",
                _markdown_table(rows, ["bucket", "count", "avg_abs_delta", "median_abs_delta", "max_abs_delta"]),
            ]
        )
    lines.append("")
    return "\n".join(lines)


def run_v1_co2_steady_state_parity_audit(
    *,
    run_dirs: Sequence[str | Path],
    output_dir: str | Path,
    qc_cfg: Optional[Mapping[str, Any]] = None,
    top_n: int = TOP_N_DEFAULT,
) -> Dict[str, Any]:
    qc_payload = normalize_co2_steady_state_qc_cfg(qc_cfg)
    point_rows: List[Dict[str, Any]] = []
    for run_dir_raw in run_dirs:
        run_dir = Path(run_dir_raw).resolve()
        metadata_by_key = _load_point_metadata(run_dir)
        sample_groups = _load_point_sample_groups(run_dir)
        for point_key_value, payload in sorted(
            sample_groups.items(),
            key=lambda item: ((_safe_int(item[0][0]) or 0), str(item[0][1]), str(item[0][2])),
        ):
            point_row, point_phase, point_tag = point_key_value
            if str(point_phase or "").strip().lower() != "co2":
                continue
            samples = list(payload.get("samples") or [])
            metadata = _choose_metadata(
                metadata_by_key,
                point_row=point_row,
                point_phase=point_phase,
                point_tag=point_tag,
            )
            point_rows.append(
                _build_point_summary_row(
                    run_dir,
                    point_key_value=point_key_value,
                    sample_path=Path(payload["sample_path"]),
                    samples=samples,
                    metadata=metadata,
                    qc_cfg=qc_payload,
                )
            )

    output_path = Path(output_dir).resolve()
    output_path.mkdir(parents=True, exist_ok=True)
    summary_csv = output_path / SUMMARY_CSV_NAME
    summary_json = output_path / SUMMARY_JSON_NAME
    report_md = output_path / REPORT_MD_NAME

    _write_csv(summary_csv, point_rows)
    aggregate = _aggregate_summary(point_rows, qc_cfg=qc_payload, top_n=top_n)
    aggregate["run_dirs"] = [str(Path(item).resolve()) for item in run_dirs]
    aggregate["summary_csv"] = str(summary_csv)
    aggregate["summary_json"] = str(summary_json)
    aggregate["report_md"] = str(report_md)
    _write_json(summary_json, aggregate)
    _write_text(report_md, _render_report(aggregate))
    return aggregate


def _default_output_dir(run_dirs: Sequence[Path]) -> Path:
    if len(run_dirs) == 1:
        return run_dirs[0] / REPORT_SUBDIR
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path.cwd() / "audit" / f"v1_co2_steady_state_parity_{stamp}"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit V1 CO2 legacy whole-window mean vs steady-state representative value on replay exports.",
    )
    parser.add_argument(
        "--run-dir",
        action="append",
        required=True,
        help="Completed V1 run directory. Can be provided multiple times.",
    )
    parser.add_argument(
        "--output-dir",
        default="",
        help="Output directory for audit artifacts. Defaults to <run_dir>/audit/v1_co2_steady_state_parity for one run.",
    )
    parser.add_argument("--top-n", type=int, default=TOP_N_DEFAULT, help="Top changed points to include in report.")
    parser.add_argument("--policy", default="", help="Override steady-state audit policy: off/warn/reject.")
    parser.add_argument("--min-samples", type=int, default=0, help="Override minimum steady-state window samples.")
    parser.add_argument("--fallback-samples", type=int, default=0, help="Override trailing fallback sample count.")
    parser.add_argument("--max-std-ppm", type=float, default=math.nan, help="Override max steady-state std.")
    parser.add_argument("--max-range-ppm", type=float, default=math.nan, help="Override max steady-state range.")
    parser.add_argument(
        "--max-abs-slope-ppm-per-s",
        type=float,
        default=math.nan,
        help="Override max steady-state absolute slope.",
    )
    return parser


def _cli_qc_cfg(args: argparse.Namespace) -> Dict[str, Any]:
    cfg = dict(DEFAULT_CO2_STEADY_STATE_QC)
    if str(args.policy or "").strip():
        cfg["policy"] = str(args.policy).strip()
    if int(args.min_samples or 0) > 0:
        cfg["min_samples"] = int(args.min_samples)
    if int(args.fallback_samples or 0) > 0:
        cfg["fallback_samples"] = int(args.fallback_samples)
    if math.isfinite(float(args.max_std_ppm)):
        cfg["max_std_ppm"] = float(args.max_std_ppm)
    if math.isfinite(float(args.max_range_ppm)):
        cfg["max_range_ppm"] = float(args.max_range_ppm)
    if math.isfinite(float(args.max_abs_slope_ppm_per_s)):
        cfg["max_abs_slope_ppm_per_s"] = float(args.max_abs_slope_ppm_per_s)
    return cfg


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    run_dirs = [Path(item).resolve() for item in args.run_dir]
    output_dir = Path(args.output_dir).resolve() if str(args.output_dir or "").strip() else _default_output_dir(run_dirs)
    result = run_v1_co2_steady_state_parity_audit(
        run_dirs=run_dirs,
        output_dir=output_dir,
        qc_cfg=_cli_qc_cfg(args),
        top_n=max(1, int(args.top_n)),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
