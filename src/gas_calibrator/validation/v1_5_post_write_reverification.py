"""Offline post-write reverification review for V1.5.

The review consumes already-recorded open-flow verification summaries. It does
not open COM ports, change routes, or write SENCO values. Its job is to turn
post-write check points into auditable evidence before database import and
formal reports.
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SCHEMA = "v1_5_post_write_reverification_review_v1"


@dataclass(frozen=True)
class VerificationLimits:
    co2_relative_pct: float = 1.5
    h2o_relative_pct: float = 2.0
    co2_zero_abs_ppm: float = 5.0
    h2o_dry_abs_mmol_mol: float = 0.5


@dataclass(frozen=True)
class VerificationPointResult:
    component: str
    device_id: str
    point_id: str
    standard_value: float | None
    measured_value: float | None
    unit: str
    error: float | None
    error_pct: float | None
    limit_value: float | None
    limit_basis: str
    status: str
    reason: str
    source_file: str
    sample_role: str = "post_write_verification"
    coefficient_epoch: str = ""

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DeviceComponentSummary:
    component: str
    device_id: str
    point_count: int
    pass_count: int
    fail_count: int
    not_evaluated_count: int
    max_abs_error: float | None
    max_abs_error_pct: float | None
    status: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PostWriteReverificationReview:
    schema: str
    created_at: str
    overall_status: str
    limits: Mapping[str, Any]
    source_files: tuple[str, ...]
    write_event_files: tuple[str, ...]
    coefficient_snapshot_files: tuple[str, ...]
    point_results: tuple[VerificationPointResult, ...]
    device_component_summary: tuple[DeviceComponentSummary, ...]
    warnings: tuple[str, ...] = field(default_factory=tuple)

    def to_json(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "created_at": self.created_at,
            "overall_status": self.overall_status,
            "limits": dict(self.limits),
            "source_files": list(self.source_files),
            "write_event_files": list(self.write_event_files),
            "coefficient_snapshot_files": list(self.coefficient_snapshot_files),
            "point_results": [row.to_json() for row in self.point_results],
            "device_component_summary": [row.to_json() for row in self.device_component_summary],
            "warnings": list(self.warnings),
        }


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


def _norm_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_device_id(value: Any) -> str:
    text = _norm_text(value).upper()
    if text.startswith("GA"):
        text = text[2:]
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _first_value(row: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _component_from_row(row: Mapping[str, Any], fallback: str) -> str:
    value = _norm_text(_first_value(row, ("component", "Component", "route", "point_component")))
    if value:
        lower = value.lower()
        if "co2" in lower or "二氧化碳" in value:
            return "co2"
        if "h2o" in lower or "water" in lower or "水" in value:
            return "h2o"
    return fallback.lower()


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _infer_component_from_path(path: Path) -> str:
    lower = path.name.lower()
    if "h2o" in lower or "water" in lower or "dewpoint" in lower:
        return "h2o"
    return "co2"


def _point_id(row: Mapping[str, Any], source_file: Path, index: int) -> str:
    value = _first_value(
        row,
        (
            "point_id",
            "point_run_id",
            "point",
            "target_id",
            "co2_group",
            "h2o_group",
            "source_nominal_ppm",
            "certificate_co2_ppm",
        ),
    )
    return _norm_text(value) or f"{source_file.stem}:{index}"


def _standard_and_measured(row: Mapping[str, Any], component: str) -> tuple[float | None, float | None, str]:
    if component == "h2o":
        standard = _safe_float(
            _first_value(
                row,
                (
                    "standard_value",
                    "reference_h2o_mmol_mol",
                    "certificate_h2o_mmol_mol",
                    "target_h2o_mmol_mol",
                    "h2o_reference_mmol_mol",
                    "dewpoint_reference_h2o_mmol_mol",
                ),
            )
        )
        measured = _safe_float(
            _first_value(
                row,
                (
                    "measured_value",
                    "measured_h2o_mmol_mol",
                    "h2o_mmol_mol",
                    "h2o_mmol",
                    "ppm_H2O",
                ),
            )
        )
        return standard, measured, "mmol/mol"
    standard = _safe_float(
        _first_value(
            row,
            (
                "standard_value",
                "certificate_co2_ppm",
                "ppm_CO2_Tank",
                "target_ppm",
                "source_nominal_ppm",
                "co2_reference_ppm",
            ),
        )
    )
    measured = _safe_float(
        _first_value(
            row,
            (
                "measured_value",
                "measured_co2_ppm",
                "ppm_CO2",
                "co2_ppm",
                "displayed_co2_ppm",
            ),
        )
    )
    return standard, measured, "ppm"


def _limit_for(component: str, standard: float | None, limits: VerificationLimits) -> tuple[float | None, str]:
    if standard is None:
        return None, "missing_standard"
    if component == "h2o":
        if abs(standard) <= limits.h2o_dry_abs_mmol_mol:
            return limits.h2o_dry_abs_mmol_mol, "h2o_dry_absolute_mmol_mol"
        return abs(standard) * limits.h2o_relative_pct / 100.0, "h2o_relative_pct"
    if abs(standard) <= limits.co2_zero_abs_ppm:
        return limits.co2_zero_abs_ppm, "co2_zero_absolute_ppm"
    return abs(standard) * limits.co2_relative_pct / 100.0, "co2_relative_pct"


def _classify(
    *,
    component: str,
    standard: float | None,
    measured: float | None,
    limits: VerificationLimits,
) -> tuple[float | None, float | None, float | None, str, str, str]:
    if standard is None or measured is None:
        limit, basis = _limit_for(component, standard, limits)
        return None, None, limit, basis, "not_evaluated", "missing_standard_or_measured_value"
    error = measured - standard
    error_pct = None if standard == 0 else error / standard * 100.0
    limit, basis = _limit_for(component, standard, limits)
    if limit is None:
        return error, error_pct, limit, basis, "not_evaluated", "missing_limit"
    status = "pass" if abs(error) <= limit else "fail"
    reason = "within_limit" if status == "pass" else "error_exceeds_limit"
    return error, error_pct, limit, basis, status, reason


def build_post_write_reverification_review(
    *,
    verification_csvs: Sequence[str | Path],
    limits: VerificationLimits = VerificationLimits(),
    write_event_files: Sequence[str | Path] = (),
    coefficient_snapshot_files: Sequence[str | Path] = (),
    coefficient_epoch: str = "",
) -> PostWriteReverificationReview:
    """Build an offline post-write reverification report from CSV evidence."""

    results: list[VerificationPointResult] = []
    warnings: list[str] = []
    for csv_path in verification_csvs:
        path = Path(csv_path).resolve()
        if not path.exists():
            warnings.append(f"missing_verification_csv:{path}")
            continue
        fallback_component = _infer_component_from_path(path)
        for index, row in enumerate(_read_csv(path), start=1):
            component = _component_from_row(row, fallback_component)
            device_id = _normalize_device_id(
                _first_value(row, ("device_id", "analyzer_device_id", "DeviceID", "Analyzer", "analyzer_label"))
            )
            standard, measured, unit = _standard_and_measured(row, component)
            error, error_pct, limit, limit_basis, status, reason = _classify(
                component=component,
                standard=standard,
                measured=measured,
                limits=limits,
            )
            results.append(
                VerificationPointResult(
                    component=component,
                    device_id=device_id,
                    point_id=_point_id(row, path, index),
                    standard_value=standard,
                    measured_value=measured,
                    unit=unit,
                    error=error,
                    error_pct=error_pct,
                    limit_value=limit,
                    limit_basis=limit_basis,
                    status=status,
                    reason=reason,
                    source_file=str(path),
                    sample_role=_norm_text(row.get("sample_role")) or "post_write_verification",
                    coefficient_epoch=coefficient_epoch or _norm_text(row.get("coefficient_epoch")),
                )
            )

    grouped: dict[tuple[str, str], list[VerificationPointResult]] = {}
    for row in results:
        grouped.setdefault((row.component, row.device_id), []).append(row)

    summaries: list[DeviceComponentSummary] = []
    for (component, device_id), items in sorted(grouped.items()):
        fails = [row for row in items if row.status == "fail"]
        passes = [row for row in items if row.status == "pass"]
        unknown = [row for row in items if row.status == "not_evaluated"]
        abs_errors = [abs(row.error) for row in items if row.error is not None]
        abs_pct_errors = [abs(row.error_pct) for row in items if row.error_pct is not None]
        if fails:
            status = "fail"
        elif unknown:
            status = "not_evaluated"
        elif passes:
            status = "pass"
        else:
            status = "not_evaluated"
        summaries.append(
            DeviceComponentSummary(
                component=component,
                device_id=device_id,
                point_count=len(items),
                pass_count=len(passes),
                fail_count=len(fails),
                not_evaluated_count=len(unknown),
                max_abs_error=max(abs_errors) if abs_errors else None,
                max_abs_error_pct=max(abs_pct_errors) if abs_pct_errors else None,
                status=status,
            )
        )

    if any(row.status == "fail" for row in summaries):
        overall = "fail"
    elif not results or any(row.status == "not_evaluated" for row in summaries):
        overall = "not_evaluated"
    else:
        overall = "pass"

    return PostWriteReverificationReview(
        schema=SCHEMA,
        created_at=datetime.now().isoformat(timespec="seconds"),
        overall_status=overall,
        limits=asdict(limits),
        source_files=tuple(str(Path(path).resolve()) for path in verification_csvs),
        write_event_files=tuple(str(Path(path).resolve()) for path in write_event_files),
        coefficient_snapshot_files=tuple(str(Path(path).resolve()) for path in coefficient_snapshot_files),
        point_results=tuple(results),
        device_component_summary=tuple(summaries),
        warnings=tuple(warnings),
    )


def _fmt(value: Any, digits: int = 3) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def write_post_write_reverification_outputs(
    review: PostWriteReverificationReview,
    output_dir: str | Path,
) -> dict[str, Path]:
    """Write JSON, CSV, and Chinese Markdown review artifacts."""

    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "post_write_reverification_review.json"
    points_csv = root / "post_write_reverification_points.csv"
    summary_csv = root / "post_write_reverification_device_summary.csv"
    md_path = root / "post_write_reverification_review.md"

    json_path.write_text(json.dumps(review.to_json(), ensure_ascii=False, indent=2), encoding="utf-8")

    point_rows = [row.to_json() for row in review.point_results]
    summary_rows = [row.to_json() for row in review.device_component_summary]
    _write_csv(points_csv, point_rows)
    _write_csv(summary_csv, summary_rows)

    lines = [
        "# V1.5 写后复验评审",
        "",
        f"- 总体状态：`{review.overall_status}`",
        f"- 复验点文件数：`{len(review.source_files)}`",
        f"- 写入事件文件数：`{len(review.write_event_files)}`",
        f"- 系数快照文件数：`{len(review.coefficient_snapshot_files)}`",
        "",
        "## 物理意义",
        "",
        "- SENCO 写入改变的是分析仪内部测量模型，不能只凭写入成功或 GETCO 读回就发布结果。",
        "- 写后复验必须使用开放流通的独立复验点，确认更新后的 CO2/H2O 输出能回到标准气或露点参考约束范围内。",
        "- 本评审只处理已经采集完成的数据，不打开串口、不控制气路/水路、不写设备。",
        "",
        "## 设备-组分摘要",
        "",
        "| 设备ID | 组分 | 点数 | 通过 | 失败 | 未评估 | 最大绝对误差 | 最大相对误差% | 状态 |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in review.device_component_summary:
        lines.append(
            "| {device} | {component} | {n} | {passed} | {failed} | {unknown} | {max_abs} | {max_pct} | {status} |".format(
                device=row.device_id,
                component=row.component.upper(),
                n=row.point_count,
                passed=row.pass_count,
                failed=row.fail_count,
                unknown=row.not_evaluated_count,
                max_abs=_fmt(row.max_abs_error),
                max_pct=_fmt(row.max_abs_error_pct),
                status=row.status,
            )
        )
    lines.extend(
        [
            "",
            "## 点位明细",
            "",
            "| 设备ID | 组分 | 点位 | 标准值 | 测量值 | 误差 | 相对误差% | 限值 | 限值依据 | 状态 | 原因 |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |",
        ]
    )
    for row in review.point_results:
        lines.append(
            "| {device} | {component} | {point} | {standard} | {measured} | {error} | {error_pct} | {limit} | {basis} | {status} | {reason} |".format(
                device=row.device_id,
                component=row.component.upper(),
                point=row.point_id,
                standard=_fmt(row.standard_value),
                measured=_fmt(row.measured_value),
                error=_fmt(row.error),
                error_pct=_fmt(row.error_pct),
                limit=_fmt(row.limit_value),
                basis=row.limit_basis,
                status=row.status,
                reason=row.reason,
            )
        )
    if review.warnings:
        lines.extend(["", "## 警告", ""])
        for warning in review.warnings:
            lines.append(f"- {warning}")

    md_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return {
        "json": json_path,
        "points_csv": points_csv,
        "summary_csv": summary_csv,
        "markdown": md_path,
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
