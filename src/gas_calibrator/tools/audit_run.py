"""Offline audit helpers for completed run directories."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from ..config import load_config
from ..data.points import CalibrationPoint, load_points_from_excel, reorder_points
from ..logging_utils import _field_label
from ..workflow.runner import CalibrationRunner

POINT_INTEGRITY_LABEL = _field_label("analyzer_integrity")
EXPECTED_ANALYZERS_LABEL = _field_label("analyzer_expected_count")


@dataclass(frozen=True)
class PlannedPointSpec:
    """Expected output for one planned calibration point."""

    point_row: int
    phase: str
    point_tag: str
    title: str = ""

    @property
    def sample_filename(self) -> str:
        suffix = f"_{self.phase}_{self.point_tag}" if self.point_tag else f"_{self.phase}"
        return f"point_{self.point_row:04d}{suffix}_samples.csv"


@dataclass
class AuditResult:
    """Structured audit outcome for one run directory."""

    run_dir: Path
    planned_points: List[PlannedPointSpec] = field(default_factory=list)
    analyzer_labels: List[str] = field(default_factory=list)
    artifacts: Dict[str, Optional[Path]] = field(default_factory=dict)
    failures: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    infos: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failures


def _no_op(*_args: Any, **_kwargs: Any) -> None:
    return None


def _latest_artifact(run_dir: Path, pattern: str) -> Optional[Path]:
    matches = [path for path in run_dir.glob(pattern) if path.is_file()]
    if not matches:
        return None
    return max(matches, key=lambda path: path.stat().st_mtime)


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def _csv_row_count(path: Optional[Path]) -> int:
    if path is None or not path.exists():
        return 0
    return len(_read_csv_rows(path))


def _safe_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def _sample_row_count(path: Path) -> int:
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def _load_runtime_cfg(
    run_dir: Path,
    *,
    runtime_cfg: Optional[Dict[str, Any]] = None,
    config_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if isinstance(runtime_cfg, dict):
        return runtime_cfg

    snapshot_path = run_dir / "runtime_config_snapshot.json"
    if snapshot_path.exists():
        try:
            return json.loads(snapshot_path.read_text(encoding="utf-8"))
        except Exception:
            return None

    if config_path:
        try:
            return load_config(config_path)
        except Exception:
            return None
    return None


def _configured_analyzer_labels(runtime_cfg: Optional[Dict[str, Any]]) -> List[str]:
    if not isinstance(runtime_cfg, dict):
        return []

    devices_cfg = runtime_cfg.get("devices", {})
    if not isinstance(devices_cfg, dict):
        return []

    labels: List[str] = []
    multi_cfg = devices_cfg.get("gas_analyzers", [])
    if isinstance(multi_cfg, list) and multi_cfg:
        for idx, item in enumerate(multi_cfg, start=1):
            if not isinstance(item, dict) or not item.get("enabled", True):
                continue
            labels.append(str(item.get("name") or f"ga{idx:02d}"))
        return labels

    single_cfg = devices_cfg.get("gas_analyzer", {})
    if isinstance(single_cfg, dict) and single_cfg.get("enabled", False):
        return ["ga01"]
    return []


def _infer_expected_analyzer_count(
    *,
    points_csv: Optional[Path],
    points_readable_csv: Optional[Path],
    configured_count: int,
) -> tuple[int, str]:
    candidate_keys = (
        EXPECTED_ANALYZERS_LABEL,
        "ExpectedAnalyzers",
        "analyzer_expected_count",
    )
    values: List[int] = []
    for path in (points_readable_csv, points_csv):
        if path is None or not path.exists():
            continue
        try:
            rows = _read_csv_rows(path)
        except Exception:
            continue
        for row in rows:
            for key in candidate_keys:
                parsed = _safe_int(row.get(key))
                if parsed is not None and parsed > 0:
                    values.append(parsed)
                    break
    if values:
        return max(values), "point_exports"
    return max(0, configured_count), "runtime_config"


def plan_points_from_runtime_config(runtime_cfg: Optional[Dict[str, Any]]) -> List[PlannedPointSpec]:
    if not isinstance(runtime_cfg, dict):
        return []

    paths_cfg = runtime_cfg.get("paths", {})
    if not isinstance(paths_cfg, dict):
        return []
    points_path = paths_cfg.get("points_excel")
    if not points_path:
        return []

    workflow_cfg = runtime_cfg.get("workflow", {})
    if not isinstance(workflow_cfg, dict):
        workflow_cfg = {}

    points = load_points_from_excel(
        points_path,
        missing_pressure_policy=str(workflow_cfg.get("missing_pressure_policy", "require") or "require"),
        carry_forward_h2o=bool(workflow_cfg.get("h2o_carry_forward", False)),
    )
    ordered_points = reorder_points(
        list(points),
        0.0,
        descending_temperatures=bool(workflow_cfg.get("temperature_descending", True)),
    )
    runner = CalibrationRunner(
        runtime_cfg,
        {},
        None,
        _no_op,
        _no_op,
    )
    filtered_points = runner._filter_selected_temperatures(ordered_points)
    route_mode = runner._route_mode()
    planned: List[PlannedPointSpec] = []

    for temp_group in runner._group_points_by_temperature(filtered_points):
        if not temp_group:
            continue
        temp_value = getattr(temp_group[0], "temp_chamber_c", None)
        try:
            is_subzero = float(temp_value) < 0.0
        except Exception:
            is_subzero = False

        if route_mode != "co2_only" and not is_subzero:
            h2o_points = [point for point in temp_group if point.is_h2o_point]
            h2o_pressure_points = runner._h2o_pressure_points_for_temperature(temp_group)
            for h2o_group in runner._group_h2o_points(h2o_points):
                if not h2o_group:
                    continue
                lead = h2o_group[0]
                for pressure_point in h2o_pressure_points:
                    planned_point = runner._build_h2o_pressure_point(lead, pressure_point)
                    planned.append(
                        PlannedPointSpec(
                            point_row=int(planned_point.index),
                            phase="h2o",
                            point_tag=runner._h2o_point_tag(planned_point),
                            title=runner._point_title(planned_point, phase="h2o", point_tag=runner._h2o_point_tag(planned_point)),
                        )
                    )

        if route_mode != "h2o_only":
            gas_sources = runner._co2_source_points(temp_group)
            co2_pressure_points = runner._co2_pressure_points_for_temperature(temp_group)
            for source_point in gas_sources:
                for pressure_point in co2_pressure_points:
                    planned_point = runner._build_co2_pressure_point(source_point, pressure_point)
                    planned.append(
                        PlannedPointSpec(
                            point_row=int(planned_point.index),
                            phase="co2",
                            point_tag=runner._co2_point_tag(planned_point),
                            title=runner._point_title(planned_point, phase="co2", point_tag=runner._co2_point_tag(planned_point)),
                        )
                    )
    return planned


def _sampling_target(runtime_cfg: Optional[Dict[str, Any]]) -> int:
    if not isinstance(runtime_cfg, dict):
        return 10
    workflow_cfg = runtime_cfg.get("workflow", {})
    if not isinstance(workflow_cfg, dict):
        return 10
    sampling_cfg = workflow_cfg.get("sampling", {})
    if not isinstance(sampling_cfg, dict):
        return 10
    try:
        target = int(sampling_cfg.get("stable_count", sampling_cfg.get("count", 10)))
    except Exception:
        return 10
    return max(1, target)


def _parse_event_labels(response_text: str) -> List[str]:
    try:
        payload = json.loads(response_text)
    except Exception:
        return []
    if not isinstance(payload, dict):
        return []
    labels = payload.get("labels", [])
    if not isinstance(labels, list):
        return []
    return [str(item).strip() for item in labels if str(item).strip()]


def audit_run_dir(
    run_dir: str | Path,
    *,
    runtime_cfg: Optional[Dict[str, Any]] = None,
    config_path: Optional[str] = None,
) -> AuditResult:
    run_path = Path(run_dir).resolve()
    result = AuditResult(run_dir=run_path)
    if not run_path.exists():
        result.failures.append(f"Run directory not found: {run_path}")
        return result

    artifacts = {
        "io_csv": _latest_artifact(run_path, "io_*.csv"),
        "points_csv": _latest_artifact(run_path, "points_*.csv"),
        "points_readable_csv": _latest_artifact(run_path, "points_readable_*.csv"),
        "points_readable_xlsx": _latest_artifact(run_path, "points_readable_*.xlsx"),
        "analyzer_summary_csv": _latest_artifact(run_path, "分析仪汇总_*.csv"),
        "analyzer_summary_xlsx": _latest_artifact(run_path, "分析仪汇总_*.xlsx"),
    }
    result.artifacts.update(artifacts)

    loaded_cfg = _load_runtime_cfg(run_path, runtime_cfg=runtime_cfg, config_path=config_path)
    if loaded_cfg is None:
        result.failures.append("Runtime config snapshot missing or unreadable; cannot compute planned points")
        return result

    if not (run_path / "runtime_config_snapshot.json").exists():
        result.warnings.append("Runtime config snapshot missing; used fallback config for planning")

    result.planned_points = plan_points_from_runtime_config(loaded_cfg)
    result.analyzer_labels = _configured_analyzer_labels(loaded_cfg)
    expected_sample_rows = _sampling_target(loaded_cfg)

    if not result.planned_points:
        result.failures.append("No planned points could be derived from runtime config")
        return result

    io_path = artifacts["io_csv"]
    if io_path is None:
        result.failures.append("Missing IO log (io_*.csv)")
    else:
        io_rows = _read_csv_rows(io_path)
        run_finished = False
        run_aborted = False
        unresolved_disabled: set[str] = set()
        logger_warns: List[str] = []
        for row in io_rows:
            port = str(row.get("port", "") or "").strip().upper()
            device = str(row.get("device", "") or "").strip().lower()
            direction = str(row.get("direction", "") or "").strip().upper()
            command = str(row.get("command", "") or "").strip()
            response = str(row.get("response", "") or "").strip()
            if port == "RUN" and device == "runner" and direction == "EVENT":
                if command == "run-finished":
                    run_finished = True
                elif command == "run-aborted":
                    run_aborted = True
                elif command == "analyzers-disabled":
                    unresolved_disabled.update(_parse_event_labels(response))
                elif command == "analyzers-restored":
                    unresolved_disabled.difference_update(_parse_event_labels(response))
                elif command == "analyzers-still-disabled":
                    unresolved_disabled.update(_parse_event_labels(response))
            elif port == "LOG" and device == "run_logger" and direction == "WARN":
                logger_warns.append(command or response or "logger warning")

        if run_aborted:
            result.failures.append("Run ended with run-aborted event")
        if not run_finished:
            result.failures.append("Run did not emit run-finished event")
        if logger_warns:
            preview = "；".join(logger_warns[:3])
            result.warnings.append(f"Logger warnings present: {preview}")
        if unresolved_disabled:
            disabled_text = ",".join(sorted(unresolved_disabled))
            result.warnings.append(f"Analyzers still disabled at end of run: {disabled_text}")

    required_artifacts = [
        ("points_csv", "Missing point summary CSV"),
        ("points_readable_csv", "Missing readable point CSV"),
        ("points_readable_xlsx", "Missing readable point workbook"),
        ("analyzer_summary_csv", "Missing analyzer summary CSV"),
        ("analyzer_summary_xlsx", "Missing analyzer summary workbook"),
    ]
    for key, message in required_artifacts:
        if artifacts.get(key) is None:
            result.failures.append(message)

    points_csv = artifacts["points_csv"]
    points_readable_csv = artifacts["points_readable_csv"]
    analyzer_summary_csv = artifacts["analyzer_summary_csv"]

    expected_points = len(result.planned_points)
    actual_point_rows = _csv_row_count(points_csv)
    readable_point_rows = _csv_row_count(points_readable_csv)
    summary_rows = _csv_row_count(analyzer_summary_csv)
    configured_analyzer_count = len(result.analyzer_labels)
    expected_analyzer_count, expected_analyzer_count_source = _infer_expected_analyzer_count(
        points_csv=points_csv,
        points_readable_csv=points_readable_csv,
        configured_count=configured_analyzer_count,
    )
    expected_summary_rows = expected_points * expected_analyzer_count

    result.infos.append(f"Planned points: {expected_points}")
    result.infos.append(f"Expected sample rows per point: {expected_sample_rows}")
    result.infos.append(f"Configured analyzers: {configured_analyzer_count}")
    result.infos.append(
        f"Expected analyzers per point: {expected_analyzer_count} ({expected_analyzer_count_source})"
    )

    if points_csv is not None and actual_point_rows != expected_points:
        result.failures.append(
            f"Point summary row count mismatch: expected {expected_points}, got {actual_point_rows}"
        )
    if points_readable_csv is not None and readable_point_rows != expected_points:
        result.failures.append(
            f"Readable point row count mismatch: expected {expected_points}, got {readable_point_rows}"
        )
    if analyzer_summary_csv is not None and summary_rows != expected_summary_rows:
        result.failures.append(
            f"Analyzer summary row count mismatch: expected {expected_summary_rows}, got {summary_rows}"
        )

    expected_files = {spec.sample_filename: spec for spec in result.planned_points}
    actual_sample_paths = {path.name: path for path in run_path.glob("point_*_samples.csv")}

    for filename, spec in expected_files.items():
        sample_path = actual_sample_paths.get(filename)
        if sample_path is None:
            result.failures.append(
                f"Missing sample file for planned point row {spec.point_row}: {filename}"
            )
            continue
        row_count = _sample_row_count(sample_path)
        if row_count < expected_sample_rows:
            result.failures.append(
                f"Sample file has too few rows ({row_count}<{expected_sample_rows}): {filename}"
            )
        elif row_count != expected_sample_rows:
            result.warnings.append(
                f"Sample file row count differs from target ({row_count}!={expected_sample_rows}): {filename}"
            )

    unexpected_samples = sorted(set(actual_sample_paths.keys()) - set(expected_files.keys()))
    if unexpected_samples:
        preview = "；".join(unexpected_samples[:3])
        result.warnings.append(f"Unexpected sample files present: {preview}")

    if points_csv is not None:
        for idx, row in enumerate(_read_csv_rows(points_csv), start=1):
            integrity = str(row.get(POINT_INTEGRITY_LABEL, "") or "").strip()
            if integrity and integrity != "完整":
                result.failures.append(
                    f"Point row {idx} integrity is not complete: {integrity}"
                )

    return result


def _resolve_run_dir(run_dir: Optional[str], config_path: Optional[str]) -> Optional[Path]:
    if run_dir:
        return Path(run_dir).resolve()

    cfg_path = config_path or "configs/default_config.json"
    try:
        cfg = load_config(cfg_path)
    except Exception:
        return None

    output_dir = cfg.get("paths", {}).get("output_dir")
    if not output_dir:
        return None
    base = Path(output_dir)
    candidates = [path for path in base.glob("run_*") if path.is_dir()]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _print_report(result: AuditResult) -> None:
    status = "PASS" if result.ok else "FAIL"
    print(f"Run audit: {status}", flush=True)
    print(f"Run dir: {result.run_dir}", flush=True)
    for item in result.infos:
        print(f"- {item}", flush=True)
    for title, items in (("Failures", result.failures), ("Warnings", result.warnings)):
        if not items:
            continue
        print(f"{title}:", flush=True)
        limit = 20
        for item in items[:limit]:
            print(f"  - {item}", flush=True)
        remain = len(items) - limit
        if remain > 0:
            print(f"  - ... {remain} more", flush=True)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit a completed calibration run directory.")
    parser.add_argument(
        "--run-dir",
        default=None,
        help="Run directory to audit. Defaults to the latest run under the configured output_dir.",
    )
    parser.add_argument(
        "--config",
        default="configs/default_config.json",
        help="Fallback config path when runtime snapshot is missing.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    run_dir = _resolve_run_dir(args.run_dir, args.config)
    if run_dir is None:
        print("No run directory found to audit.", flush=True)
        return 2

    result = audit_run_dir(run_dir, config_path=args.config)
    _print_report(result)
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())
