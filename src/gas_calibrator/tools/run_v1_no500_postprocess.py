"""Canonical offline postprocess entry that removes 500 hPa rows first.

This keeps the "2026-04-03 no-500" workflow explicit and reproducible:
1. filter completed summary rows to exclude 500 hPa points
2. export the standard calibration workbook from filtered summaries
3. skip refit/QC/AI by default so the report stays summary-driven
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

import pandas as pd

from ..v2.adapters import v1_postprocess_runner
from ..v2.export import load_summary_workbook_rows


def _log(message: str) -> None:
    print(message, flush=True)


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "null", "None"):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _first_present(row: pd.Series, keys: Iterable[str]) -> Any:
    for key in keys:
        if key in row.index:
            value = row.get(key)
            if value not in (None, ""):
                return value
    return None


def _is_500hpa_row(row: pd.Series) -> bool:
    pressure_label = str(
        _first_present(row, ("PressureTargetLabel", "压力目标标签", "pressure_target_label")) or ""
    ).strip().lower()
    if "500" in pressure_label:
        return True

    pressure_target = _safe_float(
        _first_present(row, ("PressureTarget", "目标压力hPa", "pressure_target_hpa"))
    )
    if pressure_target is not None and abs(pressure_target - 500.0) <= 0.5:
        return True

    pressure_mode = str(
        _first_present(row, ("PressureMode", "压力执行模式", "pressure_mode")) or ""
    ).strip().lower()
    if pressure_mode == "sealed_controlled" and pressure_target is not None and abs(pressure_target - 500.0) <= 5.0:
        return True

    return False


def _filter_no_500_frame(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    if frame.empty:
        return frame.copy(), {"original_rows": 0, "removed_rows": 0, "kept_rows": 0}
    mask = frame.apply(_is_500hpa_row, axis=1)
    filtered = frame.loc[~mask].copy()
    return filtered, {
        "original_rows": int(len(frame)),
        "removed_rows": int(mask.sum()),
        "kept_rows": int(len(filtered)),
    }


def _write_filtered_summary(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, encoding="utf-8-sig")


def _resolve_input_paths(
    *,
    run_dir: Optional[str],
    summary_paths: Optional[Sequence[str]],
) -> tuple[Optional[Path], list[Path]]:
    resolved_run_dir: Optional[Path] = None
    if run_dir:
        resolved_run_dir = Path(run_dir).resolve()
    resolved_summary_paths = v1_postprocess_runner._resolve_summary_paths(  # noqa: SLF001
        run_dir=resolved_run_dir,
        summary_paths=summary_paths,
    )
    return resolved_run_dir, list(resolved_summary_paths)


def run_from_cli(
    *,
    run_dir: Optional[str] = None,
    summary_paths: Optional[Sequence[str]] = None,
    config_path: Optional[str] = None,
    output_dir: Optional[str] = None,
    skip_qc: bool = True,
    skip_refit: bool = True,
    skip_ai: bool = True,
    skip_analytics: bool = True,
    skip_measurement_analytics: bool = True,
) -> dict[str, Any]:
    resolved_run_dir, resolved_summary_paths = _resolve_input_paths(
        run_dir=run_dir,
        summary_paths=summary_paths,
    )
    if output_dir:
        target_dir = Path(output_dir).resolve()
    else:
        base_dir = resolved_run_dir or resolved_summary_paths[0].parent
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        target_dir = (base_dir / f"offline_postprocess_no_500_pressure_{stamp}").resolve()
    target_dir.mkdir(parents=True, exist_ok=True)

    filtered_paths: list[Path] = []
    filter_summary: dict[str, dict[str, Any]] = {}
    for source_path in resolved_summary_paths:
        frame = load_summary_workbook_rows([source_path])
        filtered_frame, stats = _filter_no_500_frame(frame)
        filtered_name = f"{source_path.stem}_no_500hpa.csv"
        filtered_path = target_dir / filtered_name
        _write_filtered_summary(filtered_path, filtered_frame)
        filtered_paths.append(filtered_path)
        filter_summary[source_path.name] = {
            "source": str(source_path),
            "filtered_csv": str(filtered_path),
            **stats,
        }

    (target_dir / "no_500_filter_summary.json").write_text(
        json.dumps(filter_summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    exported = v1_postprocess_runner.run_from_cli(
        run_dir=str(resolved_run_dir) if resolved_run_dir is not None else None,
        summary_paths=[str(path) for path in filtered_paths],
        config_path=config_path,
        output_dir=str(target_dir),
        download=False,
        import_db=False,
        skip_qc=skip_qc,
        skip_refit=skip_refit,
        skip_ai=skip_ai,
        run_analytics=not bool(skip_analytics),
        skip_analytics=bool(skip_analytics),
        run_measurement_analytics=not bool(skip_measurement_analytics),
        skip_measurement_analytics=bool(skip_measurement_analytics),
        latest_run=False,
    )

    summary_lines = [
        f"# no-500 postprocess summary",
        "",
        f"- run_dir: {resolved_run_dir}" if resolved_run_dir is not None else "- run_dir: <summary-only>",
        f"- output_dir: {target_dir}",
        f"- workbook: {target_dir / 'calibration_coefficients.xlsx'}",
        "",
        "## filtered inputs",
    ]
    for key, payload in filter_summary.items():
        summary_lines.extend(
            [
                f"- {key}: original={payload['original_rows']} removed={payload['removed_rows']} kept={payload['kept_rows']}",
            ]
        )
    (target_dir / "summary.md").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    return {
        "output_dir": str(target_dir),
        "filtered_summary_paths": [str(path) for path in filtered_paths],
        "filter_summary": filter_summary,
        "postprocess": exported,
    }


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the canonical no-500 V1 offline postprocess flow")
    parser.add_argument("--run-dir", help="Completed V1 run directory")
    parser.add_argument(
        "--summary-path",
        dest="summary_paths",
        action="append",
        default=None,
        help="Explicit summary csv/xlsx path. Can be passed multiple times.",
    )
    parser.add_argument("--config-path", help="Optional config json path", default=None)
    parser.add_argument("--output-dir", help="Output directory", default=None)
    parser.add_argument("--enable-qc", action="store_true", help="Run QC instead of the default skip")
    parser.add_argument("--enable-refit", action="store_true", help="Run refit instead of the default skip")
    parser.add_argument("--enable-ai", action="store_true", help="Run AI note instead of the default skip")
    args = parser.parse_args(list(argv) if argv is not None else None)
    if not args.run_dir and not args.summary_paths:
        parser.error("one of --run-dir or --summary-path is required")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    exported = run_from_cli(
        run_dir=args.run_dir,
        summary_paths=args.summary_paths,
        config_path=args.config_path,
        output_dir=args.output_dir,
        skip_qc=not bool(args.enable_qc),
        skip_refit=not bool(args.enable_refit),
        skip_ai=not bool(args.enable_ai),
        skip_analytics=True,
        skip_measurement_analytics=True,
    )
    _log(json.dumps(exported, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
