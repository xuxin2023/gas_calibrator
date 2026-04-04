"""Offline validation for historical V1 run directories.

This is a sidecar diagnostic tool for no-humidity-generator / no-gas-cylinder
conditions. It does not change the V1 production workflow or write devices.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional

from ..config import load_config
from ..validation.common import analyze_sample_rows, latest_artifact, load_csv_rows
from ..validation.reporting import ValidationMetadata, write_validation_report


def _parse_csv_list(raw: str | None) -> List[str]:
    out: List[str] = []
    for part in str(raw or "").split(","):
        text = part.strip()
        if text:
            out.append(text)
    return out


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate a historical V1 run directory offline.")
    parser.add_argument("--run-dir", required=True, help="Historical run directory.")
    parser.add_argument("--config", default=None, help="Optional config path override.")
    parser.add_argument("--analyzers", default="", help="Optional analyzer labels or IDs, comma-separated.")
    parser.add_argument(
        "--gas",
        choices=("co2", "h2o", "both"),
        default="both",
        help="Gas selection for fit-input diagnostics.",
    )
    parser.add_argument(
        "--mode",
        choices=("legacy", "current", "both"),
        default="both",
        help="Comparison mode. 'both' exports legacy/current comparison.",
    )
    parser.add_argument("--output-dir", default=None, help="Optional validation output directory.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    run_dir = Path(args.run_dir).resolve()
    if not run_dir.exists():
        print(f"Run directory not found: {run_dir}", flush=True)
        return 2

    snapshot_path = run_dir / "runtime_config_snapshot.json"
    if args.config:
        cfg = load_config(args.config)
        config_path = str(Path(args.config).resolve())
    elif snapshot_path.exists():
        cfg = json.loads(snapshot_path.read_text(encoding="utf-8"))
        config_path = str(snapshot_path)
    else:
        print("No runtime snapshot found; pass --config to continue.", flush=True)
        return 2

    samples_path = latest_artifact(run_dir, "samples_*.csv")
    if samples_path is None:
        print(f"No samples_*.csv found under {run_dir}", flush=True)
        return 2

    summary_path = latest_artifact(run_dir, "分析仪汇总_*.csv")
    analyzer_book = latest_artifact(run_dir, "*analyzer_sheets_*.xlsx")
    point_csv = latest_artifact(run_dir, "points_*.csv")

    sample_rows = load_csv_rows(samples_path)
    analyzer_filter = _parse_csv_list(args.analyzers)
    modes = ("legacy", "current") if args.mode == "both" else (args.mode,)
    tables = analyze_sample_rows(
        sample_rows,
        cfg=cfg,
        analyzer_filter=analyzer_filter,
        gas=args.gas,
        modes=modes,
    )

    output_dir = Path(args.output_dir).resolve() if args.output_dir else run_dir / "validation_offline"
    metadata = ValidationMetadata(
        tool_name="validate_offline_run",
        created_at=datetime.now().isoformat(timespec="seconds"),
        analyzers=sorted({str(row.get("Analyzer") or "") for row in tables["frame_quality_summary"] if row.get("Analyzer")}),
        input_paths=[
            str(samples_path),
            str(summary_path) if summary_path else "",
            str(analyzer_book) if analyzer_book else "",
            str(point_csv) if point_csv else "",
        ],
        output_dir=str(output_dir),
        config_path=config_path,
        config_summary={
            "gas": args.gas,
            "mode": args.mode,
            "analyzer_filter": analyzer_filter,
            "legacy_note": "legacy mode simulates pre-fix alignment/pressure strategy; frame-quality relaxation is approximate.",
            "current_note": "current mode uses aligned summary references, current frame-quality rules, and current pressure-source selection.",
        },
        notes=[
            "This is an offline pre-validation tool. It does not touch live devices.",
            "Legacy mode is best-effort and intentionally does not mutate production code.",
        ],
    )
    outputs = write_validation_report(
        output_dir,
        prefix=f"offline_validation_{run_dir.name}",
        metadata=metadata,
        tables=tables,
    )
    print(f"Offline validation saved: {outputs['workbook']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
