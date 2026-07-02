"""Export V1.5 advanced QC summary from existing run artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..v1_5.qc_advanced.exporter import write_advanced_qc_summary


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write V1.5 advanced QC JSON/Markdown without touching devices."
    )
    parser.add_argument("--run-dir", required=True, help="Existing V1.5 run directory with samples_*.csv.")
    parser.add_argument("--output-dir", default=None, help="Optional output directory. Defaults to <run-dir>/advanced_qc.")
    parser.add_argument("--pressure-quick-check-csv", default=None, help="Optional pressure_channel_quick_check*.csv path.")
    parser.add_argument("--analyzer-prefix", default="ga01")
    parser.add_argument("--window-size", type=int, default=10)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_advanced_qc_summary(
            run_dir=args.run_dir,
            output_dir=args.output_dir,
            pressure_quick_check_path=args.pressure_quick_check_csv,
            analyzer_prefix=args.analyzer_prefix,
            window_size=args.window_size,
        )
    except Exception as exc:
        print(f"V1.5 advanced QC export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
