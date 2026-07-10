"""Export the offline V1.5 Pressure/SENCO9 readiness index."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Iterable, Optional

from ..validation.v1_5_pressure_s9_readiness_index import write_v1_5_pressure_s9_readiness_index


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export a V1.5 Pressure/SENCO9 readiness evidence index. "
            "Offline/no-COM/no-write/no-pressure-control/no-route/no-PostgreSQL only."
        )
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--no-write-fit-summary-csv", default="")
    parser.add_argument("--no-write-fit-summary-json", default="")
    parser.add_argument("--senco9-write-readback-csv", default="")
    parser.add_argument("--senco9-write-readback-json", default="")
    parser.add_argument("--pressure-reverify-csv", default="")
    parser.add_argument("--pressure-reverify-json", default="")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        paths = write_v1_5_pressure_s9_readiness_index(
            output_dir=args.output_dir,
            no_write_fit_summary_csv=args.no_write_fit_summary_csv or None,
            no_write_fit_summary_json=args.no_write_fit_summary_json or None,
            senco9_write_readback_csv=args.senco9_write_readback_csv or None,
            senco9_write_readback_json=args.senco9_write_readback_json or None,
            pressure_reverify_csv=args.pressure_reverify_csv or None,
            pressure_reverify_json=args.pressure_reverify_json or None,
        )
    except Exception as exc:  # pragma: no cover - CLI guardrail
        print(f"V1.5 pressure/S9 readiness index export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(path) for key, path in paths.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
