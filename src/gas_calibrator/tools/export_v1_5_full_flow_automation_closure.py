"""Export the offline V1.5 full-flow automation closure map."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Iterable, Optional

from ..validation.v1_5_full_flow_automation_closure import write_v1_5_full_flow_automation_closure


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export the V1.5 full-flow automation closure map. Offline/no-COM/no-route/no-write only; "
            "records current automation boundaries without executing hardware or database actions."
        )
    )
    parser.add_argument("--output-dir", required=True, help="Output directory for closure artifacts.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        paths = write_v1_5_full_flow_automation_closure(output_dir=args.output_dir)
    except Exception as exc:  # pragma: no cover - CLI guardrail
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(json.dumps({key: str(path) for key, path in paths.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
