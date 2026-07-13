"""Export the legacy V1.5 full-flow historical replay."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_legacy_full_flow_offline_replay import (
    write_v1_5_legacy_full_flow_offline_replay,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay the legacy V1.5 full-flow state machine from checked-in evidence without hardware execution."
    )
    parser.add_argument("--repository-root", default=str(Path.cwd()))
    parser.add_argument("--source-origin-main-commit", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        paths = write_v1_5_legacy_full_flow_offline_replay(
            output_dir=args.output_dir,
            repository_root=args.repository_root,
            source_origin_main_commit=args.source_origin_main_commit,
        )
    except Exception as exc:  # pragma: no cover - CLI guardrail
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(json.dumps({key: str(path) for key, path in paths.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
