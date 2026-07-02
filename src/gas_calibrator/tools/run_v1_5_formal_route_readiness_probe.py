"""Run the V1.5 formal route-readiness probe.

This is an initialization-stage probe, not a CO2/H2O sampling runner. It proves
that the formal route hardware is usable before any chamber soak starts.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..config import load_config
from ..validation.v1_5_formal_route_readiness import (
    build_formal_route_readiness_model,
    write_formal_route_readiness_model,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Probe V1.5 formal N2/CO2/H2O route readiness.")
    parser.add_argument("--config", required=True, help="V1.5 runtime config JSON.")
    parser.add_argument("--output-dir", required=True, help="Directory for formal_route_readiness.json.")
    parser.add_argument(
        "--n2-prepurge-s",
        type=float,
        default=None,
        help="Override N2 prepurge seconds. If omitted, workflow.nitrogen_purge is used.",
    )
    parser.add_argument(
        "--n2-source-valve",
        default=None,
        help="Override N2 logical source valve. If omitted, config valves/workflow mapping is used.",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    cfg = load_config(Path(args.config))
    model = build_formal_route_readiness_model(
        cfg,
        output_dir=args.output_dir,
        n2_prepurge_s=args.n2_prepurge_s,
        n2_source_valve=args.n2_source_valve,
    )
    path = write_formal_route_readiness_model(model, args.output_dir)
    print(
        json.dumps(
            {
                "status": model.get("status"),
                "ok": bool(model.get("ok")),
                "path": str(path),
                "issues": model.get("issues", []),
            },
            ensure_ascii=False,
        )
    )
    return 0 if model.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
