"""Atomically bootstrap a new zero-authority V1.5 production run."""

from __future__ import annotations

import argparse
import json
from typing import Iterable

from ..validation.v1_5_new_run_bootstrap import bootstrap_v1_5_new_run


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--config", required=True)
    parser.add_argument("--runs-root", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--operator", required=True)
    parser.add_argument("--reviewer", required=True)
    parser.add_argument("--approver", required=True)
    parser.add_argument("--analyzer-id", default="multi_device")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = bootstrap_v1_5_new_run(
        config_path=args.config,
        runs_root=args.runs_root,
        run_id=args.run_id,
        operator=args.operator,
        reviewer=args.reviewer,
        approver=args.approver,
        analyzer_id=args.analyzer_id,
    )
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "run_id": model["run_id"],
                "run_root": model["run_root"],
                "current_step_id": model["current_step_id"],
                "completed_step_ids": model["completed_step_ids"],
                "physical_capabilities": model["physical_capabilities"],
                "manifest_json": model["manifest_json"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
