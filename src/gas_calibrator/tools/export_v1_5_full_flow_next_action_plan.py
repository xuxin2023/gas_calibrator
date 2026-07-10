"""Export the offline V1.5 full-flow next-action plan."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Iterable, Optional

from ..validation.v1_5_full_flow_next_action_plan import write_v1_5_full_flow_next_action_plan


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export the V1.5 full-flow next-action plan. Offline/no-COM/no-route/no-write only; "
            "ranks future automation handoffs without executing hardware or database actions."
        )
    )
    parser.add_argument("--output-dir", required=True, help="Output directory for next-action artifacts.")
    parser.add_argument(
        "--automation-closure-json",
        default="",
        help="Optional explicit v1_5_full_flow_automation_closure.json input.",
    )
    parser.add_argument(
        "--completed-action-id",
        action="append",
        default=[],
        help=(
            "Action id already closed by reviewed evidence. Repeat to make the offline planner "
            "recommend the first remaining V1.5 automation handoff."
        ),
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        paths = write_v1_5_full_flow_next_action_plan(
            output_dir=args.output_dir,
            automation_closure_json=args.automation_closure_json or None,
            completed_action_ids=args.completed_action_id,
        )
    except Exception as exc:  # pragma: no cover - CLI guardrail
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(json.dumps({key: str(path) for key, path in paths.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
