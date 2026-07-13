"""Run only the blocked V1.5 new-algorithm mature-queue handoff stub."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_new_algorithm_mature_queue_live_handoff import (
    build_v1_5_new_algorithm_mature_queue_live_handoff_blocked_executor,
    write_v1_5_new_algorithm_mature_queue_live_handoff_blocked_executor,
)


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--live-handoff-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-blocked", action="store_true")
    locked = parser.add_argument_group("locked live handoff options")
    locked.add_argument("--execute", action="store_true", help=argparse.SUPPRESS)
    locked.add_argument(
        "--execute-new-algorithm-mature-queue-handoff",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    locked.add_argument("--allow-real-com", action="store_true", help=argparse.SUPPRESS)
    locked.add_argument("--authorization-packet-json", help=argparse.SUPPRESS)
    locked.add_argument("--active-analyzer-list-json", help=argparse.SUPPRESS)
    locked.add_argument("--reviewed-port-inventory-json", help=argparse.SUPPRESS)
    locked.add_argument("--runtime-config", help=argparse.SUPPRESS)
    locked.add_argument("--co2-queue-csv", help=argparse.SUPPRESS)
    locked.add_argument("--h2o-queue-csv", help=argparse.SUPPRESS)
    locked.add_argument("--operator", help=argparse.SUPPRESS)
    locked.add_argument("--reviewer", help=argparse.SUPPRESS)
    locked.add_argument("--approver", help=argparse.SUPPRESS)
    return parser.parse_args(list(argv) if argv is not None else None)


def _live_option_requested(args: argparse.Namespace) -> bool:
    return bool(
        args.execute
        or args.execute_new_algorithm_mature_queue_handoff
        or args.allow_real_com
        or args.authorization_packet_json
        or args.active_analyzer_list_json
        or args.reviewed_port_inventory_json
        or args.runtime_config
        or args.co2_queue_csv
        or args.h2o_queue_csv
        or args.operator
        or args.reviewer
        or args.approver
    )


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    if _live_option_requested(args):
        print(
            "V1.5 new-algorithm live mature-queue execution is not implemented in "
            "this blocked executor. Use a future separately reviewed and authorized adapter.",
            file=sys.stderr,
            flush=True,
        )
        return 2
    try:
        model = build_v1_5_new_algorithm_mature_queue_live_handoff_blocked_executor(
            live_handoff_json=args.live_handoff_json
        )
        outputs = write_v1_5_new_algorithm_mature_queue_live_handoff_blocked_executor(
            model, args.output_dir
        )
    except Exception as exc:
        print(f"V1.5 new-algorithm blocked handoff failed: {exc}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "overall_status": model.get("overall_status"),
                "execution_supported": model.get("execution_supported"),
                "execution_attempted": model.get("execution_attempted"),
                "live_queue_execution_allowed": model.get(
                    "live_queue_execution_allowed"
                ),
                "outputs": {
                    key: str(Path(value).resolve()) for key, value in outputs.items()
                },
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 2 if args.fail_on_blocked else 0


if __name__ == "__main__":
    raise SystemExit(main())
