"""Export the V1.5 read-only COM execution packet validator sidecar.

This command only validates future packet JSON inputs. It never opens analyzer
COM ports and refuses direct live-execution flags.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_formal_readonly_com_execution_packet_validator import (
    build_v1_5_formal_readonly_com_execution_packet_validator,
    write_v1_5_formal_readonly_com_execution_packet_validator_outputs,
)


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate future V1.5 read-only COM authorization, reviewed-port, and "
            "active-analyzer packet JSON without opening COM."
        )
    )
    parser.add_argument("--formal-readonly-com-execution-blocked-executor-json", required=True)
    parser.add_argument("--authorization-packet-json", default="")
    parser.add_argument("--reviewed-port-inventory-json", default="")
    parser.add_argument("--active-analyzer-list-json", default="")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-review-required", action="store_true")

    forbidden = parser.add_argument_group("forbidden live execution inputs")
    forbidden.add_argument("--execute", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--execute-read-only-real-com", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--execute-controlled-writes", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--allow-real-com", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--operator-confirmation-text", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--authorization-id", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--reviewer", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--approver", default=None, help=argparse.SUPPRESS)
    return parser.parse_args(list(argv) if argv is not None else None)


def _forbidden_unlock_requested(args: argparse.Namespace) -> bool:
    return bool(
        args.execute
        or args.execute_read_only_real_com
        or args.execute_controlled_writes
        or args.allow_real_com
        or args.operator_confirmation_text
        or args.authorization_id
        or args.reviewer
        or args.approver
    )


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    if _forbidden_unlock_requested(args):
        print(
            "error: direct live read-only COM unlock fields are forbidden; provide packet JSON for offline validation",
            file=sys.stderr,
        )
        return 2
    model = build_v1_5_formal_readonly_com_execution_packet_validator(
        formal_readonly_com_execution_blocked_executor_json=args.formal_readonly_com_execution_blocked_executor_json,
        authorization_packet_json=args.authorization_packet_json or None,
        reviewed_port_inventory_json=args.reviewed_port_inventory_json or None,
        active_analyzer_list_json=args.active_analyzer_list_json or None,
    )
    outputs = write_v1_5_formal_readonly_com_execution_packet_validator_outputs(
        model,
        output_dir=Path(args.output_dir),
    )
    print(
        json.dumps(
            {
                "overall_status": model.get("overall_status"),
                "packet_validator_ready": model.get("packet_validator_ready"),
                "packet_validated_offline": model.get("packet_validated_offline"),
                "outputs": {key: str(path.resolve()) for key, path in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )
    if args.fail_on_review_required and int(model.get("review_required_count") or 0):
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
