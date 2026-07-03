"""Export the V1.5 read-only COM execution packet contract.

This command is offline only. It never opens analyzer COM ports and never
consumes operator authorization as an unlock.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_formal_readonly_com_execution_contract import (
    build_v1_5_formal_readonly_com_execution_contract,
    write_v1_5_formal_readonly_com_execution_contract_outputs,
)


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export an offline contract for a future controlled V1.5 read-only COM execution packet."
    )
    parser.add_argument(
        "--formal-initialization-readonly-com-preflight-controlled-blocked-executor-json",
        required=True,
    )
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
    forbidden.add_argument("--reviewed-port-inventory-json", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--active-analyzer-list-json", default=None, help=argparse.SUPPRESS)
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
        or args.reviewed_port_inventory_json
        or args.active_analyzer_list_json
    )


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    if _forbidden_unlock_requested(args):
        print(
            "error: live read-only COM authorization and execution inputs are locked in this offline contract exporter",
            file=sys.stderr,
        )
        return 2

    model = build_v1_5_formal_readonly_com_execution_contract(
        formal_initialization_readonly_com_preflight_controlled_blocked_executor_json=(
            args.formal_initialization_readonly_com_preflight_controlled_blocked_executor_json
        )
    )
    outputs = write_v1_5_formal_readonly_com_execution_contract_outputs(model, output_dir=args.output_dir)
    print(json.dumps({"outputs": outputs, "overall_status": model["overall_status"]}, ensure_ascii=False, indent=2))
    if args.fail_on_review_required and int(model.get("review_required_count") or 0):
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
