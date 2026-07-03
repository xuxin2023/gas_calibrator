"""Run the future V1.5 read-only COM executor in blocked mode only."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_formal_readonly_com_execution_blocked_executor import (
    build_v1_5_formal_readonly_com_execution_blocked_executor,
    write_v1_5_formal_readonly_com_execution_blocked_executor_outputs,
)


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Write a no-COM/no-write blocked executor artifact for the future V1.5 read-only "
            "COM executor. This command never opens analyzer serial ports."
        )
    )
    parser.add_argument("--formal-readonly-com-execution-contract-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--fail-on-blocked",
        action="store_true",
        help="Return exit code 2 after writing the blocked executor artifact.",
    )
    parser.add_argument(
        "--fail-on-review-required",
        action="store_true",
        help="Return exit code 3 when input review is required.",
    )

    forbidden = parser.add_argument_group("locked real read-only COM options")
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


def _locked_live_option_requested(args: argparse.Namespace) -> bool:
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
    if _locked_live_option_requested(args):
        print(
            "V1.5 read-only COM execution is locked in this command. "
            "Real-COM reading must be implemented in a future separately reviewed executor.",
            file=sys.stderr,
            flush=True,
        )
        return 2
    try:
        model = build_v1_5_formal_readonly_com_execution_blocked_executor(
            formal_readonly_com_execution_contract_json=args.formal_readonly_com_execution_contract_json,
        )
        outputs = write_v1_5_formal_readonly_com_execution_blocked_executor_outputs(
            model,
            args.output_dir,
        )
    except Exception as exc:
        print(
            f"V1.5 read-only COM execution blocked executor failed: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return 1
    result = {
        "overall_status": model.get("overall_status"),
        "blocked_executor_ready": model.get("blocked_executor_ready"),
        "execution_supported": model.get("execution_supported"),
        "live_execution_allowed": model.get("live_execution_allowed"),
        "read_only_real_com_execution_allowed": model.get("read_only_real_com_execution_allowed"),
        "controlled_write_execution_allowed": model.get("controlled_write_execution_allowed"),
        "opens_com_ports": model.get("opens_com_ports"),
        "connects_postgresql": model.get("connects_postgresql"),
        "writes_sn": model.get("writes_sn"),
        "writes_coefficients": model.get("writes_coefficients"),
        "outputs": {key: str(Path(value).resolve()) for key, value in outputs.items()},
    }
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
    if args.fail_on_review_required and model.get("review_required_count"):
        return 3
    if args.fail_on_blocked:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
