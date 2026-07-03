"""Run the V1.5 minimal read-only COM executor with explicit authorization."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_formal_readonly_com_minimal_executor import (
    build_v1_5_formal_readonly_com_minimal_executor,
    write_v1_5_formal_readonly_com_minimal_executor_outputs,
)


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the minimal V1.5 read-only COM executor. It only reads identity/SN/GETCO/runtime/CHECK "
            "evidence when --execute-read-only-real-com is explicitly supplied."
        )
    )
    parser.add_argument("--authorization-packet-json", required=True)
    parser.add_argument("--reviewed-port-inventory-json", required=True)
    parser.add_argument("--active-analyzer-list-json", required=True)
    parser.add_argument("--formal-readonly-com-execution-packet-validator-json", required=True)
    parser.add_argument("--formal-readonly-com-execution-plan-preview-json", required=True)
    parser.add_argument("--formal-readonly-com-minimal-executor-stub-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--execute-read-only-real-com", action="store_true")
    parser.add_argument("--baudrate", type=int, default=115200)
    parser.add_argument("--timeout-s", type=float, default=2.0)
    parser.add_argument("--command-gap-s", type=float, default=1.0)
    parser.add_argument("--fail-on-hold", action="store_true")
    forbidden = parser.add_argument_group("forbidden write/import/route options")
    forbidden.add_argument("--execute", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--execute-controlled-writes", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--allow-senco-write", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--allow-sn-write", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--allow-database-import", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--allow-route-control", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args(list(argv) if argv is not None else None)


def _forbidden_side_effect_requested(args: argparse.Namespace) -> bool:
    return bool(
        args.execute
        or args.execute_controlled_writes
        or args.allow_senco_write
        or args.allow_sn_write
        or args.allow_database_import
        or args.allow_route_control
    )


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    if _forbidden_side_effect_requested(args):
        print(
            "error: V1.5 read-only COM executor forbids write/import/route unlock flags",
            file=sys.stderr,
            flush=True,
        )
        return 2
    try:
        model = build_v1_5_formal_readonly_com_minimal_executor(
            execute_read_only_real_com=args.execute_read_only_real_com,
            authorization_packet_json=args.authorization_packet_json,
            reviewed_port_inventory_json=args.reviewed_port_inventory_json,
            active_analyzer_list_json=args.active_analyzer_list_json,
            formal_readonly_com_execution_packet_validator_json=(
                args.formal_readonly_com_execution_packet_validator_json
            ),
            formal_readonly_com_execution_plan_preview_json=(
                args.formal_readonly_com_execution_plan_preview_json
            ),
            formal_readonly_com_minimal_executor_stub_json=args.formal_readonly_com_minimal_executor_stub_json,
            baudrate=args.baudrate,
            timeout_s=args.timeout_s,
            command_gap_s=args.command_gap_s,
        )
        outputs = write_v1_5_formal_readonly_com_minimal_executor_outputs(model, args.output_dir)
    except Exception as exc:
        print(f"V1.5 read-only COM minimal executor failed: {exc}", file=sys.stderr, flush=True)
        return 1
    result = {
        "overall_status": model.get("overall_status"),
        "execution_attempted": model.get("execution_attempted"),
        "opens_com_ports": model.get("opens_com_ports"),
        "hold_count": model.get("hold_count"),
        "command_attempt_count": model.get("command_attempt_count"),
        "writes_sn": model.get("writes_sn"),
        "writes_coefficients": model.get("writes_coefficients"),
        "connects_postgresql": model.get("connects_postgresql"),
        "controls_water_or_gas_routes": model.get("controls_water_or_gas_routes"),
        "outputs": {key: str(path.resolve()) for key, path in outputs.items()},
    }
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
    if args.fail_on_hold and int(model.get("hold_count") or 0):
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
