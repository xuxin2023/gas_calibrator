"""Run the V1.5 authoritative resume-state writer only as a blocked stub."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_authoritative_resume_state_writer_blocked_executor import (
    build_v1_5_authoritative_resume_state_writer_blocked_executor,
    write_v1_5_authoritative_resume_state_writer_blocked_executor_outputs,
)


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Write no-state-write evidence for the future V1.5 authoritative resume-state writer. "
            "This command never creates, replaces, or opens the authoritative state target."
        )
    )
    parser.add_argument("--full-flow-plan-json", required=True)
    parser.add_argument("--resume-prefix-application-review-json", required=True)
    parser.add_argument("--authoritative-resume-state-writer-design-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-blocked", action="store_true")
    parser.add_argument("--fail-on-review-required", action="store_true")

    forbidden = parser.add_argument_group("locked authoritative-state writer options")
    forbidden.add_argument("--execute", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--write-state", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--replace-state", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--authoritative-state-json", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument(
        "--expected-existing-state-sha256", default=None, help=argparse.SUPPRESS
    )
    forbidden.add_argument("--authorization-id", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--operator-confirmation-text", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--reviewer", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--approver", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--allow-real-com", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--allow-pressure-control", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--allow-route-control", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--allow-writes", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--allow-database-import", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args(list(argv) if argv is not None else None)


def _locked_option_requested(args: argparse.Namespace) -> bool:
    return bool(
        args.execute
        or args.write_state
        or args.replace_state
        or args.authoritative_state_json
        or args.expected_existing_state_sha256
        or args.authorization_id
        or args.operator_confirmation_text
        or args.reviewer
        or args.approver
        or args.allow_real_com
        or args.allow_pressure_control
        or args.allow_route_control
        or args.allow_writes
        or args.allow_database_import
    )


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    if _locked_option_requested(args):
        print(
            "V1.5 authoritative resume-state writing is locked in this command. "
            "State target, expected-state hash, authorization, execute, replace, device, route, and database options are refused.",
            file=sys.stderr,
            flush=True,
        )
        return 2
    try:
        model = build_v1_5_authoritative_resume_state_writer_blocked_executor(
            full_flow_plan_json=args.full_flow_plan_json,
            resume_prefix_application_review_json=args.resume_prefix_application_review_json,
            authoritative_resume_state_writer_design_json=(
                args.authoritative_resume_state_writer_design_json
            ),
        )
        outputs = write_v1_5_authoritative_resume_state_writer_blocked_executor_outputs(
            model, args.output_dir
        )
    except Exception as exc:  # pragma: no cover - CLI guardrail
        print(f"V1.5 authoritative resume-state blocked executor failed: {exc}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "overall_status": model.get("overall_status"),
                "blocked_executor_ready": model.get("blocked_executor_ready"),
                "execution_supported": model.get("execution_supported"),
                "authoritative_state_write_allowed": model.get(
                    "authoritative_state_write_allowed"
                ),
                "writes_authoritative_state": model.get("writes_authoritative_state"),
                "state_file_created": model.get("state_file_created"),
                "state_file_replaced": model.get("state_file_replaced"),
                "outputs": {key: str(Path(value).resolve()) for key, value in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )
    if args.fail_on_review_required and model.get("review_required_count"):
        return 3
    if args.fail_on_blocked:
        return 2
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
