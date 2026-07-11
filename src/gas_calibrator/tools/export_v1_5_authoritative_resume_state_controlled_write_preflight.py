"""Export the V1.5 authoritative resume-state controlled-write preflight."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_authoritative_resume_state_controlled_write_preflight import (
    build_v1_5_authoritative_resume_state_controlled_write_preflight,
    write_v1_5_authoritative_resume_state_controlled_write_preflight_outputs,
)


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Review an exact V1.5 resume-state target/candidate/authorization packet without writing state."
        )
    )
    parser.add_argument("--full-flow-plan-json", required=True)
    parser.add_argument("--resume-prefix-application-review-json", required=True)
    parser.add_argument("--authoritative-resume-state-writer-design-json", required=True)
    parser.add_argument(
        "--authoritative-resume-state-writer-blocked-executor-json", required=True
    )
    parser.add_argument("--authorization-packet-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-blocker", action="store_true")
    parser.add_argument("--fail-on-review-required", action="store_true")

    forbidden = parser.add_argument_group("locked state-write options")
    forbidden.add_argument("--execute", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--write-state", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--replace-state", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--allow-real-com", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument(
        "--allow-pressure-control", action="store_true", help=argparse.SUPPRESS
    )
    forbidden.add_argument("--allow-route-control", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--allow-writes", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument(
        "--allow-database-import", action="store_true", help=argparse.SUPPRESS
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _locked_option_requested(args: argparse.Namespace) -> bool:
    return bool(
        args.execute
        or args.write_state
        or args.replace_state
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
            "V1.5 authoritative resume-state preflight is no-write. Execute, replace, COM, route, write, and database options are refused.",
            file=sys.stderr,
            flush=True,
        )
        return 2
    try:
        model = build_v1_5_authoritative_resume_state_controlled_write_preflight(
            full_flow_plan_json=args.full_flow_plan_json,
            resume_prefix_application_review_json=args.resume_prefix_application_review_json,
            authoritative_resume_state_writer_design_json=(
                args.authoritative_resume_state_writer_design_json
            ),
            authoritative_resume_state_writer_blocked_executor_json=(
                args.authoritative_resume_state_writer_blocked_executor_json
            ),
            authorization_packet_json=args.authorization_packet_json,
        )
        outputs = write_v1_5_authoritative_resume_state_controlled_write_preflight_outputs(
            model, args.output_dir
        )
    except Exception as exc:  # pragma: no cover - CLI guardrail
        print(f"V1.5 authoritative resume-state preflight failed: {exc}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "overall_status": model.get("overall_status"),
                "blocker_count": model.get("blocker_count"),
                "review_required_count": model.get("review_required_count"),
                "controlled_write_preflight_ready": model.get(
                    "controlled_write_preflight_ready"
                ),
                "authoritative_state_write_allowed": model.get(
                    "authoritative_state_write_allowed"
                ),
                "writes_authoritative_state": model.get("writes_authoritative_state"),
                "candidate_state_sha256": model.get("candidate_state_sha256"),
                "outputs": {key: str(Path(value).resolve()) for key, value in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )
    if args.fail_on_blocker and model.get("blocker_count"):
        return 2
    if args.fail_on_review_required and model.get("review_required_count"):
        return 3
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
