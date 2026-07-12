"""Run the authorized V1.5 one-step offline resume-state CAS writer."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_authoritative_resume_offline_state_advance_atomic_writer import (
    COMMITTED_STATUS,
    CONFIRMATION_TEMPLATE,
    execute_v1_5_authoritative_resume_offline_state_advance_atomic_write,
)


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--state-advance-authorization-json", required=True)
    parser.add_argument("--authorization-id", required=True)
    parser.add_argument("--confirmation-template", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--execute-controlled-state-advance", action="store_true")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    if not args.execute_controlled_state_advance:
        print(
            "V1.5 offline resume state advancement is locked. "
            "Review the exact authorization before using the controlled flag.",
            file=sys.stderr,
            flush=True,
        )
        return 2
    if args.confirmation_template != CONFIRMATION_TEMPLATE:
        print("V1.5 offline state-advance confirmation mismatch.", file=sys.stderr)
        return 2
    try:
        model = execute_v1_5_authoritative_resume_offline_state_advance_atomic_write(
            state_advance_authorization_json=args.state_advance_authorization_json,
            authorization_id=args.authorization_id,
            confirmation_template=args.confirmation_template,
            output_dir=args.output_dir,
        )
    except Exception as exc:  # pragma: no cover - CLI guardrail
        print(f"V1.5 offline state-advance writer failed: {exc}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "overall_status": model.get("overall_status"),
                "authorization_id": model.get("authorization_id"),
                "authorization_recomputed_under_lock": model.get(
                    "authorization_recomputed_under_lock"
                ),
                "write_attempted": model.get("write_attempted"),
                "authoritative_state_write_committed": model.get(
                    "authoritative_state_write_committed"
                ),
                "rollback_attempted": model.get("rollback_attempted"),
                "rollback_confirmed": model.get("rollback_confirmed"),
                "authoritative_state_json": model.get("authoritative_state_json"),
                "output_dir": str(Path(args.output_dir).resolve()),
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )
    if model.get("overall_status") == COMMITTED_STATUS:
        return 0
    if model.get("rollback_confirmed"):
        return 4
    return 3


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
