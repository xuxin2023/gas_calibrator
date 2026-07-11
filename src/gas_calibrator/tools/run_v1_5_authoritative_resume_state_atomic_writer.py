"""Run the explicitly authorized V1.5 authoritative resume-state writer."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_authoritative_resume_state_atomic_writer import (
    COMMITTED_STATUS,
    CONFIRMATION_TEMPLATE,
    NOOP_STATUS,
    execute_v1_5_authoritative_resume_state_atomic_write,
)


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Atomically write the V1.5 authoritative resume state only from an exact ready preflight."
        )
    )
    parser.add_argument("--preflight-json", required=True)
    parser.add_argument("--writer-authorization-json", required=True)
    parser.add_argument("--authorization-id", required=True)
    parser.add_argument("--confirmation-template", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--execute-controlled-state-write", action="store_true")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    if not args.execute_controlled_state_write:
        print(
            "V1.5 authoritative resume-state writing is locked. "
            "Pass the explicit controlled-state-write flag only after reviewing the exact ready preflight.",
            file=sys.stderr,
            flush=True,
        )
        return 2
    if args.confirmation_template != CONFIRMATION_TEMPLATE:
        print("V1.5 authoritative resume-state confirmation template mismatch.", file=sys.stderr)
        return 2
    try:
        model = execute_v1_5_authoritative_resume_state_atomic_write(
            preflight_json=args.preflight_json,
            writer_authorization_json=args.writer_authorization_json,
            authorization_id=args.authorization_id,
            confirmation_template=args.confirmation_template,
            output_dir=args.output_dir,
        )
    except Exception as exc:  # pragma: no cover - CLI guardrail
        print(f"V1.5 authoritative resume-state atomic writer failed: {exc}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "overall_status": model.get("overall_status"),
                "authorization_id": model.get("authorization_id"),
                "preflight_recomputed_ready": model.get("preflight_recomputed_ready"),
                "current_state_sha256_rechecked": model.get(
                    "current_state_sha256_rechecked"
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
    if model.get("overall_status") in {COMMITTED_STATUS, NOOP_STATUS}:
        return 0
    if model.get("rollback_confirmed"):
        return 4
    return 3


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
