"""Export the offline V1.5 batch initialization closeout index."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Iterable, Optional

from ..validation.v1_5_batch_initialization_closeout_index import (
    write_v1_5_batch_initialization_closeout_index,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export a V1.5 batch initialization closeout evidence index. "
            "Offline/no-COM/no-write/no-route/no-PostgreSQL only."
        )
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--readonly-com-executor-json", default="")
    parser.add_argument("--readonly-identity-getco-snapshot-json", default="")
    parser.add_argument("--pressure-readiness-json", default="")
    parser.add_argument("--pressure-device-readiness-csv", default="")
    parser.add_argument("--route-readiness-json", default="")
    parser.add_argument("--pre-gas-readiness-json", default="")
    parser.add_argument(
        "--fail-on-review-required",
        action="store_true",
        help="Return exit code 2 unless the batch is ready for the mature open-flow route.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        paths = write_v1_5_batch_initialization_closeout_index(
            output_dir=args.output_dir,
            readonly_com_executor_json=args.readonly_com_executor_json or None,
            readonly_identity_getco_snapshot_json=args.readonly_identity_getco_snapshot_json or None,
            pressure_readiness_json=args.pressure_readiness_json or None,
            pressure_device_readiness_csv=args.pressure_device_readiness_csv or None,
            route_readiness_json=args.route_readiness_json or None,
            pre_gas_readiness_json=args.pre_gas_readiness_json or None,
        )
    except Exception as exc:  # pragma: no cover - CLI guardrail
        print(f"V1.5 batch initialization closeout index export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(path) for key, path in paths.items()}, ensure_ascii=False, indent=2))
    if args.fail_on_review_required:
        payload = json.loads(paths["manifest"].read_text(encoding="utf-8"))
        if payload.get("ready_for_mature_open_flow_from_initialization_index") is not True:
            return 2
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
