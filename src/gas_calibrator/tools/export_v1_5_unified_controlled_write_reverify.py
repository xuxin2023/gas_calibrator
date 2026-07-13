"""Export the offline V1.5 unified coefficient write/reverify contract."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from ..validation.v1_5_unified_controlled_write_reverify import (
    build_v1_5_unified_controlled_write_reverify,
    write_v1_5_unified_controlled_write_reverify,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", required=True, type=Path)
    parser.add_argument("--production-fit-matrix-json", required=True, type=Path)
    parser.add_argument("--approved-candidate-packet-json", type=Path)
    parser.add_argument("--current-getco-snapshot-json", type=Path)
    parser.add_argument("--authorization-json", type=Path)
    parser.add_argument("--write-events-json", type=Path)
    parser.add_argument("--short-reverify-json", type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser


def _validate_output_dir(repository_root: Path, output_dir: Path) -> None:
    root = repository_root.resolve()
    output = output_dir.resolve()
    allowed = (root / "docs" / "v1_5_flow_contract").resolve()
    if not output.is_relative_to(allowed):
        raise ValueError("output-dir must stay under docs/v1_5_flow_contract")
    if "_handoff" in {part.lower() for part in output.parts}:
        raise ValueError("output-dir must not use _handoff")


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    _validate_output_dir(args.repository_root, args.output_dir)
    model = build_v1_5_unified_controlled_write_reverify(
        production_fit_matrix_json=args.production_fit_matrix_json,
        approved_candidate_packet_json=args.approved_candidate_packet_json,
        current_getco_snapshot_json=args.current_getco_snapshot_json,
        authorization_json=args.authorization_json,
        write_events_json=args.write_events_json,
        short_reverify_json=args.short_reverify_json,
    )
    write_v1_5_unified_controlled_write_reverify(model, args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
