from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Optional


def _bootstrap_src_path_for_direct_script() -> None:
    src_root = Path(__file__).resolve().parents[3]
    src_root_text = str(src_root)
    if src_root_text not in sys.path:
        sys.path.insert(0, src_root_text)


_bootstrap_src_path_for_direct_script()

from gas_calibrator.v2.core.run001_a1r_minimal_no_write_sampling_probe import (  # noqa: E402
    load_json_mapping,
    write_a1r_minimal_no_write_sampling_probe_artifacts,
)


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run-001 Step 3A/A1R minimal no-write real-COM sampling probe. "
            "Default mode is fail-closed evidence only and does not execute sampling."
        )
    )
    parser.add_argument("--config", required=True, help="A1R minimal no-write sampling probe config JSON")
    parser.add_argument("--operator-confirmation", required=True, help="A1R operator confirmation JSON")
    parser.add_argument("--output-dir", default="", help="Artifact output directory")
    parser.add_argument("--branch", default="", help="Expected branch recorded in operator confirmation")
    parser.add_argument("--head", default="", help="Expected HEAD recorded in operator confirmation")
    parser.add_argument(
        "--allow-v2-a1r-minimal-no-write-real-com",
        action="store_true",
        help="Required A1R CLI unlock. It does not execute by itself.",
    )
    parser.add_argument(
        "--execute-sampling",
        action="store_true",
        help="After all A1R gates pass, execute one minimal read-only sampling closure.",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = create_argument_parser().parse_args(argv)
    raw_cfg = load_json_mapping(args.config)
    summary = write_a1r_minimal_no_write_sampling_probe_artifacts(
        raw_cfg,
        output_dir=args.output_dir or None,
        config_path=str(Path(args.config).resolve()),
        operator_confirmation_path=args.operator_confirmation,
        branch=args.branch,
        head=args.head,
        cli_allow=bool(args.allow_v2_a1r_minimal_no_write_real_com),
        execute_sampling=bool(args.execute_sampling),
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0 if summary.get("final_decision") == "PASS" else 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
