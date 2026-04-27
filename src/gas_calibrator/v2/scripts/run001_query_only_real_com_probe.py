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

from gas_calibrator.v2.core.run001_query_only_real_com_probe import (  # noqa: E402
    load_json_mapping,
    write_query_only_real_com_probe_artifacts,
)


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run-001 Step 3A/R0 query-only real-COM device inventory probe. "
            "Default mode is admission-only and does not open COM ports."
        )
    )
    parser.add_argument("--config", required=True, help="R0 query-only probe config JSON")
    parser.add_argument("--operator-confirmation", required=True, help="Operator confirmation JSON")
    parser.add_argument("--output-dir", default="", help="Artifact output directory")
    parser.add_argument("--branch", default="", help="Expected branch in operator confirmation")
    parser.add_argument("--head", default="", help="Expected HEAD in operator confirmation")
    parser.add_argument(
        "--allow-v2-query-only-real-com",
        action="store_true",
        help="Required R0 CLI unlock. It does not execute by itself.",
    )
    parser.add_argument(
        "--execute-query-only",
        action="store_true",
        help="After gate approval, open enabled non-H2O ports for query-only reads and close them.",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = create_argument_parser().parse_args(argv)
    raw_cfg = load_json_mapping(args.config)
    summary = write_query_only_real_com_probe_artifacts(
        raw_cfg,
        output_dir=args.output_dir or None,
        config_path=str(Path(args.config).resolve()),
        cli_allow=bool(args.allow_v2_query_only_real_com),
        operator_confirmation_path=args.operator_confirmation,
        branch=args.branch,
        head=args.head,
        execute_query_only=bool(args.execute_query_only),
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0 if summary.get("admission_approved") else 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
