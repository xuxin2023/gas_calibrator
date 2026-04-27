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

from gas_calibrator.v2.core.run001_r0_1_reference_read_probe import (  # noqa: E402
    load_json_mapping,
    write_r0_1_reference_read_probe_artifacts,
)


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run-001 Step 3A/R0.1 critical reference read-only diagnostics. "
            "Default mode writes admission evidence only and does not open COM ports."
        )
    )
    parser.add_argument("--config", required=True, help="R0.1 reference read probe config JSON")
    parser.add_argument("--output-dir", default="", help="Artifact output directory")
    parser.add_argument(
        "--allow-v2-query-only-real-com",
        action="store_true",
        help="Required R0.1 CLI unlock. It does not execute by itself.",
    )
    parser.add_argument(
        "--execute-read-only",
        action="store_true",
        help="Open COM30/COM27 only for read-only reference diagnostics.",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = create_argument_parser().parse_args(argv)
    raw_cfg = load_json_mapping(args.config)
    summary = write_r0_1_reference_read_probe_artifacts(
        raw_cfg,
        output_dir=args.output_dir or None,
        config_path=str(Path(args.config).resolve()),
        cli_allow=bool(args.allow_v2_query_only_real_com),
        execute_read_only=bool(args.execute_read_only),
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0 if summary.get("final_decision") in {"PASS", "ADMISSION_APPROVED"} else 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
