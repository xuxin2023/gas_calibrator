from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
from typing import Optional


def _bootstrap_src_path_for_direct_script() -> None:
    src_root = Path(__file__).resolve().parents[3]
    src_root_text = str(src_root)
    if src_root_text not in sys.path:
        sys.path.insert(0, src_root_text)


_bootstrap_src_path_for_direct_script()

from gas_calibrator.v2.core.run001_h2o_only_1_point_no_write_probe import (  # noqa: E402
    H2O_CLI_FLAG,
    load_json_mapping,
    write_h2o_1_point_no_write_probe_artifacts,
)


DEFAULT_CONFIG = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "validation"
    / "run001_h2o_only_1_point_no_write_real_machine.json"
)


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run-001 Step 3A H2O-only single-point no-write real-COM probe. "
            "Default mode is fail-closed evidence only and does not execute."
        )
    )
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="H2O single-point probe config JSON")
    parser.add_argument("--operator-confirmation", required=True, help="H2O operator confirmation JSON")
    parser.add_argument("--output-dir", default="", help="Probe artifact output directory")
    parser.add_argument("--branch", default="", help="Expected branch recorded in operator confirmation")
    parser.add_argument("--head", default="", help="Expected HEAD recorded in operator confirmation")
    parser.add_argument(
        H2O_CLI_FLAG,
        dest="allow_v2_h2o_1_point_no_write_real_com",
        action="store_true",
        help="Required H2O CLI unlock. It does not execute by itself.",
    )
    parser.add_argument(
        "--execute-probe",
        action="store_true",
        help="After all H2O gates pass, execute one controlled V2 H2O no-write single-point sample.",
    )
    return parser


def _git_run_app_py_untouched(_config_path: str) -> bool:
    repo_root = Path(__file__).resolve().parents[4]
    try:
        result = subprocess.run(
            ["git", "diff", "--quiet", "--", "run_app.py"],
            cwd=str(repo_root),
            text=True,
            capture_output=True,
            check=False,
        )
    except Exception:
        return True
    return result.returncode == 0


def main(argv: Optional[list[str]] = None) -> int:
    args = create_argument_parser().parse_args(argv)
    config_path = str(Path(args.config).resolve())
    raw_cfg = load_json_mapping(config_path)
    summary = write_h2o_1_point_no_write_probe_artifacts(
        raw_cfg,
        output_dir=args.output_dir or None,
        config_path=config_path,
        operator_confirmation_path=args.operator_confirmation,
        branch=args.branch,
        head=args.head,
        cli_allow=bool(args.allow_v2_h2o_1_point_no_write_real_com),
        execute_probe=bool(args.execute_probe),
        run_app_py_untouched=_git_run_app_py_untouched(config_path),
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0 if summary.get("final_decision") == "PASS" else 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
