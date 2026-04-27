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

from gas_calibrator.v2.core.real_com_probe_gate import (  # noqa: E402
    evaluate_conditioning_only_real_com_gate,
    load_json_mapping,
)


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate the Step 3A V2 conditioning-only real-COM gate. "
            "This command does not open COM ports or execute a probe."
        )
    )
    parser.add_argument("--config", required=True, help="Conditioning-only real-COM probe config JSON")
    parser.add_argument("--operator-confirmation", required=True, help="Operator confirmation JSON")
    parser.add_argument("--branch", default="", help="Expected branch recorded in operator confirmation")
    parser.add_argument("--head", default="", help="Expected HEAD recorded in operator confirmation")
    parser.add_argument(
        "--allow-v2-conditioning-only-real-com",
        action="store_true",
        help="Required Step 3A CLI unlock. The script still only evaluates admission.",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = create_argument_parser().parse_args(argv)
    raw_cfg = load_json_mapping(args.config)
    admission = evaluate_conditioning_only_real_com_gate(
        raw_cfg,
        cli_allow=bool(args.allow_v2_conditioning_only_real_com),
        operator_confirmation_path=args.operator_confirmation,
        branch=args.branch,
        head=args.head,
        config_path=str(Path(args.config).resolve()),
    )
    payload = admission.to_dict()
    payload["message"] = (
        "admission approved; real COM execution is intentionally not performed"
        if admission.approved
        else "admission rejected; real COM execution is not permitted"
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2), flush=True)
    return 0 if admission.approved else 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
