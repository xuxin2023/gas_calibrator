"""Export an offline V1.5 formal-flow contract report."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_formal_flow_contract import (
    read_json,
    render_v1_5_formal_flow_contract_markdown,
    validate_v1_5_formal_flow_contract,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate and export the V1.5 formal flow contract.")
    parser.add_argument("--plan-json", required=True, help="Path to v1_5_full_flow_plan.json.")
    parser.add_argument("--inventory-json", default=None, help="Optional V1.5 entrypoint inventory JSON.")
    parser.add_argument("--output-dir", required=True, help="Directory for contract artifacts.")
    parser.add_argument(
        "--fail-on-blocked",
        action="store_true",
        help="Return non-zero when the contract report status is blocked.",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    plan = read_json(args.plan_json)
    inventory = read_json(args.inventory_json) if args.inventory_json else None
    report = validate_v1_5_formal_flow_contract(plan, inventory_entries=inventory)

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "v1_5_formal_flow_contract.json"
    md_path = output_dir / "v1_5_formal_flow_contract.md"
    json_path.write_text(json.dumps(report.to_json(), ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_v1_5_formal_flow_contract_markdown(report), encoding="utf-8")

    print(
        json.dumps(
            {
                "status": report.status,
                "issues": len(report.issues),
                "warnings": len(report.warnings),
                "json": str(json_path),
                "markdown": str(md_path),
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )
    if args.fail_on_blocked and report.status == "blocked":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
