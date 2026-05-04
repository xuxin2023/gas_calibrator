from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Iterable, Optional


def ensure_src_root_on_path() -> Path:
    src_root = Path(__file__).resolve().parents[3]
    src_root_text = str(src_root)
    if src_root_text not in sys.path:
        sys.path.insert(0, src_root_text)
    return src_root


ensure_src_root_on_path()

from gas_calibrator.v2.core.run001_query_only_real_com_probe import (  # noqa: E402
    QUERY_ONLY_REAL_COM_ENV_VAR,
    write_query_only_real_com_probe_artifacts,
    load_json_mapping,
)
from gas_calibrator.devices.temperature_chamber import TemperatureChamber  # noqa: E402,F401


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="A2.21 query-only COM sanity probe entry")
    parser.add_argument("--config", type=Path, help="Query-only config JSON")
    parser.add_argument("--operator-confirmation", type=Path, help="Operator confirmation JSON")
    parser.add_argument("--output-dir", type=Path, help="Output directory")
    parser.add_argument("--branch", default="", help="Expected git branch")
    parser.add_argument("--head", default="", help="Expected git HEAD")
    parser.add_argument("--execute-query-only", action="store_true")
    parser.add_argument("--allow-v2-query-only-real-com", action="store_true")
    parser.add_argument(
        "--self-check-import-only",
        action="store_true",
        help="Offline import-path check; does not open COM ports",
    )
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = create_argument_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.self_check_import_only:
        print(
            json.dumps(
                {
                    "temperature_chamber_probe_import_path_fixed": True,
                    "temperature_chamber_import_ok": True,
                    "temperature_chamber_port_identity_confirmed": False,
                    "real_com_opened": False,
                    "query_only_probe_executed": False,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    if not args.config:
        parser.error("--config is required unless --self-check-import-only is used")
    raw_cfg = load_json_mapping(args.config)
    summary = write_query_only_real_com_probe_artifacts(
        raw_cfg,
        output_dir=args.output_dir,
        config_path=args.config,
        cli_allow=bool(args.allow_v2_query_only_real_com),
        env={QUERY_ONLY_REAL_COM_ENV_VAR: "1"} if args.execute_query_only else {},
        operator_confirmation_path=args.operator_confirmation,
        branch=args.branch,
        head=args.head,
        execute_query_only=bool(args.execute_query_only),
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary.get("final_decision") in {"PASS", "ADMISSION_APPROVED"} else 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
