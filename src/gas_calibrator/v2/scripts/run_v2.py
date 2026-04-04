from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Any, Optional

from gas_calibrator.v2.entry import create_calibration_service


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Gas Calibrator V2 launcher")
    parser.add_argument("--config", type=str, default=None, help="Path to V2 JSON config")
    parser.add_argument("--simulation", action="store_true", help="Force simulation mode")
    parser.add_argument("--headless", action="store_true", help="Run V2 without starting the UI")
    parser.add_argument(
        "--allow-unsafe-step2-config",
        action="store_true",
        help="Allow a non-default Step 2 config only when the matching environment unlock is also set.",
    )
    return parser


def _run_ui(argv: Optional[list[str]] = None) -> int:
    from gas_calibrator.v2.ui_v2.app import main as ui_main

    return int(ui_main(argv))


def _headless_config_safety(service: Any) -> dict[str, Any]:
    return dict(getattr(getattr(service, "config", None), "_config_safety", {}) or {})


def _emit_headless_config_safety(service: Any) -> None:
    config_safety = _headless_config_safety(service)
    if not config_safety:
        return
    execution_gate = dict(config_safety.get("execution_gate") or {})
    for line in list(config_safety.get("review_lines") or []):
        text = str(line or "").strip()
        if text:
            print(f"[Step2 config safety] {text}", flush=True)
    summary = str(execution_gate.get("summary") or "").strip()
    if summary:
        print(f"[Step2 execution gate] {summary}", flush=True)


def main(argv: Optional[list[str]] = None) -> int:
    args = create_argument_parser().parse_args(argv)
    if args.headless:
        if not args.config:
            raise SystemExit("--config is required in --headless mode")
        service = create_calibration_service(
            config_path=str(Path(args.config)),
            simulation_mode=bool(args.simulation),
            allow_unsafe_step2_config=bool(args.allow_unsafe_step2_config),
        )
        _emit_headless_config_safety(service)
        service.run()
        return 0

    ui_args: list[str] = []
    if args.config:
        ui_args.extend(["--config", str(Path(args.config))])
    if args.simulation:
        ui_args.append("--simulation")
    if args.allow_unsafe_step2_config:
        ui_args.append("--allow-unsafe-step2-config")
    return _run_ui(ui_args)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
