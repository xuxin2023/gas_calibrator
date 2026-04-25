from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Optional


def _bootstrap_src_path_for_direct_script() -> None:
    src_root = Path(__file__).resolve().parents[3]
    src_root_text = str(src_root)
    if src_root_text not in sys.path:
        sys.path.insert(0, src_root_text)


_bootstrap_src_path_for_direct_script()

from gas_calibrator.v2.core.run001_a1_dry_run import (  # noqa: E402
    RUN001_FAIL,
    build_run001_a1_evidence_payload,
    load_point_rows,
    write_run001_a1_artifacts,
)
from gas_calibrator.v2.entry import create_calibration_service, load_config_bundle  # noqa: E402


DEFAULT_CONFIG = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "validation"
    / "run001_a1_co2_only_skip0_no_write_real_machine_dry_run.json"
)


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run-001/A1 CO2-only no-write dry-run entry")
    parser.add_argument("--config", type=str, default=str(DEFAULT_CONFIG), help="Run-001/A1 JSON config")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="",
        help="Preflight evidence output directory. Defaults to config output_dir/run001_a1_preflight.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute the V2 real-machine dry-run after preflight. Never used by automated tests.",
    )
    parser.add_argument(
        "--confirm-real-machine-no-write",
        action="store_true",
        help="Required with --execute to confirm a human operator accepted no-write dry-run scope.",
    )
    parser.add_argument(
        "--allow-unsafe-step2-config",
        action="store_true",
        help="Rejected for Run-001/A1; this path uses the dedicated no-write safety gate instead.",
    )
    return parser


def _default_output_dir(raw_cfg: dict, config_path: str) -> Path:
    if raw_cfg.get("paths") and isinstance(raw_cfg["paths"], dict):
        output = str(raw_cfg["paths"].get("output_dir", "") or "").strip()
        if output:
            candidate = Path(output)
            if not candidate.is_absolute():
                candidate = Path(config_path).resolve().parent / candidate
            return candidate.resolve() / "run001_a1_preflight"
    return Path(config_path).resolve().parent / "output" / "run001_a1_preflight"


def _write_preflight(config_path: str, output_dir: Optional[str]) -> tuple[dict, dict[str, str]]:
    resolved_config_path, raw_cfg, _config = load_config_bundle(
        config_path,
        simulation_mode=False,
        allow_unsafe_step2_config=False,
        enforce_step2_execution_gate=False,
    )
    target_dir = Path(output_dir).expanduser().resolve() if output_dir else _default_output_dir(raw_cfg, resolved_config_path)
    point_rows = load_point_rows(resolved_config_path, raw_cfg)
    payload = build_run001_a1_evidence_payload(
        raw_cfg,
        config_path=resolved_config_path,
        run_dir=target_dir,
        point_rows=point_rows,
        require_runtime_artifacts=False,
    )
    written = write_run001_a1_artifacts(target_dir, payload)
    return payload, written


def main(argv: Optional[list[str]] = None) -> int:
    args = create_argument_parser().parse_args(argv)
    payload, written = _write_preflight(args.config, args.output_dir or None)
    print(f"[Run-001/A1] preflight final_decision={payload.get('final_decision')}", flush=True)
    for key, path in sorted(written.items()):
        print(f"[Run-001/A1] artifact {key}={path}", flush=True)
    if payload.get("final_decision") == RUN001_FAIL:
        return 2
    if not args.execute:
        return 0
    if not args.confirm_real_machine_no_write:
        print("[Run-001/A1] --confirm-real-machine-no-write is required with --execute", flush=True)
        return 2
    service = create_calibration_service(
        config_path=args.config,
        simulation_mode=False,
        allow_unsafe_step2_config=False,
        run001_a1_no_write_dry_run_cli_args=args,
    )
    service.run()
    status = service.get_status()
    return 0 if str(getattr(status.phase, "value", status.phase)) == "completed" else 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
