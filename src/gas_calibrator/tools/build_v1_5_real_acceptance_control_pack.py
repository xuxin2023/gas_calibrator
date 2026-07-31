"""Build the offline V1.5 real-acceptance control pack."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_real_acceptance_control_pack import (
    build_v1_5_real_acceptance_control_pack,
    build_v1_5_real_acceptance_site_profile_template,
    prefill_v1_5_site_profile_from_historical_identity,
    write_v1_5_real_acceptance_control_pack_outputs,
)


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a no-COM/no-write V1.5 site profile and real-acceptance evidence control pack."
    )
    parser.add_argument("--runtime-port-inventory-json", required=True)
    parser.add_argument("--certificate-registry-json", required=True)
    parser.add_argument("--certificate-reconciliation-json", required=True)
    parser.add_argument("--certificate-admission-json", required=True)
    parser.add_argument("--workstation-dry-run-json", required=True)
    parser.add_argument("--site-profile-json")
    parser.add_argument("--historical-identity-csv")
    parser.add_argument("--historical-runtime-config-json")
    parser.add_argument("--readonly-com-executor-json")
    parser.add_argument("--formal-archive-closure-json")
    parser.add_argument("--reported-connected-count", type=int, default=4)
    parser.add_argument("--reported-powered-count", type=int, default=2)
    parser.add_argument("--observation-id", default="operator_report_unverified")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-blocked", action="store_true")
    forbidden = parser.add_argument_group("forbidden execution options")
    forbidden.add_argument("--execute", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--execute-read-only-real-com", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--execute-controlled-writes", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.execute or args.execute_read_only_real_com or args.execute_controlled_writes:
        print("error: control-pack builder is offline only and rejects execution flags", file=sys.stderr)
        return 2
    try:
        if args.site_profile_json:
            site_profile = json.loads(Path(args.site_profile_json).read_text(encoding="utf-8-sig"))
        else:
            site_profile = build_v1_5_real_acceptance_site_profile_template(
                runtime_port_inventory_json=args.runtime_port_inventory_json,
                reported_connected_count=args.reported_connected_count,
                reported_powered_count=args.reported_powered_count,
                observation_id=args.observation_id,
            )
        historical_sources = (
            args.historical_identity_csv,
            args.historical_runtime_config_json,
        )
        if any(historical_sources) and not all(historical_sources):
            raise ValueError(
                "historical identity prefill requires both "
                "--historical-identity-csv and --historical-runtime-config-json"
            )
        if all(historical_sources):
            site_profile = prefill_v1_5_site_profile_from_historical_identity(
                site_profile=site_profile,
                historical_identity_csv=args.historical_identity_csv,
                historical_runtime_config_json=args.historical_runtime_config_json,
            )
        model = build_v1_5_real_acceptance_control_pack(
            runtime_port_inventory_json=args.runtime_port_inventory_json,
            certificate_registry_json=args.certificate_registry_json,
            certificate_reconciliation_json=args.certificate_reconciliation_json,
            certificate_admission_json=args.certificate_admission_json,
            workstation_dry_run_json=args.workstation_dry_run_json,
            site_profile=site_profile,
            readonly_com_executor_json=args.readonly_com_executor_json,
            formal_archive_closure_json=args.formal_archive_closure_json,
        )
        outputs = write_v1_5_real_acceptance_control_pack_outputs(
            model=model,
            site_profile=site_profile,
            output_dir=args.output_dir,
        )
    except Exception as exc:
        print(f"V1.5 real-acceptance control-pack build failed: {exc}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "lifecycle_status": model["lifecycle_status"],
                "blocker_count": model["blocker_count"],
                "opens_com_ports": model["opens_com_ports"],
                "writes_coefficients": model["writes_coefficients"],
                "historical_identity_prefill": site_profile.get(
                    "historical_identity_prefill"
                ),
                "outputs": {key: str(path.resolve()) for key, path in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )
    return 3 if args.fail_on_blocked and model["blocker_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
