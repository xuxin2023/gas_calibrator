from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable

from ..core.certificate_evidence_census import (
    DEFAULT_CONTRACT_PATH,
    load_certificate_evidence_census_contract,
    scan_certificate_evidence,
    write_certificate_evidence_census_artifacts,
)


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "GA-D6A read-only certificate-evidence census. The command only inventories "
            "source files and writes derived reports to --output-dir."
        )
    )
    parser.add_argument(
        "--root",
        action="append",
        required=True,
        help="Directory to scan; repeat for multiple roots.",
    )
    parser.add_argument("--output-dir", required=True, help="Derived report directory.")
    parser.add_argument("--contract", default=str(DEFAULT_CONTRACT_PATH))
    parser.add_argument(
        "--seed-manifest",
        default=None,
        help=(
            "Optional local JSON manifest of known legacy evidence paths and roles. "
            "The manifest is read-only and should remain outside the repository."
        ),
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    contract = load_certificate_evidence_census_contract(args.contract)
    seeded_candidates = []
    if args.seed_manifest:
        seed_payload = json.loads(Path(args.seed_manifest).read_text(encoding="utf-8"))
        if seed_payload.get("schema_version") != "certificate_evidence_seed_manifest_v1":
            raise ValueError("unexpected certificate evidence seed manifest schema")
        seeded_candidates = list(seed_payload.get("candidates") or [])
    roots = [Path(item).resolve() for item in args.root]
    output_dir = Path(args.output_dir).resolve()
    print("GA-D6A boundary: read-only source census; no device/database/coefficient writes")
    print("scan_roots:")
    for root in roots:
        print(f"  - {root}")
    result = scan_certificate_evidence(
        roots,
        contract=contract,
        seeded_candidates=seeded_candidates,
    )
    if args.seed_manifest:
        result["seed_manifest_source"] = str(Path(args.seed_manifest).resolve())
    artifacts = write_certificate_evidence_census_artifacts(result, output_dir=output_dir)
    print(f"status: {result['status']}")
    print(f"candidate_count: {result['candidate_count']}")
    print(f"scan_roots_complete: {result['scan_roots_complete']}")
    for role, item in result["role_summary"].items():
        print(f"role[{role}]: {item['census_state']} ({item['candidate_document_count']})")
    for key, path in artifacts.items():
        print(f"{key}: {path}")
    return 0 if result["scan_roots_complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
