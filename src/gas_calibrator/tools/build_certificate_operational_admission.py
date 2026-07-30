from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

from gas_calibrator.validation.certificate_operational_admission import (
    DEFAULT_CONTRACT_PATH,
    DEFAULT_EVIDENCE_PATH,
    evaluate_certificate_operational_admission,
    load_certificate_operational_admission_contract,
    load_owner_attested_certificate_evidence,
    verify_documentary_files,
    write_certificate_operational_admission_artifacts,
)


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "GA-D6B verifies owner-attested local documentary evidence and evaluates "
            "the offline-program operational certificate gate. It never authorizes "
            "real execution or real acceptance."
        )
    )
    parser.add_argument(
        "--evidence-root",
        action="append",
        required=True,
        help="Read-only evidence directory; repeat for multiple roots.",
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--contract", default=str(DEFAULT_CONTRACT_PATH))
    parser.add_argument("--evidence", default=str(DEFAULT_EVIDENCE_PATH))
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    contract = load_certificate_operational_admission_contract(args.contract)
    evidence = load_owner_attested_certificate_evidence(args.evidence)
    roots = [Path(item).resolve() for item in args.evidence_root]
    verification = verify_documentary_files(evidence, roots=roots)
    result = evaluate_certificate_operational_admission(
        evidence,
        contract=contract,
        source_verification=verification,
    )
    artifacts = write_certificate_operational_admission_artifacts(
        result,
        output_dir=Path(args.output_dir),
    )

    print(
        "GA-D6B boundary: offline program progress only; "
        "no device/database/coefficient writes"
    )
    print(f"status: {result['status']}")
    print(
        "operational_certificate_gate_passed: "
        f"{result['operational_certificate_gate_passed']}"
    )
    print("strict_original_certificate_gate_passed: False")
    print("ready_for_real_execution: False")
    for role, path in artifacts.items():
        print(f"{role}: {path}")
    return 0 if result["operational_certificate_gate_passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
