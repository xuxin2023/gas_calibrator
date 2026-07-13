"""Export the offline V1.5 component-QC generator contract review."""

from __future__ import annotations

import argparse
import json
from typing import Iterable

from ..validation.v1_5_component_qc_generator_contract import (
    build_v1_5_component_qc_generator_contract_review,
    write_v1_5_component_qc_generator_contract_review,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--authority-audit-json-path", required=True)
    parser.add_argument("--contract-json-path", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_component_qc_generator_contract_review(
        authority_audit_json_path=args.authority_audit_json_path,
        contract_json_path=args.contract_json_path,
    )
    outputs = write_v1_5_component_qc_generator_contract_review(model, args.output_dir)
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "blocker_codes": model["blocker_codes"],
                "outputs": {key: str(value) for key, value in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 2 if model["blocker_codes"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
