"""Export an offline V1.5 component-QC authority audit."""

from __future__ import annotations

import argparse
import json
from typing import Iterable

from ..validation.v1_5_component_qc_authority_audit import (
    build_v1_5_component_qc_authority_audit,
    write_v1_5_component_qc_authority_audit,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--polluted-root", required=True)
    parser.add_argument("--p2-design-json-path", required=True)
    parser.add_argument("--legacy-catalog-json-path", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_component_qc_authority_audit(
        repo_root=args.repo_root,
        polluted_root=args.polluted_root,
        p2_design_json_path=args.p2_design_json_path,
        legacy_catalog_json_path=args.legacy_catalog_json_path,
    )
    outputs = write_v1_5_component_qc_authority_audit(model, args.output_dir)
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "authority_gap_codes": model.get("authority_gap_codes", []),
                "historical_component_qc_artifact_count": model.get(
                    "historical_component_qc_artifact_count", 0
                ),
                "outputs": {key: str(value) for key, value in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 2 if model["overall_status"] == "blocked_invalid_upstream_evidence" else 0


if __name__ == "__main__":
    raise SystemExit(main())
