"""Export an offline task plan for legacy V1.5 evidence gaps."""

from __future__ import annotations

import argparse
import json
from typing import Iterable

from ..validation.v1_5_legacy_evidence_gap_task_plan import (
    build_v1_5_legacy_evidence_gap_task_plan,
    write_v1_5_legacy_evidence_gap_task_plan,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--catalog-json-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-integrity-mismatch", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_legacy_evidence_gap_task_plan(
        catalog_json_path=args.catalog_json_path
    )
    outputs = write_v1_5_legacy_evidence_gap_task_plan(model, args.output_dir)
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "task_count": model["task_count"],
                "artifact_integrity_mismatch_count": model.get(
                    "artifact_integrity_mismatch_count", 0
                ),
                "priority_counts": model.get("priority_counts") or {},
                "outputs": {key: str(value) for key, value in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if model["overall_status"] == "blocked_invalid_catalog":
        return 2
    if args.fail_on_integrity_mismatch and model.get("artifact_integrity_mismatch_count"):
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
