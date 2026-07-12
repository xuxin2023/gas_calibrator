"""Export an offline discovery audit for historical mature V1.5 roots."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_historical_mature_root_discovery import (
    build_v1_5_historical_mature_root_discovery,
    write_v1_5_historical_mature_root_discovery,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--summary-path", action="append", default=[])
    parser.add_argument("--summary-list-path", default="")
    parser.add_argument("--algorithm-profile-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-if-no-candidate", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    paths = list(args.summary_path)
    if args.summary_list_path:
        paths.extend(
            line.strip()
            for line in Path(args.summary_list_path).read_text(encoding="utf-8-sig").splitlines()
            if line.strip()
        )
    model = build_v1_5_historical_mature_root_discovery(
        queue_summary_paths=paths,
        algorithm_profile_path=args.algorithm_profile_path,
    )
    outputs = write_v1_5_historical_mature_root_discovery(model, args.output_dir)
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "summary_count": model["summary_count"],
                "attestation_input_candidate_count": model["attestation_input_candidate_count"],
                "classification_counts": model["classification_counts"],
                "outputs": {key: str(value) for key, value in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 2 if args.fail_if_no_candidate and not model["attestation_input_candidate_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
