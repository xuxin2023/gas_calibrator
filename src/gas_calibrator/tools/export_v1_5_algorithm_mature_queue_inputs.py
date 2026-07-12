"""Export locked queue inputs for the mature V1.5 CO2/H2O runners."""

from __future__ import annotations

import argparse
import json
from typing import Iterable

from ..validation.v1_5_algorithm_mature_queue_inputs import (
    EXPECTED_COUNTS,
    write_v1_5_algorithm_mature_queue_inputs,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--profile-path", required=True)
    parser.add_argument("--profile-id", choices=tuple(EXPECTED_COUNTS), required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = write_v1_5_algorithm_mature_queue_inputs(
        profile_path=args.profile_path,
        profile_id=args.profile_id,
        output_dir=args.output_dir,
    )
    print(json.dumps(model, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
