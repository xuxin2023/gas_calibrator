"""Export an offline catalog of legacy V1.5 point evidence."""

from __future__ import annotations

import argparse
import json
from typing import Iterable

from ..validation.v1_5_legacy_historical_evidence_catalog import (
    build_v1_5_legacy_historical_evidence_catalog,
    write_v1_5_legacy_historical_evidence_catalog,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--search-root", action="append", default=[])
    parser.add_argument("--accepted-manifest", action="append", default=[])
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_legacy_historical_evidence_catalog(
        search_roots=args.search_root,
        accepted_manifest_paths=args.accepted_manifest,
    )
    outputs = write_v1_5_legacy_historical_evidence_catalog(model, args.output_dir)
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "point_count": model["point_count"],
                "co2_point_count": model["co2_point_count"],
                "h2o_point_count": model["h2o_point_count"],
                "accepted_composite_member_count": model["accepted_composite_member_count"],
                "outputs": {key: str(value) for key, value in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
