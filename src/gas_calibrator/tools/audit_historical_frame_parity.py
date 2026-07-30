from __future__ import annotations

import argparse
from pathlib import Path

from gas_calibrator.validation.historical_frame_parity_audit import (
    DEFAULT_CATALOG_PATH,
    DEFAULT_OBSERVED_FIXTURE_PATH,
    audit_historical_frames,
    write_historical_frame_parity_artifacts,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only 0620/0621 frame parity audit"
    )
    parser.add_argument("--catalog", default=str(DEFAULT_CATALOG_PATH))
    parser.add_argument("--fixture", default=str(DEFAULT_OBSERVED_FIXTURE_PATH))
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args(argv)

    result = audit_historical_frames(
        catalog_path=Path(args.catalog),
        observed_fixture_path=Path(args.fixture),
    )
    artifacts = write_historical_frame_parity_artifacts(result, Path(args.output_dir))
    for role, path in artifacts.items():
        print(f"{role}={path}")
    return 0 if result.get("status") == "PASS" else 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
