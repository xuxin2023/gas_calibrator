"""Export the offline V1.5 production component-QC and 0613 fit matrix."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from ..validation.v1_5_production_component_qc_fit_matrix import (
    build_v1_5_production_component_qc_fit_matrix,
    write_v1_5_production_component_qc_fit_matrix,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preflight-json", required=True, type=Path)
    parser.add_argument("--contract-json", required=True, type=Path)
    parser.add_argument("--legacy-catalog-json", required=True, type=Path)
    parser.add_argument("--mature-root-discovery-json", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    model = build_v1_5_production_component_qc_fit_matrix(
        preflight_json=args.preflight_json,
        contract_json=args.contract_json,
        legacy_catalog_json=args.legacy_catalog_json,
        mature_root_discovery_json=args.mature_root_discovery_json,
    )
    write_v1_5_production_component_qc_fit_matrix(model, args.output_dir)
    return 0 if model["production_component_qc_evaluation_complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
