"""Export a synthetic-only V1.5 component-QC reference evaluation."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable

from ..validation.v1_5_component_qc_reference_evaluator import (
    evaluate_v1_5_component_qc_reference_fixture,
    write_v1_5_component_qc_reference_evaluation,
)


def _read_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--fixture-json-path", required=True)
    parser.add_argument("--contract-json-path", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        model = evaluate_v1_5_component_qc_reference_fixture(
            _read_json(args.fixture_json_path),
            _read_json(args.contract_json_path),
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(json.dumps({"overall_status": "blocked", "error": str(exc)}, ensure_ascii=False))
        return 2
    outputs = write_v1_5_component_qc_reference_evaluation(model, args.output_dir)
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "outputs": {key: str(value) for key, value in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
