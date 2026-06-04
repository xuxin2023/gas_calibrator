"""Prepare a complete offline V1.5 formal-run evidence package skeleton."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.formal_contracts import write_contract_templates
from ..validation.formal_evidence_run import prepare_formal_evidence_run


RUNBOOK_TEXT = """# V1.5 Formal No-Write Runbook

This runbook is sidecar-only. It does not open COM ports, control water/gas
routes, control valves or PACE, or write SENCO coefficients.

## 1. Fill Evidence Templates

- Fill `standard_gases_template.json` and save the reviewed copy as
  `standard_gases.json`.
- Fill `com22_pressure_reference_template.json` and save the reviewed copy as
  `com22_pressure_reference.json`.
- Keep `released_uncertainty_inputs_template.json` as not released until all
  GUM budget inputs are reviewed.
- If this package was first created from placeholders, regenerate
  `formal_plan_snapshot.json`, `com22_pressure_reference.json`, and
  `evidence_run_manifest.json` with the reviewed JSON files before sampling.

## 2. Prepare Formal Evidence Snapshot

Use `formal_plan_snapshot.json`, `com22_pressure_reference.json`, and
`evidence_run_manifest.json` as the immutable run-start evidence.

## 3. Run Order

1. Device precheck.
2. Pressure-channel quick check at current atmosphere.
3. Open-flow CO2/H2O main calibration sampling.
4. Offline preflight and formal evidence sidecar.
5. Generate reports from `evidence_bundle.json`.
6. Optional PostgreSQL import.

## 4. Formal Boundaries

- Open-flow current-atmosphere CO2/H2O is the formal main calibration scope.
- Sealed pressure points, dynamic PACE control, ACT + sink bias, and VENT-hold
  are engineering diagnostics by default.
- Missing pressure quick-check blocks formal review.
- Missing released uncertainty keeps reports in `draft_only`.
- Missing reviewer/approver keeps reports in `review_ready`.
- Any unaudited coefficient write event is `not_releasable`.

## 5. Commands

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.prepare_v1_5_formal_run_package `
  --output-dir <formal_run_package_dir> `
  --operator <operator-name> `
  --analyzer-id <analyzer-id> `
  --run-id <planned-run-id> `
  --config <v1_5_no_write_runtime_config.json> `
  --standard-gases-json <standard_gases.json> `
  --pressure-reference-json <com22_pressure_reference.json>
```

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.validate_pressure_only `
  --config <v1_5_no_write_runtime_config.json> `
  --output-dir <existing_v1_5_run_dir_parent> `
  --run-id <planned-run-id> `
  --pressure-reference-json <com22_pressure_reference.json> `
  --pressure-points ambient `
  --count 10 `
  --interval-s 1 `
  --continuous-atmosphere-hold `
  --require-continuous-atmosphere-hold `
  --no-prompt
```

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.run_v1_5_formal_evidence_sidecar `
  --run-dir <existing_v1_5_run_dir> `
  --plan-json <formal_plan_snapshot.json> `
  --pressure-reference-json <com22_pressure_reference.json> `
  --config <v1_5_no_write_runtime_config.json>
```

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.export_v1_5_calibration_reports `
  --evidence-bundle-json <evidence_bundle.json> `
  --output-dir <report_output_dir> `
  --uncertainty-json <released_uncertainty_inputs.json>
```
"""


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write V1.5 formal run templates, plan snapshot, manifest, and runbook."
    )
    parser.add_argument("--output-dir", required=True, help="Directory for the formal run package.")
    parser.add_argument("--operator", default="<operator-name>", help="Operator name for the plan snapshot.")
    parser.add_argument("--analyzer-id", default="<analyzer-under-test-id>", help="Analyzer ID under test.")
    parser.add_argument("--run-id", default=None, help="Optional formal run id.")
    parser.add_argument("--plan-id", default=None, help="Optional plan id.")
    parser.add_argument("--plan-version", default=None, help="Optional plan version.")
    parser.add_argument("--config", default=None, help="Optional runtime config JSON used to compute config_hash.")
    parser.add_argument(
        "--standard-gases-json",
        default=None,
        help="Optional reviewed standard-gases JSON. If omitted, the fill-in template is used.",
    )
    parser.add_argument(
        "--pressure-reference-json",
        default=None,
        help="Optional reviewed COM22 pressure-reference JSON. If omitted, the fill-in template is used.",
    )
    parser.add_argument("--lab", default="<lab-name>", help="Optional lab name.")
    parser.add_argument("--ambient-temperature-c", default=None, help="Optional ambient temperature.")
    parser.add_argument("--ambient-rh-pct", default=None, help="Optional ambient RH.")
    return parser.parse_args(list(argv) if argv is not None else None)


def prepare_formal_run_package(
    *,
    output_dir: str | Path,
    operator: str = "<operator-name>",
    analyzer_id: str = "<analyzer-under-test-id>",
    run_id: str | None = None,
    plan_id: str | None = None,
    plan_version: str | None = None,
    config_path: str | Path | None = None,
    standard_gases_json: str | Path | None = None,
    pressure_reference_json: str | Path | None = None,
    lab: str = "<lab-name>",
    ambient_temperature_c: str | None = None,
    ambient_rh_pct: str | None = None,
) -> dict[str, Path]:
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    templates = write_contract_templates(root)
    snapshots = prepare_formal_evidence_run(
        output_dir=root,
        operator=operator,
        analyzer_id=analyzer_id,
        run_id=run_id,
        plan_id=plan_id,
        plan_version=plan_version,
        config_path=config_path,
        standard_gases_json=standard_gases_json or templates["standard_gases_template"],
        pressure_reference_json=pressure_reference_json or templates["pressure_reference_template"],
        lab=lab,
        ambient_temperature_c=ambient_temperature_c,
        ambient_rh_pct=ambient_rh_pct,
    )
    runbook_path = root / "v1_5_formal_no_write_runbook.md"
    runbook_path.write_text(RUNBOOK_TEXT, encoding="utf-8")
    outputs: dict[str, Path] = {}
    outputs.update(templates)
    outputs.update(snapshots)
    outputs["runbook"] = runbook_path
    return outputs


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = prepare_formal_run_package(
            output_dir=args.output_dir,
            operator=args.operator,
            analyzer_id=args.analyzer_id,
            run_id=args.run_id,
            plan_id=args.plan_id,
            plan_version=args.plan_version,
            config_path=args.config,
            standard_gases_json=args.standard_gases_json,
            pressure_reference_json=args.pressure_reference_json,
            lab=args.lab,
            ambient_temperature_c=args.ambient_temperature_c,
            ambient_rh_pct=args.ambient_rh_pct,
        )
    except Exception as exc:
        print(f"Prepare V1.5 formal run package failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(value) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
