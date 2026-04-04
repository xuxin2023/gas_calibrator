from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Iterable, Optional

from ..core.offline_artifacts import export_run_offline_artifacts, export_suite_offline_artifacts


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild offline acceptance/analytics/lineage artifacts.")
    parser.add_argument("--run-dir", default=None, help="Run directory containing summary/manifest/results.")
    parser.add_argument("--suite-dir", default=None, help="Suite directory containing suite_summary.json.")
    return parser.parse_args(list(argv) if argv is not None else None)


def _load_json(path: Path) -> dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(f"required artifact missing: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _objectify(value):
    if isinstance(value, dict):
        return SimpleNamespace(**{key: _objectify(item) for key, item in value.items()})
    if isinstance(value, list):
        return [_objectify(item) for item in value]
    return value


def rebuild_run(run_dir: Path) -> dict[str, object]:
    for name in ("summary.json", "manifest.json", "results.json"):
        if not (run_dir / name).exists():
            raise FileNotFoundError(
                f"{run_dir} is not a formal V2 run directory. Missing {name}. "
                "Use a run directory that contains summary.json, manifest.json, and results.json."
            )
    summary = _load_json(run_dir / "summary.json")
    manifest = _load_json(run_dir / "manifest.json")
    results = _load_json(run_dir / "results.json")
    session = SimpleNamespace(
        run_id=str(summary.get("run_id") or manifest.get("run_id") or run_dir.name),
        config=_objectify(dict(manifest.get("config_snapshot") or {})),
    )
    return export_run_offline_artifacts(
        run_dir=run_dir,
        output_dir=run_dir.parent,
        run_id=str(session.run_id),
        session=session,
        samples=[_objectify(item) for item in list(results.get("samples") or [])],
        point_summaries=[dict(item) for item in list(results.get("point_summaries") or [])],
        output_files=list((summary.get("stats") or {}).get("output_files") or []),
        export_statuses=dict((summary.get("stats") or {}).get("artifact_exports") or {}),
        source_points_file=manifest.get("source_points_file"),
        software_build_id=str(manifest.get("software_build_id") or summary.get("software_build_id") or ""),
        config_safety=dict(summary.get("config_safety") or (summary.get("stats") or {}).get("config_safety") or {}),
        config_safety_review=dict(
            summary.get("config_safety_review") or (summary.get("stats") or {}).get("config_safety_review") or {}
        ),
    )


def rebuild_suite(suite_dir: Path) -> dict[str, object]:
    if not (suite_dir / "suite_summary.json").exists():
        raise FileNotFoundError(
            f"{suite_dir} is not a suite directory. Missing suite_summary.json."
        )
    summary = _load_json(suite_dir / "suite_summary.json")
    return export_suite_offline_artifacts(suite_dir=suite_dir, summary=summary)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    if bool(args.run_dir) == bool(args.suite_dir):
        print("Provide exactly one of --run-dir or --suite-dir.", file=sys.stderr)
        return 2
    try:
        if args.run_dir:
            payload = rebuild_run(Path(str(args.run_dir)).resolve())
            print(f"acceptance_plan: {Path(args.run_dir).resolve() / 'acceptance_plan.json'}")
            print(f"analytics_summary: {Path(args.run_dir).resolve() / 'analytics_summary.json'}")
            print(f"lineage_summary: {Path(args.run_dir).resolve() / 'lineage_summary.json'}")
            print(f"trend_registry: {Path(args.run_dir).resolve() / 'trend_registry.json'}")
            print(f"evidence_registry: {Path(args.run_dir).resolve() / 'evidence_registry.json'}")
            print(f"coefficient_registry: {Path(args.run_dir).resolve() / 'coefficient_registry.json'}")
            return 0 if payload else 1
        payload = rebuild_suite(Path(str(args.suite_dir)).resolve())
        print(f"suite_analytics_summary: {Path(args.suite_dir).resolve() / 'suite_analytics_summary.json'}")
        print(f"suite_acceptance_plan: {Path(args.suite_dir).resolve() / 'suite_acceptance_plan.json'}")
        print(f"suite_evidence_registry: {Path(args.suite_dir).resolve() / 'suite_evidence_registry.json'}")
        return 0 if payload else 1
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
