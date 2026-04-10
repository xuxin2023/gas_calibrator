from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

from ..core.artifact_compatibility import (
    PRIMARY_READER_FILENAMES,
    load_or_build_artifact_compatibility_payloads,
    regenerate_artifact_compatibility_sidecars,
)
from ._cli_safety import build_step2_historical_cli_lines


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scan or regenerate Step 2 historical artifact compatibility sidecars without rewriting primary evidence.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("scan", "export-summary", "regenerate", "reindex"):
        subparser = subparsers.add_parser(command)
        _add_target_arguments(subparser)
        subparser.add_argument("--dry-run", action="store_true", help="Preview only; do not write sidecars.")
        subparser.add_argument(
            "--output",
            type=str,
            default=None,
            help="Optional JSON output path. Required for export-summary.",
        )
    return parser


def _add_target_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--run-dir",
        action="append",
        default=[],
        help="Explicit run directory to scan/regenerate.",
    )
    parser.add_argument(
        "--root-dir",
        action="append",
        default=[],
        help="Root directory to recursively discover historical runs.",
    )


def _discover_run_dirs(root_dir: Path) -> list[Path]:
    root_dir = Path(root_dir).resolve()
    if not root_dir.exists():
        return []
    discovered: dict[str, Path] = {}
    for filename in PRIMARY_READER_FILENAMES:
        for candidate in root_dir.rglob(filename):
            if candidate.is_file():
                resolved = candidate.parent.resolve()
                discovered[str(resolved)] = resolved
    return [discovered[key] for key in sorted(discovered)]


def _collect_run_dirs(*, run_dirs: list[str], root_dirs: list[str]) -> list[Path]:
    collected: dict[str, Path] = {}
    for raw_path in list(run_dirs or []):
        candidate = Path(raw_path).resolve()
        if candidate.exists() and candidate.is_dir():
            collected[str(candidate)] = candidate
    for raw_root in list(root_dirs or []):
        for candidate in _discover_run_dirs(Path(raw_root)):
            collected[str(candidate)] = candidate
    return [collected[key] for key in sorted(collected)]


def _summarize_counts(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(dict(row or {}).get(key) or "").strip() or "missing"
        counts[value] = int(counts.get(value, 0) or 0) + 1
    return counts


def _build_run_report(
    run_dir: Path,
    *,
    operation: str,
    dry_run: bool,
) -> dict[str, Any]:
    run_dir = Path(run_dir).resolve()
    written_paths: dict[str, Any] = {}
    if operation in {"regenerate", "reindex"} and not dry_run:
        regenerate_payload = regenerate_artifact_compatibility_sidecars(run_dir)
        compatibility_scan_summary = dict(regenerate_payload.get("compatibility_scan_summary") or {})
        written_paths = dict(regenerate_payload.get("written_paths") or {})
    else:
        compatibility_payloads = load_or_build_artifact_compatibility_payloads(run_dir)
        compatibility_scan_summary = dict(compatibility_payloads.get("compatibility_scan_summary") or {})
    compatibility_overview = dict(compatibility_scan_summary.get("compatibility_overview") or {})
    current_reader_mode = str(
        compatibility_overview.get("current_reader_mode")
        or compatibility_scan_summary.get("current_reader_mode")
        or ""
    ).strip()
    return {
        "run_dir": str(run_dir),
        "operation": operation,
        "dry_run": bool(dry_run),
        "summary": str(compatibility_scan_summary.get("summary") or compatibility_overview.get("summary") or ""),
        "current_reader_mode": current_reader_mode,
        "current_reader_mode_display": str(
            compatibility_overview.get("current_reader_mode_display")
            or compatibility_scan_summary.get("current_reader_mode_display")
            or current_reader_mode
        ),
        "compatibility_status": str(
            compatibility_overview.get("compatibility_status")
            or compatibility_scan_summary.get("compatibility_status")
            or ""
        ),
        "compatibility_status_display": str(
            compatibility_overview.get("compatibility_status_display")
            or compatibility_scan_summary.get("compatibility_status_display")
            or ""
        ),
        "canonical_direct": current_reader_mode == "canonical_direct",
        "compatibility_adapter": current_reader_mode == "compatibility_adapter",
        "observed_contract_versions": list(compatibility_overview.get("observed_contract_versions") or []),
        "observed_contract_version_summary": str(
            compatibility_overview.get("observed_contract_version_summary")
            or compatibility_scan_summary.get("schema_or_contract_version_summary")
            or "--"
        ),
        "schema_contract_summary": str(compatibility_overview.get("schema_contract_summary_display") or ""),
        "regenerate_recommended": bool(
            compatibility_overview.get(
                "regenerate_recommended",
                compatibility_scan_summary.get("regenerate_recommended", False),
            )
        ),
        "regenerate_scope": str(
            compatibility_overview.get("regenerate_scope")
            or compatibility_scan_summary.get("regenerate_scope")
            or "reviewer_index_sidecar_only"
        ),
        "primary_evidence_rewritten": False,
        "non_primary_boundary": str(compatibility_overview.get("non_primary_boundary_display") or ""),
        "non_primary_chain": str(compatibility_overview.get("non_primary_chain_display") or ""),
        "boundary_digest": str(compatibility_overview.get("boundary_digest") or ""),
        "non_claim_digest": str(compatibility_overview.get("non_claim_digest") or ""),
        "written_paths": written_paths,
    }


def _build_operation_report(
    *,
    command: str,
    run_dirs: list[Path],
    dry_run: bool,
) -> dict[str, Any]:
    runs = [
        _build_run_report(run_dir, operation=command, dry_run=dry_run)
        for run_dir in run_dirs
    ]
    return {
        "operation": command,
        "run_count": len(runs),
        "target_mode": "batch" if len(runs) != 1 else "single",
        "dry_run": bool(dry_run),
        "primary_evidence_rewritten": False,
        "reader_mode_counts": _summarize_counts(runs, "current_reader_mode"),
        "compatibility_status_counts": _summarize_counts(runs, "compatibility_status"),
        "regenerate_recommended_count": sum(
            1 for row in runs if bool(row.get("regenerate_recommended", False))
        ),
        "runs": runs,
    }


def _emit_json_report(report: dict[str, Any], *, output_path: str | None) -> None:
    if output_path:
        destination = Path(output_path).resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"[historical-artifacts] summary_path={destination}", flush=True)
        return
    print(json.dumps(report, ensure_ascii=False), flush=True)


def main(argv: list[str] | None = None) -> int:
    args = create_argument_parser().parse_args(argv)
    run_dirs = _collect_run_dirs(
        run_dirs=list(args.run_dir or []),
        root_dirs=list(args.root_dir or []),
    )
    if not run_dirs:
        raise SystemExit("No historical run directories found. Provide --run-dir or --root-dir.")
    if args.command == "export-summary" and not args.output:
        raise SystemExit("--output is required for export-summary")
    for line in build_step2_historical_cli_lines(
        operation=str(args.command),
        run_count=len(run_dirs),
        dry_run=bool(args.dry_run),
    ):
        print(line, flush=True)
    report = _build_operation_report(
        command=str(args.command),
        run_dirs=run_dirs,
        dry_run=bool(args.dry_run),
    )
    _emit_json_report(report, output_path=str(args.output) if args.output else None)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
