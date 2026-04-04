from __future__ import annotations

from typing import Any


def build_traceability(features: dict[str, Any], **_: Any) -> dict[str, Any]:
    runs_output: list[dict[str, Any]] = []
    for run in features.get("runs", []):
        missing_sections: list[str] = []
        if not run.get("raw_manifest_present"):
            missing_sections.append("manifest")
        if run.get("enrich_qc_status") != "loaded":
            missing_sections.append("qc")
        if int(run.get("enrich_fit_imported_results") or 0) <= 0:
            missing_sections.append("fit")
        if not run.get("postprocess_summary_status"):
            missing_sections.append("postprocess")
        if not run.get("ai_summary_status"):
            missing_sections.append("ai")
        if not run.get("coefficient_report_status"):
            missing_sections.append("coefficient_report")

        runs_output.append(
            {
                "run_id": run.get("run_id"),
                "run_uuid": run.get("run_uuid"),
                "status": run.get("status"),
                "software_version": run.get("software_version"),
                "manifest_present": bool(run.get("raw_manifest_present")),
                "source_points_file": run.get("raw_source_points_file"),
                "manifest_schema_version": run.get("manifest_schema_version"),
                "raw_complete": bool(run.get("sample_count") or 0) and bool(run.get("total_points") or 0),
                "enrich_complete": not missing_sections,
                "ai_summary_status": run.get("ai_summary_status"),
                "postprocess_summary_status": run.get("postprocess_summary_status"),
                "coefficient_report_status": run.get("coefficient_report_status"),
                "missing_sections": missing_sections,
                "skipped_artifacts": list(run.get("skipped_artifacts") or []),
            }
        )
    return {
        "run_count": len(runs_output),
        "runs": runs_output,
    }
