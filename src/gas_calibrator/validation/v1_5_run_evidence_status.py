"""Build an offline V1.5 run evidence-status tree.

The status tree is a reviewer-facing index over existing artifacts. It does
not open COM ports, control valves/routes/PACE, or write analyzer coefficients.
Its purpose is to make the formal V1.5 run state explicit: pressure evidence,
CO2/H2O open-flow evidence, coefficient epochs, post-write verification,
database import, and reports all point back to the same evidence tree.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from ..storage.v1_5_evidence.bundle import bundle_traceability_summary, sha256_file


SCHEMA = "v1_5_run_evidence_status_v1"
SELF_ARTIFACT_NAMES = {"v1_5_run_evidence_status.json", "v1_5_run_evidence_status.md"}


@dataclass(frozen=True)
class EvidenceArtifact:
    role: str
    path: str
    sha256: str
    size_bytes: int
    source: str = "discovered"

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EvidenceStageStatus:
    stage_id: str
    title: str
    status: str
    reason: str
    artifact_roles: tuple[str, ...]
    artifact_count: int
    physical_meaning: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _load_json(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    if not source.exists():
        return {}
    payload = json.loads(source.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _safe_role(value: Any) -> str:
    text = str(value or "").strip()
    return text or "evidence_file"


def _classify_artifact(path: Path) -> str:
    name = path.name.lower()
    parent_parts = {part.lower() for part in path.parts}
    parent_text = "/".join(part.lower() for part in path.parts)
    if name == "v1_5_full_flow_plan.json":
        return "full_flow_plan"
    if name == "v1_5_full_flow_stage_manifest.json":
        return "full_flow_stage_manifest"
    if name == "v1_5_full_flow_stage_manifest.md":
        return "full_flow_stage_manifest_markdown"
    if name == "v1_5_formal_flow_contract.json":
        return "full_flow_contract"
    if name == "runtime_identity_bound_config.json":
        return "runtime_identity_bound_config"
    if (
        "optical_root_cause" in name
        or "光学根因" in path.name
        or "factory_signal_health" in name
        or "status_register" in name
        or "invalid_frame" in name
    ):
        return "diagnostic_analysis"
    if name.startswith("v1_5_recommendation_closure"):
        return "formal_analysis"
    if name in {"v1_5_full_flow_closure_readiness.json", "v1_5_full_flow_closure_readiness.md"}:
        return "full_flow_closure_readiness"
    if name == "v1_5_full_flow_closure_gaps.csv":
        return "full_flow_closure_gaps"
    if name == "v1_5_full_flow_device_closure.csv":
        return "full_flow_device_closure"
    if "post_run_coefficient_executor" in parent_parts:
        if name == "executor_manifest.json":
            return "post_run_coefficient_executor"
        if name == "executor_summary.md":
            return "post_run_coefficient_executor_summary"
        if name in {"executor_stages.csv", "executor_stage_status.csv"}:
            return "post_run_coefficient_executor_stages"
        if name == "device_eligibility.csv":
            return "post_run_device_eligibility"
        if name == "coefficient_execution_plan.csv":
            return "post_run_coefficient_execution_plan"
        if name == "controlled_write_package.csv":
            return "post_run_controlled_write_package"
        if name == "post_write_reverification_plan.csv":
            return "post_run_reverification_plan"
        if name == "archive_gap_list.csv":
            return "post_run_archive_gap_list"
    if "getco" in name or name in {
        "old_component_coefficients_snapshot.json",
        "getco_component_snapshot_identity.csv",
    }:
        return "getco_snapshot"
    if name in {"formal_plan_snapshot.json", "formal_plan.json"}:
        return "formal_plan_snapshot"
    if name in {"com22_pressure_reference.json", "pressure_reference.json"}:
        return "pressure_reference_snapshot"
    if name == "pressure_channel_completion_summary.csv" or "pressure_channel_completion" in name:
        return "pressure_channel_completion"
    if "pressure_channel_quick_check" in name or "pressure_quick_check" in name:
        return "pressure_channel_quick_check"
    if name in {"queue_abort_exclusion.csv", "queue_abort_exclusion.json"}:
        return "h2o_queue_exclusion"
    if name.startswith("samples_") and path.suffix.lower() == ".csv":
        return "raw_samples"
    if "co2_open_flow" in parent_text or ("co2" in parent_parts and "open_flow" in parent_text):
        return "co2_open_flow_evidence"
    if "h2o_open_flow" in parent_text or "dewpoint" in name or ("h2o" in parent_parts and "open_flow" in parent_text):
        return "h2o_open_flow_evidence"
    if "candidate" in name or "fit_input_quality" in parent_text or "candidate" in parent_text:
        return "candidate_review"
    if "controlled_write" in parent_text or "write_event" in name or "coefficient_write" in name:
        return "coefficient_write_event"
    if "post_write_reverification" in parent_text or "post_write_reverification" in name:
        return "post_write_reverification"
    if name == "evidence_bundle.json":
        return "evidence_bundle"
    if name == "evidence_bundle_integrity.json":
        return "evidence_bundle_integrity"
    if "database_import" in name or "import_summary" in name:
        return "database_import_summary"
    if name == "report_model.json":
        return "report_model"
    if name == "per_device_certificate_manifest.json":
        return "per_device_certificate_manifest"
    if name == "per_device_certificate_artifact_hashes.csv":
        return "per_device_certificate_artifact_hashes"
    if name == "v1_5_full_flow_release_domains.csv":
        return "full_flow_release_domains"
    if name.startswith("run_report."):
        return "run_report"
    if name.startswith("technical_report."):
        return "technical_report"
    if name.startswith("formal_calibration_report."):
        return "formal_calibration_report"
    if "per_device_certificates" in parent_parts and "_calibration_certificate." in name:
        return "per_device_calibration_certificate"
    if "per_device_certificates" in parent_parts and "_verification_certificate." in name:
        return "per_device_verification_certificate"
    if name.endswith(".json"):
        return "json_evidence"
    if name.endswith(".csv"):
        return "csv_evidence"
    return "evidence_file"


def _artifact_from_path(path: Path, *, role: str | None = None, source: str = "discovered") -> EvidenceArtifact | None:
    if not path.exists() or not path.is_file():
        return None
    return EvidenceArtifact(
        role=role or _classify_artifact(path),
        path=str(path.resolve()),
        sha256=sha256_file(path),
        size_bytes=int(path.stat().st_size),
        source=source,
    )


def _dedupe_artifacts(artifacts: Iterable[EvidenceArtifact]) -> tuple[EvidenceArtifact, ...]:
    by_key: dict[tuple[str, str], EvidenceArtifact] = {}
    for item in artifacts:
        key = (item.role, item.path.lower())
        by_key[key] = item
    return tuple(sorted(by_key.values(), key=lambda row: (row.role, row.path.lower())))


def _discover_artifacts(run_dir: Path) -> tuple[EvidenceArtifact, ...]:
    extensions = {".csv", ".json", ".xlsx", ".md", ".txt", ".log", ".pdf", ".docx"}
    artifacts: list[EvidenceArtifact] = []
    if not run_dir.exists():
        return ()
    for path in run_dir.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in extensions:
            continue
        if path.name.lower() in SELF_ARTIFACT_NAMES:
            continue
        artifact = _artifact_from_path(path)
        if artifact:
            artifacts.append(artifact)
    return _dedupe_artifacts(artifacts)


def _find_latest(run_dir: Path, pattern: str) -> Path | None:
    matches = [path for path in run_dir.rglob(pattern) if path.is_file()]
    if not matches:
        return None
    return max(matches, key=lambda item: item.stat().st_mtime)


def _bundle_artifacts(bundle: Mapping[str, Any]) -> tuple[EvidenceArtifact, ...]:
    tables = bundle.get("tables") if isinstance(bundle.get("tables"), Mapping) else {}
    files = tables.get("sample_files") if isinstance(tables, Mapping) else []
    artifacts: list[EvidenceArtifact] = []
    for row in files or []:
        if not isinstance(row, Mapping):
            continue
        path_text = str(row.get("path") or "")
        if not path_text:
            continue
        path = Path(path_text)
        if path.exists() and path.is_file():
            artifact = _artifact_from_path(
                path,
                role=_safe_role(row.get("artifact_role")),
                source="evidence_bundle",
            )
            if artifact:
                artifacts.append(artifact)
        else:
            artifacts.append(
                EvidenceArtifact(
                    role=_safe_role(row.get("artifact_role")),
                    path=path_text,
                    sha256=str(row.get("sha256") or ""),
                    size_bytes=int(row.get("size_bytes") or 0),
                    source="evidence_bundle_missing_on_disk",
                )
            )
    return _dedupe_artifacts(artifacts)


def _roles(artifacts: Sequence[EvidenceArtifact]) -> set[str]:
    return {item.role for item in artifacts}


def _stage(
    *,
    stage_id: str,
    title: str,
    roles: Sequence[str],
    artifacts: Sequence[EvidenceArtifact],
    physical_meaning: str,
    missing_reason: str,
    pass_reason: str,
    optional: bool = False,
    status_override: str | None = None,
    reason_override: str | None = None,
) -> EvidenceStageStatus:
    role_set = _roles(artifacts)
    present = [role for role in roles if role in role_set]
    if status_override:
        status = status_override
        reason = reason_override or status_override
    elif set(roles).issubset(role_set):
        status = "pass"
        reason = pass_reason
    elif present:
        status = "partial"
        reason = f"present_roles={','.join(present)}"
    elif optional:
        status = "not_attempted"
        reason = missing_reason
    else:
        status = "missing"
        reason = missing_reason
    count = sum(1 for item in artifacts if item.role in set(roles))
    return EvidenceStageStatus(
        stage_id=stage_id,
        title=title,
        status=status,
        reason=reason,
        artifact_roles=tuple(roles),
        artifact_count=count,
        physical_meaning=physical_meaning,
    )


def _component_points_present(traceability: Mapping[str, Any], component: str) -> bool:
    for row in traceability.get("calibration_points") or []:
        if str(row.get("component") or "").strip().lower() == component:
            return True
    return False


def _write_attempted(traceability: Mapping[str, Any]) -> bool:
    for row in traceability.get("coefficient_write_events") or []:
        if str(row.get("status") or "") not in {"", "not_attempted", "blocked"}:
            return True
    return False


def _report_types_present(traceability: Mapping[str, Any]) -> set[str]:
    return {
        str(row.get("report_type") or "").strip()
        for row in traceability.get("reports") or []
        if str(row.get("report_type") or "").strip()
    }


def _is_manifest_placeholder(value: Any) -> bool:
    text = str(value or "").strip()
    return not text or (text.startswith("<") and text.endswith(">"))


def _matches_expected_output(artifact: EvidenceArtifact, expected_output: str) -> bool:
    expected = str(expected_output or "").strip()
    if _is_manifest_placeholder(expected):
        return False
    expected_norm = expected.replace("\\", "/").strip("/").lower()
    artifact_norm = artifact.path.replace("\\", "/").lower()
    if artifact_norm.endswith(expected_norm):
        return True
    expected_name = Path(expected).name.lower()
    return bool(expected_name and Path(artifact.path).name.lower() == expected_name)


def _manifest_stage_status(stage: Mapping[str, Any], artifacts: Sequence[EvidenceArtifact]) -> dict[str, Any]:
    expected_outputs = [
        str(item)
        for item in stage.get("expected_outputs") or []
        if not _is_manifest_placeholder(item)
    ]
    present_outputs = []
    for expected in expected_outputs:
        if any(_matches_expected_output(artifact, expected) for artifact in artifacts):
            present_outputs.append(expected)
    missing_outputs = [item for item in expected_outputs if item not in set(present_outputs)]
    auth = stage.get("authorization_required") if isinstance(stage.get("authorization_required"), Mapping) else {}
    automation_state = str(stage.get("automation_state") or "")

    if expected_outputs and not missing_outputs:
        status = "pass"
        reason = "all_manifest_expected_outputs_present"
    elif present_outputs:
        status = "partial"
        reason = "some_manifest_expected_outputs_present"
    elif auth.get("coefficient_write") or auth.get("device_id_write"):
        status = "blocked_controlled_gate"
        reason = "controlled_write_or_device_id_gate_requires_explicit_review"
    elif auth.get("real_com") or auth.get("pressure_control") or auth.get("route_control"):
        status = "authorization_required"
        reason = "live_stage_requires_explicit_authorization_and_external_execution"
    elif expected_outputs:
        status = "waiting_for_artifacts"
        reason = "manifest_expected_outputs_missing"
    elif automation_state == "manual_review_gate":
        status = "manual_review"
        reason = "manual_review_gate_without_generated_outputs"
    else:
        status = "not_attempted"
        reason = "no_manifest_output_contract_to_evaluate"

    return {
        "order": stage.get("order"),
        "step_id": stage.get("step_id"),
        "title": stage.get("title"),
        "phase": stage.get("phase"),
        "automation_state": automation_state,
        "status": status,
        "reason": reason,
        "expected_output_count": len(expected_outputs),
        "present_output_count": len(present_outputs),
        "missing_expected_outputs": missing_outputs,
        "authorization_required": dict(auth),
    }


def _stage_manifest_summary(
    manifest: Mapping[str, Any],
    *,
    manifest_path: Path | None,
    artifacts: Sequence[EvidenceArtifact],
) -> dict[str, Any]:
    if not manifest:
        return {
            "status": "not_found",
            "source_path": str(manifest_path) if manifest_path else "",
            "stage_count": 0,
            "current_manifest_stage": "",
            "automation_summary": {},
            "stage_statuses": [],
        }

    stage_statuses = [
        _manifest_stage_status(stage, artifacts)
        for stage in manifest.get("stages") or []
        if isinstance(stage, Mapping)
    ]
    status_counts: dict[str, int] = {}
    for row in stage_statuses:
        status = str(row.get("status") or "")
        status_counts[status] = status_counts.get(status, 0) + 1
    current = next(
        (
            str(row.get("step_id") or "")
            for row in stage_statuses
            if row.get("status")
            not in {
                "pass",
                "not_attempted",
            }
        ),
        "complete" if stage_statuses and all(row.get("status") == "pass" for row in stage_statuses) else "",
    )
    return {
        "status": "present",
        "source_path": str(manifest_path) if manifest_path else "",
        "schema": str(manifest.get("schema") or ""),
        "run_id": str(manifest.get("run_id") or ""),
        "current_automation_level": str(manifest.get("current_automation_level") or ""),
        "one_button_live_runner_ready": bool(manifest.get("one_button_live_runner_ready")),
        "current_manifest_stage": current,
        "automation_summary": dict(manifest.get("automation_summary") or {}),
        "status_counts": dict(sorted(status_counts.items())),
        "stage_count": len(stage_statuses),
        "stage_statuses": stage_statuses,
    }


def build_v1_5_run_evidence_status(
    *,
    run_dir: str | Path,
    full_flow_plan_json: str | Path | None = None,
    full_flow_stage_manifest_json: str | Path | None = None,
    contract_json: str | Path | None = None,
    evidence_bundle_json: str | Path | None = None,
    component: str = "both",
) -> dict[str, Any]:
    """Return a V1.5 run evidence status tree from existing files only."""

    root = Path(run_dir).resolve()
    bundle_path = Path(evidence_bundle_json).resolve() if evidence_bundle_json else _find_latest(root, "evidence_bundle.json")
    contract_path = Path(contract_json).resolve() if contract_json else _find_latest(root, "v1_5_formal_flow_contract.json")
    plan_path = Path(full_flow_plan_json).resolve() if full_flow_plan_json else _find_latest(root, "v1_5_full_flow_plan.json")
    manifest_path = (
        Path(full_flow_stage_manifest_json).resolve()
        if full_flow_stage_manifest_json
        else _find_latest(root, "v1_5_full_flow_stage_manifest.json")
    )

    artifacts = list(_discover_artifacts(root))
    for path, role in (
        (plan_path, "full_flow_plan"),
        (manifest_path, "full_flow_stage_manifest"),
        (contract_path, "full_flow_contract"),
        (bundle_path, "evidence_bundle"),
    ):
        if path:
            artifact = _artifact_from_path(path, role=role, source="explicit_or_latest")
            if artifact:
                artifacts.append(artifact)

    bundle = _load_json(bundle_path)
    if bundle:
        artifacts.extend(_bundle_artifacts(bundle))
    artifacts_tuple = _dedupe_artifacts(artifacts)
    role_set = _roles(artifacts_tuple)

    contract = _load_json(contract_path)
    stage_manifest = _load_json(manifest_path)
    contract_status = str(contract.get("status") or "missing")
    traceability = bundle_traceability_summary(bundle) if bundle else {}
    checks = traceability.get("traceability_checks") if isinstance(traceability.get("traceability_checks"), Mapping) else {}
    candidate_review_status = "pass" if traceability.get("coefficient_candidates") else None
    post_write_status = "pass" if checks.get("has_post_write_reverification") is True else None
    bundle_evidence_status = str(traceability.get("evidence_status") or traceability.get("package_status") or "")
    bundle_status = "blocked" if bundle_evidence_status == "blocked" else ("pass" if bundle else None)
    report_types = _report_types_present(traceability)
    report_roles = ("report_model", "run_report", "technical_report", "formal_calibration_report")
    report_status = "pass" if set(report_roles).issubset(report_types) else None
    per_device_certificate_roles = (
        "per_device_certificate_manifest",
        "per_device_certificate_artifact_hashes",
        "per_device_calibration_certificate",
    )

    stages: list[EvidenceStageStatus] = []
    stages.append(
        _stage(
            stage_id="full_flow_stage_manifest",
            title="Full-flow stage manifest",
            roles=("full_flow_stage_manifest",),
            artifacts=artifacts_tuple,
            physical_meaning=(
                "The stage manifest maps the complete V1.5 flow into machine-readable "
                "automation states, evidence contracts, and live/write authorization gates."
            ),
            missing_reason="full-flow stage manifest not generated",
            pass_reason="full-flow stage manifest artifact is present",
            optional=True,
        )
    )
    stages.append(
        _stage(
            stage_id="full_flow_contract_gate",
            title="Full-flow contract audit gate",
            roles=("full_flow_contract",),
            artifacts=artifacts_tuple,
            physical_meaning="Before any formal run advance, the plan must prove pressure-first, open-flow component sampling, no V2 real COM, and no auto-write boundaries.",
            missing_reason="contract audit not found",
            pass_reason="contract audit artifact present",
            status_override="pass" if contract_status == "pass" else ("blocked" if contract_status == "blocked" else None),
            reason_override=f"contract_status={contract_status}",
        )
    )
    stages.append(
        _stage(
            stage_id="plan_traceability",
            title="Plan and traceability snapshots",
            roles=("formal_plan_snapshot", "pressure_reference_snapshot"),
            artifacts=artifacts_tuple,
            physical_meaning="The calibration result must bind to a plan, standard/reference certificates, and config identity before sampling.",
            missing_reason="formal plan or pressure reference snapshot missing",
            pass_reason="formal plan and pressure reference snapshots are indexed",
        )
    )
    stages.append(
        _stage(
            stage_id="identity_getco_epoch0",
            title="Analyzer identity and GETCO epoch 0",
            roles=("getco_snapshot", "runtime_identity_bound_config"),
            artifacts=artifacts_tuple,
            physical_meaning="COM ports are transport only; analyzer device IDs and GETCO1-9 define the pre-calibration coefficient epoch.",
            missing_reason="GETCO snapshot or runtime identity-bound config missing",
            pass_reason="device identity and coefficient epoch 0 evidence are present",
        )
    )
    pressure_input_roles = ("pressure_channel_quick_check", "pressure_channel_completion")
    pressure_roles_present = tuple(role for role in pressure_input_roles if role in role_set)
    stages.append(
        _stage(
            stage_id="pressure_quick_check",
            title="Pressure channel quick check or completion",
            roles=pressure_input_roles,
            artifacts=artifacts_tuple,
            physical_meaning="Analyzer pressure P is an input to CO2/H2O compensation and must be verified before component calibration.",
            missing_reason="pressure quick-check or pressure-channel completion evidence missing",
            pass_reason="pressure-channel input evidence is present",
            status_override="pass" if pressure_roles_present else None,
            reason_override=(
                f"pressure_input_roles_present={','.join(pressure_roles_present)}"
                if pressure_roles_present
                else None
            ),
        )
    )

    co2_present = _component_points_present(traceability, "co2") or "co2_open_flow_evidence" in role_set
    h2o_present = _component_points_present(traceability, "h2o") or "h2o_open_flow_evidence" in role_set
    component_lower = str(component or "both").lower()
    if component_lower in {"co2", "both"}:
        stages.append(
            _stage(
                stage_id="co2_open_flow",
                title="CO2 open-flow evidence",
                roles=("raw_samples",),
                artifacts=artifacts_tuple,
                physical_meaning="CO2 fitting must be based on clean open-flow samples and factory ratio evidence, not sealed contaminated pressure points.",
                missing_reason="CO2 open-flow sample evidence missing",
                pass_reason="CO2 open-flow points are present",
                status_override="pass" if co2_present else None,
                reason_override="co2 calibration points or open-flow artifacts present" if co2_present else None,
            )
        )
    if component_lower in {"h2o", "both"}:
        stages.append(
            _stage(
                stage_id="h2o_open_flow",
                title="H2O open-flow evidence",
                roles=("raw_samples",),
                artifacts=artifacts_tuple,
                physical_meaning="H2O fitting must preserve dewpoint/reference-backed water evidence and dry-gas low-water anchors separately from CO2 zero gas.",
                missing_reason="H2O open-flow sample evidence missing",
                pass_reason="H2O open-flow points are present",
                status_override="pass" if h2o_present else None,
                reason_override="h2o calibration points or open-flow artifacts present" if h2o_present else None,
            )
        )
    stages.append(
        _stage(
            stage_id="h2o_queue_exclusion",
            title="H2O queue abort/exclusion evidence",
            roles=("h2o_queue_exclusion",),
            artifacts=artifacts_tuple,
            physical_meaning="Aborted H2O queue rows are retained as diagnostic evidence and must not enter formal fit, acceptance, or SENCO review.",
            missing_reason="no H2O queue abort/exclusion artifact found",
            pass_reason="H2O queue exclusion artifact is present",
            optional=True,
        )
    )
    stages.append(
        _stage(
            stage_id="candidate_review",
            title="Candidate coefficient review",
            roles=("candidate_review", "candidate_coefficient_review"),
            artifacts=artifacts_tuple,
            physical_meaning="Only stable, role-eligible samples should enter SENCO candidate fitting and reviewer approval.",
            missing_reason="candidate review not attempted",
            pass_reason="candidate review artifacts are present",
            optional=True,
            status_override=candidate_review_status,
            reason_override="evidence bundle contains candidate coefficient rows" if candidate_review_status else None,
        )
    )
    stages.append(
        _stage(
            stage_id="post_run_coefficient_executor",
            title="Post-run coefficient closure executor",
            roles=(
                "post_run_coefficient_executor",
                "post_run_device_eligibility",
                "post_run_controlled_write_package",
                "post_run_reverification_plan",
                "post_run_archive_gap_list",
            ),
            artifacts=artifacts_tuple,
            physical_meaning=(
                "After CO2/H2O acquisition and fit-input review, this offline executor binds per-device "
                "eligibility, controlled write package, post-write reverification plan, and archive gap list "
                "before any SENCO write is allowed."
            ),
            missing_reason="post-run coefficient executor not generated",
            pass_reason="post-run executor manifest, device eligibility, write package, reverification plan, and archive gaps are present",
            optional=True,
        )
    )
    stages.append(
        _stage(
            stage_id="full_flow_closure_readiness",
            title="Full-flow closure readiness before controlled write",
            roles=(
                "full_flow_closure_readiness",
                "full_flow_closure_gaps",
                "full_flow_device_closure",
                "full_flow_release_domains",
            ),
            artifacts=artifacts_tuple,
            physical_meaning=(
                "Before controlled SENCO writes, the run must show one auditable chain from plan, "
                "raw open-flow evidence, QC decisions, candidate write package, reverification plan, "
                "archive gaps, and formal release domains. This gate is offline and does not touch "
                "COM ports or routes."
            ),
            missing_reason="full-flow closure readiness not generated",
            pass_reason="full-flow closure readiness, device closure, gap, and release-domain artifacts are present",
            optional=True,
        )
    )
    write_status = "write_attempted" if _write_attempted(traceability) else None
    stages.append(
        _stage(
            stage_id="controlled_write_events",
            title="Controlled coefficient write events",
            roles=("coefficient_write_event",),
            artifacts=artifacts_tuple,
            physical_meaning="Any SENCO write starts a new coefficient epoch and must have command, readback, approval, and rollback evidence.",
            missing_reason="no controlled write artifact found",
            pass_reason="controlled write event artifacts are present",
            optional=True,
            status_override=write_status,
            reason_override="evidence bundle contains coefficient write attempts" if write_status else None,
        )
    )
    stages.append(
        _stage(
            stage_id="post_write_reverification",
            title="Post-write reverification",
            roles=(
                "post_write_reverification",
                "post_write_reverification_review",
                "post_write_reverification_points",
                "post_write_reverification_device_summary",
            ),
            artifacts=artifacts_tuple,
            physical_meaning="After any coefficient write, independent open-flow verification points must prove the updated measurement model.",
            missing_reason="post-write verification not attempted",
            pass_reason="post-write reverification artifacts are present",
            optional=True,
            status_override=post_write_status,
            reason_override="evidence bundle contains post-write reverification rows" if post_write_status else None,
        )
    )
    stages.append(
        _stage(
            stage_id="evidence_bundle",
            title="Formal evidence bundle",
            roles=("evidence_bundle",),
            artifacts=artifacts_tuple,
            physical_meaning="The evidence bundle freezes raw artifacts, QC, traceability, coefficient events, hashes, and report inputs for reconstruction.",
            missing_reason="evidence bundle missing",
            pass_reason="evidence bundle artifact is present",
            status_override=bundle_status,
            reason_override=(
                f"evidence_bundle_status={bundle_evidence_status}"
                if bundle_status == "blocked"
                else ("evidence bundle loaded as linked input" if bundle_status else None)
            ),
        )
    )
    stages.append(
        _stage(
            stage_id="database_import",
            title="Evidence database import",
            roles=("database_import_summary",),
            artifacts=artifacts_tuple,
            physical_meaning="PostgreSQL indexes traceability and audit state; raw evidence remains in hashed evidence packages.",
            missing_reason="database import not attempted or summary not found",
            pass_reason="database import summary is present",
            optional=True,
            status_override="pass" if traceability.get("database_imported") is True else None,
        )
    )
    stages.append(
        _stage(
            stage_id="reports",
            title="Run, technical, and formal calibration reports",
            roles=report_roles,
            artifacts=artifacts_tuple,
            physical_meaning="Reports are the reviewer-facing summary of method, QC, traceability, uncertainty, coefficient write status, and limitations.",
            missing_reason="one or more formal report artifacts missing",
            pass_reason="report artifacts are present",
            optional=True,
            status_override=report_status,
            reason_override="evidence bundle contains run, technical, and formal report rows" if report_status else None,
        )
    )
    stages.append(
        _stage(
            stage_id="per_device_certificates",
            title="Per-device calibration and verification certificate package",
            roles=per_device_certificate_roles,
            artifacts=artifacts_tuple,
            physical_meaning="Per-device certificates bind the final device identity, QC result, coefficient state, traceability, hashes, and report-release boundary.",
            missing_reason="per-device certificate package not generated",
            pass_reason="per-device certificate manifest, hashes, and calibration certificates are present",
            optional=True,
        )
    )

    hard_missing = [
        stage.stage_id
        for stage in stages
        if stage.status in {"missing", "blocked"} and stage.stage_id not in {"reports", "database_import"}
    ]
    current_stage = next((stage.stage_id for stage in stages if stage.status in {"missing", "partial", "blocked"}), "complete")
    overall_status = "blocked" if any(stage.status == "blocked" for stage in stages) else (
        "incomplete" if hard_missing else "ready_for_reviewer"
    )

    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "run_dir": str(root),
        "component": component_lower,
        "overall_status": overall_status,
        "current_stage": current_stage,
        "physical_boundaries": {
            "offline_status_only": True,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "controls_valves_or_pace": False,
            "writes_coefficients": False,
            "not_real_acceptance_evidence": True,
        },
        "linked_inputs": {
            "full_flow_plan_json": str(plan_path) if plan_path else "",
            "full_flow_stage_manifest_json": str(manifest_path) if manifest_path else "",
            "contract_json": str(contract_path) if contract_path else "",
            "evidence_bundle_json": str(bundle_path) if bundle_path else "",
        },
        "contract_status": contract_status,
        "full_flow_stage_manifest": _stage_manifest_summary(
            stage_manifest,
            manifest_path=manifest_path,
            artifacts=artifacts_tuple,
        ),
        "stage_statuses": [stage.to_json() for stage in stages],
        "artifact_count": len(artifacts_tuple),
        "artifacts": [artifact.to_json() for artifact in artifacts_tuple],
        "traceability_checks": dict(checks),
        "table_counts": dict(traceability.get("table_counts") or {}),
    }


def render_v1_5_run_evidence_status_markdown(status: Mapping[str, Any]) -> str:
    lines = [
        "# V1.5 Run Evidence Status",
        "",
        f"- overall_status: `{status.get('overall_status')}`",
        f"- current_stage: `{status.get('current_stage')}`",
        f"- run_dir: `{status.get('run_dir')}`",
        f"- contract_status: `{status.get('contract_status')}`",
        "",
        "## Physical Boundaries",
        "",
    ]
    for key, value in (status.get("physical_boundaries") or {}).items():
        lines.append(f"- `{key}`: `{value}`")
    manifest = status.get("full_flow_stage_manifest") or {}
    lines.extend(["", "## Full-Flow Stage Manifest", ""])
    lines.append(f"- status: `{manifest.get('status', 'not_found')}`")
    if manifest.get("source_path"):
        lines.append(f"- source_path: `{manifest.get('source_path')}`")
    if manifest.get("schema"):
        lines.append(f"- schema: `{manifest.get('schema')}`")
    if manifest.get("current_manifest_stage"):
        lines.append(f"- current_manifest_stage: `{manifest.get('current_manifest_stage')}`")
    if "one_button_live_runner_ready" in manifest:
        lines.append(f"- one_button_live_runner_ready: `{manifest.get('one_button_live_runner_ready')}`")
    status_counts = manifest.get("status_counts") or {}
    if status_counts:
        lines.append("")
        lines.append("Stage status counts:")
        for key, value in status_counts.items():
            lines.append(f"- `{key}`: `{value}`")
    stage_rows = manifest.get("stage_statuses") or []
    if stage_rows:
        lines.append("")
        lines.append("Manifest stages:")
        for row in stage_rows:
            lines.append(
                f"- `{row.get('step_id')}`: `{row.get('status')}` - {row.get('reason')}"
            )
    lines.extend(["", "## Stages", ""])
    for stage in status.get("stage_statuses") or []:
        lines.append(
            f"- `{stage.get('stage_id')}` {stage.get('title')}: `{stage.get('status')}` - {stage.get('reason')}"
        )
        if stage.get("physical_meaning"):
            lines.append(f"  - physical_meaning: {stage.get('physical_meaning')}")
    lines.extend(["", "## Traceability Checks", ""])
    checks = status.get("traceability_checks") or {}
    if checks:
        for key, value in checks.items():
            lines.append(f"- `{key}`: `{value}`")
    else:
        lines.append("- none")
    lines.extend(["", "## Artifact Roles", ""])
    role_counts: dict[str, int] = {}
    for artifact in status.get("artifacts") or []:
        role = str(artifact.get("role") or "evidence_file")
        role_counts[role] = role_counts.get(role, 0) + 1
    for role, count in sorted(role_counts.items()):
        lines.append(f"- `{role}`: `{count}`")
    return "\n".join(lines).rstrip() + "\n"
