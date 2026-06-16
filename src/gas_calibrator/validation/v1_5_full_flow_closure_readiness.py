"""Offline V1.5 full-flow closure readiness review.

This reviewer-facing model checks whether the outputs of a V1.5 full-flow run
are connected strongly enough to advance to controlled write review,
post-write reverification, formal archive, or certificate release. It never
opens COM ports, controls valves/routes/PACE, or writes analyzer coefficients.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_full_flow_closure_readiness_v1"


@dataclass(frozen=True)
class ClosureStage:
    stage_id: str
    title: str
    status: str
    reason: str
    evidence_path: str
    physical_meaning: str
    next_action: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClosureGap:
    scope: str
    item: str
    status: str
    reason: str
    next_action: str
    physical_meaning: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClosureDomain:
    domain_id: str
    title: str
    status: str
    reason: str
    evidence_path: str
    physical_meaning: str
    next_action: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _load_json(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    if not source.exists() or not source.is_file():
        return {}
    payload = json.loads(source.read_text(encoding="utf-8-sig"))
    return payload if isinstance(payload, dict) else {}


def _read_csv(path: Path | None) -> list[dict[str, str]]:
    if not path or not path.exists() or not path.is_file():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    keys: list[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in keys})


def _safe_rglob(root: Path, pattern: str) -> tuple[Path, ...]:
    if not root.exists():
        return ()
    try:
        return tuple(path for path in root.rglob(pattern) if path.is_file())
    except OSError:
        return ()


def _latest(root: Path, *patterns: str) -> Path | None:
    matches: list[Path] = []
    for pattern in patterns:
        matches.extend(_safe_rglob(root, pattern))
    if not matches:
        return None
    return max(matches, key=lambda item: item.stat().st_mtime).resolve()


def _explicit_or_latest(root: Path, explicit: str | Path | None, *patterns: str) -> Path | None:
    if explicit:
        path = Path(explicit).resolve()
        if path.exists() and path.is_file():
            return path
    return _latest(root, *patterns)


def _path_text(path: Path | None) -> str:
    return str(path.resolve()) if path and path.exists() else ""


def _status_from_existing_json(payload: Mapping[str, Any], keys: Sequence[str] = ("overall_status", "status")) -> str:
    for key in keys:
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    return "missing"


def _stage(
    *,
    stage_id: str,
    title: str,
    path: Path | None,
    payload: Mapping[str, Any] | None = None,
    ready_values: Sequence[str] = ("pass", "ready", "complete", "ready_for_reviewer"),
    partial_values: Sequence[str] = ("partial", "incomplete", "ready_for_next_automatic_step"),
    blocked_values: Sequence[str] = ("blocked", "missing", "error", "failed"),
    physical_meaning: str,
    missing_reason: str,
    next_action_missing: str,
    status_override: str | None = None,
    reason_override: str | None = None,
) -> ClosureStage:
    source_payload = payload or {}
    raw_status = _status_from_existing_json(source_payload)
    if status_override:
        status = status_override
        reason = reason_override or status_override
    elif not path or not path.exists():
        status = "blocked"
        reason = missing_reason
    elif raw_status in ready_values:
        status = "ready"
        reason = f"source_status={raw_status}"
    elif raw_status in partial_values:
        status = "partial"
        reason = f"source_status={raw_status}"
    elif raw_status in blocked_values:
        status = "blocked"
        reason = f"source_status={raw_status}"
    else:
        status = "ready"
        reason = "artifact_present_no_blocking_status"

    if status == "ready":
        next_action = "carry_forward"
    elif status == "partial":
        next_action = "review_partial_evidence_before_release"
    else:
        next_action = next_action_missing
    return ClosureStage(
        stage_id=stage_id,
        title=title,
        status=status,
        reason=reason,
        evidence_path=_path_text(path),
        physical_meaning=physical_meaning,
        next_action=next_action,
    )


def _executor_device_rows(executor: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in executor.get("devices") or []:
        if not isinstance(row, Mapping):
            continue
        blockers = row.get("blockers")
        if isinstance(blockers, (list, tuple)):
            blockers_text = ";".join(str(item) for item in blockers if str(item))
        else:
            blockers_text = str(blockers or "")
        rows.append(
            {
                "device_id": str(row.get("device_id") or ""),
                "overall_status": str(row.get("overall_status") or ""),
                "pressure_status": str(row.get("pressure_status") or ""),
                "temperature_status": str(row.get("temperature_status") or ""),
                "co2_status": str(row.get("co2_status") or ""),
                "h2o_status": str(row.get("h2o_status") or ""),
                "output_trim_status": str(row.get("output_trim_status") or ""),
                "blockers": blockers_text,
                "next_action": str(row.get("next_action") or ""),
            }
        )
    return rows


def _executor_list_has_rows(executor: Mapping[str, Any], key: str, csv_path: Path | None) -> bool:
    value = executor.get(key)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return len(value) > 0
    return bool(_read_csv(csv_path))


def _controlled_write_ready(executor: Mapping[str, Any], csv_path: Path | None) -> tuple[str, str]:
    if not executor:
        return "blocked", "post-run executor manifest missing"
    if _executor_list_has_rows(executor, "controlled_write_package", csv_path):
        return "ready", "controlled write package rows present"
    if str(executor.get("overall_status") or "") == "ready_for_next_automatic_step":
        return "partial", "executor ready but controlled write package rows are missing"
    return "blocked", "controlled write package missing"


def _reverification_plan_ready(executor: Mapping[str, Any], csv_path: Path | None) -> tuple[str, str]:
    if not executor:
        return "blocked", "post-run executor manifest missing"
    if _executor_list_has_rows(executor, "post_write_reverification_plan", csv_path):
        return "ready", "post-write reverification plan rows present"
    return "partial", "post-write reverification plan not generated yet"


def _archive_gap_status(executor: Mapping[str, Any], csv_path: Path | None) -> tuple[str, str]:
    if not executor:
        return "blocked", "post-run executor manifest missing"
    rows = executor.get("archive_gap_list")
    if isinstance(rows, Sequence) and not isinstance(rows, (str, bytes)):
        if rows:
            return "partial", f"archive_gap_count={len(rows)}"
        return "ready", "no archive gaps listed"
    csv_rows = _read_csv(csv_path)
    if csv_rows:
        return "partial", f"archive_gap_count={len(csv_rows)}"
    if csv_path and csv_path.exists():
        return "ready", "archive gap CSV present and empty"
    return "partial", "archive gap list not generated yet"


def _source_stage_status(payload: Mapping[str, Any], stage_id: str) -> str:
    for row in payload.get("stage_statuses") or []:
        if not isinstance(row, Mapping):
            continue
        if str(row.get("stage_id") or "") == stage_id:
            return str(row.get("status") or "")
    return ""


def _closure_domain(
    *,
    domain_id: str,
    title: str,
    status: str,
    reason: str,
    evidence_path: Path | None,
    physical_meaning: str,
    next_action: str,
) -> ClosureDomain:
    return ClosureDomain(
        domain_id=domain_id,
        title=title,
        status=status,
        reason=reason,
        evidence_path=_path_text(evidence_path),
        physical_meaning=physical_meaning,
        next_action=next_action,
    )


def _report_family_present(reports: Mapping[str, Any], family: str) -> bool:
    prefix = f"{family}_"
    return any(str(key).startswith(prefix) or str(key) == family for key in reports)


def _build_closure_domains(
    *,
    stages: Sequence[ClosureStage],
    evidence_status: Mapping[str, Any],
    archive_closure: Mapping[str, Any],
    archive_path: Path | None,
) -> list[dict[str, Any]]:
    stage_by_id = {stage.stage_id: stage for stage in stages}
    archive_stage = stage_by_id.get("formal_archive_closure")
    archive_gap_stage = stage_by_id.get("archive_gap_list")
    archive_ready = bool(archive_stage and archive_stage.status == "ready")
    archive_gaps_clear = bool(archive_gap_stage and archive_gap_stage.status == "ready")
    if archive_ready and archive_gaps_clear:
        archive_status = "ready"
        archive_reason = "formal archive index present and archive gap list is clear"
        archive_next = "carry_forward"
    elif archive_stage and archive_stage.status == "blocked":
        archive_status = "blocked"
        archive_reason = archive_stage.reason
        archive_next = archive_stage.next_action
    else:
        archive_status = "partial"
        archive_reason = "formal archive or archive gap evidence still needs reviewer closure"
        archive_next = "close archive gaps and rerun formal archive closure"

    database = archive_closure.get("database") if isinstance(archive_closure.get("database"), Mapping) else {}
    evidence_database_status = _source_stage_status(evidence_status, "database_import")
    if database.get("database_imported") is True or evidence_database_status == "pass":
        database_status = "ready"
        database_reason = "database import summary confirms indexed evidence"
        database_next = "carry_forward"
    elif database:
        mode = str(database.get("mode") or "unknown")
        database_status = "partial"
        database_reason = f"database_imported=false; mode={mode}; {database.get('reason') or 'bundle is not imported yet'}"
        database_next = "import final evidence bundle into PostgreSQL or record an approved dry-run exception"
    else:
        database_status = "blocked"
        database_reason = "database import summary not found in archive closure or evidence status"
        database_next = "generate database_import_summary.json from the final evidence bundle"

    reports = archive_closure.get("reports") if isinstance(archive_closure.get("reports"), Mapping) else {}
    evidence_report_status = _source_stage_status(evidence_status, "reports")
    required_report_families = ("run_report", "technical_report", "formal_calibration_report")
    all_required_reports = all(_report_family_present(reports, family) for family in required_report_families)
    if evidence_report_status == "pass" or all_required_reports:
        reports_status = "ready"
        reports_reason = "run, technical, and formal report evidence is present"
        reports_next = "carry_forward"
    elif reports:
        reports_status = "partial"
        reports_reason = "some report artifacts are present but the required report set is incomplete"
        reports_next = "regenerate the formal report pack from the final evidence bundle"
    else:
        reports_status = "blocked"
        reports_reason = "run, technical, and formal report artifacts are missing"
        reports_next = "generate reports from the final evidence bundle"

    certificate_status = _source_stage_status(evidence_status, "per_device_certificates")
    if certificate_status == "pass":
        certificates_status = "ready"
        certificates_reason = "per-device certificate manifest, hashes, and certificate artifacts are indexed"
        certificates_next = "carry_forward"
    elif reports_status == "ready":
        certificates_status = "partial"
        certificates_reason = "reports are ready but per-device certificate package is not fully indexed"
        certificates_next = "generate per-device calibration and verification certificates"
    else:
        certificates_status = "blocked"
        certificates_reason = "certificate package cannot be released until report evidence is complete"
        certificates_next = "finish reports, then regenerate per-device certificate package"

    return [
        _closure_domain(
            domain_id="formal_archive",
            title="Formal archive package",
            status=archive_status,
            reason=archive_reason,
            evidence_path=archive_path,
            physical_meaning=(
                "The archive is the frozen evidence package: raw frames, QC decisions, coefficient state, "
                "hashes, reports, and traceability records must be reconstructable from it."
            ),
            next_action=archive_next,
        ).to_json(),
        _closure_domain(
            domain_id="database_index",
            title="PostgreSQL evidence index",
            status=database_status,
            reason=database_reason,
            evidence_path=archive_path,
            physical_meaning=(
                "The database is an audit index over the hashed evidence package; it must not replace raw "
                "CSV/JSON/PDF/DOCX evidence files."
            ),
            next_action=database_next,
        ).to_json(),
        _closure_domain(
            domain_id="formal_reports",
            title="Run, technical, and formal reports",
            status=reports_status,
            reason=reports_reason,
            evidence_path=archive_path,
            physical_meaning=(
                "Reports summarize method, open-flow physical conditions, QC, traceability, uncertainty, "
                "coefficient write status, and limitations for review."
            ),
            next_action=reports_next,
        ).to_json(),
        _closure_domain(
            domain_id="per_device_certificates",
            title="Per-device certificates",
            status=certificates_status,
            reason=certificates_reason,
            evidence_path=archive_path,
            physical_meaning=(
                "Each certificate must bind one analyzer ID to its own point evidence, QC result, coefficient "
                "state, traceability records, report-release state, and artifact hashes."
            ),
            next_action=certificates_next,
        ).to_json(),
    ]


def _release_status(domains: Sequence[Mapping[str, Any]]) -> str:
    statuses = {str(row.get("status") or "") for row in domains}
    if "blocked" in statuses:
        return "blocked"
    if "partial" in statuses:
        return "partial"
    return "ready_for_formal_release"


def _gap_from_stage(stage: ClosureStage) -> ClosureGap | None:
    if stage.status == "ready":
        return None
    return ClosureGap(
        scope="stage",
        item=stage.stage_id,
        status=stage.status,
        reason=stage.reason,
        next_action=stage.next_action,
        physical_meaning=stage.physical_meaning,
    )


def build_v1_5_full_flow_closure_readiness(
    *,
    run_dir: str | Path,
    full_flow_plan_json: str | Path | None = None,
    run_evidence_status_json: str | Path | None = None,
    post_run_executor_json: str | Path | None = None,
    archive_closure_json: str | Path | None = None,
    controlled_write_package_csv: str | Path | None = None,
    post_write_reverification_plan_csv: str | Path | None = None,
    archive_gap_list_csv: str | Path | None = None,
) -> dict[str, Any]:
    """Build an offline closure-readiness model from existing full-flow artifacts."""

    root = Path(run_dir).resolve()
    plan_path = _explicit_or_latest(root, full_flow_plan_json, "v1_5_full_flow_plan.json")
    evidence_status_path = _explicit_or_latest(root, run_evidence_status_json, "v1_5_run_evidence_status.json")
    executor_path = _explicit_or_latest(root, post_run_executor_json, "executor_manifest.json")
    archive_path = _explicit_or_latest(
        root,
        archive_closure_json,
        "v1_5_formal_archive_closure_index.json",
        "formal_archive_closure_index.json",
    )
    controlled_write_path = _explicit_or_latest(
        root,
        controlled_write_package_csv,
        "controlled_write_package.csv",
    )
    reverify_plan_path = _explicit_or_latest(
        root,
        post_write_reverification_plan_csv,
        "post_write_reverification_plan.csv",
    )
    archive_gap_path = _explicit_or_latest(root, archive_gap_list_csv, "archive_gap_list.csv")

    plan = _load_json(plan_path)
    evidence_status = _load_json(evidence_status_path)
    executor = _load_json(executor_path)
    archive_closure = _load_json(archive_path)

    cw_status, cw_reason = _controlled_write_ready(executor, controlled_write_path)
    rv_status, rv_reason = _reverification_plan_ready(executor, reverify_plan_path)
    ag_status, ag_reason = _archive_gap_status(executor, archive_gap_path)

    stages = [
        _stage(
            stage_id="full_flow_plan",
            title="Full-flow plan",
            path=plan_path,
            payload=plan,
            ready_values=("v1_5_full_calibration_flow_plan_v0",),
            physical_meaning="The plan fixes the physical order: identity/GETCO, pressure, temperature, CO2 open-flow, H2O open-flow, fit review, controlled write, reverification, evidence, reports.",
            missing_reason="v1_5_full_flow_plan.json not found",
            next_action_missing="regenerate the full-flow plan before judging closure",
            status_override="ready" if plan_path and plan else None,
            reason_override="plan artifact loaded",
        ),
        _stage(
            stage_id="run_evidence_status",
            title="Run evidence status index",
            path=evidence_status_path,
            payload=evidence_status,
            ready_values=("ready_for_reviewer", "complete", "pass"),
            partial_values=("incomplete", "partial"),
            physical_meaning="The evidence index proves that raw samples, QC, traceability snapshots, reports, and certificates point to one reconstructable run tree.",
            missing_reason="v1_5_run_evidence_status.json not found",
            next_action_missing="refresh run evidence status from the final evidence bundle",
        ),
        _stage(
            stage_id="post_run_coefficient_executor",
            title="Post-run coefficient executor manifest",
            path=executor_path,
            payload=executor,
            ready_values=("ready_for_next_automatic_step", "complete", "ready"),
            partial_values=("partial", "incomplete"),
            physical_meaning="After CO2/H2O acquisition, pressure and temperature inputs must be checked before candidate coefficients, S5/S6 trims, write packages, and reverification plans are released.",
            missing_reason="post-run coefficient executor manifest not found",
            next_action_missing="run the post-run coefficient executor after component evidence is complete",
        ),
        _stage(
            stage_id="controlled_write_package",
            title="Controlled write package",
            path=controlled_write_path,
            physical_meaning="SENCO writes are high-risk model changes; the write package must be per-device, identity-bound, and based on reviewed payloads rather than COM aliases.",
            missing_reason="controlled_write_package.csv not found",
            next_action_missing="generate controlled write package from post-run executor",
            status_override=cw_status,
            reason_override=cw_reason,
        ),
        _stage(
            stage_id="post_write_reverification_plan",
            title="Post-write reverification plan",
            path=reverify_plan_path,
            physical_meaning="A coefficient write must be followed by independent open-flow verification; fitting rows and verification rows remain explicitly auditable.",
            missing_reason="post_write_reverification_plan.csv not found",
            next_action_missing="generate or approve post-write reverification plan",
            status_override=rv_status,
            reason_override=rv_reason,
        ),
        _stage(
            stage_id="archive_gap_list",
            title="Archive gap list",
            path=archive_gap_path,
            physical_meaning="Formal archive release requires raw data, QC decisions, old/new coefficients, write events, reports, certificates, hashes, and database/audit links to be traceable.",
            missing_reason="archive_gap_list.csv not found",
            next_action_missing="generate archive gap list and close remaining evidence holes",
            status_override=ag_status,
            reason_override=ag_reason,
        ),
        _stage(
            stage_id="formal_archive_closure",
            title="Formal archive closure",
            path=archive_path,
            payload=archive_closure,
            ready_values=("ready", "complete", "pass"),
            partial_values=("partial", "incomplete"),
            physical_meaning="Formal archive closure freezes the final run evidence package so reports and database records can be rebuilt from hashed artifacts.",
            missing_reason="formal archive closure index not found",
            next_action_missing="run formal archive closure after reports and post-write reverification",
        ),
    ]

    gaps = [gap.to_json() for gap in (_gap_from_stage(stage) for stage in stages) if gap is not None]
    device_rows = _executor_device_rows(executor)
    for row in device_rows:
        if row["overall_status"] not in {"ready_for_controlled_write_review", "ready", "complete"}:
            gaps.append(
                ClosureGap(
                    scope="device",
                    item=row["device_id"],
                    status=row["overall_status"] or "unknown",
                    reason=row["blockers"] or "device not ready for controlled write review",
                    next_action=row["next_action"] or "review per-device blocker",
                    physical_meaning="Each analyzer is fitted and released independently; a failed device should be blocked with a reason without hiding other devices that are ready.",
                ).to_json()
            )

    hard_blocked = any(stage.status == "blocked" for stage in stages[:4])
    partial = any(stage.status == "partial" for stage in stages)
    if hard_blocked:
        overall_status = "blocked"
    elif partial or gaps:
        overall_status = "partial"
    else:
        overall_status = "ready_for_controlled_write_review"
    closure_domains = _build_closure_domains(
        stages=stages,
        evidence_status=evidence_status,
        archive_closure=archive_closure,
        archive_path=archive_path,
    )

    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "run_dir": str(root),
        "overall_status": overall_status,
        "release_status": _release_status(closure_domains),
        "physical_boundaries": {
            "offline_closure_review_only": True,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "controls_valves_or_pace": False,
            "writes_coefficients": False,
            "writes_device_id": False,
            "not_real_acceptance_evidence": True,
        },
        "workflow_contract": {
            "pressure_before_components": True,
            "temperature_before_components": True,
            "co2_and_h2o_open_flow_only": True,
            "sample_window_requires_route_open": True,
            "fit_all_eligible_stable_points": True,
            "fit_label_does_not_exclude_points_by_default": True,
            "co2_zero_anchor_distinct_from_h2o_dry_anchor": True,
            "s5_s6_after_main_fit": True,
            "per_device_identity_not_com_alias": True,
        },
        "linked_inputs": {
            "full_flow_plan_json": _path_text(plan_path),
            "run_evidence_status_json": _path_text(evidence_status_path),
            "post_run_executor_json": _path_text(executor_path),
            "controlled_write_package_csv": _path_text(controlled_write_path),
            "post_write_reverification_plan_csv": _path_text(reverify_plan_path),
            "archive_gap_list_csv": _path_text(archive_gap_path),
            "archive_closure_json": _path_text(archive_path),
        },
        "stage_statuses": [stage.to_json() for stage in stages],
        "closure_domains": closure_domains,
        "devices": device_rows,
        "gaps": gaps,
    }


def render_v1_5_full_flow_closure_readiness_markdown(model: Mapping[str, Any]) -> str:
    lines = [
        "# V1.5 全流程离线闭环验收",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- run_dir: `{model.get('run_dir')}`",
        "",
        "## 物理边界",
    ]
    for key, value in (model.get("physical_boundaries") or {}).items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## 工作流合同"])
    for key, value in (model.get("workflow_contract") or {}).items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## 阶段状态"])
    for stage in model.get("stage_statuses") or []:
        lines.append(
            f"- `{stage.get('stage_id')}` {stage.get('title')}: `{stage.get('status')}` - {stage.get('reason')}"
        )
        if stage.get("physical_meaning"):
            lines.append(f"  - 物理意义: {stage.get('physical_meaning')}")
        if stage.get("next_action") and stage.get("status") != "ready":
            lines.append(f"  - 下一步: {stage.get('next_action')}")
    lines.extend(["", "## 归档 / 数据库 / 报告 / 证书闭环"])
    lines.append(f"- release_status: `{model.get('release_status')}`")
    for domain in model.get("closure_domains") or []:
        lines.append(
            f"- `{domain.get('domain_id')}` {domain.get('title')}: `{domain.get('status')}` - {domain.get('reason')}"
        )
        if domain.get("physical_meaning"):
            lines.append(f"  - 物理意义: {domain.get('physical_meaning')}")
        if domain.get("next_action") and domain.get("status") != "ready":
            lines.append(f"  - 下一步: {domain.get('next_action')}")
    lines.extend(["", "## 逐台设备"])
    devices = list(model.get("devices") or [])
    if not devices:
        lines.append("- 未发现逐台设备闭环结果。")
    for row in devices:
        lines.append(
            f"- `{row.get('device_id')}`: `{row.get('overall_status')}`; "
            f"pressure=`{row.get('pressure_status')}`, temperature=`{row.get('temperature_status')}`, "
            f"CO2=`{row.get('co2_status')}`, H2O=`{row.get('h2o_status')}`, trim=`{row.get('output_trim_status')}`"
        )
        if row.get("blockers"):
            lines.append(f"  - 阻塞: {row.get('blockers')}")
    lines.extend(["", "## 缺口清单"])
    gaps = list(model.get("gaps") or [])
    if not gaps:
        lines.append("- 未发现闭环缺口。")
    for gap in gaps:
        lines.append(
            f"- `{gap.get('scope')}` `{gap.get('item')}`: `{gap.get('status')}` - {gap.get('reason')}; "
            f"下一步: {gap.get('next_action')}"
        )
    lines.extend(
        [
            "",
            "## 校准含义",
            "- 该文件只说明证据链是否能支持下一步评审，不代表已经完成真机写入或正式 acceptance。",
            "- CO2 零气低端锚点和 H2O 干气低水锚点物理意义不同，不能在拟合合同里混为一个低端点。",
            "- 采样窗口必须在阀门打开时取得，并保持标准气或湿气持续开放流通，不能先关阀再取样。",
            "- 气路/水路样本只有在开放流通、阀门保持打开、ratio/露点/状态寄存器等证据满足要求时，才应进入正式候选系数。",
            "- S5/S6 是最终显示层线性修正，应在主链路 S1/S3、S2/S4 评审后再作为输出层修正处理。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_v1_5_full_flow_closure_readiness_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "v1_5_full_flow_closure_readiness.json"
    md_path = root / "v1_5_full_flow_closure_readiness.md"
    gaps_path = root / "v1_5_full_flow_closure_gaps.csv"
    devices_path = root / "v1_5_full_flow_device_closure.csv"
    stages_path = root / "v1_5_full_flow_closure_stages.csv"
    domains_path = root / "v1_5_full_flow_release_domains.csv"

    json_path.write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_v1_5_full_flow_closure_readiness_markdown(model), encoding="utf-8")
    _write_csv(gaps_path, list(model.get("gaps") or []))
    _write_csv(devices_path, list(model.get("devices") or []))
    _write_csv(stages_path, list(model.get("stage_statuses") or []))
    _write_csv(domains_path, list(model.get("closure_domains") or []))
    return {
        "readiness_json": json_path,
        "readiness_markdown": md_path,
        "gaps": gaps_path,
        "devices": devices_path,
        "stages": stages_path,
        "release_domains": domains_path,
    }
