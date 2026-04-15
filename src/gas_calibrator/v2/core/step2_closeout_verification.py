"""Step 2 final closeout verification — reviewer-first verification object
that answers "has Step 2 reached closeout candidate state?"

This is NOT a formal release or approval. It is a Step 2 verification layer
that aggregates:
  - step2_closeout_readiness
  - step2_closeout_package
  - step2_freeze_audit
  - step3_admission_dossier
  - governance_handoff / blockers / next_steps
  - parity / resilience / phase evidence key status

Step 2 boundary:
  - evidence_source = "simulated"
  - not_real_acceptance_evidence = True
  - not_ready_for_formal_claim = True
  - reviewer_only = True
  - readiness_mapping_only = True
  - primary_evidence_rewritten = False
  - real_acceptance_ready = False
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------

CLOSEOUT_VERIFICATION_VERSION: str = "2.24.0"

# ---------------------------------------------------------------------------
# Status values
# ---------------------------------------------------------------------------

VERIFICATION_STATUS_CANDIDATE = "closeout_candidate"
VERIFICATION_STATUS_BLOCKER = "blocker"
VERIFICATION_STATUS_REVIEWER_ONLY = "reviewer_only"


# ---------------------------------------------------------------------------
# build_step2_closeout_verification — main entry point
# ---------------------------------------------------------------------------

def build_step2_closeout_verification(
    *,
    run_id: str = "",
    step2_closeout_readiness: dict[str, Any] | None = None,
    step2_closeout_package: dict[str, Any] | None = None,
    step2_freeze_audit: dict[str, Any] | None = None,
    step3_admission_dossier: dict[str, Any] | None = None,
    governance_handoff: dict[str, Any] | None = None,
    parity_resilience_summary: dict[str, Any] | None = None,
    phase_evidence: dict[str, Any] | None = None,
    lang: str = "zh",
) -> dict[str, Any]:
    """Build Step 2 final closeout verification payload."""
    _readiness = dict(step2_closeout_readiness or {})
    _pkg = dict(step2_closeout_package or {})
    _audit = dict(step2_freeze_audit or {})
    _dossier = dict(step3_admission_dossier or {})
    _governance = dict(governance_handoff or {})
    _parity = dict(parity_resilience_summary or {})
    _phase = dict(phase_evidence or {})

    # --- Aggregate blockers ---
    blockers = _aggregate_blockers(
        readiness=_readiness,
        pkg=_pkg,
        audit=_audit,
        dossier=_dossier,
        governance=_governance,
        parity=_parity,
        lang=lang,
    )

    # --- Aggregate next_steps ---
    next_steps = _aggregate_next_steps(
        blockers=blockers,
        lang=lang,
    )

    # --- Derive verification_status ---
    verification_status = _derive_verification_status(blockers=blockers)

    # --- Build missing_for_step3 ---
    missing_for_step3 = _build_missing_for_step3(lang=lang)

    # --- Build simulation_only_boundary ---
    simulation_only_boundary = _build_simulation_only_boundary(lang=lang)

    # --- Build reviewer_summary_line ---
    reviewer_summary_line = _build_reviewer_summary_line(
        verification_status=verification_status,
        blockers=blockers,
        lang=lang,
    )

    return {
        "schema_version": "1.0",
        "artifact_type": "step2_closeout_verification",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": str(run_id or ""),
        "phase": "step2_closeout_verification",
        "verification_version": CLOSEOUT_VERIFICATION_VERSION,
        "verification_status": verification_status,
        "reviewer_summary_line": reviewer_summary_line,
        "blockers": blockers,
        "next_steps": next_steps,
        "missing_for_step3": missing_for_step3,
        "simulation_only_boundary": simulation_only_boundary,
        "closeout_verification_source": "rebuilt",
        "verification_source": "rebuilt",
        "verification_fallback_reason": "",
        # Source status snapshots
        "closeout_readiness_status": str(_readiness.get("closeout_status") or ""),
        "closeout_package_status": str(_pkg.get("package_status") or ""),
        "freeze_audit_status": str(_audit.get("audit_status") or ""),
        "dossier_status": str(_dossier.get("dossier_status") or ""),
        # Step 2 boundary markers — always enforced
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "primary_evidence_rewritten": False,
        "real_acceptance_ready": False,
    }


# ---------------------------------------------------------------------------
# Surface helper
# ---------------------------------------------------------------------------

def has_closeout_verification_inputs(
    *,
    step2_closeout_readiness: dict[str, Any] | None = None,
    step2_closeout_package: dict[str, Any] | None = None,
    step2_freeze_audit: dict[str, Any] | None = None,
    step3_admission_dossier: dict[str, Any] | None = None,
    governance_handoff: dict[str, Any] | None = None,
    parity_resilience_summary: dict[str, Any] | None = None,
    phase_evidence: dict[str, Any] | None = None,
) -> bool:
    return any(
        bool(dict(payload or {}))
        for payload in (
            step2_closeout_readiness,
            step2_closeout_package,
            step2_freeze_audit,
            step3_admission_dossier,
            governance_handoff,
            parity_resilience_summary,
            phase_evidence,
        )
    )


def build_step2_closeout_verification_surface_payload(
    *,
    run_id: str = "",
    step2_closeout_readiness: dict[str, Any] | None = None,
    step2_closeout_package: dict[str, Any] | None = None,
    step2_freeze_audit: dict[str, Any] | None = None,
    step3_admission_dossier: dict[str, Any] | None = None,
    governance_handoff: dict[str, Any] | None = None,
    parity_resilience_summary: dict[str, Any] | None = None,
    phase_evidence: dict[str, Any] | None = None,
    lang: str = "zh",
) -> dict[str, Any]:
    if not has_closeout_verification_inputs(
        step2_closeout_readiness=step2_closeout_readiness,
        step2_closeout_package=step2_closeout_package,
        step2_freeze_audit=step2_freeze_audit,
        step3_admission_dossier=step3_admission_dossier,
        governance_handoff=governance_handoff,
        parity_resilience_summary=parity_resilience_summary,
        phase_evidence=phase_evidence,
    ):
        result = build_closeout_verification_fallback(lang=lang)
        result["closeout_verification_source"] = "fallback"
        result["verification_source"] = "fallback"
        result["verification_fallback_reason"] = "missing_compatible_payload"
        return result
    result = build_step2_closeout_verification(
        run_id=run_id,
        step2_closeout_readiness=step2_closeout_readiness,
        step2_closeout_package=step2_closeout_package,
        step2_freeze_audit=step2_freeze_audit,
        step3_admission_dossier=step3_admission_dossier,
        governance_handoff=governance_handoff,
        parity_resilience_summary=parity_resilience_summary,
        phase_evidence=phase_evidence,
        lang=lang,
    )
    result["closeout_verification_source"] = "rebuilt"
    result["verification_source"] = "rebuilt"
    result["verification_fallback_reason"] = ""
    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _aggregate_blockers(
    *,
    readiness: dict[str, Any],
    pkg: dict[str, Any],
    audit: dict[str, Any],
    dossier: dict[str, Any],
    governance: dict[str, Any],
    parity: dict[str, Any],
    lang: str,
) -> list[dict[str, str]]:
    blockers: list[dict[str, str]] = []

    # Closeout readiness blocker
    closeout_status = str(readiness.get("closeout_status") or "")
    if closeout_status == "blocker":
        blockers.append({
            "key": "closeout_readiness_blocker",
            "label_zh": "收官就绪度存在阻塞项",
            "label_en": "Closeout readiness has blockers",
        })

    # Closeout package blocker
    pkg_status = str(pkg.get("package_status") or "")
    if pkg_status == "blocker":
        blockers.append({
            "key": "closeout_package_blocker",
            "label_zh": "收官包存在阻塞项",
            "label_en": "Closeout package has blockers",
        })

    # Freeze audit blocker
    audit_status = str(audit.get("audit_status") or "")
    if audit_status == "blocker":
        blockers.append({
            "key": "freeze_audit_blocker",
            "label_zh": "冻结审计存在阻塞项",
            "label_en": "Freeze audit has blockers",
        })

    # Dossier blocker (only count non-real-acceptance blockers from dossier)
    dossier_status = str(dossier.get("dossier_status") or "")
    dossier_blocker_keys = {b.get("key") for b in (dossier.get("blockers") or [])}
    non_real_dossier_blockers = dossier_blocker_keys - {"real_acceptance_not_ready"}
    if dossier_status == "blocker" and non_real_dossier_blockers:
        blockers.append({
            "key": "admission_dossier_blocker",
            "label_zh": "准入材料存在阻塞项",
            "label_en": "Admission dossier has blockers",
        })

    # Governance blocker
    gov_blockers = list(governance.get("blockers") or [])
    if gov_blockers:
        blockers.append({
            "key": "governance_blocker",
            "label_zh": "治理交接存在阻塞项",
            "label_en": "Governance handoff has blockers",
        })

    # Parity mismatch
    parity_status = str(parity.get("status") or parity.get("parity_status") or "")
    if parity_status in ("fail", "failed", "mismatch", "error"):
        blockers.append({
            "key": "parity_mismatch",
            "label_zh": "一致性校验未通过",
            "label_en": "Parity check failed",
        })

    # Real acceptance not ready (always true in Step 2)
    blockers.append({
        "key": "real_acceptance_not_ready",
        "label_zh": "真实验收尚未就绪（Step 2 仿真边界）",
        "label_en": "Real acceptance not ready (Step 2 simulation boundary)",
    })

    return blockers


def _aggregate_next_steps(
    *,
    blockers: list[dict[str, str]],
    lang: str,
) -> list[dict[str, str]]:
    next_steps: list[dict[str, str]] = []
    blocker_keys = {b.get("key") for b in blockers}

    if blocker_keys & {"closeout_readiness_blocker", "closeout_package_blocker",
                       "freeze_audit_blocker", "admission_dossier_blocker"}:
        next_steps.append({
            "key": "resolve_blockers",
            "label_zh": "解决当前阻塞项",
            "label_en": "Resolve current blockers",
        })

    if "parity_mismatch" in blocker_keys:
        next_steps.append({
            "key": "fix_parity",
            "label_zh": "修复一致性问题",
            "label_en": "Fix parity issues",
        })

    # Always: real device access needed for Step 3
    next_steps.append({
        "key": "obtain_real_device_access",
        "label_zh": "获取真实设备访问权限",
        "label_en": "Obtain real device access",
    })
    next_steps.append({
        "key": "run_step3_real_validation",
        "label_zh": "执行 Step 3 真实验证",
        "label_en": "Run Step 3 real validation",
    })

    return next_steps


def _derive_verification_status(
    *,
    blockers: list[dict[str, str]],
) -> str:
    """verification_status only expresses Step 2 closeout candidate state,
    never formal release."""
    # real_acceptance_not_ready is always a blocker in Step 2
    # So we distinguish: if only real_acceptance_not_ready, it's candidate
    # If other blockers exist, it's blocker
    non_real_blockers = [b for b in blockers if b.get("key") != "real_acceptance_not_ready"]
    if non_real_blockers:
        return VERIFICATION_STATUS_BLOCKER
    return VERIFICATION_STATUS_CANDIDATE


def _build_missing_for_step3(
    *,
    lang: str,
) -> list[str]:
    """Explicitly list what is missing for Step 3."""
    if lang == "en":
        return [
            "Real device access and real serial/COM connection",
            "Real compare / real verify execution",
            "Real acceptance evidence collection",
            "Real primary latest refresh",
        ]
    return [
        "真实设备访问与真实串口/COM 连接",
        "real compare / real verify 执行",
        "真实验收证据收集",
        "real primary latest 刷新",
    ]


def _build_simulation_only_boundary(
    *,
    lang: str,
) -> str:
    if lang == "en":
        return "Step 2 closeout verification: simulation-only boundary. Not real acceptance evidence."
    return "Step 2 收官验证：仿真边界。不代表真实验收证据。"


def _build_reviewer_summary_line(
    *,
    verification_status: str,
    blockers: list[dict[str, str]],
    lang: str,
) -> str:
    non_real_blockers = [b for b in blockers if b.get("key") != "real_acceptance_not_ready"]
    if lang == "en":
        if verification_status == VERIFICATION_STATUS_CANDIDATE:
            return "Step 2 closeout candidate: simulation evidence complete. Real acceptance not yet performed."
        return f"Step 2 closeout: {len(non_real_blockers)} blocker(s) remain. Not ready for Step 3."
    if verification_status == VERIFICATION_STATUS_CANDIDATE:
        return "Step 2 收官候选：仿真证据已完整。真实验收尚未执行。"
    return f"Step 2 收官：仍有 {len(non_real_blockers)} 个阻塞项。未就绪进入 Step 3。"


# ---------------------------------------------------------------------------
# Fallback
# ---------------------------------------------------------------------------

def build_closeout_verification_fallback(
    *,
    lang: str = "zh",
) -> dict[str, Any]:
    """Build closeout verification fallback."""
    _boundary = _build_simulation_only_boundary(lang=lang)
    _summary_line = (
        "Step 2 closeout verification: fallback — no persisted data. Not real acceptance evidence."
        if lang == "en"
        else "Step 2 收官验证：fallback — 无持久化数据。不代表真实验收证据。"
    )
    return {
        "schema_version": "1.0",
        "artifact_type": "step2_closeout_verification_fallback",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": "",
        "phase": "step2_closeout_verification",
        "verification_version": CLOSEOUT_VERIFICATION_VERSION,
        "verification_status": VERIFICATION_STATUS_REVIEWER_ONLY,
        "reviewer_summary_line": _summary_line,
        "blockers": [],
        "next_steps": [],
        "missing_for_step3": _build_missing_for_step3(lang=lang),
        "simulation_only_boundary": _boundary,
        "closeout_verification_source": "fallback",
        "verification_source": "fallback",
        "verification_fallback_reason": "missing_compatible_payload",
        "closeout_readiness_status": "",
        "closeout_package_status": "",
        "freeze_audit_status": "",
        "dossier_status": "",
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "primary_evidence_rewritten": False,
        "real_acceptance_ready": False,
    }
