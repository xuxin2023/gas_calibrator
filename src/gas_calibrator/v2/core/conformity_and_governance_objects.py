"""Conformity statement, acceptance contract, and governance policy objects.

Implements the missing V1.3 Appendix F objects:
- conformity_statement_profile
- acceptance_contract
- statement_template_version

Plus P1 governance policies:
- Tamper-proof mechanism (content integrity verification)
- Data retention policy
- Archival strategy
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Sequence


# ---------------------------------------------------------------------------
# Conformity Statement Profile (V1.3 Appendix F)
# ---------------------------------------------------------------------------

class ConformityStatementType(str, Enum):
    """Types of conformity statements per ILAC G8."""
    PASS = "pass"
    FAIL = "fail"
    INCONCLUSIVE = "inconclusive"
    NOT_STATED = "not_stated"
    READINESS_MAPPING_ONLY = "readiness_mapping_only"


class GuardBandPolicy(str, Enum):
    """Guard band policies for conformity assessment."""
    SIMPLE = "simple"           # Guard band = U
    SHARED = "shared"           # Guard band = U/2 (shared risk)
    NONE = "none"               # No guard band (binary pass/fail at limit)


@dataclass(frozen=True)
class ConformityStatementProfile:
    """Conformity statement profile per V1.3 Appendix F.

    Defines how conformity is stated for a given scope and decision rule.

    Attributes
    ----------
    profile_id : str
        Unique identifier.
    scope_id : str
        Bound scope ID.
    decision_rule_id : str
        Bound decision rule ID.
    statement_type : str
        ConformityStatementType value.
    guard_band_policy : str
        GuardBandPolicy value.
    acceptance_limit : float or None
        The acceptance limit (tolerance).
    guard_band_value : float or None
        The computed guard band value.
    uncertainty_source_id : str
        ID of the uncertainty budget used.
    statement_template_id : str
        ID of the statement template version.
    applicability : dict
        Applicability constraints (route, measurand, temperature range, etc.).
    non_claim_conditions : list
        Conditions under which no conformity statement is made.
    limitation_note : str
        Limitations of this conformity statement.
    """
    profile_id: str
    scope_id: str
    decision_rule_id: str
    statement_type: str = ConformityStatementType.NOT_STATED
    guard_band_policy: str = GuardBandPolicy.SIMPLE
    acceptance_limit: float | None = None
    guard_band_value: float | None = None
    uncertainty_source_id: str = ""
    statement_template_id: str = ""
    applicability: dict[str, Any] = field(default_factory=dict)
    non_claim_conditions: list[str] = field(default_factory=list)
    limitation_note: str = ""

    def assess_conformity(
        self,
        *,
        measured_value: float,
        reference_value: float,
        expanded_uncertainty: float,
    ) -> dict[str, Any]:
        """Perform conformity assessment using this profile.

        Returns a dict with: deviation, guard_band, effective_limit,
        in_tolerance, conformity_statement, decision_rule_applied.
        """
        deviation = abs(measured_value - reference_value)

        if self.guard_band_policy == GuardBandPolicy.SHARED:
            guard_band = expanded_uncertainty / 2.0
        elif self.guard_band_policy == GuardBandPolicy.NONE:
            guard_band = 0.0
        else:
            guard_band = expanded_uncertainty

        limit = self.acceptance_limit
        if limit is None:
            return {
                "conformity_statement": ConformityStatementType.NOT_STATED,
                "reason": "no acceptance limit defined",
            }

        effective_limit = limit - guard_band

        if deviation <= effective_limit:
            statement = ConformityStatementType.PASS
        elif deviation > limit + guard_band:
            statement = ConformityStatementType.FAIL
        else:
            statement = ConformityStatementType.INCONCLUSIVE

        return {
            "measured_value": measured_value,
            "reference_value": reference_value,
            "deviation": deviation,
            "expanded_uncertainty": expanded_uncertainty,
            "acceptance_limit": limit,
            "guard_band": guard_band,
            "guard_band_policy": self.guard_band_policy,
            "effective_acceptance_limit": effective_limit,
            "in_tolerance": deviation <= limit,
            "conformity_statement": statement,
            "decision_rule_applied": f"ILAC_G8_{self.guard_band_policy}_guard_band",
            "profile_id": self.profile_id,
            "scope_id": self.scope_id,
            "decision_rule_id": self.decision_rule_id,
        }


# ---------------------------------------------------------------------------
# Acceptance Contract (V1.3 Appendix F)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AcceptanceContract:
    """Acceptance contract binding scope, decision rule, and uncertainty.

    Per V1.3 Appendix F, the acceptance contract defines the complete
    set of conditions under which a result can be formally accepted.

    Attributes
    ----------
    contract_id : str
        Unique identifier.
    scope_id : str
        Bound scope.
    decision_rule_id : str
        Bound decision rule.
    uncertainty_case_id : str
        Bound uncertainty budget case.
    method_confirmation_protocol_id : str
        Bound method confirmation protocol.
    conformity_profile_id : str
        Bound conformity statement profile.
    acceptance_conditions : list
        Conditions that must all be met for acceptance.
    rejection_conditions : list
        Conditions that cause automatic rejection.
    reviewer_gate_required : bool
        Whether reviewer gate is required before acceptance.
    dual_review_required : bool
        Whether dual review is required.
    effective_from : str
        ISO date when this contract becomes effective.
    effective_until : str or None
        ISO date when this contract expires (None = no expiry).
    """
    contract_id: str
    scope_id: str
    decision_rule_id: str
    uncertainty_case_id: str
    method_confirmation_protocol_id: str
    conformity_profile_id: str
    acceptance_conditions: list[str] = field(default_factory=list)
    rejection_conditions: list[str] = field(default_factory=list)
    reviewer_gate_required: bool = True
    dual_review_required: bool = False
    effective_from: str = ""
    effective_until: str | None = None

    def evaluate(
        self,
        *,
        pre_run_gate_passed: bool = True,
        method_confirmation_passed: bool = True,
        uncertainty_budget_complete: bool = True,
        scope_coverage: bool = True,
        certificate_valid: bool = True,
    ) -> dict[str, Any]:
        """Evaluate whether all acceptance conditions are met.

        Returns a dict with: accepted, blocked_reasons, conditions_met,
        conditions_not_met.
        """
        conditions = {
            "pre_run_gate_passed": pre_run_gate_passed,
            "method_confirmation_passed": method_confirmation_passed,
            "uncertainty_budget_complete": uncertainty_budget_complete,
            "scope_coverage": scope_coverage,
            "certificate_valid": certificate_valid,
        }

        met = [k for k, v in conditions.items() if v]
        not_met = [k for k, v in conditions.items() if not v]

        accepted = len(not_met) == 0

        return {
            "contract_id": self.contract_id,
            "accepted": accepted,
            "conditions_met": met,
            "conditions_not_met": not_met,
            "blocked_reasons": not_met if not accepted else [],
            "reviewer_gate_required": self.reviewer_gate_required,
            "dual_review_required": self.dual_review_required,
        }


# ---------------------------------------------------------------------------
# Statement Template Version (V1.3 Appendix F)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class StatementTemplateVersion:
    """Version-managed conformity statement template.

    Attributes
    ----------
    template_id : str
        Unique identifier.
    template_version : str
        Semantic version (e.g., "1.0.0").
    template_type : str
        Type of template (calibration_certificate, test_report, conformity_statement).
    language : str
        Language code (zh, en).
    template_text : str
        The template text with placeholders (e.g., {measurand}, {result}, {uncertainty}).
    placeholder_keys : list
        List of placeholder keys in the template.
    source_standard : str
        The standard this template is derived from (e.g., "ILAC G8:09/2019").
    active : bool
        Whether this is the active version.
    effective_from : str
        ISO date.
    """
    template_id: str
    template_version: str
    template_type: str
    language: str = "zh"
    template_text: str = ""
    placeholder_keys: list[str] = field(default_factory=list)
    source_standard: str = ""
    active: bool = True
    effective_from: str = ""

    def render(self, values: dict[str, Any]) -> str:
        """Render the template with provided values."""
        result = self.template_text
        for key, value in values.items():
            result = result.replace(f"{{{key}}}", str(value))
        return result


# Predefined templates
CALIBRATION_CERTIFICATE_TEMPLATE_ZH = StatementTemplateVersion(
    template_id="calibration-certificate-v1",
    template_version="1.0.0",
    template_type="calibration_certificate",
    language="zh",
    template_text=(
        "校准证书\n\n"
        "委托方：{client}\n"
        "被校仪器：{analyzer_model}，编号：{analyzer_serial}\n"
        "校准依据：{method_reference}\n"
        "测量标准：{reference_standard}\n\n"
        "被测量：{measurand}\n"
        "校准结果：{measured_value} {unit}\n"
        "扩展不确定度：U = {expanded_uncertainty} {unit} (k = {coverage_factor})\n\n"
        "符合性声明：{conformity_statement}\n"
        "决策规则：{decision_rule}\n\n"
        "校准日期：{calibration_date}\n"
        "证书编号：{certificate_number}\n"
    ),
    placeholder_keys=[
        "client", "analyzer_model", "analyzer_serial", "method_reference",
        "reference_standard", "measurand", "measured_value", "unit",
        "expanded_uncertainty", "coverage_factor", "conformity_statement",
        "decision_rule", "calibration_date", "certificate_number",
    ],
    source_standard="ILAC G8:09/2019 / CNAS-CL01:2018",
    active=True,
    effective_from="2026-01-01",
)

CONFORMITY_STATEMENT_TEMPLATE_ZH = StatementTemplateVersion(
    template_id="conformity-statement-v1",
    template_version="1.0.0",
    template_type="conformity_statement",
    language="zh",
    template_text=(
        "符合性声明\n\n"
        "被测量：{measurand}\n"
        "测量结果：{measured_value} {unit}\n"
        "参考值：{reference_value} {unit}\n"
        "偏差：{deviation} {unit}\n"
        "扩展不确定度：U = {expanded_uncertainty} {unit} (k = {coverage_factor})\n"
        "验收限：{acceptance_limit} {unit}\n"
        "保护带策略：{guard_band_policy}\n\n"
        "声明：{conformity_statement}\n"
        "依据决策规则：{decision_rule}\n"
    ),
    placeholder_keys=[
        "measurand", "measured_value", "unit", "reference_value",
        "deviation", "expanded_uncertainty", "coverage_factor",
        "acceptance_limit", "guard_band_policy", "conformity_statement",
        "decision_rule",
    ],
    source_standard="ILAC G8:09/2019",
    active=True,
    effective_from="2026-01-01",
)


# ---------------------------------------------------------------------------
# Tamper-Proof Mechanism (P1-1)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class IntegritySeal:
    """Content integrity seal for tamper detection.

    Unlike the existing artifact_hash_registry which only records hashes,
    this seal provides:
    - Content hash (SHA-256)
    - Seal timestamp
    - Sealer identity
    - Chain hash (links to previous seal for append-only audit)
    """
    seal_id: str
    content_hash: str
    chain_hash: str
    sealed_at: str
    sealed_by: str
    content_description: str
    algorithm: str = "sha256"


class TamperProofStore:
    """Append-only tamper-proof store for critical artifacts.

    Uses a hash chain (similar to blockchain) where each new seal
    includes the hash of the previous seal, making any retroactive
    modification detectable.
    """

    def __init__(self, *, store_id: str) -> None:
        self.store_id = store_id
        self._seals: list[IntegritySeal] = []
        self._chain_hash = "genesis"  # Initial chain hash

    def seal_content(
        self,
        content: dict[str, Any] | str | bytes,
        *,
        sealed_by: str,
        content_description: str,
    ) -> IntegritySeal:
        """Create an integrity seal for content and append to the chain."""
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        if isinstance(content, dict):
            raw = json.dumps(content, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
        elif isinstance(content, str):
            raw = content.encode("utf-8")
        else:
            raw = content

        content_hash = hashlib.sha256(raw).hexdigest()

        # Chain hash includes previous chain hash + current content hash
        chain_input = f"{self._chain_hash}:{content_hash}:{now}".encode("utf-8")
        chain_hash = hashlib.sha256(chain_input).hexdigest()

        seal_id = hashlib.sha256(f"{self.store_id}:{len(self._seals)}:{now}".encode("utf-8")).hexdigest()[:20]

        seal = IntegritySeal(
            seal_id=seal_id,
            content_hash=content_hash,
            chain_hash=chain_hash,
            sealed_at=now,
            sealed_by=sealed_by,
            content_description=content_description,
        )

        self._seals.append(seal)
        self._chain_hash = chain_hash
        return seal

    def verify_chain(self) -> dict[str, Any]:
        """Verify the entire hash chain for tamper detection.

        Returns: valid, broken_at, total_seals.
        """
        if not self._seals:
            return {"valid": True, "broken_at": None, "total_seals": 0}

        prev_chain_hash = "genesis"
        for i, seal in enumerate(self._seals):
            chain_input = f"{prev_chain_hash}:{seal.content_hash}:{seal.sealed_at}".encode("utf-8")
            expected = hashlib.sha256(chain_input).hexdigest()
            if seal.chain_hash != expected:
                return {"valid": False, "broken_at": i, "total_seals": len(self._seals)}
            prev_chain_hash = seal.chain_hash

        return {"valid": True, "broken_at": None, "total_seals": len(self._seals)}

    def verify_content(
        self,
        seal_index: int,
        content: dict[str, Any] | str | bytes,
    ) -> dict[str, Any]:
        """Verify that content at a given seal index matches its hash."""
        if seal_index < 0 or seal_index >= len(self._seals):
            return {"valid": False, "reason": "index out of range"}

        seal = self._seals[seal_index]
        current_hash = hashlib.sha256(
            (json.dumps(content, sort_keys=True, ensure_ascii=False, default=str) if isinstance(content, dict) else str(content)).encode("utf-8")
        ).hexdigest()

        return {
            "valid": current_hash == seal.content_hash,
            "seal_id": seal.seal_id,
            "expected_hash": seal.content_hash,
            "actual_hash": current_hash,
        }

    @property
    def seals(self) -> list[IntegritySeal]:
        return list(self._seals)


# ---------------------------------------------------------------------------
# Data Retention Policy (P1-2)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RetentionPolicy:
    """Data retention policy for artifact lifecycle management.

    Attributes
    ----------
    policy_id : str
        Unique identifier.
    artifact_category : str
        Category of artifacts this policy applies to.
    retention_days : int
        Minimum retention period in days.
    archive_after_days : int or None
        Days after which artifacts are moved to archive storage.
    delete_after_days : int or None
        Days after which artifacts are permanently deleted (None = never).
    hold_override : bool
        Whether a legal hold can override deletion.
    regulatory_basis : str
        Regulatory requirement for this retention period.
    """
    policy_id: str
    artifact_category: str
    retention_days: int
    archive_after_days: int | None = None
    delete_after_days: int | None = None
    hold_override: bool = True
    regulatory_basis: str = ""

    def compute_action(
        self,
        *,
        age_days: int,
        has_legal_hold: bool = False,
    ) -> dict[str, Any]:
        """Determine the retention action for an artifact of given age."""
        if has_legal_hold and self.hold_override:
            return {"action": "hold", "reason": "legal hold active"}

        if self.delete_after_days is not None and age_days >= self.delete_after_days:
            return {"action": "delete", "reason": f"exceeded retention period ({self.delete_after_days} days)"}

        if self.archive_after_days is not None and age_days >= self.archive_after_days:
            return {"action": "archive", "reason": f"exceeded active period ({self.archive_after_days} days)"}

        if age_days < self.retention_days:
            return {"action": "retain", "reason": f"within retention period ({self.retention_days} days)"}

        return {"action": "retain", "reason": "no deletion scheduled"}


# Predefined retention policies per metrology requirements
DEFAULT_RETENTION_POLICIES: list[RetentionPolicy] = [
    RetentionPolicy(
        policy_id="retention-calibration-records",
        artifact_category="calibration_records",
        retention_days=365 * 6,    # 6 years per CNAS-CL01
        archive_after_days=365 * 2,
        delete_after_days=None,     # Never delete calibration records
        regulatory_basis="CNAS-CL01:2018 7.5 / ISO/IEC 17025:2017 7.5",
    ),
    RetentionPolicy(
        policy_id="retention-raw-measurement-data",
        artifact_category="raw_measurement_data",
        retention_days=365 * 6,
        archive_after_days=365,
        delete_after_days=None,
        regulatory_basis="CNAS-CL01:2018 7.5",
    ),
    RetentionPolicy(
        policy_id="retention-qc-records",
        artifact_category="qc_records",
        retention_days=365 * 3,
        archive_after_days=365,
        delete_after_days=365 * 10,
        regulatory_basis="internal quality management",
    ),
    RetentionPolicy(
        policy_id="retention-audit-logs",
        artifact_category="audit_logs",
        retention_days=365 * 3,
        archive_after_days=365 * 2,
        delete_after_days=None,     # Never delete audit logs
        regulatory_basis="ISO/IEC 17025:2017 7.5",
    ),
    RetentionPolicy(
        policy_id="retention-ai-advisory",
        artifact_category="ai_advisory",
        retention_days=365,
        archive_after_days=180,
        delete_after_days=365 * 3,
        regulatory_basis="internal AI governance",
    ),
]


# ---------------------------------------------------------------------------
# Archival Strategy (P1-3)
# ---------------------------------------------------------------------------

class ArchiveFormat(str, Enum):
    """Supported archive formats."""
    ZIP = "zip"
    TAR_GZ = "tar.gz"
    PARQUET = "parquet"
    JSONL = "jsonl"


@dataclass(frozen=True)
class ArchivalStrategy:
    """Strategy for archiving artifacts.

    Attributes
    ----------
    strategy_id : str
        Unique identifier.
    source_category : str
        Artifact category to archive.
    archive_format : str
        ArchiveFormat value.
    compression_level : int
        Compression level (0-9).
    include_metadata : bool
        Whether to include metadata manifest.
    verify_after_archive : bool
        Whether to verify integrity after archiving.
    archive_path_template : str
        Path template for archive destination.
    """
    strategy_id: str
    source_category: str
    archive_format: str = ArchiveFormat.ZIP
    compression_level: int = 6
    include_metadata: bool = True
    verify_after_archive: bool = True
    archive_path_template: str = "archive/{category}/{year}/{month}/"

    def compute_archive_path(
        self,
        *,
        category: str,
        timestamp: str,
    ) -> str:
        """Compute the archive destination path."""
        try:
            dt = datetime.fromisoformat(timestamp)
        except (ValueError, TypeError):
            dt = datetime.now(timezone.utc)
        return self.archive_path_template.format(
            category=category,
            year=dt.strftime("%Y"),
            month=dt.strftime("%m"),
        )
