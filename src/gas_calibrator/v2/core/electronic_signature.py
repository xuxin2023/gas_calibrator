"""Electronic signature and approval chain implementation.

Implements the electronic signature mechanism and multi-step approval chain
required by V1.3 sections 14.6 and 14.8. The existing codebase had only
placeholder "reviewer_dual_check_placeholder" with explicit disclaimers
that it was not real electronic signoff.

This module provides:
- Electronic signature with identity binding, timestamp, and intent
- Signature verification and tamper detection
- Multi-step approval chain with configurable stages
- Dual-review enforcement for critical actions
- Approval chain state machine with audit trail

Design constraints:
- Signatures are bound to a person_id from the operator roster
- Each signature records intent (approve, reject, acknowledge, review)
- Signatures include a content hash for tamper detection
- The approval chain is a state machine with explicit transitions
- Critical actions require dual review (two independent signatures)
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Sequence


# ---------------------------------------------------------------------------
# Signature types and data structures
# ---------------------------------------------------------------------------

class SignatureIntent(str, Enum):
    """Intent of an electronic signature."""
    APPROVE = "approve"
    REJECT = "reject"
    ACKNOWLEDGE = "acknowledge"
    REVIEW = "review"
    COUNTERSIGN = "countersign"
    WAIVE = "waive"


class ApprovalChainStatus(str, Enum):
    """Status of an approval chain."""
    PENDING = "pending"
    IN_REVIEW = "in_review"
    APPROVED = "approved"
    REJECTED = "rejected"
    COUNTERSIGNED = "countersigned"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class ElectronicSignature:
    """An electronic signature bound to a person and content.

    Attributes
    ----------
    signature_id : str
        Unique signature identifier.
    person_id : str
        Identity from the operator roster (e.g., "OP-SIM-LI").
    person_name : str
        Display name for human readability.
    person_role : str
        Role at time of signing (operator, reviewer, qa_observer).
    intent : str
        SignatureIntent value.
    content_hash : str
        SHA-256 hash of the signed content for tamper detection.
    content_description : str
        Human-readable description of what was signed.
    signed_at : str
        ISO 8601 timestamp.
    reason : str
        Reason/justification for the signature.
    is_electronic : bool
        Whether this is an electronic signature (vs. placeholder).
    metadata : dict
        Additional metadata (e.g., IP, session_id).
    """
    signature_id: str
    person_id: str
    person_name: str
    person_role: str
    intent: str
    content_hash: str
    content_description: str
    signed_at: str
    reason: str = ""
    is_electronic: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ApprovalStep:
    """A step in the approval chain.

    Attributes
    ----------
    step_id : str
        Unique step identifier.
    step_name : str
        Human-readable step name.
    required_role : str
        Role required to sign this step.
    required_intent : str
        Required signature intent.
    is_dual_review : bool
        Whether this step requires two independent signatures.
    signatures : list
        Collected signatures for this step.
    status : str
        Step status (pending, completed, rejected).
    """
    step_id: str
    step_name: str
    required_role: str
    required_intent: str = SignatureIntent.APPROVE
    is_dual_review: bool = False
    signatures: list[ElectronicSignature] = field(default_factory=list)
    status: str = "pending"


# ---------------------------------------------------------------------------
# Content hashing for tamper detection
# ---------------------------------------------------------------------------

def compute_content_hash(content: dict[str, Any] | str | bytes) -> str:
    """Compute SHA-256 hash of content for signature binding.

    Parameters
    ----------
    content : dict, str, or bytes
        The content to hash. Dicts are canonicalized via JSON serialization
        with sorted keys.

    Returns
    -------
    str
        Hex-encoded SHA-256 hash.
    """
    if isinstance(content, dict):
        raw = json.dumps(content, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
    elif isinstance(content, str):
        raw = content.encode("utf-8")
    else:
        raw = content
    return hashlib.sha256(raw).hexdigest()


def verify_content_hash(content: dict[str, Any] | str | bytes, expected_hash: str) -> bool:
    """Verify that content matches the expected hash.

    Returns True if the content has not been tampered with.
    """
    return compute_content_hash(content) == expected_hash


# ---------------------------------------------------------------------------
# Signature creation
# ---------------------------------------------------------------------------

def create_electronic_signature(
    *,
    person_id: str,
    person_name: str,
    person_role: str,
    intent: str,
    content: dict[str, Any] | str | bytes,
    content_description: str,
    reason: str = "",
    metadata: dict[str, Any] | None = None,
) -> ElectronicSignature:
    """Create an electronic signature bound to content.

    Parameters
    ----------
    person_id : str
        Identity from the operator roster.
    person_name : str
        Display name.
    person_role : str
        Role at time of signing.
    intent : str
        SignatureIntent value.
    content : dict, str, or bytes
        The content being signed.
    content_description : str
        Human-readable description of what is being signed.
    reason : str
        Reason/justification.
    metadata : dict, optional
        Additional metadata.

    Returns
    -------
    ElectronicSignature
    """
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    content_hash = compute_content_hash(content)
    signature_id = hashlib.sha256(
        f"{person_id}:{intent}:{content_hash}:{now}".encode("utf-8")
    ).hexdigest()[:24]

    return ElectronicSignature(
        signature_id=signature_id,
        person_id=person_id,
        person_name=person_name,
        person_role=person_role,
        intent=intent,
        content_hash=content_hash,
        content_description=content_description,
        signed_at=now,
        reason=reason,
        is_electronic=True,
        metadata=dict(metadata or {}),
    )


# ---------------------------------------------------------------------------
# Approval Chain
# ---------------------------------------------------------------------------

class ApprovalChain:
    """Multi-step approval chain with electronic signatures.

    The chain is a sequence of approval steps, each requiring one or more
    signatures. The chain advances only when the current step is completed.
    Critical steps can require dual review (two independent signatures from
    different people).

    Usage
    -----
    chain = ApprovalChain(
        chain_id="run-001-coefficient-release",
        chain_name="Coefficient Release Approval",
    )
    chain.add_step("operator_submit", "Operator Submit", "operator", is_dual_review=False)
    chain.add_step("reviewer_approve", "Reviewer Approval", "reviewer", is_dual_review=True)
    chain.add_step("qa_release", "QA Release", "qa_observer", is_dual_review=False)

    sig = create_electronic_signature(...)
    chain.sign_step("operator_submit", sig)
    """

    def __init__(
        self,
        *,
        chain_id: str,
        chain_name: str,
        chain_type: str = "generic",
    ) -> None:
        self.chain_id = chain_id
        self.chain_name = chain_name
        self.chain_type = chain_type
        self._steps: list[ApprovalStep] = []
        self._status = ApprovalChainStatus.PENDING
        self._created_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        self._completed_at: str | None = None
        self._audit_log: list[dict[str, Any]] = []

    # -- Step management -----------------------------------------------------

    def add_step(
        self,
        step_id: str,
        step_name: str,
        required_role: str,
        *,
        required_intent: str = SignatureIntent.APPROVE,
        is_dual_review: bool = False,
    ) -> None:
        """Add a step to the approval chain."""
        step = ApprovalStep(
            step_id=step_id,
            step_name=step_name,
            required_role=required_role,
            required_intent=required_intent,
            is_dual_review=is_dual_review,
        )
        self._steps.append(step)
        self._audit("step_added", step_id=step_id, step_name=step_name)

    # -- Signing -------------------------------------------------------------

    def sign_step(
        self,
        step_id: str,
        signature: ElectronicSignature,
    ) -> dict[str, Any]:
        """Apply a signature to a step in the approval chain.

        Returns a dict with keys: success, message, step_status, chain_status.
        """
        # Find the step
        step_index = None
        step = None
        for i, s in enumerate(self._steps):
            if s.step_id == step_id:
                step_index = i
                step = s
                break

        if step is None or step_index is None:
            return {"success": False, "message": f"Step {step_id} not found", "step_status": "error", "chain_status": self._status}

        # Check that this is the current step (all previous must be completed)
        for i in range(step_index):
            if self._steps[i].status != "completed":
                return {
                    "success": False,
                    "message": f"Previous step {self._steps[i].step_id} not completed",
                    "step_status": step.status,
                    "chain_status": self._status,
                }

        # Check role match
        if signature.person_role != step.required_role and step.required_intent != SignatureIntent.ACKNOWLEDGE:
            # Allow higher roles to sign for lower ones
            role_hierarchy = {"operator": 0, "reviewer": 1, "qa_observer": 2}
            signer_level = role_hierarchy.get(signature.person_role, -1)
            required_level = role_hierarchy.get(step.required_role, -1)
            if signer_level < required_level:
                return {
                    "success": False,
                    "message": f"Role {signature.person_role} insufficient for step requiring {step.required_role}",
                    "step_status": step.status,
                    "chain_status": self._status,
                }

        # Check intent match
        if signature.intent != step.required_intent and signature.intent != SignatureIntent.REJECT:
            return {
                "success": False,
                "message": f"Intent {signature.intent} does not match required {step.required_intent}",
                "step_status": step.status,
                "chain_status": self._status,
            }

        # For dual review, check that the same person doesn't sign twice
        if step.is_dual_review:
            existing_signers = {s.person_id for s in step.signatures}
            if signature.person_id in existing_signers:
                return {
                    "success": False,
                    "message": f"Dual review: {signature.person_id} already signed this step",
                    "step_status": step.status,
                    "chain_status": self._status,
                }

        # Apply the signature
        new_signatures = list(step.signatures) + [signature]

        # Determine step completion
        if signature.intent == SignatureIntent.REJECT:
            new_status = "rejected"
            self._status = ApprovalChainStatus.REJECTED
            self._completed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        elif step.is_dual_review:
            if len(new_signatures) >= 2:
                new_status = "completed"
            else:
                new_status = "pending"  # Still waiting for second signature
        else:
            new_status = "completed"

        # Update the step
        self._steps[step_index] = ApprovalStep(
            step_id=step.step_id,
            step_name=step.step_name,
            required_role=step.required_role,
            required_intent=step.required_intent,
            is_dual_review=step.is_dual_review,
            signatures=new_signatures,
            status=new_status,
        )

        self._audit(
            "step_signed",
            step_id=step_id,
            signature_id=signature.signature_id,
            person_id=signature.person_id,
            intent=signature.intent,
            step_status=new_status,
        )

        # Check if chain is complete
        if new_status == "completed" and step_index == len(self._steps) - 1:
            self._status = ApprovalChainStatus.APPROVED
            self._completed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            self._audit("chain_completed")
        elif new_status == "completed" and self._status == ApprovalChainStatus.PENDING:
            self._status = ApprovalChainStatus.IN_REVIEW

        return {
            "success": True,
            "message": f"Step {step_id} signed by {signature.person_id}",
            "step_status": new_status,
            "chain_status": self._status,
        }

    # -- Query methods -------------------------------------------------------

    @property
    def status(self) -> str:
        return self._status

    @property
    def steps(self) -> list[ApprovalStep]:
        return list(self._steps)

    @property
    def is_complete(self) -> bool:
        return self._status == ApprovalChainStatus.APPROVED

    @property
    def is_rejected(self) -> bool:
        return self._status == ApprovalChainStatus.REJECTED

    def current_step(self) -> ApprovalStep | None:
        """Return the next step that needs a signature."""
        for step in self._steps:
            if step.status == "pending":
                return step
        return None

    def get_all_signatures(self) -> list[ElectronicSignature]:
        """Collect all signatures from all steps."""
        sigs: list[ElectronicSignature] = []
        for step in self._steps:
            sigs.extend(step.signatures)
        return sigs

    def verify_integrity(self, content: dict[str, Any] | str | bytes) -> dict[str, Any]:
        """Verify that all signatures still match their content.

        Returns a dict with: valid, invalid_signatures, total_signatures.
        """
        current_hash = compute_content_hash(content)
        valid = True
        invalid: list[str] = []

        for sig in self.get_all_signatures():
            if sig.content_hash != current_hash:
                valid = False
                invalid.append(sig.signature_id)

        return {
            "valid": valid,
            "invalid_signatures": invalid,
            "total_signatures": len(self.get_all_signatures()),
            "content_hash": current_hash,
        }

    # -- Serialization -------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Serialize the approval chain to a dict."""
        return {
            "chain_id": self.chain_id,
            "chain_name": self.chain_name,
            "chain_type": self.chain_type,
            "status": self._status,
            "created_at": self._created_at,
            "completed_at": self._completed_at,
            "steps": [
                {
                    "step_id": s.step_id,
                    "step_name": s.step_name,
                    "required_role": s.required_role,
                    "required_intent": s.required_intent,
                    "is_dual_review": s.is_dual_review,
                    "status": s.status,
                    "signatures": [
                        {
                            "signature_id": sig.signature_id,
                            "person_id": sig.person_id,
                            "person_name": sig.person_name,
                            "person_role": sig.person_role,
                            "intent": sig.intent,
                            "content_hash": sig.content_hash,
                            "content_description": sig.content_description,
                            "signed_at": sig.signed_at,
                            "reason": sig.reason,
                            "is_electronic": sig.is_electronic,
                        }
                        for sig in s.signatures
                    ],
                }
                for s in self._steps
            ],
            "audit_log": self._audit_log,
        }

    # -- Internal ------------------------------------------------------------

    def _audit(self, action: str, **kwargs: Any) -> None:
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        self._audit_log.append({
            "timestamp": now,
            "action": action,
            **kwargs,
        })


# ---------------------------------------------------------------------------
# Predefined chain templates
# ---------------------------------------------------------------------------

def create_coefficient_release_chain(
    *,
    chain_id: str,
    run_id: str,
) -> ApprovalChain:
    """Create a standard coefficient release approval chain.

    Steps:
    1. Operator submits coefficients
    2. Reviewer approves (dual review)
    3. QA observer releases
    """
    chain = ApprovalChain(
        chain_id=chain_id,
        chain_name=f"Coefficient Release: {run_id}",
        chain_type="coefficient_release",
    )
    chain.add_step(
        "operator_submit",
        "Operator Submit",
        "operator",
        required_intent=SignatureIntent.APPROVE,
        is_dual_review=False,
    )
    chain.add_step(
        "reviewer_approve",
        "Reviewer Approval",
        "reviewer",
        required_intent=SignatureIntent.APPROVE,
        is_dual_review=True,
    )
    chain.add_step(
        "qa_release",
        "QA Release",
        "qa_observer",
        required_intent=SignatureIntent.APPROVE,
        is_dual_review=False,
    )
    return chain


def create_report_signoff_chain(
    *,
    chain_id: str,
    report_type: str,
) -> ApprovalChain:
    """Create a report sign-off approval chain.

    Steps:
    1. Author submits report
    2. Technical reviewer approves
    3. Quality manager signs off
    """
    chain = ApprovalChain(
        chain_id=chain_id,
        chain_name=f"Report Sign-off: {report_type}",
        chain_type="report_signoff",
    )
    chain.add_step(
        "author_submit",
        "Author Submit",
        "operator",
        required_intent=SignatureIntent.APPROVE,
    )
    chain.add_step(
        "technical_review",
        "Technical Review",
        "reviewer",
        required_intent=SignatureIntent.REVIEW,
        is_dual_review=True,
    )
    chain.add_step(
        "quality_signoff",
        "Quality Manager Sign-off",
        "qa_observer",
        required_intent=SignatureIntent.APPROVE,
    )
    return chain


def create_scope_release_chain(
    *,
    chain_id: str,
    scope_id: str,
) -> ApprovalChain:
    """Create a scope release approval chain.

    Steps:
    1. Engineer submits scope package
    2. Metrology reviewer approves (dual review)
    3. Quality manager releases scope
    """
    chain = ApprovalChain(
        chain_id=chain_id,
        chain_name=f"Scope Release: {scope_id}",
        chain_type="scope_release",
    )
    chain.add_step(
        "engineer_submit",
        "Engineer Submit",
        "operator",
        required_intent=SignatureIntent.APPROVE,
    )
    chain.add_step(
        "metrology_review",
        "Metrology Review",
        "reviewer",
        required_intent=SignatureIntent.APPROVE,
        is_dual_review=True,
    )
    chain.add_step(
        "quality_release",
        "Quality Release",
        "qa_observer",
        required_intent=SignatureIntent.APPROVE,
    )
    return chain
