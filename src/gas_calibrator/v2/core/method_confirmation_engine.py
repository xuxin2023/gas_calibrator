"""Method confirmation validation execution engine.

Implements the method confirmation validation matrix as an executable,
re-verifiable, reviewer-auditable system per V1.3 section 14.5.

The existing method_confirmation_repository.py only had matrix definitions
and rollup structures. This module provides actual validation execution
that can run validation checks against real or simulated data.

Key capabilities:
- Validation matrix definition with per-item pass/fail contracts
- Validation execution engine that runs each check and records results
- Route-specific validation (gas, water, ambient)
- Temperature/pressure/route-switch effect validation
- Seal ingress sensitivity and freshness checks
- Writeback verification
- Verification digest and rollup generation
- Revalidation trigger logic
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Sequence


# ---------------------------------------------------------------------------
# Validation item definitions
# ---------------------------------------------------------------------------

class ValidationCategory(str, Enum):
    """Categories of method confirmation validation items per V1.3 Appendix G."""
    LINEARITY = "linearity"
    REPEATABILITY = "repeatability"
    REPRODUCIBILITY = "reproducibility"
    DRIFT = "drift"
    TEMPERATURE_EFFECT = "temperature_effect"
    PRESSURE_EFFECT = "pressure_effect"
    ROUTE_SWITCH_EFFECT = "route_switch_effect"
    SEAL_INGRESS_SENSITIVITY = "seal_ingress_sensitivity"
    FRESHNESS_CHECK = "freshness_check"
    WRITEBACK_VERIFICATION = "writeback_verification"


class ValidationStatus(str, Enum):
    """Status of a validation item execution."""
    NOT_RUN = "not_run"
    PASS = "pass"
    FAIL = "fail"
    INCONCLUSIVE = "inconclusive"
    SKIPPED = "skipped"
    WAIVED = "waived"


@dataclass(frozen=True)
class ValidationItem:
    """A single validation check in the method confirmation matrix.

    Attributes
    ----------
    item_id : str
        Unique identifier (e.g., "linearity-co2-gas-25C").
    category : str
        ValidationCategory value.
    description : str
        Human-readable description of what is being validated.
    route_type : str
        Applicable route (gas, water, ambient, both).
    measurand : str
        What is being measured (CO2, H2O).
    temperature_point_c : float or None
        Temperature point if applicable.
    acceptance_criterion : str
        Description of the pass/fail criterion.
    acceptance_limit : float or None
        Numeric limit for pass/fail.
    unit : str
        Unit of the measured value.
    required_for_scope : bool
        Whether this item is required for scope release.
    """
    item_id: str
    category: str
    description: str
    route_type: str = "both"
    measurand: str = ""
    temperature_point_c: float | None = None
    acceptance_criterion: str = ""
    acceptance_limit: float | None = None
    unit: str = ""
    required_for_scope: bool = True


@dataclass(frozen=True)
class ValidationResult:
    """Result of executing a single validation item.

    Attributes
    ----------
    item_id : str
        The validated item's ID.
    status : str
        ValidationStatus value.
    measured_value : float or None
        The measured value from the validation run.
    deviation : float or None
        Deviation from reference/expected.
    acceptance_limit : float or None
        The limit that was applied.
    evidence_ref : str
        Reference to the evidence (run_id, artifact path, etc.).
    reviewer_note : str
        Note for reviewer.
    executed_at : str
        ISO timestamp of execution.
    """
    item_id: str
    status: str
    measured_value: float | None = None
    deviation: float | None = None
    acceptance_limit: float | None = None
    evidence_ref: str = ""
    reviewer_note: str = ""
    executed_at: str = ""


@dataclass(frozen=True)
class VerificationDigest:
    """Digest of a complete method confirmation verification run.

    Attributes
    ----------
    protocol_id : str
        Method confirmation protocol ID.
    scope_id : str
        Bound scope ID.
    total_items : int
        Total validation items in the matrix.
    passed : int
        Number of passed items.
    failed : int
        Number of failed items.
    inconclusive : int
        Number of inconclusive items.
    skipped : int
        Number of skipped items.
    waived : int
        Number of waived items.
    not_run : int
        Number of not-yet-run items.
    required_for_scope_passed : bool
        Whether all scope-required items passed.
    coverage_fraction : float
        Fraction of items that have been run.
    revalidation_triggers : list
        Items that triggered revalidation.
    """
    protocol_id: str
    scope_id: str
    total_items: int
    passed: int
    failed: int
    inconclusive: int
    skipped: int
    waived: int
    not_run: int
    required_for_scope_passed: bool
    coverage_fraction: float
    revalidation_triggers: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Validation Matrix Builder
# ---------------------------------------------------------------------------

def build_default_validation_matrix(
    *,
    route_types: Sequence[str] = ("gas", "water"),
    measurands: Sequence[str] = ("CO2", "H2O"),
    temperature_points_c: Sequence[float] = (0.0, 10.0, 20.0, 30.0, 40.0),
) -> list[ValidationItem]:
    """Build the default method confirmation validation matrix.

    Creates validation items for all categories specified in V1.3 Appendix G,
    covering the specified routes, measurands, and temperature points.
    """
    items: list[ValidationItem] = []
    idx = 0

    for route in route_types:
        for measurand in measurands:
            # Skip mismatched combinations
            if route == "gas" and measurand == "H2O":
                continue
            if route == "water" and measurand == "CO2":
                continue

            # Linearity
            idx += 1
            items.append(ValidationItem(
                item_id=f"linearity-{measurand.lower()}-{route}",
                category=ValidationCategory.LINEARITY,
                description=f"{measurand} linearity across concentration range ({route} route)",
                route_type=route,
                measurand=measurand,
                acceptance_criterion="R^2 >= 0.9999 or residual max < 0.5% of range",
                acceptance_limit=0.9999,
                unit="R_squared",
            ))

            # Repeatability at each temperature point
            for temp_c in temperature_points_c:
                idx += 1
                items.append(ValidationItem(
                    item_id=f"repeatability-{measurand.lower()}-{route}-{temp_c:.0f}C",
                    category=ValidationCategory.REPEATABILITY,
                    description=f"{measurand} repeatability at {temp_c:.0f}C ({route} route)",
                    route_type=route,
                    measurand=measurand,
                    temperature_point_c=temp_c,
                    acceptance_criterion="s_r < 0.3% of reading or s_r < specified repeatability limit",
                    acceptance_limit=0.003,
                    unit="relative",
                ))

            # Reproducibility
            idx += 1
            items.append(ValidationItem(
                item_id=f"reproducibility-{measurand.lower()}-{route}",
                category=ValidationCategory.REPRODUCIBILITY,
                description=f"{measurand} reproducibility between runs ({route} route)",
                route_type=route,
                measurand=measurand,
                acceptance_criterion="s_R < 0.5% of reading or within reproducibility specification",
                acceptance_limit=0.005,
                unit="relative",
            ))

            # Drift
            idx += 1
            items.append(ValidationItem(
                item_id=f"drift-{measurand.lower()}-{route}",
                category=ValidationCategory.DRIFT,
                description=f"{measurand} short-term drift during calibration ({route} route)",
                route_type=route,
                measurand=measurand,
                acceptance_criterion="drift < 0.2% of reading over calibration duration",
                acceptance_limit=0.002,
                unit="relative",
            ))

            # Temperature effect
            idx += 1
            items.append(ValidationItem(
                item_id=f"temperature-effect-{measurand.lower()}-{route}",
                category=ValidationCategory.TEMPERATURE_EFFECT,
                description=f"{measurand} temperature influence across {min(temperature_points_c):.0f}-{max(temperature_points_c):.0f}C ({route} route)",
                route_type=route,
                measurand=measurand,
                acceptance_criterion="temperature coefficient within specification",
                acceptance_limit=0.001,
                unit="per_degC_relative",
            ))

            # Pressure effect
            idx += 1
            items.append(ValidationItem(
                item_id=f"pressure-effect-{measurand.lower()}-{route}",
                category=ValidationCategory.PRESSURE_EFFECT,
                description=f"{measurand} pressure influence ({route} route)",
                route_type=route,
                measurand=measurand,
                acceptance_criterion="pressure coefficient within specification",
                acceptance_limit=0.001,
                unit="per_hPa_relative",
            ))

            # Route switch effect (only for systems with both routes)
            if len(route_types) > 1:
                idx += 1
                items.append(ValidationItem(
                    item_id=f"route-switch-effect-{measurand.lower()}",
                    category=ValidationCategory.ROUTE_SWITCH_EFFECT,
                    description=f"{measurand} route switching influence (gas <-> water)",
                    route_type="both",
                    measurand=measurand,
                    acceptance_criterion="route switch deviation < 0.3% of reading",
                    acceptance_limit=0.003,
                    unit="relative",
                ))

            # Seal ingress sensitivity (water route only)
            if route == "water":
                idx += 1
                items.append(ValidationItem(
                    item_id=f"seal-ingress-{measurand.lower()}-{route}",
                    category=ValidationCategory.SEAL_INGRESS_SENSITIVITY,
                    description=f"{measurand} seal ingress sensitivity ({route} route)",
                    route_type=route,
                    measurand=measurand,
                    acceptance_criterion="seal ingress contribution < 0.5% of expanded uncertainty",
                    acceptance_limit=0.005,
                    unit="relative",
                ))

            # Freshness check
            idx += 1
            items.append(ValidationItem(
                item_id=f"freshness-{measurand.lower()}-{route}",
                category=ValidationCategory.FRESHNESS_CHECK,
                description=f"{measurand} sample freshness / frame staleness ({route} route)",
                route_type=route,
                measurand=measurand,
                acceptance_criterion="all frames within freshness window; no stale frames in sampling period",
                acceptance_limit=1.0,
                unit="stale_frame_count",
            ))

            # Writeback verification
            idx += 1
            items.append(ValidationItem(
                item_id=f"writeback-{measurand.lower()}-{route}",
                category=ValidationCategory.WRITEBACK_VERIFICATION,
                description=f"{measurand} coefficient writeback verification ({route} route)",
                route_type=route,
                measurand=measurand,
                acceptance_criterion="writeback echo deviation < 0.1% of coefficient value",
                acceptance_limit=0.001,
                unit="relative",
            ))

    return items


# ---------------------------------------------------------------------------
# Validation Execution Engine
# ---------------------------------------------------------------------------

class MethodConfirmationEngine:
    """Method confirmation validation execution engine.

    Executes validation items from the matrix against provided data
    and produces verification results, digest, and rollup.
    """

    def __init__(
        self,
        *,
        protocol_id: str,
        scope_id: str,
        validation_matrix: Sequence[ValidationItem] | None = None,
    ) -> None:
        self.protocol_id = protocol_id
        self.scope_id = scope_id
        self.matrix = list(validation_matrix or [])
        self._results: dict[str, ValidationResult] = {}

    # -- Execute validation items --------------------------------------------

    def execute_item(
        self,
        item: ValidationItem,
        *,
        measured_value: float | None = None,
        deviation: float | None = None,
        acceptance_limit: float | None = None,
        evidence_ref: str = "",
        reviewer_note: str = "",
        status_override: str | None = None,
    ) -> ValidationResult:
        """Execute a single validation item and record the result.

        If status_override is provided, use it directly. Otherwise,
        determine pass/fail from deviation vs acceptance_limit.
        """
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        limit = acceptance_limit or item.acceptance_limit

        if status_override is not None:
            status = status_override
        elif deviation is not None and limit is not None:
            if deviation <= limit:
                status = ValidationStatus.PASS
            else:
                status = ValidationStatus.FAIL
        elif measured_value is not None and limit is not None:
            # For metrics like R^2 where higher is better
            if item.category == ValidationCategory.LINEARITY:
                status = ValidationStatus.PASS if measured_value >= limit else ValidationStatus.FAIL
            else:
                status = ValidationStatus.PASS if measured_value <= limit else ValidationStatus.FAIL
        else:
            status = ValidationStatus.INCONCLUSIVE

        result = ValidationResult(
            item_id=item.item_id,
            status=status,
            measured_value=measured_value,
            deviation=deviation,
            acceptance_limit=limit,
            evidence_ref=evidence_ref,
            reviewer_note=reviewer_note,
            executed_at=now,
        )
        self._results[item.item_id] = result
        return result

    def execute_batch(
        self,
        items_and_data: Sequence[tuple[ValidationItem, dict[str, Any]]],
    ) -> list[ValidationResult]:
        """Execute a batch of validation items.

        Parameters
        ----------
        items_and_data : sequence of (ValidationItem, data_dict)
            Each tuple contains a validation item and a dict with keys:
            measured_value, deviation, acceptance_limit, evidence_ref,
            reviewer_note, status_override.
        """
        results: list[ValidationResult] = []
        for item, data in items_and_data:
            result = self.execute_item(item, **data)
            results.append(result)
        return results

    def mark_skipped(
        self,
        item: ValidationItem,
        *,
        reason: str = "",
    ) -> ValidationResult:
        """Mark a validation item as skipped."""
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        result = ValidationResult(
            item_id=item.item_id,
            status=ValidationStatus.SKIPPED,
            reviewer_note=reason or "Skipped",
            executed_at=now,
        )
        self._results[item.item_id] = result
        return result

    def mark_waived(
        self,
        item: ValidationItem,
        *,
        reason: str = "",
    ) -> ValidationResult:
        """Mark a validation item as waived (with justification)."""
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        result = ValidationResult(
            item_id=item.item_id,
            status=ValidationStatus.WAIVED,
            reviewer_note=reason or "Waived with justification",
            executed_at=now,
        )
        self._results[item.item_id] = result
        return result

    # -- Digest and rollup --------------------------------------------------

    def compute_digest(self) -> VerificationDigest:
        """Compute the verification digest from all executed results."""
        all_items = self.matrix
        results = self._results

        counts = {
            ValidationStatus.PASS: 0,
            ValidationStatus.FAIL: 0,
            ValidationStatus.INCONCLUSIVE: 0,
            ValidationStatus.SKIPPED: 0,
            ValidationStatus.WAIVED: 0,
            ValidationStatus.NOT_RUN: 0,
        }

        revalidation_triggers: list[str] = []
        required_for_scope_passed = True

        for item in all_items:
            result = results.get(item.item_id)
            if result is None:
                counts[ValidationStatus.NOT_RUN] += 1
                if item.required_for_scope:
                    required_for_scope_passed = False
            else:
                counts[result.status] = counts.get(result.status, 0) + 1
                if result.status == ValidationStatus.FAIL and item.required_for_scope:
                    required_for_scope_passed = False
                    revalidation_triggers.append(item.item_id)
                if result.status == ValidationStatus.INCONCLUSIVE and item.required_for_scope:
                    revalidation_triggers.append(item.item_id)

        total = len(all_items)
        run_count = total - counts[ValidationStatus.NOT_RUN]
        coverage = run_count / total if total > 0 else 0.0

        return VerificationDigest(
            protocol_id=self.protocol_id,
            scope_id=self.scope_id,
            total_items=total,
            passed=counts[ValidationStatus.PASS],
            failed=counts[ValidationStatus.FAIL],
            inconclusive=counts[ValidationStatus.INCONCLUSIVE],
            skipped=counts[ValidationStatus.SKIPPED],
            waived=counts[ValidationStatus.WAIVED],
            not_run=counts[ValidationStatus.NOT_RUN],
            required_for_scope_passed=required_for_scope_passed,
            coverage_fraction=coverage,
            revalidation_triggers=revalidation_triggers,
        )

    def compute_rollup(self) -> dict[str, Any]:
        """Compute the full verification rollup for reviewer consumption."""
        digest = self.compute_digest()
        results_by_category: dict[str, list[dict[str, Any]]] = {}

        for item in self.matrix:
            result = self._results.get(item.item_id)
            category = item.category
            entry = {
                "item_id": item.item_id,
                "description": item.description,
                "route_type": item.route_type,
                "measurand": item.measurand,
                "required_for_scope": item.required_for_scope,
                "acceptance_criterion": item.acceptance_criterion,
            }
            if result is not None:
                entry.update({
                    "status": result.status,
                    "measured_value": result.measured_value,
                    "deviation": result.deviation,
                    "acceptance_limit": result.acceptance_limit,
                    "evidence_ref": result.evidence_ref,
                    "reviewer_note": result.reviewer_note,
                    "executed_at": result.executed_at,
                })
            else:
                entry["status"] = ValidationStatus.NOT_RUN

            results_by_category.setdefault(category, []).append(entry)

        return {
            "protocol_id": self.protocol_id,
            "scope_id": self.scope_id,
            "digest": {
                "total_items": digest.total_items,
                "passed": digest.passed,
                "failed": digest.failed,
                "inconclusive": digest.inconclusive,
                "skipped": digest.skipped,
                "waived": digest.waived,
                "not_run": digest.not_run,
                "required_for_scope_passed": digest.required_for_scope_passed,
                "coverage_fraction": digest.coverage_fraction,
                "revalidation_triggers": digest.revalidation_triggers,
            },
            "results_by_category": results_by_category,
            "scope_release_candidate": digest.required_for_scope_passed and digest.coverage_fraction >= 1.0,
            "revalidation_required": len(digest.revalidation_triggers) > 0,
            "reviewer_summary": (
                f"Method confirmation {self.protocol_id}: "
                f"{digest.passed}/{digest.total_items} passed, "
                f"{digest.failed} failed, {digest.inconclusive} inconclusive. "
                f"Scope release: {'candidate' if digest.required_for_scope_passed else 'blocked'}. "
                f"Coverage: {digest.coverage_fraction:.0%}."
            ),
        }

    # -- Serialization ------------------------------------------------------

    @staticmethod
    def validation_item_to_dict(item: ValidationItem) -> dict[str, Any]:
        return {
            "item_id": item.item_id,
            "category": item.category,
            "description": item.description,
            "route_type": item.route_type,
            "measurand": item.measurand,
            "temperature_point_c": item.temperature_point_c,
            "acceptance_criterion": item.acceptance_criterion,
            "acceptance_limit": item.acceptance_limit,
            "unit": item.unit,
            "required_for_scope": item.required_for_scope,
        }

    @staticmethod
    def validation_result_to_dict(result: ValidationResult) -> dict[str, Any]:
        return {
            "item_id": result.item_id,
            "status": result.status,
            "measured_value": result.measured_value,
            "deviation": result.deviation,
            "acceptance_limit": result.acceptance_limit,
            "evidence_ref": result.evidence_ref,
            "reviewer_note": result.reviewer_note,
            "executed_at": result.executed_at,
        }
