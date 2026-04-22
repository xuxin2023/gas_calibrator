"""Uncertainty budget GUM synthesis engine.

Implements the Guide to the Expression of Uncertainty in Measurement (GUM)
law of propagation of uncertainty for combining standard uncertainties
through a measurement model using sensitivity coefficients.

This engine closes the gap identified in the V1.3 analysis: the existing
uncertainty_builder.py only had placeholder/skeleton values. This module
provides actual GUM computation that can be wired into the builder pipeline.

Key capabilities:
- Distribution-aware standard uncertainty conversion (normal, rectangular, triangular, t-distribution)
- GUM law of propagation: u_c = sqrt(sum(c_i^2 * u_i^2) + 2*sum(c_i*c_j*u_i*u_j*r_ij))
- Correlation support between input quantities
- Coverage factor selection (k=2 for normal, effective degrees of freedom via Welch-Satterthwaite)
- Point / route / result three-level budget aggregation
- Component grouping (repeatability, reference, fit_residual, environmental, etc.)
- Golden case validation
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from math import isclose, sqrt
from typing import Any, Sequence


# ---------------------------------------------------------------------------
# Distribution types and divisors
# ---------------------------------------------------------------------------

class DistributionType(str, Enum):
    """Supported probability distribution types for uncertainty inputs."""
    NORMAL = "normal"
    RECTANGULAR = "rectangular"
    TRIANGULAR = "triangular"
    T_DISTRIBUTION = "t_distribution"
    ARCSINE = "arcsine"
    U_SHAPED = "u_shaped"


# Divisors to convert half-width or expanded uncertainty to standard uncertainty
# per GUM / JCGM 100:2008 Table 1
DISTRIBUTION_DIVISORS: dict[str, float] = {
    DistributionType.NORMAL: 1.0,           # u = sigma (already standard)
    DistributionType.RECTANGULAR: sqrt(3),   # u = a / sqrt(3)
    DistributionType.TRIANGULAR: sqrt(6),    # u = a / sqrt(6)
    DistributionType.T_DISTRIBUTION: 1.0,    # requires explicit degrees_of_freedom
    DistributionType.ARCSINE: sqrt(2),       # u = a / sqrt(2)
    DistributionType.U_SHAPED: sqrt(2),      # u = a / sqrt(2)
}


def standard_uncertainty_from_half_width(
    half_width: float,
    distribution_type: str,
    *,
    divisor_override: float | None = None,
) -> float:
    """Convert a half-width (or expanded uncertainty half-range) to standard
    uncertainty using the appropriate distribution divisor.

    Parameters
    ----------
    half_width : float
        The half-width *a* of the distribution (for rectangular/triangular)
        or the standard uncertainty directly (for normal).
    distribution_type : str
        One of the DistributionType values.
    divisor_override : float, optional
        If provided, use this divisor instead of the default for the distribution.

    Returns
    -------
    float
        The standard uncertainty u(x).
    """
    if half_width <= 0.0:
        return 0.0
    divisor = divisor_override or DISTRIBUTION_DIVISORS.get(
        distribution_type, 1.0
    )
    if divisor <= 0.0:
        divisor = 1.0
    return half_width / divisor


# ---------------------------------------------------------------------------
# Core data structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class UncertaintyInput:
    """A single input quantity to the uncertainty budget.

    Attributes
    ----------
    quantity_key : str
        Unique key for this input (e.g., "reference_setpoint").
    quantity_label : str
        Human-readable label.
    quantity_value : float
        Best estimate of the quantity.
    standard_uncertainty : float
        Standard uncertainty u(x_i).
    distribution_type : str
        Distribution type (normal, rectangular, triangular, etc.).
    sensitivity_coefficient : float
        Partial derivative c_i = df/dx_i.
    unit : str
        Unit of the quantity.
    component_key : str
        Which component group this input belongs to
        (e.g., "repeatability_component", "reference_component").
    degrees_of_freedom : float
        Effective degrees of freedom (inf for large samples).
    source_type : str
        Type of source instrument/asset.
    source_note : str
        Additional note about the source.
    """
    quantity_key: str
    quantity_label: str
    quantity_value: float
    standard_uncertainty: float
    distribution_type: str = "normal"
    sensitivity_coefficient: float = 1.0
    unit: str = ""
    component_key: str = ""
    degrees_of_freedom: float = float("inf")
    source_type: str = ""
    source_note: str = ""


@dataclass(frozen=True)
class CorrelationPair:
    """Correlation coefficient between two input quantities.

    Attributes
    ----------
    quantity_key_i : str
        Key of the first input.
    quantity_key_j : str
        Key of the second input.
    correlation_coefficient : float
        r(x_i, x_j), must be in [-1, 1].
    """
    quantity_key_i: str
    quantity_key_j: str
    correlation_coefficient: float


@dataclass(frozen=True)
class BudgetResult:
    """Result of an uncertainty budget computation.

    Attributes
    ----------
    uncertainty_case_id : str
        Unique identifier for this budget case.
    measurand : str
        What is being measured (e.g., "CO2", "H2O").
    route_type : str
        Route type (gas, water, ambient).
    budget_level : str
        Aggregation level (point, route, result).
    combined_standard_uncertainty : float
        u_c(y) from GUM law of propagation.
    expanded_uncertainty : float
        U = k * u_c(y).
    coverage_factor : float
        k (typically 2.0 for ~95% confidence).
    effective_degrees_of_freedom : float
        nu_eff from Welch-Satterthwaite formula.
    component_contributions : dict
        {component_key: (u_component, variance_fraction)}.
    top_contributors : list
        Top N contributing components sorted by variance fraction.
    input_contributions : list
        Per-input contribution details.
    correlation_contribution : float
        Total contribution from correlation terms.
    golden_case_status : str
        "match" / "mismatch" / "not_checked".
    calculation_chain : list
        Step-by-step calculation trace for audit.
    """
    uncertainty_case_id: str
    measurand: str
    route_type: str
    budget_level: str
    combined_standard_uncertainty: float
    expanded_uncertainty: float
    coverage_factor: float
    effective_degrees_of_freedom: float
    component_contributions: dict[str, tuple[float, float]] = field(default_factory=dict)
    top_contributors: list[dict[str, Any]] = field(default_factory=list)
    input_contributions: list[dict[str, Any]] = field(default_factory=list)
    correlation_contribution: float = 0.0
    golden_case_status: str = "not_checked"
    calculation_chain: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# GUM Engine
# ---------------------------------------------------------------------------

class GUMUncertaintyEngine:
    """GUM law of propagation of uncertainty engine.

    Implements:
    1. Standard uncertainty computation from distribution-aware inputs
    2. GUM law: u_c^2 = sum(c_i^2 * u_i^2) + 2*sum(c_i*c_j*u_i*u_j*r_ij)
    3. Component grouping and variance decomposition
    4. Welch-Satterthwaite effective degrees of freedom
    5. Coverage factor selection
    6. Golden case validation
    """

    def __init__(
        self,
        *,
        default_coverage_factor: float = 2.0,
        top_n_contributors: int = 3,
        golden_tolerance: float = 1e-6,
    ) -> None:
        self.default_coverage_factor = default_coverage_factor
        self.top_n_contributors = top_n_contributors
        self.golden_tolerance = golden_tolerance

    # -- Core GUM computation ------------------------------------------------

    def compute_budget(
        self,
        *,
        uncertainty_case_id: str,
        measurand: str,
        route_type: str,
        budget_level: str,
        inputs: Sequence[UncertaintyInput],
        correlations: Sequence[CorrelationPair] | None = None,
        coverage_factor: float | None = None,
        expected_combined_uncertainty: float | None = None,
        expected_expanded_uncertainty: float | None = None,
    ) -> BudgetResult:
        """Compute a complete uncertainty budget using GUM law of propagation.

        Parameters
        ----------
        uncertainty_case_id : str
            Unique identifier for this budget case.
        measurand : str
            What is being measured.
        route_type : str
            Route type (gas, water, ambient).
        budget_level : str
            Aggregation level (point, route, result).
        inputs : sequence of UncertaintyInput
            All input quantities with their uncertainties and sensitivity coefficients.
        correlations : sequence of CorrelationPair, optional
            Correlation coefficients between input pairs.
        coverage_factor : float, optional
            Override coverage factor (default: 2.0).
        expected_combined_uncertainty : float, optional
            Expected u_c for golden case validation.
        expected_expanded_uncertainty : float, optional
            Expected U for golden case validation.

        Returns
        -------
        BudgetResult
            Complete budget result with all computed values.
        """
        chain: list[str] = []
        k = coverage_factor or self.default_coverage_factor

        # Step 1: Compute per-input variance contributions c_i^2 * u_i^2
        input_contributions: list[dict[str, Any]] = []
        total_uncorrelated_variance = 0.0

        for inp in inputs:
            variance_contribution = (inp.sensitivity_coefficient ** 2) * (inp.standard_uncertainty ** 2)
            total_uncorrelated_variance += variance_contribution
            input_contributions.append({
                "quantity_key": inp.quantity_key,
                "quantity_value": inp.quantity_value,
                "standard_uncertainty": inp.standard_uncertainty,
                "sensitivity_coefficient": inp.sensitivity_coefficient,
                "variance_contribution": variance_contribution,
                "component_key": inp.component_key,
                "degrees_of_freedom": inp.degrees_of_freedom,
            })
            chain.append(
                f"c({inp.quantity_key})={inp.sensitivity_coefficient:.6g} * "
                f"u({inp.quantity_key})={inp.standard_uncertainty:.6g} -> "
                f"var={variance_contribution:.6e}"
            )

        # Step 2: Correlation terms 2 * c_i * c_j * u_i * u_j * r_ij
        correlation_contribution = 0.0
        input_by_key = {inp.quantity_key: inp for inp in inputs}

        for corr in (correlations or []):
            inp_i = input_by_key.get(corr.quantity_key_i)
            inp_j = input_by_key.get(corr.quantity_key_j)
            if inp_i is None or inp_j is None:
                continue
            r = max(-1.0, min(1.0, corr.correlation_coefficient))
            term = 2.0 * inp_i.sensitivity_coefficient * inp_j.sensitivity_coefficient * inp_i.standard_uncertainty * inp_j.standard_uncertainty * r
            correlation_contribution += term
            chain.append(
                f"corr({corr.quantity_key_i},{corr.quantity_key_j}): "
                f"r={r:.4f} -> 2*c_i*c_j*u_i*u_j*r = {term:.6e}"
            )

        # Step 3: Combined standard uncertainty
        total_variance = total_uncorrelated_variance + correlation_contribution
        if total_variance < 0.0:
            chain.append(f"WARNING: total variance negative ({total_variance:.6e}), clamping to 0")
            total_variance = 0.0
        u_c = sqrt(total_variance)
        chain.append(f"u_c = sqrt({total_variance:.6e}) = {u_c:.6f}")

        # Step 4: Component grouping
        component_variances: dict[str, float] = {}
        for contrib in input_contributions:
            comp_key = contrib["component_key"] or "unassigned"
            component_variances[comp_key] = component_variances.get(comp_key, 0.0) + contrib["variance_contribution"]

        component_contributions: dict[str, tuple[float, float]] = {}
        for comp_key, comp_var in component_variances.items():
            comp_u = sqrt(max(0.0, comp_var))
            fraction = comp_var / total_variance if total_variance > 0.0 else 0.0
            component_contributions[comp_key] = (comp_u, fraction)
            chain.append(f"component {comp_key}: u_comp={comp_u:.6f}, fraction={fraction:.4f}")

        # Step 5: Top contributors
        sorted_components = sorted(
            component_contributions.items(),
            key=lambda item: item[1][1],
            reverse=True,
        )
        top_contributors = [
            {
                "component_key": comp_key,
                "component_uncertainty": comp_u,
                "variance_fraction": fraction,
            }
            for comp_key, (comp_u, fraction) in sorted_components[:self.top_n_contributors]
            if fraction > 0.0
        ]

        # Step 6: Welch-Satterthwaite effective degrees of freedom
        nu_eff = self._welch_satterthwaite(
            u_c=u_c,
            input_contributions=input_contributions,
        )
        chain.append(f"nu_eff (Welch-Satterthwaite) = {nu_eff:.1f}")

        # Step 7: Expanded uncertainty
        U = u_c * k
        chain.append(f"U = k * u_c = {k} * {u_c:.6f} = {U:.6f}")

        # Step 8: Golden case validation
        golden_status = "not_checked"
        if expected_combined_uncertainty is not None and expected_expanded_uncertainty is not None:
            if isclose(u_c, expected_combined_uncertainty, abs_tol=self.golden_tolerance) and \
               isclose(U, expected_expanded_uncertainty, abs_tol=self.golden_tolerance):
                golden_status = "match"
            else:
                golden_status = "mismatch"
            chain.append(
                f"golden check: u_c={u_c:.6f} vs expected={expected_combined_uncertainty:.6f}, "
                f"U={U:.6f} vs expected={expected_expanded_uncertainty:.6f} -> {golden_status}"
            )

        return BudgetResult(
            uncertainty_case_id=uncertainty_case_id,
            measurand=measurand,
            route_type=route_type,
            budget_level=budget_level,
            combined_standard_uncertainty=u_c,
            expanded_uncertainty=U,
            coverage_factor=k,
            effective_degrees_of_freedom=nu_eff,
            component_contributions=component_contributions,
            top_contributors=top_contributors,
            input_contributions=input_contributions,
            correlation_contribution=correlation_contribution,
            golden_case_status=golden_status,
            calculation_chain=chain,
        )

    # -- Multi-level aggregation ---------------------------------------------

    def aggregate_route_budget(
        self,
        point_budgets: Sequence[BudgetResult],
        *,
        route_type: str,
        measurand: str,
        uncertainty_case_id: str,
    ) -> BudgetResult:
        """Aggregate point-level budgets into a route-level budget.

        The route-level combined uncertainty is the RSS (root-sum-square)
        of the point-level combined uncertainties, which is appropriate
        when the point budgets are independent.

        For correlated point budgets, use compute_budget() directly
        with explicit correlation pairs.
        """
        if not point_budgets:
            return BudgetResult(
                uncertainty_case_id=uncertainty_case_id,
                measurand=measurand,
                route_type=route_type,
                budget_level="route",
                combined_standard_uncertainty=0.0,
                expanded_uncertainty=0.0,
                coverage_factor=self.default_coverage_factor,
                effective_degrees_of_freedom=float("inf"),
            )

        total_var = sum(b.combined_standard_uncertainty ** 2 for b in point_budgets)
        u_c = sqrt(total_var)
        k = self.default_coverage_factor
        U = u_c * k

        # Merge component contributions
        merged_components: dict[str, tuple[float, float]] = {}
        for b in point_budgets:
            for comp_key, (comp_u, _) in b.component_contributions.items():
                prev_var = merged_components.get(comp_key, (0.0, 0.0))[0] ** 2
                merged_components[comp_key] = (sqrt(prev_var + comp_u ** 2), 0.0)

        # Recompute fractions
        if total_var > 0.0:
            merged_components = {
                k: (u, u ** 2 / total_var)
                for k, (u, _) in merged_components.items()
            }

        top_contributors = sorted(
            [
                {"component_key": k, "component_uncertainty": u, "variance_fraction": f}
                for k, (u, f) in merged_components.items()
                if f > 0.0
            ],
            key=lambda x: x["variance_fraction"],
            reverse=True,
        )[:self.top_n_contributors]

        return BudgetResult(
            uncertainty_case_id=uncertainty_case_id,
            measurand=measurand,
            route_type=route_type,
            budget_level="route",
            combined_standard_uncertainty=u_c,
            expanded_uncertainty=U,
            coverage_factor=k,
            effective_degrees_of_freedom=min(
                b.effective_degrees_of_freedom for b in point_budgets
            ),
            component_contributions=merged_components,
            top_contributors=top_contributors,
            calculation_chain=[
                f"route aggregation: {len(point_budgets)} point budgets -> RSS -> u_c={u_c:.6f} -> U={U:.6f}"
            ],
        )

    def aggregate_result_budget(
        self,
        route_budgets: Sequence[BudgetResult],
        *,
        measurand: str = "system_result",
        uncertainty_case_id: str = "",
    ) -> BudgetResult:
        """Aggregate route-level budgets into a result-level budget."""
        if not route_budgets:
            return BudgetResult(
                uncertainty_case_id=uncertainty_case_id,
                measurand=measurand,
                route_type="result",
                budget_level="result",
                combined_standard_uncertainty=0.0,
                expanded_uncertainty=0.0,
                coverage_factor=self.default_coverage_factor,
                effective_degrees_of_freedom=float("inf"),
            )

        total_var = sum(b.combined_standard_uncertainty ** 2 for b in route_budgets)
        u_c = sqrt(total_var)
        k = self.default_coverage_factor
        U = u_c * k

        return BudgetResult(
            uncertainty_case_id=uncertainty_case_id,
            measurand=measurand,
            route_type="result",
            budget_level="result",
            combined_standard_uncertainty=u_c,
            expanded_uncertainty=U,
            coverage_factor=k,
            effective_degrees_of_freedom=min(
                b.effective_degrees_of_freedom for b in route_budgets
            ),
            calculation_chain=[
                f"result aggregation: {len(route_budgets)} route budgets -> RSS -> u_c={u_c:.6f} -> U={U:.6f}"
            ],
        )

    # -- Welch-Satterthwaite ------------------------------------------------

    @staticmethod
    def _welch_satterthwaite(
        *,
        u_c: float,
        input_contributions: Sequence[dict[str, Any]],
    ) -> float:
        """Compute effective degrees of freedom using Welch-Satterthwaite.

        nu_eff = u_c^4 / sum( (c_i^4 * u_i^4) / nu_i )

        Returns inf if all inputs have inf degrees of freedom.
        """
        if u_c <= 0.0:
            return float("inf")

        u_c_4 = u_c ** 4
        denominator = 0.0

        for contrib in input_contributions:
            c = contrib.get("sensitivity_coefficient", 1.0)
            u = contrib.get("standard_uncertainty", 0.0)
            nu = contrib.get("degrees_of_freedom", float("inf"))

            term_numerator = (c ** 4) * (u ** 4)
            if nu <= 0.0 or nu == float("inf"):
                # If nu is infinite, this term contributes 0 to denominator
                continue
            denominator += term_numerator / nu

        if denominator <= 0.0:
            return float("inf")

        return u_c_4 / denominator

    # -- Conformity assessment -----------------------------------------------

    def conformity_assessment(
        self,
        *,
        measured_value: float,
        reference_value: float,
        expanded_uncertainty: float,
        acceptance_limit: float,
        guard_band_policy: str = "simple",
    ) -> dict[str, Any]:
        """Perform conformity assessment per ILAC G8 / ISO 17025.

        Parameters
        ----------
        measured_value : float
            The measured/corrected value.
        reference_value : float
            The reference/nominal value.
        expanded_uncertainty : float
            The expanded uncertainty U (k=2).
        acceptance_limit : float
            The acceptance limit (tolerance).
        guard_band_policy : str
            "simple" = use U as guard band; "shared" = use U/2.

        Returns
        -------
        dict with keys: deviation, in_tolerance, conformity_statement,
        guard_band, decision_rule_applied.
        """
        deviation = abs(measured_value - reference_value)

        if guard_band_policy == "shared":
            guard_band = expanded_uncertainty / 2.0
        else:
            guard_band = expanded_uncertainty

        effective_limit = acceptance_limit - guard_band

        if deviation <= effective_limit:
            statement = "pass"
        elif deviation > acceptance_limit + guard_band:
            statement = "fail"
        else:
            statement = "inconclusive"

        return {
            "measured_value": measured_value,
            "reference_value": reference_value,
            "deviation": deviation,
            "expanded_uncertainty": expanded_uncertainty,
            "acceptance_limit": acceptance_limit,
            "guard_band": guard_band,
            "guard_band_policy": guard_band_policy,
            "effective_acceptance_limit": effective_limit,
            "in_tolerance": deviation <= acceptance_limit,
            "conformity_statement": statement,
            "decision_rule_applied": f"ILAC_G8_{guard_band_policy}_guard_band",
        }

    # -- Budget result to dict -----------------------------------------------

    @staticmethod
    def budget_result_to_dict(result: BudgetResult) -> dict[str, Any]:
        """Convert a BudgetResult to a serializable dict."""
        return {
            "uncertainty_case_id": result.uncertainty_case_id,
            "measurand": result.measurand,
            "route_type": result.route_type,
            "budget_level": result.budget_level,
            "combined_standard_uncertainty": result.combined_standard_uncertainty,
            "expanded_uncertainty": result.expanded_uncertainty,
            "coverage_factor": result.coverage_factor,
            "effective_degrees_of_freedom": result.effective_degrees_of_freedom,
            "component_contributions": {
                k: {"component_uncertainty": u, "variance_fraction": f}
                for k, (u, f) in result.component_contributions.items()
            },
            "top_contributors": result.top_contributors,
            "input_contributions": result.input_contributions,
            "correlation_contribution": result.correlation_contribution,
            "golden_case_status": result.golden_case_status,
            "calculation_chain": result.calculation_chain,
        }
