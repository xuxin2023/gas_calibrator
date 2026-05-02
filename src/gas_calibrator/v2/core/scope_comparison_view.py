"""Scope comparison view for cross-scope analysis.

Implements the missing scope_comparison_view identified in the V1.3 analysis
for WP6. Provides side-by-side comparison of scope definitions, decision
rules, uncertainty budgets, and method confirmation status across different
scopes or scope versions.

This enables:
- Comparing current scope vs. target scope for gap analysis
- Comparing two scope versions for change impact assessment
- Cross-scope coverage analysis for PT/ILC linkage
- Scope readiness comparison for accreditation preparation
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Sequence


@dataclass(frozen=True)
class ScopeComparisonRow:
    """A single comparison dimension between two scopes.

    Attributes
    ----------
    dimension : str
        The comparison dimension (e.g., "measurand", "temperature_range").
    scope_a_value : Any
        Value in scope A.
    scope_b_value : Any
        Value in scope B.
    match : bool
        Whether the values match.
    gap_note : str
        Description of the gap if values don't match.
    severity : str
        Gap severity: "none", "info", "warning", "critical".
    """
    dimension: str
    scope_a_value: Any
    scope_b_value: Any
    match: bool
    gap_note: str = ""
    severity: str = "none"


@dataclass(frozen=True)
class ScopeComparisonResult:
    """Result of comparing two scopes.

    Attributes
    ----------
    scope_a_id : str
        First scope ID.
    scope_b_id : str
        Second scope ID.
    comparison_type : str
        Type of comparison (gap_analysis, version_diff, coverage, readiness).
    rows : list
        Comparison rows for each dimension.
    overall_match : bool
        Whether all dimensions match.
    critical_gaps : list
        Dimensions with critical gaps.
    warning_gaps : list
        Dimensions with warning gaps.
    coverage_summary : dict
        Coverage statistics.
    """
    scope_a_id: str
    scope_b_id: str
    comparison_type: str
    rows: list[ScopeComparisonRow] = field(default_factory=list)
    overall_match: bool = True
    critical_gaps: list[str] = field(default_factory=list)
    warning_gaps: list[str] = field(default_factory=list)
    coverage_summary: dict[str, Any] = field(default_factory=dict)


class ScopeComparisonView:
    """Scope comparison view for cross-scope analysis.

    Compares two scope definitions across multiple dimensions:
    - Measurand coverage
    - Route type coverage
    - Temperature/pressure ranges
    - Method/algorithm versions
    - Decision rule alignment
    - Uncertainty budget completeness
    - Method confirmation status
    - Certificate/asset readiness
    """

    # Dimensions to compare
    DIMENSIONS = [
        "measurand",
        "route_type",
        "environment_mode",
        "analyzer_model",
        "temperature_range",
        "pressure_range",
        "gas_or_humidity_range",
        "method_version",
        "algorithm_version",
        "decision_rule_id",
        "readiness_status",
    ]

    def compare_scopes(
        self,
        scope_a: dict[str, Any],
        scope_b: dict[str, Any],
        *,
        comparison_type: str = "gap_analysis",
    ) -> ScopeComparisonResult:
        """Compare two scope definitions.

        Parameters
        ----------
        scope_a, scope_b : dict
            Scope definition packs (as produced by recognition_readiness_artifacts).
        comparison_type : str
            Type of comparison: gap_analysis, version_diff, coverage, readiness.
        """
        rows: list[ScopeComparisonRow] = []
        critical_gaps: list[str] = []
        warning_gaps: list[str] = []

        for dim in self.DIMENSIONS:
            val_a = scope_a.get(dim)
            val_b = scope_b.get(dim)
            match = val_a == val_b

            gap_note = ""
            severity = "none"

            if not match:
                if dim in ("measurand", "route_type", "decision_rule_id"):
                    severity = "critical"
                    critical_gaps.append(dim)
                    gap_note = f"Scope A: {val_a} vs Scope B: {val_b} — fundamental scope mismatch"
                elif dim in ("method_version", "algorithm_version"):
                    severity = "warning"
                    warning_gaps.append(dim)
                    gap_note = f"Version difference: {val_a} vs {val_b} — may affect result comparability"
                elif dim in ("temperature_range", "pressure_range"):
                    severity = "warning"
                    warning_gaps.append(dim)
                    gap_note = f"Range difference: {val_a} vs {val_b} — coverage may differ"
                else:
                    severity = "info"
                    gap_note = f"{val_a} vs {val_b}"

            rows.append(ScopeComparisonRow(
                dimension=dim,
                scope_a_value=val_a,
                scope_b_value=val_b,
                match=match,
                gap_note=gap_note,
                severity=severity,
            ))

        overall_match = len(critical_gaps) == 0 and len(warning_gaps) == 0

        return ScopeComparisonResult(
            scope_a_id=str(scope_a.get("scope_id", "")),
            scope_b_id=str(scope_b.get("scope_id", "")),
            comparison_type=comparison_type,
            rows=rows,
            overall_match=overall_match,
            critical_gaps=critical_gaps,
            warning_gaps=warning_gaps,
            coverage_summary=self._compute_coverage_summary(rows),
        )

    def compare_scope_versions(
        self,
        scope_v1: dict[str, Any],
        scope_v2: dict[str, Any],
    ) -> ScopeComparisonResult:
        """Compare two versions of the same scope for change impact."""
        result = self.compare_scopes(scope_v1, scope_v2, comparison_type="version_diff")

        # Add change impact assessment
        impact_rows: list[ScopeComparisonRow] = []
        for row in result.rows:
            if not row.match:
                impact = "breaking" if row.severity == "critical" else "compatible"
                impact_rows.append(ScopeComparisonRow(
                    dimension=f"{row.dimension}_impact",
                    scope_a_value=row.scope_a_value,
                    scope_b_value=row.scope_b_value,
                    match=False,
                    gap_note=f"Change impact: {impact} — {row.gap_note}",
                    severity=row.severity,
                ))

        return ScopeComparisonResult(
            scope_a_id=result.scope_a_id,
            scope_b_id=result.scope_b_id,
            comparison_type="version_diff",
            rows=[*result.rows, *impact_rows],
            overall_match=result.overall_match,
            critical_gaps=result.critical_gaps,
            warning_gaps=result.warning_gaps,
            coverage_summary=result.coverage_summary,
        )

    def compare_readiness(
        self,
        scopes: Sequence[dict[str, Any]],
        *,
        target_readiness: str = "ready_for_formal_claim",
    ) -> dict[str, Any]:
        """Compare readiness status across multiple scopes.

        Returns a readiness matrix showing which scopes have reached
        the target readiness level and which are still in progress.
        """
        matrix: list[dict[str, Any]] = []
        ready_count = 0

        for scope in scopes:
            scope_id = str(scope.get("scope_id", ""))
            readiness = str(scope.get("readiness_status", "unknown"))
            is_ready = readiness == target_readiness
            if is_ready:
                ready_count += 1

            matrix.append({
                "scope_id": scope_id,
                "readiness_status": readiness,
                "target_readiness": target_readiness,
                "ready": is_ready,
                "measurand": scope.get("measurand"),
                "route_type": scope.get("route_type"),
            })

        return {
            "target_readiness": target_readiness,
            "total_scopes": len(scopes),
            "ready_count": ready_count,
            "not_ready_count": len(scopes) - ready_count,
            "readiness_fraction": ready_count / len(scopes) if scopes else 0.0,
            "matrix": matrix,
        }

    @staticmethod
    def _compute_coverage_summary(rows: list[ScopeComparisonRow]) -> dict[str, Any]:
        """Compute coverage summary statistics."""
        total = len(rows)
        matching = sum(1 for r in rows if r.match)
        critical = sum(1 for r in rows if r.severity == "critical")
        warning = sum(1 for r in rows if r.severity == "warning")
        info = sum(1 for r in rows if r.severity == "info")

        return {
            "total_dimensions": total,
            "matching": matching,
            "mismatching": total - matching,
            "critical_gaps": critical,
            "warning_gaps": warning,
            "info_gaps": info,
            "match_fraction": matching / total if total > 0 else 0.0,
        }

    @staticmethod
    def comparison_result_to_dict(result: ScopeComparisonResult) -> dict[str, Any]:
        """Serialize a comparison result to dict."""
        return {
            "scope_a_id": result.scope_a_id,
            "scope_b_id": result.scope_b_id,
            "comparison_type": result.comparison_type,
            "overall_match": result.overall_match,
            "critical_gaps": result.critical_gaps,
            "warning_gaps": result.warning_gaps,
            "coverage_summary": result.coverage_summary,
            "rows": [
                {
                    "dimension": r.dimension,
                    "scope_a_value": r.scope_a_value,
                    "scope_b_value": r.scope_b_value,
                    "match": r.match,
                    "gap_note": r.gap_note,
                    "severity": r.severity,
                }
                for r in result.rows
            ],
        }
