"""
Adapter package entrypoints.

Exports are resolved lazily so importing ``gas_calibrator.v2.adapters`` does
not eagerly pull offline analytics, storage, or postprocess chains into
headless/runtime paths that do not use them.
"""

from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Any

_LAZY_EXPORTS = {
    "LegacyCalibrationRunner": ("gas_calibrator.v2.adapters.legacy_runner", "LegacyCalibrationRunner"),
    "download_coefficients_to_analyzers": (
        "gas_calibrator.v2.adapters.analyzer_coefficient_downloader",
        "download_coefficients_to_analyzers",
    ),
}

_METHOD_CONFIRMATION_PAYLOAD_KEYS = (
    "method_confirmation_protocol",
    "method_confirmation_matrix",
    "route_specific_validation_matrix",
    "validation_run_set",
    "verification_digest",
    "verification_rollup",
)


class MethodConfirmationGateway:
    """Read-only gateway for method confirmation reviewer payloads."""

    def __init__(self, run_dir: Path, *, repository: Any = None, **repository_kwargs: Any) -> None:
        from ..core.method_confirmation_repository import FileBackedMethodConfirmationRepository

        self.run_dir = Path(run_dir)
        self.repository = repository or FileBackedMethodConfirmationRepository(self.run_dir, **repository_kwargs)

    def read_payload(self) -> dict[str, Any]:
        snapshot = self.repository.load_snapshot()
        return {key: dict(snapshot.get(key) or {}) for key in _METHOD_CONFIRMATION_PAYLOAD_KEYS}


class RecognitionScopeGateway:
    """Read-only gateway for recognition scope reviewer payloads."""

    def __init__(self, run_dir: Path, *, repository: Any = None, **repository_kwargs: Any) -> None:
        from ..core.recognition_scope_repository import FileBackedRecognitionScopeRepository

        self.run_dir = Path(run_dir)
        self.repository = repository or FileBackedRecognitionScopeRepository(self.run_dir, **repository_kwargs)

    def read_payload(self) -> dict[str, Any]:
        snapshot = self.repository.load_snapshot()
        rollup = dict(snapshot.get("recognition_scope_rollup") or {})
        pre_run_readiness_gate = dict(snapshot.get("pre_run_readiness_gate") or {})
        return {
            "scope_definition_pack": dict(snapshot.get("scope_definition_pack") or {}),
            "decision_rule_profile": dict(snapshot.get("decision_rule_profile") or {}),
            "conformity_statement_profile": dict(
                snapshot.get("conformity_statement_profile")
                or dict(snapshot.get("decision_rule_profile") or {}).get("conformity_statement_profile")
                or {}
            ),
            "reference_asset_registry": dict(snapshot.get("reference_asset_registry") or {}),
            "certificate_lifecycle_summary": dict(snapshot.get("certificate_lifecycle_summary") or {}),
            "pre_run_readiness_gate": pre_run_readiness_gate,
            "readiness_gate": {
                "status": str(pre_run_readiness_gate.get("gate_status") or "--"),
                "legacy_status": str(pre_run_readiness_gate.get("legacy_gate_status") or "--"),
                "advisory_only": bool(pre_run_readiness_gate.get("advisory_only", True)),
                "device_control_allowed": bool(pre_run_readiness_gate.get("device_control_allowed", False)),
                "real_control_permitted": bool(pre_run_readiness_gate.get("real_control_permitted", False)),
                "blocking_items": list(pre_run_readiness_gate.get("blocking_items") or []),
                "warning_items": list(pre_run_readiness_gate.get("warning_items") or []),
            },
            "recognition_scope_rollup": rollup,
            "recognition_binding": dict(
                rollup.get("recognition_binding")
                or dict(snapshot.get("scope_definition_pack") or {}).get("recognition_binding")
                or dict(snapshot.get("decision_rule_profile") or {}).get("recognition_binding")
                or {}
            ),
        }


class SoftwareValidationGateway:
    """Read-only gateway for software validation reviewer payloads."""

    def __init__(self, run_dir: Path, *, repository: Any = None, **repository_kwargs: Any) -> None:
        from ..core.software_validation_repository import FileBackedSoftwareValidationRepository

        self.run_dir = Path(run_dir)
        self.repository = repository or FileBackedSoftwareValidationRepository(self.run_dir, **repository_kwargs)

    def read_payload(self) -> dict[str, Any]:
        snapshot = self.repository.load_snapshot()
        return {
            "software_validation_traceability_matrix": dict(
                snapshot.get("software_validation_traceability_matrix") or {}
            ),
            "requirement_design_code_test_links": dict(
                snapshot.get("requirement_design_code_test_links") or {}
            ),
            "validation_evidence_index": dict(snapshot.get("validation_evidence_index") or {}),
            "change_impact_summary": dict(snapshot.get("change_impact_summary") or {}),
            "rollback_readiness_summary": dict(snapshot.get("rollback_readiness_summary") or {}),
            "artifact_hash_registry": dict(snapshot.get("artifact_hash_registry") or {}),
            "audit_event_store": dict(snapshot.get("audit_event_store") or {}),
            "environment_fingerprint": dict(snapshot.get("environment_fingerprint") or {}),
            "config_fingerprint": dict(snapshot.get("config_fingerprint") or {}),
            "release_input_digest": dict(snapshot.get("release_input_digest") or {}),
            "release_manifest": dict(snapshot.get("release_manifest") or {}),
            "release_scope_summary": dict(snapshot.get("release_scope_summary") or {}),
            "release_boundary_digest": dict(snapshot.get("release_boundary_digest") or {}),
            "release_evidence_pack_index": dict(snapshot.get("release_evidence_pack_index") or {}),
            "release_validation_manifest": dict(snapshot.get("release_validation_manifest") or {}),
            "audit_readiness_digest": dict(snapshot.get("audit_readiness_digest") or {}),
            "software_validation_rollup": dict(snapshot.get("software_validation_rollup") or {}),
        }


class UncertaintyGateway:
    """Read-only gateway for uncertainty reviewer payloads."""

    def __init__(self, run_dir: Path, *, repository: Any = None, **repository_kwargs: Any) -> None:
        from ..core.uncertainty_repository import FileBackedUncertaintyRepository

        self.run_dir = Path(run_dir)
        self.repository = repository or FileBackedUncertaintyRepository(self.run_dir, **repository_kwargs)

    def read_payload(self) -> dict[str, Any]:
        snapshot = self.repository.load_snapshot()
        return {
            "uncertainty_model": dict(snapshot.get("uncertainty_model") or {}),
            "uncertainty_input_set": dict(snapshot.get("uncertainty_input_set") or {}),
            "sensitivity_coefficient_set": dict(snapshot.get("sensitivity_coefficient_set") or {}),
            "budget_case": dict(snapshot.get("budget_case") or {}),
            "uncertainty_golden_cases": dict(snapshot.get("uncertainty_golden_cases") or {}),
            "uncertainty_report_pack": dict(snapshot.get("uncertainty_report_pack") or {}),
            "uncertainty_digest": dict(snapshot.get("uncertainty_digest") or {}),
            "uncertainty_rollup": dict(snapshot.get("uncertainty_rollup") or {}),
        }


class Wp6Gateway:
    """Read-only gateway for PT/ILC reviewer payloads."""

    def __init__(self, run_dir: Path, *, repository: Any = None, **repository_kwargs: Any) -> None:
        from ..core.wp6_repository import FileBackedWp6Repository

        self.run_dir = Path(run_dir)
        self.repository = repository or FileBackedWp6Repository(self.run_dir, **repository_kwargs)

    def read_payload(self) -> dict[str, Any]:
        snapshot = self.repository.load_snapshot()
        return {
            "pt_ilc_registry": dict(snapshot.get("pt_ilc_registry") or {}),
            "external_comparison_importer": dict(snapshot.get("external_comparison_importer") or {}),
            "comparison_evidence_pack": dict(snapshot.get("comparison_evidence_pack") or {}),
            "scope_comparison_view": dict(snapshot.get("scope_comparison_view") or {}),
            "comparison_digest": dict(snapshot.get("comparison_digest") or {}),
            "comparison_rollup": dict(snapshot.get("comparison_rollup") or {}),
        }


__all__ = list(_LAZY_EXPORTS) + [
    "MethodConfirmationGateway",
    "RecognitionScopeGateway",
    "SoftwareValidationGateway",
    "UncertaintyGateway",
    "Wp6Gateway",
]


def __getattr__(name: str) -> Any:
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = target
    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
