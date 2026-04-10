from __future__ import annotations

from pathlib import Path
from typing import Any

from ..core.software_validation_repository import (
    FileBackedSoftwareValidationRepository,
    SoftwareValidationRepository,
)


class SoftwareValidationGateway:
    """Read-only gateway for Step 2 software validation / audit hash / release reviewer payloads."""

    def __init__(
        self,
        run_dir: Path,
        *,
        repository: SoftwareValidationRepository | None = None,
        **repository_kwargs: Any,
    ) -> None:
        self.run_dir = Path(run_dir)
        self.repository = repository or FileBackedSoftwareValidationRepository(
            self.run_dir,
            **repository_kwargs,
        )

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
