from __future__ import annotations

from pathlib import Path
from typing import Any

from ..core.recognition_scope_repository import (
    FileBackedRecognitionScopeRepository,
    RecognitionScopeRepository,
)


class RecognitionScopeGateway:
    """Read-only gateway for Step 2 scope package / decision rule reviewer payloads."""

    def __init__(
        self,
        run_dir: Path,
        *,
        repository: RecognitionScopeRepository | None = None,
        **repository_kwargs: Any,
    ) -> None:
        self.run_dir = Path(run_dir)
        self.repository = repository or FileBackedRecognitionScopeRepository(
            self.run_dir,
            **repository_kwargs,
        )

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
