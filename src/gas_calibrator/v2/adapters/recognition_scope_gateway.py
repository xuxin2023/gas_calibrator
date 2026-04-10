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
        return {
            "scope_definition_pack": dict(snapshot.get("scope_definition_pack") or {}),
            "decision_rule_profile": dict(snapshot.get("decision_rule_profile") or {}),
            "recognition_scope_rollup": dict(snapshot.get("recognition_scope_rollup") or {}),
        }
