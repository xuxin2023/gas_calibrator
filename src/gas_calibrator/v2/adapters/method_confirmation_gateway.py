from __future__ import annotations

from pathlib import Path
from typing import Any

from ..core.method_confirmation_repository import (
    FileBackedMethodConfirmationRepository,
    MethodConfirmationRepository,
)


class MethodConfirmationGateway:
    """Read-only gateway for Step 2 method confirmation / verification reviewer payloads."""

    def __init__(
        self,
        run_dir: Path,
        *,
        repository: MethodConfirmationRepository | None = None,
        **repository_kwargs: Any,
    ) -> None:
        self.run_dir = Path(run_dir)
        self.repository = repository or FileBackedMethodConfirmationRepository(
            self.run_dir,
            **repository_kwargs,
        )

    def read_payload(self) -> dict[str, Any]:
        snapshot = self.repository.load_snapshot()
        return {
            "method_confirmation_protocol": dict(snapshot.get("method_confirmation_protocol") or {}),
            "method_confirmation_matrix": dict(snapshot.get("method_confirmation_matrix") or {}),
            "route_specific_validation_matrix": dict(snapshot.get("route_specific_validation_matrix") or {}),
            "validation_run_set": dict(snapshot.get("validation_run_set") or {}),
            "verification_digest": dict(snapshot.get("verification_digest") or {}),
            "verification_rollup": dict(snapshot.get("verification_rollup") or {}),
        }
