from __future__ import annotations

from pathlib import Path
from typing import Any

from ..core.method_confirmation_repository import (
    FileBackedMethodConfirmationRepository,
    MethodConfirmationRepository,
)

_PAYLOAD_KEYS = (
    "method_confirmation_protocol",
    "method_confirmation_matrix",
    "route_specific_validation_matrix",
    "validation_run_set",
    "verification_digest",
    "verification_rollup",
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
        return {key: dict(snapshot.get(key) or {}) for key in _PAYLOAD_KEYS}
