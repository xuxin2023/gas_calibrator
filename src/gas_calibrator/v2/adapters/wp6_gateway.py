"""WP6 gateway: read-only gateway for Step 2 PT/ILC / comparison reviewer payloads."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from ..core.wp6_repository import (
    FileBackedWp6Repository,
    Wp6Repository,
)


class Wp6Gateway:
    """Read-only gateway for Step 2 PT/ILC / comparison evidence / reviewer navigation payloads."""

    def __init__(
        self,
        run_dir: Path,
        *,
        repository: Wp6Repository | None = None,
        **repository_kwargs: Any,
    ) -> None:
        self.run_dir = Path(run_dir)
        self.repository = repository or FileBackedWp6Repository(
            self.run_dir,
            **repository_kwargs,
        )

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
