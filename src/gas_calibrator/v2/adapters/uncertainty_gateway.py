from __future__ import annotations

from pathlib import Path
from typing import Any

from ..core.uncertainty_repository import (
    FileBackedUncertaintyRepository,
    UncertaintyRepository,
)


class UncertaintyGateway:
    """Read-only gateway for Step 2 uncertainty skeleton / golden-case payloads."""

    def __init__(
        self,
        run_dir: Path,
        *,
        repository: UncertaintyRepository | None = None,
        **repository_kwargs: Any,
    ) -> None:
        self.run_dir = Path(run_dir)
        self.repository = repository or FileBackedUncertaintyRepository(
            self.run_dir,
            **repository_kwargs,
        )

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
