from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.ui_v2.controllers.app_facade import AppFacade
from gas_calibrator.v2.ui_v2.utils.runtime_paths import RuntimePaths


class _FakeEventBus:
    def subscribe(self, *_args, **_kwargs) -> None:
        return None

    def unsubscribe(self, *_args, **_kwargs) -> None:
        return None


class _FakeService:
    def __init__(self, run_dir: Path) -> None:
        self.config = AppConfig.from_dict({"paths": {"output_dir": str(run_dir.parent)}})
        self._raw_cfg = getattr(self.config, "_raw_cfg", {})
        self.session = SimpleNamespace(run_id=run_dir.name, warnings=[], errors=[])
        self.event_bus = _FakeEventBus()
        self.result_store = SimpleNamespace(run_dir=run_dir)
        self._log_callback = None

    def get_output_files(self) -> list[str]:
        return []

    def set_log_callback(self, callback) -> None:
        self._log_callback = callback

    def get_status(self):
        return SimpleNamespace(phase=SimpleNamespace(value="completed"))


def test_app_facade_review_center_includes_human_governance_items(sample_run_dir, tmp_path) -> None:
    facade = AppFacade(
        service=_FakeService(sample_run_dir),
        simulation=True,
        runtime_paths=RuntimePaths.from_base_dir(tmp_path / "runtime"),
    )
    try:
        snapshot = facade.build_results_snapshot()
    finally:
        facade.shutdown()

    items = list(snapshot["review_center"]["evidence_items"] or [])
    governance_items = [
        item
        for item in items
        if item.get("type") == "readiness_governance" and "OP-SIM-LI" in str(item.get("summary") or "")
    ]

    assert governance_items
    assert any("SOP-STEP2-CAL-SIM" in str(item.get("detail_text") or "") for item in governance_items)
    assert any(
        "双人复核" in str(item.get("detail_text") or "") or "placeholder" in str(item.get("detail_text") or "")
        for item in governance_items
    )
