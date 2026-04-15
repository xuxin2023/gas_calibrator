import importlib.util
from pathlib import Path


def _load_module():
    path = Path("scripts/pace_controller_only_diagnostic.py").resolve()
    spec = importlib.util.spec_from_file_location("pace_controller_only_diagnostic", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakePace:
    def __init__(self):
        self.calls = []
        self.snapshots = 0

    def open(self):
        self.calls.append(("open",))

    def close(self):
        self.calls.append(("close",))

    def set_vent_after_valve_open(self, enabled):
        self.calls.append(("set_vent_after_valve_open", bool(enabled)))

    def diagnostic_status(self):
        self.snapshots += 1
        return {
            "pressure_hpa": 1000.5,
            "output_state": 0,
            "isolation_state": 0,
            "output_mode": "ACT",
            "vent_status": 0,
            "vent_elapsed_time_s": 4.0,
            "vent_after_valve_state": "CLOSED",
            "vent_popup_state": "ENABLED",
            "vent_orpv_state": "DISABLED",
            "vent_pupv_state": "DISABLED",
            "oper_condition": 3,
            "oper_pressure_condition": 5,
        }


def test_controller_only_diagnostic_is_read_only_by_default(tmp_path: Path) -> None:
    module = _load_module()
    fake = _FakePace()

    summary = module.run_controller_only_diagnostic(
        port="COM_TEST",
        samples=2,
        interval_s=0.0,
        output_dir=tmp_path,
        pace_factory=lambda *args, **kwargs: fake,
    )

    assert summary["allow_write_sanitize"] is False
    assert summary["sanitize_summary"]["performed"] is False
    assert ("set_vent_after_valve_open", False) not in fake.calls
    assert Path(summary["csv_path"]).exists()
    assert Path(summary["json_path"]).exists()


def test_controller_only_diagnostic_allow_write_sanitize_only_writes_aft_valve_closed(tmp_path: Path) -> None:
    module = _load_module()
    fake = _FakePace()

    summary = module.run_controller_only_diagnostic(
        port="COM_TEST",
        samples=1,
        interval_s=0.0,
        output_dir=tmp_path,
        allow_write_sanitize=True,
        pace_factory=lambda *args, **kwargs: fake,
    )

    assert summary["allow_write_sanitize"] is True
    assert summary["sanitize_summary"]["performed"] is True
    assert summary["sanitize_summary"]["command"] == "VENT:AFT:VVAL:STAT CLOSed"
    assert fake.calls == [("open",), ("set_vent_after_valve_open", False), ("close",)]
