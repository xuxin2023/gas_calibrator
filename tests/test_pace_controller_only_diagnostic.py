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
        self.vent_status = 2

    def open(self):
        self.calls.append(("open",))

    def close(self):
        self.calls.append(("close",))

    def clear_completed_vent_latch_if_present(self):
        self.calls.append(("clear_completed_vent_latch_if_present",))
        before = self.vent_status
        if self.vent_status == 2:
            self.vent_status = 0
            return {
                "before_status": before,
                "clear_attempted": True,
                "after_status": self.vent_status,
                "cleared": True,
                "command": ":SOUR:PRES:LEV:IMM:AMPL:VENT 0",
            }
        return {
            "before_status": before,
            "clear_attempted": False,
            "after_status": before,
            "cleared": before == 0,
            "command": "",
        }

    def diagnostic_status(self):
        self.snapshots += 1
        return {
            "pressure_hpa": 1000.5,
            "output_state": 0,
            "isolation_state": 0,
            "output_mode": "ACT",
            "vent_status": self.vent_status,
            "vent_completed_latched": self.vent_status == 2,
            "effort": 0.0,
            "comp1": 0.0,
            "comp2": 0.0,
            "control_pressure_hpa": 1000.4,
            "barometric_pressure_hpa": 1013.2,
            "in_limits_pressure_hpa": 1000.5,
            "in_limits_state": 1,
            "in_limits_time_s": 12.5,
            "measured_slew_hpa_s": 0.002,
            "oper_condition": 3,
            "oper_pressure_condition": 5,
            "oper_pressure_event": 1,
            "oper_pressure_vent_complete_bit": True,
            "oper_pressure_in_limits_bit": True,
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
    assert ("clear_completed_vent_latch_if_present",) not in fake.calls
    assert summary["rows"][0]["measured_slew_hpa_s"] == 0.002
    assert Path(summary["csv_path"]).exists()
    assert Path(summary["json_path"]).exists()


def test_controller_only_diagnostic_allow_write_sanitize_only_clears_completed_vent_latch(tmp_path: Path) -> None:
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
    assert summary["sanitize_summary"]["command"] == ":SOUR:PRES:LEV:IMM:AMPL:VENT 0"
    assert summary["sanitize_summary"]["before_status"] == 2
    assert summary["sanitize_summary"]["after_status"] == 0
    assert summary["sanitize_summary"]["cleared"] is True
    assert fake.calls == [("open",), ("clear_completed_vent_latch_if_present",), ("close",)]
