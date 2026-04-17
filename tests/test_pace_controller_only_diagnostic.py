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

    def query(self, command):
        self.calls.append(("query", command))
        responses = {
            "*IDN?": "*IDN GE Druck,Pace5000 User Interface,3213201,02.00.07",
            ":INST:VERS?": ':INST:VERS "02.00.07"',
            ":OUTP:STAT?": ":OUTP:STAT 0",
            ":OUTP:ISOL:STAT?": ":OUTP:ISOL:STAT 0",
            ":OUTP:MODE?": ":OUTP:MODE ACT",
            ":SOUR:PRES:LEV:IMM:AMPL:VENT?": f":SOUR:PRES:LEV:IMM:AMPL:VENT {self.vent_status}",
            ":SOUR:PRES:EFF?": ":SOUR:PRES:EFF 0.0",
            ":SOUR:PRES:COMP1?": ":SOUR:PRES:COMP1 0.0",
            ":SOUR:PRES:COMP2?": ":SOUR:PRES:COMP2 0.0",
            ":SENS:PRES:BAR?": ":SENS:PRES:BAR 1013.2",
            ":SENS:PRES:INL?": ":SENS:PRES:INL 1000.5, 1",
            ":SENS:PRES:INL:TIME?": ":SENS:PRES:INL:TIME 12.5",
            ":SENS:PRES:SLEW?": ":SENS:PRES:SLEW 0.002",
            ":STAT:OPER:PRES:COND?": ":STAT:OPER:PRES:COND 5",
            ":STAT:OPER:PRES:EVEN?": ":STAT:OPER:PRES:EVEN 1",
            ":SYST:ERR?": ':SYST:ERR 0,"No error"',
        }
        return responses[command]


class _FakeMatrixPace:
    def __init__(self):
        self.calls = []
        self.vent_status = 3
        self.even_value = 0

    def open(self):
        self.calls.append(("open",))

    def close(self):
        self.calls.append(("close",))

    def write(self, command):
        self.calls.append(("write", command))
        if command == "*CLS":
            self.even_value = 0
        elif command == ":SOUR:PRES:LEV:IMM:AMPL:VENT 0":
            self.vent_status = 3

    def query(self, command):
        self.calls.append(("query", command))
        responses = {
            "*IDN?": "*IDN GE Druck,Pace5000 User Interface,3213201,02.00.07",
            ":INST:VERS?": ':INST:VERS "02.00.07"',
            ":OUTP:STAT?": ":OUTP:STAT 0",
            ":OUTP:ISOL:STAT?": ":OUTP:ISOL:STAT 1",
            ":OUTP:MODE?": ":OUTP:MODE ACT",
            ":SOUR:PRES:LEV:IMM:AMPL:VENT?": f":SOUR:PRES:LEV:IMM:AMPL:VENT {self.vent_status}",
            ":SOUR:PRES:EFF?": ":SOUR:PRES:EFF 0.0",
            ":SOUR:PRES:COMP1?": ":SOUR:PRES:COMP 1610.0",
            ":SOUR:PRES:COMP2?": ":SOUR:PRES:COMP2 78.6",
            ":SENS:PRES:BAR?": ":SENS:PRES:BAR 1010.0",
            ":SENS:PRES:INL?": ":SENS:PRES:INL 1008.6, 0",
            ":SENS:PRES:INL:TIME?": ":SENS:PRES:INL:TIME 31.49",
            ":SENS:PRES:SLEW?": ":SENS:PRES:SLEW -0.002",
            ":STAT:OPER:PRES:COND?": ":STAT:OPER:PRES:COND 1",
            ":STAT:OPER:PRES:EVEN?": f":STAT:OPER:PRES:EVEN {self.even_value}",
            ":SYST:ERR?": ':SYST:ERR 0,"No error"',
        }
        return responses[command]


class _FakeUiAckPace:
    def __init__(self):
        self.calls = []
        self.acknowledged = False

    def open(self):
        self.calls.append(("open",))

    def close(self):
        self.calls.append(("close",))

    def acknowledge(self):
        self.acknowledged = True

    def query(self, command):
        self.calls.append(("query", command))
        vent_status = 0 if self.acknowledged else 3
        cond_value = 0 if self.acknowledged else 1
        responses = {
            "*IDN?": "*IDN GE Druck,Pace5000 User Interface,3213201,02.00.07",
            ":INST:VERS?": ':INST:VERS "02.00.07"',
            ":OUTP:STAT?": ":OUTP:STAT 0",
            ":OUTP:ISOL:STAT?": ":OUTP:ISOL:STAT 1",
            ":OUTP:MODE?": ":OUTP:MODE ACT",
            ":SOUR:PRES:LEV:IMM:AMPL:VENT?": f":SOUR:PRES:LEV:IMM:AMPL:VENT {vent_status}",
            ":SOUR:PRES:EFF?": ":SOUR:PRES:EFF 0.0",
            ":SOUR:PRES:COMP1?": ":SOUR:PRES:COMP1 1615.1",
            ":SOUR:PRES:COMP2?": ":SOUR:PRES:COMP2 78.7",
            ":SENS:PRES:BAR?": ":SENS:PRES:BAR 1010.0",
            ":SENS:PRES:INL?": ":SENS:PRES:INL 1008.6, 0",
            ":SENS:PRES:INL:TIME?": ":SENS:PRES:INL:TIME 31.49",
            ":SENS:PRES:SLEW?": ":SENS:PRES:SLEW 0.000",
            ":STAT:OPER:PRES:COND?": f":STAT:OPER:PRES:COND {cond_value}",
            ":STAT:OPER:PRES:EVEN?": ":STAT:OPER:PRES:EVEN 0",
            ":SYST:ERR?": ':SYST:ERR 0,"No error"',
        }
        return responses[command]


class _FakeUiAckAlreadyDismissedPace:
    def __init__(self):
        self.calls = []

    def open(self):
        self.calls.append(("open",))

    def close(self):
        self.calls.append(("close",))

    def query(self, command):
        self.calls.append(("query", command))
        responses = {
            "*IDN?": "*IDN GE Druck,Pace5000 User Interface,3213201,02.00.07",
            ":INST:VERS?": ':INST:VERS "02.00.07"',
            ":OUTP:STAT?": ":OUTP:STAT 0",
            ":OUTP:ISOL:STAT?": ":OUTP:ISOL:STAT 1",
            ":OUTP:MODE?": ":OUTP:MODE ACT",
            ":SOUR:PRES:LEV:IMM:AMPL:VENT?": ":SOUR:PRES:LEV:IMM:AMPL:VENT 2",
            ":SOUR:PRES:EFF?": ":SOUR:PRES:EFF 0.0",
            ":SOUR:PRES:COMP1?": ":SOUR:PRES:COMP1 1615.1",
            ":SOUR:PRES:COMP2?": ":SOUR:PRES:COMP2 78.7",
            ":SENS:PRES:BAR?": ":SENS:PRES:BAR 1010.0",
            ":SENS:PRES:INL?": ":SENS:PRES:INL 1008.6, 0",
            ":SENS:PRES:INL:TIME?": ":SENS:PRES:INL:TIME 31.49",
            ":SENS:PRES:SLEW?": ":SENS:PRES:SLEW 0.000",
            ":STAT:OPER:PRES:COND?": ":STAT:OPER:PRES:COND 1",
            ":STAT:OPER:PRES:EVEN?": ":STAT:OPER:PRES:EVEN 0",
            ":SYST:ERR?": ':SYST:ERR 0,"No error"',
        }
        return responses[command]


class _FakeUiAckStateChangeWithoutObservedAckPace:
    def __init__(self):
        self.calls = []
        self.vent_query_count = 0

    def open(self):
        self.calls.append(("open",))

    def close(self):
        self.calls.append(("close",))

    def _post_window_active(self) -> bool:
        return self.vent_query_count >= 3

    def query(self, command):
        self.calls.append(("query", command))
        if command == ":SOUR:PRES:LEV:IMM:AMPL:VENT?":
            vent_status = 0 if self._post_window_active() else 3
            self.vent_query_count += 1
            return f":SOUR:PRES:LEV:IMM:AMPL:VENT {vent_status}"

        cond_value = 0 if self._post_window_active() else 1
        responses = {
            "*IDN?": "*IDN GE Druck,Pace5000 User Interface,3213201,02.00.07",
            ":INST:VERS?": ':INST:VERS "02.00.07"',
            ":OUTP:STAT?": ":OUTP:STAT 0",
            ":OUTP:ISOL:STAT?": ":OUTP:ISOL:STAT 1",
            ":OUTP:MODE?": ":OUTP:MODE ACT",
            ":SOUR:PRES:EFF?": ":SOUR:PRES:EFF 0.0",
            ":SOUR:PRES:COMP1?": ":SOUR:PRES:COMP1 1615.1",
            ":SOUR:PRES:COMP2?": ":SOUR:PRES:COMP2 78.7",
            ":SENS:PRES:BAR?": ":SENS:PRES:BAR 1010.0",
            ":SENS:PRES:INL?": ":SENS:PRES:INL 1008.6, 0",
            ":SENS:PRES:INL:TIME?": ":SENS:PRES:INL:TIME 31.49",
            ":SENS:PRES:SLEW?": ":SENS:PRES:SLEW 0.000",
            ":STAT:OPER:PRES:COND?": f":STAT:OPER:PRES:COND {cond_value}",
            ":STAT:OPER:PRES:EVEN?": ":STAT:OPER:PRES:EVEN 0",
            ":SYST:ERR?": ':SYST:ERR 0,"No error"',
        }
        return responses[command]


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
    assert all(call != ("query", ":SENS:PRES:CONT?") for call in fake.calls)
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
    assert fake.calls[0] == ("open",)
    assert ("clear_completed_vent_latch_if_present",) in fake.calls
    assert all(call != ("query", ":SENS:PRES:CONT?") for call in fake.calls)
    assert fake.calls[-1] == ("close",)


def test_controller_only_matrix_runs_a_to_g_without_touching_main_flow(tmp_path: Path) -> None:
    module = _load_module()
    fake = _FakeMatrixPace()

    summary = module.run_controller_only_matrix(
        port="COM_TEST",
        timeout=0.1,
        output_dir=tmp_path,
        action_settle_s=0.0,
        query_interval_s=0.0,
        pace_factory=lambda *args, **kwargs: fake,
    )

    assert [step["step"] for step in summary["steps"]] == [
        "A_read_only",
        "B_cls_only",
        "C_even_read_only",
        "D_vent0_only",
        "E_even_then_vent0",
        "F_cls_then_vent0",
        "G_cls_even_vent0",
    ]
    assert summary["steps"][0]["snapshot"]["parsed"]["vent"] == 3
    assert summary["steps"][0]["snapshot"]["parsed"]["eff"] == 0.0
    assert summary["steps"][0]["snapshot"]["parsed"]["comp2"] == 78.6
    assert summary["steps"][0]["snapshot"]["parsed"]["cond_bits"]["vent_complete_bit0"] is True
    assert summary["steps"][0]["snapshot"]["parsed"]["even_bits"]["vent_complete_bit0"] is False
    assert summary["steps"][0]["pace_legacy_vent_state_3_suspect"] is True
    assert summary["steps"][0]["pace_atmosphere_connected_latched_state_suspect"] is True
    assert summary["steps"][0]["clear_result"] == "no_clear_attempt"
    assert summary["steps"][3]["clear_attempt_sequence"] == ":SOUR:PRES:LEV:IMM:AMPL:VENT 0"
    assert summary["steps"][3]["clear_result"] == "persistent_3"
    assert summary["steps"][3]["vent_complete_bit_before"] is True
    assert summary["steps"][3]["vent_complete_bit_after"] is True
    assert summary["steps"][0]["legacy_vent3_control_ready_used"] is False
    assert summary["steps"][0]["legacy_vent3_accept_scope"] == "none"
    assert summary["analysis"]["vent_status_3_count"] == 7
    assert summary["analysis"]["pace_atmosphere_connected_latched_state_suspect"] is True
    assert summary["analysis"]["legacy_vent3_control_ready_used"] is False
    assert summary["analysis"]["legacy_vent3_accept_scope"] == "none"
    assert summary["analysis"]["vent3_watchlist_only"] is True
    assert summary["analysis"]["syst_err_all_zero"] is True
    assert summary["analysis"]["eff_all_zero"] is True
    assert summary["analysis"]["observation_only_steps"] == [
        "B_cls_only",
        "C_even_read_only",
    ]
    assert summary["analysis"]["conclusion_codes"] == [
        "legacy_vent_state_problem",
        "firmware_state_persistent",
        "clear_sequence_observation_only",
    ]
    assert all(call != ("query", ":SENS:PRES:CONT?") for call in fake.calls)
    assert ("write", "*CLS") in fake.calls
    assert ("write", ":SOUR:PRES:LEV:IMM:AMPL:VENT 0") in fake.calls
    assert ("query", ":STAT:OPER:PRES:EVEN?") in fake.calls
    assert Path(summary["csv_path"]).exists()
    assert Path(summary["json_path"]).exists()


def test_controller_only_ui_ack_experiment_marks_callback_invocation_in_post_window(tmp_path: Path) -> None:
    module = _load_module()
    fake = _FakeUiAckPace()

    summary = module.run_controller_only_ui_ack_experiment(
        port="COM_TEST",
        timeout=0.1,
        output_dir=tmp_path,
        interval_s=0.0,
        hold_samples=3,
        post_ack_samples=3,
        ack_wait_s=0.0,
        ack_callback=fake.acknowledge,
        pace_factory=lambda *args, **kwargs: fake,
    )

    assert [sample["phase"] for sample in summary["samples"][:4]] == [
        "phase1_window_initial",
        "phase2_window_hold",
        "phase2_window_hold",
        "phase2_window_hold",
    ]
    assert all(
        sample["snapshot"]["parsed"]["vent"] == 3
        for sample in summary["samples"]
        if sample["phase"] != "phase3_post_window"
    )
    assert all(
        sample["snapshot"]["parsed"]["vent"] == 0
        for sample in summary["samples"]
        if sample["phase"] == "phase3_post_window"
    )
    assert all(
        sample["snapshot"]["parsed"]["ack_callback_invoked"] is True
        for sample in summary["samples"]
        if sample["phase"] == "phase3_post_window"
    )
    assert summary["analysis"]["vent3_persisted_before_window"] is True
    assert summary["analysis"]["vent3_cleared_after_window"] is True
    assert summary["analysis"]["cond_bit0_cleared_after_window"] is True
    assert summary["analysis"]["legacy_vent3_control_ready_used"] is False
    assert summary["analysis"]["legacy_vent3_accept_scope"] == "none"
    assert summary["analysis"]["ack_callback_invoked"] is True
    assert summary["analysis"]["vent3_post_window_status"] == 0
    assert "vent_status_changed_after_window" in summary["analysis"]["conclusion_codes"]
    assert all(call != ("query", ":SENS:PRES:CONT?") for call in fake.calls)
    assert Path(summary["csv_path"]).exists()
    assert Path(summary["json_path"]).exists()


def test_controller_only_ui_ack_experiment_keeps_callback_marker_false_without_callback(tmp_path: Path) -> None:
    module = _load_module()
    fake = _FakeUiAckPace()

    summary = module.run_controller_only_ui_ack_experiment(
        port="COM_TEST",
        timeout=0.1,
        output_dir=tmp_path,
        interval_s=0.0,
        hold_samples=2,
        post_ack_samples=2,
        ack_wait_s=0.0,
        pace_factory=lambda *args, **kwargs: fake,
    )

    assert all(
        sample["snapshot"]["parsed"]["vent"] == 3
        for sample in summary["samples"]
        if sample["phase"] == "phase3_post_window"
    )
    assert all(
        sample["snapshot"]["parsed"]["ack_callback_invoked"] is False
        for sample in summary["samples"]
        if sample["phase"] == "phase3_post_window"
    )
    assert summary["analysis"]["vent3_persisted_before_window"] is True
    assert summary["analysis"]["vent3_cleared_after_window"] is False
    assert summary["analysis"]["cond_bit0_cleared_after_window"] is False
    assert summary["analysis"]["ack_callback_invoked"] is False
    assert summary["analysis"]["vent3_post_window_status"] == 3
    assert summary["analysis"]["legacy_vent3_control_ready_used"] is False
    assert summary["analysis"]["legacy_vent3_accept_scope"] == "none"
    assert "vent_status_changed_after_window" not in summary["analysis"]["conclusion_codes"]
    assert all(call != ("query", ":SENS:PRES:CONT?") for call in fake.calls)


def test_controller_only_ui_ack_experiment_keeps_callback_unset_when_status_changes_without_callback(
    tmp_path: Path,
) -> None:
    module = _load_module()
    fake = _FakeUiAckStateChangeWithoutObservedAckPace()

    summary = module.run_controller_only_ui_ack_experiment(
        port="COM_TEST",
        timeout=0.1,
        output_dir=tmp_path,
        interval_s=0.0,
        hold_samples=2,
        post_ack_samples=2,
        ack_wait_s=0.0,
        pace_factory=lambda *args, **kwargs: fake,
    )

    assert summary["analysis"]["vent3_persisted_before_window"] is True
    assert summary["analysis"]["vent3_cleared_after_window"] is True
    assert summary["analysis"]["cond_bit0_cleared_after_window"] is True
    assert summary["analysis"]["ack_callback_invoked"] is False
    assert "vent_status_changed_after_window" in summary["analysis"]["conclusion_codes"]
    assert "cond_bit0_changed_after_window" in summary["analysis"]["conclusion_codes"]
    assert all(call != ("query", ":SENS:PRES:CONT?") for call in fake.calls)


def test_controller_only_ui_ack_experiment_does_not_infer_callback_when_popup_state_never_appears(tmp_path: Path) -> None:
    module = _load_module()
    fake = _FakeUiAckAlreadyDismissedPace()

    summary = module.run_controller_only_ui_ack_experiment(
        port="COM_TEST",
        timeout=0.1,
        output_dir=tmp_path,
        interval_s=0.0,
        hold_samples=2,
        post_ack_samples=2,
        ack_wait_s=0.0,
        pace_factory=lambda *args, **kwargs: fake,
    )

    assert all(
        sample["snapshot"]["parsed"]["vent"] == 2
        for sample in summary["samples"]
    )
    assert summary["analysis"]["vent_status_3_count"] == 0
    assert summary["analysis"]["vent3_persisted_before_window"] is False
    assert summary["analysis"]["vent3_cleared_after_window"] is False
    assert summary["analysis"]["cond_bit0_cleared_after_window"] is False
    assert summary["analysis"]["ack_callback_invoked"] is False
    assert summary["analysis"]["vent3_post_window_status"] == 2
    assert summary["analysis"]["legacy_vent3_control_ready_used"] is False
    assert summary["analysis"]["legacy_vent3_accept_scope"] == "none"
    assert summary["analysis"]["conclusion_codes"] == []
    assert all(call != ("query", ":SENS:PRES:CONT?") for call in fake.calls)
