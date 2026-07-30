from __future__ import annotations

import json

from gas_calibrator.tools import run_v1_5_analyzer_runtime_setup as runtime_setup


def _patch_runtime_sleep(monkeypatch):
    sleeps = []

    def fake_sleep(seconds):
        sleeps.append(float(seconds))

    monkeypatch.setattr(runtime_setup.time, "sleep", fake_sleep)
    return sleeps, fake_sleep


def _config(**overrides):
    config = {
        "schema_version": "v1_5_analyzer_runtime_setup_config_v0_test",
        "safety": {
            "requires_explicit_operator_authorization_before_real_com": True,
            "writes_senco": False,
            "writes_device_id": False,
            "writes_sn": False,
            "controls_gas_route": False,
            "controls_water_route": False,
            "controls_pressure": False,
            "controls_temperature": False,
            "runs_sampling": False,
            "runs_fitting": False,
            "not_real_acceptance_evidence": True,
        },
        "runtime_setup_contract": {
            "command_gap_s": 1.2,
            "pre_drain_s": 0,
            "mode": 2,
            "active_send": True,
            "ftd_hz": 1,
            "average1_target": 49,
            "average2_target": 49,
            "do_not_append_set_average_1_1_after_filter": True,
            "post_enable_stream_wait_s": 0,
            "post_enable_stream_ack_wait_s": 0,
            "runtime_setup_retry_count": 1,
            "runtime_setup_retry_delay_s": 0,
            "verify_active_upload_rate": True,
            "active_upload_rate_measure_s": 2,
            "active_upload_rate_tolerance_abs_hz": 0.3,
            "active_upload_rate_tolerance_fraction": 0.3,
            "ready_consecutive_mode2_frames": 2,
            "frame_attempts": 3,
            "frame_retry_delay_s": 0,
            "sn_read_timeout_s": 0.05,
            "sn_read_attempts": 1,
            "sn_retry_delay_s": 0,
            "read_sn_before_setup": True,
            "read_identity_before_setup": True,
            "read_mode2_frames_after_setup": True,
            "neutral_coefficient_restore_included": False,
        },
        "analyzers": [
            {
                "slot": "GA01",
                "enabled": True,
                "port": "COM36",
                "baud": 115200,
                "protocol_device_id": "047",
                "sn_code": "01260601",
                "device_code": "01260601",
            }
        ],
    }
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(config.get(key), dict):
            config[key].update(value)
        else:
            config[key] = value
    return config


class _FakeSerial:
    def __init__(self, sn_code: str, *, device_id: str = "047", active_rate_hz: float = 1.0, respond_to_sn: bool = True):
        self.sn_code = sn_code
        self.device_id = device_id
        self.active_rate_hz = active_rate_hz
        self.respond_to_sn = respond_to_sn
        self.writes = []
        self.flush_count = 0
        self._sn_pending = False

    def flush_input(self):
        self.flush_count += 1

    def write(self, text):
        self.writes.append(text)
        if str(text).strip().upper() == "SN,YGAS,FFF":
            self._sn_pending = True

    def readline(self):
        if self._sn_pending and self.respond_to_sn:
            self._sn_pending = False
            return f"SN,YGAS,047,{self.sn_code}"
        self._sn_pending = False
        return ""

    def drain_input_nonblock(self, drain_s=0.35, **_kwargs):
        count = max(0, int(round(float(self.active_rate_hz) * float(drain_s))))
        return [
            f"YGAS,{self.device_id},400.0,1.0,1,1,1,1,1,1,1,1,25.0,25.0,101.3,OK"
            for _idx in range(count)
        ]


class _FakeAnalyzer:
    def __init__(self, item, *, active_rate_hz: float | None = None, respond_to_sn: bool = True):
        self.item = dict(item)
        configured_rate_hz = float(active_rate_hz if active_rate_hz is not None else item.get("active_rate_hz", 1.0))
        self.ser = _FakeSerial(
            str(item["sn_code"]),
            device_id=str(item["protocol_device_id"]),
            active_rate_hz=configured_rate_hz,
            respond_to_sn=respond_to_sn,
        )
        self.calls = []
        self.opened = False
        self.closed = False

    def open(self):
        self.opened = True
        self.calls.append(("open",))

    def close(self):
        self.closed = True
        self.calls.append(("close",))

    def read_current_mode_snapshot(self, **_kwargs):
        self.calls.append(("read_current_mode_snapshot",))
        return {"mode": 2, "id": self.item["protocol_device_id"], "raw": "mode2"}

    def set_comm_way_with_ack(self, active, require_ack=False):
        self.calls.append(("set_comm_way_with_ack", bool(active), bool(require_ack)))
        return True

    def set_mode_with_ack(self, mode, require_ack=False):
        self.calls.append(("set_mode_with_ack", int(mode), bool(require_ack)))
        return True

    def set_active_freq_with_ack(self, ftd_hz, require_ack=False):
        self.calls.append(("set_active_freq_with_ack", int(ftd_hz), bool(require_ack)))
        return True

    def set_average_filter_channel_with_ack(self, channel, value, require_ack=False):
        self.calls.append(("set_average_filter_channel_with_ack", int(channel), int(value), bool(require_ack)))
        return True

    def read_latest_data(self, **_kwargs):
        self.calls.append(("read_latest_data",))
        device_id = self.item["protocol_device_id"]
        return f"YGAS,{device_id},400.0,1.0,1,1,1,1,1,1,1,1,25.0,25.0,101.3,OK"

    def parse_line_mode2(self, line):
        parts = str(line).split(",")
        return {"mode": 2, "id": parts[1], "co2_ppm": 400.0, "h2o_mmol": 1.0}

    def set_senco(self, *_args, **_kwargs):
        raise AssertionError("runtime setup must not write SENCO")

    def set_device_id(self, *_args, **_kwargs):
        raise AssertionError("runtime setup must not write device id")

    def set_device_id_with_ack(self, *_args, **_kwargs):
        raise AssertionError("runtime setup must not write device id")

    def set_average(self, *_args, **_kwargs):
        raise AssertionError("runtime setup must not append AVERAGE1/2=1")

    def set_average_with_ack(self, *_args, **_kwargs):
        raise AssertionError("runtime setup must not append AVERAGE1/2=1")


def test_build_plan_rejects_senco_restore():
    config = _config(runtime_setup_contract={"neutral_coefficient_restore_included": True})

    try:
        runtime_setup.build_plan(config)
    except runtime_setup.RuntimeSetupError as exc:
        assert "SENCO" in str(exc)
    else:
        raise AssertionError("expected SENCO restore to be rejected")


def test_build_plan_lists_only_runtime_setup_commands():
    plan = runtime_setup.build_plan(_config())

    actions = [row["action"] for row in plan["commands"]]
    assert actions == [
        "set_comm_way_inactive",
        "set_mode2",
        "set_active_frequency",
        "set_average1_filter",
        "set_average2_filter",
        "set_comm_way_active",
    ]
    assert "set_senco" in plan["forbidden_actions"]
    assert "sampling" in plan["forbidden_actions"]
    assert plan["status"] == "preview_only"
    assert plan["boundary"]["opens_com_ports"] is False
    assert plan["boundary"]["sends_device_commands"] is False
    assert plan["boundary"]["writes_runtime_settings"] is False
    assert all(row["ack_required"] is True for row in plan["commands"])
    assert (
        plan["execution_contract"]["failure_recovery"]
        == "restore_active_send_with_ack"
    )
    device_plan = plan["identity_bound_command_plans"][0]
    assert device_plan["port"] == "COM36"
    assert device_plan["protocol_device_id"] == "047"
    assert device_plan["sn_code"] == "01260601"
    assert device_plan["command_steps"] == plan["commands"]


def test_build_plan_accepts_mature_devices_gas_analyzers_shape():
    config = _config()
    config["devices"] = {"gas_analyzers": config.pop("analyzers")}

    plan = runtime_setup.build_plan(config)

    assert plan["analyzer_count"] == 1
    assert plan["analyzers"][0]["slot"] == "GA01"
    assert plan["analyzers"][0]["protocol_device_id"] == "047"
    assert plan["analyzers"][0]["sn_code"] == "01260601"


def test_runtime_contract_defaults_include_sn_retry_policy():
    config = _config()
    config["runtime_setup_contract"].pop("sn_read_timeout_s")
    config["runtime_setup_contract"].pop("sn_read_attempts")
    config["runtime_setup_contract"].pop("sn_retry_delay_s")
    config["runtime_setup_contract"].pop("post_enable_stream_ack_wait_s")
    config["runtime_setup_contract"].pop("runtime_setup_retry_delay_s")
    config["runtime_setup_contract"].pop("ftd_hz")
    config["runtime_setup_contract"].pop("active_upload_rate_measure_s")

    plan = runtime_setup.build_plan(config)

    assert plan["contract"]["ftd_hz"] == 1
    assert plan["contract"]["command_gap_s"] == 1.2
    assert plan["contract"]["sn_read_timeout_s"] == 1.2
    assert plan["contract"]["sn_read_attempts"] == 3
    assert plan["contract"]["sn_retry_delay_s"] == 0.2
    assert plan["contract"]["post_enable_stream_ack_wait_s"] == 8.0
    assert plan["contract"]["runtime_setup_retry_count"] == 1
    assert plan["contract"]["runtime_setup_retry_delay_s"] == 1.2
    assert plan["contract"]["verify_active_upload_rate"] is True
    assert plan["contract"]["active_upload_rate_measure_s"] == 6.0


def test_runtime_contract_clamps_command_gap_to_minimum():
    config = _config(runtime_setup_contract={"command_gap_s": 0})

    plan = runtime_setup.build_plan(config)

    assert plan["contract"]["command_gap_s"] == 1.0


def test_main_dry_run_writes_plan_without_execute(tmp_path, capsys):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(_config()), encoding="utf-8")

    rc = runtime_setup.main(["--config", str(config_path), "--output-dir", str(tmp_path / "out")])

    captured = capsys.readouterr()
    assert rc == 0
    assert '"status": "dry_run"' in captured.out
    assert (tmp_path / "out" / "v1_5_analyzer_runtime_setup_plan.json").exists()
    assert not (tmp_path / "out" / "v1_5_analyzer_runtime_setup_result.json").exists()


def test_main_execute_requires_operator_confirmation(tmp_path, capsys):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(_config()), encoding="utf-8")

    rc = runtime_setup.main(["--config", str(config_path), "--output-dir", str(tmp_path / "out"), "--execute"])

    captured = capsys.readouterr()
    assert rc == 2
    assert "operator-confirm" in captured.err


def test_parse_sn_readback_accepts_only_short_sn_responses():
    assert runtime_setup.parse_sn_readback("SN,YGAS,FFF,01260601") == "01260601"
    assert runtime_setup.parse_sn_readback("YGAS,047,01260601") == "01260601"
    assert runtime_setup.parse_sn_readback("SN,YGAS,FFF,ABCDEF12") is None
    assert runtime_setup.parse_sn_readback(
        "YGAS,047,3000.000,00.000,13132102,-1270.,1.2761,1.2741,0.6244,0.6247,02697,03445,01688,022.70,022.82,102.08"
    ) is None
    assert runtime_setup.parse_sn_readback("noise 11111111 ack 01260606") is None


def test_read_sn_retries_after_active_stream_noise():
    class NoisySerial:
        def __init__(self):
            self.writes = []
            self.flush_count = 0
            self._attempt = 0
            self._pending = []

        def flush_input(self):
            self.flush_count += 1

        def write(self, text):
            self.writes.append(text)
            self._attempt += 1
            if self._attempt == 1:
                self._pending = [
                    "YGAS,004,0075.068,00.000,13132102,-1270.,1.2761,1.2741,0.6244,0.6247,02697,03445,01688,022.70,022.82,102.08"
                ]
            else:
                self._pending = ["SN,YGAS,004,01260604"]

        def readline(self):
            if self._pending:
                return self._pending.pop(0)
            return ""

        def drain_input_nonblock(self, **_kwargs):
            return []

    class Analyzer:
        ser = NoisySerial()

    sn_readback, sn_raw = runtime_setup._read_sn(Analyzer(), timeout_s=0.05, attempts=2, retry_delay_s=0)

    assert sn_readback == "01260604"
    assert sn_raw == "SN,YGAS,004,01260604"
    assert Analyzer.ser.writes == ["SN,YGAS,FFF\r\n", "SN,YGAS,FFF\r\n"]


def test_execute_runtime_setup_uses_identity_bound_order_and_no_write_commands(tmp_path, monkeypatch):
    sleeps, sleep_fn = _patch_runtime_sleep(monkeypatch)
    fake_analyzers = []

    def factory(item):
        analyzer = _FakeAnalyzer(item)
        fake_analyzers.append(analyzer)
        return analyzer

    result = runtime_setup.execute_runtime_setup(
        _config(),
        output_dir=tmp_path,
        analyzer_factory=factory,
        sleep_fn=sleep_fn,
    )

    assert result["status"] == "ready"
    assert result["evidence_source"] == "real_device_runtime_setup"
    assert result["execution_mode"] == "controlled_real_com"
    assert result["engineering_setup_only"] is True
    assert result["not_real_acceptance_evidence"] is True
    assert result["results"][0]["status"] == "ready"
    assert result["results"][0]["sn_readback"] == "01260601"
    analyzer = fake_analyzers[0]
    assert analyzer.opened
    assert analyzer.closed
    call_names = [call[0] for call in analyzer.calls]
    assert call_names == [
        "open",
        "read_current_mode_snapshot",
        "set_comm_way_with_ack",
        "set_mode_with_ack",
        "set_active_freq_with_ack",
        "set_average_filter_channel_with_ack",
        "set_average_filter_channel_with_ack",
        "set_comm_way_with_ack",
        "read_latest_data",
        "read_latest_data",
        "close",
    ]
    assert ("set_average_filter_channel_with_ack", 1, 49, True) in analyzer.calls
    assert ("set_average_filter_channel_with_ack", 2, 49, True) in analyzer.calls
    assert analyzer.ser.writes == ["SN,YGAS,FFF\r\n"]
    assert result["results"][0]["active_upload_rate"]["ok"] is True
    assert result["results"][0]["active_upload_rate"]["approx_hz"] == 1.0
    assert result["results"][0]["runtime_setup_attempt_count"] == 1
    assert result["results"][0]["identity_after"] == {
        "mode": 2,
        "id": "047",
        "source": "verified_mode2_frame_after_runtime_setup",
    }
    assert all(
        event["ack_required"] is True and event["ack_received"] is True
        for event in result["results"][0]["runtime_setup_events"]
    )
    assert result["boundary"]["all_configuration_commands_require_ack"] is True
    assert any(seconds >= 1.0 for seconds in sleeps)
    assert (tmp_path / "v1_5_analyzer_runtime_setup_result.json").exists()


def test_execute_runtime_setup_rechecks_mode2_after_ack_wait(tmp_path, monkeypatch):
    _sleeps, sleep_fn = _patch_runtime_sleep(monkeypatch)
    fake_analyzers = []

    def factory(item):
        analyzer = _FakeAnalyzer(item)
        fake_analyzers.append(analyzer)
        return analyzer

    config = _config(runtime_setup_contract={"post_enable_stream_ack_wait_s": 0.01})

    result = runtime_setup.execute_runtime_setup(
        config,
        output_dir=tmp_path,
        analyzer_factory=factory,
        sleep_fn=sleep_fn,
    )

    row = result["results"][0]
    analyzer = fake_analyzers[0]
    assert row["status"] == "ready"
    assert row["post_enable_stream_ack_wait_s"] == 0.01
    assert len(row["mode2_frames"]) == 2
    assert len(row["mode2_frames_after_ack_wait"]) == 2
    assert [call[0] for call in analyzer.calls].count("read_latest_data") == 4


def test_execute_runtime_setup_blocks_when_active_upload_rate_does_not_match_ftd(tmp_path, monkeypatch):
    _sleeps, sleep_fn = _patch_runtime_sleep(monkeypatch)
    def factory(item):
        return _FakeAnalyzer(item, active_rate_hz=10.0)

    config = _config(
        runtime_setup_contract={
            "ftd_hz": 1,
            "active_upload_rate_measure_s": 2,
            "active_upload_rate_tolerance_abs_hz": 0.3,
            "active_upload_rate_tolerance_fraction": 0.3,
        }
    )

    result = runtime_setup.execute_runtime_setup(
        config,
        output_dir=tmp_path,
        analyzer_factory=factory,
        sleep_fn=sleep_fn,
    )

    row = result["results"][0]
    assert result["status"] == "partial"
    assert row["status"] == "active_upload_rate_mismatch"
    assert row["active_upload_rate"]["target_hz"] == 1
    assert row["active_upload_rate"]["approx_hz"] == 10.0
    assert row["runtime_setup_attempt_count"] == 2


def test_execute_runtime_setup_retries_runtime_sequence_when_ftd_does_not_take_effect(tmp_path, monkeypatch):
    _sleeps, sleep_fn = _patch_runtime_sleep(monkeypatch)
    fake_analyzers = []

    class FlakyFtdAnalyzer(_FakeAnalyzer):
        def __init__(self, item):
            super().__init__(item, active_rate_hz=10.0)
            self.ftd_write_count = 0

        def set_active_freq_with_ack(self, ftd_hz, require_ack=False):
            self.ftd_write_count += 1
            if self.ftd_write_count >= 2:
                self.ser.active_rate_hz = float(ftd_hz)
            return super().set_active_freq_with_ack(ftd_hz, require_ack=require_ack)

    def factory(item):
        analyzer = FlakyFtdAnalyzer(item)
        fake_analyzers.append(analyzer)
        return analyzer

    config = _config(
        runtime_setup_contract={
            "ftd_hz": 1,
            "active_upload_rate_measure_s": 2,
            "runtime_setup_retry_count": 1,
        }
    )

    result = runtime_setup.execute_runtime_setup(
        config,
        output_dir=tmp_path,
        analyzer_factory=factory,
        sleep_fn=sleep_fn,
    )

    row = result["results"][0]
    analyzer = fake_analyzers[0]
    assert result["status"] == "ready"
    assert row["status"] == "ready"
    assert row["runtime_setup_attempt_count"] == 2
    assert row["runtime_setup_attempts"][0]["status"] == "active_upload_rate_mismatch"
    assert row["runtime_setup_attempts"][0]["active_upload_rate"]["approx_hz"] == 10.0
    assert row["runtime_setup_attempts"][1]["status"] == "ready"
    assert row["runtime_setup_attempts"][1]["active_upload_rate"]["approx_hz"] == 1.0
    assert analyzer.ftd_write_count == 2


def test_execute_runtime_setup_blocks_when_sn_is_missing(tmp_path, monkeypatch):
    _sleeps, sleep_fn = _patch_runtime_sleep(monkeypatch)
    def factory(item):
        return _FakeAnalyzer(item, respond_to_sn=False)

    result = runtime_setup.execute_runtime_setup(
        _config(),
        output_dir=tmp_path,
        analyzer_factory=factory,
        sleep_fn=sleep_fn,
    )

    assert result["status"] == "partial"
    row = result["results"][0]
    assert row["status"] == "error"
    assert "SN readback missing" in row["error"]
    assert row["runtime_setup_events"] == []


def test_execute_runtime_setup_blocks_when_command_fails(tmp_path, monkeypatch):
    _sleeps, sleep_fn = _patch_runtime_sleep(monkeypatch)
    class FailingAnalyzer(_FakeAnalyzer):
        def set_active_freq_with_ack(self, ftd_hz, require_ack=False):
            self.calls.append(("set_active_freq_with_ack", int(ftd_hz), bool(require_ack)))
            return False

    def factory(item):
        return FailingAnalyzer(item)

    result = runtime_setup.execute_runtime_setup(
        _config(),
        output_dir=tmp_path,
        analyzer_factory=factory,
        sleep_fn=sleep_fn,
    )

    assert result["status"] == "partial"
    row = result["results"][0]
    assert row["status"] == "error"
    assert "set_active_frequency" in row["error"]
    assert (
        row["failure_status"]
        == "runtime_setup_command_failed_recovered_active_send"
    )
    assert [event["action"] for event in row["runtime_setup_events"]] == [
        "set_comm_way_inactive",
        "set_mode2",
        "set_active_frequency",
    ]
    assert row["failure_recovery_events"] == [
        {
            "action": "restore_comm_way_active_after_failure",
            "command_preview": "SETCOMWAY,YGAS,FFF,1",
            "ack_required": True,
            "ack_received": True,
            "ok": True,
            "category": "failure_recovery",
        }
    ]
    assert not row["mode2_frames"]


def test_execute_runtime_setup_preflights_required_methods_before_open(
    tmp_path,
    monkeypatch,
):
    _sleeps, sleep_fn = _patch_runtime_sleep(monkeypatch)
    fake_analyzers = []

    def factory(item):
        analyzer = _FakeAnalyzer(item)
        analyzer.set_average_filter_channel_with_ack = None
        fake_analyzers.append(analyzer)
        return analyzer

    result = runtime_setup.execute_runtime_setup(
        _config(),
        output_dir=tmp_path,
        analyzer_factory=factory,
        sleep_fn=sleep_fn,
    )

    row = result["results"][0]
    analyzer = fake_analyzers[0]
    assert result["status"] == "partial"
    assert row["status"] == "error"
    assert "methods missing before COM open" in row["error"]
    assert "set_average_filter_channel_with_ack" in row["error"]
    assert analyzer.opened is False
    assert row["runtime_setup_events"] == []


def test_execute_runtime_setup_records_failed_active_send_recovery(
    tmp_path,
    monkeypatch,
):
    _sleeps, sleep_fn = _patch_runtime_sleep(monkeypatch)

    class FailingRecoveryAnalyzer(_FakeAnalyzer):
        def set_comm_way_with_ack(self, active, require_ack=False):
            self.calls.append(
                ("set_comm_way_with_ack", bool(active), bool(require_ack))
            )
            return not bool(active)

        def set_active_freq_with_ack(self, ftd_hz, require_ack=False):
            self.calls.append(
                ("set_active_freq_with_ack", int(ftd_hz), bool(require_ack))
            )
            return False

    result = runtime_setup.execute_runtime_setup(
        _config(),
        output_dir=tmp_path,
        analyzer_factory=FailingRecoveryAnalyzer,
        sleep_fn=sleep_fn,
    )

    row = result["results"][0]
    assert result["status"] == "partial"
    assert row["status"] == "error"
    assert (
        row["failure_status"]
        == "runtime_setup_command_failed_recovery_failed"
    )
    assert row["failure_recovery_events"][0]["ack_received"] is False
