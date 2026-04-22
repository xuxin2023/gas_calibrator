import pytest

from gas_calibrator.devices.gas_analyzer import GasAnalyzer


def test_parse_mode2_16_fields() -> None:
    line = (
        "YGAS,097,0658.169,06.783,1240.638,05.191,1.2453,1.2430,"
        "0.7992,0.7992,03178,03948,02538,031.60,031.74,106.06"
    )
    parsed = GasAnalyzer._parse_mode2(line.split(","), line)
    assert parsed is not None
    assert parsed["mode"] == 2
    assert parsed["mode2_field_count"] == 16
    assert parsed["co2_ppm"] == 658.169
    assert parsed["h2o_mmol"] == 6.783
    assert parsed["pressure_kpa"] == 106.06
    assert parsed["status"] is None


def test_parse_mode2_17_fields_with_status() -> None:
    line = (
        "YGAS,097,0658.169,06.783,1240.638,05.191,1.2453,1.2430,"
        "0.7992,0.7992,03178,03948,02538,031.60,031.74,106.06,OK"
    )
    parsed = GasAnalyzer._parse_mode2(line.split(","), line)
    assert parsed is not None
    assert parsed["mode2_field_count"] == 17
    assert parsed["status"] == "OK"


def test_parse_mode2_keeps_extra_tokens() -> None:
    line = (
        "YGAS,097,0658.169,06.783,1240.638,05.191,1.2453,1.2430,"
        "0.7992,0.7992,03178,03948,02538,031.60,031.74,106.06,OK,EX1,EX2"
    )
    parsed = GasAnalyzer._parse_mode2(line.split(","), line)
    assert parsed is not None
    assert parsed["mode2_field_count"] == 19
    assert parsed["mode2_extra_01"] == "EX1"
    assert parsed["mode2_extra_02"] == "EX2"


def test_parse_mode2_rejects_short_legacy_like_frame() -> None:
    line = "YGAS,097,0658.169,06.783,1240.638,05.191,1.2453,1.2430,0.7992,0.7992"
    parsed = GasAnalyzer._parse_mode2(line.split(","), line)
    assert parsed is None


def test_parse_line_falls_back_to_legacy_for_short_frame() -> None:
    ga = GasAnalyzer("COM1")
    line = "YGAS,097,0658.169,06.783,1240.638,05.191,1.2453,1.2430,0.7992,0.7992"

    parsed = ga.parse_line(line)

    assert parsed is not None
    assert parsed.get("mode") != 2
    assert parsed["co2_ppm"] == 658.169
    assert parsed["h2o_mmol"] == 6.783


def test_parse_line_mode2_skips_ack_and_uses_data_frame() -> None:
    ga = GasAnalyzer("COM1")
    line = (
        "<YGAS,077,T> YGAS,097,0658.169,06.783,1240.638,05.191,1.2453,1.2430,"
        "0.7992,0.7992,03178,03948,02538,031.60,031.74,106.06,OK"
    )
    parsed = ga.parse_line_mode2(line)
    assert parsed is not None
    assert parsed["id"] == "097"
    assert parsed["status"] == "OK"
    assert parsed["pressure_kpa"] == 106.06


def test_parse_line_mode2_tolerates_noise_prefix_and_suffix() -> None:
    ga = GasAnalyzer("COM1")
    line = (
        "noise-prefix << YGAS,097,0658.169,06.783,1240.638,05.191,1.2453,1.2430,"
        "0.7992,0.7992,03178,03948,02538,031.60,031.74,106.06,OK>> tail-noise"
    )
    parsed = ga.parse_line_mode2(line)
    assert parsed is not None
    assert parsed["co2_ppm"] == 658.169
    assert parsed["status"] == "OK"


def test_parse_line_mode2_rejects_nonstandard_frame_without_required_values() -> None:
    ga = GasAnalyzer("COM1")
    assert ga.parse_line_mode2("junk<YGAS,097,T>more-junk") is None


class _FakeSerialForConfig:
    def __init__(self, ack_on_flush: int = 1) -> None:
        self.writes = []
        self.flushed = 0
        self.ack_on_flush = ack_on_flush
        self._ack_sent_for_flush = set()
        self.logged = []

    def write(self, data: str) -> None:
        self.writes.append(data)

    def flush_input(self) -> None:
        self.flushed += 1

    def drain_input_nonblock(self, drain_s: float = 0.35, read_timeout_s: float = 0.05):
        if self.flushed >= self.ack_on_flush and self.flushed not in self._ack_sent_for_flush:
            self._ack_sent_for_flush.add(self.flushed)
            return ["<YGAS,123,T>"]
        return []

    def _log_io(self, direction: str, command=None, response=None, error=None) -> None:
        self.logged.append({"direction": direction, "command": command, "response": response, "error": error})


class _FakeSerialForPassiveRead:
    def __init__(self, lines) -> None:
        self.writes = []
        self._lines = list(lines)

    def write(self, data: str) -> None:
        self.writes.append(data)

    def readline(self) -> str:
        if self._lines:
            return self._lines.pop(0)
        return ""


class _FakeSerialForActiveRead:
    def __init__(self, lines) -> None:
        self._lines = list(lines)

    def drain_input_nonblock(self, drain_s: float = 0.35, read_timeout_s: float = 0.05):
        if not self._lines:
            return []
        lines = list(self._lines)
        self._lines.clear()
        return lines


class _FakeSerialForActiveReadBatches:
    def __init__(self, batches) -> None:
        self._batches = [list(batch) for batch in batches]

    def drain_input_nonblock(self, drain_s: float = 0.35, read_timeout_s: float = 0.05):
        if not self._batches:
            return []
        return self._batches.pop(0)


def test_success_ack_accepts_any_device_id() -> None:
    assert GasAnalyzer._is_success_ack("<YGAS,123,T>") is True
    assert GasAnalyzer._is_success_ack("YGAS,FFF,T") is True
    assert GasAnalyzer._is_success_ack("YGAS,077,T YGAS,077,3000.000,...") is True
    assert GasAnalyzer._is_success_ack("YGAS,12,T") is False


def test_set_comm_way_waits_for_success_ack() -> None:
    ga = GasAnalyzer("COM1")
    fake = _FakeSerialForConfig()
    ga.ser = fake

    ga.set_comm_way(False)

    assert fake.flushed == 1
    assert fake.writes == ["SETCOMWAY,YGAS,FFF,0\r\n"]


def test_set_comm_way_retries_when_startup_ack_is_missing() -> None:
    ga = GasAnalyzer("COM1")
    fake = _FakeSerialForConfig(ack_on_flush=3)
    ga.ser = fake

    ga.set_comm_way(False)

    assert fake.flushed == 3
    assert fake.writes == [
        "SETCOMWAY,YGAS,FFF,0\r\n",
        "SETCOMWAY,YGAS,FFF,0\r\n",
        "SETCOMWAY,YGAS,FFF,0\r\n",
    ]


def test_set_mode_and_ftd_use_manual_argument_order() -> None:
    ga = GasAnalyzer("COM1")
    fake = _FakeSerialForConfig()
    ga.ser = fake

    assert ga.set_mode(2) is True
    assert ga.set_active_freq(10) is True

    assert fake.writes == [
        "MODE,YGAS,FFF,2\r\n",
        "FTD,YGAS,FFF,10\r\n",
    ]


def test_device_id_helpers_normalize_version_and_write_id_command() -> None:
    ga = GasAnalyzer("COM1")
    fake = _FakeSerialForConfig()
    ga.ser = fake

    assert GasAnalyzer.normalize_software_version("legacy") == GasAnalyzer.SOFTWARE_VERSION_PRE_V5
    assert GasAnalyzer.normalize_software_version("v5") == GasAnalyzer.SOFTWARE_VERSION_V5_PLUS
    assert ga.set_device_id("7") is True

    assert ga.device_id == "007"
    assert fake.writes == ["ID,YGAS,FFF,007\r\n"]


def test_set_average_filter_uses_broadcast_and_waits_for_success_ack() -> None:
    ga = GasAnalyzer("COM1")
    fake = _FakeSerialForConfig()
    ga.ser = fake

    assert ga.set_average_filter(49) is True

    assert fake.flushed == 2
    assert fake.writes == [
        "AVERAGE1,YGAS,FFF,49\r\n",
        "AVERAGE2,YGAS,FFF,49\r\n",
    ]


def test_minimal_init_commands_can_skip_ack_waiting() -> None:
    ga = GasAnalyzer("COM1")
    fake = _FakeSerialForConfig(ack_on_flush=999)
    ga.ser = fake

    assert ga.set_comm_way_with_ack(False, require_ack=False) is True
    assert ga.set_mode_with_ack(2, require_ack=False) is True
    assert ga.set_average_filter_with_ack(49, require_ack=False) is True
    assert ga.set_comm_way_with_ack(True, require_ack=False) is True

    assert fake.writes == [
        "SETCOMWAY,YGAS,FFF,0\r\n",
        "MODE,YGAS,FFF,2\r\n",
        "AVERAGE1,YGAS,FFF,49\r\n",
        "AVERAGE2,YGAS,FFF,49\r\n",
        "SETCOMWAY,YGAS,FFF,1\r\n",
    ]


def test_set_average_uses_h2o_on_average1_and_co2_on_average2() -> None:
    ga = GasAnalyzer("COM1")
    fake = _FakeSerialForConfig()
    ga.ser = fake

    assert ga.set_average(co2_n=7, h2o_n=5) is True

    assert fake.writes == [
        "AVERAGE1,YGAS,FFF,5\r\n",
        "AVERAGE2,YGAS,FFF,7\r\n",
    ]


def test_set_senco_supports_manual_six_value_format() -> None:
    ga = GasAnalyzer("COM1")
    fake = _FakeSerialForConfig()
    ga.ser = fake

    assert ga.set_senco(1, 65916.6, -106614.0, 57735.1, -10584.9, 0.0, 0.0) is True

    assert fake.writes == [
        "SETCOMWAY,YGAS,FFF,0\r\n",
        "SENCO1,YGAS,FFF,6.59166e04,-1.06614e05,5.77351e04,-1.05849e04,0.00000e00,0.00000e00\r\n",
    ]


def test_set_senco_accepts_legacy_four_value_payloads() -> None:
    ga = GasAnalyzer("COM1")
    fake = _FakeSerialForConfig()
    ga.ser = fake

    assert ga.set_senco(7, 0.0, 1.0, -0.15, 2.5e-6) is True

    assert fake.writes == [
        "SETCOMWAY,YGAS,FFF,0\r\n",
        "SENCO7,YGAS,FFF,0.00000e00,1.00000e00,-1.50000e-01,2.50000e-06\r\n",
    ]


def test_format_senco_value_normalizes_zero_and_positive_exponent() -> None:
    assert GasAnalyzer._format_senco_value(0.0) == "0.00000e00"
    assert GasAnalyzer._format_senco_value(-0.0) == "0.00000e00"
    assert GasAnalyzer._format_senco_value(1.0) == "1.00000e00"
    assert GasAnalyzer._format_senco_value(65916.6) == "6.59166e04"


class _FakeSerialForCoefficientRead:
    def __init__(self, lines=None, drain_batches=None) -> None:
        self.lines = list(lines or [])
        self.drain_batches = [list(batch) for batch in (drain_batches or [])]
        self.writes = []
        self.flushes = 0

    def write(self, data: str) -> None:
        self.writes.append(data)

    def flush_input(self) -> None:
        self.flushes += 1

    def readline(self) -> str:
        if self.lines:
            return self.lines.pop(0)
        return ""

    def drain_input_nonblock(self, drain_s: float = 0.35, read_timeout_s: float = 0.05):
        if self.drain_batches:
            return self.drain_batches.pop(0)
        return []


def test_parse_coefficient_group_line() -> None:
    parsed = GasAnalyzer.parse_coefficient_group_line("<C0:65916.6,C1:-106614,C2:57735.1,C3:-10584.9>")

    assert parsed == {
        "C0": 65916.6,
        "C1": -106614.0,
        "C2": 57735.1,
        "C3": -10584.9,
    }


def test_inspect_coefficient_group_line_marks_explicit_c0_source() -> None:
    inspected = GasAnalyzer.inspect_coefficient_group_line("<C0:1.1,C1:2.2,C2:3.3,C3:4.4>")

    assert inspected["source"] == GasAnalyzer.READBACK_SOURCE_EXPLICIT_C0
    assert inspected["source_line_has_explicit_c0"] is True


def test_parse_coefficient_group_line_accepts_mixed_stream_and_coefficients() -> None:
    parsed = GasAnalyzer.parse_coefficient_group_line(
        "YGAS,079,0782.713,00.000,0.99,0.99,031.94,104.24,0001,2769 <C0:1.1,C1:2.2,C2:3.3,C3:4.4>"
    )

    assert parsed == {
        "C0": 1.1,
        "C1": 2.2,
        "C2": 3.3,
        "C3": 4.4,
    }


def test_inspect_coefficient_group_line_marks_mixed_line_ambiguous() -> None:
    inspected = GasAnalyzer.inspect_coefficient_group_line(
        "YGAS,079,0782.713,00.000,0.99,0.99,031.94,104.24,0001,2769 <C0:1.1,C1:2.2,C2:3.3,C3:4.4>"
    )

    assert inspected["source"] == GasAnalyzer.READBACK_SOURCE_AMBIGUOUS
    assert inspected["source_line_has_explicit_c0"] is False


def test_parse_coefficient_group_line_does_not_misread_plain_legacy_stream() -> None:
    parsed = GasAnalyzer.parse_coefficient_group_line("YGAS,079,0782.713,00.000,0.99,0.99,031.94,104.24,0001,2769")

    assert parsed is None


def test_read_coefficient_group_uses_getco_query() -> None:
    ga = GasAnalyzer("COM1")
    fake = _FakeSerialForCoefficientRead(["<C0:1,C1:2,C2:3,C3:4>"])
    ga.ser = fake

    parsed = ga.read_coefficient_group(1, delay_s=0.0, retries=0)

    assert parsed == {"C0": 1.0, "C1": 2.0, "C2": 3.0, "C3": 4.0}
    assert fake.flushes == 3
    assert fake.writes == [
        "SETCOMWAY,YGAS,FFF,0\r\n",
        "GETCO,YGAS,000,1\r\n",
    ]


def test_read_coefficient_group_accepts_ack_then_coefficient_line() -> None:
    ga = GasAnalyzer("COM1")
    fake = _FakeSerialForCoefficientRead(
        ["<YGAS,123,T>"],
        drain_batches=[["<C0:1,C1:2,C2:3,C3:4>"]],
    )
    ga.ser = fake

    parsed = ga.read_coefficient_group(1, delay_s=0.0, retries=0, timeout_s=0.05)

    assert parsed == {"C0": 1.0, "C1": 2.0, "C2": 3.0, "C3": 4.0}


def test_read_coefficient_group_accepts_noise_then_coefficient_line() -> None:
    ga = GasAnalyzer("COM1")
    fake = _FakeSerialForCoefficientRead(
        ["noise-line"],
        drain_batches=[["junk-prefix", "<C0:1,C1:2,C2:3,C3:4>"]],
    )
    ga.ser = fake

    parsed = ga.read_coefficient_group(2, delay_s=0.0, retries=0, timeout_s=0.05)

    assert parsed == {"C0": 1.0, "C1": 2.0, "C2": 3.0, "C3": 4.0}


def test_read_coefficient_group_capture_marks_ambiguous_source() -> None:
    ga = GasAnalyzer("COM1")
    fake = _FakeSerialForCoefficientRead(
        ["YGAS,079,0782.713,00.000,0.99,0.99,031.94,104.24,0001,2769 <C0:1,C1:2,C2:3,C3:4>"]
    )
    ga.ser = fake

    capture = ga.read_coefficient_group_capture(1, delay_s=0.0, retries=0, timeout_s=0.05)

    assert capture["source"] == GasAnalyzer.READBACK_SOURCE_AMBIGUOUS
    assert capture["source_line_has_explicit_c0"] is False
    assert capture["coefficients"] == {"C0": 1.0, "C1": 2.0, "C2": 3.0, "C3": 4.0}


def test_read_coefficient_group_requires_explicit_c0_when_requested() -> None:
    ga = GasAnalyzer("COM1")
    fake = _FakeSerialForCoefficientRead(
        ["YGAS,079,0782.713,00.000,0.99,0.99,031.94,104.24,0001,2769 <C0:1,C1:2,C2:3,C3:4>"]
    )
    ga.ser = fake

    with pytest.raises(RuntimeError, match="AMBIGUOUS_COEFFICIENT_LINE"):
        ga.read_coefficient_group(1, delay_s=0.0, retries=0, timeout_s=0.05, require_explicit_c0=True)


def test_read_coefficient_group_capture_rejects_write_echo_without_c0() -> None:
    ga = GasAnalyzer("COM1")
    fake = _FakeSerialForCoefficientRead(["SENCO1,YGAS,FFF,1,2,3,4,0,0"])
    ga.ser = fake

    capture = ga.read_coefficient_group_capture(1, delay_s=0.0, retries=0, timeout_s=0.05)

    assert capture["source"] == GasAnalyzer.READBACK_SOURCE_NONE
    assert capture["error"] == "NO_VALID_COEFFICIENT_LINE"
    assert capture["coefficients"] == {}


def test_build_getco_command_supports_compact_style() -> None:
    ga = GasAnalyzer("COM1", device_id="079")

    command = ga.build_getco_command(7, target_id="079", command_style="compact")

    assert command == "GETCO7,YGAS,079\r\n"


def test_read_coefficient_group_capture_can_skip_builtin_prepare_and_use_compact_style() -> None:
    ga = GasAnalyzer("COM1", device_id="079")
    fake = _FakeSerialForCoefficientRead(["C0:-1.50402,C1:0.975407,C2:0.00190803,C3:-4.78878e-05"])
    ga.ser = fake

    capture = ga.read_coefficient_group_capture(
        7,
        delay_s=0.0,
        retries=0,
        timeout_s=0.05,
        target_id="079",
        command_style="compact",
        prepare_io=False,
    )

    assert capture["source"] == GasAnalyzer.READBACK_SOURCE_EXPLICIT_C0
    assert fake.writes == ["GETCO7,YGAS,079\r\n"]


def test_read_coefficient_group_reports_ack_only_failure() -> None:
    ga = GasAnalyzer("COM1")
    fake = _FakeSerialForCoefficientRead(["<YGAS,123,T>"])
    ga.ser = fake

    try:
        ga.read_coefficient_group(3, delay_s=0.0, retries=0, timeout_s=0.05)
    except RuntimeError as exc:
        assert "ACK_ONLY" in str(exc)
    else:
        raise AssertionError("expected ACK_ONLY failure")


def test_set_mode_returns_false_when_ack_is_missing() -> None:
    ga = GasAnalyzer("COM1")
    fake = _FakeSerialForConfig(ack_on_flush=999)
    ga.ser = fake

    assert ga.set_mode(2) is False


def test_read_data_passive_retries_once_after_empty_frame() -> None:
    ga = GasAnalyzer("COM1")
    fake = _FakeSerialForPassiveRead(["", "YGAS,123,1,2,3,4,5,6,7,8,9,10,11,12,13,14"])
    ga.ser = fake

    line = ga.read_data_passive()

    assert line == "YGAS,123,1,2,3,4,5,6,7,8,9,10,11,12,13,14"
    assert fake.writes == [
        "READDATA,YGAS,FFF\r\n",
        "READDATA,YGAS,FFF\r\n",
    ]


def test_read_latest_data_prefers_latest_stream_frame_when_active_send_enabled() -> None:
    ga = GasAnalyzer("COM1")
    ga.ser = _FakeSerialForActiveRead(
        [
            "YGAS,001,400.0,1.0,1,1,1,1,1,1,1,1,20.00,20.10,101.30,OK",
            "YGAS,001,401.5,1.1,1,1,1,1,1,1,1,1,20.05,20.15,101.35,OK",
        ]
    )
    ga.active_send = True

    line = ga.read_latest_data()

    assert line == "YGAS,001,401.5,1.1,1,1,1,1,1,1,1,1,20.05,20.15,101.35,OK"


def test_read_data_active_quickly_retries_until_full_mode2_frame_arrives() -> None:
    ga = GasAnalyzer("COM1")
    ga.ser = _FakeSerialForActiveReadBatches(
        [
            ["YGAS,086,0244.000,01.823,0235.648,00.7"],
            ["YGAS,086,0405.304,00.752,0825.985,00.633,1.3128,1.3134,0.7396,0.7398,03653,04799,02701,023.96,023.74,114.13"],
        ]
    )
    ga.active_send = True
    ga.ACTIVE_READ_RETRY_COUNT = 2
    ga.ACTIVE_READ_RETRY_DELAY_S = 0.0

    line = ga.read_data_active()

    assert line == "YGAS,086,0405.304,00.752,0825.985,00.633,1.3128,1.3134,0.7396,0.7398,03653,04799,02701,023.96,023.74,114.13"


def test_read_data_active_ignores_ack_and_short_frame_until_full_mode2_arrives() -> None:
    ga = GasAnalyzer("COM1")
    ga.ser = _FakeSerialForActiveReadBatches(
        [
            ["YGAS,097,T"],
            ["YGAS,086,0244.000,01.823,0235.648,00.7"],
            ["YGAS,086,0405.304,00.752,0825.985,00.633,1.3128,1.3134,0.7396,0.7398,03653,04799,02701,023.96,023.74,114.13"],
        ]
    )
    ga.active_send = True
    ga.ACTIVE_READ_RETRY_COUNT = 3
    ga.ACTIVE_READ_RETRY_DELAY_S = 0.0

    line = ga.read_latest_data()

    assert line == "YGAS,086,0405.304,00.752,0825.985,00.633,1.3128,1.3134,0.7396,0.7398,03653,04799,02701,023.96,023.74,114.13"


def test_set_comm_way_tracks_requested_stream_mode() -> None:
    ga = GasAnalyzer("COM1")
    fake = _FakeSerialForConfig()
    ga.ser = fake

    ga.set_comm_way(True)

    assert ga.active_send is True
    assert fake.writes == ["SETCOMWAY,YGAS,FFF,1\r\n"]


def test_set_comm_way_uses_startup_no_ack_codes_when_warning_phase_is_startup() -> None:
    ga = GasAnalyzer("COM1")
    fake = _FakeSerialForConfig(ack_on_flush=999)
    ga.ser = fake
    ga.set_warning_phase("startup")

    ga.set_comm_way(True)

    responses = [str(item.get("response") or "") for item in fake.logged]
    assert any("STARTUP_NO_ACK_RETRY" in item for item in responses)
    assert "STARTUP_NO_ACK" in responses
