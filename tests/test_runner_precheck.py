from pathlib import Path

from gas_calibrator.devices.gas_analyzer import GasAnalyzer
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow.runner import CalibrationRunner


class _FakeSerialFeed:
    def __init__(self, chunks):
        self._chunks = list(chunks)

    def read_available(self):
        if self._chunks:
            return self._chunks.pop(0)
        return ""

    def readline(self):
        return ""


class _FakeGasAnalyzer:
    def __init__(self, chunks):
        self._chunks = list(chunks)
        self.ser = _FakeSerialFeed([])
        self.calls = []

    def set_mode(self, mode):
        self.calls.append(("mode", mode, True))
        return True

    def set_mode_with_ack(self, mode, require_ack=True):
        self.calls.append(("mode", mode, bool(require_ack)))
        return True

    def set_comm_way(self, active):
        self.calls.append(("comm_way", active, True))
        return True

    def set_comm_way_with_ack(self, active, require_ack=True):
        self.calls.append(("comm_way", active, bool(require_ack)))
        return True

    def set_active_freq(self, hz):
        self.calls.append(("ftd", hz))
        return True

    def set_average(self, co2_n, h2o_n):
        self.calls.append(("avg", co2_n, h2o_n))
        return True

    def set_average_filter(self, window_n):
        self.calls.append(("avg_filter", window_n, True))
        return True

    def set_average_filter_with_ack(self, window_n, require_ack=True):
        self.calls.append(("avg_filter", window_n, bool(require_ack)))
        return True

    def read_data_passive(self):
        if self._chunks:
            return self._chunks.pop(0)
        return ""

    def read_latest_data(self, *args, **kwargs):
        return self.read_data_passive()

    @staticmethod
    def parse_line_mode2(line):
        return GasAnalyzer._parse_mode2((line or "").split(","), line)


def _runner(tmp_path: Path, strict: bool, chunks) -> CalibrationRunner:
    cfg = {
        "workflow": {
            "sensor_precheck": {
                "enabled": True,
                "mode": 2,
                "active_send": True,
                "ftd_hz": 1,
                "average_filter": 49,
                "average_co2": 1,
                "average_h2o": 1,
                "duration_s": 0.8,
                "poll_s": 0.01,
                "min_valid_frames": 2,
                "strict": strict,
            }
        }
    }
    logger = RunLogger(tmp_path)
    devices = {"gas_analyzer": _FakeGasAnalyzer(chunks)}
    return CalibrationRunner(cfg, devices, logger, lambda *_: None, lambda *_: None)


def test_sensor_precheck_passes_with_valid_frames(tmp_path: Path) -> None:
    chunks = [
        "YGAS,001,499.0,1.0,101.0,1.1,0.1,0.1,0.2,0.2,1000,1001,1002,25.0,25.1,101.3\r\n",
        "YGAS,001,500.0,1.0,101.1,1.1,0.1,0.1,0.2,0.2,1000,1001,1002,25.0,25.1,101.3\r\n",
        "YGAS,001,501.0,1.0,101.2,1.1,0.1,0.1,0.2,0.2,1000,1001,1002,25.0,25.1,101.3\r\n",
        "YGAS,001,502.0,1.0,101.3,1.1,0.1,0.1,0.2,0.2,1000,1001,1002,25.0,25.1,101.3\r\n",
    ]
    runner = _runner(tmp_path, strict=True, chunks=chunks)
    runner._sensor_precheck()

    ga = runner.devices["gas_analyzer"]
    assert ("mode", 2, False) in ga.calls
    assert ("comm_way", False, False) in ga.calls
    assert ("comm_way", True, False) in ga.calls
    assert ("ftd", 1) in ga.calls
    assert ("avg_filter", 49, False) in ga.calls
    assert not any(call[0] == "avg" for call in ga.calls)
    runner.logger.close()


def test_sensor_precheck_strict_raises_when_invalid(tmp_path: Path) -> None:
    chunks = ["bad\r\n", "also_bad\r\n"]
    runner = _runner(tmp_path, strict=True, chunks=chunks)

    raised = False
    try:
        runner._sensor_precheck()
    except RuntimeError:
        raised = True

    runner.logger.close()
    assert raised is True
