import csv
import json

from gas_calibrator.tools import run_v1_5_temperature_current_point_review as tool


def _write_json(path, payload):
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _config(tmp_path):
    return {
        "devices": {
            "gas_analyzers": [
                {
                    "name": "ga02",
                    "enabled": True,
                    "port": "COM37",
                    "baud": 115200,
                    "device_id": "002",
                    "mode": 2,
                    "active_send": True,
                    "ftd_hz": 1,
                    "average_filter": 49,
                }
            ],
            "thermometer": {"enabled": True, "port": "COM50", "baud": 9600},
        }
    }


def _rows(path):
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


class _FakeThermometer:
    def __init__(self, *_args, **_kwargs):
        self.calls = []

    def open(self):
        self.calls.append("open")

    def close(self):
        self.calls.append("close")

    def read_current(self):
        return {"temp_c": 20.0, "raw": "20.0"}


class _FakeGasAnalyzer:
    instances = {}

    def __init__(self, port, baudrate=115200, timeout=1.0, device_id="000", **_kwargs):
        self.port = port
        self.device_id = device_id
        self.raw_chamber = 18.0
        self.raw_case = 20.0
        self.coeffs = {
            7: [60.0, 0.0, 0.0, 0.0],
            8: [0.0, 1.0, 0.0, 0.0],
        }
        self.calls = []
        _FakeGasAnalyzer.instances[device_id] = self

    def open(self):
        self.calls.append(("open",))

    def close(self):
        self.calls.append(("close",))

    def read_coefficient_group(self, group):
        self.calls.append(("getco", int(group)))
        return {f"C{idx}": value for idx, value in enumerate(self.coeffs[int(group)])}

    def read_latest_data(self, **kwargs):
        self.calls.append(("read_latest_data", kwargs))
        return "YGAS,002,frame"

    def parse_line(self, _line):
        def poly(values, raw):
            c0, c1, c2, c3 = values
            return c0 + c1 * raw + c2 * raw * raw + c3 * raw * raw * raw

        return {
            "id": self.device_id,
            "chamber_temp_c": poly(self.coeffs[7], self.raw_chamber),
            "case_temp_c": poly(self.coeffs[8], self.raw_case),
            "raw": "YGAS,002,frame",
        }

    def _send_config_with_retries(self, payload, **kwargs):
        self.calls.append(("send", payload, kwargs))
        head, *parts = payload.split(",")
        if head not in {"SENCO7", "SENCO8"}:
            raise AssertionError(payload)
        group = 7 if head == "SENCO7" else 8
        self.coeffs[group] = [float(part) for part in parts[-4:]]
        return True


class _NeutralFakeGasAnalyzer(_FakeGasAnalyzer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.raw_chamber = 20.0
        self.raw_case = 20.0
        self.coeffs = {
            7: [0.0, 1.0, 0.0, 0.0],
            8: [0.0, 1.0, 0.0, 0.0],
        }


class _WarmNeutralFakeGasAnalyzer(_NeutralFakeGasAnalyzer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.raw_chamber = 24.0
        self.raw_case = 24.5


def test_current_temperature_review_blocks_subzero_sixty_projection(monkeypatch, tmp_path):
    monkeypatch.setattr(tool, "GasAnalyzer", _FakeGasAnalyzer)
    monkeypatch.setattr(tool, "Thermometer", _FakeThermometer)
    monkeypatch.setattr(tool.time, "sleep", lambda _seconds: None)
    _FakeGasAnalyzer.instances.clear()
    cfg_path = tmp_path / "cfg.json"
    out_dir = tmp_path / "out"
    _write_json(cfg_path, _config(tmp_path))

    rc = tool.main(
        [
            "--config",
            str(cfg_path),
            "--output-dir",
            str(out_dir),
            "--device-id",
            "002",
            "--sample-count",
            "1",
            "--sample-interval-s",
            "0",
            "--frame-drain-s",
            "0",
        ]
    )

    assert rc == 1
    review = _rows(out_dir / "temperature_current_point_review.csv")
    group7 = next(row for row in review if row["senco_group"] == "SENCO7")
    assert group7["status"] == "repair_required"
    assert "hard_bad" in group7["reason"]
    projections = _rows(out_dir / "temperature_senco78_projection_check.csv")
    assert any(row["senco_group"] == "SENCO7" and row["hard_bad_projection"] == "True" for row in projections)


def test_current_temperature_review_uses_epoch0_snapshot_without_duplicate_getco(monkeypatch, tmp_path):
    monkeypatch.setattr(tool, "GasAnalyzer", _NeutralFakeGasAnalyzer)
    monkeypatch.setattr(tool, "Thermometer", _FakeThermometer)
    monkeypatch.setattr(tool.time, "sleep", lambda _seconds: None)
    _FakeGasAnalyzer.instances.clear()
    cfg_path = tmp_path / "cfg.json"
    snapshot_path = tmp_path / "old_component_coefficients_snapshot.json"
    out_dir = tmp_path / "out"
    _write_json(cfg_path, _config(tmp_path))
    _write_json(
        snapshot_path,
        {
            "002": {
                "GETCO7_before": [0.0, 1.0, 0.0, 0.0],
                "GETCO8_before": [0.0, 1.0, 0.0, 0.0],
            }
        },
    )

    rc = tool.main(
        [
            "--config",
            str(cfg_path),
            "--output-dir",
            str(out_dir),
            "--device-id",
            "002",
            "--getco-snapshot-json",
            str(snapshot_path),
            "--sample-count",
            "1",
            "--sample-interval-s",
            "0",
            "--frame-drain-s",
            "0",
        ]
    )

    assert rc == 0
    instance = _FakeGasAnalyzer.instances["002"]
    assert not any(call[0] == "getco" for call in instance.calls)
    review = _rows(out_dir / "temperature_current_point_review.csv")
    assert {row["coefficient_source"] for row in review} == {"epoch0_getco_snapshot"}


def test_current_temperature_common_offset_requires_reference_equivalence_not_repair(monkeypatch, tmp_path):
    monkeypatch.setattr(tool, "GasAnalyzer", _WarmNeutralFakeGasAnalyzer)
    monkeypatch.setattr(tool, "Thermometer", _FakeThermometer)
    monkeypatch.setattr(tool.time, "sleep", lambda _seconds: None)
    _FakeGasAnalyzer.instances.clear()
    cfg_path = tmp_path / "cfg.json"
    out_dir = tmp_path / "out"
    _write_json(cfg_path, _config(tmp_path))

    rc = tool.main(
        [
            "--config",
            str(cfg_path),
            "--output-dir",
            str(out_dir),
            "--device-id",
            "002",
            "--sample-count",
            "1",
            "--sample-interval-s",
            "0",
            "--frame-drain-s",
            "0",
            "--enable-single-point-repair",
            "--operator-confirmation",
            tool.CONFIRMATION_TEXT,
            "--reviewer",
            "reviewer",
            "--approver",
            "approver",
            "--post-write-settle-s",
            "1",
            "--readback-retry-delay-s",
            "1",
            "--restore-command-gap-s",
            "1",
            "--coefficient-read-delay-s",
            "1",
        ]
    )

    assert rc == 1
    instance = _FakeGasAnalyzer.instances["002"]
    assert not any(call[0] == "send" for call in instance.calls)
    review = _rows(out_dir / "temperature_current_point_review.csv")
    assert {row["status"] for row in review} == {"reference_equivalence_required"}
    assert all(
        "temperature_reference_not_equivalent_to_analyzer_thermal_state" in row["reason"]
        for row in review
    )


def test_current_temperature_repair_neutralizes_before_single_point_offset(monkeypatch, tmp_path):
    monkeypatch.setattr(tool, "GasAnalyzer", _FakeGasAnalyzer)
    monkeypatch.setattr(tool, "Thermometer", _FakeThermometer)
    monkeypatch.setattr(tool.time, "sleep", lambda _seconds: None)
    _FakeGasAnalyzer.instances.clear()
    cfg_path = tmp_path / "cfg.json"
    out_dir = tmp_path / "out"
    _write_json(cfg_path, _config(tmp_path))

    rc = tool.main(
        [
            "--config",
            str(cfg_path),
            "--output-dir",
            str(out_dir),
            "--device-id",
            "002",
            "--sample-count",
            "1",
            "--sample-interval-s",
            "0",
            "--frame-drain-s",
            "0",
            "--enable-single-point-repair",
            "--operator-confirmation",
            tool.CONFIRMATION_TEXT,
            "--reviewer",
            "reviewer",
            "--approver",
            "approver",
            "--post-write-settle-s",
            "1",
            "--readback-retry-delay-s",
            "1",
            "--restore-command-gap-s",
            "1",
            "--coefficient-read-delay-s",
            "1",
        ]
    )

    assert rc == 0
    instance = _FakeGasAnalyzer.instances["002"]
    sent = [call[1] for call in instance.calls if call[0] == "send"]
    assert sent[0] == "SENCO7,YGAS,FFF,0.00000e00,1.00000e00,0.00000e00,0.00000e00"
    assert sent[1] == "SENCO7,YGAS,FFF,2.00000e00,1.00000e00,0.00000e00,0.00000e00"
    review = _rows(out_dir / "temperature_current_point_review.csv")
    group7 = next(row for row in review if row["senco_group"] == "SENCO7")
    assert group7["status"] == "single_point_repair_written"
    assert group7["neutral_current_analyzer_temp_c"] == "18.0"
    assert group7["post_repair_analyzer_temp_c"] == "20.0"
