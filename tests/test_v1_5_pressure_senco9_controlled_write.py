import csv
import json

from gas_calibrator.senco_format import rounded_senco_values
from gas_calibrator.tools import run_v1_5_pressure_senco9_controlled_write as writer


def _write_json(path, payload):
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _write_csv(path, rows):
    header = []
    for row in rows:
        for key in row:
            if key not in header:
                header.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        out = csv.DictWriter(handle, fieldnames=header)
        out.writeheader()
        out.writerows(rows)


def _read_csv(path):
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _config(tmp_path):
    return {
        "devices": {
            "gas_analyzers": [
                {
                    "name": "ga01",
                    "enabled": True,
                    "port": "COM35",
                    "baud": 115200,
                    "device_id": "023",
                    "mode": 2,
                    "active_send": True,
                    "ftd_hz": 1,
                    "average_filter": 49,
                },
                {
                    "name": "ga02",
                    "enabled": True,
                    "port": "COM36",
                    "baud": 115200,
                    "device_id": "030",
                    "mode": 2,
                    "active_send": True,
                    "ftd_hz": 1,
                    "average_filter": 49,
                },
            ],
            "relay": {"enabled": False},
            "relay_8": {"enabled": False},
            "humidity_generator": {"enabled": False},
            "dewpoint_meter": {"enabled": False},
        },
        "paths": {"output_dir": str(tmp_path / "logs")},
    }


def _fit_rows():
    return [
        {
            "analyzer_prefix": "ga01",
            "analyzer_device_id": "023",
            "status": "pass",
            "recommendation": "review_senco9_offset_candidate_no_write",
            "valid_pair_count": "96",
            "distinct_pressure_points": "8",
            "reference_span_hpa": "599.455",
            "offset_only_offset_kpa": "0.704736",
            "offset_only_residual_max_abs_hpa": "0.302",
            "linear_slope_bias": "-0.0004",
            "write_allowed": "False",
        },
        {
            "analyzer_prefix": "ga02",
            "analyzer_device_id": "030",
            "status": "pass",
            "recommendation": "review_senco9_offset_candidate_no_write",
            "valid_pair_count": "96",
            "distinct_pressure_points": "8",
            "reference_span_hpa": "599.455",
            "offset_only_offset_kpa": "-0.218805",
            "offset_only_residual_max_abs_hpa": "0.277",
            "linear_slope_bias": "-0.0005",
            "write_allowed": "False",
        },
    ]


class _FakeGasAnalyzer:
    instances = {}

    def __init__(self, port, baudrate=115200, timeout=1.0, device_id="000", **_kwargs):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.device_id = device_id
        self.mode = 2
        self.active = True
        self.ftd = 1
        self.average = 49
        self.coeff9 = [1.0, 1.0, 0.0, 0.0]
        self.calls = []
        _FakeGasAnalyzer.instances[port] = self

    def open(self):
        self.calls.append(("open",))

    def close(self):
        self.calls.append(("close",))

    def read_current_mode_snapshot(self, *args, **kwargs):
        self.calls.append(("snapshot", args, kwargs))
        return {"id": self.device_id, "mode": self.mode, "raw": f"YGAS,{self.device_id},..."}

    def set_mode_with_ack(self, mode, require_ack=True):
        self.calls.append(("mode", mode, require_ack))
        self.mode = int(mode)
        return True

    def set_senco(self, group, *coeffs):
        self.calls.append(("senco", group, list(coeffs)))
        assert int(group) == 9
        self.coeff9 = list(rounded_senco_values(coeffs))
        return True

    def read_coefficient_group(self, group, **_kwargs):
        self.calls.append(("getco", group))
        assert int(group) == 9
        return {f"C{idx}": value for idx, value in enumerate(self.coeff9)}

    def set_comm_way_with_ack(self, active, require_ack=True):
        self.calls.append(("comm", active, require_ack))
        self.active = bool(active)
        return True

    def set_active_freq_with_ack(self, hz, require_ack=True):
        self.calls.append(("ftd", hz, require_ack))
        self.ftd = int(hz)
        return True

    def set_average_filter_with_ack(self, window_n, require_ack=True):
        self.calls.append(("avg", window_n, require_ack))
        self.average = int(window_n)
        return True


def test_controlled_senco9_write_requires_explicit_unlock(tmp_path):
    cfg_path = tmp_path / "cfg.json"
    fit_dir = tmp_path / "fit"
    fit_dir.mkdir()
    _write_json(cfg_path, _config(tmp_path))
    _write_csv(fit_dir / "pressure_fit_summary.csv", _fit_rows())

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--fit-dir",
            str(fit_dir),
            "--output-dir",
            str(tmp_path / "out"),
            "--write-all-supported",
            "--reviewer",
            "reviewer-a",
            "--approver",
            "approver-b",
            "--pre-device-cooldown-s",
            "0",
            "--inter-device-delay-s",
            "0",
            "--restore-command-gap-s",
            "1",
        ]
    )

    assert rc == 2


def test_controlled_senco9_write_all_supported_is_sequential_and_restores_runtime(monkeypatch, tmp_path):
    _FakeGasAnalyzer.instances = {}
    monkeypatch.setattr(writer, "GasAnalyzer", _FakeGasAnalyzer)
    cfg_path = tmp_path / "cfg.json"
    fit_dir = tmp_path / "fit"
    out_dir = tmp_path / "out"
    fit_dir.mkdir()
    _write_json(cfg_path, _config(tmp_path))
    _write_csv(fit_dir / "pressure_fit_summary.csv", _fit_rows())

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--fit-dir",
            str(fit_dir),
            "--output-dir",
            str(out_dir),
            "--write-all-supported",
            "--enable-senco9-write",
            "--operator-confirmation",
            writer.CONFIRMATION_TEXT,
            "--reviewer",
            "reviewer-a",
            "--approver",
            "approver-b",
        ]
    )

    assert rc == 0
    rows = _read_csv(out_dir / "senco9_write_summary.csv")
    assert [row["analyzer_device_id"] for row in rows] == ["023", "030"]
    assert {row["status"] for row in rows} == {"written_readback_verified"}
    assert {row["controls_water_or_gas_routes"] for row in rows} == {"False"}
    assert {row["writes_device_id"] for row in rows} == {"False"}
    assert {row["writes_senco9"] for row in rows} == {"True"}
    assert (out_dir / "old_getco9_snapshot.json").exists()
    ga01 = _FakeGasAnalyzer.instances["COM35"]
    assert ga01.COEFFICIENT_COMM_QUIET_DELAY_S == 3.0
    assert ga01.COEFFICIENT_READ_TIMEOUT_S == 1.2
    assert ga01.coeff9 == [1.70474, 1.0, 0.0, 0.0]
    assert rows[0]["old_senco9_c0"] == "1.0"
    assert rows[0]["target_senco9_c0"] == "1.704736"
    assert rows[0]["candidate_offset_mode"] == "add-to-current-c0"
    detail_rows = _read_csv(out_dir / "senco9_write_detail.csv")
    assert json.loads(detail_rows[0]["coeff_target_json"]) == [1.704736, 1.0, 0.0, 0.0]
    assert ("ftd", 1, False) not in ga01.calls
    assert ("avg", 49, False) in ga01.calls
    assert ("comm", True, False) in ga01.calls
    assert rows[0]["active_freq_restore_status"] == "skipped"


def test_controlled_senco9_write_can_restore_active_freq_when_explicitly_requested(monkeypatch, tmp_path):
    _FakeGasAnalyzer.instances = {}
    monkeypatch.setattr(writer, "GasAnalyzer", _FakeGasAnalyzer)
    cfg_path = tmp_path / "cfg.json"
    fit_dir = tmp_path / "fit"
    out_dir = tmp_path / "out"
    fit_dir.mkdir()
    _write_json(cfg_path, _config(tmp_path))
    _write_csv(fit_dir / "pressure_fit_summary.csv", _fit_rows()[:1])

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--fit-dir",
            str(fit_dir),
            "--output-dir",
            str(out_dir),
            "--device-id",
            "023",
            "--enable-senco9-write",
            "--operator-confirmation",
            writer.CONFIRMATION_TEXT,
            "--reviewer",
            "reviewer-a",
            "--approver",
            "approver-b",
            "--restore-active-freq",
        ]
    )

    assert rc == 0
    rows = _read_csv(out_dir / "senco9_write_summary.csv")
    ga01 = _FakeGasAnalyzer.instances["COM35"]
    assert ("ftd", 1, False) in ga01.calls
    assert rows[0]["active_freq_restore_status"] == "restored"
