import csv
import json

import pytest

from gas_calibrator.tools import run_v1_5_co2_senco5_linear_controlled_write as writer


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


def _precheck_dir(tmp_path):
    root = tmp_path / "main_senco_precheck"
    root.mkdir(exist_ok=True)
    _write_csv(
        root / "candidate_write_review_checks.csv",
        [
            {"check": "fit_input_traceability_required_before_final_senco_review", "status": "pass"},
            {"check": "fit_input_traceability_bound:co2:022", "status": "pass"},
        ],
    )
    _write_csv(
        root / "main_senco_write_precheck_summary.csv",
        [
            {
                "analyzer_device_id": "022",
                "co2_fit_input_traceability_status": "pass",
                "co2_fit_input_traceability_blockers": "",
            }
        ],
    )
    _write_json(
        root / "main_senco_write_precheck_meta.json",
        {
            "no_write": True,
            "opens_com": False,
            "writes_senco": False,
            "controls_routes": False,
            "fit_input_traceability_required": True,
            "fit_input_traceability_status": "pass",
        },
    )
    return root


def _config(tmp_path):
    return {
        "devices": {
            "gas_analyzers": [
                {
                    "name": "ga022",
                    "enabled": True,
                    "port": "COM37",
                    "baud": 115200,
                    "device_id": "022",
                    "mode": 2,
                    "active_send": True,
                    "ftd_hz": 1,
                    "average_filter": 49,
                }
            ]
        },
        "paths": {"output_dir": str(tmp_path / "logs")},
    }


def _candidates(path):
    _write_csv(
        path,
        [
            {
                "device_id": "022",
                "senco_group": "SENCO5",
                "C0": "-30.338995500838564",
                "C1": "1.0025265692492853",
                "candidate_status": "review_ready",
            },
            {
                "device_id": "033",
                "senco_group": "SENCO5",
                "C0": "-46.26297326210659",
                "C1": "1.03277191805972",
                "candidate_status": "blocked",
            },
        ],
    )


class _FakeGasAnalyzer:
    instances = {}
    COMMAND_TARGET_ID = "FFF"
    CONFIG_ACK_RETRY_COUNT = 0
    CONFIG_ACK_RETRY_DELAY_S = 0.0

    @staticmethod
    def _split_stream_lines(raw):
        return [str(raw).strip()] if str(raw).strip() else []

    @staticmethod
    def _is_success_ack(line):
        return str(line).strip().upper() in {"YGAS,T", "OK", "T"}

    def __init__(self, port, baudrate=115200, timeout=1.0, device_id="000", **_kwargs):
        self.port = port
        self.device_id = device_id
        self.mode = 2
        self.coeff5 = [0.0, 1.0]
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
        self.calls.append(("mode", int(mode), require_ack))
        self.mode = int(mode)
        return True

    def read_coefficient_group(self, group, **_kwargs):
        self.calls.append(("getco", int(group)))
        assert int(group) == 5
        return {"C0": self.coeff5[0], "C1": self.coeff5[1]}

    def _prepare_coefficient_io(self):
        self.calls.append(("prepare",))

    def _send_config_with_retries(self, payload, **_kwargs):
        self.calls.append(("send", payload))
        parts = payload.split(",")
        assert parts[:3] == ["SENCO5", "YGAS", "FFF"]
        assert parts[3:] == ["-30.339", "1.003"]
        self.coeff5 = [float(parts[3]), float(parts[4])]
        return True

    def set_comm_way_with_ack(self, active, require_ack=True):
        self.calls.append(("comm", active, require_ack))
        return True

    def set_active_freq_with_ack(self, hz, require_ack=True):
        self.calls.append(("ftd", int(hz), require_ack))
        return True

    def set_average_filter_with_ack(self, window_n, require_ack=True):
        self.calls.append(("avg", int(window_n), require_ack))
        return True


def test_senco5_linear_writer_writes_decimal_payload_and_readback(monkeypatch, tmp_path):
    _FakeGasAnalyzer.instances = {}
    monkeypatch.setattr(writer, "GasAnalyzer", _FakeGasAnalyzer)
    cfg = tmp_path / "cfg.json"
    candidates = tmp_path / "candidates.csv"
    out = tmp_path / "out"
    _write_json(cfg, _config(tmp_path))
    _candidates(candidates)
    precheck = _precheck_dir(tmp_path)

    rc = writer.main(
        [
            "--config",
            str(cfg),
            "--candidate-coefficients-csv",
            str(candidates),
            "--main-senco-precheck-dir",
            str(precheck),
            "--output-dir",
            str(out),
            "--device-id",
            "022",
            "--enable-senco5-write",
            "--operator-confirmation",
            writer.CONFIRMATION_TEXT,
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
            "--post-write-settle-s",
            "1",
        ]
    )

    assert rc == 0
    rows = _read_csv(out / "senco5_linear_write_events.csv")
    assert rows[0]["status"] == "written_readback_verified"
    assert rows[0]["payload"] == "SENCO5,YGAS,FFF,-30.339,1.003"
    assert rows[0]["controls_water_or_gas_routes"] == "False"
    assert rows[0]["writes_senco5"] == "True"
    assert rows[0]["clears_senco"] == "False"
    ga = _FakeGasAnalyzer.instances["COM37"]
    assert ga.coeff5 == [-30.339, 1.003]
    assert (
        "send",
        "SENCO5,YGAS,FFF,-30.339,1.003",
    ) in ga.calls


def test_senco5_linear_writer_rejects_more_than_three_decimals():
    with pytest.raises(ValueError, match="at most 3 decimal"):
        writer._format_c0("-0.1234", decimals=4)


def test_senco5_linear_writer_accepts_missing_ack_when_readback_matches(monkeypatch, tmp_path):
    _FakeGasAnalyzer.instances = {}
    monkeypatch.setattr(writer, "GasAnalyzer", _FakeGasAnalyzer)

    def _write_without_ack(ga, **_kwargs):
        ga.coeff5 = [-30.339, 1.003]
        return False, "SENCO5,YGAS,FFF,-30.339,1.003", []

    monkeypatch.setattr(writer, "_write_senco5", _write_without_ack)
    cfg = tmp_path / "cfg.json"
    candidates = tmp_path / "candidates.csv"
    out = tmp_path / "out"
    _write_json(cfg, _config(tmp_path))
    _candidates(candidates)
    precheck = _precheck_dir(tmp_path)

    rc = writer.main(
        [
            "--config",
            str(cfg),
            "--candidate-coefficients-csv",
            str(candidates),
            "--main-senco-precheck-dir",
            str(precheck),
            "--output-dir",
            str(out),
            "--device-id",
            "022",
            "--enable-senco5-write",
            "--operator-confirmation",
            writer.CONFIRMATION_TEXT,
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
            "--post-write-settle-s",
            "1",
        ]
    )

    assert rc == 0
    rows = _read_csv(out / "senco5_linear_write_events.csv")
    assert rows[0]["status"] == "written_readback_verified_ack_missing"
    assert rows[0]["writes_senco5"] == "True"
    assert rows[0]["write_applied"] == "True"
    assert "ACK_MISSING_BUT_READBACK_MATCHED" in rows[0]["reason"]


def test_senco5_linear_writer_refuses_without_unlock(tmp_path):
    cfg = tmp_path / "cfg.json"
    candidates = tmp_path / "candidates.csv"
    _write_json(cfg, _config(tmp_path))
    _candidates(candidates)

    rc = writer.main(
        [
            "--config",
            str(cfg),
            "--candidate-coefficients-csv",
            str(candidates),
            "--output-dir",
            str(tmp_path / "out"),
            "--device-id",
            "022",
            "--reviewer",
            "reviewer-a",
            "--approver",
            "approver-b",
        ]
    )

    assert rc == 2


def test_senco5_linear_writer_refuses_missing_fit_input_precheck_before_open(monkeypatch, tmp_path):
    _FakeGasAnalyzer.instances = {}
    monkeypatch.setattr(writer, "GasAnalyzer", _FakeGasAnalyzer)
    cfg = tmp_path / "cfg.json"
    candidates = tmp_path / "candidates.csv"
    _write_json(cfg, _config(tmp_path))
    _candidates(candidates)

    rc = writer.main(
        [
            "--config",
            str(cfg),
            "--candidate-coefficients-csv",
            str(candidates),
            "--output-dir",
            str(tmp_path / "out"),
            "--device-id",
            "022",
            "--enable-senco5-write",
            "--operator-confirmation",
            writer.CONFIRMATION_TEXT,
            "--reviewer",
            "reviewer-a",
            "--approver",
            "approver-b",
        ]
    )

    assert rc == 2
    assert _FakeGasAnalyzer.instances == {}
