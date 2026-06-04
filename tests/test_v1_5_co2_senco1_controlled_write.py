import csv
import json

from gas_calibrator.senco_format import rounded_senco_values
from gas_calibrator.tools import run_v1_5_co2_senco1_controlled_write as writer


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


def _mapping_rows():
    return [
        {
            "component": "co2",
            "analyzer_prefix": "ga01",
            "analyzer_device_id": "023",
            "primary_senco": "SENCO1",
            "secondary_senco": "SENCO3",
            "candidate_terms": "intercept;R;R2;R3",
            "candidate_terms_complete": "True",
            "primary_candidate_values": "2.16269e04,-2.82422e04,1.04379e04,-8.05201e02,0.00000e00,0.00000e00",
            "primary_command_preview": "SENCO1,YGAS,FFF,2.16269e04,-2.82422e04,1.04379e04,-8.05201e02,0.00000e00,0.00000e00",
            "secondary_action": "preserve_existing_requires_old_snapshot_and_manual_mapping_review",
            "secondary_command_preview": "",
            "old_primary_snapshot": json.dumps([-23155.8, 57388.1, -48596.0, 11306.9, 0.0, 0.0]),
            "old_secondary_snapshot": json.dumps([35.9504, -0.108469, 16.6812, -36.728, 0.0734459, 0.0]),
            "old_snapshot_status": "primary_and_secondary_bound",
            "old_primary_snapshot_complete": "True",
            "old_secondary_snapshot_complete": "True",
            "mapping_status": "review_only_primary_preview_ready",
            "write_allowed": "False",
            "reason": "candidate model supplies primary four ratio-polynomial terms only",
        },
        {
            "component": "co2",
            "analyzer_prefix": "ga02",
            "analyzer_device_id": "030",
            "primary_senco": "SENCO1",
            "secondary_senco": "SENCO3",
            "candidate_terms": "intercept;R;R2;R3",
            "candidate_terms_complete": "True",
            "primary_candidate_values": "1.07677e04,-4.80957e03,-6.81155e03,3.48128e03,0.00000e00,0.00000e00",
            "primary_command_preview": "",
            "secondary_action": "preserve_existing_requires_old_snapshot_and_manual_mapping_review",
            "secondary_command_preview": "",
            "old_primary_snapshot": json.dumps([-219925.0, 432559.0, -309758.0, 73928.0, 0.0, 0.0]),
            "old_secondary_snapshot": json.dumps([182.551, -0.246991, -30.4072, -20.4466, 0.0294764, 0.0]),
            "old_snapshot_status": "primary_and_secondary_bound",
            "old_primary_snapshot_complete": "True",
            "old_secondary_snapshot_complete": "True",
            "mapping_status": "review_only_primary_preview_ready",
            "write_allowed": "False",
            "reason": "candidate model supplies primary four ratio-polynomial terms only",
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
        if device_id == "023":
            self.coeff1 = [-23155.8, 57388.1, -48596.0, 11306.9, 0.0, 0.0]
            self.coeff3 = [35.9504, -0.108469, 16.6812, -36.728, 0.0734459, 0.0]
        else:
            self.coeff1 = [-219925.0, 432559.0, -309758.0, 73928.0, 0.0, 0.0]
            self.coeff3 = [182.551, -0.246991, -30.4072, -20.4466, 0.0294764, 0.0]
        self.just_wrote_senco1 = False
        self.short_senco3_once_after_write = device_id == "023"
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
        self.calls.append(("senco", int(group), list(coeffs)))
        assert int(group) == 1
        self.coeff1 = list(rounded_senco_values(coeffs))
        self.just_wrote_senco1 = True
        return True

    def read_coefficient_group(self, group, **_kwargs):
        self.calls.append(("getco", int(group)))
        if int(group) == 1:
            return {f"C{idx}": value for idx, value in enumerate(self.coeff1)}
        if int(group) == 3:
            if self.just_wrote_senco1 and self.short_senco3_once_after_write:
                self.short_senco3_once_after_write = False
                return {f"C{idx}": value for idx, value in enumerate(self.coeff3[:2])}
            self.just_wrote_senco1 = False
            return {f"C{idx}": value for idx, value in enumerate(self.coeff3)}
        raise AssertionError(f"unexpected GETCO group {group}")

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


def test_controlled_co2_senco1_write_requires_explicit_unlock(tmp_path):
    cfg_path = tmp_path / "cfg.json"
    review_dir = tmp_path / "review"
    review_dir.mkdir()
    _write_json(cfg_path, _config(tmp_path))
    _write_csv(review_dir / "candidate_senco_mapping_review.csv", _mapping_rows())

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--review-dir",
            str(review_dir),
            "--output-dir",
            str(tmp_path / "out"),
            "--write-all-ready",
            "--reviewer",
            "reviewer-a",
            "--approver",
            "approver-b",
            "--pre-device-cooldown-s",
            "0",
            "--inter-device-delay-s",
            "0",
            "--restore-command-gap-s",
            "0",
        ]
    )

    assert rc == 2


def test_target_payload_matches_legacy_group_length():
    candidate = [27959.3, -44625.5, 22557.1, -3533.08, 0.0, 0.0]
    assert writer._target_payload_values(candidate, [36965.6, -49221.5, 16277.6, 0.0]) == [
        27959.3,
        -44625.5,
        22557.1,
        -3533.08,
    ]
    assert writer._target_payload_values(candidate, [1, 2, 3, 4, 5, 6]) == candidate


def test_controlled_co2_senco1_write_preserves_senco3_and_restores_runtime(monkeypatch, tmp_path):
    _FakeGasAnalyzer.instances = {}
    monkeypatch.setattr(writer, "GasAnalyzer", _FakeGasAnalyzer)
    cfg_path = tmp_path / "cfg.json"
    review_dir = tmp_path / "review"
    out_dir = tmp_path / "out"
    review_dir.mkdir()
    _write_json(cfg_path, _config(tmp_path))
    _write_csv(review_dir / "candidate_senco_mapping_review.csv", _mapping_rows())

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--review-dir",
            str(review_dir),
            "--output-dir",
            str(out_dir),
            "--write-all-ready",
            "--enable-senco1-write",
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
            "0",
        ]
    )

    assert rc == 0
    rows = _read_csv(out_dir / "co2_senco1_write_summary.csv")
    assert [row["analyzer_device_id"] for row in rows] == ["023", "030"]
    assert {row["status"] for row in rows} == {"written_readback_verified"}
    assert {row["writes_senco1"] for row in rows} == {"True"}
    assert {row["writes_senco3"] for row in rows} == {"False"}
    assert {row["clears_senco"] for row in rows} == {"False"}
    assert {row["controls_water_or_gas_routes"] for row in rows} == {"False"}
    assert {row["writes_device_id"] for row in rows} == {"False"}
    assert {row["senco3_preserve_status"] for row in rows} == {"preserved"}
    assert (out_dir / "old_getco1_getco3_snapshot.json").exists()
    ga01 = _FakeGasAnalyzer.instances["COM35"]
    assert ga01.COEFFICIENT_COMM_QUIET_DELAY_S == 3.0
    assert ga01.COEFFICIENT_READ_TIMEOUT_S == 1.5
    assert ga01.coeff1 == [21626.9, -28242.2, 10437.9, -805.201, 0.0, 0.0]
    assert ga01.coeff3 == [35.9504, -0.108469, 16.6812, -36.728, 0.0734459, 0.0]
    assert ("senco", 3, ga01.coeff3) not in ga01.calls
    assert ("ftd", 1, False) in ga01.calls
    assert ("avg", 49, False) in ga01.calls
    assert ("comm", True, False) in ga01.calls
    assert rows[0]["active_freq_restore_status"] == "restored"
