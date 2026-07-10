import csv
import json

from gas_calibrator.storage.v1_5_evidence.bundle import _build_sidecar_write_events, _load_database_sidecar_rows
from gas_calibrator.senco_format import rounded_senco_values
from gas_calibrator.tools import run_v1_5_co2_senco13_controlled_rollback as rollback
from gas_calibrator.tools import run_v1_5_co2_senco13_controlled_write as writer


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


def _write_formula_contract_pass(review_dir):
    _write_csv(
        review_dir / "candidate_write_review_checks.csv",
        [
            {
                "check": writer.FORMULA_CONTRACT_CHECK,
                "status": "pass",
                "meaning": "unit test supplies a confirmed offline firmware formula contract",
                "evidence": "test_only",
            },
            {
                "check": writer.SENCO5_CONTRACT_CHECK,
                "status": "pass",
                "meaning": "unit test supplies a reviewed CO2 SENCO5/SENCO6 linear-correction preserve decision",
                "evidence": "test_only",
            },
            {
                "check": "fit_input_traceability_required_before_final_senco_review",
                "status": "pass",
                "meaning": "unit test supplies the final fit-input traceability gate",
                "evidence": "test_only",
            },
            {"check": "fit_input_traceability_bound:co2:030", "status": "pass"},
            {"check": "fit_input_traceability_bound:co2:022", "status": "pass"},
        ],
    )
    _write_csv(
        review_dir / "main_senco_write_precheck_summary.csv",
        [
            {
                "analyzer_device_id": device_id,
                "co2_fit_input_traceability_status": "pass",
                "co2_fit_input_traceability_blockers": "",
            }
            for device_id in ("030", "022")
        ],
    )
    _write_json(
        review_dir / "main_senco_write_precheck_meta.json",
        {
            "no_write": True,
            "writes_senco": False,
            "fit_input_traceability_required": True,
            "fit_input_traceability_status": "pass",
        },
    )


def _config(tmp_path):
    return {
        "devices": {
            "gas_analyzers": [
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
                {
                    "name": "ga03",
                    "enabled": True,
                    "port": "COM37",
                    "baud": 115200,
                    "device_id": "022",
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
            "analyzer_prefix": "ga02",
            "analyzer_device_id": "030",
            "primary_senco": "SENCO1",
            "secondary_senco": "SENCO3",
            "candidate_terms": "intercept;R;R2;R3;T;T2;RT",
            "candidate_terms_complete": "True",
            "secondary_candidate_terms_complete": "True",
            "primary_candidate_values": "1.48815e04,-2.56104e04,1.34585e04,-1.52112e03,0.00000e00,0.00000e00",
            "primary_command_preview": "SENCO1,YGAS,FFF,1.48815e04,-2.56104e04,1.34585e04,-1.52112e03,0.00000e00,0.00000e00",
            "secondary_candidate_values": "2.06216e01,2.35468e-02,-2.21346e01,0.00000e00,0.00000e00,0.00000e00",
            "secondary_action": "paired_write_preview_temperature_terms_pressure_slots_zero",
            "secondary_command_preview": "SENCO3,YGAS,FFF,2.06216e01,2.35468e-02,-2.21346e01,0.00000e00,0.00000e00,0.00000e00",
            "old_primary_snapshot": json.dumps([10767.7, -4809.57, -6811.55, 3481.28, 0.0, 0.0]),
            "old_secondary_snapshot": json.dumps([182.551, -0.246991, -30.4072, -20.4466, 0.0294764, 0.0]),
            "old_snapshot_status": "primary_and_secondary_bound",
            "old_primary_snapshot_complete": "True",
            "old_secondary_snapshot_complete": "True",
            "mapping_status": "review_only_primary_secondary_preview_ready",
            "write_allowed": "False",
            "reason": "candidate model supplies primary and secondary terms",
        },
        {
            "component": "co2",
            "analyzer_prefix": "ga03",
            "analyzer_device_id": "022",
            "primary_senco": "SENCO1",
            "secondary_senco": "SENCO3",
            "candidate_terms": "intercept;R;R2;R3;T;T2;RT",
            "candidate_terms_complete": "True",
            "secondary_candidate_terms_complete": "True",
            "primary_candidate_values": "5.16789e04,-9.65318e04,6.02734e04,-1.25578e04,0.00000e00,0.00000e00",
            "secondary_candidate_values": "6.14871e00,4.62540e-03,-5.60581e00,0.00000e00,0.00000e00,0.00000e00",
            "old_primary_snapshot": "",
            "old_secondary_snapshot": "",
            "old_snapshot_status": "partial_or_missing",
            "old_primary_snapshot_complete": "False",
            "old_secondary_snapshot_complete": "False",
            "mapping_status": "review_only_primary_secondary_preview_ready",
            "write_allowed": "False",
            "reason": "old snapshot missing",
        },
    ]


def test_supported_pair_rows_refuse_four_value_old_snapshots():
    rows = _mapping_rows()
    four_value_row = dict(rows[0])
    four_value_row["analyzer_device_id"] = "100"
    four_value_row["old_primary_snapshot"] = json.dumps([1.0, 2.0, 3.0, 4.0])
    four_value_row["old_secondary_snapshot"] = json.dumps([5.0, 6.0, 7.0, 8.0])

    supported = writer._supported_pair_rows(rows + [four_value_row])

    assert [row["analyzer_device_id"] for row in supported] == ["030"]


def test_supported_pair_rows_refuse_nonzero_secondary_pressure_target_slots():
    rows = _mapping_rows()
    polluted = dict(rows[0])
    polluted["secondary_candidate_values"] = (
        "2.06216e01,2.35468e-02,-2.21346e01,1.00000e-01,0.00000e00,0.00000e00"
    )

    supported = writer._supported_pair_rows([polluted])

    assert supported == []


class _FakeGasAnalyzer:
    instances = {}

    def __init__(self, port, baudrate=115200, timeout=1.0, device_id="000", **_kwargs):
        self.port = port
        self.device_id = device_id
        self.mode = 2
        self.active = True
        self.ftd = 1
        self.average = 49
        self.coeff1 = [10767.7, -4809.57, -6811.55, 3481.28, 0.0, 0.0]
        self.coeff3 = [182.551, -0.246991, -30.4072, -20.4466, 0.0294764, 0.0]
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
        if int(group) == 1:
            self.coeff1 = list(rounded_senco_values(coeffs))
        elif int(group) == 3:
            self.coeff3 = list(rounded_senco_values(coeffs))
        else:
            raise AssertionError(f"unexpected SENCO group {group}")
        return True

    def read_coefficient_group(self, group, **_kwargs):
        self.calls.append(("getco", int(group)))
        values = self.coeff1 if int(group) == 1 else self.coeff3
        return {f"C{idx}": value for idx, value in enumerate(values)}

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


def test_controlled_co2_senco13_write_requires_explicit_unlock(tmp_path):
    cfg_path = tmp_path / "cfg.json"
    review_dir = tmp_path / "review"
    review_dir.mkdir()
    _write_json(cfg_path, _config(tmp_path))
    _write_csv(review_dir / "candidate_senco_mapping_review.csv", _mapping_rows())
    _write_formula_contract_pass(review_dir)

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--review-dir",
            str(review_dir),
            "--output-dir",
            str(tmp_path / "out"),
            "--device-id",
            "030",
            "--reviewer",
            "reviewer-a",
            "--approver",
            "approver-b",
        ]
    )

    assert rc == 2


def test_controlled_co2_senco13_pair_write_selected_only(monkeypatch, tmp_path):
    _FakeGasAnalyzer.instances = {}
    monkeypatch.setattr(writer, "GasAnalyzer", _FakeGasAnalyzer)
    cfg_path = tmp_path / "cfg.json"
    review_dir = tmp_path / "review"
    out_dir = tmp_path / "out"
    review_dir.mkdir()
    _write_json(cfg_path, _config(tmp_path))
    _write_csv(review_dir / "candidate_senco_mapping_review.csv", _mapping_rows())
    _write_formula_contract_pass(review_dir)

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--review-dir",
            str(review_dir),
            "--output-dir",
            str(out_dir),
            "--device-id",
            "030",
            "--enable-senco13-write",
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
    rows = _read_csv(out_dir / "co2_senco13_pair_write_summary.csv")
    assert [row["analyzer_device_id"] for row in rows] == ["030"]
    assert rows[0]["status"] == "written_readback_verified"
    assert rows[0]["writes_senco1"] == "True"
    assert rows[0]["writes_senco3"] == "True"
    assert rows[0]["controls_water_or_gas_routes"] == "False"
    assert rows[0]["writes_device_id"] == "False"
    assert rows[0]["clears_senco"] == "False"
    ga = _FakeGasAnalyzer.instances["COM36"]
    assert ga.coeff1 == [14881.5, -25610.4, 13458.5, -1521.12, 0.0, 0.0]
    assert ga.coeff3 == [20.6216, 0.0235468, -22.1346, 0.0, 0.0, 0.0]
    assert ("senco", 1, ga.coeff1) in ga.calls
    assert ("senco", 3, ga.coeff3) in ga.calls
    assert ("ftd", 1, False) in ga.calls
    assert ("avg", 49, False) in ga.calls
    assert ("comm", True, False) in ga.calls
    assert (out_dir / "old_getco1_getco3_snapshot.json").exists()
    sidecar_path = out_dir / "co2_senco13_pair_write_database_sidecar.json"
    assert sidecar_path.exists()
    sidecar_rows = _load_database_sidecar_rows(
        [{"id": "artifact-1", "artifact_role": "coefficient_write_log", "path": str(sidecar_path)}]
    )
    write_events = _build_sidecar_write_events(run_db_id="run-db", analyzer_id="all", sidecar_rows=sidecar_rows)
    assert write_events[0]["event_type"] == "co2_senco1_senco3_paired_write"
    assert write_events[0]["status"] == "written_readback_verified"
    assert write_events[0]["approved_by"] == "approver-b"
    assert write_events[0]["readback"]["identity_after"] == "030"


def test_controlled_co2_senco13_write_refuses_missing_old_snapshot(tmp_path):
    cfg_path = tmp_path / "cfg.json"
    review_dir = tmp_path / "review"
    review_dir.mkdir()
    _write_json(cfg_path, _config(tmp_path))
    _write_csv(review_dir / "candidate_senco_mapping_review.csv", _mapping_rows())
    _write_formula_contract_pass(review_dir)

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--review-dir",
            str(review_dir),
            "--output-dir",
            str(tmp_path / "out"),
            "--device-id",
            "022",
            "--enable-senco13-write",
            "--operator-confirmation",
            writer.CONFIRMATION_TEXT,
            "--reviewer",
            "reviewer-a",
            "--approver",
            "approver-b",
        ]
    )

    assert rc == 2


def test_controlled_co2_senco13_write_refuses_missing_fit_input_precheck_before_open(monkeypatch, tmp_path):
    _FakeGasAnalyzer.instances = {}
    monkeypatch.setattr(writer, "GasAnalyzer", _FakeGasAnalyzer)
    cfg_path = tmp_path / "cfg.json"
    review_dir = tmp_path / "review"
    review_dir.mkdir()
    _write_json(cfg_path, _config(tmp_path))
    _write_csv(review_dir / "candidate_senco_mapping_review.csv", _mapping_rows())
    _write_formula_contract_pass(review_dir)
    (review_dir / "main_senco_write_precheck_meta.json").unlink()

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--review-dir",
            str(review_dir),
            "--output-dir",
            str(tmp_path / "out"),
            "--device-id",
            "030",
            "--enable-senco13-write",
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


def test_controlled_co2_senco13_write_refuses_missing_formula_contract(tmp_path):
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
            "--device-id",
            "030",
            "--enable-senco13-write",
            "--operator-confirmation",
            writer.CONFIRMATION_TEXT,
            "--reviewer",
            "reviewer-a",
            "--approver",
            "approver-b",
        ]
    )

    assert rc == 2


def test_controlled_co2_senco13_write_refuses_missing_senco5_contract(tmp_path):
    cfg_path = tmp_path / "cfg.json"
    review_dir = tmp_path / "review"
    review_dir.mkdir()
    _write_json(cfg_path, _config(tmp_path))
    _write_csv(review_dir / "candidate_senco_mapping_review.csv", _mapping_rows())
    _write_csv(
        review_dir / "candidate_write_review_checks.csv",
        [
            {
                "check": writer.FORMULA_CONTRACT_CHECK,
                "status": "pass",
                "meaning": "formula contract confirmed",
                "evidence": "test_only",
            }
        ],
    )

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--review-dir",
            str(review_dir),
            "--output-dir",
            str(tmp_path / "out"),
            "--device-id",
            "030",
            "--enable-senco13-write",
            "--operator-confirmation",
            writer.CONFIRMATION_TEXT,
            "--reviewer",
            "reviewer-a",
            "--approver",
            "approver-b",
        ]
    )

    assert rc == 2


def test_controlled_co2_senco13_rollback_restores_old_snapshot(monkeypatch, tmp_path):
    class _BadCoeffAnalyzer(_FakeGasAnalyzer):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.coeff1 = [14881.5, -25610.4, 13458.5, -1521.12, 0.0, 0.0]
            self.coeff3 = [20.6216, 0.0235468, -22.1346, 0.0, 0.0, 0.0]

    _FakeGasAnalyzer.instances = {}
    monkeypatch.setattr(rollback, "GasAnalyzer", _BadCoeffAnalyzer)
    cfg_path = tmp_path / "cfg.json"
    write_dir = tmp_path / "write"
    out_dir = tmp_path / "rollback"
    write_dir.mkdir()
    _write_json(cfg_path, _config(tmp_path))
    _write_json(
        write_dir / "old_getco1_getco3_snapshot.json",
        {
            "030": {
                "GETCO1_before_live": [10767.7, -4809.57, -6811.55, 3481.28, 0.0, 0.0],
                "GETCO3_before_live": [182.551, -0.246991, -30.4072, -20.4466, 0.0294764, 0.0],
            }
        },
    )

    rc = rollback.main(
        [
            "--config",
            str(cfg_path),
            "--write-dir",
            str(write_dir),
            "--output-dir",
            str(out_dir),
            "--device-id",
            "030",
            "--enable-senco13-rollback",
            "--operator-confirmation",
            rollback.CONFIRMATION_TEXT,
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
    rows = _read_csv(out_dir / "co2_senco13_pair_rollback_summary.csv")
    assert rows[0]["status"] == "rollback_readback_verified"
    ga = _FakeGasAnalyzer.instances["COM36"]
    assert ga.coeff1 == [10767.7, -4809.57, -6811.55, 3481.28, 0.0, 0.0]
    assert ga.coeff3 == [182.551, -0.246991, -30.4072, -20.4466, 0.0294764, 0.0]
    assert rows[0]["controls_water_or_gas_routes"] == "False"
    assert rows[0]["writes_device_id"] == "False"
    assert rows[0]["clears_senco"] == "False"
    sidecar_rows = _load_database_sidecar_rows(
        [
            {
                "id": "artifact-rollback",
                "artifact_role": "coefficient_write_log",
                "path": str(out_dir / "co2_senco13_pair_rollback_database_sidecar.json"),
            }
        ]
    )
    write_events = _build_sidecar_write_events(run_db_id="run-db", analyzer_id="all", sidecar_rows=sidecar_rows)
    assert write_events[0]["event_type"] == "co2_senco1_senco3_pair_rollback"
    assert write_events[0]["status"] == "rollback_readback_verified"
