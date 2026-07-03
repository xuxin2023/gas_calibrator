import csv
import json

from gas_calibrator.storage.v1_5_evidence.bundle import _build_sidecar_write_events, _load_database_sidecar_rows
from gas_calibrator.senco_format import rounded_senco_values
from gas_calibrator.tools import run_v1_5_h2o_senco24_controlled_write as writer


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
                    "name": "ga051",
                    "enabled": True,
                    "port": "COM42",
                    "baud": 115200,
                    "device_id": "051",
                    "mode": 2,
                    "active_send": True,
                    "ftd_hz": 1,
                    "average_filter": 49,
                },
                {
                    "name": "ga100",
                    "enabled": True,
                    "port": "COM41",
                    "baud": 115200,
                    "device_id": "100",
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


def _write_review_artifacts(review_dir):
    _write_csv(
        review_dir / "h2o_senco24_payload_preview.csv",
        [
            {
                "component": "h2o",
                "analyzer_prefix": "ga051",
                "analyzer_device_id": "051",
                "primary_senco": "SENCO2",
                "secondary_senco": "SENCO4",
                "senco2_payload_values_json": json.dumps(
                    [1839.2885131386086, -7124.858930061349, 8654.826062921924, -3470.681156521369, 0.0, 0.0]
                ),
                "senco4_payload_values_json": json.dumps(
                    [1.2971114781717432, -0.0015236533229940944, -0.6831546647231543, 0.0, 0.0, 0.0]
                ),
                "auto_write_allowed": "False",
            },
            {
                "component": "h2o",
                "analyzer_prefix": "ga100",
                "analyzer_device_id": "100",
                "primary_senco": "SENCO2",
                "secondary_senco": "SENCO4",
                "senco2_payload_values_json": json.dumps([1.0, 2.0, 3.0, 4.0, 0.0, 0.0]),
                "senco4_payload_values_json": json.dumps([5.0, 6.0, 7.0, 8.0, 0.0, 0.0]),
                "auto_write_allowed": "False",
            },
        ],
    )
    _write_csv(
        review_dir / "h2o_senco24_device_policy.csv",
        [
            {
                "component": "h2o",
                "analyzer_prefix": "ga051",
                "analyzer_device_id": "051",
                "candidate_status": "candidate_ratio_fit_available_but_final_output_blocked",
                "blocked_reasons": "",
                "warning_reasons": "analyzer_final_h2o_output_pinned",
            },
            {
                "component": "h2o",
                "analyzer_prefix": "ga100",
                "analyzer_device_id": "100",
                "candidate_status": "blocked",
                "blocked_reasons": "manual_device_block:firmware_upgrade_required",
                "warning_reasons": "",
            },
        ],
    )
    _write_csv(
        review_dir / "h2o_senco24_output_diagnostics.csv",
        [
            {
                "component": "h2o",
                "analyzer_device_id": "051",
                "diagnosis": "final_h2o_output_pinned_with_neutral_senco6",
                "GETCO6_neutral": "True",
            },
            {
                "component": "h2o",
                "analyzer_device_id": "100",
                "diagnosis": "manual_device_block",
                "GETCO6_neutral": "",
            },
        ],
    )


def _write_new_absorption_review_artifacts(
    review_dir,
    *,
    include_contract=True,
    s4_values=None,
):
    s4_values = (
        [0.007731677978824871, -0.0001529135248583926, 1.3858142511341637e-06, -4.346735800928211e-09, 0.0, 0.0]
        if s4_values is None
        else list(s4_values)
    )
    contract = "new_algo_h2o_absorption_virtual_R0_poly_b4_k3" if include_contract else ""
    _write_csv(
        review_dir / "h2o_senco24_payload_preview.csv",
        [
            {
                "component": "h2o",
                "analyzer_prefix": "ga051",
                "analyzer_device_id": "051",
                "primary_senco": "SENCO2",
                "secondary_senco": "SENCO4",
                "senco2_payload_values_json": json.dumps(
                    [
                        -0.24030383435468217,
                        -0.0025883049211054185,
                        0.00031347462068234433,
                        -1.2527603671430974e-05,
                        1.4509637538096689e-07,
                        0.0,
                    ]
                ),
                "senco4_payload_values_json": json.dumps(s4_values),
                "auto_write_allowed": "False",
                "senco24_main_chain_contract": contract,
                "coefficient_order": "ascending_constant_first",
            }
        ],
    )
    _write_csv(
        review_dir / "h2o_senco24_device_policy.csv",
        [
            {
                "component": "h2o",
                "analyzer_prefix": "ga051",
                "analyzer_device_id": "051",
                "candidate_status": "candidate_fit_ready_new_algo_absorption_reviewed_no_write",
                "blocked_reasons": "",
                "candidate_contract": contract,
            }
        ],
    )
    _write_csv(
        review_dir / "h2o_senco24_output_diagnostics.csv",
        [
            {
                "component": "h2o",
                "analyzer_device_id": "051",
                "diagnosis": "new_algorithm_absorption_candidate_ready_for_controlled_write_preview",
                "GETCO6_neutral": "True",
            }
        ],
    )


def _write_snapshot(path, *, getco6=None):
    _write_json(
        path,
        {
            "051": {
                "analyzer_prefix": "ga051",
                "analyzer_device_id": "051",
                "port": "COM42",
                "GETCO2_before": [1288.01, 0.0, 0.766182, 0.295489, -0.0620835, 0.0],
                "GETCO4_before": [0.0, 0.0, 0.0, 1.0, 0.0, 0.0],
                "GETCO6_before": [0.0, 1.0] if getco6 is None else getco6,
            }
        },
    )


class _FakeGasAnalyzer:
    instances = {}
    default_coeff2 = [1288.01, 0.0, 0.766182, 0.295489, -0.0620835, 0.0]
    default_coeff4 = [0.0, 0.0, 0.0, 1.0, 0.0, 0.0]
    default_coeff6 = [0.0, 1.0]

    def __init__(self, port, baudrate=115200, timeout=1.0, device_id="000", **_kwargs):
        self.port = port
        self.device_id = device_id
        self.mode = 2
        self.active = True
        self.ftd = 1
        self.average = 49
        self.coeff2 = list(self.default_coeff2)
        self.coeff4 = list(self.default_coeff4)
        self.coeff6 = list(self.default_coeff6)
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

    def set_senco(self, group, *coeffs):
        group = int(group)
        values = list(rounded_senco_values(coeffs))
        self.calls.append(("senco", group, values))
        if group == 2:
            self.coeff2 = values
        elif group == 4:
            self.coeff4 = values
        else:
            raise AssertionError(f"unexpected SENCO group {group}")
        return True

    def read_coefficient_group(self, group, **_kwargs):
        self.calls.append(("getco", int(group)))
        if int(group) == 2:
            values = self.coeff2
        elif int(group) == 4:
            values = self.coeff4
        elif int(group) == 6:
            values = self.coeff6
        else:
            raise AssertionError(f"unexpected GETCO group {group}")
        return {f"C{idx}": value for idx, value in enumerate(values)}

    def set_comm_way_with_ack(self, active, require_ack=True):
        self.calls.append(("comm", active, require_ack))
        self.active = bool(active)
        return True

    def set_active_freq_with_ack(self, hz, require_ack=True):
        self.calls.append(("ftd", int(hz), require_ack))
        self.ftd = int(hz)
        return True

    def set_average_filter_with_ack(self, window_n, require_ack=True):
        self.calls.append(("avg", int(window_n), require_ack))
        self.average = int(window_n)
        return True


def test_h2o_senco24_writer_requires_explicit_unlock(tmp_path):
    cfg_path = tmp_path / "cfg.json"
    review_dir = tmp_path / "review"
    snapshot_path = tmp_path / "snapshot.json"
    review_dir.mkdir()
    _write_json(cfg_path, _config(tmp_path))
    _write_review_artifacts(review_dir)
    _write_snapshot(snapshot_path)

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--review-dir",
            str(review_dir),
            "--old-component-snapshot-json",
            str(snapshot_path),
            "--output-dir",
            str(tmp_path / "out"),
            "--device-id",
            "051",
            "--reviewer",
            "reviewer-a",
            "--approver",
            "approver-b",
        ]
    )

    assert rc == 2


def test_h2o_senco24_writer_writes_051_only_and_preserves_senco6(monkeypatch, tmp_path):
    _FakeGasAnalyzer.instances = {}
    monkeypatch.setattr(writer, "GasAnalyzer", _FakeGasAnalyzer)
    cfg_path = tmp_path / "cfg.json"
    review_dir = tmp_path / "review"
    snapshot_path = tmp_path / "snapshot.json"
    out_dir = tmp_path / "out"
    review_dir.mkdir()
    _write_json(cfg_path, _config(tmp_path))
    _write_review_artifacts(review_dir)
    _write_snapshot(snapshot_path)

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--review-dir",
            str(review_dir),
            "--old-component-snapshot-json",
            str(snapshot_path),
            "--output-dir",
            str(out_dir),
            "--device-id",
            "051",
            "--enable-senco24-write",
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
    rows = _read_csv(out_dir / "h2o_senco24_pair_write_summary.csv")
    assert [row["analyzer_device_id"] for row in rows] == ["051"]
    assert rows[0]["status"] == "written_readback_verified"
    assert rows[0]["controls_water_or_gas_routes"] == "False"
    assert rows[0]["writes_device_id"] == "False"
    assert rows[0]["writes_senco2"] == "True"
    assert rows[0]["writes_senco4"] == "True"
    assert rows[0]["writes_senco6"] == "False"
    assert rows[0]["clears_senco"] == "False"
    ga = _FakeGasAnalyzer.instances["COM42"]
    assert ga.coeff2 == [1839.29, -7124.86, 8654.83, -3470.68, 0.0, 0.0]
    assert ga.coeff4 == [1.29711, -0.00152365, -0.683155, 0.0, 0.0, 0.0]
    assert ga.coeff6 == [0.0, 1.0]
    assert ("senco", 2, ga.coeff2) in ga.calls
    assert ("senco", 4, ga.coeff4) in ga.calls
    assert not any(call[0] == "senco" and call[1] == 6 for call in ga.calls)
    assert ("ftd", 1, False) in ga.calls
    assert ("avg", 49, False) in ga.calls
    assert ("comm", True, False) in ga.calls
    assert (out_dir / "old_getco2_getco4_getco6_snapshot.json").exists()
    sidecar = json.loads((out_dir / "h2o_senco24_pair_write_database_sidecar.json").read_text(encoding="utf-8"))
    assert sidecar["controls_water_or_gas_routes"] is False
    assert sidecar["suggested_rows"][0]["event_type"] == "h2o_senco2_senco4_paired_write"
    assert sidecar["suggested_rows"][0]["status"] == "written_readback_verified"
    sidecar_rows = _load_database_sidecar_rows(
        [
            {
                "id": "artifact-h2o-write",
                "artifact_role": "coefficient_write_log",
                "path": str(out_dir / "h2o_senco24_pair_write_database_sidecar.json"),
            }
        ]
    )
    write_events = _build_sidecar_write_events(run_db_id="run-db", analyzer_id="all", sidecar_rows=sidecar_rows)
    assert write_events[0]["event_type"] == "h2o_senco2_senco4_paired_write"
    assert write_events[0]["status"] == "written_readback_verified"
    assert write_events[0]["approved_by"] == "approver-b"
    assert write_events[0]["readback"]["identity_after"] == "051"


def test_h2o_senco24_writer_accepts_short_legacy_getco4_zero_tail(monkeypatch, tmp_path):
    _FakeGasAnalyzer.instances = {}
    monkeypatch.setattr(writer, "GasAnalyzer", _FakeGasAnalyzer)
    cfg_path = tmp_path / "cfg.json"
    review_dir = tmp_path / "review"
    snapshot_path = tmp_path / "snapshot.json"
    out_dir = tmp_path / "out"
    review_dir.mkdir()
    _write_json(cfg_path, _config(tmp_path))
    _write_review_artifacts(review_dir)
    _write_snapshot(snapshot_path)
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    snapshot["051"]["GETCO4_before"] = [0.0, 0.0, 0.0, 1.0]
    _write_json(snapshot_path, snapshot)

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--review-dir",
            str(review_dir),
            "--old-component-snapshot-json",
            str(snapshot_path),
            "--output-dir",
            str(out_dir),
            "--device-id",
            "051",
            "--enable-senco24-write",
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
    rows = _read_csv(out_dir / "h2o_senco24_pair_write_summary.csv")
    assert rows[0]["status"] == "written_readback_verified"
    assert rows[0]["senco4_readback"] == json.dumps([1.29711, -0.00152365, -0.683155, 0.0, 0.0, 0.0])


def test_h2o_senco24_writer_accepts_live_getco_zero_tail_omission(monkeypatch, tmp_path):
    _FakeGasAnalyzer.instances = {}
    monkeypatch.setattr(_FakeGasAnalyzer, "default_coeff2", [1288.01, 0.0, 0.766182, 0.295489, -0.0620835])
    monkeypatch.setattr(_FakeGasAnalyzer, "default_coeff4", [0.0, 0.0, 0.0, 1.0])
    monkeypatch.setattr(writer, "GasAnalyzer", _FakeGasAnalyzer)
    cfg_path = tmp_path / "cfg.json"
    review_dir = tmp_path / "review"
    snapshot_path = tmp_path / "snapshot.json"
    out_dir = tmp_path / "out"
    review_dir.mkdir()
    _write_json(cfg_path, _config(tmp_path))
    _write_review_artifacts(review_dir)
    _write_snapshot(snapshot_path)

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--review-dir",
            str(review_dir),
            "--old-component-snapshot-json",
            str(snapshot_path),
            "--output-dir",
            str(out_dir),
            "--device-id",
            "051",
            "--enable-senco24-write",
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
    rows = _read_csv(out_dir / "h2o_senco24_pair_write_summary.csv")
    assert rows[0]["status"] == "written_readback_verified"


def test_h2o_senco24_writer_allows_review_required_only_with_explicit_override(monkeypatch, tmp_path):
    _FakeGasAnalyzer.instances = {}
    monkeypatch.setattr(writer, "GasAnalyzer", _FakeGasAnalyzer)
    cfg_path = tmp_path / "cfg.json"
    review_dir = tmp_path / "review"
    snapshot_path = tmp_path / "snapshot.json"
    out_dir = tmp_path / "out"
    review_dir.mkdir()
    _write_json(cfg_path, _config(tmp_path))
    _write_review_artifacts(review_dir)
    policies = _read_csv(review_dir / "h2o_senco24_device_policy.csv")
    policies[0]["candidate_status"] = "candidate_fit_review_required"
    policies[0]["warning_reasons"] = "manual_review_required"
    _write_csv(review_dir / "h2o_senco24_device_policy.csv", policies)
    _write_snapshot(snapshot_path)

    rc_locked = writer.main(
        [
            "--config",
            str(cfg_path),
            "--review-dir",
            str(review_dir),
            "--old-component-snapshot-json",
            str(snapshot_path),
            "--output-dir",
            str(tmp_path / "out_locked"),
            "--device-id",
            "051",
            "--enable-senco24-write",
            "--operator-confirmation",
            writer.CONFIRMATION_TEXT,
            "--reviewer",
            "reviewer-a",
            "--approver",
            "approver-b",
        ]
    )
    assert rc_locked == 2

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--review-dir",
            str(review_dir),
            "--old-component-snapshot-json",
            str(snapshot_path),
            "--output-dir",
            str(out_dir),
            "--device-id",
            "051",
            "--enable-senco24-write",
            "--allow-review-required-candidates",
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
    rows = _read_csv(out_dir / "h2o_senco24_pair_write_summary.csv")
    assert rows[0]["manual_review_required_override"] == "True"
    assert rows[0]["status"] == "written_readback_verified"


def test_h2o_senco24_writer_refuses_blocked_or_non_neutral_targets(tmp_path):
    cfg_path = tmp_path / "cfg.json"
    review_dir = tmp_path / "review"
    snapshot_path = tmp_path / "snapshot.json"
    review_dir.mkdir()
    _write_json(cfg_path, _config(tmp_path))
    _write_review_artifacts(review_dir)
    _write_snapshot(snapshot_path, getco6=[11.0, 0.66])

    rc_non_neutral = writer.main(
        [
            "--config",
            str(cfg_path),
            "--review-dir",
            str(review_dir),
            "--old-component-snapshot-json",
            str(snapshot_path),
            "--output-dir",
            str(tmp_path / "out_non_neutral"),
            "--device-id",
            "051",
            "--enable-senco24-write",
            "--operator-confirmation",
            writer.CONFIRMATION_TEXT,
            "--reviewer",
            "reviewer-a",
            "--approver",
            "approver-b",
        ]
    )
    assert rc_non_neutral == 2

    _write_snapshot(snapshot_path)
    rc_blocked = writer.main(
        [
            "--config",
            str(cfg_path),
            "--review-dir",
            str(review_dir),
            "--old-component-snapshot-json",
            str(snapshot_path),
            "--output-dir",
            str(tmp_path / "out_blocked"),
            "--device-id",
            "100",
            "--enable-senco24-write",
            "--operator-confirmation",
            writer.CONFIRMATION_TEXT,
            "--reviewer",
            "reviewer-a",
            "--approver",
            "approver-b",
        ]
    )
    assert rc_blocked == 2


def test_h2o_senco24_writer_allows_non_neutral_senco6_after_separate_layer_review(monkeypatch, tmp_path):
    _FakeGasAnalyzer.instances = {}
    monkeypatch.setattr(_FakeGasAnalyzer, "default_coeff6", [0.8, 1.04])
    monkeypatch.setattr(writer, "GasAnalyzer", _FakeGasAnalyzer)
    cfg_path = tmp_path / "cfg.json"
    review_dir = tmp_path / "review"
    snapshot_path = tmp_path / "snapshot.json"
    out_dir = tmp_path / "out"
    review_dir.mkdir()
    _write_json(cfg_path, _config(tmp_path))
    _write_review_artifacts(review_dir)
    diagnostics = _read_csv(review_dir / "h2o_senco24_output_diagnostics.csv")
    diagnostics[0]["diagnosis"] = "ratio_temperature_candidate_fit_valid_with_separate_senco6_review_required"
    diagnostics[0]["GETCO6_neutral"] = "False"
    _write_csv(review_dir / "h2o_senco24_output_diagnostics.csv", diagnostics)
    _write_snapshot(snapshot_path, getco6=[0.8, 1.04])

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--review-dir",
            str(review_dir),
            "--old-component-snapshot-json",
            str(snapshot_path),
            "--output-dir",
            str(out_dir),
            "--device-id",
            "051",
            "--enable-senco24-write",
            "--allow-separate-senco6-layer-review",
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
    rows = _read_csv(out_dir / "h2o_senco24_pair_write_summary.csv")
    assert rows[0]["status"] == "written_readback_verified"
    assert rows[0]["senco6_separate_layer_reviewed"] == "True"
    assert rows[0]["writes_senco6"] == "False"
    ga = _FakeGasAnalyzer.instances["COM42"]
    assert ga.coeff6 == [0.8, 1.04]
    assert not any(call[0] == "senco" and call[1] == 6 for call in ga.calls)


def test_h2o_senco24_writer_refuses_nonzero_secondary_pressure_target_slots(tmp_path):
    cfg_path = tmp_path / "cfg.json"
    review_dir = tmp_path / "review"
    snapshot_path = tmp_path / "snapshot.json"
    review_dir.mkdir()
    _write_json(cfg_path, _config(tmp_path))
    _write_review_artifacts(review_dir)
    rows = _read_csv(review_dir / "h2o_senco24_payload_preview.csv")
    rows[0]["senco4_payload_values_json"] = json.dumps([1.29711, -0.00152365, -0.683155, 0.1, 0.0, 0.0])
    _write_csv(review_dir / "h2o_senco24_payload_preview.csv", rows)
    _write_snapshot(snapshot_path)

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--review-dir",
            str(review_dir),
            "--old-component-snapshot-json",
            str(snapshot_path),
            "--output-dir",
            str(tmp_path / "out_pressure_slots"),
            "--device-id",
            "051",
            "--enable-senco24-write",
            "--operator-confirmation",
            writer.CONFIRMATION_TEXT,
            "--reviewer",
            "reviewer-a",
            "--approver",
            "approver-b",
        ]
    )

    assert rc == 2


def test_h2o_senco24_supported_rows_accept_new_absorption_s4_k3_only_with_new_algorithm(tmp_path):
    review_dir = tmp_path / "review"
    snapshot_path = tmp_path / "snapshot.json"
    review_dir.mkdir()
    _write_new_absorption_review_artifacts(review_dir)
    _write_snapshot(snapshot_path)
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))

    old_rows = writer._supported_rows(review_dir=review_dir, old_snapshot=snapshot)
    assert old_rows == []

    new_rows = writer._supported_rows(
        review_dir=review_dir,
        old_snapshot=snapshot,
        h2o_senco24_algorithm=writer.H2O_SENCO24_ALGORITHM_NEW_ABSORPTION,
    )
    assert len(new_rows) == 1
    assert new_rows[0]["_secondary_target_values"][3] != 0.0
    assert new_rows[0]["_secondary_target_values"][4:] == [0.0, 0.0]


def test_h2o_senco24_new_absorption_requires_explicit_contract(tmp_path):
    review_dir = tmp_path / "review"
    snapshot_path = tmp_path / "snapshot.json"
    review_dir.mkdir()
    _write_new_absorption_review_artifacts(review_dir, include_contract=False)
    _write_snapshot(snapshot_path)
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))

    rows = writer._supported_rows(
        review_dir=review_dir,
        old_snapshot=snapshot,
        h2o_senco24_algorithm=writer.H2O_SENCO24_ALGORITHM_NEW_ABSORPTION,
    )

    assert rows == []


def test_h2o_senco24_new_absorption_still_rejects_reserved_s4_tail(tmp_path):
    review_dir = tmp_path / "review"
    snapshot_path = tmp_path / "snapshot.json"
    review_dir.mkdir()
    _write_new_absorption_review_artifacts(
        review_dir,
        s4_values=[0.0077, -0.00015, 1.3e-06, -4.3e-09, 0.01, 0.0],
    )
    _write_snapshot(snapshot_path)
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))

    rows = writer._supported_rows(
        review_dir=review_dir,
        old_snapshot=snapshot,
        h2o_senco24_algorithm=writer.H2O_SENCO24_ALGORITHM_NEW_ABSORPTION,
    )

    assert rows == []


def test_h2o_senco24_new_absorption_writer_uses_controlled_flow(monkeypatch, tmp_path):
    _FakeGasAnalyzer.instances = {}
    monkeypatch.setattr(writer, "GasAnalyzer", _FakeGasAnalyzer)
    cfg_path = tmp_path / "cfg.json"
    review_dir = tmp_path / "review"
    snapshot_path = tmp_path / "snapshot.json"
    out_dir = tmp_path / "out"
    review_dir.mkdir()
    _write_json(cfg_path, _config(tmp_path))
    _write_new_absorption_review_artifacts(review_dir)
    _write_snapshot(snapshot_path)

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--review-dir",
            str(review_dir),
            "--old-component-snapshot-json",
            str(snapshot_path),
            "--output-dir",
            str(out_dir),
            "--device-id",
            "051",
            "--h2o-senco24-algorithm",
            writer.H2O_SENCO24_ALGORITHM_NEW_ABSORPTION,
            "--enable-senco24-write",
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
    rows = _read_csv(out_dir / "h2o_senco24_pair_write_summary.csv")
    assert rows[0]["status"] == "written_readback_verified"
    assert rows[0]["h2o_senco24_algorithm"] == writer.H2O_SENCO24_ALGORITHM_NEW_ABSORPTION
    ga = _FakeGasAnalyzer.instances["COM42"]
    assert ga.coeff4[3] == -4.34674e-09
    sidecar = json.loads((out_dir / "h2o_senco24_pair_write_database_sidecar.json").read_text(encoding="utf-8"))
    assert sidecar["h2o_senco24_algorithm"] == writer.H2O_SENCO24_ALGORITHM_NEW_ABSORPTION
    assert "new absorption contract" in sidecar["suggested_rows"][0]["command_summary"]
