from __future__ import annotations

import json

from gas_calibrator.tools import run_v1_5_sn_identity_initialization as sn_init


def _config():
    return {
        "schema_version": "v1_5_sn_identity_initialization_config_v0_test",
        "sn_identity_contract": {
            "hardware_version_prefix": "01",
            "year_month": "2606",
            "sequence_start": 1,
            "write_target": "FFF",
            "command_gap_s": 1.2,
            "sn_read_timeout_s": 0.05,
        },
        "analyzers": [
            {
                "slot": "GA01",
                "enabled": True,
                "port": "COM36",
                "baud": 115200,
                "protocol_device_id": "047",
                "sn_code": "00000000",
            },
            {
                "slot": "GA02",
                "enabled": True,
                "port": "COM37",
                "baud": 115200,
                "protocol_device_id": "054",
                "sn_code": "00000000",
            },
        ],
    }


class _FakeSerial:
    def __init__(self, *, protocol_device_id: str, sn_code: str):
        self.protocol_device_id = protocol_device_id
        self.sn_code = sn_code
        self.writes = []
        self._sn_pending = False

    def flush_input(self):
        return None

    def write(self, text):
        payload = str(text).strip()
        self.writes.append(payload)
        parts = [part.strip() for part in payload.split(",")]
        if parts == ["SN", "YGAS", "FFF"]:
            self._sn_pending = True
        elif len(parts) == 4 and parts[0] == "SN" and parts[1] == "YGAS":
            self.sn_code = parts[3]

    def readline(self):
        if self._sn_pending:
            self._sn_pending = False
            return f"YGAS,{self.protocol_device_id},{self.sn_code}"
        return ""

    def drain_input_nonblock(self, **_kwargs):
        return []


class _FakeAnalyzer:
    def __init__(self, item):
        self.item = dict(item)
        self.ser = _FakeSerial(
            protocol_device_id=str(item["protocol_device_id"]),
            sn_code=str(item["current_sn_expected"] or "00000000"),
        )
        self.calls = []

    def open(self):
        self.calls.append(("open",))

    def close(self):
        self.calls.append(("close",))

    def read_current_mode_snapshot(self, **_kwargs):
        self.calls.append(("read_current_mode_snapshot",))
        return {"mode": 2, "id": self.item["protocol_device_id"]}

    def set_mode_with_ack(self, mode, require_ack=False):
        self.calls.append(("set_mode_with_ack", int(mode), bool(require_ack)))
        return int(mode) == 2

    def set_comm_way_with_ack(self, active, require_ack=False):
        self.calls.append(("set_comm_way_with_ack", bool(active), bool(require_ack)))
        return True


def test_build_plan_allocates_hwver_yymm_sequence():
    plan = sn_init.build_sn_identity_initialization_plan(_config(), run_id="sn_plan")

    assert plan["status"] == "write_required"
    assert plan["write_candidate_count"] == 2
    assert [row["target_sn"] for row in plan["rows"]] == ["01260601", "01260602"]
    assert all(row["write_command"].startswith("SN,YGAS,FFF,") for row in plan["rows"])
    assert plan["boundary"]["writes_sn"] is True
    assert plan["boundary"]["writes_senco"] is False


def test_build_plan_accepts_mature_devices_gas_analyzers_shape():
    config = _config()
    config["devices"] = {"gas_analyzers": config.pop("analyzers")}

    plan = sn_init.build_sn_identity_initialization_plan(config, run_id="sn_plan")

    assert [row["slot"] for row in plan["rows"]] == ["GA01", "GA02"]
    assert [row["protocol_device_id"] for row in plan["rows"]] == ["047", "054"]
    assert [row["target_sn"] for row in plan["rows"]] == ["01260601", "01260602"]


def test_build_plan_can_target_protocol_device_id_for_sn_write():
    config = _config()
    config["sn_identity_contract"]["write_target"] = "protocol_device_id"

    plan = sn_init.build_sn_identity_initialization_plan(config, run_id="sn_plan")

    assert plan["rows"][0]["write_target_id"] == "047"
    assert plan["rows"][0]["write_command"] == "SN,YGAS,047,01260601"


def test_dry_run_writes_plan_and_result(tmp_path):
    plan = sn_init.build_sn_identity_initialization_plan(_config(), run_id="sn_plan")

    result = sn_init.execute_sn_identity_initialization(plan, output_dir=tmp_path, execute=False)

    assert result["status"] == "dry_run_ready"
    assert (tmp_path / "00_plan" / "v1_5_sn_identity_initialization_plan.json").exists()
    assert (tmp_path / "v1_5_sn_identity_initialization_result.json").exists()
    saved = json.loads((tmp_path / "v1_5_sn_identity_initialization_result.json").read_text(encoding="utf-8"))
    assert saved["write_candidate_count"] == 2


def test_execute_blocks_without_sn_write_ack(tmp_path):
    plan = sn_init.build_sn_identity_initialization_plan(_config(), run_id="sn_plan")

    result = sn_init.execute_sn_identity_initialization(plan, output_dir=tmp_path, execute=True)

    assert result["status"] == "blocked_requires_sn_write_ack"


def test_execute_writes_sn_and_verifies_double_readback(tmp_path):
    plan = sn_init.build_sn_identity_initialization_plan(_config(), run_id="sn_plan")
    analyzers = {}

    def factory(item):
        key = str(item["slot"])
        analyzers.setdefault(key, _FakeAnalyzer(item))
        return analyzers[key]

    result = sn_init.execute_sn_identity_initialization(
        plan,
        output_dir=tmp_path,
        analyzer_factory=factory,
        execute=True,
        acknowledge_sn_write=True,
        run_id="sn_execute",
        sleep_fn=lambda _seconds: None,
    )

    assert result["status"] == "success"
    assert result["completed_write_count"] == 2
    assert [row["status"] for row in result["results"]] == ["sn_write_verified_mode2", "sn_write_verified_mode2"]
    assert result["results"][0]["readback1_sn"] == "01260601"
    assert result["results"][0]["readback2_sn"] == "01260601"
    assert "SN,YGAS,FFF,01260601" in analyzers["GA01"].ser.writes
    assert ("set_mode_with_ack", 2, False) in analyzers["GA01"].calls
    assert ("set_comm_way_with_ack", False, False) in analyzers["GA01"].calls
    assert ("set_comm_way_with_ack", True, False) in analyzers["GA01"].calls
    assert analyzers["GA01"].calls.index(("set_comm_way_with_ack", False, False)) < analyzers[
        "GA01"
    ].calls.index(("set_mode_with_ack", 2, False))


def test_execute_restores_active_stream_when_sn_write_is_not_applied(tmp_path):
    config = _config()
    config["analyzers"] = config["analyzers"][:1]
    plan = sn_init.build_sn_identity_initialization_plan(config, run_id="sn_plan")
    analyzers = {}

    def factory(item):
        analyzer = _FakeAnalyzer(item)
        original_write = analyzer.ser.write

        def reject_sn_write(text):
            payload = str(text).strip()
            parts = [part.strip() for part in payload.split(",")]
            if len(parts) == 4 and parts[0] == "SN" and parts[1] == "YGAS":
                analyzer.ser.writes.append(payload)
                return
            original_write(text)

        analyzer.ser.write = reject_sn_write
        analyzers[str(item["slot"])] = analyzer
        return analyzer

    result = sn_init.execute_sn_identity_initialization(
        plan,
        output_dir=tmp_path,
        analyzer_factory=factory,
        execute=True,
        acknowledge_sn_write=True,
        run_id="sn_execute_rejected",
        sleep_fn=lambda _seconds: None,
    )

    assert result["status"] == "partial"
    assert result["results"][0]["status"] == "failed"
    assert "SN readback mismatch" in result["results"][0]["error"]
    assert ("set_comm_way_with_ack", False, False) in analyzers["GA01"].calls
    assert ("set_comm_way_with_ack", True, False) in analyzers["GA01"].calls
