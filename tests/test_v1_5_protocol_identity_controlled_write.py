from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from gas_calibrator.tools import run_v1_5_protocol_identity_controlled_write as tool


UNIQUENESS_FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "v1_5_protocol_identity_global_uniqueness_evidence.json"
)


def _authoritative_payload() -> dict[str, object]:
    payload = json.loads(UNIQUENESS_FIXTURE.read_text(encoding="utf-8"))
    payload["test_fixture_only"] = False
    payload.pop("not_real_acceptance_evidence", None)
    return payload


def _authoritative_evidence(tmp_path: Path) -> Path:
    path = tmp_path / "authoritative_identity_export.json"
    path.write_text(
        json.dumps(_authoritative_payload(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return path


def _plan(
    tmp_path: Path | None = None,
    *,
    approved: bool = True,
    rollback_authorized: bool = True,
):
    evidence_path = _authoritative_evidence(tmp_path) if approved and tmp_path else None
    if approved and evidence_path is None:
        raise ValueError("approved plan requires temporary authoritative evidence")
    evidence_sha256 = (
        hashlib.sha256(evidence_path.read_bytes()).hexdigest() if evidence_path else ""
    )
    return {
        "schema_version": tool.PLAN_SCHEMA,
        "status": "approved_single_device_write" if approved else "review_required",
        "execution_allowed": approved,
        "approval": {
            "global_sn_unique": approved,
            "global_protocol_id_unique": approved,
            "approved_by": "reviewer" if approved else "",
            "approved_at": "2026-07-31T21:00:00+08:00" if approved else "",
            "rollback_authorized": rollback_authorized,
        },
        "global_uniqueness_evidence": {
            "candidate_sn_absent": approved,
            "candidate_protocol_id_absent": approved,
            "source": str(evidence_path.resolve()) if evidence_path else "",
            "sha256": evidence_sha256,
        },
        "rows": [
            {
                "port": "COM37",
                "usb_serial_number": "OTHER",
                "sn_code": "01260712",
                "observed_protocol_device_id": "001",
                "target_protocol_device_id": "001",
                "action": "preserve_no_write",
            },
            {
                "port": "COM42",
                "baud": 115200,
                "usb_serial_number": "FTSNB6K9A",
                "sn_code": "00000000",
                "observed_protocol_device_id": "001",
                "candidate_target_sn": "01260716",
                "candidate_target_protocol_device_id": "016",
                "action": "initialize_sn_then_change_protocol_id_after_review",
            },
        ],
    }


def _inventory(*, sn_code: str = "01260716"):
    return {
        "analyzers": [
            {
                "port": "COM42",
                "usb_serial_number": "FTSNB6K9A",
                "sn_code": sn_code,
                "sn_bound_valid": sn_code != "00000000",
            }
        ]
    }


def _backup():
    return {
        "schema_version": tool.BACKUP_SCHEMA,
        "port": "COM42",
        "usb_serial_number": "FTSNB6K9A",
        "sn_code": "00000000",
        "protocol_device_id": "001",
        "identity_verified": True,
        "captured_read_only": True,
        "captured_before_persistent_identity_write": True,
        "getco_groups": {str(group): [0.0, 1.0] for group in range(1, 10)},
        "runtime_settings": {
            "mode": 1,
            "active_send": True,
            "ftd_hz": 1,
            "average1": 49,
            "average2": 49,
        },
        "safety": {
            "query_only": True,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_senco": False,
        },
    }


class _FakeSerial:
    def __init__(self, owner):
        self.owner = owner
        self.writes = []
        self._sn_pending = False

    def flush_input(self):
        return None

    def write(self, command):
        self.writes.append(str(command))
        if str(command).strip() == "SN,YGAS,FFF":
            self._sn_pending = True

    def readline(self):
        if self._sn_pending:
            self._sn_pending = False
            return f"YGAS,{self.owner.current_id},{self.owner.sn_code}"
        return ""

    def drain_input_nonblock(self, **_kwargs):
        return []


class _FakeAnalyzer:
    def __init__(self, candidate, *, ack=True, identity_sequence=None):
        self.candidate = dict(candidate)
        self.current_id = str(candidate["old_protocol_device_id"])
        self.sn_code = str(candidate["required_sn"])
        self.ack = ack
        self.identity_sequence = list(identity_sequence or [])
        self.ser = _FakeSerial(self)
        self.opened = False
        self.closed = False
        self.id_calls = []

    def open(self):
        self.opened = True

    def close(self):
        self.closed = True

    def read_current_mode_snapshot(self, **_kwargs):
        value = self.identity_sequence.pop(0) if self.identity_sequence else self.current_id
        return {"id": value, "mode": 1, "raw": f"YGAS,{value},..."}

    def set_device_id_with_ack(self, device_id, *, require_ack=True):
        assert require_ack is True
        target = str(device_id)
        self.id_calls.append(target)
        self.ser.write(f"ID,YGAS,FFF,{target}\r\n")
        if self.ack:
            self.current_id = target
        return self.ack

    def set_senco(self, *_args, **_kwargs):
        raise AssertionError("protocol identity writer must not write SENCO")


def test_unapproved_plan_is_dry_run_blocked_without_opening_com():
    preflight = tool.build_preflight(_plan(approved=False), _inventory(), None)
    factory_called = []

    result = tool.execute_controlled_write(
        preflight,
        execute=False,
        analyzer_factory=lambda candidate: factory_called.append(candidate),
    )

    assert result["status"] == "dry_run_blocked"
    assert "plan_execution_not_allowed" in preflight["blockers"]
    assert "prewrite_backup_missing" in preflight["blockers"]
    assert factory_called == []
    assert result["device_id_write_attempted"] is False


def test_preflight_requires_initialized_target_sn_and_complete_getco_backup(
    tmp_path: Path,
):
    backup = _backup()
    backup["getco_groups"].pop("9")
    preflight = tool.build_preflight(
        _plan(tmp_path), _inventory(sn_code="00000000"), backup
    )

    assert preflight["status"] == "blocked"
    assert "candidate_target_sn_not_initialized_or_mismatch" in preflight["blockers"]
    assert "prewrite_backup_getco1_9_incomplete" in preflight["blockers"]


def test_preflight_rejects_placeholder_runtime_settings(tmp_path: Path):
    backup = _backup()
    backup["runtime_settings"]["ftd_hz"] = None
    backup["runtime_settings"]["average1"] = ""
    preflight = tool.build_preflight(_plan(tmp_path), _inventory(), backup)

    assert preflight["status"] == "blocked"
    assert "prewrite_backup_runtime_settings_unverified" in preflight["blockers"]


def test_preflight_verifies_uniqueness_source_exists_and_hash_matches(tmp_path):
    missing_plan = _plan(tmp_path)
    missing_plan["global_uniqueness_evidence"]["source"] = str(
        tmp_path / "missing.json"
    )
    missing = tool.build_preflight(missing_plan, _inventory(), _backup())
    assert "global_uniqueness_evidence_source_unavailable" in missing["blockers"]

    mismatch_plan = _plan(tmp_path)
    mismatch_plan["global_uniqueness_evidence"]["sha256"] = "b" * 64
    mismatch = tool.build_preflight(mismatch_plan, _inventory(), _backup())
    assert "global_uniqueness_evidence_sha256_mismatch" in mismatch["blockers"]


def test_preflight_validates_uniqueness_source_semantics(tmp_path):
    cases = [
        (
            {"schema_version": "wrong"},
            "global_uniqueness_evidence_schema_invalid",
        ),
        (
            {"overall_status": "blocked_scope_incomplete"},
            "global_uniqueness_evidence_status_not_ready",
        ),
        (
            {"test_fixture_only": True},
            "global_uniqueness_evidence_test_fixture_forbidden",
        ),
        (
            {"scope_complete": False},
            "global_uniqueness_evidence_scope_incomplete",
        ),
        (
            {"candidate_sn_absent": False},
            "global_uniqueness_evidence_source_sn_absence_not_confirmed",
        ),
        (
            {"candidate_protocol_id_absent": False},
            "global_uniqueness_evidence_source_protocol_id_absence_not_confirmed",
        ),
        (
            {"candidate_sn": "01260799"},
            "global_uniqueness_evidence_candidate_sn_mismatch",
        ),
        (
            {"candidate_protocol_device_id": "099"},
            "global_uniqueness_evidence_candidate_protocol_id_mismatch",
        ),
    ]
    fixture = _authoritative_payload()
    for index, (updates, expected_blocker) in enumerate(cases):
        payload = {**fixture, **updates}
        path = tmp_path / f"uniqueness_{index}.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        plan = _plan(tmp_path)
        plan["global_uniqueness_evidence"].update(
            {
                "source": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )

        preflight = tool.build_preflight(plan, _inventory(), _backup())

        assert expected_blocker in preflight["blockers"]


def test_preflight_rejects_non_object_uniqueness_source(tmp_path):
    path = tmp_path / "uniqueness_list.json"
    path.write_text("[]", encoding="utf-8")
    plan = _plan(tmp_path)
    plan["global_uniqueness_evidence"].update(
        {
            "source": str(path),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
    )

    preflight = tool.build_preflight(plan, _inventory(), _backup())

    assert "global_uniqueness_evidence_source_json_invalid" in preflight["blockers"]


def test_ready_preflight_records_uniqueness_semantic_validation(tmp_path: Path):
    preflight = tool.build_preflight(_plan(tmp_path), _inventory(), _backup())

    assert preflight["status"] == "ready"
    validation = preflight["global_uniqueness_evidence_validation"]
    assert validation["source_available"] is True
    assert validation["sha256_matches"] is True
    assert validation["schema_valid"] is True
    assert validation["status_ready"] is True
    assert validation["scope_complete"] is True
    assert validation["candidate_sn_matches_plan"] is True
    assert validation["candidate_protocol_id_matches_plan"] is True
    assert validation["test_fixture_marker_explicit_false"] is True
    assert validation["test_fixture_path_forbidden"] is False
    assert validation["authority_valid"] is True
    assert validation["authority_records_valid"] is True
    assert validation["authority_sn_codes_unique"] is True
    assert validation["authority_protocol_ids_unique"] is True
    assert validation["derived_candidate_sn_absent"] is True
    assert validation["derived_candidate_protocol_id_absent"] is True
    assert validation["valid"] is True


def test_preflight_derives_absence_from_authoritative_asset_records(tmp_path):
    fixture = _authoritative_payload()
    cases = [
        (
            lambda payload: payload.pop("authority"),
            "global_uniqueness_evidence_authority_missing",
        ),
        (
            lambda payload: payload["authority"].update(
                {"source_type": "manual_note"}
            ),
            "global_uniqueness_evidence_authority_source_type_invalid",
        ),
        (
            lambda payload: payload["authority"].update(
                {"read_only_export": False}
            ),
            "global_uniqueness_evidence_authority_not_read_only",
        ),
        (
            lambda payload: payload["scope"].update(
                {"includes_unpowered_devices": False}
            ),
            "global_uniqueness_evidence_scope_includes_unpowered_devices_missing",
        ),
        (
            lambda payload: payload["scope"].update({"record_count": 99}),
            "global_uniqueness_evidence_scope_record_count_mismatch",
        ),
        (
            lambda payload: payload["records"][0].update(
                {"sn_code": "01260716"}
            ),
            "global_uniqueness_evidence_candidate_sn_present_in_authority_records",
        ),
        (
            lambda payload: payload["records"][0].update(
                {"protocol_device_id": "016"}
            ),
            "global_uniqueness_evidence_candidate_protocol_id_present_in_authority_records",
        ),
        (
            lambda payload: payload["records"][1].update(
                {"sn_code": payload["records"][0]["sn_code"]}
            ),
            "global_uniqueness_evidence_records_duplicate_sn_code",
        ),
        (
            lambda payload: payload["records"][1].update(
                {
                    "protocol_device_id": payload["records"][0][
                        "protocol_device_id"
                    ]
                }
            ),
            "global_uniqueness_evidence_records_duplicate_protocol_device_id",
        ),
    ]
    for index, (mutate, expected_blocker) in enumerate(cases):
        payload = json.loads(json.dumps(fixture))
        mutate(payload)
        path = tmp_path / f"authority_{index}.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        plan = _plan(tmp_path)
        plan["global_uniqueness_evidence"].update(
            {
                "source": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )

        preflight = tool.build_preflight(plan, _inventory(), _backup())

        assert expected_blocker in preflight["blockers"]


def test_preflight_rejects_missing_fixture_marker(tmp_path: Path):
    payload = _authoritative_payload()
    payload.pop("test_fixture_only")
    path = tmp_path / "missing_fixture_marker.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    plan = _plan(tmp_path)
    plan["global_uniqueness_evidence"].update(
        {
            "source": str(path),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
    )

    preflight = tool.build_preflight(plan, _inventory(), _backup())

    assert preflight["status"] == "blocked"
    assert "global_uniqueness_evidence_test_fixture_forbidden" in preflight["blockers"]


def test_preflight_rejects_repository_fixture_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    plan = _plan(tmp_path)
    plan["global_uniqueness_evidence"].update(
        {
            "source": str(UNIQUENESS_FIXTURE.resolve()),
            "sha256": hashlib.sha256(UNIQUENESS_FIXTURE.read_bytes()).hexdigest(),
        }
    )
    monkeypatch.setattr(tool, "_load_json", lambda _path: _authoritative_payload())

    preflight = tool.build_preflight(plan, _inventory(), _backup())

    assert preflight["status"] == "blocked"
    assert (
        "global_uniqueness_evidence_test_fixture_path_forbidden"
        in preflight["blockers"]
    )
    assert "global_uniqueness_evidence_test_fixture_forbidden" not in preflight["blockers"]


@pytest.mark.parametrize("port", ["COM34", "COM43"])
def test_preflight_rejects_port_outside_analyzer_bank(tmp_path: Path, port: str):
    plan = _plan(tmp_path)
    candidate = next(
        row
        for row in plan["rows"]
        if str(row["action"]).startswith("initialize_sn_then_change_protocol_id")
    )
    candidate["port"] = port
    inventory = _inventory()
    inventory["analyzers"][0]["port"] = port
    backup = _backup()
    backup["port"] = port

    preflight = tool.build_preflight(plan, inventory, backup)

    assert preflight["status"] == "blocked"
    assert "candidate_port_outside_analyzer_bank" in preflight["blockers"]


def test_hardware_identity_mismatch_blocks_before_analyzer_factory(tmp_path: Path):
    preflight = tool.build_preflight(_plan(tmp_path), _inventory(), _backup())
    factory_called = []

    result = tool.execute_controlled_write(
        preflight,
        execute=True,
        authorization_phrase=tool.AUTHORIZATION_PHRASE,
        isolation_phrase=tool.ISOLATION_PHRASE,
        hardware_serial_provider=lambda _port: "WRONG",
        analyzer_factory=lambda candidate: factory_called.append(candidate),
    )

    assert result["status"] == "blocked_hardware_identity_mismatch"
    assert factory_called == []


def test_replay_success_writes_one_id_and_verifies_two_readbacks(tmp_path: Path):
    preflight = tool.build_preflight(_plan(tmp_path), _inventory(), _backup())
    instances = []

    def factory(candidate):
        instance = _FakeAnalyzer(candidate)
        instances.append(instance)
        return instance

    sleeps = []
    result = tool.execute_controlled_write(
        preflight,
        execute=True,
        authorization_phrase=tool.AUTHORIZATION_PHRASE,
        isolation_phrase=tool.ISOLATION_PHRASE,
        hardware_serial_provider=lambda _port: "FTSNB6K9A",
        analyzer_factory=factory,
        sleep_fn=sleeps.append,
    )

    assert result["status"] == "success"
    assert result["device_id_write_attempted"] is True
    assert result["device_id_write_acknowledged"] is True
    assert result["postwrite_readback"]["protocol_device_ids"] == ["016", "016"]
    assert result["postwrite_readback"]["sn_code"] == "01260716"
    assert result["operator_confirmation_record"]["authorization_phrase_matched"] is True
    assert instances[0].id_calls == ["016"]
    assert instances[0].closed is True
    assert all(
        command.strip() in {"SN,YGAS,FFF", "ID,YGAS,FFF,016"}
        for command in instances[0].ser.writes
    )
    assert not any("SENCO" in command for command in instances[0].ser.writes)
    assert sleeps


def test_partial_new_id_observation_rolls_back_only_when_authorized(tmp_path: Path):
    preflight = tool.build_preflight(
        _plan(tmp_path, rollback_authorized=True), _inventory(), _backup()
    )
    instances = []

    def factory(candidate):
        instance = _FakeAnalyzer(
            candidate,
            identity_sequence=["001", "016", "001", "001", "001"],
        )
        instances.append(instance)
        return instance

    result = tool.execute_controlled_write(
        preflight,
        execute=True,
        authorization_phrase=tool.AUTHORIZATION_PHRASE,
        isolation_phrase=tool.ISOLATION_PHRASE,
        hardware_serial_provider=lambda _port: "FTSNB6K9A",
        analyzer_factory=factory,
        sleep_fn=lambda _seconds: None,
    )

    assert result["status"] == "failed_postwrite_verification_rolled_back"
    assert result["rollback_attempted"] is True
    assert result["rollback_confirmed"] is True
    assert instances[0].id_calls == ["016", "001"]


def test_missing_write_ack_is_identity_unknown_and_never_blindly_rolled_back(
    tmp_path: Path,
):
    preflight = tool.build_preflight(_plan(tmp_path), _inventory(), _backup())
    instances = []

    def factory(candidate):
        instance = _FakeAnalyzer(candidate, ack=False)
        instances.append(instance)
        return instance

    result = tool.execute_controlled_write(
        preflight,
        execute=True,
        authorization_phrase=tool.AUTHORIZATION_PHRASE,
        isolation_phrase=tool.ISOLATION_PHRASE,
        hardware_serial_provider=lambda _port: "FTSNB6K9A",
        analyzer_factory=factory,
        sleep_fn=lambda _seconds: None,
    )

    assert result["status"] == "failed_write_ack_unknown_manual_recovery_required"
    assert result["rollback_attempted"] is False
    assert instances[0].id_calls == ["016"]


def test_factory_failure_returns_evidence_instead_of_raising(tmp_path: Path):
    preflight = tool.build_preflight(_plan(tmp_path), _inventory(), _backup())

    def failing_factory(_candidate):
        raise RuntimeError("factory failed")

    result = tool.execute_controlled_write(
        preflight,
        execute=True,
        authorization_phrase=tool.AUTHORIZATION_PHRASE,
        isolation_phrase=tool.ISOLATION_PHRASE,
        hardware_serial_provider=lambda _port: "FTSNB6K9A",
        analyzer_factory=failing_factory,
    )

    assert result["status"] == "error_manual_recovery_required"
    assert result["error"] == "RuntimeError: factory failed"


def test_cli_dry_run_writes_blocked_result_for_g198_shape(tmp_path):
    plan_path = tmp_path / "plan.json"
    inventory_path = tmp_path / "inventory.json"
    output_path = tmp_path / "result.json"
    plan_path.write_text(json.dumps(_plan(approved=False)), encoding="utf-8")
    inventory_path.write_text(json.dumps(_inventory(sn_code="00000000")), encoding="utf-8")

    rc = tool.main(
        [
            "--plan-json",
            str(plan_path),
            "--sn-inventory-json",
            str(inventory_path),
            "--output-json",
            str(output_path),
        ]
    )

    assert rc == 1
    result = json.loads(output_path.read_text(encoding="utf-8"))
    assert result["status"] == "dry_run_blocked"
    assert result["execute"] is False
    assert result["device_id_write_attempted"] is False
