import json
from pathlib import Path

from gas_calibrator.tools.run_v1_5_formal_readonly_com_minimal_executor import main as executor_main
from gas_calibrator.validation.v1_5_formal_readonly_com_execution_packet_validator import (
    CONFIRMATION_TEMPLATE_ID,
    build_v1_5_formal_readonly_com_execution_packet_validator,
    write_v1_5_formal_readonly_com_execution_packet_validator_outputs,
)
from gas_calibrator.validation.v1_5_formal_readonly_com_execution_plan_preview import (
    build_v1_5_formal_readonly_com_execution_plan_preview,
    write_v1_5_formal_readonly_com_execution_plan_preview_outputs,
)
from gas_calibrator.validation.v1_5_formal_readonly_com_minimal_executor import (
    PySerialReadOnlyClient,
    build_v1_5_formal_readonly_com_minimal_executor,
    write_v1_5_formal_readonly_com_minimal_executor_outputs,
)
from gas_calibrator.validation.v1_5_formal_run_status import build_v1_5_formal_run_status


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _blocked_executor_json(tmp_path: Path) -> Path:
    return _write_json(
        tmp_path / "blocked" / "v1_5_formal_readonly_com_execution_blocked_executor.json",
        {
            "schema": "v1_5_formal_readonly_com_execution_blocked_executor_v1",
            "overall_status": "blocked_pending_readonly_com_real_executor_implementation",
            "blocked_executor_ready": True,
            "review_required_count": 0,
            "execution_supported": False,
            "live_execution_allowed": False,
            "read_only_real_com_execution_allowed": False,
            "controlled_write_execution_allowed": False,
            "real_com_execution_allowed": False,
            "execute_flag_allowed": False,
            "opens_com_ports": False,
            "connects_postgresql": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "database_written": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
            "minimum_serial_command_gap_s": 1.0,
        },
    )


def _authorization_json(tmp_path: Path) -> Path:
    return _write_json(
        tmp_path / "packet" / "authorization.json",
        {
            "authorization_id": "AUTH-READONLY-050",
            "requested_flag": "--execute-read-only-real-com",
            "operator": "operator-a",
            "reviewer": "reviewer-a",
            "approver": "approver-a",
            "operator_confirmation_text": "现场确认只读、不写入、端口已复核",
            "confirmation_template_id": CONFIRMATION_TEMPLATE_ID,
            "confirmation_fields": {
                "read_only": True,
                "no_write": True,
                "reviewed_ports": True,
                "no_senco_write": True,
                "no_database_import": True,
                "no_route_control": True,
            },
            "minimum_serial_command_gap_s": 1.0,
            "retry_gap_s": 1.0,
            "opens_com_ports": False,
            "connects_postgresql": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "database_written": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
        },
    )


def _alternate_authorization_json(tmp_path: Path, *, reviewer: str = "reviewer-b") -> Path:
    payload = json.loads(_authorization_json(tmp_path).read_text(encoding="utf-8"))
    payload["authorization_id"] = "AUTH-READONLY-ALT"
    payload["reviewer"] = reviewer
    return _write_json(tmp_path / "alternate_packet" / "authorization.json", payload)


def _reviewed_ports_json(tmp_path: Path, count: int) -> Path:
    return _write_json(
        tmp_path / "packet" / "reviewed_ports.json",
        {
            "schema": "v1_5_readonly_com_reviewed_port_inventory_v1",
            "reviewed_ports": [
                {"ga_label": f"GA{index:02d}", "port": f"COM{35 + index}"}
                for index in range(1, count + 1)
            ],
        },
    )


def _active_analyzers_json(
    tmp_path: Path,
    count: int,
    *,
    algorithm: str = "legacy_ratio",
    check_capable: bool = False,
    check_required: bool = False,
) -> Path:
    return _write_json(
        tmp_path / "packet" / "active_analyzers.json",
        {
            "schema": "v1_5_readonly_com_active_analyzer_list_v1",
            "active_analyzers": [
                {
                    "ga_label": f"GA{index:02d}",
                    "port": f"COM{35 + index}",
                    "protocol_device_id": f"{index:03d}",
                    "sn_code": f"012607{index:02d}",
                    "algorithm": algorithm,
                    "check_capable": check_capable,
                    "check_required": check_required,
                    "runtime_evidence": {
                        "ftd_hz": 1.0,
                        "average1": "AVERAGE1",
                        "average2": "AVERAGE2",
                        "filter": "reviewed_runtime_state",
                    },
                }
                for index in range(1, count + 1)
            ],
        },
    )


def _stub_json(tmp_path: Path) -> Path:
    return _write_json(
        tmp_path / "stub" / "v1_5_formal_readonly_com_minimal_executor_stub.json",
        {
            "schema": "v1_5_formal_readonly_com_minimal_executor_stub_v1",
            "overall_status": "blocked_plan_only_minimal_readonly_com_executor_stub",
            "minimal_executor_stub_ready": True,
            "would_execute_artifact_ready": True,
            "authorization_context_consumed_as_unlock": False,
            "execution_supported": False,
            "live_execution_allowed": False,
            "read_only_real_com_execution_allowed": False,
            "controlled_write_execution_allowed": False,
            "real_com_execution_allowed": False,
            "execute_flag_allowed": False,
            "opens_com_ports": False,
            "connects_postgresql": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "database_written": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
        },
    )


def _packet_and_plan(tmp_path: Path, *, count: int, active: Path, ports: Path) -> tuple[Path, Path]:
    packet = build_v1_5_formal_readonly_com_execution_packet_validator(
        formal_readonly_com_execution_blocked_executor_json=_blocked_executor_json(tmp_path),
        authorization_packet_json=_authorization_json(tmp_path),
        reviewed_port_inventory_json=ports,
        active_analyzer_list_json=active,
    )
    packet_outputs = write_v1_5_formal_readonly_com_execution_packet_validator_outputs(
        packet,
        tmp_path / "packet_validator",
    )
    plan = build_v1_5_formal_readonly_com_execution_plan_preview(
        formal_readonly_com_execution_packet_validator_json=packet_outputs["json"],
        reviewed_port_inventory_json=ports,
        active_analyzer_list_json=active,
    )
    plan_outputs = write_v1_5_formal_readonly_com_execution_plan_preview_outputs(
        plan,
        tmp_path / "plan_preview",
    )
    assert count == packet["active_analyzer_count"]
    return packet_outputs["json"], plan_outputs["json"]


class _FakeClient:
    def __init__(
        self,
        port: str,
        calls: list[tuple[str, str]],
        *,
        non_neutral: bool = False,
        active_frame_for_getco: bool = False,
        active_then_coefficient_for_getco: bool = False,
        malformed_getco: str = "",
    ) -> None:
        self.port = port
        self.calls = calls
        self.non_neutral = non_neutral
        self.active_frame_for_getco = active_frame_for_getco
        self.active_then_coefficient_for_getco = active_then_coefficient_for_getco
        self.malformed_getco = malformed_getco

    def _getco_response(self, command: str) -> str:
        group_index = command.rsplit(",", 1)[-1]
        group = f"GETCO{group_index}"
        if self.malformed_getco == "s5_gap" and group == "GETCO5":
            return "<C0:0,C2:1>"
        if self.malformed_getco == "s7_gap" and group == "GETCO7":
            return "<C0:0,C1:1,C3:0>"
        if self.malformed_getco == "s5_duplicate" and group == "GETCO5":
            return "<C0:0,C1:1,C1:1>"
        if group in {"GETCO5", "GETCO6"}:
            if self.non_neutral and group == "GETCO5":
                return "<C0:2,C1:1>"
            return "<C0:0,C1:1>"
        if group in {"GETCO7", "GETCO8"}:
            return "<C0:0,C1:1,C2:0,C3:0>"
        return "<C0:1,C1:2,C2:3,C3:4>"

    def query(self, command: str, *, timeout_s: float) -> str:
        self.calls.append((self.port, command))
        index = int(self.port.replace("COM", "")) - 35
        if command == "SN,YGAS,FFF":
            return f"SN,YGAS,FFF,012607{index:02d}"
        if command.startswith("GETCO,YGAS,FFF,"):
            if self.active_frame_for_getco:
                return "YGAS,001,0626.337,33.125,0.99,0.99,027.13,107.68,0301,2777"
            return self._getco_response(command)
        if command == "CHECK,YGAS,FFF":
            return "CHECK,YGAS,FFF,2.70,2.69,2.70,2.68"
        return "OK"

    def query_getco(self, command: str, *, timeout_s: float) -> str:
        self.calls.append((self.port, command))
        if self.active_frame_for_getco:
            return "YGAS,001,0626.337,33.125,0.99,0.99,027.13,107.68,0301,2777"
        if self.active_then_coefficient_for_getco:
            return self._getco_response(command)
        return self._getco_response(command)

    def close(self) -> None:
        return None


class _FakeSerial:
    def __init__(self, lines: list[str]) -> None:
        self.lines = [line.encode("utf-8") for line in lines]
        self.timeout = 1.0
        self.writes: list[bytes] = []

    def write(self, payload: bytes) -> None:
        self.writes.append(payload)

    def readline(self) -> bytes:
        if self.lines:
            return self.lines.pop(0)
        return b""


def test_pyserial_getco_query_scans_past_active_frame_to_coefficient_tokens() -> None:
    client = PySerialReadOnlyClient.__new__(PySerialReadOnlyClient)
    fake_serial = _FakeSerial(
        [
            "YGAS,001,0626.337,33.125,0.99,0.99,027.13,107.68,0301,2777\r\n",
            "<C0:0,C1:1>\r\n",
        ]
    )
    client._serial = fake_serial

    response = client.query_getco("GETCO,YGAS,FFF,5", timeout_s=0.2)

    assert response == "<C0:0,C1:1>"
    assert fake_serial.writes == [b"GETCO,YGAS,FFF,5\r\n"]


def test_minimal_executor_reads_six_legacy_devices_without_check(tmp_path: Path) -> None:
    ports = _reviewed_ports_json(tmp_path, 6)
    active = _active_analyzers_json(tmp_path, 6, algorithm="legacy_ratio")
    packet, plan = _packet_and_plan(tmp_path, count=6, active=active, ports=ports)
    stub = _stub_json(tmp_path)
    calls: list[tuple[str, str]] = []
    sleeps: list[float] = []

    model = build_v1_5_formal_readonly_com_minimal_executor(
        execute_read_only_real_com=True,
        authorization_packet_json=_authorization_json(tmp_path),
        reviewed_port_inventory_json=ports,
        active_analyzer_list_json=active,
        formal_readonly_com_execution_packet_validator_json=packet,
        formal_readonly_com_execution_plan_preview_json=plan,
        formal_readonly_com_minimal_executor_stub_json=stub,
        client_factory=lambda port: _FakeClient(port, calls),
        sleeper=sleeps.append,
    )

    assert model["overall_status"] == "readonly_com_minimal_executor_completed_no_write"
    assert model["execution_attempted"] is True
    assert model["opens_com_ports"] is True
    assert model["writes_sn"] is False
    assert model["writes_coefficients"] is False
    assert model["connects_postgresql"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["active_analyzer_count"] == 6
    assert len(calls) == 60
    assert all(gap >= 1.0 for gap in sleeps)
    assert all(command != "CHECK,YGAS,FFF" for _, command in calls)
    assert all(command != "GETCO5,YGAS,FFF" for _, command in calls)
    assert any(command == "GETCO,YGAS,FFF,5" for _, command in calls)
    assert all(row["auxiliary_neutrality"]["GETCO5"] == "neutral" for row in model["identity_getco_snapshots"])
    assert all(row["auxiliary_neutrality"]["GETCO6"] == "neutral" for row in model["identity_getco_snapshots"])
    assert all(row["auxiliary_neutrality"]["GETCO7"] == "neutral" for row in model["identity_getco_snapshots"])
    assert all(row["auxiliary_neutrality"]["GETCO8"] == "neutral" for row in model["identity_getco_snapshots"])


def test_minimal_executor_holds_legacy_check_capable_before_opening_com(tmp_path: Path) -> None:
    ports = _reviewed_ports_json(tmp_path, 1)
    active = _active_analyzers_json(
        tmp_path,
        1,
        algorithm="legacy_ratio",
        check_capable=True,
        check_required=False,
    )
    packet, plan = _packet_and_plan(tmp_path, count=1, active=active, ports=ports)
    calls: list[tuple[str, str]] = []

    model = build_v1_5_formal_readonly_com_minimal_executor(
        execute_read_only_real_com=True,
        authorization_packet_json=_authorization_json(tmp_path),
        reviewed_port_inventory_json=ports,
        active_analyzer_list_json=active,
        formal_readonly_com_execution_packet_validator_json=packet,
        formal_readonly_com_execution_plan_preview_json=plan,
        formal_readonly_com_minimal_executor_stub_json=_stub_json(tmp_path),
        client_factory=lambda port: _FakeClient(port, calls),
        sleeper=lambda seconds: None,
    )

    assert model["overall_status"] == "readonly_com_minimal_executor_hold"
    assert model["execution_attempted"] is False
    assert model["opens_com_ports"] is False
    assert calls == []
    assert any(
        row["reason"] == "active_1_old_algorithm_check_must_be_skipped"
        for row in model["hold_events"]
    )


def test_minimal_executor_holds_authorization_mismatch_before_opening_com(tmp_path: Path) -> None:
    ports = _reviewed_ports_json(tmp_path, 1)
    active = _active_analyzers_json(tmp_path, 1, algorithm="legacy_ratio")
    packet, plan = _packet_and_plan(tmp_path, count=1, active=active, ports=ports)
    calls: list[tuple[str, str]] = []

    model = build_v1_5_formal_readonly_com_minimal_executor(
        execute_read_only_real_com=True,
        authorization_packet_json=_alternate_authorization_json(tmp_path),
        reviewed_port_inventory_json=ports,
        active_analyzer_list_json=active,
        formal_readonly_com_execution_packet_validator_json=packet,
        formal_readonly_com_execution_plan_preview_json=plan,
        formal_readonly_com_minimal_executor_stub_json=_stub_json(tmp_path),
        client_factory=lambda port: _FakeClient(port, calls),
        sleeper=lambda seconds: None,
    )

    assert model["overall_status"] == "readonly_com_minimal_executor_hold"
    assert model["execution_attempted"] is False
    assert model["opens_com_ports"] is False
    assert calls == []
    assert any(
        row["reason"] == "authorization_packet_json_mismatch_with_packet_validator"
        for row in model["hold_events"]
    )


def test_minimal_executor_revalidates_authorization_shape_before_opening_com(tmp_path: Path) -> None:
    ports = _reviewed_ports_json(tmp_path, 1)
    active = _active_analyzers_json(tmp_path, 1, algorithm="legacy_ratio")
    packet, plan = _packet_and_plan(tmp_path, count=1, active=active, ports=ports)
    authorization = _authorization_json(tmp_path)
    payload = json.loads(authorization.read_text(encoding="utf-8"))
    payload["approver"] = payload["reviewer"]
    payload["minimum_serial_command_gap_s"] = 0.25
    authorization.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    calls: list[tuple[str, str]] = []

    model = build_v1_5_formal_readonly_com_minimal_executor(
        execute_read_only_real_com=True,
        authorization_packet_json=authorization,
        reviewed_port_inventory_json=ports,
        active_analyzer_list_json=active,
        formal_readonly_com_execution_packet_validator_json=packet,
        formal_readonly_com_execution_plan_preview_json=plan,
        formal_readonly_com_minimal_executor_stub_json=_stub_json(tmp_path),
        client_factory=lambda port: _FakeClient(port, calls),
        sleeper=lambda seconds: None,
    )

    reasons = {row["reason"] for row in model["hold_events"]}
    assert model["overall_status"] == "readonly_com_minimal_executor_hold"
    assert model["execution_attempted"] is False
    assert model["opens_com_ports"] is False
    assert calls == []
    assert "reviewer_and_approver_must_be_distinct" in reasons
    assert "minimum_serial_command_gap_s=0.25" in reasons


def test_minimal_executor_holds_non_neutral_s5_without_writing(tmp_path: Path) -> None:
    ports = _reviewed_ports_json(tmp_path, 1)
    active = _active_analyzers_json(tmp_path, 1, algorithm="legacy_ratio")
    packet, plan = _packet_and_plan(tmp_path, count=1, active=active, ports=ports)

    model = build_v1_5_formal_readonly_com_minimal_executor(
        execute_read_only_real_com=True,
        authorization_packet_json=_authorization_json(tmp_path),
        reviewed_port_inventory_json=ports,
        active_analyzer_list_json=active,
        formal_readonly_com_execution_packet_validator_json=packet,
        formal_readonly_com_execution_plan_preview_json=plan,
        formal_readonly_com_minimal_executor_stub_json=_stub_json(tmp_path),
        client_factory=lambda port: _FakeClient(port, [], non_neutral=True),
        sleeper=lambda seconds: None,
    )

    assert model["overall_status"] == "readonly_com_minimal_executor_hold"
    assert model["writes_coefficients"] is False
    assert any(row["reason"] == "getco5_non_neutral" for row in model["hold_events"])


def test_minimal_executor_rejects_active_ygas_frame_as_getco_response(tmp_path: Path) -> None:
    ports = _reviewed_ports_json(tmp_path, 1)
    active = _active_analyzers_json(tmp_path, 1, algorithm="legacy_ratio")
    packet, plan = _packet_and_plan(tmp_path, count=1, active=active, ports=ports)
    calls: list[tuple[str, str]] = []

    model = build_v1_5_formal_readonly_com_minimal_executor(
        execute_read_only_real_com=True,
        authorization_packet_json=_authorization_json(tmp_path),
        reviewed_port_inventory_json=ports,
        active_analyzer_list_json=active,
        formal_readonly_com_execution_packet_validator_json=packet,
        formal_readonly_com_execution_plan_preview_json=plan,
        formal_readonly_com_minimal_executor_stub_json=_stub_json(tmp_path),
        client_factory=lambda port: _FakeClient(port, calls, active_frame_for_getco=True),
        sleeper=lambda seconds: None,
    )

    reasons = {row["reason"] for row in model["hold_events"]}
    assert model["overall_status"] == "readonly_com_minimal_executor_hold"
    assert "getco5_parse_error" in reasons
    assert "getco6_parse_error" in reasons
    assert any(command == "GETCO,YGAS,FFF,5" for _, command in calls)
    assert all(command != "GETCO5,YGAS,FFF" for _, command in calls)


def test_minimal_executor_scans_past_active_ygas_frame_for_getco_coefficients(tmp_path: Path) -> None:
    ports = _reviewed_ports_json(tmp_path, 1)
    active = _active_analyzers_json(tmp_path, 1, algorithm="legacy_ratio")
    packet, plan = _packet_and_plan(tmp_path, count=1, active=active, ports=ports)
    calls: list[tuple[str, str]] = []

    model = build_v1_5_formal_readonly_com_minimal_executor(
        execute_read_only_real_com=True,
        authorization_packet_json=_authorization_json(tmp_path),
        reviewed_port_inventory_json=ports,
        active_analyzer_list_json=active,
        formal_readonly_com_execution_packet_validator_json=packet,
        formal_readonly_com_execution_plan_preview_json=plan,
        formal_readonly_com_minimal_executor_stub_json=_stub_json(tmp_path),
        client_factory=lambda port: _FakeClient(
            port,
            calls,
            active_then_coefficient_for_getco=True,
        ),
        sleeper=lambda seconds: None,
    )

    assert model["overall_status"] == "readonly_com_minimal_executor_completed_no_write"
    assert model["hold_count"] == 0
    assert any(command == "GETCO,YGAS,FFF,5" for _, command in calls)
    assert all(command != "GETCO5,YGAS,FFF" for _, command in calls)
    assert all(command != "CHECK,YGAS,FFF" for _, command in calls)
    snapshot = model["identity_getco_snapshots"][0]
    assert snapshot["auxiliary_neutrality"]["GETCO5"] == "neutral"
    assert snapshot["auxiliary_neutrality"]["GETCO6"] == "neutral"
    assert snapshot["auxiliary_neutrality"]["GETCO7"] == "neutral"
    assert snapshot["auxiliary_neutrality"]["GETCO8"] == "neutral"


def test_minimal_executor_rejects_malformed_getco_token_indexes(tmp_path: Path) -> None:
    ports = _reviewed_ports_json(tmp_path, 1)
    active = _active_analyzers_json(tmp_path, 1, algorithm="legacy_ratio")
    packet, plan = _packet_and_plan(tmp_path, count=1, active=active, ports=ports)

    for malformed, expected_reason in (
        ("s5_gap", "getco5_parse_error"),
        ("s7_gap", "getco7_parse_error"),
        ("s5_duplicate", "getco5_parse_error"),
    ):
        calls: list[tuple[str, str]] = []
        model = build_v1_5_formal_readonly_com_minimal_executor(
            execute_read_only_real_com=True,
            authorization_packet_json=_authorization_json(tmp_path),
            reviewed_port_inventory_json=ports,
            active_analyzer_list_json=active,
            formal_readonly_com_execution_packet_validator_json=packet,
            formal_readonly_com_execution_plan_preview_json=plan,
            formal_readonly_com_minimal_executor_stub_json=_stub_json(tmp_path),
            client_factory=lambda port, malformed=malformed: _FakeClient(port, calls, malformed_getco=malformed),
            sleeper=lambda seconds: None,
        )

        reasons = {row["reason"] for row in model["hold_events"]}
        assert model["overall_status"] == "readonly_com_minimal_executor_hold"
        assert expected_reason in reasons
        assert any(command == "GETCO,YGAS,FFF,5" for _, command in calls)


def test_minimal_executor_without_execute_flag_stays_locked_no_com(tmp_path: Path) -> None:
    ports = _reviewed_ports_json(tmp_path, 1)
    active = _active_analyzers_json(tmp_path, 1, algorithm="legacy_ratio")
    packet, plan = _packet_and_plan(tmp_path, count=1, active=active, ports=ports)

    model = build_v1_5_formal_readonly_com_minimal_executor(
        execute_read_only_real_com=False,
        authorization_packet_json=_authorization_json(tmp_path),
        reviewed_port_inventory_json=ports,
        active_analyzer_list_json=active,
        formal_readonly_com_execution_packet_validator_json=packet,
        formal_readonly_com_execution_plan_preview_json=plan,
        formal_readonly_com_minimal_executor_stub_json=_stub_json(tmp_path),
        client_factory=lambda port: _FakeClient(port, []),
        sleeper=lambda seconds: None,
    )

    assert model["overall_status"] == "blocked_missing_execute_readonly_real_com"
    assert model["execution_attempted"] is False
    assert model["opens_com_ports"] is False
    assert any(row["reason"] == "execute_read_only_real_com_flag_missing" for row in model["hold_events"])


def test_minimal_executor_writes_required_artifacts_and_cli_rejects_writes(tmp_path: Path) -> None:
    ports = _reviewed_ports_json(tmp_path, 1)
    active = _active_analyzers_json(tmp_path, 1, algorithm="legacy_ratio")
    packet, plan = _packet_and_plan(tmp_path, count=1, active=active, ports=ports)
    model = build_v1_5_formal_readonly_com_minimal_executor(
        execute_read_only_real_com=False,
        authorization_packet_json=_authorization_json(tmp_path),
        reviewed_port_inventory_json=ports,
        active_analyzer_list_json=active,
        formal_readonly_com_execution_packet_validator_json=packet,
        formal_readonly_com_execution_plan_preview_json=plan,
        formal_readonly_com_minimal_executor_stub_json=_stub_json(tmp_path),
        client_factory=lambda port: _FakeClient(port, []),
        sleeper=lambda seconds: None,
    )
    outputs = write_v1_5_formal_readonly_com_minimal_executor_outputs(model, tmp_path / "out")

    assert outputs["invocation_json"].exists()
    assert outputs["command_attempts_csv"].exists()
    assert outputs["raw_responses_csv"].exists()
    assert outputs["hold_events_csv"].exists()
    assert outputs["identity_getco_snapshot_json"].exists()
    exit_code = executor_main(
        [
            "--authorization-packet-json",
            str(_authorization_json(tmp_path)),
            "--reviewed-port-inventory-json",
            str(ports),
            "--active-analyzer-list-json",
            str(active),
            "--formal-readonly-com-execution-packet-validator-json",
            str(packet),
            "--formal-readonly-com-execution-plan-preview-json",
            str(plan),
            "--formal-readonly-com-minimal-executor-stub-json",
            str(_stub_json(tmp_path)),
            "--output-dir",
            str(tmp_path / "blocked_cli"),
            "--execute-controlled-writes",
        ]
    )
    assert exit_code == 2
    assert not (tmp_path / "blocked_cli").exists()


def test_formal_run_status_uses_executor_hold_as_physical_blocker(tmp_path: Path) -> None:
    executor_path = _write_json(
        tmp_path / "executor" / "v1_5_formal_readonly_com_minimal_executor.json",
        {
            "schema": "v1_5_formal_readonly_com_minimal_executor_v1",
            "overall_status": "readonly_com_minimal_executor_hold",
            "hold_count": 1,
            "connects_postgresql": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "database_written": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
        },
    )

    model = build_v1_5_formal_run_status(
        run_dir=tmp_path,
        formal_readonly_com_minimal_executor_json=executor_path,
    )

    gate = next(row for row in model["gates"] if row["gate_id"] == "formal_readonly_com_minimal_executor")
    assert gate["status"] == "review_required"
    assert gate["blocks_physical_flow"] is True
    assert model["can_continue_physical_flow"] is False
