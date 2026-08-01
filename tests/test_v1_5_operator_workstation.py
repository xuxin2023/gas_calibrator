import csv
import hashlib
import json
from pathlib import Path

import gas_calibrator.v1_5.orchestration.operator_workstation as workstation_module
import pytest
from gas_calibrator.tools.run_v1_5_operator_workstation_dry_run import main as cli_main
from gas_calibrator.v1_5.orchestration.operator_workstation import (
    ARCHIVE_AUTHORITY_CONFIRMATION_RECEIPT_SCHEMA,
    STARTUP_RECEIPT_SCHEMA,
    V1_5_ARCHIVE_AUTHORITY_CONFIRMATION_TEXT,
    V1_5_CONTROLLED_MATURE_ROUTE_AUTHORIZATION_TEXT,
    V1_5_ENGINEERING_PROBE_CONFIRMATION_TEXT,
    build_v1_5_archive_authority_confirmation_receipt,
    build_v1_5_controlled_route_preflight_receipt,
    build_v1_5_operator_workstation_plan,
    build_v1_5_operator_workstation_startup_receipt,
    build_v1_5_workstation_decision_model,
    execute_v1_5_controlled_mature_route,
    execute_v1_5_operator_workstation_dry_run,
    execute_v1_5_response_only_simulation,
    inspect_v1_5_runtime_config,
    load_v1_5_decision_authorities,
    run_v1_5_operator_workstation_application,
    write_v1_5_archive_authority_confirmation_receipt,
    write_v1_5_controlled_route_preflight_receipt,
    write_v1_5_operator_workstation_startup_receipt,
)
from v1_5_workstation_test_support import (
    write_csv_rows as _write_csv,
    write_decision_authority_archive,
    write_legacy_profile_queues as _legacy_queues,
)


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "configs" / "default_config.json"


def _controlled_route_plan(tmp_path: Path) -> dict:
    co2_queue, h2o_queue = _legacy_queues(tmp_path)
    return build_v1_5_operator_workstation_plan(
        config_path=CONFIG_PATH,
        co2_queue_csv=co2_queue,
        h2o_queue_csv=h2o_queue,
        output_dir=tmp_path / "controlled_workstation",
        run_id="controlled_route",
    )


def _route_hashes(plan: dict, route_kind: str) -> tuple[str, str]:
    route = next(
        row for row in plan["routes"] if row["route_kind"] == route_kind
    )
    return plan["runtime_config_inspection"]["sha256"], route["queue_csv_sha256"]


def _execute_controlled_route(
    plan: dict,
    route_kind: str,
    runner,
    *,
    execute: bool = True,
    authorization_text: str = V1_5_CONTROLLED_MATURE_ROUTE_AUTHORIZATION_TEXT,
    operator_confirmation_text: str = V1_5_ENGINEERING_PROBE_CONFIRMATION_TEXT,
) -> dict:
    runtime_sha, queue_sha = _route_hashes(plan, route_kind)
    return execute_v1_5_controlled_mature_route(
        plan,
        route_kind=route_kind,
        execute=execute,
        authorization_text=authorization_text,
        operator_confirmation_text=operator_confirmation_text,
        expected_runtime_config_sha256=runtime_sha,
        expected_queue_csv_sha256=queue_sha,
        runner_overrides={route_kind: runner},
    )


def test_operator_workstation_locks_mature_v1_5_and_keeps_certificate_non_blocking(
    tmp_path: Path,
) -> None:
    co2_queue, h2o_queue = _legacy_queues(tmp_path)
    corrupt_certificate = tmp_path / "certificate_metrics_registry.json"
    corrupt_certificate.write_text("{not-json", encoding="utf-8")

    plan = build_v1_5_operator_workstation_plan(
        config_path=CONFIG_PATH,
        co2_queue_csv=co2_queue,
        h2o_queue_csv=h2o_queue,
        output_dir=tmp_path / "workstation",
        run_id="six_device_parity",
        certificate_registry_json=corrupt_certificate,
    )

    assert plan["overall_status"] == "ready_for_v1_5_dry_run"
    assert plan["product_name"] == "V1.5 气体分析仪校准工作站"
    assert plan["calibration_kernel"] == "v1_5_legacy_ratio_0613_0620_0621"
    assert plan["profile_id"] == "legacy_ratio_production"
    assert plan["point_counts"] == {"co2": 45, "h2o": 13}
    assert plan["certificate_start_gate"] == "non_blocking"
    assert plan["warnings"] == ["certificate_registry_unreadable_non_blocking"]
    assert plan["v1_fallback_preserved"] is True
    assert plan["modifies_run_app"] is False
    assert plan["v2_role"] == "temporary_migration_and_deletion_pool_not_product_runtime"
    assert all("--dry-run" in row["argv"] for row in plan["routes"])
    assert all("--no-prompt" in row["argv"] for row in plan["routes"])
    assert all("--no-ftd-write" in row["argv"] for row in plan["routes"])
    handoff = plan["controlled_execution_handoff"]
    assert handoff["status"] == "blocked_pending_explicit_double_unlock"
    assert handoff["execution_allowed"] is False
    assert handoff["operator_confirmation_embedded"] is False
    assert handoff["uses_existing_mature_runners"] is True
    assert all("--dry-run" not in row["argv_template"] for row in handoff["commands"])
    assert all("--no-ftd-write" in row["argv_template"] for row in handoff["commands"])
    assert all("--engineering-probe-only" in row["argv_template"] for row in handoff["commands"])
    assert handoff["operator_confirmation_required_sha256"]
    assert all(row["queue_csv_sha256"] for row in handoff["commands"])
    assert handoff["default_scope"] == "response_only"
    response_scope = next(
        row
        for row in handoff["available_scopes"]
        if row["scope_id"] == "response_only"
    )
    assert response_scope["simulation_executor_available"] is True
    assert response_scope["real_execution_allowed"] is False
    assert response_scope["allowed_actions"] == [
        "passive_listen",
        "identity_query",
        "status_query",
    ]
    assert response_scope["controls_water_or_gas_routes"] is False
    assert response_scope["changes_analyzer_mode"] is False
    assert response_scope["runs_calibration_sampling"] is False
    decision_model = plan["decision_model"]
    assert decision_model["aggregate_status"] == "simulation_ready_real_locked"
    assert decision_model["can_start_simulation"] is True
    assert decision_model["can_start_real_execution"] is False
    assert decision_model["can_write_coefficients"] is False
    assert decision_model["can_issue_formal_certificate"] is False
    assert decision_model["decisions"]["start_simulation"]["authority"] == (
        "v1_5_operator_workstation_start_gate"
    )
    assert "formal_run_status_missing" in decision_model["decisions"][
        "write_coefficients"
    ]["reason_codes"]
    assert all(
        row["runner_confirmation_record_expectation"][
            "written_by_mature_runner_before_device_construction"
        ]
        is True
        for row in handoff["commands"]
    )


def test_response_only_simulation_accepts_only_read_contract(tmp_path: Path) -> None:
    co2_queue, h2o_queue = _legacy_queues(tmp_path)
    plan = build_v1_5_operator_workstation_plan(
        config_path=CONFIG_PATH,
        co2_queue_csv=co2_queue,
        h2o_queue_csv=h2o_queue,
        output_dir=tmp_path / "workstation",
        run_id="response_only_simulation",
    )
    seen: list[dict] = []

    def simulated_client(request):
        seen.append(dict(request))
        return {
            "ok": True,
            "protocol_device_id": request.get("expected_protocol_id"),
            "response_kind": "simulated_identity",
        }

    result = execute_v1_5_response_only_simulation(
        plan,
        [
            {
                "request_id": "ga01_identity",
                "device_key": "GA01",
                "port": "COM35",
                "action": "identity_query",
                "expected_protocol_id": "079",
            },
            {
                "request_id": "gauge_status",
                "device_key": "pressure_gauge",
                "port": "COM30",
                "action": "passive_listen",
            },
        ],
        client=simulated_client,
    )

    assert result["overall_status"] == "pass"
    assert result["execution_scope"] == "response_only"
    assert result["request_count"] == 2
    assert result["completed_request_count"] == 2
    assert len(seen) == 2
    assert result["evidence_source"] == "simulated"
    assert result["opens_com_ports"] is False
    assert result["controls_water_or_gas_routes"] is False
    assert result["changes_analyzer_mode"] is False
    assert result["runs_calibration_sampling"] is False
    assert result["writes_serial_configuration"] is False
    assert result["not_real_acceptance_evidence"] is True


def test_response_only_simulation_rejects_command_or_setpoint_before_client(
    tmp_path: Path,
) -> None:
    co2_queue, h2o_queue = _legacy_queues(tmp_path)
    plan = build_v1_5_operator_workstation_plan(
        config_path=CONFIG_PATH,
        co2_queue_csv=co2_queue,
        h2o_queue_csv=h2o_queue,
        output_dir=tmp_path / "workstation",
        run_id="response_only_reject_write_shape",
    )
    calls: list[dict] = []

    result = execute_v1_5_response_only_simulation(
        plan,
        [
            {
                "device_key": "pressure_controller",
                "port": "COM31",
                "action": "status_query",
                "command": "OUTP 1",
                "setpoint": 1000,
            }
        ],
        client=lambda request: calls.append(dict(request)) or {"ok": True},
    )

    assert result["overall_status"] == "blocked"
    assert result["execution_started"] is False
    assert result["request_results"] == []
    assert calls == []
    assert result["blockers"] == [
        "request_0_fields_not_response_only:command,setpoint"
    ]


def test_response_only_simulation_fails_closed_without_shared_handoff_scope() -> None:
    calls: list[dict] = []

    result = execute_v1_5_response_only_simulation(
        {
            "overall_status": "ready_for_v1_5_dry_run",
            "blockers": [],
            "controlled_execution_handoff": {"available_scopes": []},
        },
        [{"device_key": "GA01", "action": "passive_listen"}],
        client=lambda request: calls.append(dict(request)) or {"ok": True},
    )

    assert result["overall_status"] == "blocked"
    assert result["execution_started"] is False
    assert result["request_results"] == []
    assert calls == []
    assert result["blockers"] == [
        "response_only_scope_missing_from_controlled_handoff"
    ]


def test_unified_decision_model_requires_both_formal_authorities_for_issue(
    tmp_path: Path,
) -> None:
    co2_queue, h2o_queue = _legacy_queues(tmp_path)
    plan = build_v1_5_operator_workstation_plan(
        config_path=CONFIG_PATH,
        co2_queue_csv=co2_queue,
        h2o_queue_csv=h2o_queue,
        output_dir=tmp_path / "workstation",
        run_id="unified_decision_ready",
    )
    unlocked_plan = json.loads(json.dumps(plan))
    for scope in unlocked_plan["controlled_execution_handoff"]["available_scopes"]:
        if scope["scope_id"] == "response_only":
            scope["real_execution_allowed"] = True
    model = build_v1_5_workstation_decision_model(
        unlocked_plan,
        formal_run_status={
            "can_continue_physical_flow": True,
            "formal_release_allowed": True,
            "senco_artifact_authorization": {
                "controlled_write_authorization_ready": True,
            },
        },
        report_release_decision={"formal_issue_allowed": True},
    )

    assert model["aggregate_status"] == "formal_issue_ready"
    assert model["can_start_simulation"] is True
    assert model["can_start_real_execution"] is True
    assert model["can_write_coefficients"] is True
    assert model["can_issue_formal_certificate"] is True
    assert all(
        decision["reason_codes"] == ["all_required_gates_passed"]
        for key, decision in model["decisions"].items()
        if key != "start_simulation"
    )


def test_unified_decision_model_does_not_trust_report_decision_alone(
    tmp_path: Path,
) -> None:
    co2_queue, h2o_queue = _legacy_queues(tmp_path)
    plan = build_v1_5_operator_workstation_plan(
        config_path=CONFIG_PATH,
        co2_queue_csv=co2_queue,
        h2o_queue_csv=h2o_queue,
        output_dir=tmp_path / "workstation",
        run_id="unified_decision_fail_closed",
    )

    model = build_v1_5_workstation_decision_model(
        plan,
        report_release_decision={"formal_issue_allowed": True},
    )

    assert model["can_start_simulation"] is True
    assert model["can_start_real_execution"] is False
    assert model["can_write_coefficients"] is False
    assert model["can_issue_formal_certificate"] is False
    assert "formal_run_status_missing" in model["decisions"][
        "issue_formal_certificate"
    ]["reason_codes"]


def test_hash_bound_archive_loads_existing_authorities_without_unlocking_real_scope(
    tmp_path: Path,
) -> None:
    archive_path, _, _ = write_decision_authority_archive(tmp_path)
    loaded = load_v1_5_decision_authorities(
        archive_path,
        expected_run_id="formal_batch_001",
        expected_device_ids="001,002",
        expected_runtime_config_sha256=hashlib.sha256(
            CONFIG_PATH.read_bytes()
        ).hexdigest(),
    )

    assert loaded["status"] == "ready"
    assert loaded["blockers"] == []
    assert loaded["archive_index"]["sha256"]
    assert loaded["artifacts"]["formal_run_status"]["status"] == "bound"
    assert loaded["artifacts"]["report_model"]["status"] == "bound"
    assert loaded["artifacts"]["evidence_bundle"]["status"] == "bound"
    assert loaded["identity_binding"]["status"] == "ready"
    assert all(loaded["identity_binding"]["checks"].values())
    assert loaded["opens_com_ports"] is False
    assert loaded["writes_coefficients"] is False

    co2_queue, h2o_queue = _legacy_queues(tmp_path)
    plan = build_v1_5_operator_workstation_plan(
        config_path=CONFIG_PATH,
        co2_queue_csv=co2_queue,
        h2o_queue_csv=h2o_queue,
        output_dir=tmp_path / "workstation",
        run_id="hash_bound_authorities",
        decision_authority_archive_json=archive_path,
        expected_authority_run_id="formal_batch_001",
        expected_authority_device_ids="001,002",
    )

    assert plan["overall_status"] == "ready_for_v1_5_dry_run"
    assert plan["decision_authority_binding"]["status"] == "ready"
    assert "payloads" not in plan["decision_authority_binding"]
    assert plan["decision_model"]["can_start_simulation"] is True
    assert plan["decision_model"]["can_start_real_execution"] is False
    assert plan["decision_model"]["can_write_coefficients"] is True
    assert plan["decision_model"]["can_issue_formal_certificate"] is True
    receipt = build_v1_5_operator_workstation_startup_receipt(plan)
    assert receipt["decision_authority_binding"] == plan[
        "decision_authority_binding"
    ]
    assert receipt["opens_com_ports"] is False


def test_decision_authority_requires_independent_batch_confirmation(
    tmp_path: Path,
) -> None:
    archive_path, _, _ = write_decision_authority_archive(tmp_path)
    config_sha = hashlib.sha256(CONFIG_PATH.read_bytes()).hexdigest()

    missing_confirmation = load_v1_5_decision_authorities(
        archive_path,
        expected_runtime_config_sha256=config_sha,
    )
    assert missing_confirmation["status"] == "blocked"
    assert "decision_authority_expected_run_id_missing" in missing_confirmation[
        "blockers"
    ]
    assert "decision_authority_expected_device_ids_missing" in missing_confirmation[
        "blockers"
    ]

    wrong_run = load_v1_5_decision_authorities(
        archive_path,
        expected_run_id="another_batch",
        expected_device_ids="001,002",
        expected_runtime_config_sha256=config_sha,
    )
    assert "decision_authority_expected_run_id_mismatch" in wrong_run["blockers"]

    wrong_devices = load_v1_5_decision_authorities(
        archive_path,
        expected_run_id="formal_batch_001",
        expected_device_ids="001,003",
        expected_runtime_config_sha256=config_sha,
    )
    assert "decision_authority_expected_device_ids_mismatch" in wrong_devices[
        "blockers"
    ]


def test_decision_authority_blocks_config_drift_without_blocking_simulation(
    tmp_path: Path,
) -> None:
    archive_path, _, _ = write_decision_authority_archive(
        tmp_path,
        config_sha256="d" * 64,
    )
    co2_queue, h2o_queue = _legacy_queues(tmp_path)

    plan = build_v1_5_operator_workstation_plan(
        config_path=CONFIG_PATH,
        co2_queue_csv=co2_queue,
        h2o_queue_csv=h2o_queue,
        output_dir=tmp_path / "workstation",
        run_id="config_drift_authority",
        decision_authority_archive_json=archive_path,
        expected_authority_run_id="formal_batch_001",
        expected_authority_device_ids="001,002",
    )

    assert plan["overall_status"] == "ready_for_v1_5_dry_run"
    assert plan["decision_authority_binding"]["status"] == "blocked"
    assert "decision_authority_runtime_config_sha256_mismatch" in plan[
        "decision_authority_binding"
    ]["blockers"]
    assert plan["decision_model"]["can_start_simulation"] is True
    assert plan["decision_model"]["can_write_coefficients"] is False
    assert plan["decision_model"]["can_issue_formal_certificate"] is False


def test_decision_authority_rejects_mixed_internal_batch_sources(
    tmp_path: Path,
) -> None:
    archive_path, _, _ = write_decision_authority_archive(
        tmp_path,
        report_overrides={"run_id": "mixed_report_batch"},
    )

    loaded = load_v1_5_decision_authorities(
        archive_path,
        expected_run_id="formal_batch_001",
        expected_device_ids="001,002",
        expected_runtime_config_sha256=hashlib.sha256(
            CONFIG_PATH.read_bytes()
        ).hexdigest(),
    )

    assert loaded["status"] == "blocked"
    assert loaded["payloads"] == {}
    assert "decision_authority_run_id_source_mismatch" in loaded["blockers"]

    device_archive, _, _ = write_decision_authority_archive(
        tmp_path / "device_source_mismatch",
        formal_overrides={
            "senco_artifact_authorization": {
                "controlled_write_authorization_ready": True,
                "authorized_device_ids": ["001", "003"],
            }
        },
    )
    device_loaded = load_v1_5_decision_authorities(
        device_archive,
        expected_run_id="formal_batch_001",
        expected_device_ids="001,002",
        expected_runtime_config_sha256=hashlib.sha256(
            CONFIG_PATH.read_bytes()
        ).hexdigest(),
    )
    assert device_loaded["status"] == "blocked"
    assert "decision_authority_device_code_source_mismatch" in device_loaded[
        "blockers"
    ]


def test_decision_authority_bundle_fails_atomically_after_hash_tamper(
    tmp_path: Path,
) -> None:
    archive_path, _, report_path = write_decision_authority_archive(tmp_path)
    report_path.write_text("{}", encoding="utf-8")

    loaded = load_v1_5_decision_authorities(archive_path)

    assert loaded["status"] == "blocked"
    assert loaded["payloads"] == {}
    assert "decision_authority_report_model_sha256_mismatch" in loaded["blockers"]

    co2_queue, h2o_queue = _legacy_queues(tmp_path)
    plan = build_v1_5_operator_workstation_plan(
        config_path=CONFIG_PATH,
        co2_queue_csv=co2_queue,
        h2o_queue_csv=h2o_queue,
        output_dir=tmp_path / "workstation",
        run_id="tampered_authorities",
        decision_authority_archive_json=archive_path,
    )
    assert plan["overall_status"] == "ready_for_v1_5_dry_run"
    assert plan["decision_authority_binding"]["status"] == "blocked"
    assert plan["decision_model"]["can_start_simulation"] is True
    assert plan["decision_model"]["can_write_coefficients"] is False
    assert plan["decision_model"]["can_issue_formal_certificate"] is False


def test_decision_authority_rejects_simulated_or_malformed_release_evidence(
    tmp_path: Path,
) -> None:
    archive_path, _, _ = write_decision_authority_archive(
        tmp_path,
        report_overrides={
            "evidence_source": "simulated",
            "not_real_acceptance_evidence": True,
        },
    )

    loaded = load_v1_5_decision_authorities(archive_path)

    assert loaded["status"] == "blocked"
    assert loaded["payloads"] == {}
    assert (
        "decision_authority_report_model_simulated_evidence_forbidden"
        in loaded["blockers"]
    )


def test_decision_authority_distinguishes_optional_absence_from_broken_binding(
    tmp_path: Path,
) -> None:
    not_configured = load_v1_5_decision_authorities(None)
    assert not_configured["status"] == "not_configured"
    assert not_configured["blockers"] == []

    missing = load_v1_5_decision_authorities(tmp_path / "missing.json")
    assert missing["status"] == "blocked"
    assert missing["blockers"] == ["decision_authority_archive_index_missing"]

    archive_path, _, _ = write_decision_authority_archive(tmp_path)
    archive = json.loads(archive_path.read_text(encoding="utf-8"))
    archive["artifacts"] = [
        row
        for row in archive["artifacts"]
        if row["role"] != "report_report_model"
    ]
    archive_path.write_text(json.dumps(archive), encoding="utf-8")
    incomplete = load_v1_5_decision_authorities(archive_path)
    assert incomplete["status"] == "blocked"
    assert incomplete["payloads"] == {}
    assert "decision_authority_report_model_role_count_invalid:0" in incomplete[
        "blockers"
    ]


def test_decision_authority_rejects_non_boolean_formal_fields(tmp_path: Path) -> None:
    archive_path, _, _ = write_decision_authority_archive(
        tmp_path,
        formal_overrides={"formal_release_allowed": "yes"},
    )

    loaded = load_v1_5_decision_authorities(archive_path)

    assert loaded["status"] == "blocked"
    assert loaded["payloads"] == {}
    assert (
        "decision_authority_formal_run_status_shape_invalid"
        in loaded["blockers"]
    )


def test_startup_receipt_binds_inputs_but_keeps_operator_record_blank(
    tmp_path: Path,
) -> None:
    co2_queue, h2o_queue = _legacy_queues(tmp_path)
    plan = build_v1_5_operator_workstation_plan(
        config_path=CONFIG_PATH,
        co2_queue_csv=co2_queue,
        h2o_queue_csv=h2o_queue,
        output_dir=tmp_path / "workstation",
        run_id="receipt_review",
    )

    receipt = build_v1_5_operator_workstation_startup_receipt(plan)

    assert receipt["schema"] == STARTUP_RECEIPT_SCHEMA
    assert receipt["status"] == "startup_preflight_recorded_execution_locked"
    assert receipt["startup_gate_passed"] is True
    assert receipt["runtime_config"]["sha256"]
    assert receipt["queues"]["co2"]["sha256"]
    assert receipt["queues"]["h2o"]["sha256"]
    assert receipt["probe_scope_selected"] is False
    assert receipt["probe_execution_allowed"] is False
    assert receipt["operator_acknowledgement_template"]["completed"] is False
    assert receipt["operator_acknowledgement_template"]["operator_name"] == ""
    assert (
        receipt["operator_acknowledgement_template"]["execution_authorization"]
        is False
    )
    assert receipt["opens_com_ports"] is False
    assert receipt["not_real_acceptance_evidence"] is True
    assert receipt["decision_model"] == plan["decision_model"]


def test_startup_receipt_writer_is_immutable(tmp_path: Path) -> None:
    co2_queue, h2o_queue = _legacy_queues(tmp_path)
    plan = build_v1_5_operator_workstation_plan(
        config_path=CONFIG_PATH,
        co2_queue_csv=co2_queue,
        h2o_queue_csv=h2o_queue,
        output_dir=tmp_path / "workstation",
        run_id="immutable_receipt",
    )
    path = tmp_path / "receipt.json"

    written = write_v1_5_operator_workstation_startup_receipt(plan, path)

    assert written["path"] == str(path.resolve())
    assert written["sha256"]
    assert written["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert b"\r\n" not in path.read_bytes()
    assert written["probe_execution_allowed"] is False
    with pytest.raises(FileExistsError):
        write_v1_5_operator_workstation_startup_receipt(plan, path)


def test_archive_confirmation_receipt_binds_operator_and_identity_without_authority(
    tmp_path: Path,
) -> None:
    archive_path, _, _ = write_decision_authority_archive(tmp_path)
    co2_queue, h2o_queue = _legacy_queues(tmp_path)
    plan = build_v1_5_operator_workstation_plan(
        config_path=CONFIG_PATH,
        co2_queue_csv=co2_queue,
        h2o_queue_csv=h2o_queue,
        output_dir=tmp_path / "workstation",
        run_id="archive_confirmation",
        decision_authority_archive_json=archive_path,
        expected_authority_run_id="formal_batch_001",
        expected_authority_device_ids="001,002",
    )

    receipt = build_v1_5_archive_authority_confirmation_receipt(
        plan,
        operator_name="operator-a",
        confirmation_text=V1_5_ARCHIVE_AUTHORITY_CONFIRMATION_TEXT,
    )

    assert receipt["schema"] == ARCHIVE_AUTHORITY_CONFIRMATION_RECEIPT_SCHEMA
    assert receipt["status"] == "confirmed"
    assert receipt["blockers"] == []
    assert receipt["operator_confirmation"]["operator_name"] == "operator-a"
    assert receipt["operator_confirmation"]["confirmation_text_matches"] is True
    assert receipt["archive_selection"]["expected_identity"]["run_id"] == (
        "formal_batch_001"
    )
    assert receipt["archive_selection"]["expected_identity"]["device_ids"] == [
        "001",
        "002",
    ]
    assert all(receipt["archive_selection"]["identity_checks"].values())
    assert receipt["decision_model_at_confirmation"] == plan["decision_model"]
    assert receipt["formal_actions_unlocked_by_receipt"] is False
    assert receipt["probe_execution_allowed"] is False
    assert receipt["opens_com_ports"] is False
    assert receipt["writes_coefficients"] is False
    assert receipt["formal_certificate_issue_performed"] is False
    assert receipt["not_real_acceptance_evidence"] is True


@pytest.mark.parametrize(
    ("operator_name", "confirmation_text", "expected_blocker"),
    [
        (
            "",
            V1_5_ARCHIVE_AUTHORITY_CONFIRMATION_TEXT,
            "authority_confirmation_operator_missing",
        ),
        (
            "operator-a",
            "CONFIRM",
            "authority_confirmation_text_mismatch",
        ),
    ],
)
def test_archive_confirmation_receipt_fails_closed_on_human_confirmation(
    tmp_path: Path,
    operator_name: str,
    confirmation_text: str,
    expected_blocker: str,
) -> None:
    archive_path, _, _ = write_decision_authority_archive(tmp_path)
    co2_queue, h2o_queue = _legacy_queues(tmp_path)
    plan = build_v1_5_operator_workstation_plan(
        config_path=CONFIG_PATH,
        co2_queue_csv=co2_queue,
        h2o_queue_csv=h2o_queue,
        output_dir=tmp_path / "workstation",
        run_id="blocked_archive_confirmation",
        decision_authority_archive_json=archive_path,
        expected_authority_run_id="formal_batch_001",
        expected_authority_device_ids="001,002",
    )

    receipt = build_v1_5_archive_authority_confirmation_receipt(
        plan,
        operator_name=operator_name,
        confirmation_text=confirmation_text,
    )

    assert receipt["status"] == "blocked"
    assert expected_blocker in receipt["blockers"]
    assert receipt["formal_actions_unlocked_by_receipt"] is False


def test_archive_confirmation_receipt_fails_closed_without_bound_archive(
    tmp_path: Path,
) -> None:
    co2_queue, h2o_queue = _legacy_queues(tmp_path)
    plan = build_v1_5_operator_workstation_plan(
        config_path=CONFIG_PATH,
        co2_queue_csv=co2_queue,
        h2o_queue_csv=h2o_queue,
        output_dir=tmp_path / "workstation",
        run_id="missing_archive_confirmation",
    )

    receipt = build_v1_5_archive_authority_confirmation_receipt(
        plan,
        operator_name="operator-a",
        confirmation_text=V1_5_ARCHIVE_AUTHORITY_CONFIRMATION_TEXT,
    )

    assert receipt["status"] == "blocked"
    assert "authority_confirmation_binding_not_ready" in receipt["blockers"]
    assert "authority_confirmation_identity_not_ready" in receipt["blockers"]
    assert receipt["opens_com_ports"] is False


def test_archive_confirmation_receipt_rehashes_sources_at_confirmation_time(
    tmp_path: Path,
) -> None:
    archive_path, _, report_path = write_decision_authority_archive(tmp_path)
    co2_queue, h2o_queue = _legacy_queues(tmp_path)
    plan = build_v1_5_operator_workstation_plan(
        config_path=CONFIG_PATH,
        co2_queue_csv=co2_queue,
        h2o_queue_csv=h2o_queue,
        output_dir=tmp_path / "workstation",
        run_id="rehash_archive_confirmation",
        decision_authority_archive_json=archive_path,
        expected_authority_run_id="formal_batch_001",
        expected_authority_device_ids="001,002",
    )
    assert plan["decision_authority_binding"]["status"] == "ready"
    report_path.write_text("{}", encoding="utf-8")

    receipt = build_v1_5_archive_authority_confirmation_receipt(
        plan,
        operator_name="operator-a",
        confirmation_text=V1_5_ARCHIVE_AUTHORITY_CONFIRMATION_TEXT,
    )

    assert receipt["status"] == "blocked"
    assert (
        "authority_confirmation_report_model_hash_binding_invalid"
        in receipt["blockers"]
    )
    assert receipt["formal_actions_unlocked_by_receipt"] is False


def test_archive_confirmation_receipt_writer_is_immutable(tmp_path: Path) -> None:
    archive_path, _, _ = write_decision_authority_archive(tmp_path)
    co2_queue, h2o_queue = _legacy_queues(tmp_path)
    plan = build_v1_5_operator_workstation_plan(
        config_path=CONFIG_PATH,
        co2_queue_csv=co2_queue,
        h2o_queue_csv=h2o_queue,
        output_dir=tmp_path / "workstation",
        run_id="immutable_archive_confirmation",
        decision_authority_archive_json=archive_path,
        expected_authority_run_id="formal_batch_001",
        expected_authority_device_ids="001,002",
    )
    path = tmp_path / "archive_confirmation.json"

    written = write_v1_5_archive_authority_confirmation_receipt(
        plan,
        path,
        operator_name="operator-a",
        confirmation_text=V1_5_ARCHIVE_AUTHORITY_CONFIRMATION_TEXT,
    )

    assert written["path"] == str(path.resolve())
    assert written["status"] == "confirmed"
    assert written["confirmation_valid"] is True
    assert written["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert b"\r\n" not in path.read_bytes()
    with pytest.raises(FileExistsError):
        write_v1_5_archive_authority_confirmation_receipt(
            plan,
            path,
            operator_name="operator-a",
            confirmation_text=V1_5_ARCHIVE_AUTHORITY_CONFIRMATION_TEXT,
        )


def test_operator_workstation_blocks_point_count_drift_before_runner_execution(
    tmp_path: Path,
) -> None:
    co2_queue, h2o_queue = _legacy_queues(tmp_path)
    rows = list(csv.DictReader(co2_queue.open(encoding="utf-8-sig", newline="")))
    _write_csv(co2_queue, rows[:-1])

    plan = build_v1_5_operator_workstation_plan(
        config_path=CONFIG_PATH,
        co2_queue_csv=co2_queue,
        h2o_queue_csv=h2o_queue,
        output_dir=tmp_path / "workstation",
        run_id="count_drift",
    )
    result = execute_v1_5_operator_workstation_dry_run(plan)

    assert plan["overall_status"] == "blocked"
    assert "co2_legacy_point_count_mismatch:expected=45,observed=44" in plan["blockers"]
    assert result["execution_started"] is False
    assert result["route_results"] == []


def test_controlled_mature_route_preflight_remains_execution_locked(
    tmp_path: Path,
) -> None:
    plan = _controlled_route_plan(tmp_path)
    calls: list[list[str]] = []

    result = _execute_controlled_route(
        plan,
        "co2",
        lambda argv: calls.append(list(argv)) or 0,
        execute=False,
    )

    assert result["overall_status"] == "ready"
    assert result["status"] == "preflight_ready_execution_locked"
    assert result["preflight_passed"] is True
    assert result["execution_started"] is False
    assert result["execution_allowed"] is False
    assert result["runner_invocation_count"] == 0
    assert result["blockers"] == []
    assert calls == []


@pytest.mark.parametrize("route_kind", ["co2", "h2o"])
def test_controlled_mature_route_invokes_one_existing_runner_once(
    tmp_path: Path,
    route_kind: str,
) -> None:
    plan = _controlled_route_plan(tmp_path)
    calls: list[list[str]] = []

    result = _execute_controlled_route(
        plan,
        route_kind,
        lambda argv: calls.append(list(argv)) or 0,
    )

    assert result["overall_status"] == "pass"
    assert result["status"] == "completed"
    assert result["execution_started"] is True
    assert result["runner_invocation_count"] == 1
    assert result["automatic_retry_count"] == 0
    assert result["engineering_probe_only"] is True
    assert result["promotion_state"] == "blocked"
    assert result["not_real_acceptance_evidence"] is True
    assert result["no_write"] is True
    assert result["writes_coefficients"] is False
    assert result["writes_senco"] is False
    assert result["writes_device_id"] is False
    assert len(calls) == 1
    argv = calls[0]
    assert "--dry-run" not in argv
    assert "--no-prompt" in argv
    assert "--no-ftd-write" in argv
    assert "--engineering-probe-only" in argv
    assert "--operator-confirmation" in argv
    assert V1_5_ENGINEERING_PROBE_CONFIRMATION_TEXT in argv
    assert "<OPERATOR_CONFIRMATION_REQUIRED_AT_EXECUTION>" not in argv


@pytest.mark.parametrize(
    ("authorization_text", "operator_confirmation_text", "blocker"),
    [
        (
            "wrong application authorization",
            V1_5_ENGINEERING_PROBE_CONFIRMATION_TEXT,
            "application_authorization_text_mismatch",
        ),
        (
            V1_5_CONTROLLED_MATURE_ROUTE_AUTHORIZATION_TEXT,
            "wrong runner confirmation",
            "runner_operator_confirmation_text_mismatch",
        ),
    ],
)
def test_controlled_mature_route_blocks_either_missing_unlock_before_runner(
    tmp_path: Path,
    authorization_text: str,
    operator_confirmation_text: str,
    blocker: str,
) -> None:
    plan = _controlled_route_plan(tmp_path)
    calls: list[list[str]] = []

    result = _execute_controlled_route(
        plan,
        "co2",
        lambda argv: calls.append(list(argv)) or 0,
        authorization_text=authorization_text,
        operator_confirmation_text=operator_confirmation_text,
    )

    assert result["overall_status"] == "blocked"
    assert blocker in result["blockers"]
    assert result["execution_started"] is False
    assert result["runner_invocation_count"] == 0
    assert calls == []


def test_controlled_mature_route_blocks_queue_drift_and_argv_mutation(
    tmp_path: Path,
) -> None:
    plan = _controlled_route_plan(tmp_path)
    route = next(row for row in plan["routes"] if row["route_kind"] == "h2o")
    Path(route["queue_csv"]).write_text("tampered\n", encoding="utf-8")
    calls: list[list[str]] = []

    drift = _execute_controlled_route(
        plan,
        "h2o",
        lambda argv: calls.append(list(argv)) or 0,
    )

    assert "queue_csv_sha256_binding_mismatch" in drift["blockers"]
    assert drift["execution_started"] is False
    assert calls == []

    fresh_plan = _controlled_route_plan(tmp_path / "fresh")
    command = next(
        row
        for row in fresh_plan["controlled_execution_handoff"]["commands"]
        if row["route_kind"] == "h2o"
    )
    command["argv_template"].append("--allow-ftd-write")
    mutated = _execute_controlled_route(
        fresh_plan,
        "h2o",
        lambda argv: calls.append(list(argv)) or 0,
    )

    assert "selected_route_argv_not_canonical" in mutated["blockers"]
    assert mutated["execution_started"] is False
    assert calls == []


@pytest.mark.parametrize("runner_outcome", [9, RuntimeError("synthetic failure")])
def test_controlled_mature_route_never_retries_runner_failure(
    tmp_path: Path,
    runner_outcome: int | Exception,
) -> None:
    plan = _controlled_route_plan(tmp_path)
    calls: list[list[str]] = []

    def fake_runner(argv):
        calls.append(list(argv))
        if isinstance(runner_outcome, Exception):
            raise runner_outcome
        return runner_outcome

    result = _execute_controlled_route(
        plan,
        "co2",
        fake_runner,
    )

    assert result["overall_status"] == "failed"
    assert result["runner_invocation_count"] == 1
    assert result["automatic_retry_count"] == 0
    assert len(calls) == 1


def test_controlled_route_preflight_receipt_is_locked_and_immutable(
    tmp_path: Path,
) -> None:
    plan = _controlled_route_plan(tmp_path)
    preflight = _execute_controlled_route(
        plan,
        "co2",
        lambda _argv: 0,
        execute=False,
    )
    receipt = build_v1_5_controlled_route_preflight_receipt(plan, preflight)

    assert receipt["status"] == "preflight_recorded_execution_locked"
    assert receipt["preflight_passed"] is True
    assert all(receipt["checks"].values())
    assert receipt["route_binding"]["route_kind"] == "co2"
    assert receipt["route_binding"]["queue_csv"]["point_count"] == 45
    assert receipt["preflight_result"]["runner_invocation_count"] == 0
    assert receipt["real_execution_authorized"] is False
    assert receipt["opens_com_ports"] is False
    assert receipt["writes_coefficients"] is False
    assert receipt["not_real_acceptance_evidence"] is True

    output_path = tmp_path / "receipts" / "co2_preflight.json"
    written = write_v1_5_controlled_route_preflight_receipt(
        plan,
        preflight,
        output_path,
    )
    original = output_path.read_bytes()
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["sha256"] == hashlib.sha256(original).hexdigest()
    assert written["real_execution_authorized"] is False
    assert payload["status"] == "preflight_recorded_execution_locked"

    with pytest.raises(FileExistsError):
        write_v1_5_controlled_route_preflight_receipt(
            plan,
            preflight,
            output_path,
        )
    assert output_path.read_bytes() == original


def test_controlled_route_preflight_receipt_detects_post_preflight_queue_drift(
    tmp_path: Path,
) -> None:
    plan = _controlled_route_plan(tmp_path)
    preflight = _execute_controlled_route(
        plan,
        "h2o",
        lambda _argv: 0,
        execute=False,
    )
    route = next(row for row in plan["routes"] if row["route_kind"] == "h2o")
    Path(route["queue_csv"]).write_text("drifted\n", encoding="utf-8")

    receipt = build_v1_5_controlled_route_preflight_receipt(plan, preflight)

    assert receipt["status"] == "blocked_preflight_recorded_execution_locked"
    assert receipt["preflight_passed"] is False
    assert receipt["checks"]["queue_hash_still_bound"] is False
    assert "preflight_receipt_check_failed:queue_hash_still_bound" in receipt[
        "blockers"
    ]
    assert receipt["real_execution_authorized"] is False
    assert receipt["promotion_state"] == "blocked"


def test_runtime_config_gate_accepts_unique_protocol_bound_pressure_ports(
    tmp_path: Path,
) -> None:
    payload = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    payload["devices"]["pressure_controller"].update(
        {
            "configured_port": "COM23",
            "port": "COM31",
            "runtime_port": "COM31",
            "runtime_port_binding_source": "v1_5_reference_bank_shift_protocol_identity",
            "runtime_port_binding_frozen": True,
        }
    )
    payload["devices"]["pressure_gauge"].update(
        {
            "configured_port": "COM22",
            "port": "COM30",
            "runtime_port": "COM30",
            "runtime_port_binding_source": "v1_5_reference_bank_shift_protocol_identity",
            "runtime_port_binding_frozen": True,
        }
    )
    payload["devices"]["dewpoint_meter"].update(
        {
            "configured_port": "COM17",
            "port": "COM25",
            "runtime_port": "COM25",
            "runtime_port_binding_source": "v1_5_reference_bank_shift",
            "runtime_port_binding_frozen": True,
        }
    )
    payload["v1_5_serial_port_binding"] = {
        "enabled": True,
        "available_ports": ["COM22", "COM23", "COM30", "COM31"],
        "changed_count": 3,
        "blocked_count": 0,
        "gas_analyzer_ports_protected": True,
        "require_protocol_match": True,
    }
    config_path = tmp_path / "runtime_bound_config.json"
    config_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    inspection = inspect_v1_5_runtime_config(config_path)

    assert inspection["status"] == "ready_bound_runtime_config"
    assert inspection["blockers"] == []
    assert len(inspection["sha256"]) == 64
    assert inspection["pressure_devices"]["pressure_controller"]["runtime_port"] == "COM31"
    assert inspection["pressure_devices"]["pressure_gauge"]["runtime_port"] == "COM30"
    assert inspection["reference_devices"]["dewpoint_meter"]["runtime_port"] == "COM25"
    assert inspection["opens_com_ports"] is False
    assert inspection["writes_config"] is False


def test_runtime_config_gate_blocks_dual_bank_mapping_without_unique_identity(
    tmp_path: Path,
) -> None:
    payload = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    payload["devices"]["pressure_controller"].update(
        {
            "configured_port": "COM23",
            "port": "COM31",
            "runtime_port_binding_source": "v1_5_reference_bank_shift",
            "runtime_port_binding_frozen": True,
        }
    )
    payload["v1_5_serial_port_binding"] = {
        "enabled": True,
        "available_ports": ["COM23", "COM31"],
        "changed_count": 1,
        "blocked_count": 0,
        "gas_analyzer_ports_protected": True,
        "require_protocol_match": False,
    }
    config_path = tmp_path / "unsafe_bound_config.json"
    config_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    inspection = inspect_v1_5_runtime_config(config_path)

    assert inspection["status"] == "blocked"
    assert (
        "pressure_controller_dual_bank_unique_protocol_identity_missing"
        in inspection["blockers"]
    )


def test_application_service_executes_once_and_writes_once(
    tmp_path: Path,
    monkeypatch,
) -> None:
    executor_calls: list[dict] = []
    writer_calls: list[tuple[dict, Path]] = []
    plan = {
        "overall_status": "ready_for_v1_5_dry_run",
        "point_counts": {"co2": 45, "h2o": 13},
    }

    def fake_executor(payload):
        executor_calls.append(dict(payload))
        return {**dict(payload), "overall_status": "pass"}

    def fake_writer(result, output_dir):
        root = Path(output_dir)
        writer_calls.append((dict(result), root))
        return {
            "json": root / "v1_5_operator_workstation_dry_run.json",
            "markdown": root / "V1_5_OPERATOR_WORKSTATION_DRY_RUN.md",
        }

    monkeypatch.setattr(
        workstation_module,
        "write_v1_5_operator_workstation_outputs",
        fake_writer,
    )

    result, outputs = run_v1_5_operator_workstation_application(
        plan,
        output_dir=tmp_path,
        executor=fake_executor,
    )

    assert len(executor_calls) == 1
    assert len(writer_calls) == 1
    assert writer_calls[0][0] == result
    assert writer_calls[0][1] == tmp_path
    assert outputs["json"].parent == tmp_path
    assert result["overall_status"] == "pass"


def test_application_service_does_not_write_after_executor_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    writer_calls: list[dict] = []

    def failed_executor(_payload):
        raise RuntimeError("synthetic executor failure")

    def fake_writer(result, _output_dir):
        writer_calls.append(dict(result))
        return {}

    monkeypatch.setattr(
        workstation_module,
        "write_v1_5_operator_workstation_outputs",
        fake_writer,
    )

    with pytest.raises(RuntimeError, match="synthetic executor failure"):
        run_v1_5_operator_workstation_application(
            {"overall_status": "ready_for_v1_5_dry_run"},
            output_dir=tmp_path,
            executor=failed_executor,
        )

    assert writer_calls == []


def test_operator_workstation_cli_executes_both_mature_runner_dry_run_branches(
    tmp_path: Path,
) -> None:
    co2_queue, h2o_queue = _legacy_queues(tmp_path)
    output_dir = tmp_path / "workstation"

    rc = cli_main(
        [
            "--config",
            str(CONFIG_PATH),
            "--co2-queue-csv",
            str(co2_queue),
            "--h2o-queue-csv",
            str(h2o_queue),
            "--output-dir",
            str(output_dir),
            "--run-id",
            "end_to_end",
        ]
    )

    assert rc == 0
    result = json.loads(
        (output_dir / "v1_5_operator_workstation_dry_run.json").read_text(
            encoding="utf-8-sig"
        )
    )
    assert result["overall_status"] == "pass"
    assert result["point_counts"] == {"co2": 45, "h2o": 13}
    assert [(row["route_kind"], row["dry_run_points"]) for row in result["route_results"]] == [
        ("co2", 45),
        ("h2o", 13),
    ]
    assert all(row["status"] == "pass" for row in result["route_results"])
    assert result["opens_com_ports"] is False
    assert result["writes_coefficients"] is False
    assert (output_dir / "V1_5_OPERATOR_WORKSTATION_DRY_RUN.md").exists()
