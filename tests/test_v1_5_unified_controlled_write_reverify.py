import json
from pathlib import Path

import pytest

from gas_calibrator.tools.export_v1_5_unified_controlled_write_reverify import main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_unified_controlled_write_reverify import (
    APPROVED_CANDIDATE_SCHEMA,
    AUTHORIZATION_SCHEMA,
    CONFIRMATION_TEXT,
    SCHEMA,
    SNAPSHOT_SCHEMA,
    build_v1_5_unified_controlled_write_reverify,
    write_v1_5_unified_controlled_write_reverify,
)


ROOT = Path(__file__).resolve().parents[1]
CHECKED_MATRIX = (
    ROOT
    / "docs"
    / "v1_5_flow_contract"
    / "production_component_qc_fit_matrix"
    / "v1_5_production_component_qc_fit_matrix.json"
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _sha(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


def _approved_packet(tmp_path: Path, matrix: Path, *, profile: str = "legacy_ratio_production"):
    packet = _write_json(
        tmp_path / "candidate.json",
        {
            "schema": APPROVED_CANDIDATE_SCHEMA,
            "review_status": "approved_for_unified_controlled_write_review",
            "algorithm_profile_id": profile,
            "fit_matrix_sha256": _sha(matrix),
            "device_candidates": [
                {
                    "device_id": "001",
                    "sn_code": "01260701",
                    "groups": [
                        {"group": "SENCO1", "candidate_status": "approved", "candidate_values": [1, 2, 3, 4, 0, 0]},
                        {"group": "SENCO3", "candidate_status": "approved", "candidate_values": [5, 6, 7, 8, 0, 0]},
                        {"group": "SENCO2", "candidate_status": "approved", "candidate_values": [9, 10, 11, 12, 0, 0]},
                        {"group": "SENCO4", "candidate_status": "approved", "candidate_values": [13, 14, 15, 16, 0, 0]},
                        {"group": "SENCO5", "candidate_status": "approved", "candidate_layer_values": [2, 0.5]},
                        {"group": "SENCO6", "candidate_status": "approved", "candidate_layer_values": [1.5, 0.75]},
                        {"group": "SENCO9", "candidate_status": "approved", "candidate_values": [-1.25, 1, 0, 0], "model_kind": "offset_only"},
                    ],
                }
            ],
        },
    )
    snapshot = _write_json(
        tmp_path / "snapshot.json",
        {
            "schema": SNAPSHOT_SCHEMA,
            "devices": [
                {
                    "device_id": "001",
                    "sn_code": "01260701",
                    "values": {
                        "GETCO1": [0, 1, 0, 0, 0, 0],
                        "GETCO2": [0, 1, 0, 0, 0, 0],
                        "GETCO3": [0, 1, 0, 0, 0, 0],
                        "GETCO4": [0, 1, 0, 0, 0, 0],
                        "GETCO5": [10, 0.8],
                        "GETCO6": [2, 0.6],
                        "GETCO7": [0, 1, 0, 0],
                        "GETCO8": [0, 1, 0, 0],
                        "GETCO9": [0, 1, 0, 0],
                    },
                }
            ],
        },
    )
    authorization = _write_json(
        tmp_path / "authorization.json",
        {
            "schema": AUTHORIZATION_SCHEMA,
            "authorization_id": "auth-test",
            "confirmation_text": CONFIRMATION_TEXT,
            "operator": "operator-a",
            "reviewer": "reviewer-a",
            "approver": "approver-b",
            "fit_matrix_sha256": _sha(matrix),
            "candidate_packet_sha256": _sha(packet),
            "device_ids": ["001"],
            "minimum_serial_command_gap_s": 1.0,
            "no_sn_write": True,
            "no_device_id_write": True,
            "no_postgresql": True,
            "no_route_control": True,
        },
    )
    return packet, snapshot, authorization


def _ready_matrix(tmp_path: Path) -> Path:
    return _write_json(
        tmp_path / "matrix.json",
        {
            "schema": "v1_5_production_component_qc_fit_matrix_v1",
            "overall_status": "production_fit_review_ready",
            "production_fit_allowed": True,
            "fit_ready_strategy_count": 1,
        },
    )


def test_checked_historical_matrix_closes_contract_but_emits_no_write_plan():
    model = build_v1_5_unified_controlled_write_reverify(
        production_fit_matrix_json=CHECKED_MATRIX
    )

    assert model["schema"] == SCHEMA
    assert model["overall_status"] == "blocked_no_fit_approved_candidate"
    assert model["unified_contract_available"] is True
    assert model["frozen_gap_program_contract_closed"] is True
    assert model["frozen_gap_production_evidence_closed"] is False
    assert model["production_fit_allowed"] is False
    assert model["fit_ready_strategy_count"] == 0
    assert model["operation_plan_count"] == 0
    assert model["write_transaction_status"] == "not_authorized"
    assert model["getco_readback_status"] == "not_authorized"
    assert model["physical_short_reverify_status"] == "not_attempted"
    assert "approved_candidate_packet_missing" in model["review_reasons"]
    assert model["evidence_source"] == "historical_replay"
    assert model["not_real_acceptance_evidence"] is True


def test_reviewed_legacy_candidate_builds_locked_ordered_plan_and_composes_s5(tmp_path: Path):
    matrix = _ready_matrix(tmp_path)
    packet, snapshot, authorization = _approved_packet(tmp_path, matrix)
    model = build_v1_5_unified_controlled_write_reverify(
        production_fit_matrix_json=matrix,
        approved_candidate_packet_json=packet,
        current_getco_snapshot_json=snapshot,
        authorization_json=authorization,
    )

    assert model["overall_status"] == "unified_controlled_write_plan_review_ready_execution_locked"
    assert model["operation_plan_ready"] is True
    assert model["controlled_write_allowed"] is False
    assert model["writes_coefficients"] is False
    assert all(row["execution_allowed"] is False for row in model["operation_plan"])
    by_group = {row["group"]: row for row in model["group_reviews"]}
    assert json.loads(by_group["SENCO5"]["absolute_target_values"]) == [11.6, 0.4]
    assert json.loads(by_group["SENCO6"]["absolute_target_values"]) == pytest.approx([2.9, 0.45])
    s5_actions = [row["action"] for row in model["operation_plan"] if row["group"] == "SENCO5"]
    assert s5_actions == [
        "clear_existing_affine_layer",
        "verify_neutral_readback",
        "write_reviewed_target",
        "verify_getco_readback",
    ]
    s1 = next(row for row in model["operation_plan"] if row["group"] == "SENCO1" and row["action"] == "write_reviewed_target")
    s5 = next(row for row in model["operation_plan"] if row["group"] == "SENCO5" and row["action"] == "write_reviewed_target")
    s2 = next(row for row in model["operation_plan"] if row["group"] == "SENCO2" and row["action"] == "write_reviewed_target")
    s6 = next(row for row in model["operation_plan"] if row["group"] == "SENCO6" and row["action"] == "write_reviewed_target")
    assert s1["command_contract"] == "SENCO1,YGAS,FFF,1.00000e00,2.00000e00,3.00000e00,4.00000e00,0.00000e00,0.00000e00"
    assert s5["command_contract"] == "SENCO5,YGAS,FFF,11.600,0.400"
    assert s2["command_contract"].startswith("SENCO2,YGAS,FFF,9.00000e00,1.00000e01")
    assert s6["command_contract"] == "SENCO6,YGAS,FFF,2.900,0.450"


def test_s7_s8_are_neutral_only_and_legacy_cannot_plan_sencoa_b(tmp_path: Path):
    matrix = _ready_matrix(tmp_path)
    packet, snapshot, authorization = _approved_packet(tmp_path, matrix)
    payload = json.loads(packet.read_text(encoding="utf-8"))
    payload["device_candidates"][0]["groups"].extend(
        [
            {"group": "SENCO7", "candidate_status": "approved", "candidate_values": [1, 2, 3, 4]},
            {"group": "SENCOA", "candidate_status": "approved", "candidate_values": [1, 2, 3, 4]},
        ]
    )
    _write_json(packet, payload)
    auth_payload = json.loads(authorization.read_text(encoding="utf-8"))
    auth_payload["candidate_packet_sha256"] = _sha(packet)
    _write_json(authorization, auth_payload)

    model = build_v1_5_unified_controlled_write_reverify(
        production_fit_matrix_json=matrix,
        approved_candidate_packet_json=packet,
        current_getco_snapshot_json=snapshot,
        authorization_json=authorization,
    )
    by_group = {row["group"]: row for row in model["group_reviews"]}
    assert "temperature_coefficients_are_neutral_only_not_fit_candidates" in by_group["SENCO7"]["reasons"]
    assert "group_not_applicable_to_algorithm_profile" in by_group["SENCOA"]["reasons"]
    assert not any(row["group"] in {"SENCO7", "SENCOA"} for row in model["operation_plan"])


def test_s9_linear_requires_explicit_exception_evidence(tmp_path: Path):
    matrix = _ready_matrix(tmp_path)
    packet, snapshot, authorization = _approved_packet(tmp_path, matrix)
    payload = json.loads(packet.read_text(encoding="utf-8"))
    s9 = next(row for row in payload["device_candidates"][0]["groups"] if row["group"] == "SENCO9")
    s9.update({"candidate_values": [-2.96, 0.990751, 0, 0], "model_kind": "linear"})
    _write_json(packet, payload)
    auth_payload = json.loads(authorization.read_text(encoding="utf-8"))
    auth_payload["candidate_packet_sha256"] = _sha(packet)
    _write_json(authorization, auth_payload)

    blocked = build_v1_5_unified_controlled_write_reverify(
        production_fit_matrix_json=matrix,
        approved_candidate_packet_json=packet,
        current_getco_snapshot_json=snapshot,
        authorization_json=authorization,
    )
    row = next(row for row in blocked["group_reviews"] if row["group"] == "SENCO9")
    assert "senco9_linear_exception_not_approved" in row["reasons"]

    s9["linear_exception_evidence_status"] = "approved"
    _write_json(packet, payload)
    auth_payload["candidate_packet_sha256"] = _sha(packet)
    _write_json(authorization, auth_payload)
    ready = build_v1_5_unified_controlled_write_reverify(
        production_fit_matrix_json=matrix,
        approved_candidate_packet_json=packet,
        current_getco_snapshot_json=snapshot,
        authorization_json=authorization,
    )
    assert any(row["group"] == "SENCO9" for row in ready["operation_plan"])


def test_authorization_and_old_snapshot_fail_closed(tmp_path: Path):
    matrix = _ready_matrix(tmp_path)
    packet, snapshot, authorization = _approved_packet(tmp_path, matrix)
    auth_payload = json.loads(authorization.read_text(encoding="utf-8"))
    auth_payload["minimum_serial_command_gap_s"] = 0
    _write_json(authorization, auth_payload)
    snapshot_payload = json.loads(snapshot.read_text(encoding="utf-8"))
    del snapshot_payload["devices"][0]["values"]["GETCO5"]
    _write_json(snapshot, snapshot_payload)

    model = build_v1_5_unified_controlled_write_reverify(
        production_fit_matrix_json=matrix,
        approved_candidate_packet_json=packet,
        current_getco_snapshot_json=snapshot,
        authorization_json=authorization,
    )
    s5 = next(row for row in model["group_reviews"] if row["group"] == "SENCO5")
    assert "authorization_serial_gap_below_1s" in s5["reasons"]
    assert "old_snapshot_width_0_expected_2" in s5["reasons"]
    assert model["operation_plan_ready"] is False
    assert model["write_transaction_status"] == "not_authorized"


def test_temperature_neutral_snapshot_is_required_before_any_plan_is_ready(tmp_path: Path):
    matrix = _ready_matrix(tmp_path)
    packet, snapshot, authorization = _approved_packet(tmp_path, matrix)
    snapshot_payload = json.loads(snapshot.read_text(encoding="utf-8"))
    snapshot_payload["devices"][0]["values"]["GETCO8"] = [0, 0.99, 0, 0]
    _write_json(snapshot, snapshot_payload)

    model = build_v1_5_unified_controlled_write_reverify(
        production_fit_matrix_json=matrix,
        approved_candidate_packet_json=packet,
        current_getco_snapshot_json=snapshot,
        authorization_json=authorization,
    )
    assert model["operation_plan_ready"] is False
    assert "temperature_neutral_snapshot_failed:001:SENCO8" in model["review_reasons"]
    assert model["write_transaction_status"] == "not_authorized"
    assert model["frozen_gap_production_evidence_closed"] is False


def test_identity_and_duplicate_group_must_match_snapshot_before_plan(tmp_path: Path):
    matrix = _ready_matrix(tmp_path)
    packet, snapshot, authorization = _approved_packet(tmp_path, matrix)
    packet_payload = json.loads(packet.read_text(encoding="utf-8"))
    packet_payload["device_candidates"][0]["sn_code"] = "BAD"
    packet_payload["device_candidates"][0]["groups"].append(
        {"group": "SENCO1", "candidate_status": "approved", "candidate_values": [1, 2, 3, 4, 0, 0]}
    )
    _write_json(packet, packet_payload)
    auth_payload = json.loads(authorization.read_text(encoding="utf-8"))
    auth_payload["candidate_packet_sha256"] = _sha(packet)
    _write_json(authorization, auth_payload)

    model = build_v1_5_unified_controlled_write_reverify(
        production_fit_matrix_json=matrix,
        approved_candidate_packet_json=packet,
        current_getco_snapshot_json=snapshot,
        authorization_json=authorization,
    )
    assert "candidate_sn_code_invalid:001" in model["review_reasons"]
    assert "snapshot_sn_code_mismatch:001:BAD:01260701" in model["review_reasons"]
    assert "candidate_group_duplicate:001:SENCO1" in model["review_reasons"]
    assert model["operation_plan_ready"] is False


def test_write_readback_and_physical_reverify_remain_separate(tmp_path: Path):
    matrix = _ready_matrix(tmp_path)
    packet, snapshot, authorization = _approved_packet(tmp_path, matrix)
    base = build_v1_5_unified_controlled_write_reverify(
        production_fit_matrix_json=matrix,
        approved_candidate_packet_json=packet,
        current_getco_snapshot_json=snapshot,
        authorization_json=authorization,
    )
    write_targets = [
        {"device_id": row["device_id"], "group": row["group"], "write_status": "success", "readback_status": "match"}
        for row in base["operation_plan"]
        if row["action"] == "write_reviewed_target"
    ]
    events = _write_json(tmp_path / "events.json", {"events": write_targets})
    failed_verify = _write_json(
        tmp_path / "reverify.json",
        {"device_component_summary": [{"device_id": "001", "component": "co2", "status": "fail"}]},
    )
    model = build_v1_5_unified_controlled_write_reverify(
        production_fit_matrix_json=matrix,
        approved_candidate_packet_json=packet,
        current_getco_snapshot_json=snapshot,
        authorization_json=authorization,
        write_events_json=events,
        short_reverify_json=failed_verify,
    )
    assert model["write_transaction_status"] == "complete"
    assert model["getco_readback_status"] == "complete"
    assert model["physical_short_reverify_status"] == "complete_fail_or_incomplete"
    assert model["frozen_gap_production_evidence_closed"] is False


def test_writer_cli_output_boundary_and_entrypoint_classification(tmp_path: Path):
    output = tmp_path / "repo" / "docs" / "v1_5_flow_contract" / "unified"
    repo = tmp_path / "repo"
    paths = write_v1_5_unified_controlled_write_reverify(
        build_v1_5_unified_controlled_write_reverify(production_fit_matrix_json=CHECKED_MATRIX),
        output,
    )
    assert all(path.is_file() for path in paths.values())

    cli_output = repo / "docs" / "v1_5_flow_contract" / "cli"
    assert main(
        [
            "--repository-root",
            str(repo),
            "--production-fit-matrix-json",
            str(CHECKED_MATRIX),
            "--output-dir",
            str(cli_output),
        ]
    ) == 0
    payload = json.loads((cli_output / "v1_5_unified_controlled_write_readback_reverify.json").read_text(encoding="utf-8"))
    assert payload["opens_com_ports"] is False
    assert payload["writes_coefficients"] is False

    with pytest.raises(ValueError, match="docs/v1_5_flow_contract"):
        main(
            [
                "--repository-root",
                str(repo),
                "--production-fit-matrix-json",
                str(CHECKED_MATRIX),
                "--output-dir",
                str(tmp_path / "outside"),
            ]
        )

    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/export_v1_5_unified_controlled_write_reverify.py",
        root=ROOT,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.writes_coefficients is False
    assert any("unified S1-S9/SENCOA-B" in note for note in entry.notes)


def test_module_has_no_live_io_imports():
    source = (ROOT / "src/gas_calibrator/validation/v1_5_unified_controlled_write_reverify.py").read_text(encoding="utf-8")
    assert "import serial" not in source
    assert "psycopg" not in source
    assert "subprocess" not in source
