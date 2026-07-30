import hashlib
import json
from pathlib import Path

from gas_calibrator.tools.build_v1_5_real_acceptance_control_pack import main as control_pack_main
from gas_calibrator.validation.v1_5_real_acceptance_control_pack import (
    build_v1_5_real_acceptance_control_pack,
    build_v1_5_real_acceptance_site_profile_template,
    prefill_v1_5_site_profile_from_historical_identity,
    validate_v1_5_real_acceptance_site_profile,
    write_v1_5_real_acceptance_control_pack_outputs,
)


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _inventory(tmp_path: Path) -> Path:
    return _write(
        tmp_path / "runtime_serial_port_inventory.json",
        {
            "schema": "v1_5_runtime_serial_port_inventory_v1",
            "opens_com_ports": False,
            "sends_device_commands": False,
            "ports": [{"port": f"COM{index}"} for index in range(35, 43)],
        },
    )


def _site_profile(tmp_path: Path, *, mapped: bool) -> tuple[Path, dict]:
    inventory = _inventory(tmp_path)
    profile = build_v1_5_real_acceptance_site_profile_template(
        runtime_port_inventory_json=inventory,
        reported_connected_count=4,
        reported_powered_count=2,
        observation_id="test-four-connected-two-powered",
    )
    if mapped:
        for index, row in enumerate(profile["candidate_analyzers"], start=1):
            row["connected"] = index <= 4
            row["powered"] = index <= 2
            row["operator_confirmed"] = index <= 4
            if index <= 4:
                row["ga_label"] = f"GA{index:02d}"
            if index <= 2:
                row.update(
                    {
                        "protocol_device_id": f"{index:03d}",
                        "sn_code": f"012607{index:02d}",
                        "algorithm": "legacy_ratio",
                        "check_capable": False,
                        "check_required": False,
                        "runtime_evidence": {
                            "ftd_hz": 1.0,
                            "average1": "AVERAGE1",
                            "average2": "AVERAGE2",
                            "filter": "reviewed",
                        },
                    }
                )
    return inventory, profile


def _historical_identity(tmp_path: Path) -> Path:
    rows = (
        ("070", "GA01", "COM35", "004", "01260604"),
        ("058", "GA02", "COM36", "047", "01260601"),
        ("082", "GA03", "COM37", "054", "01260602"),
        ("083", "GA04", "COM39", "001", "01260605"),
        ("097", "GA05", "COM41", "052", "01260603"),
        ("098", "GA06", "COM42", "073", "01260606"),
    )
    lines = [
        (
            "old_device_id,old_slot,old_port,sn_write_protocol_device_id,"
            "final_sn,sn_readback,status,binding_basis,"
            "owner_confirmation_date,evidence_level"
        )
    ]
    lines.extend(
        ",".join(
            (
                old_id,
                slot,
                port,
                protocol_id,
                sn,
                sn,
                "formal_identity_ready",
                "same COM continuity and owner attestation",
                "2026-07-29",
                "owner_attested_traceable",
            )
        )
        for old_id, slot, port, protocol_id, sn in rows
    )
    path = tmp_path / "historical_identity.csv"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _historical_runtime(tmp_path: Path) -> Path:
    rows = (
        ("GA01", "COM35", "070"),
        ("GA02", "COM36", "058"),
        ("GA03", "COM37", "082"),
        ("GA04", "COM39", "083"),
        ("GA05", "COM41", "097"),
        ("GA06", "COM42", "098"),
    )
    return _write(
        tmp_path / "historical_runtime.json",
        {
            "devices": {
                "gas_analyzers": [
                    {
                        "name": name,
                        "port": port,
                        "device_id": device_id,
                        "ftd_hz": 1,
                        "average_co2": 49,
                        "average_h2o": 49,
                        "average_filter": 49,
                    }
                    for name, port, device_id in rows
                ]
            }
        },
    )


def _evidence(tmp_path: Path, *, ready: bool) -> dict[str, Path]:
    return {
        "registry": _write(
            tmp_path / "certificate_registry.json",
            {
                "records": [{"record_id": "gas-001"}],
                "boundary": {
                    "calibration_input_connected": False,
                    "device_io_allowed": False,
                    "coefficient_write_allowed": False,
                },
            },
        ),
        "reconciliation": _write(
            tmp_path / "certificate_reconciliation.json",
            {
                "mismatch_count": 0 if ready else 7,
                "automatic_value_binding_allowed": ready,
            },
        ),
        "admission": _write(
            tmp_path / "certificate_admission.json",
            {
                "ready_for_real_execution": ready,
                "strict_original_certificate_gate_passed": ready,
            },
        ),
        "workstation": _write(
            tmp_path / "workstation.json",
            {
                "overall_status": "pass",
                "point_counts": {"co2": 45, "h2o": 13},
                "opens_com_ports": False,
                "writes_coefficients": False,
                "writes_device_id": False,
                "controls_water_or_gas_routes": False,
            },
        ),
    }


def _build(tmp_path: Path, *, mapped: bool, evidence_ready: bool, readonly=None, archive=None):
    inventory, profile = _site_profile(tmp_path, mapped=mapped)
    evidence = _evidence(tmp_path, ready=evidence_ready)
    model = build_v1_5_real_acceptance_control_pack(
        runtime_port_inventory_json=inventory,
        certificate_registry_json=evidence["registry"],
        certificate_reconciliation_json=evidence["reconciliation"],
        certificate_admission_json=evidence["admission"],
        workstation_dry_run_json=evidence["workstation"],
        site_profile=profile,
        readonly_com_executor_json=readonly,
        formal_archive_closure_json=archive,
    )
    return inventory, profile, evidence, model


def test_current_four_connected_two_powered_template_stays_blocked_and_no_com(tmp_path: Path) -> None:
    inventory, profile, _, model = _build(
        tmp_path,
        mapped=False,
        evidence_ready=False,
    )
    site = validate_v1_5_real_acceptance_site_profile(
        site_profile=profile,
        runtime_port_inventory_json=inventory,
    )

    assert len(profile["candidate_analyzers"]) == 8
    assert profile["reported_connected_count"] == 4
    assert profile["reported_powered_count"] == 2
    assert site["ready_for_readonly_packet_build"] is False
    assert site["active_analyzer_list"]["active_analyzers"] == []
    assert model["lifecycle_status"] == "blocked_before_readonly_initialization"
    assert model["preflight_ready_for_explicit_readonly_authorization"] is False
    assert model["opens_com_ports"] is False
    assert model["sends_device_commands"] is False
    assert model["writes_coefficients"] is False
    assert model["formal_release_allowed"] is False


def test_historical_identity_prefill_fills_six_identities_but_not_current_state(
    tmp_path: Path,
) -> None:
    inventory, profile = _site_profile(tmp_path, mapped=False)
    identity = _historical_identity(tmp_path)
    runtime = _historical_runtime(tmp_path)

    prefilled = prefill_v1_5_site_profile_from_historical_identity(
        site_profile=profile,
        historical_identity_csv=identity,
        historical_runtime_config_json=runtime,
    )
    by_port = {row["port"]: row for row in prefilled["candidate_analyzers"]}
    metadata = prefilled["historical_identity_prefill"]

    assert metadata["status"] == "operator_current_state_confirmation_required"
    assert metadata["applied_ports"] == [
        "COM35",
        "COM36",
        "COM37",
        "COM39",
        "COM41",
        "COM42",
    ]
    assert metadata["reasons"] == []
    assert metadata["opens_com_ports"] is False
    assert by_port["COM35"]["ga_label"] == "GA01"
    assert by_port["COM35"]["protocol_device_id"] == "004"
    assert by_port["COM35"]["sn_code"] == "01260604"
    assert by_port["COM35"]["algorithm"] == "legacy_ratio"
    assert by_port["COM35"]["identity_evidence"]["historical_runtime_reference"] == {
        "ftd_hz": 1,
        "average1": 49,
        "average2": 49,
        "filter": 49,
    }
    assert all(row["connected"] is None for row in by_port.values())
    assert all(row["powered"] is None for row in by_port.values())
    assert all(row["operator_confirmed"] is False for row in by_port.values())
    assert all(not row["runtime_evidence"]["ftd_hz"] for row in by_port.values())

    validation = validate_v1_5_real_acceptance_site_profile(
        site_profile=prefilled,
        runtime_port_inventory_json=inventory,
    )
    assert validation["ready_for_readonly_packet_build"] is False
    assert validation["active_analyzer_list"]["active_analyzers"] == []


def test_historical_identity_prefill_preserves_conflicting_operator_value(
    tmp_path: Path,
) -> None:
    _, profile = _site_profile(tmp_path, mapped=False)
    profile["candidate_analyzers"][0]["sn_code"] = "99999999"

    prefilled = prefill_v1_5_site_profile_from_historical_identity(
        site_profile=profile,
        historical_identity_csv=_historical_identity(tmp_path),
        historical_runtime_config_json=_historical_runtime(tmp_path),
    )

    assert prefilled["candidate_analyzers"][0]["sn_code"] == "99999999"
    assert (
        "COM35_sn_code_conflicts_with_historical_identity"
        in prefilled["historical_identity_prefill"]["reasons"]
    )
    assert prefilled["historical_identity_prefill"]["status"] == "review_required"


def test_certificate_blockers_do_not_block_offline_readonly_program_progress(tmp_path: Path) -> None:
    _, _, _, model = _build(
        tmp_path,
        mapped=True,
        evidence_ready=False,
    )

    assert model["lifecycle_status"] == "preflight_ready_for_explicit_readonly_authorization"
    assert model["preflight_ready_for_explicit_readonly_authorization"] is True
    assert model["calibration_preflight_ready"] is False
    assert model["certificate_gates_do_not_block_offline_or_readonly_program_progress"] is True
    assert model["opens_com_ports"] is False


def test_mapped_site_can_reach_explicit_readonly_authorization_but_not_execute(tmp_path: Path) -> None:
    _, profile, _, model = _build(
        tmp_path,
        mapped=True,
        evidence_ready=True,
    )

    assert model["lifecycle_status"] == "preflight_ready_for_explicit_readonly_authorization"
    assert model["preflight_ready_for_explicit_readonly_authorization"] is True
    assert model["ready_for_human_acceptance_review"] is False
    assert model["real_acceptance_complete"] is False
    assert model["opens_com_ports"] is False
    active = model["site_profile_validation"]["active_analyzer_list"]["active_analyzers"]
    reviewed = model["site_profile_validation"]["reviewed_port_inventory"]["reviewed_ports"]
    assert len(active) == 2
    assert len(reviewed) == 4

    outputs = write_v1_5_real_acceptance_control_pack_outputs(
        model=model,
        site_profile=profile,
        output_dir=tmp_path / "out",
    )
    for line in outputs["sha256"].read_text(encoding="utf-8").splitlines():
        expected, relative = line.split("  ", 1)
        assert hashlib.sha256((outputs["sha256"].parent / relative).read_bytes()).hexdigest() == expected


def test_complete_evidence_only_reaches_human_review_never_auto_release(tmp_path: Path) -> None:
    readonly = _write(
        tmp_path / "readonly.json",
        {
            "schema": "v1_5_formal_readonly_com_minimal_executor_v1",
            "overall_status": "readonly_com_minimal_executor_completed_no_write",
            "execution_attempted": True,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "connects_postgresql": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
        },
    )
    archive = _write(
        tmp_path / "archive.json",
        {"formal_run_status": {"formal_release_allowed": True}},
    )
    _, _, _, model = _build(
        tmp_path,
        mapped=True,
        evidence_ready=True,
        readonly=readonly,
        archive=archive,
    )

    assert model["lifecycle_status"] == "ready_for_human_acceptance_review"
    assert model["ready_for_human_acceptance_review"] is True
    assert model["real_acceptance_complete"] is False
    assert model["formal_release_allowed"] is False
    assert model["real_primary_latest_refresh_allowed"] is False


def test_cli_rejects_execution_flag_without_writing_outputs(tmp_path: Path) -> None:
    inventory, _, evidence, _ = _build(tmp_path, mapped=False, evidence_ready=False)
    output_dir = tmp_path / "forbidden"
    rc = control_pack_main(
        [
            "--runtime-port-inventory-json",
            str(inventory),
            "--certificate-registry-json",
            str(evidence["registry"]),
            "--certificate-reconciliation-json",
            str(evidence["reconciliation"]),
            "--certificate-admission-json",
            str(evidence["admission"]),
            "--workstation-dry-run-json",
            str(evidence["workstation"]),
            "--output-dir",
            str(output_dir),
            "--execute-read-only-real-com",
        ]
    )

    assert rc == 2
    assert not output_dir.exists()


def test_cli_prefills_historical_identity_without_promoting_current_state(
    tmp_path: Path,
) -> None:
    inventory, _, evidence, _ = _build(
        tmp_path,
        mapped=False,
        evidence_ready=False,
    )
    output_dir = tmp_path / "prefilled"
    rc = control_pack_main(
        [
            "--runtime-port-inventory-json",
            str(inventory),
            "--certificate-registry-json",
            str(evidence["registry"]),
            "--certificate-reconciliation-json",
            str(evidence["reconciliation"]),
            "--certificate-admission-json",
            str(evidence["admission"]),
            "--workstation-dry-run-json",
            str(evidence["workstation"]),
            "--historical-identity-csv",
            str(_historical_identity(tmp_path)),
            "--historical-runtime-config-json",
            str(_historical_runtime(tmp_path)),
            "--output-dir",
            str(output_dir),
        ]
    )

    profile = json.loads(
        (output_dir / "v1_5_real_acceptance_site_profile.json").read_text(
            encoding="utf-8-sig"
        )
    )
    control_pack = json.loads(
        (output_dir / "v1_5_real_acceptance_control_pack.json").read_text(
            encoding="utf-8-sig"
        )
    )
    assert rc == 0
    assert profile["historical_identity_prefill"]["applied_count"] == 6
    assert profile["candidate_analyzers"][0]["connected"] is None
    assert profile["candidate_analyzers"][0]["powered"] is None
    assert control_pack["lifecycle_status"] == "blocked_before_readonly_initialization"
    assert control_pack["opens_com_ports"] is False
    assert control_pack["writes_coefficients"] is False
