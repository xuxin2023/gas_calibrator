import hashlib
import json
from pathlib import Path

from gas_calibrator.tools.build_v1_5_real_acceptance_control_pack import main as control_pack_main
from gas_calibrator.validation.v1_5_real_acceptance_control_pack import (
    build_v1_5_real_acceptance_control_pack,
    build_v1_5_real_acceptance_site_profile_template,
    confirm_v1_5_current_site_state,
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


def _site_profile(
    tmp_path: Path,
    *,
    mapped: bool,
    confirmed: bool = True,
) -> tuple[Path, dict]:
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
        if confirmed:
            profile = confirm_v1_5_current_site_state(
                site_profile=profile,
                operator_name="test-operator",
                observation_basis="test physical observation",
                confirmed_at="2026-07-30T14:30:00Z",
            )
    return inventory, profile


def _attach_current_probe_evidence(
    tmp_path: Path,
    profile: dict,
) -> tuple[dict, Path, Path]:
    passive = _write(
        tmp_path / "probe" / "passive.json",
        {
            "schema": "v1_5_passive_site_inventory_probe_v1",
            "engineering_probe_only": True,
            "not_real_acceptance_evidence": True,
            "bytes_written": 0,
            "sends_device_commands": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "streaming_powered_ports": ["COM35", "COM36"],
            "port_results": [
                {
                    "port": f"COM{index}",
                    "open_succeeded": True,
                    "observed_device_ids": (
                        [f"{index - 34:03d}"] if index in (35, 36) else []
                    ),
                }
                for index in range(35, 43)
            ],
        },
    )
    identity = _write(
        tmp_path / "probe" / "identity.json",
        {
            "schema": "v1_5_powered_analyzer_identity_query_v1",
            "engineering_probe_only": True,
            "not_real_acceptance_evidence": True,
            "sends_device_commands": True,
            "sends_write_commands": False,
            "command_attempt_count": 2,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "results": [
                {
                    "port": "COM35",
                    "open_succeeded": True,
                    "sn_code_read": "01260701",
                },
                {
                    "port": "COM36",
                    "open_succeeded": True,
                    "sn_code_read": "01260702",
                },
            ],
        },
    )
    profile["current_probe_evidence"] = {
        "schema": "v1_5_current_site_probe_evidence_binding_v1",
        "engineering_probe_only": True,
        "promotion_state": "blocked",
        "not_real_acceptance_evidence": True,
        "passive_inventory_json": str(passive),
        "passive_inventory_sha256": hashlib.sha256(passive.read_bytes()).hexdigest(),
        "identity_query_json": str(identity),
        "identity_query_sha256": hashlib.sha256(identity.read_bytes()).hexdigest(),
        "streaming_powered_ports": ["COM35", "COM36"],
        "connected_unpowered_ports_inferred": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
    }
    return profile, passive, identity


def _attach_current_initialization_probe(
    tmp_path: Path,
    profile: dict,
) -> tuple[dict, Path, Path]:
    source_dir = tmp_path / "probe" / "initialization_sources"
    source_pairs = (
        ("cadence_json", "cadence_json_sha256"),
        ("identity_json", "identity_json_sha256"),
        ("getco_snapshot_json", "getco_snapshot_json_sha256"),
        ("getco_rows_csv", "getco_rows_csv_sha256"),
        ("getco_identity_csv", "getco_identity_csv_sha256"),
        ("getco_conclusion_csv", "getco_conclusion_csv_sha256"),
        ("getco_meta_json", "getco_meta_json_sha256"),
        ("getco_probe_source_py", "getco_probe_source_py_sha256"),
    )
    sources = {}
    first_source = None
    for source_key, hash_key in source_pairs:
        source_path = source_dir / source_key
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_text(f"{source_key}\n", encoding="utf-8")
        sources[source_key] = str(source_path)
        sources[hash_key] = hashlib.sha256(source_path.read_bytes()).hexdigest()
        first_source = first_source or source_path

    results = []
    for port in ("COM35", "COM36"):
        row = next(
            item
            for item in profile["candidate_analyzers"]
            if item["port"] == port
        )
        results.append(
            {
                "port": port,
                "protocol_device_id": row["protocol_device_id"],
                "sn_code": row["sn_code"],
                "effective_ftd_hz": 1,
                "GETCO7": [0.0, 1.0, 0.0, 0.0],
                "GETCO8": [0.0, 1.0, 0.0, 0.0],
                "senco7_write_required": False,
                "senco8_write_required": False,
                "initialization_action": (
                    "already_neutral_readback_only_skip_senco78_write"
                ),
                "status": "pass",
            }
        )
    initialization = _write(
        tmp_path / "probe" / "initialization_probe.json",
        {
            "schema": "v1_5_current_powered_initialization_probe_v1",
            "overall_status": "effective_1hz_and_senco78_already_neutral",
            "engineering_probe_only": True,
            "promotion_state": "blocked",
            "not_real_acceptance_evidence": True,
            "query_command_whitelist": [
                f"GETCO,YGAS,FFF,{group}" for group in (5, 6, 7, 8)
            ],
            "query_command_count": 8,
            "minimum_inter_command_gap_s": 1.0,
            "opens_com_ports": True,
            "sends_read_only_commands": True,
            "sends_write_commands": False,
            "sets_comm_way": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "connects_postgresql": False,
            "controls_pressure": False,
            "controls_temperature": False,
            "controls_water_or_gas_routes": False,
            "database_written": False,
            "source_artifacts": sources,
            "results": results,
        },
    )
    profile["current_probe_evidence"].update(
        {
            "initialization_probe_json": str(initialization),
            "initialization_probe_sha256": hashlib.sha256(
                initialization.read_bytes()
            ).hexdigest(),
        }
    )
    assert first_source is not None
    return profile, initialization, first_source


def _attach_runtime_setting_readability_review(
    tmp_path: Path,
    profile: dict,
) -> tuple[dict, Path]:
    results = []
    for port in ("COM35", "COM36"):
        row = next(
            item
            for item in profile["candidate_analyzers"]
            if item["port"] == port
        )
        results.append(
            {
                "port": port,
                "protocol_device_id": row["protocol_device_id"],
                "sn_code": row["sn_code"],
                "historical_port_values_reused": False,
                "inferred_from_coefficient_shape": False,
                "status": "safe_hold",
            }
        )
    review = _write(
        tmp_path / "probe" / "runtime_readability_review.json",
        {
            "schema": "v1_5_runtime_setting_readability_review_v1",
            "overall_status": "safe_hold_no_supported_read_command",
            "engineering_review_only": True,
            "promotion_state": "blocked",
            "not_real_acceptance_evidence": True,
            "real_com_execution_attempted": False,
            "opens_com_ports": False,
            "sends_device_commands": False,
            "command_attempt_count": 0,
            "bytes_written": 0,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "controls_pressure": False,
            "controls_temperature": False,
            "controls_water_or_gas_routes": False,
            "connects_postgresql": False,
            "database_written": False,
            "capabilities": {
                key: {
                    "supported_read_command": "",
                    "directly_readable": False,
                }
                for key in ("average1", "average2", "filter", "algorithm")
            },
            "results": results,
        },
    )
    profile["current_probe_evidence"].update(
        {
            "runtime_setting_readability_review_json": str(review),
            "runtime_setting_readability_review_sha256": hashlib.sha256(
                review.read_bytes()
            ).hexdigest(),
        }
    )
    return profile, review


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


def test_complete_mapping_requires_hash_bound_current_site_confirmation(
    tmp_path: Path,
) -> None:
    inventory, profile = _site_profile(
        tmp_path,
        mapped=True,
        confirmed=False,
    )
    without_confirmation = validate_v1_5_real_acceptance_site_profile(
        site_profile=profile,
        runtime_port_inventory_json=inventory,
    )
    assert without_confirmation["ready_for_readonly_packet_build"] is False
    assert "current_site_confirmation_missing" in without_confirmation["reasons"]

    confirmed = confirm_v1_5_current_site_state(
        site_profile=profile,
        operator_name="operator-a",
        observation_basis="physical cable and power indicator observation",
        confirmed_at="2026-07-30T14:31:00Z",
    )
    with_confirmation = validate_v1_5_real_acceptance_site_profile(
        site_profile=confirmed,
        runtime_port_inventory_json=inventory,
    )
    assert with_confirmation["ready_for_readonly_packet_build"] is True
    assert confirmed["current_site_confirmation"]["connected_ports"] == [
        "COM35",
        "COM36",
        "COM37",
        "COM38",
    ]
    assert confirmed["current_site_confirmation"]["powered_ports"] == [
        "COM35",
        "COM36",
    ]
    assert confirmed["current_site_confirmation"]["candidate_state_sha256"] == (
        "632fa75121386dc11595e1123bcb6e24574a555f4ccdefdf89628aa12aeae2c3"
    )


def test_current_site_confirmation_becomes_invalid_after_mapping_edit(
    tmp_path: Path,
) -> None:
    inventory, profile = _site_profile(tmp_path, mapped=True)
    profile["candidate_analyzers"][0]["runtime_evidence"]["average1"] = "edited"

    validation = validate_v1_5_real_acceptance_site_profile(
        site_profile=profile,
        runtime_port_inventory_json=inventory,
    )

    assert validation["ready_for_readonly_packet_build"] is False
    assert (
        "current_site_confirmation_state_sha256_mismatch"
        in validation["reasons"]
    )


def test_current_probe_sources_are_hash_and_identity_bound(
    tmp_path: Path,
) -> None:
    inventory, profile = _site_profile(tmp_path, mapped=True, confirmed=False)
    profile, passive, identity = _attach_current_probe_evidence(tmp_path, profile)
    profile = confirm_v1_5_current_site_state(
        site_profile=profile,
        operator_name="operator-a",
        observation_basis="physical observation plus read-only probe",
        confirmed_at="2026-07-30T14:31:00Z",
    )

    validation = validate_v1_5_real_acceptance_site_profile(
        site_profile=profile,
        runtime_port_inventory_json=inventory,
    )
    evidence = _evidence(tmp_path, ready=False)
    model = build_v1_5_real_acceptance_control_pack(
        runtime_port_inventory_json=inventory,
        certificate_registry_json=evidence["registry"],
        certificate_reconciliation_json=evidence["reconciliation"],
        certificate_admission_json=evidence["admission"],
        workstation_dry_run_json=evidence["workstation"],
        site_profile=profile,
    )

    assert validation["ready_for_readonly_packet_build"] is True
    assert (
        validation["current_probe_evidence_validation"]["status"]
        == "valid_engineering_probe_binding"
    )
    assert model["source_evidence_opened_com_ports"] is True
    assert model["source_evidence_sent_read_only_device_commands"] is True
    assert model["source_evidence_sent_write_commands"] is False
    artifacts = {row["role"]: row for row in model["artifacts"]}
    assert artifacts["current_site_passive_inventory_probe"]["sha256"] == (
        hashlib.sha256(passive.read_bytes()).hexdigest()
    )
    assert artifacts["current_site_powered_identity_query"]["sha256"] == (
        hashlib.sha256(identity.read_bytes()).hexdigest()
    )


def test_current_probe_source_tamper_blocks_profile(
    tmp_path: Path,
) -> None:
    inventory, profile = _site_profile(tmp_path, mapped=True, confirmed=False)
    profile, _, identity = _attach_current_probe_evidence(tmp_path, profile)
    profile = confirm_v1_5_current_site_state(
        site_profile=profile,
        operator_name="operator-a",
        observation_basis="physical observation plus read-only probe",
        confirmed_at="2026-07-30T14:31:00Z",
    )
    payload = json.loads(identity.read_text(encoding="utf-8"))
    payload["results"][0]["sn_code_read"] = "01260799"
    identity.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    validation = validate_v1_5_real_acceptance_site_profile(
        site_profile=profile,
        runtime_port_inventory_json=inventory,
    )

    assert validation["ready_for_readonly_packet_build"] is False
    assert (
        "current_probe_identity_query_sha256_mismatch"
        in validation["reasons"]
    )
    assert "current_probe_COM35_sn_mismatch" in validation["reasons"]


def test_current_initialization_probe_binds_effective_1hz_and_neutral_senco78(
    tmp_path: Path,
) -> None:
    inventory, profile = _site_profile(tmp_path, mapped=True, confirmed=False)
    profile, _, _ = _attach_current_probe_evidence(tmp_path, profile)
    profile, initialization, _ = _attach_current_initialization_probe(
        tmp_path,
        profile,
    )
    profile = confirm_v1_5_current_site_state(
        site_profile=profile,
        operator_name="operator-a",
        observation_basis="physical observation plus read-only probes",
        confirmed_at="2026-07-30T14:31:00Z",
    )
    evidence = _evidence(tmp_path, ready=False)

    model = build_v1_5_real_acceptance_control_pack(
        runtime_port_inventory_json=inventory,
        certificate_registry_json=evidence["registry"],
        certificate_reconciliation_json=evidence["reconciliation"],
        certificate_admission_json=evidence["admission"],
        workstation_dry_run_json=evidence["workstation"],
        site_profile=profile,
    )

    current_probe = model["source_probe_evidence"]
    assert current_probe["status"] == "valid_engineering_probe_binding"
    assert (
        current_probe["initialization_probe"]["status"]
        == "valid_no_write_initialization_probe"
    )
    assert current_probe["sends_write_commands"] is False
    artifacts = {row["role"]: row for row in model["artifacts"]}
    assert artifacts["current_powered_initialization_probe"]["sha256"] == (
        hashlib.sha256(initialization.read_bytes()).hexdigest()
    )


def test_runtime_readability_review_is_bound_without_releasing_blockers(
    tmp_path: Path,
) -> None:
    inventory, profile = _site_profile(tmp_path, mapped=True, confirmed=False)
    profile, _, _ = _attach_current_probe_evidence(tmp_path, profile)
    for row in profile["candidate_analyzers"]:
        if row["port"] in {"COM35", "COM36"}:
            row["algorithm"] = ""
            row["runtime_evidence"]["average1"] = ""
            row["runtime_evidence"]["average2"] = ""
    profile, review = _attach_runtime_setting_readability_review(
        tmp_path,
        profile,
    )
    evidence = _evidence(tmp_path, ready=False)

    model = build_v1_5_real_acceptance_control_pack(
        runtime_port_inventory_json=inventory,
        certificate_registry_json=evidence["registry"],
        certificate_reconciliation_json=evidence["reconciliation"],
        certificate_admission_json=evidence["admission"],
        workstation_dry_run_json=evidence["workstation"],
        site_profile=profile,
    )

    current_probe = model["source_probe_evidence"]
    assert current_probe["status"] == "valid_engineering_probe_binding"
    assert (
        current_probe["runtime_setting_readability_review"]["status"]
        == "valid_safe_hold_review"
    )
    site_reasons = model["site_profile_validation"]["reasons"]
    assert "COM35_algorithm_invalid" in site_reasons
    assert "COM35_average1_average2_evidence_missing" in site_reasons
    artifacts = {row["role"]: row for row in model["artifacts"]}
    assert artifacts["current_runtime_setting_readability_review"][
        "sha256"
    ] == hashlib.sha256(review.read_bytes()).hexdigest()


def test_runtime_readability_review_cannot_claim_a_read_command(
    tmp_path: Path,
) -> None:
    inventory, profile = _site_profile(tmp_path, mapped=True, confirmed=False)
    profile, _, _ = _attach_current_probe_evidence(tmp_path, profile)
    profile, review = _attach_runtime_setting_readability_review(
        tmp_path,
        profile,
    )
    payload = json.loads(review.read_text(encoding="utf-8"))
    payload["capabilities"]["average1"] = {
        "supported_read_command": "GETAVERAGE1",
        "directly_readable": True,
    }
    review.write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )
    profile["current_probe_evidence"][
        "runtime_setting_readability_review_sha256"
    ] = hashlib.sha256(review.read_bytes()).hexdigest()

    validation = validate_v1_5_real_acceptance_site_profile(
        site_profile=profile,
        runtime_port_inventory_json=inventory,
    )

    assert validation["ready_for_readonly_packet_build"] is False
    assert (
        "current_probe_runtime_readability_capability_contract_invalid"
        in validation["reasons"]
    )


def test_current_initialization_probe_source_tamper_blocks_profile(
    tmp_path: Path,
) -> None:
    inventory, profile = _site_profile(tmp_path, mapped=True, confirmed=False)
    profile, _, _ = _attach_current_probe_evidence(tmp_path, profile)
    profile, _, source = _attach_current_initialization_probe(tmp_path, profile)
    profile = confirm_v1_5_current_site_state(
        site_profile=profile,
        operator_name="operator-a",
        observation_basis="physical observation plus read-only probes",
        confirmed_at="2026-07-30T14:31:00Z",
    )
    source.write_text("tampered\n", encoding="utf-8")

    validation = validate_v1_5_real_acceptance_site_profile(
        site_profile=profile,
        runtime_port_inventory_json=inventory,
    )

    assert validation["ready_for_readonly_packet_build"] is False
    assert any(
        reason.endswith("_sha256_mismatch")
        and "initialization_source_" in reason
        for reason in validation["reasons"]
    )


def test_current_initialization_probe_nonneutral_senco8_blocks_profile(
    tmp_path: Path,
) -> None:
    inventory, profile = _site_profile(tmp_path, mapped=True, confirmed=False)
    profile, _, _ = _attach_current_probe_evidence(tmp_path, profile)
    profile, initialization, _ = _attach_current_initialization_probe(
        tmp_path,
        profile,
    )
    payload = json.loads(initialization.read_text(encoding="utf-8"))
    payload["results"][0]["GETCO8"] = [0.0, 0.9, 0.0, 0.0]
    initialization.write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )
    profile["current_probe_evidence"]["initialization_probe_sha256"] = (
        hashlib.sha256(initialization.read_bytes()).hexdigest()
    )
    profile = confirm_v1_5_current_site_state(
        site_profile=profile,
        operator_name="operator-a",
        observation_basis="physical observation plus read-only probes",
        confirmed_at="2026-07-30T14:31:00Z",
    )

    validation = validate_v1_5_real_acceptance_site_profile(
        site_profile=profile,
        runtime_port_inventory_json=inventory,
    )

    assert validation["ready_for_readonly_packet_build"] is False
    assert (
        "current_probe_initialization_COM35_senco8_not_neutral"
        in validation["reasons"]
    )


def test_current_probe_declared_write_command_is_exposed_and_blocked(
    tmp_path: Path,
) -> None:
    inventory, profile = _site_profile(tmp_path, mapped=True, confirmed=False)
    profile, _, identity = _attach_current_probe_evidence(tmp_path, profile)
    payload = json.loads(identity.read_text(encoding="utf-8"))
    payload["sends_write_commands"] = True
    identity.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    profile["current_probe_evidence"]["identity_query_sha256"] = hashlib.sha256(
        identity.read_bytes()
    ).hexdigest()
    profile = confirm_v1_5_current_site_state(
        site_profile=profile,
        operator_name="operator-a",
        observation_basis="physical observation plus read-only probe",
        confirmed_at="2026-07-30T14:31:00Z",
    )
    evidence = _evidence(tmp_path, ready=False)

    model = build_v1_5_real_acceptance_control_pack(
        runtime_port_inventory_json=inventory,
        certificate_registry_json=evidence["registry"],
        certificate_reconciliation_json=evidence["reconciliation"],
        certificate_admission_json=evidence["admission"],
        workstation_dry_run_json=evidence["workstation"],
        site_profile=profile,
    )

    assert "current_probe_identity_sent_write_commands" in model[
        "site_profile_validation"
    ]["reasons"]
    assert model["source_evidence_sent_write_commands"] is True
    assert model["source_evidence_sent_read_only_device_commands"] is False
    assert model["preflight_ready_for_explicit_readonly_authorization"] is False


def test_current_probe_identity_mismatch_blocks_even_when_operator_confirms(
    tmp_path: Path,
) -> None:
    inventory, profile = _site_profile(tmp_path, mapped=True, confirmed=False)
    profile, _, _ = _attach_current_probe_evidence(tmp_path, profile)
    profile["candidate_analyzers"][0]["protocol_device_id"] = "099"
    profile = confirm_v1_5_current_site_state(
        site_profile=profile,
        operator_name="operator-a",
        observation_basis="physical observation plus read-only probe",
        confirmed_at="2026-07-30T14:31:00Z",
    )

    validation = validate_v1_5_real_acceptance_site_profile(
        site_profile=profile,
        runtime_port_inventory_json=inventory,
    )

    assert validation["ready_for_readonly_packet_build"] is False
    assert (
        "current_probe_COM35_protocol_identity_mismatch"
        in validation["reasons"]
    )


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
