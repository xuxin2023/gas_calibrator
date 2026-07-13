import csv
import json
from pathlib import Path

import pytest

from gas_calibrator.tools.export_v1_5_new_algorithm_mature_queue_live_handoff import (
    main as export_main,
)
from gas_calibrator.tools.run_v1_5_new_algorithm_mature_queue_live_handoff_blocked_executor import (
    main as blocked_main,
)
from gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue import (
    _load_queue_rows as load_co2_rows,
)
from gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue import (
    _load_queue_rows as load_h2o_rows,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import (
    classify_v1_5_entrypoint,
)
from gas_calibrator.validation.v1_5_new_algorithm_mature_queue_live_handoff import (
    BLOCKED_EXECUTOR_SCHEMA,
    FIT_FORMULA,
    SCHEMA,
    build_v1_5_new_algorithm_mature_queue_live_handoff,
    build_v1_5_new_algorithm_mature_queue_live_handoff_blocked_executor,
    write_v1_5_new_algorithm_mature_queue_live_handoff,
)


ROOT = Path(__file__).resolve().parents[1]
PROFILE = ROOT / "configs" / "v1_5_algorithm_route_profiles.json"
MATURE_ROUTE = (
    ROOT
    / "docs"
    / "v1_5_flow_contract"
    / "mature_route_contract"
    / "v1_5_mature_route_contract.json"
)


def _build() -> dict:
    return build_v1_5_new_algorithm_mature_queue_live_handoff(
        repo_root=ROOT,
        profile_path=PROFILE,
        mature_route_contract_json=MATURE_ROUTE,
    )


def test_contract_locks_legacy_and_new_algorithm_point_counts() -> None:
    model = _build()
    assert model["schema"] == SCHEMA
    assert model["overall_status"] == "offline_contract_ready_live_execution_blocked"
    assert model["contract_blocker_count"] == 0
    assert model["offline_handoff_contract_ready"] is True
    assert model["production_live_gap_closed"] is False
    assert model["legacy_default_profile_id"] == "legacy_ratio_production"
    assert model["legacy_default_preserved"] is True
    assert model["queue_contract"]["legacy_counts"] == {"co2": 45, "h2o": 13}
    assert model["queue_contract"]["new_algorithm_counts"] == {"co2": 47, "h2o": 14}
    assert model["live_queue_execution_allowed"] is False
    assert model["opens_com_ports"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["writes_coefficients"] is False
    assert model["connects_postgresql"] is False


def test_supplemental_points_are_inside_mature_temperature_segments() -> None:
    model = _build()
    co2 = [
        (float(row["temp_c"]), float(row["source_nominal_ppm"]))
        for row in model["_co2_rows"]
    ]
    h2o = [
        (
            float(row["temp_c"]),
            float(row["hgen_temp_c"]),
            float(row["hgen_rh_pct"]),
        )
        for row in model["_h2o_rows"]
    ]
    assert co2.index((-20.0, 600.0)) == co2.index((-20.0, 400.0)) + 1
    assert co2.index((-10.0, 600.0)) == co2.index((-10.0, 400.0)) + 1
    assert h2o.index((40.0, 30.0, 30.0)) < h2o.index((40.0, 30.0, 50.0))
    supplemental_rows = [
        row
        for row in [*model["_co2_rows"], *model["_h2o_rows"]]
        if row["point_role"] == "new_algorithm_required_supplemental_formal_point"
    ]
    assert len(supplemental_rows) == 3
    assert {
        row["historical_missing_point_semantics"] for row in supplemental_rows
    } == {"formal_required_point_not_historical_resampling"}


def test_contract_uses_absorption_fit_semantics_without_forking_routes() -> None:
    model = _build()
    fit = model["fit_input_contract"]
    assert fit["formula"] == FIT_FORMULA
    assert fit["pressure_sequence"] == "SENCO9_first"
    assert fit["temperature_source"] == "per_analyzer_chamber_T1"
    assert fit["temperature_coefficients"] == "SENCO7_SENCO8_neutral"
    assert fit["co2_zero_and_h2o_dry_anchor_are_separate"] is True
    assert model["queue_contract"]["co2_queue_runner"].endswith(
        "run_v1_5_formal_co2_open_flow_queue"
    )
    assert model["queue_contract"]["h2o_queue_runner"].endswith(
        "run_v1_5_formal_h2o_open_flow_queue"
    )
    assert all(row["sha256"] for row in model["runner_source_bindings"])
    assert len(model["runner_source_bindings"]) == 4


def test_contract_keeps_real_live_handoff_blocked() -> None:
    model = _build()
    blockers = {row["blocker"] for row in model["production_blockers"]}
    assert "separate_live_adapter_not_implemented" in blockers
    assert "live_authorization_packet_not_supplied" in blockers
    assert "active_analyzer_and_port_inventory_not_supplied" in blockers
    assert "current_pre_gas_pressure_route_readiness_not_supplied" in blockers
    assert "sencoa_sencob_r0_writer_not_production_ready" in blockers
    assert "h2o_absorption_firmware_input_scale_not_confirmed" in blockers
    assert model["execution_supported"] is False
    assert model["formal_release_allowed"] is False
    assert model["database_import_allowed"] is False
    requirements = {row["requirement"] for row in model["authorization_requirements"]}
    assert "exact_profile_and_queue_hashes" in requirements
    assert "exact_mature_runner_hashes" in requirements
    assert "legacy_default_unchanged" in requirements


def test_written_runlists_are_accepted_by_unchanged_mature_queue_loaders(
    tmp_path: Path,
) -> None:
    model = _build()
    outputs = write_v1_5_new_algorithm_mature_queue_live_handoff(
        model, tmp_path, repo_root=ROOT
    )
    persisted = json.loads(outputs["json"].read_text(encoding="utf-8"))
    assert len(load_co2_rows(outputs["co2_queue_csv"])) == 47
    assert len(load_h2o_rows(outputs["h2o_queue_csv"])) == 14
    assert persisted["queue_contract"]["co2_queue_csv_sha256"]
    assert persisted["queue_contract"]["h2o_queue_csv_sha256"]
    assert "_co2_rows" not in persisted
    assert "_h2o_rows" not in persisted
    with outputs["co2_queue_csv"].open(
        "r", encoding="utf-8-sig", newline=""
    ) as handle:
        rows = list(csv.DictReader(handle))
    text = json.dumps(rows, ensure_ascii=False).lower()
    assert "_handoff" not in text
    assert "0624" not in text
    assert "migration" not in text


def test_contract_blocks_tampered_mature_route_attestation(tmp_path: Path) -> None:
    payload = json.loads(MATURE_ROUTE.read_text(encoding="utf-8-sig"))
    payload["manifest"]["mature_route_contract"]["route_behavior"] = (
        "migrated_0624_route"
    )
    altered = tmp_path / "mature_route.json"
    altered.write_text(json.dumps(payload), encoding="utf-8")
    model = build_v1_5_new_algorithm_mature_queue_live_handoff(
        repo_root=ROOT,
        profile_path=PROFILE,
        mature_route_contract_json=altered,
    )
    assert model["offline_handoff_contract_ready"] is False
    reasons = {
        reason
        for row in model["checks"]
        for reason in row.get("reasons") or []
    }
    assert "mature_route_behavior_mismatch" in reasons


def test_contract_blocks_same_count_profile_tampering(tmp_path: Path) -> None:
    payload = json.loads(PROFILE.read_text(encoding="utf-8-sig"))
    profile = next(
        row
        for row in payload["profiles"]
        if row["profile_id"] == "absorption_ratio_shadow"
    )
    profile["co2_route"]["temperature_plan"]["-20"] = [0, 500, 600, 1000]
    altered = tmp_path / "profiles.json"
    altered.write_text(json.dumps(payload), encoding="utf-8")
    model = build_v1_5_new_algorithm_mature_queue_live_handoff(
        repo_root=ROOT,
        profile_path=altered,
        mature_route_contract_json=MATURE_ROUTE,
    )
    assert model["offline_handoff_contract_ready"] is False
    assert any(
        "immutable_queue_materialization_failed" in reason
        for row in model["checks"]
        for reason in row["reasons"]
    )


def test_blocked_executor_validates_contract_but_never_executes(tmp_path: Path) -> None:
    contract_dir = tmp_path / "contract"
    outputs = write_v1_5_new_algorithm_mature_queue_live_handoff(
        _build(), contract_dir, repo_root=ROOT
    )
    model = build_v1_5_new_algorithm_mature_queue_live_handoff_blocked_executor(
        live_handoff_json=outputs["json"]
    )
    assert model["schema"] == BLOCKED_EXECUTOR_SCHEMA
    assert model["blocked_executor_ready"] is True
    assert model["execution_supported"] is False
    assert model["execution_attempted"] is False
    assert model["live_queue_execution_allowed"] is False
    assert model["opens_com_ports"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["writes_coefficients"] is False
    assert model["connects_postgresql"] is False


@pytest.mark.parametrize(
    "forbidden_args",
    [
        ["--execute"],
        ["--execute-new-algorithm-mature-queue-handoff"],
        ["--allow-real-com"],
        ["--authorization-packet-json", "authorization.json"],
        ["--active-analyzer-list-json", "active.json"],
        ["--reviewed-port-inventory-json", "ports.json"],
        ["--runtime-config", "runtime.json"],
        ["--co2-queue-csv", "co2.csv"],
        ["--h2o-queue-csv", "h2o.csv"],
        ["--operator", "operator"],
        ["--reviewer", "reviewer"],
        ["--approver", "approver"],
    ],
)
def test_blocked_executor_rejects_every_live_unlock_input(
    tmp_path: Path, forbidden_args: list[str]
) -> None:
    contract_dir = tmp_path / "contract"
    outputs = write_v1_5_new_algorithm_mature_queue_live_handoff(
        _build(), contract_dir, repo_root=ROOT
    )
    blocked_dir = tmp_path / "blocked"
    args = [
        "--live-handoff-json",
        str(outputs["json"]),
        "--output-dir",
        str(blocked_dir),
        *forbidden_args,
    ]
    assert blocked_main(args) == 2
    assert not blocked_dir.exists()


def test_export_cli_and_entrypoint_inventory_are_offline(tmp_path: Path) -> None:
    output = tmp_path / "export"
    assert (
        export_main(
            [
                "--repo-root",
                str(ROOT),
                "--profile-path",
                str(PROFILE),
                "--mature-route-contract-json",
                str(MATURE_ROUTE),
                "--output-dir",
                str(output),
            ]
        )
        == 0
    )
    for filename in (
        "export_v1_5_new_algorithm_mature_queue_live_handoff.py",
        "run_v1_5_new_algorithm_mature_queue_live_handoff_blocked_executor.py",
    ):
        entry = classify_v1_5_entrypoint(
            ROOT / "src" / "gas_calibrator" / "tools" / filename,
            root=ROOT,
        )
        assert entry.category == "formal_review_evidence"
        assert entry.formal_status == "formal_support"
        assert entry.risk_level == "offline"
        assert entry.opens_com_ports is False
        assert entry.controls_routes is False
        assert entry.writes_coefficients is False


def test_implementation_has_no_hardware_database_or_subprocess_imports() -> None:
    source = (
        ROOT
        / "src"
        / "gas_calibrator"
        / "validation"
        / "v1_5_new_algorithm_mature_queue_live_handoff.py"
    ).read_text(encoding="utf-8")
    tool = (
        ROOT
        / "src"
        / "gas_calibrator"
        / "tools"
        / "run_v1_5_new_algorithm_mature_queue_live_handoff_blocked_executor.py"
    ).read_text(encoding="utf-8")
    lowered = f"{source}\n{tool}".lower()
    for forbidden in (
        "import serial",
        "from serial",
        "import psycopg",
        "from psycopg",
        "import subprocess",
        "from subprocess",
    ):
        assert forbidden not in lowered
