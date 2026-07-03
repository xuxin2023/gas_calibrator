import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_sencoa_sencob_writer_design_review import main as cli_main
from gas_calibrator.validation.v1_5_sencoa_sencob_writer_design import (
    build_v1_5_sencoa_sencob_writer_design_review,
    write_v1_5_sencoa_sencob_writer_design_review,
)


ROOT = Path(__file__).resolve().parents[1]
PROFILE_PATH = ROOT / "configs" / "v1_5_algorithm_route_profiles.json"


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def test_sencoa_sencob_writer_design_is_offline_and_blocked() -> None:
    tables = build_v1_5_sencoa_sencob_writer_design_review(PROFILE_PATH)
    manifest = tables["manifest"]
    preflight = {row["gate"]: row for row in tables["no_write_preflight"]}

    assert manifest["no_write"] is True
    assert manifest["opens_com_ports"] is False
    assert manifest["controls_water_or_gas_routes"] is False
    assert manifest["writes_coefficients"] is False
    assert manifest["writer_status"] == "design_only_no_real_writer"
    assert manifest["production_state"] == "blocked"
    assert manifest["minimum_serial_command_gap_s"] >= 1.0
    assert preflight["real_writer_exists"]["status"] == "blocked"
    assert preflight["serial_command_gap"]["status"] == "pass"


def test_sencoa_sencob_payload_contracts_define_commands_and_readbacks() -> None:
    rows = {
        row["coefficient_group"]: row
        for row in build_v1_5_sencoa_sencob_writer_design_review(PROFILE_PATH)["payload_contracts"]
    }

    assert set(rows) == {"SENCOA", "SENCOB"}
    assert rows["SENCOA"]["component"] == "co2"
    assert rows["SENCOA"]["readback_group"] == "GETCOA"
    assert rows["SENCOA"]["physical_quantity"] == "R0_CO2(T)"
    assert rows["SENCOA"]["write_command_template"] == (
        "SENCOA,YGAS,<target>,<c0>,<c1>,<c2>,<c3>"
    )
    assert rows["SENCOA"]["readback_command_template"] == "GETCOA,YGAS,<target>"
    assert rows["SENCOB"]["component"] == "h2o"
    assert rows["SENCOB"]["readback_group"] == "GETCOB"
    assert rows["SENCOB"]["physical_quantity"] == "R0_H2O(T)"
    assert rows["SENCOB"]["write_command_template"] == (
        "SENCOB,YGAS,<target>,<c0>,<c1>,<c2>,<c3>"
    )
    assert rows["SENCOB"]["readback_command_template"] == "GETCOB,YGAS,<target>"
    assert {row["payload_width"] for row in rows.values()} == {4}
    assert {row["status"] for row in rows.values()} == {"design_only_blocked_no_real_writer"}


def test_sencoa_sencob_snapshot_and_rollback_contracts_are_explicit() -> None:
    tables = build_v1_5_sencoa_sencob_writer_design_review(PROFILE_PATH)
    snapshot_steps = {row["step"]: row for row in tables["snapshot_plan"]}
    rollback_triggers = {row["trigger"]: row for row in tables["rollback_plan"]}

    assert "old_r0_snapshot" in snapshot_steps
    assert snapshot_steps["old_r0_snapshot"]["contents"] == (
        "GETCOA_before;GETCOB_before;raw_lines;timestamps;sha256"
    )
    assert "old_main_chain_snapshot" in snapshot_steps
    assert "write_ack_missing_or_readback_mismatch" in rollback_triggers
    assert rollback_triggers["write_ack_missing_or_readback_mismatch"]["order"] == (
        "reverse_changed_order"
    )
    assert rollback_triggers["SENCOA_success_then_SENCOB_failure"]["continue_policy"] == (
        "do_not_attempt_partial_production_acceptance"
    )


def test_sencoa_sencob_writer_design_writer_and_cli_create_artifacts(tmp_path: Path) -> None:
    output = tmp_path / "design"
    outputs = write_v1_5_sencoa_sencob_writer_design_review(PROFILE_PATH, output)

    manifest = json.loads(
        output.joinpath("v1_5_sencoa_sencob_writer_design_manifest.json").read_text(
            encoding="utf-8-sig"
        )
    )
    assert manifest["profile_id"] == "absorption_ratio_shadow"
    assert manifest["not_real_acceptance_evidence"] is True
    assert Path(outputs["payload_contracts"]).exists()
    rows = _read_csv(output / "v1_5_sencoa_sencob_payload_contracts.csv")
    assert {row["coefficient_group"] for row in rows} == {"SENCOA", "SENCOB"}

    cli_output = tmp_path / "cli_design"
    rc = cli_main(
        [
            "--profile-path",
            str(PROFILE_PATH),
            "--output-dir",
            str(cli_output),
        ]
    )
    assert rc == 0
    assert cli_output.joinpath("V1_5_SENCOA_SENCOB_WRITER_DESIGN_REVIEW.md").exists()
    cli_preflight = _read_csv(cli_output / "v1_5_sencoa_sencob_no_write_preflight.csv")
    assert any(
        row["gate"] == "real_writer_exists" and row["status"] == "blocked"
        for row in cli_preflight
    )
