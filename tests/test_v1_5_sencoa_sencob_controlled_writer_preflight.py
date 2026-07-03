import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_sencoa_sencob_controlled_writer_preflight import main as cli_main
from gas_calibrator.validation.v1_5_sencoa_sencob_controlled_writer_preflight import (
    build_v1_5_sencoa_sencob_controlled_writer_preflight,
    write_v1_5_sencoa_sencob_controlled_writer_preflight,
)


ROOT = Path(__file__).resolve().parents[1]
PROFILE_PATH = ROOT / "configs" / "v1_5_algorithm_route_profiles.json"


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_payload_review(path: Path) -> None:
    rows = [
        {
            "analyzer_device_id": "001",
            "sn_code": "01260607",
            "coefficient_group": "SENCOA",
            "target": "001",
            "payload_values": json.dumps([1.0, 0.1, 0.01, 0.001]),
            "source_model_hash": "sha256:co2",
        },
        {
            "analyzer_device_id": "001",
            "sn_code": "01260607",
            "coefficient_group": "SENCOB",
            "target": "001",
            "payload_values": json.dumps([2.0, 0.2, 0.02, 0.002]),
            "source_model_hash": "sha256:h2o",
        },
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _write_old_snapshot(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "001": {
                    "sn_code": "01260607",
                    "GETCOA_before": [0.0, 1.0, 0.0, 0.0],
                    "GETCOB_before": [0.0, 1.0, 0.0, 0.0],
                    "raw_lines": {"GETCOA": "GETCOA,YGAS,001,0,1,0,0"},
                    "timestamps": {"GETCOA": "2026-06-30T00:00:00"},
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def test_controlled_writer_preflight_is_offline_and_still_blocks_real_write() -> None:
    tables = build_v1_5_sencoa_sencob_controlled_writer_preflight(PROFILE_PATH)
    manifest = tables["manifest"]
    gates = {row["gate"]: row for row in tables["preflight_gates"]}

    assert manifest["no_write"] is True
    assert manifest["opens_com_ports"] is False
    assert manifest["uses_gas_analyzer"] is False
    assert manifest["writes_coefficients"] is False
    assert manifest["real_write_unlock_status"] == "blocked_pending_real_writer_implementation"
    assert manifest["minimum_serial_command_gap_s"] >= 1.0
    assert gates["real_writer_implementation"]["status"] == "blocked"
    assert gates["reviewed_payload_available"]["status"] == "planned"
    assert gates["old_getcoa_getcob_snapshot_available"]["status"] == "planned"


def test_controlled_writer_preflight_accepts_reviewed_payload_and_snapshot_but_keeps_writer_blocked(
    tmp_path: Path,
) -> None:
    payload = tmp_path / "payload.csv"
    snapshot = tmp_path / "snapshot.json"
    _write_payload_review(payload)
    _write_old_snapshot(snapshot)

    tables = build_v1_5_sencoa_sencob_controlled_writer_preflight(
        PROFILE_PATH,
        payload_review_path=payload,
        old_snapshot_json=snapshot,
    )
    gates = {row["gate"]: row for row in tables["preflight_gates"]}
    payload_rows = {row["coefficient_group"]: row for row in tables["payload_review"]}

    assert gates["reviewed_payload_available"]["status"] == "pass"
    assert gates["old_getcoa_getcob_snapshot_available"]["status"] == "pass"
    assert gates["real_writer_implementation"]["status"] == "blocked"
    assert set(payload_rows) == {"SENCOA", "SENCOB"}
    assert payload_rows["SENCOA"]["readback_group"] == "GETCOA"
    assert payload_rows["SENCOB"]["readback_group"] == "GETCOB"


def test_controlled_writer_preflight_rejects_subsecond_future_command_gap() -> None:
    try:
        build_v1_5_sencoa_sencob_controlled_writer_preflight(
            PROFILE_PATH,
            future_command_gap_s=0.5,
        )
    except ValueError as exc:
        assert "below 1s" in str(exc)
    else:
        raise AssertionError("expected subsecond command gap to be rejected")


def test_controlled_writer_preflight_writer_and_cli_create_artifacts(tmp_path: Path) -> None:
    payload = tmp_path / "payload.csv"
    snapshot = tmp_path / "snapshot.json"
    _write_payload_review(payload)
    _write_old_snapshot(snapshot)

    output = tmp_path / "preflight"
    outputs = write_v1_5_sencoa_sencob_controlled_writer_preflight(
        PROFILE_PATH,
        output,
        payload_review_path=payload,
        old_snapshot_json=snapshot,
    )
    assert Path(outputs["manifest"]).exists()
    gates = _read_csv(output / "v1_5_sencoa_sencob_controlled_writer_preflight_gates.csv")
    assert any(row["gate"] == "real_writer_implementation" and row["status"] == "blocked" for row in gates)

    cli_output = tmp_path / "cli_preflight"
    rc = cli_main(
        [
            "--profile-path",
            str(PROFILE_PATH),
            "--output-dir",
            str(cli_output),
            "--payload-review",
            str(payload),
            "--old-snapshot-json",
            str(snapshot),
        ]
    )
    assert rc == 0
    assert cli_output.joinpath("V1_5_SENCOA_SENCOB_CONTROLLED_WRITER_PREFLIGHT.md").exists()
