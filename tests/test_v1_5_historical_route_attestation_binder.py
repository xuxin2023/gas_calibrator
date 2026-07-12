import csv
import json
from pathlib import Path

import pytest

from gas_calibrator.tools.export_v1_5_historical_route_attestation_binder import main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_historical_route_attestation_binder import (
    build_v1_5_historical_route_attestation_binder,
    write_v1_5_historical_route_attestation_binder,
)
from gas_calibrator.validation.v1_5_historical_fit_evidence_normalizer import (
    _attestations,
    _route_baseline,
)


def _json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _csv(path: Path, rows: list[dict]) -> Path:
    fields = list(rows[0])
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _fixture(tmp_path: Path, route_kind: str = "co2", root_name: str = "reviewed_route") -> dict:
    root = tmp_path / root_name
    family = f"family_{route_kind}"
    point_id = "p001_T40_0ppm_fit" if route_kind == "co2" else "p001_T0_HG0C_50RH_h2o"
    point = root / point_id
    profile_route = (
        {"formal_point_count": 1, "temperature_plan": {"40": [0]}}
        if route_kind == "co2"
        else {"formal_point_count": 1, "temperature_plan": {"0": ["HGEN0C_50RH"]}}
    )
    profile = _json(
        tmp_path / "profiles.json",
        {
            "profiles": [
                {
                    "profile_id": "legacy_ratio_production",
                    "co2_route": profile_route if route_kind == "co2" else {},
                    "h2o_route": profile_route if route_kind == "h2o" else {},
                }
            ]
        },
    )
    replay = _json(
        tmp_path / "replay.json",
        {
            "evidence_roots": [
                {
                    "family_id": family,
                    "route_kind": route_kind,
                    "root_path": str(root),
                    "algorithm_profile_id": "legacy_ratio_production",
                }
            ]
        },
    )
    mature = _json(
        tmp_path / "mature.json",
        {
            "manifest": {
                "status": "pass",
                "blocker_count": 0,
                "mature_route_contract": {
                    "route_behavior": "preserve_mature_v1_5_0620_route_timing_and_quality_gates",
                    "co2_runner": "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue",
                    "h2o_runner": "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue",
                },
            }
        },
    )
    automation = _json(
        tmp_path / "automation.json",
        {
            "manifest": {
                "status": "pass",
                "blocker_count": 0,
                "mature_fitting_baseline": "0613-style V1.5 fitting method",
                "mature_physical_baseline": "0620/0621 mature physical execution path",
            }
        },
    )
    manifest_row = {
        "point_run_id": point_id,
        "temp_c": "40" if route_kind == "co2" else "0",
        "source_nominal_ppm": "0" if route_kind == "co2" else "",
        "hgen_temp_c": "" if route_kind == "co2" else "0",
        "hgen_rh_pct": "" if route_kind == "co2" else "50",
        "returncode": "0",
        "status": "ok",
    }
    _csv(root / "queue" / "queue_manifest.csv", [manifest_row])
    _json(
        root / "queue" / "queue_summary.json",
        {
            "schema_version": (
                "v1_5_co2_open_flow_queue_v0"
                if route_kind == "co2"
                else "v1_5_h2o_open_flow_queue_v0"
            ),
            "queue_run_id": f"reviewed_{route_kind}_continuous",
            "config_path": str(tmp_path / "reviewed_config.json"),
            "queue_csv": str(tmp_path / "canonical_queue.csv"),
            "output_dir": str(root),
            "selected_points": 1,
            "ok_points": 1,
            "failed_points": 0,
            "dry_run": False,
            "no_write": True,
            "writes_senco": False,
            "writes_device_id": False,
            "hard_failure": False,
            "formal_route_readiness": {"status": "pass", "ok": True},
        },
    )
    sidecar = {
        "schema_version": (
            "v1_5_formal_open_flow_sidecar_v0"
            if route_kind == "co2"
            else "v1_5_formal_h2o_open_flow_sidecar_v0"
        ),
        "run_id": point_id,
        "pace_mode": "continuous_atmosphere_hold",
        "continuous_atmosphere_hold_scope": (
            "open_flow_purge_and_sampling_only"
            if route_kind == "co2"
            else "h2o_open_flow_purge_and_sampling_only"
        ),
        "route_open_until_sample_end": True,
        "analyzer_acquisition_policy": "active_mode2_stream_1hz_ftd01_controlled",
        "analyzer_stream_native_hz": 1.0,
        "formal_sample_anchor_interval_s": 1.0,
        "actual_purge_s": 360.0,
        "minimum_purge_s": 360.0,
        "writes_senco": False,
        "writes_device_id": False,
        "sealed_pressure_control": False,
    }
    if route_kind == "co2":
        sidecar.update(
            {
                "gas_route_dewpoint_gate_enabled": True,
                "gas_route_dewpoint_gate_policy": "reject",
                "gas_route_dewpoint_gate_dry_enough_c": -28.0,
            }
        )
    sidecar_name = "formal_open_flow_sidecar_metadata.json"
    if route_kind == "h2o":
        sidecar_name = "formal_h2o_open_flow_sidecar_metadata.json"
        sidecar.update(
            {
                "h2o_hgen_shutdown_policy": "queue_managed_keep_running_between_points",
                "h2o_open_flow_wait_contract": "v1_5_dewpoint_tail_h2o_ratio_with_pressure_diagnostic_only",
                "h2o_pressure_presample_policy": "skip",
            }
        )
    _json(point / sidecar_name, sidecar)
    _csv(point / "samples_machine_readable.csv", [{"ga01_id": "001", "ga01_frame_usable": "True"}])
    _csv(
        point / "formal_open_flow_data_quality_by_analyzer.csv",
        [{"label": "ga01", "grade": "A_calibration_eligible"}],
    )
    return {
        "root": root,
        "replay": replay,
        "profile": profile,
        "mature": mature,
        "automation": automation,
        "point": point,
    }


def _build(fixture: dict) -> dict:
    return build_v1_5_historical_route_attestation_binder(
        historical_replay_evidence_json=fixture["replay"],
        algorithm_profile_path=fixture["profile"],
        mature_route_contract_json=fixture["mature"],
        automation_control_contract_json=fixture["automation"],
        reviewer="offline-reviewer",
        reviewed_at="2026-07-13T12:00:00+08:00",
    )


@pytest.mark.parametrize("route_kind", ["co2", "h2o"])
def test_complete_exact_root_emits_reviewed_attestation(tmp_path: Path, route_kind: str) -> None:
    fixture = _fixture(tmp_path, route_kind)
    model = _build(fixture)
    outputs = write_v1_5_historical_route_attestation_binder(model, tmp_path / "out")
    payload = json.loads(outputs["attestation_json"].read_text(encoding="utf-8"))
    assert model["overall_status"] == "pass"
    assert model["blocker_count"] == 0
    assert payload["families"][0]["status"] == "reviewed"
    assert payload["families"][0]["root_path"] == str(fixture["root"].resolve())
    assert payload["families"][0]["fitting_baseline"] == "0613"
    assert payload["families"][0]["route_baseline"] == ("0620" if route_kind == "co2" else "0621")
    assert len(payload["families"][0]["queue_manifest_sha256"]) == 64
    assert len(payload["families"][0]["evidence_inventory_sha256"]) == 64
    assert model["opens_com_ports"] is False
    assert model["connects_postgresql"] is False

    attestations = _attestations(outputs["attestation_json"])
    baseline, reasons = _route_baseline(
        {"family_id": f"family_{route_kind}", "route_kind": route_kind, "root_path": fixture["root"]},
        attestations[f"family_{route_kind}:{route_kind}"],
    )
    assert baseline == ("0620" if route_kind == "co2" else "0621")
    assert reasons == []


@pytest.mark.parametrize(
    ("root_name", "code"),
    [
        ("route_20260624", "0624_source_forbidden"),
        ("route_migration", "migration_source_forbidden"),
        ("route_segmented", "segmented_source_forbidden"),
        ("route_retry", "retry_source_forbidden"),
        ("route_direct", "direct_source_forbidden"),
        ("route_recovery", "recovery_source_forbidden"),
        ("route_diagnostic", "diagnostic_source_forbidden"),
    ],
)
def test_forbidden_historical_lineage_never_attests(tmp_path: Path, root_name: str, code: str) -> None:
    model = _build(_fixture(tmp_path, root_name=root_name))
    assert model["overall_status"] == "blocked"
    assert model["families"] == []
    assert code in {row["code"] for row in model["blockers"]}


def test_failed_queue_and_missing_quality_block_attestation(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    summary = fixture["root"] / "queue" / "queue_summary.json"
    payload = json.loads(summary.read_text(encoding="utf-8"))
    payload.update({"ok_points": 0, "failed_points": 1, "hard_failure": True})
    _json(summary, payload)
    (fixture["point"] / "formal_open_flow_data_quality_by_analyzer.csv").unlink()
    model = _build(fixture)
    codes = {row["code"] for row in model["blockers"]}
    assert "queue_summary_not_closed_clean_nowrite" in codes
    assert "point_component_quality_missing" in codes
    assert model["families"] == []


def test_h2o_shorter_actual_purge_and_broken_contract_block(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, "h2o")
    sidecar = fixture["point"] / "formal_h2o_open_flow_sidecar_metadata.json"
    payload = json.loads(sidecar.read_text(encoding="utf-8"))
    payload["actual_purge_s"] = 360.0
    payload["minimum_purge_s"] = 720.0
    _json(sidecar, payload)
    automation = json.loads(fixture["automation"].read_text(encoding="utf-8"))
    automation["manifest"]["mature_fitting_baseline"] = "V1 old method"
    _json(fixture["automation"], automation)
    model = _build(fixture)
    codes = {row["code"] for row in model["blockers"]}
    assert "point_actual_purge_below_minimum" in codes
    assert "0613_fitting_contract_missing" in codes


def test_duplicate_family_route_roots_are_blocked(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    replay = json.loads(fixture["replay"].read_text(encoding="utf-8"))
    replay["evidence_roots"].append(dict(replay["evidence_roots"][0]))
    _json(fixture["replay"], replay)
    model = _build(fixture)
    assert "duplicate_family_route_root" in {row["code"] for row in model["blockers"]}
    assert model["families"] == []


def test_empty_or_missing_root_fails_closed_without_scanning_cwd(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    _json(fixture["replay"], {"evidence_roots": []})
    empty = _build(fixture)
    assert empty["overall_status"] == "blocked"
    assert empty["blocker_count"] == 1
    assert empty["blockers"][0]["code"] == "historical_evidence_roots_missing"

    _json(
        fixture["replay"],
        {
            "evidence_roots": [
                {
                    "family_id": "missing",
                    "route_kind": "co2",
                    "root_path": "",
                    "algorithm_profile_id": "legacy_ratio_production",
                }
            ]
        },
    )
    missing = _build(fixture)
    assert "root_path_missing" in {row["code"] for row in missing["blockers"]}
    assert missing["families"] == []


def test_cli_fail_on_blocker_and_entrypoint_is_offline(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, root_name="route_direct")
    rc = main(
        [
            "--historical-replay-evidence-json",
            str(fixture["replay"]),
            "--algorithm-profile-path",
            str(fixture["profile"]),
            "--mature-route-contract-json",
            str(fixture["mature"]),
            "--automation-control-contract-json",
            str(fixture["automation"]),
            "--reviewer",
            "offline-reviewer",
            "--reviewed-at",
            "2026-07-13T12:00:00+08:00",
            "--output-dir",
            str(tmp_path / "out"),
            "--fail-on-blocker",
        ]
    )
    entry = classify_v1_5_entrypoint(
        Path("src/gas_calibrator/tools/export_v1_5_historical_route_attestation_binder.py"),
        root=Path.cwd(),
    )
    assert rc == 2
    assert entry.category == "formal_review_evidence"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
