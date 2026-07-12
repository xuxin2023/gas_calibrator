import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_historical_mature_root_discovery import main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_historical_mature_root_discovery import (
    build_v1_5_historical_mature_root_discovery,
    write_v1_5_historical_mature_root_discovery,
)


def _json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _csv(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return path


def _fixture(tmp_path: Path, *, root_name: str = "continuous_route", route_kind: str = "co2") -> dict:
    root = tmp_path / root_name
    point_id = "p001_T40_0ppm_fit" if route_kind == "co2" else "p001_T0_HG0C_50RH_h2o"
    point = root / point_id
    sidecar = (
        "formal_open_flow_sidecar_metadata.json"
        if route_kind == "co2"
        else "formal_h2o_open_flow_sidecar_metadata.json"
    )
    _json(point / sidecar, {"run_id": point_id})
    _csv(point / "samples_machine_readable.csv", [{"ga01_id": "001"}])
    _csv(point / "formal_open_flow_data_quality_by_analyzer.csv", [{"label": "ga01", "grade": "A"}])
    manifest_row = {
        "point_run_id": point_id,
        "returncode": "0",
        "status": "ok",
    }
    summary_dir = root / "queue"
    manifest = _csv(summary_dir / "queue_manifest.csv", [manifest_row])
    schema = "v1_5_co2_open_flow_queue_v0" if route_kind == "co2" else "v1_5_h2o_open_flow_queue_v0"
    summary = _json(
        summary_dir / "queue_summary.json",
        {
            "schema_version": schema,
            "queue_run_id": f"reviewed_{route_kind}_continuous",
            "config_path": str(tmp_path / "runtime.json"),
            "queue_csv": str(tmp_path / "queue.csv"),
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
    profile = _json(
        tmp_path / "profiles.json",
        {
            "profiles": [
                {
                    "profile_id": "test_profile",
                    "co2_route": {"formal_point_count": 1},
                    "h2o_route": {"formal_point_count": 1},
                }
            ]
        },
    )
    return {"root": root, "point": point, "summary": summary, "manifest": manifest, "profile": profile}


def _build(fixture: dict, summaries: list[Path] | None = None) -> dict:
    return build_v1_5_historical_mature_root_discovery(
        queue_summary_paths=summaries or [fixture["summary"]],
        algorithm_profile_path=fixture["profile"],
    )


def test_complete_candidate_is_forwarded_but_never_fit_authorized(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    model = _build(fixture)
    outputs = write_v1_5_historical_mature_root_discovery(model, tmp_path / "out")
    candidate = model["candidates"][0]
    assert model["overall_status"] == "ready_for_attestation_review"
    assert model["attestation_input_candidate_count"] == 1
    assert candidate["classification"] == "attestation_input_candidate"
    assert candidate["matching_profile_ids"] == ["test_profile"]
    assert candidate["historical_fit_allowed"] is False
    assert model["historical_fit_allowed"] is False
    assert model["opens_com_ports"] is False
    assert json.loads(outputs["attestation_candidates_json"].read_text(encoding="utf-8"))["evidence_roots"]


def test_0624_and_dry_run_are_not_candidates(tmp_path: Path) -> None:
    forbidden = _fixture(tmp_path / "forbidden", root_name="route_20260624")
    dry = _fixture(tmp_path.parent / "independent_dry_case")
    payload = json.loads(dry["summary"].read_text(encoding="utf-8"))
    payload["dry_run"] = True
    _json(dry["summary"], payload)
    model = build_v1_5_historical_mature_root_discovery(
        queue_summary_paths=[forbidden["summary"], dry["summary"]],
        algorithm_profile_path=forbidden["profile"],
    )
    classifications = {row["classification"] for row in model["candidates"]}
    assert model["attestation_input_candidate_count"] == 0
    assert "forbidden_source" in classifications
    assert "dry_run_only" in classifications


def test_missing_point_qc_and_unfinalized_summary_remain_review_required(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    (fixture["point"] / "formal_open_flow_data_quality_by_analyzer.csv").unlink()
    payload = json.loads(fixture["summary"].read_text(encoding="utf-8"))
    payload.pop("ok_points")
    _json(fixture["summary"], payload)
    model = _build(fixture)
    codes = set(model["candidates"][0]["blocker_codes"])
    assert "point_component_qc_incomplete" in codes
    assert "queue_summary_not_finalized_clean" in codes
    assert model["attestation_candidate_replay"]["evidence_roots"] == []


def test_duplicate_summaries_for_one_output_root_are_blocked(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    second_dir = fixture["root"] / "queue_second"
    second_summary = _json(
        second_dir / "queue_summary.json",
        json.loads(fixture["summary"].read_text(encoding="utf-8")),
    )
    _csv(
        second_dir / "queue_manifest.csv",
        [{"point_run_id": fixture["point"].name, "returncode": "0", "status": "ok"}],
    )
    model = _build(fixture, [fixture["summary"], second_summary])
    assert model["attestation_input_candidate_count"] == 0
    assert all(
        "duplicate_queue_summary_for_output_root" in row["blocker_codes"]
        for row in model["candidates"]
    )


def test_cli_list_file_and_entrypoint_are_offline(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    summary_list = tmp_path / "summaries.txt"
    summary_list.write_text(str(fixture["summary"]) + "\n", encoding="utf-8")
    rc = main(
        [
            "--summary-list-path",
            str(summary_list),
            "--algorithm-profile-path",
            str(fixture["profile"]),
            "--output-dir",
            str(tmp_path / "out"),
            "--fail-if-no-candidate",
        ]
    )
    entry = classify_v1_5_entrypoint(
        Path("src/gas_calibrator/tools/export_v1_5_historical_mature_root_discovery.py"),
        root=Path.cwd(),
    )
    assert rc == 0
    assert entry.category == "formal_review_evidence"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
