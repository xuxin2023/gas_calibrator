import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_legacy_historical_evidence_catalog import main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_legacy_historical_evidence_catalog import (
    build_v1_5_legacy_historical_evidence_catalog,
    write_v1_5_legacy_historical_evidence_catalog,
)


def _json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _csv(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return path


def _point(root: Path, name: str, *, route_kind: str = "co2", with_component_qc: bool = True) -> Path:
    point = root / name
    sidecar = (
        "formal_open_flow_sidecar_metadata.json"
        if route_kind == "co2"
        else "formal_h2o_open_flow_sidecar_metadata.json"
    )
    _json(point / sidecar, {"run_id": name, "writes_senco": False})
    _csv(point / "samples_machine_readable.csv", [{"label": "ga01", "ratio": "0.5"}])
    _csv(point / "frame_quality_summary.csv", [{"label": "ga01", "grade": "A"}])
    if with_component_qc:
        _csv(point / "formal_open_flow_data_quality_by_analyzer.csv", [{"label": "ga01", "grade": "A"}])
    return point


def test_segmented_point_is_hashed_but_never_promoted(tmp_path: Path) -> None:
    point = _point(tmp_path / "segmented_g3", "p001_T40_0ppm_fit")
    model = build_v1_5_legacy_historical_evidence_catalog(search_roots=[tmp_path])
    row = model["points"][0]
    assert row["point_dir"] == str(point.resolve())
    assert row["has_samples"] is True
    assert row["has_component_qc"] is True
    assert row["diagnostic_data_review_allowed"] is True
    assert row["continuous_route_attestation_allowed"] is False
    assert row["formal_fit_allowed"] is False
    assert model["historical_fit_allowed"] is False
    assert model["opens_com_ports"] is False
    assert model["diagnostic_data_review_count"] == 1
    assert model["missing_component_qc_counts"] == {"co2": 0, "h2o": 0}


def test_accepted_45_of_45_composite_stays_diagnostic_only(tmp_path: Path) -> None:
    point = _point(tmp_path / "co2_g3", "p002_T40_400ppm_fit_retry1")
    manifest = _csv(
        tmp_path / "accepted" / "accepted_co2_45_point_manifest.csv",
        [
            {
                "canonical_index": "2",
                "point_dir": str(point),
                "source_kind": "retry_or_direct_recovery",
                "acceptance_status": "accepted",
                "warning": "",
            }
        ],
    )
    model = build_v1_5_legacy_historical_evidence_catalog(
        search_roots=[tmp_path], accepted_manifest_paths=[manifest]
    )
    row = next(item for item in model["points"] if item["point_dir"] == str(point.resolve()))
    accepted = model["accepted_composite_manifest_rows"][0]
    assert row["accepted_manifest_member"] is True
    assert row["lineage_classification"] == "segmented_retry_or_recovery"
    assert accepted["diagnostic_composite_only"] is True
    assert accepted["continuous_route_attestation_allowed"] is False
    assert accepted["formal_fit_allowed"] is False


def test_missing_component_qc_requires_traceability_review(tmp_path: Path) -> None:
    _point(tmp_path / "g4", "p001_T0_HG0C_50RH_h2o", route_kind="h2o", with_component_qc=False)
    model = build_v1_5_legacy_historical_evidence_catalog(search_roots=[tmp_path])
    row = model["points"][0]
    assert row["route_kind"] == "h2o"
    assert row["has_component_qc"] is False
    assert row["traceability_review_required"] is True
    assert row["formal_fit_allowed"] is False
    assert model["missing_component_qc_counts"] == {"co2": 0, "h2o": 1}


def test_0624_and_legacy_archive_are_classified_without_promotion(tmp_path: Path) -> None:
    forbidden = tmp_path / "handoff_20260624"
    archive = tmp_path / "_gas_calibrator_archive" / "20260622_first_gate"
    _point(forbidden, "p001_T40_0ppm_fit")
    archive.mkdir(parents=True)
    model = build_v1_5_legacy_historical_evidence_catalog(search_roots=[forbidden, archive])
    roots = {Path(row["root_path"]).name: row for row in model["roots"]}
    assert roots["handoff_20260624"]["classification"] == "forbidden_0624_or_migration"
    assert roots["20260622_first_gate"]["classification"] == "legacy_non_v1_5_archive"
    assert all(row["formal_promotion_allowed"] is False for row in roots.values())


def test_anchor_roles_are_not_inferred_or_conflated(tmp_path: Path) -> None:
    _point(tmp_path / "co2", "p001_T40_0ppm_fit")
    _point(tmp_path / "h2o", "p001_T0_HG0C_50RH_h2o", route_kind="h2o")
    model = build_v1_5_legacy_historical_evidence_catalog(search_roots=[tmp_path])
    contract = model["interpretation_contract"]
    assert contract["co2_zero_and_h2o_dry_anchor_are_interchangeable"] is False
    assert contract["anchor_role_inference_allowed"] is False


def test_writer_cli_and_entrypoint_are_offline(tmp_path: Path) -> None:
    _point(tmp_path / "g3", "p001_T40_0ppm_fit")
    model = build_v1_5_legacy_historical_evidence_catalog(search_roots=[tmp_path])
    outputs = write_v1_5_legacy_historical_evidence_catalog(model, tmp_path / "direct_out")
    rc = main(["--search-root", str(tmp_path), "--output-dir", str(tmp_path / "cli_out")])
    entry = classify_v1_5_entrypoint(
        Path("src/gas_calibrator/tools/export_v1_5_legacy_historical_evidence_catalog.py"),
        root=Path.cwd(),
    )
    assert rc == 0
    assert outputs["json"].is_file()
    assert outputs["points_csv"].is_file()
    assert entry.category == "formal_review_evidence"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
