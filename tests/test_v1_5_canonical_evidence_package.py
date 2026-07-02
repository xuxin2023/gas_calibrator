import csv
import json

from gas_calibrator.tools.prepare_v1_5_canonical_evidence_package import main as canonical_main
from gas_calibrator.validation.v1_5_canonical_evidence import write_canonical_v1_5_evidence_package


def _read_csv(path):
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def test_canonical_package_generates_complete_simulated_no_write_evidence(tmp_path):
    outputs = write_canonical_v1_5_evidence_package(tmp_path / "canonical")

    for path in outputs.values():
        assert path.exists(), path

    manifest = json.loads(outputs["canonical_manifest"].read_text(encoding="utf-8"))
    sidecar = json.loads(outputs["sidecar_summary"].read_text(encoding="utf-8"))
    bundle = json.loads(outputs["evidence_bundle"].read_text(encoding="utf-8"))
    samples = _read_csv(outputs["samples"])

    assert manifest["evidence_source"] == "simulated"
    assert manifest["not_real_acceptance_evidence"] is True
    assert manifest["opens_com_ports"] is False
    assert manifest["controls_water_or_gas_routes"] is False
    assert manifest["controls_valves_or_pace"] is False
    assert manifest["writes_coefficients"] is False
    assert manifest["artifact_count"] == len(manifest["artifacts"])
    assert all(row["sha256"] for row in manifest["artifacts"])
    assert {"raw_samples", "pressure_channel_quick_check", "evidence_bundle", "formal_report"}.issubset(
        {row["artifact_role"] for row in manifest["artifacts"]}
    )

    assert sidecar["evidence_bundle"]["evidence_status"] == "ready_for_reviewer"
    assert sidecar["evidence_bundle"]["package_status"] == "ready_for_reviewer"
    assert bundle["tables"]["runs"][0]["package_status"] == "ready_for_reviewer"
    assert len(samples) == 21
    h2o_rows = [row for row in samples if row.get("route") == "h2o"]
    assert len(h2o_rows) == 10
    assert all(row["pressure_mode"] == "ambient_open" for row in h2o_rows)
    for field in (
        "dewpoint_c",
        "h2o_dry_ppmv",
        "h2o_wet_ppmv",
        "ga01_h2o_signal",
        "ga01_h2o_ratio_f",
        "ga01_h2o_mmol",
    ):
        assert all(row[field] for row in h2o_rows), field
    sealed = [row for row in samples if row.get("pressure_mode") == "sealed_controlled"]
    assert len(sealed) == 1
    assert sealed[0]["sample_role"] == "engineering_diagnostic_only"
    assert sealed[0]["not_real_acceptance_evidence"] == "true"

    report_model = json.loads(outputs["report_report_model"].read_text(encoding="utf-8"))
    assert report_model["decision"]["decision_status"] == "candidate_coefficients_generated_no_write"
    assert report_model["report_release_decision"]["release_status"] == "draft_only"


def test_canonical_package_cli_can_generate_evidence_bundle_without_reports(tmp_path):
    output_dir = tmp_path / "canonical_cli"
    rc = canonical_main(["--output-dir", str(output_dir), "--skip-reports"])

    assert rc == 0
    manifest_path = output_dir / "canonical_manifest.json"
    bundle_path = output_dir / "run" / "formal_evidence_sidecar" / "evidence_bundle.json"
    assert manifest_path.exists()
    assert bundle_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["report_outputs"] == {}
    assert manifest["sidecar_only"] is True
    assert manifest["physical_meaning"]["pressure_channel"].startswith("Analyzer internal pressure P")
    assert "H2O open-flow" in manifest["physical_meaning"]["water_route"]
