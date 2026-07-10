import json

from gas_calibrator.validation.v1_5_artifact_hash_binding import (
    validate_artifact_hash_manifest,
    write_artifact_hash_manifest,
)


def test_artifact_hash_manifest_round_trip(tmp_path):
    source = tmp_path / "source.csv"
    source.write_text("status\npass\n", encoding="utf-8")
    manifest = write_artifact_hash_manifest(
        tmp_path / "manifest.json",
        artifacts={"fit_input_quality_summary": source},
    )

    ok, reasons, detail = validate_artifact_hash_manifest(
        manifest,
        required_roles=["fit_input_quality_summary"],
        expected_paths={"fit_input_quality_summary": source},
    )

    assert ok is True
    assert reasons == []
    assert detail["artifact_count"] == 1


def test_artifact_hash_manifest_rejects_same_path_replacement(tmp_path):
    source = tmp_path / "source.csv"
    source.write_text("status\npass\n", encoding="utf-8")
    manifest = write_artifact_hash_manifest(
        tmp_path / "manifest.json",
        artifacts={"fit_input_quality_summary": source},
    )
    source.write_text("status\nreplaced\n", encoding="utf-8")

    ok, reasons, _ = validate_artifact_hash_manifest(
        manifest,
        required_roles=["fit_input_quality_summary"],
    )

    assert ok is False
    assert "artifact_hash_manifest_sha256_mismatch:fit_input_quality_summary" in reasons


def test_artifact_hash_manifest_rejects_missing_role_and_path_rebind(tmp_path):
    source = tmp_path / "source.csv"
    other = tmp_path / "other.csv"
    source.write_text("status\npass\n", encoding="utf-8")
    other.write_text("status\npass\n", encoding="utf-8")
    manifest = write_artifact_hash_manifest(
        tmp_path / "manifest.json",
        artifacts={"fit_input_quality_summary": source},
    )

    ok, reasons, _ = validate_artifact_hash_manifest(
        manifest,
        required_roles=["fit_input_quality_summary", "fit_input_quality_devices"],
        expected_paths={"fit_input_quality_summary": other},
    )

    assert ok is False
    assert "artifact_hash_manifest_required_role_missing:fit_input_quality_devices" in reasons
    assert "artifact_hash_manifest_path_mismatch:fit_input_quality_summary" in reasons


def test_artifact_hash_manifest_rejects_duplicate_role(tmp_path):
    source = tmp_path / "source.csv"
    source.write_text("status\npass\n", encoding="utf-8")
    manifest = write_artifact_hash_manifest(
        tmp_path / "manifest.json",
        artifacts={"fit_input_quality_summary": source},
    )
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["artifacts"].append(dict(payload["artifacts"][0]))
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    ok, reasons, _ = validate_artifact_hash_manifest(
        manifest,
        required_roles=["fit_input_quality_summary"],
    )

    assert ok is False
    assert "artifact_hash_manifest_duplicate_role:fit_input_quality_summary" in reasons


def test_artifact_hash_manifest_preserves_component_isolation(tmp_path):
    co2_source = tmp_path / "co2.csv"
    h2o_source = tmp_path / "h2o.csv"
    co2_source.write_text("status\npass\n", encoding="utf-8")
    h2o_source.write_text("status\npass\n", encoding="utf-8")
    manifest = write_artifact_hash_manifest(
        tmp_path / "manifest.json",
        artifacts={
            "co2_fit_input_quality_summary": co2_source,
            "h2o_fit_input_quality_summary": h2o_source,
        },
    )
    h2o_source.write_text("status\nreplaced\n", encoding="utf-8")

    ok, reasons, _ = validate_artifact_hash_manifest(
        manifest,
        required_roles=["co2_fit_input_quality_summary"],
    )

    assert ok is True
    assert reasons == []
