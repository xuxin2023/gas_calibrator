from gas_calibrator.v2.domain.result_models import PointResult, RunArtifactManifest


def test_point_result_defaults() -> None:
    result = PointResult(point_index=1)

    assert result.sample_count == 0
    assert result.accepted is True
    assert result.notes == ""


def test_run_artifact_manifest_fields() -> None:
    manifest = RunArtifactManifest(
        run_id="run_001",
        raw_samples_file="raw.csv",
        point_results_file="points.csv",
        run_summary_file="summary.json",
        config_snapshot_file="config.json",
    )

    assert manifest.run_id == "run_001"
    assert manifest.config_snapshot_file == "config.json"
