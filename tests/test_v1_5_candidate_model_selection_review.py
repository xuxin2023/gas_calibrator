import csv

from gas_calibrator.tools.export_v1_5_candidate_model_selection_review import main


def _write_csv(path, rows):
    keys = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path):
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def test_candidate_model_selection_exports_no_write_review(tmp_path):
    candidate_dir = tmp_path / "co2_candidate"
    rows = []
    for idx, (target, ratio) in enumerate(
        [
            (0.0, 1.30),
            (100.0, 1.28),
            (400.0, 1.22),
            (900.0, 1.12),
            (1000.0, 1.10),
        ],
        start=1,
    ):
        rows.append(
            {
                "component": "co2",
                "analyzer_prefix": "ga01",
                "analyzer_device_id": "001",
                "residual_role": "fit",
                "sample_index": f"p{idx:03d}",
                "point_identity": f"p{idx:03d}_T20_{target:g}ppm",
                "target_value": target,
                "ratio": ratio,
                "temperature_c": 20.0,
                "pressure_hpa": 1010.0,
                "h2o_mmol": 0.2,
                "prediction": target * 1.02 + 3.0,
                "error": 3.0,
            }
        )
    _write_csv(candidate_dir / "candidate_fit_residuals.csv", rows)
    _write_csv(
        candidate_dir / "candidate_policy_summary.csv",
        [
            {
                "component": "co2",
                "analyzer_prefix": "ga01",
                "analyzer_device_id": "001",
                "candidate_status": "fit_ready_requires_verification",
                "factory_signal_health_gate": "pass_factory_signal_health",
            }
        ],
    )
    _write_csv(candidate_dir / "candidate_run_summary.csv", [{"component": "co2"}])

    output_dir = tmp_path / "review"
    rc = main(
        [
            "--candidate-dir",
            str(candidate_dir),
            "--output-dir",
            str(output_dir),
        ]
    )
    assert rc == 0
    model_rows = _read_csv(output_dir / "model_selection_summary.csv")
    trim_rows = _read_csv(output_dir / "linear_trim_review.csv")
    assert any(row["recommended_model"] == "true" for row in model_rows)
    assert any(row["trim_method"] == "neutral_no_s5_s6" for row in trim_rows)
    assert any(row["trim_method"] == "minimax_relative_rounded_3dp" for row in trim_rows)
    assert (output_dir / "model_selection_review_zh.md").exists()
