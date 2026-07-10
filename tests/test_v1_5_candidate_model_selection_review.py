import csv
import json

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


def _write_fit_input_quality(root, *, devices=("001",), rejected=()):
    summary_path = root / "v1_5_fit_input_quality_summary.csv"
    devices_path = root / "v1_5_fit_input_quality_devices.csv"
    _write_csv(
        summary_path,
        [
            {
                "run_status": "pass",
                "fit_input_continuity_gate_status": "pass",
                "opens_com_ports": "False",
                "controls_water_or_gas_routes": "False",
                "writes_coefficients": "False",
            }
        ],
    )
    _write_csv(
        devices_path,
        [
            {
                "component": "co2",
                "analyzer_device_id": device_id,
                "fit_input_grade": "REJECT" if device_id in rejected else "A",
                "fit_input_status": "rejected" if device_id in rejected else "usable_for_candidate_fit",
                "reject_reasons": "route_state_conflict" if device_id in rejected else "",
            }
            for device_id in devices
        ],
    )
    return summary_path.resolve(), devices_path.resolve()


def _seed_candidate_dir(candidate_dir, *, devices=("001",), quality_paths=None):
    rows = []
    for device_offset, device_id in enumerate(devices):
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
                    "analyzer_prefix": f"ga{device_offset + 1:02d}",
                    "analyzer_device_id": device_id,
                    "residual_role": "fit",
                    "sample_index": f"p{idx:03d}",
                    "point_identity": f"p{idx:03d}_T20_{target:g}ppm",
                    "target_value": target,
                    "ratio": ratio + device_offset * 0.001,
                    "temperature_c": 20.0,
                    "pressure_hpa": 1010.0,
                    "h2o_mmol": 0.2,
                    "prediction": target * 1.02 + 3.0,
                    "error": 3.0,
                }
            )
    _write_csv(candidate_dir / "candidate_fit_residuals.csv", rows)
    summary_source = str(quality_paths[0]) if quality_paths else ""
    devices_source = str(quality_paths[1]) if quality_paths else ""
    _write_csv(
        candidate_dir / "candidate_policy_summary.csv",
        [
            {
                "component": "co2",
                "analyzer_prefix": f"ga{device_offset + 1:02d}",
                "analyzer_device_id": device_id,
                "candidate_status": "fit_ready_requires_verification",
                "factory_signal_health_gate": "pass_factory_signal_health",
                "fit_input_quality_summary_source": summary_source,
                "fit_input_quality_devices_source": devices_source,
                "fit_input_quality_grade": "A",
                "fit_input_quality_status": "usable_for_candidate_fit",
            }
            for device_offset, device_id in enumerate(devices)
        ],
    )
    _write_csv(
        candidate_dir / "candidate_run_summary.csv",
        [
            {
                "component": "co2",
                "fit_input_quality_required": bool(quality_paths),
                "fit_input_quality_gate_status": "pass" if quality_paths else "not_configured",
                "fit_input_quality_summary_source": summary_source,
                "fit_input_quality_devices_source": devices_source,
            }
        ],
    )


def test_candidate_model_selection_exports_no_write_review(tmp_path):
    candidate_dir = tmp_path / "co2_candidate"
    quality_paths = _write_fit_input_quality(tmp_path / "fit_input_quality")
    _seed_candidate_dir(candidate_dir, quality_paths=quality_paths)

    output_dir = tmp_path / "review"
    rc = main(
        [
            "--candidate-dir",
            str(candidate_dir),
            "--output-dir",
            str(output_dir),
            "--fit-input-quality-summary-csv",
            str(quality_paths[0]),
            "--fit-input-quality-devices-csv",
            str(quality_paths[1]),
            "--require-fit-input-quality",
        ]
    )
    assert rc == 0
    model_rows = _read_csv(output_dir / "model_selection_summary.csv")
    trim_rows = _read_csv(output_dir / "linear_trim_review.csv")
    assert any(row["recommended_model"] == "true" for row in model_rows)
    assert any(row["trim_method"] == "neutral_no_s5_s6" for row in trim_rows)
    assert any(row["trim_method"] == "minimax_relative_rounded_3dp" for row in trim_rows)
    meta = json.loads((output_dir / "model_selection_meta.json").read_text(encoding="utf-8"))
    assert meta["overall_status"] == "pass"
    assert meta["fit_input_quality_required"] is True
    assert meta["eligible_group_count"] == 1
    assert meta["blocked_group_count"] == 0
    assert (output_dir / "model_selection_review_zh.md").exists()


def test_candidate_model_selection_blocks_old_candidate_package_when_gate_is_required(tmp_path):
    candidate_dir = tmp_path / "legacy_candidate"
    _seed_candidate_dir(candidate_dir)
    output_dir = tmp_path / "blocked_review"

    rc = main(
        [
            "--candidate-dir",
            str(candidate_dir),
            "--output-dir",
            str(output_dir),
            "--require-fit-input-quality",
        ]
    )

    assert rc == 0
    model_rows = _read_csv(output_dir / "model_selection_summary.csv")
    assert len(model_rows) == 1
    assert model_rows[0]["fit_status"] == "blocked_fit_input_quality"
    assert model_rows[0]["model_name"] == ""
    assert _read_csv(output_dir / "model_selection_residuals.csv") == []
    assert _read_csv(output_dir / "linear_trim_review.csv") == []
    meta = json.loads((output_dir / "model_selection_meta.json").read_text(encoding="utf-8"))
    assert meta["overall_status"] == "blocked"
    assert meta["eligible_group_count"] == 0
    assert meta["blocked_group_count"] == 1


def test_candidate_model_selection_blocks_only_rejected_device(tmp_path):
    candidate_dir = tmp_path / "mixed_candidate"
    quality_paths = _write_fit_input_quality(
        tmp_path / "fit_input_quality",
        devices=("001", "002"),
        rejected=("002",),
    )
    _seed_candidate_dir(candidate_dir, devices=("001", "002"), quality_paths=quality_paths)
    output_dir = tmp_path / "partial_review"

    rc = main(
        [
            "--candidate-dir",
            str(candidate_dir),
            "--output-dir",
            str(output_dir),
            "--fit-input-quality-summary-csv",
            str(quality_paths[0]),
            "--fit-input-quality-devices-csv",
            str(quality_paths[1]),
            "--require-fit-input-quality",
        ]
    )

    assert rc == 0
    model_rows = _read_csv(output_dir / "model_selection_summary.csv")
    assert any(row["analyzer_device_id"] == "001" and row["recommended_model"] == "true" for row in model_rows)
    blocked_rows = [row for row in model_rows if row["analyzer_device_id"] == "002"]
    assert len(blocked_rows) == 1
    assert blocked_rows[0]["fit_status"] == "blocked_fit_input_quality"
    assert {row["analyzer_device_id"] for row in _read_csv(output_dir / "model_selection_residuals.csv")} == {"001"}
    assert {row["analyzer_device_id"] for row in _read_csv(output_dir / "linear_trim_review.csv")} == {"001"}
    meta = json.loads((output_dir / "model_selection_meta.json").read_text(encoding="utf-8"))
    assert meta["overall_status"] == "partial"
    assert meta["eligible_group_count"] == 1
    assert meta["blocked_group_count"] == 1


def test_candidate_model_selection_rejects_replacement_fit_input_artifacts(tmp_path):
    candidate_dir = tmp_path / "bound_candidate"
    original_quality_paths = _write_fit_input_quality(tmp_path / "original_fit_input_quality")
    replacement_quality_paths = _write_fit_input_quality(tmp_path / "replacement_fit_input_quality")
    _seed_candidate_dir(candidate_dir, quality_paths=original_quality_paths)
    output_dir = tmp_path / "replacement_blocked_review"

    rc = main(
        [
            "--candidate-dir",
            str(candidate_dir),
            "--output-dir",
            str(output_dir),
            "--fit-input-quality-summary-csv",
            str(replacement_quality_paths[0]),
            "--fit-input-quality-devices-csv",
            str(replacement_quality_paths[1]),
            "--require-fit-input-quality",
        ]
    )

    assert rc == 0
    model_rows = _read_csv(output_dir / "model_selection_summary.csv")
    assert len(model_rows) == 1
    assert model_rows[0]["fit_status"] == "blocked_fit_input_quality"
    assert "candidate_summary_fit_input_quality_summary_source_mismatch" in model_rows[0]["review_note"]
    assert _read_csv(output_dir / "linear_trim_review.csv") == []
