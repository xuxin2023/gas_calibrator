from __future__ import annotations

import csv
from pathlib import Path

from gas_calibrator.validation.co2_state_bridge import write_co2_state_bridge_report


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers: list[str] = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def _sample_row(*, target: float, ratio: float, temp: float = 20.0, h2o: float = 0.5) -> dict[str, object]:
    return {
        "co2_ppm_target": target,
        "pressure_gauge_hpa": 1005.0,
        "ga05_analyzer_device_id": "100",
        "ga05_frame_usable": "true",
        "ga05_co2_ppm": target * 0.5,
        "ga05_co2_ratio_f": ratio,
        "ga05_chamber_temp_c": temp,
        "ga05_h2o_mmol": h2o,
    }


def _make_old_run(root: Path) -> None:
    pairs = [
        ("p001_T20_100ppm_fit", 100.0, 1.40),
        ("p002_T20_200ppm_fit", 200.0, 1.35),
        ("p003_T20_300ppm_fit", 300.0, 1.30),
        ("p004_T20_400ppm_fit", 400.0, 1.25),
        ("p005_T20_500ppm_fit", 500.0, 1.20),
        ("p006_T20_600ppm_verification", 600.0, 1.15),
    ]
    for point, target, ratio in pairs:
        _write_csv(
            root / point / "samples_machine_readable.csv",
            [
                _sample_row(target=target, ratio=ratio),
                _sample_row(target=target, ratio=ratio + 0.00002),
            ],
        )


def _make_current_summary(path: Path, *, bad_ratio: bool = False) -> None:
    rows = []
    for point, target, ratio in [
        ("p001_T20_150ppm_verification", 150.0, 1.375),
        ("p002_T20_450ppm_verification", 450.0, 1.225),
    ]:
        rows.append(
            {
                "point_run_id": point,
                "device_id": "100",
                "analyzer_label": "GA05",
                "certificate_co2_ppm": target,
                "measured_co2_ppm": target * 0.8,
                "co2_ratio_f": ratio + (0.25 if bad_ratio else 0.0),
                "co2_ratio_f_dev": 0.00004,
                "chamber_temp_c": 20.0,
                "pressure_hpa": 1005.0,
                "h2o_mmol_mol": 0.5,
                "valid_frames": 20,
                "total_frames": 20,
            }
        )
    _write_csv(path, rows)


def test_co2_state_bridge_explains_current_points_by_ratio_state(tmp_path: Path) -> None:
    old_run = tmp_path / "old"
    current = tmp_path / "current.csv"
    out = tmp_path / "out"
    _make_old_run(old_run)
    _make_current_summary(current)

    outputs = write_co2_state_bridge_report(
        old_run_dir=old_run,
        current_summary_csv=current,
        output_dir=out,
        target_device_id="100",
        bridge_rel_limit_pct=1.0,
    )

    manifest = Path(outputs["manifest"]).read_text(encoding="utf-8-sig")
    assert "bridge_explained_by_r_t_p_h2o" in manifest
    predictions = Path(outputs["predictions"]).read_text(encoding="utf-8-sig")
    assert "current_postwrite_freshgate" in predictions
    assert "GA05" in predictions


def test_co2_state_bridge_blocks_unexplained_current_ratio_state(tmp_path: Path) -> None:
    old_run = tmp_path / "old"
    current = tmp_path / "current.csv"
    out = tmp_path / "out"
    _make_old_run(old_run)
    _make_current_summary(current, bad_ratio=True)

    outputs = write_co2_state_bridge_report(
        old_run_dir=old_run,
        current_summary_csv=current,
        output_dir=out,
        target_device_id="100",
        bridge_rel_limit_pct=1.0,
    )

    manifest = Path(outputs["manifest"]).read_text(encoding="utf-8-sig")
    assert "bridge_not_explained_by_r_t_p_h2o" in manifest
