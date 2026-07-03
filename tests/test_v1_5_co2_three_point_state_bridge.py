from __future__ import annotations

import csv
import json
import re
from pathlib import Path

from gas_calibrator.validation.co2_three_point_state_bridge import (
    write_three_point_state_bridge_report,
)


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers: list[str] = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def _row(
    *,
    target: float,
    ratio: float,
    h2o: float,
    temp: float,
    dewpoint: float,
    displayed: float,
    device_id: str = "100",
) -> dict[str, object]:
    return {
        "co2_ppm_target": target,
        "pressure_gauge_hpa": 1005.0,
        "dewpoint_live_c": dewpoint,
        "ga05_analyzer_device_id": device_id,
        "ga05_frame_usable": "true",
        "ga05_co2_ppm": displayed,
        "ga05_co2_ratio_f": ratio,
        "ga05_co2_ratio_raw": ratio + 0.0001,
        "ga05_h2o_ratio_f": 0.86,
        "ga05_h2o_mmol": h2o,
        "ga05_chamber_temp_c": temp,
        "ga05_case_temp_c": temp + 0.1,
        "ga05_pressure_kpa": 100.5,
    }


def _make_old_run(root: Path) -> None:
    for name, target, ratio in [
        ("p001_T20_100ppm_fit", 100.0, 1.36),
        ("p002_T20_800ppm_verification", 800.0, 1.20),
        ("p003_T20_900ppm_fit", 900.0, 1.18),
        ("p004_T30_100ppm_fit", 100.0, 1.365),
        ("p005_T30_800ppm_verification", 800.0, 1.205),
        ("p006_T30_900ppm_fit", 900.0, 1.185),
        ("p007_T10_100ppm_fit", 100.0, 1.355),
        ("p008_T10_800ppm_verification", 800.0, 1.195),
        ("p009_T10_900ppm_fit", 900.0, 1.175),
    ]:
        temp_match = re.search(r"_T(-?\d+(?:\.\d+)?)_", name)
        assert temp_match is not None
        temp = float(temp_match.group(1))
        _write_csv(
            root / name / "samples_machine_readable.csv",
            [
                _row(target=target, ratio=ratio, h2o=2.0, temp=temp, dewpoint=-24.0, displayed=target * 0.5),
                _row(target=target, ratio=ratio + 0.00002, h2o=2.1, temp=temp, dewpoint=-24.1, displayed=target * 0.5),
            ],
        )


def _make_current_points(root: Path) -> list[str]:
    files: list[str] = []
    for name, target, ratio in [
        ("p001_T20_100ppm_verification", 99.94, 1.365),
        ("p002_T20_800ppm_verification", 800.59, 1.201),
        ("p003_T20_900ppm_verification", 897.04, 1.181),
    ]:
        samples = root / name / "samples_machine_readable.csv"
        _write_csv(
            samples,
            [
                _row(target=target, ratio=ratio, h2o=0.2, temp=20.0, dewpoint=-30.0, displayed=target * 0.9),
                _row(target=target, ratio=ratio + 0.00002, h2o=0.2, temp=20.0, dewpoint=-30.0, displayed=target * 0.9),
            ],
        )
        files.append(str(samples))
    return files


def test_three_point_bridge_exports_physical_contract_and_three_current_points(tmp_path: Path) -> None:
    old_run = tmp_path / "old"
    current_root = tmp_path / "current"
    output = tmp_path / "out"
    _make_old_run(old_run)
    current_files = _make_current_points(current_root)

    outputs = write_three_point_state_bridge_report(
        old_run_dir=old_run,
        current_sample_files=current_files,
        output_dir=output,
        target_device_id="100",
    )

    manifest = json.loads(Path(outputs["manifest"]).read_text(encoding="utf-8-sig"))
    assert manifest["current_point_count"] == 3
    assert manifest["no_com_no_write"] is True
    assert "internal coefficients" in manifest["physical_contract"]

    markdown = Path(outputs["markdown"]).read_text(encoding="utf-8-sig")
    assert "旧全温数据采集时设备内部已经可能带有 SENCO/S5 等系数" in markdown
    assert "旧显示浓度不能直接跨系数状态比较" in markdown
    assert "100/800/900" in markdown

    shift_csv = Path(outputs["state_shift"]).read_text(encoding="utf-8-sig")
    assert "delta_ratio_f" in shift_csv
    assert "p002_T20_800ppm_verification" in shift_csv
