import csv
import json

from gas_calibrator.validation.co2_s13_low_end_anchor_target_audit import (
    build_co2_s13_low_end_anchor_target_audit,
    write_co2_s13_low_end_anchor_target_audit,
)


def _write_csv(path, rows):
    header = []
    for row in rows:
        for key in row:
            if key not in header:
                header.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def _rows():
    rows = []
    devices = ("058", "070", "082")
    for device_index, device in enumerate(devices):
        ratio_offset = device_index * 0.0007
        for temp in (0.0, 20.0, 30.0, 40.0):
            source = "main_T0_T20" if temp <= 20.0 else "T30_low_end_supplement_20260620"
            for target in (0.0, 100.0, 200.0, 400.0, 800.0, 1000.0):
                ratio = 0.004 + target / 1000.0 + 0.0008 * temp + ratio_offset
                physical_target = 6.0 + 900.0 * ratio + 0.035 * (temp + 273.15)
                if target == 0.0:
                    target_value = 0.0
                    zero_class = "estimated_zero_anchor"
                else:
                    target_value = physical_target
                    zero_class = "standard_fit_point"
                if temp == 20.0 and target == 100.0:
                    target_value -= 8.0
                if temp == 30.0 and target == 100.0:
                    target_value += 8.0
                rows.append(
                    {
                        "component": "co2",
                        "analyzer_device_id": device,
                        "analyzer_prefix": f"GA{device_index + 1:02d}",
                        "source_role": "fit",
                        "point_identity": f"T{temp:g}_{target:g}ppm",
                        "target_value": f"{target_value:.9f}",
                        "source_nominal_ppm": f"{target:.1f}",
                        "temp_set_c": f"{temp:.1f}",
                        "ratio": f"{ratio:.12f}",
                        "temperature_c": f"{temp:.3f}",
                        "pressure_hpa": "1012.8",
                        "zero_anchor_class": zero_class,
                        "target_uncertainty_ppm": "8.0" if target == 0.0 else "1.0",
                        "sample_count": "60",
                        "usable_sample_count": "60",
                        "co2_ratio_f_mean": f"{ratio:.12f}",
                        "co2_ratio_f_std": "0.0002",
                        "dewpoint_mean_c": "-31.5",
                        "source_label": source,
                        "source_sample_path": f"D:/logs/{source}/co2/p001_T{temp:g}_{target:g}ppm/samples_machine_readable.csv",
                        "fit_inclusion_status": "included",
                    }
                )
    return rows


def test_low_end_anchor_target_audit_tracks_partitions_and_zero_anchor(tmp_path):
    evidence = tmp_path / "fit_points.csv"
    _write_csv(evidence, _rows())

    tables = build_co2_s13_low_end_anchor_target_audit(
        fit_points_csv=evidence,
        structures=("core_plus_full_temp",),
        objectives=("absolute_lstsq", "relative_irls_lstsq"),
        zero_offsets_ppm=(0.0, 5.0),
    )

    partitions = {row["source_label"]: row for row in tables["run_partition_summary"]}
    assert "main_T0_T20" in partitions
    assert "T30_low_end_supplement_20260620" in partitions
    assert tables["zero_anchor_assignment_audit"]
    assert all(row["auto_exclude_allowed"] is False for row in tables["target_state_audit"])
    assert tables["point_exclusion_sensitivity"]
    assert all(row["auto_exclude_allowed"] is False for row in tables["point_exclusion_sensitivity"])


def test_low_end_anchor_target_audit_writes_no_write_artifacts(tmp_path):
    evidence = tmp_path / "fit_points.csv"
    _write_csv(evidence, _rows())

    outputs = write_co2_s13_low_end_anchor_target_audit(
        fit_points_csv=evidence,
        output_dir=tmp_path / "audit",
        structures=("core_plus_full_temp",),
        objectives=("absolute_lstsq", "relative_irls_lstsq"),
        zero_offsets_ppm=(0.0, 5.0),
    )

    meta = json.loads(outputs["metadata"].read_text(encoding="utf-8"))
    assert meta["boundary"]["opens_com_ports"] is False
    assert meta["boundary"]["writes_coefficients"] is False
    assert meta["boundary"]["uses_s5_output_trim"] is False
    text = outputs["markdown"].read_text(encoding="utf-8-sig")
    assert "低端锚点与目标状态审计" in text
    assert "S5 输出层线性修正不参与" in text
    assert "乱码" not in text
