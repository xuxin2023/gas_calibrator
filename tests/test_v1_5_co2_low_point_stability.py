import csv

from gas_calibrator.validation.co2_low_point_stability import (
    Co2LowPointStabilityConfig,
    build_co2_low_point_stability_tables,
)


def _write_csv(path, rows):
    fields = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _row(idx, co2, ratio, target=99.94, device="030"):
    return {
        "co2_ppm_target": target,
        "dewpoint_c": -40.0,
        "ga02_analyzer_device_id": device,
        "ga02_co2_ppm": co2,
        "ga02_co2_ratio_f": ratio,
        "ga02_h2o_mmol": 0.1,
        "ga02_chamber_temp_c": 22.5,
        "ga02_pressure_kpa": 100.1,
        "sample_index": idx,
    }


def test_low_point_stability_marks_clean_low_point_fit_eligible(tmp_path):
    sample_csv = tmp_path / "samples.csv"
    rows = [_row(i, 99.9 + (i % 2) * 0.02, 1.40000 + (i % 2) * 0.00002) for i in range(40)]
    _write_csv(sample_csv, rows)

    tables = build_co2_low_point_stability_tables(
        [sample_csv],
        cfg=Co2LowPointStabilityConfig(target_device_ids=("030",), min_samples=20),
    )

    diag = tables["device_low_point_diagnostics"][0]
    assert diag["fit_role_recommendation"] == "fit_eligible"
    assert diag["reason"] == "low_point_stable_and_within_acceptance"


def test_low_point_stability_blocks_moving_history_even_with_stable_tail(tmp_path):
    sample_csv = tmp_path / "samples.csv"
    rows = []
    for i in range(80):
        if i < 60:
            co2 = 105.0 - i * 0.1
            ratio = 1.397 + i * 0.00008
        else:
            co2 = 99.0 + (i % 2) * 0.02
            ratio = 1.402 + (i % 2) * 0.00001
        rows.append(_row(i, co2, ratio))
    _write_csv(sample_csv, rows)

    tables = build_co2_low_point_stability_tables(
        [sample_csv],
        cfg=Co2LowPointStabilityConfig(target_device_ids=("030",), min_samples=20),
    )

    diag = tables["device_low_point_diagnostics"][0]
    assert diag["fit_role_recommendation"] == "diagnostic_only"
    assert diag["reason"] == "low_point_history_moves_even_if_tail_is_stable"


def test_low_point_stability_marks_stable_biased_point_diagnostic_only(tmp_path):
    sample_csv = tmp_path / "samples.csv"
    rows = [_row(i, 97.0 + (i % 2) * 0.02, 1.40400 + (i % 2) * 0.00002) for i in range(40)]
    _write_csv(sample_csv, rows)

    tables = build_co2_low_point_stability_tables(
        [sample_csv],
        cfg=Co2LowPointStabilityConfig(target_device_ids=("030",), min_samples=20),
    )

    diag = tables["device_low_point_diagnostics"][0]
    assert diag["fit_role_recommendation"] == "diagnostic_only"
    assert diag["reason"] == "stable_low_point_bias_review_source_or_model"
