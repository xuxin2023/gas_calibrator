import csv

from gas_calibrator.validation.factory_signal_health_review import (
    FactorySignalHealthConfig,
    build_factory_signal_health_tables,
    write_factory_signal_health_report,
)


def _write_csv(path, rows):
    keys = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def test_factory_signal_review_blocks_high_ref_with_large_residual(tmp_path):
    means = tmp_path / "point_means.csv"
    residuals = tmp_path / "residuals.csv"
    _write_csv(
        means,
        [
            {
                "component": "co2",
                "device_id": "079",
                "analyzer_prefix": "ga01",
                "source_run_id": "p031_T10_500ppm_fit",
                "point_tag": "open_flow_500ppm",
                "target": "500.13",
                "temp_set_c": "10",
                "ratio_median": "1.1609",
                "ratio_span": "0.0001",
                "ref_signal_median": "4096",
                "co2_signal_median": "4754",
                "h2o_signal_median": "3034",
                "dewpoint_c_median": "-36.7",
            },
            {
                "component": "co2",
                "device_id": "084",
                "analyzer_prefix": "ga06",
                "source_run_id": "p031_T10_500ppm_fit",
                "point_tag": "open_flow_500ppm",
                "target": "500.13",
                "temp_set_c": "10",
                "ratio_median": "1.201",
                "ratio_span": "0.0001",
                "ref_signal_median": "3400",
                "co2_signal_median": "4083",
                "h2o_signal_median": "120",
                "dewpoint_c_median": "-37.0",
            },
        ],
    )
    _write_csv(
        residuals,
        [
            {
                "component": "co2",
                "device_id": "079",
                "source_run_id": "p031_T10_500ppm_fit",
                "error": "104.14",
                "relative_error_pct": "20.82",
            },
            {
                "component": "co2",
                "device_id": "084",
                "source_run_id": "p031_T10_500ppm_fit",
                "error": "3.0",
                "relative_error_pct": "0.6",
            },
        ],
    )

    tables = build_factory_signal_health_tables(
        point_means_csv=means,
        residuals_csv=residuals,
        cfg=FactorySignalHealthConfig(target_device_ids=("079", "084"), min_point_count_for_pass=1),
    )

    summary = {row["device_id"]: row for row in tables["summary"]}
    assert summary["079"]["candidate_gate"] == "block_optical_reference_health_review"
    assert summary["084"]["candidate_gate"] == "pass_factory_signal_health"
    flagged = [row for row in tables["point_flags"] if row["device_id"] == "079"][0]
    assert "ref_signal_near_configured_full_scale_hint" in flagged["signal_health_flags"]
    assert "stable_ratio_but_reference_chain_unhealthy" in flagged["signal_health_flags"]


def test_factory_signal_review_writes_utf8_chinese_report(tmp_path):
    means = tmp_path / "point_means.csv"
    _write_csv(
        means,
        [
            {
                "component": "h2o",
                "device_id": "002",
                "source_run_id": "p001_T10_HG10C_30RH_h2o",
                "target": "3.5",
                "temp_set_c": "10",
                "ratio_median": "0.741",
                "ratio_span": "0.0002",
                "ref_signal_median": "3300",
                "h2o_signal_median": "2445",
            }
        ],
    )

    outputs = write_factory_signal_health_report(
        point_means_csv=means,
        output_dir=tmp_path / "out",
        cfg=FactorySignalHealthConfig(target_device_ids=("002",)),
    )

    text = outputs["markdown"].read_text(encoding="utf-8")
    assert "工厂模式参考信号健康评审" in text
    assert "不打开 COM" in text
    assert "?" not in text


def test_factory_signal_review_does_not_pass_insufficient_coverage(tmp_path):
    means = tmp_path / "point_means.csv"
    _write_csv(
        means,
        [
            {
                "component": "co2",
                "device_id": "073",
                "source_run_id": "p001_T40_0ppm_fit",
                "target": "0",
                "ratio_median": "1.23",
                "ratio_span": "0.0001",
                "ref_signal_median": "3124",
                "co2_signal_median": "3840",
            },
            {
                "component": "h2o",
                "device_id": "073",
                "source_run_id": "p001_T10_HG10C_30RH_h2o",
                "target": "3.5",
                "ratio_median": "0.75",
                "ratio_span": "0.0001",
                "ref_signal_median": "3124",
                "h2o_signal_median": "2343",
            },
        ],
    )

    tables = build_factory_signal_health_tables(
        point_means_csv=means,
        cfg=FactorySignalHealthConfig(target_device_ids=("073",), min_point_count_for_pass=5),
    )

    assert tables["summary"][0]["candidate_gate"] == "review_insufficient_factory_signal_coverage"
