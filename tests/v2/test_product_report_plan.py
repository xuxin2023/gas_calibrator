from gas_calibrator.v2.export import build_product_report_manifest, build_product_report_templates


def test_product_report_templates_enable_calibration_only_for_auto_mode() -> None:
    auto_templates = build_product_report_templates(run_mode="auto_calibration", route_mode="h2o_then_co2")
    measurement_templates = build_product_report_templates(run_mode="co2_measurement", route_mode="co2_only")

    auto_keys = {item.key for item in auto_templates if item.enabled}
    measurement_keys = {item.key for item in measurement_templates if item.enabled}

    assert auto_keys == {
        "co2_test_report",
        "co2_calibration_report",
        "h2o_test_report",
        "h2o_calibration_report",
    }
    assert measurement_keys == {"co2_test_report"}
    h2o_calibration = next(item for item in auto_templates if item.key == "h2o_calibration_report")
    assert h2o_calibration.implementation_status == "first_exporter_available"
    assert h2o_calibration.current_exporter == "storage.exporter.export_h2o_calibration_reports"


def test_product_report_manifest_marks_per_device_template_family() -> None:
    manifest = build_product_report_manifest(run_mode="h2o_measurement", route_mode="h2o_only")

    assert manifest["report_family"] == "v2_product_report_family"
    assert manifest["per_device_output"] is True
    assert manifest["template_count"] == 2
    assert {item["key"] for item in manifest["templates"]} == {"h2o_test_report", "h2o_calibration_report"}
    assert any(item["component"] == "ratio_poly_report" for item in manifest["current_capabilities"])
    assert any(
        item["component"] == "storage.exporter.export_h2o_calibration_reports"
        for item in manifest["current_capabilities"]
    )
    calibration = next(item for item in manifest["templates"] if item["key"] == "h2o_calibration_report")
    assert calibration["enabled"] is False
    assert calibration["implementation_status"] == "gated_off"
