from gas_calibrator.v2.ui_v2.utils.app_info import APP_INFO


def test_app_info_exposes_product_identity() -> None:
    payload = APP_INFO.as_dict()

    assert payload["product_name"] == "气体校准 V2 驾驶舱"
    assert payload["product_id"] == "gas-calibrator-v2"
    assert payload["version"]
    assert payload["build"]
