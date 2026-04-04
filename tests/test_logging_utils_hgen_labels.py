from gas_calibrator.logging_utils import _field_label, _translate_row


def test_humidity_generator_field_labels_use_semantic_names() -> None:
    assert _field_label("co2_mean") == "二氧化碳平均值"
    assert _field_label("h2o_mean") == "水平均值"
    assert _field_label("pressure_mean") == "压力平均值"
    assert _field_label("hgen_Td") == "湿度发生器_露点(℃)"
    assert _field_label("hgen_Tc") == "湿度发生器_当前温度(℃)"
    assert _field_label("hgen_Tf") == "湿度发生器_霜点(℃)"
    assert _field_label("hgen_Ts") == "湿度发生器_设定温度(℃)"
    assert _field_label("hgen_Uw") == "湿度发生器_当前湿度(%RH)"
    assert _field_label("hgen_temp_c") == "湿度发生器_目标温度(℃)"
    assert _field_label("hgen_rh_pct") == "湿度发生器_目标湿度(%RH)"
    assert _field_label("hgen_Fl") == "湿度发生器_流量(L/min)"
    assert _field_label("hgen_Flux") == "湿度发生器_流量(L/min)"
    assert _field_label("hgen_Pc") == "湿度发生器_当前压力"
    assert _field_label("hgen_Ps") == "湿度发生器_供气压力"
    assert _field_label("hgen_PST") == "湿度发生器_压力稳定计时(s)"
    assert _field_label("hgen_TST") == "湿度发生器_温度稳定计时(s)"
    assert _field_label("hgen_Ui") == "湿度发生器_绝对湿度"
    assert _field_label("hgen_Td_mean") == "湿度发生器_露点(℃)_平均值"


def test_translate_row_uses_semantic_humidity_generator_labels() -> None:
    row = _translate_row({"hgen_Td": 1.2, "hgen_Tc_mean": 20.1})
    assert row["湿度发生器_露点(℃)"] == 1.2
    assert row["湿度发生器_当前温度(℃)_平均值"] == 20.1
