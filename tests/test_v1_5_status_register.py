import json

from gas_calibrator.validation.v1_5_status_register import classify_status_register


def test_status_register_decodes_manual_normal_register() -> None:
    review = classify_status_register("0001")

    assert review["qc_status"] == "pass"
    assert review["qc_reason"] == "ok"
    assert review["summary_cn"] == "状态寄存器正常"
    assert review["active_bits"] == []


def test_status_register_decodes_co2_signal_overrange() -> None:
    review = classify_status_register("0101")

    assert review["qc_status"] == "fail"
    assert "CO2信号超标" in review["qc_reason"]
    assert "CO2信号超标" in review["summary_cn"]
    bits = {row["bit"]: row for row in review["active_bits"]}
    assert bits[8]["name_cn"] == "CO2信号超标"
    assert "饱和" in bits[8]["physical_meaning_cn"]


def test_status_register_zero_is_not_manual_normal() -> None:
    review = classify_status_register("0000")

    assert review["qc_status"] == "fail"
    assert "系统运行" in review["summary_cn"]


def test_status_register_text_failure_is_preserved() -> None:
    review = classify_status_register("CO2_SIGNAL_FAIL")

    assert review["qc_status"] == "fail"
    assert review["qc_reason"] == "bad_status(CO2_SIGNAL_FAIL)"
    assert review["active_bits"][0]["name_cn"] == "文本状态异常"


def test_status_register_active_bits_are_json_serializable() -> None:
    review = classify_status_register("0301")

    encoded = json.dumps(review["active_bits"], ensure_ascii=False)

    assert "CO2信号超标" in encoded
    assert "H2O信号超标" in encoded
