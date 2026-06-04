from __future__ import annotations

from gas_calibrator.senco_format import format_senco_value, rounded_senco_values, senco_readback_matches


def test_rounded_senco_values_matches_device_payload_precision() -> None:
    rounded = rounded_senco_values((-1.737266666666675, 1.0, 0.0, 0.0))

    assert rounded == (-1.73727, 1.0, 0.0, 0.0)


def test_format_senco_value_matches_manual_lowercase_scientific_notation() -> None:
    assert format_senco_value(65916.6) == "6.59166e04"
    assert "E" not in format_senco_value(65916.6)


def test_senco_readback_matches_uses_senco_rounded_expected_values() -> None:
    expected = (-1.737266666666675, 1.0, 0.0)
    actual = (-1.73727, 1.0, 0.0)

    assert senco_readback_matches(expected, actual) is True


def test_senco_readback_matches_rejects_length_mismatch() -> None:
    assert senco_readback_matches((1.0, 2.0, 3.0), (1.0, 2.0)) is False
