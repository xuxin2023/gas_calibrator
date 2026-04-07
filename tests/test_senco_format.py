from __future__ import annotations

from gas_calibrator.senco_format import rounded_senco_values, senco_readback_matches


def test_rounded_senco_values_matches_device_payload_precision() -> None:
    rounded = rounded_senco_values((-1.737266666666675, 1.0, 0.0, 0.0))

    assert rounded == (-1.73727, 1.0, 0.0, 0.0)


def test_senco_readback_matches_uses_senco_rounded_expected_values() -> None:
    expected = (-1.737266666666675, 1.0, 0.0)
    actual = (-1.73727, 1.0, 0.0)

    assert senco_readback_matches(expected, actual) is True


def test_senco_readback_matches_rejects_length_mismatch() -> None:
    assert senco_readback_matches((1.0, 2.0, 3.0), (1.0, 2.0)) is False
