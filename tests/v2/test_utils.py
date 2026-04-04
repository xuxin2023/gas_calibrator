import pytest

from gas_calibrator.v2.utils import (
    as_bool,
    as_float,
    as_int,
    clamp,
    format_number,
    parse_first_float,
    parse_first_int,
    safe_get,
)


@pytest.mark.parametrize(
    ("value", "default", "allow_none", "expected"),
    [
        ("3.14", None, True, 3.14),
        (5, None, True, 5.0),
        ("", 1.2, True, 1.2),
        (None, 0.0, False, 0.0),
        ("bad", None, False, 0.0),
    ],
)
def test_as_float_cases(value, default, allow_none, expected) -> None:
    assert as_float(value, default=default, allow_none=allow_none) == expected


@pytest.mark.parametrize(
    ("value", "default", "allow_none", "expected"),
    [
        ("42", None, True, 42),
        ("3.9", None, True, 3),
        (8.7, None, True, 8),
        ("", 5, True, 5),
        ("bad", None, False, 0),
    ],
)
def test_as_int_cases(value, default, allow_none, expected) -> None:
    assert as_int(value, default=default, allow_none=allow_none) == expected


@pytest.mark.parametrize(
    ("value", "default", "expected"),
    [
        (True, False, True),
        ("YES", False, True),
        ("off", True, False),
        (0, True, False),
        ("unexpected", True, True),
    ],
)
def test_as_bool_cases(value, default, expected) -> None:
    assert as_bool(value, default=default) is expected


@pytest.mark.parametrize(
    ("text", "default", "allow_none", "expected"),
    [
        ("Temperature: 25.3 C", None, True, 25.3),
        ("Value is -10.5", None, True, -10.5),
        ("100", None, True, 100.0),
        ("No number", 0.0, True, 0.0),
        ("", None, False, 0.0),
    ],
)
def test_parse_first_float_cases(text, default, allow_none, expected) -> None:
    assert parse_first_float(text, default=default, allow_none=allow_none) == expected


@pytest.mark.parametrize(
    ("text", "default", "allow_none", "expected"),
    [
        ("Channel: 5", None, True, 5),
        ("Value=-12.8", None, True, -12),
        ("A100B200", None, True, 100),
        ("No number", 7, True, 7),
        ("", None, False, 0),
    ],
)
def test_parse_first_int_cases(text, default, allow_none, expected) -> None:
    assert parse_first_int(text, default=default, allow_none=allow_none) == expected


@pytest.mark.parametrize(
    ("data", "keys", "default", "expected"),
    [
        ({"a": {"b": {"c": 42}}}, ("a", "b", "c"), None, 42),
        ({"a": {"b": None}}, ("a", "b"), None, None),
        ({"a": {"b": {"c": 42}}}, ("a", "x"), "missing", "missing"),
        ({}, ("a",), 0, 0),
        ({"a": 1}, ("a", "b"), "fallback", "fallback"),
    ],
)
def test_safe_get_cases(data, keys, default, expected) -> None:
    assert safe_get(data, *keys, default=default) == expected


@pytest.mark.parametrize(
    ("value", "min_value", "max_value", "expected"),
    [
        (5, 0, 10, 5),
        (-1, 0, 10, 0),
        (11, 0, 10, 10),
        (0, 0, 10, 0),
        (10, 0, 10, 10),
    ],
)
def test_clamp_cases(value, min_value, max_value, expected) -> None:
    assert clamp(value, min_value, max_value) == expected


@pytest.mark.parametrize(
    ("value", "decimals", "unit", "expected"),
    [
        (25.567, 1, "C", "25.6 C"),
        (0, 2, "", "0.00"),
        (None, 2, "", "--"),
        (-3.1415, 3, "V", "-3.142 V"),
        (12.0, 0, "ppm", "12 ppm"),
    ],
)
def test_format_number_cases(value, decimals, unit, expected) -> None:
    assert format_number(value, decimals=decimals, unit=unit) == expected
